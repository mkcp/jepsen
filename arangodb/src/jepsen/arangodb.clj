(ns jepsen.arangodb
  "ArangoDB test, based off of work started at https://github.com/arangodb/jepsen/."
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io    :as io]
            [clojure.string     :as str]
            [jepsen
             [db         :as db]
             [checker    :as checker]
             [client     :as client]
             [control    :as c]
             [generator  :as gen]
             [independent :as independent]
             [nemesis    :as nemesis]
             [tests      :as tests]
             [util       :refer [timeout]]
             [cli :as cli]]
            [jepsen.control.net :as net]
            [jepsen.control.util :as cu]
            [jepsen.checker.timeline :as timeline]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [base64-clj.core :as base64]))

(def dir "/var/lib/arangodb3")
(def binary "arangod")
(def logfile "/var/log/arangodb3/arangod.log")
(def pidfile "/arangod.pid")
(def port 8529)

(def statuses (atom []))

(defn peer-addr [node] (str (name node) ":" port ))
(defn addr [node] (str (name node) ":" port))
(defn cluster-info [node] (str (name node) "=http://" (name node) ":" port))

(defn deb-src [version]
  (str "https://download.arangodb.com/arangodb32/Debian_8.0/amd64/arangodb3-" version "-1_amd64.deb"))

(defn deb-dest [version] (str "arangodb3-" version "-1_amd64.deb"))

;; API call to agency directly
(defn client-url
  ([node] (client-url node nil))
  ([node f] (str "http://" node ":" port "/_api/agency/" f)))

(defn db [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (info node "Installing arangodb" version)

       ;; Deps
       (c/exec :apt-get :install "-y" "-qq" (str "libjemalloc1"))

       ;; Password stuff?
       (c/exec :echo :arangodb3 "arangodb3/password" "password" "" "|" :debconf-set-selections)
       (c/exec :echo :arangodb3 "arangodb3/password_again" "password" "" "|" :debconf-set-selections)

       ;; Download & install
       (c/exec :test "-f" (deb-dest version) "||" :wget :-q (deb-src version) :-O (deb-dest version))
       (c/exec :dpkg :-i (deb-dest version))

       ;; Ensure service stopped
       (c/exec :service :arangodb3 :stop)

       ;; Ensure data directories are clean
       (c/exec :rm :-rf :/var/lib/arangodb3)
       (c/exec :mkdir :/var/lib/arangodb3)
       (c/exec :chown :-R :arangodb :/var/lib/arangodb3)
       (c/exec :chgrp :-R :arangodb :/var/lib/arangodb3)

       ;; Run as service
       (c/exec :echo (-> "arangod.conf"
                         io/resource
                         slurp
                         (str/replace "$NODE_ADDRESS" (net/local-ip)))
               :> "/etc/arangodb3/arangod.conf")
       (c/exec :service :arangodb3 :start)

       ;; Give cluster time to stabalize. Anything under 30s seems to risk hitting 503s
       (Thread/sleep 30000)))

    (teardown! [_ test node]
      (info node "Tearing down arangodb")
      (c/su
       (info node "Stopping service arangodb3")
       (c/exec :service :arangodb3 :stop)
       (info node "Clearing data directory")
       (c/exec :rm :-rf :/var/lib/arangodb3)
       (c/exec :mkdir :/var/lib/arangodb3)
       (c/exec :chown :-R :arangodb :/var/lib/arangodb3)
       (c/exec :chgrp :-R :arangodb :/var/lib/arangodb3)
       (info node "Clearing log directory")
       (c/exec :rm :-rf :/var/log/arangodb3)))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/arangodb3/arangod.log"])))

(def http-opts {:conn-timeout 5000
                :content-type :json
                :trace-redirects true
                :redirect-strategy :lax
                :socket-timeout 5000})

(defn agency-read! [node key]
  (let [url   (client-url node "read")
        body (json/generate-string [[key]])]
    (http/post url (assoc http-opts :body body))))

(defn agency-write! [node key val]
  (let [url   (client-url node "write")
        body (json/generate-string [[{(str key) val}]])]
    (http/post url (assoc http-opts :body body))))

(defn agency-cas!
  [node key old new]
  (let [url     (client-url node "write")
        k       (keyword key)
        command [[{k new} {k old}]]
        body    (json/generate-string command)]
    (http/post url (assoc http-opts :body body))))

(defn read-parse [resp]
  (-> resp
      :body
      json/parse-string
      first
      first
      (get 1)))

(defn unavailable?    [e] (->> e .getMessage (re-find #"503") some?))
(defn precond-fail?   [e] (->> e .getMessage (re-find #"412") some?))
(defn read-timed-out? [e] (->> e .getMessage (re-find #"Read timed out") some?))

(defrecord CASClient [node]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (invoke! [this test op]
    (let [[k v] (:value op)
          crash (if (= :read (:f op)) :fail :info)
          node  (:node this)]
      (try
        (case (:f op)
          ;; FIXME What to do with read nils
          :read  (let [res (agency-read! node (str "/" k))
                       v   (read-parse res)]
                   (when-not v (warn :read v))
                   (assoc op :type :ok :value (independent/tuple k v)))

          :write (let [res (agency-write! node (str "/" k) v)]
                   (assoc op :type :ok))

          :cas   (let [[v v'] v
                       res    (agency-cas! node (str "/" k) v v')]
                   (assoc op :type :ok)))

        (catch Exception e
          (cond
            (precond-fail?   e) (assoc op :type :fail)
            (unavailable?    e) (assoc op :type crash :error :node-unavailable)
            (read-timed-out? e) (assoc op :type crash :error :read-timed-out)
            :else               (do (error (pr-str (.getMessage e)))
                                    (assoc op :type crash :error :indeterminate)))))))

  ;; HTTP clients are not stateful
  (close! [_ _])
  (setup! [_ _])
  (teardown! [_ _]))

(defn client [node] (CASClient. node))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn arangodb-test
  [opts]
  (info :opts opts)
  (merge tests/noop-test
         {:name    "arangodb"
          :os      debian/os
          :db      (db "3.2.7")
          :client  (client nil)
          :nemesis (nemesis/partition-random-halves)
          :model   (model/cas-register)
          :checker (checker/compose
                    {:perf  (checker/perf)
                     :indep (independent/checker
                             (checker/compose
                              {:timeline (timeline/html)
                               :linear   (checker/linearizable)}))})
          :generator (->> (independent/concurrent-generator
                           10
                           (range)
                           (fn [k]
                             (->> (gen/mix     [r w cas])
                                  (gen/stagger 1/30)
                                  (gen/limit   300))))
                          (gen/nemesis
                           (gen/seq (cycle [(gen/sleep 5)
                                            {:type :info, :f :start}
                                            (gen/sleep 5)
                                            {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}

         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn arangodb-test})
                   (cli/serve-cmd))
            args))
