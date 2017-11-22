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
       (info node "installing arangodb" version)

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

       (Thread/sleep 30000)))

    (teardown! [_ test node]
      (info node "tearing down arangodb agency")
      (c/su
       (info node "stopping service arangodb3")
       (c/exec :service :arangodb3 :stop)
       (info node "nuking arangodb3 directory")
       (c/exec :rm :-rf :/var/lib/arangodb3)
       (c/exec :mkdir :/var/lib/arangodb3)
       (c/exec :chown :-R :arangodb :/var/lib/arangodb3)
       (c/exec :chgrp :-R :arangodb :/var/lib/arangodb3)))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/arangodb3/arangod.log"])))

(defn r   [_ _] {:type :invoke, :f :read})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(def http-opts {:conn-timeout 5000
                :content-type :json
                :follow-redirects true
                :force-redirects true
                :socket-timeout 5000})

(defn agency-read! [node key]
  (let [bbody (json/generate-string [[key]])
        url   (client-url node "read")]
    (http/post url (assoc http-opts :body bbody))))

(defn agency-write! [node key val]
  (let [bbody (json/generate-string [[{(str key) val}]])
        url   (client-url node "write")]
    (http/post url (assoc http-opts :body bbody))))

(defn agency-cas!
  [node key old new]
  (let [k       (keyword key)
        command [[{k new} {k old}]]
        body    (json/generate-string command)
        url     (client-url node "write")]
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

;; Flesh out the op responses here
(defrecord CASClient [node]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (invoke! [this test op]
    (let [k     "/jepsen"
          crash (if (= :read (:f op)) :fail :info)
          node  (:node this)]
      (try
        (case (:f op)
          :read  (let [res (agency-read! node k)
                       val (read-parse res)]
                   (case (:status res)
                     200 (assoc op :type :ok :value val)
                     307 (assoc op :type crash :error :redirect-loop)))

          ;; FIXME Surely not all of these writes are successful
          :write (let [value (:value op)
                       ok? (agency-write! node k value)]
                   (assoc op :type (if ok? :ok :fail)))

          :cas   (let [[value value'] (:value op)
                       res (agency-cas! node k value value')]
                   (case (:status res)
                     200 (assoc op :type :ok)
                     307 (assoc op :type crash :error :redirect-loop))))

        (catch Exception e
          (cond
            (precond-fail?   e) (assoc op :type :fail :error :precondition-fail)
            (unavailable?    e) (assoc op :type crash :error :node-unavailable)
            (read-timed-out? e) (assoc op :type crash :error :read-timed-out)
            :else               (do (error (pr-str (.getMessage e)))
                                    (assoc op :type crash :error :indeterminate)))))))

  ;; HTTP clients are not stateful
  (close! [_ _])
  (setup! [this test])
  (teardown! [_ test]))

(defn client
  "A compare and set register built around a single consul node."
  [node]
  (CASClient. node))

;; TODO Add concurrency scaling
(defn arangodb-test
  [opts]
  (merge tests/noop-test
         opts
         {:name    "arangodb"
          :os      debian/os
          :db      (db "3.2.7")
          :client  (client nil)
          :nemesis (nemesis/partition-random-halves)
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis
                           (gen/seq
                            (cycle [(gen/sleep 5)
                                    {:type :info, :f :start}
                                    (gen/sleep 5)
                                    {:type :info, :f :stop}])))
                          (gen/time-limit 30))
          :model   (model/cas-register 0)
          :checker (checker/compose {:perf     (checker/perf)
                                     :timeline (timeline/html)
                                     :linear   (checker/linearizable)})}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn arangodb-test})
                   (cli/serve-cmd))
            args))
