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
  ([node f] (str "http://n1:" port "/_api/agency/" f)))

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

       ;; Run as daemon
       #_(cu/start-daemon!
        {:pidfile pidfile
        ;:logfile logfile
         ;:chdir   dir
         }
        binary
        :--database.directory           dir
        :--server.endpoint              (str "tcp://0.0.0.0:" port)
        :--server.authentication        false
        :--server.statistics            true
        :--server.uid                   "arangodb"
        :--javascript.startup-directory "usr/share/arangodb3/js"
        :--javascript.app-path          "/var/lib/arangodb3-apps"
        :--foxx.queues                  true
        :--log.level                    "info"
        :--log.file                     logfile
        :--agency.activate              true
        :--agency.size                  5
        :--agency.endpoint              (str "tcp://" (-> test :nodes first name) port)
        :--agency.wait-for-sync         false
        :--agency.election-timeout-max  ".5"
        :--agency.election-timeout-min  ".15"
        :--agency.my-address            (str "tcp://" (net/local-ip) ":" port))

       ;; Run as service
       (c/exec :echo (-> "arangod.conf"
                         io/resource
                         slurp
                         (str/replace "$NODE_ADDRESS" (net/local-ip)))
               :> "/etc/arangodb3/arangod.conf")
       (c/exec :service :arangodb3 :start)

       (Thread/sleep 5000)))

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
  (let [k       (str key)
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

(defn unavailable? [e]
  (->> e
       .getMessage
       (re-find #"503")
       some?))

;; Flesh out the op responses here
(defrecord CASClient [k node]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (setup! [this test]
    ;; Create our key
    (try
      (agency-write! this k 0)
      (catch Exception e
        (info "Failed to create initial key"))))

  (invoke! [this test op]
    (try
      (let [node (:node this)]
        (case (:f op)
          :read  (let [resp (agency-read! node k)]
                   ;; FIXME Tons of :ok reads that are nil wtf
                   (assoc op
                          :type :ok
                          :value (read-parse resp)))

          ;; FIXME Surely not all of these writes are successful
          :write (let [value (:value op)
                       ok? (agency-write! node k value)]
                   (assoc op :type (if ok? :ok :fail)))

          :cas   (let [[value value'] (:value op)
                       res (agency-cas! node k value value')]
                   (when-not (:body res) "No body found in cas response!")
                   (case (:status res)
                     200 (assoc op :type :ok)
                     412 (assoc op :type :fail)
                     307 (assoc op :type :info :error :307-indeterminate)))))

      (catch Exception e
        (cond
          (unavailable? e) (do
                            (error (pr-str (.getMessage e)))
                            (assoc op :type :fail))
          :else            (do
                             (error (pr-str (.getMessage e)))
                             (assoc op
                                  :type  :info
                                  :error :indeterminate))))))

  ;; Client is http, connections are not stateful
  (close! [_ _])
  (teardown! [_ test]))

(defn client
  "A compare and set register built around a single consul node."
  []
  (CASClient. "/jepsen" 0))

;; TODO Add concurrency scaling
(defn arangodb-test
  [opts]
  (merge tests/noop-test
         opts
         {:name    "arangodb"
          :os      debian/os
          :db      (db "3.2.7")
          :client  (client)
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
