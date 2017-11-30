(ns jepsen.arangodb
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
            [clj-http.client          :as http]
            [cheshire.core            :as json]
            [jepsen.os.debian   :as debian]
            [knossos.model      :as model]
            [base64-clj.core          :as base64]))

(defn peer-addr [node] (str (name node) ":8529"))
(defn addr [node] (str (name node) ":8529"))
(defn cluster-info [node] (str (name node) "=http://" (name node) ":8529"))

(defn deb-src [version]
  (str "https://download.arangodb.com/arangodb32/Debian_8.0/amd64/arangodb3-" version "-1_amd64.deb"))

(defn deb-dest [version] (str "arangodb3-" version "-1_amd64.deb"))

(defn db [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (info node "installing arangodb" version)
       (c/exec :apt-get :install "-y" "-qq" (str "libjemalloc1"))

       (c/exec :echo :arangodb3 "arangodb3/password" "password" "" "|" :debconf-set-selections)
       (c/exec :echo :arangodb3 "arangodb3/password_again" "password" "" "|" :debconf-set-selections)

       (c/exec :test "-f" (deb-dest version) "||"
               :wget :-q (deb-src version) :-O (deb-dest version))

       (c/exec :dpkg :-i (deb-dest version))

       (c/exec :echo (-> "arangod.conf"
                         io/resource
                         slurp
                         (str/replace "$NODE_ADDRESS" (net/local-ip)))
               :> "/etc/arangodb3/arangod.conf")

       (c/exec :service :arangodb3 :stop)
       (c/exec :rm :-rf :/var/lib/arangodb3)
       (c/exec :mkdir :/var/lib/arangodb3)
       (c/exec :chown :-R :arangodb :/var/lib/arangodb3)
       (c/exec :chgrp :-R :arangodb :/var/lib/arangodb3)
       (c/exec :service :arangodb3 :start)

       (c/exec :sleep :5)

       (info node "arangodb agency ready")))

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
        url   (str "http://" node ":8529/_api/agency/read")]
    (http/post url (assoc http-opts :body bbody))))

(defn agency-write! [node key val]
  (let [bbody (json/generate-string [[{(keyword key) val}]])
        url (str "http://" node ":8529/_api/agency/write")]
    (http/post url (assoc http-opts :body bbody))))

(defn agency-cas!
  [node key old new]
  (let [bbody (json/generate-string [[{(keyword key) new},{(keyword key) old}]])
        url (str "http://" node ":8529/_api/agency/write")]
    (http/post url (assoc http-opts :body bbody))))

(defn read-parse [resp]
  (get (first (first (json/parse-string (:body resp)))) 1))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (let [client (name node)]
      (agency-write! client k 0)
      (assoc this :client client)))

  (invoke! [this test op]
    (case (:f op)
      :read  (try
               (let [resp  (agency-read! client k)]
                 (assoc op
                        :type :ok
                        :value (read-parse resp)))
               (catch Exception e
                 ;;(warn e "r failed")
                 (assoc op :type :fail)))
      :write (try
               (let [value (:value op)
                     ok? (agency-write! client k value)]
                 (assoc op :type (if ok? :ok :fail)))
               (catch Exception e
                 ;;(warn e "w failed")
                 (assoc op :type :fail)))
      :cas   (try
               (let [[value value'] (:value op)
                     ok? (agency-cas! client k value value')]
                 (assoc op :type (if ok? :ok :fail)))
               (catch Exception e
                 ;;(warn e "cas failed")
                 (assoc op :type :fail)))))
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
