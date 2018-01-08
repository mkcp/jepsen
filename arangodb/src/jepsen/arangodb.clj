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
             [util       :refer [timeout meh pprint-str]]
             [cli :as cli]]
            [jepsen.control.net :as net]
            [jepsen.control.util :as cu]
            [jepsen.checker.timeline :as timeline]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [base64-clj.core :as base64]
            [jepsen.core :as jepsen]))

(def dir "/var/lib/arangodb3")
(def binary "/usr/bin/arangodb")
(def log-file "/arangod.log")
(def pid-file "/arangod.pid")

(def db-port 8529) ;; coordinator
(def dbserver-port 8530)
(def agency-port 8531)

(def status-codes (atom #{}))

(defn deb-src [version]
  (str "https://download.arangodb.com/arangodb32/Debian_8.0/amd64/arangodb3-" version "-1_amd64.deb"))

(defn deb-dest [version] (str "arangodb3-" version "-1_amd64.deb"))

(defn db [version storage-engine]
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (try
           (info node "Installing arangodb" version)
           ;; Deps
           (c/exec :apt-get :install "-y" "-qq" "libjemalloc1")

           ;; Password stuff?
           (c/exec :echo :arangodb3 "arangodb3/password" "password" "" "|" :debconf-set-selections)
           (c/exec :echo :arangodb3 "arangodb3/password_again" "password" "" "|" :debconf-set-selections)

           ;; Download & install
           (c/exec :test "-f" (deb-dest version) "||" :wget :-q (deb-src version) :-O (deb-dest version))
           (c/exec :dpkg :-i (deb-dest version))

           ;; On a second installation, /etc/arangodb3 config changes, removing the value "auto"
           ;; from server.storage-engine in config. _This kills the dpkg_ We don't have to install again
           ;; here so we continue with the test. I don't believe this is causing any problems, but it's definitely
           ;; suspect. Debs should be idempotent... right?
           (catch RuntimeException e
             (if (re-find #"Error while processing config file" (.getMessage e))
               (warn "A dpkg error occurred during setup, continuing anyway! See DB setup docs for more info")
               (throw e))))

       ;; Ensure data directories are clean
       (c/exec :rm :-rf (keyword dir))
       (c/exec :mkdir (keyword dir))
       (c/exec :chown :-R :arangodb (keyword dir))
       (c/exec :chgrp :-R :arangodb (keyword dir))

       (c/su
        (cu/start-daemon!
         {:chdir dir
          :logfile log-file
          :pidfile pid-file}
         binary
         (when-not (= node "n1")
           "--starter.join=n1")
         "--log.verbose=true"
         (str "--server.storage-engine=" storage-engine)
         (str "--cluster.agency-size=" (-> test :nodes count))
         (str "--all.log.file=" log-file)))

       ;; Give cluster time to stabilize.
       ;; Anything under 30s seems to risk hitting 503s in the agency test.
       ;; Anything under a minute risks 503s during setup in doc tests.
       ;; Going with 90 to be safe
       (Thread/sleep 90000)))

    (teardown! [_ test node]
      (c/su
       (info node "stopping arangodb")
       (meh (c/exec :killall :-9 :arangodb))
       (meh (c/exec :killall :-9 :arangod))

       (info node "deleting data files")
       (c/exec :rm :-rf (keyword dir))
       (c/exec :mkdir (keyword dir))
       (c/exec :chown :-R :arangodb (keyword dir))
       (c/exec :chgrp :-R :arangodb (keyword dir))
       (c/exec :rm :-rf (keyword log-file))))

    db/LogFiles
    (log-files [_ test node]
      [log-file])))

;;;;;

;; For Multi
#_(defn read-only?
  "Is a transaction a read-only transaction?"
  [txn]
  (every? #{:read} (map first txn)))

#_(let [type (if (read-only? (val (:value op))) :fail :info)]
    (condp re-find (.getMessage e#)
      #"400"       (assoc ~op :type crash# :error :invalid-body)
      #"404"       (assoc ~op :type crash# :error :key-not-found)
      #"409"       (assoc ~op :type crash# :error :key-already-exists)
      #"412"       (assoc ~op :type crash# :error :precondition-failed)
      #"500"       (assoc ~op :type crash# :error :server-500)
      #"503"       (assoc ~op :type crash# :error :node-unavailable)
      #"timed out" (assoc ~op :type crash# :error :timeout)
      (throw e#)))

;;;;;

(defn already-exists? [e] (->> e .getMessage (re-find #"409") some?))

(defmacro with-errors
  [op & body]
  `(try
     ~@body
     (catch Exception e#
       (let [crash# (if (= :read (:f ~op)) :fail :info)]
         (condp re-find (.getMessage e#)
           #"400"       (assoc ~op :type crash# :error :invalid-body)
           #"404"       (assoc ~op :type crash# :error :key-not-found)
           #"409"       (assoc ~op :type crash# :error :key-already-exists)
           #"412"       (assoc ~op :type crash# :error :precondition-failed)
           #"500"       (assoc ~op :type crash# :error :server-500)
           #"503"       (assoc ~op :type crash# :error :node-unavailable)
           #"timed out" (assoc ~op :type crash# :error :timeout)
           (throw e#))))))

;;;;;

(def agency-opts {:conn-timeout 5000
                  :content-type :json
                  :trace-redirects true
                  :redirect-strategy :lax
                  :socket-timeout 5000})

(defn agency-url
  ([node]   (agency-url node nil))
  ([node f] (str "http://" node ":" agency-port "/_api/agency/" f)))

(defn agency-read! [node key]
  (let [url  (agency-url node "read")
        body (json/generate-string [[key]])]
    (http/post url (assoc agency-opts :body body))))

(defn agency-write! [node key val]
  (let [url  (agency-url node "write")
        body (json/generate-string [[{(str key) val}]])]
    (http/post url (assoc agency-opts :body body))))

(defn agency-cas!
  [node key old new]
  (let [url     (agency-url node "write")
        command [[{key new} {key old}]]
        body    (json/generate-string command)]
    (http/post url (assoc agency-opts :body body))))

(defn parse-agency-read [resp]
  (-> resp
      :body
      json/parse-string
      first
      first
      (get 1)))

(defrecord AgencyClient [node]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (invoke! [this test op]
    (with-errors op
      (let [[k v] (:value op)]
        (case (:f op)
          :read  (let [res (agency-read! node (str "/" k))
                       v   (parse-agency-read res)]
                   (if v
                     (assoc op :type :ok   :value (independent/tuple k v))
                     (assoc op
                            :type :fail
                            :value (independent/tuple k v)
                            :error :key-not-found)))

          :write (let [res (agency-write! node (str "/" k) v)]
                   (assoc op :type :ok))

          :cas   (let [[v v'] v
                       res    (agency-cas! node (str "/" k) v v')]
                   (assoc op :type :ok))))))

  ;; HTTP clients are not stateful
  (close!    [_ _])
  (setup!    [_ _])
  (teardown! [_ _]))

(defn agency-client [node] (AgencyClient. node))

;;;;;

(def doc-opts {:conn-timeout      30000
               :content-type      :json
               :trace-redirects   true
               :redirect-strategy :lax
               :socket-timeout    30000
               ;; We occasionally get "key exists" type errors when performing
               ;; unique inserts, here we disable retries client-side to ensure
               ;; these error aren't caused by our client retrying IO exceptions
               :retry-handler (fn [_ _ _] false)})

(defn doc-url
  ([node] (str "http://" node ":" db-port "/_api/document/jepsen/"))
  ([node k] (str "http://" node ":" db-port "/_api/document/jepsen/" k)))

(defn doc-setup-collection! [test node]
  (let [url (str "http://" node ":" db-port "/_api/collection/")
        body (json/generate-string {:name              "jepsen"
                                    :waitForSync       true
                                    ;:numberOfShards    (count (:nodes test))
                                    :replicationFactor (count (:nodes test))})]
    (http/post url (assoc doc-opts :body body))))

(defn init-keyspace!
  "Creates keys with value 0 in our collection. The number of keys is 10 times
  the concurrency val on the test. This isn't perfect because very long tests
  might go past and run into errors... but on the other end it's limited
  at some point we'll hit the limit
  of our linearizability checker. May need to be tuned further."
  [test node]
  (let [max (* 10 (:concurrency test))
        t   (mapv (fn [i] {:_key (str i) :value 0})
                  (range 0 max))
        body (json/generate-string t)]
    (http/post (doc-url node) (assoc doc-opts :body body))))

(defn parse-doc-read [res]
  (-> res :body json/parse-string (get "value")))

(defn doc-read! [node k]
  (let [url (doc-url node k)]
    (http/get url doc-opts)))

(defn doc-write! [node k v]
  (let [url  (doc-url node k)
        body (json/generate-string {:value v})]
    (http/put url (assoc doc-opts :body body))))

(defn doc-cas!
  "Reads the current value of the register and writes if both the current value
  matches and the revision has not changed since the read."
  [node k v v']
  (let [read-res (doc-read! node k)]
    (when (= v (parse-doc-read read-res))
      (let [rev  (-> read-res :body json/parse-string (get "_rev"))
            url  (doc-url node k)
            body (json/generate-string {:value v'})]
        (http/put url (assoc doc-opts
                             :body body
                             :headers {"If-Match" rev}))))))

(defn read-all! [node]
  (let [url  (str "http://" node ":" db-port "/_api/simple/all")
        body (json/generate-string {:collection "jepsen"
                                    :batchSize 1000000})]
    (http/put url (assoc doc-opts :body body))))

(defn read-all->ints
  "Takes an http response from a full read and produces a sequence of Arango document keys"
  [res]
  (let [result (-> res :body json/parse-string (get "result"))]
    (map #(Integer. (get % "_key")) result)))

;;;;;

(defrecord DocSetClient [node]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (setup! [this test]
    (try
      (doc-setup-collection! test node)
      (catch java.net.SocketTimeoutException _
        (warn "Timeout during collection setup, continuing anyway..."))
      (catch Exception e
        (when-not (already-exists? e)
          (throw e)))))

  (invoke! [this test op]
    (with-errors op
      (case (:f op)
        :add (let [res (http/post (doc-url node)
                                  (assoc doc-opts :body (json/generate-string {:_key (str (:value op))})))]
               (if-let [err (-> res :headers (get "X-Arango-Error-Codes"))]
                 ;; Sometimes we get a successful http response with error headers
                 (assoc op :type :info :error (keyword err))
                 (assoc op :type :ok)))
        :read (let [v (-> node read-all! read-all->ints set)]
                (assoc op :type :ok :value v)))))

  (close! [this test])
  (teardown! [this test]))

(defn doc-set-client [node] (DocSetClient. node))

;;;;;

(defrecord DocRevisionClient [node revisions]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (setup! [this test]
    (info "Setting up client for node" node)
    (try
      (doc-setup-collection! test node)
      ;; TODO We throw away our first revision... could be missing a collision!
      (http/post (doc-url node)
                 (assoc doc-opts :body (json/generate-string {:_key "0"})))
      (catch java.net.SocketTimeoutException _
        (warn "Timeout during collection setup, continuing anyway..."))
      (catch Exception e
        (when-not (already-exists? e)
          (throw e)))))

  (invoke! [this test op]
    (with-errors op
      (assert (= (:f op) :generate))
      (let [body (json/generate-string {:value "0"})
            res  (http/put (doc-url node "0") (assoc doc-opts :body body))
            rev  (-> res :body json/parse-string (get "_rev"))]
        (assoc op :type :ok :value rev))))

  (close! [this test])
  (teardown! [this test]))

(defn doc-revision-client [node revisions] (DocRevisionClient. node revisions))

;;;;;

(defrecord DocRegisterClient [node]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (setup! [this test]
    (try
      (info "Setting up client for node" node)
      (doc-setup-collection! test node)
      (init-keyspace! test node)
      (catch java.net.SocketTimeoutException _
        (warn "Timeout during collection setup, continuing anyway..."))
      (catch Exception e
        (when-not (already-exists? e)
          (throw e)))))

  (invoke! [this test op]
    (with-errors op
      (let [[k v] (:value op)]
        (case (:f op)
          :read (let [res (doc-read! node k)
                      v   (parse-doc-read res)]
                  (assoc op :type :ok :value (independent/tuple k v)))

          :write (let [res (doc-write! node k v)]
                   ;; Sometimes we get a successful http response with error headers
                   (if-let [err (-> res :headers (get "X-Arango-Error-Codes"))]
                     (assoc op :type :info :error (keyword err))
                     (assoc op :type :ok)))

          :cas (let [[v v'] v]
                 (if-let [res (doc-cas! node k v v')]
                   ;; Sometimes we get a successful http response with error headers
                   (if-let [err (-> res :headers (get "X-Arango-Error-Codes"))]
                     (assoc op :type :info :error (keyword err))
                     (assoc op :type :ok))
                   (assoc op :type :fail)))))))

  ;; HTTP clients are not stateful
  (close! [_ _])
  (teardown! [_ _]))

(defn doc-register-client [node] (DocRegisterClient. node))

;;;;;

(defrecord DocMultiClient [node ks]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (setup! [this test]
    (info "Setting up client for node" node)
    (doc-setup-collection! test node)
    (doseq [k ks]
      (http/post (doc-url node k)
                 (assoc doc-opts :body (json/generate-string {:value 0}))))
    (info "initial state populated"))

  (invoke! [this test op]
    (with-errors op
      (let [body (json/generate-string {:value "0"})
            res  (http/put (doc-url node "0") (assoc doc-opts :body body))
            rev  (-> res :body json/parse-string (get "_rev"))]
        (assoc op :type :ok :value rev))))

  (close! [this test])
  (teardown! [this test]))

(defn doc-multi-client [node ks] (DocMultiClient. node ks))

;;;;;

;; Single
(defn r   [_ _] {:type :invoke :f :read  :value nil})
(defn w   [_ _] {:type :invoke :f :write :value (rand-int 5)})
(defn cas [_ _] {:type :invoke :f :cas   :value [(rand-int 5) (rand-int 5)]})

;; Multi (from https://github.com/jepsen-io/voltdb/blob/master/src/jepsen/voltdb/multi.clj)
(defn op [k]
  (if (< (rand) 0.5)
    [:write k (rand-int 3)]
    [:read  k nil]))

(defn op-with-read
  "Like op, but yields sequences of transactions, prepending reads to writes."
  [k]
  (let [[f k v :as op] (op k)]
    (if (= f :read)
      [op]
      [[:read k nil] op])))

(defn txn
  "A transaction is a sequence of [type k v] tuples, e.g. [[:read 0 3], [:write 1 2]]. For
  grins, we always perform a read before a write. Yields a generator of transactions over
  key-count registers."
  [ks]
  (let [ks (take (inc (rand-int (count ks))) (shuffle ks))]
    (vec (mapcat op-with-read ks))))

(defn txn-gen
  "A generator of transactions on ks"
  [ks]
  (fn [_ _] {:type :invoke :f :txn :value (txn ks)}))

(defn read-only-txn-gen
  "Generator for read-only transactions"
  [ks]
  (fn [_ _]
    {:type :invoke
     :f    :txn
     :value (mapv (fn [k] [:read k nil]) ks)}))

;;;;;

(defn workloads
  "The workloads we can run. Each workload is a map like

      {:generator         a generator of client ops
       :final-generator   a generator to run after the cluster recovers
       :client            a client to execute those ops
       :checker           a checker
       :model             for the checker}

  Note that workloads are *stateful*, since they include generators; that's why
  this is a function, instead of a constant--we may need a fresh workload if we
  run more than one test."
  []
  {:agency {:client (agency-client nil)
            :generator (->> (independent/concurrent-generator
                             10
                             (range)
                             (fn [k]
                               (->> (gen/mix     [r w cas])
                                    (gen/stagger 1/30)
                                    (gen/limit   300)))))
            :checker (independent/checker (checker/compose
                                           {:timeline (timeline/html)
                                            :linear (checker/linearizable)}))
            :model (model/cas-register)}

   :doc-revision {:client (doc-revision-client nil nil)
                  :generator {:type :invoke, :f :generate}
                  :checker (checker/unique-ids)}

   :doc-insert {:client (doc-set-client nil)
                :generator (->> (range)
                                (map (fn [x] {:type :invoke
                                              :f    :add
                                              :value x}))
                                gen/seq
                                (gen/stagger 1/10))
                :final-generator (->> {:type :invoke, :f :read}
                                      gen/once
                                      gen/each)
                :checker (checker/set)
                :model (model/set)}

   :doc-register {:client (doc-register-client nil)
                  :generator (->> (independent/concurrent-generator
                                   10
                                   (range)
                                   (fn [k]
                                     (->> (gen/mix     [r w cas])
                                          (gen/stagger 1/30)
                                          (gen/limit   300)))))
                  :checker (independent/checker (checker/compose
                                                 {:timeline (timeline/html)
                                                  :linear (checker/linearizable)}))
                  :model (model/cas-register 0)}

   :doc-multi (let [ks [:x :y]]
                {:client (doc-multi-client nil ks)
                 :checker (checker/compose
                           {:perf (checker/perf)
                            :timeline (independent/checker (timeline/html))
                            :linear (independent/checker (checker/linearizable))})
                 :model (model/multi-register (zipmap ks (repeat 0)))
                 :generator (->> (independent/concurrent-generator
                                  10
                                  (range)
                                  (fn [id]
                                    (->> (txn-gen ks)
                                         (gen/stagger 5)
                                         (gen/reserve 5 (read-only-txn-gen ks))
                                         (gen/stagger 1)))))})})

(defn arangodb-test
  [opts]
  (info :opts opts)
  (let [{:keys [generator
                final-generator
                client
                checker
                model]} (get (workloads) (:workload opts))
        generator (->> generator
                       (gen/nemesis (gen/start-stop 30 20)) ;; Alternative hits: 20 10
                       (gen/time-limit (:time-limit opts)))
        generator (if-not final-generator
                    generator
                    (gen/phases generator
                                (gen/log "Healing cluster")
                                (gen/nemesis
                                 (gen/once {:type :info, :f :stop}))
                                (gen/log "Waiting for quiescence")
                                (gen/sleep 30)
                                (gen/clients final-generator)))]
    (merge tests/noop-test
           opts
           {:name      (str "arangodb " (name (:workload opts)))
            :os        debian/os
            :db        (db "3.2.9" (:storage-engine opts))
            :client    client
            :nemesis   (nemesis/partition-random-halves)
            :generator generator
            :model     model
            :checker   (checker/compose
                        {:perf  (checker/perf)
                         :workload checker})})))

;;;;;

(def opt-spec
  "Additional command line options"
  [[nil "--workload WORKLOAD" "Test workload to run, e.g. agency"
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of (workloads)))
    :validate [(workloads) (cli/one-of (workloads))]]
   [nil "--storage-engine ENGINE" "Storage engine to use: [auto, mmfiles, rocksdb] Default: auto"
    :parse-fn #(str %)
    :default "auto"]])

;; Example run: lein run test --concurrency 50 --workload document-rw --time-limit 240

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn arangodb-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
