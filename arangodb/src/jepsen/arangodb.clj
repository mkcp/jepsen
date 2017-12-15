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

;; TODO Catch and wrap this error in "install-deb" Notify users to run without the true flag. Also probably submit a ticket
;;      that the .deb should be idempotent
; Caused by: java.lang.RuntimeException: sudo -S -u root bash -c "cd /; dpkg -i arangodb3-3.2.9-1_amd64.deb" returned non-zero exit status 1 on n1. STDOUT:
; (Reading database ... 21581 files and directories currently installed.)
; Preparing to unpack arangodb3-3.2.9-1_amd64.deb ...
; Unpacking arangodb3 (3.2.9) over (3.2.9) ...
; Setting up arangodb3 (3.2.9) ...
; Processing triggers for man-db (2.7.0.2-5) ...


; STDERR:
; invoke-rc.d: policy-rc.d denied execution of stop.
; debconf: unable to initialize frontend: Dialog
; debconf: (Dialog frontend will not work on a dumb terminal, an emacs shell buffer, or without a controlling terminal.)
; debconf: falling back to frontend: Readline
; debconf: unable to initialize frontend: Readline
; debconf: (This frontend requires a controlling tty.)
; debconf: falling back to frontend: Teletype
; invoke-rc.d: policy-rc.d denied execution of stop.
; Error while processing config file '//etc/arangodb3/arangod.conf', line #29:
; error setting value for option '--server.storage-engine': invalid value ''. possible values: "auto", "mmfiles", "rocksdb"

; FATAL ERROR: EXIT_FAILED - "exit with error"
; dpkg: error processing package arangodb3 (--install):
; subprocess installed post-installation script returned error exit status 1
; Errors were encountered while processing:
; arangodb3


(defn db [version storage-engine install-deb?]
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (info node "Installing arangodb" version)

       ;; TODO Installation non-idempotency... woo spooky! (could caused by docker? permissions?)
       ;; For some reason the /etc/arangodb3 config changes, removing auto from server.storage-engine.
       ;; This kills the dpkg (like, really kills, you can't even apt and dpkg on other things.)
       (when install-deb?
         ;; Deps
         (c/exec :apt-get :install "-y" "-qq" "libjemalloc1")

         ;; Password stuff?
         (c/exec :echo :arangodb3 "arangodb3/password" "password" "" "|" :debconf-set-selections)
         (c/exec :echo :arangodb3 "arangodb3/password_again" "password" "" "|" :debconf-set-selections)

         ;; Download & install
         (c/exec :test "-f" (deb-dest version) "||" :wget :-q (deb-src version) :-O (deb-dest version))
         (c/exec :dpkg :-i (deb-dest version)))

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

;; HTTP exception classifiers
;; TODO Lots of duplication here, can probably be simplified
(defn invalid-body?     [e] (->> e .getMessage (re-find #"400") some?))
(defn not-found?        [e] (->> e .getMessage (re-find #"404") some?))
(defn already-exists?   [e] (->> e .getMessage (re-find #"409") some?))
(defn precond-fail?     [e] (->> e .getMessage (re-find #"412") some?))
(defn internal-error?   [e] (->> e .getMessage (re-find #"500") some?))
(defn unavailable?      [e] (->> e .getMessage (re-find #"503") some?))
(defn client-timed-out? [e] (->> e .getMessage (re-find #"Read timed out") some?))

#_(defn http-errors
  "Takes a crash type for the current op, and return a mapping of its error patterns to
  crash type and error message."
  [crash]
  {"400"            [:fail :key-not-found]
   "404"            [:fail :already-exists]
   "409"            [:fail :precondition-fail]
   "412"            [:fail :invalid-body]
   "500"            [crash :server-500]
   "503"            [crash :node-unavailable]
   "Read timed out" [crash :client-timeout]})

#_(let [crash        (if (= :read (:f op)) :fail :info)
      errors       (http-errors crash)
      message      (let [matchers (map re-pattern (keys errors))]
                     (->> e .getMessage (map re-find matchers) first first))
      ;; If we don't find an error, return :indeterminate
      [type error] (get errors message [crash :indeterminate])]
  (assoc op :type type :error message))

(defmacro with-errors
  [op & body]
  `(try
     ~@body
     (catch java.net.SocketTimeoutException e#
       (let [crash# (if (= :read (:f ~op)) :fail :info)]
         (assoc ~op :type crash# :error :client-timeout)))
     (catch Exception e#
       (let [crash# (if (= :read (:f ~op)) :fail :info)]
         (cond
           (not-found?        e#) (assoc ~op :type :fail  :error :key-not-found)
           (already-exists?   e#) (assoc ~op :type :fail  :error :already-exists)
           (precond-fail?     e#) (assoc ~op :type :fail  :error :precondition-fail)
           (invalid-body?     e#) (assoc ~op :type :fail  :error :invalid-body)
           (internal-error?   e#) (assoc ~op :type crash# :error :server-500)
           (unavailable?      e#) (assoc ~op :type crash# :error :node-unavailable)
           (client-timed-out? e#) (assoc ~op :type crash# :error :client-timeout)
           :else                  (do (error (pr-str (.getMessage e#)))
                                      (assoc ~op :type crash# :error :indeterminate)))))))

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
               ;; unique inserts, so we need to figure out if its our client or a bug.
               ;; clj-http performs retries on IO exceptions, so here
               ;; we return false to make sure we do not retry in the client.
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
  might go past and run into errors, though at some point we'll hit the limit
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

(defrecord DocumentClient [node]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (setup! [this test]
    (try
      (info "Setting up client for node" node)
      (doc-setup-collection! test node)
      (init-keyspace! test node)
      (catch java.net.SocketTimeoutException _
        (warn "Timeout occurred during collection setup, continuing anyway..."))
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
                     (assoc op :type :fail :error (keyword err))
                     (assoc op :type :ok)))

          :cas (let [[v v'] v]
                 (if-let [res (doc-cas! node k v v')]
                   ;; Sometimes we get a successful http response with error headers
                   (if-let [err (-> res :headers (get "X-Arango-Error-Codes"))]
                     (assoc op :type :fail :error (keyword err))
                     (assoc op :type :ok))
                   (assoc op :type :fail)))))))

  ;; HTTP clients are not stateful
  (close! [_ _])
  (teardown! [_ _]))

(defn document-register-client [node] (DocumentClient. node))

(defn read-all! [node]
  (let [url  (str "http://" node ":" db-port "/_api/simple/all")
        body (json/generate-string {:collection "jepsen"
                                    :batchSize 100000})]
    (http/put url (assoc doc-opts :body body))))

(defn read-all->ints
  "Takes an http response from a full read and produces a sequence of Arango document keys"
  [res]
  (let [result (-> res :body json/parse-string (get "result"))]
    (map #(Integer. (get % "_key")) result)))

(defrecord DocumentSetClient [node]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (setup! [this test]
    (info "Setting up client for node" node)
    (try
      (doc-setup-collection! test node)
      (catch java.net.SocketTimeoutException _
        (warn "Timeout occurred during collection setup, continuing anyway..."))
      (catch Exception e
        (when-not (already-exists? e)
          (throw e)))))

  (invoke! [this test op]
    (with-errors op
      (case (:f op)
        :add (let [res (http/post (doc-url node)
                                  (assoc doc-opts :body (json/generate-string {:_key (str (:value op))})))]
               (swap! status-codes conj (:status res))
               (assoc op :type :ok))

        :read (let [v (-> node read-all! read-all->ints set)]
                (assoc op :type :ok :value v)))))

  (close! [this test])
  (teardown! [this test]))

(defn document-set-client [node] (DocumentSetClient. node))

(defrecord DocHTTPRevisionClient [node revisions]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (setup! [this test]
    (info "Setting up client for node" node)
    (try
      (doc-setup-collection! test node)
      ;; TODO We throw away our first revision... that's maybe not great!
      (http/post (doc-url node)
                 (assoc doc-opts :body (json/generate-string {:_key "0"})))
      (catch java.net.SocketTimeoutException _
        (warn "Timeout occurred during collection setup, continuing anyway..."))
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

(defn doc-http-revision-client [node revisions] (DocHTTPRevisionClient. node revisions))

;;;;;

(defn r   [_ _] {:type :invoke :f :read  :value nil})
(defn w   [_ _] {:type :invoke :f :write :value (rand-int 5)})
(defn cas [_ _] {:type :invoke :f :cas   :value [(rand-int 5) (rand-int 5)]})

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

   :doc-http-insert {:client (document-set-client nil)
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

   :doc-http-register {:client (document-register-client nil)
                       :generator (->> (independent/concurrent-generator
                                        10
                                        (range)
                                        (fn [k]
                                          (->> (gen/mix     [r w])
                                               (gen/stagger 1/30)
                                               (gen/limit   300)))))
                       :checker (independent/checker (checker/compose
                                                      {:timeline (timeline/html)
                                                       :linear (checker/linearizable)}))
                       :model (model/register 0)}

   :doc-http-revision {:client (doc-http-revision-client nil nil)
                       :generator {:type :invoke, :f :generate}
                       :checker (checker/unique-ids)}})

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
            :db        (db "3.2.9" "auto" (:install-deb? opts))
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
   ;; TODO Shouldn't need this. Is it a bug in the env or in the deb?
   [nil "--install-deb? true/false" "Run dpkg installation during DB step? Workaround for repeated installs failing."
    :parse-fn #{"true"}
    :missing (str "--install-deb? " false)]])

;; Example run: lein run test --concurrency 50 --workload document-rw --install-deb? false --time-limit 240

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn arangodb-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
