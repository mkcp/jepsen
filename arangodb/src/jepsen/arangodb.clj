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
             [util       :refer [timeout meh]]
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
(def logfile "/arangod.log")
(def pidfile "/arangod.pid")
(def port 8530)
(def agency-port 8531)

;; TODO Verify that these aren't used
(defn peer-addr [node] (str (name node) ":" port))
(defn addr      [node] (str (name node) ":" port))
(defn cluster-info [node] (str (name node) "=http://" (name node) ":" port))

(defn deb-src [version]
  (str "https://download.arangodb.com/arangodb32/Debian_8.0/amd64/arangodb3-" version "-1_amd64.deb"))

(defn deb-dest [version] (str "arangodb3-" version "-1_amd64.deb"))

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

       ;; TODO Swapping to arangodb bin
       ;; Ensure service stopped
       ;; (c/exec :service :arangodb3 :stop)

       ;; Ensure data directories are clean
       (c/exec :rm :-rf (keyword dir))
       (c/exec :mkdir (keyword dir))
       (c/exec :chown :-R :arangodb (keyword dir))
       (c/exec :chgrp :-R :arangodb (keyword dir))

       ;; TODO Swapping to arangodb bin, this might change config needs
       ;; Adjust arangod.conf and write to disk
       ;; (c/exec :echo (-> "arangod.conf"
       ;;                   io/resource
       ;;                   slurp
       ;;                   (str/replace "$NODE_COUNT" (-> test :nodes count str))
       ;;                   (str/replace "$NODE_ADDRESS" (net/local-ip)))
       ;;         :> "/etc/arangodb3/arangod.conf")

       ;; TODO Swapping to arangodb bin
       ;; Run as service
       ;; (c/exec :service :arangodb3 :start)

       (c/su
        (cu/start-daemon!
         {:chdir dir
          :logfile logfile
          :pidfile pidfile}
         binary
         (when-not (= node "n1")
           "--starter.join=n1")
         "--log.verbose=true"
         (str "--starter.data-dir=" dir)
         (str "--cluster.agency-size=" (-> test :nodes count))
         (str "--all.log.file=" logfile)))

       ;; Give cluster time to stabilize. Anything under 30s seems to risk hitting 503s
       (Thread/sleep 30000)))

    (teardown! [_ test node]
      (info node "stopping arangodb")
      (c/su
       (meh (c/exec :killall :-9 :arangodb))
       (meh (c/exec :killall :-9 :arangod)))

      (c/su
       #_(info node "Stopping service arangodb3")
       #_(c/exec :service :arangodb3 :stop)
       (info node "deleting data files")
       (c/exec :rm :-rf (keyword dir))
       (c/exec :mkdir (keyword dir))
       (c/exec :chown :-R :arangodb (keyword dir))
       (c/exec :chgrp :-R :arangodb (keyword dir))
       (c/exec :rm :-rf (keyword logfile))))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

;;;;;

(defn unavailable?    [e] (->> e .getMessage (re-find #"503") some?))
(defn precond-fail?   [e] (->> e .getMessage (re-find #"412") some?))
(defn read-timed-out? [e] (->> e .getMessage (re-find #"Read timed out") some?))
(defn already-exists? [e] (->> e .getMessage (re-find #"409") some?))
(defn not-found?      [e] (->> e .getMessage (re-find #"404") some?))

;;;;;

(defn r   [_ _] {:type :invoke :f :read  :value nil})
(defn w   [_ _] {:type :invoke :f :write :value (rand-int 5)})
(defn cas [_ _] {:type :invoke :f :cas   :value [(rand-int 5) (rand-int 5)]})

;;;;;

(def http-opts {:conn-timeout 5000
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
    (http/post url (assoc http-opts :body body))))

(defn agency-write! [node key val]
  (let [url  (agency-url node "write")
        body (json/generate-string [[{(str key) val}]])]
    (http/post url (assoc http-opts :body body))))

(defn agency-cas!
  [node key old new]
  (let [url     (agency-url node "write")
        command [[{key new} {key old}]]
        body    (json/generate-string command)]
    (http/post url (assoc http-opts :body body))))

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
    (let [[k v] (:value op)
          crash (if (= :read (:f op)) :fail :info)
          node  (:node this)]
      (try
        (case (:f op)
          :read  (let [res (agency-read! node (str "/" k))
                       v   (parse-agency-read res)]
                   (if v
                     (assoc op :type :ok   :value (independent/tuple k v))
                     (assoc op :type :fail :value (independent/tuple k v) :error :key-not-found)))

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
  (close!    [_ _])
  (setup!    [_ _])
  (teardown! [_ _]))

(defn agency-client [node] (AgencyClient. node))

;;;;;

(def doc-opts {:conn-timeout 5000
               :content-type :json
               :trace-redirects true
               :redirect-strategy :lax
               :socket-timeout 5000})

;; FIXME condense these functions
(defn doc-url
  ([node] (str "http://" node ":" port "/_api/document/jepsen/"))
  ([node k] (str "http://" node ":" port "/_api/document/jepsen/" k)))

(defn all-keys-url [node]
  (str "http://" node ":" port "/_api/simple/all-keys"))

(defn all-docs-url [node]
  (str "http://" node ":" port "/_api/simple/all"))

(defn collection-url [node]
  (str "http://" node ":" port "/_api/collection/"))

(defn admin-url [node]
  (str "http://" node ":" port "/_admin/"))

(defn doc-setup-collection! [test node]
  (try
    (let [body (json/generate-string {:name              "jepsen"
                                      :waitForSync       true
                                      ;:numberOfShards    (count (:nodes test))
                                      :replicationFactor (count (:nodes test))
                                      })]
      (http/post (collection-url node) (assoc doc-opts :body body)))
    (catch Exception e
      (when-not (already-exists? e)
        (throw e)))))

(defn doc-read! [node k]
  (let [url (doc-url node k)]
    (http/get url doc-opts)))

(defn doc-write! [node k v]
  (let [url  (doc-url node)
        body (json/generate-string [{:_key (str k) :value v}])]
    (http/put url (assoc doc-opts
                         :body body
                         :query-params {:waitForSync true}))))

;; TODO Get _rev from read, write If-Match _rev
(defn doc-cas! [node k v v']
  (let [url  (doc-url node)
        body (json/generate-string [{k v} {k v'}])]
    (http/post url (assoc doc-opts :body body))))

(defn parse-doc-read [res]
  (-> res :body json/parse-string (get "value")))

(defrecord DocumentClient [node]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (setup! [this test]
    (info "Setting up client for node" node)
    (doc-setup-collection! test node)

    ;; Print out our collection config
    #_(println (http/get (str (admin-url node) "cluster_test")))

    ;; Initialize our keyspace. Otherwise we would have to post first then put.
    ;; TODO Why in the hecky is this returning 409s. Am I not clearing the db properly?
    (try
      (doseq [i (range 0 50)]
        (http/post (doc-url node) (assoc doc-opts :body (json/generate-string {:_key (str i) :value 0}))))
      (catch Exception e
        (when-not (already-exists? e)
          (throw e)))))

  (invoke! [this test op]
    (let [[k v] (:value op)
          crash (if (= :read (:f op)) :fail :info)
          node  (:node this)]
      (try
        (case (:f op)
          :read (let [res (doc-read! node k)
                      v   (parse-doc-read res)]
                  (assoc op :type :ok :value (independent/tuple k v)))

          :write (let [res (doc-write! node k v)
                       err (-> res :headers (get "X-Arango-Error-Codes"))]
                   (if-not err
                     (assoc op :type :ok)
                     (assoc op :type crash :error (pr-str err))))

          :cas (assoc op :type :fail :error :not-implemented))

        (catch Exception e
          (cond
            (not-found? e) (assoc op :type crash :error :not-found)
            :else          (do (error (pr-str (.getMessage e)))
                               (assoc op :type crash :error :indeterminate)))))))

  ;; HTTP clients are not stateful
  (close! [_ _])
  (teardown! [_ _]))

(defn document-register-client [node] (DocumentClient. node))

(defn doc-read-all! [node]
  (let [url  (all-docs-url node)
        body (json/generate-string {:collection "jepsen"
                                    :limit 50000})]
    (http/put url (assoc doc-opts :body body))))

(defn admin-url [node]
  (str "http://" node ":" port "/_admin/"))

(defn parse-int [s] (Integer. (re-find  #"\d+" s )))

(defn parse-read-all [res]
  (let [result (-> res :body json/parse-string (get "result"))]
    (map #(Integer. (get % "_key")) result)))

(defrecord DocumentSetClient [node]
  client/Client
  (open! [this test node]
    (assoc this :node (name node)))

  (setup! [this test]
    (info "Setting up client for node" node)
    #_(warn (http/get (str (admin-url node) "cluster-test") doc-opts))
    (doc-setup-collection! test node))

  (invoke! [this test op]
    (let [crash (if (= :read (:f op)) :fail :info)]
      (try
        (case (:f op)
          :add (let [res (http/post (doc-url node)
                                    (assoc doc-opts :body (json/generate-string {:_key (str (:value op))})))]
                 (assoc op :type :ok))

          :read (let [res (doc-read-all! node)
                      v   (parse-read-all res)]
                  (warn (:body res))
                  (assoc op :type :ok :value (set v))))
        (catch Exception e
          (cond
            (not-found? e) (assoc op :type crash :error :not-found)
            :else          (do (error (pr-str (.getMessage e)))
                               (assoc op :type crash :error :indeterminate)))))))

  (close! [this test])
  (teardown! [this test]))

(defn document-set-client [node] (DocumentSetClient. node))

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

   :document-cas {:client (document-register-client nil)
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

   :document-insert {:client (document-set-client nil)
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

   ;; TODO
   :document-revision {:client (document-register-client nil)}})

(defn arangodb-test
  [opts]
  (info :opts opts)
  (let [{:keys [generator
                final-generator
                client
                checker
                model]} (get (workloads) (:workload opts))
        generator (->> generator
                       (gen/nemesis (gen/start-stop 30 15))
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
           {:name    (str "arangodb " (name (:workload opts)))
            :os      debian/os
            :db      (db "3.2.7")
            :client  client
            ;:nemesis (nemesis/partition-random-halves)
            :generator generator
            :model   model
            :checker (checker/compose
                      {:perf  (checker/perf)
                       :workload checker})})))

;;;;;

(def opt-spec
  "Additional command line options"
  [[nil "--workload WORKLOAD" "Test workload to run, e.g. agency"
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of (workloads)))
    :validate [(workloads) (cli/one-of (workloads))]]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn arangodb-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
