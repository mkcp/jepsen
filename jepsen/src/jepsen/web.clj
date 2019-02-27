(ns jepsen.web
  "Web server frontend for browsing test results."
  (:require [jepsen.store :as store]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clojure.pprint :refer [pprint]]
            [clj-time.format :as timef]
            [hiccup.core :as h]
            [ring.util.response :as response]
            [org.httpkit.server :as server])
  (:import (java.io File
                    FileInputStream
                    PipedInputStream
                    PipedOutputStream
                    OutputStream)
           (java.nio CharBuffer)
           (java.nio.file Path
                          Files)
           (java.nio.file.attribute FileTime)
           (java.util.zip ZipEntry
                          ZipOutputStream)))

(def colors
  {:ok   "#6DB6FE"
   :info "#FFAA26"
   :fail "#FEB5DA"
   nil   "#eaeaea"})

(def valid-color
  (comp colors {true     :ok
                :unknown :info
                false    :fail}))


(defn table-css []
  ".mdc-data-table + .mdc-data-table {
  margin-top: 16px;
}

:root {
  --mdc-data-table-light-theme-bg-color: #fff;
  --mdc-data-table-dark-theme-bg-color: #303030;
  --mdc-data-table-light-theme-border-color: #e0e0e0;
  --mdc-data-table-dark-theme-border-color: #4f4f4f;
  --mdc-data-table-light-theme-row-hover: #eee;
  --mdc-data-table-dark-theme-row-hover: #414141;
  --mdc-data-table-light-theme-row-selected: #f5f5f5;
  --mdc-data-table-dark-theme-row-selected: #3a3a3a;
}
.mdc-data-table {
  box-shadow: 0 3px 1px -2px rgba(0, 0, 0, 0.2), 0 2px 2px 0 rgba(0, 0, 0, 0.14),
    0 1px 5px 0 rgba(0, 0, 0, 0.12);
  color: rgba(0, 0, 0, 0.87) !important;
  color: var(
    --mdc-theme-text-primary-on-background,
    rgba(0, 0, 0, 0.87)
  ) !important;
  -webkit-box-orient: vertical;
  -ms-flex-flow: column nowrap;
  flex-flow: column nowrap;
}
.mdc-data-table,
.mdc-data-table__header {
  display: -webkit-box;
  display: -ms-flexbox;
  display: flex;
  -webkit-box-direction: normal;
}
.mdc-data-table__header {
  -webkit-box-align: center;
  -ms-flex-align: center;
  align-items: center;
  -webkit-box-pack: justify;
  -ms-flex-pack: justify;
  justify-content: space-between;
  height: 64px;
  -webkit-box-orient: horizontal;
  -ms-flex-flow: row nowrap;
  flex-flow: row nowrap;
  padding: 0 14px 0 24px;
  -webkit-box-flex: 0;
  -ms-flex: none;
  flex: none;
}
.mdc-data-table__header-title {
  font-weight: 400;
  font-size: 20px;
  display: inline-block;
  margin: 0;
}
.mdc-data-table__header-actions {
  color: rgba(0, 0, 0, 0.54) !important;
  color: var(
    --mdc-theme-text-secondary-on-background,
    rgba(0, 0, 0, 0.54)
  ) !important;
  display: -webkit-box;
  display: -ms-flexbox;
  display: flex;
  -webkit-box-align: center;
  -ms-flex-align: center;
  align-items: center;
  -webkit-box-orient: horizontal;
  -webkit-box-direction: reverse;
  -ms-flex-flow: row-reverse nowrap;
  flex-flow: row-reverse nowrap;
}
.mdc-data-table__header-actions :nth-last-child(n + 2) {
  margin-left: 24px;
}
.mdc-data-table__content {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}
.mdc-data-table__content tr:first-child,
.mdc-data-table__content tr:nth-last-child(n + 2) {
  border-bottom: 1px solid #e0e0e0;
}
.mdc-data-table__content tr.mdc-data-table--selected {
  background-color: #f5f5f5;
}
.mdc-data-table__content td,
.mdc-data-table__content th {
  text-align: left;
  padding: 12px 24px;
  vertical-align: middle;
}
.mdc-data-table__content td.mdc-data-table--numeric,
.mdc-data-table__content th.mdc-data-table--numeric {
  text-align: right;
}
.mdc-data-table__content th {
  font-size: 13px;
  line-height: 17px;
  -webkit-user-select: none;
  -moz-user-select: none;
  -ms-user-select: none;
  user-select: none;
  color: rgba(0, 0, 0, 0.54) !important;
  color: var(
    --mdc-theme-text-secondary-on-background,
    rgba(0, 0, 0, 0.54)
  ) !important;
}
.mdc-data-table__content th.mdc-data-table--sortable {
  cursor: pointer;
}
.mdc-data-table__content th.mdc-data-table--sortable.mdc-data-table--sort-asc,
.mdc-data-table__content th.mdc-data-table--sortable.mdc-data-table--sort-desc {
  color: rgba(0, 0, 0, 0.87) !important;
  color: var(
    --mdc-theme-text-primary-on-background,
    rgba(0, 0, 0, 0.87)
  ) !important;
}
.mdc-data-table__content
  th.mdc-data-table--sortable.mdc-data-table--sort-asc:before,
.mdc-data-table__content
  th.mdc-data-table--sortable.mdc-data-table--sort-desc:before {
  font-family: Material Icons;
  font-size: 16px;
  vertical-align: text-bottom;
  line-height: 16px;
  margin-right: 8px;
}
.mdc-data-table__content
  th.mdc-data-table--sortable.mdc-data-table--sort-asc:before {
  content: 'arrow_downward';
}
.mdc-data-table__content
  th.mdc-data-table--sortable.mdc-data-table--sort-desc:before {
  content: 'arrow_upward';
}
.mdc-data-table__content td {
  font-size: 14px;
}
.mdc-data-table__content tbody tr:hover {
  background-color: #eee;
}
.mdc-data-table__footer {
  color: rgba(0, 0, 0, 0.54) !important;
  color: var(
    --mdc-theme-text-secondary-on-background,
    rgba(0, 0, 0, 0.54)
  ) !important;
  border-top: 1px solid #e0e0e0;
  display: -webkit-box;
  display: -ms-flexbox;
  display: flex;
  -ms-flex-align: center;
  height: 56px;
  -ms-flex-flow: row nowrap;
  flex-flow: row nowrap;
  padding: 0 14px 0 0;
  -webkit-box-flex: 0;
  -ms-flex: none;
  flex: none;
  font-size: 13px;
}
.mdc-data-table__footer,
.mdc-data-table__footer .mdc-data-table__per-page {
  -webkit-box-align: center;
  align-items: center;
  -webkit-box-orient: horizontal;
  -webkit-box-direction: normal;
  -webkit-box-pack: end;
  -ms-flex-pack: end;
  justify-content: flex-end;
}
.mdc-data-table__footer .mdc-data-table__per-page {
  display: -webkit-inline-box;
  display: -ms-inline-flexbox;
  display: inline-flex;
  -ms-flex-flow: row nowrap;
  flex-flow: row nowrap;
  -ms-flex-align: center;
  width: 64px;
  background-repeat: no-repeat;
  background-position: right 7px center;
  text-align: right;
  cursor: pointer;
}
.mdc-data-table__footer .mdc-data-table__per-page:after {
  font-family: Material Icons;
  font-size: 20px;
  content: 'arrow_drop_down';
  margin: 0 2px;
}
.mdc-data-table__footer .mdc-data-table__results {
  margin-left: 32px;
}
.mdc-data-table__footer .mdc-data-table__prev {
  margin-left: 32px;
  cursor: pointer;
}
.mdc-data-table__footer .mdc-data-table__next {
  margin-left: 24px;
  cursor: pointer;
}
.mdc-data-table [dir='rtl'] td,
.mdc-data-table [dir='rtl'] th,
.mdc-data-table[dir='rtl'] td,
.mdc-data-table[dir='rtl'] th,
.mdc-data-table__content[dir='rtl'] td,
.mdc-data-table__content[dir='rtl'] th {
  text-align: right;
}
.mdc-data-table [dir='rtl'] td.mdc-data-table--numeric,
.mdc-data-table [dir='rtl'] th.mdc-data-table--numeric,
.mdc-data-table[dir='rtl'] td.mdc-data-table--numeric,
.mdc-data-table[dir='rtl'] th.mdc-data-table--numeric,
.mdc-data-table__content[dir='rtl'] td.mdc-data-table--numeric,
.mdc-data-table__content[dir='rtl'] th.mdc-data-table--numeric {
  text-align: left;
}
.mdc-data-table
  [dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-asc:before,
.mdc-data-table
  [dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-desc:before,
.mdc-data-table[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-asc:before,
.mdc-data-table[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-desc:before,
.mdc-data-table__content[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-asc:before,
.mdc-data-table__content[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-desc:before {
  display: none;
}
.mdc-data-table
  [dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-asc:after,
.mdc-data-table
  [dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-desc:after,
.mdc-data-table[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-asc:after,
.mdc-data-table[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-desc:after,
.mdc-data-table__content[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-asc:after,
.mdc-data-table__content[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-desc:after {
  font-family: Material Icons;
  font-size: 16px;
  vertical-align: text-bottom;
  line-height: 16px;
  margin-left: 8px;
}
.mdc-data-table
  [dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-asc:after,
.mdc-data-table[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-asc:after,
.mdc-data-table__content[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-asc:after {
  content: 'arrow_downward';
}
.mdc-data-table
  [dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-desc:after,
.mdc-data-table[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-desc:after,
.mdc-data-table__content[dir='rtl']
  .mdc-data-table--sortable.mdc-data-table--sort-desc:after {
  content: 'arrow_upward';
}
.mdc-data-table--dark,
.mdc-theme--dark .mdc-data-table {
  color: #fff !important;
  color: var(--mdc-theme-text-primary-on-dark, #fff) !important;
  background-color: #303030;
}
.mdc-data-table--dark .mdc-data-table__header-actions,
.mdc-theme--dark .mdc-data-table .mdc-data-table__header-actions {
  color: hsla(0, 0%, 100%, 0.7) !important;
  color: var(
    --mdc-theme-text-secondary-on-dark,
    hsla(0, 0%, 100%, 0.7)
  ) !important;
}
.mdc-data-table--dark .mdc-data-table__content tr:first-child,
.mdc-data-table--dark .mdc-data-table__content tr:nth-last-child(n + 2),
.mdc-theme--dark .mdc-data-table .mdc-data-table__content tr:first-child,
.mdc-theme--dark
  .mdc-data-table
  .mdc-data-table__content
  tr:nth-last-child(n + 2) {
  border-bottom-color: #4f4f4f;
}
.mdc-data-table--dark .mdc-data-table__content tr.mdc-data-table--selected,
.mdc-theme--dark
  .mdc-data-table
  .mdc-data-table__content
  tr.mdc-data-table--selected {
  background-color: #3a3a3a;
}
.mdc-data-table--dark .mdc-data-table__content th,
.mdc-theme--dark .mdc-data-table .mdc-data-table__content th {
  color: hsla(0, 0%, 100%, 0.7) !important;
  color: var(
    --mdc-theme-text-secondary-on-dark,
    hsla(0, 0%, 100%, 0.7)
  ) !important;
}
.mdc-data-table--dark .mdc-data-table__content th.mdc-data-table--sort-asc,
.mdc-data-table--dark .mdc-data-table__content th.mdc-data-table--sort-desc,
.mdc-theme--dark
  .mdc-data-table
  .mdc-data-table__content
  th.mdc-data-table--sort-asc,
.mdc-theme--dark
  .mdc-data-table
  .mdc-data-table__content
  th.mdc-data-table--sort-desc {
  color: #fff !important;
  color: var(--mdc-theme-text-primary-on-dark, #fff) !important;
}
.mdc-data-table--dark .mdc-data-table__content tbody tr:hover,
.mdc-theme--dark .mdc-data-table .mdc-data-table__content tbody tr:hover {
  background-color: #414141;
}
.mdc-data-table--dark .mdc-data-table__footer,
.mdc-theme--dark .mdc-data-table .mdc-data-table__footer {
  color: hsla(0, 0%, 100%, 0.7) !important;
  color: var(
    --mdc-theme-text-secondary-on-dark,
    hsla(0, 0%, 100%, 0.7)
  ) !important;
  border-top-color: #4f4f4f;
}")

; Path/File protocols
(extend-protocol io/Coercions
  Path
  (as-file [p] (.toFile p))
  (as-url [p] (.toURL (.toURI p))))

(defn url-encode-path-components
  "URL encodes *individual components* of a path, leaving / as / instead of
  encoded."
  [x]
  (str/replace (java.net.URLEncoder/encode x) #"%2F" "/"))

(defn fast-tests
  "Abbreviated set of tests"
  []
  (->> (store/tests)
       (mapcat (fn [[test-name runs]]
                 (keep (fn [[test-time full-test]]
                         (try
                           {:name        test-name
                            :start-time  test-time
                            :results     (store/memoized-load-results test-name test-time)}
                           (catch java.io.FileNotFoundException e
                             ; Incomplete test
                             {:name       test-name
                              :start-time test-time
                              :results    {:valid? :incomplete}})
                           (catch java.lang.RuntimeException e
                             ; Um???
                             (warn e "Unable to parse" test-name test-time)
                             {:name       test-name
                              :start-time test-time
                              :results    {:valid? :incomplete}})))
                       runs)))))

(defn relative-path
  "Relative path, as a Path."
  [base target]
  (let [base   (.toPath (io/file base))
        target (.toPath (io/file target))]
    (.relativize base target)))

(defn url
  "Takes a test and filename components; returns a URL for that file."
  [t & args]
  (url-encode-path-components
    (str "/files/" (-> (apply store/path t args)
                       .getPath
                       (str/replace #"\Astore/" "")
                       (str/replace #"/\Z" "")))))

(defn file-url
  "URL for a File"
  [f]
  (url-encode-path-components
    (str "/files/" (->> f io/file .toPath (relative-path store/base-dir)))))

(defn test-row
  "Turns a test map into a table row."
  [t]
  (let [r    (:results t)
        time (->> t
                  :start-time
                  (timef/parse   (timef/formatters :basic-date-time))
                  (timef/unparse (timef/formatters :date-hour-minute-second)))]
    [:tr
     [:td [:a {:href (url t "")} (:name t)]]
     [:td [:a {:href (url t "")} time]]
     [:td {:style (str "background: " (valid-color (:valid? r)))}
      (:valid? r)]
     [:td [:a {:href (url t "results.edn")}    "results.edn"]]
     [:td [:a {:href (url t "history.txt")}    "history.txt"]]
     [:td [:a {:href (url t "jepsen.log")}     "jepsen.log"]]
     [:td [:a {:href (str (url t) ".zip")} "zip"]]]))

(defn home
  "Home page"
  [req]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body (let [tests (fast-tests)
               sorted (->> tests
                           (sort-by :start-time)
                           reverse)]
           (h/html
            ;; TODO Valid/error/unknown - counts/ratios/percents
            ;; TODO Get sort by from drop-down & url data
            ;; TODO Search/Filter by, tags?
            ;; TODO Add/remove from archive and delete archived tests
            [:head
             [:link {:rel "stylesheet"
                     :type "text/css"
                     :href "https://unpkg.com/material-components-web@latest/dist/material-components-web.min.css"}]
             [:link {:href "https://fonts.googleapis.com/icon?family=Material+Icons" :rel "stylesheet"}]
             [:script {:src "https://unpkg.com/material-components-web@latest/dist/material-components-web.min.js"}]]

            [:body
             {:style (table-css)}
             [:h3.mdc-typography--headline3
              "Jepsen Reports"]

             [:p
              "Tests in store: " (count tests)]

             [:div.mdc-typography
              [:div.mdc-data-table
               [:table.mdc-data-table__content
                [:thead
                 [:tr
                  [:th.mdc-data-table--sortable "Name"]
                  [:th.mdc-data-table--sortable "Time"]
                  [:th.mdc-data-table--sortable "Valid?"]
                  [:th "Results"]
                  [:th "History"]
                  [:th "Log"]
                  [:th "Zip"]]]
                [:tbody (map test-row sorted)]]]]]))})

(defn dir-cell
  "Renders a File (a directory) for a directory view."
  [^File f]
  (let [results-file  (io/file f "results.edn")
        valid?        (try (with-open [r (java.io.PushbackReader.
                                           (io/reader results-file))]
                             (:valid? (clojure.edn/read r)))
                           (catch java.io.FileNotFoundException e
                             nil)
                           (catch RuntimeException e
                             :unknown))]
  [:a {:href (file-url f)
       :style "text-decoration: none;
              color: #000;"}
   [:div {:style (str "background: " (valid-color valid?) ";\n"
                      "display: inline-block;
                      margin: 10px;
                      padding: 10px;
                      overflow: hidden;
                      width: 280px;")}
    (.getName f)]]))

(defn file-cell
  "Renders a File for a directory view."
  [^File f]
  [:div {:style "display: inline-block;
                margin: 10px;
                overflow: hidden;"}
   [:div {:style "height: 200px;
                  width: 300px;
                  overflow: hidden;"}
    [:a {:href (file-url f)
         :style "text-decoration: none;
                 color: #555;"}
     (cond
       (re-find #"\.(png|jpg|jpeg|gif)$" (.getName f))
       [:img {:src (file-url f)
              :title (.getName f)
              :style "width: auto;
                     height: 200px;"}]

       (re-find #"\.(txt|edn|json|yaml|log|stdout|stderr)$" (.getName f))
       [:pre
        (with-open [r (io/reader f)]
          (let [buf (CharBuffer/allocate 4096)]
            (.read r buf)
            (.flip buf)
            (.toString buf)))]

       true
       [:div {:style "background: #F4F4F4;
                     width: 100%;
                     height: 100%;"}])]]

   [:a {:href (file-url f)} (.getName f)]])

(defn dir-sort
  "Sort a collection of Files. If everything's an integer, sort numerically,
  else alphanumerically."
  [files]
  (if (every? (fn [^File f] (re-find #"^\d+$" (.getName f))) files)
    (sort-by #(Long/parseLong (.getName %)) files)
    (sort files)))

(defn dir
  "Serves a directory."
  [^File dir]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body (h/html (->> dir
                      (.toPath)
                      (iterate #(.getParent %))
                      (take-while #(-> store/base-dir
                                       io/file
                                       .getCanonicalFile
                                       .toPath
                                       (not= (.toAbsolutePath %))))
                      (drop 1)
                      reverse
                      (map (fn [^Path component]
                             [:a {:href (file-url component)}
                                    (.getFileName component)]))
                      (cons [:a {:href "/"} "jepsen"])
                      (interpose " / "))
                 [:h1 (.getName dir)]
                 [:div
                  (->> dir
                       .listFiles
                       (filter (fn [^File f] (.isDirectory f)))
                       dir-sort
                       (map dir-cell))]
                 [:div
                  (->> dir
                       .listFiles
                       (remove (fn [^File f] (.isDirectory f)))
                       (sort-by (fn [^File f]
                                  [(not= (.getName f) "results.edn")
                                   (not= (.getName f) "history.txt")
                                   (.getName f)]))
                       (map file-cell))])})

(defn zip-path!
  "Writes a path to a zipoutputstream"
  [^ZipOutputStream zipper base ^File file]
  (when (.isFile file)
    (let [relpath (str (relative-path base file))
          entry   (doto (ZipEntry. relpath)
                    (.setCreationTime (FileTime/fromMillis
                                        (.lastModified file))))
          buf   (byte-array 16384)]
      (with-open [input (FileInputStream. file)]
        (.putNextEntry zipper entry)
        (loop []
          (let [read (.read input buf)]
            (if (< 0 read)
              (do (.write zipper buf 0 read)
                  (recur))
              (.closeEntry zipper)))))))
  zipper)

(defn zip
  "Serves a directory as a zip file. Strips .zip off the extension."
  [req ^File dir]
  (let [f (-> dir
              (.getCanonicalFile)
              str
              (str/replace #"\.zip\z" "")
              io/file)
        pipe-in (PipedInputStream. 16384)
        pipe-out (PipedOutputStream. pipe-in)]
    (future
      (try
        (with-open [zipper (ZipOutputStream. pipe-out)]
          (doseq [file (file-seq f)]
            (zip-path! zipper f file)))
        (catch Exception e
          (warn e "Error streaming zip for" dir))
        (finally
          (.close pipe-out))))
    {:status  200
     :headers {"Content-Type" "application/zip"}
     :body    pipe-in}))

(defn assert-file-in-scope!
  "Throws if the given file is outside our store directory."
  [^File f]
  (assert (.startsWith (.toPath (.getCanonicalFile f))
                       (.toPath (.getCanonicalFile (io/file store/base-dir))))
          "File out of scope."))

(def e404
  {:status 404
   :headers {"Content-Type" "text/plain"}
   :body "404 not found"})

(def content-type
  "Map of extensions to known content-types"
  {"txt"  "text/plain"
   "log"  "text/plain"
   "edn"  "text/plain" ; Wrong, but we like seeing it in-browser
   "json" "text/plain" ; Ditto
   "html" "text/html"
   "svg"  "image/svg+xml"})

(defn files
  "Serve requests for /files/ urls"
  [req]
  (let [pathname ((re-find #"^/files/(.+)\z" (:uri req)) 1)
        ext      (when-let [m (re-find #"\.(\w+)\z" pathname)] (m 1))
        f    (File. store/base-dir pathname)]
    (assert-file-in-scope! f)
    (cond
      (.isFile f)
      (let [res (response/file-response pathname
                                        {:root             store/base-dir
                                         :index-files?     false
                                         :allow-symlinks?  false})]
          (if-let [ct (content-type ext)]
            (-> res
                (response/content-type ct)
                (response/charset "utf-8"))
            res))

      (= ext "zip")
      (zip req f)

      (.isDirectory f)
      (dir f)

      true
      e404)))

(defn app [req]
;  (info :req (with-out-str (pprint req)))
  (let [req (assoc req :uri (java.net.URLDecoder/decode (:uri req) "UTF-8"))]
    (condp re-find (:uri req)
      #"^/$"     (home req)
      #"^/files/" (files req)
      e404)))

(defn serve!
  "Starts an http server with the given httpkit options."
  ([options]
   (let [s (server/run-server app options)]
     (info "I'll see YOU after the function")
     s)))
