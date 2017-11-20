(defproject jepsen.arangodb "0.1.0-SNAPSHOT"
  :description "Jepsen test for Arangodb"
  :url "http://www.arangodb.com/"
  :license {:name "Apache2 Public License"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :main jepsen.arangodb
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.7-SNAPSHOT"]
                 [cheshire "5.6.3"]
                 [clj-http "3.7.0"]
                 [base64-clj "0.1.1"]])
