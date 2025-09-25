(ns genegraph.snapshot
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.env :as env]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.rdf.names :as names]
            [genegraph.snapshot.names]
            [genegraph.snapshot.gene-validity]
            [genegraph.snapshot.writer :as writer]
            [genegraph.snapshot.protocol :as sp]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.log :as log]
            [io.pedestal.http :as http])
  (:import [java.time Instant ZonedDateTime]
           [java.time.temporal TemporalUnit Temporal]
           [java.util.concurrent ScheduledThreadPoolExecutor
            ExecutorService ScheduledExecutorService TimeUnit])
  (:gen-class))



(def admin-env
  (if (or (System/getenv "DX_JAAS_CONFIG_DEV")
          (System/getenv "DX_JAAS_CONFIG")) ; prevent this in cloud deployments
    {:platform "prod"
     :dataexchange-genegraph (System/getenv "DX_JAAS_CONFIG")
     :local-data-path "data/"}
    {}))

(def local-env
  (case (or (:platform admin-env) (System/getenv "GENEGRAPH_PLATFORM"))
    "local" {:fs-handle {:type :file :base "data/base/"}
             :local-data-path "data/"}
    "dev" (assoc (env/build-environment "522856288592" ["dataexchange-genegraph"])
                 :version 1
                 :name "dev"
                 :kafka-user "User:2189780"
                 :fs-handle {:type :gcs
                             :bucket "genegraph-framework-dev"}
                 :public-fs-handle {:type :gcs
                                    :bucket "genegraph-dev-public"}
                 :local-data-path "/data/")
    "stage" (assoc (env/build-environment "583560269534" ["dataexchange-genegraph"])
                   :version 1
                   :name "stage"
                   :function (System/getenv "GENEGRAPH_FUNCTION")
                   :kafka-user "User:2592237"
                   :fs-handle {:type :gcs
                               :bucket "genegraph-snapshot-stage-1"}
                   :public-fs-handle {:type :gcs
                                      :bucket "genegraph-stage-public"}
                   :local-data-path "/data/")
    "prod" (assoc (env/build-environment "974091131481" ["dataexchange-genegraph"])
                  :function (System/getenv "GENEGRAPH_FUNCTION")
                  :version 1
                  :name "prod"
                  :kafka-user "User:2592237"
                  :public-fs-handle {:type :gcs
                                     :bucket "genegraph-public"}
                  :fs-handle {:type :gcs
                              :bucket "genegraph-snapshot-prod-1"}
                  :local-data-path "/data/")
    {}))

(def env
  (merge local-env admin-env))

(defn qualified-kafka-name [prefix]
  (str prefix "-" (:name env) "-" (:version env)))

(def topics-to-snapshot
  [{:name :gene-validity-nt
    :kafka-topic "gene-validity-sepio"
    :record-type :gene-validity
    :serialization ::rdf/n-triples
    :archive-name "gene-validity-nt"}
   {:name :gene-validity-json
    :kafka-topic "gene-validity-sepio-jsonld"
    :record-type :gene-validity
    :serialization :json
    :archive-name "gene-validity-jsonld"}])

(def data-exchange
  {:type :kafka-cluster
   :kafka-user (:kafka-user env)
   :common-config {"ssl.endpoint.identification.algorithm" "https"
                   "sasl.mechanism" "PLAIN"
                   "request.timeout.ms" "20000"
                   "bootstrap.servers" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                   "retry.backoff.ms" "500"
                   "security.protocol" "SASL_SSL"
                   "sasl.jaas.config" (:dataexchange-genegraph env)}
   :consumer-config {"key.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"
                     "value.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"}
   :producer-config {"key.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"
                     "value.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"}})

(def gene-validity-sepio-topic 
  {:name :gene-validity-sepio
   :kafka-cluster :data-exchange
   :serialization ::rdf/n-triples
   :kafka-topic "gene-validity-sepio"
   :kafka-topic-config {}})

(def gene-validity-sepio-jsonld-topic 
  {:name :gene-validity-sepio
   :kafka-cluster :data-exchange
   :serialization :json
   :kafka-topic "gene-validity-sepio-jsonld"
   :kafka-topic-config {}})

(def ready-server
  {:ready-server
   {:type :http-server
    :name :ready-server
    ::http/host "0.0.0.0"
    ::http/allowed-origins {:allowed-origins (constantly true)
                            :creds true}
    ::http/routes
    [["/ready"
      :get (fn [_] {:status 200 :body "server is ready"})
      :route-name ::readiness]
     ["/live"
      :get (fn [_] {:status 200 :body "server is live"})
      :route-name ::liveness]]
    ::http/type :jetty
    ::http/port 8888
    ::http/join? false
    ::http/secure-headers nil}})

(def record-store
  {:name :record-store
   :type :rocksdb
   :snapshot-handle (assoc (:fs-handle env)
                           :path "record-store.tar.lz4")
   :path (str (:local-data-path env) "record-store")
   :reset-opts {:destroy-snapshot true}})

(def curation-key-interceptor
  (interceptor/interceptor
   {:name ::curation-key-interceptor
    :enter (fn [e] (assoc e ::curation-key (sp/curation-key e)))}))

(def version-key-interceptor
  (interceptor/interceptor
   {:name ::version-key-interceptor
    :enter (fn [e] (assoc e ::version-key (sp/version-key e)))}))

(defn write-event [{::keys [version-key curation-key record-type]
                    ::event/keys [format]
                    :as event}]
  (log/info :fn ::write-event
            :curation-key curation-key
            :version-key version-key)
  (let [data-to-store (select-keys event
                                   [::event/value
                                    ::event/format
                                    ::record-type
                                    ::version-key
                                    ::curation-key])]
    (-> event
        (event/store :record-store
                     [record-type format :versions version-key]
                     data-to-store)
        (event/store :record-store
                     [record-type format :curations curation-key]
                     (dissoc data-to-store ::event/value))))) ; don't need value twice

(def write-event-interceptor
 (interceptor/interceptor
  {:name ::write-event-interceptor
   :enter (fn [e] (write-event e))}))

;; need to append name, key metadata
(def record-processor
  {:type :processor
   :backing-store :record-store
   :interceptors [curation-key-interceptor
                  version-key-interceptor
                  write-event-interceptor]})

(def snapshot-app-def
  {:type :genegraph-app
   :kafka-clusters {:data-exchange data-exchange}
   :topics
   (reduce (fn [m t]
             (assoc m
                    (:name t)
                    (assoc t
                           :kafka-cluster :data-exchange
                           :type :kafka-reader-topic)))
           {}
           topics-to-snapshot)
   :processors
   (reduce (fn [m t]
             (let [processor-name
                   (keyword (str (name (:name t)) "-processor"))]
               (assoc m
                      processor-name
                      (assoc record-processor
                             :name processor-name
                             :subscribe (:name t)
                             ::event/metadata
                             {::record-type (:record-type t)}))))
           {}
           topics-to-snapshot)
   :storage {:record-store record-store}
   :http-servers ready-server})



(def snapshot-def
  {:type :genegraph-app
   :kafka-clusters {:data-exchange data-exchange}
   :topics {:gene-validity-sepio
            (assoc gene-validity-sepio-topic
                   :type :kafka-producer-topic)
            :gene-validity-sepio-jsonld
            (assoc gene-validity-sepio-jsonld-topic
                   :type :kafka-producer-topic)}
   :storage {:record-store record-store}
   :http-servers ready-server})

(defn store-snapshots! [app]
  (->> (:storage app)
       (map val)
       (filter :snapshot-handle)
       (run! storage/store-snapshot)))

(defn periodically-store-snapshots
  "Start a thread that will create and store snapshots for
   storage instances that need/support it. Adds a variable jitter
   so that similarly configured apps don't try to backup at the same time."
  [app period-hours run-atom]
  (let [period-ms (* 60 60 1000 period-hours)]
    (Thread/startVirtualThread
     (fn []
       (while @run-atom
         (Thread/sleep period-ms)
         (try
           (store-snapshots! app)
           (catch Exception e
             (log/error :fn ::periodically-store-snapshots
                        :exception e))))))))

(defonce record-executor
  (ScheduledThreadPoolExecutor. 1))

(def record-set->filename
  {:curations "latest"
   :versions "all"})

(defn archive-file-name [topic record-set]
  (str (:archive-name topic)
       "-"
       (record-set->filename record-set)
       ".tar.gz"))

(defn topic->record-defs [topic app storage-handle-base]
  (let [base-def (assoc (select-keys topic
                                     [:record-type
                                      :serialization])
                        :app app)]
    (mapv (fn [record-set]
            (assoc base-def
                   :record-set record-set
                   :storage-handle
                   (assoc storage-handle-base
                          :path (archive-file-name topic
                                                   record-set))))
          [:curations :versions])))

(defn periodically-write-records
  [record-def]
  (.scheduleAtFixedRate record-executor
                        #(writer/write-records record-def)
                        0
                        10
                        TimeUnit/MINUTES))

(defn periodically-write-record-snapshots-for-app [app]
  (run! periodically-write-records
        (mapcat
         #(topic->record-defs %
                              app
                              (:public-fs-handle env))
         topics-to-snapshot)))


(defn -main [& args]
  (log/info :msg "starting genegraph snapshot")
  
  (let [app (p/init snapshot-app-def)
        run-atom (atom true)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (log/info :fn ::-main
                                           :msg "stopping genegraph")
                                 (reset! run-atom false)
                                 (.shutdown record-executor)
                                 (p/stop app))))
    (p/start app)
    (periodically-write-record-snapshots-for-app app)
    (periodically-store-snapshots app 6 run-atom)))

(comment
  (-main)
  (p/reset (p/init snapshot-app-def))
  )
