(ns genegraph.user
  (:require [genegraph.snapshot :as snapshot]
            [genegraph.snapshot.protocol]
            [genegraph.framework.kafka :as kafka]
            [genegraph.framework.event :as event]
            [genegraph.framework.event.store :as event-store]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.rocksdb :as rocksdb]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.storage.rdf.names :as names]
            [genegraph.snapshot.protocol :as sp]
            [genegraph.snapshot.writer :as writer]
            [genegraph.snapshot.gene-validity :as gv]
            [portal.api :as portal])
  (:import [ch.qos.logback.classic Logger Level]
           [org.slf4j LoggerFactory]
           [java.time Instant LocalDate]
           [java.util.concurrent ScheduledThreadPoolExecutor
            ExecutorService ScheduledExecutorService TimeUnit]))



(comment
  (do
    (def portal-window (portal/open))
    (add-tap #'portal/submit))
  (portal/close)
  (portal/clear))

(def root-data-dir "/Users/tristan/data/genegraph-neo/")

(defn get-events-from-topic [topic]
  ;; topic->event-file redirects stdout
  ;; need to supress kafka logs for the duration
  (.setLevel
   (LoggerFactory/getLogger Logger/ROOT_LOGGER_NAME) Level/ERROR)
  (kafka/topic->event-file
   (assoc topic
          :type :kafka-reader-topic
          :kafka-cluster snapshot/data-exchange)
   (str root-data-dir
        (:kafka-topic topic)
        "-"
        (LocalDate/now)
        ".edn.gz"))
  (.setLevel (LoggerFactory/getLogger Logger/ROOT_LOGGER_NAME) Level/INFO))


(comment
  (get-events-from-topic snapshot/gene-validity-sepio-topic)

  (event-store/with-event-reader [r (str root-data-dir "gg-gvs2-dev-1-2024-07-24.edn.gz")]
    (->> (event-store/event-seq r)
         #_(take 100)
         (run! (fn [e]
                   (p/publish (get-in test-app [:topics :gene-validity-sepio-rdf])
                              e)))
         #_(map #(-> %
                   event/deserialize
                   (assoc ::snapshot/record-type :gene-validity)
                   sp/curation-key
                   sp/version-key
                   (select-keys [::snapshot/version-key ::snapshot/curation-key])
                   #_sp/event-type-and-format))
         #_tap>))

  (event-store/with-event-reader [r (str root-data-dir "gg-gvs2-jsonld-stage-1-2024-08-14.edn.gz")]
    (->> (event-store/event-seq r)
         (take 1)
         (map event/deserialize)
         tap>))

  (storage/read @(get-in test-app [:storage :record-store :instance])
                [:gene-validity
                 ::rdf/n-triples
                 :versions
                 "http://dataexchange.clinicalgenome.org/gci/7765e2a4-19e4-4b15-9233-4847606fc501v1.0"])

  )

(comment
  (do
    (defn iri->filename [iri ext]
      (let [kw (names/iri->kw iri)]
        (str (namespace kw) "_" (name kw) "." ext)))
    (->> (rocksdb/range-get @(get-in test-app [:storage :record-store :instance])
                            {:prefix [:gene-validity ::rdf/n-triples :versions]
                             :return :ref})
         (map deref)
         (map #(assoc %
                      ::snapshot/filename
                      (iri->filename
                       (::snapshot/version-key %)
                       "nt")))
         (writer/write-snapshot {:type :file
                                 :base "/Users/tristan/Desktop/test-snapshot/"
                                 :path "test_snapshot.tar.gz"})))
  )

(def test-app-def
  {:type :genegraph-app
   :topics {:gene-validity-sepio-rdf
            {:name :gene-validity-sepio-rdf
             :type :simple-queue-topic}}
   :processors {:gene-validity-sepio-rdf-processor
                (assoc snapshot/record-processor
                       :subscribe :gene-validity-sepio-rdf
                       ::event/metadata {::snapshot/record-type :gene-validity})}
   :storage {:record-store (dissoc snapshot/record-store :snapshot-handle)}})

(comment
  (def test-app (p/init test-app-def))
  (p/start test-app)
  (p/stop test-app)

  (let [handle {:type :file
                :base "/Users/tristan/Desktop/test-snapshot/"
                :path "test_snapshot1.tar.gz"}]
    (writer/write-records {:app test-app
                           :record-type :gene-validity
                           :format ::rdf/n-triples
                           :record-set :curations
                           :storage-handle handle}))
  
  )

(comment
  (def snapshot-app (p/init snapshot/snapshot-app-def))
  (p/start snapshot-app)
  (p/stop snapshot-app)
  (get-events-from-topic (get-in snapshot/snapshot-app-def
                                 [:topics
                                  :gene-validity-json]))
  (get-events-from-topic (get-in snapshot/snapshot-app-def
                                 [:topics
                                  :gene-validity-nt]))
  (+ 1 1)
  )

(def data-root
  "/Users/tristan/data/genegraph-neo/")

(comment

  (def snapshot-test
    (p/init
     (-> snapshot/snapshot-app-def
         (update :topics assoc :gene-validity-json {:name :gene-validity-json
                                                    :type :simple-queue-topic})
         (update :topics assoc :gene-validity-nt {:name :gene-validity-nt
                                                  :type :simple-queue-topic}))))

  (p/start snapshot-test)
  (p/stop snapshot-test)

  (time
   (event-store/with-event-reader [r (str data-root "gg-gvs2-stage-1-2024-08-20.edn.gz")]
     (->> (event-store/event-seq r)
          (run! #(p/publish (get-in snapshot-test [:topics :gene-validity-nt])
                            %)))))
  (time
   (event-store/with-event-reader [r (str data-root "gg-gvs2-jsonld-stage-1-2024-08-20.edn.gz")]
     (->> (event-store/event-seq r)
          (run! #(p/publish (get-in snapshot-test [:topics :gene-validity-json])
                            %)))))

  (tap> snapshot-test)
  (let [db @(get-in snapshot-test [:storage :record-store :instance])]
    (-> (rocksdb/range-get db
                           {:prefix [:gene-validity :json :versions]
                            :return :ref})
        first
        deref
        #_tap>
        #_sp/event-type-and-format
        sp/event-filename
        #_:genegraph.snapshot/version-key
        #_(writer/iri->filename "json")))

    (let [db @(get-in snapshot-test [:storage :record-store :instance])]
      (storage/read db
                    [record-type
                     format
                     record-set
                     (:genegraph.snapshot/version-key
                      record)]))
  
  (let [handle {:type :file
                :base "/Users/tristan/data/snapshot-testing/"
                :path "test_snapshot1.tar.gz"}]
    (writer/write-records {:app snapshot-test
                           :record-type :gene-validity
                           :serialization ::rdf/n-triples
                           :record-set :curations
                           :storage-handle handle}))

  (writer/write-records {:app snapshot-test
                         :record-type :gene-validity
                         :serialization ::rdf/n-triples
                         :storage-handle {:type :file
                                          :base "/Users/tristan/Desktop/"
                                          :path "gv-nt.tar.gz"}
                         :record-set :versions})

  (event-store/with-event-reader [r (str data-root "gg-gvs2-jsonld-stage-1-2024-08-21.edn.gz")]
    (->> (event-store/event-seq r)
         (take 1)
         (mapv event/deserialize)
         tap>))
  
  (get-events-from-topic (get-in snapshot/snapshot-app-def
                                 [:topics :gene-validity-json]))
  (get-events-from-topic (get-in snapshot/snapshot-app-def
                                 [:topics :gene-validity-nt]))

  (portal/clear)
  (tap>
   (mapcat
    #(snapshot/topic->record-defs %
                                  snapshot-test
                                  {:type :file
                                   :base
                                   "/Users/tristan/data/snapshot-testing/"})
    snapshot/topics-to-snapshot))

  (tap>
   (mapcat
    #(snapshot/topic->record-defs %
                                  snapshot-app
                                  {:type :file
                                   :base
                                   "/Users/tristan/data/snapshot-testing/"})
    snapshot/topics-to-snapshot))
  (time
   (run! writer/write-records
         (mapcat
          #(snapshot/topic->record-defs %
                                        snapshot-app
                                        {:type :file
                                         :base
                                         "/Users/tristan/data/snapshot-testing/"})
          snapshot/topics-to-snapshot)))

  (snapshot/periodically-write-record-snapshots-for-app snapshot-app)
  

  (def scheduled-executor
    (ScheduledThreadPoolExecutor. 1))

  (.schedule scheduled-executor #(println "hi") 4 TimeUnit/SECONDS)

  (.scheduleAtFixedRate scheduled-executor #(println "hi") 1 4 TimeUnit/SECONDS)
  (.shutdown scheduled-executor)

  )


