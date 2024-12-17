(ns genegraph.snapshot.writer
  (:require [clojure.java.io :as io]
            [genegraph.snapshot.protocol :as sp]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage.rdf.names :as names]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.rocksdb :as rocksdb]
            [genegraph.framework.storage :as storage]
            [io.pedestal.log :as log])
  (:import [java.io BufferedOutputStream]
           [org.apache.commons.compress.archivers.tar
            TarArchiveEntry TarArchiveOutputStream]
           [org.apache.commons.compress.archivers ArchiveEntry]
           [org.apache.commons.compress.compressors.gzip
            GzipCompressorOutputStream]))

(defn write-snapshot [storage-handle records]
  (with-open [os (-> storage-handle
                     storage/as-handle
                     io/output-stream
                     BufferedOutputStream.
                     GzipCompressorOutputStream.
                     TarArchiveOutputStream.)]
    (run! (fn [record]
            (let [b (.getBytes (::event/value record))]
              (.putArchiveEntry
               os
               (doto (TarArchiveEntry.
                      (:genegraph.snapshot/filename record))
                 (.setSize (alength b))))
              (.write os b)
              (.closeArchiveEntry os)))
          records)))

(def serialization->extension
  {::rdf/n-triples "nt"
   :json "json"})

(defn write-records
  [{:keys [app record-type serialization storage-handle record-set]}]
  (log/info :fn ::write-records
            :type record-type
            :serialization serialization
            :storage-handle storage-handle)
  (try
    (let [db @(get-in app [:storage :record-store :instance])
          add-value (fn [record]
                      (if (::event/value record)
                        record
                        (storage/read db
                                      [record-type
                                       serialization
                                       :versions
                                       (:genegraph.snapshot/version-key
                                        record)])))]
      (->> (rocksdb/range-get db
                              {:prefix [record-type
                                        serialization
                                        record-set]
                               :return :ref})
           #_(take 1)
           (map deref)
           (map add-value)
           (filter :genegraph.snapshot/version-key)
           (map #(assoc %
                        :genegraph.snapshot/filename
                        (sp/event-filename %)))
           (write-snapshot storage-handle)))
    (catch Exception e
      (log/error :fn ::write-records :exception e))))
