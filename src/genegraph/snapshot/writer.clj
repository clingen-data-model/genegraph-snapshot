(ns genegraph.snapshot.writer
  (:require [clojure.java.io :as io]
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
              (log/info :filename (:genegraph.snapshot/filename record)
                        :record-size (count (::event/value record)))
              (.putArchiveEntry
               os
               (doto (TarArchiveEntry.
                      (:genegraph.snapshot/filename record))
                 (.setSize (alength b))))
              (.write os b)
              (.closeArchiveEntry os)))
          records)))

(defn iri->filename [iri ext]
  (log/info :iri iri)
  (let [kw (names/iri->kw iri)]
    (str (namespace kw) "_" (name kw) "." ext)))

(def format->extension
  {::rdf/n-triples "nt"
   :json "json"})

(defn write-records
  [{:keys [app record-type format storage-handle record-set]}]
  (let [db @(get-in app [:storage :record-store :instance])
        add-value (fn [record]
                    (if (::event/value record)
                      record
                      (storage/read db
                                    [record-type
                                     format
                                     record-set
                                     (:genegraph.snapshot/version-key
                                      record)])))]
    (->> (rocksdb/range-get db
                            {:prefix [record-type
                                      format
                                      :curations]
                             :return :ref})
         (take 5)
         (map deref)
         (map add-value)
         (map #(assoc %
                      :genegraph.snapshot/filename
                      (iri->filename
                       (:genegraph.snapshot/version-key %)
                       (format->extension format))))
         (write-snapshot storage-handle))))
