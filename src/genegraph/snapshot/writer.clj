(ns genegraph.snapshot.writer
  (:require [clojure.java.io :as io]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.event :as event]
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

