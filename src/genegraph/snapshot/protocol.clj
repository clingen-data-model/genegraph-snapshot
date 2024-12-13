(ns genegraph.snapshot.protocol)

(defn event-type-and-format [e]
  [(:genegraph.snapshot/record-type e)
   (:genegraph.framework.event/format e)])

(defmulti curation-key event-type-and-format)

(defmulti version-key event-type-and-format)

(defmulti event-filename event-type-and-format)
