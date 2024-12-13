(ns genegraph.snapshot.gene-validity
  (:require [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage.rdf.names :as names]
            [genegraph.snapshot.protocol :as sp]
            [io.pedestal.log :as log]
            [clojure.string :as str]))

(def curation-key-query
  (rdf/create-query "
select ?k where {
?c a :cg/EvidenceStrengthAssertion ;
:dc/isVersionOf ?k . }
"))

(def version-key-query
  (rdf/create-query "
select ?c where {
?c a :cg/EvidenceStrengthAssertion .}
"))

(defmethod sp/curation-key [:gene-validity ::rdf/rdf-serialization]
  [event]
  (-> event ::event/data curation-key-query first str))

(defmethod sp/version-key [:gene-validity ::rdf/rdf-serialization]
  [event]
  (-> event ::event/data version-key-query first str))

(defmethod sp/event-filename [:gene-validity ::rdf/rdf-serialization]
  [event]
  (let [kw (names/iri->kw (:genegraph.snapshot/version-key event))]
    (str (namespace kw) "_" (name kw) ".nt")))

;; TODO, change this once we correct the frame for isVersionOf
(defmethod sp/curation-key [:gene-validity :json]
  [event]
  (-> event ::event/data :dc:isVersionOf :id))

(defmethod sp/version-key [:gene-validity :json]
  [event]
  (-> event ::event/data :id))

(defmethod sp/event-filename [:gene-validity :json]
  [event]
  (str (str/replace (:genegraph.snapshot/version-key event)
                    #":"
                    "_")
       ".json"))
