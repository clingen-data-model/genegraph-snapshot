(ns genegraph.snapshot.gene-validity
  (:require [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.event :as event]
            [genegraph.snapshot.protocol :as sp]))

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

(defmethod sp/curation-key [:gene-validity :json]
  [event]
  (-> event ::event/data curation-key-query first str))

(defmethod sp/version-key [:gene-validity :json]
  [event]
  (-> event ::event/data version-key-query first str))
