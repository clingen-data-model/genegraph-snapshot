# Objective

Snapshot is intended to ingest records from a stream and output a set of snapshot files. These may be:

ndjson
newline-delimited bundles of individual json files
a single json file
deltas
copies of individual files to subdirectories

Starting with what we need for Gene Validity:

Probably one big gzipped ndjson for JSON downloads?

Possibly one big ntriples file for RDF downloads?

Also, need two different downloads, one for the full set of historical records, the other for the most recent version of each record.

The full, historical set isn't gonna be great as a single big file.

