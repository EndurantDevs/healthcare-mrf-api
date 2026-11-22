# Partial import completion

`process.import_completion_policy` defines a shared record acceptance rule.
After bounded retries are exhausted, an importer may accept at most 0.5% missing
records, including the boundary. Counts must describe distinct expected records
and retained records from the same frozen population. Failed requests, pages,
files, chunks and unresolved references are not record denominators.

The acquisition adapter must supply trustworthy population and settled retry
evidence. The publisher closes candidate writes, validates the retained records,
recomputes the decision, and records its receipt with the published snapshot.
The receipt binds the candidate, acquisition root, predecessor attempt, content
hash, record counts, retry evidence and acceptance basis. Handoff consumers
recheck it. Publication keeps the existing atomic switch and rollback behavior.

Accepted partial input never authorizes deletion by absence. Valid unresolved
references remain unresolved and are counted. Invalid records or references
still fail normal validation. The ordinary success status follows committed
publication; the receipt distinguishes accepted partial coverage.

## Unknown populations

An authorized operator may explicitly accept a retained snapshot whose remaining
population is unknown. Expected and missing counts stay null. Observed retries
and actual exhaustion are reported separately; manual acceptance does not assert
EOF, a coverage percentage, or exhaustion that did not happen.

The REST directory adapter supports this explicit retained-only operation through
`import_completion`. It requires the exact candidate, acquisition root, settled
parent attempt, selected resource counts, previous retry evidence, and explicit
unknown-coverage acceptance. It performs no source download. Known totals for
unfinished REST resources are rejected because a dispatch request alone does not
prove the upstream population. Control-plane dispatch requires a signed retained
retry; direct engine control retains its existing administrative authority.

Other importer adapters remain strict until their producers supply matching
record-level evidence. Merely importing the shared helper does not enable partial
publication for an importer. The source handoff carries the receipt. A derived
publisher must preserve the input coverage and validate its own output before
reporting coverage for that output.
