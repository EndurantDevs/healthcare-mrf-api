# Dormant FHIR formulary synchronization

The formulary synchronizer is a verify-only library contract. It loads one
explicitly enabled source, binds a single UTC cutoff, validates a complete
CoveragePlan census, and processes each DrugPlan alias serially. A completed
alias checkpoint is reused without another medication request. An absent or
incomplete checkpoint restarts that alias from page one.

The library has no ARQ worker, control route, schedule, serving route, or
publication call. Its only runtime adapter is the explicit
`python main.py manage verify-formulary-fhir` command. The command requires a
source ID, globally unique stable run ID, timezone-aware cutoff, and bounded
timeout. It remains disabled unless
`HLTHPRT_FHIR_FORMULARY_MANUAL_SYNC_ENABLED` is explicitly true, and it holds
a source-scoped single-owner database fence for the entire call. Publication
and broader runtime activation require separate reviewed delivery gates. The
database pool must provide at least two connections while the command runs:
one remains pinned to the source fence and repository work uses another.

The acquisition contract binds the exact enabled source configuration, cutoff,
CoveragePlan content, aliases, and deterministic FHIR search contracts. Source
configuration is rechecked before each alias census and immediately before
verification. Only hashes and bounded counts are returned; source locations,
continuations, and raw configuration are not retained in checkpoints.
