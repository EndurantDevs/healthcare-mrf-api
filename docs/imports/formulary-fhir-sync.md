# Dormant FHIR formulary synchronization

The formulary synchronizer is a verify-only library contract. It loads one
explicitly enabled source, binds a single UTC cutoff, validates a complete
CoveragePlan census, and processes each DrugPlan alias serially. A completed
alias checkpoint is reused without another medication request. An absent or
incomplete checkpoint restarts that alias from page one.

The library has no worker, CLI, control route, schedule, serving route, or
publication call. Callers must provide a globally unique stable run ID and
hold a source-scoped single-owner fence for the entire call. Publication and
runtime activation require separate reviewed delivery gates.

The acquisition contract binds the exact enabled source configuration, cutoff,
CoveragePlan content, aliases, and deterministic FHIR search contracts. Source
configuration is rechecked before each alias census and immediately before
verification. Only hashes and bounded counts are returned; source locations,
continuations, and raw configuration are not retained in checkpoints.
