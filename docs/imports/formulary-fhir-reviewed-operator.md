# Fixed reviewed FHIR formulary operator

This operator is the only supported runtime adapter for the fixed reviewed
FHIR formulary source. It keeps acquisition and publication as two separate,
default-off operations and exposes no source, run, dataset, generation, URL,
or publication-intent selector.

## Acquire and admit twins

The acquisition command derives two opaque run identities from the checked-in
source contract and one canonical UTC cutoff. Both roots independently acquire
every DrugPlan alias in `full` mode, even when the same content is already
current. The command admits the candidate only after PostgreSQL revalidates
the complete count and hash evidence for both roots.

```console
HLTHPRT_FHIR_FORMULARY_REVIEWED_ACQUISITION_ENABLED=true \
HLTHPRT_FHIR_FORMULARY_REVIEWED_PUBLICATION_ENABLED=false \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/formulary_fhir_reviewed_operator.py \
  acquire-twins --cutoff 2026-01-01T00:00:00Z
```

The cutoff must be an exact RFC 3339 UTC value ending in `Z`. An interrupted
run is retried with the same command and cutoff so the immutable roots and
checkpoints resume. A completed mismatch consumes both roots as durable
evidence; retrying after a mismatch requires a later reviewed cutoff.

Successful acquisition returns bounded JSON containing the opaque root and
dataset IDs, cutoff, configuration and acquisition hashes, content counts and
hashes, alternative evidence, and admission timestamp. It does not create or
advance a current pointer.

## Publish the admitted candidate

Publication uses the same cutoff and derives the exact admitted candidate. It
opens no FHIR client, performs no acquisition, accepts no dataset override, and
atomically revalidates the admission before switching the current pointer.

```console
HLTHPRT_FHIR_FORMULARY_REVIEWED_ACQUISITION_ENABLED=false \
HLTHPRT_FHIR_FORMULARY_REVIEWED_PUBLICATION_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/formulary_fhir_reviewed_operator.py \
  publish-admitted --cutoff 2026-01-01T00:00:00Z
```

An exact publication replay returns the original generation and publication
timestamp. If the current predecessor changed, source configuration drifted,
or the admission cannot be reverified, publication fails without compensating
writes or pointer movement.

## Runtime boundary

Both gates are absent by default and must never be placed in shared API,
worker, scheduler, ConfigMap, or Secret configuration. An approved operation
uses a one-shot, phase-specific Job with exactly one gate true and the other
false. There is no control route, ARQ worker, schedule, API route, or package
export for this operator.

Before either Job, bind the exact source SHA, image manifest and runtime
configuration digests, migration head, GitOps revision, workload readiness,
and cutoff in the release record. After acquisition, record that both roots
are full, admission is exact, and the pointer is unchanged. After publication,
record the generation and pointer timestamp and prove that the Job made no
other source current.

Publication makes metadata reachable through the narrow healthcare FHIR
formulary detail route. Collection, alias, medication, gateway, and MCP
availability require their separately reviewed serving contracts and must not
be inferred from a successful operator run.
