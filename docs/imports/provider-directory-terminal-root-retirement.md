# Provider Directory Terminal Root Retirement

This operator closes one exact noncurrent acquisition only when its full retry
lineage is terminal and an existing published predecessor remains current. An
explicit first-acquisition mode also accepts transport-bound-v1 roots with no
predecessor and no current or historical published dataset for the endpoint. It
supports two closed evidence profiles: a retained transport-bound-v1 root and
an explicit semantic-content-v4 root. Each profile has a distinct marker and
database validator. The operator does not delete, rewrite, validate, or publish
data. The sole first-time mutation changes the candidate status from
`acquiring` to `acquisition_retired` and appends the selected sealed marker.

Use it only to clear a stale retained root that blocks a new independent
acquisition. It is not a generic cleanup command and does not accept
transport-neutral-v2, semantic-content-v3, unknown or null explicit hash
contracts, completion-proof candidates, completed, validated, published,
current, or implicitly predecessor-less candidates. The separate direct-v4 reviewed
subset disposition command remains closed to its own failed proof-v3 shape.

## Release fence

Before previewing, prove all of the following:

- exact post-merge CI and aggregate coverage passed;
- the exact source commit and migration head are deployed;
- migrations and readiness checks passed;
- the operator is default-off in the deployed image;
- the selected root and every retry are terminal beyond the minimum age;
- no worker, queue entry, run, or advisory lock owns the selected root;
- the locked metadata selects exactly transport-bound v1 or semantic-content v4;
- the named predecessor is the sole current published dataset, or explicit
  first-acquisition mode proves there is no publication history for the endpoint.

The owner-lineage migration replaces two existing routine bodies. Deploy
compatible retained-CMS custody consumers before applying it, then recapture
affected catalog-bound custody receipts through their owning path before reuse.
Preserving routine OIDs does not preserve those catalog digests. Never rewrite
an existing receipt to conceal the change.

Keep the gate disabled except for the single preview/apply session:

The command must run through a trusted launcher that binds the issued SOURCE
transaction factory before calling this parser's `operation_runner`. Direct
standalone execution cannot infer publisher authority from an ordinary database
session. The same enablement gate applies before the launcher opens credentials.
For explicit first-acquisition maintenance only, standalone execution also accepts
a genuine PostgreSQL superuser session with `session_user = current_user`.
It never promotes an ordinary connection or changes database roles. Supply the
existing administrator connection settings only to that one operator process.
This path attests the existing retirement function bodies and trigger attachments
before and after the operation; altered, disabled or overloaded controls reject it.

```bash
export HLTHPRT_PROVIDER_DIRECTORY_TERMINAL_ROOT_RETIREMENT_ENABLED=true
```

## Preview

Preview selects the versioned profile from the locked parent metadata, then
computes a closed digest over the parent, source, endpoint, predecessor,
terminal lineage, retained resources, proof shards, checkpoints, typed rows,
and direct references. It performs no writes.

```bash
./.venv/bin/python \
  scripts/smoke/provider_directory_terminal_root_retirement.py preview \
  --source-id SOURCE_ID \
  --endpoint-id ENDPOINT_ID \
  --dataset-id DATASET_ID \
  --acquisition-root-run-id ROOT_RUN_ID \
  --owner-run-id OWNER_RUN_ID \
  --expected-current-dataset-id CURRENT_DATASET_ID
```

Record the returned `evidence_sha256`. Any intervening evidence drift makes the
apply fail closed.

## Apply once

Run the apply with the same selectors and the exact preview token:

```bash
./.venv/bin/python \
  scripts/smoke/provider_directory_terminal_root_retirement.py apply \
  --source-id SOURCE_ID \
  --endpoint-id ENDPOINT_ID \
  --dataset-id DATASET_ID \
  --acquisition-root-run-id ROOT_RUN_ID \
  --owner-run-id OWNER_RUN_ID \
  --expected-current-dataset-id CURRENT_DATASET_ID \
  --expected-evidence-sha256 EVIDENCE_SHA256
```

Do not retry after an ambiguous transport result. Resolve the parent status and
marker read-only first. An exact replay returns `already_applied: true` without
writing. Disable the gate immediately after the result is resolved.

## Postconditions

Verify read-only that:

- the parent is noncurrent and `acquisition_retired`;
- only `status` and the selected versioned retirement marker changed;
- every child relation, proof byte, checkpoint state, source row, run row, and
  current predecessor is byte-identical;
- the retired root cannot resume or accept a retry child;
- an independent fresh root can be admitted.

## First acquisition without a predecessor

Use `--expect-no-current-dataset` in place of `--expected-current-dataset-id`
for both preview and apply. Omitting both selectors, supplying both, or selecting
a different hash profile fails closed. The first-acquisition path requires the
additive guard migration and checks that the endpoint has never published a
dataset. The candidate's null predecessor is preserved; the evidence binds its
absence using the digest of JSON `null`.

The recorded owner may precede later terminal retries in the complete linear
history without transferring the candidate's ownership. This exception applies
only to first acquisition; retirement with a published predecessor still
requires the recorded owner to be the final retry.

This mode locks the exact endpoint, dataset, terminal lineage and checkpoint
controls instead of whole payload tables. Every lineage run must have no
executable ARQ state and no active exact-run worker. Unavailable queue or worker
evidence rejects the operation. Existing parent, child and retry immutability
guards remain active, including for late work after the retirement commits.
The retry fence shares per-lineage advisory locks and rechecks the complete
lineage after locking; guard owners need no additional table privileges.
The explicit first-acquisition preview and apply each have a one-hour deadline
for complete retained-payload evidence scans; ordinary retirement keeps its
three-minute deadline. Timeout cancels the transaction with no partial retirement.
An exact apply replay remains valid after the independent replacement publishes.
Downgrade is refused once a predecessor-less retirement has been used.

Retirement is not replacement proof. Do not remove the retired rows until a
fresh dataset is current, queryable, and downstream-complete.
