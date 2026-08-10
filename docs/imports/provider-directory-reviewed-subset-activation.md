# Reviewed Provider Directory subset state sync

The selector-free activation subcommand is the only supported way to move the
fixed reviewed Provider Directory source from pending review to verified
review under its immutable root-count policy. It is default-off, changes no
dataset state, and does not publish or make a dataset current. The separately
gated terminal operators below seal exact proofless failed datasets without
activating or publishing them.

## Review the neutral desired state

The checked-in
[`provider_directory_reviewed_subset_activation.json`](../../specs/provider_directory_reviewed_subset_activation.json)
is the sole authorization input. It starts in the pending state with `null`
evidence. After the configured number of acquisition roots have satisfied the
policy, a normal pull request may change it to the verified state with exactly
these neutral evidence fields:

- `source_contract_sha256`
- `cutoff`
- `verification_source_scope_sha256`
- `completion_proof_sha256`

An explicit-policy manifest also carries the closed `root_policy` object at
top level. The cutoff is canonical UTC with microseconds and a trailing `Z`.
The manifest must not contain source, endpoint, root, dataset, URL, token, or
provider identifiers. Review must bind the four values to the retained
root-neutral proof and confirm that each root has separately valid replay and
coverage evidence. The PR follows ordinary review, CI, merge, post-merge CI,
image, migration, GitOps, and workload-readiness gates before the sync is run.

After all roots required by the policy are sealed, render the complete neutral
manifest from one read-only repeatable-read database snapshot. The command
accepts no source, endpoint, root, dataset, campaign, or cutoff selector:

```console
/opt/venv/bin/python -B \
  /opt/scripts/smoke/provider_directory_fhir_reviewed_subset_state.py \
  render-neutral-evidence
```

Its single JSON result is ready to replace the checked-in manifest. It contains
only the fixed manifest fields and the four neutral evidence values; private
source, endpoint, dataset, and root identities are never rendered. Reviewers
must still compare the result with both retained roots and follow the ordinary
pull-request and release gates above. Rendering evidence is read-only and does
not enable activation or publication.

## Run the one-shot sync

Use the exact deployed image in a one-shot Job with no source or dataset
selectors. The gate is absent from shared API, worker, scheduler, ConfigMap,
and Secret configuration and is enabled only for this Job:

```console
HLTHPRT_PROVIDER_DIRECTORY_SUBSET_STATE_SYNC_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/provider_directory_fhir_reviewed_subset_state.py \
  sync-verified-state
```

Before launch, verify the exact source commit, image manifest and runtime
configuration digests, GitOps revision, migration head, workload readiness,
Python 3.14 runtime, and the checked-in manifest. The transaction runs only at
`READ COMMITTED`, locks the exact proof generation and sole source alias, and
revalidates the complete source, root-policy, completion, replay, and coverage
contract in PostgreSQL before changing state.

Successful output is bounded JSON containing only `status`, `activated`, and
`already_applied`. A concurrent exact operation may return the safe `busy`
error; retry the same command after the other operation finishes. An exact
replay before or after publication returns `already_applied=true` only when the
database marker and its retained proof still validate. Evidence or state drift
fails closed without a partial status change.

Activation changes only the source candidate status, a closed private database
marker, and `updated_at`. The private marker binds the selected candidate and,
when required, its baseline, but those identities never enter the checked-in
manifest or operator output. Ordinary source catalog upserts preserve the
verified marker and status while PostgreSQL rejects fixed-contract drift.

## Abandon an expired acquisition root

A reviewed v3 traversal whose exact retained continuation cursors all return
HTTP 410 is not resumable. The importer retains sanitized per-resource
diagnostics and attempts to seal the proofless non-current root as
`acquisition_abandoned` on the same database backend while the endpoint worker
guard remains held. The seal commits before that guard is released, so a queued
fresh root cannot overtake the terminal disposition. The original import still
exits nonzero; abandonment never turns a failed import into success.

For a previously retained root, use the same packaged selector-free operator.
It accepts no source, endpoint, dataset, root, owner, cursor, or resource
selectors. Its gate is enabled only for the one-shot Job:

```console
HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_ABANDONMENT_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/provider_directory_fhir_reviewed_subset_state.py \
  abandon-expired-root
```

At `READ COMMITTED`, the transaction locks the pagination scope, endpoint,
sole source alias, exact proofless candidate, and its ordered checkpoint rows.
It requires the checked-in reviewed source, one complete seven-resource set of
permanent cursor diagnostics, exact latest-owner lineage, no bulk checkpoint,
and parity across resources, proof shards, and checkpoint counts. It changes
only checkpoint lifecycle fields and the parent status, resource count, and
closed identifier-free marker. A replay returns `already_applied=true` only
while the retained evidence still validates, including after source
activation.

Abandonment does not publish, delete, reset, or reuse the old root. Its rows,
proof shards, checkpoints, cursor commitments, and failed Job evidence remain
immutable for audit. Continue only with genuinely fresh acquisition roots and
no retry or pagination-root arguments. Every root required by the next campaign
must use the same newly frozen cutoff, root-count policy, and reviewed transport
profile. A two-root campaign still requires distinct roots with independently
matching terminal proofs.

## Seal an exact mixed terminal root

The expired-cursor operation above remains closed to an all-resource HTTP 410
failure. It must not be widened to reinterpret a different failure. A separate
versioned disposition handles the one reviewed policy-one root shape in which
two resources completed with stable advertised counts, one terminal resource
observed a monotone advertised-count decrease of exactly one, and four
resources retained exact retryable HTTP 500 checkpoints.

Run the selector-free one-shot command only after the failed Job is terminal
and Kubernetes, database, and Redis ownership are all absent:

```console
HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_TERMINAL_DISPOSITION_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/provider_directory_fhir_reviewed_subset_state.py \
  seal-terminal-root
```

The transaction locks and revalidates the fixed reviewed source, configured
endpoint, sole failed policy-one candidate, all seven checkpoints, resource
rows, and proof shards. It requires the three persisted diagnostic copies to
match, the exact 2/1/4 disposition partition, one terminal-page delta, and
global plus per-resource count and lineage parity. The identifier-free marker
records only hashes, dispositions, and aggregate counts. Any policy, status,
diagnostic, proof, checkpoint, resource, owner, or marker drift fails closed.

The command changes only the failed parent and its checkpoints to immutable
`acquisition_abandoned` state. It preserves every retained row and proof,
never validates or publishes the dataset, and cannot turn the old v3 failure
into evidence for the bounded-drift profile. A fresh campaign, root, cutoff,
and versioned transport profile are required after the seal.

## Seal the exact direct-v4 terminal root

The mixed v1 disposition above remains frozen to its original 2/1/4 outcome.
For the separately reviewed direct-v4 root, use its distinct selector-free
command only after the acquisition Job is terminal and all ownership is quiet:

```console
HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_DIRECT_V4_TERMINAL_DISPOSITION_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/provider_directory_fhir_reviewed_subset_state.py \
  seal-direct-v4-terminal-root
```

This profile is closed to four verified-complete resources and the three
terminal census-drift resources observed by that one root. The transaction
reuses the existing terminal-disposition locks, compare-and-swap update,
checkpoint sealing, durable validator, and replay path. Its exact neutral
marker digest is checked by the database migration. It does not add a second
disposition service, reinterpret the failed proof, validate, publish, retry,
or reuse the root. A later acquisition must use a fresh root and the next
versioned census policy.

## Publication and rollback boundary

State sync does not run artifact publication, address materialization, Profile
follow-up, or API verification. Those remain separate reviewed operations and
must revalidate the activated source and retained candidate.

The activation migration is deliberately one-way while any verified state or
activation marker exists. Downgrade fails closed until an independently
reviewed recovery operation has removed that state; the operator itself has no
deactivation command. Do not bypass the guard with direct source-status or
marker updates.
