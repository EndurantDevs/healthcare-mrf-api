# Reviewed Provider Directory subset state sync

This selector-free operator is the only supported way to move the fixed
reviewed Provider Directory source from pending twin review to verified twin
review. It is default-off, changes no dataset state, and does not publish or
make a dataset current.

## Review the neutral desired state

The checked-in
[`provider_directory_reviewed_subset_activation.json`](../../specs/provider_directory_reviewed_subset_activation.json)
is the sole authorization input. It starts in the pending state with `null`
evidence. After two distinct acquisition roots have independently completed
and matched, a normal pull request may change it to the verified state with
exactly these neutral evidence fields:

- `source_contract_sha256`
- `cutoff`
- `verification_source_scope_sha256`
- `completion_proof_sha256`

The cutoff is canonical UTC with microseconds and a trailing `Z`. The manifest
must not contain source, endpoint, root, dataset, URL, token, or provider
identifiers. Review must bind the four values to the retained root-neutral
proof and confirm that each root has separately valid replay and coverage
evidence. The PR follows ordinary review, CI, merge, post-merge CI, image,
migration, GitOps, and workload-readiness gates before the sync is run.

After both roots are sealed, render the complete neutral manifest from one
read-only repeatable-read database snapshot. The command accepts no source,
endpoint, root, dataset, campaign, or cutoff selector:

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
revalidates the complete source, twin, completion, replay, and coverage
contract in PostgreSQL before changing state.

Successful output is bounded JSON containing only `status`, `activated`, and
`already_applied`. A concurrent exact operation may return the safe `busy`
error; retry the same command after the other operation finishes. An exact
replay before or after publication returns `already_applied=true` only when the
database marker and its retained proof still validate. Evidence or state drift
fails closed without a partial status change.

Activation changes only the source candidate status, a closed private database
marker, and `updated_at`. The private marker binds the selected baseline and
candidate identities, but those identities never enter the checked-in
manifest or operator output. Ordinary source catalog upserts preserve the
verified marker and status while PostgreSQL rejects fixed-contract drift.

## Publication and rollback boundary

State sync does not run artifact publication, address materialization, Profile
follow-up, or API verification. Those remain separate reviewed operations and
must revalidate the activated source and retained candidate.

The activation migration is deliberately one-way while any verified state or
activation marker exists. Downgrade fails closed until an independently
reviewed recovery operation has removed that state; the operator itself has no
deactivation command. Do not bypass the guard with direct source-status or
marker updates.
