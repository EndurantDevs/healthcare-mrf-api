# Provider Directory Terminal Root Retirement

This operator closes one exact legacy acquisition only when its full retry
lineage is terminal and an existing published predecessor remains current. It
does not delete, rewrite, or publish data. The sole first-time mutation changes
the candidate status from `acquiring` to `acquisition_retired` and appends one
sealed evidence marker.

Use it only to clear a stale legacy root that blocks a new independent
acquisition. It is not a generic cleanup command and does not accept v4,
completed, validated, published, current, or predecessor-less candidates.

## Release fence

Before previewing, prove all of the following:

- exact post-merge CI and aggregate coverage passed;
- the exact source commit and migration head are deployed;
- migrations and readiness checks passed;
- the operator is default-off in the deployed image;
- the selected root and every retry are terminal beyond the minimum age;
- no worker, queue entry, run, or advisory lock owns the selected root;
- the named predecessor is the sole current published dataset.

Keep the gate disabled except for the single preview/apply session:

```bash
export HLTHPRT_PROVIDER_DIRECTORY_TERMINAL_ROOT_RETIREMENT_ENABLED=true
```

## Preview

Preview computes a closed digest over the locked parent, source, endpoint,
predecessor, terminal lineage, retained resources, proof shards, checkpoints,
typed rows, and direct references. It performs no writes.

```bash
./venv314/bin/python \
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
./venv314/bin/python \
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
- only `status` and the retirement marker changed;
- every child relation, proof byte, checkpoint state, source row, run row, and
  current predecessor is byte-identical;
- the retired root cannot resume or accept a retry child;
- an independent fresh root can be admitted.

Retirement is not replacement proof. Do not remove the retired rows until a
fresh dataset is current, queryable, and downstream-complete.
