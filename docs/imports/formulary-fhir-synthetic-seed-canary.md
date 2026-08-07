# Synthetic FHIR formulary seed candidate

This smoke contract builds one fixed, neutral FHIR formulary seed candidate in
an exact deployed API image. It is intentionally narrower than the manual
formulary synchronizer:

- the source, base URL, runtime limits, cutoff, run ID, fixtures, and expected
  hashes are fixed in code;
- the production FHIR client performs exact count, page, and recount handling
  against an in-process session that validates every request and opens no
  network socket;
- the resulting dataset is verified with immutable seed intent, but this slice
  contains no publication call and cannot create a current pointer; and
- the synthetic source is disabled before the command returns.

The only invocation surface is the packaged smoke script:

```console
HLTHPRT_FHIR_FORMULARY_SYNTHETIC_CANARY_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/formulary_fhir_synthetic_canary.py verify-seed
```

The gate is evaluated at invocation and defaults off. It is not a deployment
setting and must not be added to a ConfigMap, Secret, worker, schedule, control
route, or API route. Before running on dev, bind the pod to the exact tested
source SHA and image, prove all FHIR formulary tables are empty, and capture a
read-only fingerprint. Stop if any non-canary source or current pointer exists.

Expected first-run evidence is one CoveragePlan, two aliases, two medication
memberships, two full alias writes, nine client requests, and the checked-in v1
hashes. An exact replay revalidates the List census, reuses both completed
checkpoints without medication requests, and returns three client requests.
Both runs leave the source disabled and `fhir_formulary_current` empty.

Publication is a separate reviewed slice with a separate default-off approval
gate and exact golden evidence. There is no supported unpublish or hard-delete
path for completed checkpoint lineages, so a later published synthetic seed is
retained as disabled, isolated evidence rather than deleted ad hoc.
