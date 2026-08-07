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

## Generation-one publication

After the exact seed candidate is verified and disabled, its separately
reviewed publisher can atomically create or exactly replay generation 1:

```console
HLTHPRT_FHIR_FORMULARY_SYNTHETIC_SEED_PUBLICATION_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/formulary_fhir_synthetic_seed_publisher.py publish-seed
```

This second gate is also process-local and default-off. It must not be added to
a ConfigMap, Secret, worker, schedule, control route, or API route. The command
accepts no source, run, dataset, cutoff, generation, or intent selector. It
locks the fixed source, recomputes the complete stored graph, compares every
count and hash with the checked-in v1 evidence, and commits the current pointer
only when all postconditions are exact.

If the command's outcome is ambiguous because the caller times out or loses
its connection, rerun the same fixed command. Exact replay returns the original
generation and publication timestamp; compensation, deletion, and unpublish
are intentionally unsupported. The disabled source, generation-one pointer,
and tiny synthetic lineage remain as durable release evidence.
