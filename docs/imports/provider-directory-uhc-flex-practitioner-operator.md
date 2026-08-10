# Exact-cohort Flex Practitioner operator

This one-shot operator enriches the current official Practitioner population by
issuing one exact `Practitioner?identifier=<NPI>` query per member of a sealed
official NPI cohort. It never traverses the generic Flex endpoint, accepts no
source URL or resource-type selector, and makes no endpoint-completeness claim.

The three phases are separately default-off. Exactly one phase gate must equal
lowercase `true`; setting more than one gate fails closed.

## Seal or inspect the official cohort

```console
HLTHPRT_UHC_FLEX_PRACTITIONER_COHORT_ENABLED=true \
HLTHPRT_UHC_FLEX_PRACTITIONER_ACQUISITION_ENABLED=false \
HLTHPRT_UHC_FLEX_PRACTITIONER_PUBLICATION_ENABLED=false \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/uhc_flex_practitioner_operator.py \
  sync-cohort
```

The command selects the sole current, canonical official Provider Directory
dataset, seals its exact distinct Practitioner NPI population, and returns a
sanitized JSON receipt. `cohort_created=true` means the immutable cohort was
created. Repeating the command against the same official dataset returns the
same cohort with `cohort_created=false`; this is the supported cohort status
check. No NPI values are emitted.

## Acquire and admit independent roots

```console
HLTHPRT_UHC_FLEX_PRACTITIONER_COHORT_ENABLED=false \
HLTHPRT_UHC_FLEX_PRACTITIONER_ACQUISITION_ENABLED=true \
HLTHPRT_UHC_FLEX_PRACTITIONER_PUBLICATION_ENABLED=false \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/uhc_flex_practitioner_operator.py \
  acquire-admit \
  --operation-key 64_LOWERCASE_HEX_CHARACTERS \
  --semantic-projection-as-of YYYY-MM-DD
```

The operation key is an external, nonsecret campaign idempotency key. Retain it
with the semantic date. Repeating those exact inputs resumes the deterministic
baseline and candidate roots: terminal work is reused, expired leases can be
reclaimed, and sealed roots are replayed. Changing either input intentionally
creates a different dataset intent.

Acquisition revalidates the dedicated exact-query source and the current
official cohort before work and again before admission. Both complete roots
must have zero errors and identical terminal hashes and resource counts. A
successful receipt is only admission evidence; it does not publish a dataset.

Optional bounded controls are `--concurrency` (1-16, default 4),
`--max-attempts` (1-8, default 3), `--lease-seconds` (30-3600, default 300),
`--retry-base-seconds` (greater than zero and at most 60, default 1), and
`--max-retry-seconds` (greater than zero and at most 60, default 60). The
runtime contract additionally requires the retry base not exceed the maximum.

SIGINT and SIGTERM cancel workers, release/drain their work, disconnect the
database, and exit with 130 or 143. Rerun the same command to resume.

## Publish one admitted candidate

```console
HLTHPRT_UHC_FLEX_PRACTITIONER_COHORT_ENABLED=false \
HLTHPRT_UHC_FLEX_PRACTITIONER_ACQUISITION_ENABLED=false \
HLTHPRT_UHC_FLEX_PRACTITIONER_PUBLICATION_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/uhc_flex_practitioner_operator.py \
  publish-admitted \
  --candidate-acquisition-id pdufpa_48_LOWERCASE_HEX_CHARACTERS
```

Publication accepts only the candidate acquisition identifier from a matched
admission. It revalidates the admission and materialized content under the
shared publication lock before atomically advancing the source-local current
dataset. Exact replay returns the same dataset with `replayed=true`.

## Profile dispatch boundary

This operator does not dispatch the global Provider Directory Profile delta.
The repository has a public Profile builder, but it is not a safe source-local
dispatcher: it requires a complete global dataset-selection fence and a
separately admitted capacity plan. The available follow-up descriptor and
queue handoff are private orchestration internals.

For that reason, every publication receipt explicitly reports:

```json
{"operator_command_available":false,"required_external_global_dispatch":true,"status":"not_dispatched"}
```

The production controller must submit the standard global Profile follow-up
after verifying the published dataset receipt. Do not infer Profile serving
readiness from this operator's publication result.

Run each mutating phase in a separate one-shot Job. Record the exact image
digest, migration head, gate, selector receipt, Job outcome, database readiness,
and subsequent global Profile outcome independently.
