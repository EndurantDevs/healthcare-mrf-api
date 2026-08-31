# Exact-cohort Flex Practitioner operator

This one-shot operator enriches the current official Practitioner population by
issuing one exact `Practitioner?identifier=<NPI>` query per member of a sealed
official NPI cohort. It never traverses the generic Flex endpoint, accepts no
source URL or resource-type selector, and makes no endpoint-completeness claim.

`sync-cohort`, reviewed single-root acquisition, and exact-selector publication
remain available behind separate default-off gates. The legacy `acquire-admit`
twin phase remains
packaged for CLI compatibility but always returns
`{"code":"disabled","status":"error"}` with exit code 1 after argument
validation, even if its old gate is set to `true`.

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

## Acquire and admit one reviewed root

```console
HLTHPRT_UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ACQUISITION_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/uhc_flex_practitioner_operator.py \
  acquire-admit-single-root \
  --operation-key 64_LOWERCASE_HEX_CHARACTERS \
  --semantic-projection-as-of YYYY-MM-DD
```

The command acquires one candidate root for the sealed official cohort and
admits it under the reviewed single-root policy. Admission rechecks the exact
current official dataset and cohort. Repeating the same inputs replays the same
authority; it does not publish. Exhausted retryable transport work is released,
cooled down for 60 seconds, and resumed against the same root in the same
process. A transient Bundle-total mismatch is retried without retaining the
invalid response. An unexpected response media type is likewise rejected
before body parsing and retried; an attempt-1 error recorded by the older
runtime is reclaimed on exact replay. Other validation failures still
terminate the root. The
production Job's `activeDeadlineSeconds` bounds repeated cooldown cycles;
direct invocation continues until completion, another error, or a signal.

## Retired acquire/admit contract

This section describes historical v1 receipts only. No new acquisition or
admission can be launched through this command.

The operation key is an external, nonsecret campaign idempotency key. Retain it
with the semantic date. Repeating those exact inputs resumes the deterministic
baseline and candidate roots: terminal work is reused, expired leases can be
reclaimed, and sealed roots are replayed. Changing either input intentionally
creates a different dataset intent.

Acquisition revalidates the dedicated exact-query source and the current
official cohort before work and again before admission. Both complete roots
must have zero errors and identical terminal hashes and resource counts. A
successful receipt is only admission evidence; it does not publish a dataset.

Optional bounded controls are `--concurrency` (1-32, default 4),
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

This command publishes or exactly replays only an already-admitted candidate,
including a reviewed single-root admission. It cannot create a new acquisition
or admission.

Publication accepts only the candidate acquisition identifier from a matched
admission. It revalidates the admission and materialized content under the
shared publication lock before atomically advancing the source-local current
dataset. Exact replay returns the same dataset with `replayed=true`.

## Profile dispatch boundary

Historical publication receipts do not dispatch the global Provider Directory
Profile delta.
The repository has a public Profile builder, but it is not a safe source-local
dispatcher: it requires a complete global dataset-selection fence and a
separately admitted capacity plan. A cohort-complete publication or an admitted
retry-exhausted single-root publication embeds the exact external controller
payload at `profile_delta_dispatch.external_followup`. The partial receipt keeps
`cohort_complete=false` and its exact `retry_exhausted_count`; Profile serving
does not turn that partial evidence into a completeness claim. Extract the
eligible receipt's payload without changing any field:

```bash
GLOBAL_PROFILE_FOLLOWUP_JSON="$(
  jq -ce '
    .profile_delta_dispatch
    | select(.status == "not_dispatched")
    | select(.required_external_global_dispatch == true)
    | select(.external_followup_contract_id == "healthporta.provider-directory.global-profile-followup.v1")
    | .external_followup
    | select(.status == "required")
    | select(.kind == "provider_directory_global_profile")
    | select(.intent == "ensure_desired_generation_observed")
  ' <<<"$UHC_FLEX_PUBLICATION_RECEIPT_JSON"
)"
```

POST the exact `GLOBAL_PROFILE_FOLLOWUP_JSON` bytes unchanged to the
authenticated external Profile controller at `/v1/provider-directory/profile-followup`,
with its bearer token and the
standard `destructive-action-v1` actor, request-ID, timestamp, and signature
headers. HTTP 201 records a new durable observation; HTTP 200 is its exact
idempotent replay. Preserve the complete response as the separate controller
receipt. The closed controller payload binds the exact source,
dataset, acquisition root, idempotency key, and complete-global-fence
parameters. Its enclosing dispatch receipt separately records
`external_followup_contract_id` and `profile_strategy_version`; those sibling
metadata fields are not part of the controller payload. This extraction does
not dispatch anything, and the Flex operator has no Profile-dispatch command.
Do not infer Profile serving readiness from the controller observation receipt.

For that reason, every Profile-eligible publication receipt explicitly reports:

```json
{"operator_command_available":false,"required_external_global_dispatch":true,"status":"not_dispatched"}
```

For an eligible receipt, the production controller must submit the
embedded standard global Profile follow-up after verifying the published
dataset receipt. Do not infer Profile serving readiness from this operator's
publication result.

Do not schedule or run the retired twin phase. Reviewed single-root acquisition
and exact-selector publication remain manual and default-off.
