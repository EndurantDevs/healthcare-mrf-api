# Rooted Provider Directory graph operator

This packaged one-shot operator can register one reviewed rooted Provider
Directory graph source, admit one reviewed root, or publish one exact admission. It is
manual-only, has no scheduler or control worker registration, and does not
activate a public API or Profile dispatch.

Registration, reviewed single-root acquisition, and exact-selector publication
remain available behind separate default-off gates. The legacy `acquire` twin phase remains
packaged for CLI compatibility but always returns
`{"code":"disabled","status":"error"}` with exit code 1 after argument
validation, even if its old gate is set to `true`.

## Register the dormant source

```console
HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_REGISTRATION_ENABLED=true \
HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_ENABLED=false \
HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_ENABLED=false \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/provider_directory_rooted_graph_operator.py \
  register
```

Registration inserts the closed source and endpoint registry rows or validates
their exact replay. It performs no FHIR acquisition and no publication. Record
the canonical JSON receipt before enabling a later phase.

## Acquire and admit one reviewed root

```console
HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ACQUISITION_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/provider_directory_rooted_graph_operator.py \
  acquire-single-root \
  --operation-key 64_LOWERCASE_HEX_CHARACTERS
```

The command derives one candidate from the exact current reviewed root, runs
the existing bounded graph acquisition, and admits the sealed result under the
reviewed single-root policy. Repeating the same root and key replays the same
authority; it does not publish.

## Retired acquisition contract

This section describes historical v1 receipts only. No new rooted acquisition
or admission can be launched through this command.

The operation key is a required nonsecret campaign key. The exact current root,
closed operator contract, and key deterministically derive one dataset intent
and isolated baseline/candidate run identifiers. Repeating the same root and
key resumes or replays the same roots. After a terminal failure, deliberately
choose a new key to create a new attempt; wall-clock identifiers are forbidden.

This phase requires the registry phase to have already completed exactly. It
does not auto-register. Under the shared dataset publication lock it selects
the sole reviewed current root, then acquisition revalidates that root and the
source before work and before sealing. Both independent roots must seal with
matching rooted-graph evidence before admission. A successful receipt reports
the exact `publication_acquisition_id`; acquisition never publishes.
Unexpected response media types remain rejected before body parsing and enter
the existing bounded retry path.

Optional bounded controls are `--concurrency` (1-16), `--max-attempts` (1-8),
`--lease-seconds` (60-3600), `--retry-base-seconds` and
`--max-retry-seconds` (positive and at most 60), and
`--root-timeout-seconds` (at most 2592000). Cross-field runtime validation also
requires the retry base not exceed the maximum.

## Publish one exact admitted receipt

```console
HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_REGISTRATION_ENABLED=false \
HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_ENABLED=false \
HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_ENABLED=true \
  /opt/venv/bin/python -B \
  /opt/scripts/smoke/provider_directory_rooted_graph_operator.py \
  publish \
  --publication-acquisition-id pdrga_48_LOWERCASE_HEX_CHARACTERS
```

This command publishes or exactly replays only an already-admitted candidate,
including a reviewed single-root admission. It cannot create a new acquisition
or admission.

Publication accepts only the exact candidate acquisition identifier emitted by
the admission receipt. There is no source-wide scan, `latest` lookup, dataset
guess, or URL selector. The publication transaction reloads the admission and
current root, validates the materialized content, supersedes the exact previous
variant, and either publishes or reports an exact replay. The default batch
size is 4096 and the accepted range is 1-4096. Each graph batch is also capped
at 32 MiB by the publication materializer.

The publication process does not statically load the HTTP acquisition runtime.
Its receipt reports Profile dispatch as `not_dispatched` and embeds the exact
external controller payload at `profile_dispatch.external_followup`. Extract
that immutable payload from the recorded publication receipt:

```bash
GLOBAL_PROFILE_FOLLOWUP_JSON="$(
  jq -ce '
    .profile_dispatch
    | select(.status == "not_dispatched")
    | select(.required_external_global_dispatch == true)
    | select(.external_followup_contract_id == "healthporta.provider-directory.global-profile-followup.v1")
    | .external_followup
    | select(.status == "required")
    | select(.kind == "provider_directory_global_profile")
    | select(.intent == "ensure_desired_generation_observed")
  ' <<<"$ROOTED_GRAPH_PUBLICATION_RECEIPT_JSON"
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
not dispatch anything, and the rooted operator has no Profile-dispatch command.
Do not infer global serving readiness from either the source-local publication
receipt or the controller observation receipt.

Every rooted generation retains a recursive lineage to one exact legacy Flex
Practitioner cohort and its corporate official-file dataset. If that official
dataset is no longer published and current, the legacy or rooted current row
remains an immutable historical publication but immediately becomes unready
for acquisition, rooted publication, and Profile selection. A new exact legacy
generation derived from the new official cohort may supersede that known stale
row under the shared publication lock; foreign or dual-current rows still fail
closed. Rooted acquisition can resume only from that newly ready generation,
so Profile never combines a stale official cohort with a newer rooted graph.

Do not schedule or run the retired twin phase. Reviewed single-root acquisition
and exact-selector publication remain manual and default-off.

SIGINT and SIGTERM cancel active work, drain owned tasks, disconnect the
database, and exit with 130 or 143. Output is canonical JSON only and error
messages never include selectors, URLs, or retained FHIR resources.
