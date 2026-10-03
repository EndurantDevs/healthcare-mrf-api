# Custom-import operator CLI

Run these commands from an installed application environment with the existing
database configuration. They use the standalone engine; no external control
service is required. IDs below are synthetic examples, not default targets.

Validate a bounded definition from standard input without opening the database:

```sh
python -m custom_import_cli validate --format json < definition.json
```

Inspect an exact execution or retained generation:

```sh
python -m custom_import_cli status --dataset-id 5 --execution-id 17
python -m custom_import_cli status --dataset-id 5 --generation-id 19
```

Inspect the retained capture bound to one exact dataset and execution:

```sh
python -m custom_import_cli captures --dataset-id 5 --execution-id 17
```

This read-only command uses the retained operator evidence reader and returns
the capture bundle ID and manifest SHA-256, plus the execution's dataset,
definition and schema revision IDs and state. An existing execution with no
bound capture returns `"capture":null`; a missing execution returns `not_found`.
Mismatched or ambiguous retained evidence fails with a redacted `failed` receipt.
It does not acquire source data or inspect replay payloads, manifest contents,
per-stream metadata, storage paths, credentials or samples. A retained manifest
digest alone does not establish that capture payloads are still replayable.

Request cooperative cancellation; a terminal execution can return an unchanged
receipt. This does not guarantee that an active worker has already stopped:

```sh
python -m custom_import_cli cancel --execution-id 17
```

Resume an existing retained Snowflake capture with its same definition, source
binding, and idempotency key:

```sh
python -m custom_import_snowflake_operator resume \
  --definition-revision-id 12 --source-binding-revision-id 14 \
  --idempotency-key synthetic-recovery
```

This source-specific command rebuilds the retained request and statement but
does not open source credentials or acquire the source again. It accepts only
an existing capture-bound execution in `running` or `canceling` state with an
expired database lease. Queued, terminal, live-lease, unbound, and mismatched
executions fail with a redacted receipt. A `canceling` execution is only
acknowledged as canceled. The command never automatically retries.

## Bounded segmented Snowflake execution

An immutable `custom-import/source-binding/v2` binding opts into segmented
capture and bounded family builds. It retains the existing binding fields and
requires a complete `processing_policy` object. A v1 binding keeps its existing
execution path and identity; it does not inherit current operator settings.

| Policy member | Required declaration |
| --- | --- |
| `capture` | `custom-import/segmented-capture-policy/v1`: per-part limits, per-stream and whole-bundle budgets, decoded Arrow and manifest bounds, retained-storage limit, acquisition deadline |
| `driver_timeout_seconds` | Positive integer, at most 120 seconds per driver operation |
| `build` | `page_row_limit`, `page_byte_limit`, `statement_timeout_ms`, `lease_seconds`, `build_deadline_seconds` |

All limits are explicit positive integers; unknown, missing, incoherent, or
over-ceiling values fail validation. Build pages admit at most 256 records and
256 MiB. Declaration ceilings are not recommended sizing or throughput claims.
Changing any retained limit requires a new binding revision, not a retry of an
old request with different settings. The complete capture-policy fields and
validation are defined in
[`segmented_capture_policy.py`](../process/custom_import/segmented_capture_policy.py).

Use the existing revision-based command and fixed local credential provider:

```sh
python -m custom_import_snowflake_operator execute \
  --definition-revision-id 12 --source-binding-revision-id 14 \
  --idempotency-key synthetic-segmented-run
```

Acquisition seals only after every stream reaches verified EOF with matching
snapshot evidence. Source replay, global duplicate detection, family replacement,
winner materialization and final sealing continue under the same execution claim.
An invalid child still rejects its whole root family. Engine activation uses the
original current-pointer compare-and-swap; a conflict retains a sealed but
unpublished candidate. Identical effective output records `no_change` only while
the original base and pointer version remain current.

The build deadline is the capture's database `sealed_at` plus the retained build
duration. Resume uses the same capture and deadline, without accessing source
credentials or fetching source rows. An expired deadline cannot be extended by
retrying. Run acquisition in a supervised worker process: driver timeouts and
cooperative cancellation alone cannot guarantee hard termination of a blocked
driver. No policy declaration grants database or source authorization.

## Related child collections

A definition may include up to eight `child_memberships` constraints when two
declared child collections must refer to the same logical item within a root:

```json
{
  "child_memberships": [
    {
      "outer_collection": "items",
      "inner_collection": "observations",
      "key_mapping": [
        {"outer_field": "item_id", "inner_field": "observation_item_id"}
      ]
    }
  ]
}
```

Each mapping must cover the outer collection's complete child key, in order,
using distinct required fields of the inner child key. Corresponding fields
must share the supported `string` or `integer` type. Both collections still
belong directly to the same root; this does not enable cross-child query joins.

Admission checks all constraints after duplicate handling, across capture parts.
A missing referenced child rejects the whole source root family with
`child_membership_missing` evidence. An upsert may retain a compatible prior
family, but every family retained in the new candidate must also satisfy the
new constraints. Incompatible retained data fails the candidate without changing
the active generation; it is not silently removed.

Changing these constraints requires a new definition revision and import.
It does not by itself change the schema revision. Omitting `child_memberships`
preserves existing definition behavior and canonical identity.

## Explicit publication and rollback

Activate an initial generation only while the current pointer is absent:

```sh
python -m custom_import_cli activate --dataset-id 5 \
  --target-generation-id 19 --expected-pointer-version 0
```

For an existing pointer, supply both the observed generation and pointer version.
Rollback selects an exact retained generation, using the same compare-and-swap
precondition:

```sh
python -m custom_import_cli activate --dataset-id 5 \
  --target-generation-id 20 --expected-generation-id 19 --expected-pointer-version 1
python -m custom_import_cli rollback --dataset-id 5 \
  --target-generation-id 19 --expected-generation-id 20 --expected-pointer-version 2
```

Publication still applies the engine's existing seal, ownership, and retention
checks. A stale pointer fails with a redacted conflict receipt; inspect current
state before choosing new preconditions. Commands never automatically retry.

Success emits a compact JSON receipt after transaction commit and connection
cleanup. Errors omit database diagnostics and record payloads. A connection
cleanup failure can occur after commit: a failed command does not prove rollback.
Reconcile exact state with `status` before another mutation.

Use `--help` on the entry point or any command for its arguments. Generic
resume, discovery, preview, and estimate remain unavailable from
`custom_import_cli`.
