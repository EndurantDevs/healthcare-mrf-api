# Snowflake Custom-Import Preflight

`process.custom_import.snowflake_preflight` is the bounded in-process preview
engine for an already validated `custom-import/v1` definition and Snowflake
source binding. The standalone operator command below provides a local
operator integration. There is no hosted preview API or workspace integration
yet.

The caller supplies an existing bounded bundle connector and a cursor-owning
adapter. The core revalidates the canonical definition and binding, derives the
approved relation and column mapping, then calls the connector's
`prepare_request` and `build_statement` methods before generating one separate
preview-only `SELECT`. It never changes the production bundle statement.

## Standalone local operator

An authorized local operator can run one retained binding by revision IDs:

```sh
python -m custom_import_snowflake_operator preflight \
  --definition-revision-id 12 \
  --source-binding-revision-id 14
```

The command accepts only the retained definition and source-binding revision
IDs plus bounded limit overrides:

- `--maximum-root-keys`
- `--maximum-child-rows`
- `--maximum-total-bytes`
- `--maximum-elapsed-seconds`

All four are positive and remain within the engine's hard caps. It accepts no
raw SQL, relation, credential, file-path, environment, or arbitrary payload
input. It loads the approved retained binding, uses the fixed local credential
composition, and performs no import, execution, registration, or database
lifecycle write.

The default JSON receipt contains only the retained digests, validation flags,
selected-scope observations and their precision, total observed bytes, status,
a generic reason, and accepted family count. It never includes generated SQL,
credential material, query identity, or a source snapshot token.

An `"unavailable"` receipt is a valid bounded observation outcome and exits
with status `0`; it does not authorize an import or imply source-wide failure.
Rejected arguments emit the fixed redacted `invalid_arguments` receipt and exit
with status `2`. A source-binding or operational failure emits a fixed redacted
error receipt and exits with status `1`; cancellation exits with status `130`.

`--include-sample` is an explicit local, sensitive-stdout option. For a
complete result it includes the bounded mapped root and child records. For an
unavailable selected-family result it includes only bounded rejected-root keys
and generic rejection codes. Do not redirect, log, attach, or otherwise retain
that stdout. The command suppresses driver and database output while it runs;
the receipt is the only intended output. It does not make a hosted preview
available.

## Result boundary

The result contains the definition, schema, and source-binding digests;
definition/mapping/runtime validation flags; scoped stream observations; and,
only when complete, a family sample. The module does not print or log rows.
Any caller that carries a sample must keep it inside an authorized private,
no-store boundary.

`date` and `timestamp` definition fields currently produce an unavailable
result because the existing capture runtime can seal only text, fixed numeric,
and boolean source types. Rejecting them here prevents a preview from accepting
a source shape that the later capture path would reject.

Every non-null declared integer must also fit the signed 64-bit replay range,
even when it is not projected for later use. The capture path still encodes
every selected source field, so an out-of-range integer produces an unavailable
runtime-compatibility result before family admission.

When selected-family admission fails, an unavailable result can carry at most
one generic rejection code for each selected root key. Those bounded keys and
codes are repr-suppressed and never logged by the core. They are selected-scope
diagnostics, not a complete source-wide rejection report.

## Sampling rules

The preview query has one deterministic root-key CTE:

- It groups by the complete logical root key and records each key's
  source-wide multiplicity.
- It ranks key groups deterministically and returns at most `R + 1` groups.
  The first `R` unique groups are the selected sample. The extra group is a
  root-selection sentinel, not an incomplete sample.
- A selected key with missing components or multiplicity other than one makes
  the sample unavailable. Duplicates outside the selected key window remain
  outside the observation scope.
- Each child stream is filtered with a full parent-key semijoin against the
  selected keys and has a `C + 1` row sentinel. Reaching that sentinel makes
  the complete sample unavailable; no partial family is returned.

Metadata rows are separate from data limits. A configured semantic snapshot is
read through the existing scalar subquery shape, so absent, null, or multi-row
metadata cannot be silently accepted. All stream tokens must match. A
single-root query-identity token remains valid only under the existing binding
rule that permits it.

The core runs `assemble_root_families` only after all selected roots and child
streams are complete. Rejections, candidate-wide errors, missing root data,
malformed result rows, cursor errors, byte caps, or elapsed-time caps discard
the sample.

## Integration boundary

The local operator composes the existing fixed key-pair provider with the
Snowflake Python preflight adapter. The adapter accepts only the engine-derived
statement, applies the bounded per-call timeout, validates the fixed result
schema, and owns exact cursor cleanup. The engine itself remains independent of
credentials and does not print or log preview rows.

## Observation semantics and limits

`exact`, `lower_bound`, and `unknown` apply only to rows observed by this
bounded query and its selected root-key scope. They are not warehouse
cardinality, import cardinality, query-credit, or cost estimates.

The limits bound selected root keys, rows per child stream, a conservative
whole-result wire allowance, client-side encoded result bytes, and elapsed
cursor work. The generated query sizes every selected metadata, key, and data
cell before returning it, including the `R + 1` and `C + 1` sentinels and a
fixed schema allowance. When that allowance exceeds the byte limit, it returns
one empty byte-limit sentinel; the core returns an unavailable result with no
sample and unknown observations. The adapter receives the execution timeout and
owns resource cleanup. `LIMIT` limits returned rows, not warehouse scan work;
grouping and joins can still scan an approved relation. The allowance is
conservative rather than a driver-allocation guarantee: a wide, mostly-null
schema can therefore be unavailable before returning a sample. The client-side
cap remains a second guard.
