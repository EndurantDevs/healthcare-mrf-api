# Required-target ledger contracts

The importer retains evidence separately from canonical networks, approvals and serving generations.
Its existing default, `--target-contract source-reference-v2`, preserves the seven-column source-reference ledger and its immutable artifact format.

`--target-contract business-context-v1` selects `registry-target-ledger-v3` and requires these exact CSV headers, in order:

```csv
COMPANY_ALIAS,COMPANY_NAME,PLAN_NAME,PLAN_TYPE,PLAN_CARRIER,FIND_CARE_NETWORK_NAME
```

The target tuple is the trimmed carrier, network name and plan type. Case and punctuation remain significant. A versioned digest of the canonical JSON tuple identifies the business target, not a canonical network or company. Every original six-cell row and its ordinal remain in the artifact. Employer names, employer aliases and plan names are context; they never become insurer aliases. Additional identifier columns are rejected by this format.

An empty network name remains a target in the denominator and cannot receive a resolved decision. Explicit reviewed bindings retain integer canonical network IDs and all existing source-coordinate and approval checks. Existing review artifacts remain snapshot-and-digest-bound; decisions are never transferred between ledgers implicitly.

The report identifies business context and source-row counts separately from canonical network evidence. `any_approved_company_link` reports a generic approved relationship; `carrier_company_status: not_assessed` does not establish a relationship to the requested carrier. Directory availability, source status and pricing remain independent. Groups are optional, and neither company nor group affiliation implies sibling network membership.

These commands do not create API/UI endpoints, approve records or publish a serving generation. Existing read-only snapshot, bounded COPY, replay and cancellation behavior remains in force.
