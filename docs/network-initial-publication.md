# Initial network inventory publication

`POST /api/v1/registry/publication/initial-inventory` queues the distinct
`publish_initial_inventory` operation. It initializes an approved inventory
without approving saved drafts. Existing selected-record publication and
retained-generation operations keep their existing contracts.

The operation requires an absent serving head, approved revision zero and no
approved records. A configured trusted authority must verify a current,
unimpersonated platform administrator and the initialized policy revision.
The protected worker and independently authenticated custodian must already
have their required access. Missing access is refused; this operation does not
provision roles or reader grants.
Use the existing [run-once worker configuration](../doc/registry-publication-worker.md)
and its separate custodian connection; no additional worker command is required.

## Command and authority

The command contains exactly:

```json
{
  "operation": "publish_initial_inventory",
  "bootstrap_id": "22222222-2222-4222-8222-222222222222",
  "bootstrap_generation_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "expected_serving_generation": 0,
  "expected_approved_revision": 0,
  "reason": "Publish the verified initial inventory",
  "idempotency_key": "initial-inventory-example"
}
```

Use the UUID and digest of an actual protected bootstrap capture; example values
do not identify an admitted source. The trusted transport appends the existing
`actor` and `session_token_sha256` fields. Callers cannot select raw source pins,
recipes, roles, draft revisions or approval selections through this command.
UUIDs must be canonical and nonzero, hashes lowercase SHA-256, both initial
integers exactly zero, the reason printable and at most 512 UTF-8 bytes, and the
unchanged idempotency key at most 128 characters. Booleans are not integers.

Fresh authority binds the complete canonical command, including its key, using
sorted compact JSON with ASCII escaping. Missing or revoked authority cannot
publish. Invalid commands are rejected before native work; stale state conflicts
and worker refusals preserve the serving head.

## Publication and status

The ordinary queue receipt contains `request_id`, `state`, `created_at` and
`replayed`. Poll the existing publication request endpoint. Pending requests have
a null result. Rejected requests retain the sanitized
`registry_publication_rejected` error. A completed initial result contains:

| Field | Meaning |
| --- | --- |
| `operation` | `publish_initial_inventory` |
| `candidate_id` | Canonical candidate UUID |
| `previous_generation_id` | Integer zero |
| `generation_id` | Published positive generation |
| `approved_custom_revision` | Integer zero |
| `membership_rows` | Verified integer zero |
| `manifest_sha256` | Exact retained manifest digest |
| `policy_revision` | Revision from final fresh authority |

The worker verifies the retained bootstrap, composes and prepares an isolated
candidate, requires independent native custody, and commits the manifest and
completion together. Drafts, approval history and visibility policy remain
unchanged. An available empty approved inventory requires that real manifest;
missing preparation or custody remains unavailable.

Retain the original command after an uncertain response. Inspect its known
request ID or retry explicitly with the same key; completed exact replay remains
valid after the head advances. A different initial publication cannot replace an
existing head. Use retained-generation operations only when their ordinary
preconditions hold; clearing serving configuration is not a rollback.

Directory inventory alone does not establish pricing availability or nationwide
network coverage.
