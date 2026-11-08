# Reviewed network catalog import

Use `python -m process.network_catalog_import` with existing trusted operator
database credentials to adopt one reviewed alias batch and create its missing
named network drafts. The configured connection must already be able to assume
the protected registry owner. The command verifies the existing management
permission policy and closed source custody before alias allocation. It grants
no authority and creates no functions or triggers.

Prepare one UTF-8 JSON array, bounded to 5,000 rows and 8 MiB. Each row contains
the existing alias adoption fields plus `display_name` and `aliases`:

```json
[
  {
    "source_system": "fhir",
    "source_id": "source-v1",
    "scope_key": "medical",
    "evidence_id": "reviewed-source-evidence",
    "legacy_uuid": "11111111-1111-4111-8111-111111111111",
    "reviewed_network_id": null,
    "display_name": "Example Network",
    "aliases": ["Example Network Alias"]
  }
]
```

Use `reviewed_network_id` only for an explicitly reviewed binding to an existing
integer identity. Otherwise use `null`; durable allocation uses the legacy UUID
continuity key. Names never determine or merge integer identities. All rows must
match the explicit source system, source ID and alias scope supplied to the
command. Retain the reviewed provenance behind every `evidence_id` with the
operator's private input and its SHA-256 digest.

```sh
python -m process.network_catalog_import \
  --input-file reviewed-catalog.json --input-sha256 "$CATALOG_SHA256" \
  --source-system fhir --source-id source-v1 --alias-scope medical \
  --actor-user-id 22222222-2222-4222-8222-222222222222 --actor-client-id system \
  --reason "Reviewed source names" --idempotency-key source-v1-catalog \
  --api-role registry_api --owner-role registry_owner --control-schema protected_registry
```

The explicit actor is recorded as a platform administrator performing a trusted
source operation. Alias adoption, draft heads and their initial history commit
together. Any binding, evidence or name conflict rolls back the complete batch.
Replay preserves the same integer IDs and history. Later source editions create
only missing drafts; they preserve manual names, aliases and archived records.

The command emits one JSON receipt with input and request digests, aggregate
created/preserved counts and draft/approved revisions. Failures emit a fixed
error code without input, connection or exception text. The command closes its
pool on success, failure and cancellation. It does not approve records, publish
serving data, import provider/location memberships or change source evidence.
