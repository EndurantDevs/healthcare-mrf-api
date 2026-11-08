# Publication worker permissions

The lifecycle and execution connections use separate, pre-existing direct LOGIN
roles. The protected control namespace belongs to a NOLOGIN queue owner. Private
candidate and address artifacts belong to a different NOLOGIN physical owner.
Neither worker, the API role nor the physical owner may assume the queue owner.
The API and lifecycle roles may not assume the physical owner.

A trusted bootstrap executor calls the installer using its existing database
connection in the normal application image. The operator must be able to assume
both protected owners and have PostgreSQL authority to grant the physical role
and database CREATE privilege. No roles or credentials are created; existing role flags, source ACLs and API
grants remain unchanged. Use dedicated worker roles: their direct grants within the
protected registry namespace are reconciled to this exact policy.

```python
from process.registry_publication_worker_permissions import (
    install_registry_publication_worker_permissions,
)

async with connection.transaction():
    receipt = await install_registry_publication_worker_permissions(
        connection,
        lifecycle_role="registry_lifecycle",
        execution_role="registry_execution",
        api_role="registry_api",
        queue_owner_role="registry_queue_owner",
        physical_owner_role="registry_snapshot_owner",
        control_schema="registry",
    )
```

The lifecycle role has schema USAGE, queue SELECT, and UPDATE on exactly `state`,
`lease_id`, `lease_expires_at`, `result_json`, and `updated_at`. The execution role
has the same queue rights, SELECT on the explicit migrated registry inventory,
and these column grants:

| Relation | INSERT columns | UPDATE columns |
| --- | --- | --- |
| `registry_approval_history` | approved_revision, previous_approved_revision, expected_draft_revision, actor_key, actor_json, selection_json, reason, idempotency_key, request_sha256 | none |
| `registry_approved_record` | approved_revision, record_kind, record_key, record_revision, custom_revision, record_json | none |
| `network_membership_candidate` | candidate_id, dataset_id, schema_id, producer_id, schema_name, source_generations, approved_custom_revision, expected_head, expected_rows, source_recipes_json, validation_json | accepted_rows, state, validation_json, index_ready |
| `network_serving_manifest` | generation_id, candidate_id, schema_revision, source_generations, approved_custom_revision, manifest_sha256 | none |
| `registry_revision_control` | none | draft_revision, approved_revision |
| `network_serving_control` | none | generation_id |

Approval advances both revision columns in `registry_approval_store._write_approval`.
Qualified candidate registration writes its retention descriptor into
`validation_json`. Publication allocates a serving generation using USAGE on its
sequence. The verifier checks both catalog sequences for each worker and every
role it can assume. Workers have no network-ID allocation rights; API allocation
rights remain unchanged. No worker receives queue INSERT, immutable queue UPDATE, legacy
membership-batch INSERT, DELETE, TRUNCATE, REFERENCES, TRIGGER or MAINTAIN grants.

The execution role inherits the physical owner and may SET that role, without
ADMIN option. Database CREATE is granted to the physical owner, allowing trusted
execution to construct and transfer its private artifacts. Artifact tables,
indexes and namespace receipts are written inside those private schemas; no
additional global artifact table is writable. Existing verified source-reader
memberships must provide source access separately. Never widen frozen source
ACLs or reuse an API or unrelated job credential for execution.

`verify_registry_publication_worker_permissions` accepts the same configuration
and an optional `role` keyword. It performs catalog and privilege reads in a
caller-owned transaction, including a read-only transaction. Runtime callers
supply their exact lifecycle or execution role; this also requires
`current_user = session_user`. The installer omits `role` for its trusted
postcondition check. The receipt contains the control schema and four worker and
owner identities. The existing direct-login lifecycle verifier remains required
before every queue claim or lease renewal.

The installer uses a savepoint inside the caller's transaction. Invalid native
roles, inherited excess rights, incomplete migrated objects, grant failures or
queue custody failures refuse the policy atomically. The caller controls commit
or rollback. This policy prepares the database boundary; consumer registration,
source custody, publication authorization and serving acceptance are separate.
