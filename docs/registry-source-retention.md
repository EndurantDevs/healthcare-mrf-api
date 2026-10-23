# Source retention for published reviews

Published plan and office reviews use a dedicated source retention Publisher.
Configure `source_retention_dsn` in `REGISTRY_PTG_SCOPE_RUNTIME` with a separate
LOGIN role on the same database as the existing Reader and Approver. It must be
different from those roles, both protected owners and the office Publisher.
The office Publisher keeps its existing closed-write policy.

The role must have no object ownership, role memberships, SET paths, CREATE
authority or privileged role flags. It must neither belong to another role nor
be granted to another role, including ADMIN-only membership links. Existing
source and protected-registry
SELECT and namespace USAGE grants are prerequisites. Its only write privileges
are UPDATE of the constant `registry_ptg_read_lock` column on the six source
headers and column INSERT plus DELETE on `ptg2_snapshot_pin`. The explicit
`source_retention_provisioning_statements` operator helper returns those grants;
startup and requests verify native effective privileges without granting them.
Provision the existing constant lock columns before adding these grants.

Source preparation authorizes the exact command before its short database
transaction. That transaction authenticates the source, rechecks the current
association, and commits the existing operation-owned archive pin. The separate
Approver transaction consumes the exact retained receipt without inserting or
deleting source pins. It verifies the physical evidence and fresh authority
before appending the original approval and office hold atomically.

An interrupted prepare or approval reconciles through the retention Publisher.
Reconciliation acquires the existing exclusive company fence before its fresh
snapshot, preserving approved and uncertain obligations. It releases only an
exact operation with no original approval or office hold. Cleanup between pin
preparation and approval makes the consumer refuse and reprepare; it cannot
create an unretained approval. An unknown acknowledgement is resolved from the
stored approval rather than treated as absence. Failed reconciliation leaves
the source retained for an exact retry.

Without the configured Publisher, published approval refuses; Reader operations
remain available. Existing ordinary pins and approval documents keep their
identities. After the native actor/race tests pass, use this short cutover:

1. Prepare and provision the separate source retention Publisher and its existing
   constant lock columns before changing the active approval callers.
2. Stop and drain only the legacy published approval and office approval pin
   writers. Keep unrelated readers and work available.
3. While those writers are stopped, apply
   `published_pin_writer_revocation_statements`. Revoke both table and column
   INSERT grants on source pins from the Reader and Approver. Initial
   `published_lock_provisioning_statements` plans include the same revocation.
4. Enable the configured retained-source callers and verify their installed
   role checks. The new read-only guard requires revocation before these callers
   can approve.

Do not overlap legacy and retained-source approval callers. Startup and requests
never change grants. Keep the other source lifecycle and predecessor retirement
fences until every remaining source pin writer and privilege path is closed.
