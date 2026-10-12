# Protected registry provisioning

Run the normal migrations before provisioning. The operator command relocates
only the complete, explicitly enumerated registry tables and sequences from the
existing namespace into a dedicated namespace. It keeps native object identities
and sequence positions and applies the existing registry permission policy.
Unrelated tables and grants in the source namespace remain untouched.

The command accepts only an unprovisioned registry: all business tables must be
empty, draft and approved revisions must be zero, and the serving head must be
unset. Any manual records, history, source observations, protected receipts,
approvals or serving data require a separate retained-data migration. Relocating
such data would change namespace and custody identities; this command refuses it
without rewriting records, receipts or reader pins.
The complete roster includes the four native PTG producer, published-plan,
office-approval and office-retention tables created by the normal migrations.
Existing PTG approvals and retention records are also retained data.

Use existing trusted operator database credentials in the environment. The
operator must be able to assume every existing object owner, the source namespace
owner and the target owner, have their effective native ownership rights, and
create the target namespace. Membership in the
target owner alone does not authorize changing objects owned by another role.
The target owner must be an existing nonprivileged NOLOGIN role. The API role
must have no owner membership or SET path. No temporary bypass is installed.
The protected owner must already be permitted to execute the native PostgreSQL
`pg_control_system()` metadata reader required by the existing custody guard.
Provisioning checks this prerequisite before DDL and does not create functions.
The protected owner must also already have native CREATE authority in the old
namespace, and the API role must already have USAGE there. Provisioning does not
temporarily grant privileges or change the shared namespace's existing ACLs.

```sh
python -m process.registry_management_permissions \
  --source-schema source_registry --target-schema protected_registry \
  --api-role registry_api --owner-role registry_owner
```

The move transaction creates read-only compatibility views at the 38 old table
names. Their explicit ordered columns read the target tables with native
`security_invoker=true`; the protected owner owns them and the API receives only
SELECT. Existing catalog processes keep their old-schema reads while replacement
processes start with `HLTHPRT_NETWORK_REGISTRY_SCHEMA` set to the target. Keep the
shared application database schema setting unchanged. Old-schema writes are
denied; enable the management application bridge only after every registry user
has moved to the target and the compatibility views have been removed.
The four PTG tables remain read-only to the ordinary API. Existing dedicated PTG
reader and approval grants stay on the moved tables, and safe roles with direct
PTG SELECT grants and effective old-namespace USAGE are captured before the move.
Only that captured set receives target-namespace USAGE in the move transaction;
dormant table grants acquire no new namespace access. The ordinary permission
installer does not grant namespace access to PTG roles. Source-namespace CREATE
grants and owner membership are not copied. PTG protected-store guards still require
real tables and reject compatibility views. Inventory and drain any old-bound
PTG processes before provisioning, then restart them with the target namespace;
the catalog read bridge does not establish PTG read availability during that
transition.

Before retiring the bridge, the operator must verify that every registry reader
and writer process has restarted with the target namespace and no old-bound
registry request remains. Use the same trusted operator connection:

```sh
python -m process.registry_management_permissions \
  --source-schema source_registry --target-schema protected_registry \
  --api-role registry_api --owner-role registry_owner --retire-read-views
```

Retirement verifies the complete view inventory, native target OIDs, definitions,
column order, ownership, options and effective privileges. Missing, altered or
overlapping objects and retained dependencies are refused. It removes only these
verified views with native DROP VIEW RESTRICT in one transaction. It does not
move tables back or alter unrelated objects. Repeating provisioning verifies
either the complete exact bridge or a completely retired bridge; partial bridges
are refused. Admit source data or publish serving snapshots only after all
registry users have moved and retirement has completed. Once immutable receipts
or reader pins exist, changing the target namespace requires a separate retained
data migration; rolling back an image must keep its target namespace binding.

Keep the ordinary API credentials for manual writes. Their existing precise
column grants preserve draft CAS, history append and bounded reads without
allowing source, approved or retained-history tampering.

For SOURCE admission, use the existing import CLI with a separate trusted
operator connection and add `--protected-owner-role registry_owner`. Set
`HLTHPRT_NETWORK_REGISTRY_SCHEMA` to the target namespace before running this
command. The optional closed mode verifies the safe owner, exact schema ownership,
native SET capability and protected source custody before admission. Native transaction
scope restores the operator role on completion or failure. Ordinary CLI behavior
and receipt fields remain unchanged when this option is omitted.

SOURCE admission commits durable source evidence and identities. It does not
approve records, change serving publication policy or establish current ownership.
