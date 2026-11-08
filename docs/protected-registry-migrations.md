# Protected registry migrations

Ordinary application migrations retain their existing database identity. Five
registry changes require the existing protected owner: the published-plan review
constraint, CMS notification storage, the nullable office-recipe binding column,
candidate custody, and ACA notification storage. Prepare those changes separately with existing
maintenance credentials. The ordinary Alembic upgrade then validates their full
native postimages before recording its normal revision progress. It does not
assume the protected owner, grant permissions, or skip an unprepared revision.

Preparation requires a reviewed, operation-scoped cutover. Stop new management,
registry source-import, office-coordinator and publication admissions at their
actual callers. Let admitted work finish, including physical candidate work after
a queue lease transaction has committed. Account for every active process and
transaction; an expired lease or an available company fence does not prove drain.
Keep approved catalogue readers and unrelated application operations available.
There is no single application switch that performs this entire drain.

The preparation command adds native refusal checks after this external drain:
the existing company fence, the revision row, shared source/queue table locks,
an empty queued/running request inventory, and immediate exclusive locks on the
two changed existing tables. Lock order preserves company-fence-before-revision.
It refuses contention rather than waiting for or terminating another operation.
These checks do not replace caller admission control or the physical-work drain.

With the maintenance identity already authorized to assume the protected owner:

```sh
python -m process.registry_migration_preparation \
  --control-schema protected_registry --owner-role registry_owner
```

Use existing operator credentials in the environment; do not change ordinary
application credentials or owner membership. The owner must already satisfy
source custody, including its existing native system-identity read permission.
The command validates the old layouts and executes the five protected changes in
one transaction with bounded timeouts. It checks complete native columns,
defaults, constraints, indexes, ownership and bounded ACLs, preserves existing
object identities and grants, and creates owner-only notification storage.
Unexpected default grants cause refusal. It writes no migration version.
Failure rolls back the whole preparation. A successful preparation is not a
command to repeat: proceed through ordinary Alembic adoption, or verify the exact
prepared postimage when recovering a later deployment failure.

If the original four changes have already committed while `network_candidate_custody`
is absent, do not repeat the complete preparation command. In a caller-owned
maintenance transaction with the same bounded timeouts, call
`prepare_registry_candidate_custody(connection, owner_role=..., control_schema=...)`
from `process.registry_candidate_custody_preparation`. Retain the same scoped
admission hold and physical-work drain. This forward-only call assumes the existing
protected owner, takes an immediate shared lock on the candidate parent, and
creates only missing custody storage. It preserves parent ownership, OID, and all
ACLs while granting custody SELECT only to the existing explicit nongrantable
candidate readers. Its real foreign key must target the existing candidate primary
key. A pre-existing custody relation is fully validated without repair or new grants.
Malformed storage or ACLs refuse; errors roll back the call and restore the caller
role. Existing readers remain permitted during the shared parent lock.

Custody adoption compares all native columns, defaults, checks, and indexes using
private temporary storage. It separately verifies the real permanent-parent foreign
key, enforcement, validation, match and action modes, and deferrability, plus exact
owner and reader ACLs. It never substitutes a same-named reference relation or grants
ordinary migration authority. After custody-only preparation commits, run the narrow
office-column enrollment below and the ordinary Alembic upgrade; preserve all four
previously prepared postimages and immutable data throughout.

Keep old management processes stopped after preparation. Their exact catalogue
column verifier deliberately refuses the new binding layout, but some old write
paths do not call that verifier and can resume when native locks release. This
refusal is not an admission hold. Approved readers use
their retained approved projections; their compatibility must be checked with
the complete pinned old source and its model imports, then with the replacement.
Old approval and preview code also reject retained binding history that lacks
the new nullable field, even while the current field remains NULL. The replacement
comparison supplies NULL only for that missing field; explicit recipes, other
fields and revisions remain strict, and stored history is never rewritten.
Ordinary adoption requires existing database TEMP permission for native canonical
comparison. Missing TEMP is a prerequisite failure, not permission to add a grant.

Management enrollment is separate from DDL and Alembic. The new binding column
needs exactly INSERT and UPDATE column privileges on the already authorized
management role. The general permission installer resets broader ACLs and is
not appropriate for this incremental cutover. After verifying the exact existing
role and complete preimage, use `install_registry_office_recipe_permissions` in
an explicit maintenance transaction. It retains the ordinary owner/role and
effective-policy checks, accepts only those two missing privileges, grants only
them, verifies the resulting full policy and refuses every other drift. Record
the exact before/after column ACLs and role membership outside public artifacts.
Adoption itself never enrolls permissions. Resume management and worker admission
only after replacement processes, full policy, retained-reader behavior and
normal migration post-hooks pass their separate checks.

Before owner preparation, image rollback leaves the old schema intact. After
preparation commits, preserve the additive storage and immutable receipts when
rolling back an image; do not drop the new column, notification tables, or allowed
review history. Old approved readers may continue after their compatibility proof,
but old management remains stopped because its column policy is intentionally
incompatible. Before reopening that path, either restore the verified replacement
or review a separate retained-data migration. Do not stamp revisions or broaden
the ordinary role to bypass a failed check.
