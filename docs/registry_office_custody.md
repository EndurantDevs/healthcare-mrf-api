# Office capture custody

The existing source-review runtime supports an optional server setting:

```json
"office_custody": {
  "owner_role": "office_owner",
  "publisher_role": "office_publisher"
}
```

Omitting this setting preserves plan review operations and refuses published
office preparation, approval and release. Review requests cannot select these
roles. The installed service carries the immutable profile; internal capture
contexts must use that same profile and its owner, with the original source,
review-store, reader and approver identities.

Provision a dedicated protected NOLOGIN office owner and a genuine LOGIN
Publisher. The Publisher may assume only the office owner and must not own
source, serving or review-store objects. The office owner must neither assume
nor inherit another role, own unrelated objects, nor have foreign table/column
write, MAINTAIN, sequence mutation or schema-create privileges. Both roles must lack superuser, createdb,
createrole, replication and bypassrls flags. Reader and Approver must be genuine
LOGIN roles unable to assume either the office or review-store owner.

Native checks corroborate owner and Publisher identities, role memberships,
object ownership, table and column privileges, and every retained office
family's original catalog, column, ACL and OID custody. A schema name alone is
insufficient. Unsupported ownership or more than 128 retained families refuses
the office operation. Role grants and native catalog provisioning must remain
stable during each owned transaction; administrative mutation is outside this
profile's trust boundary.

The configured Publisher retains protected-review SELECT and source SELECT,
plus UPDATE only on the six original source metadata `registry_ptg_read_lock`
columns. The original native NOT NULL smallint and validated CHECK=0 guard is
reused for those exact columns; broad header UPDATE, other column writes,
MAINTAIN and sequence mutation refuse the operation. Native OID and attribute
numbers bind the exception to the configured source relations. The configured
Publisher is checked even during Approver execution, without changing roles.
The Publisher receives no source-pin INSERT exception or source-owner authority.
No SQL privileges are provisioned by the application.

Office approval continues to recheck the full source and exact-office witness,
authorize freshly, and retain its original source pin and immutable hold in one
transaction. Prepared captures are not approvals. Approved or uncertain families
remain held. Release reads those obligations as the genuine Publisher before
assuming the office owner for exact unheld-family cleanup. This profile does not
establish composition, pricing, publication or deployment readiness.
