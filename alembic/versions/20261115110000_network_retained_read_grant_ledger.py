# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Record retained read grants per generation so a shared base keeps the grants another generation needs."""

from alembic import op
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier

revision = "20261115110000_network_retained_read_grant_ledger"
down_revision = "20261115100000_prescription_rollup_named_fingerprint"
branch_labels = None
depends_on = None


def _ddl(schema):
    namespace = _identifier(schema)
    return [
        f"CREATE TABLE {namespace}.network_retained_read_grant_guard (reader_role varchar(63) PRIMARY KEY)",
        f"REVOKE ALL ON {namespace}.network_retained_read_grant_guard FROM PUBLIC",
        f"""CREATE TABLE {namespace}.network_retained_read_grant (
      reader_role varchar(63) NOT NULL,
      schema_name varchar(63) NOT NULL,
      manifest_sha256 varchar(64) NOT NULL CHECK(manifest_sha256 ~ '^[0-9a-f]{{64}}$'),
      relation_names text[] NOT NULL CHECK(cardinality(relation_names) BETWEEN 1 AND 128),
      PRIMARY KEY(reader_role,schema_name,manifest_sha256))""",
        f"REVOKE ALL ON {namespace}.network_retained_read_grant FROM PUBLIC",
    ]


def upgrade():
    """Create the ledger and persistent reader rows that fence stale grant decisions."""
    for statement in _ddl(registry_schema()):
        op.execute(statement)


def downgrade():
    """Dropping the ledger would forget which shared grants other generations still need."""
    raise RuntimeError("Retained read grants require an explicit ledger retirement")
