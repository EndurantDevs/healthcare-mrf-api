# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add a protected metadata descriptor and independently pinned serving pointer."""

from sqlalchemy import text

from alembic import op
from db.migration_registry_preparation import can_adopt_prepared_registry_table
from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import table_sql
from db.registry_schema import registry_schema

revision = "20261028010000_registry_fhir_catalog"
down_revision = "20261025020000_company_affiliation_join"
branch_labels = None
depends_on = None
TABLE = "registry_fhir_catalog_snapshot"


def _postimage(schema):
    return table_sql(storage_identifier(schema), TABLE)


def upgrade():
    """Create or adopt the catalog descriptor and add its independently pinned serving pointer."""
    bind = op.get_bind()
    schema = registry_schema()
    if not can_adopt_prepared_registry_table(op, schema, TABLE, _postimage):
        _create(bind, schema)
    columns = (
        bind.execute(
            text(
                "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass(:relation) "
                "AND attnum>0 AND NOT attisdropped ORDER BY attnum"
            ),
            {"relation": storage_identifier(schema) + ".network_serving_control"},
        )
        .scalars()
        .all()
    )
    if columns == ["id", "generation_id"]:
        bind.execute(
            text(
                f"ALTER TABLE {storage_identifier(schema)}.network_serving_control "
                "ADD COLUMN metadata_generation varchar(64), "
                "ADD CONSTRAINT network_serving_metadata_generation CHECK "
                "(metadata_generation IS NULL OR metadata_generation ~ '^[0-9a-f]{64}$')"
            )
        )
    elif columns != ["id", "generation_id", "metadata_generation"]:
        raise RuntimeError("Registry metadata pointer preparation differs")


def _create(bind, schema):
    owner = bind.execute(
        text(
            "SELECT pg_get_userbyid(relowner) FROM pg_class "
            "WHERE oid=to_regclass(:relation) AND relkind='r' AND relpersistence='p'"
        ),
        {"relation": storage_identifier(schema) + ".network_serving_control"},
    ).scalar_one_or_none()
    if owner is None:
        raise RuntimeError("Registry catalog requires the existing serving owner")
    for statement in _postimage(schema):
        bind.execute(text(statement))
    relation = storage_identifier(schema) + "." + TABLE
    bind.execute(text(f"ALTER TABLE {relation} OWNER TO {storage_identifier(owner)}"))
    bind.execute(text(f"REVOKE ALL ON {relation} FROM PUBLIC"))


def downgrade():
    """Refuse automatic removal of retained catalog snapshots."""
    raise RuntimeError("Retained catalog snapshots require an explicit retained-data migration")
