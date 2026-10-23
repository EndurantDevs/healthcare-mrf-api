# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain ordinary serving publication and inverse receipts atomically."""

import os

from alembic import op
from db.plan_release_storage import storage_identifier, storage_relation

revision = "20261023100000_provider_directory_serving_recovery"
down_revision = "20261024050000_provider_directory_first_retirement_owner_lineage"
branch_labels = None
depends_on = None
TABLE = "provider_directory_serving_recovery"


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def upgrade():
    """Follow the existing overlay owner before the first Address publication."""
    import sqlalchemy as sa

    schema = _schema()
    owner = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT role.rolname FROM pg_class relation JOIN pg_roles role ON role.oid=relation.relowner "
                "WHERE relation.oid=to_regclass(:overlay) AND relation.relkind='r' "
                "AND relation.relpersistence='p' AND NOT relation.relispartition"
            ),
            {"overlay": storage_relation(schema, "provider_directory_address_overlay")},
        )
        .scalar_one_or_none()
    )
    if owner is None:
        raise RuntimeError("provider_directory_serving_recovery_overlay_required")
    table = storage_relation(schema, TABLE)
    op.execute(f"""CREATE TABLE {table} (
        generation uuid NOT NULL,
        phase text NOT NULL,
        source_id text NOT NULL,
        document jsonb NOT NULL,
        document_sha256 text NOT NULL,
        CONSTRAINT pd_serving_recovery_key PRIMARY KEY(generation,phase),
        CONSTRAINT pd_serving_recovery_phase CHECK(phase IN ('overlay','published','recovered')),
        CONSTRAINT pd_serving_recovery_document CHECK(jsonb_typeof(document)='object'
          AND document->>'version'='1' AND octet_length(document::text)<=2097152),
        CONSTRAINT pd_serving_recovery_hash CHECK(document_sha256=encode(sha256(convert_to(document::text,'UTF8')),'hex'))
    )""")
    op.execute(f"ALTER TABLE {table} OWNER TO {storage_identifier(owner)}")
    op.execute(f"REVOKE ALL ON {table} FROM PUBLIC")


def downgrade():
    """Refuse to erase recorded serving recovery history."""
    import sqlalchemy as sa

    table = storage_relation(_schema(), TABLE)
    if op.get_bind().execute(sa.text(f"SELECT EXISTS(SELECT FROM {table})")).scalar_one():
        raise RuntimeError("provider_directory_serving_recovery_history_prevents_downgrade")
    op.execute(f"DROP TABLE {table}")
