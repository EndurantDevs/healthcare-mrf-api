# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind reviewed CMS Organizations to existing payer identities."""

from __future__ import annotations

import os

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260929030000_provider_directory_mrf_payer_binding"
down_revision = "20260929020000_provider_directory_insurance_network_identity"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _qualified(schema: str, name: str) -> str:
    return '"' + schema.replace('"', '""') + '"."' + name + '"'


def _ensure_payer_identity(schema: str) -> None:
    # Existing installations already have this runtime table; fresh migration
    # chains need the same payer identity before creating reviewed links.
    sa.Table(
        "mrf_payer",
        sa.MetaData(),
        sa.Column("payer_id", sa.String(64), primary_key=True),
        sa.Column("canonical_name", sa.String(256), nullable=False),
        sa.Column("aliases", sa.JSON()),
        sa.Column("parent_group", sa.String(128)),
        sa.Column("entity_type", sa.String(64)),
        sa.Column("states", sa.JSON()),
        sa.Column("eins", sa.JSON()),
        sa.Column("lifecycle", sa.String(32), nullable=False),
        sa.Column("source_coverage", sa.JSON()),
        sa.Column("metadata_json", sa.JSON()),
        sa.Column("created_at", sa.TIMESTAMP()),
        sa.Column("updated_at", sa.TIMESTAMP()),
        schema=schema,
    ).create(op.get_bind(), checkfirst=True)


def _create_review_decisions(schema: str) -> None:
    """Create the append-only decisions with exact source and payer parents."""
    op.create_table(
        "provider_directory_mrf_payer_review_decision",
        sa.Column("decision_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("action", sa.String(8), nullable=False),
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("resource_type", sa.String(16), nullable=False),
        sa.Column("resource_id", sa.String(256), nullable=False),
        sa.Column("release_id", sa.String(256), nullable=False),
        sa.Column("source_payload_sha256", sa.String(64), nullable=False),
        sa.Column("payer_id", sa.String(64), nullable=False),
        sa.Column("prior_decision_id", postgresql.UUID(as_uuid=True)),
        sa.Column("review_receipt_id", sa.String(160), nullable=False),
        sa.Column("review_receipt_sha256", sa.String(64), nullable=False),
        sa.Column("review_actor", sa.String(160), nullable=False),
        sa.Column("reviewed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("decision_id"),
        sa.UniqueConstraint("decision_id", "source_id", "resource_type", "resource_id", "payer_id"),
        sa.UniqueConstraint("prior_decision_id"),
        sa.CheckConstraint(
            "resource_type = 'Organization' AND source_id = 'cms-npd'", name="pd_mrf_payer_review_source_check"
        ),
        sa.CheckConstraint(
            "(action = 'bind' AND prior_decision_id IS NULL) OR (action = 'close' AND prior_decision_id IS NOT NULL)",
            name="pd_mrf_payer_review_action_check",
        ),
        sa.CheckConstraint("review_receipt_sha256 ~ '^[0-9a-f]{64}$'", name="pd_mrf_payer_review_sha256_check"),
        sa.CheckConstraint("source_payload_sha256 ~ '^[0-9a-f]{64}$'", name="pd_mrf_payer_source_sha256_check"),
        sa.ForeignKeyConstraint(
            ("source_id", "resource_type", "resource_id", "release_id"),
            (
                f"{schema}.provider_directory_entity_release_evidence.source_id",
                f"{schema}.provider_directory_entity_release_evidence.resource_type",
                f"{schema}.provider_directory_entity_release_evidence.resource_id",
                f"{schema}.provider_directory_entity_release_evidence.release_id",
            ),
            ondelete="RESTRICT",
            name="pd_mrf_payer_review_release_fkey",
        ),
        sa.ForeignKeyConstraint(("payer_id",), (f"{schema}.mrf_payer.payer_id",), ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ("prior_decision_id", "source_id", "resource_type", "resource_id", "payer_id"),
            (
                f"{schema}.provider_directory_mrf_payer_review_decision.decision_id",
                f"{schema}.provider_directory_mrf_payer_review_decision.source_id",
                f"{schema}.provider_directory_mrf_payer_review_decision.resource_type",
                f"{schema}.provider_directory_mrf_payer_review_decision.resource_id",
                f"{schema}.provider_directory_mrf_payer_review_decision.payer_id",
            ),
            ondelete="RESTRICT",
            name="pd_mrf_payer_review_prior_fkey",
        ),
        schema=schema,
    )


def _create_active_binding(schema: str) -> None:
    op.create_table(
        "provider_directory_mrf_payer_binding",
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("resource_type", sa.String(16), nullable=False),
        sa.Column("resource_id", sa.String(256), nullable=False),
        sa.Column("payer_id", sa.String(64), nullable=False),
        sa.Column("binding_decision_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("source_id", "resource_id"),
        sa.UniqueConstraint("binding_decision_id"),
        sa.CheckConstraint(
            "resource_type = 'Organization' AND source_id = 'cms-npd'", name="pd_mrf_payer_binding_source_check"
        ),
        sa.ForeignKeyConstraint(
            ("source_id", "resource_type", "resource_id"),
            (
                f"{schema}.provider_directory_entity_source_binding.source_id",
                f"{schema}.provider_directory_entity_source_binding.resource_type",
                f"{schema}.provider_directory_entity_source_binding.resource_id",
            ),
            ondelete="RESTRICT",
            name="pd_mrf_payer_binding_organization_fkey",
        ),
        sa.ForeignKeyConstraint(
            ("binding_decision_id", "source_id", "resource_type", "resource_id", "payer_id"),
            (
                f"{schema}.provider_directory_mrf_payer_review_decision.decision_id",
                f"{schema}.provider_directory_mrf_payer_review_decision.source_id",
                f"{schema}.provider_directory_mrf_payer_review_decision.resource_type",
                f"{schema}.provider_directory_mrf_payer_review_decision.resource_id",
                f"{schema}.provider_directory_mrf_payer_review_decision.payer_id",
            ),
            ondelete="RESTRICT",
            name="pd_mrf_payer_binding_decision_fkey",
        ),
        sa.ForeignKeyConstraint(("payer_id",), (f"{schema}.mrf_payer.payer_id",), ondelete="RESTRICT"),
        schema=schema,
    )
    op.create_index(
        "pd_mrf_payer_binding_payer_idx",
        "provider_directory_mrf_payer_binding",
        ("payer_id",),
        schema=schema,
    )


def _make_review_immutable(schema: str) -> None:
    # Review evidence is append-only even outside the service path.
    quoted_schema = '"' + schema.replace('"', '""') + '"'
    op.execute(
        sa.text(
            f"CREATE FUNCTION {quoted_schema}.pd_mrf_payer_review_immutable() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN "
            "RAISE EXCEPTION 'provider_directory_mrf_payer_review_immutable'; "
            "END $$"
        )
    )
    op.execute(
        sa.text(
            f"CREATE TRIGGER pd_mrf_payer_review_immutable BEFORE UPDATE OR DELETE "
            f"ON {_qualified(schema, 'provider_directory_mrf_payer_review_decision')} "
            f"FOR EACH ROW EXECUTE FUNCTION {quoted_schema}.pd_mrf_payer_review_immutable()"
        )
    )
    for table in ("provider_directory_mrf_payer_review_decision", "provider_directory_mrf_payer_binding"):
        op.execute(
            sa.text(
                f"CREATE TRIGGER pd_mrf_payer_truncate_guard BEFORE TRUNCATE ON {_qualified(schema, table)} "
                f"FOR EACH STATEMENT EXECUTE FUNCTION {quoted_schema}.pd_mrf_payer_review_immutable()"
            )
        )


def _require_bind_decision(schema: str) -> None:
    """Only insert open bind decisions; active rows never change in place."""
    quoted_schema = '"' + schema.replace('"', '""') + '"'
    op.execute(
        sa.text(
            f"CREATE FUNCTION {quoted_schema}.pd_mrf_payer_binding_bind_only() RETURNS trigger "
            "LANGUAGE plpgsql SET search_path = pg_catalog AS $$ BEGIN "
            "IF TG_OP = 'UPDATE' THEN "
            "RAISE EXCEPTION 'provider_directory_mrf_payer_binding_immutable'; END IF; "
            f"IF NOT EXISTS (SELECT 1 FROM {_qualified(schema, 'provider_directory_mrf_payer_review_decision')} "
            "WHERE decision_id = NEW.binding_decision_id AND source_id = NEW.source_id "
            "AND resource_type = NEW.resource_type AND resource_id = NEW.resource_id "
            "AND payer_id = NEW.payer_id AND action = 'bind') "
            f"OR EXISTS (SELECT 1 FROM {_qualified(schema, 'provider_directory_mrf_payer_review_decision')} "
            "WHERE prior_decision_id = NEW.binding_decision_id AND action = 'close') THEN "
            "RAISE EXCEPTION 'provider_directory_mrf_payer_binding_requires_bind_decision'; "
            "END IF; RETURN NEW; END $$"
        )
    )
    op.execute(
        sa.text(
            f"CREATE TRIGGER pd_mrf_payer_binding_bind_only BEFORE INSERT OR UPDATE "
            f"ON {_qualified(schema, 'provider_directory_mrf_payer_binding')} "
            f"FOR EACH ROW EXECUTE FUNCTION {quoted_schema}.pd_mrf_payer_binding_bind_only()"
        )
    )


def _require_close_decision(schema: str) -> None:
    """Preserve an active link until a matching close review is recorded."""
    quoted_schema = '"' + schema.replace('"', '""') + '"'
    op.execute(
        sa.text(
            f"CREATE FUNCTION {quoted_schema}.pd_mrf_payer_binding_close_only() RETURNS trigger "
            "LANGUAGE plpgsql SET search_path = pg_catalog AS $$ BEGIN "
            f"IF NOT EXISTS (SELECT 1 FROM {_qualified(schema, 'provider_directory_mrf_payer_review_decision')} "
            "WHERE prior_decision_id = OLD.binding_decision_id "
            "AND source_id = OLD.source_id AND resource_type = OLD.resource_type "
            "AND resource_id = OLD.resource_id AND payer_id = OLD.payer_id AND action = 'close') THEN "
            "RAISE EXCEPTION 'provider_directory_mrf_payer_binding_requires_close_decision'; "
            "END IF; RETURN OLD; END $$"
        )
    )
    op.execute(
        sa.text(
            f"CREATE TRIGGER pd_mrf_payer_binding_close_only BEFORE DELETE "
            f"ON {_qualified(schema, 'provider_directory_mrf_payer_binding')} "
            f"FOR EACH ROW EXECUTE FUNCTION {quoted_schema}.pd_mrf_payer_binding_close_only()"
        )
    )


def upgrade() -> None:
    """Create reviewed links after stable Organization and network identities."""
    schema = _schema()
    _ensure_payer_identity(schema)
    _create_review_decisions(schema)
    op.create_index(
        "pd_mrf_payer_review_source_idx",
        "provider_directory_mrf_payer_review_decision",
        ("source_id", "resource_id", "reviewed_at"),
        schema=schema,
    )
    _create_active_binding(schema)
    _make_review_immutable(schema)
    _require_bind_decision(schema)
    _require_close_decision(schema)


def downgrade() -> None:
    """Remove only empty review tables, preserving retained decisions."""
    schema = _schema()
    connection = op.get_bind()
    tables = ("provider_directory_mrf_payer_binding", "provider_directory_mrf_payer_review_decision")
    connection.exec_driver_sql(
        "LOCK TABLE " + ", ".join(_qualified(schema, table) for table in tables) + " IN ACCESS EXCLUSIVE MODE"
    )
    for table in tables:
        if connection.exec_driver_sql(f"SELECT EXISTS (SELECT 1 FROM {_qualified(schema, table)})").scalar_one():
            raise RuntimeError("provider_directory_mrf_payer_binding_downgrade_requires_empty_tables")
    op.drop_index("pd_mrf_payer_binding_payer_idx", table_name=tables[0], schema=schema)
    op.execute(sa.text(f"DROP TRIGGER pd_mrf_payer_binding_close_only ON {_qualified(schema, tables[0])}"))
    op.execute(sa.text(f"DROP TRIGGER pd_mrf_payer_binding_bind_only ON {_qualified(schema, tables[0])}"))
    op.execute(sa.text(f"DROP TRIGGER pd_mrf_payer_truncate_guard ON {_qualified(schema, tables[0])}"))
    op.drop_table(tables[0], schema=schema)
    op.execute(sa.text(f"DROP FUNCTION {_qualified(schema, 'pd_mrf_payer_binding_close_only')}()"))
    op.execute(sa.text(f"DROP FUNCTION {_qualified(schema, 'pd_mrf_payer_binding_bind_only')}()"))
    op.execute(sa.text(f"DROP TRIGGER pd_mrf_payer_truncate_guard ON {_qualified(schema, tables[1])}"))
    op.execute(sa.text(f"DROP TRIGGER pd_mrf_payer_review_immutable ON {_qualified(schema, tables[1])}"))
    op.execute(sa.text(f"DROP FUNCTION {_qualified(schema, 'pd_mrf_payer_review_immutable')}()"))
    op.drop_index("pd_mrf_payer_review_source_idx", table_name=tables[1], schema=schema)
    op.drop_table(tables[1], schema=schema)
