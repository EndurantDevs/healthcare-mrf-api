"""Add canonical address key SQL helper.

The resolver and archive migration now materialize canonical keys set-based in
PostgreSQL from an already computed identity string. Existing databases may
already be past the foundation/reapply revisions, so create the helper in a
new revision without replacing older functions that might be owned by a DBA
role after manual dev/prod patching.
"""

from __future__ import annotations

from alembic import op


revision = "20260612193000_address_key_from_identity_function"
down_revision = "20260611130000_reapply_address_canonical_functions"
branch_labels = None
depends_on = None


def _schema() -> str:
    bind = op.get_bind()
    value = bind.exec_driver_sql("SHOW search_path").scalar()
    if value:
        for part in str(value).split(","):
            cleaned = part.strip().strip('"')
            if cleaned and cleaned not in {"$user", "public"}:
                return cleaned
    return "mrf"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def upgrade():
    schema = _schema()
    qschema = _quote_ident(schema)
    schema_lit = schema.replace("'", "''")
    op.get_bind().exec_driver_sql(
        f"""
        DO $$
        BEGIN
            IF to_regprocedure('{schema_lit}.addr_key_from_identity_v1(text)') IS NULL THEN
                EXECUTE $function$
                    CREATE FUNCTION {qschema}.addr_key_from_identity_v1(identity_key text)
                    RETURNS uuid
                    LANGUAGE sql
                    IMMUTABLE
                    PARALLEL SAFE
                    AS $body$
                        SELECT CASE
                            WHEN identity_key IS NULL
                            THEN NULL
                            ELSE encode(substr(sha256(convert_to(identity_key, 'UTF8')), 1, 16), 'hex')::uuid
                        END
                    $body$
                $function$;
            END IF;
        END $$;
        """
    )


def downgrade():
    return None
