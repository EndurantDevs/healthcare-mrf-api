# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Track mutations of explicitly registered native address inputs."""

import os
import re

from alembic import op

revision = "20260930120000_cms_native_input_revision"
down_revision = "20260930110000_cms_npd_nonprofile_capacity"
branch_labels = None
depends_on = None
_TABLE = "cms_native_input_revision"


def _schema():
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("cms_native_input_schema_invalid")
    return f'"{schema}"'


def upgrade():
    """Install additive content baselines; registration never grants source acceptance."""
    schema = _schema()
    table = f'{schema}."{_TABLE}"'
    op.execute(f"""CREATE TABLE {table} (
        relation_oid bigint PRIMARY KEY CHECK (relation_oid BETWEEN 1 AND 4294967295),
        schema_name name NOT NULL, table_name name NOT NULL,
        revision bigint NOT NULL DEFAULT 0 CHECK (revision>=0)
    )""")
    op.execute(f"""CREATE FUNCTION {schema}.cms_native_input_advance() RETURNS trigger
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
BEGIN
    UPDATE {table} SET revision=revision+1 WHERE relation_oid=TG_RELID::bigint;
    IF NOT FOUND THEN RAISE EXCEPTION 'cms_native_input_not_registered'; END IF;
    RETURN NULL;
END;
$body$""")
    op.execute(f"""CREATE FUNCTION {schema}.cms_native_input_revision_guard() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $body$
BEGIN
    IF TG_OP='UPDATE' AND pg_trigger_depth()=2
       AND NEW.relation_oid=OLD.relation_oid AND NEW.schema_name=OLD.schema_name
       AND NEW.table_name=OLD.table_name AND NEW.revision=OLD.revision+1 THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'cms_native_input_revision_immutable';
END;
$body$""")
    op.execute(f"""CREATE TRIGGER cms_native_input_revision_immutable
        BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW
        EXECUTE FUNCTION {schema}.cms_native_input_revision_guard()""")
    op.execute(f"""CREATE TRIGGER cms_native_input_revision_no_truncate
        BEFORE TRUNCATE ON {table} FOR EACH STATEMENT
        EXECUTE FUNCTION {schema}.cms_native_input_revision_guard()""")
    for trigger in ("cms_native_input_revision_immutable", "cms_native_input_revision_no_truncate"):
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger}")
    op.execute(f"REVOKE ALL ON TABLE {table} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {schema}.cms_native_input_advance() FROM PUBLIC")


def downgrade():
    """Refuse to remove any registered input or its mutation history."""
    schema = _schema()
    table = f'{schema}."{_TABLE}"'
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    op.execute(
        f"DO $$ BEGIN IF EXISTS(SELECT 1 FROM {table}) THEN "
        "RAISE EXCEPTION 'cms_native_input_history_requires_retention'; END IF; END $$"
    )
    op.execute(f"DROP TABLE {table}")
    op.execute(f"DROP FUNCTION {schema}.cms_native_input_revision_guard()")
    op.execute(f"DROP FUNCTION {schema}.cms_native_input_advance()")
