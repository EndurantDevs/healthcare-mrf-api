# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Restore retirement immutability for every parent-column update."""

import os

from alembic import op
from db import migration_provider_directory_first_acquisition_retirement as first
from db.migration_provider_directory_terminal_root_retirement_evidence import _q, _qf, _ql

revision = "20261023100000_provider_directory_retirement_parent_updates"
down_revision = "20261022120000_provider_directory_first_retirement"
branch_labels = None
depends_on = None

# Frozen predecessor scope; the replacement deliberately has no column list.
_LEGACY_COLUMNS = (
    "dataset_id",
    "endpoint_id",
    "import_run_id",
    "acquisition_root_run_id",
    "previous_dataset_id",
    "dataset_hash",
    "status",
    "is_current",
    "resource_count",
    "created_at",
    "validated_at",
    "published_at",
    "superseded_at",
    "publication_metadata_json",
    "completion_proof_required_version",
    "completion_proof_json",
    "completion_proof_sha256",
)
_TABLE = "provider_directory_endpoint_dataset"
_TRIGGER = "pd_trr_dataset_row"
_FUNCTION = "guard_provider_directory_terminal_root_retirement_parent"


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _fence_sql(schema, *, upgrade):
    table, signature = _qf(schema, _TABLE), _qf(schema, _FUNCTION) + "()"
    body = first.parent_sql(schema).split("AS $function$\n", 1)[1].rsplit("\n    $function$;", 1)[0]
    columns = ",".join(_ql(column) for column in _LEGACY_COLUMNS)
    return f"""
    DO $migration$
    BEGIN
        LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE;
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang
             WHERE p.oid=pg_catalog.to_regprocedure({_ql(signature)})
               AND p.prokind='f' AND p.prosecdef AND p.prorettype='pg_catalog.trigger'::regtype
               AND p.provolatile='v' AND p.proparallel='u' AND NOT p.proisstrict AND NOT p.proretset
               AND NOT p.proleakproof AND p.provariadic=0 AND p.pronargdefaults=0
               AND p.proargdefaults IS NULL AND p.prosupport=0 AND l.lanname='plpgsql'
               AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
               AND pg_catalog.btrim(pg_catalog.regexp_replace(p.prosrc,'\\s+',' ','g'))=
                   pg_catalog.btrim(pg_catalog.regexp_replace({_ql(body)},'\\s+',' ','g'))
               AND NOT EXISTS (SELECT FROM pg_catalog.pg_proc other
                    WHERE other.pronamespace=p.pronamespace AND other.proname=p.proname AND other.oid<>p.oid)
        ) OR NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger t
             WHERE t.tgrelid={_ql(table)}::regclass AND t.tgname={_ql(_TRIGGER)}
               AND NOT t.tgisinternal AND t.tgenabled='A' AND t.tgtype=31
               AND t.tgfoid=pg_catalog.to_regprocedure({_ql(signature)})
               AND t.tgconstraint=0 AND t.tgconstrrelid=0 AND t.tgparentid=0
               AND t.tgqual IS NULL AND t.tgnargs=0 AND t.tgargs='\\x'::bytea
               AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL
               AND NOT t.tgdeferrable AND NOT t.tginitdeferred
               AND (t.tgattr::text='' OR ({str(upgrade).lower()} AND
                   pg_catalog.cardinality(t.tgattr::smallint[])={len(_LEGACY_COLUMNS)} AND
                   ARRAY(SELECT a.attname::text FROM unnest(t.tgattr::smallint[]) WITH ORDINALITY x(num,pos)
                         JOIN pg_catalog.pg_attribute a ON a.attrelid=t.tgrelid AND a.attnum=x.num
                         WHERE NOT a.attisdropped ORDER BY x.pos)=ARRAY[{columns}]::text[]))
        ) THEN
            RAISE EXCEPTION 'provider_directory_retirement_parent_control_changed' USING ERRCODE='55000';
        END IF;
        IF {str(not upgrade).lower()} AND EXISTS(SELECT FROM {table} WHERE status='acquisition_retired') THEN
            RAISE EXCEPTION 'provider_directory_retirement_parent_downgrade_blocked' USING ERRCODE='55000';
        END IF;
    END $migration$;
    """


def _migrate(*, upgrade):
    schema = _schema()
    table = _qf(schema, _TABLE)
    event = "UPDATE" if upgrade else "UPDATE OF " + ",".join(_q(column) for column in _LEGACY_COLUMNS)
    op.execute("SET LOCAL lock_timeout='1s'")
    op.execute("SET LOCAL statement_timeout='10s'")
    op.execute(_fence_sql(schema, upgrade=upgrade))
    op.execute(
        f"CREATE OR REPLACE TRIGGER {_q(_TRIGGER)} BEFORE INSERT OR DELETE OR {event} ON {table} "
        f"FOR EACH ROW EXECUTE FUNCTION {_qf(schema, _FUNCTION)}()"
    )
    op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {_q(_TRIGGER)}")


def upgrade():
    """Replace only the authenticated existing hook, leaving every row intact."""
    _migrate(upgrade=True)


def downgrade():
    """Restore the prior scope only when no retired row would lose protection."""
    _migrate(upgrade=False)
