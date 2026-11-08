# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep a first candidate's owner when later terminal retries claimed no rows."""

import os

from alembic import op
from db import migration_provider_directory_first_acquisition_retirement as first
from db import migration_provider_directory_first_retirement_owner_lineage as owner_lineage
from db.migration_provider_directory_terminal_root_retirement_evidence import _qf, _ql

revision = "20261024050000_provider_directory_first_retirement_owner_lineage"
down_revision = "20261023220000_merge_directory_pricing_controls"
branch_labels = None
depends_on = None


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _controls_sql(schema):
    checks = []
    for name, arguments, result_type, settings in (
        (
            "provider_directory_terminal_root_retirement_evidence",
            "text",
            "jsonb",
            "'search_path=pg_catalog','TimeZone=UTC'",
        ),
        ("provider_directory_terminal_root_retirement_eligible", "text,integer", "bool", "'search_path=pg_catalog'"),
    ):
        signature = _qf(schema, name) + "(" + arguments + ")"
        checks.append(f"""EXISTS (
            SELECT FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang
             WHERE p.oid=pg_catalog.to_regprocedure({_ql(signature)})
               AND p.prokind='f' AND p.prosecdef AND p.prorettype='{result_type}'::regtype
               AND p.provolatile='s' AND p.proparallel='u' AND l.lanname='sql'
               AND NOT p.proisstrict AND NOT p.proretset AND NOT p.proleakproof
               AND p.provariadic=0 AND p.pronargdefaults=0 AND p.proargdefaults IS NULL AND p.prosupport=0
               AND p.proconfig=ARRAY[{settings}]::text[]
               AND NOT EXISTS (SELECT FROM pg_catalog.pg_proc other WHERE other.pronamespace=p.pronamespace
                    AND other.proname=p.proname AND other.oid<>p.oid)
        )""")
    return (
        "DO $migration$ BEGIN IF NOT ("
        + " AND ".join(checks)
        + """ ) THEN
        RAISE EXCEPTION 'provider_directory_retirement_owner_lineage_control_changed' USING ERRCODE='55000';
    END IF; END $migration$;"""
    )


def _migrate(*, upgrade):
    schema = _schema()
    op.execute("SET LOCAL lock_timeout='1s'")
    op.execute("SET LOCAL statement_timeout='10s'")
    op.execute(_controls_sql(schema))
    if not upgrade:
        dataset, runs = _qf(schema, "provider_directory_endpoint_dataset"), _qf(schema, "import_run")
        op.execute(f"""DO $migration$ BEGIN
            LOCK TABLE {dataset} IN SHARE ROW EXCLUSIVE MODE;
            IF EXISTS (SELECT FROM {dataset} parent WHERE parent.status='acquisition_retired'
                AND parent.previous_dataset_id IS NULL
                AND EXISTS (SELECT FROM {runs} child WHERE child.retry_of_run_id=parent.import_run_id)) THEN
                RAISE EXCEPTION 'provider_directory_retirement_owner_lineage_downgrade_blocked' USING ERRCODE='55000';
            END IF;
        END $migration$;""")
    for old, new in owner_lineage.replacements(schema):
        for statement in first.guarded_replacement(old if upgrade else new, new if upgrade else old):
            op.execute(statement)


def upgrade():
    """Extend two authenticated existing routines without changing rows or grants."""
    _migrate(upgrade=True)


def downgrade():
    """Restore the old bodies only while no retired ancestor owner needs them."""
    _migrate(upgrade=False)
