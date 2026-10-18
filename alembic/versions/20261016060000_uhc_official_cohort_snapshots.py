# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain complete cohort history and make future member sets attachable."""

import importlib.util
import re
from pathlib import Path

import sqlalchemy as sa

from alembic import op
from db import migration_provider_directory_terminal_root_retirement_guards as retirement_guards
from db.plan_release_storage import plan_release_schema, storage_identifier, storage_relation

revision = "20261016060000_uhc_official_cohort_snapshots"
down_revision = "20261016050000_acquisition_validator_argument_types"
branch_labels = None
depends_on = None

HEADER = "provider_directory_uhc_flex_npi_cohort"
MEMBER = "provider_directory_uhc_flex_npi_member"
HISTORY = MEMBER + "_legacy"
BOUND = "pd_uhc_flex_npi_legacy_scope"
PKEY = "pd_uhc_flex_npi_snapshot_pkey"
LOOKUP = "pd_uhc_flex_npi_snapshot_npi_idx"
NPI_CHECK = "pd_uhc_flex_npi_member_npi_check"
_PATTERN = re.compile(r"pdufc_[0-9a-f]{48}\Z")


def _require(condition):
    if not condition:
        raise RuntimeError("uhc_cohort_snapshot_migration_changed")


def _scope(cohorts):
    _require(
        all(type(value) is str and _PATTERN.fullmatch(value) for value in cohorts) and cohorts == sorted(set(cohorts))
    )
    return (
        "cohort_id IS NOT NULL AND cohort_id IN (" + ",".join("'" + value + "'" for value in cohorts) + ")"
        if cohorts
        else "false"
    )


def _predecessor():
    path = Path(__file__).with_name("20260810050000_provider_directory_uhc_flex_npi_cohort.py")
    spec = importlib.util.spec_from_file_location("_uhc_cohort_snapshot_predecessor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _cohorts(connection, schema):
    return list(
        connection.scalars(
            sa.text(f'SELECT cohort_id FROM {storage_relation(schema, HEADER)} ORDER BY cohort_id COLLATE "C"')
        )
    )


def _guard_body(sql, delimiter="$guard$"):
    return sql.split("AS " + delimiter, 1)[1].rsplit(delimiter, 1)[0]


def _member_columns(connection, member):
    columns = list(
        connection.execute(
            sa.text("""SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
        pg_get_expr(d.adbin,d.adrelid),a.attidentity::text,a.attgenerated::text
        FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=CAST(:relation AS regclass) AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"""),
            {"relation": member},
        )
    )
    _require(
        [tuple(column) for column in columns]
        == [
            ("cohort_id", "character varying(54)", True, None, "", ""),
            ("npi", "bigint", True, None, "", ""),
            ("created_at", "timestamp with time zone", True, "transaction_timestamp()", "", ""),
        ]
    )


def _member_constraints(connection, member, header_oid):
    constraints = list(
        connection.execute(
            sa.text("""SELECT conname,contype::text,convalidated,condeferrable,condeferred,
        confrelid::bigint,conkey,confkey,conenforced,
        CASE WHEN contype='f' THEN confupdtype::text END,CASE WHEN contype='f' THEN confdeltype::text END,
        CASE WHEN contype='f' THEN confmatchtype::text END
        FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass) AND contype<>'n' ORDER BY conname"""),
            {"relation": member},
        )
    )
    expected_by_name = {
        "pd_uhc_flex_npi_member_pkey": ("p", True, False, False, 0, [1, 2], None, True, None, None, None),
        "pd_uhc_flex_npi_member_cohort_fkey": ("f", True, True, True, header_oid, [1], [1], True, "a", "a", "s"),
        "pd_uhc_flex_npi_member_npi_check": ("c", True, False, False, 0, [2], None, True, None, None, None),
    }
    actual_by_name = {constraint[0]: tuple(constraint[1:]) for constraint in constraints if constraint[0] != BOUND}
    _require(actual_by_name == expected_by_name)
    # Compare canonical native CHECK expression, never accept its name alone.
    op.execute(
        "CREATE TEMP TABLE pd_uhc_expected_npi(npi bigint CHECK(npi BETWEEN 1000000000 AND 2999999999)) ON COMMIT DROP"
    )
    actual = connection.scalar(
        sa.text(
            "SELECT pg_get_expr(conbin,conrelid,true) FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass) AND conname=:name"
        ),
        {"relation": member, "name": NPI_CHECK},
    )
    expected = connection.scalar(
        sa.text(
            "SELECT pg_get_expr(conbin,conrelid,true) FROM pg_constraint WHERE conrelid='pg_temp.pd_uhc_expected_npi'::regclass"
        )
    )
    op.execute("DROP TABLE pg_temp.pd_uhc_expected_npi")
    _require(actual == expected)
    incoming = connection.scalar(
        sa.text("SELECT count(*) FROM pg_constraint WHERE contype='f' AND confrelid=CAST(:relation AS regclass)"),
        {"relation": member},
    )
    _require(incoming == 0)
    _require(
        not connection.scalar(
            sa.text("""SELECT EXISTS(SELECT FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid
        WHERE d.classid='pg_rewrite'::regclass AND d.refclassid='pg_class'::regclass AND d.refobjid=CAST(:relation AS regclass))"""),
            {"relation": member},
        )
    )


def _member_dependencies(connection, member):
    """Refuse undeclared rewrite, row-policy, publication or composite-callable edges."""
    _require(
        not connection.scalar(
            sa.text("""SELECT
        EXISTS(SELECT FROM pg_rewrite WHERE ev_class=CAST(:relation AS regclass))
        OR EXISTS(SELECT FROM pg_policy WHERE polrelid=CAST(:relation AS regclass))
        OR EXISTS(SELECT FROM pg_publication_rel WHERE prrelid=CAST(:relation AS regclass))
        OR EXISTS(SELECT FROM pg_depend d JOIN pg_type t ON t.oid=d.refobjid
            WHERE d.classid='pg_proc'::regclass AND d.refclassid='pg_type'::regclass
            AND t.typrelid=CAST(:relation AS regclass))"""),
            {"relation": member},
        )
    )


def _member_indexes(connection, member):
    indexes = list(
        connection.execute(
            sa.text("""SELECT i.indexrelid::bigint,c.relname,i.indisunique,i.indisvalid AND i.indisready AND i.indislive,
        i.indkey::smallint[],i.indpred IS NULL AND i.indexprs IS NULL,i.indoption::smallint[],i.indnatts,i.indnkeyatts,am.amname
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam
        WHERE i.indrelid=CAST(:relation AS regclass) ORDER BY c.relname"""),
            {"relation": member},
        )
    )
    _require(
        [tuple(index[1:]) for index in indexes]
        == [
            ("pd_uhc_flex_npi_member_npi_idx", False, True, [2, 1], True, [0, 0], 2, 2, "btree"),
            ("pd_uhc_flex_npi_member_pkey", True, True, [1, 2], True, [0, 0], 2, 2, "btree"),
        ]
    )
    return [tuple(index) for index in indexes]


def _functions(connection, schema):
    """Authenticate all overloads and immutable native attributes of the original guards."""
    predecessor = _predecessor()
    expected_by_name = {
        predecessor._COHORT_GUARD: _guard_body(predecessor._cohort_guard_function_sql(schema)),
        predecessor._MEMBER_GUARD: _guard_body(predecessor._member_guard_function_sql(schema)),
        predecessor._MEMBER_INSERT_GUARD: _guard_body(predecessor._member_insert_guard_function_sql(schema)),
        retirement_guards.CHILD_GUARD: _guard_body(retirement_guards.child_guard_function_sql(schema), "$function$"),
    }
    functions = list(
        connection.execute(
            sa.text("""SELECT p.proname,p.oid::bigint,p.prosrc,p.prosecdef,p.provolatile::text,p.proconfig,
        p.proowner::bigint,p.pronargs,p.prorettype='trigger'::regtype,p.prokind::text
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=:schema AND p.proname=ANY(:names) ORDER BY p.proname,p.oid"""),
            {"schema": schema, "names": list(expected_by_name)},
        )
    )
    _require(
        len(functions) == len(expected_by_name)
        and all(
            function[2] == expected_by_name[function[0]]
            and function[3]
            and function[4] == "v"
            and function[5] == ["search_path=pg_catalog"]
            and function[7:] == (0, True, "f")
            for function in functions
        )
    )
    return predecessor, functions


def _guards(connection, schema, member_oid, header_oid):
    predecessor, functions = _functions(connection, schema)
    guards = list(
        connection.execute(
            sa.text("""SELECT t.tgrelid::bigint,t.tgname,t.tgtype,t.tgenabled::text,t.tgfoid::bigint,
        t.tgargs='\\x'::bytea,t.tgqual IS NULL,t.tgnewtable::text,t.tgoldtable::text
        FROM pg_trigger t WHERE t.tgrelid IN(:member,:header) AND NOT t.tgisinternal ORDER BY t.tgname"""),
            {"member": member_oid, "header": header_oid},
        )
    )
    functions_by_name = {function[0]: function[1] for function in functions}
    expected = _expected_guards(predecessor, functions_by_name, member_oid, header_oid)
    _require([tuple(guard) for guard in guards] == expected)
    return dict(functions=[tuple(function) for function in functions], guards=[tuple(guard) for guard in guards])


def _expected_guards(predecessor, functions_by_name, member_oid, header_oid):
    """Keep cohort and retirement event masks and the sole transition table exact."""
    specifications = (
        (header_oid, "pd_trr_flex_cohort_row", 31, retirement_guards.CHILD_GUARD, None),
        (header_oid, "pd_trr_flex_cohort_truncate", 34, retirement_guards.CHILD_GUARD, None),
        (header_oid, "pd_uhc_flex_npi_cohort_guard", 31, predecessor._COHORT_GUARD, None),
        (header_oid, "pd_uhc_flex_npi_cohort_guard_truncate", 34, predecessor._COHORT_GUARD, None),
        (member_oid, "pd_uhc_flex_npi_member_guard", 27, predecessor._MEMBER_GUARD, None),
        (member_oid, "pd_uhc_flex_npi_member_guard_truncate", 34, predecessor._MEMBER_GUARD, None),
        (member_oid, "pd_uhc_flex_npi_member_insert_guard", 4, predecessor._MEMBER_INSERT_GUARD, "new_rows"),
    )
    return sorted(
        [
            (oid, name, events, "A", functions_by_name[function], True, True, transition, None)
            for oid, name, events, function, transition in specifications
        ],
        key=lambda guard: guard[1],
    )


def _original(connection, schema):
    """Reject unknown dependencies, guards, columns and indexes before changing DDL."""
    member = storage_relation(schema, MEMBER)
    header = storage_relation(schema, HEADER)
    original = (
        connection.execute(
            sa.text("""SELECT c.oid::bigint AS oid,c.relowner::bigint AS owner,
        c.relkind::text AS kind,c.relpersistence::text AS persistence,c.relrowsecurity OR c.relforcerowsecurity AS rls
        FROM pg_class c WHERE c.oid=to_regclass(:relation)"""),
            {"relation": member},
        )
        .mappings()
        .one()
    )
    _require(original["kind"] == "r" and original["persistence"] == "p" and not original["rls"])
    _member_columns(connection, member)
    _member_dependencies(connection, member)
    header_oid = connection.scalar(sa.text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": header})
    _member_constraints(connection, member, header_oid)
    return dict(
        oid=original["oid"],
        owner=original["owner"],
        header=header_oid,
        indexes=_member_indexes(connection, member),
        hooks=_guards(connection, schema, original["oid"], header_oid),
    )


def _close(connection, schema, root):
    relation = storage_relation(schema, root)
    grants = list(
        connection.execute(
            sa.text("""SELECT DISTINCT a.grantee,pg_get_userbyid(a.grantee) FROM pg_class c
        CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
        WHERE c.oid=CAST(:relation AS regclass) AND a.grantee<>c.relowner"""),
            {"relation": relation},
        )
    )
    for oid, name in grants:
        connection.execute(
            sa.text(
                f"REVOKE ALL ON TABLE {relation} FROM "
                + ("PUBLIC" if oid == 0 else storage_identifier(name))
                + " CASCADE"
            )
        )


def _validate_bound(connection, member, expression):
    """Leave only the exact native-validated CHECK if a later short cut refuses."""
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout='100ms'")
        try:
            existing = connection.execute(
                sa.text(
                    "SELECT pg_get_expr(conbin,conrelid,true),convalidated FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass) AND conname=:name"
                ),
                {"relation": member, "name": BOUND},
            ).first()
            if existing is None:
                op.execute(
                    f"ALTER TABLE {member} ADD CONSTRAINT {storage_identifier(BOUND)} CHECK ({expression}) NOT VALID"
                )
            else:
                # PostgreSQL canonicalizes the expression: compare via an isolated native CHECK.
                op.execute(
                    f"CREATE TEMP TABLE pd_uhc_cohort_expected_bound(cohort_id varchar(54) NOT NULL CHECK ({expression}))"
                )
                expected = connection.scalar(
                    sa.text(
                        "SELECT pg_get_expr(conbin,conrelid,true) FROM pg_constraint WHERE conrelid='pg_temp.pd_uhc_cohort_expected_bound'::regclass"
                    )
                )
                op.execute("DROP TABLE pg_temp.pd_uhc_cohort_expected_bound")
                _require(existing[0] == expected)
            op.execute(f"ALTER TABLE {member} VALIDATE CONSTRAINT {storage_identifier(BOUND)}")
        finally:
            op.execute("RESET lock_timeout")


def upgrade():
    """Validate one complete historical bound without rebuilding its heap or indexes."""
    schema = plan_release_schema()
    member = storage_relation(schema, MEMBER)
    history = storage_relation(schema, HISTORY)
    header = storage_relation(schema, HEADER)
    connection = op.get_bind()
    _require(connection.scalar(sa.text("SELECT to_regclass(:relation)"), {"relation": history}) is None)
    original = _original(connection, schema)
    cohorts = _cohorts(connection, schema)
    _validate_bound(connection, member, _scope(cohorts))
    op.execute("SET LOCAL lock_timeout='100ms'")
    op.execute(f"LOCK TABLE {header},{member} IN ACCESS EXCLUSIVE MODE NOWAIT")
    _require(_original(connection, schema) == original and _cohorts(connection, schema) == cohorts)
    _require(
        connection.scalar(
            sa.text(
                "SELECT convalidated FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass) AND conname=:name"
            ),
            {"relation": member, "name": BOUND},
        )
        is True
    )
    op.execute(f"ALTER TABLE {member} RENAME TO {storage_identifier(HISTORY)}")
    op.execute(
        f"CREATE TABLE {member}(LIKE {history} INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING STORAGE,"
        f"CONSTRAINT {storage_identifier(NPI_CHECK)} CHECK(npi BETWEEN 1000000000 AND 2999999999),"
        f"CONSTRAINT {storage_identifier(PKEY)} PRIMARY KEY(cohort_id,npi)) PARTITION BY LIST(cohort_id)"
    )
    op.execute(f"CREATE INDEX {storage_identifier(LOOKUP)} ON {member}(npi,cohort_id)")
    owner = connection.scalar(sa.text("SELECT pg_get_userbyid(:oid)"), {"oid": original["owner"]})
    op.execute(f"ALTER TABLE {member} OWNER TO {storage_identifier(owner)}")
    _close(connection, schema, MEMBER)
    if cohorts:
        cohort_literals = ",".join("'" + cohort_id + "'" for cohort_id in cohorts)
        op.execute(f"ALTER TABLE {member} ATTACH PARTITION {history} FOR VALUES IN ({cohort_literals})")
    _require(_member_indexes(connection, history) == original["indexes"])
    # Original header/history guards remain until genuine protected-owner adoption.
    _require(
        connection.scalar(sa.text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": history})
        == original["oid"]
    )


def downgrade():
    """Retain historical serving storage until an explicit reverse migration."""
    raise RuntimeError("uhc_cohort_snapshots_downgrade_requires_explicit_data_migration")
