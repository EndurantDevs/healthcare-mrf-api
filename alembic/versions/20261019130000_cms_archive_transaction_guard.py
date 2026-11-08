# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep archive revision ownership bound to the full native transaction identity."""

from alembic import op
from db.plan_release_storage import plan_release_schema, storage_identifier, storage_relation

revision = "20261019130000_cms_archive_transaction_guard"
down_revision = "20261019030000_provider_directory_role_schema"
branch_labels = None
depends_on = None
# Canonical bodies preserve both identifier and literal schema references.
_CONTRACTS = (
    (
        "cms_native_input_advance",
        "2b769106e796f7d2f8645f836b5eff960def7b6283e9766e4d94843cd2c7ad55",
        "492ee877ef3c4ee7a45515665321405898815f35e6c13da96e571ab3f9425b6d",
        "trigger",
        "true",
    ),
    (
        "cms_native_input_revision_guard",
        "3598ddfa41664f2f965f5c042bae8fb869161b238ea9370edafad1c00f164846",
        "0206ebcd3e5ce798c84e0df20877eee256792825fd2f722dacf182cfc3a363dc",
        "trigger",
        "false",
    ),
    (
        "cms_serving_archive_result_matches",
        "6fdd19a239328e8f7d89a0c7f05892e8d5e7dd5673d525eff4452845f790202e",
        "38426c90255c28e871039fe2aa9e844758cdb67de6ab8e2199ef04912ef06b66",
        "boolean",
        "false",
    ),
)


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def _replacements(schema):
    table = storage_relation(schema, "cms_native_input_revision")
    return {
        "cms_native_input_advance": (
            (
                f"UPDATE {table} SET revision=revision+1 WHERE",
                f"UPDATE {table} SET revision=revision+1,mutation_xid=pg_current_xact_id() WHERE",
            ),
        ),
        "cms_native_input_revision_guard": (
            (
                "NEW.table_name=OLD.table_name AND NEW.revision=OLD.revision+1 THEN",
                "NEW.table_name=OLD.table_name AND NEW.revision=OLD.revision+1\n"
                "       AND NEW.mutation_xid=pg_current_xact_id() THEN",
            ),
        ),
        "cms_serving_archive_result_matches": (
            ("current_revision bigint; mutation_xid bigint;", "current_revision bigint; mutation_xid xid8;"),
            ("SELECT revision,xmin::text::bigint FROM", "SELECT revision,mutation_xid FROM"),
            (
                "mutation_xid=mod(pg_current_xact_id()::text::numeric,4294967296)::bigint",
                "mutation_xid=pg_current_xact_id()",
            ),
        ),
    }


def _canonical_hash(column, schema):
    quoted_schema, quoted_canonical = _literal(storage_identifier(schema)), _literal('"__schema__"')
    literal_schema, literal_canonical = _literal(_literal(schema)), _literal("'__schema__'")
    return (
        f"encode(sha256(convert_to(replace(replace({column},{quoted_schema},{quoted_canonical}),"
        f"{literal_schema},{literal_canonical}),'UTF8')),'hex')"
    )


def _replacement_sql(schema, names):
    repairs = []
    for name in names:
        for previous, current in _replacements(schema)[name]:
            repairs.append(f"""IF expected.name={_literal(name)} THEN
                IF (length(replacement)-length(replace(replacement,{_literal(previous)},'')))
                    /length({_literal(previous)})<>1 THEN RAISE EXCEPTION 'cms_archive_guard_dispatch_changed'; END IF;
                replacement:=replace(replacement,{_literal(previous)},{_literal(current)});
                definition:=replace(definition,{_literal(previous)},{_literal(current)});
            END IF;""")
    return "".join(repairs)


def _function_sql(schema, names):
    contracts = [contract for contract in _CONTRACTS if contract[0] in names]
    contract_rows_sql = ",".join(
        "(" + ",".join(_literal(str(field)) for field in contract) + ")" for contract in contracts
    )
    name_list = ",".join(_literal(name) for name in names)
    return f"""DO $migration$
    DECLARE expected record; original pg_proc%ROWTYPE; updated pg_proc%ROWTYPE;
        body_hash text; replacement text; definition text;
    BEGIN
        IF (SELECT count(*) FROM pg_proc WHERE pronamespace=to_regnamespace({_literal(schema)})
            AND proname IN ({name_list}))<>{len(names)} THEN
            RAISE EXCEPTION 'cms_archive_guard_function_set_changed';
        END IF;
        FOR expected IN SELECT * FROM (VALUES {contract_rows_sql}) AS contracts(name,previous_hash,next_hash,result,definer) LOOP
            SELECT * INTO STRICT original FROM pg_proc
                WHERE pronamespace=to_regnamespace({_literal(schema)}) AND proname=expected.name;
            IF original.prokind<>'f' OR original.prorettype<>expected.result::regtype
                OR original.prolang<>(SELECT oid FROM pg_language WHERE lanname='plpgsql')
                OR original.pronargs<>(CASE WHEN expected.result='boolean' THEN 1 ELSE 0 END)
                OR original.proargtypes<>(CASE WHEN expected.result='boolean' THEN '3802' ELSE '' END)::oidvector
                OR original.proargnames IS DISTINCT FROM (CASE WHEN expected.result='boolean' THEN ARRAY['p']::text[] END)
                OR original.pronargdefaults<>0 OR original.provariadic<>0 OR original.proargmodes IS NOT NULL
                OR original.proallargtypes IS NOT NULL OR original.proargdefaults IS NOT NULL
                OR original.protrftypes IS NOT NULL OR original.probin IS NOT NULL
                OR original.prosqlbody IS NOT NULL OR original.prosupport<>0
                OR original.prosecdef<>expected.definer::boolean
                OR original.proleakproof OR original.proisstrict OR original.proretset
                OR original.provolatile<>(CASE WHEN expected.result='boolean' THEN 's' ELSE 'v' END)::"char"
                OR original.proparallel<>'u' OR original.procost<>100 OR original.prorows<>0
                OR original.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[] THEN
                RAISE EXCEPTION 'cms_archive_guard_definition_changed';
            END IF;
            body_hash:={_canonical_hash("original.prosrc", schema)};
            replacement:=original.prosrc;
            IF body_hash=expected.previous_hash THEN
                definition:=pg_get_functiondef(original.oid);
                {_replacement_sql(schema, names)}
                EXECUTE definition;
            ELSIF body_hash<>expected.next_hash THEN RAISE EXCEPTION 'cms_archive_guard_body_changed'; END IF;
            SELECT * INTO STRICT updated FROM pg_proc WHERE oid=original.oid;
            IF (to_jsonb(updated)-'prosrc') IS DISTINCT FROM (to_jsonb(original)-'prosrc')
                OR updated.prosrc IS DISTINCT FROM replacement
                OR {_canonical_hash("updated.prosrc", schema)}<>expected.next_hash THEN
                RAISE EXCEPTION 'cms_archive_guard_replacement_changed';
            END IF;
        END LOOP;
        IF (SELECT count(*) FROM pg_proc WHERE pronamespace=to_regnamespace({_literal(schema)})
            AND proname IN ({name_list}))<>{len(names)} THEN
            RAISE EXCEPTION 'cms_archive_guard_function_set_changed';
        END IF;
    END $migration$"""


def _counter_sql(schema):
    """Upgrade the existing statement counter without rewriting historical revision rows."""
    table = storage_relation(schema, "cms_native_input_revision")
    column_sql = f"""
        IF NOT EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass({_literal(table)})
            AND attname='mutation_xid' AND NOT attisdropped) THEN
            ALTER TABLE {table} ADD COLUMN mutation_xid pg_catalog.xid8;
        END IF;
        IF (SELECT count(*) FROM pg_attribute WHERE attrelid=to_regclass({_literal(table)}) AND attnum>0)<>5
            OR NOT EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass({_literal(table)})
                AND attnum=5 AND attname='mutation_xid' AND atttypid='pg_catalog.xid8'::regtype
                AND atttypmod=-1 AND NOT attnotnull AND NOT attisdropped AND attcollation=0
                AND NOT atthasdef AND NOT atthasmissing AND attmissingval IS NULL
                AND attidentity='' AND attgenerated='' AND attacl IS NULL) THEN
            RAISE EXCEPTION 'cms_native_input_mutation_identity_changed';
        END IF;
    """
    repair = _function_sql(schema, ("cms_native_input_advance", "cms_native_input_revision_guard"))
    return repair.replace("    BEGIN\n", "    BEGIN\n" + column_sql, 1)


def _repair_sql(schema):
    """Keep the matcher callable during bootstrap before optional input registration."""
    return _function_sql(schema, ("cms_serving_archive_result_matches",))


def upgrade():
    """Preserve rows, object identities, grants and every existing publication predicate."""
    schema = plan_release_schema()
    storage_identifier(schema)
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SET LOCAL search_path=pg_catalog")
    op.execute(
        "LOCK TABLE " + storage_relation(schema, "cms_native_input_revision") + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    op.execute(
        "LOCK TABLE "
        + storage_relation(schema, "provider_directory_cms_serving_receipt")
        + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    op.execute(_counter_sql(schema))
    op.execute(_repair_sql(schema))


def downgrade():
    """Retain full transaction identity and its existing revision history."""
    raise RuntimeError("cms_archive_transaction_guard_downgrade_unsupported")
