# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve existing source transition checks with exact argument types."""

from alembic import op
from db.plan_release_storage import plan_release_schema, storage_identifier, storage_relation

revision = "20261016030000_cms_source_guard_argument_types"
down_revision = "20261016010000_cms_registry_source_pair_slot"
branch_labels = None
depends_on = None

# Frozen predecessor and successor bodies, canonicalized only for the quoted schema.
_CONTRACTS = (
    (
        "cms_serving_dataset_transition",
        "c0a744d4a503d5ab4584b243b5e10e7584261c292af4ae465f92fd566adcb726",
        "64da00faca333a3e97821a2bceeaaf6900618f22a8119de54b08f78109367522",
    ),
    (
        "cms_serving_source_transition",
        "21fbd7cca43ca5ed7f4d66d39061cfcfd046af895ea27c0f1f7b33ca7940d2c2",
        "a54cf5a5ba64a12d78832923b8482bd2fc266058f2b80d2438b861ad96b43de6",
    ),
)


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def _calls(schema):
    qualified = storage_identifier(schema) + ".cms_serving_require_fresh"
    return qualified + "('source',NULL)", qualified + "('source'::text,NULL::jsonb)"


def _repair_sql(schema):
    old_call, new_call = _calls(schema)
    contracts = ",".join("(" + ",".join(_literal(field) for field in contract) + ")" for contract in _CONTRACTS)
    return f"""DO $migration$
    DECLARE expected record; original pg_proc%ROWTYPE; updated pg_proc%ROWTYPE;
        body_hash text; replacement text; definition text;
    BEGIN
        IF (SELECT count(*) FROM pg_proc WHERE pronamespace=to_regnamespace({_literal(schema)})
            AND proname IN ('cms_serving_dataset_transition','cms_serving_source_transition'))<>2 THEN
            RAISE EXCEPTION 'cms_source_guard_function_set_changed';
        END IF;
        FOR expected IN SELECT * FROM (VALUES {contracts}) AS contracts(name,previous_hash,next_hash) LOOP
            SELECT * INTO STRICT original FROM pg_proc
                WHERE pronamespace=to_regnamespace({_literal(schema)}) AND proname=expected.name;
            IF original.prokind<>'f' OR original.prorettype<>'trigger'::regtype
                OR original.prolang<>(SELECT oid FROM pg_language WHERE lanname='plpgsql')
                OR original.pronargs<>0 OR original.pronargdefaults<>0 OR original.provariadic<>0
                OR original.proargnames IS NOT NULL OR original.proargmodes IS NOT NULL
                OR original.proallargtypes IS NOT NULL OR original.proargdefaults IS NOT NULL
                OR original.protrftypes IS NOT NULL OR original.probin IS NOT NULL
                OR original.prosqlbody IS NOT NULL OR original.prosupport<>0
                OR original.prosecdef OR original.proleakproof OR original.proisstrict OR original.proretset
                OR original.provolatile<>'v' OR original.proparallel<>'u'
                OR original.procost<>100 OR original.prorows<>0
                OR original.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[] THEN
                RAISE EXCEPTION 'cms_source_guard_definition_changed';
            END IF;
            body_hash:=encode(sha256(convert_to(replace(original.prosrc,
                {_literal(storage_identifier(schema))},'"__schema__"'),'UTF8')),'hex');
            IF body_hash=expected.previous_hash THEN
                IF (length(original.prosrc)-length(replace(original.prosrc,{_literal(old_call)},'')))
                    /length({_literal(old_call)})<>1 THEN
                    RAISE EXCEPTION 'cms_source_guard_dispatch_changed';
                END IF;
                replacement:=replace(original.prosrc,{_literal(old_call)},{_literal(new_call)});
                definition:=pg_get_functiondef(original.oid);
                EXECUTE replace(definition,{_literal(old_call)},{_literal(new_call)});
            ELSIF body_hash=expected.next_hash THEN
                replacement:=original.prosrc;
            ELSE
                RAISE EXCEPTION 'cms_source_guard_body_changed';
            END IF;
            SELECT * INTO STRICT updated FROM pg_proc WHERE oid=original.oid;
            IF (to_jsonb(updated)-'prosrc') IS DISTINCT FROM (to_jsonb(original)-'prosrc')
                OR updated.prosrc IS DISTINCT FROM replacement
                OR encode(sha256(convert_to(replace(updated.prosrc,
                    {_literal(storage_identifier(schema))},'"__schema__"'),'UTF8')),'hex')<>expected.next_hash THEN
                RAISE EXCEPTION 'cms_source_guard_replacement_changed';
            END IF;
        END LOOP;
        IF (SELECT count(*) FROM pg_proc WHERE pronamespace=to_regnamespace({_literal(schema)})
            AND proname IN ('cms_serving_dataset_transition','cms_serving_source_transition'))<>2 THEN
            RAISE EXCEPTION 'cms_source_guard_function_set_changed';
        END IF;
    END $migration$"""


def upgrade():
    """Keep both hooks and function identities; change only their two dispatch expressions."""
    schema = plan_release_schema()
    storage_identifier(schema)
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SET LOCAL search_path=pg_catalog")
    op.execute(
        "LOCK TABLE "
        + ",".join(
            storage_relation(schema, name)
            for name in (
                "provider_directory_endpoint_dataset",
                "provider_directory_source",
            )
        )
        + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    op.execute(_repair_sql(schema))


def downgrade():
    """Do not restore unknown-argument dispatch in a writable schema."""
    raise RuntimeError("cms_source_guard_argument_types_downgrade_unsupported")
