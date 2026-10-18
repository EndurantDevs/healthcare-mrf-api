# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve existing source validators using their exact declared argument types."""

from alembic import op
from db.plan_release_storage import plan_release_schema, storage_identifier, storage_relation

revision = "20261016040000_source_validator_argument_types"
down_revision = "20261016030000_cms_source_guard_argument_types"
branch_labels = None
depends_on = None

# Latest authentic constructors; only thirteen varchar-to-text call arguments change.
_CONTRACTS = (
    (
        "guard_pd_uhc_flex_practitioner_dataset_endpoint",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        "e71ad0b2cc788e5a7f032527fefdbe198a74229b45a725e05c0b890d8fc556e6",
        "4edc0f6abf9fbd66fe9ddc5f66f0a604b2bc6a90a54ae94b692025cab15f4e21",
    ),
    (
        "guard_pd_uhc_flex_practitioner_dataset_source",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        "d047a3210b63da91b2c8a7c8983e5b53e6a1abd95055aa5c6b64d8d797342b1d",
        "d6c441ab63677f9253bac748293cf061ed43269813060a79cd2b87440131b6f5",
    ),
    (
        "guard_provider_directory_reviewed_subset_activation_source",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        "6a8a6b10a1add6501fea1aa1df5c57727f16317c2de3b13e4f3799470d14a61d",
        "198b932e871554ad324276d2b3c0f34982710d4dd685e120a33c6f51ad4331c3",
    ),
    (
        "guard_provider_directory_rooted_graph_dependency",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        "6205b8bd18883eb93305b719ebcdc7c81bc4c5ef0ac90b70af89387f69e06048",
        "627aded81121718e2041ba80751bb08a5086503c91649947fa2e1a43f6f225a6",
    ),
    (
        "guard_provider_directory_subset_published_source",
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        "19db33ae97daa0002477eb473e9e94abfae52fb8f1abd2ba7789f4c224ce4538",
        "19db33ae97daa0002477eb473e9e94abfae52fb8f1abd2ba7789f4c224ce4538",
    ),
    (
        "provider_directory_reviewed_subset_activation_valid",
        "candidate_source_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        "947612992adc3cc59381f9844cdd10ac0ed189257018b71db1ed5f0c23b3cde5",
        "e45a0c892a551ef7081f6f8f9466c98cedb181037ec1ea455348229b2014ac64",
    ),
    (
        "provider_directory_rooted_graph_dataset_intrinsic_valid",
        "candidate_dataset_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        "f3f81a3ebf594b6de6a59110b637c65972c51ced3775c96be719f5261e56a6b1",
        "9fed3d575d7c41c67e54ea2d28871e19e1c355917f62ba3c8e5a83312e90c770",
    ),
    (
        "provider_directory_subset_canonical_sha256",
        "candidate",
        "jsonb",
        "text",
        "sql",
        "i",
        True,
        "s",
        "b2113ad61fc542dd81ee7afde3afe31eab2a549f0e4e983b4e422a75c5c33cb6",
        "b2113ad61fc542dd81ee7afde3afe31eab2a549f0e4e983b4e422a75c5c33cb6",
    ),
    (
        "provider_directory_subset_completion_canonical_json",
        "candidate",
        "jsonb",
        "text",
        "plpgsql",
        "i",
        True,
        "s",
        "b6109ebe0d2c8a7bdaf383ab044cd90a4c98745b4ebc41bc1e077ddcdb763e96",
        "b6109ebe0d2c8a7bdaf383ab044cd90a4c98745b4ebc41bc1e077ddcdb763e96",
    ),
    (
        "provider_directory_subset_content_proof_valid",
        "candidate,expected_dataset_id,expected_endpoint_id,expected_root_run_id,expected_source_ids,expected_selected_resources,expected_dataset_hash,expected_resource_count,expected_resource_hashes,expected_resource_counts",
        "jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb",
        "boolean",
        "plpgsql",
        "i",
        True,
        "s",
        "6ffdbd5dee49b00851284cc57051d3af0d29cefa59d28300d50b8b7ecb539113",
        "6ffdbd5dee49b00851284cc57051d3af0d29cefa59d28300d50b8b7ecb539113",
    ),
    (
        "provider_directory_uhc_flex_practitioner_dataset_valid",
        "candidate_dataset_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        "9bd066663a46371855b3e0e6382294a19daa498a58ab471d19b81c0d41cc8a1a",
        "9bd066663a46371855b3e0e6382294a19daa498a58ab471d19b81c0d41cc8a1a",
    ),
)
_REPAIRS = (
    (
        "guard_pd_uhc_flex_practitioner_dataset_endpoint",
        "provider_directory_uhc_flex_practitioner_dataset_valid",
        "header.dataset_id",
        1,
    ),
    (
        "guard_provider_directory_reviewed_subset_activation_source",
        "provider_directory_reviewed_subset_activation_valid",
        "NEW.source_id",
        4,
    ),
    (
        "guard_provider_directory_reviewed_subset_activation_source",
        "provider_directory_reviewed_subset_activation_valid",
        "affected_source.source_id",
        1,
    ),
    (
        "guard_pd_uhc_flex_practitioner_dataset_source",
        "provider_directory_uhc_flex_practitioner_dataset_valid",
        "header.dataset_id",
        1,
    ),
    (
        "guard_provider_directory_rooted_graph_dependency",
        "provider_directory_rooted_graph_dataset_intrinsic_valid",
        "dataset.dataset_id",
        1,
    ),
    (
        "provider_directory_rooted_graph_dataset_intrinsic_valid",
        "provider_directory_uhc_flex_practitioner_dataset_valid",
        "root_header.dataset_id",
        1,
    ),
)
_TYPE_OIDS = {"": "", "text": "25", "jsonb": "3802", "boolean": "16", "trigger": "2279", "bigint": "20"}


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def _contracts_sql():
    rows = []
    for name, argument, kind, returns, language, volatility, strict, parallel, previous, current in _CONTRACTS:
        argument_oids = " ".join(_TYPE_OIDS[part] for part in kind.split(","))
        fields = (name, argument, argument_oids, _TYPE_OIDS[returns], language, volatility, parallel, previous, current)
        rows.append("(" + ",".join(map(_literal, fields)) + "," + str(strict).lower() + ")")
    return (
        "(VALUES "
        + ",".join(rows)
        + ") AS contracts("
        + ("name,argument,argument_oid,return_oid,language,volatility,parallel,previous_hash,next_hash,strict)")
    )


def _calls(schema):
    calls = tuple(
        (
            name,
            storage_relation(schema, callee) + "(" + argument + ")",
            storage_relation(schema, callee) + "(" + argument + "::text)",
            count,
        )
        for name, callee, argument, count in _REPAIRS
    )
    return calls + (
        (
            "provider_directory_reviewed_subset_activation_valid",
            _content_call(schema, typed=False),
            _content_call(schema, typed=True),
            1,
        ),
    )


def _content_call(schema, *, typed):
    arguments = (
        "(policy_candidate.publication_metadata_json::jsonb -> 'provider_directory_content_proof_v1')",
        "policy_candidate.dataset_id",
        "policy_candidate.endpoint_id",
        "policy_candidate.acquisition_root_run_id",
        "policy_candidate.publication_metadata_json::jsonb -> 'source_ids'",
        "policy_candidate.publication_metadata_json::jsonb -> 'selected_resources'",
        "policy_candidate.dataset_hash",
        "policy_candidate.resource_count",
        "policy_candidate.completion_proof_json\n                -> 'dataset' -> 'resource_hashes'",
        "policy_candidate.completion_proof_json\n                -> 'dataset' -> 'resource_counts'",
    )
    if typed:
        arguments = tuple(
            argument + "::text" if index in {1, 2, 3, 6} else argument for index, argument in enumerate(arguments)
        )
    return (
        storage_relation(schema, "provider_directory_subset_content_proof_valid")
        + "(\n            "
        + ",\n            ".join(arguments)
        + "\n        )"
    )


def _validation_sql(schema):
    return f"""FOR expected IN SELECT * FROM {_contracts_sql()} LOOP
        SELECT * INTO STRICT original FROM pg_proc
            WHERE pronamespace=to_regnamespace({_literal(schema)}) AND proname=expected.name;
        IF original.prokind<>'f' OR original.prorettype::text<>expected.return_oid
            OR original.prolang<>(SELECT oid FROM pg_language WHERE lanname=expected.language)
            OR original.proargtypes::text<>expected.argument_oid
            OR original.pronargs<>(CASE WHEN expected.argument='' THEN 0
                ELSE cardinality(string_to_array(expected.argument,',')) END)
            OR original.proargnames IS DISTINCT FROM (CASE WHEN expected.argument='' THEN NULL::text[]
                ELSE string_to_array(expected.argument,',') END)
            OR original.pronargdefaults<>0 OR original.provariadic<>0
            OR original.proargmodes IS NOT NULL OR original.proallargtypes IS NOT NULL
            OR original.proargdefaults IS NOT NULL OR original.protrftypes IS NOT NULL
            OR original.probin IS NOT NULL OR original.prosqlbody IS NOT NULL OR original.prosupport<>0
            OR NOT original.prosecdef OR original.proleakproof OR original.proretset
            OR original.proisstrict<>expected."strict" OR original.procost<>100 OR original.prorows<>0
            OR original.provolatile::text<>expected.volatility OR original.proparallel::text<>expected.parallel
            OR original.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[]
            OR NOT EXISTS(SELECT FROM pg_roles WHERE oid=original.proowner) THEN
            RAISE EXCEPTION 'source_validator_definition_changed';
        END IF;
        body_hash:=encode(sha256(convert_to(replace(original.prosrc,
            {_literal(storage_identifier(schema))},'"__schema__"'),'UTF8')),'hex');
        IF body_hash NOT IN (expected.previous_hash,expected.next_hash) THEN
            RAISE EXCEPTION 'source_validator_body_changed';
        END IF;
    END LOOP;"""


def _replacement_sql(schema):
    repair_values = ",".join(
        "(" + ",".join(map(_literal, (name, old, new))) + "," + str(count) + ")"
        for name, old, new, count in _calls(schema)
    )
    return f"""FOR expected IN SELECT * FROM {_contracts_sql()} LOOP
        SELECT * INTO STRICT original FROM pg_proc
            WHERE pronamespace=to_regnamespace({_literal(schema)}) AND proname=expected.name;
        body_hash:=encode(sha256(convert_to(replace(original.prosrc,
            {_literal(storage_identifier(schema))},'"__schema__"'),'UTF8')),'hex');
        replacement:=original.prosrc;
        definition:=pg_get_functiondef(original.oid);
        IF body_hash<>expected.next_hash THEN
            FOR repair IN SELECT * FROM (VALUES {repair_values}) AS repairs(name,previous_call,next_call,occurrences)
                WHERE name=expected.name LOOP
                IF (length(replacement)-length(replace(replacement,repair.previous_call,'')))
                    /length(repair.previous_call)<>repair.occurrences THEN
                    RAISE EXCEPTION 'source_validator_dispatch_changed';
                END IF;
                replacement:=replace(replacement,repair.previous_call,repair.next_call);
                definition:=replace(definition,repair.previous_call,repair.next_call);
            END LOOP;
            EXECUTE definition;
        END IF;
        SELECT * INTO STRICT updated FROM pg_proc WHERE oid=original.oid;
        IF (to_jsonb(updated)-'prosrc') IS DISTINCT FROM (to_jsonb(original)-'prosrc')
            OR updated.prosrc IS DISTINCT FROM replacement
            OR encode(sha256(convert_to(replace(updated.prosrc,
                {_literal(storage_identifier(schema))},'"__schema__"'),'UTF8')),'hex')<>expected.next_hash THEN
            RAISE EXCEPTION 'source_validator_replacement_changed';
        END IF;
    END LOOP;"""


def _repair_sql(schema):
    storage_identifier(schema)
    names = "ARRAY[" + ",".join(_literal(entry[0]) for entry in _CONTRACTS) + "]::text[]"
    return f"""DO $migration$
    DECLARE expected record; repair record; original pg_proc%ROWTYPE; updated pg_proc%ROWTYPE;
        body_hash text; replacement text; definition text; before_catalog jsonb; after_catalog jsonb;
        before_hooks jsonb; after_hooks jsonb; before_dependencies jsonb; after_dependencies jsonb;
        function_oids oid[];
    BEGIN
        IF (SELECT count(*) FROM pg_proc WHERE pronamespace=to_regnamespace({_literal(schema)})
            AND proname=ANY({names}))<>11 THEN
            RAISE EXCEPTION 'source_validator_function_set_changed';
        END IF;
        {_validation_sql(schema)}
        SELECT array_agg(p.oid ORDER BY p.oid),jsonb_agg(to_jsonb(p)-'prosrc' ORDER BY p.oid)
            INTO function_oids,before_catalog FROM pg_proc p
            WHERE pronamespace=to_regnamespace({_literal(schema)}) AND proname=ANY({names});
        SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.oid),'[]'::jsonb) INTO before_hooks
            FROM pg_trigger t WHERE tgfoid=ANY(function_oids);
        SELECT COALESCE(jsonb_agg(to_jsonb(d) ORDER BY classid,objid,objsubid,refclassid,refobjid,refobjsubid,deptype),
            '[]'::jsonb) INTO before_dependencies FROM pg_depend d
            WHERE (classid='pg_proc'::regclass AND objid=ANY(function_oids))
               OR (refclassid='pg_proc'::regclass AND refobjid=ANY(function_oids));
        {_replacement_sql(schema)}
        SELECT jsonb_agg(to_jsonb(p)-'prosrc' ORDER BY p.oid) INTO after_catalog FROM pg_proc p
            WHERE pronamespace=to_regnamespace({_literal(schema)}) AND proname=ANY({names});
        SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.oid),'[]'::jsonb) INTO after_hooks
            FROM pg_trigger t WHERE tgfoid=ANY(function_oids);
        SELECT COALESCE(jsonb_agg(to_jsonb(d) ORDER BY classid,objid,objsubid,refclassid,refobjid,refobjsubid,deptype),
            '[]'::jsonb) INTO after_dependencies FROM pg_depend d
            WHERE (classid='pg_proc'::regclass AND objid=ANY(function_oids))
               OR (refclassid='pg_proc'::regclass AND refobjid=ANY(function_oids));
        IF before_catalog IS DISTINCT FROM after_catalog OR before_hooks IS DISTINCT FROM after_hooks
            OR before_dependencies IS DISTINCT FROM after_dependencies THEN
            RAISE EXCEPTION 'source_validator_catalog_changed';
        END IF;
    END $migration$"""


def upgrade():
    """Preserve all identities and guards while changing only exact dispatch."""
    schema = plan_release_schema()
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SET LOCAL search_path=pg_catalog")
    op.execute(
        "LOCK TABLE "
        + ",".join(
            storage_relation(schema, name)
            for name in (
                "provider_directory_source",
                "provider_directory_api_endpoint",
                "provider_directory_endpoint_dataset",
                "provider_directory_uhc_flex_practitioner_dataset",
                "provider_directory_rooted_graph_dataset",
            )
        )
        + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    op.execute(_repair_sql(schema))


def downgrade():
    """Never restore implicit dispatch in an ordinary-writable schema."""
    raise RuntimeError("source_validator_argument_types_downgrade_unsupported")
