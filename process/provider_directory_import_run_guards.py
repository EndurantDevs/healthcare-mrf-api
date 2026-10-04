# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact recognition of the frozen migration-installed ImportRun guard contract."""

import hashlib

# Body digests are derived from the existing migration constructors, with only the
# quoted schema qualifier replaced before whitespace normalization.
_FUNCTIONS = {
    "guard_npi_canonical_publication_run": {
        "source_sha256": "844e905ae49ee95f818c2332c4a55f5e5ce69a59a2f50640dcb976a24764fb5c",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": True,
        "settings": ["search_path=pg_catalog"],
    },
    "guard_provider_directory_terminal_root_retirement_run": {
        "source_sha256": "6716ee8b525eff2d51f055b180a00b88660802852df1dd290dfd3e655e5c01fa",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": True,
        "settings": ["search_path=pg_catalog"],
    },
    "provider_directory_terminal_root_run_retired": {
        "source_sha256": "a7c311976b6bdd1fa4eae11baea177e2e2b3cd48d8d355c26082de5b11627bc1",
        "argument_types": "text",
        "argument_names": ["candidate_run_id"],
        "return_type": "boolean",
        "language": "sql",
        "volatility": "s",
        "security_definer": True,
        "settings": ["search_path=pg_catalog"],
    },
    "ptg_import_wave_abandonment_run_guard": {
        "source_sha256": "1d5faba46a4b13537fced07ab65e05098e42470a133fb7a74454644582769c9d",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": False,
        "settings": None,
    },
    "ptg_import_wave_abandonment_truncate_guard": {
        "source_sha256": "6f0ba1fee747c1e27b76bdb6c5967357af3ac316d6e560d10afbc31964a9a0cf",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": False,
        "settings": None,
    },
    "ptg_import_wave_materialized_run_guard": {
        "source_sha256": "b69a89f2081608118a86f6bf584b1e84c9fda3a5ff1a88dbdc6bd99c1e791bad",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": False,
        "settings": None,
    },
    "ptg_import_wave_materialized_write_isolation_guard": {
        "source_sha256": "ffa1174a85191b76f320b025c62a99a2938ba2e0a3647823b23c9c0dffff8944",
        "argument_types": "boolean, text[], text[], text[]",
        "argument_names": ["is_v5_retirement", "candidate_run_ids", "candidate_wave_ids", "candidate_wave_digests"],
        "return_type": "void",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": False,
        "settings": None,
    },
    "ptg_import_wave_retired_run_guard": {
        "source_sha256": "bc0eb7b5878847cd8431500d916ae7f6ea6982e581d7b4670979f6af91fff456",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": False,
        "settings": None,
    },
    "ptg_import_wave_v12_abandoned_run_guard": {
        "source_sha256": "d2e8d73667ea6e08481a1e59fe71ad3b401b3051aeabab142d61550dbb0c8c7f",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": False,
        "settings": None,
    },
    "ptg_import_wave_v12_abandoned_truncate_guard": {
        "source_sha256": "81e39ec812921cdcca39acb1bdb168622afe6c1b268162fba27b6dfd28bd4f17",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": False,
        "settings": None,
    },
    "ptg_import_wave_v13_abandoned_run_guard": {
        "source_sha256": "bf6f924eb4b97fb89d29d5ff0fa0368920e61de6c6615f778db000daf700ef90",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": False,
        "settings": None,
    },
    "ptg_import_wave_v13_abandoned_truncate_guard": {
        "source_sha256": "d9b05fda29c0d2802e3790fd04b3852390bad45d85ba49cdf2af8d2efd666f95",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": False,
        "settings": None,
    },
    "ptg_wave_ordinary_terminal_run_immutable_guard": {
        "source_sha256": "e5d44e8c3c84e3d1918477a4327a22046070156dd5e30e87643787511d06f1ee",
        "argument_types": "",
        "argument_names": None,
        "return_type": "trigger",
        "language": "plpgsql",
        "volatility": "v",
        "security_definer": False,
        "settings": None,
    },
}

_TRIGGERS = {
    "ptg_wave_retired_run_guard": (23, "ptg_import_wave_retired_run_guard"),
    "ptg_import_wave_materialized_retired_run_guard": (31, "ptg_import_wave_materialized_run_guard"),
    "npi_canonical_publication_import_run_guard": (27, "guard_npi_canonical_publication_run"),
    "ptg_import_wave_abandonment_run_guard": (31, "ptg_import_wave_abandonment_run_guard"),
    "import_run_abandonment_truncate_guard": (34, "ptg_import_wave_abandonment_truncate_guard"),
    "ptg_import_wave_v12_abandoned_run_guard": (31, "ptg_import_wave_v12_abandoned_run_guard"),
    "import_run_v12_abandoned_truncate_guard": (34, "ptg_import_wave_v12_abandoned_truncate_guard"),
    "ptg_wave_ordinary_terminal_run_guard": (27, "ptg_wave_ordinary_terminal_run_immutable_guard"),
    "ptg_import_wave_v13_abandoned_run_guard": (31, "ptg_import_wave_v13_abandoned_run_guard"),
    "import_run_v13_abandoned_truncate_guard": (34, "ptg_import_wave_v13_abandoned_truncate_guard"),
    "pd_trr_import_run_row": (31, "guard_provider_directory_terminal_root_retirement_run"),
    "pd_trr_import_run_truncate": (34, "guard_provider_directory_terminal_root_retirement_run"),
}


_FUNCTION_CATALOG = """
SELECT p.oid::bigint AS function_oid,n.nspname AS function_schema,p.proname AS function_name,
       p.prosrc AS source,oidvectortypes(p.proargtypes) AS argument_types,p.proargnames AS argument_names,
       format_type(p.prorettype,NULL) AS return_type,l.lanname AS language,p.provolatile::text AS volatility,
       p.prosecdef AS security_definer,p.proconfig AS settings,p.prokind::text AS kind,
       p.proretset AS returns_set,p.proisstrict AS strict,p.proleakproof AS leakproof,
       p.proparallel::text AS parallel,p.pronargdefaults AS default_count,
       p.proargmodes IS NULL AS no_argument_modes,p.proallargtypes IS NULL AS no_output_arguments,
       p.probin IS NULL AS no_binary,p.prosqlbody IS NULL AS no_sql_body,p.prosupport=0 AS no_support,
       p.procost::float8 AS cost,p.prorows::float8 AS rows
FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
WHERE n.nspname=:schema AND p.proname=ANY(CAST(:names AS text[])) ORDER BY p.proname,p.oid
"""
_TRIGGER_CATALOG = """
SELECT t.tgname AS trigger_name,t.tgtype AS event_mask,t.tgenabled::text AS enabled,
       t.tgdeferrable AS deferrable,t.tginitdeferred AS initially_deferred,t.tgnargs AS argument_count,
       t.tgqual IS NULL AS no_condition,t.tgattr::text AS update_columns,
       encode(t.tgargs,'hex') AS arguments,t.tgoldtable IS NULL AS no_old_table,t.tgnewtable IS NULL AS no_new_table,
       t.tgconstraint=0 AS no_constraint,t.tgfoid::bigint AS function_oid,
       n.nspname AS function_schema,p.proname AS function_name
FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace
WHERE t.tgrelid=:relation_oid AND NOT t.tgisinternal ORDER BY t.tgname
"""
_FUNCTION_DEFAULTS = {
    "kind": "f",
    "returns_set": False,
    "strict": False,
    "leakproof": False,
    "parallel": "u",
    "default_count": 0,
    "no_argument_modes": True,
    "no_output_arguments": True,
    "no_binary": True,
    "no_sql_body": True,
    "no_support": True,
    "cost": 100.0,
    "rows": 0.0,
}
_TRIGGER_DEFAULTS = {
    "enabled": "A",
    "deferrable": False,
    "initially_deferred": False,
    "argument_count": 0,
    "no_condition": True,
    "update_columns": "",
    "arguments": "",
    "no_old_table": True,
    "no_new_table": True,
    "no_constraint": True,
}


def _source_hash(source, schema):
    quoted_schema = '"' + schema.replace('"', '""') + '".'
    normalized = " ".join(source.replace(quoted_schema, '"__import_guard_schema__".').split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _fail():
    raise RuntimeError("provider_directory_profile_capacity_import_run_guard_shape_changed")


async def assert_import_run_guards(database, relation_map, captured=None):
    """Validate all migration guards and return their complete dependency fingerprint entries."""
    schema = relation_map["schema_name"]
    functions = [
        dict(catalog_row._mapping)
        for catalog_row in await database.all(_FUNCTION_CATALOG, schema=schema, names=list(_FUNCTIONS))
    ]
    if len(functions) != len(_FUNCTIONS) or {catalog_row["function_name"] for catalog_row in functions} != set(
        _FUNCTIONS
    ):
        _fail()
    functions_by_name = {}
    for catalog_row in functions:
        function_source = catalog_row.pop("source")
        catalog_row["source_sha256"] = _source_hash(function_source, schema)
        expected = _FUNCTIONS[catalog_row["function_name"]] | _FUNCTION_DEFAULTS | {"function_schema": schema}
        if any(catalog_row[name] != expected_value for name, expected_value in expected.items()):
            _fail()
        catalog_row["raw_source_sha256"] = hashlib.sha256(function_source.encode("utf-8")).hexdigest()
        catalog_row["dependency_kind"] = "function"
        functions_by_name[catalog_row["function_name"]] = catalog_row
    triggers = [
        dict(catalog_row._mapping)
        for catalog_row in await database.all(_TRIGGER_CATALOG, relation_oid=relation_map["relation_oid"])
    ]
    if len(triggers) != len(_TRIGGERS) or {catalog_row["trigger_name"] for catalog_row in triggers} != set(_TRIGGERS):
        _fail()
    if captured is not None and (
        len(captured) != len(_TRIGGERS) or {catalog_row["trigger_name"] for catalog_row in captured} != set(_TRIGGERS)
    ):
        _fail()
    captured_by_name = {catalog_row["trigger_name"]: catalog_row for catalog_row in captured or ()}
    for catalog_row in triggers:
        event_mask, function_name = _TRIGGERS[catalog_row["trigger_name"]]
        function = functions_by_name[function_name]
        expected = _TRIGGER_DEFAULTS | {
            "event_mask": event_mask,
            "function_name": function_name,
            "function_schema": schema,
            "function_oid": function["function_oid"],
        }
        if any(catalog_row[name] != expected_value for name, expected_value in expected.items()):
            _fail()
        if captured is not None:
            prior = captured_by_name[catalog_row["trigger_name"]]
            if (
                prior["tgtype"] != event_mask
                or prior["trigger_enabled"] != "A"
                or prior["trigger_function_oid"] != function["function_oid"]
                or _source_hash(prior["trigger_function_source"], schema) != function["source_sha256"]
            ):
                _fail()
        catalog_row["dependency_kind"] = "trigger"
    return tuple(functions + triggers)
