# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Physical input generations and existing write guards for a validated rooted predecessor."""

import json

from db.migration_generic_source_candidates import _canonical_copy_guard_bodies, _cms_canonical_copy_guard_body
from db.migration_provider_directory_dataset_candidates import candidate_functions
from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_cms_callable_custody import _ORIGINAL_ALLOCATION_GUARDS
from process.provider_directory_generic_source_release_checks import _payload_hooks
from process.provider_directory_rooted_graph_legacy_validation import require
from process.provider_directory_source_acquisition_hooks import (
    _bulk_parent_hooks,
    _member_hook_contracts,
    _require_hooks,
)
from process.provider_directory_source_callable_custody import _GUARD_BODIES, _is_approved_body

_INPUTS = (
    "provider_directory_dataset_resource",
    "provider_directory_uhc_flex_practitioner_dataset_resource",
    "provider_directory_uhc_flex_practitioner_resource",
    "provider_directory_uhc_flex_practitioner_work",
)
_FENCE_SQL = """WITH selected AS (SELECT unnest(CAST(:oids AS oid[])) AS oid)
SELECT jsonb_build_object(
 'tables',(SELECT jsonb_agg(jsonb_build_object(
   'oid',c.oid::bigint,'schema',n.nspname,'name',c.relname,'kind',c.relkind::text,
   'owner',c.relowner::bigint,'filenode',pg_relation_filenode(c.oid)::bigint,
   'rls',c.relrowsecurity OR c.relforcerowsecurity,'persistence',c.relpersistence::text,
   'partition',c.relispartition,'bounds',pg_get_expr(c.relpartbound,c.oid),
   'parents',ARRAY(SELECT jsonb_build_object('oid',i.inhparent::bigint,'xmin',i.xmin::text,
      'pending',i.inhdetachpending) FROM pg_inherits i WHERE i.inhrelid=c.oid ORDER BY i.inhseqno),
   'attributes',ARRAY(SELECT jsonb_build_object('attnum',a.attnum,'attname',a.attname,
      'type',a.atttypid::bigint,'typmod',a.atttypmod,'collation',a.attcollation::bigint,
      'not_null',a.attnotnull,'generated',a.attgenerated,'dropped',a.attisdropped,'xmin',a.xmin::text)
      FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 ORDER BY a.attnum),
   'keys',ARRAY(SELECT jsonb_build_object('oid',k.oid::bigint,'kind',k.contype::text,
      'definition',pg_get_constraintdef(k.oid),'valid',k.convalidated)
      FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype IN ('p','u') ORDER BY k.oid)) ORDER BY c.oid)
   FROM selected s JOIN pg_class c ON c.oid=s.oid JOIN pg_namespace n ON n.oid=c.relnamespace),
 'triggers',(SELECT jsonb_agg(jsonb_build_object('oid',t.oid::bigint,'relation',t.tgrelid::bigint,
   'tgname',t.tgname,'tgtype',t.tgtype,'tgenabled',t.tgenabled::text,'xmin',t.xmin::text,
   'tgfoid',t.tgfoid::text,'tgargs','\\x'||encode(t.tgargs,'hex'),'tgattr',t.tgattr::smallint[],
   'tgparentid',t.tgparentid::text,'tgdeferrable',t.tgdeferrable,'tginitdeferred',t.tginitdeferred,
   'tgqual',t.tgqual::text,'tgnargs',t.tgnargs,'tgisinternal',t.tgisinternal,
   'tgoldtable',t.tgoldtable,'tgnewtable',t.tgnewtable) ORDER BY t.oid)
   FROM pg_trigger t JOIN selected s ON s.oid=t.tgrelid WHERE NOT t.tgisinternal),
 'functions',(SELECT jsonb_agg(jsonb_build_object('definition',(to_jsonb(p)-'prosrc')||jsonb_build_object('prosrc',encode(sha256(convert_to(p.prosrc,'UTF8')),'hex')),'xmin',p.xmin::text,
    'language',l.lanname __BODY_FIELD__) ORDER BY p.oid) FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
    WHERE p.oid IN (SELECT t.tgfoid FROM pg_trigger t JOIN selected s ON s.oid=t.tgrelid WHERE NOT t.tgisinternal)),
 'control_epochs',__CONTROL_EPOCHS__,
 'origin',current_setting('session_replication_role')='origin',
 'bypass_absent',NOT EXISTS(SELECT FROM pg_roles actor WHERE actor.rolcanlogin AND NOT actor.rolsuper
   AND EXISTS(SELECT FROM pg_roles writer WHERE
     (pg_has_role(actor.oid,writer.oid,'USAGE') OR pg_has_role(actor.oid,writer.oid,'SET'))
     AND EXISTS(SELECT FROM selected s WHERE has_table_privilege(writer.oid,s.oid,'INSERT,UPDATE,DELETE,TRUNCATE')
         OR has_any_column_privilege(writer.oid,s.oid,'INSERT,UPDATE')))
   AND EXISTS(SELECT FROM pg_roles bypass WHERE
     (pg_has_role(actor.oid,bypass.oid,'USAGE') OR pg_has_role(actor.oid,bypass.oid,'SET'))
     AND has_parameter_privilege(bypass.oid,'session_replication_role','SET'))))"""


def _payload_guard_contract(root, *, history):
    """Reuse the complete existing canonical parent/history callback inventory."""
    contracts_by_name = {}
    for name, specification in _payload_hooks(root, history=history).items():
        function, mask, mode, old, new, arguments, columns = specification
        decoded = tuple(part.decode() for part in arguments.rstrip(b"\0").split(b"\0")) if arguments else ()
        contracts_by_name[name] = (function, mask, decoded, new, columns, mode, old, None)
    return contracts_by_name


def _authenticate_functions(document, schema):
    """Reuse the reviewed callable bodies and the exact existing deny/canonical guards."""
    canonical = (
        *_canonical_copy_guard_bodies(schema, "hp_snapshot_retention"),
        _cms_canonical_copy_guard_body(schema, "hp_snapshot_retention"),
    )
    deny = (
        candidate_functions(schema)
        .split(f"CREATE FUNCTION {storage_identifier(schema)}.deny_pd_dataset_candidate_write()", 1)[1]
        .split("AS $fn$", 1)[1]
        .split("$fn$;", 1)[0]
    )
    by_name = {}
    for function in document["functions"] or []:
        definition_by_field = {**function["definition"], "prosrc": function["body"]}
        name = definition_by_field["proname"]
        expected = _GUARD_BODIES.get(name + "()") or _ORIGINAL_ALLOCATION_GUARDS.get(name)
        is_approved = (
            expected is not None
            and definition_by_field["prosecdef"] is expected[1]
            and _is_approved_body(definition_by_field, schema, expected[0])
        )
        if name == "guard_pd_dataset_candidate_parent":
            is_approved = definition_by_field["prosrc"] in canonical and definition_by_field["prosecdef"] is True
        if name == "deny_pd_dataset_candidate_write":
            is_approved = definition_by_field["prosrc"] == deny and definition_by_field["prosecdef"] is False
        require(is_approved and function["language"] == "plpgsql")
        require(
            definition_by_field["prokind"] == "f"
            and definition_by_field["pronargs"] == 0
            and int(definition_by_field["prorettype"]) == 2279
            and definition_by_field["proisstrict"] is False
            and definition_by_field["proleakproof"] is False
            and definition_by_field["proparallel"] == "u"
        )
        require(definition_by_field["proconfig"] == ["search_path=pg_catalog"])
        by_name[name] = function
    return by_name


def _authenticate_hooks(document, roots_by_name, functions):
    """Authenticate both parent-targeted and direct-leaf INSERT/UPDATE/DELETE paths."""
    by_oid = {table["oid"]: table for table in document["tables"]}
    for root, relation_oids in roots_by_name.items():
        for oid in relation_oids:
            table = by_oid[oid]
            require(table["kind"] in {"r", "p"} and table["persistence"] == "p" and table["rls"] is False)
            hooks = [hook for hook in document["triggers"] or [] if hook["relation"] == oid]
            if root in _INPUTS[:2]:
                contracts_by_name = _payload_guard_contract(root, history=table["name"] != root)
            elif table["name"] == root:
                contracts_by_name = _bulk_parent_hooks(root)
            else:
                contracts_by_name = _member_hook_contracts(
                    root, attached=table["partition"], history=table["name"] == root + "_legacy"
                )
            _require_hooks(
                {"triggers": hooks, "attributes": table["attributes"]},
                contracts_by_name,
                functions,
                inherited=table["partition"],
            )


async def capture_input_fence(database, schema, leaves, dataset_id):
    """Observe only selected physical input generations and their complete existing write guards."""
    roots_by_name = {}
    for root in _INPUTS:
        root_oid = await database.scalar(
            "SELECT to_regclass(:relation)::oid::bigint", relation=storage_relation(schema, root)
        )
        require(type(root_oid) is int)
        roots_by_name[root] = {root_oid}
    for root, oids in leaves.items():
        roots_by_name[root].update(oids)
    selected = sorted(set().union(*roots_by_name.values()))
    encoded = await database.scalar(
        input_fence_query(schema, ":dataset_id", "CAST(:oids AS oid[])", include_bodies=True),
        oids=selected,
        dataset_id=dataset_id,
    )
    document = json.loads(encoded) if type(encoded) is str else encoded
    require(type(document) is dict and document["origin"] is True and document["bypass_absent"] is True)
    require({table["oid"] for table in document["tables"]} == set(selected))
    functions = _authenticate_functions(document, schema)
    _authenticate_hooks(document, roots_by_name, functions)
    for function in document["functions"]:
        function.pop("body")
    return json.dumps(
        {"roots": {root: sorted(oids) for root, oids in roots_by_name.items()}, "catalog": document},
        sort_keys=True,
        separators=(",", ":"),
    )


def input_fence_query(schema, dataset_sql, oids_sql, *, include_bodies=False):
    """Render the same bounded catalog and control-row epoch projection for native consumers."""
    header = storage_relation(schema, "provider_directory_uhc_flex_practitioner_dataset")
    parent = storage_relation(schema, "provider_directory_endpoint_dataset")
    acquisition = storage_relation(schema, "provider_directory_uhc_flex_practitioner_acquisition")
    epochs = f"""(SELECT jsonb_build_object('header',h.xmin::text,'parent',p.xmin::text,'candidate',a.xmin::text)
        FROM {header} h JOIN {parent} p USING(dataset_id)
        JOIN {acquisition} a ON a.acquisition_id=h.candidate_acquisition_id WHERE h.dataset_id={dataset_sql})"""
    query = (
        _FENCE_SQL.replace("CAST(:oids AS oid[])", oids_sql)
        .replace("__CONTROL_EPOCHS__", epochs)
        .replace("__BODY_FIELD__", ",'body',p.prosrc" if include_bodies else "")
    )
    # Consumers compare the already authenticated function digest, never header-provided code.
    return query


def root_input_fence_sql(schema, dataset_sql, roots_sql, *, variant="uhc_flex_practitioner"):
    """Project an attested protected predecessor's exact physical input fence as jsonb."""
    if variant == "rooted_combined":
        from db.migration_rooted_publication_freeze import CONTRACT
        from process.provider_directory_rooted_graph_terminal import (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
        )

        header = storage_relation(schema, "provider_directory_rooted_graph_dataset")
        parent = storage_relation(schema, "provider_directory_endpoint_dataset")
        return f"""SELECT jsonb_build_object('roots',({roots_sql}),'catalog',jsonb_build_object(
            'control_epochs',(SELECT jsonb_build_object('header',h.xmin::text,'parent',p.xmin::text)
                FROM {header} h JOIN {parent} p USING(dataset_id) WHERE h.dataset_id={dataset_sql}),
            'published_proof',(SELECT jsonb_build_object('preparation_id',preparation.handoff->>'preparation_id',
                'handoff_sha256',encode(sha256(convert_to(preparation.handoff::text,'UTF8')),'hex'),
                'publication_sha256',encode(sha256(convert_to(preparation.publication::text,'UTF8')),'hex'))
                FROM hp_snapshot_retention.preparation preparation JOIN {parent} p ON p.dataset_id={dataset_sql}
                WHERE preparation.handoff=p.candidate_storage_json
                  AND preparation.importer_id='provider-directory-fhir' AND preparation.state='published'
                  AND preparation.handoff->>'contract'='{CONTRACT}'
                  AND preparation.handoff->'identity'->'metadata'->>'query_filter_recipe_id'=
                    '{PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID}'
                  AND preparation.handoff->>'preparation_id'=p.candidate_storage_json->>'preparation_id'
                  AND preparation.handoff->'identity'->>'dataset_id'=p.dataset_id)))"""
    require(variant == "uhc_flex_practitioner")
    oids = f"ARRAY(SELECT DISTINCT entry::oid FROM jsonb_each({roots_sql}) root CROSS JOIN LATERAL jsonb_array_elements_text(root.value) entry ORDER BY entry::oid)"
    query = input_fence_query(schema, dataset_sql, oids)
    return f"SELECT jsonb_build_object('roots',({roots_sql}),'catalog',document) FROM ({query}) fence(document)"
