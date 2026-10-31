# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin fixed validator reads and physical storage without collecting new descendants."""

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_control_custody as controls
from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_cms_preparation_policy as policy_checks
from process import provider_directory_source_acquisition_custody as acquisition

CONTRACT = "cms-preparation-custody-evidence-v3"
CONTROL_ROOTS = (
    "provider_directory_dataset_proof_shard",
    "provider_directory_bulk_acquisition_checkpoint",
    "provider_directory_bulk_output_checkpoint",
    "provider_directory_dataset_rehydration_checkpoint",
    "provider_directory_pagination_checkpoint",
)
HISTORY_ROOTS = acquisition.HISTORY_ROOTS[:2]
PROVENANCE_ROOT = acquisition.METADATA_ROOTS[0]
# Exact PG18 deparser output from the existing source model expression constructors.
_HISTORY_CHECKS = {
    "provider_directory_uhc_flex_practitioner_work": {
        "pd_uhc_flex_practitioner_work_state_check": (
            "CHECK (status::text = 'pending'::text AND attempt_count >= 0 AND lease_token IS NULL AND "
            "lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND result_sha256 IS NULL AND "
            "resource_count IS NULL AND error_code IS NULL AND terminal_record_sha256 IS NULL AND terminal_at "
            "IS NULL OR status::text = 'leased'::text AND attempt_count > 0 AND lease_token IS NOT NULL AND "
            "lease_expires_at IS NOT NULL AND lease_heartbeat_at IS NOT NULL AND result_sha256 IS NULL AND "
            "resource_count IS NULL AND error_code IS NULL AND terminal_record_sha256 IS NULL AND terminal_at "
            "IS NULL OR (status::text = ANY (ARRAY['matched'::character varying, 'unmatched'::character "
            "varying]::text[])) AND attempt_count > 0 AND lease_token IS NULL AND lease_expires_at IS NULL AND "
            "lease_heartbeat_at IS NULL AND result_sha256 IS NOT NULL AND resource_count >= 0 AND error_code IS"
            " NULL AND terminal_record_sha256 IS NOT NULL AND terminal_at IS NOT NULL OR status::text = "
            "'error'::text AND attempt_count > 0 AND lease_token IS NULL AND lease_expires_at IS NULL AND "
            "lease_heartbeat_at IS NULL AND result_sha256 IS NULL AND resource_count = 0 AND error_code::text ~"
            " '^[a-z][a-z0-9_]{0,127}$'::text AND terminal_record_sha256 IS NOT NULL AND terminal_at IS NOT "
            "NULL)"
        ),
        "pd_uhc_flex_practitioner_work_value_check": (
            "CHECK (npi >= 1000000000 AND npi <= '2999999999'::bigint AND attempt_count >= 0 AND (lease_token "
            "IS NULL OR lease_token::text ~ '^[0-9a-f]{64}$'::text) AND (result_sha256 IS NULL OR "
            "result_sha256::text ~ '^[0-9a-f]{64}$'::text) AND (terminal_record_sha256 IS NULL OR "
            "terminal_record_sha256::text ~ '^[0-9a-f]{64}$'::text))"
        ),
    },
    "provider_directory_uhc_flex_practitioner_resource": {
        "pd_uhc_flex_practitioner_resource_value_check": (
            "CHECK (npi >= 1000000000 AND npi <= '2999999999'::bigint AND attempt > 0 AND resource_id::text ~ "
            "'^[A-Za-z0-9.-]{1,64}$'::text AND payload_sha256::text ~ '^[0-9a-f]{64}$'::text AND "
            "octet_length(payload_json_text) >= 2 AND octet_length(payload_json_text) <= 1048576)"
        ),
    },
}
ROOTS = (
    "provider_directory_source",
    "provider_directory_api_endpoint",
    "provider_directory_rooted_graph_acquisition",
    "provider_directory_rooted_graph_dataset",
    "provider_directory_rooted_graph_resource",
    "provider_directory_rooted_graph_twin_admission",
    "provider_directory_rooted_graph_twin_attempt",
    "provider_directory_rooted_graph_work",
    "provider_directory_uhc_flex_npi_cohort",
    "provider_directory_uhc_flex_practitioner_acquisition",
    "provider_directory_uhc_flex_practitioner_dataset",
    "provider_directory_uhc_flex_practitioner_resource",
    "provider_directory_uhc_flex_practitioner_twin_admission",
    "provider_directory_uhc_flex_practitioner_work",
    *CONTROL_ROOTS,
    PROVENANCE_ROOT,
)
# Stored calls have no pg_depend edge. These pins cover the existing body installer,
# not ImportRun writes or the complete worker terminal lifecycle.
_RETIREMENT_CALLEES = {
    "provider_directory_terminal_root_retirement_valid": ("text",),
    "provider_directory_terminal_root_retirement_v2_valid": ("text",),
    "guard_provider_directory_terminal_root_retirement_run": (),
    "provider_directory_terminal_root_run_retired": ("text",),
}
# These source-typed calls have no pg_depend edge; no observed names are adopted.
_CALLBACK_CALLEES = {
    "pd_uhc_flex_practitioner_terminal_set_sha256": ("text",),
    "provider_directory_reviewed_subset_activation_valid": ("text",),
    "provider_directory_rooted_graph_dataset_ready": ("text",),
    "provider_directory_rooted_graph_dataset_valid": ("text",),
    "provider_directory_rooted_graph_edge_set_sha256": ("text",),
    "provider_directory_rooted_graph_official_lineage_current": ("text",),
    "provider_directory_rooted_graph_request_failure_coverage": ("text",),
    "provider_directory_rooted_graph_resource_set_sha256": ("text",),
    "provider_directory_rooted_graph_sha256": ("text",),
    "provider_directory_rooted_graph_terminal_set_sha256": ("text",),
    "provider_directory_uhc_flex_practitioner_dataset_ready": ("text",),
    "ptg_wave_canonical_json_ascii_v1": ("jsonb",),
    "ptg_wave_json_ascii_text_v1": ("text",),
    "public_evidence_npi_valid": ("text",),
}
_CALLBACK_OVERLOADS = {"ptg_wave_canonical_json_ascii_v1": (("json",), ("jsonb",))}
_CALLBACK_CALLEES_SQL = """SELECT expected.signature,p.oid::bigint,p.prokind='f' AS function,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(overloaded.prokind::text,
 overloaded.proargtypes::text,overloaded.prorettype::text,overloaded.pronargs,
 overloaded.pronargdefaults,overloaded.proargdefaults IS NULL,overloaded.provariadic::text,
 overloaded.proargmodes IS NULL,overloaded.proallargtypes IS NULL,overloaded.proretset)
 ORDER BY overloaded.proargtypes::text,overloaded.oid)
 FROM pg_catalog.pg_proc overloaded
 WHERE overloaded.pronamespace=p.pronamespace AND overloaded.proname=p.proname),'[]'::jsonb) AS overloads
 FROM unnest($1::text[]) WITH ORDINALITY expected(signature,position)
 LEFT JOIN pg_catalog.pg_proc p ON p.oid=pg_catalog.to_regprocedure(expected.signature)
 ORDER BY expected.position"""
_CHILD_UNION = (
    " UNION SELECT child.inhrelid FROM pg_catalog.pg_inherits child JOIN family parent ON parent.oid=child.inhparent\n"
)
# Keep raw constraints, including FK clones; an unqualified changing clone refuses.
_PARENTS_SQL = custody._CATALOG_SQL.replace(_CHILD_UNION, "")
# Ordinary writes change TOAST statistics; retain its structural catalog fields.
_PHYSICAL_SQL = """WITH roots AS (
 SELECT oid,reltype,reltoastrelid,relowner,relacl FROM pg_catalog.pg_class WHERE oid=ANY($1::oid[])
), toast_objects AS (
 SELECT r.oid AS root,c.* FROM roots r JOIN pg_catalog.pg_class c ON c.oid=r.reltoastrelid
 UNION ALL
 SELECT r.oid AS root,c.* FROM roots r JOIN pg_catalog.pg_index i ON i.indrelid=r.reltoastrelid
 JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
)
SELECT r.oid::bigint,jsonb_build_object(
 'relfilenode',pg_catalog.pg_relation_filenode(r.oid)::bigint,
 'table_acl',COALESCE((SELECT jsonb_agg(jsonb_build_array(acl.grantee::bigint,acl.grantor::bigint,
 acl.privilege_type,acl.is_grantable) ORDER BY acl.grantee,acl.grantor,acl.privilege_type,acl.is_grantable)
 FROM pg_catalog.aclexplode(COALESCE(r.relacl,pg_catalog.acldefault('r',r.relowner))) acl),'[]'::jsonb),
 'column_acl',COALESCE((SELECT jsonb_agg(jsonb_build_array(a.attnum,acl.grantee::bigint,acl.grantor::bigint,
 acl.privilege_type,acl.is_grantable) ORDER BY a.attnum,acl.grantee,acl.grantor,acl.privilege_type,acl.is_grantable)
 FROM pg_catalog.pg_attribute a CROSS JOIN LATERAL pg_catalog.aclexplode(a.attacl) acl
 WHERE a.attrelid=r.oid AND a.attnum>0),'[]'::jsonb),
 'rowtypes',COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_catalog.pg_type t
 WHERE t.oid=r.reltype OR t.oid=(SELECT typarray FROM pg_catalog.pg_type WHERE oid=r.reltype)),'[]'::jsonb),
 'toast_oid',r.reltoastrelid::bigint,
 'toast_classes',COALESCE((SELECT jsonb_agg(to_jsonb(c)-ARRAY['root','relpages','reltuples',
 'relallvisible','relallfrozen','relfrozenxid','relminmxid'] ORDER BY c.oid)
 FROM toast_objects c WHERE c.root=r.oid),'[]'::jsonb),
 'toast_attributes',COALESCE((SELECT jsonb_agg(to_jsonb(a)-'attstattarget' ORDER BY a.attnum)
 FROM pg_catalog.pg_attribute a WHERE a.attrelid=r.reltoastrelid AND a.attnum>0),'[]'::jsonb),
 'toast_indexes',COALESCE((SELECT jsonb_agg(to_jsonb(i) ORDER BY i.indexrelid)
 FROM pg_catalog.pg_index i WHERE i.indrelid=r.reltoastrelid),'[]'::jsonb),
 'toast_constraints',COALESCE((SELECT jsonb_agg(to_jsonb(k) ORDER BY k.oid)
 FROM pg_catalog.pg_constraint k WHERE k.conrelid=r.reltoastrelid),'[]'::jsonb),
 'toast_triggers',COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid)
 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=r.reltoastrelid),'[]'::jsonb),
 'toast_rules',COALESCE((SELECT jsonb_agg(to_jsonb(w) ORDER BY w.oid)
 FROM pg_catalog.pg_rewrite w WHERE w.ev_class=r.reltoastrelid),'[]'::jsonb),
 'toast_policies',COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY p.oid)
 FROM pg_catalog.pg_policy p WHERE p.polrelid=r.reltoastrelid),'[]'::jsonb)
) AS native FROM roots r ORDER BY r.oid"""
_ROUTINE_ACL_SQL = """ 'acl',COALESCE((SELECT jsonb_agg(jsonb_build_array(acl.grantee::bigint,acl.grantor::bigint,
 acl.privilege_type,acl.is_grantable) ORDER BY acl.grantee,acl.grantor,acl.privilege_type,acl.is_grantable)
 FROM pg_catalog.aclexplode(COALESCE(p.proacl,pg_catalog.acldefault('f',p.proowner))) acl),'[]'::jsonb),
"""
_ROUTINES_SQL = custody._ROUTINES_SQL.replace(
    " 'body',", _ROUTINE_ACL_SQL + " 'namespace_identifier',pg_catalog.quote_ident(n.nspname),\n 'body',", 1
)
_HISTORY_CHECKS_SQL = """SELECT k.conrelid::bigint AS oid,jsonb_agg(jsonb_build_array(
 k.oid::bigint,k.conname,pg_catalog.pg_get_constraintdef(k.oid,true)) ORDER BY k.oid) AS checks
 FROM pg_catalog.pg_constraint k WHERE k.conrelid=ANY($1::oid[]) AND k.contype='c'
 GROUP BY k.conrelid ORDER BY k.conrelid"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_dependency_policy_" + code)


def _layout_select(statement):
    return (
        "(SELECT COALESCE(jsonb_agg(to_jsonb(checked)),'[]'::jsonb) FROM ("
        + statement.replace(":relation", "target.name")
        + ") checked)"
    )


_LAYOUT_SQL = (
    "SELECT target.name,jsonb_build_object("
    "'parent'," + _layout_select(acquisition._ROOT_SQL) + ","
    "'columns'," + _layout_select(controls._COLUMN_SQL) + ","
    "'indexes',"
    + _layout_select(
        controls._INDEX_SQL.replace(
            'i.indisunique AS "unique",', 'i.indexprs IS NULL AS expression_free,i.indisunique AS "unique",'
        )
    )
    + ","
    "'partial'," + _layout_select(acquisition._PARTIAL_INDEX_SQL) + ") AS layout "
    "FROM unnest($1::text[]) WITH ORDINALITY target(name,position) ORDER BY target.position"
)


async def _parents(connection, schema):
    """Return exactly the fixed raw parents; this is never a descendant census."""
    parents = [
        {**dict(entry), "native": custody._decoded(entry["native"])}
        for entry in await connection.fetch(_PARENTS_SQL, schema, list(ROOTS))
    ]
    _require(
        len(parents) == len(ROOTS)
        and {entry["relname"] for entry in parents} == set(ROOTS)
        and len({entry["oid"] for entry in parents}) == len(ROOTS),
        "parents_changed",
    )
    _require(
        all(
            type(entry["oid"]) is int
            and 0 < entry["oid"] < 2**32
            and entry["nspname"] == schema
            and entry["relkind"] == ("p" if entry["relname"] in acquisition.BULK_ROOTS else "r")
            and entry["relpersistence"] == "p"
            and not entry["native"]["parents"]
            and entry["native"]["relation"][5] is False
            for entry in parents
        ),
        "parent_shape_changed",
    )
    return sorted(parents, key=lambda entry: entry["oid"])


def _models():
    from db.models.provider_directory_rooted_graph_publication import ProviderDirectoryRootedGraphDataset
    from db.models.provider_directory_uhc_flex_practitioner_publication import (
        ProviderDirectoryUHCFlexPractitionerDataset,
    )

    models_by_name = {**controls._models(), **acquisition._models()}
    for model in (ProviderDirectoryRootedGraphDataset, ProviderDirectoryUHCFlexPractitionerDataset):
        models_by_name[model.__tablename__] = model
    return {name: models_by_name[name] for name in ROOTS if name in models_by_name}


def _control_columns(root):
    """Expose only existing canonical control/plan source column contracts."""
    from process import provider_directory_source_checkpoint_catalog as checkpoints
    from process import provider_directory_source_proof_control as proofs

    if root == proofs.PROOF_ROOT:
        return [[name, kind, True, "now()" if name == "created_at" else None] for name, kind in proofs._COLUMNS]
    if root in checkpoints.CHECKPOINT_LAYOUTS:
        return [list(column) for column in checkpoints.CHECKPOINT_LAYOUTS[root]["columns"]]
    _require(root == PROVENANCE_ROOT, "control_root_changed")
    return [
        ["singleton", "boolean", True, None],
        *[
            [name, "oid", name not in {"parent_oid", "work_parent_oid"}, None]
            for name in ("acquisition_oid", "work_oid", "resource_oid", "parent_oid", "work_parent_oid")
        ],
    ]


def _constraint_catalog(parent):
    return {
        "constraints": [
            {**native, "constraint_definition": definition} for native, definition in parent["native"]["constraints"]
        ],
        "attributes": [
            {**column["attribute"], "attrelid": parent["oid"], "default_expression": column["default"]}
            for column in parent["native"]["columns"]
        ],
    }


async def _migration_plan(connection, schema, parents):
    parent = next(parent for parent in parents if parent["relname"] == PROVENANCE_ROOT)
    layouts = await connection.fetch(_LAYOUT_SQL, [storage_relation(schema, PROVENANCE_ROOT)])
    _require(
        len(layouts) == 1 and layouts[0]["name"] == storage_relation(schema, PROVENANCE_ROOT), "plan_layout_changed"
    )
    layout = custody._decoded(layouts[0]["layout"])
    _require(
        len(layout["parent"]) == 1
        and layout["parent"][0]["oid"] == parent["oid"]
        and layout["parent"][0]["kind"] == "r"
        and layout["parent"][0]["persistence"] == "p"
        and layout["parent"][0]["rls"] is False
        and layout["parent"][0]["options"] is None
        and not parent["native"]["rules"],
        "plan_layout_changed",
    )
    acquisition._require_plan_columns(PROVENANCE_ROOT, layout["columns"])
    _require(
        all(
            native["convalidated"] and native.get("conenforced", True)
            for native, _definition in parent["native"]["constraints"]
        ),
        "plan_layout_changed",
    )
    acquisition._require_metadata_layout(_constraint_catalog(parent), parent)
    await _native_layouts(connection, [parent["oid"]])
    return await _read_migration_plan(connection, schema)


async def _read_migration_plan(connection, schema):
    rows = await connection.fetch(
        "SELECT to_jsonb(r)||jsonb_build_object('acquisition_oid',r.acquisition_oid::bigint,"
        "'work_oid',r.work_oid::bigint,'resource_oid',r.resource_oid::bigint,"
        "'parent_oid',r.parent_oid::bigint,'work_parent_oid',r.work_parent_oid::bigint) AS body FROM "
        + storage_relation(schema, PROVENANCE_ROOT)
        + " r"
    )
    return [custody._decoded(row["body"]) for row in rows]


def _require_history_shape(history, parent, schema):
    _require(
        history["nspname"] == schema
        and history["relkind"] == "r"
        and history["relpersistence"] == "p"
        and history["native"]["parents"] in ([], [parent["oid"]])
        and history["native"]["relation"][5] is bool(history["native"]["parents"])
        and history["native"]["relation"][6:8] == [False, False]
        and not history["native"]["rules"],
        "history_shape_changed",
    )
    _require(
        all(
            native["contype"] in {"p", "u", "c", "n"} and native["convalidated"] and native.get("conenforced", True)
            for native, _definition in history["native"]["constraints"]
        ),
        "history_constraints_changed",
    )


async def _history_relations(connection, schema, parents, plan):
    histories = [
        {**dict(history), "native": custody._decoded(history["native"])}
        for history in await connection.fetch(_PARENTS_SQL, schema, list(HISTORY_ROOTS))
    ]
    _require(
        len(histories) == 2
        and {history["relname"] for history in histories} == set(HISTORY_ROOTS)
        and len({history["oid"] for history in histories}) == 2,
        "histories_changed",
    )
    _require(
        not {history["oid"] for history in histories}.intersection(parent["oid"] for parent in parents),
        "history_identity_changed",
    )
    by_name = {history["relname"]: history for history in (*parents, *histories)}
    acquisition._require_practitioner_migration_plan(plan, by_name)
    _require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhparent=ANY($1::oid[]))",
            sorted(history["oid"] for history in histories),
        )
        is True,
        "history_children_changed",
    )
    for history_name, root in zip(HISTORY_ROOTS, acquisition.BULK_ROOTS[:2], strict=True):
        history = by_name[history_name]
        _require_history_shape(history, by_name[root], schema)
        scope = acquisition._partition_values(history["native"]["relation"][11])
        _require((scope is not None) is bool(history["native"]["parents"]), "history_parent_changed")
    return sorted(histories, key=lambda history: history["oid"])


async def _history_scopes(connection, schema, histories):
    for history in histories:
        scope = acquisition._partition_values(history["native"]["relation"][11])
        if scope is None:
            _require(
                await connection.fetchval(
                    "SELECT NOT EXISTS(SELECT FROM " + storage_relation(schema, history["relname"]) + ")"
                )
                is True,
                "detached_history_nonempty",
            )
        else:
            found = await connection.fetch(
                "SELECT acquisition_id FROM "
                + storage_relation(schema, acquisition.ACQUISITION_ROOTS[2])
                + " WHERE acquisition_id=ANY($1::text[]) ORDER BY acquisition_id",
                scope,
            )
            _require([entry["acquisition_id"] for entry in found] == sorted(scope), "history_scope_changed")


async def _history_layouts(connection, schema, histories):
    await _native_layouts(connection, sorted(row["oid"] for row in histories))
    models = acquisition._models()
    names = [storage_relation(schema, row["relname"]) for row in histories]
    layouts = await connection.fetch(_LAYOUT_SQL, names)
    _require(len(layouts) == 2 and [row["name"] for row in layouts] == names, "history_layout_changed")
    for history, selected in zip(histories, layouts, strict=True):
        root = history["relname"].removesuffix("_legacy")
        layout = custody._decoded(selected["layout"])
        _require(len(layout["parent"]) == 1 and layout["parent"][0]["oid"] == history["oid"], "history_layout_changed")
        controls._require_model_columns(models[root], layout["columns"])
        acquisition._require_parent_indexes(root, models[root], layout["indexes"], layout["partial"])
        catalog = _constraint_catalog(history)
        formatted = await connection.fetch(
            "SELECT a.attname,pg_catalog.pg_get_expr(d.adbin,d.adrelid) AS expression FROM pg_catalog.pg_attribute a "
            "LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
            "WHERE a.attrelid=$1::oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
            history["oid"],
        )
        defaults_by_name = {row["attname"]: row["expression"] for row in formatted}
        catalog["attributes"] = [
            {**attribute, "default_expression": defaults_by_name[attribute["attname"]]}
            for attribute in catalog["attributes"]
            if not attribute["attisdropped"]
        ]
        acquisition._require_defaults(root, catalog, history)


async def _history_checks(connection, histories):
    check_rows = await connection.fetch(_HISTORY_CHECKS_SQL, sorted(check_result["oid"] for check_result in histories))
    _require(
        [check_result["oid"] for check_result in check_rows]
        == sorted(check_result["oid"] for check_result in histories),
        "history_checks_changed",
    )
    by_oid = {check_result["oid"]: check_result for check_result in histories}
    for check_result in check_rows:
        history = by_oid[check_result["oid"]]
        checks = custody._decoded(check_result["checks"])
        expected = _HISTORY_CHECKS[history["relname"].removesuffix("_legacy")]
        native_by_oid = {
            int(entry["oid"]): entry
            for entry, _definition in history["native"]["constraints"]
            if entry["contype"] == "c"
        }
        _require(
            len(checks) == len(native_by_oid) and {check[0] for check in checks} == native_by_oid.keys(),
            "history_checks_changed",
        )
        _require(all(native_by_oid[check[0]]["conname"] == check[1] for check in checks), "history_checks_changed")
        actual_by_name = {check[1]: _normalized_expression(check[2]) for check in checks}
        scope = actual_by_name.pop("pd_uhc_flex_legacy_scope", None)
        _require(
            scope is not None
            and all(entry["convalidated"] and entry.get("conenforced", True) for entry in native_by_oid.values()),
            "history_checks_changed",
        )
        scope_values = acquisition._partition_values(history["native"]["relation"][11]) or []
        acquisition._require_acquisition_scope(scope, scope_values)
        _require(
            actual_by_name == {name: _normalized_expression(expression) for name, expression in expected.items()},
            "history_checks_changed",
        )


def _proof_foreign(parents, canonical):
    proof = next(row for row in parents if row["relname"] == CONTROL_ROOTS[0])
    header = next(row for row in canonical if row["relname"] == "provider_directory_endpoint_dataset")
    foreign_keys = [entry for entry, _definition in proof["native"]["constraints"] if entry["contype"] == "f"]
    _require(
        len(foreign_keys) == 1
        and int(foreign_keys[0]["confrelid"]) == header["oid"]
        and foreign_keys[0]["conkey"] == foreign_keys[0]["confkey"] == [1]
        and foreign_keys[0]["convalidated"]
        and not foreign_keys[0]["condeferrable"]
        and not foreign_keys[0]["condeferred"]
        and foreign_keys[0]["confupdtype"] == "a"
        and foreign_keys[0]["confdeltype"] == "c"
        and foreign_keys[0]["confmatchtype"] == "s",
        "proof_foreign_changed",
    )


def _history_hooks(histories, parents, routines):
    functions_by_name = {entry["native"]["definition"]["proname"]: entry["native"] for entry in routines}
    for row in [*histories, *(parent for parent in parents if parent["relname"] == PROVENANCE_ROOT)]:
        is_history = row["relname"] in HISTORY_ROOTS
        root = row["relname"].removesuffix("_legacy")
        contracts = (
            acquisition._member_hook_contracts(root, attached=bool(row["native"]["parents"]), history=True)
            if is_history
            else acquisition._root_hook_contracts()[PROVENANCE_ROOT]
        )
        catalog = _constraint_catalog(row)
        catalog["triggers"] = [
            {**hook, "tgattr": [int(number) for number in hook["tgattr"].split()]}
            if type(hook["tgattr"]) is str
            else hook
            for hook in row["native"]["triggers"]
        ]
        acquisition._require_hooks(catalog, contracts, functions_by_name, inherited=bool(row["native"]["parents"]))


def _header_indexes(model, indexes, partial):
    """Validate the two source-defined header predicates before common key checks."""
    partial_by_name = {entry["name"]: entry for entry in partial}
    specification_by_name = {
        specification["name"]: specification
        for specification in model.__my_additional_indexes__
        if "where" in specification
    }
    _require(
        len(partial_by_name) == len(partial) and set(partial_by_name) == set(specification_by_name),
        "header_index_changed",
    )
    for name, specification in specification_by_name.items():
        native = partial_by_name[name]
        _require(
            native["ready"] is True
            and native["unique"] is bool(specification.get("unique"))
            and native["columns"] == list(specification["index_elements"])
            and native["predicate"] == specification["where"],
            "header_index_changed",
        )
    controls._require_model_indexes(
        model,
        [
            {**entry, "plain": entry["expression_free"]} if entry["name"] in specification_by_name else entry
            for entry in indexes
        ],
    )


def _require_layout(parent, model, layout):
    _require(len(layout["parent"]) == 1 and layout["parent"][0]["oid"] == parent["oid"], "parent_replaced")
    native = layout["parent"][0]
    controls._require_model_columns(model, layout["columns"])
    name = parent["relname"]
    if name in acquisition.ACQUISITION_ROOTS:
        acquisition_column = next(
            (position for position, column in enumerate(layout["columns"], 1) if column["name"] == "acquisition_id"),
            None,
        )
        acquisition._require_parent(name, native, acquisition_column)
        acquisition._require_parent_indexes(name, model, layout["indexes"], layout["partial"])
    else:
        _require(
            native["kind"] == "r"
            and native["persistence"] == "p"
            and native["rls"] is False
            and native["options"] is None
            and native["strategy"] is native["partition_columns"] is native["expressions"] is None,
            "parent_shape_changed",
        )
        if name.endswith("_dataset"):
            _header_indexes(model, layout["indexes"], layout["partial"])
        else:
            _require(not layout["partial"], "parent_index_changed")
            controls._require_model_indexes(model, layout["indexes"])


async def _layouts(connection, schema, parents):
    models_by_name = _models()
    modeled_parents = [parent for parent in parents if parent["relname"] in models_by_name]
    names = [storage_relation(schema, parent["relname"]) for parent in modeled_parents]
    layouts = await connection.fetch(_LAYOUT_SQL, names)
    _require(len(layouts) == len(modeled_parents) and [entry["name"] for entry in layouts] == names, "layouts_changed")
    for parent, entry in zip(modeled_parents, layouts, strict=True):
        _require_layout(parent, models_by_name[parent["relname"]], custody._decoded(entry["layout"]))


async def _control_layouts(connection, schema, parents):
    """Check canonical layouts without borrowing isolated Source role authority."""
    from process import provider_directory_source_checkpoint_catalog as checkpoints
    from process import provider_directory_source_proof_control as proofs
    from process.plan_release_storage_catalog import CATALOG_SQL

    parents_by_name = {parent["relname"]: parent for parent in parents if parent["relname"] in CONTROL_ROOTS}
    catalog_rows = await connection.fetch(
        CATALOG_SQL,
        [storage_relation(schema, root) for root in CONTROL_ROOTS],
        parents_by_name[CONTROL_ROOTS[0]]["relowner"],
        [],
    )
    _require(
        len(catalog_rows) == len(parents_by_name) == len(CONTROL_ROOTS)
        and {relation["oid"] for relation in catalog_rows} == {parent["oid"] for parent in parents_by_name.values()},
        "control_relations_changed",
    )
    for relation in catalog_rows:
        _require(
            relation["relname"] in parents_by_name
            and relation["oid"] == parents_by_name[relation["relname"]]["oid"]
            and relation["nspname"] == schema,
            "control_relations_changed",
        )
        if relation["relname"] == proofs.PROOF_ROOT:
            _require(
                relation["relkind"] == "r"
                and relation["relpersistence"] == "p"
                and not relation["parents"]
                and not relation["relispartition"]
                and not relation["relrowsecurity"]
                and not relation["relforcerowsecurity"]
                and relation["native_checks"],
                "proof_shape_changed",
            )
            proofs._require_proof_layout(relation)
        else:
            checkpoints._require_layout(relation, schema)
    await _native_layouts(connection, sorted(parent["oid"] for parent in parents_by_name.values()))


async def _native_layouts(connection, oids):
    from process import provider_directory_source_checkpoint_catalog as checkpoints

    native_sql = checkpoints._NATIVE_SQL.strip().removeprefix("SELECT").replace("$1::oid", "target.oid")
    native = await connection.fetch(
        "SELECT target.oid,(" + native_sql + ") AS valid FROM unnest($1::oid[]) target(oid) ORDER BY target.oid",
        oids,
    )
    _require(
        [relation["oid"] for relation in native] == oids and all(relation["valid"] is True for relation in native),
        "storage_native_changed",
    )


def _control_hook_contracts(root, names):
    from process import provider_directory_source_checkpoint_catalog as checkpoints
    from process import provider_directory_source_proof_control as proofs

    if root == proofs.PROOF_ROOT:
        transitions_by_event = {4: (None, "new_rows"), 16: ("old_rows", "new_rows"), 8: ("old_rows", None)}
        return {
            name: (event, False, False, *transitions_by_event.get(event, (None, None)), function)
            for name, (event, function) in proofs.PROOF_HOOKS.items()
        }
    expected = checkpoints._GUARDS[root]
    if root == checkpoints.BULK_ROOT and names == set(expected) - checkpoints.RETIRED_BULK_HOOKS:
        return {name: contract for name, contract in expected.items() if name not in checkpoints.RETIRED_BULK_HOOKS}
    return expected


def _control_hooks(parents, routines):
    by_oid = {entry["oid"]: entry["native"] for entry in routines}
    for parent in parents:
        if parent["relname"] not in CONTROL_ROOTS:
            continue
        hooks = [hook for hook in parent["native"]["triggers"] if not hook["tgisinternal"]]
        expected = _control_hook_contracts(parent["relname"], {hook["tgname"] for hook in hooks})
        _require(
            len(hooks) == len(expected) and {hook["tgname"] for hook in hooks} == expected.keys(),
            "control_hooks_changed",
        )
        for hook in hooks:
            function = by_oid.get(int(hook["tgfoid"]))
            _require(
                function is not None and function["namespace"][:2] == parent["native"]["namespace"][:2],
                "control_function_changed",
            )
            invocation_parts = tuple(
                hook[field] for field in ("tgtype", "tgdeferrable", "tginitdeferred", "tgoldtable", "tgnewtable")
            )
            _require(
                (*invocation_parts, function["definition"]["proname"]) == expected[hook["tgname"]]
                and hook["tgenabled"] == "A"
                and hook["tgnargs"] == 0
                and hook["tgargs"] == "\\x"
                and hook["tgattr"] in ("", [])
                and hook["tgqual"] is None
                and hook["tgparentid"] == "0",
                "control_hooks_changed",
            )


async def _routines(connection, schema, oids, original):
    frontier_rows = [
        *await custody._callee_frontier(connection, schema),
        *await _retirement_frontier(connection, schema),
        *await _callback_frontier(connection, schema),
    ]
    routines = [
        {"oid": entry["oid"], "native": custody._decoded(entry["native"])}
        for entry in await connection.fetch(_ROUTINES_SQL, oids, [entry["oid"] for entry in frontier_rows])
    ]
    routines_by_oid = {entry["oid"]: entry for entry in routines}
    legacy_by_oid = {
        entry["oid"]: {
            **entry,
            "native": {
                key: field_value
                for key, field_value in entry["native"].items()
                if key not in {"acl", "namespace_identifier"}
            },
        }
        for entry in routines
    }
    _require(
        len(routines_by_oid) == len(routines)
        and {entry["oid"] for entry in frontier_rows} <= routines_by_oid.keys()
        and all(legacy_by_oid.get(entry["oid"]) == entry for entry in original)
        and [
            *await custody._callee_frontier(connection, schema),
            *await _retirement_frontier(connection, schema),
            *await _callback_frontier(connection, schema),
        ]
        == frontier_rows,
        "routines_changed",
    )
    return routines


async def _retirement_frontier(connection, schema):
    signatures = [
        storage_relation(schema, name) + "(" + ",".join("pg_catalog." + kind for kind in arguments) + ")"
        for name, arguments in _RETIREMENT_CALLEES.items()
    ]
    rows = [dict(row) for row in await connection.fetch(custody._CALLEES_SQL, signatures)]
    _require(
        len(rows) == len(signatures)
        and [row["signature"] for row in rows] == signatures
        and all(row["oid"] is not None and row["function"] is True and row["overloads"] == 1 for row in rows)
        and len({row["oid"] for row in rows}) == len(signatures),
        "retirement_callees_missing_or_ambiguous",
    )
    return rows


def _callback_shapes(name, arguments):
    from process.provider_directory_source_callable_custody import _CONTRACTS, _TYPE_OIDS

    shapes = []
    for kinds in _CALLBACK_OVERLOADS.get(name, (arguments,)):
        spec = _CONTRACTS[name + "(" + ",".join(kinds) + ")"]
        _require(tuple(kind for _argument, kind in spec[0]) == kinds and not spec[2], "callback_contract_changed")
        shapes.append(
            [
                "f",
                " ".join(_TYPE_OIDS[kind] for kind in kinds),
                _TYPE_OIDS[spec[1]],
                len(kinds),
                0,
                True,
                "0",
                True,
                True,
                False,
            ]
        )
    return sorted(shapes)


async def _callback_frontier(connection, schema):
    """Resolve fourteen exact roots and refuse unsupported same-name signatures."""
    signatures = [
        storage_relation(schema, name) + "(" + ",".join("pg_catalog." + kind for kind in arguments) + ")"
        for name, arguments in _CALLBACK_CALLEES.items()
    ]
    rows = [dict(row) for row in await connection.fetch(_CALLBACK_CALLEES_SQL, signatures)]
    _require(
        len(rows) == len(signatures)
        and [row["signature"] for row in rows] == signatures
        and all(type(row["oid"]) is int and 0 < row["oid"] < 2**32 and row["function"] is True for row in rows)
        and len({row["oid"] for row in rows}) == len(signatures),
        "callback_callees_missing_or_ambiguous",
    )
    for row, (name, arguments) in zip(rows, _CALLBACK_CALLEES.items(), strict=True):
        _require(custody._decoded(row["overloads"]) == _callback_shapes(name, arguments), "callback_overloads_changed")
    return rows


async def _physical(connection, relation_oids):
    """Capture row types and dependent storage for exactly the fixed relation set."""
    _require(
        type(relation_oids) is list
        and all(type(oid) is int and 0 < oid < 2**32 for oid in relation_oids)
        and relation_oids == sorted(set(relation_oids)),
        "physical_identity_required",
    )
    rows = [
        {"oid": row["oid"], "native": custody._decoded(row["native"])}
        for row in await connection.fetch(_PHYSICAL_SQL, relation_oids)
    ]
    _require([row["oid"] for row in rows] == relation_oids, "physical_relations_changed")
    return rows


async def _collect(connection, storage_binding, policy, retained_oids):
    """Extend v2 read and physical evidence without changing its fixed roots."""
    observed = await policy_checks._collect_objects(connection, storage_binding, policy, retained_oids)
    schema = storage_binding.catalog_schema
    parents, histories, plan = await _dependency_relations(connection, schema, observed["canonical"]["relations"])
    oids = sorted(
        {entry["oid"] for entry in (*observed["canonical"]["relations"], *observed["private"], *parents, *histories)}
    )
    physical = await _physical(
        connection, sorted({entry["oid"] for entry in (*observed["canonical"]["relations"], *parents, *histories)})
    )
    routines = await _routines(connection, schema, oids, observed["routines"])
    _control_hooks(parents, routines)
    _history_hooks(histories, parents, routines)
    roles = custody._decoded(
        await connection.fetchval(
            custody._ROLES_SQL,
            oids,
            [schema, storage_binding.storage_schema],
            [entry["oid"] for entry in routines],
            [policy.owner_role, policy.publisher_role, *policy.runtime_roles],
        )
    )
    return {
        "canonical": {
            **observed["canonical"],
            "dependency_relations": parents,
            "dependency_histories": histories,
            "dependency_migration_plan": {PROVENANCE_ROOT: plan},
            "physical": physical,
        },
        "private": observed["private"],
        "routines": routines,
        "roles": roles,
    }


async def _dependency_relations(connection, schema, canonical):
    parents = await _parents(connection, schema)
    await connection.execute(
        "LOCK TABLE "
        + ",".join("ONLY " + storage_relation(schema, parent["relname"]) for parent in parents)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    await _layouts(connection, schema, parents)
    await _control_layouts(connection, schema, parents)
    _proof_foreign(parents, canonical)
    plan = await _migration_plan(connection, schema, parents)
    histories = await _history_relations(connection, schema, parents, plan)
    await connection.execute(
        "LOCK TABLE "
        + ",".join("ONLY " + storage_relation(schema, relation["relname"]) for relation in histories)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    await _history_layouts(connection, schema, histories)
    await _history_checks(connection, histories)
    await _history_scopes(connection, schema, histories)
    _require(await _parents(connection, schema) == parents, "parents_changed")
    _require(await _read_migration_plan(connection, schema) == plan, "migration_plan_changed")
    _require(
        [
            {**dict(relation), "native": custody._decoded(relation["native"])}
            for relation in await connection.fetch(_PARENTS_SQL, schema, list(HISTORY_ROOTS))
        ]
        == histories,
        "histories_changed",
    )
    return parents, histories, plan
