# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed mutable CMS controls: protected DDL with explicit existing writer rights."""

import asyncio
from copy import deepcopy

from sqlalchemy.dialects import postgresql

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_cms_callable_custody import (
    _FUNCTION_SQL,
    _admin_identity,
    _body_digest,
    _decoded,
    capture_cms_callable_custody,
)
from process.provider_directory_cms_control_privileges import (
    ADDRESS_GENERATION_UPDATE,
    GEO_STATE_UPDATE,
    PROFILE_DELTA_INSERT,
    PROFILE_GENERATION_UPDATE,
    PROFILE_INITIAL_INSERT,
    cms_control_read_edges,
    cms_control_read_oids,
)
from process.provider_directory_cms_receipt_custody import _CATALOG_SQL, _require_migration_identity
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_profile_initial_custody import (
    _OWNER_USAGE_SQL,
    _role_rows,
    capture_initial_callable_custody,
)
from process.provider_directory_source_builder_delegation import (
    builder_delegation,
    issue_legacy_delegation,
    require_delegated_access,
    require_legacy_observation,
)
from process.provider_directory_source_cms_epoch_control import capture_source_cms_epoch_controls

CONTROL_ROOTS = (
    "provider_directory_source",
    "provider_directory_api_endpoint",
    "provider_directory_profile_serving_generation",
    "provider_directory_profile_initial_receipt",
    "provider_directory_profile_delta_receipt",
    "provider_directory_profile_build_checkpoint",
    "entity_address_result_generation",
    "entity_address_geo_assurance_state",
    "address_alias_artifact_state_v1",
    "reference_family_result_generation",
)
_RIGHTS = {"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}
_COLUMN_SQL = """SELECT a.attname AS name,format_type(a.atttypid,a.atttypmod) AS type,
 a.attnotnull AS required,a.attidentity::text AS identity,a.attgenerated::text AS generated
 FROM pg_attribute a WHERE a.attrelid=to_regclass(:relation)
 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"""
_INDEX_SQL = """SELECT c.relname AS name,m.amname AS method,i.indisunique AS "unique",
 i.indisprimary AS "primary",i.indisvalid AND i.indisready AND i.indislive AS ready,
 i.indexprs IS NULL AND i.indpred IS NULL AS plain,
 ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(number,position)
 JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.number ORDER BY position) AS columns
 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am m ON m.oid=c.relam
 WHERE i.indrelid=to_regclass(:relation) ORDER BY c.relname"""
_FOREIGN_SQL = """SELECT to_jsonb(k) AS "constraint",
 jsonb_build_array(child.oid::bigint,cn.nspname,child.relname,child.relowner::bigint) AS child,
 jsonb_build_array(parent.oid::bigint,pn.nspname,parent.relname,parent.relowner::bigint) AS parent,
 (SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_trigger t WHERE t.tgconstraint=k.oid) AS hooks
 FROM pg_constraint k JOIN pg_class child ON child.oid=k.conrelid
 JOIN pg_namespace cn ON cn.oid=child.relnamespace JOIN pg_class parent ON parent.oid=k.confrelid
 JOIN pg_namespace pn ON pn.oid=parent.relnamespace WHERE k.contype='f'
 AND (k.conrelid=ANY($1::oid[]) OR k.confrelid=ANY($1::oid[])) ORDER BY k.oid"""
_TYPE_SQL = """SELECT to_jsonb(t) FROM pg_type t WHERE t.typrelid=$1::oid"""
_NATIVE_DEPENDENCIES_SQL = """WITH objects(classid,objid) AS (
 SELECT 'pg_constraint'::regclass,oid FROM pg_constraint WHERE conrelid=$1::oid
 UNION SELECT 'pg_attrdef'::regclass,oid FROM pg_attrdef WHERE adrelid=$1::oid
 UNION SELECT 'pg_class'::regclass,indexrelid FROM pg_index WHERE indrelid=$1::oid)
 SELECT NOT EXISTS(SELECT FROM objects x JOIN pg_depend d ON (d.classid,d.objid)=(x.classid,x.objid)
 LEFT JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND p.oid=d.refobjid
 LEFT JOIN pg_operator o ON d.refclassid='pg_operator'::regclass AND o.oid=d.refobjid
 WHERE (p.oid IS NOT NULL AND p.pronamespace<>'pg_catalog'::regnamespace)
 OR (o.oid IS NOT NULL AND o.oprnamespace<>'pg_catalog'::regnamespace))"""
_EXTRA_COLUMNS = {
    CONTROL_ROOTS[3]: (
        ("build_id", "character varying(64)", True),
        ("attestation_id", "character varying(64)", True),
        ("generation_id", "character varying(64)", True),
        ("singleton_key", "character varying(16)", True),
        ("run_id", "character varying(64)", True),
        ("contract_id", "text", True),
        ("payload", "jsonb", True),
        ("payload_sha256", "character varying(64)", True),
        ("publication_xid", "xid8", True),
        ("committed_at", "timestamp with time zone", True),
    ),
    CONTROL_ROOTS[7]: (
        ("singleton", "boolean", True),
        ("active_geo_assurance_version", "smallint", False),
        ("active_table_oid", "oid", False),
        ("active_relation_signature", "jsonb", False),
        ("candidate_geo_assurance_version", "smallint", False),
        ("candidate_table_oid", "oid", False),
        ("candidate_relation_signature", "jsonb", False),
        ("candidate_projected_rows", "bigint", False),
        ("active_dependency_bindings", "jsonb", False),
        ("candidate_dependency_bindings", "jsonb", False),
    ),
    CONTROL_ROOTS[9]: (
        ("importer_id", "text", True),
        ("local_lineage_id", "uuid", True),
        ("local_generation", "bigint", True),
        ("origin_lineage_id", "uuid", False),
        ("origin_generation", "bigint", False),
        ("published_at", "timestamp with time zone", False),
        ("relation_oids", "bigint[]", False),
        ("source_revision_tracked", "boolean", True),
    ),
}


def _require(condition, code):
    if not condition:
        raise ValueError("cms_control_" + code)


def _models():
    from db.models import AddressAliasArtifactStateV1
    from db.models.system import (
        EntityAddressResultGeneration,
        ProviderDirectoryAPIEndpoint,
        ProviderDirectoryProfileBuildCheckpoint,
        ProviderDirectoryProfileDeltaReceipt,
        ProviderDirectoryProfileServingGeneration,
        ProviderDirectorySource,
    )

    return {
        model.__tablename__: model
        for model in (
            ProviderDirectorySource,
            ProviderDirectoryAPIEndpoint,
            ProviderDirectoryProfileServingGeneration,
            ProviderDirectoryProfileDeltaReceipt,
            ProviderDirectoryProfileBuildCheckpoint,
            EntityAddressResultGeneration,
            AddressAliasArtifactStateV1,
        )
    }


def _model_columns(model):
    column_spec_by_name = {}
    for column in model.__table__.columns:
        kind = str(column.type.compile(dialect=postgresql.dialect())).lower()
        kind = kind.replace("varchar", "character varying")
        if kind == "timestamp without time zone" or kind == "timestamp":
            kind = "timestamp without time zone"
        if kind == "json":
            kind = "jsonb"
        column_spec_by_name[column.name] = (kind, not column.nullable)
    return column_spec_by_name


def _require_model_columns(model, rows):
    column_spec_by_name = {row["name"]: (row["type"], row["required"]) for row in rows}
    _require(
        column_spec_by_name == _model_columns(model) and len(column_spec_by_name) == len(rows),
        "administrator_shape_required",
    )
    _require(all(row["identity"] == row["generated"] == "" for row in rows), "administrator_shape_required")


def _require_model_indexes(model, index_rows):
    from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint

    index_by_name = {index_row["name"]: index_row for index_row in index_rows}
    for constraint in model.__table__.constraints:
        if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint)):
            keys = [column.name for column in constraint.columns]
            _require(
                any(
                    index_row["ready"]
                    and index_row["plain"]
                    and index_row["unique"]
                    and index_row["columns"] == keys
                    and (not isinstance(constraint, PrimaryKeyConstraint) or index_row["primary"])
                    for index_row in index_rows
                ),
                "administrator_index_required",
            )
    for specification in getattr(model, "__my_additional_indexes__", ()):
        name = specification.get("name") or "_".join(specification["index_elements"])
        index_row = index_by_name.get(name)
        _require(
            index_row is not None
            and index_row["ready"]
            and index_row["plain"]
            and index_row["columns"] == list(specification["index_elements"])
            and index_row["method"] == specification.get("using", "btree")
            and index_row["unique"] is bool(specification.get("unique")),
            "administrator_index_required",
        )


async def require_cms_control_model_layout(database, model, schema):
    """Runtime schema checks are read-only; mutable DML remains supported."""
    from process.provider_directory_source_acquisition_custody import (
        ACQUISITION_ROOTS,
        require_acquisition_model_layout,
    )

    if model.__tablename__ in ACQUISITION_ROOTS:
        return await require_acquisition_model_layout(database, model, schema)
    _require(model.__tablename__ in _models() and model is _models()[model.__tablename__], "model_required")
    table = storage_relation(schema, model.__tablename__)
    rows = await database.all(_COLUMN_SQL, relation=table)
    _require_model_columns(model, [dict(row._mapping if hasattr(row, "_mapping") else row) for row in rows])
    indexes = await database.all(_INDEX_SQL, relation=table)
    _require_model_indexes(model, [dict(row._mapping if hasattr(row, "_mapping") else row) for row in indexes])


def _writer_rights(root, columns):
    """Fixed existing writers, not a role inferred from a newly observed ACL."""
    all_columns = tuple(columns)
    if root in CONTROL_ROOTS[:2]:
        return {"SELECT", "MAINTAIN", *(("DELETE",) if root == CONTROL_ROOTS[0] else ())}, {
            "INSERT": all_columns,
            "UPDATE": all_columns,
        }
    if root == CONTROL_ROOTS[2]:
        return {"SELECT", "MAINTAIN"}, {"INSERT": all_columns, "UPDATE": PROFILE_GENERATION_UPDATE}
    if root == CONTROL_ROOTS[3]:
        return {"SELECT", "MAINTAIN"}, {"INSERT": PROFILE_INITIAL_INSERT}
    if root == CONTROL_ROOTS[4]:
        return {"SELECT"}, {"INSERT": PROFILE_DELTA_INSERT, "UPDATE": ("build_id",)}
    if root == CONTROL_ROOTS[5]:
        updates = (
            "owner_run_id",
            "state",
            "last_error",
            "updated_at",
            "evidence_next_batch",
            "profile_next_batch",
            "completed_at",
            "cutover_forecast_status",
            "cutover_forecast_hash",
            "cutover_forecast_json",
        )
        return {"SELECT", "DELETE", "MAINTAIN"}, {"INSERT": all_columns, "UPDATE": updates}
    if root == CONTROL_ROOTS[6]:
        return {"SELECT"}, {"UPDATE": ADDRESS_GENERATION_UPDATE}
    if root == CONTROL_ROOTS[7]:
        return {"SELECT"}, {"INSERT": ("singleton", *GEO_STATE_UPDATE[4:]), "UPDATE": GEO_STATE_UPDATE}
    if root == CONTROL_ROOTS[8]:
        return {"SELECT"}, {"INSERT": all_columns, "UPDATE": ("generation", "updated_at")}
    _require(root == CONTROL_ROOTS[9], "writer_scope_changed")
    return {"SELECT", "MAINTAIN"}, {"UPDATE": (*ADDRESS_GENERATION_UPDATE, "source_revision_tracked")}


# Exact post-repair source graph, plus two unchanged historical control callables.
_CALLABLES = {
    "guard_pd_uhc_flex_practitioner_dataset_endpoint": (
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "4edc0f6abf9fbd66fe9ddc5f66f0a604b2bc6a90a54ae94b692025cab15f4e21",
    ),
    "guard_pd_uhc_flex_practitioner_dataset_source": (
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "d6c441ab63677f9253bac748293cf061ed43269813060a79cd2b87440131b6f5",
    ),
    "guard_provider_directory_reviewed_subset_activation_source": (
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "198b932e871554ad324276d2b3c0f34982710d4dd685e120a33c6f51ad4331c3",
    ),
    "guard_provider_directory_rooted_graph_dependency": (
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "627aded81121718e2041ba80751bb08a5086503c91649947fa2e1a43f6f225a6",
    ),
    "guard_provider_directory_subset_published_source": (
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "19db33ae97daa0002477eb473e9e94abfae52fb8f1abd2ba7789f4c224ce4538",
    ),
    "provider_directory_reviewed_subset_activation_valid": (
        "candidate_source_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        True,
        "e45a0c892a551ef7081f6f8f9466c98cedb181037ec1ea455348229b2014ac64",
    ),
    "provider_directory_rooted_graph_dataset_intrinsic_valid": (
        "candidate_dataset_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        True,
        "9fed3d575d7c41c67e54ea2d28871e19e1c355917f62ba3c8e5a83312e90c770",
    ),
    "provider_directory_subset_canonical_sha256": (
        "candidate",
        "jsonb",
        "text",
        "sql",
        "i",
        True,
        "s",
        True,
        "b2113ad61fc542dd81ee7afde3afe31eab2a549f0e4e983b4e422a75c5c33cb6",
    ),
    "provider_directory_subset_completion_canonical_json": (
        "candidate",
        "jsonb",
        "text",
        "plpgsql",
        "i",
        True,
        "s",
        True,
        "b6109ebe0d2c8a7bdaf383ab044cd90a4c98745b4ebc41bc1e077ddcdb763e96",
    ),
    "provider_directory_subset_content_proof_valid": (
        "candidate,expected_dataset_id,expected_endpoint_id,expected_root_run_id,expected_source_ids,expected_selected_resources,expected_dataset_hash,expected_resource_count,expected_resource_hashes,expected_resource_counts",
        "jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb",
        "boolean",
        "plpgsql",
        "i",
        True,
        "s",
        True,
        "6ffdbd5dee49b00851284cc57051d3af0d29cefa59d28300d50b8b7ecb539113",
    ),
    "provider_directory_uhc_flex_practitioner_dataset_valid": (
        "candidate_dataset_id",
        "text",
        "boolean",
        "sql",
        "s",
        False,
        "u",
        True,
        "9bd066663a46371855b3e0e6382294a19daa498a58ab471d19b81c0d41cc8a1a",
    ),
    "provider_directory_profile_delta_receipt_immutable_v1": (
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        False,
        "cdc932f14df3ded994c886d3a806d4dba781259914828831ff4aa002b766a7e3",
    ),
    "advance_reference_source_generation": (
        "",
        "",
        "trigger",
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "bc43708d0f74a9085b2a4ecfd2333ddec9eb4596f6cb1cb26fe56d3271db65a7",
    ),
}

_TYPE_OIDS = {"text": "25", "jsonb": "3802", "bigint": "20", "boolean": "16", "trigger": "2279"}
_PUBLIC_CALLABLES = {
    "provider_directory_uhc_flex_practitioner_dataset_valid",
    "provider_directory_profile_delta_receipt_immutable_v1",
}


def _signatures(schema):
    return tuple(storage_relation(schema, name) + "(" + spec[1] + ")" for name, spec in _CALLABLES.items())


def _require_callable_definition(entry, schema, namespace):
    definition = entry["definition"]
    arguments, kinds, returns, language, volatility, strict, parallel, security, digest = _CALLABLES[
        definition["proname"]
    ]
    expected_by_field = dict(
        prokind="f",
        prorettype=_TYPE_OIDS[returns],
        pronargs=len(kinds.split(",")) if kinds else 0,
        proargtypes=[_TYPE_OIDS[kind] for kind in kinds.split(",")] if kinds else [],
        proargnames=arguments.split(",") if arguments else None,
        proargmodes=None,
        proallargtypes=None,
        proargdefaults=None,
        pronargdefaults=0,
        provariadic="0",
        prosecdef=security,
        proleakproof=False,
        proisstrict=strict,
        proretset=False,
        prosupport="-",
        probin=None,
        prosqlbody=None,
        protrftypes=None,
        procost=100,
        prorows=0,
        provolatile=volatility,
        proparallel=parallel,
        proconfig=["search_path=pg_catalog"]
        if definition["proname"] != "provider_directory_profile_delta_receipt_immutable_v1"
        else None,
    )
    _require(
        int(definition["pronamespace"]) == namespace
        and entry["language"] == language
        and all(definition[name] == field_value for name, field_value in expected_by_field.items())
        and _body_digest(definition["prosrc"], schema) == digest,
        "callable_definition_changed",
    )


def _require_callable_acl(entry, owner, builder, *, legacy):
    previous = int(entry["definition"]["proowner"])
    _require(previous in ({owner, builder} if legacy else {owner}), "callable_owner_changed")
    expected_acl_rows = [[previous, previous, "EXECUTE", False]]
    if entry["definition"]["proname"] in _PUBLIC_CALLABLES:
        expected_acl_rows.append([0, previous, "EXECUTE", False])
    extra_acl_rows = [row for row in entry["acl"] if row not in expected_acl_rows]
    _require(
        all(
            row in ([owner, previous, "EXECUTE", False], [builder, previous, "EXECUTE", False])
            for row in extra_acl_rows
        )
        and sorted(entry["acl"]) == sorted(expected_acl_rows + extra_acl_rows),
        "callable_acl_changed",
    )


def _require_owner_reach(access, owner, publisher, migration):
    for role in access:
        if not role["superuser"] and role["role"] not in {owner, publisher, migration}:
            _require(not role["owner_reach"], "owner_authority")


async def _capture_callables(connection, policy, roles, owner, builder, migration):
    rows = await connection.fetch(
        _FUNCTION_SQL, list(_signatures(policy.schema)), policy.schema, list(_CALLABLES), list(policy.runtime_roles)
    )
    _require(
        len(rows) == len(_CALLABLES) and {row["signature"] for row in rows} == set(_signatures(policy.schema)),
        "callable_set_changed",
    )
    namespace = await connection.fetchval("SELECT to_regnamespace($1)::oid::bigint", policy.schema)
    role_ids = {int(role["oid"]) for role in roles["roles"]}
    function_by_name = {}
    for row in rows:
        function_by_field = {
            key: row[key] if key in {"signature", "language"} else _decoded(row[key]) for key in row.keys()
        }
        _require_callable_definition(function_by_field, policy.schema, namespace)
        _role_rows(function_by_field["access"], role_ids, 6)
        function_by_name[function_by_field["definition"]["proname"]] = function_by_field
    return function_by_name


def _source_hook_contracts():
    return {
        "provider_directory_subset_published_source_guard": (
            "guard_provider_directory_subset_published_source",
            29,
            "A",
            True,
        ),
        "provider_directory_subset_published_source_truncate_guard": (
            "guard_provider_directory_subset_published_source",
            34,
            "A",
            False,
        ),
        "provider_directory_reviewed_subset_activation_source_guard": (
            "guard_provider_directory_reviewed_subset_activation_source",
            29,
            "A",
            True,
        ),
        "provider_directory_reviewed_subset_source_truncate_guard": (
            "guard_provider_directory_reviewed_subset_activation_source",
            34,
            "A",
            False,
        ),
    }


def _receipt_hook_contracts():
    contracts_by_root = {root: {} for root in CONTROL_ROOTS}
    for name, mask in (("write", 26), ("truncate", 34)):
        contracts_by_root[CONTROL_ROOTS[4]][f"provider_directory_profile_delta_receipt_{name}_guard"] = (
            "provider_directory_profile_delta_receipt_immutable_v1",
            mask,
            "A",
            False,
        )
        hook = f"pd_profile_initial_receipt_{name}_guard"
        contracts_by_root[CONTROL_ROOTS[3]][hook] = (hook, mask, "A", False)
    contracts_by_root[CONTROL_ROOTS[3]]["pd_profile_initial_receipt_insert"] = (
        "pd_profile_initial_receipt_insert",
        5,
        "A",
        True,
    )
    contracts_by_root[CONTROL_ROOTS[2]]["pd_profile_initial_serving_insert"] = (
        "pd_profile_initial_serving_insert",
        21,
        "A",
        True,
    )
    return contracts_by_root


def _hook_contracts():
    """Map only authentic installed hooks; adoption preserves their native identities."""
    contracts_by_root = {root: {} for root in CONTROL_ROOTS}
    source_contracts = contracts_by_root[CONTROL_ROOTS[0]]
    source_contracts.update(_source_hook_contracts())
    for root, kind in zip(CONTROL_ROOTS[:2], ("source", "endpoint"), strict=True):
        contracts_by_root[root][f"pd_uhc_flex_dataset_{kind}_guard"] = (
            f"guard_pd_uhc_flex_practitioner_dataset_{kind}",
            25,
            "O",
            True,
        )
        contracts_by_root[root][f"pd_uhc_flex_dataset_{kind}_truncate_guard"] = (
            f"guard_pd_uhc_flex_practitioner_dataset_{kind}",
            34,
            "O",
            False,
        )
        contracts_by_root[root][f"pd_rooted_graph_{root}_dependency_guard"[:63]] = (
            "guard_provider_directory_rooted_graph_dependency",
            29,
            "A",
            True,
        )
        contracts_by_root[root]["cms_npd_covered_truncate_forbidden"] = (
            "guard_cms_npd_covered_truncate",
            34,
            "O",
            False,
        )
    for root, kind in (
        (CONTROL_ROOTS[0], "source"),
        (CONTROL_ROOTS[2], "profile"),
        (CONTROL_ROOTS[6], "address"),
        (CONTROL_ROOTS[9], "doctors"),
    ):
        name = f"cms_serving_{kind}_transition"
        contracts_by_root[root][name] = (name, 29, "O", True)
        contracts_by_root[root]["cms_serving_no_truncate"] = ("cms_serving_no_truncate", 34, "O", False)
    for root, hooks in _receipt_hook_contracts().items():
        contracts_by_root[root].update(hooks)
    return contracts_by_root


def _require_hooks(root, catalog, functions):
    hooks_by_name = {hook["tgname"]: hook for hook in catalog["triggers"] or () if not hook["tgisinternal"]}
    expected = _hook_contracts()[root]
    _require(hooks_by_name.keys() == expected.keys(), "hooks_changed")
    for name, (function, mask, enabled, deferred) in expected.items():
        hook = hooks_by_name[name]
        _require(
            hook["tgfoid"] == functions[function]["definition"]["oid"]
            and hook["tgtype"] == mask
            and hook["tgenabled"] == enabled
            and hook["tgdeferrable"] is deferred
            and hook["tginitdeferred"] is deferred
            and hook["tgqual"] is None
            and hook["tgnargs"] == 0
            and hook["tgattr"] == []
            and hook["tgargs"] == "\\x"
            and hook["tgparentid"] == "0"
            and hook["tgoldtable"] is hook["tgnewtable"] is None,
            "hooks_changed",
        )


def _require_foreign_scope(edges, roots, schema, owner, family=None):
    from process.provider_directory_generic_source_provision import CONTROL_ROOTS as source_controls

    protected_oids = {int(root["oid"]) for root in roots.values()}
    for edge in edges:
        constraint = edge["constraint"]
        _require(constraint["convalidated"] and constraint["conenforced"] and edge["hooks"], "foreign_key_changed")
        for relation in (edge["child"], edge["parent"]):
            if relation[0] not in protected_oids:
                _require(
                    relation[1] == schema
                    and relation[3] == owner
                    and relation[2] in source_controls
                    or family is not None
                    and edge in family["foreign"],
                    "external_fk_owner_required:" + relation[1] + "." + relation[2],
                )
        _require(
            all(
                hook["tgisinternal"] and hook["tgenabled"] in {"O", "A"} and hook["tgconstraint"] == constraint["oid"]
                for hook in edge["hooks"]
            ),
            "foreign_key_hooks_changed",
        )


def _require_source_endpoint_fk(edges, relations_by_root, *, legacy=False):
    """Bind the alias fence to its original native constraint and four hooks."""
    native_by_root = {
        root: next(entry for entry in relations_by_root[root]["catalog"]["classes"] if entry["relname"] == root)
        for root in CONTROL_ROOTS[:2]
    }
    column_by_root = {
        root: next(
            entry["attnum"]
            for entry in relations_by_root[root]["catalog"]["attributes"]
            if entry["attrelid"] == native["oid"] and entry["attname"] == "endpoint_id"
        )
        for root, native in native_by_root.items()
    }
    matches = [
        edge
        for edge in edges
        if edge["constraint"]["conname"] == "provider_directory_source_endpoint_id_fkey"
        and int(edge["constraint"]["conrelid"]) == int(native_by_root[CONTROL_ROOTS[0]]["oid"])
    ]
    _require(len(matches) == 1, "source_endpoint_fk_required")
    edge = matches[0]
    constraint = edge["constraint"]
    _require(
        int(constraint["confrelid"]) == int(native_by_root[CONTROL_ROOTS[1]]["oid"])
        and constraint["conkey"] == [column_by_root[CONTROL_ROOTS[0]]]
        and constraint["confkey"] == [column_by_root[CONTROL_ROOTS[1]]]
        and constraint["confmatchtype"] == "s"
        and constraint["confupdtype"] == "a"
        and constraint["confdeltype"] == "n"
        and constraint["confdelsetcols"] is None
        and not constraint["condeferrable"]
        and not constraint["condeferred"]
        and constraint["convalidated"]
        and constraint["conenforced"],
        "source_endpoint_fk_changed",
    )
    _require_source_endpoint_hooks(edge, relations_by_root, legacy=legacy)
    return edge


def _require_source_endpoint_hooks(edge, relations_by_root, *, legacy):
    """Verify the same four native hook records in both captured representations."""
    constraint = edge["constraint"]
    hooks = edge["hooks"] or ()
    child, parent = int(constraint["conrelid"]), int(constraint["confrelid"])
    _require(
        len(hooks) == 4
        and len({hook["oid"] for hook in hooks}) == 4
        and {(int(hook["tgrelid"]), hook["tgtype"]) for hook in hooks}
        == {(child, 5), (child, 17), (parent, 9), (parent, 17)}
        and all(
            hook["tgisinternal"]
            and str(hook["tgconstraint"]) == str(constraint["oid"])
            and hook["tgenabled"] in ({"O", "A"} if legacy else {"A"})
            for hook in hooks
        ),
        "source_endpoint_fk_hooks_changed",
    )
    catalog_hooks = [
        hook
        for root in CONTROL_ROOTS[:2]
        for hook in relations_by_root[root]["catalog"]["triggers"] or ()
        if str(hook["tgconstraint"]) == str(constraint["oid"])
    ]
    _require(
        sorted(catalog_hooks, key=lambda hook: int(hook["oid"])) == sorted(hooks, key=lambda hook: int(hook["oid"])),
        "source_endpoint_fk_catalog_changed",
    )


async def _enable_source_endpoint_fk(connection, policy, before):
    """Keep the existing native alias FK active in every writer's replication mode."""
    edge = _require_source_endpoint_fk(before["foreign"], before["relations"], legacy=True)
    roots_by_oid = {
        int(edge["constraint"]["conrelid"]): CONTROL_ROOTS[0],
        int(edge["constraint"]["confrelid"]): CONTROL_ROOTS[1],
    }
    for hook in sorted(edge["hooks"], key=lambda hook: (int(hook["tgrelid"]), int(hook["oid"]))):
        if hook["tgenabled"] != "A":
            await connection.execute(
                f"ALTER TABLE {storage_relation(policy.schema, roots_by_oid[int(hook['tgrelid'])])} "
                f"ENABLE ALWAYS TRIGGER {storage_identifier(hook['tgname'])}"
            )


def _expected_relation_acl(root, catalog, owner, builder):
    previous = int(next(entry for entry in catalog["classes"] if entry["relname"] == root)["relowner"])
    number_by_name = {
        entry["attname"]: entry["attnum"]
        for entry in catalog["attributes"]
        if entry["attrelid"] == next(entry for entry in catalog["classes"] if entry["relname"] == root)["oid"]
    }
    tables, selected = _writer_rights(root, number_by_name)
    table_edges = {
        (grantee, owner if grantor == previous else grantor, right, grantable)
        for grantee, grantor, right, grantable in catalog["table_acl"]
        if right == "SELECT" and grantee not in {owner, builder, previous}
    }
    column_edges = {
        (column, grantee, owner if grantor == previous else grantor, right, grantable)
        for column, grantee, grantor, right, grantable in catalog["column_acl"]
        if right == "SELECT" and grantee not in {owner, builder, previous}
    }
    table_edges |= {(owner, owner, right, False) for right in _RIGHTS}
    table_edges |= {(builder, owner, right, False) for right in tables}
    column_edges |= {
        (number_by_name[column], builder, owner, right, False) for right, names in selected.items() for column in names
    }
    return table_edges, column_edges


def _require_relation_acl(root, catalog, native, owner, builder, *, legacy, delegation=None):
    """Preserve exact explicit grants before checking native group authority."""
    previous = int(native["relowner"])
    _require(previous in ({owner, builder} if legacy else {owner}), "relation_owner_changed")
    if previous == owner:
        expected_table, expected_columns = _expected_relation_acl(root, catalog, owner, builder)
        _require(
            set(map(tuple, catalog["table_acl"])) == expected_table
            and set(map(tuple, catalog["column_acl"])) == expected_columns,
            "relation_acl_changed",
        )
    else:
        _require(
            all(
                grantor in {previous, owner}
                and (not grantable or (right == "SELECT" and grantee not in {previous, owner}))
                and (grantee in {previous, owner} or right == "SELECT")
                for grantee, grantor, right, grantable in catalog["table_acl"]
            )
            and all(
                grantor in {previous, owner}
                and (not grantable or (right == "SELECT" and grantee not in {previous, owner}))
                and (grantee in {previous, owner} or right == "SELECT")
                for _, grantee, grantor, right, grantable in catalog["column_acl"]
            ),
            "legacy_relation_acl_changed",
        )
    _require_control_effective_access(root, catalog, native, owner, builder, delegation)


def _require_control_effective_access(root, catalog, native, owner, builder, delegation):
    """Bound ordinary delegates while preserving the exact Builder grant."""
    previous = int(native["relowner"])
    tables, selected = _writer_rights(
        root,
        {entry["attname"]: entry["attnum"] for entry in catalog["attributes"] if entry["attrelid"] == native["oid"]},
    )
    number_by_right = {
        right: {
            entry["attnum"]
            for entry in catalog["attributes"]
            if entry["attname"] in names and entry["attrelid"] == native["oid"]
        }
        for right, names in selected.items()
    }
    if delegation is not None:
        require_delegated_access(
            catalog["access"],
            delegation,
            owner=owner,
            builder=builder,
            relation_oid=int(native["oid"]),
            legacy_builder=previous == builder,
        )
    for access in catalog["access"]:
        if access["superuser"] or access["role"] == owner or access["owner_reach"]:
            continue
        if access["role"] == builder and previous == owner:
            _require(set(access["table"]) == tables, "writer_effective_acl_changed")
            _require(
                all(
                    set(rights)
                    == {"SELECT", *(right for right, numbers in number_by_right.items() if column in numbers)}
                    for column, rights in access["columns"]
                ),
                "writer_effective_acl_changed",
            )
        elif delegation is None and access["role"] != builder and not access["builtin"]:
            _require(
                set(access["table"]) <= {"SELECT"}
                and all(set(rights) <= {"SELECT"} for _, rights in access["columns"]),
                "inherited_writer_authority",
            )


def _require_native_indexes(catalog):
    _require(
        all(
            entry["native"]
            and entry["method"] == "btree"
            and all(entry[name] for name in ("indisvalid", "indisready", "indislive", "indimmediate"))
            and not any(
                entry[name] for name in ("indisexclusion", "indisclustered", "indisreplident", "indnullsnotdistinct")
            )
            and entry["indnatts"] == entry["indnkeyatts"]
            and entry["indoption"] == [0] * entry["indnatts"]
            and entry["index_expressions"] == entry["index_predicate"] == ""
            for entry in catalog["indexes"] or ()
        ),
        "native_index_changed",
    )


def _require_initial_catalog(catalog, native):
    from process.provider_directory_profile_initial_guards import assert_initial_receipt_catalog

    oid = int(native["oid"])
    attributes = [
        dict(
            entry,
            relation_oid=int(entry["attrelid"]),
            atttypid=int(entry["atttypid"]),
            attcollation=int(entry["attcollation"]),
        )
        for entry in catalog["attributes"]
    ]
    indexes = [
        dict(
            entry,
            relation_oid=int(entry["indrelid"]),
            index_am=entry["method"],
            reloptions=[],
            indkey=" ".join(map(str, entry["indkey"])),
            indclass=" ".join(entry["indclass"]),
            indcollation=" ".join(entry["indcollation"]),
            indoption=" ".join(map(str, entry["indoption"])),
        )
        for entry in catalog["indexes"]
    ]
    constraints = [
        dict(entry, relation_oid=int(entry["conrelid"]), constraint_type=entry["contype"])
        for entry in catalog["constraints"]
    ]
    assert_initial_receipt_catalog(oid, attributes, indexes, constraints)


async def _capture_relation(connection, policy, root, functions, principal):
    owner, publisher, builder, migration = principal
    table = storage_relation(policy.schema, root)
    catalog = _decoded(await connection.fetchval(_CATALOG_SQL, table, owner, builder))
    _require(catalog is not None and catalog["classes"], "relations_missing")
    natives = [entry for entry in catalog["classes"] if entry["relname"] == root]
    _require(len(natives) == 1, "relations_missing")
    native = natives[0]
    _require(
        native["relkind"] == "r"
        and native["relpersistence"] == "p"
        and native["relam"] == "2"
        and native["reloptions"] is None
        and native["relreplident"] == "d"
        and not any(native[key] for key in ("relispartition", "relrowsecurity", "relforcerowsecurity"))
        and all(catalog[key] is None for key in ("rules", "policies", "inheritance")),
        "relation_layout_changed",
    )
    _require(all(entry["relowner"] == native["relowner"] for entry in catalog["classes"]), "dependent_owner_changed")
    _require(
        all(entry["relacl"] is None for entry in catalog["classes"] if entry is not native), "dependent_acl_changed"
    )
    _require_owner_reach(catalog["access"], owner, publisher, migration)
    _require_hooks(root, catalog, functions)
    _require_native_indexes(catalog)
    _require(
        all(entry["convalidated"] and entry["conenforced"] for entry in catalog["constraints"] or ()),
        "native_constraint_changed",
    )
    if root == CONTROL_ROOTS[3]:
        _require_initial_catalog(catalog, native)
    _require(
        await connection.fetchval(_NATIVE_DEPENDENCIES_SQL, int(native["oid"])) is True, "native_dependency_required"
    )
    columns = [
        dict(catalog_row) for catalog_row in await connection.fetch(_COLUMN_SQL.replace(":relation", "$1"), table)
    ]
    if root in _models():
        _require_model_columns(_models()[root], columns)
        _require_model_indexes(
            _models()[root],
            [dict(catalog_row) for catalog_row in await connection.fetch(_INDEX_SQL.replace(":relation", "$1"), table)],
        )
    else:
        _require(
            {catalog_row["name"]: (catalog_row["type"], catalog_row["required"]) for catalog_row in columns}
            == {name: (kind, required) for name, kind, required in _EXTRA_COLUMNS[root]},
            "relation_columns_changed",
        )
    composite = _decoded(await connection.fetchval(_TYPE_SQL, int(native["oid"])))
    _require(composite["typowner"] == native["relowner"] and composite["typtype"] == "c", "composite_changed")
    row_accounting = await connection.fetchrow(
        f"SELECT count(*)::bigint AS count,encode(sha256(convert_to(COALESCE(string_agg(encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex'),'' ORDER BY to_jsonb(r)::text),''),'UTF8')),'hex') AS sha256 FROM {table} r"
    )
    return {"catalog": catalog, "composite": composite, "rows": dict(row_accounting)}


async def _capture_control_relations(
    connection, policy, principal, hook_functions_by_name, *, legacy, family=None, delegation=None
):
    owner, publisher, builder, migration = principal
    relations_by_root = {
        root: await _capture_relation(
            connection, policy, root, hook_functions_by_name, (owner, publisher, builder, migration)
        )
        for root in CONTROL_ROOTS
    }
    natives_by_root = {
        root: next(entry for entry in relation["catalog"]["classes"] if entry["relname"] == root)
        for root, relation in relations_by_root.items()
    }
    for root, entry in relations_by_root.items():
        _require_relation_acl(
            root, entry["catalog"], natives_by_root[root], owner, builder, legacy=legacy, delegation=delegation
        )
    foreign_edges = [
        {key: _decoded(foreign_row[key]) for key in foreign_row.keys()}
        for foreign_row in await connection.fetch(
            _FOREIGN_SQL, [int(native["oid"]) for native in natives_by_root.values()]
        )
    ]
    _require_foreign_scope(foreign_edges, natives_by_root, policy.schema, owner, family)
    _require_source_endpoint_fk(foreign_edges, relations_by_root, legacy=legacy)
    return relations_by_root, foreign_edges


async def _capture_complete_foreign(connection, relations_by_root, family, fixed_edges):
    """Reject external constraints on every retained or private heap, not only its parent."""
    relation_by_name = {**relations_by_root, **family["relations"]}
    relation_oids = [
        int(native["oid"])
        for name, entry in relation_by_name.items()
        for native in entry["catalog"]["classes"]
        if native["relname"] == name
    ]
    foreign_edges = [
        {key: _decoded(foreign_row[key]) for key in foreign_row.keys()}
        for foreign_row in await connection.fetch(_FOREIGN_SQL, relation_oids)
    ]
    allowed_oids = {str(edge["constraint"]["oid"]) for edge in (*fixed_edges, *family["foreign"])}
    allowed_oids.update(
        str(constraint["oid"])
        for entry in family["cohort"]["relations"].values()
        for constraint in entry["catalog"]["constraints"] or ()
        if constraint["contype"] == "f"
    )
    _require(
        {str(edge["constraint"]["oid"]) for edge in foreign_edges} == allowed_oids,
        "external_foreign_authority_changed",
    )
    hook_oids = set()
    for edge in foreign_edges:
        for hook in edge["hooks"] or ():
            _require(
                hook["tgisinternal"]
                and hook["tgenabled"] in {"O", "A"}
                and str(hook["tgconstraint"]) == str(edge["constraint"]["oid"])
                and hook["tgqual"] is None
                and hook["tgnargs"] == 0
                and not hook["tgattr"],
                "native_foreign_hook_changed",
            )
            hook_oids.add(int(hook["tgfoid"]))
    await _require_native_foreign_functions(connection, hook_oids)
    return foreign_edges


async def _require_native_foreign_functions(connection, hook_oids):
    _require(
        await connection.fetchval(
            "SELECT count(*)=cardinality($1::oid[]) AND COALESCE(bool_and(p.pronamespace='pg_catalog'::regnamespace "
            "AND p.proowner=10 AND l.lanname='internal' AND NOT p.prosecdef "
            "AND p.prosrc=p.proname AND p.proname=ANY($2::text[])),true) "
            "FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=ANY($1::oid[])",
            sorted(hook_oids),
            [
                "RI_FKey_" + name
                for name in (
                    "check_ins",
                    "check_upd",
                    "noaction_del",
                    "noaction_upd",
                    "restrict_del",
                    "restrict_upd",
                    "cascade_del",
                    "cascade_upd",
                    "setnull_del",
                    "setnull_upd",
                    "setdefault_del",
                    "setdefault_upd",
                )
            ],
        )
        is True,
        "native_foreign_function_changed",
    )


async def _capture_source_history_inputs(connection, policy):
    """Reuse the existing protected SOURCE family for one-time historical input reads."""
    from db.models.provider_directory_rooted_graph_publication import ProviderDirectoryRootedGraphDataset
    from db.models.provider_directory_uhc_flex_practitioner_publication import (
        ProviderDirectoryUHCFlexPractitionerDataset,
    )
    from db.models.system import ProviderDirectoryEndpointDataset
    from process.provider_directory_generic_source_provision import verify_generic_source_family
    from process.provider_directory_generic_source_storage import HEADER

    payload_rows = [dict(source_row) for source_row in await verify_generic_source_family(connection, policy)]
    models = (
        ProviderDirectoryEndpointDataset,
        ProviderDirectoryUHCFlexPractitionerDataset,
        ProviderDirectoryRootedGraphDataset,
    )
    names = sorted(
        {*(model.__tablename__ for model in models), *(source_row["relname"] for source_row in payload_rows)}
    )
    await connection.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(policy.schema, name) for name in names)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    expected_by_name = {source_row["relname"]: source_row["oid"] for source_row in payload_rows}
    actual_by_name = {
        source_row["relname"]: source_row["oid"]
        for source_row in await connection.fetch(
            "SELECT relname,oid::bigint AS oid FROM pg_class WHERE relnamespace=to_regnamespace($1) "
            "AND relname=ANY($2::text[])",
            policy.schema,
            list(expected_by_name),
        )
    }
    _require(actual_by_name == expected_by_name, "source_history_identity_changed")
    owner = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", policy.owner_role)
    builder = await connection.fetchval(
        "SELECT oid::bigint FROM pg_roles WHERE rolname=$1", policy.control_runtime_roles[0]
    )
    headers_by_name = {
        model.__tablename__: await _capture_source_history_header(connection, policy, model, (owner, builder))
        for model in models
    }
    return dict(header=headers_by_name[HEADER], headers=headers_by_name, family=payload_rows)


async def _capture_source_history_header(connection, policy, model, principals):
    owner, builder = principals
    name = model.__tablename__
    table = storage_relation(policy.schema, name)
    catalog = _decoded(await connection.fetchval(_CATALOG_SQL, table, owner, builder))
    _require(catalog and catalog["classes"], "source_header_missing")
    native = next(row for row in catalog["classes"] if row["relname"] == name)
    _require(
        int(native["relowner"]) == owner
        and native["relkind"] == "r"
        and native["relpersistence"] == "p"
        and not native["relrowsecurity"]
        and not native["relforcerowsecurity"]
        and catalog["rules"] is catalog["policies"] is catalog["inheritance"] is None,
        "source_header_changed",
    )
    columns = [dict(row) for row in await connection.fetch(_COLUMN_SQL.replace(":relation", "$1"), table)]
    _require_model_columns(model, columns)
    return dict(name=name, oid=int(native["oid"]), owner=owner, catalog=catalog, columns=columns)


async def _control_delegation(connection, policy, source_receipt, legacy):
    """Use only this transaction's granted locks for legacy Owner observation."""
    return (
        await issue_legacy_delegation(connection, policy, source_receipt)
        if legacy
        else builder_delegation(source_receipt)
    )


async def _require_legacy_observation(connection, policy, actor, delegation):
    """Keep the initial administrator scope and recheck the issued native proof."""
    await require_legacy_observation(connection, policy, delegation, legacy=actor is not None)
    if actor is not None:
        _require(
            asyncio.current_task() is actor[1] and await _admin_identity(connection, policy) == actor[0],
            "admin_transaction_changed",
        )


def _control_hook_functions(source_receipt, protected, initial, epoch):
    """Preserve all existing authenticated guard functions in the same closure."""
    hook_functions_by_name = {entry["definition"]["proname"]: entry for entry in source_receipt["functions"].values()}
    hook_functions_by_name.update({**protected["functions"], **initial["functions"]})
    hook_functions_by_name.update(
        {
            guard["proname"]: {"definition": {"oid": str(guard["tgfoid"])}}
            for control in epoch.values()
            for guard in control["guards"]
        }
    )
    return hook_functions_by_name


def _control_principals(policy, role_graph):
    """Resolve the three existing policy principals in the authenticated graph."""
    by_name = {role["rolname"]: int(role["oid"]) for role in role_graph["roles"]}
    return tuple(by_name[name] for name in (policy.owner_role, policy.publisher_role, policy.control_runtime_roles[0]))


async def capture_cms_control_custody(connection, policy, *, migration_role, namespace_owner_role, legacy=False):
    """Capture the exact ten-root closure; unknown external FK ownership always refuses."""
    _require(
        type(policy) is GenericSourcePolicy and type(legacy) is bool and connection.is_in_transaction() is True,
        "policy_required",
    )
    from process.provider_directory_generic_source_provision import _verify_control_closure
    from process.provider_directory_source_acquisition_custody import capture_source_acquisition_custody
    from process.provider_directory_source_callable_custody import capture_source_callable_custody

    actor = (await _admin_identity(connection, policy), asyncio.current_task()) if legacy else None
    await _verify_control_closure(connection, policy)
    protected = await capture_cms_callable_custody(connection, policy)
    initial = await capture_initial_callable_custody(connection, policy)
    epoch = await capture_source_cms_epoch_controls(connection, policy)
    roles = protected["role_graph"]
    by_name = {role["rolname"]: int(role["oid"]) for role in roles["roles"]}
    owner, publisher, builder = _control_principals(policy, roles)
    migration = _require_migration_identity(protected, policy, migration_role)
    source_receipt = await capture_source_callable_custody(
        connection, policy, migration_role=migration_role, namespace_owner_role=namespace_owner_role, legacy=legacy
    )
    delegation = await _control_delegation(connection, policy, source_receipt, legacy)
    family = await capture_source_acquisition_custody(
        connection, policy, source_receipt, legacy=legacy, delegation=delegation
    )
    family["source_inputs"] = await _capture_source_history_inputs(connection, policy)
    hook_functions_by_name = _control_hook_functions(source_receipt, protected, initial, epoch)
    relations_by_root, foreign_edges = await _capture_control_relations(
        connection,
        policy,
        (owner, publisher, builder, migration),
        hook_functions_by_name,
        legacy=legacy,
        family=family,
        delegation=delegation,
    )
    foreign_edges = await _capture_complete_foreign(connection, relations_by_root, family, foreign_edges)
    owner_usage = _decoded(await connection.fetchval(_OWNER_USAGE_SQL, owner))
    _role_rows(owner_usage, set(by_name.values()), 2)
    await _require_legacy_observation(connection, policy, actor, delegation)
    return dict(
        schema=policy.schema,
        owner=owner,
        publisher=publisher,
        builder=builder,
        migration=migration,
        role_graph=roles,
        owner_usage=owner_usage,
        functions=source_receipt["functions"],
        source=source_receipt,
        acquisition=family,
        relations=relations_by_root,
        foreign=foreign_edges,
        protected=protected,
        initial_functions=initial["functions"],
        epoch=epoch,
    )


def _require_relation_delta(root, before, after, owner, builder, read_grantees=(), *, source_endpoint_hooks=()):
    expected = deepcopy(before)
    _require(
        {entry["oid"] for entry in before["catalog"]["classes"]}
        == {entry["oid"] for entry in after["catalog"]["classes"]}
        and {(entry["attrelid"], entry["attnum"]) for entry in before["catalog"]["attributes"]}
        == {(entry["attrelid"], entry["attnum"]) for entry in after["catalog"]["attributes"]},
        "adoption_identity_changed",
    )
    tables, columns = _expected_relation_acl(root, before["catalog"], owner, builder)
    native = next(entry for entry in before["catalog"]["classes"] if entry["relname"] == root)
    attributes = [entry for entry in before["catalog"]["attributes"] if entry["attrelid"] == native["oid"]]
    columns |= {
        edge
        for edge in cms_control_read_edges(root, attributes, read_grantees, owner)
        if (*edge[:-1], True) not in columns
    }
    _require(
        set(map(tuple, after["catalog"]["table_acl"])) == tables
        and set(map(tuple, after["catalog"]["column_acl"])) == columns,
        "adoption_acl_changed",
    )
    for entry in expected["catalog"]["classes"]:
        entry["relowner"] = str(owner)
        entry["relacl"] = next(
            current["relacl"] for current in after["catalog"]["classes"] if current["oid"] == entry["oid"]
        )
    for entry in expected["catalog"]["attributes"]:
        entry["attacl"] = next(
            current["attacl"]
            for current in after["catalog"]["attributes"]
            if (current["attrelid"], current["attnum"]) == (entry["attrelid"], entry["attnum"])
        )
    expected["composite"]["typowner"] = str(owner)
    source_hooks_by_oid = {str(hook["oid"]): hook for hook in source_endpoint_hooks}
    for hook in expected["catalog"]["triggers"] or ():
        if str(hook["oid"]) in source_hooks_by_oid:
            _require(hook == source_hooks_by_oid[str(hook["oid"])], "adoption_source_endpoint_hook_changed")
            hook["tgenabled"] = "A"
    for field in ("table_acl", "column_acl", "access"):
        expected["catalog"][field] = after["catalog"][field]
    _require(expected == after, "adoption_relation_changed")


def require_cms_control_adoption_delta(before, after, expected_checks, *, read_policy=None):
    """Preserve native snapshots except fixed ownership/ACL and alias FK enablement."""
    _require(
        {
            key: receipt_value
            for key, receipt_value in before.items()
            if key not in {"relations", "functions", "foreign", "source", "acquisition"}
        }
        == {
            key: receipt_value
            for key, receipt_value in after.items()
            if key not in {"relations", "functions", "foreign", "source", "acquisition"}
        },
        "adoption_scope_changed",
    )
    _require(before["relations"].keys() == after["relations"].keys() == set(CONTROL_ROOTS), "adoption_roots_changed")
    read_grantees = () if read_policy is None else cms_control_read_oids(read_policy, before["role_graph"])
    _require(read_policy is None or read_policy.schema == before["schema"], "adoption_read_scope_changed")
    source_endpoint = _require_source_endpoint_fk(before["foreign"], before["relations"], legacy=True)
    _require_source_endpoint_fk(after["foreign"], after["relations"])
    for root in CONTROL_ROOTS:
        _require_relation_delta(
            root,
            before["relations"][root],
            after["relations"][root],
            after["owner"],
            after["builder"],
            read_grantees,
            source_endpoint_hooks=source_endpoint["hooks"],
        )
    from process.provider_directory_source_acquisition_custody import require_source_acquisition_delta
    from process.provider_directory_source_callable_custody import require_source_callable_delta

    _require(
        before["functions"] == before["source"]["functions"] and after["functions"] == after["source"]["functions"],
        "adoption_functions_changed",
    )
    require_source_callable_delta(before["source"], after["source"])
    require_source_acquisition_delta(
        before["acquisition"], after["acquisition"], after["source"]["roles"], expected_checks
    )
    _require_control_foreign_delta(before, after, source_endpoint)


def _require_control_foreign_delta(before, after, source_endpoint):
    """Permit only existing owner changes, retired cohort FK, and exact alias hook modes."""
    from process.provider_directory_source_cohort_custody import HISTORY

    retired_oids = {
        str(constraint["oid"])
        for constraint in before["acquisition"]["cohort"]["relations"][HISTORY]["catalog"]["constraints"] or ()
        if constraint["contype"] == "f" and constraint["conname"] == "pd_uhc_flex_npi_member_cohort_fkey"
    }
    expected_foreign_rows = [
        edge for edge in deepcopy(before["foreign"]) if str(edge["constraint"]["oid"]) not in retired_oids
    ]
    for edge in expected_foreign_rows:
        if str(edge["constraint"]["oid"]) == str(source_endpoint["constraint"]["oid"]):
            for hook in edge["hooks"]:
                hook["tgenabled"] = "A"
        for field in ("child", "parent"):
            if edge[field][1] == before["schema"] and edge[field][2] in {
                *CONTROL_ROOTS,
                *before["acquisition"]["relations"],
            }:
                edge[field][3] = after["owner"]
    _require(expected_foreign_rows == after["foreign"], "adoption_foreign_changed")


async def _grant_writer(connection, policy, root, catalog, *, owner_oid, builder_oid, delegation=None):
    table = storage_relation(policy.schema, root)
    native = next(entry for entry in catalog["classes"] if entry["relname"] == root)
    columns = tuple(entry["attname"] for entry in catalog["attributes"] if entry["attrelid"] == native["oid"])
    selected = ",".join(storage_identifier(column) for column in columns)
    builder, owner = storage_identifier(policy.control_runtime_roles[0]), storage_identifier(policy.owner_role)
    if int(native["relowner"]) == owner_oid:
        _require_relation_acl(root, catalog, native, owner_oid, builder_oid, legacy=False, delegation=delegation)
        # The preceding fixed grants may add redundant Owner column entries.
        rights = ",".join(f"{right}({selected})" for right in ("SELECT", "INSERT", "UPDATE", "REFERENCES"))
        await connection.execute(f"REVOKE {rights} ON TABLE {table} FROM {owner}")
        return
    await connection.execute(f"REVOKE ALL ON TABLE {table} FROM {builder}")
    rights = ",".join(f"{right}({selected})" for right in ("SELECT", "INSERT", "UPDATE", "REFERENCES"))
    await connection.execute(f"REVOKE {rights} ON TABLE {table} FROM {builder},{owner}")
    tables, by_right = _writer_rights(root, columns)
    await connection.execute(f"GRANT ALL ON TABLE {table} TO {owner}")
    await connection.execute(f"GRANT {','.join(sorted(tables))} ON TABLE {table} TO {builder}")
    for right, names in by_right.items():
        await connection.execute(
            f"GRANT {right}({','.join(storage_identifier(name) for name in names)}) ON TABLE {table} TO {builder}"
        )


async def adopt_cms_control_custody(connection, policy, before):
    """Internal admin fragment; the existing final operator owns full verification and CAS."""
    _require(
        type(policy) is GenericSourcePolicy
        and connection.is_in_transaction() is True
        and before["schema"] == policy.schema,
        "explicit_admin_required",
    )
    actor, task = await _admin_identity(connection, policy), asyncio.current_task()
    role_by_name = {role["rolname"]: int(role["oid"]) for role in before["role_graph"]["roles"]}
    owner_oid, builder_oid = role_by_name[policy.owner_role], role_by_name[policy.control_runtime_roles[0]]
    _require((before["owner"], before["builder"]) == (owner_oid, builder_oid), "adoption_roles_changed")
    delegation = builder_delegation(before["source"])
    for root, entry in sorted(
        before["relations"].items(),
        key=lambda item: int(
            next(native for native in item[1]["catalog"]["classes"] if native["relname"] == item[0])["oid"]
        ),
    ):
        await connection.execute(
            f"ALTER TABLE {storage_relation(policy.schema, root)} OWNER TO {storage_identifier(policy.owner_role)}"
        )
        await _grant_writer(
            connection,
            policy,
            root,
            entry["catalog"],
            owner_oid=owner_oid,
            builder_oid=builder_oid,
            delegation=delegation,
        )
    await _enable_source_endpoint_fk(connection, policy, before)
    from process.provider_directory_source_acquisition_custody import (
        adopt_source_acquisition_custody,
        canonical_acquisition_checks,
    )
    from process.provider_directory_source_callable_custody import adopt_source_callable_custody
    from process.provider_directory_source_cohort_custody import retire_cohort_intake_guards

    await adopt_source_acquisition_custody(connection, policy, before["acquisition"])
    await adopt_source_callable_custody(connection, policy, before["source"])
    await retire_cohort_intake_guards(connection, policy, before["acquisition"]["cohort"])
    role_by_oid = {int(role["oid"]): role["rolname"] for role in before["role_graph"]["roles"]}
    expected_checks = await canonical_acquisition_checks(
        connection,
        policy,
        migration_role=role_by_oid[before["migration"]],
        namespace_owner_role=role_by_oid[before["source"]["roles"]["namespace_owner"]],
    )
    _require(
        asyncio.current_task() is task and await _admin_identity(connection, policy) == actor,
        "admin_transaction_changed",
    )
    return expected_checks
