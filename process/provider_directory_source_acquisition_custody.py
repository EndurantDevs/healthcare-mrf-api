# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed acquisition storage and migration metadata checks for SOURCE custody."""

import re
from copy import deepcopy

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_source_acquisition_hooks import (
    _capture_hook_predicates as _capture_hook_predicates,
)
from process.provider_directory_source_acquisition_hooks import (
    _member_hook_contracts as _member_hook_contracts,
)
from process.provider_directory_source_acquisition_hooks import (
    _require_hooks as _require_hooks,
)
from process.provider_directory_source_acquisition_hooks import (
    _root_hook_contracts as _root_hook_contracts,
)
from process.provider_directory_source_builder_delegation import (
    receipt_delegation,
    require_delegated_access,
    require_legacy_observation,
)
from process.provider_directory_source_callable_custody import (
    capture_source_callable_adoption_capability as capture_source_callable_adoption_capability,
)
from process.provider_directory_source_callable_custody import (
    capture_source_callable_capability as capture_source_callable_capability,
)
from process.provider_directory_source_cohort_custody import (
    capture_cohort_snapshot_custody as capture_cohort_snapshot_custody,
)

ACQUISITION_ROOTS = (
    "provider_directory_uhc_flex_npi_cohort",
    "provider_directory_uhc_flex_npi_member",
    "provider_directory_uhc_flex_practitioner_acquisition",
    "provider_directory_uhc_flex_practitioner_work",
    "provider_directory_uhc_flex_practitioner_resource",
    "provider_directory_uhc_flex_practitioner_twin_attempt",
    "provider_directory_uhc_flex_practitioner_twin_admission",
    "provider_directory_rooted_graph_acquisition",
    "provider_directory_rooted_graph_work",
    "provider_directory_rooted_graph_resource",
    "provider_directory_rooted_graph_edge",
    "provider_directory_rooted_graph_twin_attempt",
    "provider_directory_rooted_graph_twin_admission",
)
BULK_ROOTS = (
    ACQUISITION_ROOTS[3],
    ACQUISITION_ROOTS[4],
    ACQUISITION_ROOTS[8],
    ACQUISITION_ROOTS[9],
    ACQUISITION_ROOTS[10],
)
HISTORY_ROOTS = tuple(root + "_legacy" for root in BULK_ROOTS)
METADATA_ROOTS = ("pd_practitioner_storage_migration", "pdrg_storage_migration")
_ROOT_SQL = """SELECT c.oid::bigint AS oid,c.relkind::text AS kind,c.relpersistence::text AS persistence,
 c.relrowsecurity OR c.relforcerowsecurity AS rls,c.reloptions AS options,
 p.partstrat::text AS strategy,p.partattrs::smallint[] AS partition_columns,p.partexprs::text AS expressions
 FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_partitioned_table p ON p.partrelid=c.oid
 WHERE c.oid=pg_catalog.to_regclass(:relation)"""
_PARTIAL_INDEX_SQL = """SELECT c.relname AS name,i.indisunique AS "unique",i.indisvalid AND i.indisready
 AND i.indislive AS ready,pg_catalog.pg_get_expr(i.indpred,i.indrelid,true) AS predicate,
 ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(number,position)
 JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.number ORDER BY position) AS columns
 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
 WHERE i.indrelid=pg_catalog.to_regclass(:relation) AND i.indpred IS NOT NULL"""
_CANDIDATE_PATTERNS = {
    ACQUISITION_ROOTS[3]: re.compile(r"pd_uhc_flex_pw_[0-9a-f]{40}\Z"),
    ACQUISITION_ROOTS[4]: re.compile(r"pd_uhc_flex_pr_[0-9a-f]{40}\Z"),
    ACQUISITION_ROOTS[8]: re.compile(r"pdrgw_[0-9a-f]{48}\Z"),
    ACQUISITION_ROOTS[9]: re.compile(r"pdrgr_[0-9a-f]{48}\Z"),
    ACQUISITION_ROOTS[10]: re.compile(r"pdrge_[0-9a-f]{48}\Z"),
}


def _require(value, code):
    if not value:
        raise ValueError("source_acquisition_" + code)


def _models():
    from db.models.provider_directory_rooted_graph import (
        ProviderDirectoryRootedGraphAcquisition,
        ProviderDirectoryRootedGraphWork,
    )
    from db.models.provider_directory_rooted_graph_twin import (
        ProviderDirectoryRootedGraphTwinAdmission,
        ProviderDirectoryRootedGraphTwinAttempt,
    )
    from db.models.provider_directory_rooted_graph_witness import (
        ProviderDirectoryRootedGraphEdge,
        ProviderDirectoryRootedGraphResource,
    )
    from db.models.provider_directory_uhc_flex import (
        ProviderDirectoryUHCFlexNPICohort,
        ProviderDirectoryUHCFlexNPIMember,
    )
    from db.models.provider_directory_uhc_flex_practitioner import (
        ProviderDirectoryUHCFlexPractitionerAcquisition,
        ProviderDirectoryUHCFlexPractitionerResource,
        ProviderDirectoryUHCFlexPractitionerWork,
    )
    from db.models.provider_directory_uhc_flex_practitioner_twin import (
        UHCFlexPractitionerTwinAdmissionModel,
        UHCFlexPractitionerTwinAttemptModel,
    )

    models = (
        ProviderDirectoryUHCFlexNPICohort,
        ProviderDirectoryUHCFlexNPIMember,
        ProviderDirectoryUHCFlexPractitionerAcquisition,
        ProviderDirectoryUHCFlexPractitionerWork,
        ProviderDirectoryUHCFlexPractitionerResource,
        UHCFlexPractitionerTwinAttemptModel,
        UHCFlexPractitionerTwinAdmissionModel,
        ProviderDirectoryRootedGraphAcquisition,
        ProviderDirectoryRootedGraphWork,
        ProviderDirectoryRootedGraphResource,
        ProviderDirectoryRootedGraphEdge,
        ProviderDirectoryRootedGraphTwinAttempt,
        ProviderDirectoryRootedGraphTwinAdmission,
    )
    return dict(zip(ACQUISITION_ROOTS, models, strict=True))


def _require_parent(root, native, acquisition_column):
    _require(
        native is not None
        and native["kind"] == ("p" if root in BULK_ROOTS or root == ACQUISITION_ROOTS[1] else "r")
        and native["persistence"] == "p"
        and native["rls"] is False
        and native["options"] is None,
        "administrator_shape_required",
    )
    if root in BULK_ROOTS or root == ACQUISITION_ROOTS[1]:
        _require(
            native["strategy"] == "l"
            and native["partition_columns"] == [acquisition_column]
            and native["expressions"] is None,
            "administrator_partition_required",
        )
    else:
        _require(
            native["strategy"] is native["partition_columns"] is native["expressions"] is None,
            "administrator_partition_required",
        )


def _require_parent_indexes(root, model, indexes, partial):
    """Require the authentic essential keys without rebuilding any installed index."""
    from process.provider_directory_cms_control_custody import _require_model_indexes

    if root == ACQUISITION_ROOTS[1]:
        _require(not partial and len(indexes) == 2, "administrator_index_required")
        for keys, unique in ((["cohort_id", "npi"], True), (["npi", "cohort_id"], False)):
            _require(
                any(
                    index["ready"]
                    and index["plain"]
                    and index["primary"] is unique
                    and index["unique"] is unique
                    and index["method"] == "btree"
                    and index["columns"] == keys
                    for index in indexes
                ),
                "administrator_index_required",
            )
        return
    if root not in BULK_ROOTS:
        _require_model_indexes(model, indexes)
        _require(not partial, "administrator_index_required")
        return
    for constraint in model.__table__.constraints:
        if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint)):
            _require(
                any(
                    index["ready"]
                    and index["plain"]
                    and index["unique"]
                    and index["columns"] == [column.name for column in constraint.columns]
                    and (not isinstance(constraint, PrimaryKeyConstraint) or index["primary"])
                    for index in indexes
                ),
                "administrator_index_required",
            )
    if root == ACQUISITION_ROOTS[3]:
        _require(
            any(
                index["ready"]
                and index["plain"]
                and not index["unique"]
                and index["method"] == "btree"
                and index["columns"] == ["acquisition_id", "status", "lease_expires_at", "npi"]
                for index in indexes
            ),
            "administrator_index_required",
        )
    if root == ACQUISITION_ROOTS[8]:
        _require(
            len(partial) == 1
            and partial[0]["ready"]
            and partial[0]["unique"]
            and partial[0]["columns"] == ["acquisition_id", "kind"]
            and partial[0]["predicate"] == "kind::text = 'full_insurance_plan_census'::text",
            "administrator_index_required",
        )
    else:
        _require(not partial, "administrator_index_required")


async def require_acquisition_model_layout(database, model, schema):
    """Check installed mutable controls without changing their native storage."""
    from process.provider_directory_cms_control_custody import _COLUMN_SQL, _INDEX_SQL, _require_model_columns

    root = model.__tablename__
    _require(root in _models() and model is _models()[root], "model_required")
    table = storage_relation(schema, root)
    columns = [
        dict(column_row._mapping if hasattr(column_row, "_mapping") else column_row)
        for column_row in await database.all(_COLUMN_SQL, relation=table)
    ]
    _require_model_columns(model, columns)
    native = await database.first(_ROOT_SQL, relation=table)
    native = dict(native._mapping if hasattr(native, "_mapping") else native) if native is not None else None
    acquisition_column = next(
        (
            position
            for position, column in enumerate(columns, 1)
            if column["name"] == ("cohort_id" if root == ACQUISITION_ROOTS[1] else "acquisition_id")
        ),
        None,
    )
    _require_parent(root, native, acquisition_column)
    indexes = [
        dict(column_row._mapping if hasattr(column_row, "_mapping") else column_row)
        for column_row in await database.all(_INDEX_SQL, relation=table)
    ]
    partial_indexes = [
        dict(column_row._mapping if hasattr(column_row, "_mapping") else column_row)
        for column_row in await database.all(_PARTIAL_INDEX_SQL, relation=table)
    ]
    _require_parent_indexes(root, model, indexes, partial_indexes)


def _require_migration_plans(plans, roots):
    """Bind completed authentic migration receipts to the original and current OIDs."""
    _require(set(plans) == set(METADATA_ROOTS), "migration_plan_missing")
    rooted = plans[METADATA_ROOTS[1]]
    _require(rooted == [{"singleton": True, "complete": True}], "rooted_migration_incomplete")
    _require_practitioner_migration_plan(plans[METADATA_ROOTS[0]], roots)


def _require_practitioner_migration_plan(practitioner, roots):
    """Bind the unchanged practitioner singleton to its five original/current OIDs."""
    expected_plan_by_field = dict(
        singleton=True,
        acquisition_oid=roots[ACQUISITION_ROOTS[2]]["oid"],
        work_oid=roots[HISTORY_ROOTS[0]]["oid"],
        resource_oid=roots[HISTORY_ROOTS[1]]["oid"],
        parent_oid=roots[BULK_ROOTS[1]]["oid"],
        work_parent_oid=roots[BULK_ROOTS[0]]["oid"],
    )
    _require(len(practitioner) == 1 and practitioner[0] == expected_plan_by_field, "practitioner_migration_changed")
    _require(
        all(type(value) is int and value > 0 for key, value in practitioner[0].items() if key != "singleton"),
        "practitioner_migration_changed",
    )


def _foreign_contracts():
    """Retain native control relationships, excluding the retired bulk RI graph."""
    from db.models.provider_directory_rooted_graph_publication import ProviderDirectoryRootedGraphDataset
    from db.models.provider_directory_uhc_flex_practitioner_publication import (
        ProviderDirectoryUHCFlexPractitionerDataset,
    )

    contract_by_name = {}
    models = (*_models().values(), ProviderDirectoryRootedGraphDataset, ProviderDirectoryUHCFlexPractitionerDataset)
    for model in models:
        if model.__tablename__ in BULK_ROOTS or model.__tablename__ == ACQUISITION_ROOTS[1]:
            continue
        for constraint in model.__table__.constraints:
            if not isinstance(constraint, ForeignKeyConstraint):
                continue
            reference_coordinates = [element.target_fullname.split(".")[-2:] for element in constraint.elements]
            parent = reference_coordinates[0][0]
            if model.__tablename__ not in ACQUISITION_ROOTS and parent not in ACQUISITION_ROOTS:
                continue
            _require(
                all(reference_coordinate[0] == parent for reference_coordinate in reference_coordinates),
                "foreign_contract_changed",
            )
            contract_by_name[str(constraint.name)] = dict(
                child=model.__tablename__,
                parent=parent,
                columns=[column.name for column in constraint.columns],
                parent_columns=[reference_coordinate[1] for reference_coordinate in reference_coordinates],
                deferred=bool(constraint.deferrable),
                initially_deferred=constraint.initially == "DEFERRED",
                update=constraint.onupdate or "NO ACTION",
                delete=constraint.ondelete or "NO ACTION",
            )
    return contract_by_name


def _require_foreign_graph(edges, roots, schema, *, owner, builder, legacy=False):
    """Unknown inbound owners or native relationships never join custody implicitly."""
    contracts = _foreign_contracts()
    expected_names = set(contracts)
    seen_names = set()
    action_by_name = {"NO ACTION": "a", "RESTRICT": "r", "CASCADE": "c", "SET NULL": "n", "SET DEFAULT": "d"}
    for edge in edges:
        native = edge["constraint"]
        name = native["conname"]
        _require(name in expected_names and name not in seen_names, "foreign_scope_changed")
        seen_names.add(name)
        contract = contracts[name]
        for field in ("child", "parent"):
            _require(contract[field] in roots, "foreign_identity_changed")
            root = roots[contract[field]]
            is_mutable_root = contract[field] in ACQUISITION_ROOTS or contract[field] in {
                "provider_directory_source",
                "provider_directory_api_endpoint",
            }
            allowed = {owner, builder} if legacy and is_mutable_root else {owner}
            _require(
                root["owner"] in allowed and edge[field] == [root["oid"], schema, contract[field], root["owner"]],
                "foreign_identity_changed",
            )
        child, parent = roots[contract["child"]], roots[contract["parent"]]
        _require(
            int(native["conrelid"]) == child["oid"]
            and int(native["confrelid"]) == parent["oid"]
            and native["conkey"] == [child["columns"].index(column) + 1 for column in contract["columns"]]
            and native["confkey"] == [parent["columns"].index(column) + 1 for column in contract["parent_columns"]]
            and native["confmatchtype"] == "s"
            and native["confupdtype"] == action_by_name[contract["update"]]
            and native["confdeltype"] == action_by_name[contract["delete"]]
            and native["condeferrable"] is contract["deferred"]
            and native["condeferred"] is contract["initially_deferred"]
            and native["convalidated"] is True
            and native["conenforced"] is True
            and native["conparentid"] == "0",
            "foreign_contract_changed",
        )
    _require(seen_names == expected_names, "foreign_constraint_missing")


def _expression_table_sql(model, table, schema):
    """Compile only trusted model types and checks; copy no native expressions or rows."""
    from process.provider_directory_cms_control_custody import _model_columns

    _require(model.__tablename__ in _models() and model is _models()[model.__tablename__], "model_required")
    _require(str(model.__table__.schema) == schema, "model_schema_changed")
    columns = [storage_identifier(name) + " " + kind for name, (kind, _required) in _model_columns(model).items()]
    checks = [
        "CONSTRAINT " + storage_identifier(str(constraint.name)) + " CHECK(" + str(constraint.sqltext) + ")"
        for constraint in model.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    ]
    return (
        "CREATE TEMPORARY TABLE " + storage_identifier(table) + "(" + ",".join((*columns, *checks)) + ") ON COMMIT DROP"
    )


async def canonical_acquisition_checks(connection, policy, *, migration_role, namespace_owner_role):
    """Canonicalize fixed expected CHECKs once in the original protected admin cut."""
    import asyncio
    from uuid import uuid4

    from db.migration_expression_adoption import _normalized_expression
    from process.provider_directory_cms_callable_custody import _admin_identity

    task, identity = asyncio.current_task(), await _admin_identity(connection, policy)
    await capture_source_callable_capability(
        connection, policy, migration_role=migration_role, namespace_owner_role=namespace_owner_role
    )
    checks_by_root = {}
    dependency_by_root = {}
    for root, model in _models().items():
        table = "pdac_expected_" + uuid4().hex
        await connection.execute(_expression_table_sql(model, table, policy.schema))
        checks_by_root[root] = {
            check["conname"]: _normalized_expression(check["definition"])
            for check in await connection.fetch(
                "SELECT conname,pg_catalog.pg_get_constraintdef(oid,true) AS definition "
                "FROM pg_catalog.pg_constraint WHERE conrelid=pg_catalog.to_regclass($1) AND contype='c'",
                "pg_temp." + table,
            )
        }
        dependency_by_root[root] = [
            dict(dependency_row)
            for dependency_row in await connection.fetch(
                "SELECT k.conname,d.refclassid::bigint,d.refobjid::bigint FROM pg_constraint k "
                "JOIN pg_depend d ON d.classid='pg_constraint'::regclass AND d.objid=k.oid "
                "WHERE k.conrelid=to_regclass($1) AND k.contype='c' "
                "AND d.refclassid IN ('pg_proc'::regclass,'pg_operator'::regclass) "
                "ORDER BY k.conname,d.refclassid,d.refobjid",
                "pg_temp." + table,
            )
        ]
        await connection.execute("DROP TABLE " + storage_relation("pg_temp", table))
        _require(
            await connection.fetchval("SELECT pg_catalog.to_regclass($1)", "pg_temp." + table) is None,
            "expression_table_cleanup_failed",
        )
    _require(
        asyncio.current_task() is task and await _admin_identity(connection, policy) == identity,
        "admin_transaction_changed",
    )
    return {**checks_by_root, "_dependencies": dependency_by_root}


def _candidate_name(root, acquisition_id):
    """Derive only the existing protocol's exact private relation coordinate."""
    from hashlib import sha256

    if root in BULK_ROOTS[:2]:
        _require(re.fullmatch(r"pdufpa_[0-9a-f]{48}", acquisition_id) is not None, "candidate_identity_changed")
        prefix = "pd_uhc_flex_pw_" if root == BULK_ROOTS[0] else "pd_uhc_flex_pr_"
        return prefix + sha256(acquisition_id.encode()).hexdigest()[:40]
    _require(
        root in BULK_ROOTS[2:] and re.fullmatch(r"pdrga_[0-9a-f]{48}", acquisition_id) is not None,
        "candidate_identity_changed",
    )
    prefix = dict(zip(BULK_ROOTS[2:], ("pdrgw_", "pdrgr_", "pdrge_"), strict=True))[root]
    return prefix + acquisition_id[6:]


def _require_family_members(roots, members, acquisitions):
    """Reject unrelated children and bind every candidate to its actual acquisition."""
    _require(set(roots) == set((*ACQUISITION_ROOTS, *HISTORY_ROOTS, *METADATA_ROOTS)), "family_roots_changed")
    by_oid = {root["oid"]: name for name, root in roots.items()}
    _require(len(by_oid) == len(roots), "family_oid_reused")
    seen_oids = set(by_oid)
    for member in members:
        root, name, oid = member["root"], member["name"], member["oid"]
        _require(root in BULK_ROOTS and type(oid) is int and oid > 0, "family_member_changed")
        _require(oid not in seen_oids and member["parent_oid"] in {None, roots[root]["oid"]}, "family_parent_changed")
        seen_oids.add(oid)
        _require(_CANDIDATE_PATTERNS[root].fullmatch(name) is not None, "family_member_changed")
        acquisition = member["acquisition_id"]
        _require(name == _candidate_name(root, acquisition), "family_member_changed")
        _require(acquisition in acquisitions[root], "family_scope_changed")
        is_attached = member["parent_oid"] is not None
        _require(
            (is_attached and member["phase"] in {"attached", "sealed"} and member["bounds"] == [acquisition])
            or (
                not is_attached
                and member["phase"] in {"loading", "indexed"}
                and member["bounds"] is None
                and acquisitions[root][acquisition] == "building"
            ),
            "family_phase_changed",
        )
        _require(member["kind"] == "r" and member["persistence"] == "p", "family_member_changed")
    scopes = {(member["root"], member["acquisition_id"]) for member in members}
    _require(len(scopes) == len(members), "family_scope_reused")
    by_scope = {(member["root"], member["acquisition_id"]): member for member in members}
    for member in members:
        root, acquisition = member["root"], member["acquisition_id"]
        group = BULK_ROOTS[:2] if root in BULK_ROOTS[:2] else BULK_ROOTS[2:]
        if root == BULK_ROOTS[0] and member["phase"] == "loading":
            _require((BULK_ROOTS[1], acquisition) not in by_scope, "family_phase_changed")
            continue
        siblings = [by_scope.get((parent, acquisition)) for parent in group]
        _require(all(sibling is not None for sibling in siblings), "family_group_incomplete")
        _require(
            all((sibling["parent_oid"] is None) == (member["parent_oid"] is None) for sibling in siblings),
            "family_phase_changed",
        )


_MEMBER_SQL = """SELECT c.oid::bigint AS oid,c.relname AS name,c.relkind::text AS kind,
 c.relpersistence::text AS persistence,c.relowner::bigint AS owner,i.inhparent::bigint AS parent_oid,
 EXISTS(SELECT FROM pg_index x WHERE x.indrelid=c.oid AND x.indisprimary AND x.indisvalid) AS indexed,
 pg_catalog.pg_get_expr(c.relpartbound,c.oid,true) AS partition_bound
 FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid=c.oid
 WHERE c.relnamespace=pg_catalog.to_regnamespace($1) AND
 (c.relname=ANY($2::text[]) OR i.inhparent=ANY($3::oid[]) OR
 c.relname ~ '^(pd_uhc_flex_p[wr]_[0-9a-f]{40}|pdrg[wre]_[0-9a-f]{48})$')
 ORDER BY c.oid"""
_EXPRESSION_DEPENDENCY_SQL = """WITH objects(classid,objid) AS (
 SELECT 'pg_constraint'::regclass,oid FROM pg_constraint WHERE conrelid=$1::oid
 UNION SELECT 'pg_attrdef'::regclass,oid FROM pg_attrdef WHERE adrelid=$1::oid
 UNION SELECT 'pg_class'::regclass,indexrelid FROM pg_index WHERE indrelid=$1::oid)
 SELECT d.classid::bigint,d.classid::regclass::text AS object_kind,d.objid::bigint,d.refclassid::bigint,d.refobjid::bigint,
 p.pronamespace::regnamespace::text AS function_schema,o.oprnamespace::regnamespace::text AS operator_schema
 FROM objects x JOIN pg_depend d ON (d.classid,d.objid)=(x.classid,x.objid)
 LEFT JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND p.oid=d.refobjid
 LEFT JOIN pg_operator o ON d.refclassid='pg_operator'::regclass AND o.oid=d.refobjid
 WHERE p.oid IS NOT NULL OR o.oid IS NOT NULL ORDER BY d.classid,d.objid,d.refclassid,d.refobjid"""
_WORK_UPDATE = (
    "status",
    "attempt_count",
    "lease_token",
    "lease_expires_at",
    "lease_heartbeat_at",
    "result_sha256",
    "resource_count",
    "error_code",
    "terminal_record_sha256",
    "terminal_at",
    "updated_at",
)
_ROOTED_WORK_UPDATE = (
    *_WORK_UPDATE,
    "edge_count",
    "resource_set_sha256",
    "edge_set_sha256",
    "advertised_total",
    "terminal_page_count",
    "pagination_terminal",
    "missing_http_status",
    "missing_response_sha256",
    "missing_response_bytes",
    "missing_response_json_text",
)
_ACQUISITION_UPDATE = (
    "status",
    "cohort_complete",
    "pending_count",
    "leased_count",
    "matched_count",
    "unmatched_count",
    "error_count",
    "resource_count",
    "terminal_set_sha256",
    "sealed_at",
    "updated_at",
)
_ROOTED_ACQUISITION_UPDATE = (
    "status",
    "rooted_graph_complete",
    "request_failure_coverage",
    "pending_count",
    "leased_count",
    "completed_count",
    "error_count",
    "resource_count",
    "edge_count",
    "insurance_plan_count",
    "insurance_plan_page_count",
    "terminal_set_sha256",
    "resource_set_sha256",
    "edge_set_sha256",
    "rooted_graph_sha256",
    "sealed_at",
    "updated_at",
)


def _writer_rights(root, columns, *, history=False, loading=False):
    """Preserve fixed existing producer DML; payload INSERT remains exclusively staged."""
    tables, by_right = {"SELECT"}, {}
    if loading and root == BULK_ROOTS[0]:
        return {"INSERT"}, by_right
    if history or root in METADATA_ROOTS:
        return tables if history else set(), by_right
    if root == ACQUISITION_ROOTS[0]:
        return tables, by_right
    if root == ACQUISITION_ROOTS[1]:
        return tables, by_right
    if root not in BULK_ROOTS:
        by_right["INSERT"] = tuple(
            column for column in columns if column not in {"created_at", "updated_at", "attempted_at", "admitted_at"}
        )
        if root in (ACQUISITION_ROOTS[2], ACQUISITION_ROOTS[7]):
            by_right["UPDATE"] = _ACQUISITION_UPDATE if root == ACQUISITION_ROOTS[2] else _ROOTED_ACQUISITION_UPDATE
            tables.add("MAINTAIN")
        elif root in (
            ACQUISITION_ROOTS[5],
            ACQUISITION_ROOTS[6],
            ACQUISITION_ROOTS[11],
            ACQUISITION_ROOTS[12],
        ):
            # Existing immutable guards reject mutation; the key right permits their native FOR SHARE replay fence.
            by_right["UPDATE"] = tuple(column.name for column in _models()[root].__table__.primary_key.columns)
        return tables, by_right
    if root in (BULK_ROOTS[0], BULK_ROOTS[2]):
        by_right["UPDATE"] = _WORK_UPDATE if root == BULK_ROOTS[0] else _ROOTED_WORK_UPDATE
        tables.add("MAINTAIN")
    elif root == BULK_ROOTS[1]:
        tables.add("MAINTAIN")
    return tables, by_right


def _require_relation_access(catalog, native, roles, *, legacy, root, history=False, loading=False, delegation=None):
    from process.provider_directory_cms_control_custody import _RIGHTS

    owner, builder = roles["owner"], roles["builder"]
    previous = int(native["relowner"])
    _require(previous in ({owner, builder} if legacy else {owner}), "relation_owner_changed")
    columns = [entry["attname"] for entry in catalog["attributes"] if entry["attrelid"] == native["oid"]]
    tables, by_right = _writer_rights(root, columns, history=history, loading=loading)
    number_by_column = {name: position for position, name in enumerate(columns, 1)}
    permitted = {(builder, right) for right in tables} | {(owner, right) for right in _RIGHTS}
    for grantee, grantor, right, grantable in catalog["table_acl"]:
        _require(
            grantor == previous
            and not grantable
            and (
                grantee == previous
                or (grantee, right) in permitted
                or right == "SELECT"
                and grantee not in {0, owner, builder}
            ),
            "relation_acl_changed",
        )
    for column, grantee, grantor, right, grantable in catalog["column_acl"]:
        _require(
            grantor == previous
            and not grantable
            and (
                right == "SELECT"
                and grantee != 0
                or grantee == builder
                and column in {number_by_column[name] for name in by_right.get(right, ())}
            ),
            "column_acl_changed",
        )
    _require_effective_access(catalog["access"], columns, roles, previous, (tables, by_right), delegation, native)


def _require_effective_access(access_rows, columns, roles, previous, rights, delegation=None, native=None):
    owner, builder = roles["owner"], roles["builder"]
    tables, by_right = rights
    if delegation is not None:
        require_delegated_access(
            access_rows,
            delegation,
            owner=owner,
            builder=builder,
            relation_oid=int(native["oid"]),
            legacy_builder=previous == builder,
        )
    for access in access_rows:
        if access["superuser"] or access["role"] in {owner, roles["publisher"], roles["migration"]}:
            continue
        _require(not access["owner_reach"], "relation_owner_authority")
        if access["role"] == builder and previous == owner:
            _require(set(access["table"]) == tables, "writer_effective_acl_changed")
            for column, rights in access["columns"]:
                expected = tables & {"SELECT", "INSERT", "UPDATE", "REFERENCES"}
                expected.update(right for right, names in by_right.items() if columns[column - 1] in names)
                _require(set(rights) == expected, "writer_effective_acl_changed")
        elif delegation is None and access["role"] != builder and not access["builtin"]:
            _require(
                set(access["table"]) <= {"SELECT"}
                and all(set(rights) <= {"SELECT"} for _, rights in access["columns"]),
                "inherited_writer_authority",
            )


def _partition_values(definition):
    if definition is None:
        return None
    _require(
        re.fullmatch(r"FOR VALUES IN \('[a-z0-9_]+(?:', '[a-z0-9_]+)*'\)", definition) is not None,
        "partition_bounds_changed",
    )
    values = re.findall(r"'([a-z0-9_]+)'", definition)
    _require(len(set(values)) == len(values), "partition_bounds_changed")
    return values


async def _acquisitions(connection, schema):
    by_root = {}
    for header, roots in ((ACQUISITION_ROOTS[2], BULK_ROOTS[:2]), (ACQUISITION_ROOTS[7], BULK_ROOTS[2:])):
        rows = await connection.fetch("SELECT acquisition_id,status FROM " + storage_relation(schema, header))
        by_id = {row["acquisition_id"]: row["status"] for row in rows}
        _require(len(by_id) == len(rows), "acquisition_identity_reused")
        for root in roots:
            for acquisition_id in by_id:
                _candidate_name(root, acquisition_id)
            by_root[root] = by_id
    return by_root


async def _native_family(connection, policy):
    """Resolve only fixed roots and genuine header-derived candidate coordinates."""
    fixed = (*ACQUISITION_ROOTS, *HISTORY_ROOTS, *METADATA_ROOTS)
    roots = await connection.fetch(
        "SELECT relname,oid::bigint AS oid FROM pg_class WHERE relnamespace=to_regnamespace($1) "
        "AND relname=ANY($2::text[])",
        policy.schema,
        list(fixed),
    )
    _require(
        len(roots) == len(fixed) and {member_row["relname"] for member_row in roots} == set(fixed),
        "family_roots_missing",
    )
    await _require_safe_headers(connection, policy)
    oid_by_root = {member_row["relname"]: member_row["oid"] for member_row in roots}
    member_rows = [
        dict(member_row)
        for member_row in await connection.fetch(
            _MEMBER_SQL, policy.schema, list(fixed), [oid_by_root[root] for root in BULK_ROOTS]
        )
    ]
    acquisitions = await _acquisitions(connection, policy.schema)
    known_by_name = {
        _candidate_name(root, acquisition): (root, acquisition)
        for root, by_id in acquisitions.items()
        for acquisition in by_id
    }
    members = []
    for member_row in member_rows:
        if member_row["name"] in fixed:
            continue
        _require(member_row["name"] in known_by_name, "unknown_family_member")
        root, acquisition = known_by_name[member_row["name"]]
        member_row.update(
            root=root, acquisition_id=acquisition, bounds=_partition_values(member_row.pop("partition_bound"))
        )
        member_row["phase"] = (
            "sealed"
            if acquisitions[root][acquisition] == "sealed"
            else "attached"
            if member_row["parent_oid"]
            else "indexed"
            if member_row["indexed"]
            else "loading"
        )
        members.append(member_row)
    native_by_name = {member_row["name"]: member_row for member_row in member_rows if member_row["name"] in fixed}
    _require_family_members(native_by_name, members, acquisitions)
    await _lock_candidates(connection, policy, members)
    plans = await _capture_history_plans(connection, policy, native_by_name, oid_by_root, acquisitions)
    return native_by_name, members, acquisitions, plans


async def _lock_candidates(connection, policy, members):
    if members:
        await connection.execute(
            "LOCK TABLE "
            + ",".join(
                storage_relation(policy.schema, member_row["name"])
                for member_row in sorted(members, key=lambda member_row: member_row["oid"])
            )
            + " IN ACCESS EXCLUSIVE MODE NOWAIT"
        )
        oid_by_name = {
            member_row["relname"]: member_row["oid"]
            for member_row in await connection.fetch(
                "SELECT relname,oid::bigint AS oid FROM pg_class WHERE relnamespace=to_regnamespace($1) "
                "AND relname=ANY($2::text[])",
                policy.schema,
                [member_row["name"] for member_row in members],
            )
        }
        _require(
            oid_by_name == {member_row["name"]: member_row["oid"] for member_row in members}, "family_member_changed"
        )


async def _capture_history_plans(connection, policy, native_by_name, oid_by_root, acquisitions):
    from process.provider_directory_cms_callable_custody import _decoded

    for root, history in zip(BULK_ROOTS, HISTORY_ROOTS, strict=True):
        member_row = native_by_name[history]
        scope_ids = _partition_values(member_row["partition_bound"])
        _require(member_row["parent_oid"] in {None, oid_by_root[root]}, "history_parent_changed")
        _require(scope_ids is not None if member_row["parent_oid"] else scope_ids is None, "history_parent_changed")
        if scope_ids is not None:
            _require(set(scope_ids) <= acquisitions[root].keys(), "history_scope_changed")
        else:
            _require(
                await connection.fetchval(
                    "SELECT NOT EXISTS(SELECT FROM " + storage_relation(policy.schema, history) + ")"
                ),
                "detached_history_nonempty",
            )
    projection_by_root = {
        METADATA_ROOTS[0]: "to_jsonb(r) || jsonb_build_object("
        "'acquisition_oid',r.acquisition_oid::bigint,'work_oid',r.work_oid::bigint,"
        "'resource_oid',r.resource_oid::bigint,'parent_oid',r.parent_oid::bigint,"
        "'work_parent_oid',r.work_parent_oid::bigint)",
        METADATA_ROOTS[1]: "to_jsonb(r)",
    }
    plan_by_root = {
        root: [
            _decoded(member_row["body"])
            for member_row in await connection.fetch(
                "SELECT " + projection_by_root[root] + " AS body FROM " + storage_relation(policy.schema, root) + " r"
            )
        ]
        for root in METADATA_ROOTS
    }
    _require_migration_plans(plan_by_root, native_by_name)
    return plan_by_root


async def _require_safe_headers(connection, policy):
    """Authenticate native columns and SELECT surface before reading any legacy control values."""
    from process.provider_directory_cms_control_custody import _COLUMN_SQL, _require_model_columns

    for name in (ACQUISITION_ROOTS[2], ACQUISITION_ROOTS[7], *HISTORY_ROOTS, *METADATA_ROOTS):
        relation = storage_relation(policy.schema, name)
        safe = await connection.fetchval(
            "SELECT relkind='r' AND relpersistence='p' AND NOT relrowsecurity "
            "AND NOT relforcerowsecurity AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid) "
            "FROM pg_class c WHERE oid=to_regclass($1)",
            relation,
        )
        _require(safe is True, "header_shape_changed")
        column_rows = [dict(row) for row in await connection.fetch(_COLUMN_SQL.replace(":relation", "$1"), relation)]
        if name in METADATA_ROOTS:
            _require_plan_columns(name, column_rows)
        else:
            _require_model_columns(_models()[name.removesuffix("_legacy")], column_rows)


def _require_plan_columns(root, column_rows):
    expected_by_name = {
        "singleton": ("boolean", True),
        **(
            {"complete": ("boolean", True)}
            if root == METADATA_ROOTS[1]
            else {
                key: ("oid", key not in {"parent_oid", "work_parent_oid"})
                for key in ("acquisition_oid", "work_oid", "resource_oid", "parent_oid", "work_parent_oid")
            }
        ),
    }
    _require(
        {row["name"]: (row["type"], row["required"]) for row in column_rows} == expected_by_name
        and all(not row["identity"] and not row["generated"] for row in column_rows),
        "plan_columns_changed",
    )


async def lock_source_control_family(connection, policy):
    """Hold the complete fixed administrative cut before legacy rows or native expressions are read."""
    from process.provider_directory_cms_control_custody import CONTROL_ROOTS
    from process.provider_directory_source_cohort_custody import HISTORY

    names = sorted(
        {*CONTROL_ROOTS, *ACQUISITION_ROOTS, *HISTORY_ROOTS, *METADATA_ROOTS, HISTORY, "pd_dataset_migration_plan"}
    )
    await connection.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(policy.schema, name) for name in names)
        + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    children = await connection.fetch(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid "
        "JOIN pg_class p ON p.oid=i.inhparent WHERE p.relnamespace=to_regnamespace($1) "
        "AND p.relname=ANY($2::text[]) ORDER BY c.oid",
        policy.schema,
        [*BULK_ROOTS, ACQUISITION_ROOTS[1]],
    )
    if children:
        await connection.execute(
            "LOCK TABLE "
            + ",".join(storage_relation(policy.schema, row["relname"]) for row in children)
            + " IN ACCESS EXCLUSIVE MODE NOWAIT"
        )


def _require_catalog_shape(catalog, name, native, roles, *, legacy):
    owner = int(native["relowner"])
    _require(
        native["relpersistence"] == "p"
        and native["reloptions"] is None
        and not native["relrowsecurity"]
        and not native["relforcerowsecurity"]
        and native["relreplident"] == "d"
        and catalog["rules"] is catalog["policies"] is None,
        "relation_shape_changed",
    )
    _require(owner in ({roles["owner"], roles["builder"]} if legacy else {roles["owner"]}), "relation_owner_changed")
    _require(all(int(entry["relowner"]) == owner for entry in catalog["classes"]), "dependent_owner_changed")
    _require(
        all(entry["relacl"] is None for entry in catalog["classes"] if entry is not native)
        and all(entry["attacl"] is None for entry in catalog["attributes"] if entry["attrelid"] != native["oid"]),
        "dependent_acl_changed",
    )
    _require(
        all(
            not entry["attisdropped"] and not entry["attidentity"] and not entry["attgenerated"]
            for entry in catalog["attributes"]
            if entry["attrelid"] == native["oid"]
        ),
        "columns_changed",
    )
    _require(
        all(
            entry["convalidated"] and entry["conenforced"] and entry["contype"] in {"p", "u", "c", "f", "n"}
            for entry in catalog["constraints"] or ()
        ),
        "constraints_changed",
    )
    _require(
        all(
            entry["native"]
            and entry["method"] == "btree"
            and entry["indisvalid"]
            and entry["indisready"]
            and entry["indislive"]
            and not entry["index_expressions"]
            and entry["indoption"] == [0] * entry["indnatts"]
            for entry in catalog["indexes"] or ()
        ),
        "indexes_changed",
    )


def _require_defaults(root, catalog, native):
    from db.migration_expression_adoption import _normalized_expression

    expected_by_column = {
        column.name: str(column.server_default.arg) if column.server_default else ""
        for column in _models()[root].__table__.columns
    }
    actual_by_column = {
        entry["attname"]: _normalized_expression(entry["default_expression"])
        for entry in catalog["attributes"]
        if entry["attrelid"] == native["oid"]
    }
    _require(
        actual_by_column
        == {name: _normalized_expression(expression) for name, expression in expected_by_column.items()},
        "defaults_changed",
    )


def _require_relation_checks(entry, expected):
    from db.migration_expression_adoption import _normalized_expression

    name, root = entry["name"], entry["root"]
    check_by_name = {
        check["conname"]: _normalized_expression(check["constraint_definition"])
        for check in entry["catalog"]["constraints"] or ()
        if check["contype"] == "c"
    }
    if root in BULK_ROOTS and name == root:
        _require(not check_by_name, "parent_checks_changed")
        return
    if name in HISTORY_ROOTS:
        bound = (
            "pd_uhc_flex_legacy_scope" if root in BULK_ROOTS[:2] else "pdrg_" + root.rsplit("_", 1)[1] + "_legacy_scope"
        )
        _require(bound in check_by_name, "history_bound_missing")
        _require_acquisition_scope(check_by_name.pop(bound), entry["scope_values"])
    elif name != root:
        scope_checks = []
        for scope in (name + "_scope", "pdrg_candidate_scope"):
            if scope in check_by_name:
                scope_checks.append(check_by_name.pop(scope))
        _require(len(scope_checks) == (1 if entry["attached"] else 0), "candidate_scope_changed")
        for expression in scope_checks:
            _require_acquisition_scope(expression, entry["scope_values"])
        if root not in (BULK_ROOTS[0], BULK_ROOTS[2]):
            _require(not check_by_name, "candidate_checks_changed")
            return
        if entry.get("loading"):
            _require(not check_by_name, "loading_checks_changed")
            return
    _require(check_by_name == expected[root], "checks_changed")
    constraint_by_oid = {
        int(check["oid"]): check["conname"]
        for check in entry["catalog"]["constraints"] or ()
        if check["contype"] == "c" and check["conname"] in check_by_name
    }
    actual_dependencies = {
        (constraint_by_oid[dependency_row["objid"]], dependency_row["refclassid"], dependency_row["refobjid"])
        for dependency_row in entry["dependencies"]
        if dependency_row["object_kind"] == "pg_constraint" and dependency_row["objid"] in constraint_by_oid
    }
    _require(
        actual_dependencies
        == {
            (dependency_row["conname"], dependency_row["refclassid"], dependency_row["refobjid"])
            for dependency_row in expected["_dependencies"][root]
        },
        "check_dependency_changed",
    )


def _require_acquisition_scope(expression, scope_values):
    from process.provider_directory_source_cohort_custody import _simple_check

    comparison = (
        "false"
        if not scope_values
        else "acquisition_id='" + scope_values[0] + "'"
        if len(scope_values) == 1
        else "acquisition_id=anyarray[" + ",".join("'" + value + "'" for value in scope_values) + "]"
    )
    _require(_simple_check(expression) == "check" + comparison, "scope_expression_changed")


async def _capture_family_relation(
    connection, policy, name, root, callable_receipt, *, legacy, member=None, delegation=None
):
    """Capture one fixed heap without executing its data or mutable CHECK expressions."""
    from process.provider_directory_cms_callable_custody import _decoded
    from process.provider_directory_cms_control_custody import _COLUMN_SQL, _require_model_columns
    from process.provider_directory_cms_receipt_custody import _CATALOG_SQL

    roles = callable_receipt["roles"]
    table = storage_relation(policy.schema, name)
    catalog = _decoded(await connection.fetchval(_CATALOG_SQL, table, roles["owner"], roles["builder"]))
    _require(catalog and catalog["classes"], "family_relation_missing")
    native = next(entry for entry in catalog["classes"] if entry["relname"] == name)
    _require_catalog_shape(catalog, name, native, roles, legacy=legacy)
    columns = [
        dict(catalog_row) for catalog_row in await connection.fetch(_COLUMN_SQL.replace(":relation", "$1"), table)
    ]
    if root in _models():
        _require_model_columns(_models()[root], columns)
        _require_defaults(root, catalog, native)
    else:
        _require_plan_columns(root, columns)
        _require_metadata_layout(catalog, native)
    is_history = name in HISTORY_ROOTS
    is_loading = member is not None and member["phase"] == "loading"
    _require_relation_access(
        catalog,
        native,
        roles,
        legacy=legacy,
        root=root,
        history=is_history or member is not None,
        loading=is_loading,
        delegation=delegation,
    )
    await _require_family_indexes(connection, policy, name, root, columns, is_loading)
    function_by_name = {entry["definition"]["proname"]: entry for entry in callable_receipt["functions"].values()}
    hook_by_name = _root_hook_contracts().get(name)
    if name == ACQUISITION_ROOTS[0]:
        from process.provider_directory_source_cohort_custody import _header_hooks

        hook_by_name = _header_hooks(legacy and int(native["relowner"]) == roles["builder"])
    if is_history or member:
        hook_by_name = _member_hook_contracts(
            root, attached=bool(native["relispartition"]), history=is_history, empty=not native["relispartition"]
        )
        if root == BULK_ROOTS[0] and is_loading:
            hook_by_name = {}
    _require(hook_by_name is not None, "cohort_snapshot_cut_required")
    await _capture_hook_predicates(connection, catalog, native, hook_by_name)
    _require_hooks(catalog, hook_by_name, function_by_name, inherited=bool(native["relispartition"]))
    dependencies, composite = await _capture_expression_type(connection, native, callable_receipt)
    return dict(
        name=name, root=root, catalog=catalog, composite=composite, dependencies=dependencies, loading=is_loading
    )


async def _require_family_indexes(connection, policy, name, root, columns, is_loading):
    from process.provider_directory_cms_control_custody import _INDEX_SQL

    table = storage_relation(policy.schema, name)
    if name == root and root in _models():
        _require_parent(
            root,
            dict(await connection.fetchrow(_ROOT_SQL.replace(":relation", "$1"), table)),
            next(
                (index for index, catalog_row in enumerate(columns, 1) if catalog_row["name"] == "acquisition_id"), None
            ),
        )
        _require_parent_indexes(
            root,
            _models()[root],
            [dict(catalog_row) for catalog_row in await connection.fetch(_INDEX_SQL.replace(":relation", "$1"), table)],
            [
                dict(catalog_row)
                for catalog_row in await connection.fetch(_PARTIAL_INDEX_SQL.replace(":relation", "$1"), table)
            ],
        )
    elif root in BULK_ROOTS:
        index_rows = [
            dict(catalog_row) for catalog_row in await connection.fetch(_INDEX_SQL.replace(":relation", "$1"), table)
        ]
        partial_rows = [
            dict(catalog_row)
            for catalog_row in await connection.fetch(_PARTIAL_INDEX_SQL.replace(":relation", "$1"), table)
        ]
        if is_loading:
            _require(not index_rows and not partial_rows, "loading_indexes_changed")
        else:
            _require_parent_indexes(root, _models()[root], index_rows, partial_rows)


async def _capture_expression_type(connection, native, callable_receipt):
    from process.provider_directory_cms_callable_custody import _decoded

    dependencies = [
        dict(catalog_row) for catalog_row in await connection.fetch(_EXPRESSION_DEPENDENCY_SQL, int(native["oid"]))
    ]
    allowed_oids = {int(entry["definition"]["oid"]) for entry in callable_receipt["functions"].values()}
    _require(
        all(
            (catalog_row["operator_schema"] in {None, "pg_catalog"})
            and (catalog_row["function_schema"] in {None, "pg_catalog"} or catalog_row["refobjid"] in allowed_oids)
            for catalog_row in dependencies
        ),
        "expression_dependency_changed",
    )
    composite = _decoded(
        await connection.fetchval("SELECT to_jsonb(t) FROM pg_type t WHERE t.typrelid=$1::oid", int(native["oid"]))
    )
    _require(
        composite["typtype"] == "c"
        and composite["typowner"] == native["relowner"]
        and composite["typnamespace"] == native["relnamespace"],
        "rowtype_changed",
    )
    composite["array"] = _decoded(
        await connection.fetchval("SELECT to_jsonb(t) FROM pg_type t WHERE oid=$1::oid", int(composite["typarray"]))
    )
    _require(
        composite["array"] is not None
        and composite["array"]["typowner"] == native["relowner"]
        and composite["array"]["typelem"] == composite["oid"]
        and composite["array"]["typnamespace"] == native["relnamespace"],
        "rowtype_array_changed",
    )
    return dependencies, composite


def _require_metadata_layout(catalog, native):
    from db.migration_expression_adoption import _normalized_expression

    primary_rows = [entry for entry in catalog["constraints"] or () if entry["contype"] == "p"]
    check_rows = [entry for entry in catalog["constraints"] or () if entry["contype"] == "c"]
    _require(
        len(primary_rows) == len(check_rows) == 1
        and primary_rows[0]["conkey"] == [1]
        and not primary_rows[0]["condeferrable"]
        and not primary_rows[0]["condeferred"]
        and _normalized_expression(check_rows[0]["constraint_definition"]) == "check(singleton)"
        and all(entry["contype"] in {"p", "c", "n"} for entry in catalog["constraints"] or ())
        and all(
            not entry["default_expression"] for entry in catalog["attributes"] if entry["attrelid"] == native["oid"]
        ),
        "metadata_constraints_changed",
    )


async def capture_source_acquisition_custody(connection, policy, callable_receipt, *, legacy=False, delegation=None):
    """Capture exact permanent roots and deterministic current family members once per admin cut."""
    from process.provider_directory_source_cohort_custody import capture_cohort_family

    delegation = receipt_delegation(callable_receipt, delegation)
    await require_legacy_observation(connection, policy, delegation, legacy=legacy)
    native_by_name, members, acquisitions, plans = await _native_family(connection, policy)
    cohort = await capture_cohort_family(connection, policy, callable_receipt, legacy=legacy, delegation=delegation)
    relation_by_name = dict(cohort["relations"])
    for name in native_by_name:
        if name in ACQUISITION_ROOTS[:2]:
            continue
        root = name.removesuffix("_legacy") if name in HISTORY_ROOTS else name
        relation_by_name[name] = await _capture_family_relation(
            connection, policy, name, root, callable_receipt, legacy=legacy, delegation=delegation
        )
        if name in HISTORY_ROOTS:
            relation_by_name[name]["scope_values"] = _partition_values(native_by_name[name]["partition_bound"]) or []
    for member in members:
        relation_by_name[member["name"]] = await _capture_family_relation(
            connection,
            policy,
            member["name"],
            member["root"],
            callable_receipt,
            legacy=legacy,
            member=member,
            delegation=delegation,
        )
        relation_by_name[member["name"]].update(
            scope_values=[member["acquisition_id"]], attached=bool(member["parent_oid"])
        )
    edges = await _capture_family_foreign(
        connection, policy, native_by_name, relation_by_name, callable_receipt["roles"], legacy
    )
    receipt_by_field = dict(
        schema=policy.schema,
        relations=relation_by_name,
        roots=native_by_name,
        members=members,
        acquisitions=acquisitions,
        plans=plans,
        dataset_plan=await _capture_dataset_plan(connection, policy, callable_receipt),
        cohort=cohort,
        foreign=edges,
    )
    await require_legacy_observation(connection, policy, delegation)
    return receipt_by_field


async def _capture_family_foreign(connection, policy, native_by_name, relation_by_name, roles, legacy):
    from process.provider_directory_cms_callable_custody import _decoded
    from process.provider_directory_cms_control_custody import _FOREIGN_SQL
    from process.provider_directory_source_cohort_custody import HISTORY as cohort_history

    edges = [
        {key: _decoded(native_row[key]) for key in native_row.keys()}
        for native_row in await connection.fetch(_FOREIGN_SQL, [entry["oid"] for entry in native_by_name.values()])
    ]
    cohort_foreign_oids = {
        constraint["oid"]
        for constraint in relation_by_name[cohort_history]["catalog"]["constraints"] or ()
        if constraint["contype"] == "f"
    }
    edges = [edge for edge in edges if edge["constraint"]["oid"] not in cohort_foreign_oids]
    foreign_by_name = {
        name: dict(
            oid=native_row["oid"],
            owner=native_row["owner"],
            columns=[
                entry["attname"]
                for entry in relation_by_name[name]["catalog"]["attributes"]
                if int(entry["attrelid"]) == native_row["oid"]
            ],
        )
        for name, native_row in native_by_name.items()
    }
    missing = {
        part for contract in _foreign_contracts().values() for part in (contract["child"], contract["parent"])
    } - foreign_by_name.keys()
    for name in sorted(missing):
        native_row = await connection.fetchrow(
            "SELECT c.oid::bigint AS oid,c.relowner::bigint AS owner,ARRAY(SELECT attname FROM pg_attribute "
            "WHERE attrelid=c.oid AND attnum>0 AND NOT attisdropped ORDER BY attnum) AS columns FROM pg_class c WHERE oid=to_regclass($1)",
            storage_relation(policy.schema, name),
        )
        _require(native_row is not None, "foreign_root_missing")
        foreign_by_name[name] = dict(native_row)
    _require_foreign_graph(
        edges,
        foreign_by_name,
        policy.schema,
        owner=roles["owner"],
        builder=roles["builder"],
        legacy=legacy,
    )
    return edges


def _require_dataset_relationships(plan, schema):
    from db.migration_provider_dataset_online import _expected_bulk_relationships

    rows = plan.get("relationships")
    _require(
        type(rows) is list
        and all(
            type(row) is dict
            and set(row)
            == {"source_table", "target_table", "target_schema", "source_columns", "target_columns", "delete_action"}
            and row["target_schema"] == schema
            for row in rows
        ),
        "dataset_relationships_changed",
    )
    actual_relationships = {
        (
            row["source_table"],
            row["target_table"],
            tuple(row["source_columns"]),
            tuple(row["target_columns"]),
            row["delete_action"],
        )
        for row in rows
    }
    _require(
        len(actual_relationships) == len(rows) and actual_relationships == _expected_bulk_relationships(),
        "dataset_relationships_changed",
    )


async def _capture_dataset_plan(connection, policy, callable_receipt):
    from process.provider_directory_cms_callable_custody import _decoded
    from process.provider_directory_cms_receipt_custody import _CATALOG_SQL
    from process.provider_directory_generic_source_provision import _completed_legacy_dataset_plan

    plan = await _completed_legacy_dataset_plan(connection, policy)
    _require_dataset_relationships(plan, policy.schema)
    roles = callable_receipt["roles"]
    catalog = _decoded(
        await connection.fetchval(
            _CATALOG_SQL, storage_relation(policy.schema, "pd_dataset_migration_plan"), roles["owner"], roles["builder"]
        )
    )
    native = next(row for row in catalog["classes"] if row["relname"] == "pd_dataset_migration_plan")
    _require(int(native["relowner"]) == roles["owner"], "dataset_plan_owner_changed")
    function_by_name = {entry["definition"]["proname"]: entry for entry in callable_receipt["functions"].values()}
    _require_hooks(
        catalog, {"pd_dataset_plan_owner": ("guard_pd_dataset_migration_owner", 62, (), None, ())}, function_by_name
    )
    return dict(plan=plan, catalog=catalog)


async def adopt_source_acquisition_custody(connection, policy, before):
    """Preserve every native object while closing DDL and explicitly restoring fixed producer DML."""
    from process.provider_directory_cms_control_custody import _RIGHTS

    owner = storage_identifier(policy.owner_role)
    builder = storage_identifier(policy.control_runtime_roles[0])
    for name, entry in sorted(
        before["relations"].items(),
        key=lambda pair: int(
            next(native["oid"] for native in pair[1]["catalog"]["classes"] if native["relname"] == pair[0])
        ),
    ):
        table = storage_relation(policy.schema, name)
        native = next(catalog_row for catalog_row in entry["catalog"]["classes"] if catalog_row["relname"] == name)
        columns = [
            catalog_row["attname"]
            for catalog_row in entry["catalog"]["attributes"]
            if catalog_row["attrelid"] == native["oid"]
        ]
        selected = ",".join(storage_identifier(column) for column in columns)
        await connection.execute(f"ALTER TABLE {table} OWNER TO {owner}")
        await connection.execute(f"REVOKE ALL ON TABLE {table} FROM {builder}")
        rights = ",".join(right + "(" + selected + ")" for right in ("SELECT", "INSERT", "UPDATE", "REFERENCES"))
        await connection.execute(f"REVOKE {rights} ON TABLE {table} FROM {builder},{owner}")
        tables, by_right = _writer_rights(
            entry["root"], columns, history=name != entry["root"], loading=entry.get("loading", False)
        )
        await connection.execute(f"GRANT {','.join(sorted(_RIGHTS))} ON TABLE {table} TO {owner}")
        if tables:
            await connection.execute(f"GRANT {','.join(sorted(tables))} ON TABLE {table} TO {builder}")
        for right, names in by_right.items():
            await connection.execute(
                f"GRANT {right}({','.join(storage_identifier(column) for column in names)}) "
                f"ON TABLE {table} TO {builder}"
            )


def _expected_family_acl(entry, owner, builder):
    from process.provider_directory_cms_control_custody import _RIGHTS

    native = next(row for row in entry["catalog"]["classes"] if row["relname"] == entry["name"])
    previous = int(native["relowner"])
    columns = [row["attname"] for row in entry["catalog"]["attributes"] if row["attrelid"] == native["oid"]]
    table_edges = {
        (grantee, owner if grantor == previous else grantor, right, grantable)
        for grantee, grantor, right, grantable in entry["catalog"]["table_acl"]
        if grantee not in {previous, owner, builder}
    }
    column_edges = {
        (column, grantee, owner if grantor == previous else grantor, right, grantable)
        for column, grantee, grantor, right, grantable in entry["catalog"]["column_acl"]
        if grantee not in {previous, owner, builder}
    }
    tables, by_right = _writer_rights(
        entry["root"], columns, history=entry["name"] != entry["root"], loading=entry.get("loading", False)
    )
    table_edges.update((owner, owner, right, False) for right in _RIGHTS)
    table_edges.update((builder, owner, right, False) for right in tables)
    column_edges.update(
        (columns.index(column) + 1, builder, owner, right, False)
        for right, names in by_right.items()
        for column in names
    )
    return table_edges, column_edges


def _require_family_relation_delta(previous, current, roles):
    expected = deepcopy(previous)
    tables, columns = _expected_family_acl(previous, roles["owner"], roles["builder"])
    _require(
        set(map(tuple, current["catalog"]["table_acl"])) == tables
        and set(map(tuple, current["catalog"]["column_acl"])) == columns,
        "adoption_acl_changed",
    )
    class_by_oid = {row["oid"]: row for row in current["catalog"]["classes"]}
    attribute_by_key = {(row["attrelid"], row["attnum"]): row for row in current["catalog"]["attributes"]}
    _require(
        set(class_by_oid) == {row["oid"] for row in previous["catalog"]["classes"]}
        and set(attribute_by_key) == {(row["attrelid"], row["attnum"]) for row in previous["catalog"]["attributes"]},
        "adoption_identity_changed",
    )
    for row in expected["catalog"]["classes"]:
        row["relowner"], row["relacl"] = str(roles["owner"]), class_by_oid[row["oid"]]["relacl"]
    for row in expected["catalog"]["attributes"]:
        row["attacl"] = attribute_by_key[row["attrelid"], row["attnum"]]["attacl"]
    expected["composite"]["typowner"] = str(roles["owner"])
    if "array" in expected["composite"]:
        expected["composite"]["array"]["typowner"] = str(roles["owner"])
    for field in ("table_acl", "column_acl", "access"):
        expected["catalog"][field] = current["catalog"][field]
    _require(expected == current, "adoption_relation_changed")


def require_source_acquisition_delta(before, after, roles, expected_checks):
    """Adoption cannot replace heaps, indexes, constraints, bounds, plans or data authority."""
    _require(
        before.keys() == after.keys() and before["relations"].keys() == after["relations"].keys(),
        "adoption_scope_changed",
    )
    from process.provider_directory_cms_control_custody import CONTROL_ROOTS
    from process.provider_directory_source_cohort_custody import require_cohort_adoption_delta

    require_cohort_adoption_delta(before["cohort"], after["cohort"], roles)
    for name, previous in before["relations"].items():
        current = after["relations"][name]
        if name in before["cohort"]["relations"]:
            continue
        _require_family_relation_delta(previous, current, roles)
        if current["root"] in _models():
            _require_relation_checks(current, expected_checks)
    expected = deepcopy(before)
    expected["cohort"] = after["cohort"]
    expected["relations"] = after["relations"]
    for row in expected["roots"].values():
        row["owner"] = roles["owner"]
    for row in expected["members"]:
        row["owner"] = roles["owner"]
    for edge in expected["foreign"]:
        for field in ("child", "parent"):
            if edge[field][1] == before["schema"] and edge[field][2] in {*before["roots"], *CONTROL_ROOTS}:
                edge[field][3] = roles["owner"]
    _require(expected == after, "adoption_family_changed")
