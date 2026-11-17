# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed acquisition storage and migration metadata checks for SOURCE custody."""

import re

from sqlalchemy import CheckConstraint, PrimaryKeyConstraint, UniqueConstraint

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
