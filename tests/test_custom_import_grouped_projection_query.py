# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admitted wide values preserve selected families and complete-child semantics."""

import hashlib
import json
from dataclasses import replace

import pytest
from sqlalchemy import BigInteger, Numeric
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import visitors
from sqlalchemy.sql.compiler import FROM_LINTING
from sqlalchemy.sql.elements import Case
from sqlalchemy.sql.selectable import CTE

from process.custom_import import grouped_read, read_core
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.query_projection_storage import query_projection_tables
from process.custom_import.storage_layout import snapshot_models
from tests import custom_import_grouped_child_support as fixture
from tests import test_custom_import_grouped_score_query_postgres as scores


def _context(*, snapshot=False, key_type="string"):
    document = json.loads(scores._definition().canonical)
    document["query"]["filterable_fields"] = [
        "root_cost",
        "root_quality",
        "child_cost",
        "child_quality",
        "score",
        "amount",
    ]
    document["query"]["aliases"].update({"output_cost": "root_cost", "output_quality": "child_quality"})
    document["schema"]["children"][0]["fields"][1]["type"] = key_type
    definition = CustomImportDefinition.from_mapping(document)
    return replace(
        fixture.context(),
        definition=definition,
        storage_models=snapshot_models(1) if snapshot else None,
        projection_tables=query_projection_tables(1, definition),
    )


def _prepare(context, **changes_map):
    return grouped_read.prepare_relation(
        context, fixture.query(**changes_map), read_core.ExtensionReadScope("synthetic")
    )


def _sql(prepared):
    return str(prepared.statement.compile(dialect=postgresql.dialect(), linting=FROM_LINTING))


def _ctes(prepared):
    return {node.name: node for node in visitors.iterate(prepared.statement) if isinstance(node, CTE)}


@pytest.mark.parametrize("entity_values", (None, (), ("1234567893",)))
@pytest.mark.parametrize("snapshot", (False, True))
@pytest.mark.filterwarnings("error:SELECT statement has a cartesian product:sqlalchemy.exc.SAWarning")
def test_projection_retains_canonical_membership_default_year_and_exact_scope(entity_values, snapshot):
    prepared = _prepare(
        _context(snapshot=snapshot),
        context_filters=(read_core.ReadFilter("service_code", "eq", "chosen"),),
        filters=(read_core.ReadFilter("output_quality", "gte", "2"),),
        order_terms=(read_core.ReadOrderTerm("output_cost", "asc", "last"),),
        entity_values=entity_values,
    )
    sql = _sql(prepared)
    ctes = _ctes(prepared)
    selected = str(ctes["selected_score_families"].element.compile(dialect=postgresql.dialect()))
    assert "custom_import_entity_binding" in selected and "canonical_value" in selected
    assert "custom_import_generation_seal" in selected and "custom_import_generation_family" in selected
    assert "selected_entity_value" in selected and "helper_slot_6" in selected
    assert "custom_import_derived_" not in selected
    assert {"entity_binding_id", "selected_value", "group_value"} <= set(ctes["validated_score_children"].c.keys())
    assert "derived_score_values" not in sql and "score_root_12" not in sql and "score_child_11" not in sql
    assert "count(selected_score_children.child_revision_id) OVER" in sql
    assert "validated_score_children AS MATERIALIZED" in sql
    assert ("selected_root_member_pairs AS MATERIALIZED" in sql) is (entity_values is None)
    assert ("candidate_complete_score_members" in sql) is (entity_values is None)
    assert sql.count("LEFT OUTER JOIN ci_snapshot_1.custom_import_derived_root ON") == 1
    assert sql.count("LEFT OUTER JOIN ci_snapshot_1.custom_import_derived_child ON") == 1
    for table in ("custom_import_derived_root", "custom_import_derived_child"):
        assert f"{table}.entity_binding_id = validated_score_children.entity_binding_id" in sql
        assert f"{table}.selected_value = validated_score_children.selected_value" in sql
    namespace = "ci_snapshot_1" if snapshot else "mrf"
    assert f"{namespace}.custom_import_child_revision AS projected_score_child" in sql
    for identity in ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot"):
        assert f"projected_score_child.{identity} =" in sql
    assert "custom_import_derived_child.child_key_sha256 = projected_score_child.child_key_sha256" in sql
    assert 'custom_import_derived_child.canonical_child_key COLLATE "C")' in sql
    assert '(projected_score_child.canonical_child_key COLLATE "C")' in sql
    assert "CAST(" not in sql and "jsonb_extract" not in sql
    assert prepared.normalized_order_terms == (read_core.ReadOrderTerm("root_cost", "asc", "last"),)


@pytest.mark.parametrize("operator", ("eq", "gt", "gte", "lt", "lte"))
@pytest.mark.parametrize("raw_field", ("amount", "score"))
def test_raw_and_derived_predicates_follow_validation_on_the_same_child(operator, raw_field):
    prepared = _prepare(
        _context(),
        context_filters=(
            read_core.ReadFilter("period", "eq", 2024),
            read_core.ReadFilter("segment", "eq", "segment_a"),
            read_core.ReadFilter("service_code", "eq", "chosen"),
        ),
        filters=(read_core.ReadFilter("output_quality", operator, 2), read_core.ReadFilter(raw_field, "lt", 9)),
    )
    ctes = _ctes(prepared)
    checked = str(ctes["projected_score_values"].element.compile(dialect=postgresql.dialect()))
    final = _sql(prepared).split("\n SELECT DISTINCT projected_score_values.entity_value", 1)[1]
    assert ("score_child_5" in checked) is (raw_field == "amount")
    assert ("score_root_8" in checked) is (raw_field == "score")
    assert "score_root_12" not in checked
    assert "UNION ALL SELECT" in checked and "validated_score_children.validated_child_id IS NULL OR" in checked
    assert "field_4" in checked and "field_3" not in checked
    assert "projected_score_values.selected_child_revision_id IS NOT NULL" in final
    assert " AND " in final and " < " in final
    assert "field_4" not in final and "score_child" not in final
    assert 2024 in prepared.statement.compile().params.values()
    assert "selected_entity_value" not in ctes


@pytest.mark.parametrize("child", (False, True))
def test_null_scores_keep_presence_and_missing_rows_fail_before_metric_filters(child):
    context = _context()
    metric = "child_cost" if child else "root_cost"
    prepared = _prepare(
        context,
        context_filters=(read_core.ReadFilter("service_code", "eq", "chosen"),) if child else (),
        order_terms=(read_core.ReadOrderTerm(metric, "desc", "last"),),
    )
    checked = _ctes(prepared)["projected_score_values"]
    guards = " ".join(str(clause.compile(dialect=postgresql.dialect())) for clause in checked.element._where_criteria)
    assert "entity_binding_id IS NOT NULL" in guards
    guarded_columns = {
        node.name
        for clause in checked.element._where_criteria
        for node in visitors.iterate(clause)
        if getattr(getattr(node, "table", None), "name", None)
        in {"custom_import_derived_root", "custom_import_derived_child"}
    }
    assert guarded_columns == {"entity_binding_id"}
    source_name = "validated_score_children" if child else "selected_score_families"
    identity = f"{source_name}.entity_binding_id"
    checked_sql = str(checked.element.compile(dialect=postgresql.dialect()))
    assert f"ELSE (SELECT {identity} UNION ALL SELECT {identity}) END IS NOT NULL" in checked_sql
    projected_states = [
        node
        for expression in checked.element.selected_columns
        for node in visitors.iterate(expression)
        if isinstance(node, Case) and getattr(node.else_, "value", None) == "null"
    ]
    assert projected_states and all(state.whens[0][1].value == "value" for state in projected_states)
    assert "projected_score_values AS MATERIALIZED" in _sql(prepared)
    assert prepared.normalized_order_terms[0].nulls == "last"
    assert set(prepared.statement.selected_columns.keys()) == {"entity_value", "sort_0"}


def test_root_projection_retains_sibling_roots():
    prepared = _prepare(
        _context(),
        context_filters=(read_core.ReadFilter("service_code", "eq", "chosen"),),
        filters=(read_core.ReadFilter("root_cost", "gt", 2),),
    )
    sql = _sql(prepared)
    assert "FULL OUTER JOIN" in sql and "count(selected_score_children.child_revision_id) OVER" in sql
    assert "custom_import_derived_root" in sql and "custom_import_derived_child" not in sql
    assert "projected_score_child" not in sql and "bool_or(" not in sql
    assert "projected_score_values.selected_child_revision_id IS NOT NULL" in sql


@pytest.mark.parametrize(
    ("key_type", "key_value"),
    [
        ("string", "chosen"),
        ("integer", 7),
        ("decimal", "7.50"),
        ("boolean", True),
        ("date", "2024-01-01"),
        ("timestamp", "2024-01-01T00:00:00Z"),
    ],
)
def test_projection_uses_typed_selector_and_owned_revision_key_without_reencoding(key_type, key_value):
    sql = _sql(
        _prepare(
            _context(key_type=key_type),
            context_filters=(read_core.ReadFilter("service_code", "eq", key_value),),
            order_terms=(read_core.ReadOrderTerm("child_cost", "asc", "last"),),
        )
    )
    assert f"complete_child_key.{read_core._SCALAR_COLUMNS[key_type]} AS selected_key_value" in sql
    assert "projected_score_child.child_key_sha256" in sql
    assert "projected_score_child.canonical_child_key" in sql
    assert "digest(" not in sql


def test_global_declaration_slots_survive_disabled_middle_fields_and_default_alias_order():
    document = json.loads(_context().definition.canonical)
    document["query"]["filterable_fields"] = ["root_cost", "child_quality"]
    document["query"]["sortable_fields"] = ["root_cost", "child_quality"]
    document["query"]["order"] = [{"field": "child_quality", "direction": "desc", "nulls": "last"}]
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(_context(), definition=definition, projection_tables=query_projection_tables(1, definition))
    assert "field_4" in context.projection_tables["custom_import_derived_child"].c
    assert "field_3" not in context.projection_tables["custom_import_derived_child"].c
    prepared = _prepare(context, context_filters=(read_core.ReadFilter("service_code", "eq", "chosen"),))
    sql = _sql(prepared)
    assert prepared.normalized_order_terms == (read_core.ReadOrderTerm("child_quality", "desc", "last"),)
    assert "custom_import_derived_child.field_4" in sql and "custom_import_derived_root" not in sql


def test_multiple_numeric_columns_share_one_join_per_scope():
    fields = ("root_cost", "root_quality", "child_cost", "child_quality")
    prepared = _prepare(
        _context(),
        context_filters=(read_core.ReadFilter("service_code", "eq", "chosen"),),
        order_terms=tuple(read_core.ReadOrderTerm(field, "asc", "last") for field in fields),
    )
    sql = _sql(prepared)
    assert sql.count("LEFT OUTER JOIN ci_snapshot_1.custom_import_derived_root ON") == 1
    assert sql.count("LEFT OUTER JOIN ci_snapshot_1.custom_import_derived_child ON") == 1
    assert isinstance(prepared.statement.selected_columns.sort_1.type, BigInteger)
    for index in (0, 2, 3):
        column_type = prepared.statement.selected_columns[f"sort_{index}"].type
        assert isinstance(column_type, Numeric) and (column_type.precision, column_type.scale) == (31, 12)


def test_legacy_and_raw_only_queries_keep_the_existing_statement():
    context = _context()
    changes_map = {
        "context_filters": (
            read_core.ReadFilter("service_code", "eq", "chosen"),
            read_core.ReadFilter("segment", "eq", "segment_a"),
        ),
        "filters": (read_core.ReadFilter("amount", "gte", 2),),
    }
    assert _sql(_prepare(context, **changes_map)) == _sql(
        _prepare(replace(context, projection_tables=None), **changes_map)
    )
    changes_map["filters"] = (read_core.ReadFilter("root_cost", "gte", 2),)
    legacy = _prepare(replace(context, projection_tables=None), **changes_map)
    assert "derived_score_values" in _sql(legacy) and "custom_import_derived_" not in _sql(legacy)
    broken = replace(context, projection_tables={})
    with pytest.raises(KeyError, match="custom_import_derived_root"):
        _prepare(broken, **changes_map)


@pytest.mark.parametrize("scope", ("root", "child"))
@pytest.mark.parametrize("omit_scope", (None, "root", "child"))
def test_native_fixture_values_compile_full_relation(scope, omit_scope):
    from api.custom_import_provider_sql import compile_npi_entity_relation
    from tests import custom_import_grouped_projection_postgres as native_projection

    bindings_by_npi = {
        npi: index
        for index, npi in enumerate(
            (native_projection.native._A, native_projection.native._B, native_projection.native._C), 1
        )
    }
    keys_by_code = {
        key: (hashlib.sha256(json.dumps([key]).encode()).digest(), json.dumps([key]))
        for _npi, _year, key, _amount, _quality in native_projection._CHILD_SCORES
    }
    context = replace(
        _context(),
        projection_tables=native_projection._projection_tables(bindings_by_npi, keys_by_code, omit_scope=omit_scope),
    )
    prepared = _prepare(
        context,
        context_filters=(read_core.ReadFilter("service_code", "eq", "chosen"),) if scope == "child" else (),
        filters=(read_core.ReadFilter("root_cost" if scope == "root" else "child_cost", "gt", 10000),),
        order_terms=(read_core.ReadOrderTerm("root_cost", "asc", "last"),),
    )
    compiled = compile_npi_entity_relation(prepared.statement)
    assert "synthetic_projected_roots" in compiled.sql
    assert ("synthetic_projected_children" in compiled.sql) is (scope == "child")
    assert "custom_import_generation_seal" in compiled.sql and "projected_score_values AS MATERIALIZED" in compiled.sql
    assert set(compiled.values) == {bind.key for bind in compiled.typed_binds}
