# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL execution proof for bounded Provider Directory selection."""

import pytest
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ClauseElement, Executable

from api import provider_directory_source_outcomes as outcomes
from api.provider_directory_source_dataset_selection import (
    _source_local_current_published_dataset_statement,
)
from tests.test_provider_directory_source_local_outcomes import (
    _outcome_selection_schema,
)


class _PostgresExplain(Executable, ClauseElement):
    inherit_cache = False

    def __init__(self, statement):
        self.statement = statement
        self._execution_options = statement._execution_options


@compiles(_PostgresExplain, "postgresql")
def _compile_postgres_explain(element, compiler, **kwargs):
    return "EXPLAIN (FORMAT JSON) " + compiler.process(element.statement, **kwargs)


def _plan_nodes(plan_map):
    yield plan_map
    for child_plan_map in plan_map.get("Plans", []):
        yield from _plan_nodes(child_plan_map)


def _assert_bounded_indexed_plans(plan_maps, source_id_groups):
    plan_node_maps = [node for plan in plan_maps for node in _plan_nodes(plan)]
    assert all(plan["Plan Rows"] <= len(source_id_groups) for plan in plan_maps)
    index_names = {node.get("Index Name") for node in plan_node_maps}
    assert "provider_directory_endpoint_dataset_endpoint_idx" in index_names
    assert "provider_directory_source_pkey" in index_names
    assert all(
        node.get("Relation Name") != "provider_directory_dataset_resource"
        for node in plan_node_maps
    )


@pytest.mark.asyncio
async def test_ranked_selector_is_bounded_on_disposable_postgres(monkeypatch):
    source_id_groups = {
        ("source-published",), ("source-reassigned",), ("source-rotated",),
        ("source-validated",), ("source-shared",),
        ("source-multi-a", "source-multi-b"),
    } | {(f"synthetic-source-{index}",) for index in range(36)}
    async with _outcome_selection_schema(monkeypatch) as (database, schema):
        statement = outcomes._current_published_dataset_statement(source_id_groups)
        translated_statement = statement.execution_options(
            schema_translate_map={"mrf": schema}
        )
        selected_row_maps = (await database.execute(translated_statement)).mappings().all()
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
            "publication_metadata_summary_json = NULL, publication_metadata_sha256 = NULL, "
            "content_proof_admission_version = NULL, content_proof_admission_kind = NULL, "
            "content_proof_admission_sha256 = NULL, content_proof_resource_types = NULL "
            "WHERE dataset_id = 'published-current';"
        )
        current_statement = _source_local_current_published_dataset_statement(
            source_id_groups
        ).execution_options(schema_translate_map={"mrf": schema})
        current_row_maps = (await database.execute(current_statement)).mappings().all()
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET content_proof_admission_sha256 = 'tampered' "
            "WHERE dataset_id = 'reassigned-current';"
        )
        tampered_row_maps = (await database.execute(current_statement)).mappings().all()
        plan_maps = (
            (await database.execute(_PostgresExplain(translated_statement))).scalars().one()[0]["Plan"],
            (await database.execute(_PostgresExplain(current_statement))).scalars().one()[0]["Plan"],
        )

    assert [row_map["dataset_id"] for row_map in selected_row_maps] == [
        "multi-current", "published-current", "reassigned-current",
        "rotated-candidate", "validated-candidate",
    ]
    assert next(
        row_map for row_map in selected_row_maps
        if row_map["dataset_id"] == "rotated-candidate"
    )["current_source_ids"] == ["source-rotated"]
    assert [row_map["dataset_id"] for row_map in current_row_maps] == [
        "multi-current", "reassigned-current", "rotated-incumbent",
        "validated-incumbent",
    ]
    assert [row_map["dataset_id"] for row_map in tampered_row_maps] == [
        "multi-current", "rotated-incumbent", "validated-incumbent",
    ]
    _assert_bounded_indexed_plans(plan_maps, source_id_groups)
