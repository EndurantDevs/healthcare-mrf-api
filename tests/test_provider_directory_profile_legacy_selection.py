# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused legacy admission-state checks for Profile dataset selection."""

import pytest

from api.provider_directory_source_dataset_selection import (
    _source_local_current_published_dataset_statement,
)
from db.connection import Database
from tests.test_provider_directory_source_local_outcomes import (
    _outcome_selection_schema,
)


_PARTIAL_ADMISSION_VALUES = {
    "publication_metadata_summary_json": "'{}'::jsonb",
    "publication_metadata_sha256": "'a' || repeat('0', 63)",
    "content_proof_admission_version": "1",
    "content_proof_admission_kind": "'generic'",
    "content_proof_admission_sha256": "'b' || repeat('0', 63)",
    "content_proof_resource_types": "ARRAY[]::varchar[]",
}


def _admission_assignments(partial_field: str | None = None) -> str:
    return ", ".join(
        f"{field} = {value if field == partial_field else 'NULL'}"
        for field, value in _PARTIAL_ADMISSION_VALUES.items()
    )


async def _set_admission_state(
    database: Database,
    schema: str,
    dataset_id: str,
    *,
    partial_field: str | None = None,
) -> None:
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        f"SET {_admission_assignments(partial_field)} "
        "WHERE dataset_id = :dataset_id;",
        dataset_id=dataset_id,
    )


async def _selected_rows(
    database: Database,
    schema: str,
    source_id_groups: set[tuple[str, ...]],
):
    statement = _source_local_current_published_dataset_statement(
        source_id_groups
    ).execution_options(schema_translate_map={"mrf": schema})
    return (await database.execute(statement)).mappings().all()


@pytest.mark.asyncio
async def test_legacy_current_all_null_selected(monkeypatch):
    async with _outcome_selection_schema(monkeypatch) as (database, schema):
        await _set_admission_state(
            database,
            schema,
            "reassigned-current",
        )
        selected_rows = await _selected_rows(
            database,
            schema,
            {("source-reassigned",)},
        )

    assert [row["dataset_id"] for row in selected_rows] == ["reassigned-current"]
    assert selected_rows[0]["publication_metadata"] == {
        "source_ids": ["source-reassigned"]
    }


@pytest.mark.parametrize("partial_field", _PARTIAL_ADMISSION_VALUES)
@pytest.mark.asyncio
async def test_partial_seal_fails_closed(monkeypatch, partial_field):
    async with _outcome_selection_schema(monkeypatch) as (database, schema):
        await _set_admission_state(
            database,
            schema,
            "reassigned-current",
            partial_field=partial_field,
        )
        selected_rows = await _selected_rows(
            database,
            schema,
            {("source-reassigned",)},
        )

    assert selected_rows == []


@pytest.mark.asyncio
async def test_tampered_full_seal_fails_closed(monkeypatch):
    async with _outcome_selection_schema(monkeypatch) as (database, schema):
        sealed_rows = await _selected_rows(
            database,
            schema,
            {("source-reassigned",)},
        )
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET content_proof_admission_sha256 = 'tampered' "
            "WHERE dataset_id = 'reassigned-current';"
        )
        selected_rows = await _selected_rows(
            database,
            schema,
            {("source-reassigned",)},
        )

    assert [row["dataset_id"] for row in sealed_rows] == ["reassigned-current"]
    assert selected_rows == []


@pytest.mark.asyncio
async def test_cross_group_legacy_rejected(monkeypatch):
    async with _outcome_selection_schema(monkeypatch) as (database, schema):
        await _set_admission_state(
            database,
            schema,
            "published-current",
        )
        selected_rows = await _selected_rows(
            database,
            schema,
            {("source-published",), ("source-shared",)},
        )

    assert selected_rows == []
