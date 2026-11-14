# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded source observation preserves logical publication and typed content checks."""

from unittest.mock import AsyncMock, MagicMock, Mock

import asyncpg
import pytest

from process import registry_fhir_catalog_observed_source as observed
from tests.test_registry_fhir_catalog_sources import descriptor, published_row, read_profile


def connection_mock():
    connection = MagicMock(
        is_in_transaction=Mock(return_value=True), fetchval=AsyncMock(return_value="repeatable read"), fetch=AsyncMock()
    )
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock(return_value=False)
    connection.transaction.return_value = transaction
    return connection


def test_replacement_scope_keeps_catalog_stable_across_named_payload_editions():
    predecessor = descriptor()
    successor = descriptor()
    successor["coordinates"]["dataset_schema"] = "payload_next"
    successor["tables"]["resource"]["schema"] = "payload_next"
    assert observed.catalog_replacement_scope(predecessor) == observed.catalog_replacement_scope(successor)

    successor["tables"]["header"]["schema"] = "separate_catalog"
    assert observed.catalog_replacement_scope(predecessor) != observed.catalog_replacement_scope(successor)


def test_replacement_scope_supports_stored_catalog_and_legacy_payload_names():
    selected_by_field = {
        "catalog_schema": "source_catalog",
        "tables": {"header": {"schema": "superseded_catalog"}},
        "coordinates": {"dataset_schema": "payload_next"},
    }
    assert observed.catalog_replacement_scope(selected_by_field) == "source_catalog"
    del selected_by_field["catalog_schema"]
    assert observed.catalog_replacement_scope(selected_by_field) == "superseded_catalog"
    del selected_by_field["tables"]
    assert observed.catalog_replacement_scope(selected_by_field) == "payload_next"
    assert observed.catalog_replacement_scope_sql("source.descriptor_json") == (
        "COALESCE(source.descriptor_json->>'catalog_schema',"
        "source.descriptor_json->'tables'->'header'->>'schema',"
        "source.descriptor_json->'coordinates'->>'dataset_schema')"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "dataset", "payload_schema"])
async def test_named_payload_keeps_canonical_header_and_exact_dataset_scope(changed):
    from process.provider_directory_generic_candidate import GenericCandidateError

    document_by_field = {
        "identity": {"dataset_id": "other" if changed == "dataset" else "dataset-one"},
        "storage": {"catalog_schema": "source_catalog", "storage_schema": "source_payload"},
        "tables": {
            "provider_directory_endpoint_dataset": {"schema": "source_payload", "name": "frozen_header"},
            "provider_directory_dataset_resource": {
                "schema": "another_snapshot" if changed == "payload_schema" else "source_payload",
                "name": "dataset_resources",
            },
        },
    }
    connection = connection_mock()
    connection.fetchrow = AsyncMock(return_value={"candidate_storage_json": document_by_field})
    invocation = observed.catalog_observation_tables(connection, read_profile(), "dataset-one")
    if changed is not None:
        with pytest.raises(GenericCandidateError):
            await invocation
    else:
        assert await invocation == {
            "header": {"schema": "source_catalog", "name": "provider_directory_endpoint_dataset"},
            "resource": {"schema": "source_payload", "name": "dataset_resources"},
        }


@pytest.mark.asyncio
async def test_collection_resolves_names_and_preserves_current_publication(monkeypatch):
    connection = connection_mock()
    selected = descriptor()
    header = published_row()
    monkeypatch.setattr(observed, "_header", AsyncMock(return_value=header))
    monkeypatch.setattr(
        observed, "_registration", AsyncMock(return_value=(selected["source_uri"], selected["source_aliases"]))
    )
    monkeypatch.setattr(observed, "catalog_observation_tables", AsyncMock(return_value=selected["tables"]))
    monkeypatch.setattr(observed, "_facts", AsyncMock(return_value=selected["admission"]["families"]))
    result = await observed.collect_fhir_catalog_observation(
        connection, read_profile(), endpoint_id="endpoint-one", dataset_id="dataset-one"
    )
    assert result.status == "accepted" and result.descriptor == selected
    observed._header.assert_awaited_once_with(connection, read_profile(), "endpoint-one", "dataset-one", True)
    observed._facts.assert_awaited_once_with(
        connection, read_profile(), "dataset-one", selected["tables"], selected["source_uri"]
    )


@pytest.mark.asyncio
async def test_header_reads_canonical_name_and_rejects_ambiguous_current_generation():
    connection = connection_mock()
    header_by_field = {**published_row(), "hash_contract_type": None}
    connection.fetch.return_value = [header_by_field]
    assert (
        await observed.read_fhir_catalog_observation_header(
            connection, read_profile(), "endpoint-one", "dataset-one", True
        )
        == header_by_field
    )
    assert 'FROM "source_catalog"."provider_directory_endpoint_dataset"' in connection.fetch.await_args.args[0]
    header_by_field["current_count"] = 2
    with pytest.raises(ValueError, match="publication_invalid"):
        await observed.read_fhir_catalog_observation_header(
            connection, read_profile(), "endpoint-one", "dataset-one", True
        )


@pytest.mark.asyncio
async def test_registration_requires_exact_ordered_aliases_and_source_uri():
    connection = connection_mock()
    connection.fetch.side_effect = [
        [{"endpoint_id": "endpoint-one", "source_uri": "https://directory.example.test/fhir/"}],
        [
            {"endpoint_id": "endpoint-one", "source_id": "alias-a"},
            {"endpoint_id": "endpoint-one", "source_id": "alias-a"},
        ],
    ]
    with pytest.raises(ValueError, match="registration_invalid"):
        await observed.read_fhir_catalog_observation_registration(connection, read_profile(), "endpoint-one")


@pytest.mark.asyncio
async def test_observation_count_mismatch_rejects_incomplete_stream(monkeypatch):
    connection = connection_mock()
    connection.fetch.return_value = [{"resource_type": "Organization", "rows": 1}]

    async def pages(*args, **kwargs):
        if False:
            yield ()

    monkeypatch.setattr(observed, "iter_fhir_catalog_raw_pages", pages)
    with pytest.raises(ValueError, match="resource_count_changed"):
        await observed.collect_fhir_catalog_observation_facts(
            connection, read_profile(), "dataset-one", descriptor()["tables"], "https://directory.example.test/fhir/"
        )
    assert 'FROM "source_payload"."dataset_resources"' in connection.fetch.await_args.args[0]


@pytest.mark.asyncio
async def test_database_failure_rolls_back_observation_savepoint(monkeypatch):
    connection = connection_mock()
    error = asyncpg.UndefinedTableError("synthetic missing relation")
    monkeypatch.setattr(observed, "_header", AsyncMock(side_effect=error))
    with pytest.raises(observed.FHIRCatalogObservedSourceError, match="read_unavailable"):
        await observed.collect_fhir_catalog_observation(
            connection, read_profile(), endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
    assert connection.transaction.return_value.__aexit__.await_args.args[1] is error
