# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the Guardian source through the catalog, worker admission, and candidate writer."""

from contextlib import asynccontextmanager
from functools import partial
from importlib import import_module
from sqlite3 import connect
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_source_worker as worker
from api import provider_directory_sources as catalog
from process import guardian_directory_integration as guardian
from process import provider_directory_source_proxy as proxy
from process import provider_directory_source_selection as selection

directory = import_module("process.provider_directory_fhir")


def test_scoped_catalog_selection_and_worker_admission(monkeypatch):
    source_by_field = guardian.source_row(None)
    metadata = source_by_field["metadata_json"]
    assert metadata["provider_directory_coverage_mode"] == "first_party_employer_dental_windows"
    assert "provider_directory_fully_enumerable_resources" not in metadata
    source_ids = [guardian.SOURCE_ID]
    task_params_map = {
        "source_ids": source_ids,
        "provider_directory_endpoint_scope": guardian.ENDPOINT_SCOPE,
        "probe": False,
        "import_resources": True,
    }
    monkeypatch.setattr(catalog, "has_fresh_source_route", lambda *_: False)
    assert all(
        catalog_entry["source_ids"] != source_ids
        for catalog_entry in catalog.provider_directory_source_catalog()["items"]
    )
    monkeypatch.setattr(
        catalog,
        "has_fresh_source_route",
        lambda source, base: (source, base) == (guardian.SOURCE_ID, guardian.ENDPOINT_SCOPE),
    )
    catalog_entries = [
        catalog_entry
        for catalog_entry in catalog.provider_directory_source_catalog()["items"]
        if catalog_entry["source_ids"] == source_ids
    ]
    assert len(catalog_entries) == 1
    catalog_entry = catalog_entries[0]
    assert catalog_entry["runnable"] and catalog_entry["resources"] == list(guardian.RESOURCE_TYPES)
    assert selection.requested_directory_adapter(source_ids, None, None) is guardian
    assert selection.search_directory_adapter(source_by_field) is guardian
    assert directory._endpoint_dataset_expected_resources([source_by_field]) == guardian.RESOURCE_TYPES
    with pytest.raises(ValueError, match="single_source_required"):
        selection.requested_directory_adapter([*source_ids, "another-source"], None, None)
    with pytest.raises(ValueError, match="source_identity_changed"):
        selection.search_directory_adapter({**source_by_field, "canonical_api_base": "https://example.org"})
    with pytest.raises(ValueError, match="full_deferred_acquisition_required"):
        selection.require_directory_acquisition(source_by_field, source_ids, ["Location"], True)
    disabled = SimpleNamespace(use_proxy=lambda *_: False)
    enabled = SimpleNamespace(use_proxy=lambda sid, base: sid == guardian.SOURCE_ID and base == guardian.ENDPOINT_SCOPE)
    for route, policy in [(None, enabled), ({}, disabled)]:
        with pytest.raises(ValueError, match="worker_proxy_route_required"):
            worker._require_selected_directory_route(task_params_map, route, policy)
    worker._require_selected_directory_route(task_params_map, {}, enabled)
    with pytest.raises(ValueError, match="acquisition_mode_invalid"):
        worker._require_selected_directory_route({**task_params_map, "probe": True}, {}, enabled)


@pytest.fixture
def candidate_fixture(monkeypatch):
    """Bind a retained edition and its selected source to a configured route."""
    resource_map = {"resourceType": "Organization", "id": "example-network", "name": "Example network"}
    counts_by_family = {family: int(family == "Organization") for family in guardian.RESOURCE_TYPES}
    retained = SimpleNamespace(iter_resources=lambda: iter([resource_map]))

    @asynccontextmanager
    async def acquire(plan, root, binding, *, require_retained, **_callbacks):
        assert plan is search_plan
        assert root == "original-run" and binding is route and require_retained
        yield retained, {"resource_counts": counts_by_family}

    route = object()
    search_plan = object()
    monkeypatch.setattr(guardian, "load_search_plan", lambda: search_plan)
    monkeypatch.setattr(guardian.capture, "acquire_directory", acquire)
    checkpoint = SimpleNamespace(
        dataset_id="candidate",
        acquisition_root_run_id="original-run",
        retry_of_run_id="parent-run",
        allow_initial_nomi_capture=False,
    )
    source_by_field = {
        **guardian.source_row(None),
        "_semantic_projection_as_of": "2026-01-01",
        "_endpoint_dataset_id": "candidate",
        "_pagination_checkpoint_context": checkpoint,
        "_resource_hash_contract": "test-resource-contract",
    }
    scope = SimpleNamespace(bindings={guardian.SOURCE_ID: route}, credentials={})
    monkeypatch.setattr(proxy, "current_run", lambda: scope)
    return source_by_field, counts_by_family


@pytest.mark.asyncio
async def test_completed_capture_uses_normal_parser_and_copy_counts(monkeypatch, candidate_fixture):
    """A failed COPY or changed total rolls back the writes for this capture."""
    source_by_field, counts_by_family = candidate_fixture
    copied = connect(":memory:")
    copied.execute("CREATE TABLE resource(id TEXT)")

    @asynccontextmanager
    async def transaction(db):
        assert db is directory.db
        with copied:
            yield

    async def copy_rows(_model, parsed_rows):
        copied.execute("INSERT INTO resource VALUES (?)", (parsed_rows[0]["resource_id"],))
        return len(parsed_rows)

    monkeypatch.setattr(guardian.capture, "candidate_write_scope", transaction)
    persist = AsyncMock(side_effect=copy_rows)
    import_candidate = partial(
        guardian.import_group,
        directory,
        source_by_field,
        run_id="retry",
        persist=persist,
        progress_callback=AsyncMock(),
    )
    import_summary = await import_candidate(cancel_check=AsyncMock())
    assert import_summary[0] == [guardian.SOURCE_ID] and import_summary[2] == counts_by_family
    assert import_summary[1]["Organization"]["guardian_capture"]["resource_counts"] == counts_by_family
    model, parsed_rows = persist.await_args.args
    assert model.__name__ == "ProviderDirectoryOrganization" and len(parsed_rows) == 1
    assert import_summary[4]["Location"]["sources_empty"] == 1
    assert import_summary[4]["Organization"]["sources_empty"] == 0
    assert copied.execute("SELECT count(*) FROM resource").fetchone() == (1,)
    with pytest.raises(RuntimeError, match="canceled"):
        await import_candidate(cancel_check=AsyncMock(side_effect=[None, RuntimeError("canceled")]))
    assert copied.execute("SELECT count(*) FROM resource").fetchone() == (1,)
    counts_by_family["Location"] = 1
    with pytest.raises(RuntimeError, match="counts_changed"):
        await import_candidate(cancel_check=AsyncMock())
    assert copied.execute("SELECT count(*) FROM resource").fetchone() == (1,)
    persist.side_effect = None
    persist.return_value = 0
    with pytest.raises(RuntimeError, match="copy_incomplete"):
        await import_candidate(cancel_check=AsyncMock())
    copied.close()
