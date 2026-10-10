# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated local payload routing keeps logical source authority separate."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_cohort_authority as authority
from process import registry_ptg_graph_reader as graph
from process import registry_ptg_office_witness as office
from process import registry_ptg_producer_scope as producer
from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import ptg2_schema
from tests import test_registry_ptg_cohort_authority as cohort_fixture
from tests import test_registry_ptg_graph_reader as graph_fixture
from tests import test_registry_ptg_producer_scope as producer_fixture
from tests.test_ptg2_physical_binding import _binding


def _session(*results):
    for result in results:
        result.scalar_one_or_none = result.scalar_one
    session = cohort_fixture._session(*results)
    transaction = object()
    session.get_transaction = lambda: transaction
    return session


def _specification(binding):
    return cohort_fixture._specification(snapshot_id=binding.snapshot_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("marker", ["physical_binding", "physical_binding_contract", "local_data_preparation"])
async def test_every_declared_marker_uses_the_installed_resolver(monkeypatch, marker):
    binding = _binding()
    session = _session(cohort_fixture._Result(scalar=True))
    resolver = AsyncMock(return_value=binding)
    monkeypatch.setattr(native, "resolve_local_physical_binding", resolver)
    monkeypatch.setattr(ptg2_schema, "resolve_ptg2_schema", lambda: "synthetic_ptg")
    assert await authority._physical_binding(session, _specification(binding)) == binding
    query = str(session.execute.await_args.args[0])
    assert "'" + marker + "'" in query and "LEFT JOIN" in query
    resolver.assert_awaited_once_with(session, binding.snapshot_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["schema", "descriptor", "resolver", "transaction"])
async def test_local_custody_failure_has_no_canonical_fallback(monkeypatch, drift):
    binding = _binding()
    session = _session(cohort_fixture._Result(scalar=True))
    resolver = AsyncMock(return_value=binding)
    if drift == "descriptor":
        resolver.return_value = SimpleNamespace(**binding.__dict__)
    elif drift == "resolver":
        resolver.side_effect = native.PTG2PhysicalBindingError("changed")
    elif drift == "transaction":

        async def changed(*_):
            session.get_transaction = lambda: object()
            return binding

        resolver.side_effect = changed
    monkeypatch.setattr(native, "resolve_local_physical_binding", resolver)
    monkeypatch.setattr(
        ptg2_schema, "resolve_ptg2_schema", lambda: "other_schema" if drift == "schema" else "synthetic_ptg"
    )
    with pytest.raises(authority.RegistryPTGCohortAuthorityError):
        await authority._physical_binding(session, _specification(binding))
    assert session.execute.await_count == 1
    if drift == "schema":
        resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_canonical_source_keeps_existing_resolution(monkeypatch):
    session = _session(cohort_fixture._Result(scalar=False))
    resolver = AsyncMock()
    monkeypatch.setattr(native, "resolve_local_physical_binding", resolver)
    assert await authority._physical_binding(session, cohort_fixture._specification()) is None
    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_state_binds_payload_roots_and_source_vector(monkeypatch):
    binding = _binding()
    specification = _specification(binding)
    monkeypatch.setattr(authority, "_physical_binding", AsyncMock(return_value=binding))
    records = [cohort_fixture._source()]
    source_by_field = {"snapshot_key": binding.payload_snapshot_key, "import_run_id": "logical-run"}
    assignments = AsyncMock(return_value=records)
    monkeypatch.setattr(authority, "_source_assignments", assignments)
    session = _session(cohort_fixture._Result(rows=[source_by_field]))
    assert await authority._resolved_source_state(session, specification) == (source_by_field, records, binding)
    query, parameters = session.execute.await_args.args
    assert '"synthetic_ptg".ptg2_snapshot snapshot' in str(query)
    for table in ("ptg2_v3_snapshot_layout", "ptg2_v4_snapshot_map_root", "ptg2_v4_finalizer_map_root"):
        assert f'"{binding.schema_name}".{table}' in str(query)
    assert parameters == {"snapshot_id": binding.snapshot_id, "payload_key": binding.payload_snapshot_key}
    assignments.assert_awaited_once_with(session, f'"{binding.schema_name}"', binding.payload_snapshot_id)


@pytest.mark.asyncio
async def test_graph_reads_payload_schema_and_preserves_encoded_payload_key(monkeypatch):
    binding = replace(_binding(), payload_snapshot_key=11)
    graph_fixture._source_state.__wrapped__(monkeypatch)
    monkeypatch.setattr(authority, "_physical_binding", AsyncMock(return_value=binding))
    monkeypatch.setattr(graph, "_native", lambda *_: {"owner_keys": [], "coordinates": []})
    monkeypatch.setattr(graph, "_checked_proof", lambda *_: b"verified")
    session = graph_fixture._Session()
    assert (
        await graph._verify_page(
            session,
            _specification(binding),
            graph_fixture._page(),
            read_budget=graph.RegistryPTGGraphReadBudget(1048576),
        )
        == b"verified"
    )
    payload_queries = [(str(sql), parameters) for sql, parameters in session.queries if "ptg2_v4_" in str(sql)]
    assert payload_queries
    assert all(
        f'"{binding.schema_name}".' in sql and parameters["snapshot_key"] == 11 for sql, parameters in payload_queries
    )


@pytest.mark.asyncio
async def test_office_scope_uses_logical_identity_and_payload_dictionary(monkeypatch):
    binding = _binding()
    specification = _specification(binding)
    monkeypatch.setattr(authority, "_physical_binding", AsyncMock(return_value=binding))
    session = _session(cohort_fixture._Result(rows=[cohort_fixture._page()]))
    scope_by_field = {
        "snapshot_id": binding.snapshot_id,
        "binding_source_key": "binding",
        "company_key": "company",
        "cohort_id": "cohort",
        "evidence": {"selected_dense_source_keys": [0]},
    }
    page = await office._source_page(
        session,
        SimpleNamespace(source_specification=specification),
        scope_by_field,
        {"snapshot_key": binding.payload_snapshot_key},
        0,
        '"office".office_assertion',
    )
    query, parameters = session.execute.await_args.args
    assert parameters["snapshot_id"] == binding.snapshot_id
    assert parameters["payload_snapshot_id"] == binding.payload_snapshot_id
    assert "page.snapshot_id IS DISTINCT FROM :snapshot_id" in str(query)
    assert "source.snapshot_id=:payload_snapshot_id" in str(query)
    assert f'"{binding.schema_name}".ptg2_v3_provider_group' in str(query)
    assert page["graph_identity"]["snapshot_key"] == binding.payload_snapshot_key


@pytest.mark.asyncio
async def test_producer_file_admission_uses_payload_source_versions(monkeypatch):
    binding = _binding()
    monkeypatch.setattr(producer, "_physical_binding", AsyncMock(return_value=binding))
    session = producer_fixture._session(producer_fixture._Result(rows=producer_fixture._source_rows()))
    versions = producer._command_document(
        producer_fixture._command(), producer_fixture._specification(), producer_fixture._actor()
    )["file_versions"]
    assert await producer._selected_versions(session, _specification(binding), versions) == [1]
    query, parameters = session.execute.await_args.args
    assert f'"{binding.schema_name}".ptg2_v3_snapshot_source' in str(query)
    assert parameters == {"snapshot_id": binding.payload_snapshot_id}
