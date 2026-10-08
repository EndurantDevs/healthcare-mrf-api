# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Interchangeable payload keys never reopen the canonical or another family."""

from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import ptg2_audit_occurrences as audit_api
from api import ptg2_candidate_audit_codes as audit_codes
from api import ptg2_candidate_audit_integrity as integrity
from api import ptg2_code_scope as code_scope
from api import ptg2_serving as serving
from api import ptg2_tables as tables_module
from api.ptg2_candidate_audit import PTG2_CANDIDATE_AUDIT_ACCESS_ARG, PTG2CandidateAuditAccess
from api.ptg2_v4_graph import V4GraphRoot
from tests import ptg2_v3_audit_occurrences_support as audit_fixture
from tests.ptg2_candidate_audit_batch_postgres_fixture import SOURCE_DIGEST, source_witness
from tests.test_ptg2_candidate_audit_batch_integrity import _persisted_audit_rows, _sample_serving_tables
from tests.test_ptg2_physical_binding import _binding
from tests.test_ptg2_v4_serving_exact_paths import _tables


def _local_tables(ordinal=1):
    """Use different destination identities with identical producer coordinates."""
    binding = replace(_binding(), snapshot_id=f"destination-{ordinal}", dataset_id=UUID(int=ordinal))
    return replace(
        _tables(),
        snapshot_id=binding.snapshot_id,
        physical_binding=binding,
        shared_snapshot_key=binding.payload_snapshot_key,
        source_key=f"source-{ordinal}",
        atom_key_bits=24,
        price_key_block_span=512,
        atom_key_block_span=512,
        price_dictionary_item_count=1,
        price_dictionary_block_bytes=512,
        price_atom_constant_values={"billing_class": "professional"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinal", [1, 2, 3], ids=["main", "transplant", "retained"])
async def test_called_search_reacquires_exact_family_in_owning_session(monkeypatch, ordinal):
    tables = _local_tables(ordinal)
    session = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())
    resolution = AsyncMock(return_value=tables)
    monkeypatch.setattr(tables_module, "snapshot_serving_tables", resolution)
    read = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(serving, "_search_manifest_serving_table", read)
    await serving.search_ptg2_serving_table(session, tables.snapshot_id, {}, object(), serving_tables=tables)
    resolution.assert_awaited_once_with(session, tables.snapshot_id, candidate_audit_access=None)
    assert read.await_args.args[0] is session and read.await_args.args[4] is tables
    session.rollback.assert_not_awaited()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_local_audit_sample_uses_bound_payload():
    persisted_rows = _persisted_audit_rows()
    tables = replace(_local_tables(), source_count=2, audit_sample=_sample_serving_tables(persisted_rows).audit_sample)
    session = SimpleNamespace(execute=AsyncMock(return_value=persisted_rows))

    occurrences = await integrity.validate_persisted_audit_sample(session, tables)

    assert [occurrence.occurrence_id for occurrence in occurrences] == [b"a" * 32, b"b" * 32]
    session.execute.assert_awaited_once()
    query, parameters_by_name = session.execute.await_args.args
    assert f"FROM {tables.physical_binding.schema_name}.ptg2_v3_audit_occurrence" in str(query)
    assert parameters_by_name == {"shared_snapshot_key": tables.physical_binding.payload_snapshot_key}


@pytest.mark.asyncio
async def test_local_witness_reads_authenticated_payload():
    witness_payload, witness_metadata = source_witness()
    tables = replace(_local_tables(), source_count=1, source_witness=witness_metadata)
    session = SimpleNamespace(execute=AsyncMock(return_value=[{"part_number": 0, "payload": witness_payload}]))

    scope = await integrity._sealed_witness_challenges(session, tables, (SOURCE_DIGEST,))

    assert scope.record_count == 2
    assert sum(challenge.multiplicity for challenge in scope.challenges) == 2
    assert {challenge.source_artifact_key for challenge in scope.challenges} == {0}
    session.execute.assert_awaited_once()
    query, parameters_by_name = session.execute.await_args.args
    for table_name in ("ptg2_v3_source_audit_witness", "ptg2_v3_source_audit_witness_part"):
        assert f"FROM {tables.physical_binding.schema_name}.{table_name}\n" in str(query)
    assert parameters_by_name == {"shared_snapshot_key": tables.physical_binding.payload_snapshot_key}


@pytest.mark.asyncio
async def test_local_occurrences_preserve_payload_and_authority(monkeypatch):
    binding = _local_tables().physical_binding
    tables = replace(
        audit_fixture._serving_tables(),
        snapshot_id=binding.snapshot_id,
        physical_binding=binding,
        shared_snapshot_key=binding.payload_snapshot_key,
    )
    access = PTG2CandidateAuditAccess(tables.snapshot_id, tables.source_key, audit_fixture.PLAN_ID, "group")
    session = audit_fixture.RecordingSession(audit_fixture._audit_digest_rows(2))
    atoms = audit_fixture._patch_resolution(monkeypatch)
    audit_api.current_snapshot_id.return_value = tables.snapshot_id
    audit_api.snapshot_serving_tables.return_value = tables

    response = await audit_api.audit_occurrences_payload(
        session,
        audit_fixture._args(
            snapshot_id=tables.snapshot_id, plan_market_type="group", **{PTG2_CANDIDATE_AUDIT_ACCESS_ARG: access}
        ),
    )

    assert len(response["items"]) == 2 and response["resolved_snapshot_id"] == tables.snapshot_id
    page_sql, parameters_by_name = session.calls[0]
    for table_name in ("ptg2_v3_audit_occurrence", "ptg2_v3_code", "ptg2_v3_provider_set"):
        assert f"{binding.schema_name}.{table_name}" in page_sql
        assert f"{audit_api.PTG2_SCHEMA}.{table_name}" not in page_sql
    for table_name in ("ptg2_v3_snapshot_plan_scope", "ptg2_v3_snapshot_scope"):
        assert f"{audit_api.PTG2_SCHEMA}.{table_name}" in page_sql
    assert parameters_by_name["snapshot_id"] == tables.snapshot_id
    assert parameters_by_name["shared_snapshot_key"] == binding.payload_snapshot_key
    assert parameters_by_name["plan_market_type"] == "group"
    assert parameters_by_name["plan_ids"] == [audit_fixture.PLAN_ID, "123456789"]
    assert f"FROM {binding.schema_name}.ptg2_v3_audit_occurrence" in session.calls[1][0]
    assert session.calls[1][1] == {"shared_snapshot_key": binding.payload_snapshot_key}
    assert atoms.await_args.args == (session, binding.payload_snapshot_key)
    assert atoms.await_args.kwargs["schema_name"] == binding.schema_name
    audit_api.fetch_snapshot_source_set_metadata.assert_awaited_once_with(
        session,
        schema_name=audit_api.PTG2_SCHEMA,
        logical_snapshot_id=tables.snapshot_id,
        expected_source_count=2,
        serving_tables=tables,
        candidate_audit_access=access,
    )
    audit_api.fetch_snapshot_source_provenance.assert_awaited_once_with(
        session,
        schema_name=audit_api.PTG2_SCHEMA,
        logical_snapshot_id=tables.snapshot_id,
        source_keys={1},
        expected_source_count=2,
        serving_tables=tables,
        candidate_audit_access=access,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["missing", "other-family", "other-snapshot"])
async def test_cached_local_descriptor_never_falls_back(monkeypatch, drift):
    tables = _local_tables()
    changed = replace(tables, physical_binding=None) if drift == "missing" else _local_tables(2)
    resolution = AsyncMock(return_value=changed)
    monkeypatch.setattr(tables_module, "snapshot_serving_tables", resolution)
    read = AsyncMock()
    monkeypatch.setattr(serving, "_search_manifest_serving_table", read)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="physical read"):
        await serving.search_ptg2_serving_table(
            object(),
            "wrong-snapshot" if drift == "other-snapshot" else tables.snapshot_id,
            {},
            object(),
            serving_tables=tables,
        )
    read.assert_not_awaited()
    if drift == "other-snapshot":
        resolution.assert_not_awaited()


@pytest.mark.asyncio
async def test_reverse_network_sessions_reauthenticate_each_exact_family(monkeypatch):
    sessions = []

    @asynccontextmanager
    async def network_session(*, independent):
        assert independent is True
        session = object()
        sessions.append(session)
        yield session

    first, second = _local_tables(1), _local_tables(2)
    resolution = AsyncMock(side_effect=[first, second])
    monkeypatch.setattr(serving.sa_db, "reader_session", network_session)
    monkeypatch.setattr(tables_module, "snapshot_serving_tables", resolution)
    read = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(serving, "_search_ptg2_manifest_provider_procedures", read)
    for tables in (first, second):
        network_response = await serving._search_provider_procedures_network(
            tables.source_key,
            tables.snapshot_id,
            1234567890,
            {},
            object(),
            serving_tables=tables,
        )
        assert network_response[:2] == (tables.source_key, tables.snapshot_id)
    assert sessions[0] is not sessions[1]
    for index, tables in enumerate((first, second)):
        assert resolution.await_args_list[index].args == (sessions[index], tables.snapshot_id)
        assert read.await_args_list[index].args[0] is sessions[index]
        assert read.await_args_list[index].kwargs["serving_tables"] is tables


@pytest.mark.asyncio
async def test_legacy_descriptor_and_schema_are_unchanged(monkeypatch):
    tables = _tables()
    resolution = AsyncMock()
    monkeypatch.setattr(tables_module, "snapshot_serving_tables", resolution)
    assert await tables_module.read_serving_tables(object(), "legacy", serving_tables=tables) is tables
    resolution.assert_not_awaited()
    assert serving._payload_schema(tables) == serving.PTG2_SCHEMA
    assert serving._shared_v3_code_table(tables) == f"{serving.PTG2_SCHEMA}.ptg2_v3_code"
    assert serving._shared_v3_price_attr_table(tables) == f"{serving.PTG2_SCHEMA}.ptg2_v3_price_attr"


@pytest.mark.asyncio
async def test_native_dictionary_and_code_queries_preserve_logical_plan_scope():
    tables = _local_tables()
    binding = tables.physical_binding
    session = SimpleNamespace(execute=AsyncMock(return_value=[]))
    await serving._version_three_dictionary_query(session, tables, {("billing_class", 0)})
    assert binding.relation("ptg2_v3_price_attr") in str(session.execute.await_args.args[0])
    await serving._manifest_reverse_code_rows(
        session,
        tables,
        code_keys=[1],
        requested_plan="plan-a",
        code_value="99213",
        q_text="",
    )
    query, parameters_by_name = session.execute.await_args.args
    assert binding.relation("ptg2_v3_code") in str(query)
    assert f"{serving.PTG2_SCHEMA}.ptg2_v3_snapshot_plan_scope" in str(query)
    assert parameters_by_name["logical_snapshot_id"] == binding.snapshot_id
    assert parameters_by_name["shared_snapshot_key"] == binding.payload_snapshot_key
    await serving._shared_rate_code_scope_rows(
        session,
        tables,
        plan_id="plan-a",
        plan_market_type="group",
        reported_code="99213",
        code_system="CPT",
    )
    assert binding.relation("ptg2_v3_code") in str(session.execute.await_args.args[0])
    assert binding.relation("ptg2_v4_npi_scope") == serving._ptg2_npi_scope_table(tables)
    assert f"{serving.PTG2_SCHEMA}.npi" in serving._ptg2_individual_npi_exists_sql("candidate.npi")


@pytest.mark.asyncio
async def test_extracted_code_loader_preserves_physical_and_logical_scope():
    tables = _local_tables()
    binding = tables.physical_binding
    code_by_field = {"code_key": 7, "reported_code_system": "CPT", "reported_code": "99213"}
    session = SimpleNamespace(execute=AsyncMock(return_value=[code_by_field]))
    assert await code_scope.load_sealed_code_rows(
        session,
        tables,
        {"code_system": "CPT", "code": "99213", "plan_id": "plan-a", "plan_market_type": "group"},
    ) == [code_by_field]
    session.execute.assert_awaited_once()
    query, parameters_by_name = session.execute.await_args.args
    sql = str(query)
    assert f"FROM {binding.relation('ptg2_v3_code')} code_metadata" in sql
    assert f"JOIN {serving.PTG2_SCHEMA}.ptg2_v3_snapshot_scope physical_scope" in sql
    assert f"FROM {serving.PTG2_SCHEMA}.ptg2_v3_snapshot_plan_scope plan_scope" in sql
    assert "physical_scope.snapshot_id = :logical_snapshot_id" in sql
    assert "physical_scope.coverage_scope_id = code_metadata.coverage_scope_id" in sql
    assert "plan_scope.snapshot_id = :logical_snapshot_id" in sql
    assert "code_metadata.snapshot_key = :shared_snapshot_key" in sql
    assert "plan_scope.plan_id = :plan_id" in sql
    assert "plan_scope.plan_market_type = :plan_market_type" in sql
    assert parameters_by_name["logical_snapshot_id"] == binding.snapshot_id
    assert parameters_by_name["shared_snapshot_key"] == binding.payload_snapshot_key
    assert parameters_by_name["plan_id"] == "plan-a"
    assert parameters_by_name["plan_market_type"] == "group"


@pytest.mark.asyncio
async def test_actual_candidate_code_caller_routes_bound_table(monkeypatch):
    tables = _local_tables()
    access = PTG2CandidateAuditAccess(tables.snapshot_id, "source-1", "plan-a", "group")
    session = SimpleNamespace(execute=AsyncMock(return_value=[]))
    index = await audit_codes.candidate_code_records_by_pair(session, tables, access, ())
    assert index.by_key == {}
    assert tables.physical_binding.relation("ptg2_v3_code") in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("representation", ["direct_v1", "pattern_v1"])
async def test_graph_hops_keep_selected_schema(monkeypatch, representation):
    tables = _local_tables()
    session = object()
    root = AsyncMock(return_value=V4GraphRoot(tables.shared_snapshot_key, representation, b"p" * 32))
    prefixes = AsyncMock(return_value={1: (2,)})
    members = AsyncMock(return_value={1: (3,)} if representation == "direct_v1" else {2: (3,)})
    monkeypatch.setattr(serving, "load_v4_graph_root", root)
    monkeypatch.setattr(serving, "lookup_v4_relation_member_prefixes", prefixes)
    monkeypatch.setattr(serving, "lookup_v4_relation_members", members)
    assert await serving._v4_members_through_projection(
        session,
        tables,
        owner_keys=(1,),
        direct_relation="set_groups_direct",
        projection_relation="set_patterns",
        projected_member_relation="pattern_groups",
        max_members=10,
    ) == {1: (3,)}
    for call in root.await_args_list + prefixes.await_args_list + members.await_args_list:
        assert call.args[0] is session
        assert call.kwargs["schema_name"] == tables.physical_binding.schema_name


@pytest.mark.asyncio
async def test_price_and_forward_readers_keep_payload_keys_and_schema(monkeypatch):
    tables = _local_tables()
    session = object()
    memberships, atoms, forward = AsyncMock(return_value={0: (1,)}), AsyncMock(return_value={1: object()}), AsyncMock()
    monkeypatch.setattr(serving, "lookup_shared_price_atom_memberships_from_db", memberships)
    monkeypatch.setattr(serving, "lookup_shared_price_atoms_from_db", atoms)
    monkeypatch.setattr(serving, "lookup_serving_binary_by_code_from_db", forward)
    await serving._version_three_price_memberships(session, tables, (0,), 24, None)
    await serving._version_three_price_atoms(session, tables, (1,), 24, None)
    await serving._lookup_shared_forward_rows(session, tables, 1)
    for reader in (memberships, atoms, forward):
        assert reader.await_args.args[0] is session
        assert reader.await_args.kwargs["schema_name"] == tables.physical_binding.schema_name
    assert memberships.await_args.args[1] == atoms.await_args.args[1] == tables.shared_snapshot_key
    assert forward.await_args.kwargs["shared_snapshot_key"] == tables.shared_snapshot_key


@pytest.mark.asyncio
async def test_provenance_forwards_only_explicit_local_audit_access(monkeypatch):
    tables = _local_tables()
    access = PTG2CandidateAuditAccess(tables.snapshot_id, "source-1", "plan-a", "group")
    read = AsyncMock(return_value={0: {}})
    monkeypatch.setattr(serving, "fetch_snapshot_source_provenance", read)
    await serving._ptg2_source_provenance_for_rows(
        object(),
        tables,
        [{"source_key": 0}],
        candidate_audit_access=access,
    )
    assert read.await_args.kwargs["serving_tables"] is tables
    assert read.await_args.kwargs["candidate_audit_access"] is access
    assert read.await_args.kwargs["logical_snapshot_id"] == tables.snapshot_id
    await serving._ptg2_source_provenance_for_rows(
        object(), replace(_tables(), snapshot_id="legacy"), [{"source_key": 0}]
    )
    assert "serving_tables" not in read.await_args.kwargs
    assert "candidate_audit_access" not in read.await_args.kwargs


@pytest.mark.parametrize("same_namespace", [False, True])
def test_all_payload_caches_distinguish_same_key_native_families(monkeypatch, same_namespace):
    first, second = _local_tables(1), _local_tables(2)
    if same_namespace:
        second = replace(first, physical_binding=replace(first.physical_binding, schema_oid=1001))
    monkeypatch.setattr(serving, "_PTG2_PROVIDER_NPI_PREFIX_CACHE", OrderedDict())
    monkeypatch.setattr(serving, "_PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE", OrderedDict())
    serving._cache_provider_npi_prefix(first, "set", 1, (1234567890,), is_complete=True)
    assert serving._cached_provider_npi_prefixes(first, ("set",), 1) == ({"set": (1234567890,)}, ())
    assert serving._cached_provider_npi_prefixes(second, ("set",), 1) == ({}, ("set",))
    serving._cache_provider_set_ids_for_npis(serving._provider_cache_snapshot_key(first), {1234567890: ("set",)})
    assert serving._cached_provider_set_ids_for_npis(serving._provider_cache_snapshot_key(second), (1234567890,)) == (
        {},
        (1234567890,),
    )
    assert serving._filtered_provider_prefix_cache_key(
        first, "set", {}, 1
    ) != serving._filtered_provider_prefix_cache_key(second, "set", {}, 1)
    assert serving._version_three_price_cache_layout(first) != serving._version_three_price_cache_layout(second)
    assert serving._provider_cache_snapshot_key(_tables()) == _tables().shared_snapshot_key


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["columns", "directory", "procedure"])
async def test_optional_failures_preserve_outer_family_read_fence(query):
    events = ["family-read-locks"]

    @asynccontextmanager
    async def savepoint():
        events.append("savepoint")
        try:
            yield
        except RuntimeError:
            events.append("savepoint-rollback")
            raise

    session = SimpleNamespace(
        begin_nested=savepoint,
        execute=AsyncMock(side_effect=RuntimeError("optional relation unavailable")),
        rollback=AsyncMock(),
        commit=AsyncMock(),
    )
    if query == "columns":
        assert await serving._ptg2_table_columns(session, "mrf.npi") == frozenset()
    elif query == "directory":
        assert (
            await serving._provider_directory_corroboration_by_key(
                session,
                "mrf.provider_directory_address_corroboration",
                [(1234567890, str(UUID(int=1)))],
                plan_id=None,
                snapshot_id=None,
                source_key=None,
            )
            is None
        )
    else:
        assert (
            await serving._procedure_details_for_rows(
                session, [{"reported_code_system": "CPT", "reported_code": "99213"}]
            )
            == {}
        )
    assert events == ["family-read-locks", "savepoint", "savepoint-rollback"]
    session.rollback.assert_not_awaited()
    session.commit.assert_not_awaited()
