# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A readiness flag cannot replace the native profile of any searched heap."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from process import network_membership_serving_indexes as indexes
from process.network_compact_layout import CompactAddressRelation, CompactNetworkLayout


def _candidate(compact=True):
    return {
        "schema_name": "candidate_example",
        "validation_json": {
            "serving_readiness": {
                "component": "compact_network_serving" if compact else "unified_address_serving",
                "index_definition_sha256": "a" * 64,
            },
            "writer_closure": {"relation_oids": {"entity_address_unified": 44}},
        },
    }


@pytest.mark.parametrize("table_oid", [0, -1, True, 1.0, "44", 4294967296])
def test_geo_profile_refuses_non_native_oid_before_sql_rendering(table_oid):
    from process.entity_address_unified import _required_geo_taxonomy_stage_index_sql

    with pytest.raises(ValueError, match="identity is invalid"):
        _required_geo_taxonomy_stage_index_sql("base_example", "entity_address_unified", table_oid=table_oid)


@pytest.mark.asyncio
async def test_nonbase_address_profile_refuses_other_relation_before_sql():
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock())
    relation = CompactAddressRelation("candidate_example", "npi")
    with pytest.raises(indexes.NetworkServingIndexesError, match="Search address relation differs"):
        await indexes._require_address_search_indexes(connection, relation)
    connection.execute.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("custom", [False, True])
async def test_every_effective_address_heap_and_membership_digest_are_checked(monkeypatch, custom):
    from process import network_compact_readiness

    base = CompactAddressRelation("base_example", "entity_address_unified")
    extra = CompactAddressRelation("candidate_example", "entity_address_unified") if custom else None
    layout = CompactNetworkLayout(CompactAddressRelation("candidate_example", "network_slice_ref"), base, (), extra)
    profile = AsyncMock(return_value="b" * 64)
    digest = AsyncMock(return_value="a" * 64)
    monkeypatch.setattr(indexes, "_require_address_search_indexes", profile)
    monkeypatch.setattr(network_compact_readiness, "compact_index_digest", digest)
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    candidate = _candidate()
    await indexes.verify_network_search_indexes(connection, candidate, compact_layout=layout)
    assert profile.await_args_list == [call(connection, base, compact_base=True)] + (
        [call(connection, extra)] if custom else []
    )
    digest.assert_awaited_once_with(connection, "candidate_example", candidate=candidate)
    digest.return_value = "c" * 64
    with pytest.raises(indexes.NetworkServingIndexesError, match="definitions differ"):
        await indexes.verify_network_search_indexes(connection, candidate, compact_layout=layout)


@pytest.mark.asyncio
async def test_noncompact_profile_uses_exact_closure_oid_and_requires_retained_digest(monkeypatch):
    profile = AsyncMock(return_value="a" * 64)
    monkeypatch.setattr(indexes, "_require_address_search_indexes", profile)
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    candidate = _candidate(False)
    await indexes.verify_network_search_indexes(connection, candidate)
    profile.assert_awaited_once_with(
        connection, CompactAddressRelation("candidate_example", "entity_address_unified"), canonical=True
    )
    profile.return_value = "c" * 64
    with pytest.raises(indexes.NetworkServingIndexesError, match="definitions differ"):
        await indexes.verify_network_search_indexes(connection, candidate)


@pytest.mark.asyncio
async def test_compact_base_checks_exact_rotated_heap_without_canonical_array_authority(monkeypatch):
    relation = CompactAddressRelation("base_example", "entity_address_unified_retained_2c")
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(side_effect=[44, True]))
    catalog = AsyncMock()
    digest = AsyncMock(side_effect=AssertionError("Compact edges own membership"))
    monkeypatch.setattr(indexes, "_catalog_readiness", catalog)
    monkeypatch.setattr(indexes, "_index_digest", digest)
    await indexes._require_address_search_indexes(connection, relation, compact_base=True)
    catalog.assert_awaited_once_with(
        connection, "base_example", table_name="entity_address_unified_retained_2c", canonical=False
    )
    digest.assert_not_awaited()
    sql = connection.fetchval.await_args_list[-1].args[0]
    assert "stage_table.oid = 44" in sql and "stage_table.relname = 'entity_address_unified_retained_2c'" in sql
    assert "required_index.relname =" not in sql and "geo_index.relname =" not in sql
    assert all(flag in sql for flag in ("indisvalid", "indisready", "indislive", "gist_geography_ops"))


@pytest.mark.asyncio
async def test_unbound_layout_or_missing_transaction_cannot_authorize_search(monkeypatch):
    profile = AsyncMock()
    monkeypatch.setattr(indexes, "_require_address_search_indexes", profile)
    with pytest.raises(indexes.NetworkServingIndexesError, match="layout is required"):
        await indexes.verify_network_search_indexes(SimpleNamespace(is_in_transaction=lambda: True), _candidate())
    with pytest.raises(indexes.NetworkServingIndexesError, match="transaction"):
        await indexes.verify_network_search_indexes(SimpleNamespace(is_in_transaction=lambda: False), _candidate())
    profile.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["oid", "geo", "profile"])
async def test_address_profile_refuses_changed_identity_or_missing_native_paths(monkeypatch, damage):
    relation = CompactAddressRelation("candidate_example", "entity_address_unified")
    connection = SimpleNamespace(
        execute=AsyncMock(), fetchval=AsyncMock(side_effect=[45 if damage == "oid" else 44, False])
    )
    digest = AsyncMock(return_value="a" * 64)
    if damage == "profile":
        digest.side_effect = indexes.NetworkServingIndexesError("native profile missing")
    monkeypatch.setattr(indexes, "_index_digest", digest)
    with pytest.raises(indexes.NetworkServingIndexesError):
        await indexes._require_address_search_indexes(connection, relation)
    assert all(not args.args[0].startswith(("CREATE", "ALTER", "DROP")) for args in connection.execute.await_args_list)


@pytest.mark.asyncio
async def test_initial_publication_refuses_before_manifest_sequence(monkeypatch):
    from process import network_membership_publication as publication
    from process import registry_fhir_serving_preservation

    candidate_by_field = {
        "state": "ready",
        "expected_head": 0,
        "approved_custom_revision": 0,
        "source_generations": {},
        "validation_json": {},
    }
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    monkeypatch.setattr(publication, "_lock_publication_controls", AsyncMock(return_value=(0, 0)))
    monkeypatch.setattr(publication, "_locked_candidate", AsyncMock(return_value=candidate_by_field))
    monkeypatch.setattr(publication, "_publication_readiness", Mock(return_value=({}, {}, {}, {})))
    monkeypatch.setattr(publication, "_require_publication_custody", AsyncMock(return_value=None))
    monkeypatch.setattr(publication, "_verify_provider_custody", AsyncMock())
    monkeypatch.setattr(
        registry_fhir_serving_preservation, "fhir_preservation_obligation", AsyncMock(return_value=None)
    )
    gate = AsyncMock(side_effect=indexes.NetworkServingIndexesError("missing search profile"))
    monkeypatch.setattr(indexes, "verify_network_search_indexes", gate)
    insert = AsyncMock()
    monkeypatch.setattr(publication, "_insert_manifest", insert)
    with pytest.raises(indexes.NetworkServingIndexesError, match="missing search"):
        await publication._publish_candidate(
            connection, SimpleNamespace(candidate_id="00000000-0000-0000-0000-000000000001"), '"control_example"'
        )
    gate.assert_awaited_once_with(connection, candidate_by_field, compact_layout=None)
    insert.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["target", "source", "same_schema"])
async def test_preparation_refuses_untrusted_or_self_sourced_inputs_before_sql(invalid):
    from process.network_address_projection import PinnedAddressSource
    from process.network_membership_copy import MembershipCopyTarget

    identity = "00000000-0000-0000-0000-000000000001"
    target = MembershipCopyTarget(
        identity, identity, identity, identity, "network_candidate_" + identity.replace("-", "")
    )
    source = PinnedAddressSource(
        target.schema_name if invalid == "same_schema" else "source_example", "entity_address_unified", "generation-one"
    )
    connection = SimpleNamespace(is_in_transaction=lambda: True, execute=AsyncMock(), transaction=Mock())
    with pytest.raises(indexes.NetworkServingIndexesError, match="Trusted candidate|outside the candidate"):
        await indexes.prepare_network_serving_indexes(
            connection, object() if invalid == "target" else target, object() if invalid == "source" else source
        )
    connection.execute.assert_not_awaited()
    connection.transaction.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["open", "published"])
async def test_unready_or_unreceipted_candidates_cannot_build_indexes(monkeypatch, state):
    connection = SimpleNamespace(execute=AsyncMock())
    target = SimpleNamespace(schema_name="candidate_example")
    source = SimpleNamespace(schema_name="source_example", table_name="entity_address_unified")
    monkeypatch.setattr(indexes, "_locked_candidate", AsyncMock(return_value={"state": state}))
    monkeypatch.setattr(indexes, "_matching_inputs", AsyncMock(return_value=({}, {})))
    semantics, build = AsyncMock(), AsyncMock()
    monkeypatch.setattr(indexes, "_source_semantics", semantics)
    monkeypatch.setattr(indexes, "_build_serving_indexes", build)
    with pytest.raises(indexes.NetworkServingIndexesError, match="must be ready|cannot acquire"):
        await indexes._prepare_serving(connection, target, source, "control_example")
    semantics.assert_not_awaited()
    build.assert_not_awaited()
    assert all(call.args[0].startswith("LOCK TABLE ") for call in connection.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["source", "canonical_column"])
async def test_copied_schema_requires_complete_pinned_source_columns(missing):
    columns = (
        []
        if missing == "source"
        else [{"attname": "entity_id", "is_source": True}, {"attname": "entity_id", "is_source": False}]
    )
    connection = SimpleNamespace(fetch=AsyncMock(return_value=columns), execute=AsyncMock())
    with pytest.raises(indexes.NetworkServingIndexesError, match="complete pinned source"):
        await indexes._column_metadata(connection, '"source_example".addresses', '"candidate_example".addresses')
    connection.execute.assert_not_awaited()
