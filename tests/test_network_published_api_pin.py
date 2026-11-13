# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published-request reuse stays local to its original connection and scope."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_published_api_pin as published
from process.registry_network_catalog_read import _PINNED_READ


def test_supplied_manifest_flag_cannot_skip_request_audits():
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    manifest = SimpleNamespace(_published_api_request=True)
    assert published.active_published_api_pin(connection, manifest) is None


@pytest.mark.parametrize("damage", [None, "connection", "manifest", "schema", "transaction", "replaced", "unpublished"])
def test_reuse_requires_exact_connection_manifest_and_active_scope(damage):
    connection = SimpleNamespace(is_in_transaction=lambda: damage != "transaction")
    transaction = object()
    session = SimpleNamespace(get_transaction=lambda: object() if damage == "replaced" else transaction)
    manifest = SimpleNamespace(
        _published_api_request=damage != "unpublished",
        _api_request_session=session,
        _api_request_transaction=transaction,
    )
    token = _PINNED_READ.set((connection, manifest, "example_control"))
    try:
        actual = object() if damage == "connection" else connection
        requested = object() if damage == "manifest" else manifest
        schema = "other_control" if damage == "schema" else "example_control"
        if damage in {"manifest", "schema", "transaction", "replaced"}:
            with pytest.raises(published.NetworkServingReadUnavailable):
                published.active_published_api_pin(actual, requested, control_schema=schema)
        else:
            resolved = published.active_published_api_pin(actual, requested, control_schema=schema)
            assert resolved is (manifest if damage is None else None)
    finally:
        _PINNED_READ.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("generation", [True, False, 0, -1, 1.0, "1", 2**63])
async def test_invalid_generation_never_runs_metadata_sql(generation):
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(published.NetworkServingReadUnavailable):
        await published.read_published_api_pin(connection, generation, "example_control")
    connection.fetchrow.assert_not_awaited()


def _published_entry(descriptor, *, base_rls=False):
    candidate_json = json.dumps({"validation_json": {"serving_readiness": {"address_artifact": descriptor}}})
    return {"candidate_json": candidate_json, "base_rls": base_rls}


@pytest.mark.asyncio
async def test_missing_published_generation_is_unavailable():
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    with pytest.raises(published.NetworkServingReadUnavailable, match="generation is unavailable"):
        await published.read_published_api_pin(connection, 3, "example_control")
    statement, generation = connection.fetchrow.await_args.args
    assert '"example_control".network_serving_manifest' in statement and generation == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("descriptor", [None, {"contract": "other.address.artifact"}])
async def test_generation_without_an_indexed_address_artifact_has_no_published_pin(descriptor):
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=_published_entry(descriptor)))
    assert await published.read_published_api_pin(connection, None, "example_control") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("base_rls", [True, None], ids=["row security", "no heap under either name"])
async def test_unreadable_published_address_heap_is_unavailable(base_rls):
    entry = _published_entry({"contract": published.INDEXED_CONTRACT}, base_rls=base_rls)
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=entry))
    with pytest.raises(published.NetworkServingReadUnavailable, match="address rows are unavailable"):
        await published.read_published_api_pin(connection, None, "example_control")


@pytest.mark.asyncio
async def test_manifest_only_read_touches_no_approved_records(monkeypatch):
    """A reader without access to approved records still resolves the published manifest."""
    from process import network_compact_layout as layouts

    report_by_field = {
        "serving_readiness": {
            "address_artifact": {"contract": published.INDEXED_CONTRACT, "table": {"oid": 11}},
            "edge_artifacts": [],
            "index_definition_sha256": "d" * 64,
        },
        "candidate_readiness": {"ref_map_oid": 12},
        "writer_closure": {},
    }
    entry_by_field = {
        "candidate_json": json.dumps(
            {"schema_name": "network_candidate_" + "a" * 32, "validation_json": report_by_field}
        ),
        "base_rls": False,
        "base_schema": "example_addresses",
        "base_name": "entity_address_unified",
        "candidate_table_oid": 41,
    }
    manifest = SimpleNamespace(approved_custom_revision=4, source_generations={"custom_membership": "c" * 64})
    evidence = []
    monkeypatch.setattr(published, "_pinned_manifest", lambda _entry, custody: evidence.append(custody) or manifest)
    monkeypatch.setattr(layouts, "bind_compact_geo_search_candidate", lambda layout, _candidate: layout)
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=entry_by_field))
    resolved = await published.read_published_api_pin(connection, 3, "example_control", approved=False)
    assert resolved == (manifest, None, None) and not hasattr(manifest, "_published_api_request")
    approved_records = 'FROM "example_control".registry_approved_record'
    statement = connection.fetchrow.await_args.args[0]
    assert "NULL::bigint AS approved_rows" in statement and approved_records not in statement
    # The candidate's own validation report supplies the relation names; the ledger is never read.
    assert "network_candidate_custody" not in statement and evidence[0]["seal_sha256"] is None
    assert evidence[0]["receipt"] == {
        "closure": {},
        "control": {"candidate_table_oid": 41},
        "index_definition_sha256": "d" * 64,
    }

    entry_by_field.update(approved_oid=31, approved_rows=5)
    pinned, approved, relation = await published.read_published_api_pin(connection, 3, "example_control")
    assert approved_records in connection.fetchrow.await_args.args[0]
    assert pinned is manifest and manifest._published_api_request is True
    assert (approved.approved_revision, approved.generation_id, approved.total_rows) == (4, "c" * 64, 5)
    assert (relation.schema_name, relation.table_name, relation.table_oid) == (
        "example_control",
        "registry_approved_record",
        31,
    )


def _custom_candidate():
    closure_by_field = {
        "relation_oids": {"provider_location_binding": 21, "entity_address_unified": 22},
        "loader_roles": [],
        "reader_roles": ["example_reader"],
    }
    source_by_field = {
        "schema_name": "example_addresses",
        "table_name": "entity_address_unified",
        "generation_id": "a" * 64,
    }
    return {
        "schema_name": "network_candidate_" + "a" * 32,
        "validation_json": {"address_source": source_by_field, "writer_closure": closure_by_field},
    }


def _custom_receipt(**changes):
    values_by_field = {
        "base_source": published.PinnedAddressSource("example_addresses", "entity_address_unified", "a" * 64),
        "base_artifact_sha256": "b" * 64,
        "binding_table_oid": 21,
        "runtime_roles": ("example_reader",),
        "address_table_oid": 22,
        "custom_pair_count": 1,
        "materialized_address_rows": 0,
    }
    return SimpleNamespace(**(values_by_field | changes))


@pytest.mark.parametrize(
    ("custom", "carrier", "expected_oid"),
    [
        (_custom_receipt(), None, 22),
        (_custom_receipt(custom_pair_count=0), None, None),
        (_custom_receipt(custom_pair_count=0, address_table_oid=23), _custom_receipt(materialized_address_rows=3), 22),
        (_custom_receipt(custom_pair_count=0), _custom_receipt(), None),
    ],
    ids=["custom offices", "no custom offices", "carrier offices", "empty carrier"],
)
def test_custom_address_relation_follows_the_recorded_preparation(monkeypatch, custom, carrier, expected_oid):
    monkeypatch.setattr(published, "compact_custom_receipt", lambda _candidate: custom)
    monkeypatch.setattr(published, "compact_carrier_receipt", lambda _candidate: carrier)
    candidate = _custom_candidate()
    relation = published._custom_address_relation(candidate, {"descriptor_sha256": "b" * 64})
    if expected_oid is None:
        assert relation is None
    else:
        assert (relation.schema_name, relation.table_name, relation.table_oid) == (
            candidate["schema_name"],
            "entity_address_unified",
            expected_oid,
        )


@pytest.mark.parametrize(
    ("custom", "carrier"),
    [
        (_custom_receipt(binding_table_oid=99), None),
        (_custom_receipt(base_artifact_sha256="f" * 64), None),
        (_custom_receipt(address_table_oid=99), None),
        (_custom_receipt(), _custom_receipt(runtime_roles=("other_reader",))),
        (_custom_receipt(), _custom_receipt(address_table_oid=99)),
    ],
    ids=["binding", "artifact", "address", "carrier roles", "carrier address"],
)
def test_custom_address_relation_refuses_inconsistent_preparation(monkeypatch, custom, carrier):
    monkeypatch.setattr(published, "compact_custom_receipt", lambda _candidate: custom)
    monkeypatch.setattr(published, "compact_carrier_receipt", lambda _candidate: carrier)
    with pytest.raises(published.NetworkServingReadUnavailable, match="metadata is unavailable"):
        published._custom_address_relation(_custom_candidate(), {"descriptor_sha256": "b" * 64})
