# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Physical identity, source authority and request validation remain fail closed."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import BadRequest
from sanic.request import RequestParameters

from api import control_imports
from api import control_ptg_membership as control
from api import ptg2_serving as serving
from api import ptg_membership_reader as reader
from api import ptg_membership_source as source
from api.ptg_membership_contract import IMPORTER, physical_identity
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_ptg2_physical_binding import _binding
from tests.test_ptg_membership_projection import PARAMS, _request


def _tables(**changes):
    return SimpleNamespace(
        **(
            dict(
                source_key=PARAMS["source_key"],
                snapshot_id=PARAMS["snapshot_id"],
                uses_v4_graph=True,
                physical_binding=_binding(),
                shared_snapshot_key=19,
                schema_name="synthetic",
            )
            | changes
        )
    )


def test_physical_identity_serializes_complete_binding_without_mutating_it():
    binding = _binding()
    document = physical_identity(_tables(physical_binding=binding))
    assert json.loads(json.dumps(document)) == document
    assert document["dataset_id"] == str(binding.dataset_id)
    assert document["relation_oids"] == [list(pair) for pair in binding.relation_oids]
    assert document["sequence_oids"] == [list(entry) for entry in binding.sequence_oids]
    assert document["payload_snapshot_key"] == binding.payload_snapshot_key
    assert document["destination_layout_key"] == binding.destination_layout_key
    document["relation_oids"][0][1] += 1
    assert binding.relation_oids == _binding().relation_oids


@pytest.mark.parametrize(
    "changes", [{"source_key": "other-network"}, {"snapshot_id": "other-snapshot"}, {"uses_v4_graph": False}]
)
def test_source_selectors_and_graph_contract_must_match(monkeypatch, changes):
    strict = Mock()
    monkeypatch.setattr(serving, "_require_strict_shared_v3", strict)
    with pytest.raises(ValueError, match="audited V4 serving source"):
        source._require_source_custody(_tables(**changes), None, PARAMS["source_key"], PARAMS["snapshot_id"])
    strict.assert_called_once()


@pytest.mark.parametrize("kind", ["physical", "key", "schema"])
def test_supplied_descriptor_cannot_substitute_physical_custody(monkeypatch, kind):
    actual, expected = _tables(), _tables()
    if kind == "physical":
        expected.physical_binding = replace(_binding(), owner_oid=999)
    elif kind == "key":
        expected.shared_snapshot_key = 20
    else:
        expected.schema_name = "other"
    monkeypatch.setattr(serving, "_require_strict_shared_v3", lambda _tables: None)
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda tables: tables.shared_snapshot_key)
    monkeypatch.setattr(serving, "_payload_schema", lambda tables: tables.schema_name)
    with pytest.raises(PTG2ManifestArtifactError, match="physical custody changed"):
        source._require_source_custody(actual, expected, PARAMS["source_key"], PARAMS["snapshot_id"])


@pytest.mark.asyncio
async def test_packed_finalizer_binds_all_three_root_digests(monkeypatch):
    packed = AsyncMock(return_value=True)
    monkeypatch.setattr(source, "has_complete_v4_finalizer_map", packed)
    digests_by_name = dict(
        map_digest=b"a" * 32, canonical_mapping_digest=memoryview(b"b" * 32), target_identity_digest=b"c" * 32
    )
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: digests_by_name))
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    assert await source._finalizer_identity(session, "synthetic", 19) == dict(
        state="packed", **{name: bytes(value).hex() for name, value in digests_by_name.items()}
    )
    packed.assert_awaited_once_with(session, schema_name="synthetic", snapshot_key=19)
    assert session.execute.call_args.args[1] == {"key": 19}
    assert '"synthetic".ptg2_v4_finalizer_map_root' in str(session.execute.call_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows", [[], [dict(database_oid=1, schema_oid=2, relname="ptg2_v4_snapshot_map_root", relation_oid=3)]]
)
async def test_incomplete_source_inventory_never_uses_an_audit_attestation(rows):
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))
    session = SimpleNamespace(execute=AsyncMock(return_value=result), scalar=AsyncMock())
    with pytest.raises(PTG2ManifestArtifactError, match="relation inventory is incomplete"):
        await source._source_storage(session, "synthetic", PARAMS["snapshot_id"])
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_checked_attachment_is_reused_within_one_request_only(monkeypatch):
    resolve, locate = AsyncMock(return_value=(None, None, PARAMS)), AsyncMock(return_value=object())
    monkeypatch.setattr(reader, "source_identity", resolve)
    monkeypatch.setattr(reader, "locate", locate)
    session, tables, request = object(), _tables(), _request()
    state = serving._LocalDistanceGraphState()
    first = await reader._attached_index(session, tables, request, state)
    assert await reader._attached_index(session, tables, request, state) is first
    resolve.assert_awaited_once_with(
        session, PARAMS["source_key"], PARAMS["snapshot_id"], "CPT", "99213", tables=tables
    )
    locate.assert_awaited_once_with(session, PARAMS)
    await reader._attached_index(session, tables, request, serving._LocalDistanceGraphState())
    assert resolve.await_count == locate.await_count == 2


def test_out_of_scope_npi_never_enters_retained_membership_state():
    state = serving._LocalDistanceGraphState()
    projection_by_field = dict(first_count=1, priced_count=1, pairs=[[1000000002, 31]])
    with pytest.raises(PTG2ManifestArtifactError, match="out-of-scope NPI"):
        reader._admit_memberships(projection_by_field, (1000000001,), _request(), state, (10, 10, 1))
    assert state.retained_memberships == state.indexed_projection_members == 0
    assert state.code_sets == set()


def _readiness_handler():
    routes_by_path = {}

    class Blueprint:
        def get(self, path):
            def register(handler):
                routes_by_path[path] = handler
                return handler

            return register

    control.register_membership_projection_routes(Blueprint())
    return routes_by_path["/ptg/provider-membership-projection"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["duplicate", "alias", "extra"])
async def test_readiness_rejects_ambiguous_or_noncanonical_params_after_auth_before_database(monkeypatch, kind):
    params_by_name = {name: [value] for name, value in PARAMS.items()}
    if kind == "duplicate":
        params_by_name["code"].append("99214")
    elif kind == "alias":
        params_by_name["code_system"] = ["cpt"]
    else:
        params_by_name["schema"] = ["other"]
    auth, transaction, resolve = Mock(), Mock(), AsyncMock()
    monkeypatch.setattr(control, "require_control_auth", auth)
    monkeypatch.setattr(control.db, "transaction", transaction)
    monkeypatch.setattr(control, "source_identity", resolve)
    request = SimpleNamespace(args=RequestParameters(params_by_name))
    with pytest.raises(BadRequest):
        await _readiness_handler()(request)
    auth.assert_called_once_with(request)
    transaction.assert_not_called()
    resolve.assert_not_awaited()


@pytest.mark.asyncio
async def test_unauthenticated_readiness_never_checks_custody(monkeypatch):
    auth = Mock(side_effect=PermissionError("unauthorized"))
    transaction = Mock()
    monkeypatch.setattr(control, "require_control_auth", auth)
    monkeypatch.setattr(control.db, "transaction", transaction)
    with pytest.raises(PermissionError, match="unauthorized"):
        await _readiness_handler()(SimpleNamespace(args=RequestParameters({})))
    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_import_admission_validates_closed_membership_params_before_writes(monkeypatch):
    admit, enqueue = AsyncMock(), AsyncMock()
    monkeypatch.setattr(control_imports, "_admit_import_row", admit)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    with pytest.raises(ValueError, match="exactly source_key"):
        await control_imports.create_import_run(dict(importer=IMPORTER, params=PARAMS | {"schema": "other"}))
    admit.assert_not_awaited()
    enqueue.assert_not_awaited()
