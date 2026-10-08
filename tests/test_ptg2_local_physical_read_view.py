# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read views are qualified native interfaces, never caller-selected authority."""

import hashlib
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import result_archive_candidate_validation as validation
from tests.test_ptg2_local_preparation_authority import _ownership, _protected_preparation_fixture


def _view_fixture():
    """Preserve the original protected digest independently of the projected evidence."""
    physical_binding, preparation_by_field = _protected_preparation_fixture()
    ownership = _ownership(physical_binding)
    relations = [
        {"ordinal": ordinal, "table_name": name, "relation_oid": oid}
        for ordinal, (name, oid) in enumerate(ownership.relation_oids)
    ]
    sequences = [
        {"ordinal": ordinal, "sequence_name": name, "sequence_oid": oid, "owner_table": table, "owner_column": column}
        for ordinal, (name, oid, table, column) in enumerate(ownership.sequence_oids)
    ]
    inventory_by_field = {
        "schema_name": ownership.schema_name,
        "schema_oid": ownership.schema_oid,
        "relations": [
            {key: field_value for key, field_value in relation.items() if key != "ordinal"} for relation in relations
        ],
        "sequences": [
            {key: field_value for key, field_value in sequence.items() if key != "ordinal"} for sequence in sequences
        ],
    }
    evidence = deepcopy(preparation_by_field["validation"]["evidence"])
    evidence["native_audit"]["identity"] = {}
    authority_by_field = dict.fromkeys(native._local_read_view_columns(True), "a" * 64)
    authority_by_field.update(
        contract="ptg.prepared-physical-binding-read.v1",
        destination_snapshot_id=physical_binding.snapshot_id,
        owner_oid=physical_binding.owner_oid,
        native_validation=evidence,
        relation_inventory=relations,
        sequence_inventory=sequences,
        inventory_sha256=native._native_metadata_digest(inventory_by_field),
    )
    return physical_binding, authority_by_field


@pytest.mark.asyncio
async def test_unqualified_view_refuses_before_catalog_or_lock(monkeypatch):
    monkeypatch.setattr(native, "PREPARED_LOCAL_READ_VIEW_SHA256", None)
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=True))
    with pytest.raises(native.PTG2PhysicalBindingError, match="not qualified"):
        await native.require_local_physical_read_view(session, is_prepared=True)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "body", "acl", "oid", "columns"])
async def test_fixed_read_interface_rechecks_catalog_after_access_share(monkeypatch, drift):
    """A valid first observation never admits a swapped view or changed privileges."""
    definition = "SELECT 1;"
    monkeypatch.setattr(native, "PREPARED_LOCAL_READ_VIEW_SHA256", hashlib.sha256(definition.encode()).hexdigest())
    monkeypatch.setattr(native, "local_preparation_catalog_owner", AsyncMock(return_value=73))
    first_by_field = {
        "oid": 88,
        "definition": definition,
        "read_only": True,
        "columns": native._local_read_view_columns(True),
    }
    second_by_field = deepcopy(first_by_field)
    mutations_by_name = {
        "body": ("definition", "SELECT 2;"),
        "acl": ("read_only", False),
        "oid": ("oid", 89),
        "columns": ("columns", {}),
    }
    if drift:
        key, field_value = mutations_by_name[drift]
        second_by_field[key] = field_value
    proof = AsyncMock(side_effect=[first_by_field, second_by_field])
    monkeypatch.setattr(native, "_local_read_view_proof", proof)
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=True))
    if drift:
        with pytest.raises(native.PTG2PhysicalBindingError):
            await native.require_local_physical_read_view(session, is_prepared=True)
    else:
        assert await native.require_local_physical_read_view(session, is_prepared=True) == 73
    assert "ACCESS SHARE MODE NOWAIT" in str(session.execute.await_args.args[0])
    catalog_sql = str(proof.await_args.args[1])
    assert "security_invoker=false" in catalog_sql and "a.privilege_type<>'SELECT'" in catalog_sql
    assert "NOT pg_has_role(current_user,CAST(:owner_oid AS oid),'MEMBER')" in str(session.scalar.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("does_refuse", [False, True])
async def test_deparser_path_is_transaction_local_and_restored_on_refusal(does_refuse):
    """The fixed deparser environment must not escape into the caller's following work."""
    events = []

    async def execute(query, parameters_by_name=None):
        sql = str(query)
        events.append((sql, parameters_by_name))
        if "current_setting" in sql:
            return SimpleNamespace(scalar_one=lambda: "public, pg_catalog")
        if sql == "SELECT native proof":
            if does_refuse:
                raise ValueError("proof refused")
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: {"oid": 88}))
        return None

    session = SimpleNamespace(execute=execute)
    if does_refuse:
        with pytest.raises(ValueError, match="proof refused"):
            await native._local_read_view_proof(session, "SELECT native proof", {})
    else:
        assert await native._local_read_view_proof(session, "SELECT native proof", {}) == {"oid": 88}
    assert "'pg_catalog, pg_temp',true" in events[1][0]
    assert events[-1][1] == {"original_path": "public, pg_catalog"}
    assert ":original_path,true" in events[-1][0]


@pytest.mark.parametrize("drift", [None, "contract", "owner", "children", "model", "digest"])
def test_projected_read_authority_requires_complete_native_inventory(drift):
    """The original validation SHA is not falsely recomputed from a privacy-safe projection."""
    physical_binding, authority_by_field = _view_fixture()
    mutations_by_name = {
        "contract": lambda: authority_by_field.update(contract="ptg.installed-physical-binding-read.v1"),
        "owner": lambda: authority_by_field.update(owner_oid=physical_binding.owner_oid + 1),
        "children": lambda: authority_by_field["relation_inventory"].pop(),
        "model": lambda: authority_by_field["native_validation"]["data"].update(model_sha256="0" * 64),
        "digest": lambda: authority_by_field.update(inventory_sha256="0" * 64),
    }
    if drift:
        mutations_by_name[drift]()
        with pytest.raises(native.PTG2PhysicalBindingError):
            validation._local_read_authority_binding(
                authority_by_field, physical_binding.snapshot_id, physical_binding.owner_oid, is_prepared=True
            )
    else:
        assert authority_by_field["validation_sha256"] != native._native_metadata_digest(
            authority_by_field["native_validation"]
        )
        assert (
            validation._local_read_authority_binding(
                authority_by_field, physical_binding.snapshot_id, physical_binding.owner_oid, is_prepared=True
            )[1]
            == physical_binding
        )


@pytest.mark.parametrize(
    "field,digest,does_refuse",
    [
        ("artifact_sha256", None, False),
        ("artifact_sha256", "a" * 64, False),
        ("artifact_sha256", "", True),
        ("artifact_sha256", 42, True),
        ("manifest_sha256", None, True),
        ("validation_sha256", None, True),
        ("inventory_sha256", None, True),
    ],
)
def test_projected_digest_is_optional_only_for_transport_artifact(field, digest, does_refuse):
    """A native archive needs no second transport artifact; other binding digests remain mandatory."""
    physical_binding, authority_by_field = _view_fixture()
    authority_by_field[field] = digest
    if does_refuse:
        with pytest.raises(native.PTG2PhysicalBindingError):
            validation._local_read_authority_binding(
                authority_by_field,
                physical_binding.snapshot_id,
                physical_binding.owner_oid,
                is_prepared=True,
            )
    else:
        assert (
            validation._local_read_authority_binding(
                authority_by_field,
                physical_binding.snapshot_id,
                physical_binding.owner_oid,
                is_prepared=True,
            )[1]
            == physical_binding
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("does_refuse", [False, True])
async def test_called_read_state_fences_all_heaps_before_control_reads(monkeypatch, does_refuse):
    """Serving evidence remains unread until every exact immutable heap is locked and rechecked."""
    physical_binding, authority_by_field = _view_fixture()
    events = []
    monkeypatch.setattr(native, "PREPARED_LOCAL_READ_VIEW_SHA256", "a" * 64)
    monkeypatch.setattr(native, "require_local_physical_read_view", AsyncMock(return_value=physical_binding.owner_oid))
    monkeypatch.setattr(native, "local_preparation_catalog_owner", AsyncMock(return_value=physical_binding.owner_oid))

    async def execute(query, parameters_by_name=None):
        sql = str(query)
        events.append(sql)
        rows = [authority_by_field] if "physical_binding" in sql and sql.startswith("SELECT") else []
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    async def verify(session, ownership):
        assert len([event for event in events if event.startswith("LOCK TABLE")]) == 42
        events.append("catalog")

    async def control(session, schema_name, binding, *, lock_controls):
        assert events[-1] == "catalog" and lock_controls is False
        events.append("control")
        return {"snapshot_id": binding.snapshot_id}

    monkeypatch.setattr(native, "verify_local_data_family", verify)
    monkeypatch.setattr(native, "_require_closed_local_custody", AsyncMock())
    monkeypatch.setattr(native, "local_data_catalog_digest", AsyncMock(return_value="c" * 64))
    monkeypatch.setattr(native, "_local_candidate_control", control)

    def postimage(candidate, *args):
        """Only a successfully rechecked postimage may contribute a cache namespace."""
        if does_refuse:
            raise native.PTG2PhysicalBindingError("postimage drift")
        return candidate

    monkeypatch.setattr(native, "_require_local_control_postimage", postimage)
    session = SimpleNamespace(
        execute=execute, scalar=AsyncMock(return_value=False), in_transaction=lambda: True, info={}
    )
    if does_refuse:
        with pytest.raises(native.PTG2PhysicalBindingError, match="postimage drift"):
            await validation.local_data_physical_read_state(session, physical_binding.snapshot_id, is_prepared=True)
        assert session.info == {}
    else:
        state = await validation.local_data_physical_read_state(session, physical_binding.snapshot_id, is_prepared=True)
        assert state[2] == physical_binding and "control" in events
        assert session.info["ptg2_local_read_bindings"] == {physical_binding.schema_name: physical_binding}
        assert session.info["ptg2_local_read_catalog_sha256"] == {physical_binding.schema_name: "c" * 64}
    assert not any("FOR SHARE" in event or "FOR UPDATE" in event for event in events)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_publisher", [False, True])
async def test_publisher_view_requires_actual_verified_owner_inheritance(monkeypatch, is_publisher):
    """Caller input cannot promote a reader into the separate native publisher route."""
    monkeypatch.setattr(native, "PREPARED_LOCAL_READ_VIEW_SHA256", "a" * 64)
    monkeypatch.setattr(native, "_local_preparation_owner", AsyncMock(return_value=73))
    attester = AsyncMock(return_value=73)
    monkeypatch.setattr(native, "_require_local_physical_view_catalog", attester)
    session = SimpleNamespace(scalar=AsyncMock(return_value=is_publisher))
    if is_publisher:
        assert await native.require_local_physical_publisher_view(session, is_prepared=True) == 73
        attester.assert_awaited_once_with(session, is_prepared=True, owner_oid=73)
    else:
        with pytest.raises(native.PTG2PhysicalBindingError, match="publisher privileges"):
            await native.require_local_physical_publisher_view(session, is_prepared=True)
        attester.assert_not_awaited()
    assert "CAST(:owner_oid AS oid),'USAGE'" in str(session.scalar.await_args.args[0])


@pytest.mark.asyncio
async def test_installed_api_reader_uses_actual_payload_schema_and_preserved_key(monkeypatch):
    """The called API branch reuses strict manifest parsing after genuine installed resolution."""
    from api import ptg2_tables
    from tests.ptg2_manifest_tables_support import (
        FakeResult,
        strict_snapshot_row,
        strict_source_identity_rows,
        strict_v4_root_row,
        strict_v4_serving_index,
    )

    physical_binding, _authority = _view_fixture()
    serving_index = strict_v4_serving_index(physical_binding.payload_snapshot_key)
    snapshot_by_field = strict_snapshot_row(serving_index, has_local_physical_binding=True)
    candidate_by_field = {
        "manifest": {"serving_index": serving_index},
        "layout_manifest": {"serving_index": serving_index},
        "plan_id": snapshot_by_field["snapshot_plan_id"],
        "plan_market_type": snapshot_by_field["snapshot_plan_market_type"],
        "coverage_scope_id": bytes.fromhex(snapshot_by_field["snapshot_coverage_scope_id"]),
        **{key: field_value for key, field_value in snapshot_by_field.items() if key.startswith("attested_")},
    }
    resolver = AsyncMock(return_value=({}, {}, physical_binding, candidate_by_field))
    monkeypatch.setattr(validation, "local_data_physical_read_state", resolver)
    finalizer = AsyncMock()
    monkeypatch.setattr(ptg2_tables, "_validate_v4_finalizer_map_root", finalizer)
    calls = []

    async def execute(query, parameters_by_name):
        sql = str(query)
        calls.append((sql, parameters_by_name))
        if "LIMIT 257" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: strict_source_identity_rows()))
        return FakeResult(strict_v4_root_row(serving_index) if "root.representation" in sql else snapshot_by_field)

    tables = await ptg2_tables.snapshot_serving_tables(SimpleNamespace(execute=execute), physical_binding.snapshot_id)
    assert (
        tables.physical_binding == physical_binding
        and tables.shared_snapshot_key == physical_binding.payload_snapshot_key
    )
    resolver.assert_awaited_once_with(SimpleNamespace(execute=execute), physical_binding.snapshot_id, is_prepared=False)
    source_query = next((sql, parameters) for sql, parameters in calls if "LIMIT 257" in sql)
    assert physical_binding.relation("ptg2_v3_snapshot_source") in source_query[0]
    assert source_query[1]["snapshot_id"] == physical_binding.payload_snapshot_id
    assert f"{physical_binding.schema_name}.ptg2_v4_snapshot_map_root" in calls[-1][0]
    assert finalizer.await_args.kwargs["physical_binding"] == physical_binding


@pytest.mark.asyncio
@pytest.mark.parametrize("does_refuse", [False, True])
@pytest.mark.parametrize(
    "reader_name",
    ["fetch_snapshot_source_provenance", "fetch_snapshot_source_set_identity", "fetch_snapshot_source_set_metadata"],
)
async def test_local_provenance_reauthenticates_before_reading_selected_payload(monkeypatch, does_refuse, reader_name):
    """Source readers must authenticate the family before using its payload ID."""
    from api import ptg2_shared_blocks, ptg2_tables
    from tests.test_ptg2_shared_blocks_guards import _provenance_row

    physical_binding, _authority = _view_fixture()
    descriptor = SimpleNamespace(physical_binding=physical_binding)
    read = AsyncMock(
        side_effect=native.PTG2PhysicalBindingError("binding drift") if does_refuse else None, return_value=descriptor
    )
    monkeypatch.setattr(ptg2_tables, "read_serving_tables", read)
    execute = AsyncMock(return_value=[_provenance_row()])
    session = SimpleNamespace(execute=execute)
    access = object()
    arguments_by_name = {
        "schema_name": "mrf",
        "logical_snapshot_id": physical_binding.snapshot_id,
        "expected_source_count": 1,
        "serving_tables": descriptor,
        "candidate_audit_access": access,
    }
    if reader_name == "fetch_snapshot_source_provenance":
        arguments_by_name["source_keys"] = (0,)
    reader = getattr(ptg2_shared_blocks, reader_name)
    if does_refuse:
        with pytest.raises(native.PTG2PhysicalBindingError, match="binding drift"):
            await reader(session, **arguments_by_name)
        execute.assert_not_awaited()
    else:
        observed_read = await reader(session, **arguments_by_name)
        if reader_name == "fetch_snapshot_source_provenance":
            assert set(observed_read) == {0}
        elif reader_name == "fetch_snapshot_source_set_identity":
            assert observed_read[2] == ("2" * 64,)
        else:
            assert observed_read["source_count"] == 1
        sql, parameters_by_name = execute.await_args.args
        assert physical_binding.relation("ptg2_v3_snapshot_source") in str(sql)
        if reader_name == "fetch_snapshot_source_provenance":
            assert all(
                physical_binding.relation(table) in str(sql) for table in ("ptg2_source_trace_set", "ptg2_source_trace")
            )
        assert parameters_by_name["snapshot_id"] == physical_binding.payload_snapshot_id
    read.assert_awaited_once_with(
        session, physical_binding.snapshot_id, serving_tables=descriptor, candidate_audit_access=access
    )
