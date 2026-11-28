# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native attachment custody failures must not authorize reads or retirement."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import plan_pricing_code_storage as code_storage
from api import ptg_membership_storage as storage
from api.ptg_membership_contract import CONTRACT, SPEC, digest
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_ptg_membership_projection import PARAMS


def _session(*, row=None, rows=None, scalar=None):
    result = SimpleNamespace(first=lambda: row, all=lambda: rows, mappings=lambda: SimpleNamespace(one=lambda: row))
    return SimpleNamespace(
        execute=AsyncMock(return_value=result), scalar=AsyncMock(return_value=scalar), in_transaction=lambda: True
    )


@pytest.mark.asyncio
async def test_lookup_uses_the_named_attachment_without_role_census(monkeypatch):
    session = _session()
    view = AsyncMock(return_value=None)
    monkeypatch.setattr(storage, "_active_view", view)
    assert await storage.locate(session, PARAMS) is None
    view.assert_awaited_once_with(session, PARAMS)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_absent_named_attachment_has_no_active_index():
    session = _session()
    assert await storage.locate(session, PARAMS) is None
    assert "relkind='v'" in str(session.execute.await_args.args[0])
    assert "pg_roles" not in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [False, None, 1])
async def test_missing_durable_source_pin_is_fail_closed(value):
    session = _session(scalar=value)
    with pytest.raises(PTG2ManifestArtifactError, match="retention pin is missing"):
        await storage.source_pin(session, PARAMS)
    session.execute.assert_not_awaited()
    assert session.scalar.call_args.args[1] == dict(
        owner_type=CONTRACT, owner_id=digest(PARAMS), snapshot_id=PARAMS["snapshot_id"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("row", [(False, True), (True, False), (False, False)])
async def test_attachment_requires_a_persistent_native_view(row):
    with pytest.raises(PTG2ManifestArtifactError, match="attachment layout"):
        await storage._active_view(_session(row=row), PARAMS)


def _dependencies(schema="reference_family_archive_00000000000000000000000000000001"):
    return [(schema, table) for table in SPEC.table_names]


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["missing_table", "extra_table", "two_schemas", "wrong_prefix", "invalid_uuid"])
async def test_attachment_dependency_and_namespace_drift_refuses_before_storage_lock(monkeypatch, mutation):
    rows = _dependencies()
    if mutation == "missing_table":
        rows.pop()
    elif mutation == "extra_table":
        rows.append((rows[0][0], "unexpected_table"))
    elif mutation == "two_schemas":
        rows[-1] = ("other_schema", rows[-1][1])
    else:
        schema = "other_" + "0" * 32 if mutation == "wrong_prefix" else "reference_family_archive_invalid"
        rows = _dependencies(schema)
    lock = AsyncMock()
    monkeypatch.setattr(storage.native, "_lock_family", lock)
    message = "namespace" if mutation in {"wrong_prefix", "invalid_uuid"} else "dependencies"
    with pytest.raises(PTG2ManifestArtifactError, match=message):
        await storage._attachment_storage(
            _session(rows=rows), "hp_snapshot_retention." + storage.attachment_name(PARAMS)
        )
    lock.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "schema", "relation", "row_security"])
async def test_attachment_locks_complete_names_and_checks_recaptured_layout(monkeypatch, drift):
    rows = _dependencies()
    ownership = SimpleNamespace(schema_name=rows[0][0], relation_names=tuple(row[1] for row in rows))
    if drift == "schema":
        ownership.schema_name = "other_schema"
    elif drift == "relation":
        ownership.relation_names = ownership.relation_names[:-1] + ("other_table",)
    lock, capture = AsyncMock(), AsyncMock(return_value=ownership)
    monkeypatch.setattr(storage.native, "_lock_family", lock)
    monkeypatch.setattr(storage, "read_model_family_ownership", capture)
    plain = AsyncMock(return_value=drift != "row_security")
    monkeypatch.setattr(code_storage, "has_locked_plain_heaps", plain)
    session = _session(rows=rows)
    view = "hp_snapshot_retention." + storage.attachment_name(PARAMS)
    if drift:
        refusal = "storage is not plain" if drift == "row_security" else "storage names changed"
        with pytest.raises(PTG2ManifestArtifactError, match=refusal):
            await storage._attachment_storage(session, view)
    else:
        assert await storage._attachment_storage(session, view) is ownership
    lock.assert_awaited_once_with(session, rows[0][0], SPEC.table_names, "ACCESS SHARE", nowait=True)
    capture.assert_awaited_once_with(session, SPEC, UUID(int=1))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"identity": PARAMS | {"code": "99214"}},
        {"content_digest": "g" * 64},
        {"content_digest": None},
        {"content_digest": "a" * 63},
    ],
)
async def test_sealed_header_identity_and_digest_refused_before_coverage_read(changes):
    header = dict(identity=PARAMS, content_digest="a" * 64, counts={"code_coverage": []}) | changes
    session = _session(row=header)
    with pytest.raises(PTG2ManifestArtifactError, match="sealed identity"):
        await storage._sealed_header(session, "synthetic", PARAMS)
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_external_pin_is_preserved_and_vetoes_retirement(monkeypatch):
    from process.ptg_parts import source_snapshot_references as references

    external_pin_by_field = dict(
        owner_type="synthetic_external_consumer", owner_id="consumer", snapshot_id=PARAMS["snapshot_id"]
    )
    monkeypatch.setattr(
        references, "load_snapshot_reference_rows", AsyncMock(return_value={"pin": [external_pin_by_field]})
    )
    sibling = AsyncMock()
    monkeypatch.setattr(storage, "_require_sibling_pin", sibling)
    assert await storage._external_pins(object(), PARAMS, storage._pin(PARAMS), [external_pin_by_field]) == [
        external_pin_by_field
    ]
    with pytest.raises(PTG2ManifestArtifactError, match="still has retention references"):
        await storage._require_no_references(object(), PARAMS)
    sibling.assert_not_awaited()


@pytest.mark.asyncio
async def test_malformed_sibling_pin_never_reads_an_attachment():
    session = _session()
    with pytest.raises(PTG2ManifestArtifactError, match="sibling retention pin"):
        await storage._require_sibling_pin(session, PARAMS, "not-a-digest")
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["not_document", "digest", "snapshot", "source", "absent", "ready"])
async def test_sibling_pin_requires_exact_sealed_same_source_attachment(monkeypatch, kind):
    sibling = PARAMS | {"code": "99214"}
    if kind == "snapshot":
        sibling["snapshot_id"] = "other-snapshot"
    elif kind == "source":
        sibling["source_key"] = "other-network"
    key = digest(sibling)
    row = sibling
    if kind == "not_document":
        row = None
    elif kind == "digest":
        row = sibling | {"code": "99215"}
    locate = AsyncMock(return_value=None if kind == "absent" else object())
    monkeypatch.setattr(storage, "locate", locate)
    session = _session(scalar=row)
    if kind == "ready":
        await storage._require_sibling_pin(session, PARAMS, key)
        locate.assert_awaited_once_with(session, sibling)
    else:
        with pytest.raises(PTG2ManifestArtifactError, match="sibling retention pin"):
            await storage._require_sibling_pin(session, PARAMS, key)
        if kind != "absent":
            locate.assert_not_awaited()


@pytest.mark.asyncio
async def test_absent_retirement_holds_existing_fences_but_never_drops_or_deletes(monkeypatch):
    from process.ptg_parts import ptg2_lifecycle_lock as lifecycle

    session = _session()
    fence, lock, locate = AsyncMock(), AsyncMock(), AsyncMock(return_value=None)
    monkeypatch.setattr(lifecycle, "acquire_ptg2_lifecycle_lock", fence)
    monkeypatch.setattr(storage, "lock_identity", lock)
    monkeypatch.setattr(storage, "locate", locate)
    assert await storage.retire_unreferenced(session, PARAMS) is False
    fence.assert_awaited_once_with(session)
    lock.assert_awaited_once_with(session, PARAMS)
    locate.assert_awaited_once_with(session, PARAMS)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_membership_storage_read_names_its_tables_in_one_query():
    dataset_id = UUID(int=7)
    names = sorted(SPEC.table_names)
    rows = [(11, name, 100 + index) for index, name in enumerate(names)]
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: rows)),
    )
    ownership = await storage.read_model_family_ownership(session, SPEC, dataset_id)
    assert session.execute.await_count == 1
    assert ownership.schema_name == storage.native.reference_family_stage_schema(dataset_id)
    assert ownership.relation_names == tuple(names) and ownership.schema_oid == 11
    session.execute.return_value = SimpleNamespace(all=lambda: rows[:-1])
    with pytest.raises(storage.native.ReferenceFamilyArchiveError, match="relation is missing"):
        await storage.read_model_family_ownership(session, SPEC, dataset_id)
