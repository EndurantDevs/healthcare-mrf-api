# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native attachment custody failures must not authorize reads or retirement."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import ptg_membership_storage as storage
from api.ptg_membership_contract import CONTRACT, SPEC, digest
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_ptg_membership_projection import PARAMS


def _session(*, row=None, rows=None, scalar=None):
    result = SimpleNamespace(first=lambda: row, all=lambda: rows, mappings=lambda: SimpleNamespace(one=lambda: row))
    return SimpleNamespace(execute=AsyncMock(return_value=result), scalar=AsyncMock(return_value=scalar))


@pytest.mark.asyncio
async def test_unsafe_namespace_owner_never_authorizes_attachment_lookup(monkeypatch):
    session = _session(row=(101, False))
    view = AsyncMock()
    monkeypatch.setattr(storage, "_active_view", view)
    with pytest.raises(PTG2ManifestArtifactError, match="namespace owner"):
        await storage.locate(session, PARAMS)
    view.assert_not_awaited()
    assert "NOT owner.rolcanlogin" in str(session.execute.call_args.args[0])


@pytest.mark.asyncio
async def test_absent_protected_namespace_has_no_active_index(monkeypatch):
    view = AsyncMock()
    monkeypatch.setattr(storage, "_active_view", view)
    assert await storage.locate(_session(), PARAMS) is None
    view.assert_not_awaited()


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
@pytest.mark.parametrize("row", [(12, 999, True, True), (12, 101, False, True), (12, 101, True, False)])
async def test_attachment_requires_protected_persistent_view_before_acl_check(monkeypatch, row):
    check = AsyncMock()
    monkeypatch.setattr(storage, "_require_no_untrusted_mutation", check)
    with pytest.raises(PTG2ManifestArtifactError, match="attachment custody"):
        await storage._active_view(_session(row=row), PARAMS, 101)
    check.assert_not_awaited()


def _dependencies(schema="reference_family_archive_00000000000000000000000000000001"):
    return [(index + 200, 100, schema, table) for index, table in enumerate(SPEC.table_names)]


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["missing_table", "extra_table", "two_schemas", "wrong_prefix", "invalid_uuid"])
async def test_attachment_dependency_and_namespace_drift_refuses_before_storage_lock(monkeypatch, mutation):
    rows = _dependencies()
    if mutation == "missing_table":
        rows.pop()
    elif mutation == "extra_table":
        rows.append((999, 100, rows[0][2], "unexpected_table"))
    elif mutation == "two_schemas":
        rows[-1] = (rows[-1][0], 999, "other_schema", rows[-1][3])
    else:
        schema = "other_" + "0" * 32 if mutation == "wrong_prefix" else "reference_family_archive_invalid"
        rows = _dependencies(schema)
    lock = AsyncMock()
    monkeypatch.setattr(storage.native, "_lock_family", lock)
    message = "namespace" if mutation in {"wrong_prefix", "invalid_uuid"} else "dependencies"
    with pytest.raises(PTG2ManifestArtifactError, match=message):
        await storage._attachment_storage(_session(rows=rows), 12, 101)
    lock.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "schema", "relation"])
async def test_attachment_locks_exact_dependencies_and_checks_recaptured_custody(monkeypatch, drift):
    rows = _dependencies()
    ownership = SimpleNamespace(schema_oid=100, relation_oids=tuple((row[3], row[0]) for row in rows))
    if drift == "schema":
        ownership.schema_oid += 1
    elif drift == "relation":
        ownership.relation_oids = ownership.relation_oids[:-1] + ((rows[-1][3], 999),)
    lock, capture, closed = AsyncMock(), AsyncMock(return_value=ownership), AsyncMock()
    monkeypatch.setattr(storage.native, "_lock_family", lock)
    monkeypatch.setattr(storage.native, "capture_model_family_stage_ownership", capture)
    monkeypatch.setattr(storage, "_require_closed_local_custody", closed)
    session = _session(rows=rows)
    if drift:
        with pytest.raises(PTG2ManifestArtifactError, match="storage identity changed"):
            await storage._attachment_storage(session, 12, 101)
        closed.assert_not_awaited()
    else:
        assert await storage._attachment_storage(session, 12, 101) is ownership
        closed.assert_awaited_once_with(session, ownership, 101)
    lock.assert_awaited_once_with(session, rows[0][2], SPEC.table_names, "ACCESS SHARE", nowait=True)
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
        await storage._require_no_references(object(), '"synthetic"', PARAMS)
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
    owner, fence, lock, locate = AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(return_value=None)
    monkeypatch.setattr(storage.native, "protected_publisher_owner", owner)
    monkeypatch.setattr(lifecycle, "acquire_ptg2_lifecycle_lock", fence)
    monkeypatch.setattr(storage, "lock_identity", lock)
    monkeypatch.setattr(storage, "locate", locate)
    assert await storage.retire_unreferenced(session, PARAMS) is False
    owner.assert_awaited_once_with(session)
    fence.assert_awaited_once_with(session)
    lock.assert_awaited_once_with(session, PARAMS)
    locate.assert_awaited_once_with(session, PARAMS)
    session.execute.assert_not_awaited()
