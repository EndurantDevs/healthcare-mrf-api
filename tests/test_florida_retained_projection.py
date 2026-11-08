# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained publication keeps the original heaps and only approved read access."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_snapshot_preparation as preparation
from process import florida_projection_archive as archive


def _rows(values):
    rows = SimpleNamespace(all=lambda: values, one_or_none=lambda: values[0] if values else None)
    return SimpleNamespace(scalars=lambda: rows, mappings=lambda: rows)


@pytest.mark.parametrize("oid", [True, 0, -1, 2**32, "20"])
def test_retained_alias_rejects_non_oid_values(oid):
    with pytest.raises(archive.FloridaProjectionArchiveError, match="retained OID differs"):
        archive.retained_projection_name(oid)


@pytest.mark.parametrize("fault", [None, "missing", "mixed"])
async def test_legacy_pointer_requires_one_available_current_generation(monkeypatch, fault):
    from tests.test_florida_projection_archive import _run

    values = ["a" * 32, "b" * 32] if fault == "mixed" else ["a" * 32]
    session = SimpleNamespace(execute=AsyncMock(return_value=_rows(values)))
    monkeypatch.setattr(
        archive.native, "_relation_oid", AsyncMock(side_effect=[None, None if fault == "missing" else 20, None])
    )
    reader = AsyncMock(return_value=_run())
    monkeypatch.setattr(archive, "_run", reader)
    if fault:
        with pytest.raises(archive.FloridaProjectionArchiveError, match="unavailable|mixed"):
            await archive.publication_pointer(session, "fixture")
        reader.assert_not_awaited()
    else:
        assert await archive.publication_pointer(session, "fixture") == {
            "current_run_id": "a" * 32,
            "current_relation_oid": 20,
            "previous_run_id": None,
            "previous_relation_oid": None,
        }
        reader.assert_awaited_once_with(session, "fixture", "a" * 32)


@pytest.mark.parametrize("previous", [None, "b" * 32])
async def test_managed_pointer_binds_current_and_retained_heaps(monkeypatch, previous):
    pointer_by_name = {"current_run_id": "a" * 32, "previous_run_id": previous}
    session = SimpleNamespace(execute=AsyncMock(return_value=_rows([pointer_by_name])))
    monkeypatch.setattr(archive.native, "_relation_oid", AsyncMock(return_value=10))
    retained = AsyncMock(side_effect=[20, 30])
    monkeypatch.setattr(archive, "_retained_run_projection_oid", retained)
    assert await archive.publication_pointer(session, "fixture") == {
        "current_run_id": pointer_by_name["current_run_id"],
        "current_relation_oid": 20,
        "previous_run_id": previous,
        "previous_relation_oid": 30 if previous else None,
    }
    assert retained.await_args_list[0].args == (session, "fixture", pointer_by_name["current_run_id"])
    assert retained.await_args_list[0].kwargs == {"serving": True}
    assert retained.await_count == (2 if previous else 1)


@pytest.mark.parametrize(
    "contract,field",
    [
        (archive.profiles.NATIVE_PUBLICATION_CONTRACT, "handoff"),
        (archive.profiles.NATIVE_CAPTURE_CONTRACT, "serving"),
        (archive.profiles.VALIDATION_CONTRACT, "projection"),
    ],
)
@pytest.mark.parametrize("serving", [False, True])
async def test_retained_authority_selects_the_exact_original_oid(monkeypatch, contract, field, serving):
    projection_by_name = {"relation_oid": 20}
    validation_by_name = {"projection": projection_by_name} if field == "handoff" else projection_by_name
    if field == "projection":
        validation_by_name = {"cutover": projection_by_name}
    proof_by_name = {"validation": {"contract": contract, field: validation_by_name}}
    session = SimpleNamespace(execute=AsyncMock(return_value=_rows([proof_by_name, proof_by_name])))
    relation = AsyncMock(return_value=20)
    monkeypatch.setattr(archive.native, "_relation_oid", relation)
    assert await archive._retained_run_projection_oid(session, "fixture", "a" * 32, serving=serving) == 20
    name = archive.PROJECTION if serving else archive.retained_projection_name(20)
    relation.assert_awaited_once_with(session, "fixture", name)
    assert session.execute.await_args.args[1] == {"source": archive.FL_MQA_SOURCE_KEY, "run": "a" * 32}


@pytest.mark.parametrize("fault", ["missing", "oversized", "unknown", "conflicting", "replaced"])
async def test_retained_authority_refuses_missing_mixed_or_replaced_heaps(monkeypatch, fault):
    proof_by_name = {
        "validation": {"contract": archive.profiles.NATIVE_CAPTURE_CONTRACT, "serving": {"relation_oid": 20}}
    }
    values = [proof_by_name]
    if fault == "missing":
        values = []
    elif fault == "oversized":
        values = [proof_by_name] * 65
    elif fault == "unknown":
        values = [{"validation": {"contract": "unsupported"}}]
    elif fault == "conflicting":
        values.append(
            {"validation": {"contract": archive.profiles.NATIVE_CAPTURE_CONTRACT, "serving": {"relation_oid": 21}}}
        )
    session = SimpleNamespace(execute=AsyncMock(return_value=_rows(values)))
    relation = AsyncMock(return_value=21 if fault == "replaced" else 20)
    monkeypatch.setattr(archive.native, "_relation_oid", relation)
    with pytest.raises(archive.FloridaProjectionArchiveError, match="authority|heap changed"):
        await archive._retained_run_projection_oid(session, "fixture", "a" * 32)
    if fault != "replaced":
        relation.assert_not_awaited()


@pytest.mark.parametrize("bootstrap", [False, True])
async def test_publication_preserves_read_access_before_atomic_alias_cutover(monkeypatch, bootstrap):
    expected_by_name = {"current_run_id": None if bootstrap else "a" * 32, "current_relation_oid": 20}
    seal_by_name = {"table_name": "candidate", "relation_oid": 30}
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[40, False, False]), execute=AsyncMock())
    monkeypatch.setattr(archive.native, "protected_publisher_owner", AsyncMock(return_value=40))
    monkeypatch.setattr(archive, "_publication_lock", AsyncMock())
    monkeypatch.setattr(archive, "publication_pointer", AsyncMock(return_value=expected_by_name))
    monkeypatch.setattr(archive.native, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive.native, "_relation_oid", AsyncMock(side_effect=[30, None, 50]))

    async def preserve(*_args):
        session.execute.assert_not_awaited()

    access = AsyncMock(side_effect=preserve)
    monkeypatch.setattr(archive, "_preserve_retained_read_access", access)
    sealed = AsyncMock()
    monkeypatch.setattr(preparation, "_seal_published_relation", sealed)
    assert await archive.publish_retained_projection(
        session, "fixture", seal_by_name, expected_by_name, "b" * 32, 40
    ) == {
        "run_id": "b" * 32,
        "relation_oid": 30,
    }
    access.assert_awaited_once_with(session, "fixture", "candidate", 20, 40)
    sealed.assert_awaited_once_with(session, 50, 40)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    first = (
        'DROP TABLE "fixture"."provider_profile_projection" RESTRICT'
        if bootstrap
        else ('ALTER TABLE "fixture"."provider_profile_projection" RENAME TO "provider_profile_projection_retained_20"')
    )
    assert statements[:2] == [first, 'ALTER TABLE "fixture"."candidate" RENAME TO "provider_profile_projection"']
    assert session.execute.await_args.args[1] == {
        "source": archive.FL_MQA_SOURCE_KEY,
        "run": "b" * 32,
        "previous": expected_by_name["current_run_id"],
    }


@pytest.mark.parametrize("fault", ["publisher", "predecessor", "candidate", "owner", "dependent", "alias", "bootstrap"])
async def test_publication_refuses_drift_before_any_table_mutation(monkeypatch, fault):
    expected_by_name = {"current_run_id": None if fault == "bootstrap" else "a" * 32, "current_relation_oid": 20}
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[41 if fault == "owner" else 40, fault == "dependent", fault == "bootstrap"]),
        execute=AsyncMock(),
    )
    monkeypatch.setattr(
        archive.native, "protected_publisher_owner", AsyncMock(return_value=41 if fault == "publisher" else 40)
    )
    monkeypatch.setattr(archive, "_publication_lock", AsyncMock())
    monkeypatch.setattr(
        archive, "publication_pointer", AsyncMock(return_value={} if fault == "predecessor" else expected_by_name)
    )
    monkeypatch.setattr(archive.native, "_lock_family", AsyncMock())
    monkeypatch.setattr(
        archive.native,
        "_relation_oid",
        AsyncMock(side_effect=[31 if fault == "candidate" else 30, 21 if fault == "alias" else None]),
    )
    monkeypatch.setattr(archive, "_preserve_retained_read_access", AsyncMock())
    monkeypatch.setattr(preparation, "_seal_published_relation", AsyncMock())
    with pytest.raises(archive.FloridaProjectionArchiveError):
        await archive.publish_retained_projection(
            session, "fixture", {"table_name": "candidate", "relation_oid": 30}, expected_by_name, "b" * 32, 40
        )
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("count", [128, 129])
async def test_read_acl_collection_enforces_its_bound(count):
    session = SimpleNamespace(execute=AsyncMock(return_value=_rows([{}] * count)))
    if count == 129:
        with pytest.raises(archive.FloridaProjectionArchiveError, match="ACL exceeds"):
            await archive._retained_read_grants(session, 20)
    else:
        assert len(await archive._retained_read_grants(session, 20)) == count


async def test_retained_grants_keep_select_but_strip_former_owner_delegation(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock())
    clear = AsyncMock()
    monkeypatch.setattr(archive, "_clear_stage_grants", clear)
    grants = [
        {"grantee": 40, "rolname": "snapshot_owner", "is_grantable": True},
        {"grantee": 20, "rolname": "former_writer", "is_grantable": True},
        {"grantee": 30, "rolname": "reader", "is_grantable": True},
        {"grantee": 0, "rolname": None, "is_grantable": False},
    ]
    await archive._apply_retained_read_grants(session, "fixture", "candidate", grants, 20, 40)
    clear.assert_awaited_once_with(session, "fixture", "candidate", 40)
    assert [str(call.args[0]) for call in session.execute.await_args_list] == [
        'GRANT SELECT ON TABLE "fixture"."candidate" TO "former_writer"',
        'GRANT SELECT ON TABLE "fixture"."candidate" TO "reader" WITH GRANT OPTION',
        'GRANT SELECT ON TABLE "fixture"."candidate" TO PUBLIC',
    ]


async def test_changed_read_role_is_refused_before_grant(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(archive, "_clear_stage_grants", AsyncMock())
    with pytest.raises(archive.FloridaProjectionArchiveError, match="read role changed"):
        await archive._apply_retained_read_grants(
            session, "fixture", "candidate", [{"grantee": 30, "rolname": None}], 20, 40
        )
    session.execute.assert_not_awaited()


async def test_retained_access_uses_observed_owner_and_actual_read_grants(monkeypatch):
    session = object()
    grants = [{"grantee": 30, "rolname": "reader", "is_grantable": False}]
    monkeypatch.setattr(archive, "_live_projection_security", AsyncMock(return_value={"relowner": 20}))
    monkeypatch.setattr(archive, "_retained_read_grants", AsyncMock(return_value=grants))
    apply = AsyncMock()
    monkeypatch.setattr(archive, "_apply_retained_read_grants", apply)
    await archive._preserve_retained_read_access(session, "fixture", "candidate", 10, 40)
    apply.assert_awaited_once_with(session, "fixture", "candidate", grants, 20, 40)


@pytest.mark.parametrize("replaced", [False, True])
async def test_capture_seals_the_same_heap_before_reapplying_read_access(monkeypatch, replaced):
    events = []
    session = object()
    monkeypatch.setattr(archive.native, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive.native, "_relation_oid", AsyncMock(return_value=21 if replaced else 20))
    monkeypatch.setattr(archive, "_live_projection_security", AsyncMock(return_value={"relowner": 10}))
    grants = [{"grantee": 30, "rolname": "reader", "is_grantable": False}]
    monkeypatch.setattr(archive, "_retained_read_grants", AsyncMock(return_value=grants))

    async def seal(*_args):
        events.append("seal")

    async def grant(*_args):
        events.append("grant")

    monkeypatch.setattr(preparation, "_seal_published_relation", seal)
    apply = AsyncMock(side_effect=grant)
    monkeypatch.setattr(archive, "_apply_retained_read_grants", apply)
    serving_by_name = {"table_name": archive.PROJECTION, "relation_oid": 20}
    if replaced:
        with pytest.raises(archive.FloridaProjectionArchiveError, match="serving heap changed"):
            await archive.seal_retained_projection(session, "fixture", serving_by_name, 40)
        assert events == []
    else:
        await archive.seal_retained_projection(session, "fixture", serving_by_name, 40)
        assert events == ["seal", "grant"]
        apply.assert_awaited_once_with(session, "fixture", archive.PROJECTION, grants, 10, 40)
