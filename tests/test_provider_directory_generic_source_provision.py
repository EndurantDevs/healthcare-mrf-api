# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source family shape checks and the managed payload roots."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_provision as provision
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy


def _policy():
    return GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))


def test_doctors_maps_are_managed_global_payload_roots():
    from db.models.provider_directory_entity_identity import (
        ProviderDirectoryCMSDoctorsGroupBinding,
        ProviderDirectoryCMSDoctorsSiteBinding,
    )

    for model, root in (
        (ProviderDirectoryCMSDoctorsGroupBinding, provision.CMS_DOCTORS_ROOTS[0]),
        (ProviderDirectoryCMSDoctorsSiteBinding, provision.CMS_DOCTORS_ROOTS[1]),
    ):
        table = model.__table__
        assert model.__runtime_schema_sync__ is False
        assert table.info["managed_immutable_storage"] is True
        assert root in provision.PAYLOAD_ROOTS and root not in provision.PARTITIONED_ROOTS
        assert not table.foreign_keys
        assert table.primary_key
        assert {constraint.__class__.__name__ for constraint in table.constraints} >= {
            "PrimaryKeyConstraint",
            "UniqueConstraint",
            "CheckConstraint",
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_root", provision.TABLES)
async def test_legacy_heap_root_refuses_before_any_guard_retirement(monkeypatch, changed_root):
    relation_rows = [
        dict(
            relname=name,
            nspname="source_schema",
            relowner=17,
            relpersistence="p",
            relkind="r" if name in {changed_root, provision.WITNESS} else "p",
            partition_key=None if name in {changed_root, provision.WITNESS} else "LIST (dataset_id)",
            parents=[],
            relrowsecurity=False,
            relforcerowsecurity=False,
            direct_acl_closed=True,
            readers_closed=True,
        )
        for name in provision.PAYLOAD_ROOTS
    ]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=relation_rows), execute=AsyncMock())
    with pytest.raises(GenericCandidateError, match="partitioned_roots_required"):
        await provision.verify_generic_source_family(connection, _policy(), require_inert=False)
    connection.execute.assert_not_awaited()


def _shape_rows():
    return [
        dict(
            oid=index + 1,
            relname=name,
            nspname="source_schema",
            relowner=9999,
            relpersistence="p",
            relkind="p" if name in provision.PARTITIONED_ROOTS else "r",
            partition_key="LIST (dataset_id)" if name in provision.PARTITIONED_ROOTS else None,
            parents=[],
            relrowsecurity=False,
            relforcerowsecurity=False,
            direct_acl_closed=False,
            readers_closed=False,
            inert=True,
            native_checks=True,
            constraints=[{"type": "p", "valid": True}],
        )
        for index, name in enumerate(provision.PAYLOAD_ROOTS)
    ]


@pytest.mark.asyncio
async def test_family_check_runs_no_role_owner_or_acl_proof(monkeypatch):
    """Only table shape is checked; PostgreSQL enforces who may read or write the family."""
    rows = _shape_rows()
    for name in ("verify_proof_control", "capture_source_coverage_control", "capture_checkpoint_controls"):
        monkeypatch.setattr(provision, name, AsyncMock())
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    assert await provision.verify_generic_source_family(connection, _policy()) == rows
    connection.fetch.assert_awaited_once_with(
        provision.CATALOG_SQL,
        [provision.storage_relation("source_schema", name) for name in provision.PAYLOAD_ROOTS],
        0,
        [],
    )


@pytest.mark.asyncio
async def test_read_family_check_runs_no_role_owner_or_acl_proof():
    rows = _shape_rows()
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    assert (
        await provision.verify_generic_source_read_family(connection, "source_schema", "source_owner", ("reader",))
        == rows
    )
    connection.fetch.assert_awaited_once_with(
        provision.CATALOG_SQL,
        [provision.storage_relation("source_schema", name) for name in provision.PAYLOAD_ROOTS],
        0,
        [],
    )
    rows[0]["relrowsecurity"] = True
    with pytest.raises(GenericCandidateError, match="reader_family_changed"):
        await provision.verify_generic_source_read_family(connection, "source_schema", "source_owner", ("reader",))


@pytest.mark.asyncio
async def test_global_root_capture_keeps_complete_family_gate_and_exact_nonpartitioned_layout(monkeypatch):
    from process import provider_directory_generic_source_policy as policy_module

    publisher = SimpleNamespace(connection=object(), policy=_policy())
    monkeypatch.setattr(policy_module, "bound_publisher", lambda: publisher)
    root = provision.CMS_DOCTORS_ROOTS[0]
    family = AsyncMock(return_value=[{"relname": root, "oid": 17}])
    monkeypatch.setattr(provision, "verify_generic_source_family", family)
    assert await provision.capture_generic_source_global_roots(publisher, (root,)) == {
        root: {"relname": root, "oid": 17}
    }
    family.assert_awaited_once_with(publisher.connection, publisher.policy)
    with pytest.raises(GenericCandidateError, match="global_roots_changed"):
        await provision.capture_generic_source_global_roots(publisher, (provision.TABLES[0],))
    assert family.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "insert", "function", "source", "columns", "rules", "extra"])
async def test_only_exact_manual_review_native_target_hooks_survive_payload_closure(drift):
    triggers = []
    for name, (
        source_root,
        target_root,
        source_columns,
        target_columns,
    ) in provision._NATIVE_CONTROL_REFERENCES.items():
        for event, function in ((9, "RI_FKey_restrict_del"), (17, "RI_FKey_noaction_upd")):
            triggers.append(
                dict(
                    conname=name,
                    tgtype=event,
                    proname=function,
                    tgisinternal=True,
                    tgenabled="O",
                    function_schema="pg_catalog",
                    contype="f",
                    convalidated=True,
                    condeferrable=False,
                    confupdtype="a",
                    confdeltype="r",
                    confrelid=17 if target_root.endswith("evidence") else 18,
                    nspname="source_schema",
                    relname=source_root,
                    has_rules=False,
                    source_columns=list(source_columns),
                    target_columns=list(target_columns),
                )
            )
    if drift == "extra":
        triggers.append(dict(triggers[0]))
    elif drift is not None:
        key, changed_value = {
            "insert": ("tgtype", 5),
            "function": ("function_schema", "source_schema"),
            "source": ("relname", "other_control"),
            "columns": ("target_columns", ["resource_id"]),
            "rules": ("has_rules", True),
        }[drift]
        triggers[0][key] = changed_value
    connection = SimpleNamespace(fetch=AsyncMock(return_value=triggers))
    family_rows = [
        {"oid": oid, "relname": root, "inert": False}
        for oid, root in ((17, provision.CMS_IDENTITY_ROOTS[1]), (18, provision.CMS_IDENTITY_ROOTS[0]))
    ]
    if drift is None:
        assert await provision._native_control_references(connection, "source_schema", family_rows) == {17, 18}
    else:
        with pytest.raises(GenericCandidateError, match="family_guard_or_foreign_key"):
            await provision._native_control_references(connection, "source_schema", family_rows)
    connection.fetch.assert_awaited_once()


def test_resource_uuid_root_is_managed_and_closed_before_guard_retirement():
    from db.models.provider_directory_resource_identity import ProviderDirectoryResourceIdentity

    model = ProviderDirectoryResourceIdentity
    assert model.__runtime_schema_sync__ is False
    assert model.__table__.info["managed_immutable_storage"] is True
    assert model.__tablename__ == provision.CMS_RESOURCE_IDENTITY_ROOT
    assert model.__tablename__ in provision.GLOBAL_ROOTS
    assert not model.__table__.foreign_keys
    assert model.__table__.primary_key
    assert any(index.unique for index in model.__table__.indexes)
