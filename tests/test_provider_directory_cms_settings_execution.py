# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Whole-inventory settings composition; installer/catalog IO is explicitly replaced."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_owner_execution as execution
from process import provider_directory_cms_routine_settings as settings


@pytest.fixture
def settings_case(monkeypatch):
    case = SimpleNamespace(
        inventory={
            "canonical": {"relations": ["protected original"]},
            "private": ["protected private"],
            "routines": [{"definition": "protected predecessor"}],
            "roles": {"roles": ["protected vector"]},
        },
        routines=[{"definition": "independently verified exact successor"}],
        binding=SimpleNamespace(catalog_schema="snapshot_schema", catalog_schema_oid=42),
        connection=SimpleNamespace(commit=AsyncMock(), execute=AsyncMock()),
        profile={"contract": "explicit IO substitute"},
        policy=object(),
    )
    case.after = {**deepcopy(case.inventory), "routines": deepcopy(case.routines)}
    case.install = AsyncMock(return_value=deepcopy(case.routines))
    case.collect = AsyncMock(return_value=deepcopy(case.after))
    monkeypatch.setattr(settings, "install_cms_routine_settings", case.install)
    monkeypatch.setattr(execution.profiles, "_collect_profile", case.collect)
    return case


async def _apply(case, deadline):
    return await execution.apply_cms_common_routine_settings(
        case.connection,
        inventory=case.inventory,
        storage_binding=case.binding,
        policy=case.policy,
        cms_profile=case.profile,
        deadline=deadline,
    )


@pytest.mark.asyncio
async def test_complete_postimage_requires_installer_output_and_preserves_original(settings_case):
    case = settings_case
    saved = deepcopy(case.inventory)
    deadline = asyncio.get_running_loop().time() + 2
    assert await _apply(case, deadline) == case.after
    case.install.assert_awaited_once_with(
        case.connection,
        expected_routines=case.inventory["routines"],
        catalog_schema="snapshot_schema",
        catalog_schema_oid=42,
        preparation_schema="hp_snapshot_retention",
        deadline=deadline,
    )
    case.collect.assert_awaited_once_with(case.connection, case.binding, case.policy, case.profile)
    assert case.inventory == saved
    case.connection.commit.assert_not_awaited()
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("section", ["canonical", "private", "routines", "roles"])
async def test_any_unplanned_catalog_projection_refuses(settings_case, section):
    case = settings_case
    case.collect.return_value[section] = "unreviewed observed drift"
    with pytest.raises(ValueError, match="settings_postimage_changed"):
        await _apply(case, asyncio.get_running_loop().time() + 2)
    case.connection.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_deadline_refuses_before_installer_or_catalog(settings_case):
    case = settings_case
    with pytest.raises(ValueError, match="deadline_required"):
        await _apply(case, asyncio.get_running_loop().time() - 1)
    case.install.assert_not_awaited()
    case.collect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError, TimeoutError, asyncio.CancelledError])
async def test_installer_failures_propagate_without_catalog_or_commit(settings_case, failure):
    case = settings_case
    original = failure("settings installer failed")
    case.install.side_effect = original
    with pytest.raises(failure) as caught:
        await _apply(case, asyncio.get_running_loop().time() + 2)
    assert caught.value is original
    case.collect.assert_not_awaited()
    case.connection.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_absolute_deadline_covers_the_final_catalog_read(settings_case):
    case = settings_case

    async def waiting(*arguments):
        await asyncio.Future()

    case.collect.side_effect = waiting
    with pytest.raises(TimeoutError):
        await _apply(case, asyncio.get_running_loop().time() + 0.02)
    case.install.assert_awaited_once()
    case.collect.assert_awaited_once()
    case.connection.commit.assert_not_awaited()
