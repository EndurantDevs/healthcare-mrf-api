# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical publication scope keeps native transaction and accounting boundaries."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace

import pytest

from process import provider_directory_cms_source_runtime as runtime
from process import provider_directory_generic_source_policy as policy
from tests.test_provider_directory_cms_source_runtime import registry_publication_case as registry_publication_case


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["closed_transaction", "isolation", "readonly", "lock"])
async def test_publication_requires_active_bounded_native_transaction(registry_publication_case, changed):
    async with registry_publication_case() as case:
        async with runtime.registry_source_publication(
            case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        ):
            match changed:
                case "closed_transaction":
                    case.native.active = False
                case "isolation":
                    case.native.isolation = "repeatable read"
                case "readonly":
                    case.native.readonly = "on"
                case "lock":
                    case.native.locked = False
            try:
                with pytest.raises(ValueError, match="cms_registry_source_publication_invalid"):
                    await runtime.require_registry_epoch_transaction(
                        case.driver, case.pair.request.source_pin, case.pair.recipe.source_pin.retained_epoch
                    )
                assert case.group["consumed"] is False
                assert case.outcome.commit_state == "not_attempted"
            finally:
                case.native.active, case.native.locked = True, True
                case.native.isolation, case.native.readonly = "read committed", "off"
        assert runtime._REGISTRY_PUBLICATION.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["missing", "duplicate"])
async def test_publication_requires_one_pending_logical_accounting_group(registry_publication_case, changed):
    async with registry_publication_case() as case:
        async with runtime.registry_source_publication(
            case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        ):
            groups = case.prepared.fhir._provider_directory_profile_capacity_admission().wal_tracker.owned_control_transaction_groups
            original_groups = list(groups)
            groups[:] = [] if changed == "missing" else [case.group, dict(case.group)]
            try:
                with pytest.raises(ValueError, match="cms_registry_source_publication_invalid"):
                    await runtime.require_registry_epoch_transaction(
                        case.driver, case.pair.request.source_pin, case.pair.recipe.source_pin.retained_epoch
                    )
                assert case.group["consumed"] is False
            finally:
                groups[:] = original_groups
        assert runtime._REGISTRY_PUBLICATION.get() is None


@pytest.mark.asyncio
async def test_publication_receipt_payload_change_cannot_authorize_the_retained_epoch(registry_publication_case):
    async with registry_publication_case() as case:
        async with runtime.registry_source_publication(
            case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        ):
            original = deepcopy(case.row["payload"])
            case.row["payload"]["unexpected_authority"] = "foreign"
            try:
                with pytest.raises(ValueError, match="cms_registry_source_publication_invalid"):
                    await runtime.require_registry_epoch_transaction(
                        case.driver, case.pair.request.source_pin, case.pair.recipe.source_pin.retained_epoch
                    )
                assert case.outcome.commit_state == "not_attempted"
            finally:
                case.row["payload"] = original
        assert runtime._REGISTRY_PUBLICATION.get() is None


@pytest.mark.asyncio
async def test_nested_publication_attempt_keeps_the_original_scope_intact(registry_publication_case):
    async with registry_publication_case() as case:
        arguments = case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        async with runtime.registry_source_publication(*arguments):
            original_scope = runtime._REGISTRY_PUBLICATION.get()
            with pytest.raises(ValueError, match="cms_registry_source_publication_invalid"):
                async with runtime.registry_source_publication(*arguments):
                    pytest.fail("nested publication must not acquire custody")
            assert runtime._REGISTRY_PUBLICATION.get() is original_scope
        assert runtime._REGISTRY_PUBLICATION.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("synthetic publication failure"), asyncio.CancelledError()])
async def test_failed_publication_clears_context_without_consuming_owned_group(registry_publication_case, failure):
    async with registry_publication_case() as case:
        with pytest.raises(type(failure)) as observed:
            async with runtime.registry_source_publication(
                case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
            ):
                raise failure
        assert observed.value is failure
        assert runtime._REGISTRY_PUBLICATION.get() is None
        assert case.group["consumed"] is False
        assert case.outcome.commit_state == "not_attempted"
    assert policy._PUBLISHER.get() is policy._OWNER.get() is None
