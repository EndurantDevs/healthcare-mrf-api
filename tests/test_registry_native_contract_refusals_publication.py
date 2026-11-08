# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original publication custody cannot survive changed native/session authority."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace

import pytest

from process import provider_directory_cms_source_runtime as runtime
from process import provider_directory_generic_source_policy as policy
from tests.test_provider_directory_cms_source_runtime import registry_publication_case as registry_publication_case


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["nested", "connection", "closed_transaction", "publisher_with_owner_context"])
async def test_publication_rejects_changed_original_session_before_epoch_use(registry_publication_case, changed):
    async with registry_publication_case() as case:
        async with runtime.registry_source_publication(
            case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        ):
            original_connection = case.outcome.connection
            match changed:
                case "nested":
                    case.session.in_nested_transaction = lambda: True
                case "connection":
                    case.session.connection.return_value = SimpleNamespace()
                case "closed_transaction":
                    case.native.active = False
                case "publisher_with_owner_context":
                    case.native.actor = "source_publisher"
            try:
                with pytest.raises(ValueError, match="cms_registry_source_publication_invalid"):
                    await runtime.require_registry_epoch_transaction(
                        case.driver, case.pair.request.source_pin, case.pair.recipe.source_pin.retained_epoch
                    )
                assert case.group["consumed"] is False
                assert case.outcome.commit_state == "not_attempted"
            finally:
                case.session.in_nested_transaction = lambda: False
                case.session.connection.return_value = original_connection
                case.native.active, case.native.actor = True, "source_owner"
        assert runtime._REGISTRY_PUBLICATION.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["missing", "replacement"])
async def test_publication_requires_the_same_registered_owned_group(registry_publication_case, changed):
    async with registry_publication_case() as case:
        async with runtime.registry_source_publication(
            case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        ):
            groups = case.prepared.nonprofile_admission.wal_tracker.owned_control_transaction_groups
            original_groups = list(groups)
            groups[:] = [] if changed == "missing" else [dict(case.group)]
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
