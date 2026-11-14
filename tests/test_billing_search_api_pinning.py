# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""API billing cursors use logical publication generations and named relations."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from api import billing_search_pagination as pagination
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests import test_billing_search_pagination as fixture


def _session(*, generation=1, modes=("repeatable read", "on")):
    transaction = object()
    return SimpleNamespace(
        get_transaction=lambda: transaction,
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(one=lambda: modes),
                SimpleNamespace(one_or_none=lambda: ("11111111-1111-4111-8111-111111111111", generation)),
            ]
        ),
    )


async def test_api_pin_matches_named_logical_generation_bundle():
    session = _session()
    pin = await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert pin == fixture._pin()
    assert len(session.execute.await_args_list) == 2
    assert all(
        "pg_catalog" not in str(call.args[0]) and "LOCK TABLE" not in str(call.args[0])
        for call in session.execute.await_args_list
    )


async def test_changed_logical_publication_expires_the_previous_bundle():
    first = await pagination.capture_billing_search_generation_pin(_session(), fixture._selection(), api_read=True)
    second = await pagination.capture_billing_search_generation_pin(
        _session(generation=2), fixture._selection(), api_read=True
    )
    assert first.snapshot_set_sha256 == second.snapshot_set_sha256
    assert first.generation_bundle_sha256 != second.generation_bundle_sha256


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off")])
async def test_api_pin_requires_read_only_repeatable_transaction(modes):
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(
            _session(modes=modes), fixture._selection(), api_read=True
        )


async def test_cancellation_reaches_the_transaction_owner():
    failure = asyncio.CancelledError()
    session = _session()
    session.execute.side_effect = failure
    with pytest.raises(asyncio.CancelledError) as caught:
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert caught.value is failure


async def test_generation_metadata_sql_failure_uses_the_normal_unavailable_error():
    session = _session()
    failure = DBAPIError(None, None, RuntimeError("missing metadata"))
    session.execute.side_effect = failure
    with pytest.raises(PTG2ManifestArtifactError) as caught:
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert caught.value.__cause__ is failure
