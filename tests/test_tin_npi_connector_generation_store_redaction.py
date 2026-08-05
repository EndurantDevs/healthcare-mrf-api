# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exception-graph redaction proofs for connector generation storage."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from process.tin_npi_connector_generation_store import (
    TinNpiConnectorGenerationStoreError,
    load_and_seal_admitted_connector_generation,
)
from tests.test_tin_npi_connector_generation_store import (
    _StoreConnection,
    _limits_for,
    _multi_source_bundle,
)
from tests.tin_npi_connector_unit_support import TEST_HMAC_HEX


@pytest.mark.asyncio
async def test_store_timeout_is_redacted_and_rolls_back(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    connection = _TimeoutConnection(bundle)
    limits = replace(
        _limits_for(bundle),
        build_lease_seconds=30,
        lock_timeout_ms=1,
        statement_timeout_ms=2,
        operation_timeout_seconds=0.01,
    )

    with pytest.raises(
        TinNpiConnectorGenerationStoreError,
        match="^connector generation load timed out$",
    ) as captured_error:
        await load_and_seal_admitted_connector_generation(
            connection, bundle, limits=limits, schema="mrf"
        )

    _assert_exception_graph_redacted(captured_error.value)
    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.asyncio
async def test_store_database_failure_is_redacted_and_rolls_back(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    connection = _DatabaseFailureConnection(bundle)

    with pytest.raises(
        TinNpiConnectorGenerationStoreError,
        match="^connector generation database operation failed$",
    ) as captured_error:
        await load_and_seal_admitted_connector_generation(
            connection, bundle, limits=_limits_for(bundle), schema="mrf"
        )

    _assert_exception_graph_redacted(captured_error.value)
    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.asyncio
async def test_store_connection_state_failure_is_redacted(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    connection = _ConnectionStateFailure(bundle)

    with pytest.raises(
        TinNpiConnectorGenerationStoreError,
        match="^connector generation connection state is unavailable$",
    ) as captured_error:
        await load_and_seal_admitted_connector_generation(
            connection, bundle, limits=_limits_for(bundle), schema="mrf"
        )

    _assert_exception_graph_redacted(captured_error.value)
    assert connection.transactions == 0


def _assert_exception_graph_redacted(root_error: BaseException) -> None:
    assert root_error.__cause__ is None
    assert root_error.__context__ is None
    pending_errors: list[BaseException | None] = [root_error]
    seen_error_ids: set[int] = set()
    while pending_errors:
        current_error = pending_errors.pop()
        if current_error is None or id(current_error) in seen_error_ids:
            continue
        seen_error_ids.add(id(current_error))
        rendered_forms = (
            repr(current_error),
            str(current_error),
            repr(vars(current_error)),
        )
        assert all(TEST_HMAC_HEX not in rendered for rendered in rendered_forms)
        pending_errors.extend((current_error.__cause__, current_error.__context__))


class _TimeoutConnection(_StoreConnection):
    async def execute(self, sql, *arguments):
        await asyncio.sleep(1)
        return await super().execute(sql, *arguments)


class _DatabaseFailureConnection(_StoreConnection):
    async def execute(self, sql, *arguments):
        raise RuntimeError(f"synthetic database detail {TEST_HMAC_HEX}")


class _ConnectionStateFailure(_StoreConnection):
    def is_in_transaction(self):
        raise RuntimeError(f"synthetic connection detail {TEST_HMAC_HEX}")
