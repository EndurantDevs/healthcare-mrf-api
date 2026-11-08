# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The coverage body borrows its caller's transaction without SOURCE authority."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_serving_coverage as coverage


async def test_prepared_coverage_refuses_before_sql_without_original_transaction():
    session = SimpleNamespace(in_transaction=lambda: False, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(ValueError, match="coverage_requires_transaction"):
        await coverage.prepare_cms_candidate_coverage_in_session(
            session, SimpleNamespace(dataset_id="candidate", endpoint_id="endpoint"), "a" * 64, "b" * 64
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


async def test_prepared_coverage_refuses_invalid_header_before_validation_or_insert(monkeypatch):
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: None))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(return_value=result), scalar=AsyncMock())
    validator = AsyncMock()

    @asynccontextmanager
    async def query_scope(actual):
        assert actual is session
        yield

    monkeypatch.setattr(coverage, "_coverage_query_scope", query_scope)
    monkeypatch.setattr(coverage, "validate_cms_candidate_coverage", validator)
    with pytest.raises(RuntimeError, match="candidate_coverage_invalid"):
        await coverage.prepare_cms_candidate_coverage_in_session(
            session, SimpleNamespace(dataset_id="candidate", endpoint_id="endpoint"), "a" * 64, "b" * 64
        )
    validator.assert_not_awaited()
    session.scalar.assert_not_awaited()
    assert not any("INSERT INTO" in str(call.args[0]) for call in session.execute.await_args_list)
