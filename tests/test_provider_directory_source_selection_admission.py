# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional selection admission precedes transport and storage of resolved sources."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_generic_source_policy as policy
from tests.test_provider_directory_proxy_qualification import source_entry as source_entry
from tests.test_provider_directory_source_qualification import directory


@pytest.mark.asyncio
async def test_selection_admission_is_optional_and_uses_only_the_bound_factory():
    rows = [{"source_id": "synthetic-source"}]
    policy.require_source_selection(rows)
    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=lambda: None)):
        policy.require_source_selection(rows)
    validate = Mock(return_value=None)
    factory = SimpleNamespace(transaction=lambda: None, require_source_selection=validate)
    async with policy.bind_generic_source_transaction_factory(factory):
        policy.require_source_selection(rows)
    validate.assert_called_once_with(rows)
    policy.require_source_selection(rows)
    validate.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("validator", [False, Mock(return_value=False)])
async def test_malformed_selection_validator_refuses(validator):
    factory = SimpleNamespace(transaction=lambda: None, require_source_selection=validator)
    async with policy.bind_generic_source_transaction_factory(factory):
        with pytest.raises(ValueError, match="source_selection_validator_"):
            policy.require_source_selection([])


@pytest.mark.asyncio
async def test_alternate_first_seed_refuses_before_proxy_probe_or_source_writes(source_entry, monkeypatch):
    seed = directory._seed_rows_from_sqlite.return_value[0]
    alternate_seed_by_field = {**seed, "endpoint_practitioner": seed["api_base"] + "/other"}
    directory._seed_rows_from_sqlite.return_value = [alternate_seed_by_field, seed]

    def validate(rows):
        assert rows[0]["source_id"] == source_entry.source_record["source_id"]
        assert rows[0]["endpoint_practitioner"] == alternate_seed_by_field["endpoint_practitioner"]
        raise ValueError("synthetic_selection_changed")

    policy._FACTORY.get().require_source_selection = validate
    prepare, qualify = AsyncMock(), AsyncMock()
    monkeypatch.setattr(directory.source_proxy.SourceProxyRun, "prepare", prepare)
    monkeypatch.setattr(directory.source_selection, "qualify_stale_isolated_source", qualify)
    with pytest.raises(ValueError, match="synthetic_selection_changed"):
        await directory.process_provider_directory_fhir_data(source_entry.context, source_entry.task)
    prepare.assert_not_awaited()
    qualify.assert_not_awaited()
    source_entry.upsert.assert_not_awaited()
    directory._fetch_json_sync.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_selection_is_rechecked_after_qualification_before_source_writes(source_entry, monkeypatch, changed):
    events = []

    def validate(rows):
        events.append("admit")
        if rows[0].get("endpoint_practitioner"):
            raise ValueError("synthetic_selection_changed")

    async def prepare(*arguments, **keywords):
        events.append("proxy")

    async def qualify(_directory, rows, *arguments, **keywords):
        events.append("qualify")
        qualified_source_by_field = {**rows[0], "last_validated_status": "valid"}
        if changed:
            qualified_source_by_field["endpoint_practitioner"] = qualified_source_by_field["api_base"] + "/other"
        return [qualified_source_by_field]

    async def upsert(rows, **keywords):
        events.append("write")
        assert rows[0]["last_validated_status"] == "valid"
        raise RuntimeError("synthetic_storage_reached")

    policy._FACTORY.get().require_source_selection = validate
    monkeypatch.setattr(directory.source_proxy.SourceProxyRun, "prepare", prepare)
    monkeypatch.setattr(directory.source_selection, "qualify_stale_isolated_source", qualify)
    source_entry.upsert.side_effect = upsert
    with pytest.raises(ValueError if changed else RuntimeError, match="synthetic_"):
        await directory.process_provider_directory_fhir_data(source_entry.context, source_entry.task)
    assert events == ["admit", "proxy", "qualify", "admit", *([] if changed else ["write"])]
    assert source_entry.upsert.await_count == (0 if changed else 1)
