# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit format selection and physical-key-first read bounds."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_compact_edition as edition
from process import provider_directory_compact_payload as compact
from process import provider_directory_generic_candidate as candidate

pytestmark = pytest.mark.asyncio


def connection(*, format_present=True, aliases_present=True, formats=None):
    return SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value={"format_present": format_present, "aliases_present": aliases_present}),
        fetch=AsyncMock(return_value=[{"format_id": compact.FORMAT}] if formats is None else formats),
    )


async def test_legacy_absence_does_not_read_payload_or_create_layout():
    driver = connection(format_present=False, aliases_present=False)
    assert await edition.compact_edition_tables(driver, "published") is None
    driver.fetch.assert_not_awaited()


@pytest.mark.parametrize(
    "arguments",
    [
        {"format_present": False},
        {"aliases_present": False},
        {"formats": []},
        {"formats": [{"format_id": "unknown"}]},
        {"formats": [{"format_id": compact.FORMAT}] * 2},
        {"formats": [{"format_id": compact.FORMAT, "unexpected": True}]},
    ],
)
async def test_missing_unknown_or_multiple_formats_refuse(arguments):
    with pytest.raises(candidate.GenericCandidateError, match="compact_edition_"):
        await edition.compact_edition_tables(connection(**arguments), "published")


async def test_only_selected_fitting_keys_are_reconstructed(monkeypatch):
    driver = connection()
    tables = await edition.compact_edition_tables(driver, "published")
    keys = [
        dict(resource_type="PractitionerRole", resource_id=str(i), batch_ordinal=1, row_ordinal=i, read_bytes=1000)
        for i in range(3)
    ]
    driver.fetch.return_value = keys
    reconstruct = AsyncMock(return_value=[{"resource_id": "0"}])
    monkeypatch.setattr(compact, "read_compact_payload_keys", reconstruct)
    assert await edition.compact_edition_page(
        driver,
        tables,
        "dataset",
        resource_types=("PractitionerRole",),
        page_bytes=candidate.COPY_FRAME_BYTES + 1500,
        npi="1000000000",
    ) == [{"resource_id": "0"}]
    assert reconstruct.await_args.args == (driver, tables, "dataset", keys[:1])
    key_query = driver.fetch.await_args.args[0]
    assert "npi_text=$6" in key_query and "payload_json" not in key_query
    assert driver.fetch.await_args.args[-1] == "1000000000"


async def test_first_oversize_key_refuses_without_reconstruction(monkeypatch):
    driver = connection()
    driver.fetch.return_value = [{"read_bytes": 1000}]
    reconstruct = AsyncMock()
    monkeypatch.setattr(compact, "read_compact_payload_keys", reconstruct)
    tables = compact.CompactPayloadTables("published", "aliases", "sets", "blocks")
    with pytest.raises(candidate.GenericCandidateError, match="row_too_large"):
        await edition.compact_edition_page(
            driver, tables, "dataset", resource_types=("PractitionerRole",), page_bytes=10
        )
    reconstruct.assert_not_awaited()


async def test_empty_page_is_the_only_successful_empty_result(monkeypatch):
    driver = connection()
    driver.fetch.return_value = []
    reconstruct = AsyncMock()
    monkeypatch.setattr(compact, "read_compact_payload_keys", reconstruct)
    tables = compact.CompactPayloadTables("published", "aliases", "sets", "blocks")
    assert (
        await edition.compact_edition_page(
            driver, tables, "dataset", resource_types=("PractitionerRole",), page_bytes=4096
        )
        == []
    )
    reconstruct.assert_not_awaited()


async def test_bad_read_context_refuses_before_query():
    driver = connection()
    driver.is_in_transaction = lambda: False
    tables = compact.CompactPayloadTables("published", "aliases", "sets", "blocks")
    with pytest.raises(candidate.GenericCandidateError, match="transaction_required"):
        await edition.compact_edition_page(
            driver, tables, "dataset", resource_types=("PractitionerRole",), page_bytes=4096
        )
    driver.fetch.assert_not_awaited()
