# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

import api.ptg2_db_sidecars as db_sidecars
from api.ptg2_db_serving_v3_pages import (
    PTG2_SERVING_BINARY_V3_BY_CODE_PAGE_KIND,
    PTG2_SERVING_BINARY_V3_PAGE_FORMAT_VERSION,
    PTG2_SERVING_BINARY_V3_PAGE_ROWS,
    PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN,
    PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND,
    has_provider_pages_in_db,
    lookup_code_page_from_db,
    lookup_provider_pages_from_db,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_serving_binary_v3 import append_uvarint


@pytest.fixture(autouse=True)
def clear_binary_block_cache():
    db_sidecars._BINARY_BLOCK_CACHE.clear()
    db_sidecars._BINARY_BLOCK_CACHE_STATE["byte_count"] = 0


class FakeResult:
    def __init__(self, rows):
        self.rows = list(rows)

    def __iter__(self):
        return iter(self.rows)


class ScalarResult:
    def __init__(self, scalar_value):
        self.scalar_value = scalar_value

    def scalar(self):
        return self.scalar_value


class ScalarSession:
    def __init__(self, scalar_value):
        self.scalar_value = scalar_value
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        return ScalarResult(self.scalar_value)


class FakeSession:
    def __init__(self, rows_by_kind_and_key):
        self.rows_by_kind_and_key = rows_by_kind_and_key
        self.calls = []

    async def execute(self, _statement, params):
        self.calls.append(dict(params))
        rows = []
        for block_key in params["block_keys"]:
            rows.extend(
                {**row, "block_key": block_key}
                for row in self.rows_by_kind_and_key.get(
                    (params["artifact_kind"], block_key),
                    (),
                )
            )
        return FakeResult(rows)


def block_row(payload, entry_count):
    return {
        "block_no": 0,
        "entry_count": entry_count,
        "payload": bytes(payload),
        "payload_compression": "none",
        "raw_payload_bytes": 0,
    }


def code_page_payload(rows):
    payload = bytearray((PTG2_SERVING_BINARY_V3_PAGE_FORMAT_VERSION,))
    append_uvarint(payload, len(rows))
    for provider_set_key, provider_count, price_key in rows:
        append_uvarint(payload, provider_set_key)
        append_uvarint(payload, provider_count)
        append_uvarint(payload, price_key)
    return payload


def provider_page_payload(block_key, provider_rows):
    payload = bytearray((PTG2_SERVING_BINARY_V3_PAGE_FORMAT_VERSION,))
    append_uvarint(payload, len(provider_rows))
    block_start = block_key * PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN
    for provider_set_key, provider_count, total_row_count, rows in provider_rows:
        append_uvarint(payload, provider_set_key - block_start)
        append_uvarint(payload, provider_count)
        append_uvarint(payload, total_row_count)
        append_uvarint(payload, len(rows))
        previous_code_key = 0
        for code_key, price_key in rows:
            append_uvarint(payload, code_key - previous_code_key)
            append_uvarint(payload, price_key)
            previous_code_key = code_key
    return payload


@pytest.mark.asyncio
async def test_code_page_reader_preserves_ranked_rows():
    payload = code_page_payload(((4, 10, 7), (3, 5, 8)))
    session = FakeSession(
        {(PTG2_SERVING_BINARY_V3_BY_CODE_PAGE_KIND, 9): [block_row(payload, 2)]}
    )

    records = await lookup_code_page_from_db(session, "mrf.ptg2_v3", 9)

    assert [(row.provider_set_key, row.provider_count, row.price_key) for row in records] == [
        (4, 10, 7),
        (3, 5, 8),
    ]


@pytest.mark.asyncio
async def test_provider_page_reader_returns_requested_rows_and_total():
    provider_set_key = 4
    block_key = provider_set_key
    projected_rows = tuple(
        (code_key, code_key + 4)
        for code_key in range(3, 3 + PTG2_SERVING_BINARY_V3_PAGE_ROWS)
    )
    payload = provider_page_payload(
        block_key,
        (
            (provider_set_key, 12, 100, projected_rows),
        ),
    )
    session = FakeSession(
        {(PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND, block_key): [block_row(payload, 1)]}
    )

    pages = await lookup_provider_pages_from_db(
        session,
        "mrf.ptg2_v3",
        (provider_set_key,),
    )

    assert set(pages) == {provider_set_key}
    assert pages[provider_set_key].total_row_count == 100
    assert len(pages[provider_set_key].entries) == PTG2_SERVING_BINARY_V3_PAGE_ROWS
    assert pages[provider_set_key].entries[0].code_key == 3
    assert pages[provider_set_key].entries[-1].code_key == 66


@pytest.mark.asyncio
async def test_page_readers_return_none_when_projection_is_absent():
    session = FakeSession({})

    assert await lookup_code_page_from_db(session, "mrf.ptg2_v3", 3) is None
    assert await lookup_provider_pages_from_db(session, "mrf.ptg2_v3", (4,)) is None


@pytest.mark.asyncio
async def test_provider_page_availability_uses_indexable_artifact_probe():
    session = ScalarSession(True)

    assert await has_provider_pages_in_db(session, "mrf.ptg2_v3")
    statement, params = session.calls[0]
    assert "artifact_kind =" in statement
    assert "LIMIT 1" in statement
    assert params["artifact_kind"] == PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND


@pytest.mark.asyncio
async def test_code_page_reader_rejects_out_of_order_rows():
    payload = code_page_payload(((4, 5, 7), (3, 10, 8)))
    session = FakeSession(
        {(PTG2_SERVING_BINARY_V3_BY_CODE_PAGE_KIND, 9): [block_row(payload, 2)]}
    )

    with pytest.raises(PTG2ManifestArtifactError, match="corrupt"):
        await lookup_code_page_from_db(session, "mrf.ptg2_v3", 9)


@pytest.mark.asyncio
async def test_provider_page_reader_rejects_underfilled_projection():
    provider_set_key = 4
    payload = provider_page_payload(
        provider_set_key,
        ((provider_set_key, 12, 100, ((3, 7),)),),
    )
    session = FakeSession(
        {
            (PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND, provider_set_key): [
                block_row(payload, 1)
            ]
        }
    )

    with pytest.raises(PTG2ManifestArtifactError, match="corrupt"):
        await lookup_provider_pages_from_db(session, "mrf.ptg2_v3", (provider_set_key,))
