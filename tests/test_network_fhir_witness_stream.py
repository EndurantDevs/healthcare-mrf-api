# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded high-fanout transfer preserves complete v2 facts and rollback semantics."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_membership_source as fhir
from process import network_fhir_witness_stream as stream
from process import network_provider_only_projection as projection
from process import registry_source_contribution_store as facts
from process import registry_verified_witness_stage as stage
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_verified_office_source_witnesses_v2 import _edge, _header


class Connection:
    def __init__(self, documents, *, fail_after=None):
        self.documents, self.fail_after = documents, fail_after
        self.records, self.chunks, self.transitions = [], [], []
        self.exhausted = False

    def is_in_transaction(self):
        return True

    @asynccontextmanager
    async def transaction(self):
        before = len(self.records)
        self.transitions.append("enter")
        try:
            yield
        except BaseException:
            del self.records[before:]
            self.transitions.append("rollback")
            raise
        else:
            self.transitions.append("release")

    async def cursor(self, query, *arguments, prefetch):
        self.query, self.arguments, self.prefetch = query, arguments, prefetch
        try:
            for index, document in enumerate(self.documents):
                if self.fail_after == index:
                    raise RuntimeError("native cursor failed")
                yield {
                    "fact": document if document is None or isinstance(document, str) else json.dumps(document),
                    "resource_type": "PractitionerRole",
                    "resource_id": "role-one",
                }
            self.exhausted = True
        finally:
            self.transitions.append("cursor_exit")

    async def copy_records_to_table(self, table, **options):
        records = options["records"]
        self.chunks.append(records)
        self.records.extend(records)
        return f"COPY {len(records)}"


def arguments(source_pin):
    return dict(
        registry_schema="synthetic_registry",
        after_resource=None,
        approved_source=object(),
        binding_coordinates=object(),
        retained_read=object(),
        copy_position=("synthetic_facts", "a" * 64, 17),
    )


@pytest.fixture
def source_pin(monkeypatch):
    source_pin = pin(retained())
    monkeypatch.setattr(fhir, "_reviewed_descriptor", lambda *args: object())
    monkeypatch.setattr(fhir, "_require_source_recipe", AsyncMock())
    return source_pin


@pytest.mark.asyncio
@pytest.mark.parametrize("edges", [1, 5001, 9000])
async def test_complete_ordered_facts_header_once_bounded_copy_and_post_eof_authority(source_pin, monkeypatch, edges):
    documents = [_header(source_pin), *[_edge(source_pin) for _ in range(edges)]]
    connection = Connection(documents)
    checks = []

    async def authorize(*args):
        checks.append(connection.exhausted)

    monkeypatch.setattr(fhir, "_require_source_recipe", authorize)
    cursor, ordinal = await stream.copy_fhir_witness_resource_v2(connection, source_pin, **arguments(source_pin))
    assert cursor == ("PractitionerRole", "role-one") and ordinal == 18 + edges
    assert [json.loads(row[2]) for row in connection.records] == documents
    assert [row[1] for row in connection.records] == list(range(18, 19 + edges))
    assert sum(json.loads(row[2])["kind"] == "resource" for row in connection.records) == 1
    assert all(
        len(chunk) <= 5000 and sum(len(row[2].encode()) + 1 for row in chunk) + 2 <= 8388608
        for chunk in connection.chunks
    )
    assert checks == [False, True] and connection.prefetch == 32
    assert connection.arguments[3] == 1 and connection.arguments[-1] == source_pin.retained_capture.capture_sha256
    assert connection.transitions[-1] == "release"


@pytest.mark.asyncio
async def test_byte_bound_flushes_before_row_limit(source_pin, monkeypatch):
    monkeypatch.setattr(fhir, "MAX_INPUT_BYTES", 4096)
    connection = Connection([_header(source_pin), *[_edge(source_pin) for _ in range(12)]])
    await stream.copy_fhir_witness_resource_v2(connection, source_pin, **arguments(source_pin))
    assert len(connection.chunks) > 1
    assert all(sum(len(row[2].encode()) + 1 for row in chunk) + 2 <= 4096 for chunk in connection.chunks)
    assert len(connection.records) == 13


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "scope",
        "payload",
        "second_header",
        "missing_header",
        "empty",
        "oversized",
        "null",
        "cursor",
        "post_admission",
        "copy",
    ],
)
async def test_invalid_or_late_failure_rolls_back_all_chunks(source_pin, monkeypatch, damage):
    documents = [_header(source_pin), *[_edge(source_pin) for _ in range(5001)]]
    options = arguments(source_pin)
    if damage == "scope":
        documents[-1]["source_scope_json"]["capture_sha256"] = "f" * 64
    if damage == "payload":
        documents[-1]["payload_sha256"] = "c" * 64
    if damage == "second_header":
        documents[-1] = _header(source_pin)
    if damage == "missing_header":
        documents = documents[1:]
    if damage == "empty":
        documents = []
    if damage == "oversized":
        documents[-1] = "x" * (8388608 + 1)
    if damage == "null":
        documents[-1] = None
    if damage == "cursor":
        options["after_resource"] = ("PractitionerRole", "role-one")
    connection = Connection(documents)
    if damage == "post_admission":
        monkeypatch.setattr(
            fhir, "_require_source_recipe", AsyncMock(side_effect=[None, ValueError("admission changed")])
        )
    if damage == "copy":
        connection.copy_records_to_table = AsyncMock(return_value="COPY 0")
    with pytest.raises((ValueError, RuntimeError)):
        await stream.copy_fhir_witness_resource_v2(connection, source_pin, **options)
    assert connection.records == [] and connection.transitions[-1] == "rollback"


@pytest.mark.asyncio
async def test_cursor_failure_after_partial_copy_is_terminal(source_pin):
    connection = Connection([_header(source_pin), *[_edge(source_pin) for _ in range(6000)]], fail_after=5500)
    with pytest.raises(RuntimeError, match="native cursor failed"):
        await stream.copy_fhir_witness_resource_v2(connection, source_pin, **arguments(source_pin))
    assert connection.chunks and connection.records == [] and connection.transitions[-1] == "rollback"


@pytest.mark.asyncio
async def test_cancellation_drains_copy_then_rolls_back(source_pin):
    connection = Connection([_header(source_pin), *[_edge(source_pin) for _ in range(5001)]])
    started, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = connection.copy_records_to_table

    async def copy_rows(*args, **kwargs):
        started.set()
        await release.wait()
        result = await original(*args, **kwargs)
        completed.set()
        return result

    connection.copy_records_to_table = copy_rows
    operation = asyncio.create_task(
        stream.copy_fhir_witness_resource_v2(connection, source_pin, **arguments(source_pin))
    )
    try:
        await asyncio.wait_for(started.wait(), 5)
        operation.cancel()
        await asyncio.sleep(0)
        assert not completed.is_set() and not operation.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert completed.is_set() and not connection.records and connection.transitions[-1] == "rollback"
    finally:
        release.set()
        if not operation.done():
            operation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await operation


@pytest.mark.parametrize("mode", ["legacy", "retained", "source_witness", "plan_tier"])
def test_stream_sql_changes_only_transfer_and_expansion_bound(mode):
    from tests.test_network_fhir_source_witness_recipe import _source_recipe
    from tests.test_registry_fhir_plan_tier_bindings import witness_pin
    from tests.test_registry_generic_fhir_facts import _legacy_pin

    source_pin = {
        "legacy": _legacy_pin,
        "retained": lambda: pin(retained()),
        "source_witness": lambda: _source_recipe().source_pin,
        "plan_tier": witness_pin,
    }[mode]()
    ordinary = fhir._source_witness_sql_v2(source_pin)
    query = fhir._source_witness_stream_sql_v2(source_pin)
    assert query.split("      ) SELECT resource_type", 1)[0] == ordinary.split("      ), transfer AS (\n", 1)[
        0
    ].replace("        LIMIT 5001\n", "", 1)
    assert "LIMIT $4" in query and "8388608" in query and "5001" not in query
    assert "fact->>'kind' DESC,fact::text" in query


@pytest.mark.parametrize("anchor", ["        LIMIT 5001\n", "      ), transfer AS (\n"])
@pytest.mark.parametrize("copies", [0, 2])
def test_stream_sql_rejects_missing_or_duplicate_required_anchor(source_pin, monkeypatch, anchor, copies):
    ordinary = fhir._source_witness_sql_v2(source_pin)
    monkeypatch.setattr(fhir, "_source_witness_sql_v2", lambda pin: ordinary.replace(anchor, anchor * copies))
    with pytest.raises(fhir.FHIRMembershipSourceError, match="stream SQL anchors"):
        fhir._source_witness_stream_sql_v2(source_pin)


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
async def test_both_complete_extractors_fallback_only_at_one_then_require_terminal_page(
    source_pin, monkeypatch, independent
):
    from tests.test_registry_generic_fhir_facts import _recipe

    module = projection if independent else stage
    recipe = _recipe(source_pin)
    terminal = fhir.FHIRSourceWitnessPageV2(source_pin, 0, None, b"[]")
    limits = []

    async def reader(*args, **kwargs):
        limits.append(kwargs["limit"])
        if kwargs["after_resource"] is None:
            raise fhir.FHIRMembershipBatchBoundsError("bounded")
        return terminal

    fallback = AsyncMock(return_value=(("PractitionerRole", "role-one"), 5002))
    monkeypatch.setattr(module, "read_fhir_source_witness_page_v2", reader)
    monkeypatch.setattr(module, "copy_fhir_witness_resource_v2", fallback)
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    if independent:
        monkeypatch.setattr(module, "_require_recipe_custody", AsyncMock(return_value="read"))
        monkeypatch.setattr(module, "_source_witness_census_v2", AsyncMock(return_value=1))
        await module._rebuild_source_witnesses_v2(connection, recipe, "facts", None, "controls", None)
    else:
        assert await module._capture_witness_facts(connection, recipe, None, "controls", "facts", "read") == (5002, 1)
    assert limits == [1000, 500, 250, 125, 62, 31, 15, 7, 3, 1, 1]
    fallback.assert_awaited_once()
    assert fallback.await_args.kwargs["retained_read"] == "read"


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
@pytest.mark.parametrize("outliers", [1, 3])
async def test_mixed_outliers_then_normal_suffix_recovers_batches_without_repeated_full_shrink(
    source_pin, monkeypatch, independent, outliers
):
    from tests.test_registry_generic_fhir_facts import _recipe

    module = projection if independent else stage
    recipe = _recipe(source_pin)
    normal_rows = 4096
    count = outliers + normal_rows
    calls, successes, copied = [], [], []
    header = _header(source_pin, omission_reason="inactive")

    async def reader(*args, **kwargs):
        after = _resource_index(kwargs["after_resource"])
        limit = kwargs["limit"]
        calls.append((after, limit))
        if after + 1 < outliers:
            raise fhir.FHIRMembershipBatchBoundsError("bounded")
        selected_indices = list(range(after + 1, min(count, after + 1 + limit)))
        successes.append((after, limit, len(selected_indices)))
        documents = [{**header, "resource_id": _resource_position(item)[1]} for item in selected_indices]
        return fhir.FHIRSourceWitnessPageV2(
            source_pin,
            len(selected_indices),
            _resource_position(selected_indices[-1]) if selected_indices else None,
            json.dumps(documents).encode(),
        )

    async def fallback(*args, **kwargs):
        after = _resource_index(kwargs["after_resource"])
        copied.append(after + 1)
        return _resource_position(after + 1), kwargs["copy_position"][2] + 5002

    async def copy_rows(*args, **kwargs):
        return f"COPY {len(kwargs['records'])}"

    monkeypatch.setattr(module, "read_fhir_source_witness_page_v2", reader)
    monkeypatch.setattr(module, "copy_fhir_witness_resource_v2", fallback)
    connection = SimpleNamespace(is_in_transaction=lambda: True, copy_records_to_table=copy_rows)
    counts = await _extract_complete(module, connection, recipe, monkeypatch, independent, count)
    assert independent or counts == (outliers * 5002 + normal_rows, count)
    assert copied == list(range(outliers))
    assert [limit for after, limit in calls if after == -1] == [1000, 500, 250, 125, 62, 31, 15, 7, 3, 1]
    assert all(limit == 1 for after, limit in calls if 0 <= after < outliers - 1)
    assert [limit for _, limit, page_rows in successes if page_rows][:11] == [
        1,
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1000,
    ]
    assert sum(page_rows for _, _, page_rows in successes) == normal_rows
    assert successes[-1][1:] == (1000, 0) and len(successes) <= 16


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
async def test_ordinary_bounded_pages_regrow_after_success_without_streaming(source_pin, monkeypatch, independent):
    from tests.test_registry_generic_fhir_facts import _recipe

    module = projection if independent else stage
    recipe = _recipe(source_pin)
    limits = []
    header = _header(source_pin, omission_reason="inactive")

    async def reader(*args, **kwargs):
        limits.append(kwargs["limit"])
        if len(limits) == 1:
            raise fhir.FHIRMembershipBatchBoundsError("bounded")
        if len(limits) > 3:
            return fhir.FHIRSourceWitnessPageV2(source_pin, 0, None, b"[]")
        document_by_field = {**header, "resource_id": f"role-{len(limits)}"}
        return fhir.FHIRSourceWitnessPageV2(
            source_pin,
            1,
            ("PractitionerRole", document_by_field["resource_id"]),
            json.dumps([document_by_field]).encode(),
        )

    fallback = AsyncMock(side_effect=AssertionError("ordinary page selected the stream"))
    monkeypatch.setattr(module, "read_fhir_source_witness_page_v2", reader)
    monkeypatch.setattr(module, "copy_fhir_witness_resource_v2", fallback)
    connection = SimpleNamespace(is_in_transaction=lambda: True, copy_records_to_table=AsyncMock(return_value="COPY 1"))
    if independent:
        monkeypatch.setattr(module, "_require_recipe_custody", AsyncMock(return_value="read"))
        monkeypatch.setattr(module, "_source_witness_census_v2", AsyncMock(return_value=2))
        await module._rebuild_source_witnesses_v2(connection, recipe, "facts", None, "controls", None)
    else:
        assert await module._capture_witness_facts(connection, recipe, None, "controls", "facts", "read") == (2, 2)
    assert limits == [1000, 500, 1000, 1000]
    fallback.assert_not_awaited()


def _resource_position(index):
    return ("PractitionerRole", f"role-{index:06d}")


def _resource_index(cursor):
    return int(cursor[1].split("-")[1]) if cursor is not None else -1


async def _extract_complete(module, connection, recipe, monkeypatch, independent, count):
    if independent:
        monkeypatch.setattr(module, "_require_recipe_custody", AsyncMock(return_value="read"))
        monkeypatch.setattr(module, "_source_witness_census_v2", AsyncMock(return_value=count))
        await module._rebuild_source_witnesses_v2(connection, recipe, "facts", None, "controls", None)
        return None
    return await module._capture_witness_facts(connection, recipe, None, "controls", "facts", "read")
