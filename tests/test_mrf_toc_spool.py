# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt
import gzip
import json
import sys

import pytest

from process.mrf_toc_spool import DecodedTocBudget, IncompleteToc, open_toc_spool, open_toc_store
from process.ptg_parts.source_jobs import parse_toc_catalog_entries

URL = "https://catalog.example.test/index.json"


class _Reader:
    def __init__(self, body: bytes, chunk_size: int = 97):
        self.body = body
        self.offset = 0
        self.chunk_size = chunk_size

    async def read(self, size: int) -> bytes:
        assert 0 < size <= 64 * 1024
        end = self.offset + min(size, self.chunk_size)
        result = self.body[self.offset : end]
        self.offset = end
        await asyncio.sleep(0)
        return result


def _body(value):
    return json.dumps(value, ensure_ascii=False).encode()


def _structure(plans, files, **other):
    return {"reporting_plans": plans, "in_network_files": files, **other}


def _file(name, description=None):
    return {"location": f"https://files.example.test/{name}.json", "description": description}


async def _entries(toc_dict, tmp_path, predicate=None):
    async with open_toc_spool(
        _Reader(_body(toc_dict)),
        URL,
        max_bytes=2_000_000,
        row_bytes=500_000,
        plan_predicate=predicate,
        spool_bytes=64 * 1024 * 1024,
        temporary_parent=tmp_path,
    ) as spool:
        return [entry async for entry in spool.catalog_entries()]


@pytest.mark.asyncio
async def test_duplicate_files_keep_global_plan_union_first_order_and_last_fields(tmp_path):
    first_plan_dict = {"plan_id": "first", "plan_sponser_name": "Example Sponsor", "custom": {"limit": 1.25}}
    second_plan_dict = {"plan_id": "second", "plan_name": "Second"}
    toc_dict = {
        "reporting_structure": [
            _structure([first_plan_dict, first_plan_dict], [_file("shared", "old"), _file("other")]),
            _structure(
                [second_plan_dict, first_plan_dict],
                [_file("shared", "new")],
                allowed_amount_files=[_file("allowed", "allowed amount")],
                drug_files=[_file("drug", "pharmacy")],
            ),
        ],
        "reporting_entity_name": "Example Payer",
        "reporting_entity_type": "health insurance issuer",
        "version": "1.0",
    }
    assert await _entries(toc_dict, tmp_path) == parse_toc_catalog_entries(toc_dict, URL)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_plan_filter_does_not_take_fields_from_excluded_last_structure(tmp_path):
    toc_dict = {
        "reporting_structure": [
            _structure([{"plan_id": "wanted"}], [_file("shared", "matching")]),
            _structure([{"plan_id": "excluded"}], [_file("shared", "excluded")]),
        ]
    }
    predicate = lambda plan: plan["plan_id"] == "wanted"
    assert await _entries(toc_dict, tmp_path, predicate) == parse_toc_catalog_entries(
        toc_dict,
        URL,
        plan_predicate=predicate,
    )


@pytest.mark.asyncio
async def test_one_large_structure_keeps_every_file_without_structure_materialization(tmp_path):
    plans = [{"plan_id": str(index)} for index in range(80)]
    toc_dict = {"reporting_structure": [_structure(plans, [_file(str(index)) for index in range(240)])]}
    async with open_toc_spool(
        _Reader(_body(toc_dict), 13),
        URL,
        max_bytes=200_000,
        row_bytes=80_000,
        spool_bytes=64 * 1024 * 1024,
        temporary_parent=tmp_path,
    ) as spool:
        count = 0
        async for entry in spool.catalog_entries():
            if entry.source_type != "table-of-contents":
                count += 1
                assert list(entry.plan_info) == plans
        assert count == 240
        assert spool.connection.execute("SELECT count(*) FROM plans").fetchone()[0] == 0
        assert spool.connection.execute("SELECT count(*) FROM group_plans").fetchone()[0] == 80
        assert spool.connection.execute("SELECT count(*) FROM entry_groups").fetchone()[0] == 240
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_many_plan_group_references_cannot_bypass_row_budget(tmp_path):
    document_dict = {
        "reporting_structure": [_structure([{"plan_id": str(index)}], [_file("shared")]) for index in range(700)]
    }
    with pytest.raises(IncompleteToc, match="group references"):
        async with open_toc_spool(
            _Reader(_body(document_dict)),
            URL,
            max_bytes=200_000,
            row_bytes=3000,
            spool_bytes=32 * 1024**2,
            temporary_parent=tmp_path,
        ) as spool:
            unexpected_entries = [entry async for entry in spool.catalog_entries()]
            assert unexpected_entries
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_small_encoded_group_set_stops_at_heap_budget(monkeypatch, tmp_path):
    group_count, row_budget = 80, 3000
    references = [f"s:{index}" for index in range(group_count)]
    assert len(_body(references)) < row_budget
    assert sys.getsizeof(references) + sum(sys.getsizeof(group) for group in references) > row_budget
    document_dict = {"reporting_structure": [_structure([{"plan_id": "same"}], [_file("shared")]) for _ in references]}
    consumed_groups = []
    with pytest.raises(IncompleteToc, match="group references"):
        async with open_toc_spool(
            _Reader(_body(document_dict)),
            URL,
            max_bytes=200_000,
            row_bytes=row_budget,
            spool_bytes=32 * 1024**2,
            temporary_parent=tmp_path,
        ) as spool:
            original = spool._ordered

            def ordered(query, parameters=()):
                for row in original(query, parameters):
                    if query.startswith("SELECT group_id FROM entry_groups"):
                        consumed_groups.append(row[0])
                    yield row

            monkeypatch.setattr(spool, "_ordered", ordered)
            unexpected_entries = [entry async for entry in spool.catalog_entries()]
            assert unexpected_entries
    assert 0 < len(consumed_groups) < group_count
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_large_healthsparq_plan_array_cannot_bypass_row_budget(tmp_path):
    document_dict = {
        "files": [{"filePath": "rates.json", "reportingPlans": [{"planId": str(index)} for index in range(100)]}]
    }
    with pytest.raises(IncompleteToc, match="file plan metadata"):
        async with open_toc_spool(
            _Reader(_body(document_dict)),
            URL,
            max_bytes=20_000,
            row_bytes=3000,
            spool_bytes=32 * 1024**2,
            temporary_parent=tmp_path,
        ) as spool:
            unexpected_files = [entry async for entry in spool.healthsparq_files()]
            assert unexpected_files
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_large_integer_scalar_is_bounded_by_encoded_bytes(tmp_path):
    document_dict = {"reporting_structure": [_structure([{"plan_id": int("9" * 1200)}], [_file("shared")])]}
    with pytest.raises(IncompleteToc, match="object exceeds"):
        async with open_toc_spool(
            _Reader(_body(document_dict)),
            URL,
            max_bytes=20_000,
            row_bytes=1100,
            spool_bytes=32 * 1024**2,
            temporary_parent=tmp_path,
        ):
            pytest.fail("oversized integer scalar was admitted")
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size,batch_bytes", [(0, 2000), (1, 0), (1, 50)])
async def test_invalid_write_budget_never_reaches_sink(tmp_path, batch_size, batch_bytes):
    writes = []

    async def sink(plans, files):
        writes.append((plans, files))

    with pytest.raises(IncompleteToc, match="budget"):
        async with open_toc_spool(
            _Reader(b"{}"),
            URL,
            max_bytes=1000,
            row_bytes=2000,
            spool_bytes=32 * 1024**2,
            temporary_parent=tmp_path,
        ) as spool:
            spool.add_rows([{"mrf_plan_id": "plan"}], [])
            await spool.drain_rows(sink, batch_size=batch_size, batch_bytes=batch_bytes)
    assert not writes and not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_empty_spool_drain_emits_no_empty_batch(tmp_path):
    writes = []

    async def sink(plans, files):
        writes.append((plans, files))

    async with open_toc_spool(
        _Reader(b"{}"),
        URL,
        max_bytes=1000,
        row_bytes=2000,
        spool_bytes=32 * 1024**2,
        temporary_parent=tmp_path,
    ) as spool:
        assert await spool.drain_rows(sink, batch_size=1, batch_bytes=2000) == (0, 0)
    assert not writes and not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_normalized_row_budget_cannot_be_bypassed_by_spilling(tmp_path):
    with pytest.raises(IncompleteToc, match="normalized TOC row"):
        async with open_toc_spool(
            _Reader(b"{}"),
            URL,
            max_bytes=1000,
            row_bytes=2000,
            spool_bytes=32 * 1024**2,
            temporary_parent=tmp_path,
        ) as spool:
            spool.add_rows([{"mrf_plan_id": "plan", "value": "x" * 3000}], [])
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("shared_plan_set", [False, True])
async def test_decoded_row_budget_is_checked_before_any_sink_call(tmp_path, shared_plan_set):
    from process.mrf_toc_spool import _heap_size

    plan_labels = ["value" * 52] * 100
    writes = []
    budget = 80_000

    async def sink(plans, files):
        writes.append((plans, files))

    with pytest.raises(IncompleteToc, match="^normalized TOC row exceeds the row budget$"):
        async with open_toc_spool(
            _Reader(_body({"reporting_structure": [_structure([{"plan_id": "p"}], [_file("a")])]})),
            URL,
            max_bytes=10_000,
            row_bytes=budget,
            spool_bytes=32 * 1024**2,
            temporary_parent=tmp_path,
        ) as spool:
            entry = await anext(spool.catalog_entries())
            plans = spool.normalized_entry_plans(entry, lambda raw: raw)
            file_row_dict = {"mrf_file_id": "oversized", "plan_ids": plan_labels, "plan_names": plan_labels}
            if shared_plan_set:
                file_row_dict.update(market_types=[], metadata_json={"plan_info": plans})
            encoded = json.dumps(file_row_dict, ensure_ascii=False, separators=(",", ":")).encode()
            assert len(encoded) + _heap_size(file_row_dict) + 8 < budget
            assert len(encoded) + _heap_size(json.loads(encoded)) + 8 > budget
            spool.add_rows([], [{"mrf_file_id": "first"}, file_row_dict])
            await spool.drain_rows(sink, batch_size=1, batch_bytes=budget)
    assert not writes and not list(tmp_path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"In-network negotiated rate files": [_file("first"), {"url": "https://files.example.test/second.json"}]},
        {"items": [{"filePath": "rates.json", "fileSchema": "in_network_rates", "reportingPlans": [{"planId": "1"}]}]},
        {
            "data": {
                "files": [
                    {"filePath": "rates.json", "fileSchema": "allowed_amounts", "reportingPlans": [{"planId": "2"}]}
                ]
            }
        },
        {"data": [{"filePath": "drug.json", "fileSchema": "prescription_drug", "reportingPlans": [{"planId": "3"}]}]},
        {
            "items": [],
            "data": {
                "files": [
                    {"filePath": "rates.json", "fileSchema": "in_network_rates", "reportingPlans": [{"planId": "4"}]}
                ]
            },
        },
        {"provider_urls": [], "plan_urls": []},
    ],
)
async def test_flat_nested_and_directory_layouts_match_existing_parser(tmp_path, payload):
    payload = payload | {"reporting_entity_name": "Example Payer"}
    assert await _entries(payload, tmp_path) == parse_toc_catalog_entries(payload, URL)


@pytest.mark.asyncio
async def test_root_healthsparq_files_keep_original_adapter_payloads(tmp_path):
    files = [
        {
            "filePath": "rates.json",
            "fileSchema": "in_network_rates",
            "fileName": "Example",
            "reportingPlans": [{"planId": "a"}, {"planId": "a"}],
            "plans": [{"planId": "ignored"}],
        },
        {"filePath": "other.json", "reportingPlans": [], "plans": [{"planId": "ignored"}]},
        {"filePath": "last.json", "reportingPlans": None, "plans": [{"planId": "used"}]},
    ]
    async with open_toc_spool(
        _Reader(_body({"files": files})),
        URL,
        max_bytes=20_000,
        row_bytes=20_000,
        spool_bytes=64 * 1024 * 1024,
        temporary_parent=tmp_path,
    ) as spool:
        assert spool.is_healthsparq
        fragments = [file async for file in spool.healthsparq_files()]
        from process import mrf_source_discovery as discovery

        source_dict = {"source_id": "source_example", "payer_id": "payer_example"}
        original_rows = discovery._healthsparq_rows_from_metadata(source_dict, URL, {"files": files})
        fragment_rows = discovery._healthsparq_rows_from_metadata(source_dict, URL, {"files": fragments})
        for converted_rows in (original_rows, fragment_rows):
            for normalized_row in converted_rows[0] + converted_rows[1]:
                normalized_row.pop("first_seen_at")
                normalized_row.pop("last_seen_at")
        assert fragment_rows == original_rows
        with pytest.raises(ValueError, match="HealthSparq"):
            unexpected_entries = [entry async for entry in spool.catalog_entries()]
            assert unexpected_entries
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("compressed", [False, True])
async def test_narrow_adjacent_object_repair_across_chunks(tmp_path, compressed):
    toc_dict = {
        "reporting_structure": [
            _structure(
                [{"plan_id": "one", "label": 'literal } { and \\"quote"'}, {"plan_id": "two"}],
                [_file("shared")],
            )
        ]
    }
    damaged = _body(toc_dict).replace(b'}, {"plan_id": "two"}', b'} {"plan_id": "two"}')
    assert damaged != _body(toc_dict)
    async with open_toc_spool(
        _Reader(gzip.compress(damaged) if compressed else damaged, 1),
        URL,
        max_bytes=10_000,
        row_bytes=10_000,
        spool_bytes=64 * 1024 * 1024,
        temporary_parent=tmp_path,
    ) as spool:
        assert [entry async for entry in spool.catalog_entries()] == parse_toc_catalog_entries(toc_dict, URL)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body,limit",
    [
        (gzip.compress(_body({"padding": "x" * 10_000})), 512),
        (gzip.compress(_body({"reporting_structure": []}))[:-4], 1000),
        (b'{"reporting_structure":[', 1000),
        (b'{"something":[{} {}]}', 1000),
        (_body({"reporting_structure": []}) + b"trailing", 1000),
        (b"[]", 1000),
    ],
)
async def test_expansion_truncation_and_invalid_streams_fail_before_admission(tmp_path, body, limit):
    has_entered = False
    with pytest.raises(IncompleteToc):
        async with open_toc_spool(
            _Reader(body, 3),
            URL,
            max_bytes=limit,
            row_bytes=10_000,
            spool_bytes=64 * 1024 * 1024,
            temporary_parent=tmp_path,
        ):
            has_entered = True
    assert not has_entered
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_merged_single_file_over_budget_is_not_truncated(tmp_path):
    toc_dict = {
        "reporting_structure": [
            _structure([{"plan_id": str(index), "label": "x" * 100}], [_file("shared")]) for index in range(30)
        ]
    }
    with pytest.raises(IncompleteToc, match="merged TOC"):
        async with open_toc_spool(
            _Reader(_body(toc_dict)),
            URL,
            max_bytes=50_000,
            row_bytes=2_000,
            spool_bytes=64 * 1024 * 1024,
            temporary_parent=tmp_path,
        ) as spool:
            unexpected_entries = [entry async for entry in spool.catalog_entries()]
            assert unexpected_entries
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_row_dedup_dates_and_awaited_byte_and_count_batches(tmp_path):
    moment = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    writes = []
    active_counts = [0]

    async def sink(plans, files):
        active_counts[0] += 1
        assert active_counts[0] == 1
        await asyncio.sleep(0)
        assert len(plans) + len(files) <= 2
        assert all(isinstance(row["first_seen_at"], dt.datetime) for row in plans + files)
        writes.extend(("plan", row) for row in plans)
        writes.extend(("file", row) for row in files)
        active_counts[0] -= 1

    async with open_toc_spool(
        _Reader(b"{}"),
        URL,
        max_bytes=1000,
        row_bytes=2000,
        spool_bytes=64 * 1024 * 1024,
        temporary_parent=tmp_path,
    ) as spool:
        spool.add_rows([{"mrf_plan_id": "p", "value": "old", "first_seen_at": moment}], [])
        spool.add_rows(
            [{"mrf_plan_id": "p", "value": "new", "first_seen_at": moment}],
            [{"mrf_file_id": str(index), "first_seen_at": moment} for index in range(5)],
        )
        assert await spool.drain_rows(sink, batch_size=2, batch_bytes=1200) == (1, 5)
    assert writes[0][1]["value"] == "new"
    assert len(writes) == 6
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_sink_failure_or_cancellation_cleans_spool(tmp_path, cancel):
    async def sink(_plans, _files):
        await asyncio.sleep(0)
        if cancel:
            raise asyncio.CancelledError
        raise RuntimeError("sink unavailable")

    with pytest.raises(asyncio.CancelledError if cancel else IncompleteToc):
        async with open_toc_spool(
            _Reader(b"{}"),
            URL,
            max_bytes=1000,
            row_bytes=2000,
            spool_bytes=64 * 1024 * 1024,
            temporary_parent=tmp_path,
        ) as spool:
            spool.add_rows([], [{"mrf_file_id": "f"}])
            await spool.drain_rows(sink, batch_size=2, batch_bytes=2000)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b'{"reporting_structure": {"unexpected": 1}}',
        b'{"reporting_structure": [null]}',
        b'{"reporting_structure": [{"reporting_plans": [1]}]}',
        b'{"reporting_structure": [{"reporting_plans": {"unexpected": 1}}]}',
        b"",
    ],
)
async def test_unsupported_structures_do_not_become_empty_success(tmp_path, body):
    with pytest.raises(IncompleteToc):
        async with open_toc_spool(
            _Reader(body),
            URL,
            max_bytes=1000,
            row_bytes=2000,
            spool_bytes=64 * 1024 * 1024,
            temporary_parent=tmp_path,
        ):
            pytest.fail("invalid structure was admitted")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_duplicate_json_members_keep_last_value_and_key_position(tmp_path):
    body = b"""{"reporting_structure":[{"reporting_plans":[{"plan_id":"old"}],
    "in_network_files":[{"location":"https://files.example.test/old.json"}]}],
    "reporting_structure":[{"reporting_plans":[{"plan_id":"new"}],
    "reporting_plans":[{"plan_id":"final"}],
    "in_network_files":[{"location":"https://files.example.test/new.json"}],
    "in_network_files":[{"location":"https://files.example.test/final.json"}]}]}"""
    async with open_toc_spool(
        _Reader(body, 11),
        URL,
        max_bytes=10_000,
        row_bytes=10_000,
        spool_bytes=64 * 1024 * 1024,
        temporary_parent=tmp_path,
    ) as spool:
        assert [entry async for entry in spool.catalog_entries()] == parse_toc_catalog_entries(json.loads(body), URL)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b'{"in_network_files":[{"location":"https://files.example.test/old.json"}],'
        b'"in_network_files":[{"location":"https://files.example.test/final.json"}]}',
        b'{"reporting_structure":[{"reporting_plans":false,"reporting_plans":[{"plan_id":"final"}],'
        b'"in_network_files":[{"location":"https://files.example.test/final.json"}]}]}',
        b'{"reporting_entity_name":{"nested":["Example",1.25]},"description":["first",{"last":true}]}',
    ],
)
async def test_replaced_fields_and_container_metadata_match_complete_json(tmp_path, body):
    async with open_toc_spool(
        _Reader(body, 3),
        URL,
        max_bytes=10_000,
        row_bytes=10_000,
        spool_bytes=64 * 1024**2,
        temporary_parent=tmp_path,
    ) as spool:
        assert [entry async for entry in spool.catalog_entries()] == parse_toc_catalog_entries(json.loads(body), URL)
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "document_dict,message",
    [
        ({"reporting_entity_name": ["x"] * 100}, "row memory"),
        ({"ignored": {"nested": {"value": 1}}}, "JSON depth"),
    ],
)
async def test_metadata_allocations_and_ignored_nesting_cannot_bypass_limits(
    monkeypatch, tmp_path, document_dict, message
):
    if message == "JSON depth":
        monkeypatch.setattr("process.mrf_toc_spool.sys.getrecursionlimit", lambda: 52)
    with pytest.raises(IncompleteToc, match=message):
        async with open_toc_spool(
            _Reader(_body(document_dict)),
            URL,
            max_bytes=10_000,
            row_bytes=2000,
            spool_bytes=64 * 1024**2,
            temporary_parent=tmp_path,
        ):
            pytest.fail("out-of-budget metadata was admitted")
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_body_decode_replacement_and_arbitrary_integer_match_json(tmp_path):
    body = b'{"reporting_entity_name":"Example\xff", "reporting_structure":[{"reporting_plans":[{"plan_id":123456789012345678901234567890}],"in_network_files":[{"location":"https://files.example.test/a.json"}]}]}'
    toc_dict = json.loads(body.decode("utf-8", errors="replace"))
    async with open_toc_spool(
        _Reader(body, 7),
        URL,
        max_bytes=10_000,
        row_bytes=10_000,
        spool_bytes=64 * 1024 * 1024,
        temporary_parent=tmp_path,
    ) as spool:
        assert [entry async for entry in spool.catalog_entries()] == parse_toc_catalog_entries(toc_dict, URL)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "toc_dict",
    [
        {"data.files": [{"filePath": "wrong.json", "fileSchema": "in_network_rates", "plans": [{"planId": "wrong"}]}]},
        {
            "data": {
                "files": [],
                "files.item": {
                    "filePath": "wrong.json",
                    "fileSchema": "in_network_rates",
                    "plans": [{"planId": "wrong"}],
                },
            }
        },
        {"reporting_structure": [{"reporting_plans.item": {"plan_id": "wrong"}, "in_network_files": [_file("wrong")]}]},
        {"reporting_structure": [{"reporting_plans": [{"plan_id": "valid"}], "in_network_files.item": _file("wrong")}]},
    ],
)
async def test_literal_dotted_member_names_do_not_impersonate_nested_paths(tmp_path, toc_dict):
    assert await _entries(toc_dict, tmp_path) == parse_toc_catalog_entries(toc_dict, URL)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_root_files_adapter_retains_precedence_over_unused_structure(tmp_path):
    file_dict = {"filePath": "valid.json", "fileSchema": "in_network_rates", "plans": [{"planId": "valid"}]}
    toc_dict = {"reporting_structure": "unused", "files": [file_dict]}
    async with open_toc_spool(
        _Reader(_body(toc_dict)),
        URL,
        max_bytes=10_000,
        row_bytes=10_000,
        spool_bytes=64 * 1024 * 1024,
        temporary_parent=tmp_path,
    ) as spool:
        assert spool.is_healthsparq
        assert [file async for file in spool.healthsparq_files()] == [file_dict]
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("spool_bytes", [1, 200_000, 800_000])
async def test_disk_budget_exhaustion_never_admits_partial_rows_and_cleans(tmp_path, spool_bytes):
    document_dict = {"padding": "x" * 400_000, "reporting_structure": [_structure([], [_file("a")])]}
    with pytest.raises(IncompleteToc):
        async with open_toc_spool(
            _Reader(_body(document_dict), 64 * 1024),
            URL,
            max_bytes=1_000_000,
            row_bytes=100_000,
            spool_bytes=spool_bytes,
            temporary_parent=tmp_path,
        ):
            pytest.fail("exhausted spool was admitted")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_sqlite_page_cap_covers_journal_and_rows_without_temp_sorts(tmp_path):
    document_dict = {"reporting_structure": [_structure([{"plan_id": "a"}], [_file("a")])]}
    with pytest.raises(IncompleteToc, match="byte budget exhausted|insufficient database space"):
        async with open_toc_spool(
            _Reader(_body(document_dict)),
            URL,
            max_bytes=10_000,
            row_bytes=200_000,
            spool_bytes=2_000_000,
            temporary_parent=tmp_path,
        ) as spool:
            assert [entry async for entry in spool.catalog_entries()]
            pages = spool.connection.execute("PRAGMA max_page_count").fetchone()[0]
            size = spool.connection.execute("PRAGMA page_size").fetchone()[0]
            assert pages * size * 6 <= spool.storage.limit - spool.storage.used
            for index in range(100):
                spool.add_rows([], [{"mrf_file_id": str(index), "padding": "x" * 50_000}])
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_multi_document_target_retains_unique_final_rows(tmp_path):
    async with open_toc_spool(
        _Reader(b"{}"),
        URL,
        max_bytes=10_000,
        row_bytes=10_000,
        spool_bytes=2_000_000,
        temporary_parent=tmp_path,
    ) as spool:
        spool.add_rows([], [{"mrf_file_id": "shared", "value": "old"}])
        await spool.read_document(_Reader(b"{}"), max_bytes=10_000)
        spool.add_rows([], [{"mrf_file_id": "shared", "value": "new"}, {"mrf_file_id": "other"}])
        captured_files = []

        async def sink(plans, files):
            captured_files.extend(files)

        assert await spool.drain_rows(sink, batch_size=2, batch_bytes=10_000) == (0, 2)
        assert captured_files == [{"mrf_file_id": "shared", "value": "new"}, {"mrf_file_id": "other"}]


@pytest.mark.asyncio
@pytest.mark.parametrize("compressed", [False, True])
async def test_shared_decoded_budget_counts_documents_before_charset_conversion(tmp_path, compressed):
    first_body = b'{"padding":"' + b"\xe9" * 1000 + b'"}'
    last_body = gzip.compress(b"{}") if compressed else b"{}"
    budget = DecodedTocBudget(len(first_body) + 2)
    if compressed:
        assert len(last_body) > 2
    async with open_toc_store(URL, row_bytes=10_000, spool_bytes=2_000_000, temporary_parent=tmp_path) as spool:
        await spool.read_document(_Reader(first_body), max_bytes=budget.limit, charset="latin-1", decoded_budget=budget)
        await spool.read_document(_Reader(last_body), max_bytes=budget.limit, decoded_budget=budget)
        assert budget.used == budget.limit
        with pytest.raises(IncompleteToc, match="combined discovery byte limit"):
            await spool.read_document(_Reader(b"{}"), max_bytes=budget.limit, decoded_budget=budget)
        assert budget.used == budget.limit
    assert not list(tmp_path.iterdir())
