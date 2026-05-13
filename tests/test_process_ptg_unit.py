# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import gzip
import importlib
import io
import json
import os
import subprocess
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pytest


process_pkg = importlib.import_module("process")
process_ptg = importlib.import_module("process.ptg")


def test_filter_reporting_plans_matches_group_plan_id():
    plans = [
        {
            "plan_name": "Example Individual",
            "plan_id": "81974",
            "plan_market_type": "individual",
        },
        {
            "plan_name": "DEWITT, LLP-HPS",
            "plan_id": "391804522",
            "plan_sponsor_name": "DEWITT, LLP",
            "issuer_name": "WPS",
            "plan_market_type": "group",
        },
    ]

    result = process_ptg._filter_reporting_plans(
        plans,
        plan_ids=["391804522"],
        plan_market_types=["group"],
    )

    assert result == [plans[1]]


def test_ptg2_filter_jobs_by_url_contains_keeps_matching_rate_file():
    jobs = [
        {"type": "in_network", "url": "https://example.test/CMC_CRS_MRRF_in-network-rates.json.gz"},
        {"type": "in_network", "url": "https://example.test/PS1-50_C2_in-network-rates.json.gz"},
    ]

    result = process_ptg._filter_jobs_by_url_contains(jobs, ["ps1-50_c2"])

    assert result == [jobs[1]]


def test_ptg2_fast_object_iterator_yields_selected_top_level_arrays():
    payload = (
        b'{"version":"1.0","provider_references":[{"provider_group_id":7,'
        b'"provider_groups":[{"npi":[1],"note":"brace } in string"}]}],'
        b'"in_network":[{"negotiation_arrangement":"ffs","billing_code":"001",'
        b'"negotiated_rates":[{"negotiated_prices":[{"negotiated_rate":1.2}]}]}]}'
    )

    result = list(
        process_ptg._iter_top_level_objects_fast(
            io.BytesIO(payload),
            {
                "provider_reference": "provider_references.item",
                "in_network": "in_network.item",
            },
        )
    )

    assert [name for name, _ in result] == ["provider_reference", "in_network"]
    assert result[0][1]["provider_group_id"] == 7
    assert result[1][1]["billing_code"] == "001"


def test_filter_reporting_plans_matches_name_contains_case_insensitive():
    plans = [
        {
            "plan_name": "LIBERTY TITLE AND ABSTRACT INC-Statewide",
            "plan_id": "391937180",
            "plan_market_type": "group",
        },
    ]

    result = process_ptg._filter_reporting_plans(
        plans,
        plan_name_contains=["liberty title"],
        plan_market_types=["GROUP"],
    )

    assert result == plans


def test_filter_reporting_plans_returns_original_without_filters():
    plans = [{"plan_name": "Any Plan", "plan_id": "1", "plan_market_type": "group"}]

    assert process_ptg._filter_reporting_plans(plans) == plans


def test_as_int_list_normalizes_npi_strings():
    assert process_ptg._as_int_list(["1053488122", 1093228306, "", None, "bad"]) == [
        1053488122,
        1093228306,
    ]


def test_ptg2_semantic_hash_ignores_set_like_array_order():
    first = {
        "npi": [1093228306, 1053488122],
        "service_code": ["02", "01"],
        "billing_code_modifier": ["TC", "26"],
        "bundled_codes": [
            {"billing_code": "B", "billing_code_type": "HCPCS"},
            {"billing_code": "A", "billing_code_type": "CPT"},
        ],
        "negotiated_rate": Decimal("12.3400"),
    }
    second = {
        "npi": [1053488122, 1093228306],
        "service_code": ["01", "02"],
        "billing_code_modifier": ["26", "TC"],
        "bundled_codes": [
            {"billing_code_type": "CPT", "billing_code": "A"},
            {"billing_code_type": "HCPCS", "billing_code": "B"},
        ],
        "negotiated_rate": "12.34",
    }

    assert process_ptg.semantic_hash(first, domain="rate") == process_ptg.semantic_hash(second, domain="rate")


@pytest.mark.parametrize(
    "changed",
    [
        {"negotiated_rate": Decimal("12.35")},
        {"billing_code_modifier": ["26", "GT"]},
        {"setting": "inpatient"},
        {"context": {"plan_id": "different"}},
    ],
)
def test_ptg2_semantic_hash_changes_for_rate_context_modifier_and_setting(changed):
    base = {
        "negotiated_rate": Decimal("12.34"),
        "billing_code_modifier": ["26", "TC"],
        "setting": "outpatient",
        "context": {"plan_id": "010854205"},
    }
    modified = dict(base)
    modified.update(changed)

    assert process_ptg.semantic_hash(base, domain="rate") != process_ptg.semantic_hash(modified, domain="rate")


def test_ptg2_semantic_hash_defaults_to_short_checksum_mode(monkeypatch):
    monkeypatch.delenv(process_ptg.PTG2_HASH_MODE_ENV, raising=False)

    value = {"a": 1, "b": [3, 2, 1]}
    result = process_ptg.semantic_hash(value, domain="x")

    assert len(result) == 16
    assert int(result, 16) >= 0


def test_ptg2_semantic_hash_can_switch_back_to_sha256(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_HASH_MODE_ENV, "sha256")

    value = {"a": 1, "b": [3, 2, 1]}
    result = process_ptg.semantic_hash(value, domain="x")

    assert len(result) == 64
    assert int(result, 16) >= 0


def test_ptg2_rejects_float_money_values():
    with pytest.raises(TypeError):
        process_ptg.semantic_hash({"negotiated_rate": 12.34}, domain="rate")


def test_ptg2_modes_and_confidence_wording_are_explicit():
    assert process_ptg.normalize_ptg2_search_mode(None) == "product_search"
    assert process_ptg.normalize_ptg2_search_mode("exact_source") == "exact_source"
    with pytest.raises(ValueError):
        process_ptg.normalize_ptg2_search_mode("loose")
    assert "Published negotiated rate" in process_ptg.ptg2_confidence_statement("tic_rate_npi_tin")


def test_ptg2_runtime_checksum_uses_bigint_hash_space():
    hashes = [process_ptg._make_checksum("rate", idx) for idx in range(5000)]

    assert len(set(hashes)) == len(hashes)
    assert all(0 <= value < 2**63 for value in hashes)
    assert max(hashes) > 2**32


def test_ptg2_provider_group_identity_is_source_independent_and_order_insensitive():
    tin_a = {"type": "EIN", "value": "12-3456789"}
    tin_b = {"type": "ein", "value": "123456789"}

    first = process_ptg._provider_group_identity_hash(tin_a, [3, 1, 2, 2])
    second = process_ptg._provider_group_identity_hash(tin_b, [2, 3, 1])

    assert first == second


def test_ptg2_provider_set_entry_packs_groups_order_insensitively():
    groups_a = [
        {"tin": {"type": "ein", "value": "111"}, "npi": [3, 1]},
        {"tin": {"type": "ein", "value": "222"}, "npi": [2]},
    ]
    groups_b = list(reversed(groups_a))

    entry_a, row_a = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=groups_a,
        network_names=["A"],
    )
    entry_b, row_b = process_ptg._build_provider_set_entry(
        file_id=2,
        provider_group_ref=20,
        provider_groups=groups_b,
        network_names=["B"],
    )

    assert entry_a["__hash__"] == entry_b["__hash__"]
    assert row_a["provider_group_hash"] == row_b["provider_group_hash"]
    assert row_a["npi"] == [1, 2, 3]
    assert row_a["tin_type"] == "set"


def test_ptg2_combined_provider_set_entry_packs_rate_provider_refs():
    first, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=[{"tin": {"type": "ein", "value": "111"}, "npi": [1, 2]}],
        network_names=["A"],
    )
    second, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=11,
        provider_groups=[{"tin": {"type": "ein", "value": "222"}, "npi": [3]}],
        network_names=["A"],
    )

    combined_a, row_a = process_ptg._combine_provider_set_entries(file_id=1, entries=[first, second])
    combined_b, row_b = process_ptg._combine_provider_set_entries(file_id=2, entries=[second, first])

    assert combined_a["__hash__"] == combined_b["__hash__"]
    assert row_a["provider_group_hash"] == row_b["provider_group_hash"]
    assert row_a["npi"] == [1, 2, 3]
    assert row_a["tin_type"] == "set"


def test_ptg2_provider_group_rows_are_canonical_and_source_independent():
    groups_a = [{"tin": {"type": "ein", "value": "12-3456789"}, "npi": [3, 1, 2]}]
    groups_b = [{"tin": {"type": "EIN", "value": "123456789"}, "npi": [2, 3, 1]}]

    row_a = process_ptg._ptg2_provider_group_rows(provider_groups=groups_a)[0]
    row_b = process_ptg._ptg2_provider_group_rows(provider_groups=groups_b)[0]

    assert row_a["provider_group_hash"] == row_b["provider_group_hash"]
    assert row_a["npi"] == [1, 2, 3]
    assert row_a["tin_type"] == "ein"
    assert row_a["tin_value"] == "123456789"


def test_ptg2_large_provider_set_row_omits_inline_npi_when_group_hashes_available(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV, "2")
    first, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=[{"tin": {"type": "ein", "value": "111"}, "npi": [1, 2]}],
    )
    second, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=11,
        provider_groups=[{"tin": {"type": "ein", "value": "222"}, "npi": [3]}],
    )
    combined, _ = process_ptg._combine_provider_set_entries(file_id=1, entries=[first, second])

    row = process_ptg._ptg2_provider_set_row(combined)

    assert row["provider_count"] == 3
    assert row["npi"] is None
    assert row["canonical_payload"]["npi_inline"] is False
    assert row["canonical_payload"]["provider_group_hashes"]


def test_ptg2_provider_set_row_omits_inline_npi_by_default_when_group_hashes_available(monkeypatch):
    monkeypatch.delenv(process_ptg.PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV, raising=False)
    entry, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=[{"tin": {"type": "ein", "value": "111"}, "npi": [1]}],
    )

    row = process_ptg._ptg2_provider_set_row(entry)

    assert row["provider_count"] == 1
    assert row["npi"] is None
    assert row["canonical_payload"]["npi_inline"] is False


def test_ptg2_procedure_identity_groups_display_text_variants():
    base = {
        "billing_code_type": "CPT",
        "billing_code_type_version": "2026",
        "billing_code": "99213",
        "negotiation_arrangement": "ffs",
        "name": "Office visit",
        "description": "First description",
    }
    variant = {**base, "name": "Established patient visit", "description": "Different description"}
    changed_arrangement = {**base, "negotiation_arrangement": "bundle"}

    assert process_ptg._ptg2_procedure_row(base)["procedure_hash"] == process_ptg._ptg2_procedure_row(variant)["procedure_hash"]
    assert (
        process_ptg._ptg2_procedure_row(base)["procedure_hash"]
        != process_ptg._ptg2_procedure_row(changed_arrangement)["procedure_hash"]
    )


def test_ptg2_canonicalize_url_strips_signed_params():
    url = (
        "HTTPS://Example.COM:443/path/file.json.gz?"
        "sig=secret&sv=2020&foo=bar&Signature=abc&Key-Pair-Id=k&b=2&a=1"
    )

    assert process_ptg.canonicalize_url(url) == "https://example.com/path/file.json.gz?a=1&b=2&foo=bar"


def test_ptg2_toc_parser_handles_uhc_sponsor_typo_and_duplicate_signed_urls():
    toc = {
        "reporting_entity_name": "UHC",
        "reporting_entity_type": "payer",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "Heartland",
                        "plan_id": "010854205",
                        "plan_sponser_name": "Heartland Co",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {"location": "https://cdn.test/rates.json.gz?sig=a&foo=1"},
                    {"location": "https://cdn.test/rates.json.gz?sig=b&foo=1"},
                ],
            }
        ],
    }

    entries = process_ptg.parse_toc_catalog_entries(
        toc,
        "https://payer.test/toc.json",
        plan_ids=["010854205"],
    )
    in_network_entries = [entry for entry in entries if entry.source_type == "in-network"]

    assert len(in_network_entries) == 1
    assert in_network_entries[0].canonical_url == "https://cdn.test/rates.json.gz?foo=1"
    assert in_network_entries[0].plan_info[0]["plan_sponsor_name"] == "Heartland Co"


def test_ptg2_artifact_reuse_by_strong_etag_and_length(tmp_path):
    raw_path = tmp_path / "raw.json"
    raw_path.write_text('{"ok": true}', encoding="utf-8")
    raw_sha, byte_count = process_ptg.sha256_file(raw_path)
    store = process_ptg.PTG2ArtifactStore(tmp_path)
    candidate = {
        "artifact_kind": process_ptg.PTG2_ARTIFACT_RAW,
        "canonical_url": "https://example.test/raw.json",
        "raw_storage_uri": raw_path.resolve().as_uri(),
        "raw_sha256": raw_sha,
        "content_length": byte_count,
        "etag": '"strong"',
    }
    head = process_ptg.PTG2HeadMetadata(
        url="https://example.test/raw.json",
        status=200,
        etag='"strong"',
        content_length=byte_count,
        supports_head=True,
    )

    reused, mode = process_ptg.choose_reusable_raw_artifact([candidate], head, store=store)

    assert reused == candidate
    assert mode == "strong_etag_length"


def test_ptg2_packing_helpers_are_order_insensitive_for_sets():
    provider_a = process_ptg.build_provider_set([3, 1, 2, 2], tin_type="ein", tin_value="123")
    provider_b = process_ptg.build_provider_set([2, 3, 1], tin_type="ein", tin_value="123")
    price_set_a = process_ptg.build_price_set(["b", "a", "a"])
    price_set_b = process_ptg.build_price_set(["a", "b"])
    chunk_a = process_ptg.build_fact_chunk("ctx", "in_network", "proc", "0a", ["pack-b", "pack-a"])
    chunk_b = process_ptg.build_fact_chunk("ctx", "in_network", "proc", "0a", ["pack-a", "pack-b"])
    rate_set_a = process_ptg.build_rate_set("ctx", ["chunk-b", "chunk-a"])
    rate_set_b = process_ptg.build_rate_set("ctx", ["chunk-a", "chunk-b"])

    assert provider_a["provider_set_hash"] == provider_b["provider_set_hash"]
    assert provider_a["npi"] == [1, 2, 3]
    assert price_set_a["price_set_hash"] == price_set_b["price_set_hash"]
    assert chunk_a["fact_chunk_hash"] == chunk_b["fact_chunk_hash"]
    assert rate_set_a["rate_set_hash"] == rate_set_b["rate_set_hash"]
    assert process_ptg.provider_hash_bucket(provider_a["provider_set_hash"], bucket_count=16)


def test_ptg2_provider_bucket_count_is_configurable(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_PROVIDER_BUCKET_COUNT_ENV, "16")

    assert process_ptg.ptg2_provider_bucket_count() == 16


def test_ptg2_rate_pack_group_is_order_insensitive_for_provider_sets():
    pack_a = process_ptg.build_rate_pack_group(
        "ctx",
        "in_network",
        "proc",
        ["provider-b", "provider-a", "provider-a"],
        "price",
        "source",
    )
    pack_b = process_ptg.build_rate_pack_group(
        "ctx",
        "in_network",
        "proc",
        ["provider-a", "provider-b"],
        "price",
        "source",
    )
    changed = process_ptg.build_rate_pack_group(
        "ctx",
        "in_network",
        "proc",
        ["provider-a"],
        "price",
        "source",
    )

    assert pack_a["rate_pack_hash"] == pack_b["rate_pack_hash"]
    assert pack_a["provider_set_hash"] == pack_b["provider_set_hash"]
    assert pack_a["canonical_payload"]["provider_set_hashes"] == ["provider-a", "provider-b"]
    assert pack_a["rate_pack_hash"] != changed["rate_pack_hash"]


def test_ptg2_rate_pack_procedure_group_is_order_insensitive():
    pack_a = process_ptg.build_rate_pack_procedure_group(
        "ctx",
        "in_network",
        ["proc-b", "proc-a", "proc-a"],
        ["provider-b", "provider-a"],
        "price",
        "source",
    )
    pack_b = process_ptg.build_rate_pack_procedure_group(
        "ctx",
        "in_network",
        ["proc-a", "proc-b"],
        ["provider-a", "provider-b"],
        "price",
        "source",
    )
    changed = process_ptg.build_rate_pack_procedure_group(
        "ctx",
        "in_network",
        ["proc-a"],
        ["provider-a", "provider-b"],
        "price",
        "source",
    )

    assert pack_a["rate_pack_hash"] == pack_b["rate_pack_hash"]
    assert pack_a["canonical_payload"]["procedure_hashes"] == ["proc-a", "proc-b"]
    assert pack_a["canonical_payload"]["provider_set_hashes"] == ["provider-a", "provider-b"]
    assert pack_a["rate_pack_hash"] != changed["rate_pack_hash"]


def test_ptg2_compact_rate_pack_flush_groups_procedures(monkeypatch):
    pushed = {}

    async def fake_push(rows, cls, **_kwargs):
        pushed.setdefault(getattr(cls, "__name__", str(cls)), []).extend(rows)

    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    state = process_ptg._compact_state(batch_rows=1)
    state["rate_pack_groups"] = {
        ("proc-a", "price", "source"): {"provider-a"},
        ("proc-b", "price", "source"): {"provider-a"},
    }

    asyncio.run(process_ptg._flush_compact_rate_pack_groups(state, "ctx"))

    assert state["rate_pack_groups"] == {}
    assert state["counts"]["rate_packs"] == 1
    assert len(pushed["PTG2RatePack"]) == 1
    assert pushed["PTG2RatePack"][0]["canonical_payload"]["procedure_hashes"] == ["proc-a", "proc-b"]
    assert sum(len(v) for v in state["chunk_rate_packs"].values()) == 2


def test_ptg2_compact_rate_pack_flush_writes_serving_rows(monkeypatch):
    pushed = {}

    async def fake_push(rows, cls, **_kwargs):
        pushed.setdefault(getattr(cls, "__name__", str(cls)), []).extend(rows)

    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    state = process_ptg._compact_state(batch_rows=1)
    state["snapshot_id"] = "snap"
    state["plan_fields"] = {"plan_id": "010854205", "plan_name": "Heartland"}
    state["procedure_payloads"] = {
        "proc-a": {
            "billing_code_type": "CPT",
            "billing_code": "00102",
            "name": "Anesthesia",
            "description": "Anesthesia description",
        }
    }
    state["price_payloads"] = {"price": [{"negotiated_rate": "50", "negotiated_type": "negotiated"}]}
    state["provider_set_counts"] = {"provider-a": 10, "provider-b": 20}
    state["source_trace_payload"] = [{"url": "https://example.test/rates.json.gz"}]
    state["rate_pack_groups"] = {("proc-a", "price", "source"): {"provider-a", "provider-b"}}

    asyncio.run(process_ptg._flush_compact_rate_pack_groups(state, "ctx"))

    serving_row = pushed["PTG2ServingRate"][0]
    assert serving_row["snapshot_id"] == "snap"
    assert serving_row["plan_id"] == "010854205"
    assert serving_row["reported_code"] == "00102"
    assert serving_row["procedure_code"] == process_ptg.return_checksum(["CPT", "00102"])
    assert serving_row["provider_count"] == 30
    assert serving_row["prices"][0]["negotiated_rate"] == 50


def test_ptg2_compact_rows_can_schedule_async_writes(monkeypatch):
    pushed = []

    class FakeClass:
        __name__ = "FakeClass"

    async def fake_push(rows, cls, **_kwargs):
        await asyncio.sleep(0)
        pushed.append((cls, rows))

    monkeypatch.setenv(process_ptg.PTG2_ASYNC_WRITE_TASKS_ENV, "2")
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    state = process_ptg._compact_state(batch_rows=1)

    asyncio.run(process_ptg._schedule_compact_write(state, [{"a": 1}], FakeClass))
    assert state["pending_writes"]
    asyncio.run(process_ptg._drain_compact_writes(state))

    assert not state["pending_writes"]
    assert pushed == [(FakeClass, [{"a": 1}])]


def test_ptg2_serving_only_worker_chunk_dedupes_rows(monkeypatch):
    rows_a = {
        "serving_rows": [{"serving_rate_id": "r1"}],
        "serving_rate_compact_rows": [{"serving_rate_id": "cr1"}],
        "provider_set_rows": [{"provider_set_hash": "ps1"}],
        "price_set_rows": [{"price_set_hash": "pr1"}],
        "provider_set_component_rows": [{"provider_set_hash": "ps1", "provider_group_hash": 10}],
        "provider_group_member_rows": [{"provider_group_hash": 10, "npi": 123}],
        "procedure_rows": [{"procedure_hash": "p1"}],
    }
    rows_b = {
        "serving_rows": [{"serving_rate_id": "r1"}, {"serving_rate_id": "r2"}],
        "serving_rate_compact_rows": [{"serving_rate_id": "cr1"}, {"serving_rate_id": "cr2"}],
        "provider_set_rows": [{"provider_set_hash": "ps1"}, {"provider_set_hash": "ps2"}],
        "price_set_rows": [{"price_set_hash": "pr1"}, {"price_set_hash": "pr2"}],
        "provider_set_component_rows": [
            {"provider_set_hash": "ps1", "provider_group_hash": 10},
            {"provider_set_hash": "ps2", "provider_group_hash": 11},
        ],
        "provider_group_member_rows": [
            {"provider_group_hash": 10, "npi": 123},
            {"provider_group_hash": 11, "npi": 456},
        ],
        "procedure_rows": [{"procedure_hash": "p1"}, {"procedure_hash": "p2"}],
    }
    it = iter([rows_a, rows_b])
    monkeypatch.setattr(process_ptg, "_serving_only_worker_process", lambda _payload: next(it))

    merged = process_ptg._serving_only_worker_process_chunk([b"a", b"b"])

    assert [row["serving_rate_id"] for row in merged["serving_rows"]] == ["r1", "r2"]
    assert [row["serving_rate_id"] for row in merged["serving_rate_compact_rows"]] == ["cr1", "cr2"]
    assert [row["provider_set_hash"] for row in merged["provider_set_rows"]] == ["ps1", "ps2"]
    assert [row["price_set_hash"] for row in merged["price_set_rows"]] == ["pr1", "pr2"]
    assert len(merged["provider_set_component_rows"]) == 2
    assert len(merged["provider_group_member_rows"]) == 2
    assert [row["procedure_hash"] for row in merged["procedure_rows"]] == ["p1", "p2"]


def test_ptg2_provider_reference_cache_round_trips_numeric_and_string_refs(tmp_path):
    cache = process_ptg.PTG2ProviderReferenceCache(tmp_path / "provider_refs.sqlite")
    try:
        cache.put(123, [{"__hash__": 42, "npi": [1], "provider_group_id": 123}])
        cache.put("abc", [{"__hash__": 43, "npi": [2], "provider_group_id": "abc"}])
        cache.commit()

        assert cache.get("123")[0]["npi"] == [1]
        assert cache.get("abc")[0]["npi"] == [2]
        assert cache.provider_hashes == {42, 43}
    finally:
        cache.close()


def test_ptg2_provider_reference_cache_uses_bounded_memory_lru(tmp_path, monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV, "1")
    cache = process_ptg.PTG2ProviderReferenceCache(tmp_path / "provider_refs.sqlite")
    try:
        cache.put(1, [{"__hash__": 1, "npi": [1]}])
        cache.put(2, [{"__hash__": 2, "npi": [2]}])
        cache.commit()

        assert cache.get(2)[0]["npi"] == [2]
        assert cache.get(1)[0]["npi"] == [1]

        stats = cache.stats()
        assert stats["provider_cache_memory_limit"] == 1
        assert stats["provider_cache_memory_size"] == 1
        assert stats["provider_cache_gets"] == 2
        assert stats["provider_cache_sqlite_hits"] == 1
        assert stats["provider_cache_memory_hits"] == 1
    finally:
        cache.close()


def test_ptg2_in_memory_provider_reference_cache_tracks_hits():
    cache = process_ptg.PTG2InMemoryProviderReferenceCache()

    cache.put(123, [{"__hash__": 42, "npi": [1], "provider_group_id": 123}])

    assert cache.get("123")[0]["npi"] == [1]
    assert cache.get("missing") == []
    assert cache.provider_hashes == {42}
    stats = cache.stats()
    assert stats["provider_cache_memory_hits"] == 1
    assert stats["provider_cache_misses"] == 1
    assert stats["provider_cache_sqlite_hits"] == 0


def test_ptg2_provider_combo_cache_is_order_insensitive_and_bounded():
    cache = process_ptg.OrderedDict()
    stats = {
        "provider_combo_cache_gets": 0,
        "provider_combo_cache_hits": 0,
        "provider_combo_cache_misses": 0,
        "provider_combo_cache_size": 0,
        "provider_combo_cache_limit": 1,
    }
    key_a = process_ptg._provider_combo_cache_key([2, "1", 1])
    key_b = process_ptg._provider_combo_cache_key(["1", 2])

    assert key_a == key_b
    assert process_ptg._provider_combo_cache_get(cache, key_a, stats) is None
    process_ptg._provider_combo_cache_put(cache, key_a, {"__hash__": 1}, stats, limit=1)
    assert process_ptg._provider_combo_cache_get(cache, key_b, stats)["__hash__"] == 1
    process_ptg._provider_combo_cache_put(cache, ("3",), {"__hash__": 3}, stats, limit=1)

    assert key_a not in cache
    assert stats["provider_combo_cache_hits"] == 1
    assert stats["provider_combo_cache_misses"] == 1


def test_ptg2_worker_capacity_waits_on_batch_count_or_bytes():
    assert process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=2,
        pending_input_bytes=8,
        next_batch_bytes=1,
        max_pending_batches=2,
        max_pending_bytes=100,
    )
    assert process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=1,
        pending_input_bytes=8,
        next_batch_bytes=4,
        max_pending_batches=4,
        max_pending_bytes=10,
    )
    assert not process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=0,
        pending_input_bytes=0,
        next_batch_bytes=20,
        max_pending_batches=4,
        max_pending_bytes=10,
    )
    assert not process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=1,
        pending_input_bytes=4,
        next_batch_bytes=4,
        max_pending_batches=4,
        max_pending_bytes=10,
    )


def test_ptg2_fast_provider_union_carries_count_without_npi_materialization(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_FAST_PROVIDER_UNION_ENV, "true")

    combined_entry, _row = process_ptg._combine_provider_set_entries(
        file_id=1,
        entries=[
            {
                "__hash__": 11,
                "npi": [1, 2],
                "provider_count": 2,
                "provider_group_hashes": [11],
            },
            {
                "__hash__": 22,
                "npi": [2, 3],
                "provider_count": 2,
                "provider_group_hashes": [22],
            },
        ],
    )

    provider_set_row = process_ptg._ptg2_provider_set_row(combined_entry)

    assert combined_entry["npi"] == []
    assert combined_entry["provider_count"] == 4
    assert provider_set_row["provider_count"] == 4
    assert provider_set_row["npi"] is None
    assert provider_set_row["canonical_payload"]["provider_count_mode"] == "summed_provider_groups"


def test_ptg2_single_pass_in_network_parser_uses_provider_cache(tmp_path, monkeypatch):
    raw_path = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"tin": {"type": "ein", "value": "123"}, "npi": ["1234567890"]}],
            }
        ],
        "in_network": [
            {
                "negotiation_arrangement": "ffs",
                "name": "Office visit",
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 12.34,
                                "expiration_date": "2026-12-31",
                                "billing_class": "professional",
                                "service_code": ["11"],
                            }
                        ],
                    }
                ],
            }
        ],
    }
    with gzip.open(raw_path, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    pushed = {}

    async def fake_push(rows, cls, **_kwargs):
        pushed.setdefault(cls, []).extend(rows)

    monkeypatch.setattr(process_ptg, "push_objects", fake_push)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "log_error", AsyncMock())

    cache = process_ptg.PTG2ProviderReferenceCache(tmp_path / "provider_refs.sqlite")
    classes = {
        "PTGProviderGroup": "providers",
        "PTGInNetworkItem": "items",
        "PTGBillingCode": "billing",
        "PTGNegotiatedRate": "rates",
        "PTGNegotiatedPrice": "prices",
    }
    try:
        asyncio.run(
            process_ptg._parse_in_network_file_single_pass(
                str(raw_path),
                99,
                {"plan_id": "010854205"},
                None,
                cache,
                classes,
                False,
                "log",
                "file://rates.json.gz",
            )
        )
    finally:
        cache.close()

    assert pushed["providers"][0]["npi"] == [1234567890]
    assert pushed["items"][0]["billing_code"] == "99213"
    assert pushed["rates"][0]["provider_group_hash"] == pushed["providers"][0]["provider_group_hash"]
    assert pushed["prices"][0]["negotiated_rate"] == 12.34
    process_ptg.log_error.assert_not_awaited()


def test_ptg2_stream_logical_artifact_handles_gzip_without_loading_all(tmp_path):
    raw_path = tmp_path / "toc.json.gz"
    expected = b'{"reporting_structure":[]}'
    with gzip.open(raw_path, "wb") as fp:
        fp.write(expected)

    logical = process_ptg.stream_logical_artifact(raw_path, output_dir=tmp_path)

    assert logical.compression == "gzip"
    assert Path(logical.logical_path).read_bytes() == expected
    assert logical.logical_sha256 == process_ptg.sha256_bytes(expected)


def test_ptg2_logical_identity_streams_gzip_without_materializing_json(tmp_path):
    raw_path = tmp_path / "rates.json.gz"
    expected = b'{"in_network":[]}'
    with gzip.open(raw_path, "wb") as fp:
        fp.write(expected)

    logical = process_ptg.logical_artifact_identity(raw_path)

    assert logical.compression == "gzip"
    assert logical.logical_path == str(raw_path)
    assert logical.logical_sha256 == process_ptg.sha256_bytes(expected)
    assert not list(tmp_path.glob("*_logical.json"))
    with process_ptg.open_json_artifact_stream(raw_path) as fp:
        assert fp.read() == expected


def test_ptg2_ensure_tables_uses_existing_db_create_table(monkeypatch):
    created = []

    async def fake_status(_statement):
        return None

    async def fake_create_table(table, **_kwargs):
        created.append(table.name)

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "create_table", fake_create_table)

    asyncio.run(process_ptg.ensure_ptg2_tables())

    assert "ptg2_import_run" in created
    assert "ptg2_current_snapshot" in created
    assert "ptg2_source_file_version" in created
    assert "ptg2_provider_group" in created
    assert "ptg2_rate_pack" in created


def test_ptg2_ensure_tables_fails_fast_on_create_error(monkeypatch):
    async def fake_status(_statement):
        return None

    async def fake_create_table(table, **_kwargs):
        if table.name == "ptg2_snapshot":
            raise RuntimeError("no permission")

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "create_table", fake_create_table)

    with pytest.raises(RuntimeError, match="ptg2_snapshot"):
        asyncio.run(process_ptg.ensure_ptg2_tables())


def test_ptg2_in_network_download_failure_returns_failed_result(monkeypatch, tmp_path):
    async def fake_materialize(*_args, **_kwargs):
        raise RuntimeError("download failed")

    monkeypatch.setattr(process_ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)

    result = asyncio.run(
        process_ptg._process_in_network_file(
            {"type": "in_network", "url": "https://example.test/rates.json.gz"},
            {"PTGFile": "files", "ImportLog": "log"},
            {},
            False,
        )
    )

    assert result.success is False
    assert result.source_type == "in_network"
    assert result.url == "https://example.test/rates.json.gz"
    assert "download failed" in result.error


def test_ptg2_main_marks_failed_when_all_discovered_jobs_fail(monkeypatch):
    pushed = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_process(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            False,
            error="download failed",
        )

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "build_ptg2_snapshot_index_artifact", AsyncMock())
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "false")

    with pytest.raises(RuntimeError, match="processed zero files"):
        asyncio.run(
            process_ptg.main(
                in_network_url="https://example.test/rates.json.gz",
                import_month="2026-04",
                import_id="state_machine_test",
            )
        )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    snapshot_rows = [row for cls_name, row in pushed if cls_name == "PTG2Snapshot"]
    current_rows = [row for cls_name, row in pushed if cls_name == "PTG2CurrentSnapshot"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert import_run_rows[-1]["report"]["files_processed"] == 0
    assert import_run_rows[-1]["report"]["files_failed"] == 1
    assert "download failed" in import_run_rows[-1]["report"]["failed_files"][0]["error"]
    assert snapshot_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert current_rows == []


def test_ptg2_snapshot_artifact_builder_writes_serving_index(monkeypatch, tmp_path):
    class Table:
        def __init__(self, name):
            self.schema = "mrf"
            self.name = name

    class Model:
        def __init__(self, name):
            self.__tablename__ = name
            self.__table__ = Table(name)

    rows = [
        {
            "plan_id": "010854205",
            "plan_name": "Heartland",
            "plan_id_type": "EIN",
            "plan_market_type": "group",
            "issuer_name": "Heartland",
            "plan_sponsor_name": "Heartland",
            "billing_code": "70551",
            "billing_code_type": "CPT",
            "procedure_name": "MRI brain",
            "procedure_description": "MRI brain",
            "provider_npi": [1234567890],
            "tin_type": "ein",
            "tin_value": "123",
            "tin_business_name": "Example Imaging",
            "negotiated_type": "negotiated",
            "negotiated_rate": "450.00",
            "expiration_date": "2026-12-31",
            "service_code": ["11"],
            "billing_class": "professional",
            "setting": "outpatient",
            "billing_code_modifier": [],
            "additional_information": None,
            "source_url": "https://example.test/rates.json.gz",
        }
    ]

    async def fake_all(_sql):
        return rows

    async def fake_push_objects(_payload, _cls, rewrite=False):
        return None

    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(tmp_path))
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    classes = {
        "PTGInNetworkItem": Model("ptg_in_network_item_test"),
        "PTGNegotiatedRate": Model("ptg_negotiated_rate_test"),
        "PTGNegotiatedPrice": Model("ptg_negotiated_price_test"),
        "PTGProviderGroup": Model("ptg_provider_group_test"),
        "PTGFile": Model("ptg_file_test"),
    }

    result = asyncio.run(process_ptg.build_ptg2_snapshot_index_artifact(classes, "snap-test", "run-test"))

    assert result["plan_count"] == 1
    artifact_path = tmp_path / "snapshot_index" / "snap-test.json"
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert payload["rates"]["010854205"]["70551"][0]["prices"][0]["negotiated_rate"] == 450


def test_ptg2_db_serving_index_builder_materializes_table(monkeypatch, tmp_path):
    statuses = []

    async def fake_scalar(sql, **params):
        if "to_regclass" in sql:
            return params["table_name"]
        if "COUNT(*)" in sql:
            return 1
        if "COUNT(DISTINCT plan_id)" in sql:
            return 1
        if "COUNT(DISTINCT COALESCE" in sql:
            return 1
        if "SUM(provider_count)" in sql:
            return 123
        return None

    async def fake_status(sql, **params):
        statuses.append((sql, params))
        return 1

    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(tmp_path))
    monkeypatch.setattr(process_ptg.db, "scalar", fake_scalar)
    monkeypatch.setattr(process_ptg.db, "status", fake_status)

    result = asyncio.run(process_ptg.build_ptg2_db_serving_index("snap-compact", "run-compact"))

    insert_sql = next(sql for sql, _params in statuses if "INSERT INTO mrf.ptg2_serving_rate" in sql)
    assert result["storage"] == "db"
    assert result["table"] == "mrf.ptg2_serving_rate"
    assert result["provider_granularity"] == "provider_set"
    assert result["procedure_consolidation"]["system"] == "HP_PROCEDURE_CODE"
    assert "code_crosswalk" in insert_sql
    assert "pricing_procedure" in insert_sql
    assert "code_catalog" in insert_sql
    assert "snapshot_index" not in [part.name for part in tmp_path.iterdir()]


def test_ptg_cli_passes_plan_filters(monkeypatch):
    fake_initiate = AsyncMock()
    monkeypatch.setattr(process_pkg, "initiate_ptg", fake_initiate)

    def fake_run(coro):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(process_pkg.asyncio, "run", fake_run)

    process_pkg.ptg.callback(
        toc_url=("https://example.test/toc.json",),
        toc_list=None,
        in_network_url=None,
        allowed_url=None,
        provider_ref_url=None,
        import_id="ptg_smoke",
        import_month="2026-04-01",
        max_files=1,
        max_items=2,
        plan_id=("391804522",),
        plan_name_contains=("dewitt",),
        plan_market_type=("group",),
        file_url_contains=("ps1-50",),
        reuse_raw_artifacts=False,
        keep_partial_artifacts=True,
        test=True,
    )

    fake_initiate.assert_called_once_with(
        test_mode=True,
        toc_urls=["https://example.test/toc.json"],
        toc_list=None,
        in_network_url=None,
        allowed_url=None,
        provider_ref_url=None,
        import_id="ptg_smoke",
        import_month="2026-04-01",
        max_files=1,
        max_items=2,
        plan_ids=["391804522"],
        plan_name_contains=["dewitt"],
        plan_market_types=["group"],
        file_url_contains=["ps1-50"],
        reuse_raw_artifacts=False,
        keep_partial_artifacts=True,
    )


def test_ptg2_rust_scanner_emits_top_level_object_bytes(tmp_path):
    if process_ptg._ptg2_rust_scanner_binary() is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [
            {"provider_group_id": 1, "provider_groups": [{"npi": [123], "tin": {"type": "ein", "value": "1"}}]},
            {"provider_group_id": 2, "provider_groups": [{"npi": [456], "tin": {"type": "ein", "value": "2"}}]},
        ],
        "in_network": [
            {"billing_code": "99213", "negotiated_rates": []},
            {"billing_code": "70551", "negotiated_rates": []},
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    rows = list(process_ptg._iter_top_level_object_bytes_rust(artifact, {"provider_references", "in_network"}))

    assert [name for name, _raw in rows] == [
        "provider_references",
        "provider_references",
        "in_network",
        "in_network",
    ]
    assert json.loads(rows[0][1])["provider_group_id"] == 1
    assert json.loads(rows[-1][1])["billing_code"] == "70551"


def test_ptg2_rust_scanner_splits_large_in_network_objects(tmp_path, monkeypatch):
    if process_ptg._ptg2_rust_scanner_binary() is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "name": "Office visit",
                "negotiated_rates": [
                    {"provider_references": [index], "negotiated_prices": [{"negotiated_rate": index}]}
                    for index in range(5)
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))
    monkeypatch.setenv(process_ptg.PTG2_RUST_SPLIT_IN_NETWORK_ENV, "true")
    monkeypatch.setenv("HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES", "2")

    rows = list(process_ptg._iter_top_level_object_bytes_rust(artifact, {"provider_references", "in_network"}))
    in_network_rows = [json.loads(raw) for name, raw in rows if name == "in_network"]

    assert [len(row["negotiated_rates"]) for row in in_network_rows] == [2, 2, 1]
    assert {row["billing_code"] for row in in_network_rows} == {"99213"}
    assert in_network_rows[0]["negotiated_rates"][0]["provider_references"] == [0]
    assert in_network_rows[-1]["negotiated_rates"][0]["provider_references"] == [4]


def test_ptg2_rust_compact_serving_mode_emits_copy_oriented_rows(tmp_path):
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code_type_version": "2026",
                "billing_code": "99213",
                "name": "Office visit",
                "description": "Established patient office visit",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 100,
                                "expiration_date": "2026-12-31",
                                "service_code": ["11"],
                                "billing_class": "professional",
                            }
                        ],
                    }
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    frames = []
    stream = io.BytesIO(completed.stdout)
    while True:
        header = stream.readline()
        if not header:
            break
        name, size = header.rstrip(b"\n").split(b"\t", 1)
        payload_bytes = stream.read(int(size))
        assert stream.read(1) == b"\n"
        frames.append((name.decode("utf-8"), json.loads(payload_bytes)))

    kinds = [kind for kind, _row in frames]
    assert "procedure" in kinds
    assert "price_set" in kinds
    assert "provider_set" in kinds
    assert "serving_rate_compact" in kinds
    compact_row = [row for kind, row in frames if kind == "serving_rate_compact"][0]
    assert compact_row["snapshot_id"] == "snapshot"
    assert compact_row["plan_id"] == "plan"
    assert compact_row["billing_code"] == "99213"
    assert compact_row["provider_count"] == 1
    assert b"PTG2_SCANNER_PROGRESS" in completed.stderr


def test_ptg2_rust_compact_serving_mode_can_write_copy_file(tmp_path):
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    copy_path = tmp_path / "compact.copy"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    }
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(copy_path),
        "HLTHPRT_PTG2_RUST_SPLIT_IN_NETWORK": "true",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert copy_path.exists()
    copy_lines = copy_path.read_text().splitlines()
    assert len(copy_lines) == 1
    fields = copy_lines[0].split("\t")
    assert fields[1] == "snapshot"
    assert fields[2] == "plan"
    assert fields[8] == "99213"
    assert fields[12] == "1"

    assert b"serving_rate_compact" not in completed.stdout
    assert b"compact_copy_file" in completed.stdout
