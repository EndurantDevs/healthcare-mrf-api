# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
import importlib
import re

import pytest

partd_network = importlib.import_module("process.partd_formulary_network")


def test_activity_chunk_progress_falls_back_to_chunks_and_unknown():
    chunk_progress = partd_network.ActivityChunkProgress(
        total_chunks=4,
        done_chunks=5,
        row_count=0,
        bytes_total=0,
        bytes_done=0,
        started_chunks=4,
    )
    unknown_progress = partd_network.ActivityChunkProgress(
        total_chunks=0,
        done_chunks=0,
        row_count=0,
        bytes_total=0,
        bytes_done=0,
        started_chunks=0,
    )

    assert chunk_progress.pct() == 99.9
    assert unknown_progress.pct() is None


def test_partd_identifier_normalization_handles_invalid_and_missing_values(
    monkeypatch,
):
    monkeypatch.setattr(
        partd_network.secrets,
        "token_hex",
        lambda _size: "deadbeef",
    )

    assert partd_network._normalize_run_id(" run/id:one ") == "run_id_one"
    assert partd_network._normalize_run_id("***").endswith("_deadbeef")
    assert partd_network._normalize_run_id(None).endswith("_deadbeef")
    assert partd_network._normalize_import_id(" import/id ") == "_import_id_"
    assert len(partd_network._normalize_import_id("a" * 80)) == 32
    assert re.fullmatch(r"\d{8}", partd_network._normalize_import_id(None))


@pytest.mark.parametrize(
    ("raw_value", "expected_date"),
    [
        ("   ", None),
        ("2026-13", None),
        ("2026-07-29T12:30:00Z", datetime.date(2026, 7, 29)),
        ("released-20260730-final", datetime.date(2026, 7, 30)),
        ("released-20260230-final", None),
        ("not-a-date", None),
    ],
)
def test_partd_date_parser_covers_fallback_and_invalid_values(
    raw_value,
    expected_date,
):
    assert partd_network._parse_date(raw_value) == expected_date


def test_partd_numeric_and_npi_parsers_fail_closed():
    assert partd_network._to_int("   ") is None
    assert partd_network._to_int("1,234.9") == 1234
    assert partd_network._to_int("unknown") is None
    assert partd_network._to_float("   ") is None
    assert partd_network._to_float("1,234.5") == 1234.5
    assert partd_network._to_float("unknown") is None
    assert partd_network._to_npi(None) is None
    assert partd_network._to_npi("1-1234567890-0") == 1234567890
    assert partd_network._to_npi("1-2345678901") == 2345678901
    assert partd_network._to_npi("123") is None
    assert partd_network._to_npi("0000000000") is None


def test_partd_cost_field_matching_skips_invalid_duplicates_and_caps_results():
    cost_by_field = {
        "": "1.00",
        "Unrelated": "2.00",
        "Copay Amount": "invalid",
        "copay-amount": "3.00",
        **{f"Cost {index}": str(index) for index in range(40)},
    }

    matched_costs = partd_network._match_cost_fields(cost_by_field)

    assert len(matched_costs) == 32
    assert matched_costs[0] == ("copayamount", 3.0)


def test_partd_catalog_helpers_select_valid_dated_zip_distributions():
    catalog_by_field = {
        "dataset": [
            {"title": "Other"},
            {
                "title": "Target Dataset",
                "distribution": [
                    {},
                    {"downloadURL": ""},
                    {"downloadURL": "https://files.example.test/readme.txt"},
                    {"downloadURL": ("https://files.example.test/bad-20260230.zip")},
                    {
                        "downloadURL": "https://files.example.test/old.zip",
                        "issued": "2026-06-01",
                    },
                    {"downloadURL": ("https://files.example.test/new-20260701.zip")},
                ],
            },
        ]
    }

    dataset_by_field = partd_network._resolve_dataset(
        catalog_by_field,
        " target dataset ",
    )
    distributions_by_field = partd_network._zip_distributions(dataset_by_field)

    assert [
        distribution_by_field["artifact_name"]
        for distribution_by_field in distributions_by_field
    ] == [
        "new-20260701.zip",
        "old.zip",
    ]
    with pytest.raises(LookupError, match="Missing Dataset"):
        partd_network._resolve_dataset(catalog_by_field, "Missing Dataset")


def test_partd_scalar_progress_helpers_cover_empty_and_byte_inputs():
    assert partd_network._safe_int(None, 7) == 7
    assert partd_network._safe_int(b"12") == 12
    assert partd_network._safe_int("invalid", 3) == 3
    assert partd_network._format_bytes(0) == "0 B"
    assert partd_network._format_bytes(1024) == "1.0 KiB"
    assert partd_network._format_bytes(1024**2) == "1.0 MiB"
    assert partd_network._redis_hash_int_sum(None) == 0
    assert partd_network._redis_hash_int_sum([("ignored", 5)]) == 0
    assert (
        partd_network._redis_hash_int_sum(
            {b"kept": b"4", b"excluded": b"9"},
            exclude={"excluded"},
        )
        == 4
    )
