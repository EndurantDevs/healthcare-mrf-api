# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from scripts.research.ptg_client_carrier_coverage_audit import (
    audit_carrier_rows,
    is_placeholder_carrier,
    split_carrier_cell,
)


def test_split_carrier_cell_prefers_json_array_and_preserves_commas():
    assert split_carrier_cell('["Example Carrier, Inc.", "Example Dental"]') == [
        "Example Carrier, Inc.",
        "Example Dental",
    ]
    assert split_carrier_cell("Example Medical\nExample Vision; Example Dental") == [
        "Example Medical",
        "Example Vision",
        "Example Dental",
    ]


def test_placeholder_carrier_detection():
    assert is_placeholder_carrier("N/A")
    assert is_placeholder_carrier("not offered")
    assert is_placeholder_carrier("-")
    assert is_placeholder_carrier("self-funded")
    assert is_placeholder_carrier("Employer Sponsored")
    assert not is_placeholder_carrier("Example Carrier")


def test_audit_carrier_rows_reports_importable_catalog_and_unmatched_counts():
    rows = [
        {
            "MEDICAL_CARRIERS": '["Example Medical", "Example Evidence Only", "Missing Medical"]',
            "DENTAL_CARRIERS": '["Example Dental", "N/A"]',
            "VISION_CARRIERS": "Example Vision",
        },
        {
            "MEDICAL_CARRIERS": '["Example Medical"]',
            "DENTAL_CARRIERS": "",
            "VISION_CARRIERS": '["Missing Vision"]',
        },
    ]
    all_candidates = ["Example Medical", "Example Evidence Only", "Example Dental", "Example Vision"]
    importable_candidates = ["Example Medical", "Example Dental"]

    stats, unmatched = audit_carrier_rows(
        rows,
        all_candidates=all_candidates,
        importable_candidates=importable_candidates,
        matcher=lambda candidate, carrier: str(candidate).lower() == carrier.lower(),
    )
    by_line = {item.line: item for item in stats}

    assert by_line["medical"].mentions_total == 4
    assert by_line["medical"].importable_mentions == 2
    assert by_line["medical"].catalog_mentions == 3
    assert by_line["medical"].unmatched_mentions == 1
    assert by_line["medical"].distinct_total == 3
    assert by_line["medical"].distinct_importable == 1
    assert by_line["medical"].distinct_catalog == 2
    assert by_line["medical"].distinct_unmatched == 1
    assert unmatched["medical"] == [("Missing Medical", 1)]

    assert by_line["dental"].mentions_total == 2
    assert by_line["dental"].placeholders == 1
    assert by_line["dental"].importable_mentions == 1
    assert by_line["dental"].unmatched_mentions == 0

    assert by_line["vision"].mentions_total == 2
    assert by_line["vision"].importable_mentions == 0
    assert by_line["vision"].catalog_mentions == 1
    assert by_line["vision"].unmatched_mentions == 1
    assert unmatched["vision"] == [("Missing Vision", 1)]
