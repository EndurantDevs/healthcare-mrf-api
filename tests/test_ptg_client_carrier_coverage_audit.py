# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace

from scripts.research.ptg_client_carrier_coverage_audit import (
    _add_optional_report_sections,
    audit_carrier_rows,
    audit_non_importable_carrier_rows,
    has_catalog_source_candidate,
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


def test_audit_carrier_rows_respects_candidate_benefit_lines():
    rows = [
        {
            "MEDICAL_CARRIERS": "Shared Carrier",
            "DENTAL_CARRIERS": "Shared Carrier",
            "VISION_CARRIERS": "Shared Carrier",
        },
    ]
    candidates = [
        SimpleNamespace(name="Shared Carrier", benefit_lines=("medical",)),
        SimpleNamespace(name="Shared Carrier", benefit_lines=("vision",)),
    ]

    stats, unmatched = audit_carrier_rows(
        rows,
        all_candidates=candidates,
        importable_candidates=candidates,
        matcher=lambda candidate, carrier: candidate.name.lower() == carrier.lower(),
    )
    by_line = {item.line: item for item in stats}

    assert by_line["medical"].importable_mentions == 1
    assert by_line["dental"].importable_mentions == 0
    assert by_line["dental"].catalog_mentions == 0
    assert by_line["vision"].importable_mentions == 1
    assert unmatched["dental"] == [("Shared Carrier", 1)]


def test_catalog_source_filter_excludes_master_list_placeholders():
    assert has_catalog_source_candidate(
        SimpleNamespace(index_url="https://example.test/index.json")
    )
    assert has_catalog_source_candidate(
        SimpleNamespace(human_url="https://example.test/mrf")
    )
    assert not has_catalog_source_candidate(
        SimpleNamespace(index_url=None, human_url=None)
    )


def test_audit_carrier_rows_uses_confirmed_ancillary_blue_benefit_lines():
    rows = [
        {
            "MEDICAL_CARRIERS": "Example All Line",
            "DENTAL_CARRIERS": "Example All Line\nExample Dental Blue\nExample Vision Blue",
            "VISION_CARRIERS": "Example All Line\nExample Vision Blue\nExample Prairie Blue\nExample Dental Blue",
        },
    ]
    candidates = [
        SimpleNamespace(name="Example All Line", benefit_lines=("medical", "dental", "vision")),
        SimpleNamespace(name="Example Dental Blue", benefit_lines=("medical", "dental")),
        SimpleNamespace(name="Example Vision Blue", benefit_lines=("medical", "vision")),
        SimpleNamespace(name="Example Prairie Blue", benefit_lines=("medical", "vision")),
    ]

    stats, unmatched = audit_carrier_rows(
        rows,
        all_candidates=candidates,
        importable_candidates=candidates,
        matcher=lambda candidate, carrier: candidate.name.lower() == carrier.lower(),
    )
    by_line = {item.line: item for item in stats}

    assert by_line["medical"].importable_mentions == 1
    assert by_line["dental"].importable_mentions == 2
    assert by_line["vision"].importable_mentions == 3
    assert unmatched["dental"] == [("Example Vision Blue", 1)]
    assert unmatched["vision"] == [("Example Dental Blue", 1)]


def test_audit_cache_reuses_carrier_match():
    client_rows = [
        {
            "MEDICAL_CARRIERS": "Repeated Carrier",
            "DENTAL_CARRIERS": "",
            "VISION_CARRIERS": "",
        },
        {
            "MEDICAL_CARRIERS": "Repeated Carrier",
            "DENTAL_CARRIERS": "",
            "VISION_CARRIERS": "",
        },
        {
            "MEDICAL_CARRIERS": "REPEATED  CARRIER",
            "DENTAL_CARRIERS": "",
            "VISION_CARRIERS": "",
        },
    ]
    matcher_calls = []

    def matcher(candidate, carrier):
        matcher_calls.append((candidate.name, carrier))
        return "match" if candidate.name == "Repeated Carrier" else ""

    stats, unmatched = audit_carrier_rows(
        client_rows,
        all_candidates=[SimpleNamespace(name="Repeated Carrier")],
        importable_candidates=[SimpleNamespace(name="Repeated Carrier")],
        matcher=matcher,
    )

    by_line = {coverage_stat.line: coverage_stat for coverage_stat in stats}
    expected_distinct_importable = 1
    assert by_line["medical"].importable_mentions == 3
    assert by_line["medical"].distinct_importable == expected_distinct_importable
    assert unmatched["medical"] == []
    assert matcher_calls == [("Repeated Carrier", "Repeated Carrier")]


def test_audit_non_importable_carrier_rows_reports_evidence_only_matches():
    client_rows = [
        {
            "MEDICAL_CARRIERS": "Importable Medical\nEvidence Medical",
            "DENTAL_CARRIERS": "Evidence Dental\nMissing Dental",
            "VISION_CARRIERS": "Importable Vision",
        },
        {
            "MEDICAL_CARRIERS": "Evidence Medical",
            "DENTAL_CARRIERS": "Evidence Dental",
            "VISION_CARRIERS": "",
        },
    ]

    non_importable = audit_non_importable_carrier_rows(
        client_rows,
        all_candidates=[
            SimpleNamespace(name="Importable Medical"),
            SimpleNamespace(name="Evidence Medical"),
            SimpleNamespace(name="Evidence Dental"),
            SimpleNamespace(name="Importable Vision"),
        ],
        importable_candidates=[
            SimpleNamespace(name="Importable Medical"),
            SimpleNamespace(name="Importable Vision"),
        ],
        matcher=lambda candidate, carrier: candidate.name.lower() == carrier.lower(),
    )

    assert non_importable["medical"] == [("Evidence Medical", 2)]
    assert non_importable["dental"] == [("Evidence Dental", 2)]
    assert non_importable["vision"] == []


def test_optional_sections_redact_private_carrier_labels():
    report_by_section = {}
    client_rows = [
        {
            "MEDICAL_CARRIERS": "Private Evidence\nPrivate Missing",
            "DENTAL_CARRIERS": "",
            "VISION_CARRIERS": "",
        }
    ]
    parsed_args = SimpleNamespace(
        show_unmatched=True,
        show_non_importable=False,
        top_unmatched=5,
        top_non_importable=5,
        redact_labels=True,
    )

    _add_optional_report_sections(
        report_by_section,
        parsed_args=parsed_args,
        client_rows=client_rows,
        all_candidates=[SimpleNamespace(name="Private Evidence")],
        importable_candidates=[],
        unmatched={"medical": [("Private Missing", 1)]},
    )

    unmatched_carrier = report_by_section["top_unmatched"]["medical"][0]["carrier"]
    assert unmatched_carrier.startswith("carrier:")
    assert "Private" not in unmatched_carrier
