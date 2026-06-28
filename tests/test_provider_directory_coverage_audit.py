from scripts.research import provider_directory_coverage_audit as audit


def test_provider_directory_coverage_audit_parse_args_accepts_ptg_plan_filter():
    args = audit.parse_args(["--ptg-plan-id", "010854205"])

    assert args.ptg_plan_id == "010854205"


def test_provider_directory_coverage_audit_parse_args_accepts_skip_ptg():
    args = audit.parse_args(["--skip-ptg"])

    assert args.skip_ptg is True


def test_provider_directory_coverage_audit_skipped_ptg_summary_shape():
    summary = audit._skipped_ptg_summary()

    assert summary["ptg_address"] == {
        "available": False,
        "skipped": True,
        "reason": "disabled by --skip-ptg",
    }
    assert summary["ptg_corroboration"] == {
        "available": False,
        "skipped": True,
        "reason": "disabled by --skip-ptg",
    }


def test_provider_directory_coverage_audit_ref_match_accepts_absolute_url_suffixes():
    sql = audit._sql_ref_matches_resource("refs.ref", "Organization", "org.resource_id")

    assert "refs.ref IN (org.resource_id, 'Organization/' || org.resource_id)" in sql
    assert "refs.ref LIKE '%/Organization/' || org.resource_id" in sql


def test_provider_directory_coverage_audit_gaps_when_requested_plan_has_no_ptg_rows():
    report = {
        "ptg_plan_filter": "010854205",
        "ptg_summary": {
            "ptg_address": {"available": True, "ptg_address_rows": 0},
            "ptg_corroboration": {"available": True, "corroboration_rows": 0},
        },
    }

    assert audit._derive_gaps(report) == [
        "Requested PTG plan `010854205` has no ptg_address rows in this database."
    ]


def test_provider_directory_coverage_audit_gaps_when_requested_plan_lacks_corroboration():
    report = {
        "ptg_plan_filter": "codex_plan_a",
        "ptg_summary": {
            "ptg_address": {"available": True, "ptg_address_rows": 10},
            "ptg_corroboration": {"available": True, "corroboration_rows": 0},
        },
    }

    assert audit._derive_gaps(report) == [
        "Requested PTG plan `codex_plan_a` has no Provider Directory address corroboration rows."
    ]


def test_provider_directory_coverage_audit_gaps_for_missing_unified_source_ids_and_numeric_country():
    report = {
        "unified_summary": {
            "available": True,
            "provider_directory_rows": 10,
            "provider_directory_source_record_id_rows": 7,
            "provider_directory_country_001_rows": 2,
        },
    }

    gaps = audit._derive_gaps(report)

    assert "3 Provider Directory unified-address rows lack retained FHIR source record IDs." in gaps
    assert "2 Provider Directory unified-address rows still expose country_code `001`." in gaps


def test_provider_directory_coverage_audit_markdown_includes_unified_source_id_and_country_counts():
    markdown = audit.render_markdown(
        {
            "generated_at": "2026-06-28T00:00:00Z",
            "schema": "mrf",
            "ptg_plan_filter": None,
            "unified_summary": {
                "available": True,
                "provider_directory_rows": 282912,
                "provider_directory_keyed_rows": 282912,
                "provider_directory_keyed_pct": 100.0,
                "provider_directory_phone_rows": 282000,
                "provider_directory_phone_pct": 99.68,
                "provider_directory_source_record_id_rows": 282912,
                "provider_directory_source_record_id_pct": 100.0,
                "provider_directory_country_001_rows": 0,
            },
            "ptg_summary": audit._skipped_ptg_summary(),
        }
    )

    assert "- Provider Directory rows with source record IDs: `282912` (100.0%)" in markdown
    assert "- Provider Directory rows with country `001`: `0`" in markdown
    assert "- PTG corroboration: skipped (disabled by --skip-ptg)" in markdown
