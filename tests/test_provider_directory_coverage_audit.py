# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from scripts.research import provider_directory_coverage_audit as audit


def test_provider_directory_coverage_audit_derives_actionable_gaps():
    report = {
        "source_summary": {
            "available": True,
            "live_auth_required_count": 12,
            "never_probed_count": 1,
        },
        "capability_status_counts": [{"probe_status": "valid_non_fhir", "count": 3}],
        "unified_summary": {
            "available": True,
            "provider_directory_null_key_rows": 5,
        },
        "network_resolution_summary": {
            "available": True,
            "unresolved_network_refs": 7,
        },
        "valid_sources_without_resource_rows": {
            "available": True,
            "source_count": 2,
        },
        "ptg_summary": {
            "ptg_corroboration": {
                "available": True,
                "network_context_rows": 9,
                "resolved_network_match_rows": 0,
            }
        },
    }

    gaps = audit._derive_gaps(report)  # pylint: disable=protected-access

    assert "12 Provider Directory sources require auth/registration before full import." in gaps
    assert "1 Provider Directory source(s) have not been probed." in gaps
    assert "3 seed URLs responded but did not expose a FHIR CapabilityStatement." in gaps
    assert "5 Provider Directory unified-address rows still lack address_key." in gaps
    assert "7 distinct Provider Directory network refs are unresolved to FHIR Organization names." in gaps
    assert "2 Provider Directory source(s) have valid unauthenticated metadata but no imported resource rows." in gaps
    assert "PTG-overlap Provider Directory rows carry network refs" in gaps[-1]


def test_provider_directory_coverage_audit_markdown_includes_core_sections():
    report = {
        "generated_at": "2026-06-28T00:00:00Z",
        "schema": "mrf",
        "source_summary": {
            "available": True,
            "source_count": 734,
            "live_valid_count": 42,
            "live_valid_pct": 5.72,
            "live_auth_required_count": 329,
            "auth_required_pct": 44.82,
            "api_base_count": 728,
            "api_base_pct": 99.18,
        },
        "unified_summary": {
            "available": True,
            "provider_directory_rows": 616,
            "provider_directory_keyed_rows": 502,
            "provider_directory_keyed_pct": 81.49,
            "provider_directory_phone_rows": 407,
            "provider_directory_phone_pct": 66.07,
        },
        "ptg_summary": {
            "ptg_corroboration": {
                "available": True,
                "corroboration_rows": 193,
                "plan_context_match_rows": 0,
                "resolved_network_match_rows": 0,
            }
        },
        "network_resolution_summary": {
            "available": True,
            "resolved_network_refs": 4,
            "distinct_network_refs": 8,
            "resolved_network_ref_pct": 50.0,
            "top_unresolved_refs": [
                {
                    "org_name": "Example Payer",
                    "ref": "Organization/network-x",
                    "reference_count": 10,
                }
            ],
        },
        "capability_status_counts": [{"probe_status": "valid", "count": 42}],
        "top_source_yield": [
            {
                "org_name": "Example Payer",
                "last_probe_status": "valid",
                "resource_rows": 123,
                "resource_counts": {"location": 10},
            }
        ],
        "valid_sources_without_resource_rows": {
            "available": True,
            "source_count": 1,
            "samples": [
                {
                    "org_name": "Aetna",
                    "plan_name": "Aetna Better Health",
                    "auth_type": "OAuth2/SMART",
                    "canonical_api_base": "https://apif1.aetna.com/fhir/v1/providerdirectory",
                    "last_resource_import": {
                        "resources": {
                            "Practitioner": {"error": "http_401"},
                            "Location": {"error": "http_401"},
                        }
                    },
                }
            ],
        },
        "gaps": ["Example gap"],
    }

    markdown = audit.render_markdown(report)

    assert "# Provider Directory Coverage Audit" in markdown
    assert "- sources: `734`" in markdown
    assert "- keyed Provider Directory rows: `502` (81.49%)" in markdown
    assert "## Gaps" in markdown
    assert "| `valid` | 42 |" in markdown
    assert "| Example Payer | `Organization/network-x` | 10 |" in markdown
    assert "## Valid Metadata With No Imported Rows" in markdown
    assert "| Aetna | Aetna Better Health | `OAuth2/SMART` | `https://apif1.aetna.com/fhir/v1/providerdirectory` | `http_401` |" in markdown
    assert '`{"location": 10}`' in markdown


def test_provider_directory_coverage_audit_rejects_unsafe_identifier():
    with pytest.raises(ValueError):
        audit._validate_identifier("mrf;drop schema", label="schema")  # pylint: disable=protected-access


def test_provider_directory_coverage_audit_parse_skip_network_resolution():
    args = audit.parse_args(["--skip-network-resolution"])

    assert args.skip_network_resolution is True
