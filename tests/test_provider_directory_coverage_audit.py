from scripts.research import provider_directory_coverage_audit as audit


def test_provider_directory_coverage_audit_parse_args_accepts_ptg_plan_filter():
    args = audit.parse_args(["--ptg-plan-id", "010854205"])

    assert args.ptg_plan_id == "010854205"


def test_provider_directory_coverage_audit_includes_endpoint_table():
    assert "provider_directory_endpoint" in audit.PROVIDER_DIRECTORY_RESOURCE_TABLES
    assert audit.PROVIDER_DIRECTORY_RESOURCE_TABLE_BY_TYPE["Endpoint"] == "provider_directory_endpoint"


def test_provider_directory_coverage_audit_parse_args_accepts_skip_ptg():
    args = audit.parse_args(["--skip-ptg"])

    assert args.skip_ptg is True


def test_provider_directory_coverage_audit_parse_args_accepts_pod_safe_skip_flags():
    args = audit.parse_args(
        [
            "--skip-unified",
            "--skip-ptg",
            "--skip-network-resolution",
            "--skip-top-source-yield",
            "--skip-advertised-resource-gaps",
            "--skip-valid-zero-row-sources",
            "--skip-ptg-corroboration",
            "--skip-ptg-network-overlap",
            "--force-ptg-live-view-scans",
            "--statement-timeout-ms",
            "5000",
        ]
    )

    assert args.skip_unified is True
    assert args.skip_ptg is True
    assert args.skip_network_resolution is True
    assert args.skip_top_source_yield is True
    assert args.skip_advertised_resource_gaps is True
    assert args.skip_valid_zero_row_sources is True
    assert args.skip_ptg_corroboration is True
    assert args.skip_ptg_network_overlap is True
    assert args.force_ptg_live_view_scans is True
    assert args.statement_timeout_ms == 5000


def test_provider_directory_coverage_audit_skipped_ptg_summary_shape():
    summary = audit._skipped_ptg_summary()

    assert summary["ptg_unified_address"] == {
        "available": False,
        "skipped": True,
        "reason": "disabled by --skip-ptg",
    }
    assert summary["ptg_corroboration"] == {
        "available": False,
        "skipped": True,
        "reason": "disabled by --skip-ptg",
    }
    assert summary["ptg_network_name_overlap"] == {
        "available": False,
        "skipped": True,
        "reason": "disabled by --skip-ptg",
        "samples": [],
    }


def test_provider_directory_coverage_audit_ptg_network_overlap_sql_uses_serving_network_names():
    sql = audit._ptg_network_name_overlap_cte_sql("mrf", ptg_plan_filter="AND plan_ids.plan_id = $1")

    assert '"mrf"."ptg_provider_directory_address_corroboration"' in sql
    assert '"mrf"."ptg2_serving_rate_compact"' in sql
    assert "provider_directory_network_names" in sql
    assert "rates.network_names" in sql
    assert "plan_pairs.snapshot_id = rates.snapshot_id" in sql
    assert "plan_pairs.plan_id = rates.plan_id" in sql
    assert "AND plan_ids.plan_id = $1" in sql


def test_provider_directory_coverage_audit_ref_match_accepts_absolute_url_suffixes():
    sql = audit._sql_ref_matches_resource("refs.ref", "Organization", "org.resource_id")

    assert "refs.ref IN (org.resource_id, 'Organization/' || org.resource_id)" in sql
    assert "refs.ref LIKE '%/Organization/' || org.resource_id" in sql


def test_provider_directory_coverage_audit_gaps_when_requested_plan_has_no_ptg_rows():
    report = {
        "ptg_plan_filter": "010854205",
        "ptg_summary": {
            "ptg_unified_address": {"available": True, "ptg_unified_address_rows": 0},
            "ptg_corroboration": {"available": True, "corroboration_rows": 0},
        },
    }

    assert audit._derive_gaps(report) == [
        "Requested PTG plan `010854205` has no PTG-associated unified address rows."
    ]


def test_provider_directory_coverage_audit_gaps_when_requested_plan_lacks_corroboration():
    report = {
        "ptg_plan_filter": "codex_plan_a",
        "ptg_summary": {
            "ptg_unified_address": {"available": True, "ptg_unified_address_rows": 10},
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


def test_provider_directory_coverage_audit_gaps_for_advertised_resource_without_rows():
    report = {
        "advertised_resource_gap_summary": {
            "available": True,
            "advertised_without_rows": 3,
            "resources": [
                {
                    "resource_type": "Location",
                    "advertised_without_rows_count": 2,
                },
                {
                    "resource_type": "Endpoint",
                    "advertised_without_rows_count": 1,
                },
            ],
        }
    }

    assert audit._derive_gaps(report) == [
        "Provider Directory advertised-resource imports have supported sources with zero rows: Location=2, Endpoint=1."
    ]


def test_provider_directory_coverage_audit_gaps_for_resource_level_auth_blocks():
    report = {
        "valid_sources_without_resource_rows": {
            "available": True,
            "source_count": 3,
            "resource_auth_required_source_count": 2,
        },
        "advertised_resource_gap_summary": {
            "available": True,
            "advertised_without_rows": 5,
            "advertised_auth_blocked_without_rows": 4,
            "resources": [
                {
                    "resource_type": "Location",
                    "advertised_without_rows_count": 3,
                    "auth_blocked_without_rows_count": 2,
                },
                {
                    "resource_type": "Practitioner",
                    "advertised_without_rows_count": 2,
                    "auth_blocked_without_rows_count": 2,
                },
            ],
        },
    }

    assert audit._derive_gaps(report) == [
        "2 Provider Directory source(s) have valid metadata but resource endpoints require auth.",
        "3 Provider Directory source(s) have valid metadata but no imported resource rows.",
        "Provider Directory advertised-resource imports are auth-blocked after metadata success: Location=2, Practitioner=2.",
        "Provider Directory advertised-resource imports have supported sources with zero rows: Location=3, Practitioner=2.",
    ]


def test_provider_directory_coverage_audit_gaps_for_non_fhir_credential_gateways():
    report = {
        "source_summary": {
            "available": True,
            "live_auth_required_count": 329,
            "live_credential_or_gateway_non_fhir_count": 263,
            "never_probed_count": 0,
        },
        "capability_status_counts": [{"probe_status": "valid_non_fhir", "count": 299}],
    }

    gaps = audit._derive_gaps(report)

    assert "329 Provider Directory sources require auth/registration before full import." in gaps
    assert (
        "263 Provider Directory non-FHIR probe responses look like credentialed/onboarding gateway responses."
        in gaps
    )
    assert "299 seed URLs responded but did not expose a FHIR CapabilityStatement." in gaps


def test_provider_directory_coverage_audit_gaps_for_zero_ptg_network_name_overlap():
    report = {
        "ptg_summary": {
            "ptg_network_name_overlap": {
                "available": True,
                "provider_directory_plan_network_names": 7,
                "matched_plan_network_names": 0,
            }
        }
    }

    assert audit._derive_gaps(report) == [
        "Provider Directory network names are present for PTG plan pairs, but none match PTG serving network_names."
    ]


def test_provider_directory_coverage_audit_markdown_includes_ptg_network_overlap():
    markdown = audit.render_markdown(
        {
            "generated_at": "2026-06-28T00:00:00Z",
            "schema": "mrf",
            "ptg_plan_filter": None,
            "ptg_summary": {
                "ptg_corroboration": {"available": True, "corroboration_rows": 10},
                "ptg_network_name_overlap": {
                    "available": True,
                    "matched_plan_network_names": 4,
                    "provider_directory_plan_network_names": 10,
                    "provider_directory_network_match_pct": 40.0,
                    "matched_plan_pairs": 3,
                    "plan_pairs_with_both_network_sets": 5,
                    "plan_pair_match_pct": 60.0,
                    "samples": [
                        {
                            "provider_directory_org_name": "Example Payer",
                            "provider_directory_network_name": "C2",
                            "plan_pair_count": 2,
                            "sample_ptg_network_names": ["Choice Plus", "Nexus"],
                        }
                    ],
                },
            },
        }
    )

    assert "- PTG/FHIR network-name overlap: `4` / `10` provider-directory plan-network names (40.0%); matched plan pairs `3` / `5` (60.0%)" in markdown
    assert "## PTG/FHIR Network Name Overlap Gaps" in markdown
    assert "| Example Payer | `C2` | 2 | Choice Plus, Nexus |" in markdown


def test_provider_directory_coverage_audit_markdown_includes_unified_source_id_and_country_counts():
    markdown = audit.render_markdown(
        {
            "generated_at": "2026-06-28T00:00:00Z",
            "schema": "mrf",
            "ptg_plan_filter": None,
            "source_summary": {
                "available": True,
                "source_count": 734,
                "live_valid_count": 68,
                "live_valid_pct": 9.26,
                "live_auth_required_count": 329,
                "auth_required_pct": 44.82,
                "live_credential_or_gateway_non_fhir_count": 263,
                "live_valid_non_fhir_count": 299,
                "api_base_count": 728,
                "api_base_pct": 99.18,
            },
            "credential_onboarding_backlog": {
                "available": True,
                "blocked_source_count": 592,
                "group_count": 2,
                "groups": [
                    {
                        "source_host": "apps.availity.com",
                        "probe_status": "valid_non_fhir",
                        "auth_type": "OAuth2/SMART",
                        "reason": "onboarding_gateway",
                        "source_count": 263,
                        "sample_payers": ["Aetna / Provider Directory", "Availity payer"],
                    },
                    {
                        "source_host": "partners.centene.com",
                        "probe_status": "auth_required",
                        "auth_type": "token",
                        "reason": "auth_required",
                        "source_count": 329,
                        "sample_payers": ["Centene"],
                    },
                ],
            },
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
            "advertised_resource_gap_summary": {
                "available": True,
                "advertised_without_rows": 2,
                "advertised_auth_blocked_without_rows": 0,
                "advertised_source_resources": 5,
                "advertised_with_rows_pct": 60.0,
                "resources": [
                    {
                        "resource_type": "Location",
                        "advertised_source_count": 3,
                        "source_with_rows_count": 2,
                        "advertised_without_rows_count": 1,
                        "auth_blocked_without_rows_count": 0,
                        "resource_error_counts": {},
                    },
                    {
                        "resource_type": "Endpoint",
                        "advertised_source_count": 2,
                        "source_with_rows_count": 1,
                        "advertised_without_rows_count": 1,
                        "auth_blocked_without_rows_count": 0,
                        "resource_error_counts": {},
                    },
                ],
            },
            "alias_fanout_summary": {
                "available": True,
                "resource_count": 1,
                "excess_source_resource_rows": 2720000,
                "resources": [
                    {
                        "resource_type": "Practitioner",
                        "excess_source_resource_rows": 2720000,
                        "samples": [
                            {
                                "api_base": "https://fhir.humana.com/api",
                                "sample_org_name": "Humana Inc.",
                                "sample_plan_name": "Humana Choice PPO",
                                "source_count": 18,
                                "source_resource_rows": 2880000,
                                "distinct_resource_ids": 160000,
                                "excess_source_resource_rows": 2720000,
                                "fanout_ratio": 18.0,
                            }
                        ],
                    }
                ],
            },
            "canonical_resource_summary": {
                "available": True,
                "canonical_rows": 160000,
                "source_edge_rows": 2880000,
                "edge_surplus_rows": 2720000,
                "source_count": 18,
                "canonical_api_base_count": 1,
                "resources": [
                    {
                        "resource_type": "Practitioner",
                        "canonical_rows": 160000,
                        "source_edge_rows": 2880000,
                        "edge_surplus_rows": 2720000,
                        "source_count": 18,
                        "canonical_api_base_count": 1,
                    }
                ],
            },
        }
    )

    assert "- Provider Directory rows with source record IDs: `282912` (100.0%)" in markdown
    assert "- non-FHIR credential/gateway responses: `263` / `299` valid_non_fhir" in markdown
    assert "- credential/onboarding backlog: `592` source(s) across `2` group(s)" in markdown
    assert "- Provider Directory rows with country `001`: `0`" in markdown
    assert "- PTG corroboration: skipped (disabled by --skip-ptg)" in markdown
    assert "- advertised resource/source gaps: `2` / `5` (60.0% with rows); auth-blocked after metadata: `0`" in markdown
    assert "- alias fan-out excess source/resource rows: `2720000` across `1` resource type(s)" in markdown
    assert "## Advertised Resource Import Gaps" in markdown
    assert "| `Endpoint` | 2 | 1 | 1 | 0 | `` |" in markdown
    assert "## Credential/Onboarding Backlog" in markdown
    assert "| `apps.availity.com` | `valid_non_fhir` | `OAuth2/SMART` | `onboarding_gateway` | 263 | Aetna / Provider Directory, Availity payer |" in markdown
    assert "## Alias Fan-Out" in markdown
    assert "| `Practitioner` | `https://fhir.humana.com/api` | 18 | 2880000 | 160000 | 2720000 | 18.0 | Humana Inc. |" in markdown
    assert "- canonical resource storage: `160000` canonical row(s), `2880000` source edge row(s), `2720000` edge surplus row(s)" in markdown
    assert "## Canonical Resource Storage" in markdown
    assert "| `Practitioner` | 160000 | 2880000 | 2720000 | 18 | 1 |" in markdown


def test_provider_directory_coverage_audit_markdown_includes_skipped_live_sections():
    markdown = audit.render_markdown(
        {
            "generated_at": "2026-06-28T00:00:00Z",
            "schema": "mrf",
            "ptg_plan_filter": None,
            "unified_summary": {"available": False, "skipped": True, "reason": "disabled by --skip-unified"},
            "ptg_summary": audit._skipped_ptg_summary(),
            "network_resolution_summary": {
                "available": False,
                "skipped": True,
                "reason": "disabled by --skip-network-resolution",
            },
            "advertised_resource_gap_summary": {
                "available": False,
                "skipped": True,
                "reason": "disabled by --skip-advertised-resource-gaps",
            },
            "alias_fanout_summary": {
                "available": False,
                "skipped": True,
                "reason": "disabled by --skip-top-source-yield",
            },
        }
    )

    assert "- unified Provider Directory rows: skipped (disabled by --skip-unified)" in markdown
    assert "- PTG corroboration: skipped (disabled by --skip-ptg)" in markdown
    assert "- network resolution: skipped (disabled by --skip-network-resolution)" in markdown
    assert "- advertised resource/source gaps: skipped (disabled by --skip-advertised-resource-gaps)" in markdown
    assert "- alias fan-out: skipped (disabled by --skip-top-source-yield)" in markdown
