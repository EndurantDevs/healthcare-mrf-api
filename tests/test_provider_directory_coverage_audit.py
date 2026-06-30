from unittest.mock import AsyncMock

import pytest

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
            "--skip-canonical-resource-summary",
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
    assert args.skip_canonical_resource_summary is True
    assert args.skip_ptg_corroboration is True
    assert args.skip_ptg_network_overlap is True
    assert args.force_ptg_live_view_scans is True
    assert args.statement_timeout_ms == 5000


def test_provider_directory_coverage_audit_pod_safe_sets_expensive_skip_flags():
    args = audit.parse_args(["--pod-safe"])

    assert args.pod_safe is True
    assert args.skip_unified is True
    assert args.skip_ptg is True
    assert args.skip_network_resolution is True
    assert args.skip_top_source_yield is True
    assert args.skip_advertised_resource_gaps is True
    assert args.skip_valid_zero_row_sources is True
    assert args.skip_canonical_resource_summary is True


def test_provider_directory_coverage_audit_accepts_credential_backlog_json_format():
    args = audit.parse_args(["--format", "credential-backlog-json"])

    assert args.format == "credential-backlog-json"


def test_provider_directory_coverage_audit_accepts_credential_api_bases_json_format():
    args = audit.parse_args(["--format", "credential-api-bases-json"])

    assert args.format == "credential-api-bases-json"


def test_provider_directory_coverage_audit_accepts_credential_config_template_json_format():
    args = audit.parse_args(["--format", "credential-config-template-json"])

    assert args.format == "credential-config-template-json"


def test_provider_directory_coverage_audit_accepts_credential_priority_json_format():
    args = audit.parse_args(["--format", "credential-priority-json"])

    assert args.format == "credential-priority-json"


def test_provider_directory_coverage_audit_accepts_credential_config_file():
    args = audit.parse_args(["--credential-config-file", "/tmp/provider-directory-credentials.json"])

    assert args.credential_config_file == "/tmp/provider-directory-credentials.json"


def test_provider_directory_coverage_audit_endpoint_discovery_classifier():
    assert audit._looks_like_provider_directory_portal_target(
        source_host="www.uhc.com",
        api_base="https://www.uhc.com/legal/interoperability-apis",
    )
    assert audit._looks_like_provider_directory_portal_target(
        source_host="developer.kp.org",
        api_base=None,
    )
    assert audit._credential_backlog_endpoint_discovery_needed(
        {
            "source_host": "partners.centene.com",
            "portal_url": "https://partners.centene.com/apis",
        },
        api_base=None,
    )
    assert not audit._looks_like_provider_directory_portal_target(
        source_host="api.1up.health",
        api_base="https://api.1up.health/fhir-r4/payer",
    )
    assert not audit._looks_like_provider_directory_portal_target(
        source_host="fhir.cigna.com",
        api_base="https://fhir.cigna.com/ProviderDirectory/v1",
    )


def test_provider_directory_coverage_audit_source_sample_retains_portal_url_without_api_base():
    sample = audit._credential_source_sample(
        {
            "source_id": "pdfhir_uhc",
            "org_name": "UnitedHealthcare",
            "portal_url": "https://www.uhc.com/legal/interoperability-apis",
            "probe_status": "auth_required",
            "auth_type": "OAuth2 Client Credentials",
            "reason": "auth_required",
        },
        payer_label="UnitedHealthcare",
        api_base=None,
    )

    assert sample["api_base"] is None
    assert sample["portal_url"] == "https://www.uhc.com/legal/interoperability-apis"


@pytest.mark.asyncio
async def test_provider_directory_coverage_audit_credential_backlog_uses_seed_validation_fallback(monkeypatch):
    captured_sql = {}

    class FakeConn:
        async def fetch(self, sql):
            captured_sql["sql"] = sql
            return [
                {
                    "source_id": "pdfhir_auth_seed",
                    "org_name": "Auth Seed Payer",
                    "plan_name": "Provider Directory Retest",
                    "canonical_api_base": "https://api.1up.health/fhir-r4/authseed",
                    "api_base": "https://api.1up.health/fhir-r4/authseed",
                    "portal_url": None,
                    "source_host": "api.1up.health",
                    "probe_status": "auth_required",
                    "auth_type": "OAuth2 Client Credentials",
                    "reason": "auth_required",
                    "is_medicare_advantage": True,
                    "is_medicaid_mco": False,
                    "is_chip": False,
                    "is_qhp": False,
                }
            ]

    monkeypatch.setattr(audit, "_relation_exists", AsyncMock(return_value=True))

    backlog = await audit._credential_onboarding_backlog(
        FakeConn(),
        "mrf",
        sample_limit=5,
    )

    assert "COALESCE(NULLIF(last_probe_status, ''), NULLIF(last_validated_status, ''))" in captured_sql["sql"]
    assert backlog["blocked_source_count"] == 1
    assert backlog["credential_config_missing_source_count"] == 1
    assert backlog["credential_rule_candidate_source_count"] == 1
    assert backlog["endpoint_discovery_needed_source_count"] == 0
    assert backlog["groups"][0]["probe_status"] == "auth_required"
    assert backlog["groups"][0]["sample_sources"][0]["source_id"] == "pdfhir_auth_seed"


def test_provider_directory_coverage_audit_accepts_retest_results_path():
    args = audit.parse_args(["--retest-results-path", "/tmp/retest_results.json"])

    assert args.retest_results_path == "/tmp/retest_results.json"


def test_provider_directory_coverage_audit_retest_coverage_counts_current_redirected_and_missing():
    coverage = audit._source_catalog_retest_coverage(
        {
            "tested_at": "2026-06-03T17:43:09Z",
            "results": [
                {
                    "classification": "valid",
                    "org_name": "Cigna",
                    "api_base": "https://fhir.cigna.com/ProviderDirectory/v1",
                    "status_code": 200,
                    "payer_id": 62,
                },
                {
                    "classification": "valid",
                    "org_name": "Aetna R4",
                    "api_base": "https://fhir-ehr.cerner.com/r4/aetna",
                    "status_code": 200,
                    "payer_id": 299,
                },
                {
                    "classification": "valid_non_fhir",
                    "org_name": "Missing Public Provider App",
                    "api_base": "https://public-directory.example/providers",
                    "status_code": 200,
                    "payer_id": 999,
                },
                {
                    "classification": "auth_required",
                    "org_name": "Ignored Auth Payer",
                    "api_base": "https://auth.example/fhir",
                    "status_code": 403,
                    "payer_id": 1000,
                },
            ],
        },
        [
            {
                "source_id": "pdfhir_cigna",
                "org_name": "Cigna",
                "api_base": "https://fhir.cigna.com/ProviderDirectory/v1",
                "canonical_api_base": "https://fhir.cigna.com/ProviderDirectory/v1",
                "metadata_json": {},
            },
            {
                "source_id": "pdfhir_aetna",
                "org_name": "Aetna R4",
                "api_base": "https://apif1.aetna.com/fhir/v1/providerdirectory",
                "canonical_api_base": "https://apif1.aetna.com/fhir/v1/providerdirectory",
                "metadata_json": {
                    "provider_directory_previous_api_base": "https://fhir-ehr.cerner.com/r4/aetna",
                    "provider_directory_confirmed_base": "https://apif1.aetna.com/fhir/v1/providerdirectory",
                },
            },
            {
                "source_id": "pdfhir_auth",
                "org_name": "Ignored Auth Payer",
                "api_base": "https://auth.example/fhir",
                "canonical_api_base": "https://auth.example/fhir",
                "metadata_json": {},
            },
        ],
        sample_limit=3,
    )

    assert coverage["available"] is True
    assert coverage["tested_at"] == "2026-06-03T17:43:09Z"
    assert coverage["checked_result_count"] == 4
    assert coverage["covered_current_base"] == 2
    assert coverage["covered_by_redirect"] == 1
    assert coverage["missing_result_count"] == 1
    assert coverage["covered_count"] == 3
    assert coverage["classification_counts"] == {"auth_required": 1, "valid": 2, "valid_non_fhir": 1}
    assert coverage["importable_checked_result_count"] == 3
    assert coverage["importable_covered_count"] == 2
    assert coverage["importable_missing_result_count"] == 1
    assert coverage["credential_gated_checked_result_count"] == 1
    assert coverage["credential_gated_covered_count"] == 1
    assert coverage["credential_gated_missing_result_count"] == 0
    assert coverage["redirected_samples"] == [
        {
            "classification": "valid",
            "org_name": "Aetna R4",
            "api_base": "https://fhir-ehr.cerner.com/r4/aetna",
            "covered_by_source_id": "pdfhir_aetna",
            "covered_by_api_base": "https://apif1.aetna.com/fhir/v1/providerdirectory",
        }
    ]
    assert coverage["missing_samples"] == [
        {
            "classification": "valid_non_fhir",
            "org_name": "Missing Public Provider App",
            "api_base": "https://public-directory.example/providers",
            "status_code": 200,
            "payer_id": 999,
        }
    ]


def test_provider_directory_coverage_audit_renders_probe_timeout_backlog():
    report = {
        "generated_at": "2026-06-29T23:52:16Z",
        "schema": "mrf",
        "source_summary": {
            "available": True,
            "source_count": 10,
            "live_valid_count": 2,
            "live_valid_pct": 20.0,
            "live_auth_required_count": 3,
            "auth_required_pct": 30.0,
            "live_timeout_count": 4,
            "timeout_pct": 40.0,
            "live_credential_or_gateway_non_fhir_count": 0,
            "live_valid_non_fhir_count": 0,
            "api_base_count": 10,
            "api_base_pct": 100.0,
        },
        "probe_timeout_summary": {
            "available": True,
            "timeout_source_count": 4,
            "host_count": 1,
            "groups": [
                {
                    "source_host": "slow.example",
                    "auth_type": "none",
                    "source_count": 4,
                    "medicare_advantage_source_count": 1,
                    "medicaid_mco_source_count": 2,
                    "chip_source_count": 0,
                    "qhp_source_count": 1,
                    "sample_api_bases": ["https://slow.example/fhir"],
                    "sample_payers": ["Slow Payer / Medicaid"],
                    "sample_errors": ["source metadata probe exceeded 10s hard deadline"],
                }
            ],
        },
    }
    report["gaps"] = audit._derive_gaps(report)

    markdown = audit.render_markdown(report)

    assert "4 Provider Directory source probe(s) timed out across 1 host(s)." in report["gaps"]
    assert "- timed-out source probes: `4` (40.0%)" in markdown
    assert "- probe timeout backlog: `4` source(s) across `1` host(s)" in markdown
    assert "## Probe Timeout Backlog" in markdown
    assert "`slow.example`" in markdown


def test_provider_directory_coverage_audit_credential_spec_matches_importer_keys():
    config = {
        "defaults": {"headers": {"X-Default": "env:DEFAULT_TOKEN"}},
        "hosts": {"payer.example": {"headers": {"X-Host": "env:HOST_TOKEN"}}},
        "api_bases": {"https://payer.example/fhir": {"query_params": {"client_id": "env:CLIENT_ID"}}},
        "org_names": {"Example Payer": {"bearer_token": "env:BEARER_TOKEN"}},
        "sources": {"source_a": {"api_key": {"header": "Ocp-Apim-Subscription-Key", "value": "env:API_KEY"}}},
    }

    spec = audit._credential_spec_for_source(
        {
            "source_id": "source_a",
            "org_name": "Example Payer",
            "api_base": "https://payer.example/fhir/",
        },
        config,
    )

    assert spec["_matched_by"] == [
        "defaults",
        "hosts:payer.example",
        "api_bases:https://payer.example/fhir",
        "org_names:example payer",
        "sources:source_a",
    ]
    assert audit._credential_spec_has_material(spec) is True


def test_provider_directory_coverage_audit_credential_spec_disabled_rule_counts_missing():
    spec = audit._credential_spec_for_source(
        {"source_id": "source_a", "api_base": "https://payer.example/fhir"},
        {"defaults": {"headers": {"X-Default": "env:DEFAULT_TOKEN"}}, "sources": {"source_a": {"enabled": False}}},
    )

    assert spec == {}
    assert audit._credential_spec_has_material(spec) is False


def test_provider_directory_coverage_audit_credential_secret_status_tracks_missing_env(monkeypatch):
    monkeypatch.delenv("PAYER_DIRECTORY_CLIENT_ID", raising=False)
    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT_SECRET", "client-secret")

    status = audit._credential_secret_status(
        {
            "oauth2": {
                "token_url": "https://auth.example/token",
                "client_id": "env:PAYER_DIRECTORY_CLIENT_ID",
                "client_secret": "env:PAYER_DIRECTORY_CLIENT_SECRET",
            }
        }
    )

    assert status == {
        "has_material": True,
        "env_ref_count": 2,
        "missing_env_refs": ["PAYER_DIRECTORY_CLIENT_ID"],
        "ready": False,
    }


def test_provider_directory_coverage_audit_credential_secret_status_ready_when_env_present(monkeypatch):
    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT_ID", "client-id")
    monkeypatch.setenv("PAYER_DIRECTORY_CLIENT_SECRET", "client-secret")

    status = audit._credential_secret_status(
        {
            "oauth2": {
                "token_url": "https://auth.example/token",
                "client_id": "env:PAYER_DIRECTORY_CLIENT_ID",
                "client_secret": "env:PAYER_DIRECTORY_CLIENT_SECRET",
            }
        }
    )

    assert status["ready"] is True
    assert status["missing_env_refs"] == []


def test_provider_directory_coverage_audit_loads_candidate_credential_file_without_env_merge(tmp_path, monkeypatch):
    candidate = tmp_path / "candidate-provider-directory-credentials.json"
    candidate.write_text(
        '{"hosts":{"candidate.example":{"headers":{"X-Candidate":"env:CANDIDATE_TOKEN"}}}}',
        encoding="utf-8",
    )
    monkeypatch.setenv(
        audit.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        '{"hosts":{"env.example":{"headers":{"X-Env":"env:ENV_TOKEN"}}}}',
    )

    config, source = audit._load_credentials_config(
        credential_config_file=str(candidate)
    )

    assert source == "argument_file"
    assert sorted(config["hosts"]) == ["candidate.example"]


def test_provider_directory_coverage_audit_invalid_candidate_credential_file_is_empty(tmp_path):
    candidate = tmp_path / "bad-provider-directory-credentials.json"
    candidate.write_text("{bad json", encoding="utf-8")

    config, source = audit._load_credentials_config(
        credential_config_file=str(candidate)
    )

    assert config == {}
    assert source == "argument_file_invalid"


def test_provider_directory_coverage_audit_credential_backlog_export_is_non_secret():
    export = audit._credential_backlog_export(
        {
            "generated_at": "2026-06-29T00:00:00Z",
            "schema": "mrf",
            "credential_onboarding_backlog": {
                "available": True,
                "blocked_source_count": 12,
                "credential_config_available": False,
                "credential_config_source": "argument_file",
                "credential_configured_source_count": 2,
                "credential_config_missing_source_count": 10,
                "credential_secret_ready_source_count": 1,
                "credential_secret_missing_source_count": 1,
                "credential_rule_candidate_source_count": 12,
                "endpoint_discovery_needed_source_count": 0,
                "credential_missing_secret_env_vars": ["PAYER_DIRECTORY_CLIENT_ID"],
                "group_count": 1,
                "groups": [
                    {
                        "source_host": "api.payer.example",
                        "probe_status": "auth_required",
                        "auth_type": "OAuth2 Client Credentials",
                        "reason": "auth_required",
                        "source_count": 12,
                        "credential_configured_source_count": 2,
                        "credential_config_missing_source_count": 10,
                        "credential_secret_ready_source_count": 1,
                        "credential_secret_missing_source_count": 1,
                        "credential_rule_candidate_source_count": 12,
                        "endpoint_discovery_needed_source_count": 0,
                        "medicare_advantage_source_count": 7,
                        "medicaid_mco_source_count": 3,
                        "chip_source_count": 1,
                        "qhp_source_count": 2,
                        "sample_payers": ["Example Payer / Medicare Advantage"],
                        "sample_missing_credential_payers": ["Example Payer / Medicare Advantage"],
                        "sample_missing_secret_payers": ["Example Payer / Medicare Advantage"],
                        "sample_missing_secret_env_vars": ["PAYER_DIRECTORY_CLIENT_ID"],
                        "sample_source_ids": ["pdfhir_123"],
                        "sample_api_bases": ["https://api.payer.example/fhir"],
                        "sample_sources": [
                            {
                                "source_id": "pdfhir_123",
                                "payer": "Example Payer / Medicare Advantage",
                                "api_base": "https://api.payer.example/fhir",
                                "markets": {"medicare_advantage": True},
                            }
                        ],
                        "sample_missing_credential_sources": [
                            {
                                "source_id": "pdfhir_123",
                                "payer": "Example Payer / Medicare Advantage",
                                "api_base": "https://api.payer.example/fhir",
                            }
                        ],
                        "sample_missing_secret_sources": [
                            {
                                "source_id": "pdfhir_123",
                                "payer": "Example Payer / Medicare Advantage",
                                "api_base": "https://api.payer.example/fhir",
                            }
                        ],
                        "api_base_count": 1,
                        "sample_api_base_count": 1,
                        "api_base_sample_complete": True,
                    }
                ],
            },
        }
    )

    assert export["credential_config_missing_source_count"] == 10
    assert export["credential_config_source"] == "argument_file"
    assert export["credential_secret_ready_source_count"] == 1
    assert export["credential_secret_missing_source_count"] == 1
    assert export["credential_rule_candidate_source_count"] == 12
    assert export["endpoint_discovery_needed_source_count"] == 0
    assert export["credential_missing_secret_env_vars"] == ["PAYER_DIRECTORY_CLIENT_ID"]
    assert export["groups"][0]["sample_missing_secret_env_vars"] == ["PAYER_DIRECTORY_CLIENT_ID"]
    assert export["groups"][0]["host"] == "api.payer.example"
    assert export["groups"][0]["source_host"] == "api.payer.example"
    assert export["groups"][0]["credential_rule_candidate_source_count"] == 12
    assert export["groups"][0]["endpoint_discovery_needed_source_count"] == 0
    assert export["groups"][0]["medicare_advantage_source_count"] == 7
    assert export["groups"][0]["medicaid_mco_source_count"] == 3
    assert export["groups"][0]["chip_source_count"] == 1
    assert export["groups"][0]["qhp_source_count"] == 2
    assert export["groups"][0]["sample_source_ids"] == ["pdfhir_123"]
    assert export["groups"][0]["sample_api_bases"] == ["https://api.payer.example/fhir"]
    assert export["groups"][0]["sample_sources"][0]["source_id"] == "pdfhir_123"
    assert export["groups"][0]["sample_sources"][0]["markets"] == {"medicare_advantage": True}
    assert export["groups"][0]["sample_missing_credential_sources"][0]["source_id"] == "pdfhir_123"
    assert export["groups"][0]["sample_missing_secret_sources"][0]["source_id"] == "pdfhir_123"
    assert export["groups"][0]["api_base_count"] == 1
    assert export["groups"][0]["sample_api_base_count"] == 1
    assert export["groups"][0]["api_base_sample_complete"] is True
    assert export["groups"][0]["suggested_credential_rule"] == {
        "section": "hosts",
        "key": "api.payer.example",
        "template": {
            "oauth2": {
                "token_url": "env:PROVIDER_DIRECTORY_TOKEN_URL",
                "client_id": "env:PROVIDER_DIRECTORY_CLIENT_ID",
                "client_secret": "env:PROVIDER_DIRECTORY_CLIENT_SECRET",
                "scope": "system/*.read",
                "auth": "basic",
            }
        },
    }
    assert "secret-value" not in str(export)


def test_provider_directory_coverage_audit_credential_api_base_targets_export_is_actionable():
    export = audit._credential_api_base_targets_export(
        {
            "generated_at": "2026-06-29T00:00:00Z",
            "schema": "mrf",
            "credential_onboarding_backlog": {
                "available": True,
                "blocked_source_count": 30,
                "credential_config_available": False,
                "credential_config_source": "argument_file",
                "credential_configured_source_count": 0,
                "credential_config_missing_source_count": 30,
                "credential_secret_ready_source_count": 0,
                "credential_secret_missing_source_count": 0,
                "credential_rule_candidate_source_count": 3,
                "endpoint_discovery_needed_source_count": 0,
                "credential_missing_secret_env_vars": [],
                "groups": [
                    {
                        "source_host": "apps.availity.com",
                        "probe_status": "auth_required",
                        "auth_type": "OAuth2 Client Credentials",
                        "reason": "auth_required",
                        "credential_rule_candidate_source_count": 2,
                        "endpoint_discovery_needed_source_count": 0,
                        "api_base_targets": [
                            {
                                "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/a/r4",
                                "source_count": 2,
                                "credential_configured_source_count": 0,
                                "credential_config_missing_source_count": 2,
                                "credential_secret_ready_source_count": 0,
                                "credential_secret_missing_source_count": 0,
                                "credential_rule_candidate_source_count": 2,
                                "endpoint_discovery_needed_source_count": 0,
                                "medicare_advantage_source_count": 1,
                                "medicaid_mco_source_count": 1,
                                "chip_source_count": 0,
                                "qhp_source_count": 1,
                                "sample_payers": ["Availity OAuth payer"],
                                "sample_source_ids": ["pdfhir_oauth"],
                                "sample_missing_credential_payers": ["Availity OAuth payer"],
                                "sample_missing_secret_payers": [],
                                "sample_missing_secret_env_vars": [],
                            }
                        ],
                    },
                    {
                        "source_host": "apps.availity.com",
                        "probe_status": "auth_required",
                        "auth_type": "API Key",
                        "reason": "auth_required",
                        "credential_rule_candidate_source_count": 1,
                        "endpoint_discovery_needed_source_count": 0,
                        "api_base_targets": [
                            {
                                "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/b/r4",
                                "source_count": 1,
                                "credential_configured_source_count": 0,
                                "credential_config_missing_source_count": 1,
                                "credential_secret_ready_source_count": 0,
                                "credential_secret_missing_source_count": 0,
                                "credential_rule_candidate_source_count": 1,
                                "endpoint_discovery_needed_source_count": 0,
                                "medicare_advantage_source_count": 0,
                                "medicaid_mco_source_count": 1,
                                "chip_source_count": 0,
                                "qhp_source_count": 0,
                                "sample_payers": ["Availity API key payer"],
                                "sample_source_ids": ["pdfhir_api_key"],
                                "sample_missing_credential_payers": ["Availity API key payer"],
                                "sample_missing_secret_payers": [],
                                "sample_missing_secret_env_vars": [],
                            }
                        ],
                    },
                ],
            },
        }
    )

    assert export["api_base_target_count"] == 2
    assert export["credential_rule_candidate_source_count"] == 3
    assert export["endpoint_discovery_needed_source_count"] == 0
    oauth_target, api_key_target = export["targets"]
    assert oauth_target["host"] == "apps.availity.com"
    assert oauth_target["source_host"] == "apps.availity.com"
    assert oauth_target["api_base"] == "https://apps.availity.com/availity/public-fhir/fhir/v1/a/r4"
    assert oauth_target["credential_config_missing_source_count"] == 2
    assert oauth_target["credential_rule_candidate_source_count"] == 2
    assert oauth_target["endpoint_discovery_needed_source_count"] == 0
    assert oauth_target["medicare_advantage_source_count"] == 1
    assert "oauth2" in oauth_target["credential_rule_template"]
    assert "api_key" not in oauth_target["credential_rule_template"]
    assert api_key_target["api_base"] == "https://apps.availity.com/availity/public-fhir/fhir/v1/b/r4"
    assert "api_key" in api_key_target["credential_rule_template"]
    assert "oauth2" not in api_key_target["credential_rule_template"]
    assert "secret-value" not in str(export)


def test_provider_directory_coverage_audit_credential_config_template_groups_by_host():
    template = audit._credential_config_template_export(
        {
            "generated_at": "2026-06-29T00:00:00Z",
            "schema": "mrf",
            "credential_onboarding_backlog": {
                "available": True,
                "blocked_source_count": 42,
                "credential_config_available": False,
                "credential_config_source": "argument_file",
                "credential_configured_source_count": 0,
                "credential_config_missing_source_count": 42,
                "credential_rule_candidate_source_count": 42,
                "endpoint_discovery_needed_source_count": 0,
                "group_count": 2,
                "groups": [
                    {
                        "source_host": "apps.availity.com",
                        "probe_status": "auth_required",
                        "auth_type": "OAuth2 Client Credentials",
                        "reason": "auth_required",
                        "source_count": 30,
                        "credential_configured_source_count": 0,
                        "credential_config_missing_source_count": 30,
                        "credential_rule_candidate_source_count": 30,
                        "endpoint_discovery_needed_source_count": 0,
                        "medicare_advantage_source_count": 18,
                        "medicaid_mco_source_count": 8,
                        "chip_source_count": 2,
                        "qhp_source_count": 4,
                        "sample_missing_credential_payers": ["Availity OAuth payer"],
                        "sample_source_ids": ["pdfhir_oauth"],
                        "sample_api_bases": ["https://apps.availity.com/availity/public-fhir/fhir/v1/a/r4"],
                        "api_base_count": 3,
                        "sample_api_base_count": 1,
                        "api_base_sample_complete": False,
                    },
                    {
                        "source_host": "apps.availity.com",
                        "probe_status": "auth_required",
                        "auth_type": "API Key",
                        "reason": "auth_required",
                        "source_count": 12,
                        "credential_configured_source_count": 0,
                        "credential_config_missing_source_count": 12,
                        "credential_rule_candidate_source_count": 12,
                        "endpoint_discovery_needed_source_count": 0,
                        "medicare_advantage_source_count": 0,
                        "medicaid_mco_source_count": 11,
                        "chip_source_count": 1,
                        "qhp_source_count": 0,
                        "sample_missing_credential_payers": ["Availity API key payer"],
                        "sample_source_ids": ["pdfhir_api_key"],
                        "sample_api_bases": ["https://apps.availity.com/availity/public-fhir/fhir/v1/b/r4"],
                        "api_base_count": 2,
                        "sample_api_base_count": 1,
                        "api_base_sample_complete": False,
                    },
                ],
            },
        }
    )

    rule = template["credential_config_template"]["hosts"]["apps.availity.com"]
    api_base_rules = template["credential_config_template"]["api_bases"]
    oauth_api_base = "https://apps.availity.com/availity/public-fhir/fhir/v1/a/r4"
    api_key_api_base = "https://apps.availity.com/availity/public-fhir/fhir/v1/b/r4"
    assert template["credential_config_source"] == "argument_file"
    assert template["credential_rule_candidate_source_count"] == 42
    assert template["endpoint_discovery_needed_source_count"] == 0
    assert rule["oauth2"] == {
        "token_url": "env:PROVIDER_DIRECTORY_APPS_AVAILITY_COM_TOKEN_URL",
        "client_id": "env:PROVIDER_DIRECTORY_APPS_AVAILITY_COM_CLIENT_ID",
        "client_secret": "env:PROVIDER_DIRECTORY_APPS_AVAILITY_COM_CLIENT_SECRET",
        "scope": "system/*.read",
        "auth": "basic",
    }
    assert rule["api_key"] == {
        "header": "env:PROVIDER_DIRECTORY_APPS_AVAILITY_COM_API_KEY_HEADER",
        "value": "env:PROVIDER_DIRECTORY_APPS_AVAILITY_COM_API_KEY",
    }
    assert rule["_review"] == [
        "This host has several known or sampled FHIR path variants; verify one host-level credential is valid for every path before enabling.",
        "This host has both OAuth2 and API-key credential groups; avoid a single host-level rule unless the payer portal confirms both credentials are required together for every path. Prefer api_bases or sources rules for payer-specific auth."
    ]
    oauth_env_prefix = audit._credential_api_base_env_prefix(oauth_api_base)
    api_key_env_prefix = audit._credential_api_base_env_prefix(api_key_api_base)
    assert api_base_rules[oauth_api_base]["oauth2"] == {
        "token_url": f"env:{oauth_env_prefix}_TOKEN_URL",
        "client_id": f"env:{oauth_env_prefix}_CLIENT_ID",
        "client_secret": f"env:{oauth_env_prefix}_CLIENT_SECRET",
        "scope": "system/*.read",
        "auth": "basic",
    }
    assert "api_key" not in api_base_rules[oauth_api_base]
    assert api_base_rules[api_key_api_base]["api_key"] == {
        "header": f"env:{api_key_env_prefix}_API_KEY_HEADER",
        "value": f"env:{api_key_env_prefix}_API_KEY",
    }
    assert "oauth2" not in api_base_rules[api_key_api_base]
    assert template["rule_summaries"][0]["api_base_rule_templates"][oauth_api_base] == api_base_rules[oauth_api_base]
    assert template["rule_summaries"][1]["api_base_rule_templates"][api_key_api_base] == api_base_rules[api_key_api_base]
    assert len(template["rule_summaries"]) == 2
    assert template["rule_summaries"][0]["api_base_count"] == 3
    assert template["rule_summaries"][0]["sample_api_base_count"] == 1
    assert template["rule_summaries"][0]["api_base_sample_complete"] is False
    assert template["rule_summaries"][0]["api_base_rule_template_count"] == 1
    assert template["rule_summaries"][0]["credential_rule_candidate_source_count"] == 30
    assert template["rule_summaries"][0]["endpoint_discovery_needed_source_count"] == 0
    assert template["rule_summaries"][0]["medicare_advantage_source_count"] == 18
    assert template["rule_summaries"][0]["medicaid_mco_source_count"] == 8
    assert template["rule_summaries"][0]["chip_source_count"] == 2
    assert template["rule_summaries"][0]["qhp_source_count"] == 4
    assert "secret-value" not in str(template)


def test_provider_directory_coverage_audit_credential_priority_export_rolls_up_hosts():
    priority = audit._credential_priority_export(
        {
            "generated_at": "2026-06-29T00:00:00Z",
            "schema": "mrf",
            "credential_onboarding_backlog": {
                "available": True,
                "blocked_source_count": 92,
                "credential_config_available": False,
                "credential_config_source": "argument_file",
                "credential_configured_source_count": 1,
                "credential_config_missing_source_count": 91,
                "credential_secret_ready_source_count": 0,
                "credential_secret_missing_source_count": 1,
                "credential_rule_candidate_source_count": 92,
                "endpoint_discovery_needed_source_count": 0,
                "credential_missing_secret_env_vars": ["AVAILITY_API_KEY"],
                "group_count": 3,
                "groups": [
                    {
                        "source_host": "api.1up.health",
                        "probe_status": "auth_required",
                        "auth_type": "OAuth2 Client Credentials",
                        "reason": "auth_required",
                        "source_count": 50,
                        "credential_configured_source_count": 0,
                        "credential_config_missing_source_count": 50,
                        "credential_secret_ready_source_count": 0,
                        "credential_secret_missing_source_count": 0,
                        "credential_rule_candidate_source_count": 50,
                        "endpoint_discovery_needed_source_count": 0,
                        "regulated_market_source_count": 50,
                        "medicare_advantage_source_count": 20,
                        "medicaid_mco_source_count": 27,
                        "chip_source_count": 3,
                        "qhp_source_count": 6,
                        "sample_missing_credential_payers": ["1up payer"],
                        "sample_source_ids": ["pdfhir_1up"],
                        "sample_api_bases": ["https://api.1up.health/fhir-r4/payer"],
                        "sample_sources": [
                            {
                                "source_id": "pdfhir_1up",
                                "payer": "1up payer",
                                "api_base": "https://api.1up.health/fhir-r4/payer",
                            }
                        ],
                        "sample_missing_credential_sources": [
                            {
                                "source_id": "pdfhir_1up",
                                "payer": "1up payer",
                                "api_base": "https://api.1up.health/fhir-r4/payer",
                            }
                        ],
                        "api_base_count": 50,
                        "sample_api_base_count": 1,
                        "api_base_sample_complete": False,
                    },
                    {
                        "source_host": "apps.availity.com",
                        "probe_status": "auth_required",
                        "auth_type": "OAuth2 Client Credentials",
                        "reason": "auth_required",
                        "source_count": 30,
                        "credential_configured_source_count": 0,
                        "credential_config_missing_source_count": 30,
                        "credential_secret_ready_source_count": 0,
                        "credential_secret_missing_source_count": 0,
                        "credential_rule_candidate_source_count": 30,
                        "endpoint_discovery_needed_source_count": 0,
                        "regulated_market_source_count": 25,
                        "medicare_advantage_source_count": 18,
                        "medicaid_mco_source_count": 8,
                        "chip_source_count": 2,
                        "qhp_source_count": 4,
                        "sample_missing_credential_payers": ["Availity OAuth payer"],
                        "sample_source_ids": ["pdfhir_oauth"],
                        "sample_api_bases": ["https://apps.availity.com/availity/public-fhir/fhir/v1/a/r4"],
                        "sample_sources": [
                            {
                                "source_id": "pdfhir_oauth",
                                "payer": "Availity OAuth payer",
                                "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/a/r4",
                            }
                        ],
                        "sample_missing_credential_sources": [
                            {
                                "source_id": "pdfhir_oauth",
                                "payer": "Availity OAuth payer",
                                "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/a/r4",
                            }
                        ],
                        "api_base_count": 30,
                        "sample_api_base_count": 1,
                        "api_base_sample_complete": False,
                    },
                    {
                        "source_host": "apps.availity.com",
                        "probe_status": "auth_required",
                        "auth_type": "API Key",
                        "reason": "auth_required",
                        "source_count": 12,
                        "credential_configured_source_count": 1,
                        "credential_config_missing_source_count": 11,
                        "credential_secret_ready_source_count": 0,
                        "credential_secret_missing_source_count": 1,
                        "credential_rule_candidate_source_count": 12,
                        "endpoint_discovery_needed_source_count": 0,
                        "regulated_market_source_count": 12,
                        "medicare_advantage_source_count": 0,
                        "medicaid_mco_source_count": 11,
                        "chip_source_count": 1,
                        "qhp_source_count": 0,
                        "sample_missing_credential_payers": ["Availity API key payer"],
                        "sample_missing_secret_payers": ["Availity configured payer"],
                        "sample_missing_secret_env_vars": ["AVAILITY_API_KEY"],
                        "sample_source_ids": ["pdfhir_api_key"],
                        "sample_api_bases": ["https://apps.availity.com/availity/public-fhir/fhir/v1/b/r4"],
                        "sample_sources": [
                            {
                                "source_id": "pdfhir_api_key",
                                "payer": "Availity API key payer",
                                "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/b/r4",
                            }
                        ],
                        "sample_missing_credential_sources": [
                            {
                                "source_id": "pdfhir_api_key",
                                "payer": "Availity API key payer",
                                "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/b/r4",
                            }
                        ],
                        "sample_missing_secret_sources": [
                            {
                                "source_id": "pdfhir_api_key",
                                "payer": "Availity configured payer",
                                "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/b/r4",
                            }
                        ],
                        "api_base_count": 12,
                        "sample_api_base_count": 1,
                        "api_base_sample_complete": False,
                    },
                ],
            },
        }
    )

    assert priority["credential_config_missing_source_count"] == 91
    assert priority["credential_rule_candidate_source_count"] == 92
    assert priority["endpoint_discovery_needed_source_count"] == 0
    assert priority["host_count"] == 2
    assert [host["source_host"] for host in priority["hosts"]] == ["api.1up.health", "apps.availity.com"]
    assert [host["host"] for host in priority["hosts"]] == ["api.1up.health", "apps.availity.com"]
    oneup, availity = priority["hosts"]
    assert oneup["priority_rank"] == 1
    assert oneup["credential_config_missing_source_count"] == 50
    assert oneup["credential_rule_candidate_source_count"] == 50
    assert oneup["endpoint_discovery_needed_source_count"] == 0
    assert oneup["regulated_market_source_count"] == 50
    assert oneup["api_base_group_count"] == 50
    assert oneup["sample_api_base_count"] == 1
    assert availity["priority_rank"] == 2
    assert availity["source_count"] == 42
    assert availity["credential_config_missing_source_count"] == 41
    assert availity["credential_configured_source_count"] == 1
    assert availity["credential_secret_missing_source_count"] == 1
    assert availity["credential_rule_candidate_source_count"] == 42
    assert availity["endpoint_discovery_needed_source_count"] == 0
    assert availity["regulated_market_source_count"] == 37
    assert availity["medicaid_mco_source_count"] == 19
    assert availity["market_flag_source_mentions"] == 44
    assert availity["api_base_group_count"] == 42
    assert availity["sample_api_base_count"] == 2
    assert availity["auth_types"] == ["API Key", "OAuth2 Client Credentials"]
    assert availity["sample_missing_secret_env_vars"] == ["AVAILITY_API_KEY"]
    assert "oauth2" in availity["credential_rule_template"]
    assert "api_key" in availity["credential_rule_template"]
    assert availity["credential_rule_template"]["_review"] == [
        "This host has several known or sampled FHIR path variants; verify one host-level credential is valid for every path before enabling.",
        "This host has both OAuth2 and API-key credential groups; avoid a single host-level rule unless the payer portal confirms both credentials are required together for every path. Prefer api_bases or sources rules for payer-specific auth."
    ]
    assert len(availity["groups"]) == 2
    assert availity["groups"][0]["api_base_count"] == 30
    assert availity["groups"][0]["api_base_sample_complete"] is False
    assert oneup["sample_sources"][0]["source_id"] == "pdfhir_1up"
    assert availity["sample_sources"] == [
        {
            "source_id": "pdfhir_oauth",
            "payer": "Availity OAuth payer",
            "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/a/r4",
        },
        {
            "source_id": "pdfhir_api_key",
            "payer": "Availity API key payer",
            "api_base": "https://apps.availity.com/availity/public-fhir/fhir/v1/b/r4",
        },
    ]
    assert availity["groups"][0]["sample_sources"][0]["source_id"] == "pdfhir_oauth"
    assert availity["sample_missing_secret_sources"][0]["source_id"] == "pdfhir_api_key"
    assert "secret-value" not in str(priority)


def test_provider_directory_coverage_audit_credential_config_template_flags_portal_like_bases():
    template = audit._credential_config_template_export(
        {
            "generated_at": "2026-06-29T00:00:00Z",
            "schema": "mrf",
            "credential_onboarding_backlog": {
                "available": True,
                "blocked_source_count": 27,
                "credential_config_available": False,
                "credential_configured_source_count": 0,
                "credential_config_missing_source_count": 27,
                "credential_rule_candidate_source_count": 0,
                "endpoint_discovery_needed_source_count": 27,
                "group_count": 1,
                "groups": [
                    {
                        "source_host": "www.uhc.com",
                        "probe_status": "auth_required",
                        "auth_type": "OAuth2 Client Credentials",
                        "reason": "auth_required",
                        "source_count": 27,
                        "credential_configured_source_count": 0,
                        "credential_config_missing_source_count": 27,
                        "credential_rule_candidate_source_count": 0,
                        "endpoint_discovery_needed_source_count": 27,
                        "sample_missing_credential_payers": ["UnitedHealthcare"],
                        "sample_source_ids": ["pdfhir_uhc"],
                        "sample_api_bases": ["https://www.uhc.com/legal/interoperability-apis"],
                        "sample_endpoint_discovery_sources": [
                            {
                                "source_id": "pdfhir_uhc",
                                "payer": "UnitedHealthcare",
                                "api_base": "https://www.uhc.com/legal/interoperability-apis",
                            }
                        ],
                    }
                ],
            },
        }
    )

    assert template["credential_rule_candidate_source_count"] == 0
    assert template["endpoint_discovery_needed_source_count"] == 27
    review = template["credential_config_template"]["hosts"]["www.uhc.com"]["_review"]
    assert review == [
        "Sample API bases or source hosts look like portal/documentation URLs; confirm the real FHIR base before enabling a host-level rule."
    ]
    assert template["rule_summaries"][0]["review_reasons"] == review
    assert template["rule_summaries"][0]["credential_rule_candidate_source_count"] == 0
    assert template["rule_summaries"][0]["endpoint_discovery_needed_source_count"] == 27
    assert template["rule_summaries"][0]["sample_endpoint_discovery_sources"][0]["source_id"] == "pdfhir_uhc"


def test_provider_directory_coverage_audit_credential_config_template_flags_developer_portal_hosts():
    template = audit._credential_config_template_export(
        {
            "generated_at": "2026-06-29T00:00:00Z",
            "schema": "mrf",
            "credential_onboarding_backlog": {
                "available": True,
                "blocked_source_count": 2,
                "credential_config_available": False,
                "credential_configured_source_count": 0,
                "credential_config_missing_source_count": 2,
                "credential_rule_candidate_source_count": 0,
                "endpoint_discovery_needed_source_count": 2,
                "group_count": 1,
                "groups": [
                    {
                        "source_host": "developer.kp.org",
                        "probe_status": "auth_required",
                        "auth_type": "OAuth2 Client Credentials",
                        "reason": "auth_required",
                        "source_count": 2,
                        "credential_configured_source_count": 0,
                        "credential_config_missing_source_count": 2,
                        "credential_rule_candidate_source_count": 0,
                        "endpoint_discovery_needed_source_count": 2,
                        "sample_missing_credential_payers": ["Kaiser Permanente"],
                        "sample_source_ids": ["pdfhir_kp"],
                        "sample_api_bases": ["https://developer.kp.org"],
                    }
                ],
            },
        }
    )

    assert template["credential_rule_candidate_source_count"] == 0
    assert template["endpoint_discovery_needed_source_count"] == 2
    review = template["credential_config_template"]["hosts"]["developer.kp.org"]["_review"]
    assert review == [
        "Sample API bases or source hosts look like portal/documentation URLs; confirm the real FHIR base before enabling a host-level rule."
    ]
    assert template["rule_summaries"][0]["review_reasons"] == review


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

    assert '"mrf"."provider_directory_address_corroboration"' in sql
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


def test_provider_directory_coverage_audit_plan_network_context_sql_uses_ref_bearing_resources():
    sql = audit._plan_network_context_cte_sql("mrf")

    assert '"mrf"."provider_directory_insurance_plan"' in sql
    assert '"mrf"."provider_directory_practitioner_role"' in sql
    assert '"mrf"."provider_directory_organization_affiliation"' in sql
    assert '"mrf"."provider_directory_organization"' in sql
    assert "jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb))" in sql
    assert "refs.ref IN (org.resource_id, 'Organization/' || org.resource_id)" in sql
    assert "sample_resolved_network_names" in sql


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


def test_provider_directory_coverage_audit_gaps_for_missing_retest_source_catalog_coverage():
    report = {
        "source_catalog_retest_coverage": {
            "available": True,
            "missing_result_count": 2,
            "importable_missing_result_count": 1,
            "credential_gated_missing_result_count": 1,
        }
    }

    assert audit._derive_gaps(report) == [
        "1 importable and 1 credential-gated retest result(s) are not covered by the current normalized Provider Directory source catalog."
    ]


def test_provider_directory_coverage_audit_gaps_for_source_resource_projection_coverage():
    report = {
        "source_resource_coverage_summary": {
            "available": True,
            "catalog_only_source_count": 4,
            "sources_with_location_rows_without_keys": 2,
            "unified_available": True,
            "sources_with_location_rows_without_unified_rows": 1,
            "sources_with_valid_npi_organization_address_rows_without_unified_rows": 3,
        }
    }

    assert audit._derive_gaps(report) == [
        "4 Provider Directory source(s) have no imported resource rows.",
        "2 Provider Directory source(s) have Location rows but no keyed Location rows.",
        "1 Provider Directory source(s) have Location rows but no unified-address projection rows.",
        "3 Provider Directory source(s) have valid-NPI Organization address rows but no unified-address projection rows.",
    ]


def test_provider_directory_coverage_audit_projection_gap_reason_classifier():
    reason = audit._provider_directory_projection_gap_reason

    assert reason({"valid_npi_organization_address_rows": 7}) == (
        "valid_npi_organization_address_projection_pending"
    )
    assert reason({"role_projectable_location_refs": 1}) == "linked_location_projection_pending"
    assert reason({"role_healthcare_service_projectable_location_refs": 1}) == (
        "linked_healthcare_service_location_projection_pending"
    )
    assert reason({}) == "no_role_or_affiliation_location_refs"
    assert reason({"role_location_refs": 4, "role_matching_location_refs": 0}) == (
        "location_refs_do_not_match_imported_locations"
    )
    assert reason(
        {
            "role_healthcare_service_location_refs": 4,
            "role_healthcare_service_matching_location_refs": 0,
        }
    ) == "healthcare_service_location_refs_do_not_match_imported_locations"
    assert reason({"role_location_refs": 4, "role_matching_location_refs": 2, "role_valid_npi_refs": 0}) == (
        "linked_providers_lack_valid_npi"
    )
    assert reason(
        {
            "role_healthcare_service_location_refs": 4,
            "role_healthcare_service_matching_location_refs": 2,
            "role_healthcare_service_valid_npi_refs": 0,
        }
    ) == "linked_providers_lack_valid_npi"
    assert reason({"role_location_refs": 4, "role_matching_location_refs": 2, "role_valid_npi_refs": 1}) == (
        "linked_rows_filtered_inactive_or_unkeyable"
    )


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


def test_provider_directory_coverage_audit_gaps_for_missing_credential_config():
    report = {
        "credential_onboarding_backlog": {
            "available": True,
            "credential_config_missing_source_count": 7,
        }
    }

    assert audit._derive_gaps(report) == [
        "7 Provider Directory auth/onboarding source(s) do not match a configured credential rule."
    ]


def test_provider_directory_coverage_audit_gaps_for_missing_credential_env_secrets():
    report = {
        "credential_onboarding_backlog": {
            "available": True,
            "credential_config_missing_source_count": 0,
            "credential_secret_missing_source_count": 5,
        }
    }

    assert audit._derive_gaps(report) == [
        "5 Provider Directory credential-configured source(s) reference missing environment secrets."
    ]


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


def test_provider_directory_coverage_audit_gaps_for_network_refs_without_resolved_names():
    report = {
        "plan_network_context_summary": {
            "available": True,
            "insurance_plan_rows": 12,
            "sources_with_insurance_plan_network_refs": 3,
            "distinct_network_refs": 5,
            "resolved_network_refs": 0,
        }
    }

    assert audit._derive_gaps(report) == [
        "Provider Directory network refs are present, but none resolve to network Organization names for PTG matching."
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


def test_provider_directory_coverage_audit_markdown_includes_plan_network_context():
    markdown = audit.render_markdown(
        {
            "generated_at": "2026-06-28T00:00:00Z",
            "schema": "mrf",
            "ptg_plan_filter": None,
            "plan_network_context_summary": {
                "available": True,
                "sources_with_insurance_plans": 2,
                "insurance_plan_source_pct": 50.0,
                "sources_with_any_network_refs": 1,
                "network_ref_source_pct": 25.0,
                "resolved_network_refs": 3,
                "distinct_network_refs": 4,
                "resolved_network_ref_pct": 75.0,
                "samples": [
                    {
                        "org_name": "Example Payer",
                        "insurance_plan_rows": 8,
                        "network_ref_rows": 4,
                        "resolved_network_refs": 3,
                        "sample_resolved_network_names": ["Choice", "Premier"],
                    }
                ],
            },
        }
    )

    assert "- plan/network context: `2` source(s) with InsurancePlan rows (50.0%), `1` source(s) with network refs (25.0%), resolved refs `3` / `4` (75.0%)" in markdown
    assert "## Provider Directory Plan/Network Context" in markdown
    assert "| Example Payer | 8 | 4 | 3 | Choice, Premier |" in markdown


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
                "credential_configured_source_count": 11,
                "credential_config_missing_source_count": 581,
                "credential_secret_ready_source_count": 9,
                "credential_secret_missing_source_count": 2,
                "credential_rule_candidate_source_count": 263,
                "endpoint_discovery_needed_source_count": 329,
                "group_count": 2,
                "groups": [
                    {
                        "source_host": "apps.availity.com",
                        "probe_status": "valid_non_fhir",
                        "auth_type": "OAuth2/SMART",
                        "reason": "onboarding_gateway",
                        "source_count": 263,
                        "credential_configured_source_count": 10,
                        "credential_config_missing_source_count": 253,
                        "credential_secret_ready_source_count": 9,
                        "credential_secret_missing_source_count": 1,
                        "credential_rule_candidate_source_count": 263,
                        "endpoint_discovery_needed_source_count": 0,
                        "medicare_advantage_source_count": 121,
                        "medicaid_mco_source_count": 88,
                        "chip_source_count": 0,
                        "qhp_source_count": 44,
                        "api_base_count": 263,
                        "sample_api_base_count": 2,
                        "sample_payers": ["Aetna / Provider Directory", "Availity payer"],
                        "sample_missing_credential_payers": ["Availity payer"],
                        "sample_missing_secret_env_vars": ["AVAILITY_CLIENT_SECRET"],
                    },
                    {
                        "source_host": "partners.centene.com",
                        "probe_status": "auth_required",
                        "auth_type": "token",
                        "reason": "auth_required",
                        "source_count": 329,
                        "credential_configured_source_count": 1,
                        "credential_config_missing_source_count": 328,
                        "credential_secret_ready_source_count": 0,
                        "credential_secret_missing_source_count": 1,
                        "credential_rule_candidate_source_count": 0,
                        "endpoint_discovery_needed_source_count": 329,
                        "medicare_advantage_source_count": 0,
                        "medicaid_mco_source_count": 329,
                        "chip_source_count": 21,
                        "qhp_source_count": 0,
                        "api_base_count": 329,
                        "sample_api_base_count": 1,
                        "sample_payers": ["Centene"],
                        "sample_missing_credential_payers": ["Centene"],
                        "sample_missing_secret_env_vars": ["CENTENE_CLIENT_SECRET"],
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
    assert "- credential config coverage: `11` configured / `592` gated source(s); missing config `581`" in markdown
    assert "- credential work split: `263` credential-rule candidate(s); `329` endpoint-discovery source(s)" in markdown
    assert "- credential secret readiness: `9` ready / `11` configured source(s); missing env `2`" in markdown
    assert "- Provider Directory rows with country `001`: `0`" in markdown
    assert "- PTG corroboration: skipped (disabled by --skip-ptg)" in markdown
    assert "- advertised resource/source gaps: `2` / `5` (60.0% with rows); auth-blocked after metadata: `0`" in markdown
    assert "- alias fan-out excess source/resource rows: `2720000` across `1` resource type(s)" in markdown
    assert "## Advertised Resource Import Gaps" in markdown
    assert "| `Endpoint` | 2 | 1 | 1 | 0 | `` |" in markdown
    assert "## Credential/Onboarding Backlog" in markdown
    assert "| `apps.availity.com` | `valid_non_fhir` | `OAuth2/SMART` | `onboarding_gateway` | 263 | 263 | 0 | 2/263 | MA=121, Medicaid=88, QHP=44 | 10 | 9 | 253 | 1 | Aetna / Provider Directory, Availity payer | Availity payer | AVAILITY_CLIENT_SECRET |" in markdown
    assert "## Alias Fan-Out" in markdown
    assert "| `Practitioner` | `https://fhir.humana.com/api` | 18 | 2880000 | 160000 | 2720000 | 18.0 | Humana Inc. |" in markdown
    assert "- canonical resource storage: `160000` canonical row(s), `2880000` source edge row(s), `2720000` edge surplus row(s)" in markdown
    assert "## Canonical Resource Storage" in markdown
    assert "| `Practitioner` | 160000 | 2880000 | 2720000 | 18 | 1 |" in markdown


def test_provider_directory_coverage_audit_markdown_caps_credential_groups_without_mutating_report():
    groups = [
        {
            "source_host": f"host-{index}.example",
            "probe_status": "auth_required",
            "auth_type": "OAuth2 Client Credentials",
            "reason": "auth_required",
            "source_count": 1,
            "credential_configured_source_count": 0,
            "credential_secret_ready_source_count": 0,
            "credential_config_missing_source_count": 1,
            "credential_secret_missing_source_count": 0,
            "api_base_count": 1,
            "sample_api_base_count": 1,
            "sample_payers": [f"Example Payer {index}"],
            "sample_missing_credential_payers": [f"Example Payer {index}"],
        }
        for index in range(audit.CREDENTIAL_BACKLOG_MARKDOWN_GROUP_LIMIT + 2)
    ]
    report = {
        "generated_at": "2026-06-29T00:00:00Z",
        "schema": "mrf",
        "credential_onboarding_backlog": {
            "available": True,
            "blocked_source_count": len(groups),
            "credential_configured_source_count": 0,
            "credential_config_missing_source_count": len(groups),
            "credential_secret_ready_source_count": 0,
            "credential_secret_missing_source_count": 0,
            "group_count": len(groups),
            "groups": groups,
        },
    }

    markdown = audit.render_markdown(report)

    assert len(report["credential_onboarding_backlog"]["groups"]) == (
        audit.CREDENTIAL_BACKLOG_MARKDOWN_GROUP_LIMIT + 2
    )
    assert "`host-0.example`" in markdown
    assert f"`host-{audit.CREDENTIAL_BACKLOG_MARKDOWN_GROUP_LIMIT - 1}.example`" in markdown
    assert f"`host-{audit.CREDENTIAL_BACKLOG_MARKDOWN_GROUP_LIMIT}.example`" not in markdown
    assert "2 additional credential/onboarding group(s) omitted from markdown" in markdown


@pytest.mark.asyncio
async def test_provider_directory_coverage_audit_source_resource_coverage_summary(monkeypatch):
    async def relation_exists(_conn, _schema, name):
        return name in {
            "provider_directory_source",
            "provider_directory_location",
            "provider_directory_practitioner",
            "provider_directory_organization",
            "provider_directory_practitioner_role",
            "provider_directory_healthcare_service",
            "provider_directory_organization_affiliation",
            "entity_address_unified",
        }

    async def column_exists(_conn, _schema, table, column):
        return (table, column) in {
            ("provider_directory_location", "address_key"),
            ("provider_directory_organization", "address_json"),
            ("entity_address_unified", "address_sources"),
            ("entity_address_unified", "source_record_ids"),
            ("entity_address_unified", "address_key"),
            ("entity_address_unified", "telephone_number"),
        }

    class FakeConn:
        def __init__(self):
            self.fetch_calls = 0

        async def fetchrow(self, _sql):
            return {
                "source_count": 10,
                "sources_with_resource_rows": 4,
                "catalog_only_source_count": 6,
                "sources_with_location_rows": 3,
                "sources_with_keyed_location_rows": 2,
                "sources_with_location_rows_without_keys": 1,
                "location_rows": 30,
                "keyed_location_rows": 20,
                "sources_with_organization_address_rows": 2,
                "sources_with_valid_npi_organization_address_rows": 1,
                "sources_with_valid_npi_organization_address_rows_without_unified_rows": 1,
                "organization_address_rows": 12,
                "valid_npi_organization_address_rows": 9,
                "sources_with_unified_rows": 2,
                "sources_with_keyed_unified_rows": 1,
                "sources_with_phone_unified_rows": 1,
                "sources_with_location_rows_without_unified_rows": 1,
                "unified_rows": 12,
                "keyed_unified_rows": 10,
                "phone_unified_rows": 8,
            }

        async def fetch(self, _sql, _limit):
            self.fetch_calls += 1
            if self.fetch_calls == 1:
                return [
                    {
                        "source_id": "pdfhir_catalog_only",
                        "org_name": "Catalog Only",
                        "plan_name": None,
                        "canonical_api_base": "https://catalog.example/fhir",
                        "last_probe_status": "auth_required",
                        "last_validated_status": "auth_required",
                        "auth_type": "OAuth2 Client Credentials",
                }
            ]
            if self.fetch_calls == 2:
                assert "provider_directory_healthcare_service" in _sql
                assert "COALESCE(role.healthcare_service_refs::jsonb, '[]'::jsonb)" in _sql
                assert "COALESCE(affiliation.healthcare_service_refs::jsonb, '[]'::jsonb)" in _sql
                assert "COALESCE(healthcare_service.location_refs::jsonb, '[]'::jsonb)" in _sql
                return [
                    {
                        "source_id": "pdfhir_no_projection",
                        "org_name": "No Projection",
                        "plan_name": None,
                        "canonical_api_base": "https://projection.example/fhir",
                        "last_probe_status": "valid",
                        "auth_type": "none",
                        "location_rows": 9,
                        "keyed_location_rows": 9,
                        "role_healthcare_service_location_refs": 3,
                        "role_healthcare_service_valid_npi_refs": 3,
                        "role_healthcare_service_matching_location_refs": 3,
                        "role_healthcare_service_projectable_location_refs": 3,
                        "affiliation_healthcare_service_location_refs": 2,
                        "affiliation_healthcare_service_valid_npi_refs": 0,
                        "affiliation_healthcare_service_matching_location_refs": 2,
                        "affiliation_healthcare_service_projectable_location_refs": 0,
                    }
                ]
            return [
                {
                    "source_id": "pdfhir_org_no_projection",
                    "org_name": "Org No Projection",
                    "plan_name": None,
                    "canonical_api_base": "https://org-projection.example/fhir",
                    "last_probe_status": "valid",
                    "auth_type": "none",
                    "organization_address_rows": 12,
                    "valid_npi_organization_address_rows": 9,
                }
            ]

    monkeypatch.setattr(audit, "_relation_exists", relation_exists)
    monkeypatch.setattr(audit, "_column_exists", column_exists)

    summary = await audit._source_resource_coverage_summary(
        FakeConn(),
        "mrf",
        sample_limit=5,
        include_unified=True,
    )

    assert summary["available"] is True
    assert summary["unified_available"] is True
    assert summary["resource_source_pct"] == 40.0
    assert summary["location_source_pct"] == 30.0
    assert summary["keyed_location_source_pct"] == 66.67
    assert summary["sources_with_valid_npi_organization_address_rows"] == 1
    assert summary["valid_npi_organization_address_rows"] == 9
    assert summary["unified_source_pct"] == 20.0
    assert summary["keyed_unified_source_pct"] == 50.0
    assert summary["phone_unified_source_pct"] == 50.0
    assert summary["catalog_only_samples"][0]["source_id"] == "pdfhir_catalog_only"
    assert summary["location_without_unified_samples"][0]["source_id"] == "pdfhir_no_projection"
    assert (
        summary["location_without_unified_samples"][0]["projection_gap_reason"]
        == "linked_healthcare_service_location_projection_pending"
    )
    assert summary["location_without_unified_samples"][0]["role_healthcare_service_location_refs"] == 3
    assert (
        summary["organization_address_without_unified_samples"][0]["source_id"]
        == "pdfhir_org_no_projection"
    )


def test_provider_directory_coverage_audit_markdown_includes_source_resource_coverage():
    markdown = audit.render_markdown(
        {
            "generated_at": "2026-06-28T00:00:00Z",
            "schema": "mrf",
            "source_resource_coverage_summary": {
                "available": True,
                "source_count": 10,
                "sources_with_resource_rows": 4,
                "resource_source_pct": 40.0,
                "sources_with_location_rows": 3,
                "location_source_pct": 30.0,
                "unified_available": True,
                "sources_with_unified_rows": 2,
                "unified_source_pct": 20.0,
                "sources_with_keyed_unified_rows": 1,
                "keyed_unified_source_pct": 50.0,
                "sources_with_valid_npi_organization_address_rows": 2,
                "valid_npi_organization_address_rows": 996,
                "catalog_only_source_count": 6,
                "catalog_only_samples": [
                    {
                        "source_id": "pdfhir_catalog_only",
                        "org_name": "Catalog Only",
                        "canonical_api_base": "https://catalog.example/fhir",
                        "last_probe_status": "auth_required",
                        "last_validated_status": "auth_required",
                        "auth_type": "OAuth2 Client Credentials",
                    }
                ],
                "sources_with_location_rows_without_unified_rows": 1,
                "location_without_unified_samples": [
                    {
                        "source_id": "pdfhir_no_projection",
                        "org_name": "No Projection",
                        "canonical_api_base": "https://projection.example/fhir",
                        "last_probe_status": "valid",
                        "auth_type": "none",
                        "projection_gap_reason": "linked_providers_lack_valid_npi",
                        "location_rows": 9,
                        "keyed_location_rows": 9,
                    }
                ],
                "sources_with_valid_npi_organization_address_rows_without_unified_rows": 1,
                "organization_address_without_unified_samples": [
                    {
                        "source_id": "pdfhir_org_no_projection",
                        "org_name": "Org No Projection",
                        "canonical_api_base": "https://org-projection.example/fhir",
                        "last_probe_status": "valid",
                        "auth_type": "none",
                        "organization_address_rows": 1000,
                        "valid_npi_organization_address_rows": 996,
                    }
                ],
            },
        }
    )

    assert "- source/resource coverage: `4` / `10` source(s) have resource rows (40.0%); Location sources `3` (30.0%)" in markdown
    assert "- organization-address coverage: `2` source(s), `996` valid-NPI address row(s)" in markdown
    assert "- source/search projection coverage: `2` / `10` source(s) have unified rows (20.0%); keyed unified sources `1` (50.0%)" in markdown
    assert "## Source Resource/Search Coverage" in markdown
    assert "| Catalog Only | `auth_required` | `auth_required` | `OAuth2 Client Credentials` | `https://catalog.example/fhir` |" in markdown
    assert "| No Projection | `valid` | `none` | `linked_providers_lack_valid_npi` | 9 | 9 | `https://projection.example/fhir` |" in markdown
    assert "| Org No Projection | `valid` | `none` | 1000 | 996 | `https://org-projection.example/fhir` |" in markdown


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
            "canonical_resource_summary": {
                "available": False,
                "skipped": True,
                "reason": "disabled by --skip-canonical-resource-summary",
                "resources": [],
            },
        }
    )

    assert "- unified Provider Directory rows: skipped (disabled by --skip-unified)" in markdown
    assert "- PTG corroboration: skipped (disabled by --skip-ptg)" in markdown
    assert "- network resolution: skipped (disabled by --skip-network-resolution)" in markdown
    assert "- advertised resource/source gaps: skipped (disabled by --skip-advertised-resource-gaps)" in markdown
    assert "- alias fan-out: skipped (disabled by --skip-top-source-yield)" in markdown
    assert "- canonical resource storage: skipped (disabled by --skip-canonical-resource-summary)" in markdown
