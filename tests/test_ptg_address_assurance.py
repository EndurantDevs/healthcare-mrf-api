# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import importlib.util
import sys
from pathlib import Path

from process.ptg_parts.address_assurance import (
    build_ptg_address_assurance_report,
    source_file_version_ids_from_ptg_payload,
    summarize_ptg_price_address_payload,
)

ROOT = Path(__file__).resolve().parents[1]


def _write_json(path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


def _load_cli_module():
    spec = importlib.util.spec_from_file_location(
        "ptg_address_assurance_report_script",
        ROOT / "scripts" / "research" / "ptg_address_assurance_report.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_ptg_address_assurance_accepts_plan_context_provider_directory_address():
    payload = {
        "data": {
            "items": [
                {
                    "address": {
                        "address_key": "3817f328-22dd-96bb-170d-9a39db95c331",
                        "first_line": "900 W Temple Ave",
                        "city": "Effingham",
                        "state": "IL",
                        "postal_code": "62401",
                    },
                    "network_names": ["C2"],
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_directory_corroborated_location",
                        "address_evidence_level": "payer_directory_network_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "provider_directory_plan_context_matched": True,
                        "address_verification_evidence": {
                            "matched_on": "npi_address_key_role_location_plan",
                        },
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is True
    assert summary["item_count"] == 1
    assert summary["displayed_address_rows"] == 1
    assert summary["network_name_rows"] == 1
    assert summary["network_name_values"] == ["C2"]
    assert summary["source_trace_rows"] == 0
    assert summary["source_file_version_id_rows"] == 0
    assert summary["address_network_binding_counts"] == {"payer_directory_corroborated_location": 1}
    assert summary["issues"] == []


def test_ptg_address_assurance_accepts_network_name_provider_directory_address():
    payload = {
        "data": {
            "items": [
                {
                    "address": {
                        "address_key": "3817f328-22dd-96bb-170d-9a39db95c331",
                        "first_line": "900 W Temple Ave",
                        "city": "Effingham",
                        "state": "IL",
                        "postal_code": "62401",
                    },
                    "network_names": ["C2"],
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_directory_corroborated_location",
                        "address_evidence_level": "payer_directory_network_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "provider_directory_plan_context_matched": False,
                        "provider_directory_network_name_matched": True,
                        "provider_directory_network_matches": [
                            {
                                "ptg_network_name": "C2",
                                "provider_directory_network_name": "C2",
                            }
                        ],
                        "address_verification_evidence": {
                            "matched_on": "npi_address_key_role_location_network_name",
                        },
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is True
    assert summary["issues"] == []


def test_ptg_address_assurance_rejects_network_name_proof_that_does_not_match_served_network():
    payload = {
        "data": {
            "items": [
                {
                    "address": {
                        "first_line": "900 W Temple Ave",
                        "city": "Effingham",
                        "state": "IL",
                        "postal_code": "62401",
                    },
                    "network_names": ["PPO NDC"],
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_directory_corroborated_location",
                        "address_evidence_level": "payer_directory_network_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "provider_directory_plan_context_matched": False,
                        "provider_directory_network_name_matched": True,
                        "provider_directory_network_matches": [
                            {
                                "ptg_network_name": "C2",
                                "provider_directory_network_name": "C2",
                            }
                        ],
                        "address_verification_evidence": {
                            "matched_on": "npi_address_key_role_location_network_name",
                        },
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "payer-directory network-name proof must match served row network_names",
    } in summary["issues"]


def test_ptg_address_assurance_rejects_network_name_proof_without_served_network_names():
    payload = {
        "data": {
            "items": [
                {
                    "address": {
                        "first_line": "900 W Temple Ave",
                        "city": "Effingham",
                        "state": "IL",
                        "postal_code": "62401",
                    },
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_directory_corroborated_location",
                        "address_evidence_level": "payer_directory_network_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "provider_directory_plan_context_matched": False,
                        "provider_directory_network_name_matched": True,
                        "provider_directory_network_matches": [
                            {
                                "ptg_network_name": "C2",
                                "provider_directory_network_name": "C2",
                            }
                        ],
                        "address_verification_evidence": {
                            "matched_on": "npi_address_key_role_location_network_name",
                        },
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "payer-directory network-name proof must match served row network_names",
    } in summary["issues"]


def test_ptg_address_assurance_rejects_network_name_marker_without_evidence():
    payload = {
        "data": {
            "items": [
                {
                    "source_trace": [{"source_file_version_id": "version-a"}],
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_directory_corroborated_location",
                        "address_evidence_level": "payer_directory_network_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "provider_directory_plan_context_matched": False,
                        "provider_directory_network_name_matched": True,
                        "address_verification_evidence": {
                            "matched_on": "npi_address_key_role_location",
                        },
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "payer-directory context match must expose plan context or network-name proof",
    } in summary["issues"]


def test_ptg_address_assurance_rejects_malformed_provider_directory_network_match():
    payload = {
        "data": {
            "items": [
                {
                    "source_trace": [{"source_file_version_id": "version-a"}],
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_directory_corroborated_location",
                        "address_evidence_level": "payer_directory_network_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "provider_directory_plan_context_matched": False,
                        "provider_directory_network_name_matched": True,
                        "provider_directory_network_matches": [{"provider_directory_network_name": "C2"}],
                        "address_verification_evidence": {
                            "matched_on": "npi_address_key_role_location_network_name",
                            "network_name_matches": [{"ptg_network_name": "C2"}],
                        },
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "provider_directory_network_matches entries must include ptg_network_name and provider_directory_network_name",
    } in summary["issues"]


def test_ptg_address_assurance_rejects_network_binding_without_plan_context():
    payload = {
        "data": {
            "items": [
                {
                    "source_trace": [{"source_file_version_id": "version-a"}],
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_directory_corroborated_location",
                        "address_evidence_level": "provider_directory_address",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "provider_directory_plan_context_matched": False,
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    messages = [issue["message"] for issue in summary["issues"]]
    assert "payer-directory context match must use payer_directory_network_location evidence" in messages
    assert "payer-directory context match must expose plan context or network-name proof" in messages


def test_ptg_address_assurance_rejects_empty_api_payload():
    summary = summarize_ptg_price_address_payload({"data": {"items": []}})

    assert summary["ok"] is False
    assert summary["issues"] == [
        {"severity": "error", "item_index": None, "message": "no PTG price rows found"}
    ]


def test_ptg_address_assurance_requires_address_verification_on_every_row():
    summary = summarize_ptg_price_address_payload(
        {
            "data": {
                "items": [
                    {
                        "provider_name": "TiC provider set",
                        "network_names": ["C2"],
                    }
                ]
            }
        }
    )

    assert summary["ok"] is False
    assert summary["address_verification_rows"] == 0
    assert summary["issues"] == [
        {
            "severity": "error",
            "item_index": 0,
            "message": "PTG price row is missing address_verification",
        }
    ]


def test_ptg_address_assurance_requires_displayed_address_present_boolean():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "nppes_provider_address",
                        "requires_location_confirmation": True,
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "displayed_address_present must be boolean",
    } in summary["issues"]


def test_ptg_address_assurance_rejects_provider_directory_string_booleans():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "provider_directory_address",
                        "requires_location_confirmation": True,
                        "displayed_address_present": True,
                        "provider_directory_plan_context_matched": "false",
                        "provider_directory_network_context_present": "true",
                        "provider_directory_network_name_matched": "false",
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    messages = [issue["message"] for issue in summary["issues"]]
    assert "provider_directory_plan_context_matched must be boolean when present" in messages
    assert "provider_directory_network_context_present must be boolean when present" in messages
    assert "provider_directory_network_name_matched must be boolean when present" in messages


def test_ptg_address_assurance_rejects_provider_directory_non_list_fields():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "provider_directory_address",
                        "requires_location_confirmation": True,
                        "displayed_address_present": True,
                        "address_sources": "provider_directory_fhir",
                        "provider_directory_network_refs": "Organization/network-1",
                        "provider_directory_network_names": "C2",
                        "provider_directory_network_matches": {"ptg_network_name": "C2"},
                        "provider_directory_insurance_plan_refs": "InsurancePlan/plan-1",
                        "provider_directory_insurance_plan_matches": "InsurancePlan/plan-1",
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    messages = [issue["message"] for issue in summary["issues"]]
    assert "address_sources must be a string list when present" in messages
    assert "provider_directory_network_refs must be a string list when present" in messages
    assert "provider_directory_network_names must be a string list when present" in messages
    assert "provider_directory_network_matches must be a list when present" in messages
    assert "provider_directory_insurance_plan_refs must be a string list when present" in messages
    assert "provider_directory_insurance_plan_matches must be a string list when present" in messages


def test_ptg_address_assurance_rejects_unknown_evidence_for_displayed_address():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "unknown",
                        "requires_location_confirmation": True,
                        "displayed_address_present": True,
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "address_evidence_level=unknown is not sufficient for a displayed PTG address",
    } in summary["issues"]


def test_ptg_address_assurance_rejects_ptg_address_source_without_payer_confirmed_binding():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "nppes_provider_address",
                        "requires_location_confirmation": True,
                        "displayed_address_present": True,
                        "address_sources": ["nppes", "ptg"],
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "PTG/TiC may not appear in address_sources unless the address is payer-confirmed",
    } in summary["issues"]


def test_ptg_address_assurance_accepts_payer_confirmed_with_direct_location_source():
    payload = {
        "data": {
            "items": [
                {
                    "source_trace": [{"source_file_version_id": "version-a"}],
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_confirmed_location",
                        "address_evidence_level": "payer_confirmed_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "location_source": "payer_provider_group_location",
                        "address_verification_evidence": {
                            "source": "payer_provider_group_location",
                            "provider_group_id": 1662,
                            "json_pointer": "/provider_references/0/provider_groups/0/address",
                        },
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is True
    assert summary["issues"] == []


def test_ptg_address_assurance_rejects_payer_confirmed_without_source_trace():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_confirmed_location",
                        "address_evidence_level": "payer_confirmed_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "location_source": "payer_provider_group_location",
                        "address_verification_evidence": {
                            "source": "payer_provider_group_location",
                            "provider_group_id": 1662,
                            "json_pointer": "/provider_references/0/provider_groups/0/address",
                        },
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "payer-confirmed address must include source_trace.source_file_version_id for raw TiC verification",
    } in summary["issues"]


def test_ptg_address_assurance_rejects_payer_confirmed_without_source_record_evidence():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_confirmed_location",
                        "address_evidence_level": "payer_confirmed_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "location_source": "payer_provider_group_location",
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "payer-confirmed address must include materialized PTG/TiC source record evidence",
    } in summary["issues"]


def test_ptg_address_assurance_rejects_payer_confirmed_without_direct_location_source():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_confirmed_location",
                        "address_evidence_level": "payer_confirmed_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "payer-confirmed address must include direct PTG/TiC payer-location evidence",
    } in summary["issues"]


def test_ptg_address_assurance_rejects_payer_confirmed_with_only_tic_address_source():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_confirmed_location",
                        "address_evidence_level": "payer_confirmed_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "address_sources": ["tic"],
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "payer-confirmed address must include direct PTG/TiC payer-location evidence",
    } in summary["issues"]


def test_ptg_address_assurance_rejects_address_key_only_as_displayed_address():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"address_key": "3817f328-22dd-96bb-170d-9a39db95c331"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "unknown",
                        "requires_location_confirmation": True,
                        "displayed_address_present": True,
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert summary["displayed_address_rows"] == 0
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "address_verification is present but no usable address fields are displayed",
    } in summary["issues"]


def test_ptg_address_assurance_schema_mode_accepts_explicit_no_address_row():
    payload = {
        "data": {
            "items": [
                {
                    "provider_name": "TiC provider set",
                    "network_names": ["C2"],
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "unknown",
                        "requires_location_confirmation": True,
                        "displayed_address_present": False,
                    },
                }
            ]
        }
    }

    strict_summary = summarize_ptg_price_address_payload(payload)
    schema_summary = summarize_ptg_price_address_payload(payload, require_displayed_address=False)

    assert strict_summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "displayed_address_present=false",
    } in strict_summary["issues"]
    assert schema_summary["ok"] is True
    assert schema_summary["displayed_address_rows"] == 0
    assert schema_summary["address_evidence_level_counts"] == {"unknown": 1}
    assert schema_summary["issues"] == []


def test_ptg_address_assurance_schema_mode_rejects_false_displayed_address_with_fields():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "unknown",
                        "requires_location_confirmation": True,
                        "displayed_address_present": False,
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload, require_displayed_address=False)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "displayed_address_present=false but usable address fields are present",
    } in summary["issues"]
    assert any(
        issue["message"].startswith(
            "displayed_address_present=false but address/map/phone/location fields are present:"
        )
        for issue in summary["issues"]
    )


def test_ptg_address_assurance_schema_mode_rejects_false_displayed_address_with_phone_only():
    payload = {
        "data": {
            "items": [
                {
                    "provider_name": "TiC provider set",
                    "telephone_number": "217-555-0100",
                    "distance_miles": 2.4,
                    "location_source": "entity_address_unified",
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "unknown",
                        "requires_location_confirmation": True,
                        "displayed_address_present": False,
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload, require_displayed_address=False)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": (
            "displayed_address_present=false but address/map/phone/location fields are present: "
            "distance_miles, location_source, telephone_number"
        ),
    } in summary["issues"]


def test_ptg_address_assurance_schema_mode_rejects_no_display_verification_evidence_fields():
    payload = {
        "data": {
            "items": [
                {
                    "provider_name": "TiC provider set",
                    "network_names": ["C2"],
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "unknown",
                        "requires_location_confirmation": True,
                        "displayed_address_present": False,
                        "location_source": "entity_address_unified",
                        "address_sources": ["nppes"],
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload, require_displayed_address=False)

    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": (
            "displayed_address_present=false but address_verification includes address/location "
            "evidence fields: address_sources, location_source"
        ),
    } in summary["issues"]


def test_ptg_address_assurance_rejects_city_only_as_displayed_address():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"city": "Effingham"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "city_zip_fallback",
                        "requires_location_confirmation": True,
                        "displayed_address_present": True,
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is False
    assert summary["displayed_address_rows"] == 0
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "address_verification is present but no usable address fields are displayed",
    } in summary["issues"]


def test_ptg_address_assurance_accepts_city_state_fallback_as_displayable_but_unconfirmed():
    payload = {
        "data": {
            "items": [
                {
                    "address": {"city": "Effingham", "state": "IL"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "city_zip_fallback",
                        "requires_location_confirmation": True,
                        "displayed_address_present": True,
                    },
                }
            ]
        }
    }

    summary = summarize_ptg_price_address_payload(payload)

    assert summary["ok"] is True
    assert summary["displayed_address_rows"] == 1
    assert summary["address_evidence_level_counts"] == {"city_zip_fallback": 1}


def test_ptg_address_assurance_rejects_report_without_inputs():
    report = build_ptg_address_assurance_report()

    assert report["ok"] is False
    assert report["issues"] == [
        {"severity": "error", "message": "no API payload or raw artifacts supplied"}
    ]
    assert report["api_payload"] is None
    assert report["raw_artifacts"] == []


def test_ptg_address_assurance_combines_raw_artifact_and_api_payload(tmp_path):
    raw_artifact = tmp_path / "rates.json"
    _write_json(
        raw_artifact,
        {
            "provider_references": [
                {
                    "provider_group_id": 1662,
                    "provider_groups": [
                        {
                            "tin": {"type": "ein", "value": "123456789"},
                            "npi": [1679524805],
                            "address": {"city": "Effingham"},
                        }
                    ],
                }
            ],
            "in_network": [],
        },
    )
    api_payload = {
        "data": {
            "items": [
                {
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "nppes_provider_address",
                        "requires_location_confirmation": True,
                        "displayed_address_present": True,
                    },
                }
            ]
        }
    }

    report = build_ptg_address_assurance_report(
        api_payload=api_payload,
        raw_artifact_paths=[raw_artifact],
    )

    assert report["ok"] is True
    assert report["raw_direct_location_fields_present"] is True
    assert report["raw_direct_displayable_location_fields_present"] is False
    assert report["api_payload"]["address_evidence_level_counts"] == {"nppes_provider_address": 1}
    assert "network_names identify the priced TiC/PTG network" in report["notes"][0]


def test_ptg_address_assurance_rejects_payer_confirmed_when_traced_raw_lacks_address(tmp_path):
    raw_artifact = tmp_path / "rates.json"
    _write_json(
        raw_artifact,
        {
            "provider_references": [
                {
                    "provider_group_id": 1662,
                    "network_name": ["C2"],
                    "provider_groups": [
                        {
                            "tin": {"type": "ein", "value": "123456789"},
                            "npi": [1679524805],
                        }
                    ],
                }
            ],
            "in_network": [],
        },
    )
    api_payload = {
        "data": {
            "items": [
                {
                    "source_trace": [{"source_file_version_id": "version-a"}],
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_confirmed_location",
                        "address_evidence_level": "payer_confirmed_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "location_source": "payer_provider_group_location",
                        "address_verification_evidence": {
                            "source": "payer_provider_group_location",
                            "provider_group_id": 1662,
                            "json_pointer": "/provider_references/0/provider_groups/0/address",
                        },
                    },
                }
            ]
        }
    }

    report = build_ptg_address_assurance_report(
        api_payload=api_payload,
        raw_artifact_paths=[raw_artifact],
        raw_artifact_source_file_version_ids_by_path={str(raw_artifact): "version-a"},
    )

    assert report["ok"] is False
    assert report["raw_artifacts"][0]["source_file_version_ids"] == ["version-a"]
    assert {
        "severity": "error",
        "item_index": 0,
        "message": (
            "payer-confirmed address source raw TiC artifact has no direct displayable "
            "provider location fields: version-a"
        ),
    } in report["issues"]


def test_ptg_address_assurance_accepts_payer_confirmed_when_traced_raw_has_address(tmp_path):
    raw_artifact = tmp_path / "rates.json"
    _write_json(
        raw_artifact,
        {
            "provider_references": [
                {
                    "provider_group_id": 1662,
                    "provider_groups": [
                        {
                            "tin": {"type": "ein", "value": "123456789"},
                            "npi": [1679524805],
                            "address": {
                                "street": "900 W Temple Ave",
                                "city": "Effingham",
                                "state": "IL",
                            },
                        }
                    ],
                }
            ],
            "in_network": [],
        },
    )
    api_payload = {
        "data": {
            "items": [
                {
                    "source_trace": [{"source_file_version_id": "version-a"}],
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_confirmed_location",
                        "address_evidence_level": "payer_confirmed_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "location_source": "payer_provider_group_location",
                        "address_verification_evidence": {
                            "source": "payer_provider_group_location",
                            "provider_group_id": 1662,
                            "json_pointer": "/provider_references/0/provider_groups/0/address",
                        },
                    },
                }
            ]
        }
    }

    report = build_ptg_address_assurance_report(
        api_payload=api_payload,
        raw_artifact_paths=[raw_artifact],
        raw_artifact_source_file_version_ids_by_path={str(raw_artifact): ["version-a"]},
    )

    assert report["ok"] is True
    assert report["issues"] == []
    assert report["raw_direct_displayable_location_fields_present"] is True


def test_ptg_address_assurance_rejects_payer_confirmed_when_source_trace_unresolved(tmp_path):
    raw_artifact = tmp_path / "rates.json"
    _write_json(
        raw_artifact,
        {
            "provider_references": [
                {
                    "provider_group_id": 1662,
                    "provider_groups": [
                        {
                            "tin": {"type": "ein", "value": "123456789"},
                            "npi": [1679524805],
                            "address": {
                                "street": "900 W Temple Ave",
                                "city": "Effingham",
                                "state": "IL",
                            },
                        }
                    ],
                }
            ],
            "in_network": [],
        },
    )
    api_payload = {
        "data": {
            "items": [
                {
                    "source_trace": [{"source_file_version_id": "version-missing"}],
                    "address": {"first_line": "900 W Temple Ave"},
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "payer_confirmed_location",
                        "address_evidence_level": "payer_confirmed_location",
                        "requires_location_confirmation": False,
                        "displayed_address_present": True,
                        "location_source": "payer_provider_group_location",
                        "address_verification_evidence": {
                            "source": "payer_provider_group_location",
                            "provider_group_id": 1662,
                            "json_pointer": "/provider_references/0/provider_groups/0/address",
                        },
                    },
                }
            ]
        }
    }

    report = build_ptg_address_assurance_report(
        api_payload=api_payload,
        raw_artifact_paths=[raw_artifact],
        raw_artifact_source_file_version_ids_by_path={str(raw_artifact): ["version-a"]},
    )

    assert report["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": (
            "payer-confirmed address source_file_version_id was not resolved to an audited "
            "raw artifact: version-missing"
        ),
    } in report["issues"]


def test_ptg_address_assurance_report_exposes_raw_displayable_location_fields(tmp_path):
    raw_artifact = tmp_path / "rates.json"
    _write_json(
        raw_artifact,
        {
            "provider_references": [
                {
                    "provider_group_id": 1662,
                    "provider_groups": [
                        {
                            "tin": {"type": "ein", "value": "123456789"},
                            "npi": [1679524805],
                            "address": {
                                "street": "900 W Temple Ave",
                                "city": "Effingham",
                                "state": "IL",
                            },
                        }
                    ],
                }
            ],
            "in_network": [],
        },
    )

    report = build_ptg_address_assurance_report(raw_artifact_paths=[raw_artifact])

    assert report["ok"] is True
    assert report["api_payload"] is None
    assert report["raw_direct_location_fields_present"] is True
    assert report["raw_direct_displayable_location_fields_present"] is True
    assert report["raw_artifacts"][0]["provider_groups_with_displayable_location_fields"] == 1


def test_source_file_version_ids_from_ptg_payload_extracts_nested_source_trace_ids():
    payload = {
        "data": {
            "items": [
                {
                    "source_trace": [
                        {"source_file_version_id": "version-b"},
                        {"source_file_version_id": "version-a"},
                    ],
                    "prices": [
                        {
                            "source_trace": [
                                {"source_file_version_id": "version-a"},
                                {"source_file_version_id": ""},
                            ]
                        }
                    ],
                }
            ]
        }
    }

    assert source_file_version_ids_from_ptg_payload(payload) == ["version-a", "version-b"]


def test_ptg_address_assurance_report_includes_api_source_file_version_ids():
    payload = {
        "data": {
            "items": [
                {
                    "provider_name": "TiC provider set",
                    "network_names": ["C2"],
                    "source_trace": [{"source_file_version_id": "version-a"}],
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "unknown",
                        "requires_location_confirmation": True,
                        "displayed_address_present": False,
                    },
                }
            ]
        }
    }

    report = build_ptg_address_assurance_report(
        api_payload=payload,
        require_displayed_address=False,
    )

    assert report["ok"] is True
    assert report["api_source_file_version_ids"] == ["version-a"]
    assert report["api_payload"]["network_name_rows"] == 1
    assert report["api_payload"]["network_name_values"] == ["C2"]
    assert report["api_payload"]["source_trace_rows"] == 1
    assert report["api_payload"]["source_file_version_id_rows"] == 1


def test_ptg_address_assurance_report_can_require_source_context():
    payload = {
        "data": {
            "items": [
                {
                    "provider_name": "TiC provider set",
                    "network_names": ["C2"],
                    "source_trace": [{"source_file_version_id": "version-a"}],
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "unknown",
                        "requires_location_confirmation": True,
                        "displayed_address_present": False,
                    },
                }
            ]
        }
    }

    report = build_ptg_address_assurance_report(
        api_payload=payload,
        require_displayed_address=False,
        require_network_names=True,
        require_source_file_version_id=True,
    )

    assert report["ok"] is True
    assert report["api_payload"]["network_name_rows"] == 1
    assert report["api_payload"]["source_file_version_id_rows"] == 1


def test_ptg_address_assurance_report_rejects_missing_required_source_context():
    payload = {
        "data": {
            "items": [
                {
                    "provider_name": "TiC provider set",
                    "source_trace": [{"original_url": "https://payer.example.invalid/mrf/rates.json.gz"}],
                    "address_verification": {
                        "rate_network_binding": "tic_provider_group_npi_tin",
                        "address_network_binding": "inferred_from_provider_identity",
                        "address_evidence_level": "unknown",
                        "requires_location_confirmation": True,
                        "displayed_address_present": False,
                    },
                }
            ]
        }
    }

    report = build_ptg_address_assurance_report(
        api_payload=payload,
        require_displayed_address=False,
        require_network_names=True,
        require_source_file_version_id=True,
    )

    assert report["ok"] is False
    assert {
        "severity": "error",
        "item_index": None,
        "message": "no PTG price rows include network_names",
    } in report["api_payload"]["issues"]
    assert {
        "severity": "error",
        "item_index": None,
        "message": "no PTG price rows include source_trace.source_file_version_id",
    } in report["api_payload"]["issues"]


def test_ptg_address_assurance_cli_helpers_prepare_raw_artifact_resolution():
    script = _load_cli_module()

    assert script._dedupe(["a", "", "a", "b"]) == ["a", "b"]
    assert script._file_uri_to_path("file:///Volumes/Data/data/raw/aa/bb/file.json.gz") == (
        "/Volumes/Data/data/raw/aa/bb/file.json.gz"
    )
    assert script._file_uri_to_path("s3://bucket/key.json.gz") is None


def test_ptg_address_assurance_cli_can_require_resolved_raw_artifacts():
    script = _load_cli_module()

    assert script._raw_artifact_resolution_issues([], [], resolve_attempted=True) == [
        {
            "severity": "error",
            "message": "raw artifact resolution was required but no source_file_version_id values were available",
        }
    ]
    assert script._raw_artifact_resolution_issues(
        ["version-a"],
        [],
        resolve_attempted=False,
    ) == [
        {
            "severity": "error",
            "message": "raw artifact resolution was required but --resolve-raw-artifacts-from-db was not used",
        }
    ]
    assert script._raw_artifact_resolution_issues(
        ["version-a", "version-b"],
        [
            {"source_file_version_id": "version-a", "status": "resolved"},
            {"source_file_version_id": "version-b", "status": "missing_file"},
        ],
        resolve_attempted=True,
    ) == [
        {
            "severity": "error",
            "source_file_version_id": "version-b",
            "message": (
                "source_file_version_id did not resolve to an existing local raw artifact: "
                "version-b (missing_file)"
            ),
        }
    ]
    assert (
        script._raw_artifact_resolution_issues(
            ["version-a"],
            [{"source_file_version_id": "version-a", "status": "resolved"}],
            resolve_attempted=True,
        )
        == []
    )


def test_ptg_address_assurance_cli_fetches_api_url(monkeypatch):
    script = _load_cli_module()
    captured = {}

    class Headers:
        def get_content_charset(self):
            return None

    class Response:
        headers = Headers()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return b'{"data":{"items":[]}}'

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["authorization"] = request.get_header("Authorization")
        captured["accept"] = request.get_header("Accept")
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(script.urllib.request, "urlopen", fake_urlopen)

    payload = script._load_api_payload(None, "https://api.example.test/ptg", "secret-token", 2.5)

    assert payload == {"data": {"items": []}}
    assert captured == {
        "url": "https://api.example.test/ptg",
        "authorization": "Bearer secret-token",
        "accept": "application/json",
        "timeout": 2.5,
    }
