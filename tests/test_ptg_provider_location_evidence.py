# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import gzip
import json

import pytest

from process.ptg_parts import provider_location_evidence
from process.ptg_parts.provider_location_evidence import audit_tic_provider_location_evidence


def _write_json(path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_audit_tic_provider_location_evidence_reports_absent_direct_addresses(tmp_path):
    artifact = tmp_path / "rates.json"
    _write_json(
        artifact,
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

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["provider_references"] == 1
    assert summary["provider_groups"] == 1
    assert summary["provider_references_with_network_names"] == 1
    assert summary["direct_location_fields_present"] is False
    assert summary["direct_displayable_location_fields_present"] is False
    assert summary["direct_phone_fields_present"] is False
    assert summary["address_field_paths"] == {}
    assert summary["phone_field_paths"] == {}


def test_audit_tic_provider_location_evidence_samples_direct_provider_group_addresses(tmp_path):
    artifact = tmp_path / "rates.json"
    _write_json(
        artifact,
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
                                "postal_code": "62401",
                            },
                            "phone": "217-540-2350",
                        }
                    ],
                }
            ],
            "in_network": [],
        },
    )

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["direct_location_fields_present"] is True
    assert summary["direct_displayable_location_fields_present"] is True
    assert summary["direct_phone_fields_present"] is True
    assert summary["provider_groups_with_direct_location_fields"] == 1
    assert summary["provider_groups_with_displayable_location_fields"] == 1
    assert summary["address_field_paths"]["address"] == 1
    assert summary["address_field_paths"]["address.city"] == 1
    assert summary["phone_field_paths"]["phone"] == 1
    assert summary["samples"][0]["provider_group_id"] == 1662
    assert summary["samples"][0]["displayable_address_present"] is True
    assert summary["samples"][0]["address_fields"]["address.city"] == "Effingham"


def test_audit_tic_provider_location_evidence_samples_provider_reference_address_aliases(tmp_path):
    artifact = tmp_path / "rates.json"
    _write_json(
        artifact,
        {
            "provider_references": [
                {
                    "provider_group_id": 1662,
                    "network_name": ["C2"],
                    "provider_address": "900 W Temple Ave, Effingham, IL 62401",
                    "primary_phone": "217-540-2350",
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

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["provider_references_with_direct_location_fields"] == 1
    assert summary["provider_references_with_displayable_location_fields"] == 1
    assert summary["direct_displayable_location_fields_present"] is True
    assert summary["direct_phone_fields_present"] is True
    assert summary["address_field_paths"]["provider_address"] == 1
    assert summary["phone_field_paths"]["primary_phone"] == 1
    assert summary["samples"][0]["scope"] == "provider_reference"
    assert summary["samples"][0]["displayable_address_present"] is True


def test_audit_tic_provider_location_evidence_scans_gzip_inline_provider_groups(tmp_path):
    artifact = tmp_path / "rates.json.gz"
    source_payload_map = {
        "provider_references": [],
        "in_network": [
            {
                "negotiated_rates": [
                    {
                        "provider_groups": [
                            {
                                "tin": {"type": "ein", "value": "123456789"},
                                "npi": [1467618561],
                                "locations": [{"address_line_1": "1303 W Evergreen Ave"}],
                            }
                        ],
                        "negotiated_prices": [{"negotiated_rate": 1905.33}],
                    }
                ]
            }
        ],
    }
    with gzip.open(artifact, "wt", encoding="utf-8") as fp:
        json.dump(source_payload_map, fp)

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["inline_provider_groups"] == 1
    assert summary["inline_provider_groups_with_direct_location_fields"] == 1
    assert summary["inline_provider_groups_with_displayable_location_fields"] == 1
    assert summary["direct_displayable_location_fields_present"] is True
    assert summary["address_field_paths"]["locations"] == 1
    assert summary["address_field_paths"]["locations[0].address_line_1"] == 1


def test_audit_tic_provider_location_evidence_scans_inline_address_line_aliases(tmp_path):
    artifact = tmp_path / "rates.json"
    _write_json(
        artifact,
        {
            "provider_references": [],
            "in_network": [
                {
                    "negotiated_rates": [
                        {
                            "provider_groups": [
                                {
                                    "tin": {"type": "ein", "value": "123456789"},
                                    "npi": [1467618561],
                                    "practice_location": {"line1": "1303 W Evergreen Ave"},
                                    "tel": "217-555-0100",
                                }
                            ],
                            "negotiated_prices": [{"negotiated_rate": 1905.33}],
                        }
                    ]
                }
            ],
        },
    )

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["inline_provider_groups"] == 1
    assert summary["inline_provider_groups_with_direct_location_fields"] == 1
    assert summary["inline_provider_groups_with_displayable_location_fields"] == 1
    assert summary["direct_displayable_location_fields_present"] is True
    assert summary["direct_phone_fields_present"] is True
    assert summary["address_field_paths"]["practice_location"] == 1
    assert summary["address_field_paths"]["practice_location.line1"] == 1
    assert summary["phone_field_paths"]["tel"] == 1


def test_audit_tic_provider_location_evidence_scans_fhir_address_line_arrays(tmp_path):
    artifact = tmp_path / "rates.json"
    _write_json(
        artifact,
        {
            "provider_references": [
                {
                    "provider_group_id": 1662,
                    "provider_groups": [
                        {
                            "tin": {"type": "ein", "value": "123456789"},
                            "npi": [1679524805],
                            "address": {
                                "line": ["900 W Temple Ave", "Bldg B Ste 2500"],
                                "city": "Effingham",
                                "state": "IL",
                                "postalCode": "62401",
                            },
                        }
                    ],
                }
            ],
            "in_network": [],
        },
    )

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["provider_groups_with_direct_location_fields"] == 1
    assert summary["provider_groups_with_displayable_location_fields"] == 1
    assert summary["direct_displayable_location_fields_present"] is True
    assert summary["address_field_paths"]["address.line"] == 1
    assert summary["samples"][0]["address_fields"]["address.line"] == [
        "900 W Temple Ave",
        "Bldg B Ste 2500",
    ]


def test_small_json_uses_fallback(monkeypatch, tmp_path):
    artifact = tmp_path / "rates.json"
    _write_json(
        artifact,
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
    monkeypatch.setattr(provider_location_evidence, "ijson", None)

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["provider_groups_with_direct_location_fields"] == 1
    assert summary["provider_groups_with_displayable_location_fields"] == 0
    assert summary["direct_displayable_location_fields_present"] is False
    assert summary["address_field_paths"]["address.city"] == 1


def test_phone_only_evidence_stays_separate(tmp_path):
    artifact = tmp_path / "rates.json"
    _write_json(
        artifact,
        {
            "provider_references": [
                {
                    "provider_group_id": 1662,
                    "provider_groups": [
                        {
                            "tin": {"type": "ein", "value": "123456789"},
                            "npi": [1679524805],
                            "phone": "217-540-2350",
                        }
                    ],
                }
            ],
            "in_network": [],
        },
    )

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["direct_location_fields_present"] is True
    assert summary["direct_phone_fields_present"] is True
    assert summary["direct_displayable_location_fields_present"] is False
    assert summary["provider_groups_with_direct_location_fields"] == 1
    assert summary["provider_groups_with_displayable_location_fields"] == 0
    assert summary["samples"][0]["displayable_address_present"] is False


def test_audit_tic_provider_location_evidence_detects_fhir_telecom_phone(tmp_path):
    artifact = tmp_path / "rates.json"
    _write_json(
        artifact,
        {
            "provider_references": [
                {
                    "provider_group_id": 1662,
                    "provider_groups": [
                        {
                            "tin": {"type": "ein", "value": "123456789"},
                            "npi": [1679524805],
                            "address": {
                                "line": ["900 W Temple Ave"],
                                "city": "Effingham",
                                "state": "IL",
                            },
                            "telecom": [
                                {"system": "email", "value": "office@example.test"},
                                {"system": "phone", "value": "217-540-2350"},
                            ],
                        }
                    ],
                }
            ],
            "in_network": [],
        },
    )

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["direct_displayable_location_fields_present"] is True
    assert summary["direct_phone_fields_present"] is True
    assert summary["phone_field_paths"]["telecom"] == 1
    assert summary["samples"][0]["phone_fields"]["telecom"][1] == {
        "system": "phone",
        "value": "217-540-2350",
    }


def test_audit_tic_provider_location_evidence_ignores_email_only_telecom_as_phone(tmp_path):
    artifact = tmp_path / "rates.json"
    _write_json(
        artifact,
        {
            "provider_references": [
                {
                    "provider_group_id": 1662,
                    "provider_groups": [
                        {
                            "tin": {"type": "ein", "value": "123456789"},
                            "npi": [1679524805],
                            "telecom": [{"system": "email", "value": "office@example.test"}],
                        }
                    ],
                }
            ],
            "in_network": [],
        },
    )

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["direct_location_fields_present"] is False
    assert summary["direct_phone_fields_present"] is False
    assert summary["phone_field_paths"] == {}


def test_evidence_helpers_cover_bounded_and_recursive_shapes(tmp_path):
    class DescribedValue:
        def __str__(self):
            return "described"

    assert provider_location_evidence._compact_scalar(7) == 7
    assert provider_location_evidence._compact_scalar(DescribedValue()) == "described"
    assert provider_location_evidence._sample_value(
        {"first": 1, "second": 2},
        max_items=1,
    ) == {"first": 1, "...": "1 more field(s)"}
    assert provider_location_evidence._sample_value(
        [1, 2],
        max_items=1,
    ) == [1, "... 1 more item(s)"]
    assert provider_location_evidence._has_displayable_address_value(
        {"city": "Springfield", "state": "IL"}
    )
    assert not provider_location_evidence._has_displayable_address_value(
        {"city": "Springfield", "state": ""}
    )
    assert provider_location_evidence._has_phone_telecom_value(
        {"nested": {"system": "phone", "value": "555-0100"}}
    )
    assert provider_location_evidence._has_phone_telecom_value(["555-0100"])
    assert not provider_location_evidence._has_phone_telecom_value(42)

    oversized_path = tmp_path / "oversized.json"
    oversized_path.write_text("{}", encoding="utf-8")
    with pytest.raises(RuntimeError, match="ijson is required"):
        provider_location_evidence._load_small_artifact_without_ijson(
            oversized_path,
            max_json_fallback_bytes=1,
        )


def test_fallback_iterators_skip_non_mapping_groups(monkeypatch, tmp_path):
    artifact = tmp_path / "rates.json"
    _write_json(
        artifact,
        {
            "provider_references": [
                {
                    "provider_groups": [
                        None,
                        {"network_name": ["example network"]},
                    ]
                }
            ],
            "in_network": [
                None,
                {},
                {
                    "negotiated_rates": [
                        None,
                        {},
                        {
                            "provider_groups": [
                                None,
                                "ignored",
                                {"network_name": ["inline network"]},
                            ]
                        },
                    ]
                },
            ],
        },
    )
    monkeypatch.setattr(provider_location_evidence, "ijson", None)

    summary = audit_tic_provider_location_evidence(artifact)

    assert summary["provider_groups"] == 1
    assert summary["inline_provider_groups"] == 1
    assert summary["network_name_field_paths"] == {
        "network_name": 2,
    }
