# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused contracts for deterministic PTG import-row identifiers."""

from process.ptg_parts import import_rows


def test_import_id_normalization_handles_missing_and_oversized_values() -> None:
    """Fallback and bounded IDs remain safe for table-name construction."""

    fallback = import_rows._normalize_import_id(None)
    oversized = import_rows._normalize_import_id("a" * 80)

    assert len(fallback) == 8
    assert fallback.isdigit()
    assert len(oversized) == 34
    assert oversized.startswith("a" * 25 + "_")


def test_provider_set_combination_preserves_one_exact_group() -> None:
    """A single valid group remains identifiable without union synthesis."""

    assert import_rows._combine_provider_set_entries(
        file_id=7,
        entries=[],
    ) == (None, None)
    combined, provider_row = import_rows._combine_provider_set_entries(
        file_id=7,
        entries=[
            {
                "__hash__": 11,
                "npi": [1234567890],
                "network_name": ["network-a"],
                "tin": {"type": "ein", "value": "123456789"},
            }
        ],
    )

    assert combined["provider_count"] == 1
    assert provider_row == {
        "provider_group_hash": 11,
        "provider_group_ref": None,
        "file_id": 7,
        "network_names": ["network-a"],
        "tin_type": "ein",
        "tin_value": "123456789",
        "tin_business_name": None,
        "npi": [1234567890],
    }


def test_provider_group_builder_handles_empty_and_name_only_groups() -> None:
    """Provider-group rows retain available identity without inventing a TIN."""

    assert import_rows._build_provider_set_entry(
        file_id=7,
        provider_group_ref=None,
        provider_groups=[],
    ) == (None, None)

    provider_entry, provider_row = import_rows._build_provider_set_entry(
        file_id=7,
        provider_group_ref="group-name-only",
        provider_groups=[
            {
                "tin": {"business_name": "Example Provider Group"},
                "npi": [],
            }
        ],
    )

    assert provider_entry["tin"] == {
        "type": None,
        "value": None,
        "business_name": "Example Provider Group",
    }
    assert provider_row["tin_type"] is None
    assert provider_row["tin_value"] is None
    assert provider_row["tin_business_name"] == "Example Provider Group"


def test_provider_set_row_inlines_npis_without_group_dictionary() -> None:
    """Legacy NPI-only provider sets retain their exact inline members."""

    provider_set_row = import_rows._ptg2_provider_set_row(
        {
            "npi": [1234567890, 1234567891],
            "provider_count_mode": "exact_npi_union",
        }
    )

    assert provider_set_row["provider_count"] == 2
    assert provider_set_row["npi"] == [1234567890, 1234567891]
    assert provider_set_row["canonical_payload"]["npi_inline"] is True
    assert provider_set_row["canonical_payload"]["npi"] == [
        1234567890,
        1234567891,
    ]
