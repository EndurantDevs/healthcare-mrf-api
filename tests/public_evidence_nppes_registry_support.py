# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic NPPES registry rows for public-evidence replay tests."""

from __future__ import annotations

from public_evidence.nppes_registry_primitives import build_nppes_archive_identity


HEADER = (
    "NPI",
    "Entity Type Code",
    "Provider Enumeration Date",
    "Last Update Date",
    "NPI Deactivation Date",
    "NPI Reactivation Date",
)


def archive_identity(**overrides):
    identity_values_by_name = {
        "source_url": (
            "https://download.cms.gov/nppes/"
            "NPPES_Data_Dissemination_July_2026_V2.zip"
        ),
        "archive_name": "NPPES_Data_Dissemination_July_2026_V2.zip",
        "primary_member_name": "npidata_pfile_20050523-20260712.csv",
        "artifact_sha256": "a1" * 32,
        "artifact_byte_count": 1_145_146_362,
        "rights_proof_sha256": "b2" * 32,
    }
    identity_values_by_name.update(overrides)
    return build_nppes_archive_identity(**identity_values_by_name)


def active_type_1_row() -> tuple[str, ...]:
    return (
        "1003000100",
        "1",
        "05/23/2005",
        "07/01/2026",
        "",
        "",
    )


def active_type_2_row() -> tuple[str, ...]:
    return (
        "1003000118",
        "2",
        "05/23/2005",
        "07/02/2026",
        "",
        "",
    )


def reactivated_type_1_row() -> tuple[str, ...]:
    return (
        "1003022534",
        "1",
        "05/23/2005",
        "07/03/2026",
        "06/01/2026",
        "06/15/2026",
    )


def equal_day_reactivated_type_1_row() -> tuple[str, ...]:
    return (
        "1003000100",
        "1",
        "05/23/2005",
        "07/12/2026",
        "06/15/2026",
        "06/15/2026",
    )


def orphan_reactivated_type_2_row() -> tuple[str, ...]:
    return (
        "1003000118",
        "2",
        "05/23/2005",
        "07/12/2026",
        "",
        "06/15/2026",
    )


def future_deactivation_type_1_row() -> tuple[str, ...]:
    return (
        "1003022534",
        "1",
        "05/23/2005",
        "07/12/2026",
        "07/13/2026",
        "",
    )


def future_last_update_type_1_row() -> tuple[str, ...]:
    return (
        "1000000012",
        "1",
        "05/23/2005",
        "07/13/2026",
        "",
        "",
    )


def sparse_deactivated_row() -> tuple[str, ...]:
    return (
        "1003001314",
        "",
        "",
        "",
        "06/20/2026",
        "",
    )


def known_type_deactivated_row() -> tuple[str, ...]:
    return (
        "1000000004",
        "2",
        "05/23/2005",
        "06/20/2026",
        "06/20/2026",
        "",
    )
