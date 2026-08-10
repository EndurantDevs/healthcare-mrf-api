# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure tests for the dedicated Flex Practitioner connector identity."""

from dataclasses import replace
import json
from pathlib import Path

import pytest

from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
    UHC_FLEX_OFFICIAL_NPI_SYSTEM,
)
from process.uhc_flex_practitioner_contract import (
    UHCFlexPractitionerContractError,
    UHC_FLEX_PRACTITIONER_API_BASE,
    UHC_FLEX_PRACTITIONER_COHORT_SCOPE,
    UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_QUERY_COUNT,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
    uhc_flex_practitioner_connector_identity_payload,
    uhc_flex_practitioner_source_identity_payload,
)


GENERIC_UHC_SOURCE_ID = "pdfhir_0b5cfd565c53364a73981dcb"


def test_contract_derives_a_dedicated_source_and_connector_identity():
    contract = UHC_FLEX_PRACTITIONER_QUERY_CONTRACT

    assert contract.connector_id == (
        "pdufpc_16ebdbf260dc9815ae38830a6991fea5d6533ab8db7389da"
    )
    assert contract.source_id == "pdfhir_1ceb7c0986c320b7eb924881"
    assert contract.source_id != GENERIC_UHC_SOURCE_ID
    assert contract.authority_id == UHC_FLEX_OFFICIAL_AUTHORITY_ID
    assert contract.canonical_api_base == UHC_FLEX_PRACTITIONER_API_BASE
    assert contract.query_count == UHC_FLEX_PRACTITIONER_QUERY_COUNT == 16
    assert contract.query_values_per_request == 1
    assert contract.pagination == "forbidden"
    assert contract.endpoint_collection_complete is False
    assert contract.endpoint_complete is False


def test_endpoint_signature_freezes_exact_cohort_query_semantics():
    signature = UHC_FLEX_PRACTITIONER_QUERY_CONTRACT.endpoint_signature()

    assert signature == {
        "connector_acquisition_contract": {
            "cohort_scope": UHC_FLEX_PRACTITIONER_COHORT_SCOPE,
            "connector_id": UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
            "contract_id": UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
            "endpoint_collection_complete": False,
            "endpoint_complete": False,
            "identifier_system": UHC_FLEX_OFFICIAL_NPI_SYSTEM,
            "pagination": "forbidden",
            "query_count": 16,
            "query_values_per_request": 1,
            "resource_type": "Practitioner",
            "search_parameter": "identifier",
            "transport": "fhir_rest_exact_identifier",
        }
    }

    signature["connector_acquisition_contract"]["query_count"] = 100
    assert (
        UHC_FLEX_PRACTITIONER_QUERY_CONTRACT.endpoint_signature()[
            "connector_acquisition_contract"
        ]["query_count"]
        == 16
    )


def test_identity_payloads_are_reproducible_and_exclude_dynamic_cohort_ids():
    connector_identity = uhc_flex_practitioner_connector_identity_payload()
    source_identity = uhc_flex_practitioner_source_identity_payload()

    assert "cohort_id" not in connector_identity
    assert "cohort_id" not in source_identity
    assert source_identity["connector_id"] == UHC_FLEX_PRACTITIONER_CONNECTOR_ID
    assert source_identity["source_role"] == (
        "official-practitioner-npi-enrichment"
    )


def test_existing_generic_uhc_manifest_entry_remains_probe_only():
    repository_root = Path(__file__).resolve().parents[1]
    manifest = json.loads(
        (
            repository_root
            / "specs/provider_directory_endpoint_acquisition_manifest.json"
        ).read_text()
    )
    generic_entry = next(
        entry for entry in manifest["entries"] if entry["entry_id"] == "uhc"
    )

    assert generic_entry == {
        "entry_id": "uhc",
        "display_name": "UHC",
        "owner_id": "unitedhealthcare-community-plan-louisiana",
        "source_ids": [GENERIC_UHC_SOURCE_ID],
        "canonical_base": UHC_FLEX_PRACTITIONER_API_BASE,
        "classification": "probe_only",
        "launch_mode": "create",
        "resource_profile": "NONE",
        "resources": [],
    }
    assert UHC_FLEX_PRACTITIONER_SOURCE_ID not in generic_entry["source_ids"]


@pytest.mark.parametrize(
    "change",
    [
        {"source_id": GENERIC_UHC_SOURCE_ID},
        {"connector_id": "pdufpc_" + "0" * 48},
        {"query_values_per_request": 2},
        {"query_count": 100},
        {"pagination": "allowed"},
        {"endpoint_collection_complete": True},
        {"endpoint_complete": True},
    ],
)
def test_contract_rejects_identity_or_completeness_drift(change):
    with pytest.raises(
        UHCFlexPractitionerContractError,
        match="query contract is inconsistent",
    ):
        replace(UHC_FLEX_PRACTITIONER_QUERY_CONTRACT, **change)
