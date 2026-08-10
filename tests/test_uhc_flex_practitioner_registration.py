# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused storage tests for the exact-query Flex registry pair."""

from __future__ import annotations

from contextlib import asynccontextmanager
import copy
import json
import re
from typing import Any

import pytest

from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_API_BASE,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process import uhc_flex_practitioner_registration as registration


GENERIC_FLEX_SOURCE_ID = "pdfhir_0b5cfd565c53364a73981dcb"
OFFICIAL_FILE_SOURCE_ID = "pdfhir_2754e999dd691175821ec26e"


class _RegistrationDatabase:
    def __init__(self) -> None:
        self.endpoints: dict[str, dict[str, Any]] = {}
        self.sources: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.transaction_entries = 0
        self.transaction_rollbacks = 0

    @asynccontextmanager
    async def transaction(self):
        self.transaction_entries += 1
        endpoint_snapshot = copy.deepcopy(self.endpoints)
        source_snapshot = copy.deepcopy(self.sources)
        try:
            yield self
        except BaseException:
            self.transaction_rollbacks += 1
            self.endpoints = endpoint_snapshot
            self.sources = source_snapshot
            raise

    async def scalar(self, statement: str, **params: Any) -> None:
        self.calls.append(("scalar", statement, dict(params)))
        return None

    async def status(self, statement: str, **params: Any) -> int:
        self.calls.append(("status", statement, dict(params)))
        if "provider_directory_api_endpoint" in statement:
            endpoint_id = params["endpoint_id"]
            if endpoint_id in self.endpoints:
                return 0
            self.endpoints[endpoint_id] = {
                "endpoint_id": endpoint_id,
                "canonical_api_base": params["canonical_api_base"],
                "credential_descriptor_hash": params[
                    "credential_descriptor_hash"
                ],
                "endpoint_signature_hash": params["endpoint_signature_hash"],
                "credential_descriptor_json": json.loads(
                    params["credential_descriptor_json"]
                ),
                "endpoint_signature_json": json.loads(
                    params["endpoint_signature_json"]
                ),
                "metadata_json": json.loads(params["metadata_json"]),
            }
            return 1
        if "provider_directory_source" in statement:
            source_id = params["source_id"]
            if source_id in self.sources:
                return 0
            source_row = registration._expected_source_row(params["endpoint_id"])
            source_row["metadata_json"] = json.loads(params["metadata_json_text"])
            self.sources[source_id] = source_row
            return 1
        raise AssertionError(statement)

    async def first(self, statement: str, **params: Any) -> dict[str, Any] | None:
        self.calls.append(("first", statement, dict(params)))
        if "provider_directory_api_endpoint" in statement:
            return copy.deepcopy(self.endpoints.get(params["endpoint_id"]))
        if "provider_directory_source" in statement:
            return copy.deepcopy(self.sources.get(params["source_id"]))
        raise AssertionError(statement)


def test_endpoint_identity_is_dedicated_no_auth_and_hashes_exact_contract():
    identity = registration.uhc_flex_practitioner_endpoint_identity()
    endpoint_metadata = registration.uhc_flex_practitioner_endpoint_metadata()

    assert identity.public_payload() == {
        "endpoint_id": (
            "ad53a7446514ed65b3a8ea7ab68ceb9a1ef85bf6c04fcb882219ecb50928bab5"
        ),
        "canonical_api_base": UHC_FLEX_PRACTITIONER_API_BASE,
        "credential_descriptor_hash": (
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        ),
        "endpoint_signature_hash": (
            "bdee3163e522418c674885160e14681ee5bab00819b022cc72428d9b49845458"
        ),
    }
    assert endpoint_metadata["auth_type"] == "none"
    assert endpoint_metadata["requires_api_key"] is False
    assert endpoint_metadata["requires_registration"] is False
    assert endpoint_metadata["connector_acquisition_contract"] == (
        UHC_FLEX_PRACTITIONER_QUERY_CONTRACT.endpoint_signature()[
            "connector_acquisition_contract"
        ]
    )


def test_source_metadata_is_manual_cohort_scoped_and_profile_fail_closed():
    metadata = registration.uhc_flex_practitioner_source_metadata()

    assert metadata == {
        "provider_directory_acquisition_enabled": False,
        "provider_directory_acquisition_mode": "manual",
        "provider_directory_authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        "provider_directory_cohort_complete": False,
        "provider_directory_cohort_complete_semantics": (
            "all_members_of_one_sealed_official_practitioner_npi_cohort_have_"
            "terminal_exact_query_results"
        ),
        "provider_directory_connector_id": (
            "pdufpc_16ebdbf260dc9815ae38830a6991fea5d6533ab8db7389da"
        ),
        "provider_directory_default_enabled": False,
        "provider_directory_endpoint_collection_complete": False,
        "provider_directory_endpoint_complete": False,
        "provider_directory_fhir_endpoint": True,
        "provider_directory_manual_only": True,
        "provider_directory_profile_eligible": False,
        "provider_directory_profile_eligibility_gate": (
            "separately_sealed_dataset_readiness"
        ),
        "provider_directory_query_contract_id": (
            "healthporta.provider-directory.uhc-flex-practitioner-exact-npi.v1"
        ),
        "provider_directory_resource_types": ["Practitioner"],
        "provider_directory_source_identity_contract_id": (
            "healthporta.provider-directory.derived-enrichment-source.v1"
        ),
        "provider_directory_source_kind": (
            "derived_official_npi_cohort_enrichment"
        ),
        "provider_directory_source_role": (
            "official-practitioner-npi-enrichment"
        ),
        "provider_directory_transport": "fhir_rest_exact_identifier",
    }


@pytest.mark.asyncio
async def test_registration_inserts_once_then_exactly_replays_without_updates():
    database = _RegistrationDatabase()

    created = await registration.register_uhc_flex_practitioner_source(
        database=database
    )
    endpoint_before = copy.deepcopy(database.endpoints)
    source_before = copy.deepcopy(database.sources)
    replayed = await registration.register_uhc_flex_practitioner_source(
        database=database
    )

    assert created.created is True
    assert created.endpoint_created is True
    assert created.source_created is True
    assert replayed.created is False
    assert replayed.endpoint_created is False
    assert replayed.source_created is False
    assert replayed.endpoint_id == created.endpoint_id
    assert replayed.source_id == UHC_FLEX_PRACTITIONER_SOURCE_ID
    assert database.endpoints == endpoint_before
    assert database.sources == source_before
    assert database.transaction_entries == 2
    lock_calls = [call for call in database.calls if call[0] == "scalar"]
    assert len(lock_calls) == 4
    assert all("pg_advisory_xact_lock" in call[1] for call in lock_calls)
    assert all("hashtextextended" in call[1] for call in lock_calls)
    mutation_sql = "\n".join(call[1] for call in database.calls)
    assert re.search(r"\bUPDATE\s", mutation_sql, re.IGNORECASE) is None
    assert re.search(r"\bDELETE\s", mutation_sql, re.IGNORECASE) is None
    assert GENERIC_FLEX_SOURCE_ID not in database.sources
    assert OFFICIAL_FILE_SOURCE_ID not in database.sources


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target", "field_name"),
    [
        ("source", "provider_directory_profile_eligible"),
        ("source", "provider_directory_endpoint_collection_complete"),
        ("source", "provider_directory_authority_id"),
        ("endpoint", "default_enabled"),
    ],
)
async def test_registration_rejects_metadata_drift_without_repair(
    target: str,
    field_name: str,
):
    database = _RegistrationDatabase()
    result = await registration.register_uhc_flex_practitioner_source(
        database=database
    )
    if target == "source":
        metadata = database.sources[result.source_id]["metadata_json"]
    else:
        metadata = database.endpoints[result.endpoint_id]["metadata_json"]
    metadata[field_name] = not metadata.get(field_name, False)
    drifted_endpoints = copy.deepcopy(database.endpoints)
    drifted_sources = copy.deepcopy(database.sources)

    with pytest.raises(
        registration.UHCFlexPractitionerRegistrationError,
        match="registration has drifted",
    ):
        await registration.register_uhc_flex_practitioner_source(
            database=database
        )

    assert database.endpoints == drifted_endpoints
    assert database.sources == drifted_sources
    assert database.transaction_rollbacks == 1


@pytest.mark.asyncio
async def test_source_collision_rolls_back_a_new_endpoint():
    database = _RegistrationDatabase()
    database.sources[UHC_FLEX_PRACTITIONER_SOURCE_ID] = (
        registration._expected_source_row("0" * 64)
    )

    with pytest.raises(registration.UHCFlexPractitionerRegistrationError):
        await registration.register_uhc_flex_practitioner_source(
            database=database
        )

    assert database.endpoints == {}
    assert database.sources[UHC_FLEX_PRACTITIONER_SOURCE_ID]["endpoint_id"] == (
        "0" * 64
    )
    assert database.transaction_rollbacks == 1


@pytest.mark.asyncio
async def test_default_database_is_resolved_at_each_call(monkeypatch):
    import db.connection as connection

    first_database = _RegistrationDatabase()
    second_database = _RegistrationDatabase()
    monkeypatch.setattr(connection, "db", first_database)
    first_result = await registration.register_uhc_flex_practitioner_source()
    monkeypatch.setattr(connection, "db", second_database)
    second_result = await registration.register_uhc_flex_practitioner_source()

    assert first_result.created is True
    assert second_result.created is True
    assert first_database is not second_database
    assert len(first_database.sources) == len(second_database.sources) == 1


def test_schema_resolution_rejects_ambiguous_or_unsafe_names(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "source_a")
    monkeypatch.setenv("DB_SCHEMA", "source_b")
    with pytest.raises(registration.UHCFlexPractitionerRegistrationError):
        registration._schema_name()

    monkeypatch.setenv("DB_SCHEMA", "source_a")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", 'source_a";DROP SCHEMA source_a')
    with pytest.raises(registration.UHCFlexPractitionerRegistrationError):
        registration._schema_name()
