# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed identity and storage tests for the dormant rooted graph source."""

from __future__ import annotations

from contextlib import asynccontextmanager
import copy
import hashlib
import json
import re
from typing import Any

import pytest

from process import provider_directory_rooted_graph_registration as registration
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES,
)
from process.provider_directory_rooted_graph_source_contract import (
    provider_directory_rooted_graph_credential_descriptor,
    provider_directory_rooted_graph_endpoint_signature,
    provider_directory_rooted_graph_source_identity_payload,
    ProviderDirectoryRootedGraphSourceContract,
    ProviderDirectoryRootedGraphSourceContractError,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
)


def _canonical_json(document: object) -> str:
    return json.dumps(
        document,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _sha256(document: object) -> str:
    return hashlib.sha256(_canonical_json(document).encode("utf-8")).hexdigest()


class _RegistrationDatabase:
    def __init__(self) -> None:
        self.endpoints: dict[str, dict[str, Any]] = {}
        self.sources: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.transaction_rollbacks = 0

    @asynccontextmanager
    async def transaction(self):
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


def test_graph_registry_ids_recompute_from_the_exact_connector_signature() -> None:
    source_identity = provider_directory_rooted_graph_source_identity_payload()
    endpoint_signature = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT.endpoint_signature()
    expected_source_id = "pdfhir_" + _sha256(source_identity)[:24]
    expected_endpoint_id = _sha256(
        {
            "canonical_api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
            "credential_descriptor": {},
            "endpoint_signature": endpoint_signature,
        }
    )

    assert PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID == expected_source_id == (
        "pdfhir_2b088f28554b9e51505b455e"
    )
    assert PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID == expected_endpoint_id == (
        "42d85e85d6214cf898aef33591756d0231d11f1ef250d8c404c804cda8f36161"
    )
    endpoint_identity = registration.provider_directory_rooted_graph_endpoint_identity()
    assert endpoint_identity.public_payload() == {
        "endpoint_id": expected_endpoint_id,
        "canonical_api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        "credential_descriptor_hash": _sha256({}),
        "endpoint_signature_hash": _sha256(endpoint_signature),
    }
    assert PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID != (
        UHC_FLEX_PRACTITIONER_SOURCE_ID
    )
    assert PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID != (
        uhc_flex_practitioner_endpoint_identity().endpoint_id
    )


def test_graph_registry_identity_documents_are_fresh_and_mutation_safe() -> None:
    signature = provider_directory_rooted_graph_endpoint_signature()
    descriptor = provider_directory_rooted_graph_credential_descriptor()
    signature["connector_id"] = "mutated"
    descriptor["token"] = "mutated"

    fresh_signature = provider_directory_rooted_graph_endpoint_signature()
    fresh_descriptor = provider_directory_rooted_graph_credential_descriptor()
    identity = registration.provider_directory_rooted_graph_endpoint_identity()

    assert fresh_signature == (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT.endpoint_signature()
    )
    assert fresh_descriptor == {}
    assert identity.endpoint_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
    assert identity.endpoint_signature_hash == _sha256(fresh_signature)
    assert identity.credential_descriptor_hash == _sha256(fresh_descriptor)


def test_graph_source_contract_is_closed_to_seven_non_root_families() -> None:
    assert PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES == (
        "PractitionerRole",
        "OrganizationAffiliation",
        "Organization",
        "Location",
        "HealthcareService",
        "InsurancePlan",
        "Endpoint",
    )
    assert "Practitioner" not in PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES
    assert registration.provider_directory_rooted_graph_endpoint_metadata()[
        "resource_types"
    ] == list(PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES)
    with pytest.raises(ProviderDirectoryRootedGraphSourceContractError):
        ProviderDirectoryRootedGraphSourceContract(
            resource_types=("Practitioner",)
        )


def test_graph_source_metadata_is_manual_default_off_and_profile_gated() -> None:
    metadata = registration.provider_directory_rooted_graph_source_metadata()

    assert metadata["provider_directory_authority_id"] == "unitedhealthcare"
    assert metadata["provider_directory_acquisition_enabled"] is False
    assert metadata["provider_directory_default_enabled"] is False
    assert metadata["provider_directory_manual_only"] is True
    assert metadata["provider_directory_profile_eligible"] is False
    assert metadata["provider_directory_profile_eligibility_gate"] == (
        "separately_sealed_rooted_graph_dataset_readiness"
    )
    assert metadata["provider_directory_rooted_graph_complete"] is False
    assert metadata["provider_directory_endpoint_collection_complete"] is False
    assert metadata["provider_directory_endpoint_complete"] is False
    assert metadata["provider_directory_resource_types"] == list(
        PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES
    )
    source_row = registration._expected_source_row(
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
    )
    assert source_row["endpoint_practitioner"] is None
    assert source_row["team_status"] == "manual_default_off"


@pytest.mark.asyncio
async def test_registration_inserts_once_then_exactly_validates_under_sorted_locks():
    database = _RegistrationDatabase()

    created = await registration.register_provider_directory_rooted_graph_source(
        database=database
    )
    endpoint_before = copy.deepcopy(database.endpoints)
    source_before = copy.deepcopy(database.sources)
    replayed = await registration.register_provider_directory_rooted_graph_source(
        database=database
    )

    assert created.created is True
    assert replayed.created is False
    assert database.endpoints == endpoint_before
    assert database.sources == source_before
    lock_calls = [call for call in database.calls if call[0] == "scalar"]
    assert len(lock_calls) == 4
    first_call_locks = [call[2]["lock_identity"] for call in lock_calls[:2]]
    assert first_call_locks == sorted(first_call_locks)
    assert all("pg_advisory_xact_lock" in call[1] for call in lock_calls)
    mutation_sql = "\n".join(call[1] for call in database.calls)
    assert re.search(r"\bUPDATE\s", mutation_sql, re.IGNORECASE) is None
    assert re.search(r"\bDELETE\s", mutation_sql, re.IGNORECASE) is None


@pytest.mark.asyncio
async def test_registration_rejects_drift_without_repair_or_partial_insert():
    database = _RegistrationDatabase()
    result = await registration.register_provider_directory_rooted_graph_source(
        database=database
    )
    database.sources[result.source_id]["metadata_json"][
        "provider_directory_profile_eligible"
    ] = True
    drifted_endpoints = copy.deepcopy(database.endpoints)
    drifted_sources = copy.deepcopy(database.sources)

    with pytest.raises(
        registration.ProviderDirectoryRootedGraphRegistrationError,
        match="registration has drifted",
    ):
        await registration.register_provider_directory_rooted_graph_source(
            database=database
        )

    assert database.endpoints == drifted_endpoints
    assert database.sources == drifted_sources
    assert database.transaction_rollbacks == 1


def test_schema_resolution_rejects_ambiguous_or_unsafe_names(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "source_a")
    monkeypatch.setenv("DB_SCHEMA", "source_b")
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        registration._schema_name()

    monkeypatch.setenv("DB_SCHEMA", "source_a")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", 'source_a";DROP SCHEMA source_a')
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        registration._schema_name()
