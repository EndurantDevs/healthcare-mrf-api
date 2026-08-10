# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register the dormant, manual rooted Provider Directory graph source."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
from typing import Any, Mapping

from process.provider_directory_logical_scope import (
    ProviderDirectoryEndpointIdentity,
    ProviderDirectoryEndpointIdentityError,
    build_provider_directory_endpoint_identity,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES,
)
from process.provider_directory_rooted_graph_source_contract import (
    provider_directory_rooted_graph_credential_descriptor,
    provider_directory_rooted_graph_endpoint_signature,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CREDENTIAL_DESCRIPTOR_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_CONTRACT,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_IDENTITY_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ROLE,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_TRANSPORT,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
)


_API_ENDPOINT_TABLE = "provider_directory_api_endpoint"
_SOURCE_TABLE = "provider_directory_source"
_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_SOURCE_KIND = "derived_official_practitioner_rooted_graph_enrichment"
_PROFILE_READINESS_GATE = "separately_sealed_rooted_graph_dataset_readiness"
_ROOTED_GRAPH_COMPLETE_SEMANTICS = (
    "one_matched_sealed_rooted_reference_closure_for_one_immutable_"
    "practitioner_root_dataset"
)
_SOURCE_COMPARE_FIELDS = tuple(
    """source_id org_tin org_name plan_name portal_url api_base
    canonical_api_base endpoint_id endpoint_insurance_plan endpoint_practitioner
    endpoint_practitioner_role endpoint_organization
    endpoint_organization_affiliation endpoint_location
    endpoint_healthcare_service endpoint_network endpoint_endpoint
    requires_registration requires_api_key auth_type last_validated
    last_validated_status fhir_version compliance_flag violation_type
    violation_detail data_quality_flag data_quality_sample_npi
    data_quality_practitioner_count data_quality_checked
    is_medicare_advantage is_medicaid_mco is_chip is_qhp seed_source
    seed_source_detail seed_source_url seed_source_date seed_row_id
    id_provider_alt team_status last_probe_status last_probe_status_code
    last_probe_error last_probe_run_id last_probed_at metadata_json""".split()
)
_RegistrationFlag = bool


class ProviderDirectoryRootedGraphRegistrationError(RuntimeError):
    """Reject missing schema state or drift from the reviewed graph registry."""

    def __init__(self, code: str = "drift") -> None:
        message_by_code = {
            "drift": "Provider Directory rooted graph registration has drifted",
            "state": "Provider Directory rooted graph registration state is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphRegistrationResult:
    """Describe one exact graph-source insert or idempotent replay."""

    source_id: str
    endpoint_id: str
    endpoint_created: bool
    source_created: bool

    def __post_init__(self) -> None:
        if (
            self.source_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
            or self.endpoint_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
            or type(self.endpoint_created) is not bool
            or type(self.source_created) is not bool
        ):
            raise ValueError(
                "provider_directory_rooted_graph_registration_result_invalid"
            )

    @property
    def created(self) -> _RegistrationFlag:
        """Report whether this call inserted either immutable registry row."""

        return any((self.endpoint_created, self.source_created))


def _schema_name() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise ProviderDirectoryRootedGraphRegistrationError("state")
    schema_name = runtime_schema or legacy_schema or "mrf"
    if _SCHEMA_PATTERN.fullmatch(schema_name) is None:
        raise ProviderDirectoryRootedGraphRegistrationError("state")
    return schema_name


def _table(table_name: str) -> str:
    return f'"{_schema_name()}"."{table_name}"'


def _canonical_json(document: object) -> str:
    return json.dumps(
        document,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def provider_directory_rooted_graph_endpoint_identity() -> (
    ProviderDirectoryEndpointIdentity
):
    """Return the deterministic no-auth identity for graph acquisition."""

    return build_provider_directory_endpoint_identity(
        endpoint_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
        canonical_api_base=PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        credential_descriptor_hash=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_CREDENTIAL_DESCRIPTOR_SHA256
        ),
        endpoint_signature_hash=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
        ),
        credential_descriptor_json=(
            provider_directory_rooted_graph_credential_descriptor()
        ),
        endpoint_signature_json=(provider_directory_rooted_graph_endpoint_signature()),
    )


def provider_directory_rooted_graph_endpoint_metadata() -> dict[str, Any]:
    """Return fresh, non-secret and default-off endpoint metadata."""

    return {
        "authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        "auth_type": "none",
        "connector_acquisition_contract": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_CONTRACT.endpoint_signature()[
                "connector_acquisition_contract"
            ]
        ),
        "default_enabled": False,
        "identity_version": "derived-rooted-graph-enrichment-v1",
        "manual_only": True,
        "requires_api_key": False,
        "requires_registration": False,
        "resource_types": list(PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES),
        "source_role": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ROLE,
    }


def provider_directory_rooted_graph_source_metadata() -> dict[str, Any]:
    """Return the fail-closed policy persisted with the graph source."""

    return {
        "provider_directory_acquisition_enabled": False,
        "provider_directory_acquisition_mode": "manual",
        "provider_directory_authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        "provider_directory_cohort_complete": False,
        "provider_directory_connector_id": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID
        ),
        "provider_directory_default_enabled": False,
        "provider_directory_endpoint_collection_complete": False,
        "provider_directory_endpoint_complete": False,
        "provider_directory_fhir_endpoint": True,
        "provider_directory_manual_only": True,
        "provider_directory_profile_eligible": False,
        "provider_directory_profile_eligibility_gate": _PROFILE_READINESS_GATE,
        "provider_directory_query_contract_id": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID
        ),
        "provider_directory_resource_types": list(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES
        ),
        "provider_directory_rooted_graph_complete": False,
        "provider_directory_rooted_graph_complete_semantics": (
            _ROOTED_GRAPH_COMPLETE_SEMANTICS
        ),
        "provider_directory_source_identity_contract_id": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_IDENTITY_CONTRACT_ID
        ),
        "provider_directory_source_kind": _SOURCE_KIND,
        "provider_directory_source_role": (PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ROLE),
        "provider_directory_transport": PROVIDER_DIRECTORY_ROOTED_GRAPH_TRANSPORT,
    }


def _resource_endpoint(resource_type: str) -> str:
    return f"{PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE}/{resource_type}"


def _expected_source_row(endpoint_id: str) -> dict[str, Any]:
    metadata_endpoint = f"{PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE}/metadata"
    return {
        "source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        "org_tin": None,
        "org_name": "Provider Directory Rooted Graph Enrichment",
        "plan_name": None,
        "portal_url": metadata_endpoint,
        "api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        "canonical_api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        "endpoint_id": endpoint_id,
        "endpoint_insurance_plan": _resource_endpoint("InsurancePlan"),
        "endpoint_practitioner": None,
        "endpoint_practitioner_role": _resource_endpoint("PractitionerRole"),
        "endpoint_organization": _resource_endpoint("Organization"),
        "endpoint_organization_affiliation": _resource_endpoint(
            "OrganizationAffiliation"
        ),
        "endpoint_location": _resource_endpoint("Location"),
        "endpoint_healthcare_service": _resource_endpoint("HealthcareService"),
        "endpoint_network": None,
        "endpoint_endpoint": _resource_endpoint("Endpoint"),
        "requires_registration": False,
        "requires_api_key": False,
        "auth_type": "none",
        "last_validated": None,
        "last_validated_status": "registration_only",
        "fhir_version": "4.0.1",
        "compliance_flag": "derived_enrichment",
        "violation_type": None,
        "violation_detail": None,
        "data_quality_flag": "sealed_dataset_readiness_required",
        "data_quality_sample_npi": None,
        "data_quality_practitioner_count": None,
        "data_quality_checked": None,
        "is_medicare_advantage": None,
        "is_medicaid_mco": None,
        "is_chip": None,
        "is_qhp": None,
        "seed_source": "manual_rooted_graph_registration",
        "seed_source_detail": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID,
        "seed_source_url": metadata_endpoint,
        "seed_source_date": None,
        "seed_row_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
        "id_provider_alt": None,
        "team_status": "manual_default_off",
        "last_probe_status": None,
        "last_probe_status_code": None,
        "last_probe_error": None,
        "last_probe_run_id": None,
        "last_probed_at": None,
        "metadata_json": provider_directory_rooted_graph_source_metadata(),
    }


def _row_fields(database_row: Any) -> dict[str, Any]:
    if database_row is None:
        return {}
    row_mapping = (
        database_row._mapping if hasattr(database_row, "_mapping") else database_row
    )
    if not isinstance(row_mapping, Mapping):
        raise ProviderDirectoryRootedGraphRegistrationError("state")
    return dict(row_mapping)


def _json_object(raw_document: object) -> dict[str, Any]:
    if type(raw_document) is str:
        try:
            raw_document = json.loads(raw_document)
        except ValueError as error:
            raise ProviderDirectoryRootedGraphRegistrationError("drift") from error
    if not isinstance(raw_document, Mapping):
        raise ProviderDirectoryRootedGraphRegistrationError("drift")
    return dict(raw_document)


def _is_inserted(value: object) -> bool:
    if type(value) is not int or value not in {0, 1}:
        raise ProviderDirectoryRootedGraphRegistrationError("state")
    return value == 1


async def _has_created_endpoint(
    database: Any,
    identity: ProviderDirectoryEndpointIdentity,
) -> bool:
    public_identity = identity.public_payload()
    inserted_count = await database.status(
        f"""
        INSERT INTO {_table(_API_ENDPOINT_TABLE)} (
            endpoint_id, canonical_api_base, credential_descriptor_hash,
            endpoint_signature_hash, credential_descriptor_json,
            endpoint_signature_json, first_seen_at, last_seen_at,
            metadata_json, created_at, updated_at
        ) VALUES (
            :endpoint_id, :canonical_api_base, :credential_descriptor_hash,
            :endpoint_signature_hash,
            CAST(:credential_descriptor_json AS jsonb),
            CAST(:endpoint_signature_json AS jsonb),
            pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp(),
            CAST(:metadata_json AS jsonb),
            pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()
        )
        ON CONFLICT DO NOTHING;
        """,
        **public_identity,
        credential_descriptor_json=_canonical_json(
            provider_directory_rooted_graph_credential_descriptor()
        ),
        endpoint_signature_json=_canonical_json(
            provider_directory_rooted_graph_endpoint_signature()
        ),
        metadata_json=_canonical_json(
            provider_directory_rooted_graph_endpoint_metadata()
        ),
    )
    return _is_inserted(inserted_count)


async def _validate_endpoint(
    database: Any,
    expected_identity: ProviderDirectoryEndpointIdentity,
) -> None:
    endpoint_row = _row_fields(
        await database.first(
            f"""
            SELECT endpoint_id, canonical_api_base,
                   credential_descriptor_hash, endpoint_signature_hash,
                   credential_descriptor_json, endpoint_signature_json,
                   metadata_json
              FROM {_table(_API_ENDPOINT_TABLE)}
             WHERE endpoint_id = :endpoint_id
             FOR UPDATE;
            """,
            endpoint_id=expected_identity.endpoint_id,
        )
    )
    if not endpoint_row:
        raise ProviderDirectoryRootedGraphRegistrationError("drift")
    try:
        stored_identity = build_provider_directory_endpoint_identity(
            endpoint_id=endpoint_row.get("endpoint_id"),
            canonical_api_base=endpoint_row.get("canonical_api_base"),
            credential_descriptor_hash=endpoint_row.get("credential_descriptor_hash"),
            endpoint_signature_hash=endpoint_row.get("endpoint_signature_hash"),
            credential_descriptor_json=_json_object(
                endpoint_row.get("credential_descriptor_json")
            ),
            endpoint_signature_json=_json_object(
                endpoint_row.get("endpoint_signature_json")
            ),
        )
    except (ProviderDirectoryEndpointIdentityError, TypeError, ValueError) as error:
        raise ProviderDirectoryRootedGraphRegistrationError("drift") from error
    if (
        stored_identity != expected_identity
        or _json_object(endpoint_row.get("metadata_json"))
        != provider_directory_rooted_graph_endpoint_metadata()
    ):
        raise ProviderDirectoryRootedGraphRegistrationError("drift")


async def _has_created_source(database: Any, endpoint_id: str) -> bool:
    source_by_field = _expected_source_row(endpoint_id)
    inserted_count = await database.status(
        f"""
        INSERT INTO {_table(_SOURCE_TABLE)} (
            source_id, org_name, portal_url, api_base, canonical_api_base,
            endpoint_id, endpoint_insurance_plan, endpoint_practitioner_role,
            endpoint_organization, endpoint_organization_affiliation,
            endpoint_location, endpoint_healthcare_service, endpoint_endpoint,
            requires_registration, requires_api_key, auth_type,
            last_validated_status, fhir_version, compliance_flag,
            data_quality_flag, seed_source, seed_source_detail,
            seed_source_url, seed_row_id, team_status, metadata_json,
            created_at, updated_at
        ) VALUES (
            :source_id, :org_name, :portal_url, :api_base,
            :canonical_api_base, :endpoint_id, :endpoint_insurance_plan,
            :endpoint_practitioner_role, :endpoint_organization,
            :endpoint_organization_affiliation, :endpoint_location,
            :endpoint_healthcare_service, :endpoint_endpoint,
            :requires_registration, :requires_api_key, :auth_type,
            :last_validated_status, :fhir_version, :compliance_flag,
            :data_quality_flag, :seed_source, :seed_source_detail,
            :seed_source_url, :seed_row_id, :team_status,
            CAST(:metadata_json_text AS jsonb),
            pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()
        )
        ON CONFLICT DO NOTHING;
        """,
        **{
            field_name: field_value
            for field_name, field_value in source_by_field.items()
            if field_name != "metadata_json"
        },
        metadata_json_text=_canonical_json(source_by_field["metadata_json"]),
    )
    return _is_inserted(inserted_count)


async def _validate_source(database: Any, endpoint_id: str) -> None:
    selected_fields = ", ".join(_SOURCE_COMPARE_FIELDS)
    source_row = _row_fields(
        await database.first(
            f"""
            SELECT {selected_fields}
              FROM {_table(_SOURCE_TABLE)}
             WHERE source_id = :source_id
             FOR UPDATE;
            """,
            source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        )
    )
    if not source_row:
        raise ProviderDirectoryRootedGraphRegistrationError("drift")
    source_row["metadata_json"] = _json_object(source_row.get("metadata_json"))
    if source_row != _expected_source_row(endpoint_id):
        raise ProviderDirectoryRootedGraphRegistrationError("drift")


async def register_provider_directory_rooted_graph_source(
    *,
    database: Any | None = None,
) -> ProviderDirectoryRootedGraphRegistrationResult:
    """Insert or exactly validate the manual, default-off registry pair."""

    runtime_database = database
    if runtime_database is None:
        from db.connection import db

        runtime_database = db
    endpoint_identity = provider_directory_rooted_graph_endpoint_identity()
    lock_identities = sorted(
        (
            "provider-directory-api-endpoint-registration:"
            f"{endpoint_identity.endpoint_id}",
            "provider-directory-source-registration:"
            f"{PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID}",
        )
    )
    async with runtime_database.transaction():
        for lock_identity in lock_identities:
            await runtime_database.scalar(
                """
                SELECT pg_catalog.pg_advisory_xact_lock(
                           pg_catalog.hashtextextended(:lock_identity, 0)
                       );
                """,
                lock_identity=lock_identity,
            )
        endpoint_created = await _has_created_endpoint(
            runtime_database,
            endpoint_identity,
        )
        await _validate_endpoint(runtime_database, endpoint_identity)
        source_created = await _has_created_source(
            runtime_database,
            endpoint_identity.endpoint_id,
        )
        await _validate_source(runtime_database, endpoint_identity.endpoint_id)
    return ProviderDirectoryRootedGraphRegistrationResult(
        source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        endpoint_id=endpoint_identity.endpoint_id,
        endpoint_created=endpoint_created,
        source_created=source_created,
    )


__all__ = (
    "provider_directory_rooted_graph_endpoint_identity",
    "provider_directory_rooted_graph_endpoint_metadata",
    "provider_directory_rooted_graph_source_metadata",
    "ProviderDirectoryRootedGraphRegistrationError",
    "ProviderDirectoryRootedGraphRegistrationResult",
    "register_provider_directory_rooted_graph_source",
)
