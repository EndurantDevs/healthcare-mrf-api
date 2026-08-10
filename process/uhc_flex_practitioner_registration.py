# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register the manual exact-NPI Flex Practitioner enrichment source."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
import re
from typing import Any, Mapping

from process.provider_directory_logical_scope import (
    ProviderDirectoryEndpointIdentity,
    ProviderDirectoryEndpointIdentityError,
    build_provider_directory_endpoint_identity,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_API_BASE,
    UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
    UHC_FLEX_PRACTITIONER_SOURCE_IDENTITY_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_SOURCE_ROLE,
    UHC_FLEX_PRACTITIONER_TRANSPORT,
)


_API_ENDPOINT_TABLE = "provider_directory_api_endpoint"
_SOURCE_TABLE = "provider_directory_source"
_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_SOURCE_KIND = "derived_official_npi_cohort_enrichment"
_PROFILE_READINESS_GATE = "separately_sealed_dataset_readiness"
_COHORT_COMPLETE_SEMANTICS = (
    "all_members_of_one_sealed_official_practitioner_npi_cohort_have_"
    "terminal_exact_query_results"
)

_SOURCE_COMPARE_FIELDS = (
    "source_id",
    "org_tin",
    "org_name",
    "plan_name",
    "portal_url",
    "api_base",
    "canonical_api_base",
    "endpoint_id",
    "endpoint_insurance_plan",
    "endpoint_practitioner",
    "endpoint_practitioner_role",
    "endpoint_organization",
    "endpoint_organization_affiliation",
    "endpoint_location",
    "endpoint_healthcare_service",
    "endpoint_network",
    "endpoint_endpoint",
    "requires_registration",
    "requires_api_key",
    "auth_type",
    "last_validated",
    "last_validated_status",
    "fhir_version",
    "compliance_flag",
    "violation_type",
    "violation_detail",
    "data_quality_flag",
    "data_quality_sample_npi",
    "data_quality_practitioner_count",
    "data_quality_checked",
    "is_medicare_advantage",
    "is_medicaid_mco",
    "is_chip",
    "is_qhp",
    "seed_source",
    "seed_source_detail",
    "seed_source_url",
    "seed_source_date",
    "seed_row_id",
    "id_provider_alt",
    "team_status",
    "last_probe_status",
    "last_probe_status_code",
    "last_probe_error",
    "last_probe_run_id",
    "last_probed_at",
    "metadata_json",
)


class UHCFlexPractitionerRegistrationError(RuntimeError):
    """Reject missing schema state or drift from the reviewed registration."""

    def __init__(self, code: str = "drift") -> None:
        message_by_code = {
            "drift": "UHC Flex Practitioner registration has drifted",
            "state": "UHC Flex Practitioner registration state is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


@dataclass(frozen=True, slots=True)
class UHCFlexPractitionerRegistrationResult:
    """Describe one exact insert or idempotent replay."""

    source_id: str
    endpoint_id: str
    endpoint_created: bool
    source_created: bool

    def __post_init__(self) -> None:
        if (
            self.source_id != UHC_FLEX_PRACTITIONER_SOURCE_ID
            or type(self.endpoint_id) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.endpoint_id) is None
            or type(self.endpoint_created) is not bool
            or type(self.source_created) is not bool
        ):
            raise ValueError("UHC Flex Practitioner registration result is invalid")

    @property
    def created(self) -> bool:
        """Return whether this call inserted either immutable registry row."""

        return self.endpoint_created or self.source_created


def _schema_name() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise UHCFlexPractitionerRegistrationError("state")
    schema_name = runtime_schema or legacy_schema or "mrf"
    if _SCHEMA_PATTERN.fullmatch(schema_name) is None:
        raise UHCFlexPractitionerRegistrationError("state")
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


def _identity_hash(document: object) -> str:
    return hashlib.sha256(_canonical_json(document).encode("utf-8")).hexdigest()


def uhc_flex_practitioner_endpoint_identity() -> ProviderDirectoryEndpointIdentity:
    """Return the public-facade identity for the dedicated no-auth endpoint."""

    credential_descriptor: dict[str, Any] = {}
    endpoint_signature = UHC_FLEX_PRACTITIONER_QUERY_CONTRACT.endpoint_signature()
    endpoint_id = _identity_hash(
        {
            "canonical_api_base": UHC_FLEX_PRACTITIONER_API_BASE,
            "credential_descriptor": credential_descriptor,
            "endpoint_signature": endpoint_signature,
        }
    )
    return build_provider_directory_endpoint_identity(
        endpoint_id=endpoint_id,
        canonical_api_base=UHC_FLEX_PRACTITIONER_API_BASE,
        credential_descriptor_hash=_identity_hash(credential_descriptor),
        endpoint_signature_hash=_identity_hash(endpoint_signature),
        credential_descriptor_json=credential_descriptor,
        endpoint_signature_json=endpoint_signature,
    )


def uhc_flex_practitioner_endpoint_metadata() -> dict[str, Any]:
    """Return fresh non-secret metadata for the exact-query endpoint."""

    return {
        "authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        "auth_type": "none",
        "connector_acquisition_contract": (
            UHC_FLEX_PRACTITIONER_QUERY_CONTRACT.endpoint_signature()[
                "connector_acquisition_contract"
            ]
        ),
        "default_enabled": False,
        "identity_version": "derived-exact-identifier-enrichment-v1",
        "manual_only": True,
        "requires_api_key": False,
        "requires_registration": False,
        "resource_types": [UHC_FLEX_OFFICIAL_RESOURCE_TYPE],
        "source_role": UHC_FLEX_PRACTITIONER_SOURCE_ROLE,
    }


def uhc_flex_practitioner_source_metadata() -> dict[str, Any]:
    """Return the fail-closed source policy stored with the registry row."""

    return {
        "provider_directory_acquisition_enabled": False,
        "provider_directory_acquisition_mode": "manual",
        "provider_directory_authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        "provider_directory_cohort_complete": False,
        "provider_directory_cohort_complete_semantics": (
            _COHORT_COMPLETE_SEMANTICS
        ),
        "provider_directory_connector_id": UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
        "provider_directory_default_enabled": False,
        "provider_directory_endpoint_collection_complete": False,
        "provider_directory_endpoint_complete": False,
        "provider_directory_fhir_endpoint": True,
        "provider_directory_manual_only": True,
        "provider_directory_profile_eligible": False,
        "provider_directory_profile_eligibility_gate": _PROFILE_READINESS_GATE,
        "provider_directory_query_contract_id": (
            UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID
        ),
        "provider_directory_resource_types": [
            UHC_FLEX_OFFICIAL_RESOURCE_TYPE
        ],
        "provider_directory_source_identity_contract_id": (
            UHC_FLEX_PRACTITIONER_SOURCE_IDENTITY_CONTRACT_ID
        ),
        "provider_directory_source_kind": _SOURCE_KIND,
        "provider_directory_source_role": UHC_FLEX_PRACTITIONER_SOURCE_ROLE,
        "provider_directory_transport": UHC_FLEX_PRACTITIONER_TRANSPORT,
    }


def _expected_source_row(endpoint_id: str) -> dict[str, Any]:
    practitioner_endpoint = (
        f"{UHC_FLEX_PRACTITIONER_API_BASE}/{UHC_FLEX_OFFICIAL_RESOURCE_TYPE}"
    )
    metadata_endpoint = f"{UHC_FLEX_PRACTITIONER_API_BASE}/metadata"
    return {
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "org_tin": None,
        "org_name": "Provider Directory Derived Practitioner Enrichment",
        "plan_name": None,
        "portal_url": metadata_endpoint,
        "api_base": UHC_FLEX_PRACTITIONER_API_BASE,
        "canonical_api_base": UHC_FLEX_PRACTITIONER_API_BASE,
        "endpoint_id": endpoint_id,
        "endpoint_insurance_plan": None,
        "endpoint_practitioner": practitioner_endpoint,
        "endpoint_practitioner_role": None,
        "endpoint_organization": None,
        "endpoint_organization_affiliation": None,
        "endpoint_location": None,
        "endpoint_healthcare_service": None,
        "endpoint_network": None,
        "endpoint_endpoint": None,
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
        "seed_source": "manual_exact_query_registration",
        "seed_source_detail": UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
        "seed_source_url": metadata_endpoint,
        "seed_source_date": None,
        "seed_row_id": UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
        "id_provider_alt": None,
        "team_status": "manual_default_off",
        "last_probe_status": None,
        "last_probe_status_code": None,
        "last_probe_error": None,
        "last_probe_run_id": None,
        "last_probed_at": None,
        "metadata_json": uhc_flex_practitioner_source_metadata(),
    }


def _row_fields(database_row: Any) -> dict[str, Any]:
    if database_row is None:
        return {}
    row_mapping = (
        database_row._mapping
        if hasattr(database_row, "_mapping")
        else database_row
    )
    return dict(row_mapping)


def _json_object(raw_document: object) -> dict[str, Any]:
    if type(raw_document) is str:
        try:
            raw_document = json.loads(raw_document)
        except ValueError as error:
            raise UHCFlexPractitionerRegistrationError("drift") from error
    if not isinstance(raw_document, Mapping):
        raise UHCFlexPractitionerRegistrationError("drift")
    return dict(raw_document)


def _insert_count(value: object) -> tuple[int, bool]:
    if type(value) is not int or value not in {0, 1}:
        raise UHCFlexPractitionerRegistrationError("state")
    return value, value == 1


async def _insert_endpoint(
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
        credential_descriptor_json=_canonical_json({}),
        endpoint_signature_json=_canonical_json(
            UHC_FLEX_PRACTITIONER_QUERY_CONTRACT.endpoint_signature()
        ),
        metadata_json=_canonical_json(
            uhc_flex_practitioner_endpoint_metadata()
        ),
    )
    return _insert_count(inserted_count)[1]


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
        raise UHCFlexPractitionerRegistrationError("drift")
    try:
        stored_identity = build_provider_directory_endpoint_identity(
            endpoint_id=endpoint_row.get("endpoint_id"),
            canonical_api_base=endpoint_row.get("canonical_api_base"),
            credential_descriptor_hash=endpoint_row.get(
                "credential_descriptor_hash"
            ),
            endpoint_signature_hash=endpoint_row.get(
                "endpoint_signature_hash"
            ),
            credential_descriptor_json=_json_object(
                endpoint_row.get("credential_descriptor_json")
            ),
            endpoint_signature_json=_json_object(
                endpoint_row.get("endpoint_signature_json")
            ),
        )
    except (ProviderDirectoryEndpointIdentityError, TypeError, ValueError) as error:
        raise UHCFlexPractitionerRegistrationError("drift") from error
    if (
        stored_identity != expected_identity
        or _json_object(endpoint_row.get("metadata_json"))
        != uhc_flex_practitioner_endpoint_metadata()
    ):
        raise UHCFlexPractitionerRegistrationError("drift")


async def _insert_source(database: Any, endpoint_id: str) -> bool:
    source_by_field = _expected_source_row(endpoint_id)
    inserted_count = await database.status(
        f"""
        INSERT INTO {_table(_SOURCE_TABLE)} (
            source_id, org_name, portal_url, api_base, canonical_api_base,
            endpoint_id, endpoint_practitioner, requires_registration,
            requires_api_key, auth_type, last_validated_status, fhir_version,
            compliance_flag, data_quality_flag, seed_source,
            seed_source_detail, seed_source_url, seed_row_id, team_status,
            metadata_json, created_at, updated_at
        ) VALUES (
            :source_id, :org_name, :portal_url, :api_base,
            :canonical_api_base, :endpoint_id, :endpoint_practitioner,
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
    return _insert_count(inserted_count)[1]


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
            source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        )
    )
    if not source_row:
        raise UHCFlexPractitionerRegistrationError("drift")
    source_row["metadata_json"] = _json_object(source_row.get("metadata_json"))
    if source_row != _expected_source_row(endpoint_id):
        raise UHCFlexPractitionerRegistrationError("drift")


async def register_uhc_flex_practitioner_source(
    *,
    database: Any | None = None,
) -> UHCFlexPractitionerRegistrationResult:
    """Insert or exactly validate the manual, default-off registry pair."""

    runtime_database = database
    if runtime_database is None:
        from db.connection import db

        runtime_database = db
    endpoint_identity = uhc_flex_practitioner_endpoint_identity()
    lock_identities = sorted(
        (
            "provider-directory-api-endpoint-registration:"
            f"{endpoint_identity.endpoint_id}",
            "provider-directory-source-registration:"
            f"{UHC_FLEX_PRACTITIONER_SOURCE_ID}",
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
        endpoint_created = await _insert_endpoint(
            runtime_database,
            endpoint_identity,
        )
        await _validate_endpoint(runtime_database, endpoint_identity)
        source_created = await _insert_source(
            runtime_database,
            endpoint_identity.endpoint_id,
        )
        await _validate_source(runtime_database, endpoint_identity.endpoint_id)
    return UHCFlexPractitionerRegistrationResult(
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        endpoint_id=endpoint_identity.endpoint_id,
        endpoint_created=endpoint_created,
        source_created=source_created,
    )


__all__ = (
    "register_uhc_flex_practitioner_source",
    "uhc_flex_practitioner_endpoint_identity",
    "uhc_flex_practitioner_endpoint_metadata",
    "uhc_flex_practitioner_source_metadata",
    "UHCFlexPractitionerRegistrationError",
    "UHCFlexPractitionerRegistrationResult",
)
