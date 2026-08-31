# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Contract and public API for sealed-cohort Flex Practitioner publication."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
import re
from typing import Any, Mapping

from db.connection import db
from process.provider_directory_dataset_scoped_publication import exact_dataset_variant
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_registration import (
    register_uhc_flex_practitioner_source,
    uhc_flex_practitioner_endpoint_identity,
)
from process.uhc_flex_practitioner_store_contract import (
    ACQUISITION_PATTERN,
    COHORT_PATTERN,
    INTENT_PATTERN,
)
from process.uhc_flex_practitioner_single_root_contract import (
    UHCFlexPractitionerAdmission,
    UHCFlexPractitionerSingleRootAdmission,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    ADMISSION_PATTERN,
    canonical_semantic_projection_as_of,
    UHCFlexPractitionerTwinAdmission,
)


UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID = (
    "healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1"
)
UHC_FLEX_PRACTITIONER_DATASET_ROOT_CONTRACT_ID = (
    "healthporta.provider-directory.uhc-flex-practitioner-dataset-root.v1"
)

_HEADER = "provider_directory_uhc_flex_practitioner_dataset"
_PROVENANCE = "provider_directory_uhc_flex_practitioner_dataset_resource"
_ENDPOINT_DATASET = "provider_directory_endpoint_dataset"
_DATASET_RESOURCE = "provider_directory_dataset_resource"
_SOURCE = "provider_directory_source"
_VALID_FUNCTION = "provider_directory_uhc_flex_practitioner_dataset_valid"
_READY_FUNCTION = "provider_directory_uhc_flex_practitioner_dataset_ready"
_DATASET_PATTERN = re.compile(r"pdufpd_[0-9a-f]{48}\Z")
_ROOT_PATTERN = re.compile(r"pdufpar_[0-9a-f]{48}\Z")
_HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_DEFAULT_BATCH_SIZE = 500


class UHCFlexPractitionerPublicationError(RuntimeError):
    """Expose bounded publication failures without provider payloads or NPIs."""

    def __init__(self, code: str = "state") -> None:
        message_by_code = {
            "admission": "Flex Practitioner publication admission is invalid",
            "content": "Flex Practitioner publication content is invalid",
            "foreign_current": (
                "Flex Practitioner endpoint has an unrelated current dataset"
            ),
            "replay": "Flex Practitioner publication replay is not current",
            "source_drift": "Flex Practitioner publication source has drifted",
            "state": "Flex Practitioner publication state is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


def _schema_name() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise UHCFlexPractitionerPublicationError("state")
    schema_name = runtime_schema or legacy_schema or "mrf"
    if _SCHEMA_PATTERN.fullmatch(schema_name) is None:
        raise UHCFlexPractitionerPublicationError("state")
    return schema_name


def _table(table_name: str) -> str:
    return f'"{_schema_name()}"."{table_name}"'


def _function(function_name: str) -> str:
    return f'"{_schema_name()}"."{function_name}"'


def _canonical_json(document: object) -> str:
    try:
        return json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (MemoryError, OverflowError, RecursionError, TypeError, ValueError):
        raise UHCFlexPractitionerPublicationError("content") from None


def _digest_identifier(prefix: str, fields: tuple[object, ...]) -> str:
    digest = hashlib.sha256(
        "\x1f".join(str(field) for field in fields).encode("utf-8")
    ).hexdigest()
    return prefix + digest[:48]


def _row_fields(database_row: Any) -> dict[str, Any]:
    if database_row is None:
        return {}
    mapping = (
        database_row._mapping if hasattr(database_row, "_mapping") else database_row
    )
    if not isinstance(mapping, Mapping):
        raise UHCFlexPractitionerPublicationError("state")
    return dict(mapping)


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerDatasetIdentity:
    """Bind deterministic dataset and acquisition-root IDs to one admission."""

    dataset_id: str
    acquisition_root_run_id: str
    endpoint_id: str
    admission_id: str
    candidate_acquisition_id: str
    cohort_id: str
    dataset_intent_id: str
    semantic_projection_as_of: str
    operation_key: str
    terminal_set_sha256: str
    resource_count: int
    source_id: str = UHC_FLEX_PRACTITIONER_SOURCE_ID
    publication_contract_id: str = UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID

    def __post_init__(self) -> None:
        try:
            projection_date = canonical_semantic_projection_as_of(
                self.semantic_projection_as_of
            )
        except ValueError:
            projection_date = None
        expected_dataset_id = _digest_identifier(
            "pdufpd_",
            (
                self.publication_contract_id,
                self.admission_id,
                self.candidate_acquisition_id,
                self.cohort_id,
                self.dataset_intent_id,
                self.source_id,
                self.endpoint_id,
                self.semantic_projection_as_of,
                self.operation_key,
                self.terminal_set_sha256,
                self.resource_count,
            ),
        )
        expected_root_id = _digest_identifier(
            "pdufpar_",
            (
                UHC_FLEX_PRACTITIONER_DATASET_ROOT_CONTRACT_ID,
                self.admission_id,
                self.candidate_acquisition_id,
                self.cohort_id,
                self.dataset_intent_id,
                self.semantic_projection_as_of,
                self.operation_key,
                self.terminal_set_sha256,
                self.resource_count,
            ),
        )
        if (
            self.publication_contract_id
            != UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID
            or self.source_id != UHC_FLEX_PRACTITIONER_SOURCE_ID
            or _DATASET_PATTERN.fullmatch(self.dataset_id) is None
            or self.dataset_id != expected_dataset_id
            or _ROOT_PATTERN.fullmatch(self.acquisition_root_run_id) is None
            or self.acquisition_root_run_id != expected_root_id
            or ADMISSION_PATTERN.fullmatch(self.admission_id) is None
            or ACQUISITION_PATTERN.fullmatch(self.candidate_acquisition_id) is None
            or COHORT_PATTERN.fullmatch(self.cohort_id) is None
            or INTENT_PATTERN.fullmatch(self.dataset_intent_id) is None
            or projection_date != self.semantic_projection_as_of
            or type(self.resource_count) is not int
            or self.resource_count < 0
            or _HASH_PATTERN.fullmatch(self.operation_key) is None
            or _HASH_PATTERN.fullmatch(self.terminal_set_sha256) is None
            or type(self.endpoint_id) is not str
            or _HASH_PATTERN.fullmatch(self.endpoint_id) is None
            or self.endpoint_id != uhc_flex_practitioner_endpoint_identity().endpoint_id
        ):
            raise ValueError("Flex Practitioner dataset identity is invalid")


def build_uhc_flex_practitioner_dataset_identity(
    admission: UHCFlexPractitionerAdmission,
    *,
    endpoint_id: str | None = None,
) -> UHCFlexPractitionerDatasetIdentity:
    """Derive the sole dataset identity authorized by one matched admission."""

    if type(admission) not in {
        UHCFlexPractitionerTwinAdmission,
        UHCFlexPractitionerSingleRootAdmission,
    }:
        raise ValueError("Flex Practitioner admission is invalid")
    selected_endpoint_id = (
        endpoint_id
        if endpoint_id is not None
        else uhc_flex_practitioner_endpoint_identity().endpoint_id
    )
    if selected_endpoint_id != uhc_flex_practitioner_endpoint_identity().endpoint_id:
        raise ValueError("Flex Practitioner endpoint ID is invalid")
    identity_tail = (
        admission.admission_id,
        admission.candidate_acquisition_id,
        admission.cohort_id,
        admission.dataset_intent_id,
        admission.semantic_projection_as_of,
        admission.operation_key,
        admission.terminal_set_sha256,
        admission.resource_count,
    )
    dataset_id = _digest_identifier(
        "pdufpd_",
        (
            UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID,
            *identity_tail[:4],
            admission.source_id,
            selected_endpoint_id,
            *identity_tail[4:],
        ),
    )
    root_id = _digest_identifier(
        "pdufpar_",
        (UHC_FLEX_PRACTITIONER_DATASET_ROOT_CONTRACT_ID, *identity_tail),
    )
    return UHCFlexPractitionerDatasetIdentity(
        dataset_id=dataset_id,
        acquisition_root_run_id=root_id,
        endpoint_id=selected_endpoint_id,
        admission_id=admission.admission_id,
        candidate_acquisition_id=admission.candidate_acquisition_id,
        cohort_id=admission.cohort_id,
        dataset_intent_id=admission.dataset_intent_id,
        semantic_projection_as_of=admission.semantic_projection_as_of,
        operation_key=admission.operation_key,
        terminal_set_sha256=admission.terminal_set_sha256,
        resource_count=admission.resource_count,
        source_id=admission.source_id,
    )


def _require_publication_metadata_identity(
    identity: UHCFlexPractitionerDatasetIdentity,
    admission: UHCFlexPractitionerAdmission,
    retry_exhausted_count: int,
) -> None:
    if (
        type(identity) is not UHCFlexPractitionerDatasetIdentity
        or type(admission) not in {UHCFlexPractitionerTwinAdmission, UHCFlexPractitionerSingleRootAdmission}
        or identity.admission_id != admission.admission_id
        or identity.candidate_acquisition_id != admission.candidate_acquisition_id
        or identity.cohort_id != admission.cohort_id
        or identity.dataset_intent_id != admission.dataset_intent_id
        or identity.semantic_projection_as_of != admission.semantic_projection_as_of
        or identity.operation_key != admission.operation_key
        or identity.terminal_set_sha256 != admission.terminal_set_sha256
        or identity.resource_count != admission.resource_count
        or identity.source_id != admission.source_id
        or type(retry_exhausted_count) is not int
        or retry_exhausted_count < 0
        or (retry_exhausted_count > 0 and type(admission) is not UHCFlexPractitionerSingleRootAdmission)
    ):
        raise ValueError("Flex Practitioner publication identity is invalid")


def uhc_flex_practitioner_publication_metadata(
    identity: UHCFlexPractitionerDatasetIdentity,
    admission: UHCFlexPractitionerAdmission,
    retry_exhausted_count: int = 0,
) -> dict[str, Any]:
    """Return the exact closed metadata object checked by PostgreSQL."""

    _require_publication_metadata_identity(identity, admission, retry_exhausted_count)
    metadata = {
        "acquisition_root_run_id": identity.acquisition_root_run_id,
        "admission_contract_id": admission.admission_contract_id,
        "admission_id": admission.admission_id,
        "candidate_acquisition_id": admission.candidate_acquisition_id,
        "candidate_run_id": admission.candidate_run_id,
        "cohort_complete": True,
        "cohort_id": admission.cohort_id,
        "connector_id": admission.connector_id,
        "dataset_id": identity.dataset_id,
        "dataset_intent_id": admission.dataset_intent_id,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "endpoint_id": identity.endpoint_id,
        "expected_npi_count": admission.expected_npi_count,
        "expected_resources": [UHC_FLEX_OFFICIAL_RESOURCE_TYPE],
        "operation_key": admission.operation_key,
        "publication_contract_id": identity.publication_contract_id,
        "query_contract_id": admission.query_contract_id,
        "resource_counts": {UHC_FLEX_OFFICIAL_RESOURCE_TYPE: admission.resource_count},
        "resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "selected_resources": [UHC_FLEX_OFFICIAL_RESOURCE_TYPE],
        "semantic_projection_as_of": admission.semantic_projection_as_of,
        "source_authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        "source_id": admission.source_id,
        "source_ids": [admission.source_id],
        "storage_contract_id": admission.storage_contract_id,
        "terminal_set_sha256": admission.terminal_set_sha256,
    }
    if type(admission) is UHCFlexPractitionerTwinAdmission:
        metadata["baseline_acquisition_id"] = admission.baseline_acquisition_id
        metadata["baseline_run_id"] = admission.baseline_run_id
    else:
        metadata["provider_directory_reviewed_root_policy_v1"] = (
            admission.reviewed_root_policy_json
        )
    if retry_exhausted_count:
        metadata["cohort_complete"] = False
        metadata["retry_exhausted_count"] = retry_exhausted_count
    return metadata


@dataclass(frozen=True, slots=True)
class UHCFlexPractitionerDatasetReadiness:
    """Public bounded readiness for the current Flex dataset."""

    dataset_id: str
    previous_dataset_id: str | None
    admission_id: str
    candidate_acquisition_id: str
    acquisition_root_run_id: str
    cohort_id: str
    dataset_intent_id: str
    endpoint_id: str
    semantic_projection_as_of: str
    operation_key: str
    dataset_hash: str
    resource_count: int
    source_id: str
    source_authority_id: str
    cohort_complete: bool
    endpoint_collection_complete: bool
    endpoint_complete: bool
    retry_exhausted_count: int = 0

    def __post_init__(self) -> None:
        try:
            projection_date = canonical_semantic_projection_as_of(
                self.semantic_projection_as_of
            )
        except ValueError:
            projection_date = None
        if (
            _DATASET_PATTERN.fullmatch(self.dataset_id) is None
            or (
                self.previous_dataset_id is not None
                and exact_dataset_variant(self.previous_dataset_id) is None
            )
            or ADMISSION_PATTERN.fullmatch(self.admission_id) is None
            or ACQUISITION_PATTERN.fullmatch(self.candidate_acquisition_id) is None
            or _ROOT_PATTERN.fullmatch(self.acquisition_root_run_id) is None
            or COHORT_PATTERN.fullmatch(self.cohort_id) is None
            or INTENT_PATTERN.fullmatch(self.dataset_intent_id) is None
            or projection_date != self.semantic_projection_as_of
            or _HASH_PATTERN.fullmatch(self.endpoint_id) is None
            or self.endpoint_id != uhc_flex_practitioner_endpoint_identity().endpoint_id
            or _HASH_PATTERN.fullmatch(self.operation_key) is None
            or _HASH_PATTERN.fullmatch(self.dataset_hash) is None
            or type(self.resource_count) is not int
            or self.resource_count < 0
            or self.source_id != UHC_FLEX_PRACTITIONER_SOURCE_ID
            or self.source_authority_id != UHC_FLEX_OFFICIAL_AUTHORITY_ID
            or type(self.retry_exhausted_count) is not int
            or self.retry_exhausted_count < 0
            or self.cohort_complete is not (self.retry_exhausted_count == 0)
            or self.endpoint_collection_complete is not False
            or self.endpoint_complete is not False
        ):
            raise ValueError("Flex Practitioner dataset readiness is invalid")


@dataclass(frozen=True, slots=True)
class UHCFlexPractitionerPublicationResult:
    """Return a new atomic publication or an exact current replay."""

    readiness: UHCFlexPractitionerDatasetReadiness
    replayed: bool

    def __post_init__(self) -> None:
        if (
            type(self.readiness) is not UHCFlexPractitionerDatasetReadiness
            or type(self.replayed) is not bool
        ):
            raise ValueError("Flex Practitioner publication result is invalid")


async def load_uhc_flex_practitioner_dataset_readiness(
    dataset_id: str,
    *,
    database: Any = db,
) -> UHCFlexPractitionerDatasetReadiness | None:
    """Load one dataset only when every admitted publication link is ready."""

    if type(dataset_id) is not str or _DATASET_PATTERN.fullmatch(dataset_id) is None:
        raise ValueError("Flex Practitioner dataset ID is invalid")
    from process.uhc_flex_practitioner_publication_store import (
        load_dataset_readiness,
    )

    return await load_dataset_readiness(dataset_id, database=database)


async def load_current_uhc_flex_dataset_readiness(
    *,
    database: Any = db,
) -> UHCFlexPractitionerDatasetReadiness | None:
    """Load the sole current source-local dataset without endpoint claims."""

    from process.uhc_flex_practitioner_publication_store import (
        load_current_readiness,
    )

    return await load_current_readiness(database=database)


load_current_uhc_flex_practitioner_dataset_readiness = (
    load_current_uhc_flex_dataset_readiness
)


async def publish_uhc_flex_practitioner_dataset(
    candidate_acquisition_id: str,
    *,
    database: Any = db,
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> UHCFlexPractitionerPublicationResult:
    """Publish only a DB-admitted sealed candidate generation."""

    if (
        type(candidate_acquisition_id) is not str
        or ACQUISITION_PATTERN.fullmatch(candidate_acquisition_id) is None
    ):
        raise ValueError("Flex Practitioner candidate acquisition ID is invalid")
    if type(batch_size) is not int or not 1 <= batch_size <= 1000:
        raise ValueError("Flex Practitioner publication batch size is invalid")
    registration = await register_uhc_flex_practitioner_source(database=database)
    endpoint_id = uhc_flex_practitioner_endpoint_identity().endpoint_id
    if registration.endpoint_id != endpoint_id:
        raise UHCFlexPractitionerPublicationError("source_drift")
    from process.uhc_flex_practitioner_publication_store import (
        publish_registered_uhc_flex_dataset,
    )

    return await publish_registered_uhc_flex_dataset(
        candidate_acquisition_id,
        endpoint_id,
        batch_size,
        database=database,
    )


__all__ = (
    "build_uhc_flex_practitioner_dataset_identity",
    "load_current_uhc_flex_dataset_readiness",
    "load_current_uhc_flex_practitioner_dataset_readiness",
    "load_uhc_flex_practitioner_dataset_readiness",
    "publish_uhc_flex_practitioner_dataset",
    "uhc_flex_practitioner_publication_metadata",
    "UHCFlexPractitionerDatasetIdentity",
    "UHCFlexPractitionerDatasetReadiness",
    "UHCFlexPractitionerPublicationError",
    "UHCFlexPractitionerPublicationResult",
    "UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID",
    "UHC_FLEX_PRACTITIONER_DATASET_ROOT_CONTRACT_ID",
)
