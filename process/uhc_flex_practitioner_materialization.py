# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure normalization of exact Flex Practitioner query results."""

from __future__ import annotations

from dataclasses import dataclass
import datetime
import hashlib
import json
import re
from typing import Any
import urllib.parse

from db.models import ProviderDirectoryPractitioner
from process.provider_directory_fhir import (
    FHIRAcquisitionContext,
    parse_fhir_resource,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    canonical_practitioner_payload,
    resource_payload_sha256_for_contract,
)
from process.uhc_flex_official_cohort_contract import canonical_uhc_flex_npi
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_API_BASE,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_query import (
    UHCFlexPractitionerQueryResult,
    uhc_flex_practitioner_query_url,
)


_FHIR_ID_PATTERN = re.compile(r"[A-Za-z0-9\-.]{1,64}\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_DATASET_RESOURCE_FIELDS = frozenset(
    {
        "acquired_resource_sha256",
        "dataset_id",
        "payload_hash",
        "payload_json",
        "resource_id",
        "resource_type",
    }
)
_VOLATILE_NORMALIZED_FIELDS = frozenset(
    {
        "_acquired_resource_sha256",
        "last_seen_run_id",
        "observed_at",
        "source_id",
        "updated_at",
    }
)


class UHCFlexPractitionerMaterializationError(ValueError):
    """Reject materialization drift without retaining provider payloads."""

    def __init__(self, code: str = "result_invalid") -> None:
        message_by_code = {
            "dataset_id_invalid": "Flex Practitioner dataset ID is invalid",
            "normalized_payload_invalid": (
                "Flex Practitioner normalized payload is invalid"
            ),
            "raw_content_drift": ("Flex Practitioner acquired resource digest changed"),
            "resource_id_drift": ("Flex Practitioner normalized resource ID changed"),
            "resource_id_invalid": "Flex Practitioner resource ID is invalid",
            "resource_model_drift": (
                "Flex Practitioner parser returned another resource model"
            ),
            "resource_npi_drift": ("Flex Practitioner normalized NPI changed"),
            "result_invalid": "Flex Practitioner query result is invalid",
            "run_id_invalid": "Flex Practitioner run ID is invalid",
            "semantic_collision": (
                "Flex Practitioner semantic resource collision detected"
            ),
            "semantic_projection_as_of_invalid": (
                "Flex Practitioner semantic projection date is invalid"
            ),
            "source_drift": "Flex Practitioner source identity changed",
        }
        self.code = code if code in message_by_code else "result_invalid"
        super().__init__(message_by_code[self.code])


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (
        MemoryError,
        OverflowError,
        RecursionError,
        TypeError,
        UnicodeError,
        ValueError,
    ):
        raise UHCFlexPractitionerMaterializationError(
            "normalized_payload_invalid"
        ) from None


def _strict_text(
    value: object,
    *,
    maximum_length: int,
    error_code: str,
) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum_length
        or value != value.strip()
        or any(not character.isprintable() for character in value)
    ):
        raise UHCFlexPractitionerMaterializationError(error_code)
    return value


def _canonical_projection_date(value: object) -> datetime.date:
    if type(value) is not str or len(value) != 10 or value != value.strip():
        raise UHCFlexPractitionerMaterializationError(
            "semantic_projection_as_of_invalid"
        )
    try:
        projection_date = datetime.date.fromisoformat(value)
    except ValueError:
        raise UHCFlexPractitionerMaterializationError(
            "semantic_projection_as_of_invalid"
        ) from None
    if projection_date.isoformat() != value:
        raise UHCFlexPractitionerMaterializationError(
            "semantic_projection_as_of_invalid"
        )
    return projection_date


def _raw_resource_sha256(resource_by_field: dict[str, Any]) -> str:
    return hashlib.sha256(
        _canonical_json(resource_by_field).encode("utf-8")
    ).hexdigest()


def _normalized_payload(resource_row_by_field: dict[str, Any]) -> dict[str, Any]:
    payload_by_field = {
        key: value
        for key, value in resource_row_by_field.items()
        if key not in _VOLATILE_NORMALIZED_FIELDS
    }
    try:
        return canonical_practitioner_payload(payload_by_field)
    except (TypeError, ValueError):
        raise UHCFlexPractitionerMaterializationError(
            "normalized_payload_invalid"
        ) from None


def _dataset_resource_mapping(
    *,
    dataset_id: str,
    resource_id: str,
    payload_by_field: dict[str, Any],
    acquired_resource_sha256: str,
) -> dict[str, Any]:
    try:
        payload_hash = resource_payload_sha256_for_contract(
            payload_by_field,
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        )
    except (TypeError, ValueError):
        raise UHCFlexPractitionerMaterializationError(
            "normalized_payload_invalid"
        ) from None
    if _SHA256_PATTERN.fullmatch(payload_hash) is None:
        raise UHCFlexPractitionerMaterializationError("normalized_payload_invalid")
    return {
        "dataset_id": dataset_id,
        "resource_type": "Practitioner",
        "resource_id": resource_id,
        "payload_hash": payload_hash,
        "payload_json": payload_by_field,
        "acquired_resource_sha256": acquired_resource_sha256,
    }


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerMaterializedRow:
    """One immutable link from an exact NPI result to a retained row."""

    requested_npi: int
    resource_id: str
    acquired_resource_sha256: str
    _dataset_resource_json: str

    def __post_init__(self) -> None:
        try:
            canonical_npi = canonical_uhc_flex_npi(self.requested_npi)
        except ValueError:
            raise UHCFlexPractitionerMaterializationError(
                "normalized_payload_invalid"
            ) from None
        try:
            dataset_resource_by_field = json.loads(self._dataset_resource_json)
        except (MemoryError, RecursionError, UnicodeError, ValueError):
            raise UHCFlexPractitionerMaterializationError(
                "normalized_payload_invalid"
            ) from None
        if (
            type(self.requested_npi) is not int
            or type(self.resource_id) is not str
            or _FHIR_ID_PATTERN.fullmatch(self.resource_id) is None
            or type(self.acquired_resource_sha256) is not str
            or _SHA256_PATTERN.fullmatch(self.acquired_resource_sha256) is None
            or type(dataset_resource_by_field) is not dict
            or set(dataset_resource_by_field) != _DATASET_RESOURCE_FIELDS
            or type(dataset_resource_by_field.get("dataset_id")) is not str
            or not dataset_resource_by_field["dataset_id"]
            or len(dataset_resource_by_field["dataset_id"]) > 96
            or dataset_resource_by_field["dataset_id"]
            != dataset_resource_by_field["dataset_id"].strip()
            or dataset_resource_by_field.get("resource_type") != "Practitioner"
            or dataset_resource_by_field.get("resource_id") != self.resource_id
            or dataset_resource_by_field.get("acquired_resource_sha256")
            != self.acquired_resource_sha256
            or _canonical_json(dataset_resource_by_field) != self._dataset_resource_json
        ):
            raise UHCFlexPractitionerMaterializationError("normalized_payload_invalid")
        payload_by_field = dataset_resource_by_field.get("payload_json")
        if (
            type(payload_by_field) is not dict
            or payload_by_field.get("resource_id") != self.resource_id
            or payload_by_field.get("npi") != canonical_npi
            or not _VOLATILE_NORMALIZED_FIELDS.isdisjoint(payload_by_field)
        ):
            raise UHCFlexPractitionerMaterializationError("normalized_payload_invalid")
        try:
            expected_payload_hash = resource_payload_sha256_for_contract(
                payload_by_field,
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            )
        except (TypeError, ValueError):
            raise UHCFlexPractitionerMaterializationError(
                "normalized_payload_invalid"
            ) from None
        if dataset_resource_by_field.get("payload_hash") != expected_payload_hash:
            raise UHCFlexPractitionerMaterializationError("normalized_payload_invalid")

    @property
    def dataset_resource(self) -> dict[str, Any]:
        """Return a fresh persistence mapping without exposing row state."""

        return json.loads(self._dataset_resource_json)


def materialize_uhc_flex_practitioner_result(
    result: UHCFlexPractitionerQueryResult,
    *,
    dataset_id: str,
    source_id: str,
    run_id: str,
    semantic_projection_as_of: str,
) -> tuple[UHCFlexPractitionerMaterializedRow, ...]:
    """Map one validated exact-NPI result into semantic-v3 dataset rows."""

    if type(result) is not UHCFlexPractitionerQueryResult:
        raise UHCFlexPractitionerMaterializationError("result_invalid")
    canonical_dataset_id = _strict_text(
        dataset_id,
        maximum_length=96,
        error_code="dataset_id_invalid",
    )
    if source_id != UHC_FLEX_PRACTITIONER_SOURCE_ID:
        raise UHCFlexPractitionerMaterializationError("source_drift")
    canonical_run_id = _strict_text(
        run_id,
        maximum_length=64,
        error_code="run_id_invalid",
    )
    projection_date = _canonical_projection_date(semantic_projection_as_of)
    if result.is_unmatched:
        return ()

    raw_hash_by_resource_id = dict(result.resource_sha256_by_id)
    raw_payloads = result.resource_payloads()
    if (
        len(raw_hash_by_resource_id) != result.resource_count
        or len(raw_payloads) != result.resource_count
    ):
        raise UHCFlexPractitionerMaterializationError("result_invalid")

    materialized_by_id: dict[str, UHCFlexPractitionerMaterializedRow] = {}
    payload_json_by_hash: dict[str, str] = {}
    fetch_url = uhc_flex_practitioner_query_url(result.requested_npi)
    for expected_resource_id, resource_by_field in zip(
        result.resource_ids,
        raw_payloads,
        strict=True,
    ):
        if (
            type(expected_resource_id) is not str
            or _FHIR_ID_PATTERN.fullmatch(expected_resource_id) is None
        ):
            raise UHCFlexPractitionerMaterializationError("resource_id_invalid")
        acquired_resource_sha256 = raw_hash_by_resource_id.get(expected_resource_id)
        if (
            type(acquired_resource_sha256) is not str
            or _SHA256_PATTERN.fullmatch(acquired_resource_sha256) is None
            or acquired_resource_sha256 != _raw_resource_sha256(resource_by_field)
        ):
            raise UHCFlexPractitionerMaterializationError("raw_content_drift")
        resource_url = (
            f"{UHC_FLEX_PRACTITIONER_API_BASE}/Practitioner/"
            f"{urllib.parse.quote(expected_resource_id, safe='')}"
        )
        parsed = parse_fhir_resource(
            source_id,
            resource_by_field,
            resource_url=resource_url,
            acquisition=FHIRAcquisitionContext(
                self_url=resource_url,
                fetch_url=fetch_url,
                fetch_mode="rest_bundle",
                semantic_projection_as_of=projection_date,
            ),
            run_id=canonical_run_id,
        )
        if parsed is None or type(parsed) is not tuple or len(parsed) != 2:
            raise UHCFlexPractitionerMaterializationError("resource_model_drift")
        model, resource_row_by_field = parsed
        if model is not ProviderDirectoryPractitioner:
            raise UHCFlexPractitionerMaterializationError("resource_model_drift")
        if type(resource_row_by_field) is not dict:
            raise UHCFlexPractitionerMaterializationError("normalized_payload_invalid")
        normalized_resource_id = resource_row_by_field.get("resource_id")
        if normalized_resource_id != expected_resource_id:
            raise UHCFlexPractitionerMaterializationError("resource_id_drift")
        if resource_row_by_field.get("npi") != result.requested_npi:
            raise UHCFlexPractitionerMaterializationError("resource_npi_drift")
        if (
            resource_row_by_field.get("source_id") != source_id
            or resource_row_by_field.get("last_seen_run_id") != canonical_run_id
        ):
            raise UHCFlexPractitionerMaterializationError("source_drift")

        payload_by_field = _normalized_payload(resource_row_by_field)
        dataset_resource_by_field = _dataset_resource_mapping(
            dataset_id=canonical_dataset_id,
            resource_id=expected_resource_id,
            payload_by_field=payload_by_field,
            acquired_resource_sha256=acquired_resource_sha256,
        )
        dataset_resource_json = _canonical_json(dataset_resource_by_field)
        payload_hash = dataset_resource_by_field["payload_hash"]
        payload_json = _canonical_json(payload_by_field)
        previous_payload_json = payload_json_by_hash.get(payload_hash)
        if previous_payload_json is not None and previous_payload_json != payload_json:
            raise UHCFlexPractitionerMaterializationError("semantic_collision")
        payload_json_by_hash[payload_hash] = payload_json
        materialized_row = UHCFlexPractitionerMaterializedRow(
            requested_npi=result.requested_npi,
            resource_id=expected_resource_id,
            acquired_resource_sha256=acquired_resource_sha256,
            _dataset_resource_json=dataset_resource_json,
        )
        previous_row = materialized_by_id.get(expected_resource_id)
        if (
            previous_row is not None
            and previous_row.dataset_resource != materialized_row.dataset_resource
        ):
            raise UHCFlexPractitionerMaterializationError("semantic_collision")
        materialized_by_id[expected_resource_id] = materialized_row

    if len(materialized_by_id) != result.resource_count:
        raise UHCFlexPractitionerMaterializationError("semantic_collision")
    return tuple(
        materialized_by_id[resource_id] for resource_id in sorted(materialized_by_id)
    )


__all__ = (
    "materialize_uhc_flex_practitioner_result",
    "UHCFlexPractitionerMaterializationError",
    "UHCFlexPractitionerMaterializedRow",
)
