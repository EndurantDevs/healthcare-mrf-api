# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cross-service contracts for global Provider Directory Profile selection."""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Mapping

from process import provider_directory_profile as profile_artifact


PROFILE_SELECTION_REQUEST_CONTRACT_ID = (
    "healthporta.provider-directory-profile-selection-attestation-request.v1"
)
PROFILE_SELECTION_ATTESTATION_CONTRACT_ID = (
    "healthporta.provider-directory-profile-selection-attestation.v1"
)
PROFILE_SELECTION_RESULT_CONTRACT_ID = (
    "healthporta.provider-directory-profile-selection-result.v1"
)
PROFILE_EXECUTION_CONTRACT_ID = (
    "healthporta.provider-directory-profile-execution.v2"
)
PROFILE_SELECTION_LINEAGE_AUTHORITY = (
    "healthcare-mrf-api.provider-directory-selection-proof.v1"
)
PROFILE_SELECTION_RESULT_METRIC = "profile_selection_result"

PROFILE_SELECTION_DESIRED_REQUEST_CONTRACT_ID = (
    "healthporta.provider-directory-profile-selection-attestation-request.v2"
)
PROFILE_SELECTION_DESIRED_ATTESTATION_CONTRACT_ID = (
    "healthporta.provider-directory-profile-selection-attestation.v2"
)
PROFILE_SELECTION_DESIRED_RESULT_CONTRACT_ID = (
    "healthporta.provider-directory-profile-selection-result.v2"
)
_DESIRED_SELECTION_FIELDS = {
    "desired_profile_as_of", "desired_cms_dataset", "expected_cms_incumbent",
}


_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_PAIR_FIELDS = {
    "source_id",
    "endpoint_id",
    "dataset_id",
    "dataset_hash",
    "acquisition_root_run_id",
    "publication_status",
    "is_current",
    "lineage_authority",
}
_ATTESTATION_FIELDS = {
    "contract_id",
    "proof_id",
    "node_id",
    "catalog_digest",
    "selection_fingerprint",
    "authority_revision",
    "profile_schema_version",
    "profile_strategy_version",
    "source_context_digest",
    "profile_input_digest",
    "operation",
    "pairs",
}
_GLOBAL_PROFILE_PARAMS = {
    "publish_artifacts_only": True,
    "publish_artifacts_targets": ["profile"],
    "source_ids": [],
    "require_complete_global_profile_fence": True,
    "publish_corroboration": False,
    "probe": False,
    "import_resources": False,
    "provider_directory_profile_contract_id": PROFILE_EXECUTION_CONTRACT_ID,
}
CMS_CAPACITY_EXECUTION_PARAM = "provider_directory_cms_nonprofile_capacity_attestation"


class ProviderDirectoryProfileSelectionError(ValueError):
    """Report malformed Profile selection input or proof data."""


class ProviderDirectoryProfileSelectionDrift(RuntimeError):
    """Report a caller selection that differs from authoritative state."""


class ProviderDirectoryProfileSelectionStale(RuntimeError):
    """Report a registered proof that no longer matches live state."""


@dataclass(frozen=True)
class ProviderDirectoryProfileSelectionAttestation:
    """Validated immutable global Profile selection proof."""

    proof_id: str
    node_id: str
    catalog_digest: str
    selection_fingerprint: str
    authority_revision: int
    profile_schema_version: int
    profile_strategy_version: str
    source_context_digest: str
    profile_input_digest: str
    operation: str
    pairs: tuple[dict[str, Any], ...]
    payload: dict[str, Any]

    @property
    def desired_profile_as_of(self) -> str | None:
        """Return the explicit desired snapshot date, absent for legacy proofs."""
        return self.payload.get("desired_profile_as_of")

    @property
    def desired_cms_dataset(self) -> dict[str, Any] | None:
        """Return the proved CMS dataset without changing its publication state."""
        return self.payload.get("desired_cms_dataset")

    @property
    def expected_cms_incumbent(self) -> dict[str, Any] | None:
        """Return the exact required incumbent, absent only before first publication."""
        return self.payload.get("expected_cms_incumbent")


@dataclass(frozen=True)
class ProviderDirectoryProfileExecution:
    """Validated proof plus its exact durable outbox generation."""

    attestation: ProviderDirectoryProfileSelectionAttestation
    generation: int
    capacity_attestation: Mapping[str, Any] | None = None
    cms_nonprofile_capacity_attestation: Mapping[str, Any] | None = None


def stable_hash(payload: Any, *, domain: str) -> str:
    """Match the peer control plane's canonical JSON identity hash."""

    body = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(f"{domain}:{body}".encode("utf-8")).hexdigest()


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned_text = value.strip()
    return cleaned_text or None


def _required_text(
    value_map: Mapping[str, Any],
    name: str,
    *,
    limit: int,
) -> str:
    raw_text = value_map.get(name)
    cleaned_text = _clean_text(raw_text)
    if cleaned_text is None or cleaned_text != raw_text or len(cleaned_text) > limit:
        raise ProviderDirectoryProfileSelectionError(
            f"Profile selection {name} is invalid"
        )
    return cleaned_text


def _required_hash(value_map: Mapping[str, Any], name: str) -> str:
    digest = _required_text(value_map, name, limit=64)
    if not _HASH_PATTERN.fullmatch(digest):
        raise ProviderDirectoryProfileSelectionError(
            f"Profile selection {name} is invalid"
        )
    return digest


def _positive_integer(value_map: Mapping[str, Any], name: str) -> int:
    integer_value = value_map.get(name)
    if (
        not isinstance(integer_value, int)
        or isinstance(integer_value, bool)
        or integer_value < 1
    ):
        raise ProviderDirectoryProfileSelectionError(
            f"Profile selection {name} is invalid"
        )
    return integer_value


def configured_node_id() -> str:
    """Return the independently configured healthcare import-node identity."""

    node_id = _clean_text(os.getenv("HLTHPRT_IMPORT_NODE_ID"))
    if node_id is None or len(node_id) > 64:
        raise RuntimeError("provider_directory_profile_selection_node_not_configured")
    return node_id



def _validated_desired_selection(payload) -> dict[str, Any]:
    """Bind an exact CMS replacement or current-dataset date refresh."""
    date_text = _required_text(payload, "desired_profile_as_of", limit=10)
    try:
        if datetime.date.fromisoformat(date_text).isoformat() != date_text:
            raise ValueError
    except ValueError as exc:
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection desired_profile_as_of is invalid"
        ) from exc
    desired = _validated_pair(payload.get("desired_cms_dataset"), allow_desired=True)
    if desired["source_id"] != "cms-npd":
        raise ProviderDirectoryProfileSelectionError("Profile selection desired CMS source is invalid")
    raw_incumbent = payload.get("expected_cms_incumbent")
    incumbent = None if raw_incumbent is None else _validated_pair(raw_incumbent)
    if incumbent is not None and (
        incumbent["source_id"] != "cms-npd" or incumbent["endpoint_id"] != desired["endpoint_id"]
    ):
        raise ProviderDirectoryProfileSelectionError("Profile selection CMS incumbent is invalid")
    if desired["publication_status"] == "published":
        if desired != incumbent:
            raise ProviderDirectoryProfileSelectionError("Profile selection current CMS dataset differs from incumbent")
    elif incumbent is not None and desired["dataset_id"] == incumbent["dataset_id"]:
        raise ProviderDirectoryProfileSelectionError("Profile selection replacement CMS dataset equals incumbent")
    return {"desired_profile_as_of": date_text, "desired_cms_dataset": desired,
            "expected_cms_incumbent": incumbent}


def _attestation_fields(payload) -> set[str]:
    return _ATTESTATION_FIELDS | (
        _DESIRED_SELECTION_FIELDS
        if payload.get("contract_id") == PROFILE_SELECTION_DESIRED_ATTESTATION_CONTRACT_ID else set()
    )


def desired_selection_fingerprint(node_id, catalog_digest, datasets, desired_selection) -> str:
    """Hash the full retained vector, expected incumbent, and desired date."""
    return stable_hash({"node_id": node_id, "catalog_digest": catalog_digest,
                        "datasets": [[row["source_id"], row["dataset_id"]] for row in datasets],
                        **desired_selection}, domain="provider_directory_profile_desired_selection.v2")

def _proof_id(attestation_map: Mapping[str, Any]) -> str:
    proof_identity_map = {
        name: attestation_map[name]
        for name in sorted(
            _attestation_fields(attestation_map) - {"proof_id", "authority_revision"}
        )
    }
    return stable_hash(
        proof_identity_map,
        domain=("provider_directory_profile_selection_attestation.v2"
                if attestation_map["contract_id"] == PROFILE_SELECTION_DESIRED_ATTESTATION_CONTRACT_ID
                else "provider_directory_profile_selection_attestation.v1"),
    )


def _validated_pair(raw_pair: Any, *, allow_desired: bool = False) -> dict[str, Any]:
    if not isinstance(raw_pair, Mapping) or set(raw_pair) != _PAIR_FIELDS:
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection pair fields are invalid"
        )
    pair_map = dict(raw_pair)
    normalized_pair_map = {
        "source_id": _required_text(pair_map, "source_id", limit=96),
        "endpoint_id": _required_text(pair_map, "endpoint_id", limit=128),
        "dataset_id": _required_text(pair_map, "dataset_id", limit=128),
        "dataset_hash": _required_hash(pair_map, "dataset_hash"),
        "acquisition_root_run_id": _required_text(
            pair_map,
            "acquisition_root_run_id",
            limit=64,
        ),
        "publication_status": pair_map.get("publication_status"),
        "is_current": pair_map.get("is_current"),
        "lineage_authority": pair_map.get("lineage_authority"),
    }
    if (
        (not (normalized_pair_map["publication_status"] == "published"
              and normalized_pair_map["is_current"] is True)
         and not (allow_desired and normalized_pair_map["source_id"] == "cms-npd"
                  and normalized_pair_map["publication_status"] == "validated"
                  and normalized_pair_map["is_current"] is False))
        or normalized_pair_map["lineage_authority"]
        != PROFILE_SELECTION_LINEAGE_AUTHORITY
    ):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection pair publication authority is invalid"
        )
    return normalized_pair_map


def _normalized_attestation_pairs(
    attestation_map: Mapping[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    raw_pairs = attestation_map.get("pairs")
    if not isinstance(raw_pairs, list):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection pairs are invalid"
        )
    desired = (_validated_desired_selection(attestation_map)
               if attestation_map.get("contract_id") == PROFILE_SELECTION_DESIRED_ATTESTATION_CONTRACT_ID else None)
    normalized_pairs = [_validated_pair(raw_pair, allow_desired=bool(desired and raw_pair == desired["desired_cms_dataset"]))
                        for raw_pair in raw_pairs]
    if desired and (normalized_pairs.count(desired["desired_cms_dataset"]) != 1
                    or len({pair["source_id"] for pair in normalized_pairs}) != len(normalized_pairs)):
        raise ProviderDirectoryProfileSelectionError("Profile selection desired CMS vector is invalid")
    sorted_pairs = sorted(
        normalized_pairs,
        key=lambda pair_map: (
            pair_map["source_id"],
            pair_map["dataset_id"],
            pair_map["endpoint_id"],
        ),
    )
    pair_keys = [
        (pair_map["source_id"], pair_map["dataset_id"])
        for pair_map in normalized_pairs
    ]
    if normalized_pairs != sorted_pairs or len(pair_keys) != len(set(pair_keys)):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection pairs are not unique and sorted"
        )
    if desired and attestation_map["selection_fingerprint"] != desired_selection_fingerprint(
        attestation_map["node_id"], attestation_map["catalog_digest"],
        [{"source_id": pair["source_id"], "dataset_id": pair["dataset_id"]} for pair in normalized_pairs], desired
    ):
        raise ProviderDirectoryProfileSelectionError("Profile selection desired fingerprint is invalid")

    return ("publish" if normalized_pairs else "purge"), normalized_pairs


def _normalized_attestation_map(
    attestation_map: Mapping[str, Any],
) -> dict[str, Any]:
    operation, normalized_pairs = _normalized_attestation_pairs(attestation_map)
    return {
        "contract_id": attestation_map["contract_id"],
        **(_validated_desired_selection(attestation_map)
           if attestation_map["contract_id"] == PROFILE_SELECTION_DESIRED_ATTESTATION_CONTRACT_ID else {}),
        "proof_id": _required_hash(attestation_map, "proof_id"),
        "node_id": _required_text(attestation_map, "node_id", limit=64),
        "catalog_digest": _required_hash(attestation_map, "catalog_digest"),
        "selection_fingerprint": _required_hash(
            attestation_map,
            "selection_fingerprint",
        ),
        "authority_revision": _positive_integer(
            attestation_map,
            "authority_revision",
        ),
        "profile_schema_version": _positive_integer(
            attestation_map,
            "profile_schema_version",
        ),
        "profile_strategy_version": _required_text(
            attestation_map,
            "profile_strategy_version",
            limit=128,
        ),
        "source_context_digest": _required_hash(
            attestation_map,
            "source_context_digest",
        ),
        "profile_input_digest": _required_hash(
            attestation_map,
            "profile_input_digest",
        ),
        "operation": operation,
        "pairs": normalized_pairs,
    }


def validated_profile_selection_attestation(
    raw_attestation: Any,
) -> ProviderDirectoryProfileSelectionAttestation:
    """Validate the exact immutable attestation response contract."""

    if (
        not isinstance(raw_attestation, Mapping)
        or set(raw_attestation) != _attestation_fields(raw_attestation)
        or raw_attestation.get("contract_id") not in {
            PROFILE_SELECTION_ATTESTATION_CONTRACT_ID, PROFILE_SELECTION_DESIRED_ATTESTATION_CONTRACT_ID
        }
    ):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection attestation fields are invalid"
        )
    attestation_map = dict(raw_attestation)
    normalized_map = _normalized_attestation_map(attestation_map)
    if attestation_map != normalized_map:
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection attestation is not canonical"
        )
    if _proof_id(normalized_map) != normalized_map["proof_id"]:
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection proof_id is invalid"
        )
    return ProviderDirectoryProfileSelectionAttestation(
        proof_id=normalized_map["proof_id"],
        node_id=normalized_map["node_id"],
        catalog_digest=normalized_map["catalog_digest"],
        selection_fingerprint=normalized_map["selection_fingerprint"],
        authority_revision=normalized_map["authority_revision"],
        profile_schema_version=normalized_map["profile_schema_version"],
        profile_strategy_version=normalized_map["profile_strategy_version"],
        source_context_digest=normalized_map["source_context_digest"],
        profile_input_digest=normalized_map["profile_input_digest"],
        operation=normalized_map["operation"],
        pairs=tuple(dict(pair_map) for pair_map in normalized_map["pairs"]),
        payload=normalized_map,
    )


def _identity_without_authority(
    attestation_map: Mapping[str, Any],
) -> dict[str, Any]:
    identity_fields = (
        "contract_id",
        "node_id",
        "catalog_digest",
        "selection_fingerprint",
        "profile_schema_version",
        "profile_strategy_version",
        "source_context_digest",
        "profile_input_digest",
        "operation",
        "pairs",
    )
    return {name: attestation_map[name] for name in (*identity_fields,
        *(_DESIRED_SELECTION_FIELDS if attestation_map["contract_id"] == PROFILE_SELECTION_DESIRED_ATTESTATION_CONTRACT_ID else ())) }


def validated_profile_execution(
    task_map: Mapping[str, Any],
) -> ProviderDirectoryProfileExecution:
    """Validate the full proof-bearing global Profile execution contract."""

    for field_name, expected_value in _GLOBAL_PROFILE_PARAMS.items():
        if task_map.get(field_name) != expected_value:
            raise ProviderDirectoryProfileSelectionError(
                f"Profile execution {field_name} is invalid"
            )
    generation = _positive_integer(
        task_map,
        "provider_directory_profile_generation",
    )
    attestation = validated_profile_selection_attestation(
        task_map.get("provider_directory_profile_selection_attestation")
    )
    if attestation.desired_profile_as_of is not None:
        for name in ("profile_as_of", "provider_directory_profile_as_of"):
            if name in task_map and task_map[name] != attestation.desired_profile_as_of:
                raise ProviderDirectoryProfileSelectionError("Profile execution desired date does not match its proof")
    capacity_attestation = task_map.get(
        "provider_directory_profile_capacity_attestation"
    )
    if not isinstance(capacity_attestation, Mapping):
        raise ProviderDirectoryProfileSelectionError(
            "Profile execution capacity attestation is invalid"
        )
    if attestation.node_id != configured_node_id():
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection node does not match this engine"
        )
    return ProviderDirectoryProfileExecution(
        attestation,
        generation,
        dict(capacity_attestation),
        _cms_execution_capacity(task_map, attestation, generation, capacity_attestation),
    )


def _cms_execution_capacity(task_map, attestation, generation, profile_envelope):
    """Retain an explicit closed CMS pair without granting signature or runtime admission."""
    if CMS_CAPACITY_EXECUTION_PARAM not in task_map:
        return None
    from process.provider_directory_cms_capacity_contract import validated_cms_execution_capacity

    try:
        return validated_cms_execution_capacity(
            task_map[CMS_CAPACITY_EXECUTION_PARAM],
            profile_envelope=profile_envelope,
            attestation=attestation,
            generation=generation,
        )
    except (ValueError, KeyError, TypeError) as error:
        raise ProviderDirectoryProfileSelectionError("Profile execution CMS capacity attestation is invalid") from error


def _profile_result_counts(
    execution: ProviderDirectoryProfileExecution,
    profile_rows: int,
    evidence_rows: int,
) -> dict[str, int]:
    for row_count in (profile_rows, evidence_rows):
        if not isinstance(row_count, int) or isinstance(row_count, bool) or row_count < 0:
            raise ProviderDirectoryProfileSelectionError(
                "Profile result row count is invalid"
            )
    if execution.attestation.operation == "purge" and (profile_rows or evidence_rows):
        raise ProviderDirectoryProfileSelectionError(
            "Profile purge result is not empty"
        )
    return {
        "profile_rows": profile_rows,
        "profile_source_evidence_rows": evidence_rows,
        "source_count": len(execution.attestation.pairs),
        "dataset_count": len({
            pair_map["dataset_id"] for pair_map in execution.attestation.pairs
        }),
    }


def profile_selection_result(
    execution: ProviderDirectoryProfileExecution,
    *,
    profile_generation_id: str,
    profile_as_of: str,
    profile_rows: int,
    profile_source_evidence_rows: int,
) -> dict[str, Any]:
    """Build strict terminal evidence for one promoted Profile generation."""

    generation_id = _clean_text(profile_generation_id)
    if generation_id is None or len(generation_id) > 128:
        raise ProviderDirectoryProfileSelectionError(
            "Profile result generation identity is invalid"
        )
    normalized_profile_as_of = _clean_text(profile_as_of)
    try:
        if normalized_profile_as_of is None:
            raise ValueError
        parsed_profile_as_of = datetime.date.fromisoformat(
            normalized_profile_as_of
        )
        if parsed_profile_as_of.isoformat() != normalized_profile_as_of:
            raise ValueError
    except ValueError as exc:
        raise ProviderDirectoryProfileSelectionError(
            "Profile result profile_as_of is invalid"
        ) from exc
    attestation = execution.attestation
    if attestation.desired_profile_as_of is not None and normalized_profile_as_of != attestation.desired_profile_as_of:
        raise ProviderDirectoryProfileSelectionError("Profile result desired date does not match its proof")
    identity_fields = (
        "proof_id",
        "node_id",
        "catalog_digest",
        "selection_fingerprint",
        "authority_revision",
        "profile_schema_version",
        "profile_strategy_version",
        "source_context_digest",
        "profile_input_digest",
        "operation",
        "pairs",
    )
    return {
        "contract_id": (PROFILE_SELECTION_DESIRED_RESULT_CONTRACT_ID if attestation.desired_profile_as_of is not None
                        else PROFILE_SELECTION_RESULT_CONTRACT_ID),
        **({name: attestation.payload[name] for name in _DESIRED_SELECTION_FIELDS}
           if attestation.desired_profile_as_of is not None else {}),
        **{name: attestation.payload[name] for name in identity_fields},
        "status": "published" if attestation.operation == "publish" else "purged",
        "generation": execution.generation,
        "profile_generation_id": generation_id,
        "profile_as_of": normalized_profile_as_of,
        "row_counts": _profile_result_counts(
            execution,
            profile_rows,
            profile_source_evidence_rows,
        ),
    }
