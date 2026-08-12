# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed gates and deterministic identities for the manual graph operator."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
import re
from typing import Any


REGISTRATION_ENABLED_ENV = (
    "HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_REGISTRATION_ENABLED"
)
ACQUISITION_ENABLED_ENV = "HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_ENABLED"
PUBLICATION_ENABLED_ENV = "HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_ENABLED"
OPERATOR_PHASES = ("register", "acquire", "publish")
_GATE_BY_PHASE = {
    "register": REGISTRATION_ENABLED_ENV,
    "acquire": ACQUISITION_ENABLED_ENV,
    "publish": PUBLICATION_ENABLED_ENV,
}
PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-manual-operator.v1"
)
_OPERATOR_CONTRACT_PAYLOAD = {
    "acquisition": "deterministic-baseline-then-candidate-exact-current-root",
    "admission": "exact-sealed-twin-match-still-current",
    "contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_ID,
    "gates": {
        "acquire": ACQUISITION_ENABLED_ENV,
        "publish": PUBLICATION_ENABLED_ENV,
        "register": REGISTRATION_ENABLED_ENV,
    },
    "operation_key": "required-exact-lowercase-sha256-resume-selector",
    "publication_selector": "exact-publication-acquisition-id",
    "registration": "insert-or-exact-validate-only",
    "scheduling": "none",
}
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_ACQUISITION_PATTERN = re.compile(r"pdrga_[0-9a-f]{48}\Z")
_PRESERVED_ERROR_CODES = frozenset(
    {
        "admission",
        "both_current",
        "busy",
        "content",
        "drift",
        "foreign_current",
        "identity",
        "input_drift",
        "mismatch",
        "missing",
        "replay",
        "root_unsealable",
        "source_drift",
        "stale",
        "state",
    }
)
_MESSAGE_BY_CODE = {
    "acquisition": "rooted graph acquisition failed",
    "admission": "rooted graph admission is invalid",
    "both_current": "rooted graph current dataset is ambiguous",
    "busy": "rooted graph source is busy",
    "content": "rooted graph content is invalid",
    "disabled": "rooted graph operator phase is disabled",
    "drift": "rooted graph registration has drifted",
    "evidence": "rooted graph operator evidence is invalid",
    "foreign_current": "rooted graph current dataset is unsupported",
    "gate_conflict": "rooted graph operator gates conflict",
    "identity": "rooted graph operation identity is invalid",
    "input_drift": "rooted graph acquisition input changed",
    "invalid_request": "rooted graph operator request is invalid",
    "mismatch": "rooted graph independent acquisitions do not match",
    "missing": "rooted graph exact evidence is missing",
    "publication": "rooted graph publication failed",
    "registration": "rooted graph registration failed",
    "replay": "rooted graph publication replay is invalid",
    "root_unsealable": "rooted graph acquisition cannot be sealed",
    "source_drift": "rooted graph exact source has drifted",
    "stale": "rooted graph acquisition root is no longer current",
    "state": "rooted graph operation state is invalid",
}


class ProviderDirectoryRootedGraphOperatorError(RuntimeError):
    """Expose one bounded failure without URLs, resources, or selectors."""

    def __init__(self, code: str = "evidence") -> None:
        self.code = code if code in _MESSAGE_BY_CODE else "evidence"
        super().__init__(_MESSAGE_BY_CODE[self.code])


def _canonical_json(document: object) -> str:
    try:
        return json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (MemoryError, OverflowError, RecursionError, TypeError, ValueError):
        raise ProviderDirectoryRootedGraphOperatorError("evidence") from None


PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_SHA256 = hashlib.sha256(
    _canonical_json(_OPERATOR_CONTRACT_PAYLOAD).encode("utf-8")
).hexdigest()


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphOperatorIdentities:
    """Keep one deterministic intent and its isolated baseline/candidate roots."""

    operation_key: str = field(repr=False)
    dataset_intent_id: str = field(repr=False)
    scope: Any = field(repr=False)
    baseline: Any = field(repr=False)
    candidate: Any = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.operation_key) is not str
            or _SHA256_PATTERN.fullmatch(self.operation_key) is None
            or getattr(self.baseline, "dataset_intent_id", None)
            != self.dataset_intent_id
            or getattr(self.candidate, "dataset_intent_id", None)
            != self.dataset_intent_id
            or getattr(self.baseline, "scope_id", None)
            != getattr(self.scope, "scope_id", None)
            or getattr(self.candidate, "scope_id", None)
            != getattr(self.scope, "scope_id", None)
            or getattr(self.baseline, "acquisition_role", None) != "baseline"
            or getattr(self.candidate, "acquisition_role", None) != "candidate"
            or getattr(self.baseline, "acquisition_id", None)
            == getattr(self.candidate, "acquisition_id", None)
            or getattr(self.baseline, "run_id", None)
            == getattr(self.candidate, "run_id", None)
        ):
            raise ValueError(
                "provider_directory_rooted_graph_operator_identity_invalid"
            )


def rooted_graph_operator_contract_payload() -> dict[str, Any]:
    """Return a fresh copy of the closed manual-operation contract."""

    return json.loads(_canonical_json(_OPERATOR_CONTRACT_PAYLOAD))


def require_rooted_graph_operator_gate(phase: str) -> None:
    """Require exactly one lowercase-true phase before runtime imports."""

    expected_gate = _GATE_BY_PHASE.get(phase)
    if expected_gate is None:
        raise ProviderDirectoryRootedGraphOperatorError("invalid_request")
    if phase == "acquire":
        raise ProviderDirectoryRootedGraphOperatorError("disabled")
    enabled_gates = {
        gate_name
        for gate_name in _GATE_BY_PHASE.values()
        if os.getenv(gate_name, "") == "true"
    }
    if len(enabled_gates) > 1:
        raise ProviderDirectoryRootedGraphOperatorError("gate_conflict")
    if enabled_gates != {expected_gate}:
        raise ProviderDirectoryRootedGraphOperatorError("disabled")


def _digest_identifier(prefix: str, payload: object) -> str:
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return prefix + digest[:48]


def _exact_operation_key(operation_key: object) -> str:
    if (
        type(operation_key) is not str
        or _SHA256_PATTERN.fullmatch(operation_key) is None
    ):
        raise ValueError("provider_directory_rooted_graph_operation_key_invalid")
    return operation_key


def _exact_publication_acquisition_id(selector: object) -> str:
    if type(selector) is not str or _ACQUISITION_PATTERN.fullmatch(selector) is None:
        raise ValueError("provider_directory_rooted_graph_publication_selector_invalid")
    return selector


def _exact_current_payload(current: Any) -> dict[str, Any]:
    field_names = (
        "dataset_id",
        "endpoint_id",
        "source_id",
        "root_source_id",
        "root_endpoint_id",
        "acquisition_source_id",
        "acquisition_endpoint_id",
        "practitioner_origin_source_id",
        "practitioner_origin_endpoint_id",
        "source_authority_id",
        "endpoint_signature_sha256",
        "dataset_hash",
        "resource_count",
        "practitioner_resource_count",
        "root_content_proof_sha256",
        "root_cohort_id",
        "semantic_projection_as_of",
        "operation_key",
        "acquisition_root_run_id",
        "variant",
        "root_publication_contract_id",
    )
    return {field_name: getattr(current, field_name) for field_name in field_names}


def _operator_scope(current: Any) -> Any:
    """Bind acquisition limits and lineage to the selected current root."""

    from process.provider_directory_rooted_graph_identity import (
        build_provider_directory_rooted_graph_scope,
    )

    return build_provider_directory_rooted_graph_scope(
        root_dataset_variant=current.variant,
        root_publication_contract_id=current.root_publication_contract_id,
        root_source_id=current.root_source_id,
        root_endpoint_id=current.root_endpoint_id,
        acquisition_source_id=current.acquisition_source_id,
        acquisition_endpoint_id=current.acquisition_endpoint_id,
        source_authority_id=current.source_authority_id,
        root_dataset_id=current.dataset_id,
        root_dataset_hash=current.dataset_hash,
        root_content_proof_sha256=current.root_content_proof_sha256,
        root_resource_count=current.practitioner_resource_count,
    )


def _root_acquisition_identity(
    current: Any,
    scope: Any,
    operation_key: str,
    dataset_intent_id: str,
    acquisition_role: str,
) -> Any:
    """Build one stable role-specific run inside a shared dataset intent."""

    from process.provider_directory_rooted_graph_store_contract import (
        build_provider_directory_rooted_graph_acquisition_identity,
    )

    run_id = _digest_identifier(
        "pdrgr_",
        {
            "acquisition_role": acquisition_role,
            "dataset_intent_id": dataset_intent_id,
            "operation_key": operation_key,
            "operator_contract_sha256": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_SHA256
            ),
        },
    )
    return build_provider_directory_rooted_graph_acquisition_identity(
        scope,
        root_cohort_id=current.root_cohort_id,
        endpoint_signature_sha256=current.endpoint_signature_sha256,
        acquisition_role=acquisition_role,
        run_id=run_id,
        dataset_intent_id=dataset_intent_id,
    )


def build_rooted_graph_operator_identities(
    current: Any,
    *,
    operation_key: str,
) -> ProviderDirectoryRootedGraphOperatorIdentities:
    """Derive stable twin identities from only one exact current root."""

    from process.provider_directory_dataset_scoped_publication_contract import (
        ExactCurrentDataset,
    )

    if type(current) is not ExactCurrentDataset:
        raise ValueError("provider_directory_rooted_graph_operator_root_invalid")
    exact_operation_key = _exact_operation_key(operation_key)
    scope = _operator_scope(current)
    dataset_intent_id = _digest_identifier(
        "pdrgi_",
        {
            "operation_key": exact_operation_key,
            "operator_contract_sha256": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_SHA256
            ),
            "root": _exact_current_payload(current),
            "scope_id": scope.scope_id,
        },
    )

    return ProviderDirectoryRootedGraphOperatorIdentities(
        operation_key=exact_operation_key,
        dataset_intent_id=dataset_intent_id,
        scope=scope,
        baseline=_root_acquisition_identity(
            current,
            scope,
            exact_operation_key,
            dataset_intent_id,
            "baseline",
        ),
        candidate=_root_acquisition_identity(
            current,
            scope,
            exact_operation_key,
            dataset_intent_id,
            "candidate",
        ),
    )


def _operation_error(error: Exception, default_code: str) -> Exception:
    code = getattr(error, "code", None)
    if type(code) is str and code in _PRESERVED_ERROR_CODES:
        return ProviderDirectoryRootedGraphOperatorError(code)
    return ProviderDirectoryRootedGraphOperatorError(default_code)


__all__ = (
    "ACQUISITION_ENABLED_ENV",
    "OPERATOR_PHASES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_SHA256",
    "PUBLICATION_ENABLED_ENV",
    "ProviderDirectoryRootedGraphOperatorError",
    "ProviderDirectoryRootedGraphOperatorIdentities",
    "REGISTRATION_ENABLED_ENV",
    "build_rooted_graph_operator_identities",
    "require_rooted_graph_operator_gate",
    "rooted_graph_operator_contract_payload",
)
