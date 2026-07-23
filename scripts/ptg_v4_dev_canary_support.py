"""Pure validation helpers for the read-only PTG V4 dev canary."""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from scripts.ptg_v4_dev_canary_cold_evidence import (
    ColdEvidenceExpectation,
    validated_cold_latencies,
)
from scripts.ptg_v4_dev_canary_metrics import (
    GraphReadLimits,
    evaluate_graph_metric_delta,
    parse_graph_metrics,
)


GIBIBYTE = 1 << 30
ROOT_COUNT_FIELDS = frozenset(
    {
        "object_kind_count",
        "map_pack_count",
        "coordinate_count",
        "entry_count",
        "logical_byte_count",
        "stored_map_byte_count",
        "npi_count",
        "component_count",
        "pattern_count",
        "relation_count",
        "heavy_owner_count",
    }
)
RELATION_COUNT_FIELDS = frozenset(
    {
        "owner_count",
        "logical_member_count",
        "vector_member_count",
        "member_width",
        "member_page_bytes",
        "locator_page_bytes",
        "locator_owner_span",
    }
)
PROTECTED_API_PARAMETERS = frozenset(
    {
        "snapshot_id",
        "code_system",
        "code",
        "npi",
        "limit",
        "offset",
        "include_providers",
        "order_by",
        "order",
    }
)
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HEADER_PATTERN = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")


class CanaryConfigurationError(ValueError):
    """Raised when a canary input would be unsafe or ambiguous."""


@dataclass(frozen=True)
class ElapsedBudget:
    """Size-derived import ceiling inputs."""

    compressed_input_bytes: int
    component_fact_count: int
    fixed_seconds: float
    seconds_per_input_gib: float
    seconds_per_million_component_facts: float

    @property
    def ceiling_seconds(self) -> float:
        """Calculate the size- and factor-derived elapsed-time ceiling."""

        return (
            self.fixed_seconds
            + (self.compressed_input_bytes / GIBIBYTE) * self.seconds_per_input_gib
            + (self.component_fact_count / 1_000_000)
            * self.seconds_per_million_component_facts
        )

    def report(self) -> dict[str, int | float]:
        """Return the exact elapsed-budget inputs and calculated ceiling."""

        return {
            "compressed_input_bytes": self.compressed_input_bytes,
            "component_fact_count": self.component_fact_count,
            "fixed_seconds": self.fixed_seconds,
            "seconds_per_input_gib": self.seconds_per_input_gib,
            "seconds_per_million_component_facts": (
                self.seconds_per_million_component_facts
            ),
            "ceiling_seconds": round(self.ceiling_seconds, 3),
        }


@dataclass(frozen=True)
class BoundedApiSpec:
    """Exact query whose warm p95 is eligible for the latency gate."""

    snapshot_id: str
    npi: int | None
    code_system: str
    code: str
    limit: int
    expected_item_count: int
    expected_result_state: str
    cold_p95_limit_ms: float
    warm_p95_limit_ms: float
    minimum_cold_process_samples: int

    def parameters(self, extra_parameters: Mapping[str, str]) -> dict[str, str]:
        """Return the fixed bounded query with caller extras unable to override it."""

        parameter_by_name = {
            **dict(extra_parameters),
            "snapshot_id": self.snapshot_id,
            "code_system": self.code_system,
            "code": self.code,
            "limit": str(self.limit),
            "offset": "0",
            "include_providers": "true",
            "order_by": "negotiated_rate",
            "order": "asc",
        }
        if self.npi is not None:
            parameter_by_name["npi"] = str(self.npi)
        return parameter_by_name


def validate_identifier(value: str, *, label: str) -> str:
    """Return a safe simple SQL identifier or reject the canary input."""

    normalized = str(value or "").strip()
    if not _IDENTIFIER_PATTERN.fullmatch(normalized):
        raise CanaryConfigurationError(f"{label} must be a simple SQL identifier")
    return normalized


def validate_base_url(value: str, *, allow_insecure_http: bool) -> str:
    """Return a credential-free service base URL using an allowed scheme."""

    normalized = str(value or "").strip().rstrip("/")
    parsed = urlsplit(normalized)
    allowed_schemes = {"https"} | ({"http"} if allow_insecure_http else set())
    if parsed.scheme not in allowed_schemes or not parsed.hostname:
        raise CanaryConfigurationError("service URLs must use an allowed HTTP scheme")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise CanaryConfigurationError(
            "service URLs cannot contain credentials, query parameters, or fragments"
        )
    return normalized


def parse_string_pairs(
    values: Sequence[str],
    *,
    label: str,
    protected_keys: frozenset[str] = frozenset(),
) -> dict[str, str]:
    """Parse unique key-value declarations while protecting fixed query keys."""

    parsed_by_key: dict[str, str] = {}
    for raw_value in values:
        key, separator, value = str(raw_value).partition("=")
        key = key.strip()
        if not separator or not key or not value:
            raise CanaryConfigurationError(f"{label} must use KEY=VALUE")
        if key in protected_keys:
            raise CanaryConfigurationError(f"{label} cannot override {key}")
        if key in parsed_by_key:
            raise CanaryConfigurationError(f"{label} repeats {key}")
        parsed_by_key[key] = value
    return parsed_by_key


def headers_from_environment(
    specifications: Sequence[str],
    *,
    environ: Mapping[str, str] | None = None,
) -> tuple[dict[str, str], list[str]]:
    """Resolve HTTP headers from named environment variables without logging values."""

    environment = os.environ if environ is None else environ
    header_by_name: dict[str, str] = {}
    environment_names: list[str] = []
    for raw_specification in specifications:
        header_name, separator, environment_name = raw_specification.partition("=")
        header_name = header_name.strip()
        environment_name = environment_name.strip()
        if (
            not separator
            or not _HEADER_PATTERN.fullmatch(header_name)
            or not _IDENTIFIER_PATTERN.fullmatch(environment_name)
        ):
            raise CanaryConfigurationError(
                "header inputs must use HTTP-Header=ENVIRONMENT_VARIABLE"
            )
        secret_value = environment.get(environment_name)
        if not secret_value:
            raise CanaryConfigurationError(
                f"required header environment variable is unset: {environment_name}"
            )
        header_by_name[header_name] = secret_value
        environment_names.append(environment_name)
    return header_by_name, environment_names


def parse_count_expectations(
    declarations: Sequence[str],
    *,
    allowed_fields: frozenset[str],
    relation_scoped: bool,
) -> dict[str, int]:
    """Parse exact non-negative count expectations for root or relation fields."""

    expectation_by_field: dict[str, int] = {}
    for raw_value in declarations:
        key, separator, count_text = str(raw_value).partition("=")
        key = key.strip()
        field_name = key.rsplit(".", 1)[-1]
        if (
            not separator
            or not key
            or field_name not in allowed_fields
            or (relation_scoped and "." not in key)
            or (not relation_scoped and "." in key)
        ):
            scope = "RELATION.FIELD" if relation_scoped else "FIELD"
            raise CanaryConfigurationError(f"count expectations must use {scope}=INTEGER")
        try:
            count = int(count_text)
        except ValueError as exc:
            raise CanaryConfigurationError("expected counts must be integers") from exc
        if count < 0 or key in expectation_by_field:
            raise CanaryConfigurationError(
                "expected counts must be unique non-negative integers"
            )
        expectation_by_field[key] = count
    return expectation_by_field


def nearest_rank(samples: Sequence[float], quantile: float) -> float:
    """Return the deterministic nearest-rank quantile for non-empty samples."""

    if not samples:
        raise ValueError("latency samples cannot be empty")
    if not 0 < quantile <= 1:
        raise ValueError("quantile must be within (0, 1]")
    ordered_samples = sorted(float(sample) for sample in samples)
    index = max(math.ceil(quantile * len(ordered_samples)) - 1, 0)
    return ordered_samples[index]


def latency_summary(samples: Sequence[float]) -> dict[str, int | float]:
    """Summarize bounded latency samples with the exact gate quantiles."""

    return {
        "sample_count": len(samples),
        "minimum_ms": round(min(samples), 3),
        "p50_ms": round(nearest_rank(samples, 0.50), 3),
        "p95_ms": round(nearest_rank(samples, 0.95), 3),
        "maximum_ms": round(max(samples), 3),
    }


def evaluate_api_probe(
    *,
    spec: BoundedApiSpec,
    cold_sample_evidence: Sequence[Mapping[str, Any]],
    warm_samples_ms: Sequence[float],
    response_documents: Sequence[Mapping[str, Any]],
    graph_read_evidence: Mapping[str, Any],
    semantic_reference: Mapping[str, Any] | None = None,
    expected_query_fingerprint: str | None = None,
    cold_not_before: str | None = None,
) -> dict[str, Any]:
    """Evaluate response shape, unique cold processes, latency, and graph I/O."""

    failures = _response_failures(
        spec,
        response_documents,
        semantic_reference,
    )
    failures.extend(
        str(graph_failure)
        for graph_failure in graph_read_evidence.get("failures", [])
    )
    cold_samples_ms = validated_cold_latencies(
        cold_sample_evidence,
        failures,
        _cold_evidence_expectation(
            spec,
            semantic_reference=semantic_reference,
            expected_query_fingerprint=expected_query_fingerprint,
            graph_read_evidence=graph_read_evidence,
            cold_not_before=cold_not_before,
        ),
    )
    cold_summary_by_field = _latency_gate(
        cold_samples_ms,
        limit_ms=spec.cold_p95_limit_ms,
        label="cold",
        failures=failures,
    )
    warm_summary_by_field = _latency_gate(
        warm_samples_ms,
        limit_ms=spec.warm_p95_limit_ms,
        label="warm",
        failures=failures,
    )
    return _api_probe_report(
        spec,
        failures=failures,
        cold_sample_evidence=cold_sample_evidence,
        cold_summary_by_field=cold_summary_by_field,
        warm_summary_by_field=warm_summary_by_field,
        graph_read_evidence=graph_read_evidence,
    )


def _response_failures(
    spec: BoundedApiSpec,
    response_documents: Sequence[Mapping[str, Any]],
    semantic_reference: Mapping[str, Any] | None,
) -> list[str]:
    """Collect shape and frozen-reference failures for every warm document."""

    failures: list[str] = []
    for document_by_field in response_documents:
        failures.extend(validate_api_document(document_by_field, spec))
        if semantic_reference is None:
            continue
        from scripts.ptg_v4_dev_canary_reference import (
            compare_v4_document_to_reference,
        )

        semantic_failures, _semantic_page = (
            compare_v4_document_to_reference(
                document_by_field,
                spec,
                semantic_reference,
            )
        )
        failures.extend(semantic_failures)
    return failures


def _cold_evidence_expectation(
    spec: BoundedApiSpec,
    *,
    semantic_reference: Mapping[str, Any] | None,
    expected_query_fingerprint: str | None,
    graph_read_evidence: Mapping[str, Any],
    cold_not_before: str | None,
) -> ColdEvidenceExpectation:
    """Bind persisted samples to the exact V3 page, V4 build, and query."""

    runtime_identity_by_field = _mapping(
        graph_read_evidence.get("runtime_identity")
    )
    return ColdEvidenceExpectation(
        minimum_sample_count=spec.minimum_cold_process_samples,
        semantic_page_digest=(
            str(semantic_reference.get("page_digest") or "")
            if semantic_reference is not None
            else None
        ),
        reference_snapshot_id=(
            str(semantic_reference.get("reference_snapshot_id") or "")
            if semantic_reference is not None
            else None
        ),
        v4_snapshot_id=spec.snapshot_id,
        query_fingerprint=expected_query_fingerprint,
        image_identity=(
            str(runtime_identity_by_field.get("image_identity") or "")
            if semantic_reference is not None
            else None
        ),
        not_before=cold_not_before,
    )


def _latency_gate(
    samples_ms: Sequence[float],
    *,
    limit_ms: float,
    label: str,
    failures: list[str],
) -> dict[str, int | float]:
    """Summarize latency and append one deterministic p95 gate failure."""

    summary_by_field = latency_summary(samples_ms)
    if float(summary_by_field["p95_ms"]) > limit_ms:
        failures.append(
            f"bounded {label} p95 {summary_by_field['p95_ms']}ms exceeds "
            f"{limit_ms:.3f}ms"
        )
    return summary_by_field


def _api_probe_report(
    spec: BoundedApiSpec,
    *,
    failures: Sequence[str],
    cold_sample_evidence: Sequence[Mapping[str, Any]],
    cold_summary_by_field: Mapping[str, int | float],
    warm_summary_by_field: Mapping[str, int | float],
    graph_read_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Shape the bounded public serving report after all checks complete."""

    return {
        "passed": not failures,
        "failures": sorted(set(failures)),
        "query": {
            "snapshot_id": spec.snapshot_id,
            "code_system": spec.code_system,
            "code": spec.code,
            "limit": spec.limit,
            "npi": spec.npi,
            "order_by": "negotiated_rate",
            "order": "asc",
        },
        "cold": {
            **cold_summary_by_field,
            "gate_applied": True,
            "p95_limit_ms": spec.cold_p95_limit_ms,
            "meaning": "first V4 request from a unique fresh API process",
            "unique_process_count": len(
                {str(sample.get("process_identity")) for sample in cold_sample_evidence}
            ),
            "minimum_unique_process_count": spec.minimum_cold_process_samples,
        },
        "warm": {
            **warm_summary_by_field,
            "gate_applied": True,
            "p95_limit_ms": spec.warm_p95_limit_ms,
        },
        "bounded_read_counts": dict(graph_read_evidence),
    }


def validate_api_document(
    document_by_field: Mapping[str, Any],
    spec: BoundedApiSpec,
) -> list[str]:
    """Return all contract failures for one bounded public API response."""

    failures: list[str] = []
    if document_by_field.get("result_state") != spec.expected_result_state:
        failures.append("bounded API result_state differs from the expectation")
    items = document_by_field.get("items")
    item_rows = list(items) if isinstance(items, list) else []
    if len(item_rows) != spec.expected_item_count:
        failures.append("bounded API item cardinality differs from the expectation")
    for item in item_rows:
        if not isinstance(item, Mapping):
            failures.append("bounded API returned a non-object item")
            continue
        if spec.npi is not None and _optional_int(item.get("npi")) != spec.npi:
            failures.append("bounded API returned an unexpected NPI")
        if str(item.get("reported_code") or "") != spec.code:
            failures.append("bounded API returned an unexpected procedure code")
    provenance = _mapping(document_by_field.get("provenance"))
    if provenance.get("snapshot_id") != spec.snapshot_id:
        failures.append("bounded API provenance snapshot differs from the request")
    if provenance.get("storage_generation") != "shared_blocks_v4":
        failures.append("bounded API did not serve from shared_blocks_v4")
    return failures


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
