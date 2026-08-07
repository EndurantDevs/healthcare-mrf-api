# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic tuning report contract for bounded Provider Directory runs.

The contract intentionally contains no HTTP client. Operators collect bounded
observations from the US dev-server execution path and feed only aggregate,
non-payload metrics into this evaluator.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable


CONTRACT_VERSION = "provider-directory-fhir-benchmark-v1"
ALLOWED_CONCURRENCY = (1, 2, 4)
MAX_TRANSIENT_RETRY_RATE = 0.01
MIN_DEADLINE_MARGIN = 0.30
MIN_CONCURRENCY_FOUR_THROUGHPUT_GAIN = 0.10
TOP_LEVEL_FIELDS = frozenset({"observations"})
OBSERVATION_FIELDS = frozenset(
    "concurrency cutoff elapsed_seconds deadline_seconds memory_budget_bytes "
    "db_backlog_budget_rows proxy_route resources".split()
)
RESOURCE_FIELDS = frozenset(
    "resource_type pre_count processed_rows unique_staged_ids post_count pages "
    "elapsed_seconds requests transient_retries unresolved_throttling "
    "peak_memory_bytes peak_db_backlog_rows".split()
)

@dataclass(frozen=True)
class ResourceObservation:
    resource_type: str
    pre_count: int
    processed_rows: int
    unique_staged_ids: int
    post_count: int
    pages: int
    elapsed_seconds: float
    requests: int
    transient_retries: int
    unresolved_throttling: bool
    peak_memory_bytes: int
    peak_db_backlog_rows: int

    @property
    def is_exact(self) -> bool:
        """Return whether every exact-census cardinality is identical."""
        return (
            self.pre_count
            == self.processed_rows
            == self.unique_staged_ids
            == self.post_count
        )

    @property
    def throughput_rows_per_second(self) -> float:
        """Return processed rows per elapsed second."""
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.processed_rows / self.elapsed_seconds

    @property
    def retry_rate(self) -> float:
        """Return transient retries divided by requests."""
        if self.requests <= 0:
            return 0.0 if self.transient_retries == 0 else 1.0
        return self.transient_retries / self.requests

@dataclass(frozen=True)
class ConcurrencyObservation:
    concurrency: int
    cutoff: str
    elapsed_seconds: float
    deadline_seconds: float
    memory_budget_bytes: int
    db_backlog_budget_rows: int
    resources: tuple[ResourceObservation, ...]
    proxy_route: str

    @property
    def is_exact(self) -> bool:
        """Return whether all resource observations are exact."""
        return bool(self.resources) and all(
            observation.is_exact for observation in self.resources
        )

    @property
    def has_unresolved_throttling(self) -> bool:
        """Return whether any resource retained unresolved throttling."""
        return any(
            observation.unresolved_throttling
            for observation in self.resources
        )

    @property
    def retry_rate(self) -> float:
        """Return aggregate transient retries divided by requests."""

        requests = sum(observation.requests for observation in self.resources)
        retries = sum(
            observation.transient_retries for observation in self.resources
        )
        return retries / requests if requests else (0.0 if retries == 0 else 1.0)

    @property
    def has_acceptable_retry_rates(self) -> bool:
        """Return whether aggregate and per-resource retry rates are bounded."""

        return self.retry_rate < MAX_TRANSIENT_RETRY_RATE and all(
            observation.retry_rate < MAX_TRANSIENT_RETRY_RATE
            for observation in self.resources
        )

    @property
    def throughput_rows_per_second(self) -> float:
        """Return aggregate processed rows per elapsed second."""

        if self.elapsed_seconds <= 0:
            return 0.0
        return (
            sum(observation.processed_rows for observation in self.resources)
            / self.elapsed_seconds
        )

    @property
    def is_within_resource_budgets(self) -> bool:
        """Return whether observed memory and database backlog stay bounded."""

        peak_memory_bytes = max(
            observation.peak_memory_bytes for observation in self.resources
        )
        peak_db_backlog_rows = max(
            observation.peak_db_backlog_rows
            for observation in self.resources
        )
        return (
            peak_memory_bytes <= self.memory_budget_bytes
            and peak_db_backlog_rows <= self.db_backlog_budget_rows
        )

    @property
    def deadline_margin(self) -> float:
        """Return the unused share of the configured deadline."""

        if self.deadline_seconds <= 0:
            return 0.0
        return 1.0 - (self.elapsed_seconds / self.deadline_seconds)

    @property
    def has_passed_safety_gates(self) -> bool:
        """Return whether completeness and bounded-performance gates pass."""

        return (
            self.proxy_route == "dev-server"
            and self.is_exact
            and not self.has_unresolved_throttling
            and self.is_within_resource_budgets
            and self.has_acceptable_retry_rates
            and self.deadline_margin >= MIN_DEADLINE_MARGIN
        )

def _exact_object(
    raw: Any,
    scope: str,
    expected_fields: frozenset[str],
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"benchmark {scope} must be an object")
    observed_fields = frozenset(raw)
    missing_fields = expected_fields - observed_fields
    unknown_fields = observed_fields - expected_fields
    if missing_fields or unknown_fields:
        problems = []
        if missing_fields:
            problems.append("missing=" + ",".join(sorted(missing_fields)))
        if unknown_fields:
            problems.append(
                "unknown="
                + ",".join(sorted(str(field) for field in unknown_fields))
            )
        raise ValueError(
            f"benchmark {scope} fields are invalid ({'; '.join(problems)})"
        )
    return raw


def _bounded_integer(
    raw: dict[str, Any],
    field_name: str,
    *,
    minimum: int = 0,
) -> int:
    raw_value = raw[field_name]
    if type(raw_value) is not int:
        raise ValueError(f"benchmark {field_name} must be an integer")
    if raw_value < minimum:
        raise ValueError(
            f"benchmark {field_name} must be at least {minimum}"
        )
    return raw_value


def _positive_finite_float(raw: dict[str, Any], field_name: str) -> float:
    raw_value = raw[field_name]
    if type(raw_value) not in (int, float):
        raise ValueError(
            f"benchmark {field_name} must be a finite positive number"
        )
    value = float(raw_value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(
            f"benchmark {field_name} must be finite and greater than zero"
        )
    return value


def _resource_observation(raw: Any) -> ResourceObservation:
    resource_by_field = _exact_object(
        raw, "resource observation", RESOURCE_FIELDS
    )
    raw_resource_type = resource_by_field["resource_type"]
    if not isinstance(raw_resource_type, str):
        raise ValueError("benchmark resource_type must be a string")
    resource_type = raw_resource_type.strip()
    if not resource_type:
        raise ValueError("benchmark resource_type must not be empty")
    requests = _bounded_integer(resource_by_field, "requests", minimum=1)
    transient_retries = _bounded_integer(
        resource_by_field, "transient_retries"
    )
    if transient_retries > requests:
        raise ValueError(
            "benchmark transient_retries must not exceed requests"
        )
    raw_unresolved_throttling = resource_by_field["unresolved_throttling"]
    if not isinstance(raw_unresolved_throttling, bool):
        raise ValueError(
            "benchmark unresolved_throttling must be a boolean"
        )
    return ResourceObservation(
        resource_type=resource_type,
        pre_count=_bounded_integer(resource_by_field, "pre_count"),
        processed_rows=_bounded_integer(resource_by_field, "processed_rows"),
        unique_staged_ids=_bounded_integer(
            resource_by_field, "unique_staged_ids"
        ),
        post_count=_bounded_integer(resource_by_field, "post_count"),
        pages=_bounded_integer(resource_by_field, "pages", minimum=1),
        elapsed_seconds=_positive_finite_float(
            resource_by_field, "elapsed_seconds"
        ),
        requests=requests,
        transient_retries=transient_retries,
        unresolved_throttling=raw_unresolved_throttling,
        peak_memory_bytes=_bounded_integer(
            resource_by_field, "peak_memory_bytes"
        ),
        peak_db_backlog_rows=_bounded_integer(
            resource_by_field, "peak_db_backlog_rows"
        ),
    )


def parse_observation(raw: Any) -> ConcurrencyObservation:
    """Parse and validate one aggregate concurrency observation."""

    observation_by_field = _exact_object(
        raw, "concurrency observation", OBSERVATION_FIELDS
    )
    concurrency = _bounded_integer(
        observation_by_field, "concurrency", minimum=1
    )
    if concurrency not in ALLOWED_CONCURRENCY:
        raise ValueError("benchmark concurrency must be one of 1, 2, or 4")
    raw_resources = observation_by_field["resources"]
    if not isinstance(raw_resources, list):
        raise ValueError("benchmark resources must be an array")
    resource_observations = tuple(
        sorted(
            (
                _resource_observation(resource_by_field)
                for resource_by_field in raw_resources
            ),
            key=lambda observation: observation.resource_type,
        )
    )
    if not resource_observations:
        raise ValueError("benchmark requires at least one resource observation")
    resource_names = [
        resource_observation.resource_type
        for resource_observation in resource_observations
    ]
    if len(resource_names) != len(set(resource_names)):
        raise ValueError("benchmark resource observations must be unique")
    raw_cutoff = observation_by_field["cutoff"]
    if not isinstance(raw_cutoff, str):
        raise ValueError("benchmark cutoff must be a string")
    cutoff = raw_cutoff.strip()
    if not cutoff:
        raise ValueError("benchmark cutoff must not be empty")
    raw_proxy_route = observation_by_field["proxy_route"]
    if not isinstance(raw_proxy_route, str) or not raw_proxy_route:
        raise ValueError("benchmark proxy_route must be a non-empty string")
    return ConcurrencyObservation(
        concurrency=concurrency,
        cutoff=cutoff,
        elapsed_seconds=_positive_finite_float(
            observation_by_field, "elapsed_seconds"
        ),
        deadline_seconds=_positive_finite_float(
            observation_by_field, "deadline_seconds"
        ),
        memory_budget_bytes=_bounded_integer(
            observation_by_field, "memory_budget_bytes", minimum=1
        ),
        db_backlog_budget_rows=_bounded_integer(
            observation_by_field, "db_backlog_budget_rows", minimum=1
        ),
        resources=resource_observations,
        proxy_route=raw_proxy_route,
    )


def parse_benchmark_document(raw: Any) -> tuple[ConcurrencyObservation, ...]:
    """Parse the closed top-level benchmark JSON object."""

    document_by_field = _exact_object(raw, "document", TOP_LEVEL_FIELDS)
    raw_observations = document_by_field["observations"]
    if not isinstance(raw_observations, list):
        raise ValueError("benchmark observations must be an array")
    return tuple(parse_observation(raw_item) for raw_item in raw_observations)


def _observations_by_concurrency(
    observations: Iterable[ConcurrencyObservation],
) -> dict[int, ConcurrencyObservation]:
    benchmark_observations = list(observations)
    observations_by_concurrency = {
        observation.concurrency: observation
        for observation in benchmark_observations
    }
    if (
        len(observations_by_concurrency) != len(benchmark_observations)
        or set(observations_by_concurrency) != set(ALLOWED_CONCURRENCY)
    ):
        raise ValueError(
            "benchmark requires exactly one observation for concurrency 1, 2, and 4"
        )
    return observations_by_concurrency


def select_resource_concurrency(
    observations: Iterable[ConcurrencyObservation],
) -> int:
    """Select the fastest concurrency that passes every safety gate."""

    observations_by_concurrency = _observations_by_concurrency(observations)
    safe_one = observations_by_concurrency[1]
    if not safe_one.has_passed_safety_gates:
        return 0
    safe_two = observations_by_concurrency[2]
    if (
        not safe_two.has_passed_safety_gates
        or safe_two.throughput_rows_per_second
        <= safe_one.throughput_rows_per_second
    ):
        return 1
    candidate_four = observations_by_concurrency[4]
    if not candidate_four.has_passed_safety_gates:
        return 2
    best_lower_throughput = max(
        safe_one.throughput_rows_per_second,
        safe_two.throughput_rows_per_second,
    )
    minimum_four_throughput = best_lower_throughput * (
        1.0 + MIN_CONCURRENCY_FOUR_THROUGHPUT_GAIN
    )
    return (
        4
        if candidate_four.throughput_rows_per_second >= minimum_four_throughput
        else 2
    )


def _census_vector(
    observation: ConcurrencyObservation,
) -> tuple[tuple[str, int, int, int, int], ...]:
    return tuple(sorted(
        (
            resource.resource_type, resource.pre_count,
            resource.processed_rows, resource.unique_staged_ids,
            resource.post_count,
        )
        for resource in observation.resources
    ))


def _budget_vector(observation: ConcurrencyObservation) -> tuple[float, int, int]:
    return (
        observation.deadline_seconds,
        observation.memory_budget_bytes,
        observation.db_backlog_budget_rows,
    )


def _report_observation(
    observation: ConcurrencyObservation,
) -> dict[str, Any]:
    ordered_resources = sorted(
        observation.resources,
        key=lambda resource: resource.resource_type,
    )
    return {
        **asdict(observation),
        "resources": [asdict(resource) for resource in ordered_resources],
        "exact": observation.is_exact,
        "retry_rate": observation.retry_rate,
        "retry_rates_within_limit": observation.has_acceptable_retry_rates,
        "deadline_margin": observation.deadline_margin,
        "throughput_rows_per_second": observation.throughput_rows_per_second,
        "is_within_resource_budgets": observation.is_within_resource_budgets,
        "passes_safety_gates": observation.has_passed_safety_gates,
    }


def build_tuning_report(
    concurrency_observations: Iterable[ConcurrencyObservation],
) -> dict[str, Any]:
    """Build the deterministic operator-facing tuning report."""

    observations_by_concurrency = _observations_by_concurrency(
        concurrency_observations
    )
    ordered_observations = [
        observations_by_concurrency[concurrency]
        for concurrency in ALLOWED_CONCURRENCY
    ]
    observed_cutoffs = {
        observation.cutoff for observation in ordered_observations
    }
    if len(observed_cutoffs) != 1:
        raise ValueError("all benchmark observations must use the same cutoff")
    census_vectors = {
        _census_vector(observation) for observation in ordered_observations
    }
    if len(census_vectors) != 1:
        raise ValueError(
            "all benchmark observations must use the same per-resource census"
        )
    budget_vectors = {
        _budget_vector(observation) for observation in ordered_observations
    }
    if len(budget_vectors) != 1:
        raise ValueError(
            "all benchmark observations must use the same deadline and resource budgets"
        )
    return {
        "contract_version": CONTRACT_VERSION,
        "cutoff": next(iter(observed_cutoffs), None),
        "recommended_resource_concurrency": select_resource_concurrency(
            ordered_observations
        ),
        "observations": [
            _report_observation(observation)
            for observation in ordered_observations
        ],
    }


def main(argv: list[str] | None = None) -> int:
    """Evaluate aggregate observations from a local JSON file."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Aggregate benchmark JSON.")
    parser.add_argument("--output", type=Path, help="Optional report output path.")
    args = parser.parse_args(argv)
    raw = json.loads(args.input.read_text(encoding="utf-8"))
    observations = parse_benchmark_document(raw)
    report_text = json.dumps(
        build_tuning_report(observations),
        indent=2,
        sort_keys=True,
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report_text + "\n", encoding="utf-8")
    else:
        print(report_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
