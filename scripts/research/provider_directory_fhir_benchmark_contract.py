# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic tuning report contract for bounded Provider Directory runs.

The contract intentionally contains no HTTP client. Operators collect bounded
observations from the US dev-server execution path and feed only aggregate,
non-payload metrics into this evaluator.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable


CONTRACT_VERSION = "provider-directory-fhir-benchmark-v1"
ALLOWED_CONCURRENCY = (1, 2, 4)
MAX_TRANSIENT_RETRY_RATE = 0.01
MIN_DEADLINE_MARGIN = 0.30
MIN_CONCURRENCY_FOUR_THROUGHPUT_GAIN = 0.10


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
    unresolved_throttling: bool = False
    peak_memory_bytes: int = 0
    peak_db_backlog_rows: int = 0

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
    resources: tuple[ResourceObservation, ...]
    proxy_route: str = "dev-server"

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
    def throughput_rows_per_second(self) -> float:
        """Return aggregate processed rows per elapsed second."""

        if self.elapsed_seconds <= 0:
            return 0.0
        return (
            sum(observation.processed_rows for observation in self.resources)
            / self.elapsed_seconds
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
            and self.retry_rate < MAX_TRANSIENT_RETRY_RATE
            and self.deadline_margin >= MIN_DEADLINE_MARGIN
        )


def _resource_observation(raw: dict[str, Any]) -> ResourceObservation:
    return ResourceObservation(
        resource_type=str(raw["resource_type"]),
        pre_count=int(raw["pre_count"]),
        processed_rows=int(raw["processed_rows"]),
        unique_staged_ids=int(raw["unique_staged_ids"]),
        post_count=int(raw["post_count"]),
        pages=int(raw["pages"]),
        elapsed_seconds=float(raw["elapsed_seconds"]),
        requests=int(raw["requests"]),
        transient_retries=int(raw["transient_retries"]),
        unresolved_throttling=bool(raw.get("unresolved_throttling", False)),
        peak_memory_bytes=int(raw.get("peak_memory_bytes", 0)),
        peak_db_backlog_rows=int(raw.get("peak_db_backlog_rows", 0)),
    )


def parse_observation(raw: dict[str, Any]) -> ConcurrencyObservation:
    """Parse and validate one aggregate concurrency observation."""

    concurrency = int(raw["concurrency"])
    if concurrency not in ALLOWED_CONCURRENCY:
        raise ValueError("benchmark concurrency must be one of 1, 2, or 4")
    resources = tuple(
        _resource_observation(item) for item in raw.get("resources", [])
    )
    resource_names = [item.resource_type for item in resources]
    if len(resource_names) != len(set(resource_names)):
        raise ValueError("benchmark resource observations must be unique")
    return ConcurrencyObservation(
        concurrency=concurrency,
        cutoff=str(raw["cutoff"]),
        elapsed_seconds=float(raw["elapsed_seconds"]),
        deadline_seconds=float(raw["deadline_seconds"]),
        resources=resources,
        proxy_route=str(raw.get("proxy_route") or ""),
    )


def select_resource_concurrency(
    observations: Iterable[ConcurrencyObservation],
) -> int:
    """Select the fastest concurrency that passes every safety gate."""

    by_concurrency = {
        observation.concurrency: observation
        for observation in observations
    }
    safe_two = by_concurrency.get(2)
    if safe_two is None or not safe_two.has_passed_safety_gates:
        safe_one = by_concurrency.get(1)
        return 1 if safe_one and safe_one.has_passed_safety_gates else 0
    candidate_four = by_concurrency.get(4)
    if (
        candidate_four is None
        or not candidate_four.has_passed_safety_gates
    ):
        return 2
    minimum_four_throughput = safe_two.throughput_rows_per_second * (
        1 + MIN_CONCURRENCY_FOUR_THROUGHPUT_GAIN
    )
    return (
        4
        if candidate_four.throughput_rows_per_second >= minimum_four_throughput
        else 2
    )


def build_tuning_report(
    observations: Iterable[ConcurrencyObservation],
) -> dict[str, Any]:
    """Build the deterministic operator-facing tuning report."""

    ordered = sorted(
        observations,
        key=lambda observation: observation.concurrency,
    )
    cutoffs = {observation.cutoff for observation in ordered}
    if len(cutoffs) != 1:
        raise ValueError("all benchmark observations must use the same cutoff")
    return {
        "contract_version": CONTRACT_VERSION,
        "cutoff": next(iter(cutoffs), None),
        "recommended_resource_concurrency": select_resource_concurrency(ordered),
        "observations": [
            {
                **asdict(observation),
                "exact": observation.is_exact,
                "retry_rate": observation.retry_rate,
                "deadline_margin": observation.deadline_margin,
                "throughput_rows_per_second": (
                    observation.throughput_rows_per_second
                ),
                "passes_safety_gates": (
                    observation.has_passed_safety_gates
                ),
            }
            for observation in ordered
        ],
    }


def main() -> int:
    """Evaluate aggregate observations from a local JSON file."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Aggregate benchmark JSON.")
    parser.add_argument("--output", type=Path, help="Optional report output path.")
    args = parser.parse_args()
    raw = json.loads(args.input.read_text(encoding="utf-8"))
    observations = [parse_observation(item) for item in raw["observations"]]
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
