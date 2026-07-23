"""Read-only two-owner production-path probe for the PTG V4 dev canary."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from scripts.ptg_v4_dev_canary_internal_runtime import (
    InternalProbeRuntime,
    production_runtime,
)
from scripts.ptg_v4_dev_canary_internal_evaluation import (
    _DELTA_FIELDS,
    compiler_work as _compiler_work,
    compiler_work_failures as _compiler_work_failures,
    metric_delta as _metric_delta,
    metric_failures as _metric_failures,
    owner_mode as _owner_mode,
    prefix_digest as _prefix_digest,
    series_report as _series_report,
)
from scripts.ptg_v4_dev_canary_io import write_json
from scripts.ptg_v4_dev_canary_support import nearest_rank, validate_identifier


INTERNAL_OWNER_EVIDENCE_CONTRACT = "ptg_v4_internal_owner_probe_v1"
_MINIMUM_SAMPLE_COUNT = 5
_EXACT_PREFIX_LIMIT = 201


@dataclass(frozen=True)
class _OwnerSpec:
    """Compiler-selected owner identity and its expected serving mode."""

    role: str
    provider_set_key: int
    expected_member_count: int
    expected_member_digest: str
    uses_override: bool
    uses_component_fallback: bool


@dataclass(frozen=True)
class _SampleSeries:
    """One owner's redacted samples plus the common exact NPI prefix."""

    latencies_ms: tuple[float, ...]
    metric_deltas: tuple[dict[str, int], ...]
    prefix_npis: tuple[int, ...]
    is_prefix_stable: bool


async def run_internal_owner_probe(
    args: Any,
    *,
    runtime: InternalProbeRuntime | None = None,
) -> dict[str, Any]:
    """Probe the exact worst and worst-online owners through production code."""

    _validate_probe_arguments(args)
    active_runtime = runtime or production_runtime()
    runtime_identity_by_field = _validated_runtime_identity_by_field(
        active_runtime.runtime_identity()
    )
    await active_runtime.database.connect()
    async with active_runtime.database.session() as session:
        await session.execute(active_runtime.text("SET TRANSACTION READ ONLY"))
        serving_tables = await active_runtime.load_serving_tables(
            session,
            args.snapshot_id,
        )
        snapshot_key, diagnostic = _serving_contract(
            serving_tables,
            args.snapshot_id,
        )
        owner_specs = _compiler_owner_specs(diagnostic)
        provider_set_id_by_key = await _provider_set_ids(
            session,
            runtime=active_runtime,
            snapshot_key=snapshot_key,
            owner_specs=owner_specs,
        )
        owner_reports = [
            await _probe_owner(
                session,
                args=args,
                runtime=active_runtime,
                serving_tables=serving_tables,
                snapshot_key=snapshot_key,
                diagnostic=diagnostic,
                owner_spec=owner_spec,
                provider_set_id=provider_set_id_by_key[
                    owner_spec.provider_set_key
                ],
            )
            for owner_spec in owner_specs
        ]
    return _persist_internal_report(
        args,
        runtime_identity_by_field=runtime_identity_by_field,
        owner_reports=owner_reports,
    )


def _persist_internal_report(
    args: Any,
    *,
    runtime_identity_by_field: Mapping[str, str],
    owner_reports: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Persist the provenance-bound two-owner probe report."""

    failures = [
        failure
        for owner_report in owner_reports
        for failure in owner_report["failures"]
    ]
    report_by_field = {
        "contract": INTERNAL_OWNER_EVIDENCE_CONTRACT,
        "snapshot_id": args.snapshot_id,
        "reference_snapshot_id": args.reference_snapshot_id,
        **runtime_identity_by_field,
        "passed": not failures,
        "failures": failures,
        "owners": list(owner_reports),
    }
    write_json(Path(args.output), report_by_field)
    return report_by_field


def _validated_runtime_identity_by_field(
    identity_by_field: Mapping[str, Any],
) -> dict[str, str]:
    """Require this exact process and image identity before probing."""

    identity_fields = (
        "process_identity",
        "process_started_at",
        "image_identity",
    )
    validated_identity_by_field: dict[str, str] = {}
    for field_name in identity_fields:
        raw_value = identity_by_field.get(field_name)
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise RuntimeError(
                f"internal owner probe runtime {field_name} is missing"
            )
        validated_identity_by_field[field_name] = raw_value.strip()
    return validated_identity_by_field


def _validate_probe_arguments(args: Any) -> None:
    """Reject weak sampling, a non-production prefix, or nonpositive gates."""

    if not str(args.reference_snapshot_id).strip():
        raise ValueError("internal owner probe reference snapshot is missing")
    if int(args.prefix_limit) != _EXACT_PREFIX_LIMIT:
        raise ValueError("internal owner probe requires limit_per_set=201")
    if (
        int(args.cold_samples) < _MINIMUM_SAMPLE_COUNT
        or int(args.warm_samples) < _MINIMUM_SAMPLE_COUNT
    ):
        raise ValueError("internal owner probe requires at least five samples")
    positive_values = (
        args.cold_p95_limit_ms,
        args.warm_p95_limit_ms,
    )
    if any(float(value) <= 0 for value in positive_values):
        raise ValueError("internal owner probe gates must be positive")
    if (
        float(args.cold_p95_limit_ms) > 50
        or float(args.warm_p95_limit_ms) > 50
    ):
        raise ValueError("internal owner latency gates cannot exceed 50ms")


def _serving_contract(
    serving_tables: Any,
    snapshot_id: str,
) -> tuple[int, dict[str, Any]]:
    """Require the sealed V4 loader result and its authenticated hot contract."""

    snapshot_key = getattr(serving_tables, "shared_snapshot_key", None)
    diagnostic = getattr(
        serving_tables,
        "provider_graph_v4_hot_prefix",
        None,
    )
    if (
        getattr(serving_tables, "snapshot_id", None) != snapshot_id
        or getattr(serving_tables, "uses_v4_graph", False) is not True
        or isinstance(snapshot_key, bool)
        or not isinstance(snapshot_key, int)
        or snapshot_key <= 0
        or not isinstance(diagnostic, Mapping)
        or int(diagnostic.get("npi_prefix_target") or 0) != _EXACT_PREFIX_LIMIT
    ):
        raise RuntimeError("production loader did not return the sealed V4 contract")
    return snapshot_key, dict(diagnostic)


def _compiler_owner_specs(
    diagnostic: Mapping[str, Any],
) -> tuple[_OwnerSpec, _OwnerSpec]:
    """Build deterministic overall-worst and non-override online owner specs."""

    overall = _build_owner_spec(diagnostic, role="overall_worst", prefix="worst")
    online = _build_owner_spec(
        diagnostic,
        role="worst_online_non_override",
        prefix="worst_online",
    )
    if online.uses_override:
        raise RuntimeError("compiler-selected online owner uses a prefix override")
    return overall, online


def _build_owner_spec(
    diagnostic: Mapping[str, Any],
    *,
    role: str,
    prefix: str,
) -> _OwnerSpec:
    """Read one complete owner identity from compiler-authenticated fields."""

    key = diagnostic.get(f"{prefix}_provider_set_key")
    member_count = diagnostic.get(f"{prefix}_member_count")
    digest = diagnostic.get(f"{prefix}_member_digest")
    if (
        isinstance(key, bool)
        or not isinstance(key, int)
        or key < 0
        or isinstance(member_count, bool)
        or not isinstance(member_count, int)
        or member_count != _EXACT_PREFIX_LIMIT
        or not isinstance(digest, str)
        or len(digest) != 64
    ):
        raise RuntimeError(
            f"compiler diagnostic lacks exact 201-member {role} evidence"
        )
    uses_override = (
        bool(diagnostic.get("worst_uses_override"))
        if prefix == "worst"
        else False
    )
    return _OwnerSpec(
        role=role,
        provider_set_key=key,
        expected_member_count=member_count,
        expected_member_digest=digest,
        uses_override=uses_override,
        uses_component_fallback=bool(
            diagnostic.get(f"{prefix}_uses_component_fallback")
        ),
    )


async def _provider_set_ids(
    session: Any,
    *,
    runtime: InternalProbeRuntime,
    snapshot_key: int,
    owner_specs: Sequence[_OwnerSpec],
) -> dict[int, str]:
    """Resolve opaque serving ids for dense owner keys without reporting them."""

    owner_keys = sorted({owner_spec.provider_set_key for owner_spec in owner_specs})
    schema_name = validate_identifier(
        runtime.schema_name,
        label="production database schema",
    )
    query_result = await session.execute(
        runtime.text(
            f"""
            SELECT provider_set_key,
                   encode(provider_set_global_id_128, 'hex') AS provider_set_id
              FROM {schema_name}.ptg2_v3_provider_set
             WHERE snapshot_key = :snapshot_key
               AND provider_set_key = ANY(CAST(:owner_keys AS integer[]))
            """
        ),
        {"snapshot_key": snapshot_key, "owner_keys": owner_keys},
    )
    provider_set_id_by_key = {
        int(record_by_field["provider_set_key"]): str(
            record_by_field["provider_set_id"]
        )
        for record_by_field in (
            _row_mapping(raw_record) for raw_record in query_result
        )
    }
    if set(provider_set_id_by_key) != set(owner_keys):
        raise RuntimeError("compiler-selected provider-set dictionary row is missing")
    return provider_set_id_by_key


async def _probe_owner(
    session: Any,
    *,
    args: Any,
    runtime: InternalProbeRuntime,
    serving_tables: Any,
    snapshot_key: int,
    diagnostic: Mapping[str, Any],
    owner_spec: _OwnerSpec,
    provider_set_id: str,
) -> dict[str, Any]:
    """Collect cold and warm production-path evidence for one exact owner."""

    cold = await _sample_series(
        session,
        runtime=runtime,
        serving_tables=serving_tables,
        provider_set_id=provider_set_id,
        prefix_limit=args.prefix_limit,
        sample_count=args.cold_samples,
        reset_caches=runtime.reset_cold_caches,
    )
    warm = await _sample_series(
        session,
        runtime=runtime,
        serving_tables=serving_tables,
        provider_set_id=provider_set_id,
        prefix_limit=args.prefix_limit,
        sample_count=args.warm_samples,
        reset_caches=runtime.reset_warm_caches,
    )
    dense_keys = await _dense_prefix_keys(
        session,
        runtime=runtime,
        snapshot_key=snapshot_key,
        npis=cold.prefix_npis,
    )
    return _evaluate_owner(
        args,
        diagnostic=diagnostic,
        owner_spec=owner_spec,
        cold=cold,
        warm=warm,
        dense_keys=dense_keys,
    )


async def _sample_series(
    session: Any,
    *,
    runtime: InternalProbeRuntime,
    serving_tables: Any,
    provider_set_id: str,
    prefix_limit: int,
    sample_count: int,
    reset_caches: Callable[[], None],
) -> _SampleSeries:
    """Measure repeated uncached-prefix calls while retaining no public ids."""

    latencies_ms: list[float] = []
    metric_deltas: list[dict[str, int]] = []
    observed_prefix: tuple[int, ...] | None = None
    is_prefix_stable = True
    for _sample_index in range(int(sample_count)):
        reset_caches()
        before = runtime.metrics_snapshot()
        started_at = runtime.monotonic()
        npis_by_set = await runtime.provider_npis_for_sets(
            session,
            serving_tables,
            [provider_set_id],
            limit_per_set=int(prefix_limit),
        )
        elapsed_ms = (runtime.monotonic() - started_at) * 1_000
        after = runtime.metrics_snapshot()
        if set(npis_by_set) != {provider_set_id}:
            raise RuntimeError("production prefix path returned the wrong owner")
        prefix_npis = tuple(npis_by_set[provider_set_id])
        if observed_prefix is None:
            observed_prefix = prefix_npis
        elif prefix_npis != observed_prefix:
            is_prefix_stable = False
        latencies_ms.append(round(elapsed_ms, 3))
        metric_deltas.append(_metric_delta(before, after))
    return _SampleSeries(
        tuple(latencies_ms),
        tuple(metric_deltas),
        observed_prefix or (),
        is_prefix_stable,
    )


async def _dense_prefix_keys(
    session: Any,
    *,
    runtime: InternalProbeRuntime,
    snapshot_key: int,
    npis: Sequence[int],
) -> tuple[int, ...]:
    """Translate the returned exact order to snapshot-local keys for hashing."""

    npi_key_by_value = await runtime.npi_keys_for_values(
        session,
        snapshot_key=snapshot_key,
        npis=npis,
        schema_name=runtime.schema_name,
    )
    if set(npi_key_by_value) != set(npis):
        raise RuntimeError("production NPI dictionary is incomplete for canary prefix")
    return tuple(int(npi_key_by_value[int(npi)]) for npi in npis)


def _evaluate_owner(
    args: Any,
    *,
    diagnostic: Mapping[str, Any],
    owner_spec: _OwnerSpec,
    cold: _SampleSeries,
    warm: _SampleSeries,
    dense_keys: Sequence[int],
) -> dict[str, Any]:
    """Apply exact prefix, latency, metric, and compiler-work gates."""

    actual_digest = _prefix_digest(dense_keys)
    failures: list[str] = []
    if (
        not cold.is_prefix_stable
        or not warm.is_prefix_stable
        or warm.prefix_npis != cold.prefix_npis
    ):
        failures.append(f"{owner_spec.role} exact prefix changed across samples")
    if (
        len(dense_keys) != owner_spec.expected_member_count
        or actual_digest != owner_spec.expected_member_digest
    ):
        failures.append(f"{owner_spec.role} prefix differs from compiler digest")
    if len(dense_keys) != _EXACT_PREFIX_LIMIT:
        failures.append(
            f"{owner_spec.role} prefix is not the exact 201-member workload"
        )
    cold_p95 = nearest_rank(cold.latencies_ms, 0.95)
    warm_p95 = nearest_rank(warm.latencies_ms, 0.95)
    if cold_p95 > float(args.cold_p95_limit_ms):
        failures.append(f"{owner_spec.role} cold p95 exceeds 50ms gate")
    if warm_p95 > float(args.warm_p95_limit_ms):
        failures.append(f"{owner_spec.role} warm p95 exceeds 50ms gate")
    for phase_name, series in (("cold", cold), ("warm", warm)):
        failures.extend(
            _metric_failures(
                diagnostic=diagnostic,
                owner_spec=owner_spec,
                phase_name=phase_name,
                metric_deltas=series.metric_deltas,
            )
        )
    failures.extend(_compiler_work_failures(diagnostic, owner_spec))
    return {
        "role": owner_spec.role,
        "provider_set_key": owner_spec.provider_set_key,
        "mode": _owner_mode(owner_spec),
        "expected_member_count": owner_spec.expected_member_count,
        "actual_member_count": len(dense_keys),
        "expected_member_digest": owner_spec.expected_member_digest,
        "actual_member_digest": actual_digest,
        "cold": _series_report(cold, cold_p95),
        "warm": _series_report(warm, warm_p95),
        "compiler_work": _compiler_work(diagnostic, owner_spec),
        "passed": not failures,
        "failures": failures,
    }


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})
