# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source, release, and rollback fences for the projection-v3 census."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import text

from api.plan_pricing_projection_build import (
    MAX_PROJECTION_BINDINGS,
    MAX_PROJECTION_CODE_ROWS,
)
from api.plan_pricing_projection_contract import (
    lock_provider_generation,
    normalized_bindings,
    provider_signature,
    table,
)
from api.plan_pricing_projection_source import BindingProjection, binding_projection
from api.plan_pricing_projection_v3_provider import _validated_binding_ordinals
from api.plan_release_serving import (
    PLAN_RELEASE_PIN_OWNER_TYPE,
    PlanReleaseServingSelection,
    resolve_plan_release_serving,
)
from db.connection import db
from scripts.research.plan_pricing_projection_v3_census_transaction import (
    _await_cleanup_task,
    cancellation_safe,
)

HEX_40, HEX_64 = re.compile(r"^[0-9a-f]{40}$"), re.compile(r"^[0-9a-f]{64}$")
SOURCE_PATHS = tuple(f"api/{module_name}.py" for module_name in """
        plan_pricing_aggregate_pack plan_pricing_prewarm plan_pricing_prewarm_selection
        plan_pricing_projection plan_pricing_projection_aggregate_read
        plan_pricing_projection_build plan_pricing_projection_contract
        plan_pricing_projection_read plan_pricing_projection_source
        plan_pricing_projection_v3 plan_pricing_projection_v3_aggregate
        plan_pricing_projection_v3_code plan_pricing_projection_v3_price
        plan_pricing_projection_v3_provider
        plan_pricing_projection_v3_provider_cells plan_pricing_projection_v3_receipts
        plan_pricing_projection_v3_types plan_pricing_projection_v3_work
        plan_release_pricing_projection plan_release_serving
        plan_release_serving_metadata ptg2_audit_occurrences ptg2_db_sidecars
        ptg2_db_serving_v3 ptg2_serving ptg2_snapshot ptg2_v4_graph code_systems
        ptg2_code_filters ptg2_geo_projection ptg2_serving_utils ptg2_tables
        ptg2_types ptg2_candidate_audit_capacity
    """.split())
HARNESS_PATHS = tuple(f"scripts/research/{file_name}" for file_name in """
        plan_pricing_projection_v3_census.py plan_pricing_projection_v3_census_support.py
        plan_pricing_projection_v3_census_authority.py
        plan_pricing_projection_v3_census_arc.py
        plan_pricing_projection_v3_census_contract.py
        plan_pricing_projection_v3_census_diagnostics.py
        plan_pricing_projection_v3_census_identity.py
        plan_pricing_projection_v3_census_transaction.py
        run_plan_pricing_projection_v3_census_envelope.sh
    """.split())
PROJECTION_RELATIONS = (
    "plan_pricing_projection_candidate",
    "plan_pricing_card",
    "plan_pricing_cell_aggregate",
    "plan_pricing_provider_membership",
    "plan_pricing_provider_cell",
    "plan_pricing_rate_profile",
    "plan_pricing_aggregate_pack",
    "plan_pricing_prewarm_shape",
)


@dataclass(frozen=True)
class ReleaseInput:
    """Frozen release identity and its exact binding manifest."""

    identity: dict[str, Any]
    binding_manifest: list[dict[str, Any]]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _observed_git_head(root: Path) -> str | None:
    try:
        return subprocess.run(
            ("git", "rev-parse", "HEAD"),
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def capture_source_identity(
    driver_path: Path,
    expected_source_sha: str,
    expected_manifest_sha256: str,
    expected_harness_manifest_sha256: str,
) -> dict[str, Any]:
    """Hash the exact production and harness source used by the census."""

    if not HEX_40.fullmatch(expected_source_sha):
        raise ValueError("expected source commit is invalid")
    if not HEX_64.fullmatch(expected_manifest_sha256):
        raise ValueError("expected source manifest digest is invalid")
    if not HEX_64.fullmatch(expected_harness_manifest_sha256):
        raise ValueError("expected harness manifest digest is invalid")
    driver_path = driver_path.resolve()
    root = driver_path.parents[2]
    source_files = [
        [relative_path, _sha256_file(root / relative_path)]
        for relative_path in SOURCE_PATHS
    ]
    manifest_sha256 = _canonical_sha256(source_files)
    observed_head = _observed_git_head(root)
    if manifest_sha256 != expected_manifest_sha256 or (
        observed_head is not None and observed_head != expected_source_sha
    ):
        raise RuntimeError("pricing projection census source identity changed")
    if driver_path.relative_to(root).as_posix() != HARNESS_PATHS[0]:
        raise RuntimeError("pricing projection census driver identity changed")
    harness_files = [
        [relative_path, _sha256_file(root / relative_path)]
        for relative_path in HARNESS_PATHS
    ]
    harness_manifest_sha256 = _canonical_sha256(harness_files)
    if harness_manifest_sha256 != expected_harness_manifest_sha256:
        raise RuntimeError("pricing projection census harness identity changed")
    return {
        "declared_git_head": expected_source_sha,
        "observed_git_head": observed_head,
        "manifest_sha256": manifest_sha256,
        "files": source_files,
        "harness_files": harness_files,
        "harness_manifest_sha256": harness_manifest_sha256,
    }


def _json_object(value: Any, field_name: str) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{field_name} is invalid") from exc
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} is invalid")
    return dict(value)


def _binding_identity(binding_by_field: Mapping[str, Any]) -> tuple[Any, ...]:
    raw_ordinal = binding_by_field.get(
        "ordinal", binding_by_field.get("binding_ordinal")
    )
    if isinstance(raw_ordinal, bool):
        raise ValueError("pricing projection binding ordinal is invalid")
    try:
        ordinal = int(raw_ordinal)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("pricing projection binding ordinal is invalid") from exc
    return (
        str(binding_by_field.get("role") or ""),
        ordinal,
        str(binding_by_field.get("snapshot_id") or ""),
        str(binding_by_field.get("source_key") or ""),
        str(binding_by_field.get("plan_id") or ""),
        str(
            binding_by_field.get("market_type")
            or binding_by_field.get("plan_market_type")
            or ""
        )
        .strip()
        .lower(),
        bool(binding_by_field.get("required", True)),
    )


def _selection_identity(
    selection: PlanReleaseServingSelection,
) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            binding.role,
            binding.binding_ordinal,
            binding.snapshot_id,
            binding.source_key,
            binding.plan_id,
            binding.plan_market_type,
            binding.required,
        )
        for binding in selection.bindings
    )


async def _locked_revision_row(
    session: Any,
    plan_release_id: str,
) -> dict[str, Any]:
    revision_result = await session.execute(
        text(f"""
            SELECT serving_revision_id, plan_release_id, healthporta_plan_id,
                   binding_set_digest, expected_binding_count, source_manifest,
                   published_at
              FROM {table('plan_release_serving_revision')}
             WHERE plan_release_id = :plan_release_id
               AND serving_status = 'published'
               AND release_status = 'published'
               AND is_current
             FOR SHARE
            """),
        {"plan_release_id": plan_release_id},
    )
    release_row_by_field = revision_result.mappings().one_or_none()
    if release_row_by_field is None or release_row_by_field.get("published_at") is None:
        raise ValueError("pricing projection census release is not current")
    return dict(release_row_by_field)


async def _locked_binding_rows(
    session: Any,
    serving_revision_id: str,
) -> list[dict[str, Any]]:
    binding_result = await session.execute(
        text(f"""
            SELECT binding.binding_ordinal, binding.snapshot_id,
                   binding.source_key, binding.plan_id,
                   binding.plan_market_type, binding.role, binding.required
              FROM {table('plan_release_snapshot_binding')} binding
              JOIN {table('ptg2_snapshot')} snapshot
                ON snapshot.snapshot_id = binding.snapshot_id
               AND snapshot.status = 'published'
              JOIN {table('ptg2_snapshot_pin')} pin
                ON pin.owner_type = :pin_owner_type
               AND pin.owner_id = binding.serving_revision_id
               AND pin.snapshot_id = binding.snapshot_id
             WHERE binding.serving_revision_id = :serving_revision_id
             ORDER BY CASE binding.role WHEN 'in_network' THEN 0 ELSE 1 END,
                      binding.binding_ordinal
             FOR SHARE OF binding, snapshot, pin
            """),
        {
            "pin_owner_type": PLAN_RELEASE_PIN_OWNER_TYPE,
            "serving_revision_id": serving_revision_id,
        },
    )
    return [dict(binding_row) for binding_row in binding_result.mappings()]


def _release_input_from_rows(
    plan_release_id: str,
    release_row_by_field: Mapping[str, Any],
    locked_binding_rows: list[dict[str, Any]],
) -> ReleaseInput:
    source_manifest = _json_object(
        release_row_by_field["source_manifest"],
        "pricing projection source manifest",
    )
    binding_manifest = normalized_bindings(source_manifest.get("bindings"))
    expected_count = int(release_row_by_field["expected_binding_count"])
    if expected_count != len(binding_manifest) or tuple(
        map(_binding_identity, binding_manifest)
    ) != tuple(map(_binding_identity, locked_binding_rows)):
        raise ValueError("pricing projection census bindings changed")
    binding_digest = str(release_row_by_field["binding_set_digest"])
    return ReleaseInput(
        {
            "healthporta_plan_id": str(release_row_by_field["healthporta_plan_id"]),
            "plan_release_id": plan_release_id,
            "serving_revision_id": str(release_row_by_field["serving_revision_id"]),
            "binding_set_digest": binding_digest,
            "published_at": str(release_row_by_field["published_at"]),
            "binding_count": len(binding_manifest),
        },
        binding_manifest,
    )


async def locked_release_input(
    session: Any,
    plan_release_id: str,
) -> ReleaseInput:
    """Lock and validate one complete serving-ready release."""

    release_row_by_field = await _locked_revision_row(session, plan_release_id)
    locked_binding_rows = await _locked_binding_rows(
        session, str(release_row_by_field["serving_revision_id"])
    )
    release_input = _release_input_from_rows(
        plan_release_id, release_row_by_field, locked_binding_rows
    )
    selection = await resolve_plan_release_serving(session, plan_release_id)
    if (
        selection is None
        or selection.serving_revision_id
        != release_input.identity["serving_revision_id"]
        or selection.binding_set_digest != release_input.identity["binding_set_digest"]
        or _selection_identity(selection)
        != tuple(map(_binding_identity, release_input.binding_manifest))
    ):
        raise ValueError("pricing projection census release is not serving-ready")
    return release_input


async def load_binding_projections(
    session: Any,
    binding_manifest: list[dict[str, Any]],
) -> list[BindingProjection]:
    """Load the same bounded in-network code catalogs as the builder."""

    in_network_bindings = [
        binding_by_field
        for binding_by_field in binding_manifest
        if str(binding_by_field.get("role")) == "in_network"
    ]
    if not 1 <= len(in_network_bindings) <= MAX_PROJECTION_BINDINGS:
        raise ValueError("pricing projection census binding bound exceeded")
    remaining_code_rows = MAX_PROJECTION_CODE_ROWS
    binding_projection_list: list[BindingProjection] = []
    for binding_by_field in in_network_bindings:
        if remaining_code_rows <= 0:
            raise ValueError("pricing projection census code-row bound exceeded")
        loaded_binding = await binding_projection(
            session,
            binding_by_field,
            maximum_code_rows=remaining_code_rows,
        )
        if loaded_binding.raw_code_row_count > remaining_code_rows:
            raise ValueError("pricing projection census code-row bound exceeded")
        remaining_code_rows -= loaded_binding.raw_code_row_count
        binding_projection_list.append(loaded_binding)
    _validated_binding_ordinals(binding_projection_list)
    return binding_projection_list


async def projection_row_counts(
    session: Any,
    candidate_id: str,
) -> dict[str, int]:
    """Count exact candidate rows across every known projection relation."""

    counts_by_relation: dict[str, int] = {}
    for relation_name in PROJECTION_RELATIONS:
        qualified_relation = table(relation_name)
        exists_result = await session.execute(
            text("SELECT to_regclass(:relation_name) IS NOT NULL"),
            {"relation_name": qualified_relation},
        )
        if not bool(exists_result.scalar_one()):
            raise RuntimeError("pricing projection relation is unavailable")
        count_result = await session.execute(
            text(
                f"SELECT COUNT(*) FROM {qualified_relation} "
                "WHERE projection_id = :projection_id"
            ),
            {"projection_id": candidate_id},
        )
        counts_by_relation[relation_name] = int(count_result.scalar_one())
    return counts_by_relation


def memory_sample() -> dict[str, int | None]:
    """Return process and cgroup-v2 memory counters when available."""

    rss_bytes = None
    try:
        status_lines = (
            Path("/proc/self/status").read_text(encoding="utf-8").splitlines()
        )
        rss_line = next(line for line in status_lines if line.startswith("VmRSS:"))
        rss_bytes = int(rss_line.split()[1]) * 1024
    except (OSError, StopIteration, ValueError):
        rss_bytes = None

    def _cgroup_value(name: str) -> int | None:
        try:
            raw_value = (
                (Path("/sys/fs/cgroup") / name).read_text(encoding="utf-8").strip()
            )
            return None if raw_value == "max" else int(raw_value)
        except (OSError, ValueError):
            return None

    return {
        "process_rss_bytes": rss_bytes,
        "cgroup_current_bytes": _cgroup_value("memory.current"),
        "cgroup_peak_bytes": _cgroup_value("memory.peak"),
        "cgroup_limit_bytes": _cgroup_value("memory.max"),
    }


async def _finish_postflight_rollback(session: Any, transaction: Any) -> None:
    if transaction.is_active:
        await transaction.rollback()
    await session.rollback()


async def _postflight(
    plan_release_id: str,
    expected_result: Mapping[str, Any],
) -> dict[str, Any]:
    async with db.session() as session:
        transaction = await session.begin()
        try:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL lock_timeout = '5s'"))
            await session.execute(text("SET LOCAL statement_timeout = '20min'"))
            await lock_provider_generation(session)
            release_input = await locked_release_input(session, plan_release_id)
            signature = await provider_signature(session)
            persistent_counts = await projection_row_counts(
                session, str(expected_result["projection_id"])
            )
        finally:
            active_error = sys.exc_info()[1]
            cleanup_task = asyncio.create_task(
                _finish_postflight_rollback(session, transaction)
            )
            try:
                await _await_cleanup_task(
                    cleanup_task,
                    propagate_cancellation=active_error is None,
                )
            except BaseException:
                if active_error is None:
                    raise
    result_by_field = {
        "release_matches": release_input.identity == expected_result["release"],
        "provider_signature_matches": (
            signature == expected_result["provider_signature"]
        ),
        "persistent_counts_match": (
            persistent_counts == expected_result["persistent_counts_before"]
        ),
        "persistent_counts_after": persistent_counts,
    }
    result_by_field["accepted"] = all(
        result_by_field[field_name] is True
        for field_name in (
            "release_matches",
            "provider_signature_matches",
            "persistent_counts_match",
        )
    )
    return result_by_field


async def postflight(
    plan_release_id: str,
    expected_result: Mapping[str, Any],
) -> dict[str, Any]:
    """Rebind release/provider identity and prove no projection rows changed."""

    return await cancellation_safe(_postflight(plan_release_id, expected_result))


def runtime_identity(expected_image_digest: str) -> dict[str, Any]:
    """Read the required non-secret Kubernetes execution identity."""

    environment_by_field = {
        "job_name": "HLTHPRT_PLAN_PRICING_V3_CENSUS_JOB_NAME",
        "pod_name": "HOSTNAME",
        "pod_uid": "HLTHPRT_PLAN_PRICING_V3_CENSUS_POD_UID",
        "image_digest": "HLTHPRT_PLAN_PRICING_V3_CENSUS_IMAGE_DIGEST",
    }
    identity_by_field = {
        field_name: str(os.getenv(environment_name) or "").strip()
        for field_name, environment_name in environment_by_field.items()
    }
    if not all(identity_by_field.values()):
        raise RuntimeError("pricing projection census runtime identity is incomplete")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", expected_image_digest):
        raise RuntimeError("pricing projection census image digest is invalid")
    if identity_by_field["image_digest"] != expected_image_digest:
        raise RuntimeError("pricing projection census image identity changed")
    return {
        **identity_by_field,
        "container_name": "census",
        "identity_contract": "immutable-image-plus-source-overlay-v1",
        "external_pod_image_id_attestation_required": True,
    }
