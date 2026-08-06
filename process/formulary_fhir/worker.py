# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""ARQ entrypoint for the gated FHIR formulary synchronizer."""

from __future__ import annotations

import asyncio
import datetime as dt
import os
import re
import uuid
from dataclasses import dataclass
from typing import Any

from db.models import db
from process.formulary_fhir.client import FHIRFormularyClient
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository import SOURCE_ID
from process.formulary_fhir.synchronizer import synchronize

FORMULARY_FHIR_QUEUE_NAME = "arq:FormularyFHIR"
FORMULARY_FHIR_LOCK_NAME = "healthporta:formulary-fhir:global:v1"
WEEKDAY_DEADLINE_SECONDS = 16 * 60 * 60
INITIAL_SEED_DEADLINE_SECONDS = 72 * 60 * 60
DATASET_ID_PATTERN = re.compile(r"^ffd_[0-9a-f]{48}$")


def _is_enabled(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _cutoff(value: Any) -> dt.datetime:
    if value in (None, ""):
        return dt.datetime.now(dt.UTC).replace(microsecond=0)
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        try:
            parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                "formulary-fhir cutoff must be an ISO-8601 timestamp"
            ) from exc
    if parsed.tzinfo is None:
        raise ValueError("formulary-fhir cutoff must include a timezone")
    return parsed.astimezone(dt.UTC)


def _concurrency(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("formulary-fhir alias concurrency must be numeric")
    candidate = 4 if value in (None, "") else value
    try:
        parsed = int(candidate)
    except (TypeError, ValueError) as exc:
        raise ValueError("formulary-fhir alias concurrency must be numeric") from exc
    if isinstance(candidate, float) and not candidate.is_integer():
        raise ValueError("formulary-fhir alias concurrency must be numeric")
    if parsed not in {1, 2, 4, 8}:
        raise ValueError("formulary-fhir alias concurrency must be 1, 2, 4, or 8")
    cap = min(max(int(os.getenv("HLTHPRT_FORMULARY_FHIR_ALIAS_CAP") or 8), 1), 8)
    if parsed > cap:
        raise ValueError("formulary-fhir alias concurrency exceeds the runtime cap")
    return parsed


def _is_boolean_argument(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"formulary-fhir {name} must be a boolean")
    return value


def _deadline_seconds(value: Any, *, manual_seed: bool) -> int:
    ceiling = INITIAL_SEED_DEADLINE_SECONDS if manual_seed else WEEKDAY_DEADLINE_SECONDS
    candidate = ceiling if value in (None, "") else value
    if isinstance(candidate, bool):
        raise ValueError("formulary-fhir deadline_seconds must be numeric")
    try:
        parsed = int(candidate)
    except (TypeError, ValueError) as exc:
        raise ValueError("formulary-fhir deadline_seconds must be numeric") from exc
    if isinstance(candidate, float) and not candidate.is_integer():
        raise ValueError("formulary-fhir deadline_seconds must be numeric")
    if not 1 <= parsed <= ceiling:
        raise ValueError("formulary-fhir deadline_seconds exceeds the run-mode ceiling")
    return parsed


@dataclass(frozen=True)
class _FormularyRunSettings:
    run_id: str
    is_manual_seed: bool
    is_publication_proof: bool
    seed_dataset_id: str | None
    should_publish: bool
    cutoff: dt.datetime
    concurrency: int
    deadline_seconds: int


def _validate_run_modes(
    *,
    manual_seed: bool,
    publication_proof: bool,
    publish: bool,
) -> None:
    """Enforce the distinct seed, proof, and automated run modes."""

    if manual_seed and publication_proof:
        raise RuntimeError("initial formulary-fhir seed cannot be a publication proof")
    if publication_proof and not publish:
        raise RuntimeError("formulary-fhir publication proof must request publication")
    if (
        not manual_seed
        and not publication_proof
        and not _is_enabled("HLTHPRT_FORMULARY_FHIR_AUTOMATION_ENABLED")
    ):
        raise RuntimeError("formulary-fhir automation is disabled pending seed proof")
    if (
        publish
        and not publication_proof
        and not _is_enabled("HLTHPRT_FORMULARY_FHIR_PUBLISH_ENABLED")
    ):
        raise RuntimeError(
            "formulary-fhir publication is disabled pending publication proof"
        )
    if manual_seed and publish:
        raise RuntimeError("initial formulary-fhir seed must be non-publishing")


def _seed_dataset_id(value: Any, *, publication_proof: bool) -> str | None:
    if value in (None, ""):
        if publication_proof:
            raise ValueError(
                "formulary-fhir publication proof requires seed_dataset_id"
            )
        return None
    if not isinstance(value, str) or not DATASET_ID_PATTERN.fullmatch(value):
        raise ValueError("formulary-fhir seed_dataset_id is invalid")
    if not publication_proof:
        raise ValueError(
            "formulary-fhir seed_dataset_id is reserved for publication proof"
        )
    return value


def _validated_run_settings(
    ctx: dict[str, Any],
    task_by_field: dict[str, Any],
) -> _FormularyRunSettings:
    """Validate one control-plane task before any database access."""

    run_id = (
        str(task_by_field.get("run_id") or ctx.get("control_run_id") or "").strip()
        or f"formulary_fhir_{uuid.uuid4().hex}"
    )
    source_id = str(task_by_field.get("source_id") or SOURCE_ID).strip()
    if source_id != SOURCE_ID:
        raise ValueError(f"formulary-fhir source_id must be {SOURCE_ID}")
    is_manual_seed = _is_boolean_argument(
        task_by_field.get("manual_seed", False),
        "manual_seed",
    )
    should_publish = _is_boolean_argument(
        task_by_field.get("publish", False),
        "publish",
    )
    is_publication_proof = _is_boolean_argument(
        task_by_field.get("publication_proof", False),
        "publication_proof",
    )
    _validate_run_modes(
        manual_seed=is_manual_seed,
        publication_proof=is_publication_proof,
        publish=should_publish,
    )
    return _FormularyRunSettings(
        run_id=run_id,
        is_manual_seed=is_manual_seed,
        is_publication_proof=is_publication_proof,
        seed_dataset_id=_seed_dataset_id(
            task_by_field.get("seed_dataset_id"),
            publication_proof=is_publication_proof,
        ),
        should_publish=should_publish,
        cutoff=_cutoff(task_by_field.get("cutoff")),
        concurrency=_concurrency(task_by_field.get("alias_concurrency")),
        deadline_seconds=_deadline_seconds(
            task_by_field.get("deadline_seconds"),
            manual_seed=is_manual_seed,
        ),
    )


async def _execute_run(
    settings: _FormularyRunSettings,
    repository: FHIRFormularyRepository,
) -> dict[str, Any]:
    if settings.is_publication_proof:
        assert settings.seed_dataset_id is not None
        proof_by_field = await repository.verify_dataset(settings.seed_dataset_id)
        generation = await repository.publish_verified_seed(settings.seed_dataset_id)
        return {
            "dataset_id": settings.seed_dataset_id,
            "published": True,
            "generation": generation,
            "publication_proof_dataset_reused": True,
            "request_count": 0,
            "transient_retry_count": 0,
            "throttle_count": 0,
            **proof_by_field,
        }
    async with FHIRFormularyClient() as client:
        sync_result_by_field = await synchronize(
            client=client,
            repository=repository,
            run_id=settings.run_id,
            cutoff=settings.cutoff,
            publish=settings.should_publish,
            seed_eligible=settings.is_manual_seed,
            alias_concurrency=settings.concurrency,
        )
        sync_result_by_field.update(
            {
                "request_count": client.request_count,
                "transient_retry_count": client.transient_retry_count,
                "throttle_count": client.throttle_count,
            }
        )
        return sync_result_by_field


async def _synchronize_with_global_lock(
    ctx: dict[str, Any],
    settings: _FormularyRunSettings,
) -> dict[str, Any]:
    """Run one fenced synchronization while owning the global advisory lock."""

    repository = FHIRFormularyRepository()
    async with db.acquire() as lock_connection:
        is_acquired = bool(
            await lock_connection.scalar(
                "SELECT pg_try_advisory_lock(hashtextextended(:lock_name, 0));",
                lock_name=FORMULARY_FHIR_LOCK_NAME,
            )
        )
        if not is_acquired:
            raise RuntimeError("another formulary-fhir run is already active")
        try:
            async with asyncio.timeout(settings.deadline_seconds):
                sync_result_by_field = await _execute_run(settings, repository)
                sync_result_by_field.update(
                    {
                        "run_id": settings.run_id,
                        "manual_seed": settings.is_manual_seed,
                        "publication_proof": settings.is_publication_proof,
                        "deadline_seconds": settings.deadline_seconds,
                    }
                )
                ctx.setdefault("context", {})["audit"] = sync_result_by_field
                return sync_result_by_field
        finally:
            await lock_connection.scalar(
                "SELECT pg_advisory_unlock(hashtextextended(:lock_name, 0));",
                lock_name=FORMULARY_FHIR_LOCK_NAME,
            )


async def process_data(
    ctx: dict[str, Any],
    task: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run one validated control-plane task through the fenced synchronizer."""

    settings = _validated_run_settings(ctx, dict(task or {}))
    return await _synchronize_with_global_lock(ctx, settings)


async def main(
    *,
    manual_seed: bool = False,
    publication_proof: bool = False,
    seed_dataset_id: str | None = None,
    publish: bool = False,
    cutoff: str | None = None,
    alias_concurrency: int = 4,
) -> dict[str, Any]:
    """Run the CLI entrypoint with explicit non-automated settings."""

    await db.connect()
    try:
        return await process_data(
            {"context": {}},
            {
                "manual_seed": manual_seed,
                "publication_proof": publication_proof,
                "seed_dataset_id": seed_dataset_id,
                "publish": publish,
                "cutoff": cutoff,
                "alias_concurrency": alias_concurrency,
            },
        )
    finally:
        await db.disconnect()
