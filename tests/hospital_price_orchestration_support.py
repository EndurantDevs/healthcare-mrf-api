# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Isolated module harness for hospital-price orchestration tests."""

from __future__ import annotations

import contextlib
import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from process.ptg_parts.canonical import canonicalize_url


ROOT = Path(__file__).resolve().parents[1]


def _module(name: str, **attributes: Any) -> types.ModuleType:
    module = types.ModuleType(name)
    for attribute, value in attributes.items():
        setattr(module, attribute, value)
    return module


@dataclass
class Attempt:
    attempt_id: str
    hospital_id: str
    hospital_name: str
    source_url: str
    expected_generation: int
    locator_name: str | None = None
    locator_url: str | None = None
    final_source_url: str | None = None
    source_http_status: int | None = None


@dataclass(frozen=True)
class DownloadedSource:
    url: str
    raw: Any | None
    attempts: tuple[Attempt, ...]
    error_code: str | None = None
    error_detail: str | None = None
    auth_refresh_required: bool = False


class ArtifactStore:
    def __init__(self, tmp_dir: Path | None = None) -> None:
        self.tmp_dir = tmp_dir
        self.root = tmp_dir


def configure_incomplete_import(
    orchestrator: Any,
    monkeypatch: Any,
    resolved_failures: int,
    pipeline_metrics: dict[str, int],
) -> None:
    """Install deterministic collaborators for incomplete-cohort proof."""

    async def noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def bounded(*_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def resolve(*_args: Any, **_kwargs: Any) -> tuple[dict[str, Any], int, int]:
        return {}, 0, resolved_failures

    async def pipeline(*_args: Any, **_kwargs: Any) -> dict[str, int]:
        return dict(pipeline_metrics)

    monkeypatch.setattr(orchestrator, "sync_registry", noop)
    monkeypatch.setattr(orchestrator, "raise_if_cancelled", noop)
    monkeypatch.setattr(orchestrator, "_bounded", bounded)
    monkeypatch.setattr(orchestrator, "_resolve_attempts", resolve)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)
    monkeypatch.setattr(
        orchestrator, "_resource_limits",
        lambda *_args: (1, 1, 1024, 4096, 2048, 1),
    )
    monkeypatch.setattr(orchestrator, "_stream_sources", pipeline)


def _acquisition_module(noop: Any) -> types.ModuleType:
    return _module(
        "process.hospital_price_acquisition",
        REGISTRY_VERSION=1,
        Attempt=Attempt,
        Candidate=object,
        canonicalize_url=canonicalize_url,
        MAX_HOSPITAL_HPT_LOCATOR_BYTES=1_000_000,
        PTG2_DEFAULT_MAX_BYTES=64 * 1024**3,
        DownloadedSource=DownloadedSource,
        candidates_from_locators=lambda _rows: (),
        download_source=noop,
        error_details=lambda exc: ("runtime", str(exc)),
        fetch_locator=noop,
        positive_env=lambda _name, default: default,
        run_native_parser=noop,
        schema_name=lambda: "mrf",
        sync_registry=noop,
    )


def _replacement_modules(noop: Any, lease_context: Any) -> dict[str, types.ModuleType]:
    return {
        "db.models": _module("db.models", db=SimpleNamespace()),
        "process.control_cancel": _module(
            "process.control_cancel", ImportCancelledError=RuntimeError,
            raise_if_cancelled=noop,
        ),
        "process.ext.utils": _module("process.ext.utils", ensure_database=noop),
        "process.hospital_hpt_registry": _module(
            "process.hospital_hpt_registry",
            selected_hospital_hpt_registry=lambda *_args, **_kwargs: (),
        ),
        "process.hospital_price_acquisition": _acquisition_module(noop),
        "process.hospital_price_native": _module(
            "process.hospital_price_native",
            HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES_ENV=(
                "HLTHPRT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES"
            ),
            detect_hospital_mrf_format=lambda _path, _max_bytes=None: "json",
            hospital_price_version_id=lambda digest: digest,
        ),
        "process.hospital_price_store": _module(
            "process.hospital_price_store", admit_attempts=noop,
            fail_attempts=noop, garbage_collect_superseded_versions=noop,
            has_existing_version=noop, publish_existing=noop,
            rebind_attempt_sources=noop, renew_attempt_leases=noop,
            stage_content=noop,
        ),
        "process.live_progress": _module(
            "process.live_progress", enqueue_live_progress=lambda **_kwargs: None
        ),
        "process.ptg_parts.artifacts": _module(
            "process.ptg_parts.artifacts", PTG2ArtifactStore=ArtifactStore
        ),
        "process.ptg_parts.db_tables": _module(
            "process.ptg_parts.db_tables", _quote_ident=lambda value: value
        ),
        "process.ptg_parts.input_artifact_retention": _module(
            "process.ptg_parts.input_artifact_retention",
            artifact_lease_context=lease_context,
            guard_artifact_lease=noop,
        ),
    }


def orchestrator_module() -> Any:
    async def noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def guard(_lease: Any, operation: Any) -> Any:
        return await operation

    @contextlib.contextmanager
    def lease_context(**_kwargs: Any):
        yield object()

    replacements_by_name = _replacement_modules(noop, lease_context)
    replacements_by_name[
        "process.ptg_parts.input_artifact_retention"
    ].guard_artifact_lease = guard
    orchestrator_name = "hospital_prices_orchestration_test"
    runtime_name = "process.hospital_price_runtime"
    replaced_names = (orchestrator_name, runtime_name, *replacements_by_name)
    prior_module_by_name = {
        module_name: sys.modules.get(module_name) for module_name in replaced_names
    }
    for module_name in replaced_names:
        sys.modules.pop(module_name, None)
    sys.modules.update(replacements_by_name)
    runtime_spec = importlib.util.spec_from_file_location(
        runtime_name, ROOT / "process/hospital_price_runtime.py"
    )
    assert runtime_spec is not None and runtime_spec.loader is not None
    runtime_module = importlib.util.module_from_spec(runtime_spec)
    sys.modules[runtime_name] = runtime_module
    runtime_spec.loader.exec_module(runtime_module)
    orchestrator_spec = importlib.util.spec_from_file_location(
        orchestrator_name, ROOT / "process/hospital_prices.py"
    )
    assert orchestrator_spec is not None and orchestrator_spec.loader is not None
    orchestrator = importlib.util.module_from_spec(orchestrator_spec)
    sys.modules[orchestrator_name] = orchestrator
    try:
        orchestrator_spec.loader.exec_module(orchestrator)
        return orchestrator
    finally:
        for module_name, prior_module in prior_module_by_name.items():
            if prior_module is None:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = prior_module
