# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capacity admission proof for hospital-price orchestration."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from tests.hospital_price_orchestration_support import (
    ArtifactStore as _ArtifactStore,
    orchestrator_module as _orchestrator_module,
)


def _configure_resource_budgets(orchestrator, monkeypatch) -> int:
    parser_memory = orchestrator._runtime.HOSPITAL_MRF_PARSER_BASE_MEMORY_BYTES + 80
    configured_env_by_name = {
        "HLTHPRT_HOSPITAL_MRF_MAX_BYTES": "100",
        "HLTHPRT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES": "120",
        "HLTHPRT_HOSPITAL_MRF_MAX_OUTPUT_BYTES": "80",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_RAW_BYTES": "250",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_SCRATCH_BYTES": "650",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_MEMORY_BYTES": str(2 * parser_memory),
        "HLTHPRT_HOSPITAL_PRICE_DATABASE_GROWTH_BYTES": "200",
        "HLTHPRT_HOSPITAL_PRICE_MIN_FREE_BYTES": "20",
    }
    for env_name, env_value in configured_env_by_name.items():
        monkeypatch.setenv(env_name, env_value)
    monkeypatch.setattr(
        orchestrator._runtime.shutil, "disk_usage",
        lambda _path: SimpleNamespace(free=2000),
    )
    return parser_memory


def test_artifact_store_requires_dedicated_absolute_root(tmp_path, monkeypatch):
    orchestrator = _orchestrator_module()
    env_name = orchestrator._runtime.HOSPITAL_PRICE_ARTIFACT_DIR_ENV
    monkeypatch.delenv(env_name, raising=False)
    with pytest.raises(RuntimeError, match="must be an absolute path"):
        orchestrator._runtime.hospital_price_artifact_store()
    monkeypatch.setenv(env_name, "relative")
    with pytest.raises(RuntimeError, match="must be an absolute path"):
        orchestrator._runtime.hospital_price_artifact_store()
    monkeypatch.setenv(env_name, str(tmp_path))
    assert orchestrator._runtime.hospital_price_artifact_store().root == tmp_path


def test_resource_limits_derive_workers_from_explicit_byte_budgets(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    parser_memory = _configure_resource_budgets(orchestrator, monkeypatch)

    assert orchestrator._runtime.resource_limits(
        _ArtifactStore(tmp_path), 8, 5, 0
    ) == (
        2, 2, 2, 2, 100, 120, 80, 1640,
    )
    assert orchestrator._runtime.resource_limits(
        _ArtifactStore(tmp_path), 1, 1, 0
    )[:4] == (1, 1, 2, 2)

    required_byte_counts: list[int] = []
    require_disk_capacity = orchestrator._runtime.require_disk_capacity
    monkeypatch.setattr(
        orchestrator._runtime, "require_disk_capacity",
        lambda _store, byte_count: required_byte_counts.append(byte_count),
    )
    orchestrator._runtime.resource_limits(_ArtifactStore(tmp_path), 8, 5, 3)
    assert required_byte_counts == [3_001_640]
    monkeypatch.setattr(
        orchestrator._runtime, "require_disk_capacity", require_disk_capacity
    )

    free_bytes = 1840
    monkeypatch.setattr(
        orchestrator._runtime.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=free_bytes),
    )
    limits = orchestrator._runtime.resource_limits(
        _ArtifactStore(tmp_path), 8, 5, 0
    )
    free_bytes = limits[-1]
    orchestrator._runtime.require_disk_capacity(
        _ArtifactStore(tmp_path), limits[-1]
    )
    free_bytes -= 1
    with pytest.raises(RuntimeError, match="storage capacity is insufficient"):
        orchestrator._runtime.require_disk_capacity(
            _ArtifactStore(tmp_path), limits[-1]
        )

    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_MEMORY_BYTES", str(parser_memory - 1)
    )
    with pytest.raises(RuntimeError, match="cannot admit one source"):
        orchestrator._runtime.resource_limits(_ArtifactStore(tmp_path), 8, 5, 0)
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_MEMORY_BYTES", str(2 * parser_memory)
    )
    monkeypatch.setenv("HLTHPRT_HOSPITAL_PRICE_ACTIVE_RAW_BYTES", "99")
    with pytest.raises(RuntimeError, match="cannot admit one source"):
        orchestrator._runtime.resource_limits(_ArtifactStore(tmp_path), 8, 5, 0)


def test_resource_limits_fail_before_work_when_capacity_is_unconfigured_or_low(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    for name in (
        "HLTHPRT_HOSPITAL_MRF_MAX_OUTPUT_BYTES",
        "HLTHPRT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_RAW_BYTES",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_SCRATCH_BYTES",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_MEMORY_BYTES",
        "HLTHPRT_HOSPITAL_PRICE_DATABASE_GROWTH_BYTES",
        "HLTHPRT_HOSPITAL_PRICE_MIN_FREE_BYTES",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError, match="MAX_OUTPUT_BYTES"):
        orchestrator._runtime.resource_limits(_ArtifactStore(tmp_path), 1, 1, 0)
    for env_name, env_value in {
        "HLTHPRT_HOSPITAL_MRF_MAX_BYTES": "100",
        "HLTHPRT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES": "100",
        "HLTHPRT_HOSPITAL_MRF_MAX_OUTPUT_BYTES": "100",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_RAW_BYTES": "100",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_SCRATCH_BYTES": "400",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_MEMORY_BYTES": str(
            2 * orchestrator._runtime.HOSPITAL_MRF_PARSER_BASE_MEMORY_BYTES
            + 200
        ),
        "HLTHPRT_HOSPITAL_PRICE_DATABASE_GROWTH_BYTES": "100",
        "HLTHPRT_HOSPITAL_PRICE_MIN_FREE_BYTES": "1",
    }.items():
        monkeypatch.setenv(env_name, env_value)
    monkeypatch.setattr(
        orchestrator._runtime.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=200),
    )
    monkeypatch.delenv(
        "HLTHPRT_HOSPITAL_PRICE_DATABASE_GROWTH_BYTES", raising=False
    )
    with pytest.raises(RuntimeError, match="DATABASE_GROWTH_BYTES"):
        orchestrator._runtime.resource_limits(_ArtifactStore(tmp_path), 1, 1, 0)
    monkeypatch.setenv("HLTHPRT_HOSPITAL_PRICE_DATABASE_GROWTH_BYTES", "100")
    with pytest.raises(RuntimeError, match="storage capacity is insufficient"):
        orchestrator._runtime.resource_limits(_ArtifactStore(tmp_path), 1, 1, 0)
