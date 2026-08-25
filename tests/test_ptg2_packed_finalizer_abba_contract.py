"""Pure contract checks for the non-representative packed-finalizer screen."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts.ptg2_shared_block_copy import scan_shared_block_copy
from scripts.research.ptg2_packed_finalizer_abba_contract import (
    ALL_OBJECT_KINDS,
    ARTIFACT_CONTRACT,
    ArtifactFileReceipt,
    BenchmarkShape,
    CANONICAL_FIELDS,
    PRICE_OBJECT_KINDS,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
    REPRESENTATIVE_CLASSIFICATION,
    SHAPE_CONTRACT,
    SYNTHETIC_CLASSIFICATION,
    _mechanism_gates,
    default_synthetic_shape,
)
from scripts.research.ptg2_packed_finalizer_abba_artifacts import generate_artifacts
from scripts.research.ptg2_packed_finalizer_abba_inputs import (
    load_representative_artifacts,
)
from scripts.research import ptg2_packed_finalizer_abba as abba
from scripts.research import ptg2_packed_finalizer_abba_lifecycle as lifecycle
from scripts.research import ptg2_packed_finalizer_abba_local_screen as local_screen


def test_mechanism_speed_gate_uses_comparable_whole_wrapper_time():
    def arm(kind: str, finalizer_seconds: float, whole_seconds: float):
        return {
            "arm": kind,
            "finalizer_seconds": finalizer_seconds,
            "finalizer_rows_per_second": 300_000.0,
            "prepare_plus_publication_plus_summary_seconds": whole_seconds,
            "prepare_plus_publication_plus_summary_rows_per_second": 154_160.0,
            "summary": {field: field for field in CANONICAL_FIELDS},
        }

    gates = _mechanism_gates(
        [arm("legacy", 1.0, 2.0), arm("packed", 10.0, 1.0)] * 2
    )

    assert gates["packed_faster_than_legacy_whole_wrapper"] is True


@pytest.mark.asyncio
async def test_install_arm_schema_binds_both_schema_environment_names(monkeypatch):
    schema_name = "ptg_packed_abba_0123456789ab_b1"
    observed_by_name = {}

    async def capture_migration(_dsn, _migration, _action):
        observed_by_name.update(
            DB_SCHEMA=os.environ.get("DB_SCHEMA"),
            HLTHPRT_DB_SCHEMA=os.environ.get("HLTHPRT_DB_SCHEMA"),
        )

    monkeypatch.setattr(lifecycle, "_create_shared_schema", AsyncMock())
    monkeypatch.setattr(lifecycle.db, "execute_ddl", AsyncMock())
    monkeypatch.setattr(lifecycle, "migration", lambda _name: object())
    monkeypatch.setattr(lifecycle, "run_migration_action", capture_migration)
    monkeypatch.setenv("DB_SCHEMA", "mrf")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")

    await lifecycle.install_arm_schema("postgresql://test", schema_name=schema_name)

    assert observed_by_name == {
        "DB_SCHEMA": schema_name,
        "HLTHPRT_DB_SCHEMA": schema_name,
    }
    assert os.environ["DB_SCHEMA"] == "mrf"
    assert os.environ["HLTHPRT_DB_SCHEMA"] == "mrf"


def test_synthetic_shape_and_artifacts_are_exact_and_fail_closed(tmp_path):
    default = default_synthetic_shape()
    assert (
        default.mapping_count,
        default.unique_block_count,
        default.finalizer_mapping_count,
        default.finalizer_unique_block_count,
        default.map_pack_count,
    ) == (8_000_000, 1_301_768, 7_999_998, 1_301_766, 31_254)
    tiny_shape = BenchmarkShape.from_mapping(
        {
            "contract": SHAPE_CONTRACT,
            "classification": SYNTHETIC_CLASSIFICATION,
            "allocation_by_kind": {
                kind: {
                    "mapping_count": 1 if kind in PRICE_OBJECT_KINDS else 2,
                    "unique_block_count": 1,
                }
                for kind in ALL_OBJECT_KINDS
            },
        }
    )
    artifacts = generate_artifacts(tmp_path / "artifacts", tiny_shape)
    try:
        scans = tuple(
            scan_shared_block_copy(artifact.path)
            for artifact in (
                artifacts.serving,
                artifacts.price_dictionary,
                artifacts.relational_price,
            )
        )
        assert tuple(scan.row_count for scan in scans) == (10, 2, 2)
        assert artifacts.expected_summary["mapping_count"] == 14
        assert artifacts.expected_summary["unique_block_count"] == 8
        assert artifacts.expected_summary["map_pack_count"] == 6
        assert artifacts.expected_summary["packed_canonical_byte_count"] > 0
        summary = artifacts.finalizer_summary()["blocks"]
        assert sum(summary["serving"]["artifact_record_counts"].values()) == 10
        assert summary["serving"]["row_count"] == scans[0].row_count
        assert summary["serving"]["stored_payload_bytes"] == scans[0].stored_payload_bytes
        manifest = json.loads(artifacts.manifest_path.read_text())
        assert manifest["contract"] == ARTIFACT_CONTRACT
        assert manifest["shape"] == tiny_shape.as_dict()
    finally:
        artifacts.cleanup()
    assert not artifacts.directory.exists()
    representative = tiny_shape.as_dict()
    representative["classification"] = REPRESENTATIVE_CLASSIFICATION
    representative["source_receipt_sha256"] = "00" * 32
    with pytest.raises(ValueError, match="synthetic, not representative"):
        generate_artifacts(tmp_path / "forbidden", BenchmarkShape.from_mapping(representative))
    assert tuple(sorted(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS)) == (
        PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    )


def test_representative_artifacts_reject_self_attested_promotion(tmp_path):
    tiny_shape = BenchmarkShape.from_mapping(
        {
            "contract": SHAPE_CONTRACT,
            "classification": SYNTHETIC_CLASSIFICATION,
            "allocation_by_kind": {
                kind: {"mapping_count": 1, "unique_block_count": 1}
                for kind in ALL_OBJECT_KINDS
            },
        }
    )
    artifacts = generate_artifacts(tmp_path / "artifacts", tiny_shape)
    source_receipt = tmp_path / "source.json"
    source_receipt.write_text('{"source":"fixture"}\n', encoding="utf-8")
    source_sha256 = hashlib.sha256(source_receipt.read_bytes()).hexdigest()
    manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))
    manifest["shape"].update(
        classification=REPRESENTATIVE_CLASSIFICATION,
        source_receipt_sha256=source_sha256,
    )
    representative_shape = BenchmarkShape.from_mapping(manifest["shape"])
    manifest["shape_sha256"] = representative_shape.sha256()
    artifacts.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="externally authenticated"):
        load_representative_artifacts(
            artifacts.manifest_path,
            source_receipt,
        )
    artifacts.cleanup()


def test_artifact_receipt_serializes_beneath_symlinked_root(tmp_path):
    real_root = tmp_path / "real"
    real_root.mkdir()
    linked_root = tmp_path / "linked"
    linked_root.symlink_to(real_root, target_is_directory=True)
    artifact_path = real_root / "lane.copy"
    artifact_path.write_bytes(b"x")

    receipt = ArtifactFileReceipt(
        path=artifact_path.resolve(),
        byte_count=1,
        row_count=1,
        stored_payload_bytes=1,
        sha256=hashlib.sha256(b"x").hexdigest(),
    )

    assert receipt.as_dict(linked_root)["path"] == "lane.copy"


@pytest.mark.asyncio
async def test_abba_coordinator_records_failure_and_cleans_root(monkeypatch, tmp_path):
    root = tmp_path / "run"

    def _make_root(**_kwargs):
        root.mkdir()
        return str(root)

    resource_configuration = SimpleNamespace(
        contract_metadata=lambda: {},
        validation_metadata=lambda: {},
    )
    monkeypatch.setenv(abba.OPT_IN_ENV, "1")
    monkeypatch.setenv(
        abba.DSN_ENV,
        "postgresql://postgres@127.0.0.1:5440/ptg_packed_finalizer_test_guard",
    )
    monkeypatch.setattr(abba.tempfile, "mkdtemp", _make_root)
    monkeypatch.setattr(abba, "capture_source_identity", lambda _path: {})
    monkeypatch.setattr(
        abba,
        "_load_v3_finalizer_resource_configuration",
        lambda: resource_configuration,
    )
    monkeypatch.setattr(abba.db, "connect", AsyncMock(side_effect=RuntimeError("boom")))
    disconnect = AsyncMock()
    monkeypatch.setattr(abba.db, "disconnect", disconnect)

    database_environment_names = (
        "HLTHPRT_DB_DRIVER",
        "HLTHPRT_DB_HOST",
        "HLTHPRT_DB_PORT",
        "HLTHPRT_DB_USER",
        "HLTHPRT_DB_PASSWORD",
        "HLTHPRT_DB_DATABASE",
        "HLTHPRT_DB_POOL_MIN_SIZE",
        "HLTHPRT_DB_POOL_MAX_SIZE",
    )
    original_port = os.environ.get("HLTHPRT_DB_PORT")
    with monkeypatch.context() as database_environment:
        for name in database_environment_names:
            database_environment.setenv(name, os.environ.get(name, ""))
        receipt, exit_code = await abba._run_abba(
            SimpleNamespace(artifacts=None, source_receipt=None, shape=None)
        )
    assert os.environ.get("HLTHPRT_DB_PORT") == original_port

    assert exit_code == 1
    assert receipt["status"] == "failed"
    assert receipt["errors"] == [
        {"phase": "run", "type": "RuntimeError", "message": "boom"}
    ]
    assert receipt["cleanup"]["local_root_removed"] is True
    assert not root.exists()
    disconnect.assert_awaited_once()


def test_local_screen_requires_opt_in_and_synthetic_inputs(monkeypatch):
    monkeypatch.delenv(local_screen.LOCAL_SCREEN_OPT_IN_ENV, raising=False)
    with pytest.raises(RuntimeError, match="for this local screen"):
        local_screen.main()

    monkeypatch.setenv(local_screen.LOCAL_SCREEN_OPT_IN_ENV, "1")
    monkeypatch.setattr(sys, "argv", ["local-screen", "--artifacts", "manifest"])
    with pytest.raises(RuntimeError, match="synthetic inputs only"):
        local_screen.main()

    monkeypatch.setattr(sys, "argv", ["local-screen"])
    monkeypatch.setattr(local_screen.abba, "main", lambda: 7)
    monkeypatch.setattr(
        local_screen.abba,
        "verify_benchmark_environment",
        local_screen.abba.verify_benchmark_environment,
    )
    assert local_screen.main() == 7
    assert local_screen.abba.verify_benchmark_environment is local_screen._verify_local_screen
