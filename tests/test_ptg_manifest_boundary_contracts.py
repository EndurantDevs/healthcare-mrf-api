# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewer-facing contracts for strict scanner manifest boundaries."""

from __future__ import annotations

import asyncio
import importlib
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.ptg_parts.domain import PTG2DownloadedJob
from tests.ptg_frozen_test_support import (
    frozen_artifacts,
    frozen_descriptor_by_ordinal,
)

ptg = importlib.import_module("process.ptg")


def test_manifest_copy_collection_skips_malformed_entries():
    summaries = [
        {},
        {"summary": {"manifest": {"copy_files": "bad"}}},
        {
            "summary": {
                "manifest": {
                    "copy_files": {
                        "price_atom": [
                            "bad",
                            {"path": ""},
                            {"path": "/tmp/price.copy", "row_count": "bad"},
                        ]
                    }
                }
            }
        },
    ]

    paths_by_kind, emitted_rows_by_kind = ptg._collect_manifest_copy_files(
        summaries,
        ["price_atom"],
    )
    entries = ptg._collect_manifest_copy_entries(
        summaries,
        ["price_atom"],
    )

    assert paths_by_kind == {"price_atom": [Path("/tmp/price.copy")]}
    assert emitted_rows_by_kind == {"price_atom": 0}
    assert entries == {"price_atom": [{"path": "/tmp/price.copy", "row_count": "bad"}]}
    assert ptg._count_manifest_copy_sources(
        [{"summary": {"manifest": {"copy_files": "bad"}}}],
        ["price_atom"],
    ) == {"price_atom": 0}
    assert ptg._manifest_summary_payloads({"summary": "bad"}) == ({}, {})
    assert (
        ptg._manifest_serving_row_count(
            {"serving_rates": "bad"},
            {},
        )
        == 0
    )


@pytest.mark.asyncio
async def test_manifest_copy_concurrency_and_progress_boundaries(monkeypatch):
    copied_paths = []

    async def copy_file(path, **kwargs):
        copied_paths.append((path, kwargs["target_table"]))

    progress = ptg._ManifestCopyProgress(
        kind="price_atom",
        target_table="stage",
        completed_steps_before_copy=1,
        total_steps=2,
        input_file_count=2,
        input_bytes=10,
        started_at=0.0,
        progress_by_field={
            "copied_bytes": 0,
            "last_emitted_bytes": 0,
            "next_progress_at": 100.0,
        },
        lock=threading.Lock(),
    )
    progress._report_copied_bytes(0)
    await ptg._copy_one_manifest_path(
        Path("one"),
        target_table="stage",
        copy_func=copy_file,
        progress_callback=lambda _count: None,
        semaphore=asyncio.Semaphore(1),
    )
    await ptg._copy_manifest_paths(
        [Path("two"), Path("three")],
        target_table="stage",
        copy_func=copy_file,
        progress_callback=lambda _count: None,
        copy_tasks=2,
    )

    assert copied_paths == [
        (Path("one"), "stage"),
        (Path("two"), "stage"),
        (Path("three"), "stage"),
    ]

    def reject_unlink(*_args, **_kwargs):
        raise OSError("locked")

    monkeypatch.setattr(Path, "unlink", reject_unlink)
    monkeypatch.setattr(
        ptg,
        "_cleanup_empty_manifest_copy_siblings",
        lambda _path: None,
    )
    ptg._cleanup_manifest_copy_paths({"price_atom": [Path("/tmp/price.copy")]})


def test_pending_sidecar_shapes_are_bounded_and_deduplicated():
    assert ptg._pending_sidecar_entries({}) is None
    mapped = ptg._pending_sidecar_entries(
        {"summary": {"manifest": {"sidecars": {"a": {"path": "/a"}}}}}
    )
    listed = ptg._pending_sidecar_entries(
        {"summary": {"manifest": {"sidecars": [{"path": "/b"}, "bad"]}}}
    )
    fallback = ptg._pending_sidecar_entries(
        {
            "summary": {
                "manifest": {
                    "sidecars": "bad",
                    "sidecar_paths": {"c": "/c", "empty": ""},
                }
            }
        }
    )

    assert mapped == [{"path": "/a"}]
    assert listed == [{"path": "/b"}]
    assert fallback == [{"name": "c", "path": "/c"}]

    state = ptg._PendingStrictV3State(
        copy_entries_by_kind={},
        graph_artifacts_map={"sidecars": "bad"},
    )
    ptg._merge_pending_sidecars(
        state,
        [{"path": ""}, {"path": "/a"}, {"path": "/a"}],
    )
    assert state.graph_artifacts_map["sidecars"] == [{"path": "/a"}]


def test_file_result_and_allowed_metric_shapes_are_explicit():
    context = ptg._InNetworkFileContext(
        job={"url": "https://rates.example.test/rates.json.gz"},
        classes={},
        test_mode=True,
    )
    parsed = ptg._InNetworkParseResult(
        url=context.job["url"],
        file_id="file-a",
        source_version=None,
        parse_summary={"serving_rates": 0},
    )

    file_result = ptg._in_network_file_result(context, parsed)
    metrics = ptg._allowed_amount_metrics_from_results(
        [
            {"source_type": "in_network"},
            {"source_type": "allowed_amounts", "summary": "bad"},
            {
                "source_type": "allowed_amounts",
                "summary": {
                    "allowed_amount_payments": "bad",
                    "allowed_amount_provider_payments": 2,
                },
            },
        ]
    )

    assert file_result.skipped is False
    assert file_result.summary["serving_rates"] == 0
    assert metrics["allowed_amount_payments"] == 0
    assert metrics["allowed_amount_evidence"] is True


@pytest.mark.parametrize(
    ("manifest_value", "expected"),
    [
        ('{"ready":true}', {"ready": True}),
        ("not-json", {}),
        ("[]", {}),
        (None, {}),
    ],
)
def test_published_manifest_accepts_only_objects(manifest_value, expected):
    assert ptg._published_snapshot_manifest({"manifest": manifest_value}) == expected


def test_manifest_sidecars_and_fallback_trace_are_deterministic(monkeypatch):
    assert ptg._manifest_sidecars_list(
        {"sidecars": {"a": {"path": "/a"}, "b": "bad"}}
    ) == [{"path": "/a"}]
    assert ptg._manifest_sidecars_list({"sidecars": [{"path": "/b"}, "bad"]}) == [
        {"path": "/b"}
    ]
    assert ptg._manifest_sidecars_list({"sidecars": "bad"}) == []

    fallback = ptg._collect_manifest_artifacts(
        [
            {},
            {"summary": {}},
            {
                "summary": {
                    "manifest": {
                        "source_trace_set_hash": "trace-set-a",
                        "sidecars": [{"path": "/a"}],
                    }
                }
            },
        ]
    )
    assert fallback["source_trace_set_hash"] == "trace-set-a"
    assert fallback["sidecars"][0]["source_shard_id"] == "manifest:2"

    monkeypatch.setattr(
        ptg,
        "_collect_ptg2_manifest_sidecar_artifacts",
        lambda _paths, **_kwargs: {"x": {"path": "/fallback"}},
    )
    artifacts = ptg._collect_manifest_artifacts(
        [
            {
                "file_id": 7,
                "summary": {
                    "manifest": {
                        "source_trace_hash": "trace-a",
                        "network_names": ["Network A"],
                        "sidecar_paths": {"x": "/ignored"},
                    }
                },
            }
        ]
    )
    assert artifacts["network_names"] == ["Network A"]
    assert artifacts["sidecars"] == [{"path": "/fallback", "source_shard_id": "file:7"}]


@pytest.mark.asyncio
async def test_provider_membership_builder_rejects_wrong_record(monkeypatch):
    monkeypatch.setattr(
        ptg,
        "_ptg2_existing_manifest_copy_paths",
        lambda paths: paths,
    )
    monkeypatch.setattr(
        ptg,
        "_ptg2_provider_membership_sidecar_command",
        lambda **_kwargs: ["scanner"],
    )
    monkeypatch.setattr(
        ptg.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(stdout=b"wrong\t2\n{}"),
    )

    with pytest.raises(RuntimeError, match="invalid output"):
        await ptg._build_ptg2_provider_membership_sidecars(
            provider_group_npi_path=Path("/tmp/group"),
            provider_npi_group_path=Path("/tmp/npi"),
            provider_npi_scope_copy_path=Path("/tmp/scope"),
            input_paths=[],
        )


def test_v3_identity_metadata_rejects_malformed_and_conflicting_rows():
    identity_payload_by_field = {"raw_container_sha256": "a" * 64}
    with pytest.raises(RuntimeError, match="must be a list"):
        ptg._bind_v3_entry_identity(
            "bad",
            identity_payload=identity_payload_by_field,
            label="x",
        )
    with pytest.raises(RuntimeError, match="objects"):
        ptg._bind_v3_entry_identity(
            ["bad"],
            identity_payload=identity_payload_by_field,
            label="x",
        )
    with pytest.raises(RuntimeError, match="conflicting physical"):
        ptg._bind_v3_entry_identity(
            [{"raw_container_sha256": "b" * 64}],
            identity_payload=identity_payload_by_field,
            label="x",
        )
    with pytest.raises(RuntimeError, match="source-run contract"):
        ptg._bind_provider_metadata_contract(
            {"provider_set_metadata": [{"source_run_contract_sha256": "wrong"}]},
            identity_payload=identity_payload_by_field,
            source_run_contract_sha256="c" * 64,
        )


def test_shared_preflight_requires_complete_in_network_plan(tmp_path):
    descriptor = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(descriptor, tmp_path)
    assert ptg._is_shared_v3_preflight_eligible([]) is False
    assert (
        ptg._is_shared_v3_preflight_eligible(
            [
                PTG2DownloadedJob(
                    job={"type": "in_network", "plan_info": []},
                    raw_artifact=raw_artifact,
                    logical_artifact=logical_artifact,
                )
            ]
        )
        is False
    )
    assert (
        ptg._is_shared_v3_preflight_eligible(
            [
                PTG2DownloadedJob(
                    job={"type": "allowed_amounts", "plan_info": [{"plan_id": "x"}]},
                    raw_artifact=raw_artifact,
                    logical_artifact=logical_artifact,
                )
            ]
        )
        is False
    )


def test_small_publication_and_identity_helpers_fail_closed(tmp_path):
    descriptor = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(descriptor, tmp_path)
    downloaded = PTG2DownloadedJob(
        job={"type": "in_network"},
        raw_artifact=raw_artifact,
        logical_artifact=logical_artifact,
    )
    screen_line = ptg._raw_job_dedupe_screen_line(
        {"type": "in_network", "url": descriptor["canonical_url"]},
        downloaded,
    )

    assert "raw_sha256=" in screen_line
    assert ptg._frozen_rate_file_proof({}, []) == []
    assert ptg._frozen_publication_fields({}, []) == {}
    assert ptg._normalize_source_network_names(
        ["Network A", "", "Network A", "Network B"]
    ) == ["Network A", "Network B"]
    with pytest.raises(TypeError, match="unexpected keyword"):
        ptg._forwarded_main_arguments(
            {"runtime_options_by_name": {"unsupported": True}}
        )


def test_default_import_ids_keep_legacy_month_without_source_inputs():
    import_month = ptg.datetime.date(2026, 7, 1)
    assert ptg._default_ptg2_import_id(import_month, None) == "20260701"
    assert (
        ptg._default_ptg2_import_id(
            import_month,
            "source-a",
        )
        == "20260701"
    )
    assert (
        ptg._frozen_ptg2_import_id(
            import_month,
            None,
            frozen_rate_file_set_sha256="a" * 64,
            frozen_rate_file_count=2,
            arch_variant="shared_blocks_v4",
        )
        == "20260701"
    )


def test_json_scanner_and_toc_loader_reject_non_object(monkeypatch):
    assert ptg._json_string_scan_state(
        '"',
        is_in_string=False,
        is_escaped=False,
    ) == (True, False, True)
    assert ptg._json_string_scan_state(
        "x",
        is_in_string=True,
        is_escaped=True,
    ) == (True, False, True)
    monkeypatch.setattr(ptg, "load_json_artifact", lambda _path: [])
    with pytest.raises(ValueError, match="JSON object"):
        ptg._load_table_of_contents_artifact("ignored")


@pytest.mark.asyncio
async def test_candidate_reuse_requires_activation_source_index_and_resources(
    monkeypatch,
):
    valid_activation_by_field = {
        "contract": ptg.PTG2_CANDIDATE_ACTIVATION_CONTRACT,
        "state": "validated",
        "source_key": "source-a",
    }
    cases = [
        ({"manifest": {}}, "candidate contract"),
        (
            {
                "manifest": {
                    "activation": {
                        **valid_activation_by_field,
                        "source_key": "other",
                    }
                }
            },
            "candidate source",
        ),
        (
            {"manifest": {"activation": valid_activation_by_field}},
            "serving index",
        ),
    ]
    for snapshot_attributes, message in cases:
        with pytest.raises(RuntimeError, match=message):
            await ptg._validated_candidate_reuse_state(
                snapshot_attributes,
                snapshot_id="snapshot-a",
                source_key="source-a",
            )

    async def missing_resources(*_args, **_kwargs):
        return ["missing-table"], []

    monkeypatch.setattr(
        ptg,
        "_missing_snapshot_serving_resources",
        missing_resources,
    )
    with pytest.raises(RuntimeError, match="resources are missing"):
        await ptg._validated_candidate_reuse_state(
            {
                "manifest": {
                    "activation": valid_activation_by_field,
                    "serving_index": {},
                }
            },
            snapshot_id="snapshot-a",
            source_key="source-a",
        )


def test_v3_result_identity_requires_manifest_and_copy_contract():
    identity = SimpleNamespace(as_dict=lambda: {"raw_container_sha256": "a" * 64})
    missing_manifest = ptg.PTG2FileProcessResult(
        "in_network",
        "https://rates.example.test/rates.json.gz",
        True,
        summary={},
    )
    with pytest.raises(RuntimeError, match="missing its manifest"):
        ptg._annotate_v3_result_identity(missing_manifest, identity, {})

    skipped = ptg.PTG2FileProcessResult(
        "in_network",
        missing_manifest.url,
        True,
        summary={"manifest": {}},
        skipped=True,
    )
    assert ptg._annotate_v3_result_identity(skipped, identity, {}) is skipped

    incomplete = ptg.PTG2FileProcessResult(
        "in_network",
        missing_manifest.url,
        True,
        summary={"manifest": {}},
    )
    with pytest.raises(RuntimeError, match="deferred COPY"):
        ptg._annotate_v3_result_identity(incomplete, identity, {})
