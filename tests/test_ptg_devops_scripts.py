# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
import asyncio
import os
from pathlib import Path
import subprocess

import yaml


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts" / "devops"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _load_script(module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_DIR / f"{module_name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ptg_devops_scripts_import_without_database_connection():
    for module_name in (
        "ptg2_rebuild_snapshot_from_options",
        "ptg2_remove_source_snapshot",
        "ptg2_strict_v3_cutover_ready",
    ):
        assert _load_script(module_name).__name__ == module_name


def test_dev_deploy_workflow_requires_successful_ci_for_exact_main_sha():
    workflow = (REPOSITORY_ROOT / ".github/workflows/deploy-dev.yml").read_text()

    assert 'workflows: ["CI"]' in workflow
    assert "github.event.workflow_run.conclusion == 'success'" in workflow
    assert "Authorize exact tested source SHA" in workflow
    assert "context.ref !== 'refs/heads/main'" in workflow
    assert "workflow_id: 'ci.yml'" in workflow
    assert "head_sha: deploySha" in workflow
    assert "run.head_sha === deploySha" in workflow
    assert "run.event === 'push'" in workflow
    assert "run.head_repository?.full_name === expectedRepository" in workflow
    assert "deploySha !== mainSha" in workflow
    assert "Reader promotion requires the exact staged deploy_sha" in workflow
    assert "!['auto', 'reader', 'recovery-writer'].includes(phase)" in workflow
    assert "${phase} deployment requires current main ${mainSha}" in workflow
    assert workflow.count("phase = 'auto'") == 1
    assert "workflow_dispatch:" in workflow
    assert "- recovery-writer" in workflow
    assert "--detach" not in workflow
    assert "timeout-minutes: 90" in workflow
    assert workflow.count("-o ServerAliveInterval=30") == 1
    assert workflow.count("-o ServerAliveCountMax=3") == 1
    assert "run.conclusion === 'success'" in workflow


def test_dev_deploy_workflow_requires_one_terminal_source_bound_receipt():
    workflow = (REPOSITORY_ROOT / ".github/workflows/deploy-dev.yml").read_text()

    assert "set -o pipefail" in workflow
    assert '2>&1 | tee "${receipt_log}"' in workflow
    assert "receipt_count=$(grep -Ec" in workflow
    assert 'tail -n 1 "${receipt_log}"' in workflow
    for field in (
        "healthporta_deploy_receipt_v1 service=healthcare-mrf-api",
        "source_sha=${DEPLOY_SHA}",
        "deploy_sha=[0-9a-f]{40}",
        "image=[^[:space:]]+",
        "manifest_digest=sha256:[0-9a-f]{64}",
        "config_digest=sha256:[0-9a-f]{64}",
    ):
        assert field in workflow


def test_dev_deploy_workflow_executes_receipt_failure_paths(tmp_path):
    workflow = yaml.safe_load(
        (REPOSITORY_ROOT / ".github/workflows/deploy-dev.yml").read_text()
    )
    deploy_script = next(
        step["run"]
        for step in workflow["jobs"]["queue"]["steps"]
        if step.get("name") == "Deploy on dev node"
    )
    ssh_stub = tmp_path / "ssh"
    ssh_stub.write_text(
        '#!/bin/sh\nprintf \'%s\\n\' "${SSH_OUTPUT}"\nexit "${SSH_STATUS}"\n'
    )
    ssh_stub.chmod(0o755)

    source_sha = "a" * 40
    receipt = (
        "healthporta_deploy_receipt_v1 service=healthcare-mrf-api "
        f"source_sha={source_sha} deploy_sha={'b' * 40} image=registry/image@sha256:digest "
        f"manifest_digest=sha256:{'c' * 64} config_digest=sha256:{'d' * 64}"
    )
    environment_by_name = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "RUNNER_TEMP": str(tmp_path),
        "DEV_DEPLOY_HOST": "localhost",
        "DEPLOY_SHA": source_sha,
        "DEPLOY_BRANCH": "main",
        "PTG_V3_PHASE": "auto",
    }
    cases = (
        ("valid", f"progress\n{receipt}", 0, True),
        ("duplicate", f"{receipt}\n{receipt}", 0, False),
        ("non-terminal", f"{receipt}\ntrailing output", 0, False),
        ("ssh failure", receipt, 42, False),
    )

    for label, output, ssh_status, should_succeed in cases:
        completed = subprocess.run(
            ["bash", "-c", deploy_script],
            env={
                **environment_by_name,
                "SSH_OUTPUT": output,
                "SSH_STATUS": str(ssh_status),
            },
            check=False,
            capture_output=True,
            text=True,
        )
        assert (completed.returncode == 0) is should_succeed, (
            label,
            completed.stdout,
            completed.stderr,
        )


def test_cutover_requires_idle_valid_pointers():
    module = _load_script("ptg2_strict_v3_cutover_ready")
    statements = []
    parameters = []

    class Executor:
        async def all(self, statement, **params):
            statements.append(statement)
            parameters.append(params)
            if "WITH pointers AS" in statement:
                return [
                    {
                        "pointer_count": 7,
                        "missing_snapshot_count": 0,
                        "unpublished_snapshot_count": 0,
                        "invalid_arch_count": 0,
                        "invalid_manifest_generation_count": 0,
                        "unsealed_source_set_count": 0,
                        "unsealed_audit_sample_count": 0,
                        "missing_binding_count": 0,
                        "unsealed_layout_count": 0,
                        "invalid_layout_generation_count": 0,
                        "mismatched_binding_count": 0,
                    }
                ]
            return [
                {
                    "active_import_run_count": 0,
                    "building_snapshot_count": 0,
                    "stale_import_run_count": 2,
                    "stale_building_snapshot_count": 2,
                }
            ]

    readiness = asyncio.run(
        module.collect_cutover_readiness(Executor(), schema_name="mrf")
    )

    assert readiness["ready"] is True
    assert readiness["pointer_count"] == 7
    assert readiness["stale_import_run_count"] == 2
    assert readiness["stale_building_snapshot_count"] == 2
    assert readiness["stale_activity_seconds"] == 21_600
    assert not any(readiness["failure_counts"].values())
    assert "manifest::jsonb AS manifest_jsonb" in statements[0]
    assert "jsonb_typeof(snapshot.manifest_jsonb" in statements[0]
    assert "heartbeat_at" in statements[1]
    assert parameters[1] == {"stale_activity_seconds": 21_600}


def test_cutover_rejects_legacy_or_active():
    module = _load_script("ptg2_strict_v3_cutover_ready")

    class Executor:
        async def all(self, statement, **_params):
            if "WITH pointers AS" in statement:
                return [
                    {
                        "pointer_count": 3,
                        "missing_snapshot_count": 0,
                        "unpublished_snapshot_count": 0,
                        "invalid_arch_count": 1,
                        "invalid_manifest_generation_count": 1,
                        "unsealed_source_set_count": 1,
                        "unsealed_audit_sample_count": 1,
                        "missing_binding_count": 1,
                        "unsealed_layout_count": 1,
                        "invalid_layout_generation_count": 1,
                        "mismatched_binding_count": 1,
                    }
                ]
            return [
                {
                    "active_import_run_count": 1,
                    "building_snapshot_count": 1,
                }
            ]

    readiness = asyncio.run(
        module.collect_cutover_readiness(Executor(), schema_name="mrf")
    )

    assert readiness["ready"] is False
    assert readiness["active_import_run_count"] == 1
    assert readiness["failure_counts"]["invalid_arch_count"] == 1


def test_cutover_stale_activity_is_reported_without_blocking():
    module = _load_script("ptg2_strict_v3_cutover_ready")

    class Executor:
        async def all(self, statement, **params):
            if "WITH pointers AS" in statement:
                return [
                    {
                        "pointer_count": 1,
                        "missing_snapshot_count": 0,
                        "unpublished_snapshot_count": 0,
                        "invalid_arch_count": 0,
                        "invalid_manifest_generation_count": 0,
                        "unsealed_source_set_count": 0,
                        "unsealed_audit_sample_count": 0,
                        "missing_binding_count": 0,
                        "unsealed_layout_count": 0,
                        "invalid_layout_generation_count": 0,
                        "mismatched_binding_count": 0,
                    }
                ]
            assert params == {"stale_activity_seconds": 900}
            return [
                {
                    "active_import_run_count": 0,
                    "building_snapshot_count": 0,
                    "stale_import_run_count": 6,
                    "stale_building_snapshot_count": 6,
                }
            ]

    readiness = asyncio.run(
        module.collect_cutover_readiness(
            Executor(),
            schema_name="mrf",
            stale_activity_seconds=900,
        )
    )

    assert readiness["ready"] is True
    assert readiness["stale_import_run_count"] == 6
    assert readiness["stale_building_snapshot_count"] == 6
    assert readiness["stale_activity_seconds"] == 900


def test_cutover_stale_activity_window_cannot_disable_live_run_guard():
    module = _load_script("ptg2_strict_v3_cutover_ready")

    assert module._stale_activity_seconds(0) == 300


def test_ptg_rebuild_expands_uhc_toc_url_candidates():
    module = _load_script("ptg2_rebuild_snapshot_from_options")

    candidates = module._uhc_toc_url_candidates(
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json?sig=stale"
    )

    assert candidates == [
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json?sig=stale",
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json",
        "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/2026-07-01/example_index.json",
    ]


def test_ptg_rebuild_uses_first_reachable_uhc_toc_candidate(monkeypatch):
    module = _load_script("ptg2_rebuild_snapshot_from_options")
    checked_urls = []

    async def is_toc_head_reachable(url):
        checked_urls.append(url)
        return "blobs/download" in url

    monkeypatch.setattr(module, "_is_toc_head_reachable", is_toc_head_reachable)

    resolved = asyncio.run(
        module._resolve_toc_url(
            "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json?sig=stale",
            refresh_toc_urls=True,
        )
    )

    assert resolved == "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/2026-07-01/example_index.json"
    assert checked_urls == [
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json?sig=stale",
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json",
        "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/2026-07-01/example_index.json",
    ]
