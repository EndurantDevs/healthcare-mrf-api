# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from click.testing import CliRunner

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

import process as process_cli


def _assert_cli_forwards(
    monkeypatch,
    *,
    command,
    target_name: str,
    args: list[str],
    expected: dict,
) -> None:
    """Assert that Click parsing reaches exactly one importer invocation."""

    coroutine_token = object()
    target_calls: list[dict] = []
    run_calls: list[object] = []

    def fake_target(**kwargs):
        target_calls.append(kwargs)
        return coroutine_token

    monkeypatch.setattr(process_cli, target_name, fake_target)
    monkeypatch.setattr(process_cli, "_run", run_calls.append)

    result = CliRunner().invoke(command, args)

    assert result.exit_code == 0, result.output
    assert result.exception is None
    assert target_calls == [expected]
    assert run_calls == [coroutine_token]


def test_provider_adjacent_commands_preserve_scope_and_publish_controls(monkeypatch) -> None:
    _assert_cli_forwards(
        monkeypatch,
        command=process_cli.entity_address_unified,
        target_name="initiate_entity_address_unified",
        args=[
            "--test",
            "--limit-per-source",
            "9",
            "--publish",
            "--refresh-mode",
            "provider-directory-partial",
            "--serving-only-refresh",
            "--provider-directory-run-id",
            "directory-run",
            "--provider-directory-source-id",
            "payer-a",
            "--provider-directory-source-id",
            "payer-b",
            "--provider-directory-partial-scope",
            "latest-run",
            "--provider-directory-source-batch-size",
            "3",
        ],
        expected={
            "test_mode": True,
            "limit_per_source": 9,
            "publish": True,
            "refresh_mode": "provider-directory-partial",
            "serving_only_refresh": True,
            "provider_directory_run_id": "directory-run",
            "provider_directory_source_ids": ["payer-a", "payer-b"],
            "provider_directory_partial_scope": "latest-run",
            "provider_directory_source_batch_size": 3,
        },
    )


def test_ptg_failure_retention_alias_and_filters_reach_importer(monkeypatch) -> None:
    _assert_cli_forwards(
        monkeypatch,
        command=process_cli.ptg,
        target_name="initiate_ptg",
        args=[
            "--test",
            "--toc-url",
            "https://example.test/a.json",
            "--toc-url",
            "https://example.test/b.json",
            "--source-key",
            "payer",
            "--import-month",
            "2026-07",
            "--plan-id",
            "plan-a",
            "--plan-name-contains",
            "employer",
            "--plan-market-type",
            "group",
            "--file-url-contains",
            "in-network",
            "--no-reuse-raw-artifacts",
            "--keep-artifacts-on-failure",
        ],
        expected={
            "test_mode": True,
            "toc_urls": [
                "https://example.test/a.json",
                "https://example.test/b.json",
            ],
            "toc_list": None,
            "in_network_url": None,
            "allowed_url": None,
            "provider_ref_url": None,
            "import_id": None,
            "source_key": "payer",
            "import_month": "2026-07",
            "max_files": None,
            "max_items": None,
            "plan_ids": ["plan-a"],
            "plan_name_contains": ["employer"],
            "plan_market_types": ["group"],
            "file_url_contains": ["in-network"],
            "reuse_raw_artifacts": False,
            "keep_partial_artifacts": True,
        },
    )


def test_reference_commands_preserve_download_and_relationship_controls(monkeypatch) -> None:
    _assert_cli_forwards(
        monkeypatch,
        command=process_cli.ms_drg,
        target_name="initiate_ms_drg",
        args=[
            "--test",
            "--skip-relationships",
            "--relationship-page-limit",
            "4",
            "--concurrency",
            "2",
            "--source-url",
            "https://example.test/drg",
            "--manual-toc-url",
            "https://example.test/manual",
            "--import-id",
            "drg-1",
        ],
        expected={
            "test_mode": True,
            "include_relationships": False,
            "relationship_page_limit": 4,
            "concurrency": 2,
            "source_url": "https://example.test/drg",
            "manual_toc_url": "https://example.test/manual",
            "import_id": "drg-1",
        },
    )
    _assert_cli_forwards(
        monkeypatch,
        command=process_cli.clinical_reference,
        target_name="initiate_clinical_reference",
        args=[
            "--test",
            "--import-id",
            "clinical-1",
            "--sources",
            "mesh,rxnorm",
            "--artifact-root",
            "/tmp/clinical",
            "--force-download",
        ],
        expected={
            "test_mode": True,
            "import_id": "clinical-1",
            "sources": "mesh,rxnorm",
            "artifact_root": "/tmp/clinical",
            "force_download": True,
        },
    )


def test_audit_and_source_discovery_preserve_public_run_controls(monkeypatch) -> None:
    """Verify audit and source discovery preserve public run controls."""
    _assert_cli_forwards(
        monkeypatch,
        command=process_cli.ptg_candidate_audit,
        target_name="initiate_ptg_candidate_audit",
        args=[
            "--candidate-run-id",
            "candidate-1",
            "--snapshot-id",
            "snapshot-1",
            "--import-id",
            "import-1",
        ],
        expected={
            "candidate_run_id": "candidate-1",
            "snapshot_id": "snapshot-1",
            "import_id": "import-1",
        },
    )
    _assert_cli_forwards(
        monkeypatch,
        command=process_cli.mrf_source_discovery_command,
        target_name="initiate_mrf_source_discovery",
        args=[
            "--test",
            "--provider",
            "master-list",
            "--limit",
            "8",
            "--source-entity-types",
            "tpa",
            "--source-payer-query",
            "example",
            "--dry-run",
            "--check-urls",
            "--crawl",
            "--probe-files",
            "--file-probe-limit",
            "0",
            "--file-probe-types",
            "in-network",
            "--file-probe-entity-types",
            "network",
            "--file-probe-payer-query",
            "payer",
            "--max-toc-bytes",
            "0",
            "--concurrency",
            "0",
            "--crawl-target-limit",
            "0",
        ],
        expected={
            "test_mode": True,
            "provider": "master-list",
            "limit": 8,
            "source_entity_types": "tpa",
            "source_payer_query": "example",
            "dry_run": True,
            "check_urls": True,
            "crawl": True,
            "probe_files": True,
            "file_probe_limit": None,
            "file_probe_types": "in-network",
            "file_probe_entity_types": "network",
            "file_probe_payer_query": "payer",
            "max_toc_bytes": None,
            "concurrency": None,
            "crawl_target_limit": None,
        },
    )


def test_address_import_commands_preserve_staging_and_migration_controls(monkeypatch) -> None:
    """Verify address import commands preserve staging and migration controls."""
    _assert_cli_forwards(
        monkeypatch,
        command=process_cli.openaddresses,
        target_name="initiate_openaddresses",
        args=[
            "--test",
            "--backfill-only",
            "--resume-stage",
            "--import-id",
            "oa-1",
            "--local-file",
            "/tmp/a.geojson",
            "--local-file",
            "/tmp/b.geojson.gz",
            "--batch-size",
            "25",
            "--source-concurrency",
            "3",
            "--backfill-match-modes",
            "exact,fuzzy",
            "--zip-restore-shards",
            "6",
        ],
        expected={
            "test_mode": True,
            "backfill_only": True,
            "load_only": False,
            "publish_only": False,
            "resume_stage": True,
            "import_id": "oa-1",
            "local_files": ("/tmp/a.geojson", "/tmp/b.geojson.gz"),
            "batch_size": 25,
            "source_concurrency": 3,
            "max_files": None,
            "start_index": None,
            "end_index": None,
            "start_source": None,
            "min_rows": None,
            "test_file_limit": None,
            "test_row_limit": None,
            "backfill_state_code": None,
            "backfill_zip_prefix": None,
            "backfill_concurrency": None,
            "backfill_zip_prefix_length": None,
            "backfill_match_modes": "exact,fuzzy",
            "zip_restore_concurrency": None,
            "zip_restore_shards": 6,
        },
    )
    _assert_cli_forwards(
        monkeypatch,
        command=process_cli.address_archive_v2_migrate,
        target_name="initiate_address_archive_migration",
        args=[
            "--dry-run",
            "--legacy-table",
            "legacy_addresses",
            "--archive-table",
            "address_archive_v2",
            "--work-mem",
            "1GB",
            "--timeout",
            "5min",
            "--sample-limit",
            "7",
            "--enqueue",
            "--test",
        ],
        expected={
            "dry_run": True,
            "legacy_table": "legacy_addresses",
            "archive_table": "address_archive_v2",
            "work_mem": "1GB",
            "timeout": "5min",
            "sample_limit": 7,
            "enqueue": True,
            "test_mode": True,
        },
    )
