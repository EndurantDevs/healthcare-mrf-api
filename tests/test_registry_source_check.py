# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual bounded HTTP and preparation, with closed control dispatch."""

import asyncio
import hashlib
import importlib.util
import json
import os
import stat
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import process
from api import control_imports, control_workers
from process import registry_source_check as check
from process.registry_source_editions import discover_registry_source_edition
from process.registry_source_fetch import fetch_registry_source
from tests.test_registry_source_editions import _mlr, _plan
from tests.test_registry_source_fetch import _source_http
from tests.test_registry_source_preparation import _mlr_members, _zip

NOW = datetime(2026, 10, 17, tzinfo=timezone.utc)
SCOPE = dict(source_system="cms", source_id="commercial-mlr", edition_policy="latest-verified", reporting_year=None)


def _idempotency_migration():
    """Load the applied source-check index migration without running unrelated revisions."""
    path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20261019150000_registry_source_check_idempotency.py"
    )
    spec = importlib.util.spec_from_file_location("source_check_idempotency_migration", path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_all_status_idempotency_index_matches_model_and_migration(monkeypatch):
    """Keep existing-table migration and fresh-table admission equally durable."""
    from db.models import ImportRun

    migration = _idempotency_migration()
    index = next(spec for spec in ImportRun.__my_additional_indexes__ if spec["name"] == migration.INDEX_NAME)
    assert index == {
        "index_elements": ("importer", "idempotency_key"),
        "name": "import_run_registry_source_check_idempotency_idx",
        "unique": True,
        "where": "importer = 'registry-source-check' AND idempotency_key IS NOT NULL",
    }
    statements = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "source_check_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    assert statements == [
        'DROP INDEX IF EXISTS "source_check_test"."import_run_registry_source_check_idempotency_idx"',
        'CREATE UNIQUE INDEX "import_run_registry_source_check_idempotency_idx" '
        'ON "source_check_test"."import_run" (importer, idempotency_key) WHERE ' + index["where"],
    ]
    statements.clear()
    migration.downgrade()
    assert statements == ['DROP INDEX "source_check_test"."import_run_registry_source_check_idempotency_idx"']


def _config(tmp_path, scope=None, **options):
    return check.RegistrySourceCheckConfig(
        tmp_path, (check.validate_registry_source_check_scope(scope or SCOPE),), **options
    )


def test_cache_creation_is_private_without_changing_existing_directories(tmp_path):
    cache_path = tmp_path / "new-cache"
    config = _config(cache_path)
    with check._cache(config, config.scopes[0]) as selected:
        assert selected.parent == cache_path
        assert stat.S_IMODE(cache_path.stat().st_mode) == 0o700
        assert cache_path.stat().st_uid == os.geteuid()
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o700


@pytest.mark.asyncio
@pytest.mark.parametrize("access_mode", (0o755, 0o777, 0o710, 0o704))
async def test_cache_shared_access_is_rejected_before_lock_or_network(tmp_path, monkeypatch, access_mode):
    cache_path = tmp_path / "shared-cache"
    cache_path.mkdir(mode=access_mode)
    cache_path.chmod(access_mode)
    fetch = AsyncMock(side_effect=AssertionError("Must refuse before network"))
    monkeypatch.setattr(check, "discover_registry_source_edition", fetch)
    with pytest.raises(ValueError, match="registry_source_check_config_invalid"):
        await check.run_registry_source_check(_config(cache_path), SCOPE, now=NOW)
    fetch.assert_not_awaited()
    assert list(cache_path.iterdir()) == []
    assert stat.S_IMODE(cache_path.stat().st_mode) == access_mode


def test_cache_foreign_owner_is_rejected_without_changing_permissions(tmp_path, monkeypatch):
    actual_owner = tmp_path.stat().st_uid
    config = _config(tmp_path)
    monkeypatch.setattr(check.os, "geteuid", lambda: actual_owner + 1)
    with pytest.raises(ValueError, match="registry_source_check_config_invalid"):
        with check._cache(config, config.scopes[0]):
            pytest.fail("Foreign cache must not be entered")
    assert list(tmp_path.iterdir()) == []
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o700


@asynccontextmanager
async def _http(monkeypatch, catalog=None, archive=None):
    async with (
        _source_http((catalog or _mlr()).encode()) as (catalog_state, catalog_transport),
        _source_http(archive or _zip(_mlr_members())) as (artifact_state, artifact_transport),
    ):

        async def discover(*args, **kwargs):
            return await discover_registry_source_edition(*args, **kwargs, _test_transport=catalog_transport)

        async def fetch(*args, **kwargs):
            return await fetch_registry_source(*args, **kwargs, _test_transport=artifact_transport)

        monkeypatch.setattr(check, "discover_registry_source_edition", discover)
        monkeypatch.setattr(check, "fetch_registry_source", fetch)
        yield catalog_state, artifact_state


@pytest.mark.asyncio
async def test_real_http_prepare_window_replay_and_unavailable_preserve_candidate(tmp_path, monkeypatch):
    config = _config(tmp_path)
    async with _http(monkeypatch) as (catalog, artifact):
        first = await check.run_registry_source_check(config, SCOPE, now=NOW)
        assert first["state"] == "prepared" and first["candidate"]["parser_version"] == "cms-header-v1"
        assert first["candidate"]["artifact_sha256"] == hashlib.sha256(artifact.body).hexdigest()
        assert "source_url" not in json.dumps(first) and str(tmp_path) not in json.dumps(first)
        saved = next(tmp_path.rglob("prepared.json"))
        previous = saved.read_bytes()
        second = await check.run_registry_source_check(config, SCOPE, now=NOW)
        assert second["state"] == "already_checked" and second["candidate"] == first["candidate"]
        assert len(catalog.requests) == len(artifact.requests) == 1
        catalog.status = 503
        unavailable = await check.run_registry_source_check(config, SCOPE, now=NOW.replace(month=11))
        assert unavailable["state"] == "unavailable" and saved.read_bytes() == previous
        assert not list(tmp_path.rglob(".registry-*"))


@pytest.mark.asyncio
async def test_tampered_prepared_cache_cannot_count_as_checked(tmp_path, monkeypatch):
    config = _config(tmp_path)
    async with _http(monkeypatch):
        await check.run_registry_source_check(config, SCOPE, now=NOW)
        next(tmp_path.rglob("*.csv")).write_bytes(b"changed")
        with pytest.raises(ValueError):
            await check.run_registry_source_check(config, SCOPE, now=NOW)


@pytest.mark.asyncio
async def test_unknown_planfinder_layout_requires_review(tmp_path, monkeypatch):
    scope_by_name = {**SCOPE, "source_id": "plan-finder"}
    archive = _zip([("2026-09-01-HIOS.xlsx", b"unsupported")])
    async with _http(monkeypatch, _plan(quarter=4, release="(FINAL)"), archive):
        outcome = await check.run_registry_source_check(_config(tmp_path, scope_by_name), scope_by_name, now=NOW)
        assert outcome["state"] == "review_required" and not list(tmp_path.rglob("prepared.json"))


@pytest.mark.asyncio
async def test_verified_naic_layouts_prepare_only_pinned_edition(tmp_path, monkeypatch):
    from process.registry_naic_source import MEMBERS

    pdfs_by_member = {name: b"%PDF-1.4\nsynthetic layout " + name.encode() for name in MEMBERS}
    archive = _zip(
        [(name, b"synthetic listing") for name in MEMBERS]
        + [(name.replace(".csv", ".pdf"), body) for name, body in pdfs_by_member.items()]
    )
    scope_by_name = dict(
        source_system="naic", source_id="company-listings", edition_policy="fixed-year", reporting_year=2026
    )
    naic_by_field = dict(
        edition_id="verified-2026",
        source_url="https://content.naic.org/synthetic-company-listings.zip",
        artifact_sha256=hashlib.sha256(archive).hexdigest(),
        layout_sha256={name: hashlib.sha256(body).hexdigest() for name, body in pdfs_by_member.items()},
    )
    async with _http(monkeypatch, archive=archive):
        first = await check.run_registry_source_check(
            _config(tmp_path, scope_by_name, naic=naic_by_field), scope_by_name, now=NOW
        )
        assert first["state"] == "prepared" and len(first["candidate"]["members"]) == 3
        assert first["candidate"]["edition_id"] == "verified-2026"
        assert (
            await check.run_registry_source_check(
                _config(tmp_path, scope_by_name, naic=naic_by_field), scope_by_name, now=NOW
            )
        )["state"] == "already_checked"
        changed_by_field = {**naic_by_field, "layout_sha256": {**naic_by_field["layout_sha256"], "HLTH.csv": "a" * 64}}
        prior = next(tmp_path.rglob("prepared.json")).read_bytes()
        changed = await check.run_registry_source_check(
            _config(tmp_path, scope_by_name, naic=changed_by_field), scope_by_name, now=NOW
        )
        assert changed["state"] == "review_required"
        assert next(tmp_path.rglob("prepared.json")).read_bytes() == prior


@pytest.mark.parametrize("damage", ["url", "path", "role", "dsn", "year", "source", "missing", "clock"])
def test_closed_logical_scope(damage):
    document_by_name = dict(SCOPE)
    if damage in {"url", "path", "role", "dsn", "clock"}:
        document_by_name[damage] = "synthetic"
    elif damage == "year":
        document_by_name["reporting_year"] = True
    elif damage == "source":
        document_by_name["source_id"] = "unsupported"
    else:
        document_by_name.pop("reporting_year")
    with pytest.raises(ValueError):
        check.validate_registry_source_check_scope(document_by_name)


def test_actual_catalog_queue_worker_and_payload_are_connected():
    item = next(item for item in control_imports.importer_registry() if item["name"] == "registry-source-check")
    assert item["schedulable"] and item["cancelable"] and item["function"] == "control_single_job_start"
    assert {field["name"] for field in item["params_schema"]} == check.SCOPE_FIELDS
    spec = control_workers._BY_QUEUE["arq:RegistrySourceCheck"]
    assert spec.worker_class == "process.RegistrySourceCheck" and spec.importers == ("registry-source-check",)
    assert process.RegistrySourceCheck.functions == [process.control_single_job_start]
    assert process.RegistrySourceCheck.max_jobs == process.RegistrySourceCheck.max_tries == 1
    payload = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS["registry-source-check"],
        {"run_id": "synthetic", "importer": "registry-source-check"},
        SCOPE,
    )
    assert payload["task"] == SCOPE and payload["target_module"] == "process.registry_source_check"
    assert payload["target_function"] == "control_start"


@pytest.mark.asyncio
async def test_actual_enqueue_uses_registered_adapter(monkeypatch):
    redis = SimpleNamespace(enqueue_job=AsyncMock(return_value=SimpleNamespace(job_id="synthetic")))
    monkeypatch.setattr(control_imports, "create_pool", AsyncMock(return_value=redis))
    await control_imports._enqueue_import_start(
        {"run_id": "synthetic", "importer": "registry-source-check", "params": SCOPE}
    )
    args, options = redis.enqueue_job.await_args
    assert args[0] == "control_single_job_start" and args[1]["task"] == SCOPE
    assert options["_queue_name"] == "arq:RegistrySourceCheck"


@pytest.mark.asyncio
async def test_cache_is_locked_without_wait_and_bounded_before_fetch(tmp_path, monkeypatch):
    config = _config(tmp_path)
    fetch = AsyncMock(side_effect=AssertionError("Must refuse before network"))
    monkeypatch.setattr(check, "discover_registry_source_edition", fetch)
    with check._cache(config, config.scopes[0]):
        assert (await check.run_registry_source_check(config, SCOPE, now=NOW))["state"] == "busy"
    small = _config(tmp_path, max_cache_bytes=32 * 1024**2)
    with pytest.raises(RuntimeError, match="cache_full"):
        await check.run_registry_source_check(small, SCOPE, now=NOW)
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_known_planfinder_workbook_is_prepared(tmp_path, monkeypatch):
    from tests.test_cms_planfinder_workbook_input import _workbook

    workbook, _ = _workbook(tmp_path)
    archive = _zip([("2026-08-06-HIOS.xlsx", workbook.read_bytes()), ("RBIS.xlsx", b"never parsed")])
    scope_by_name = {**SCOPE, "source_id": "plan-finder"}
    async with _http(monkeypatch, _plan(quarter=4, release="(FINAL)"), archive):
        result = await check.run_registry_source_check(
            _config(tmp_path / "cache", scope_by_name), scope_by_name, now=NOW
        )
        assert result["state"] == "prepared"
        assert result["candidate"]["parser_version"] == "cms-planfinder-hios-2026-08-06-v2"
        assert result["candidate"]["member_name"] == "2026-08-06-HIOS.xlsx"


@pytest.mark.asyncio
async def test_cancellation_drains_real_preparation_before_cache_unlock(tmp_path, monkeypatch):
    import threading

    config = _config(tmp_path)
    entered = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original = check._prepare

    def held(*args):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(check, "_prepare", held)
    async with _http(monkeypatch):
        work = asyncio.create_task(check.run_registry_source_check(config, SCOPE, now=NOW))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            work.cancel()
            await asyncio.sleep(0)
            assert not work.done()
            assert (await check.run_registry_source_check(config, SCOPE, now=NOW))["state"] == "busy"
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await work
        assert not list(tmp_path.rglob("prepared.json"))


@pytest.mark.asyncio
async def test_actual_control_target_calls_real_check_with_operator_scope(tmp_path, monkeypatch):
    document_by_name = dict(cache_dir=str(tmp_path), scopes=[SCOPE])
    monkeypatch.setenv(check.CONFIG_ENV, json.dumps(document_by_name))
    async with _http(monkeypatch):
        result = await check.control_start({}, SCOPE | {"run_id": "synthetic-control"})
        assert result["state"] == "prepared"


@pytest.mark.asyncio
async def test_forbidden_control_override_refuses_before_queue(monkeypatch):
    pool = AsyncMock(side_effect=AssertionError("Queue must remain untouched"))
    monkeypatch.setattr(control_imports, "create_pool", pool)
    params_by_name = SCOPE | {"source_url": "https://example.invalid"}
    with pytest.raises(ValueError):
        await control_imports.create_import_run({"importer": "registry-source-check", "params": params_by_name})
    result = await control_imports._enqueue_import_start(
        {"run_id": "synthetic", "importer": "registry-source-check", "params": params_by_name}
    )
    assert result["status"] == "failed" and result["error"]["code"] == "invalid_enqueue_adapter"
    pool.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["succeeded", "failed", "canceled", "dead_letter"])
async def test_terminal_check_replay_keeps_exact_scope_without_new_queue_work(monkeypatch, status):
    existing_run_dict = dict(
        run_id="completed-source-check", importer="registry-source-check", status=status, params=SCOPE
    )
    find = AsyncMock(return_value=existing_run_dict)
    active = AsyncMock(side_effect=AssertionError("Terminal check replay must use retained identity"))
    monkeypatch.setattr(control_imports, "find_importer_run_by_idempotency_key", find)
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", active)
    assert (
        await control_imports._idempotent_import_run("registry-source-check", "same-occurrence", params=SCOPE)
        == existing_run_dict
    )
    with pytest.raises(ValueError, match="registry_source_check_scope_mismatch"):
        await control_imports._idempotent_import_run(
            "registry-source-check", "same-occurrence", params=SCOPE | {"source_id": "plan-finder"}
        )
    active.assert_not_awaited()


@pytest.mark.asyncio
async def test_completed_check_api_replay_never_creates_or_enqueues_again(monkeypatch):
    existing_run_dict = dict(
        run_id="completed-source-check", importer="registry-source-check", status="succeeded", params=SCOPE
    )
    monkeypatch.setattr(
        control_imports, "find_importer_run_by_idempotency_key", AsyncMock(return_value=existing_run_dict)
    )
    enqueue = AsyncMock(side_effect=AssertionError("Completed check must not enqueue again"))
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    result, created = await control_imports.create_import_run(
        dict(importer="registry-source-check", idempotency_key="same-occurrence", params=SCOPE)
    )
    assert not created and result["run_id"] == existing_run_dict["run_id"]
    enqueue.assert_not_awaited()


@pytest.mark.parametrize(
    "damage", ["relative", "duplicate", "unknown", "url_override", "oversized", "empty", "duplicate_json"]
)
def test_operator_configuration_is_closed(tmp_path, damage):
    configured_by_name = dict(cache_dir=str(tmp_path), scopes=[SCOPE])
    if damage == "relative":
        configured_by_name["cache_dir"] = "relative"
    if damage == "duplicate":
        configured_by_name["scopes"] = [SCOPE, SCOPE]
    if damage == "unknown":
        configured_by_name["dsn"] = "synthetic"
    if damage == "url_override":
        configured_by_name["scopes"] = [SCOPE | {"url": "https://example.invalid"}]
    if damage == "empty":
        configured_by_name["scopes"] = []
    encoded = json.dumps(configured_by_name)
    if damage == "oversized":
        encoded = " " * 16385
    if damage == "duplicate_json":
        encoded = '{"scopes":[],"scopes":[]}'
    with pytest.raises(ValueError):
        check.parse_registry_source_check_config(encoded)


@pytest.mark.asyncio
async def test_cached_window_requires_matching_checked_clock(tmp_path, monkeypatch):
    async with _http(monkeypatch):
        await check.run_registry_source_check(_config(tmp_path), SCOPE, now=NOW)
        saved = next(tmp_path.rglob("prepared.json"))
        checkpoint_by_name = json.loads(saved.read_text())
        checkpoint_by_name["checked_at"] = NOW.replace(month=9).isoformat()
        saved.write_text(json.dumps(checkpoint_by_name))
        with pytest.raises(ValueError):
            await check.run_registry_source_check(_config(tmp_path), SCOPE, now=NOW)


def test_cli_is_registered_and_does_not_echo_forbidden_arguments(tmp_path, monkeypatch):
    from click.testing import CliRunner

    assert process.process_group.commands["registry-source-check"] is check.registry_source_check_command
    runner = CliRunner()
    result = runner.invoke(check.registry_source_check_command, ["synthetic-private-value"])
    assert result.exit_code == 2 and "synthetic-private-value" not in result.output
    monkeypatch.setenv(check.CONFIG_ENV, json.dumps(dict(cache_dir=str(tmp_path), scopes=[SCOPE])))
    run = AsyncMock(return_value={"state": "unavailable", "scope": SCOPE})
    monkeypatch.setattr(check, "run_registry_source_check", run)
    result = runner.invoke(check.registry_source_check_command, [])
    assert result.exit_code == 0 and json.loads(result.output) == [{"state": "unavailable", "scope": SCOPE}]
    assert run.await_args.args[1] == SCOPE and str(tmp_path) not in result.output


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["catalog", "artifact", "candidate", "scope", "oversized", "missing artifact", "published at"]
)
async def test_retained_candidate_requires_exact_original_identity(tmp_path, monkeypatch, damage):
    config = _config(tmp_path)
    async with _http(monkeypatch) as (catalog, artifact):
        await check.run_registry_source_check(config, SCOPE, now=NOW)
        saved = next(tmp_path.rglob("prepared.json"))
        checkpoint_by_name = json.loads(saved.read_text())
        if damage in {"catalog", "artifact"}:
            checkpoint_by_name[damage]["spec"]["parser_version"] = "foreign-parser"
        if damage == "published at":
            checkpoint_by_name["artifact"]["spec"]["published_at"] = NOW.isoformat()
        if damage == "candidate":
            checkpoint_by_name["candidate"]["edition_id"] = "foreign-edition"
        if damage == "scope":
            checkpoint_by_name["scope"]["source_id"] = "plan-finder"
        saved.write_text(" " * 65537 if damage == "oversized" else json.dumps(checkpoint_by_name))
        if damage == "missing artifact":
            (saved.parent / (checkpoint_by_name["artifact"]["artifact_sha256"] + ".artifact")).unlink()
        prior = saved.read_bytes()
        with pytest.raises(ValueError):
            await check.run_registry_source_check(config, SCOPE, now=NOW)
        assert saved.read_bytes() == prior
        assert len(catalog.requests) == len(artifact.requests) == 1
        assert not list(tmp_path.rglob(".prepared.tmp"))


@pytest.mark.asyncio
async def test_retained_artifact_cannot_change_source_producer(tmp_path, monkeypatch):
    from process.registry_naic_source import MEMBERS

    async with _http(monkeypatch):
        await check.run_registry_source_check(_config(tmp_path), SCOPE, now=NOW)
        saved = next(tmp_path.rglob("prepared.json"))
        checkpoint_by_name = json.loads(saved.read_text())
        scope = check.validate_registry_source_check_scope(
            dict(source_system="naic", source_id="company-listings", edition_policy="fixed-year", reporting_year=2026)
        )
        naic_by_field = dict(
            edition_id="verified-2026",
            source_url="https://content.naic.org/synthetic-company-listings.zip",
            artifact_sha256="a" * 64,
            layout_sha256={name: "b" * 64 for name in MEMBERS},
        )
        config = check.RegistrySourceCheckConfig(tmp_path, (scope,), naic=naic_by_field)
        before = saved.read_bytes()
        with pytest.raises(ValueError, match="registry_source_check_config_invalid"):
            await check._retained(config, scope, saved.parent, checkpoint_by_name)
        assert saved.read_bytes() == before


@pytest.mark.asyncio
async def test_changed_edition_does_not_reuse_foreign_artifact_receipt(tmp_path, monkeypatch):
    async with _http(monkeypatch) as (catalog, _artifact):
        config = _config(tmp_path)
        await check.run_registry_source_check(config, SCOPE, now=NOW)
        catalog.body = _mlr(2025).encode()
        original_fetch = check.fetch_registry_source
        previous_receipts = []

        async def fetch(*args, **kwargs):
            previous_receipts.append(kwargs["previous"])
            return await original_fetch(*args, **kwargs)

        monkeypatch.setattr(check, "fetch_registry_source", fetch)
        result = await check.run_registry_source_check(config, SCOPE, now=NOW.replace(month=11))
        assert result["state"] in {"prepared", "review_required"}
        assert previous_receipts == [None]


@pytest.mark.parametrize(
    "changes",
    [
        {"scopes": ()},
        {"scopes": (object(),)},
        {"max_cache_files": True},
        {"max_cache_files": 31},
        {"max_cache_bytes": True},
        {"scopes": (check.validate_registry_source_check_scope(SCOPE),) * 2},
        {"scopes": (replace(check.validate_registry_source_check_scope(SCOPE), available=False),)},
        {"scopes": (replace(check.validate_registry_source_check_scope(SCOPE), last_checked_at=NOW),)},
    ],
)
def test_direct_configuration_enforces_typed_bounded_scopes(tmp_path, changes):
    with pytest.raises(ValueError):
        replace(_config(tmp_path), **changes)


@pytest.mark.parametrize("encoded", ['{"cache_dir":false,"scopes":[]}', '{"cache_dir":"/unused","scopes":{}}'])
def test_operator_document_requires_text_path_and_scope_list(encoded):
    with pytest.raises(ValueError, match="registry_source_check_config_invalid"):
        check.parse_registry_source_check_config(encoded)


@pytest.mark.parametrize("year", [2009, 2101])
def test_logical_scope_refuses_unsupported_reporting_year(year):
    with pytest.raises(ValueError, match="registry_source_check_config_invalid"):
        check.validate_registry_source_check_scope({**SCOPE, "reporting_year": year})


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["symlink", "file count"])
async def test_cache_refuses_unsafe_or_exhausted_inventory(tmp_path, monkeypatch, damage):
    config = _config(tmp_path, max_cache_files=32)
    if damage == "symlink":
        (tmp_path / "foreign").symlink_to(tmp_path)
    else:
        for index in range(32):
            (tmp_path / str(index)).touch()
    discovery = AsyncMock(side_effect=AssertionError("unexpected discovery"))
    monkeypatch.setattr(check, "discover_registry_source_edition", discovery)
    with pytest.raises(ValueError if damage == "symlink" else RuntimeError):
        await check.run_registry_source_check(config, SCOPE, now=NOW)
    discovery.assert_not_awaited()


@pytest.mark.asyncio
async def test_unconfigured_naic_requires_review_without_fetch(tmp_path, monkeypatch):
    scope_by_name = dict(
        source_system="naic", source_id="company-listings", edition_policy="fixed-year", reporting_year=2026
    )
    fetch = AsyncMock(side_effect=AssertionError("unexpected artifact fetch"))
    monkeypatch.setattr(check, "fetch_registry_source", fetch)
    result = await check.run_registry_source_check(_config(tmp_path, scope_by_name), scope_by_name, now=NOW)
    assert result["state"] == "review_required" and result["scope"] == scope_by_name
    fetch.assert_not_awaited()
    assert not list(tmp_path.rglob("prepared.json"))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["config", "scope", "not due"])
async def test_refusal_and_not_due_leave_cache_uncreated(tmp_path, damage):
    cache = tmp_path / "uncreated"
    config = _config(cache)
    if damage == "not due":
        result = await check.run_registry_source_check(config, SCOPE, now=datetime(1, 1, 1, tzinfo=timezone.utc))
        assert result == {"state": "not_due", "scope": SCOPE}
    else:
        with pytest.raises(ValueError):
            await check.run_registry_source_check(
                object() if damage == "config" else config,
                SCOPE if damage == "config" else {**SCOPE, "source_id": "plan-finder"},
                now=NOW,
            )
    assert not cache.exists()


def test_naic_layout_digests_and_serialization_are_closed(tmp_path):
    from process.registry_naic_source import MEMBERS

    naic_by_field = dict(
        edition_id="verified-2026",
        source_url="https://content.naic.org/synthetic.zip",
        artifact_sha256="A" * 64,
        layout_sha256={name: "b" * 64 for name in MEMBERS},
    )
    with pytest.raises(ValueError, match="registry_source_check_config_invalid"):
        _config(tmp_path, naic=naic_by_field)
    with pytest.raises(TypeError):
        check._json_default(object())


@pytest.mark.asyncio
async def test_control_rejects_invalid_run_before_configuration(monkeypatch):
    parse = AsyncMock(side_effect=AssertionError("unexpected configuration read"))
    monkeypatch.setattr(check, "parse_registry_source_check_config", parse)
    with pytest.raises(ValueError, match="registry_source_check_config_invalid"):
        await check.control_start({}, {**SCOPE, "run_id": " invalid "})
    parse.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["cancel", "prepare"])
async def test_control_failure_drains_preparation_before_unlock(tmp_path, monkeypatch, failure):
    import threading

    from process import control_cancel, registry_cms_source_worker

    monkeypatch.setenv(check.CONFIG_ENV, json.dumps(dict(cache_dir=str(tmp_path), scopes=[SCOPE])))
    monkeypatch.setattr(control_cancel, "raise_if_cancelled", AsyncMock())
    entered, release = asyncio.Event(), threading.Event()
    loop, original = asyncio.get_running_loop(), check._prepare

    def held(*args):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        if failure == "prepare":
            raise RuntimeError("sensitive preparation failure")
        return original(*args)

    async def watch(ctx, task):
        await entered.wait()
        if failure == "cancel":
            raise control_cancel.ImportCancelledError("synthetic cancellation")
        await asyncio.Future()

    monkeypatch.setattr(check, "_prepare", held)
    monkeypatch.setattr(registry_cms_source_worker, "_watch_control_cancel", watch)
    async with _http(monkeypatch):
        work = asyncio.create_task(check.control_start({}, {**SCOPE, "run_id": "synthetic-control"}))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            await asyncio.sleep(0)
            assert not work.done()
            assert (await check.run_registry_source_check(_config(tmp_path), SCOPE, now=NOW))["state"] == "busy"
        finally:
            release.set()
        with pytest.raises(control_cancel.ImportCancelledError if failure == "cancel" else RuntimeError) as caught:
            await work
        if failure == "prepare":
            assert str(caught.value) == "registry_source_check_failed" and caught.value.__cause__ is None
        assert not list(tmp_path.rglob("prepared.json"))
        with check._cache(_config(tmp_path), _config(tmp_path).scopes[0]) as selected:
            assert selected is not None
