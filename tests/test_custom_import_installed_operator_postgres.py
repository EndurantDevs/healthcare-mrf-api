# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in native PostgreSQL proof through installed operator entry scripts."""

from __future__ import annotations

import asyncio
import compileall
import json
import os
import shlex
import shutil
import sys
import warnings
from contextlib import suppress
from pathlib import Path

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.engine import make_url

from db.models.custom_import import (
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportCurrentGeneration,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportPublicationEvent,
    CustomImportSourceBindingRevision,
)
from process.custom_import.capture import capture_stream
from process.custom_import.capture_store import register_replayable_parquet_bundle
from process.custom_import.definition import canonical_json
from process.custom_import.execution import (
    bind_execution_capture_bundle,
    claim_execution,
    finish_execution,
    request_cancellation,
    reserve_execution,
)
from process.custom_import.snowflake import SnowflakeResultColumn
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleAcquisition,
    SnowflakeBundleStatementBuilder,
    SnowflakeBundleStreamCapture,
    replayable_parquet_captures,
)
from process.custom_import.snowflake_candidate import bundle_request_identity_sha256
from process.custom_import.snowflake_python import _parquet_reader
from process.custom_import.snowflake_source_binding import load_snowflake_source_binding
from tests.custom_import_postgres_support import POSTGRES_DSN_ENV, isolated_publication_case, seed_publication_graph
from tests.test_custom_import_snowflake_operator_cli import _loaded_binding, _registration_document

_ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def installed_application(tmp_path_factory):
    if not os.getenv(POSTGRES_DSN_ENV):
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    assert sys.version_info[:2] == (3, 14)
    assert make_url(os.environ[POSTGRES_DSN_ENV]).host in {"127.0.0.1", "localhost", "::1"}
    temporary = tmp_path_factory.mktemp("installed-custom-import")
    try:
        installed = temporary / "opt"
        outside = temporary / "outside"
        outside.mkdir()
        # Mirror literal application COPY destinations; omit builder artifacts.
        for line in (_ROOT / "Dockerfile").read_text().splitlines():
            if not line.startswith("COPY ") or line.startswith("COPY --"):
                continue
            parts = shlex.split(line)
            *copy_sources, destination = parts[1:]
            if not destination.startswith("/opt/"):
                continue
            copy_destination = installed / destination.removeprefix("/opt/")
            for source_path in copy_sources:
                original = _ROOT / source_path
                if original.is_dir():
                    shutil.copytree(original, copy_destination, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
                else:
                    copied = copy_destination / original.name if destination.endswith("/") else copy_destination
                    copied.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(original, copied)
        for name in ("custom_import_cli.py", "custom_import_snowflake_operator.py"):
            assert (installed / name).read_bytes() == (_ROOT / name).read_bytes()
        assert not (installed / "tests").exists()
        # Default bytecode destinations stay beside sources in this owned tree.
        assert sys.pycache_prefix is None
        with warnings.catch_warnings(action="error"):
            assert compileall.compile_dir(installed / "process", quiet=1, workers=1, limit_sl_dest=installed)
        yield installed, outside
    finally:
        shutil.rmtree(temporary)
        assert not temporary.exists()


async def _communicate_with_cleanup(process, stdin_payload):
    try:
        return await asyncio.wait_for(process.communicate(stdin_payload), timeout=30)
    except BaseException:
        # Cleanup failures must not replace the original error or cancellation.
        if process.returncode is None:
            with suppress(BaseException):
                process.kill()
        with suppress(BaseException):
            await asyncio.wait_for(process.communicate(), timeout=5)
        raise


async def _command(installed_application, case, *arguments, snowflake=False, stdin_payload=None, error=None):
    installed, outside = installed_application
    url = case.engine.url
    assert url.host in {"127.0.0.1", "localhost", "::1"}
    environment_by_key = {
        "PATH": os.defpath,
        "PYTHONWARNINGS": "error",
        "PYTHONDONTWRITEBYTECODE": "1",
        "HLTHPRT_LOG_CFG": str(installed / "logging.yaml"),
        "HLTHPRT_DB_DRIVER": "asyncpg",
        "HLTHPRT_DB_HOST": url.host,
        "HLTHPRT_DB_PORT": str(url.port or 5432),
        "HLTHPRT_DB_USER": url.username,
        "HLTHPRT_DB_PASSWORD": url.password or "",
        "HLTHPRT_DB_DATABASE": url.database,
        "HLTHPRT_DB_SCHEMA": case.schema_name,
        "DB_SCHEMA": case.schema_name,
        "HLTHPRT_DB_POOL_MIN_SIZE": "1",
        "HLTHPRT_DB_POOL_MAX_SIZE": "1",
        "HLTHPRT_DB_ECHO": "False",
        "HLTHPRT_REDIS_ADDRESS": "redis://127.0.0.1:6379",
        "HLTHPRT_PTG2_SOURCE_IMPORT_LOCK_ENABLED": "false",
    }
    entry = "custom_import_snowflake_operator.py" if snowflake else "custom_import_cli.py"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(installed / entry),
        *(str(argument) for argument in arguments),
        cwd=outside,
        env=environment_by_key,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await _communicate_with_cleanup(process, stdin_payload)
    if error is not None:
        assert process.returncode == 1
        assert stdout == b""
        assert stderr == (canonical_json({"code": error, "status": "error"}) + "\n").encode()
        return None
    assert process.returncode == 0
    assert stderr == b""
    assert len(stdout.splitlines()) == 1
    return json.loads(stdout)


async def _register(installed, case):
    return await _command(
        installed, case, "register", snowflake=True, stdin_payload=canonical_json(_registration_document()).encode()
    )


async def test_installed_registration_commits_and_replays_exact_revisions(installed_application):
    async with isolated_publication_case() as case:
        first = await _register(installed_application, case)
        replay = await _register(installed_application, case)
        assert first.pop("status") == "registered"
        assert replay.pop("status") == "replayed"
        assert first == replay
        async with case.sessions() as session:
            loaded = await load_snowflake_source_binding(
                session,
                definition_revision_id=first["definition_revision_id"],
                source_binding_revision_id=first["source_binding_revision_id"],
            )
            assert loaded.definition == _loaded_binding().definition
            assert loaded.binding == _loaded_binding().binding
            assert loaded.source_binding_sha256.hex() == first["source_binding_sha256"]
            for model in (CustomImportDataset, CustomImportDefinitionRevision, CustomImportSourceBindingRevision):
                assert await session.scalar(select(func.count()).select_from(model)) == 1


async def _publication_state(case, dataset_id):
    async with case.sessions() as session:
        pointer = await session.get(CustomImportCurrentGeneration, dataset_id)
        events = tuple(
            (
                await session.execute(
                    select(
                        CustomImportPublicationEvent.publication_event_id,
                        CustomImportPublicationEvent.committed_pointer_version,
                    )
                    .where(CustomImportPublicationEvent.dataset_id == dataset_id)
                    .order_by(
                        CustomImportPublicationEvent.publication_event_id,
                    )
                )
            ).all()
        )
        return (None if pointer is None else (pointer.generation_id, pointer.pointer_version)), events


def _publication_arguments(command, graph, target, version, previous=None):
    arguments = [
        command,
        "--dataset-id",
        graph.dataset_id,
        "--target-generation-id",
        target,
        "--expected-pointer-version",
        version,
    ]
    if previous is not None:
        arguments.extend(["--expected-generation-id", previous])
    return arguments


async def _assert_installed_status_and_capture(installed_application, case, graph):
    """Verify sealed status and retained capture digest through the installed entries."""

    status = await _command(
        installed_application,
        case,
        "status",
        "--dataset-id",
        graph.dataset_id,
        "--generation-id",
        graph.first_generation_id,
    )
    assert status["publication_state"] == "sealed_unpublished"
    captures = await _command(
        installed_application,
        case,
        "captures",
        "--dataset-id",
        graph.dataset_id,
        "--execution-id",
        graph.first_execution_id,
    )
    async with case.sessions() as session:
        bundle = await session.get(CustomImportCaptureBundle, graph.capture_bundle_id)
        assert captures["capture"] == {
            "bundle_id": graph.capture_bundle_id,
            "manifest_sha256": bundle.manifest_sha256.hex(),
        }


async def test_installed_publication_status_capture_replays_and_stale_cas(installed_application):
    """Verify exact publication replay, stale CAS denial and installed lifecycle receipts."""

    async with isolated_publication_case() as case:
        async with case.sessions() as session, session.begin():
            graph = await seed_publication_graph(session)
        await _assert_installed_status_and_capture(installed_application, case, graph)
        first_arguments = _publication_arguments("activate", graph, graph.first_generation_id, 0)
        first = await _command(installed_application, case, *first_arguments)
        assert first["committed_pointer_version"] == 1 and first["replayed"] is False
        first_state = await _publication_state(case, graph.dataset_id)
        replay = await _command(installed_application, case, *first_arguments)
        assert replay["publication_event_id"] == first["publication_event_id"] and replay["replayed"] is True
        assert await _publication_state(case, graph.dataset_id) == first_state
        await _command(
            installed_application,
            case,
            *_publication_arguments("activate", graph, graph.second_generation_id, 0),
            error="conflict",
        )
        assert await _publication_state(case, graph.dataset_id) == first_state
        second = await _command(
            installed_application,
            case,
            *_publication_arguments("activate", graph, graph.second_generation_id, 1, graph.first_generation_id),
        )
        assert second["committed_pointer_version"] == 2
        rollback_arguments = _publication_arguments(
            "rollback", graph, graph.first_generation_id, 2, graph.second_generation_id
        )
        rollback = await _command(installed_application, case, *rollback_arguments)
        assert rollback["committed_pointer_version"] == 3
        final_state = await _publication_state(case, graph.dataset_id)
        assert final_state[0] == (graph.first_generation_id, 3)
        assert len(final_state[1]) == 3
        replay = await _command(installed_application, case, *rollback_arguments)
        assert replay["publication_event_id"] == rollback["publication_event_id"] and replay["replayed"] is True
        await _command(
            installed_application,
            case,
            *_publication_arguments("rollback", graph, graph.second_generation_id, 2, graph.first_generation_id),
            error="conflict",
        )
        assert await _publication_state(case, graph.dataset_id) == final_state
        status = await _command(
            installed_application,
            case,
            "status",
            "--dataset-id",
            graph.dataset_id,
            "--execution-id",
            graph.first_execution_id,
        )
        assert status["state"] == "completed" and status["capture_bundle_id"] == graph.capture_bundle_id


async def _retained_request_acquisition(installed, case):
    """Register the binding and prepare its matching synthetic sealed capture."""

    registered = await _register(installed, case)
    async with case.sessions() as session:
        loaded = await load_snowflake_source_binding(
            session,
            definition_revision_id=registered["definition_revision_id"],
            source_binding_revision_id=registered["source_binding_revision_id"],
        )
    builder = SnowflakeBundleStatementBuilder(approved_relations=loaded.approved_relations)
    request = builder.prepare_request(loaded.definition, bindings=loaded.bundle_bindings)
    statement = builder.build_statement(request)
    identity = bundle_request_identity_sha256(request, statement, source_binding_sha256=loaded.source_binding_sha256)
    schema = (SnowflakeResultColumn(field_id="npi", source_type="TEXT", nullable=False),)
    snapshot = "synthetic-retained-snapshot"
    capture = capture_stream(
        _parquet_reader((("1234567893",),), schema),
        loaded.definition.source_streams[0],
        source_snapshot_token=snapshot,
        limits=request.capture_limits,
    )
    acquisition = SnowflakeBundleAcquisition(
        statement=statement,
        source_snapshot_token=snapshot,
        stream_captures=(SnowflakeBundleStreamCapture(stream_id="root_source", schema=schema, captures=(capture,)),),
        capture_limits=request.capture_limits,
    )
    return loaded, identity, acquisition


async def _seed_retained_request(installed, case, *, mode="expired"):
    """Commit one canonical request with the requested lease and retained-capture state."""

    loaded, identity, acquisition = await _retained_request_acquisition(installed, case)
    key, lease_token = "synthetic-retained-request", "synthetic-retained-owner"
    async with case.sessions() as session, session.begin():
        submission = await reserve_execution(
            session,
            dataset_id=loaded.dataset_id,
            definition_revision_id=loaded.definition_revision_id,
            schema_revision_id=loaded.schema_revision_id,
            idempotency_key=key,
            mechanism="local",
            request_identity_sha256=identity,
            source_binding_revision_id=loaded.source_binding_revision_id,
        )
        if mode != "queued":
            grant = await claim_execution(session, execution_id=submission.execution_id, token=lease_token)
            assert grant is not None
            if mode != "unbound":
                bundle = await register_replayable_parquet_bundle(
                    session,
                    dataset_id=loaded.dataset_id,
                    definition_revision_id=loaded.definition_revision_id,
                    schema_revision_id=loaded.schema_revision_id,
                    captures=replayable_parquet_captures(acquisition),
                )
                assert (
                    await bind_execution_capture_bundle(
                        session,
                        execution_id=submission.execution_id,
                        dataset_id=loaded.dataset_id,
                        definition_revision_id=loaded.definition_revision_id,
                        schema_revision_id=loaded.schema_revision_id,
                        capture_bundle_id=bundle.capture_bundle_id,
                        fence=grant.fence,
                        token=lease_token,
                    )
                    is not None
                )
            if mode == "terminal":
                await request_cancellation(session, execution_id=submission.execution_id)
                assert (
                    await finish_execution(
                        session,
                        execution_id=submission.execution_id,
                        fence=grant.fence,
                        token=lease_token,
                        terminal_state="canceled",
                    )
                ).state == "canceled"
            elif mode != "live":
                await session.execute(
                    update(CustomImportLease)
                    .where(CustomImportLease.execution_id == submission.execution_id)
                    .values(expires_at=func.clock_timestamp() - text("interval '1 second'"))
                )
    return loaded, submission.execution_id, key


async def _retained_state(case, execution_id):
    async with case.sessions() as session:
        execution = await session.get(CustomImportExecution, execution_id)
        lease = await session.get(CustomImportLease, execution_id)
        parts = tuple(
            (
                await session.execute(
                    select(
                        CustomImportCaptureParquetPart.capture_bundle_id,
                        CustomImportCaptureParquetPart.payload,
                    ).order_by(
                        CustomImportCaptureParquetPart.capture_bundle_id,
                        CustomImportCaptureParquetPart.stream_slot,
                        CustomImportCaptureParquetPart.part_ordinal,
                    )
                )
            ).all()
        )
        return (
            execution.state,
            execution.capture_bundle_id,
            execution.request_identity_sha256,
            None if lease is None else (lease.fence, lease.token_sha256, lease.expires_at),
            parts,
            await session.scalar(select(func.count()).select_from(CustomImportGeneration)),
        )


async def _resume(installed, case, loaded, key, *, error=None):
    return await _command(
        installed,
        case,
        "resume",
        "--definition-revision-id",
        loaded.definition_revision_id,
        "--source-binding-revision-id",
        loaded.source_binding_revision_id,
        "--idempotency-key",
        key,
        snowflake=True,
        error=error,
    )


async def test_installed_expired_resume_materializes_only_retained_capture(installed_application):
    async with isolated_publication_case() as case:
        loaded, execution_id, key = await _seed_retained_request(installed_application, case)
        before = await _retained_state(case, execution_id)
        result = await _resume(installed_application, case, loaded, key)
        assert result["status"] == "activated" and result["execution_id"] == execution_id
        after = await _retained_state(case, execution_id)
        assert after[0] == "completed" and after[1:3] == before[1:3]
        assert after[3][0] == before[3][0] + 1 and after[4] == before[4]
        assert before[5] == 0 and after[5] == 1
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, loaded.dataset_id)
            seal = await session.get(CustomImportGenerationSeal, result["generation_id"])
            assert pointer.generation_id == result["generation_id"] and pointer.pointer_version == 1
            assert seal.root_count == seal.family_count == 1
        await _resume(installed_application, case, loaded, key, error="failed")
        assert await _retained_state(case, execution_id) == after


async def test_installed_cancel_then_resume_acknowledges_without_materialization(installed_application):
    async with isolated_publication_case() as case:
        loaded, execution_id, key = await _seed_retained_request(installed_application, case)
        before = await _retained_state(case, execution_id)
        canceled = await _command(installed_application, case, "cancel", "--execution-id", execution_id)
        assert canceled["state"] == "canceling" and canceled["changed"] is True
        resumed = await _resume(installed_application, case, loaded, key)
        assert resumed == {"execution_id": execution_id, "status": "canceled"}
        after = await _retained_state(case, execution_id)
        assert after[0] == "canceled" and after[1:3] == before[1:3]
        assert after[4:] == before[4:]
        unchanged = await _command(installed_application, case, "cancel", "--execution-id", execution_id)
        assert unchanged["state"] == "canceled" and unchanged["changed"] is False
        assert await _retained_state(case, execution_id) == after


@pytest.mark.parametrize("mode", ["queued", "live", "unbound", "terminal", "mismatched"])
async def test_installed_resume_denials_do_not_mutate_retained_state(installed_application, mode):
    async with isolated_publication_case() as case:
        loaded, execution_id, key = await _seed_retained_request(installed_application, case, mode=mode)
        if mode == "mismatched":
            document = _registration_document()
            document["source_binding"]["source_object"]["version"] = "synthetic-other-snapshot"
            other = await _command(
                installed_application, case, "register", snowflake=True, stdin_payload=canonical_json(document).encode()
            )
            async with case.sessions() as session:
                loaded = await load_snowflake_source_binding(
                    session,
                    definition_revision_id=other["definition_revision_id"],
                    source_binding_revision_id=other["source_binding_revision_id"],
                )
        before = await _retained_state(case, execution_id)
        await _resume(
            installed_application,
            case,
            loaded,
            key,
            error="failed",
        )
        assert await _retained_state(case, execution_id) == before
