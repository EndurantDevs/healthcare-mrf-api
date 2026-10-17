# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in installed CLI proof against one reviewed immutable synthetic HTTPS object."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from sqlalchemy import func, select

from db.models.custom_import import (
    CustomImportCaptureBundle,
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportPublicationEvent,
)
from process.custom_import.capture_store import load_replayable_parquet_bundle
from process.custom_import.definition import canonical_json
from tests import custom_import_installed_acquisition as support
from tests import custom_import_installed_https as proof
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_installed_acquisition_postgres import _reserve_snapshot_identity, _searchable_registration
from tests.test_custom_import_installed_operator_postgres import _command
from tests.test_custom_import_installed_operator_postgres import installed_application as installed_application


@pytest.fixture(scope="module")
def public_fixture():
    url = os.getenv("HLTHPRT_CUSTOM_IMPORT_HTTPS_FIXTURE_URL")
    path = os.getenv("HLTHPRT_CUSTOM_IMPORT_HTTPS_FIXTURE_FILE")
    if url is None and path is None:
        pytest.skip("provide the reviewed public fixture URL and exact local fixture for the HTTPS proof")
    assert url and path
    return proof.checked_fixture(path, url)


async def _capture_state(case, registered, execution_id, public_fixture):
    async with case.sessions() as session:
        execution = await session.get(CustomImportExecution, execution_id)
        lease = await session.get(CustomImportLease, execution_id)
        assert execution is not None and execution.capture_bundle_id is not None and lease is not None
        captures = await load_replayable_parquet_bundle(
            session,
            capture_bundle_id=execution.capture_bundle_id,
            dataset_id=registered["dataset_id"],
            definition_revision_id=registered["definition_revision_id"],
            schema_revision_id=registered["schema_revision_id"],
        )
        assert len(captures) == 1
        payload = b"".join(captures[0].parts)
        assert len(payload) == public_fixture["expected_bytes"]
        assert hashlib.sha256(payload).hexdigest() == public_fixture["expected_sha256"]
        return execution.state, execution.capture_bundle_id, execution.request_identity_sha256, lease.fence


async def _https_command(installed_application, case, registered, monkeypatch, mode, request_dict):
    installed, outside = installed_application
    with TemporaryDirectory(prefix="installed-https-", dir=outside) as temporary:
        temporary = Path(temporary)
        for module in (support, proof):
            shutil.copyfile(module.__file__, temporary / Path(module.__file__).name)
        configuration = temporary / "registered.json"
        configuration.write_text(json.dumps(registered))
        launch = asyncio.create_subprocess_exec

        async def installed_with_guard(executable, entry, *arguments, **options):
            assert executable == sys.executable and entry == str(installed / "custom_import_cli.py")
            return await launch(
                executable,
                str(temporary / Path(proof.__file__).name),
                "run",
                entry,
                str(configuration),
                mode,
                *arguments,
                **options,
            )

        with monkeypatch.context() as context:
            context.setattr(asyncio, "create_subprocess_exec", installed_with_guard)
            receipt = await _command(
                installed_application,
                case,
                "resume" if mode == "resume" else "execute",
                "--source",
                "https",
                "--dataset-id",
                registered["dataset_id"],
                "--definition-revision-id",
                registered["definition_revision_id"],
                "--schema-revision-id",
                registered["schema_revision_id"],
                stdin_payload=canonical_json(request_dict).encode(),
            )
    assert not temporary.exists()
    return receipt


async def _register_fresh_case(installed_application, case, definition):
    await _reserve_snapshot_identity(case)
    registered = await _command(
        installed_application,
        case,
        "register",
        "--dataset-key",
        "synthetic_https",
        "--format",
        "json",
        stdin_payload=canonical_json(definition).encode(),
    )
    async with case.sessions() as session:
        for model in (CustomImportExecution, CustomImportCaptureBundle, CustomImportGeneration):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
    return registered


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupted", (False, True))
async def test_installed_https_capture_publication_and_offline_resume(
    public_fixture, installed_application, monkeypatch, interrupted
):
    async with isolated_publication_case() as case:
        definition = _searchable_registration()["definition"]
        registered = await _register_fresh_case(installed_application, case, definition)
        request_dict = {
            "definition": definition,
            "objects": [public_fixture],
            "source_snapshot_token": support.SNAPSHOT,
            "idempotency_key": "synthetic-https",
            "lease_token": "synthetic-owner",
            "limits": {},
            "timeout_seconds": 15,
            "complete_scope": True,
        }
        mode = "interrupt" if interrupted else "execute"
        receipt = await _https_command(installed_application, case, registered, monkeypatch, mode, request_dict)
        before = await _capture_state(case, registered, receipt["execution_id"], public_fixture)
        assert before[0] == ("running" if interrupted else "completed")
        if interrupted:
            assert receipt["status"] == "lease_lost"
            original_execution_id = receipt["execution_id"]
            async with case.sessions() as session:
                assert await session.get(CustomImportCurrentGeneration, registered["dataset_id"]) is None
            request_dict["lease_token"] = "synthetic-recovery-owner"
            receipt = await _https_command(installed_application, case, registered, monkeypatch, "resume", request_dict)
            assert receipt["execution_id"] == original_execution_id
            after = await _capture_state(case, registered, receipt["execution_id"], public_fixture)
            assert after[0] == "completed" and after[1:3] == before[1:3] and after[3] == before[3] + 1
        assert receipt["status"] == "activated"
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, registered["dataset_id"])
            seal = await session.get(CustomImportGenerationSeal, receipt["generation_id"])
            assert pointer.generation_id == receipt["generation_id"] and pointer.pointer_version == 1
            assert seal.root_count == seal.family_count == seal.winner_count == len(support.NPIS)
            assert await session.scalar(select(func.count()).select_from(CustomImportPublicationEvent)) == 1
