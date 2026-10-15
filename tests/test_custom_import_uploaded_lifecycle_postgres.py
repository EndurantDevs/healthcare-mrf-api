# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in staged-original acquisition and offline installed upload-resume proof."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import shutil
import sys
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

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
from db.models.custom_import_raw_capture import CustomImportCaptureRawObject
from process.custom_import import cli, https_candidate, object_candidate, raw_capture_store, runner, upload
from process.custom_import.definition import canonical_json
from tests import custom_import_installed_acquisition as installed_support
from tests import custom_import_installed_https as installed_https
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_installed_https_postgres import _register_fresh_case
from tests.test_custom_import_installed_operator_postgres import _command
from tests.test_custom_import_installed_operator_postgres import installed_application as installed_application
from tests.test_custom_import_raw_capture_postgres import (
    _interrupt_bound_candidate,
    _original_cli_request,
)


def _uploaded_cli_request(format_name="csv"):
    request_by_key, payloads_by_stream = _original_cli_request()
    if format_name != "csv":
        stream = request_by_key["definition"]["streams"][0]
        stream.update(format=format_name, compression="none")
        originals_by_format = {
            "tsv": "ROOT_NPI\tSOURCE_NOTE\r\n"
            + "".join(f'{npi}\t"synthetic\tnote"\r\n' for npi in installed_support.NPIS),
            "json": json.dumps([{"ROOT_NPI": npi} for npi in installed_support.NPIS], indent=2) + "\n",
            "xml": '<?xml version="1.0"?>\n<records>\n'
            + "".join(f"<record><ROOT_NPI>{npi}</ROOT_NPI></record>\n" for npi in installed_support.NPIS)
            + "</records>\n",
        }
        if format_name == "xml":
            stream["record_path"] = "record"
        payload = originals_by_format[format_name].encode()
        payloads_by_stream = {"root_source": payload}
        request_by_key["objects"][0].update(
            expected_bytes=len(payload), expected_sha256=hashlib.sha256(payload).hexdigest()
        )
    original_object = request_by_key["objects"][0]
    request_by_key["objects"] = [
        {name: value for name, value in original_object.items() if name != "url"} | {"object_id": "uploaded_root"}
    ]
    return request_by_key, payloads_by_stream


def _observe_retention(monkeypatch, payloads_by_stream):
    observations = SimpleNamespace(registrations=[], readbacks=[])
    register = https_candidate.register_raw_capture_bundle
    load = https_candidate.load_replayable_raw_bundle

    async def register_originals(session, **arguments):
        observations.registrations.append(session)
        return await register(session, **arguments)

    async def read_originals(session, **arguments):
        observations.readbacks.append(session)
        captures = await load(session, **arguments)
        assert {capture.manifest.stream_id: capture.payload for capture in captures} == payloads_by_stream
        return captures

    monkeypatch.setattr(https_candidate, "register_raw_capture_bundle", register_originals)
    monkeypatch.setattr(https_candidate, "load_replayable_raw_bundle", read_originals)
    return observations


async def _bound_uploaded_state(case, registered, execution_id, request, payloads_by_stream):
    async with case.sessions() as session:
        execution = await session.get(CustomImportExecution, execution_id)
        lease = await session.get(CustomImportLease, execution_id)
        replayed = await raw_capture_store.load_replayable_raw_bundle(
            session,
            capture_bundle_id=execution.capture_bundle_id,
            dataset_id=registered["dataset_id"],
            definition_revision_id=registered["definition_revision_id"],
            schema_revision_id=registered["schema_revision_id"],
        )
        assert {capture.manifest.stream_id: capture.payload for capture in replayed} == payloads_by_stream
        assert [(capture.manifest.format, capture.manifest.compression) for capture in replayed] == [
            (stream.format, stream.compression) for stream in request.definition.source_streams
        ]
        assert await session.scalar(select(func.count()).select_from(CustomImportCaptureRawObject)) == 1
        return execution.state, execution.capture_bundle_id, bytes(execution.request_identity_sha256), lease.fence


def _offline_resume_main():
    import os
    import sys
    from pathlib import Path

    import custom_import_installed_acquisition as support
    import custom_import_installed_https as proof

    diagnostic_by_key = {"network_event": None}
    sys.addaudithook(support.loopback_database_only(int(os.environ["HLTHPRT_DB_PORT"]), diagnostic_by_key))
    sys.path.insert(0, str(Path(sys.argv[2]).resolve().parent))
    from process.custom_import import https, object_candidate, s3, s3_credentials, snowflake, upload

    denied_attempts = []

    def forbid_source_access(*_arguments, **_keywords):
        denied_attempts.append(True)
        raise AssertionError("offline resume attempted source access")

    upload.StagedUploadProvider.__post_init__ = forbid_source_access
    upload.StagedUploadProvider._open = forbid_source_access
    upload.os.open = forbid_source_access
    s3_credentials.FixedLocalS3CredentialProvider.__init__ = forbid_source_access
    s3_credentials.FixedLocalS3CredentialProvider.load_credentials = forbid_source_access
    s3_credentials._read_credentials = forbid_source_access
    snowflake.FixedLocalKeyPairCredentialProvider.__init__ = forbid_source_access
    snowflake._read_fixed_credential_file = forbid_source_access
    object_candidate._acquire_object = forbid_source_access
    upload.acquire_upload = forbid_source_access
    s3.acquire_s3 = forbid_source_access
    https.acquire_https = forbid_source_access
    exit_status = proof.main()
    assert not denied_attempts and diagnostic_by_key["network_event"] is None
    raise SystemExit(exit_status)


async def _installed_upload_resume(installed_application, case, registered, monkeypatch, request_by_key):
    installed, outside = installed_application
    with TemporaryDirectory(prefix="installed-upload-resume-", dir=outside) as temporary:
        temporary = Path(temporary)
        for module in (installed_support, installed_https):
            shutil.copyfile(module.__file__, temporary / Path(module.__file__).name)
        configuration = temporary / "registered.json"
        configuration.write_text(json.dumps(registered))
        bootstrap = temporary / "offline_upload.py"
        bootstrap.write_text(inspect.getsource(_offline_resume_main) + "\n_offline_resume_main()\n")
        launch = asyncio.create_subprocess_exec

        async def installed_with_guard(executable, entry, *arguments, **options):
            assert executable == sys.executable and entry == str(installed / "custom_import_cli.py")
            return await launch(
                executable, str(bootstrap), "run", entry, str(configuration), "resume", *arguments, **options
            )

        with monkeypatch.context() as context:
            context.setattr(asyncio, "create_subprocess_exec", installed_with_guard)
            receipt = await _command(
                installed_application,
                case,
                "resume",
                "--source",
                "upload",
                "--dataset-id",
                registered["dataset_id"],
                "--definition-revision-id",
                registered["definition_revision_id"],
                "--schema-revision-id",
                registered["schema_revision_id"],
                stdin_payload=canonical_json(request_by_key).encode(),
            )
    assert not temporary.exists()
    return receipt


async def _assert_uploaded_publication(case, registered, receipt, expected_object):
    async with case.sessions() as session:
        pointer = await session.get(CustomImportCurrentGeneration, registered["dataset_id"])
        seal = await session.get(CustomImportGenerationSeal, receipt["generation_id"])
        assert pointer.generation_id == receipt["generation_id"] and pointer.pointer_version == 1
        assert seal.root_count == seal.family_count == seal.winner_count == len(installed_support.NPIS)
        assert await session.scalar(select(func.count()).select_from(CustomImportPublicationEvent)) == 1
        raw = (await session.scalars(select(CustomImportCaptureRawObject))).one()
        assert raw.byte_count == expected_object["expected_bytes"]
        assert bytes(raw.payload_sha256).hex() == expected_object["expected_sha256"]
        assert hashlib.sha256(bytes(raw.payload)).hexdigest() == expected_object["expected_sha256"]


@pytest.mark.parametrize("format_name", ("csv", "tsv", "json", "xml"))
async def test_staged_upload_replays_durably_through_offline_installed_cli(
    installed_application, monkeypatch, tmp_path, format_name
):
    async with isolated_publication_case() as case:
        request_by_key, payloads_by_stream = _uploaded_cli_request(format_name)
        registered = await _register_fresh_case(installed_application, case, request_by_key["definition"])
        request = cli._source_request_from_stdin(
            SimpleNamespace(source="upload", **registered), BytesIO(canonical_json(request_by_key).encode())
        )
        assert type(request) is object_candidate.ObjectCandidateRequest and request.source_kind == "upload"
        staging = tmp_path / "staged-originals"
        staging.mkdir()
        original = staging / "uploaded_root"
        original.write_bytes(payloads_by_stream["root_source"])
        provider = upload.StagedUploadProvider(staging)
        with monkeypatch.context() as interruption:
            observed = _observe_retention(interruption, payloads_by_stream)
            interruption.setattr(runner, "run_claimed_candidate", _interrupt_bound_candidate)
            interrupted = await object_candidate.run_object_candidate(case.sessions, request, upload_provider=provider)
        assert interrupted.status == "lease_lost"
        assert len(observed.registrations) == len(observed.readbacks) == 1
        assert observed.registrations[0] is not observed.readbacks[0]
        before = await _bound_uploaded_state(case, registered, interrupted.execution_id, request, payloads_by_stream)
        assert before[0] == "running" and before[3] == 1
        assert before[2] == object_candidate.object_request_identity_sha256(request)
        async with case.sessions() as session:
            assert await session.get(CustomImportCurrentGeneration, registered["dataset_id"]) is None
        assert original.read_bytes() == payloads_by_stream["root_source"]
        unavailable = tmp_path / "offline-originals"
        staging.rename(unavailable)
        request_by_key["lease_token"] = "synthetic-upload-recovery-owner"
        receipt = await _installed_upload_resume(installed_application, case, registered, monkeypatch, request_by_key)
        assert receipt["status"] == "activated" and receipt["execution_id"] == interrupted.execution_id
        after = await _bound_uploaded_state(case, registered, receipt["execution_id"], request, payloads_by_stream)
        assert after[0] == "completed" and after[1:3] == before[1:3] and after[3] == 2
        await _assert_uploaded_publication(case, registered, receipt, request_by_key["objects"][0])
        assert (
            not staging.exists() and (unavailable / "uploaded_root").read_bytes() == payloads_by_stream["root_source"]
        )


async def test_malformed_uploaded_xml_fails_without_retention_or_publication(
    installed_application, monkeypatch, tmp_path
):
    async with isolated_publication_case() as case:
        request_by_key, _ = _uploaded_cli_request("xml")
        malformed_xml = b"<records><record><ROOT_NPI>1003000126</ROOT_NPI></records>"
        request_by_key["objects"][0].update(
            expected_bytes=len(malformed_xml), expected_sha256=hashlib.sha256(malformed_xml).hexdigest()
        )
        registered = await _register_fresh_case(installed_application, case, request_by_key["definition"])
        request = cli._source_request_from_stdin(
            SimpleNamespace(source="upload", **registered), BytesIO(canonical_json(request_by_key).encode())
        )
        original = tmp_path / "uploaded_root"
        original.write_bytes(malformed_xml)
        observed = _observe_retention(monkeypatch, {"root_source": malformed_xml})
        with pytest.raises(object_candidate.ObjectCandidateError, match="^object_candidate_failed$"):
            await object_candidate.run_object_candidate(
                case.sessions, request, upload_provider=upload.StagedUploadProvider(tmp_path)
            )
        assert not observed.registrations and not observed.readbacks
        async with case.sessions() as session:
            execution = (await session.scalars(select(CustomImportExecution))).one()
            assert execution.state == "failed" and execution.capture_bundle_id is None
            for model in (
                CustomImportCaptureBundle,
                CustomImportCaptureRawObject,
                CustomImportGeneration,
                CustomImportCurrentGeneration,
                CustomImportPublicationEvent,
            ):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
        assert original.read_bytes() == malformed_xml
