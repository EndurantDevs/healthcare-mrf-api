# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in signed-object capture and installed, source-free durable replay."""

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
from urllib.parse import parse_qsl, quote

from process.custom_import import cli, https, object_candidate, runner, s3
from process.custom_import.definition import canonical_json
from tests import custom_import_installed_acquisition as installed_support
from tests import custom_import_installed_https as installed_https
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_https import _Response
from tests.test_custom_import_https_candidate_postgres import forbid_https_transport as forbid_https_transport
from tests.test_custom_import_installed_https_postgres import _register_fresh_case
from tests.test_custom_import_installed_operator_postgres import _command, _publication_state
from tests.test_custom_import_installed_operator_postgres import installed_application as installed_application
from tests.test_custom_import_raw_capture_postgres import (
    _bound_raw_state,
    _interrupt_bound_candidate,
    _original_cli_request,
)
from tests.test_custom_import_s3 import _signature_from_wire
from tests.test_custom_import_s3_credentials import _credential_file, _document
from tests.test_custom_import_uploaded_lifecycle_postgres import _assert_uploaded_publication, _observe_retention


def _s3_request():
    request_by_key, payloads_by_stream = _original_cli_request()
    original_object = request_by_key["objects"][0]
    request_by_key["objects"] = [
        {name: value for name, value in original_object.items() if name != "url"}
        | dict(
            endpoint="https://objects.example:8443",
            region="us-east-1",
            addressing="path",
            bucket="synthetic-captures",
            key="nested/../é +%/root.csv.gz",
            version_id="version+/=% &é",
        )
    ]
    return request_by_key, payloads_by_stream


def _s3_arguments(registered, command):
    return [
        command,
        "--source",
        "s3",
        "--dataset-id",
        str(registered["dataset_id"]),
        "--definition-revision-id",
        str(registered["definition_revision_id"]),
        "--schema-revision-id",
        str(registered["schema_revision_id"]),
    ]


def _assert_signed_get(wire_url, source_object):
    query_by_name = dict(parse_qsl(wire_url.raw_query_string))
    credentials_by_name = _document()
    assert wire_url.raw_authority == "objects.example:8443"
    assert wire_url.raw_path == "/synthetic-captures/" + quote(source_object.key, safe="/")
    assert query_by_name["versionId"] == source_object.version_id
    assert query_by_name["X-Amz-Security-Token"] == credentials_by_name["session_token"]
    assert query_by_name["X-Amz-Algorithm"] == "AWS4-HMAC-SHA256"
    assert query_by_name["X-Amz-SignedHeaders"] == "host"
    expected_scope = "/".join((query_by_name["X-Amz-Date"][:8], source_object.region, "s3", "aws4_request"))
    assert query_by_name["X-Amz-Credential"] == credentials_by_name["access_key_id"] + "/" + expected_scope
    assert 0 < int(query_by_name["X-Amz-Expires"]) <= 15
    assert query_by_name["X-Amz-Signature"] == _signature_from_wire(wire_url, credentials_by_name["secret_access_key"])


def _observe_s3_body(monkeypatch, source_object, payload):
    request_digests = []

    async def fetch_original(wire_url, spool, expected_sha256, expected_bytes, limits, timeout_seconds, *, exact_url):
        assert exact_url is True and 0 < timeout_seconds <= 15
        _assert_signed_get(wire_url, source_object)
        request_digests.append(hashlib.sha256(str(wire_url).encode()).hexdigest())
        async with _Response((payload,), headers=(("Content-Length", str(len(payload))),)) as response:
            await https._copy_response(response, spool, expected_sha256, expected_bytes, limits)
        assert response.closed

    monkeypatch.setattr(https, "_fetch_object", fetch_original)
    return request_digests


def _offline_s3_main():
    import os
    import sys
    from pathlib import Path

    import custom_import_installed_acquisition as support
    import custom_import_installed_https as proof

    diagnostic_by_key = {"network_event": None}
    sys.addaudithook(support.loopback_database_only(int(os.environ["HLTHPRT_DB_PORT"]), diagnostic_by_key))
    installed_root = Path(sys.argv[2]).resolve().parent
    sys.path.insert(0, str(installed_root))
    from process.custom_import import https, object_candidate, s3, s3_credentials, upload

    denied_attempts = []

    def forbid_source_access(*_arguments, **_keywords):
        denied_attempts.append(True)
        raise AssertionError("offline resume attempted source access")

    for module in (https, object_candidate, s3, s3_credentials):
        assert Path(module.__file__).resolve().is_relative_to(installed_root)
    for owner, name in (
        (s3_credentials.FixedLocalS3CredentialProvider, "__init__"),
        (s3_credentials.FixedLocalS3CredentialProvider, "load_credentials"),
        (s3_credentials, "_read_credentials"),
        (s3_credentials, "_open_fixed_credential_directory"),
        (s3_credentials.os, "open"),
        (upload.StagedUploadProvider, "__post_init__"),
        (upload.StagedUploadProvider, "_open"),
        (object_candidate, "_acquire_object"),
        (s3, "_signed_url"),
        (s3, "acquire_s3"),
        (https, "acquire_https"),
        (https, "_fetch_object"),
    ):
        setattr(owner, name, forbid_source_access)
    exit_status = proof.main()
    assert not denied_attempts and diagnostic_by_key["network_event"] is None
    raise SystemExit(exit_status)


async def _installed_s3_resume(installed_application, case, registered, monkeypatch, request_by_key):
    installed, outside = installed_application
    with TemporaryDirectory(prefix="installed-s3-resume-", dir=outside) as temporary:
        temporary = Path(temporary)
        for module in (installed_support, installed_https):
            shutil.copyfile(module.__file__, temporary / Path(module.__file__).name)
        configuration = temporary / "registered.json"
        configuration.write_text(json.dumps(registered))
        bootstrap = temporary / "offline_s3.py"
        bootstrap.write_text(inspect.getsource(_offline_s3_main) + "\n_offline_s3_main()\n")
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
                *_s3_arguments(registered, "resume"),
                stdin_payload=canonical_json(request_by_key).encode(),
            )
    assert not temporary.exists()
    return receipt


async def test_s3_installed_offline_resume(installed_application, monkeypatch, tmp_path):
    async with isolated_publication_case() as case:
        request_by_key, payloads_by_stream = _s3_request()
        registered = await _register_fresh_case(installed_application, case, request_by_key["definition"])
        encoded_request = canonical_json(request_by_key).encode()
        parsed = cli._parse_arguments(_s3_arguments(registered, "execute"))
        request = cli._prepare_stdin(parsed, BytesIO(encoded_request))
        assert type(request) is object_candidate.ObjectCandidateRequest and request.source_kind == "s3"
        assert type(request.objects[0]) is s3.S3Object
        identity = object_candidate.object_request_identity_sha256(request)
        credential_directory, credential_file = _credential_file(tmp_path)
        assert credential_file.stat().st_mode & 0o777 == 0o400
        assert str(credential_directory).encode() not in encoded_request
        assert all(credential_value.encode() not in encoded_request for credential_value in _document().values())
        with monkeypatch.context() as interruption:
            interruption.setattr(object_candidate, "FIXED_S3_CREDENTIAL_DIRECTORY", credential_directory)
            signed_requests = _observe_s3_body(interruption, request.objects[0], payloads_by_stream["root_source"])
            observed = _observe_retention(interruption, payloads_by_stream)
            interruption.setattr(runner, "run_claimed_candidate", _interrupt_bound_candidate)
            interrupted = await object_candidate.run_object_candidate(case.sessions, request)
        assert interrupted.status == "lease_lost" and len(signed_requests) == 1
        assert len(observed.registrations) == len(observed.readbacks) == 1
        assert observed.registrations[0] is not observed.readbacks[0]
        before = await _bound_raw_state(case, registered, interrupted.execution_id, payloads_by_stream)
        assert before[0] == "running" and before[2] == identity and before[3] == 1
        assert await _publication_state(case, registered["dataset_id"]) == (None, ())
        unavailable = tmp_path / "unavailable-operator"
        credential_directory.rename(unavailable)
        request_by_key["lease_token"] = "synthetic-s3-recovery-owner"
        receipt = await _installed_s3_resume(installed_application, case, registered, monkeypatch, request_by_key)
        assert receipt["status"] == "activated" and receipt["execution_id"] == interrupted.execution_id
        after = await _bound_raw_state(case, registered, receipt["execution_id"], payloads_by_stream)
        assert after[0] == "completed" and after[1:3] == before[1:3] and after[3] == 2
        await _assert_uploaded_publication(case, registered, receipt, request_by_key["objects"][0])
        assert not credential_directory.exists() and (unavailable / credential_file.name).is_file()
