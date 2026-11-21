# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure native source parsing and existing operator receipt dispatch."""

import json

import pytest

from process.custom_import import cli, object_candidate, upload
from process.custom_import.object_candidate import ObjectCandidateRequest
from process.custom_import.runner_types import CandidateRunResult
from tests.test_custom_import_cli import _CandidateDatabase, _document_stream, _https_arguments, _https_document


def _arguments(source_kind, command="execute"):
    arguments = _https_arguments(command)
    arguments[arguments.index("https")] = source_kind
    return arguments


def _document(source_kind):
    document = _https_document()
    document["maximum_dataset_retained_bytes"] = 1024 * 1024
    for item in document["objects"]:
        del item["url"]
        if source_kind == "s3":
            item.update(endpoint="https://objects.example", region="us-east-1", bucket="synthetic-captures", key="rows")
        else:
            item["object_id"] = f"source_{item['stream_id']}"
    return document


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
@pytest.mark.parametrize("command", ("execute", "resume"))
def test_native_cli_dispatch_uses_same_redacted_receipt(monkeypatch, capsys, source_kind, command):
    database, calls = _CandidateDatabase(), []

    async def run_candidate(_sessions, request, *, resume):
        calls.append((request, resume))
        return CandidateRunResult("no_change", 17)

    monkeypatch.setattr(cli, "db", database)
    monkeypatch.setattr(cli, "run_object_candidate", run_candidate)
    assert cli.run_command(_arguments(source_kind, command), stream=_document_stream(_document(source_kind))) == 0
    request, resume = calls[0]
    assert type(request) is ObjectCandidateRequest and request.source_kind == source_kind
    assert resume is (command == "resume")
    assert request.maximum_dataset_retained_bytes == 1024 * 1024
    assert database.events == ["connect", "disconnect"]
    output = capsys.readouterr()
    assert not output.err
    assert json.loads(output.out)["status"] == "no_change"


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
def test_native_stdin_parsing_never_constructs_source_providers(monkeypatch, source_kind):
    def forbidden(*_arguments, **_options):
        pytest.fail("pure parsing cannot access source providers")

    monkeypatch.setattr(object_candidate, "FixedLocalS3CredentialProvider", forbidden)
    monkeypatch.setattr(upload, "StagedUploadProvider", forbidden)
    parsed = cli._parse_arguments(_arguments(source_kind))
    assert cli._prepare_stdin(parsed, _document_stream(_document(source_kind))).source_kind == source_kind


def test_upload_child_order_is_bound_to_the_execution():
    parsed = cli._parse_arguments(_arguments("upload"))
    document = _document("upload")
    document["definition"]["refresh_mode"] = "snapshot"
    original = cli._prepare_stdin(parsed, _document_stream(document))
    ordered = cli._prepare_stdin(parsed, _document_stream(document | {"preserve_child_order": True}))

    assert original.preserve_child_order is False
    assert ordered.preserve_child_order is True
    assert object_candidate.object_request_identity_sha256(original) != (
        object_candidate.object_request_identity_sha256(ordered)
    )


@pytest.mark.parametrize(
    ("source_kind", "mode", "order_value"),
    [
        ("s3", "snapshot", True),
        ("upload", "upsert", True),
        ("upload", "snapshot", 1),
    ],
)
def test_child_order_requires_a_typed_staged_snapshot(monkeypatch, source_kind, mode, order_value):
    document = _document(source_kind)
    document["definition"]["refresh_mode"] = mode
    document["preserve_child_order"] = order_value
    database = _CandidateDatabase()
    monkeypatch.setattr(cli, "db", database)
    assert cli.run_command(_arguments(source_kind), stream=_document_stream(document)) == 1
    assert not database.events


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
@pytest.mark.parametrize("invalid", ("missing_cap", "bad_cap", "path", "credential", "missing_pin", "wrong_object"))
def test_native_invalid_stdin_fails_before_database(monkeypatch, capsys, source_kind, invalid):
    document = _document(source_kind)
    scope, key, value = {
        "missing_cap": ("envelope", "maximum_dataset_retained_bytes", None),
        "bad_cap": ("envelope", "maximum_dataset_retained_bytes", True),
        "path": ("object", "path", "/private-source-marker"),
        "credential": ("object", "secret_access_key", "private-source-marker"),
        "missing_pin": ("object", "expected_sha256", None),
        "wrong_object": ("object", "url", "https://objects.example/unrelated"),
    }[invalid]
    target = document if scope == "envelope" else document["objects"][0]
    if value is None:
        del target[key]
    else:
        target[key] = value
    database = _CandidateDatabase()
    monkeypatch.setattr(cli, "db", database)
    assert cli.run_command(_arguments(source_kind), stream=_document_stream(document)) == 1
    assert not database.events
    output = capsys.readouterr()
    assert not output.out and "private-source-marker" not in output.err
    assert json.loads(output.err)["status"] == "error"
