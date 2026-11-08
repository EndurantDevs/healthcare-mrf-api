# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic stdin dispatch checks; no database, network or source data."""

import json

import pytest

from process.custom_import import cli
from process.custom_import.https_candidate import https_request_identity_sha256
from process.custom_import.runner_types import CandidateRunResult
from tests.test_custom_import_cli import _CandidateDatabase, _document_stream, _https_arguments, _https_document


@pytest.mark.parametrize("command", ("execute", "resume"))
@pytest.mark.parametrize("format_name", ("csv", "tsv", "json", "ndjson", "xml", "parquet"))
def test_original_formats_dispatch_explicit_retention_policy(monkeypatch, capsys, command, format_name):
    document = _https_document()
    document["maximum_dataset_retained_bytes"] = 1024 * 1024
    for stream in document["definition"]["streams"]:
        stream.update(format=format_name, compression="gzip")
        if format_name == "xml":
            stream["record_path"] = "row"
    database = _CandidateDatabase()
    calls = []

    async def run_candidate(_session_factory, request, *, resume):
        calls.append((request, resume))
        return CandidateRunResult("no_change", 17)

    monkeypatch.setattr(cli, "db", database)
    monkeypatch.setattr(cli, "run_https_candidate", run_candidate)
    assert cli.run_command(_https_arguments(command), stream=_document_stream(document)) == 0
    request, resume = calls[0]
    assert resume is (command == "resume")
    assert request.maximum_dataset_retained_bytes == 1024 * 1024
    assert all(
        stream.format == format_name and stream.compression == "gzip" for stream in request.definition.source_streams
    )
    assert database.events == ["connect", "disconnect"]
    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out)["status"] == "no_change"


@pytest.mark.parametrize("cap", (None, True, -1, 2**63))
def test_invalid_original_retention_cap_fails_before_database_access(monkeypatch, capsys, cap):
    document = _https_document()
    document["definition"]["streams"][0]["format"] = "csv"
    document["maximum_dataset_retained_bytes"] = cap
    database = _CandidateDatabase()
    monkeypatch.setattr(cli, "db", database)
    assert cli.run_command(_https_arguments(), stream=_document_stream(document)) == 1
    assert database.events == []
    captured = capsys.readouterr()
    assert captured.out == "" and json.loads(captured.err)["status"] == "error"


def test_null_cap_preserves_the_legacy_stdin_request_identity():
    document = _https_document()
    parsed = cli._parse_arguments(_https_arguments())
    original = cli._source_request_from_stdin(parsed, _document_stream(document))
    document["maximum_dataset_retained_bytes"] = None
    explicit_null = cli._source_request_from_stdin(parsed, _document_stream(document))
    assert explicit_null.maximum_dataset_retained_bytes is None
    assert https_request_identity_sha256(explicit_null) == https_request_identity_sha256(original)
