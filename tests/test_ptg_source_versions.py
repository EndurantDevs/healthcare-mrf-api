import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models import (
    PTG2ArtifactManifest,
    PTG2ContentIdentity,
    PTG2SourceFileVersion,
    PTG2SourceIdentity,
)
from process.ptg_parts import source_versions
from process.ptg_parts.domain import (
    PTG2HeadMetadata,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
)


@pytest.mark.asyncio
async def test_record_source_version_preserves_identity_rows_and_order(monkeypatch):
    push_objects = AsyncMock()
    monkeypatch.setitem(
        sys.modules,
        "process.ptg",
        SimpleNamespace(_push_ptg2_objects=push_objects),
    )
    raw_artifact = PTG2RawArtifact(
        original_url="https://example.test/source.json?token=raw",
        canonical_url="https://example.test/source.json",
        raw_path="/tmp/source.json",
        raw_storage_uri="file:///retained/source.json",
        raw_sha256="a" * 64,
        byte_count=123,
        head=PTG2HeadMetadata(
            url="https://example.test/source.json",
            etag='"etag"',
            content_length=123,
            last_modified="Wed, 03 Sep 2026 00:00:00 GMT",
        ),
    )
    logical_artifact = PTG2LogicalArtifact(
        logical_path="/tmp/source.json",
        logical_sha256="b" * 64,
        byte_count=120,
    )

    source_version = await source_versions._record_source_version(
        "in_network",
        "rates",
        raw_artifact,
        logical_artifact,
        import_run_id="run-1",
    )

    calls = push_objects.await_args_list
    assert [call.args[1] for call in calls] == [
        PTG2SourceIdentity,
        PTG2ContentIdentity,
        PTG2SourceFileVersion,
        PTG2ArtifactManifest,
    ]
    recorded_rows = [call.args[0][0] for call in calls]
    assert all(call.kwargs == {"rewrite": True} for call in calls)
    assert recorded_rows[0]["canonical_url"] == raw_artifact.canonical_url
    assert recorded_rows[1]["logical_sha256"] == logical_artifact.logical_sha256
    assert recorded_rows[2]["source_file_version_id"] == source_version.source_file_version_id
    assert recorded_rows[2]["payload"]["import_run_id"] == "run-1"
    assert recorded_rows[3]["storage_uri"] == raw_artifact.raw_storage_uri
    assert source_version.raw_byte_count == raw_artifact.byte_count
