from __future__ import annotations

import hashlib
import zlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import ptg2_artifact_blobs as artifact_blobs


class AsyncRows:
    def __init__(self, rows):
        self._rows = iter(rows)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._rows)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class StreamingSession:
    def __init__(self, rows):
        self.rows = rows
        self.stream_calls = []

    async def stream(self, statement, params):
        self.stream_calls.append((str(statement), params))
        return AsyncRows(self.rows)


class RecordingSession:
    def __init__(self):
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), params))


class Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


class RecordingDB:
    def __init__(self):
        self.session = RecordingSession()

    def transaction(self):
        return Transaction(self.session)


def _artifact_uri() -> str:
    return artifact_blobs.ptg2_db_artifact_uri("a" * 32)


def test_artifact_blob_environment_parsing_and_uri_validation(monkeypatch):
    monkeypatch.delenv(artifact_blobs.PTG2_ARTIFACT_DB_STORE_ENV, raising=False)
    assert artifact_blobs.is_artifact_db_store_enabled() is True
    monkeypatch.setenv(artifact_blobs.PTG2_ARTIFACT_DB_STORE_ENV, "off")
    assert artifact_blobs.is_artifact_db_store_enabled() is False

    monkeypatch.setenv(artifact_blobs.PTG2_ARTIFACT_DB_CHUNK_BYTES_ENV, "invalid")
    assert artifact_blobs._artifact_db_chunk_bytes() == artifact_blobs._DEFAULT_CHUNK_BYTES
    monkeypatch.setenv(artifact_blobs.PTG2_ARTIFACT_DB_CHUNK_BYTES_ENV, "1")
    assert artifact_blobs._artifact_db_chunk_bytes() == 1024 * 1024

    monkeypatch.setenv(artifact_blobs.PTG2_ARTIFACT_DB_COMPRESSION_LEVEL_ENV, "invalid")
    assert artifact_blobs._artifact_db_compression_level() == 6
    monkeypatch.setenv(artifact_blobs.PTG2_ARTIFACT_DB_COMPRESSION_LEVEL_ENV, "99")
    assert artifact_blobs._artifact_db_compression_level() == 9
    monkeypatch.setenv(artifact_blobs.PTG2_ARTIFACT_DB_COMPRESSION_LEVEL_ENV, "-1")
    assert artifact_blobs._artifact_db_compression_level() == 0

    assert artifact_blobs.ptg2_artifact_id_from_uri("https://example.test/a") is None
    assert artifact_blobs.ptg2_artifact_id_from_uri("db://ptg2_artifact/") is None
    assert artifact_blobs.ptg2_artifact_id_from_uri("db://ptg2_artifact/a/b") is None
    assert artifact_blobs.ptg2_artifact_id_from_uri("db://ptg2_artifact/../a") is None
    assert artifact_blobs.ptg2_artifact_id_from_uri(_artifact_uri()) == "a" * 32


def test_artifact_blob_row_mapping_supports_driver_and_plain_rows():
    assert artifact_blobs._row_mapping(SimpleNamespace(_mapping={"value": 1})) == {
        "value": 1
    }
    assert artifact_blobs._row_mapping({"value": 2}) == {"value": 2}
    assert artifact_blobs._row_mapping((("value", 3),)) == {"value": 3}


@pytest.mark.asyncio
async def test_delete_artifacts_ignores_blank_snapshot_and_deletes_in_order(monkeypatch):
    fake_db = RecordingDB()
    ensure_table = AsyncMock()
    monkeypatch.setattr(artifact_blobs, "db", fake_db)
    monkeypatch.setattr(
        artifact_blobs,
        "ensure_ptg2_artifact_blob_table",
        ensure_table,
    )

    await artifact_blobs.delete_ptg2_artifacts_for_snapshot("   ")
    assert fake_db.session.calls == []
    assert ensure_table.await_count == 0

    await artifact_blobs.delete_ptg2_artifacts_for_snapshot(
        "ptg2:test",
        schema_name="custom",
    )
    assert ensure_table.await_args.args == ("custom",)
    assert len(fake_db.session.calls) == 3
    assert "guard_ptg2_v4_attempt" in fake_db.session.calls[0][0]
    assert "ptg2_artifact_blob_chunk" in fake_db.session.calls[1][0]
    assert "ptg2_artifact_manifest" in fake_db.session.calls[2][0]


def test_artifact_cache_paths_and_validation(monkeypatch, tmp_path):
    monkeypatch.setenv(
        artifact_blobs.PTG2_ARTIFACT_DB_CACHE_DIR_ENV,
        str(tmp_path),
    )
    artifact_id = "a" * 32
    named_path = artifact_blobs._artifact_cache_path(
        artifact_id,
        {"path": "/transient/provider-forward.ptg2sc"},
    )
    generated_path = artifact_blobs._artifact_cache_path(
        artifact_id,
        {"name": "provider-forward", "suffix": "ptg2sc"},
    )
    assert named_path.name == "provider-forward.ptg2sc"
    assert generated_path.name == f"provider-forward_{artifact_id}.ptg2sc"
    assert named_path.parent == tmp_path / "aa" / "aa"

    assert artifact_blobs._is_cached_file_valid(named_path, {}) is False
    named_path.parent.mkdir(parents=True)
    named_path.write_bytes(b"artifact-bytes")
    assert artifact_blobs._is_cached_file_valid(
        named_path,
        {"byte_count": 1},
    ) is False
    assert artifact_blobs._is_cached_file_valid(
        named_path,
        {"byte_count": len(b"artifact-bytes")},
    ) is True
    expected_sha = hashlib.sha256(b"artifact-bytes").hexdigest()
    assert artifact_blobs._is_cached_file_valid(
        named_path,
        {"sha256": expected_sha},
    ) is True
    assert artifact_blobs._is_cached_file_valid(
        named_path,
        {"sha256": "0" * 64},
    ) is False
    broken_cached_path = Mock()
    broken_cached_path.exists.return_value = True
    broken_cached_path.is_file.return_value = True
    broken_cached_path.stat.side_effect = OSError("cache disappeared")
    assert artifact_blobs._is_cached_file_valid(
        broken_cached_path,
        {"byte_count": 1},
    ) is False


@pytest.mark.asyncio
async def test_materialize_artifact_streams_mixed_chunks_and_reuses_valid_cache(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv(
        artifact_blobs.PTG2_ARTIFACT_DB_CACHE_DIR_ENV,
        str(tmp_path),
    )
    first_raw = b"first chunk"
    second_raw = b" and second"
    expected_bytes = first_raw + second_raw
    session = StreamingSession(
        [
            {
                "chunk_no": 0,
                "compression": "zlib",
                "payload": zlib.compress(first_raw),
                "raw_byte_count": len(first_raw),
            },
            {
                "chunk_no": 1,
                "compression": "none",
                "payload": second_raw,
                "raw_byte_count": len(second_raw),
            },
        ]
    )
    metadata = {
        "name": "provider-forward",
        "suffix": ".ptg2sc",
        "byte_count": len(expected_bytes),
        "sha256": hashlib.sha256(expected_bytes).hexdigest(),
    }

    cache_path = await artifact_blobs.materialize_ptg2_artifact_from_db(
        session,
        _artifact_uri(),
        metadata=metadata,
    )
    assert cache_path.read_bytes() == expected_bytes
    assert len(session.stream_calls) == 1

    reused_path = await artifact_blobs.materialize_ptg2_artifact_from_db(
        session,
        _artifact_uri(),
        metadata=metadata,
    )
    assert reused_path == cache_path
    assert len(session.stream_calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "metadata", "message"),
    (
        ([], {}, "has no PostgreSQL chunks"),
        (
            [
                {
                    "compression": "none",
                    "payload": b"abc",
                    "raw_byte_count": 99,
                }
            ],
            {},
            "raw byte_count mismatch",
        ),
        (
            [
                {
                    "compression": "none",
                    "payload": b"abc",
                    "raw_byte_count": 3,
                }
            ],
            {"byte_count": 99},
            "artifact byte_count mismatch",
        ),
        (
            [
                {
                    "compression": "none",
                    "payload": b"abc",
                    "raw_byte_count": 3,
                }
            ],
            {"sha256": "0" * 64},
            "artifact checksum mismatch",
        ),
    ),
)
async def test_materialize_artifact_rejects_corrupt_or_missing_chunks(
    monkeypatch,
    tmp_path,
    rows,
    metadata,
    message,
):
    monkeypatch.setenv(
        artifact_blobs.PTG2_ARTIFACT_DB_CACHE_DIR_ENV,
        str(tmp_path),
    )
    metadata = {"name": "corrupt", **metadata}

    with pytest.raises((FileNotFoundError, ValueError), match=message):
        await artifact_blobs.materialize_ptg2_artifact_from_db(
            StreamingSession(rows),
            _artifact_uri(),
            metadata=metadata,
        )

    assert list(tmp_path.rglob("*.tmp.*")) == []


@pytest.mark.asyncio
async def test_materialize_rejects_non_database_uri_and_hydrate_handles_both_storage_modes(
    monkeypatch,
    tmp_path,
):
    session = StreamingSession([])
    with pytest.raises(ValueError, match="unsupported PTG2 artifact storage uri"):
        await artifact_blobs.materialize_ptg2_artifact_from_db(
            session,
            "file:///tmp/artifact",
        )

    local_entry_map = {
        "path": "/tmp/local",
        "storage_uri": "file:///tmp/local",
    }
    assert await artifact_blobs.hydrate_ptg2_artifact_entry(
        session,
        local_entry_map,
    ) == local_entry_map

    hydrated_path = tmp_path / "hydrated.ptg2sc"
    materialize = AsyncMock(return_value=hydrated_path)
    monkeypatch.setattr(
        artifact_blobs,
        "materialize_ptg2_artifact_from_db",
        materialize,
    )
    hydrated_entry_map = await artifact_blobs.hydrate_ptg2_artifact_entry(
        session,
        {"storage_uri": _artifact_uri(), "name": "provider-forward"},
        schema_name="custom",
    )
    assert hydrated_entry_map["path"] == str(hydrated_path)
    assert hydrated_entry_map["cache_path"] == str(hydrated_path)
    assert materialize.await_args.kwargs["schema_name"] == "custom"
