import json

import pytest

from process.ptg_parts import ptg2_artifact_blobs as artifact_blobs


class FakeSession:
    def __init__(self):
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), params))


class FakeTransaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


class FakeDB:
    def __init__(self):
        self.session = FakeSession()

    def transaction(self):
        return FakeTransaction(self.session)


@pytest.mark.asyncio
async def test_store_ptg2_artifact_in_db_removes_transient_local_path(monkeypatch, tmp_path):
    artifact_path = tmp_path / "provider-forward.ptg2sc"
    artifact_path.write_bytes(b"provider-membership")
    fake_db = FakeDB()

    async def fake_ensure(_schema):
        return None

    monkeypatch.setattr(artifact_blobs, "db", fake_db)
    monkeypatch.setattr(artifact_blobs, "ensure_ptg2_artifact_blob_table", fake_ensure)
    monkeypatch.setattr(artifact_blobs, "ptg2_artifact_db_retain_local_cache", lambda: False)

    stored = await artifact_blobs.store_ptg2_artifact_file_in_db(
        artifact_path,
        snapshot_id="ptg2:test",
        artifact_kind="provider_forward",
        metadata={"name": "provider_forward", "path": str(artifact_path)},
    )

    manifest_params = next(params for sql, params in fake_db.session.calls if "ptg2_artifact_manifest" in sql)
    manifest_payload = json.loads(manifest_params["payload"])
    assert not artifact_path.exists()
    assert "path" not in stored
    assert "path" not in manifest_payload
    assert stored["storage_uri"].startswith("db://ptg2_artifact/")


@pytest.mark.asyncio
async def test_store_ptg2_artifact_honors_per_artifact_chunk_size(monkeypatch, tmp_path):
    artifact_path = tmp_path / "provider-graph.ptg2sc"
    artifact_path.write_bytes(b"x" * (2 * 1024 * 1024 + 1))
    fake_db = FakeDB()

    async def fake_ensure(_schema):
        return None

    monkeypatch.setattr(artifact_blobs, "db", fake_db)
    monkeypatch.setattr(artifact_blobs, "ensure_ptg2_artifact_blob_table", fake_ensure)

    stored = await artifact_blobs.store_ptg2_artifact_file_in_db(
        artifact_path,
        snapshot_id="ptg2:test",
        artifact_kind="provider_npi_group",
        retain_local_cache=True,
        metadata={"chunk_bytes": 1024 * 1024},
    )

    chunk_inserts = [
        params
        for sql, params in fake_db.session.calls
        if "INSERT INTO" in sql and "ptg2_artifact_blob_chunk" in sql
    ]
    assert len(chunk_inserts) == 3
    assert stored["chunk_bytes"] == 1024 * 1024
