# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import json
from pathlib import Path
from uuid import UUID

import pytest

from api import ptg2_serving
from api.ptg2_manifest_artifacts import PTG2ManifestArtifactError, load_ptg2_manifest_snapshot
from process.ptg_parts.ptg2_manifest_artifacts import write_global_membership_sidecar


class FakeResult:
    def __init__(self, scalar=None, rows=None):
        self._scalar = scalar
        self._rows = list(rows or [])

    def scalar(self):
        return self._scalar

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    async def execute(self, *_args, **_kwargs):
        self.calls.append((_args, _kwargs))
        value = self._results.pop(0) if self._results else None
        if isinstance(value, FakeResult):
            return value
        return FakeResult(value)


class FakePagination:
    limit = 25
    offset = 0


def _write_sidecar(path, payload, *, jsonl=False):
    if jsonl:
        encoded = "".join(json.dumps(row, sort_keys=True) + "\n" for row in payload).encode("utf-8")
    else:
        encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    path.write_bytes(encoded)
    return {
        "path": path.name,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "byte_count": len(encoded),
    }


def _write_membership_sidecar(tmp_path, name, kind, mapping):
    manifest = write_global_membership_sidecar(tmp_path, name, mapping)
    sidecar = dict(manifest["sidecars"][0])
    sidecar["kind"] = kind
    return sidecar


def _write_manifest_snapshot(tmp_path):
    rows_sidecar = _write_sidecar(
        tmp_path / "serving_rows.jsonl",
        [
            {
                "serving_rate_id": "rate-1",
                "plan_id": "010854205",
                "procedure_code": 123456,
                "reported_code_system": "CPT",
                "reported_code": "70551",
                "billing_code": "70551",
                "billing_code_type": "CPT",
                "procedure_name": "MRI brain",
                "provider_set_hash": "provider-set-1",
                "provider_set_hashes": ["provider-set-a"],
                "provider_count": 123,
                "provider_set_count": 1,
                "price_set_id": "price-set-1",
                "source_trace_set_id": "trace-set-1",
                "confidence": {"network": "tic_rate_npi_tin"},
            }
        ],
        jsonl=True,
    )
    price_sidecar = _write_sidecar(
        tmp_path / "prices.json",
        {
            "price-set-1": [
                {
                    "negotiated_type": "negotiated",
                    "negotiated_rate": 450,
                    "service_code": ["23"],
                    "billing_code_modifier": ["TC"],
                }
            ]
        },
    )
    trace_sidecar = _write_sidecar(
        tmp_path / "source_traces.json",
        {"trace-set-1": [{"url": "https://example.test/rates.json.gz"}]},
    )
    manifest = {
        "version": 3,
        "artifact_type": "ptg2_manifest_snapshot",
        "snapshot_id": "snap-manifest",
        "plans": {
            "010854205": {
                "plan_id": "010854205",
                "plan_name": "Example Plan",
            }
        },
        "procedures": {
            "CPT:70551": {
                "code": "70551",
                "billing_code": "70551",
                "billing_code_type": "CPT",
                "name": "MRI brain without contrast",
            }
        },
        "sidecars": [
            {"kind": "skinny_serving_rows", "format": "jsonl", **rows_sidecar},
            {"kind": "price_sets", "format": "json", **price_sidecar},
            {"kind": "source_trace_sets", "format": "json", **trace_sidecar},
        ],
    }
    manifest_path = tmp_path / "snapshot.manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_path


def _write_manifest_snapshot_with_binary_sidecars(tmp_path):
    provider_set_id = bytes.fromhex("0000000000000000000000000000000a")
    provider_id = bytes.fromhex("0000000000000000000000000000000b")
    price_set_id = bytes.fromhex("0000000000000000000000000000000c")
    price_atom_id = bytes.fromhex("0000000000000000000000000000000d")
    rows_sidecar = _write_sidecar(
        tmp_path / "serving_rows.jsonl",
        [
            {
                "serving_rate_id": "rate-1",
                "plan_id": "010854205",
                "procedure_global_id_128": "0000000000000000000000000000000e",
                "reported_code_system": "CPT",
                "reported_code": "70551",
                "provider_set_global_id_128": provider_set_id.hex(),
                "provider_count": 1,
                "price_set_global_id_128": price_set_id.hex(),
            }
        ],
        jsonl=True,
    )
    provider_members = _write_membership_sidecar(
        tmp_path,
        "provider_set_members",
        "provider_set_members",
        {provider_set_id: [provider_id]},
    )
    price_members = _write_membership_sidecar(
        tmp_path,
        "price_set_members",
        "price_set_members",
        {price_set_id: [price_atom_id]},
    )
    manifest = {
        "version": 3,
        "artifact_type": "ptg2_manifest_snapshot",
        "snapshot_id": "snap-manifest-sidecars",
        "plans": {"010854205": {"plan_name": "Example Plan"}},
        "procedures": {"CPT:70551": {"name": "MRI brain without contrast"}},
        "providers": {
            provider_id.hex(): {
                "npi": 1234567890,
                "provider_name": "Example Imaging",
                "state": "IL",
                "city": "Peoria",
                "zip5": "61636",
            }
        },
        "price_atoms": {
            price_atom_id.hex(): {
                "negotiated_type": "negotiated",
                "negotiated_rate": 451,
                "service_code": ["23"],
            }
        },
        "sidecars": [
            {"kind": "skinny_serving_rows", "format": "jsonl", **rows_sidecar},
            provider_members,
            price_members,
        ],
    }
    manifest_path = tmp_path / "snapshot_sidecars.manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_path


def test_ptg2_manifest_reader_loads_manifest_backed_snapshot_sidecars(tmp_path):
    manifest_path = _write_manifest_snapshot(tmp_path)

    snapshot = load_ptg2_manifest_snapshot(manifest_path)

    assert snapshot.snapshot_id == "snap-manifest"
    assert snapshot.rows[0]["serving_rate_id"] == "rate-1"
    assert snapshot.price_sets["price-set-1"][0]["negotiated_rate"] == 450
    assert snapshot.source_trace_sets["trace-set-1"][0]["url"] == "https://example.test/rates.json.gz"


def test_ptg2_manifest_reader_rejects_oversized_manifest_rows(tmp_path, monkeypatch):
    rows_sidecar = _write_sidecar(
        tmp_path / "serving_rows.jsonl",
        [{"plan_id": "010854205"}, {"plan_id": "010854206"}],
        jsonl=True,
    )
    manifest = {
        "version": 3,
        "artifact_type": "ptg2_manifest_snapshot",
        "snapshot_id": "snap-manifest",
        "sidecars": [{"kind": "skinny_serving_rows", "format": "jsonl", **rows_sidecar}],
    }
    manifest_path = tmp_path / "snapshot.manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_ROW_LIMIT", "1")

    with pytest.raises(PTG2ManifestArtifactError, match="too large"):
        load_ptg2_manifest_snapshot(manifest_path)


@pytest.mark.asyncio
async def test_search_current_ptg2_index_routes_manifest_snapshot_to_manifest_exact_lookup(tmp_path):
    manifest_path = _write_manifest_snapshot(tmp_path)
    ptg2_serving.clear_ptg2_index_cache()
    session = FakeSession(
        [
            {
                "serving_index": {
                    "type": "snapshot_index",
                    "storage": "manifest_snapshot",
                    "snapshot_scoped": True,
                    "artifact_uri": manifest_path.resolve().as_uri(),
                }
            }
        ]
    )

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {
            "snapshot_id": "snap-manifest",
            "plan_id": "010854205",
            "code": "70551",
            "code_system": "CPT",
            "include_providers": "false",
        },
        FakePagination(),
    )

    assert len(session.calls) == 1
    assert "source" not in payload["query"]
    assert "serving_table" not in payload["query"]
    assert "procedure_consolidation" not in payload["query"]
    assert payload["pagination"]["total"] == 1
    item = payload["items"][0]
    assert item["procedure_code"] == 123456
    assert item["reported_code_system"] == "CPT"
    assert item["prices"][0]["negotiated_rate"] == 450
    assert item["provider_count"] == 123
    assert "source_trace" not in item
    assert "confidence" not in item
    assert "price_set_hash" not in item
    assert "provider_set_hash" not in item
    assert "service_code" not in item
    assert "tic_prices" not in item


@pytest.mark.asyncio
async def test_ptg2_manifest_snapshot_returns_none_for_provider_expansion(tmp_path):
    manifest_path = _write_manifest_snapshot(tmp_path)
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        artifact_uri=manifest_path.resolve().as_uri(),
    )

    payload = await ptg2_serving.search_ptg2_serving_table(
        FakeSession([]),
        "snap-manifest",
        {
            "plan_id": "010854205",
            "code": "70551",
            "code_system": "CPT",
            "include_providers": "true",
        },
        FakePagination(),
        serving_tables=tables,
    )

    assert payload is None


@pytest.mark.asyncio
async def test_ptg2_manifest_snapshot_expands_provider_and_price_sidecars(tmp_path):
    manifest_path = _write_manifest_snapshot_with_binary_sidecars(tmp_path)
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        artifact_uri=manifest_path.resolve().as_uri(),
    )

    payload = await ptg2_serving.search_ptg2_serving_table(
        FakeSession([]),
        "snap-manifest-sidecars",
        {
            "plan_id": "010854205",
            "code": "70551",
            "code_system": "CPT",
            "include_providers": "true",
        },
        FakePagination(),
        serving_tables=tables,
    )

    assert payload["pagination"]["total"] == 1
    assert "result_granularity" not in payload["query"]
    item = payload["items"][0]
    assert item["npi"] == 1234567890
    assert item["provider_name"] == "Example Imaging"
    assert item["prices"][0]["negotiated_rate"] == 451


@pytest.mark.asyncio
async def test_ptg2_manifest_db_snapshot_serves_exact_plan_code_lookup():
    serving_hash = "00000000000000000000000000000001"
    procedure_id = "00000000000000000000000000000002"
    provider_set_id = "00000000000000000000000000000003"
    price_set_id = "00000000000000000000000000000004"
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        serving_table="mrf.ptg2_manifest_serving_snap_manifest",
        id_storage="uuid",
    )
    session = FakeSession(
        [
            True,
            1,
            FakeResult(rows=[("network_names",)]),
            FakeResult(
                rows=[
                    {
                        "serving_content_hash_128": UUID(serving_hash),
                        "plan_id": "010854205",
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "procedure_global_id_128": UUID(procedure_id),
                        "provider_set_global_id_128": UUID(provider_set_id),
                        "provider_count": 42,
                        "price_set_global_id_128": UUID(price_set_id),
                        "source_trace_set_hash": "trace-set-hash",
                    }
                ]
            ),
        ]
    )

    payload = await ptg2_serving.search_ptg2_serving_table(
        session,
        "snap-manifest",
        {
            "plan_id": "010854205",
            "code": "70551",
            "code_system": "CPT",
            "include_providers": "false",
        },
        FakePagination(),
        serving_tables=tables,
    )

    assert payload["pagination"]["total"] == 1
    assert "source" not in payload["query"]
    assert "serving_table" not in payload["query"]
    item = payload["items"][0]
    assert item["reported_code"] == "70551"
    assert item["provider_count"] == 42
    assert "service_code" not in item
    assert "provider_set_hash" not in item
    assert "price_set_hash" not in item


@pytest.mark.asyncio
async def test_ptg2_manifest_db_snapshot_serves_lean_provider_key_layout():
    provider_set_id = "00000000000000000000000000000003"
    price_set_id = "00000000000000000000000000000004"
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        serving_table="mrf.ptg2_manifest_serving_snap_manifest",
        code_count_table="mrf.ptg2_code_count_snap_manifest",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dict_snap_manifest",
        serving_table_layout="lean_provider_key_v1",
        source_trace_set_hash="trace-set-hash",
        network_names=["C2"],
        id_storage="uuid",
    )
    session = FakeSession(
        [
            True,
            FakeResult(
                rows=[
                    {
                        "code_key": 7,
                        "plan_id": "010854205",
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "rate_count": 123,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "serving_content_hash_128": UUID("00000000000000000000000000000009"),
                        "plan_id": "010854205",
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "procedure_global_id_128": None,
                        "provider_set_global_id_128": UUID(provider_set_id),
                        "provider_count": 42,
                        "price_set_global_id_128": UUID(price_set_id),
                        "source_trace_set_hash": "trace-set-hash",
                        "network_names": ["C2"],
                    }
                ]
            ),
        ]
    )

    payload = await ptg2_serving.search_ptg2_serving_table(
        session,
        "snap-manifest",
        {
            "plan_id": "010854205",
            "code": "70551",
            "code_system": "CPT",
            "include_providers": "false",
        },
        FakePagination(),
        serving_tables=tables,
    )

    assert payload["pagination"]["total"] == 123
    item = payload["items"][0]
    assert item["reported_code"] == "70551"
    assert item["provider_count"] == 42
    row_sql = str(session.calls[2][0][0])
    row_params = session.calls[2][0][1]
    assert "serving.code_key = :code_key" in row_sql
    assert "provider_sets.provider_set_global_id_128" in row_sql
    assert "serving.price_set_global_id_128" in row_sql
    assert row_params["lean_network_names"] == ["C2"]


@pytest.mark.asyncio
async def test_ptg2_manifest_db_snapshot_defers_provider_expansion():
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        serving_table="mrf.ptg2_manifest_serving_snap_manifest",
    )

    payload = await ptg2_serving.search_ptg2_serving_table(
        FakeSession([True]),
        "snap-manifest",
        {
            "plan_id": "010854205",
            "code": "70551",
            "code_system": "CPT",
            "include_providers": "true",
        },
        FakePagination(),
        serving_tables=tables,
    )

    assert payload is None


@pytest.mark.asyncio
async def test_ptg2_manifest_db_snapshot_expands_provider_npi_sidecar(tmp_path):
    provider_set_id = "0000000000000000000000000000000a"
    npi_member = bytes.fromhex("0000000000000000") + (1234567890).to_bytes(8, "big")
    provider_npi_sidecar = _write_membership_sidecar(
        tmp_path,
        "provider_set_npis",
        "provider_npi",
        {bytes.fromhex(provider_set_id): [npi_member]},
    )
    provider_npi_sidecar["path"] = str(tmp_path / provider_npi_sidecar["path"])
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        serving_table="mrf.ptg2_manifest_serving_snap_manifest",
        artifacts={"provider_npi": provider_npi_sidecar},
    )
    session = FakeSession(
        [
            True,
            1,
            FakeResult(rows=[("network_names",)]),
            FakeResult(
                rows=[
                    {
                        "serving_content_hash_128": "serving-hash",
                        "plan_id": "010854205",
                        "reported_code_system": "CPT",
                        "reported_code": "99214",
                        "procedure_global_id_128": "procedure-hash",
                        "provider_set_global_id_128": provider_set_id,
                        "provider_count": 1,
                        "price_set_global_id_128": "price-set-hash",
                        "source_trace_set_hash": "trace-set-hash",
                    }
                ]
            ),
            False,
        ]
    )

    payload = await ptg2_serving.search_ptg2_serving_table(
        session,
        "snap-manifest",
        {
            "plan_id": "010854205",
            "code": "99214",
            "code_system": "CPT",
            "include_providers": "true",
        },
        FakePagination(),
        serving_tables=tables,
    )

    assert "source" not in payload["query"]
    assert "result_granularity" not in payload["query"]
    assert payload["items"][0]["npi"] == 1234567890
    assert payload["items"][0]["provider_name"] == "TiC provider"


def test_ptg2_manifest_sidecar_lookup_merges_multiple_artifacts(tmp_path):
    provider_set_id = "0000000000000000000000000000000a"
    npi_member_1 = bytes.fromhex("0000000000000000") + (1234567890).to_bytes(8, "big")
    npi_member_2 = bytes.fromhex("0000000000000000") + (1234567891).to_bytes(8, "big")
    sidecar_1 = _write_membership_sidecar(
        tmp_path,
        "provider_set_npis_1",
        "provider_npi",
        {bytes.fromhex(provider_set_id): [npi_member_1]},
    )
    sidecar_1["name"] = "provider_npi"
    sidecar_1["path"] = str(tmp_path / sidecar_1["path"])
    sidecar_2 = _write_membership_sidecar(
        tmp_path,
        "provider_set_npis_2",
        "provider_npi",
        {bytes.fromhex(provider_set_id): [npi_member_2]},
    )
    sidecar_2["name"] = "provider_npi"
    sidecar_2["path"] = str(tmp_path / sidecar_2["path"])
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        serving_table="mrf.ptg2_manifest_serving_snap_manifest",
        artifacts={"sidecars": [sidecar_1, sidecar_2]},
    )

    members = ptg2_serving._ptg2_manifest_sidecar_members(tables, "provider_npi", provider_set_id)
    members_many = ptg2_serving._ptg2_manifest_sidecar_members_many(tables, "provider_npi", [provider_set_id])

    assert members == (npi_member_1.hex(), npi_member_2.hex())
    assert members_many[provider_set_id] == (npi_member_1.hex(), npi_member_2.hex())


def test_ptg2_manifest_sidecar_lookup_remaps_stale_default_artifact_root(tmp_path, monkeypatch):
    provider_set_id = "0000000000000000000000000000000a"
    npi_member = bytes.fromhex("0000000000000000") + (1234567890).to_bytes(8, "big")
    artifact_root = tmp_path / "ptg2-artifacts"
    serving_dir = artifact_root / "serving"
    serving_dir.mkdir(parents=True)
    sidecar = _write_membership_sidecar(
        serving_dir,
        "provider_set_npis",
        "provider_npi",
        {bytes.fromhex(provider_set_id): [npi_member]},
    )
    sidecar["name"] = "provider_npi"
    sidecar["path"] = f"/tmp/healthporta-ptg2-artifacts/serving/{sidecar['path']}"
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(artifact_root))
    ptg2_serving._PTG2_MANIFEST_SIDECAR_CACHE.clear()
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        serving_table="mrf.ptg2_manifest_serving_snap_manifest",
        artifacts={"sidecars": [sidecar]},
    )

    members = ptg2_serving._ptg2_manifest_sidecar_members(tables, "provider_npi", provider_set_id)
    members_many = ptg2_serving._ptg2_manifest_sidecar_members_many(tables, "provider_npi", [provider_set_id])

    assert members == (npi_member.hex(),)
    assert members_many[provider_set_id] == (npi_member.hex(),)


def test_ptg2_manifest_sidecar_lookup_remaps_stale_serving_directory(tmp_path, monkeypatch):
    provider_set_id = "0000000000000000000000000000000a"
    npi_member = bytes.fromhex("0000000000000000") + (1234567890).to_bytes(8, "big")
    artifact_root = tmp_path / "ptg2-artifacts"
    serving_dir = artifact_root / "serving" / "current"
    serving_dir.mkdir(parents=True)
    sidecar = _write_membership_sidecar(
        serving_dir,
        "provider_set_npis",
        "provider_npi",
        {bytes.fromhex(provider_set_id): [npi_member]},
    )
    sidecar["name"] = "provider_npi"
    stale_path = Path(sidecar["path"])
    sidecar["path"] = f"/tmp/healthporta-ptg2-artifacts/serving/stale/{stale_path.name}"
    sidecar["byte_count"] = 1
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(artifact_root))
    ptg2_serving._PTG2_MANIFEST_SIDECAR_CACHE.clear()
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        serving_table="mrf.ptg2_manifest_serving_snap_manifest",
        artifacts={"sidecars": [sidecar]},
    )

    members = ptg2_serving._ptg2_manifest_sidecar_members(tables, "provider_npi", provider_set_id)
    members_many = ptg2_serving._ptg2_manifest_sidecar_members_many(tables, "provider_npi", [provider_set_id])

    assert members == (npi_member.hex(),)
    assert members_many[provider_set_id] == (npi_member.hex(),)


def test_ptg2_manifest_sidecar_path_resolver_remaps_stale_hash_suffix(tmp_path, monkeypatch):
    artifact_root = tmp_path / "ptg2-artifacts"
    current_dir = artifact_root / "serving" / "current"
    current_dir.mkdir(parents=True)
    actual = current_dir / "price_forward_currenthash.ptg2sc"
    actual.write_bytes(b"ptg2")
    stale = "/tmp/healthporta-ptg2-artifacts/serving/stale/price_forward_stalehash.ptg2sc"
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(artifact_root))

    resolved = ptg2_serving._resolve_ptg2_manifest_sidecar_path(stale)

    assert resolved == actual


def test_ptg2_manifest_sidecar_path_resolver_remaps_work_artifact_root(tmp_path, monkeypatch):
    artifact_root = tmp_path / "ptg2-artifacts"
    current_dir = artifact_root / "serving" / "current"
    current_dir.mkdir(parents=True)
    actual = current_dir / "price_forward_0ad5cd3fa87618bb.ptg2sc"
    actual.write_bytes(b"ptg2")
    stale = "/work/ptg2-artifacts/serving/stale/price_forward_0ad5cd3fa87618bb.ptg2sc"
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(artifact_root))

    resolved = ptg2_serving._resolve_ptg2_manifest_sidecar_path(stale)

    assert resolved == actual


@pytest.mark.asyncio
async def test_ptg2_manifest_provider_procedures_uses_inverted_provider_sidecar(tmp_path):
    provider_group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    price_set_id = "00000000000000000000000000000013"
    price_atom_id = "00000000000000000000000000000014"
    provider_inverted = _write_membership_sidecar(
        tmp_path,
        "provider_inverted",
        "provider_inverted",
        {bytes.fromhex(provider_group_id): [bytes.fromhex(provider_set_id)]},
    )
    provider_inverted["name"] = "provider_inverted"
    provider_inverted["path"] = str(tmp_path / provider_inverted["path"])
    price_forward = _write_membership_sidecar(
        tmp_path,
        "price_set_members",
        "price_set_members",
        {bytes.fromhex(price_set_id): [bytes.fromhex(price_atom_id)]},
    )
    price_forward["name"] = "price_forward"
    price_forward["path"] = str(tmp_path / price_forward["path"])
    session = FakeSession(
        [
            "snap-manifest",
            {
                "storage": "manifest_snapshot",
                "table": "mrf.ptg2_manifest_serving_snap_manifest",
                "price_atom_table": "mrf.ptg2_manifest_price_atom_snap_manifest",
                "provider_group_member_table": "mrf.ptg2_manifest_provider_group_member_snap_manifest",
                "id_storage": "uuid",
                "artifacts": {
                    "provider_inverted": provider_inverted,
                    "price_forward": price_forward,
                },
            },
            True,
            FakeResult(rows=[{"provider_group_global_id_128": UUID(provider_group_id)}]),
            FakeResult(rows=[]),
            FakeResult(rows=[("network_names",)]),
            FakeResult(
                rows=[
                    {
                        "serving_content_hash_128": UUID("00000000000000000000000000000015"),
                        "plan_id": "010854205",
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "procedure_global_id_128": UUID("00000000000000000000000000000016"),
                        "provider_set_global_id_128": UUID(provider_set_id),
                        "provider_count": 1,
                        "price_set_global_id_128": UUID(price_set_id),
                        "source_trace_set_hash": "trace-set-hash",
                    }
                ]
            ),
            FakeResult(rows=[]),
            FakeResult(
                rows=[
                    {
                        "price_atom_global_id_128": UUID(price_atom_id),
                        "negotiated_type": "negotiated",
                        "negotiated_rate": "451.25",
                        "expiration_date": None,
                        "service_code": ["23"],
                        "billing_class": "professional",
                        "setting": None,
                        "billing_code_modifier": ["26"],
                        "additional_information": None,
                    }
                ]
            ),
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1234567890,
        {
            "plan_id": "010854205",
            "code": "70551",
            "code_system": "CPT",
            "include_details": "true",
        },
        FakePagination(),
    )

    assert payload["query"]["source"] == "ptg2_db"
    assert payload["query"]["provider_reverse_index"] is True
    assert payload["items"][0]["npi"] == 1234567890
    assert payload["items"][0]["provider_set_hash"] == provider_set_id
    assert payload["items"][0]["reported_code"] == "70551"
    assert payload["items"][0]["tic_prices"][0]["negotiated_rate"] == 451.25
