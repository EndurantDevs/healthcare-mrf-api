# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guarded compiler-to-reader PostgreSQL proof for packed PTG V4."""

from __future__ import annotations

from collections import OrderedDict
import copy
from dataclasses import dataclass
import hashlib
import importlib
import importlib.util
import json
import os
from pathlib import Path
import statistics
import struct
import time
from typing import Awaitable, Callable
import uuid

import asyncpg
import pytest
import sqlalchemy as sa

from api import ptg2_candidate_audit_v4 as candidate_v4
from api import ptg2_v4_graph as graph
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)
from api.ptg2_types import PTG2ServingTables
from db.connection import Database
from db.migration_ptg2_frozen_source_file_binding import (
    install_frozen_source_file_binding,
)
from process.ptg_parts import (
    frozen_rate_binding_store,
    ptg2_shared_publish,
    ptg2_v4_audit,
    source_download,
)
from process.ptg_parts import ptg2_shared_snapshot_publish as snapshot_publish
from process.ptg_parts.domain import (
    PTG2DownloadedJob,
    PTG2HeadMetadata,
    PTG2RawArtifact,
)
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_OPTION,
    frozen_rate_binding_from_params,
    normalize_protected_frozen_rate_params,
)
from process.ptg_parts.frozen_rate_binding_store import (
    insert_or_compare_frozen_binding,
)
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_SET_CONTRACT,
    frozen_rate_file_proof_sha256,
    frozen_rate_file_set_sha256,
    normalize_frozen_rate_file_set,
)
from process.ptg_parts.frozen_rate_runtime import (
    build_frozen_rate_jobs,
    validate_frozen_processed_results,
)
from process.ptg_parts import ptg2_v4_failed_layout_recovery as recovery
from process.ptg_parts.ptg2_shared_blocks import SharedBlock
from process.ptg_parts.ptg2_shared_gc import (
    PTG2_V3_MIGRATION_OWNED_TABLE_NAMES,
    abandon_owned_v4_layout,
)
from process.ptg_parts.ptg2_v4_graph_compiler import (
    compile_provider_graph_v4_rust,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
    publish_v4_snapshot_maps,
    reserve_v4_shared_layout,
    seal_v4_shared_layout,
)
from scripts.ptg_v4_dev_canary_storage import relation_size_rows
from tests.ptg2_v4_migration_catalog_support import (
    attempt_guard_prerequisite_ddl,
    v3_provider_set_prerequisite_ddl,
)
from tests.ptg2_v4_graph_compiler_test_support import _write_tax_identity
from tests.ptg2_v4_provider_prefix_support import sealed_v4_hot_prefix
from tests import test_ptg2_scanner_v3_runs as scanner_support


ROOT = Path(__file__).resolve().parents[1]
ptg_candidate_audit = importlib.import_module("process.ptg_candidate_audit")
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260723100000_ptg2_v4_snapshot_map_pack.py"
)
TAXONOMY_MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260724120000_ptg2_v4_taxonomy_candidates.py"
)
TAX_IDENTITY_MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260727100000_ptg2_provider_tax_identity.py"
)
_STANDARD_FORMAT = (
    "magic8:uint32_le_version:uint64_le_entry_count:"
    "index(owner16:uint64_le_offset:uint32_le_count):members16"
)
_GROUP_COUNT = 5_000
_SET_COUNT = 16
_NPI = 1_234_567_890


class _OpRecorder:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, statement) -> None:
        self.executed.append(str(statement))


def _load_v4_migration():
    spec = importlib.util.spec_from_file_location(
        "ptg2_v4_postgres_e2e_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_v4_taxonomy_migration():
    spec = importlib.util.spec_from_file_location(
        "ptg2_v4_postgres_e2e_taxonomy_migration",
        TAXONOMY_MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_v4_tax_identity_migration():
    spec = importlib.util.spec_from_file_location(
        "ptg2_v4_tax_identity_migration",
        TAX_IDENTITY_MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _quoted(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _global(domain: int, value: int) -> bytes:
    return bytes([domain]) + bytes(7) + int(value).to_bytes(8, "big")


def _npi(value: int) -> bytes:
    return bytes(8) + int(value).to_bytes(8, "big")


def _write_membership(
    path: Path,
    *,
    name: str,
    pairs: list[tuple[bytes, bytes]],
) -> dict[str, object]:
    by_owner: dict[bytes, set[bytes]] = {}
    for owner, member in pairs:
        by_owner.setdefault(owner, set()).add(member)
    normalized_memberships = [
        (owner, sorted(members))
        for owner, members in sorted(by_owner.items())
    ]
    membership_payload = bytearray(b"PTG2MNSC")
    membership_payload.extend(struct.pack("<IQ", 1, len(normalized_memberships)))
    offset = 0
    for owner, members in normalized_memberships:
        membership_payload.extend(owner)
        membership_payload.extend(struct.pack("<QI", offset, len(members)))
        offset += len(members)
    for _owner, members in normalized_memberships:
        for member in members:
            membership_payload.extend(member)
    path.write_bytes(membership_payload)
    return {
        "name": name,
        "source_shard_id": "postgres-e2e",
        "path": str(path),
        "record_format": _STANDARD_FORMAT,
        "sha256": hashlib.sha256(membership_payload).hexdigest(),
        "byte_count": len(membership_payload),
        "owner_count": len(normalized_memberships),
        "member_count": offset,
    }


def _factor_fixture(tmp_path: Path) -> tuple[list[dict[str, object]], Path]:
    component = _global(2, 1)
    groups = [_global(3, index + 1) for index in range(_GROUP_COUNT)]
    provider_sets = [_global(1, index + 1) for index in range(_SET_COUNT)]
    npi = _npi(_NPI)
    artifacts = [
        _write_membership(
            tmp_path / "set-component.sidecar",
            name="provider_set_component",
            pairs=[(provider_set, component) for provider_set in provider_sets],
        ),
        _write_membership(
            tmp_path / "component-group.sidecar",
            name="provider_component_group",
            pairs=[(component, group) for group in groups],
        ),
        _write_membership(
            tmp_path / "group-npi.sidecar",
            name="provider_group_npi",
            pairs=[(group, npi) for group in groups],
        ),
        _write_membership(
            tmp_path / "npi-group.sidecar",
            name="provider_npi_group",
            pairs=[(npi, group) for group in groups],
        ),
        _write_tax_identity(
            tmp_path / "group-tax-identity.sidecar",
            shard_id="postgres-e2e",
            tax_observations=[(group, 2, None) for group in groups],
        ),
    ]
    provider_map = tmp_path / "provider-set-map.tsv"
    provider_map.write_text(
        "".join(
            f"{provider_set.hex()}\t{index}\n"
            for index, provider_set in enumerate(provider_sets, start=1)
        ),
        encoding="ascii",
    )
    return artifacts, provider_map


def _direct_factor_fixture(
    tmp_path: Path,
) -> tuple[list[dict[str, object]], Path]:
    provider_sets = [_global(1, 1), _global(1, 2)]
    components = [_global(2, 1), _global(2, 2)]
    groups = [_global(3, 1), _global(3, 2)]
    npis = [_npi(1_111_111_111), _npi(2_222_222_222)]
    artifacts = [
        _write_membership(
            tmp_path / "direct-set-component.sidecar",
            name="provider_set_component",
            pairs=list(zip(provider_sets, components, strict=True)),
        ),
        _write_membership(
            tmp_path / "direct-component-group.sidecar",
            name="provider_component_group",
            pairs=list(zip(components, reversed(groups), strict=True)),
        ),
        _write_membership(
            tmp_path / "direct-group-npi.sidecar",
            name="provider_group_npi",
            pairs=list(zip(groups, npis, strict=True)),
        ),
        _write_membership(
            tmp_path / "direct-npi-group.sidecar",
            name="provider_npi_group",
            pairs=list(zip(npis, groups, strict=True)),
        ),
        _write_tax_identity(
            tmp_path / "direct-group-tax-identity.sidecar",
            shard_id="postgres-e2e",
            tax_observations=[(group, 2, None) for group in groups],
        ),
    ]
    provider_map = tmp_path / "direct-provider-set-map.tsv"
    provider_map.write_text(
        "".join(
            f"{provider_set.hex()}\t{index}\n"
            for index, provider_set in enumerate(provider_sets)
        ),
        encoding="ascii",
    )
    return artifacts, provider_map


@dataclass(frozen=True)
class _FrozenScanBatch:
    descriptors: list[dict[str, object]]
    set_digest: str
    proof_rows: list[dict[str, object]]
    scans: tuple[dict[str, object], ...]


def _multipart_scanner_payloads() -> tuple[dict[str, object], ...]:
    first_payload = scanner_support._fixture_payload(
        provider_references_first=True
    )
    second_payload = copy.deepcopy(first_payload)
    second_payload["provider_references"][0]["provider_group_id"] = 2
    second_payload["provider_references"][0]["provider_groups"][0]["npi"] = [
        1234567892,
        1234567893,
    ]
    second_payload["provider_references"][0]["provider_groups"][0]["tin"] = {
        "type": "ein",
        "value": "98-7654321",
    }
    second_payload["in_network"][0]["billing_code"] = "99214"
    second_payload["in_network"][0]["negotiated_rates"][0][
        "provider_references"
    ] = [2]
    return first_payload, second_payload


def _frozen_descriptor(
    *,
    artifact_path: Path,
    ordinal: int,
) -> dict[str, object]:
    raw_payload = artifact_path.read_bytes()
    canonical_url = (
        "https://rates.example.test/frozen/"
        f"part-{ordinal:03d}.json"
    )
    raw_sha256 = hashlib.sha256(raw_payload).hexdigest()
    return {
        "source_type": "in_network",
        "canonical_url": canonical_url,
        "content_length": len(raw_payload),
        "etag": f'"frozen-part-{ordinal:03d}"',
        "last_modified": None,
        "raw_sha256": raw_sha256,
        "logical_sha256": raw_sha256,
        "logical_hash_deferred": False,
        "engine_source_identity_hash": hashlib.blake2b(
            f"identity:{canonical_url}".encode(),
            digest_size=8,
        ).hexdigest(),
        "engine_source_file_version_id": hashlib.blake2b(
            f"version:{canonical_url}:{raw_sha256}".encode(),
            digest_size=8,
        ).hexdigest(),
        "ordinal": ordinal,
    }


async def _acquire_and_scan_frozen_parts(
    tmp_path: Path,
    monkeypatch,
) -> _FrozenScanBatch:
    """Acquire two deterministic files, then scan those exact local bytes."""

    artifact_paths, descriptors = _write_frozen_rate_inputs(tmp_path)
    set_digest = frozen_rate_file_set_sha256(descriptors)
    normalized_descriptors, normalized_digest = (
        normalize_frozen_rate_file_set(descriptors, set_digest)
    )
    raw_artifacts_by_url = _frozen_raw_artifacts_by_url(
        normalized_descriptors,
        artifact_paths,
    )

    async def download_local_artifact(url: str, **options):
        assert options["exact_get_evidence"] is True
        return raw_artifacts_by_url[url]

    monkeypatch.setattr(
        source_download,
        "download_raw_artifact",
        download_local_artifact,
    )
    downloaded_jobs = await _download_frozen_jobs(normalized_descriptors)
    scans = _scan_downloaded_frozen_parts(
        tmp_path,
        downloaded_jobs,
    )
    return _FrozenScanBatch(
        descriptors=normalized_descriptors,
        set_digest=normalized_digest,
        proof_rows=validate_frozen_processed_results(
            normalized_descriptors,
            _processed_frozen_results(normalized_descriptors),
        ),
        scans=scans,
    )


def _write_frozen_rate_inputs(
    tmp_path: Path,
) -> tuple[list[Path], list[dict[str, object]]]:
    acquired_directory = tmp_path / "frozen-acquired"
    acquired_directory.mkdir()
    artifact_paths: list[Path] = []
    descriptors: list[dict[str, object]] = []
    for ordinal, rate_payload in enumerate(
        _multipart_scanner_payloads(),
        start=1,
    ):
        artifact_path = acquired_directory / f"part-{ordinal:03d}.json"
        artifact_path.write_text(
            json.dumps(rate_payload, separators=(",", ":")),
            encoding="utf-8",
        )
        artifact_paths.append(artifact_path)
        descriptors.append(
            _frozen_descriptor(
                artifact_path=artifact_path,
                ordinal=ordinal,
            )
        )
    return artifact_paths, descriptors


def _frozen_raw_artifacts_by_url(
    descriptors: list[dict[str, object]],
    artifact_paths: list[Path],
) -> dict[str, PTG2RawArtifact]:
    return {
        str(descriptor["canonical_url"]): PTG2RawArtifact(
            original_url=str(descriptor["canonical_url"]),
            canonical_url=str(descriptor["canonical_url"]),
            raw_path=str(artifact_path),
            raw_storage_uri=str(artifact_path),
            raw_sha256=str(descriptor["raw_sha256"]),
            byte_count=int(descriptor["content_length"]),
            head=PTG2HeadMetadata(
                url=str(descriptor["canonical_url"]),
                status=200,
                etag=str(descriptor["etag"]),
                content_length=int(descriptor["content_length"]),
                content_type="application/json",
                supports_head=True,
            ),
            verification_mode="downloaded",
        )
        for descriptor, artifact_path in zip(
            descriptors,
            artifact_paths,
            strict=True,
        )
    }


async def _download_frozen_jobs(
    descriptors: list[dict[str, object]],
) -> list[PTG2DownloadedJob]:
    jobs = build_frozen_rate_jobs(
        descriptors,
        plan_info=(),
        source_network_names=(),
    )
    downloaded_jobs = [
        await source_download._download_ptg_job_artifact(
            job,
            reuse_raw_artifacts=False,
            max_bytes=None,
            keep_partial_artifacts=False,
        )
        for job in jobs
    ]
    assert all(downloaded_job.error is None for downloaded_job in downloaded_jobs)
    return downloaded_jobs


def _scan_downloaded_frozen_parts(
    tmp_path: Path,
    downloaded_jobs: list[PTG2DownloadedJob],
) -> tuple[dict[str, object], ...]:
    scanner_binary = scanner_support._built_scanner_binary()
    scans = tuple(
        scanner_support._run_scanner(
            scanner_binary,
            tmp_path,
            f"frozen-scanner-{ordinal:03d}",
            arch="postgres_binary_v3",
            provider_references_first=True,
            grouped=False,
            input_artifact=Path(downloaded_job.raw_artifact.raw_path),
        )
        for ordinal, downloaded_job in enumerate(downloaded_jobs, start=1)
    )
    assert [
        scanner_support._single_frame(
            scan["frames"],
            "scanner_summary",
        )["serving_run_rows"]
        for scan in scans
    ] == [1, 1]
    return scans


def _processed_frozen_results(
    descriptors: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [
        {
            "success": True,
            "source_type": descriptor["source_type"],
            "url": descriptor["canonical_url"],
            "summary": {
                **descriptor,
                "raw_byte_count": descriptor["content_length"],
                "verification_mode": "downloaded",
            },
        }
        for descriptor in descriptors
    ]


def _write_provider_graph_artifacts(
    tmp_path: Path,
    *,
    set_component_pairs,
    component_group_pairs,
    group_npi_pairs,
    npi_group_pairs,
    tax_observations,
) -> list[dict[str, object]]:
    return [
        _write_membership(
            tmp_path / "frozen-set-component.sidecar",
            name="provider_set_component",
            pairs=set_component_pairs,
        ),
        _write_membership(
            tmp_path / "frozen-component-group.sidecar",
            name="provider_component_group",
            pairs=component_group_pairs,
        ),
        _write_membership(
            tmp_path / "frozen-group-npi.sidecar",
            name="provider_group_npi",
            pairs=group_npi_pairs,
        ),
        _write_membership(
            tmp_path / "frozen-npi-group.sidecar",
            name="provider_npi_group",
            pairs=npi_group_pairs,
        ),
        _write_tax_identity(
            tmp_path / "frozen-group-tax-identity.sidecar",
            shard_id="postgres-e2e",
            tax_observations=tax_observations,
        ),
    ]


def _provider_graph_identities(
    scan: dict[str, object],
) -> tuple[bytes, bytes, list[bytes]]:
    serving_records = [
        scanner_support._SERVING_RECORD.unpack_from(
            scan["partition_bytes"],
            offset,
        )
        for offset in range(
            0,
            len(scan["partition_bytes"]),
            scanner_support._SERVING_RECORD.size,
        )
    ]
    assert len(serving_records) == 1
    member_records = [
        member_line.split(b"\t", 1)
        for frame in scan["provider_group_member_frames"]
        for member_line in Path(frame["path"]).read_bytes().splitlines()
    ]
    provider_group_ids = {
        bytes.fromhex(member_record[0].decode("ascii"))
        for member_record in member_records
    }
    assert len(provider_group_ids) == 1
    return (
        serving_records[0][1],
        next(iter(provider_group_ids)),
        [_npi(int(raw_npi)) for _group_hex, raw_npi in member_records],
    )


def _write_provider_set_map(
    tmp_path: Path,
    provider_sets_by_key: dict[int, bytes],
) -> Path:
    provider_map = tmp_path / "frozen-provider-set-map.tsv"
    provider_map.write_text(
        "".join(
            f"{provider_set_id.hex()}\t{provider_set_key}\n"
            for provider_set_key, provider_set_id in sorted(
                provider_sets_by_key.items()
            )
        ),
        encoding="ascii",
    )
    return provider_map


def _scan_provider_graph_fixture(
    tmp_path: Path,
    scans: tuple[dict[str, object], ...],
) -> tuple[list[dict[str, object]], Path, dict[int, bytes]]:
    """Convert the two scanner identities into the V4 compiler input."""

    provider_sets_by_key: dict[int, bytes] = {}
    set_component_pairs = []
    component_group_pairs = []
    group_npi_pairs = []
    npi_group_pairs = []
    tax_observations = []
    for provider_set_key, scan in enumerate(scans):
        provider_set_id, provider_group_id, npi_ids = (
            _provider_graph_identities(scan)
        )
        provider_sets_by_key[provider_set_key] = provider_set_id
        component_id = hashlib.blake2b(
            b"frozen-component:" + provider_group_id,
            digest_size=16,
        ).digest()
        set_component_pairs.append((provider_set_id, component_id))
        component_group_pairs.append((component_id, provider_group_id))
        group_npi_pairs.extend(
            (provider_group_id, npi_id) for npi_id in npi_ids
        )
        npi_group_pairs.extend(
            (npi_id, provider_group_id) for npi_id in npi_ids
        )
        tax_observations.append((provider_group_id, 2, None))
    artifacts = _write_provider_graph_artifacts(
        tmp_path,
        set_component_pairs=set_component_pairs,
        component_group_pairs=component_group_pairs,
        group_npi_pairs=group_npi_pairs,
        npi_group_pairs=npi_group_pairs,
        tax_observations=tax_observations,
    )
    provider_map = _write_provider_set_map(
        tmp_path,
        provider_sets_by_key,
    )
    return artifacts, provider_map, provider_sets_by_key


async def _insert_provider_set_rows(
    session,
    *,
    schema_name: str,
    snapshot_key: int,
    provider_sets_by_key: dict[int, bytes],
) -> None:
    """Persist the V3 provider-set identities consumed by V4 diagnostics."""

    schema = _quoted(schema_name)
    parameter_rows = [
        {
            "snapshot_key": int(snapshot_key),
            "provider_set_key": int(provider_set_key),
            "provider_set_global_id_128": bytes(provider_set_global_id),
            "provider_count": 1,
        }
        for provider_set_key, provider_set_global_id in sorted(
            provider_sets_by_key.items()
        )
    ]
    await session.execute(
        sa.text(
            f"""
            INSERT INTO {schema}.ptg2_v3_provider_set
                (snapshot_key, provider_set_key,
                 provider_set_global_id_128, provider_count)
            VALUES
                (:snapshot_key, :provider_set_key,
                 :provider_set_global_id_128, :provider_count)
            """
        ),
        parameter_rows,
    )


def _compiler_binary() -> Path:
    return Path(
        os.getenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4_BIN")
        or ROOT
        / "support"
        / "ptg2_scanner"
        / "target"
        / "debug"
        / "ptg2_provider_graph_v4"
    )


def _isolate_graph_caches(monkeypatch) -> None:
    monkeypatch.setattr(graph, "_MAP_COORDINATE_CACHE", graph._ByteLRU(8 << 20))
    monkeypatch.setattr(graph, "_PHYSICAL_BLOCK_CACHE", graph._ByteLRU(8 << 20))
    monkeypatch.setattr(graph, "_ROOT_CACHE", OrderedDict())
    monkeypatch.setattr(graph, "_RELATION_CACHE", OrderedDict())
    monkeypatch.setattr(graph, "_HEAVY_OWNER_CACHE", OrderedDict())
    monkeypatch.setattr(graph, "_HEAVY_OWNER_NEGATIVE_CACHE", OrderedDict())


async def _create_v4_layout_tables(database: Database, schema: str) -> None:
    for statement in (
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
            snapshot_key bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            storage_shard_id smallint NOT NULL DEFAULT 0,
            build_token varchar(96) NOT NULL,
            generation varchar(32) NOT NULL,
            state varchar(16) NOT NULL,
            mapping_digest bytea,
            support_digest bytea,
            layout_manifest jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            logical_byte_count bigint NOT NULL DEFAULT 0,
            created_at timestamptz NOT NULL DEFAULT now(),
            heartbeat_at timestamptz NOT NULL DEFAULT now(),
            lease_until timestamptz,
            published_at timestamptz
        )
        """,
        f"""
        CREATE UNIQUE INDEX ptg2_v4_e2e_sealed_mapping_idx
            ON {schema}.ptg2_v3_snapshot_layout
               (generation, mapping_digest, support_digest)
         WHERE state = 'sealed'
           AND mapping_digest IS NOT NULL
           AND support_digest IS NOT NULL
        """,
        v3_provider_set_prerequisite_ddl(schema),
        f"""
        CREATE TABLE {schema}.ptg2_v3_layout_fingerprint (
            semantic_fingerprint bytea PRIMARY KEY,
            snapshot_key bigint NOT NULL REFERENCES
                {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """,
    ):
        await database.execute_ddl(statement)


async def _create_v4_provider_tables(database: Database, schema: str) -> None:
    for statement in (
        f"""
        CREATE TABLE {schema}.ptg2_v3_provider_group (
            snapshot_key bigint NOT NULL,
            provider_group_key integer NOT NULL,
            provider_group_global_id_128 bytea NOT NULL,
            PRIMARY KEY (snapshot_key, provider_group_key),
            UNIQUE (snapshot_key, provider_group_global_id_128)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_npi_scope (
            snapshot_key bigint NOT NULL,
            npi bigint NOT NULL,
            PRIMARY KEY (snapshot_key, npi)
        )
        """,
        f"""
        CREATE TABLE {schema}.npi (
            npi bigint PRIMARY KEY,
            entity_type_code integer
        )
        """,
        f"""
        CREATE TABLE {schema}.npi_taxonomy (
            npi bigint NOT NULL,
            checksum integer NOT NULL,
            healthcare_provider_taxonomy_code varchar,
            PRIMARY KEY (npi, checksum)
        )
        """,
    ):
        await database.execute_ddl(statement)


async def _create_v4_block_tables(database: Database, schema: str) -> None:
    for statement in (
        f"""
        CREATE TABLE {schema}.ptg2_v3_block (
            block_hash bytea PRIMARY KEY,
            format_version smallint NOT NULL,
            object_kind varchar(64) NOT NULL,
            codec varchar(16) NOT NULL,
            entry_count bigint NOT NULL,
            raw_byte_count bigint NOT NULL,
            stored_byte_count bigint NOT NULL,
            payload bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_block (
            snapshot_key bigint NOT NULL REFERENCES
                {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
            object_kind varchar(64) NOT NULL,
            block_key bigint NOT NULL,
            fragment_no integer NOT NULL,
            entry_count bigint NOT NULL,
            block_hash bytea NOT NULL REFERENCES
                {schema}.ptg2_v3_block(block_hash),
            PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_gc_candidate (
            block_hash bytea PRIMARY KEY REFERENCES
                {schema}.ptg2_v3_block(block_hash) ON DELETE CASCADE,
            eligible_at timestamptz NOT NULL,
            queued_at timestamptz NOT NULL DEFAULT now()
        )
        """,
    ):
        await database.execute_ddl(statement)


async def _apply_v4_test_migrations(
    database: Database,
    *,
    schema_name: str,
    monkeypatch,
) -> None:
    schema = _quoted(schema_name)
    await database.execute_ddl(attempt_guard_prerequisite_ddl(schema))

    migration = _load_v4_migration()
    recorder = _OpRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setattr(migration, "_schema", lambda: schema_name)
    migration.upgrade()
    taxonomy_migration = _load_v4_taxonomy_migration()
    monkeypatch.setattr(taxonomy_migration, "op", recorder)
    monkeypatch.setattr(taxonomy_migration, "_schema", lambda: schema_name)
    taxonomy_migration.upgrade()
    for statement in recorder.executed:
        await database.execute_ddl(statement)

    tax_recorder = _OpRecorder()
    tax_identity_migration = _load_v4_tax_identity_migration()
    monkeypatch.setattr(tax_identity_migration, "op", tax_recorder)
    monkeypatch.setattr(tax_identity_migration, "_schema", lambda: schema_name)
    tax_identity_migration.upgrade()
    async with database.transaction() as session:
        connection = await session.connection()
        for statement in tax_recorder.executed:
            await connection.exec_driver_sql(statement)


async def _create_v4_test_schema(
    database: Database,
    *,
    schema_name: str,
    monkeypatch,
) -> None:
    """Create the minimal migrated V4 catalog required by the E2E tests."""

    schema = _quoted(schema_name)
    await database.execute_ddl(f"CREATE SCHEMA {schema}")
    await _create_v4_layout_tables(database, schema)
    await _create_v4_provider_tables(database, schema)
    await _create_v4_block_tables(database, schema)
    await _apply_v4_test_migrations(
        database,
        schema_name=schema_name,
        monkeypatch=monkeypatch,
    )


async def _install_frozen_candidate_test_schema(
    database: Database,
    *,
    schema_name: str,
) -> None:
    """Install the real binding DDL plus source rows read by candidate audit."""

    recorder = _OpRecorder()
    install_frozen_source_file_binding(recorder, schema_name)
    for statement in recorder.executed:
        await database.execute_ddl(statement)
    schema = _quoted(schema_name)
    for statement in (
        f"""
        CREATE TABLE {schema}.ptg2_source_identity (
            source_identity_hash varchar(64) PRIMARY KEY,
            source_type varchar(64) NOT NULL,
            canonical_url text NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_source_file_version (
            source_file_version_id varchar(64) PRIMARY KEY,
            source_identity_hash varchar(64) NOT NULL,
            raw_sha256 varchar(64) NOT NULL,
            logical_sha256 varchar(64) NOT NULL,
            content_length bigint NOT NULL,
            etag text,
            last_modified text,
            verification_mode varchar(64) NOT NULL,
            payload jsonb NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_source_trace (
            source_trace_hash varchar(64) PRIMARY KEY,
            source_file_version_id varchar(64) NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_source_trace_set (
            source_trace_set_hash varchar(64) PRIMARY KEY,
            source_trace_hashes varchar(64)[] NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_source (
            snapshot_id varchar(96) NOT NULL,
            source_key smallint NOT NULL,
            raw_container_sha256 varchar(64) NOT NULL,
            source_trace_set_hash varchar(64) NOT NULL,
            PRIMARY KEY (snapshot_id, source_key)
        )
        """,
    ):
        await database.execute_ddl(statement)


async def _seed_frozen_candidate_sources(
    database: Database,
    *,
    schema_name: str,
    snapshot_id: str,
    descriptors: list[dict[str, object]],
) -> None:
    """Persist exact source-version chains for candidate-audit replay."""

    schema = _quoted(schema_name)
    for source_key, descriptor in enumerate(descriptors):
        trace_hash = hashlib.sha256(
            f"trace:{source_key}".encode()
        )
        trace_set_hash = hashlib.sha256(
            f"trace-set:{source_key}".encode()
        )
        await _seed_frozen_source_trace(
            database,
            schema=schema,
            descriptor=descriptor,
            trace_hash=trace_hash.hexdigest(),
        )
        await _seed_frozen_snapshot_source(
            database,
            schema=schema,
            snapshot_id=snapshot_id,
            source_key=source_key,
            descriptor=descriptor,
            trace_hash=trace_hash.hexdigest(),
            trace_set_hash=trace_set_hash.hexdigest(),
        )


async def _seed_frozen_source_trace(
    database: Database,
    *,
    schema: str,
    descriptor: dict[str, object],
    trace_hash: str,
) -> None:
    """Persist one exact source identity, version, and trace chain."""

    source_file_version_id = str(
        descriptor["engine_source_file_version_id"]
    )
    source_identity_hash = str(
        descriptor["engine_source_identity_hash"]
    )
    await _insert_frozen_source_identity(
        database,
        schema=schema,
        descriptor=descriptor,
        source_identity_hash=source_identity_hash,
    )
    await _insert_frozen_source_version(
        database,
        schema=schema,
        descriptor=descriptor,
        source_file_version_id=source_file_version_id,
        source_identity_hash=source_identity_hash,
    )
    await _insert_frozen_source_trace(
        database,
        schema=schema,
        source_file_version_id=source_file_version_id,
        trace_hash=trace_hash,
    )


async def _insert_frozen_source_identity(
    database: Database,
    *,
    schema: str,
    descriptor: dict[str, object],
    source_identity_hash: str,
) -> None:
    """Insert the canonical source identity used by frozen evidence."""

    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_source_identity
            (source_identity_hash, source_type, canonical_url)
        VALUES (
            :source_identity_hash, :source_type, :canonical_url
        )
        """,
        source_identity_hash=source_identity_hash,
        source_type=str(descriptor["source_type"]),
        canonical_url=str(descriptor["canonical_url"]),
    )


async def _insert_frozen_source_version(
    database: Database,
    *,
    schema: str,
    descriptor: dict[str, object],
    source_file_version_id: str,
    source_identity_hash: str,
) -> None:
    """Insert the exact retained source-version byte declaration."""

    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_source_file_version
            (source_file_version_id, source_identity_hash, raw_sha256,
             logical_sha256, content_length, etag, last_modified,
             verification_mode, payload)
        VALUES (
            :source_file_version_id, :source_identity_hash, :raw_sha256,
            :logical_sha256, :content_length, :etag, :last_modified,
            :verification_mode, CAST(:payload AS jsonb)
        )
        """,
        source_file_version_id=source_file_version_id,
        source_identity_hash=source_identity_hash,
        raw_sha256=str(descriptor["raw_sha256"]),
        logical_sha256=str(descriptor["logical_sha256"]),
        content_length=int(descriptor["content_length"]),
        etag=descriptor["etag"],
        last_modified=descriptor["last_modified"],
        verification_mode="downloaded",
        payload=json.dumps(
            {
                "raw_byte_count": descriptor["content_length"],
                "logical_hash_deferred": descriptor[
                    "logical_hash_deferred"
                ],
            },
            sort_keys=True,
        ),
    )


async def _insert_frozen_source_trace(
    database: Database,
    *,
    schema: str,
    source_file_version_id: str,
    trace_hash: str,
) -> None:
    """Bind the source-version row to its immutable trace hash."""

    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_source_trace
            (source_trace_hash, source_file_version_id)
        VALUES (:source_trace_hash, :source_file_version_id)
        """,
        source_trace_hash=trace_hash,
        source_file_version_id=source_file_version_id,
    )


async def _seed_frozen_snapshot_source(
    database: Database,
    *,
    schema: str,
    snapshot_id: str,
    source_key: int,
    descriptor: dict[str, object],
    trace_hash: str,
    trace_set_hash: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_source_trace_set
            (source_trace_set_hash, source_trace_hashes)
        VALUES (
            :source_trace_set_hash,
            CAST(:source_trace_hashes AS varchar[])
        )
        """,
        source_trace_set_hash=trace_set_hash,
        source_trace_hashes=[trace_hash],
    )
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_source
            (snapshot_id, source_key, raw_container_sha256,
             source_trace_set_hash)
        VALUES (
            :snapshot_id, :source_key, :raw_sha256,
            :source_trace_set_hash
        )
        """,
        snapshot_id=snapshot_id,
        source_key=source_key,
        raw_sha256=str(descriptor["raw_sha256"]),
        source_trace_set_hash=trace_set_hash,
    )


def _frozen_candidate_params(batch: _FrozenScanBatch) -> dict[str, object]:
    return normalize_protected_frozen_rate_params(
        {
            "source_file_import_id": "frozen-multipart-e2e-001",
            "import_id": "frozen-multipart-e2e-001",
            "source_key": "synthetic-source",
            "import_month": "2026-07",
            "plan_ids": ["synthetic-plan"],
            "plan_market_types": ["group"],
            "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
            "frozen_rate_files": batch.descriptors,
            "frozen_rate_file_set_sha256": batch.set_digest,
            "frozen_rate_file_count": len(batch.descriptors),
        }
    )


def _frozen_candidate_manifest(
    batch: _FrozenScanBatch,
    binding: dict[str, object],
) -> dict[str, object]:
    return {
        "source_file_import_id": binding["source_file_import_id"],
        "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
        "frozen_rate_files": batch.descriptors,
        "frozen_rate_file_set_sha256": batch.set_digest,
        "frozen_rate_file_count": len(batch.descriptors),
        "frozen_rate_file_proof": batch.proof_rows,
        "frozen_rate_file_proof_sha256": frozen_rate_file_proof_sha256(
            batch.proof_rows
        ),
        "source_file_versions": [
            {
                **proof_row,
                "url": proof_row["canonical_url"],
            }
            for proof_row in batch.proof_rows
        ],
        FROZEN_RATE_FILE_BINDING_OPTION: binding,
    }


async def _complete_shared_gc_test_schema(
    database: Database,
    *,
    schema_name: str,
) -> None:
    """Add minimal unused V3 relations required by strict GC ownership checks."""

    schema = _quoted(schema_name)
    existing_rows = await database.all(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = :schema_name
        """,
        schema_name=schema_name,
    )
    existing_names = {
        str(table_record._mapping["table_name"])
        for table_record in existing_rows
    }
    columns_by_table = {
        "ptg2_v3_snapshot_binding": (
            "snapshot_id varchar(96), snapshot_key bigint"
        ),
        "ptg2_v3_snapshot_scope": (
            "snapshot_id varchar(96), snapshot_key bigint"
        ),
        "ptg2_v3_snapshot_source": (
            "snapshot_id varchar(96), snapshot_key bigint"
        ),
        "ptg2_v3_candidate_audit_attestation": (
            "snapshot_id varchar(96), snapshot_key bigint"
        ),
    }
    for table_name, column_sql in columns_by_table.items():
        if table_name in existing_names:
            continue
        await database.execute_ddl(
            f"CREATE TABLE {schema}.{_quoted(table_name)} ({column_sql})"
        )
        existing_names.add(table_name)
    for table_name in PTG2_V3_MIGRATION_OWNED_TABLE_NAMES:
        if table_name in existing_names:
            continue
        await database.execute_ddl(
            f"CREATE TABLE {schema}.{_quoted(table_name)} "
            "(snapshot_key bigint)"
        )


async def _create_failed_recovery_control_schema(
    database: Database,
    *,
    schema_name: str,
) -> None:
    """Create the minimal logical-owner tables read by exact recovery."""

    schema = _quoted(schema_name)
    for statement in (
        f"""
        CREATE TABLE {schema}.ptg2_snapshot (
            snapshot_id varchar(96) PRIMARY KEY,
            import_run_id varchar(96) NOT NULL,
            status varchar(16) NOT NULL,
            published_at timestamptz
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_import_run (
            import_run_id varchar(96) PRIMARY KEY,
            status varchar(16) NOT NULL,
            report jsonb NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_current_snapshot (
            snapshot_id varchar(96),
            previous_snapshot_id varchar(96)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_current_source_snapshot (
            snapshot_id varchar(96),
            previous_snapshot_id varchar(96)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_current_plan_source (
            snapshot_id varchar(96),
            previous_snapshot_id varchar(96)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_snapshot_pin (
            snapshot_id varchar(96)
        )
        """,
    ):
        await database.execute_ddl(statement)


@dataclass(frozen=True)
class _FailedRecoverySeed:
    schema_name: str
    schema: str
    semantic_fingerprint: bytes
    build_token: str
    snapshot_id: str
    import_run_id: str
    snapshot_key: int


def _recovery_block(object_kind: str, payload: bytes) -> SharedBlock:
    return SharedBlock(
        object_kind, 0, 0, 1, "none", len(payload), payload
    )


async def _insert_recovery_blocks(
    session,
    *,
    schema: str,
    snapshot_key: int,
    mapped_block: SharedBlock,
    target_block: SharedBlock,
) -> None:
    for physical_block in (mapped_block, target_block):
        await session.execute(
            sa.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_block
                    (block_hash, format_version, object_kind, codec,
                     entry_count, raw_byte_count, stored_byte_count, payload)
                VALUES
                    (:block_hash, 2, :object_kind, 'none', 1, 6, 6, :payload)
                """
            ),
            {
                "block_hash": physical_block.block_hash,
                "object_kind": physical_block.object_kind,
                "payload": physical_block.payload,
            },
        )
    await session.execute(
        sa.text(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_block
                (snapshot_key, object_kind, block_key, fragment_no,
                 entry_count, block_hash)
            VALUES (:snapshot_key, :object_kind, 0, 0, 1, :block_hash)
            """
        ),
        {
            "snapshot_key": snapshot_key,
            "object_kind": mapped_block.object_kind,
            "block_hash": mapped_block.block_hash,
        },
    )


async def _insert_failed_recovery_owner(
    session,
    *,
    schema: str,
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    semantic_fingerprint: bytes,
) -> None:
    await session.execute(
        sa.text(
            f"""
            INSERT INTO {schema}.ptg2_snapshot
                (snapshot_id, import_run_id, status)
            VALUES (:snapshot_id, :import_run_id, 'failed')
            """
        ),
        {"snapshot_id": snapshot_id, "import_run_id": import_run_id},
    )
    report_by_field = {
        "shared_snapshot_key": snapshot_key,
        "shared_semantic_fingerprint": semantic_fingerprint.hex(),
        "shared_layout_abandoned": False,
        "shared_layout_abandonment_deferred": True,
    }
    await session.execute(
        sa.text(
            f"""
            INSERT INTO {schema}.ptg2_import_run
                (import_run_id, status, report)
            VALUES (:import_run_id, 'failed', CAST(:report AS jsonb))
            """
        ),
        {
            "import_run_id": import_run_id,
            "report": json.dumps(report_by_field),
        },
    )


async def _seed_failed_recovery(
    database: Database,
    *,
    schema_name: str,
) -> _FailedRecoverySeed:
    """Seed one failed logical owner with an active-lease packed V4 layout."""

    schema = _quoted(schema_name)
    semantic_fingerprint = hashlib.sha256(b"owned-v4-layout").digest()
    build_token = f"owned-v4-{uuid.uuid4().hex}"
    snapshot_id = "ptg2:202607:failed-recovery"
    import_run_id = "ptg2:failed-recovery-run"
    mapped_block = _recovery_block("owned_v4_mapped_v1", b"mapped")
    target_block = _recovery_block("owned_v4_target_v1", b"target")
    async with database.transaction() as session:
        reservation = await reserve_v4_shared_layout(
            session,
            schema_name=schema_name,
            semantic_fingerprint=semantic_fingerprint,
            build_token=build_token,
        )
        await _insert_recovery_blocks(
            session,
            schema=schema,
            snapshot_key=reservation.snapshot_key,
            mapped_block=mapped_block,
            target_block=target_block,
        )
        await _insert_provider_set_rows(
            session,
            schema_name=schema_name,
            snapshot_key=reservation.snapshot_key,
            provider_sets_by_key={1: _global(1, 1)},
        )
        await publish_v4_snapshot_maps(
            session,
            schema_name=schema_name,
            snapshot_key=reservation.snapshot_key,
            build_token=build_token,
            representation="direct_v1",
            references=(target_block.reference(),),
            max_coordinates_per_pack=1,
        )
        await _insert_failed_recovery_owner(
            session,
            schema=schema,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            snapshot_key=reservation.snapshot_key,
            semantic_fingerprint=semantic_fingerprint,
        )
    return _FailedRecoverySeed(
        schema_name=schema_name,
        schema=schema,
        semantic_fingerprint=semantic_fingerprint,
        build_token=build_token,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        snapshot_key=reservation.snapshot_key,
    )


async def _assert_cross_key_binding_rejected(
    database: Database,
    seed: _FailedRecoverySeed,
) -> None:
    """Prove a binding on another key still fences the logical snapshot."""

    await database.status(
        f"""
        INSERT INTO {seed.schema}.ptg2_v3_snapshot_binding
            (snapshot_id, snapshot_key)
        VALUES (:snapshot_id, :snapshot_key)
        """,
        snapshot_id=seed.snapshot_id,
        snapshot_key=seed.snapshot_key + 10_000,
    )
    with pytest.raises(
        recovery.PTG2V4RecoveryConflict,
        match="recovery gates did not pass",
    ):
        await recovery.plan_ptg2_v4_recovery(
            schema_name=seed.schema_name,
            snapshot_id=seed.snapshot_id,
            import_run_id=seed.import_run_id,
            snapshot_key=seed.snapshot_key,
        )
    await database.status(
        f"""
        DELETE FROM {seed.schema}.ptg2_v3_snapshot_binding
         WHERE snapshot_id = :snapshot_id
        """,
        snapshot_id=seed.snapshot_id,
    )


async def _assert_recovery_replay(
    seed: _FailedRecoverySeed,
    *,
    plan_digest: str,
) -> None:
    """Prove response loss can replay the durable result exactly."""

    replay_plan_by_field = await recovery.plan_ptg2_v4_recovery(
        schema_name=seed.schema_name,
        snapshot_id=seed.snapshot_id,
        import_run_id=seed.import_run_id,
        snapshot_key=seed.snapshot_key,
    )
    assert replay_plan_by_field["executed"] is True
    assert replay_plan_by_field["idempotent"] is True
    assert replay_plan_by_field["plan_digest"] == plan_digest
    replay_by_field = await recovery.recover_ptg2_v4_layout(
        schema_name=seed.schema_name,
        snapshot_id=seed.snapshot_id,
        import_run_id=seed.import_run_id,
        snapshot_key=seed.snapshot_key,
        expected_plan_digest=plan_digest,
    )
    assert replay_by_field["executed"] is True
    assert replay_by_field["idempotent"] is True
    with pytest.raises(
        recovery.PTG2V4RecoveryConflict,
        match="recovery plan changed",
    ):
        await recovery.recover_ptg2_v4_layout(
            schema_name=seed.schema_name,
            snapshot_id=seed.snapshot_id,
            import_run_id=seed.import_run_id,
            snapshot_key=seed.snapshot_key,
            expected_plan_digest="f" * 64,
        )


async def _recover_failed_seed(
    database: Database,
    seed: _FailedRecoverySeed,
) -> int:
    """Recover one active-lease seed and prove every exact fence."""

    cas_count_before = int(
        await database.scalar(
            f"SELECT COUNT(*) FROM {seed.schema}.ptg2_v3_block"
        )
        or 0
    )
    assert await database.scalar(
        f"""
        SELECT lease_until > transaction_timestamp()
          FROM {seed.schema}.ptg2_v3_snapshot_layout
         WHERE snapshot_key = :snapshot_key
        """,
        snapshot_key=seed.snapshot_key,
    )
    async with database.acquire() as connection:
        wrong_owner = await abandon_owned_v4_layout(
            schema_name=seed.schema_name,
            snapshot_key=seed.snapshot_key,
            build_token="another-owner",
            executor=connection,
        )
    assert wrong_owner.logical_layout_count == 0
    await _assert_cross_key_binding_rejected(database, seed)
    plan_by_field = await recovery.plan_ptg2_v4_recovery(
        schema_name=seed.schema_name,
        snapshot_id=seed.snapshot_id,
        import_run_id=seed.import_run_id,
        snapshot_key=seed.snapshot_key,
    )
    assert seed.build_token not in json.dumps(plan_by_field, default=str)
    assert plan_by_field["candidate_hash_count"] == 3
    recovery_by_field = await recovery.recover_ptg2_v4_layout(
        schema_name=seed.schema_name,
        snapshot_id=seed.snapshot_id,
        import_run_id=seed.import_run_id,
        snapshot_key=seed.snapshot_key,
        expected_plan_digest=str(plan_by_field["plan_digest"]),
    )
    assert recovery_by_field["executed"] is True
    assert recovery_by_field["released_layouts"] == 1
    assert recovery_by_field["queued_candidate_hashes"] == 3
    await _assert_recovery_replay(
        seed,
        plan_digest=str(plan_by_field["plan_digest"]),
    )
    return cas_count_before


async def _assert_recovered_seed(
    database: Database,
    seed: _FailedRecoverySeed,
    cas_count_before: int,
) -> None:
    assert await database.scalar(
        f"SELECT COUNT(*) FROM {seed.schema}.ptg2_v3_block"
    ) == cas_count_before
    assert await database.scalar(
        f"SELECT COUNT(*) FROM {seed.schema}.ptg2_v3_gc_candidate"
    ) == 3
    assert await database.scalar(
        f"""
        SELECT bool_and(eligible_at > transaction_timestamp())
          FROM {seed.schema}.ptg2_v3_gc_candidate
        """
    )
    for table_name in (
        "ptg2_v3_snapshot_layout",
        "ptg2_v3_layout_fingerprint",
        "ptg2_v3_snapshot_block",
        "ptg2_v3_provider_set",
        "ptg2_v4_snapshot_map_root",
        "ptg2_v4_snapshot_map_pack",
    ):
        assert await database.scalar(
            f'SELECT COUNT(*) FROM {seed.schema}."{table_name}" '
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=seed.snapshot_key,
        ) == 0
    async with database.transaction() as session:
        replacement = await reserve_v4_shared_layout(
            session,
            schema_name=seed.schema_name,
            semantic_fingerprint=seed.semantic_fingerprint,
            build_token="replacement-owner",
        )
    assert replacement.snapshot_key != seed.snapshot_key


def _base_layout_manifest() -> dict[str, object]:
    return {
        "serving_index": {
            "arch_version": "postgres_binary_v3",
            "type": "ptg2_shared_blocks_v3",
            "storage_generation": "shared_blocks_v3",
            "provider_scope_strategy": "postgres_shared_graph",
            "shared_block_layout": "dense_shared_blocks_v3",
            "serving_binary": {
                "format": "postgres_binary_v3",
                "price_dictionary": {"preserved": True},
            },
        }
    }


async def _prove_candidates_in_postgres(
    database: Database,
    *,
    schema_name: str,
    snapshot_key: int,
    candidate_keys_by_npi: dict[int, set[int]],
) -> dict[int, tuple[int, ...]]:
    """Execute the bounded candidate graph proof against durable V4 rows."""

    serving_tables = PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=snapshot_key,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        provider_graph_v4_hot_prefix=sealed_v4_hot_prefix(),
    )
    retention_budget = CandidateAuditDecodedRetentionBudget()
    retention_budget.claim(
        candidate_v4._candidate_map_retained_bytes(candidate_keys_by_npi),
        category="the candidate provider map",
    )
    async with database.transaction() as session:
        return await candidate_v4.prove_v4_candidate_sets(
            session,
            serving_tables,
            candidate_keys_by_npi,
            retention_budget,
            schema_name=schema_name,
        )


@pytest.mark.asyncio
async def test_v4_storage_relation_lookup_accepts_bound_identifiers_on_postgres() -> None:
    """Prove the canary storage catalog lookup against real PostgreSQL."""

    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST=1 for PostgreSQL E2E")

    dsn = os.environ["HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN"]
    connection = await asyncpg.connect(dsn)
    try:
        relation_size_records = await relation_size_rows(
            connection,
            "pg_catalog",
            ("pg_class",),
        )

        assert len(relation_size_records) == 1
        assert relation_size_records[0]["relation"] == "pg_class"
        assert relation_size_records[0]["exists"] is True
        assert relation_size_records[0]["total_bytes"] > 0
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_v4_compiler_publish_seal_and_reader_are_exact_on_postgres(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Prove the exact pattern and heavy-bitmap paths through durable CAS."""

    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST=1 for PostgreSQL E2E")

    binary_path = _compiler_binary()
    assert binary_path.is_file(), f"missing V4 compiler binary: {binary_path}"
    artifacts, provider_map = _factor_fixture(tmp_path)
    compilation_started = time.perf_counter()
    compilation = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=tmp_path / "compiled-v4",
        options={"member_page_bytes": 64},
        binary_path=binary_path,
    )
    compilation_ms = (time.perf_counter() - compilation_started) * 1_000
    assert compilation.selected_layout == "pattern"
    assert compilation.observe["group_count"] == _GROUP_COUNT
    assert compilation.observe["provider_set_count"] == _SET_COUNT
    heavy_npi = next(
        bitmap_summary
        for bitmap_summary in compilation.heavy_bitmaps
        if bitmap_summary["relation"] == "npi_groups_exact"
        and bitmap_summary["owner_key"] == 0
    )
    assert int(heavy_npi["block_count"]) > 1
    reference_rows = [
        json.loads(line)
        for line in compilation.reference_manifest_path.read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    heavy_references = [
        reference_entry
        for reference_entry in reference_rows
        if reference_entry["object_kind"] == heavy_npi["object_kind"]
        and int(reference_entry["block_key"]) == 0
    ]
    assert len(heavy_references) == int(heavy_npi["block_count"])
    assert (
        sum(int(reference_entry["entry_count"]) for reference_entry in heavy_references)
        == _GROUP_COUNT
    )

    schema_name = f"ptg2_v4_e2e_{uuid.uuid4().hex}"
    schema = _quoted(schema_name)
    database = Database()
    await database.connect()
    monkeypatch.setattr(ptg2_shared_publish, "db", database)
    monkeypatch.setattr(snapshot_publish, "db", database)
    _isolate_graph_caches(monkeypatch)
    try:
        await _create_v4_test_schema(
            database,
            schema_name=schema_name,
            monkeypatch=monkeypatch,
        )
        build_token = f"v4-e2e-{uuid.uuid4().hex}"
        async with database.transaction() as session:
            reservation = await reserve_v4_shared_layout(
                session,
                schema_name=schema_name,
                semantic_fingerprint=hashlib.sha256(build_token.encode()).digest(),
                build_token=build_token,
            )
            await _insert_provider_set_rows(
                session,
                schema_name=schema_name,
                snapshot_key=reservation.snapshot_key,
                provider_sets_by_key={
                    provider_set_key: _global(1, provider_set_key)
                    for provider_set_key in range(1, _SET_COUNT + 1)
                },
            )
        publication_started = time.perf_counter()
        publication_progress: list[tuple[str, int]] = []
        publication = await snapshot_publish._publish_v4_graph(
            compilation,
            schema_name=schema_name,
            snapshot_key=reservation.snapshot_key,
            build_token=build_token,
            compressed_acquisition_bytes=1024,
            empty_npi_tin_only_normalization_count=0,
            progress_callback=lambda metric, amount: publication_progress.append(
                (metric, int(amount))
            ),
        )
        taxonomy_manifest = publication.inferred_taxonomy_candidates
        assert taxonomy_manifest["rule_count"] > 0
        assert taxonomy_manifest["observe_only_rule_count"] == 0
        assert taxonomy_manifest["member_count"] == 0
        assert await database.scalar(
            f"SELECT COUNT(*) FROM "
            f"{schema}.ptg2_v4_inferred_taxonomy_candidate "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=reservation.snapshot_key,
        ) == taxonomy_manifest["rule_count"]
        async with database.transaction() as session:
            sealed = await seal_v4_shared_layout(
                session,
                schema_name=schema_name,
                snapshot_key=reservation.snapshot_key,
                build_token=build_token,
                expected_summary=publication.map_summary,
                support_digest=publication.support_digest,
                layout_manifest=_base_layout_manifest(),
            )
        publication_ms = (time.perf_counter() - publication_started) * 1_000
        progress_by_metric: dict[str, int] = {}
        for metric, amount in publication_progress:
            progress_by_metric[metric] = progress_by_metric.get(metric, 0) + amount
        assert progress_by_metric["validated_dictionary_rows"] > 0
        assert progress_by_metric["published_dictionary_rows"] > 0
        assert progress_by_metric["publish_batches"] > 0
        assert sealed.snapshot_key == reservation.snapshot_key
        assert publication.representation == "pattern_v1"
        assert publication.mapping_count > 0
        assert publication.unique_block_count > 0
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_block "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 0
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_npi_scope "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 0
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v4_npi_scope "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 1
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_provider_set "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == _SET_COUNT

        expected_groups = tuple(range(_GROUP_COUNT))
        before_metrics = graph.v4_graph_metrics_snapshot()
        cold_started = time.perf_counter()
        async with database.transaction() as session:
            npi_keys = await graph.v4_npi_keys_for_values(
                session,
                snapshot_key=sealed.snapshot_key,
                npis=(_NPI,),
                schema_name=schema_name,
            )
            exact_groups = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="npi_groups_exact",
                owner_keys=(npi_keys[_NPI],),
                schema_name=schema_name,
            )
        cold_ms = (time.perf_counter() - cold_started) * 1_000
        after_cold_metrics = graph.v4_graph_metrics_snapshot()
        assert npi_keys == {_NPI: 0}
        assert exact_groups == {0: expected_groups}
        assert (
            after_cold_metrics["bitmap_owner_hits"]
            == before_metrics["bitmap_owner_hits"] + 1
        )
        assert after_cold_metrics["database_blocks"] > before_metrics["database_blocks"]

        async with database.transaction() as session:
            npi_patterns = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="npi_patterns",
                owner_keys=(0,),
                schema_name=schema_name,
            )
            pattern_groups = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="pattern_groups",
                owner_keys=npi_patterns[0],
                schema_name=schema_name,
            )
            pattern_sets = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="pattern_sets",
                owner_keys=npi_patterns[0],
                schema_name=schema_name,
            )
            set_patterns = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="set_patterns",
                owner_keys=range(1, _SET_COUNT + 1),
                schema_name=schema_name,
            )
        assert npi_patterns == {0: (0,)}
        assert pattern_groups == {0: expected_groups}
        assert pattern_sets == {0: tuple(range(1, _SET_COUNT + 1))}
        assert set_patterns == {
            provider_set_key: (0,)
            for provider_set_key in range(1, _SET_COUNT + 1)
        }
        candidate_sets = await _prove_candidates_in_postgres(
            database,
            schema_name=schema_name,
            snapshot_key=sealed.snapshot_key,
            candidate_keys_by_npi={_NPI: {1, _SET_COUNT}},
        )
        assert candidate_sets == {_NPI: (1, _SET_COUNT)}
        assert set().union(*(set(groups) for groups in pattern_groups.values())) == set(
            expected_groups
        )

        warm_durations_ms: list[float] = []
        warm_metrics_before = graph.v4_graph_metrics_snapshot()
        async with database.transaction() as session:
            for _ in range(7):
                started = time.perf_counter()
                warm = await graph.lookup_v4_relation_members(
                    session,
                    snapshot_key=sealed.snapshot_key,
                    relation="npi_groups_exact",
                    owner_keys=(0,),
                    schema_name=schema_name,
                )
                warm_durations_ms.append((time.perf_counter() - started) * 1_000)
                assert warm == {0: expected_groups}
        warm_metrics_after = graph.v4_graph_metrics_snapshot()
        warm_p50_ms = statistics.median(warm_durations_ms)
        assert warm_metrics_after["database_bytes"] == warm_metrics_before["database_bytes"]
        assert (
            warm_metrics_after["bitmap_owner_hits"]
            == warm_metrics_before["bitmap_owner_hits"] + len(warm_durations_ms)
        )
        assert warm_p50_ms < 50

        physical_bytes = int(
            await database.scalar(
                f"""
                SELECT SUM(pg_total_relation_size(relation_name::regclass))::bigint
                  FROM unnest(ARRAY[
                       '{schema_name}.ptg2_v3_block',
                       '{schema_name}.ptg2_v3_provider_group',
                       '{schema_name}.ptg2_v4_npi_scope',
                       '{schema_name}.ptg2_v4_snapshot_map_root',
                       '{schema_name}.ptg2_v4_snapshot_map_pack',
                       '{schema_name}.ptg2_v4_provider_component',
                       '{schema_name}.ptg2_v4_pattern',
                       '{schema_name}.ptg2_v4_relation_manifest',
                       '{schema_name}.ptg2_v4_heavy_owner'
                  ]) AS relation_name
                """
            )
            or 0
        )
        performance_evidence_map = {
            "block_count": compilation.block_count,
            "cold_reader_ms": round(cold_ms, 3),
            "compiler_ms": round(compilation_ms, 3),
            "coordinate_count": publication.map_summary.coordinate_count,
            "group_count": _GROUP_COUNT,
            "heavy_owner_count": len(compilation.heavy_bitmaps),
            "layout": compilation.selected_layout,
            "physical_bytes": physical_bytes,
            "publication_and_seal_ms": round(publication_ms, 3),
            "set_count": _SET_COUNT,
            "warm_reader_p50_ms": round(warm_p50_ms, 3),
            "warm_reader_max_ms": round(max(warm_durations_ms), 3),
        }
        print(
            "PTG2_V4_POSTGRES_E2E "
            + json.dumps(performance_evidence_map, sort_keys=True)
        )
        assert physical_bytes > 0
    finally:
        compilation.cleanup()
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_owned_v4_build_abandons_before_lease_without_deleting_cas(
    monkeypatch,
) -> None:
    """Prove exact failed-build recovery retains payloads and unlocks reuse."""

    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST=1 for PostgreSQL E2E")

    monkeypatch.delenv("HLTHPRT_PTG2_V3_BLOCK_GC_GRACE_SECONDS", raising=False)
    schema_name = f"ptg2_v4_abandon_{uuid.uuid4().hex}"
    schema = _quoted(schema_name)
    database = Database()
    await database.connect()
    try:
        await _create_v4_test_schema(
            database,
            schema_name=schema_name,
            monkeypatch=monkeypatch,
        )
        await _complete_shared_gc_test_schema(
            database,
            schema_name=schema_name,
        )
        await _create_failed_recovery_control_schema(
            database,
            schema_name=schema_name,
        )
        monkeypatch.setattr(recovery, "db", database)
        seed = await _seed_failed_recovery(
            database,
            schema_name=schema_name,
        )
        cas_count_before = await _recover_failed_seed(database, seed)
        await _assert_recovered_seed(database, seed, cas_count_before)
    finally:
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


async def _compile_frozen_provider_graph(
    tmp_path: Path,
    batch: _FrozenScanBatch,
):
    artifacts, provider_map, provider_sets_by_key = (
        _scan_provider_graph_fixture(tmp_path, batch.scans)
    )
    binary_path = _compiler_binary()
    assert binary_path.is_file(), f"missing V4 compiler binary: {binary_path}"
    compilation = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=tmp_path / "compiled-frozen-v4",
        binary_path=binary_path,
    )
    assert compilation.observe["provider_set_count"] == 2
    assert compilation.observe["group_count"] == 2
    return compilation, provider_sets_by_key


async def _publish_frozen_provider_graph_with_patches(
    database: Database,
    *,
    schema_name: str,
    schema: str,
    batch: _FrozenScanBatch,
    compilation,
    provider_sets_by_key: dict[int, bytes],
    monkeypatch,
) -> None:
    await _create_v4_test_schema(
        database,
        schema_name=schema_name,
        monkeypatch=monkeypatch,
    )
    await _install_frozen_candidate_test_schema(
        database,
        schema_name=schema_name,
    )
    build_token = f"frozen-v4-e2e-{uuid.uuid4().hex}"
    snapshot_key = await _reserve_frozen_layout(
        database,
        schema_name=schema_name,
        build_token=build_token,
        provider_sets_by_key=provider_sets_by_key,
    )
    publication = await snapshot_publish._publish_v4_graph(
        compilation,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        build_token=build_token,
        compressed_acquisition_bytes=sum(
            int(descriptor["content_length"])
            for descriptor in batch.descriptors
        ),
        empty_npi_tin_only_normalization_count=0,
    )
    await _seal_frozen_publication(
        database,
        schema_name=schema_name,
        schema=schema,
        snapshot_key=snapshot_key,
        build_token=build_token,
        publication=publication,
    )


async def _reserve_frozen_layout(
    database: Database,
    *,
    schema_name: str,
    build_token: str,
    provider_sets_by_key: dict[int, bytes],
) -> int:
    async with database.transaction() as session:
        reservation = await reserve_v4_shared_layout(
            session,
            schema_name=schema_name,
            semantic_fingerprint=hashlib.sha256(
                build_token.encode()
            ).digest(),
            build_token=build_token,
        )
        await _insert_provider_set_rows(
            session,
            schema_name=schema_name,
            snapshot_key=reservation.snapshot_key,
            provider_sets_by_key=provider_sets_by_key,
        )
    return reservation.snapshot_key


async def _seal_frozen_publication(
    database: Database,
    *,
    schema_name: str,
    schema: str,
    snapshot_key: int,
    build_token: str,
    publication,
) -> None:
    async with database.transaction() as session:
        sealed = await seal_v4_shared_layout(
            session,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            build_token=build_token,
            expected_summary=publication.map_summary,
            support_digest=publication.support_digest,
            layout_manifest=_base_layout_manifest(),
        )
    assert publication.representation == "direct_v1"
    assert await database.scalar(
        f"SELECT state FROM {schema}.ptg2_v4_snapshot_map_root "
        "WHERE snapshot_key = :snapshot_key",
        snapshot_key=sealed.snapshot_key,
    ) == "complete"


async def _store_frozen_candidate_binding(
    database: Database,
    *,
    schema: str,
    batch: _FrozenScanBatch,
) -> tuple[dict[str, object], dict[str, object]]:
    params_by_name = _frozen_candidate_params(batch)
    expected_binding = frozen_rate_binding_from_params(params_by_name)
    assert expected_binding is not None
    async with database.acquire() as connection:
        assert (
            await insert_or_compare_frozen_binding(
                connection,
                params_by_name,
            )
            == expected_binding
        )
    stored_binding = await database.scalar(
        f"""
        SELECT binding_payload
          FROM {schema}.ptg2_frozen_source_file_binding
         WHERE source_file_import_id = :source_file_import_id
        """,
        source_file_import_id="frozen-multipart-e2e-001",
    )
    if isinstance(stored_binding, str):
        stored_binding = json.loads(stored_binding)
    assert dict(stored_binding) == expected_binding
    return params_by_name, dict(stored_binding)


async def _assert_frozen_candidate_replay(
    *,
    snapshot_id: str,
    candidate_run_id: str,
    manifest: dict[str, object],
    stored_binding: dict[str, object],
) -> None:
    raw_sources = await ptg_candidate_audit._candidate_raw_sources(
        snapshot_id
    )
    identity = ptg_candidate_audit._validated_frozen_candidate_identity(
        manifest,
        {"frozen_binding_payload": stored_binding},
        candidate_run_id=candidate_run_id,
        raw_container_sha256=raw_sources,
    )
    replayed_identity = (
        ptg_candidate_audit._validated_frozen_candidate_identity(
            manifest,
            {"frozen_binding_payload": stored_binding},
            candidate_run_id=candidate_run_id,
            raw_container_sha256=(
                await ptg_candidate_audit._candidate_raw_sources(
                    snapshot_id
                )
            ),
        )
    )
    assert identity == replayed_identity
    assert "ptg_frozen_candidate_identity_v1" in str(identity)


async def _assert_frozen_candidate_drift_rejected(
    database: Database,
    *,
    schema: str,
    snapshot_id: str,
    candidate_run_id: str,
    manifest: dict[str, object],
    stored_binding: dict[str, object],
    drifted_descriptor: dict[str, object],
) -> None:
    """Prove live source-version and identity drift fail the audit gate."""

    async def assert_rejected() -> None:
        with pytest.raises(
            ptg_candidate_audit.CandidateAuditReleaseGateError,
            match="database source evidence changed",
        ):
            ptg_candidate_audit._validated_frozen_candidate_identity(
                manifest,
                {"frozen_binding_payload": stored_binding},
                candidate_run_id=candidate_run_id,
                raw_container_sha256=(
                    await ptg_candidate_audit._candidate_raw_sources(
                        snapshot_id
                    )
                ),
            )

    await _assert_candidate_version_length_drift(
        database,
        schema=schema,
        drifted_descriptor=drifted_descriptor,
        assert_rejected=assert_rejected,
    )
    await _assert_candidate_source_url_drift(
        database,
        schema=schema,
        drifted_descriptor=drifted_descriptor,
        assert_rejected=assert_rejected,
    )
    await _assert_candidate_raw_hash_drift(
        database,
        schema=schema,
        drifted_descriptor=drifted_descriptor,
        assert_rejected=assert_rejected,
    )


async def _assert_candidate_version_length_drift(
    database: Database,
    *,
    schema: str,
    drifted_descriptor: dict[str, object],
    assert_rejected: Callable[[], Awaitable[None]],
) -> None:
    """Change and restore a frozen source-version content length."""

    drifted_version_id = str(
        drifted_descriptor["engine_source_file_version_id"]
    )
    await database.status(
        f"""
        UPDATE {schema}.ptg2_source_file_version
           SET content_length = :content_length
         WHERE source_file_version_id = :source_file_version_id
        """,
        content_length=int(drifted_descriptor["content_length"]) + 1,
        source_file_version_id=drifted_version_id,
    )
    await assert_rejected()
    await database.status(
        f"""
        UPDATE {schema}.ptg2_source_file_version
           SET content_length = :content_length
         WHERE source_file_version_id = :source_file_version_id
        """,
        content_length=int(drifted_descriptor["content_length"]),
        source_file_version_id=drifted_version_id,
    )


async def _assert_candidate_source_url_drift(
    database: Database,
    *,
    schema: str,
    drifted_descriptor: dict[str, object],
    assert_rejected: Callable[[], Awaitable[None]],
) -> None:
    """Change and restore the canonical URL behind a frozen source."""

    source_identity_hash = str(
        drifted_descriptor["engine_source_identity_hash"]
    )
    await database.status(
        f"""
        UPDATE {schema}.ptg2_source_identity
           SET canonical_url = :canonical_url
         WHERE source_identity_hash = :source_identity_hash
        """,
        canonical_url="https://rates.example.test/changed.json.gz",
        source_identity_hash=source_identity_hash,
    )
    await assert_rejected()
    await database.status(
        f"""
        UPDATE {schema}.ptg2_source_identity
           SET canonical_url = :canonical_url
         WHERE source_identity_hash = :source_identity_hash
        """,
        canonical_url=drifted_descriptor["canonical_url"],
        source_identity_hash=source_identity_hash,
    )


async def _assert_candidate_raw_hash_drift(
    database: Database,
    *,
    schema: str,
    drifted_descriptor: dict[str, object],
    assert_rejected: Callable[[], Awaitable[None]],
) -> None:
    """Change the retained raw hash and require candidate rejection."""

    drifted_version_id = str(
        drifted_descriptor["engine_source_file_version_id"]
    )
    await database.status(
        f"""
        UPDATE {schema}.ptg2_source_file_version
           SET raw_sha256 = :raw_sha256
         WHERE source_file_version_id = :source_file_version_id
        """,
        raw_sha256="f" * 64,
        source_file_version_id=drifted_version_id,
    )
    await assert_rejected()


def _configure_frozen_e2e_database(
    monkeypatch,
    database: Database,
    schema_name: str,
) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    monkeypatch.setattr(ptg2_shared_publish, "db", database)
    monkeypatch.setattr(snapshot_publish, "db", database)
    monkeypatch.setattr(frozen_rate_binding_store, "db", database)
    monkeypatch.setattr(ptg_candidate_audit, "db", database)
    _isolate_graph_caches(monkeypatch)


async def _assert_frozen_candidate_sequence(
    database: Database,
    *,
    schema_name: str,
    schema: str,
    batch: _FrozenScanBatch,
) -> None:
    snapshot_id = "candidate-frozen-v4"
    candidate_run_id = "ptg2:frozen-multipart-e2e-001"
    _, stored_binding = await _store_frozen_candidate_binding(
        database,
        schema=schema,
        batch=batch,
    )
    await _seed_frozen_candidate_sources(
        database,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        descriptors=batch.descriptors,
    )
    manifest = _frozen_candidate_manifest(batch, stored_binding)
    await _assert_frozen_candidate_replay(
        snapshot_id=snapshot_id,
        candidate_run_id=candidate_run_id,
        manifest=manifest,
        stored_binding=stored_binding,
    )
    await _assert_frozen_candidate_drift_rejected(
        database,
        schema=schema,
        snapshot_id=snapshot_id,
        candidate_run_id=candidate_run_id,
        manifest=manifest,
        stored_binding=stored_binding,
        drifted_descriptor=batch.descriptors[1],
    )


@pytest.mark.asyncio
async def test_frozen_multipart_scans_publish_and_candidate_audit_exactly(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Prove two acquired files through Rust, V4, and the DB audit gate."""

    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST=1 for PostgreSQL E2E")

    batch = await _acquire_and_scan_frozen_parts(tmp_path, monkeypatch)
    compilation, provider_sets_by_key = await _compile_frozen_provider_graph(
        tmp_path,
        batch,
    )
    schema_name = f"ptg2_frozen_v4_e2e_{uuid.uuid4().hex}"
    schema = _quoted(schema_name)
    database = Database()
    await database.connect()
    _configure_frozen_e2e_database(
        monkeypatch,
        database,
        schema_name,
    )
    try:
        await _publish_frozen_provider_graph_with_patches(
            database,
            schema_name=schema_name,
            schema=schema,
            batch=batch,
            compilation=compilation,
            provider_sets_by_key=provider_sets_by_key,
            monkeypatch=monkeypatch,
        )
        await _assert_frozen_candidate_sequence(
            database,
            schema_name=schema_name,
            schema=schema,
            batch=batch,
        )
    finally:
        compilation.cleanup()
        try:
            await database.execute_ddl(
                f"DROP SCHEMA IF EXISTS {schema} CASCADE"
            )
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_v4_direct_layout_publishes_only_exact_direct_relations_on_postgres(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Prove the smaller direct layout remains exact in both directions."""

    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST=1 for PostgreSQL E2E")

    binary_path = _compiler_binary()
    assert binary_path.is_file(), f"missing V4 compiler binary: {binary_path}"
    artifacts, provider_map = _direct_factor_fixture(tmp_path)
    compilation = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=tmp_path / "compiled-direct-v4",
        binary_path=binary_path,
    )
    assert compilation.selected_layout == "direct"
    assert compilation.observe["pattern_count"] == 2
    relation_names = {
        str(relation["relation"])
        for relation in compilation.relation_summaries
    }
    assert {"group_sets_direct", "set_groups_direct"} <= relation_names
    assert not relation_names.intersection(graph.PTG2_V4_PATTERN_RELATIONS)

    schema_name = f"ptg2_v4_direct_e2e_{uuid.uuid4().hex}"
    schema = _quoted(schema_name)
    database = Database()
    await database.connect()
    monkeypatch.setattr(ptg2_shared_publish, "db", database)
    monkeypatch.setattr(snapshot_publish, "db", database)
    _isolate_graph_caches(monkeypatch)
    try:
        await _create_v4_test_schema(
            database,
            schema_name=schema_name,
            monkeypatch=monkeypatch,
        )
        build_token = f"v4-direct-e2e-{uuid.uuid4().hex}"
        async with database.transaction() as session:
            reservation = await reserve_v4_shared_layout(
                session,
                schema_name=schema_name,
                semantic_fingerprint=hashlib.sha256(build_token.encode()).digest(),
                build_token=build_token,
            )
            await _insert_provider_set_rows(
                session,
                schema_name=schema_name,
                snapshot_key=reservation.snapshot_key,
                provider_sets_by_key={
                    provider_set_key: _global(1, provider_set_key + 1)
                    for provider_set_key in range(2)
                },
            )
        publication = await snapshot_publish._publish_v4_graph(
            compilation,
            schema_name=schema_name,
            snapshot_key=reservation.snapshot_key,
            build_token=build_token,
            compressed_acquisition_bytes=1024,
            empty_npi_tin_only_normalization_count=0,
        )
        async with database.transaction() as session:
            sealed = await seal_v4_shared_layout(
                session,
                schema_name=schema_name,
                snapshot_key=reservation.snapshot_key,
                build_token=build_token,
                expected_summary=publication.map_summary,
                support_digest=publication.support_digest,
                layout_manifest=_base_layout_manifest(),
        )

        assert publication.representation == "direct_v1"
        assert publication.inferred_taxonomy_candidates["rule_count"] > 0
        assert (
            publication.inferred_taxonomy_candidates[
                "observe_only_rule_count"
            ]
            == 0
        )
        assert publication.inferred_taxonomy_candidates["member_count"] == 0
        assert publication.inferred_taxonomy_candidates["pattern_count"] == 0
        assert await database.scalar(
            f"SELECT representation FROM {schema}.ptg2_v4_snapshot_map_root "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == "direct_v1"
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v4_pattern "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 0
        async with database.transaction() as session:
            set_groups = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="set_groups_direct",
                owner_keys=(0, 1),
                schema_name=schema_name,
            )
            group_sets = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="group_sets_direct",
                owner_keys=(0, 1),
                schema_name=schema_name,
            )
            npi_groups = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="npi_groups_exact",
                owner_keys=(0, 1),
                schema_name=schema_name,
            )
            audit_reader = ptg2_v4_audit._V4PersistedGraphReader(
                session,
                schema_name=schema_name,
                snapshot_key=sealed.snapshot_key,
                representation="direct_v1",
                budget=ptg2_v4_audit._ReadBudget(),
            )
            audit_membership = await audit_reader.contains_edges(
                "set_groups_direct",
                ((0, 0), (0, 1), (1, 0), (1, 1)),
            )
        assert set_groups == {0: (1,), 1: (0,)}
        assert group_sets == {0: (1,), 1: (0,)}
        assert npi_groups == {0: (0,), 1: (1,)}
        assert audit_membership == {
            (0, 0): False,
            (0, 1): True,
            (1, 0): True,
            (1, 1): False,
        }
        candidate_sets = await _prove_candidates_in_postgres(
            database,
            schema_name=schema_name,
            snapshot_key=sealed.snapshot_key,
            candidate_keys_by_npi={
                1_111_111_111: {0, 1},
                2_222_222_222: {0, 1},
            },
        )
        assert candidate_sets == {
            1_111_111_111: (1,),
            2_222_222_222: (0,),
        }

        persisted_relations = {
            str(relation_row[0])
            for relation_row in await database.all(
                f"SELECT relation FROM {schema}.ptg2_v4_relation_manifest "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=sealed.snapshot_key,
            )
        }
        assert persisted_relations == relation_names
        assert not persisted_relations.intersection(graph.PTG2_V4_PATTERN_RELATIONS)
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_block "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 0
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_provider_set "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 2
    finally:
        compilation.cleanup()
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()
