# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import process.uhc_retained_source_registry as source_registry
from process.uhc_retained_native import retain_source_native
from process.uhc_retained_range_manifest import (
    RANGE_CANONICALIZATION_ID,
    RANGE_CONTRACT_ID,
    RANGE_CONTRACT_VERSION,
    range_manifest_path,
    range_set_digest,
    retained_raw_path,
)
from process.uhc_retained_registry_contract import SourceBinding
from process.uhc_retained_types import RawRangeProof


PRODUCER_BUILD_ID = "ptg2_scanner-test-producer"
VERIFIER_BUILD_ID = "ptg2_scanner-test-verifier"


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def source_binding(
    artifact_sha256: str,
    artifact_byte_count: int,
    *,
    catalog_set: str = "catalog-one",
    collection_kind: str = "provider_membership",
    file_name: str | None = None,
    source_url: str | None = None,
    size_bytes: int | None | object = ...,
) -> SourceBinding:
    if collection_kind == "plan_reference":
        file_name = file_name or "JSON_PLANS_WY.json"
        source_url = source_url or (
            "https://providermrf.uhc.com/api/stream/ui/ifp/plans/"
            "JSON_PLANS_WY.json"
        )
    else:
        file_name = file_name or "JSON_Providers_AZDC.json"
        source_url = source_url or (
            "https://providermrf.uhc.com/api/stream/ui/ifp/providers/"
            "JSON_Providers_AZDC.json"
        )
    catalog_size = artifact_byte_count if size_bytes is ... else size_bytes
    catalog_modified_at = "2026-07-20T08:00:00Z"
    catalog_entry_sha256, source_file_id = (
        source_registry._expected_catalog_file_hash_pair(
            family="ifp",
            collection_kind=collection_kind,
            file_name=file_name,
            source_url=source_url,
            catalog_modified_at=catalog_modified_at,
            size_bytes=catalog_size,
        )
    )
    return SourceBinding(
        catalog_set_sha256=digest(catalog_set),
        source_file_id=source_file_id,
        family="ifp",
        collection_kind=collection_kind,
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=catalog_modified_at,
        size_bytes=catalog_size,
        catalog_entry_sha256=catalog_entry_sha256,
        artifact_sha256=artifact_sha256,
    )


def records_payload(label: str = "provider", count: int = 16) -> list[dict]:
    return [
        {
            "ordinal": ordinal,
            "label": label,
            "padding": "x" * 64,
        }
        for ordinal in range(count)
    ]


def _encoded_records(records: list[dict]) -> tuple[bytes, list[tuple[int, int]]]:
    encoded_objects = [
        json.dumps(
            record,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        for record in records
    ]
    offsets = []
    cursor = 1
    for encoded_object in encoded_objects:
        offsets.append((cursor, cursor + len(encoded_object)))
        cursor += len(encoded_object) + 1
    return b"[" + b",".join(encoded_objects) + b"]", offsets


def write_retained_fixture(
    root: Path,
    records: list[dict],
    *,
    range_count: int = 4,
    producer_build_id: str = PRODUCER_BUILD_ID,
) -> dict[str, object]:
    root.mkdir(parents=True, exist_ok=True)
    raw_bytes, offsets = _encoded_records(records)
    artifact_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    raw_path = retained_raw_path(root, artifact_sha256)
    raw_path.write_bytes(raw_bytes)
    quotient, remainder = divmod(len(records), range_count)
    ranges = []
    record_start = 0
    for range_ordinal in range(range_count):
        record_count = quotient + (1 if range_ordinal < remainder else 0)
        if record_count <= 0:
            raise ValueError("fixture needs at least one record per range")
        record_end = record_start + record_count
        raw_byte_start = offsets[record_start][0]
        raw_byte_end = offsets[record_end - 1][1]
        raw_slice = raw_bytes[raw_byte_start:raw_byte_end]
        canonical_bytes = b"".join(
            raw_bytes[start:end].replace(b"\r", b"").replace(b"\n", b"")
            + b"\n"
            for start, end in offsets[record_start:record_end]
        )
        ranges.append(
            RawRangeProof(
                artifact_sha256=artifact_sha256,
                contract_version=RANGE_CONTRACT_VERSION,
                range_count=range_count,
                range_ordinal=range_ordinal,
                raw_byte_start=raw_byte_start,
                raw_byte_end=raw_byte_end,
                raw_sha256=hashlib.sha256(raw_slice).hexdigest(),
                raw_byte_count=len(raw_slice),
                record_start=record_start,
                record_end=record_end,
                record_count=record_count,
                canonical_sha256=hashlib.sha256(canonical_bytes).hexdigest(),
                canonical_byte_count=len(canonical_bytes),
                path=str(raw_path),
            )
        )
        record_start = record_end
    range_tuple = tuple(ranges)
    range_set_sha256 = range_set_digest(
        artifact_sha256,
        len(raw_bytes),
        len(records),
        range_tuple,
    )
    manifest = {
        "contract_id": RANGE_CONTRACT_ID,
        "contract_version": RANGE_CONTRACT_VERSION,
        "canonicalization_id": RANGE_CANONICALIZATION_ID,
        "producer_build_id": producer_build_id,
        "raw_artifact": {
            "file_name": raw_path.name,
            "sha256": artifact_sha256,
            "byte_count": len(raw_bytes),
            "record_count": len(records),
        },
        "range_count": range_count,
        "ranges": [
            {
                "range_ordinal": raw_range.range_ordinal,
                "raw_byte_start": raw_range.raw_byte_start,
                "raw_byte_end": raw_range.raw_byte_end,
                "raw_byte_count": raw_range.raw_byte_count,
                "raw_sha256": raw_range.raw_sha256,
                "record_start": raw_range.record_start,
                "record_end": raw_range.record_end,
                "record_count": raw_range.record_count,
                "canonical_sha256": raw_range.canonical_sha256,
                "canonical_byte_count": raw_range.canonical_byte_count,
            }
            for raw_range in range_tuple
        ],
        "range_set_sha256": range_set_sha256,
    }
    manifest_bytes = json.dumps(
        manifest,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    manifest_path = range_manifest_path(root, artifact_sha256, range_count)
    manifest_path.write_bytes(manifest_bytes)
    return {
        "source_bytes": raw_bytes,
        "artifact_sha256": artifact_sha256,
        "artifact_byte_count": len(raw_bytes),
        "record_count": len(records),
        "raw_path": raw_path,
        "manifest_path": manifest_path,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "manifest_byte_count": len(manifest_bytes),
        "ranges": range_tuple,
        "producer_build_id": producer_build_id,
        "range_count": range_count,
    }


def native_summary(fixture: dict[str, object], **updates) -> dict[str, object]:
    summary = {
        "record_kind": "uhc_retained_summary",
        "contract_id": RANGE_CONTRACT_ID,
        "contract_version": RANGE_CONTRACT_VERSION,
        "canonicalization_id": RANGE_CANONICALIZATION_ID,
        "producer_build_id": fixture["producer_build_id"],
        "verifier_build_id": VERIFIER_BUILD_ID,
        "raw_artifact_path": str(fixture["raw_path"]),
        "raw_artifact_sha256": fixture["artifact_sha256"],
        "raw_artifact_byte_count": fixture["artifact_byte_count"],
        "record_count": fixture["record_count"],
        "range_count": fixture["range_count"],
        "manifest_path": str(fixture["manifest_path"]),
        "manifest_sha256": fixture["manifest_sha256"],
        "manifest_byte_count": fixture["manifest_byte_count"],
        "raw_reused": False,
        "manifest_reused": False,
        "timings_seconds": {"total": 0.001},
    }
    summary.update(updates)
    return summary


def install_fake_native(
    tmp_path: Path,
    monkeypatch,
    summary: dict[str, object],
) -> Path:
    summary_path = tmp_path / "native-summary.json"
    summary_path.write_text(
        json.dumps(summary, separators=(",", ":")),
        encoding="utf-8",
    )
    binary = tmp_path / "fake-ptg2-scanner"
    binary.write_text(
        "#!/usr/bin/env python3\n"
        "import os, pathlib, sys\n"
        "if len(sys.argv) != 7 or sys.argv[1] != '--uhc-retain':\n"
        "    raise SystemExit(64)\n"
        "sys.stdout.buffer.write(pathlib.Path("
        "os.environ['UHC_TEST_NATIVE_SUMMARY']).read_bytes())\n",
        encoding="utf-8",
    )
    binary.chmod(0o755)
    monkeypatch.setenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", str(binary))
    monkeypatch.setenv("UHC_TEST_NATIVE_SUMMARY", str(summary_path))
    return binary


async def native_verified_source(
    tmp_path: Path,
    monkeypatch,
    *,
    records: list[dict] | None = None,
    range_count: int = 4,
):
    retained_root = tmp_path / "retained"
    fixture = write_retained_fixture(
        retained_root,
        records or records_payload(),
        range_count=range_count,
    )
    source_path = tmp_path / "source.json"
    source_path.write_bytes(fixture["source_bytes"])
    install_fake_native(tmp_path, monkeypatch, native_summary(fixture))
    source = await retain_source_native(
        source_path=source_path,
        output_root=retained_root,
        expected_sha256=str(fixture["artifact_sha256"]),
        expected_byte_count=int(fixture["artifact_byte_count"]),
        range_count=range_count,
    )
    return fixture, source


class FakeTransaction:
    def __init__(self, connection) -> None:
        self.connection = connection
        self.rows_before = None

    async def __aenter__(self):
        self.rows_before = deepcopy(self.connection.rows_by_table)
        return self

    async def __aexit__(self, exception_type, _exception, _traceback):
        if exception_type is not None:
            self.connection.rows_by_table = self.rows_before
        return False


class FakeRegistryConnection:
    def __init__(self) -> None:
        self.rows_by_table = {
            "catalog": {},
            "raw": {},
            "layout": {},
            "binding": {},
            "range": {},
            "reference": {},
        }
        self.after_execute = None
        self.executed_statements = []

    def transaction(self):
        return FakeTransaction(self)

    def add_catalog(
        self,
        binding: SourceBinding,
        *,
        size_bytes: int | None | object = ...,
        availability: str = "published",
        catalog_support: str = "cataloged",
    ) -> None:
        catalog_size = binding.size_bytes if size_bytes is ... else size_bytes
        self.rows_by_table["catalog"][
            (binding.catalog_set_sha256, binding.source_file_id)
        ] = {
            "family": binding.family,
            "collection_kind": binding.collection_kind,
            "file_name": binding.file_name,
            "source_url": binding.source_url,
            "catalog_modified_at": binding.catalog_modified_at,
            "catalog_entry_sha256": binding.catalog_entry_sha256,
            "size_bytes": catalog_size,
            "availability": availability,
            "catalog_support": catalog_support,
        }

    async def execute(self, statement: str, *parameters):
        self.executed_statements.append(statement)
        if "INSERT INTO" not in statement:
            return "SELECT 1"
        if "provider_directory_uhc_raw_layout" in statement:
            self._insert_layout(parameters)
        elif "provider_directory_uhc_raw_artifact" in statement:
            self._insert_raw(parameters)
        elif "provider_directory_uhc_source_binding" in statement:
            self._insert_binding(parameters)
        elif "provider_directory_uhc_raw_range" in statement:
            self._insert_range(parameters)
        elif "provider_directory_uhc_artifact_reference" in statement:
            self._insert_reference(parameters)
        if self.after_execute is not None:
            self.after_execute(statement, self)
        return "INSERT 0 1"

    def _insert_raw(self, parameters) -> None:
        artifact_sha256, byte_count, storage_uri = parameters
        self.rows_by_table["raw"].setdefault(
            artifact_sha256,
            {
                "artifact_sha256": artifact_sha256,
                "byte_count": byte_count,
                "storage_uri": storage_uri,
                "status": "verified",
            },
        )

    def _insert_layout(self, parameters) -> None:
        (
            artifact_sha256,
            contract_version,
            range_count,
            record_count,
            contract_id,
            canonicalization_id,
            producer_build_id,
            range_set_sha256,
            canonical_byte_count,
            manifest_sha256,
            manifest_byte_count,
            manifest_storage_uri,
        ) = parameters
        self.rows_by_table["layout"].setdefault(
            (artifact_sha256, contract_version, range_count),
            {
                "artifact_sha256": artifact_sha256,
                "contract_version": contract_version,
                "range_count": range_count,
                "record_count": record_count,
                "contract_id": contract_id,
                "canonicalization_id": canonicalization_id,
                "producer_build_id": producer_build_id,
                "range_set_sha256": range_set_sha256,
                "canonical_byte_count": canonical_byte_count,
                "manifest_sha256": manifest_sha256,
                "manifest_byte_count": manifest_byte_count,
                "manifest_storage_uri": manifest_storage_uri,
                "status": "verified",
            },
        )

    def _insert_binding(self, parameters) -> None:
        (
            catalog_set_sha256,
            source_file_id,
            family,
            collection_kind,
            file_name,
            source_url,
            catalog_modified_at,
            size_bytes,
            catalog_entry_sha256,
            artifact_sha256,
        ) = parameters
        self.rows_by_table["binding"].setdefault(
            (catalog_set_sha256, source_file_id),
            {
                "catalog_set_sha256": catalog_set_sha256,
                "source_file_id": source_file_id,
                "family": family,
                "collection_kind": collection_kind,
                "file_name": file_name,
                "source_url": source_url,
                "catalog_modified_at": catalog_modified_at,
                "size_bytes": size_bytes,
                "catalog_entry_sha256": catalog_entry_sha256,
                "artifact_sha256": artifact_sha256,
                "released_at": None,
            },
        )

    def _insert_range(self, parameters) -> None:
        (
            artifact_sha256,
            contract_version,
            range_count,
            range_ordinal,
            raw_byte_start,
            raw_byte_end,
            raw_byte_count,
            raw_sha256,
            record_start,
            record_end,
            record_count,
            canonical_sha256,
            canonical_byte_count,
        ) = parameters
        self.rows_by_table["range"].setdefault(
            (artifact_sha256, contract_version, range_count, range_ordinal),
            {
                "artifact_sha256": artifact_sha256,
                "contract_version": contract_version,
                "range_count": range_count,
                "range_ordinal": range_ordinal,
                "raw_byte_start": raw_byte_start,
                "raw_byte_end": raw_byte_end,
                "raw_byte_count": raw_byte_count,
                "raw_sha256": raw_sha256,
                "record_start": record_start,
                "record_end": record_end,
                "record_count": record_count,
                "canonical_sha256": canonical_sha256,
                "canonical_byte_count": canonical_byte_count,
                "status": "verified",
            },
        )

    def _insert_reference(self, parameters) -> None:
        (
            content_sha256,
            artifact_kind,
            layout_artifact_sha256,
            contract_version,
            range_count,
            catalog_set_sha256,
            source_file_id,
            storage_uri,
        ) = parameters
        self.rows_by_table["reference"].setdefault(
            (
                catalog_set_sha256,
                source_file_id,
                artifact_kind,
                contract_version,
                range_count,
            ),
            {
                "content_sha256": content_sha256,
                "layout_artifact_sha256": layout_artifact_sha256,
                "storage_uri": storage_uri,
                "retain_until": None,
                "released_at": None,
            },
        )

    async def fetchrow(self, statement: str, *parameters):
        if "uhc_retained_proof_batch_v2" in statement:
            return self._persist_batch(statement, parameters)
        if "provider_directory_uhc_catalog_file" in statement:
            return self.rows_by_table["catalog"].get(tuple(parameters))
        if "provider_directory_uhc_raw_layout" in statement:
            return self.rows_by_table["layout"].get(tuple(parameters))
        if "provider_directory_uhc_raw_artifact" in statement:
            return self.rows_by_table["raw"].get(parameters[0])
        if "provider_directory_uhc_source_binding" in statement:
            return self.rows_by_table["binding"].get(tuple(parameters))
        if "provider_directory_uhc_raw_range" in statement:
            return self.rows_by_table["range"].get(tuple(parameters))
        if "provider_directory_uhc_artifact_reference" in statement:
            return self.rows_by_table["reference"].get(tuple(parameters))
        raise AssertionError(f"unexpected registry query: {statement}")

    def _persist_batch(self, statement: str, parameters):
        (
            artifact_sha256,
            byte_count,
            raw_uri,
            contract_version,
            range_count,
            record_count,
            contract_id,
            canonicalization_id,
            producer_build_id,
            range_set_sha256,
            canonical_byte_count,
            manifest_sha256,
            manifest_byte_count,
            manifest_uri,
            catalog_set_sha256,
            source_file_id,
            family,
            collection_kind,
            file_name,
            source_url,
            catalog_modified_at,
            size_bytes,
            catalog_entry_sha256,
            encoded_ranges,
            encoded_references,
            _advisory_lock_key,
        ) = parameters
        catalog_row = self.rows_by_table["catalog"].get(
            (catalog_set_sha256, source_file_id)
        )
        if catalog_row is None:
            return {
                "proof_rows": json.dumps(
                    {
                        "catalog": None,
                        "raw": None,
                        "layout": None,
                        "binding": None,
                        "ranges": [],
                        "references": [],
                    },
                    separators=(",", ":"),
                )
            }
        self._insert_raw((artifact_sha256, byte_count, raw_uri))
        self._insert_layout(
            (
                artifact_sha256,
                contract_version,
                range_count,
                record_count,
                contract_id,
                canonicalization_id,
                producer_build_id,
                range_set_sha256,
                canonical_byte_count,
                manifest_sha256,
                manifest_byte_count,
                manifest_uri,
            )
        )
        self._insert_binding(
            (
                catalog_set_sha256,
                source_file_id,
                family,
                collection_kind,
                file_name,
                source_url,
                catalog_modified_at,
                size_bytes,
                catalog_entry_sha256,
                artifact_sha256,
            )
        )
        if self.after_execute is not None:
            self.after_execute(statement, self)
        range_rows = json.loads(encoded_ranges)
        for range_row in range_rows:
            self._insert_range(
                (
                    artifact_sha256,
                    contract_version,
                    range_count,
                    range_row["range_ordinal"],
                    range_row["raw_byte_start"],
                    range_row["raw_byte_end"],
                    range_row["raw_byte_count"],
                    range_row["raw_sha256"],
                    range_row["record_start"],
                    range_row["record_end"],
                    range_row["record_count"],
                    range_row["canonical_sha256"],
                    range_row["canonical_byte_count"],
                )
            )
        reference_rows = json.loads(encoded_references)
        for reference_row in reference_rows:
            self._insert_reference(
                (
                    reference_row["content_sha256"],
                    reference_row["artifact_kind"],
                    reference_row["layout_artifact_sha256"],
                    reference_row["contract_version"],
                    reference_row["range_count"],
                    catalog_set_sha256,
                    source_file_id,
                    reference_row["storage_uri"],
                )
            )
        proof_rows = {
            "catalog": catalog_row,
            "raw": self.rows_by_table["raw"].get(artifact_sha256),
            "layout": self.rows_by_table["layout"].get(
                (artifact_sha256, contract_version, range_count)
            ),
            "binding": self.rows_by_table["binding"].get(
                (catalog_set_sha256, source_file_id)
            ),
            "ranges": [
                row
                for key, row in sorted(self.rows_by_table["range"].items())
                if key[:3] == (artifact_sha256, contract_version, range_count)
            ],
            "references": [
                {
                    "artifact_kind": key[2],
                    "contract_version": key[3],
                    "range_count": key[4],
                    **row,
                }
                for key, row in sorted(
                    self.rows_by_table["reference"].items(),
                    key=lambda item: item[0][2],
                )
                if key[:2] == (catalog_set_sha256, source_file_id)
                and (
                    key[2] == "raw"
                    or (key[3], key[4]) == (contract_version, range_count)
                )
            ],
        }
        return {"proof_rows": json.dumps(proof_rows, separators=(",", ":"))}
