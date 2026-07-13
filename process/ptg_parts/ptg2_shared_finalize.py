# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Orchestration for the strict V3 Rust run finalizer."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_COLD_LOOKUP_CONTRACT,
    PTG2_V3_SHARED_BLOCK_LAYOUT,
    PTG2_V3_SHARED_FORMAT_VERSION,
    PTG2_V3_SHARED_GENERATION,
)
from process.ptg_parts.ptg2_shared_reuse import (
    SharedPhysicalArtifactIdentity,
    deterministic_source_key_assignments,
    normalized_physical_artifact_identity,
)
from process.ptg_parts.rust_scanner import _ptg2_rust_scanner_binary


PTG2_V3_FINALIZER_FORMAT = "ptg2_v3_direct_finalizer_v3"
PTG2_V3_FINALIZER_MEMORY_RECORDS_ENV = "HLTHPRT_PTG2_V3_FINALIZER_MEMORY_RECORDS"
PTG2_V3_SERVING_RUN_FORMAT = "ptg2_v3_serving_run"
PTG2_V3_SERVING_RUN_VERSION = 1
PTG2_V3_SERVING_RUN_RECORD_BYTES = 52
PTG2_V3_CODE_DICTIONARY_FORMAT = "ptg2_v3_serving_code_dictionary"
PTG2_V3_CODE_DICTIONARY_VERSION = 4
PTG2_V3_SOURCE_RUN_CONTRACT_VERSION = 1
_SOURCE_RUN_CONTRACT_FIELDS = frozenset(
    {
        "version",
        "source_identity",
        "partition_count",
        "partition_rows",
        "file_count",
        "row_count",
        "byte_count",
        "files",
    }
)
_SOURCE_RUN_FILE_FIELDS = frozenset({"partition", "row_count", "bytes", "sha256"})
_PHYSICAL_IDENTITY_FIELDS = (
    "source_type",
    "identity_kind",
    "identity_sha256",
)
_FINALIZER_BLOCK_KINDS = {
    "serving": frozenset(
        {
            "by_code_provider_shard_v1",
            "by_code_price_page_v4",
            "provider_set_count_dictionary",
            "provider_set_codes_v3",
            "provider_set_page_v3_s2",
        }
    ),
    "price_dictionary": frozenset({"by_code_price_dictionary"}),
}


def _canonical_json_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _required_sha256(value: Any, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise RuntimeError(f"strict V3 finalizer entry has invalid {field_name}")
    return normalized


def _validated_entries(
    entries: Iterable[Mapping[str, Any]],
    *,
    label: str,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen_paths: set[Path] = set()
    for raw_entry in entries:
        entry = dict(raw_entry)
        raw_path = str(entry.get("path") or "").strip()
        if not raw_path:
            raise RuntimeError(f"strict V3 {label} entry is missing path")
        path = Path(raw_path).resolve()
        if path in seen_paths:
            raise RuntimeError(f"strict V3 {label} repeats path {path}")
        if not path.is_file() or path.stat().st_size <= 0:
            raise RuntimeError(f"strict V3 {label} file is missing or empty: {path}")
        seen_paths.add(path)
        entry["path"] = str(path)
        normalized.append(entry)
    if not normalized:
        raise RuntimeError(f"strict V3 finalizer requires at least one {label} entry")
    return normalized


def _required_non_negative_integer(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"strict V3 finalizer entry has invalid {field_name}")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"strict V3 finalizer entry has invalid {field_name}"
        ) from exc
    if normalized < 0:
        raise RuntimeError(f"strict V3 finalizer entry has negative {field_name}")
    return normalized


def _validate_file_metadata(
    entry: dict[str, Any],
    *,
    label: str,
    expected_format: str,
    expected_version: int,
) -> None:
    if str(entry.get("format") or "") != expected_format:
        raise RuntimeError(f"strict V3 {label} entry has an incompatible format")
    if _required_non_negative_integer(
        entry.get("version"), field_name=f"{label} version"
    ) != int(expected_version):
        raise RuntimeError(f"strict V3 {label} entry has an incompatible version")
    row_count = _required_non_negative_integer(
        entry.get("row_count"), field_name=f"{label} row_count"
    )
    byte_count = _required_non_negative_integer(
        entry.get("bytes"), field_name=f"{label} bytes"
    )
    path_size = Path(str(entry["path"])).stat().st_size
    if row_count <= 0 or byte_count <= 0 or byte_count != path_size:
        raise RuntimeError(f"strict V3 {label} entry metadata does not match its file")


def attach_v3_source_run_contract(
    entries: Iterable[Mapping[str, Any]],
    *,
    source_identity: Mapping[str, Any] | SharedPhysicalArtifactIdentity,
    scanner_summary: Mapping[str, Any],
    scanner_config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Bind one scanner's complete sparse run set to its physical source."""

    normalized = _validated_entries(entries, label="serving-run")
    identity = normalized_physical_artifact_identity(source_identity)
    expected_partition_count = _required_non_negative_integer(
        scanner_config.get("serving_run_partition_count"),
        field_name="scanner serving_run_partition_count",
    )
    if expected_partition_count <= 0:
        raise RuntimeError("strict V3 scanner reported no serving-run partitions")

    partition_rows = [0] * expected_partition_count
    file_descriptors: list[dict[str, Any]] = []
    total_rows = 0
    total_bytes = 0
    for entry in normalized:
        _validate_file_metadata(
            entry,
            label="serving-run",
            expected_format=PTG2_V3_SERVING_RUN_FORMAT,
            expected_version=PTG2_V3_SERVING_RUN_VERSION,
        )
        partition_count = _required_non_negative_integer(
            entry.get("partition_count"), field_name="serving-run partition_count"
        )
        partition = _required_non_negative_integer(
            entry.get("partition"), field_name="serving-run partition"
        )
        if (
            partition_count != expected_partition_count
            or partition >= expected_partition_count
        ):
            raise RuntimeError(
                "strict V3 serving-run partition metadata disagrees with scanner config"
            )
        row_count = _required_non_negative_integer(
            entry.get("row_count"), field_name="serving-run row_count"
        )
        byte_count = _required_non_negative_integer(
            entry.get("bytes"), field_name="serving-run bytes"
        )
        if row_count * PTG2_V3_SERVING_RUN_RECORD_BYTES != byte_count:
            raise RuntimeError(
                "strict V3 serving-run row and byte counts are inconsistent"
            )
        file_sha256 = _sha256_file(Path(str(entry["path"])))
        partition_rows[partition] += row_count
        total_rows += row_count
        total_bytes += byte_count
        for field_name, expected_value in identity.as_dict().items():
            observed_value = entry.setdefault(field_name, expected_value)
            if observed_value != expected_value:
                raise RuntimeError(
                    "strict V3 serving-run entry has conflicting physical identity"
                )
        entry["source_run_file_sha256"] = file_sha256
        file_descriptors.append(
            {
                "partition": partition,
                "row_count": row_count,
                "bytes": byte_count,
                "sha256": file_sha256,
            }
        )

    expected_file_count = _required_non_negative_integer(
        scanner_summary.get("serving_run_files"),
        field_name="scanner serving_run_files",
    )
    expected_row_count = _required_non_negative_integer(
        scanner_summary.get("serving_run_rows"),
        field_name="scanner serving_run_rows",
    )
    expected_byte_count = _required_non_negative_integer(
        scanner_summary.get("serving_run_bytes"),
        field_name="scanner serving_run_bytes",
    )
    if (
        expected_file_count != len(normalized)
        or expected_row_count != total_rows
        or expected_byte_count != total_bytes
    ):
        raise RuntimeError(
            "strict V3 serving-run files do not match the scanner aggregate summary"
        )

    file_descriptors.sort(
        key=lambda value: (
            int(value["partition"]),
            str(value["sha256"]),
            int(value["row_count"]),
            int(value["bytes"]),
        )
    )
    contract = {
        "version": PTG2_V3_SOURCE_RUN_CONTRACT_VERSION,
        "source_identity": identity.as_dict(),
        "partition_count": expected_partition_count,
        "partition_rows": partition_rows,
        "file_count": expected_file_count,
        "row_count": expected_row_count,
        "byte_count": expected_byte_count,
        "files": file_descriptors,
    }
    contract_sha256 = _canonical_json_sha256(contract)
    for entry in normalized:
        entry["source_run_contract_sha256"] = contract_sha256
    normalized[0]["source_run_contract"] = contract
    return normalized


def _prepare_serving_entries(
    entries: Iterable[Mapping[str, Any]],
    *,
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
) -> tuple[list[dict[str, Any]], int, list[dict[str, Any]]]:
    """Validate serving runs and return keyed entries, source count, and contracts."""

    normalized = _validated_entries(entries, label="serving-run")
    dense = deterministic_source_key_assignments(expected_source_identities)
    source_key_by_identity = {identity: source_key for source_key, identity in dense}
    source_count = len(dense)
    observed_source_keys: set[int] = set()
    partition_count: int | None = None
    entries_by_source: dict[int, list[dict[str, Any]]] = defaultdict(list)
    contract_digest_by_source: dict[int, str] = {}
    contract_by_source: dict[int, dict[str, Any]] = {}
    for entry in normalized:
        _validate_file_metadata(
            entry,
            label="serving-run",
            expected_format=PTG2_V3_SERVING_RUN_FORMAT,
            expected_version=PTG2_V3_SERVING_RUN_VERSION,
        )
        identity = normalized_physical_artifact_identity(entry)
        try:
            source_key = source_key_by_identity[identity]
        except KeyError as exc:
            raise RuntimeError(
                "strict V3 serving-run entry is not part of the complete physical input set"
            ) from exc
        observed_source_keys.add(source_key)
        observed_partition_count = _required_non_negative_integer(
            entry.get("partition_count"), field_name="serving-run partition_count"
        )
        partition = _required_non_negative_integer(
            entry.get("partition"), field_name="serving-run partition"
        )
        if observed_partition_count <= 0 or partition >= observed_partition_count:
            raise RuntimeError("strict V3 serving-run partition metadata is invalid")
        if partition_count is None:
            partition_count = observed_partition_count
        elif partition_count != observed_partition_count:
            raise RuntimeError("strict V3 serving-run partition counts are inconsistent")
        row_count = _required_non_negative_integer(
            entry.get("row_count"), field_name="serving-run row_count"
        )
        byte_count = _required_non_negative_integer(
            entry.get("bytes"), field_name="serving-run bytes"
        )
        if row_count * PTG2_V3_SERVING_RUN_RECORD_BYTES != byte_count:
            raise RuntimeError(
                "strict V3 serving-run row and byte counts are inconsistent"
            )
        expected_file_sha256 = _required_sha256(
            entry.get("source_run_file_sha256"),
            field_name="source_run_file_sha256",
        )
        if _sha256_file(Path(str(entry["path"]))) != expected_file_sha256:
            raise RuntimeError(
                "strict V3 serving-run content digest does not match its source contract"
            )
        contract_digest = _required_sha256(
            entry.get("source_run_contract_sha256"),
            field_name="source_run_contract_sha256",
        )
        previous_digest = contract_digest_by_source.setdefault(
            source_key, contract_digest
        )
        if previous_digest != contract_digest:
            raise RuntimeError(
                "strict V3 serving-run files disagree on their source contract"
            )
        raw_contract = entry.get("source_run_contract")
        if raw_contract is not None:
            if not isinstance(raw_contract, Mapping) or source_key in contract_by_source:
                raise RuntimeError(
                    "strict V3 serving-run source contract must appear exactly once"
                )
            contract = dict(raw_contract)
            if _canonical_json_sha256(contract) != contract_digest:
                raise RuntimeError(
                    "strict V3 serving-run source contract digest is invalid"
                )
            contract_by_source[source_key] = contract
        for field_name in _PHYSICAL_IDENTITY_FIELDS:
            entry.pop(field_name, None)
        entry.pop("source_run_contract", None)
        entry.pop("source_run_contract_sha256", None)
        entry.pop("source_run_file_sha256", None)
        entry["source_key"] = source_key
        entry["source_count"] = source_count
        entries_by_source[source_key].append(
            {
                "partition": partition,
                "row_count": row_count,
                "bytes": byte_count,
                "sha256": expected_file_sha256,
            }
        )
    if observed_source_keys != set(range(source_count)):
        raise RuntimeError(
            "strict V3 finalizer requires complete dense source keys before finalization"
        )

    prepared_contracts: list[dict[str, Any]] = []
    for source_key, identity in dense:
        contract = contract_by_source.get(source_key)
        if contract is None:
            raise RuntimeError(
                "strict V3 finalizer is missing a complete source-run contract"
            )
        if set(contract) != set(_SOURCE_RUN_CONTRACT_FIELDS):
            raise RuntimeError("strict V3 source-run contract fields are incompatible")
        if _required_non_negative_integer(
            contract.get("version"), field_name="source-run contract version"
        ) != PTG2_V3_SOURCE_RUN_CONTRACT_VERSION:
            raise RuntimeError("strict V3 source-run contract version is incompatible")
        raw_contract_identity = contract.get("source_identity")
        if not isinstance(raw_contract_identity, Mapping) or (
            normalized_physical_artifact_identity(raw_contract_identity) != identity
        ):
            raise RuntimeError(
                "strict V3 source-run contract is bound to another physical source"
            )
        contract_partition_count = _required_non_negative_integer(
            contract.get("partition_count"),
            field_name="source-run contract partition_count",
        )
        if contract_partition_count != partition_count:
            raise RuntimeError(
                "strict V3 source-run contract has incomplete partition coverage"
            )
        raw_partition_rows = contract.get("partition_rows")
        if not isinstance(raw_partition_rows, list) or len(raw_partition_rows) != int(
            contract_partition_count
        ):
            raise RuntimeError(
                "strict V3 source-run contract has incomplete partition coverage"
            )
        expected_partition_rows = [
            _required_non_negative_integer(
                value, field_name="source-run contract partition row count"
            )
            for value in raw_partition_rows
        ]
        observed_partition_rows = [0] * int(contract_partition_count)
        observed_files = entries_by_source[source_key]
        for descriptor in observed_files:
            observed_partition_rows[int(descriptor["partition"])] += int(
                descriptor["row_count"]
            )
        if observed_partition_rows != expected_partition_rows:
            raise RuntimeError(
                "strict V3 serving-run partition rows do not match the complete source contract"
            )

        expected_file_count = _required_non_negative_integer(
            contract.get("file_count"), field_name="source-run contract file_count"
        )
        expected_row_count = _required_non_negative_integer(
            contract.get("row_count"), field_name="source-run contract row_count"
        )
        expected_byte_count = _required_non_negative_integer(
            contract.get("byte_count"), field_name="source-run contract byte_count"
        )
        if (
            expected_file_count != len(observed_files)
            or expected_row_count != sum(
                int(descriptor["row_count"]) for descriptor in observed_files
            )
            or expected_byte_count != sum(
                int(descriptor["bytes"]) for descriptor in observed_files
            )
        ):
            raise RuntimeError(
                "strict V3 serving-run aggregates do not match the complete source contract"
            )

        raw_expected_files = contract.get("files")
        if not isinstance(raw_expected_files, list):
            raise RuntimeError("strict V3 source-run contract is missing file digests")
        expected_files: list[dict[str, Any]] = []
        for raw_descriptor in raw_expected_files:
            if not isinstance(raw_descriptor, Mapping):
                raise RuntimeError("strict V3 source-run contract has an invalid file digest")
            if set(raw_descriptor) != set(_SOURCE_RUN_FILE_FIELDS):
                raise RuntimeError(
                    "strict V3 source-run contract file fields are incompatible"
                )
            expected_files.append(
                {
                    "partition": _required_non_negative_integer(
                        raw_descriptor.get("partition"),
                        field_name="source-run file partition",
                    ),
                    "row_count": _required_non_negative_integer(
                        raw_descriptor.get("row_count"),
                        field_name="source-run file row_count",
                    ),
                    "bytes": _required_non_negative_integer(
                        raw_descriptor.get("bytes"),
                        field_name="source-run file bytes",
                    ),
                    "sha256": _required_sha256(
                        raw_descriptor.get("sha256"),
                        field_name="source-run file sha256",
                    ),
                }
            )
        def descriptor_key(value: Mapping[str, Any]) -> tuple[int, str, int, int]:
            """Return the canonical sort key for a source-run file descriptor."""

            return (
                int(value["partition"]),
                str(value["sha256"]),
                int(value["row_count"]),
                int(value["bytes"]),
            )

        if sorted(expected_files, key=descriptor_key) != sorted(
            observed_files, key=descriptor_key
        ):
            raise RuntimeError(
                "strict V3 serving-run file digests do not match the complete source contract"
            )
        prepared_contracts.append(
            {
                "source_key": source_key,
                "contract_sha256": contract_digest_by_source[source_key],
                **contract,
            }
        )
    return normalized, source_count, prepared_contracts


def _prepare_code_dictionary_entries(
    entries: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    normalized = _validated_entries(entries, label="code-dictionary")
    for entry in normalized:
        _validate_file_metadata(
            entry,
            label="code-dictionary",
            expected_format=PTG2_V3_CODE_DICTIONARY_FORMAT,
            expected_version=PTG2_V3_CODE_DICTIONARY_VERSION,
        )
    return normalized


def write_v3_finalizer_input_manifest(
    path: str | Path,
    *,
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
) -> Path:
    """Write the small validated manifest consumed by the Rust finalizer."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    prepared_serving_entries, source_count, source_run_contracts = _prepare_serving_entries(
        serving_run_entries,
        expected_source_identities=expected_source_identities,
    )
    payload = {
        "storage_generation": PTG2_V3_SHARED_GENERATION,
        "format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "source_count": source_count,
        "source_run_contracts": source_run_contracts,
        "source_run_contract_set_sha256": _canonical_json_sha256(
            {"source_run_contracts": source_run_contracts}
        ),
        "expected_serving_run_files": sum(
            int(contract["file_count"]) for contract in source_run_contracts
        ),
        "expected_serving_run_rows": sum(
            int(contract["row_count"]) for contract in source_run_contracts
        ),
        "expected_serving_run_bytes": sum(
            int(contract["byte_count"]) for contract in source_run_contracts
        ),
        "serving_run_partition_files": prepared_serving_entries,
        "serving_run_code_dictionary_files": _prepare_code_dictionary_entries(
            code_dictionary_entries,
        ),
    }
    with target.open("x", encoding="ascii") as output:
        json.dump(payload, output, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        output.write("\n")
    return target


def validate_v3_finalizer_summary(
    payload: Mapping[str, Any],
    *,
    expected_source_count: int | None = None,
) -> dict[str, Any]:
    """Validate the strict finalizer contract and return a shallow summary copy."""

    summary = dict(payload)
    if summary.get("format") != PTG2_V3_FINALIZER_FORMAT:
        raise RuntimeError("strict V3 finalizer returned an incompatible summary")
    if summary.get("storage_generation") != PTG2_V3_SHARED_GENERATION:
        raise RuntimeError("strict V3 finalizer returned another storage generation")
    if summary.get("cold_lookup_contract") != PTG2_V3_COLD_LOOKUP_CONTRACT:
        raise RuntimeError("strict V3 finalizer returned another cold lookup contract")
    if summary.get("shared_block_layout") != PTG2_V3_SHARED_BLOCK_LAYOUT:
        raise RuntimeError("strict V3 finalizer returned another shared block layout")
    source_count = _required_non_negative_integer(
        summary.get("source_count"), field_name="source_count"
    )
    if source_count <= 0 or (
        expected_source_count is not None
        and source_count != int(expected_source_count)
    ):
        raise RuntimeError("strict V3 finalizer returned an incompatible source_count")
    blocks = summary.get("blocks")
    if not isinstance(blocks, Mapping):
        raise RuntimeError("strict V3 finalizer summary is missing blocks")
    for section_name, expected_kinds in _FINALIZER_BLOCK_KINDS.items():
        section = blocks.get(section_name)
        if not isinstance(section, Mapping):
            raise RuntimeError(
                f"strict V3 finalizer summary is missing {section_name} blocks"
            )
        artifact_counts = section.get("artifact_record_counts")
        if not isinstance(artifact_counts, Mapping):
            raise RuntimeError(
                f"strict V3 finalizer {section_name} object markers are missing"
            )
        observed_kinds = {
            str(kind)
            for kind, count in artifact_counts.items()
            if _required_non_negative_integer(
                count,
                field_name=f"{section_name} artifact count",
            )
            > 0
        }
        if observed_kinds != set(expected_kinds):
            raise RuntimeError(
                f"strict V3 finalizer {section_name} object markers are incompatible"
            )
    dense_keys = summary.get("dense_keys")
    price_keys = dense_keys.get("price") if isinstance(dense_keys, Mapping) else None
    price_key_map = summary.get("price_key_map")
    if not isinstance(price_keys, Mapping) or not isinstance(price_key_map, Mapping):
        raise RuntimeError("strict V3 finalizer summary is missing its price-key contract")
    price_count = _required_non_negative_integer(
        price_keys.get("count"), field_name="price key count"
    )
    if price_count <= 0 or price_keys.get("ordering") != (
        "minimum_negotiated_rate_then_global_id_128_v1"
    ):
        raise RuntimeError("strict V3 finalizer returned incompatible dense price keys")
    if (
        _required_non_negative_integer(
            price_key_map.get("row_count"), field_name="price map row count"
        )
        != price_count
        or price_key_map.get("copy_format") != "postgresql_binary_copy"
        or price_key_map.get("keys_unique_dense_contiguous") is not True
        or price_key_map.get("source_ids_exact_match") is not True
        or price_key_map.get("dense_price_ordering")
        != "minimum_negotiated_rate_then_global_id_128_v1"
    ):
        raise RuntimeError("strict V3 finalizer returned an incompatible price-key map")
    return summary


def parse_v3_finalizer_stdout(stdout: bytes) -> dict[str, Any]:
    """Parse one length-framed finalizer summary and reject extra output."""

    header_end = stdout.find(b"\n")
    if header_end < 0:
        raise RuntimeError("strict V3 finalizer returned an incomplete frame header")
    header = stdout[:header_end]
    try:
        kind, raw_length = header.split(b"\t", 1)
        payload_length = int(raw_length)
    except (ValueError, TypeError) as exc:
        raise RuntimeError("strict V3 finalizer returned an invalid frame header") from exc
    if kind != b"v3_finalizer_summary" or payload_length < 2:
        raise RuntimeError("strict V3 finalizer returned an unexpected record kind")
    payload_start = header_end + 1
    payload_end = payload_start + payload_length
    if payload_end > len(stdout):
        raise RuntimeError("strict V3 finalizer returned a truncated summary")
    if stdout[payload_end:].strip():
        raise RuntimeError("strict V3 finalizer returned unexpected trailing output")
    try:
        payload = json.loads(stdout[payload_start:payload_end])
    except json.JSONDecodeError as exc:
        raise RuntimeError("strict V3 finalizer returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("strict V3 finalizer returned an incompatible summary")
    return validate_v3_finalizer_summary(payload)


def _optional_memory_records() -> list[str]:
    raw_value = str(os.getenv(PTG2_V3_FINALIZER_MEMORY_RECORDS_ENV) or "").strip()
    if not raw_value:
        return []
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(
            f"invalid {PTG2_V3_FINALIZER_MEMORY_RECORDS_ENV}={raw_value!r}"
        ) from exc
    if value <= 0:
        raise RuntimeError(f"{PTG2_V3_FINALIZER_MEMORY_RECORDS_ENV} must be positive")
    return ["--memory-records", str(value)]


async def run_v3_direct_finalizer(
    *,
    work_directory: str | Path,
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
    price_key_map_input: str | Path,
    price_membership_inputs: Sequence[str | Path] = (),
    price_atom_inputs: Sequence[str | Path] = (),
) -> dict[str, Any]:
    """Run the bounded Rust external-sort/finalize path without Python row materialization."""

    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError("strict V3 finalization requires the PTG2 Rust scanner binary")
    work_root = Path(work_directory)
    work_root.mkdir(parents=True, exist_ok=True)
    manifest_path = write_v3_finalizer_input_manifest(
        work_root / "scanner-summary.json",
        serving_run_entries=serving_run_entries,
        code_dictionary_entries=code_dictionary_entries,
        expected_source_identities=expected_source_identities,
    )
    manifest_payload = json.loads(manifest_path.read_text(encoding="ascii"))
    expected_source_count = int(manifest_payload["source_count"])
    output_directory = work_root / "finalized"
    price_key_map_path = Path(price_key_map_input).resolve()
    if not price_key_map_path.is_file() or price_key_map_path.stat().st_size <= 0:
        raise RuntimeError("strict V3 finalization requires a non-empty price-key map")
    command = [
        str(binary),
        "--finalize-v3-runs",
        str(output_directory),
        "--price-key-map-input",
        str(price_key_map_path),
        *_optional_memory_records(),
    ]
    for path in price_membership_inputs:
        command.extend(("--price-membership-input", str(Path(path).resolve())))
    for path in price_atom_inputs:
        command.extend(("--price-atom-input", str(Path(path).resolve())))
    command.append(str(manifest_path))
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        stderr_text = stderr.decode("utf-8", errors="replace")[-4000:]
        raise RuntimeError(
            f"strict V3 Rust finalizer failed with exit {process.returncode}: {stderr_text}"
        )
    summary = validate_v3_finalizer_summary(
        parse_v3_finalizer_stdout(stdout),
        expected_source_count=expected_source_count,
    )
    if Path(str(summary.get("output_directory") or "")).resolve() != output_directory.resolve():
        raise RuntimeError("strict V3 finalizer reported another output directory")
    return summary


__all__ = [
    "PTG2_V3_FINALIZER_FORMAT",
    "PTG2_V3_FINALIZER_MEMORY_RECORDS_ENV",
    "PTG2_V3_SOURCE_RUN_CONTRACT_VERSION",
    "attach_v3_source_run_contract",
    "parse_v3_finalizer_stdout",
    "run_v3_direct_finalizer",
    "validate_v3_finalizer_summary",
    "write_v3_finalizer_input_manifest",
]
