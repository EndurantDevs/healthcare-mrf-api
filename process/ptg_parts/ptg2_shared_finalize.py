# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Orchestration for the strict V3 Rust run finalizer."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import resource
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from process.ptg_parts.config import (
    PTG2_V3_FINALIZER_CONFIGURED_MEMORY_DENOMINATOR,
    PTG2_V3_FINALIZER_CONFIGURED_MEMORY_NUMERATOR,
    PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES_ENV,
    PTG2_V3_FINALIZER_LEGACY_MEMORY_RECORDS_ENV,
    PTG2_V3_FINALIZER_MAX_IDENTITY_MAP_MAX_BYTES,
    PTG2_V3_FINALIZER_MAX_TOTAL_SORT_MEMORY_BYTES,
    PTG2_V3_FINALIZER_MAX_WORKERS,
    PTG2_V3_FINALIZER_MIN_IDENTITY_MAP_MAX_BYTES,
    PTG2_V3_FINALIZER_MIN_SORT_MEMORY_BYTES_PER_WORKER,
    PTG2_V3_FINALIZER_MIN_UNBUDGETED_MEMORY_BYTES,
    PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES_ENV,
    PTG2_V3_FINALIZER_WORKERS_ENV,
)
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
from process.ptg_parts.rust_scanner import (
    _ptg2_rust_scanner_binary,
    _subprocess_session_options,
    _terminate_asyncio_subprocess_group,
)


PTG2_V3_FINALIZER_FORMAT = "ptg2_v3_direct_finalizer_v3"
PTG2_V3_FINALIZER_RESOURCE_CONTRACT = "ptg2_v3_finalizer_resources_v1"
PTG2_V3_SERVING_RUN_FORMAT = "ptg2_v3_serving_run"
PTG2_V3_SERVING_RUN_VERSION = 1
PTG2_V3_SERVING_RUN_RECORD_BYTES = 52
PTG2_V3_CODE_DICTIONARY_FORMAT = "ptg2_v3_serving_code_dictionary"
PTG2_V3_CODE_DICTIONARY_VERSION = 4
PTG2_V3_SOURCE_RUN_CONTRACT_VERSION = 1
PTG2_V3_CODE_DICTIONARY_SOURCE_CONTRACT_VERSION = 1
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
_CODE_DICTIONARY_SOURCE_CONTRACT_FIELDS = frozenset(
    {
        "version",
        "source_identity",
        "source_run_contract_sha256",
        "file_count",
        "row_count",
        "byte_count",
        "files",
    }
)
_CODE_DICTIONARY_SOURCE_FILE_FIELDS = frozenset(
    {"row_count", "bytes", "sha256"}
)
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
_FINALIZER_RESOURCE_CONFIGURATION_FIELDS = frozenset(
    {
        "contract",
        "workers",
        "identity_map_max_bytes",
        "total_sort_memory_bytes",
        "sort_memory_scope",
    }
)
_FINALIZER_SORT_MEMORY_SCOPE = "process_total_across_workers_v1"
_MIB = 1024 * 1024
_CGROUP_MEMORY_LIMIT_PATHS = (
    Path("/sys/fs/cgroup/memory.max"),
    Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
)
_PRACTICALLY_UNLIMITED_BYTES = 1 << 60


@dataclass(frozen=True, slots=True)
class V3FinalizerResourceConfiguration:
    """Validated process-wide resource limits for one strict V3 finalizer."""

    workers: int
    identity_map_max_bytes: int
    total_sort_memory_bytes: int
    process_memory_limit_bytes: int | None = None

    def __post_init__(self) -> None:
        _validate_v3_finalizer_resource_configuration(self)

    def command_arguments(self) -> tuple[str, ...]:
        """Return the unambiguous Rust CLI resource contract."""

        return (
            "--workers",
            str(self.workers),
            "--identity-map-max-bytes",
            str(self.identity_map_max_bytes),
            "--total-sort-memory-bytes",
            str(self.total_sort_memory_bytes),
        )

    def contract_metadata(self) -> dict[str, Any]:
        """Return values Rust must echo to prove it honored the invocation."""

        return {
            "contract": PTG2_V3_FINALIZER_RESOURCE_CONTRACT,
            "workers": self.workers,
            "identity_map_max_bytes": self.identity_map_max_bytes,
            "total_sort_memory_bytes": self.total_sort_memory_bytes,
            "sort_memory_scope": _FINALIZER_SORT_MEMORY_SCOPE,
        }

    def validation_metadata(self) -> dict[str, Any]:
        """Return wrapper-side evidence suitable for progress and reports."""

        return {
            "configured_memory_budget_bytes": (
                self.identity_map_max_bytes + self.total_sort_memory_bytes
            ),
            "sort_memory_bytes_per_worker": (
                self.total_sort_memory_bytes // self.workers
            ),
            "finite_process_memory_limit_bytes": self.process_memory_limit_bytes,
            "configured_memory_limit_fraction": (
                f"{PTG2_V3_FINALIZER_CONFIGURED_MEMORY_NUMERATOR}/"
                f"{PTG2_V3_FINALIZER_CONFIGURED_MEMORY_DENOMINATOR}"
            ),
        }


def _required_positive_decimal_env(name: str) -> int:
    raw_value = os.getenv(name)
    normalized = str(raw_value or "").strip()
    if raw_value is None or not normalized:
        raise RuntimeError(f"strict V3 finalization requires {name}")
    if (
        not normalized.isascii()
        or not normalized.isdecimal()
        or normalized.startswith("0")
    ):
        raise RuntimeError(f"invalid {name}={raw_value!r}; expected a positive decimal")
    return int(normalized)


def _finite_process_memory_limit_bytes() -> int | None:
    limits: list[int] = []
    for path in _CGROUP_MEMORY_LIMIT_PATHS:
        try:
            raw_value = path.read_text(encoding="ascii").strip()
        except (FileNotFoundError, OSError, UnicodeError):
            continue
        if not raw_value or raw_value == "max":
            continue
        try:
            value = int(raw_value)
        except ValueError:
            continue
        if 0 < value < _PRACTICALLY_UNLIMITED_BYTES:
            limits.append(value)

    try:
        address_space_limit, _ = resource.getrlimit(resource.RLIMIT_AS)
    except (AttributeError, OSError, ValueError):
        address_space_limit = resource.RLIM_INFINITY
    if (
        address_space_limit != resource.RLIM_INFINITY
        and 0 < address_space_limit < _PRACTICALLY_UNLIMITED_BYTES
    ):
        limits.append(int(address_space_limit))
    return min(limits) if limits else None


def _validate_v3_finalizer_resource_configuration(
    configuration: V3FinalizerResourceConfiguration,
) -> None:
    """Validate static bounds and finite process-memory headroom."""

    _validate_finalizer_resource_bounds(configuration)
    _validate_finalizer_process_headroom(configuration)


def _validate_finalizer_resource_bounds(
    configuration: V3FinalizerResourceConfiguration,
) -> None:
    """Reject resource values that cannot form a bounded worker allocation."""

    workers = configuration.workers
    identity_bytes = configuration.identity_map_max_bytes
    sort_bytes = configuration.total_sort_memory_bytes
    if (
        type(workers) is not int
        or not 1 <= workers <= PTG2_V3_FINALIZER_MAX_WORKERS
    ):
        raise RuntimeError(
            f"{PTG2_V3_FINALIZER_WORKERS_ENV} must be between 1 and "
            f"{PTG2_V3_FINALIZER_MAX_WORKERS}"
        )
    if (
        type(identity_bytes) is not int
        or not PTG2_V3_FINALIZER_MIN_IDENTITY_MAP_MAX_BYTES
        <= identity_bytes
        <= PTG2_V3_FINALIZER_MAX_IDENTITY_MAP_MAX_BYTES
        or identity_bytes % _MIB
    ):
        raise RuntimeError(
            f"{PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES_ENV} must be a MiB-aligned "
            f"value between {PTG2_V3_FINALIZER_MIN_IDENTITY_MAP_MAX_BYTES} and "
            f"{PTG2_V3_FINALIZER_MAX_IDENTITY_MAP_MAX_BYTES}"
        )
    minimum_sort_bytes = (
        workers * PTG2_V3_FINALIZER_MIN_SORT_MEMORY_BYTES_PER_WORKER
    )
    if (
        type(sort_bytes) is not int
        or sort_bytes < minimum_sort_bytes
        or sort_bytes > PTG2_V3_FINALIZER_MAX_TOTAL_SORT_MEMORY_BYTES
        or sort_bytes % (workers * _MIB)
    ):
        raise RuntimeError(
            f"{PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES_ENV} must be no more than "
            f"{PTG2_V3_FINALIZER_MAX_TOTAL_SORT_MEMORY_BYTES}, divide evenly into "
            f"MiB-aligned shares for {workers} workers, and provide at least "
            f"{PTG2_V3_FINALIZER_MIN_SORT_MEMORY_BYTES_PER_WORKER} bytes per worker"
        )


def _validate_finalizer_process_headroom(
    configuration: V3FinalizerResourceConfiguration,
) -> None:
    """Reserve memory for finalizer structures outside the explicit caps."""

    process_limit = configuration.process_memory_limit_bytes
    if process_limit is None:
        return
    if type(process_limit) is not int or process_limit <= 0:
        raise RuntimeError("strict V3 finalizer process memory limit is invalid")
    configured_bytes = (
        configuration.identity_map_max_bytes
        + configuration.total_sort_memory_bytes
    )
    fraction_limit = (
        process_limit * PTG2_V3_FINALIZER_CONFIGURED_MEMORY_NUMERATOR
        // PTG2_V3_FINALIZER_CONFIGURED_MEMORY_DENOMINATOR
    )
    reserve_limit = max(
        0,
        process_limit - PTG2_V3_FINALIZER_MIN_UNBUDGETED_MEMORY_BYTES,
    )
    safe_configured_bytes = min(fraction_limit, reserve_limit)
    if configured_bytes > safe_configured_bytes:
        raise RuntimeError(
            "strict V3 finalizer configured memory exceeds its safe process budget: "
            f"identity maps plus total sort memory require {configured_bytes} bytes, "
            f"but at most {safe_configured_bytes} bytes may be configured under the "
            f"{process_limit}-byte process limit"
        )


def _load_v3_finalizer_resource_configuration(
) -> V3FinalizerResourceConfiguration:
    """Load the required strict resource contract without permissive fallbacks."""

    legacy_value = os.getenv(PTG2_V3_FINALIZER_LEGACY_MEMORY_RECORDS_ENV)
    if legacy_value is not None and str(legacy_value).strip():
        raise RuntimeError(
            f"{PTG2_V3_FINALIZER_LEGACY_MEMORY_RECORDS_ENV} is unsupported because "
            "record limits multiply with worker count; configure the process-wide "
            f"{PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES_ENV} budget instead"
        )
    return V3FinalizerResourceConfiguration(
        workers=_required_positive_decimal_env(PTG2_V3_FINALIZER_WORKERS_ENV),
        identity_map_max_bytes=_required_positive_decimal_env(
            PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES_ENV
        ),
        total_sort_memory_bytes=_required_positive_decimal_env(
            PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES_ENV
        ),
        process_memory_limit_bytes=_finite_process_memory_limit_bytes(),
    )


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
    if type(value) is not str or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise RuntimeError(f"strict V3 finalizer entry has invalid {field_name}")
    return value


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
    if type(value) is not int:
        raise RuntimeError(f"strict V3 finalizer entry has invalid {field_name}")
    if value < 0:
        raise RuntimeError(f"strict V3 finalizer entry has negative {field_name}")
    if value > (1 << 64) - 1:
        raise RuntimeError(f"strict V3 finalizer entry has oversized {field_name}")
    return value


def _validate_file_metadata(
    entry: dict[str, Any],
    *,
    label: str,
    expected_format: str,
    expected_version: int,
) -> None:
    if type(entry.get("format")) is not str or entry["format"] != expected_format:
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


def _code_dictionary_descriptor_key(
    value: Mapping[str, Any],
) -> tuple[str, int, int]:
    """Return the canonical ordering for dictionary shard descriptors."""

    return (
        str(value["sha256"]),
        int(value["row_count"]),
        int(value["bytes"]),
    )


def _attach_dictionary_entry_metadata(
    entry: dict[str, Any],
    *,
    identity: SharedPhysicalArtifactIdentity,
    source_run_digest: str,
) -> dict[str, Any]:
    """Authenticate one scanner dictionary shard and attach its source identity."""

    _validate_file_metadata(
        entry,
        label="code-dictionary",
        expected_format=PTG2_V3_CODE_DICTIONARY_FORMAT,
        expected_version=PTG2_V3_CODE_DICTIONARY_VERSION,
    )
    identity_field_presence_flags = [
        field_name in entry for field_name in _PHYSICAL_IDENTITY_FIELDS
    ]
    if any(identity_field_presence_flags) and not all(identity_field_presence_flags):
        raise RuntimeError(
            "strict V3 code-dictionary entry has an incomplete physical identity"
        )
    if all(identity_field_presence_flags) and (
        normalized_physical_artifact_identity(entry) != identity
    ):
        raise RuntimeError(
            "strict V3 code-dictionary entry has conflicting physical identity"
        )
    raw_source_run_digest = entry.get("source_run_contract_sha256")
    if raw_source_run_digest is not None and (
        _required_sha256(
            raw_source_run_digest,
            field_name="code-dictionary source_run_contract_sha256",
        )
        != source_run_digest
    ):
        raise RuntimeError(
            "strict V3 code-dictionary entry has conflicting source contract"
        )
    row_count = _required_non_negative_integer(
        entry.get("row_count"), field_name="code-dictionary row_count"
    )
    byte_count = _required_non_negative_integer(
        entry.get("bytes"), field_name="code-dictionary bytes"
    )
    file_sha256 = _sha256_file(Path(str(entry["path"])))
    entry.update(identity.as_dict())
    entry["source_run_contract_sha256"] = source_run_digest
    return {"row_count": row_count, "bytes": byte_count, "sha256": file_sha256}


def _dictionary_scanner_aggregates(
    scanner_summary: Mapping[str, Any],
) -> tuple[int, int, int]:
    """Return exact dictionary file, row, and byte counts from scanner output."""

    return (
        _required_non_negative_integer(
            scanner_summary.get("serving_code_dictionary_files"),
            field_name="scanner serving_code_dictionary_files",
        ),
        _required_non_negative_integer(
            scanner_summary.get("serving_code_dictionary_rows"),
            field_name="scanner serving_code_dictionary_rows",
        ),
        _required_non_negative_integer(
            scanner_summary.get("serving_code_dictionary_bytes"),
            field_name="scanner serving_code_dictionary_bytes",
        ),
    )


def attach_v3_dictionary_contract(
    entries: Iterable[Mapping[str, Any]],
    *,
    source_identity: Mapping[str, Any] | SharedPhysicalArtifactIdentity,
    source_run_contract_sha256: str,
    scanner_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Bind one scanner's complete dictionary shard set to its source."""

    normalized = _validated_entries(entries, label="code-dictionary")
    identity = normalized_physical_artifact_identity(source_identity)
    source_run_digest = _required_sha256(
        source_run_contract_sha256,
        field_name="code-dictionary source_run_contract_sha256",
    )
    file_descriptors = [
        _attach_dictionary_entry_metadata(
            entry,
            identity=identity,
            source_run_digest=source_run_digest,
        )
        for entry in normalized
    ]
    total_rows = sum(int(descriptor["row_count"]) for descriptor in file_descriptors)
    total_bytes = sum(int(descriptor["bytes"]) for descriptor in file_descriptors)

    expected_file_count, expected_row_count, expected_byte_count = (
        _dictionary_scanner_aggregates(scanner_summary)
    )
    if (
        expected_file_count != len(normalized)
        or expected_row_count != total_rows
        or expected_byte_count != total_bytes
    ):
        raise RuntimeError(
            "strict V3 code-dictionary files do not match the scanner aggregate summary"
        )

    file_descriptors.sort(key=_code_dictionary_descriptor_key)
    contract_map = {
        "version": PTG2_V3_CODE_DICTIONARY_SOURCE_CONTRACT_VERSION,
        "source_identity": identity.as_dict(),
        "source_run_contract_sha256": source_run_digest,
        "file_count": expected_file_count,
        "row_count": expected_row_count,
        "byte_count": expected_byte_count,
        "files": file_descriptors,
    }
    contract_sha256 = _canonical_json_sha256(contract_map)
    for entry in normalized:
        entry["code_dictionary_contract_sha256"] = contract_sha256
    normalized[0]["code_dictionary_source_contract"] = contract_map
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
        entry["sha256"] = expected_file_sha256
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
        if (
            not isinstance(raw_contract_identity, Mapping)
            or set(raw_contract_identity) != set(_PHYSICAL_IDENTITY_FIELDS)
            or dict(raw_contract_identity) != identity.as_dict()
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


def _source_contracts_by_identity(
    source_run_contracts: Sequence[Mapping[str, Any]],
) -> dict[SharedPhysicalArtifactIdentity, Mapping[str, Any]]:
    """Index complete source-run contracts by physical artifact identity."""

    contract_by_identity: dict[SharedPhysicalArtifactIdentity, Mapping[str, Any]] = {}
    for contract in source_run_contracts:
        raw_identity = contract.get("source_identity")
        if (
            not isinstance(raw_identity, Mapping)
            or set(raw_identity) != set(_PHYSICAL_IDENTITY_FIELDS)
        ):
            raise RuntimeError(
                "strict V3 source-run contract is missing its physical identity"
            )
        identity = normalized_physical_artifact_identity(raw_identity)
        if dict(raw_identity) != identity.as_dict():
            raise RuntimeError(
                "strict V3 source-run contract physical identity is not canonical"
            )
        if identity in contract_by_identity:
            raise RuntimeError("strict V3 source-run contracts repeat physical identity")
        contract_by_identity[identity] = contract
    return contract_by_identity


def _dictionary_entry_source_binding(
    entry: dict[str, Any],
    contract_by_identity: Mapping[
        SharedPhysicalArtifactIdentity, Mapping[str, Any]
    ],
) -> tuple[int, str]:
    """Return the authenticated source key and source-run digest for one shard."""

    _validate_file_metadata(
        entry,
        label="code-dictionary",
        expected_format=PTG2_V3_CODE_DICTIONARY_FORMAT,
        expected_version=PTG2_V3_CODE_DICTIONARY_VERSION,
    )
    if not all(field_name in entry for field_name in _PHYSICAL_IDENTITY_FIELDS):
        raise RuntimeError(
            "strict V3 code-dictionary entry has an incomplete physical identity"
        )
    identity = normalized_physical_artifact_identity(entry)
    try:
        source_run_contract = contract_by_identity[identity]
    except KeyError as exc:
        raise RuntimeError(
            "strict V3 code-dictionary entry is not part of the complete physical input set"
        ) from exc
    source_key = _required_non_negative_integer(
        source_run_contract.get("source_key"), field_name="source-run source_key"
    )
    source_run_digest = _required_sha256(
        source_run_contract.get("contract_sha256"),
        field_name="source-run contract_sha256",
    )
    if (
        _required_sha256(
            entry.get("source_run_contract_sha256"),
            field_name="code-dictionary source_run_contract_sha256",
        )
        != source_run_digest
    ):
        raise RuntimeError(
            "strict V3 code-dictionary entry is bound to another source contract"
        )
    return source_key, source_run_digest


def _register_dictionary_source_contract(
    entry: Mapping[str, Any],
    *,
    source_key: int,
    dictionary_contract_sha256: str,
    dictionary_contract_by_source: dict[int, dict[str, Any]],
) -> None:
    """Authenticate and retain the complete source contract carried by one shard."""

    raw_dictionary_contract = entry.get("code_dictionary_source_contract")
    if raw_dictionary_contract is None:
        return
    if not isinstance(raw_dictionary_contract, Mapping):
        raise RuntimeError(
            "strict V3 code-dictionary source contract is incompatible"
        )
    if source_key in dictionary_contract_by_source:
        raise RuntimeError(
            "strict V3 code-dictionary source contracts repeat physical identity"
        )
    dictionary_contract_map = dict(raw_dictionary_contract)
    if _canonical_json_sha256(dictionary_contract_map) != dictionary_contract_sha256:
        raise RuntimeError(
            "strict V3 code-dictionary source contract digest is invalid"
        )
    dictionary_contract_by_source[source_key] = dictionary_contract_map


def _bind_code_dictionary_entry(
    entry: dict[str, Any],
    *,
    contract_by_identity: Mapping[SharedPhysicalArtifactIdentity, Mapping[str, Any]],
    source_count: int,
    contract_digest_by_source: dict[int, str],
    dictionary_contract_by_source: dict[int, dict[str, Any]],
    entries_by_source: dict[int, list[dict[str, Any]]],
) -> int:
    """Validate one dictionary shard and bind it to one source contract."""

    source_key, source_run_contract_sha256 = _dictionary_entry_source_binding(
        entry, contract_by_identity
    )
    dictionary_contract_sha256 = _required_sha256(
        entry.get("code_dictionary_contract_sha256"),
        field_name="code_dictionary_contract_sha256",
    )
    previous_dictionary_digest = contract_digest_by_source.setdefault(
        source_key, dictionary_contract_sha256
    )
    if previous_dictionary_digest != dictionary_contract_sha256:
        raise RuntimeError(
            "strict V3 code-dictionary files disagree on their source contract"
        )
    _register_dictionary_source_contract(
        entry,
        source_key=source_key,
        dictionary_contract_sha256=dictionary_contract_sha256,
        dictionary_contract_by_source=dictionary_contract_by_source,
    )

    row_count = _required_non_negative_integer(
        entry.get("row_count"), field_name="code-dictionary row_count"
    )
    byte_count = _required_non_negative_integer(
        entry.get("bytes"), field_name="code-dictionary bytes"
    )
    file_sha256 = _sha256_file(Path(str(entry["path"])))
    entries_by_source[source_key].append(
        {
            "row_count": row_count,
            "bytes": byte_count,
            "sha256": file_sha256,
        }
    )
    for field_name in _PHYSICAL_IDENTITY_FIELDS:
        entry.pop(field_name, None)
    entry.pop("code_dictionary_source_contract", None)
    entry["source_key"] = source_key
    entry["source_count"] = source_count
    entry["source_run_contract_sha256"] = source_run_contract_sha256
    entry["code_dictionary_contract_sha256"] = dictionary_contract_sha256
    entry["sha256"] = file_sha256
    return source_key


def _source_run_contract_binding(
    source_run_contract: Mapping[str, Any],
) -> tuple[int, SharedPhysicalArtifactIdentity, str]:
    """Return the key, identity, and digest authenticated by a source contract."""

    source_key = _required_non_negative_integer(
        source_run_contract.get("source_key"), field_name="source-run source_key"
    )
    raw_source_identity = source_run_contract.get("source_identity")
    if not isinstance(raw_source_identity, Mapping):
        raise RuntimeError(
            "strict V3 source-run contract is missing its physical identity"
        )
    identity = normalized_physical_artifact_identity(raw_source_identity)
    source_run_digest = _required_sha256(
        source_run_contract.get("contract_sha256"),
        field_name="source-run contract_sha256",
    )
    return source_key, identity, source_run_digest


def _validated_dictionary_source_contract(
    source_run_contract: Mapping[str, Any],
    dictionary_contract_by_source: Mapping[int, dict[str, Any]],
) -> tuple[int, dict[str, Any]]:
    """Authenticate one dictionary source contract against its source-run contract."""

    source_key, identity, source_run_digest = _source_run_contract_binding(
        source_run_contract
    )
    dictionary_contract = dictionary_contract_by_source.get(source_key)
    if dictionary_contract is None:
        raise RuntimeError(
            "strict V3 finalizer is missing a complete code-dictionary source contract"
        )
    if set(dictionary_contract) != set(_CODE_DICTIONARY_SOURCE_CONTRACT_FIELDS):
        raise RuntimeError(
            "strict V3 code-dictionary source contract fields are incompatible"
        )
    if (
        type(dictionary_contract.get("version")) is not int
        or dictionary_contract["version"]
        != PTG2_V3_CODE_DICTIONARY_SOURCE_CONTRACT_VERSION
    ):
        raise RuntimeError(
            "strict V3 code-dictionary source contract version is incompatible"
        )
    raw_contract_identity = dictionary_contract.get("source_identity")
    if (
        not isinstance(raw_contract_identity, Mapping)
        or set(raw_contract_identity) != set(_PHYSICAL_IDENTITY_FIELDS)
        or dict(raw_contract_identity) != identity.as_dict()
    ):
        raise RuntimeError(
            "strict V3 code-dictionary source contract is bound to another physical source"
        )
    if (
        _required_sha256(
            dictionary_contract.get("source_run_contract_sha256"),
            field_name="code-dictionary source_run_contract_sha256",
        )
        != source_run_digest
    ):
        raise RuntimeError(
            "strict V3 code-dictionary source contract is bound to another source-run contract"
        )
    return source_key, dictionary_contract


def _validated_dictionary_file_descriptors(
    dictionary_contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Return canonical file descriptors from one dictionary source contract."""

    raw_expected_files = dictionary_contract.get("files")
    if not isinstance(raw_expected_files, list):
        raise RuntimeError(
            "strict V3 code-dictionary source contract is missing file digests"
        )
    expected_files: list[dict[str, Any]] = []
    for raw_descriptor in raw_expected_files:
        if not isinstance(raw_descriptor, Mapping) or set(raw_descriptor) != set(
            _CODE_DICTIONARY_SOURCE_FILE_FIELDS
        ):
            raise RuntimeError(
                "strict V3 code-dictionary source contract file fields are incompatible"
            )
        if (
            type(raw_descriptor.get("row_count")) is not int
            or type(raw_descriptor.get("bytes")) is not int
            or int(raw_descriptor["row_count"]) < 0
            or int(raw_descriptor["bytes"]) < 0
        ):
            raise RuntimeError(
                "strict V3 code-dictionary source contract file counts are incompatible"
            )
        expected_files.append(
            {
                "row_count": int(raw_descriptor["row_count"]),
                "bytes": int(raw_descriptor["bytes"]),
                "sha256": _required_sha256(
                    raw_descriptor.get("sha256"),
                    field_name="code-dictionary file sha256",
                ),
            }
        )
    if expected_files != sorted(expected_files, key=_code_dictionary_descriptor_key):
        raise RuntimeError(
            "strict V3 code-dictionary source contract file order is incompatible"
        )
    return expected_files


def _validate_dictionary_contract_files(
    dictionary_contract: Mapping[str, Any],
    observed_files: Sequence[Mapping[str, Any]],
) -> None:
    """Require exact aggregate counts and file digests for one source contract."""

    exact_integer_fields = ("file_count", "row_count", "byte_count")
    if any(
        type(dictionary_contract.get(field_name)) is not int
        or int(dictionary_contract[field_name]) < 0
        for field_name in exact_integer_fields
    ):
        raise RuntimeError(
            "strict V3 code-dictionary source contract aggregates are incompatible"
        )
    if (
        int(dictionary_contract["file_count"]) != len(observed_files)
        or int(dictionary_contract["row_count"])
        != sum(int(descriptor["row_count"]) for descriptor in observed_files)
        or int(dictionary_contract["byte_count"])
        != sum(int(descriptor["bytes"]) for descriptor in observed_files)
    ):
        raise RuntimeError(
            "strict V3 code-dictionary aggregates do not match the complete source contract"
        )
    if _validated_dictionary_file_descriptors(dictionary_contract) != list(
        observed_files
    ):
        raise RuntimeError(
            "strict V3 code-dictionary file digests do not match the complete source contract"
        )


def _prepared_dictionary_source_contract(
    source_run_contract: Mapping[str, Any],
    *,
    dictionary_contract_by_source: Mapping[int, dict[str, Any]],
    contract_digest_by_source: Mapping[int, str],
    entries_by_source: Mapping[int, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Return one fully authenticated dictionary contract for the finalizer."""

    source_key, dictionary_contract = _validated_dictionary_source_contract(
        source_run_contract, dictionary_contract_by_source
    )
    observed_files = sorted(
        entries_by_source[source_key], key=_code_dictionary_descriptor_key
    )
    _validate_dictionary_contract_files(dictionary_contract, observed_files)
    return {
        "source_key": source_key,
        "contract_sha256": contract_digest_by_source[source_key],
        **dictionary_contract,
    }


def _prepare_code_dictionary_entries(
    entries: Iterable[Mapping[str, Any]],
    *,
    source_run_contracts: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Authenticate all dictionary shards and require dense source coverage."""

    normalized = _validated_entries(entries, label="code-dictionary")
    source_count = len(source_run_contracts)
    contract_by_identity = _source_contracts_by_identity(source_run_contracts)
    contract_digest_by_source: dict[int, str] = {}
    dictionary_contract_by_source: dict[int, dict[str, Any]] = {}
    entries_by_source: dict[int, list[dict[str, Any]]] = defaultdict(list)
    observed_source_keys = {
        _bind_code_dictionary_entry(
            entry,
            contract_by_identity=contract_by_identity,
            source_count=source_count,
            contract_digest_by_source=contract_digest_by_source,
            dictionary_contract_by_source=dictionary_contract_by_source,
            entries_by_source=entries_by_source,
        )
        for entry in normalized
    }
    if observed_source_keys != set(range(source_count)):
        raise RuntimeError(
            "strict V3 finalizer requires complete dense code-dictionary source keys"
        )
    prepared_contracts = [
        _prepared_dictionary_source_contract(
            source_run_contract,
            dictionary_contract_by_source=dictionary_contract_by_source,
            contract_digest_by_source=contract_digest_by_source,
            entries_by_source=entries_by_source,
        )
        for source_run_contract in source_run_contracts
    ]
    return normalized, prepared_contracts


def _source_run_manifest_fields(
    source_run_contracts: Sequence[Mapping[str, Any]],
    prepared_serving_entries: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build authenticated serving-run fields for the finalizer manifest."""

    return {
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
    }


def _code_dictionary_manifest_fields(
    prepared_dictionary_entries: Sequence[Mapping[str, Any]],
    code_dictionary_source_contracts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build authenticated code-dictionary fields for the finalizer manifest."""

    dictionary_contracts = [
        {
            "source_key": int(entry["source_key"]),
            "row_count": int(entry["row_count"]),
            "bytes": int(entry["bytes"]),
            "sha256": str(entry["sha256"]),
            "source_run_contract_sha256": str(entry["source_run_contract_sha256"]),
            "code_dictionary_contract_sha256": str(
                entry["code_dictionary_contract_sha256"]
            ),
        }
        for entry in prepared_dictionary_entries
    ]
    return {
        "code_dictionary_source_contracts": code_dictionary_source_contracts,
        "code_dictionary_source_contract_set_sha256": _canonical_json_sha256(
            {
                "code_dictionary_source_contracts": (
                    code_dictionary_source_contracts
                )
            }
        ),
        "expected_code_dictionary_files": sum(
            int(contract["file_count"])
            for contract in code_dictionary_source_contracts
        ),
        "expected_code_dictionary_rows": sum(
            int(contract["row_count"])
            for contract in code_dictionary_source_contracts
        ),
        "expected_code_dictionary_bytes": sum(
            int(contract["byte_count"])
            for contract in code_dictionary_source_contracts
        ),
        "code_dictionary_contract_set_sha256": _canonical_json_sha256(
            {"code_dictionary_contracts": dictionary_contracts}
        ),
        "serving_run_code_dictionary_files": prepared_dictionary_entries,
    }


def write_v3_finalizer_input_manifest(
    path: str | Path,
    *,
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
    resource_configuration: V3FinalizerResourceConfiguration | None = None,
) -> Path:
    """Write the small validated manifest consumed by the Rust finalizer."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    prepared_serving_entries, source_count, source_run_contracts = _prepare_serving_entries(
        serving_run_entries,
        expected_source_identities=expected_source_identities,
    )
    (
        prepared_code_dictionary_entries,
        code_dictionary_source_contracts,
    ) = _prepare_code_dictionary_entries(
        code_dictionary_entries,
        source_run_contracts=source_run_contracts,
    )
    payload = {
        "storage_generation": PTG2_V3_SHARED_GENERATION,
        "format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "source_count": source_count,
        **_source_run_manifest_fields(source_run_contracts, prepared_serving_entries),
        **_code_dictionary_manifest_fields(
            prepared_code_dictionary_entries,
            code_dictionary_source_contracts,
        ),
    }
    if resource_configuration is not None:
        payload["resource_configuration"] = (
            resource_configuration.contract_metadata()
        )
        payload["resource_validation"] = (
            resource_configuration.validation_metadata()
        )
    with target.open("x", encoding="ascii") as output:
        json.dump(payload, output, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        output.write("\n")
    return target


def _validated_finalizer_resource_contract(
    resource_metadata: Any,
    *,
    label: str,
) -> dict[str, Any]:
    if not isinstance(resource_metadata, Mapping) or set(resource_metadata) != set(
        _FINALIZER_RESOURCE_CONFIGURATION_FIELDS
    ):
        raise RuntimeError(f"strict V3 finalizer {label} is incomplete")
    contract_map = dict(resource_metadata)
    if (
        contract_map.get("contract") != PTG2_V3_FINALIZER_RESOURCE_CONTRACT
        or contract_map.get("sort_memory_scope") != _FINALIZER_SORT_MEMORY_SCOPE
    ):
        raise RuntimeError(f"strict V3 finalizer {label} is incompatible")
    for field_name in (
        "workers",
        "identity_map_max_bytes",
        "total_sort_memory_bytes",
    ):
        value = contract_map.get(field_name)
        if type(value) is not int or value <= 0:
            raise RuntimeError(
                f"strict V3 finalizer {label} has invalid {field_name}"
            )
    return contract_map


def validate_v3_finalizer_summary(
    payload: Mapping[str, Any],
    *,
    expected_source_count: int | None = None,
    expected_resource_configuration: Mapping[str, Any] | None = None,
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
    observed_resources = summary.get("resource_configuration")
    normalized_observed_resources = (
        _validated_finalizer_resource_contract(
            observed_resources,
            label="resource configuration",
        )
        if observed_resources is not None
        else None
    )
    if expected_resource_configuration is not None:
        normalized_expected_resources = _validated_finalizer_resource_contract(
            expected_resource_configuration,
            label="expected resource configuration",
        )
        if normalized_observed_resources != normalized_expected_resources:
            raise RuntimeError(
                "strict V3 finalizer did not confirm the invoked resource configuration"
            )
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


def _remove_finalizer_attempt_path(path: Path) -> None:
    try:
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
    except FileNotFoundError:
        return


def _cleanup_unacknowledged_finalizer_attempt(
    *,
    manifest_path: Path,
    output_directory: Path,
    process_id: int | None,
) -> None:
    """Remove outputs that this invocation never returned to its caller."""

    _remove_finalizer_attempt_path(output_directory)
    if process_id is not None:
        staging_pattern = (
            f".{output_directory.name}.ptg2-finalizer-{process_id}-*.tmp"
        )
        for staging_path in output_directory.parent.glob(staging_pattern):
            _remove_finalizer_attempt_path(staging_path)
    _remove_finalizer_attempt_path(manifest_path)


async def _await_cleanup_task(task: asyncio.Task[Any]) -> Any:
    """Finish spawn/termination cleanup even if cancellation is repeated."""

    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()


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

    resource_configuration = _load_v3_finalizer_resource_configuration()
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError("strict V3 finalization requires the PTG2 Rust scanner binary")
    work_root = Path(work_directory)
    work_root.mkdir(parents=True, exist_ok=True)
    output_directory = work_root / "finalized"
    if output_directory.exists() or output_directory.is_symlink():
        raise RuntimeError(
            f"strict V3 finalizer output already exists: {output_directory}"
        )
    price_key_map_path = Path(price_key_map_input).resolve()
    if not price_key_map_path.is_file() or price_key_map_path.stat().st_size <= 0:
        raise RuntimeError("strict V3 finalization requires a non-empty price-key map")
    manifest_path = write_v3_finalizer_input_manifest(
        work_root / "scanner-summary.json",
        serving_run_entries=serving_run_entries,
        code_dictionary_entries=code_dictionary_entries,
        expected_source_identities=expected_source_identities,
        resource_configuration=resource_configuration,
    )
    manifest_payload = json.loads(manifest_path.read_text(encoding="ascii"))
    expected_source_count = int(manifest_payload["source_count"])
    command = [
        str(binary),
        "--finalize-v3-runs",
        str(output_directory),
        "--price-key-map-input",
        str(price_key_map_path),
        *resource_configuration.command_arguments(),
    ]
    for path in price_membership_inputs:
        command.extend(("--price-membership-input", str(Path(path).resolve())))
    for path in price_atom_inputs:
        command.extend(("--price-atom-input", str(Path(path).resolve())))
    command.append(str(manifest_path))
    process: asyncio.subprocess.Process | None = None
    spawn_task: asyncio.Task[asyncio.subprocess.Process] | None = None
    try:
        spawn_task = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **_subprocess_session_options(asyncio.create_subprocess_exec),
            )
        )
        process = await asyncio.shield(spawn_task)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            stderr_text = stderr.decode("utf-8", errors="replace")[-4000:]
            raise RuntimeError(
                "strict V3 Rust finalizer failed with exit "
                f"{process.returncode}: {stderr_text}"
            )
        summary = validate_v3_finalizer_summary(
            parse_v3_finalizer_stdout(stdout),
            expected_source_count=expected_source_count,
            expected_resource_configuration=(
                resource_configuration.contract_metadata()
            ),
        )
        if (
            Path(str(summary.get("output_directory") or "")).resolve()
            != output_directory.resolve()
        ):
            raise RuntimeError("strict V3 finalizer reported another output directory")
        summary["resource_validation"] = resource_configuration.validation_metadata()
        return summary
    except BaseException:
        if process is None and spawn_task is not None:
            try:
                process = await _await_cleanup_task(spawn_task)
            except BaseException:
                process = None
        process_id = int(process.pid) if process is not None else None
        try:
            if process is not None:
                termination_task = asyncio.create_task(
                    _terminate_asyncio_subprocess_group(process)
                )
                await _await_cleanup_task(termination_task)
        finally:
            _cleanup_unacknowledged_finalizer_attempt(
                manifest_path=manifest_path,
                output_directory=output_directory,
                process_id=process_id,
            )
        raise


__all__ = [
    "PTG2_V3_CODE_DICTIONARY_SOURCE_CONTRACT_VERSION",
    "PTG2_V3_FINALIZER_FORMAT",
    "PTG2_V3_FINALIZER_RESOURCE_CONTRACT",
    "PTG2_V3_SOURCE_RUN_CONTRACT_VERSION",
    "V3FinalizerResourceConfiguration",
    "attach_v3_dictionary_contract",
    "attach_v3_source_run_contract",
    "parse_v3_finalizer_stdout",
    "run_v3_direct_finalizer",
    "validate_v3_finalizer_summary",
    "write_v3_finalizer_input_manifest",
]
