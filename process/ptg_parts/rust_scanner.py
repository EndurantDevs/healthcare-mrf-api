# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Python subprocess bridge for the PTG2 Rust scanner."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import queue
import signal
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Mapping, NamedTuple

try:
    import orjson
except ImportError:  # pragma: no cover - optional acceleration
    orjson = None

from process.ptg_parts.config import (
    PTG2_DEFAULT_RUST_EVENT_QUEUE,
    PTG2_DEFAULT_RUST_SPLIT_NEGOTIATED_RATES,
    PTG2_DEFAULT_RUST_WORK_QUEUE,
    PTG2_DEFAULT_RUST_WORKERS,
    PTG2_FAST_JSON_LOADS_ENV,
    PTG2_RUST_EVENT_QUEUE_ENV,
    PTG2_RUST_REQUIRE_RELEASE_ENV,
    PTG2_RUST_SCANNER_BIN_ENV,
    PTG2_RUST_SPLIT_NEGOTIATED_RATES_ENV,
    PTG2_RUST_WORK_QUEUE_ENV,
    PTG2_RUST_WORKERS_ENV,
    _env_bool,
    _env_int,
)
from process.ptg_parts.domain import PTG2_CONFIDENCE_TIC_RATE_NPI_TIN
from process.ptg_parts.live_progress import current_live_progress_context, write_live_progress
from process.ptg_parts.progress import _scale_stage_progress_pct
from process.ptg_parts.ptg2_shared_blocks import PTG2_V3_SHARED_GENERATION
from process.ptg_parts.ptg2_shared_graph import (
    MembershipArtifact,
    SharedGraphConversionResult,
    SharedGraphDirectionMetrics,
    SharedGraphEdgeMetrics,
    SharedGraphIntegrityMetrics,
    SharedGraphShardBundle,
)
from process.ptg_parts.screen import _emit_screen_line

logger = logging.getLogger(__name__)

_SCANNER_PROGRESS_PREFIX = "PTG2_SCANNER_PROGRESS\t"
_SCANNER_PROGRESS_BASE_KEYS = {
    "path",
    "compressed_bytes",
    "total_bytes",
    "percent",
    "compressed_mib_s",
    "elapsed_seconds",
    "eta_seconds",
    "objects",
    "done",
}
_SCANNER_METRIC_RECORD_KINDS = {"scanner_config", "scanner_summary"}
_V3_SERVING_RUN_FORMAT = "ptg2_v3_serving_run"
_V3_SERVING_RUN_VERSION = 1
_V3_CODE_DICTIONARY_FORMAT = "ptg2_v3_serving_code_dictionary"
_V3_CODE_DICTIONARY_VERSION = 4
_V3_SHARED_GRAPH_SUMMARY_FORMAT = "ptg2_v3_shared_graph_summary_v1"
_V3_SHARED_GRAPH_SUMMARY_FRAME = b"v3_shared_graph_summary"
_V3_SHARED_GRAPH_SUMMARY_MAX_BYTES = 1024 * 1024
_PROCESS_GROUP_TERM_TIMEOUT_SECONDS = 2.0
_PROCESS_GROUP_KILL_TIMEOUT_SECONDS = 2.0
_PROCESS_GROUP_POLL_SECONDS = 0.02
_ASYNC_READER_QUEUE_PUT_TIMEOUT_SECONDS = 0.05
_V3_SHARED_GRAPH_OUTPUT_NAMES = {
    "block_copy_path": "graph-blocks.copy",
    "owner_copy_path": "graph-owners.copy",
    "group_copy_path": "provider-groups.copy",
    "npi_copy_path": "npi-scope.copy",
    "block_spool_path": "graph-blocks.spool",
    "owner_spool_path": "graph-owners.spool",
    "group_map_path": "provider-group.map",
    "reference_path": "graph-references.run",
}
_V3_SHARED_GRAPH_DIRECTIONS = {
    1: ("graph_npi_groups_v1", 4),
    2: ("graph_group_npis_v1", 8),
    3: ("graph_group_provider_sets_v1", 4),
    4: ("graph_provider_set_groups_v1", 4),
}
_V3_SHARED_GRAPH_SUMMARY_FIELDS = frozenset(
    {
        "format",
        "scratch_directory",
        "output_directory",
        *_V3_SHARED_GRAPH_OUTPUT_NAMES,
        "block_count",
        "owner_count",
        "provider_group_count",
        "npi_count",
        "support_digest",
        "direction_metrics",
        "edge_metrics",
        "input_byte_count",
        "raw_block_byte_count",
        "stored_block_byte_count",
        "integrity",
    }
)


class _SharedGraphExpected(NamedTuple):
    shard_count: int
    input_byte_count: int
    group_npi_input_edge_count: int
    group_provider_set_input_edge_count: int


def _subprocess_session_options(spawn: Any | None = None) -> dict[str, bool]:
    """Start native workers in a group that can be cancelled as one unit."""

    if os.name != "posix":
        return {}
    if spawn is not None:
        try:
            parameters = inspect.signature(spawn).parameters.values()
        except (TypeError, ValueError):
            parameters = ()
        if parameters and not any(
            parameter.name == "start_new_session"
            or parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        ):
            return {}
    return {"start_new_session": True}


def _is_process_group_alive(process_group_id: int) -> bool:
    if os.name != "posix":
        return False
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _signal_process_group(process: Any, signal_number: int) -> None:
    """Signal the session created for a child, falling back to its leader."""

    if os.name == "posix":
        try:
            os.killpg(int(process.pid), signal_number)
            return
        except ProcessLookupError:
            if getattr(process, "returncode", None) is not None:
                return
        except PermissionError:
            if process.poll() is not None:
                return
    try:
        process.send_signal(signal_number)
    except ProcessLookupError:
        return


def _wait_for_subprocess_group_exit(
    process: subprocess.Popen[Any], timeout: float
) -> int:
    deadline = time.monotonic() + timeout
    while True:
        return_code = process.poll()
        is_group_alive = _is_process_group_alive(int(process.pid))
        if return_code is not None and not is_group_alive:
            return int(return_code)
        if time.monotonic() >= deadline:
            raise TimeoutError
        time.sleep(_PROCESS_GROUP_POLL_SECONDS)


def _terminate_subprocess_group(process: subprocess.Popen[Any]) -> int:
    """Terminate a native process tree with bounded TERM/KILL escalation."""

    if process.poll() is not None and not _is_process_group_alive(int(process.pid)):
        return int(process.wait())
    _signal_process_group(process, signal.SIGTERM)
    try:
        return _wait_for_subprocess_group_exit(
            process, _PROCESS_GROUP_TERM_TIMEOUT_SECONDS
        )
    except TimeoutError:
        _signal_process_group(process, signal.SIGKILL)
    try:
        return _wait_for_subprocess_group_exit(
            process, _PROCESS_GROUP_KILL_TIMEOUT_SECONDS
        )
    except TimeoutError as exc:
        if process.poll() is None:
            process.kill()
        process.wait()
        raise RuntimeError("PTG2 Rust process group did not exit after SIGKILL") from exc


async def _wait_for_asyncio_subprocess_group_exit(
    process: asyncio.subprocess.Process, timeout: float
) -> int:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        return_code = process.returncode
        is_group_alive = _is_process_group_alive(int(process.pid))
        if return_code is not None and not is_group_alive:
            await process.wait()
            return int(return_code)
        if loop.time() >= deadline:
            raise TimeoutError
        await asyncio.sleep(_PROCESS_GROUP_POLL_SECONDS)


async def _terminate_asyncio_subprocess_group(
    process: asyncio.subprocess.Process,
) -> int:
    """Async counterpart of `_terminate_subprocess_group`."""

    if process.returncode is not None and not _is_process_group_alive(int(process.pid)):
        return int(await process.wait())
    _signal_process_group(process, signal.SIGTERM)
    try:
        return await _wait_for_asyncio_subprocess_group_exit(
            process, _PROCESS_GROUP_TERM_TIMEOUT_SECONDS
        )
    except TimeoutError:
        _signal_process_group(process, signal.SIGKILL)
    try:
        return await _wait_for_asyncio_subprocess_group_exit(
            process, _PROCESS_GROUP_KILL_TIMEOUT_SECONDS
        )
    except TimeoutError as exc:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise RuntimeError("PTG2 Rust process group did not exit after SIGKILL") from exc


async def _await_cancellation_resistant_cleanup(
    cleanup_task: asyncio.Task[Any],
) -> Any:
    """Finish cleanup before re-delivering cancellation to the caller."""

    pending_cancellation: asyncio.CancelledError | None = None
    while not cleanup_task.done():
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError as exc:
            pending_cancellation = exc
    result = cleanup_task.result()
    if pending_cancellation is not None:
        raise pending_cancellation
    return result


async def _cleanup_failed_shared_graph_conversion(
    *,
    process: asyncio.subprocess.Process | None,
    spawn_task: asyncio.Task[asyncio.subprocess.Process],
    output_directory: Path,
    manifest_path: Path,
) -> None:
    """Reap a converter spawned during cancellation and remove retry blockers."""

    if process is None:
        try:
            process = await asyncio.shield(spawn_task)
        except asyncio.CancelledError:
            process = None
        except Exception:
            process = None
    try:
        process_id = getattr(process, "pid", None)
        is_process_group_alive = bool(
            process_id is not None
            and _is_process_group_alive(int(process_id))
        )
        if process is not None and (
            process.returncode is None or is_process_group_alive
        ):
            await _terminate_asyncio_subprocess_group(process)
    finally:
        shutil.rmtree(output_directory, ignore_errors=True)
        manifest_path.unlink(missing_ok=True)


class _ScannerProcessControl:
    """Allow another thread to stop and reap a scanner blocked in pipe IO."""

    def __init__(self) -> None:
        self._state_lock = threading.Lock()
        self._termination_lock = threading.Lock()
        self._process: subprocess.Popen[Any] | None = None
        self._termination_requested = False

    @property
    def is_termination_requested(self) -> bool:
        """Return whether cancellation has been requested for this process."""

        with self._state_lock:
            return self._termination_requested

    def attach(self, process: subprocess.Popen[Any]) -> None:
        """Attach the spawned scanner and honor any earlier cancellation."""

        with self._state_lock:
            if self._process is not None:
                raise RuntimeError("PTG2 scanner process control is already attached")
            self._process = process
            should_terminate = self._termination_requested
        if should_terminate:
            self.terminate()

    def terminate(self) -> int | None:
        """Request termination and synchronously reap an attached process."""

        with self._state_lock:
            self._termination_requested = True
            process = self._process
        if process is None:
            return None
        with self._termination_lock:
            return _terminate_subprocess_group(process)


def _ptg2_scanner_binary_profile(path: Path) -> str:
    parts = set(path.parts)
    if "release" in parts:
        return "release"
    if "debug" in parts:
        return "debug"
    return "custom"


def _json_loads(value: str | bytes | bytearray) -> Any:
    if orjson is not None and _env_bool(PTG2_FAST_JSON_LOADS_ENV, True):
        return orjson.loads(value)
    return json.loads(value)


def _read_exactly(stream: BinaryIO, byte_count: int) -> bytes:
    chunks: list[bytes] = []
    remaining = byte_count
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _shared_graph_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise ValueError(f"duplicate JSON field {key!r}")
        payload[key] = value
    return payload


def _shared_graph_json_loads(payload: bytes) -> Any:
    try:
        return json.loads(payload, object_pairs_hook=_shared_graph_json_object)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("strict V3 shared-graph summary is invalid JSON") from exc


def _shared_graph_strict_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"strict V3 shared-graph summary has invalid {field_name}")
    if value < 0 or value > 2**63 - 1:
        raise RuntimeError(
            f"strict V3 shared-graph summary has out-of-range {field_name}"
        )
    return value


def _shared_graph_manifest_int(
    metadata: Mapping[str, Any], field_name: str, *, required: bool = True
) -> int | None:
    value = metadata.get(field_name)
    if value is None and not required:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(
            f"strict V3 graph artifact metadata has invalid {field_name}"
        )
    return value


def _shared_graph_artifact_manifest(
    artifact: MembershipArtifact,
) -> tuple[dict[str, Any], int, int]:
    """Validate a graph artifact and return its manifest entry, bytes, and members."""
    if not isinstance(artifact, MembershipArtifact):
        raise RuntimeError("strict V3 graph shard has an invalid artifact descriptor")
    if not isinstance(artifact.metadata, Mapping):
        raise RuntimeError("strict V3 graph artifact metadata must be an object")
    metadata = artifact.metadata
    path = Path(artifact.path).resolve()
    if not path.is_file():
        raise RuntimeError(f"strict V3 graph artifact is unavailable: {path}")
    record_format = metadata.get("record_format")
    if not isinstance(record_format, str) or not record_format:
        raise RuntimeError(
            "strict V3 graph artifact metadata has invalid record_format"
        )
    digest_text = metadata.get("sha256")
    if not isinstance(digest_text, str):
        raise RuntimeError("strict V3 graph artifact metadata has invalid sha256")
    try:
        digest = bytes.fromhex(digest_text)
    except ValueError as exc:
        raise RuntimeError(
            "strict V3 graph artifact metadata has invalid sha256"
        ) from exc
    if len(digest) != 32:
        raise RuntimeError("strict V3 graph artifact metadata has invalid sha256")
    byte_count = _shared_graph_manifest_int(metadata, "byte_count")
    owner_count = _shared_graph_manifest_int(metadata, "owner_count")
    member_count = _shared_graph_manifest_int(metadata, "member_count")
    assert byte_count is not None and owner_count is not None and member_count is not None
    if byte_count <= 0 or path.stat().st_size != byte_count:
        raise RuntimeError(
            f"strict V3 graph artifact byte count does not match {path}"
        )
    metadata_by_name: dict[str, Any] = {
        "record_format": record_format,
        "sha256": digest.hex(),
        "byte_count": byte_count,
        "owner_count": owner_count,
        "member_count": member_count,
    }
    member_global_count = _shared_graph_manifest_int(
        metadata, "member_global_count", required=False
    )
    if member_global_count is not None:
        metadata_by_name["member_global_count"] = member_global_count
    for field_name in ("name", "source_shard_id", "shard_id"):
        metadata_value = metadata.get(field_name)
        if metadata_value is None:
            continue
        if not isinstance(metadata_value, str):
            raise RuntimeError(
                f"strict V3 graph artifact metadata has invalid {field_name}"
            )
        metadata_by_name[field_name] = metadata_value
    return (
        {"path": str(path), "metadata": metadata_by_name},
        byte_count,
        member_count,
    )


def _shared_graph_manifest(
    *,
    shards: Iterable[SharedGraphShardBundle],
    provider_set_key_map_path: Path,
    output_directory: Path,
) -> tuple[dict[str, Any], _SharedGraphExpected]:
    normalized_shards = tuple(shards)
    if not normalized_shards:
        raise RuntimeError("strict V3 shared-graph conversion requires graph shards")
    shard_ids: set[str] = set()
    manifest_shards: list[dict[str, Any]] = []
    input_byte_count = 0
    group_npi_input_edge_count = 0
    group_provider_set_input_edge_count = 0
    for shard in sorted(normalized_shards, key=lambda item: str(item.shard_id)):
        if not isinstance(shard, SharedGraphShardBundle):
            raise RuntimeError("strict V3 shared-graph conversion has an invalid shard")
        shard_id = str(shard.shard_id or "").strip()
        if not shard_id or shard_id in shard_ids:
            raise RuntimeError(
                "strict V3 shared-graph conversion requires unique shard ids"
            )
        shard_ids.add(shard_id)
        shard_manifest_map: dict[str, Any] = {"shard_id": shard_id}
        for field_name in (
            "group_npi",
            "npi_group",
            "group_provider_set",
            "provider_set_group",
        ):
            artifact_payload, artifact_bytes, member_count = (
                _shared_graph_artifact_manifest(getattr(shard, field_name))
            )
            shard_manifest_map[field_name] = artifact_payload
            input_byte_count += artifact_bytes
            if field_name == "group_npi":
                group_npi_input_edge_count += member_count
            elif field_name == "group_provider_set":
                group_provider_set_input_edge_count += member_count
        manifest_shards.append(shard_manifest_map)
    return (
        {
            "provider_set_key_map_path": str(provider_set_key_map_path),
            "output_directory": str(output_directory),
            "shards": manifest_shards,
        },
        _SharedGraphExpected(
            shard_count=len(manifest_shards),
            input_byte_count=input_byte_count,
            group_npi_input_edge_count=group_npi_input_edge_count,
            group_provider_set_input_edge_count=(
                group_provider_set_input_edge_count
            ),
        ),
    )


def _shared_graph_summary_path(
    summary: Mapping[str, Any],
    field_name: str,
    *,
    expected_path: Path,
) -> Path:
    raw_path = summary.get(field_name)
    if not isinstance(raw_path, str) or not raw_path:
        raise RuntimeError(
            f"strict V3 shared-graph summary has invalid {field_name}"
        )
    path = Path(raw_path)
    if not path.is_absolute() or path.is_symlink() or path.resolve() != expected_path:
        raise RuntimeError(
            f"strict V3 shared-graph summary has unexpected {field_name}"
        )
    if not expected_path.is_file():
        raise RuntimeError(
            f"strict V3 shared-graph output is unavailable: {expected_path}"
        )
    return expected_path


def _shared_graph_metric_object(
    value: Any, *, field_name: str, expected_fields: frozenset[str]
) -> dict[str, Any]:
    if not isinstance(value, dict) or frozenset(value) != expected_fields:
        raise RuntimeError(
            f"strict V3 shared-graph summary has invalid {field_name}"
        )
    return value


def _shared_graph_result_from_summary(
    summary_fields: Any,
    *,
    expected_output_directory: Path,
    expected: _SharedGraphExpected,
) -> SharedGraphConversionResult:
    """Validate strict V3 graph outputs and build their conversion result."""
    if not isinstance(summary_fields, dict) or frozenset(summary_fields) != (
        _V3_SHARED_GRAPH_SUMMARY_FIELDS
    ):
        raise RuntimeError("strict V3 shared-graph summary has incompatible fields")
    if summary_fields.get("format") != _V3_SHARED_GRAPH_SUMMARY_FORMAT:
        raise RuntimeError("strict V3 shared-graph summary has incompatible format")
    output_directory = expected_output_directory.resolve()
    for field_name in ("scratch_directory", "output_directory"):
        raw_path = summary_fields.get(field_name)
        if (
            not isinstance(raw_path, str)
            or not Path(raw_path).is_absolute()
            or Path(raw_path).resolve() != output_directory
        ):
            raise RuntimeError(
                f"strict V3 shared-graph summary has unexpected {field_name}"
            )
    if not output_directory.is_dir() or output_directory.is_symlink():
        raise RuntimeError("strict V3 shared-graph output directory is unavailable")
    observed_names = {path.name for path in output_directory.iterdir()}
    if observed_names != set(_V3_SHARED_GRAPH_OUTPUT_NAMES.values()):
        raise RuntimeError("strict V3 shared-graph output set is incomplete or unexpected")
    output_path_by_name = {
        field_name: _shared_graph_summary_path(
            summary_fields,
            field_name,
            expected_path=(output_directory / output_name).resolve(),
        )
        for field_name, output_name in _V3_SHARED_GRAPH_OUTPUT_NAMES.items()
    }

    block_count = _shared_graph_strict_int(
        summary_fields.get("block_count"), "block_count"
    )
    owner_count = _shared_graph_strict_int(
        summary_fields.get("owner_count"), "owner_count"
    )
    provider_group_count = _shared_graph_strict_int(
        summary_fields.get("provider_group_count"), "provider_group_count"
    )
    npi_count = _shared_graph_strict_int(summary_fields.get("npi_count"), "npi_count")
    input_byte_count = _shared_graph_strict_int(
        summary_fields.get("input_byte_count"), "input_byte_count"
    )
    raw_block_byte_count = _shared_graph_strict_int(
        summary_fields.get("raw_block_byte_count"), "raw_block_byte_count"
    )
    stored_block_byte_count = _shared_graph_strict_int(
        summary_fields.get("stored_block_byte_count"), "stored_block_byte_count"
    )
    if input_byte_count != expected.input_byte_count:
        raise RuntimeError("strict V3 shared-graph summary input byte count changed")
    if stored_block_byte_count != raw_block_byte_count:
        raise RuntimeError("strict V3 shared-graph summary has incompatible block storage")

    digest_text = summary_fields.get("support_digest")
    if not isinstance(digest_text, str) or digest_text != digest_text.lower():
        raise RuntimeError("strict V3 shared-graph summary has invalid support_digest")
    try:
        support_digest = bytes.fromhex(digest_text)
    except ValueError as exc:
        raise RuntimeError(
            "strict V3 shared-graph summary has invalid support_digest"
        ) from exc
    if len(support_digest) != 32:
        raise RuntimeError("strict V3 shared-graph summary has invalid support_digest")

    raw_direction_metrics = summary_fields.get("direction_metrics")
    if not isinstance(raw_direction_metrics, list) or len(raw_direction_metrics) != 4:
        raise RuntimeError("strict V3 shared-graph summary has invalid direction_metrics")
    direction_fields = frozenset(
        {
            "direction",
            "object_kind",
            "member_width",
            "owner_count",
            "member_count",
            "empty_owner_count",
            "block_count",
            "raw_byte_count",
        }
    )
    direction_metrics: list[SharedGraphDirectionMetrics] = []
    seen_directions: set[int] = set()
    for raw_metric in raw_direction_metrics:
        metric = _shared_graph_metric_object(
            raw_metric,
            field_name="direction metric",
            expected_fields=direction_fields,
        )
        direction = _shared_graph_strict_int(metric.get("direction"), "direction")
        expected_direction = _V3_SHARED_GRAPH_DIRECTIONS.get(direction)
        if expected_direction is None or direction in seen_directions:
            raise RuntimeError(
                "strict V3 shared-graph summary has invalid graph directions"
            )
        seen_directions.add(direction)
        object_kind, expected_member_width = expected_direction
        member_width = _shared_graph_strict_int(
            metric.get("member_width"), "member_width"
        )
        if metric.get("object_kind") != object_kind or member_width != (
            expected_member_width
        ):
            raise RuntimeError(
                "strict V3 shared-graph summary has incompatible graph direction"
            )
        metric_owner_count = _shared_graph_strict_int(
            metric.get("owner_count"), "direction owner_count"
        )
        member_count = _shared_graph_strict_int(
            metric.get("member_count"), "direction member_count"
        )
        empty_owner_count = _shared_graph_strict_int(
            metric.get("empty_owner_count"), "empty_owner_count"
        )
        metric_block_count = _shared_graph_strict_int(
            metric.get("block_count"), "direction block_count"
        )
        metric_raw_byte_count = _shared_graph_strict_int(
            metric.get("raw_byte_count"), "direction raw_byte_count"
        )
        if (
            empty_owner_count > metric_owner_count
            or member_count * member_width != metric_raw_byte_count
        ):
            raise RuntimeError(
                "strict V3 shared-graph summary has inconsistent direction metrics"
            )
        direction_metrics.append(
            SharedGraphDirectionMetrics(
                direction=direction,
                object_kind=object_kind,
                member_width=member_width,
                owner_count=metric_owner_count,
                member_count=member_count,
                empty_owner_count=empty_owner_count,
                block_count=metric_block_count,
                raw_byte_count=metric_raw_byte_count,
            )
        )
    direction_metrics.sort(key=lambda metric: metric.direction)
    if seen_directions != set(_V3_SHARED_GRAPH_DIRECTIONS):
        raise RuntimeError("strict V3 shared-graph summary omits graph directions")
    if block_count != sum(metric.block_count for metric in direction_metrics):
        raise RuntimeError("strict V3 shared-graph summary block counts disagree")
    if owner_count != sum(metric.owner_count for metric in direction_metrics):
        raise RuntimeError("strict V3 shared-graph summary owner counts disagree")
    if raw_block_byte_count != sum(
        metric.raw_byte_count for metric in direction_metrics
    ):
        raise RuntimeError("strict V3 shared-graph summary block bytes disagree")

    raw_edge_metrics = summary_fields.get("edge_metrics")
    if not isinstance(raw_edge_metrics, list) or len(raw_edge_metrics) != 2:
        raise RuntimeError("strict V3 shared-graph summary has invalid edge_metrics")
    edge_fields = frozenset(
        {
            "edge_kind",
            "input_edge_count",
            "unique_edge_count",
            "duplicate_edge_count",
        }
    )
    expected_edge_count_by_kind = {
        "group_npi": expected.group_npi_input_edge_count,
        "group_provider_set": expected.group_provider_set_input_edge_count,
    }
    edge_metrics: list[SharedGraphEdgeMetrics] = []
    seen_edge_kinds: set[str] = set()
    for raw_metric in raw_edge_metrics:
        metric = _shared_graph_metric_object(
            raw_metric,
            field_name="edge metric",
            expected_fields=edge_fields,
        )
        edge_kind = metric.get("edge_kind")
        if not isinstance(edge_kind, str) or edge_kind not in expected_edge_count_by_kind:
            raise RuntimeError("strict V3 shared-graph summary has invalid edge kind")
        if edge_kind in seen_edge_kinds:
            raise RuntimeError("strict V3 shared-graph summary repeats an edge kind")
        seen_edge_kinds.add(edge_kind)
        metric_input_count = _shared_graph_strict_int(
            metric.get("input_edge_count"), "edge input_edge_count"
        )
        unique_edge_count = _shared_graph_strict_int(
            metric.get("unique_edge_count"), "edge unique_edge_count"
        )
        duplicate_edge_count = _shared_graph_strict_int(
            metric.get("duplicate_edge_count"), "edge duplicate_edge_count"
        )
        if (
            metric_input_count != expected_edge_count_by_kind[edge_kind]
            or metric_input_count != unique_edge_count + duplicate_edge_count
        ):
            raise RuntimeError("strict V3 shared-graph summary edge counts disagree")
        edge_metrics.append(
            SharedGraphEdgeMetrics(
                edge_kind=edge_kind,
                input_edge_count=metric_input_count,
                unique_edge_count=unique_edge_count,
                duplicate_edge_count=duplicate_edge_count,
            )
        )
    edge_metrics.sort(key=lambda metric: metric.edge_kind)
    if seen_edge_kinds != set(expected_edge_count_by_kind):
        raise RuntimeError("strict V3 shared-graph summary omits an edge kind")

    integrity_fields = frozenset(
        {
            "shard_count",
            "artifact_count",
            "checksum_byte_count",
            "reciprocal_pair_count",
            "reciprocal_edge_count",
            "input_edge_count",
            "unique_edge_count",
            "duplicate_edge_count",
        }
    )
    raw_integrity = _shared_graph_metric_object(
        summary_fields.get("integrity"),
        field_name="integrity",
        expected_fields=integrity_fields,
    )
    integrity_value_by_name = {
        field_name: _shared_graph_strict_int(
            raw_integrity.get(field_name), f"integrity {field_name}"
        )
        for field_name in integrity_fields
    }
    edge_input_count = sum(metric.input_edge_count for metric in edge_metrics)
    unique_edge_count = sum(metric.unique_edge_count for metric in edge_metrics)
    duplicate_edge_count = sum(metric.duplicate_edge_count for metric in edge_metrics)
    expected_integrity_by_name = {
        "shard_count": expected.shard_count,
        "artifact_count": expected.shard_count * 4,
        "checksum_byte_count": expected.input_byte_count,
        "reciprocal_pair_count": 2,
        "reciprocal_edge_count": unique_edge_count,
        "input_edge_count": edge_input_count,
        "unique_edge_count": unique_edge_count,
        "duplicate_edge_count": duplicate_edge_count,
    }
    if integrity_value_by_name != expected_integrity_by_name:
        raise RuntimeError("strict V3 shared-graph summary integrity counts disagree")

    fixed_file_size_by_name = {
        "owner_spool_path": owner_count * 25,
        "group_map_path": provider_group_count * 20,
        "reference_path": block_count * 57,
    }
    for field_name, expected_size in fixed_file_size_by_name.items():
        if output_path_by_name[field_name].stat().st_size != expected_size:
            raise RuntimeError(
                f"strict V3 shared-graph output has invalid {field_name} size"
            )
    for field_name in (
        "block_copy_path",
        "owner_copy_path",
        "group_copy_path",
        "npi_copy_path",
    ):
        if output_path_by_name[field_name].stat().st_size < 21:
            raise RuntimeError(
                f"strict V3 shared-graph output has invalid {field_name} size"
            )
    if block_count and output_path_by_name["block_spool_path"].stat().st_size <= 0:
        raise RuntimeError("strict V3 shared-graph block spool is empty")

    return SharedGraphConversionResult(
        scratch_directory=output_directory,
        block_copy_path=output_path_by_name["block_copy_path"],
        owner_copy_path=output_path_by_name["owner_copy_path"],
        group_copy_path=output_path_by_name["group_copy_path"],
        npi_copy_path=output_path_by_name["npi_copy_path"],
        block_spool_path=output_path_by_name["block_spool_path"],
        owner_spool_path=output_path_by_name["owner_spool_path"],
        group_map_path=output_path_by_name["group_map_path"],
        reference_path=output_path_by_name["reference_path"],
        block_count=block_count,
        owner_count=owner_count,
        provider_group_count=provider_group_count,
        npi_count=npi_count,
        support_digest=support_digest,
        direction_metrics=tuple(direction_metrics),
        edge_metrics=tuple(edge_metrics),
        input_byte_count=input_byte_count,
        raw_block_byte_count=raw_block_byte_count,
        stored_block_byte_count=stored_block_byte_count,
        integrity=SharedGraphIntegrityMetrics(**integrity_value_by_name),
    )


def _parse_shared_graph_summary_frame(
    stdout: bytes,
    *,
    expected_output_directory: Path,
    expected: _SharedGraphExpected,
) -> SharedGraphConversionResult:
    header, separator, framed_payload = stdout.partition(b"\n")
    if not separator:
        raise RuntimeError("strict V3 shared-graph converter omitted its frame header")
    try:
        frame_name, raw_length = header.split(b"\t", 1)
    except ValueError as exc:
        raise RuntimeError(
            "strict V3 shared-graph converter emitted an invalid frame header"
        ) from exc
    if frame_name != _V3_SHARED_GRAPH_SUMMARY_FRAME:
        raise RuntimeError("strict V3 shared-graph converter emitted an unknown frame")
    if not raw_length or not raw_length.isdigit():
        raise RuntimeError(
            "strict V3 shared-graph converter emitted an invalid frame length"
        )
    payload_length = int(raw_length)
    if payload_length <= 0 or payload_length > _V3_SHARED_GRAPH_SUMMARY_MAX_BYTES:
        raise RuntimeError(
            "strict V3 shared-graph converter emitted an out-of-range frame length"
        )
    if len(framed_payload) != payload_length + 1 or not framed_payload.endswith(b"\n"):
        raise RuntimeError(
            "strict V3 shared-graph converter emitted a truncated or trailing frame"
        )
    return _shared_graph_result_from_summary(
        _shared_graph_json_loads(framed_payload[:-1]),
        expected_output_directory=expected_output_directory,
        expected=expected,
    )


def _strict_non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"strict V3 scanner emitted invalid {field_name}")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"strict V3 scanner emitted invalid {field_name}"
        ) from exc
    if normalized < 0:
        raise RuntimeError(f"strict V3 scanner emitted negative {field_name}")
    return normalized


def _validated_v3_file_frame(
    frame_fields: Any,
    *,
    label: str,
    fields: tuple[str, ...],
    expected_format: str,
    expected_version: int,
) -> dict[str, Any]:
    if not isinstance(frame_fields, dict):
        raise RuntimeError(f"strict V3 scanner emitted invalid {label} frame")
    field_by_name = {
        field_name: frame_fields.get(field_name) for field_name in fields
    }
    raw_path = str(field_by_name.get("path") or "").strip()
    if not raw_path:
        raise RuntimeError(f"strict V3 scanner emitted {label} without a path")
    if str(field_by_name.get("format") or "") != expected_format:
        raise RuntimeError(f"strict V3 scanner emitted incompatible {label} format")
    if _strict_non_negative_int(
        field_by_name.get("version"), f"{label} version"
    ) != int(expected_version):
        raise RuntimeError(f"strict V3 scanner emitted incompatible {label} version")
    row_count = _strict_non_negative_int(
        field_by_name.get("row_count"), f"{label} row_count"
    )
    byte_count = _strict_non_negative_int(
        field_by_name.get("bytes"), f"{label} bytes"
    )
    if row_count <= 0 or byte_count <= 0:
        raise RuntimeError(f"strict V3 scanner emitted empty {label} metadata")
    if "partition" in field_by_name:
        partition = _strict_non_negative_int(
            field_by_name.get("partition"), f"{label} partition"
        )
        partition_count = _strict_non_negative_int(
            field_by_name.get("partition_count"), f"{label} partition_count"
        )
        if partition_count <= 0 or partition >= partition_count:
            raise RuntimeError(f"strict V3 scanner emitted invalid {label} partition")
    return field_by_name


def _validate_v3_scanner_config(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise RuntimeError("strict V3 scanner emitted invalid scanner_config")
    required_value_by_name = {
        "snapshot_arch": "postgres_binary_v3",
        "storage_generation": PTG2_V3_SHARED_GENERATION,
        "serving_row_semantics": "source_multiset_v1",
        "serving_run_format": _V3_SERVING_RUN_FORMAT,
    }
    for field_name, expected in required_value_by_name.items():
        if str(payload.get(field_name) or "") != expected:
            raise RuntimeError(
                f"strict V3 scanner_config has incompatible {field_name}"
            )
    if _strict_non_negative_int(
        payload.get("serving_run_version"), "serving_run_version"
    ) != _V3_SERVING_RUN_VERSION:
        raise RuntimeError("strict V3 scanner_config has incompatible serving_run_version")
    return payload


def _ptg2_rust_scanner_binary() -> Path | None:
    configured = os.getenv(PTG2_RUST_SCANNER_BIN_ENV)
    require_release = _env_bool(PTG2_RUST_REQUIRE_RELEASE_ENV, False)
    if configured:
        candidate = Path(configured)
        if candidate.exists() and os.access(candidate, os.X_OK):
            profile = _ptg2_scanner_binary_profile(candidate)
            if require_release and profile == "debug":
                logger.warning(
                    "Ignoring PTG2 Rust scanner debug binary because %s is enabled: %s",
                    PTG2_RUST_REQUIRE_RELEASE_ENV,
                    candidate,
                )
                return None
            return candidate
        return None
    root = Path(__file__).resolve().parents[2]
    candidates = [
        root / "support" / "ptg2_scanner" / "target" / "release" / "ptg2_scanner",
        root / "support" / "ptg2_scanner" / "target" / "debug" / "ptg2_scanner",
    ]
    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            profile = _ptg2_scanner_binary_profile(candidate)
            if require_release and profile == "debug":
                logger.warning(
                    "Ignoring PTG2 Rust scanner debug binary because %s is enabled: %s",
                    PTG2_RUST_REQUIRE_RELEASE_ENV,
                    candidate,
                )
                continue
            return candidate
    return None


def _log_ptg2_rust_scanner_binary(binary: Path) -> None:
    logger.info(
        "PTG2 Rust scanner binary selected path=%s profile=%s require_release=%s",
        binary,
        _ptg2_scanner_binary_profile(binary),
        _env_bool(PTG2_RUST_REQUIRE_RELEASE_ENV, False),
    )


async def convert_v3_provider_membership_shards_to_shared_graph_rust(
    *,
    shards: Iterable[SharedGraphShardBundle],
    provider_set_key_map_path: str | Path,
    output_directory: str | Path,
) -> SharedGraphConversionResult:
    """Convert strict-V3 graph shards with the native bounded-memory converter."""

    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "strict V3 shared-graph conversion requires the PTG2 Rust scanner; "
            "build it with `cargo build --release --manifest-path "
            "support/ptg2_scanner/Cargo.toml`"
        )
    binary = Path(binary).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(
            f"strict V3 shared-graph scanner binary is unavailable: {binary}"
        )
    _log_ptg2_rust_scanner_binary(binary)

    provider_map = Path(provider_set_key_map_path).resolve()
    if not provider_map.is_file() or provider_map.stat().st_size <= 0:
        raise RuntimeError(
            "strict V3 shared-graph conversion requires a provider-set key map"
        )
    output = Path(output_directory).resolve()
    if output.exists():
        raise RuntimeError(
            f"strict V3 shared-graph output directory already exists: {output}"
        )
    if not output.parent.is_dir():
        raise RuntimeError(
            "strict V3 shared-graph output parent directory is unavailable"
        )
    manifest, expected = _shared_graph_manifest(
        shards=shards,
        provider_set_key_map_path=provider_map,
        output_directory=output,
    )
    manifest_path = output.parent / f"{output.name}.manifest.json"
    if manifest_path.exists():
        raise RuntimeError(
            f"strict V3 shared-graph manifest already exists: {manifest_path}"
        )
    manifest_bytes = json.dumps(
        manifest,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    with manifest_path.open("xb") as manifest_file:
        manifest_file.write(manifest_bytes)
        manifest_file.flush()
        os.fsync(manifest_file.fileno())

    process: asyncio.subprocess.Process | None = None
    spawn_task = asyncio.create_task(
        asyncio.create_subprocess_exec(
            str(binary),
            "--convert-shared-graph",
            str(manifest_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **_subprocess_session_options(asyncio.create_subprocess_exec),
        )
    )
    try:
        process = await asyncio.shield(spawn_task)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            stderr_tail = stderr.decode("utf-8", errors="replace")[-2000:]
            raise RuntimeError(
                _scanner_error_message(
                    "strict V3 shared-graph converter",
                    int(process.returncode or 0),
                    [stderr_tail],
                )
            )
        if stderr:
            logger.info(
                "PTG2 native shared-graph converter stderr: %s",
                stderr.decode("utf-8", errors="replace")[-2000:],
            )
        conversion_result = _parse_shared_graph_summary_frame(
            stdout,
            expected_output_directory=output,
            expected=expected,
        )
        manifest_path.unlink()
        return conversion_result
    except BaseException:
        cleanup_task = asyncio.create_task(
            _cleanup_failed_shared_graph_conversion(
                process=process,
                spawn_task=spawn_task,
                output_directory=output,
                manifest_path=manifest_path,
            )
        )
        await _await_cancellation_resistant_cleanup(cleanup_task)
        raise


def _scanner_error_message(prefix: str, return_code: int, stderr_tail: list[str]) -> str:
    return f"{prefix} failed with {_scanner_return_code_label(return_code)}: {chr(10).join(stderr_tail)[-1000:]}"


def _scanner_progress_fields(line: str) -> dict[str, str] | None:
    if not line.startswith(_SCANNER_PROGRESS_PREFIX):
        return None
    field_by_name: dict[str, str] = {}
    for part in line[len(_SCANNER_PROGRESS_PREFIX):].split("\t"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if key:
            field_by_name[key] = value.strip()
    return field_by_name


def _coerce_progress_int(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _coerce_progress_float(value: Any) -> float | None:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _format_progress_mib(value: int | None) -> str:
    if value is None:
        return "unknown"
    return f"{value / (1024 ** 2):,.1f} MiB"


def _format_progress_duration(value: float | None) -> str | None:
    if value is None or value < 0:
        return None
    elapsed_seconds = int(value)
    minutes, seconds = divmod(elapsed_seconds, 60)
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def _format_object_rate(value: float | None) -> str | None:
    if value is None or value < 0:
        return None
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M objects/s"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K objects/s"
    return f"{value:.0f} objects/s"


class _ScannerProgress(NamedTuple):
    compressed_bytes: int | None
    total_bytes: int | None
    percent: float | None
    eta_seconds: float | None
    elapsed_seconds: float | None
    compressed_mib_s: float | None
    object_count: int | None
    scanner_object_count_by_kind: dict[str, int]
    scanner_path: str | None
    is_done: bool


def _parse_scanner_progress(fields: dict[str, str]) -> _ScannerProgress:
    scanner_object_count_by_kind = {
        key: count
        for key, value in fields.items()
        if key not in _SCANNER_PROGRESS_BASE_KEYS
        and (count := _coerce_progress_int(value)) is not None
    }
    return _ScannerProgress(
        compressed_bytes=_coerce_progress_int(fields.get("compressed_bytes")),
        total_bytes=_coerce_progress_int(fields.get("total_bytes")),
        percent=_coerce_progress_float(fields.get("percent")),
        eta_seconds=_coerce_progress_float(fields.get("eta_seconds")),
        elapsed_seconds=_coerce_progress_float(fields.get("elapsed_seconds")),
        compressed_mib_s=_coerce_progress_float(fields.get("compressed_mib_s")),
        object_count=_coerce_progress_int(fields.get("objects")),
        scanner_object_count_by_kind=scanner_object_count_by_kind,
        scanner_path=fields.get("path"),
        is_done=str(fields.get("done") or "").lower() == "true",
    )


def _scanner_object_progress_message(
    phase: str,
    scanner_objects: dict[str, int],
    *,
    object_count: int,
    elapsed_seconds: float | None,
    total_bytes: int | None,
) -> tuple[str, float | None]:
    negotiated_rates = scanner_objects.get("negotiated_rates", 0)
    provider_references = scanner_objects.get("provider_references", 0)
    in_network = scanner_objects.get("in_network", 0)
    activity = "rate stream" if negotiated_rates or in_network else "provider-reference stream"
    message_parts = [f"{phase}: {activity}"]
    if negotiated_rates:
        message_parts.append(f"rates={negotiated_rates:,}")
    if provider_references:
        message_parts.append(f"provider refs={provider_references:,}")
    if in_network:
        message_parts.append(f"code groups={in_network:,}")
    if not any((negotiated_rates, provider_references, in_network)):
        message_parts.append(f"objects={object_count:,}")
    object_rate = (
        object_count / elapsed_seconds
        if elapsed_seconds is not None and elapsed_seconds > 0
        else None
    )
    formatted_rate = _format_object_rate(object_rate)
    if formatted_rate:
        message_parts.append(f"throughput={formatted_rate}")
    formatted_elapsed = _format_progress_duration(elapsed_seconds)
    if formatted_elapsed:
        message_parts.append(f"elapsed={formatted_elapsed}")
    if total_bytes is not None:
        message_parts.append(f"input={_format_progress_mib(total_bytes)}")
    return ", ".join(message_parts), object_rate


def _emit_scanner_live_progress(
    line: str,
    *,
    phase: str,
    live_progress_context: dict[str, Any] | None = None,
) -> None:
    """Parse a scanner progress frame and publish scaled live-progress fields."""
    fields = _scanner_progress_fields(line)
    if not fields:
        return
    progress = _parse_scanner_progress(fields)
    progress_context_map = {
        context_key: context_value
        for context_key, context_value in (live_progress_context or {}).items()
        if context_value not in (None, "")
    }
    use_object_progress = bool(
        not progress.is_done
        and progress.object_count is not None
        and progress.object_count > 0
        and (progress.compressed_bytes is None or progress.compressed_bytes <= 0)
    )
    phase_pct = None if use_object_progress else progress.percent
    overall_pct = _scale_stage_progress_pct(
        phase_pct,
        progress_context_map.get("overall_progress_start_pct"),
        progress_context_map.get("overall_progress_end_pct"),
    )
    pct = overall_pct if overall_pct is not None else phase_pct
    object_rate = None
    if use_object_progress:
        pct = _coerce_progress_float(
            progress_context_map.get("overall_progress_start_pct")
        )
        message, object_rate = _scanner_object_progress_message(
            phase,
            progress.scanner_object_count_by_kind,
            object_count=progress.object_count,
            elapsed_seconds=progress.elapsed_seconds,
            total_bytes=progress.total_bytes,
        )
        progress_unit = "objects"
        progress_done = progress.object_count
        progress_total = None
        progress_eta = None
    else:
        message_parts = [
            f"{phase} {progress.percent:.2f}%" if progress.percent is not None else phase,
            (
                f"read {_format_progress_mib(progress.compressed_bytes)} "
                f"of {_format_progress_mib(progress.total_bytes)}"
            ),
        ]
        if progress.object_count is not None:
            message_parts.append(f"objects={progress.object_count}")
        if progress.compressed_mib_s is not None:
            message_parts.append(f"{progress.compressed_mib_s:.2f} MiB/s")
        message = ", ".join(message_parts)
        progress_unit = "compressed_bytes"
        progress_done = progress.compressed_bytes
        progress_total = progress.total_bytes
        progress_eta = progress.eta_seconds
    progress_value_by_field: dict[str, Any] = {
        **progress_context_map,
        "phase": phase,
        "unit": progress_unit,
        "done": progress_done,
        "total": progress_total,
        "pct": pct,
        "phase_pct": phase_pct,
        "eta_seconds": progress_eta,
        "elapsed_seconds": progress.elapsed_seconds,
        "message": message,
        "detail": message,
        "source": "ptg2-scanner-progress",
        "confidence": "live",
        "scanner_path": progress.scanner_path,
        "scanner_done": progress.is_done,
        "scanner_objects": progress.scanner_object_count_by_kind or None,
        "scanner_progress_basis": progress_unit,
        "scanner_object_rate": object_rate,
    }
    try:
        write_live_progress(**progress_value_by_field)
    except Exception:
        logger.debug("Failed to write PTG2 scanner live progress", exc_info=True)


def _scanner_metric_message(record_kind: str, payload: dict[str, Any]) -> str:
    mode = str(payload.get("execution_mode") or "unknown")
    if record_kind == "scanner_config":
        order = str(payload.get("provider_reference_order") or "unknown")
        selected = bool(payload.get("top_level_byte_scan_selected"))
        message = f"compact-serving scanner mode={mode}, provider_order={order}, byte_scan={selected}"
        fallback_reason = payload.get("top_level_byte_scan_fallback_reason")
        if fallback_reason:
            message += f", fallback={fallback_reason}"
        return message

    elapsed = _coerce_progress_float(payload.get("elapsed_seconds"))
    nonblocked = _coerce_progress_float(payload.get("producer_nonblocked_seconds"))
    blocked = _coerce_progress_float(payload.get("producer_blocked_seconds"))
    raw_mib_s = _coerce_progress_float(payload.get("producer_nonblocked_raw_mib_s"))
    parts = [f"compact-serving scanner complete mode={mode}"]
    if elapsed is not None:
        parts.append(f"elapsed={elapsed:.3f}s")
    if nonblocked is not None:
        parts.append(f"producer_nonblocked={nonblocked:.3f}s")
    if blocked is not None:
        parts.append(f"queue_blocked={blocked:.3f}s")
    if raw_mib_s is not None:
        parts.append(f"producer_raw={raw_mib_s:.2f} MiB/s")
    return ", ".join(parts)


def _emit_scanner_metric_progress(
    record_kind: str,
    metric_fields: dict[str, Any],
    *,
    live_progress_context: dict[str, Any] | None = None,
) -> None:
    if record_kind not in _SCANNER_METRIC_RECORD_KINDS:
        return
    message = _scanner_metric_message(record_kind, metric_fields)
    metric_line = f"PTG2_SCANNER_METRICS\tkind={record_kind}\t{message}"
    _emit_screen_line(metric_line)
    logger.info(metric_line)
    progress_context_map = {
        context_key: context_value
        for context_key, context_value in (live_progress_context or {}).items()
        if context_value not in (None, "")
    }
    try:
        write_live_progress(
            **progress_context_map,
            phase="compact-serving scanner",
            message=message,
            detail=message,
            source="ptg2-scanner-metrics",
            confidence="live",
            scanner_record_kind=record_kind,
            scanner_execution_mode=metric_fields.get("execution_mode"),
            scanner_fallback_reason=metric_fields.get("top_level_byte_scan_fallback_reason"),
        )
    except Exception:
        logger.debug("Failed to write PTG2 scanner metric progress", exc_info=True)


def _iter_top_level_object_bytes_rust(
    path: str | Path,
    array_names: set[str],
    *,
    live_progress_context: dict[str, Any] | None = None,
):
    """Run the Rust scanner and yield top-level object frames as raw bytes."""
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "PTG2 Rust scanner is enabled but no scanner binary was found; "
            "build it with `cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml`"
        )
    _log_ptg2_rust_scanner_binary(binary)
    if live_progress_context is None:
        live_progress_context = current_live_progress_context()
    process = subprocess.Popen(
        [str(binary), str(path), *sorted(array_names)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **_subprocess_session_options(subprocess.Popen),
    )
    process_control = _ScannerProcessControl()
    process_control.attach(process)
    assert process.stdout is not None
    stderr_lines: list[str] = []
    stderr_thread: threading.Thread | None = None
    if process.stderr is not None:

        def _read_scanner_stderr() -> None:
            assert process.stderr is not None
            for raw_line in iter(process.stderr.readline, b""):
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                stderr_lines.append(line)
                if len(stderr_lines) > 20:
                    del stderr_lines[:-20]
                if line.startswith(_SCANNER_PROGRESS_PREFIX):
                    _emit_screen_line(line)
                    logger.info(line)
                    _emit_scanner_live_progress(
                        line,
                        phase="PTG scanner",
                        live_progress_context=live_progress_context,
                    )
                else:
                    logger.warning("PTG2 Rust scanner stderr: %s", line)

        stderr_thread = threading.Thread(
            target=_read_scanner_stderr,
            name="ptg2-rust-scanner-stderr",
            daemon=True,
        )
        stderr_thread.start()
    is_terminated_by_consumer = False
    try:
        while True:
            header = process.stdout.readline()
            if not header:
                break
            try:
                name_bytes, length_bytes = header.rstrip(b"\n").split(b"\t", 1)
                payload_len = int(length_bytes)
            except Exception as exc:
                raise RuntimeError(f"Invalid PTG2 Rust scanner frame header: {header!r}") from exc
            frame_bytes = _read_exactly(process.stdout, payload_len)
            if len(frame_bytes) != payload_len:
                raise RuntimeError("PTG2 Rust scanner ended mid-frame")
            trailer = process.stdout.read(1)
            if trailer not in {b"", b"\n"}:
                raise RuntimeError("Invalid PTG2 Rust scanner frame trailer")
            yield name_bytes.decode("utf-8"), frame_bytes
    finally:
        if process.poll() is None:
            is_terminated_by_consumer = True
            process_control.terminate()
        return_code = process.wait()
        if stderr_thread is not None:
            stderr_thread.join()
        if return_code != 0 and not is_terminated_by_consumer:
            raise RuntimeError(
                _scanner_error_message("PTG2 Rust scanner", return_code, stderr_lines)
            )


def _iter_compact_serving_records_rust(
    path: str | Path,
    *,
    raw_source_sha256: str,
    snapshot_id: str,
    plan_id: str,
    coverage_scope_id: str,
    plan_month_id: str,
    source_trace_set_hash: str,
    confidence_code: str = PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
    compact_copy_path: str | Path | None = None,
    procedure_copy_path: str | Path | None = None,
    price_code_set_copy_path: str | Path | None = None,
    price_atom_copy_path: str | Path | None = None,
    price_set_entry_copy_path: str | Path | None = None,
    provider_set_copy_path: str | Path | None = None,
    provider_group_member_copy_path: str | Path | None = None,
    provider_set_component_copy_path: str | Path | None = None,
    provider_set_entry_copy_path: str | Path | None = None,
    provider_entry_component_copy_path: str | Path | None = None,
    manifest_serving_copy_path: str | Path | None = None,
    manifest_lean_serving_copy_path: str | Path | None = None,
    v3_serving_run_directory: str | Path | None = None,
    manifest_provider_forward_sidecar_path: str | Path | None = None,
    manifest_provider_inverted_sidecar_path: str | Path | None = None,
    manifest_provider_npi_sidecar_path: str | Path | None = None,
    manifest_price_forward_sidecar_path: str | Path | None = None,
    manifest_price_atom_copy_path: str | Path | None = None,
    manifest_price_set_atom_copy_path: str | Path | None = None,
    manifest_provider_group_member_copy_path: str | Path | None = None,
    manifest_code_count_copy_path: str | Path | None = None,
    manifest_provider_set_dictionary_copy_path: str | Path | None = None,
    source_network_names: list[str] | tuple[str, ...] | set[str] | None = None,
    manifest_only: bool | None = None,
    live_progress_context: dict[str, Any] | None = None,
    _process_control: _ScannerProcessControl | None = None,
):
    """Run the Rust compact scanner and yield validated decoded record frames."""
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "PTG2 Rust compact serving is enabled but no scanner binary was found; "
            "build it with `cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml`"
        )
    _log_ptg2_rust_scanner_binary(binary)
    if live_progress_context is None:
        live_progress_context = current_live_progress_context()
    scanner_environment_map = {
        **os.environ,
        "HLTHPRT_PTG2_SNAPSHOT_ARCH": "postgres_binary_v3",
        "HLTHPRT_PTG2_RAW_SOURCE_SHA256": raw_source_sha256,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": snapshot_id,
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": plan_id,
        "HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID": coverage_scope_id,
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": plan_month_id,
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": source_trace_set_hash,
        "HLTHPRT_PTG2_COMPACT_CONFIDENCE_CODE": confidence_code,
    }
    scanner_environment_map.pop("HLTHPRT_PTG2_SOURCE_NETWORK_NAMES_JSON", None)
    if compact_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH"] = str(
            compact_copy_path
        )
    if procedure_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_PROCEDURE_COPY_PATH"] = str(
            procedure_copy_path
        )
    if price_code_set_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_PRICE_CODE_SET_COPY_PATH"] = str(
            price_code_set_copy_path
        )
    if price_atom_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_PRICE_ATOM_COPY_PATH"] = str(
            price_atom_copy_path
        )
    if price_set_entry_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_PRICE_SET_ENTRY_COPY_PATH"] = str(
            price_set_entry_copy_path
        )
    if provider_set_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_PROVIDER_SET_COPY_PATH"] = str(
            provider_set_copy_path
        )
    if provider_group_member_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH"] = str(
            provider_group_member_copy_path
        )
    if provider_set_component_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_PROVIDER_SET_COMPONENT_COPY_PATH"] = str(
            provider_set_component_copy_path
        )
    if provider_set_entry_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_PROVIDER_SET_ENTRY_COPY_PATH"] = str(
            provider_set_entry_copy_path
        )
    if provider_entry_component_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_PROVIDER_ENTRY_COMPONENT_COPY_PATH"] = str(
            provider_entry_component_copy_path
        )
    if manifest_serving_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_MANIFEST_SERVING_COPY_PATH"] = str(
            manifest_serving_copy_path
        )
    if manifest_lean_serving_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_MANIFEST_LEAN_SERVING_COPY_PATH"] = str(
            manifest_lean_serving_copy_path
        )
    if v3_serving_run_directory is None:
        raise RuntimeError("strict V3 scanner requires an explicit serving-run directory")
    scanner_environment_map["HLTHPRT_PTG2_V3_SERVING_RUN_DIR"] = str(
        v3_serving_run_directory
    )
    scanner_environment_map["HLTHPRT_PTG2_MANIFEST_SPILL_DIR"] = str(
        v3_serving_run_directory
    )
    if manifest_provider_forward_sidecar_path is not None:
        scanner_environment_map[
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_FORWARD_SIDECAR_PATH"
        ] = str(manifest_provider_forward_sidecar_path)
    if manifest_provider_inverted_sidecar_path is not None:
        scanner_environment_map[
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_INVERTED_SIDECAR_PATH"
        ] = str(manifest_provider_inverted_sidecar_path)
    if manifest_provider_npi_sidecar_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_MANIFEST_PROVIDER_NPI_SIDECAR_PATH"] = str(
            manifest_provider_npi_sidecar_path
        )
    if manifest_price_forward_sidecar_path is not None:
        scanner_environment_map[
            "HLTHPRT_PTG2_MANIFEST_PRICE_FORWARD_SIDECAR_PATH"
        ] = str(manifest_price_forward_sidecar_path)
    if manifest_price_atom_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH"] = str(
            manifest_price_atom_copy_path
        )
    if manifest_price_set_atom_copy_path is not None:
        scanner_environment_map[
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_ATOM_COPY_PATH"
        ] = str(manifest_price_set_atom_copy_path)
    if manifest_provider_group_member_copy_path is not None:
        scanner_environment_map[
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH"
        ] = str(manifest_provider_group_member_copy_path)
    if manifest_code_count_copy_path is not None:
        scanner_environment_map["HLTHPRT_PTG2_MANIFEST_CODE_COUNT_COPY_PATH"] = str(
            manifest_code_count_copy_path
        )
    if manifest_provider_set_dictionary_copy_path is not None:
        scanner_environment_map[
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH"
        ] = str(manifest_provider_set_dictionary_copy_path)
    if manifest_only is not None:
        scanner_environment_map["HLTHPRT_PTG2_MANIFEST_ONLY"] = (
            "true" if manifest_only else "false"
        )
    scanner_environment_map.setdefault(
        PTG2_RUST_WORKERS_ENV, str(PTG2_DEFAULT_RUST_WORKERS)
    )
    scanner_environment_map.setdefault(
        PTG2_RUST_WORK_QUEUE_ENV, str(PTG2_DEFAULT_RUST_WORK_QUEUE)
    )
    scanner_environment_map.setdefault(
        PTG2_RUST_EVENT_QUEUE_ENV, str(PTG2_DEFAULT_RUST_EVENT_QUEUE)
    )
    scanner_environment_map.setdefault(
        PTG2_RUST_SPLIT_NEGOTIATED_RATES_ENV,
        str(PTG2_DEFAULT_RUST_SPLIT_NEGOTIATED_RATES),
    )
    process_control = _process_control or _ScannerProcessControl()
    process = subprocess.Popen(
        [str(binary), "--compact-serving", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=scanner_environment_map,
        **_subprocess_session_options(subprocess.Popen),
    )
    process_control.attach(process)
    assert process.stdout is not None
    stderr_lines: list[str] = []
    stderr_thread: threading.Thread | None = None
    if process.stderr is not None:

        def _read_scanner_stderr() -> None:
            assert process.stderr is not None
            for raw_line in iter(process.stderr.readline, b""):
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                stderr_lines.append(line)
                if len(stderr_lines) > 20:
                    del stderr_lines[:-20]
                if (
                    line.startswith(_SCANNER_PROGRESS_PREFIX)
                    or line.startswith("PTG2_DEDUPE_SUMMARY\t")
                    or line.startswith("PTG2_SCANNER_WORKER_FAILED\t")
                ):
                    _emit_screen_line(line)
                    logger.info(line)
                    if line.startswith(_SCANNER_PROGRESS_PREFIX):
                        _emit_scanner_live_progress(
                            line,
                            phase="compact-serving scanner",
                            live_progress_context=live_progress_context,
                        )
                else:
                    logger.warning("PTG2 Rust compact scanner stderr: %s", line)

        stderr_thread = threading.Thread(
            target=_read_scanner_stderr,
            name="ptg2-rust-compact-stderr",
            daemon=True,
        )
        stderr_thread.start()
    is_terminated_by_consumer = False
    has_frame_stream_failure = False
    has_stdout_terminal_summary = False
    has_scanner_config = False
    has_scanner_summary = False
    serving_run_partition_files: list[dict[str, Any]] = []
    serving_run_code_dictionary_files: list[dict[str, Any]] = []
    try:
        while True:
            header = process.stdout.readline()
            if not header:
                break
            try:
                name_bytes, length_bytes = header.rstrip(b"\n").split(b"\t", 1)
                payload_len = int(length_bytes)
            except Exception as exc:
                raise RuntimeError(f"Invalid PTG2 Rust compact frame header: {header!r}") from exc
            frame_bytes = _read_exactly(process.stdout, payload_len)
            if len(frame_bytes) != payload_len:
                has_frame_stream_failure = True
                try:
                    frame_return_code = process.wait(timeout=2)
                    frame_process_status = _scanner_return_code_label(frame_return_code)
                except subprocess.TimeoutExpired:
                    frame_process_status = "still running after stdout closed"
                if stderr_thread is not None:
                    stderr_thread.join(timeout=2)
                stderr_detail = chr(10).join(stderr_lines)[-1000:]
                raise RuntimeError(
                    "PTG2 Rust compact scanner ended mid-frame "
                    f"kind={name_bytes.decode('utf-8', errors='replace')} "
                    f"expected_bytes={payload_len} received_bytes={len(frame_bytes)} "
                    f"process={frame_process_status}: {stderr_detail}"
                )
            trailer = process.stdout.read(1)
            if trailer not in {b"", b"\n"}:
                raise RuntimeError("Invalid PTG2 Rust compact scanner frame trailer")
            record_kind = name_bytes.decode("utf-8")
            frame_field_map = _json_loads(frame_bytes)
            if record_kind == "v3_serving_run_partition_file":
                serving_run_partition_files.append(
                    _validated_v3_file_frame(
                        frame_field_map,
                        label="serving-run partition",
                        fields=(
                            "path",
                            "partition",
                            "partition_count",
                            "row_count",
                            "bytes",
                            "format",
                            "version",
                        ),
                        expected_format=_V3_SERVING_RUN_FORMAT,
                        expected_version=_V3_SERVING_RUN_VERSION,
                    )
                )
            elif record_kind == "v3_serving_code_dictionary_file":
                serving_run_code_dictionary_files.append(
                    _validated_v3_file_frame(
                        frame_field_map,
                        label="code-dictionary",
                        fields=("path", "row_count", "bytes", "format", "version"),
                        expected_format=_V3_CODE_DICTIONARY_FORMAT,
                        expected_version=_V3_CODE_DICTIONARY_VERSION,
                    )
                )
            elif record_kind == "scanner_summary" and isinstance(frame_field_map, dict):
                has_scanner_summary = True
                frame_field_map = {
                    **frame_field_map,
                    "serving_run_partition_files": list(serving_run_partition_files),
                    "serving_run_code_dictionary_files": list(
                        serving_run_code_dictionary_files
                    ),
                }
            elif record_kind == "scanner_summary":
                raise RuntimeError("strict V3 scanner emitted invalid scanner_summary")
            elif record_kind == "scanner_config":
                frame_field_map = _validate_v3_scanner_config(frame_field_map)
                has_scanner_config = True
            if record_kind in {"dedupe_summary", "scanner_summary"}:
                has_stdout_terminal_summary = True
            if record_kind in _SCANNER_METRIC_RECORD_KINDS and isinstance(
                frame_field_map, dict
            ):
                _emit_scanner_metric_progress(
                    record_kind,
                    frame_field_map,
                    live_progress_context=live_progress_context,
                )
            yield record_kind, frame_field_map
        if not has_scanner_config or not has_scanner_summary:
            raise RuntimeError(
                "strict V3 scanner completed without its required config and summary frames"
            )
    finally:
        is_terminated_by_consumer = (
            is_terminated_by_consumer or process_control.is_termination_requested
        )
        if process.poll() is None:
            is_terminated_by_consumer = True
            process_control.terminate()
        return_code = process.wait()
        if stderr_thread is not None:
            stderr_thread.join()
        if (
            return_code != 0
            and not is_terminated_by_consumer
            and not has_frame_stream_failure
            and not _is_scanner_sigterm_after_dedupe(
                return_code,
                stderr_lines,
                has_stdout_terminal_summary=has_stdout_terminal_summary,
            )
        ):
            raise RuntimeError(
                _scanner_error_message(
                    "PTG2 Rust compact scanner", return_code, stderr_lines
                )
            )


async def _aiter_compact_serving_records_rust(
    path: str | Path,
    *,
    raw_source_sha256: str,
    snapshot_id: str,
    plan_id: str,
    coverage_scope_id: str,
    plan_month_id: str,
    source_trace_set_hash: str,
    confidence_code: str = PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
    compact_copy_path: str | Path | None = None,
    procedure_copy_path: str | Path | None = None,
    price_code_set_copy_path: str | Path | None = None,
    price_atom_copy_path: str | Path | None = None,
    price_set_entry_copy_path: str | Path | None = None,
    provider_set_copy_path: str | Path | None = None,
    provider_group_member_copy_path: str | Path | None = None,
    provider_set_component_copy_path: str | Path | None = None,
    provider_set_entry_copy_path: str | Path | None = None,
    provider_entry_component_copy_path: str | Path | None = None,
    manifest_serving_copy_path: str | Path | None = None,
    manifest_lean_serving_copy_path: str | Path | None = None,
    v3_serving_run_directory: str | Path | None = None,
    manifest_provider_forward_sidecar_path: str | Path | None = None,
    manifest_provider_inverted_sidecar_path: str | Path | None = None,
    manifest_provider_npi_sidecar_path: str | Path | None = None,
    manifest_price_forward_sidecar_path: str | Path | None = None,
    manifest_price_atom_copy_path: str | Path | None = None,
    manifest_price_set_atom_copy_path: str | Path | None = None,
    manifest_provider_group_member_copy_path: str | Path | None = None,
    manifest_code_count_copy_path: str | Path | None = None,
    manifest_provider_set_dictionary_copy_path: str | Path | None = None,
    source_network_names: list[str] | tuple[str, ...] | set[str] | None = None,
    manifest_only: bool | None = None,
):
    """Yield compact scanner records asynchronously through a bounded queue."""
    live_progress_context = current_live_progress_context()
    process_control = _ScannerProcessControl()
    iterator = _iter_compact_serving_records_rust(
        path,
        raw_source_sha256=raw_source_sha256,
        snapshot_id=snapshot_id,
        plan_id=plan_id,
        coverage_scope_id=coverage_scope_id,
        plan_month_id=plan_month_id,
        source_trace_set_hash=source_trace_set_hash,
        confidence_code=confidence_code,
        compact_copy_path=compact_copy_path,
        procedure_copy_path=procedure_copy_path,
        price_code_set_copy_path=price_code_set_copy_path,
        price_atom_copy_path=price_atom_copy_path,
        price_set_entry_copy_path=price_set_entry_copy_path,
        provider_set_copy_path=provider_set_copy_path,
        provider_group_member_copy_path=provider_group_member_copy_path,
        provider_set_component_copy_path=provider_set_component_copy_path,
        provider_set_entry_copy_path=provider_set_entry_copy_path,
        provider_entry_component_copy_path=provider_entry_component_copy_path,
        manifest_serving_copy_path=manifest_serving_copy_path,
        manifest_lean_serving_copy_path=manifest_lean_serving_copy_path,
        v3_serving_run_directory=v3_serving_run_directory,
        manifest_provider_forward_sidecar_path=manifest_provider_forward_sidecar_path,
        manifest_provider_inverted_sidecar_path=manifest_provider_inverted_sidecar_path,
        manifest_provider_npi_sidecar_path=manifest_provider_npi_sidecar_path,
        manifest_price_forward_sidecar_path=manifest_price_forward_sidecar_path,
        manifest_price_atom_copy_path=manifest_price_atom_copy_path,
        manifest_price_set_atom_copy_path=manifest_price_set_atom_copy_path,
        manifest_provider_group_member_copy_path=manifest_provider_group_member_copy_path,
        manifest_code_count_copy_path=manifest_code_count_copy_path,
        manifest_provider_set_dictionary_copy_path=manifest_provider_set_dictionary_copy_path,
        source_network_names=source_network_names,
        manifest_only=manifest_only,
        live_progress_context=live_progress_context,
        _process_control=process_control,
    )
    event_queue: queue.Queue[Any] = queue.Queue(
        maxsize=max(_env_int(PTG2_RUST_EVENT_QUEUE_ENV, PTG2_DEFAULT_RUST_EVENT_QUEUE), 1)
    )
    sentinel = object()
    reader_stop = threading.Event()
    event_ready = asyncio.Event()
    event_loop = asyncio.get_running_loop()

    def notify_event_ready() -> None:
        """Wake the async consumer when the reader thread publishes an event."""

        try:
            event_loop.call_soon_threadsafe(event_ready.set)
        except RuntimeError:
            return

    def has_enqueued_reader_event(scanner_event: Any) -> bool:
        """Enqueue one event unless shutdown has already stopped the reader."""

        while not reader_stop.is_set():
            try:
                event_queue.put(
                    scanner_event,
                    timeout=_ASYNC_READER_QUEUE_PUT_TIMEOUT_SECONDS,
                )
            except queue.Full:
                continue
            notify_event_ready()
            return True
        return False

    def read_records() -> None:
        """Forward records or failures to the queue, followed by a sentinel."""
        try:
            for scanner_event in iterator:
                if not has_enqueued_reader_event(scanner_event):
                    break
        except BaseException as exc:
            has_enqueued_reader_event(exc)
        finally:
            has_enqueued_reader_event(sentinel)
            notify_event_ready()

    reader_thread = threading.Thread(
        target=read_records,
        name="ptg2-rust-compact-stdout",
        daemon=True,
    )
    reader_thread.start()
    is_reader_complete = False

    def close_iterator() -> None:
        """Close the scanner iterator once it is no longer executing."""

        close = getattr(iterator, "close", None)
        if close is None:
            return
        try:
            close()
        except ValueError as exc:
            if "generator already executing" not in str(exc):
                raise

    def complete_reader_cleanup() -> None:
        """Terminate, close, and join every reader-side resource in order."""

        try:
            if not is_reader_complete:
                process_control.terminate()
        finally:
            try:
                close_iterator()
            finally:
                try:
                    reader_thread.join()
                finally:
                    close_iterator()

    try:
        while True:
            event_ready.clear()
            try:
                scanner_event = event_queue.get_nowait()
            except queue.Empty:
                await event_ready.wait()
                continue
            if scanner_event is sentinel:
                is_reader_complete = True
                break
            if isinstance(scanner_event, BaseException):
                raise scanner_event
            yield scanner_event
    finally:
        reader_stop.set()
        event_ready.set()
        cleanup_task = asyncio.create_task(asyncio.to_thread(complete_reader_cleanup))
        await _await_cancellation_resistant_cleanup(cleanup_task)


def _scanner_return_code_label(return_code: int) -> str:
    if return_code >= 0:
        return f"exit code {return_code}"
    signal_number = -return_code
    try:
        signal_name = signal.Signals(signal_number).name
    except ValueError:
        return f"signal {signal_number}"
    return f"signal {signal_number} ({signal_name})"


def _is_scanner_sigterm_after_dedupe(
    return_code: int,
    stderr_tail: list[str],
    *,
    has_stdout_terminal_summary: bool = False,
) -> bool:
    return (
        return_code == -15
        and has_stdout_terminal_summary
        and any(line.startswith("PTG2_DEDUPE_SUMMARY\t") for line in stderr_tail)
    )
