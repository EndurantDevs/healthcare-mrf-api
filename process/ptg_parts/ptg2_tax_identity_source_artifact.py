# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate PTG2TAX1 artifacts and stage their source-local observations."""

from __future__ import annotations

import hashlib
import hmac
import os
import stat
import struct
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT,
    PreparedTaxIdentitySourceProjection,
    TaxIdentitySourceProjectionError,
    _MAGIC,
    _RECORD_BYTES,
    _STATE_COUNT_FIELDS,
    _SourceBinding,
    _VERSION,
    _digest_ascii,
    _fail,
    _has_prepared_copy_identity,
    _has_same_file_identity,
    _remove_ephemeral_copy,
    _source_ordinal_by_shard,
    _strict_policy,
    _validated_bindings,
)

_CONTENT_DOMAIN = b"PTG2TAXSOURCECONTENT\x01"
_PG_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)
_PG_COPY_TRAILER = struct.pack(">h", -1)
_STATE_BY_CODE = {
    1: "matched_ein",
    2: "missing",
    3: "malformed",
    4: "unsupported_type",
}
_STATE_CODE = {state: code for code, state in _STATE_BY_CODE.items()}
_COPY_COLUMNS = (
    "source_key",
    "source_ordinal",
    "source_record_ordinal",
    "provider_group_global_id_128",
    "tax_identity_state",
    "tin_id_128",
    "tin_hmac_sha256",
)
_COPY_READ_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class _ProjectionInputs:
    policy_id: str
    policy_descriptor: bytes
    ordinal_digest: bytes
    aggregate_digest: bytes
    bindings: tuple[_SourceBinding, ...]


@dataclass(frozen=True, slots=True)
class _ProjectionCopySummary:
    occurrence_count: int
    counts_by_state: Mapping[str, int]
    content_digest: bytes
    copy_sha256: str
    copy_byte_count: int
    copy_device: int
    copy_inode: int
    copy_mtime_ns: int


def _copy_field(output_file: BinaryIO, field_bytes: bytes | None) -> None:
    if field_bytes is None:
        output_file.write(struct.pack(">i", -1))
        return
    output_file.write(struct.pack(">i", len(field_bytes)))
    output_file.write(field_bytes)


def _write_copy_row(
    output_file: BinaryIO,
    *,
    binding: _SourceBinding,
    record_ordinal: int,
    provider_group_id: bytes,
    identity_state: str,
    tin_id_128: bytes | None,
    tin_hmac_sha256: bytes | None,
) -> None:
    output_file.write(struct.pack(">h", len(_COPY_COLUMNS)))
    field_values = (
        struct.pack(">i", binding.source_key),
        struct.pack(">i", binding.source_ordinal),
        struct.pack(">q", record_ordinal),
        provider_group_id,
        identity_state.encode("ascii"),
        tin_id_128,
        tin_hmac_sha256,
    )
    for field_bytes in field_values:
        _copy_field(output_file, field_bytes)


def _open_source_sidecar(
    binding: _SourceBinding,
) -> tuple[BinaryIO, os.stat_result]:
    try:
        path_metadata = os.lstat(binding.path)
        open_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        open_flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(binding.path, open_flags)
        sidecar_file = os.fdopen(descriptor, "rb", closefd=True)
        opened_metadata = os.fstat(sidecar_file.fileno())
        is_expected_file = (
            not stat.S_ISLNK(path_metadata.st_mode)
            and stat.S_ISREG(path_metadata.st_mode)
            and stat.S_ISREG(opened_metadata.st_mode)
            and opened_metadata.st_dev == path_metadata.st_dev
            and opened_metadata.st_ino == path_metadata.st_ino
            and opened_metadata.st_size == binding.artifact_byte_count
            and opened_metadata.st_mtime_ns == path_metadata.st_mtime_ns
        )
        if not is_expected_file:
            sidecar_file.close()
            raise _fail()
        return sidecar_file, path_metadata
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None


def _is_source_unchanged(
    sidecar_file: BinaryIO,
    source_path: Path,
    expected_metadata: os.stat_result,
) -> bool:
    try:
        opened_metadata = os.fstat(sidecar_file.fileno())
        current_metadata = os.lstat(source_path)
        return (
            stat.S_ISREG(opened_metadata.st_mode)
            and stat.S_ISREG(current_metadata.st_mode)
            and opened_metadata.st_dev
            == expected_metadata.st_dev
            == current_metadata.st_dev
            and opened_metadata.st_ino
            == expected_metadata.st_ino
            == current_metadata.st_ino
            and opened_metadata.st_size
            == expected_metadata.st_size
            == current_metadata.st_size
            and opened_metadata.st_mtime_ns
            == expected_metadata.st_mtime_ns
            == current_metadata.st_mtime_ns
        )
    except Exception:
        return False


def _hash_binding(content_digest: Any, binding: _SourceBinding) -> None:
    content_digest.update(binding.source_key.to_bytes(4, "big"))
    content_digest.update(binding.source_ordinal.to_bytes(4, "big"))
    _digest_ascii(content_digest, binding.source_type)
    _digest_ascii(content_digest, binding.identity_kind)
    content_digest.update(bytes.fromhex(binding.identity_sha256))
    content_digest.update(binding.artifact_sha256)
    numeric_values = (
        binding.artifact_byte_count,
        binding.provider_group_count,
        binding.matched_ein_count,
        binding.missing_count,
        binding.malformed_count,
        binding.unsupported_type_count,
    )
    for numeric_value in numeric_values:
        content_digest.update(numeric_value.to_bytes(8, "big"))


def _validated_header(
    sidecar_file: BinaryIO,
    artifact_digest: Any,
    *,
    policy_id: str,
) -> None:
    policy_bytes = policy_id.encode("ascii")
    header_byte_count = 13 + len(policy_bytes)
    header_bytes = sidecar_file.read(header_byte_count)
    artifact_digest.update(header_bytes)
    if (
        len(header_bytes) != header_byte_count
        or header_bytes[:8] != _MAGIC
        or int.from_bytes(header_bytes[8:10], "little") != _VERSION
        or int.from_bytes(header_bytes[10:12], "little") != _RECORD_BYTES
        or header_bytes[12] != len(policy_bytes)
        or header_bytes[13:] != policy_bytes
    ):
        raise _fail()


def _copy_sidecar_records(
    sidecar_file: BinaryIO,
    output_file: BinaryIO,
    *,
    binding: _SourceBinding,
    artifact_digest: Any,
    content_digest: Any,
) -> dict[str, int]:
    counts_by_state = {name: 0 for name in _STATE_COUNT_FIELDS}
    previous_group_id: bytes | None = None
    for record_ordinal in range(binding.provider_group_count):
        record_bytes = sidecar_file.read(_RECORD_BYTES)
        artifact_digest.update(record_bytes)
        if len(record_bytes) != _RECORD_BYTES:
            raise _fail()
        provider_group_id = record_bytes[:16]
        identity_state = _STATE_BY_CODE.get(record_bytes[16])
        tin_id_128 = record_bytes[17:33]
        full_hmac = record_bytes[33:65]
        if (
            identity_state is None
            or previous_group_id is not None
            and provider_group_id <= previous_group_id
            or identity_state == "matched_ein"
            and tin_id_128 != full_hmac[:16]
            or identity_state != "matched_ein"
            and (any(tin_id_128) or any(full_hmac))
        ):
            raise _fail()
        previous_group_id = provider_group_id
        counts_by_state[f"{identity_state}_count"] += 1
        content_digest.update(binding.source_key.to_bytes(4, "big"))
        content_digest.update(record_ordinal.to_bytes(8, "big"))
        content_digest.update(provider_group_id)
        content_digest.update(bytes((_STATE_CODE[identity_state],)))
        content_digest.update(
            full_hmac if identity_state == "matched_ein" else bytes(32)
        )
        _write_copy_row(
            output_file,
            binding=binding,
            record_ordinal=record_ordinal,
            provider_group_id=provider_group_id,
            identity_state=identity_state,
            tin_id_128=tin_id_128 if identity_state == "matched_ein" else None,
            tin_hmac_sha256=full_hmac if identity_state == "matched_ein" else None,
        )
    return counts_by_state


def _parse_source_sidecar(
    binding: _SourceBinding,
    *,
    output_file: BinaryIO,
    content_digest: Any,
    policy_id: str,
) -> dict[str, int]:
    sidecar_file, path_metadata = _open_source_sidecar(binding)
    artifact_digest = hashlib.sha256()
    try:
        _validated_header(sidecar_file, artifact_digest, policy_id=policy_id)
        counts_by_state = _copy_sidecar_records(
            sidecar_file,
            output_file,
            binding=binding,
            artifact_digest=artifact_digest,
            content_digest=content_digest,
        )
        trailing_bytes = sidecar_file.read(1)
        artifact_digest.update(trailing_bytes)
        if trailing_bytes:
            raise _fail()
        has_expected_digest = hmac.compare_digest(
            artifact_digest.digest(), binding.artifact_sha256
        )
        has_expected_counts = all(
            counts_by_state[name] == getattr(binding, name)
            for name in _STATE_COUNT_FIELDS
        )
        if not (
            has_expected_digest
            and has_expected_counts
            and _is_source_unchanged(sidecar_file, binding.path, path_metadata)
        ):
            raise _fail()
        return counts_by_state
    finally:
        sidecar_file.close()


def _strict_digest_bytes(raw_digest: object) -> bytes:
    if not isinstance(raw_digest, (bytes, bytearray, memoryview)):
        raise _fail()
    digest_bytes = bytes(raw_digest)
    if len(digest_bytes) != 32:
        raise _fail()
    return digest_bytes


def _validated_projection_inputs(
    bound_sidecars: Iterable[Mapping[str, Any]],
    *,
    token_policy_id: str,
    token_policy_descriptor_sha256: bytes,
    source_ordinal_map: Iterable[Mapping[str, Any]],
    source_ordinal_map_digest: bytes,
    aggregate_tax_content_digest: bytes,
) -> _ProjectionInputs:
    policy_id = _strict_policy(token_policy_id)
    policy_descriptor = _strict_digest_bytes(token_policy_descriptor_sha256)
    ordinal_digest = _strict_digest_bytes(source_ordinal_map_digest)
    aggregate_digest = _strict_digest_bytes(aggregate_tax_content_digest)
    ordinal_by_shard, rebuilt_ordinal_digest = _source_ordinal_by_shard(
        source_ordinal_map
    )
    if not hmac.compare_digest(rebuilt_ordinal_digest, ordinal_digest):
        raise _fail()
    bindings = _validated_bindings(
        bound_sidecars,
        source_ordinal_by_shard=ordinal_by_shard,
        token_policy_id=policy_id,
    )
    return _ProjectionInputs(
        policy_id=policy_id,
        policy_descriptor=policy_descriptor,
        ordinal_digest=ordinal_digest,
        aggregate_digest=aggregate_digest,
        bindings=bindings,
    )


def _projection_content_digest(inputs: _ProjectionInputs) -> Any:
    content_digest = hashlib.sha256()
    content_digest.update(_CONTENT_DOMAIN)
    _digest_ascii(content_digest, PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT)
    _digest_ascii(content_digest, inputs.policy_id)
    content_digest.update(inputs.policy_descriptor)
    content_digest.update(inputs.ordinal_digest)
    content_digest.update(inputs.aggregate_digest)
    content_digest.update(len(inputs.bindings).to_bytes(4, "big"))
    for binding in inputs.bindings:
        _hash_binding(content_digest, binding)
    return content_digest


def _write_projection_copy(
    output_path: Path,
    inputs: _ProjectionInputs,
) -> _ProjectionCopySummary:
    open_flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
    open_flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    output_descriptor = os.open(output_path, open_flags, 0o600)
    totals_by_state = {name: 0 for name in _STATE_COUNT_FIELDS}
    occurrence_count = 0
    content_digest = _projection_content_digest(inputs)
    with os.fdopen(output_descriptor, "w+b", closefd=True) as output_file:
        output_file.write(_PG_COPY_HEADER)
        for binding in inputs.bindings:
            counts_by_state = _parse_source_sidecar(
                binding,
                output_file=output_file,
                content_digest=content_digest,
                policy_id=inputs.policy_id,
            )
            occurrence_count += binding.provider_group_count
            for count_name, state_count in counts_by_state.items():
                totals_by_state[count_name] += state_count
        output_file.write(_PG_COPY_TRAILER)
        copy_sha256, copy_metadata = _authenticated_open_copy(
            output_file,
            output_path,
        )
    return _ProjectionCopySummary(
        occurrence_count=occurrence_count,
        counts_by_state=totals_by_state,
        content_digest=content_digest.digest(),
        copy_sha256=copy_sha256,
        copy_byte_count=copy_metadata.st_size,
        copy_device=copy_metadata.st_dev,
        copy_inode=copy_metadata.st_ino,
        copy_mtime_ns=copy_metadata.st_mtime_ns,
    )


def _authenticated_open_copy(
    output_file: BinaryIO,
    output_path: Path,
) -> tuple[str, os.stat_result]:
    """Hash the original creation descriptor and freeze its exact identity."""

    output_file.flush()
    initial_metadata = os.fstat(output_file.fileno())
    output_file.seek(0)
    copy_digest = hashlib.sha256()
    observed_byte_count = 0
    while file_chunk := output_file.read(_COPY_READ_BYTES):
        copy_digest.update(file_chunk)
        observed_byte_count += len(file_chunk)
    final_metadata = os.fstat(output_file.fileno())
    path_metadata = os.lstat(output_path)
    if (
        observed_byte_count != initial_metadata.st_size
        or not _has_same_file_identity(initial_metadata, final_metadata)
        or not _has_same_file_identity(final_metadata, path_metadata)
    ):
        raise _fail()
    return copy_digest.hexdigest(), final_metadata


def prepare_tax_identity_source_projection(
    bound_sidecars: Iterable[Mapping[str, Any]],
    *,
    output_path: str | Path,
    token_policy_id: str,
    token_policy_descriptor_sha256: bytes,
    source_ordinal_map: Iterable[Mapping[str, Any]],
    source_ordinal_map_digest: bytes,
    aggregate_tax_content_digest: bytes,
) -> PreparedTaxIdentitySourceProjection:
    """Authenticate every source sidecar and emit one bounded-memory COPY."""

    copy_path = Path(output_path)
    try:
        inputs = _validated_projection_inputs(
            bound_sidecars,
            token_policy_id=token_policy_id,
            token_policy_descriptor_sha256=token_policy_descriptor_sha256,
            source_ordinal_map=source_ordinal_map,
            source_ordinal_map_digest=source_ordinal_map_digest,
            aggregate_tax_content_digest=aggregate_tax_content_digest,
        )
        copy_summary = _write_projection_copy(copy_path, inputs)
        return PreparedTaxIdentitySourceProjection(
            copy_path=copy_path,
            copy_sha256=copy_summary.copy_sha256,
            copy_byte_count=copy_summary.copy_byte_count,
            copy_device=copy_summary.copy_device,
            copy_inode=copy_summary.copy_inode,
            copy_mtime_ns=copy_summary.copy_mtime_ns,
            bindings=inputs.bindings,
            token_policy_id=inputs.policy_id,
            token_policy_descriptor_sha256=inputs.policy_descriptor,
            source_ordinal_map_digest=inputs.ordinal_digest,
            aggregate_tax_content_digest=inputs.aggregate_digest,
            provider_group_occurrence_count=copy_summary.occurrence_count,
            matched_ein_count=copy_summary.counts_by_state["matched_ein_count"],
            missing_count=copy_summary.counts_by_state["missing_count"],
            malformed_count=copy_summary.counts_by_state["malformed_count"],
            unsupported_type_count=copy_summary.counts_by_state[
                "unsupported_type_count"
            ],
            content_digest=copy_summary.content_digest,
        )
    except TaxIdentitySourceProjectionError:
        _remove_ephemeral_copy(copy_path)
        raise
    except Exception:
        _remove_ephemeral_copy(copy_path)
        raise _fail() from None


def _is_copy_file_unchanged(
    copy_file: BinaryIO,
    prepared: PreparedTaxIdentitySourceProjection,
) -> bool:
    try:
        initial_metadata = os.fstat(copy_file.fileno())
        if not _has_prepared_copy_identity(initial_metadata, prepared):
            return False
        copy_file.seek(0)
        observed_sha256 = hashlib.sha256()
        observed_byte_count = 0
        while file_chunk := copy_file.read(_COPY_READ_BYTES):
            observed_sha256.update(file_chunk)
            observed_byte_count += len(file_chunk)
        opened_metadata = os.fstat(copy_file.fileno())
        current_metadata = os.lstat(prepared.copy_path)
        return (
            observed_byte_count == prepared.copy_byte_count
            and hmac.compare_digest(observed_sha256.hexdigest(), prepared.copy_sha256)
            and _has_prepared_copy_identity(opened_metadata, prepared)
            and _has_prepared_copy_identity(current_metadata, prepared)
        )
    except Exception:
        return False


__all__ = ["prepare_tax_identity_source_projection"]
