# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Receipt validation contract for the native hospital-price parser."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any


HOSPITAL_MRF_LEGACY_SCHEMA_REVISION = "hospital-mrf-packed-blocks-v1"
HOSPITAL_MRF_LEGACY_SUMMARY_CONTRACT = "hospital-mrf-copy-v3-packed-v1"
HOSPITAL_MRF_LEGACY_PARSER_CONTRACT = (
    f"{HOSPITAL_MRF_LEGACY_SUMMARY_CONTRACT}-resource-bounded:"
    f"{HOSPITAL_MRF_LEGACY_SCHEMA_REVISION}"
)
HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256 = hashlib.sha256(
    HOSPITAL_MRF_LEGACY_PARSER_CONTRACT.encode("ascii")
).hexdigest()
HOSPITAL_MRF_PACKED_V2_PARSER_CONTRACT_SHA256 = hashlib.sha256(
    b"hospital-mrf-copy-v3-packed-v2-resource-bounded:"
    b"hospital-mrf-packed-blocks-v2"
).hexdigest()
HOSPITAL_MRF_PACKED_V3_PARSER_CONTRACT_SHA256 = hashlib.sha256(
    b"hospital-mrf-copy-v2-v3-packed-v3-resource-bounded:"
    b"hospital-mrf-packed-blocks-v3"
).hexdigest()
HOSPITAL_MRF_PACKED_V4_PARSER_CONTRACT_SHA256 = hashlib.sha256(
    b"hospital-mrf-copy-v2-v3-packed-v4-resource-bounded:"
    b"hospital-mrf-packed-blocks-v3"
).hexdigest()
HOSPITAL_MRF_SCHEMA_REVISION = "hospital-mrf-packed-blocks-v3"
HOSPITAL_MRF_SUMMARY_CONTRACT = "hospital-mrf-copy-v2-v3-packed-v5"
HOSPITAL_MRF_PARSER_CONTRACT = (
    f"{HOSPITAL_MRF_SUMMARY_CONTRACT}-resource-bounded:"
    f"{HOSPITAL_MRF_SCHEMA_REVISION}"
)
HOSPITAL_MRF_PARSER_CONTRACT_SHA256 = hashlib.sha256(
    HOSPITAL_MRF_PARSER_CONTRACT.encode("ascii")
).hexdigest()
HOSPITAL_MRF_TEXT_COPY_COLUMNS = {
    "mrf": (
        "version_id", "source_hospital_name", "last_updated_on", "template_version",
        "attestation_text", "confirm_attestation", "attester_name",
        "financial_aid_policy",
    ),
    "location": (
        "version_id", "location_ordinal", "location_name", "hospital_address",
    ),
    "npi": ("version_id", "npi_ordinal", "npi"),
    "license": (
        "version_id", "license_ordinal", "license_number", "state",
    ),
    "contract_provision": (
        "version_id", "provision_ordinal", "payer_name", "plan_name",
        "provisions",
    ),
    "modifier": (
        "version_id", "modifier_ordinal", "code", "description", "setting",
        "additional_generic_notes",
    ),
    "modifier_payer": (
        "version_id", "modifier_ordinal", "payer_ordinal", "payer_name",
        "plan_name", "description", "standard_charge_dollar",
        "standard_charge_percentage", "standard_charge_algorithm",
    ),
}
HOSPITAL_MRF_PACKED_COPY_COLUMNS = (
    "version_id", "block_kind", "block_ordinal", "logical_first",
    "logical_count", "secondary_first", "secondary_count", "page_index",
    "page_count", "key_sha256", "parent_sha256", "payload_sha256", "payload",
)
HOSPITAL_MRF_BINARY_COPY_KINDS = frozenset(
    {"service_block", "fact_block", "selector_page"}
)
HOSPITAL_MRF_COPY_COLUMNS = {
    **HOSPITAL_MRF_TEXT_COPY_COLUMNS,
    **{
        kind: HOSPITAL_MRF_PACKED_COPY_COLUMNS
        for kind in ("service_block", "fact_block", "selector_page")
    },
}
_REQUIRED_NONEMPTY_RELATIONS = frozenset({"mrf", "location", "license"})
_SOURCE_SCHEMA_VERSIONS = {
    "json": frozenset({"2.2.0", "2.2.1", "3.0.0"}),
    "csv-tall": frozenset({"2", "2.0.0", "2.2.0", "2.2.1", "3.0.0"}),
    "csv-wide": frozenset({"2", "2.0.0", "2.2.0", "2.2.1", "3.0.0"}),
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_U64_MAX = (1 << 64) - 1
_PG_BINARY_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + b"\0" * 8
_PG_BINARY_COPY_TRAILER = b"\xff\xff"


@dataclass(frozen=True)
class HospitalParserArtifact:
    kind: str
    path: Path
    rows: int
    bytes: int
    sha256: str


@dataclass(frozen=True)
class HospitalPackedRoot:
    service_count: int
    charge_count: int
    fact_count: int
    code_selector_key_count: int
    payer_plan_selector_key_count: int
    code_selector_ref_count: int
    payer_plan_selector_ref_count: int
    code_selector_page_count: int
    payer_plan_selector_page_count: int
    service_block_count: int
    fact_block_count: int
    code_selector_block_count: int
    payer_plan_selector_block_count: int
    selector_spool_bytes: int
    peak_scratch_bytes: int


_PACKED_ROOT_FIELDS = tuple(HospitalPackedRoot.__dataclass_fields__)


@dataclass(frozen=True)
class HospitalParserReceipt:
    version_id: str
    source_format: str
    schema_version: str
    semantic_sha256: str
    max_fanout_rows: int
    max_decompressed_bytes: int
    max_output_bytes: int
    artifacts: tuple[HospitalParserArtifact, ...]
    root: HospitalPackedRoot

    def artifact(self, kind: str) -> HospitalParserArtifact:
        """Return the artifact for one known COPY relation."""

        return self.artifacts[tuple(HOSPITAL_MRF_COPY_COLUMNS).index(kind)]


def _is_u64(number: Any, *, positive: bool = False) -> bool:
    return (
        type(number) is int
        and (number > 0 if positive else number >= 0)
        and number <= _U64_MAX
    )


def _parser_artifact(
    artifact_fields: Any, *, kind: str, output_directory: Path
) -> HospitalParserArtifact:
    if (
        not isinstance(artifact_fields, dict)
        or set(artifact_fields) != {"kind", "path", "rows", "bytes", "sha256"}
        or artifact_fields.get("kind") != kind
    ):
        raise ValueError(f"hospital parser artifact {kind} is missing")
    raw_path = artifact_fields.get("path")
    path = Path(raw_path) if isinstance(raw_path, str) else Path()
    expected_path = output_directory / f"{kind}.copy"
    try:
        metadata = path.stat(follow_symlinks=False)
        resolved_path = path.resolve(strict=True)
    except OSError as exc:
        raise ValueError(
            f"hospital parser artifact {kind} has an unsafe path"
        ) from exc
    if (
        resolved_path != expected_path
        or path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
    ):
        raise ValueError(f"hospital parser artifact {kind} has an unsafe path")
    row_count = artifact_fields.get("rows")
    byte_count = artifact_fields.get("bytes")
    digest = artifact_fields.get("sha256")
    is_binary = kind in HOSPITAL_MRF_BINARY_COPY_KINDS
    if (
        not _is_u64(row_count)
        or not _is_u64(byte_count)
        or metadata.st_size != byte_count
        or not isinstance(digest, str) or _SHA256.fullmatch(digest) is None
        or (kind in _REQUIRED_NONEMPTY_RELATIONS and row_count == 0)
        or (kind == "mrf" and row_count != 1)
        or (not is_binary and (row_count > 0) != (byte_count > 0))
        or (is_binary and byte_count < 21)
        or (is_binary and row_count == 0 and byte_count != 21)
        or (is_binary and row_count > 0 and byte_count == 21)
    ):
        raise ValueError(f"hospital parser artifact {kind} is invalid")
    with path.open("rb") as artifact_stream:
        actual_digest = hashlib.file_digest(artifact_stream, "sha256").hexdigest()
        if is_binary:
            artifact_stream.seek(0)
            header = artifact_stream.read(len(_PG_BINARY_COPY_HEADER))
            artifact_stream.seek(-len(_PG_BINARY_COPY_TRAILER), os.SEEK_END)
            trailer = artifact_stream.read()
            if header != _PG_BINARY_COPY_HEADER or trailer != _PG_BINARY_COPY_TRAILER:
                raise ValueError(
                    f"hospital parser artifact {kind} is not PostgreSQL binary COPY"
                )
    if actual_digest != digest:
        raise ValueError(f"hospital parser artifact {kind} is invalid")
    return HospitalParserArtifact(kind, resolved_path, row_count, byte_count, digest)


def _is_parser_contract_valid(
    summary_fields: dict[str, Any], version_id: str, source_format: str,
    input_bytes: int, max_decompressed_bytes: int, max_output_bytes: int,
) -> bool:
    return (
        summary_fields["contract"] == HOSPITAL_MRF_SUMMARY_CONTRACT
        and summary_fields["version_id"] == version_id
        and isinstance(summary_fields["schema_version"], str)
        and summary_fields["schema_version"]
        in _SOURCE_SCHEMA_VERSIONS.get(source_format, ())
        and summary_fields["schema_revision"] == HOSPITAL_MRF_SCHEMA_REVISION
        and summary_fields["format"] == source_format
        and source_format in {"json", "csv-tall", "csv-wide"}
        and _is_u64(input_bytes)
        and _is_u64(summary_fields["compressed_input_bytes"])
        and summary_fields["compressed_input_bytes"] == input_bytes
        and _is_u64(summary_fields["max_fanout_rows"], positive=True)
        and _is_u64(summary_fields["max_decompressed_bytes"], positive=True)
        and summary_fields["max_decompressed_bytes"] == max_decompressed_bytes
        and _is_u64(max_decompressed_bytes, positive=True)
        and _is_u64(summary_fields["max_output_bytes"], positive=True)
        and summary_fields["max_output_bytes"] == max_output_bytes
        and _is_u64(max_output_bytes, positive=True)
        and isinstance(summary_fields["artifacts"], list)
        and len(summary_fields["artifacts"]) == len(HOSPITAL_MRF_COPY_COLUMNS)
        and isinstance(summary_fields["root"], dict)
    )


def _checked_add(left: int, right: int, message: str) -> int:
    if right > _U64_MAX - left:
        raise ValueError(message)
    return left + right


def _checked_multiply(left: int, right: int, message: str) -> int:
    if left and right > _U64_MAX // left:
        raise ValueError(message)
    return left * right


def _has_valid_root_counts(packed_root: HospitalPackedRoot) -> bool:
    payer_count_values = (
        packed_root.payer_plan_selector_key_count,
        packed_root.payer_plan_selector_ref_count,
        packed_root.payer_plan_selector_page_count,
        packed_root.payer_plan_selector_block_count,
    )
    return not (
        packed_root.service_count == 0
        or packed_root.charge_count < packed_root.service_count
        or packed_root.service_block_count == 0
        or packed_root.code_selector_key_count == 0
        or packed_root.code_selector_ref_count < packed_root.charge_count
        or packed_root.code_selector_page_count < packed_root.code_selector_key_count
        or packed_root.code_selector_page_count > packed_root.code_selector_ref_count
        or packed_root.code_selector_block_count == 0
        or packed_root.code_selector_block_count > packed_root.code_selector_page_count
        or packed_root.payer_plan_selector_ref_count != packed_root.fact_count
        or (packed_root.fact_count == 0) != all(
            count == 0 for count in payer_count_values
        )
        or (packed_root.fact_count > 0) != all(
            count > 0 for count in payer_count_values
        )
        or (packed_root.fact_count == 0) != (packed_root.fact_block_count == 0)
        or (
            packed_root.fact_count > 0
            and (
                packed_root.fact_block_count > packed_root.fact_count
                or packed_root.payer_plan_selector_page_count
                < packed_root.payer_plan_selector_key_count
            )
        )
        or packed_root.payer_plan_selector_block_count
        > packed_root.payer_plan_selector_page_count
        or packed_root.payer_plan_selector_page_count
        > packed_root.payer_plan_selector_ref_count
    )


def _has_matching_artifact_counts(
    packed_root: HospitalPackedRoot,
    artifacts: tuple[HospitalParserArtifact, ...],
) -> bool:
    artifacts_by_kind = {artifact.kind: artifact for artifact in artifacts}
    return (
        artifacts_by_kind["service_block"].rows == packed_root.service_block_count
        and artifacts_by_kind["fact_block"].rows == packed_root.fact_block_count
        and artifacts_by_kind["selector_page"].rows
        == packed_root.code_selector_block_count
        + packed_root.payer_plan_selector_block_count
    )


def _validate_root_scratch(
    packed_root: HospitalPackedRoot, max_output_bytes: int
) -> None:
    reference_count = _checked_add(
        packed_root.code_selector_ref_count,
        packed_root.payer_plan_selector_ref_count,
        "hospital parser root reference count overflows",
    )
    expected_spool_bytes = _checked_multiply(
        reference_count,
        13,
        "hospital parser root selector spool count overflows",
    )
    expected_peak_bytes = _checked_multiply(
        expected_spool_bytes,
        3,
        "hospital parser root scratch count overflows",
    )
    if (
        packed_root.selector_spool_bytes != expected_spool_bytes
        or packed_root.peak_scratch_bytes != expected_peak_bytes
    ):
        raise ValueError("hospital parser root scratch counts are invalid")
    if packed_root.peak_scratch_bytes > max_output_bytes:
        raise ValueError("hospital parser scratch exceeds its output-derived limit")


def _packed_root(
    root_fields: Any,
    artifacts: tuple[HospitalParserArtifact, ...],
    max_output_bytes: int,
) -> HospitalPackedRoot:
    """Validate packed root geometry and its bounded scratch receipt."""

    if not isinstance(root_fields, dict) or set(root_fields) != set(_PACKED_ROOT_FIELDS):
        raise ValueError("hospital parser root shape is invalid")
    if any(not _is_u64(root_fields[field]) for field in _PACKED_ROOT_FIELDS):
        raise ValueError("hospital parser root counts are invalid")
    packed_root = HospitalPackedRoot(**root_fields)
    if not _has_valid_root_counts(packed_root) or not _has_matching_artifact_counts(
        packed_root, artifacts
    ):
        raise ValueError("hospital parser root counts are invalid")
    _validate_root_scratch(packed_root, max_output_bytes)
    return packed_root


def _decode_summary(summary_bytes: bytes) -> dict[str, Any]:
    if len(summary_bytes) > 1_000_000:
        raise ValueError("hospital parser summary is oversized")
    try:
        summary_fields = json.loads(summary_bytes)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("hospital parser summary is invalid JSON") from exc
    expected_fields = {
        "contract", "version_id", "schema_version", "schema_revision", "format",
        "compressed_input_bytes", "max_fanout_rows", "max_decompressed_bytes",
        "max_output_bytes", "artifacts", "root",
    }
    if not isinstance(summary_fields, dict) or expected_fields != set(summary_fields):
        raise ValueError("hospital parser summary shape is invalid")
    return summary_fields


def _validated_output_directory(output_directory: str | Path) -> Path:
    output_path = Path(output_directory)
    if output_path.is_symlink() or not output_path.is_dir():
        raise ValueError("hospital parser output directory is unsafe")
    return output_path.resolve()


def _validated_artifact_tuple(
    artifact_records: list[Any], output_directory: Path
) -> tuple[HospitalParserArtifact, ...]:
    return tuple(
        _parser_artifact(
            artifact_fields, kind=kind, output_directory=output_directory
        )
        for kind, artifact_fields in zip(HOSPITAL_MRF_COPY_COLUMNS, artifact_records)
    )


def _retained_artifact_bytes(
    artifacts: tuple[HospitalParserArtifact, ...]
) -> int:
    retained_byte_count = 0
    for artifact in artifacts:
        retained_byte_count = _checked_add(
            retained_byte_count,
            artifact.bytes,
            "hospital parser artifact byte count overflows",
        )
    return retained_byte_count


def _semantic_sha256(
    artifacts: tuple[HospitalParserArtifact, ...], packed_root: HospitalPackedRoot
) -> str:
    digest = hashlib.sha256()
    for artifact in artifacts:
        digest.update(
            f"{artifact.kind}\0{artifact.rows}\0{artifact.sha256}\n".encode("ascii")
        )
    canonical_root_dict = {
        field: getattr(packed_root, field) for field in _PACKED_ROOT_FIELDS
    }
    digest.update(b"root\0")
    digest.update(json.dumps(
        canonical_root_dict, sort_keys=True, separators=(",", ":")
    ).encode("ascii"))
    return digest.hexdigest()


def validate_hospital_parser_summary(
    summary_bytes: bytes, *,
    version_id: str,
    source_format: str,
    input_bytes: int,
    output_directory: str | Path,
    max_decompressed_bytes: int,
    max_output_bytes: int,
) -> HospitalParserReceipt:
    """Validate the bounded native receipt and its private COPY paths."""

    if _SHA256.fullmatch(version_id) is None:
        raise ValueError("hospital version identity is invalid")
    summary_fields = _decode_summary(summary_bytes)
    if not _is_parser_contract_valid(
        summary_fields, version_id, source_format, input_bytes,
        max_decompressed_bytes, max_output_bytes,
    ):
        raise ValueError("hospital parser summary contract is invalid")
    output_path = _validated_output_directory(output_directory)
    artifacts = _validated_artifact_tuple(summary_fields["artifacts"], output_path)
    if summary_fields["schema_version"] == "3.0.0" and not next(
        artifact.rows for artifact in artifacts if artifact.kind == "npi"
    ):
        raise ValueError("hospital parser v3 NPI artifact is empty")
    if _retained_artifact_bytes(artifacts) > max_output_bytes:
        raise ValueError("hospital parser artifacts exceed their output limit")
    packed_root = _packed_root(summary_fields["root"], artifacts, max_output_bytes)
    return HospitalParserReceipt(
        version_id=version_id,
        source_format=source_format,
        schema_version=summary_fields["schema_version"],
        semantic_sha256=_semantic_sha256(artifacts, packed_root),
        max_fanout_rows=summary_fields["max_fanout_rows"],
        max_decompressed_bytes=summary_fields["max_decompressed_bytes"],
        max_output_bytes=max_output_bytes,
        artifacts=artifacts,
        root=packed_root,
    )
