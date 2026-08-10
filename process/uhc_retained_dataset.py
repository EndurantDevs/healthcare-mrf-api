# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained-only UHC semantic-set and canonical-dataset materialization.

This module deliberately has no HTTP client dependency.  Its only inputs are
the immutable catalog/admission rows, their retained ``file://`` artifacts,
and SEALED semantic stages produced by the native v3 encoder.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import time
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Iterable,
    Mapping,
)
import urllib.parse
import zlib

import asyncpg

from process.provider_directory_resource_hash import resource_payload_sha256
from process.uhc_canonical_proof import (
    UhcCanonicalContentDigest,
    UhcCanonicalMaterializationIdentity,
    UhcCanonicalNpiProof,
    UhcCanonicalProofBuilder,
    canonical_materialization_proof,
)
from process.provider_directory_source_summary import (
    SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS,
    SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS,
    SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_KEY,
    SOURCE_SUMMARY_UHC_SELECTED_RESOURCES,
    SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID,
)
from process.uhc_provider_file_identity import (
    PAIRING_UNPAIRED_RETAINED_ONLY,
    UHCLogicalScope,
    UHCSourceFileDescriptor,
    logical_scope_for_file,
)
from process.uhc_provider_file_semantic_identity import (
    UHCPlanKey,
    network_key_for_plan,
    plan_key_for_scope,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from process.uhc_provider_quarantine_contract import (
    UHC_PROVIDER_QUARANTINE_COUNTER_BY_RAW_FIELD,
    UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
    UHC_PROVIDER_QUARANTINE_REASONS,
    UHC_PROVIDER_QUARANTINE_REJECTED_COUNT_FIELDS,
    UhcProviderQuarantineError,
    provider_quarantine_catalog_limit,
    provider_quarantine_limit,
    provider_quarantine_rejected_counts,
    provider_quarantine_rejected_totals,
    validate_provider_quarantine_fact,
)
from process.uhc_provider_quarantine_raw_verifier import (
    UhcProviderQuarantineRawSource,
)
from process.uhc_semantic_build_store import (
    UHC_SEMANTIC_CONTRACT_ID,
    UHC_SEMANTIC_CONTRACT_VERSION,
    UhcSemanticBuildClaim,
    UhcSemanticBuildError,
    UhcSemanticBuildIdentity,
    claim_uhc_semantic_build,
    copy_uhc_semantic_stage,
    load_sealed_uhc_semantic_build,
    prepare_uhc_semantic_stage_indexes,
    quarantine_uhc_semantic_build,
    seal_uhc_semantic_build,
)
from process.uhc_semantic_evidence import (
    UhcNpiEvidenceSummary,
    summarize_uhc_npi_evidence_stages,
)
from process.uhc_semantic_verifier_identity import (
    semantic_verifier_identity_sha256,
)
from process.uhc_semantic_stage_verifier import (
    verify_sealed_uhc_semantic_build,
    verify_uhc_semantic_stage,
)


UHC_RETAINED_SUMMARY_INPUT_CONTRACT_ID = (
    "healthporta.uhc.retained-summary-input.v1"
)
UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY = "uhc_retained_summary_input_v1"
UHC_RETAINED_PUBLICATION_CONTRACT_ID = (
    "healthporta.uhc.retained-publication.v1"
)
UHC_RETAINED_PUBLICATION_METADATA_KEY = "uhc_retained_publication_v1"
UHC_RETAINED_SOURCE_ID = UHC_PROVIDER_FILE_SOURCE_ID
UHC_RETAINED_CANONICAL_CONTRACT_ID = (
    "healthporta.uhc.provider-directory-canonical.v2"
)
UHC_TIN_STATUS_UNAVAILABLE = "unavailable_from_uhc_source"
UHC_PROVIDER_PLAN_RELATIONSHIP_TYPE = (
    "payer_reported_provider_plan_membership"
)
UHC_OWNERSHIP_STATUS_NOT_ASSERTED = "not_asserted"

_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_STDERR_BYTES = 8 * 1024 * 1024
_MAX_FACT_RECORD_BYTES = 64 * 1024 * 1024
_FACT_READ_CHUNK_BYTES = 1024 * 1024
DEFAULT_SEMANTIC_FILE_CONCURRENCY = 2
MAX_SEMANTIC_FILE_CONCURRENCY = 4
_COPY_BATCH_ROWS = 4096
_SUMMARY_ADDITIVE_COUNTERS = tuple(
    field
    for field in SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS
    if field
    not in {
        "distinct_npis",
        "duplicate_npi_groups",
        "conflicting_npi_groups",
        "provider_file_count",
        "plan_file_count",
        "membership_plan_key_count",
        "detail_plan_key_count",
        "matched_plan_key_count",
        "missing_plan_detail_count",
        "orphan_plan_detail_count",
    }
)


class UhcRetainedDatasetError(RuntimeError):
    """Fail closed when retained UHC publication evidence is incomplete."""


@dataclass(frozen=True)
class UhcAdmittedFile:
    catalog_set_sha256: str
    source_file_id: str
    family: str
    collection_kind: str
    file_name: str
    artifact_sha256: str
    artifact_byte_count: int
    raw_contract_version: int
    raw_range_count: int
    record_count: int
    range_set_sha256: str
    manifest_sha256: str
    raw_producer_build_id: str
    raw_path: Path
    manifest_path: Path

    @property
    def logical_scope(self) -> UHCLogicalScope:
        """Return the reviewed market/product scope for this exact basename."""

        return logical_scope_for_file(
            UHCSourceFileDescriptor(
                family=self.family,
                collection_kind=self.collection_kind,
                file_name=self.file_name,
            )
        )

    def semantic_identity(self, encoder_sha256: str) -> UhcSemanticBuildIdentity:
        """Bind this admitted file to one immutable semantic encoder."""

        return UhcSemanticBuildIdentity(
            catalog_set_sha256=self.catalog_set_sha256,
            source_file_id=self.source_file_id,
            artifact_sha256=self.artifact_sha256,
            raw_contract_version=self.raw_contract_version,
            raw_range_count=self.raw_range_count,
            manifest_sha256=self.manifest_sha256,
            range_set_sha256=self.range_set_sha256,
            raw_record_count=self.record_count,
            raw_producer_build_id=self.raw_producer_build_id,
            collection_kind=self.collection_kind,
            encoder_sha256=encoder_sha256,
            semantic_verifier_sha256=(
                semantic_verifier_identity_sha256()
            ),
        )


@dataclass(frozen=True)
class UhcAdmittedCatalogSet:
    catalog_set_sha256: str
    files: tuple[UhcAdmittedFile, ...]
    provider_file_count: int
    plan_file_count: int


@dataclass(frozen=True)
class UhcSealedSemanticFile:
    admitted: UhcAdmittedFile
    identity: UhcSemanticBuildIdentity
    build_row: Mapping[str, Any]

    @property
    def stage_ref(self) -> str:
        """Return the quoted semantic stage relation identity."""

        return (
            f"{self.build_row['stage_schema']}."
            f"{self.build_row['stage_relation']}"
        )


@dataclass(frozen=True)
class UhcCanonicalStage:
    schema: str
    resource_relation: str
    auxiliary_relations: tuple[str, ...]
    resource_counts: dict[str, int]
    content_proof: dict[str, Any]
    summary_input: dict[str, Any]
    semantic_build_ids: tuple[str, ...]
    phase_metrics: dict[str, Any]

    @property
    def resource_ref(self) -> str:
        """Return the quoted canonical resource-stage identity."""

        return _qualified(self.schema, self.resource_relation)


def _schema_name() -> str:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if _IDENTIFIER_RE.fullmatch(schema) is None:
        raise UhcRetainedDatasetError("invalid retained UHC schema")
    return schema


def _quoted(identifier: str) -> str:
    if _IDENTIFIER_RE.fullmatch(identifier) is None:
        raise UhcRetainedDatasetError("invalid retained UHC identifier")
    return f'"{identifier}"'


def _qualified(schema: str, relation: str) -> str:
    return f"{_quoted(schema)}.{_quoted(relation)}"


def _table(relation: str) -> str:
    return _qualified(_schema_name(), relation)


def _require_sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise UhcRetainedDatasetError(f"retained UHC {field} is not a SHA-256")
    return value


def _positive_int(value: Any, field: str, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise UhcRetainedDatasetError(f"retained UHC {field} is invalid")
    return value


def _required_printable_text(
    value: Any,
    field: str,
    *,
    max_length: int,
) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > max_length
        or not value.isascii()
        or not value.isprintable()
    ):
        raise UhcRetainedDatasetError(f"retained UHC {field} is invalid")
    return value


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as error:
            raise UhcRetainedDatasetError(
                f"retained UHC {field} is invalid"
            ) from error
    if not isinstance(value, Mapping):
        raise UhcRetainedDatasetError(f"retained UHC {field} is invalid")
    return dict(value)


def _retained_file_path(value: Any, field: str) -> Path:
    if not isinstance(value, str):
        raise UhcRetainedDatasetError(f"retained UHC {field} URI is invalid")
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "file"
        or parsed.netloc not in {"", "localhost"}
        or parsed.query
        or parsed.fragment
    ):
        raise UhcRetainedDatasetError(f"retained UHC {field} URI is invalid")
    path = Path(urllib.parse.unquote(parsed.path))
    try:
        path_stat = os.stat(path, follow_symlinks=False)
    except OSError as error:
        raise UhcRetainedDatasetError(
            f"retained UHC {field} file is unavailable"
        ) from error
    if (
        not path.is_absolute()
        or not stat.S_ISREG(path_stat.st_mode)
        or path_stat.st_nlink != 1
        or path_stat.st_mode & 0o022
    ):
        raise UhcRetainedDatasetError(
            f"retained UHC {field} file is unsafe"
        )
    return path


def _catalog_set_query() -> str:
    """Return the strict admitted-catalog lineage query."""

    return f"""
        SELECT catalog.catalog_set_sha256, catalog.file_count AS catalog_file_count,
               catalog.provider_file_count, catalog.plan_reference_file_count,
               file.file_id AS source_file_id, file.family, file.collection_kind,
               file.file_name, file.availability, file.catalog_support,
               binding.artifact_sha256,
               binding.released_at AS binding_released_at,
               artifact.byte_count AS artifact_byte_count,
               artifact.storage_uri AS raw_storage_uri,
               artifact.status AS artifact_status,
               layout.contract_version AS raw_contract_version,
               layout.range_count AS raw_range_count,
               layout.record_count,
               layout.range_set_sha256,
               layout.manifest_sha256,
               layout.producer_build_id AS raw_producer_build_id,
               layout.manifest_storage_uri,
               layout.status AS layout_status,
               raw_ref.storage_uri AS raw_reference_uri,
               raw_ref.released_at AS raw_reference_released_at,
               manifest_ref.storage_uri AS manifest_reference_uri,
               manifest_ref.released_at AS manifest_reference_released_at,
               (
                   SELECT count(*)
                     FROM {_table('provider_directory_uhc_raw_range')} AS range
                    WHERE range.artifact_sha256=layout.artifact_sha256
                      AND range.contract_version=layout.contract_version
                      AND range.range_count=layout.range_count
                      AND range.status='verified'
               )::bigint AS verified_range_count
          FROM {_table('provider_directory_uhc_catalog_set')} AS catalog
          JOIN {_table('provider_directory_uhc_catalog_file')} AS file
            ON file.catalog_set_sha256=catalog.catalog_set_sha256
          LEFT JOIN {_table('provider_directory_uhc_source_binding')} AS binding
            ON binding.catalog_set_sha256=file.catalog_set_sha256
           AND binding.source_file_id=file.file_id
          LEFT JOIN {_table('provider_directory_uhc_raw_artifact')} AS artifact
            ON artifact.artifact_sha256=binding.artifact_sha256
          LEFT JOIN {_table('provider_directory_uhc_raw_layout')} AS layout
            ON layout.artifact_sha256=binding.artifact_sha256
          LEFT JOIN {_table('provider_directory_uhc_artifact_reference')} AS raw_ref
            ON raw_ref.catalog_set_sha256=binding.catalog_set_sha256
           AND raw_ref.source_file_id=binding.source_file_id
           AND raw_ref.artifact_kind='raw'
           AND raw_ref.contract_version=0
           AND raw_ref.range_count=0
          LEFT JOIN {_table('provider_directory_uhc_artifact_reference')} AS manifest_ref
            ON manifest_ref.catalog_set_sha256=binding.catalog_set_sha256
           AND manifest_ref.source_file_id=binding.source_file_id
           AND manifest_ref.artifact_kind='manifest'
           AND manifest_ref.layout_artifact_sha256=layout.artifact_sha256
           AND manifest_ref.contract_version=layout.contract_version
           AND manifest_ref.range_count=layout.range_count
         WHERE catalog.catalog_set_sha256=$1
         ORDER BY file.family, file.collection_kind, file.file_name, file.file_id
    """


def _catalog_file_counts(
    admitted_bindings: list[Any],
) -> tuple[int, int, int]:
    first_binding = admitted_bindings[0]
    catalog_file_count = _positive_int(
        first_binding["catalog_file_count"],
        "file_count",
    )
    provider_file_count = _positive_int(
        first_binding["provider_file_count"],
        "provider_file_count",
    )
    plan_file_count = _positive_int(
        first_binding["plan_reference_file_count"],
        "plan_reference_file_count",
    )
    if (
        len(admitted_bindings) != catalog_file_count
        or catalog_file_count != provider_file_count + plan_file_count
    ):
        raise UhcRetainedDatasetError(
            "retained UHC catalog set file counts are incomplete"
        )
    return catalog_file_count, provider_file_count, plan_file_count


def _validated_binding_identity(
    binding: Mapping[str, Any],
    seen_source_file_ids: set[str],
) -> tuple[str, str, str, int, int]:
    source_file_id = _require_sha256(binding["source_file_id"], "source_file_id")
    artifact_sha256 = _require_sha256(binding["artifact_sha256"], "artifact_sha256")
    collection_kind = binding["collection_kind"]
    has_invalid_binding = (
        source_file_id in seen_source_file_ids
        or collection_kind not in {"provider_membership", "plan_reference"}
        or binding["family"] not in {"cs", "ifp"}
        or binding["availability"] != "published"
        or binding["catalog_support"] != "cataloged"
        or binding["binding_released_at"] is not None
        or binding["artifact_status"] != "verified"
        or binding["layout_status"] != "verified"
        or binding["raw_reference_released_at"] is not None
        or binding["manifest_reference_released_at"] is not None
    )
    if has_invalid_binding:
        raise UhcRetainedDatasetError(
            "retained UHC catalog set has an inactive or ambiguous binding"
        )
    contract_version = _positive_int(binding["raw_contract_version"], "raw_contract_version")
    range_count = _positive_int(binding["raw_range_count"], "raw_range_count")
    verified_range_count = _positive_int(
        binding["verified_range_count"],
        "verified_range_count",
    )
    if not 4 <= range_count <= 256 or verified_range_count != range_count:
        raise UhcRetainedDatasetError(
            "retained UHC catalog set range proof is incomplete"
        )
    return source_file_id, artifact_sha256, str(collection_kind), contract_version, range_count


def _validated_binding_paths(binding: Mapping[str, Any]) -> tuple[Path, Path]:
    raw_path = _retained_file_path(binding["raw_storage_uri"], "raw")
    raw_reference_path = _retained_file_path(
        binding["raw_reference_uri"],
        "raw reference",
    )
    manifest_path = _retained_file_path(binding["manifest_storage_uri"], "manifest")
    manifest_reference_path = _retained_file_path(
        binding["manifest_reference_uri"],
        "manifest reference",
    )
    if raw_path != raw_reference_path or manifest_path != manifest_reference_path:
        raise UhcRetainedDatasetError(
            "retained UHC artifact references disagree with admitted layout"
        )
    return raw_path, manifest_path


def _admitted_uhc_file(
    binding: Mapping[str, Any],
    catalog_hash: str,
    seen_source_file_ids: set[str],
) -> UhcAdmittedFile:
    source_id, artifact_digest, collection_kind, contract_version, range_count = (
        _validated_binding_identity(binding, seen_source_file_ids)
    )
    raw_path, manifest_path = _validated_binding_paths(binding)
    return UhcAdmittedFile(
        catalog_set_sha256=catalog_hash,
        source_file_id=source_id,
        family=str(binding["family"]),
        collection_kind=collection_kind,
        file_name=str(binding["file_name"]),
        artifact_sha256=artifact_digest,
        artifact_byte_count=_positive_int(binding["artifact_byte_count"], "artifact_byte_count"),
        raw_contract_version=contract_version,
        raw_range_count=range_count,
        record_count=_positive_int(binding["record_count"], "record_count"),
        range_set_sha256=_require_sha256(binding["range_set_sha256"], "range_set_sha256"),
        manifest_sha256=_require_sha256(binding["manifest_sha256"], "manifest_sha256"),
        raw_producer_build_id=_required_printable_text(
            binding["raw_producer_build_id"],
            "raw_producer_build_id",
            max_length=256,
        ),
        raw_path=raw_path,
        manifest_path=manifest_path,
    )


async def load_complete_admitted_uhc_catalog_set(
    connection: asyncpg.Connection,
    catalog_set_sha256: str,
) -> UhcAdmittedCatalogSet:
    """Load only one exact catalog whose every published file is admitted."""

    catalog_hash = _require_sha256(catalog_set_sha256, "catalog_set_sha256")
    admitted_bindings = await connection.fetch(
        _catalog_set_query(),
        catalog_hash,
    )
    if not admitted_bindings:
        raise UhcRetainedDatasetError("retained UHC catalog set was not found")
    _catalog_file_count, provider_file_count, plan_file_count = (
        _catalog_file_counts(admitted_bindings)
    )
    admitted_files: list[UhcAdmittedFile] = []
    seen_source_file_ids: set[str] = set()
    observed_count_by_kind = {
        "provider_membership": 0,
        "plan_reference": 0,
    }
    for binding in admitted_bindings:
        admitted_file = _admitted_uhc_file(binding, catalog_hash, seen_source_file_ids)
        admitted_files.append(admitted_file)
        seen_source_file_ids.add(admitted_file.source_file_id)
        observed_count_by_kind[admitted_file.collection_kind] += 1
    if observed_count_by_kind != {
        "provider_membership": provider_file_count,
        "plan_reference": plan_file_count,
    }:
        raise UhcRetainedDatasetError(
            "retained UHC catalog set collection counts are incomplete"
        )
    return UhcAdmittedCatalogSet(
        catalog_set_sha256=catalog_hash,
        files=tuple(admitted_files),
        provider_file_count=provider_file_count,
        plan_file_count=plan_file_count,
    )


def uhc_semantic_binary() -> Path:
    """Resolve the separately packaged native semantic encoder."""

    configured = os.getenv("HLTHPRT_UHC_SEMANTIC_BIN")
    if configured:
        candidate = Path(configured)
    else:
        scanner = os.getenv("HLTHPRT_PTG2_RUST_SCANNER_BIN")
        if not scanner:
            raise UhcRetainedDatasetError(
                "retained UHC publication requires HLTHPRT_UHC_SEMANTIC_BIN"
            )
        candidate = Path(scanner).with_name("uhc_semantic_facts")
    try:
        resolved = candidate.resolve(strict=True)
        resolved_stat = os.stat(resolved, follow_symlinks=False)
    except OSError as error:
        raise UhcRetainedDatasetError(
            "retained UHC semantic binary is unavailable"
        ) from error
    if not stat.S_ISREG(resolved_stat.st_mode) or not os.access(resolved, os.X_OK):
        raise UhcRetainedDatasetError(
            "retained UHC semantic binary is not executable"
        )
    return resolved


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


async def _read_bounded_stderr(stream: asyncio.StreamReader) -> bytes:
    output = bytearray()
    while chunk := await stream.read(64 * 1024):
        output.extend(chunk)
        if len(output) > _MAX_STDERR_BYTES:
            raise UhcRetainedDatasetError(
                "retained UHC semantic stderr exceeded its bound"
            )
    return bytes(output)


class _CopyStreamObservation:
    def __init__(self) -> None:
        self.byte_count = 0
        self.digest = hashlib.sha256()

    def observe(self, chunk: bytes) -> None:
        """Add one emitted COPY chunk to the exact byte proof."""

        self.byte_count += len(chunk)
        self.digest.update(chunk)

    @property
    def sha256(self) -> str:
        """Return the digest for every emitted COPY byte."""

        return self.digest.hexdigest()


async def _stdout_chunks(
    stream: asyncio.StreamReader,
    observation: _CopyStreamObservation,
) -> AsyncIterator[bytes]:
    while chunk := await stream.read(1024 * 1024):
        observation.observe(chunk)
        yield chunk


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        await process.wait()
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        await process.wait()


def _semantic_arguments(
    binary: Path,
    admitted: UhcAdmittedFile,
) -> tuple[str, ...]:
    return (
        str(binary),
        "--input",
        str(admitted.raw_path),
        "--manifest",
        str(admitted.manifest_path),
        "--output",
        "-",
        "--artifact-sha256",
        admitted.artifact_sha256,
        "--artifact-byte-count",
        str(admitted.artifact_byte_count),
        "--manifest-sha256",
        admitted.manifest_sha256,
        "--range-set-sha256",
        admitted.range_set_sha256,
        "--record-count",
        str(admitted.record_count),
        "--range-count",
        str(admitted.raw_range_count),
        "--source-file-id",
        admitted.source_file_id,
        "--source-binding-id",
        f"{admitted.catalog_set_sha256}/{admitted.source_file_id}",
        "--collection-kind",
        admitted.collection_kind,
    )


def _native_report(stderr: bytes) -> dict[str, Any]:
    try:
        decoded = stderr.decode("utf-8").strip()
        report = json.loads(decoded)
    except (UnicodeDecodeError, ValueError) as error:
        raise UhcRetainedDatasetError(
            "retained UHC semantic native report is invalid"
        ) from error
    if not isinstance(report, dict):
        raise UhcRetainedDatasetError(
            "retained UHC semantic native report is invalid"
        )
    return report


async def _reused_semantic_file(
    connection: asyncpg.Connection,
    admitted: UhcAdmittedFile,
    identity: UhcSemanticBuildIdentity,
    claim: UhcSemanticBuildClaim,
) -> UhcSealedSemanticFile | None:
    if not claim.sealed_reuse:
        return None
    build_row = await load_sealed_uhc_semantic_build(connection, identity)
    if build_row is None:
        raise UhcRetainedDatasetError(
            "retained UHC SEALED semantic build disappeared"
        )
    await verify_sealed_uhc_semantic_build(connection, identity, build_row)
    return UhcSealedSemanticFile(admitted, identity, build_row)


async def _seal_native_semantic_report(
    connection: asyncpg.Connection,
    admitted: UhcAdmittedFile,
    identity: UhcSemanticBuildIdentity,
    claim: UhcSemanticBuildClaim,
    report: dict[str, Any],
    copy_observation: dict[str, Any],
) -> UhcSealedSemanticFile:
    await prepare_uhc_semantic_stage_indexes(connection, claim)
    verifier_report = await verify_uhc_semantic_stage(
        connection,
        claim,
        identity,
        report,
        copy_observation=copy_observation,
        quarantine_source=UhcProviderQuarantineRawSource(
            raw_path=admitted.raw_path,
            manifest_path=admitted.manifest_path,
            artifact_sha256=admitted.artifact_sha256,
            artifact_byte_count=admitted.artifact_byte_count,
            raw_contract_version=admitted.raw_contract_version,
            manifest_sha256=admitted.manifest_sha256,
            range_set_sha256=admitted.range_set_sha256,
            record_count=admitted.record_count,
            range_count=admitted.raw_range_count,
            raw_producer_build_id=admitted.raw_producer_build_id,
            source_file_id=admitted.source_file_id,
        ),
    )
    await seal_uhc_semantic_build(
        connection,
        claim,
        identity,
        report,
        verifier_report,
    )
    build_row = await load_sealed_uhc_semantic_build(connection, identity)
    if build_row is None:
        raise UhcRetainedDatasetError(
            "retained UHC semantic seal was not persisted"
        )
    return UhcSealedSemanticFile(admitted, identity, build_row)


async def _quarantine_failed_semantic_build(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
    process: asyncio.subprocess.Process | None,
    stderr_task: asyncio.Task[bytes] | None,
) -> None:
    if process is not None:
        await _terminate_process(process)
    if stderr_task is not None and not stderr_task.done():
        stderr_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await stderr_task
    with contextlib.suppress(Exception):
        await quarantine_uhc_semantic_build(
            connection,
            claim,
            failure_code="native_or_independent_verification_failed",
        )


async def _start_semantic_encoder(
    binary: Path,
    admitted: UhcAdmittedFile,
) -> tuple[asyncio.subprocess.Process, asyncio.Task[bytes]]:
    process = await asyncio.create_subprocess_exec(
        *_semantic_arguments(binary, admitted),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    if process.stdout is None or process.stderr is None:
        raise UhcRetainedDatasetError(
            "retained UHC semantic process pipes are unavailable"
        )
    return process, asyncio.create_task(_read_bounded_stderr(process.stderr))


async def _consume_semantic_encoder(
    connection: asyncpg.Connection,
    admitted: UhcAdmittedFile,
    identity: UhcSemanticBuildIdentity,
    claim: UhcSemanticBuildClaim,
    process: asyncio.subprocess.Process,
    stderr_task: asyncio.Task[bytes],
) -> UhcSealedSemanticFile:
    assert process.stdout is not None
    copy_observation = _CopyStreamObservation()
    copied_row_count = await copy_uhc_semantic_stage(
        connection,
        claim,
        _stdout_chunks(process.stdout, copy_observation),
    )
    return_code = await process.wait()
    stderr = await stderr_task
    if return_code != 0:
        raise UhcRetainedDatasetError(
            "retained UHC semantic encoder failed: "
            + stderr.decode("utf-8", errors="replace")[-2000:]
        )
    observed_copy_by_field = {
        "output_bytes": copy_observation.byte_count,
        "output_sha256": copy_observation.sha256,
        "copy_row_count": copied_row_count,
    }
    return await _seal_native_semantic_report(
        connection,
        admitted,
        identity,
        claim,
        _native_report(stderr),
        observed_copy_by_field,
    )


async def _build_semantic_file(
    connection: asyncpg.Connection,
    admitted: UhcAdmittedFile,
    binary: Path,
    identity: UhcSemanticBuildIdentity,
    claim: UhcSemanticBuildClaim,
) -> UhcSealedSemanticFile:
    process: asyncio.subprocess.Process | None = None
    stderr_task: asyncio.Task[bytes] | None = None
    try:
        process, stderr_task = await _start_semantic_encoder(binary, admitted)
        return await _consume_semantic_encoder(
            connection,
            admitted,
            identity,
            claim,
            process,
            stderr_task,
        )
    except BaseException:
        await _quarantine_failed_semantic_build(
            connection,
            claim,
            process,
            stderr_task,
        )
        raise


async def _run_one_semantic_build(
    connection: asyncpg.Connection,
    admitted: UhcAdmittedFile,
    binary: Path,
    encoder_sha256: str,
) -> UhcSealedSemanticFile:
    """Reuse or build, verify, and seal one admitted semantic file."""

    identity = admitted.semantic_identity(encoder_sha256)
    claim = await claim_uhc_semantic_build(connection, identity)
    reused_file = await _reused_semantic_file(
        connection,
        admitted,
        identity,
        claim,
    )
    if reused_file is not None:
        return reused_file

    return await _build_semantic_file(
        connection,
        admitted,
        binary,
        identity,
        claim,
    )


def uhc_semantic_file_concurrency() -> int:
    """Return the bounded number of simultaneous file encoders."""

    raw_value = os.getenv(
        "HLTHPRT_UHC_SEMANTIC_FILE_CONCURRENCY",
        str(DEFAULT_SEMANTIC_FILE_CONCURRENCY),
    )
    try:
        concurrency = int(raw_value)
    except ValueError as error:
        raise UhcRetainedDatasetError(
            "UHC semantic file concurrency is invalid"
        ) from error
    if not 1 <= concurrency <= MAX_SEMANTIC_FILE_CONCURRENCY:
        raise UhcRetainedDatasetError(
            "UHC semantic file concurrency must be in "
            f"1..={MAX_SEMANTIC_FILE_CONCURRENCY}"
        )
    return concurrency


async def _run_parallel_semantic_builds(
    admitted_files: tuple[UhcAdmittedFile, ...],
    binary: Path,
    encoder_sha256: str,
    connection_factory: Callable[[], AsyncContextManager[Any]],
    concurrency: int,
) -> list[UhcSealedSemanticFile]:
    """Build files concurrently while giving each COPY its own connection."""

    semaphore = asyncio.Semaphore(concurrency)

    async def run_file(
        admitted_file: UhcAdmittedFile,
    ) -> UhcSealedSemanticFile:
        """Build one retained file while holding a bounded worker slot."""
        async with semaphore:
            async with connection_factory() as worker_connection:
                return await _run_one_semantic_build(
                    worker_connection,
                    admitted_file,
                    binary,
                    encoder_sha256,
                )

    tasks = [
        asyncio.create_task(run_file(admitted_file))
        for admitted_file in admitted_files
    ]
    try:
        return list(await asyncio.gather(*tasks))
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _validated_semantic_file_concurrency(
    requested_concurrency: int | None,
) -> int:
    concurrency = (
        uhc_semantic_file_concurrency()
        if requested_concurrency is None
        else requested_concurrency
    )
    if not 1 <= concurrency <= MAX_SEMANTIC_FILE_CONCURRENCY:
        raise UhcRetainedDatasetError(
            "UHC semantic file concurrency must be in "
            f"1..={MAX_SEMANTIC_FILE_CONCURRENCY}"
        )
    return concurrency


def _require_complete_semantic_set(
    sealed_files: list[UhcSealedSemanticFile],
    admitted_set: UhcAdmittedCatalogSet,
    encoder_sha256: str,
) -> None:
    sealed_source_ids = {
        sealed_file.admitted.source_file_id for sealed_file in sealed_files
    }
    admitted_source_ids = {
        admitted_file.source_file_id for admitted_file in admitted_set.files
    }
    encoder_hashes = {
        sealed_file.identity.encoder_sha256 for sealed_file in sealed_files
    }
    if (
        len(sealed_files) != len(admitted_set.files)
        or sealed_source_ids != admitted_source_ids
        or encoder_hashes != {encoder_sha256}
    ):
        raise UhcRetainedDatasetError(
            "retained UHC semantic set is incomplete or mixed"
        )


async def ensure_sealed_uhc_semantic_set(
    connection: asyncpg.Connection | None,
    admitted_set: UhcAdmittedCatalogSet,
    *,
    binary: Path | None = None,
    connection_factory: (
        Callable[[], AsyncContextManager[Any]] | None
    ) = None,
    file_concurrency: int | None = None,
) -> tuple[UhcSealedSemanticFile, ...]:
    """Build/reuse and independently prove every file in one admitted set."""

    semantic_binary = binary or uhc_semantic_binary()
    encoder_sha256 = await asyncio.to_thread(_sha256_file, semantic_binary)
    if connection_factory is None:
        if connection is None:
            raise UhcRetainedDatasetError(
                "UHC semantic build connection is unavailable"
            )
        sealed_files = [
            await _run_one_semantic_build(
                connection,
                admitted_file,
                semantic_binary,
                encoder_sha256,
            )
            for admitted_file in admitted_set.files
        ]
    else:
        sealed_files = await _run_parallel_semantic_builds(
            admitted_set.files,
            semantic_binary,
            encoder_sha256,
            connection_factory,
            _validated_semantic_file_concurrency(file_concurrency),
        )
    _require_complete_semantic_set(
        sealed_files,
        admitted_set,
        encoder_sha256,
    )
    return tuple(sealed_files)


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _payload_hash(payload: Mapping[str, Any]) -> str:
    """Match the retained candidate's explicit transport-neutral contract."""

    return resource_payload_sha256(payload)


def _resource_id(prefix: str, *identity: Any) -> str:
    digest = hashlib.sha256(_stable_json(identity).encode()).hexdigest()[:48]
    return f"{prefix}-{digest}"


def _clean_text(value: Any, *, upper: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.replace("\x00", "").strip()
    if not cleaned:
        return None
    return cleaned.upper() if upper else cleaned


def _plan_years(plan: Mapping[str, Any]) -> tuple[int, ...]:
    years = plan.get("years")
    if (
        not isinstance(years, list)
        or not years
        or any(
            isinstance(year, bool)
            or not isinstance(year, int)
            or not 2000 <= year <= 9999
            for year in years
        )
    ):
        raise UhcRetainedDatasetError("retained UHC plan years are invalid")
    normalized_years = tuple(sorted(set(years)))
    if len(normalized_years) != len(years):
        raise UhcRetainedDatasetError("retained UHC plan years are not unique")
    return normalized_years


def _plan_key(
    scope: UHCLogicalScope,
    plan: Mapping[str, Any],
    plan_year: int,
) -> UHCPlanKey:
    plan_id_type = _clean_text(plan.get("plan_id_type"))
    plan_id = _clean_text(plan.get("plan_id"))
    if not plan_id_type or not plan_id:
        raise UhcRetainedDatasetError("retained UHC plan key is empty")
    return plan_key_for_scope(
        scope,
        plan_id_type=plan_id_type,
        plan_id=plan_id,
        plan_year=plan_year,
    )


def _plan_key_payload(plan_key: UHCPlanKey) -> dict[str, Any]:
    scope = plan_key.scope
    return {
        "logical_scope_id": plan_key.logical_scope_id,
        "family": scope.family,
        "market": scope.market,
        "product": scope.product,
        "jurisdiction": scope.jurisdiction,
        "plan_id_type": plan_key.plan_id_type,
        "plan_id": plan_key.plan_id,
        "plan_year": plan_key.plan_year,
        "plan_key_id": plan_key.plan_key_id,
    }


def _plan_resource_id(plan_key: UHCPlanKey) -> str:
    return f"uhcplan-{plan_key.plan_key_id[:48]}"


def _address_payload(address: Mapping[str, Any]) -> dict[str, Any]:
    first_line = _clean_text(address.get("address"))
    second_line = _clean_text(address.get("address_2"))
    city = _clean_text(address.get("city"))
    state = _clean_text(address.get("state"), upper=True)
    postal_code = _clean_text(address.get("zip"))
    payload: dict[str, Any] = {
        "line": [line for line in (first_line, second_line) if line],
        "city": city,
        "state": state,
        "postalCode": postal_code,
        "country": "US",
    }
    return {key: value for key, value in payload.items() if value not in (None, [])}


def _telecom(addresses: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    phones = []
    seen_phones = set()
    for address in addresses:
        phone = _clean_text(address.get("phone"))
        if phone and phone not in seen_phones:
            phones.append({"system": "phone", "value": phone})
            seen_phones.add(phone)
    return phones


def _phone_digits(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    digits = "".join(character for character in text if character.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else None


def _provider_name(record: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str | None, list[str], str | None]:
    name = record.get("name")
    if not isinstance(name, Mapping):
        return [], None, [], None
    family = _clean_text(name.get("last"))
    given_names = [
        value
        for field in ("first", "middle")
        if (value := _clean_text(name.get(field)))
    ]
    name_by_field = {
        **({"family": family} if family else {}),
        **({"given": given_names} if given_names else {}),
    }
    full_name = (
        " ".join([*given_names, *([family] if family else [])]) or None
    )
    return (
        [name_by_field] if name_by_field else [],
        family,
        given_names,
        full_name,
    )


def _canonical_row(
    resource_type: str,
    resource_id: str,
    payload: dict[str, Any],
    source_rank: str,
) -> tuple[str, str, str, str, str]:
    payload = {"resource_id": resource_id, **payload}
    return (
        resource_type,
        resource_id,
        _payload_hash(payload),
        json.dumps(payload, sort_keys=True, default=str),
        source_rank,
    )


@dataclass(frozen=True)
class _ProviderResourceContext:
    provider_type: str
    npi: str
    addresses: list[Mapping[str, Any]]
    plans: list[Mapping[str, Any]]
    provider_id: str
    source_rank: str
    address_payloads: list[dict[str, Any]]
    telecom: list[dict[str, str]]
    location_ids: list[str]
    display_name: str | None
    source_lineage: dict[str, Any]


@dataclass(frozen=True)
class _ProviderPlanIteration:
    source_file_id: str
    ordinal: int
    plan_index: int
    plan_year: int
    year_index: int
    logical_scope: UHCLogicalScope


def _provider_resource_context(
    provider_fact: Mapping[str, Any],
    source_file_id: str,
    ordinal: int,
    source_lineage: Mapping[str, Any],
) -> _ProviderResourceContext:
    provider_type = _clean_text(provider_fact.get("type"), upper=True)
    npi = _clean_text(provider_fact.get("npi"))
    addresses = provider_fact.get("addresses")
    plans = provider_fact.get("plans")
    if (
        provider_type not in {"INDIVIDUAL", "FACILITY"}
        or not npi
        or not isinstance(addresses, list)
        or not addresses
        or not all(isinstance(address, Mapping) for address in addresses)
        or not isinstance(plans, list)
        or not plans
        or not all(isinstance(plan, Mapping) for plan in plans)
    ):
        raise UhcRetainedDatasetError(
            "SEALED retained UHC provider fact has an invalid shape"
        )
    provider_id = _resource_id("uhcprv", source_file_id, ordinal)
    display_name = (
        _provider_name(provider_fact)[3]
        if provider_type == "INDIVIDUAL"
        else _clean_text(provider_fact.get("facility_name"))
    )
    return _ProviderResourceContext(
        provider_type=provider_type,
        npi=npi,
        addresses=addresses,
        plans=plans,
        provider_id=provider_id,
        source_rank=f"{source_file_id}:{ordinal:020d}",
        address_payloads=[_address_payload(address) for address in addresses],
        telecom=_telecom(addresses),
        location_ids=[
            _resource_id("uhcloc", source_file_id, ordinal, address_index)
            for address_index in range(len(addresses))
        ],
        display_name=display_name,
        source_lineage=dict(source_lineage),
    )


def _practitioner_payload(
    provider_fact: Mapping[str, Any],
    context: _ProviderResourceContext,
) -> dict[str, Any]:
    names, family_name, given_names, full_name = _provider_name(provider_fact)
    return {
        "npi": int(context.npi),
        "active": True,
        "identifiers": [
            {"system": "http://hl7.org/fhir/sid/us-npi", "value": context.npi}
        ],
        "names": names,
        "family_name": family_name,
        "given_names": given_names,
        "full_name": full_name,
        "administrative_gender": _clean_text(provider_fact.get("gender")),
        "telecom": context.telecom,
        "addresses": context.address_payloads,
        "qualification_codes": sorted(
            specialty
            for raw_specialty in (provider_fact.get("specialty") or [])
            if (specialty := _clean_text(raw_specialty))
        ),
    }


def _organization_payload(
    provider_fact: Mapping[str, Any],
    context: _ProviderResourceContext,
) -> dict[str, Any]:
    return {
        "npi": int(context.npi),
        "active": True,
        "identifiers": [
            {"system": "http://hl7.org/fhir/sid/us-npi", "value": context.npi}
        ],
        "name": context.display_name or context.npi,
        "type_codes": sorted(
            facility_type
            for raw_facility_type in (provider_fact.get("facility_type") or [])
            if (facility_type := _clean_text(raw_facility_type))
        ),
        "telecom": context.telecom,
        "address_json": context.address_payloads,
        "tax_id": None,
        "tin_status": UHC_TIN_STATUS_UNAVAILABLE,
        "source_lineage": context.source_lineage,
    }


def _provider_base_row(
    provider_fact: Mapping[str, Any],
    context: _ProviderResourceContext,
) -> tuple[str, str, str, str, str]:
    if context.provider_type == "INDIVIDUAL":
        resource_type = "Practitioner"
        payload_by_field = _practitioner_payload(provider_fact, context)
    else:
        resource_type = "Organization"
        payload_by_field = _organization_payload(provider_fact, context)
    return _canonical_row(
        resource_type,
        context.provider_id,
        payload_by_field,
        context.source_rank,
    )


def _location_payload(
    address: Mapping[str, Any],
    address_payload: dict[str, Any],
    context: _ProviderResourceContext,
) -> dict[str, Any]:
    phone = _clean_text(address.get("phone"))
    postal_code = _clean_text(address.get("zip"))
    city = _clean_text(address.get("city"))
    state = _clean_text(address.get("state"), upper=True)
    return {
        "status": "active",
        "name": context.display_name,
        "first_line": _clean_text(address.get("address")),
        "city_name": city,
        "state_name": state,
        "state_code": state if state and len(state) == 2 else None,
        "postal_code": postal_code,
        "zip5": postal_code[:5] if postal_code else None,
        "city_norm": city.lower() if city else None,
        "country_code": "US",
        "telephone_number": phone,
        "phone_number": _phone_digits(phone),
        "telecom": ([{"system": "phone", "value": phone}] if phone else []),
        "addresses": [address_payload],
        "managing_organization_ref": (
            f"Organization/{context.provider_id}"
            if context.provider_type == "FACILITY"
            else None
        ),
    }


def _provider_location_rows(
    context: _ProviderResourceContext,
) -> list[tuple[str, str, str, str, str]]:
    return [
        _canonical_row(
            "Location",
            location_id,
            _location_payload(address, address_payload, context),
            f"{context.source_rank}:{address_index:08d}",
        )
        for address_index, (address, address_payload, location_id) in enumerate(
            zip(
                context.addresses,
                context.address_payloads,
                context.location_ids,
                strict=True,
            )
        )
    ]


def _plan_common_fields(
    provider_fact: Mapping[str, Any],
    plan: Mapping[str, Any],
    context: _ProviderResourceContext,
    plan_id: str,
    plan_key: UHCPlanKey,
) -> dict[str, Any]:
    network_tier = _clean_text(plan.get("network_tier"))
    network_key = (
        network_key_for_plan(plan_key, network_tier=network_tier)
        if network_tier
        else None
    )
    return {
        "npi": int(context.npi),
        "active": True,
        "location_refs": [
            f"Location/{location_id}" for location_id in context.location_ids
        ],
        "network_refs": [],
        "insurance_plan_refs": [f"InsurancePlan/{plan_id}"],
        "specialty_codes": sorted(
            specialty
            for raw_specialty in (provider_fact.get("specialty") or [])
            if (specialty := _clean_text(raw_specialty))
        ),
        "telecom": context.telecom,
        "accepting_patients": (
            {"code": accepting}
            if (accepting := _clean_text(provider_fact.get("accepting")))
            else None
        ),
        "period_start": f"{plan_key.plan_year:04d}-01-01",
        "period_end": f"{plan_key.plan_year:04d}-12-31",
        "plan_scope": _plan_key_payload(plan_key),
        "network_tier": network_tier,
        "network_key_id": network_key.network_key_id if network_key else None,
    }


def _provider_plan_relationship_fields(
    context: _ProviderResourceContext,
    iteration: _ProviderPlanIteration,
    common_by_field: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    """Return resource identity and payload for one provider-plan edge."""
    if context.provider_type == "INDIVIDUAL":
        resource_id = _resource_id(
            "uhcrole",
            iteration.source_file_id,
            iteration.ordinal,
            iteration.plan_index,
            iteration.plan_year,
        )
        return (
            "PractitionerRole",
            resource_id,
            {
                **common_by_field,
                "practitioner_ref": f"Practitioner/{context.provider_id}",
            },
        )
    resource_id = _resource_id(
        "uhcaff",
        iteration.source_file_id,
        iteration.ordinal,
        iteration.plan_index,
        iteration.plan_year,
    )
    return (
        "OrganizationAffiliation",
        resource_id,
        {
            field_name: field_value
            for field_name, field_value in {
                **common_by_field,
                "organization_ref": None,
                "participating_organization_ref": (
                    f"Organization/{context.provider_id}"
                ),
                "code_codes": [
                    {
                        "system": (
                            "https://healthporta.com/fhir/CodeSystem/"
                            "provider-directory-relationship"
                        ),
                        "code": UHC_PROVIDER_PLAN_RELATIONSHIP_TYPE,
                        "display": "Payer-reported provider plan membership",
                    }
                ],
                "relationship_type": UHC_PROVIDER_PLAN_RELATIONSHIP_TYPE,
                "ownership_status": UHC_OWNERSHIP_STATUS_NOT_ASSERTED,
                "source_lineage": context.source_lineage,
            }.items()
            if field_name not in {"npi", "accepting_patients"}
        },
    )


def _provider_plan_resource_row(
    provider_fact: Mapping[str, Any],
    context: _ProviderResourceContext,
    plan: Mapping[str, Any],
    iteration: _ProviderPlanIteration,
) -> tuple[tuple[str, str, str, str, str], tuple[str, str, str]]:
    """Build one plan relationship resource and its membership key."""
    plan_key = _plan_key(
        iteration.logical_scope,
        plan,
        iteration.plan_year,
    )
    plan_id = _plan_resource_id(plan_key)
    resource_type, resource_id, payload_by_field = (
        _provider_plan_relationship_fields(
            context,
            iteration,
            _plan_common_fields(
                provider_fact,
                plan,
                context,
                plan_id,
                plan_key,
            ),
        )
    )
    resource_row = _canonical_row(
        resource_type,
        resource_id,
        payload_by_field,
        (
            f"{context.source_rank}:{iteration.plan_index:08d}:"
            f"{iteration.year_index:04d}"
        ),
    )
    membership_key = (
        "membership",
        _stable_json(_plan_key_payload(plan_key)),
        plan_id,
    )
    return resource_row, membership_key


def _provider_plan_rows(
    provider_fact: Mapping[str, Any],
    context: _ProviderResourceContext,
    source_file_id: str,
    ordinal: int,
    logical_scope: UHCLogicalScope,
) -> tuple[list[tuple[str, str, str, str, str]], list[tuple[str, str, str]]]:
    """Build plan relationship rows and exact setwise join keys."""
    resource_rows = []
    key_rows = []
    for plan_index, plan in enumerate(context.plans):
        for year_index, plan_year in enumerate(_plan_years(plan)):
            resource_row, membership_key = _provider_plan_resource_row(
                provider_fact,
                context,
                plan,
                _ProviderPlanIteration(
                    source_file_id=source_file_id,
                    ordinal=ordinal,
                    plan_index=plan_index,
                    plan_year=plan_year,
                    year_index=year_index,
                    logical_scope=logical_scope,
                ),
            )
            resource_rows.append(resource_row)
            key_rows.append(membership_key)
    return resource_rows, key_rows


def _provider_resource_rows(
    provider_fact: Mapping[str, Any],
    *,
    source_file_id: str,
    ordinal: int,
    logical_scope: UHCLogicalScope,
    source_lineage: Mapping[str, Any],
) -> tuple[list[tuple[str, str, str, str, str]], list[tuple[str, str, str]]]:
    """Map one provider fact to canonical resources and plan keys."""

    context = _provider_resource_context(
        provider_fact,
        source_file_id,
        ordinal,
        source_lineage,
    )
    resource_rows = [_provider_base_row(provider_fact, context)]
    resource_rows.extend(_provider_location_rows(context))
    plan_rows, key_rows = _provider_plan_rows(
        provider_fact,
        context,
        source_file_id,
        ordinal,
        logical_scope,
    )
    resource_rows.extend(plan_rows)
    return resource_rows, key_rows


def _plan_detail_rows(
    record: Mapping[str, Any],
    *,
    source_file_id: str,
    ordinal: int,
    logical_scope: UHCLogicalScope,
) -> list[tuple[str, str, str, int, str]]:
    detail_rows = []
    for plan_year in _plan_years(record):
        key = _plan_key(logical_scope, record, plan_year)
        plan_detail_map = {
            **dict(record),
            "years": [plan_year],
            "logical_scope": _plan_key_payload(key),
        }
        detail_rows.append(
            (
                _stable_json(_plan_key_payload(key)),
                _plan_resource_id(key),
                source_file_id,
                ordinal,
                json.dumps(plan_detail_map, sort_keys=True, default=str),
            )
        )
    return detail_rows


def _framed_fact_lines(line_buffer: bytearray) -> Iterable[bytes]:
    while (newline := line_buffer.find(b"\n")) >= 0:
        line = bytes(line_buffer[:newline])
        del line_buffer[: newline + 1]
        if not line:
            raise UhcRetainedDatasetError("SEALED UHC fact framing is invalid")
        yield line


def _decoded_fact_lines(compressed: bytes) -> Iterable[bytes]:
    decoder = zlib.decompressobj()
    line_buffer = bytearray()
    for offset in range(0, len(compressed), _FACT_READ_CHUNK_BYTES):
        pending = compressed[offset : offset + _FACT_READ_CHUNK_BYTES]
        while pending:
            decoded = decoder.decompress(
                pending,
                max_length=_FACT_READ_CHUNK_BYTES,
            )
            pending = decoder.unconsumed_tail
            line_buffer.extend(decoded)
            yield from _framed_fact_lines(line_buffer)
            if len(line_buffer) > _MAX_FACT_RECORD_BYTES:
                raise UhcRetainedDatasetError(
                    "SEALED UHC fact exceeded its memory bound"
                )
    line_buffer.extend(decoder.flush())
    yield from _framed_fact_lines(line_buffer)
    if line_buffer or not decoder.eof or decoder.unused_data:
        raise UhcRetainedDatasetError("SEALED UHC fact compression is incomplete")


def _decoded_semantic_fact(line: bytes) -> dict[str, Any]:
    try:
        semantic_fact = json.loads(line)
    except (UnicodeDecodeError, ValueError) as error:
        raise UhcRetainedDatasetError(
            "SEALED UHC fact JSON is invalid"
        ) from error
    if not isinstance(semantic_fact, dict):
        raise UhcRetainedDatasetError("SEALED UHC fact JSON is not an object")
    return semantic_fact


def _sealed_semantic_stage_ref(build_row: Mapping[str, Any]) -> str:
    return _qualified(
        str(build_row["stage_schema"]),
        str(build_row["stage_relation"]),
    )


async def _fact_records(
    connection: asyncpg.Connection,
    sealed_file: UhcSealedSemanticFile,
) -> AsyncIterator[tuple[int, int, str, dict[str, Any]]]:
    """Stream verified decoded fact records from one sealed stage."""
    build_row = sealed_file.build_row
    stage_ref = _sealed_semantic_stage_ref(build_row)
    blocks = build_row["fact_blocks_json"]
    if isinstance(blocks, str):
        blocks = json.loads(blocks)
    if not isinstance(blocks, list):
        raise UhcRetainedDatasetError("SEALED UHC fact blocks are invalid")
    next_ordinal = 0
    for expected_range_ordinal, block in enumerate(blocks):
        if not isinstance(block, Mapping):
            raise UhcRetainedDatasetError("SEALED UHC fact block is invalid")
        range_ordinal = block.get("range_ordinal")
        record_start = block.get("record_start")
        record_count = block.get("record_count")
        if (
            range_ordinal != expected_range_ordinal
            or record_start != next_ordinal
            or isinstance(record_count, bool)
            or not isinstance(record_count, int)
            or record_count <= 0
        ):
            raise UhcRetainedDatasetError("SEALED UHC fact block order is invalid")
        compressed = await connection.fetchval(
            f"""
            SELECT payload_bytes
              FROM {stage_ref}
             WHERE row_kind=1 AND range_ordinal=$1
            """,
            range_ordinal,
        )
        if not isinstance(compressed, bytes):
            raise UhcRetainedDatasetError("SEALED UHC fact block is missing")
        if hashlib.sha256(compressed).hexdigest() != block.get(
            "compressed_payload_sha256"
        ):
            raise UhcRetainedDatasetError("SEALED UHC fact block hash changed")
        input_sha256 = _require_sha256(
            block.get("semantic_block_sha256"),
            "semantic block hash",
        )
        observed = 0
        for line in _decoded_fact_lines(compressed):
            yield (
                range_ordinal,
                next_ordinal,
                input_sha256,
                _decoded_semantic_fact(line),
            )
            next_ordinal += 1
            observed += 1
        if observed != record_count:
            raise UhcRetainedDatasetError(
                "SEALED UHC fact record count changed"
            )


def _stage_names() -> tuple[str, str, str, str, str]:
    token = hashlib.sha256(os.urandom(32)).hexdigest()[:16]
    return tuple(
        f"provider_directory_uhc_{suffix}_{token}"
        for suffix in ("resource", "plan", "key", "evidence", "sealed")
    )


async def _create_canonical_stages(
    connection: asyncpg.Connection,
    schema: str,
    names: tuple[str, str, str, str, str],
) -> None:
    resource, plan, key, _evidence, _sealed = names
    await connection.execute(
        f"""
        CREATE UNLOGGED TABLE {_qualified(schema, resource)} (
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            source_rank text NOT NULL
        );
        CREATE UNLOGGED TABLE {_qualified(schema, plan)} (
            plan_key text NOT NULL,
            resource_id varchar(256) NOT NULL,
            source_file_id varchar(64) NOT NULL,
            occurrence_ordinal bigint NOT NULL,
            payload_json jsonb NOT NULL
        );
        CREATE UNLOGGED TABLE {_qualified(schema, key)} (
            key_kind varchar(16) NOT NULL,
            plan_key text NOT NULL,
            resource_id varchar(256) NOT NULL
        );
        """
    )


async def _copy_batches(
    connection: asyncpg.Connection,
    relation: str,
    columns: tuple[str, ...],
    rows: list[tuple[Any, ...]],
) -> None:
    if not rows:
        return
    await connection.copy_records_to_table(
        relation,
        schema_name=_schema_name(),
        columns=columns,
        records=rows,
    )
    rows.clear()


@dataclass
class _CanonicalLandingBuffers:
    resource_rows: list[tuple[Any, ...]]
    plan_rows: list[tuple[Any, ...]]
    key_rows: list[tuple[Any, ...]]


def _append_provider_membership_fact(
    buffers: _CanonicalLandingBuffers,
    admitted: UhcAdmittedFile,
    ordinal: int,
    semantic_fact: Mapping[str, Any],
    proof_builder: UhcCanonicalProofBuilder,
    input_lineage: tuple[dict[str, Any], ...],
) -> None:
    logical_scope = admitted.logical_scope
    generated_rows, generated_keys = _provider_resource_rows(
        semantic_fact,
        source_file_id=admitted.source_file_id,
        ordinal=ordinal,
        logical_scope=logical_scope,
        source_lineage={
            "catalog_set_sha256": admitted.catalog_set_sha256,
            "source_file_id": admitted.source_file_id,
            "file_name": Path(admitted.file_name).name,
            "artifact_sha256": admitted.artifact_sha256,
            "record_ordinal": ordinal,
            "logical_scope_id": logical_scope.logical_scope_id,
        },
    )
    proof_builder.observe_rows(generated_rows, input_lineage=input_lineage)
    buffers.resource_rows.extend(generated_rows)
    buffers.key_rows.extend(generated_keys)


def _append_plan_reference_fact(
    buffers: _CanonicalLandingBuffers,
    admitted: UhcAdmittedFile,
    ordinal: int,
    semantic_fact: Mapping[str, Any],
) -> None:
    plan_rows = _plan_detail_rows(
        semantic_fact,
        source_file_id=admitted.source_file_id,
        ordinal=ordinal,
        logical_scope=admitted.logical_scope,
    )
    buffers.plan_rows.extend(plan_rows)
    buffers.key_rows.extend(
        ("detail", plan_key, resource_id)
        for plan_key, resource_id, *_rest in plan_rows
    )


def _append_canonical_fact(
    buffers: _CanonicalLandingBuffers,
    admitted: UhcAdmittedFile,
    range_ordinal: int,
    ordinal: int,
    semantic_fact: Mapping[str, Any],
    proof_builder: UhcCanonicalProofBuilder,
    input_lineage: tuple[dict[str, Any], ...],
) -> None:
    """Append one admitted fact while omitting exact quarantined records."""
    try:
        quarantine = validate_provider_quarantine_fact(
            semantic_fact,
            expected_source_file_id=admitted.source_file_id,
            expected_range_ordinal=range_ordinal,
            expected_occurrence_ordinal=ordinal,
        )
    except UhcProviderQuarantineError as error:
        raise UhcRetainedDatasetError(str(error)) from error
    if quarantine is not None:
        if admitted.collection_kind != "provider_membership":
            raise UhcRetainedDatasetError(
                "SEALED UHC plan fact contains provider quarantine"
            )
        return
    if admitted.logical_scope.pairing_status == PAIRING_UNPAIRED_RETAINED_ONLY:
        if admitted.collection_kind != "provider_membership":
            raise UhcRetainedDatasetError(
                "retained-only UHC scope has an invalid collection kind"
            )
        return
    if admitted.collection_kind == "provider_membership":
        _append_provider_membership_fact(
            buffers,
            admitted,
            ordinal,
            semantic_fact,
            proof_builder,
            input_lineage,
        )
        return
    _append_plan_reference_fact(
        buffers,
        admitted,
        ordinal,
        semantic_fact,
    )


async def _flush_canonical_buffers(
    connection: asyncpg.Connection,
    names: tuple[str, str, str, str, str],
    buffers: _CanonicalLandingBuffers,
    *,
    force: bool,
) -> None:
    resource_relation, plan_relation, key_relation, _evidence, _sealed = names
    copy_specs = (
        (
            resource_relation,
            ("resource_type", "resource_id", "payload_hash", "payload_json", "source_rank"),
            buffers.resource_rows,
        ),
        (
            plan_relation,
            ("plan_key", "resource_id", "source_file_id", "occurrence_ordinal", "payload_json"),
            buffers.plan_rows,
        ),
        (
            key_relation,
            ("key_kind", "plan_key", "resource_id"),
            buffers.key_rows,
        ),
    )
    for relation, columns, copy_rows in copy_specs:
        if force or len(copy_rows) >= _COPY_BATCH_ROWS:
            await _copy_batches(connection, relation, columns, copy_rows)


async def _land_canonical_facts(
    connection: asyncpg.Connection,
    sealed_files: tuple[UhcSealedSemanticFile, ...],
    names: tuple[str, str, str, str, str],
    proof_builder: UhcCanonicalProofBuilder,
) -> None:
    """COPY semantic facts into minimally indexed canonical stages."""

    buffers = _CanonicalLandingBuffers([], [], [])
    for sealed_file in sealed_files:
        admitted = sealed_file.admitted
        async for range_ordinal, ordinal, input_sha256, semantic_fact in _fact_records(
            connection,
            sealed_file,
        ):
            _append_canonical_fact(
                buffers,
                admitted,
                range_ordinal,
                ordinal,
                semantic_fact,
                proof_builder,
                (
                    {
                        "source_file_id": admitted.source_file_id,
                        "range_ordinal": range_ordinal,
                        "input_sha256": input_sha256,
                        "artifact_sha256": admitted.artifact_sha256,
                    },
                ),
            )
            await _flush_canonical_buffers(
                connection,
                names,
                buffers,
                force=False,
            )
    await _flush_canonical_buffers(connection, names, buffers, force=True)


def _validated_plan_key_map(plan_key: str) -> dict[str, Any]:
    """Decode and validate one staged semantic plan key."""
    try:
        plan_key_map = json.loads(plan_key)
    except (ValueError, TypeError) as error:
        raise UhcRetainedDatasetError("retained UHC staged plan key is invalid") from error
    if (
        not isinstance(plan_key_map, dict)
        or set(plan_key_map)
        != {
            "logical_scope_id",
            "family",
            "market",
            "product",
            "jurisdiction",
            "plan_id_type",
            "plan_id",
            "plan_year",
            "plan_key_id",
        }
    ):
        raise UhcRetainedDatasetError("retained UHC staged plan key is invalid")
    return plan_key_map


def _plan_detail_metadata(
    details: list[Any],
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """Normalize retained plan details, marketing names, and network tiers."""
    detail_maps = [
        dict(detail) for detail in details if isinstance(detail, Mapping)
    ]
    marketing_names = sorted(
        {
            marketing_name
            for detail in detail_maps
            if (
                marketing_name := _clean_text(
                    detail.get("marketing_name")
                )
            )
        }
    )
    network_tiers = sorted(
        {
            network_tier
            for detail in detail_maps
            for network in (detail.get("network") or [])
            if isinstance(network, Mapping)
            and (network_tier := _clean_text(network.get("network_tier")))
        }
    )
    return detail_maps, marketing_names, network_tiers


def _plan_payload(
    plan_key: str,
    resource_id: str,
    details: list[Any],
) -> dict[str, Any]:
    """Build one canonical InsurancePlan payload from semantic plan facts."""
    plan_key_map = _validated_plan_key_map(plan_key)
    plan_id_type = _clean_text(plan_key_map.get("plan_id_type"))
    plan_id = _clean_text(plan_key_map.get("plan_id"))
    plan_year = plan_key_map.get("plan_year")
    if (
        not plan_id_type
        or not plan_id
        or isinstance(plan_year, bool)
        or not isinstance(plan_year, int)
    ):
        raise UhcRetainedDatasetError("retained UHC staged plan key is invalid")
    detail_maps, marketing_names, network_tiers = _plan_detail_metadata(
        details
    )
    return {
        "resource_id": resource_id,
        "plan_identifier": (
            f"{plan_id_type}:{plan_id}:{plan_year}:"
            f"{plan_key_map['logical_scope_id']}"
        ),
        "product_identifiers": [
            {"system": plan_id_type, "value": plan_id}
        ],
        "status": "active",
        "name": marketing_names[0] if marketing_names else plan_id,
        "aliases": marketing_names[1:],
        "network_refs": [],
        "coverage_area_refs": [],
        "plan_json": {
            "canonical_contract_id": UHC_RETAINED_CANONICAL_CONTRACT_ID,
            "plan_key": plan_key_map,
            "external_plan_identifier": f"{plan_id_type}:{plan_id}",
            "network_tiers": network_tiers,
            "detail_available": bool(detail_maps),
            "details": detail_maps,
        },
        "period_start": f"{plan_year:04d}-01-01",
        "period_end": f"{plan_year:04d}-12-31",
    }


async def _plan_resource_page(
    connection: asyncpg.Connection,
    plan_relation: str,
    key_relation: str,
    after_key: str | None,
) -> list[Any]:
    return await connection.fetch(
        f"""
        WITH plan_keys AS MATERIALIZED (
            SELECT plan_key, min(resource_id) AS resource_id
              FROM {_qualified(_schema_name(), key_relation)}
             WHERE $1::text IS NULL OR plan_key > $1
             GROUP BY plan_key ORDER BY plan_key LIMIT $2
        )
        SELECT key.plan_key, key.resource_id,
               COALESCE(
                   jsonb_agg(plan.payload_json ORDER BY plan.source_file_id, plan.occurrence_ordinal)
                       FILTER (WHERE plan.plan_key IS NOT NULL),
                   '[]'::jsonb
               ) AS details
          FROM plan_keys AS key
          LEFT JOIN {_qualified(_schema_name(), plan_relation)} AS plan
            ON plan.plan_key=key.plan_key
         GROUP BY key.plan_key, key.resource_id ORDER BY key.plan_key
        """,
        after_key,
        _COPY_BATCH_ROWS,
    )


def _canonical_plan_row(plan_key_row: Mapping[str, Any]) -> tuple[str, ...]:
    details = plan_key_row["details"]
    if isinstance(details, str):
        details = json.loads(details)
    plan_payload_by_field = _plan_payload(
        str(plan_key_row["plan_key"]),
        str(plan_key_row["resource_id"]),
        list(details),
    )
    return _canonical_row(
        "InsurancePlan",
        str(plan_key_row["resource_id"]),
        {
            field_name: field_value
            for field_name, field_value in plan_payload_by_field.items()
            if field_name != "resource_id"
        },
        f"plan:{plan_key_row['plan_key']}",
    )


def _semantic_input_lineage(
    sealed_files: tuple[UhcSealedSemanticFile, ...],
) -> tuple[dict[str, Any], ...]:
    lineage_descriptors = []
    for sealed_file in sorted(
        sealed_files,
        key=lambda item: item.admitted.source_file_id,
    ):
        blocks = sealed_file.build_row["fact_blocks_json"]
        if isinstance(blocks, str):
            blocks = json.loads(blocks)
        if not isinstance(blocks, list):
            raise UhcRetainedDatasetError("SEALED UHC fact blocks are invalid")
        for block in blocks:
            if not isinstance(block, Mapping):
                raise UhcRetainedDatasetError("SEALED UHC fact block is invalid")
            lineage_descriptors.append(
                {
                    "source_file_id": sealed_file.admitted.source_file_id,
                    "range_ordinal": _positive_int(
                        block.get("range_ordinal"),
                        "range_ordinal",
                        allow_zero=True,
                    ),
                    "input_sha256": _require_sha256(
                        block.get("semantic_block_sha256"),
                        "semantic block hash",
                    ),
                    "artifact_sha256": sealed_file.admitted.artifact_sha256,
                }
            )
    return tuple(lineage_descriptors)


async def _land_plan_resources(
    connection: asyncpg.Connection,
    names: tuple[str, str, str, str, str],
    sealed_files: tuple[UhcSealedSemanticFile, ...],
    proof_builder: UhcCanonicalProofBuilder,
) -> None:
    """Materialize canonical InsurancePlan resources from staged keys."""

    resource_relation, plan_relation, key_relation, _evidence, _sealed = names
    input_lineage = _semantic_input_lineage(sealed_files)
    after_key: str | None = None
    while True:
        plan_page_rows = await _plan_resource_page(
            connection,
            plan_relation,
            key_relation,
            after_key,
        )
        if not plan_page_rows:
            break
        canonical_rows = [
            _canonical_plan_row(plan_key_row)
            for plan_key_row in plan_page_rows
        ]
        proof_builder.observe_rows(
            canonical_rows,
            input_lineage=input_lineage,
        )
        await _copy_batches(
            connection,
            resource_relation,
            (
                "resource_type",
                "resource_id",
                "payload_hash",
                "payload_json",
                "source_rank",
            ),
            canonical_rows,
        )
        after_key = str(plan_page_rows[-1]["plan_key"])
        if len(plan_page_rows) < _COPY_BATCH_ROWS:
            break


async def _seal_canonical_resource_stage(
    connection: asyncpg.Connection,
    names: tuple[str, str, str, str, str],
    content: UhcCanonicalContentDigest,
) -> dict[str, int]:
    """Index the direct landing after its mergeable identity proof sealed."""

    resource_relation, _plan, _key, _evidence, _sealed = names
    await connection.execute(
        f"""
        CREATE UNIQUE INDEX {_quoted(resource_relation + '_pkey')}
            ON {_qualified(_schema_name(), resource_relation)}
               (resource_type, resource_id);
        ANALYZE {_qualified(_schema_name(), resource_relation)};
        """
    )
    if (
        tuple(sorted(content.resource_counts))
        != tuple(sorted(SOURCE_SUMMARY_UHC_SELECTED_RESOURCES))
        or any(count <= 0 for count in content.resource_counts.values())
    ):
        raise UhcRetainedDatasetError(
            "retained UHC canonical six-resource profile is incomplete"
        )
    return content.resource_counts


async def _plan_key_counts(
    connection: asyncpg.Connection,
    key_relation: str,
) -> dict[str, int]:
    plan_count_row = await connection.fetchrow(
        f"""
        WITH membership AS (
            SELECT DISTINCT plan_key
              FROM {_qualified(_schema_name(), key_relation)}
             WHERE key_kind='membership'
        ), detail AS (
            SELECT DISTINCT plan_key
              FROM {_qualified(_schema_name(), key_relation)}
             WHERE key_kind='detail'
        )
        SELECT (SELECT count(*) FROM membership)::bigint AS membership,
               (SELECT count(*) FROM detail)::bigint AS detail,
               (SELECT count(*) FROM membership JOIN detail USING (plan_key))::bigint AS matched
        """
    )
    if plan_count_row is None:
        raise UhcRetainedDatasetError("retained UHC plan-key proof is missing")
    membership = int(plan_count_row["membership"])
    detail = int(plan_count_row["detail"])
    matched = int(plan_count_row["matched"])
    return {
        "membership_plan_key_count": membership,
        "detail_plan_key_count": detail,
        "matched_plan_key_count": matched,
        "missing_plan_detail_count": membership - matched,
        "orphan_plan_detail_count": detail - matched,
    }


def _json_digest(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode()).hexdigest()


def _semantic_set_identity(
    admitted_set: UhcAdmittedCatalogSet,
    sealed_files: tuple[UhcSealedSemanticFile, ...],
) -> tuple[str, str, str, tuple[str, ...]]:
    ordered = sorted(
        sealed_files,
        key=lambda semantic_file: semantic_file.admitted.source_file_id,
    )
    encoder_digests = {
        semantic_file.identity.encoder_sha256
        for semantic_file in ordered
    }
    if len(encoder_digests) != 1:
        raise UhcRetainedDatasetError("retained UHC semantic encoder set is mixed")
    input_set = [
        [
            semantic_file.admitted.source_file_id,
            semantic_file.admitted.artifact_sha256,
            semantic_file.admitted.collection_kind,
            semantic_file.build_row["fact_set_sha256"],
            semantic_file.build_row["record_identity_set_sha256"],
        ]
        for semantic_file in ordered
    ]
    layout_set = [
        [
            semantic_file.admitted.source_file_id,
            semantic_file.admitted.range_set_sha256,
            semantic_file.build_row["evidence_identity_set_sha256"],
            semantic_file.build_row["evidence_layout_set_sha256"],
            semantic_file.build_row["verifier_sha256"],
        ]
        for semantic_file in ordered
    ]
    build_ids = tuple(
        sorted(
            semantic_file.identity.semantic_build_id
            for semantic_file in ordered
        )
    )
    semantic_set_sha256 = _json_digest(
        [
            UHC_SEMANTIC_CONTRACT_ID,
            UHC_SEMANTIC_CONTRACT_VERSION,
            admitted_set.catalog_set_sha256,
            list(build_ids),
        ]
    )
    return (
        _json_digest(input_set),
        _json_digest(layout_set),
        semantic_set_sha256,
        build_ids,
    )


def _assert_provider_quarantine_file_ceiling(
    counters: Mapping[str, Any],
) -> None:
    provider_count = _positive_int(
        counters.get("raw_provider_records"),
        "raw_provider_records",
    )
    invalid_npi_count = _positive_int(
        counters.get("invalid_npi_count"),
        "invalid_npi_count",
        allow_zero=True,
    )
    if invalid_npi_count > provider_quarantine_limit(provider_count):
        raise UhcRetainedDatasetError(
            "retained UHC provider quarantine exceeds its file ceiling"
        )


def _add_provider_rejected_counts(
    counters: Mapping[str, Any],
    rejected_count_by_field: dict[str, int],
) -> None:
    try:
        file_rejected_count_by_field = provider_quarantine_rejected_counts(
            counters
        )
    except UhcProviderQuarantineError as error:
        raise UhcRetainedDatasetError(str(error)) from error
    for field_name in rejected_count_by_field:
        rejected_count_by_field[field_name] += (
            file_rejected_count_by_field.get(field_name, 0)
        )


def _add_retained_only_drop_counts(
    sealed_file: UhcSealedSemanticFile,
    counters: Mapping[str, Any],
    retained_only_drop_count_by_field: dict[str, int],
) -> None:
    if (
        sealed_file.admitted.logical_scope.pairing_status
        != PAIRING_UNPAIRED_RETAINED_ONLY
    ):
        return
    for drop_key, counter_field in (
        SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS.items()
    ):
        quarantine_counter_field = UHC_PROVIDER_QUARANTINE_COUNTER_BY_RAW_FIELD[
            counter_field
        ]
        retained_only_drop_count_by_field[drop_key] += _positive_int(
            counters.get(counter_field),
            counter_field,
            allow_zero=True,
        ) - _positive_int(
            counters.get(quarantine_counter_field),
            quarantine_counter_field,
            allow_zero=True,
        )


def _provider_quarantine_proof_shard(
    sealed_file: UhcSealedSemanticFile,
    counters: Mapping[str, Any],
) -> list[Any]:
    return [
        sealed_file.admitted.source_file_id,
        _positive_int(
            counters.get("invalid_npi_count"),
            "invalid_npi_count",
            allow_zero=True,
        ),
        _require_sha256(
            counters.get("quarantine_identity_set_sha256"),
            "quarantine identity set hash",
        ),
    ]


def _accumulate_sealed_summary_counts(
    sealed_files: tuple[UhcSealedSemanticFile, ...],
    count_by_field: dict[str, int],
    retained_only_drop_count_by_field: dict[str, int],
    rejected_count_by_field: dict[str, int],
) -> tuple[int, list[str], str]:
    """Accumulate sealed counters and return evidence census and stages."""
    expected_evidence_count = 0
    evidence_stage_refs = []
    quarantine_proof_shards = []
    for sealed_file in sealed_files:
        counters = _mapping(sealed_file.build_row["counters_json"], "counters")
        for field in _SUMMARY_ADDITIVE_COUNTERS:
            count_by_field[field] += _positive_int(
                counters.get(field), field, allow_zero=True
            )
        if sealed_file.admitted.collection_kind != "provider_membership":
            continue
        _assert_provider_quarantine_file_ceiling(counters)
        expected_evidence_count += int(sealed_file.build_row["evidence_count"])
        evidence_stage_refs.append(sealed_file.stage_ref)
        _add_provider_rejected_counts(counters, rejected_count_by_field)
        quarantine_proof_shards.append(
            _provider_quarantine_proof_shard(sealed_file, counters)
        )
        _add_retained_only_drop_counts(
            sealed_file,
            counters,
            retained_only_drop_count_by_field,
        )
    return (
        expected_evidence_count,
        evidence_stage_refs,
        _json_digest(
            [
                UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
                sorted(quarantine_proof_shards),
            ]
        ),
    )


def _empty_summary_count_maps() -> tuple[
    dict[str, int],
    dict[str, int],
    dict[str, int],
]:
    return (
        dict.fromkeys(SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS, 0),
        dict.fromkeys(SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS, 0),
        dict.fromkeys(UHC_PROVIDER_QUARANTINE_REJECTED_COUNT_FIELDS, 0),
    )


def _summary_count_categories(
    evidence: UhcNpiEvidenceSummary,
    rejected_count_by_field: Mapping[str, int],
    retained_only_drop_count_by_field: Mapping[str, int],
) -> dict[str, dict[str, int]]:
    rejected_counts = (
        dict(rejected_count_by_field)
        if any(
            rejected_count_by_field[reason]
            for reason in UHC_PROVIDER_QUARANTINE_REASONS
        )
        else {}
    )
    intentional_drop_counts = (
        dict(retained_only_drop_count_by_field)
        if retained_only_drop_count_by_field[
            SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_KEY
        ]
        else {}
    )
    return {
        "conflict_counts": evidence.conflict_counts,
        "rejected_counts": rejected_counts,
        "intentional_drop_counts": intentional_drop_counts,
        "unknown_field_counts": {},
    }


def _assert_summary_provider_balance(
    count_by_field: Mapping[str, int],
    expected_evidence_count: int,
    provider_file_count: int,
) -> None:
    if count_by_field["raw_provider_records"] != (
        expected_evidence_count + count_by_field["invalid_npi_count"]
    ):
        raise UhcRetainedDatasetError(
            "retained UHC provider counters disagree with set evidence"
        )
    if count_by_field["invalid_npi_count"] > provider_quarantine_catalog_limit(
        provider_file_count
    ):
        raise UhcRetainedDatasetError(
            "retained UHC provider quarantine exceeds its publication ceiling"
        )


async def _combined_summary_counts(
    connection: asyncpg.Connection,
    admitted_set: UhcAdmittedCatalogSet,
    sealed_files: tuple[UhcSealedSemanticFile, ...],
    names: tuple[str, str, str, str, str],
) -> tuple[
    dict[str, int],
    dict[str, dict[str, int]],
    UhcNpiEvidenceSummary,
    str,
]:
    """Combine sealed per-file counters for the complete catalog set."""
    _resource, _plan, key_relation, _evidence, _sealed = names
    (
        count_by_field,
        retained_only_drop_count_by_field,
        rejected_count_by_field,
    ) = _empty_summary_count_maps()
    expected_evidence_count, evidence_stage_refs, quarantine_proof_sha256 = (
        _accumulate_sealed_summary_counts(
            sealed_files,
            count_by_field,
            retained_only_drop_count_by_field,
            rejected_count_by_field,
        )
    )
    evidence = await summarize_uhc_npi_evidence_stages(
        connection,
        evidence_stage_refs,
        expected_evidence_count=expected_evidence_count,
    )
    count_by_field.update(
        distinct_npis=evidence.distinct_npis,
        duplicate_npi_groups=evidence.duplicate_npi_groups,
        conflicting_npi_groups=evidence.conflicting_npi_groups,
        provider_file_count=admitted_set.provider_file_count,
        plan_file_count=admitted_set.plan_file_count,
        **await _plan_key_counts(connection, key_relation),
    )
    _assert_summary_provider_balance(
        count_by_field,
        expected_evidence_count,
        admitted_set.provider_file_count
    )
    return (
        count_by_field,
        _summary_count_categories(
            evidence,
            rejected_count_by_field,
            retained_only_drop_count_by_field,
        ),
        evidence,
        quarantine_proof_sha256,
    )


def _npi_evidence_proof_shards(
    sealed_files: tuple[UhcSealedSemanticFile, ...],
) -> tuple[dict[str, Any], ...]:
    shards = []
    for sealed_file in sorted(
        sealed_files,
        key=lambda item: item.admitted.source_file_id,
    ):
        if sealed_file.admitted.collection_kind != "provider_membership":
            continue
        ranges = sealed_file.build_row["evidence_ranges_json"]
        if isinstance(ranges, str):
            ranges = json.loads(ranges)
        if not isinstance(ranges, list):
            raise UhcRetainedDatasetError(
                "SEALED UHC evidence ranges are invalid"
            )
        input_sha256 = _require_sha256(
            sealed_file.build_row["evidence_identity_set_sha256"],
            "evidence identity set hash",
        )
        for evidence_range in ranges:
            if not isinstance(evidence_range, Mapping):
                raise UhcRetainedDatasetError(
                    "SEALED UHC evidence range is invalid"
                )
            shards.append(
                {
                    "source_id": UHC_RETAINED_SOURCE_ID,
                    "source_file_id": sealed_file.admitted.source_file_id,
                    "range_ordinal": _positive_int(
                        evidence_range.get("range_ordinal"),
                        "range_ordinal",
                        allow_zero=True,
                    ),
                    "row_count": _positive_int(
                        evidence_range.get("evidence_count"),
                        "evidence_count",
                        allow_zero=True,
                    ),
                    "input_sha256": input_sha256,
                    "artifact_sha256": sealed_file.admitted.artifact_sha256,
                    "layout_sha256": _require_sha256(
                        evidence_range.get("layout_sha256"),
                        "evidence range layout hash",
                    ),
                }
            )
    return tuple(shards)


def _summary_input_hash(summary_input: Mapping[str, Any]) -> str:
    return _json_digest(
        {key: value for key, value in summary_input.items() if key != "input_sha256"}
    )


def _validate_summary_contract(summary_input_by_field: dict[str, Any]) -> None:
    expected_fields = {
        "contract_id", "complete", "source_id", "catalog_set_sha256",
        "semantic_contract_id", "semantic_contract_version",
        "canonical_contract_id", "semantic_build_ids", "semantic_set_sha256",
        "input_set_sha256", "layout_set_sha256", "encoder_digest",
        "quarantine_proof_sha256", "count_by_field", "count_by_category",
        "input_sha256",
    }
    if set(summary_input_by_field) != expected_fields:
        raise UhcRetainedDatasetError("retained UHC summary input shape is invalid")
    expected_identity_by_field = {
        "contract_id": UHC_RETAINED_SUMMARY_INPUT_CONTRACT_ID,
        "complete": True,
        "source_id": UHC_RETAINED_SOURCE_ID,
        "semantic_contract_id": UHC_SEMANTIC_CONTRACT_ID,
        "semantic_contract_version": UHC_SEMANTIC_CONTRACT_VERSION,
        "canonical_contract_id": UHC_RETAINED_CANONICAL_CONTRACT_ID,
    }
    if any(
        summary_input_by_field[field_name] != expected_value
        for field_name, expected_value in expected_identity_by_field.items()
    ):
        raise UhcRetainedDatasetError("retained UHC summary input contract is invalid")
    for field_name in (
        "catalog_set_sha256", "semantic_set_sha256", "input_set_sha256",
        "layout_set_sha256", "encoder_digest", "input_sha256",
        "quarantine_proof_sha256",
    ):
        _require_sha256(summary_input_by_field[field_name], field_name)


def _validate_summary_build_ids(summary_input_by_field: dict[str, Any]) -> None:
    build_ids = summary_input_by_field["semantic_build_ids"]
    if (
        not isinstance(build_ids, list)
        or not build_ids
        or build_ids != sorted(set(build_ids))
        or any(_SHA256_RE.fullmatch(build_id) is None for build_id in build_ids)
    ):
        raise UhcRetainedDatasetError(
            "retained UHC summary semantic build set is invalid"
        )


def _validated_summary_count_maps(
    summary_input_by_field: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Mapping[str, Any]]]:
    count_by_field = _mapping(
        summary_input_by_field["count_by_field"], "count_by_field"
    )
    if set(count_by_field) != set(SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS):
        raise UhcRetainedDatasetError(
            "retained UHC summary count fields are incomplete"
        )
    for field_name, count in count_by_field.items():
        _positive_int(count, field_name, allow_zero=True)
    count_by_category = _mapping(
        summary_input_by_field["count_by_category"],
        "count_by_category",
    )
    expected_categories = {
        "conflict_counts",
        "rejected_counts",
        "intentional_drop_counts",
        "unknown_field_counts",
    }
    if set(count_by_category) != expected_categories or any(
        not isinstance(category_map, Mapping)
        for category_map in count_by_category.values()
    ):
        raise UhcRetainedDatasetError(
            "retained UHC summary count categories are invalid"
        )
    return count_by_field, count_by_category


def _is_summary_rejected_count_map_valid(
    count_by_field: Mapping[str, Any],
    rejected_count_by_field: Mapping[str, Any],
) -> bool:
    invalid_npi_count = count_by_field["invalid_npi_count"]
    try:
        rejected_totals = provider_quarantine_rejected_totals(
            rejected_count_by_field,
            invalid_npi_count,
        )
    except UhcProviderQuarantineError:
        return False
    return (
        rejected_totals["individual_records"]
        <= count_by_field["raw_individual_records"]
        and rejected_totals["facility_records"]
        <= count_by_field["raw_facility_records"]
        and rejected_totals["address_rows"]
        <= count_by_field["raw_address_rows"]
        and rejected_totals["provider_plan_rows"]
        <= count_by_field["raw_provider_plan_rows"]
        and invalid_npi_count
        <= provider_quarantine_catalog_limit(
            count_by_field["provider_file_count"]
        )
    )


def _is_summary_drop_count_map_valid(
    intentional_drop_count_by_field: Mapping[str, Any],
) -> bool:
    return (
        not intentional_drop_count_by_field
        or (
            set(intentional_drop_count_by_field)
            == set(SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS)
            and _positive_int(
                intentional_drop_count_by_field.get(
                    SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_KEY
                ),
                SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_KEY,
            )
            > 0
            and all(
                _positive_int(
                    intentional_drop_count_by_field.get(drop_key),
                    drop_key,
                    allow_zero=True,
                )
                >= 0
                for drop_key in SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS
            )
        )
    )


def _validate_summary_counts(summary_input_by_field: dict[str, Any]) -> None:
    """Reject unbalanced or unaccounted UHC summary counters."""
    count_by_field, count_by_category = _validated_summary_count_maps(
        summary_input_by_field
    )
    rejected_count_by_field = count_by_category["rejected_counts"]
    intentional_drop_count_by_field = count_by_category[
        "intentional_drop_counts"
    ]
    if (
        not _is_summary_rejected_count_map_valid(
            count_by_field, rejected_count_by_field
        )
        or not _is_summary_drop_count_map_valid(
            intentional_drop_count_by_field
        )
        or count_by_category["unknown_field_counts"]
    ):
        raise UhcRetainedDatasetError(
            "retained UHC summary contains unaccounted semantics"
        )


def validate_uhc_summary_input(raw_value: Any) -> dict[str, Any]:
    """Validate the durable pre-validation input used to build source_summary_v1."""

    summary_input_by_field = _mapping(raw_value, "summary input")
    _validate_summary_contract(summary_input_by_field)
    _validate_summary_build_ids(summary_input_by_field)
    _validate_summary_counts(summary_input_by_field)
    if summary_input_by_field["input_sha256"] != _summary_input_hash(
        summary_input_by_field
    ):
        raise UhcRetainedDatasetError(
            "retained UHC summary input hash is invalid"
        )
    return dict(summary_input_by_field)


def _assert_complete_semantic_set(
    admitted_set: UhcAdmittedCatalogSet,
    sealed_files: tuple[UhcSealedSemanticFile, ...],
) -> None:
    admitted_ids = {item.source_file_id for item in admitted_set.files}
    sealed_ids = {item.admitted.source_file_id for item in sealed_files}
    if len(sealed_files) != len(admitted_set.files) or sealed_ids != admitted_ids:
        raise UhcRetainedDatasetError(
            "retained UHC canonical build received a partial semantic set"
        )


def _canonical_summary_input(
    admitted_set: UhcAdmittedCatalogSet,
    sealed_files: tuple[UhcSealedSemanticFile, ...],
    count_by_field: dict[str, int],
    count_by_category: dict[str, dict[str, int]],
    quarantine_proof_sha256: str,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    input_digest, layout_digest, semantic_set_digest, build_ids = (
        _semantic_set_identity(admitted_set, sealed_files)
    )
    summary_input_by_field = {
        "contract_id": UHC_RETAINED_SUMMARY_INPUT_CONTRACT_ID,
        "complete": True,
        "source_id": UHC_RETAINED_SOURCE_ID,
        "catalog_set_sha256": admitted_set.catalog_set_sha256,
        "semantic_contract_id": UHC_SEMANTIC_CONTRACT_ID,
        "semantic_contract_version": UHC_SEMANTIC_CONTRACT_VERSION,
        "canonical_contract_id": UHC_RETAINED_CANONICAL_CONTRACT_ID,
        "semantic_build_ids": list(build_ids),
        "semantic_set_sha256": semantic_set_digest,
        "input_set_sha256": input_digest,
        "layout_set_sha256": layout_digest,
        "encoder_digest": sealed_files[0].identity.encoder_sha256,
        "quarantine_proof_sha256": quarantine_proof_sha256,
        "count_by_field": count_by_field,
        "count_by_category": count_by_category,
    }
    summary_input_by_field["input_sha256"] = _summary_input_hash(
        summary_input_by_field
    )
    validate_uhc_summary_input(summary_input_by_field)
    return summary_input_by_field, build_ids


@dataclass(frozen=True)
class _CanonicalLanding:
    content: UhcCanonicalContentDigest
    resource_counts: dict[str, int]
    phase_seconds_by_name: dict[str, float]


async def _land_uhc_canonical_content(
    connection: asyncpg.Connection,
    sealed_files: tuple[UhcSealedSemanticFile, ...],
    names: tuple[str, str, str, str, str],
    proof_builder: UhcCanonicalProofBuilder,
) -> _CanonicalLanding:
    """Land resources, merge exact proof, and build deferred indexes."""

    phase_seconds_by_name: dict[str, float] = {}
    await _create_canonical_stages(connection, _schema_name(), names)
    fact_started = time.perf_counter()
    await _land_canonical_facts(
        connection,
        sealed_files,
        names,
        proof_builder,
    )
    phase_seconds_by_name["fact_decode_copy_seconds"] = (
        time.perf_counter() - fact_started
    )
    plan_started = time.perf_counter()
    await _land_plan_resources(connection, names, sealed_files, proof_builder)
    phase_seconds_by_name["plan_materialize_copy_seconds"] = (
        time.perf_counter() - plan_started
    )
    proof_started = time.perf_counter()
    content = proof_builder.complete()
    phase_seconds_by_name["identity_proof_merge_seconds"] = (
        time.perf_counter() - proof_started
    )
    index_started = time.perf_counter()
    resource_counts = await _seal_canonical_resource_stage(
        connection,
        names,
        content,
    )
    phase_seconds_by_name["deferred_index_seconds"] = (
        time.perf_counter() - index_started
    )
    return _CanonicalLanding(content, resource_counts, phase_seconds_by_name)


def _canonical_phase_metrics(
    landing: _CanonicalLanding,
    evidence: UhcNpiEvidenceSummary,
    summary_seconds: float,
    total_seconds: float,
) -> dict[str, float | int]:
    """Report each independent landing, proof, index, and summary phase."""

    phase_seconds_by_name = dict(landing.phase_seconds_by_name)
    phase_seconds_by_name["npi_merge_summary_seconds"] = summary_seconds
    fact_seconds = phase_seconds_by_name["fact_decode_copy_seconds"]
    plan_seconds = phase_seconds_by_name["plan_materialize_copy_seconds"]
    proof_seconds = phase_seconds_by_name["identity_proof_merge_seconds"]
    return {
        **phase_seconds_by_name,
        "canonical_materialization_seconds": total_seconds,
        "canonical_resource_rows": landing.content.resource_count,
        "canonical_rows_per_second": landing.content.resource_count
        / max(fact_seconds + plan_seconds + proof_seconds, 1e-9),
        "npi_evidence_rows": evidence.evidence_count,
        "npi_evidence_rows_per_second": evidence.evidence_count
        / max(summary_seconds, 1e-9),
    }


def _canonical_content_proof(
    admitted_set: UhcAdmittedCatalogSet,
    sealed_files: tuple[UhcSealedSemanticFile, ...],
    landing: _CanonicalLanding,
    summary_input_by_field: Mapping[str, Any],
    build_ids: tuple[str, ...],
    evidence: UhcNpiEvidenceSummary,
) -> dict[str, Any]:
    identity = UhcCanonicalMaterializationIdentity(
        catalog_set_sha256=admitted_set.catalog_set_sha256,
        semantic_set_sha256=summary_input_by_field["semantic_set_sha256"],
        semantic_build_ids=build_ids,
        source_id=UHC_RETAINED_SOURCE_ID,
        semantic_contract_id=UHC_SEMANTIC_CONTRACT_ID,
        semantic_contract_version=UHC_SEMANTIC_CONTRACT_VERSION,
        canonical_contract_id=UHC_RETAINED_CANONICAL_CONTRACT_ID,
    )
    npi_proof = UhcCanonicalNpiProof(
        evidence_count=evidence.evidence_count,
        distinct_npis=evidence.distinct_npis,
        proof_sha256=evidence.proof_sha256,
        shards=_npi_evidence_proof_shards(sealed_files),
    )
    return canonical_materialization_proof(landing.content, identity, npi_proof)


def _canonical_stage_result(
    names: tuple[str, str, str, str, str],
    landing: _CanonicalLanding,
    content_proof: Mapping[str, Any],
    summary_input_by_field: Mapping[str, Any],
    build_ids: tuple[str, ...],
    evidence: UhcNpiEvidenceSummary,
    summary_seconds: float,
    total_seconds: float,
) -> UhcCanonicalStage:
    return UhcCanonicalStage(
        schema=_schema_name(),
        resource_relation=names[0],
        auxiliary_relations=names[1:],
        resource_counts=landing.resource_counts,
        content_proof=dict(content_proof),
        summary_input=dict(summary_input_by_field),
        semantic_build_ids=build_ids,
        phase_metrics=_canonical_phase_metrics(
            landing,
            evidence,
            summary_seconds,
            total_seconds,
        ),
    )


async def _finalize_uhc_canonical_stage(
    connection: asyncpg.Connection,
    admitted_set: UhcAdmittedCatalogSet,
    sealed_files: tuple[UhcSealedSemanticFile, ...],
    names: tuple[str, str, str, str, str],
    landing: _CanonicalLanding,
    started: float,
) -> UhcCanonicalStage:
    """Build source summary and bind the content proof to semantic lineage."""

    summary_started = time.perf_counter()
    (
        count_by_field,
        count_by_category,
        evidence,
        quarantine_proof_sha256,
    ) = await _combined_summary_counts(
        connection,
        admitted_set,
        sealed_files,
        names,
    )
    summary_seconds = time.perf_counter() - summary_started
    summary_input_by_field, build_ids = _canonical_summary_input(
        admitted_set,
        sealed_files,
        count_by_field,
        count_by_category,
        quarantine_proof_sha256,
    )
    content_proof = _canonical_content_proof(
        admitted_set,
        sealed_files,
        landing,
        summary_input_by_field,
        build_ids,
        evidence,
    )
    return _canonical_stage_result(
        names,
        landing,
        content_proof,
        summary_input_by_field,
        build_ids,
        evidence,
        summary_seconds,
        time.perf_counter() - started,
    )


async def build_uhc_canonical_stage(
    connection: asyncpg.Connection,
    admitted_set: UhcAdmittedCatalogSet,
    sealed_files: tuple[UhcSealedSemanticFile, ...],
) -> UhcCanonicalStage:
    """Build and independently seal the six-family canonical resource stage."""

    _assert_complete_semantic_set(admitted_set, sealed_files)
    names = _stage_names()
    schema = _schema_name()
    proof_builder = UhcCanonicalProofBuilder(source_id=UHC_RETAINED_SOURCE_ID)
    started = time.perf_counter()
    try:
        landing = await _land_uhc_canonical_content(
            connection,
            sealed_files,
            names,
            proof_builder,
        )
        return await _finalize_uhc_canonical_stage(
            connection,
            admitted_set,
            sealed_files,
            names,
            landing,
            started,
        )
    except BaseException:
        proof_builder.close()
        await cleanup_uhc_canonical_stage(
            connection,
            UhcCanonicalStage(
                schema=schema,
                resource_relation=names[-1],
                auxiliary_relations=names[:-1],
                resource_counts={},
                content_proof={},
                summary_input={},
                semantic_build_ids=(),
                phase_metrics={},
            ),
        )
        raise


async def cleanup_uhc_canonical_stage(
    connection: asyncpg.Connection,
    stage: UhcCanonicalStage,
) -> None:
    """Drop only the private relations owned by one canonical build."""

    for relation in (*stage.auxiliary_relations, stage.resource_relation):
        if _IDENTIFIER_RE.fullmatch(relation) is None or not relation.startswith(
            "provider_directory_uhc_"
        ):
            raise UhcRetainedDatasetError(
                "refusing to clean an unowned retained UHC relation"
            )
    await connection.execute(
        ";\n".join(
            f"DROP TABLE IF EXISTS {_qualified(stage.schema, relation)}"
            for relation in (*stage.auxiliary_relations, stage.resource_relation)
        )
    )


def publication_identity(
    summary_input: Mapping[str, Any],
    *,
    dataset_id: str,
    acquisition_root_run_id: str,
) -> dict[str, Any]:
    """Return the exact retained lineage marker persisted on the candidate."""

    validated = validate_uhc_summary_input(summary_input)
    return {
        "contract_id": UHC_RETAINED_PUBLICATION_CONTRACT_ID,
        "complete": True,
        "source_id": UHC_RETAINED_SOURCE_ID,
        "dataset_id": dataset_id,
        "acquisition_root_run_id": acquisition_root_run_id,
        "catalog_set_sha256": validated["catalog_set_sha256"],
        "semantic_contract_id": validated["semantic_contract_id"],
        "semantic_contract_version": validated["semantic_contract_version"],
        "semantic_set_sha256": validated["semantic_set_sha256"],
        "canonical_contract_id": validated["canonical_contract_id"],
        "summary_input_sha256": validated["input_sha256"],
    }
