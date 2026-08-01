# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable BUILDING/SEALED state for bounded UHC semantic COPY stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import os
import re
import secrets
from typing import Any, AsyncIterator, Mapping

import asyncpg

from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from process.uhc_provider_quarantine_contract import (
    UHC_PROVIDER_QUARANTINE_MAX_COUNT,
    UhcProviderQuarantineError,
    provider_quarantine_limit,
    provider_quarantine_rejected_counts,
)
from process.uhc_semantic_evidence import (
    UhcNpiEvidenceSummary,
    summarize_uhc_npi_evidence,
)
from process.uhc_semantic_verifier_identity import (
    semantic_verifier_identity_sha256,
)


UHC_SEMANTIC_CONTRACT_ID = "healthporta.uhc.semantic-facts.v3"
UHC_SEMANTIC_CONTRACT_VERSION = 3
UHC_SEMANTIC_COPY_FORMAT_ID = "postgres-copy-binary-uhc-fact-evidence-v2"
UHC_SEMANTIC_SOURCE_ID = UHC_PROVIDER_FILE_SOURCE_ID
UHC_SEMANTIC_COPY_COLUMNS = (
    "row_kind",
    "range_ordinal",
    "run_ordinal",
    "occurrence_ordinal",
    "record_start",
    "record_count",
    "npi",
    "conflict_signature_pack",
    "payload_hash",
    "semantic_hash",
    "payload_bytes",
)

_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COLLECTION_KINDS = {"provider_membership", "plan_reference"}


class UhcSemanticBuildError(RuntimeError):
    """Base error for a fail-closed semantic build."""


class UhcSemanticBuildBusy(UhcSemanticBuildError):
    """Another live lease owns the exact semantic build."""


class UhcSemanticBuildStale(UhcSemanticBuildError):
    """The caller no longer owns the semantic build lease."""


def _schema_name() -> str:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if _IDENTIFIER_RE.fullmatch(schema) is None:
        raise UhcSemanticBuildError("invalid UHC semantic registry schema")
    return schema


def _quoted_identifier(identifier: str) -> str:
    if _IDENTIFIER_RE.fullmatch(identifier) is None:
        raise UhcSemanticBuildError("invalid UHC semantic identifier")
    return f'"{identifier}"'


def _table_ref(table: str) -> str:
    return f'{_quoted_identifier(_schema_name())}.{_quoted_identifier(table)}'


def _stage_ref(schema: str, relation: str) -> str:
    return f"{_quoted_identifier(schema)}.{_quoted_identifier(relation)}"


def _require_sha256(value: str, field: str) -> str:
    if _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return value


@dataclass(frozen=True)
class UhcSemanticBuildIdentity:
    catalog_set_sha256: str
    source_file_id: str
    artifact_sha256: str
    raw_contract_version: int
    raw_range_count: int
    manifest_sha256: str
    range_set_sha256: str
    raw_record_count: int
    raw_producer_build_id: str
    collection_kind: str
    encoder_sha256: str
    semantic_verifier_sha256: str = field(
        default_factory=semantic_verifier_identity_sha256
    )

    def validate(self) -> None:
        """Reject any semantic identity outside the immutable contract."""

        _require_sha256(self.catalog_set_sha256, "catalog_set_sha256")
        _require_sha256(self.source_file_id, "source_file_id")
        _require_sha256(self.artifact_sha256, "artifact_sha256")
        _require_sha256(self.manifest_sha256, "manifest_sha256")
        _require_sha256(self.range_set_sha256, "range_set_sha256")
        _require_sha256(self.encoder_sha256, "encoder_sha256")
        _require_sha256(
            self.semantic_verifier_sha256,
            "semantic_verifier_sha256",
        )
        if self.raw_contract_version <= 0:
            raise ValueError("raw_contract_version must be positive")
        if not 4 <= self.raw_range_count <= 256:
            raise ValueError("raw_range_count must be in 4..=256")
        if self.raw_record_count <= 0:
            raise ValueError("raw_record_count must be positive")
        if (
            not self.raw_producer_build_id
            or len(self.raw_producer_build_id) > 256
            or not self.raw_producer_build_id.isascii()
            or not self.raw_producer_build_id.isprintable()
        ):
            raise ValueError("raw_producer_build_id must be printable ASCII")
        if self.collection_kind not in _COLLECTION_KINDS:
            raise ValueError("collection_kind is unsupported")

    @property
    def semantic_build_id(self) -> str:
        """Return the deterministic identity for this semantic build."""

        self.validate()
        identity = json.dumps(
            [
                UHC_SEMANTIC_SOURCE_ID,
                self.catalog_set_sha256,
                self.source_file_id,
                self.artifact_sha256,
                self.raw_contract_version,
                self.raw_range_count,
                self.manifest_sha256,
                self.range_set_sha256,
                self.raw_record_count,
                self.raw_producer_build_id,
                self.collection_kind,
                UHC_SEMANTIC_CONTRACT_ID,
                UHC_SEMANTIC_CONTRACT_VERSION,
                UHC_SEMANTIC_COPY_FORMAT_ID,
                self.encoder_sha256,
                self.semantic_verifier_sha256,
            ],
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(identity).hexdigest()

    @property
    def stage_relation(self) -> str:
        """Return the bounded physical stage relation name."""

        return f"provider_directory_uhc_sem_{self.semantic_build_id[:24]}"


@dataclass(frozen=True)
class UhcSemanticBuildClaim:
    semantic_build_id: str
    lease_token: str | None
    attempt_count: int
    stage_schema: str
    stage_relation: str
    sealed_reuse: bool

    @property
    def stage_ref(self) -> str:
        """Return the quoted physical stage relation identity."""

        return _stage_ref(self.stage_schema, self.stage_relation)


def _advisory_lock_key(semantic_build_id: str) -> int:
    unsigned = int(semantic_build_id[:16], 16)
    return unsigned if unsigned < 2**63 else unsigned - 2**64


def _stage_create_sql(stage_ref: str) -> str:
    return f"""
        CREATE TABLE {stage_ref} (
            row_kind smallint NOT NULL,
            range_ordinal bigint NOT NULL,
            run_ordinal bigint,
            occurrence_ordinal bigint,
            record_start bigint,
            record_count bigint,
            npi text,
            conflict_signature_pack bytea,
            payload_hash text,
            semantic_hash text,
            payload_bytes bytea
        )
    """


def _identity_fields(identity: UhcSemanticBuildIdentity) -> dict[str, Any]:
    return {
        "catalog_set_sha256": identity.catalog_set_sha256,
        "source_file_id": identity.source_file_id,
        "artifact_sha256": identity.artifact_sha256,
        "raw_contract_version": identity.raw_contract_version,
        "raw_range_count": identity.raw_range_count,
        "manifest_sha256": identity.manifest_sha256,
        "range_set_sha256": identity.range_set_sha256,
        "raw_record_count": identity.raw_record_count,
        "raw_producer_build_id": identity.raw_producer_build_id,
        "collection_kind": identity.collection_kind,
        "semantic_contract_id": UHC_SEMANTIC_CONTRACT_ID,
        "semantic_contract_version": UHC_SEMANTIC_CONTRACT_VERSION,
        "copy_format_id": UHC_SEMANTIC_COPY_FORMAT_ID,
        "encoder_sha256": identity.encoder_sha256,
        "semantic_verifier_sha256": identity.semantic_verifier_sha256,
    }


def _assert_identity_row(
    row: Mapping[str, Any],
    identity: UhcSemanticBuildIdentity,
) -> None:
    mismatch_by_field = {
        field: (row[field], expected)
        for field, expected in _identity_fields(identity).items()
        if row[field] != expected
    }
    if mismatch_by_field:
        raise UhcSemanticBuildError(
            "immutable UHC semantic build identity mismatch: "
            f"{mismatch_by_field}"
        )


async def _assert_active_raw_layout(
    connection: asyncpg.Connection,
    identity: UhcSemanticBuildIdentity,
    binding_table: str,
    layout_table: str,
) -> None:
    gate = await connection.fetchrow(
        f"""
        SELECT binding.catalog_set_sha256, binding.source_file_id,
               binding.artifact_sha256, binding.collection_kind,
               layout.contract_version, layout.range_count,
               layout.manifest_sha256, layout.range_set_sha256,
               layout.record_count, layout.producer_build_id
          FROM {binding_table} AS binding
          JOIN {layout_table} AS layout
            ON layout.artifact_sha256=binding.artifact_sha256
           AND layout.contract_version=$4
           AND layout.range_count=$5
           AND layout.manifest_sha256=$6
           AND layout.range_set_sha256=$7
           AND layout.record_count=$8
           AND layout.producer_build_id=$9
         WHERE binding.catalog_set_sha256=$1
           AND binding.source_file_id=$2
           AND binding.artifact_sha256=$3
           AND binding.collection_kind=$10
           AND binding.released_at IS NULL
           AND layout.status='verified'
         FOR SHARE OF binding, layout
        """,
        identity.catalog_set_sha256,
        identity.source_file_id,
        identity.artifact_sha256,
        identity.raw_contract_version,
        identity.raw_range_count,
        identity.manifest_sha256,
        identity.range_set_sha256,
        identity.raw_record_count,
        identity.raw_producer_build_id,
        identity.collection_kind,
    )
    if gate is None:
        raise UhcSemanticBuildError(
            "UHC semantic build is not bound to one active verified raw layout"
        )


def _existing_build_claim(
    build_record: Mapping[str, Any] | None,
    identity: UhcSemanticBuildIdentity,
    schema: str,
    stage_relation: str,
) -> UhcSemanticBuildClaim | None:
    if build_record is None:
        return None
    _assert_identity_row(build_record, identity)
    if (
        build_record["stage_schema"] != schema
        or build_record["stage_relation"] != stage_relation
    ):
        raise UhcSemanticBuildError("immutable UHC semantic stage identity mismatch")
    if build_record["status"] == "sealed":
        return UhcSemanticBuildClaim(
            semantic_build_id=identity.semantic_build_id,
            lease_token=None,
            attempt_count=int(build_record["attempt_count"]),
            stage_schema=schema,
            stage_relation=stage_relation,
            sealed_reuse=True,
        )
    if bool(build_record["lease_active"]):
        raise UhcSemanticBuildBusy("UHC semantic build has a live lease")
    return None


async def _insert_semantic_build(
    connection: asyncpg.Connection,
    build_table: str,
    identity: UhcSemanticBuildIdentity,
    lease_token: str,
    lease_seconds: int,
    schema: str,
    stage_relation: str,
) -> None:
    await connection.execute(
        f"""
        INSERT INTO {build_table} (
            semantic_build_id, catalog_set_sha256, source_file_id,
            artifact_sha256, raw_contract_version, raw_range_count,
            manifest_sha256, range_set_sha256, raw_record_count,
            raw_producer_build_id,
            collection_kind, semantic_contract_id,
            semantic_contract_version, copy_format_id, encoder_sha256,
            semantic_verifier_sha256,
            status, attempt_count, lease_token, lease_expires_at,
            heartbeat_at, stage_schema, stage_relation, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16,
            'building', 1, $17,
            now() + ($18::double precision * interval '1 second'),
            now(), $19, $20, now(), now()
        )
        """,
        identity.semantic_build_id,
        identity.catalog_set_sha256,
        identity.source_file_id,
        identity.artifact_sha256,
        identity.raw_contract_version,
        identity.raw_range_count,
        identity.manifest_sha256,
        identity.range_set_sha256,
        identity.raw_record_count,
        identity.raw_producer_build_id,
        identity.collection_kind,
        UHC_SEMANTIC_CONTRACT_ID,
        UHC_SEMANTIC_CONTRACT_VERSION,
        UHC_SEMANTIC_COPY_FORMAT_ID,
        identity.encoder_sha256,
        identity.semantic_verifier_sha256,
        lease_token,
        lease_seconds,
        schema,
        stage_relation,
    )


async def _recover_semantic_build(
    connection: asyncpg.Connection,
    build_table: str,
    build_id: str,
    lease_token: str,
    lease_seconds: int,
) -> None:
    updated = await connection.execute(
        f"""
        UPDATE {build_table}
           SET status='building', attempt_count=attempt_count + 1,
               lease_token=$2,
               lease_expires_at=now() + ($3::double precision * interval '1 second'),
               heartbeat_at=now(), fact_count=NULL, evidence_count=NULL,
               fact_set_sha256=NULL, record_identity_set_sha256=NULL,
               evidence_identity_set_sha256=NULL, evidence_layout_set_sha256=NULL,
               verifier_sha256=NULL, counters_json=NULL, fact_blocks_json=NULL,
               evidence_ranges_json=NULL, failure_code=NULL, verified_at=NULL,
               sealed_at=NULL, updated_at=now()
         WHERE semantic_build_id=$1 AND status IN ('building', 'quarantined')
        """,
        build_id,
        lease_token,
        lease_seconds,
    )
    if updated != "UPDATE 1":
        raise UhcSemanticBuildStale("UHC semantic build changed during recovery")


async def _claim_semantic_build_transaction(
    connection: asyncpg.Connection,
    identity: UhcSemanticBuildIdentity,
    lease_token: str,
    lease_seconds: int,
    schema: str,
    stage_relation: str,
) -> tuple[UhcSemanticBuildClaim | None, int]:
    build_id = identity.semantic_build_id
    build_table = _table_ref("provider_directory_uhc_semantic_build")
    async with connection.transaction():
        await connection.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _advisory_lock_key(build_id),
        )
        await _assert_active_raw_layout(
            connection,
            identity,
            _table_ref("provider_directory_uhc_source_binding"),
            _table_ref("provider_directory_uhc_raw_layout"),
        )
        build_record = await connection.fetchrow(
            f"""
            SELECT *, status='building' AND lease_expires_at > now() AS lease_active
              FROM {build_table}
             WHERE semantic_build_id=$1 FOR UPDATE
            """,
            build_id,
        )
        existing_claim = _existing_build_claim(
            build_record,
            identity,
            schema,
            stage_relation,
        )
        if existing_claim is not None:
            return existing_claim, 0
        stage_ref = _stage_ref(schema, stage_relation)
        await connection.execute(f"DROP TABLE IF EXISTS {stage_ref}")
        await connection.execute(_stage_create_sql(stage_ref))
        if build_record is None:
            await _insert_semantic_build(
                connection, build_table, identity, lease_token,
                lease_seconds, schema, stage_relation,
            )
            return None, 1
        attempt_count = int(build_record["attempt_count"]) + 1
        await _recover_semantic_build(
            connection,
            build_table,
            build_id,
            lease_token,
            lease_seconds,
        )
        return None, attempt_count


async def claim_uhc_semantic_build(
    connection: asyncpg.Connection,
    identity: UhcSemanticBuildIdentity,
    *,
    lease_seconds: int = 300,
) -> UhcSemanticBuildClaim:
    """Claim or recover one exact build and recreate only its private stage."""

    identity.validate()
    if not 30 <= lease_seconds <= 3600:
        raise ValueError("lease_seconds must be in 30..=3600")
    build_id = identity.semantic_build_id
    lease_token = secrets.token_hex(32)
    schema = _schema_name()
    stage_relation = identity.stage_relation
    existing_claim, attempt_count = await _claim_semantic_build_transaction(
        connection,
        identity,
        lease_token,
        lease_seconds,
        schema,
        stage_relation,
    )
    if existing_claim is not None:
        return existing_claim
    return UhcSemanticBuildClaim(
        semantic_build_id=build_id,
        lease_token=lease_token,
        attempt_count=attempt_count,
        stage_schema=schema,
        stage_relation=stage_relation,
        sealed_reuse=False,
    )


async def heartbeat_uhc_semantic_build(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
    *,
    lease_seconds: int = 300,
) -> None:
    """Extend the active build lease after proving exact ownership."""

    if claim.sealed_reuse or claim.lease_token is None:
        raise UhcSemanticBuildStale("sealed UHC semantic builds have no lease")
    if not 30 <= lease_seconds <= 3600:
        raise ValueError("lease_seconds must be in 30..=3600")
    status = await connection.execute(
        f"""
        UPDATE {_table_ref('provider_directory_uhc_semantic_build')}
           SET heartbeat_at=now(),
               lease_expires_at=now() + (
                   $3::double precision * interval '1 second'
               ),
               updated_at=now()
         WHERE semantic_build_id=$1
           AND status='building'
           AND lease_token=$2
        """,
        claim.semantic_build_id,
        claim.lease_token,
        lease_seconds,
    )
    if status != "UPDATE 1":
        raise UhcSemanticBuildStale("UHC semantic build lease was lost")


async def copy_uhc_semantic_stage(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
    copy_stream: AsyncIterator[bytes],
) -> int:
    """Commit one all-or-nothing binary COPY while the exact lease is live."""

    if claim.sealed_reuse or claim.lease_token is None:
        raise UhcSemanticBuildStale("sealed UHC semantic builds cannot be copied")
    async with connection.transaction():
        owns_lease = await connection.fetchval(
            f"""
            SELECT EXISTS (
                SELECT 1
                  FROM {_table_ref('provider_directory_uhc_semantic_build')}
                 WHERE semantic_build_id=$1
                   AND status='building'
                   AND lease_token=$2
                   AND lease_expires_at > now()
            )
            """,
            claim.semantic_build_id,
            claim.lease_token,
        )
        if not owns_lease:
            raise UhcSemanticBuildStale("UHC semantic COPY lease is stale")
        copy_status = await connection.copy_to_table(
            claim.stage_relation,
            schema_name=claim.stage_schema,
            columns=UHC_SEMANTIC_COPY_COLUMNS,
            source=copy_stream,
            format="binary",
        )
        match = re.search(r"(\d+)$", str(copy_status))
        if match is None:
            raise UhcSemanticBuildError(
                "UHC semantic COPY row count is missing"
            )
        copied_row_count = int(match.group(1))
        status = await connection.execute(
            f"""
            UPDATE {_table_ref('provider_directory_uhc_semantic_build')}
               SET heartbeat_at=now(), updated_at=now()
             WHERE semantic_build_id=$1
               AND status='building'
               AND lease_token=$2
            """,
            claim.semantic_build_id,
            claim.lease_token,
        )
        if status != "UPDATE 1":
            raise UhcSemanticBuildStale(
                "UHC semantic build lease was lost during COPY"
            )
    return copied_row_count


async def quarantine_uhc_semantic_build(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
    *,
    failure_code: str,
) -> None:
    """Quarantine one owned build under a stable failure code."""

    if claim.sealed_reuse or claim.lease_token is None:
        raise UhcSemanticBuildStale("sealed UHC semantic builds cannot quarantine")
    if (
        not failure_code
        or len(failure_code) > 128
        or re.fullmatch(r"[a-z][a-z0-9_]{0,127}", failure_code) is None
    ):
        raise ValueError("failure_code must be a stable lowercase identifier")
    status = await connection.execute(
        f"""
        UPDATE {_table_ref('provider_directory_uhc_semantic_build')}
           SET status='quarantined',
               lease_token=NULL,
               lease_expires_at=NULL,
               failure_code=$3,
               updated_at=now()
         WHERE semantic_build_id=$1
           AND status='building'
           AND lease_token=$2
        """,
        claim.semantic_build_id,
        claim.lease_token,
        failure_code,
    )
    if status != "UPDATE 1":
        raise UhcSemanticBuildStale(
            "UHC semantic build lease was lost before quarantine"
        )


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise UhcSemanticBuildError(f"UHC semantic {field} is not an object")
    return value


def _report_int(
    report: Mapping[str, Any],
    field: str,
    *,
    positive: bool = False,
) -> int:
    value = report.get(field)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < (1 if positive else 0)
    ):
        raise UhcSemanticBuildError(f"UHC semantic {field} is invalid")
    return value


def _report_sha256(report: Mapping[str, Any], field: str) -> str:
    value = report.get(field)
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise UhcSemanticBuildError(f"UHC semantic {field} is invalid")
    return value


def _assert_native_contract_and_lineage(
    identity: UhcSemanticBuildIdentity,
    report: Mapping[str, Any],
) -> None:
    if (
        report.get("contract_id") != UHC_SEMANTIC_CONTRACT_ID
        or report.get("contract_version") != UHC_SEMANTIC_CONTRACT_VERSION
        or report.get("copy_format_id") != UHC_SEMANTIC_COPY_FORMAT_ID
        or report.get("source_id") != UHC_SEMANTIC_SOURCE_ID
        or report.get("encoder_sha256") != identity.encoder_sha256
    ):
        raise UhcSemanticBuildError("UHC semantic native contract mismatch")
    lineage = _mapping(report.get("lineage"), "lineage")
    expected_lineage_by_field = {
        "artifact_sha256": identity.artifact_sha256,
        "manifest_sha256": identity.manifest_sha256,
        "range_set_sha256": identity.range_set_sha256,
        "source_file_id": identity.source_file_id,
        "source_binding_id": (
            f"{identity.catalog_set_sha256}/{identity.source_file_id}"
        ),
        "collection_kind": identity.collection_kind,
    }
    if any(
        lineage.get(field) != expected_value
        for field, expected_value in expected_lineage_by_field.items()
    ):
        raise UhcSemanticBuildError("UHC semantic native lineage mismatch")


def _native_evidence_count(
    identity: UhcSemanticBuildIdentity,
    report: Mapping[str, Any],
    fact_count: int,
    quarantine_count: int,
) -> int:
    evidence_count = _report_int(report, "evidence_count")
    expected_evidence = 0
    if identity.collection_kind == "provider_membership":
        expected_evidence = fact_count - quarantine_count
    elif quarantine_count:
        raise UhcSemanticBuildError(
            "UHC semantic plan facts cannot contain provider quarantine"
        )
    if evidence_count != expected_evidence:
        raise UhcSemanticBuildError("UHC semantic native evidence count mismatch")
    return evidence_count


def _validated_native_counters(
    report: Mapping[str, Any],
    fact_count: int,
    quarantine_count: int,
) -> Mapping[str, Any]:
    counters = _mapping(report.get("counters"), "counters")
    if _report_int(counters, "invalid_npi_count") != quarantine_count:
        raise UhcSemanticBuildError(
            "UHC semantic native quarantine counters do not balance"
        )
    provider_count = _report_int(counters, "raw_provider_records")
    if quarantine_count > provider_quarantine_limit(provider_count):
        raise UhcSemanticBuildError(
            "UHC semantic native quarantine rate exceeds its ceiling"
        )
    try:
        provider_quarantine_rejected_counts(counters)
    except UhcProviderQuarantineError as error:
        raise UhcSemanticBuildError(
            "UHC semantic native quarantine counters are invalid"
        ) from error
    if provider_count + _report_int(counters, "raw_plan_records") != fact_count:
        raise UhcSemanticBuildError("UHC semantic native counters do not balance")
    return counters


def _validated_native_ranges(
    identity: UhcSemanticBuildIdentity,
    report: Mapping[str, Any],
) -> tuple[list[Any], list[Any]]:
    fact_blocks = report.get("fact_blocks")
    evidence_ranges = report.get("evidence_ranges")
    if (
        not isinstance(fact_blocks, list)
        or len(fact_blocks) != identity.raw_range_count
        or not isinstance(evidence_ranges, list)
        or len(evidence_ranges) != identity.raw_range_count
    ):
        raise UhcSemanticBuildError("UHC semantic range proof count mismatch")
    return fact_blocks, evidence_ranges


def _validate_native_report(
    identity: UhcSemanticBuildIdentity,
    report: Mapping[str, Any],
) -> tuple[int, int, Mapping[str, Any], list[Any], list[Any]]:
    """Validate one native report against the exact admitted raw identity."""
    _assert_native_contract_and_lineage(identity, report)
    fact_count = _report_int(report, "fact_count", positive=True)
    if fact_count != identity.raw_record_count:
        raise UhcSemanticBuildError(
            "UHC semantic native fact count does not match admitted raw layout"
        )
    quarantine_count = _report_int(report, "quarantine_count")
    _report_sha256(report, "quarantine_identity_set_sha256")
    if quarantine_count > UHC_PROVIDER_QUARANTINE_MAX_COUNT:
        raise UhcSemanticBuildError(
            "UHC semantic native quarantine count exceeds its ceiling"
        )
    evidence_count = _native_evidence_count(
        identity, report, fact_count, quarantine_count
    )
    for field in (
        "fact_set_sha256",
        "record_identity_set_sha256",
        "evidence_identity_set_sha256",
        "evidence_layout_set_sha256",
    ):
        _report_sha256(report, field)
    _report_sha256(report, "output_sha256")
    output_bytes = _report_int(report, "output_bytes", positive=True)
    copy_row_count = _report_int(report, "copy_row_count", positive=True)
    if output_bytes <= 0 or copy_row_count != evidence_count + identity.raw_range_count:
        raise UhcSemanticBuildError("UHC semantic native COPY proof mismatch")
    counters = _validated_native_counters(report, fact_count, quarantine_count)
    fact_blocks, evidence_ranges = _validated_native_ranges(identity, report)
    return fact_count, evidence_count, counters, fact_blocks, evidence_ranges


def _assert_verifier_report(
    identity: UhcSemanticBuildIdentity,
    native_report: Mapping[str, Any],
    verifier_report: Mapping[str, Any],
) -> str:
    verifier_sha256 = _report_sha256(verifier_report, "verifier_sha256")
    if verifier_sha256 != identity.semantic_verifier_sha256:
        raise UhcSemanticBuildError(
            "independent UHC semantic verifier identity changed"
        )
    for field in (
        "fact_count",
        "evidence_count",
        "quarantine_count",
        "quarantine_identity_set_sha256",
        "fact_set_sha256",
        "record_identity_set_sha256",
        "evidence_identity_set_sha256",
        "evidence_layout_set_sha256",
        "output_bytes",
        "output_sha256",
        "copy_row_count",
    ):
        if verifier_report.get(field) != native_report.get(field):
            raise UhcSemanticBuildError(
                f"independent UHC semantic verifier disagrees on {field}"
            )
    return verifier_sha256


def _stage_index_sql(claim: UhcSemanticBuildClaim) -> tuple[str, ...]:
    suffix = claim.semantic_build_id[:24]
    stage = claim.stage_ref
    return (
        f'CREATE UNIQUE INDEX IF NOT EXISTS "uhcs_{suffix}_fact_uq" '
        f"ON {stage} (range_ordinal) WHERE row_kind=1",
        f'CREATE UNIQUE INDEX IF NOT EXISTS "uhcs_{suffix}_evidence_uq" '
        f"ON {stage} (occurrence_ordinal) WHERE row_kind=2",
        f'CREATE INDEX IF NOT EXISTS "uhcs_{suffix}_npi_idx" '
        f"ON {stage} (npi) WHERE row_kind=2",
        f'CREATE INDEX IF NOT EXISTS "uhcs_{suffix}_run_idx" '
        f"ON {stage} (range_ordinal, run_ordinal, npi, occurrence_ordinal) "
        "WHERE row_kind=2",
    )


def _stage_shape_sql(stage_ref: str) -> str:
    return f"""
        SELECT count(*) FILTER (WHERE row_kind=1)::bigint AS fact_block_count,
               COALESCE(
                   sum(record_count) FILTER (WHERE row_kind=1),
                   0
               )::bigint AS fact_count,
               count(*) FILTER (WHERE row_kind=2)::bigint AS evidence_count,
               COALESCE(bool_and(
                   CASE row_kind
                     WHEN 1 THEN
                         range_ordinal >= 0
                         AND run_ordinal IS NULL
                         AND occurrence_ordinal IS NULL
                         AND record_start >= 0
                         AND record_count > 0
                         AND npi IS NULL
                         AND conflict_signature_pack IS NULL
                         AND payload_hash ~ '^[0-9a-f]{{64}}$'
                         AND semantic_hash ~ '^[0-9a-f]{{64}}$'
                         AND payload_bytes IS NOT NULL
                     WHEN 2 THEN
                         range_ordinal >= 0
                         AND run_ordinal >= 0
                         AND occurrence_ordinal >= 0
                         AND record_start IS NULL
                         AND record_count IS NULL
                         AND npi ~ '^[0-9]{{10}}$'
                         AND octet_length(conflict_signature_pack) = 288
                         AND payload_hash IS NULL
                         AND semantic_hash IS NULL
                         AND payload_bytes IS NULL
                     ELSE false
                   END
               ), false) AS rows_valid
          FROM {stage_ref}
    """


def _combined_counters(
    counters: Mapping[str, Any],
    evidence: UhcNpiEvidenceSummary,
    verifier_report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    combined_count_by_field = dict(counters)
    combined_count_by_field.update(
        {
            "distinct_npis": evidence.distinct_npis,
            "duplicate_npi_groups": evidence.duplicate_npi_groups,
            "conflicting_npi_groups": evidence.conflicting_npi_groups,
            "conflict_counts": evidence.conflict_counts,
            "rejected_counts": provider_quarantine_rejected_counts(counters),
            "unknown_field_counts": {},
            "intentional_drop_counts": {},
        }
    )
    if verifier_report is not None:
        combined_count_by_field["copy_proof"] = {
            field_name: verifier_report[field_name]
            for field_name in (
                "output_bytes",
                "output_sha256",
                "copy_row_count",
            )
        }
        combined_count_by_field["npi_evidence_proof_sha256"] = (
            evidence.proof_sha256
        )
        combined_count_by_field["quarantine_identity_set_sha256"] = (
            verifier_report["quarantine_identity_set_sha256"]
        )
    return combined_count_by_field


@dataclass(frozen=True)
class UhcSemanticSealResult:
    semantic_build_id: str
    attempt_count: int
    fact_count: int
    evidence_count: int
    source_summary: dict[str, Any]
    sealed_at: datetime


@dataclass(frozen=True)
class _SemanticSealProof:
    fact_count: int
    evidence_count: int
    counters: dict[str, int]
    fact_blocks: list[dict[str, Any]]
    evidence_ranges: list[dict[str, Any]]
    verifier_sha256: str


def _semantic_seal_proof(
    identity: UhcSemanticBuildIdentity,
    native_report: Mapping[str, Any],
    verifier_report: Mapping[str, Any],
) -> _SemanticSealProof:
    fact_count, evidence_count, counters, fact_blocks, evidence_ranges = (
        _validate_native_report(identity, native_report)
    )
    return _SemanticSealProof(
        fact_count=fact_count,
        evidence_count=evidence_count,
        counters=counters,
        fact_blocks=fact_blocks,
        evidence_ranges=evidence_ranges,
        verifier_sha256=_assert_verifier_report(
            identity,
            native_report,
            verifier_report,
        ),
    )


async def prepare_uhc_semantic_stage_indexes(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
) -> None:
    """Build retry-safe deferred indexes before independent verification."""

    if claim.sealed_reuse or claim.lease_token is None:
        raise UhcSemanticBuildStale("sealed UHC semantic builds need no indexes")
    async with connection.transaction():
        owns_lease = await connection.fetchval(
            f"""
            SELECT EXISTS (
                SELECT 1
                  FROM {_table_ref('provider_directory_uhc_semantic_build')}
                 WHERE semantic_build_id=$1
                   AND status='building'
                   AND lease_token=$2
                   AND lease_expires_at > now()
            )
            """,
            claim.semantic_build_id,
            claim.lease_token,
        )
        if not owns_lease:
            raise UhcSemanticBuildStale(
                "UHC semantic index-build lease is stale"
            )
        for statement in _stage_index_sql(claim):
            await connection.execute(statement)
        await connection.execute(f"ANALYZE {claim.stage_ref}")


async def _lock_build_for_seal(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
    identity: UhcSemanticBuildIdentity,
    build_table: str,
) -> Mapping[str, Any]:
    build_row = await connection.fetchrow(
        f"""
        SELECT *, lease_expires_at > now() AS lease_active
          FROM {build_table}
         WHERE semantic_build_id=$1
         FOR UPDATE
        """,
        claim.semantic_build_id,
    )
    if (
        build_row is None
        or build_row["status"] != "building"
        or build_row["lease_token"] != claim.lease_token
        or not bool(build_row["lease_active"])
    ):
        raise UhcSemanticBuildStale("UHC semantic seal lease is stale")
    _assert_identity_row(build_row, identity)
    return build_row


async def _assert_semantic_stage_shape(
    connection: asyncpg.Connection,
    stage_ref: str,
    identity: UhcSemanticBuildIdentity,
    fact_count: int,
    evidence_count: int,
    fact_blocks: list[dict[str, Any]],
) -> None:
    shape = await connection.fetchrow(_stage_shape_sql(stage_ref))
    if (
        shape is None
        or int(shape["fact_block_count"]) != identity.raw_range_count
        or int(shape["fact_count"]) != fact_count
        or int(shape["evidence_count"]) != evidence_count
        or not bool(shape["rows_valid"])
    ):
        raise UhcSemanticBuildError("UHC semantic stage shape proof failed")
    staged_blocks = await connection.fetch(
        f"""
        SELECT range_ordinal, record_start, record_count,
               record_count AS fact_count,
               octet_length(payload_bytes)::bigint AS compressed_bytes,
               payload_hash AS compressed_payload_sha256,
               semantic_hash AS semantic_block_sha256
          FROM {stage_ref}
         WHERE row_kind=1 ORDER BY range_ordinal
        """
    )
    if [dict(stage_row) for stage_row in staged_blocks] != fact_blocks:
        raise UhcSemanticBuildError(
            "UHC semantic fact block metadata disagrees with native proof"
        )


async def _store_semantic_seal(
    connection: asyncpg.Connection,
    build_table: str,
    claim: UhcSemanticBuildClaim,
    native_report: Mapping[str, Any],
    proof: _SemanticSealProof,
    source_summary: dict[str, Any],
) -> Mapping[str, Any]:
    sealed_build_record = await connection.fetchrow(
        f"""
        UPDATE {build_table}
           SET status='sealed', lease_token=NULL, lease_expires_at=NULL,
               heartbeat_at=now(), fact_count=$3, evidence_count=$4,
               fact_set_sha256=$5, record_identity_set_sha256=$6,
               evidence_identity_set_sha256=$7, evidence_layout_set_sha256=$8,
               verifier_sha256=$9, counters_json=$10::jsonb,
               fact_blocks_json=$11::jsonb, evidence_ranges_json=$12::jsonb,
               failure_code=NULL, verified_at=now(), sealed_at=now(), updated_at=now()
         WHERE semantic_build_id=$1 AND status='building'
           AND lease_token=$2 AND lease_expires_at > now()
     RETURNING attempt_count, sealed_at
        """,
        claim.semantic_build_id,
        claim.lease_token,
        proof.fact_count,
        proof.evidence_count,
        _report_sha256(native_report, "fact_set_sha256"),
        _report_sha256(native_report, "record_identity_set_sha256"),
        _report_sha256(native_report, "evidence_identity_set_sha256"),
        _report_sha256(native_report, "evidence_layout_set_sha256"),
        proof.verifier_sha256,
        json.dumps(source_summary, sort_keys=True, separators=(",", ":")),
        json.dumps(proof.fact_blocks, sort_keys=True, separators=(",", ":")),
        json.dumps(proof.evidence_ranges, sort_keys=True, separators=(",", ":")),
    )
    if sealed_build_record is None:
        raise UhcSemanticBuildStale("UHC semantic lease expired during final seal")
    return sealed_build_record


async def seal_uhc_semantic_build(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
    identity: UhcSemanticBuildIdentity,
    native_report: Mapping[str, Any],
    verifier_report: Mapping[str, Any],
) -> UhcSemanticSealResult:
    """Create deferred indexes, validate all proofs, and atomically seal."""
    if claim.sealed_reuse or claim.lease_token is None:
        raise UhcSemanticBuildStale("reused UHC semantic build is already sealed")
    if claim.semantic_build_id != identity.semantic_build_id:
        raise UhcSemanticBuildError("UHC semantic claim identity mismatch")
    proof = _semantic_seal_proof(identity, native_report, verifier_report)
    stage_ref = claim.stage_ref
    build_table = _table_ref("provider_directory_uhc_semantic_build")

    async with connection.transaction():
        await _lock_build_for_seal(
            connection,
            claim,
            identity,
            build_table,
        )
        await _assert_semantic_stage_shape(
            connection,
            stage_ref,
            identity,
            proof.fact_count,
            proof.evidence_count,
            proof.fact_blocks,
        )
        for statement in _stage_index_sql(claim):
            await connection.execute(statement)
        await connection.execute(f"ANALYZE {stage_ref}")
        evidence = await summarize_uhc_npi_evidence(
            connection,
            f"{claim.stage_schema}.{claim.stage_relation}",
            expected_evidence_count=proof.evidence_count,
        )
        source_summary = _combined_counters(
            proof.counters,
            evidence,
            verifier_report,
        )
        sealed_build_record = await _store_semantic_seal(
            connection,
            build_table,
            claim,
            native_report,
            proof,
            source_summary,
        )
    return UhcSemanticSealResult(
        semantic_build_id=claim.semantic_build_id,
        attempt_count=int(sealed_build_record["attempt_count"]),
        fact_count=proof.fact_count,
        evidence_count=proof.evidence_count,
        source_summary=source_summary,
        sealed_at=sealed_build_record["sealed_at"],
    )


async def load_sealed_uhc_semantic_build(
    connection: asyncpg.Connection,
    identity: UhcSemanticBuildIdentity,
) -> Mapping[str, Any] | None:
    """Return only an immutable exact-identity SEALED build."""

    sealed_build_record = await connection.fetchrow(
        f"""
        SELECT *
          FROM {_table_ref('provider_directory_uhc_semantic_build')}
         WHERE semantic_build_id=$1
           AND status='sealed'
        """,
        identity.semantic_build_id,
    )
    if sealed_build_record is None:
        return None
    _assert_identity_row(sealed_build_record, identity)
    for field in (
        "fact_set_sha256",
        "record_identity_set_sha256",
        "evidence_identity_set_sha256",
        "evidence_layout_set_sha256",
        "verifier_sha256",
    ):
        proof_hash = sealed_build_record[field]
        if (
            not isinstance(proof_hash, str)
            or _SHA256_RE.fullmatch(proof_hash) is None
        ):
            raise UhcSemanticBuildError(
                "sealed UHC semantic registry proof is invalid"
            )
    return sealed_build_record
