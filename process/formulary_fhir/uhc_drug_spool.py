# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict streaming normalization of retained UHC drug JSON into SQLite."""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import sqlite3
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

import ijson

from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.source_artifacts import open_verified_source_artifact
from process.formulary_fhir.uhc_drug_normalization import (
    NormalizedUHCDrugMembership,
)
from process.formulary_fhir.uhc_drug_normalization import SPOOL_CONTRACT
from process.formulary_fhir.uhc_drug_normalization import (
    UHCDrugNormalizationError,
)
from process.formulary_fhir.uhc_drug_normalization import (
    normalized_uhc_drug_memberships,
)
from process.formulary_fhir.uhc_drug_payload import UHCDrugPayloadError
from process.formulary_fhir.uhc_drug_payload import count_uhc_drug_stream_items
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence
from process.formulary_fhir.uhc_drug_spool_contract import artifact_proof_rows
from process.formulary_fhir.uhc_drug_spool_contract import spool_evidence_payload
from process.formulary_fhir.uhc_drug_spool_merge import upsert_spool_membership
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID


def _create_spool(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode = DELETE;
        PRAGMA synchronous = FULL;
        PRAGMA foreign_keys = ON;
        CREATE TABLE membership (
            source_plan_identifier TEXT NOT NULL,
            family TEXT NOT NULL,
            plan_id_type TEXT NOT NULL,
            plan_id TEXT NOT NULL,
            plan_year INTEGER NOT NULL,
            rxnorm_id TEXT NOT NULL,
            drug_name TEXT NOT NULL,
            drug_tier TEXT NOT NULL,
            prior_authorization INTEGER,
            step_therapy INTEGER,
            quantity_limit INTEGER,
            effective_updated_at TEXT NOT NULL,
            semantic_json TEXT NOT NULL,
            provenance_json TEXT NOT NULL,
            PRIMARY KEY (source_plan_identifier, rxnorm_id)
        ) WITHOUT ROWID;
        CREATE TABLE spool_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            evidence_json TEXT NOT NULL,
            artifact_proof_json TEXT NOT NULL
        );
        """
    )


def _spool_content_sha256(
    connection: sqlite3.Connection,
    cancel_check: Callable[[], None] | None,
) -> str:
    digest = hashlib.sha256()
    digest.update(SPOOL_CONTRACT.encode("ascii"))
    digest.update(b"\n")
    metadata_rows = connection.execute(
        "SELECT evidence_json, artifact_proof_json FROM spool_metadata "
        "WHERE singleton = 1"
    ).fetchall()
    if len(metadata_rows) != 1:
        raise RuntimeError("UHC drug spool metadata is invalid")
    digest.update(json_text(list(metadata_rows[0])).encode("utf-8"))
    digest.update(b"\n")
    cursor = connection.execute(
        "SELECT source_plan_identifier, family, plan_id_type, plan_id, plan_year, "
        "rxnorm_id, drug_name, drug_tier, prior_authorization, step_therapy, "
        "quantity_limit, effective_updated_at, semantic_json, provenance_json "
        "FROM membership ORDER BY source_plan_identifier, rxnorm_id"
    )
    for row_index, database_row in enumerate(cursor, start=1):
        if cancel_check is not None and row_index % 1_024 == 0:
            cancel_check()
        digest.update(json_text(list(database_row)).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _validated_artifact_set(
    artifact_set: object,
) -> VerifiedSourceArtifactSet:
    if (
        type(artifact_set) is not VerifiedSourceArtifactSet
        or artifact_set.source_id != UHC_FORMULARY_SOURCE_ID
        or not 1 <= len(artifact_set.artifacts) <= 48
    ):
        raise UHCDrugNormalizationError("UHC drug artifact set is invalid")
    family_count_by_name = {
        family: sum(
            artifact.identity.family == family for artifact in artifact_set.artifacts
        )
        for family in ("cs", "ifp")
    }
    if any(count > 24 for count in family_count_by_name.values()):
        raise UHCDrugNormalizationError("UHC drug artifact census is incomplete")
    return artifact_set


def _validated_spool_destination(spool_path: Path | str) -> Path:
    try:
        exact_path = Path(spool_path)
        parent_path = exact_path.parent
        resolved_parent = parent_path.resolve(strict=True)
        parent_state = parent_path.lstat()
    except (OSError, TypeError, ValueError):
        raise UHCDrugNormalizationError("UHC drug spool path is invalid") from None
    if (
        not exact_path.is_absolute()
        or exact_path.exists()
        or parent_path != resolved_parent
        or not stat.S_ISDIR(parent_state.st_mode)
        or parent_state.st_uid != os.geteuid()
        or stat.S_IMODE(parent_state.st_mode) & 0o077
    ):
        raise UHCDrugNormalizationError("UHC drug spool path is invalid")
    return exact_path


@dataclass(slots=True)
class _SpoolCensus:
    raw_record_count: int = 0
    raw_plan_entry_count: int = 0
    duplicate_count: int = 0
    superseded_count: int = 0
    maximum_updated_at: dt.datetime | None = None

    def observe(
        self,
        membership: NormalizedUHCDrugMembership,
        duplicate_delta: int,
        superseded_delta: int,
    ) -> None:
        """Accumulate one normalized membership and its merge outcome."""

        self.duplicate_count += duplicate_delta
        self.superseded_count += superseded_delta
        if (
            self.maximum_updated_at is None
            or membership.effective_updated_at > self.maximum_updated_at
        ):
            self.maximum_updated_at = membership.effective_updated_at


def _evidence_payload(
    artifact_set: VerifiedSourceArtifactSet,
    census: _SpoolCensus,
    plan_count: int,
    membership_count: int,
) -> dict[str, object]:
    if census.maximum_updated_at is None:
        raise RuntimeError("UHC drug spool timestamp census is empty")
    evidence_by_field = {
        "artifact_set_sha256": artifact_set.artifact_set_sha256,
        "duplicate_count": census.duplicate_count,
        "file_count": len(artifact_set.artifacts),
        "max_last_updated_at": census.maximum_updated_at.isoformat(),
        "medication_membership_count": membership_count,
        "plan_count": plan_count,
        "raw_plan_entry_count": census.raw_plan_entry_count,
        "raw_record_count": census.raw_record_count,
        "source_file_set_sha256": artifact_set.source_file_set_sha256,
        "source_id": artifact_set.source_id,
        "superseded_count": census.superseded_count,
    }
    if len(artifact_set.artifacts) != 48:
        evidence_by_field.update(
            {
                "excluded_file_count": 48 - len(artifact_set.artifacts),
                "expected_file_count": 48,
            }
        )
    return evidence_by_field


def _install_spool_metadata(
    connection: sqlite3.Connection,
    artifact_set: VerifiedSourceArtifactSet,
    census: _SpoolCensus,
    plan_count: int,
    membership_count: int,
) -> dict[str, object]:
    evidence_by_field = _evidence_payload(
        artifact_set,
        census,
        plan_count,
        membership_count,
    )
    connection.execute(
        "INSERT INTO spool_metadata (singleton, evidence_json, "
        "artifact_proof_json) VALUES (1, ?, ?)",
        (
            json_text(evidence_by_field),
            json_text(list(artifact_proof_rows(artifact_set))),
        ),
    )
    return evidence_by_field


def _observe_normalized_memberships(
    connection: sqlite3.Connection,
    normalized_memberships: tuple[NormalizedUHCDrugMembership, ...],
    census: _SpoolCensus,
    cancel_check: Callable[[], None] | None,
) -> None:
    for membership in normalized_memberships:
        if cancel_check is not None:
            cancel_check()
        duplicate_delta, superseded_delta = upsert_spool_membership(
            connection,
            membership,
        )
        census.observe(membership, duplicate_delta, superseded_delta)


def normalized_uhc_drug_source_records(
    artifact: object,
    input_file: object,
    cancel_check: Callable[[], None] | None,
) -> Iterator[tuple[dict[str, Any], tuple[NormalizedUHCDrugMembership, ...]]]:
    """Yield every source record with the exact normalized memberships."""

    source_records = ijson.items(input_file, "item", use_float=False)
    for record_ordinal, source_record in enumerate(source_records, start=1):
        if cancel_check is not None:
            cancel_check()
        normalized_memberships = normalized_uhc_drug_memberships(
            source_record,
            artifact,
            record_ordinal,
        )
        yield source_record, normalized_memberships


def _consume_source_records(
    connection: sqlite3.Connection,
    artifact: object,
    input_file: object,
    census: _SpoolCensus,
    cancel_check: Callable[[], None] | None,
) -> int:
    observed_record_count = 0
    for source_record, normalized_memberships in normalized_uhc_drug_source_records(
        artifact,
        input_file,
        cancel_check,
    ):
        observed_record_count += 1
        census.raw_record_count += 1
        census.raw_plan_entry_count += len(source_record["plans"])
        _observe_normalized_memberships(
            connection,
            normalized_memberships,
            census,
            cancel_check,
        )
    return observed_record_count


def _consume_artifact(
    connection: sqlite3.Connection,
    artifact: object,
    census: _SpoolCensus,
    cancel_check: Callable[[], None] | None,
) -> None:
    try:
        if cancel_check is not None:
            cancel_check()
        with open_verified_source_artifact(artifact) as validation_file:
            expected_record_count = count_uhc_drug_stream_items(
                validation_file,
                cancel_check=cancel_check,
            )
        with open_verified_source_artifact(artifact) as input_file:
            observed_record_count = _consume_source_records(
                connection,
                artifact,
                input_file,
                census,
                cancel_check,
            )
        if observed_record_count != expected_record_count:
            raise UHCDrugNormalizationError(
                "UHC drug retained JSON record census changed"
            )
    except (ijson.JSONError, UHCDrugPayloadError):
        raise UHCDrugNormalizationError(
            "UHC drug retained JSON is invalid"
        ) from None


def _spool_evidence(
    connection: sqlite3.Connection,
    artifact_set: VerifiedSourceArtifactSet,
    census: _SpoolCensus,
    cancel_check: Callable[[], None] | None,
) -> UHCDrugSpoolEvidence:
    plan_count = connection.execute(
        "SELECT COUNT(DISTINCT source_plan_identifier) FROM membership"
    ).fetchone()[0]
    membership_count = connection.execute(
        "SELECT COUNT(*) FROM membership"
    ).fetchone()[0]
    evidence_by_field = _install_spool_metadata(
        connection,
        artifact_set,
        census,
        plan_count,
        membership_count,
    )
    evidence = UHCDrugSpoolEvidence(
        source_id=artifact_set.source_id,
        source_file_set_sha256=artifact_set.source_file_set_sha256,
        artifact_set_sha256=artifact_set.artifact_set_sha256,
        spool_content_sha256=_spool_content_sha256(connection, cancel_check),
        file_count=len(artifact_set.artifacts),
        raw_record_count=census.raw_record_count,
        raw_plan_entry_count=census.raw_plan_entry_count,
        plan_count=plan_count,
        medication_membership_count=membership_count,
        duplicate_count=census.duplicate_count,
        superseded_count=census.superseded_count,
        max_last_updated_at=census.maximum_updated_at,
        expected_file_count=48,
        excluded_file_count=48 - len(artifact_set.artifacts),
    )
    if spool_evidence_payload(evidence) != evidence_by_field:
        raise RuntimeError("UHC drug spool evidence metadata is inconsistent")
    return evidence


def materialize_uhc_drug_spool(
    artifact_set: VerifiedSourceArtifactSet,
    *,
    spool_path: Path | str,
    cancel_check: Callable[[], None] | None = None,
) -> UHCDrugSpoolEvidence:
    """Parse all retained artifacts once into one deterministic bounded spool."""

    exact_set = _validated_artifact_set(artifact_set)
    exact_path = _validated_spool_destination(spool_path)
    census = _SpoolCensus()
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(exact_path)
        os.chmod(exact_path, 0o600)
        _create_spool(connection)
        connection.execute("BEGIN IMMEDIATE")
        for artifact in exact_set.artifacts:
            _consume_artifact(connection, artifact, census, cancel_check)
        if cancel_check is not None:
            cancel_check()
        evidence = _spool_evidence(
            connection,
            exact_set,
            census,
            cancel_check,
        )
        connection.commit()
        return evidence
    except BaseException:
        if connection is not None:
            connection.rollback()
            connection.close()
            connection = None
        exact_path.unlink(missing_ok=True)
        raise
    finally:
        if connection is not None:
            connection.close()


__all__ = (
    "SPOOL_CONTRACT",
    "UHCDrugNormalizationError",
    "materialize_uhc_drug_spool",
    "normalized_uhc_drug_source_records",
)
