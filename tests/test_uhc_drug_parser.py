# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
from dataclasses import replace
import json
import os
import sqlite3

import pytest

import process.formulary_fhir.uhc_drug_parser as parser
import process.formulary_fhir.uhc_drug_spool as spool
import process.formulary_fhir.uhc_drug_spool_storage as spool_storage
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.types import RXNORM_SYSTEM_URI
from process.formulary_fhir.uhc_source import UHC_FORMULARY_CANONICAL_BASE
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID
from tests.uhc_drug_parser_test_support import artifact_set
from tests.uhc_drug_parser_test_support import install_artifact_reader


def _prepared_spool(
    monkeypatch,
    tmp_path,
    *,
    records_by_artifact=None,
    timestamps_by_artifact=None,
):
    tmp_path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(tmp_path, 0o700)
    artifacts, bodies = artifact_set(
        records_by_artifact,
        timestamps_by_artifact,
    )
    install_artifact_reader(monkeypatch, spool, bodies)
    spool_path = tmp_path / "uhc-drugs.sqlite"
    evidence = spool.materialize_uhc_drug_spool(
        artifacts,
        spool_path=spool_path,
    )
    return artifacts, spool_path, evidence


def test_parser_materializes_repository_native_plan_and_medication(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, spool_path, evidence = _prepared_spool(monkeypatch, tmp_path)

    with parser.open_verified_uhc_drug_spool(
        spool_path,
        evidence,
        artifacts,
    ) as spool_snapshot:
        keys = parser.spooled_uhc_plan_keys(spool_snapshot)
        materialized = parser.load_spooled_uhc_plan(
            spool_snapshot,
            keys[0],
            source_id=UHC_FORMULARY_SOURCE_ID,
            canonical_base=UHC_FORMULARY_CANONICAL_BASE,
            evidence=evidence,
        )

    plan = materialized.coverage_plan
    medication = materialized.medications[0]
    assert len(keys) == 2
    assert keys[0].source_plan_identifier.startswith("uhc:")
    assert "PLAN / PUBLIC 01" not in keys[0].source_plan_identifier
    assert plan.upstream_list_id.startswith("uhc-")
    assert plan.period_start == dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    assert plan.period_end == dt.datetime(2027, 1, 1, tzinfo=dt.UTC)
    assert plan.raw_identifiers[0]["plan_id"] == "PLAN / PUBLIC 01"
    assert medication.upstream_medication_id == "1234567"
    assert medication.codings[0].system == RXNORM_SYSTEM_URI
    assert medication.drug_tier == "Preferred Brand"
    assert medication.prior_authorization is False
    assert medication.quantity_limit is True
    assert medication.source_plan_identifiers == (
        keys[0].source_plan_identifier,
    )


def test_parser_rejects_spool_hash_or_policy_tamper(monkeypatch, tmp_path) -> None:
    artifacts, spool_path, evidence = _prepared_spool(monkeypatch, tmp_path)
    with parser.open_verified_uhc_drug_spool(
        spool_path,
        evidence,
        artifacts,
    ) as spool_snapshot:
        key = parser.spooled_uhc_plan_keys(spool_snapshot)[0]
        connection = sqlite3.connect(spool_path)
        try:
            connection.execute("UPDATE membership SET prior_authorization = 2")
            connection.commit()
        finally:
            connection.close()
        with pytest.raises(ValueError, match="spool is unavailable"):
            parser.load_spooled_uhc_plan(
                spool_snapshot,
                key,
                source_id=UHC_FORMULARY_SOURCE_ID,
                canonical_base=UHC_FORMULARY_CANONICAL_BASE,
                evidence=evidence,
            )

    with pytest.raises(RuntimeError, match="evidence changed"):
        parser.verify_spooled_uhc_evidence(spool_path, evidence, artifacts)


@pytest.mark.parametrize("invalid_value", (2, -1, "1", True))
def test_parser_rejects_nonexact_sqlite_policy_values(invalid_value) -> None:
    with pytest.raises(RuntimeError, match="policy flag"):
        parser.spool_policy_value(invalid_value)


@pytest.mark.parametrize(
    "field_name",
    (
        "raw_record_count",
        "raw_plan_entry_count",
        "duplicate_count",
        "superseded_count",
    ),
)
def test_parser_rejects_forged_audit_census(
    monkeypatch,
    tmp_path,
    field_name,
) -> None:
    artifacts, spool_path, evidence = _prepared_spool(monkeypatch, tmp_path)
    forged_evidence = replace(
        evidence,
        **{field_name: getattr(evidence, field_name) + 1},
    )

    with pytest.raises(RuntimeError, match="metadata is inconsistent"):
        parser.verify_spooled_uhc_evidence(
            spool_path,
            forged_evidence,
            artifacts,
        )


def test_spool_evidence_requires_canonical_utc_timestamp(
    monkeypatch,
    tmp_path,
) -> None:
    _artifacts, _spool_path, evidence = _prepared_spool(monkeypatch, tmp_path)
    offset_timestamp = evidence.max_last_updated_at.astimezone(
        dt.timezone(dt.timedelta(hours=2))
    )

    with pytest.raises(ValueError, match="maximum update timestamp"):
        replace(evidence, max_last_updated_at=offset_timestamp)


def test_parser_rejects_tampered_metadata_and_artifact_set(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, spool_path, evidence = _prepared_spool(monkeypatch, tmp_path)
    connection = sqlite3.connect(spool_path)
    try:
        raw_metadata = connection.execute(
            "SELECT evidence_json FROM spool_metadata WHERE singleton = 1"
        ).fetchone()[0]
        metadata_by_field = json.loads(raw_metadata)
        metadata_by_field["duplicate_count"] += 1
        connection.execute(
            "UPDATE spool_metadata SET evidence_json = ? WHERE singleton = 1",
            (json_text(metadata_by_field),),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RuntimeError, match="metadata is inconsistent"):
        parser.verify_spooled_uhc_evidence(spool_path, evidence, artifacts)

    other_artifacts, _bodies = artifact_set(
        timestamps_by_index={0: "2026-08-09T00:00:00Z"}
    )
    with pytest.raises(ValueError, match="evidence is invalid"):
        parser.verify_spooled_uhc_evidence(
            spool_path,
            evidence,
            other_artifacts,
        )


def test_parser_rejects_provenance_artifact_drift(monkeypatch, tmp_path) -> None:
    artifacts, spool_path, evidence = _prepared_spool(monkeypatch, tmp_path)
    connection = sqlite3.connect(spool_path)
    try:
        alias, rxnorm_id, raw_provenance = connection.execute(
            "SELECT source_plan_identifier, rxnorm_id, provenance_json "
            "FROM membership ORDER BY source_plan_identifier LIMIT 1"
        ).fetchone()
        provenance_records = json.loads(raw_provenance)
        provenance_records[0]["file_name"] = "different.json"
        connection.execute(
            "UPDATE membership SET provenance_json = ? WHERE "
            "source_plan_identifier = ? AND rxnorm_id = ?",
            (json_text(provenance_records), alias, rxnorm_id),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RuntimeError, match="provenance artifact is invalid"):
        parser.verify_spooled_uhc_evidence(spool_path, evidence, artifacts)


def test_parser_requires_every_source_file_witness(monkeypatch, tmp_path) -> None:
    artifacts, spool_path, evidence = _prepared_spool(monkeypatch, tmp_path)
    omitted_file_id = artifacts.artifacts[0].identity.source_file_id
    connection = sqlite3.connect(spool_path)
    try:
        membership_rows = connection.execute(
            "SELECT source_plan_identifier, rxnorm_id, provenance_json "
            "FROM membership"
        ).fetchall()
        for alias, rxnorm_id, raw_provenance in membership_rows:
            provenance_records = [
                provenance_by_field
                for provenance_by_field in json.loads(raw_provenance)
                if provenance_by_field["source_file_id"] != omitted_file_id
            ]
            connection.execute(
                "UPDATE membership SET provenance_json = ? WHERE "
                "source_plan_identifier = ? AND rxnorm_id = ?",
                (json_text(provenance_records), alias, rxnorm_id),
            )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RuntimeError, match="artifact census is incomplete"):
        parser.verify_spooled_uhc_evidence(spool_path, evidence, artifacts)


def test_spool_reader_rejects_symlink_ancestor_and_pins_leaf(
    monkeypatch,
    tmp_path,
) -> None:
    _artifacts, spool_path, _evidence = _prepared_spool(monkeypatch, tmp_path)
    alias_directory = tmp_path.parent / f"{tmp_path.name}-alias"
    alias_directory.symlink_to(tmp_path, target_is_directory=True)
    try:
        with pytest.raises(ValueError, match="spool is unavailable"):
            with parser.open_uhc_drug_spool(
                alias_directory / spool_path.name
            ):
                raise AssertionError("symlinked spool unexpectedly opened")
    finally:
        alias_directory.unlink()

    replacement_path = tmp_path / "replacement.sqlite"
    replacement = sqlite3.connect(replacement_path)
    replacement.execute("CREATE TABLE marker (value TEXT)")
    replacement.commit()
    replacement.close()
    os.chmod(replacement_path, 0o600)
    with parser.open_uhc_drug_spool(spool_path) as connection:
        os.replace(replacement_path, spool_path)
        membership_count = connection.execute(
            "SELECT COUNT(*) FROM membership"
        ).fetchone()[0]
    assert membership_count > 0


def test_verified_snapshot_rejects_valid_path_replacement(
    monkeypatch,
    tmp_path,
) -> None:
    from tests.uhc_drug_parser_test_support import source_record

    first_directory = tmp_path / "first"
    second_directory = tmp_path / "second"
    artifacts, first_path, evidence = _prepared_spool(
        monkeypatch,
        first_directory,
        records_by_artifact={
            index: [source_record(tier="First tier")] for index in range(48)
        },
    )
    _other_artifacts, second_path, _other_evidence = _prepared_spool(
        monkeypatch,
        second_directory,
        records_by_artifact={
            index: [source_record(tier="Second tier")] for index in range(48)
        },
    )

    with parser.open_verified_uhc_drug_spool(
        first_path,
        evidence,
        artifacts,
        ) as spool_snapshot:
        plan_key = parser.spooled_uhc_plan_keys(spool_snapshot)[0]
        os.replace(second_path, first_path)
        with pytest.raises(ValueError, match="spool is unavailable"):
            parser.load_spooled_uhc_plan(
                spool_snapshot,
                plan_key,
                source_id=UHC_FORMULARY_SOURCE_ID,
                canonical_base=UHC_FORMULARY_CANONICAL_BASE,
                evidence=evidence,
            )


def test_parser_enforces_bounded_plan_membership(
    monkeypatch,
    tmp_path,
) -> None:
    from tests.uhc_drug_parser_test_support import source_record

    source_records = [
        source_record(rxnorm_id="1234567"),
        source_record(rxnorm_id="7654321"),
    ]
    artifacts, spool_path, evidence = _prepared_spool(
        monkeypatch,
        tmp_path,
        records_by_artifact={index: source_records for index in range(48)},
    )
    monkeypatch.setattr(parser, "MAX_MEDICATIONS_PER_PLAN", 1)

    with parser.open_verified_uhc_drug_spool(
        spool_path,
        evidence,
        artifacts,
    ) as spool_snapshot:
        plan_key = parser.spooled_uhc_plan_keys(spool_snapshot)[0]
        with pytest.raises(RuntimeError, match="plan is incomplete"):
            parser.load_spooled_uhc_plan(
                spool_snapshot,
                plan_key,
                source_id=UHC_FORMULARY_SOURCE_ID,
                canonical_base=UHC_FORMULARY_CANONICAL_BASE,
                evidence=evidence,
            )


def test_parser_enforces_bounded_plan_materialized_bytes(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, spool_path, evidence = _prepared_spool(monkeypatch, tmp_path)
    monkeypatch.setattr(parser, "MAX_PLAN_MATERIALIZED_BYTES", 1)

    with parser.open_verified_uhc_drug_spool(
        spool_path,
        evidence,
        artifacts,
    ) as spool_snapshot:
        plan_key = parser.spooled_uhc_plan_keys(spool_snapshot)[0]
        with pytest.raises(RuntimeError, match="plan is too large"):
            parser.load_spooled_uhc_plan(
                spool_snapshot,
                plan_key,
                source_id=UHC_FORMULARY_SOURCE_ID,
                canonical_base=UHC_FORMULARY_CANONICAL_BASE,
                evidence=evidence,
            )


def test_verified_spool_capability_rejects_caller_token() -> None:
    with pytest.raises(ValueError, match="verification capability"):
        spool_storage._VerifiedUHCDrugSpool(
            descriptor=-1,
            device=1,
            inode=1,
            byte_count=1,
            modified_ns=1,
            changed_ns=1,
            source_id=UHC_FORMULARY_SOURCE_ID,
            spool_content_sha256="a" * 64,
            artifact_set_sha256="b" * 64,
            verification_token=object(),
        )
    assert "_VERIFIED_SPOOL_TOKEN" not in parser.__dict__


def test_parser_outputs_are_deterministic(monkeypatch, tmp_path) -> None:
    _first_artifacts, first_path, first_evidence = _prepared_spool(
        monkeypatch,
        tmp_path / "a",
    )
    _second_artifacts, second_path, second_evidence = _prepared_spool(
        monkeypatch,
        tmp_path / "b",
    )

    assert first_evidence == second_evidence
    assert parser.spooled_uhc_plan_keys(first_path) == (
        parser.spooled_uhc_plan_keys(second_path)
    )


def test_parser_preserves_selected_and_superseded_source_witnesses(
    monkeypatch,
    tmp_path,
) -> None:
    from tests.uhc_drug_parser_test_support import source_record

    records_by_artifact = {0: [source_record(tier="Old tier")]}
    records_by_artifact.update(
        {index: [source_record(tier="New tier")] for index in range(1, 24)}
    )
    timestamps_by_artifact = {0: "2026-08-09T00:00:00Z"}
    timestamps_by_artifact.update(
        {index: "2026-08-10T00:00:00Z" for index in range(1, 24)}
    )
    artifacts, spool_path, evidence = _prepared_spool(
        monkeypatch,
        tmp_path,
        records_by_artifact=records_by_artifact,
        timestamps_by_artifact=timestamps_by_artifact,
    )
    with parser.open_verified_uhc_drug_spool(
        spool_path,
        evidence,
        artifacts,
    ) as spool_snapshot:
        plan_key = next(
            key
            for key in parser.spooled_uhc_plan_keys(spool_snapshot)
            if key.family == "cs"
        )
        materialized = parser.load_spooled_uhc_plan(
            spool_snapshot,
            plan_key,
            source_id=UHC_FORMULARY_SOURCE_ID,
            canonical_base=UHC_FORMULARY_CANONICAL_BASE,
            evidence=evidence,
        )

    medication = materialized.medications[0]
    provenance_rows = medication.raw_extensions[0]["provenance"]
    assert medication.drug_tier == "New tier"
    assert any(provenance_row["selected"] for provenance_row in provenance_rows)
    assert any(not provenance_row["selected"] for provenance_row in provenance_rows)
