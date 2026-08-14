# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import itertools
import datetime as dt
import json
import os

import pytest

import process.formulary_fhir.uhc_drug_spool as spool
from process.formulary_fhir.source_artifact_contract import artifact_set_sha256
from process.formulary_fhir.async_safety import CooperativeThreadCancellation
from process.formulary_fhir.uhc_drug_normalization import (
    normalized_uhc_drug_memberships,
)
from tests.uhc_drug_parser_test_support import artifact_set
from tests.uhc_drug_parser_test_support import install_artifact_reader
from tests.uhc_drug_parser_test_support import source_record


def _materialize(
    monkeypatch,
    tmp_path,
    *,
    records_by_artifact=None,
    timestamps_by_artifact=None,
):
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
    return artifacts, evidence, spool_path


def test_spool_streams_exact_set_and_merges_identical_provenance(
    monkeypatch,
    tmp_path,
) -> None:
    _artifacts, evidence, spool_path = _materialize(monkeypatch, tmp_path)

    assert spool_path.stat().st_mode & 0o777 == 0o600
    assert evidence.file_count == 48
    assert evidence.raw_record_count == 48
    assert evidence.raw_plan_entry_count == 48
    assert evidence.plan_count == 2
    assert evidence.medication_membership_count == 2
    assert evidence.duplicate_count == 46
    assert evidence.superseded_count == 0
    assert evidence.max_last_updated_at == dt.datetime(
        2026,
        8,
        10,
        tzinfo=dt.UTC,
    )


def test_spool_streams_every_selected_artifact_and_reports_partial_coverage(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, bodies = artifact_set()
    selected = artifacts.artifacts[1:]
    partial_artifacts = type(artifacts)(
        source_id=artifacts.source_id,
        source_file_set_sha256=artifacts.source_file_set_sha256,
        raw_listing_projection_sha256=artifacts.raw_listing_projection_sha256,
        artifacts=selected,
        artifact_set_sha256=artifact_set_sha256(selected),
    )
    install_artifact_reader(monkeypatch, spool, bodies)

    evidence = spool.materialize_uhc_drug_spool(
        partial_artifacts,
        spool_path=tmp_path / "partial.sqlite",
    )

    assert evidence.file_count == 47
    assert evidence.expected_file_count == 48
    assert evidence.excluded_file_count == 1
    assert evidence.is_coverage_complete is False


def test_spool_expands_years_and_preserves_unknown_fields(
    monkeypatch,
    tmp_path,
) -> None:
    record = source_record(
        years=[2026, 2027],
        record_note="retained",
        plan_extension={"mail_order": True},
    )
    _artifacts, evidence, _spool_path = _materialize(
        monkeypatch,
        tmp_path,
        records_by_artifact={index: [record] for index in range(48)},
    )

    assert evidence.plan_count == 4
    assert evidence.medication_membership_count == 4
    assert evidence.raw_plan_entry_count == 48


def test_spool_uses_strictly_newer_catalog_observation_for_conflict(
    monkeypatch,
    tmp_path,
) -> None:
    records_by_artifact = {0: [source_record(tier="Old tier")]}
    records_by_artifact.update(
        {index: [source_record(tier="New tier")] for index in range(1, 24)}
    )
    timestamps_by_artifact = {0: "2026-08-09T00:00:00Z"}
    timestamps_by_artifact.update(
        {index: "2026-08-10T00:00:00Z" for index in range(1, 24)}
    )
    _artifacts, evidence, _spool_path = _materialize(
        monkeypatch,
        tmp_path,
        records_by_artifact=records_by_artifact,
        timestamps_by_artifact=timestamps_by_artifact,
    )

    assert evidence.superseded_count >= 1
    assert evidence.medication_membership_count == 2


def test_spool_rejects_equal_time_content_conflict(monkeypatch, tmp_path) -> None:
    artifacts, bodies = artifact_set(
        {
            0: [source_record(tier="First tier")],
            1: [source_record(tier="Different tier")],
        }
    )
    install_artifact_reader(monkeypatch, spool, bodies)

    with pytest.raises(
        spool.UHCDrugNormalizationError,
        match="equal-time content conflict",
    ):
        spool.materialize_uhc_drug_spool(
            artifacts,
            spool_path=tmp_path / "conflict.sqlite",
        )


def test_spool_rejects_symlinked_destination_before_creation(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, bodies = artifact_set()
    install_artifact_reader(monkeypatch, spool, bodies)
    actual_directory = tmp_path / "actual"
    actual_directory.mkdir(mode=0o700)
    os.chmod(actual_directory, 0o700)
    alias_directory = tmp_path / "alias"
    alias_directory.symlink_to(actual_directory, target_is_directory=True)
    spool_path = alias_directory / "spool.sqlite"

    with pytest.raises(spool.UHCDrugNormalizationError, match="path is invalid"):
        spool.materialize_uhc_drug_spool(
            artifacts,
            spool_path=spool_path,
        )

    assert not (actual_directory / "spool.sqlite").exists()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row.update({"rxnorm_id": "not-numeric"}),
        lambda row: row.update({"plans": []}),
        lambda row: row["plans"][0].update({"prior_authorization": 1}),
        lambda row: row["plans"][0].update({"years": []}),
        lambda row: row["plans"][0].update({"plan_id": " padded "}),
        lambda row: row["plans"][0].update({"plan_id_type": "invalid type"}),
    ],
)
def test_spool_rejects_noncontract_source_shapes(
    monkeypatch,
    tmp_path,
    mutation,
) -> None:
    invalid_record = source_record()
    mutation(invalid_record)
    artifacts, bodies = artifact_set({0: [invalid_record]})
    install_artifact_reader(monkeypatch, spool, bodies)

    with pytest.raises(spool.UHCDrugNormalizationError):
        spool.materialize_uhc_drug_spool(
            artifacts,
            spool_path=tmp_path / "invalid.sqlite",
        )


def test_normalizer_rejects_membership_expansion_before_object_fanout() -> None:
    artifacts, _bodies_by_name = artifact_set()
    oversized_record = source_record()
    plan_entry = oversized_record["plans"][0]
    plan_entry["years"] = list(range(2000, 2101))
    oversized_record["plans"] = [dict(plan_entry) for _index in range(991)]

    with pytest.raises(
        spool.UHCDrugNormalizationError,
        match="expanded membership collection is too large",
    ):
        normalized_uhc_drug_memberships(
            oversized_record,
            artifacts.artifacts[0],
            1,
        )


def test_spool_rejects_duplicate_keys_in_retained_bytes(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, bodies_by_name = artifact_set()
    first_name = artifacts.artifacts[0].identity.file_name
    valid_body = bodies_by_name[first_name]
    bodies_by_name[first_name] = valid_body.replace(
        b'"rxnorm_id":"1234567"',
        b'"rxnorm_id":"1234567","rxnorm_id":"7654321"',
    )
    install_artifact_reader(monkeypatch, spool, bodies_by_name)

    with pytest.raises(spool.UHCDrugNormalizationError, match="retained JSON"):
        spool.materialize_uhc_drug_spool(
            artifacts,
            spool_path=tmp_path / "duplicate-key.sqlite",
        )


def test_spool_rejects_unmodeled_precision_loss(monkeypatch, tmp_path) -> None:
    artifacts, bodies_by_name = artifact_set()
    first_name = artifacts.artifacts[0].identity.file_name
    precise_record = source_record(record_note="PRECISE_NUMBER")
    precise_body = json.dumps(
        [precise_record],
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    bodies_by_name[first_name] = precise_body.replace(
        b'"PRECISE_NUMBER"',
        b"0.123456789012345678901234567890",
    )
    install_artifact_reader(monkeypatch, spool, bodies_by_name)

    with pytest.raises(spool.UHCDrugNormalizationError, match="extension is invalid"):
        spool.materialize_uhc_drug_spool(
            artifacts,
            spool_path=tmp_path / "precise-number.sqlite",
        )


def test_spool_cancellation_removes_partial_private_database(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, bodies_by_name = artifact_set()
    install_artifact_reader(monkeypatch, spool, bodies_by_name)
    spool_path = tmp_path / "cancelled.sqlite"
    check_indexes = itertools.count(1)

    def cancel_check() -> None:
        """Cancel after enough cooperative checkpoints to create the spool."""

        if next(check_indexes) >= 12:
            raise CooperativeThreadCancellation("cancelled")

    with pytest.raises(CooperativeThreadCancellation):
        spool.materialize_uhc_drug_spool(
            artifacts,
            spool_path=spool_path,
            cancel_check=cancel_check,
        )

    assert not spool_path.exists()
