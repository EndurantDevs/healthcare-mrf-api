# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed tests for private formulary normalization spools."""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
import datetime as dt
import io
import json
import os
from pathlib import Path
import sqlite3

import pytest

from process.formulary_fhir.repository_shared import json_text
import process.formulary_fhir.uhc_drug_normalization as normalization
import process.formulary_fhir.uhc_drug_parser as parser
import process.formulary_fhir.uhc_drug_spool as spool
import process.formulary_fhir.uhc_drug_spool_contract as spool_contract
import process.formulary_fhir.uhc_drug_spool_merge as spool_merge
import process.formulary_fhir.uhc_drug_spool_reader as spool_reader
import process.formulary_fhir.uhc_drug_spool_storage as spool_storage
from tests.uhc_drug_parser_test_support import artifact_set
from tests.uhc_drug_parser_test_support import install_artifact_reader
from tests.uhc_drug_parser_test_support import source_record


def _prepared_spool(monkeypatch, tmp_path):
    tmp_path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(tmp_path, 0o700)
    artifacts, bodies_by_name = artifact_set()
    install_artifact_reader(monkeypatch, spool, bodies_by_name)
    spool_path = tmp_path / "drugs.sqlite"
    evidence = spool.materialize_uhc_drug_spool(
        artifacts,
        spool_path=spool_path,
    )
    return artifacts, spool_path, evidence


def _membership_row(spool_path):
    connection = sqlite3.connect(spool_path)
    connection.row_factory = sqlite3.Row
    try:
        return connection.execute(
            "SELECT * FROM membership ORDER BY source_plan_identifier LIMIT 1"
        ).fetchone()
    finally:
        connection.close()


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _Connection:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _sql, *_arguments):
        return _Cursor(self.rows)


def test_parser_cancellation_and_provenance_fail_closed(
    monkeypatch,
    tmp_path,
) -> None:
    _artifacts, spool_path, _evidence = _prepared_spool(monkeypatch, tmp_path)
    membership_row = _membership_row(spool_path)
    provenance_by_field = {
        "artifact_sha256": "a",
        "source_file_id": "b",
        "file_name": "c",
        "timestamp_basis": "d",
    }
    monkeypatch.setattr(
        parser,
        "validated_spool_provenance",
        lambda *_arguments, **_keywords: (provenance_by_field,),
    )
    with pytest.raises(RuntimeError, match="cancelled"):
        parser._plan_provenance(
            [membership_row] * 1_024,
            lambda: (_ for _ in ()).throw(RuntimeError("cancelled")),
        )

    invalid_provenance_by_field = dict(
        provenance_by_field,
        artifact_sha256=None,
    )
    monkeypatch.setattr(
        parser,
        "validated_spool_provenance",
        lambda *_arguments, **_keywords: (invalid_provenance_by_field,),
    )
    with pytest.raises(RuntimeError, match="provenance"):
        parser._plan_provenance([membership_row], None)


def test_parser_handles_yearless_plan_and_rejects_spool_semantic_drift(
    monkeypatch,
    tmp_path,
) -> None:
    _artifacts, spool_path, _evidence = _prepared_spool(monkeypatch, tmp_path)
    membership_by_field = dict(_membership_row(spool_path))
    key = spool_reader.spool_plan_key(membership_by_field)
    yearless_key = replace(
        key,
        plan_year=None,
        source_plan_identifier=spool_reader.uhc_drug_plan_alias(
            key.family,
            key.plan_id_type,
            key.plan_id,
            None,
        ),
    )
    assert parser._plan_period(yearless_key) == (None, None)

    semantic_by_field = json.loads(membership_by_field["semantic_json"])
    invalid_semantic_by_field = dict(
        membership_by_field,
        semantic_json=json_text({**semantic_by_field, "unexpected": True}),
    )
    with pytest.raises(RuntimeError, match="semantics are invalid"):
        parser._validated_medication_source(key, invalid_semantic_by_field)

    with monkeypatch.context() as local_patch:
        local_patch.setattr(
            parser,
            "validated_spool_provenance",
            lambda *_arguments, **_keywords: ({"timestamp_basis": "invalid"},),
        )
        with pytest.raises(RuntimeError, match="provenance"):
            parser._validated_medication_source(key, membership_by_field)

    with pytest.raises(RuntimeError, match="semantics are inconsistent"):
        parser._validated_medication_source(
            key,
            dict(membership_by_field, drug_name="different name"),
        )


def test_parser_materialization_guards_and_byte_accounting(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, spool_path, evidence = _prepared_spool(monkeypatch, tmp_path)
    row = _membership_row(spool_path)
    key = spool_reader.spool_plan_key(row)

    monkeypatch.setattr(parser, "_medication", lambda *_arguments: object())
    with pytest.raises(RuntimeError, match="cancelled"):
        parser._materialized_medications(
            key,
            [row] * 1_024,
            lambda: (_ for _ in ()).throw(RuntimeError("cancelled")),
        )
    with pytest.raises(ValueError, match="plan key"):
        parser._require_plan_materialization_request(
            object(),
            key,
            artifacts.source_id,
            evidence,
        )
    assert parser._database_row_bytes((b"abc",)) == 515


def test_spooled_plan_row_reader_cancels_and_rejects_empty_result(
    monkeypatch,
    tmp_path,
) -> None:
    _artifacts, spool_path, evidence = _prepared_spool(monkeypatch, tmp_path)
    row = _membership_row(spool_path)
    key = spool_reader.spool_plan_key(row)

    @contextmanager
    def many_rows(_spool):
        yield _Connection([row] * 1_024)

    monkeypatch.setattr(parser, "open_uhc_drug_spool", many_rows)
    with pytest.raises(RuntimeError, match="cancelled"):
        parser._spooled_plan_database_rows(
            object(),
            key,
            evidence,
            lambda: (_ for _ in ()).throw(RuntimeError("cancelled")),
        )

    @contextmanager
    def no_rows(_spool):
        yield _Connection([])

    monkeypatch.setattr(parser, "open_uhc_drug_spool", no_rows)
    with pytest.raises(RuntimeError, match="incomplete"):
        parser._spooled_plan_database_rows(object(), key, evidence, None)


def test_spool_hash_and_artifact_set_guards(monkeypatch) -> None:
    with pytest.raises(RuntimeError, match="metadata"):
        spool._spool_content_sha256(_Connection([]), None)

    class HashConnection:
        def execute(self, sql):
            if "spool_metadata" in sql:
                return _Cursor([("{}", "[]")])
            return _Cursor([("value",)] * 1_024)

    with pytest.raises(RuntimeError, match="cancelled"):
        spool._spool_content_sha256(
            HashConnection(),
            lambda: (_ for _ in ()).throw(RuntimeError("cancelled")),
        )

    with pytest.raises(normalization.UHCDrugNormalizationError, match="artifact set"):
        spool._validated_artifact_set(object())

    artifacts, _bodies = artifact_set()
    forged = deepcopy(artifacts)
    object.__setattr__(forged.artifacts[24].identity, "family", "cs")
    with pytest.raises(normalization.UHCDrugNormalizationError, match="census"):
        spool._validated_artifact_set(forged)


def test_spool_destination_and_empty_census_fail_before_writes() -> None:
    with pytest.raises(normalization.UHCDrugNormalizationError, match="path"):
        spool._validated_spool_destination(object())
    artifacts, _bodies = artifact_set()
    with pytest.raises(RuntimeError, match="timestamp census"):
        spool._evidence_payload(artifacts, spool._SpoolCensus(), 1, 1)


def test_artifact_record_census_and_spool_metadata_are_recomputed(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, _bodies = artifact_set()
    artifact = artifacts.artifacts[0]

    @contextmanager
    def open_bytes(_artifact):
        yield io.BytesIO(b"[{}]")

    monkeypatch.setattr(spool, "open_verified_source_artifact", open_bytes)
    monkeypatch.setattr(spool, "count_uhc_drug_stream_items", lambda *_a, **_k: 2)
    monkeypatch.setattr(spool, "_consume_source_records", lambda *_a, **_k: 1)
    connection = sqlite3.connect(":memory:")
    try:
        with pytest.raises(normalization.UHCDrugNormalizationError, match="census"):
            spool._consume_artifact(
                connection,
                artifact,
                spool._SpoolCensus(),
                None,
            )
    finally:
        connection.close()

    class CountConnection:
        def __init__(self):
            self.calls = 0

        def execute(self, _sql):
            self.calls += 1
            return _Cursor([(1,)])

    census = spool._SpoolCensus(
        raw_record_count=1,
        raw_plan_entry_count=1,
        maximum_updated_at=dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
    )
    monkeypatch.setattr(spool, "_install_spool_metadata", lambda *_a: {})
    monkeypatch.setattr(spool, "_spool_content_sha256", lambda *_a: "c" * 64)
    with pytest.raises(RuntimeError, match="metadata"):
        spool._spool_evidence(CountConnection(), artifacts, census, None)


def test_materializer_removes_destination_when_connect_fails(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts, _bodies = artifact_set()
    spool_path = tmp_path / "connect-failure.sqlite"
    monkeypatch.setattr(
        spool.sqlite3,
        "connect",
        lambda *_arguments: (_ for _ in ()).throw(RuntimeError("connect failed")),
    )
    with pytest.raises(RuntimeError, match="connect failed"):
        spool.materialize_uhc_drug_spool(artifacts, spool_path=spool_path)
    assert not spool_path.exists()


def test_spool_contract_helpers_require_exact_types() -> None:
    with pytest.raises(ValueError, match="artifact set"):
        spool_contract.artifact_proof_rows(object())
    with pytest.raises(ValueError, match="spool evidence"):
        spool_contract.spool_evidence_payload(object())


def test_provenance_merge_rejects_size_syntax_and_shape(monkeypatch) -> None:
    monkeypatch.setattr(spool_merge, "MAX_PROVENANCE_JSON_BYTES", 1)
    with pytest.raises(normalization.UHCDrugNormalizationError, match="too large"):
        spool_merge._merged_provenance("[]", "[]", selected_semantic_json="{}")

    monkeypatch.setattr(spool_merge, "MAX_PROVENANCE_JSON_BYTES", 1_000)
    for stored_json, incoming_json in (("[", "[]"), ("{}", "[]"), ("[1]", "[]")):
        with pytest.raises(RuntimeError, match="provenance"):
            spool_merge._merged_provenance(
                stored_json,
                incoming_json,
                selected_semantic_json="{}",
            )

    monkeypatch.setattr(spool_merge, "MAX_PROVENANCE_RECORDS_PER_MEMBERSHIP", 0)
    with pytest.raises(normalization.UHCDrugNormalizationError, match="too large"):
        spool_merge._merged_provenance("[{}]", "[]", selected_semantic_json="{}")

    monkeypatch.setattr(spool_merge, "MAX_PROVENANCE_RECORDS_PER_MEMBERSHIP", 10)
    monkeypatch.setattr(spool_merge, "MAX_PROVENANCE_JSON_BYTES", 4)
    with pytest.raises(normalization.UHCDrugNormalizationError, match="too large"):
        spool_merge._merged_provenance("[{}]", "[]", selected_semantic_json="{}")


def test_older_observation_only_updates_provenance() -> None:
    artifacts, _bodies = artifact_set()
    artifact = artifacts.artifacts[0]
    newer = normalization.normalized_uhc_drug_memberships(
        source_record(
            drug_name="newer",
            last_updated_on="2026-08-10T00:00:00Z",
        ),
        artifact,
        1,
    )[0]
    older = normalization.normalized_uhc_drug_memberships(
        source_record(
            drug_name="older",
            last_updated_on="2026-08-09T00:00:00Z",
        ),
        artifact,
        2,
    )[0]
    connection = sqlite3.connect(":memory:")
    try:
        spool._create_spool(connection)
        assert spool_merge.upsert_spool_membership(connection, newer) == (0, 0)
        assert spool_merge.upsert_spool_membership(connection, older) == (0, 1)
        stored_name = connection.execute(
            "SELECT drug_name FROM membership"
        ).fetchone()[0]
        assert stored_name == "newer"
    finally:
        connection.close()


def test_spool_reader_rejects_json_policy_timestamp_and_plan_drift(
    monkeypatch,
    tmp_path,
) -> None:
    with pytest.raises(RuntimeError, match="JSON"):
        spool_reader.decode_spool_json(object(), dict)
    with pytest.raises(RuntimeError, match="JSON"):
        spool_reader.decode_spool_json("{", dict)
    with pytest.raises(RuntimeError, match="canonical"):
        spool_reader.decode_spool_json('{"b":1, "a":2}', dict)
    assert spool_reader.spool_policy_value(None) is None
    for raw_timestamp in (
        object(),
        "not-a-timestamp",
        "2026-08-10T00:00:00",
        "2026-08-10T01:00:00+01:00",
    ):
        with pytest.raises(RuntimeError, match="timestamp"):
            spool_reader.spool_timestamp(raw_timestamp)

    _artifacts, spool_path, _evidence = _prepared_spool(monkeypatch, tmp_path)
    row = _membership_row(spool_path)
    monkeypatch.setattr(spool_reader, "uhc_drug_plan_alias", lambda *_a: "different")
    with pytest.raises(RuntimeError, match="plan identity"):
        spool_reader.spool_plan_key(row)


def test_spool_reader_rejects_provenance_shape_hash_and_selection(
    monkeypatch,
    tmp_path,
) -> None:
    _artifacts, spool_path, _evidence = _prepared_spool(monkeypatch, tmp_path)
    row = _membership_row(spool_path)
    valid = json.loads(row["provenance_json"])[0]
    invalid_values = (
        [{}],
        [{**valid, "artifact_sha256": "invalid"}],
        [{**valid, "family": "invalid"}],
        [{**valid, "selected": False}],
    )
    for invalid_provenance in invalid_values:
        with pytest.raises(RuntimeError, match="provenance"):
            spool_reader.validated_spool_provenance(
                json_text(invalid_provenance),
            )


def test_plan_key_census_cancels_and_rejects_empty_spool(
    monkeypatch,
    tmp_path,
) -> None:
    _artifacts, spool_path, _evidence = _prepared_spool(monkeypatch, tmp_path)
    row = _membership_row(spool_path)
    key = spool_reader.spool_plan_key(row)

    @contextmanager
    def many_rows(_spool):
        yield _Connection([row] * 1_024)

    monkeypatch.setattr(spool_reader, "open_uhc_drug_spool", many_rows)
    monkeypatch.setattr(spool_reader, "spool_plan_key", lambda _row: key)
    with pytest.raises(RuntimeError, match="cancelled"):
        spool_reader.spooled_uhc_plan_keys(
            object(),
            cancel_check=lambda: (_ for _ in ()).throw(RuntimeError("cancelled")),
        )

    @contextmanager
    def no_rows(_spool):
        yield _Connection([])

    monkeypatch.setattr(spool_reader, "open_uhc_drug_spool", no_rows)
    with pytest.raises(RuntimeError, match="plan census"):
        spool_reader.spooled_uhc_plan_keys(object())
