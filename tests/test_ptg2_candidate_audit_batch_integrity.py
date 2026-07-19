from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_integrity as integrity
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
    AuditBatchRequest,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_shared_audit import persisted_audit_sample_digest
from process.ptg_parts.ptg2_shared_source_set import (
    ordered_source_ordinal_digest,
)


def _audit_request() -> AuditBatchRequest:
    return AuditBatchRequest(
        snapshot_id="candidate-snapshot",
        source_key="source-a",
        plan_id="12-3456789",
        plan_market_type="group",
        audit_sample_digest="c" * 64,
        source_witness_sample_digest="d" * 64,
        source_witness_payload_sha256="e" * 64,
        ordered_source_ordinal_digest=ordered_source_ordinal_digest(("a" * 64,)),
        challenge_count=2,
        request_digest="f" * 64,
    )


def _persisted_audit_rows() -> list[dict]:
    coordinates_by_field = {
        "code_key": 7,
        "provider_set_key": 5,
        "price_key": 8,
        "source_key": 0,
        "npi": 1234567890,
        "atom_key": 9,
    }
    return [
        {
            **coordinates_by_field,
            "occurrence_id": b"a" * 32,
            "atom_ordinal": 0,
        },
        {
            **coordinates_by_field,
            "occurrence_id": b"b" * 32,
            "atom_ordinal": 1,
        },
    ]


class _ResultSession:
    def __init__(self, result_rows):
        self.result_rows = result_rows
        self.calls = []

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))
        return self.result_rows


def _sample_serving_tables(rows) -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        shared_snapshot_key=41,
        source_count=2,
        audit_sample={
            "sample_count": len(rows),
            "sample_digest": persisted_audit_sample_digest(rows),
        },
    )


def _source_scope_serving_tables(
    audit_request: AuditBatchRequest,
    source_set_by_field: dict,
) -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        shared_snapshot_key=41,
        source_count=1,
        audit_sample={
            "sample_count": 1,
            "sample_digest": audit_request.audit_sample_digest,
        },
        source_witness={
            "occurrence_witness_count": audit_request.challenge_count,
            "sample_digest": audit_request.source_witness_sample_digest,
            "payload_sha256": audit_request.source_witness_payload_sha256,
        },
        source_set=source_set_by_field,
    )


def _valid_source_scope_tables() -> PTG2ServingTables:
    audit_request = _audit_request()
    return _source_scope_serving_tables(
        audit_request,
        {"contract": "test", "source_count": 1},
    )


@pytest.mark.asyncio
async def test_persisted_audit_sample_is_read_and_validated_once():
    persisted_rows = _persisted_audit_rows()
    session = _ResultSession(persisted_rows)

    validated_rows = await integrity.validate_persisted_audit_sample(
        session,
        _sample_serving_tables(persisted_rows),
    )

    assert validated_rows == (
        integrity.PersistedAuditOccurrence(
            occurrence_id=b"a" * 32,
            code_key=7,
            provider_set_key=5,
            price_key=8,
            source_artifact_key=0,
            npi=1234567890,
            atom_ordinal=0,
            atom_key=9,
        ),
        integrity.PersistedAuditOccurrence(
            occurrence_id=b"b" * 32,
            code_key=7,
            provider_set_key=5,
            price_key=8,
            source_artifact_key=0,
            npi=1234567890,
            atom_ordinal=1,
            atom_key=9,
        ),
    )
    assert len(session.calls) == 1
    sql, params = session.calls[0]
    assert "mrf.ptg2_v3_audit_occurrence" in sql
    assert "ORDER BY occurrence_id" in sql
    assert params == {"shared_snapshot_key": 41}


@pytest.mark.asyncio
async def test_persisted_audit_sample_rejects_digest_mismatch():
    persisted_rows = _persisted_audit_rows()
    serving_tables = replace(
        _sample_serving_tables(persisted_rows),
        audit_sample={"sample_count": 2, "sample_digest": "0" * 64},
    )

    with pytest.raises(PTG2ManifestArtifactError, match="sample digest"):
        await integrity.validate_persisted_audit_sample(
            _ResultSession(persisted_rows),
            serving_tables,
        )


@pytest.mark.asyncio
async def test_persisted_audit_sample_rejects_duplicate_occurrence_ids():
    persisted_rows = _persisted_audit_rows()
    persisted_rows[1] = {
        **persisted_rows[1],
        "occurrence_id": persisted_rows[0]["occurrence_id"],
    }

    with pytest.raises(PTG2ManifestArtifactError, match="duplicate occurrence"):
        await integrity.validate_persisted_audit_sample(
            _ResultSession(persisted_rows),
            _sample_serving_tables(persisted_rows),
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "message"),
    [
        ("occurrence_id", b"short", "occurrence id"),
        ("source_key", 2, "source key"),
        ("npi", 999, "NPI"),
        ("atom_ordinal", -1, "coordinate"),
    ],
)
@pytest.mark.asyncio
async def test_persisted_audit_sample_rejects_invalid_coordinates(
    field_name,
    invalid_value,
    message,
):
    valid_rows = _persisted_audit_rows()
    invalid_rows = [dict(row) for row in valid_rows]
    invalid_rows[0][field_name] = invalid_value

    with pytest.raises(PTG2ManifestArtifactError, match=message):
        await integrity.validate_persisted_audit_sample(
            _ResultSession(invalid_rows),
            _sample_serving_tables(valid_rows),
        )


@pytest.mark.asyncio
async def test_candidate_scope_binds_exact_ordered_source_identity(monkeypatch):
    """Bind the candidate descriptor, persisted sample, and ordered sources once."""

    audit_request = _audit_request()
    source_set_by_field = {"contract": "test", "source_count": 1}
    persisted_occurrences = (
        integrity.PersistedAuditOccurrence(
            occurrence_id=b"a" * 32,
            code_key=7,
            provider_set_key=5,
            price_key=8,
            source_artifact_key=0,
            npi=1234567890,
            atom_ordinal=0,
            atom_key=9,
        ),
    )
    persisted_sample = AsyncMock(return_value=persisted_occurrences)
    source_identity = AsyncMock(
        return_value=(
            source_set_by_field,
            audit_request.ordered_source_ordinal_digest,
            ("a" * 64,),
        )
    )
    witness_scope = integrity.CandidateWitnessScope(
        challenges=(
            AuditBatchChallenge(
                code_system="CPT",
                code="99213",
                npi=1234567890,
                source_artifact_key=0,
                tuple_digest="b" * 64,
                network_name_digests=(),
                multiplicity=2,
            ),
        ),
        record_count=2,
        unique_evidence_count=1,
        evidence_reference_count=2,
        persisted_audit_occurrences=persisted_occurrences,
    )
    sealed_witness = AsyncMock(
        return_value=replace(witness_scope, persisted_audit_occurrences=())
    )
    monkeypatch.setattr(
        integrity,
        "validate_persisted_audit_sample",
        persisted_sample,
    )
    monkeypatch.setattr(
        integrity,
        "fetch_snapshot_source_set_identity",
        source_identity,
    )
    monkeypatch.setattr(
        integrity,
        "_sealed_witness_challenges",
        sealed_witness,
    )

    observed_scope = await integrity.validate_candidate_source_scope(
        object(),
        _source_scope_serving_tables(audit_request, source_set_by_field),
        audit_request,
    )

    persisted_sample.assert_awaited_once()
    source_identity.assert_awaited_once()
    sealed_witness.assert_awaited_once()
    assert observed_scope == witness_scope


@pytest.mark.asyncio
async def test_candidate_scope_rejects_swapped_source_ordinals(monkeypatch):
    audit_request = _audit_request()
    source_set_by_field = {"contract": "test", "source_count": 1}
    monkeypatch.setattr(
        integrity,
        "validate_persisted_audit_sample",
        AsyncMock(),
    )
    monkeypatch.setattr(
        integrity,
        "fetch_snapshot_source_set_identity",
        AsyncMock(
            return_value=(source_set_by_field, "0" * 64, ("a" * 64,))
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="source ordinals"):
        await integrity.validate_candidate_source_scope(
            object(),
            _source_scope_serving_tables(
                audit_request,
                source_set_by_field,
            ),
            audit_request,
        )


def test_integrity_record_fields_accepts_row_mappings_and_row_pairs():
    mapped_row = type("MappedRow", (), {"_mapping": {"value": 1}})()

    assert integrity._record_fields(mapped_row) == {"value": 1}
    assert integrity._record_fields((("value", 2),)) == {"value": 2}


@pytest.mark.parametrize(
    ("serving_tables", "message"),
    [
        (replace(_valid_source_scope_tables(), audit_sample=None), "audit sample"),
        (replace(_valid_source_scope_tables(), source_witness=None), "source witness"),
        (
            replace(
                _valid_source_scope_tables(),
                audit_sample={"sample_count": 1, "sample_digest": "0" * 64},
            ),
            "sealed audit sample",
        ),
        (
            replace(
                _valid_source_scope_tables(),
                source_witness={
                    "occurrence_witness_count": 3,
                    "sample_digest": "d" * 64,
                    "payload_sha256": "e" * 64,
                },
            ),
            "sealed source witness",
        ),
    ],
)
def test_request_scope_rejects_unsealed_or_mismatched_descriptors(
    serving_tables,
    message,
):
    with pytest.raises(PTG2ManifestArtifactError, match=message):
        integrity._validated_request_source_count(serving_tables, _audit_request())


@pytest.mark.parametrize(
    ("audit_sample", "rows", "message"),
    [
        (None, (), "persisted audit sample"),
        ({"sample_count": 0, "sample_digest": "0" * 64}, (), "count is invalid"),
        (
            {"sample_count": 3, "sample_digest": "0" * 64},
            _persisted_audit_rows(),
            "sealed sample count",
        ),
    ],
)
@pytest.mark.asyncio
async def test_persisted_sample_rejects_missing_or_invalid_counts(
    audit_sample,
    rows,
    message,
):
    serving_tables = replace(
        _sample_serving_tables(_persisted_audit_rows()),
        audit_sample=audit_sample,
    )
    with pytest.raises(PTG2ManifestArtifactError, match=message):
        await integrity.validate_persisted_audit_sample(
            _ResultSession(rows),
            serving_tables,
        )


@pytest.mark.asyncio
async def test_candidate_scope_wraps_source_identity_errors(monkeypatch):
    monkeypatch.setattr(
        integrity,
        "validate_persisted_audit_sample",
        AsyncMock(return_value=()),
    )
    monkeypatch.setattr(
        integrity,
        "fetch_snapshot_source_set_identity",
        AsyncMock(side_effect=integrity.PTG2SharedBlockError("invalid source")),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="invalid source"):
        await integrity.validate_candidate_source_scope(
            object(),
            _valid_source_scope_tables(),
            _audit_request(),
        )


@pytest.mark.asyncio
async def test_candidate_scope_rejects_source_set_and_challenge_count(monkeypatch):
    tables = _valid_source_scope_tables()
    audit_request = _audit_request()
    monkeypatch.setattr(
        integrity,
        "validate_persisted_audit_sample",
        AsyncMock(return_value=()),
    )
    source_identity = AsyncMock(
        return_value=({"other": True}, audit_request.ordered_source_ordinal_digest, ())
    )
    monkeypatch.setattr(integrity, "fetch_snapshot_source_set_identity", source_identity)

    with pytest.raises(PTG2ManifestArtifactError, match="source rows"):
        await integrity.validate_candidate_source_scope(object(), tables, audit_request)

    source_identity.return_value = (
        tables.source_set,
        audit_request.ordered_source_ordinal_digest,
        (),
    )
    monkeypatch.setattr(
        integrity,
        "_sealed_witness_challenges",
        AsyncMock(
            return_value=integrity.CandidateWitnessScope(
                challenges=(),
                record_count=0,
                unique_evidence_count=0,
                evidence_reference_count=0,
            )
        ),
    )
    with pytest.raises(PTG2ManifestArtifactError, match="challenge scope"):
        await integrity.validate_candidate_source_scope(object(), tables, audit_request)
