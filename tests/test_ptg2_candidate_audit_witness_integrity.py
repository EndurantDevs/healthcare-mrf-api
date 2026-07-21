from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_candidate_audit_integrity as integrity
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_ptg2_candidate_audit_batch_integrity import (
    _valid_source_scope_tables,
)


@pytest.mark.asyncio
async def test_sealed_witness_reads_validates_and_groups_one_payload(monkeypatch):
    challenge = AuditBatchChallenge(
        code_system="CPT",
        code="99213",
        npi=1234567890,
        source_artifact_key=0,
        tuple_digest="b" * 64,
        network_name_digests=(),
        multiplicity=1,
    )
    provider_record = object()
    occurrence_record = object()
    condition = object()
    loaded_witness = SimpleNamespace(
        provider_records=(provider_record,),
        occurrence_records=(occurrence_record,),
        records=(SimpleNamespace(linked_provider_sha256="provider"),),
        evidence_by_sha256={"evidence": {}},
    )
    decoder = Mock(return_value=loaded_witness)
    provider_validator = Mock()
    condition_builder = Mock(return_value=condition)
    challenge_grouper = Mock(return_value=(challenge,))
    payload_result = (
        {"part_number": 0, "payload": b"sealed"},
        {"part_number": 1, "payload": b"part", "part_sha256": b"x" * 32},
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=payload_result))
    assembler = Mock(return_value=b"sealed")
    monkeypatch.setattr(integrity, "assemble_source_witness_payload", assembler)
    monkeypatch.setattr(integrity, "decode_persisted_source_witness", decoder)
    monkeypatch.setattr(integrity, "validate_provider_witness", provider_validator)
    monkeypatch.setattr(integrity, "source_audit_condition", condition_builder)
    monkeypatch.setattr(integrity, "group_audit_batch_challenges", challenge_grouper)

    scope = await integrity._sealed_witness_challenges(
        session,
        _valid_source_scope_tables(),
        ("a" * 64,),
    )

    assert scope.challenges == (challenge,)
    assert scope.record_count == 1
    assert scope.unique_evidence_count == 1
    assert scope.evidence_reference_count == 2
    provider_validator.assert_called_once_with(
        provider_record,
        parsed_evidence_by_sha256=loaded_witness.evidence_by_sha256,
    )
    condition_builder.assert_called_once_with(
        occurrence_record,
        parsed_evidence_by_sha256=loaded_witness.evidence_by_sha256,
    )
    challenge_grouper.assert_called_once_with(("a" * 64,), (condition,))
    assembler.assert_called_once()


@pytest.mark.asyncio
async def test_sealed_witness_accepts_iterable_result_without_provider_records(
    monkeypatch,
):
    loaded_witness = SimpleNamespace(
        provider_records=(),
        occurrence_records=(),
        records=(SimpleNamespace(linked_provider_sha256=None),),
        evidence_by_sha256={},
    )
    session = SimpleNamespace(
        execute=AsyncMock(return_value=({"payload": b"sealed"},))
    )
    monkeypatch.setattr(
        integrity,
        "decode_persisted_source_witness",
        Mock(return_value=loaded_witness),
    )
    monkeypatch.setattr(
        integrity,
        "assemble_source_witness_payload",
        Mock(return_value=b"sealed"),
    )
    monkeypatch.setattr(
        integrity,
        "group_audit_batch_challenges",
        Mock(return_value=()),
    )

    scope = await integrity._sealed_witness_challenges(
        session,
        _valid_source_scope_tables(),
        ("a" * 64,),
    )

    assert scope.challenges == ()
    assert scope.evidence_reference_count == 1


@pytest.mark.asyncio
async def test_sealed_witness_wraps_decode_failure(monkeypatch):
    session = SimpleNamespace(
        execute=AsyncMock(return_value=({"payload": b"invalid"},))
    )
    monkeypatch.setattr(
        integrity,
        "decode_persisted_source_witness",
        Mock(side_effect=ValueError("invalid witness")),
    )
    monkeypatch.setattr(
        integrity,
        "assemble_source_witness_payload",
        Mock(return_value=b"invalid"),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="persisted source witness"):
        await integrity._sealed_witness_challenges(
            session,
            _valid_source_scope_tables(),
            ("a" * 64,),
        )
