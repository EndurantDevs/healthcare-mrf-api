from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_audit as audit


def _candidate(
    *,
    provider_set_key: int = 2,
    provider_count: int = 4,
    candidate_ordinal: int = 0,
) -> audit.AuditCandidate:
    return audit.AuditCandidate(
        1,
        provider_set_key,
        3,
        0,
        provider_count,
        candidate_ordinal=candidate_ordinal,
    )


def _sealed_metadata():
    return {
        "contract": audit.PTG2_V3_AUDIT_CONTRACT,
        "format_version": 2,
        "method": audit.PTG2_V3_AUDIT_METHOD,
        "serving_multiplicity_semantics": (
            audit.PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS
        ),
        "sample_count": 1,
        "sample_digest": "a" * 64,
    }


def test_candidate_path_rejects_output_directory_escape(tmp_path):
    output_directory = tmp_path / "output"
    output_directory.mkdir()

    with pytest.raises(RuntimeError, match="escapes its output directory"):
        audit._candidate_path(
            {"output_directory": str(output_directory)},
            {"path": "../outside.bin"},
        )


def test_candidate_file_rejects_summary_size_mismatch(tmp_path):
    candidate_path = tmp_path / "audit-candidates.bin"
    candidate_path.write_bytes(b"")
    finalizer_summary_by_field = {
        "output_directory": str(tmp_path),
        "source_count": 1,
        "dense_keys": {
            "code": {"count": 2},
            "provider_set": {"count": 3},
            "price": {"count": 4},
        },
        "audit_candidates": {
            "path": candidate_path.name,
            "record_format": "ptg2_v3_audit_candidates_v2",
            "format_version": 2,
            "record_bytes": 20,
            "row_count": 1,
            "maximum_rows": 4096,
            "selection_method": "equal_interval_assigned_rows_v1",
            "source_row_count": 1,
            "row_digest": "0" * 64,
        },
    }

    with pytest.raises(RuntimeError, match="file size"):
        audit.load_audit_candidates(finalizer_summary_by_field)


@pytest.mark.asyncio
async def test_candidate_provider_counts_reject_conflicting_candidates():
    candidates = (
        _candidate(provider_count=4),
        _candidate(provider_count=5, candidate_ordinal=1),
    )

    with pytest.raises(RuntimeError, match="disagree on one provider-set count"):
        await audit._validate_candidate_provider_counts(
            AsyncMock(),
            schema_name="mrf",
            snapshot_key=1,
            candidates=candidates,
        )


@pytest.mark.asyncio
async def test_candidate_provider_counts_accept_empty_selection():
    session = AsyncMock()

    await audit._validate_candidate_provider_counts(
        session,
        schema_name="mrf",
        snapshot_key=1,
        candidates=(),
    )

    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_provider_counts_require_database_match():
    session = AsyncMock()
    session.execute.return_value = ()

    with pytest.raises(RuntimeError, match="disagree with PostgreSQL"):
        await audit._validate_candidate_provider_counts(
            session,
            schema_name="mrf",
            snapshot_key=1,
            candidates=(_candidate(),),
        )


@pytest.mark.asyncio
async def test_snapshot_source_dictionary_requires_positive_count():
    with pytest.raises(RuntimeError, match="positive source_count"):
        await audit._validate_snapshot_source_dictionary(
            AsyncMock(),
            schema_name="mrf",
            logical_snapshot_id="snapshot",
            source_count=0,
            required_source_keys=(),
        )


@pytest.mark.asyncio
async def test_snapshot_source_dictionary_requires_dense_database_rows():
    session = AsyncMock()
    session.execute.return_value = ()

    with pytest.raises(RuntimeError, match="not complete and dense"):
        await audit._validate_snapshot_source_dictionary(
            session,
            schema_name="mrf",
            logical_snapshot_id="snapshot",
            source_count=1,
            required_source_keys=(),
        )


@pytest.mark.asyncio
async def test_snapshot_source_dictionary_requires_occurrence_sources():
    session = AsyncMock()
    session.execute.return_value = ({"source_key": 0},)

    with pytest.raises(RuntimeError, match="source keys are absent"):
        await audit._validate_snapshot_source_dictionary(
            session,
            schema_name="mrf",
            logical_snapshot_id="snapshot",
            source_count=1,
            required_source_keys=(1,),
        )


def test_candidate_npis_skip_missing_graph_selection():
    candidate_npis_by_ordinal = audit._candidate_npis_from_selections(
        (_candidate(),),
        selections_per_candidate=1,
        group_key_by_selection={},
        npi_ordinal_by_selection={},
        npi_by_ordinal_by_group={},
    )

    assert candidate_npis_by_ordinal == {0: ()}


@pytest.mark.asyncio
async def test_price_memberships_require_every_requested_price():
    reader = AsyncMock()
    reader.logical_blocks.return_value = {}

    with pytest.raises(RuntimeError, match="membership is missing or empty"):
        await audit._price_memberships(
            reader,
            price_keys=(3,),
            atom_key_bits=1,
            block_span=16,
        )


def test_zero_row_limit():
    assert audit.build_audit_occurrences(
        candidates=(_candidate(),),
        provider_npis={0: (1_234_567_890,)},
        price_memberships={3: (4,)},
        core_layout_id=b"\x01" * 32,
        budget=audit._ReadBudget(),
        maximum_rows=0,
    ) == ()


def test_occurrence_collision_rejected(monkeypatch):
    candidate = _candidate()
    original = audit._occurrence(
        b"\x02" * 32,
        candidate,
        1_234_567_890,
        0,
        4,
    )
    collision = replace(original, npi=1_234_567_891)
    monkeypatch.setattr(audit, "_occurrence", lambda *_args: collision)

    with pytest.raises(RuntimeError, match="hash collision"):
        audit._store_audit_occurrence(
            {original.occurrence_id: original},
            core_layout_id=b"\x02" * 32,
            candidate=candidate,
            npi=collision.npi,
            atom_ordinal=collision.atom_ordinal,
            atom_key=collision.atom_key,
            budget=audit._ReadBudget(),
            row_limit=2,
        )


@pytest.mark.parametrize(
    ("mapping_digest", "core_support_digest", "message"),
    (
        (b"\x03" * 32, b"\x04" * 31, "core support digest"),
        (b"\x03" * 31, b"\x04" * 32, "mapping digest"),
    ),
)
@pytest.mark.asyncio
async def test_publication_rejects_invalid_digests(
    mapping_digest,
    core_support_digest,
    message,
):
    with pytest.raises(ValueError, match=message):
        await audit.publish_shared_audit_sample(
            schema_name="mrf",
            build_ownership=None,
            logical_snapshot_id="snapshot",
            finalizer_summary={},
            mapping_digest=mapping_digest,
            core_support_digest=core_support_digest,
            atom_key_bits=1,
            price_membership_block_span=1,
        )


def test_layout_requires_audit_metadata():
    with pytest.raises(RuntimeError, match="audit sample contract"):
        audit._audit_metadata_from_layout({})


@pytest.mark.parametrize(
    "invalid_field_by_name",
    (
        {"contract": "invalid"},
        {"sample_count": audit.PTG2_V3_AUDIT_MAX_SAMPLE_ROWS + 1},
        {"sample_digest": "invalid"},
    ),
)
def test_sealed_contract_rejects_invalid_metadata(invalid_field_by_name):
    metadata = _sealed_metadata()
    metadata.update(invalid_field_by_name)

    with pytest.raises(RuntimeError, match="reused strict V3 layout"):
        audit._validated_sealed_audit_contract(metadata)
