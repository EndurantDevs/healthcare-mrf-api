from __future__ import annotations

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
