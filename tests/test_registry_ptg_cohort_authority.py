# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen source and native witness-page gates never confer producer authority."""

import hashlib
import struct
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_cohort_authority as authority
from process.ptg_parts.result_archive_source_authority import PtgResultArchiveSourceAuthority


@pytest.fixture(autouse=True)
def _canonical_payload(monkeypatch):
    monkeypatch.setattr(authority, "_physical_binding", AsyncMock(return_value=None))


def _specification(**changes):
    specification_by_field = {
        "capture_id": "11111111-1111-4111-8111-111111111111",
        "ptg_schema_name": "synthetic_ptg",
        "snapshot_id": "synthetic-snapshot",
        "binding_source_key": "synthetic-binding",
        "company_key": "synthetic-company",
        "cohort_id": "synthetic-cohort",
        "scope_assertion_id": "synthetic-assertion",
        "scope_assertion_sha256": "a" * 64,
        "office_evidence_kind": "reviewed_exact_office",
        "expected_rows": 2,
    }
    return SimpleNamespace(**(specification_by_field | changes))


def _frozen_source():
    return PtgResultArchiveSourceAuthority(
        authority._specification(_specification()),
        "synthetic-snapshot",
        "synthetic-import",
        "synthetic-logical-source",
        "a" * 64,
        "b" * 64,
    )


class _Result:
    def __init__(self, *, rows=(), scalar=None):
        self.rows = rows
        self.value = scalar

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)

    def one_or_none(self):
        assert len(self.rows) <= 1
        return self.rows[0] if self.rows else None

    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]

    def scalar_one(self):
        return self.value


def _session(*results, transaction=True):
    return SimpleNamespace(
        in_transaction=lambda: transaction,
        execute=AsyncMock(side_effect=results),
        info={},
    )


@pytest.mark.asyncio
async def test_no_supplied_scope_receipt_can_admit_a_cohort(monkeypatch):
    prepared = AsyncMock(return_value=_frozen_source())
    monkeypatch.setattr(authority, "prepare_ptg_result_archive_source_authority", prepared)
    specification = _specification()
    specification.scope_assertion = {"contract": "registry_ptg_cohort_scope.v1", "admitted": True}
    specification.scope_authority = True
    specification.scope_resolver = AsyncMock(return_value=specification.scope_assertion)
    session = _session(_Result(scalar="repeatable read"))
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_scope_unavailable"):
        await authority.require_registry_ptg_cohort_authority(
            session,
            specification,
            frozen_authority=_frozen_source().as_dict(),
            graph_identity={},
            office_assertion_table_oid=42,
        )
    prepared.assert_awaited_once_with(
        session,
        schema_name="synthetic_ptg",
        operation_id=authority._specification(specification),
        snapshot_id="synthetic-snapshot",
    )
    specification.scope_resolver.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["read committed", "read uncommitted"])
async def test_source_corroboration_requires_one_stable_snapshot(monkeypatch, isolation):
    prepared = AsyncMock()
    monkeypatch.setattr(authority, "prepare_ptg_result_archive_source_authority", prepared)
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_transaction_required"):
        await authority._require_frozen_source(
            _session(_Result(scalar=isolation)),
            _specification(),
            _frozen_source().as_dict(),
        )
    prepared.assert_not_awaited()


@pytest.mark.asyncio
async def test_receipt_cannot_relabel_source_authority(monkeypatch):
    ordinary = SimpleNamespace(as_dict=lambda: {"contract": "ptg_published_result_source_authority.v1"})
    prepared = AsyncMock(return_value=ordinary)
    monkeypatch.setattr(authority, "prepare_ptg_result_archive_source_authority", prepared)
    for supplied, error in (
        (_frozen_source().as_dict(), "registry_ptg_authority_kind_unsupported"),
        (ordinary.as_dict(), "registry_ptg_source_changed"),
    ):
        with pytest.raises(authority.RegistryPTGCohortAuthorityError, match=error):
            await authority._require_frozen_source(
                _session(_Result(scalar="serializable")),
                _specification(),
                supplied,
            )
    assert prepared.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["snapshot_manifest_sha256", "frozen_binding_sha256", "source_key", "pin"])
async def test_frozen_receipt_is_compared_to_actual_source(monkeypatch, field):
    monkeypatch.setattr(
        authority, "prepare_ptg_result_archive_source_authority", AsyncMock(return_value=_frozen_source())
    )
    supplied = _frozen_source().as_dict()
    supplied[field] = "changed"
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_source_changed"):
        await authority._require_frozen_source(
            _session(_Result(scalar="repeatable read")),
            _specification(),
            supplied,
        )


@pytest.mark.parametrize("selected", [(), [0], (True,), (1, 0), (0, 0), (-1,), (2,)])
def test_selected_sources_are_exact_bounded_dense_coordinates(selected):
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_scope_changed"):
        authority._selected_sources(selected, 2)
    assert authority._selected_sources((0, 1), 2) == (0, 1)


def _source():
    return {
        "source_key": 0,
        "source_type": "in_network",
        "identity_kind": "raw_container_sha256_v1",
        "identity_sha256": "a" * 64,
        "raw_container_sha256": "a" * 64,
        "logical_json_sha256": None,
        "logical_hash_deferred": True,
        "source_trace_set_hash": "b" * 64,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [_source() | {"source_key": 1}], [_source(), _source()]])
async def test_source_assignments_require_a_complete_unique_dense_vector(rows):
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_source_changed"):
        await authority._source_assignments(_session(_Result(rows=rows)), '"synthetic_ptg"', "synthetic-snapshot")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [{"snapshot_key": True}, {"map_sha256": "f" * 64}, {"unknown": 0}])
async def test_graph_identity_is_bound_to_actual_sealed_layout(monkeypatch, change):
    source_rows = [_source()]
    source_by_field = {
        "snapshot_key": 31,
        "layout_generation": "shared_blocks_v4",
        "layout_mapping_sha256": "c" * 64,
        "map_sha256": "c" * 64,
        "finalizer_map_sha256": "d" * 64,
    }
    graph = source_by_field | {
        "source_assignments_sha256": hashlib.sha256(authority._canonical(source_rows)).hexdigest()
    }
    monkeypatch.setattr(authority, "_source_assignments", AsyncMock(return_value=source_rows))
    validate = AsyncMock()
    monkeypatch.setattr(authority, "_validate_frozen_assignments", validate)
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_source_changed"):
        await authority._source_state(_session(_Result(rows=[source_by_field])), _specification(), graph | change)
    validate.assert_not_awaited()


def _page(*, after=0, count=2):
    return {
        "row_count": count,
        "ordinal_count": count,
        "first_ordinal": after + 1 if count else None,
        "last_ordinal": after + count if count else None,
        "mismatch_count": 0,
        "scope_mismatch_count": 0,
        "edge_count": 1 if count else 0,
        "selected_edges": struct.pack(">II", 4, 9) if count else b"",
    }


@pytest.mark.parametrize(
    "change",
    [
        {"first_ordinal": 2},
        {"last_ordinal": 3},
        {"ordinal_count": 1},
        {"row_count": True},
        {"row_count": 4097},
        {"selected_edges": b"short"},
        {"edge_count": 3},
    ],
)
def test_native_page_accounting_rejects_gaps_duplicates_and_bad_frames(change):
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_accounting_invalid"):
        authority._checked_page(_page() | change, 0)


def test_unresolved_source_occurrence_or_dictionary_fails_the_page():
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_provider_unresolved"):
        authority._checked_page(_page() | {"mismatch_count": 1}, 0)
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_scope_changed"):
        authority._checked_page(_page() | {"scope_mismatch_count": 1}, 0)
    assert authority._checked_page(_page(), 0) == (2, struct.pack(">II", 4, 9))
    assert authority._checked_page(_page(count=0), 2) == (0, b"")


@pytest.mark.asyncio
@pytest.mark.parametrize("census", [(2, 2, 0, 1), (3, 3, 0, 2), (2, 1, 1, 2), (1, 1, 1, 1)])
async def test_full_landing_census_cannot_hide_nonpositive_or_extra_ordinals(census):
    session = _session(_Result(), _Result(scalar=True), _Result(rows=[census]))
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_accounting_invalid"):
        await authority._office_relation(session, _specification(), 42)


@pytest.mark.asyncio
async def test_stream_exhaustion_requires_exact_census_and_advancing_pages(monkeypatch):
    graph_by_field = {"snapshot_key": 31}
    monkeypatch.setattr(authority, "_require_frozen_source", AsyncMock())
    monkeypatch.setattr(authority, "_source_state", AsyncMock(return_value=(graph_by_field, [_source()])))
    monkeypatch.setattr(authority, "_office_relation", AsyncMock(return_value='"synthetic_capture".office_assertion'))
    session = _session(
        _Result(rows=[_page(count=1)]), _Result(rows=[_page(after=1, count=1)]), _Result(rows=[_page(count=0)])
    )
    pages = [
        page
        async for page in authority.read_registry_ptg_source_witness_pages(
            session,
            _specification(),
            frozen_authority={},
            graph_identity=graph_by_field,
            office_assertion_table_oid=42,
            selected_dense_source_keys=(0,),
        )
    ]
    assert [(page["after_ordinal"], page["last_ordinal"], page["row_count"]) for page in pages] == [
        (0, 1, 1),
        (1, 2, 1),
    ]
    assert [call.args[1]["after"] for call in session.execute.await_args_list] == [0, 1, 2]
    assert all(
        "source_record_ordinal=page.source_record_ordinal" in str(call.args[0])
        for call in session.execute.await_args_list
    )
    short_session = _session(_Result(rows=[_page(count=1)]), _Result(rows=[_page(count=0)]))
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_accounting_invalid"):
        short_pages = [
            page
            async for page in authority.read_registry_ptg_source_witness_pages(
                short_session,
                _specification(),
                frozen_authority={},
                graph_identity=graph_by_field,
                office_assertion_table_oid=42,
                selected_dense_source_keys=(0,),
            )
        ]


@pytest.mark.asyncio
async def test_detached_candidate_context_cannot_substitute_for_sealed_source():
    session = _session()
    session.info["ptg_snapshot_candidate_reads"] = {"synthetic_ptg": {"source": "different_source"}}
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="registry_ptg_source_unavailable"):
        await authority._source_state(session, _specification(), {})
    session.execute.assert_not_awaited()
