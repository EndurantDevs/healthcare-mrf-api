# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Tagged published reads and explicit independent-scope refusal."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_ptg_cohort_authority as authority
from process import registry_ptg_producer_scope as producer
from process import registry_ptg_scope_engine as engine
from process.ptg_parts import result_archive_published_authority as published
from process.ptg_parts.result_archive_published_identity import (
    validate_published_result_identity,
)
from tests import test_registry_ptg_cohort_authority as fixture
from tests import test_registry_ptg_scope_engine as scope_fixture
from tests.test_result_archive_published_identity import _published_row


def _case(**changes):
    snapshot_by_field = _published_row()
    source_by_field = fixture._source() | {
        "identity_sha256": (b"r" * 32).hex(),
        "raw_container_sha256": (b"r" * 32).hex(),
    }
    snapshot_by_field["source_assignments"] = [source_by_field]
    specification = fixture._specification(
        snapshot_id=snapshot_by_field["snapshot_id"],
        binding_source_key=snapshot_by_field["run_source_key"],
        **changes,
    )
    published_authority = published._authority_from_identity(
        authority._source_specification(specification),
        validate_published_result_identity(snapshot_by_field),
    )
    graph_by_field = {
        "snapshot_key": published_authority.identity["snapshot_key"],
        "layout_generation": "shared_blocks_v4",
        "layout_mapping_sha256": published_authority.identity["layout_mapping_digest"],
        "map_sha256": published_authority.identity["map_digest"],
        "finalizer_map_sha256": published_authority.identity["finalizer_map_digest"],
        "source_assignments_sha256": published_authority.identity["source_assignments_sha256"],
    }
    return specification, published_authority, source_by_field, graph_by_field


def _session():
    return fixture._session(*[fixture._Result(scalar="repeatable read") for _ in range(4)])


@pytest.mark.asyncio
async def test_published_authority_keeps_tag_and_verifies_exact_pin(monkeypatch):
    specification, published_authority, _, _ = _case()
    clone = AsyncMock(return_value=published_authority)
    prepare = AsyncMock()
    monkeypatch.setattr(authority, "lock_ptg_published_result_for_clone", clone)
    monkeypatch.setattr(authority, "prepare_ptg_result_archive_source_authority", prepare)
    session = _session()
    assert (
        await authority._require_frozen_source(session, specification, published_authority.as_dict())
        == published_authority.as_dict()
    )
    clone.assert_awaited_once_with(session, schema_name="synthetic_ptg", authority=published_authority.as_dict())
    prepare.assert_not_awaited()
    assert "source_file_import_id" not in published_authority.as_dict()
    assert "frozen_binding_sha256" not in published_authority.as_dict()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["operation_id", "snapshot_id", "source_key", "pin", "identity", "extra"])
async def test_published_receipt_substitution_refuses_before_clone(monkeypatch, field):
    specification, published_authority, _, _ = _case()
    receipt = published_authority.as_dict()
    receipt[field] = "changed"
    clone = AsyncMock()
    monkeypatch.setattr(authority, "lock_ptg_published_result_for_clone", clone)
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="source_changed"):
        await authority._require_frozen_source(_session(), specification, receipt)
    clone.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("coordinate", ["capture_id", "snapshot_id", "binding_source_key"])
async def test_published_receipt_cannot_move_to_another_capture(monkeypatch, coordinate):
    specification, published_authority, _, _ = _case()
    setattr(
        specification,
        coordinate,
        scope_fixture.OTHER if coordinate == "capture_id" else "other",
    )
    clone = AsyncMock()
    monkeypatch.setattr(authority, "lock_ptg_published_result_for_clone", clone)
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="source_changed"):
        await authority._require_frozen_source(_session(), specification, published_authority.as_dict())
    clone.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_or_changed_pin_propagates_refusal(monkeypatch):
    specification, published_authority, _, _ = _case()
    monkeypatch.setattr(
        authority,
        "lock_ptg_published_result_for_clone",
        AsyncMock(side_effect=published.PtgPublishedResultSourceAuthorityError("synthetic pin changed")),
    )
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="source_changed"):
        await authority._require_frozen_source(_session(), specification, published_authority.as_dict())


@pytest.mark.asyncio
async def test_registry_capture_and_release_use_existing_owned_pin_apis(monkeypatch):
    specification, published_authority, _, _ = _case()
    prepare = AsyncMock(return_value=published_authority)
    commit = AsyncMock(return_value=published_authority)
    release = AsyncMock(return_value=1)
    monkeypatch.setattr(authority, "prepare_ptg_result_archive_source_authority", prepare)
    monkeypatch.setattr(authority, "commit_ptg_result_archive_source_authority", commit)
    monkeypatch.setattr(authority, "release_ptg_result_archive_source_authority", release)
    session = _session()
    receipt = await authority.capture_registry_ptg_source(session, specification)
    assert receipt == published_authority.as_dict()
    prepare.assert_awaited_once_with(
        session,
        schema_name="synthetic_ptg",
        operation_id=authority._source_specification(specification),
        snapshot_id=specification.snapshot_id,
    )
    commit.assert_awaited_once_with(session, schema_name="synthetic_ptg", authority=receipt)
    assert await authority.release_registry_ptg_source(session, specification, authority=receipt) == 1
    release.assert_awaited_once_with(session, schema_name="synthetic_ptg", authority=receipt)
    specification.capture_id = scope_fixture.OTHER
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="source_changed"):
        await authority.release_registry_ptg_source(session, specification, authority=receipt)
    assert release.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "assignments", "count", "root", "local"])
async def test_published_graph_binds_full_source_vector_and_roots(monkeypatch, drift):
    specification, published_authority, assignment_by_field, graph_by_field = _case()
    identity_by_field = dict(published_authority.identity)
    if drift == "assignments":
        identity_by_field["source_assignments_sha256"] = "f" * 64
    elif drift == "count":
        identity_by_field["source_count"] = 2
    elif drift == "root":
        identity_by_field["finalizer_map_digest"] = "f" * 64
    changed = published.PtgPublishedResultSourceAuthority(published_authority.operation_id, identity_by_field)
    source_by_field = {**graph_by_field, "binding_payload": None}
    binding = (
        SimpleNamespace(schema_name="synthetic_ptg", payload_snapshot_id=specification.snapshot_id)
        if drift == "local"
        else None
    )
    monkeypatch.setattr(
        authority,
        "_resolved_source_state",
        AsyncMock(return_value=(source_by_field, [assignment_by_field], binding)),
    )
    prepare = AsyncMock(return_value=changed)
    clone = AsyncMock(return_value=changed)
    projection = AsyncMock()
    monkeypatch.setattr(authority, "prepare_ptg_published_result_source_authority", prepare)
    monkeypatch.setattr(authority, "lock_ptg_published_result_for_clone", clone)
    monkeypatch.setattr(authority, "_validate_frozen_assignments", projection)
    if drift:
        with pytest.raises(authority.RegistryPTGCohortAuthorityError):
            await authority._source_state(_session(), specification, graph_by_field)
        projection.assert_not_awaited()
    else:
        assert await authority._source_state(_session(), specification, graph_by_field) == (
            graph_by_field,
            [assignment_by_field],
        )
        projection.assert_awaited_once()
        assert prepare.await_args.kwargs["snapshot_id"] == specification.snapshot_id
        assert clone.await_args.kwargs["authority"] == published_authority.as_dict()


@pytest.mark.asyncio
async def test_optional_frozen_join_still_selects_one_explicit_snapshot(monkeypatch):
    specification, _, source_by_field, _ = _case()
    monkeypatch.setattr(authority, "_physical_binding", AsyncMock(return_value=None))
    monkeypatch.setattr(authority, "_source_assignments", AsyncMock(return_value=[source_by_field]))
    session = fixture._session(fixture._Result(rows=[{"binding_payload": None}]))
    await authority._resolved_source_state(session, specification)
    query, parameters = session.execute.await_args.args
    assert "LEFT JOIN" in str(query) and "ptg2_frozen_source_file_binding" in str(query)
    assert "WHERE snapshot.snapshot_id=:snapshot_id" in str(query)
    assert parameters == {"snapshot_id": specification.snapshot_id}
    assert "current" not in str(query)


@pytest.mark.asyncio
async def test_published_assignments_keep_sealed_occurrence_proof(monkeypatch):
    specification, _, source_by_field, _ = _case()
    snapshot_by_field = {"binding_payload": None, "layout_manifest": {}, "snapshot_key": 17}
    frozen = Mock()
    projection = AsyncMock(return_value=(None, [source_by_field]))
    identities = Mock()
    monkeypatch.setattr(authority, "validate_frozen_candidate_evidence", frozen)
    monkeypatch.setattr(authority, "_validate_tax_identity_source_projection_state", projection)
    monkeypatch.setattr(authority, "_validate_reused_binding_identities", identities)
    monkeypatch.setattr(
        authority,
        "_tax_identity_source_seal_metadata",
        lambda _: ({"sealed": True}, {"sealed": True}),
    )
    session = _session()
    await authority._validate_frozen_assignments(
        session, '"synthetic_ptg"', specification.snapshot_id, snapshot_by_field, [source_by_field]
    )
    frozen.assert_not_called()
    session.execute.assert_not_awaited()
    projection.assert_awaited_once()
    identities.assert_called_once_with([source_by_field], expected_bindings=[source_by_field])
    monkeypatch.setattr(authority, "_tax_identity_source_seal_metadata", lambda _: None)
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="source_unavailable"):
        await authority._validate_frozen_assignments(
            session, '"synthetic_ptg"', specification.snapshot_id, snapshot_by_field, [source_by_field]
        )


@pytest.mark.asyncio
async def test_published_source_never_confers_producer_scope(monkeypatch):
    specification, published_authority, _, graph_by_field = _case()
    monkeypatch.setattr(authority, "_require_frozen_source", AsyncMock(return_value=published_authority.as_dict()))
    with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="published_scope_unavailable"):
        await authority.require_registry_ptg_cohort_authority(
            _session(),
            specification,
            frozen_authority=published_authority.as_dict(),
            graph_identity=graph_by_field,
            office_assertion_table_oid=42,
        )
    monkeypatch.setattr(producer, "_require_frozen_source", AsyncMock(return_value=published_authority.as_dict()))
    source_state = AsyncMock()
    monkeypatch.setattr(producer, "_source_state", source_state)
    with pytest.raises(producer.RegistryPTGProducerScopeError, match="published_scope_unavailable"):
        await producer._evidence(_session(), specification, {}, published_authority.as_dict(), graph_by_field)
    source_state.assert_not_awaited()
    monkeypatch.setattr(
        engine,
        "prepare_ptg_result_archive_source_authority",
        AsyncMock(return_value=published_authority),
    )
    evidence = AsyncMock()
    monkeypatch.setattr(engine, "_evidence", evidence)
    with pytest.raises(producer.RegistryPTGProducerScopeError, match="published_scope_unavailable"):
        await engine._resolved_source(
            _session(),
            "synthetic_ptg",
            scope_fixture._intent(),
            scope_fixture._ownership(),
            None,
        )
    evidence.assert_not_awaited()
