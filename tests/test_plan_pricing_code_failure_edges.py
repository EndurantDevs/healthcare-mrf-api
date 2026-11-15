# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Malformed and stale capabilities refuse reads and preparation before writes."""

import copy
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from api import plan_pricing_code_build as build
from api import plan_pricing_code_read as read
from api import plan_pricing_code_storage as storage
from api.plan_pricing_code_contract import (
    capability_identity,
    sealed_read_digest,
    validate_params,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_plan_pricing_code_projection import (
    _capability,
    _identity,
    _selection,
    _source,
)


@pytest.mark.parametrize("change", [{"expected_identity_digest": "A" * 64}, {"code": " 27447"}])
def test_worker_rejects_noncanonical_expectation(change):
    payload = {
        "plan_release_id": _selection().plan_release_id,
        "serving_revision_id": _selection().serving_revision_id,
        "code_system": "CPT",
        "code": "27447",
        "expected_identity_digest": "b" * 64,
    }
    with pytest.raises(ValueError):
        validate_params(payload | change)


@pytest.mark.parametrize("representative", [True, 1, -1])
def test_capability_rejects_unproven_physical_representative(representative):
    source = _source() | {"materialized_binding_ordinal": representative}
    with pytest.raises(ValueError, match="representative"):
        capability_identity(_selection(), ("CPT", "27447"), "b" * 64, [source])


def test_alias_cannot_claim_another_physical_read_or_incomplete_source():
    first = _selection().bindings[0]
    selection = replace(_selection(), bindings=(first, replace(first, binding_ordinal=1)))
    alias = _source() | {"binding_ordinal": 1, "read_identity_digest": "f" * 64}
    with pytest.raises(ValueError, match="aliased physical read"):
        capability_identity(selection, ("CPT", "27447"), "b" * 64, [_source(), alias])
    source = _source()
    del source["membership"]
    with pytest.raises(ValueError, match="source identity is incomplete"):
        capability_identity(_selection(), ("CPT", "27447"), "b" * 64, [source])


def test_capability_refuses_invalid_provider_and_oversized_metadata():
    with pytest.raises(ValueError, match="provider identity"):
        capability_identity(_selection(), ("CPT", "27447"), "invalid", [_source()])
    source = _source()
    source["source_identity"]["oversized"] = "x" * (512 * 1024)
    with pytest.raises(ValueError, match="metadata bound"):
        capability_identity(_selection(), ("CPT", "27447"), "b" * 64, [source])


@pytest.mark.parametrize("untrusted", [{"snapshot": "synthetic"}, [1, 2], 1.5])
def test_sealed_read_digest_rejects_unauthenticated_container_shapes(untrusted):
    with pytest.raises(ValueError, match="sealed read identity"):
        sealed_read_digest((UUID(int=1), untrusted))


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 17])
async def test_binding_bound_refuses_before_source_reads_or_writes(monkeypatch, count):
    session = SimpleNamespace(execute=AsyncMock(), commit=AsyncMock())
    source = AsyncMock()
    monkeypatch.setattr(build, "binding_source", source)
    selected = replace(
        _selection(),
        bindings=tuple(replace(_selection().bindings[0], binding_ordinal=i) for i in range(count)),
    )
    with pytest.raises(ValueError, match="binding count"):
        await build.resolve_inputs(session, selected, ("CPT", "27447"))
    source.assert_not_awaited()
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_membership_refuses_before_code_scan_or_provider_generation(monkeypatch):
    monkeypatch.setattr(build, "binding_source", AsyncMock(return_value=(object(), ("sealed",))))
    monkeypatch.setattr(build, "source_identity", AsyncMock(return_value=(None, None, {})))
    monkeypatch.setattr(build, "locate_membership", AsyncMock(return_value=None))
    codes, providers = AsyncMock(), AsyncMock()
    monkeypatch.setattr(build, "load_sealed_code_rows", codes)
    monkeypatch.setattr(build, "provider_signature", providers)
    with pytest.raises(build.CodeProjectionMembershipNotReady, match="ready complete"):
        await build.resolve_inputs(object(), _selection(), ("CPT", "27447"))
    codes.assert_not_awaited()
    providers.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_retention_pin_never_creates_one_during_read():
    session = SimpleNamespace(scalar=AsyncMock(return_value=False), execute=AsyncMock(), commit=AsyncMock())
    with pytest.raises(PTG2ManifestArtifactError, match="retention pin is missing"):
        await storage.source_pins(session, _identity())
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_absent_named_attachment_does_not_query_storage(monkeypatch):
    family = AsyncMock()
    monkeypatch.setattr(storage, "_storage", family)
    session = SimpleNamespace(scalar=AsyncMock(return_value=False), execute=AsyncMock(), commit=AsyncMock())
    assert await storage.locate(session, _identity()) is None
    family.assert_not_awaited()
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["invalid_digest", "provider", "source"])
async def test_stale_storage_never_authorizes_ready_access(monkeypatch, drift):
    identity = _identity()
    stored = copy.deepcopy(identity)
    if drift == "provider":
        stored["provider_signature"] = "c" * 64
    elif drift == "source":
        stored["sources"][0]["source_identity"]["snapshot_id"] = "different-snapshot"
    header_map = {
        "identity": stored,
        "content_digest": "invalid" if drift == "invalid_digest" else "d" * 64,
        "counts": {},
    }
    monkeypatch.setattr(storage, "_storage", AsyncMock(return_value=SimpleNamespace(schema_name="synthetic_archive")))
    pins = AsyncMock()
    monkeypatch.setattr(storage, "source_pins", pins)
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=True),
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: header_map))),
        commit=AsyncMock(),
    )
    if drift == "provider":
        assert await storage.locate(session, identity, allow_stale_provider=True) is None
        pins.assert_awaited_once_with(session, stored)
    else:
        with pytest.raises(PTG2ManifestArtifactError, match="sealed identity"):
            await storage.locate(session, identity, allow_stale_provider=True)
        pins.assert_not_awaited()
    assert all(str(call.args[0]).startswith("SELECT") for call in session.execute.await_args_list)
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_ready", [{"identity": {"contract": "unknown"}}, {"identity": _identity(), "projection_id": "f" * 64}]
)
async def test_retirement_refuses_forged_identity_before_any_mutation(monkeypatch, bad_ready):
    session = SimpleNamespace(execute=AsyncMock(), commit=AsyncMock())
    with pytest.raises(PTG2ManifestArtifactError, match="retirement identity"):
        await storage.retire_unreferenced(session, bad_ready)
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("found", [None, ("synthetic", {"identity": _identity()})])
async def test_absent_capability_keeps_ordinary_routing_and_stored_identity_serves(monkeypatch, found):
    session = SimpleNamespace(scalar=AsyncMock(), execute=AsyncMock(), commit=AsyncMock())
    monkeypatch.setattr(read, "locate", AsyncMock(return_value=found))
    capability = await read.resolve_code_pricing(session, _selection(), {"code_system": "CPT", "code": "27447"})
    if found is None:
        assert capability is None
    else:
        assert capability.identity == _identity() and capability.schema_name == "synthetic"
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()


def _worker_boundary(monkeypatch, outcome):
    """Record worker fencing and transaction exit without native database effects."""
    from api import plan_release_serving_resolution as resolution

    selected = _selection()
    events = []
    session = SimpleNamespace(commit=AsyncMock(), execute=AsyncMock())

    @asynccontextmanager
    async def transaction():
        events.append("enter")
        try:
            yield session
        except Exception as error:
            events.append(("exit", type(error).__name__))
            raise
        else:
            events.append(("exit", None))

    async def fence(actual):
        assert actual is session
        events.append("fence")
        if outcome == "lock_error":
            raise RuntimeError("synthetic provider fence failure")

    async def resolve(actual, release):
        assert actual is session and release == selected.plan_release_id
        events.append("resolve")
        if outcome == "unavailable":
            return resolution.PlanReleaseServingResolution(resolution.PLAN_RELEASE_RESOLUTION_UNAVAILABLE, None)
        if outcome == "missing":
            return SimpleNamespace(state=resolution.PLAN_RELEASE_RESOLUTION_READY, selection=None)
        current = replace(selected, serving_revision_id="superseded-revision") if outcome == "superseded" else selected
        return resolution.PlanReleaseServingResolution(resolution.PLAN_RELEASE_RESOLUTION_READY, current)

    async def prepare(actual, **preparation_by_field):
        assert actual is session and preparation_by_field["selection"] is selected
        events.append("prepare")
        if outcome == "build_error":
            raise RuntimeError("synthetic preparation failure")
        return {"projection_id": "a" * 64}

    begin = Mock(side_effect=transaction)
    builder = AsyncMock(side_effect=prepare)
    monkeypatch.setattr(build.db, "transaction", begin)
    monkeypatch.setattr(build, "lock_provider_generation", AsyncMock(side_effect=fence))
    monkeypatch.setattr(resolution, "resolve_plan_release_serving_resolution", AsyncMock(side_effect=resolve))
    monkeypatch.setattr(build, "build_in_session", builder)
    return SimpleNamespace(selected=selected, events=events, session=session, begin=begin, builder=builder)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["ready", "unavailable", "missing", "superseded", "lock_error", "build_error"])
async def test_worker_fences_release_and_closes_transaction_on_every_outcome(monkeypatch, outcome):
    """Keep every worker outcome fenced and close its caller-owned transaction."""
    boundary = _worker_boundary(monkeypatch, outcome)
    selected, events, session = boundary.selected, boundary.events, boundary.session
    begin, builder = boundary.begin, boundary.builder
    parameters_by_name = {
        "plan_release_id": selected.plan_release_id,
        "serving_revision_id": selected.serving_revision_id,
        "code_system": "CPT",
        "code": "27447",
        "expected_identity_digest": "b" * 64,
    }
    if outcome == "ready":
        assert await build.build_plan_pricing_code_projection(**parameters_by_name) == {"projection_id": "a" * 64}
        assert events == ["enter", "fence", "resolve", "prepare", ("exit", None)]
    else:
        error_type = RuntimeError if outcome in {"lock_error", "build_error"} else ValueError
        with pytest.raises(error_type, match="synthetic|exact current published revision"):
            await build.build_plan_pricing_code_projection(**parameters_by_name)
        expected_events = ["enter", "fence"]
        if outcome != "lock_error":
            expected_events.append("resolve")
        if outcome == "build_error":
            expected_events.append("prepare")
        assert events == expected_events + [("exit", error_type.__name__)]
    begin.assert_called_once_with()
    if outcome in {"ready", "build_error"}:
        builder.assert_awaited_once_with(
            session,
            selection=selected,
            code_system="CPT",
            code="27447",
            expected_identity_digest="b" * 64,
            scope=None,
            audit={},
            phase_timings={},
        )
        assert builder.await_args.kwargs["audit"] is not builder.await_args.kwargs["phase_timings"]
    else:
        builder.assert_not_awaited()
    session.commit.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_invalid_selectors_never_open_transaction(monkeypatch):
    begin = Mock()
    monkeypatch.setattr(build.db, "transaction", begin)
    with pytest.raises(ValueError, match="expected identity digest"):
        await build.build_plan_pricing_code_projection(
            plan_release_id=_selection().plan_release_id,
            serving_revision_id=_selection().serving_revision_id,
            code_system="CPT",
            code="27447",
            expected_identity_digest="invalid",
        )
    begin.assert_not_called()


@pytest.mark.asyncio
async def test_build_refuses_noncanonical_code_before_resolving_or_writing(monkeypatch):
    resolve = AsyncMock()
    monkeypatch.setattr(build, "resolve_inputs", resolve)
    session = SimpleNamespace(execute=AsyncMock(), commit=AsyncMock())
    with pytest.raises(ValueError, match="canonical procedure"):
        await build.build_in_session(
            session, selection=_selection(), code_system="cpt", code="27447", expected_identity_digest="b" * 64
        )
    resolve.assert_not_awaited()
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()
    with pytest.raises(ValueError, match="canonical procedure"):
        capability_identity(_selection(), ("cpt", "27447"), "b" * 64, [_source()])


@pytest.mark.asyncio
@pytest.mark.parametrize("binding_count", [1, 2])
async def test_occurrence_overflow_refuses_before_provider_reads_or_writes(monkeypatch, binding_count):
    limit = build.MAX_CODE_OCCURRENCES
    count = limit + 1 if binding_count == 1 else limit // 2 + 1
    bindings = tuple(replace(_selection().bindings[0], binding_ordinal=i) for i in range(binding_count))

    async def source(_session, params):
        return object(), ("sealed-read", params["ordinal"])

    monkeypatch.setattr(build, "binding_source", source)
    monkeypatch.setattr(build, "source_identity", AsyncMock(return_value=(None, None, {"snapshot_id": "synthetic"})))
    monkeypatch.setattr(build, "locate_membership", AsyncMock(return_value=(object(),)))
    monkeypatch.setattr(build, "load_sealed_code_rows", AsyncMock(return_value=[{"code_key": 1, "rate_count": count}]))
    member = _source()["membership"] | {
        "projection_id": "a" * 64,
        "content_digest": "b" * 64,
        "storage": _source()["membership"]["storage"],
        "code_coverage": [{"code_key": 1, "occurrence_count": count}],
    }
    monkeypatch.setattr(build, "membership_receipt", lambda _found: member)
    providers = AsyncMock()
    monkeypatch.setattr(build, "provider_signature", providers)
    session = SimpleNamespace(execute=AsyncMock(), commit=AsyncMock())
    with pytest.raises(ValueError, match="occurrence count exceeds"):
        await build.resolve_inputs(session, replace(_selection(), bindings=bindings), ("CPT", "27447"))
    providers.assert_not_awaited()
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_attachment_read_denial_is_not_treated_as_absence(monkeypatch):
    family = AsyncMock()
    monkeypatch.setattr(storage, "_storage", family)
    session = SimpleNamespace(scalar=AsyncMock(side_effect=PermissionError("native read denied")))
    with pytest.raises(PermissionError, match="native read denied"):
        await storage.locate(session, _identity())
    family.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("selected,args", [(None, {"code_system": "CPT", "code": "27447"}), (_selection(), {})])
async def test_ordinary_routing_without_release_or_code_never_queries_attachment(selected, args):
    session = SimpleNamespace(scalar=AsyncMock(), execute=AsyncMock())
    assert await read.resolve_code_pricing(session, selected, args) is None
    session.scalar.assert_not_awaited()
    session.execute.assert_not_awaited()
    capability = _capability()
    assert capability.contributors(None) is capability


def _storage_capture():
    return storage.native.ReferenceFamilyStageOwnership(
        importer_id=storage.SPEC.importer_id,
        dataset_id=UUID(int=1),
        schema_name="reference_family_archive_" + UUID(int=1).hex,
        schema_oid=11,
        relation_oids=tuple((name, 300 + index) for index, name in enumerate(storage.SPEC.table_names)),
    )


@pytest.mark.asyncio
async def test_storage_reuses_named_common_family_and_membership_contract(monkeypatch):
    ownership = _storage_capture()
    shared = AsyncMock(return_value=ownership)
    monkeypatch.setattr(storage.attachments, "attachment_storage", shared)
    session, view = object(), "hp_snapshot_retention." + storage.attachment_name(_identity())
    assert await storage._storage(session, view, _identity()) is ownership
    shared.assert_awaited_once_with(session, view, storage.SPEC, membership_names=("pm_" + "a" * 52,))


@pytest.mark.asyncio
async def test_storage_preserves_common_layout_refusal_without_fallback(monkeypatch):
    shared = AsyncMock(side_effect=PTG2ManifestArtifactError("attachment dependencies differ"))
    monkeypatch.setattr(storage.attachments, "attachment_storage", shared)
    with pytest.raises(PTG2ManifestArtifactError, match="dependencies differ"):
        await storage._storage(object(), "hp_snapshot_retention." + storage.attachment_name(_identity()), _identity())


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["schema_name", "relation_name", "identity", "digest"])
async def test_retirement_drift_preserves_storage_and_source_pins(monkeypatch, drift):
    """Retirement cannot remove attachments or source pins from stale evidence."""
    from process.ptg_parts import ptg2_lifecycle_lock

    ownership = _storage_capture()
    identity = _identity()
    ready = storage.receipt((ownership, {"identity": identity, "content_digest": "d" * 64, "counts": {}}, 42, 900))
    header_map = {"identity": copy.deepcopy(identity), "content_digest": ready["content_digest"]}
    captured = ownership
    if drift == "schema_name":
        captured = replace(ownership, schema_name="different_schema")
    elif drift == "relation_name":
        captured = replace(ownership, relation_oids=ownership.relation_oids[:-1])
    elif drift == "identity":
        header_map["identity"]["code"] = "99213"
    elif drift == "digest":
        header_map["content_digest"] = "e" * 64
    monkeypatch.setattr(ptg2_lifecycle_lock, "acquire_ptg2_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(storage, "lock_identity", AsyncMock())
    monkeypatch.setattr(storage.native, "capture_model_family_stage_ownership", AsyncMock(return_value=captured))
    references, cleanup, lock = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(storage, "_unreferenced_source_schema", references)
    monkeypatch.setattr(storage.native, "cleanup_model_family_stage", cleanup)
    monkeypatch.setattr(storage.native, "_lock_family", lock)
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: header_map))),
        scalar=AsyncMock(),
        commit=AsyncMock(),
    )
    with pytest.raises(
        PTG2ManifestArtifactError, match="retirement storage identity changed|retirement sealed identity differs"
    ):
        await storage.retire_unreferenced(session, ready)
    references.assert_not_awaited()
    cleanup.assert_not_awaited()
    lock.assert_not_awaited()
    session.scalar.assert_not_awaited()
    session.commit.assert_not_awaited()
    assert session.execute.await_count == (1 if drift in {"identity", "digest"} else 0)
    assert all(str(call.args[0]).startswith("SELECT") for call in session.execute.await_args_list)


def _retained_legacy_identity():
    """Keep original token bytes available for retained pin accounting only."""
    identity = copy.deepcopy(_identity())
    member = identity["sources"][0]["membership"]
    member["active_attachment"].pop("schema_name")
    member["active_attachment"]["relation_oid"] = None
    original = member["storage"]
    member["storage"] = {
        "dataset_id": original["dataset_id"],
        "schema_oid": False,
        "relation_oids": [[name, None] for name in original["relation_names"]],
        "owner_oid": True,
    }
    return identity


@pytest.mark.asyncio
async def test_named_locate_preserves_original_retained_pin_identity_and_emits_names(monkeypatch):
    stored_identity = _retained_legacy_identity()
    header_by_field = {"identity": stored_identity, "content_digest": "d" * 64, "counts": {}}
    original = copy.deepcopy(header_by_field)
    ownership = _storage_capture()
    monkeypatch.setattr(storage, "_storage", AsyncMock(return_value=ownership))
    pins = AsyncMock()
    monkeypatch.setattr(storage, "source_pins", pins)
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=True),
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: header_by_field))),
    )
    found = await storage.locate(session, _identity())
    pins.assert_awaited_once_with(session, stored_identity)
    ready = storage.receipt(found)
    assert ready["identity"] == _identity()
    assert ready["active_attachment"] == {
        "schema_name": "hp_snapshot_retention",
        "name": storage.attachment_name(_identity()),
    }
    assert ready["storage"] == {
        "dataset_id": str(ownership.dataset_id),
        "schema_name": ownership.schema_name,
        "relation_names": list(ownership.relation_names),
    }
    assert header_by_field == original


@pytest.mark.asyncio
async def test_named_retirement_removes_only_original_retained_pin_digest(monkeypatch):
    from process.ptg_parts import ptg2_lifecycle_lock

    stored_identity = _retained_legacy_identity()
    header_by_field = {"identity": stored_identity, "content_digest": "d" * 64, "counts": {}}
    ownership = _storage_capture()
    ready = storage.receipt(
        (ownership, header_by_field, None, "hp_snapshot_retention." + storage.attachment_name(_identity()))
    )
    monkeypatch.setattr(ptg2_lifecycle_lock, "acquire_ptg2_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(storage, "lock_identity", AsyncMock())
    monkeypatch.setattr(storage.native, "capture_model_family_stage_ownership", AsyncMock(return_value=ownership))
    references = AsyncMock(return_value='"mrf"')
    monkeypatch.setattr(storage, "_unreferenced_source_schema", references)
    monkeypatch.setattr(storage.native, "_lock_family", AsyncMock())
    cleanup = AsyncMock()
    monkeypatch.setattr(storage.native, "cleanup_model_family_stage", cleanup)
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=False),
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: header_by_field))),
    )
    assert await storage.retire_unreferenced(session, ready)
    references.assert_awaited_once_with(session, stored_identity)
    deletes = [call for call in session.execute.await_args_list if str(call.args[0]).startswith("DELETE")]
    assert len(deletes) == 1 and deletes[0].args[1]["owner_id"] == storage.identity_digest(stored_identity)
    assert deletes[0].args[1]["owner_id"] != ready["projection_id"]
    cleanup.assert_awaited_once_with(session, storage.SPEC, ownership)
