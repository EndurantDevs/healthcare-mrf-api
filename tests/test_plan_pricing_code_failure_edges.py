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
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
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
@pytest.mark.parametrize("attachment", [None, (7, 99, True, True), (7, 42, False, True), (7, 42, True, False)])
async def test_attachment_custody_refuses_before_storage_access(monkeypatch, attachment):
    monkeypatch.setattr(storage, "protected_owner", AsyncMock(return_value=42))
    family = AsyncMock()
    monkeypatch.setattr(storage, "_storage", family)
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(first=lambda: attachment)), commit=AsyncMock()
    )
    if attachment is None:
        assert await storage.locate(session, _identity()) is None
    else:
        with pytest.raises(PTG2ManifestArtifactError, match="attachment custody"):
            await storage.locate(session, _identity())
    family.assert_not_awaited()
    session.commit.assert_not_awaited()
    assert session.execute.await_count == 1


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
    monkeypatch.setattr(storage, "protected_owner", AsyncMock(return_value=42))
    monkeypatch.setattr(storage, "_require_no_untrusted_mutation", AsyncMock())
    monkeypatch.setattr(storage, "_storage", AsyncMock(return_value=SimpleNamespace(schema_name="synthetic_archive")))
    pins = AsyncMock()
    monkeypatch.setattr(storage, "source_pins", pins)
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(first=lambda: (7, 42, True, True)),
                SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: header_map)),
            ]
        ),
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
    publisher = AsyncMock()
    monkeypatch.setattr(storage.native, "protected_publisher_owner", publisher)
    session = SimpleNamespace(execute=AsyncMock(), commit=AsyncMock())
    with pytest.raises(PTG2ManifestArtifactError, match="retirement identity"):
        await storage.retire_unreferenced(session, bad_ready)
    publisher.assert_not_awaited()
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("found", [None, _capability().found])
async def test_attached_but_missing_capability_fails_closed(monkeypatch, found):
    session = SimpleNamespace(scalar=AsyncMock(return_value=True), execute=AsyncMock(), commit=AsyncMock())
    monkeypatch.setattr(build, "resolve_inputs", AsyncMock(return_value=(_identity(), [])))
    monkeypatch.setattr(read, "locate", AsyncMock(return_value=found))
    if found is None:
        with pytest.raises(PlanPricingProjectionUnavailable, match="capability is unavailable"):
            await read.resolve_code_pricing(session, _selection(), {"code_system": "CPT", "code": "27447"})
    else:
        capability = await read.resolve_code_pricing(session, _selection(), {"code_system": "CPT", "code": "27447"})
        assert capability.identity == _identity() and capability.found is found
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
            session, selection=selected, code_system="CPT", code="27447", expected_identity_digest="b" * 64
        )
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
        "storage": {"schema_oid": 2},
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
async def test_missing_protected_owner_never_queries_attachment(monkeypatch):
    monkeypatch.setattr(storage, "protected_owner", AsyncMock(return_value=None))
    session = SimpleNamespace(execute=AsyncMock(), commit=AsyncMock())
    assert await storage.locate(session, _identity()) is None
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()


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
@pytest.mark.parametrize("drift", ["dependency", "split_schema", "prefix", "uuid", "schema_oid", "relation_oid"])
async def test_storage_refuses_untrusted_catalog_capture(monkeypatch, drift):
    """Reject mismatched dependencies before custody acceptance or any mutation."""
    ownership = _storage_capture()
    namespace = {"prefix": "synthetic_namespace", "uuid": "reference_family_archive_invalid"}.get(
        drift, ownership.schema_name
    )
    dependencies = [(oid, 11, namespace, name) for name, oid in ownership.relation_oids]
    dependencies.append((1001, 12, "hp_snapshot_retention", "pm_" + "a" * 52))
    captured = ownership
    if drift == "dependency":
        dependencies.pop()
    elif drift == "split_schema":
        oid, _schema_oid, schema, name = dependencies[0]
        dependencies[0] = (oid, 99, schema, name)
    elif drift == "schema_oid":
        captured = replace(ownership, schema_oid=99)
    elif drift == "relation_oid":
        captured = replace(ownership, relation_oids=ownership.relation_oids[:-1])
    lock, capture, custody = AsyncMock(), AsyncMock(return_value=captured), AsyncMock()
    monkeypatch.setattr(storage.native, "_lock_family", lock)
    monkeypatch.setattr(storage.native, "capture_model_family_stage_ownership", capture)
    monkeypatch.setattr(storage, "_require_closed_local_custody", custody)
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: dependencies)), commit=AsyncMock()
    )
    with pytest.raises(
        PTG2ManifestArtifactError, match="dependencies differ|family differs|namespace differs|identity changed"
    ):
        await storage._storage(session, 900, 42, _identity())
    custody.assert_not_awaited()
    if drift in {"schema_oid", "relation_oid"}:
        lock.assert_awaited_once()
        capture.assert_awaited_once_with(session, storage.SPEC, ownership.dataset_id)
    else:
        lock.assert_not_awaited()
        capture.assert_not_awaited()
    assert session.execute.await_count == 1
    assert str(session.execute.await_args.args[0]).startswith("SELECT")
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["schema_oid", "relation_oid", "owner_oid", "identity", "digest"])
async def test_retirement_drift_preserves_storage_and_source_pins(monkeypatch, drift):
    """Retirement cannot remove attachments or source pins from stale evidence."""
    from process.ptg_parts import ptg2_lifecycle_lock

    ownership = _storage_capture()
    identity = _identity()
    ready = storage.receipt((ownership, {"identity": identity, "content_digest": "d" * 64, "counts": {}}, 42, 900))
    header_map = {"identity": copy.deepcopy(identity), "content_digest": ready["content_digest"]}
    captured = ownership
    if drift == "schema_oid":
        captured = replace(ownership, schema_oid=99)
    elif drift == "relation_oid":
        captured = replace(ownership, relation_oids=ownership.relation_oids[:-1])
    elif drift == "identity":
        header_map["identity"]["code"] = "99213"
    elif drift == "digest":
        header_map["content_digest"] = "e" * 64
    monkeypatch.setattr(
        storage.native, "protected_publisher_owner", AsyncMock(return_value=43 if drift == "owner_oid" else 42)
    )
    monkeypatch.setattr(ptg2_lifecycle_lock, "acquire_ptg2_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(storage, "lock_identity", AsyncMock())
    monkeypatch.setattr(storage.native, "capture_model_family_stage_ownership", AsyncMock(return_value=captured))
    monkeypatch.setattr(storage, "_require_closed_local_custody", AsyncMock())
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
