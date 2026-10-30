# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused ownership, scope, COPY, closure and failure checks for retained rows."""

import asyncio
import contextvars
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_ordinary_row_retention as retention


@pytest.fixture
def owner(monkeypatch):
    capture = SimpleNamespace(
        scope_payload={
            "owner_oid": 10,
            "schema_oid": 11,
            "affected_scope_sha256": "a" * 64,
            "retention_plan_sha256": "b" * 64,
        },
        affected_scope={"source_ids": ["source_one"]},
        predecessor={"kind": "published"},
        recheck=AsyncMock(),
    )
    admission = SimpleNamespace(
        plan=object(),
        captured_inputs=capture,
        profile_admission=object(),
        lease=SimpleNamespace(database_oid=12),
        _external_relations=set(),
        _relations={},
        assert_ready=AsyncMock(),
        before_logging=AsyncMock(),
        retire_external_relation=AsyncMock(),
    )
    database = SimpleNamespace(status=AsyncMock())
    fhir = SimpleNamespace(
        db=database,
        _schema=lambda: "fixture",
        _PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION=contextvars.ContextVar("fixture_profile", default=None),
    )
    prepared = retention.preparation.PreparedServingArtifacts(
        fhir, None, None, admission, None, SimpleNamespace(stages=(), profile_delta=None), {}, {}, None
    )
    prepared.retained_generation = None
    actor_by_field = {"owner": 10, "database_oid": 12, "backend": 13, "xid": "14"}
    origin = object(), SimpleNamespace(), actor_by_field
    monkeypatch.setattr(retention.receipts, "normalized_receipt_scope", lambda *_args: nullcontext())
    monkeypatch.setattr(retention, "require_capture", lambda *_args: capture)
    monkeypatch.setattr(retention, "_origin", AsyncMock(return_value=origin))
    monkeypatch.setattr(retention, "_same_origin", AsyncMock())
    monkeypatch.setattr(retention.capacity, "check_ordinary_retained_growth", AsyncMock(), raising=False)
    token = retention.preparation._ACTIVE.set(admission)
    try:
        yield prepared, origin
    finally:
        retention.preparation._ACTIVE.reset(token)


def _relation(family="unified", *, kind="family", initial=False):
    return retention.RetainedRows(
        kind,
        family,
        "fixture",
        "live_" + family,
        20,
        "pd_" + family + "_before_" + "a" * 20,
        21,
        "c" * 64,
        ("location_key",),
        2,
        None if initial else "d" * 64,
        0,
        initial,
    )


def _retained(prepared, origin, scopes=(), families=()):
    value = retention.RetainedServingGeneration(
        prepared,
        prepared.fhir.db,
        prepared.nonprofile_admission,
        prepared.nonprofile_admission.captured_inputs,
        asyncio.current_task(),
        *origin,
        scopes,
        families,
    )
    prepared.retained_generation = value
    return value


def test_composite_keyset_keeps_scope_and_both_bounds():
    query, arguments, where, comparison, columns, order = retention._page_sql(
        '"fixture"."bridge"',
        "live.source_id=ANY($1::varchar[])",
        ("location_key", "plan_id"),
        ("location_key", "plan_id"),
        (["source_one"],),
        ("location_a", "plan_a"),
    )
    assert arguments == [["source_one"], "location_a", "plan_a"]
    assert 'AND (live."location_key",live."plan_id")>($2,$3)' in where
    assert query.endswith('ORDER BY live."location_key",live."plan_id" LIMIT 512')
    assert comparison == '(live."location_key",live."plan_id")'
    assert columns == order
    assert "OFFSET" not in query


def test_scope_queries_use_retained_keys_and_exact_sources(owner):
    prepared, _origin = owner
    scopes = (_relation("npi_scope", kind="scope"), _relation("location_scope", kind="scope"))
    for family in ("profile", "evidence"):
        predicate, arguments = retention._family_scope(prepared, family, scopes)
        assert scopes[0].name in predicate and "scope.npi=live.npi" in predicate and arguments == ()
    predicate, arguments = retention._family_scope(prepared, "overlay", scopes)
    assert predicate == "live.source_id=ANY($1::varchar[])" and arguments == (["source_one"],)
    for family in (
        "unified",
        "address_evidence",
        "plan_bridge",
        "network_bridge",
        "procedure_bridge",
        "medication_bridge",
        "facility_candidate",
    ):
        predicate, arguments = retention._family_scope(prepared, family, scopes)
        assert scopes[1].name in predicate and "scope.location_key=live.location_key" in predicate and arguments == ()
    assert retention._family_scope(prepared, "profile", ()) == ("FALSE", ())


@pytest.mark.asyncio
async def test_native_copy_is_bounded_and_keeps_exact_last_key(monkeypatch, owner):
    prepared, origin = owner
    driver = SimpleNamespace(fetch=AsyncMock(side_effect=[[{"key_0": 2}, {"key_0": 4}], [{"key_0": 5}], []]))
    origin = origin[0], driver, origin[2]
    native_copy = AsyncMock(side_effect=[100, 60])
    monkeypatch.setattr(retention, "native_copy_projection", native_copy)
    monkeypatch.setattr(retention.preparation, "remaining_build_seconds", AsyncMock(return_value=30))
    target = retention.preparation.OwnedRelation("fixture", "retained", 21, 0, "u")
    assert (
        await retention._copy_rows(prepared, origin, '"fixture"."live"', target, ("npi",), ("npi",), "TRUE", ()) == 160
    )
    calls = native_copy.await_args_list
    assert calls[0].args[2:] == (4,) and calls[1].args[2:] == (4, 5)
    assert 'WHERE TRUE AND (live."npi")<=($1)' in calls[0].args[1]
    assert '(live."npi")>($1)' in calls[1].args[1]
    assert '(live."npi")<=($2)' in calls[1].args[1]
    assert all(call.kwargs["max_bytes"] == 16 * 1024 * 1024 for call in calls)
    assert all(call.args[1].endswith(" LIMIT 512") for call in calls)
    assert retention.capacity.check_ordinary_retained_growth.await_count == 2
    prepared.nonprofile_admission.assert_ready.assert_not_awaited()


@pytest.mark.asyncio
async def test_repeated_native_key_page_is_refused(monkeypatch, owner):
    prepared, origin = owner
    driver = SimpleNamespace(fetch=AsyncMock(side_effect=[[{"key_0": 2}], [{"key_0": 2}]]))
    native_copy = AsyncMock(return_value=10)
    monkeypatch.setattr(retention, "native_copy_projection", native_copy)
    monkeypatch.setattr(retention.preparation, "remaining_build_seconds", AsyncMock(return_value=30))
    target = retention.preparation.OwnedRelation("fixture", "retained", 21, 0, "u")
    with pytest.raises(RuntimeError, match="row_retention_changed"):
        await retention._copy_rows(
            prepared, (origin[0], driver, origin[2]), "fixture.live", target, ("npi",), ("npi",), "TRUE", ()
        )
    native_copy.assert_awaited_once()


@pytest.mark.asyncio
async def test_full_row_parity_uses_bidirectional_jsonb_multisets():
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=True))
    await retention._assert_parity(
        driver, "fixture.live", "fixture.before", "live.source_id=ANY($1::varchar[])", (["source_one"],)
    )
    statement = driver.fetchval.await_args.args[0]
    assert statement.count("EXCEPT ALL") == 2 and "to_jsonb(live)" in statement and "to_jsonb(saved)" in statement
    assert driver.fetchval.await_args.args[1:] == (["source_one"],)
    driver.fetchval.return_value = False
    with pytest.raises(RuntimeError, match="row_retention_changed"):
        await retention._assert_parity(driver, "fixture.live", "fixture.before", "TRUE", ())


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("copy refused"), asyncio.CancelledError()])
@pytest.mark.parametrize("cleanup_failure", [False, True])
async def test_failed_prepare_preserves_error_and_rolls_back_exact_clones(monkeypatch, owner, failure, cleanup_failure):
    prepared, _origin = owner
    events = []
    admission = prepared.nonprofile_admission
    name = "pd_unified_before_" + "a" * 20

    @asynccontextmanager
    async def transaction(*_args):
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        events.append("commit")

    async def scopes(*_args):
        admission._external_relations.add(("fixture", name))
        admission._relations[("fixture", name)] = 21
        raise failure

    monkeypatch.setattr(retention.preparation, "nonprofile_sql_transaction", transaction)
    monkeypatch.setattr(retention, "_prepare_scopes", scopes)
    if cleanup_failure:
        admission.retire_external_relation.side_effect = RuntimeError("cleanup refused")
    with pytest.raises(type(failure)) as raised:
        await retention.prepare_retained_generation(prepared)
    assert raised.value is failure
    assert events == ["begin", "rollback"]
    admission.retire_external_relation.assert_awaited_once_with(prepared.fhir, "fixture", name, 21)
    assert prepared.retained_generation is None
    assert (
        prepared.retention_cleanup_pending == (("fixture", name, 21),)
        if cleanup_failure
        else prepared.retention_cleanup_pending == ()
    )


@pytest.mark.asyncio
async def test_initial_target_is_registered_without_copy_or_row_hash(monkeypatch, owner):
    prepared, _origin = owner
    entry_by_field = {
        "family": "profile",
        "scope_kind": "initial_full_root",
        "source_schema": "fixture",
        "source_relation": "provider_directory_profile",
        "source_oid": 20,
        "source_layout_sha256": "c" * 64,
    }
    prepared.nonprofile_admission.captured_inputs.predecessor = {"kind": "initial", "profile": {"profile_rows": 12}}
    relation = retention.preparation.OwnedRelation("fixture", entry_by_field["source_relation"], 20, 100, "p")
    monkeypatch.setattr(retention, "_native_relation", AsyncMock(return_value=relation))
    register = AsyncMock()
    monkeypatch.setattr(retention.capacity, "register_ordinary_retained_relation", register)
    monkeypatch.setattr(retention.capacity, "retention_layout_sha256", AsyncMock(return_value="c" * 64))
    row_hash = AsyncMock()
    monkeypatch.setattr(retention, "_row_identity", row_hash)
    retained = await retention._reuse_initial(prepared, entry_by_field)
    assert retained.reused_initial and retained.rows == 12 and retained.copied_bytes == 0
    assert retained.oid == retained.source_oid == 20 and retained.data_sha256 is None
    register.assert_awaited_once()
    row_hash.assert_not_awaited()
    prepared.fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_cut_rechecks_read_compatible_locks_content_and_distinct_transaction(monkeypatch, owner):
    prepared, origin = owner
    clone = _relation()
    retained = _retained(prepared, origin, families=(clone,))
    lock = AsyncMock()
    monkeypatch.setattr(retention.receipts, "lock", lock)
    monkeypatch.setattr(
        retention,
        "_native_relation",
        AsyncMock(return_value=retention.preparation.OwnedRelation("fixture", clone.name, 21, 10, "p")),
    )
    monkeypatch.setattr(retention, "_layout", AsyncMock(return_value=clone.layout_sha256))
    monkeypatch.setattr(retention, "_row_identity", AsyncMock(return_value=(clone.rows, clone.data_sha256)))
    monkeypatch.setattr(retention, "_family_scope", lambda *_args: ("TRUE", ()))
    parity = AsyncMock()
    monkeypatch.setattr(retention, "_assert_parity", parity)
    cut_origin = origin[0], origin[1], dict(origin[2], xid="15")
    monkeypatch.setattr(retention, "_origin", AsyncMock(return_value=cut_origin))
    result = await retention.recheck_retained_generation(prepared)
    assert result["creator"]["xid"] == "14" and retained.cutover_actor["xid"] == "15"
    assert lock.await_args.kwargs == {"mode": "SHARE"}
    parity.assert_awaited_once()
    prepared.fhir.db.status.assert_not_awaited()
    monkeypatch.setattr(retention, "_origin", AsyncMock(return_value=(origin[0], origin[1], dict(origin[2], xid="16"))))
    with pytest.raises(RuntimeError, match="row_retention_changed"):
        await retention.recheck_retained_generation(prepared)


@pytest.mark.asyncio
async def test_cleanup_skips_initial_targets_and_refuses_a_publication_attempt(monkeypatch, owner):
    prepared, origin = owner
    clone, initial = _relation(), _relation("profile", initial=True)
    retained = _retained(prepared, origin, families=(initial, clone))
    drop = AsyncMock()
    monkeypatch.setattr(retention.preparation, "_drop_owned_relation", drop)
    await retention.discard_unpublished_retention(prepared)
    drop.assert_awaited_once_with(prepared.fhir, retained.admission, clone.schema, clone.name)
    retained.admission.retire_external_relation.assert_awaited_once_with(
        prepared.fhir, clone.schema, clone.name, clone.oid
    )
    retained.cutover_actor = dict(origin[2])
    with pytest.raises(RuntimeError, match="row_retention_changed"):
        await retention.discard_unpublished_retention(prepared)
    assert drop.await_count == 1


@pytest.mark.asyncio
async def test_clone_checks_parity_before_logging_and_preserves_constraints(monkeypatch, owner):
    prepared, origin = owner
    events = []
    source = retention.preparation.OwnedRelation("fixture", "provider_directory_profile", 20, 100, "p")
    clone = retention.preparation.OwnedRelation("fixture", "pd_profile_before_" + "a" * 20, 21, 100, "u")
    logged = retention.preparation.OwnedRelation("fixture", clone.relation, 21, 100, "p")
    monkeypatch.setattr(retention.uuid, "uuid4", lambda: SimpleNamespace(hex="a" * 32))
    monkeypatch.setattr(retention, "_native_relation", AsyncMock(side_effect=[source, clone, logged]))
    monkeypatch.setattr(retention, "_columns", AsyncMock(return_value=("npi", "profile_json")))
    monkeypatch.setattr(retention, "_copy_rows", AsyncMock(side_effect=lambda *_args: events.append("copy") or 100))
    monkeypatch.setattr(retention, "_assert_parity", AsyncMock(side_effect=lambda *_args: events.append("parity")))
    monkeypatch.setattr(retention, "_row_identity", AsyncMock(return_value=(2, "d" * 64)))
    monkeypatch.setattr(retention, "_layout", AsyncMock(return_value="c" * 64))
    register = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("register"))
    monkeypatch.setattr(retention.capacity, "register_ordinary_retained_relation", register)
    prepared.nonprofile_admission.before_logging.side_effect = lambda *_args: events.append("before_logging")
    prepared.fhir.db.status.side_effect = lambda statement: events.append(
        "logged" if "SET LOGGED" in statement else "create"
    )
    result = await retention._clone(
        prepared, origin, "family", "profile", (source.schema, source.relation, 20), ("npi",), "TRUE", ()
    )
    assert events == ["create", "register", "copy", "parity", "before_logging", "logged"]
    assert result.rows == 2 and result.data_sha256 == "d" * 64 and result.oid == 21
    create_sql = prepared.fhir.db.status.await_args_list[0].args[0]
    for clause in ("INCLUDING DEFAULTS", "INCLUDING CONSTRAINTS", "INCLUDING STORAGE", "INCLUDING INDEXES"):
        assert clause in create_sql
    assert "DISABLE" not in create_sql and "TRIGGER" not in create_sql


@pytest.mark.parametrize("initial", [False, True])
def test_scope_verifier_refuses_an_unproved_missing_profile_delta(owner, initial):
    prepared, _origin = owner
    prepared.profile_bundle = SimpleNamespace(
        stages=(SimpleNamespace(target_relation="provider_directory_profile"),), profile_delta=None
    )
    prepared.fhir.profile_initial = SimpleNamespace(build_from_stages=lambda *_args: None)
    prepared.nonprofile_admission.captured_inputs.predecessor = {"kind": "initial" if initial else "published"}
    with pytest.raises(RuntimeError, match="row_retention_changed"):
        retention.retained_scope_source(prepared, "npi")
    if initial:
        prepared.fhir.profile_initial.build_from_stages = lambda *_args: object()
    else:
        prepared.profile_bundle.stages = ()
    assert retention.retained_scope_source(prepared, "npi") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["identity", "generated"])
async def test_copy_columns_refuse_implicit_generated_values(changed):
    column_by_field = {
        "attname": "npi",
        "attgenerated": "s" if changed == "generated" else "",
        "attidentity": "a" if changed == "identity" else "",
    }
    driver = SimpleNamespace(fetch=AsyncMock(return_value=[column_by_field]))
    with pytest.raises(RuntimeError, match="row_retention_changed"):
        await retention._columns(driver, 20)


@pytest.mark.asyncio
async def test_local_growth_refusal_stops_copy_without_authority_refresh(monkeypatch, owner):
    prepared, origin = owner
    driver = SimpleNamespace(fetch=AsyncMock(return_value=[{"key_0": 2}]))
    native_copy = AsyncMock(return_value=10)
    monkeypatch.setattr(retention, "native_copy_projection", native_copy)
    monkeypatch.setattr(retention.preparation, "remaining_build_seconds", AsyncMock(return_value=30))
    retention.capacity.check_ordinary_retained_growth.side_effect = RuntimeError("retention quota exceeded")
    target = retention.preparation.OwnedRelation("fixture", "retained", 21, 0, "u")
    with pytest.raises(RuntimeError, match="quota exceeded"):
        await retention._copy_rows(
            prepared, (origin[0], driver, origin[2]), "fixture.live", target, ("npi",), ("npi",), "TRUE", ()
        )
    native_copy.assert_awaited_once()
    prepared.nonprofile_admission.assert_ready.assert_not_awaited()
