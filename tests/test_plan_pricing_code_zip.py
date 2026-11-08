# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed ZIP capability identity, routing and unqualified-work refusal."""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import orjson
import pytest

from api import plan_pricing_code_build as build
from api import plan_pricing_code_zip_build as scoped
from api.plan_pricing_code_contract import (
    BUILD_REVISION,
    CONTRACT,
    ZIP_BUILD_REVISION,
    ZIP_CONTRACT,
    capability_identity,
    identity_digest,
    scope_selectors,
    validate_params,
)
from api.plan_pricing_code_read import CodePricingCapability, _request_scope
from api.plan_pricing_code_storage import attachment_name
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable, code_projection_scope
from tests.test_plan_pricing_code_projection import _identity, _selection, _source


def _params(*, expected=True):
    response_by_field = {
        "plan_release_id": _selection().plan_release_id,
        "serving_revision_id": _selection().serving_revision_id,
        "code_system": "CPT",
        "code": "27447",
        "scope_kind": "zip_radius",
        "zip5": "48104",
        "zip_radius_miles": "25",
    }
    if expected:
        response_by_field["expected_identity_digest"] = "a" * 64
    return response_by_field


def _scope():
    return {**scope_selectors(_params()), "geo_cells": ["48104", "48105"]}


def _zip_identity():
    return capability_identity(_selection(), ("CPT", "27447"), "b" * 64, [_source()], scope=_scope())


def test_old_identity_stays_unscoped_and_zip_identity_is_distinct():
    plain, scoped = _identity(), _zip_identity()
    assert plain["contract"] == CONTRACT and plain["build_revision"] == BUILD_REVISION
    assert "scope" not in plain
    assert scoped["contract"] == ZIP_CONTRACT and scoped["build_revision"] == ZIP_BUILD_REVISION
    assert attachment_name(plain) != attachment_name(scoped)
    for field, value in (("zip5", "48105"), ("zip_radius_miles", "26")):
        other = scoped | {"scope": scoped["scope"] | {field: value}}
        assert attachment_name(other) != attachment_name(scoped)
    changed = scoped | {"scope": scoped["scope"] | {"geo_cells": ["48104"]}, "provider_signature": "c" * 64}
    assert attachment_name(changed) == attachment_name(scoped)
    assert identity_digest(changed) != identity_digest(scoped)


@pytest.mark.parametrize("radius", ["0", "25", "0.5", "1" * 32])
def test_closed_scope_accepts_canonical_radius(radius):
    validate_params(_params() | {"zip_radius_miles": radius})
    validate_params(_params(expected=False) | {"zip_radius_miles": radius}, include_identity=False)


@pytest.mark.parametrize(
    "changes",
    [
        {"scope_kind": "state"},
        {"scope_kind": None},
        {"zip5": "4810"},
        {"zip5": "４８１０４"},
        {"zip_radius_miles": 25},
        {"zip_radius_miles": "25.0"},
        {"zip_radius_miles": "01"},
        {"zip_radius_miles": "1e2"},
        {"zip_radius_miles": "NaN"},
        {"zip_radius_miles": "Infinity"},
        {"zip_radius_miles": "-0"},
        {"zip_radius_miles": "-1"},
        {"zip_radius_miles": " 25"},
        {"geo_cells": ["48104"]},
        {"binding_ordinal": "0"},
    ],
)
def test_scope_rejects_noncanonical_or_open_params(changes):
    with pytest.raises(ValueError):
        validate_params(_params() | changes)


@pytest.mark.parametrize("missing", ["scope_kind", "zip5", "zip_radius_miles"])
def test_scope_selectors_are_all_or_none(missing):
    params = _params()
    del params[missing]
    with pytest.raises(ValueError):
        validate_params(params)


def test_scoped_cells_are_complete_canonical_identity_data():
    for cells in (["48105", "48104"], ["48104", "48104"], ["4810"], "48104"):
        with pytest.raises(ValueError):
            capability_identity(
                _selection(), ("CPT", "27447"), "b" * 64, [_source()], scope=_scope() | {"geo_cells": cells}
            )


@pytest.mark.asyncio
async def test_new_work_gates_refuse_before_candidate_ddl(monkeypatch):
    create = AsyncMock()
    monkeypatch.setattr(build.native, "precreate_model_family_stage", create)
    with pytest.raises(ValueError, match="not calibrated"):
        await build.build_in_session(
            object(),
            selection=_selection(),
            code_system="CPT",
            code="27447",
            expected_identity_digest=identity_digest(_zip_identity()),
            scope=scope_selectors(_params()),
        )
    create.assert_not_awaited()
    assert build._factor_join_limit() == 16384 and build._member_cell_limit() == 32768


def test_zip_capability_cannot_be_used_for_state_or_provider_witnesses():
    from api.endpoint.pagination import PaginationParams
    from api.plan_pricing_state_scan import _validate_search_request
    from tests.test_plan_pricing_state_scan import _args

    capability = CodePricingCapability(_zip_identity(), (SimpleNamespace(schema_name="synthetic"),))
    with code_projection_scope(capability), pytest.raises(PlanPricingProjectionUnavailable, match="state-scan"):
        _validate_search_request(
            capability.selection(_selection()), _args(), PaginationParams(limit=1, page=1, offset=0, source="test")
        )
    for name in ("plan_pricing_provider_state", "plan_pricing_rate_occurrence"):
        with pytest.raises(PlanPricingProjectionUnavailable, match="nationwide"):
            capability.relation(name)


def test_scope_is_only_selected_for_supported_zip_aggregates():
    request_by_field = {
        "code_system": "CPT",
        "code": "27447",
        "view": "card",
        "include_providers": "false",
        "zip5": "48104",
        "zip_radius_miles": "25.0",
    }
    assert _request_scope(request_by_field) == scope_selectors(_params())
    for change in ({"include_providers": "true"}, {"state": "MI"}, {"taxonomy_code": "207Q00000X"}, {"view": "full"}):
        assert _request_scope(request_by_field | change) is None


@pytest.mark.parametrize("radius", [True, "1" * 33, "0." + "0" * 33 + "1", "invalid", "1e32"])
def test_invalid_reader_radius_is_refused_instead_of_using_unscoped_prices(radius):
    from api.plan_pricing_projection_contract import PlanPricingProjectionUnsupported

    with pytest.raises(PlanPricingProjectionUnsupported, match="radius is invalid"):
        _request_scope(dict(zip5="48104", zip_radius_miles=radius, view="card", include_providers="false"))


def test_identity_requires_closed_scope_shape_even_if_cells_are_valid():
    from api.plan_pricing_code_contract import validate_scope

    for scope in (
        None,
        _scope() | {"sql": "SELECT 1"},
        {key: value for key, value in _scope().items() if key != "geo_cells"},
    ):
        with pytest.raises(ValueError, match="coverage is invalid"):
            validate_scope(scope)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "counts,error,writes", [([6], "classified address", 0), ([1, 6], "assured", 2), ([1, 1, 6], "member-cell", 2)]
)
async def test_assurance_and_member_cells_refuse_overflow_before_materialization(counts, error, writes):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=counts), execute=AsyncMock())
    with pytest.raises(ValueError, match=error):
        await scoped._stage_provider_cells(session, "candidate-test", 5, 5)
    assert session.execute.await_count == writes
    assert not any(
        "INSERT INTO plan_pricing_provider_cell_stage" in str(call.args[0]) for call in session.execute.await_args_list
    )


@pytest.mark.asyncio
async def test_factor_overflow_and_missing_source_are_not_publishable_memberships():
    from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

    binding = SimpleNamespace(
        binding={"ordinal": 0}, serving_tables=SimpleNamespace(uses_shared_blocks=True, shared_snapshot_key=1)
    )
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[2, 3, 7]), execute=AsyncMock())
    with pytest.raises(ValueError, match="pre-deduplication factor"):
        await scoped._stage_local_factors(
            session, binding, (SimpleNamespace(schema_name="candidate_test"),), {}, dict(first=5, priced=5, factor=6)
        )
    assert session.execute.await_count == 3
    session = SimpleNamespace(scalar=AsyncMock(return_value=True), execute=AsyncMock())
    with pytest.raises(PTG2ManifestArtifactError, match="source coverage differs"):
        await scoped._store_provider_sets(session, binding)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [5, 6])
async def test_membership_probe_cannot_report_success_after_exhausting_bound(monkeypatch, count):
    binding = SimpleNamespace(
        binding={"ordinal": 0}, serving_tables=SimpleNamespace(uses_shared_blocks=True, shared_snapshot_key=1)
    )
    measured_by_name = dict(first=2, priced=3, factor=4)
    monkeypatch.setattr(scoped, "_stage_local_factors", AsyncMock(return_value=measured_by_name))
    session = SimpleNamespace(scalar=AsyncMock(return_value=count))
    call = scoped.stage_memberships(session, binding, (), [{"code_key": 1}], dict(first=5, priced=5, member_cell=5))
    if count > 5:
        with pytest.raises(ValueError, match="membership work bound"):
            await call
    else:
        assert await call == (count, measured_by_name)
    assert session.scalar.await_args.args[1]["member_probe"] == 6


@pytest.mark.asyncio
async def test_changed_complete_zip_scope_cannot_read_an_attached_pack(monkeypatch):
    from api import plan_pricing_projection_read as reader

    monkeypatch.setattr(reader, "geo_cells", AsyncMock(return_value=["48104"]))
    monkeypatch.setattr(reader, "current_code_projection", lambda: SimpleNamespace(identity=_zip_identity()))
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(PlanPricingProjectionUnavailable, match="complete ZIP aggregate scope changed"):
        await reader._projection_page_response(
            session, SimpleNamespace(result_type="rate_aggregates"), {}, object(), None, _selection()
        )
    session.execute.assert_not_awaited()


def test_all_work_units_must_be_qualified_together(monkeypatch):
    names = (
        "MAX_ZIP_RAW_ADDRESS_WORK_ROWS",
        "MAX_ZIP_ADDRESS_WORK_ROWS",
        "MAX_ZIP_FIRST_FACTOR_ROWS",
        "MAX_ZIP_PRICED_FACTOR_ROWS",
        "MAX_ZIP_FACTOR_JOIN_WORK_ROWS",
        "MAX_ZIP_MEMBER_CELL_WORK_ROWS",
    )
    assert all(getattr(scoped, name) is None for name in names)
    for index, name in enumerate(names, 1):
        monkeypatch.setattr(scoped, name, index)
        if index != len(names):
            with pytest.raises(ValueError, match="not calibrated"):
                scoped.work_limits()
    assert scoped.work_limits() == dict(raw_address=1, address=2, first=3, priced=4, factor=5, member_cell=6)


def test_two_membership_dependencies_cannot_reuse_a_name_for_different_relations():
    from api.plan_pricing_code_storage import _membership_views
    from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

    identity_by_field = _zip_identity()
    source_by_field = _source()
    source_by_field["membership"]["active_attachment"]["relation_oid"] += 1
    identity_by_field["sources"].append(source_by_field)
    with pytest.raises(PTG2ManifestArtifactError, match="identity collides"):
        _membership_views(identity_by_field)


@pytest.mark.asyncio
async def test_exact_zip_attachment_is_preferred_and_invalid_storage_never_falls_back(monkeypatch):
    from api import plan_pricing_code_read as reader

    identity = _zip_identity()
    session = SimpleNamespace(scalar=AsyncMock(return_value=True))
    monkeypatch.setattr(reader, "_lock_provider_relations", AsyncMock())
    monkeypatch.setattr(build, "resolve_inputs", AsyncMock(return_value=(identity, [])))
    locate = AsyncMock(return_value=(SimpleNamespace(schema_name="synthetic"),))
    monkeypatch.setattr(reader, "locate", locate)
    request_by_field = {**_params(expected=False), "view": "card", "include_providers": "false"}
    capability = await reader.resolve_code_pricing(session, _selection(), request_by_field)
    assert capability.identity == identity and session.scalar.await_count == 1
    locate.side_effect = PlanPricingProjectionUnavailable("invalid attached storage")
    with pytest.raises(PlanPricingProjectionUnavailable, match="invalid attached"):
        await reader.resolve_code_pricing(session, _selection(), request_by_field)
    assert session.scalar.await_count == 2


@pytest.mark.asyncio
async def test_readiness_preserves_scope_and_refuses_uncalibrated_work(monkeypatch):
    from api import control_plan_pricing_code as control
    from api.plan_release_serving_resolution import PLAN_RELEASE_RESOLUTION_READY

    routes_by_path = {}

    class Blueprint:
        def get(self, path):
            def register(function):
                routes_by_path[path] = function
                return function

            return register

    class Args(dict):
        def getlist(self, key):
            return [self[key]]

    @asynccontextmanager
    async def transaction():
        yield object()

    monkeypatch.setattr(control, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(control, "db", SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(control, "_lock_provider_relations", AsyncMock())
    monkeypatch.setattr(
        control,
        "resolve_plan_release_serving_resolution",
        AsyncMock(return_value=SimpleNamespace(state=PLAN_RELEASE_RESOLUTION_READY, selection=_selection())),
    )
    identity = _zip_identity()
    monkeypatch.setattr(control, "resolve_inputs", AsyncMock(return_value=(identity, [])))
    monkeypatch.setattr(control, "locate", AsyncMock(return_value=None))
    control.register_code_pricing_routes(Blueprint())
    request = SimpleNamespace(args=Args(_params(expected=False)))
    response_by_field = orjson.loads((await routes_by_path["/plan-pricing/code-projection"](request)).body)
    assert (
        response_by_field["state"] == "unqualified"
        and response_by_field["missing"] == "scoped_card_resource_calibration"
    )
    assert response_by_field["identity"] == identity and response_by_field[
        "expected_identity_digest"
    ] == identity_digest(identity)
    assert response_by_field["contract"] == ZIP_CONTRACT and response_by_field["zip5"] == "48104"
    monkeypatch.setattr(scoped, "work_limits", lambda: {"synthetic": 1})
    response_by_field = orjson.loads((await routes_by_path["/plan-pricing/code-projection"](request)).body)
    assert response_by_field["state"] == "absent" and "missing" not in response_by_field


def test_shared_assurance_preserves_ordinary_query_bytes():
    from api import ptg2_serving as serving

    assert hashlib.sha256(serving._MEMBERSHIP_UNIFIED_ASSURED_LOCATION_SQL.encode()).hexdigest() == (
        "908e1cfd54175b9ed763a7a8e1b59cc9954bfde289c961e0db56706d409262df"
    )


@pytest.mark.asyncio
async def test_group_enumeration_charges_complete_raw_count_before_pending_membership(monkeypatch):
    monkeypatch.setattr(build, "CODE_PROVIDER_READ_BATCH_SIZE", 2)
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(side_effect=[7, 12]))
    assert await scoped._stage_address_batch(session, "48104", 10, 7) == (7, 12)
    query, params = session.execute.await_args_list[1].args
    assert "GROUP BY addr.npi ORDER BY addr.npi" in str(query)
    assert "location_key" not in str(query) and params == {"cell": "48104", "after_npi": 10, "probe_limit": 2}
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=8))
    with pytest.raises(ValueError, match="raw address work bound"):
        await scoped._stage_address_batch(session, "48104", 10, 7)
    assert session.execute.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("expected,actual,remaining", [(7, 7, 7), (7, 6, 7), (8, 8, 7), (0, 0, 0)])
async def test_matching_address_fetch_is_complete_and_guarded_before_assurance(expected, actual, remaining):
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(side_effect=[expected, actual]))
    if expected > remaining:
        with pytest.raises(ValueError, match="classified address work bound"):
            await scoped._stage_matching_addresses(session, "48104", remaining)
        session.execute.assert_not_awaited()
    elif actual != expected:
        with pytest.raises(ValueError, match="grouped raw address coverage"):
            await scoped._stage_matching_addresses(session, "48104", remaining)
    else:
        await scoped._stage_matching_addresses(session, "48104", remaining)
        query, params = session.execute.await_args.args
        assert "LIMIT" not in str(query) and "plan_pricing_provider_member_stage" in str(query)
        assert params == {"cell": "48104"}


def _zip_audit_stages(monkeypatch, *, failure=None):
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(side_effect=[True, True, False, 3, 4]))
    monkeypatch.setattr(scoped, "_create_zip_stages", AsyncMock())
    monkeypatch.setattr(
        scoped, "_stage_address_batch", AsyncMock(side_effect=[(5, 10), (3, 20), (0, 20), (4, 30), (0, 30)])
    )
    monkeypatch.setattr(scoped, "_stage_matching_addresses", AsyncMock())
    monkeypatch.setattr(
        scoped,
        "stage_memberships",
        AsyncMock(
            side_effect=[
                (4, {"first": 1, "priced": 2, "factor": 3}),
                (2, {"first": 2, "priced": 1, "factor": 2}),
                (1, {"first": 1, "priced": 1, "factor": 1}),
                (0, {"first": 0, "priced": 0, "factor": 0}),
            ]
        ),
    )
    monkeypatch.setattr(
        scoped, "_stage_provider_cells", AsyncMock(side_effect=failure or [(4, 3, 5), (2, 1, 1), (3, 2, 2)])
    )
    monkeypatch.setattr(scoped, "_store_provider_sets", AsyncMock())
    binding = SimpleNamespace(code_rows_by_identity={("CPT", "27447"): []})
    return session, [(binding, object()), (binding, object())]


@pytest.mark.asyncio
async def test_zip_audit_keeps_pre_filter_work_maxima_and_complete_exhaustion(monkeypatch):
    session, inputs = _zip_audit_stages(monkeypatch)
    audit_by_name, timings_by_phase = {}, {}
    state = SimpleNamespace(audit=audit_by_name, phase_timings=timings_by_phase)
    work_counts_by_name = await scoped.stage_inputs(
        session,
        "synthetic",
        _scope(),
        inputs,
        ("CPT", "27447"),
        state,
        dict.fromkeys(("raw_address", "address", "first", "priced", "factor", "member_cell"), 100),
    )
    measured = audit_by_name["zip_input_work"]
    assert measured["membership_count"] == 7 and state.provider_membership_count == 3
    assert measured["member_cell_count"] == 8 and state.provider_cell_count == 4
    assert measured["enumeration_pages"] == 5
    assert measured["batches_started"] == measured["batches_completed"] == 3
    assert measured["cells"] == measured["exhausted_cells"] == 2
    assert measured["exhausted"] and measured["complete"]
    assert measured["batch_maxima"] == {
        "raw_address": 5,
        "classified_address": 4,
        "address": 3,
        "first": 3,
        "priced": 3,
        "factor": 5,
        "membership": 6,
        "member_cell": 5,
    }
    assert work_counts_by_name == {
        "factor_join_work_rows": 6,
        "zip_work": {
            "raw_address": 12,
            "classified_address": 9,
            "address": 6,
            "first": 4,
            "priced": 4,
            "factor": 6,
        },
    }
    assert measured["work"] == work_counts_by_name["zip_work"]
    assert audit_by_name["phase"] == "zip.finalize" and all(elapsed >= 0 for elapsed in timings_by_phase.values())
    assert set(timings_by_phase) == {
        "zip.setup",
        "zip.enumerate",
        "zip.membership",
        "zip.addresses",
        "zip.assurance",
        "zip.finalize",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError("synthetic refusal"), asyncio.CancelledError()])
async def test_zip_audit_retains_partial_work_and_phase_without_claiming_exhaustion(monkeypatch, failure):
    session, inputs = _zip_audit_stages(monkeypatch, failure=failure)
    audit_by_name, timings_by_phase = {}, {}
    with pytest.raises(type(failure)):
        await scoped.stage_inputs(
            session,
            "synthetic",
            _scope(),
            inputs,
            ("CPT", "27447"),
            SimpleNamespace(audit=audit_by_name, phase_timings=timings_by_phase),
            dict.fromkeys(("raw_address", "address", "first", "priced", "factor", "member_cell"), 100),
        )
    measured = audit_by_name["zip_input_work"]
    assert measured["membership_count"] == 6 and measured["member_cell_count"] == 0
    assert measured["batches_started"] == 1 and measured["batches_completed"] == 0
    assert measured["exhausted_cells"] == 0 and not measured["exhausted"] and not measured["complete"]
    assert measured["batch_maxima"]["membership"] == 6
    assert audit_by_name["phase"] == "zip.assurance" and timings_by_phase["zip.assurance"] >= 0
    scoped._store_provider_sets.assert_not_awaited()


def _code_worker_dependencies(monkeypatch, failure=None):
    from api import plan_release_serving_resolution as releases

    @asynccontextmanager
    async def transaction():
        yield object()

    async def prepared(_session, **kwargs):
        kwargs["audit"]["zip_input_work"] = {"membership_count": 7, "member_cell_count": 8}
        kwargs["phase_timings"]["zip.assurance"] = 1.25
        if failure is not None:
            raise failure("synthetic interruption")
        return {"state": "ready"}

    monkeypatch.setattr(build, "db", SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(build, "lock_provider_generation", AsyncMock())
    monkeypatch.setattr(build, "build_in_session", prepared)
    monkeypatch.setattr(
        releases,
        "resolve_plan_release_serving_resolution",
        AsyncMock(
            return_value=SimpleNamespace(
                state=releases.PLAN_RELEASE_RESOLUTION_READY,
                selection=_selection(),
            )
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, asyncio.CancelledError, RuntimeError])
async def test_code_projection_passes_job_owned_audit_through_existing_worker_context(monkeypatch, failure):
    from api import control_imports
    from process import control_lifecycle

    _code_worker_dependencies(monkeypatch, failure)
    marks = AsyncMock(return_value=True)
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(control_lifecycle, "mark_control_run", marks)
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    importer = "plan-pricing-code-projection"
    adapter = control_imports._SINGLE_JOB_ADAPTERS[importer]
    task_by_field = control_imports._control_wrapped_adapter_payload(
        adapter,
        {"run_id": "synthetic_code", "importer": importer},
        _params(),
        test_mode=False,
    )
    assert task_by_field["call_style"] == "ctx_task" and task_by_field["target_function"] == "control_start"
    assert task_by_field["task"]["test_mode"] is False
    context_by_name = {"context": {"phase_timings": {"unrelated": 99}}}
    call = control_lifecycle.control_single_job_start(context_by_name, task_by_field)
    if failure is RuntimeError:
        with pytest.raises(RuntimeError, match="synthetic interruption"):
            await call
    else:
        await call
    terminal_by_field = marks.await_args_list[-1].kwargs
    assert terminal_by_field["status"] == ("succeeded" if failure is None else "failed")
    assert terminal_by_field["metrics"]["audit"] == {
        "zip_input_work": {"membership_count": 7, "member_cell_count": 8},
    }
    assert terminal_by_field["metrics"]["phase_timings"] == {"zip.assurance": 1.25}
    assert context_by_name == {"context": {"phase_timings": {"unrelated": 99}}}


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["_control_context", "context", "audit", "phase_timings", "sql", "limits", "limit"])
async def test_code_projection_refuses_unknown_worker_fields_before_database(monkeypatch, field):
    transaction = Mock(side_effect=AssertionError("database must not be opened"))
    monkeypatch.setattr(build, "db", SimpleNamespace(transaction=transaction))
    task_by_name = {**_params(), "run_id": "synthetic_code", "test_mode": False, field: {"forged": True}}
    with pytest.raises(ValueError, match="exact release"):
        await build.control_start({"context": {}}, task_by_name)
    transaction.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_scope", [False, True])
async def test_code_projection_retains_queued_kwargs_and_direct_compatibility(monkeypatch, has_scope):
    from process import control_lifecycle

    _code_worker_dependencies(monkeypatch)
    params_by_name = _params()
    if not has_scope:
        for field in ("scope_kind", "zip5", "zip_radius_miles"):
            params_by_name.pop(field)
    assert await build.build_plan_pricing_code_projection(**params_by_name) == {"state": "ready"}
    context_by_name = {"context": {}}
    outcome = await control_lifecycle._invoke_control_target(
        build,
        target_function="build_plan_pricing_code_projection",
        call_style="kwargs",
        control_context=context_by_name,
        target_task_by_field={
            **params_by_name,
            "run_id": "persisted",
            "test_mode": False,
            "_control_context": {"forged": True},
        },
    )
    assert outcome == {"state": "ready"} and context_by_name == {"context": {}}
