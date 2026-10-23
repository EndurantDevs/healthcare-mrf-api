# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete-code identity and request-local physical read boundaries."""

import asyncio
import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from api.plan_pricing_code_contract import (
    BUILD_REVISION,
    CONTRACT,
    capability_identity,
    identity_digest,
    source_coverage,
    validate_params,
)
from api.plan_pricing_code_read import CodePricingCapability
from api.plan_pricing_code_storage import _membership_views, attachment_name
from api.plan_pricing_projection_contract import code_projection_scope, current_code_projection, table
from api.plan_release_serving import PlanReleaseServingSelection, PlanReleaseSnapshotBinding


def _selection():
    return PlanReleaseServingSelection(
        "hpserve_01J00000000000000000000000",
        "hprelease_01J00000000000000000000000",
        "hpplan_01J00000000000000000000000",
        None,
        "2026-01",
        "published",
        "a" * 64,
        (
            PlanReleaseSnapshotBinding(
                0, "synthetic-snapshot", "synthetic-source", "synthetic-plan", "group", "in_network", True, True
            ),
        ),
    )


def _source():
    return {
        "binding_ordinal": 0,
        "materialized_binding_ordinal": 0,
        "read_identity_digest": "e" * 64,
        "source_identity": {"snapshot_id": "synthetic-snapshot", "code_system": "CPT", "code": "27447"},
        "code_rows": [{"code_key": 1, "occurrence_count": 3}],
        "membership": {"active_attachment": {"name": "pm_" + "a" * 52, "relation_oid": 1001}},
    }


def _identity():
    return capability_identity(_selection(), ("CPT", "27447"), "b" * 64, [_source()])


def _capability(schema="reference_family_archive_00000000000000000000000000000001"):
    return CodePricingCapability(_identity(), (SimpleNamespace(schema_name=schema), {}, 1, 2))


def test_complete_identity_is_separate_from_full_projection_and_binds_all_inputs():
    identity = _identity()
    from api.plan_pricing_projection_contract import projection_id

    assert identity["contract"] == CONTRACT and identity["build_revision"] == BUILD_REVISION
    assert identity_digest(identity) != projection_id(identity["binding_set_digest"], identity["provider_signature"])
    for field, value in (
        ("provider_signature", "c" * 64),
        ("code", "99213"),
        ("serving_revision_id", "other-revision"),
    ):
        changed = identity | {field: value}
        assert identity_digest(changed) != identity_digest(identity)
    changed = capability_identity(_selection(), ("CPT", "27447"), "c" * 64, [_source()])
    assert attachment_name(identity) == attachment_name(changed)
    assert attachment_name(identity) != attachment_name(identity | {"code": "99213"})


def test_recipe_refuses_missing_or_wrong_logical_binding_coverage():
    with pytest.raises(ValueError, match="coverage"):
        capability_identity(_selection(), ("CPT", "27447"), "b" * 64, [])
    with pytest.raises(ValueError, match="coverage"):
        capability_identity(_selection(), ("CPT", "27447"), "b" * 64, [_source() | {"binding_ordinal": 1}])


@pytest.mark.parametrize(
    "rows",
    [
        [{"code_key": 2, "rate_count": 3}],
        [{"code_key": 1, "rate_count": 2}],
        [{"code_key": 1, "rate_count": 3}, {"code_key": 1, "rate_count": 3}],
    ],
)
def test_every_physical_code_and_occurrence_count_must_be_covered(rows):
    membership_by_key = {"code_coverage": [{"code_key": 1, "occurrence_count": 3}]}
    with pytest.raises(ValueError, match="coverage|occurrence"):
        source_coverage(0, {}, rows, membership_by_key, read_identity_digest="e" * 64)


def test_complete_alias_coverage_is_part_of_the_identity():
    membership_by_key = {
        "projection_id": "a" * 64,
        "content_digest": "b" * 64,
        "active_attachment": {"name": "pm_" + "a" * 52, "relation_oid": 1},
        "storage": {"schema_oid": 2},
        "code_coverage": [{"code_key": 1, "occurrence_count": 2}, {"code_key": 2, "occurrence_count": 3}],
    }
    source = source_coverage(
        0,
        {"snapshot_id": "synthetic-snapshot"},
        [{"code_key": 2, "rate_count": 3}, {"code_key": 1, "rate_count": 2}],
        membership_by_key,
        read_identity_digest="e" * 64,
    )
    assert source["code_rows"] == [{"code_key": 1, "occurrence_count": 2}, {"code_key": 2, "occurrence_count": 3}]
    before = capability_identity(_selection(), ("CPT", "27447"), "c" * 64, [source])
    changed = copy.deepcopy(source)
    changed["source_identity"]["map_digest"] = "d" * 64
    after = capability_identity(_selection(), ("CPT", "27447"), "c" * 64, [changed])
    assert identity_digest(before) != identity_digest(after)


@pytest.mark.parametrize(
    "changes", [{"code_system": "cpt"}, {"unexpected": True}, {"code": ""}, {"source_key": "raw-source"}]
)
def test_normal_worker_params_are_closed(changes):
    params_by_name = {
        "plan_release_id": _selection().plan_release_id,
        "serving_revision_id": _selection().serving_revision_id,
        "code_system": "CPT",
        "code": "27447",
        "expected_identity_digest": "c" * 64,
    }
    validate_params(params_by_name)
    with pytest.raises(ValueError):
        validate_params(params_by_name | changes)


def test_native_membership_dependencies_cannot_be_forged_by_names():
    assert _membership_views(_identity()) == {"pm_" + "a" * 52: 1001}
    source = _source()
    source["membership"]["active_attachment"]["name"] = 'unsafe".table'
    with pytest.raises(Exception, match="dependency identity"):
        _membership_views(_identity() | {"sources": [source]})


@pytest.mark.asyncio
async def test_physical_capability_scope_is_local_and_restored_on_failure():
    first, second = _capability(), _capability("reference_family_archive_00000000000000000000000000000002")

    async def read(capability):
        with code_projection_scope(capability):
            await asyncio.sleep(0)
            assert capability.found[0].schema_name in table("plan_pricing_provider_membership")
            assert table("npi") == '"mrf"."npi"'
            assert current_code_projection() is capability
        return current_code_projection()

    assert await asyncio.gather(read(first), read(second)) == [None, None]
    with pytest.raises(RuntimeError), code_projection_scope(first):
        raise RuntimeError("synthetic failure")
    assert table("plan_pricing_provider_membership") == '"mrf"."plan_pricing_provider_membership"'


def test_state_sql_and_cursor_preserve_physical_family_limits():
    from api.endpoint.pagination import PaginationParams
    from api.plan_pricing_state_scan import _cursor_scope, _validate_search_request
    from api.plan_pricing_state_scan_contract import query_metadata
    from api.plan_pricing_state_scan_sql import state_scan_page_sql, state_scan_provider_page_sql
    from tests.test_plan_pricing_state_scan import _args

    capability = _capability()
    selected = capability.selection(_selection())
    with code_projection_scope(capability):
        sql = state_scan_page_sql((0,))
        assert "plan_pricing_code_occurrence" in sql and "plan_pricing_code_membership" in sql
        assert ":membership_sentinel_limit" in sql and ":occurrence_sentinel_limit" in sql
        assert "admitted_binding_ordinals" in state_scan_provider_page_sql((0,))
        assert query_metadata("CPT", "27447", "MI")["projection_contract"] == CONTRACT
        args = _args(code="27447", plan_release_id=selected.plan_release_id)
        _validate_search_request(selected, args, PaginationParams(limit=1, page=1, offset=0, source="test"))
        before = _cursor_scope(selected, args, "CPT", "27447", "MI", 1)
        after = _cursor_scope(replace(selected, pricing_projection_id="c" * 64), args, "CPT", "27447", "MI", 1)
        assert before[2] != after[2]
    with pytest.raises(Exception, match="no ready"):
        _validate_search_request(selected, args, PaginationParams(limit=1, page=1, offset=0, source="test"))


@pytest.mark.parametrize("drift", ["client", "code", "provider", "source"])
def test_code_state_cursor_refuses_cross_scope_replay(monkeypatch, drift):
    from api import plan_pricing_state_scan as scan
    from api.billing_search_cursor import BillingSearchCursorError
    from tests.test_plan_pricing_state_scan import _args, _keyring

    capability = _capability()
    selection = capability.selection(_selection())
    manifest = SimpleNamespace(
        generation_id="synthetic-network-generation",
        approved_custom_revision="synthetic-network-revision",
        manifest_sha256="e" * 64,
    )
    access = SimpleNamespace(network_ids=None, access_scope_sha256="a" * 64, manifest=manifest)
    monkeypatch.setattr(scan, "current_network_address_scope", lambda: access)
    args = _args(code="27447", plan_release_id=selection.plan_release_id)
    before = scan._cursor_scope(selection, args, "CPT", "27447", "MI", 1)
    cursor = scan._seal_position((1000000001, 1, 1, 1), keyring=_keyring(), trusted_now=1000, scope=before)
    if drift == "client":
        access.access_scope_sha256 = "b" * 64
    elif drift == "code":
        args["code"] = "99213"
    elif drift == "provider":
        selection = replace(selection, pricing_projection_id="f" * 64)
    else:
        selection = replace(selection, bindings=(replace(selection.bindings[0], snapshot_id="another-snapshot"),))
    after = scan._cursor_scope(selection, args, "CPT", args["code"], "MI", 1)
    with pytest.raises(BillingSearchCursorError):
        scan._open_position(cursor, keyring=_keyring(), trusted_now=1001, scope=after)


@pytest.mark.asyncio
async def test_expected_identity_drift_refuses_before_candidate_ddl(monkeypatch):
    from unittest.mock import AsyncMock

    from api import plan_pricing_code_build as build

    monkeypatch.setattr(build, "MAX_CODE_MEMBER_CELL_WORK_ROWS", 1000)
    monkeypatch.setattr(build, "MAX_CODE_FACTOR_JOIN_WORK_ROWS", 1000)
    monkeypatch.setattr(build, "resolve_inputs", AsyncMock(return_value=(_identity(), [])))
    create = AsyncMock()
    monkeypatch.setattr(build.native, "precreate_model_family_stage", create)
    with pytest.raises(ValueError, match="identity changed"):
        await build.build_in_session(
            object(), selection=_selection(), code_system="CPT", code="27447", expected_identity_digest="f" * 64
        )
    create.assert_not_awaited()


def test_whole_release_work_guards_remain_uncalibrated(monkeypatch):
    import hashlib

    from api import plan_pricing_code_build as build
    from api.plan_pricing_projection_v3_types import _BuildState
    from api.plan_pricing_projection_v3_work import _work_limits

    with pytest.raises(ValueError, match="not calibrated"):
        _work_limits(_BuildState(hashlib.sha256()))
    assert build._member_cell_limit() == 10_000
    assert build._factor_join_limit() == 8_192
    monkeypatch.setattr(build, "MAX_CODE_MEMBER_CELL_WORK_ROWS", None)
    monkeypatch.setattr(build, "MAX_CODE_FACTOR_JOIN_WORK_ROWS", None)
    with pytest.raises(ValueError, match="not calibrated"):
        build._member_cell_limit()
    with pytest.raises(ValueError, match="not calibrated"):
        build._factor_join_limit()


def test_importer_discovery_and_worker_share_closed_contract():
    from api import control_imports as imports
    from api.control_workers import _START_WORKERS

    entry = imports._code_pricing_registry_entry()
    assert entry["name"] == "plan-pricing-code-projection"
    assert {parameter["name"] for parameter in entry["params_schema"]} == {
        "plan_release_id",
        "serving_revision_id",
        "code_system",
        "code",
        "expected_identity_digest",
    }
    adapter = imports._SINGLE_JOB_ADAPTERS[entry["name"]]
    assert adapter["target_function"] == "build_plan_pricing_code_projection"
    assert any(entry["name"] in spec.importers for spec in _START_WORKERS)
    assert entry["name"] in imports.ALL_STATUS_IDEMPOTENCY_IMPORTERS


def test_code_preparation_all_status_key_has_its_own_unique_migration(monkeypatch):
    import importlib.util
    from pathlib import Path

    from db.models import ImportRun
    from tests.test_control_imports_api import _OpRecorder

    path = Path(__file__).parents[1] / "alembic/versions/20261023110000_code_pricing_import_idempotency.py"
    spec = importlib.util.spec_from_file_location("code_pricing_idempotency_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    recorder = _OpRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    declared = next(
        index
        for index in ImportRun.__my_additional_indexes__
        if index["name"] == "import_run_code_pricing_idempotency_idx"
    )
    migration.upgrade()
    actual = recorder.indexes[declared["name"]]
    assert actual["unique"] and actual["columns"] == list(declared["index_elements"])
    assert str(actual["postgresql_where"]) == declared["where"]


@pytest.mark.asyncio
@pytest.mark.parametrize("different", [None, "plan", "market", "physical_family"])
async def test_authoritative_physical_aliases_are_validated_but_materialized_once(monkeypatch, different):
    from unittest.mock import AsyncMock

    from api import plan_pricing_code_build as build

    first = _selection().bindings[0]
    alias = replace(first, binding_ordinal=1, snapshot_id="alias-snapshot", source_key="alias-source")
    if different == "plan":
        alias = replace(alias, plan_id="different-plan")
    if different == "market":
        alias = replace(alias, plan_market_type="individual")
    selected = replace(_selection(), bindings=(first, alias))
    tables = SimpleNamespace()

    async def source(_session, params):
        family = "different-family" if different == "physical_family" and params["ordinal"] == 1 else "same-family"
        return tables, ("sealed-generation", 1, "logical-coverage", params["plan_id"], params["market_type"], family)

    loader = AsyncMock(side_effect=source)
    monkeypatch.setattr(build, "binding_source", loader)

    async def identity(_session, source, snapshot, *_args, **_kwargs):
        return tables, None, {"source_key": source, "snapshot_id": snapshot}

    monkeypatch.setattr(build, "source_identity", identity)
    monkeypatch.setattr(build, "locate_membership", AsyncMock(return_value=(object(),)))
    member_by_key = {
        "code_coverage": [{"code_key": 1, "occurrence_count": 3}],
        "projection_id": "a" * 64,
        "content_digest": "b" * 64,
        "active_attachment": {"name": "pm_" + "a" * 52, "relation_oid": 1},
        "storage": {"schema_oid": 2},
    }
    monkeypatch.setattr(build, "membership_receipt", lambda _found: member_by_key)
    monkeypatch.setattr(build, "load_sealed_code_rows", AsyncMock(return_value=[{"code_key": 1, "rate_count": 3}]))
    monkeypatch.setattr(build, "provider_signature", AsyncMock(return_value="c" * 64))
    recipe, inputs = await build.resolve_inputs(object(), selected, ("CPT", "27447"))
    assert loader.await_count == 2 and len(recipe["sources"]) == 2
    assert recipe["sources"][1]["source_identity"]["source_key"] == "alias-source"
    assert len(inputs) == (1 if different is None else 2)
    assert recipe["sources"][1]["materialized_binding_ordinal"] == (0 if different is None else 1)


def test_policy_alias_remapping_and_inferred_taxonomy_stay_inside_native_read_scope():
    first = _source()
    alias = copy.deepcopy(first)
    alias["binding_ordinal"] = 1
    alias["source_identity"]["snapshot_id"] = "alias-snapshot"
    selected = replace(
        _selection(),
        bindings=(
            _selection().bindings[0],
            replace(
                _selection().bindings[0], binding_ordinal=1, snapshot_id="alias-snapshot", source_key="alias-source"
            ),
        ),
    )
    identity = capability_identity(selected, ("CPT", "27447"), "b" * 64, [first, alias])
    scope = CodePricingCapability(identity, _capability().found).contributors((selected.bindings[1],))
    sql = scope.relation("plan_pricing_provider_membership")
    assert "WHEN 0 THEN 1" in sql
    cells = scope.relation("plan_pricing_provider_cell")
    assert "stored.entity_type_code=1" in cells and "taxonomy_code" in cells
    with pytest.raises(Exception, match="coverage"):
        scope.contributors((replace(selected.bindings[0], binding_ordinal=9),))


@pytest.mark.asyncio
async def test_code_provider_reads_preserve_all_witnesses_across_hundred_npi_boundary(monkeypatch):
    from api import plan_pricing_code_build as build

    session = object()
    npis = list(range(1000000001, 1000000102))
    expected_by_npi = {
        npi: (
            {"npi": npi, "state_address_rank": 1, "zip5": "48104"},
            {"npi": npi, "state_address_rank": 2, "zip5": "48105"},
        )
        for npi in npis
    }
    calls = []

    async def read(actual_session, batch):
        assert actual_session is session
        calls.append(tuple(batch))
        return {npi: expected_by_npi[npi] for npi in batch}

    monkeypatch.setattr(build, "projection_provider_rows_for_npis", read)
    assert await build._code_provider_rows_for_npis(session, npis) == expected_by_npi
    assert calls == [tuple(npis[:100]), tuple(npis[100:])]


@pytest.mark.asyncio
async def test_code_provider_subbatches_preserve_cumulative_original_row_refusal(monkeypatch):
    from api import plan_pricing_code_build as build

    calls = []

    async def read(_session, batch):
        calls.append(tuple(batch))
        return {npi: ({"npi": npi},) for npi in batch}

    monkeypatch.setattr(build, "projection_provider_rows_for_npis", read)
    monkeypatch.setattr(build, "MAX_PROVIDER_ROWS_PER_BATCH", 100)
    with pytest.raises(ValueError, match="provider-row bound exceeded"):
        await build._code_provider_rows_for_npis(object(), list(range(101)))
    assert [len(batch) for batch in calls] == [100, 1]


@pytest.mark.asyncio
async def test_code_provider_subbatch_rejects_rows_outside_its_scope(monkeypatch):
    from api import plan_pricing_code_build as build

    async def read(_session, _batch):
        return {1000000002: ({"npi": 1000000002},)}

    monkeypatch.setattr(build, "projection_provider_rows_for_npis", read)
    with pytest.raises(ValueError, match="provider-row bound exceeded"):
        await build._code_provider_rows_for_npis(object(), [1000000001])


@pytest.mark.asyncio
@pytest.mark.parametrize("excess", [0, 1])
async def test_code_factor_boundary_refuses_before_membership_writes(monkeypatch, excess):
    from unittest.mock import AsyncMock

    from api import plan_pricing_code_build as build
    from api import ptg2_serving as serving

    limit = build._factor_join_limit()
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[limit + excess, 1, False]), execute=AsyncMock())
    binding = SimpleNamespace(binding={"ordinal": 0}, serving_tables=object())
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 1)
    monkeypatch.setattr(serving, "_shared_v3_provider_set_table", lambda _tables: "synthetic.provider_set")
    monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda _tables: "synthetic.dictionary")
    args = (session, binding, (SimpleNamespace(schema_name="synthetic"),), [{"code_key": 1}], 100, limit)
    if excess:
        with pytest.raises(ValueError, match="factor join exceeds"):
            await build._stage_memberships(*args)
        session.execute.assert_not_awaited()
    else:
        assert await build._stage_memberships(*args) == (1, limit)
        assert session.execute.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("excess", [0, 1])
async def test_code_member_cell_boundary_refuses_before_aggregate_work(monkeypatch, excess):
    from unittest.mock import AsyncMock

    from api import plan_pricing_code_build as build
    from api import plan_pricing_projection_v3_work as work

    limit = build._member_cell_limit()
    session = SimpleNamespace(execute=AsyncMock(side_effect=[None, SimpleNamespace(scalar_one=lambda: limit + excess)]))
    monkeypatch.setattr(work, "_normalized_taxonomy_codes", lambda _code: (False, None))

    async def statement(_stage, sql):
        return sql

    if excess:
        with pytest.raises(ValueError, match="member-cell work bound exceeded"):
            await work._stage_member_cells(session, "a" * 64, ("CPT", "00000"), limit, statement)
    else:
        assert await work._stage_member_cells(session, "a" * 64, ("CPT", "00000"), limit, statement) == limit
    assert session.execute.await_count == 2


@pytest.mark.parametrize(
    "host,port,database,accepted",
    [
        ("127.0.0.1", 55487, "hc_code_pricing_" + "a" * 32, True),
        ("postgres", 5432, "hc_code_pricing_" + "a" * 32, True),
        ("postgres", 1024, "hc_code_pricing_" + "b" * 32, True),
        ("postgres", 65535, "hc_code_pricing_" + "b" * 32, True),
        ("localhost", 5432, "hc_code_pricing_" + "a" * 32, False),
        ("127.0.0.2", 5432, "hc_code_pricing_" + "a" * 32, False),
        ("postgres.example", 5432, "hc_code_pricing_" + "a" * 32, False),
        ("postgres", 1023, "hc_code_pricing_" + "a" * 32, False),
        ("postgres", None, "hc_code_pricing_" + "a" * 32, False),
        ("postgres", 5432, "other_" + "a" * 32, False),
        ("postgres", 5432, "hc_code_pricing_" + "a" * 31, False),
        ("postgres", 5432, "hc_code_pricing_" + "a" * 33, False),
        ("postgres", 5432, "hc_code_pricing_" + "A" * 32, False),
    ],
)
def test_code_native_fixture_keeps_exact_database_and_host_guards(monkeypatch, host, port, database, accepted):
    from tests.test_plan_pricing_code_postgres import _url

    endpoint = host if port is None else f"{host}:{port}"
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_CODE_TEST_DSN", f"postgresql://postgres@{endpoint}/{database}")
    if accepted:
        value = _url()
        assert (value.host, value.database, value.drivername) == (host, database, "postgresql+asyncpg")
    else:
        with pytest.raises(pytest.fail.Exception, match="task-owned loopback database"):
            _url()


def test_code_native_fixture_requires_an_explicit_database(monkeypatch):
    from tests.test_plan_pricing_code_postgres import _url

    monkeypatch.delenv("HLTHPRT_PLAN_PRICING_CODE_TEST_DSN", raising=False)
    with pytest.raises(pytest.skip.Exception, match="HLTHPRT_PLAN_PRICING_CODE_TEST_DSN is not set"):
        _url()
