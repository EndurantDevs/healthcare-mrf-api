# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A diagnostic run cannot publish, weaken normal limits, or select authority."""

import asyncio
from collections import Counter
from contextlib import asynccontextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_workers as workers
from api import plan_pricing_code_calibration as calibration
from api import plan_pricing_code_zip_build as scoped
from api.plan_pricing_code_contract import validate_params


def _params():
    return dict(
        plan_release_id="release-test",
        serving_revision_id="revision-test",
        code_system="CPT",
        code="27447",
        expected_identity_digest="a" * 64,
        scope_kind="zip_radius",
        zip5="10000",
        zip_radius_miles="25",
        operation="calibrate",
    )


@pytest.mark.parametrize(
    "extra",
    [
        {"operation": True},
        {"operation": "publish"},
        {"sql": "SELECT 1"},
        {"limits": {}},
        {"role": "owner"},
        {"image": "arbitrary"},
    ],
)
def test_calibration_accepts_no_caller_authority_or_budget(extra):
    with pytest.raises(ValueError):
        validate_params(_params() | extra)


def test_calibration_cannot_be_requested_as_readiness_or_without_full_scope():
    with pytest.raises(ValueError):
        validate_params(_params(), include_identity=False)
    params = _params()
    params.pop("zip5")
    with pytest.raises(ValueError):
        validate_params(params)


@pytest.fixture
def transaction_case(monkeypatch):
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield object()
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    monkeypatch.setattr(calibration.db, "transaction", transaction)
    prepare = AsyncMock(return_value={"complete": True})
    cleanup = AsyncMock()
    monkeypatch.setattr(calibration, "_prepare", prepare)
    monkeypatch.setattr(calibration, "_cleanup_proof", cleanup)
    return SimpleNamespace(events=events, prepare=prepare, cleanup=cleanup)


@pytest.mark.asyncio
async def test_success_rolls_back_before_receipt_and_never_promotes_normal_caps(transaction_case):
    context_by_field = {"audit": {}, "phase_timings": {}}
    result = await calibration.calibrate(_params(), context_by_field)
    assert result == {"calibration": {"complete": True, "rolled_back": True, "cleanup_verified": True}}
    assert transaction_case.events == ["begin", "rollback"]
    transaction_case.cleanup.assert_awaited_once()
    assert context_by_field["audit"]["calibration"]["complete"]
    with pytest.raises(ValueError, match="not calibrated"):
        scoped.work_limits()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError("oracle mismatch"), asyncio.CancelledError()])
async def test_failure_and_cancellation_roll_back_and_keep_failure_diagnostics(transaction_case, failure):
    transaction_case.prepare.side_effect = failure
    context_by_field = {"audit": {}, "phase_timings": {}}
    with pytest.raises(type(failure)):
        await calibration.calibrate(_params(), context_by_field)
    assert transaction_case.events == ["begin", "rollback"]
    transaction_case.cleanup.assert_awaited_once()
    assert context_by_field["audit"]["calibration"]["complete"] is False


@pytest.mark.asyncio
async def test_failed_cleanup_cannot_be_reported_as_success(transaction_case):
    transaction_case.cleanup.side_effect = ValueError("stage survived")
    context_by_field = {"audit": {}, "phase_timings": {}}
    with pytest.raises(ValueError, match="stage survived"):
        await calibration.calibrate(_params(), context_by_field)
    assert transaction_case.events == ["begin", "rollback"]
    assert context_by_field["audit"]["calibration"]["complete"] is False


def test_only_persisted_exact_admission_selects_calibration_deadline():
    spec = workers._BY_WORKER_CLASS["process.PTGCandidateAudit"]
    payload = workers._AdmittedWorkerRequest(run_id="run-test", importer="plan-pricing-code-projection")
    assert not workers._is_code_calibration_job(spec, payload)
    run_by_field = dict(
        payload,
        params=_params(),
        metrics=dict(
            queue=spec.queue, function="control_single_job_start", job_id="plan_pricing_code_projection_run-test"
        ),
    )
    workers._bind_admitted_job(payload, run_by_field)
    assert workers._is_code_calibration_job(spec, payload)
    assert not workers._is_code_calibration_job(spec, dict(payload, code_calibration_job=payload.admitted_job))
    payload.admitted_job = payload.admitted_job[:-1] + ("foreign-job",)
    assert not workers._is_code_calibration_job(spec, payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [False, True])
async def test_current_resolution_and_publisher_guard_precede_staging(monkeypatch, drift):
    from api import plan_pricing_code_build as build

    selected = SimpleNamespace(serving_revision_id="revision-test")
    resolved = SimpleNamespace(state=calibration.PLAN_RELEASE_RESOLUTION_READY, selection=selected)
    identity_by_field = {"scope": {"geo_cells": ["10000"]}, "provider_signature": "a" * 64}
    params = _params() | {
        "expected_identity_digest": "f" * 64 if drift else calibration.identity_digest(identity_by_field)
    }
    monkeypatch.setattr(calibration, "resolve_plan_release_serving_resolution", AsyncMock(return_value=resolved))
    monkeypatch.setattr(calibration, "lock_identity", AsyncMock())
    monkeypatch.setattr(build, "resolve_inputs", AsyncMock(return_value=(identity_by_field, ())))
    publisher = AsyncMock()
    monkeypatch.setattr(calibration.native, "protected_publisher_owner", publisher)
    monkeypatch.setattr(calibration, "_publication_state", AsyncMock(return_value=(None, ())))
    session = SimpleNamespace(execute=AsyncMock())
    if drift:
        with pytest.raises(ValueError, match="identity changed"):
            await calibration._resolved_inputs(session, params, {})
        publisher.assert_not_awaited()
    else:
        _, authenticated, _, _ = await calibration._resolved_inputs(session, params, {})
        assert authenticated == identity_by_field
        publisher.assert_awaited_once_with(session)
    # Exercise the real isolation and physical-lock setup: no SELECT may take
    # a transaction snapshot before REPEATABLE READ has been established.
    setup_statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert setup_statements[:4] == [
        "SET LOCAL statement_timeout = '5000'",
        "SET LOCAL lock_timeout = '500'",
        "SET LOCAL idle_in_transaction_session_timeout = '15000'",
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    ]
    assert len(setup_statements) == 6
    assert all(statement.startswith("LOCK TABLE ") for statement in setup_statements[4:])


def test_only_observed_calibration_job_gets_shorter_deadline(monkeypatch):
    spec = workers._BY_WORKER_CLASS["process.PTGCandidateAudit"]
    launch = workers._AdmittedWorkerRequest(run_id="run-test", importer="plan-pricing-code-projection")
    run_by_field = dict(
        launch,
        params=_params(),
        metrics=dict(
            queue=spec.queue, function="control_single_job_start", job_id="plan_pricing_code_projection_run-test"
        ),
    )
    workers._bind_admitted_job(launch, run_by_field)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "1000")
    _, admitted = workers._worker_job_spec(spec, launch, {}, [], "run-test")
    _, ordinary = workers._worker_job_spec(spec, dict(launch), {}, [], "run-test")
    assert admitted["activeDeadlineSeconds"] == 415 and ordinary["activeDeadlineSeconds"] == 1000
    environment, _ = workers._worker_job_environment(spec, launch)
    assert {"name": "HLTHPRT_PTG_CANDIDATE_AUDIT_JOB_TIMEOUT", "value": "385"} in environment


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "frame", "encoded"])
async def test_complete_input_digest_enforces_byte_bounds_and_closes_stream(monkeypatch, failure):
    import hashlib

    from api import plan_pricing_code_oracle as oracle_module

    class InputStream:
        def __init__(self):
            self.closed = False

        def __aiter__(self):
            return self._iterate()

        async def _iterate(self):
            for encoded_row in ('{"npi":1}', '{"npi":1}', '{"npi":2}'):
                yield (encoded_row,)

        async def close(self):
            self.closed = True

    stream = InputStream()
    session = SimpleNamespace(stream=AsyncMock(return_value=stream))
    oracle = oracle_module.CodeInputOracle(session, {}, [])
    if failure == "frame":
        monkeypatch.setattr(oracle_module, "MAX_FRAME_BYTES", 1)
    elif failure == "encoded":
        monkeypatch.setattr(oracle_module, "MAX_ENCODED_BYTES", 12)
    if failure:
        with pytest.raises(ValueError, match="byte bound"):
            await oracle._digest()
    else:
        expected = hashlib.sha256()
        for encoded in (b'{"npi":1}', b'{"npi":1}', b'{"npi":2}'):
            expected.update(len(encoded).to_bytes(4, "big") + encoded)
        assert await oracle._digest() == expected.hexdigest()
        assert oracle.encoded_bytes == 39
    assert stream.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("remaining", ["stage", "temporary", "attachment", "pins", None])
async def test_cleanup_requires_removed_stage_and_unchanged_publication(monkeypatch, remaining):
    session = SimpleNamespace(
        execute=AsyncMock(),
        scalar=AsyncMock(side_effect=[71 if remaining == "stage" else None, int(remaining == "temporary")]),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(calibration.db, "transaction", transaction)
    before = (None, ("snapshot-original",))
    after = (42, before[1]) if remaining == "attachment" else (None, ("snapshot-foreign",))
    publication = AsyncMock(return_value=before if remaining is None else after)
    monkeypatch.setattr(calibration, "_publication_state", publication)
    audit_by_field = dict(schema="candidate_test", temporary_namespace_oid=17, identity={}, publication_before=before)
    if remaining:
        with pytest.raises(ValueError, match="survived rollback|changed publication"):
            await calibration._cleanup_proof(audit_by_field)
        assert "cleanup_verified" not in audit_by_field
    else:
        await calibration._cleanup_proof(audit_by_field)
        assert audit_by_field["cleanup_verified"] is True
    assert publication.await_count == int(remaining not in {"stage", "temporary"})
    await calibration._cleanup_proof({})
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state,selection",
    [
        ("UNAVAILABLE", None),
        (calibration.PLAN_RELEASE_RESOLUTION_READY, None),
        (calibration.PLAN_RELEASE_RESOLUTION_READY, SimpleNamespace(serving_revision_id="foreign")),
    ],
)
async def test_unpublished_or_changed_revision_never_resolves_candidate_inputs(monkeypatch, state, selection):
    from api import plan_pricing_code_build as build

    monkeypatch.setattr(
        calibration,
        "resolve_plan_release_serving_resolution",
        AsyncMock(return_value=SimpleNamespace(state=state, selection=selection)),
    )
    resolve = AsyncMock()
    monkeypatch.setattr(build, "resolve_inputs", resolve)
    with pytest.raises(ValueError, match="exact current published revision"):
        await calibration._resolved_inputs(SimpleNamespace(execute=AsyncMock()), _params(), {})
    resolve.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage,temporary", [(91, 0), (None, 1), (None, 0)])
async def test_calibration_requires_empty_owned_preimage_before_temporary_ddl(stage, temporary):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[stage, temporary, 17]), execute=AsyncMock())
    audit_by_field = {}
    if stage is not None or temporary:
        with pytest.raises(ValueError, match="preimage is not empty"):
            await calibration._register_stage(session, audit_by_field)
        session.execute.assert_not_awaited()
        assert "temporary_namespace_oid" not in audit_by_field
    else:
        dataset_id = await calibration._register_stage(session, audit_by_field)
        assert audit_by_field == dict(
            schema=calibration.native.reference_family_stage_schema(dataset_id), temporary_namespace_oid=17
        )
        assert (
            str(session.execute.await_args.args[0])
            == "CREATE TEMP TABLE code_calibration_registration(owner uuid) ON COMMIT DROP"
        )


@pytest.mark.asyncio
async def test_worker_dispatches_calibration_with_fresh_audit_and_refuses_ordinary_operation(transaction_case):
    from api import plan_pricing_code_build as build

    context_by_field = {"context": {"audit": {"stale": True}, "phase_timings": {"stale": 99}}}
    result = await build.control_start(context_by_field, _params() | {"run_id": "run-test", "test_mode": False})
    assert result["calibration"]["rolled_back"] is True
    assert transaction_case.events == ["begin", "rollback"]
    assert context_by_field["context"]["audit"] == {"calibration": {"complete": True, "cleanup_verified": True}}
    assert context_by_field["context"]["phase_timings"] == {}
    params = _params()
    params.pop("operation")
    with pytest.raises(ValueError, match="exact operator operation"):
        await calibration.calibrate(params, {"audit": {}, "phase_timings": {}})
    assert transaction_case.events == ["begin", "rollback"]


@pytest.mark.asyncio
async def test_oracle_refuses_candidate_bytes_before_copying_raw_input():
    from api import plan_pricing_code_oracle as oracle_module

    session = SimpleNamespace(
        scalar=AsyncMock(return_value=oracle_module.MAX_CALIBRATION_BYTES + 1),
        execute=AsyncMock(),
        connection=AsyncMock(),
    )
    with pytest.raises(ValueError, match="candidate byte budget"):
        await oracle_module.require_budget(session, "candidate_test")
    session.scalar.side_effect = [oracle_module.MAX_CALIBRATION_BYTES, 1]
    oracle = oracle_module.CodeInputOracle(session, {}, [])
    with pytest.raises(ValueError, match="raw byte budget"):
        await oracle._raw_batch("SELECT 1", "'10000'", "10000", [(1, 2)], [1])
    session.execute.assert_not_awaited()
    session.connection.assert_not_awaited()
    assert oracle.batch_count == 0 and oracle.work["raw_address"] == 0


@pytest.mark.asyncio
async def test_oracle_membership_budget_preserves_measured_work_without_writing():
    from api import plan_pricing_code_oracle as oracle_module

    binding = SimpleNamespace(
        binding={"ordinal": 0},
        serving_tables=SimpleNamespace(uses_shared_blocks=True, shared_snapshot_key=1),
        code_rows_by_identity={("CPT", "27447"): [{"code_key": 1}]},
    )
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[2, 3, 6, oracle_module.MAX_CALIBRATION_BYTES, 1]), execute=AsyncMock()
    )
    oracle = oracle_module.CodeInputOracle(session, {}, [])
    with pytest.raises(ValueError, match="membership byte budget"):
        await oracle._members(binding, (SimpleNamespace(schema_name="candidate_test"),), [1, 2])
    session.execute.assert_not_awaited()
    assert {key: oracle.work[key] for key in ("first", "priced", "factor")} == dict(first=2, priced=3, factor=6)


@pytest.mark.asyncio
async def test_oracle_copy_boundary_keeps_all_occurrences_and_duplicate_multiplicity():
    from api import plan_pricing_code_oracle_rates as rates
    from tests.test_plan_pricing_projection_v4_occurrence import _occurrence, _serving

    copied_batches = []

    async def copy_records(_name, **kwargs):
        copied_batches.append(list(kwargs["records"]))

    driver = SimpleNamespace(copy_records_to_table=copy_records)
    raw = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(connection=AsyncMock(return_value=raw), scalar=AsyncMock(return_value=0))
    source_rows = [_occurrence(source_key=index) for index in range(1001)]
    source_rows.append(dict(source_rows[0]))
    await rates._store_occurrences(
        session, _serving(), SimpleNamespace(binding={"ordinal": 2}), source_rows, {"2" * 32}
    )
    assert [len(batch) for batch in copied_batches] == [1000, 1]
    records = [record for batch in copied_batches for record in batch]
    assert [record[1] for record in records] == list(range(1001))
    assert {record[7] for record in records} == set(range(1001))
    assert sum(record[-1] for record in records) == 1002
    assert records[0][-1] == 2 and all(record[0] == 2 for record in records)


def _oracle_rate_case(monkeypatch, price_count, atom_count):
    from api import plan_pricing_code_build as build
    from api import plan_pricing_code_oracle_rates as rates
    from api import ptg2_db_sidecars as sidecars
    from api import ptg2_serving as serving
    from api.ptg2_price_hydration_cache import PriceHydrationCache
    from process.ptg_parts import ptg2_serving_binary_v3 as codec
    from tests.test_plan_pricing_projection_v4_occurrence import _occurrence
    from tests.test_ptg2_price_hydration_cache import _serving_tables
    from tests.test_ptg2_shared_alias_readers import _logical_block

    blocks_by_kind = {
        "price_set_atom_memberships_v3": _logical_block(
            codec.encode_price_memberships(((key, range(atom_count)) for key in range(price_count)), 24),
            entry_count=price_count,
        ),
        "price_atoms_v3": _logical_block(
            codec.encode_price_atoms(
                codec.PTG2V3PriceAtomRecord(str(10 + key % 2), (None,) * 7) for key in range(atom_count)
            ),
            entry_count=atom_count,
        ),
    }

    async def read_blocks(_session, **kwargs):
        return {0: blocks_by_kind[kwargs["artifact_kind"]]}

    monkeypatch.setattr(sidecars, "_shared_logical_blocks_by_key", read_blocks)
    monkeypatch.setattr(sidecars, "_claim_logical_block_processing", Mock())
    cache = PriceHydrationCache(8 * 1024 * 1024)
    monkeypatch.setattr(serving, "PRICE_HYDRATION_CACHE", cache)
    binding = SimpleNamespace(
        binding={"ordinal": 2},
        serving_tables=_serving_tables(atom_key_block_span=1024),
        code_rows_by_identity={("CPT", "27447"): [{"code_key": 1}]},
    )
    price_key_by_set_id = {f"{key:032x}": key for key in range(price_count)}
    source_rows = [
        _occurrence(price_key=key, price_set_global_id_128=price_id) for price_id, key in price_key_by_set_id.items()
    ]
    monkeypatch.setattr(build, "_binding_code_rows", AsyncMock(return_value=(source_rows * 2, price_key_by_set_id)))
    copy_records = AsyncMock()
    driver = SimpleNamespace(copy_records_to_table=copy_records)
    raw = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(
        connection=AsyncMock(return_value=raw), execute=AsyncMock(), scalar=AsyncMock(return_value=True)
    )
    reader = AsyncMock(wraps=rates.prices._price_batch_rates)
    monkeypatch.setattr(rates.prices, "_price_batch_rates", reader)
    return SimpleNamespace(session=session, binding=binding, copied=copy_records, reader=reader, cache=cache)


def _assert_oracle_rate_multisets(copied):
    import json

    from tests.test_plan_pricing_projection_v4_occurrence import _occurrence

    copied_by_table = {
        name: [record for call in copied.await_args_list if call.args[0] == name for record in call.kwargs["records"]]
        for name in ("code_calibration_rates", "code_calibration_occurrences")
    }
    assert Counter(copied_by_table["code_calibration_rates"]) == Counter(
        (2, f"{key:032x}", Decimal(rate), 350) for key in range(100) for rate in (10, 11)
    )
    fragment = _occurrence(network_names=["Network A", "Network B"])
    for field in (
        "_ptg_provider_set_key",
        "provider_set_global_id_128",
        "price_key",
        "price_set_global_id_128",
        "serving_content_hash_128",
        "source_key",
        "provider_count",
    ):
        del fragment[field]
    encoded_fragment = json.dumps(fragment, sort_keys=True, separators=(",", ":"))
    assert Counter(copied_by_table["code_calibration_occurrences"]) == Counter(
        (2, key, 7, "1" * 32, key, f"{key:032x}", "3" * 32, 11, 2, encoded_fragment, 2) for key in range(100)
    )


@pytest.mark.asyncio
async def test_oracle_splits_native_atom_overflow(monkeypatch):
    from api import plan_pricing_code_oracle_rates as rates

    case = _oracle_rate_case(monkeypatch, 100, 700)
    for warm in (False, True):
        if warm:
            await rates.prices._price_batch_rates(
                case.session, case.binding, tuple((f"{key:032x}", key) for key in range(10))
            )
            assert case.cache.metrics().entries == 10
            case.reader.reset_mock()
        cache_hits = case.cache.metrics().hits
        proof = await rates.verify_rate_inputs(
            case.session, SimpleNamespace(schema_name="candidate_test"), [(case.binding, None)]
        )
        assert proof == {"occurrence_multisets": True, "price_atom_multisets": True, "rate_profiles": True}
        assert [len(call.args[2]) for call in case.reader.await_args_list] == [100, 50, 50]
        _assert_oracle_rate_multisets(case.copied)
        if warm:
            assert case.cache.metrics().hits > cache_hits
        case.reader.reset_mock()
        case.copied.reset_mock()


@pytest.mark.asyncio
async def test_oracle_refuses_native_singleton_overflow(monkeypatch):
    from api import plan_pricing_code_oracle_rates as rates

    case = _oracle_rate_case(monkeypatch, 1, 65_537)
    with pytest.raises(rates.prices.ManifestReadLimitError, match="atom limit"):
        await rates.verify_rate_inputs(
            case.session, SimpleNamespace(schema_name="candidate_test"), [(case.binding, None)]
        )
    case.reader.assert_awaited_once()
    case.copied.assert_not_awaited()
    case.session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_oracle_split_preserves_cumulative_budget(monkeypatch):
    from api import plan_pricing_code_oracle as oracle
    from api import plan_pricing_code_oracle_rates as rates

    case = _oracle_rate_case(monkeypatch, 100, 700)
    case.session.scalar.return_value = oracle.MAX_CALIBRATION_BYTES + 1
    with pytest.raises(ValueError, match="candidate byte budget"):
        await rates.verify_rate_inputs(
            case.session, SimpleNamespace(schema_name="candidate_test"), [(case.binding, None)]
        )
    assert [len(call.args[2]) for call in case.reader.await_args_list] == [100, 50]
    assert case.copied.await_count == 50
    assert all(call.args[0] == "code_calibration_rates" for call in case.copied.await_args_list)
    case.session.scalar.assert_awaited_once()


@pytest.mark.asyncio
async def test_completed_oracle_cannot_hide_input_identity_drift(monkeypatch):
    from api import plan_pricing_code_build as build

    identity_by_field = {"scope": {"geo_cells": ["10000"]}, "provider_signature": "a" * 64}
    monkeypatch.setattr(
        calibration, "_resolved_inputs", AsyncMock(return_value=(object(), identity_by_field, [], None))
    )
    monkeypatch.setattr(calibration, "_register_stage", AsyncMock(return_value=object()))
    oracle = SimpleNamespace(prepare=AsyncMock(return_value={}), verify=AsyncMock(return_value={}))
    monkeypatch.setattr(calibration, "CodeInputOracle", lambda *_args: oracle)
    monkeypatch.setattr(
        calibration.native,
        "precreate_model_family_stage",
        AsyncMock(return_value=SimpleNamespace(schema_name="candidate_test")),
    )
    monkeypatch.setattr(build, "_create_stage_tables", AsyncMock())
    monkeypatch.setattr(build, "_prepare_code_candidate", AsyncMock())
    monkeypatch.setattr(
        build, "resolve_inputs", AsyncMock(return_value=(identity_by_field | {"provider_signature": "b" * 64}, []))
    )
    budget = AsyncMock()
    monkeypatch.setattr(calibration, "require_budget", budget)
    with pytest.raises(ValueError, match="identity changed during preparation"):
        await calibration._prepare(object(), _params(), {"audit": {}, "phase_timings": {}}, {})
    oracle.verify.assert_awaited_once()
    budget.assert_not_awaited()
