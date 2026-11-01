# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker wiring and refusal tests; boundary doubles do not confer native custody."""

import asyncio
import json
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, create_autospec

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from process import network_candidate_custody as custody
from process import registry_ptg_office_membership as offices
from process import registry_publication_worker as worker
from process.network_address_projection import PinnedAddressSource
from process.registry_composition_addresses import RegistryCompositionAddressSources
from process.registry_source_recipe_composition import SourceWitnessRuntime
from tests.test_network_fhir_source_witness_custody import _profile
from tests.test_registry_candidate_custody_runtime import WORKERS, _configured, _runtime_custody_case
from tests.test_registry_publication_worker import configuration

_ACTORS = (
    "protected_control_owner",
    "registry_api",
    "registry_queue_owner",
    "registry_reader",
    "registry_snapshot_owner",
    "sealed_operator",
    "worker_execution",
    "worker_lifecycle",
)


def _runtime(roles=_ACTORS):
    return SourceWitnessRuntime(
        Mock(side_effect=AssertionError("Source must not open in a worker wiring test")),
        (_profile(),),
        "capture_owner",
        tuple(sorted(roles)),
    )


def _sources(runtime):
    return RegistryCompositionAddressSources(
        PinnedAddressSource("address_source", "address_rows", "generation-one"),
        compact=True,
        source_witness_runtime=runtime,
    )


def _config(runtime, *, session=False):
    document, _ = _configured(configuration)
    if session:
        document["ptg"] = {
            "owner_role": "protected_control_owner",
            "approval_role": "office_approver",
            "read_limits": {"maximum_bytes": 1048576, "maximum_pages": 10, "maximum_coordinates": 100},
        }
    return replace(
        worker.parse_registry_publication_worker_config(json.dumps(document)), initial_address_sources=_sources(runtime)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("omitted", _ACTORS)
async def test_incomplete_source_actor_census_refuses_before_custodian_pool(monkeypatch, omitted):
    runtime = _runtime(tuple(role for role in _ACTORS if role != omitted))
    config = _config(runtime)
    case, create, finalizer, verifier, _ = _runtime_custody_case(monkeypatch, config, "complete", False)
    async with AsyncExitStack() as stack:
        with pytest.raises(ValueError, match="registry_source_witness_runtime_census_differs"):
            await worker._custody_runtime(config, stack, config.consumer, case.execution)
        assert not stack._exit_callbacks
    assert case.execution.identity_reads == 1 and case.events == []
    create.assert_not_awaited()
    finalizer.assert_not_awaited()
    verifier.assert_not_awaited()
    runtime.source_export.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("omitted", ("registry_queue_owner", "effective_queue", "registry_reader", "effective_reader"))
async def test_original_and_effective_queue_and_reader_census_precedes_pool(monkeypatch, omitted):
    roles = set(_ACTORS) | {"effective_queue", "effective_reader"}
    runtime = _runtime(roles - {omitted})
    config = _config(runtime)
    effective = replace(config.consumer, queue_owner_role="effective_queue", reader_roles=("effective_reader",))
    case, create, finalizer, verifier, _ = _runtime_custody_case(monkeypatch, config, "complete", False)
    async with AsyncExitStack() as stack:
        with pytest.raises(ValueError, match="registry_source_witness_runtime_census_differs"):
            await worker._custody_runtime(config, stack, effective, case.execution)
    create.assert_not_awaited()
    finalizer.assert_not_awaited()
    verifier.assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_owner_that_is_a_reader_cannot_escape_source_census(monkeypatch):
    runtime = _runtime(set(_ACTORS) - {"registry_reader"})
    config = _config(runtime)
    case, create, finalizer, verifier, _ = _runtime_custody_case(monkeypatch, config, "complete", False)
    case.identity_by_name.update(schema_owner_role="registry_reader", schema_owner_oid=400, candidate_owner_oid=400)
    async with AsyncExitStack() as stack:
        with pytest.raises(ValueError, match="registry_source_witness_runtime_census_differs"):
            await worker._custody_runtime(config, stack, config.consumer, case.execution)
    create.assert_not_awaited()
    finalizer.assert_not_awaited()
    verifier.assert_not_awaited()


@pytest.mark.parametrize("parser,factory", WORKERS[1:])
def test_source_consumer_uses_its_actual_owner_and_notification_actors(parser, factory):
    parsed = parser(json.dumps(_configured(factory)[0]))
    roles = {
        parsed.consumer.execution_role,
        parsed.consumer.owner_role,
        parsed.custody_url.username,
        "protected_control_owner",
    }
    if parsed.ptg is None:
        roles.update((parsed.notification_store.publisher_role, *parsed.notification_store.reader_roles))
    else:
        roles.update((parsed.reader_role, parsed.ptg.store.approval_role))
    runtime = _runtime(roles)
    config = SimpleNamespace(**vars(parsed), initial_address_sources=_sources(runtime))
    identity_by_name = {"custody_owner_role": "protected_control_owner"}
    assert worker._configured_source_witness_runtime(config, parsed.consumer, identity_by_name) is runtime
    for actor in sorted(roles):
        config.initial_address_sources = _sources(_runtime(roles - {actor}))
        with pytest.raises(ValueError, match="registry_source_witness_runtime_census_differs"):
            worker._configured_source_witness_runtime(config, parsed.consumer, identity_by_name)


@pytest.mark.asyncio
@pytest.mark.parametrize("session", (False, True))
async def test_worker_forwards_source_capability_to_both_independent_sealers(monkeypatch, session):
    runtime = _runtime(set(_ACTORS) | ({"office_approver"} if session else set()))
    config = _config(runtime, session=session)
    case, create, finalizer, verifier, _ = _runtime_custody_case(monkeypatch, config, "complete", False)
    case.pre_enrolled_only = True
    original_finalize = finalizer.side_effect

    async def finalize(*arguments, source_witness_runtime, **options):
        assert source_witness_runtime is runtime
        return await original_finalize(*arguments, **options)

    finalizer.side_effect = finalize
    async with AsyncExitStack() as stack:
        async with case.execution_pool.acquire(timeout=config.sql_limits.connect_timeout_seconds) as connection:
            consumer = await worker._custody_runtime(config, stack, config.consumer, connection)
            await consumer.seal_candidate(case.copy_target, pre_enrolled_only=True)
    assert create.await_count == int(not session)
    assert finalizer.await_count == verifier.await_count == 1
    assert case.custody_pool.closes == 1 and not case.custody_pool.active
    assert case.events.index("q reach") < case.events.index("custody created")
    runtime.source_export.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (None, ValueError("source replay refused"), asyncio.CancelledError()))
@pytest.mark.parametrize("pre_enrolled_only", (None, False, True))
async def test_session_sealer_keeps_source_replay_inside_existing_company_fence(
    monkeypatch, failure, pre_enrolled_only
):
    runtime, authority, events = _runtime(), object(), []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    driver = SimpleNamespace(transaction=transaction)
    raw = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    require_authority = AsyncMock(return_value=authority)
    seal = create_autospec(custody._seal_in_transaction, return_value={"seal_sha256": "a" * 64}, side_effect=failure)
    monkeypatch.setattr(offices, "require_ptg_office_authority", require_authority)
    monkeypatch.setattr(custody, "_seal_in_transaction", seal)
    async with AsyncSession() as session, session.begin():
        monkeypatch.setattr(session, "connection", AsyncMock(return_value=raw))
        options_by_name = dict(
            control_schema="registry",
            runtime_roles=("reader",),
            authorize=AsyncMock(),
            office_store=object(),
            office_read_budget=object(),
            source_witness_runtime=runtime,
        )
        if pre_enrolled_only is not None:
            options_by_name["pre_enrolled_only"] = pre_enrolled_only
        if failure is None:
            assert (
                await custody.seal_network_candidate_custody_in_session(session, object(), **options_by_name)
                == seal.return_value
            )
        else:
            with pytest.raises(type(failure)):
                await custody.seal_network_candidate_custody_in_session(session, object(), **options_by_name)
    assert seal.await_args.kwargs["source_witness_runtime"] is runtime
    assert seal.await_args.kwargs["office_authority"] is authority
    assert seal.await_args.kwargs["pre_enrolled_only"] is (pre_enrolled_only or False)
    assert require_authority.await_count == (2 if failure is None else 1)
    assert events == ["begin", "commit" if failure is None else "rollback"]


@pytest.mark.asyncio
@pytest.mark.parametrize("options_by_name", ({"office_authority": object()}, {"pre_enrolled_only": "true"}))
async def test_session_sealer_refuses_caller_authority_and_nonboolean_enrollment(monkeypatch, options_by_name):
    require_authority = AsyncMock(side_effect=AssertionError("invalid seal options must refuse before native reads"))
    monkeypatch.setattr(offices, "require_ptg_office_authority", require_authority)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody.seal_network_candidate_custody_in_session(
            object(),
            object(),
            control_schema="registry",
            runtime_roles=("reader",),
            authorize=AsyncMock(),
            office_store=object(),
            office_read_budget=object(),
            **options_by_name,
        )
    require_authority.assert_not_awaited()
