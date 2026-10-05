# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Host contract checks; PostgreSQL authority and deferred checks need native tests."""

from __future__ import annotations

import hashlib
import json
import time
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import dialect

from db.models.custom_import import CustomImportLease
from process.custom_import import materialization as material
from process.custom_import import materialization_store as store
from process.custom_import import runner_registry as registry
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import LeaseGrant, lease_token_sha256
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"


class Session:
    """Record transport, never emulate database validation or claim native proof."""

    def __init__(self):
        self.info = {}
        self.transaction = object()
        self.active = True
        self.no_autoflush = nullcontext()
        self.calls = []
        self.flushes = 0
        self.pending_pages = []
        self.committed_pages = []
        self.failed_page = None
        self.page_count = 0
        self.renewed = None
        self.return_count = None

    def in_transaction(self):
        return self.active

    async def connection(self):
        return SimpleNamespace(
            dialect=dialect(),
            get_transaction=lambda: self.transaction,
            info={},
            sync_connection=SimpleNamespace(
                get_execution_options=lambda: {
                    "schema_translate_map": {store.CustomImportRootScalar.__table__.schema: "synthetic schema"}
                }
            ),
        )

    async def flush(self):
        self.flushes += 1

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.calls.append((sql, parameters))
        if ".persist_custom_import_" in sql:
            self.page_count += 1
            if self.failed_page == self.page_count:
                raise RuntimeError("synthetic page failure")
            self.pending_pages.append(parameters)
            index = "p2" if "scalar_set" in sql else "p4"
            count = self.return_count if self.return_count is not None else len(parameters[index])
            return SimpleNamespace(scalar_one=lambda: count)
        return SimpleNamespace(scalar_one=lambda: None, scalar_one_or_none=lambda: self.renewed)

    @asynccontextmanager
    async def begin(self):
        try:
            yield self
        except BaseException:
            self.pending_pages.clear()
            raise
        else:
            self.committed_pages.extend(self.pending_pages)
            self.pending_pages.clear()


def definition():
    return CustomImportDefinition.from_json(_FIXTURE.read_text())


def root_rows(contract, count=257):
    return tuple(
        row
        for index in range(count)
        for row in material.project_root_scalars(
            contract,
            root_target=material.RootScalarTarget(11, 31, 1000 + index, 2000 + index),
            root_values={"npi": str(1000000000 + index), "display_name": f"Synthetic {index}"},
        )
    )


def winners(contract, count=257):
    generation = material.GenerationIdentity(61, 11, 21, 31)
    return material.materialize_winners(
        contract,
        generation=generation,
        child_collection_slots={"rates": 7},
        candidates=material.ValidatedWinnerCandidateStream(
            generation,
            (
                material.WinnerCandidate(
                    entity_binding_id=3000 + index,
                    family_revision_id=4000 + index,
                    family_sha256=hashlib.sha256(str(index).encode()).digest(),
                    context_collection_slot=7,
                    context_child_revision_id=5000 + index,
                    context_child_key_sha256=hashlib.sha256(f"child {index}".encode()).digest(),
                    values_by_field={"service_code": "synthetic", "amount": Decimal("1.250000000000")},
                )
                for index in range(count)
            ),
        ),
    )


async def bind_actual_runner(session):
    now = datetime.now(UTC)
    request = SimpleNamespace(dataset_id=11, definition_revision_id=21, schema_revision_id=31, lease_token="synthetic")
    execution = SimpleNamespace(execution_id=41, capture_bundle_id=51)
    grant = LeaseGrant(41, 2, now + timedelta(seconds=60), "running")
    lease = CustomImportLease(execution_id=41, fence=2, token_sha256=lease_token_sha256(request.lease_token))
    session.renewed = now + timedelta(seconds=registry._MATERIALIZATION_LEASE_WINDOW_SECONDS)
    await registry.establish_materialization_authority(session, request, grant, execution, lease, now)
    session.calls.clear()
    return session.info[registry._MATERIALIZATION_WINDOW_KEY]


@pytest.mark.asyncio
async def test_standalone_scalars_use_native_pages_without_runner_or_orm_adds():
    session = Session()
    contract = definition()
    assert await material.persist_scalar_projections(session, contract, root_scalars=root_rows(contract)) == 514
    pages = [(sql, args) for sql, args in session.calls if ".persist_custom_import_scalar_set" in sql]
    assert [len(args["p2"]) for _, args in pages] == [256, 256, 2]
    assert all('"synthetic schema".persist_custom_import_scalar_set' in sql for sql, _ in pages)
    assert all(args["p15"] is args["p16"] is args["p17"] is None for _, args in pages)
    assert session.info == {} and session.flushes == 1
    assert len(session.calls) == 6


@pytest.mark.asyncio
async def test_standalone_does_not_require_one_producer_per_owner_page():
    session = Session()
    contract = definition()
    # Revision producer ownership is deliberately unavailable to Python. SQL
    # resolves both revisions; the helper must not invent a single execution.
    rows = root_rows(contract, 2)
    assert await material.persist_scalar_projections(session, contract, root_scalars=rows) == 4
    budget = session.calls[0][1]
    assert budget["p1"] == (2000, 2000, 2001, 2001)
    assert budget["p4"] is budget["p5"] is budget["p6"] is None


@pytest.mark.asyncio
async def test_standalone_winners_keep_native_hash_and_canonical_context():
    session = Session()
    result = winners(definition())
    assert await material.persist_winner_materialization(session, result) == 257
    pages = [args for sql, args in session.calls if ".persist_custom_import_winner_set" in sql]
    assert [len(args["p4"]) for args in pages] == [256, 1]
    assert pages[0]["p8"] == tuple(row.context_key_sha256 for row in result.winners[:256])
    assert pages[0]["p10"] == tuple(row.canonical_context_key for row in result.winners[:256])
    assert pages[0]["p11"] is pages[0]["p12"] is pages[0]["p13"] is None


@pytest.mark.asyncio
async def test_empty_standalone_calls_only_flush_and_return_zero():
    session = Session()
    contract = definition()
    assert await material.persist_scalar_projections(session, contract) == 0
    assert await material.persist_winner_materialization(session, winners(contract, 0)) == 0
    assert session.flushes == 2 and session.calls == [] and session.info == {}


@pytest.mark.asyncio
async def test_validation_of_entire_input_precedes_flush_and_first_page():
    session = Session()
    contract = definition()
    rows = root_rows(contract)
    with pytest.raises(material.ScalarProjectionError, match="repeat"):
        await material.persist_scalar_projections(session, contract, root_scalars=(*rows, rows[0]))
    result = winners(contract)
    bad = replace(result.winners[-1], context_key_sha256=b"x" * 32)
    with pytest.raises(material.WinnerMaterializationError, match="digest"):
        await material.persist_winner_materialization(session, replace(result, winners=(*result.winners[:-1], bad)))
    assert session.flushes == 0 and session.calls == []


@pytest.mark.asyncio
async def test_active_caller_transaction_remains_required_even_for_empty():
    session = Session()
    session.active = False
    with pytest.raises(material.WinnerMaterializationError, match="active caller transaction"):
        await material.persist_scalar_projections(session, definition())
    assert session.flushes == 0 and session.calls == []


@pytest.mark.asyncio
async def test_real_runner_establishment_adds_exact_expectation_and_rejects_transaction_reuse():
    session = Session()
    window = await bind_actual_runner(session)
    contract = definition()
    assert await material.persist_scalar_projections(session, contract, root_scalars=root_rows(contract, 1)) == 2
    args = next(args for sql, args in session.calls if ".persist_custom_import_scalar_set" in sql)
    assert args["p15"] == (11, 21, 31, 41, 51, 2)
    assert args["p16"] == lease_token_sha256("synthetic") and args["p17"] == window.expires_at
    assert "authority=" not in repr(window)
    session.transaction = object()
    flushes, calls = session.flushes, len(session.calls)
    with pytest.raises(CandidateRunnerError, match="not bound"):
        await material.persist_winner_materialization(session, winners(contract, 1))
    assert session.flushes == flushes and len(session.calls) == calls


@pytest.mark.asyncio
async def test_expired_real_runner_window_fails_before_pending_flush():
    session = Session()
    window = await bind_actual_runner(session)
    session.info[registry._MATERIALIZATION_WINDOW_KEY] = replace(window, monotonic_deadline=time.monotonic() - 1)
    with pytest.raises(LeaseAuthorityLost, match="window expired"):
        await material.persist_scalar_projections(session, definition(), root_scalars=root_rows(definition(), 1))
    assert session.flushes == 0 and session.calls == []


@pytest.mark.asyncio
async def test_page_failure_escapes_to_caller_and_rolls_back_prior_transport():
    session = Session()
    session.failed_page = 2
    with pytest.raises(RuntimeError, match="page failure"):
        async with session.begin():
            await material.persist_scalar_projections(session, definition(), root_scalars=root_rows(definition()))
    assert session.page_count == 2 and session.pending_pages == [] and session.committed_pages == []


@pytest.mark.asyncio
async def test_typed_boundaries_and_missing_null_keep_the_public_codec():
    document = json.loads(_FIXTURE.read_text())
    for slot, kind in enumerate(("integer", "decimal", "boolean", "date", "timestamp"), start=6):
        document["schema"]["root"]["fields"].append(
            {
                "id": "typed_" + kind,
                "slot": slot,
                "type": kind,
                "nullable": True,
                "projection_slot": slot,
            }
        )
    contract = CustomImportDefinition.from_mapping(document)
    typed_values_by_field = {
        "npi": "1000000000",
        "display_name": "Synthetic 🧪",
        "typed_integer": -(2**63),
        "typed_decimal": Decimal("123456789012345678.123456789012"),
        "typed_boolean": False,
        "typed_date": date(2026, 1, 2),
        "typed_timestamp": datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
    }
    roots = material.project_root_scalars(
        contract, root_target=material.RootScalarTarget(11, 31, 101, 201), root_values=typed_values_by_field
    )
    children = material.project_child_scalars(
        contract,
        collection="rates",
        child_target=material.ChildScalarTarget(11, 31, 101, 7, 301),
        child_values={"service_code": "synthetic", "amount": None},
        child_collection_slots={"rates": 7},
    ) + material.project_child_scalars(
        contract,
        collection="rates",
        child_target=material.ChildScalarTarget(11, 31, 101, 7, 302),
        child_values={"service_code": "synthetic"},
        child_collection_slots={"rates": 7},
    )
    session = Session()
    assert (
        await material.persist_scalar_projections(
            session, contract, root_scalars=roots, child_scalars=children, child_collection_slots={"rates": 7}
        )
        == 10
    )
    args = next(args for sql, args in session.calls if ".persist_custom_import_scalar_set" in sql)
    assert args["p10"][2] == -(2**63)
    assert isinstance(args["p11"][3], Decimal) and args["p11"][3] == typed_values_by_field["typed_decimal"]
    assert (
        args["p12"][4] is False
        and args["p13"][5] == typed_values_by_field["typed_date"]
        and args["p14"][6] == typed_values_by_field["typed_timestamp"]
    )
    assert args["p8"][8] == "null" and all(args[f"p{index}"][8] is None for index in range(9, 15))
    assert args["p2"].count(302) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, True])
async def test_invalid_persisted_count_escapes(count):
    session = Session()
    session.return_count = count
    with pytest.raises(CandidateRunnerError, match="count differs"):
        await material.persist_scalar_projections(session, definition(), root_scalars=root_rows(definition(), 1))


@pytest.mark.asyncio
async def test_internal_final_runner_check_requires_its_binding_and_is_fresh():
    session = Session()
    with pytest.raises(CandidateRunnerError, match="not bound"):
        await store.verify_materialization_authority(session)
    assert session.calls == []
    await bind_actual_runner(session)
    await store.verify_materialization_authority(session)
    assert "check_custom_import_materialization_authority" in session.calls[-1][0]


@pytest.mark.asyncio
async def test_flush_error_prevents_any_native_page():
    session = Session()
    session.flush = AsyncMock(side_effect=RuntimeError("pending flush failed"))
    with pytest.raises(RuntimeError, match="pending flush failed"):
        await material.persist_scalar_projections(session, definition(), root_scalars=root_rows(definition(), 1))
    assert session.calls == []


@pytest.mark.asyncio
async def test_internal_legacy_helpers_still_require_a_bound_runner():
    session = Session()
    with pytest.raises(CandidateRunnerError, match="not bound"):
        await store._authority(session)
    with pytest.raises(CandidateRunnerError, match="not bound"):
        await store._flush_pending(session)
    with pytest.raises(CandidateRunnerError, match="not bound"):
        await store._call(session, "synthetic_function", ())
    assert session.flushes == 0 and session.calls == []
    window = await bind_actual_runner(session)
    assert await store._authority(session) is window
    arguments = store._authority_arguments(window)
    assert tuple(kind for kind, _ in arguments) == ("bigint",) * 6 + ("bytea", "timestamptz")
    assert tuple(value for _, value in arguments) == (*window.authority, window.expires_at)
    await store._flush_pending(session)
    await store._call(session, "synthetic_function", arguments)
    assert session.flushes == 1 and ".synthetic_function(" in session.calls[-1][0]


@pytest.mark.asyncio
async def test_store_requires_a_transaction_capable_session_before_transport():
    session = Session()
    session.in_transaction = None
    with pytest.raises(TypeError, match="AsyncSession-style transaction"):
        await store._runner_window(session)
    assert session.flushes == 0 and session.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["type", "authority", "transaction"])
async def test_store_rejects_incomplete_runner_binding_before_transport(fault):
    session = Session()
    window = await bind_actual_runner(session)
    invalid = object() if fault == "type" else replace(window, **{fault: None})
    session.info[registry._MATERIALIZATION_WINDOW_KEY] = invalid
    with pytest.raises(CandidateRunnerError, match="not bound to this transaction"):
        await store._runner_window(session)
    assert session.flushes == 0 and session.calls == []


@pytest.mark.asyncio
async def test_store_does_not_invent_a_schema_or_reuse_changed_authority():
    session = Session()
    session.connection = AsyncMock(
        return_value=SimpleNamespace(
            sync_connection=SimpleNamespace(
                get_execution_options=lambda: {
                    "schema_translate_map": {store.CustomImportRootScalar.__table__.schema: None}
                }
            )
        )
    )
    with pytest.raises(CandidateRunnerError, match="explicit model schema"):
        await store._execute(session, "persist_custom_import_scalar_set", ())
    assert session.calls == []
    session = Session()
    window = await bind_actual_runner(session)
    session.info[registry._MATERIALIZATION_WINDOW_KEY] = replace(window)
    with pytest.raises(CandidateRunnerError, match="authority changed during persistence"):
        await store._page(session, "persist_custom_import_scalar_set", (), (), window)
    assert session.flushes == 0 and session.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["scalar", "winner"])
async def test_store_rejects_other_dataset_under_bound_runner_before_flush(kind):
    session = Session()
    await bind_actual_runner(session)
    contract = definition()
    if kind == "scalar":
        models = material.scalar_projection_models(contract, root_scalars=root_rows(contract, 1))
        models[0].dataset_id = 99
        operation = store.persist_scalar_models(session, models)
    else:
        result = winners(contract, 1)
        models = material.winner_materialization_models(result)
        result = replace(result, generation=replace(result.generation, dataset_id=99))
        operation = store.persist_winner_models(session, result, models)
    with pytest.raises(CandidateRunnerError, match="authority identity differs"):
        await operation
    assert session.flushes == 0 and session.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, True])
async def test_invalid_winner_count_escapes_for_caller_rollback(count):
    session = Session()
    session.return_count = count
    with pytest.raises(CandidateRunnerError, match="winner materialization persisted count differs"):
        async with session.begin():
            await material.persist_winner_materialization(session, winners(definition(), 1))
    assert session.pending_pages == [] and session.committed_pages == []
