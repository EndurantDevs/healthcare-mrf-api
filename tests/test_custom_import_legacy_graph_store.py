# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Normal-import host checks for protected legacy pack and rejection callers."""

from __future__ import annotations

import time
from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.orm import Session

from db.models.custom_import import CustomImportDataset, CustomImportExecution, CustomImportLease
from process.custom_import import legacy_graph_store as store
from process.custom_import import runner_graph as graph
from process.custom_import import runner_registry as registry
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import LeaseGrant, lease_token_sha256
from process.custom_import.family import FamilyBuildResult, FamilyRejection, RootFamily
from process.custom_import.runner_codec import family_child_payload_hashes, root_payload_hash
from process.custom_import.runner_types import (
    CancellationRequested,
    CandidateRegistry,
    CandidateRunnerError,
    CandidateRunRequest,
    LeaseAuthorityLost,
)

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"


class _Session:
    def __init__(self, request):
        self.transaction = object()
        self.info = {}
        self.no_autoflush = nullcontext()
        self.calls = []
        self.flushes = 0
        self.now = datetime.now(UTC)
        self.active = True
        self.fail_set = None
        self.set_count = 0
        self.pack_result = None
        self.rejection_result = None
        self.dataset = CustomImportDataset(dataset_id=request.dataset_id)
        self.execution = CustomImportExecution(
            execution_id=request.execution_id,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            capture_bundle_id=51,
            state="running",
        )
        self.lease = CustomImportLease(
            execution_id=request.execution_id,
            fence=2,
            token_sha256=lease_token_sha256(request.lease_token),
            expires_at=self.now + timedelta(seconds=60),
        )

    def in_transaction(self):
        return self.active

    async def connection(self):
        return SimpleNamespace(
            dialect=dialect(),
            get_transaction=lambda: self.transaction,
            info={},
            sync_connection=SimpleNamespace(
                get_execution_options=lambda: {
                    "schema_translate_map": {store.CustomImportPack.__table__.schema: "synthetic schema"},
                }
            ),
        )

    async def scalar(self, statement):
        assert "clock_timestamp" in str(statement)
        return self.now

    async def flush(self):
        self.flushes += 1

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.calls.append((sql, parameters))
        if "persist_custom_import_legacy_" in sql:
            self.set_count += 1
            if self.fail_set == self.set_count:
                raise RuntimeError("synthetic protected-set failure")
            if "legacy_pack_set" in sql:
                value = self.pack_result if self.pack_result is not None else [7000 + slot for slot in parameters["p8"]]
            else:
                value = self.rejection_result if self.rejection_result is not None else len(parameters["p8"])
            return SimpleNamespace(scalar_one=lambda: value)
        if sql.startswith("UPDATE "):
            value = self.now + timedelta(seconds=registry._MATERIALIZATION_LEASE_WINDOW_SECONDS)
        elif "custom_import_dataset" in sql:
            value = self.dataset
        elif "custom_import_execution" in sql:
            value = self.execution
        elif "custom_import_lease" in sql:
            value = self.lease
        else:
            value = None
        return SimpleNamespace(scalar_one=lambda: value, scalar_one_or_none=lambda: value)


def _request():
    return CandidateRunRequest(
        dataset_id=11,
        definition_revision_id=21,
        schema_revision_id=31,
        execution_id=41,
        lease_token="synthetic-legacy-lease",
        definition=CustomImportDefinition.from_json(_FIXTURE.read_text()),
        roots=(),
        children_by_collection={},
    )


def _grant(session):
    return LeaseGrant(execution_id=41, fence=2, expires_at=session.lease.expires_at, state="running")


def _registry():
    # Deliberately not insertion-ordered by stream slot: returned ID correlation
    # must follow the input ordinality, not a database sort assumption.
    return CandidateRegistry(
        child_collection_slots={"rates": 7},
        stream_slots={"providers": 9, "rates": 3},
        root_stream_slot=9,
    )


async def _bind(session, request):
    await registry.lock_dataset(session, request.dataset_id)
    execution = await registry.lock_execution(session, request)
    lease = await registry.lock_lease(session, request.execution_id)
    grant = _grant(session)
    now = await registry.verify_live_attempt(session, request, grant, execution, lease)
    await registry.establish_materialization_authority(session, request, grant, execution, lease, now)


def _sets(session, kind):
    return [(sql, parameters) for sql, parameters in session.calls if f"persist_custom_import_legacy_{kind}_set" in sql]


def _page_keys(page):
    return (page["p13"],) if len(page["p8"]) == 1 else page["p9"]


def _family():
    return RootFamily(
        root_key=("1234567893",),
        root={"npi": "1234567893", "display_name": "Synthetic"},
        children={
            "rates": (
                {"rate_npi": "1234567893", "service_code": "b", "amount": Decimal("2.50")},
                {"rate_npi": "1234567893", "service_code": "a", "amount": None},
            )
        },
    )


def _admitted(rejections):
    return FamilyBuildResult(families=(), rejections=tuple(rejections), candidate_errors=())


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [True, False])
async def test_create_packs_keeps_existing_codec_and_empty_streams(empty):
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    selected = () if empty else (_family(),)
    expected = graph.pack_models(
        request,
        _grant(session),
        _registry(),
        51,
        [root_payload_hash(request.definition, family) for family in selected],
        {}
        if empty
        else {"rates": [value for _, value in family_child_payload_hashes(request.definition, selected[0])]},
    )
    actual = await graph.create_packs(session, request, _grant(session), _registry(), selected, 51)
    assert tuple(actual) == (None, "rates")
    assert [row.pack_id for row in actual.values()] == [7009, 7003]
    assert [row.pack_ordinal for row in actual.values()] == [0, 0]
    assert [row.record_count for row in actual.values()] == ([0, 0] if empty else [1, 2])
    assert [row.pack_sha256 for row in actual.values()] == [row.pack_sha256 for row in expected.values()]
    assert all(inspect(row).transient for row in actual.values())
    assert session.flushes == 1 and len(_sets(session, "pack")) == 1
    sql, arguments = _sets(session, "pack")[0]
    assert '"synthetic schema".persist_custom_import_legacy_pack_set' in sql
    assert arguments["p8"] == (9, 3) and arguments["p9"] == (0, 0)
    assert arguments["p6"] == lease_token_sha256(request.lease_token)
    assert "check_custom_import_materialization_authority" in session.calls[-1][0]


@pytest.mark.asyncio
async def test_pack_retry_retains_natural_identity_and_requires_same_returned_ids():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    packs = graph.pack_models(request, _grant(session), _registry(), 51, (), {})
    await store.persist_pack_models(session, packs)
    await store.persist_pack_models(session, packs)
    assert _sets(session, "pack")[0][1] == _sets(session, "pack")[1][1]
    session.pack_result = [8009, 8003]
    with pytest.raises(CandidateRunnerError, match="returned identity differs"):
        await store.persist_pack_models(session, packs)
    assert [row.pack_id for row in packs.values()] == [7009, 7003]


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, store.MAX_CHILD_COLLECTIONS + 2])
async def test_pack_stream_bounds_precede_flush(count):
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    pack = graph.pack_models(request, _grant(session), _registry(), 51, (), {})[None]
    with pytest.raises(CandidateRunnerError, match="pack stream count differs"):
        await store.persist_pack_models(session, dict.fromkeys(range(count), pack))
    assert session.flushes == 0 and _sets(session, "pack") == []
    assert pack.pack_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_type", [False, True])
async def test_pack_capture_scope_precedes_flush(wrong_type):
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    packs = graph.pack_models(request, _grant(session), _registry(), 51, (), {})
    if wrong_type:
        packs["rates"] = object()
    else:
        packs["rates"].capture_bundle_id = 52
    with pytest.raises(CandidateRunnerError, match="pack capture identity differs"):
        await store.persist_pack_models(session, packs)
    assert session.flushes == 0 and _sets(session, "pack") == []
    assert packs[None].pack_id is None


@pytest.mark.asyncio
async def test_duplicate_pack_positions_precede_flush():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    packs = graph.pack_models(request, _grant(session), _registry(), 51, (), {})
    packs["rates"].stream_slot = packs[None].stream_slot
    with pytest.raises(CandidateRunnerError, match="pack identities repeat"):
        await store.persist_pack_models(session, packs)
    assert session.flushes == 0 and _sets(session, "pack") == []
    assert all(pack.pack_id is None for pack in packs.values())


@pytest.mark.asyncio
async def test_rejections_keep_ordered_evidence_and_null_source_positions():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    admitted = _admitted(
        [
            FamilyRejection(("z\u2603",), "field_type_invalid"),
            FamilyRejection(None, "root_key_invalid"),
            FamilyRejection(("a",), "child_parent_invalid"),
            FamilyRejection(("a",), "field_type_invalid"),
        ]
    )
    expected_models = [
        graph.rejection_model(request, _grant(session), ordinal, value)
        for ordinal, value in enumerate(
            sorted(admitted.rejections, key=lambda value: (repr(value.root_key), value.code))
        )
    ]
    await graph.persist_rejections(session, request, _grant(session), admitted)
    assert len(_sets(session, "rejection")) == 1 and session.flushes == 1
    sql, arguments = _sets(session, "rejection")[0]
    attributes = ("rejection_ordinal", "canonical_root_key", "root_key_sha256", "code", "canonical_evidence")
    for index, attribute in enumerate(attributes, start=8):
        assert arguments[f"p{index}"] == tuple(getattr(row, attribute) for row in expected_models)
    assert all(
        row.pack_id is row.collection_slot is row.source_ordinal is row.field_slot is None for row in expected_models
    )
    assert "CAST(:p8 AS bigint[])" in sql and "CAST(:p10 AS bytea[])" in sql
    assert "check_custom_import_materialization_authority" in session.calls[-1][0]


@pytest.mark.asyncio
async def test_rejection_rows_use_bounded_set_calls_without_ordinal_restart():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    admitted = _admitted(FamilyRejection((str(value),), "field_type_invalid") for value in range(513))
    await graph.persist_rejections(session, request, _grant(session), admitted)
    pages = [arguments for _, arguments in _sets(session, "rejection")]
    assert [len(arguments["p8"]) for arguments in pages] == [256, 256, 1]
    assert tuple(value for page in pages for value in page["p8"]) == tuple(range(513))
    assert session.flushes == 1


@pytest.mark.asyncio
async def test_rejection_byte_pages_measure_utf8_and_preserve_full_keys():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    key = "\u2603" * 366_667
    admitted = _admitted(FamilyRejection((key + str(index),), "field_type_invalid") for index in range(8))
    await graph.persist_rejections(session, request, _grant(session), admitted)
    pages = [arguments for _, arguments in _sets(session, "rejection")]
    assert [len(arguments["p8"]) for arguments in pages] == [7, 1]
    assert all(key in text for page in pages for text in _page_keys(page))
    for page in pages:
        cost = 1024 + sum(
            128 + len(code) + len(evidence.encode()) + len(root.encode()) + len(digest)
            for root, digest, code, evidence in zip(
                _page_keys(page),
                page["p10"],
                page["p11"],
                page["p12"],
                strict=True,
            )
        )
        assert cost <= store._REJECTION_PAGE_BYTES


@pytest.mark.asyncio
async def test_oversized_accepted_rejection_key_uses_same_protected_set_entrypoint():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    # The unchanged codec really accepts this root key; the projected scalar
    # 2,048-byte rule cannot be claimed as its existing storage bound.
    rejection = FamilyRejection(("x" * store._REJECTION_PAGE_BYTES,), "field_type_invalid")
    row = graph.rejection_model(request, _grant(session), 0, rejection)
    assert len(row.canonical_root_key.encode()) > store._REJECTION_PAGE_BYTES
    await graph.persist_rejections(session, request, _grant(session), _admitted([rejection]))
    [(sql, arguments)] = _sets(session, "rejection")
    assert arguments["p9"] == (None,) and arguments["p13"] == row.canonical_root_key
    assert arguments["p10"] == (row.root_key_sha256,) and arguments["p12"] == (row.canonical_evidence,)
    assert "CAST(:p13 AS text)" in sql and session.flushes == 1
    assert "check_custom_import_materialization_authority" in session.calls[-1][0]


@pytest.mark.asyncio
async def test_oversized_middle_key_is_isolated_without_restarting_ordinals():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    rejections = [
        FamilyRejection((key,), "field_type_invalid") for key in ("a", "b" * store._REJECTION_PAGE_BYTES, "c")
    ]
    await graph.persist_rejections(session, request, _grant(session), _admitted(rejections))
    pages = [arguments for _, arguments in _sets(session, "rejection")]
    assert [page["p8"] for page in pages] == [(0,), (1,), (2,)]
    assert all(page["p9"] == (None,) for page in pages)
    assert all(len(_page_keys(page)) == 1 for page in pages)
    assert len(pages[1]["p13"].encode()) > store._REJECTION_PAGE_BYTES


@pytest.mark.asyncio
async def test_all_rejection_rows_validate_before_any_page_flush():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    rows = [
        graph.rejection_model(request, _grant(session), index, FamilyRejection(None, "root_key_invalid"))
        for index in range(257)
    ]
    rows[-1].producing_fence = 3
    with pytest.raises(CandidateRunnerError, match="authority or pending state differs"):
        await store.persist_rejection_models(session, rows)
    assert session.flushes == 0 and _sets(session, "rejection") == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("attribute", "value", "message"),
    [
        (None, None, "row is malformed"),
        ("pack_id", 71, "identity or evidence differs"),
        ("rejection_ordinal", True, "identity or evidence differs"),
        ("code", "invalid code", "identity or evidence differs"),
        ("canonical_evidence", None, "identity or evidence differs"),
        ("root_key_sha256", b"x" * 32, "identity or evidence differs"),
        ("canonical_evidence", "\u2603" * 86, "evidence exceeds its codec bound"),
    ],
)
async def test_late_rejection_shape_precedes_flush(attribute, value, message):
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    rejections = [
        graph.rejection_model(request, _grant(session), index, FamilyRejection(None, "root_key_invalid"))
        for index in range(257)
    ]
    if attribute is None:
        rejections[-1] = object()
    else:
        setattr(rejections[-1], attribute, value)
    with pytest.raises(CandidateRunnerError, match=message):
        await store.persist_rejection_models(session, rejections)
    assert session.flushes == 0 and _sets(session, "rejection") == []


@pytest.mark.asyncio
async def test_duplicate_rejection_ordinals_precede_flush():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    rejections = [
        graph.rejection_model(request, _grant(session), 0, FamilyRejection(key, "field_type_invalid"))
        for key in (("first",), ("second",))
    ]
    with pytest.raises(CandidateRunnerError, match="rejection ordinals repeat"):
        await store.persist_rejection_models(session, rejections)
    assert session.flushes == 0 and _sets(session, "rejection") == []


@pytest.mark.asyncio
async def test_pending_models_cannot_leak_into_an_orm_flush():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    packs = graph.pack_models(request, _grant(session), _registry(), 51, (), {})
    with Session() as unrelated_session:
        unrelated_session.add(packs[None])
        with pytest.raises(CandidateRunnerError, match="pending state differs"):
            await store.persist_pack_models(session, packs)
    assert session.flushes == 0 and _sets(session, "pack") == []


@pytest.mark.asyncio
async def test_lost_authority_cannot_fall_back_to_direct_writes():
    request = _request()
    session = _Session(request)
    session.execution.state = "canceling"
    with pytest.raises(CancellationRequested):
        await _bind(session, request)
    session.execution.state = "running"
    session.lease.token_sha256 = lease_token_sha256("different-synthetic-token")
    with pytest.raises(LeaseAuthorityLost):
        await _bind(session, request)
    session.lease.token_sha256 = lease_token_sha256(request.lease_token)
    await _bind(session, request)
    window = session.info[registry._MATERIALIZATION_WINDOW_KEY]
    session.info[registry._MATERIALIZATION_WINDOW_KEY] = replace(window, monotonic_deadline=time.monotonic() - 1)
    with pytest.raises(LeaseAuthorityLost, match="window expired"):
        await graph.create_packs(session, request, _grant(session), _registry(), (), 51)
    assert session.flushes == 0 and session.set_count == 0


@pytest.mark.asyncio
async def test_second_page_failure_propagates_to_caller_transaction():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    session.fail_set = 2
    admitted = _admitted(FamilyRejection((str(index),), "field_type_invalid") for index in range(600))
    with pytest.raises(RuntimeError, match="synthetic protected-set failure"):
        await graph.persist_rejections(session, request, _grant(session), admitted)
    assert len(_sets(session, "rejection")) == 2 and session.flushes == 1
    assert "check_custom_import_materialization_authority" not in session.calls[-1][0]
    # The fake has no transaction begin/commit/rollback or ORM add methods.
    # All page failures must escape to the real caller's transaction envelope.


@pytest.mark.asyncio
async def test_empty_rejection_set_rechecks_authority_without_inserting():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    await graph.persist_rejections(session, request, _grant(session), _admitted([]))
    assert session.flushes == 0 and _sets(session, "rejection") == []
    assert "check_custom_import_materialization_authority" in session.calls[-1][0]


@pytest.mark.asyncio
async def test_rejection_response_mismatch_is_not_success():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    session.rejection_result = 0
    with pytest.raises(CandidateRunnerError, match="persisted count differs"):
        await graph.persist_rejections(
            session,
            request,
            _grant(session),
            _admitted([FamilyRejection(None, "root_key_invalid")]),
        )
