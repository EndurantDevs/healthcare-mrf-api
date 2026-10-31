# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Application boundary proofs; SQL semantics still require the native fixture."""

import asyncio
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import provider_directory_cms_identity_candidate as candidate
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_entity_identity import bind_entity_batch


class Connection:
    def __init__(self, binding):
        self.binding = binding
        self.rows = {}
        self.reads = []
        self.copies = []
        self.statements = []
        self.outcomes = []
        self.late_conflict = False
        self.canonical_collision = False

    def is_in_transaction(self):
        return True

    async def fetchval(self, query, *args):
        self.reads.append((query, args))
        if "SELECT candidate_storage_json" in query:
            return self.binding
        if "INSERT INTO" in query:
            self.statements.append(query)
            return self.canonical_collision
        writes = sum("INSERT INTO" in statement for statement in self.statements)
        return bool(self.late_conflict and writes == 7 and "LEFT JOIN" in query)

    async def fetch(self, query, source, kind, ids, *release):
        self.reads.append((query, (source, kind, ids, *release)))
        table = re.search(r'FROM "mrf"\."([a-z0-9_]+)"', query).group(1)
        return [
            row
            for row in self.rows.get(table, [])
            if row["source_id"] == source
            and row["resource_type"] == kind
            and row["resource_id"] in ids
            and (not release or row["release_id"] == release[0])
        ]

    async def copy_records_to_table(self, name, *, schema_name, columns, records):
        self.copies.append((name, schema_name, tuple(columns), tuple(records)))
        self.rows.setdefault(name, []).extend(dict(zip(columns, record, strict=True)) for record in records)
        return f"COPY {len(records)}"

    async def execute(self, query, *args):
        self.statements.append(query)

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        except BaseException:
            self.outcomes.append("rollback")
            raise
        else:
            self.outcomes.append("commit")


@pytest.fixture
def scope(monkeypatch):
    identity_by_field = {
        "dataset_id": "candidate-one",
        "endpoint_id": "endpoint-one",
        "acquisition_root_run_id": "run-one",
        "metadata": {
            "source_ids": ["cms-npd"],
            "source_release": {"source_id": "cms-npd", "vector_sha256": "a" * 64},
            "network_bindings": {"producer_id": "cms-npd-bulk-fhir-v1", "alias_scope": "cms-npd"},
        },
    }
    receipts_by_root = {
        root: {"name": f"pdgs_{'1' * 32}_{suffix}", "oid": 10 + i, "owner": 5, "layout_sha256": "b" * 64}
        for i, (root, suffix) in enumerate(zip(candidate.CMS_IDENTITY_TABLES, ("eb", "ee", "nb", "ne"), strict=True))
    }

    def receipt(suffix, oid):
        return {"name": f"pdgs_{'1' * 32}_{suffix}", "oid": oid, "owner": 5, "layout_sha256": "b" * 64}

    binding_by_field = {
        "contract": storage.CONTRACT,
        "identity": identity_by_field,
        "phase": "prepared",
        "ordinal": 0,
        "resets": {},
        "tables": {
            **{root: receipt(storage._SUFFIXES[root], 30 + index) for index, root in enumerate(storage.RELATIONS)},
            "resource_input": receipt("ri", 41),
            "witness_input": receipt("wi", 42),
        },
        "relationship_sha256": None,
        "auxiliary": {
            "phase": "loading",
            "tables": {**receipts_by_root, storage.CMS_RESOURCE_IDENTITY_ROOT: receipt("re", 43)},
            "sealed_sha256": None,
        },
    }
    connection = Connection(binding_by_field)
    publisher = SimpleNamespace(connection=connection, policy=SimpleNamespace(schema="mrf"))
    session = object()
    monkeypatch.setattr(candidate, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(candidate, "source_copy_session", lambda: session)
    return publisher, session, identity_by_field, receipts_by_root


def organizations(count):
    return [{"resourceType": "Organization", "id": f"organization-{i}", "name": f"Example {i}"} for i in range(count)]


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 125])
async def test_entity_batch_has_constant_reads_and_two_bounded_private_copies(scope, count):
    publisher, session, identity, receipts = scope
    async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
        ids = await bind_entity_batch(session, source_id="cms-npd", release_id="a" * 64, resources=organizations(count))
    assert len(ids) == len(set(ids)) == count
    assert len(publisher.connection.reads) == 5
    assert [len(copy[3]) for copy in publisher.connection.copies] == [count, count]
    assert {copy[0] for copy in publisher.connection.copies} == {
        receipts[root]["name"] for root in candidate.CMS_IDENTITY_TABLES[:2]
    }
    assert publisher.connection.statements == []


@pytest.mark.asyncio
async def test_exact_batch_replay_preserves_uuid_timestamp_and_has_no_copy(scope):
    publisher, session, identity, receipts = scope
    async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
        first = await bind_entity_batch(session, source_id="cms-npd", release_id="a" * 64, resources=organizations(2))
        before_copies = list(publisher.connection.copies)
        again = await bind_entity_batch(session, source_id="cms-npd", release_id="a" * 64, resources=organizations(2))
    assert first == again
    assert publisher.connection.copies == before_copies


@pytest.mark.asyncio
async def test_published_global_id_and_created_at_are_reused(scope):
    publisher, session, identity, receipts = scope
    expected, timestamp = uuid4(), datetime(2026, 1, 1, tzinfo=timezone.utc)
    publisher.connection.rows[candidate.CMS_IDENTITY_TABLES[0]] = [
        {
            "source_id": "cms-npd",
            "resource_type": "Organization",
            "resource_id": "organization-0",
            "organization_id": expected,
            "site_id": None,
            "created_at": timestamp,
        }
    ]
    async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
        ids = await bind_entity_batch(session, source_id="cms-npd", release_id="a" * 64, resources=organizations(1))
    row = publisher.connection.rows[receipts[candidate.CMS_IDENTITY_TABLES[0]]["name"]][0]
    assert ids == [expected] and row["created_at"] == timestamp


@pytest.mark.asyncio
async def test_changed_release_replay_refuses_before_any_new_copy(scope):
    publisher, session, identity, receipts = scope
    async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
        await bind_entity_batch(session, source_id="cms-npd", release_id="a" * 64, resources=organizations(1))
        before_copies = list(publisher.connection.copies)
        with pytest.raises(candidate.GenericCandidateError, match="release_payload_conflict"):
            await bind_entity_batch(
                session, source_id="cms-npd", release_id="a" * 64, resources=[organizations(1)[0] | {"name": "Changed"}]
            )
    assert publisher.connection.copies == before_copies


@pytest.mark.asyncio
async def test_inherited_child_task_and_wrong_physical_session_refuse_before_io(scope):
    publisher, session, identity, receipts = scope
    async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
        with pytest.raises(candidate.GenericCandidateError, match="physical_session_required"):
            await asyncio.create_task(candidate.identity_candidate(session))
        with pytest.raises(candidate.GenericCandidateError, match="physical_session_required"):
            await candidate.identity_candidate(object())
    assert publisher.connection.reads == []


@pytest.mark.asyncio
async def test_sealed_native_header_refuses_more_writes(scope):
    publisher, session, identity, receipts = scope
    async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
        publisher.connection.binding["auxiliary"].update(phase="sealed", sealed_sha256="c" * 64)
        with pytest.raises(candidate.GenericCandidateError, match="protected_binding_changed"):
            await bind_entity_batch(session, source_id="cms-npd", release_id="a" * 64, resources=organizations(1))
        await candidate.identity_candidate(session, phases=("sealed",))
    assert not publisher.connection.copies


@pytest.mark.asyncio
@pytest.mark.parametrize("part", ["source_id", "release_id"])
async def test_wrong_batch_source_or_release_refuses_without_copy(scope, part):
    publisher, session, identity, receipts = scope
    params_by_field = dict(source_id="cms-npd", release_id="a" * 64, resources=organizations(1))
    params_by_field[part] = "other"
    async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
        with pytest.raises(candidate.GenericCandidateError, match="batch_scope_changed"):
            await bind_entity_batch(session, **params_by_field)
    assert not publisher.connection.copies


@pytest.mark.asyncio
async def test_wrong_producer_or_global_target_receipt_refuses(scope):
    publisher, _, identity, receipts = scope
    identity["metadata"]["network_bindings"]["producer_id"] = "other"
    with pytest.raises(candidate.GenericCandidateError, match="source_identity_changed"):
        async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
            pytest.fail("invalid binding entered")
    identity["metadata"]["network_bindings"]["producer_id"] = "cms-npd-bulk-fhir-v1"
    receipts[candidate.CMS_IDENTITY_TABLES[0]]["name"] = candidate.CMS_IDENTITY_TABLES[0]
    with pytest.raises(candidate.GenericCandidateError, match="global_target_forbidden"):
        async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
            pytest.fail("invalid binding entered")


@pytest.mark.asyncio
async def test_typed_copy_splits_at_native_byte_budget_and_rejects_one_oversize(scope, monkeypatch):
    publisher, _, _, _ = scope
    monkeypatch.setattr(candidate, "COPY_MAX_BYTES", 100)
    rows = [("x" * 30, uuid4(), datetime.now(timezone.utc)) for _ in range(3)]
    await candidate.copy_identity_rows(publisher, "private_heap", ("value", "id", "created_at"), rows)
    assert [len(copy[3]) for copy in publisher.connection.copies] == [1, 1, 1]
    with pytest.raises(candidate.GenericCandidateError, match="copy_record_too_large"):
        await candidate.copy_identity_rows(publisher, "private_heap", ("value",), [("x" * 100,)])


@pytest.mark.asyncio
async def test_copy_cancellation_drains_original_transport_and_invalidates_original_owner(scope, monkeypatch):
    publisher, session, _, _ = scope
    publisher.connection.copy_records_to_table = AsyncMock(side_effect=asyncio.CancelledError)
    close, invalidate = AsyncMock(), AsyncMock()
    monkeypatch.setattr(candidate, "close_interrupted_copy", close)
    monkeypatch.setattr(candidate, "invalidate_copy_owner", invalidate)
    with pytest.raises(asyncio.CancelledError):
        await candidate.copy_identity_rows(publisher, "private_heap", ("value",), [("x",)])
    close.assert_awaited_once_with(publisher.connection)
    invalidate.assert_awaited_once_with(session, publisher.connection)


@pytest.mark.asyncio
async def test_native_asyncpg_uuid_subclass_is_charged_and_copied_as_native_uuid(scope):
    from asyncpg.pgproto.pgproto import UUID as NativeUUID

    publisher, _, _, _ = scope
    value = NativeUUID("00000000-0000-0000-0000-000000000001")
    await candidate.copy_identity_rows(publisher, "private_heap", ("id",), [(value,)])
    assert publisher.connection.copies[0][3] == ((value,),)


@pytest.mark.asyncio
async def test_read_committed_conflict_and_repeatable_read_serialization_share_reprepare_boundary(scope):
    import asyncpg

    publisher, _, identity, receipts = scope
    publisher.connection.binding["auxiliary"].update(phase="sealed", sealed_sha256="c" * 64)
    publisher.connection.execute = AsyncMock(side_effect=asyncpg.SerializationError("synthetic concurrent winner"))
    with pytest.raises(candidate.CMSIdentityReprepareRequired):
        await candidate.publish_identity_stages(publisher, identity, receipts)
    assert publisher.connection.outcomes == ["rollback"]


def test_reference_copy_query_compiles_complete_identity_binds_with_no_insert_target():
    from sqlalchemy import text
    from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

    from process.provider_directory_cms_npd_relationship import _projection_sql

    directory = SimpleNamespace(_schema=lambda: "mrf", _qt=lambda schema, table: f'"{schema}"."{table}"')
    sql = "SELECT * FROM (" + _projection_sql(directory) + ") AS relationships"
    compiled = text(sql).compile(dialect=PGDialect_asyncpg())
    assert str(compiled).startswith("SELECT ") and "INSERT INTO" not in str(compiled)
    assert set(compiled.positiontup) == {"dataset_id", "release_id", "resource_type"}


@pytest.mark.asyncio
async def test_late_global_disagreement_is_retryable_and_exits_the_complete_savepoint(scope):
    publisher, _, identity, receipts = scope
    publisher.connection.late_conflict = True
    publisher.connection.binding["auxiliary"].update(phase="sealed", sealed_sha256="c" * 64)
    with pytest.raises(candidate.CMSIdentityReprepareRequired, match="whole_candidate_reprepare"):
        await candidate.publish_identity_stages(publisher, identity, receipts)
    assert publisher.connection.outcomes == ["rollback"]
    assert sum("INSERT INTO" in s for s in publisher.connection.statements) == 7


@pytest.mark.asyncio
async def test_successful_global_publication_has_one_savepoint_and_seven_set_writes(scope):
    publisher, _, identity, receipts = scope
    publisher.connection.binding["auxiliary"].update(phase="sealed", sealed_sha256="c" * 64)
    await candidate.publish_identity_stages(publisher, identity, receipts)
    assert publisher.connection.outcomes == ["commit"]
    assert sum("INSERT INTO" in s for s in publisher.connection.statements) == 7


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 125])
async def test_network_sql_targets_only_bound_private_stages_at_constant_statement_count(scope, monkeypatch, count):
    from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

    from process import provider_directory_insurance_network_batch as network

    publisher, session, identity, receipts = scope
    sql_statements = []

    class SQLConnection:
        dialect = PGDialect_asyncpg()

        async def execute(self, statement, parameters=None):
            sql_statements.append(str(statement))
            return SimpleNamespace(one=lambda: (0, count))

        async def scalar(self, statement, parameters=None):
            sql_statements.append(str(statement))
            return None if "release_plan_conflict" in str(statement) else count

    from process import provider_directory_generic_source_policy as policy

    monkeypatch.setattr(policy, "source_copy_session", lambda: session)
    copy = AsyncMock()
    monkeypatch.setattr(network, "copy_network_stage_native", copy)
    async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
        receipt = await network._write_batch(
            SQLConnection(),
            (b"synthetic transport", count, count, 0),
            {
                "source_id": "cms-npd",
                "release_id": "a" * 64,
                "resource_type": "InsurancePlan",
                "network_resource_id": None,
            },
        )
    assert receipt["plan_link_count"] == count
    assert len(sql_statements) == 9
    copy.assert_awaited_once()
    assert copy.await_args.args[1:3] == (session, publisher.connection)
    writes = [statement for statement in sql_statements if "INSERT INTO" in statement]
    assert len(writes) == 2
    assert all(
        any(f'INSERT INTO "mrf"."{receipts[root]["name"]}"' in statement for root in candidate.CMS_IDENTITY_TABLES[2:])
        for statement in writes
    )
    assert not any("pg_advisory" in statement or "FOR KEY SHARE" in statement for statement in sql_statements)
    assert not any(
        f'INSERT INTO "mrf"."{root}"' in statement for root in candidate.CMS_IDENTITY_TABLES for statement in writes
    )


@pytest.mark.asyncio
async def test_relationship_indexes_are_sealed_after_copy_before_complete_read_and_receipt(monkeypatch):
    from process import provider_directory_cms_npd_relationship as relationship
    from process import provider_directory_generic_source_auxiliary as auxiliary
    from process import provider_directory_generic_source_policy as policy
    from process import provider_directory_source_cms_epoch_control as controls

    events = []

    async def seal_identity(*args):
        events.append("identity seal")

    async def copy_pages(*args, **kwargs):
        events.append("copy all pages")
        return 5

    async def seal_relationship(*args):
        events.append("relationship indexes and seal")

    async def complete(*args):
        events.append("indexed completeness read")
        return 5

    async def store_receipt(*args, **kwargs):
        events.append("receipt")

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        assert kwargs == {"isolation": "repeatable read"}
        yield

    directory = SimpleNamespace(
        db=SimpleNamespace(scalar=AsyncMock(return_value=False), status=store_receipt),
        _schema=lambda: "mrf",
        _qt=lambda schema, table: f'"{schema}"."{table}"',
    )
    source_candidate = SimpleNamespace(dataset_id="candidate-one", already_validated=False, already_published=False)
    monkeypatch.setattr(relationship, "completed_receipt_count", AsyncMock(side_effect=[None, 5]))
    monkeypatch.setattr(relationship, "_project_families", copy_pages)
    monkeypatch.setattr(relationship, "assert_complete", complete)
    monkeypatch.setattr(auxiliary, "seal_identity_candidate", seal_identity)
    monkeypatch.setattr(auxiliary, "seal_relationship_candidate", seal_relationship)
    monkeypatch.setattr(policy, "source_transaction", transaction)
    monkeypatch.setattr(policy, "bound_publisher", lambda: SimpleNamespace(connection=object(), policy=object()))
    monkeypatch.setattr(controls, "capture_source_cms_epoch_controls", AsyncMock())
    monkeypatch.setattr(relationship, "_assert_relationship_sealed", AsyncMock())
    await relationship.materialize(directory, source_candidate, "a" * 64)
    assert events == [
        "identity seal",
        "copy all pages",
        "relationship indexes and seal",
        "indexed completeness read",
        "receipt",
    ]


@pytest.mark.asyncio
async def test_relationship_unsealed_rows_require_whole_reprepare_without_delete_or_copy(monkeypatch):
    from process import provider_directory_cms_npd_relationship as relationship
    from process import provider_directory_generic_source_auxiliary as auxiliary

    directory = SimpleNamespace(
        db=SimpleNamespace(scalar=AsyncMock(return_value=True), status=AsyncMock()),
        _schema=lambda: "mrf",
        _qt=lambda schema, table: f'"{schema}"."{table}"',
    )
    source_candidate = SimpleNamespace(dataset_id="candidate-one", already_validated=False, already_published=False)
    copy_pages = AsyncMock()
    monkeypatch.setattr(relationship, "completed_receipt_count", AsyncMock(return_value=None))
    monkeypatch.setattr(relationship, "_project_families", copy_pages)
    monkeypatch.setattr(auxiliary, "seal_identity_candidate", AsyncMock())
    with pytest.raises(RuntimeError, match="unsealed_candidate_reprepare"):
        await relationship.materialize(directory, source_candidate, "a" * 64)
    copy_pages.assert_not_awaited()
    directory.db.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancellation_after_partial_global_writes_exits_savepoint_without_remap(scope):
    publisher, _, identity, receipts = scope
    publisher.connection.binding["auxiliary"].update(phase="sealed", sealed_sha256="c" * 64)
    original_execute = publisher.connection.execute

    async def cancel_after_first_write(statement, *arguments):
        await original_execute(statement, *arguments)
        if statement.startswith("INSERT INTO"):
            raise asyncio.CancelledError

    publisher.connection.execute = cancel_after_first_write
    with pytest.raises(asyncio.CancelledError):
        await candidate.publish_identity_stages(publisher, identity, receipts)
    assert publisher.connection.outcomes == ["rollback"]
    assert sum("INSERT INTO" in statement for statement in publisher.connection.statements) == 4


@pytest.mark.asyncio
async def test_forced_different_key_canonical_uuid_winner_requires_whole_candidate_reprepare(scope):
    publisher, _, identity, receipts = scope
    publisher.connection.binding["auxiliary"].update(phase="sealed", sealed_sha256="c" * 64)
    publisher.connection.canonical_collision = True
    with pytest.raises(candidate.CMSIdentityReprepareRequired, match="whole_candidate_reprepare"):
        await candidate.publish_identity_stages(publisher, identity, receipts)
    assert publisher.connection.outcomes == ["rollback"]
    assert len(publisher.connection.statements) == 1
    statement = publisher.connection.statements[0]
    assert "WITH needed AS MATERIALIZED" in statement
    assert "ON CONFLICT" in statement and "DO NOTHING RETURNING" in statement
    assert "count(DISTINCT" in statement and "FROM inserted" in statement
    assert not any(statement.startswith("INSERT INTO") for statement in publisher.connection.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_table", [0, 1, 2])
@pytest.mark.parametrize("incumbent_changed", [False, True])
async def test_canonical_authority_rechecks_drift_without_accepting_uuid_reuse(
    scope, monkeypatch, invalid_table, incumbent_changed
):
    publisher, _, _, receipts = scope
    publisher.connection.fetchval = AsyncMock(side_effect=[False] * invalid_table + [True])
    changed = candidate.CMSIdentityReprepareRequired("whole_candidate_reprepare_required")
    replay = AsyncMock(side_effect=changed if incumbent_changed else None)
    monkeypatch.setattr(candidate, "verify_identity_replay", replay)
    expected = candidate.CMSIdentityReprepareRequired if incumbent_changed else candidate.GenericCandidateError
    message = "whole_candidate_reprepare" if incumbent_changed else "canonical_identity_reuse_forbidden"
    with pytest.raises(expected, match=message):
        await candidate._canonical_authority(publisher.connection, publisher.policy, receipts)
    replay.assert_awaited_once_with(publisher.connection, candidate._relations(publisher.policy, receipts), "mrf")
    assert publisher.connection.fetchval.await_count == invalid_table + 1
    assert publisher.connection.statements == publisher.connection.copies == []


@pytest.mark.asyncio
async def test_valid_canonical_authority_does_not_repeat_global_reads(scope, monkeypatch):
    publisher, _, _, receipts = scope
    replay = AsyncMock()
    monkeypatch.setattr(candidate, "verify_identity_replay", replay)
    await candidate._canonical_authority(publisher.connection, publisher.policy, receipts)
    replay.assert_not_awaited()
    assert len(publisher.connection.reads) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing_fifth", "untracked_extra", "wrong_fourth"])
async def test_complete_five_stage_binding_is_required_before_any_identity_copy(scope, change):
    publisher, session, identity, receipts = scope
    tables = publisher.connection.binding["auxiliary"]["tables"]
    if change == "missing_fifth":
        tables.pop(storage.CMS_RESOURCE_IDENTITY_ROOT)
    elif change == "untracked_extra":
        tables["untracked"] = dict(tables[storage.CMS_RESOURCE_IDENTITY_ROOT])
    else:
        tables[candidate.CMS_IDENTITY_TABLES[-1]] = {**tables[candidate.CMS_IDENTITY_TABLES[-1]], "oid": 99}
    async with candidate.bind_cms_identity_candidate(publisher, identity, receipts):
        with pytest.raises(Exception, match="auxiliary_binding_changed|protected_binding_changed"):
            await bind_entity_batch(session, source_id="cms-npd", release_id="a" * 64, resources=organizations(1))
    assert not publisher.connection.copies


@pytest.mark.asyncio
async def test_finished_source_checks_authenticate_the_four_root_projection_of_five(scope):
    publisher, _, identity, receipts = scope
    await candidate._complete_source_facts(publisher.connection, publisher.policy, identity, receipts)
    assert len(publisher.connection.reads) == 6
    assert not publisher.connection.copies
