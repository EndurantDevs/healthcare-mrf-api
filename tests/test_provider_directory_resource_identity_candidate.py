# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Component boundary proofs for isolated resource identity preparation."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest

from process import provider_directory_cms_identity_candidate as common
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_resource_identity_candidate as candidate
from process.provider_directory_resource_identity import bind_resource_identity_batch, source_resource_uuid


class Connection:
    def __init__(self, binding):
        self.binding = binding
        self.reads = []
        self.statements = []
        self.copies = []
        self.rows = []
        self.answers = []
        self.outcomes = []
        self.write_error = None

    def is_in_transaction(self):
        return True

    async def fetchval(self, query, *arguments):
        self.reads.append((query, arguments))
        if "SELECT candidate_storage_json" in query:
            return self.binding
        return self.answers.pop(0) if self.answers else False

    async def execute(self, query, *arguments):
        self.statements.append(query)
        if self.write_error is not None and "ON CONFLICT" in query:
            raise self.write_error

    async def copy_records_to_table(self, name, *, schema_name, columns, records):
        self.copies.append((name, schema_name, tuple(columns), tuple(records)))
        self.rows.extend(dict(zip(columns, record, strict=True)) for record in records)
        return f"COPY {len(records)}"

    async def cursor(self, query, *, prefetch):
        self.reads.append((query, (prefetch,)))
        for row in self.rows:
            yield row

    @asynccontextmanager
    async def transaction(self):
        before_statements = list(self.statements)
        try:
            yield
        except BaseException:
            self.outcomes.append("rollback")
            self.statements[:] = before_statements
            raise
        else:
            self.outcomes.append("commit")


def _receipt(suffix, ordinal):
    return {"name": f"pdgs_{'1' * 32}_{suffix}", "oid": ordinal, "owner": 5, "layout_sha256": "b" * 64}


@pytest.fixture
def scope(monkeypatch):
    identity_by_field = {
        "dataset_id": "candidate-one",
        "endpoint_id": "endpoint-one",
        "acquisition_root_run_id": "run-one",
        "metadata": {
            "source_ids": ["cms-npd"],
            "source_release": {"source_id": "cms-npd", "vector_sha256": "a" * 64},
            "network_bindings": {
                "source_system": "fhir",
                "source_id": "cms-npd",
                "dataset_schema": "mrf",
                "dataset_id": "candidate-one",
                "edition_id": "candidate-one",
                "producer_id": "cms-npd-bulk-fhir-v1",
                "alias_scope": "cms-npd",
                "source_key_kind": "organization_resource_id",
            },
        },
    }
    receipts_by_root = {
        root: _receipt(storage._SUFFIXES[root], i + 20) for i, root in enumerate(storage.AUXILIARY_ROOTS)
    }
    tables_by_root = {root: _receipt(storage._SUFFIXES[root], i + 40) for i, root in enumerate(storage.RELATIONS)}
    tables_by_root.update(resource_input=_receipt("ri", 10), witness_input=_receipt("wi", 11))
    binding_by_field = dict(
        contract=storage.CONTRACT,
        identity=identity_by_field,
        phase="prepared",
        ordinal=0,
        resets={},
        tables=tables_by_root,
        auxiliary={"phase": "loading", "tables": receipts_by_root, "sealed_sha256": None},
        relationship_sha256=None,
    )
    connection = Connection(binding_by_field)
    publisher = SimpleNamespace(connection=connection, policy=SimpleNamespace(schema="mrf"))
    session = object()
    for module in (candidate, common):
        monkeypatch.setattr(module, "bound_publisher", lambda: publisher)
        monkeypatch.setattr(module, "source_copy_session", lambda: session)
    monkeypatch.setattr(storage, "_close_grants", AsyncMock())
    return publisher, session, identity_by_field, receipts_by_root[candidate.ROOT]


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 125, 1000])
async def test_intake_copies_only_private_stage(scope, count):
    publisher, session, identity, receipt = scope
    resource_ids = [f"resource-{index}" for index in range(count)]
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        ids = await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=resource_ids
        )
    assert ids == [source_resource_uuid("cms-npd", "InsurancePlan", value) for value in resource_ids]
    assert len(publisher.connection.reads) == len(publisher.connection.copies) == 1
    assert publisher.connection.copies[0][:3] == (receipt["name"], "mrf", candidate.RESOURCE_IDENTITY_COLUMNS)
    assert len(publisher.connection.copies[0][3]) == count
    assert publisher.connection.statements == []
    assert all(candidate.ROOT not in query for query, _ in publisher.connection.reads)


@pytest.mark.asyncio
async def test_duplicate_replay_keeps_uuid_in_raw_stage(scope):
    publisher, session, identity, receipt = scope
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        first = await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type="PractitionerRole", resource_ids=["role", "role"]
        )
        again = await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type="PractitionerRole", resource_ids=["role"]
        )
    assert first == again * 2
    assert [len(copy[3]) for copy in publisher.connection.copies] == [1, 1]
    assert all(row["entity_id"] == first[0] for row in publisher.connection.rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_ids", [None, [], (), ["x"] * 1001, ["invalid_id"]])
async def test_invalid_batch_fails_before_database(scope, resource_ids):
    publisher, session, _, _ = scope
    with pytest.raises(ValueError):
        await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=resource_ids
        )
    assert not publisher.connection.reads and not publisher.connection.copies


@pytest.mark.asyncio
async def test_scope_and_physical_session_are_required(scope):
    publisher, session, identity, receipt = scope
    params_by_field = dict(source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["plan"])
    with pytest.raises(candidate.GenericCandidateError, match="physical_session_required"):
        await bind_resource_identity_batch(session, **params_by_field)
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        with pytest.raises(candidate.GenericCandidateError, match="physical_session_required"):
            await bind_resource_identity_batch(object(), **params_by_field)
        with pytest.raises(candidate.GenericCandidateError, match="physical_session_required"):
            await asyncio.create_task(bind_resource_identity_batch(session, **params_by_field))
    assert not publisher.connection.reads and not publisher.connection.copies


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["dataset_schema", "dataset_id", "edition_id", "producer_id", "source_id"])
async def test_full_producer_identity_is_exact(scope, field):
    publisher, _, identity, receipt = scope
    identity["metadata"]["network_bindings"][field] = "other"
    with pytest.raises(candidate.GenericCandidateError, match="source_identity_changed"):
        async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
            pytest.fail("invalid identity entered")
    assert not publisher.connection.reads


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["sealed", "extra_stage", "receipt", "identity"])
async def test_header_is_rechecked_before_copy(scope, change):
    publisher, session, identity, receipt = scope
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        header = deepcopy(publisher.connection.binding)
        if change == "sealed":
            header["auxiliary"].update(phase="sealed", sealed_sha256="c" * 64)
        elif change == "extra_stage":
            header["auxiliary"]["tables"]["unknown"] = _receipt("re", 999)
        elif change == "receipt":
            header["auxiliary"]["tables"][candidate.ROOT]["oid"] += 1
        else:
            header["identity"]["endpoint_id"] = "other"
        publisher.connection.binding = header
        with pytest.raises(candidate.GenericCandidateError):
            await bind_resource_identity_batch(
                session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["plan"]
            )
    assert not publisher.connection.copies


@pytest.mark.asyncio
async def test_finish_indexes_before_whole_family_check(scope):
    publisher, session, identity, receipt = scope
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["plan", "plan"]
        )
        await candidate.finish_resource_identity_stage(publisher, identity, receipt)
    statements = publisher.connection.statements
    assert any("GROUP BY source_id,resource_type,resource_id,entity_id" in query for query in statements)
    assert any("COALESCE(incumbent.created_at,candidate.created_at)" in query for query in statements)
    assert sum("ADD PRIMARY KEY" in query for query in statements) == 1
    assert sum("CREATE UNIQUE INDEX" in query for query in statements) == 1
    assert any("ANALYZE" in query for query in statements)
    assert publisher.connection.reads[-1][0].startswith("SELECT EXISTS")
    assert "resource_witness" not in publisher.connection.reads[-1][0]
    assert receipt["name"].removesuffix("_re") + "_w" in publisher.connection.reads[-1][0]
    assert publisher.connection.outcomes == ["commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["uuid", "incumbent", "coverage"])
async def test_finish_damage_rolls_back_stage_replacement(scope, damage):
    publisher, session, identity, receipt = scope
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["plan"]
        )
        if damage == "uuid":
            publisher.connection.rows[0]["entity_id"] = UUID(int=1)
        else:
            publisher.connection.answers[:] = [damage == "incumbent", damage == "coverage"]
        with pytest.raises(candidate.GenericCandidateError):
            await candidate.finish_resource_identity_stage(publisher, identity, receipt)
    assert publisher.connection.outcomes == ["rollback"]
    assert publisher.connection.statements == []


def _seal(publisher):
    publisher.connection.binding["auxiliary"].update(phase="sealed", sealed_sha256="c" * 64)


@pytest.mark.asyncio
async def test_publication_is_insert_select_with_immutable_replay(scope):
    publisher, _, identity, receipt = scope
    _seal(publisher)
    await candidate.publish_resource_identity_stage(publisher, identity, receipt)
    assert len(publisher.connection.statements) == 1
    query = publisher.connection.statements[0]
    assert "SELECT source_id,resource_type,resource_id,entity_id,created_at FROM" in query
    assert "ON CONFLICT(source_id,resource_type,resource_id) DO NOTHING" in query
    assert "VALUES" not in query and "UPDATE" not in query
    assert "incumbent.source_id IS NULL OR" in publisher.connection.reads[-1][0]
    assert "created_at" not in publisher.connection.reads[-1][0]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["late_conflict", "unique", "serialization", "cancel"])
async def test_publication_failure_preserves_atomic_rollback(scope, failure):
    publisher, _, identity, receipt = scope
    _seal(publisher)
    expected_error = candidate.CMSIdentityReprepareRequired
    if failure == "late_conflict":
        publisher.connection.answers[:] = [False, True]
    elif failure == "cancel":
        publisher.connection.write_error = asyncio.CancelledError()
        expected_error = asyncio.CancelledError
    else:
        publisher.connection.write_error = (
            asyncpg.UniqueViolationError("collision") if failure == "unique" else asyncpg.SerializationError("changed")
        )
    with pytest.raises(expected_error):
        await candidate.publish_resource_identity_stage(publisher, identity, receipt)
    assert publisher.connection.outcomes == ["rollback"]
    assert publisher.connection.statements == []


@pytest.mark.asyncio
async def test_wrong_observed_uuid_refuses_before_copy(scope):
    publisher, session, identity, receipt = scope
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        with pytest.raises(candidate.GenericCandidateError, match="identity_changed"):
            await candidate.stage_resource_identities(
                session,
                source_id="cms-npd",
                resource_type="InsurancePlan",
                identities_by_resource_id={"plan": UUID(int=1)},
            )
    assert not publisher.connection.copies


def test_namespace_and_canonical_encoder_are_unchanged():
    assert (
        str(source_resource_uuid("cms-npd", "InsurancePlan", "plan-example")) == "721df49c-f11d-5534-b759-42a838dd7549"
    )
    assert source_resource_uuid("cms-npd", "PractitionerRole", "plan-example") != source_resource_uuid(
        "cms-npd", "InsurancePlan", "plan-example"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_type", [asyncio.CancelledError, TimeoutError])
async def test_interrupted_copy_closes_exact_driver(scope, monkeypatch, failure_type):
    publisher, session, identity, receipt = scope
    publisher.connection.copy_records_to_table = AsyncMock(side_effect=failure_type())
    close_copy = AsyncMock()
    invalidate_owner = AsyncMock()
    monkeypatch.setattr(common, "close_interrupted_copy", close_copy)
    monkeypatch.setattr(common, "invalidate_copy_owner", invalidate_owner)
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        with pytest.raises(failure_type):
            await bind_resource_identity_batch(
                session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["plan"]
            )
    close_copy.assert_awaited_once_with(publisher.connection)
    invalidate_owner.assert_awaited_once_with(session, publisher.connection)
    assert not publisher.connection.statements


@pytest.mark.asyncio
async def test_fresh_and_replayed_batches_preserve_order_without_global_reads(scope):
    publisher, session, identity, receipt = scope
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        first = await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["existing", "existing"]
        )
        mixed = await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type="InsurancePlan", resource_ids=["fresh", "existing", "fresh"]
        )
    assert first == [source_resource_uuid("cms-npd", "InsurancePlan", "existing")] * 2
    assert mixed == [source_resource_uuid("cms-npd", "InsurancePlan", key) for key in ("fresh", "existing", "fresh")]
    assert len(publisher.connection.reads) == 2
    assert all("candidate_storage_json" in query for query, _ in publisher.connection.reads)
    assert publisher.connection.statements == []
    assert [len(copy[3]) for copy in publisher.connection.copies] == [1, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_type", ["InsurancePlan", "PractitionerRole"])
async def test_candidate_intake_preserves_type_scope_and_refuses_other_sources(scope, resource_type):
    publisher, session, identity, receipt = scope
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        ids = await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type=resource_type, resource_ids=["same-key"]
        )
        previous_copies = list(publisher.connection.copies)
        with pytest.raises(candidate.GenericCandidateError, match="batch_scope_changed"):
            await bind_resource_identity_batch(
                session, source_id="other-source", resource_type=resource_type, resource_ids=["same-key"]
            )
    assert ids == [source_resource_uuid("cms-npd", resource_type, "same-key")]
    assert ids != [source_resource_uuid("other-source", resource_type, "same-key")]
    assert publisher.connection.copies == previous_copies


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_field", ["source", "type", "resource", "entity"])
async def test_identity_binder_rejects_invalid_observed_mapping_before_copy(scope, invalid_field):
    publisher, session, identity, receipt = scope
    source_id, resource_type, key = "cms-npd", "InsurancePlan", "fresh-plan"
    entity = source_resource_uuid(source_id, resource_type, key)
    if invalid_field == "source":
        source_id = "other-source"
    elif invalid_field == "type":
        resource_type = "PractitionerRole"
    elif invalid_field == "resource":
        key = "unexpected-plan"
    else:
        entity = UUID(int=99)
    async with candidate.bind_resource_identity_candidate(publisher, identity, receipt):
        with pytest.raises(candidate.GenericCandidateError):
            await candidate.stage_resource_identities(
                session, source_id=source_id, resource_type=resource_type, identities_by_resource_id={key: entity}
            )
    assert not publisher.connection.copies
    assert not publisher.connection.reads
