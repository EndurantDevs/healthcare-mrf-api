"""Candidate orchestration checks; native catalog and permission acceptance is separate."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import nucc_protected_stage as stage

DATASET = UUID("11111111-1111-4111-8111-111111111111")


def ownership():
    return stage.native.ReferenceFamilyStageOwnership(
        "nucc",
        DATASET,
        stage.native.reference_family_stage_schema(DATASET),
        301,
        (("nucc_taxonomy", 201),),
    )


class Transaction:
    def in_transaction(self):
        return self

    @asynccontextmanager
    async def begin_nested(self):
        yield self

    async def connection(self):
        return self

    async def execute(self, statement):
        assert str(statement).startswith("LOCK TABLE ONLY ") and "IN SHARE MODE NOWAIT" in str(statement)


@pytest.mark.asyncio
async def test_precreate_defers_all_indexes_and_seals_empty_model(monkeypatch):
    events = []
    candidate = ownership()

    async def owner(_session):
        return 101

    async def create(_session, spec, schema, **options):
        assert spec.importer_id == "nucc" and schema == candidate.schema_name
        assert options == dict(create_indexes=False, ordinary_heaps=True, protected_owner_oid=101)
        events.append("empty-model-without-indexes")

    async def capture(_session, spec, dataset):
        assert spec.importer_id == "nucc" and dataset == DATASET
        return candidate

    async def seal(_session, actual, owner_oid, *, require_empty):
        assert actual is candidate and owner_oid == 101 and require_empty is True
        events.append("closed-owner-heaps")

    monkeypatch.setattr(stage.native, "protected_publisher_owner", owner)
    monkeypatch.setattr(stage.native, "_create_model_family", create)
    monkeypatch.setattr(stage.native, "capture_model_family_stage_ownership", capture)
    monkeypatch.setattr(stage.native, "seal_model_family_storage", seal)
    assert await stage.precreate_nucc_stage(Transaction(), dataset_id=DATASET) is candidate
    assert events == ["empty-model-without-indexes", "closed-owner-heaps"]


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset", [str(DATASET), None, 101])
async def test_precreate_refuses_non_native_dataset(monkeypatch, dataset):
    with pytest.raises(stage.native.ReferenceFamilyArchiveError, match="dataset differs"):
        await stage.precreate_nucc_stage(Transaction(), dataset_id=dataset)


@pytest.mark.asyncio
@pytest.mark.parametrize("records", [[], (), [None] * 5001, {"code": "111111111A"}])
async def test_copy_refuses_outside_bounded_batch_before_native_io(records):
    with pytest.raises(stage.native.ReferenceFamilyArchiveError, match="batch exceeds bound"):
        await stage.copy_nucc_stage(Transaction(), ownership(), records)


@pytest.mark.asyncio
async def test_finish_closes_writes_then_indexes_then_set_validation(monkeypatch):
    events = []
    candidate = ownership()

    async def owner(_session):
        return 101

    async def verify(_session, actual):
        assert actual is candidate

    async def close(_session, actual, owner_oid, source):
        assert actual is candidate and owner_oid == 101 and source is None
        events.append("close-writes")

    async def indexes(_session, actual):
        assert actual is candidate
        events.append("indexes")

    async def validate(_session, *, ownership):
        assert ownership is candidate
        events.append("set-validation")
        return {"row_count": 4}

    async def incoming(_session, oid, owner_oid):
        assert (oid, owner_oid) == (201, 101)
        return {"oid": oid}

    async def content(*_arguments, **_keywords):
        return 4, "a" * 64

    async def schema(*_arguments):
        return "b" * 64

    for name, callback in (
        ("protected_publisher_owner", owner),
        ("verify_reference_family_stage_ownership", verify),
        ("complete_reference_family_restore", indexes),
        ("validate_nucc_reference_set", validate),
        ("_projected_row_identity", content),
        ("_family_schema_identity", schema),
    ):
        monkeypatch.setattr(stage.native, name, callback)
    monkeypatch.setattr(stage, "_close_candidate_grants", close)
    monkeypatch.setattr(stage.partition, "_capture_incoming", incoming)
    finished_by_field = await stage.finish_nucc_stage(Transaction(), candidate)
    assert events == ["close-writes", "indexes", "set-validation"]
    assert finished_by_field["validation"] == {"row_count": 4} and finished_by_field["semantic_sha256"] == "a" * 64
    events.clear()
    assert await stage.revalidate_nucc_stage(Transaction(), candidate) == finished_by_field
    assert await stage.revalidate_nucc_stage(Transaction(), candidate) == finished_by_field
    assert events == ["set-validation", "set-validation"]


class CandidateGrants:
    def __init__(self, edges):
        self.edges, self.statements = edges, []

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        if "aclexplode" in sql:
            assert parameters == {"oid": 201}
            return self
        self.statements.append(sql)

    def all(self):
        return self.edges

    async def scalar(self, statement, parameters=None):
        sql = str(statement)
        if "quote_ident" in sql:
            assert parameters == {"oid": 103}
            return '"source_builder"'
        assert sql == "SELECT (SELECT oid FROM pg_roles WHERE rolname=current_user)::bigint"
        return 102


def candidate_acl(*, source=False):
    edges = [(101, 101, right, False) for right in sorted(stage.partition._OWNER_RIGHTS)]
    edges.append((102, 101, "SELECT", False))
    return edges + ([(103, 101, "INSERT", False)] if source else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("source", [False, True])
async def test_close_preserves_only_actual_publisher_read_and_closes_source_insert(monkeypatch, source):
    session = CandidateGrants(candidate_acl(source=source))
    source_session = object() if source else None

    async def actor(publisher, actual_source, owner):
        assert publisher is session and actual_source is source_session and owner == 101
        return {"actor_oid": 103}

    async def seal(publisher, oid, owner):
        assert publisher is session and (oid, owner) == (201, 101)
        session.statements.append("seal")

    monkeypatch.setattr(stage, "_source_actor", actor)
    monkeypatch.setattr(stage, "_seal_published_relation", seal)
    await stage._close_candidate_grants(session, ownership(), 101, source_session)
    assert session.statements[0].startswith("LOCK TABLE ONLY ") and "ACCESS EXCLUSIVE" in session.statements[0]
    assert session.statements[-1] == "seal"
    assert any(sql.startswith("REVOKE INSERT ON ") for sql in session.statements) is source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "edge",
    [
        (104, 101, "SELECT", False),
        (102, 101, "SELECT", True),
        (102, 104, "SELECT", False),
        (102, 101, "UPDATE", False),
        (103, 101, "SELECT", False),
    ],
)
async def test_close_refuses_foreign_reader_option_grantor_or_mutation(monkeypatch, edge):
    session = CandidateGrants(candidate_acl(source=True) + [edge])

    async def actor(*_arguments):
        return {"actor_oid": 103}

    monkeypatch.setattr(stage, "_source_actor", actor)
    with pytest.raises(stage.native.ReferenceFamilyArchiveError, match="candidate grants differ"):
        await stage._close_candidate_grants(session, ownership(), 101, object())
    assert session.statements == []


@pytest.fixture
def candidate_grants(monkeypatch):
    class Session:
        publisher_oid = 102

        def __init__(self):
            rights = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN")
            self.entries = [(101, 101, right, False) for right in rights] + [(102, 101, "SELECT", False)]
            self.events = []

        async def execute(self, statement, parameters=None):
            if parameters is not None:
                assert parameters == {"oid": 201} and "aclexplode" in str(statement)
                return SimpleNamespace(all=lambda: self.entries)
            assert "LOCK TABLE ONLY" in str(statement) and "ACCESS EXCLUSIVE" in str(statement)
            self.events.append("locked")

        async def scalar(self, statement):
            assert str(statement) == "SELECT (SELECT oid FROM pg_roles WHERE rolname=current_user)::bigint"
            return self.publisher_oid

    async def seal(session, oid, owner_oid):
        assert (oid, owner_oid) == (201, 101) and session.events == ["locked"]
        session.events.append("sealed")

    monkeypatch.setattr(stage, "_seal_published_relation", seal)
    return Session()


@pytest.mark.asyncio
async def test_close_candidate_accepts_only_exact_publisher_select(candidate_grants):
    await stage._close_candidate_grants(candidate_grants, ownership(), 101, None)
    assert candidate_grants.events == ["locked", "sealed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("publisher_oid", [None, 0, 101, True, "102"])
async def test_close_candidate_requires_actual_distinct_publisher_oid(candidate_grants, publisher_oid):
    candidate_grants.publisher_oid = publisher_oid
    with pytest.raises(stage.native.ReferenceFamilyArchiveError, match="Publisher identity differs"):
        await stage._close_candidate_grants(candidate_grants, ownership(), 101, None)
    assert candidate_grants.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "other", "grantable", "grantor", "write", "duplicate"])
async def test_close_candidate_refuses_changed_publisher_acl_before_sealing(candidate_grants, change):
    candidate_grants.entries[-1:] = {
        "missing": [],
        "other": [(103, 101, "SELECT", False)],
        "grantable": [(102, 101, "SELECT", True)],
        "grantor": [(102, 104, "SELECT", False)],
        "write": [(102, 101, "SELECT", False), (102, 101, "INSERT", False)],
        "duplicate": [(102, 101, "SELECT", False), (102, 101, "SELECT", False)],
    }[change]
    with pytest.raises(stage.native.ReferenceFamilyArchiveError, match="candidate grants differ"):
        await stage._close_candidate_grants(candidate_grants, ownership(), 101, None)
    assert candidate_grants.events == []
