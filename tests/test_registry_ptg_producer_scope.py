# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Producer approval identity and server-derived file subsets without services."""

import hashlib
import importlib.util
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql

from process import registry_ptg_producer_scope as scope
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_record_store import RegistryActor

_ID = UUID("11111111-1111-4111-8111-111111111111")
_OTHER_ID = UUID("22222222-2222-4222-8222-222222222222")


@pytest.fixture(autouse=True)
def _canonical_payload(monkeypatch):
    monkeypatch.setattr(scope, "_physical_binding", AsyncMock(return_value=None))


def _specification(**changes):
    return SimpleNamespace(
        **{
            "ptg_schema_name": "synthetic_ptg",
            "snapshot_id": "synthetic-snapshot",
            "binding_source_key": "synthetic-binding",
            "company_key": "synthetic-company",
            "cohort_id": "synthetic-cohort",
            **changes,
        }
    )


def _command(**changes):
    return replace(
        scope.RegistryPTGProducerScopeCommand(
            _ID,
            RegistryNetworkSourceCoordinates(
                "ptg", "synthetic-source", "synthetic_ptg", "dataset", "producer", "edition"
            ),
            "client_example",
            _OTHER_ID,
            3,
            "statement_example",
            "a" * 64,
            "import_example",
            (scope.RegistryPTGProducerFileVersion("version_second", "b" * 64, "c" * 64),),
            "Explicit producer scope review",
            "review_example",
        ),
        **changes,
    )


def _store():
    return scope.RegistryPTGProducerScopeStore("scope_owner", "scope_approver", "synthetic_control")


def _actor(client_id="client_example"):
    return RegistryActor("client_owner", _ID, client_id)


class _Result:
    def __init__(self, *, value=None, rows=()):
        self.value, self.rows = value, list(rows)

    def scalar_one(self):
        return self.value

    scalar_one_or_none = scalar_one

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def one_or_none(self):
        assert len(self.rows) <= 1
        return self.rows[0] if self.rows else None


def _session(*results, transaction=True):
    return SimpleNamespace(in_transaction=lambda: transaction, execute=AsyncMock(side_effect=results))


def _source_rows():
    return [
        {
            "source_key": 0,
            "source_file_version_id": "version_first",
            "source_file_version_count": 1,
            "version_source_identity_hash": "d" * 64,
            "version_raw_sha256": "e" * 64,
            "raw_container_sha256": "e" * 64,
        },
        {
            "source_key": 1,
            "source_file_version_id": "version_second",
            "source_file_version_count": 1,
            "version_source_identity_hash": "b" * 64,
            "version_raw_sha256": "c" * 64,
            "raw_container_sha256": "c" * 64,
        },
    ]


def _evidence():
    return {
        "source_file_import_id": "import_example",
        "source_key": "source_example",
        "snapshot_manifest_sha256": "f" * 64,
        "frozen_binding_sha256": "a" * 64,
        "graph_identity": {"snapshot_key": 7},
        "selected_dense_source_keys": [1],
    }


def _approved():
    return {**scope._command_document(_command(), _specification(), _actor()), "evidence": _evidence()}


def _record(document=None):
    document = _approved() if document is None else document
    return {"approval_json": document, "approval_sha256": scope._digest(document)}


@pytest.mark.parametrize("command", [None, {}, {"admitted": True}, {"network_binding": "approved"}])
def test_unresolved_or_mapping_only_command_never_admits(command):
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="authority_unavailable"):
        scope._command_document(command, _specification(), _actor())


def test_action_owns_the_client_and_version_set_is_complete_unique():
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="owner_changed"):
        scope._command_document(_command(), _specification(), _actor("client_other"))
    for versions in ((), _command().file_versions * 2, tuple([_command().file_versions[0]] * 129)):
        with pytest.raises(scope.RegistryPTGProducerScopeError):
            scope._command_document(_command(file_versions=versions), _specification(), _actor())
    versions = (scope.RegistryPTGProducerFileVersion("version_first", "d" * 64, "e" * 64), *_command().file_versions)
    first = scope._command_document(_command(file_versions=versions), _specification(), _actor())
    second = scope._command_document(_command(file_versions=tuple(reversed(versions))), _specification(), _actor())
    assert first == second
    assert all("dense_source_key" not in version for version in first["file_versions"])


@pytest.mark.parametrize(
    "field,value",
    [
        ("client_id", " client_example"),
        ("producer_statement_sha256", "bad"),
        ("approved_revision", True),
        ("source_file_import_id", ""),
        ("scope_id", UUID(int=0)),
    ],
)
def test_invalid_action_material_refuses(field, value):
    with pytest.raises(scope.RegistryPTGProducerScopeError):
        scope._command_document(_command(**{field: value}), _specification(), _actor())


@pytest.mark.parametrize("change", ["missing_company", "extra_boolean", "wrong_actor", "bad_statement"])
def test_stored_hash_does_not_replace_complete_action_validation(change):
    document = _approved()
    if change == "missing_company":
        document.pop("legal_company_id")
    elif change == "extra_boolean":
        document["admitted"] = True
    elif change == "wrong_actor":
        document["actor"]["client_id"] = "other_client"
    else:
        document["producer_statement_sha256"] = "invalid"
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="scope_changed"):
        scope._validated_approval(document, _specification())


@pytest.mark.asyncio
async def test_subset_is_derived_with_one_bounded_source_join():
    session = _session(_Result(rows=_source_rows()))
    versions = scope._command_document(_command(), _specification(), _actor())["file_versions"]
    assert await scope._selected_versions(session, _specification(), versions) == [1]
    assert session.execute.await_count == 1
    statement, parameters = session.execute.await_args.args
    query = str(statement.compile(dialect=postgresql.dialect()))
    assert all(name in query for name in ("ptg2_source_trace_set", "ptg2_source_trace", "ptg2_source_file_version"))
    assert "LIMIT 129" in query and parameters == {"snapshot_id": "synthetic-snapshot"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"source_file_version_count": 2},
        {"version_source_identity_hash": "a" * 64},
        {"version_raw_sha256": "a" * 64},
        {"raw_container_sha256": "a" * 64},
        {"source_key": True},
        {"source_file_version_id": "missing_version"},
    ],
)
async def test_selected_file_substitution_or_ambiguity_refuses(change):
    rows = _source_rows()
    rows[1].update(change)
    versions = scope._command_document(_command(), _specification(), _actor())["file_versions"]
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="source_changed"):
        await scope._selected_versions(_session(_Result(rows=rows)), _specification(), versions)


@pytest.mark.asyncio
async def test_evidence_rechecks_existing_frozen_and_sealed_graph_gates(monkeypatch):
    frozen_by_field = {
        name: value
        for name, value in _evidence().items()
        if name not in {"graph_identity", "selected_dense_source_keys"}
    }
    frozen = AsyncMock(return_value=frozen_by_field)
    graph = AsyncMock(return_value=({"snapshot_key": 7}, _source_rows()))
    monkeypatch.setattr(scope, "_require_frozen_source", frozen)
    monkeypatch.setattr(scope, "_source_state", graph)
    document = scope._command_document(_command(), _specification(), _actor())
    assert (
        await scope._evidence(_session(_Result(rows=_source_rows())), _specification(), document, {}, {}) == _evidence()
    )
    assert frozen.await_count == graph.await_count == 1
    frozen_by_field["source_file_import_id"] = "other_import"
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="source_changed"):
        await scope._evidence(_session(), _specification(), document, {}, {})
    assert graph.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["read committed", "read uncommitted"])
async def test_unstable_transaction_is_rejected(isolation):
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="transaction_required"):
        await scope._protected_store(
            _session(_Result(value=isolation)),
            store=_store(),
            write=False,
        )


@pytest.mark.asyncio
async def test_unprotected_store_refuses_before_source_or_write():
    session = _session(_Result(value="repeatable read"), _Result(value=False))
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="store_unprotected"):
        await scope._protected_store(
            session,
            store=_store(),
            write=True,
        )
    assert session.execute.await_count == 2
    query = str(session.execute.await_args.args[0])
    assert all(fragment in query for fragment in ("aclexplode(att.attacl)", "relrowsecurity", "TRUNCATE"))


@pytest.mark.asyncio
async def test_exact_approval_replay_and_conflicting_identity(monkeypatch):
    protected = AsyncMock(return_value='"synthetic_control"."registry_ptg_producer_scope"')
    evidence = AsyncMock(return_value=_evidence())
    monkeypatch.setattr(scope, "_protected_store", protected)
    monkeypatch.setattr(scope, "_evidence", evidence)
    monkeypatch.setattr(scope, "require_registry_company_approval_fence", AsyncMock())
    for changed in (None, {"reason": "Other review"}, {"legal_company_id": str(_ID)}):
        stored = _approved() if changed is None else {**_approved(), **changed}
        session = _session(_Result(rows=[_record(stored)]), _Result(), _Result(rows=[_record(stored)]))
        argument_by_name = dict(frozen_authority={}, graph_identity={}, store=_store())
        if changed is None:
            assert (
                await scope.approve_registry_ptg_producer_scope(
                    session, _specification(), _command(), _actor(), **argument_by_name
                )
                == stored
            )
        else:
            with pytest.raises(scope.RegistryPTGProducerScopeError, match="idempotency_conflict"):
                await scope.approve_registry_ptg_producer_scope(
                    session, _specification(), _command(), _actor(), **argument_by_name
                )
        if changed is None:
            insert = str(session.execute.await_args_list[1].args[0])
            assert "ON CONFLICT DO NOTHING" in insert and "UPDATE" not in insert
        query = str(session.execute.await_args.args[0])
        assert "scope_key=:scope_key" in query


@pytest.mark.asyncio
async def test_unapproved_company_cannot_be_substituted(monkeypatch):
    monkeypatch.setattr(
        scope, "_protected_store", AsyncMock(return_value='"synthetic_control"."registry_ptg_producer_scope"')
    )
    evidence = AsyncMock(return_value=_evidence())
    monkeypatch.setattr(scope, "_evidence", evidence)
    monkeypatch.setattr(scope, "require_registry_company_approval_fence", AsyncMock())
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="company_unapproved"):
        await scope.approve_registry_ptg_producer_scope(
            _session(_Result(rows=[]), _Result(value=False)),
            _specification(),
            _command(),
            _actor(),
            frozen_authority={},
            graph_identity={},
            store=_store(),
        )
    evidence.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"client_id": "other_client"},
        {"company_key": "other_company"},
        {"cohort_id": "other_cohort"},
        {"snapshot_id": "other_snapshot"},
        {"coordinates": {"source_system": "fhir"}},
    ],
)
async def test_verified_read_rejects_cross_scope_approval(monkeypatch, changes):
    monkeypatch.setattr(
        scope, "_protected_store", AsyncMock(return_value='"synthetic_control"."registry_ptg_producer_scope"')
    )
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="scope_changed"):
        await scope.read_registry_ptg_producer_scope(
            _session(_Result(rows=[_record({**_approved(), **changes})])),
            _specification(),
            scope_id=_ID,
            client_id="client_example",
            coordinates=_command().coordinates,
            frozen_authority={},
            graph_identity={},
            store=_store(),
        )


@pytest.mark.asyncio
async def test_read_requires_durable_approval_and_unchanged_evidence(monkeypatch):
    monkeypatch.setattr(
        scope, "_protected_store", AsyncMock(return_value='"synthetic_control"."registry_ptg_producer_scope"')
    )
    evidence = AsyncMock(return_value=_evidence())
    monkeypatch.setattr(scope, "_evidence", evidence)
    argument_by_name = dict(
        scope_id=_ID,
        client_id="client_example",
        coordinates=_command().coordinates,
        frozen_authority={},
        graph_identity={},
        store=_store(),
    )
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="scope_unavailable"):
        await scope.read_registry_ptg_producer_scope(_session(_Result()), _specification(), **argument_by_name)
    evidence.assert_not_awaited()
    receipt = await scope.read_registry_ptg_producer_scope(
        _session(_Result(rows=[_record()])), _specification(), **argument_by_name
    )
    assert receipt["evidence"]["selected_dense_source_keys"] == [1]
    receipt["evidence"]["graph_identity"]["snapshot_key"] = 99
    assert _approved()["evidence"]["graph_identity"]["snapshot_key"] == 7
    evidence.return_value = {**_evidence(), "selected_dense_source_keys": [0]}
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="source_changed"):
        await scope.read_registry_ptg_producer_scope(
            _session(_Result(rows=[_record()])), _specification(), **argument_by_name
        )


def test_migration_adds_native_immutable_identity_without_database_hooks():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261009010000_registry_ptg_producer_scope.py"
    spec = importlib.util.spec_from_file_location("producer_scope_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    sql = "\n".join(migration._ddl("synthetic_control"))
    assert "scope_key VARCHAR(64) NOT NULL UNIQUE" in sql
    assert "UNIQUE (actor_key,idempotency_key)" in sql and "COALESCE(" in sql
    assert "REVOKE ALL" in sql and "FROM PUBLIC" in sql
    assert "CREATE FUNCTION" not in sql and "TRIGGER" not in sql and "FOREIGN KEY" not in sql
    assert migration.down_revision == "20261007150000_provider_directory_content_cursor_index"
    with pytest.raises(RuntimeError, match="retained-data"):
        migration.downgrade()


def _ownership(**changes):
    return replace(
        scope.RegistryPTGSourceOwnershipWitness(
            "import_example",
            "client_example",
            "file_example",
            "opaque-content-version",
            "opaque-period",
            "node_example",
            "succeeded",
            "run_example",
            "synthetic-snapshot",
            "source_example",
            "b" * 64,
            "version_second",
        ),
        **changes,
    )


def _review_command(**changes):
    base = _command()
    return replace(
        scope.RegistryPTGOperatorReviewCommand(
            **{
                name: getattr(base, name)
                for name in (
                    "scope_id",
                    "coordinates",
                    "client_id",
                    "legal_company_id",
                    "approved_revision",
                    "source_file_import_id",
                    "file_versions",
                    "reason",
                    "idempotency_key",
                )
            },
            statement_id=_OTHER_ID,
            ownership=_ownership(),
        ),
        **changes,
    )


def _administrator(**changes):
    return replace(RegistryActor("platform_admin", _ID, "system"), **changes)


def _review_approved(command=None, actor=None, specification=None):
    return scope._approval_document(
        scope._command_document(
            command or _review_command(), specification or _specification(), actor or _administrator()
        ),
        {**_evidence(), "engine_run_id": "run_example"},
    )


@pytest.mark.parametrize("width", [16, 32, 64])
def test_retained_review_preserves_native_engine_identity(width):
    identity = "a" * width
    versions = tuple(replace(version, source_identity_sha256=identity) for version in _command().file_versions)
    command = _review_command(file_versions=versions, ownership=_ownership(engine_source_identity_hash=identity))
    approved = _review_approved(command=command)
    assert all(version["source_identity_sha256"] == identity for version in approved["file_versions"])
    assert approved["operator_review"]["source_ownership"]["engine_source_identity_hash"] == identity
    assert len(approved["producer_statement_sha256"]) == 64


def test_operator_review_is_complete_human_provenance_with_server_computed_digest():
    document = _review_approved()
    statement = document["operator_review"]
    assert statement["contract"] == "authenticated_operator_review.v1"
    assert statement["actor"] == {
        "kind": "platform_admin",
        "user_id": str(_ID),
        "client_id": "system",
        "impersonator_id": None,
    }
    assert statement["client_id"] == "client_example"
    assert statement["source_ownership"] == scope.asdict(_ownership())
    assert statement["evidence"]["engine_run_id"] == "run_example"
    expected = hashlib.sha256(
        json.dumps(statement, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
    ).hexdigest()
    assert document["producer_statement_sha256"] == expected
    assert document["producer_statement_id"] == f"operator-review:v1:{_OTHER_ID}"
    assert "idempotency_key" not in statement and "producer_statement_sha256" not in statement
    with pytest.raises(TypeError):
        scope.RegistryPTGOperatorReviewCommand(
            **{**scope.asdict(_review_command()), "producer_statement_sha256": "a" * 64}
        )
    scope._validated_approval(document, _specification())


@pytest.mark.parametrize(
    "changes",
    [
        {"kind": "client_owner", "client_id": "client_example"},
        {"client_id": "client_example"},
        {"impersonator_id": _OTHER_ID},
    ],
)
def test_operator_review_refuses_owner_or_impersonated_administrator(changes):
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="actor_invalid"):
        scope._command_document(_review_command(), _specification(), _administrator(**changes))


@pytest.mark.parametrize(
    "changes",
    [
        {"client_id": "other_client"},
        {"source_file_import_id": "other_import"},
        {"snapshot_id": "other_snapshot"},
        {"assigned_node_id": ""},
        {"engine_run_id": ""},
        {"engine_source_identity_hash": "invalid"},
        {"status": True},
        {"source_file_id": "f" * 65},
    ],
)
def test_operator_review_refuses_incomplete_or_cross_scope_ownership(changes):
    with pytest.raises(scope.RegistryPTGProducerScopeError):
        scope._command_document(_review_command(ownership=_ownership(**changes)), _specification(), _administrator())


def test_operator_review_preserves_controller_opaque_versions_and_blank_terminal_status():
    command = _review_command(ownership=_ownership(status="", content_version="é" * 64))
    document = scope._command_document(command, _specification(), _administrator())
    assert document["operator_review"]["source_ownership"]["content_version"] == "é" * 64
    assert document["operator_review"]["source_ownership"]["import_month"] == "opaque-period"
    scope._validated_approval(_review_approved(command), _specification())


def test_operator_provenance_cannot_be_downgraded_to_a_supplied_digest():
    document = _review_approved()
    document.pop("operator_review")
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="scope_changed"):
        scope._validated_approval(document, _specification())
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="input_invalid"):
        scope._command_document(
            _command(producer_statement_id=f"operator-review:v1:{_OTHER_ID}"), _specification(), _actor()
        )


@pytest.mark.parametrize(
    "change",
    [
        "contract",
        "extra_boolean",
        "statement_id",
        "client",
        "company",
        "revision",
        "cohort",
        "actor",
        "coordinates",
        "version",
        "ownership",
        "run",
        "digest",
    ],
)
def test_rehashing_stored_operator_statement_cannot_replace_closed_reconstruction(change):
    document = _review_approved()
    statement = document["operator_review"]
    replacement_by_change = {
        "extra_boolean": {"reviewed": True},
        "contract": {"contract": "carrier_signed.v1"},
        "statement_id": {"statement_id": str(_ID)},
        "client": {"client_id": "other_client"},
        "company": {"legal_company_id": str(_ID)},
        "revision": {"approved_revision": 4},
        "cohort": {"cohort_id": "other_cohort"},
        "actor": {"actor": {**statement["actor"], "user_id": str(_OTHER_ID)}},
        "coordinates": {"coordinates": {**statement["coordinates"], "edition_id": "other_edition"}},
        "version": {"file_versions": [{**statement["file_versions"][0], "raw_sha256": "a" * 64}]},
        "run": {"evidence": {**statement["evidence"], "engine_run_id": "other_run"}},
    }
    statement.update(replacement_by_change.get(change, {}))
    if change == "ownership":
        statement["source_ownership"]["client_id"] = "other_client"
    document["producer_statement_sha256"] = "a" * 64 if change == "digest" else scope._digest(statement)
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="scope_changed"):
        scope._validated_approval(document, _specification())


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "run", "source_key", "version", "identity"])
async def test_operator_evidence_binds_actual_run_and_controller_file_identity(monkeypatch, change):
    frozen = AsyncMock(
        return_value={
            name: field_value
            for name, field_value in _evidence().items()
            if name not in {"graph_identity", "selected_dense_source_keys"}
        }
    )
    monkeypatch.setattr(scope, "_require_frozen_source", frozen)
    monkeypatch.setattr(scope, "_source_state", AsyncMock(return_value=({"snapshot_key": 7}, _source_rows())))
    ownership_changes = {
        "run": {"engine_run_id": "other_run"},
        "source_key": {"source_key": "other_source"},
        "version": {"engine_source_file_version_id": "other_version"},
        "identity": {"engine_source_identity_hash": "a" * 64},
    }.get(change, {})
    document = scope._command_document(
        _review_command(ownership=_ownership(**ownership_changes)), _specification(), _administrator()
    )
    session = _session(_Result(value="run_example"), _Result(rows=_source_rows()))
    if change is not None:
        with pytest.raises(scope.RegistryPTGProducerScopeError, match="source_changed"):
            await scope._evidence(session, _specification(), document, {}, {})
    else:
        assert await scope._evidence(session, _specification(), document, {}, {}) == {
            **_evidence(),
            "engine_run_id": "run_example",
        }
        assert session.execute.await_count == 2
    assert "SELECT import_run_id" in str(session.execute.await_args_list[0].args[0])


@pytest.mark.asyncio
async def test_operator_approval_replay_is_immutable_and_verified_read_is_detached(monkeypatch):
    monkeypatch.setattr(
        scope, "_protected_store", AsyncMock(return_value='"synthetic_control"."registry_ptg_producer_scope"')
    )
    monkeypatch.setattr(scope, "_evidence", AsyncMock(return_value={**_evidence(), "engine_run_id": "run_example"}))
    monkeypatch.setattr(scope, "require_registry_company_approval_fence", AsyncMock())
    expected = _review_approved()
    session = _session(_Result(rows=[_record(expected)]), _Result(), _Result(rows=[_record(expected)]))
    assert (
        await scope.approve_registry_ptg_producer_scope(
            session,
            _specification(),
            _review_command(),
            _administrator(),
            frozen_authority={},
            graph_identity={},
            store=_store(),
        )
        == expected
    )
    assert "ON CONFLICT DO NOTHING" in str(session.execute.await_args_list[1].args[0])
    receipt = await scope.read_registry_ptg_producer_scope(
        _session(_Result(rows=[_record(expected)])),
        _specification(),
        scope_id=_ID,
        client_id="client_example",
        coordinates=_command().coordinates,
        frozen_authority={},
        graph_identity={},
        store=_store(),
    )
    receipt["operator_review"]["source_ownership"]["assigned_node_id"] = "other_node"
    assert expected["operator_review"]["source_ownership"]["assigned_node_id"] == "node_example"
    with pytest.raises(scope.RegistryPTGProducerScopeError, match="idempotency_conflict"):
        await scope.approve_registry_ptg_producer_scope(
            _session(_Result(rows=[_record(expected)]), _Result(), _Result(rows=[_record(expected)])),
            _specification(),
            _review_command(ownership=_ownership(assigned_node_id="other_node")),
            _administrator(),
            frozen_authority={},
            graph_identity={},
            store=_store(),
        )
