# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address transitions keep bounded copies and exact native custody fences."""

import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from process import entity_address_native_publication as publication
from process import entity_address_snapshot_adoption as adoption
from process import entity_address_snapshot_ownership as ownership
from process import entity_address_snapshot_preparation as preparation
from process import entity_address_snapshot_receipt as receipt
from process import entity_address_snapshot_restore as restore
from process import entity_address_snapshot_source as source
from tests.test_entity_address_archive_ownership_guards import _owner, _set_owner
from tests.test_entity_address_archive_receipt_guards import _set_receipt
from tests.test_entity_address_archive_restore_guards import _stored_restore
from tests.test_geo_assurance_dependency_bindings import _example_bindings


def _result(rows=()):
    query_result = Mock()
    query_result.mappings.return_value = query_result
    query_result.scalars.return_value = query_result
    query_result.all.return_value = list(rows)
    query_result.one.return_value = rows[0] if rows else None
    query_result.one_or_none.return_value = rows[0] if rows else None
    return query_result


def _handoff():
    attempt = "synthetic:" + "1" * 32
    suffix = publication._attempt_suffix(attempt)
    names = publication._stage_names("mrf", suffix)
    handoff_by_field = {
        "contract": publication.HANDOFF_CONTRACT,
        "run_id": "synthetic",
        "attempt_id": attempt,
        "attempt_started_at": "2026-01-01T00:00:00Z",
        "schema_name": "mrf",
        "import_date": suffix,
        "database_oid": 41,
        "import_run_oid": 42,
        "stage_relations": [
            {"table_name": name, "stage_name": stage, "relation_oid": 100 + index, "relfilenode": 200 + index}
            for index, (name, stage) in enumerate(names.items())
        ],
        "incumbent": {
            "local_lineage_id": "550e8400-e29b-41d4-a716-446655440000",
            "local_generation": 0,
            "serving_generation": None,
            "relation_oids": None,
        },
        "alias_generation": 3,
        "dependency_bindings": _example_bindings(),
        "row_count": 1,
        "indexes": [
            {
                "relation_oid": 100 + index,
                "index_oid": 300 + index,
                "relfilenode": 400 + index,
                "definition": "synthetic index metadata",
            }
            for index in range(7)
        ],
    }
    handoff_by_field["source_contract_sha256"] = publication._source_contract(handoff_by_field, {})
    handoff_by_field["handoff_sha256"] = publication._digest(handoff_by_field)
    return publication.validate_entity_address_native_handoff(handoff_by_field)


def _publication_receipt(handoff_by_field):
    relations = handoff_by_field["stage_relations"]
    validation_by_field = {
        "rows": 1,
        "integrity": {"synthetic": True},
        "catalog": {
            "shapes": [
                [relation_entry["table_name"], relation_entry["relation_oid"], "a" * 64] for relation_entry in relations
            ],
            "indexes": deepcopy(handoff_by_field["indexes"]),
            "columns": [{"relation_oid": relation_entry["relation_oid"]} for relation_entry in relations],
            "constraints": [
                {"relation_oid": relation_entry["relation_oid"], "kind": "p"} for relation_entry in relations
            ],
        },
    }
    return {
        "contract": publication.PUBLICATION_CONTRACT,
        "handoff": handoff_by_field,
        "validation": validation_by_field,
        "validation_sha256": publication._digest(validation_by_field),
        "source_contract_sha256": handoff_by_field["source_contract_sha256"],
        "inventory": {
            "database_oid": handoff_by_field["database_oid"],
            "relations": [
                {
                    "schema_name": "mrf",
                    "schema_oid": 51,
                    "schema_owner_oid": 52,
                    "owner_oid": 52,
                    "relation_name": relation_entry["table_name"],
                    "relation_oid": relation_entry["relation_oid"],
                    "relfilenode": relation_entry["relfilenode"],
                }
                for relation_entry in relations
            ],
        },
        "alias": {"generation": handoff_by_field["alias_generation"], "catalog_sha256": "b" * 64},
        "result_generation": {
            **handoff_by_field["incumbent"],
            "local_generation": 1,
            "serving_generation": {
                "origin_lineage_id": handoff_by_field["incumbent"]["local_lineage_id"],
                "origin_generation": 1,
                "published_at": "2026-01-01T00:01:00Z",
            },
            "relation_oids": [relation_entry["relation_oid"] for relation_entry in relations],
        },
    }


@pytest.mark.parametrize(
    "change",
    [
        None,
        "attempt",
        "rows",
        "alias",
        "database",
        "missing_heap",
        "duplicate_heap",
        "stage_name",
        "missing_index",
        "index_owner",
        "digest",
    ],
)
def test_handoff_decoder_requires_exact_attempt_and_complete_seven_table_identity(change):
    handoff_by_field = _handoff()
    mutations_by_name = {
        "attempt": lambda: handoff_by_field.update(import_date="a" * 26),
        "rows": lambda: handoff_by_field.update(row_count=True),
        "alias": lambda: handoff_by_field.update(alias_generation=-1),
        "database": lambda: handoff_by_field.update(database_oid=0),
        "missing_heap": lambda: handoff_by_field["stage_relations"].pop(),
        "duplicate_heap": lambda: handoff_by_field["stage_relations"][0].update(relation_oid=101),
        "stage_name": lambda: handoff_by_field["stage_relations"][0].update(stage_name="foreign_stage"),
        "missing_index": lambda: handoff_by_field["indexes"].pop(),
        "index_owner": lambda: handoff_by_field["indexes"][0].update(relation_oid=999),
        "digest": lambda: handoff_by_field.update(handoff_sha256="a" * 64),
    }
    if change:
        mutations_by_name[change]()
    if change and change != "digest":
        handoff_by_field["handoff_sha256"] = publication._digest(
            {key: field_value for key, field_value in handoff_by_field.items() if key != "handoff_sha256"}
        )
    if change:
        with pytest.raises(RuntimeError, match="address native publication"):
            publication.validate_entity_address_native_handoff(handoff_by_field)
    else:
        assert publication.validate_entity_address_native_handoff(handoff_by_field) == handoff_by_field


@pytest.mark.parametrize(
    "change",
    [
        None,
        "counter",
        "lineage",
        "origin",
        "oid_order",
        "row_count",
        "source",
        "inventory_file",
        "inventory_owner",
        "shape",
        "catalog_family",
        "payload_fk",
        "alias",
    ],
)
def test_publication_decoder_keeps_generation_accounting_and_physical_catalog_bound(change):
    receipt = _publication_receipt(_handoff())
    mutations_by_name = {
        "counter": lambda: receipt["result_generation"].update(local_generation=2),
        "lineage": lambda: receipt["result_generation"].update(local_lineage_id="550e8400-e29b-41d4-a716-446655440001"),
        "origin": lambda: receipt["result_generation"]["serving_generation"].update(origin_generation=2),
        "oid_order": lambda: receipt["result_generation"]["relation_oids"].reverse(),
        "row_count": lambda: receipt["validation"].update(rows=2),
        "source": lambda: receipt.update(source_contract_sha256="c" * 64),
        "inventory_file": lambda: receipt["inventory"]["relations"][0].update(relfilenode=999),
        "inventory_owner": lambda: receipt["inventory"]["relations"][0].update(owner_oid=999),
        "shape": lambda: receipt["validation"]["catalog"]["shapes"][0].__setitem__(1, 999),
        "catalog_family": lambda: receipt["validation"]["catalog"]["columns"].pop(),
        "payload_fk": lambda: receipt["validation"]["catalog"]["constraints"][0].update(kind="f"),
        "alias": lambda: receipt["alias"].update(generation=4),
    }
    if change:
        mutations_by_name[change]()
        receipt["validation_sha256"] = publication._digest(receipt["validation"])
        with pytest.raises(RuntimeError, match="address native publication"):
            publication.validate_entity_address_native_publication(receipt)
    else:
        assert publication.validate_entity_address_native_publication(receipt) == receipt


def _clone_session(events, equality_probes, failure):
    async def execute(statement, *_args):
        events.append(str(statement.compile(dialect=postgresql.dialect())))
        return _result()

    async def has_exact_set(statement, *_args):
        query = str(statement)
        events.append(query)
        if "FULL JOIN" in query:
            return failure != "alias"
        if query.startswith("SELECT EXISTS"):
            return False
        assert query.startswith("SELECT NOT EXISTS"), query
        equality_probes.append(query)
        return not (failure == "set" and len(equality_probes) == 3)

    return SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(side_effect=execute), scalar=AsyncMock(side_effect=has_exact_set)
    )


def _copy_callback(session, events, copy_calls, failure):
    async def copy_rows(actual_session, query, **copy_options_by_field):
        assert actual_session is session
        copy_calls.append((query, copy_options_by_field))
        events.append("COPY " + copy_options_by_field["table_name"])
        assert 0 < copy_options_by_field["timeout"] <= 100
        if failure == "copy":
            raise RuntimeError("synthetic COPY failed")
        if failure == "cancel":
            raise asyncio.CancelledError()
        return True if failure == "bounds" else 10

    return copy_rows


def _empty_custody_callback(session, events):
    async def precreated(actual_session):
        assert actual_session is session
        assert sum(query.lstrip().startswith("CREATE TABLE") for query in events) == 8
        assert not any(query.startswith("COPY") or "PRIMARY KEY" in query for query in events)
        events.append("custody")

    return AsyncMock(side_effect=precreated)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "copy", "cancel", "bounds", "set", "alias"])
async def test_source_clone_orders_custody_copy_indexes_and_exact_sets(failure):
    events, copy_calls, equality_probes = [], [], []
    capture = source.EntityAddressArchiveSourceCapture(
        source.CONTRACT, "source", source.entity_address_archive_relations(source.CONTRACT), "00000003-0000001B-1"
    )
    session = _clone_session(events, equality_probes, failure)
    callback = _empty_custody_callback(session, events)
    copy_options_by_field = {
        "source_capture": capture,
        "stage_schema": "candidate",
        "source_copy": source.EntityAddressSourceCopy(_copy_callback(session, events, copy_calls, failure), 512, 100),
        "copy_deadline": asyncio.get_running_loop().time() + 100,
        "on_precreated": callback,
    }
    if failure:
        with pytest.raises(
            asyncio.CancelledError
            if failure == "cancel"
            else (RuntimeError, source.alias_authority.EntityAddressSnapshotAliasError)
        ):
            await source._clone_entity_address_archive_source(session, **copy_options_by_field)
    else:
        await source._clone_entity_address_archive_source(session, **copy_options_by_field)
    callback.assert_awaited_once_with(session)
    assert events[:3] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'",
        'CREATE SCHEMA "candidate"',
    ]
    assert not any("FOREIGN KEY" in query or "CREATE TRIGGER" in query or query == "COMMIT" for query in events)
    assert [copy_options_by_field["max_bytes"] for _query, copy_options_by_field in copy_calls] == list(
        range(512, 512 - 10 * len(copy_calls), -10)
    )
    if failure in {"copy", "cancel", "bounds"}:
        assert len(copy_calls) == 1 and len(equality_probes) == 0
        assert not any("CREATE INDEX" in query or "PRIMARY KEY" in query for query in events)
    else:
        assert len(copy_calls) == 8
        assert copy_calls[-1][1]["table_name"] == source.alias_authority.AUTHORITY_TABLE
        assert "revoked_at IS NULL" in copy_calls[-1][0]
        assert copy_calls[-1][1]["columns"] == source.alias_authority._SEMANTIC_COLUMNS
        assert max(index for index, query in enumerate(events) if query.startswith("COPY")) < min(
            index for index, query in enumerate(events) if "PRIMARY KEY" in query
        )
        assert max(index for index, query in enumerate(events) if "CREATE INDEX" in query) < min(
            index for index, query in enumerate(events) if query.startswith("SELECT")
        )
        assert len(equality_probes) == (3 if failure == "set" else 7)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["snapshot", "callback", "budget", "timeout", "deadline", "bundle"])
async def test_source_copy_admission_refuses_open_or_unbounded_inputs_before_ddl(failure):
    capture = source.EntityAddressArchiveSourceCapture(
        source.CONTRACT,
        "source",
        source.entity_address_archive_relations(source.CONTRACT),
        "invalid" if failure == "snapshot" else "00000003-0000001B-1",
    )
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises((ValueError, RuntimeError, TimeoutError)):
        copier = source.EntityAddressSourceCopy(
            AsyncMock(), True if failure == "budget" else 512, float("nan") if failure == "timeout" else 100
        )
        await source._clone_entity_address_archive_source(
            session,
            source_capture=capture,
            stage_schema="candidate",
            source_copy={} if failure == "bundle" else copier,
            copy_deadline=0 if failure == "deadline" else asyncio.get_running_loop().time() + 100,
            on_precreated=None if failure == "callback" else AsyncMock(),
        )
    session.execute.assert_not_awaited()


def _abandonment(handoff_by_field):
    return {
        "contract": publication.ABANDONMENT_CONTRACT,
        "preparation_id": "550e8400-e29b-41d4-a716-446655440001",
        "admission_sha256": "a" * 64,
        "generation_id": "550e8400-e29b-41d4-a716-446655440002",
        "publication_fence": "550e8400-e29b-41d4-a716-446655440003",
        "reason": "terminal-cancellation",
        "stage_disposition": "retained-unpublished",
        "physical_cleanup_completed": False,
        "same_run_readmission": "requires-exact-candidate-cleanup-and-ledger-retirement",
        "observed_run": {
            "run_id": handoff_by_field["run_id"],
            "node_id": "synthetic",
            "status": "cancelled",
            "progress": {key: handoff_by_field[key] for key in ("attempt_id", "attempt_started_at")},
            "handoff_sha256": handoff_by_field["handoff_sha256"],
            "phase_detail": "cancel requested",
            "finished_at": "2026-01-01T00:01:00Z",
        },
        "candidate_custody": {
            "builder_oid": 54,
            **{
                key: handoff_by_field[key]
                for key in (
                    "database_oid",
                    "import_run_oid",
                    "schema_name",
                    "attempt_id",
                    "attempt_started_at",
                    "handoff_sha256",
                    "source_contract_sha256",
                    "stage_relations",
                    "indexes",
                )
            },
        },
        "released_pins": [],
    }


def _cleanup_run(handoff_by_field, abandonment, failure):
    run_by_field = {
        **{key: abandonment["observed_run"][key] for key in ("status", "progress", "phase_detail", "finished_at")},
        "error": None,
        "params": {"test_mode": True} if failure == "params" else {},
        "metrics": {
            "address_handoff": handoff_by_field,
            "address_native_abandonments": {abandonment["preparation_id"]: abandonment},
        },
    }
    if failure == "active":
        run_by_field.update(status="running", finished_at=None)
    if failure == "published":
        run_by_field["metrics"]["address_native_publication"] = {}
    return run_by_field


def _cleanup_catalog_rows(query, run_by_field, stages, indexes, failure):
    if query.startswith('SELECT * FROM "mrf".import_run'):
        return _result([run_by_field])
    if query.startswith("SELECT owner.oid AS owner_oid"):
        return _result([{"owner_oid": 52, "owner_safe": True, "caller_safe": True}])
    if query.startswith("SELECT (SELECT oid::bigint FROM pg_database"):
        return _result([(999 if failure == "database" else 41, 42)])
    if query.startswith("SELECT c.relname,c.oid"):
        return _result(stages)
    if query.startswith("SELECT i.indrelid::bigint"):
        return _result(indexes)
    assert query.startswith(("LOCK TABLE", "SET LOCAL", "SELECT set_config", "DROP TABLE")), query
    return _result()


def _cleanup_scalar(query, handoff_by_field, transactions, failure):
    if query == "SHOW search_path":
        return "synthetic"
    if "pg_current_xact_id" in query:
        return next(transactions)
    if "relation_oids OPERATOR" in query:
        return failure != "serving"
    if "SELECT count(DISTINCT relation.relowner)" in query:
        return failure != "owner"
    if "pg_index i JOIN pg_class idx" in query:
        return failure != "executable"
    if "obj_description" in query:
        return (
            "changed"
            if failure == "marker"
            else publication._json(
                {key: handoff_by_field[key] for key in ("contract", "run_id", "attempt_id", "handoff_sha256")}
            )
        )
    raise AssertionError(query)


def _cleanup_session(handoff_by_field, run_by_field, failure, queries):
    stages = [
        {
            "relname": relation_entry["stage_name"],
            "oid": relation_entry["relation_oid"],
            "relfilenode": relation_entry["relfilenode"],
        }
        for relation_entry in handoff_by_field["stage_relations"]
    ]
    indexes = deepcopy(handoff_by_field["indexes"])
    if failure == "heap":
        stages[0]["relfilenode"] += 1
    if failure == "index":
        indexes[0]["index_oid"] += 1
    transactions = iter(["transaction-1", "transaction-2" if failure == "transaction" else "transaction-1"])

    async def execute(statement, *_args):
        query = str(statement)
        queries.append(query)
        return _cleanup_catalog_rows(query, run_by_field, stages, indexes, failure)

    async def scalar(statement, *_args):
        query = str(statement)
        queries.append(query)
        return _cleanup_scalar(query, handoff_by_field, transactions, failure)

    return SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(side_effect=execute), scalar=AsyncMock(side_effect=scalar)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        None,
        "active",
        "published",
        "params",
        "database",
        "heap",
        "index",
        "marker",
        "executable",
        "owner",
        "serving",
        "late_reference",
        "transaction",
    ],
)
async def test_native_cleanup_rechecks_catalog_and_reference_guards_before_drop(failure):
    handoff_by_field, queries = _handoff(), []
    abandonment = _abandonment(handoff_by_field)
    session = _cleanup_session(handoff_by_field, _cleanup_run(handoff_by_field, abandonment, failure), failure, queries)
    unreferenced = AsyncMock(side_effect=[True, failure != "late_reference"])
    if failure:
        with pytest.raises(RuntimeError, match="address native publication"):
            await publication.cleanup_entity_address_native_handoff(
                session,
                handoff_by_field,
                abandonment=abandonment,
                runtime_owner_oids=(54,),
                assert_unreferenced=unreferenced,
            )
    else:
        query_result = await publication.cleanup_entity_address_native_handoff(
            session,
            handoff_by_field,
            abandonment=abandonment,
            runtime_owner_oids=(54,),
            assert_unreferenced=unreferenced,
        )
        assert query_result["physical_cleanup_completed"] is True
        assert query_result["stage_relations"] == handoff_by_field["stage_relations"]
    drops = [query for query in queries if query.startswith("DROP")]
    assert drops == (
        []
        if failure
        else [
            "DROP TABLE "
            + ",".join(
                f'"mrf"."{relation_entry["stage_name"]}"' for relation_entry in handoff_by_field["stage_relations"]
            )
            + " RESTRICT"
        ]
    )
    assert queries[-1] == "SELECT set_config('search_path',:path,true)"
    assert not any("CASCADE" in query or query == "COMMIT" for query in queries)


@pytest.mark.asyncio
@pytest.mark.parametrize("already_owned", [False, True])
@pytest.mark.parametrize("unsafe", [False, True])
async def test_native_sealing_closes_table_columns_and_sequence_mutation_without_losing_reads(already_owned, unsafe):
    queries = []

    async def execute(statement, *_args):
        query = str(statement)
        queries.append(query)
        if query.startswith("SELECT owner.oid AS owner_oid"):
            return _result([{"owner_oid": 52, "owner_safe": True, "caller_safe": True}])
        if query.startswith("SELECT quote_ident(n.nspname)"):
            return _result(
                [
                    {
                        "name": '"candidate"."entity_address_evidence"',
                        "owner": '"synthetic_owner"',
                        "relowner": 52 if already_owned else 54,
                    }
                ]
            )
        if query.startswith("SELECT quote_ident(principal.rolname)"):
            return _result(['"synthetic_reader"'])
        if query.startswith("SELECT quote_ident(attname)"):
            return _result(['"evidence_id"', '"payload"'])
        if query.startswith("SELECT s.oid"):
            return _result([{"oid": 201, "name": '"candidate"."entity_address_evidence_evidence_id_seq"'}])
        assert query.startswith(("ALTER TABLE", "ALTER SEQUENCE", "REVOKE")), query
        return _result()

    session = SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(side_effect=execute), scalar=AsyncMock(return_value=unsafe)
    )
    if unsafe:
        with pytest.raises(RuntimeError, match="ordinary mutation"):
            await preparation._seal_published_relation(session, 100, 52)
    else:
        await preparation._seal_published_relation(session, 100, 52)
    assert not any("SELECT ON" in query or "REVOKE ALL" in query for query in queries)
    if already_owned:
        assert not any(query.startswith("ALTER TABLE") for query in queries)
    else:
        assert sum(query.startswith("REVOKE INSERT,UPDATE") for query in queries) == 2
        assert sum(query.startswith("REVOKE INSERT (") for query in queries) == 2
    if not unsafe:
        assert sum(query.startswith("REVOKE USAGE,UPDATE ON SEQUENCE") for query in queries) == 2
        assert session.scalar.await_args.args[1] == {"relation_oids": [201], "owner_oid": 52}
        assert "has_sequence_privilege" in str(session.scalar.await_args.args[0])


def _loaded_inputs():
    semantic = preparation.destination.validate_entity_address_archive_receipt(_set_receipt())
    aliases = preparation.alias.validate_entity_address_alias_semantic_receipt(
        {
            "contract": preparation.alias.SET_CONTRACT,
            "receipt_version": preparation.alias.SET_CONTRACT,
            "alias_schema_version": 2,
            "active_ruleset_version": 1,
            "local_generation": 3,
            "active_alias_count": 1,
        }
    )
    return _set_owner(), semantic, aliases, {"db_schema": "mrf", "import_date": "synthetic"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["extra_destination", "alias_accounting", "unselected_mrf"])
async def test_loaded_preparation_rejects_unbound_inputs_before_privileged_catalog_access(failure):
    owner, semantic, aliases, destination = _loaded_inputs()
    if failure == "extra_destination":
        destination["unexpected"] = True
    if failure == "alias_accounting":
        aliases = preparation.alias.validate_entity_address_alias_semantic_receipt(
            {**aliases.as_dict(), "active_alias_count": 2}
        )
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    authenticate = AsyncMock()
    with pytest.raises(RuntimeError):
        await preparation.prepare_loaded_entity_address_archive_destination(
            session,
            owner=owner,
            semantic_receipt=semantic,
            source_alias_receipt=aliases,
            destination=destination,
            authenticate_preparation=authenticate,
            authenticate_selected_mrf=AsyncMock() if failure == "unselected_mrf" else None,
        )
    authenticate.assert_not_awaited()
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_loaded_publication_receipt_retains_exact_seven_serving_oids_and_alias_custody(changed):
    owner = _set_owner()
    queries = []

    async def scalar(statement, parameters):
        queries.append(str(statement))
        name = parameters["relation"].rsplit(".", 1)[1].strip('"')
        return dict(owner.relation_oids)[name] + int(changed)

    session = SimpleNamespace(scalar=AsyncMock(side_effect=scalar))
    prepared = SimpleNamespace(context={"retained_relations": [{"synthetic": True}]})
    if changed:
        with pytest.raises(RuntimeError, match="published OID"):
            await preparation._publication_receipt(session, "mrf", owner, prepared, {"published": True})
        assert len(queries) == 1
    else:
        receipt = await preparation._publication_receipt(session, "mrf", owner, prepared, {"published": True})
        assert receipt["published"] is True and len(receipt["publication"]["relations"]) == 7
        assert (
            receipt["publication"]["alias_authority"]["relation_oid"]
            == dict(owner.relation_oids)[preparation.alias.AUTHORITY_TABLE]
        )
        assert receipt["publication"]["retained_relations"] == prepared.context["retained_relations"]


def _attempt_context(handoff_by_field):
    return {
        "control_run_id": handoff_by_field["run_id"],
        "import_date": "synthetic",
        "context": {
            "_control_attempt_id": handoff_by_field["attempt_id"],
            "_control_attempt_started_at": handoff_by_field["attempt_started_at"],
            "stage_prepared": True,
            "stage_indexes_prepared": True,
            "support_stage_prepared": True,
            "support_stage_indexes_prepared": True,
        },
    }


@pytest.mark.parametrize("mode", ["fresh", "replay", "changed", "disabled", "test", "unpublished", "invalid"])
def test_controlled_attempt_uses_uuid_stages_and_preserves_only_same_attempt_replay(monkeypatch, mode):
    handoff_by_field = _handoff()
    ctx = _attempt_context(handoff_by_field)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROTECTED_PUBLICATION", str(mode != "disabled"))
    if mode in {"replay", "changed"}:
        ctx["context"]["_address_stage_attempt"] = handoff_by_field["attempt_id"]
        ctx["import_date"] = "changed" if mode == "changed" else handoff_by_field["import_date"]
    if mode in {"test", "unpublished"}:
        ctx["context"]["test_mode" if mode == "test" else "publish_requested"] = mode == "test"
    if mode == "invalid":
        ctx["context"]["_control_attempt_id"] = "foreign:" + "1" * 32
    before = deepcopy(ctx)
    if mode in {"changed", "invalid"}:
        with pytest.raises(RuntimeError, match="attempt"):
            publication.bind_controlled_address_attempt(ctx)
        assert ctx == before
    else:
        publication.bind_controlled_address_attempt(ctx)
        if mode == "fresh":
            assert ctx["import_date"] == handoff_by_field["import_date"]
            assert ctx["context"]["_address_stage_attempt"] == handoff_by_field["attempt_id"]
            assert all(ctx["context"][field] is False for field in before["context"] if field.endswith("prepared"))
        else:
            assert ctx == before


@pytest.mark.asyncio
@pytest.mark.parametrize("supplied,changed", [(False, False), (True, False), (True, True)])
async def test_dependency_binding_locks_then_rechecks_exact_physical_inputs(supplied, changed):
    bindings_by_name = _example_bindings() if supplied else None
    queries = []

    async def execute(statement, *args):
        query = str(statement)
        queries.append(query)
        if query.startswith("LOCK TABLE"):
            return _result()
        assert "FROM pg_class WHERE oid=to_regclass(:name)" in query
        assert args[0]["name"].startswith(('"mrf".', '"tiger".'))
        return _result([{"relation_oid": len(queries), "relfilenode": 100 + len(queries)}])

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute), scalar=AsyncMock(return_value=not changed))
    if changed:
        with pytest.raises(RuntimeError, match="dependencies changed"):
            await publication._dependency_bindings(session, "mrf", bindings_by_name)
    else:
        observed_by_name = await publication._dependency_bindings(session, "mrf", bindings_by_name)
        assert set(observed_by_name) == set(_example_bindings())
        if supplied:
            assert observed_by_name == bindings_by_name
        else:
            for index, (namespace, table) in enumerate(publication.projection._PROJECTION_DEPENDENCIES, 1):
                namespace = namespace or "mrf"
                assert observed_by_name[f"{namespace}.{table}"] == {
                    "schema_name": namespace,
                    "table_name": table,
                    "relation_oid": index,
                    "relfilenode": 100 + index,
                }
    assert queries[-1].startswith("LOCK TABLE ONLY") and queries[-1].endswith(" IN ACCESS SHARE MODE;")
    assert "pg_relation_filenode" in str(session.scalar.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize("changed", [False, True])
async def test_control_transition_uses_exact_attempt_cas_and_never_commits(completed, changed):
    handoff_by_field = _handoff()
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=None if changed else "synthetic"))
    transition = publication._complete_control if completed else publication._write_handoff
    args = (
        (session, handoff_by_field, _publication_receipt(handoff_by_field))
        if completed
        else (session, handoff_by_field)
    )
    if changed:
        with pytest.raises(RuntimeError, match="attempt changed"):
            await transition(*args)
    else:
        await transition(*args)
    query, parameters = session.scalar.await_args.args
    assert "progress->>'attempt_id'=:attempt_id" in str(query)
    assert "progress->>'attempt_started_at'=:attempt_started_at" in str(query)
    assert "finished_at IS NULL" in str(query) and str(query).endswith(" RETURNING run_id")
    assert json.loads(parameters["handoff"]) == handoff_by_field
    comments = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(comments) == (0 if completed else 7)
    assert all(
        query.startswith('COMMENT ON TABLE "mrf".') and handoff_by_field["handoff_sha256"] in query
        for query in comments
    )
    if completed:
        assert parameters["rows"] == handoff_by_field["row_count"]
        assert parameters["phase"] == publication.PUBLICATION_PHASE
    assert all("COMMIT" not in query for query in comments)


def _finalizing_run(handoff_by_field):
    return {
        "params": {},
        "node_id": "synthetic",
        "finished_at": None,
        "phase_detail": publication.HANDOFF_PHASE,
        "metrics": {"address_handoff": handoff_by_field},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "missing", "test", "finished", "phase", "handoff", "params", "database"])
async def test_native_attempt_requires_finalizing_source_and_database_identity(failure):
    handoff_by_field = _handoff()
    run_by_field = _finalizing_run(handoff_by_field)
    mutations_by_name = {
        "test": lambda: run_by_field["params"].update(test_mode=True),
        "finished": lambda: run_by_field.update(finished_at="closed"),
        "phase": lambda: run_by_field.update(phase_detail="foreign"),
        "handoff": lambda: run_by_field["metrics"].clear(),
        "params": lambda: run_by_field["params"].update(unreviewed=True),
    }
    if failure in mutations_by_name:
        mutations_by_name[failure]()
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _result([] if failure == "missing" else [run_by_field]),
                _result([(999 if failure == "database" else 41, 42)]),
            ]
        )
    )
    if failure:
        with pytest.raises(RuntimeError, match="address native publication"):
            await publication._require_native_attempt(session, handoff_by_field)
    else:
        await publication._require_native_attempt(session, handoff_by_field)
    assert "FOR UPDATE" in str(session.execute.await_args_list[0].args[0])
    assert session.execute.await_count == (2 if failure in {None, "database"} else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "missing", "unfinished", "phase", "handoff", "params", "receipt"])
async def test_durable_outcome_requires_exact_receipt_and_successful_source_attempt(failure):
    handoff_by_field = _handoff()
    receipt = _publication_receipt(handoff_by_field)
    run_by_field = {
        **_finalizing_run(handoff_by_field),
        "finished_at": "closed",
        "phase_detail": publication.PUBLICATION_PHASE,
    }
    run_by_field["metrics"]["address_native_publication"] = receipt
    if failure == "receipt":
        different_handoff = deepcopy(handoff_by_field)
        different_handoff["attempt_started_at"] = "2026-01-01T00:00:01Z"
        different_handoff["handoff_sha256"] = publication._digest(
            {key: field_value for key, field_value in different_handoff.items() if key != "handoff_sha256"}
        )
        run_by_field["metrics"]["address_native_publication"] = _publication_receipt(different_handoff)
    mutations_by_name = {
        "unfinished": lambda: run_by_field.update(finished_at=None),
        "phase": lambda: run_by_field.update(phase_detail="foreign"),
        "handoff": lambda: run_by_field["metrics"].update(address_handoff=None),
        "params": lambda: run_by_field["params"].update(unreviewed=True),
    }
    if failure in mutations_by_name:
        mutations_by_name[failure]()
    session = SimpleNamespace(execute=AsyncMock(return_value=_result([] if failure == "missing" else [run_by_field])))
    if failure not in {None, "missing"}:
        with pytest.raises(RuntimeError, match="recorded"):
            await publication.read_entity_address_native_publication(session, handoff_by_field)
    else:
        assert await publication.read_entity_address_native_publication(session, handoff_by_field) == (
            None if failure else receipt
        )
    assert session.execute.await_count == 1 and "FOR SHARE" in str(session.execute.await_args.args[0])


def _guard_result(rows):
    query_result = MagicMock()
    query_result.mappings.return_value = query_result
    query_result.scalars.return_value = query_result
    query_result.all.return_value = rows
    query_result.one.return_value = rows[0] if rows else None
    query_result.one_or_none.return_value = rows[0] if rows else None
    query_result.__iter__.side_effect = lambda: iter(rows)
    return query_result


def _retirement_proof():
    owner = _set_owner()
    relations = [
        {"table_name": model.__tablename__, "relation_oid": dict(owner.relation_oids)[model.__tablename__]}
        for model in ownership._models()
    ]
    publication_by_field = {
        "contract": "entity-address-table-publication.v2",
        "schema_name": "mrf",
        "relations": relations,
        "retained_relations": [],
        "alias_authority": {
            "schema_name": owner.schema_name,
            "schema_oid": owner.schema_oid,
            "relation_oid": dict(owner.relation_oids)[preparation.alias.AUTHORITY_TABLE],
        },
    }
    return owner, publication_by_field


@pytest.mark.parametrize("fault", [None, "fields", "contract", "inventory", "alias", "absent_alias"])
def test_retirement_owner_rejects_substituted_publication(fault):
    owner, proof = _retirement_proof()
    mutations_by_name = {
        "fields": lambda: proof.update(foreign=True),
        "contract": lambda: proof.update(contract="unsupported"),
        "inventory": lambda: proof["relations"][0].update(relation_oid=999),
        "alias": lambda: proof["alias_authority"].update(schema_oid=999),
    }
    if fault in mutations_by_name:
        mutations_by_name[fault]()
    owner = _owner() if fault == "absent_alias" else owner
    if fault:
        with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="cleanup"):
            ownership._validated_publication_owner(owner, proof)
    else:
        assert ownership._validated_publication_owner(owner, proof) == (
            "mrf",
            proof["relations"],
            proof["alias_authority"],
        )


def _retired_catalog(owner, proof):
    rows = []
    for entry in proof["relations"]:
        oid = entry["relation_oid"]
        rows.append(
            {
                "oid": oid,
                "nspname": "mrf",
                "relname": ownership.entity_address_unified._archived_identifier(
                    f"{entry['table_name']}_retained_{oid:x}", suffix=""
                ),
            }
        )
    rows.append({"oid": 8, "nspname": owner.schema_name, "relname": preparation.alias.AUTHORITY_TABLE})
    return [
        {
            **row,
            "relowner": 52,
            "relkind": "r",
            "relpersistence": "p",
            "relrowsecurity": False,
            "relforcerowsecurity": False,
            "inherited": False,
        }
        for row in rows
    ]


@pytest.mark.parametrize(
    "field,value",
    [
        ("relowner", 99),
        ("relkind", "v"),
        ("relpersistence", "u"),
        ("relrowsecurity", True),
        ("relforcerowsecurity", True),
        ("inherited", True),
        ("nspname", "foreign"),
        ("relname", "entity_address_unified"),
        ("oid", 99),
        (None, None),
    ],
)
def test_retired_catalog_rejects_serving_or_foreign_heap(field, value):
    owner, proof = _retirement_proof()
    rows = _retired_catalog(owner, proof)
    if field is None:
        rows.pop()
    else:
        rows[0][field] = value
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="catalog differs|serving or substituted"):
        ownership._require_retained_catalog(rows, 52, owner, "mrf", proof["relations"])


def test_retired_catalog_accepts_only_exact_retained_names():
    owner, proof = _retirement_proof()
    rows = _retired_catalog(owner, proof)
    ownership._require_retained_catalog(rows, 52, owner, "mrf", proof["relations"])
    rows[-1]["nspname"] = "mrf"
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="serving or substituted"):
        ownership._require_retained_catalog(rows, 52, owner, "mrf", proof["relations"])


def _retirement_session(owner, rows, queries, fault):
    catalog_observations = []

    async def execute(statement, _parameters=None):
        query = str(statement)
        queries.append(query)
        if query.startswith("SELECT owner.oid AS owner_oid"):
            return _guard_result([{"owner_oid": 52, "owner_safe": True, "caller_safe": True}])
        if query.startswith("SELECT c.oid,n.nspname"):
            catalog_observations.append(query)
            return _guard_result(rows if len(catalog_observations) == 1 or fault != "catalog" else [])
        if query.startswith("SELECT relation.oid"):
            return _guard_result([{"oid": 8, "relkind": "r", "index_table_oid": None}])
        if query.startswith("DROP TABLE") and fault == "table_drop":
            raise RuntimeError("synthetic table drop failed")
        if query.startswith("DROP SCHEMA") and fault == "schema_drop":
            raise RuntimeError("synthetic schema drop failed")
        assert query.startswith(("LOCK TABLE", "DROP TABLE", "DROP SCHEMA")), query
        return _guard_result([])

    return SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(side_effect=execute),
        scalar=AsyncMock(return_value=999 if fault == "namespace" else owner.schema_oid),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["early_pin", "late_pin", "catalog", "namespace", "table_drop", "schema_drop"])
async def test_retirement_failure_preserves_exact_cleanup_fences(fault):
    owner, proof = _retirement_proof()
    queries = []
    session = _retirement_session(owner, _retired_catalog(owner, proof), queries, fault)
    unreferenced = AsyncMock(side_effect=[fault != "early_pin", fault != "late_pin"])
    with pytest.raises(RuntimeError, match="referenced|catalog changed|namespace changed|drop failed"):
        await ownership.cleanup_entity_address_archive_publication(
            session, owner=owner, publication=proof, assert_unreferenced=unreferenced
        )
    drops = [query for query in queries if query.startswith("DROP")]
    assert len(drops) == {"table_drop": 1, "schema_drop": 2}.get(fault, 0)
    assert all(query.endswith(" RESTRICT") for query in drops)
    assert not any("CASCADE" in query or query == "COMMIT" for query in queries)
    locks = [query for query in queries if query.startswith("LOCK TABLE")]
    assert len(locks) == int(fault != "early_pin")
    assert all(query.endswith(" ACCESS EXCLUSIVE MODE NOWAIT") for query in locks)
    assert unreferenced.await_count == (2 if fault in {"late_pin", "table_drop", "schema_drop"} else 1)


@pytest.mark.asyncio
async def test_retirement_requires_a_reference_probe():
    owner, proof = _retirement_proof()
    queries = []
    session = _retirement_session(owner, _retired_catalog(owner, proof), queries, "probe")
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="remains referenced"):
        await ownership.cleanup_entity_address_archive_publication(
            session, owner=owner, publication=proof, assert_unreferenced=None
        )
    assert len(queries) == 1 and queries[0].startswith("SELECT owner.oid AS owner_oid")


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["inventory", "oid", "legacy"])
async def test_protected_cutover_refuses_inventory_and_oid_drift(monkeypatch, fault):
    stage = SimpleNamespace(__tablename__="entity_address_unified_synthetic")
    swap = SimpleNamespace(stage_cls=stage, live_cls=SimpleNamespace(__main_table__="entity_address_unified"))
    context_by_field = {
        "snapshot_contract": source.CONTRACT,
        "protected_stage_oids": {stage.__tablename__: 41},
    }
    if fault == "inventory":
        context_by_field["protected_stage_oids"] = {}
    if fault == "legacy":
        context_by_field.clear()
    scalar = AsyncMock(return_value=True if fault == "legacy" else 99)
    monkeypatch.setattr(adoption.entity_address_unified.db, "scalar", scalar)
    before = deepcopy(context_by_field)
    with pytest.raises(RuntimeError, match="inventory differs|OID differs|legacy address rotation"):
        await adoption._require_protected_cutover("mrf", [swap], context_by_field)
    assert context_by_field == before
    assert scalar.await_count == int(fault != "inventory")


@asynccontextmanager
async def _observed_transaction(session, events):
    events.append("entered")
    try:
        yield session
    finally:
        events.append("exited")


@pytest.mark.asyncio
@pytest.mark.parametrize("partial", [False, True])
async def test_protected_reimport_requires_full_publisher_custody(monkeypatch, partial):
    events, context = [], {"retained": "unchanged"}
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=_guard_result([{"owner_oid": 52, "owner_safe": True, "caller_safe": False}])),
    )
    database = adoption.entity_address_unified.db
    monkeypatch.setattr(database, "scalar", AsyncMock(return_value=52))
    monkeypatch.setattr(database, "transaction", lambda: _observed_transaction(session, events))
    stage = SimpleNamespace(__tablename__="entity_address_unified_synthetic")
    reason = "full isolated replacement" if partial else "publisher is unavailable"
    with pytest.raises(RuntimeError, match=reason):
        await adoption._prepare_protected_ordinary_rotation("mrf", stage, partial, context)
    assert context == {"retained": "unchanged"}
    assert events == ([] if partial else ["entered", "exited"])
    assert session.execute.await_count == int(not partial)


def _sealed_guard_session(queries):
    async def execute(statement, parameters=None):
        query = str(statement)
        queries.append(query)
        if query.startswith("SELECT owner.oid AS owner_oid"):
            return _guard_result([{"owner_oid": 52, "owner_safe": True, "caller_safe": True}])
        if query.startswith("SELECT quote_ident(n.nspname)"):
            return _guard_result(
                [{"name": f'"mrf"."heap_{parameters["oid"]}"', "owner": '"synthetic_owner"', "relowner": 52}]
            )
        assert query.startswith(("SELECT s.oid", "SELECT quote_ident(principal.rolname)")), query
        return _guard_result([])

    return SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(side_effect=execute), scalar=AsyncMock(return_value=False)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["transaction", "stage", "collision", "rename"])
async def test_sealed_rotation_failure_never_overwrites_predecessor(monkeypatch, fault):
    queries = []
    session = _sealed_guard_session(queries)
    database = adoption.entity_address_unified.db
    binding = None if fault == "transaction" else SimpleNamespace(session=session)
    monkeypatch.setattr(database, "_transaction_binding", lambda: binding)
    values = [None if fault == "rename" else 41, None if fault == "stage" else 42, 99]
    scalar = AsyncMock(side_effect=values)
    status = AsyncMock(side_effect=RuntimeError("synthetic rename failed"))
    monkeypatch.setattr(database, "scalar", scalar)
    monkeypatch.setattr(database, "status", status)
    live = SimpleNamespace(__main_table__="entity_address_unified")
    stage = SimpleNamespace(__tablename__="entity_address_unified_synthetic")
    reasons_by_fault = {
        "transaction": "publisher transaction",
        "stage": "stage is missing",
        "collision": "retained name",
        "rename": "rename failed",
    }
    with pytest.raises(RuntimeError, match=reasons_by_fault[fault]):
        await adoption._swap_sealed_stage_table("mrf", live, stage, 52)
    assert status.await_count == int(fault == "rename")
    assert scalar.await_count == {"transaction": 0, "stage": 2, "collision": 3, "rename": 2}[fault]
    assert not any(query.startswith("ALTER") for query in queries)
    if fault == "rename":
        assert (
            status.await_args.args[0]
            == 'ALTER TABLE "mrf"."entity_address_unified_synthetic" RENAME TO "entity_address_unified"'
        )


@pytest.mark.asyncio
async def test_v2_restore_rejects_legacy_completion_before_mutation():
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="protected loaded preparation"):
        await restore.finalize_entity_address_archive_restore(
            session, owner=_set_owner(), semantic_receipt=_set_receipt(), db_schema="mrf", import_date="synthetic"
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_v2_restore_rejects_unauthenticated_rehydration():
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="authenticated sealed activation"):
        await restore.rehydrate_entity_address_archive_restore(session, stored={"semantic_receipt": _set_receipt()})
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_precreation_rejects_unsupported_contract():
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="contract is unsupported"):
        await restore.precreate_entity_address_archive_restore(
            session,
            dataset_id=_set_owner().dataset_id,
            db_schema="mrf",
            import_date="synthetic",
            contract="unsupported",
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_completion_requires_the_v2_inventory():
    owner, queries = _owner(), []

    async def execute(statement, _parameters=None):
        query = str(statement)
        queries.append(query)
        if query.startswith("SELECT relname, oid"):
            return _guard_result([{"relname": name, "oid": oid} for name, oid in owner.relation_oids])
        assert query.startswith("SELECT relation.oid"), query
        return _guard_result(
            [{"oid": oid, "relkind": "r", "index_table_oid": None} for _name, oid in owner.relation_oids]
        )

    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(return_value=owner.schema_oid),
        execute=AsyncMock(side_effect=execute),
    )
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="v2 inventory differs"):
        await restore.complete_entity_address_archive_restore(
            session, owner=owner, db_schema="mrf", import_date="synthetic"
        )
    assert len(queries) == 2 and all(query.startswith("SELECT") for query in queries)


@pytest.mark.asyncio
async def test_restore_namespace_rejects_alias_oid_substitution():
    owner = _set_owner()
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[owner.schema_oid, 1, 999]), execute=AsyncMock())
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="alias authority OID differs"):
        await restore._drop_empty_owned_schema(session, owner)
    session.execute.assert_not_awaited()
    assert session.scalar.await_args.args[1] == {
        "relation": f'"{owner.schema_name}"."{preparation.alias.AUTHORITY_TABLE}"'
    }


def test_restore_input_cannot_mix_seven_and_eight_tables():
    stored = _stored_restore()
    stored["semantic_receipt"] = _set_receipt()
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="contract inventory differs"):
        restore._validated_restore_input(stored)


def test_restore_context_cannot_relabel_the_legacy_contract():
    stored = _stored_restore()
    stored["context"]["snapshot_contract"] = source.CONTRACT
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="context contract differs"):
        restore._validated_rehydration_state(stored)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", [False, True])
async def test_receipt_capture_rejects_unsupported_contract(stage):
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="contract is unsupported"):
        if stage:
            _schema, _date, names = restore._stage_plan(db_schema="mrf", import_date="synthetic")
            await receipt.capture_entity_address_stage_integrity_receipt(
                session, schema_name="mrf", stage_table_names=names, contract="unsupported"
            )
        else:
            await receipt.capture_entity_address_archive_receipt(session, schema_name="mrf", contract="unsupported")
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [("model_name", "Foreign"), ("row_count", True), ("schema_sha256", "invalid"), ("receipt_schema", "b" * 64)],
)
def test_set_receipt_rejects_malformed_table_identity(field, value):
    stored = _set_receipt()
    if field == "receipt_schema":
        stored["schema_sha256"] = value
    else:
        stored["tables"][0][field] = value
    reason = "set schema receipt differs" if field == "receipt_schema" else "set table receipt is invalid"
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match=f"^entity-address {reason}$"):
        receipt.validate_entity_address_archive_receipt(stored)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["absent", "malformed", "finished", "test"])
async def test_controlled_node_rejects_changed_source_attempt(fault):
    handoff = _handoff()
    run_by_field = {"params": {}, "finished_at": None, "node_id": "synthetic_node"}
    if fault in {"absent", "malformed"}:
        run_by_field["node_id"] = None if fault == "absent" else "foreign node"
    if fault == "finished":
        run_by_field["finished_at"] = "closed"
    if fault == "test":
        run_by_field["params"]["test_mode"] = True
    session = SimpleNamespace(execute=AsyncMock(return_value=_guard_result([run_by_field])))
    reasons_by_fault = {
        "absent": "controlled node differs",
        "malformed": "controlled node differs",
        "finished": "attempt already finished",
        "test": "source run differs",
    }
    with pytest.raises(RuntimeError, match=f"^address native publication {reasons_by_fault[fault]}$"):
        await publication.controlled_dependency_node(session, "mrf", _attempt_context(handoff))
    statement, parameters = session.execute.await_args.args
    assert "FOR UPDATE" not in str(statement) and parameters["status"] == "running"
    assert parameters["attempt_id"] == handoff["attempt_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_catalog_restore_preserves_original_failure(cleanup_fails):
    original = RuntimeError("synthetic catalog read failed")

    async def execute(statement, _parameters=None):
        if str(statement).startswith("SELECT set_config") and cleanup_fails:
            raise RuntimeError("synthetic setting restore failed")

    session = SimpleNamespace(scalar=AsyncMock(return_value="synthetic"), execute=AsyncMock(side_effect=execute))
    with pytest.raises(RuntimeError, match="catalog read failed") as failure:
        async with publication._catalog_search_path(session):
            raise original
    assert failure.value is original
    assert session.execute.await_count == 2
    assert session.execute.await_args.args[1] == {"path": "synthetic"}


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_readback", [False, True])
async def test_handoff_readback_drains_owned_cancellation(cancel_readback):
    started, release = asyncio.Event(), asyncio.Event()
    queries = []

    async def read_handoff(statement, parameters):
        queries.append((str(statement), parameters))
        started.set()
        await release.wait()
        if cancel_readback:
            raise asyncio.CancelledError()
        return None

    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(side_effect=read_handoff))
    events = []
    database = SimpleNamespace(transaction=lambda: _observed_transaction(session, events))
    handoff = _handoff()
    task = asyncio.create_task(publication._reconcile_handoff(database, handoff))
    await started.wait()
    if not cancel_readback:
        task.cancel()
        await asyncio.sleep(0)
    release.set()
    if cancel_readback:
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        assert await task is None
    assert events == ["entered", "exited"]
    assert queries[0][1] == handoff and " FOR UPDATE" in queries[0][0]
    assert "status IN ('finalizing','succeeded')" in queries[0][0]
    session.execute.assert_awaited_once()


def test_native_receipt_envelope_and_nullable_catalog_keys():
    with pytest.raises(RuntimeError, match="receipt exceeds its envelope"):
        publication._json("a" * publication.MAX_RECEIPT_BYTES)
    assert publication._catalog_column_names(None, {}) is None
    assert publication._catalog_column_names("{1,2}", {1: "id"}) == ["id", 0]


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["source", "stage", "receipt", "prepare"])
async def test_v2_source_rejects_ordinary_export_paths(entrypoint):
    factory, copy_rows = Mock(), AsyncMock()
    owner = _set_owner()
    reasons_by_entrypoint = {
        "source": "entity-address v2 source requires the bounded native COPY coordinator",
        "stage": "entity-address v2 stage requires the bounded native COPY coordinator",
        "receipt": "entity-address v2 export requires a retained protected source",
        "prepare": "entity-address protected source requires the set contract",
    }
    with pytest.raises(ValueError, match=f"^{reasons_by_entrypoint[entrypoint]}$"):
        if entrypoint == "source":
            await source.export_entity_address_archive_source(
                factory, schema_name="mrf", archive_copy=copy_rows, contract=source.CONTRACT
            )
        elif entrypoint == "stage":
            await source.export_entity_address_archive_stage(
                factory,
                schema_name="mrf",
                dataset_id=owner.dataset_id,
                archive_copy=copy_rows,
                contract=source.CONTRACT,
            )
        elif entrypoint == "receipt":
            await source.export_entity_address_archive_with_receipt(
                factory,
                schema_name="mrf",
                dataset_id=owner.dataset_id,
                queued_serving_capture={},
                archive_copy=copy_rows,
                contract=source.CONTRACT,
            )
        else:
            capture = source.EntityAddressArchiveSourceCapture(
                source.LEGACY_CONTRACT, "mrf", source.entity_address_archive_relations(), "00000003-0000001B-1"
            )
            await source.prepare_entity_address_archive_source(
                SimpleNamespace(),
                source_capture=capture,
                dataset_id=owner.dataset_id,
                source_copy=source.EntityAddressSourceCopy(copy_rows, 512, 100),
                on_precreated=AsyncMock(),
            )
    factory.assert_not_called()
    copy_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_copy_deadline_refuses_driver_before_payload():
    copy_rows = AsyncMock()
    with pytest.raises(TimeoutError, match="COPY deadline exceeded"):
        await source._copy_pinned_model_rows(
            SimpleNamespace(),
            source.entity_address_unified.EntityAddressUnified,
            source_schema="mrf",
            target_schema="candidate",
            copy_source_rows=copy_rows,
            max_bytes=512,
            deadline=0,
        )
    copy_rows.assert_not_awaited()


@pytest.mark.parametrize("contract", ["unsupported", None])
def test_source_inventory_rejects_unsupported_contract(contract):
    with pytest.raises(ValueError, match="contract is unsupported"):
        source.entity_address_archive_relations(contract)
