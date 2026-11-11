# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen proof successors retain the exact historical contracts and indexed admission."""

import json
import re

import pytest

from db import migration_rooted_publication_freeze as freeze
from process import provider_directory_source_callable_custody as custody
from process.provider_directory_rooted_graph_publication_candidate import ROOTS
from tests.test_acquisition_validator_argument_types_migration import _current_functions, _migration
from tests.test_provider_directory_generic_source_storage import _catalog_row
from tests.test_source_validator_argument_types_migration import _load

SCHEMA = "snapshot_schema"


def _current_bodies(names=freeze.ORIGINAL_DIGESTS):
    bodies_by_name = {}
    if freeze.WORK_GUARD in names:
        sql = _load("20260909120000_fhir_request_failure_budget.py")._work_guard_sql(SCHEMA)
        bodies_by_name[freeze.WORK_GUARD] = sql.split("$guard$", 2)[1]
    for (name, _kinds), attributes in _current_functions().items():
        if name not in names:
            continue
        body = attributes[-1]
        for caller, old, new, count in _migration()._calls(SCHEMA):
            if name == caller:
                assert body.count(old) == count
                body = body.replace(old, new)
        bodies_by_name[name] = body
    assert bodies_by_name.keys() == set(names)
    return bodies_by_name


@pytest.mark.parametrize("name", freeze.ORIGINAL_DIGESTS)
def test_successor_preserves_exact_history_and_only_its_reviewed_extension(name):
    original = _current_bodies()[name]
    expected = freeze.ORIGINAL_DIGESTS[name]
    assert freeze.body_digest(original, SCHEMA) == expected
    successor = freeze.wrapped_body(name, SCHEMA, original)
    assert freeze.is_approved_successor(name, SCHEMA, successor, expected)
    assert custody._is_approved_body({"proname": name, "prosrc": successor}, SCHEMA, expected)
    assert not freeze.is_approved_successor(name, SCHEMA, successor.replace("SELECT", "SELECT false AND", 1), expected)
    forged = freeze.wrapped_body(name, SCHEMA, original + "\n-- changed predecessor\n")
    assert not freeze.is_approved_successor(name, SCHEMA, forged, expected)


def test_single_root_successor_keeps_record_types():
    original = _current_bodies()[freeze.SINGLE_ROOT_GUARD]
    successor = freeze.wrapped_body(freeze.SINGLE_ROOT_GUARD, SCHEMA, original)
    old, new = freeze._SINGLE_ROOT_SELECT
    assert successor.replace(new, old, 1) == original
    assert successor.count("::text AS acquisition_source_id") == 2
    assert successor.count("::text AS acquisition_endpoint_id") == 2
    assert successor.count("INTO current_root") == 2
    assert "FOR SHARE OF header, parent, graph_endpoint" in successor
    assert "FOR SHARE OF header, parent;" in successor
    for field in ("source", "endpoint"):
        changed = successor.replace(f"header.acquisition_{field}_id::text", f"header.acquisition_{field}_id", 1)
        assert not freeze.is_approved_successor(
            freeze.SINGLE_ROOT_GUARD, SCHEMA, changed, freeze.ORIGINAL_DIGESTS[freeze.SINGLE_ROOT_GUARD]
        )


def test_protected_proof_uses_exact_indexed_handoff_and_exact_physical_generation():
    query = freeze.native_receipt_sql(SCHEMA)
    assert "WITH proof AS MATERIALIZED (" in query and "), selected AS MATERIALIZED (" in query
    assert "parent ON parent.dataset_id=candidate_dataset_id" in query
    assert "WHERE h.dataset_id=candidate_dataset_id\n    ) SELECT" in query
    assert "SELECT count(*)=1 AND bool_and(" in query and "LIMIT 1" not in query
    assert "preparation.handoff=parent.candidate_storage_json" in query
    assert "preparation.preparation_id" not in query
    assert "preparation.handoff,preparation.publication,preparation.state" in query
    assert "c.relfilenode::bigint<>(entry.value->>'relfilenode')::bigint" in query
    assert "publication->'rooted_input_fence'" in query
    assert "jsonb_array_elements(b->'catalog')" in query
    assert "string_agg" not in query
    assert "am.amname='hash'" in query
    assert "i.indexprs IS NULL AND i.indpred IS NULL" in query


def test_catalog_projection_preserves_all_compared_fields_for_seven_objects():
    query = freeze.native_receipt_sql(SCHEMA)
    projection = re.search(r"SELECT catalog\.oid,.*?FROM \(", query, re.S)[0]
    compared_field_names = re.findall(r"catalog\.(\w+)", projection)
    assert len(compared_field_names) == len(set(compared_field_names)) == 14
    ignored_field_names = {"relowner", "parents", "relispartition", "bound", "inert"}
    complete_catalog_rows = [
        _catalog_row()
        | dict(
            oid=index,
            relname=root,
            nspname="candidate",
            bound=None if index < 2 else "FOR VALUES IN ('edition')",
            partition_key=None,
            parents=[] if index < 2 else [100 + index],
            relispartition=index >= 2,
            inert=index < 2,
            constraints=[{"name": "sample_key", "type": "p", "valid": True}],
            indexes=[{"oid": 200 + index, "keys": ["dataset_id"], "valid": True}],
        )
        for index, root in enumerate(ROOTS, 1)
    ]
    assert len(complete_catalog_rows) == 7
    assert all(
        set(catalog_by_field) == set(compared_field_names) | ignored_field_names
        for catalog_by_field in complete_catalog_rows
    )
    previous_rows = [
        {
            field_name: field_value
            for field_name, field_value in catalog_by_field.items()
            if field_name not in ignored_field_names
        }
        for catalog_by_field in complete_catalog_rows
    ]
    projected_rows = [
        {field_name: catalog_by_field[field_name] for field_name in compared_field_names}
        for catalog_by_field in complete_catalog_rows
    ]
    assert json.dumps(projected_rows, sort_keys=True) == json.dumps(previous_rows, sort_keys=True)
    assert [catalog_by_field["oid"] for catalog_by_field in projected_rows] == [
        catalog_by_field["oid"] for catalog_by_field in complete_catalog_rows
    ]
    assert freeze.frozen_table_custody_sql(SCHEMA) in query
    assert "(SELECT count(*) FROM jsonb_each(b->'tables'))=7" in query


def test_historical_candidates_keep_original_validation_dispatch():
    body = freeze.wrapped_body(freeze.ROOTED, SCHEMA, _current_bodies()[freeze.ROOTED])
    assert "candidate_storage_json->>'contract'='" + freeze.CONTRACT + "'" in body
    assert freeze.original_query(body) == _current_bodies()[freeze.ROOTED]


def test_fresh_core_defers_publication_until_protected_catalog_exists(monkeypatch):
    from unittest.mock import Mock

    connection = Mock()
    connection.in_transaction.return_value = True
    connection.execute.side_effect = [Mock(scalar_one_or_none=lambda: True), Mock(scalar_one_or_none=lambda: None)]
    observed_signatures = []
    monkeypatch.setattr(
        freeze, "_function_snapshot", lambda _connection, signature: observed_signatures.append(signature)
    )
    with pytest.raises(RuntimeError, match="rooted_publication_function_identity_changed"):
        freeze.upgrade_rooted_publication_freeze(connection, SCHEMA)
    assert observed_signatures == ['"snapshot_schema"."guard_provider_directory_rooted_graph_work"()']
    connection.exec_driver_sql.assert_not_called()


def test_function_dependency_inventory_binds_native_oid_values():
    from unittest.mock import Mock

    connection = Mock()
    freeze._function_auxiliary(connection, {"function": ("signature", {"identity": {"oid": "123"}})})
    assert connection.execute.call_args.args[1] == {"oids": [123]}


@pytest.mark.asyncio
async def test_changed_header_check_is_rejected_before_callable_execution():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process.provider_directory_generic_candidate import GenericCandidateError
    from process.provider_directory_rooted_graph_header_checks import capture_rooted_header_check

    connection = SimpleNamespace(fetchval=AsyncMock(return_value={"definition": "CHECK (true)"}), execute=AsyncMock())
    with pytest.raises(GenericCandidateError, match="rooted_header_check_changed"):
        await capture_rooted_header_check(connection, SCHEMA)
    assert connection.fetchval.await_count == 1
    connection.execute.assert_not_called()


@pytest.mark.parametrize("in_transaction,admin", [(False, True), (True, False), (True, None)])
def test_freeze_install_requires_original_admin_before_any_catalog_change(in_transaction, admin):
    from unittest.mock import Mock

    connection = Mock()
    connection.in_transaction.return_value = in_transaction
    connection.execute.return_value.scalar_one_or_none.return_value = admin
    with pytest.raises(RuntimeError, match="original_admin_required"):
        freeze.upgrade_rooted_publication_freeze(connection, SCHEMA)
    connection.exec_driver_sql.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parent,candidate,accepted",
    [
        ([{"signature": {"keys": "1 2"}, "valid": True}], [{"signature": '{"keys":"1 2"}', "valid": True}], True),
        ([{"signature": {"keys": "1 2"}, "valid": True}], [{"signature": {"keys": "1"}, "valid": True}], False),
        ([{"signature": {"keys": "1"}, "valid": False}], [{"signature": {"keys": "1"}, "valid": True}], False),
        ([], [], False),
        ([{"signature": {"keys": "1"}, "valid": True}] * 2, [{"signature": {"keys": "1"}, "valid": True}], False),
    ],
)
async def test_parent_indexes_must_be_complete_before_attachment(parent, candidate, accepted):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process.provider_directory_generic_candidate import GenericCandidateError
    from process.provider_directory_rooted_graph_candidate_indexes import require_prepared_parent_indexes

    connection = SimpleNamespace(fetch=AsyncMock(side_effect=[parent, candidate]), execute=AsyncMock())
    if accepted:
        await require_prepared_parent_indexes(connection, SCHEMA, "private_source", {"root": "candidate"})
    else:
        with pytest.raises(GenericCandidateError, match="rooted_parent_index"):
            await require_prepared_parent_indexes(connection, SCHEMA, "private_source", {"root": "candidate"})
    connection.execute.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_npi_index_is_prepared_without_building_unknown_definitions():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process import provider_directory_rooted_graph_candidate_indexes as indexes
    from process.provider_directory_generic_candidate import GenericCandidateError

    legacy_by_field = {"signature": {"keys": "0 1 2", "predicate": indexes._LEGACY_NPI_PREDICATE}, "valid": True}
    current_by_field = {"signature": {"keys": "0 1 2", "predicate": indexes._CURRENT_NPI_PREDICATE}, "valid": True}
    relations_by_root = {"provider_directory_dataset_resource": "candidate"}
    connection = SimpleNamespace(
        fetch=AsyncMock(
            side_effect=[[legacy_by_field], [current_by_field], [legacy_by_field], [current_by_field, legacy_by_field]]
        ),
        execute=AsyncMock(),
    )
    await indexes.prepare_rooted_parent_indexes(connection, SCHEMA, "private_source", relations_by_root)
    connection.execute.assert_awaited_once()
    assert "CREATE INDEX" in connection.execute.call_args.args[0]
    assert indexes._LEGACY_NPI_PREDICATE in connection.execute.call_args.args[0]
    changed_by_field = {"signature": {"keys": "0 1 2", "predicate": "false"}, "valid": True}
    connection.fetch = AsyncMock(side_effect=[[changed_by_field], [current_by_field]])
    connection.execute.reset_mock()
    with pytest.raises(GenericCandidateError, match="rooted_parent_index_unsupported"):
        await indexes.prepare_rooted_parent_indexes(connection, SCHEMA, "private_source", relations_by_root)
    connection.execute.assert_not_called()


@pytest.mark.asyncio
async def test_index_assertion_runs_under_parent_locks(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process import provider_directory_rooted_graph_candidate_indexes as indexes
    from process import provider_directory_rooted_graph_publication_finalize as finalize

    events = []
    connection = SimpleNamespace(execute=AsyncMock(side_effect=lambda sql, *args: events.append(sql)))
    monkeypatch.setattr(
        indexes, "require_prepared_parent_indexes", AsyncMock(side_effect=lambda *args: events.append("indexes"))
    )
    runtime = SimpleNamespace(catalog_schema=SCHEMA, candidate_schema="private_source")
    document_by_field = {
        "identity": {"dataset_id": "candidate"},
        "tables": {root: {"name": root} for root in finalize.ROOTS},
    }
    await finalize._attach_frozen_group(connection, runtime, document_by_field)
    assert events[0].startswith("LOCK TABLE ") and "SHARE UPDATE EXCLUSIVE MODE NOWAIT" in events[0]
    assert events[1] == "indexes"
    assert all("ATTACH PARTITION" in statement for statement in events[2:])


@pytest.mark.asyncio
async def test_frozen_hash_preserves_projection_date_contract(monkeypatch):
    import hashlib
    import importlib
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from process import provider_directory_rooted_graph_publication_candidate as candidate

    directory = importlib.import_module("process.provider_directory_fhir")
    resource_by_field = {
        "resource_type": "Practitioner",
        "resource_id": "resource",
        "payload_hash": "a" * 64,
        "payload_json": '{"resource_id":"resource"}',
    }
    cursor = SimpleNamespace(fetch=AsyncMock(side_effect=[[resource_by_field], []]))
    connection = SimpleNamespace(cursor=AsyncMock(return_value=cursor))
    monkeypatch.setattr(directory, "resource_payload_sha256_for_contract", Mock(return_value="a" * 64))
    monkeypatch.setattr(directory, "_validated_semantic_dataset_payload", Mock())
    projection = Mock()
    monkeypatch.setattr(directory, "_assert_practitioner_semantic_projection_as_of", projection)
    document_by_field = {
        "storage": {"storage_schema": "private_source"},
        "identity": {"dataset_id": "candidate", "semantic_projection_as_of": "2026-01-01"},
        "tables": {candidate.RESOURCE: {"name": "candidate"}},
    }
    digest, count = await candidate._frozen_resource_hash(connection, document_by_field)
    assert (
        count == 1
        and digest
        == hashlib.sha256(candidate.candidate_json(["Practitioner", "resource", "a" * 64]).encode()).hexdigest()
    )
    assert projection.call_args.args[-1] == "2026-01-01"


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "owner", "native", "lineage", "events", "validated", "inheritance"])
async def test_existing_foreign_guard_scope_is_authenticated(monkeypatch, drift):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process import provider_directory_cms_control_custody as custody
    from process import provider_directory_rooted_graph_foreign_guards as guards
    from process.provider_directory_generic_candidate import GenericCandidateError

    guard_by_field = {
        "parent_owner": 100,
        "referencing_owner": 100,
        "constraint": {"oid": "1000", "convalidated": True, "conenforced": True, "connoinherit": True},
        "hooks": [
            {
                "trigger": {
                    "tgtype": event,
                    "tgisinternal": True,
                    "tgenabled": "O",
                    "tgconstraint": "1000",
                    "tgparentid": "0",
                    "tgfoid": str(function),
                }
            }
            for event, function in ((9, 1646), (17, 1655))
        ],
    }
    if drift == "owner":
        guard_by_field["referencing_owner"] = 101
    if drift == "native":
        guard_by_field["hooks"][0]["trigger"]["tgisinternal"] = False
    if drift == "lineage":
        guard_by_field["hooks"][0]["trigger"]["tgparentid"] = "1"
    if drift == "events":
        guard_by_field["hooks"][0]["trigger"]["tgtype"] = 5
    if drift == "inheritance":
        guard_by_field["constraint"]["connoinherit"] = False
    if drift == "validated":
        guard_by_field["constraint"]["convalidated"] = False
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=[guard_by_field]))
    verify = AsyncMock()
    monkeypatch.setattr(custody, "_require_native_foreign_functions", verify)
    if drift is None:
        assert await guards.capture_parent_foreign_guards(connection, SCHEMA) == [guard_by_field]
        verify.assert_awaited_once_with(connection, {1646, 1655})
    else:
        with pytest.raises(GenericCandidateError, match="rooted_foreign_guard_changed"):
            await guards.capture_parent_foreign_guards(connection, SCHEMA)
        verify.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [True, False, None])
async def test_attached_foreign_guard_readback_requires_complete_proof(accepted):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process import provider_directory_rooted_graph_foreign_guards as guards
    from process.provider_directory_generic_candidate import GenericCandidateError

    connection = SimpleNamespace(fetchval=AsyncMock(return_value=accepted))
    document_by_field = {"snapshot": {"foreign_guards": [{"root": "resource", "hooks": []}]}}
    if accepted is True:
        await guards.require_inherited_foreign_guards(connection, document_by_field, "resource", 123)
        assert connection.fetchval.call_args.args[1:] == (
            123,
            "resource",
            '[{"root": "resource", "hooks": []}]',
        )
    else:
        with pytest.raises(GenericCandidateError, match="rooted_foreign_guard_changed"):
            await guards.require_inherited_foreign_guards(connection, document_by_field, "resource", 123)


@pytest.mark.parametrize("drift", [None, "missing", "native", "owner", "body"])
def test_guard_implication_predicates_require_exact_unchanged_custody(monkeypatch, drift):
    names = {"provider_directory_rooted_graph_dataset_valid", "provider_directory_rooted_graph_dataset_ready"}
    bodies_by_name = _current_bodies(names)
    records_by_signature = {
        '"snapshot_schema"."' + name + '"(text)': {"body": body, "identity": {"proowner": "100"}}
        for name, body in bodies_by_name.items()
    }
    target = next(iter(records_by_signature))
    if drift == "missing":
        records_by_signature[target] = None
    if drift == "owner":
        records_by_signature[target]["identity"]["proowner"] = "101"
    if drift == "body":
        records_by_signature[target]["body"] += " -- changed"
    monkeypatch.setattr(freeze, "_function_snapshot", lambda connection, signature: records_by_signature[signature])
    monkeypatch.setattr(freeze, "_is_native_definition", lambda row, name: drift != "native")
    if drift is None:
        freeze._require_guard_implications(object(), SCHEMA, "100")
    else:
        with pytest.raises(RuntimeError, match="guard_implication_changed"):
            freeze._require_guard_implications(object(), SCHEMA, "100")


def test_strongest_guard_predicate_preserves_null_and_failure_semantics():
    from itertools import product

    def sql_and(left, right):
        return False if left is False or right is False else (None if left is None or right is None else True)

    for status, intrinsic, lineage, headers_ready in product(
        (None, "building", "validated", "published", "superseded", "unknown"),
        (True, False, None),
        (True, False, None),
        (True, False, None),
    ):
        valid = sql_and(intrinsic, sql_and(intrinsic, lineage))
        is_ready = headers_ready is True and valid is True
        is_original_invalid = (
            intrinsic is not True
            or (status in {"validated", "published"} and valid is not True)
            or (status == "published" and is_ready is not True)
        )
        strongest = is_ready if status == "published" else valid if status == "validated" else intrinsic
        direct = sql_and(
            sql_and(intrinsic, lineage if status in {"validated", "published"} else True),
            headers_ready if status == "published" else True,
        )
        assert is_original_invalid == (strongest is not True) == (direct is not True)
    extension = freeze._guard_extension(SCHEMA)
    assert "FROM receipt) IS DISTINCT FROM TRUE" in extension
    assert "receipt(accepted) AS MATERIALIZED" in extension
    for name in (
        freeze.ROOTED,
        "provider_directory_rooted_graph_dataset_valid",
        "provider_directory_rooted_graph_dataset_ready",
    ):
        assert freeze.storage_relation(SCHEMA, name) + "(NEW.dataset_id" not in extension
    assert "WHERE proof.state='published'" in extension
    assert "RETURN NULL" in extension


def test_stale_native_events_only_yield_to_a_later_published_tuple():
    from itertools import product

    for event, final, same_tuple, current, preparation_published in product(
        ("building", "validated", "published", "superseded"),
        (None, "building", "validated", "published", "superseded"),
        (True, False),
        (True, False),
        (True, False),
    ):
        can_coalesce = (
            event in {"building", "validated"}
            and final == "published"
            and not same_tuple
            and current
            and preparation_published
        )
        if can_coalesce:
            assert event != "published" and final != "superseded" and final is not None
        if same_tuple or event in {"published", "superseded"} or final != "published":
            assert not can_coalesce
    extension = freeze._guard_extension(SCHEMA)
    assert "final.ctid<>NEW.ctid" in extension
    assert "final.status='published' AND final.is_current IS TRUE" in extension
    assert extension.count("WHERE proof.state='published'") == 2
    original = _current_bodies()[freeze.GUARD]
    successor = freeze.wrapped_body(freeze.GUARD, SCHEMA, original)
    assert successor.replace("\n" + extension, "", 1) == original
    assert "ELSIF TG_OP = 'DELETE' THEN" in original
    assert "NEW.dataset_id" in original and "OLD.dataset_id" in original
    assert "NEW.created_at" in original and "OLD.created_at" in original


def test_direct_header_predicate_is_literal_ready_contract_without_valid():
    name = "provider_directory_rooted_graph_dataset_ready"
    body = _current_bodies({name})[name]
    valid = freeze.storage_relation(SCHEMA, "provider_directory_rooted_graph_dataset_valid")
    original = " ".join(body.split()).removesuffix(";")
    for argument in ("header.dataset_id::text", "header.dataset_id"):
        original = original.replace(" AND " + valid + "(" + argument + ")", "")
    direct = " ".join(("SELECT " + freeze._published_headers_sql(SCHEMA)).split())
    assert direct == original


def test_direct_lineage_retains_every_attested_comparison():
    lineage = freeze._lineage_receipt_sql(SCHEMA)
    direct = freeze._lineage_metadata_sql(SCHEMA)
    assert (
        lineage
        == "SELECT " + freeze.storage_relation(SCHEMA, freeze.ROOTED) + "(candidate_dataset_id)\n        AND " + direct
    )
    for comparison in (
        "to_jsonb(c)=proof.handoff->'lineage'->'cohort'",
        "official.dataset_id=c.official_dataset_id",
        "official.endpoint_id=c.official_endpoint_id",
        "official.acquisition_root_run_id=c.official_acquisition_root_run_id",
        "official.dataset_hash=c.official_dataset_hash",
        "official.status='published' AND official.is_current IS TRUE",
    ):
        assert comparison in direct
