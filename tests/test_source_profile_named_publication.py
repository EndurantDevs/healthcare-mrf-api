# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named publication preserves content, configured access and atomic rollback fences."""

import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from process import florida_projection_named_source as projection
from process import source_profile_result_archive as archive
from tests.test_source_profile_result_archive import _versioned_manifest


def prepared_result():
    importer = archive.PROJECTION_IMPORTER
    return archive.PreparedResult(
        _versioned_manifest(archive.CONTRACT, importer),
        archive.StageOwnership(importer, uuid4(), tuple(sorted(archive.source_spec(importer).table_names))),
    )


@pytest.mark.parametrize("edition", ["named", "saved_names"])
def test_ownership_is_portable_and_rejects_missing_or_changed_names(edition):
    prepared = prepared_result()
    ownership_by_field = archive.ownership_dict(prepared.ownership)
    if edition == "saved_names":
        ownership_by_field = {key: field_value for key, field_value in ownership_by_field.items() if key != "relations"}
        ownership_by_field.update(
            schema_oid="opaque", relation_oids=[[name, "opaque"] for name in prepared.ownership.relation_names]
        )
    assert archive.ownership_value(ownership_by_field) == prepared.ownership
    entries = "relations" if edition == "named" else "relation_oids"
    for invalid in (None, [{}], [[]], ownership_by_field[entries][:-1]):
        with pytest.raises(archive.SourceProfileArchiveError):
            archive.ownership_value({**ownership_by_field, entries: invalid})
    with pytest.raises(archive.SourceProfileArchiveError):
        archive.ownership_value({**ownership_by_field, "schema_name": "other"})


def test_named_validation_keeps_every_data_digest_exact():
    prepared = prepared_result()
    prepared = archive.PreparedResult(_versioned_manifest(archive.LEGACY_CONTRACT), prepared.ownership)
    receipt = archive._validation(prepared, "a" * 64, None, "serving", None, uuid4())
    with pytest.raises(archive.SourceProfileArchiveError, match="digest differs"):
        archive.logical_validation({**receipt, "sealed_owner_oid": "opaque", "result_sha256": "b" * 64})
    for field in ("result_sha256", "package_id"):
        changed_by_field = {**receipt, field: "b" * 64}
        with pytest.raises(archive.SourceProfileArchiveError, match="digest differs"):
            archive.logical_validation(changed_by_field)
        changed_by_field["validation_sha256"] = archive._digest(
            {key: value for key, value in changed_by_field.items() if key != "validation_sha256"}
        )
        assert archive.logical_validation(changed_by_field) != archive.logical_validation(receipt)


@pytest.mark.parametrize("location", ["live", "retained", "legacy", "missing", "ambiguous"])
@pytest.mark.asyncio
async def test_projection_locator_resolves_qualified_names_with_one_row_reads(monkeypatch, location):
    run = "a" * 32
    retained = projection._retained_name(run)
    legacy_names = [projection.PROJECTION + "_old", projection.PROJECTION + "_retained_1"]
    generations_by_name = {
        projection.PROJECTION: "b" * 32,
        retained: None,
        legacy_names[0]: "c" * 32,
        legacy_names[1]: "d" * 32,
    }
    target_by_location = {"live": projection.PROJECTION, "retained": retained, "legacy": legacy_names[0]}
    if location in target_by_location:
        generations_by_name[target_by_location[location]] = run
    if location == "ambiguous":
        generations_by_name.update({legacy_names[0]: run, legacy_names[1]: run})
    queries = []

    async def generation_row(statement):
        query = str(statement)
        queries.append(query)
        assert query.endswith("LIMIT 1")
        return next(value for name, value in generations_by_name.items() if f'"{name}"' in query)

    monkeypatch.setattr(
        projection.native, "_relation_oid", AsyncMock(side_effect=lambda *args: generations_by_name[args[2]] and 1)
    )
    listing = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: legacy_names))
    session = SimpleNamespace(execute=AsyncMock(return_value=listing), scalar=generation_row)
    if location in target_by_location:
        assert await projection.named_source_projection(session, "serving", run) == {
            "schema": "serving",
            "table_name": target_by_location[location],
        }
        assert session.execute.await_count == (1 if location == "legacy" else 0)
    else:
        with pytest.raises(projection.legacy.FloridaProjectionArchiveError, match="missing or ambiguous"):
            await projection.named_source_projection(session, "serving", run)
    assert not any("<>" in query for query in queries)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "pointer", "scope", "dependent", "access"])
async def test_named_switch_checks_before_rename_and_updates_one_logical_pointer(monkeypatch, fault):
    prepared = prepared_result()
    expected_by_field = {"current_run_id": "a" * 32, "previous_run_id": None}
    candidate_by_field = {"schema": "serving", "table_name": "synthetic_candidate"}
    observed = {**expected_by_field, "current_run_id": "b" * 32} if fault == "pointer" else expected_by_field
    monkeypatch.setattr(projection, "_publication_lock", AsyncMock())
    monkeypatch.setattr(projection, "named_publication_pointer", AsyncMock(return_value=observed))
    monkeypatch.setattr(projection, "named_projection_seal", AsyncMock())
    monkeypatch.setattr(projection.native, "_lock_family", AsyncMock())
    monkeypatch.setattr(projection.native, "_relation_oid", AsyncMock(return_value=None))
    dependencies = AsyncMock(side_effect=RuntimeError("dependent") if fault == "dependent" else None)
    freeze = AsyncMock(side_effect=PermissionError("access") if fault == "access" else None)
    replay = AsyncMock()
    monkeypatch.setattr(projection, "_require_named_projection_dependencies", dependencies)
    monkeypatch.setattr(projection, "freeze_named_projection", freeze)
    monkeypatch.setattr(projection, "_named_projection_reads", AsyncMock(return_value=[{"synthetic": True}]))
    monkeypatch.setattr(projection, "_replay_named_projection_reads", replay)
    session = SimpleNamespace(scalar=AsyncMock(return_value=fault != "scope"), execute=AsyncMock())
    pending = projection.publish_named_source_projection(
        session, "serving", candidate_by_field, expected_by_field, prepared.manifest["run_id"]
    )
    if fault:
        with pytest.raises((RuntimeError, PermissionError)):
            await pending
        session.execute.assert_not_awaited()
    else:
        result_by_field = await pending
        assert result_by_field["relation"] == {"schema": "serving", "table_name": projection.PROJECTION}
        queries = [str(call.args[0]) for call in session.execute.await_args_list]
        assert len(queries) == 3 and queries[0].startswith("ALTER TABLE") and queries[1].startswith("ALTER TABLE")
        assert "_r_" in queries[0] and '"synthetic_candidate" RENAME TO' in queries[1]
        assert "ON CONFLICT(source_key)" in queries[2]
        assert session.execute.await_args.args[1]["previous"] == expected_by_field["current_run_id"]
        assert [call.args for call in replay.await_args_list] == [
            (session, "serving", projection.PROJECTION, [{"synthetic": True}]),
            (session, "serving", "synthetic_candidate", [{"synthetic": True}]),
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize("equal", [False, True])
async def test_named_rollback_compares_full_typed_content_before_any_switch(monkeypatch, equal):
    prepared = prepared_result()
    target_by_field = {"schema": "serving", "table_name": projection.PROJECTION + "_old"}
    pointer_by_field = {
        "current_run_id": "b" * 32,
        "previous_run_id": prepared.manifest["run_id"],
        "previous_relation": target_by_field,
    }
    monkeypatch.setattr(projection, "named_publication_pointer", AsyncMock(return_value=pointer_by_field))
    monkeypatch.setattr(projection.native, "_lock_family", AsyncMock())
    parity = AsyncMock(return_value=equal)
    publish = AsyncMock()
    monkeypatch.setattr(projection.native, "_is_model_table_equal", parity)
    monkeypatch.setattr(projection, "publish_named_source_projection", publish)
    session = object()
    if equal:
        await projection.rollback_named_source_projection(session, prepared, "serving", pointer_by_field)
        publish.assert_awaited_once_with(
            session, "serving", target_by_field, pointer_by_field, prepared.manifest["run_id"], roles=None
        )
    else:
        with pytest.raises(projection.legacy.FloridaProjectionArchiveError, match="content differs"):
            await projection.rollback_named_source_projection(session, prepared, "serving", pointer_by_field)
        publish.assert_not_awaited()
    assert parity.await_args.kwargs == {
        "left_schema": prepared.ownership.schema_name,
        "left_name": projection.PROJECTION,
        "right_schema": "serving",
        "right_name": target_by_field["table_name"],
    }


@pytest.mark.asyncio
async def test_read_grants_replay_only_select_and_preserve_grant_option():
    session = SimpleNamespace(execute=AsyncMock())
    grants = [
        {"public": True, "rolname": None, "is_grantable": False, "owner": False},
        {"public": False, "rolname": "synthetic_reader", "is_grantable": True, "owner": False},
        {"public": False, "rolname": "synthetic_prior_owner", "is_grantable": True, "owner": True},
    ]
    await projection._replay_named_projection_reads(session, "serving", "candidate", grants)
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    assert all(query.startswith('GRANT SELECT ON TABLE "serving"."candidate" TO ') for query in queries)
    assert queries[0].endswith("PUBLIC") and queries[1].endswith('"synthetic_reader" WITH GRANT OPTION')
    assert queries[2].endswith('"synthetic_prior_owner"')


def test_named_guard_migration_only_replaces_the_existing_function(monkeypatch):
    path = Path(__file__).parents[1] / "alembic/versions/20261114170000_source_profile_named_attachment.py"
    spec = importlib.util.spec_from_file_location("named_source_attachment", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    execute = Mock()
    monkeypatch.setattr(migration.op, "execute", execute)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_serving")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.upgrade()
    sql = execute.call_args.args[0]
    assert "to_regclass" in sql and "to_regprocedure" in sql and "IS NULL THEN RETURN" in sql
    assert "CREATE OR REPLACE FUNCTION" in sql and "CREATE TRIGGER" not in sql
    assert "OWNER TO" not in sql and "GRANT" not in sql and "DROP" not in sql
    assert "schema_name" in sql and "relation.relname=child.value->>1" in sql
    assert migration.down_revision == "20261114160000_npi_named_generation"
    migration.downgrade()
    assert execute.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("attached", [False, True])
@pytest.mark.parametrize("edition", ["named", "saved_names"])
async def test_pin_release_resolves_declared_saved_child_names(monkeypatch, attached, edition):
    prepared = prepared_result()
    ownership = archive.StageOwnership(
        prepared.ownership.importer_id,
        prepared.ownership.dataset_id,
        tuple(sorted(archive.source_spec(prepared.ownership.importer_id, publication=True).table_names)),
    )
    publication_by_field = {
        "parents": [[name, archive._reference("serving", name)] for name in archive.TABLES],
        "children": [
            [name, child, archive._reference(ownership.schema_name, child)]
            for name, child in zip(archive.TABLES, archive.PUBLICATION_TABLES, strict=True)
        ],
    }
    if edition == "saved_names":
        publication_by_field["parents"] = [[name, -1] for name in archive.TABLES]
        publication_by_field["children"] = [
            [name, child, -1] for name, child in zip(archive.TABLES, archive.PUBLICATION_TABLES, strict=True)
        ]
    receipt_by_field = {
        "ownership": archive.ownership_dict(ownership),
        "handoff": {"schema_name": "serving"},
        "publication": publication_by_field,
    }
    monkeypatch.setattr(archive, "_source_lock", AsyncMock())
    monkeypatch.setattr(
        archive, "_pin_group", AsyncMock(return_value=[{"authority_json": {"validation": receipt_by_field}}])
    )
    monkeypatch.setattr(archive, "_validate_pin_group", Mock(return_value=prepared.manifest["run_ids"]))
    monkeypatch.setattr(archive.pins, "lock_run", AsyncMock())
    session = SimpleNamespace(scalar=AsyncMock(return_value=attached), execute=AsyncMock())
    pending = archive.release_source_pin(
        session, schema="serving", importer_id=ownership.importer_id, run_id=prepared.manifest["run_id"], pin_id=uuid4()
    )
    if attached:
        with pytest.raises(archive.SourceProfileArchiveError, match="attached source seal is retained"):
            await pending
        session.execute.assert_not_awaited()
    else:
        assert await pending == "released"
        assert str(session.execute.await_args.args[0]).startswith("DELETE")
    query, parameters = session.scalar.await_args.args
    assert "n.nspname=:schema" in str(query) and "oid[]" not in str(query)
    assert parameters == {"schema": ownership.schema_name, "names": list(archive.PUBLICATION_TABLES)}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("copy failed"), asyncio.CancelledError()])
async def test_named_preparation_propagates_failure_before_freeze(monkeypatch, failure):
    prepared = prepared_result()
    expected_by_field = {"current_run_id": None, "previous_run_id": None}
    monkeypatch.setattr(projection, "_publication_lock", AsyncMock())
    monkeypatch.setattr(projection, "named_publication_pointer", AsyncMock(return_value=expected_by_field))
    monkeypatch.setattr(projection, "require_native_publication_order", AsyncMock())
    monkeypatch.setattr(projection, "_copy_named_projection_candidate", AsyncMock(side_effect=failure))
    freeze = AsyncMock()
    monkeypatch.setattr(projection, "freeze_named_projection", freeze)
    with pytest.raises(type(failure)):
        await projection.prepare_named_cutover(
            object(),
            prepared=prepared,
            schema="serving",
            expected=expected_by_field,
            cutover_id=uuid4(),
            source_copy=object(),
            deadline=1.0,
        )
    freeze.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "unlogged", "partition", "rls", "force_rls", "extra"])
async def test_named_stage_still_requires_the_closed_native_model_layout(fault):
    prepared = prepared_result()
    relation_rows = [
        {
            "relname": name,
            "relkind": "r",
            "relpersistence": "p",
            "relispartition": False,
            "relrowsecurity": False,
            "relforcerowsecurity": False,
            "index_table": None,
        }
        for name in prepared.ownership.relation_names
    ]
    field_by_fault = {
        "unlogged": ("relpersistence", "u"),
        "partition": ("relispartition", True),
        "rls": ("relrowsecurity", True),
        "force_rls": ("relforcerowsecurity", True),
    }
    if fault in field_by_fault:
        field, replacement = field_by_fault[fault]
        relation_rows[0][field] = replacement
    if fault == "extra":
        relation_rows.append({"relname": "unrelated_view", "relkind": "v", "index_table": None})
    mapped = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: relation_rows))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(return_value=mapped))
    if fault:
        with pytest.raises(archive.SourceProfileArchiveError, match="unrelated relation"):
            await archive.capture_ownership(session, prepared.ownership.importer_id, prepared.ownership.dataset_id)
    else:
        assert (
            await archive.capture_ownership(session, prepared.ownership.importer_id, prepared.ownership.dataset_id)
            == prepared.ownership
        )
