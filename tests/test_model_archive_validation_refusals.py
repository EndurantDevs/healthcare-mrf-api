# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native archive candidates require exact complete custody before publication."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import entity_address_snapshot_preparation as preparation
from process import mrf_address_publication as address
from process import reference_family_archive as archive
from process import tiger_held_inputs as held
from tests.test_reference_family_archive import _ownership
from tests.test_reference_family_source_copy import (
    _nucc_completed_handoff,
    _nucc_owning_transaction,
    _nucc_stage_receipt,
)


def _session(*, rows=(), scalar=None):
    result = Mock()
    result.mappings.return_value = result
    result.scalars.return_value = result
    result.all.return_value = rows
    result.one.return_value = rows
    return SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=result),
        scalar=AsyncMock(return_value=scalar),
    )


def _inventory(names):
    return {
        "database_oid": 51,
        "relations": [
            {
                "schema_name": "candidate",
                "schema_oid": 52,
                "schema_owner_oid": 53,
                "owner_oid": 53,
                "relation_name": name,
                "relation_oid": 100 + index,
                "relfilenode": 200 + index,
            }
            for index, name in enumerate(names)
        ],
    }


def _selected(relation):
    return {
        "table_name": relation["relation_name"],
        **{key: relation[key] for key in ("schema_name", "relation_oid", "relfilenode")},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("row_count", True),
        ("row_count", 0),
        ("row_count", 10_001),
        ("distinct_codes", 0),
        ("invalid", 1),
        ("csv_upper_bound", True),
        ("csv_upper_bound", 0),
        ("csv_upper_bound", 8 * 1024**2 + 1),
    ],
)
async def test_nucc_aggregate_refuses_invalid_counts_keys_or_byte_bounds(field, value):
    census_by_field = {"row_count": 1, "distinct_codes": 1, "invalid": 0, "csv_upper_bound": 64}
    census_by_field[field] = value
    session = _session(rows=census_by_field)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="indexed set"):
        await archive._validate_nucc_table_set(session, "candidate", "nucc_taxonomy")
    query = str(session.execute.await_args.args[0])
    assert 'FROM "candidate"."nucc_taxonomy"' in query
    assert "count(DISTINCT code)" in query and "count(*) FILTER" in query
    assert "octet_length" in query and "SELECT code" not in query
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("count,size", [(1, 1), (10_000, 8 * 1024**2)])
async def test_nucc_aggregate_accepts_exact_inclusive_bounds(count, size):
    session = _session(rows={"row_count": count, "distinct_codes": count, "invalid": 0, "csv_upper_bound": size})
    assert await archive._validate_nucc_table_set(session, "candidate", "nucc_taxonomy") == {
        "contract": "nucc-indexed-set.v1",
        "row_count": count,
        "csv_upper_bound": size,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("deferred", (False, True))
@pytest.mark.parametrize("failure", (None, "missing", "catalog", "declaration", "columns", "indexes"))
async def test_nucc_completion_locks_names_and_validates_columns_before_indexes_and_data(
    monkeypatch, deferred, failure
):
    receipt = _nucc_stage_receipt()
    model = SimpleNamespace(__tablename__=receipt["stage"]["table_name"])
    if failure == "declaration":
        receipt["schema_name"] = "other"
    events = []
    session = _session()
    session.scalar.side_effect = [failure != "catalog", failure != "indexes"]
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=None if failure == "missing" else 42))
    monkeypatch.setattr(archive, "_require_nucc_precreated_stage", AsyncMock())

    async def columns(*_args):
        events.append("columns")
        if failure == "columns":
            raise archive.ReferenceFamilyArchiveError("actual model column differs")

    async def indexes(*_args, **_kwargs):
        events.append("indexes")

    async def validate(*_args):
        events.append("validate")
        return {"row_count": 3}

    monkeypatch.setattr(archive, "_require_nucc_native_columns", columns)
    indexer = AsyncMock(side_effect=indexes)
    monkeypatch.setattr(archive, "_create_model_indexes", indexer)
    monkeypatch.setattr(archive, "_validate_nucc_table_set", validate)
    failed = failure is not None and (failure != "declaration" or deferred)
    if failed:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive.prepare_nucc_native_publication_stage(
                session, stage_model=model, schema_name="mrf", native_stage=receipt if deferred else None
            )
    else:
        assert await archive.prepare_nucc_native_publication_stage(
            session, stage_model=model, schema_name="mrf", native_stage=receipt if deferred else None
        ) == {"relation": ("mrf", model.__tablename__), "row_count": 3}
    assert (
        str(session.execute.await_args_list[0].args[0])
        == f'LOCK TABLE ONLY "mrf"."{model.__tablename__}" IN SHARE MODE NOWAIT'
    )
    if failure in {"missing", "catalog", "columns"} or failure == "declaration" and deferred:
        indexer.assert_not_awaited()
        assert "validate" not in events
    else:
        assert events == ["columns", "indexes", "validate"]
        assert indexer.await_args.kwargs == ({"create_constraints": True} if deferred else {})


@pytest.mark.asyncio
@pytest.mark.parametrize("table", ["nucc_taxonomy", "other_candidate", "nucc_taxonomy_unsafe-name"])
async def test_nucc_completion_refuses_foreign_names_without_catalog_reads(table):
    session = _session()
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="candidate name"):
        await archive.prepare_nucc_native_publication_stage(
            session, stage_model=SimpleNamespace(__tablename__=table), schema_name="candidate"
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


def _selected_family_view(monkeypatch, kind, failure):
    """Build complete named selection and retain catalog, validation and failure ordering."""
    spec = archive.reference_family_spec("mrf", canonical=kind == "canonical")
    names = spec.archive_names if kind == "mrf" else spec.table_names
    inventory = _inventory(names)
    relation = next(
        relation_entry
        for relation_entry in inventory["relations"]
        if relation_entry["relation_name"] == archive.models.MRFAddress.__tablename__
    )
    selected = _selected(relation)
    session = _session(scalar="pg_temp_7")
    events = []
    original_execute = session.execute

    async def execute(statement, *args):
        events.append(str(statement))
        return await original_execute(statement, *args)

    session.execute = AsyncMock(side_effect=execute)

    async def authenticate(_session):
        assert _session is session
        events.append("authenticate")
        return inventory if kind == "mrf" else {"mrf": inventory}

    async def check_catalog(_session, relations, owner):
        assert owner == 53
        assert {relation_entry["relation_name"] for relation_entry in relations} == set(names)
        assert relations == sorted(
            relations, key=lambda relation_entry: (relation_entry["schema_name"], relation_entry["relation_name"])
        )
        assert "LOCK TABLE" in events[-1]
        events.append("catalog")
        if failure == "catalog":
            raise RuntimeError("catalog changed")

    catalog = AsyncMock(side_effect=check_catalog)
    monkeypatch.setattr(archive, "_require_selected_mrf_catalog", catalog)
    validate = AsyncMock()
    monkeypatch.setattr(archive, "_validate_mrf_canonical_address_merge", validate)
    monkeypatch.setattr(address, "validate_canonical_contributions", validate)
    monkeypatch.setattr(address, "require_canonical_publication_catalog", AsyncMock())
    base = AsyncMock(side_effect=RuntimeError("base changed") if failure == "base" else None)
    monkeypatch.setattr(address, "require_canonical_output_base", base)
    context = (
        archive.selected_mrf_canonical_archive(
            session, schema_name="mrf", owner_oid=53, selected_relation=selected, authenticate_inventory=authenticate
        )
        if kind == "mrf"
        else archive.selected_canonical_archive(
            session,
            schema_name="mrf",
            owner_oid=53,
            selected_relations={"mrf": selected},
            authenticate_inventory=authenticate,
            authenticate_base=AsyncMock(return_value={}) if failure == "base" else None,
        )
    )
    return events, validate, context


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["mrf", "canonical"])
@pytest.mark.parametrize("failure", [None, "catalog", "body", "base"])
async def test_selected_family_view_holds_complete_heaps_and_never_masks_failure(monkeypatch, kind, failure):
    events, validate, context = _selected_family_view(monkeypatch, kind, failure)
    has_failure = failure in {"catalog", "body"} or (failure == "base" and kind == "canonical")
    try:
        async with context as view:
            assert view[0] == "pg_temp_7"
            assert view[1].startswith("address_")
            assert events[-1].startswith("CREATE TEMP VIEW")
            assert "NOT EXISTS" in events[-1]
            if failure == "body":
                raise RuntimeError("body changed")
    except RuntimeError as error:
        assert has_failure and str(error) == failure + " changed"
    else:
        assert not has_failure
    assert events[0] == "authenticate"
    queries = [event for event in events if event.startswith(("LOCK", "CREATE", "DROP"))]
    assert "NOWAIT" in queries[0]
    if failure in {"catalog", "base"} and has_failure:
        assert not any(query.startswith("CREATE") for query in queries)
        validate.assert_not_awaited()
    elif has_failure:
        assert not any(query.startswith("DROP") for query in queries)
    else:
        assert queries[-1].startswith('DROP VIEW "pg_temp_7".') and queries[-1].endswith(" RESTRICT")
        validate.assert_awaited_once()
    assert all("DROP TABLE" not in query and "ALTER TABLE" not in query for query in queries)


@pytest.mark.asyncio
async def test_installed_mrf_requires_authenticating_callback_and_only_locks_archive(monkeypatch):
    session = _session()
    authenticate = AsyncMock(return_value=None)
    async with archive.selected_mrf_canonical_archive(
        session, schema_name="mrf", owner_oid=53, selected_relation={}, authenticate_inventory=authenticate
    ) as view:
        assert view is None
    authenticate.assert_awaited_once_with(session)
    assert session.execute.await_count == 1
    assert "IN ACCESS SHARE MODE NOWAIT" in str(session.execute.await_args.args[0])
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("kind", ["mrf", "canonical"])
@pytest.mark.parametrize(
    "change", ["missing", "extra", "duplicate_oid", "schema", "schema_oid", "owner", "schema_owner", "pin"]
)
def test_selected_family_keeps_names_complete_and_old_coordinates_opaque(kind, change):
    names = archive.reference_family_spec("mrf").archive_names
    inventory = _inventory(names)
    relation = next(
        entry for entry in inventory["relations"] if entry["relation_name"] == archive.models.MRFAddress.__tablename__
    )
    selected = _selected(relation)
    if change == "missing":
        inventory["relations"].pop()
    elif change == "extra":
        inventory["relations"].append({**relation, "relation_name": "foreign_heap"})
    elif change == "schema":
        inventory["relations"][0]["schema_name"] = "foreign"
    elif change == "pin":
        selected["relfilenode"] = "opaque copied edition"
    else:
        field = {"duplicate_oid": "relation_oid", "owner": "owner_oid", "schema_owner": "schema_owner_oid"}.get(
            change, change
        )
        inventory["relations"][0][field] = "opaque copied edition"
    operation = lambda: (
        archive._selected_mrf_relations(inventory, selected, None)
        if kind == "mrf"
        else archive._selected_canonical_relations(inventory, selected, None, names)
    )
    if change in {"missing", "extra", "schema"}:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="complete family|role identity"):
            operation()
    else:
        actual = operation()
        assert {entry["relation_name"] for entry in actual} == set(names)
        assert all(
            set(entry) == {"schema_name", "relation_name"} and entry["schema_name"] == "candidate" for entry in actual
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "missing", "write_acl", "oid", "name", "schema", "file", "owner"])
async def test_locked_catalog_checks_named_shape_and_native_read_permissions(change):
    relations = _inventory(("first", "second"))["relations"]
    if change in {"oid", "file", "owner"}:
        relations[0][{"oid": "relation_oid", "file": "relfilenode", "owner": "owner_oid"}[change]] = (
            "opaque copied edition"
        )
    if change == "name":
        relations[0]["relation_name"] = "missing"
    if change == "schema":
        relations[0]["schema_name"] = "foreign"
    session = _session(scalar=change not in {"missing", "name", "schema"})
    if change == "write_acl":
        session.scalar.side_effect = PermissionError("native SELECT denied")
    if change in {"missing", "name", "schema", "write_acl"}:
        with pytest.raises(
            (archive.ReferenceFamilyArchiveError, PermissionError), match="locked catalog|SELECT denied"
        ):
            await archive._require_selected_mrf_catalog(session, relations, None)
    else:
        await archive._require_selected_mrf_catalog(session, relations, None)
        assert session.scalar.await_count == 2
    query, parameters = session.scalar.await_args_list[0].args
    assert parameters == {"relation": f'"{relations[0]["schema_name"]}"."{relations[0]["relation_name"]}"'}
    for predicate in ("NOT indisvalid", "NOT indislive", "NOT c.relispartition", "pg_inherits", "NOT c.relrowsecurity"):
        assert predicate in str(query)
    assert "relowner" not in str(query) and "has_table_privilege" not in str(query)
    session.execute.assert_not_awaited()


def _captured_model():
    identifier = "550e8400-e29b-41d4-a716-446655440000"
    schema = "reference_family_archive_" + "a" * 32
    inventory_by_field = {"relations": [dict(schema_name=schema, relation_name=name) for name in ("first", "second")]}
    authority_by_field = dict(
        contract="reference-family-source.v1",
        manifest=dict(
            importer_id="geo", tables=[dict(table_name=name, schema_sha256=name) for name in ("first", "second")]
        ),
        ownership=dict(
            dataset_id=identifier,
            schema_name=schema,
            relations=[dict(schema_name=schema, relation_name=name) for name in ("first", "second")],
        ),
    )
    manifest_by_field = dict(
        producer_node_id="synthetic",
        importer_id="geo",
        dataset_key="geo",
        contract_version=authority_by_field["contract"],
        adapter_metadata=dict(family=authority_by_field["manifest"]),
    )
    return dict(
        generation_id=identifier,
        origin_kind="captured",
        node_id="synthetic",
        importer_id="geo",
        dataset_key="geo",
        validation_sha256="opaque old edition",
        registration_sha256="opaque old edition",
        inventory=inventory_by_field,
        source_operation_id=identifier,
        source_capture=dict(contract="snapshot-captured-source.v1", source_authority=authority_by_field),
        selected_manifest=manifest_by_field,
        selected_package_id=held._digest(manifest_by_field),
        current_location_fence=None,
        state="retained",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [None, "registration", "authority", "package", "database", "schema", "relation", "catalog", "fence", "state"],
)
async def test_captured_model_validates_package_names_schema_and_retained_state(monkeypatch, change):
    capture_by_field = _captured_model()
    if change in {"registration", "authority"}:
        capture_by_field[{"registration": "registration_sha256", "authority": "validation_sha256"}[change]] = (
            "different opaque edition"
        )
    if change == "package":
        capture_by_field["selected_package_id"] = "a" * 64
    if change == "database":
        capture_by_field["inventory"]["database_oid"] = "opaque copied edition"
    if change == "fence":
        capture_by_field["current_location_fence"] = {"opaque": "old"}
    if change == "state":
        capture_by_field["state"] = "current"
    if change == "schema":
        capture_by_field["source_capture"]["source_authority"]["ownership"]["schema_name"] = "foreign"
    if change == "relation":
        capture_by_field["inventory"]["relations"][0]["relation_name"] = "other"
    lock = AsyncMock(return_value={})
    identity = AsyncMock(side_effect=["changed" if change == "catalog" else "first", "second"])
    monkeypatch.setattr(held, "_protected_relation", lock)
    monkeypatch.setattr(held, "_captured_schema_identity", identity)
    session = _session()
    failures = {"package", "schema", "relation", "catalog", "fence", "state"}
    if change in failures:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held._require_model_capture(session, capture_by_field)
    else:
        expected_by_field = {"relations": capture_by_field["inventory"]["relations"]}
        assert await held._require_model_capture(session, capture_by_field) == expected_by_field
        assert lock.await_count == identity.await_count == 2
        assert lock.await_args_list[0].args == (session, '"reference_family_archive_' + "a" * 32 + '"."first"')
        assert lock.await_args_list[0].kwargs == {}
    if change in failures - {"catalog"}:
        lock.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["npi", "geo"])
async def test_model_capture_reuses_the_correct_existing_schema_attester(monkeypatch, importer):
    from process import npi_result_archive

    npi = AsyncMock(return_value={"npi": True})
    family = AsyncMock(return_value={"family": True})
    monkeypatch.setattr(npi_result_archive, "_npi_schema_identity", npi)
    monkeypatch.setattr(archive, "_family_schema_identity", family)
    relation = _inventory(("first",))["relations"][0]
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=100))
    session = object()
    assert await held._captured_schema_identity(session, importer, relation) == (
        {"npi": True} if importer == "npi" else {"family": True}
    )
    if importer == "npi":
        npi.assert_awaited_once_with(session, 100, "candidate", "first")
        family.assert_not_awaited()
    else:
        family.assert_awaited_once_with(session, "geo", 100, "candidate", "first")
        npi.assert_not_awaited()


def _nucc_cutover_fixture(monkeypatch, failure):
    from dataclasses import replace

    from process import provider_directory_generic_source_provision as provision

    handoff = _nucc_completed_handoff(_nucc_stage_receipt())
    incumbent = handoff["incumbent"]
    published = archive._nucc_result_authority(
        {
            **incumbent,
            "local_generation": 1,
            "serving_generation": {
                "origin_lineage_id": incumbent["local_lineage_id"],
                "origin_generation": 1,
                "published_at": "2026-10-01T00:01:00Z",
            },
            "relations": [{"schema_name": "mrf", "relation_name": "nucc_taxonomy"}],
        }
    )
    if failure == "generation":
        published = replace(published, relations=(("other", "nucc_taxonomy"),))
    session = _nucc_owning_transaction(_session())
    transaction = object()
    session.get_transaction = lambda: transaction
    session.nucc_events = []
    monkeypatch.setattr(archive, "require_nucc_native_handoff", AsyncMock(return_value=handoff))
    monkeypatch.setattr(
        archive, "model_family_roles", lambda: ("snapshot_owner", ("snapshot_owner", "publisher", "reader"))
    )
    monkeypatch.setattr(archive, "_native_model_copy_driver", AsyncMock(return_value="named-driver"))

    async def freeze(*_args, **_kwargs):
        session.nucc_events.append("freeze")

    session.nucc_freeze = AsyncMock(side_effect=freeze)
    monkeypatch.setattr(provision, "freeze_fhir_source_snapshot", session.nucc_freeze)

    async def validate(*_args):
        session.nucc_events.append("validate")
        return {"contract": "nucc-indexed-set.v1", "row_count": 2 if failure == "count" else 1, "csv_upper_bound": 64}

    monkeypatch.setattr(archive, "_validate_nucc_table_set", validate)
    monkeypatch.setattr(archive, "_require_nucc_sealed_handoff", AsyncMock())
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=99 if failure == "predecessor" else None))
    rename, publish, finish = AsyncMock(), AsyncMock(return_value=published), AsyncMock()
    monkeypatch.setattr(archive, "rename_nucc_published_indexes", rename)
    monkeypatch.setattr(archive, "_publish_nucc_handoff_generation", publish)
    monkeypatch.setattr(archive, "_finish_nucc_native_attempt", finish)
    return handoff, session, rename, publish, finish


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (None, "count", "unused", "reused", "transaction", "predecessor", "generation"))
async def test_nucc_cutover_runs_once_in_transaction_after_write_closure(monkeypatch, failure):
    handoff, session, rename, publish, finish = _nucc_cutover_fixture(monkeypatch, failure)

    async def continuation(actual, prepared, cutover):
        assert actual is session and prepared["handoff"] == handoff
        assert set(prepared) == {"handoff", "validation", "validation_sha256"}
        if failure == "unused":
            return None
        if failure == "transaction":
            session.get_transaction = lambda: object()
        receipt = await cutover()
        assert archive.validate_nucc_native_publication(receipt) == receipt
        if failure == "reused":
            await cutover()
        return receipt

    if failure:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive.complete_nucc_native_handoff(session, handoff, publication_continuation=continuation)
    else:
        receipt = await archive.complete_nucc_native_handoff(session, handoff, publication_continuation=continuation)
        assert receipt["result_generation"]["relations"] == [{"schema_name": "mrf", "relation_name": "nucc_taxonomy"}]
    assert session.nucc_events == ["freeze", "validate"]
    session.nucc_freeze.assert_awaited_once_with(
        "named-driver",
        "mrf",
        namespace_owner="snapshot_owner",
        reader_roles=("snapshot_owner", "publisher", "reader"),
        application_roles=(),
        relation_names=(handoff["stage"]["table_name"],),
        freeze_schema=False,
    )
    if failure in {"count", "unused", "transaction", "predecessor"}:
        session.execute.assert_not_awaited()
        publish.assert_not_awaited()
        finish.assert_not_awaited()
    else:
        session.execute.assert_awaited_once()
        assert (
            str(session.execute.await_args.args[0])
            == f'ALTER TABLE "mrf"."{handoff["stage"]["table_name"]}" RENAME TO nucc_taxonomy'
        )
        rename.assert_awaited_once_with(session, schema_name="mrf")
        assert finish.await_count == int(failure != "generation")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", (None, "run", "phase", "heap", "index-definition"))
async def test_completed_handoff_rechecks_persisted_run_and_named_native_layout(monkeypatch, change):
    handoff = _nucc_completed_handoff(_nucc_stage_receipt())
    run_by_field = {
        "metrics": {"nucc_handoff": handoff},
        "finished_at": None,
        "phase_detail": archive.NUCC_HANDOFF_PHASE,
    }
    if change == "run":
        run_by_field["finished_at"] = "closed"
    if change == "phase":
        run_by_field["phase_detail"] = "other"
    observed = deepcopy(handoff["stage"])
    if change == "heap":
        observed["table_name"] = "other"
    if change == "index-definition":
        observed["indexes"][0]["definition"] = "other definition"
    monkeypatch.setattr(archive, "_nucc_locked_attempt", AsyncMock(return_value=run_by_field))
    monkeypatch.setattr(archive, "_require_nucc_native_location", AsyncMock())
    leaf = AsyncMock(return_value=observed)
    monkeypatch.setattr(archive, "_nucc_native_stage", leaf)
    session = _session()
    if change:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive._require_nucc_sealed_handoff(session, handoff)
    else:
        await archive._require_nucc_sealed_handoff(session, handoff)
    assert leaf.await_count == int(change not in {"run", "phase"})
    session.scalar.assert_not_awaited()


def _retained_inventory():
    return {"relations": [{"schema_name": archive._PREDECESSOR_PREFIX + "1" * 32, "relation_name": "nucc_taxonomy"}]}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (None, "referenced", "late_reference", "transaction", "layout"))
async def test_retained_nucc_cleanup_keeps_named_scope_references_layout_and_restrictive_drop(monkeypatch, failure):
    inventory = _retained_inventory()
    schema = inventory["relations"][0]["schema_name"]
    session = _nucc_owning_transaction(_session())
    session.first = AsyncMock(
        return_value=dict(
            importer_id="nucc",
            local_lineage_id="11111111-1111-4111-8111-111111111111",
            local_generation=0,
            origin_lineage_id=None,
            origin_generation=None,
            published_at=None,
            serving_schema=None,
            serving_relation_names=None,
        )
    )
    token = object()
    current_entries = [token]
    session.get_transaction = lambda: current_entries[0]

    async def scalar(statement, *_args):
        return "synthetic" if str(statement) == "SHOW search_path" else failure != "layout"

    session.scalar.side_effect = scalar
    leaf = AsyncMock()
    monkeypatch.setattr(archive, "_nucc_native_stage", leaf)
    calls = []

    async def is_unreferenced(*_args):
        calls.append(1)
        if failure == "transaction" and len(calls) == 2:
            current_entries[0] = object()
        return not (failure == "referenced" or failure == "late_reference" and len(calls) == 2)

    if failure:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive.cleanup_nucc_retained_publication(
                session, inventory=inventory, assert_unreferenced=is_unreferenced
            )
    else:
        assert await archive.cleanup_nucc_retained_publication(
            session, inventory=inventory, assert_unreferenced=is_unreferenced
        ) == {"inventory_sha256": archive.nucc_native_digest(inventory), "removed_schema": schema}
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert [sql for sql in statements if sql.startswith("DROP")] == (
        [] if failure else [f'DROP TABLE "{schema}"."nucc_taxonomy" RESTRICT', f'DROP SCHEMA "{schema}" RESTRICT']
    )
    assert statements[-1] == "SELECT set_config('search_path',:path,true)"
    assert len(calls) == (1 if failure in {"referenced", "layout"} else 2)
    assert leaf.await_count == int(failure not in {"referenced", "layout"})


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "protected", "lookup", "claimed", "uncertain", "empty"])
async def test_legacy_nucc_rotation_cannot_erase_any_protected_ledger_claim(monkeypatch, failure):
    from process import nucc_retention_read_view as claims

    session = _session()
    monkeypatch.setattr(archive, "is_nucc_native_handoff_required", AsyncMock(return_value=failure == "protected"))
    session.scalar.side_effect = (
        [False, False] if failure == "empty" else [None if failure == "lookup" else True, False]
    )
    check = AsyncMock(
        side_effect=claims.NuccRetentionReadError("protected claim exists or is uncertain")
        if failure in {"claimed", "uncertain"}
        else None
    )
    monkeypatch.setattr(claims, "require_unretained_nucc_relations", check)
    if failure in {"protected", "lookup", "claimed", "uncertain"}:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive.require_nucc_unretained_rotation(session, schema_name="mrf")
    else:
        await archive.require_nucc_unretained_rotation(session, schema_name="mrf")
    if failure in {"protected", "empty", "lookup"}:
        session.execute.assert_not_awaited()
    if failure == "protected":
        session.scalar.assert_not_awaited()
    if failure not in {"protected", "empty", "lookup"}:
        assert (
            str(session.execute.await_args.args[0])
            == 'LOCK TABLE ONLY "mrf"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT'
        )
        assert all("DROP" not in str(call.args[0]) for call in session.execute.await_args_list)
    if failure in {"protected", "empty", "lookup"}:
        check.assert_not_awaited()
    else:
        check.assert_awaited_once_with(session, schema_name="mrf", relation_names=["nucc_taxonomy"])


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "publisher", "schema", "replaced", "nonempty", "closure"])
async def test_model_sealing_validates_named_inventory_before_configured_dcl(monkeypatch, failure):
    from process import provider_directory_generic_source_provision as provision

    ownership = _ownership(relation_oids=(("first", 100), ("second", 101)))
    session = _session(scalar=failure == "nonempty")
    roles = SimpleNamespace(
        owner_role="bad-role" if failure == "publisher" else "snapshot_owner",
        publisher_role="publisher",
        runtime_roles=("reader",),
    )
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(side_effect=[None if failure == "replaced" else 100, 101]))
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=52))
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=()))
    namespace_relations = [dict(oid=100, relkind="v" if failure == "schema" else "r"), dict(oid=101, relkind="r")]
    monkeypatch.setattr(archive, "_namespace_relations", AsyncMock(return_value=namespace_relations))
    driver = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(archive, "_native_model_copy_driver", AsyncMock(return_value=driver))
    freeze = AsyncMock(side_effect=RuntimeError("native DCL denied") if failure == "closure" else None)
    monkeypatch.setattr(provision, "freeze_fhir_source_snapshot", freeze)
    if failure:
        with pytest.raises((archive.ReferenceFamilyArchiveError, RuntimeError)):
            await archive.seal_model_family_storage(session, ownership, require_empty=True, roles=roles)
    else:
        await archive.seal_model_family_storage(session, ownership, require_empty=True, roles=roles)
    if failure in {"publisher", "schema", "replaced", "nonempty"}:
        freeze.assert_not_awaited()
        driver.execute.assert_not_awaited()
    else:
        driver.execute.assert_awaited_once_with(f'ALTER SCHEMA "{ownership.schema_name}" OWNER TO "snapshot_owner"')
        freeze.assert_awaited_once_with(
            driver,
            ownership.schema_name,
            namespace_owner="snapshot_owner",
            reader_roles=("snapshot_owner", "publisher", "reader"),
            application_roles=(),
            relation_names=None,
            freeze_schema=True,
        )
