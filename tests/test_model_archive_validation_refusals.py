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
from tests.test_reference_family_source_copy import _nucc_completed_handoff, _nucc_stage_receipt


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
@pytest.mark.parametrize("deferred", [False, True])
@pytest.mark.parametrize("failure", [None, "missing", "catalog", "custody", "indexes", "replaced"])
async def test_nucc_completion_fences_custody_before_indexes_and_rechecks_oid(monkeypatch, deferred, failure):
    receipt = _nucc_stage_receipt()
    oid = receipt["stage"]["relation_oid"]
    model = SimpleNamespace(__tablename__=receipt["stage"]["table_name"])
    events = []

    async def relation_oid(*_args):
        events.append("identity")
        if failure == "missing":
            return None
        return oid + int(failure == "replaced" and events.count("identity") == 2)

    async def custody(*_args):
        events.append("custody")

    async def columns(*_args):
        events.append("columns")

    async def indexes(*_args, **_kwargs):
        events.append("indexes")

    async def validate(*_args):
        events.append("validate")
        return {"row_count": 3}

    session = _session()
    session.scalar.side_effect = [failure != "catalog", failure != "indexes"]
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(side_effect=relation_oid))
    monkeypatch.setattr(archive, "_require_nucc_precreated_stage", AsyncMock(side_effect=custody))
    monkeypatch.setattr(archive, "_require_nucc_native_columns", AsyncMock(side_effect=columns))
    indexer = AsyncMock(side_effect=indexes)
    monkeypatch.setattr(archive, "_create_model_indexes", indexer)
    monkeypatch.setattr(archive, "_validate_nucc_table_set", AsyncMock(side_effect=validate))
    if failure == "custody":
        receipt["stage"]["relation_oid"] += 1
    expected_failure = failure is not None and (failure != "custody" or deferred)
    if expected_failure:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive.prepare_nucc_native_publication_stage(
                session, stage_model=model, schema_name="mrf", native_stage=receipt if deferred else None
            )
    else:
        assert await archive.prepare_nucc_native_publication_stage(
            session, stage_model=model, schema_name="mrf", native_stage=receipt if deferred else None
        ) == {"relation_oid": oid, "row_count": 3}
    if failure == "missing":
        session.execute.assert_not_awaited()
    else:
        assert "IN SHARE MODE NOWAIT" in str(session.execute.await_args_list[0].args[0])
    if failure in {"missing", "catalog"} or (failure == "custody" and deferred):
        indexer.assert_not_awaited()
        assert "validate" not in events
    else:
        assert events.index("columns") < events.index("indexes") < events.index("validate")
        assert indexer.await_args.kwargs == ({"create_constraints": True} if deferred else {})
    if session.scalar.await_args_list:
        catalog = str(session.scalar.await_args_list[0].args[0])
        assert "NOT EXISTS(SELECT 1 FROM pg_trigger" in catalog
        assert "contype!='n'" in catalog if deferred else "conkey=ARRAY" in catalog


@pytest.mark.asyncio
@pytest.mark.parametrize("table", ["nucc_taxonomy", "other_candidate", "nucc_taxonomy_unsafe-name"])
async def test_nucc_completion_refuses_foreign_names_without_catalog_reads(table):
    session = _session()
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="candidate identity"):
        await archive.prepare_nucc_native_publication_stage(
            session, stage_model=SimpleNamespace(__tablename__=table), schema_name="candidate"
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


def _selected_family_view(monkeypatch, kind, failure):
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
    monkeypatch.setattr(preparation, "_publisher_authority", AsyncMock(return_value=53))

    async def authenticate(_session):
        assert _session is session
        events.append("authenticate")
        return inventory if kind == "mrf" else {"mrf": inventory}

    async def check_catalog(_session, relations, owner):
        assert owner == 53
        assert {relation_entry["relation_name"] for relation_entry in relations} == set(names)
        assert relations == sorted(relations, key=lambda relation_entry: relation_entry["relation_oid"])
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
    monkeypatch.setattr(preparation, "_publisher_authority", AsyncMock(return_value=53))
    authenticate = AsyncMock(return_value=None)
    async with archive.selected_mrf_canonical_archive(
        session, schema_name="mrf", owner_oid=53, selected_relation={}, authenticate_inventory=authenticate
    ) as view:
        assert view is None
    authenticate.assert_awaited_once_with(session)
    assert session.execute.await_count == 1
    assert "IN SHARE MODE NOWAIT" in str(session.execute.await_args.args[0])
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("kind", ["mrf", "canonical"])
@pytest.mark.parametrize(
    "change", ["missing", "extra", "duplicate_oid", "schema", "schema_oid", "owner", "schema_owner", "pin"]
)
def test_selected_family_refuses_incomplete_foreign_or_replaced_custody(kind, change):
    names = archive.reference_family_spec("mrf").archive_names
    inventory = _inventory(names)
    relation = next(
        row for row in inventory["relations"] if row["relation_name"] == archive.models.MRFAddress.__tablename__
    )
    selected = _selected(relation)
    if change == "missing":
        inventory["relations"].pop()
    elif change == "extra":
        inventory["relations"].append({**relation, "relation_name": "foreign_heap", "relation_oid": 999})
    elif change == "duplicate_oid":
        inventory["relations"][0]["relation_oid"] = inventory["relations"][1]["relation_oid"]
    elif change == "pin":
        selected["relfilenode"] += 1
    else:
        key = {"schema": "schema_name", "owner": "owner_oid", "schema_owner": "schema_owner_oid"}.get(change, change)
        inventory["relations"][0][key] = "foreign" if change == "schema" else 999
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="custody|identity"):
        if kind == "mrf":
            archive._selected_mrf_relations(inventory, selected, 53)
        else:
            archive._selected_canonical_relations(inventory, selected, 53, names)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "missing", "write_acl", "oid", "name", "schema", "file", "owner"])
async def test_locked_catalog_rechecks_every_physical_field_and_closed_writes(change):
    relations = _inventory(("first", "second"))["relations"]
    observed_relations = [{**relation_entry, "closed": True} for relation_entry in relations]
    if change == "missing":
        observed_relations.pop()
    elif change == "write_acl":
        observed_relations[0]["closed"] = False
    elif change:
        key = {
            "oid": "relation_oid",
            "name": "relation_name",
            "schema": "schema_oid",
            "file": "relfilenode",
            "owner": "owner_oid",
        }[change]
        observed_relations[0][key] = "foreign" if change == "name" else 999
    session = _session(rows=observed_relations)
    if change:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="locked catalog"):
            await archive._require_selected_mrf_catalog(session, relations, 53)
    else:
        await archive._require_selected_mrf_catalog(session, relations, 53)
    query, parameters = session.execute.await_args.args
    assert parameters == {"oids": [100, 101], "owner_oid": 53}
    for predicate in (
        "NOT indisvalid",
        "NOT indislive",
        "NOT c.relispartition",
        "'INSERT','UPDATE','DELETE'",
        "pg_inherits",
    ):
        assert predicate in str(query)


def _captured_model():
    inventory = _inventory(("first", "second"))
    identifier = "550e8400-e29b-41d4-a716-446655440000"
    authority_by_field = {
        "contract": "reference-family-source.v1",
        "manifest": {"importer_id": "geo"},
        "ownership": {
            "dataset_id": identifier,
            "schema_name": "candidate",
            "schema_oid": 52,
            "relation_oids": [
                [relation_entry["relation_name"], relation_entry["relation_oid"]]
                for relation_entry in inventory["relations"]
            ],
        },
        "custody": {"catalog_sha256": held._digest([{"name": "first"}, {"name": "second"}])},
    }
    manifest_by_field = {
        "producer_node_id": "synthetic",
        "importer_id": "geo",
        "dataset_key": "geo",
        "contract_version": authority_by_field["contract"],
        "adapter_metadata": {"family": authority_by_field["manifest"]},
    }
    registration_by_field = {
        "generation_id": identifier,
        "origin_kind": "captured",
        "node_id": "synthetic",
        "importer_id": "geo",
        "dataset_key": "geo",
        "validation_sha256": held._digest(authority_by_field),
        "inventory": inventory,
        "source_operation_id": identifier,
        "source_capture": {"contract": "snapshot-captured-source.v1", "source_authority": authority_by_field},
    }
    return {
        **registration_by_field,
        "registration_sha256": held._digest(registration_by_field),
        "selected_manifest": manifest_by_field,
        "selected_package_id": held._digest(manifest_by_field),
        "current_location_fence": None,
        "state": "retained",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [None, "registration", "authority", "package", "database", "schema", "relation", "catalog", "fence", "state"],
)
async def test_captured_model_requires_registration_package_and_locked_physical_custody(monkeypatch, change):
    capture_by_field = _captured_model()
    if change in {"registration", "authority", "package"}:
        capture_by_field[
            {"registration": "registration_sha256", "authority": "validation_sha256", "package": "selected_package_id"}[
                change
            ]
        ] = "a" * 64
    if change == "fence":
        capture_by_field["current_location_fence"] = {"relation_oid": 100}
    if change == "state":
        capture_by_field["state"] = "current"
    observed = deepcopy(capture_by_field["inventory"]["relations"])
    if change == "relation":
        observed[0]["relfilenode"] += 1
    if change == "schema":
        capture_by_field["source_capture"]["source_authority"]["ownership"]["schema_oid"] += 1
        capture_by_field["validation_sha256"] = held._digest(capture_by_field["source_capture"]["source_authority"])
        capture_by_field["registration_sha256"] = held._digest(
            {
                key: capture_by_field[key]
                for key in (
                    "generation_id",
                    "origin_kind",
                    "node_id",
                    "importer_id",
                    "dataset_key",
                    "validation_sha256",
                    "inventory",
                    "source_operation_id",
                    "source_capture",
                )
            }
        )
    lock = AsyncMock(side_effect=observed)
    identity = AsyncMock(side_effect=[{"name": "changed" if change == "catalog" else "first"}, {"name": "second"}])
    monkeypatch.setattr(held, "_protected_relation", lock)
    monkeypatch.setattr(held, "_captured_schema_identity", identity)
    session = _session(scalar=999 if change == "database" else 51)
    if change:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held._require_model_capture(session, capture_by_field, 53)
    else:
        assert await held._require_model_capture(session, capture_by_field, 53) == capture_by_field["inventory"]
        assert lock.await_count == identity.await_count == 2
        assert lock.await_args_list[0].args == (session, '"candidate"."first"')
        assert lock.await_args_list[0].kwargs == {"expected_owner": 53}
    if change in {"registration", "authority", "package", "database", "schema", "fence", "state"}:
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
            "relation_oids": [handoff["stage"]["relation_oid"] + int(failure == "generation")],
        }
    )
    session = _session(scalar="transaction-1")
    monkeypatch.setattr(archive, "require_nucc_native_handoff", AsyncMock(return_value=handoff))
    monkeypatch.setattr(archive, "_nucc_native_publisher_owner", AsyncMock(return_value=53))
    monkeypatch.setattr(archive, "_require_nucc_native_columns", AsyncMock())
    monkeypatch.setattr(
        archive,
        "_validate_nucc_table_set",
        AsyncMock(
            return_value={
                "contract": "nucc-indexed-set.v1",
                "row_count": 2 if failure == "count" else 1,
                "csv_upper_bound": 64,
            }
        ),
    )
    sealed = AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_sealed_handoff", sealed)
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=99 if failure == "predecessor" else None))
    rename = AsyncMock()
    monkeypatch.setattr(archive, "rename_nucc_published_indexes", rename)
    publish = AsyncMock(return_value=published)
    finish = AsyncMock()
    monkeypatch.setattr(archive, "_publish_nucc_handoff_generation", publish)
    monkeypatch.setattr(archive, "_finish_nucc_native_attempt", finish)
    return handoff, session, rename, publish, finish


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "count", "unused", "reused", "transaction", "predecessor", "generation"])
async def test_nucc_cutover_is_once_only_in_the_owning_transaction(monkeypatch, failure):
    handoff, session, rename, publish, finish = _nucc_cutover_fixture(monkeypatch, failure)

    async def continuation(actual_session, prepared, cutover):
        assert actual_session is session
        assert prepared["handoff"] == handoff and prepared["sealed_owner_oid"] == 53
        if failure == "unused":
            return None
        if failure == "transaction":
            session.scalar.return_value = "transaction-2"
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
        assert receipt["result_generation"]["relation_oids"] == [handoff["stage"]["relation_oid"]]
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
        rename.assert_awaited_once_with(
            session, schema_name="mrf", expected_relation_oid=handoff["stage"]["relation_oid"]
        )
        assert finish.await_count == int(failure != "generation")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "run", "publisher", "heap", "marker"])
async def test_sealed_handoff_rechecks_owner_transition_and_original_marker(monkeypatch, change):
    handoff = _nucc_completed_handoff(_nucc_stage_receipt())
    run_by_field = {
        "metrics": {"nucc_handoff": handoff},
        "finished_at": None,
        "phase_detail": archive.NUCC_HANDOFF_PHASE,
    }
    if change == "run":
        run_by_field["finished_at"] = "closed"
    monkeypatch.setattr(archive, "_nucc_locked_attempt", AsyncMock(return_value=run_by_field))
    monkeypatch.setattr(archive, "_require_nucc_native_location", AsyncMock())
    monkeypatch.setattr(
        archive, "_nucc_native_publisher_owner", AsyncMock(return_value=999 if change == "publisher" else 53)
    )
    heap_by_field = {
        **handoff["stage"],
        "owner_oid": 53,
        "relfilenode": 999 if change == "heap" else handoff["stage"]["relfilenode"],
    }
    observed = AsyncMock(return_value=heap_by_field)
    monkeypatch.setattr(archive, "_nucc_native_stage", observed)
    marker_by_field = {key: handoff[key] for key in ("contract", "run_id", "attempt_id", "handoff_sha256")}
    session = _session(scalar=None if change == "marker" else json.dumps(marker_by_field))
    if change:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive._require_nucc_sealed_handoff(session, handoff, 53)
    else:
        await archive._require_nucc_sealed_handoff(session, handoff, 53)
        assert session.scalar.await_args.args[1] == {"oid": handoff["stage"]["relation_oid"]}
    if change in {"run", "publisher"}:
        observed.assert_not_awaited()
    session.execute.assert_not_awaited()


def _retained_inventory():
    inventory = _inventory(("nucc_taxonomy",))
    inventory["relations"][0]["schema_name"] = archive._PREDECESSOR_PREFIX + "1" * 32
    return inventory


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "referenced", "late_reference", "transaction", "physical", "foreign_owner"])
async def test_retained_nucc_cleanup_rechecks_references_and_custody_before_restrict_drop(monkeypatch, failure):
    inventory = _retained_inventory()
    schema = inventory["relations"][0]["schema_name"]
    if failure == "foreign_owner":
        inventory["relations"][0]["owner_oid"] = 999
    session = _session()
    transactions = iter(["transaction-1", "transaction-2" if failure == "transaction" else "transaction-1"])

    async def scalar(statement, *_args):
        query = str(statement)
        if query == "SHOW search_path":
            return "synthetic"
        if "pg_current_xact_id" in query:
            return next(transactions)
        return failure != "physical"

    session.scalar.side_effect = scalar
    monkeypatch.setattr(archive, "_nucc_native_publisher_owner", AsyncMock(return_value=53))
    columns = AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_native_columns", columns)
    unreferenced = AsyncMock(side_effect=[failure != "referenced", failure != "late_reference"])
    if failure:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive.cleanup_nucc_retained_publication(
                session, inventory=inventory, assert_unreferenced=unreferenced
            )
    else:
        assert await archive.cleanup_nucc_retained_publication(
            session, inventory=inventory, assert_unreferenced=unreferenced
        ) == {
            "inventory_sha256": archive.nucc_native_digest(inventory),
            "removed_schema_oid": 52,
        }
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    drops = [query for query in queries if query.startswith("DROP")]
    assert drops == (
        [] if failure else [f'DROP TABLE "{schema}"."nucc_taxonomy" RESTRICT', f'DROP SCHEMA "{schema}" RESTRICT']
    )
    assert queries[0] == "SET LOCAL search_path=pg_catalog,pg_temp"
    assert queries[-1] == "SELECT set_config('search_path',:path,true)"
    assert session.execute.await_args.args[1] == {"path": "synthetic"}
    assert unreferenced.await_count == (1 if failure in {"referenced", "physical", "foreign_owner"} else 2)
    if failure in {"referenced", "physical", "foreign_owner"}:
        columns.assert_not_awaited()
    else:
        columns.assert_awaited_once_with(session, 100)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "protected", "replaced", "claimed", "uncertain", "empty"])
async def test_legacy_nucc_rotation_cannot_erase_any_protected_ledger_claim(monkeypatch, failure):
    session = _session()
    monkeypatch.setattr(archive, "is_nucc_native_handoff_required", AsyncMock(return_value=failure == "protected"))
    oids = [None, None] if failure == "empty" else [100, 999 if failure == "replaced" else 100, None]
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(side_effect=oids))
    session.scalar.side_effect = [
        200,
        True if failure == "claimed" else None if failure == "uncertain" else False,
        None,
        None,
        None,
    ]
    if failure in {"protected", "replaced", "claimed", "uncertain"}:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive.require_nucc_unretained_rotation(session, schema_name="mrf")
    else:
        await archive.require_nucc_unretained_rotation(session, schema_name="mrf")
    if failure in {"protected", "empty"}:
        session.execute.assert_not_awaited()
        session.scalar.assert_not_awaited()
    else:
        assert (
            str(session.execute.await_args.args[0])
            == 'LOCK TABLE ONLY "mrf"."nucc_taxonomy" IN ACCESS EXCLUSIVE MODE NOWAIT'
        )
        assert all("DROP" not in str(call.args[0]) for call in session.execute.await_args_list)
    if failure not in {"protected", "empty", "replaced"}:
        assert session.scalar.await_args_list[1].args[1] == {"oids": [100]}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "publisher", "schema", "replaced", "nonempty", "closure"])
async def test_model_sealing_checks_each_heap_and_revokes_schema_create_before_attesting(monkeypatch, failure):
    from process.ptg_parts import ptg2_physical_binding as binding

    ownership = _ownership(relation_oids=(("first", 100), ("second", 101)))
    session = _session(rows=["PUBLIC", '"synthetic_reader"'])
    session.scalar.side_effect = [999 if failure == "schema" else 53, failure == "nonempty", False]
    monkeypatch.setattr(
        archive, "protected_publisher_owner", AsyncMock(return_value=999 if failure == "publisher" else 53)
    )
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(side_effect=[999 if failure == "replaced" else 100, 101]))
    seal = AsyncMock()
    closed = AsyncMock(side_effect=RuntimeError("closure changed") if failure == "closure" else None)
    monkeypatch.setattr(preparation, "_seal_published_relation", seal)
    monkeypatch.setattr(binding, "_require_closed_local_custody", closed)
    if failure:
        with pytest.raises(RuntimeError):
            await archive.seal_model_family_storage(session, ownership, 53, require_empty=True)
    else:
        await archive.seal_model_family_storage(session, ownership, 53, require_empty=True)
    if failure in {"publisher", "schema", "replaced", "nonempty"}:
        seal.assert_not_awaited()
        closed.assert_not_awaited()
        session.execute.assert_not_awaited()
    else:
        assert [call.args for call in seal.await_args_list] == [(session, 100, 53), (session, 101, 53)]
        statements = [str(call.args[0]) for call in session.execute.await_args_list]
        assert statements[1:] == [
            f'REVOKE CREATE ON SCHEMA "{ownership.schema_name}" FROM PUBLIC',
            f'REVOKE CREATE ON SCHEMA "{ownership.schema_name}" FROM "synthetic_reader"',
        ]
        closed.assert_awaited_once_with(session, ownership, 53)
        assert "a.grantee<>:owner" in statements[0] and "a.privilege_type='CREATE'" in statements[0]
