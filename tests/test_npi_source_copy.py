# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host SOURCE ordering and failure regressions; native custody requires PostgreSQL."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import npi_result_archive as archive

_COLUMN_CATALOG = {
    "attnum": 1,
    "attname": "npi",
    "type": "bigint",
    "attnotnull": True,
    "attgenerated": "",
    "attidentity": "",
    "collation_schema": None,
    "collation_name": None,
    "default_expression": None,
}
_CONSTRAINT_CATALOG = {
    "contype": "p",
    "condeferrable": False,
    "condeferred": False,
    "convalidated": True,
    "key_columns": "{1}",
    "referenced_columns": None,
    "referenced_table": None,
    "referenced_in_archive_schema": None,
    "check_expression": None,
}
_INDEX_CATALOG = {
    "indisunique": True,
    "indisprimary": True,
    "indimmediate": True,
    "indisvalid": True,
    "indnkeyatts": 1,
    "indnatts": 1,
    "method": "btree",
    "predicate": None,
    "expressions": None,
    "keys": "1",
    "options": "0",
    "key_attributes": [
        {
            "position": 0,
            "attribute_number": 1,
            "collation_schema": None,
            "collation_name": None,
            "opclass_schema": "pg_catalog",
            "opclass_name": "int8_ops",
        }
    ],
}
_GEOGRAPHY_CATALOG = {
    **deepcopy(_INDEX_CATALOG),
    "indisunique": False,
    "indisprimary": False,
    "method": "gist",
    "keys": "0",
    "expressions": "public.geography(public.st_makepoint(long, lat))",
    "predicate": "lat IS NOT NULL AND long IS NOT NULL",
    "key_attributes": [
        {
            "position": 0,
            "attribute_number": 0,
            "collation_schema": None,
            "collation_name": None,
            "opclass_schema": "public",
            "opclass_name": "gist_geography_ops",
        }
    ],
}


def _catalog_case(monkeypatch, *, postgis=False):
    """Exercise the real canonical catalog digest, including sequence normalization."""
    table_name = "npi_address" if postgis else "npi"
    catalogs_by_oid = {
        10: {
            "columns": [deepcopy(_COLUMN_CATALOG)],
            "constraints": [deepcopy(_CONSTRAINT_CATALOG)],
            "indexes": [deepcopy(_INDEX_CATALOG)],
        }
    }
    if postgis:
        catalogs_by_oid[10]["indexes"].append(deepcopy(_GEOGRAPHY_CATALOG))
    catalogs_by_oid[20] = deepcopy(catalogs_by_oid[10])
    if not postgis:
        catalogs_by_oid[10]["columns"][0]["default_expression"] = "nextval('source.ordinary_owned_seq'::regclass)"
        catalogs_by_oid[20]["columns"][0]["default_expression"] = "nextval('stage.npi_npi_seq'::regclass)"

    async def column_catalog(_session, oid):
        return deepcopy(catalogs_by_oid[oid]["columns"])

    async def constraints_catalog(_session, oid, _schema_name):
        return deepcopy(catalogs_by_oid[oid]["constraints"])

    async def indexes_catalog(_session, oid):
        return deepcopy(catalogs_by_oid[oid]["indexes"])

    async def relation_oid(_session, schema_name, _table_name=None):
        return {"source": 10, "stage": 20}[schema_name]

    monkeypatch.setattr(archive, "_relation_oid", relation_oid)
    monkeypatch.setattr(archive, "_schema_oid", relation_oid)
    monkeypatch.setattr(
        archive, "_owned_sequences", AsyncMock(return_value=() if postgis else (("owned", 40, "npi", "npi"),))
    )
    monkeypatch.setattr(archive.catalog_identity, "_catalog_columns", column_catalog)
    monkeypatch.setattr(archive.catalog_identity, "_catalog_constraints", constraints_catalog)
    monkeypatch.setattr(archive.catalog_identity, "_catalog_indexes", indexes_catalog)
    return catalogs_by_oid, table_name


@pytest.mark.asyncio
@pytest.mark.parametrize("postgis", [False, True])
async def test_source_full_schema_accepts_matching_supported_layout(monkeypatch, postgis):
    _catalogs_by_oid, table_name = _catalog_case(monkeypatch, postgis=postgis)
    await archive._require_source_column_shape(object(), "source", "stage", table_name)


_CHECK_CATALOG = {**deepcopy(_CONSTRAINT_CATALOG), "contype": "c", "check_expression": "npi > 0"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_checks", "stage_checks"),
    [
        ([_CHECK_CATALOG], []),
        ([], [_CHECK_CATALOG]),
        ([{**_CHECK_CATALOG, "check_expression": "npi > 1"}], [_CHECK_CATALOG]),
        ([{**_CHECK_CATALOG, "convalidated": False}], [_CHECK_CATALOG]),
    ],
)
async def test_source_full_schema_rejects_unsupported_check_loss(monkeypatch, source_checks, stage_checks):
    catalogs_by_oid, table_name = _catalog_case(monkeypatch)
    catalogs_by_oid[10]["constraints"].extend(deepcopy(source_checks))
    catalogs_by_oid[20]["constraints"].extend(deepcopy(stage_checks))
    with pytest.raises(archive.NpiResultArchiveError, match="source schema catalog is unsupported"):
        await archive._require_source_column_shape(object(), "source", "stage", table_name)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_path", "changed_field_value"),
    [
        (("constraints",), []),
        (("constraints", 0, "condeferrable"), True),
        (("indexes",), [_INDEX_CATALOG, _INDEX_CATALOG]),
        (("indexes",), []),
        (("indexes", 0, "indisvalid"), False),
        (("indexes", 0, "predicate"), "npi > 0"),
        (("indexes", 0, "expressions"), "npi + 1"),
        (("indexes", 0, "indnatts"), 2),
        (("indexes", 0, "method"), "hash"),
        (("indexes", 0, "options"), "1"),
        (("indexes", 0, "key_attributes", 0, "opclass_name"), "int4_ops"),
        (("indexes", 0, "key_attributes", 0, "collation_name"), "C"),
    ],
)
async def test_source_full_schema_rejects_unsupported_key_and_index_loss(monkeypatch, field_path, changed_field_value):
    catalogs_by_oid, table_name = _catalog_case(monkeypatch)
    catalog_by_field = catalogs_by_oid[10]
    for field_name in field_path[:-1]:
        catalog_by_field = catalog_by_field[field_name]
    catalog_by_field[field_path[-1]] = deepcopy(changed_field_value)
    with pytest.raises(archive.NpiResultArchiveError, match="source schema catalog is unsupported"):
        await archive._require_source_column_shape(object(), "source", "stage", table_name)


def _case(monkeypatch, *, postgis=False):
    events = []
    dataset_id = uuid4()
    ownership = archive.NpiStageOwnership(
        dataset_id,
        archive.npi_stage_schema(dataset_id),
        90,
        tuple((name, index + 101) for index, name in enumerate(sorted(archive.npi_archive_names(canonical=True)))),
        (("npi_npi_seq", 201, "npi", "npi"),),
    )

    async def execute(statement, _parameters=None):
        events.append(str(statement.compile(dialect=archive.postgresql.dialect())).lstrip())

    session = SimpleNamespace(execute=execute, scalar=AsyncMock(return_value=False), in_transaction=lambda: True)
    monkeypatch.setattr(archive, "capture_npi_stage_ownership", AsyncMock(return_value=ownership))
    monkeypatch.setattr(archive, "verify_npi_stage_ownership", AsyncMock())
    monkeypatch.setattr(archive, "_source_family_sequences", AsyncMock(return_value=ownership.sequence_oids))
    monkeypatch.setattr(archive, "_verify_stage_sequence_owners", AsyncMock())
    monkeypatch.setattr(archive, "_advance_and_verify_stage_sequences", AsyncMock())
    monkeypatch.setattr(archive, "_has_postgis", AsyncMock(return_value=postgis))
    monkeypatch.setattr(archive, "_require_source_column_shape", AsyncMock())
    monkeypatch.setattr(archive, "canonical_schema_identity", AsyncMock(return_value="a" * 64))
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=101))
    monkeypatch.setattr(archive.native_archive, "_is_model_table_equal", AsyncMock(return_value=True))
    capture = archive.NpiSourceCapture({}, "a" * 64, "legacy-manual", None, None, "synthetic_source", "1-1-1", True)
    return session, capture, ownership, events


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["clone", "prepare", "convenience"])
async def test_creation_requires_native_copy_and_custody_before_database_access(entrypoint):
    with pytest.raises(archive.NpiResultArchiveError, match="protected source COPY capability"):
        if entrypoint == "clone":
            await archive._clone_source(object(), object(), "stage")
        elif entrypoint == "prepare":
            await archive.prepare_npi_archive_source(
                object(), schema_name="mrf", source_metadata={}, dataset_id=uuid4(), on_prepared=AsyncMock()
            )
        else:
            await archive.export_npi_archive(
                object(), schema_name="mrf", source_metadata={}, dataset_id=uuid4(), archive_copy=AsyncMock()
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["source_copy", "on_precreated", "deadline"])
async def test_direct_copy_capability_is_complete_before_sql(monkeypatch, missing):
    session, capture, ownership, events = _case(monkeypatch)
    options_by_field = {
        "source_copy": archive.native_archive.ReferenceFamilySourceCopy(AsyncMock(), 4096, 30),
        "on_precreated": AsyncMock(),
        "deadline": asyncio.get_running_loop().time() + 30,
    }
    options_by_field[missing] = None
    with pytest.raises(archive.NpiResultArchiveError, match="COPY (capability|deadline)"):
        await archive._clone_source(session, capture, ownership.schema_name, **options_by_field)
    assert not events


@pytest.mark.asyncio
@pytest.mark.parametrize("postgis", [False, True])
async def test_all_heaps_are_sealed_before_copy_then_indexes_and_sets(monkeypatch, postgis):
    session, capture, ownership, events = _case(monkeypatch, postgis=postgis)
    copy_options = []

    async def seal(observed_session, observed):
        assert observed_session is session and observed is ownership
        assert sum(event.startswith("CREATE TABLE") for event in events) == 7
        assert not any(" ADD " in event or "CREATE INDEX" in event for event in events)
        events.append("seal")

    async def copy_rows(observed_session, query, **options):
        assert observed_session is session and query.startswith("SELECT ")
        copy_options.append(options)
        events.append("copy:" + options["table_name"])
        return 1 if len(copy_options) == 1 else 2 if len(copy_options) == 2 else 0

    await archive._clone_source(
        session,
        capture,
        ownership.schema_name,
        source_copy=archive.native_archive.ReferenceFamilySourceCopy(copy_rows, 3, 30),
        on_precreated=seal,
        deadline=asyncio.get_running_loop().time() + 30,
    )
    assert [options["max_bytes"] for options in copy_options] == [3, 2, 0, 0, 0, 0, 0]
    assert [options["table_name"] for options in copy_options] == list(archive.npi_archive_names(canonical=True))
    assert [options["columns"] for options in copy_options] == [
        tuple(column.name for column in model.__table__.columns) for model in archive.npi_archive_models(canonical=True)
    ]
    assert all(0 < options["timeout"] <= 30 for options in copy_options)
    assert [options["timeout"] for options in copy_options] == sorted(
        (options["timeout"] for options in copy_options), reverse=True
    )
    index_positions = [
        index
        for index, event in enumerate(events)
        if event.startswith(("CREATE INDEX", "CREATE UNIQUE INDEX")) or " ADD " in event
    ]
    assert (
        events.index("seal") < events.index("copy:npi") < events.index("copy:npi_phone_staffing") < min(index_positions)
    )
    assert not any("INSERT INTO" in event or "INCLUDING ALL" in event or "FOREIGN KEY" in event for event in events)
    assert any("ST_MakePoint" in event for event in events) is postgis
    archive._advance_and_verify_stage_sequences.assert_awaited_once_with(session, ownership)
    assert archive.native_archive._is_model_table_equal.await_count == 7
    assert archive._require_source_column_shape.await_count == 6
    assert (
        sum(
            "WHERE NOT EXISTS" in str(call.args[0])
            for call in session.scalar.await_args_list
            if archive.CANONICAL_TABLE not in str(call.args[0])
        )
        == 4
    )
    assert any("WITH RECURSIVE" in str(call.args[0]) for call in session.scalar.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("copied", [True, -1, 4, None])
async def test_invalid_copy_accounting_never_builds_indexes(monkeypatch, copied):
    session, capture, ownership, events = _case(monkeypatch)
    with pytest.raises(archive.native_archive.ReferenceFamilyArchiveError, match="COPY accounting"):
        await archive._clone_source(
            session,
            capture,
            ownership.schema_name,
            source_copy=archive.native_archive.ReferenceFamilySourceCopy(AsyncMock(return_value=copied), 3, 30),
            on_precreated=AsyncMock(),
            deadline=asyncio.get_running_loop().time() + 30,
        )
    assert not any(event.startswith(("CREATE INDEX", "CREATE UNIQUE INDEX")) or " ADD " in event for event in events)
    archive._advance_and_verify_stage_sequences.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["capture", "index", "persist"])
async def test_one_source_deadline_covers_capture_validation_and_authority(monkeypatch, phase):
    session, capture, ownership, events = _case(monkeypatch)
    session.begin = lambda: _transaction(events)
    capture_source = AsyncMock(return_value=capture)
    persist = AsyncMock()
    monkeypatch.setattr(archive, "capture_npi_source", capture_source)

    async def never_finish(*_args, **_options):
        await asyncio.Event().wait()

    if phase == "capture":
        capture_source.side_effect = never_finish
    elif phase == "index":
        monkeypatch.setattr(archive, "complete_npi_restore", never_finish)
    else:
        persist.side_effect = never_finish
        tables = tuple(
            archive.NpiTableReceipt(model.__name__, model.__tablename__, "a" * 64, 0) for model in archive._MODEL_TYPES
        )
        monkeypatch.setattr(archive, "_manifest_tables", AsyncMock(return_value=tables))

    @asynccontextmanager
    async def sessions():
        yield session

    with pytest.raises(TimeoutError):
        await archive.prepare_npi_archive_source(
            sessions,
            schema_name="synthetic_source",
            source_metadata={},
            dataset_id=ownership.dataset_id,
            on_prepared=persist,
            source_copy=archive.native_archive.ReferenceFamilySourceCopy(AsyncMock(return_value=0), 4096, 0.01),
            on_precreated=AsyncMock(),
        )
    assert "commit" not in events and "rollback" in events
    if phase != "persist":
        persist.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["seal", "copy", "index", "sequence", "columns", "equality"])
async def test_failed_clone_never_mints_prepared_authority(monkeypatch, failure):
    session, capture, ownership, events = _case(monkeypatch)
    session.begin = lambda: _transaction(events)
    monkeypatch.setattr(archive, "capture_npi_source", AsyncMock(return_value=capture))
    if failure == "index":
        monkeypatch.setattr(archive, "complete_npi_restore", AsyncMock(side_effect=RuntimeError("rejected")))
    if failure == "sequence":
        archive._advance_and_verify_stage_sequences.side_effect = RuntimeError("rejected")
    if failure == "equality":
        archive.native_archive._is_model_table_equal.return_value = False
    if failure == "columns":
        archive._require_source_column_shape.side_effect = RuntimeError("rejected")
    seal = AsyncMock(side_effect=RuntimeError("rejected") if failure == "seal" else None)
    copy = AsyncMock(return_value=0, side_effect=RuntimeError("rejected") if failure == "copy" else None)
    persist = AsyncMock()

    @asynccontextmanager
    async def sessions():
        yield session

    with pytest.raises(RuntimeError):
        await archive.prepare_npi_archive_source(
            sessions,
            schema_name="synthetic_source",
            source_metadata={},
            dataset_id=ownership.dataset_id,
            on_prepared=persist,
            source_copy=archive.native_archive.ReferenceFamilySourceCopy(copy, 4096, 30),
            on_precreated=seal,
        )
    persist.assert_not_awaited()
    assert events.count("rollback") == 2 and "commit" not in events


@asynccontextmanager
async def _transaction(events):
    try:
        yield
    except BaseException:
        events.append("rollback")
        raise
    else:
        events.append("commit")


@pytest.mark.asyncio
async def test_protected_export_requires_actual_custody_before_dump(monkeypatch):
    from tests.test_npi_result_archive_failure_contracts import _manifest, _ownership

    ownership = replace(_ownership(), freeze_function_oid=None, freeze_trigger_oids=(), freeze_catalog_versions=())
    events = []
    session = SimpleNamespace(execute=AsyncMock(), begin=lambda: _transaction(events))

    @asynccontextmanager
    async def sessions():
        yield session

    @asynccontextmanager
    async def catalog(_session):
        yield

    async def custody(observed_session, prepared):
        assert observed_session is session and prepared.ownership is ownership
        raise RuntimeError("custody changed")

    monkeypatch.setattr(archive, "_bounded_catalog_work", catalog)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive, "verify_npi_stage_ownership", AsyncMock())
    monkeypatch.setattr(archive, "_validate_stage_manifest", AsyncMock())
    dump = AsyncMock()
    with pytest.raises(RuntimeError, match="custody changed"):
        await archive.export_prepared_npi_archive(
            sessions,
            prepared=archive.NpiPreparedSource(_manifest(), ownership),
            archive_copy=dump,
            verify_custody=custody,
        )
    archive._lock_family.assert_awaited_once_with(
        session, ownership.schema_name, "ACCESS SHARE", names=archive.RELATION_NAMES
    )
    archive._validate_stage_manifest.assert_not_awaited()
    dump.assert_not_awaited()
    assert events == ["rollback"]
