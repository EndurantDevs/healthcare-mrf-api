# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy epoch compatibility uses logical names and data, independent of catalog identity."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest

from process import network_fhir_source_epoch as epoch
from process import provider_directory_cms_source_runtime as runtime
from process.network_fhir_membership_source import PinnedFHIRMembershipSource


def _edition():
    pin = PinnedFHIRMembershipSource(
        "source_example",
        "cms-npd",
        "endpoint",
        "dataset",
        "a" * 64,
        "b" * 64,
        {"schema": "source_example", "name": "provider_directory_dataset_resource"},
        "alias",
        "2026-01-01",
    )
    identity = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    return pin, epoch.RetainedCMSFHIRSourceEpoch(
        epoch._origin(pin), identity, "registry_cms_epoch_" + identity.hex, "c" * 64, "d" * 64, "e" * 64
    )


def _legacy_wire():
    _, edition = _edition()
    return {
        **edition.as_dict(),
        "schema_oid": 100,
        "relation_oids": [[name, ordinal] for ordinal, name in enumerate(sorted(epoch._TABLES), 200)],
        "owner_role": "old_owner",
        "runtime_roles": ["old_reader"],
        "catalog_sha256": "f" * 64,
    }


def test_complete_legacy_wire_projects_to_same_named_edition_after_copy():
    _, edition = _edition()
    original, copied = _legacy_wire(), _legacy_wire()
    copied.update(schema_oid=500, owner_role="copy_owner", runtime_roles=["copy_reader"], catalog_sha256="1" * 64)
    copied["relation_oids"] = [[name, oid + 1000] for name, oid in copied["relation_oids"]]
    copied["origin_coordinates"][6] = 9000
    original["origin_coordinates"][6] = 8000
    assert epoch.validate_retained_cms_fhir_source_epoch(original) == edition
    assert epoch.validate_retained_cms_fhir_source_epoch(copied) == edition
    assert epoch.validate_retained_cms_fhir_source_epoch(edition.as_dict()) == edition
    assert not set(edition.as_dict()) & {"schema_oid", "relation_oids", "owner_role", "runtime_roles", "catalog_sha256"}


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_oid", True),
        ("relation_oids", []),
        ("runtime_roles", "reader"),
        ("owner_role", "old_reader"),
        ("catalog_sha256", "bad"),
        ("epoch_id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        ("origin_coordinates", "invalid"),
        ("admission_sha256", "bad"),
    ],
)
def test_legacy_wire_still_requires_a_complete_well_formed_document(field, value):
    wire = _legacy_wire()
    wire[field] = value
    with pytest.raises(epoch.FHIRSourceEpochError, match="^fhir_source_epoch_unavailable$"):
        epoch.validate_retained_cms_fhir_source_epoch(wire)


def _metadata(pin, edition):
    return dict(
        endpoint_id=pin.endpoint_id,
        dataset_hash=pin.dataset_sha256,
        content_proof_admission_sha256=edition.admission_sha256,
        publication_metadata_sha256=edition.metadata_sha256,
        source_matches=True,
        release_matches=True,
    )


@pytest.fixture
def reader(monkeypatch):
    pin, edition = _edition()
    connection = SimpleNamespace(execute=AsyncMock(), fetchrow=AsyncMock(return_value=_metadata(pin, edition)))
    transaction = AsyncMock()
    monkeypatch.setattr(runtime, "require_registry_epoch_transaction", transaction)
    return pin, edition, connection, transaction


@pytest.mark.asyncio
async def test_metadata_mode_checks_named_tables_and_logical_declarations_without_full_scans(reader, monkeypatch):
    pin, edition, connection, transaction = reader
    catalog, content = (
        AsyncMock(side_effect=AssertionError("catalog scan")),
        AsyncMock(side_effect=AssertionError("content scan")),
    )
    monkeypatch.setattr(epoch, "_catalog", catalog)
    monkeypatch.setattr(epoch, "_content", content)
    assert (
        await epoch.require_retained_cms_fhir_source_epoch(connection, pin, _legacy_wire(), verify_content=False)
        == edition
    )
    transaction.assert_awaited_once_with(connection, pin, edition)
    catalog.assert_not_awaited()
    content.assert_not_awaited()
    assert len(connection.execute.await_args_list) == len(epoch._TABLES)
    for table, call in zip(epoch._TABLES, connection.execute.await_args_list, strict=True):
        assert call.args == (f'SELECT 1 FROM "{edition.schema_name}"."{table}" LIMIT 0',)
    query, dataset, source_ids, release_id = connection.fetchrow.await_args.args
    assert json.loads(source_ids) == [pin.source_id] and release_id == pin.release_id
    assert dataset == pin.dataset_id and "WHERE dataset_id=$1" in query
    assert not any(word in query for word in ("pg_roles", "obj_description", "oid", "count(*)", "sha256("))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "endpoint_id",
        "dataset_hash",
        "content_proof_admission_sha256",
        "publication_metadata_sha256",
        "source_matches",
        "release_matches",
    ],
)
async def test_metadata_mode_refuses_changed_logical_source_or_admission(reader, field):
    pin, edition, connection, _ = reader
    connection.fetchrow.return_value[field] = False if field in {"source_matches", "release_matches"} else "different"
    with pytest.raises(epoch.FHIRSourceEpochError):
        await epoch.require_retained_cms_fhir_source_epoch(connection, pin, edition, verify_content=False)


@pytest.mark.asyncio
async def test_metadata_mode_refuses_changed_source_coordinates_before_io(reader):
    pin, edition, connection, _ = reader
    with pytest.raises(epoch.FHIRSourceEpochError):
        await epoch.require_retained_cms_fhir_source_epoch(
            connection, replace(pin, release_id="other"), edition, verify_content=False
        )
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("observed", ["e" * 64, "0" * 64])
async def test_full_mode_checks_functional_catalog_and_complete_content(reader, monkeypatch, observed):
    pin, edition, connection, _ = reader
    catalog, content = AsyncMock(), AsyncMock(return_value=observed)
    monkeypatch.setattr(epoch, "_catalog", catalog)
    monkeypatch.setattr(epoch, "_content", content)
    if observed == edition.content_sha256:
        assert await epoch.require_retained_cms_fhir_source_epoch(connection, pin, edition) == edition
    else:
        with pytest.raises(epoch.FHIRSourceEpochError):
            await epoch.require_retained_cms_fhir_source_epoch(connection, pin, edition)
    catalog.assert_awaited_once_with(connection, edition.schema_name)
    content.assert_awaited_once_with(connection, edition.schema_name)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        asyncpg.QueryCanceledError("cancelled"),
        asyncpg.InterfaceError("closed"),
        TimeoutError(),
        ConnectionError(),
        RuntimeError(),
        asyncio.CancelledError(),
    ],
)
async def test_reader_preserves_database_failure_causes_and_cancellation(reader, failure):
    pin, edition, connection, _ = reader
    connection.fetchrow.side_effect = failure
    if isinstance(failure, asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError) as caught:
            await epoch.require_retained_cms_fhir_source_epoch(connection, pin, edition, verify_content=False)
        assert caught.value is failure
    else:
        with pytest.raises(epoch.FHIRSourceEpochError) as caught:
            await epoch.require_retained_cms_fhir_source_epoch(connection, pin, edition, verify_content=False)
        assert caught.value.__cause__ is (None if type(failure) is RuntimeError else failure)


def _relations():
    indexes_by_table = {
        table: [("cms_epoch_pk_" + str(index), keys, True)] for index, (table, keys) in enumerate(epoch._PRIMARY_KEYS)
    }
    for name, table, keys in epoch._LOOKUP_INDEXES:
        indexes_by_table[table].append((name, keys, False))
    relation_rows = []
    for table in sorted(epoch._TABLES):
        keys = set(key for _, columns, _ in indexes_by_table[table] for key in columns)
        relation_rows.append(
            dict(
                relname=table,
                relkind=b"r",
                relpersistence=b"p",
                relhastriggers=False,
                relrowsecurity=False,
                relforcerowsecurity=False,
                inherited=False,
                defaults=False,
                columns=json.dumps([[key, 1043, -1, True, "", ""] for key in sorted(keys)]),
                constraints=json.dumps([["p", "PRIMARY KEY"]]),
                indexes=json.dumps(
                    [
                        dict(
                            name=name,
                            columns=columns,
                            method="btree",
                            unique=primary,
                            primary=primary,
                            immediate=True,
                            standard_keys=True,
                            valid=True,
                            ready=True,
                            live=True,
                            key_count=len(columns),
                            attribute_count=len(columns),
                            expressions=False,
                            predicate=False,
                            options=" ".join("0" for _ in columns),
                        )
                        for name, columns, primary in sorted(indexes_by_table[table])
                    ]
                ),
            )
        )
    return relation_rows


def _mutate_epoch_relations(relation_rows, damage):
    if damage == "missing_table":
        relation_rows.pop()
        return
    if damage == "row_security":
        relation_rows[0]["relrowsecurity"] = True
        return
    if damage == "index":
        relation_rows[0]["indexes"] = "[]"
        return
    if damage in {"opclass", "deferred"}:
        indexes = json.loads(relation_rows[0]["indexes"])
        indexes[0]["standard_keys" if damage == "opclass" else "immediate"] = False
        relation_rows[0]["indexes"] = json.dumps(indexes)
        return
    if damage in {"column", "string_type", "nullable_key"}:
        columns = json.loads(relation_rows[0]["columns"])
        if damage == "string_type":
            columns[0][1] = "1043"
        else:
            columns[0][1 if damage == "column" else 3] = 99999 if damage == "column" else False
        relation_rows[0]["columns"] = json.dumps(columns)
        return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [None, "missing_table", "row_security", "index", "column", "string_type", "nullable_key", "opclass", "deferred"],
)
async def test_functional_catalog_uses_only_named_layout_keys_and_indexes(monkeypatch, damage):
    _, edition = _edition()
    relation_rows = _relations()
    _mutate_epoch_relations(relation_rows, damage)
    connection = SimpleNamespace(execute=AsyncMock())
    capture = AsyncMock(return_value=relation_rows)
    monkeypatch.setattr(epoch, "_epoch_catalog_relations", capture)
    if damage:
        with pytest.raises(epoch.FHIRSourceEpochError):
            await epoch._catalog(connection, edition.schema_name)
    else:
        assert await epoch._catalog(connection, edition.schema_name) is None
    capture.assert_awaited_once_with(connection, edition.schema_name)
    assert connection.execute.await_args.args[0].startswith('LOCK TABLE "' + edition.schema_name + '".')
    assert connection.execute.await_args.args[0].endswith(" IN ACCESS SHARE MODE NOWAIT")


def test_historical_recipe_keeps_its_published_generation_after_logical_decode():
    import hashlib

    from process.registry_source_recipe_store import _decode_flat_fhir_pin

    pin, edition = _edition()
    document_by_field = {**pin.coordinates, "retained_epoch": _legacy_wire()}
    document_by_field.pop("resource_relation")
    document_by_field["resource_table_oid"] = 777
    expected = hashlib.sha256(json.dumps(document_by_field, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    decoded = _decode_flat_fhir_pin(document_by_field)
    assert decoded.retained_epoch == edition
    assert decoded.generation_id == expected
    assert _decode_flat_fhir_pin(decoded.coordinates).generation_id == expected


@pytest.mark.asyncio
async def test_epoch_catalog_query_collects_functional_metadata_without_physical_census(monkeypatch):
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def settings(_connection):
        yield

    connection = SimpleNamespace(execute=AsyncMock(), fetch=AsyncMock(return_value=[]))
    monkeypatch.setattr(epoch, "_borrowed_epoch_settings", settings)
    assert await epoch._epoch_catalog_relations(connection, "synthetic") == []
    query, schema = connection.fetch.await_args.args
    assert schema == "synthetic"
    assert "jsonb_build_array(a.attname,a.atttypid::bigint,a.atttypmod" in query
    assert all(word in query for word in ("columns", "indexes", "constraints", "standard_keys", "opcdefault"))
    assert not any(word in query for word in ("filenode", "relowner", "nspowner", "pg_roles", "acl", "obj_description"))


@pytest.mark.asyncio
async def test_capture_closes_candidate_with_existing_native_freeze_helper(monkeypatch):
    from process import provider_directory_generic_source_provision as provision

    pin, edition = _edition()
    connection = SimpleNamespace(execute=AsyncMock())
    freeze = AsyncMock()
    monkeypatch.setattr(epoch, "_require_epoch_copy_key", AsyncMock())
    monkeypatch.setattr(epoch, "_copy_epoch_rows", AsyncMock())
    monkeypatch.setattr(epoch, "_create_epoch_indexes", AsyncMock())
    monkeypatch.setattr(provision, "freeze_fhir_source_snapshot", freeze)
    await epoch._copy_epoch(connection, pin, edition.schema_name, "candidate_owner", ("reader",))
    freeze.assert_awaited_once_with(
        connection,
        edition.schema_name,
        namespace_owner="candidate_owner",
        reader_roles=("reader",),
        application_roles=("reader",),
    )
    assert connection.execute.await_args.args == ("SET LOCAL ROLE NONE",)
    assert not any(call.args[0].startswith(("GRANT", "ALTER")) for call in connection.execute.await_args_list)
