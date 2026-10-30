# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed policy parsing and native published-read proof boundaries."""

import asyncio
import copy
import hashlib
from contextlib import nullcontext
from dataclasses import FrozenInstanceError
from datetime import datetime
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import asyncpg
import pytest

from process import registry_fhir_catalog_published_read as published
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile


def _pin(name, oid, schema="payload"):
    return {"schema_name": schema, "relation_name": name, "relation_oid": oid}


def _document():
    return {
        "version": 1,
        "reader_role": "published_reader",
        "reader_oid": 101,
        "database_oid": 102,
        "database_system_identifier": "123456789",
        "header": {"relation": _pin("provider_directory_endpoint_dataset", 1), "projection": "catalog_observation_v1"},
        "source_registration": _pin("provider_directory_source", 2, "catalog"),
        "endpoint_registration": _pin("provider_directory_api_endpoint", 3, "catalog"),
        "resource_root": _pin("provider_directory_dataset_resource", 4),
        "bindings": {
            "relation": _pin("provider_directory_insurance_network_source_binding", 5),
            "source_aliases": ["source-a"],
        },
        "datasets": [
            {
                "endpoint_id": "endpoint-a",
                "dataset_id": "dataset-a",
                "dataset_sha256": "a" * 64,
                "source_aliases": ["source-a"],
                "resource_leaves": [_pin("resources_low", 7), _pin("resources_high", 8)],
                "publication_sha256": "b" * 64,
            }
        ],
    }


def _profile(document=None):
    return FHIRCatalogSourceReadProfile(
        "payload",
        "source_owner",
        ("source_runtime",),
        "catalog",
        published_read_scope=published.parse_fhir_catalog_published_read_scope(document or _document()),
    )


def _descriptor(profile):
    entry = profile.published_read_scope.datasets[0]
    publication_by_field = {
        "published_at": "2026-01-01T00:00:00+00:00",
        "root_run_id": "root",
        "resource_hash_contract": None,
        "original_admission_version": None,
        "original_admission_kind": None,
        "original_admission_sha256": None,
        "original_metadata_sha256": None,
        "claimed_resource_count": 0,
    }
    return {
        "coordinates": {
            "source_system": "fhir",
            "source_id": "source-a",
            "dataset_schema": "payload",
            "dataset_id": "dataset-a",
            "producer_id": "registry-fhir-catalog-v1",
            "edition_id": "dataset-a",
        },
        "profile": "published_normalized",
        "endpoint_id": "endpoint-a",
        "source_aliases": ["source-a"],
        "dataset_sha256": entry.dataset_sha256,
        "resource_table_oid": 4,
        "resource_leaf_oids": [7, 8],
        "header_table_oid": 1,
        "database_oid": 102,
        "native_read_profile": profile.document(),
        "admission": {"kind": "catalog_observation", "publication": publication_by_field},
    }


def test_scope_is_canonical_detached_and_deeply_immutable():
    document = _document()
    scope = published.parse_fhir_catalog_published_read_scope(document)
    assert scope.document() == document
    assert type(scope.datasets) is tuple and type(scope.datasets[0].resource_leaves) is tuple
    assert type(scope.bindings.source_aliases) is tuple
    document["datasets"][0]["source_aliases"].append("source-b")
    exported = scope.document()
    exported["header"]["relation"]["relation_oid"] = 999
    assert scope.datasets[0].source_aliases == ("source-a",)
    assert scope.header.relation.relation_oid == 1
    with pytest.raises(FrozenInstanceError):
        scope.datasets[0].resource_leaves[0].relation_oid = 999


@pytest.mark.parametrize(
    "path,value",
    [
        (("version",), True),
        (("version",), 2),
        (("reader_oid",), True),
        (("reader_oid",), 0),
        (("reader_oid",), 2**32),
        (("reader_role",), "bad;sql"),
        (("database_system_identifier",), 123),
        (("database_system_identifier",), "01"),
        (("database_system_identifier",), str(2**64)),
        (("header", "projection"), []),
        (("header", "projection"), "unknown"),
        (("source_registration",), None),
        (("datasets",), []),
        (("datasets",), None),
        (("datasets", 0, "dataset_id"), " space "),
        (("datasets", 0, "dataset_id"), "a" * 97),
        (("datasets", 0, "dataset_id"), "bad\x00value"),
        (("datasets", 0, "dataset_sha256"), "A" * 64),
        (("datasets", 0, "source_aliases"), ["z", "a"]),
        (("datasets", 0, "source_aliases"), ["a", "a"]),
        (("datasets", 0, "resource_leaves"), []),
        (("bindings", "source_aliases"), ["other"]),
    ],
)
def test_malformed_scope_values_always_fail_closed(path, value):
    document = _document()
    target = document
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = value
    with pytest.raises(ValueError):
        published.parse_fhir_catalog_published_read_scope(document)


@pytest.mark.parametrize(
    "path",
    [(), ("header",), ("header", "relation"), ("bindings",), ("datasets", 0), ("datasets", 0, "resource_leaves", 0)],
)
def test_unknown_keys_rejected_at_every_closed_level(path):
    document = _document()
    target = document
    for component in path:
        target = target[component]
    target["unknown"] = None
    with pytest.raises(ValueError):
        published.parse_fhir_catalog_published_read_scope(document)


@pytest.mark.parametrize(
    "change", ["duplicate_oid", "duplicate_name", "leaf_order", "wrong_relation", "duplicate_dataset"]
)
def test_ambiguous_physical_and_dataset_pins_rejected(change):
    document = _document()
    if change == "duplicate_oid":
        document["datasets"][0]["resource_leaves"][0]["relation_oid"] = 1
    elif change == "duplicate_name":
        document["datasets"][0]["resource_leaves"][1]["relation_name"] = "resources_low"
    elif change == "leaf_order":
        document["datasets"][0]["resource_leaves"].reverse()
    elif change == "wrong_relation":
        document["source_registration"]["relation_name"] = "another_source"
    else:
        document["datasets"].append(copy.deepcopy(document["datasets"][0]))
    with pytest.raises(ValueError):
        published.parse_fhir_catalog_published_read_scope(document)


def test_encoded_and_aggregate_limits_are_enforced():
    document = _document()
    document["datasets"] *= 1001
    with pytest.raises(ValueError):
        published.parse_fhir_catalog_published_read_scope(document)
    document = _document()
    document["reader_role"] = "x" * (1024 * 1024)
    with pytest.raises(ValueError, match="bounds"):
        published.parse_fhir_catalog_published_read_scope(document)


def test_descriptor_linkage_has_no_native_authority():
    document = _document()
    descriptor = _descriptor(_profile(document))
    document["datasets"][0]["publication_sha256"] = hashlib.sha256(
        published._json(descriptor["admission"]["publication"])
    ).hexdigest()
    profile = _profile(document)
    descriptor["native_read_profile"] = profile.document()
    assert published.validate_fhir_catalog_published_descriptor(profile, descriptor) is descriptor
    for field, changed in (
        ("database_oid", 999),
        ("resource_leaf_oids", [7]),
        ("source_aliases", ["other"]),
        ("dataset_sha256", "c" * 64),
        ("header_table_oid", 9),
    ):
        invalid_descriptor_by_field = {**descriptor, field: changed}
        with pytest.raises(ValueError, match="linkage"):
            published.validate_fhir_catalog_published_descriptor(profile, invalid_descriptor_by_field)
    descriptor["admission"]["publication"]["claimed_resource_count"] = 1
    with pytest.raises(ValueError, match="publication_changed"):
        published.validate_fhir_catalog_published_descriptor(profile, descriptor)


def _row(pin, **changes):
    return {
        "oid": pin.relation_oid,
        "nspname": pin.schema_name,
        "relname": pin.relation_name,
        "relkind": "r",
        "relpersistence": "p",
        "relispartition": False,
        "relowner": 200,
        "relrowsecurity": False,
        "relforcerowsecurity": False,
        "bound": None,
        "partition_key": None,
        "parents": [],
        "columns": [],
        "constraints": [],
        "indexes": [],
        "inert": True,
        "native_checks": True,
        **changes,
    }


def _physical(scope):
    physical_rows = [_row(pin) for pin, _ in published._metadata(scope)]
    binding = physical_rows[-1]
    binding["columns"] = [
        ["source_id", "character varying(64)", True, None],
        ["resource_id", "character varying(256)", True, None],
        ["network_id", "uuid", True, None],
    ]
    binding["indexes"] = [
        {
            "unique": True,
            "valid": True,
            "method": "btree",
            "native": True,
            "keys": ["source_id", "resource_id"],
            "predicate": None,
            "expression": None,
        }
    ]
    physical_rows.append(_row(scope.resource_root, relkind="p", partition_key="LIST (dataset_id)"))
    anchor = published.RelationPin("payload", "dataset_anchor", 6)
    physical_rows.append(
        _row(
            anchor,
            relkind="p",
            partition_key="RANGE (payload_hash)",
            relispartition=True,
            bound="FOR VALUES IN ('dataset-a')",
            parents=[4],
        )
    )
    physical_rows.extend(
        _row(pin, relispartition=True, parents=[6], bound="FOR VALUES FROM (MINVALUE) TO (MAXVALUE)")
        for pin in scope.datasets[0].resource_leaves
    )
    physical_rows.append(
        _row(
            published.RelationPin("payload", "acquiring", 9),
            parents=[4],
            relispartition=True,
            bound="FOR VALUES IN ('acquiring')",
        )
    )
    return physical_rows


def test_physical_closure_tracks_dataset_subtree_and_not_unrelated_sibling_definition():
    scope = published.parse_fhir_catalog_published_read_scope(_document())
    rows = _physical(scope)
    rows[1]["relowner"] = 300
    rows[2]["relowner"] = 301
    by_oid, routed = published._physical_scope(rows, scope, 200)
    assert routed == list(range(1, 9)) and 9 in by_oid
    rows[-1]["columns"] = [["unrelated", "text", False, None]]
    assert published._physical_scope(rows, scope, 200)[1] == routed


def test_exact_dataset_anchor_can_follow_other_native_partition_levels():
    scope = published.parse_fhir_catalog_published_read_scope(_document())
    rows = _physical(scope)
    next(row for row in rows if row["oid"] == 4)["partition_key"] = "HASH (dataset_id)"
    intermediate = _row(
        published.RelationPin("payload", "hash_bucket", 10),
        relkind="p",
        relispartition=True,
        parents=[4],
        partition_key="LIST (dataset_id)",
        bound="FOR VALUES WITH (modulus 2, remainder 0)",
    )
    next(row for row in rows if row["oid"] == 6)["parents"] = [10]
    rows.append(intermediate)
    assert published._physical_scope(rows, scope, 200)[1] == [*range(1, 9), 10]


@pytest.mark.parametrize(
    "change,error",
    [
        ("mixed_bound", "dataset_partition_required"),
        ("hash_without_list", "dataset_partition_required"),
        ("omitted_leaf", "resource_leaves_changed"),
        ("extra_leaf", "resource_leaves_changed"),
        ("wrong_leaf_name", "physical_identity_invalid"),
        ("binding_owner", "physical_owner_changed"),
        ("binding_type", "binding_shape_invalid"),
        ("binding_nullable", "binding_shape_invalid"),
        ("binding_index", "binding_shape_invalid"),
        ("binding_hook", "binding_shape_invalid"),
    ],
)
def test_native_physical_mismatch_is_refused(change, error):
    scope = published.parse_fhir_catalog_published_read_scope(_document())
    rows = _physical(scope)
    by_oid = {row["oid"]: row for row in rows}
    if change == "omitted_leaf":
        rows.remove(by_oid[8])
    else:
        changes_by_name = {
            "mixed_bound": (6, ("bound",), "FOR VALUES IN ('dataset-a', 'acquiring')"),
            "hash_without_list": (4, ("partition_key",), "HASH (dataset_id)"),
            "extra_leaf": (9, ("parents",), [6]),
            "wrong_leaf_name": (7, ("relname",), "replacement"),
            "binding_owner": (5, ("relowner",), 999),
            "binding_type": (5, ("columns", 0, 1), "text"),
            "binding_nullable": (5, ("columns", 0, 2), False),
            "binding_index": (5, ("indexes", 0, "native"), False),
            "binding_hook": (5, ("inert",), False),
        }
        oid, path, changed_value = changes_by_name[change]
        target = by_oid[oid]
        for component in path[:-1]:
            target = target[component]
        target[path[-1]] = changed_value
    with pytest.raises(ValueError, match=error):
        published._physical_scope(rows, scope, 200)


@pytest.mark.parametrize(
    "column_type,is_notnull,definition,is_validated,should_accept",
    [
        ("character varying(16)", True, "CHECK (((resource_type)::text = 'Organization'::text))", True, True),
        ("character varying(16)", True, None, True, False),
        ("character varying(16)", True, "CHECK ((resource_type IN ('Organization','InsurancePlan')))", True, False),
        ("character varying(16)", True, "CHECK (((resource_type)::text = 'Organization'::text))", False, False),
        ("text", True, "CHECK (((resource_type)::text = 'Organization'::text))", True, False),
        ("character varying(16)", False, "CHECK (((resource_type)::text = 'Organization'::text))", True, False),
    ],
)
def test_optional_binding_type_requires_native_organization_constraint(
    column_type, is_notnull, definition, is_validated, should_accept
):
    scope = published.parse_fhir_catalog_published_read_scope(_document())
    binding_by_field = next(physical_row for physical_row in _physical(scope) if physical_row["oid"] == 5)
    binding_by_field["columns"].append(["resource_type", column_type, is_notnull, "'Organization'::character varying"])
    binding_by_field["constraints"] = (
        [] if definition is None else [{"type": "c", "valid": is_validated, "definition": definition}]
    )
    if should_accept:
        published._binding_shape(binding_by_field)
    else:
        with pytest.raises(ValueError, match="binding_shape_invalid"):
            published._binding_shape(binding_by_field)


@pytest.mark.asyncio
async def test_native_pin_cannot_be_manufactured_by_connection_or_read_ducks():
    connection = Mock(is_in_transaction=Mock(return_value=True), fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="native_connection_required"):
        await published.pin_fhir_catalog_published_read(
            connection, _profile(), endpoint_id="endpoint-a", dataset_id="dataset-a"
        )
    connection.fetchrow.assert_not_awaited()
    forged = object.__new__(published.FHIRCatalogPublishedRead)
    with pytest.raises(ValueError, match="native_pin_required"):
        await forged.verify(connection)
    with pytest.raises(ValueError, match="native_pin_required"):
        forged.resource_sql()


@pytest.mark.asyncio
async def test_key_proof_is_set_based_without_payload_spooling():
    scope = published.parse_fhir_catalog_published_read_scope(_document())
    connection = Mock(fetchval=AsyncMock(return_value=True))
    await published._resource_keys(connection, scope.datasets[0])
    query, dataset_id = connection.fetchval.await_args.args
    assert dataset_id == "dataset-a"
    assert query.count("FROM ONLY") == 2 and "UNION ALL" in query
    assert "GROUP BY resource_type,resource_id HAVING count(*)>1" in query
    assert "dataset_id IS DISTINCT FROM $1" in query
    assert "payload_json" not in query and "payload_hash" not in query
    connection.fetchval.return_value = False
    with pytest.raises(ValueError, match="resource_scope_or_keys_invalid"):
        await published._resource_keys(connection, scope.datasets[0])


@pytest.mark.asyncio
async def test_projection_lock_needs_only_fixed_columns():
    scope = published.parse_fhir_catalog_published_read_scope(_document())
    connection = Mock(fetch=AsyncMock())
    pin, columns = published._metadata(scope)[1]
    await published._lock_projection(connection, pin, columns)
    assert connection.fetch.await_args.args == (
        'SELECT tableoid,source_id,endpoint_id FROM ONLY "catalog"."provider_directory_source" LIMIT 0',
    )


def _two_entry_profile():
    document = _document()
    publication = _descriptor(_profile())["admission"]["publication"]
    document["datasets"][0]["publication_sha256"] = hashlib.sha256(published._json(publication)).hexdigest()
    second_by_field = copy.deepcopy(document["datasets"][0])
    second_by_field.update(
        endpoint_id="endpoint-b",
        dataset_id="dataset-b",
        source_aliases=["source-b"],
        resource_leaves=[_pin("resources_second", 10)],
        dataset_sha256="c" * 64,
    )
    document["datasets"].append(second_by_field)
    document["bindings"]["source_aliases"] = ["source-a", "source-b"]
    return _profile(document)


@pytest.mark.asyncio
async def test_read_rejects_another_admitted_entry_descriptor(monkeypatch):
    profile = _two_entry_profile()
    scope = profile.published_read_scope
    actor_by_field = {
        "backend": 1,
        "transaction": "1/2",
        "reader_oid": scope.reader_oid,
        "database_oid": scope.database_oid,
        "database_system_identifier": scope.database_system_identifier,
    }
    connection = Mock(
        is_in_transaction=Mock(return_value=True), fetchval=AsyncMock(return_value=True), execute=AsyncMock()
    )
    monkeypatch.setattr(published, "_identity", AsyncMock(return_value=actor_by_field))
    read = published.FHIRCatalogPublishedRead(
        connection,
        asyncio.current_task(),
        profile,
        scope,
        scope.datasets[0],
        tuple(actor_by_field.values()),
        (4, (7, 8), 1, 102, "d" * 64),
        (),
        MappingProxyType({"previous_dataset_id": None}),
        "https://example.test/fhir",
        ("source-a",),
    )
    published._ISSUED[read] = tuple(getattr(read, name) for name in read.__dataclass_fields__)
    descriptor = _descriptor(profile)
    descriptor.update(native_catalog_sha256="d" * 64, source_uri=read.source_uri, previous_dataset_id=None)
    try:
        assert await read.verify(connection, descriptor=descriptor) is read
        second_descriptor = copy.deepcopy(descriptor)
        second_descriptor["coordinates"].update(source_id="source-b", dataset_id="dataset-b", edition_id="dataset-b")
        second_descriptor.update(
            endpoint_id="endpoint-b", source_aliases=["source-b"], dataset_sha256="c" * 64, resource_leaf_oids=[10]
        )
        published.validate_fhir_catalog_published_descriptor(profile, second_descriptor)
        with pytest.raises(ValueError, match="native_pin_changed"):
            await read.verify(connection, descriptor=second_descriptor)
    finally:
        published._ISSUED.pop(read, None)


@pytest.mark.asyncio
async def test_inventory_pins_only_exact_fixed_metadata_columns(monkeypatch):
    connection = MagicMock(fetch=AsyncMock(), execute=AsyncMock(), fetchval=AsyncMock(side_effect=["0", True, "0"]))
    identity = AsyncMock(return_value={"transaction": "1/2"})
    monkeypatch.setattr(published, "_identity", identity)
    assert await published.pin_fhir_catalog_published_inventory(connection, _profile()) is None
    assert identity.await_count == 2
    queries = [call.args[0] for call in connection.fetch.await_args_list]
    assert len(queries) == 4 and all(" FROM ONLY " in query and query.endswith(" LIMIT 0") for query in queries)
    assert not any("payload_json" in query or "provider_directory_dataset_resource" in query for query in queries)
    lock_query, lock_document = connection.fetchval.await_args_list[1].args
    assert lock_query == published._LOCKS_SQL
    assert [relation["oid"] for relation in published.json.loads(lock_document)] == [1, 2, 3, 5]


@pytest.mark.asyncio
async def test_pin_checks_every_admitted_publication_before_resource_traversal(monkeypatch):
    connection = MagicMock(fetchval=AsyncMock(return_value="0"), execute=AsyncMock())
    header = AsyncMock(side_effect=[{}, published.FHIRCatalogPublishedReadError("publication_invalid")])
    keys = AsyncMock()
    monkeypatch.setattr(published, "_identity", AsyncMock(return_value=_native_actor()))
    monkeypatch.setattr(published, "_physical", AsyncMock(return_value=("d" * 64, ())))
    monkeypatch.setattr(published, "_registration", AsyncMock(return_value="https://example.test/fhir"))
    monkeypatch.setattr(published, "_header", header)
    monkeypatch.setattr(published, "_resource_keys", keys)
    profile = _two_entry_profile()
    with pytest.raises(ValueError, match="publication_invalid"):
        await published.pin_fhir_catalog_published_read(
            connection, profile, endpoint_id="endpoint-a", dataset_id="dataset-a"
        )
    assert [(call.args[2].dataset_id, call.args[3]) for call in header.await_args_list] == [
        ("dataset-a", False),
        ("dataset-b", False),
    ]
    keys.assert_not_awaited()


def _native_actor():
    return {
        "backend": 1,
        "transaction": "1/2",
        "reader_oid": 101,
        "database_oid": 102,
        "database_system_identifier": "123456789",
    }


@pytest.fixture
def cached_scope_case(monkeypatch):
    connection = MagicMock(is_in_transaction=Mock(return_value=True), execute=AsyncMock())
    connection.fetchval = AsyncMock(side_effect=lambda sql, *_: "0" if "lock_timeout" in sql else True)
    identity = AsyncMock(return_value=_native_actor())
    headers = AsyncMock(
        side_effect=lambda *_: {
            "status": "published",
            "is_current": True,
            "current_count": 1,
            "previous_dataset_id": None,
        }
    )
    registrations = AsyncMock(return_value="https://example.test/fhir")
    bindings, keys, locks = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(published, "_identity", identity)
    monkeypatch.setattr(published, "_physical", AsyncMock(return_value=("d" * 64, ())))
    monkeypatch.setattr(published, "_header", headers)
    monkeypatch.setattr(published, "_registration", registrations)
    monkeypatch.setattr(published, "_binding_scope", bindings)
    monkeypatch.setattr(published, "_resource_keys", keys)
    monkeypatch.setattr(published, "_require_relation_locks", locks)
    return SimpleNamespace(
        connection=connection,
        profile=_two_entry_profile(),
        identity=identity,
        headers=headers,
        registrations=registrations,
        bindings=bindings,
        keys=keys,
        locks=locks,
    )


@pytest.mark.asyncio
async def test_owned_scope_reuses_full_proof_but_checks_each_handle(cached_scope_case):
    case = cached_scope_case
    with published.fhir_catalog_published_reads(case.connection):
        for dataset in ("dataset-a", "dataset-b", "dataset-a"):
            with published.fhir_catalog_published_reads(case.connection):
                await published.pin_fhir_catalog_published_read(
                    case.connection, case.profile, endpoint_id="endpoint-" + dataset[-1], dataset_id=dataset
                )
        assert case.headers.await_count == case.registrations.await_count == 2
        case.bindings.assert_awaited_once()
        assert case.keys.await_count == 2
        assert case.locks.await_count == 4
        assert case.identity.await_count == 7
        assert sum(call.args[0] == published._ACCESS_SQL for call in case.connection.fetchval.await_args_list) == 4


@pytest.mark.asyncio
async def test_naked_pin_does_not_reuse_scope(cached_scope_case):
    case = cached_scope_case
    for _ in range(2):
        await published.pin_fhir_catalog_published_read(
            case.connection, case.profile, endpoint_id="endpoint-a", dataset_id="dataset-a"
        )
    assert case.headers.await_count == case.registrations.await_count == 4
    assert case.bindings.await_count == case.keys.await_count == 2


@pytest.mark.asyncio
async def test_lost_locks_refuse_before_refresh_or_cached_reuse(cached_scope_case):
    case = cached_scope_case
    with published.fhir_catalog_published_reads(case.connection):
        await published.pin_fhir_catalog_published_read(
            case.connection, case.profile, endpoint_id="endpoint-a", dataset_id="dataset-a"
        )
        case.connection.execute.reset_mock()
        case.locks.side_effect = published.FHIRCatalogPublishedReadError("native_projection_locks_changed")
        with pytest.raises(ValueError, match="native_projection_locks_changed"):
            await published.pin_fhir_catalog_published_read(
                case.connection, case.profile, endpoint_id="endpoint-b", dataset_id="dataset-b"
            )
        case.connection.execute.assert_not_awaited()
        assert published._owned_context(case.connection).scopes_by_profile == {}
        assert case.keys.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError, asyncio.CancelledError])
async def test_scope_context_clears_data_and_owner_on_failure(error):
    connection = Mock()
    validation = published._ScopeValidation("d" * 64, (), {}, {"dataset"})
    with pytest.raises(error):
        with published.fhir_catalog_published_reads(connection):
            context = published._owned_context(connection)
            context.remember(b"profile", validation)
            raise error()
    assert context.active is False and context.scopes_by_profile == {} and context.encoded_bytes == 0
    assert context.task is context.connection is context.origin is None
    assert not validation.checked_datasets
    assert published._READ_CONTEXT.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", ["profiles", "bytes"])
async def test_scope_context_bounds_are_enforced(limit):
    with published.fhir_catalog_published_reads(Mock()):
        context = published._READ_CONTEXT.get()
        if limit == "profiles":
            for number in range(64):
                context.remember(str(number).encode(), published._ScopeValidation("d" * 64, (), {}))
        else:
            context.remember(b"x" * published._MAX_BYTES, published._ScopeValidation("d" * 64, (), {}))
        with pytest.raises(ValueError, match="scope_cache_bounds"):
            context.remember(b"over-limit", published._ScopeValidation("d" * 64, (), {}))


@pytest.mark.parametrize(
    "change",
    ["duplicate", "metadata_parent", "foreign_schema", "row_security", "temporary", "root_heap", "metadata_partition"],
)
def test_physical_catalog_requires_exact_permanent_unprotected_relation_shapes(change):
    scope = published.parse_fhir_catalog_published_read_scope(_document())
    rows = _physical(scope)
    by_oid = {row["oid"]: row for row in rows}
    if change == "duplicate":
        rows.append(copy.deepcopy(rows[0]))
    else:
        oid, field, replacement = {
            "metadata_parent": (1, "parents", [4]),
            "foreign_schema": (9, "nspname", "unadmitted"),
            "row_security": (7, "relrowsecurity", True),
            "temporary": (7, "relpersistence", "t"),
            "root_heap": (4, "relkind", "r"),
            "metadata_partition": (1, "relkind", "p"),
        }[change]
        by_oid[oid][field] = replacement
    with pytest.raises(ValueError, match="physical_identity_invalid|dataset_partition_required|metadata_shape_invalid"):
        published._physical_scope(rows, scope, 200)


@pytest.mark.parametrize("change", ["profile", "scope", "reader_alias", "schema"])
def test_native_profile_scope_cannot_be_substituted(change):
    profile = _profile()
    with pytest.raises(ValueError, match="profile_invalid|scope_required|profile_linkage_invalid"):
        if change == "profile":
            profile = SimpleNamespace(**profile.document())
        else:
            from dataclasses import replace

            profile = replace(
                profile,
                **{
                    "scope": {"published_read_scope": None},
                    "reader_alias": {"runtime_roles": ("published_reader",)},
                    "schema": {"schema_name": "other_payload"},
                }[change],
            )
        published.validate_fhir_catalog_published_descriptor(profile, _descriptor(_profile()))


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("coordinates", None, "descriptor_invalid"),
        ("resource_leaf_oids", (7, 8), "descriptor_invalid"),
        ("admission", None, "descriptor_invalid"),
        ("admission", {"kind": "catalog_observation", "publication": []}, "descriptor_linkage_invalid"),
        ("admission", {"kind": "semantic"}, "descriptor_linkage_invalid"),
        ("endpoint_id", "unadmitted", "dataset_not_admitted"),
    ],
)
def test_descriptor_requires_complete_selected_publication_contract(field, value, error):
    descriptor = _descriptor(_profile())
    descriptor[field] = value
    with pytest.raises(ValueError, match=error):
        published.validate_fhir_catalog_published_descriptor(_profile(), descriptor)


@pytest.mark.asyncio
async def test_inventory_database_error_preserves_cause_without_partial_pin(monkeypatch):
    connection = MagicMock(fetchval=AsyncMock(return_value="0"), execute=AsyncMock())
    error = asyncpg.InsufficientPrivilegeError("projection refused")
    monkeypatch.setattr(published, "_identity", AsyncMock(side_effect=error))
    with pytest.raises(published.FHIRCatalogPublishedReadError, match="unavailable") as caught:
        await published.pin_fhir_catalog_published_inventory(connection, _profile())
    assert caught.value.__cause__ is error
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_incomplete_native_acl_inventory_cannot_produce_a_fingerprint():
    connection = Mock(fetch=AsyncMock(return_value=[]))
    with pytest.raises(ValueError, match="physical_identity_invalid"):
        await published._native_fingerprint(connection, {1: {}}, [1])


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [asyncpg.InsufficientPrivilegeError, asyncio.CancelledError])
async def test_failed_pin_discards_cached_proof_and_next_attempt_revalidates(cached_scope_case, error_type):
    case = cached_scope_case
    with published.fhir_catalog_published_reads(case.connection):
        await published.pin_fhir_catalog_published_read(
            case.connection, case.profile, endpoint_id="endpoint-a", dataset_id="dataset-a"
        )
        case.keys.side_effect = error_type("proof interrupted")
        error = (
            published.FHIRCatalogPublishedReadError if error_type is asyncpg.InsufficientPrivilegeError else error_type
        )
        with pytest.raises(error):
            await published.pin_fhir_catalog_published_read(
                case.connection, case.profile, endpoint_id="endpoint-b", dataset_id="dataset-b"
            )
        assert not published._owned_context(case.connection).scopes_by_profile
        case.keys.side_effect = None
        await published.pin_fhir_catalog_published_read(
            case.connection, case.profile, endpoint_id="endpoint-a", dataset_id="dataset-a"
        )
    assert case.headers.await_count == case.registrations.await_count == 4
    assert case.bindings.await_count == 2


@pytest.mark.asyncio
async def test_issued_read_cannot_escape_issuing_task_or_changed_native_snapshot(cached_scope_case):
    case = cached_scope_case
    read = await published.pin_fhir_catalog_published_read(
        case.connection, case.profile, endpoint_id="endpoint-a", dataset_id="dataset-a"
    )
    assert read.binding_relation == 'ONLY "payload"."provider_directory_insurance_network_source_binding"'
    assert read.native["resource_leaf_oids"] == [7, 8]
    read.native["resource_leaf_oids"].append(999)
    assert read.native["resource_leaf_oids"] == [7, 8]
    with pytest.raises(ValueError, match="native_pin_required"):
        await asyncio.create_task(read.verify(case.connection))
    case.connection.is_in_transaction.side_effect = asyncpg.InterfaceError("connection closed")
    with pytest.raises(ValueError, match="native_pin_required"):
        read.resource_sql()


@pytest.mark.asyncio
async def test_published_context_requires_an_actual_task_in_the_running_event_loop():
    loop = asyncio.get_running_loop()
    result = loop.create_future()

    def enter_without_task():
        try:
            with published.fhir_catalog_published_reads(Mock()):
                result.set_result(None)
        except BaseException as error:
            result.set_exception(error)

    loop.call_soon(enter_without_task)
    with pytest.raises(published.FHIRCatalogPublishedReadError, match="native_pin_required"):
        await asyncio.wait_for(result, 1)
    assert published._READ_CONTEXT.get() is None


@pytest.mark.parametrize("field", ["source_aliases", "bindings"])
def test_published_scope_cannot_admit_an_empty_source_identity_set(field):
    document = _document()
    if field == "source_aliases":
        document["datasets"][0]["source_aliases"] = []
    else:
        document["bindings"]["source_aliases"] = []
    with pytest.raises(published.FHIRCatalogPublishedReadError):
        published.parse_fhir_catalog_published_read_scope(document)


def test_relation_pin_document_is_detached_and_keeps_exact_physical_coordinates():
    relation = published.RelationPin("payload", "resources_low", 7)
    document = relation.document()
    assert document == _pin("resources_low", 7)
    document["relation_oid"] = 99
    assert relation.relation_oid == 7


@pytest.mark.asyncio
async def test_inventory_projection_rechecks_the_native_transaction_after_lock_timeout_restoration(monkeypatch):
    connection = MagicMock(fetch=AsyncMock(), execute=AsyncMock(), fetchval=AsyncMock(side_effect=["0", True, "0"]))
    identity = AsyncMock(side_effect=[{"transaction": "1/2"}, {"transaction": "1/3"}])
    monkeypatch.setattr(published, "_identity", identity)
    with pytest.raises(published.FHIRCatalogPublishedReadError, match="native_transaction_changed"):
        await published.pin_fhir_catalog_published_inventory(connection, _profile())
    assert identity.await_count == 2
    assert connection.fetchval.await_args.args == ("SELECT pg_catalog.set_config('lock_timeout',$1,true)", "0")


def _published_header(profile):
    entry = profile.published_read_scope.datasets[0]
    publication = _descriptor(profile)["admission"]["publication"]
    return {
        **publication,
        "oid": profile.published_read_scope.header.relation.relation_oid,
        "endpoint_id": entry.endpoint_id,
        "dataset_id": entry.dataset_id,
        "dataset_hash": entry.dataset_sha256,
        "status": "published",
        "is_current": True,
        "current_count": 1,
        "published_at": datetime.fromisoformat(publication["published_at"]),
        "resource_count": publication["claimed_resource_count"],
        "hash_contract_type": None,
        "previous_dataset_id": None,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "missing", "duplicate", "resource_count"])
async def test_header_read_needs_one_exact_publication_and_original_resource_count(damage):
    profile = _two_entry_profile()
    scope, header = profile.published_read_scope, _published_header(profile)
    rows = [] if damage == "missing" else [header] * (2 if damage == "duplicate" else 1)
    if damage == "resource_count":
        header["resource_count"] += 1
    connection = Mock(fetch=AsyncMock(return_value=rows))
    reason = "publication_changed" if damage == "resource_count" else "publication_unavailable"
    with pytest.raises(published.FHIRCatalogPublishedReadError, match=reason) if damage else nullcontext():
        assert await published._header(connection, scope, scope.datasets[0], True) == header
    assert "FROM ONLY" in connection.fetch.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("require_current", [None, 0, 1])
async def test_published_pin_requires_a_boolean_freshness_request_before_native_queries(require_current):
    connection = Mock(fetchrow=AsyncMock())
    with pytest.raises(published.FHIRCatalogPublishedReadError):
        await published.pin_fhir_catalog_published_read(
            connection, _profile(), endpoint_id="endpoint-a", dataset_id="dataset-a", require_current=require_current
        )
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("native_catalog_sha256", "e" * 64),
        ("source_uri", "https://other.test/fhir"),
        ("previous_dataset_id", "older-dataset"),
    ],
)
async def test_issued_handle_refuses_changed_native_catalog_or_registration_evidence(cached_scope_case, field, value):
    case = cached_scope_case
    read = await published.pin_fhir_catalog_published_read(
        case.connection, case.profile, endpoint_id="endpoint-a", dataset_id="dataset-a"
    )
    descriptor = _descriptor(case.profile)
    descriptor.update(native_catalog_sha256="d" * 64, source_uri=read.source_uri, previous_dataset_id=None)
    assert await read.verify(case.connection, descriptor=descriptor) is read
    descriptor[field] = value
    with pytest.raises(published.FHIRCatalogPublishedReadError, match="native_catalog_changed"):
        await read.verify(case.connection, descriptor=descriptor)
