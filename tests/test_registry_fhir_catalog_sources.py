# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source inventory never upgrades catalog names or incomplete receipts into authority."""

import asyncio
import copy
import json
from contextlib import nullcontext
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_source_fence as source_fence
from process import registry_fhir_catalog_sources as sources
from process.provider_directory_admission_seal import ADMISSION_GENERIC_PROOF_SUMMARY_KEY, _receipt


def read_profile():
    return sources.FHIRCatalogSourceReadProfile("source_payload", "source_owner", ("source_reader",), "source_catalog")


def scoped_read_profile(*, projection="semantic_admission_v1"):
    from process.registry_fhir_catalog_published_read import parse_fhir_catalog_published_read_scope

    def pin(schema, name, oid):
        return {"schema_name": schema, "relation_name": name, "relation_oid": oid}

    scope_by_field = {
        "version": 1,
        "reader_role": "published_reader",
        "reader_oid": 601,
        "database_oid": 104,
        "database_system_identifier": "123456789",
        "header": {
            "relation": pin("source_payload", "provider_directory_endpoint_dataset", 103),
            "projection": projection,
        },
        "source_registration": pin("source_catalog", "provider_directory_source", 130),
        "endpoint_registration": pin("source_catalog", "provider_directory_api_endpoint", 131),
        "resource_root": pin("source_payload", "provider_directory_dataset_resource", 101),
        "bindings": {
            "relation": pin("source_payload", "provider_directory_insurance_network_source_binding", 132),
            "source_aliases": ["alias-a", "alias-b"],
        },
        "datasets": [
            {
                "endpoint_id": "endpoint-one",
                "dataset_id": "dataset-one",
                "dataset_sha256": "a" * 64,
                "source_aliases": ["alias-a", "alias-b"],
                "resource_leaves": [pin("source_payload", "source_resources_one", 102)],
                "publication_sha256": "b" * 64,
            }
        ],
    }
    return sources.FHIRCatalogSourceReadProfile(
        "source_payload",
        "source_owner",
        ("source_reader",),
        "source_catalog",
        parse_fhir_catalog_published_read_scope(scope_by_field),
    )


def test_optional_published_profile_preserves_legacy_document_and_roundtrips():
    legacy = read_profile().document()
    assert set(legacy) == {"schema_name", "owner_role", "runtime_roles", "catalog_schema_name"}
    assert sources.parse_fhir_catalog_source_profile(legacy).document() == legacy
    scoped = scoped_read_profile().document()
    assert set(scoped) == set(legacy) | {"published_read_scope"}
    parsed = sources.parse_fhir_catalog_source_profile(scoped)
    scoped["published_read_scope"]["datasets"][0]["source_aliases"].append("alias-c")
    assert parsed.document() == scoped_read_profile().document()


@pytest.mark.parametrize("change", ["null", "unknown", "runtime_reader", "wrong_schema"])
def test_published_profile_rejects_unknown_null_or_ambiguous_custody(change):
    profile = scoped_read_profile().document()
    if change == "null":
        profile["published_read_scope"] = None
    elif change == "unknown":
        profile["other_scope"] = {}
    elif change == "runtime_reader":
        profile["published_read_scope"]["reader_role"] = "source_reader"
    else:
        profile["published_read_scope"]["header"]["relation"]["schema_name"] = "other_schema"
    with pytest.raises(ValueError):
        sources.parse_fhir_catalog_source_profile(profile)


def test_scoped_observation_inventory_reads_only_its_fixed_metadata_projection():
    query = sources._page_query(scoped_read_profile(projection="catalog_observation_v1"), {})
    for table in (sources._HEADER_TABLE, sources._SOURCE_TABLE, sources._ENDPOINT_TABLE):
        assert "." + table in query
    assert 'FROM ONLY "source_payload".' + sources._HEADER_TABLE in query
    assert 'FROM ONLY "source_catalog".' + sources._SOURCE_TABLE in query
    assert 'FROM ONLY "source_catalog".' + sources._ENDPOINT_TABLE in query
    physical_header = query.split("headers AS MATERIALIZED (", 1)[1].split("), ranked AS", 1)[0]
    assert "NULL AS publication_metadata_summary_json" in physical_header
    assert "d.publication_metadata_summary_json" not in physical_header
    assert "d.completion_proof_json" not in physical_header


@pytest.mark.asyncio
async def test_scoped_semantic_inventory_requires_owner_compatibility_without_observation_fallback():
    connection = Mock(fetch=AsyncMock(), fetchval=AsyncMock(), is_in_transaction=Mock(return_value=True))
    with pytest.raises(sources.FHIRCatalogSourceError, match="scoped_semantic_compatibility_required"):
        async for _ in sources.iter_fhir_catalog_sources(connection, scoped_read_profile()):
            pytest.fail("unqualified semantic source admitted")
    connection.fetch.assert_not_called()
    connection.fetchval.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("outer_context", [False, True])
async def test_partial_inventory_does_not_own_context_across_yield(monkeypatch, outer_context):
    from process import registry_fhir_catalog_published_read as published

    async def pages(*_args, **_kwargs):
        yield ("page",)

    monkeypatch.setattr(sources, "_transaction", AsyncMock())
    monkeypatch.setattr(sources, "_catalog_access", AsyncMock(return_value={}))
    monkeypatch.setattr(sources, "_native_source", AsyncMock(return_value=(None, None)))
    monkeypatch.setattr(sources, "_source_pages", pages)
    connection = Mock()
    with published.fhir_catalog_published_reads(connection) if outer_context else nullcontext():
        original = published._READ_CONTEXT.get()
        inventory = sources.iter_fhir_catalog_sources(connection, read_profile())
        assert await anext(inventory) == ("page",)
        assert published._READ_CONTEXT.get() is original

        async def close():
            await inventory.aclose()

        await asyncio.create_task(close())
        assert published._READ_CONTEXT.get() is original
    assert published._READ_CONTEXT.get() is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("resource_leaf_oids", [102, 133]),
        ("database_oid", 105),
        ("dataset_sha256", "d" * 64),
        ("source_aliases", ["alias-a"]),
    ],
)
def test_scoped_descriptor_links_exact_owner_issued_source_identity(field, value):
    selected = descriptor()
    selected["native_read_profile"] = scoped_read_profile().document()
    assert sources.validate_fhir_catalog_source_descriptor(selected) == selected
    selected[field] = value
    with pytest.raises(ValueError):
        sources.validate_fhir_catalog_source_descriptor(selected)


def native_inventory():
    root_by_field = {
        "oid": 101,
        "relname": "provider_directory_dataset_resource",
        "parents": [],
        "bound": None,
        "relowner": 501,
    }
    leaf_by_field = {
        "oid": 102,
        "relname": "source_resources_one",
        "parents": [101],
        "bound": "FOR VALUES IN ('dataset-one')",
        "relowner": 501,
    }
    return {
        "family": [root_by_field, leaf_by_field],
        "header": {"oid": 103, "relname": "provider_directory_endpoint_dataset", "relowner": 501},
        "database_oid": 104,
    }


def published_row(
    *, endpoint_id="endpoint-one", dataset_id="dataset-one", aliases=("alias-a", "alias-b"), selected=None, scope=None
):
    selected_resources = list(selected or sources.RESOURCE_TYPES)
    counts_by_resource = {resource: int(resource == "Organization") for resource in selected_resources}
    proof_by_field = {
        "dataset_hash": "a" * 64,
        "resource_count": sum(counts_by_resource.values()),
        "resource_counts": counts_by_resource,
        "resource_hashes": {resource: "b" * 64 for resource in counts_by_resource},
    }
    metadata = {"source_ids": list(aliases), "selected_resources": list(reversed(selected_resources))}
    if scope is not None:
        metadata["proof_resource_scope"] = scope
    seal = _receipt(
        metadata,
        admission_kind="generic",
        proof_sha256="c" * 64,
        resource_counts=counts_by_resource,
        proof_summary=proof_by_field,
    )
    return {
        "endpoint_id": endpoint_id,
        "canonical_api_base": "https://example.test/fhir",
        "source_aliases": list(aliases),
        "alias_count": len(aliases),
        "dataset_id": dataset_id,
        "dataset_hash": "a" * 64,
        "status": "published",
        "is_current": True,
        "published_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "resource_count": sum(counts_by_resource.values()),
        "previous_dataset_id": None,
        "content_proof_admission_version": 1,
        "content_proof_admission_kind": "generic",
        "content_proof_admission_sha256": "c" * 64,
        "content_proof_resource_types": list(seal.resource_types),
        "publication_metadata_sha256": seal.metadata_sha256,
        "completion_proof_required_version": None,
        "completion_proof_sha256": None,
        "physical_schema": "source_payload",
        "current_count": 1,
        "metadata_json": json.dumps(seal.metadata_summary),
        "completion_json": None,
        "metadata_bytes": len(json.dumps(seal.metadata_summary)),
        "completion_bytes": 0,
        "row_bytes": 8192,
    }


def descriptor():
    disposition = sources._disposition(published_row(), read_profile(), native_inventory(), None, sources.DEFAULT_BYTES)
    assert disposition.status == "accepted"
    return disposition.descriptor


def test_closed_descriptor_binds_whole_alias_set_and_exact_existing_admission():
    row = published_row()
    disposition = sources._disposition(row, read_profile(), native_inventory(), None, sources.DEFAULT_BYTES)
    accepted = disposition.descriptor
    assert accepted["coordinates"] == {
        "source_system": "fhir",
        "source_id": "alias-a",
        "dataset_schema": "source_payload",
        "dataset_id": "dataset-one",
        "producer_id": "registry-fhir-catalog-v1",
        "edition_id": "dataset-one",
    }
    assert accepted["source_aliases"] == ["alias-a", "alias-b"] and accepted["complete"] is True
    assert accepted["resource_table_oid"] == 101 and accepted["resource_leaf_oids"] == [102]
    assert accepted["accepted_receipt_sha256"] == row["publication_metadata_sha256"]
    assert disposition.accepted_at.tzinfo is timezone.utc
    assert sources.validate_fhir_catalog_source_descriptor(accepted) == accepted
    later_by_field = {**row, "published_at": datetime(2026, 2, 1)}
    assert (
        sources._disposition(later_by_field, read_profile(), native_inventory(), None, sources.DEFAULT_BYTES).descriptor
        == accepted
    )
    assert len(sources.fhir_catalog_source_generation(accepted)) == 64


@pytest.mark.parametrize(
    "mutation",
    ["unknown-field", "wrong-producer", "fake-receipt", "unsorted-aliases", "missing-native", "wrong-completeness"],
)
def test_descriptor_shape_is_closed_but_never_source_authority(mutation):
    selected = descriptor()
    if mutation == "unknown-field":
        selected["capture_sha256"] = "d" * 64
    if mutation == "wrong-producer":
        selected["coordinates"]["producer_id"] = "registry-retained-normalized-fhir-v1"
    if mutation == "fake-receipt":
        selected["accepted_receipt_sha256"] = "f" * 64
    if mutation == "unsorted-aliases":
        selected["source_aliases"].reverse()
    if mutation == "missing-native":
        selected["resource_leaf_oids"] = []
    if mutation == "wrong-completeness":
        selected["complete"] = False
    with pytest.raises(sources.FHIRCatalogSourceError):
        sources.validate_fhir_catalog_source_descriptor(selected)


@pytest.mark.parametrize(
    "mutation,expected",
    [
        ("no-dataset", "current_dataset_missing"),
        ("legacy", "legacy_source_not_enrolled"),
        ("not-enrolled", "native_source_profile_unsupported"),
        ("proof-bounds", "source_proof_bounds"),
        ("alias-bounds", "source_alias_bounds"),
        ("unpublished", "current_dataset_unpublished"),
        ("digest-changed", "source_admission_unavailable"),
        ("alias-not-admitted", "source_admission_unavailable"),
        ("bad-completion", "source_admission_unavailable"),
    ],
)
def test_unavailable_and_unsupported_sources_remain_explicit_dispositions(mutation, expected):
    row, native = published_row(), native_inventory()
    if mutation == "no-dataset":
        row["dataset_id"] = None
    if mutation == "legacy":
        row["physical_schema"] = "source_catalog"
    if mutation == "not-enrolled":
        native = None
    if mutation == "proof-bounds":
        row["metadata_bytes"] = sources._PROOF_LIMIT + 1
    if mutation == "alias-bounds":
        row["alias_count"] = sources.MAX_ALIASES + 1
    if mutation == "unpublished":
        row["published_at"] = None
    if mutation == "digest-changed":
        row["publication_metadata_sha256"] = "f" * 64
    if mutation == "alias-not-admitted":
        row["source_aliases"].pop()
    if mutation == "bad-completion":
        row["completion_json"] = "{}"
    disposition = sources._disposition(
        row, read_profile(), native, "native_source_profile_unsupported", sources.DEFAULT_BYTES
    )
    assert disposition.status in {"unavailable", "unsupported"} and disposition.reason == expected
    assert disposition.descriptor is None


@pytest.mark.parametrize(
    "selected,scope",
    [
        (["Organization", "InsurancePlan"], None),
        (list(sources.RESOURCE_TYPES), ["Organization", "InsurancePlan"]),
    ],
)
def test_partial_family_coverage_keeps_an_explicit_incomplete_admitted_descriptor(selected, scope):
    disposition = sources._disposition(
        published_row(selected=selected, scope=scope), read_profile(), native_inventory(), None, sources.DEFAULT_BYTES
    )
    assert disposition.status == "incomplete" and disposition.reason == "catalog_families_incomplete"
    assert disposition.descriptor["complete"] is False


def test_unrelated_source_partition_addition_does_not_change_selected_native_identity():
    native = native_inventory()
    before = sources._native_identity(native, "dataset-one")
    native["family"].append(
        {
            "oid": 202,
            "relname": "other_resources",
            "parents": [101],
            "bound": "FOR VALUES IN ('other-dataset')",
            "relowner": 501,
        }
    )
    assert sources._native_identity(native, "dataset-one") == before
    native["family"][1]["oid"] = 303
    assert sources._native_identity(native, "dataset-one") != before


def test_unaccepted_registration_alias_does_not_change_published_source_authority():
    original = descriptor()
    row = published_row()
    row["source_aliases"].insert(0, "alias-0")
    row["alias_count"] = 3
    disposition = sources._disposition(row, read_profile(), native_inventory(), None, sources.DEFAULT_BYTES)
    assert disposition.source_aliases == ("alias-0", "alias-a", "alias-b")
    assert disposition.descriptor == original
    assert disposition.descriptor["coordinates"]["source_id"] == "alias-a"


@pytest.mark.asyncio
async def test_inventory_uses_endpoint_keyset_pages_and_one_native_verification(monkeypatch):
    profile, native = read_profile(), native_inventory()
    access_by_name = {
        "source_payload.provider_directory_endpoint_dataset": {"readable": True},
        "source_catalog.provider_directory_endpoint_dataset": {"readable": True},
    }
    monkeypatch.setattr(sources, "_catalog_access", AsyncMock(return_value=access_by_name))
    verify = AsyncMock(return_value=(native, None))
    monkeypatch.setattr(sources, "_native_source", verify)
    second = published_row(endpoint_id="endpoint-two")
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(side_effect=[[published_row()], [second], []]),
    )
    pages = [page async for page in sources.iter_fhir_catalog_sources(connection, profile, page_size=1)]
    assert [page[0].endpoint_id for page in pages] == ["endpoint-one", "endpoint-two"]
    assert all(page[0].descriptor["source_aliases"] == ["alias-a", "alias-b"] for page in pages)
    assert verify.await_count == 1 and connection.fetch.await_count == 3
    assert connection.fetch.await_args_list[1].args[1] == "endpoint-one"
    query = connection.fetch.await_args_list[0].args[0]
    assert "GROUP BY endpoint_id" in query and "e.endpoint_id>$1::text" in query
    assert "registry_network" not in query and "import_run" not in query


@pytest.mark.asyncio
async def test_final_source_recheck_detects_new_head_alias_or_native_identity(monkeypatch):
    expected = descriptor()
    profile, native = read_profile(), native_inventory()
    monkeypatch.setattr(sources, "_catalog_access", AsyncMock(return_value={}))
    monkeypatch.setattr(sources, "_native_source", AsyncMock(return_value=(native, None)))
    original_page = sources._source_pages

    async def inventory_page(*arguments, **keywords):
        yield (
            sources.FHIRCatalogSourceDisposition(
                "endpoint-one",
                "dataset-one",
                ("alias-a", "alias-b"),
                2,
                "accepted",
                None,
                copy.deepcopy(expected),
                datetime(2026, 1, 1, tzinfo=timezone.utc),
            ),
        )

    monkeypatch.setattr(sources, "_source_pages", inventory_page)
    lock = AsyncMock()
    monkeypatch.setattr(source_fence, "require_fhir_catalog_source_publication_fence", lock)
    connection = Mock(is_in_transaction=Mock(return_value=True), fetchval=AsyncMock(return_value="repeatable read"))
    assert await sources.require_current_fhir_catalog_sources(connection, (expected,), lock=True) == (
        sources.fhir_catalog_source_generation(expected),
    )
    lock.assert_awaited_once_with(connection, (expected,), required=True)
    changed = copy.deepcopy(expected)
    changed["native_catalog_sha256"] = "f" * 64
    with pytest.raises(sources.FHIRCatalogSourceError, match="source_changed"):
        await sources.require_current_fhir_catalog_sources(connection, (changed,))
    monkeypatch.setattr(sources, "_source_pages", original_page)


@pytest.mark.asyncio
async def test_final_fence_requires_pre_snapshot_session_context_without_source_row_privileges():
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(),
        execute=AsyncMock(),
    )
    with pytest.raises(source_fence.FHIRCatalogSourceFenceError, match="unavailable"):
        await sources.require_current_fhir_catalog_sources(connection, (descriptor(),), lock=True)
    connection.fetch.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_name", "source_payload; SELECT 1"),
        ("runtime_roles", "source_reader"),
        ("runtime_roles", []),
        ("runtime_roles", ["source_reader", "source_reader"]),
        ("runtime_roles", ["source_owner"]),
        ("published_read_scope", False),
    ],
)
def test_read_profile_refuses_malformed_identifiers_roles_and_scope(field, value):
    document = read_profile().document()
    document[field] = value
    with pytest.raises(ValueError):
        sources.parse_fhir_catalog_source_profile(document)


@pytest.mark.parametrize(
    "path,value",
    [
        (("coordinates",), None),
        (("coordinates", "dataset_schema"), "different_payload"),
        (("coordinates", "source_id"), "alias-b"),
        (("source_uri",), None),
        (("database_oid",), True),
        (("previous_dataset_id",), " "),
        (("complete",), 1),
        (("admission",), None),
        (("admission", "version"), True),
        (("admission", "resource_count"), -1),
        (("admission", "resource_counts"), {"Organization": 1}),
        (("admission", "resource_counts", "Organization"), -1),
        (("admission", "selected_resources"), ["Organization"]),
        (("admission", "proof_resource_scope"), ["UnselectedFamily"]),
        (("admission", "completion_sha256"), "d" * 64),
        (("admission", "completion_required_version"), 2),
    ],
)
def test_source_descriptor_refuses_incoherent_receipt_coordinates_and_counts(path, value):
    selected = descriptor()
    target = selected
    for field in path[:-1]:
        target = target[field]
    target[path[-1]] = value
    with pytest.raises(ValueError):
        sources.validate_fhir_catalog_source_descriptor(selected)


def test_source_descriptor_preserves_a_complete_version_three_completion_pin():
    selected = descriptor()
    selected["previous_dataset_id"] = "previous-dataset"
    selected["admission"].update(completion_required_version=3, completion_sha256="d" * 64)
    assert sources.validate_fhir_catalog_source_descriptor(selected) == selected
    selected["admission"]["completion_sha256"] = "invalid-digest"
    with pytest.raises(ValueError):
        sources.validate_fhir_catalog_source_descriptor(selected)


@pytest.mark.asyncio
@pytest.mark.parametrize("active,isolation", [(False, "repeatable read"), (True, "read committed")])
async def test_inventory_refuses_a_missing_or_unpinned_transaction_before_catalog_access(active, isolation):
    connection = Mock(
        is_in_transaction=Mock(return_value=active),
        fetchval=AsyncMock(return_value=isolation),
        fetch=AsyncMock(),
    )
    with pytest.raises(sources.FHIRCatalogSourceError, match="transaction_required"):
        async for _ in sources.iter_fhir_catalog_sources(connection, read_profile()):
            pytest.fail("inventory escaped its required snapshot")
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["native_profile", "duplicate_endpoint", "missing_endpoint"])
async def test_final_recheck_refuses_missing_native_custody_or_endpoint_evidence(monkeypatch, failure):
    expected = descriptor()
    native = None if failure == "native_profile" else native_inventory()
    monkeypatch.setattr(sources, "_catalog_access", AsyncMock(return_value={}))
    monkeypatch.setattr(sources, "_native_source", AsyncMock(return_value=(native, "native_changed")))
    monkeypatch.setattr(source_fence, "require_fhir_catalog_source_publication_fence", AsyncMock())

    async def empty_pages(*_args, **_kwargs):
        yield ()

    monkeypatch.setattr(sources, "_source_pages", empty_pages)
    connection = Mock(is_in_transaction=Mock(return_value=True), fetchval=AsyncMock(return_value="repeatable read"))
    selected = (expected, copy.deepcopy(expected)) if failure == "duplicate_endpoint" else (expected,)
    reason = {"native_profile": "native_changed", "duplicate_endpoint": "duplicate_endpoint"}.get(
        failure, "source_changed"
    )
    with pytest.raises(sources.FHIRCatalogSourceError, match=reason):
        await sources.require_current_fhir_catalog_sources(connection, selected)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "family", "missing_root", "missing_header", "owner", "acl", "readers"])
async def test_native_source_census_requires_complete_family_and_closed_header_rights(monkeypatch, failure):
    native = native_inventory()
    family_rows = [
        native["family"][0],
        {"relname": "provider_directory_insurance_network_source_binding", "oid": 130, "relowner": 501},
    ]
    header_by_field = {**native["header"], "direct_acl_closed": True, "readers_closed": True}
    verify = AsyncMock(return_value=family_rows)
    match failure:
        case "family":
            verify.side_effect = sources.GenericCandidateError("source role changed")
        case "missing_root":
            family_rows.pop()
        case "owner":
            header_by_field["relowner"] += 1
        case "acl":
            header_by_field["direct_acl_closed"] = False
        case "readers":
            header_by_field["readers_closed"] = False
    monkeypatch.setattr(sources, "verify_generic_source_read_family", verify)
    connection = Mock(
        transaction=Mock(return_value=AsyncMock()),
        fetch=AsyncMock(side_effect=[[{"oid": 601}], [] if failure == "missing_header" else [header_by_field]]),
        fetchval=AsyncMock(return_value=104),
        execute=AsyncMock(),
    )
    accepted, reason = await sources._native_source(
        connection, read_profile(), {"source_payload.provider_directory_endpoint_dataset": {"readable": True}}
    )
    if failure is not None:
        assert accepted is None and reason == "native_source_profile_unsupported"
        connection.execute.assert_not_awaited()
    else:
        assert reason is None and accepted["header"] == header_by_field and accepted["database_oid"] == 104
        assert accepted["family"] == family_rows
        connection.execute.assert_awaited_once()
    verify.assert_awaited_once_with(connection, "source_payload", "source_owner", ("source_reader",))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"profile": None},
        {"observation_contract": False},
        {"observation_contract": "unrecognized-observation"},
        {"page_size": True},
        {"page_size": 0},
        {"page_bytes": True},
        {"page_bytes": sources.MAX_BYTES + 1},
        {"after_endpoint": ""},
    ],
)
async def test_inventory_invalid_request_never_borrows_catalog_or_source_metadata(changes):
    connection = Mock(fetch=AsyncMock(), fetchval=AsyncMock(), is_in_transaction=Mock(return_value=True))
    request_by_field = {"profile": read_profile(), **changes}
    with pytest.raises(sources.FHIRCatalogSourceError):
        async for _page in sources.iter_fhir_catalog_sources(connection, **request_by_field):
            pytest.fail("invalid inventory request yielded a partial page")
    connection.fetch.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("unreadable", ["provider_directory_source", "provider_directory_api_endpoint"])
async def test_discovery_requires_both_registration_column_projections(unreadable):
    profile = read_profile()
    records = [
        {"name": schema + "." + relation, "oid": index + 1, "readable": relation != unreadable}
        for index, (schema, relation) in enumerate(
            (
                (profile.schema_name, "provider_directory_endpoint_dataset"),
                (profile.catalog_schema, "provider_directory_endpoint_dataset"),
                (profile.catalog_schema, "provider_directory_source"),
                (profile.catalog_schema, "provider_directory_api_endpoint"),
            )
        )
    ]
    connection = Mock(fetch=AsyncMock(return_value=records))
    with pytest.raises(sources.FHIRCatalogSourceError, match="catalog_unavailable"):
        await sources._catalog_access(connection, profile)
    query, references, source_name, endpoint_name = connection.fetch.await_args.args
    assert "has_column_privilege" in query and "'CREATE'" not in query
    assert source_name == "source_catalog.provider_directory_source"
    assert endpoint_name == "source_catalog.provider_directory_api_endpoint"
    assert len(json.loads(references)) == 4


def test_shared_catalog_schema_scans_one_header_and_never_unreadable_header():
    profile = sources.FHIRCatalogSourceReadProfile("source_payload", "source_owner", ("source_reader",))
    header = "source_payload.provider_directory_endpoint_dataset"
    query = sources._page_query(profile, {header: {"readable": True}})
    assert query.count('FROM "source_payload".provider_directory_endpoint_dataset') == 1
    assert " UNION ALL " not in query
    with pytest.raises(sources.FHIRCatalogSourceError, match="headers_unavailable"):
        sources._page_query(profile, {header: {"readable": False}})


@pytest.mark.parametrize(
    "damage",
    [
        "invalid_json",
        "missing_summary",
        "open_summary",
        "dataset_hash",
        "empty_counts",
        "count_keys",
        "boolean_count",
        "negative_count",
        "total_count",
    ],
)
def test_malformed_published_proof_is_explicitly_unsupported_without_poisoning_valid_source(damage):
    row = published_row()
    metadata = json.loads(row["metadata_json"])
    summary = metadata[ADMISSION_GENERIC_PROOF_SUMMARY_KEY]
    if damage == "missing_summary":
        metadata.pop(ADMISSION_GENERIC_PROOF_SUMMARY_KEY)
    elif damage == "open_summary":
        summary["unrecognized"] = True
    elif damage == "dataset_hash":
        summary["dataset_hash"] = "f" * 64
    else:
        _damage_source_counts(summary, damage)
    row["metadata_json"] = "{" if damage == "invalid_json" else json.dumps(metadata)
    disposition = sources._disposition(row, read_profile(), native_inventory(), None, sources.DEFAULT_BYTES)
    assert disposition.status == "unsupported" and disposition.reason == "source_admission_unavailable"
    assert disposition.descriptor is None and disposition.accepted_at is None
    assert descriptor()["coordinates"]["dataset_id"] == "dataset-one"


def _damage_source_counts(summary, damage):
    counts = summary["resource_counts"]
    if damage == "empty_counts":
        counts.clear()
    elif damage == "count_keys":
        summary["resource_hashes"].pop("Organization")
    elif damage in {"boolean_count", "negative_count", "total_count"}:
        counts["Organization"] = {"boolean_count": True, "negative_count": -1, "total_count": 2}[damage]


@pytest.mark.parametrize("field,value", [("source_ids", None), ("source_ids", [None]), ("selected_resources", None)])
def test_closed_admission_digest_cannot_replace_alias_or_resource_scope_validation(field, value):
    row = published_row()
    metadata = json.loads(row["metadata_json"])
    proof = metadata.pop(ADMISSION_GENERIC_PROOF_SUMMARY_KEY)
    metadata[field] = value
    seal = _receipt(
        metadata,
        admission_kind="generic",
        proof_sha256=row["content_proof_admission_sha256"],
        resource_counts=proof["resource_counts"],
        proof_summary=proof,
    )
    row.update(metadata_json=json.dumps(seal.metadata_summary), publication_metadata_sha256=seal.metadata_sha256)
    disposition = sources._disposition(row, read_profile(), native_inventory(), None, sources.DEFAULT_BYTES)
    assert disposition.status == "unsupported" and disposition.descriptor is None
    assert disposition.reason == "source_admission_unavailable"
