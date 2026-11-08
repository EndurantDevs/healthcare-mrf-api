# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public observation mechanics retain existing bodies, SQL and refusal behavior."""

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_observed_source as observed
from process import registry_fhir_catalog_sources as sources
from process.plan_release_storage_catalog import CATALOG_SQL
from process.registry_fhir_catalog_observation import FHIRCatalogObservationDigests
from tests.test_registry_fhir_catalog_observation import _receipt
from tests.test_registry_fhir_catalog_sources import descriptor, published_row, read_profile, scoped_read_profile


def test_public_helpers_are_exact_existing_callables():
    for public, private in (
        ("pin_fhir_catalog_registration", "_pin_registration_catalog"),
        ("fhir_catalog_relation_families", "_relation_families"),
        ("read_fhir_catalog_observation_header", "_header"),
        ("read_fhir_catalog_observation_registration", "_registration"),
        ("collect_fhir_catalog_observation_facts", "_facts"),
        ("build_fhir_catalog_observation_descriptor", "_descriptor"),
    ):
        assert getattr(observed, public) is getattr(observed, private)
    assert sources.fhir_catalog_list_dataset_routes is sources._dataset_leaves


def test_public_physical_constants_preserve_bounded_catalog_sql():
    assert observed.FHIR_CATALOG_PHYSICAL_SQL is observed._PHYSICAL_SQL
    assert observed.FHIR_CATALOG_MAX_PHYSICAL_ROWS == observed._MAX_CATALOG_ROWS == 1000
    assert observed.FHIR_CATALOG_PHYSICAL_DEFINITION_FIELDS is observed._DEFINITION_FIELDS
    assert observed.FHIR_CATALOG_PHYSICAL_DEFINITION_FIELDS == (
        "oid",
        "nspname",
        "relname",
        "relkind",
        "relpersistence",
        "relispartition",
        "relowner",
        "relrowsecurity",
        "relforcerowsecurity",
        "bound",
        "partition_key",
        "parents",
        "columns",
        "constraints",
        "indexes",
    )
    assert [
        observed.FHIR_CATALOG_HEADER_TABLE,
        observed.FHIR_CATALOG_RESOURCE_TABLE,
        observed.FHIR_CATALOG_SOURCE_TABLE,
        observed.FHIR_CATALOG_ENDPOINT_TABLE,
    ] == [observed._HEADER, observed._RESOURCE, observed._SOURCE, observed._ENDPOINT]
    assert observed.FHIR_CATALOG_PHYSICAL_SQL == (
        "WITH physical AS MATERIALIZED (\n SELECT row_to_json(c)::text AS document,c.oid FROM ("
        + CATALOG_SQL
        + ") c LIMIT 1001\n), bounded AS (\n"
        " SELECT *,sum(octet_length(document)) OVER(ORDER BY oid) AS bytes FROM physical\n"
        ") SELECT CASE WHEN bytes<=$4 THEN document END AS document FROM bounded ORDER BY oid"
    )


def relation(oid, name, *, schema="source_payload", parents=(), kind="r", bound=None):
    return {
        "oid": oid,
        "nspname": schema,
        "relname": name,
        "parents": list(parents),
        "relkind": kind,
        "relpersistence": "p",
        "relispartition": bool(parents),
        "relowner": 10,
        "relrowsecurity": False,
        "relforcerowsecurity": False,
        "bound": bound,
    }


def physical_family():
    return [
        relation(1, observed.FHIR_CATALOG_HEADER_TABLE),
        relation(2, observed.FHIR_CATALOG_RESOURCE_TABLE, kind="p"),
        relation(3, observed.FHIR_CATALOG_SOURCE_TABLE, schema="source_catalog"),
        relation(4, observed.FHIR_CATALOG_ENDPOINT_TABLE, schema="source_catalog"),
        relation(5, "resource_leaf", parents=(2,), bound="FOR VALUES IN ('edition-one', 'edition-two')"),
    ]


def test_public_relation_family_keeps_physical_identity_and_owner_checks():
    rows = physical_family()
    roots = [(row["nspname"], row["relname"]) for row in rows if not row["parents"]]
    by_oid, by_name, families = observed.fhir_catalog_relation_families(rows, roots, read_profile(), 10)
    assert by_oid[5] is rows[-1]
    assert by_name[("source_payload", observed.FHIR_CATALOG_RESOURCE_TABLE)] is rows[1]
    assert families[observed.FHIR_CATALOG_RESOURCE_TABLE] == {2, 5}
    for field, value, reason in (
        ("relowner", 11, "physical_owner_changed"),
        ("parents", [99], "physical_identity_invalid"),
        ("relrowsecurity", True, "physical_identity_invalid"),
    ):
        changed = deepcopy(rows)
        changed[-1][field] = value
        with pytest.raises(observed.FHIRCatalogObservedSourceError, match=reason):
            observed.fhir_catalog_relation_families(changed, roots, read_profile(), 10)


@pytest.mark.parametrize("dataset", ["edition-one", "edition-two", "edition'quoted"])
def test_public_list_routes_match_direct_multi_id_bounds_and_sort_leaves(dataset):
    root = relation(2, observed.FHIR_CATALOG_RESOURCE_TABLE, kind="p")
    bound = "FOR VALUES IN ('edition-one', 'edition-two', 'edition''quoted')"
    leaves = [relation(7, "leaf_seven", parents=(2,), bound=bound), relation(5, "leaf_five", parents=(2,), bound=bound)]
    unrelated = relation(9, "other_root_child", parents=(99,), bound="DEFAULT")
    selected_root, selected = sources.fhir_catalog_list_dataset_routes({"family": [root, *leaves, unrelated]}, dataset)
    assert selected_root is root
    assert selected == [leaves[1], leaves[0]]


@pytest.mark.parametrize("bound", [None, "FOR VALUES IN ('other-edition')"])
def test_public_list_routes_refuse_no_matching_direct_leaf(bound):
    root = relation(2, observed.FHIR_CATALOG_RESOURCE_TABLE, kind="p")
    family = [root] if bound is None else [root, relation(5, "other_leaf", parents=(2,), bound=bound)]
    with pytest.raises(sources.FHIRCatalogSourceError, match="dataset_not_enrolled"):
        sources.fhir_catalog_list_dataset_routes({"family": family}, "edition-one")


@pytest.mark.parametrize("bound", [None, "DEFAULT", "FOR VALUES FROM ('a') TO ('z')", "FOR VALUES IN (1, 2)"])
def test_public_list_routes_preserve_unsupported_bound_refusal(bound):
    family_rows = [
        relation(2, observed.FHIR_CATALOG_RESOURCE_TABLE, kind="p"),
        relation(5, "leaf", parents=(2,), bound=bound),
    ]
    with pytest.raises(sources.FHIRCatalogSourceError, match="native_partition_unsupported"):
        sources.fhir_catalog_list_dataset_routes({"family": family_rows}, "edition-one")


@pytest.mark.asyncio
async def test_public_registration_pin_keeps_column_projections_and_timeout_restore():
    @asynccontextmanager
    async def transaction():
        yield

    connection = SimpleNamespace(
        transaction=transaction, fetch=AsyncMock(), fetchval=AsyncMock(return_value="2s"), execute=AsyncMock()
    )
    await observed.pin_fhir_catalog_registration(connection, read_profile())
    assert [call.args[0] for call in connection.fetch.await_args_list] == [
        'SELECT tableoid,source_id,endpoint_id FROM "source_catalog"."provider_directory_source" LIMIT 0',
        'SELECT tableoid,endpoint_id,canonical_api_base FROM "source_catalog"."provider_directory_api_endpoint" LIMIT 0',
    ]
    connection.execute.assert_awaited_once_with("SET LOCAL lock_timeout='1ms'")
    assert connection.fetchval.await_args.args == ("SELECT set_config('lock_timeout',$1,true)", "2s")


def test_public_descriptor_builder_preserves_receipt_and_serialized_fields():
    publication = _receipt()["publication"]
    header_by_field = {
        "dataset_id": "dataset-one",
        "endpoint_id": "endpoint-one",
        "dataset_hash": "a" * 64,
        "previous_dataset_id": None,
        "published_at": datetime.fromisoformat(publication["published_at"]),
        "resource_count": 0,
        **{
            key: field_value
            for key, field_value in publication.items()
            if key not in {"published_at", "claimed_resource_count"}
        },
    }
    original = descriptor()
    native_by_field = {
        key: original[key]
        for key in (
            "resource_table_oid",
            "resource_leaf_oids",
            "header_table_oid",
            "database_oid",
            "native_catalog_sha256",
        )
    }
    families = FHIRCatalogObservationDigests().document()
    actual_descriptor = observed.build_fhir_catalog_observation_descriptor(
        read_profile(),
        header_by_field,
        native_by_field,
        ("https://example.test/fhir", ["alias-a", "alias-b"]),
        families,
    )
    assert sources.validate_fhir_catalog_source_descriptor(actual_descriptor) == actual_descriptor
    assert set(actual_descriptor) == set(original)
    assert actual_descriptor["admission"]["families"] == families
    assert actual_descriptor["admission"]["publication"] == {**publication, "claimed_resource_count": 0}
    assert actual_descriptor["source_aliases"] == ["alias-a", "alias-b"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"dataset_id": None}, "current_dataset_missing"),
        ({"alias_count": sources.MAX_ALIASES + 1}, "source_alias_bounds"),
        ({"physical_schema": "other_source"}, "source_namespace_unavailable"),
        ({"current_count": 2}, "current_dataset_ambiguous"),
    ],
)
async def test_observation_inventory_keeps_unsupported_registration_explicit_without_collection(
    monkeypatch, changes, reason
):
    collect = AsyncMock(side_effect=AssertionError("unavailable inventory must not collect Source facts"))
    monkeypatch.setattr(observed, "collect_fhir_catalog_observation", collect)
    disposition = await sources._observed_disposition(object(), read_profile(), published_row() | changes)
    assert disposition.status == "unavailable" and disposition.reason == reason
    assert disposition.descriptor is None
    collect.assert_not_awaited()


@pytest.mark.asyncio
async def test_observation_collection_drift_does_not_fall_back_to_semantic_admission(monkeypatch):
    collect = AsyncMock(side_effect=ValueError("source changed during collection"))
    monkeypatch.setattr(observed, "collect_fhir_catalog_observation", collect)
    disposition = await sources._observed_disposition(object(), read_profile(), published_row())
    assert disposition.status == "unavailable" and disposition.reason == "catalog_observation_unavailable"
    assert disposition.descriptor is None
    collect.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("scoped", [False, True])
async def test_semantic_native_path_never_substitutes_for_an_unreadable_or_scoped_source(scoped):
    profile = scoped_read_profile() if scoped else read_profile()
    access_by_name = {"source_payload.provider_directory_endpoint_dataset": {"readable": False}}
    native, reason = await sources._native_source(object(), profile, access_by_name)
    assert native is None
    assert reason == ("scoped_semantic_compatibility_required" if scoped else "source_namespace_unavailable")
