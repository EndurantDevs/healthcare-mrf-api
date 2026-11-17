# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-qualified override identity survives new editions and explicit legacy ACA plan keys."""

import json
from uuid import UUID, uuid4

import pytest

from process.registry_binding_override_sql import binding_override_identity_sql
from process.registry_effective_network_catalog import effective_catalog_sql
from tests.effective_catalog_support import effective_catalog_db as effective_catalog_db
from tests.effective_catalog_support import manual_record

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "family,key",
    [("aca", "ACAPlan/12345CA1234567/17"), ("aca", "ACAPlan/opaque/plan/17"), ("ptg", "PTGProviderSet/source-one")],
)
@pytest.mark.parametrize("archived", [False, True])
async def test_native_override_survives_new_edition_and_binding_id(effective_catalog_db, family, key, archived):
    db = effective_catalog_db
    imported_by_field = json.loads(
        await db.connection.fetchval(f'SELECT document_json FROM "{db.catalog}".registry_fhir_catalog_binding')
    )
    imported_by_field.update(
        source_system=family,
        source_key=key,
        producer_id="registry-native-network-source-v1",
        dataset_schema="new_data",
        dataset_id="edition-two",
        edition_id="edition-two",
    )
    scope_by_field = {
        "issuer_id": "12345",
        "plan_year": 2026,
        "state": "CA",
        "plan_id": key.removeprefix("ACAPlan/").rsplit("/", 1)[0],
        "checksum_network": 17,
        "source_generation": "b" * 64,
    }
    imported_by_field["source_scope"] = scope_by_field
    await db.connection.execute(
        f'UPDATE "{db.catalog}".registry_fhir_catalog_binding SET binding_id=$1,document_json=$2::jsonb',
        UUID(int=2),
        json.dumps(imported_by_field),
    )
    await manual_record(
        db, "network", 8, {"network_id": 8, "display_name": "Manual destination", "aliases": [], "archived": False}
    )
    await manual_record(
        db,
        "network_binding",
        uuid4(),
        {
            "network_id": 8,
            "source_system": family,
            "source_id": "source-one",
            "source_key": key,
            "producer_id": "registry-native-network-source-v1",
            "dataset_schema": "old_data",
            "dataset_id": "edition-one",
            "edition_id": "edition-one",
            "archived": archived,
            "source_scope_json": scope_by_field | {"source_generation": "c" * 64},
        },
    )
    relation = await effective_catalog_sql(db.connection, db.manifest, db.control)
    native = await db.connection.fetchval(
        "SELECT record_json->>'network_id' FROM "
        + relation
        + " effective WHERE record_kind='network_binding' AND record_key=$1",
        str(UUID(int=2)),
    )
    assert native == (None if archived else "8")


@pytest.mark.parametrize(
    "key",
    [
        "17",
        "ACAPlan/12345CA1234567",
        "ACAPlan/12345CA1234567/-0",
        "ACAPlan/12345CA1234567/2147483648",
        "unqualified-plan",
    ],
)
async def test_native_identity_rejects_checksum_only_or_malformed_keys(effective_catalog_db, key):
    db = effective_catalog_db
    manual_by_field = {
        "source_system": "aca",
        "source_id": "source-one",
        "source_key": key,
        "producer_id": "registry-native-network-source-v1",
        "dataset_schema": "old",
        "dataset_id": "old",
        "edition_id": "old",
    }
    imported_by_field = {**manual_by_field, "dataset_schema": "new", "dataset_id": "new", "edition_id": "new"}
    predicate = binding_override_identity_sql("m.record_json", "b.document_json", "b.source_aliases")
    assert (
        await db.connection.fetchval(
            "SELECT "
            + predicate
            + " FROM (SELECT $1::jsonb AS record_json) m CROSS JOIN (SELECT $2::jsonb AS document_json,NULL::jsonb AS source_aliases) b",
            json.dumps(manual_by_field),
            json.dumps(imported_by_field),
        )
        is False
    )


async def legacy_aca_override_case(db, archived, mismatch):
    imported_by_field = json.loads(
        await db.connection.fetchval(f'SELECT document_json FROM "{db.catalog}".registry_fhir_catalog_binding')
    )
    scope_by_field = {
        "issuer_id": "12345",
        "plan_year": 2026,
        "state": "CA",
        "plan_id": "12345CA1234567",
        "checksum_network": 17,
    }
    imported_by_field.update(
        source_system="aca",
        source_key="ACAPlan/12345CA1234567/17",
        producer_id="registry-native-network-source-v1",
        dataset_schema="new_aca",
        dataset_id="new-edition",
        edition_id="new-edition",
        source_scope=scope_by_field | {"source_generation": "b" * 64},
    )
    await db.connection.execute(
        f'UPDATE "{db.catalog}".registry_fhir_catalog_binding SET document_json=$1::jsonb',
        json.dumps(imported_by_field),
    )
    manual_by_field = {
        **{key: imported_by_field[key] for key in ("source_system", "source_id")},
        "producer_id": "retained-plan-producer",
        "dataset_schema": "old_aca",
        "dataset_id": "old-edition",
        "edition_id": "old-edition",
        "source_key": "12345CA1234567",
        "source_scope_json": dict(scope_by_field),
        "network_id": 8,
        "archived": archived,
    }
    if mismatch in {"issuer_id", "plan_year", "state", "plan_id", "checksum_network"}:
        manual_by_field["source_scope_json"][mismatch] = {
            "issuer_id": "54321",
            "plan_year": 2027,
            "state": "NY",
            "plan_id": "12345CA7654321",
            "checksum_network": 18,
        }[mismatch]
    if mismatch == "source_id":
        manual_by_field["source_id"] = "other-source"
    if mismatch == "source_key_kind":
        manual_by_field["source_key_kind"] = "unrelated-key"
    await manual_record(
        db, "network", 8, {"network_id": 8, "display_name": "Manual target", "aliases": [], "archived": False}
    )
    await manual_record(db, "network_binding", uuid4(), manual_by_field)
    return imported_by_field


@pytest.mark.parametrize("archived", [False, True])
@pytest.mark.parametrize(
    "mismatch", [None, "issuer_id", "plan_year", "state", "plan_id", "checksum_network", "source_id", "source_key_kind"]
)
async def test_legacy_aca_override_is_exact_in_read_and_writer_mapping(effective_catalog_db, archived, mismatch):
    from process.registry_imported_catalog_bindings import _binding_sql
    from tests.effective_catalog_support import BINDING_ID, GENERATION

    db = effective_catalog_db
    imported_by_field = await legacy_aca_override_case(db, archived, mismatch)
    relation = await effective_catalog_sql(db.connection, db.manifest, db.control)
    actual = await db.connection.fetchval(
        "SELECT record_json->>'network_id' FROM "
        + relation
        + " effective WHERE record_kind='network_binding' AND record_key=$1",
        str(BINDING_ID),
    )
    expected = "7" if mismatch is not None else None if archived else "8"
    assert actual == expected
    writer_sql = _binding_sql('"' + db.control + '"', '"' + db.catalog + '"', GENERATION)
    arguments = (
        db.manifest.approved_custom_revision,
        *(
            imported_by_field[field]
            for field in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
        ),
    )
    mapped = await db.connection.fetch(
        "WITH " + writer_sql + " SELECT network_id FROM approved_network_bindings", *arguments
    )
    assert [row["network_id"] for row in mapped] == ([] if expected is None else [int(expected)])
