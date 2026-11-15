# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed Geo physical SOURCE custody; retained clone authority stays separate."""

from contextlib import asynccontextmanager
from dataclasses import replace
from uuid import UUID

from sqlalchemy import text

from db.models import GeoZipLookup
from db.plan_release_storage import storage_identifier, storage_relation
from process.cms_doctors_immutable import (
    _archive_writer,
    _create_protected_predecessor_schema,
    _heap_custody,
    _writer,
)
from process.geo_source_storage import ROOT, replay_geo_reads, require
from process.provider_directory_generic_source_policy import _PUBLISHER, bound_publisher, source_publisher_body


def is_source_geo_active(importer):
    """Only an actual same-task physical SourcePublisher selects this body."""
    return importer == "geo" and _PUBLISHER.get() is not None and bound_publisher() is not None


async def create_geo_archive_schema(session, schema, protected_owner=None):
    """Honor explicit retained custody before choosing a physical SOURCE creator."""
    from process.reference_family_archive import _STAGE_PREFIX, reference_family_stage_schema

    if protected_owner is not None or not is_source_geo_active("geo"):
        return None
    require(schema == reference_family_stage_schema(UUID(schema.removeprefix(_STAGE_PREFIX))), "stage_scope_changed")
    owner = await _writer(session)
    async with source_publisher_body(session) as publisher:
        await session.execute(
            text(
                f"CREATE SCHEMA {storage_identifier(schema)} AUTHORIZATION {storage_identifier(publisher.policy.owner_role)}"
            )
        )
    return owner


async def create_geo_export_clone(session, spec, schema, protected_owner=None):
    """Close the explicit physical or retained destination before payload COPY."""
    from process import reference_family_archive as archive

    require(spec.importer_id == "geo" and spec.model_types == (GeoZipLookup,), "family_changed")
    await archive._create_model_family(
        session,
        spec,
        schema,
        create_indexes=False,
        ordinary_heaps=True,
    )
    ownership = await archive._capture_model_family_ownership(
        session,
        spec,
        UUID(schema.removeprefix(archive._STAGE_PREFIX)),
    )
    return ownership


async def seal_geo_model_relation(session, ownership, oid, owner):
    """Freeze declared Geo snapshot relations using the configured role policy."""
    from process.cms_doctors_immutable import seal_model_family_relation

    require(ownership.importer_id == "geo", "family_changed")
    await seal_model_family_relation(session, ownership)


async def create_geo_predecessor_schema(session, schema):
    """Retain the original Publisher's CREATE authority and physical SourceOwner."""
    from process.reference_family_archive import _PREDECESSOR_PREFIX, reference_family_predecessor_schema

    require(
        schema == reference_family_predecessor_schema(UUID(schema.removeprefix(_PREDECESSOR_PREFIX))),
        "predecessor_scope_changed",
    )
    if await _create_protected_predecessor_schema(session, "geo", schema):
        return
    await _writer(session)
    async with source_publisher_body(session) as publisher:
        await session.execute(
            text(
                f"CREATE SCHEMA {storage_identifier(schema)} AUTHORIZATION {storage_identifier(publisher.policy.owner_role)}"
            )
        )


async def preserve_geo_cut_reads(session, ownership, incumbent):
    """Replay only actual incumbent SELECT on the exact locked private successor."""
    from process.geo_source_storage import require_geo_storage

    await _archive_writer(session, "geo", incumbent.schema_name)
    require(ownership.importer_id == incumbent.importer_id == "geo", "family_changed")
    require(
        tuple(name for name, _old in incumbent.relation_oids) == (ROOT,)
        and tuple(name for name, _old in ownership.relation_oids) == (ROOT,),
        "family_changed",
    )
    await require_geo_storage(session, schema_name=incumbent.schema_name, is_incumbent=True)
    await _heap_custody(session, GeoZipLookup, ownership.schema_name, ROOT)
    from process.native_publication_read_access import preserve_publication_reads
    from process.provider_directory_projection_stage import _copy_driver

    await preserve_publication_reads(
        await _copy_driver(session),
        schema=incumbent.schema_name,
        live_table=ROOT,
        stage_schema=ownership.schema_name,
        stage_table=ROOT,
    )


def has_compatible_geo_indexes(manifest, tables):
    """Accept only exact historical Geo receipts rebuilt with the complete current indexes."""
    if manifest.importer_id != "geo" or len(manifest.tables) != 1 or len(tables) != 1:
        return False
    expected, observed = manifest.tables[0], tables[0]
    expected_fields, observed_fields = expected.as_dict(), observed.as_dict()
    expected_fields.pop("schema_sha256")
    observed_fields.pop("schema_sha256")
    return (
        expected_fields == observed_fields
        and (expected.schema_sha256, observed.schema_sha256) in geo_index_schema_hashes()
    )


def geo_index_schema_hashes():
    """Pair exact index catalogs without changing archived manifests or constraint versions."""
    from process import reference_family_archive as archive
    from process.geo_source_storage import GEO_LEGACY_INDEX_PROFILES

    columns = _geo_catalog_columns()
    indexes_by_keys = _geo_catalog_indexes(columns)
    current_keys = frozenset(indexes_by_keys)
    pairs = set()
    for constraints in archive._cms_group_constraint_orders(columns):
        catalog_by_field = {"table_name": ROOT, "columns": columns, "constraints": constraints}
        current_hash = archive.catalog_identity._canonical_digest(
            {**catalog_by_field, "indexes": sorted(indexes_by_keys.values(), key=archive._canonical_json)}
        )
        for profile in (*GEO_LEGACY_INDEX_PROFILES, current_keys):
            historical_hash = archive.catalog_identity._canonical_digest(
                {
                    **catalog_by_field,
                    "indexes": sorted((indexes_by_keys[key] for key in profile), key=archive._canonical_json),
                }
            )
            pairs.add((historical_hash, current_hash))
    return pairs


def _geo_catalog_columns():
    """Describe the fixed native model with PostgreSQL's catalog type spellings."""
    from process.cms_doctors_immutable import _column_shape

    shapes_by_name = {name: (kind, required) for name, kind, required in _column_shape(GeoZipLookup)}
    columns = []
    for position, column in enumerate(GeoZipLookup.__table__.columns, 1):
        kind, required = shapes_by_name[column.name]
        is_collatable = kind.startswith("character varying")
        columns.append(
            {
                "attnum": position,
                "attname": column.name,
                "type": kind,
                "attnotnull": required,
                "attgenerated": "",
                "attidentity": "",
                "collation_schema": "pg_catalog" if is_collatable else None,
                "collation_name": "default" if is_collatable else None,
                "default_expression": None,
            }
        )
    return columns


def _geo_catalog_indexes(columns):
    """Describe only declared btree keys, including their exact default operator classes."""
    columns_by_name = {column["attname"]: column for column in columns}
    indexes_by_keys = {}
    for is_primary, names in (
        (True, ("zip_code",)),
        *((False, tuple(index["index_elements"])) for index in GeoZipLookup.__my_additional_indexes__),
    ):
        key_attributes = []
        for position, name in enumerate(names):
            column = columns_by_name[name]
            key_attributes.append(
                {
                    "position": position,
                    "attribute_number": column["attnum"],
                    "collation_schema": column["collation_schema"],
                    "collation_name": column["collation_name"],
                    "opclass_schema": "pg_catalog",
                    "opclass_name": {"character varying": "text_ops", "double precision": "float8_ops"}[
                        column["type"].split("(", 1)[0]
                    ],
                }
            )
        indexes_by_keys[(is_primary, names)] = {
            "indisunique": is_primary,
            "indisprimary": is_primary,
            "indimmediate": True,
            "indisvalid": True,
            "indnkeyatts": len(names),
            "indnatts": len(names),
            "method": "btree",
            "predicate": None,
            "expressions": None,
            "keys": " ".join(str(column["attribute_number"]) for column in key_attributes),
            "options": " ".join("0" for _ in names),
            "key_attributes": key_attributes,
        }
    return indexes_by_keys


@asynccontextmanager
async def geo_cut_savepoint(session, importer):
    """A caught physical Geo cut failure cannot commit its partial rotation."""
    if importer == "geo":
        await _archive_writer(session, "geo")
        async with session.begin_nested():
            yield
    else:
        from process.cms_doctors_immutable import doctors_cut_savepoint

        async with doctors_cut_savepoint(session, importer):
            yield


async def create_model_family_archive_schema(session, importer, schema, protected_owner):
    """Keep existing families unchanged and honor explicit retained destination first."""
    if importer == "geo":
        return await create_geo_archive_schema(session, schema, protected_owner)
    if importer == "cms-doctors":
        from process.cms_doctors_immutable import create_doctors_archive_schema

        return await create_doctors_archive_schema(session, schema, protected_owner)
    return False


async def create_model_family_export_clone(session, spec, schema):
    """Dispatch only physical Geo while retained and other families keep their API."""
    if spec.importer_id == "geo":
        return await create_geo_export_clone(session, spec, schema)
    from process.cms_doctors_immutable import create_model_family_export_clone as original

    return await original(session, spec, schema)


async def create_model_family_predecessor_schema(session, importer, schema):
    """Physical Geo requires its genuine writer; other families retain their creator."""
    if importer == "geo":
        return await create_geo_predecessor_schema(session, schema)
    from process.cms_doctors_immutable import create_model_family_predecessor_schema as original

    return await original(session, importer, schema)


async def model_family_publisher_owner(session, importer):
    """Keep the physical SourceOwner distinct from existing protected RetOwner."""
    if is_source_geo_active(importer):
        return await _writer(session)
    from process.cms_doctors_immutable import model_family_publisher_owner as original

    return await original(session, importer)


async def seal_model_family_relation(session, ownership, oid, owner):
    """Only an original physical Geo publisher chooses SourceOwner stage custody."""
    if is_source_geo_active(ownership.importer_id):
        return await seal_geo_model_relation(session, ownership, oid, owner)
    from process.cms_doctors_immutable import seal_model_family_relation as original

    return await original(session, ownership, oid, owner)
