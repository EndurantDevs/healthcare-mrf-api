# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed Geo data validation and application-owned generation publication."""

from dataclasses import dataclass

from sqlalchemy import text

from db.models import GeoZipLookup
from db.plan_release_storage import storage_identifier, storage_relation
from process import reference_family_result_generation as generation
from process.cms_doctors_immutable import (
    _archive_writer,
    _executable_subjects,
    _heap_custody,
    _legacy_contracts,
    _protected_reference_writer,
    _ProtectedReferencePublication,
    _storage_owner,
    _validate_legacy_hooks,
    _writer,
)

ROOT = "geo_zip_lookup"
GEO_LEGACY_INDEX_PROFILES = (
    frozenset({(True, ("zip_code",)), (False, ("latitude", "longitude", "zip_code"))}),
    frozenset(
        {
            (True, ("zip_code",)),
            (False, ("state",)),
            (False, ("city_lower", "state")),
            (False, ("city_lower",)),
            (False, ("latitude", "longitude")),
        }
    ),
)


@dataclass(frozen=True)
class GeoRetainedCut:
    """Content and declared location held through one native atomic cutover."""

    source_schema: str
    index_profile_sha256: str
    schema_sha256: str
    row_count: int
    row_sha256: str
    source_generation: object
    manifest_sha256: str


def require(condition, code):
    """Refuse a changed fixed native contract without granting authority."""
    if not condition:
        raise RuntimeError("geo_source_" + code)


async def _legacy_hooks(session, schema, relation_name):
    """Admit only the original complete native revision guards before retirement."""
    from process.reference_source_generation import REVISION_TRIGGER

    hook_rows = (
        (
            await session.execute(
                text(
                    "SELECT c.relname::text relation,t.tgname::text name,t.tgtype,l.lanname,t.tgenabled::text enabled,"
                    "p.proname::text function,p.prosrc body,p.prosecdef,p.proconfig,p.prorettype='trigger'::regtype returns_trigger,"
                    "p.pronargs=0 AND p.prokind='f' AND NOT p.proisstrict AND NOT p.proleakproof AND NOT p.proretset "
                    "AND p.provolatile='v' AND p.proparallel='u' AND t.tgnargs=0 AND t.tgattr=''::int2vector "
                    "AND octet_length(t.tgargs)=0 AND t.tgqual IS NULL AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL "
                    "AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgconstraint=0 AND t.tgconstrrelid=0 "
                    "AND n.nspname=:schema AS shape FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
                    "JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang "
                    "JOIN pg_class c ON c.oid=t.tgrelid WHERE t.tgrelid=to_regclass(:relation) AND NOT t.tgisinternal"
                ),
                {"schema": schema, "relation": storage_relation(schema, relation_name)},
            )
        )
        .mappings()
        .all()
    )
    contracts_by_name = {
        name: contract
        for name, contract in _legacy_contracts(schema).items()
        if name in (REVISION_TRIGGER, "cms_native_input_mutation")
    }
    native = _validate_legacy_hooks(hook_rows, [relation_name], contracts_by_name)
    if native:
        from process.provider_directory_cms_native_inputs import _require_ledger_guards

        await _require_ledger_guards(session, schema)
        require(
            await session.scalar(
                text(
                    f"SELECT count(*)=1 FROM {storage_relation(schema, 'cms_native_input_revision')} "
                    "WHERE schema_name=:schema AND table_name=:name AND revision>=0"
                ),
                {"schema": schema, "name": ROOT},
            ),
            "legacy_revision_changed",
        )


async def require_geo_storage(
    session, *, schema_name, relation_name=ROOT, expected_oid=None, legacy=False, native_policy=None, is_incumbent=False
):
    """Validate the declared fixed model and complete native storage shape."""
    from process.mrf_address_publication import require_native_read_catalog

    schema = generation._schema_name(schema_name)
    require(relation_name == ROOT or relation_name.startswith("geo_candidate_"), "relation_scope_changed")
    relation = storage_relation(schema, relation_name)
    oid = await session.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": relation})
    require(type(oid) is int and oid > 0, "relation_changed")
    await _storage_owner(session, "geo", schema, ((relation_name, oid),), native_policy=native_policy)
    require(not is_incumbent or relation_name == ROOT, "incumbent_scope_changed")
    await _heap_custody(session, GeoZipLookup, schema, relation_name, is_geo_incumbent=is_incumbent)
    hooks = await session.scalar(
        text("SELECT count(*) FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal"), {"oid": oid}
    )
    if hooks:
        require(legacy and relation_name == ROOT, "hooks_changed")
        await _legacy_hooks(session, schema, relation_name)
    else:
        await require_native_read_catalog(session, (oid,))
    return oid


async def capture_geo_generation(session, schema):
    """A real ledger origin plus exact closed current heap authorizes immutable reads."""
    oid = await require_geo_storage(session, schema_name=schema, is_incumbent=True)
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="geo",
        schema_name=schema,
    )
    require(
        authority.serving_generation is not None and authority.relations == ((schema, ROOT),), "generation_unavailable"
    )
    tracked = await session.scalar(
        text(
            f"SELECT source_revision_tracked FROM {storage_relation(schema, generation.TABLE_NAME)} WHERE importer_id='geo'"
        )
    )
    require(tracked is False, "revision_contract_changed")
    return authority.serving_generation


async def _geo_archive_publisher(session):
    from process.provider_directory_generic_source_policy import bound_publisher

    protected = await _protected_reference_writer(session, "geo")
    return bound_publisher() if protected is None else protected


def _publication_schema(publisher):
    return publisher.schema_name if type(publisher) is _ProtectedReferencePublication else publisher.policy.schema


async def _retained_geo_catalog(session, schema, oid, owner):
    """Validate retained schema, indexes and the absence of executable hooks."""
    from process import reference_family_archive as archive
    from process.mrf_address_publication import require_native_read_catalog

    await _heap_custody(session, GeoZipLookup, schema, ROOT, is_geo_incumbent=True)
    require(
        not await session.scalar(
            text("SELECT count(*) FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal"), {"oid": oid}
        ),
        "hooks_changed",
    )
    await require_native_read_catalog(session, (oid,))
    receipt = await archive._table_receipt(session, importer_id="geo", schema_name=schema, model_type=GeoZipLookup)
    profile = archive.catalog_identity._canonical_digest(
        await archive.catalog_identity._catalog_indexes(session, oid, admission=True)
    )
    return receipt, profile


async def capture_geo_retained_cut(
    session, *, schema_name, expected_relation_oid=None, expected_relfilenode=None, manifest
):
    """Validate the declared retained heap and complete data without old physical coordinates."""
    from process import reference_family_archive as archive
    from process.entity_address_snapshot_receipt import _projected_row_identity
    from process.geo_source_archive import geo_index_schema_hashes, has_compatible_geo_indexes

    manifest = archive.validate_reference_family_manifest(manifest)
    require(manifest.importer_id == "geo" and len(manifest.tables) == 1, "retained_manifest_changed")
    schema = generation._schema_name(schema_name)
    await _archive_writer(session, "geo")
    publisher = await _geo_archive_publisher(session)
    require(schema != _publication_schema(publisher), "retained_location_changed")
    address = await require_geo_storage(session, schema_name=schema, is_incumbent=True)
    receipt, profile = await _retained_geo_catalog(session, schema, address, None)
    known_hashes = {digest for pair in geo_index_schema_hashes() for digest in pair}
    require(
        (receipt == manifest.tables[0] and receipt.schema_sha256 in known_hashes)
        or has_compatible_geo_indexes(manifest, (receipt,)),
        "retained_manifest_changed",
    )
    count, digest = await _projected_row_identity(session, schema, ROOT, row_json_sql="pg_catalog.to_jsonb(row_value)")
    require(count == receipt.row_count, "retained_rows_changed")
    return GeoRetainedCut(
        schema,
        profile,
        receipt.schema_sha256,
        count,
        digest,
        manifest.source_serving_generation,
        archive.catalog_identity._canonical_digest(manifest.as_dict()),
    )


def require_geo_retained_manifest(cut, manifest, live_pairs):
    """Bind the complete moved named family and the validated incoming manifest."""
    from process import reference_family_archive as archive

    require(type(cut) is GeoRetainedCut, "retained_cut_required")
    manifest = archive.validate_reference_family_manifest(manifest)
    require(
        manifest.importer_id == "geo"
        and tuple(name for name, _old in live_pairs) == (ROOT,)
        and archive.catalog_identity._canonical_digest(manifest.as_dict()) == cut.manifest_sha256,
        "retained_manifest_changed",
    )


async def _consume_geo_retained_cut(session, schema, source_generation, cut):
    """Recheck full data, index shape and origin after the caller's atomic name rotation."""
    from process.entity_address_snapshot_receipt import _projected_row_identity
    from process.provider_directory_generic_source_policy import _require_active_source_session

    require(type(cut) is GeoRetainedCut, "retained_cut_required")
    _require_active_source_session(session)
    publisher = await _geo_archive_publisher(session)
    require(schema == _publication_schema(publisher), "retained_location_changed")
    await _archive_writer(session, "geo", schema)
    origin = (
        None
        if source_generation is None
        else generation.validate_reference_family_serving_generation(source_generation)
    )
    require(origin == cut.source_generation, "retained_generation_changed")
    address = await require_geo_storage(session, schema_name=schema, is_incumbent=True)
    receipt, profile = await _retained_geo_catalog(session, schema, address, None)
    count, digest = await _projected_row_identity(session, schema, ROOT, row_json_sql="pg_catalog.to_jsonb(row_value)")
    require(
        (receipt.schema_sha256, receipt.row_count, profile, count, digest)
        == (cut.schema_sha256, cut.row_count, cut.index_profile_sha256, cut.row_count, cut.row_sha256),
        "retained_catalog_changed",
    )
    return address


async def publish_geo_generation(
    session, *, schema_name, expected_authority, source_generation=None, local=True, geo_retained_cut=None
):
    """CAS the existing generation row inside the original authenticated heap cut."""
    if local:
        await _writer(session)
    else:
        await _archive_writer(session, "geo", schema_name)
    if geo_retained_cut is None:
        oid = await require_geo_storage(session, schema_name=schema_name)
    else:
        require(local is False, "retained_local_publication_forbidden")
        oid = await _consume_geo_retained_cut(session, schema_name, source_generation, geo_retained_cut)
    current = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="geo",
        schema_name=schema_name,
        lock=True,
    )
    require(current.as_dict() == expected_authority.as_dict(), "generation_changed")
    require(current.local_generation < generation._MAX_GENERATION, "generation_exhausted")
    origin = (
        None
        if source_generation is None
        else generation.validate_reference_family_serving_generation(source_generation)
    )
    fields_by_name = dict(
        next=current.local_generation + int(local),
        lineage=current.local_lineage_id,
        prior=current.local_generation,
        previous_schema=None if current.relations is None else current.relations[0][0],
        previous_names=None if current.relations is None else [name for _schema, name in current.relations],
        origin=current.local_lineage_id if local else None if origin is None else origin.origin_lineage_id,
        generation=current.local_generation + 1 if local else None if origin is None else origin.origin_generation,
        published=None if origin is None else origin.published_at,
        serving_schema=schema_name if local or origin else None,
        serving_names=[ROOT] if local or origin else None,
    )
    authority_row = await generation._first(
        session,
        text(
            f"UPDATE {storage_relation(schema_name, generation.TABLE_NAME)} SET "
            "local_generation=:next,origin_lineage_id=CAST(:origin AS uuid),origin_generation=:generation,published_at="
            + ("clock_timestamp()" if local else "CAST(:published AS timestamptz)")
            + ","
            "serving_schema=:serving_schema,serving_relation_names=CAST(:serving_names AS text[]),source_revision_tracked=FALSE "
            "WHERE importer_id='geo' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior "
            "AND serving_schema IS NOT DISTINCT FROM :previous_schema AND serving_relation_names IS NOT DISTINCT FROM CAST(:previous_names AS text[]) "
            "RETURNING importer_id,local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,serving_schema,serving_relation_names"
        ),
        **fields_by_name,
    )
    require(authority_row is not None, "generation_changed")
    return generation.validate_reference_family_result_generation_authority(authority_row)


async def replay_geo_reads(session, oid, relation, *, exclude_owner=True):
    """Preserve only existing SELECT access, including column and grant-option scope."""
    from process.code_catalog_snapshot import _relation_access

    access = await _relation_access(session, oid)
    for grant in access.grants:
        if grant.privilege_type != "SELECT" or (exclude_owner and grant.grantee_name == access.owner_name):
            continue
        principal = "PUBLIC" if grant.grantee_name is None else storage_identifier(grant.grantee_name)
        column = "" if grant.column_name is None else f" ({storage_identifier(grant.column_name)})"
        option = " WITH GRANT OPTION" if grant.is_grantable else ""
        await session.execute(text(f"GRANT SELECT{column} ON TABLE {relation} TO {principal}{option}"))


async def capture_geo_predecessor(session, schema, pairs):
    """Authenticate an actual complete or empty incumbent before archive rotation."""
    await _archive_writer(session, "geo", schema)
    require(tuple(name for name, _oid in pairs) == (ROOT,), "family_changed")
    oid = pairs[0][1]
    if oid is not None:
        await require_geo_storage(session, schema_name=schema, expected_oid=oid, is_incumbent=True)
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="geo",
        schema_name=schema,
        lock=True,
    )
    require(authority.relations is None or authority.relations == ((schema, ROOT),), "incumbent_changed")
    return authority


async def adopt_geo_generation(session, schema, source_generation, source_revision_tracked):
    """Keep adopted origins without reinstalling the retired Geo revision hooks."""
    if source_revision_tracked:
        raise ValueError("geo_source_native_publication_cannot_install_revision_hooks")
    current = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="geo",
        schema_name=schema,
        lock=True,
    )
    return await publish_geo_generation(
        session,
        schema_name=schema,
        expected_authority=current,
        source_generation=source_generation,
        local=False,
    )
