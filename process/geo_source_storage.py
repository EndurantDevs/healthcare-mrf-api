# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed Geo native custody and application-owned generation publication."""

from dataclasses import dataclass

from sqlalchemy import text

from db.models import GeoZipLookup
from db.plan_release_storage import storage_identifier, storage_relation
from process import reference_family_result_generation as generation
from process.cms_doctors_immutable import (
    _executable_subjects,
    _heap_custody,
    _legacy_contracts,
    _owner,
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
    """One locked retained heap, bound to its saved manifest and original publisher transaction."""

    publisher: object
    session: object
    transaction_identity: tuple
    source_schema: str
    relation_oid: int
    relfilenode: int
    owner_oid: int
    index_oids: tuple[int, ...]
    index_profile_sha256: str
    schema_sha256: str
    row_count: int
    source_generation: object
    manifest_sha256: str


def require(condition, code):
    """Refuse a changed fixed native contract without granting authority."""
    if not condition:
        raise RuntimeError("geo_source_" + code)


async def _legacy_hooks(session, schema, oid):
    """Admit only the original complete native revision guards before retirement."""
    from process.reference_source_generation import REVISION_TRIGGER

    hook_rows = (
        (
            await session.execute(
                text(
                    "SELECT t.tgrelid::bigint oid,t.tgname::text name,t.tgtype,l.lanname,t.tgenabled::text enabled,"
                    "p.proname::text function,p.prosrc body,p.prosecdef,p.proconfig,p.prorettype='trigger'::regtype returns_trigger,"
                    "p.pronargs=0 AND p.prokind='f' AND NOT p.proisstrict AND NOT p.proleakproof AND NOT p.proretset "
                    "AND p.provolatile='v' AND p.proparallel='u' AND t.tgnargs=0 AND t.tgattr=''::int2vector "
                    "AND octet_length(t.tgargs)=0 AND t.tgqual IS NULL AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL "
                    "AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgconstraint=0 AND t.tgconstrrelid=0 "
                    "AND n.nspname=:schema AS shape FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
                    "JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang "
                    "WHERE t.tgrelid=:oid AND NOT t.tgisinternal"
                ),
                {"schema": schema, "oid": oid},
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
    native = _validate_legacy_hooks(hook_rows, [oid], contracts_by_name)
    if native:
        from process.provider_directory_cms_native_inputs import _require_ledger_guards

        await _require_ledger_guards(session, schema)
        require(
            await session.scalar(
                text(
                    f"SELECT count(*)=1 FROM {storage_relation(schema, 'cms_native_input_revision')} "
                    "WHERE relation_oid=:oid AND schema_name=:schema AND table_name=:name AND revision>=0"
                ),
                {"oid": oid, "schema": schema, "name": ROOT},
            ),
            "legacy_revision_changed",
        )


async def require_geo_storage(
    session, *, schema_name, relation_name=ROOT, expected_oid=None, legacy=False, native_policy=None, is_incumbent=False
):
    """Authenticate the fixed model, physical owner, ACL and complete native shape."""
    from process.entity_address_snapshot_preparation import _require_no_untrusted_mutation
    from process.mrf_address_publication import require_native_read_catalog

    schema = generation._schema_name(schema_name)
    require(relation_name == ROOT or relation_name.startswith("geo_candidate_"), "relation_scope_changed")
    relation = storage_relation(schema, relation_name)
    oid = await session.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": relation})
    require(type(oid) is int and oid > 0 and (expected_oid is None or expected_oid == oid), "relation_changed")
    owner = await _owner(session, schema, native_policy=native_policy)
    require(not is_incumbent or relation_name == ROOT, "incumbent_scope_changed")
    await _heap_custody(session, GeoZipLookup, schema, relation_name, oid, owner, is_geo_incumbent=is_incumbent)
    hooks = await session.scalar(
        text("SELECT count(*) FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal"), {"oid": oid}
    )
    if hooks:
        require(legacy and relation_name == ROOT, "hooks_changed")
        await _legacy_hooks(session, schema, oid)
    else:
        subjects = None if native_policy is None else await _executable_subjects(session, schema, native_policy)
        await require_native_read_catalog(session, (oid,), executable_roles=subjects)
    await _require_no_untrusted_mutation(session, [oid], owner)
    return oid


async def capture_geo_generation(session, schema):
    """A real ledger origin plus exact closed current heap authorizes immutable reads."""
    oid = await require_geo_storage(session, schema_name=schema, is_incumbent=True)
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="geo",
        schema_name=schema,
    )
    require(authority.serving_generation is not None and authority.relation_oids == (oid,), "generation_unavailable")
    tracked = await session.scalar(
        text(
            f"SELECT source_revision_tracked FROM {storage_relation(schema, generation.TABLE_NAME)} WHERE importer_id='geo'"
        )
    )
    require(tracked is False, "revision_contract_changed")
    return authority.serving_generation


async def _retained_geo_identity(session, schema, oid, relfilenode, owner):
    """Require this exact locked physical heap on the original writable backend and top transaction."""
    from process.provider_directory_generic_source_policy import bound_publisher
    from process.provider_directory_projection_stage import _copy_driver

    publisher = bound_publisher()
    require(await _copy_driver(session) is publisher.connection, "retained_connection_changed")
    observed = (
        await session.execute(
            text(
                "SELECT pg_backend_pid(),pg_current_xact_id()::text,"
                "(SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() "
                "AND locktype='virtualxid' AND granted AND mode='ExclusiveLock'),"
                "(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),"
                "c.relowner::bigint,pg_relation_filenode(c.oid)::bigint,"
                "EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND relation=c.oid "
                "AND locktype='relation' AND mode='AccessExclusiveLock' AND granted),"
                "(SELECT array_agg(indexrelid::bigint ORDER BY indexrelid) FROM pg_index WHERE indrelid=c.oid),"
                "current_setting('transaction_read_only')='off' "
                "FROM pg_class c WHERE c.oid=:oid AND c.oid=to_regclass(:relation)"
            ),
            {"oid": oid, "relation": storage_relation(schema, ROOT)},
        )
    ).one_or_none()
    require(
        observed is not None
        and all(observed[index] is not None for index in range(4))
        and observed[4] == owner
        and observed[5] == relfilenode
        and observed[6] is True
        and observed[8] is True,
        "retained_identity_changed",
    )
    return publisher, tuple(observed[:4]), tuple(observed[7] or ())


async def _retained_geo_catalog(session, schema, oid, owner):
    """Keep every native storage, executable-code and write-closure check on retained heaps."""
    from process import reference_family_archive as archive
    from process.entity_address_snapshot_preparation import _require_no_untrusted_mutation
    from process.mrf_address_publication import require_native_read_catalog

    await _heap_custody(session, GeoZipLookup, schema, ROOT, oid, owner, is_geo_incumbent=True)
    require(
        not await session.scalar(
            text("SELECT count(*) FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal"), {"oid": oid}
        ),
        "hooks_changed",
    )
    await require_native_read_catalog(session, (oid,))
    await _require_no_untrusted_mutation(session, [oid], owner)
    receipt = await archive._table_receipt(session, importer_id="geo", schema_name=schema, model_type=GeoZipLookup)
    profile = archive.catalog_identity._canonical_digest(
        await archive.catalog_identity._catalog_indexes(session, oid, admission=True)
    )
    return receipt, profile


async def capture_geo_retained_cut(session, *, schema_name, expected_relation_oid, expected_relfilenode, manifest):
    """Attest an already retained, locked heap before relocation; never infer custody from its name."""
    from process import reference_family_archive as archive
    from process.geo_source_archive import geo_index_schema_hashes, has_compatible_geo_indexes

    manifest = archive.validate_reference_family_manifest(manifest)
    require(manifest.importer_id == "geo" and len(manifest.tables) == 1, "retained_manifest_changed")
    require(
        type(expected_relation_oid) is int
        and 0 < expected_relation_oid < 2**32
        and type(expected_relfilenode) is int
        and expected_relfilenode > 0,
        "retained_identity_changed",
    )
    schema, owner = generation._schema_name(schema_name), await _writer(session)
    publisher, transaction, index_oids = await _retained_geo_identity(
        session, schema, expected_relation_oid, expected_relfilenode, owner
    )
    require(schema != publisher.policy.schema, "retained_location_changed")
    receipt, profile = await _retained_geo_catalog(session, schema, expected_relation_oid, owner)
    known_hashes = {digest for pair in geo_index_schema_hashes() for digest in pair}
    require(
        (receipt == manifest.tables[0] and receipt.schema_sha256 in known_hashes)
        or has_compatible_geo_indexes(manifest, (receipt,)),
        "retained_manifest_changed",
    )
    require(
        await _retained_geo_identity(session, schema, expected_relation_oid, expected_relfilenode, owner)
        == (publisher, transaction, index_oids),
        "retained_transaction_changed",
    )
    return GeoRetainedCut(
        publisher,
        session,
        transaction,
        schema,
        expected_relation_oid,
        expected_relfilenode,
        owner,
        index_oids,
        profile,
        receipt.schema_sha256,
        receipt.row_count,
        manifest.source_serving_generation,
        archive.catalog_identity._canonical_digest(manifest.as_dict()),
    )


def require_geo_retained_manifest(cut, manifest, live_pairs):
    """Bind reuse to the already validated incoming package and exact moved relation inventory."""
    from process import reference_family_archive as archive

    require(type(cut) is GeoRetainedCut, "retained_cut_required")
    manifest = archive.validate_reference_family_manifest(manifest)
    require(
        manifest.importer_id == "geo"
        and tuple(live_pairs) == ((ROOT, cut.relation_oid),)
        and archive.catalog_identity._canonical_digest(manifest.as_dict()) == cut.manifest_sha256,
        "retained_manifest_changed",
    )


async def _consume_geo_retained_cut(session, schema, source_generation, cut):
    """Recheck physical identity, indexes and provenance after the locked schema exchange."""
    from process.provider_directory_generic_source_policy import bound_publisher

    require(type(cut) is GeoRetainedCut and cut.session is session, "retained_cut_required")
    publisher = bound_publisher()
    require(publisher is cut.publisher and schema == publisher.policy.schema, "retained_publisher_changed")
    require(await _writer(session) == cut.owner_oid, "retained_owner_changed")
    origin = (
        None
        if source_generation is None
        else generation.validate_reference_family_serving_generation(source_generation)
    )
    require(origin == cut.source_generation, "retained_generation_changed")
    require(
        await _retained_geo_identity(session, schema, cut.relation_oid, cut.relfilenode, cut.owner_oid)
        == (publisher, cut.transaction_identity, cut.index_oids),
        "retained_transaction_changed",
    )
    receipt, profile = await _retained_geo_catalog(session, schema, cut.relation_oid, cut.owner_oid)
    require(
        (receipt.schema_sha256, receipt.row_count, profile)
        == (cut.schema_sha256, cut.row_count, cut.index_profile_sha256),
        "retained_catalog_changed",
    )
    return cut.relation_oid


async def publish_geo_generation(
    session, *, schema_name, expected_authority, source_generation=None, local=True, geo_retained_cut=None
):
    """CAS the existing generation row inside the original authenticated heap cut."""
    await _writer(session)
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
        previous=None if current.relation_oids is None else list(current.relation_oids),
        origin=current.local_lineage_id if local else None if origin is None else origin.origin_lineage_id,
        generation=current.local_generation + 1 if local else None if origin is None else origin.origin_generation,
        published=None if origin is None else origin.published_at,
        oids=[oid] if local or origin else None,
    )
    authority_row = await generation._first(
        session,
        text(
            f"UPDATE {storage_relation(schema_name, generation.TABLE_NAME)} SET "
            "local_generation=:next,origin_lineage_id=CAST(:origin AS uuid),origin_generation=:generation,published_at="
            + ("clock_timestamp()" if local else "CAST(:published AS timestamptz)")
            + ","
            "relation_oids=CAST(:oids AS bigint[]),source_revision_tracked=FALSE "
            "WHERE importer_id='geo' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior "
            "AND relation_oids IS NOT DISTINCT FROM CAST(:previous AS bigint[]) "
            "RETURNING importer_id,local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,relation_oids"
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
    await _writer(session)
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
    require(authority.relation_oids is None or authority.relation_oids == (oid,), "incumbent_changed")
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
