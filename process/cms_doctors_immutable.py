# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native custody and ordinary generation CAS for the fixed Doctors family."""

from contextlib import asynccontextmanager

from sqlalchemy import MetaData, text
from sqlalchemy.dialects import postgresql

from db.plan_release_storage import storage_identifier, storage_relation
from process import reference_family_result_generation as generation


def _models():
    from db.models import CMSDoctorEducation, CMSDoctorGroupSite, DoctorClinicianAddress

    return (DoctorClinicianAddress, CMSDoctorEducation, CMSDoctorGroupSite)


def _require(condition, code):
    if not condition:
        raise RuntimeError("cms_doctors_immutable_" + code)


async def _owner(session, schema, *, native_policy=None):
    """Authenticate physical SOURCE custody separately from retained clone ownership."""
    from process.plan_release_storage_custody import storage_roles
    from process.provider_directory_generic_source_policy import _PUBLISHER, GenericSourcePolicy, bound_publisher
    from process.provider_directory_projection_stage import _copy_driver

    policy = native_policy
    if _PUBLISHER.get() is not None:
        current = bound_publisher().policy
        _require(policy is None or policy == current, "native_policy_changed")
        policy = current
    if policy is not None:
        _require(type(policy) is GenericSourcePolicy and policy.schema == schema, "native_policy_changed")
        owner, _readers = await storage_roles(await _copy_driver(session), schema, **policy.role_arguments())
        return owner
    owner = await session.scalar(
        text(
            "SELECT r.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "JOIN pg_roles r ON r.oid=c.relowner WHERE n.nspname=:schema "
            "AND c.relname='provider_directory_endpoint_dataset' AND c.relkind='r' AND c.relpersistence='p' "
            "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT r.rolcanlogin AND NOT r.rolsuper "
            "AND NOT r.rolcreaterole AND NOT r.rolcreatedb AND NOT r.rolreplication AND NOT r.rolbypassrls "
            "AND NOT EXISTS(SELECT FROM pg_roles elevated WHERE "
            "(pg_has_role(r.oid,elevated.oid,'SET') OR pg_has_role(r.oid,elevated.oid,'USAGE')) "
            "AND (elevated.rolsuper OR elevated.rolcreaterole OR elevated.rolcreatedb "
            "OR elevated.rolreplication OR elevated.rolbypassrls))"
        ),
        {"schema": schema},
    )
    _require(type(owner) is int and owner > 0, "owner_unavailable")
    return owner


async def _writer(session):
    from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session

    publisher, original = bound_publisher(), source_copy_session()
    _require(original is session, "original_session_changed")
    actor = await publisher.connection.fetchrow("SELECT session_user::text s,current_user::text c")
    _require(
        actor["s"] == publisher.policy.publisher_role and actor["c"] == publisher.policy.owner_role,
        "publisher_changed",
    )
    return await _owner(session, publisher.policy.schema, native_policy=publisher.policy)


async def create_doctors_stage(database, model, import_date, schema):
    """Create only a selected private model on the already bound original session."""
    import re

    from process.ext.utils import make_class
    from process.provider_directory_generic_source_policy import source_copy_session, source_owner_transaction
    from process.provider_directory_generic_source_storage import _close_grants

    _require(
        model in _models() and type(import_date) is str and re.fullmatch(r"[A-Za-z0-9]{1,32}", import_date),
        "stage_identity_invalid",
    )
    async with source_owner_transaction(database) as publisher:
        session = source_copy_session()
        _require(schema == publisher.policy.schema, "schema_changed")
        _require(
            await _owner(session, schema)
            == await publisher.connection.fetchval(
                "SELECT oid::bigint FROM pg_roles WHERE rolname=$1", publisher.policy.owner_role
            ),
            "owner_tuple_changed",
        )
        stage = make_class(model, import_date, schema_override=schema)
        name = str(stage.__table__.name)
        relation = storage_relation(schema, name)
        _require(
            await publisher.connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation) is None, "stage_exists"
        )
        table = stage.__table__.to_metadata(MetaData())
        for column in table.columns:
            column.autoincrement = False
        connection = await session.connection()
        await connection.run_sync(lambda sync: table.create(sync, checkfirst=False))
        await _close_grants(publisher.connection, publisher.policy, name)
        oid = await publisher.connection.fetchval(
            "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass($1) AND relowner="
            "(SELECT oid FROM pg_roles WHERE rolname=$2)",
            relation,
            publisher.policy.owner_role,
        )
        _require(type(oid) is int and oid > 0, "stage_owner_changed")
        await _shape(session, model, oid, indexed=False)
        from process.entity_address_snapshot_preparation import _require_no_untrusted_mutation
        from process.mrf_address_publication import require_native_read_catalog

        await require_native_read_catalog(session, (oid,))
        await _require_no_untrusted_mutation(session, [oid], await _owner(session, schema))
        return stage


async def create_doctors_archive_schema(session, schema, protected_owner=None):
    """Use original Publisher CREATE authority, retaining the physical SOURCE owner."""
    from uuid import UUID

    from process.provider_directory_generic_source_policy import _PUBLISHER, bound_publisher, source_publisher_body
    from process.reference_family_archive import _STAGE_PREFIX, reference_family_stage_schema

    if protected_owner is not None or _PUBLISHER.get() is None:
        return None
    _require(
        schema == reference_family_stage_schema(UUID(schema.removeprefix(_STAGE_PREFIX))), "stage_identity_invalid"
    )
    owner = await _writer(session)
    _require(protected_owner is None or protected_owner == owner, "owner_tuple_changed")
    async with source_publisher_body(session) as publisher:
        await session.execute(
            text(
                f"CREATE SCHEMA {storage_identifier(schema)} "
                f"AUTHORIZATION {storage_identifier(publisher.policy.owner_role)}"
            )
        )
        _require(publisher is bound_publisher(), "publisher_changed")
    return owner


async def create_model_family_export_clone(session, spec, schema):
    """Keep export clones protected separately from physical SOURCE publication."""
    from uuid import UUID

    from process import reference_family_archive as archive

    owner = await archive.protected_publisher_owner(session) if spec.importer_id == "cms-doctors" else None
    await archive._create_model_family(
        session,
        spec,
        schema,
        create_indexes=False,
        ordinary_heaps=True,
        protected_owner_oid=owner,
    )
    ownership = await archive._capture_model_family_ownership(
        session,
        spec,
        UUID(schema.removeprefix(archive._STAGE_PREFIX)),
    )
    if owner is not None:
        await archive.seal_model_family_storage(session, ownership, owner, require_empty=True)
    return ownership


async def create_model_family_predecessor_schema(session, importer, schema):
    """Preserve native creation while Doctors use their original Publisher's authority."""
    from process.provider_directory_generic_source_policy import source_publisher_body
    from process.reference_family_archive import _quoted

    if importer != "cms-doctors":
        await session.execute(text(f"CREATE SCHEMA {_quoted(schema)}"))
        return
    async with source_publisher_body(session) as publisher:
        await session.execute(
            text(f"CREATE SCHEMA {_quoted(schema)} AUTHORIZATION {_quoted(publisher.policy.owner_role)}")
        )


async def model_family_publisher_owner(session, importer):
    """Keep native SOURCE publication distinct from retained clone authority."""
    from process.provider_directory_generic_source_policy import _PUBLISHER
    from process.reference_family_archive import protected_publisher_owner

    if importer == "cms-doctors" and _PUBLISHER.get() is not None:
        return await _writer(session)
    return await protected_publisher_owner(session)


async def seal_model_family_relation(session, ownership, oid, owner):
    """Seal SOURCE Doctors under their physical owner; retained families keep their own authority."""
    from dataclasses import replace

    from process.entity_address_snapshot_preparation import _require_no_untrusted_mutation, _seal_published_relation
    from process.mrf_address_publication import require_native_read_catalog
    from process.provider_directory_generic_source_policy import _PUBLISHER, bound_publisher
    from process.provider_directory_generic_source_storage import _close_grants

    if ownership.importer_id != "cms-doctors" or _PUBLISHER.get() is None:
        await _seal_published_relation(session, oid, owner)
        return
    _require(await _writer(session) == owner and not ownership.sequence_oids, "owner_tuple_changed")
    models_by_name = {str(model.__tablename__): model for model in _models()}
    _require(set(dict(ownership.relation_oids)) == set(models_by_name), "family_invalid")
    name = next(name for name, relation_oid in ownership.relation_oids if relation_oid == oid)
    publisher = bound_publisher()
    policy = replace(publisher.policy, schema=ownership.schema_name)
    await _close_grants(publisher.connection, policy, name, readable=True)
    await _heap_custody(session, models_by_name[name], ownership.schema_name, name, oid, owner)
    _require(
        not await session.scalar(
            text("SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal)"), {"oid": oid}
        ),
        "hooks_changed",
    )
    await _require_no_untrusted_mutation(session, [oid], owner)
    await require_native_read_catalog(session, (oid,))


async def seal_doctors_stages(database, schema, import_date):
    """Expose only SELECT after the complete private family is ready."""
    from process.ext.utils import make_class
    from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session
    from process.provider_directory_generic_source_storage import _close_grants

    publisher, session = bound_publisher(), source_copy_session()
    _require(
        database._transaction_binding().session is session and publisher.policy.schema == schema,
        "original_session_changed",
    )
    pairs = []
    for model in _models():
        name = str(make_class(model, import_date, schema_override=schema).__table__.name)
        oid = await publisher.connection.fetchval("SELECT to_regclass($1)::oid::bigint", storage_relation(schema, name))
        await _close_grants(publisher.connection, publisher.policy, name, readable=True)
        pairs.append((name, oid))
    return await require_doctors_storage(session, schema_name=schema, relation_pairs=pairs)


def _column_shape(model):
    replacements_by_type = {"varchar": "character varying", "float": "double precision"}
    columns = []
    for column in model.__table__.columns:
        native = column.type.compile(dialect=postgresql.dialect()).lower()
        for original, replacement in replacements_by_type.items():
            if native == original or native.startswith(original + "("):
                native = replacement + native[len(original) :]
        columns.append((str(column.name), native, not column.nullable))
    return sorted(columns)


async def _shape(session, model, oid, *, indexed, is_legacy_group=False, is_geo_incumbent=False):
    columns = (
        await session.execute(
            text(
                "SELECT attname::text,format_type(atttypid,atttypmod),attnotnull FROM pg_attribute "
                "WHERE attrelid=:oid AND attnum>0 AND NOT attisdropped ORDER BY attname"
            ),
            {"oid": oid},
        )
    ).all()
    _require([tuple(column_row) for column_row in columns] == _column_shape(model), "columns_changed")
    keys = tuple(str(column.name) for column in model.__table__.primary_key.columns)
    primary = await session.scalar(
        text(
            "SELECT array_agg(a.attname::text ORDER BY key.ordinal) FROM pg_constraint c "
            "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY key(number,ordinal) "
            "JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=key.number "
            "WHERE c.conrelid=:oid AND c.contype='p'"
        ),
        {"oid": oid},
    )
    _require(tuple(primary or ()) == keys, "primary_key_changed")
    if indexed:
        indexes = (
            await session.execute(
                text(
                    "SELECT i.indisunique,array_agg(a.attname::text ORDER BY key.ordinal) "
                    "FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY key(number,ordinal) "
                    "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.number "
                    "WHERE i.indrelid=:oid GROUP BY i.indexrelid,i.indisunique"
                ),
                {"oid": oid},
            )
        ).all()
        actual_keys = {(unique, tuple(names)) for unique, names in indexes}
        from db.models import GeoZipLookup

        if model is GeoZipLookup:
            _require(len(indexes) == len(actual_keys), "indexes_duplicated")
        required = {(True, keys)} | {
            (False, tuple(index["index_elements"])) for index in getattr(model, "__my_additional_indexes__", ())
        }
        _require(
            _index_shape_matches(model, required, actual_keys, is_legacy_group, is_geo_incumbent), "indexes_incomplete"
        )


def _index_shape_matches(model, required, actual, is_legacy_group, is_geo_incumbent=False):
    from db.models import GeoZipLookup

    if is_geo_incumbent and model is GeoZipLookup:
        from process.geo_source_storage import GEO_LEGACY_INDEX_PROFILES

        return actual == required or actual in GEO_LEGACY_INDEX_PROFILES
    return actual == required or (
        is_legacy_group and model is _models()[2] and actual == required - {(False, ("adrs_id",))}
    )


async def _executable_subjects(session, schema, native_policy):
    from process.plan_release_storage_custody import storage_roles
    from process.provider_directory_generic_source_policy import _PUBLISHER, GenericSourcePolicy, bound_publisher
    from process.provider_directory_projection_stage import _copy_driver

    _require(type(native_policy) is GenericSourcePolicy and native_policy.schema == schema, "native_policy_changed")
    if _PUBLISHER.get() is not None:
        _require(bound_publisher().policy == native_policy, "native_policy_changed")
    owner, _readers = await storage_roles(await _copy_driver(session), schema, **native_policy.role_arguments())
    _require(owner == await _owner(session, schema, native_policy=native_policy), "owner_tuple_changed")
    return (native_policy.owner_role, native_policy.publisher_role)


async def require_doctors_storage(session, *, schema_name, relation_pairs=None, legacy=False, native_policy=None):
    """Authenticate all three native heaps; callers cannot assert immutability."""
    from process.entity_address_snapshot_preparation import _require_no_untrusted_mutation
    from process.mrf_address_publication import require_native_read_catalog

    schema = generation._schema_name(schema_name)
    executable_roles = None if native_policy is None else await _executable_subjects(session, schema, native_policy)
    names = generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]
    pairs = (
        tuple(relation_pairs)
        if relation_pairs is not None
        else tuple(
            zip(
                names,
                await generation.current_reference_family_relation_oids(
                    session, importer_id="cms-doctors", schema_name=schema
                ),
            )
        )
    )
    _require(
        session.in_transaction()
        and len(pairs) == 3
        and len({oid for _, oid in pairs}) == 3
        and all(type(oid) is int and 0 < oid < 2**32 for _, oid in pairs),
        "family_invalid",
    )
    owner = await _owner(session, schema, native_policy=native_policy)
    from process.provider_directory_generic_source_policy import _PUBLISHER

    if native_policy is None and _PUBLISHER.get() is None:
        await _require_read_pointer(session, schema, pairs)
    oids = [oid for _, oid in pairs]
    hooks = await session.scalar(
        text("SELECT count(*) FROM pg_trigger WHERE tgrelid=ANY(:oids) AND NOT tgisinternal"), {"oids": oids}
    )
    is_legacy_group = False
    if hooks and legacy:
        _require(tuple(name for name, _ in pairs) == names, "legacy_hooks_changed")
        await _legacy_hooks(session, schema, oids)
        authority = await generation.read_reference_family_result_generation_authority(
            session,
            importer_id="cms-doctors",
            schema_name=schema,
        )
        is_legacy_group = authority.serving_generation is not None and authority.relation_oids == tuple(oids)
    for model, (name, oid) in zip(_models(), pairs, strict=True):
        await _heap_custody(session, model, schema, name, oid, owner, is_legacy_group=is_legacy_group)
    await _require_no_untrusted_mutation(session, oids, owner)
    if not (hooks and legacy):
        _require(hooks == 0, "hooks_changed")
        if executable_roles is None:
            await require_native_read_catalog(session, tuple(oids))
        else:
            await require_native_read_catalog(session, tuple(oids), executable_roles=executable_roles)
    return pairs


async def _require_read_pointer(session, schema, pairs):
    names = generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]
    _require(tuple(name for name, _ in pairs) == names, "read_pointer_changed")
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="cms-doctors",
        schema_name=schema,
    )
    _require(
        authority.relation_oids is None or authority.relation_oids == tuple(oid for _, oid in pairs),
        "read_pointer_changed",
    )


async def _heap_custody(session, model, schema, name, oid, owner, *, is_legacy_group=False, is_geo_incumbent=False):
    """Authenticate native identity before inspecting a fixed model or its payload."""
    relation = storage_relation(schema, name)
    await session.execute(text(f"LOCK TABLE ONLY {relation} IN ACCESS SHARE MODE NOWAIT"))
    safe = await session.scalar(
        text(
            "SELECT oid=:oid AND relowner=:owner AND relkind='r' AND relpersistence='p' "
            "AND NOT relispartition AND NOT relrowsecurity AND NOT relforcerowsecurity "
            "AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) "
            "AND NOT EXISTS(SELECT FROM pg_constraint WHERE conrelid=c.oid AND "
            "(contype NOT IN ('p','n') OR NOT convalidated OR condeferrable)) "
            "AND NOT EXISTS(SELECT FROM pg_attrdef WHERE adrelid=c.oid) "
            "AND NOT EXISTS(SELECT FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid "
            "WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
            "AND (a.attgenerated<>'' OR a.attidentity<>'' OR a.attcollation<>t.typcollation)) "
            "AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid) "
            "AND NOT EXISTS(SELECT FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid "
            "JOIN pg_am am ON am.oid=idx.relam WHERE i.indrelid=c.oid AND "
            "(NOT indisvalid OR NOT indisready OR NOT indislive OR indexprs IS NOT NULL "
            "OR indpred IS NOT NULL OR indnatts<>indnkeyatts OR am.amname<>'btree' "
            "OR EXISTS(SELECT FROM unnest(i.indclass,i.indcollation,i.indkey,i.indoption) "
            "index_key(opclass_oid,collation_oid,column_number,sort_options) JOIN pg_attribute a ON a.attrelid=i.indrelid "
            "AND a.attnum=index_key.column_number JOIN pg_opclass op ON op.oid=index_key.opclass_oid "
            "WHERE index_key.sort_options<>0 OR index_key.collation_oid<>a.attcollation OR NOT op.opcdefault "
            "OR op.opcnamespace<>'pg_catalog'::regnamespace))) "
            "FROM pg_class c WHERE oid=to_regclass(:relation)"
        ),
        {"oid": oid, "owner": owner, "relation": relation},
    )
    _require(safe is True, "storage_changed")
    await _shape(session, model, oid, indexed=True, is_legacy_group=is_legacy_group, is_geo_incumbent=is_geo_incumbent)


async def _legacy_hooks(session, schema, oids):
    """Recognize only authentic old guards; new heaps always require no hooks."""
    from process import provider_directory_cms_native_inputs as inputs
    from process.reference_source_generation import _guarded_oids

    guarded = await _guarded_oids(
        session,
        schema_name=schema,
        authority_schema=schema,
        relation_names=generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"],
    )
    _require(len(guarded) == 3 and set(guarded) == set(oids), "legacy_hooks_changed")
    guard_rows = (
        (
            await session.execute(
                text(
                    "SELECT t.tgrelid::bigint oid,t.tgname::text name,t.tgtype,type.lanname,t.tgenabled::text enabled,"
                    "p.proname::text function,p.prosrc body,p.prosecdef,p.proconfig,p.prorettype='trigger'::regtype returns_trigger,"
                    "p.pronargs=0 AND p.prokind='f' AND NOT p.proisstrict AND NOT p.proleakproof AND NOT p.proretset "
                    "AND p.provolatile='v' AND p.proparallel='u' AND t.tgnargs=0 AND t.tgattr=''::int2vector "
                    "AND octet_length(t.tgargs)=0 AND t.tgqual IS NULL AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL "
                    "AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgconstraint=0 AND t.tgconstrrelid=0 "
                    "AND n.nspname=:schema AS shape FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
                    "JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language type ON type.oid=p.prolang "
                    "WHERE t.tgrelid=ANY(:oids) AND NOT t.tgisinternal"
                ),
                {"schema": schema, "oids": oids},
            )
        )
        .mappings()
        .all()
    )
    contracts = _legacy_contracts(schema)
    native = _validate_legacy_hooks(guard_rows, oids, contracts)
    if native:
        await inputs._require_ledger_guards(session, schema)
        matched = await session.scalar(
            text(
                f"SELECT count(*)=1 FROM {storage_relation(schema, 'cms_native_input_revision')} "
                "WHERE relation_oid=:oid AND schema_name=:schema AND table_name='doctor_clinician_address' AND revision>=0"
            ),
            {"oid": oids[0], "schema": schema},
        )
        _require(matched is True, "legacy_revision_changed")


def _validate_legacy_hooks(rows, oids, contracts):
    from process.reference_source_generation import REVISION_TRIGGER

    oids_by_guard = {}
    for row in rows:
        _require(row["name"] in contracts, "legacy_hooks_changed")
        kind, function, security, body = contracts[row["name"]]
        _require(
            row["shape"]
            and row["returns_trigger"]
            and row["lanname"] == "plpgsql"
            and row["tgtype"] == kind
            and row["enabled"] == "A"
            and row["function"] == function
            and row["prosecdef"] is security
            and row["proconfig"] == ["search_path=pg_catalog"]
            and " ".join(row["body"].split()) == " ".join(body.split()),
            "legacy_hooks_changed",
        )
        oids_by_guard.setdefault(row["name"], []).append(row["oid"])
    _require(set(oids_by_guard.get(REVISION_TRIGGER, ())) == set(oids), "legacy_hooks_changed")
    seal = oids_by_guard.get("cms_doctors_prepared_write_seal", ())
    _require(not seal or set(seal) == set(oids), "legacy_hooks_changed")
    native = oids_by_guard.get("cms_native_input_mutation", ())
    _require(not native or native == [oids[0]], "legacy_hooks_changed")
    return native


def _legacy_contracts(schema):
    from process import provider_directory_cms_native_inputs as inputs
    from process.reference_source_generation import REVISION_TRIGGER

    reference_body = f'''DECLARE affected_importer text;
        BEGIN
            FOR affected_importer IN
                SELECT importer_id FROM "{schema}"."reference_family_result_generation"
                WHERE source_revision_tracked AND TG_RELID::bigint = ANY(relation_oids)
                ORDER BY importer_id FOR UPDATE
            LOOP
                UPDATE "{schema}"."reference_family_result_generation"
                   SET local_generation=local_generation+1,
                       origin_lineage_id=local_lineage_id,
                       origin_generation=local_generation+1,
                       published_at=transaction_timestamp()
                 WHERE importer_id=affected_importer;
            END LOOP;
            RETURN NULL;
        END;'''
    return {
        REVISION_TRIGGER: (60, "advance_reference_source_generation", True, reference_body),
        "cms_doctors_prepared_write_seal": (
            62,
            "cms_doctors_prepared_immutable",
            False,
            "BEGIN RAISE EXCEPTION 'cms_doctors_prepared_read_only'; END;",
        ),
        "cms_native_input_mutation": (60, "cms_native_input_advance", True, inputs._advance_body(schema)),
    }


async def capture_doctors_predecessor(session, schema):
    """Hold exact incumbent authority through the original atomic family cut."""
    await _writer(session)
    authority = await generation.read_reference_family_result_generation_authority(
        session, importer_id="cms-doctors", schema_name=schema, lock=True
    )
    pairs = await require_doctors_storage(session, schema_name=schema, legacy=True)
    _require(
        authority.relation_oids is None or authority.relation_oids == tuple(oid for _, oid in pairs),
        "predecessor_changed",
    )
    return authority


async def publish_doctors_generation(session, *, schema_name, expected_authority):
    """Advance an application-owned local epoch without installing hooks."""
    current = expected_authority
    _require(current.local_generation < generation._MAX_GENERATION, "generation_exhausted")
    return await _write_generation(
        session,
        schema_name,
        current,
        dict(
            next=current.local_generation + 1,
            origin=current.local_lineage_id,
            generation=current.local_generation + 1,
            published=None,
        ),
        local=True,
    )


async def adopt_doctors_generation(session, *, schema_name, expected_authority, source_generation):
    """Preserve portable origin, including genuinely generationless legacy archives."""
    origin = (
        None
        if source_generation is None
        else generation.validate_reference_family_serving_generation(source_generation)
    )
    return await _write_generation(
        session,
        schema_name,
        expected_authority,
        dict(
            next=expected_authority.local_generation,
            origin=None if origin is None else origin.origin_lineage_id,
            generation=None if origin is None else origin.origin_generation,
            published=None if origin is None else origin.published_at,
        ),
        local=False,
    )


async def _write_generation(session, schema, expected, fields, *, local):
    await _writer(session)
    pairs = await require_doctors_storage(session, schema_name=schema)
    current = await generation.read_reference_family_result_generation_authority(
        session, importer_id="cms-doctors", schema_name=schema, lock=True
    )
    _require(current.as_dict() == expected.as_dict(), "predecessor_changed")
    row = await generation._first(
        session,
        text(
            f"UPDATE {storage_relation(schema, generation.TABLE_NAME)} SET "
            "local_generation=:next,origin_lineage_id=CAST(:origin AS uuid),origin_generation=:generation,"
            "published_at=" + ("clock_timestamp()" if local else "CAST(:published AS timestamptz)") + ","
            "relation_oids=CAST(:oids AS bigint[]),source_revision_tracked=FALSE "
            "WHERE importer_id='cms-doctors' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior "
            "AND relation_oids IS NOT DISTINCT FROM CAST(:previous_oids AS bigint[]) "
            "RETURNING importer_id,local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,relation_oids"
        ),
        **fields,
        prior=current.local_generation,
        lineage=current.local_lineage_id,
        previous_oids=None if current.relation_oids is None else list(current.relation_oids),
        oids=None if fields["origin"] is None else [oid for _, oid in pairs],
    )
    _require(row is not None, "generation_changed")
    return generation.validate_reference_family_result_generation_authority(row)


async def doctors_capture_contract(session, schema, contract):
    """Choose immutable capture only from actual native storage, never a marker."""
    from process.reference_family_archive import IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT

    if contract is not None:
        return contract
    pairs = await require_doctors_storage(session, schema_name=schema, legacy=True)
    hooks = await session.scalar(
        text("SELECT count(*) FROM pg_trigger WHERE tgrelid=ANY(:oids) AND NOT tgisinternal"),
        {"oids": [oid for _, oid in pairs]},
    )
    return IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT if hooks == 0 else None


async def capture_doctors_serving_generation(session, schema):
    """Only sealed current storage plus real ledger origin authorizes export."""
    pairs = await require_doctors_storage(session, schema_name=schema)
    authority = await generation.read_reference_family_result_generation_authority(
        session, importer_id="cms-doctors", schema_name=schema
    )
    _require(
        authority.serving_generation is not None and authority.relation_oids == tuple(oid for _, oid in pairs),
        "serving_generation_unavailable",
    )
    return authority.serving_generation


@asynccontextmanager
async def doctors_cut_savepoint(session, importer_id):
    """A caught archive failure cannot commit a partial Doctors cut."""
    if importer_id != "cms-doctors":
        yield
        return
    async with session.begin_nested():
        yield


def doctors_manifest_spec(spec, manifest_value):
    """Retain exact two-table historical payloads without granting three-table origin."""
    from db import models
    from process.reference_family_archive import ReferenceFamilySpec

    if (
        spec.importer_id == "cms-doctors"
        and isinstance(manifest_value["tables"], list)
        and len(manifest_value["tables"]) == 2
    ):
        return ReferenceFamilySpec(spec.importer_id, (models.DoctorClinicianAddress, models.CMSDoctorEducation))
    return spec
