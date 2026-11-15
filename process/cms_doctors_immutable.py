# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named native models and ordinary generation CAS for the fixed Doctors family."""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from sqlalchemy import MetaData, text
from sqlalchemy.dialects import postgresql

from db.plan_release_storage import storage_identifier, storage_relation
from process import reference_family_result_generation as generation


@dataclass(frozen=True)
class _ProtectedReferencePublication:
    importer_id: str
    schema_name: str
    owner_role: str
    role_policy: object = None


_PROTECTED_PUBLICATION = ContextVar("protected_reference_publication", default=None)


def _models():
    from db.models import CMSDoctorEducation, CMSDoctorGroupSite, DoctorClinicianAddress

    return (DoctorClinicianAddress, CMSDoctorEducation, CMSDoctorGroupSite)


def _require(condition, code):
    if not condition:
        raise RuntimeError("cms_doctors_immutable_" + code)


@asynccontextmanager
async def protected_reference_family_publication(
    session, *, importer_id, schema_name, expected_owner_oid=None, roles=None
):
    """Keep the existing atomic cutover scope, using configured names for administrative DCL."""
    from process.provider_directory_generic_source_policy import _PUBLISHER, _require_active_source_session
    from process.reference_family_archive import model_family_roles

    _require_active_source_session(session)
    _require(
        importer_id in {"geo", "cms-doctors"} and _PROTECTED_PUBLICATION.get() is None and _PUBLISHER.get() is None,
        "protected_scope_invalid",
    )
    schema = generation._schema_name(schema_name)
    owner, _readers = model_family_roles(roles)
    token = _PROTECTED_PUBLICATION.set(_ProtectedReferencePublication(importer_id, schema, owner, roles))
    try:
        async with session.begin_nested():
            yield
            await _protected_reference_writer(session, importer_id, schema)
    finally:
        _PROTECTED_PUBLICATION.reset(token)


async def _protected_reference_writer(session, importer_id, schema_name=None):
    """Retain declared cutover scope while PostgreSQL authorizes the native statements."""
    publication = _PROTECTED_PUBLICATION.get()
    if publication is None:
        return None
    from process.provider_directory_generic_source_policy import _require_active_source_session

    _require_active_source_session(session)
    _require(
        publication.importer_id == importer_id and (schema_name is None or publication.schema_name == schema_name),
        "protected_scope_changed",
    )
    return publication


async def _archive_writer(session, importer_id, schema_name=None):
    publication = await _protected_reference_writer(session, importer_id, schema_name)
    return await _writer(session) if publication is None else publication.owner_role


async def _owner(session, schema, *, native_policy=None):
    """Return declared administrative role names without deriving ownership from the catalog."""
    from process.provider_directory_generic_source_policy import _PUBLISHER, GenericSourcePolicy, bound_publisher
    from process.reference_family_archive import model_family_roles

    generation._schema_name(schema)
    policy = native_policy
    if _PUBLISHER.get() is not None:
        current = bound_publisher().policy
        _require(policy is None or policy == current, "native_policy_changed")
        policy = current
    if policy is not None:
        _require(type(policy) is GenericSourcePolicy and policy.schema == schema, "native_policy_changed")
        return model_family_roles(policy)[0]
    publication = _PROTECTED_PUBLICATION.get()
    return model_family_roles(None if publication is None else publication.role_policy)[0]


async def _storage_owner(session, importer, schema, pairs, *, native_policy=None):
    """Validate the named family without using roles as published read authority."""
    from process.provider_directory_generic_source_policy import _PUBLISHER, bound_publisher

    generation._schema_name(schema)
    names = tuple(name for name, _old_address in pairs)
    _require(bool(names) and len(set(names)) == len(names), "family_invalid")
    for name in names:
        generation._schema_name(name)
    if native_policy is not None or _PUBLISHER.get() is not None:
        policy = bound_publisher().policy if _PUBLISHER.get() is not None else native_policy
        return await _owner(session, policy.schema, native_policy=native_policy)
    # Stage and retained heaps live outside the cutover's serving schema, under the same importer scope.
    publication = await _protected_reference_writer(session, importer)
    return None if publication is None else publication.owner_role


async def _create_protected_predecessor_schema(session, importer, schema):
    publication = await _protected_reference_writer(session, importer)
    if publication is None:
        return None
    await session.execute(
        text(f"CREATE SCHEMA {storage_identifier(schema)} AUTHORIZATION {storage_identifier(publication.owner_role)}")
    )
    return publication.owner_role


async def _writer(session):
    from process.provider_directory_generic_source_policy import _require_active_source_session, bound_publisher

    _require_active_source_session(session)
    policy = bound_publisher().policy
    storage_identifier(policy.owner_role)
    return policy.owner_role


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
            "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass($1)",
            relation,
        )
        _require(type(oid) is int and oid > 0, "stage_owner_changed")
        await _shape(session, model, oid, indexed=False)
        from process.mrf_address_publication import require_native_read_catalog

        await require_native_read_catalog(session, (oid,))
        return stage


async def create_doctors_archive_schema(session, schema, protected_owner=None):
    """Use original Publisher CREATE authority, retaining the physical SOURCE owner."""
    from uuid import UUID

    from process.provider_directory_generic_source_policy import _PUBLISHER, source_publisher_body
    from process.reference_family_archive import _STAGE_PREFIX, reference_family_stage_schema

    if protected_owner is not None or _PUBLISHER.get() is None:
        return None
    _require(
        schema == reference_family_stage_schema(UUID(schema.removeprefix(_STAGE_PREFIX))), "stage_identity_invalid"
    )
    owner = await _writer(session)
    async with source_publisher_body(session) as publisher:
        await session.execute(
            text(
                f"CREATE SCHEMA {storage_identifier(schema)} "
                f"AUTHORIZATION {storage_identifier(publisher.policy.owner_role)}"
            )
        )
    return owner


async def create_model_family_export_clone(session, spec, schema):
    """Keep export clones protected separately from physical SOURCE publication."""
    from uuid import UUID

    from process import reference_family_archive as archive

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


async def create_model_family_predecessor_schema(session, importer, schema):
    """Preserve native creation while Doctors use their original Publisher's authority."""
    from process.provider_directory_generic_source_policy import source_publisher_body
    from process.reference_family_archive import _quoted

    if importer != "cms-doctors":
        await session.execute(text(f"CREATE SCHEMA {_quoted(schema)}"))
        return
    if await _create_protected_predecessor_schema(session, importer, schema):
        return
    async with source_publisher_body(session) as publisher:
        await session.execute(
            text(f"CREATE SCHEMA {_quoted(schema)} AUTHORIZATION {_quoted(publisher.policy.owner_role)}")
        )


async def model_family_publisher_owner(session, importer):
    """Keep native SOURCE publication distinct from retained clone authority."""
    from process.provider_directory_generic_source_policy import _PUBLISHER
    from process.reference_family_archive import model_family_roles

    if importer == "cms-doctors" and _PUBLISHER.get() is not None:
        return await _writer(session)
    return model_family_roles()[0]


async def _freeze_named_model_relations(session, schema, names, roles=None):
    from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
    from process.provider_directory_projection_stage import _copy_driver
    from process.reference_family_archive import model_family_roles

    owner, readers = model_family_roles(roles)
    await freeze_fhir_source_snapshot(
        await _copy_driver(session),
        schema,
        namespace_owner=owner,
        reader_roles=readers,
        application_roles=(),
        relation_names=tuple(names),
        freeze_schema=False,
    )


async def seal_model_family_relation(session, ownership, oid=None, owner=None):
    """Close the complete declared model family, ignoring old physical compatibility arguments."""
    from process.provider_directory_generic_source_policy import _PUBLISHER, bound_publisher

    names = getattr(ownership, "relation_names", None)
    if names is None:
        names = tuple(name for name, _old_address in ownership.relation_oids)
    policy = bound_publisher().policy if _PUBLISHER.get() is not None else None
    protected = _PROTECTED_PUBLICATION.get()
    if policy is None and protected is not None:
        policy = protected.role_policy
    await _freeze_named_model_relations(session, ownership.schema_name, names, policy)


async def seal_doctors_stages(database, schema, import_date):
    """Close finished private heaps with configured SELECT; leave shared controls unchanged."""
    from process.ext.utils import make_class
    from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session

    publisher, session = bound_publisher(), source_copy_session()
    from process.provider_directory_generic_source_policy import _require_active_source_session

    _require_active_source_session(session)
    _require(publisher.policy.schema == schema, "schema_changed")
    names = tuple(str(make_class(model, import_date, schema_override=schema).__table__.name) for model in _models())
    await _freeze_named_model_relations(session, schema, names, publisher.policy)
    return await require_doctors_storage(
        session, schema_name=schema, relation_names=names, native_policy=publisher.policy
    )


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
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    _require(type(native_policy) is GenericSourcePolicy and native_policy.schema == schema, "native_policy_changed")
    return (native_policy.owner_role, native_policy.publisher_role)


async def require_doctors_storage(
    session, *, schema_name, relation_pairs=None, relation_names=None, legacy=False, native_policy=None
):
    """Authenticate all three native heaps; callers cannot assert immutability."""
    from process.mrf_address_publication import require_native_read_catalog

    schema = generation._schema_name(schema_name)
    names = generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]
    requested = (
        tuple(relation_names)
        if relation_names is not None
        else (tuple(name for name, _old in relation_pairs) if relation_pairs is not None else names)
    )
    _require(
        session.in_transaction()
        and len(requested) == 3
        and len(set(requested)) == 3
        and not (relation_pairs is not None and relation_names is not None),
        "family_invalid",
    )
    from process.reference_family_archive import _relation_oid

    pairs = tuple([(generation._schema_name(name), await _relation_oid(session, schema, name)) for name in requested])
    _require(all(address is not None for _name, address in pairs), "family_invalid")
    # Validates the declared names and active publication scope; the catalog owner is not compared.
    await _storage_owner(session, "cms-doctors", schema, pairs, native_policy=native_policy)
    from process.provider_directory_generic_source_policy import _PUBLISHER

    if native_policy is None and _PUBLISHER.get() is None and _PROTECTED_PUBLICATION.get() is None:
        await _require_read_pointer(session, schema, pairs)
    hooks = await _named_hook_count(session, schema, requested)
    is_legacy_group = False
    if hooks and legacy:
        _require(requested == names, "legacy_hooks_changed")
        await _legacy_hooks(session, schema, requested)
        authority = await generation.read_reference_family_result_generation_authority(
            session,
            importer_id="cms-doctors",
            schema_name=schema,
        )
        is_legacy_group = authority.serving_generation is not None and authority.relations == tuple(
            (schema, name) for name, _ in pairs
        )
    for model, (name, _address) in zip(_models(), pairs, strict=True):
        await _heap_custody(session, model, schema, name, is_legacy_group=is_legacy_group)
    if not (hooks and legacy):
        _require(hooks == 0, "hooks_changed")
        await require_native_read_catalog(session, tuple(address for _name, address in pairs))
    return pairs


_NAMED_HOOKS_SQL = (
    "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgrelid=ANY(ARRAY(SELECT "
    "to_regclass(format('%I.%I',CAST(:schema AS text),name)) FROM unnest(CAST(:names AS text[])) name))"
)


async def _named_hook_count(session, schema, names):
    """Count user triggers on the declared qualified names."""
    return await session.scalar(text(_NAMED_HOOKS_SQL), {"schema": schema, "names": list(names)})


def _authority_schema():
    """The configured application schema holds every family's serving authority."""
    import os

    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    _require(not (runtime and legacy and runtime != legacy), "schema_configuration_changed")
    return generation._schema_name(runtime or legacy or "mrf")


async def _require_read_pointer(session, schema, pairs):
    names = generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]
    _require(tuple(name for name, _ in pairs) == names, "read_pointer_changed")
    # Read the serving authority, not a ledger beside the heaps being validated.
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="cms-doctors",
        schema_name=_authority_schema(),
    )
    _require(
        authority.relations is None or authority.relations == tuple((schema, name) for name, _ in pairs),
        "read_pointer_changed",
    )


async def _heap_custody(session, model, schema, name, *, is_legacy_group=False, is_geo_incumbent=False):
    """Validate declared heap, columns, keys and complete indexes under reader locks."""
    relation = storage_relation(schema, name)
    await session.execute(text(f"LOCK TABLE ONLY {relation} IN ACCESS SHARE MODE NOWAIT"))
    from process.reference_family_archive import _relation_oid

    oid = await _relation_oid(session, schema, name)
    _require(oid is not None, "storage_changed")
    safe = await session.scalar(
        text(
            "SELECT relkind='r' AND relpersistence='p' "
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
        {"relation": relation},
    )
    _require(safe is True, "storage_changed")
    await _shape(session, model, oid, indexed=True, is_legacy_group=is_legacy_group, is_geo_incumbent=is_geo_incumbent)


async def _legacy_hooks(session, schema, names):
    """Recognize only authentic old guards; new heaps always require no hooks."""
    from process import provider_directory_cms_native_inputs as inputs
    from process.reference_source_generation import _guarded_oids

    guarded = await _guarded_oids(
        session,
        schema_name=schema,
        authority_schema=schema,
        relation_names=generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"],
    )
    _require(len(set(guarded)) == 3, "legacy_hooks_changed")
    guard_rows = (
        (
            await session.execute(
                text(
                    "SELECT c.relname::text relation,t.tgname::text name,t.tgtype,type.lanname,t.tgenabled::text enabled,"
                    "p.proname::text function,p.prosrc body,p.prosecdef,p.proconfig,p.prorettype='trigger'::regtype returns_trigger,"
                    "p.pronargs=0 AND p.prokind='f' AND NOT p.proisstrict AND NOT p.proleakproof AND NOT p.proretset "
                    "AND p.provolatile='v' AND p.proparallel='u' AND t.tgnargs=0 AND t.tgattr=''::int2vector "
                    "AND octet_length(t.tgargs)=0 AND t.tgqual IS NULL AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL "
                    "AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgconstraint=0 AND t.tgconstrrelid=0 "
                    "AND n.nspname=:schema AS shape FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
                    "JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language type ON type.oid=p.prolang "
                    "JOIN pg_class c ON c.oid=t.tgrelid "
                    "WHERE t.tgrelid=ANY(ARRAY(SELECT to_regclass(format('%I.%I',CAST(:schema AS text),name)) "
                    "FROM unnest(CAST(:names AS text[])) name)) AND NOT t.tgisinternal"
                ),
                {"schema": schema, "names": list(names)},
            )
        )
        .mappings()
        .all()
    )
    contracts = _legacy_contracts(schema)
    native = _validate_legacy_hooks(guard_rows, names, contracts)
    if native:
        await inputs._require_ledger_guards(session, schema)
        matched = await session.scalar(
            text(
                f"SELECT count(*)=1 FROM {storage_relation(schema, 'cms_native_input_revision')} "
                "WHERE schema_name=:schema AND table_name='doctor_clinician_address' AND revision>=0"
            ),
            {"schema": schema},
        )
        _require(matched is True, "legacy_revision_changed")


def _validate_legacy_hooks(rows, relations, contracts):
    from process.reference_source_generation import REVISION_TRIGGER

    relations = list(relations)
    relations_by_guard = {}
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
        relations_by_guard.setdefault(row["name"], []).append(row["relation"])
    _require(set(relations_by_guard.get(REVISION_TRIGGER, ())) == set(relations), "legacy_hooks_changed")
    seal = relations_by_guard.get("cms_doctors_prepared_write_seal", ())
    _require(not seal or set(seal) == set(relations), "legacy_hooks_changed")
    native = relations_by_guard.get("cms_native_input_mutation", ())
    _require(not native or native == [relations[0]], "legacy_hooks_changed")
    return native


def _legacy_contracts(schema):
    from process import provider_directory_cms_native_inputs as inputs
    from process.reference_source_generation import REVISION_TRIGGER

    reference_body = f'''DECLARE affected_importer text;
        BEGIN
            FOR affected_importer IN
                SELECT importer_id FROM "{schema}"."reference_family_result_generation"
                WHERE source_revision_tracked AND serving_schema=TG_TABLE_SCHEMA
                  AND TG_TABLE_NAME=ANY(serving_relation_names)
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
    await _archive_writer(session, "cms-doctors", schema)
    authority = await generation.read_reference_family_result_generation_authority(
        session, importer_id="cms-doctors", schema_name=schema, lock=True
    )
    pairs = await require_doctors_storage(session, schema_name=schema, legacy=True)
    _require(
        authority.relations is None or authority.relations == tuple((schema, name) for name, _ in pairs),
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
    if local:
        await _writer(session)
    else:
        await _archive_writer(session, "cms-doctors", schema)
    pairs = await require_doctors_storage(session, schema_name=schema)
    current = await generation.read_reference_family_result_generation_authority(
        session, importer_id="cms-doctors", schema_name=schema, lock=True
    )
    _require(current.as_dict() == expected.as_dict(), "predecessor_changed")
    generation_row = await generation._first(
        session,
        text(
            f"UPDATE {storage_relation(schema, generation.TABLE_NAME)} SET "
            "local_generation=:next,origin_lineage_id=CAST(:origin AS uuid),origin_generation=:generation,"
            "published_at=" + ("clock_timestamp()" if local else "CAST(:published AS timestamptz)") + ","
            "serving_schema=:serving_schema,serving_relation_names=CAST(:names AS text[]),source_revision_tracked=FALSE "
            "WHERE importer_id='cms-doctors' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior "
            "AND serving_schema IS NOT DISTINCT FROM :previous_schema "
            "AND serving_relation_names IS NOT DISTINCT FROM CAST(:previous_names AS text[]) "
            "RETURNING importer_id,local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,serving_schema,serving_relation_names"
        ),
        **fields,
        prior=current.local_generation,
        lineage=current.local_lineage_id,
        previous_schema=None if current.relations is None else current.relations[0][0],
        previous_names=None if current.relations is None else [name for _, name in current.relations],
        serving_schema=None if fields["origin"] is None else schema,
        names=None if fields["origin"] is None else [name for name, _ in pairs],
    )
    _require(generation_row is not None, "generation_changed")
    return generation.validate_reference_family_result_generation_authority(generation_row)


async def doctors_capture_contract(session, schema, contract):
    """Choose immutable capture only from actual native storage, never a marker."""
    from process.reference_family_archive import IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT

    if contract is not None:
        return contract
    pairs = await require_doctors_storage(session, schema_name=schema, legacy=True)
    hooks = await _named_hook_count(session, schema, tuple(name for name, _address in pairs))
    return IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT if hooks == 0 else None


async def capture_doctors_serving_generation(session, schema):
    """Only sealed current storage plus real ledger origin authorizes export."""
    pairs = await require_doctors_storage(session, schema_name=schema)
    authority = await generation.read_reference_family_result_generation_authority(
        session, importer_id="cms-doctors", schema_name=schema
    )
    _require(
        authority.serving_generation is not None and authority.relations == tuple((schema, name) for name, _ in pairs),
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
