# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded mutation and publication fence for external native address inputs.

FHIR stages and their admission, desired vector, overlay and alias selection remain
owned by the composite preparation contract. This fence covers the additional native
reads. Registration precedes signing and never grants source publication acceptance.
"""

from __future__ import annotations

from importlib import import_module

from sqlalchemy import text

from api.ptg2_geo_projection import validate_projection_dependency_bindings
from process import npi_result_generation as npi
from process import reference_family_result_generation as reference
from process.provider_directory_cms_native_registration import (
    NativeInputCandidate as NativeInputCandidate,
)
from process.provider_directory_cms_native_registration import (
    capture_current_native_input_candidates as capture_current_native_input_candidates,
)
from process.provider_directory_cms_native_registration import (
    capture_native_input_candidate as capture_native_input_candidate,
)
from process.provider_directory_cms_native_registration import (
    native_input_registration_roots as native_input_registration_roots,
)
from process.provider_directory_cms_native_registration import (
    prepare_local_native_input_publication as prepare_local_native_input_publication,
)
from process.provider_directory_cms_native_registration import (
    register_native_input_candidates as register_native_input_candidates,
)
from process.provider_directory_cms_native_registration import (
    require_native_input_registration_delta as require_native_input_registration_delta,
)
from process.provider_directory_cms_native_registration import (
    resolve_installed_native_input_guard as resolve_installed_native_input_guard,
)
from process.provider_directory_cms_native_registration import (
    verify_native_input_candidate_catalog as verify_native_input_candidate_catalog,
)
from process.provider_directory_cms_native_registration import (
    verify_native_input_candidates as verify_native_input_candidates,
)
from process.provider_directory_cms_tiger_inputs import (
    capture_protected_tiger_input,
    capture_selected_captured_tiger_input,
    captured_tiger_bindings,
    captured_tiger_relations,
    protected_tiger_relations,
)

_ADDITIONAL_INPUTS = (
    "provider_directory_dataset_network_plan",
    "provider_directory_dataset_affiliation_organization",
    "provider_directory_dataset_insurance_plan",
    "nucc_taxonomy",
    "provider_enrollment_hospital",
    "provider_enrollment_fqhc",
    "provider_enrollment_ffs_additional_npi",
    "provider_enrollment_ffs",
    "provider_enrollment_ffs_address",
    "facility_anchor_npi_override",
    "provider_enrichment_summary",
    "address_archive_v2",
    "openaddresses_geocode",
    "address_alias_v1",
)
_FAMILIES = ("cms-doctors", "facility-anchors", "geo", "mrf-address", "mrf", "tiger")
_TABLE = "cms_native_input_revision"
_TRIGGER = "cms_native_input_mutation"
_GEO = ("npi_address", "mrf_address", "doctor_clinician_address", "geo_zip_lookup")


def _relations(schema):
    """Enumerate external reads and the complete six-table NPI revision family."""
    names = {(schema, name) for name in (*npi.RELATION_NAMES, *_ADDITIONAL_INPUTS)}
    names.update((schema, name) for name in (*_GEO, "facility_anchor"))
    names.update(("tiger", name) for name in ("zip_state", "zcta5"))
    return tuple(sorted(names))


async def _physical_relations(session, schema, *, captured_tiger=None):
    """Resolve a fixed catalog set, including absent optional inputs, without row scans."""
    if captured_tiger is not None:
        return await _captured_physical_relations(session, schema, captured_tiger)
    rows = (
        await session.execute(
            text("""
        SELECT requested.name, relation.oid::bigint AS relation_oid,
               pg_relation_filenode(relation.oid)::bigint AS relfilenode,
               relation.relkind::text AS relkind, relation.relpersistence::text AS relpersistence,
               relation.relispartition OR EXISTS (
                   SELECT 1 FROM pg_inherits WHERE inhrelid=relation.oid OR inhparent=relation.oid
               ) AS inherited
          FROM unnest(CAST(:names AS text[])) AS requested(name)
          LEFT JOIN pg_class relation ON relation.oid=to_regclass(requested.name)
         ORDER BY requested.name
    """),
            {"names": [f'"{namespace}"."{name}"' for namespace, name in _relations(schema)]},
        )
    ).mappings()
    return {row["name"]: dict(row) for row in rows}


def _tiger_read_names(protected_tiger, captured_tiger, relations):
    return protected_tiger_relations(protected_tiger, relations) | captured_tiger_relations(captured_tiger, relations)


async def _lock_inputs(
    session,
    schema,
    relations,
    *,
    register=False,
    cutover=False,
    protected_nucc=None,
    protected_tiger=None,
    captured_tiger=None,
):
    """Lock mutable inputs; verified protected NUCC already holds exact parent/child read locks."""
    nucc_name = '"mrf"."nucc_taxonomy"'
    nucc_relation = relations.get(nucc_name)
    if protected_nucc is not None:
        _require_protected_nucc_proof(protected_nucc)
        if (
            schema != "mrf"
            or register
            or nucc_relation is None
            or nucc_relation["relation_oid"] != protected_nucc["physical"]["parent_oid"]
            or nucc_relation.get("relkind") != "p"
            or nucc_relation.get("relpersistence") != "p"
            or nucc_relation.get("inherited") is not True
            or nucc_relation.get("relfilenode") is not None
        ):
            raise RuntimeError("cms_address_native_nucc_proof_changed")
    elif nucc_relation is not None and nucc_relation.get("relkind") == "p":
        raise RuntimeError("cms_address_native_nucc_proof_changed")
    tiger_names = _tiger_read_names(protected_tiger, captured_tiger, relations)
    if register and tiger_names:
        raise RuntimeError("cms_address_native_tiger_proof_changed")
    names = [
        name
        for name, physical in relations.items()
        if physical["relation_oid"] is not None
        and name not in tiger_names
        and not (protected_nucc is not None and name == nucc_name)
    ]
    if names:
        await session.execute(
            text(
                "LOCK TABLE "
                + ", ".join(names)
                + (
                    " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
                    if register
                    else " IN SHARE MODE NOWAIT"
                    if cutover
                    else " IN ACCESS SHARE MODE NOWAIT"
                )
            )
        )
    for table in (npi.TABLE_NAME, reference.TABLE_NAME):
        name = f'"{schema}"."{table}"'
        exists = (await session.execute(text("SELECT to_regclass(:name)"), {"name": name})).scalar_one()
        if exists is not None:
            mode = "SHARE" if cutover or register else "ACCESS SHARE"
            await session.execute(text(f"LOCK TABLE {name} IN {mode} MODE NOWAIT"))


def _npi_body(schema):
    """The existing migration's statement revision function, with whitespace normalized only."""
    return f'''
        BEGIN
            UPDATE "{schema}"."npi_result_generation"
               SET local_generation = local_generation + 1,
                   origin_lineage_id = CASE WHEN origin_lineage_id IS NULL THEN NULL ELSE local_lineage_id END,
                   origin_generation = CASE WHEN origin_lineage_id IS NULL THEN NULL ELSE local_generation + 1 END,
                   published_at = CASE WHEN origin_lineage_id IS NULL THEN NULL ELSE transaction_timestamp() END
             WHERE singleton IS TRUE AND relation_oids IS NOT NULL AND TG_RELID::bigint = ANY(relation_oids);
            RETURN NULL;
        END;
    '''


async def _npi_revision(session, schema):
    """Require the actual all-event ALWAYS revision guards on all six accepted OIDs."""
    authority = await npi.capture_npi_serving_generation(session, schema_name=schema)
    guard_rows = (
        (
            await session.execute(
                text("""
        SELECT trigger.tgrelid::bigint AS relation_oid, trigger.tgtype, trigger.tgenabled::text AS tgenabled,
               trigger.tgnargs, trigger.tgqual IS NULL AS unconditional,
               trigger.tgfoid::bigint AS function_oid, function.prosrc, function.prosecdef,
               function.proconfig, language.lanname
          FROM pg_trigger trigger JOIN pg_proc function ON function.oid=trigger.tgfoid
          JOIN pg_language language ON language.oid=function.prolang
         WHERE trigger.tgrelid=ANY(CAST(:oids AS oid[])) AND trigger.tgname=:trigger
           AND trigger.tgfoid=to_regprocedure(:function)
         ORDER BY trigger.tgrelid
    """),
                {
                    "oids": list(authority.relation_oids),
                    "trigger": npi.REVISION_TRIGGER,
                    "function": f'"{schema}"."{npi.REVISION_FUNCTION}"()',
                },
            )
        )
        .mappings()
        .all()
    )
    if len(guard_rows) != len(npi.RELATION_NAMES) or any(
        guard["tgtype"] != 60
        or guard["tgenabled"] != "A"
        or guard["tgnargs"] != 0
        or not guard["unconditional"]
        or not guard["prosecdef"]
        or guard["proconfig"] != ["search_path=pg_catalog"]
        or guard["lanname"] != "plpgsql"
        or " ".join(guard["prosrc"].split()) != " ".join(_npi_body(schema).split())
        for guard in guard_rows
    ):
        raise RuntimeError("cms_address_npi_revision_guard_unavailable")
    return {"authority": authority.as_dict(), "guards": [dict(guard) for guard in guard_rows]}


async def _reference_authorities(session, schema, *, protected_tiger=None):
    """Preserve current native acceptance separately from mutation baselines."""
    name = f'"{schema}"."{reference.TABLE_NAME}"'
    authority_rows = (
        await session.execute(
            text(f"SELECT * FROM {name} WHERE importer_id=ANY(CAST(:families AS text[])) ORDER BY importer_id"),
            {"families": [family for family in _FAMILIES if family != "tiger" or protected_tiger is None]},
        )
    ).mappings()
    authorities_by_family = {}
    for authority_row in authority_rows:
        authority = reference.validate_reference_family_result_generation_authority(authority_row)
        namespace = "tiger" if authority.importer_id == "tiger" else schema
        names = reference.RELATION_NAMES_BY_IMPORTER[authority.importer_id]
        actual_oids = tuple(
            (
                await session.execute(
                    text(
                        "SELECT to_regclass(name)::oid::bigint FROM unnest(CAST(:names AS text[])) WITH ORDINALITY "
                        "AS requested(name,ordinal) ORDER BY ordinal"
                    ),
                    {"names": [f'"{namespace}"."{table}"' for table in names]},
                )
            ).scalars()
        )
        if authority.serving_generation is not None and authority.relation_oids == actual_oids:
            if authority.importer_id == "geo":
                from process.geo_source_storage import capture_geo_generation

                await capture_geo_generation(session, schema)
            if authority.importer_id == "cms-doctors":
                await reference.capture_reference_family_serving_generation(
                    session, importer_id="cms-doctors", schema_name=schema
                )
            authorities_by_family[authority.importer_id] = authority.as_dict()
    return authorities_by_family


def _geo_bindings(schema, relations, *, captured_tiger=None):
    """Reuse the exact geo physical binding contract; missing inputs remain unsupported."""
    names = [(schema, name) for name in _GEO] + [("tiger", "zip_state"), ("tiger", "zcta5")]
    captured_by_name = {} if captured_tiger is None else captured_tiger_bindings(captured_tiger)
    bindings_by_name = {}
    for namespace, name in names:
        physical = relations[f'"{namespace}"."{name}"']
        if physical["relation_oid"] is None:
            return None
        bindings_by_name[f"{namespace}.{name}"] = {
            "schema_name": captured_by_name.get(f"{namespace}.{name}", {}).get("schema_name", namespace),
            "table_name": name,
            "relation_oid": physical["relation_oid"],
            "relfilenode": physical["relfilenode"],
        }
    return validate_projection_dependency_bindings(schema, bindings_by_name)


def _require_heaps(relations, *, protected_nucc=None, protected_tiger=None, captured_tiger=None, protected_fhir=None):
    tiger_names = protected_tiger_relations(protected_tiger, relations) if protected_tiger is not None else set()
    captured_tiger_relations(captured_tiger, relations)
    fhir_names = _fhir_input_names(protected_fhir, relations)
    for name, value in relations.items():
        if name in tiger_names or name in fhir_names:
            continue
        if (
            protected_nucc is not None
            and name == '"mrf"."nucc_taxonomy"'
            and value["relation_oid"] == protected_nucc["physical"]["parent_oid"]
        ):
            if value["relkind"] != "p" or value["relpersistence"] != "p" or value["inherited"] is not True:
                raise RuntimeError("cms_address_native_input_requires_persistent_heap")
            continue
        if value["relation_oid"] is not None and (
            value["relkind"] != "r" or value["relpersistence"] != "p" or value["inherited"]
        ):
            raise RuntimeError("cms_address_native_input_requires_persistent_heap")


def _advance_body(schema):
    return f'''
BEGIN
    UPDATE "{schema}"."{_TABLE}" SET revision=revision+1,mutation_xid=pg_current_xact_id() WHERE relation_oid=TG_RELID::bigint;
    IF NOT FOUND THEN RAISE EXCEPTION 'cms_native_input_not_registered'; END IF;
    RETURN NULL;
END;
'''


async def _require_ledger_guards(session, schema):
    """Ensure registered revisions cannot be reset through a disabled or replaced ledger guard."""
    guard_rows = (
        (
            await session.execute(
                text("""
        SELECT trigger.tgname, trigger.tgtype, trigger.tgenabled::text AS enabled,
               function.prosrc, function.proconfig, function.prosecdef,
               language.lanname, trigger.tgnargs, trigger.tgqual IS NULL AS unconditional
          FROM pg_trigger trigger JOIN pg_proc function ON function.oid=trigger.tgfoid
          JOIN pg_language language ON language.oid=function.prolang
         WHERE trigger.tgrelid=to_regclass(:table) AND trigger.tgname=ANY(CAST(:names AS text[]))
           AND trigger.tgfoid=to_regprocedure(:function)
    """),
                {
                    "table": f'"{schema}"."{_TABLE}"',
                    "function": f'"{schema}".cms_native_input_revision_guard()',
                    "names": ["cms_native_input_revision_immutable", "cms_native_input_revision_no_truncate"],
                },
            )
        )
        .mappings()
        .all()
    )
    _validate_ledger_guards(guard_rows)


def _validate_ledger_guards(guard_rows):
    expected_body = """
BEGIN
    IF TG_OP='UPDATE' AND pg_trigger_depth()=2
       AND NEW.relation_oid=OLD.relation_oid AND NEW.schema_name=OLD.schema_name
       AND NEW.table_name=OLD.table_name AND NEW.revision=OLD.revision+1
       AND NEW.mutation_xid=pg_current_xact_id() THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'cms_native_input_revision_immutable';
END;
"""
    expected_type_by_trigger = {"cms_native_input_revision_immutable": 27, "cms_native_input_revision_no_truncate": 34}
    if len(guard_rows) != 2 or any(
        guard["tgtype"] != expected_type_by_trigger[guard["tgname"]]
        or guard["enabled"] != "A"
        or guard["prosrc"] != expected_body
        or guard["proconfig"] != ["search_path=pg_catalog"]
        or guard["prosecdef"]
        or guard["lanname"] != "plpgsql"
        or guard["tgnargs"] != 0
        or not guard["unconditional"]
        for guard in guard_rows
    ):
        raise RuntimeError("cms_address_native_revision_ledger_unavailable")


def _require_native_revision_triggers(guard_rows, oids, schema):
    """Verify every exact native revision guard before reading its revision."""
    if len(guard_rows) != len(oids) or any(
        guard["tgtype"] != 60
        or guard["tgenabled"] != "A"
        or guard["tgnargs"] != 0
        or not guard["unconditional"]
        or not guard["prosecdef"]
        or not guard["returns_trigger"]
        or guard["proconfig"] != ["search_path=pg_catalog"]
        or guard["lanname"] != "plpgsql"
        or guard["prosrc"] != _advance_body(schema)
        for guard in guard_rows
    ):
        raise RuntimeError("cms_address_native_revision_guard_unavailable")


def _revision_input_oids(schema, relations, protected_nucc, protected_tiger, captured_tiger, protected_fhir):
    tiger_names = _tiger_read_names(protected_tiger, captured_tiger, relations)
    npi_names = {f'"{schema}"."{name}"' for name in npi.RELATION_NAMES}
    fhir_names = _fhir_input_names(protected_fhir, relations, schema)
    return [
        physical["relation_oid"]
        for name, physical in relations.items()
        if physical["relation_oid"] is not None
        and name not in tiger_names
        and name not in npi_names
        and name not in fhir_names
        and not (
            protected_nucc is not None
            and name == '"mrf"."nucc_taxonomy"'
            and physical["relation_oid"] == protected_nucc["physical"]["parent_oid"]
        )
        and name not in {f'"{schema}"."doctor_clinician_address"', f'"{schema}"."geo_zip_lookup"'}
    ]


async def _revision_guards(
    session, schema, relations, *, protected_nucc=None, protected_tiger=None, captured_tiger=None, protected_fhir=None
):
    """Require exact all-event, unconditional ALWAYS guards and the migration function."""
    await session.execute(text(f'SELECT relation_oid FROM "{schema}"."{_TABLE}" WHERE FALSE'))
    await _require_ledger_guards(session, schema)
    oids = _revision_input_oids(schema, relations, protected_nucc, protected_tiger, captured_tiger, protected_fhir)
    guard_rows = (
        (
            await session.execute(
                text("""
        SELECT trigger.tgrelid::bigint AS relation_oid, trigger.tgtype, trigger.tgenabled::text AS tgenabled,
               trigger.tgnargs, trigger.tgqual IS NULL AS unconditional,
               function.prosrc, function.prosecdef, function.proconfig,
               function.prorettype='trigger'::regtype AS returns_trigger,
               language.lanname
          FROM pg_trigger trigger JOIN pg_proc function ON function.oid=trigger.tgfoid
          JOIN pg_language language ON language.oid=function.prolang
         WHERE trigger.tgrelid=ANY(CAST(:oids AS oid[])) AND trigger.tgname=:trigger
           AND trigger.tgfoid=to_regprocedure(:function)
         ORDER BY trigger.tgrelid
    """),
                {"oids": oids, "trigger": _TRIGGER, "function": f'"{schema}".cms_native_input_advance()'},
            )
        )
        .mappings()
        .all()
    )
    _require_native_revision_triggers(guard_rows, oids, schema)
    revisions = (
        (
            await session.execute(
                text(
                    f'SELECT relation_oid, schema_name, table_name, revision FROM "{schema}"."{_TABLE}" '
                    "WHERE relation_oid=ANY(CAST(:oids AS bigint[])) ORDER BY relation_oid"
                ),
                {"oids": oids},
            )
        )
        .mappings()
        .all()
    )
    if len(revisions) != len(oids):
        raise RuntimeError("cms_address_native_input_not_registered")
    return [dict(guard) for guard in revisions]


async def register_native_address_inputs(session, schema: str) -> None:
    """Register current heaps additively before signed preflight in a short caller transaction."""
    schema = npi._schema_name(schema)
    relations = await _physical_relations(session, schema)
    _require_heaps(relations)
    await _lock_inputs(session, schema, relations, register=True)
    if relations != await _physical_relations(session, schema):
        raise RuntimeError("cms_address_native_inputs_changed")
    from process.cms_doctors_immutable import require_doctors_storage

    await require_doctors_storage(session, schema_name=schema)
    from process.geo_source_storage import capture_geo_generation

    await capture_geo_generation(session, schema)
    for namespace, name in _relations(schema):
        relation = f'"{namespace}"."{name}"'
        oid = relations[relation]["relation_oid"]
        if oid is not None and not (
            namespace == schema and name in (*npi.RELATION_NAMES, "doctor_clinician_address", "geo_zip_lookup")
        ):
            await _register_relation(session, schema, namespace, name, oid)
    await _revision_guards(session, schema, relations)


async def _register_relation(session, schema, namespace, name, oid):
    relation = f'"{namespace}"."{name}"'
    registered = (
        await session.execute(
            text(
                f'INSERT INTO "{schema}"."{_TABLE}" (relation_oid,schema_name,table_name) '
                "VALUES (:oid,:namespace,:name) ON CONFLICT (relation_oid) DO NOTHING RETURNING relation_oid"
            ),
            {"oid": oid, "namespace": namespace, "name": name},
        )
    ).scalar_one_or_none()
    exists = (
        await session.execute(
            text("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=:oid AND tgname=:trigger)"),
            {"oid": oid, "trigger": _TRIGGER},
        )
    ).scalar_one()
    if not exists:
        if registered is None:
            raise RuntimeError("cms_address_native_revision_guard_unavailable")
        await session.execute(
            text(
                f"CREATE TRIGGER {_TRIGGER} AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {relation} "
                f'FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".cms_native_input_advance()'
            )
        )
        await session.execute(text(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {_TRIGGER}"))


async def _protected_inputs(session, schema, relations):
    nucc_relation = relations.get('"mrf"."nucc_taxonomy"')
    protected_nucc = None
    if schema == "mrf" and nucc_relation is not None and nucc_relation["relkind"] == "p":
        protected_nucc = await capture_protected_nucc_input(session, schema=schema)
    protected_tiger = None
    if any(relations.get(f'"tiger"."{name}"', {}).get("inherited") for name in ("zcta5", "zip_state")):
        protected_tiger = await capture_protected_tiger_input(session, relations)
    protected_by_kind = {}
    if _has_partitioned_fhir(relations, schema):
        protected_by_kind["protected_fhir"] = await _capture_bound_fhir(session, schema)
    if protected_nucc is not None:
        protected_by_kind["protected_nucc"] = protected_nucc
    if protected_tiger is not None:
        protected_by_kind["protected_tiger"] = protected_tiger
    return protected_by_kind


async def _capture_selected_input_relations(session, schema):
    captured_tiger = await capture_selected_captured_tiger_input(session)
    captured_options = {"captured_tiger": captured_tiger} if captured_tiger is not None else {}
    relations = await _physical_relations(session, schema, **captured_options)
    protected_by_kind = await _protected_inputs(session, schema, relations)
    protected_by_kind.update(captured_options)
    return relations, protected_by_kind


async def _input_database_identity(session):
    return (
        (
            await session.execute(
                text("""
        SELECT current_database() AS database_name, oid::bigint AS database_oid,
               (SELECT system_identifier::text FROM pg_control_system()) AS system_identifier
          FROM pg_database WHERE datname=current_database()
    """)
            )
        )
        .mappings()
        .one()
    )


async def capture_native_address_input_fence(session, schema: str, *, cutover=False) -> dict:
    """Read registered proofs in a repeatable snapshot; never register during signing."""
    schema = npi._schema_name(schema)
    from process.provider_directory_cms_archive import require_source_archive_owner

    await require_source_archive_owner(session, schema)
    if not cutover:
        isolation = (await session.execute(text("SHOW transaction_isolation"))).scalar_one()
        if isolation not in ("repeatable read", "serializable"):
            raise RuntimeError("cms_address_native_snapshot_required")
    before, protected_by_kind = await _capture_selected_input_relations(session, schema)
    captured_tiger = protected_by_kind.get("captured_tiger")
    captured_options = {"captured_tiger": captured_tiger} if captured_tiger is not None else {}
    protected_nucc = protected_by_kind.get("protected_nucc")
    protected_tiger = protected_by_kind.get("protected_tiger")
    tiger_authority = protected_tiger or captured_tiger
    _require_heaps(before, **protected_by_kind)
    await _lock_inputs(
        session,
        schema,
        before,
        cutover=cutover,
        **{key: proof_option for key, proof_option in protected_by_kind.items() if key != "protected_fhir"},
    )
    if await _physical_relations(session, schema, **captured_options) != before:
        raise RuntimeError("cms_address_native_inputs_changed")
    protected_fhir = protected_by_kind.get("protected_fhir")
    if protected_fhir is not None and cutover:
        if (await session.execute(text("SHOW transaction_isolation"))).scalar_one() != "read committed":
            raise RuntimeError("cms_address_native_fhir_cutover_isolation_changed")
    if protected_fhir is not None and await _capture_bound_fhir(session, schema) != protected_fhir:
        raise RuntimeError("cms_address_native_inputs_changed")
    database = await _input_database_identity(session)
    fence_by_field = {
        "version": 1,
        "schema": schema,
        "database": dict(database),
        "relations": before,
        "npi": await _npi_revision(session, schema),
        "reference_authorities": await _reference_authorities(
            session,
            schema,
            **({"protected_tiger": tiger_authority} if tiger_authority is not None else {}),
        ),
        "geo_bindings": _geo_bindings(schema, before, **captured_options),
        "revisions": await _revision_guards(session, schema, before, **protected_by_kind),
        "cms_office_read_relations": await _cms_office_read_relations(
            session, schema, cutover=cutover, protected_fhir=protected_fhir
        ),
    }
    if protected_nucc is not None:
        fence_by_field.update(version=2, nucc_protected=protected_nucc)
    if protected_tiger is not None:
        fence_by_field.update(version=3, tiger_protected=protected_tiger)
    if captured_tiger is not None:
        fence_by_field.update(version=4, tiger_captured=captured_tiger)
    if protected_fhir is not None:
        fence_by_field.update(version=5, fhir_protected=protected_fhir)
    require_supported_native_address_inputs(fence_by_field)
    return fence_by_field


async def _cms_office_read_relations(session, schema, *, cutover=False, protected_fhir=None):
    """Pin existing sealed CMS read catalogs; never install mutation hooks."""
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES

    statement = text("""SELECT requested.name,relation.oid::bigint AS relation_oid,
      namespace.oid::bigint AS schema_oid,namespace.nspowner::bigint AS schema_owner_oid,
      relation.relowner::bigint AS owner_oid,relation.relkind::text AS relkind,
      relation.relpersistence::text AS relpersistence,relation.relacl::text AS raw_acl,
      namespace.nspacl::text AS raw_schema_acl,relation.relrowsecurity,relation.relforcerowsecurity,
      relation.relispartition,
      relation.relispartition OR EXISTS(SELECT 1 FROM pg_inherits
        WHERE inhrelid=relation.oid OR inhparent=relation.oid) AS inherited
      FROM unnest(CAST(:names AS text[])) requested(name)
      LEFT JOIN pg_class relation ON relation.oid=to_regclass(:schema||'.'||requested.name)
      LEFT JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace ORDER BY requested.name""")
    parameters_by_name = {"names": list(CMS_OFFICE_READ_TABLES), "schema": '"' + schema + '"'}
    before_catalog_by_name = {
        catalog_row["name"]: dict(catalog_row)
        for catalog_row in (await session.execute(statement, parameters_by_name)).mappings()
    }
    protected_names = {
        "provider_directory_cms_npd_resource_witness",
        "provider_directory_entity_source_binding",
    }
    partitioned_names = {name for name, catalog_row in before_catalog_by_name.items() if catalog_row["relkind"] == "p"}
    if partitioned_names:
        from process.provider_directory_generic_source_provision import require_protected_fhir_input

        if protected_fhir is None or not partitioned_names <= protected_names:
            raise RuntimeError("cms_typed_office_read_heap_unavailable")
        require_protected_fhir_input(protected_fhir)
        if protected_fhir["schema"] != schema or await _capture_bound_fhir(session, schema) != protected_fhir:
            raise RuntimeError("cms_address_native_fhir_proof_changed")
    for name, catalog_row in before_catalog_by_name.items():
        if catalog_row["relation_oid"] is not None:
            if (
                (
                    name in partitioned_names
                    and (catalog_row["relispartition"] or catalog_row["owner_oid"] != protected_fhir["owner_oid"])
                )
                or (name not in partitioned_names and (catalog_row["relkind"] != "r" or catalog_row["inherited"]))
                or catalog_row["relpersistence"] != "p"
                or catalog_row["relrowsecurity"]
                or catalog_row["relforcerowsecurity"]
            ):
                raise RuntimeError("cms_typed_office_read_heap_unavailable")
            await session.execute(
                text(f'LOCK TABLE "{schema}"."{name}" IN ' + ("SHARE" if cutover else "ACCESS SHARE") + " MODE NOWAIT")
            )
    if partitioned_names and await _capture_bound_fhir(session, schema) != protected_fhir:
        raise RuntimeError("cms_address_native_fhir_proof_changed")
    after_catalog_by_name = {
        catalog_row["name"]: dict(catalog_row)
        for catalog_row in (await session.execute(statement, parameters_by_name)).mappings()
    }
    if before_catalog_by_name != after_catalog_by_name:
        raise RuntimeError("cms_typed_office_read_catalog_changed")
    return before_catalog_by_name


def _require_fhir_fence(fence, schema):
    protected_fhir = fence.get("fhir_protected")
    if fence["version"] == 5:
        if protected_fhir is None or (
            fence.get("tiger_protected") is not None and fence.get("tiger_captured") is not None
        ):
            raise RuntimeError("cms_address_native_fhir_proof_changed")
        _fhir_input_names(protected_fhir, fence["relations"], schema)
        if protected_fhir["database_oid"] != fence["database"]["database_oid"]:
            raise RuntimeError("cms_address_native_fhir_proof_changed")
    elif protected_fhir is not None or _has_partitioned_fhir(fence["relations"], schema):
        raise RuntimeError("cms_address_native_fhir_proof_changed")


def require_supported_native_address_inputs(fence: dict) -> None:
    """Require complete physical pins plus accepted native publication families."""
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES

    if set(fence.get("cms_office_read_relations", {})) != set(CMS_OFFICE_READ_TABLES):
        raise RuntimeError("cms_address_native_office_read_fence_invalid")
    schema = npi._schema_name(fence["schema"])
    expected_names = {f'"{namespace}"."{name}"' for namespace, name in _relations(schema)}
    if fence.get("version") not in (1, 2, 3, 4, 5) or set(fence["relations"]) != expected_names:
        raise RuntimeError("cms_address_native_input_fence_invalid")
    protected_nucc = fence.get("nucc_protected")
    if fence["version"] == 2 or (fence["version"] in (3, 4, 5) and protected_nucc is not None):
        if protected_nucc is None:
            raise RuntimeError("cms_address_native_input_fence_invalid")
        _require_protected_nucc_proof(protected_nucc)
        observed_nucc = fence["relations"].get('"mrf"."nucc_taxonomy"', {})
        if (
            schema != "mrf"
            or observed_nucc.get("relation_oid") != protected_nucc["physical"]["parent_oid"]
            or observed_nucc.get("relkind") != "p"
            or observed_nucc.get("inherited") is not True
        ):
            raise RuntimeError("cms_address_native_nucc_proof_changed")
    elif protected_nucc is not None:
        raise RuntimeError("cms_address_native_nucc_proof_changed")
    protected_tiger = fence.get("tiger_protected")
    if fence["version"] == 3 or (fence["version"] == 5 and protected_tiger is not None):
        if protected_tiger is None:
            raise RuntimeError("cms_address_native_tiger_proof_changed")
        protected_tiger_relations(protected_tiger, fence["relations"])
        if protected_tiger["database_oid"] != fence["database"]["database_oid"]:
            raise RuntimeError("cms_address_native_tiger_proof_changed")
    elif protected_tiger is not None:
        raise RuntimeError("cms_address_native_tiger_proof_changed")
    if fence.get("geo_bindings") is None:
        raise RuntimeError("cms_address_native_geo_inputs_unavailable")
    captured_tiger = fence.get("tiger_captured")
    if fence["version"] == 4 or (fence["version"] == 5 and captured_tiger is not None):
        if captured_tiger is None:
            raise RuntimeError("cms_address_native_tiger_proof_changed")
        captured_tiger_relations(captured_tiger, fence["relations"])
        if captured_tiger["database_oid"] != fence["database"]["database_oid"]:
            raise RuntimeError("cms_address_native_tiger_proof_changed")
        expected_tiger = captured_tiger_bindings(captured_tiger)
        if any(fence["geo_bindings"].get(name) != binding for name, binding in expected_tiger.items()):
            raise RuntimeError("cms_address_native_tiger_proof_changed")
    elif captured_tiger is not None:
        raise RuntimeError("cms_address_native_tiger_proof_changed")
    _require_fhir_fence(fence, schema)
    authorities = fence["reference_authorities"]
    required_families = {"cms-doctors", "geo"} | (
        {"tiger"} if protected_tiger is None and captured_tiger is None else set()
    )
    if fence["relations"][f'"{schema}"."facility_anchor"']["relation_oid"] is not None:
        required_families.add("facility-anchors")
    if not required_families <= authorities.keys() or not {"mrf", "mrf-address"} & authorities.keys():
        raise RuntimeError("cms_address_native_publication_unavailable")


async def assert_native_address_input_fence(session, schema: str, expected: dict) -> None:
    """Hold source locks through caller cutover; changed rows, swaps or acceptance reject."""
    require_supported_native_address_inputs(expected)
    if await capture_native_address_input_fence(session, schema, cutover=True) != expected:
        raise RuntimeError("cms_address_native_inputs_changed")


def _require_protected_nucc_proof(proof):
    from process import nucc_snapshot_partition as partition

    if (
        type(proof) is not dict
        or set(proof) != {"version", "physical", "generation"}
        or type(proof["version"]) is not int
        or proof["version"] != 1
    ):
        raise RuntimeError("cms_address_native_nucc_proof_changed")
    physical = proof["physical"]
    fields = {
        "contract",
        "owner_oid",
        "parent_oid",
        "parent_index_oid",
        "child_oid",
        "child_schema",
        "child_schema_oid",
        "child_table",
        "child_filenode",
    }
    if type(physical) is not dict or set(physical) != fields or physical["contract"] != partition.CONTRACT:
        raise RuntimeError("cms_address_native_nucc_proof_changed")
    for key in fields - {"contract", "child_schema", "child_table"}:
        partition._oid(physical[key])
    if (
        type(physical["child_schema"]) is not str
        or partition._CHILD_SCHEMA.fullmatch(physical["child_schema"]) is None
        or physical["child_table"] != "nucc_taxonomy"
    ):
        raise RuntimeError("cms_address_native_nucc_proof_changed")
    authority = reference._nucc_result_authority(proof["generation"])
    if authority.relation_oids != (physical["child_oid"],):
        raise RuntimeError("cms_address_native_nucc_generation_changed")


async def capture_protected_nucc_input(session, *, schema):
    """Bind the exact native protected parent/physical child to its accepted generation."""
    from process import nucc_snapshot_partition as partition

    if schema != "mrf":
        raise RuntimeError("cms_address_native_nucc_schema_changed")
    physical = await partition.capture_nucc_parent(session)
    authority = await reference.read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name=schema
    )
    proof_by_field = dict(version=1, physical=physical, generation=authority.as_dict())
    _require_protected_nucc_proof(proof_by_field)
    return proof_by_field


async def _captured_physical_relations(session, schema, proof):
    """Keep canonical fence keys while resolving only the selected physical Tiger heaps."""
    bindings = captured_tiger_bindings(proof)
    names = [f'"{namespace}"."{name}"' for namespace, name in _relations(schema)]
    physical_names = []
    for namespace, name in _relations(schema):
        binding = bindings.get(f"{namespace}.{name}")
        physical_names.append(
            f'"{binding["schema_name"]}"."{binding["table_name"]}"' if binding else f'"{namespace}"."{name}"'
        )
    rows = (
        (
            await session.execute(
                text("""SELECT requested.name,relation.oid::bigint AS relation_oid,
        pg_relation_filenode(relation.oid)::bigint AS relfilenode,relation.relkind::text AS relkind,
        relation.relpersistence::text AS relpersistence,relation.relispartition OR EXISTS(
          SELECT 1 FROM pg_inherits WHERE inhrelid=relation.oid OR inhparent=relation.oid) AS inherited
        FROM unnest(CAST(:names AS text[]),CAST(:physical_names AS text[])) requested(name,physical_name)
        LEFT JOIN pg_class relation ON relation.oid=to_regclass(requested.physical_name) ORDER BY requested.name"""),
                dict(names=names, physical_names=physical_names),
            )
        )
        .mappings()
        .all()
    )
    relations_by_name = {row["name"]: dict(row) for row in rows}
    captured_tiger_relations(proof, relations_by_name)
    return relations_by_name


async def capture_protected_fhir_input(connection, *, source_policy):
    """Expose native Source-family observation to the existing original-admin seam."""
    from process.provider_directory_generic_source_provision import capture_protected_fhir_input as capture

    return await capture(connection, source_policy=source_policy)


def _has_partitioned_fhir(relations, schema):
    from process.provider_directory_generic_source_provision import FHIR_INPUT_ROOTS

    return any(relations.get(f'"{schema}"."{name}"', {}).get("relkind") == "p" for name in FHIR_INPUT_ROOTS)


def _fhir_input_names(proof, relations, schema=None):
    from process.provider_directory_generic_source_provision import require_protected_fhir_input

    if proof is None:
        return set()
    require_protected_fhir_input(proof)
    if schema is not None and proof["schema"] != schema:
        raise RuntimeError("cms_address_native_fhir_proof_changed")
    names = set()
    for name, oid in proof["parent_oids"]:
        quoted = f'"{proof["schema"]}"."{name}"'
        observed = relations.get(quoted, {})
        inherited = any(row["parents"] == [oid] for row in proof["inventory"])
        if (
            observed.get("relation_oid") != oid
            or observed.get("relkind") != "p"
            or observed.get("relpersistence") != "p"
            or observed.get("relfilenode") is not None
            or observed.get("inherited") is not inherited
        ):
            raise RuntimeError("cms_address_native_fhir_proof_changed")
        names.add(quoted)
    return names


async def _capture_bound_fhir(session, schema):
    from process import provider_directory_source_observation as observation
    from process.provider_directory_generic_source_policy import bound_publisher, source_callable_capability
    from process.provider_directory_projection_stage import _copy_driver

    directory = import_module("process.provider_directory_fhir")
    if observation.current_observation(directory) is not None:
        return await observation.capture_fhir_input(directory, session, schema)
    publisher = bound_publisher()
    connection = await _copy_driver(session)
    if publisher.connection is not connection or publisher.policy.schema != schema:
        raise RuntimeError("cms_address_native_fhir_source_binding_changed")
    await source_callable_capability(connection, publisher.policy)
    proof = await capture_protected_fhir_input(connection, source_policy=publisher.policy)
    if bound_publisher() is not publisher or proof is None:
        raise RuntimeError("cms_address_native_fhir_source_binding_changed")
    return proof
