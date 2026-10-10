# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded mutation and publication fence for external native address inputs.

FHIR stages and their admission, desired vector, overlay and alias selection remain
owned by the composite preparation contract. This fence covers the additional native
reads. Registration precedes signing and never grants source publication acceptance.
"""

from __future__ import annotations

from sqlalchemy import text

from api.ptg2_geo_projection import validate_projection_dependency_bindings
from process import npi_result_generation as npi
from process import reference_family_result_generation as reference

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


async def _physical_relations(session, schema):
    """Resolve a fixed catalog set, including absent optional inputs, without row scans."""
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


async def _lock_inputs(session, schema, relations, *, register=False, cutover=False):
    """Take short table locks before authority rows; caller releases at transaction end."""
    names = [name for name, value in relations.items() if value["relation_oid"] is not None]
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
    for table in (npi.TABLE_NAME, reference.TABLE_NAME, _TABLE):
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


async def _reference_authorities(session, schema):
    """Preserve current native acceptance separately from mutation baselines."""
    name = f'"{schema}"."{reference.TABLE_NAME}"'
    rows = (
        await session.execute(
            text(f"SELECT * FROM {name} WHERE importer_id=ANY(CAST(:families AS text[])) ORDER BY importer_id"),
            {"families": list(_FAMILIES)},
        )
    ).mappings()
    authorities_by_family = {}
    for row in rows:
        authority = reference.validate_reference_family_result_generation_authority(row)
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
            authorities_by_family[authority.importer_id] = authority.as_dict()
    return authorities_by_family


def _geo_bindings(schema, relations):
    """Reuse the exact geo physical binding contract; missing inputs remain unsupported."""
    names = [(schema, name) for name in _GEO] + [("tiger", "zip_state"), ("tiger", "zcta5")]
    bindings_by_name = {}
    for namespace, name in names:
        physical = relations[f'"{namespace}"."{name}"']
        if physical["relation_oid"] is None:
            return None
        bindings_by_name[f"{namespace}.{name}"] = {
            "schema_name": namespace,
            "table_name": name,
            "relation_oid": physical["relation_oid"],
            "relfilenode": physical["relfilenode"],
        }
    return validate_projection_dependency_bindings(schema, bindings_by_name)


def _require_heaps(relations):
    for value in relations.values():
        if value["relation_oid"] is not None and (
            value["relkind"] != "r" or value["relpersistence"] != "p" or value["inherited"]
        ):
            raise RuntimeError("cms_address_native_input_requires_persistent_heap")


def _advance_body(schema):
    return f'''
BEGIN
    UPDATE "{schema}"."{_TABLE}" SET revision=revision+1 WHERE relation_oid=TG_RELID::bigint;
    IF NOT FOUND THEN RAISE EXCEPTION 'cms_native_input_not_registered'; END IF;
    RETURN NULL;
END;
'''


async def _require_ledger_guards(session, schema):
    """Ensure registered revisions cannot be reset through a disabled or replaced ledger guard."""
    expected_body = """
BEGIN
    IF TG_OP='UPDATE' AND pg_trigger_depth()=2
       AND NEW.relation_oid=OLD.relation_oid AND NEW.schema_name=OLD.schema_name
       AND NEW.table_name=OLD.table_name AND NEW.revision=OLD.revision+1 THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'cms_native_input_revision_immutable';
END;
"""
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


async def _revision_guards(session, schema, relations):
    """Require exact all-event, unconditional ALWAYS guards and the migration function."""
    await _require_ledger_guards(session, schema)
    npi_names = {f'"{schema}"."{name}"' for name in npi.RELATION_NAMES}
    oids = [
        physical["relation_oid"]
        for name, physical in relations.items()
        if physical["relation_oid"] is not None and name not in npi_names
    ]
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
    for namespace, name in _relations(schema):
        relation = f'"{namespace}"."{name}"'
        oid = relations[relation]["relation_oid"]
        if oid is not None and not (namespace == schema and name in npi.RELATION_NAMES):
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


async def capture_native_address_input_fence(session, schema: str, *, cutover=False) -> dict:
    """Read registered proofs in a repeatable snapshot; never register during signing."""
    schema = npi._schema_name(schema)
    if not cutover:
        isolation = (await session.execute(text("SHOW transaction_isolation"))).scalar_one()
        if isolation not in ("repeatable read", "serializable"):
            raise RuntimeError("cms_address_native_snapshot_required")
    before = await _physical_relations(session, schema)
    _require_heaps(before)
    await _lock_inputs(session, schema, before, cutover=cutover)
    if await _physical_relations(session, schema) != before:
        raise RuntimeError("cms_address_native_inputs_changed")
    database = (
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
    fence_by_field = {
        "version": 1,
        "schema": schema,
        "database": dict(database),
        "relations": before,
        "npi": await _npi_revision(session, schema),
        "reference_authorities": await _reference_authorities(session, schema),
        "geo_bindings": _geo_bindings(schema, before),
        "revisions": await _revision_guards(session, schema, before),
        "cms_office_read_relations": await _cms_office_read_relations(session, schema, cutover=cutover),
    }
    require_supported_native_address_inputs(fence_by_field)
    return fence_by_field


async def _cms_office_read_relations(session, schema, *, cutover=False):
    """Pin existing sealed CMS read catalogs; never install mutation hooks."""
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES

    statement = text("""SELECT requested.name,relation.oid::bigint AS relation_oid,
      namespace.oid::bigint AS schema_oid,namespace.nspowner::bigint AS schema_owner_oid,
      relation.relowner::bigint AS owner_oid,relation.relkind::text AS relkind,
      relation.relpersistence::text AS relpersistence,relation.relacl::text AS raw_acl,
      namespace.nspacl::text AS raw_schema_acl,relation.relrowsecurity,relation.relforcerowsecurity,
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
    for name, catalog_row in before_catalog_by_name.items():
        if catalog_row["relation_oid"] is not None:
            if (
                catalog_row["relkind"] != "r"
                or catalog_row["relpersistence"] != "p"
                or catalog_row["inherited"]
                or catalog_row["relrowsecurity"]
                or catalog_row["relforcerowsecurity"]
            ):
                raise RuntimeError("cms_typed_office_read_heap_unavailable")
            await session.execute(
                text(f'LOCK TABLE "{schema}"."{name}" IN ' + ("SHARE" if cutover else "ACCESS SHARE") + " MODE NOWAIT")
            )
    after_catalog_by_name = {
        catalog_row["name"]: dict(catalog_row)
        for catalog_row in (await session.execute(statement, parameters_by_name)).mappings()
    }
    if before_catalog_by_name != after_catalog_by_name:
        raise RuntimeError("cms_typed_office_read_catalog_changed")
    return before_catalog_by_name


def require_supported_native_address_inputs(fence: dict) -> None:
    """Require complete physical pins plus accepted native publication families."""
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES

    if set(fence.get("cms_office_read_relations", {})) != set(CMS_OFFICE_READ_TABLES):
        raise RuntimeError("cms_address_native_office_read_fence_invalid")
    schema = npi._schema_name(fence["schema"])
    expected_names = {f'"{namespace}"."{name}"' for namespace, name in _relations(schema)}
    if fence.get("version") != 1 or set(fence["relations"]) != expected_names:
        raise RuntimeError("cms_address_native_input_fence_invalid")
    if fence.get("geo_bindings") is None:
        raise RuntimeError("cms_address_native_geo_inputs_unavailable")
    authorities = fence["reference_authorities"]
    required_families = {"cms-doctors", "geo", "tiger"}
    if fence["relations"][f'"{schema}"."facility_anchor"']["relation_oid"] is not None:
        required_families.add("facility-anchors")
    if not required_families <= authorities.keys() or not {"mrf", "mrf-address"} & authorities.keys():
        raise RuntimeError("cms_address_native_publication_unavailable")


async def assert_native_address_input_fence(session, schema: str, expected: dict) -> None:
    """Hold source locks through caller cutover; changed rows, swaps or acceptance reject."""
    require_supported_native_address_inputs(expected)
    if await capture_native_address_input_fence(session, schema, cutover=True) != expected:
        raise RuntimeError("cms_address_native_inputs_changed")
