# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observe the existing bounded facility recipe and hold its exact native reads."""

import importlib
from types import SimpleNamespace

from sqlalchemy import text

from process import entity_address_dependency_bindings as dependencies

_ENROLLMENT = ("provider_enrollment_hospital", "provider_enrollment_fqhc", "provider_enrollment_ffs_additional_npi")
_NPI = ("npi", "npi_address", "npi_taxonomy", "npi_other_identifier")
_NAMES = (*_ENROLLMENT, *_NPI, "facility_anchor", "nucc_taxonomy")


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_ordinary_facility_inputs_changed")


async def _relations(session, schema):
    schema = dependencies.validate_schema_name(schema)
    rows = (
        (
            await session.execute(
                text("""
        SELECT requested.name,c.oid::bigint AS relation_oid,c.relkind::text AS kind,
               c.relpersistence::text AS persistence,c.relowner::bigint AS owner_oid,
               pg_relation_filenode(c.oid)::bigint AS relfilenode,
               c.relrowsecurity OR c.relforcerowsecurity AS row_security,
               EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=c.oid AND attnum>0
                      AND NOT attisdropped AND attname='address_key') AS address_key,
               EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=c.oid AND attnum>0
                      AND NOT attisdropped AND attname='medicare_ccn') AS medicare_ccn,
               c.relowner=(SELECT nspowner FROM pg_namespace WHERE nspname='hp_snapshot_retention') AS protected_owner
          FROM unnest(CAST(:names AS text[])) requested(name)
          LEFT JOIN pg_class c ON c.oid=to_regclass(format('%I.%I',CAST(:schema AS text),requested.name))
         ORDER BY requested.name
    """),
                {"names": list(_NAMES), "schema": schema},
            )
        )
        .mappings()
        .all()
    )
    _require(len(rows) == len(_NAMES) and {row["name"] for row in rows} == set(_NAMES))
    return {row["name"]: None if row["relation_oid"] is None else {"schema": schema, **dict(row)} for row in rows}


def _recipe(relations_by_name):
    native = importlib.import_module("process.entity_address_unified")
    available_by_name = {name: relation is not None for name, relation in relations_by_name.items()}
    for name, column in (("npi_address", "address_key"), ("facility_anchor", "medicare_ccn")):
        available_by_name[name + "." + column] = bool(relations_by_name[name] and relations_by_name[name][column])
    return {
        "enabled": native._is_env_enabled(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_FACILITY_CANDIDATES", native.DEFAULT_BUILD_FACILITY_CANDIDATES
        )
        and available_by_name["facility_anchor"]
        and available_by_name["facility_anchor.medicare_ccn"],
        "include_nppes": native._is_env_enabled("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES", False),
        "include_other_identifier": native._is_env_enabled(
            "HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_OTHER_IDENTIFIER", False
        ),
        "candidate_limit": native._env_int("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_LIMIT", 25, minimum=1),
        "options": native._facility_candidate_options(SimpleNamespace(available=available_by_name)),
    }


def _used_names(recipe, inference_enabled=False):
    names = set(_NPI[:3]) | {"nucc_taxonomy"} if inference_enabled else set()
    if not recipe["enabled"]:
        return tuple(sorted(names))
    names.add("facility_anchor")
    options = recipe["options"]
    name_by_option = {
        "include_hospital_enrollment": _ENROLLMENT[0],
        "include_fqhc_enrollment": _ENROLLMENT[1],
        "include_provider_additional_npi": _ENROLLMENT[2],
        "include_npi_taxonomy": "npi_taxonomy",
        "include_nucc_taxonomy": "nucc_taxonomy",
        "include_npi_other_identifier": "npi_other_identifier",
    }
    names.update(name for option, name in name_by_option.items() if options[option])
    if options["include_npi_address_key"] or options["include_npi_registry"]:
        names.update(("npi", "npi_address"))
    return tuple(sorted(names))


async def _enrollment_identity(session, schema, relations_by_name):
    from process import provider_directory_cms_native_inputs as native_inputs
    from process.entity_address_snapshot_receipt import _row_identity

    ledger = f'"{schema}"."cms_native_input_revision"'
    exists = await session.scalar(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": ledger})
    identities_by_name = {}
    for name, relation in relations_by_name.items():
        registered = exists and await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {ledger} WHERE relation_oid=:oid)"), {"oid": relation["relation_oid"]}
        )
        if registered:
            revisions = await native_inputs._revision_guards(session, schema, {f'"{schema}"."{name}"': relation})
            identities_by_name[name] = {"revision": revisions}
        else:
            count, digest = await _row_identity(session, schema, name)
            identities_by_name[name] = {"row_count": count, "row_sha256": digest}
    return identities_by_name


async def _semantic_inputs(session, schema, relations_by_name):
    from process import provider_directory_cms_native_inputs as native_inputs
    from process import reference_family_result_generation as reference

    identities_by_family = {}
    if set(relations_by_name).intersection(_NPI):
        identities_by_family["npi"] = await native_inputs._npi_revision(session, schema)
    if "facility_anchor" in relations_by_name:
        authority = await reference.capture_reference_family_serving_generation(
            session, importer_id="facility-anchors", schema_name=schema
        )
        identities_by_family["facility"] = authority.as_dict()
    if "nucc_taxonomy" in relations_by_name:
        identities_by_family["nucc"] = await dependencies._capture_nucc_generation(
            session, relations_by_name["nucc_taxonomy"]
        )
    identities_by_family["enrollment"] = await _enrollment_identity(
        session, schema, {name: relation for name, relation in relations_by_name.items() if name in _ENROLLMENT}
    )
    return identities_by_family


async def capture_facility_inputs(session, schema, selected_relations, *, inference_enabled=False):
    """Read the generator's actual canonical roots, never substitute unrelated selected OIDs."""
    del selected_relations
    observed_by_name = await _relations(session, schema)
    recipe = _recipe(observed_by_name)
    inference_available = inference_enabled and all(observed_by_name[name] for name in (*_NPI[:3], "nucc_taxonomy"))
    used_by_name = {name: observed_by_name[name] for name in _used_names(recipe, inference_available)}
    _require(
        all(
            value and value["persistence"] == "p" and not value["row_security"] and value["kind"] in {"r", "p"}
            for value in used_by_name.values()
        )
    )
    inputs_by_field = {
        "inference_available": inference_available,
        "relations": used_by_name,
        "identities": await _semantic_inputs(session, schema, used_by_name),
    }
    return recipe, inputs_by_field


async def _lock_inputs(session, schema, capture):
    inputs = capture.predecessor["native"]["facility_inputs"]
    names = set(inputs["relations"])
    _require(names == set(_used_names(capture.affected_scope["facility_recipe"], inputs["inference_available"])))
    _require(names <= set(_NAMES))
    if names.intersection(_NPI):
        from process.npi_result_generation import RELATION_NAMES, TABLE_NAME

        names.update((*RELATION_NAMES, TABLE_NAME))
    if "facility_anchor" in names:
        names.update(("facility_address_contribution", "reference_family_result_generation"))
    if any("revision" in value for value in inputs["identities"]["enrollment"].values()):
        names.add("cms_native_input_revision")
    if names:
        namespace = dependencies.validate_schema_name(schema)
        await session.execute(
            text(
                "LOCK TABLE " + ",".join(f'"{namespace}"."{name}"' for name in sorted(names)) + " IN SHARE MODE NOWAIT"
            )
        )


async def require_facility_inputs(session, schema, capture):
    """Hold the same native reads through the caller-owned generator transaction."""
    _require(session.in_transaction())
    await _lock_inputs(session, schema, capture)
    recipe, inputs = await capture_facility_inputs(
        session, schema, {}, inference_enabled=capture.affected_scope["inference_enabled"]
    )
    _require(recipe == capture.affected_scope["facility_recipe"])
    _require(inputs == capture.predecessor["native"]["facility_inputs"])


async def lock_ordinary_input_dependencies(fhir, capture):
    """Keep exact native inputs locked until the original common publication commits."""
    from process.provider_directory_artifact_bundle_preparation import CapturedServingInputs

    _require(
        type(capture) is CapturedServingInputs and capture.database is fhir.db and capture._engine is fhir.db.engine
    )
    binding = fhir.db._transaction_binding()
    _require(binding is not None and binding.session.in_transaction())
    _require(await binding.session.scalar(text("SHOW transaction_read_only")) == "off")
    from process.provider_directory_serving_semantic_inputs import lock_semantic_dependencies

    await lock_semantic_dependencies(binding.session, fhir._schema(), capture)
    await _lock_inputs(binding.session, fhir._schema(), capture)
