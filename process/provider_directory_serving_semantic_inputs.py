# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Project existing immutable inputs for conservative ordinary unchanged detection."""

import datetime
import hashlib
import importlib
import json
import re
from collections.abc import Mapping

from sqlalchemy import text

from process import entity_address_dependency_bindings as dependencies

CONTRACT = "provider-directory-serving-semantic-inputs.v1"
_HASH_FIELDS = frozenset(
    {
        "source_vector_sha256",
        "source_context_sha256",
        "profile_input_digest",
        "address_recipe_sha256",
        "native_dependencies_sha256",
        "inference_inputs_sha256",
        "facility_inputs_sha256",
        "alias_state_sha256",
        "address_mapper_sha256",
    }
)
_FIELDS = _HASH_FIELDS | {
    "contract_id",
    "profile_as_of",
    "profile_schema_version",
    "profile_strategy_version",
    "healthcare_source_commit",
    "dependency_proofs_complete",
}
_FAMILIES = {
    "npi": ("npi_address",),
    "mrf-address": ("mrf_address",),
    "cms-doctors": ("doctor_clinician_address",),
    "geo": ("geo_zip_lookup",),
    "tiger": ("zip_state", "zcta5"),
}


def _require(condition):
    if not condition:
        raise ValueError("provider_directory_serving_semantic_inputs_invalid")


def _canonical(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(name, value):
    return hashlib.sha256((CONTRACT + ":" + name + "\0" + _canonical(value)).encode("ascii")).hexdigest()


def validated_semantic_inputs(raw):
    """Validate the closed journal projection; this value alone conveys no freshness authority."""
    _require(isinstance(raw, Mapping) and set(raw) == _FIELDS and raw["contract_id"] == CONTRACT)
    result = json.loads(_canonical(raw))
    _require(all(type(result[name]) is str and re.fullmatch(r"[0-9a-f]{64}", result[name]) for name in _HASH_FIELDS))
    _require(type(result["dependency_proofs_complete"]) is bool)
    _require(type(result["profile_schema_version"]) is int and 0 < result["profile_schema_version"] < 2**31)
    _require(type(result["profile_strategy_version"]) is str and 0 < len(result["profile_strategy_version"]) <= 256)
    _require(
        type(result["healthcare_source_commit"]) is str
        and re.fullmatch(r"[0-9a-f]{40}", result["healthcare_source_commit"])
    )
    _require(result["healthcare_source_commit"] != "0" * 40)
    as_of = result["profile_as_of"]
    if as_of is not None:
        _require(type(as_of) is str and datetime.date.fromisoformat(as_of).isoformat() == as_of)
    return result


def semantic_inputs(execution, affected, native):
    """Exclude predecessor outputs, actor state and requested scope from the semantic comparison."""
    from api import ptg2_geo_projection as geo
    from process.provider_directory_profile_runtime_observation import build_baked_healthcare_source_commit

    address = importlib.import_module("process.entity_address_unified")
    proof = execution.attestation
    proofs = native["semantic_dependency_proofs"]
    _require(set(proofs) == set(_FAMILIES))
    components_by_name = {
        "source_vector": proof.payload["pairs"],
        "source_context": affected["desired_context"],
        "address_recipe": {
            "refresh_mode": "provider-directory-partial",
            "serving_only_refresh": True,
            "test_mode": False,
            "inference_enabled": affected["inference_enabled"],
            "facility_recipe": affected["facility_recipe"],
        },
        "native_dependencies": {"bindings": native["current_dependency_bindings"], "proofs": proofs},
        "inference_inputs": native["inference_inputs"],
        "facility_inputs": native["facility_inputs"],
        "alias_state": native["observed"]["alias_state"],
        "address_mapper": {
            "archive_identity_version": address.ARCHIVE_IDENTITY_VERSION,
            "base_address_version": address.BASE_ADDRESS_VERSION,
            "alias_base_address_version_prefix": address.ALIAS_BASE_ADDRESS_VERSION_PREFIX,
            "geo_assurance_version": geo.GEO_ASSURANCE_VERSION,
        },
    }
    return validated_semantic_inputs(
        {
            "contract_id": CONTRACT,
            "profile_as_of": proof.desired_profile_as_of,
            "profile_schema_version": proof.profile_schema_version,
            "profile_strategy_version": proof.profile_strategy_version,
            "profile_input_digest": proof.profile_input_digest,
            "healthcare_source_commit": build_baked_healthcare_source_commit(),
            "dependency_proofs_complete": all(component_value is not None for component_value in proofs.values()),
            **{
                name + "_sha256": _digest(name, component_value) for name, component_value in components_by_name.items()
            },
        }
    )


async def _canonical_dependency_proof(session, schema, family):
    """Use an existing published observer; untracked legacy data cannot prove a no-op."""
    from process import npi_result_generation as npi
    from process import reference_family_result_generation as reference
    from process.provider_directory_cms_native_inputs import _npi_revision

    namespace = "tiger" if family == "tiger" else schema
    ledger_schema = schema if family != "tiger" else reference._authority_schema(family, namespace)
    ledger_name = npi.TABLE_NAME if family == "npi" else reference.TABLE_NAME
    ledger = f'"{ledger_schema}"."{ledger_name}"'
    if not await session.scalar(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": ledger}):
        return None
    predicate = "singleton IS TRUE" if family == "npi" else "importer_id=:family"
    if not await session.scalar(
        text(f"SELECT EXISTS(SELECT 1 FROM {ledger} WHERE {predicate} AND published_at IS NOT NULL)"),
        {"family": family},
    ):
        return None
    if family == "npi":
        guarded = await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname=:name AND NOT tgisinternal AND tgrelid=to_regclass(:relation))"
            ),
            {"name": npi.REVISION_TRIGGER, "relation": f'"{schema}"."npi_address"'},
        )
        return {"kind": "revision", "identity": await _npi_revision(session, schema)} if guarded else None
    tracked = await session.scalar(
        text(f"SELECT source_revision_tracked FROM {ledger} WHERE importer_id=:family"), {"family": family}
    )
    if family in {"mrf-address", "tiger"} and tracked is not True or family == "geo" and tracked is not False:
        return None
    observed = await reference.capture_reference_family_serving_generation(
        session, importer_id=family, schema_name=namespace
    )
    return {"kind": "revision" if tracked else "immutable", "identity": observed.as_dict()}


async def capture_semantic_dependencies(session, schema, selected_relations):
    """The selected map was authenticated by the original protected-selection observer."""
    schema = dependencies.validate_schema_name(schema)
    proof_by_family = {}
    for family, names in _FAMILIES.items():
        family_relations = [selected_relations[name] for name in names if name in selected_relations]
        _require(not family_relations or len(family_relations) == len(names))
        proof_by_family[family] = (
            {"kind": "immutable", "identity": family_relations}
            if family_relations
            else await _canonical_dependency_proof(session, schema, family)
        )
    return proof_by_family


async def lock_semantic_dependencies(session, schema, capture):
    """Keep each used revision family's full input generation stable through commit."""
    from process import npi_result_generation as npi
    from process import reference_family_result_generation as reference

    schema = dependencies.validate_schema_name(schema)
    native = capture.predecessor["native"]
    proofs = native["semantic_dependency_proofs"]
    bindings = native["current_dependency_bindings"]
    physical_lock = dependencies.projection.projection_dependency_lock_sql(schema, dependency_bindings=bindings)
    await session.execute(text(physical_lock.rstrip().removesuffix(";") + " NOWAIT"))
    relation_refs = set()
    for family in _FAMILIES:
        if proofs[family] is None or proofs[family]["kind"] != "revision":
            continue
        names = npi.RELATION_NAMES if family == "npi" else reference.RELATION_NAMES_BY_IMPORTER[family]
        namespace = "tiger" if family == "tiger" else schema
        relation_refs.update(f'"{namespace}"."{name}"' for name in names)
    if relation_refs:
        await session.execute(text("LOCK TABLE " + ",".join(sorted(relation_refs)) + " IN SHARE MODE NOWAIT"))
