# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit exact ACA recipes only inside protected reference-export custody.

The protected schema comment is a retention receipt, not an accepted-event
inbox. Discovery, retention holds and refresh scheduling must use an independently
qualified notification store before this source can drive recurring publication.
"""

import hashlib
from uuid import UUID

from sqlalchemy import text

from process.mrf_publication_receipt import require_completed_publication
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import native_driver
from process.network_legacy_membership_source import PinnedACAMembershipSource, _require_archive
from process.reference_family_archive import (
    _canonical_json,
    _source_metadata,
    prepare_reference_family_activation,
    protected_publisher_owner,
    reference_family_profile_contract,
    reference_family_stage_schema,
)
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)

CONTRACT = "registry-aca-retained-recipe/v1"
_PREFIX = CONTRACT + ":"
_MEMBERSHIP_FIELDS = {
    "source_id",
    "release_id",
    "import_id",
    "issuer_id",
    "year",
    "source_url",
    "alias_scope",
    "reviewed_alias_sha256",
    "runtime_roles",
}


def _invalid():
    raise ValueError("registry_aca_source_recipe_invalid")


def _scope(metadata, runtime_roles):
    if not isinstance(metadata, dict) or set(metadata) != {"network_membership", "network_bindings"}:
        _invalid()
    membership = metadata["network_membership"]
    bindings = metadata["network_bindings"]
    if type(membership) is not dict or set(membership) != _MEMBERSHIP_FIELDS or type(bindings) is not dict:
        _invalid()
    if set(bindings) != {*RegistryNetworkSourceCoordinates.__dataclass_fields__, "source_key_kind"}:
        _invalid()
    coordinates = RegistryNetworkSourceCoordinates(
        **{key: value for key, value in bindings.items() if key != "source_key_kind"}
    )
    if (
        coordinates.source_system != "aca"
        or coordinates.source_id != membership["source_id"]
        or bindings["source_key_kind"] != "hios_plan_id"
        or type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 32
        or tuple(sorted(set(runtime_roles))) != runtime_roles
        or membership["runtime_roles"] != list(runtime_roles)
    ):
        _invalid()
    return coordinates, membership


def _bind_dataset(metadata, dataset_id):
    """Derive only the operation-owned physical identity; refuse conflicting claims."""
    if type(dataset_id) is not UUID or not dataset_id.int:
        _invalid()
    metadata = _source_metadata(metadata)[0]
    bindings = metadata.get("network_bindings")
    if type(bindings) is not dict:
        _invalid()
    physical_coordinates_map = {
        "dataset_schema": reference_family_stage_schema(dataset_id),
        "dataset_id": str(dataset_id),
    }
    if any(key in bindings and bindings[key] != value for key, value in physical_coordinates_map.items()):
        _invalid()
    if bool("dataset_schema" in bindings) != bool("dataset_id" in bindings):
        _invalid()
    return {**metadata, "network_bindings": {**bindings, **physical_coordinates_map}}


async def capture_registry_aca_source_metadata(session, metadata, *, schema, runtime_roles, dataset_id):
    """Bind explicit coordinates to the locked, completed summary publication."""
    metadata = _bind_dataset(metadata, dataset_id)
    _coordinates, membership = _scope(metadata, runtime_roles)
    completed = await require_completed_publication(session, schema, native_set=True)
    if completed["import_id"] != membership["import_id"]:
        _invalid()
    completion_map = {
        "attempt_id": str(completed["attempt_id"]),
        "import_id": completed["import_id"],
        "generation": completed["generation"],
        "summary_oid": completed["summary_oid"],
        "summary_inputs": completed["summary_inputs"],
        "address_content_sha256": hashlib.sha256(_canonical_json(completed["native_address_coverage"])).hexdigest(),
    }
    captured_metadata_map = {**metadata, "aca_completed_publication": completion_map}
    return _source_metadata(captured_metadata_map)[0]


def _recipe(prepared, validation, runtime_roles):
    metadata = dict(prepared.manifest.source_metadata)
    completion = metadata.pop("aca_completed_publication", None)
    coordinates, membership = _scope(metadata, runtime_roles)
    if (
        type(completion) is not dict
        or set(completion)
        != {"attempt_id", "import_id", "generation", "summary_oid", "summary_inputs", "address_content_sha256"}
        or prepared.manifest.importer_id != "mrf"
        or prepared.manifest.publication_authority != "tracked-generation"
        or prepared.manifest.source_serving_generation is None
        or completion["generation"]["serving_generation"] != prepared.manifest.source_serving_generation.as_dict()
        or completion["import_id"] != membership["import_id"]
        or coordinates.dataset_schema != prepared.ownership.schema_name
        or coordinates.dataset_id != str(prepared.ownership.dataset_id)
    ):
        _invalid()
    pin = PinnedACAMembershipSource(
        prepared,
        validation,
        **{key: value for key, value in membership.items() if key != "runtime_roles"},
        runtime_roles=runtime_roles,
    )
    return RegistrySourceMembershipRecipe(pin, coordinates)


async def admit_registry_aca_source_recipe(session, prepared, *, runtime_roles):
    """Validate and stamp one protected clone before its original lease CAS commits.

    The caller owns the source capture and clone transactions. Its precreation
    custody closes writes before COPY; no role grant or source approval occurs.
    Any validation, recording or cancellation failure rolls back this stamp with
    the operation's original clone transaction.
    """
    if not session.in_transaction():
        _invalid()
    owner_oid = await protected_publisher_owner(session)
    validation = await prepare_reference_family_activation(
        session,
        ownership=prepared.ownership,
        manifest=prepared.manifest,
        package_id=hashlib.sha256(_canonical_json(prepared.manifest.as_dict())).hexdigest(),
        profile_contract=reference_family_profile_contract(prepared.manifest),
        sealed_owner_oid=owner_oid,
    )
    recipe = _recipe(prepared, validation, runtime_roles)
    await _require_archive(await native_driver(session), recipe.source_pin)
    encoded = _PREFIX + canonical_registry_source_recipes((recipe,))
    current = await session.scalar(
        text("SELECT obj_description(CAST(:oid AS oid),'pg_namespace')"), {"oid": prepared.ownership.schema_oid}
    )
    if current not in (None, encoded):
        _invalid()
    await (await native_driver(session)).execute(
        f'COMMENT ON SCHEMA "{prepared.ownership.schema_name}" IS ' + "'" + encoded.replace("'", "''") + "'"
    )
    return recipe


async def require_registry_aca_source_recipe(session, prepared, *, runtime_roles):
    """Recheck the protected retained receipt and exact native archive after commit."""
    if not session.in_transaction():
        _invalid()
    encoded = await session.scalar(
        text("SELECT obj_description(CAST(:oid AS oid),'pg_namespace')"), {"oid": prepared.ownership.schema_oid}
    )
    if type(encoded) is not str or not encoded.startswith(_PREFIX):
        _invalid()
    recipes = decode_registry_source_recipes(encoded[len(_PREFIX) :])
    if len(recipes) != 1 or type(recipes[0].source_pin) is not PinnedACAMembershipSource:
        _invalid()
    recipe = recipes[0]
    if (
        recipe.source_pin.prepared != prepared
        or recipe != _recipe(prepared, recipe.source_pin.validation, runtime_roles)
        or encoded != _PREFIX + canonical_registry_source_recipes((recipe,))
    ):
        _invalid()
    await _require_archive(await native_driver(session), recipe.source_pin)
    return recipe
