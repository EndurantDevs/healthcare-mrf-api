# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Choose only accepted captures of the completed current native MRF publication."""

import json
from dataclasses import replace

from process.mrf_publication_receipt import SUMMARY_INPUTS
from process.network_address_projection import _identifier
from process.network_legacy_membership_source import with_aca_global_epoch, with_aca_successor
from process.reference_family_result_generation import (
    current_reference_family_relation_oids,
    read_reference_family_result_generation_authority,
)
from process.registry_aca_global_epoch import PinnedACAGlobalEpoch
from process.registry_aca_refresh_anchors import _accepted_original, _capture_groups
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe, canonical_registry_source_recipes


def _unavailable():
    raise RegistryRefreshAnchorUnavailable("registry_refresh_current_aca_unavailable")


async def _current_publication(session, driver, source_schema):
    namespace = _identifier(source_schema)
    publication = await driver.fetchrow(
        f"""SELECT contract_version,state,attempt_id::text,import_id,generation::text,
        summary_oid::bigint,summary_inputs::text FROM {namespace}.mrf_publication_receipt WHERE singleton"""
    )
    if publication is None or publication["contract_version"] != 1 or publication["state"] != "complete":
        _unavailable()
    generation = await read_reference_family_result_generation_authority(
        session, importer_id="mrf", schema_name=source_schema
    )
    physical = await current_reference_family_relation_oids(session, importer_id="mrf", schema_name=source_schema)
    if json.loads(publication["generation"]) != generation.as_dict() or generation.relation_oids != physical:
        _unavailable()
    completion_by_field = {name: publication[name] for name in ("attempt_id", "import_id", "summary_oid")}
    completion_by_field["generation"] = generation.as_dict()
    completion_by_field["summary_inputs"] = json.loads(publication["summary_inputs"])
    if type(completion_by_field["summary_inputs"]) is not dict or set(completion_by_field["summary_inputs"]) != set(
        SUMMARY_INPUTS
    ):
        _unavailable()
    names = [*completion_by_field["summary_inputs"], "plan_search_summary"]
    actual = await driver.fetch(
        "SELECT c.relname,c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=ANY($2::text[])",
        source_schema,
        names,
    )
    expected_by_name = {
        **completion_by_field["summary_inputs"],
        "plan_search_summary": completion_by_field["summary_oid"],
    }
    if (
        len(actual) != len(expected_by_name)
        or dict(actual) != expected_by_name
        or generation.serving_generation is None
    ):
        _unavailable()
    return completion_by_field, generation.serving_generation.as_dict()


async def _current_accepted(session, driver, store, completion, serving, budget):
    await store.qualify(driver, writing=False)
    headers = await driver.fetch(
        f"""WITH subjects AS (
          SELECT capture_id,octet_length(notification_json) AS raw_byte_count,
          CASE WHEN notification_json::jsonb->>'contract'='registry-aca-source-notification/v2'
            THEN (notification_json::jsonb->>'epoch_json')::jsonb
            ELSE ((notification_json::jsonb->>'recipe_json')::jsonb->0->'source_pin') END AS subject
          FROM {_identifier(store.control_schema)}.registry_aca_source_notification
        ) SELECT capture_id,raw_byte_count FROM subjects
          WHERE subject#>'{{prepared,manifest,source_serving_generation}}'=$1::jsonb
          AND coalesce(subject#>>'{{prepared,manifest,source_metadata,registry_aca_global_epoch,completed_publication,attempt_id}}',
            subject#>>'{{prepared,manifest,source_metadata,aca_completed_publication,attempt_id}}')=$2
          ORDER BY capture_id LIMIT 101""",
        json.dumps(serving),
        completion["attempt_id"],
    )
    if not headers or len(headers) > 100:
        _unavailable()
    accepted_subjects = []
    for header in headers:
        budget.reserve((dict(header),), 1)
        receipt = await store.accepted(session, str(header["capture_id"]))
        subject = receipt.get("epoch", receipt.get("recipe"))
        pin = subject if type(subject) is PinnedACAGlobalEpoch else subject.source_pin
        native = (
            pin.prepared.manifest.source_metadata["registry_aca_global_epoch"]["completed_publication"]
            if type(pin) is PinnedACAGlobalEpoch
            else pin.prepared.manifest.source_metadata.get("aca_completed_publication")
        )
        if type(native) is not dict or any(
            native.get(key) != expected_value for key, expected_value in completion.items()
        ):
            _unavailable()
        if pin.prepared.manifest.source_serving_generation.as_dict() != serving:
            _unavailable()
        accepted_subjects.append(subject)
    return tuple(accepted_subjects)


def _successor_for_subject(original, subject):
    if type(subject) is PinnedACAGlobalEpoch:
        return with_aca_global_epoch(original.source_pin, subject)
    if type(subject) is not RegistrySourceMembershipRecipe:
        _unavailable()
    current, anchor = subject.source_pin, original.source_pin
    if any(getattr(current, key) != getattr(anchor, key) for key in ("source_id", "issuer_id", "year", "alias_scope")):
        return None
    if subject.binding_coordinates.producer_id != original.binding_coordinates.producer_id:
        return None
    return with_aca_successor(anchor, current)


def _current_recipe(original, accepted):
    recipes_by_wire = {}
    for subject in accepted:
        try:
            source = _successor_for_subject(original, subject)
            if source is None:
                continue
            recipe = replace(original, source_pin=source)
            recipes_by_wire[canonical_registry_source_recipes((recipe,))] = recipe
        except ValueError:
            continue
    if len(recipes_by_wire) != 1:
        _unavailable()
    return next(iter(recipes_by_wire.values()))


async def resolve_accepted_aca_recipes(session, driver, bindings, *, readers, read_budget):
    """Require original accepted anchors and one unambiguous native current successor."""
    completion, serving = await _current_publication(session, driver, readers.aca_source_schema)
    accepted = await _current_accepted(session, driver, readers.aca_store, completion, serving, read_budget)
    scopes, recipes = {}, []
    for coordinates, selected in _capture_groups(bindings).items():
        pin = await _accepted_original(session, driver, readers.aca_store, coordinates, read_budget)
        original = RegistrySourceMembershipRecipe(pin, coordinates)
        recipes.append(_current_recipe(original, accepted))
        for binding in selected:
            scopes[binding.binding_id] = {
                "source_system": "aca",
                "source_id": coordinates.source_id,
                "dataset_schema": coordinates.dataset_schema,
                "producer_id": coordinates.producer_id,
                "source_key": binding.record_json["source_key"],
                "approved_binding_sha256": binding.approved_binding_sha256,
                "capture_generation": pin.generation_id,
            }
    return scopes, tuple(recipes)
