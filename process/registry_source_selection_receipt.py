# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain bounded reviewed source-selection counts on an isolated candidate."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from uuid import UUID

import asyncpg

from process.network_approved_membership_source import ApprovedMembershipSource, pin_approved_membership_source
from process.network_membership_candidate_lifecycle import _control_namespace, _locked_candidate, _require_transaction
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_recipe_store import (
    MAX_SOURCE_RECIPE_BYTES,
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
    verify_registry_source_recipes,
)
from process.registry_source_retention import RegistrySourceRetentionError, verify_registry_source_retention

SUMMARY_KEY = "registry_source_selection"
MAX_RECEIPT_BYTES = 4096
_MAX_COUNTER = 2**63 - 1
_FIELDS = {
    "component",
    "revision",
    "selection_policy",
    "recipe_sha256",
    "recipe_count",
    "approved_revision",
    "approved_generation_sha256",
    "stage_generation_sha256",
    "mapped_rows",
    "omitted_rows",
}


class RegistrySourceSelectionError(ValueError):
    """The selection receipt, source pin or native prerequisites are invalid."""


class RegistrySourceSelectionConflict(RegistrySourceSelectionError):
    """An immutable candidate recipe or previously retained receipt differs."""


def _unique_object(pairs):
    fields_by_name = {}
    for name, value in pairs:
        if name in fields_by_name:
            raise RegistrySourceSelectionError("registry_source_selection_invalid")
        fields_by_name[name] = value
    return fields_by_name


def _canonical(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _inventory_root(root):
    if root is None:
        return None
    from dataclasses import asdict

    from process.registry_source_inventory_contract import SourceInventoryRoot

    if type(root) is not SourceInventoryRoot:
        raise RegistrySourceSelectionError("registry_source_selection_invalid")
    return SourceInventoryRoot(**asdict(root))


def _source_metadata(candidate):
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_recipe_store import MAX_SOURCE_RECIPE_INPUT_BYTES

    document = candidate["source_recipes_json"]
    if type(document) is str and (
        len(document) > MAX_SOURCE_RECIPE_INPUT_BYTES or len(document.encode()) > MAX_SOURCE_RECIPE_INPUT_BYTES
    ):
        raise RegistrySourceSelectionError("registry_source_selection_invalid")
    return verify_source_recipe_metadata(candidate)


def validate_registry_source_selection_receipt(receipt_by_field, *, inventory_root=None) -> dict:
    """Accept only the closed bounded summary; no descriptors or row data occur in it."""
    try:
        root = _inventory_root(inventory_root)
        if type(receipt_by_field) is str:
            if len(receipt_by_field) > MAX_RECEIPT_BYTES or len(receipt_by_field.encode()) > MAX_RECEIPT_BYTES:
                raise ValueError
            receipt_by_field = json.loads(receipt_by_field, object_pairs_hook=_unique_object)
        fields = _FIELDS if root is None else _FIELDS | {"inventory_entries_sha256"}
        if type(receipt_by_field) is not dict or set(receipt_by_field) != fields:
            raise ValueError
        if (
            type(receipt_by_field["component"]) is not str
            or receipt_by_field["component"] != SUMMARY_KEY
            or type(receipt_by_field["selection_policy"]) is not str
            or receipt_by_field["selection_policy"] != "approved-bindings-only"
        ):
            raise ValueError
        if type(receipt_by_field["revision"]) is not int or receipt_by_field["revision"] != (1 if root is None else 2):
            raise ValueError
        for field in ("recipe_count", "approved_revision", "mapped_rows", "omitted_rows"):
            if type(receipt_by_field[field]) is not int or not 0 <= receipt_by_field[field] <= _MAX_COUNTER:
                raise ValueError
        if (
            not 1 <= receipt_by_field["recipe_count"] <= (100 if root is None else 1000)
            or receipt_by_field["mapped_rows"] + receipt_by_field["omitted_rows"] > _MAX_COUNTER
        ):
            raise ValueError
        if root is not None and (
            receipt_by_field["recipe_count"] != root.entry_count
            or receipt_by_field["recipe_sha256"] != root.recipes_sha256
            or receipt_by_field["inventory_entries_sha256"] != root.entries_sha256
        ):
            raise ValueError
        for field in ("recipe_sha256", "approved_generation_sha256", "stage_generation_sha256"):
            if (
                type(receipt_by_field[field]) is not str
                or re.fullmatch(r"[0-9a-f]{64}", receipt_by_field[field]) is None
            ):
                raise ValueError
        if len(_canonical(receipt_by_field).encode()) > MAX_RECEIPT_BYTES:
            raise ValueError
        return dict(receipt_by_field)
    except ValueError, TypeError, UnicodeError, RecursionError:
        raise RegistrySourceSelectionError("registry_source_selection_invalid") from None


def _recipe_identity(recipes, root):
    if root is None:
        recipe_json = canonical_registry_source_recipes(recipes)
        return len(recipes), registry_source_recipes_sha256(recipe_json)
    from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root

    actual = source_inventory_root(
        make_source_inventory_entry(index, recipe) for index, recipe in enumerate(recipes, 1)
    )
    if (actual.entry_count, actual.recipes_sha256) != (root.entry_count, root.recipes_sha256):
        raise RegistrySourceSelectionError("registry_source_selection_invalid")
    return actual.entry_count, actual.recipes_sha256


def _stage_matches(stage, approved_source, recipe_count, recipe_sha256):
    if type(approved_source) is not ApprovedMembershipSource or type(stage) is not RegistrySourceRecipeStage:
        raise ValueError
    if (
        type(stage.table_name) is not str
        or re.fullmatch(r"registry_source_page_[0-9a-f]{32}", stage.table_name) is None
    ):
        raise ValueError
    if stage.membership_rows + approved_source.total_rows > _MAX_COUNTER:
        raise ValueError
    if (
        type(stage.source_generations) is not tuple
        or len(stage.source_generations) != recipe_count
        or any(
            type(generation) is not str or re.fullmatch(r"[0-9a-f]{64}", generation) is None
            for generation in stage.source_generations
        )
    ):
        raise ValueError
    stage_identity = json.dumps(
        [
            recipe_sha256,
            approved_source.generation_id,
            sorted(stage.source_generations),
            stage.membership_rows,
            stage.omitted_rows,
        ],
        separators=(",", ":"),
    )
    if hashlib.sha256(stage_identity.encode()).hexdigest() != stage.generation_sha256:
        raise ValueError


def _selected_stage(stage):
    """Keep selection commitments on current inputs when the same heap also holds carry."""
    if type(stage) is not RegistrySourceRecipeStage:
        raise ValueError
    selected = stage.selected_stage
    if selected is None:
        return stage
    if (
        type(selected) is not RegistrySourceRecipeStage
        or selected.selected_stage is not None
        or selected.table_name != stage.table_name
    ):
        raise ValueError
    return selected


def build_registry_source_selection_receipt(recipes, approved_source, stage, *, inventory_root=None) -> dict:
    """Bind actual reviewed counts and recipes; inventory metadata grants no SOURCE authority."""
    try:
        root = _inventory_root(inventory_root)
        recipe_count, recipe_sha256 = _recipe_identity(recipes, root)
        stage = _selected_stage(stage)
        _stage_matches(stage, approved_source, recipe_count, recipe_sha256)
        return validate_registry_source_selection_receipt(
            {
                "component": SUMMARY_KEY,
                "revision": 1 if root is None else 2,
                "selection_policy": "approved-bindings-only",
                "recipe_sha256": recipe_sha256,
                "recipe_count": recipe_count,
                "approved_revision": approved_source.approved_revision,
                "approved_generation_sha256": approved_source.generation_id,
                "stage_generation_sha256": stage.generation_sha256,
                "mapped_rows": stage.membership_rows,
                "omitted_rows": stage.omitted_rows,
                **({"inventory_entries_sha256": root.entries_sha256} if root is not None else {}),
            },
            inventory_root=root,
        )
    except ValueError, TypeError, UnicodeError, RecursionError:
        raise RegistrySourceSelectionError("registry_source_selection_invalid") from None


def _generations(candidate):
    generations_by_source = candidate["source_generations"]
    if type(generations_by_source) is str:
        if len(generations_by_source) > 1048576 or len(generations_by_source.encode()) > 1048576:
            raise RegistrySourceSelectionError("registry_source_selection_invalid")
        generations_by_source = json.loads(generations_by_source, object_pairs_hook=_unique_object)
    if type(generations_by_source) is not dict:
        raise RegistrySourceSelectionError("registry_source_selection_invalid")
    return generations_by_source


def _report(candidate):
    report_by_name = candidate["validation_json"]
    if type(report_by_name) is str:
        if len(report_by_name) > 1048576 or len(report_by_name.encode()) > 1048576:
            raise RegistrySourceSelectionError("registry_source_selection_invalid")
        report_by_name = json.loads(report_by_name, object_pairs_hook=_unique_object)
    if report_by_name is None:
        return {}
    if type(report_by_name) is not dict:
        raise RegistrySourceSelectionError("registry_source_selection_invalid")
    return report_by_name


def _is_candidate_match(candidate, receipt, recipe_count):
    generations_by_source = _generations(candidate)
    return (
        receipt["recipe_count"] == recipe_count
        and candidate["approved_custom_revision"] == receipt["approved_revision"]
        and generations_by_source.get(RECIPE_DIGEST_KEY) == receipt["recipe_sha256"]
        and generations_by_source.get("custom_membership") == receipt["approved_generation_sha256"]
        and generations_by_source.get("registry_source_membership") == receipt["stage_generation_sha256"]
    )


def _registered_source_rows(candidate, receipt):
    from process.registry_fhir_serving_preservation import verify_fhir_serving_preservation

    preservation = verify_fhir_serving_preservation(candidate)
    count = receipt["mapped_rows"] + (preservation["carried_rows"] if preservation is not None else 0)
    if count > _MAX_COUNTER:
        raise RegistrySourceSelectionError("registry_source_selection_invalid")
    return count


def verify_registry_source_selection_receipt(candidate) -> dict | None:
    """Recheck staged receipts; older unstaged manifests preserve their original shape."""
    try:
        if not isinstance(candidate, Mapping):
            raise ValueError
        report_by_name = _report(candidate)
        generations_by_source = _generations(candidate)
        if SUMMARY_KEY not in report_by_name and "registry_source_membership" not in generations_by_source:
            return None
        metadata = _source_metadata(candidate)
        root = None if metadata.inventory is None else metadata.inventory.root
        receipt = validate_registry_source_selection_receipt(report_by_name[SUMMARY_KEY], inventory_root=root)
        retention = verify_registry_source_retention(candidate)
        if not _is_candidate_match(candidate, receipt, metadata.recipe_count) or (
            retention is not None and _registered_source_rows(candidate, receipt) != retention["source_rows"]
        ):
            raise ValueError
        return receipt
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceSelectionError("registry_source_selection_invalid") from None


def _is_candidate_replay(candidate, recipes, receipt, approved_source):
    metadata = _source_metadata(candidate)
    root = None if metadata.inventory is None else metadata.inventory.root
    if root is None:
        stored = canonical_registry_source_recipes(verify_registry_source_recipes(candidate))
        is_recipe_match = stored == canonical_registry_source_recipes(recipes)
    else:
        receipt = validate_registry_source_selection_receipt(receipt, inventory_root=root)
        is_recipe_match = (receipt["recipe_count"], receipt["recipe_sha256"]) == (root.entry_count, root.recipes_sha256)
    try:
        retention = verify_registry_source_retention(candidate)
    except RegistrySourceRetentionError:
        raise RegistrySourceSelectionConflict("registry_source_selection_conflict") from None
    if retention is None:
        expected_rows = _registered_source_rows(candidate, receipt) + approved_source.total_rows
    else:
        expected_rows = retention["effective_rows"]
        if _registered_source_rows(candidate, receipt) != retention["source_rows"] or approved_source.total_rows != (
            retention["add_rows"] + retention["remove_rows"]
        ):
            raise RegistrySourceSelectionConflict("registry_source_selection_conflict")
    if (
        candidate["state"] != "open"
        or not is_recipe_match
        or not _is_candidate_match(candidate, receipt, metadata.recipe_count)
        or candidate["expected_rows"] != expected_rows
    ):
        raise RegistrySourceSelectionConflict("registry_source_selection_conflict")
    report_by_name = _report(candidate)
    if SUMMARY_KEY in report_by_name:
        retained = validate_registry_source_selection_receipt(report_by_name[SUMMARY_KEY], inventory_root=root)
        if retained != receipt:
            raise RegistrySourceSelectionConflict("registry_source_selection_conflict")
        return True
    return False


async def record_registry_source_selection(
    connection,
    copy_target,
    recipes,
    approved_source,
    stage,
    *,
    control_schema=None,
) -> dict:
    """Write once to an open owned candidate, preserving ordinary validation fields.

    The caller owns a repeatable-read or serializable transaction and the actual
    temporary source stage. This stores extraction evidence, never admission or
    publication authority. Closed candidates cannot acquire or change receipts.
    """
    try:
        legacy_receipt = _bounded_legacy_receipt(recipes, approved_source, stage)
        _require_transaction(connection, copy_target)
        namespace = _control_namespace(control_schema)
        async with connection.transaction():
            actual = await pin_approved_membership_source(
                connection,
                approved_revision=approved_source.approved_revision,
                control_schema=control_schema,
            )
            if actual != approved_source:
                raise RegistrySourceSelectionConflict("registry_source_selection_conflict")
            candidate = await _locked_candidate(connection, copy_target, namespace)
            metadata = _source_metadata(candidate)
            root = None if metadata.inventory is None else metadata.inventory.root
            receipt = (
                legacy_receipt
                if root is None and legacy_receipt is not None
                else build_registry_source_selection_receipt(recipes, approved_source, stage, inventory_root=root)
            )
            is_replay = _is_candidate_replay(candidate, recipes, receipt, approved_source)
            count = await connection.fetchval(f'SELECT count(*) FROM pg_temp."{stage.table_name}"')
            if count != _registered_source_rows(candidate, receipt):
                raise RegistrySourceSelectionConflict("registry_source_selection_conflict")
            if not is_replay:
                await connection.execute(
                    f"UPDATE {namespace}.network_membership_candidate SET validation_json="
                    "jsonb_set(coalesce(validation_json,'{}'::jsonb),ARRAY[$2::text],$3::jsonb) WHERE candidate_id=$1",
                    UUID(copy_target.candidate_id),
                    SUMMARY_KEY,
                    _canonical(receipt),
                )
        return receipt
    except RegistrySourceSelectionError:
        raise
    except asyncpg.PostgresError as error:
        raise RegistrySourceSelectionError("registry_source_selection_unavailable") from error
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
        raise RegistrySourceSelectionError("registry_source_selection_invalid") from error


def _bounded_legacy_receipt(recipes, approved_source, stage):
    legacy_receipt = None
    if type(recipes) in {tuple, list} and len(recipes) <= 100:
        recipe_bytes = 2
        for index, recipe in enumerate(recipes):
            recipe_bytes += len(canonical_registry_source_recipes((recipe,)).encode()) - 2 + int(index > 0)
            if recipe_bytes > MAX_SOURCE_RECIPE_BYTES:
                break
        else:
            legacy_receipt = build_registry_source_selection_receipt(recipes, approved_source, stage)
    return legacy_receipt
