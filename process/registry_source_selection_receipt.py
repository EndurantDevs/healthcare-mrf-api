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
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
    verify_registry_source_recipes,
)

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


def validate_registry_source_selection_receipt(receipt_by_field) -> dict:
    """Accept only the closed bounded summary; no descriptors or row data occur in it."""
    try:
        if type(receipt_by_field) is str:
            if len(receipt_by_field.encode()) > MAX_RECEIPT_BYTES:
                raise ValueError
            receipt_by_field = json.loads(receipt_by_field, object_pairs_hook=_unique_object)
        if type(receipt_by_field) is not dict or set(receipt_by_field) != _FIELDS:
            raise ValueError
        if (
            type(receipt_by_field["component"]) is not str
            or receipt_by_field["component"] != SUMMARY_KEY
            or type(receipt_by_field["selection_policy"]) is not str
            or receipt_by_field["selection_policy"] != "approved-bindings-only"
        ):
            raise ValueError
        if type(receipt_by_field["revision"]) is not int or receipt_by_field["revision"] != 1:
            raise ValueError
        for field in ("recipe_count", "approved_revision", "mapped_rows", "omitted_rows"):
            if type(receipt_by_field[field]) is not int or not 0 <= receipt_by_field[field] <= _MAX_COUNTER:
                raise ValueError
        if (
            not 1 <= receipt_by_field["recipe_count"] <= 100
            or receipt_by_field["mapped_rows"] + receipt_by_field["omitted_rows"] > _MAX_COUNTER
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


def build_registry_source_selection_receipt(recipes, approved_source, stage) -> dict:
    """Bind actual reviewed extraction counts to its immutable source identities."""
    try:
        if type(approved_source) is not ApprovedMembershipSource or type(stage) is not RegistrySourceRecipeStage:
            raise ValueError
        if (
            type(stage.table_name) is not str
            or re.fullmatch(r"registry_source_page_[0-9a-f]{32}", stage.table_name) is None
        ):
            raise ValueError
        recipe_json = canonical_registry_source_recipes(recipes)
        if stage.membership_rows + approved_source.total_rows > _MAX_COUNTER:
            raise ValueError
        if (
            type(stage.source_generations) is not tuple
            or len(stage.source_generations) != len(recipes)
            or any(
                type(generation) is not str or re.fullmatch(r"[0-9a-f]{64}", generation) is None
                for generation in stage.source_generations
            )
        ):
            raise ValueError
        recipe_sha256 = registry_source_recipes_sha256(recipe_json)
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
        return validate_registry_source_selection_receipt(
            {
                "component": SUMMARY_KEY,
                "revision": 1,
                "selection_policy": "approved-bindings-only",
                "recipe_sha256": recipe_sha256,
                "recipe_count": len(recipes),
                "approved_revision": approved_source.approved_revision,
                "approved_generation_sha256": approved_source.generation_id,
                "stage_generation_sha256": stage.generation_sha256,
                "mapped_rows": stage.membership_rows,
                "omitted_rows": stage.omitted_rows,
            }
        )
    except ValueError, TypeError, UnicodeError, RecursionError:
        raise RegistrySourceSelectionError("registry_source_selection_invalid") from None


def _generations(candidate):
    generations_by_source = candidate["source_generations"]
    if type(generations_by_source) is str:
        generations_by_source = json.loads(generations_by_source, object_pairs_hook=_unique_object)
    if type(generations_by_source) is not dict:
        raise RegistrySourceSelectionError("registry_source_selection_invalid")
    return generations_by_source


def _report(candidate):
    report_by_name = candidate["validation_json"]
    if type(report_by_name) is str:
        if len(report_by_name.encode()) > 1048576:
            raise RegistrySourceSelectionError("registry_source_selection_invalid")
        report_by_name = json.loads(report_by_name, object_pairs_hook=_unique_object)
    if report_by_name is None:
        return {}
    if type(report_by_name) is not dict:
        raise RegistrySourceSelectionError("registry_source_selection_invalid")
    return report_by_name


def _is_candidate_match(candidate, receipt, recipes):
    generations_by_source = _generations(candidate)
    return (
        receipt["recipe_count"] == len(recipes)
        and candidate["approved_custom_revision"] == receipt["approved_revision"]
        and generations_by_source.get(RECIPE_DIGEST_KEY) == receipt["recipe_sha256"]
        and generations_by_source.get("custom_membership") == receipt["approved_generation_sha256"]
        and generations_by_source.get("registry_source_membership") == receipt["stage_generation_sha256"]
    )


def verify_registry_source_selection_receipt(candidate) -> dict | None:
    """Recheck staged receipts; older unstaged manifests preserve their original shape."""
    try:
        if not isinstance(candidate, Mapping):
            raise ValueError
        report_by_name = _report(candidate)
        generations_by_source = _generations(candidate)
        if SUMMARY_KEY not in report_by_name and "registry_source_membership" not in generations_by_source:
            return None
        receipt = validate_registry_source_selection_receipt(report_by_name[SUMMARY_KEY])
        recipes = verify_registry_source_recipes(candidate)
        if not _is_candidate_match(candidate, receipt, recipes):
            raise ValueError
        return receipt
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceSelectionError("registry_source_selection_invalid") from None


def _is_candidate_replay(candidate, recipes, receipt, approved_source):
    stored_recipes = verify_registry_source_recipes(candidate)
    if (
        candidate["state"] != "open"
        or canonical_registry_source_recipes(stored_recipes) != canonical_registry_source_recipes(recipes)
        or not _is_candidate_match(candidate, receipt, stored_recipes)
        or candidate["expected_rows"] != receipt["mapped_rows"] + approved_source.total_rows
    ):
        raise RegistrySourceSelectionConflict("registry_source_selection_conflict")
    report_by_name = _report(candidate)
    if SUMMARY_KEY in report_by_name:
        retained = validate_registry_source_selection_receipt(report_by_name[SUMMARY_KEY])
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
    receipt = build_registry_source_selection_receipt(recipes, approved_source, stage)
    try:
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
            is_replay = _is_candidate_replay(candidate, recipes, receipt, approved_source)
            count = await connection.fetchval(f'SELECT count(*) FROM pg_temp."{stage.table_name}"')
            if count != receipt["mapped_rows"]:
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
    except asyncpg.PostgresError:
        raise RegistrySourceSelectionError("registry_source_selection_unavailable") from None
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceSelectionError("registry_source_selection_invalid") from None
