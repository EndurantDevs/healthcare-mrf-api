# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stream exact reviewed PTG offices into a replacement address artifact."""

import hashlib
import json
from dataclasses import dataclass

from process.registry_retained_site_adoption import resolve_retained_site_adoptions


@dataclass(frozen=True)
class RegistryPTGAddressCarry:
    """Compact identity of native-verified pages, without retaining their rows."""

    sha256: str | None = None
    pages: int = 0
    rows: int = 0
    origin_references: tuple[tuple, ...] = ()


def retained_site_origin_reference(receipt):
    """Retain native closure coordinates without accumulating selected row payloads."""
    return tuple(
        getattr(receipt, name)
        for name in (
            "generation_id",
            "manifest_sha256",
            "schema_oid",
            "address_table_oid",
            "binding_table_oid",
            "owner_role_oid",
        )
    )


def _canonical(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


async def prepare_ptg_address_carry(
    connection, recipes, approved, control_schema, office_authority, target_schema=None, *, inventory=False
):
    """Stream bounded pages with the existing shared graph budget and exact custody.

    A replay computes the same compact identity without writing any address rows.
    The caller owns the transaction, so a later conflict rolls back all page copies.
    """
    from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
    from process.registry_source_recipe_store import canonical_registry_source_recipes, decode_registry_source_recipes

    if type(inventory) is not bool:
        raise ValueError("registry_ptg_address_carry_inventory_invalid")
    if inventory:
        from process.registry_source_inventory_preparation import SourceInventoryPreparation

        recipes = SourceInventoryPreparation(recipes).recipes
    else:
        recipes = decode_registry_source_recipes(canonical_registry_source_recipes(recipes))
    selected_recipes = tuple(recipe for recipe in recipes if type(recipe.source_pin) is PinnedPTGOfficeMembershipSource)
    if not selected_recipes:
        return RegistryPTGAddressCarry()
    if inventory:
        from process.registry_source_recipe_store import _source_inventory_recipe_digest

        digest = _source_inventory_recipe_digest(selected_recipes)
    else:
        digest = hashlib.sha256(canonical_registry_source_recipes(selected_recipes).encode())
    pages = office_rows = 0
    origins_by_identity = {}
    for recipe in selected_recipes:
        page_count, row_count = await _stream_recipe(
            connection, recipe, approved, control_schema, office_authority, (digest, target_schema, origins_by_identity)
        )
        pages += page_count
        office_rows += row_count
    digest.update(_canonical({"pages": pages, "rows": office_rows}))
    return RegistryPTGAddressCarry(
        digest.hexdigest(),
        pages,
        office_rows,
        tuple(
            (*origin, origins_by_identity[origin][0], origins_by_identity[origin][1].hexdigest())
            for origin in sorted(origins_by_identity)
        ),
    )


async def _stream_recipe(connection, recipe, approved, control_schema, office_authority, destination):
    from process.network_approved_site_source import copy_approved_site_rows
    from process.registry_source_recipe_composition import _read_recipe_page, _require_recipe_custody

    digest, target_schema, origins_by_identity = destination
    verified = await _require_recipe_custody(connection, recipe, approved, control_schema, office_authority)
    cursor, limit = None, 1000
    pages = carried_rows = 0
    while True:
        batch, next_cursor, limit = await _read_recipe_page(
            connection, recipe, approved, cursor, control_schema, limit, verified
        )
        if not batch.source_rows:
            if batch.membership_rows or batch.office_records:
                raise ValueError("registry_ptg_address_carry_accounting_invalid")
            return pages, carried_rows
        receipt = await _page_receipt(connection, recipe, verified, batch, control_schema)
        if next_cursor is None or next_cursor == cursor:
            raise ValueError("registry_ptg_address_carry_cursor_invalid")
        encoded = _canonical(receipt.as_dict())
        origin = retained_site_origin_reference(receipt)
        if origin not in origins_by_identity:
            if len(origins_by_identity) >= 100:
                raise ValueError("registry_ptg_address_carry_origin_limit")
            origins_by_identity[origin] = [0, hashlib.sha256()]
        origins_by_identity[origin][0] += len(receipt.records)
        origins_by_identity[origin][1].update(len(encoded).to_bytes(8, "big"))
        origins_by_identity[origin][1].update(encoded)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        if target_schema is not None:
            await copy_approved_site_rows(
                connection,
                (receipt,),
                target_schema,
                exact_rows=True,
                reproject_network_ids=True,
                control_schema=control_schema,
            )
        pages += 1
        carried_rows += len(receipt.records)
        cursor = next_cursor


async def _page_receipt(connection, recipe, verified, batch, control_schema):
    selections = [
        {
            name: getattr(record, name)
            for name in ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
        }
        for record in batch.office_records
    ]
    if (
        len(selections) != batch.source_rows
        or batch.source_rows != batch.membership_rows
        or batch.source != recipe.source_pin
    ):
        raise ValueError("registry_ptg_address_carry_accounting_invalid")
    receipt = await resolve_retained_site_adoptions(
        connection, verified.serving, _canonical(selections), control_schema=control_schema
    )
    if receipt.records != batch.office_records or receipt.generation_id != recipe.source_pin.retained_generation_id:
        raise ValueError("registry_ptg_address_carry_page_changed")
    return receipt
