# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep a selected replacement from silently unioning an older capture of its scope."""

import json

from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource


def _scope_key(coordinates, network_id, scope):
    return (*coordinates, network_id, json.dumps(scope, sort_keys=True, separators=(",", ":")))


async def refuse_replaced_capture_union(connection, namespace, bindings, new_recipes, candidate_recipes):
    """Reject an older same-scope capture when a new exact replacement is selected."""
    selected_keys = {
        _scope_key(
            tuple(
                approval_record[name]
                for name in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
            ),
            approval_record["network_id"],
            approval_record["source_scope_json"],
        )
        for approval_record in bindings
    }
    new_ids = {recipe.source_pin.capture_id for recipe in new_recipes}
    retained_pins = [
        recipe.source_pin
        for recipe in candidate_recipes
        if type(recipe.source_pin) is PinnedPTGOfficeMembershipSource and recipe.source_pin.capture_id not in new_ids
    ]
    for start in range(0, len(retained_pins), 25):
        page = retained_pins[start : start + 25]
        approval_records = await connection.fetch(
            f"""SELECT capture_id::text,approval_sha256,
          CASE WHEN octet_length((approval_json->'command'->'source'->'source_scope')::text)<=16384
          THEN (approval_json->'command'->'source'->'source_scope')::text END AS scope_json
          FROM {namespace}.registry_ptg_office_approval WHERE capture_id::text=ANY($1::text[]) LIMIT $2""",
            [pin.capture_id for pin in page],
            len(page) + 1,
        )
        by_capture = {approval_record["capture_id"]: approval_record for approval_record in approval_records}
        if len(approval_records) != len(page) or len(by_capture) != len(page):
            raise ValueError("registry_office_selection_changed")
        for pin in page:
            approval_record = by_capture[pin.capture_id]
            if (
                approval_record["approval_sha256"] != pin.approval_sha256
                or type(approval_record["scope_json"]) is not str
            ):
                raise ValueError("registry_office_selection_changed")
            if (
                _scope_key(pin.coordinates.sql_parameters, pin.network_id, json.loads(approval_record["scope_json"]))
                in selected_keys
            ):
                raise ValueError("registry_office_selection_ambiguous")
