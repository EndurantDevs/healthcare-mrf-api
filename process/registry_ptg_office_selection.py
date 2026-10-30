# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Match explicit D04 office selections to real immutable approval and hold rows."""

import json
import re
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from db.registry_schema import registry_schema
from process.network_approved_membership_source import pin_approved_membership_source
from process.registry_ptg_office_capture import _digest
from process.registry_ptg_office_membership_contract import (
    PinnedPTGOfficeMembershipSource,
    approved_office_recipe_receipt,
)
from process.registry_ptg_producer_scope import _PERMISSIONS_SQL, RegistryPTGProducerScopeStore
from process.registry_source_observation_store import _namespace
from process.registry_source_recipe_store import (
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
    registry_source_recipes_sha256,
)

PAGE_ROWS = 25
MAX_SELECTION_CHANGES = 100


def _normalized_recipes(recipes, inventory):
    if inventory:
        from process.registry_source_inventory_preparation import SourceInventoryPreparation

        return SourceInventoryPreparation(recipes).recipes
    return decode_registry_source_recipes(canonical_registry_source_recipes(recipes))


def _recipe_commitment(recipes, inventory):
    if inventory:
        from process.registry_source_inventory_preparation import SourceInventoryPreparation

        return SourceInventoryPreparation(recipes).recipe_root.recipes_sha256
    return canonical_registry_source_recipes(recipes)


def office_recipe_selection(value):
    """Absent selectors preserve old bindings; present selectors are closed and canonical."""
    if value is None:
        return None
    if (
        type(value) is not dict
        or set(value) != {"capture_id", "recipe_sha256"}
        or type(value["capture_id"]) is not str
        or type(value["recipe_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", value["recipe_sha256"]) is None
    ):
        raise ValueError("registry_office_selection_invalid")
    try:
        capture = UUID(value["capture_id"])
        if not capture.int or str(capture) != value["capture_id"]:
            raise ValueError
    except ValueError, AttributeError:
        raise ValueError("registry_office_selection_invalid") from None
    return dict(value)


def _requested(native_records):
    requested_rows = []
    for native_record in native_records:
        if type(native_record) is not dict:
            raise ValueError("registry_office_selection_invalid")
        selector = office_recipe_selection(native_record.get("office_recipe_json"))
        if selector is not None:
            if (
                native_record.get("source_system") != "ptg"
                or type(native_record.get("source_scope_json")) is not dict
                or native_record["source_scope_json"].get("review_type") != "published_complete_snapshot_plan"
            ):
                raise ValueError("registry_office_selection_invalid")
            requested_rows.append({**native_record, "office_recipe_json": selector})
    return requested_rows


async def _protected_tables(connection, namespace, store):
    if (
        type(store) is not RegistryPTGProducerScopeStore
        or (store.control_schema or registry_schema()) != namespace[1:-1]
    ):
        raise ValueError("registry_office_selection_store_unprotected")
    if not connection.is_in_transaction() or await connection.fetchval("SHOW transaction_isolation") not in {
        "repeatable read",
        "serializable",
    }:
        raise ValueError("registry_office_selection_transaction_required")
    for table in ("registry_ptg_office_approval", "registry_ptg_office_hold"):
        statement = text(_PERMISSIONS_SQL.replace("'registry_ptg_producer_scope'", "'" + table + "'"))
        compiled = statement.compile(dialect=PGDialect_asyncpg())
        parameters_by_name = dict(
            schema_name=namespace[1:-1], owner_role=store.owner_role, approval_role=store.approval_role, write=False
        )
        if (
            await connection.fetchval(str(compiled), *(parameters_by_name[name] for name in compiled.positiontup))
            is not True
        ):
            raise ValueError("registry_office_selection_store_unprotected")


_PAIR_SQL = """SELECT request.ordinal,a.approval_sha256,h.hold_sha256,
 CASE WHEN octet_length(a.approval_json::text)<=131072 THEN a.approval_json::text END AS approval_json,
 CASE WHEN octet_length(h.hold_json::text)<=32768 THEN h.hold_json::text END AS hold_json
 FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY request(document,ordinal)
 LEFT JOIN {namespace}.registry_ptg_office_approval a
 ON a.capture_id=(request.document->'office_recipe_json'->>'capture_id')::uuid
 LEFT JOIN {namespace}.registry_ptg_office_hold h USING(capture_id,approval_sha256)
 ORDER BY request.ordinal"""


def _paired_recipe(native_record, binding):
    try:
        return _checked_pair(native_record, binding)
    except KeyError, TypeError, AttributeError, UnicodeError, RecursionError:
        raise ValueError("registry_office_selection_changed") from None


def _checked_pair(native_record, binding):
    if type(native_record["approval_json"]) is not str or type(native_record["hold_json"]) is not str:
        raise ValueError("registry_office_selection_unavailable")
    document, hold = json.loads(native_record["approval_json"]), json.loads(native_record["hold_json"])
    selector = binding["office_recipe_json"]
    if _digest(document) != native_record["approval_sha256"] or _digest(hold) != native_record["hold_sha256"]:
        raise ValueError("registry_office_selection_changed")
    command = document["command"]
    source_by_field = command["source"]
    if (
        document["contract"] != "registry_ptg_office_approval.v1"
        or document["capture_id"] != selector["capture_id"]
        or document["client_id"] != command["client_id"]
        or command["capture_id"] != selector["capture_id"]
        or any(
            source_by_field["coordinates"][name] != binding[name]
            for name in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
        )
        or source_by_field["binding_source_key"] != binding["source_key"]
        or source_by_field["network_id"] != binding["network_id"]
        or source_by_field["source_scope"] != binding["source_scope_json"]
        or hold
        != {
            "contract": "registry_ptg_office_hold.v1",
            "capture_id": selector["capture_id"],
            "approval_sha256": native_record["approval_sha256"],
            "manifest_sha256": document["manifest_sha256"],
            "witness_sha256": _digest(document["witness"]),
            "schema_name": "registry_ptg_office_" + UUID(selector["capture_id"]).hex,
            "custody": document["witness"]["custody"],
            "retained_serving": command["retained_serving"],
        }
    ):
        raise ValueError("registry_office_selection_changed")
    receipt = approved_office_recipe_receipt(
        command, document["manifest_sha256"], native_record["approval_sha256"], native_record["hold_sha256"]
    )
    if receipt["source_recipes_sha256"] != selector["recipe_sha256"]:
        raise ValueError("registry_office_selection_changed")
    return decode_registry_source_recipes(receipt["source_recipes"])[0]


async def _read_references(connection, namespace, requested_rows, store):
    if not requested_rows:
        return ()
    await _protected_tables(connection, namespace, store)
    recipes = []
    for start in range(0, len(requested_rows), PAGE_ROWS):
        page = requested_rows[start : start + PAGE_ROWS]
        pairs = await connection.fetch(_PAIR_SQL.format(namespace=namespace), json.dumps(page, separators=(",", ":")))
        if len(pairs) != len(page) or any(
            native_record["ordinal"] != index + 1 for index, native_record in enumerate(pairs)
        ):
            raise ValueError("registry_office_selection_changed")
        recipes.extend(
            _paired_recipe(native_record, binding) for native_record, binding in zip(pairs, page, strict=True)
        )
    return tuple(recipes)


def _binding_changes_sql(namespace, staging):
    effective = f"""SELECT record_key,record_json FROM {namespace}.registry_approved_record
      WHERE approved_revision=$1 AND record_kind='network_binding'"""
    if staging is not None:
        effective += f""" AND NOT EXISTS(SELECT FROM {staging} selected
          WHERE selected.record_kind='network_binding' AND selected.record_key=registry_approved_record.record_key)
          UNION ALL SELECT history.record_key,history.record_json FROM {staging} selected
          JOIN {namespace}.registry_record_history history
            ON (history.record_kind,history.record_key,history.revision)=
              (selected.record_kind,selected.record_key,selected.revision)
          WHERE selected.record_kind='network_binding'"""
    return f"""WITH effective AS MATERIALIZED ({effective}), previous AS MATERIALIZED (
      SELECT record_key,record_json FROM {namespace}.registry_approved_record
      WHERE approved_revision=$2 AND record_kind='network_binding'
    ) SELECT previous.record_key IS NOT NULL AS has_previous,effective.record_key IS NOT NULL AS has_effective,
      CASE WHEN octet_length(previous.record_json::text)<=32768 THEN previous.record_json::text END AS previous_json,
      CASE WHEN octet_length(effective.record_json::text)<=32768 THEN effective.record_json::text END AS effective_json
      FROM previous FULL JOIN effective USING(record_key)
      WHERE (previous.record_json->>'source_system'='ptg' OR effective.record_json->>'source_system'='ptg')
        AND (NULLIF(previous.record_json->'office_recipe_json','null'::jsonb) IS NOT NULL
          OR NULLIF(effective.record_json->'office_recipe_json','null'::jsonb) IS NOT NULL)
        AND (previous.record_json-'revision'-'created_at') IS DISTINCT FROM
          (effective.record_json-'revision'-'created_at')
      ORDER BY coalesce(previous.record_key,effective.record_key) LIMIT $3"""


def _changed_office_bindings(changes, prefix):
    requested_rows = []
    for change in changes:
        if type(change["has_" + prefix]) is not bool:
            raise ValueError("registry_office_selection_changed")
        if not change["has_" + prefix]:
            continue
        document = change[prefix + "_json"]
        if type(document) is not str:
            raise ValueError("registry_office_selection_changed")
        binding = json.loads(document)
        if type(binding) is not dict or type(binding.get("archived")) is not bool:
            raise ValueError("registry_office_selection_changed")
        if binding.get("source_system") == "ptg" and not binding["archived"]:
            requested_rows.extend(_requested([binding]))
    return requested_rows


async def resolve_ptg_office_recipe_selection(
    connection,
    recipes,
    approved_revision,
    source_manifest,
    *,
    control_schema,
    office_authority=None,
    staging=None,
    inventory=False,
):
    """Apply only explicit protected binding deltas to their exact retained recipe identities."""
    namespace = _namespace(control_schema)
    changes = await connection.fetch(
        _binding_changes_sql(namespace, staging),
        approved_revision,
        0 if source_manifest is None else source_manifest.approved_custom_revision,
        MAX_SELECTION_CHANGES + 1,
    )
    if len(changes) > MAX_SELECTION_CHANGES:
        raise ValueError("registry_office_selection_too_many_changes")
    if not changes:
        return recipes
    before = _changed_office_bindings(changes, "previous")
    after = _changed_office_bindings(changes, "effective")
    if not before and not after:
        return recipes
    if any(
        _changed_office_bindings((change,), "previous")
        and change["has_effective"]
        and (document := json.loads(change["effective_json"])).get("source_system") == "ptg"
        and document.get("archived") is False
        and document.get("office_recipe_json") is None
        for change in changes
    ):
        raise ValueError("registry_office_selection_unavailable")
    if type(office_authority) is not tuple or len(office_authority) != 3:
        raise ValueError("registry_office_selection_store_unprotected")
    old = await _read_references(connection, namespace, before, office_authority[1])
    new = await _read_references(connection, namespace, after, office_authority[1])
    if any(binding["network_id"] != recipe.source_pin.network_id for binding, recipe in zip(after, new, strict=True)):
        raise ValueError("registry_office_selection_changed")
    removed_identities = {canonical_registry_source_recipes((recipe,)) for recipe in old}
    recipes_by_identity = {
        canonical_registry_source_recipes((recipe,)): recipe
        for recipe in recipes
        if canonical_registry_source_recipes((recipe,)) not in removed_identities
    }
    recipes_by_identity.update({canonical_registry_source_recipes((recipe,)): recipe for recipe in new})
    resolved = _normalized_recipes(tuple(recipes_by_identity.values()), inventory)
    from process.registry_ptg_office_selection_replay import refuse_replaced_capture_union

    await refuse_replaced_capture_union(connection, namespace, after, new, resolved)
    return resolved


async def require_staged_binding_office_references(connection, namespace, staging, store):
    """Preserve omitted selections, then validate effective authority in bounded pages."""
    await connection.execute(f"""UPDATE {staging} target SET office_recipe_json=head.office_recipe_json
      FROM {namespace}.registry_network_binding head WHERE target.binding_id=head.binding_id
      AND target.operation IN ('rebind','close') AND target.office_recipe_json IS NULL""")
    after = None
    while True:
        records = await connection.fetch(
            f"""SELECT binding_id,to_jsonb(target)::text AS document FROM {staging} target
              WHERE office_recipe_json IS NOT NULL AND office_recipe_json<>'null'::jsonb
              AND ($1::uuid IS NULL OR binding_id>$1) ORDER BY binding_id LIMIT $2""",
            after,
            PAGE_ROWS,
        )
        if not records:
            return
        await _read_references(
            connection, namespace, _requested([json.loads(record["document"]) for record in records]), store
        )
        after = records[-1]["binding_id"]


async def require_selected_ptg_office_recipes(
    connection, recipes, approved, *, control_schema, office_authority, candidate_recipes=(), inventory=False
):
    """Require current reviewed selection only for new recipes, never retained replay."""
    selected_recipes = [recipe for recipe in recipes if type(recipe.source_pin) is PinnedPTGOfficeMembershipSource]
    if not selected_recipes:
        return
    if type(office_authority) is not tuple or len(office_authority) != 3:
        raise ValueError("registry_office_selection_store_unprotected")
    if (
        await pin_approved_membership_source(
            connection, approved_revision=approved.approved_revision, control_schema=control_schema
        )
        != approved
    ):
        raise ValueError("registry_office_selection_approved_changed")
    namespace = '"' + control_schema + '"'
    wanted_rows = [
        {
            "capture_id": recipe.source_pin.capture_id,
            "recipe_sha256": registry_source_recipes_sha256(canonical_registry_source_recipes((recipe,))),
        }
        for recipe in selected_recipes
    ]
    native_records = await connection.fetch(
        f"""SELECT record_json::text AS document FROM {namespace}.registry_approved_record
      WHERE approved_revision=$1 AND record_kind='network_binding' AND record_json->'archived'='false'::jsonb
      AND EXISTS(SELECT FROM jsonb_array_elements($2::jsonb) wanted
        WHERE record_json->'office_recipe_json'=wanted) LIMIT $3""",
        approved.approved_revision,
        json.dumps(wanted_rows),
        len(wanted_rows) + 1,
    )
    if len(native_records) != len(wanted_rows):
        raise ValueError("registry_office_selection_unselected")
    requested_rows = _requested([json.loads(native_record["document"]) for native_record in native_records])
    actual = await _read_references(connection, namespace, requested_rows, office_authority[1])
    if _recipe_commitment(actual, inventory) != _recipe_commitment(selected_recipes, inventory):
        raise ValueError("registry_office_selection_ambiguous")
    from process.registry_ptg_office_selection_replay import refuse_replaced_capture_union

    await refuse_replaced_capture_union(connection, namespace, requested_rows, selected_recipes, candidate_recipes)


def require_ptg_startup_selection(configured_recipes, resolved_recipes, *, inventory=False):
    """Refuse an explicit startup set that contradicts reviewed office binding changes."""

    def offices(recipes):
        """Compare only exact PTG office recipes; other startup families remain explicit."""
        return _recipe_commitment(
            tuple(recipe for recipe in recipes if type(recipe.source_pin) is PinnedPTGOfficeMembershipSource), inventory
        )

    if offices(configured_recipes) != offices(resolved_recipes):
        raise ValueError("registry_office_selection_startup_conflict")
