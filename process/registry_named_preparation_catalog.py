# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Locked named admissions, claim checks and QName reservations."""

import json

from process.registry_named_preparation_records import (
    PREPARATION,
    PREPARATION_TABLES,
    claim_values,
    local_uuid,
    qualified,
    relation_inventory,
    require,
)
from process.registry_named_preparation_transactions import lock_scopes


def preparation_record(row):
    """Return detached JSON values while preserving exact database UUIDs and timestamps."""
    require(row is not None, "claim_expired_or_changed")
    record_dict = dict(row)
    for field in ("admission", "handoff", "intake_inventory", "frozen_inventory", "validation", "publication"):
        if isinstance(record_dict.get(field), str):
            record_dict[field] = json.loads(record_dict[field])
    for field in ("intake_inventory", "frozen_inventory"):
        if record_dict.get(field) is not None:
            record_dict[field] = relation_inventory(record_dict[field])
    return record_dict


async def preparation_authority(store):
    """Hold the finite metadata tables through the caller transaction."""
    await store.connection.execute(
        "LOCK TABLE ONLY " + ", ONLY ".join(PREPARATION_TABLES) + " IN ACCESS SHARE MODE NOWAIT"
    )


async def locked_preparation(store, preparation_id, admission_sha256=None):
    """Keep one named admission and candidate identity locked through the transaction."""
    preparation = await store.connection.fetchrow(
        f"SELECT * FROM {PREPARATION} WHERE preparation_id=$1 FOR UPDATE NOWAIT", local_uuid(preparation_id)
    )
    require(preparation is not None, "preparation_missing")
    preparation = preparation_record(preparation)
    require(
        preparation["importer_id"] == preparation["admission"].get("kind") == store.named_kind
        and preparation["preparation_id"] == preparation["generation_id"],
        "named_candidate_changed",
    )
    if preparation["handoff"] is not None:
        from process.registry_named_preparation_store import named_preparation_document

        document = named_preparation_document(preparation["handoff"], store.named_kind)
        require(document["candidate_id"] == str(preparation["preparation_id"]), "named_candidate_changed")
    require(admission_sha256 is None or preparation["admission_sha256"] == admission_sha256, "admission_changed")
    return preparation


async def locked_claim(store, claim, *, allow_canceled=False):
    """An expired or canceled worker cannot advance any preparation evidence."""
    preparation_id, token, attempt = claim_values(claim)
    row = await locked_preparation(store, preparation_id)
    require(row["state"] not in {"abandoned", "published"}, "preparation_terminal")
    require(row["claim_token"] == token and row["claim_attempt"] == attempt, "claim_changed")
    require(
        await store.connection.fetchval(
            "SELECT $1::timestamptz > pg_catalog.clock_timestamp()", row["claim_expires_at"]
        ),
        "claim_expired",
    )
    require(allow_canceled or not row["cancellation_requested"], "preparation_canceled")
    return row


async def verify_intake_inventory(store, inventory, *, named_handoff=None):
    """Require the complete closed handoff inventory before reserving candidate names."""
    from process.registry_named_preparation_store import named_preparation_document

    document = named_preparation_document(named_handoff, store.named_kind)
    expected_by_field = {
        "relations": [{"schema_name": table["schema"], "relation_name": table["name"]} for table in document["tables"]]
    }
    require(relation_inventory(inventory) == relation_inventory(expected_by_field), "named_inventory_changed")


async def register_stage_claims(store, preparation_id, inventory):
    """Reserve qualified candidate names for one preparation."""
    names = [(relation["schema_name"], relation["relation_name"]) for relation in inventory["relations"]]
    await lock_scopes(store.connection, ["relation:" + schema + "." + name for schema, name in names])
    for schema, name in names:
        require(
            not await store.connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM hp_snapshot_retention.relation WHERE schema_name=$1 AND relation_name=$2)",
                schema,
                name,
            ),
            "relation_already_retained",
        )
        await store.connection.execute(
            "INSERT INTO hp_snapshot_retention.preparation_relation(schema_name,relation_name,preparation_id) "
            "VALUES($1,$2,$3)",
            schema,
            name,
            preparation_id,
        )


async def verify_inventory(connection, inventory, authority, *, readonly=True):
    """Retain reader locks and require native SELECT access to each declared table."""
    inventory = relation_inventory(inventory)
    names = [qualified(relation["schema_name"], relation["relation_name"]) for relation in inventory["relations"]]
    require(readonly is True, "named_inventory_readonly_required")
    await connection.execute("LOCK TABLE ONLY " + ", ONLY ".join(names) + " IN ACCESS SHARE MODE NOWAIT")
    for relation in names:
        await connection.fetch("SELECT * FROM " + relation + " LIMIT 0")
