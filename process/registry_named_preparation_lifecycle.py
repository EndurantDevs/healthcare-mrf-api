# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic admission, intake and completed administrative freeze."""

import json

from process.registry_named_preparation_catalog import (
    locked_claim,
    locked_preparation,
    preparation_record,
    register_stage_claims,
    verify_intake_inventory,
    verify_inventory,
)
from process.registry_named_preparation_records import PREPARATION, digest, local_uuid, relation_inventory, require
from process.registry_named_preparation_transactions import lock_scopes


async def admit_preparation(store, admission, previous):
    """Only an identical replay can reuse an admitted real native run ID."""
    identity = digest(admission)
    scope_dict = {key: admission[key] for key in ("node_id", "importer_id", "dataset_key", "run_id")}
    run_scope_dict = {key: admission[key] for key in ("node_id", "importer_id", "run_id")}
    await lock_scopes(
        store.connection, ["preparation:" + admission["preparation_id"], "native-admission:" + digest(run_scope_dict)]
    )
    existing = await store.connection.fetchrow(
        f"SELECT preparation_id FROM {PREPARATION} WHERE preparation_id=$1 OR "
        "(node_id=$2 AND importer_id=$3 AND run_id=$4)",
        local_uuid(admission["preparation_id"]),
        *run_scope_dict.values(),
    )
    if existing:
        require(str(existing["preparation_id"]) == admission["preparation_id"], "native_run_already_admitted")
        stored = await locked_preparation(store, existing["preparation_id"], identity)
        return stored
    require(
        previous is None and admission["expected_generation_id"] is None and admission["expected_fence_token"] is None,
        "predecessor_invalid",
    )
    return preparation_record(
        await store.connection.fetchrow(
            f"INSERT INTO {PREPARATION} "
            "(preparation_id,node_id,importer_id,dataset_key,run_id,generation_id,publication_fence,"
            "admission_sha256,admission,expected_generation_id,expected_fence_token,predecessor_pin_id) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12) RETURNING *",
            local_uuid(admission["preparation_id"]),
            *scope_dict.values(),
            local_uuid(admission["generation_id"]),
            local_uuid(admission["publication_fence"]),
            identity,
            json.dumps(admission),
            local_uuid(admission["expected_generation_id"])
            if admission["expected_generation_id"] is not None
            else None,
            local_uuid(admission["expected_fence_token"]) if admission["expected_fence_token"] is not None else None,
            local_uuid(admission["predecessor_pin_id"]) if admission["predecessor_pin_id"] is not None else None,
        )
    )


async def intake_preparation(store, claim, intake):
    """Record the logical handoff once under its current claim."""
    preparation_row = await locked_claim(store, claim)
    identity = digest(intake)
    if preparation_row["intake_sha256"] is not None:
        require(preparation_row["intake_sha256"] == identity, "intake_changed")
        return preparation_row
    require(preparation_row["state"] == "admitted", "preparation_state_invalid")
    await verify_intake_inventory(store, intake["inventory"], named_handoff=intake["receipt"])
    from process.registry_named_preparation_store import named_preparation_document, require_named_ready

    document = named_preparation_document(intake["receipt"], store.named_kind)
    require(document["candidate_id"] == str(preparation_row["preparation_id"]), "named_candidate_changed")
    await require_named_ready(store.connection, document)
    await locked_claim(store, claim)
    await register_stage_claims(store, preparation_row["preparation_id"], intake["inventory"])
    return preparation_record(
        await store.connection.fetchrow(
            f"UPDATE {PREPARATION} SET state='handed_off',attempt_id=$2,handoff=$3::jsonb,"
            "intake_inventory=$4::jsonb,intake_sha256=$5,updated_at=pg_catalog.clock_timestamp() "
            "WHERE preparation_id=$1 AND claim_token=$6 AND claim_attempt=$7 AND NOT cancellation_requested "
            "AND claim_expires_at>pg_catalog.clock_timestamp() RETURNING *",
            preparation_row["preparation_id"],
            intake["attempt_id"],
            json.dumps(intake["receipt"]),
            json.dumps(intake["inventory"]),
            identity,
            preparation_row["claim_token"],
            preparation_row["claim_attempt"],
        )
    )


async def _frozen_inventory(store, preparation_row):
    """Resolve the qualified names recorded when candidate writes closed."""
    require(preparation_row["intake_inventory"] is not None, "intake_required")
    inventory = relation_inventory(preparation_row["intake_inventory"])
    claims = await store.connection.fetch(
        "SELECT schema_name,relation_name FROM hp_snapshot_retention.preparation_relation "
        "WHERE preparation_id=$1 ORDER BY schema_name,relation_name",
        preparation_row["preparation_id"],
    )
    require([dict(entry) for entry in claims] == inventory["relations"], "stage_claim_changed")
    await verify_inventory(store.connection, inventory, store.authority, readonly=True)
    if preparation_row["state"] != "handed_off":
        require(preparation_row["frozen_inventory"] is not None, "freeze_required")
        require(preparation_row["frozen_sha256"] == digest(preparation_row["frozen_inventory"]), "freeze_changed")
    if preparation_row["frozen_inventory"] is not None:
        require(relation_inventory(preparation_row["frozen_inventory"]) == inventory, "freeze_changed")
    return inventory


async def observe_frozen(store, claim):
    """Record real protected catalogs after external freeze, never a typed claim of ownership."""
    preparation_row = await locked_claim(store, claim)
    require(preparation_row["state"] in {"handed_off", "frozen", "validated"}, "preparation_state_invalid")
    inventory = await _frozen_inventory(store, preparation_row)
    if preparation_row["state"] in {"frozen", "validated"}:
        return preparation_row
    await locked_claim(store, claim)
    return preparation_record(
        await store.connection.fetchrow(
            f"UPDATE {PREPARATION} SET state='frozen',frozen_inventory=$2::jsonb,frozen_sha256=$3,"
            "updated_at=pg_catalog.clock_timestamp() WHERE preparation_id=$1 AND claim_token=$4 AND claim_attempt=$5 "
            "AND NOT cancellation_requested AND claim_expires_at>pg_catalog.clock_timestamp() RETURNING *",
            preparation_row["preparation_id"],
            json.dumps(inventory),
            digest(inventory),
            preparation_row["claim_token"],
            preparation_row["claim_attempt"],
        )
    )
