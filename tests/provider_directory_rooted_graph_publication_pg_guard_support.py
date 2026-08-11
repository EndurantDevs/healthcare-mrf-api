# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reusable PostgreSQL guard probes for rooted graph publication tests."""

from __future__ import annotations

import asyncio
import hashlib
import json

import asyncpg

from process.provider_directory_dataset_scoped_publication import (
    exact_uhc_dataset_pair,
    lock_exact_current_dataset,
    supersede_exact_current_dataset,
)
from process.provider_directory_rooted_graph_store import (
    complete_provider_directory_rooted_graph_missing,
    initialize_provider_directory_rooted_graph_acquisition,
)
from process.provider_directory_rooted_graph_store_contract import _sha256_text
from process.provider_directory_rooted_graph_store_support import set_store_action
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_terminal import (
    _edge_hash,
    _missing_terminal_hash,
    _resource_hash,
)
from tests.formulary_fhir_twin_admission_pg_support import (
    assert_sqlstate,
    connect,
    database_url,
    quoted,
)
from tests.provider_directory_rooted_graph_pg_support import resources_for_kind
from tests.test_provider_directory_rooted_graph_acquisition_postgres import (
    _claim_missing_endpoint,
    _complete_initial_work,
    _complete_organization_read,
    _identity,
)
from tests.test_provider_directory_rooted_graph_publication_postgres import (
    _prove_first_generation,
    _publish_legacy_root,
)


PARENT_RELATION = "provider_directory_endpoint_dataset"
LEGACY_RELATION = "provider_directory_uhc_flex_practitioner_dataset"
ROOTED_RELATION = "provider_directory_rooted_graph_dataset"


def _canonical_payload_bytes(resource_by_field: dict) -> int:
    return len(
        json.dumps(
            resource_by_field,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )


def missing_response_text() -> str:
    return json.dumps(
        {
            "issue": [{"code": "not-found", "severity": "error"}],
            "resourceType": "OperationOutcome",
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


async def _clear_exact_current(context) -> None:
    async with context.database.transaction():
        current = await lock_exact_current_dataset(
            context.database,
            pair=exact_uhc_dataset_pair(),
        )
        assert current is not None
        await supersede_exact_current_dataset(context.database, current)


async def _isolate_logical_current_triggers(connection, schema_name: str) -> None:
    schema = quoted(schema_name)
    for relation in (PARENT_RELATION, LEGACY_RELATION, ROOTED_RELATION):
        trigger = f"pd_exact_logical_current_{relation}_guard"
        await connection.execute(
            f"ALTER TABLE {schema}.{quoted(relation)} DISABLE TRIGGER ALL"
        )
        await connection.execute(
            f"ALTER TABLE {schema}.{quoted(relation)} ENABLE ALWAYS TRIGGER "
            f"{quoted(trigger)}"
        )


async def _commit_direct_current(
    *,
    schema_name: str,
    dataset_id: str,
    dedicated_relation: str,
    ready: asyncio.Event,
    release: asyncio.Event,
) -> str:
    connection = await connect(database_url())
    transaction = connection.transaction()
    await transaction.start()
    schema = quoted(schema_name)
    try:
        await connection.execute(
            f"UPDATE {schema}.{quoted(PARENT_RELATION)} SET status = 'published', "
            "is_current = true, superseded_at = NULL WHERE dataset_id = $1",
            dataset_id,
        )
        await connection.execute(
            f"UPDATE {schema}.{quoted(dedicated_relation)} "
            "SET status = 'published', is_current = true, superseded_at = NULL "
            "WHERE dataset_id = $1",
            dataset_id,
        )
        ready.set()
        await release.wait()
        await transaction.commit()
        return "committed"
    except asyncpg.PostgresError as error:
        return str(error.sqlstate)
    finally:
        await connection.close()


async def _probe_direct_readiness(
    connection,
    schema_name: str,
    dataset_id: str,
    dedicated_relation: str,
) -> tuple[bool, bool]:
    transaction = connection.transaction()
    await transaction.start()
    schema = quoted(schema_name)
    try:
        await connection.execute(
            f"UPDATE {schema}.{quoted(PARENT_RELATION)} SET status = 'published', "
            "is_current = true, superseded_at = NULL WHERE dataset_id = $1",
            dataset_id,
        )
        await connection.execute(
            f"UPDATE {schema}.{quoted(dedicated_relation)} "
            "SET status = 'published', is_current = true, superseded_at = NULL "
            "WHERE dataset_id = $1",
            dataset_id,
        )
        readiness_function = (
            "provider_directory_rooted_graph_dataset_ready"
            if dedicated_relation == ROOTED_RELATION
            else "provider_directory_uhc_flex_practitioner_dataset_ready"
        )
        validity_function = readiness_function.replace("_ready", "_valid")
        readiness = await connection.fetchval(
            f"SELECT {schema}.{quoted(readiness_function)}($1)", dataset_id
        )
        validity = await connection.fetchval(
            f"SELECT {schema}.{quoted(validity_function)}($1)", dataset_id
        )
        return bool(readiness), bool(validity)
    finally:
        await transaction.rollback()


async def prepare_logical_current_race(context) -> tuple[str, str]:
    original_legacy = await _publish_legacy_root(context.database)
    _, _, rooted_publication = await _prove_first_generation(
        context,
        original_legacy,
    )
    latest_legacy = await _publish_legacy_root(context.database, "2" * 64)
    await _clear_exact_current(context)
    await _isolate_logical_current_triggers(
        context.connection,
        context.schema_name,
    )
    rooted_id = rooted_publication.readiness.dataset_id
    legacy_probe = await _probe_direct_readiness(
        context.connection,
        context.schema_name,
        latest_legacy.dataset_id,
        LEGACY_RELATION,
    )
    rooted_probe = await _probe_direct_readiness(
        context.connection,
        context.schema_name,
        rooted_id,
        ROOTED_RELATION,
    )
    assert legacy_probe == (True, True)
    assert rooted_probe == (True, True)
    return latest_legacy.dataset_id, rooted_id


async def commit_logical_current_race(
    context,
    first_relation: str,
    legacy_id: str,
    rooted_id: str,
) -> tuple[str, str, list[str]]:
    ready_by_relation = {
        LEGACY_RELATION: asyncio.Event(),
        ROOTED_RELATION: asyncio.Event(),
    }
    release_by_relation = {
        LEGACY_RELATION: asyncio.Event(),
        ROOTED_RELATION: asyncio.Event(),
    }
    id_by_relation = {LEGACY_RELATION: legacy_id, ROOTED_RELATION: rooted_id}
    task_by_relation = {
        relation: asyncio.create_task(
            _commit_direct_current(
                schema_name=context.schema_name,
                dataset_id=id_by_relation[relation],
                dedicated_relation=relation,
                ready=ready_by_relation[relation],
                release=release_by_relation[relation],
            )
        )
        for relation in (LEGACY_RELATION, ROOTED_RELATION)
    }
    await asyncio.gather(*(event.wait() for event in ready_by_relation.values()))
    second_relation = (
        LEGACY_RELATION if first_relation == ROOTED_RELATION else ROOTED_RELATION
    )
    release_by_relation[first_relation].set()
    first_outcome = await task_by_relation[first_relation]
    release_by_relation[second_relation].set()
    second_outcome = await task_by_relation[second_relation]
    current_rows = await context.database.all(
        f"SELECT dataset_id FROM {context.schema}.{quoted(PARENT_RELATION)} "
        "WHERE is_current IS TRUE AND endpoint_id IN "
        "(:legacy_endpoint_id, :rooted_endpoint_id)",
        legacy_endpoint_id=exact_uhc_dataset_pair().legacy_endpoint_id,
        rooted_endpoint_id=exact_uhc_dataset_pair().rooted_endpoint_id,
    )
    current_ids = [current_row.dataset_id for current_row in current_rows]
    return first_outcome, second_outcome, current_ids


def payload_budget_identity(current):
    role_resource_by_field = resources_for_kind("exact_reference_search")[0]
    organization_by_field = {
        "resourceType": "Organization",
        "id": "org.synthetic-1",
    }
    retained_resource_bytes = sum(
        _canonical_payload_bytes(resource_by_field)
        for resource_by_field in (role_resource_by_field, organization_by_field)
    )
    missing_bytes = len(missing_response_text().encode("utf-8"))
    identity = _identity(
        current,
        "baseline",
        "c",
        "8",
        max_payload_bytes=retained_resource_bytes + missing_bytes - 1,
    )
    return identity, retained_resource_bytes


async def prepare_missing_claim(context, identity):
    await initialize_provider_directory_rooted_graph_acquisition(
        identity,
        database=context.database,
    )
    role_result = await _complete_initial_work(
        context.database,
        identity,
        None,
        context.schema_name,
    )
    await _complete_organization_read(
        context.database,
        identity,
        role_result,
    )
    claim = await _claim_missing_endpoint(
        context.database,
        identity,
        role_result,
    )
    return role_result, claim


async def complete_valid_missing(database, claim) -> None:
    response_text = missing_response_text()
    response_bytes = len(response_text.encode("utf-8"))
    await complete_provider_directory_rooted_graph_missing(
        claim,
        missing_http_status=404,
        missing_response_sha256=hashlib.sha256(
            response_text.encode("utf-8")
        ).hexdigest(),
        missing_response_bytes=response_bytes,
        missing_response_json_text=response_text,
        database=database,
    )


def forged_missing_by_field(claim) -> dict[str, object]:
    forged_text = json.dumps(
        {
            "issue": [{"code": "forged", "severity": "error"}],
            "resourceType": "OperationOutcome",
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    forged_bytes = len(forged_text.encode("utf-8"))
    forged_sha = hashlib.sha256(forged_text.encode("utf-8")).hexdigest()
    resource_set_sha = _resource_hash(())
    edge_set_sha = _edge_hash(())
    result_sha = _sha256_text(
        "\x1f".join((resource_set_sha, edge_set_sha, forged_sha, str(forged_bytes)))
    )
    return {
        "result_sha": result_sha,
        "resource_set_sha": resource_set_sha,
        "edge_set_sha": edge_set_sha,
        "forged_sha": forged_sha,
        "forged_bytes": forged_bytes,
        "forged_text": forged_text,
        "terminal_sha": _missing_terminal_hash(
            claim,
            404,
            forged_sha,
            forged_bytes,
            result_sha,
        ),
    }


async def write_forged_missing(context, claim) -> None:
    forged_by_field = forged_missing_by_field(claim)
    async with context.database.transaction():
        await set_store_action(
            context.database,
            "terminal",
            claim.acquisition_id,
            claim.lease_token,
        )
        await context.database.status(
            f"UPDATE {context.schema}.provider_directory_rooted_graph_work "
            "SET status = 'completed', lease_token = NULL, "
            "lease_expires_at = NULL, lease_heartbeat_at = NULL, "
            "result_sha256 = :result_sha, resource_count = 0, edge_count = 0, "
            "resource_set_sha256 = :resource_set_sha, "
            "edge_set_sha256 = :edge_set_sha, advertised_total = NULL, "
            "terminal_page_count = 1, pagination_terminal = true, "
            "missing_http_status = 404, missing_response_sha256 = :forged_sha, "
            "missing_response_bytes = :forged_bytes, "
            "missing_response_json_text = :forged_text, "
            "terminal_record_sha256 = :terminal_sha, "
            "terminal_at = transaction_timestamp(), updated_at = "
            "transaction_timestamp() WHERE acquisition_id = :acquisition_id "
            "AND query_id = :query_id AND status = 'leased' "
            "AND attempt_count = :attempt AND lease_token = :lease_token",
            **forged_by_field,
            acquisition_id=claim.acquisition_id,
            query_id=claim.query_id,
            attempt=claim.attempt,
            lease_token=claim.lease_token,
        )


async def assert_graph_registry_drift_fences(context) -> None:
    endpoint_row = await context.connection.fetchrow(
        f"SELECT endpoint_id, endpoint_signature_hash FROM {context.schema}."
        "provider_directory_api_endpoint WHERE endpoint_id = $1",
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    )
    source_row = await context.connection.fetchrow(
        f"SELECT source_id, endpoint_id FROM {context.schema}."
        "provider_directory_source WHERE source_id = $1",
        PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
    )
    assert tuple(endpoint_row.values()) == (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
    )
    assert tuple(source_row.values()) == (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    )
    await assert_sqlstate(
        context.connection,
        {"23514", "55000"},
        f"UPDATE {context.schema}.provider_directory_api_endpoint "
        "SET endpoint_signature_hash = repeat('0', 64) WHERE endpoint_id = "
        f"'{PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID}'",
    )
    await assert_sqlstate(
        context.connection,
        {"23514", "55000"},
        f"UPDATE {context.schema}.provider_directory_source "
        "SET canonical_api_base = 'https://invalid.example' WHERE source_id = "
        f"'{PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID}'",
    )
    signature = await context.connection.fetchval(
        f"SELECT endpoint_signature_hash FROM {context.schema}."
        "provider_directory_api_endpoint WHERE endpoint_id = $1",
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    )
    assert signature == PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256


__all__ = (
    "assert_graph_registry_drift_fences",
    "commit_logical_current_race",
    "complete_valid_missing",
    "forged_missing_by_field",
    "LEGACY_RELATION",
    "missing_response_text",
    "PARENT_RELATION",
    "payload_budget_identity",
    "prepare_logical_current_race",
    "prepare_missing_claim",
    "ROOTED_RELATION",
    "write_forged_missing",
)
