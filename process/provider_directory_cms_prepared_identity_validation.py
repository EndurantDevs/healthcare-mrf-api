# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only parity checks for prepared CMS identities, without publication."""

import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID

from db.plan_release_storage import storage_identifier, storage_relation
from process import cms_npd_source
from process import provider_directory_cms_identity_candidate as entities
from process import provider_directory_cms_prepared_relationships as relationships
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_resource_identity_candidate as resources
from process.provider_directory_generic_candidate import COPY_MAX_BYTES, COPY_MAX_ROWS
from process.provider_directory_generic_source_policy import _savepoint
from process.provider_directory_resource_identity import source_resource_uuid

CONTRACT = "cms-prepared-identity-parity-v1"
CONTENT_CONTRACT = "sha256-canonical-typed-identity-lines-v1"
_PAGE_BYTES = COPY_MAX_BYTES
_ROW_BYTES = 2 * COPY_MAX_BYTES
_JSON_COLUMNS = {"payload_json", "network_refs", "plan_payload_json"}


def _require(condition, code):
    if not condition:
        raise ValueError("cms_prepared_identity_validation_" + code)


async def _frozen_source_coordinates(workspace, relations):
    identity = workspace.identity
    invalid = await workspace.connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {relations[storage.RESOURCE]} WHERE dataset_id<>$1 "
        "OR resource_type<>ALL($3::varchar[])) OR EXISTS("
        f"SELECT FROM {relations[storage.WITNESS]} WHERE dataset_id<>$1 OR source_id<>'cms-npd' "
        "OR release_id<>$2 OR resource_type<>ALL($3::varchar[]))",
        identity["dataset_id"],
        identity["metadata"]["source_release"]["vector_sha256"],
        [family for _, family in cms_npd_source.RESOURCE_FILES],
    )
    _require(invalid is False, "frozen_source_coordinates_changed")


async def _resource_replay_coordinates(connection, stage, catalog_schema):
    canonical = storage_relation(catalog_schema, resources.RESOURCE_IDENTITY_ROOT)
    changed = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {stage} candidate JOIN {canonical} incumbent "
        "USING(source_id,resource_type,resource_id) "
        "WHERE candidate.created_at IS DISTINCT FROM incumbent.created_at) OR EXISTS("
        f"SELECT FROM {stage} candidate JOIN {canonical} incumbent "
        "USING(source_id,resource_type,entity_id) WHERE candidate.resource_id<>incumbent.resource_id) "
        f"OR EXISTS(SELECT FROM {stage} GROUP BY source_id,resource_type,entity_id HAVING count(*)>1)"
    )
    if changed is not False:
        raise entities.CMSIdentityReprepareRequired("cms_prepared_identity_validation_resource_replay_conflict")


def _typed_row(columns, row):
    values = []
    for column in columns:
        value = row[column]
        if column in _JSON_COLUMNS and isinstance(value, str):
            value = json.loads(value)
        elif isinstance(value, UUID):
            value = str(value)
        elif isinstance(value, datetime):
            _require(value.tzinfo is not None, "timestamp_timezone_required")
            value = value.astimezone(timezone.utc).isoformat(timespec="microseconds")
        values.append(value)
    return values


async def _content_commitment(workspace, relations):
    """Hash exact values in bounded native keyset pages; no retained portals.

    JSONB text can add whitespace beyond the compact COPY representation. The
    first-row fallback therefore permits one row up to twice the COPY byte cap,
    while ordinary pages stay within the COPY byte cap and row bound.
    """
    digest, counts = hashlib.sha256(), {}
    for root, relation in relations.items():
        columns = entities.CMS_IDENTITY_COLUMNS.get(root, resources.RESOURCE_IDENTITY_COLUMNS)
        keys = entities.CMS_IDENTITY_KEYS.get(root, ("source_id", "resource_type", "resource_id"))
        fields = ",".join(storage_identifier(column) for column in columns)
        order = ",".join(storage_identifier(key) + ' COLLATE "C"' for key in keys)
        arguments = ",".join(f'${n}::text COLLATE "C"' for n in range(1, len(keys) + 1))
        selected = ",".join(
            f"CASE WHEN prepared_row_bytes<={_ROW_BYTES} THEN {storage_identifier(column)} END AS {storage_identifier(column)}"
            for column in columns
        )
        query = (
            f"WITH page AS MATERIALIZED (SELECT {fields},octet_length(to_jsonb(candidate)::text) AS prepared_row_bytes "
            f"FROM {relation} candidate WHERE ({order})>({arguments}) ORDER BY {order} LIMIT {COPY_MAX_ROWS}), "
            f"bounded AS (SELECT *,row_number() OVER (ORDER BY {order}) AS prepared_ordinal,"
            f"sum(prepared_row_bytes) OVER (ORDER BY {order}) AS prepared_page_bytes FROM page) "
            f"SELECT {selected},prepared_row_bytes FROM bounded "
            f"WHERE prepared_ordinal=1 OR prepared_page_bytes<={_PAGE_BYTES} ORDER BY {order}"
        )
        digest.update((storage._encoded({"table": root, "columns": columns, "keys": keys}) + "\n").encode())
        after, count = [""] * len(keys), 0
        while identity_page := await workspace.connection.fetch(query, *after):
            _require(len(identity_page) <= COPY_MAX_ROWS, "commitment_rows_unbounded")
            _require(
                all(0 < identity_record["prepared_row_bytes"] <= _ROW_BYTES for identity_record in identity_page),
                "commitment_row_unbounded",
            )
            _require(
                len(identity_page) == 1
                or sum(identity_record["prepared_row_bytes"] for identity_record in identity_page) <= _PAGE_BYTES,
                "commitment_page_unbounded",
            )
            for identity_record in identity_page:
                if root == resources.RESOURCE_IDENTITY_ROOT:
                    resources._require(
                        identity_record["entity_id"]
                        == source_resource_uuid(
                            identity_record["source_id"],
                            identity_record["resource_type"],
                            identity_record["resource_id"],
                        ),
                        "identity_changed",
                    )
                encoded = (storage._encoded(_typed_row(columns, identity_record)) + "\n").encode()
                _require(len(encoded) <= _ROW_BYTES, "commitment_row_unbounded")
                digest.update(encoded)
            next_keys = [identity_page[-1][key] for key in keys]
            _require(tuple(next_keys) > tuple(after), "commitment_key_not_advanced")
            after, count = next_keys, count + len(identity_page)
        counts[root] = count
    return counts, digest.hexdigest()


async def assert_prepared_cms_identities(workspace, handoff):
    """Verify all five private maps against exact frozen facts and accepted keys.

    This observes indexed outputs on the original Publisher. It never writes
    canonical rows, binds a Source context, issues authority or completes the
    CMS finisher. Later input, output or canonical changes require revalidation.
    """
    relations = await relationships._coordinates(workspace, handoff)
    _require(workspace.binding["indexed"], "indexes_required")
    receipts = workspace.receipts
    identity = workspace.identity
    entity_receipts_by_root = {root: receipts[root] for root in entities.CMS_IDENTITY_TABLES}
    entity_relations_by_root = {
        root: storage_relation(workspace.policy.schema, receipt["name"])
        for root, receipt in entity_receipts_by_root.items()
    }
    resource_root = resources.RESOURCE_IDENTITY_ROOT
    resource_stage = storage_relation(workspace.policy.schema, receipts[resource_root]["name"])
    witness = relations[storage.WITNESS]
    connection = workspace.connection
    async with _savepoint(connection):
        await _frozen_source_coordinates(workspace, relations)
        await relationships._require_twins(workspace, relations)
        await entities._source_coordinates(connection, workspace.policy, identity, entity_receipts_by_root)
        await entities.verify_identity_replay(connection, entity_relations_by_root, workspace.catalog_schema)
        await entities.verify_identity_relationships(connection, entity_relations_by_root)
        await entities.verify_identity_source_facts(connection, entity_relations_by_root, identity, witness)
        await entities.verify_canonical_identity_authority(
            connection, entity_relations_by_root, workspace.catalog_schema
        )
        await resources.verify_resource_identity_source(
            connection,
            resource_stage,
            witness,
            identity["dataset_id"],
            identity["metadata"]["source_release"]["vector_sha256"],
        )
        await resources._global_replay(connection, workspace.catalog_schema, resource_stage)
        await _resource_replay_coordinates(connection, resource_stage, workspace.catalog_schema)
        counts, content_sha256 = await _content_commitment(
            workspace, {**entity_relations_by_root, resource_root: resource_stage}
        )
        _require(await relationships._coordinates(workspace, handoff) == relations, "coordinates_changed")
    binding = workspace.binding
    return {
        "contract": CONTRACT,
        "state": "verified",
        "dataset_id": identity["dataset_id"],
        "release_id": identity["metadata"]["source_release"]["vector_sha256"],
        "handoff_sha256": handoff.document_sha256,
        "preparation_id": binding["preparation_id"],
        "admission_sha256": binding["admission_sha256"],
        "tables": {root: receipts[root] for root in counts},
        "row_counts": counts,
        "content_sha256": content_sha256,
        "content_contract": CONTENT_CONTRACT,
    }
