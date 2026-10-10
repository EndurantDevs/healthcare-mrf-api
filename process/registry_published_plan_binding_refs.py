# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consume protected published-plan reviews for binding metadata, never offices."""

import json

from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from process.registry_ptg_published_plan_contract import (
    REVIEW_TYPE,
    SELECTION_MODE,
    _digest,
    _text,
    _uuid,
    validated_command,
)

_SCOPE_FIELDS = {
    "review_type",
    "scope_id",
    "approval_sha256",
    "snapshot_id",
    "plan_id",
    "plan_market_type",
    "selection_mode",
}
_MAX_BUNDLE_BYTES = 8 * 1024 * 1024


def validated_published_plan_binding_scope(scope):
    """Validate a distinct closed namespace without inventing cohort fields."""
    if type(scope) is not dict or set(scope) != _SCOPE_FIELDS:
        raise ValueError("registry_published_plan_binding_invalid")
    if scope["review_type"] != REVIEW_TYPE or scope["selection_mode"] != SELECTION_MODE:
        raise ValueError("registry_published_plan_binding_invalid")
    _uuid(scope["scope_id"])
    _digest(scope["approval_sha256"])
    for name, maximum in (
        ("snapshot_id", 96),
        ("plan_id", 64),
        ("plan_market_type", 32),
    ):
        _text(scope[name], maximum, characters=True)
    if scope["plan_market_type"] != scope["plan_market_type"].lower():
        raise ValueError("registry_published_plan_binding_invalid")
    return dict(scope)


def _unique_object(pairs):
    fields_by_name = dict(pairs)
    if len(fields_by_name) != len(pairs):
        raise ValueError("registry_published_plan_binding_invalid")
    return fields_by_name


def published_plan_binding_requests(input_bytes):
    """Select only the explicit new namespace; legacy bindings stay unchanged."""
    if type(input_bytes) is not bytes or len(input_bytes) > _MAX_BUNDLE_BYTES:
        raise ValueError("registry_published_plan_binding_invalid")
    rows = json.loads(input_bytes, object_pairs_hook=_unique_object)
    if type(rows) is not list or len(rows) > 5000:
        raise ValueError("registry_published_plan_binding_invalid")
    requested_rows = []
    for row in rows:
        scope = row.get("source_scope_json") if type(row) is dict else None
        if type(scope) is dict and "review_type" in scope:
            validated_published_plan_binding_scope(scope)
            requested_rows.append(row)
    return requested_rows


async def _protected_reviews(connection, namespace, store):
    from db.registry_schema import registry_schema
    from process.network_address_projection import _identifier
    from process.registry_ptg_producer_scope import (
        _PERMISSIONS_SQL,
        RegistryPTGProducerScopeStore,
    )

    if (
        type(store) is not RegistryPTGProducerScopeStore
        or (store.control_schema or registry_schema()) != namespace[1:-1]
    ):
        raise ValueError("registry_published_plan_binding_store_unprotected")
    if not connection.is_in_transaction() or await connection.fetchval("SHOW transaction_isolation") not in {
        "repeatable read",
        "serializable",
    }:
        raise ValueError("registry_published_plan_binding_transaction_required")
    for role in (store.owner_role, store.approval_role):
        _identifier(role)
    statement = text(_PERMISSIONS_SQL.replace("'registry_ptg_producer_scope'", "'registry_ptg_published_plan_scope'"))
    compiled = statement.compile(dialect=PGDialect_asyncpg())
    parameters_by_name = {
        "schema_name": namespace[1:-1],
        "owner_role": store.owner_role,
        "approval_role": store.approval_role,
        "write": False,
    }
    if (
        await connection.fetchval(str(compiled), *(parameters_by_name[name] for name in compiled.positiontup))
        is not True
    ):
        raise ValueError("registry_published_plan_binding_store_unprotected")


_SIZE_CONSTRAINT_SQL = """SELECT EXISTS (
 SELECT FROM pg_catalog.pg_constraint constraint_row
 JOIN pg_catalog.pg_class relation ON relation.oid=constraint_row.conrelid
 JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
 JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid=relation.oid
   AND attribute.attname='approval_json' AND NOT attribute.attisdropped
 WHERE namespace.nspname=$1 AND relation.relname='registry_ptg_published_plan_scope'
   AND constraint_row.contype='c' AND constraint_row.convalidated
   AND coalesce((to_jsonb(constraint_row)->>'conenforced')::boolean,true)
   AND constraint_row.conkey=ARRAY[attribute.attnum]::smallint[]
   AND regexp_replace(pg_catalog.pg_get_expr(constraint_row.conbin,constraint_row.conrelid),
      '[[:space:]()]','','g')=
      'jsonb_typeofapproval_json=''object''::textANDoctet_lengthapproval_json::text<=131072'
)"""


_REVIEWS_SQL = """WITH requested AS MATERIALIZED (
 SELECT DISTINCT scope_id,approval_sha256 FROM jsonb_to_recordset($1::jsonb)
   AS ref(scope_id uuid,approval_sha256 text)
), reviews AS MATERIALIZED (
 SELECT requested.*,review.approval_json FROM requested
 LEFT JOIN {namespace}.registry_ptg_published_plan_scope review USING(scope_id,approval_sha256)
), bounds AS (
 SELECT count(*)=count(approval_json) AS complete,
   coalesce(sum(octet_length(approval_json::text)+256),0)<=$2 AS bounded FROM reviews
)
SELECT complete,bounded,CASE WHEN complete AND bounded THEN
 (SELECT jsonb_agg(jsonb_build_object('scope_id',scope_id::text,'approval_sha256',approval_sha256,
   'document',approval_json) ORDER BY scope_id) FROM reviews)::text END AS documents_json FROM bounds"""


def _complete_source_keys(source_keys, source_count):
    return (
        type(source_keys) is list
        and len(source_keys) == source_count
        and all(type(key) is int and 0 <= key < 2**63 for key in source_keys)
        and len(set(source_keys)) == source_count
    )


def _review_commands(documents_json):
    from process.ptg_parts.result_archive_published_authority import (
        validate_ptg_published_result_source_authority,
    )
    from process.registry_ptg_published_plan_scope import _digest as document_digest
    from process.registry_ptg_published_plan_scope import _document

    command_by_review = {}
    for entry in json.loads(documents_json, object_pairs_hook=_unique_object):
        if type(entry) is not dict or set(entry) != {
            "scope_id",
            "approval_sha256",
            "document",
        }:
            raise ValueError("registry_published_plan_binding_review_changed")
        document = entry["document"]
        if type(document) is not dict or not {"command", "actor", "evidence"} <= set(document):
            raise ValueError("registry_published_plan_binding_review_changed")
        if type(document["evidence"]) is not dict or set(document["evidence"]) != {
            "source_authority",
            "selected_source_keys",
            "selection_mode",
        }:
            raise ValueError("registry_published_plan_binding_review_changed")
        command = validated_command(document["command"])
        authority = validate_ptg_published_result_source_authority(document["evidence"]["source_authority"])
        if (
            document_digest(document) != entry["approval_sha256"]
            or document != _document(command, document["actor"], document["evidence"])
            or command["scope_id"] != entry["scope_id"]
            or authority["identity"] != command["published_identity"]
            or document["evidence"]["selection_mode"] != SELECTION_MODE
            or not _complete_source_keys(
                document["evidence"]["selected_source_keys"],
                command["published_identity"]["source_count"],
            )
        ):
            raise ValueError("registry_published_plan_binding_review_changed")
        identity = (entry["scope_id"], entry["approval_sha256"])
        if identity in command_by_review:
            raise ValueError("registry_published_plan_binding_review_changed")
        command_by_review[identity] = command
    return command_by_review


def _require_reference(row, command):
    scope = validated_published_plan_binding_scope(row["source_scope_json"])
    if (
        row["source_system"] != "ptg"
        or any(row[name] != value for name, value in command["coordinates"].items())
        or row["source_key"] != command["source"]["binding_source_key"]
        or row["network_id"] != command["network_id"]
        or scope["snapshot_id"] != command["source"]["snapshot_id"]
        or any(
            scope[name] != command[name]
            for name in (
                "review_type",
                "scope_id",
                "plan_id",
                "plan_market_type",
                "selection_mode",
            )
        )
        or row["evidence_id"] != "published-plan-review:" + scope["scope_id"]
        or row["evidence_sha256"] != scope["approval_sha256"]
    ):
        raise ValueError("registry_published_plan_binding_review_changed")


async def _require_published_plan_reviews(connection, namespace, requests, store):
    """Recheck every review together, including exact-byte replay, before writes.

    This is protected review/metadata evidence only. Physical graph/source and
    explicit office correspondence must still pass the membership recipe path.
    """
    await _protected_reviews(connection, namespace, store)
    if await connection.fetchval(_SIZE_CONSTRAINT_SQL, namespace[1:-1]) is not True:
        raise ValueError("registry_published_plan_binding_store_unbounded")
    refs = [{name: row["source_scope_json"][name] for name in ("scope_id", "approval_sha256")} for row in requests]
    result = await connection.fetchrow(_REVIEWS_SQL.format(namespace=namespace), json.dumps(refs), _MAX_BUNDLE_BYTES)
    if result is None or result["complete"] is not True or result["bounded"] is not True:
        raise ValueError("registry_published_plan_binding_review_unavailable")
    encoded = result["documents_json"]
    if type(encoded) is not str or len(encoded.encode()) > _MAX_BUNDLE_BYTES:
        raise ValueError("registry_published_plan_binding_review_unavailable")
    reviews = _review_commands(encoded)
    if set(reviews) != {(ref["scope_id"], ref["approval_sha256"]) for ref in refs}:
        raise ValueError("registry_published_plan_binding_review_changed")
    for row in requests:
        scope = row["source_scope_json"]
        _require_reference(row, reviews[(scope["scope_id"], scope["approval_sha256"])])


async def require_published_plan_binding_references(connection, namespace, input_bytes, store):
    """Authenticate protected binding metadata only; never source/office admission."""
    requests = published_plan_binding_requests(input_bytes)
    if not requests:
        return
    # Savepoint rollback also restores the caller's path on error/cancellation.
    async with connection.transaction():
        original_path = await connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
        await connection.fetchval("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        await _require_published_plan_reviews(connection, namespace, requests, store)
        await connection.fetchval("SELECT pg_catalog.set_config('search_path',$1,true)", original_path)
