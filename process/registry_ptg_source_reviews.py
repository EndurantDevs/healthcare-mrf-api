# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded rediscovery of original protected published-plan reviews."""

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.ptg_parts.result_archive_published_authority import _authority_from_identity
from process.ptg_parts.result_archive_source_authority import (
    PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE,
    _bounded_clone_lock_reads,
)
from process.registry_company_approval_fence import require_registry_company_approval_fence
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeError, _canonical, _digest, _protected_store
from process.registry_ptg_published_plan_contract import INTENT_FIELDS, validated_command
from process.registry_ptg_published_plan_scope import TABLE, _document, resolved_published_plan_inventory
from process.registry_ptg_published_plan_source import published_plan_inventory
from process.registry_ptg_published_provisioning import require_published_lock_privileges
from process.registry_record_store import _bounded_name

MAX_PAGE_BYTES = 245760


async def _review_rows(session, table, ownership, selection):
    namespace = table.rsplit(".", 1)[0]
    await require_registry_company_approval_fence(session, namespace[1:-1])
    return (
        (
            await session.execute(
                text(f"""
        SELECT review.scope_id, review.approval_sha256,
          CASE WHEN octet_length(review.approval_json::text)<=:byte_limit THEN review.approval_json END AS approval_json,
          CASE WHEN char_length(company.record_json->>'display_name')<=512
            THEN company.record_json->>'display_name' END AS company_name,
          CASE WHEN char_length(network.record_json->>'display_name')<=512
            THEN network.record_json->>'display_name' END AS network_name,
          (current.approved_revision::text=review.approval_json->'command'->>'approved_revision'
            AND company.record_json->>'company_id'=company.record_key
            AND company.record_json->'archived'='false'::jsonb
            AND network.record_json->>'network_id'=network.record_key
            AND network.record_json->'archived'='false'::jsonb
            AND links.record_json->>'company_id'=company.record_key
            AND links.record_json->'network_ids' @> jsonb_build_array(
              review.approval_json->'command'->'network_id')) AS association_current
        FROM {table} review
        LEFT JOIN {namespace}.registry_revision_control current ON current.id=1
        LEFT JOIN {namespace}.registry_approved_record company
          ON company.approved_revision=current.approved_revision AND company.record_kind='company'
          AND company.record_key=review.approval_json->'command'->>'legal_company_id'
        LEFT JOIN {namespace}.registry_approved_record network
          ON network.approved_revision=current.approved_revision AND network.record_kind='network'
          AND network.record_key=review.approval_json->'command'->>'network_id'
        LEFT JOIN {namespace}.registry_approved_record links
          ON links.approved_revision=current.approved_revision AND links.record_kind='company_links'
          AND links.record_key=company.record_key
        WHERE review.approval_json->'command'->>'client_id'=:client_id
          AND review.approval_json->'command'->>'source_file_import_id'=:source_file_import_id
          AND review.approval_json->'command'->'ownership'=CAST(:ownership AS jsonb)
          AND (CAST(:after AS uuid) IS NULL OR review.scope_id>CAST(:after AS uuid))
        ORDER BY review.scope_id LIMIT :maximum
    """),
                {
                    **selection,
                    "ownership": _canonical(ownership),
                    "maximum": selection["limit"] + 1,
                    "byte_limit": MAX_PAGE_BYTES,
                },
            )
        )
        .mappings()
        .all()
    )


def _label(value):
    """Project bounded writer-valid names without changing approved review identity."""
    try:
        if type(value) is not str or value != _bounded_name(value, 512, "display_name"):
            raise ValueError
    except ValueError:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_changed") from None
    display = "".join(
        " " if character.isspace() else character if character.isprintable() else "�" for character in value
    )
    encoded = display.encode("utf-8")
    if len(encoded) <= 512:
        return display
    return encoded[:509].decode("utf-8", errors="ignore").rstrip() + "…"


def _verified_review(approval_record, schema_name, ownership, inventory):
    document = approval_record["approval_json"]
    if (
        type(document) is not dict
        or set(document)
        != {
            "contract",
            "scope_id",
            "idempotency_key",
            "actor",
            "command",
            "evidence",
            "producer_statement_id",
            "producer_statement_sha256",
        }
        or _digest(document) != approval_record["approval_sha256"]
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_changed")
    full = validated_command(document["command"])
    if full["scope_id"] != str(approval_record["scope_id"]) or full["ownership"] != ownership:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    authority = _authority_from_identity(
        "registry_ptg_published_review_" + full["scope_id"].replace("-", ""), inventory["identity"]
    )
    current, _, evidence = resolved_published_plan_inventory(
        schema_name,
        {name: full[name] for name in INTENT_FIELDS},
        ownership,
        {**inventory, "authority": authority.as_dict()},
    )
    if current != full or _document(full, document["actor"], evidence) != document:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    if approval_record["association_current"] is not True:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_company_unapproved")
    receipt = {name: full[name] for name in ("scope_id", "statement_id", "client_id")} | {
        "command_sha256": _digest(full),
        "approval_sha256": approval_record["approval_sha256"],
        "producer_statement_sha256": document["producer_statement_sha256"],
    }
    return {
        "command": full,
        "approval": receipt,
        "labels": {
            "company_name": _label(approval_record["company_name"]),
            "network_name": _label(approval_record["network_name"]),
            "plan_id": full["plan_id"],
            "plan_market_type": full["plan_market_type"],
        },
    }, authority


async def _require_review_pins(session, schema_name, authorities):
    if not authorities:
        return
    _identifier(schema_name)
    expected_by_owner = {authority.owner_id: (authority.snapshot_id, authority.pin_reason) for authority in authorities}
    async with _bounded_clone_lock_reads(session):
        pins = (
            (
                await session.execute(
                    text(f"""
            SELECT owner_id,snapshot_id,reason FROM "{schema_name}".ptg2_snapshot_pin
            WHERE owner_type=:owner_type AND owner_id=ANY(CAST(:owners AS text[]))
            ORDER BY owner_id,snapshot_id LIMIT 21 FOR UPDATE
        """),
                    {"owner_type": PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE, "owners": sorted(expected_by_owner)},
                )
            )
            .mappings()
            .all()
        )
    actual_pins = [(pin["owner_id"], pin["snapshot_id"], pin["reason"]) for pin in pins]
    required_pins = [(owner, *expected_by_owner[owner]) for owner in sorted(expected_by_owner)]
    if actual_pins != required_pins:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")


def _page(source_by_field, items, next_cursor):
    return {"source": source_by_field, "items": items, "next_cursor": next_cursor}


def _bounded_reviews(approval_records, schema_name, ownership, selection, inventory):
    source_by_field = {
        "ownership": ownership,
        "ptg_schema_name": schema_name,
        "published_identity": inventory["identity"],
    }
    items, authorities = [], []
    for approval_record in approval_records[: selection["limit"]]:
        item, authority = _verified_review(approval_record, schema_name, ownership, inventory)
        candidate = _page(source_by_field, items + [item], item["command"]["scope_id"])
        if len(_canonical(candidate).encode("utf-8")) > MAX_PAGE_BYTES:
            if not items:
                raise RegistryPTGProducerScopeError("registry_ptg_scope_changed")
            break
        items.append(item)
        authorities.append(authority)
    next_cursor = items[-1]["command"]["scope_id"] if len(approval_records) > len(items) else None
    return _page(source_by_field, items, next_cursor), authorities


async def read_registry_ptg_source_reviews(session, schema_name, ownership, selection, store):
    """Read one source-bound page; caller must authorize the whole returned batch."""
    table = await _protected_store(session, store, write=False, table_name=TABLE)
    await require_published_lock_privileges(session, schema_name, pin_writer=False)
    inventory = await published_plan_inventory(
        session, schema_name, ownership, operation_id="registry_ptg_inventory_" + _digest(ownership)
    )
    approval_records = await _review_rows(session, table, ownership, selection)
    page, authorities = _bounded_reviews(approval_records, schema_name, ownership, selection, inventory)
    await _require_review_pins(session, schema_name, authorities)
    if len(_canonical(page).encode("utf-8")) > MAX_PAGE_BYTES:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_changed")
    return page
