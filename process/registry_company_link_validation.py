# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preflight benefit authority and recheck explicit local company-link targets."""

import json

from sqlalchemy import Integer, and_, any_, cast, column, func, literal, or_, select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from db.models.company_group_registry import CompanyGroupRegistry
from db.models.company_registry import CompanyRegistry
from db.models.company_registry_links import CompanyRegistryLinks
from db.models.network_registry import NetworkRegistryIdentity, NetworkRegistryRecord
from process.registry_benefit_plan_metadata import collect_benefit_plan_ids, validate_benefit_plan_ids


async def _preflight_company_link_benefits(session, command, fields, history, metadata, schema, client, actor):
    """Read authority before locks; the caller still rechecks its exact local head."""
    from process.registry_record_store import _read_record_replay

    replay = await _read_record_replay(session, history, command, command.record_id, metadata)
    if replay is not None:
        return replay
    metadata["operation_context"].update(
        await validate_benefit_plan_ids(await _company_link_benefits(session, command, fields, schema), client, actor)
    )
    return None


async def _company_link_benefits(session, command, fields, schema):
    from process.registry_record_store import _table, _validated_network_assertions

    if command.operation == "archive":
        return ()
    if "network_assertions" in fields:
        fields["network_assertions"] = _validated_network_assertions(fields["network_assertions"])
        assertions = fields["network_assertions"]
    else:
        links = _table(CompanyRegistryLinks, schema)
        assertions = await session.scalar(
            select(links.c.network_assertions).where(
                links.c.company_id == command.record_id,
                links.c.revision == command.expected_revision,
            )
        )
    return collect_benefit_plan_ids(assertions or [])


def _company_assertion_scope(links, command, fields, network_ids):
    """Validate explicit assertion targets without expanding a legacy correction."""
    retained_assertions = (
        select(links.c.network_assertions)
        .where(links.c.company_id == command.record_id, links.c.revision == command.expected_revision)
        .scalar_subquery()
    )
    assertions = (
        literal(fields.get("network_assertions", []), type_=links.c.network_assertions.type)
        if command.operation == "create" or "network_assertions" in fields
        else func.coalesce(retained_assertions, literal([], type_=links.c.network_assertions.type))
    )
    entries = func.jsonb_array_elements(assertions).table_valued(column("value", links.c.network_assertions.type))
    company_id = entries.c.value["company_id"].astext
    network_id = cast(entries.c.value["network_id"].astext, Integer)
    return (
        ~select(literal(1))
        .select_from(entries)
        .where(
            or_(
                company_id.is_distinct_from(str(command.record_id)),
                network_id.is_(None),
                ~(network_id == any_(network_ids)),
            )
        )
        .exists()
    )


async def _validate_company_link_targets(session, command, fields, schema):
    """Validate active references and allocated networks together under the edit lock."""
    from process.registry_record_store import RegistryRecordConflict, _table

    companies = _table(CompanyRegistry, schema)
    groups = _table(CompanyGroupRegistry, schema)
    networks = _table(NetworkRegistryRecord, schema)
    identities = _table(NetworkRegistryIdentity, schema)
    links = _table(CompanyRegistryLinks, schema)
    if command.operation in {"create", "correct"}:
        network_ids = literal(fields["network_ids"], type_=ARRAY(Integer))
        group_id = literal(fields["group_id"], type_=PG_UUID(as_uuid=True))
    else:
        links = _table(CompanyRegistryLinks, schema)
        retained = select(links.c.network_ids, links.c.group_id).where(links.c.company_id == command.record_id).cte()
        network_ids, group_id = retained.c.network_ids, retained.c.group_id
    company_active = (
        select(companies.c.company_id)
        .where(companies.c.company_id == command.record_id, companies.c.archived.is_(False))
        .exists()
    )
    group_active = select(groups.c.group_id).where(groups.c.group_id == group_id, groups.c.archived.is_(False)).exists()
    active_network_count = (
        select(func.count())
        .select_from(networks.join(identities, identities.c.network_id == networks.c.network_id))
        .where(networks.c.network_id == any_(network_ids), networks.c.archived.is_(False))
        .scalar_subquery()
    )
    valid = await session.scalar(
        select(
            and_(
                company_active,
                or_(group_id.is_(None), group_active),
                active_network_count == func.cardinality(network_ids),
                _company_assertion_scope(links, command, fields, network_ids),
            )
        )
    )
    if valid is None and command.operation not in {"create", "correct"}:
        raise RegistryRecordConflict("registry_record_revision_conflict")
    if valid is not True:
        raise ValueError("registry_company_links_target_invalid")


def _validate_company_link_snapshot(snapshot_by_field, reason):
    """Keep every single-record history page and undo target within its read bound."""
    from process.registry_manual_undo import MAX_UNDO_SNAPSHOT_BYTES

    snapshot_bytes = len(json.dumps(snapshot_by_field, ensure_ascii=False, allow_nan=False).encode())
    # Default JSON separators match JSONB text size for this closed primitive DTO.
    # Reserve the actual escaped reason and the largest supported history counters.
    history_by_field = {
        "record_kind": "company_links",
        "record_id": snapshot_by_field["company_id"],
        "current_revision": 9223372036854775807,
        "limit": 1,
        "offset": 1000000,
        "has_more": False,
        "records": [
            {
                "revision": snapshot_by_field["revision"],
                "custom_revision": 9223372036854775807,
                "reason": reason.strip(),
                "record": snapshot_by_field,
            }
        ],
    }
    page_bytes = len(json.dumps(history_by_field, ensure_ascii=False, allow_nan=False).encode())
    if max(snapshot_bytes + len(reason.strip().encode()) + 256, page_bytes) > MAX_UNDO_SNAPSHOT_BYTES:
        raise ValueError("registry_company_links_snapshot_limit")
