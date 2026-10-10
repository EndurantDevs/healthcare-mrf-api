# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected human review of a published complete-snapshot plan and approved association."""

import json

from sqlalchemy import text

from db.registry_schema import registry_schema
from process.ptg_parts.result_archive_source_authority import (
    commit_ptg_result_archive_source_authority,
)
from process.registry_company_approval_fence import (
    registry_company_approval_transaction,
    require_registry_company_approval_fence,
)
from process.registry_ptg_producer_scope import (
    RegistryPTGProducerScopeError,
    _canonical,
    _digest,
    _protected_store,
)
from process.registry_ptg_published_plan_contract import (
    AUTHORITY_CONTRACT,
    INTENT_FIELDS,
    OPERATION,
    SELECTION_MODE,
    validated_command,
    validated_intent,
)
from process.registry_ptg_published_plan_source import published_plan_inventory
from process.registry_ptg_published_provisioning import require_published_lock_privileges

TABLE = "registry_ptg_published_plan_scope"
CONTRACT = "registry_ptg_published_plan_scope.v1"


async def resolve_published_plan(session, schema_name, intent_by_field, ownership, actor, *, pin_writer=False):
    """Resolve an exact real plan and complete files without company/cohort relabeling."""
    intent_by_field = validated_intent(intent_by_field)
    await require_published_lock_privileges(session, schema_name, pin_writer=pin_writer)
    inventory = await published_plan_inventory(
        session,
        schema_name,
        ownership,
        operation_id="registry_ptg_published_review_" + intent_by_field["scope_id"].replace("-", ""),
    )
    if {
        "plan_id": intent_by_field["plan_id"],
        "plan_market_type": intent_by_field["plan_market_type"],
    } not in inventory["plan_scopes"] or intent_by_field["file_versions"] != inventory["file_versions"]:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    identity = inventory["identity"]
    full = validated_command(
        {
            **intent_by_field,
            "operation": OPERATION,
            "ownership": ownership,
            "published_identity": identity,
            "coordinates": {
                "source_system": "ptg",
                "source_id": identity["source_key"],
                "dataset_schema": schema_name,
                "dataset_id": identity["snapshot_id"],
                "producer_id": AUTHORITY_CONTRACT,
                "edition_id": identity["snapshot_manifest_sha256"],
            },
            "source": {
                "binding_source_key": identity["source_key"],
                "snapshot_id": identity["snapshot_id"],
                "ptg_schema_name": schema_name,
            },
        }
    )
    evidence_by_field = {
        "source_authority": inventory["authority"],
        "selected_source_keys": inventory["source_keys"],
        "selection_mode": SELECTION_MODE,
    }
    return full, inventory["authority"], evidence_by_field


def _document(full, actor, evidence_by_field):
    statement_by_field = {
        "contract": "authenticated_published_plan_review.v1",
        "statement_id": full["statement_id"],
        "actor": actor,
        "command": full,
        "evidence": evidence_by_field,
    }
    return {
        "contract": CONTRACT,
        "scope_id": full["scope_id"],
        "idempotency_key": full["idempotency_key"],
        "actor": actor,
        "command": full,
        "evidence": evidence_by_field,
        "producer_statement_id": "published-operator-review:v1:" + full["statement_id"],
        "producer_statement_sha256": _digest(statement_by_field),
    }


def _parameters(document):
    command = document["command"]
    return {
        "scope_id": document["scope_id"],
        "actor_key": _digest(document["actor"]),
        "idempotency_key": document["idempotency_key"],
        "approval_sha256": _digest(document),
        "document": _canonical(document),
        "scope_key": _digest(
            {
                name: command[name]
                for name in (
                    "client_id",
                    "legal_company_id",
                    "network_id",
                    "coordinates",
                    "plan_id",
                    "plan_market_type",
                    "selection_mode",
                )
            }
        ),
    }


async def _existing(session, table, parameters_by_field):
    return (
        (
            await session.execute(
                text(f"""
        SELECT approval_sha256,approval_json FROM {table}
        WHERE scope_id=CAST(:scope_id AS uuid) OR scope_key=:scope_key
           OR (actor_key=:actor_key AND idempotency_key=:idempotency_key)
    """),
                parameters_by_field,
            )
        )
        .mappings()
        .all()
    )


def _checked_replay(stored_reviews, parameters_by_field, document):
    if (
        len(stored_reviews) != 1
        or stored_reviews[0]["approval_sha256"] != parameters_by_field["approval_sha256"]
        or stored_reviews[0]["approval_json"] != document
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_idempotency_conflict")


async def _approved_association(session, table, full):
    namespace = table.rsplit(".", 1)[0]
    await require_registry_company_approval_fence(session, namespace[1:-1])
    matches = (
        await session.execute(
            text(f"""
        SELECT EXISTS (
          SELECT FROM {namespace}.registry_revision_control current
          JOIN {namespace}.registry_approved_record company ON company.approved_revision=current.approved_revision
          JOIN {namespace}.registry_approved_record network ON network.approved_revision=current.approved_revision
          JOIN {namespace}.registry_approved_record links ON links.approved_revision=current.approved_revision
          WHERE current.id=1 AND current.approved_revision=:revision
            AND company.record_kind='company' AND company.record_key=:company
            AND company.record_json->>'company_id'=:company AND company.record_json->'archived'='false'::jsonb
            AND network.record_kind='network' AND network.record_key=:network
            AND network.record_json->>'network_id'=:network AND network.record_json->'archived'='false'::jsonb
            AND links.record_kind='company_links' AND links.record_key=:company
            AND links.record_json->>'company_id'=:company
            AND links.record_json->'network_ids' @> CAST(:network_ids AS jsonb)
        )
    """),
            {
                "revision": full["approved_revision"],
                "company": full["legal_company_id"],
                "network": str(full["network_id"]),
                "network_ids": json.dumps([full["network_id"]]),
            },
        )
    ).scalar_one()
    if matches is not True:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_company_unapproved")


async def approve_published_plan(service, envelope, deadline, state):
    """Pin and append only after protected source/association and fresh app authority."""
    from process.registry_ptg_scope_engine import _remaining

    async with registry_company_approval_transaction(
        service.approval_sessions, control_schema=service.store.control_schema or registry_schema()
    ) as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        table = await _protected_store(session, service.store, write=True, table_name=TABLE)
        intent_by_field = {name: envelope["command"][name] for name in INTENT_FIELDS}
        full, authority, evidence_by_field = await resolve_published_plan(
            session,
            service.schema_name,
            intent_by_field,
            envelope["command"]["ownership"],
            envelope["actor"],
            pin_writer=True,
        )
        if full != envelope["command"]:
            raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
        document = _document(full, envelope["actor"], evidence_by_field)
        parameters_by_field = _parameters(document)
        stored_reviews = await _existing(session, table, parameters_by_field)
        if stored_reviews:
            _checked_replay(stored_reviews, parameters_by_field, document)
        else:
            await _approved_association(session, table, full)
        _remaining(deadline)
        await service.authority.authorize(envelope, deadline=deadline)
        _remaining(deadline)
        await commit_ptg_result_archive_source_authority(session, schema_name=service.schema_name, authority=authority)
        await session.execute(
            text(f"""
            INSERT INTO {table}(scope_id,scope_key,actor_key,idempotency_key,approval_sha256,approval_json)
            VALUES(CAST(:scope_id AS uuid),:scope_key,:actor_key,:idempotency_key,:approval_sha256,CAST(:document AS jsonb))
            ON CONFLICT DO NOTHING
        """),
            parameters_by_field,
        )
        _checked_replay(await _existing(session, table, parameters_by_field), parameters_by_field, document)
        _remaining(deadline)
        state["commit_started"] = True
    return {name: full[name] for name in ("scope_id", "statement_id", "client_id")} | {
        "command_sha256": _digest(full),
        "approval_sha256": parameters_by_field["approval_sha256"],
        "producer_statement_sha256": document["producer_statement_sha256"],
    }


async def read_registry_ptg_published_plan_scope(session, *, scope_id, client_id, approval_sha256, store):
    """Reconstruct an exact retained human review and original operation-owned pin."""
    from process.ptg_parts.result_archive_published_authority import (
        lock_ptg_published_result_for_clone,
    )
    from process.registry_ptg_published_plan_contract import _digest as check_digest
    from process.registry_ptg_published_plan_contract import _uuid

    _uuid(str(scope_id))
    check_digest(approval_sha256)
    table = await _protected_store(session, store, write=False, table_name=TABLE)
    return await _read_protected_published_scope(session, table, scope_id, client_id, approval_sha256)


async def _read_protected_published_scope(session, table, scope_id, client_id, approval_sha256):
    """Reconstruct the original proof after a caller verified the exact store role."""
    from process.ptg_parts.result_archive_published_authority import (
        lock_ptg_published_result_for_clone,
    )
    from process.registry_ptg_published_plan_contract import _digest as check_digest
    from process.registry_ptg_published_plan_contract import _uuid

    _uuid(str(scope_id))
    check_digest(approval_sha256)
    stored = (
        (
            await session.execute(
                text(f"SELECT approval_sha256,approval_json FROM {table} WHERE scope_id=CAST(:scope_id AS uuid)"),
                {"scope_id": str(scope_id)},
            )
        )
        .mappings()
        .one_or_none()
    )
    if stored is None or stored["approval_sha256"] != approval_sha256:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_unavailable")
    document = stored["approval_json"]
    if type(document) is not dict or _digest(document) != approval_sha256:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_changed")
    full = validated_command(document["command"])
    if full["scope_id"] != str(scope_id) or full["client_id"] != client_id:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_changed")
    current, authority, evidence = await resolve_published_plan(
        session,
        full["source"]["ptg_schema_name"],
        {name: full[name] for name in INTENT_FIELDS},
        full["ownership"],
        document["actor"],
    )
    if current != full or _document(full, document["actor"], evidence) != document:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    retained = await lock_ptg_published_result_for_clone(
        session, schema_name=full["source"]["ptg_schema_name"], authority=authority
    )
    if retained.as_dict() != authority:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    return {**document, "approval_sha256": approval_sha256}
