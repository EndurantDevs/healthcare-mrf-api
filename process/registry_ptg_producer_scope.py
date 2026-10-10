# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected producer approvals, separate from network mapping and office proof.

Only a dedicated approval-role action may call ``approve_registry_ptg_producer_scope``.
That action must authenticate the actor, authorize the owning client, and verify an
explicit statement binding its producer/edition, legal company/cohort and complete
selected immutable file-version identities. An operator review records a human
assertion, never a carrier signature. Its upstream action must recheck the live
administrator session and normalized source-import client admission. Statement IDs,
ownership witnesses and digests are audit coordinates, never authentication. No
such upstream action is installed here;
missing producer authority must remain unresolved rather than infer it from names,
ownership, a network binding or a caller's dense source keys.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from uuid import UUID

from sqlalchemy import text

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.ptg_parts.ptg2_candidate_attestation import CANDIDATE_SOURCE_RECORDS_SQL
from process.ptg_parts.result_archive_source_authority import (
    PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT,
)
from process.registry_company_approval_fence import (
    require_registry_company_approval_fence,
)
from process.registry_ptg_cohort_authority import (
    _physical_binding,
    _require_frozen_source,
    _source_state,
)
from process.registry_record_store import RegistryActor, _validated_actor

CONTRACT = "registry_ptg_producer_scope.v1"
OPERATOR_REVIEW_CONTRACT = "authenticated_operator_review.v1"
TABLE = "registry_ptg_producer_scope"
_MAX_FILES = 128
_MAX_DOCUMENT_BYTES = 131072


class RegistryPTGProducerScopeError(ValueError):
    """A missing, conflicting or unprotected producer approval cannot admit scope."""


@dataclass(frozen=True)
class RegistryPTGProducerFileVersion:
    source_file_version_id: str
    source_identity_sha256: str
    raw_sha256: str


@dataclass(frozen=True)
class RegistryPTGProducerScopeCommand:
    """Explicit authenticated action input; contains no dense source keys."""

    scope_id: UUID
    coordinates: RegistryNetworkSourceCoordinates
    client_id: str
    legal_company_id: UUID
    approved_revision: int
    producer_statement_id: str
    producer_statement_sha256: str
    source_file_import_id: str
    file_versions: tuple[RegistryPTGProducerFileVersion, ...]
    reason: str
    idempotency_key: str


@dataclass(frozen=True)
class RegistryPTGSourceOwnershipWitness:
    """Actual controller row joined to its normalized source_file_import_client.

    The trusted action verifies admission; this value does not authenticate it.
    """

    source_file_import_id: str
    client_id: str
    source_file_id: str
    content_version: str
    import_month: str
    assigned_node_id: str
    status: str
    engine_run_id: str
    snapshot_id: str
    source_key: str
    engine_source_identity_hash: str
    engine_source_file_version_id: str


@dataclass(frozen=True)
class RegistryPTGOperatorReviewCommand:
    """Explicit human assertion input; accepts neither statement digest nor keys."""

    scope_id: UUID
    coordinates: RegistryNetworkSourceCoordinates
    client_id: str
    legal_company_id: UUID
    approved_revision: int
    source_file_import_id: str
    file_versions: tuple[RegistryPTGProducerFileVersion, ...]
    reason: str
    idempotency_key: str
    statement_id: UUID
    ownership: RegistryPTGSourceOwnershipWitness


@dataclass(frozen=True)
class RegistryPTGProducerScopeStore:
    """Explicit protected role boundary; does not grant any role or privilege."""

    owner_role: str
    approval_role: str
    control_schema: str | None = None


def _canonical(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _digest(document):
    return hashlib.sha256(_canonical(document).encode()).hexdigest()


def _required_text(field_value, maximum):
    if (
        type(field_value) is not str
        or not 1 <= len(field_value.encode()) <= maximum
        or field_value.strip() != field_value
        or any(not character.isprintable() for character in field_value)
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
    return field_value


def _sha256(field_value):
    if type(field_value) is not str or re.fullmatch(r"[0-9a-f]{64}", field_value) is None:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
    return field_value


def _engine_identity(field_value):
    from process.ptg_parts.frozen_rate_files import _ENGINE_ID_PATTERN

    if type(field_value) is not str or _ENGINE_ID_PATTERN.fullmatch(field_value) is None:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
    return field_value


def _command_document(command, specification, actor):
    if (
        type(command) not in {RegistryPTGProducerScopeCommand, RegistryPTGOperatorReviewCommand}
        or type(actor) is not RegistryActor
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_authority_unavailable")
    actor_document = _validated_actor(actor)
    if actor.kind == "client_owner" and actor.client_id != command.client_id:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_owner_changed")
    if actor.kind == "platform_admin" and (actor.client_id != "system" or actor.impersonator_id is not None):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_actor_invalid")
    if any(
        type(field_value) is not UUID or not field_value.int
        for field_value in (command.scope_id, command.legal_company_id)
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
    if type(command.approved_revision) is not int or not 1 <= command.approved_revision < 2**63:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
    if (
        type(command.coordinates) is not RegistryNetworkSourceCoordinates
        or command.coordinates.source_system != "ptg"
        or command.coordinates.dataset_schema != specification.ptg_schema_name
        or type(command.file_versions) is not tuple
        or not 1 <= len(command.file_versions) <= _MAX_FILES
        or any(type(version) is not RegistryPTGProducerFileVersion for version in command.file_versions)
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
    versions = [
        {
            "source_file_version_id": _required_text(version.source_file_version_id, 128),
            "source_identity_sha256": _engine_identity(version.source_identity_sha256),
            "raw_sha256": _sha256(version.raw_sha256),
        }
        for version in command.file_versions
    ]
    versions.sort(key=lambda version: version["source_file_version_id"])
    if len({version["source_file_version_id"] for version in versions}) != len(versions):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_ambiguous")
    command_by_field = {
        "contract": CONTRACT,
        "scope_id": str(command.scope_id),
        "coordinates": asdict(command.coordinates),
        "client_id": _required_text(command.client_id, 64),
        "legal_company_id": str(command.legal_company_id),
        "approved_revision": command.approved_revision,
        "company_key": _required_text(specification.company_key, 512),
        "cohort_id": _required_text(specification.cohort_id, 128),
        "binding_source_key": _required_text(specification.binding_source_key, 512),
        "snapshot_id": _required_text(specification.snapshot_id, 96),
        "source_file_import_id": _required_text(command.source_file_import_id, 64),
        "file_versions": versions,
        "actor": actor_document,
        "reason": _required_text(command.reason, 1000),
        "idempotency_key": _required_text(command.idempotency_key, 128),
    }
    return _statement_command_document(command, command_by_field, actor)


def _statement_command_document(command, command_by_field, actor):
    if type(command) is RegistryPTGProducerScopeCommand:
        statement_id = _required_text(command.producer_statement_id, 128)
        if statement_id.startswith("operator-review:"):
            raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
        return {
            **command_by_field,
            "producer_statement_id": statement_id,
            "producer_statement_sha256": _sha256(command.producer_statement_sha256),
        }
    if actor.kind != "platform_admin":
        raise RegistryPTGProducerScopeError("registry_ptg_scope_actor_invalid")
    if (
        type(command.statement_id) is not UUID
        or not command.statement_id.int
        or type(command.ownership) is not RegistryPTGSourceOwnershipWitness
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
    ownership = asdict(command.ownership)
    for name, field_value in ownership.items():
        if name == "engine_source_identity_hash":
            _engine_identity(field_value)
        elif name == "status" and field_value == "" and type(field_value) is str:
            continue  # The existing controller terminal protocol permits a blank status.
        else:
            maximum = {"import_month": 16, "status": 32, "snapshot_id": 96, "source_key": 128}.get(name, 64)
            _required_text(field_value, maximum * 4)
            if len(field_value) > maximum:
                raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
    if any(ownership[name] != command_by_field[name] for name in ("source_file_import_id", "client_id", "snapshot_id")):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_owner_changed")
    return {
        **command_by_field,
        "producer_statement_id": f"operator-review:v1:{command.statement_id}",
        "operator_review": {"statement_id": str(command.statement_id), "source_ownership": ownership},
    }


def _approval_document(document, evidence):
    """Compute reconstructible human provenance from validated action and source."""
    if "operator_review" not in document:
        return {**document, "evidence": evidence}
    statement_by_field = {
        "contract": OPERATOR_REVIEW_CONTRACT,
        **document["operator_review"],
        **{
            name: value
            for name, value in document.items()
            if name
            not in {
                "contract",
                "operator_review",
                "producer_statement_id",
                "idempotency_key",
            }
        },
        "evidence": evidence,
    }
    return {
        **document,
        "operator_review": statement_by_field,
        "producer_statement_sha256": _digest(statement_by_field),
        "evidence": evidence,
    }


_PERMISSIONS_SQL = """
WITH scope AS (
 SELECT c.*,n.oid AS schema_oid,n.nspowner,o.oid AS owner_oid,w.oid AS approval_oid,r.oid AS reader_oid,
        o.rolcanlogin OR o.rolsuper OR o.rolcreatedb OR o.rolcreaterole OR o.rolreplication OR o.rolbypassrls
          AS owner_unsafe
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_roles o ON o.rolname=:owner_role JOIN pg_roles w ON w.rolname=:approval_role
 JOIN pg_roles r ON r.rolname=current_user
 WHERE n.nspname=:schema_name AND c.relname='registry_ptg_producer_scope'
), grants AS (
 SELECT a.* FROM scope s CROSS JOIN LATERAL aclexplode(coalesce(s.relacl,acldefault('r',s.relowner))) a
 UNION ALL SELECT a.* FROM scope s JOIN pg_attribute att ON att.attrelid=s.oid
 CROSS JOIN LATERAL aclexplode(att.attacl) a WHERE att.attnum>0 AND NOT att.attisdropped
)
SELECT relkind='r' AND relpersistence='p' AND NOT relrowsecurity AND NOT relforcerowsecurity
 AND relowner=owner_oid AND nspowner=owner_oid AND NOT owner_unsafe AND approval_oid<>owner_oid
 AND NOT EXISTS(SELECT 1 FROM grants g WHERE g.grantee<>owner_oid AND
   (g.grantee=0 OR g.is_grantable OR g.privilege_type<>'SELECT' AND
      NOT(g.grantee=approval_oid AND g.privilege_type='INSERT')))
 AND NOT EXISTS(SELECT 1 FROM pg_roles p WHERE
   (p.oid=reader_oid OR p.oid=approval_oid OR p.oid IN (SELECT grantee FROM grants WHERE grantee<>owner_oid))
   AND (p.rolsuper OR p.rolcreaterole OR p.rolreplication OR p.rolbypassrls
     OR pg_has_role(p.oid,owner_oid,'MEMBER') OR pg_has_role(p.oid,owner_oid,'SET')
     OR EXISTS(SELECT 1 FROM pg_roles elevated WHERE (elevated.rolsuper OR elevated.rolcreaterole)
       AND pg_has_role(p.oid,elevated.oid,'MEMBER'))))
 AND has_schema_privilege(reader_oid,schema_oid,'USAGE')
 AND NOT has_schema_privilege(reader_oid,schema_oid,'CREATE')
 AND has_table_privilege(reader_oid,oid,'SELECT')
 AND NOT has_table_privilege(reader_oid,oid,'UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 AND NOT has_any_column_privilege(reader_oid,oid,'UPDATE,REFERENCES')
 AND CASE WHEN :write THEN reader_oid=approval_oid AND has_table_privilege(reader_oid,oid,'INSERT')
   ELSE NOT has_table_privilege(reader_oid,oid,'INSERT')
     AND NOT has_any_column_privilege(reader_oid,oid,'INSERT')
     AND NOT pg_has_role(reader_oid,approval_oid,'MEMBER')
     AND NOT pg_has_role(reader_oid,approval_oid,'SET') END AS closed FROM scope
"""


async def _protected_store(session, store, *, write, table_name=TABLE):
    if (
        table_name
        not in {TABLE, "registry_ptg_published_plan_scope", "registry_ptg_office_approval", "registry_ptg_office_hold"}
        or type(store) is not RegistryPTGProducerScopeStore
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_store_unprotected")
    namespace = _identifier(store.control_schema or registry_schema())
    _identifier(store.owner_role)
    _identifier(store.approval_role)
    if not session.in_transaction() or (await session.execute(text("SHOW transaction_isolation"))).scalar_one() not in {
        "repeatable read",
        "serializable",
    }:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_transaction_required")
    closed = (
        await session.execute(
            text(
                _PERMISSIONS_SQL
                if table_name == TABLE
                else _PERMISSIONS_SQL.replace("'registry_ptg_producer_scope'", "'" + table_name + "'")
            ),
            {
                "schema_name": namespace[1:-1],
                "owner_role": store.owner_role,
                "approval_role": store.approval_role,
                "write": write,
            },
        )
    ).scalar_one_or_none()
    if closed is not True:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_store_unprotected")
    return namespace + "." + _identifier(table_name)


async def _selected_versions(session, specification, versions, *, ownership=None):
    """Resolve a complete bounded approved version set through actual source joins."""
    binding = await _physical_binding(session, specification)
    schema = _identifier(binding.schema_name if binding is not None else specification.ptg_schema_name)
    payload_id = binding.payload_snapshot_id if binding is not None else specification.snapshot_id
    source_records = (
        (
            await session.execute(
                text(CANDIDATE_SOURCE_RECORDS_SQL.format(schema=schema) + " LIMIT 129"),
                {"snapshot_id": payload_id},
            )
        )
        .mappings()
        .all()
    )
    approved_version_by_id = {version["source_file_version_id"]: version for version in versions}
    dense_source_keys = []
    seen_version_ids = set()
    if not 1 <= len(source_records) <= _MAX_FILES:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    if (
        ownership is not None
        and sum(
            record_by_field["source_file_version_count"] == 1
            and record_by_field["source_file_version_id"] == ownership["engine_source_file_version_id"]
            and record_by_field["version_source_identity_hash"] == ownership["engine_source_identity_hash"]
            for record_by_field in source_records
        )
        != 1
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    for record_by_field in source_records:
        version = approved_version_by_id.get(record_by_field["source_file_version_id"])
        if version is None:
            continue
        if (
            record_by_field["source_file_version_count"] != 1
            or record_by_field["source_file_version_id"] in seen_version_ids
            or record_by_field["version_source_identity_hash"] != version["source_identity_sha256"]
            or record_by_field["version_raw_sha256"] != version["raw_sha256"]
            or record_by_field["raw_container_sha256"] != version["raw_sha256"]
            or type(record_by_field["source_key"]) is not int
            or not 0 <= record_by_field["source_key"] < len(source_records)
        ):
            raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
        seen_version_ids.add(record_by_field["source_file_version_id"])
        dense_source_keys.append(record_by_field["source_key"])
    if seen_version_ids != set(approved_version_by_id) or len(set(dense_source_keys)) != len(dense_source_keys):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    return sorted(dense_source_keys)


async def _evidence(session, specification, document, frozen_authority, graph_identity):
    actual = await _require_frozen_source(session, specification, frozen_authority)
    if actual.get("contract") == PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT:
        raise RegistryPTGProducerScopeError("registry_ptg_published_scope_unavailable")
    if actual["source_file_import_id"] != document["source_file_import_id"]:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    graph, _ = await _source_state(session, specification, graph_identity)
    ownership = document.get("operator_review", {}).get("source_ownership")
    run_by_field = {}
    if ownership is not None:
        engine_run_id = (
            await session.execute(
                text(
                    f"SELECT import_run_id FROM {_identifier(specification.ptg_schema_name)}.ptg2_snapshot WHERE snapshot_id=:snapshot_id"
                ),
                {"snapshot_id": specification.snapshot_id},
            )
        ).scalar_one_or_none()
        if engine_run_id != ownership["engine_run_id"] or actual["source_key"] != ownership["source_key"]:
            raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
        run_by_field["engine_run_id"] = engine_run_id
    dense_source_keys = await _selected_versions(session, specification, document["file_versions"], ownership=ownership)
    return {
        "source_file_import_id": actual["source_file_import_id"],
        "source_key": actual["source_key"],
        "snapshot_manifest_sha256": actual["snapshot_manifest_sha256"],
        "frozen_binding_sha256": actual["frozen_binding_sha256"],
        "graph_identity": graph,
        "selected_dense_source_keys": dense_source_keys,
        **run_by_field,
    }


async def approve_registry_ptg_producer_scope(
    session,
    specification,
    command,
    actor,
    *,
    frozen_authority,
    graph_identity,
    store,
):
    """Persist a verified explicit action under its restricted approval role.

    The upstream action must verify external producer authority, or a live admin
    review and normalized controller ownership/admission, before entering this
    role. Operator statement digests are computed here, never supplied as proof.
    """
    document = _command_document(command, specification, actor)
    table = await _protected_store(session, store, write=True)
    evidence = await _evidence(session, specification, document, frozen_authority, graph_identity)
    approval_by_field = _approval_document(document, evidence)
    await _require_current_approved_company(session, table, approval_by_field)
    return await _retain_approval(session, table, approval_by_field)


def _approval_parameters(document):
    encoded = _canonical(document)
    if len(encoded.encode()) > _MAX_DOCUMENT_BYTES:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
    return {
        "scope_id": document["scope_id"],
        "actor_key": _digest(document["actor"]),
        "idempotency_key": document["idempotency_key"],
        "approval_sha256": _digest(document),
        "document": encoded,
        "scope_key": _digest(
            {
                name: document[name]
                for name in (
                    "coordinates",
                    "client_id",
                    "company_key",
                    "cohort_id",
                    "binding_source_key",
                    "snapshot_id",
                )
            }
        ),
    }


async def _matching_approval(session, table, parameters_by_name):
    return (
        (
            await session.execute(
                text(f"""SELECT approval_sha256,approval_json FROM {table}
      WHERE scope_id=CAST(:scope_id AS uuid) OR scope_key=:scope_key
        OR (actor_key=:actor_key AND idempotency_key=:idempotency_key)"""),
                parameters_by_name,
            )
        )
        .mappings()
        .all()
    )


async def _require_current_approved_company(session, table, document):
    """Allow exact retained replay; new documents require the fenced current map."""
    namespace = table.rsplit(".", 1)[0]
    await require_registry_company_approval_fence(session, namespace[1:-1])
    parameters = _approval_parameters(document)
    records = await _matching_approval(session, table, parameters)
    if records:
        _checked_approval(records, parameters, document)
        return
    company = (
        await session.execute(
            text(f"""SELECT EXISTS(
      SELECT FROM {namespace}.registry_revision_control current
      JOIN {namespace}.registry_approved_record company
        ON company.approved_revision=current.approved_revision
      WHERE current.id=1 AND current.approved_revision=:revision
        AND company.record_kind='company' AND company.record_key=:company
        AND company.record_json->>'company_id'=:company
        AND company.record_json->'archived'='false'::jsonb)"""),
            {"revision": document["approved_revision"], "company": document["legal_company_id"]},
        )
    ).scalar_one()
    if company is not True:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_company_unapproved")


def _checked_approval(approval_records, parameters_by_name, document):
    if (
        len(approval_records) != 1
        or approval_records[0]["approval_sha256"] != parameters_by_name["approval_sha256"]
        or approval_records[0]["approval_json"] != document
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_idempotency_conflict")
    return document


async def _retain_approval(session, table, document):
    parameters_by_name = _approval_parameters(document)
    await session.execute(
        text(f"""INSERT INTO {table} (scope_id,scope_key,actor_key,idempotency_key,approval_sha256,approval_json)
          VALUES (CAST(:scope_id AS uuid),:scope_key,:actor_key,:idempotency_key,:approval_sha256,CAST(:document AS jsonb))
          ON CONFLICT DO NOTHING"""),
        parameters_by_name,
    )
    return _checked_approval(await _matching_approval(session, table, parameters_by_name), parameters_by_name, document)


def _validated_approval(document, specification):
    """Reconstruct the closed action grammar before treating storage as approval."""
    try:
        actor_by_field = document["actor"]
        actor = RegistryActor(
            actor_by_field["kind"],
            UUID(actor_by_field["user_id"]),
            actor_by_field["client_id"],
            UUID(actor_by_field["impersonator_id"]) if actor_by_field["impersonator_id"] is not None else None,
        )
        command_by_field = dict(
            scope_id=UUID(document["scope_id"]),
            coordinates=RegistryNetworkSourceCoordinates(**document["coordinates"]),
            client_id=document["client_id"],
            legal_company_id=UUID(document["legal_company_id"]),
            approved_revision=document["approved_revision"],
            source_file_import_id=document["source_file_import_id"],
            file_versions=tuple(RegistryPTGProducerFileVersion(**version) for version in document["file_versions"]),
            reason=document["reason"],
            idempotency_key=document["idempotency_key"],
        )
        if "operator_review" in document:
            statement = document["operator_review"]
            command = RegistryPTGOperatorReviewCommand(
                **command_by_field,
                statement_id=UUID(statement["statement_id"]),
                ownership=RegistryPTGSourceOwnershipWitness(**statement["source_ownership"]),
            )
        else:
            command = RegistryPTGProducerScopeCommand(
                **command_by_field,
                producer_statement_id=document["producer_statement_id"],
                producer_statement_sha256=document["producer_statement_sha256"],
            )
        canonical = _command_document(command, specification, actor)
        if _approval_document(canonical, document["evidence"]) != document:
            raise ValueError
    except KeyError, TypeError, ValueError, AttributeError:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_changed") from None


async def read_registry_ptg_producer_scope(
    session,
    specification,
    *,
    scope_id,
    client_id,
    coordinates,
    frozen_authority,
    graph_identity,
    store,
):
    """Read durable approval and recheck source identities before returning its subset.

    This receipt establishes producer scope only. It neither proves the sealed
    office census nor packed graph edges; the caller must still check both.
    """
    if type(scope_id) is not UUID or not scope_id.int or type(coordinates) is not RegistryNetworkSourceCoordinates:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_input_invalid")
    _required_text(client_id, 64)
    table = await _protected_store(session, store, write=False)
    record_by_field = (
        (
            await session.execute(
                text(f"SELECT approval_sha256,approval_json FROM {table} WHERE scope_id=CAST(:scope_id AS uuid)"),
                {"scope_id": str(scope_id)},
            )
        )
        .mappings()
        .one_or_none()
    )
    if record_by_field is None:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_unavailable")
    document = record_by_field["approval_json"]
    if (
        type(document) is not dict
        or document.get("contract") != CONTRACT
        or record_by_field["approval_sha256"] != _digest(document)
        or document.get("scope_id") != str(scope_id)
        or document.get("client_id") != client_id
        or document.get("coordinates") != asdict(coordinates)
        or coordinates.source_system != "ptg"
        or coordinates.dataset_schema != specification.ptg_schema_name
        or any(
            document.get(name) != getattr(specification, name)
            for name in ("company_key", "cohort_id", "binding_source_key", "snapshot_id")
        )
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_changed")
    _validated_approval(document, specification)
    current = await _evidence(session, specification, document, frozen_authority, graph_identity)
    if document.get("evidence") != current:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    return json.loads(_canonical({**document, "approval_sha256": record_by_field["approval_sha256"]}))
