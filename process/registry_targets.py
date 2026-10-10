# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact bounded management-target existence; tenant grants live elsewhere."""

from uuid import UUID

from sqlalchemy import Boolean, Integer, MetaData, String, and_, case, cast, column, or_, select, values
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from db.models.company_group_registry import CompanyGroupRegistry
from db.models.company_registry import CompanyRegistry
from db.models.manual_directory_registry import ManualLocationRegistry, ManualProviderRegistry
from db.models.network_registry import NetworkRegistryIdentity, NetworkRegistryRecord
from db.models.registry_network_binding import RegistryNetworkBinding
from db.models.registry_site_binding import RegistrySiteBinding

MAX_REGISTRY_TARGETS = 100
_RECORD_KINDS = {"group", "company", "network", "provider", "location", "site_binding", "network_binding"}
_ACTIONS = {"edit", "archive", "restore", "publish", "membership", "link", "create"}


def _canonical_uuid(key):
    try:
        parsed = UUID(key)
    except (ValueError, AttributeError) as error:
        raise ValueError("registry_target_uuid_invalid") from error
    if str(parsed) != key:
        raise ValueError("registry_target_uuid_invalid")
    return parsed


def _canonical_network_id(key):
    if not key.isascii() or not key.isdecimal() or key.startswith("0") or len(key) > 10:
        raise ValueError("registry_target_network_id_invalid")
    parsed = int(key)
    if not 1 <= parsed <= 2147483647:
        raise ValueError("registry_target_network_id_invalid")
    return parsed


def _validated_target(target):
    if type(target) is not dict or set(target) != {"record_kind", "action", "target_key"}:
        raise ValueError("registry_target_fields_invalid")
    kind, action, key = target["record_kind"], target["action"], target["target_key"]
    if type(kind) is not str or kind not in _RECORD_KINDS or type(action) is not str or action not in _ACTIONS:
        raise ValueError("registry_target_kind_or_action_invalid")
    if type(key) is not str or not key or len(key) > 64:
        raise ValueError("registry_target_key_invalid")
    response_key = f"{kind}:{action}:{key}"
    if action == "create":
        if key == "root:":
            return response_key, "", None, None, True
        scope_kind, separator, scope_id = key.partition(":")
        if not separator or scope_kind not in {"group", "company"}:
            raise ValueError("registry_creation_scope_invalid")
        return response_key, scope_kind, _canonical_uuid(scope_id), None, False
    if kind == "network":
        return response_key, kind, None, _canonical_network_id(key), False
    return response_key, kind, _canonical_uuid(key), None, False


def _table(model, schema):
    table = model.__table__
    return table if schema is None else table.to_metadata(MetaData(), schema=schema)


def _requested_targets(requested_rows):
    """Encode UUID and integer selectors without mixing their namespaces."""
    typed_rows = [
        (key, kind, cast(record_uuid, PG_UUID(as_uuid=True)), cast(network_id, Integer), root_scope)
        for key, kind, record_uuid, network_id, root_scope in requested_rows
    ]
    return (
        values(
            column("response_key", String(128)),
            column("lookup_kind", String(16)),
            column("record_uuid", PG_UUID(as_uuid=True)),
            column("network_id", Integer),
            column("root_scope", Boolean),
        )
        .data(typed_rows)
        .cte("registry_requested_targets")
    )


def _target_statement(requested_rows, schema):
    """Join each typed target to its exact durable identity namespace."""
    requested = _requested_targets(requested_rows)
    groups, companies = _table(CompanyGroupRegistry, schema), _table(CompanyRegistry, schema)
    networks = _table(NetworkRegistryRecord, schema)
    identities = _table(NetworkRegistryIdentity, schema)
    providers, locations = _table(ManualProviderRegistry, schema), _table(ManualLocationRegistry, schema)
    site_bindings = _table(RegistrySiteBinding, schema)
    network_bindings = _table(RegistryNetworkBinding, schema)
    joined = (
        requested.outerjoin(
            groups, and_(requested.c.lookup_kind == "group", groups.c.group_id == requested.c.record_uuid)
        )
        .outerjoin(
            companies, and_(requested.c.lookup_kind == "company", companies.c.company_id == requested.c.record_uuid)
        )
        .outerjoin(
            networks, and_(requested.c.lookup_kind == "network", networks.c.network_id == requested.c.network_id)
        )
        .outerjoin(identities, identities.c.network_id == networks.c.network_id)
        .outerjoin(
            providers, and_(requested.c.lookup_kind == "provider", providers.c.provider_id == requested.c.record_uuid)
        )
        .outerjoin(
            locations, and_(requested.c.lookup_kind == "location", locations.c.location_id == requested.c.record_uuid)
        )
        .outerjoin(
            site_bindings,
            and_(requested.c.lookup_kind == "site_binding", site_bindings.c.binding_id == requested.c.record_uuid),
        )
        .outerjoin(
            network_bindings,
            and_(
                requested.c.lookup_kind == "network_binding", network_bindings.c.binding_id == requested.c.record_uuid
            ),
        )
    )
    status_pairs = [
        (groups.c.group_id.is_not(None), groups.c.archived),
        (companies.c.company_id.is_not(None), companies.c.archived),
        (and_(networks.c.network_id.is_not(None), identities.c.network_id.is_not(None)), networks.c.archived),
        (providers.c.provider_id.is_not(None), providers.c.archived),
        (locations.c.location_id.is_not(None), locations.c.archived),
        (site_bindings.c.binding_id.is_not(None), site_bindings.c.archived),
        (network_bindings.c.binding_id.is_not(None), network_bindings.c.archived),
    ]
    return select(
        requested.c.response_key,
        or_(requested.c.root_scope, *(present for present, _ in status_pairs)).label("known"),
        case(*status_pairs, else_=False).label("archived"),
    ).select_from(joined)


async def verify_registry_targets(session, targets, *, schema=None, boundary="management"):
    """Validate the entire batch, then read one native statement/snapshot.

    Keys match typed GrantTarget encoding: UUID, positive int32, or explicit
    root/group/company creation scope. Manual provider and site UUIDs use their
    own durable namespaces; an identical company UUID cannot grant access.
    """
    if type(targets) is not list or len(targets) > MAX_REGISTRY_TARGETS:
        raise ValueError("registry_target_batch_invalid")
    requested_rows = [_validated_target(target) for target in targets]
    if type(boundary) is not str or boundary not in {"management", "identity"}:
        raise ValueError("registry_target_boundary_invalid")
    if boundary == "identity":
        if any(target["record_kind"] != "network" or target["action"] != "edit" for target in targets):
            raise ValueError("registry_identity_target_invalid")
        identities = _table(NetworkRegistryIdentity, schema)
        network_ids = [row[3] for row in requested_rows]
        known_ids = (
            set(await session.scalars(select(identities.c.network_id).where(identities.c.network_id.in_(network_ids))))
            if network_ids
            else set()
        )
        # Serving eligibility is resolved from an approved manifest elsewhere.
        # Pending management archives must not change public search policy.
        return {row[0]: {"known": row[3] in known_ids, "archived": False} for row in requested_rows}
    if not requested_rows:
        return {}
    rows = (await session.execute(_target_statement(requested_rows, schema))).mappings()
    return {row["response_key"]: {"known": row["known"], "archived": row["archived"]} for row in rows}
