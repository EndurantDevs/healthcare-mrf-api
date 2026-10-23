# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attest existing native Builder group rights without granting new authority.

The immutable Source payload boundary remains separate. These proofs cover only
the ordinary writer rights deliberately retained by the control-family contract.
"""

import asyncio
import json
from dataclasses import dataclass, replace

_ELEVATED = ("rolsuper", "rolcreaterole", "rolcreatedb", "rolreplication", "rolbypassrls")
_SERVER_ROLES = {"pg_read_server_files", "pg_write_server_files", "pg_execute_server_program"}


@dataclass(frozen=True)
class _LegacyObservation:
    """Bind one original native administrator transaction to its physical caller."""

    connection: object
    task: object
    actor: tuple
    locked_relations: frozenset[int]


def _require(condition, code):
    """Refuse incomplete or excessive native delegation evidence."""
    if not condition:
        raise ValueError("source_builder_delegation_" + code)


def _reachable(start, edges):
    """Follow one native membership option, including the current role itself."""
    reached, pending = {start}, [start]
    while pending:
        for target in edges.get(pending.pop(), ()):
            if target not in reached:
                reached.add(target)
                pending.append(target)
    return frozenset(reached)


def _native_graph(graph):
    """Validate the complete captured role/membership census before deriving rights."""
    roles = graph["roles"]
    by_oid = {int(role["oid"]): role for role in roles}
    _require(len(by_oid) == len(roles) and bool(by_oid), "role_coverage_changed")
    for role in roles:
        _require(all(type(role[field]) is bool for field in (*_ELEVATED, "rolcanlogin")), "role_flags_changed")
    usage_edges, set_edges, membership_keys = {}, {}, set()
    memberships = graph.get("memberships") or ()
    for membership in memberships:
        member, target = int(membership["member"]), int(membership["roleid"])
        key = (member, target, int(membership["grantor"]))
        _require(member in by_oid and target in by_oid and key not in membership_keys, "membership_changed")
        _require(
            all(type(membership[field]) is bool for field in ("inherit_option", "set_option", "admin_option")),
            "membership_changed",
        )
        membership_keys.add(key)
        if membership["inherit_option"]:
            usage_edges.setdefault(member, set()).add(target)
        if membership["set_option"]:
            set_edges.setdefault(member, set()).add(target)
    return by_oid, usage_edges, set_edges


@dataclass(frozen=True)
class BuilderDelegation:
    """Immutable evidence derived from the already-authenticated native catalog."""

    builder: int
    owner: int
    role_ids: frozenset[int]
    delegates: frozenset[int]
    builder_reach: frozenset[int]
    trusted: frozenset[int]
    superusers: frozenset[int]
    exposed_roles: frozenset[int]
    locked_relations: frozenset[int]
    policy_roles: tuple
    role_graph: bytes
    legacy_observation: _LegacyObservation | None = None


def builder_delegation(receipt, *, locked_relations=frozenset()):
    """Derive all direct/SET Builder authority from the pinned graph, never names."""
    _require(
        type(locked_relations) is frozenset and all(type(oid) is int and 0 < oid < 2**32 for oid in locked_relations),
        "transition_locks_changed",
    )
    roles = receipt["roles"]
    by_oid, usage_edges, set_edges = _native_graph(receipt["role_graph"])
    builder = roles["builder"]
    trusted = frozenset(roles[name] for name in ("owner", "publisher", "migration"))
    protected = trusted | {roles["namespace_owner"]}
    _require(builder in by_oid and protected <= by_oid.keys() and builder not in protected, "policy_roles_changed")
    usage_by_role = {role: _reachable(role, usage_edges) for role in by_oid}
    set_targets_by_role = {role: _reachable(role, set_edges) for role in by_oid}
    builder_reach = frozenset(
        role for role in by_oid if builder in usage_by_role[role] or builder in set_targets_by_role[role]
    )
    superusers = frozenset(role for role, native in by_oid.items() if native["rolsuper"])
    exposed_roles = frozenset().union(
        *(
            set_targets_by_role[role]
            for role, native in by_oid.items()
            if role not in trusted | superusers and (role >= 16384 or native["rolcanlogin"])
        )
    )
    delegates = frozenset(
        role
        for role in by_oid
        if role not in trusted | superusers
        and any(builder in usage_by_role[current] for current in set_targets_by_role[role])
    )
    _require_delegate_roles(receipt["role_graph"], by_oid, delegates, usage_by_role, set_targets_by_role, protected)
    proof = BuilderDelegation(
        builder,
        roles["owner"],
        frozenset(by_oid),
        delegates,
        builder_reach,
        trusted,
        superusers,
        exposed_roles,
        frozenset(locked_relations),
        tuple(sorted(roles.items())),
        _graph_bytes(receipt["role_graph"]),
    )
    require_callable_delegation(receipt["functions"], proof)
    return proof


def _graph_bytes(graph):
    """Freeze the complete authenticated native graph, including membership options."""
    return json.dumps(graph, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def receipt_delegation(receipt, proof=None):
    """Reject a proof reused with different native role or callable observations."""
    if proof is None:
        return builder_delegation(receipt)
    _require(
        type(proof) is BuilderDelegation
        and proof.policy_roles == tuple(sorted(receipt["roles"].items()))
        and proof.role_graph == _graph_bytes(receipt["role_graph"]),
        "receipt_changed",
    )
    require_callable_delegation(receipt["functions"], proof)
    return proof


async def locked_legacy_relations(connection):
    """Read actual granted locks from this native database and physical backend."""
    lock_rows = await connection.fetch(
        "SELECT relation::bigint AS oid FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='relation' "
        "AND mode='AccessExclusiveLock' AND granted "
        "AND database=(SELECT oid FROM pg_database WHERE datname=current_database())"
    )
    return frozenset(lock_row["oid"] for lock_row in lock_rows)


async def issue_legacy_delegation(connection, policy, receipt):
    """Issue only to the original independent administrator holding real native locks."""
    from process.provider_directory_cms_callable_custody import _admin_identity

    _require(connection.is_in_transaction() is True, "legacy_transaction_required")
    actor_values = tuple(sorted((await _admin_identity(connection, policy)).items()))
    proof = builder_delegation(receipt, locked_relations=await locked_legacy_relations(connection))
    proof = replace(
        proof,
        legacy_observation=_LegacyObservation(connection, asyncio.current_task(), actor_values, proof.locked_relations),
    )
    await require_legacy_observation(connection, policy, proof, legacy=True)
    return proof


async def require_legacy_observation(connection, policy, proof, *, legacy=False):
    """Recheck native authority and lifetime; lock-OID metadata alone authorizes nothing."""
    from process.provider_directory_cms_callable_custody import _admin_identity

    if not (legacy or proof.locked_relations or proof.legacy_observation is not None):
        return
    observation = proof.legacy_observation
    _require(type(observation) is _LegacyObservation, "legacy_issuer_required")
    _require(
        connection is observation.connection
        and asyncio.current_task() is observation.task
        and proof.locked_relations == observation.locked_relations
        and connection.is_in_transaction() is True,
        "legacy_context_changed",
    )
    _require(
        tuple(sorted((await _admin_identity(connection, policy)).items())) == observation.actor,
        "legacy_context_changed",
    )
    _require(proof.locked_relations <= await locked_legacy_relations(connection), "legacy_locks_changed")


def _require_delegate_roles(graph, by_oid, delegates, usage_by_role, set_targets_by_role, protected):
    """Reject direct, inherited and SET-reachable elevation or membership administration."""
    for delegate in delegates:
        reachable = set().union(
            *(usage_by_role[current] | set_targets_by_role[current] for current in set_targets_by_role[delegate])
        )
        _require(not reachable.intersection(protected), "owner_authority")
        _require(
            all(
                not any(by_oid[role][field] for field in _ELEVATED) and by_oid[role]["rolname"] not in _SERVER_ROLES
                for role in reachable
            ),
            "elevated_authority",
        )
        _require(
            not any(
                membership["admin_option"] and int(membership["member"]) in set_targets_by_role[delegate]
                for membership in graph.get("memberships") or ()
            ),
            "membership_admin_authority",
        )


def require_callable_delegation(functions, proof):
    """Close inherited execution over the existing exact callable ACL/body checks."""
    namespace_owner = dict(proof.policy_roles)["namespace_owner"]
    for entry in functions.values():
        by_role = {row[0]: row for row in entry["access"]}
        _require(len(by_role) == len(entry["access"]) and by_role.keys() == proof.role_ids, "callable_coverage_changed")
        has_public_execute = any(edge[0] == 0 and edge[2] == "EXECUTE" for edge in entry["acl"])
        builder_executes = by_role[proof.builder][-1]
        for role, access in by_role.items():
            if (
                role in proof.trusted | proof.superusers
                or has_public_execute
                or role == namespace_owner == int(entry["definition"]["proowner"])
            ):
                continue
            _require(
                not access[-1] or role in proof.delegates and builder_executes,
                "callable_authority",
            )


def require_delegated_access(access_rows, proof, *, owner, builder, relation_oid, legacy_builder):
    """Require every delegated current role to stay inside native Builder rights."""
    _require(type(proof) is BuilderDelegation, "proof_required")
    _require((proof.owner, proof.builder) == (owner, builder), "policy_roles_changed")
    by_role = {access["role"]: access for access in access_rows}
    _require(len(by_role) == len(access_rows) and by_role.keys() == proof.role_ids, "access_coverage_changed")
    builder_access = by_role[proof.builder]
    builder_rights_by_column = {column: set(rights) for column, rights in builder_access["columns"]}
    for role, access in by_role.items():
        _require(access["superuser"] is (role in proof.superusers), "native_role_changed")
        if role in proof.trusted | proof.superusers:
            continue
        _require(access["builder_reach"] is (role in proof.builder_reach), "native_membership_changed")
        _require(not access["owner_reach"], "owner_authority")
        if access["builtin"] and role not in proof.exposed_roles:
            continue
        if role not in proof.delegates:
            _require(
                set(access["table"]) <= {"SELECT"}
                and all(set(rights) <= {"SELECT"} for _, rights in access["columns"]),
                "unrelated_writer",
            )
            continue
        _require(
            not legacy_builder
            or role == proof.builder
            or type(proof.legacy_observation) is _LegacyObservation
            and relation_oid in proof.locked_relations,
            "legacy_transition_required",
        )
        _require(
            set(access["table"]) <= {"SELECT", *builder_access["table"]}
            and {column for column, _ in access["columns"]} == builder_rights_by_column.keys()
            and all(
                set(rights) <= {"SELECT", *builder_rights_by_column[column]} for column, rights in access["columns"]
            ),
            "writer_authority",
        )
