# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One owner-authorized direct reader for a retained, unsealed network candidate.

A legacy published candidate keeps the writer closure it was published with; its
manifest digest pins those reader roles, so a new reader cannot be recorded there.
This mirrors the bootstrap read-access successor: the candidate's owner grants one
login role USAGE and SELECT, and a closed proof schema binds the exact before/after
ACLs and the reader's native role component. The serving read accepts that reader
only while the proof, the live ACL and the role component all still match.
"""

import json
import re
from dataclasses import dataclass
from types import SimpleNamespace

from process.network_address_projection import _identifier
from process.network_bootstrap_read_access import (
    BootstrapReadAccessError,
    _acl,
    _extended_acl,
    _grant,
    _operator,
    _reader,
)

_CONTRACT = "network-retained-read-access/v1"
_FIELDS = {"contract", "schema_name", "manifest_sha256", "reader", "reader_oid", "roles_sha256", "before", "after"}
_CANDIDATE_SCHEMA = re.compile(r"network_candidate_([0-9a-f]{32})")


class RetainedReadAccessError(BootstrapReadAccessError):
    """A retained candidate's additional reader or its proof differs."""


def _require(condition):
    if not condition:
        raise RetainedReadAccessError("Retained read-access successor is unavailable or differs")


@dataclass(frozen=True)
class _RetainedReceipt:
    """The bootstrap helpers' receipt shape, built from a retained writer closure."""

    owner_role: str
    owner_oid: int
    schema_oid: int
    runtime_roles: tuple[str, ...]
    relation_oids: tuple[tuple[str, int], ...]
    address_source: SimpleNamespace


def proof_namespace(schema_name):
    """Name the closed proof schema of one retained candidate schema."""
    match = _CANDIDATE_SCHEMA.fullmatch(schema_name) if type(schema_name) is str else None
    _require(match is not None)
    return "network_retained_access_" + match.group(1)


async def _receipt(connection, candidate, catalog):
    closure = candidate["validation_json"]["writer_closure"]
    owner_oid = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", closure["owner_role"])
    relation_oids = catalog["relation_oids"]
    _require(owner_oid is not None and type(relation_oids) is dict and relation_oids)
    return _RetainedReceipt(
        closure["owner_role"],
        owner_oid,
        catalog["schema_oid"],
        tuple(sorted({*closure["loader_roles"], *closure["reader_roles"]})),
        tuple(sorted(relation_oids.items())),
        SimpleNamespace(schema_name=candidate["schema_name"]),
    )


async def _proof(connection, receipt, schema_name, manifest_sha256):
    row = await connection.fetchrow(
        "SELECT n.nspowner,obj_description(n.oid,'pg_namespace') AS document,"
        "NOT EXISTS(SELECT FROM aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a "
        "WHERE a.grantee<>n.nspowner) AND NOT EXISTS(SELECT FROM pg_depend "
        "WHERE refclassid='pg_namespace'::regclass AND refobjid=n.oid) "
        "AND NOT EXISTS(SELECT FROM pg_default_acl WHERE defaclnamespace=n.oid) AS closed "
        "FROM pg_namespace n WHERE n.nspname=$1",
        proof_namespace(schema_name),
    )
    if row is None:
        return None
    _require(row["nspowner"] == receipt.owner_oid and row["closed"] is True)
    _require(type(row["document"]) is str and len(row["document"].encode()) <= 32768)
    try:
        proof = json.loads(row["document"])
    except ValueError:
        raise RetainedReadAccessError("Retained read-access proof is invalid") from None
    _require(
        type(proof) is dict
        and set(proof) == _FIELDS
        and proof["contract"] == _CONTRACT
        and proof["schema_name"] == schema_name
        and proof["manifest_sha256"] == manifest_sha256
    )
    return proof


async def retained_extra_readers(connection, entry):
    """Return the one proven direct reader of an unsealed retained candidate, otherwise ()."""
    candidate, catalog = json.loads(entry["candidate_json"]), json.loads(entry["catalog_json"])
    schema_name = candidate.get("schema_name")
    if type(schema_name) is not str or _CANDIDATE_SCHEMA.fullmatch(schema_name) is None:
        return ()
    if await connection.fetchval("SELECT to_regnamespace($1) IS NULL", proof_namespace(schema_name)):
        return ()
    try:
        receipt = await _receipt(connection, candidate, catalog)
        proof = await _proof(connection, receipt, schema_name, json.loads(entry["manifest_json"])["manifest_sha256"])
        _require(proof is not None and proof["after"] == _extended_acl(receipt, proof["before"], proof["reader_oid"]))
        _require(await _acl(connection, receipt) == proof["after"])
        _require(await _reader(connection, receipt, proof["reader"]) == (proof["reader_oid"], proof["roles_sha256"]))
    except BootstrapReadAccessError, KeyError, TypeError, ValueError:
        # Fail closed: without a matching proof the recorded writer closure alone decides.
        return ()
    return (proof["reader"],)


async def _retained_entry(connection, address_source, control_schema):
    """Read the pinned candidate without trusting its live ACL, so a stale proof can still be revoked."""
    from process.network_serving_read import _read_manifest

    namespace = _identifier(control_schema)
    proof_namespace(address_source.schema_name)
    generation_id = await connection.fetchval(
        f"SELECT manifest.generation_id FROM {namespace}.network_serving_manifest manifest "
        f"JOIN {namespace}.network_membership_candidate candidate USING(candidate_id) "
        "WHERE candidate.schema_name=$1 AND manifest.manifest_sha256=$2",
        address_source.schema_name,
        address_source.generation_id,
    )
    _require(generation_id is not None)
    entry = await _read_manifest(connection, namespace, generation_id, include_ineligible=True)
    _require(entry is not None)
    candidate = json.loads(entry["candidate_json"])
    _require(candidate["schema_name"] == address_source.schema_name)
    return candidate, await _receipt(connection, candidate, json.loads(entry["catalog_json"]))


async def install_retained_read_access(connection, address_source, *, reader_role, control_schema):
    """Grant one proven direct reader from the original ACL; re-arming rewrites the proof."""
    from process.network_serving_read import resolve_retained_network_address_source

    async with connection.transaction():
        # Arm only from a closure that verifies on its own or under the matching proof.
        await resolve_retained_network_address_source(connection, address_source, control_schema=control_schema)
        candidate, receipt = await _retained_entry(connection, address_source, control_schema)
        schema_name, manifest_sha256 = candidate["schema_name"], address_source.generation_id
        original_role = await _operator(connection, receipt)
        existing = await _proof(connection, receipt, schema_name, manifest_sha256)
        before = await _acl(connection, receipt)
        _require(existing is None or (existing["reader"] == reader_role and existing["before"] == before))
        reader_oid, roles_sha256 = await _reader(connection, receipt, reader_role)
        proof_by_field = {
            "contract": _CONTRACT,
            "schema_name": schema_name,
            "manifest_sha256": manifest_sha256,
            "reader": reader_role,
            "reader_oid": reader_oid,
            "roles_sha256": roles_sha256,
            "before": before,
            "after": _extended_acl(receipt, before, reader_oid),
        }
        await _grant(connection, receipt, reader_role)
        _require(await _acl(connection, receipt) == proof_by_field["after"])
        namespace, owner = _identifier(proof_namespace(schema_name)), _identifier(receipt.owner_role)
        await connection.execute(f"SET LOCAL ROLE {original_role}")
        if existing is None:
            await connection.execute(f"CREATE SCHEMA {namespace} AUTHORIZATION {owner}")
        await connection.execute(f"SET LOCAL ROLE {owner}")
        encoded = json.dumps(proof_by_field, sort_keys=True, separators=(",", ":"))
        _require(len(encoded.encode()) <= 32768)
        literal = await connection.fetchval("SELECT quote_literal($1::text)", encoded)
        await connection.execute(f"COMMENT ON SCHEMA {namespace} IS {literal}")
        await connection.execute(f"SET LOCAL ROLE {original_role}")
        await resolve_retained_network_address_source(connection, address_source, control_schema=control_schema)
        return proof_by_field


async def revoke_retained_read_access(connection, address_source, *, reader_role, control_schema):
    """Restore only the original ACL; the proof stays and can be re-armed from it."""
    from process.network_serving_read import resolve_retained_network_address_source

    async with connection.transaction():
        candidate, receipt = await _retained_entry(connection, address_source, control_schema)
        original_role = await _operator(connection, receipt)
        proof = await _proof(connection, receipt, candidate["schema_name"], address_source.generation_id)
        _require(proof is not None and proof["reader"] == reader_role)
        current = await _acl(connection, receipt)
        _require(current in (proof["before"], proof["after"]))
        if current != proof["before"]:
            await _grant(connection, receipt, reader_role, revoke=True)
        _require(await _acl(connection, receipt) == proof["before"])
        await connection.execute(f"SET LOCAL ROLE {original_role}")
        await resolve_retained_network_address_source(connection, address_source, control_schema=control_schema)
        return proof
