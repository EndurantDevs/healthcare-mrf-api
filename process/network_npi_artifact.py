# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only current NPI authority for direct custom provider references."""

import asyncpg
from sqlalchemy import text

from process import mrf_address_publication as native
from process import network_address_artifact as catalog
from process import npi_result_generation as npi
from process.entity_address_snapshot_preparation import _require_no_untrusted_mutation
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_membership_writer_closure import _protected_owner
from process.provider_directory_cms_native_inputs import _npi_revision


class NetworkNPIArtifactError(ValueError):
    """Native NPI publication, immutable custody or the exact source pin differs."""


def _require(condition, reason):
    if not condition:
        raise NetworkNPIArtifactError("network_npi_artifact_" + reason)


async def _relation_oids(connection, names):
    rows = await connection.fetch(
        "SELECT to_regclass(name)::oid::bigint AS oid FROM unnest($1::text[]) WITH ORDINALITY requested(name,ordinal) ORDER BY ordinal",
        names,
    )
    oids = tuple(row["oid"] for row in rows)
    _require(
        len(oids) == len(names)
        and all(type(oid) is int and 0 < oid < 2**32 for oid in oids)
        and len(set(oids)) == len(oids),
        "family_unavailable",
    )
    return oids


async def _locked_family(connection, schema):
    tables = (*npi.RELATION_NAMES, npi.TABLE_NAME)
    names = [_identifier(schema) + "." + _identifier(name) for name in tables]
    oids = await _relation_oids(connection, names)
    before_relations = [await catalog._physical(connection, oid) for oid in oids]
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + name for name in names) + " IN ACCESS SHARE MODE NOWAIT"
    )
    _require(await _relation_oids(connection, names) == oids, "family_changed")
    physical_relations = [await catalog._physical(connection, oid) for oid in oids]
    _require(physical_relations == before_relations, "family_changed")
    for relation in physical_relations:
        _require(
            relation is not None
            and relation["relkind"] == "r"
            and relation["relpersistence"] == "p"
            and not any(
                relation[key]
                for key in ("relrowsecurity", "relforcerowsecurity", "relispartition", "rules", "inherited")
            ),
            "heap_unsupported",
        )
    return oids, physical_relations


async def _read_subjects(connection, runtime_roles):
    roles = catalog._roles(runtime_roles)
    return tuple(sorted(set((*roles, await connection.fetchval("SELECT current_user")))))


async def _closed_family(connection, session, oids, physical, subjects):
    owner_oid = physical[0]["owner_oid"]
    owner = await connection.fetchrow("SELECT * FROM pg_catalog.pg_roles WHERE oid=$1::oid", owner_oid)
    _require(
        _protected_owner(owner) and all(relation["owner_oid"] == owner_oid for relation in physical),
        "protected_owner_invalid",
    )
    for relation in physical:
        await catalog._role_custody(connection, relation, owner_oid, subjects)
    await _require_no_untrusted_mutation(session, list(oids), owner_oid)
    await native._require_native_read_structure(session, oids, ())
    pairs = (None,) if len(subjects) == 1 else tuple((subjects[0], role) for role in subjects[1:])
    for pair in pairs:
        for oid in oids:
            await native._require_native_expression_owners(session, oid, executable_roles=pair)
    return owner_oid


async def _guard_owner(connection, proof, owner_oid, subjects):
    function_oid = proof["guards"][0]["function_oid"]
    guard = await connection.fetchrow(
        "SELECT p.oid::bigint,p.proowner::bigint,p.proacl::text,p.xmin::text AS catalog_xmin,p.ctid::text AS catalog_ctid,"
        "NOT EXISTS(SELECT FROM unnest($2::text[]) requested(name) JOIN pg_roles r ON r.rolname=requested.name "
        "WHERE pg_has_role(r.oid,p.proowner,'MEMBER')) AND NOT EXISTS(SELECT FROM pg_roles principal WHERE "
        + catalog.protected._ORDINARY_PRINCIPAL_SQL.replace(":owner_oid", "$3")
        + " AND pg_has_role(principal.oid,p.proowner,'MEMBER')) AS safe FROM pg_proc p WHERE p.oid=$1::oid",
        function_oid,
        list(subjects),
        owner_oid,
    )
    _require(guard is not None and guard["safe"] is True, "revision_owner_unsafe")
    return dict(guard)


async def _indexed_npi(connection, oid):
    valid = await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_attribute a JOIN pg_index i ON i.indrelid=a.attrelid "
        "JOIN pg_class idx ON idx.oid=i.indexrelid JOIN pg_am am ON am.oid=idx.relam "
        "WHERE a.attrelid=$1::oid AND a.attname='npi' AND a.atttypid='int8'::regtype AND a.attnotnull "
        "AND i.indisunique AND i.indisvalid AND i.indisready AND i.indislive AND i.indnkeyatts=1 "
        "AND i.indkey[0]=a.attnum AND i.indpred IS NULL AND i.indexprs IS NULL AND am.amname='btree')",
        oid,
    )
    _require(valid is True, "indexed_npi_required")


async def capture_network_npi_artifact(connection, schema_name, *, runtime_roles):
    """Resolve current native authority; an external generation label grants nothing.

    No bootstrap, registration, source data scan or privilege change occurs here.
    Both the actual caller and all consumer roles must be read-only NPI subjects.
    A native authority failure is terminal; the caller must never use legacy fallback.
    """
    _identifier(schema_name)
    _require(
        connection.is_in_transaction()
        and await connection.fetchval("SHOW transaction_isolation") in {"repeatable read", "serializable"},
        "snapshot_required",
    )
    try:
        session = catalog._CatalogSession(connection)
        oids, physical = await _locked_family(connection, schema_name)
        subjects = await _read_subjects(connection, runtime_roles)
        owner_oid = await _closed_family(connection, session, oids, physical, subjects)
        proof = await _npi_revision(session, schema_name)
        _require(tuple(proof["authority"]["relation_oids"]) == oids[:-1], "native_generation_changed")
        guard = await _guard_owner(connection, proof, owner_oid, subjects)
        await _indexed_npi(connection, oids[0])
        evidence = await catalog.publication._catalog_evidence(
            session,
            {
                "schema_name": schema_name,
                "stage_relations": [
                    {"table_name": name, "stage_name": name, "relation_oid": oid}
                    for name, oid in zip((*npi.RELATION_NAMES, npi.TABLE_NAME), oids, strict=True)
                ],
            },
        )
        generation = catalog._digest(proof["authority"])
        identity = catalog._digest(
            {
                "component": "network_npi_artifact",
                "revision": 1,
                "database": await catalog._database(connection),
                "physical": [dict(relation) for relation in physical],
                "native": proof,
                "guard_owner": guard,
                "catalog": evidence,
            }
        )
        return PinnedAddressSource(schema_name, "npi", generation), (
            physical[0]["schema_oid"],
            oids[0],
            owner_oid,
            identity,
        )
    except (asyncpg.PostgresError, RuntimeError, catalog.NetworkAddressArtifactError) as error:
        raise NetworkNPIArtifactError("network_npi_artifact_native_proof_unavailable") from error


async def verify_network_npi_artifact(connection, pinned_source, identity, *, runtime_roles):
    """Independently recapture before new publication; old readers use sealed rows."""
    _require(type(pinned_source) is PinnedAddressSource and pinned_source.table_name == "npi", "source_invalid")
    _require(
        type(identity) is tuple
        and len(identity) == 4
        and all(type(oid) is int and 0 < oid < 2**32 for oid in identity[:3])
        and type(identity[3]) is str
        and len(identity[3]) == 64
        and all(character in "0123456789abcdef" for character in identity[3]),
        "identity_invalid",
    )
    current_source, current_identity = await capture_network_npi_artifact(
        connection, pinned_source.schema_name, runtime_roles=runtime_roles
    )
    _require((current_source, current_identity) == (pinned_source, identity), "source_changed")
    return current_identity
