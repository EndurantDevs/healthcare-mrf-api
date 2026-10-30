# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Batched native metadata keeps exact artifact custody and catalog bytes."""

import json
from contextlib import AsyncExitStack
from time import perf_counter
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import mrf_address_publication as native
from process import network_address_artifact as artifact
from tests.test_network_address_artifact_postgres import _rotate, address_db


@pytest.fixture
async def catalog_db(address_db):
    """Add only exact disposable reader subjects to the existing artifact fixture."""
    created_roles = []
    async with AsyncExitStack() as cleanup:

        async def remove_roles():
            for role in reversed(created_roles):
                await address_db.admin.execute(f'DROP OWNED BY "{role}"')
                await address_db.admin.execute(f'DROP ROLE "{role}"')

        cleanup.push_async_callback(remove_roles)
        for ordinal in range(7):
            role = f"catalog_subject_{uuid4().hex}_{ordinal}"
            await address_db.admin.execute(
                f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
            )
            created_roles.append(role)
            await address_db.admin.execute(f'GRANT USAGE ON SCHEMA "{address_db.schema}" TO "{role}"')
            await address_db.admin.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{address_db.schema}" TO "{role}"')
        yield SimpleNamespace(
            resources=address_db, subjects=tuple(sorted((address_db.roles["reader"], *created_roles)))
        )


async def _legacy_subjects(session, oids, subjects):
    pairs = (None,) if len(subjects) == 1 else tuple((subjects[0], role) for role in subjects[1:])
    for pair in pairs:
        await native.require_native_read_catalog(session, oids, executable_roles=pair)


async def _legacy_catalog(session, physical, authority):
    catalogs_by_label = {}
    for label, relation, logical_name in (
        ("address", physical, artifact._TABLE),
        ("authority", authority, artifact._AUTHORITY),
    ):
        catalogs_by_label[label] = await artifact.publication._catalog_evidence(
            session,
            {
                "schema_name": relation["schema_name"],
                "stage_relations": [
                    {"table_name": logical_name, "stage_name": relation["relname"], "relation_oid": relation["oid"]}
                ],
            },
        )
    return catalogs_by_label


class _CountedConnection:
    def __init__(self, connection):
        self.connection = connection
        self.statements = []

    def __getattr__(self, name):
        original = getattr(self.connection, name)
        if name not in {"execute", "fetch", "fetchrow", "fetchval", "prepare"}:
            return original

        async def counted(statement, *arguments, **keywords):
            self.statements.append(statement)
            return await original(statement, *arguments, **keywords)

        return counted


async def _descriptor(resources, roles):
    async with resources.reader.transaction(isolation="repeatable_read", readonly=True):
        return await artifact.capture_network_address_artifact(resources.reader, resources.source, runtime_roles=roles)


async def _verification(resources, descriptor, roles):
    counted = _CountedConnection(resources.reader)
    async with resources.reader.transaction(readonly=True):
        started = perf_counter()
        result = await artifact.verify_network_address_artifact(counted, descriptor, runtime_roles=roles)
        elapsed = perf_counter() - started
    return result, {"statements": len(counted.statements), "wall_seconds": elapsed}


async def test_full_verification_metadata_cost_and_parity(catalog_db, monkeypatch):
    resources = catalog_db.resources
    measurements = []
    for count in (1, 2, 4, 8):
        roles = catalog_db.subjects[:count]
        descriptor = await _descriptor(resources, roles)
        physical = await artifact._physical(resources.reader, descriptor["table"]["oid"])
        authority = await artifact._physical(resources.reader, descriptor["authority_table_oid"])
        async with resources.reader.transaction(readonly=True):
            session = artifact._CatalogSession(resources.reader)
            assert artifact._json(await _legacy_catalog(session, physical, authority)) == artifact._json(
                descriptor["catalog"]
            )
        with monkeypatch.context() as legacy:
            legacy.setattr(artifact, "require_native_read_catalog_subjects", _legacy_subjects)
            legacy.setattr(artifact, "_catalog", _legacy_catalog)
            before, old_cost = await _verification(resources, descriptor, roles)
        after, new_cost = await _verification(resources, descriptor, roles)
        assert before == after
        assert new_cost["statements"] < old_cost["statements"]
        measurements.append({"subjects": count, "legacy": old_cost, "batched": new_cost})
    assert len({entry["batched"]["statements"] for entry in measurements}) == 1
    assert [entry["legacy"]["statements"] for entry in measurements] == sorted(
        entry["legacy"]["statements"] for entry in measurements
    )
    print("NETWORK_ADDRESS_CATALOG_COST=" + json.dumps(measurements, sort_keys=True))


async def test_batched_catalog_bytes_survive_native_rename(catalog_db, monkeypatch):
    resources = catalog_db.resources
    await resources.admin.execute(f'ALTER INDEX "{resources.schema}".address_npi RENAME TO renamed_native_index')
    await _rotate(resources, monkeypatch)
    async with resources.reader.transaction(readonly=True):
        physical = await artifact._physical(resources.reader, resources.descriptor["table"]["oid"])
        authority = await artifact._physical(resources.reader, resources.descriptor["authority_table_oid"])
        session = artifact._CatalogSession(resources.reader)
        original = artifact._json(resources.descriptor["catalog"])
        assert artifact._json(await _legacy_catalog(session, physical, authority)) == original
        assert artifact._json(await artifact._catalog(session, physical, authority)) == original
        assert (
            await artifact.verify_network_address_artifact(
                resources.reader, resources.descriptor, runtime_roles=(resources.roles["reader"],)
            )
        ).table_oid == physical["oid"]


async def _native_index_function(resources):
    return await resources.admin.fetchrow(
        "SELECT quote_ident(n.nspname)||'.'||quote_ident(p.proname)||'('||pg_get_function_identity_arguments(p.oid)||')' AS identity,"
        "quote_ident(r.rolname) AS owner FROM pg_index i,LATERAL unnest(i.indclass) cls(oid) "
        "JOIN pg_opclass c ON c.oid=cls.oid JOIN pg_amproc a ON a.amprocfamily=c.opcfamily "
        "JOIN pg_proc p ON p.oid=a.amproc JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_roles r ON r.oid=p.proowner "
        "WHERE i.indexrelid=to_regclass($1) ORDER BY p.oid LIMIT 1",
        f'"{resources.schema}".address_npi',
    )


@pytest.mark.parametrize("unsafe", ("final_subject", "executable_owner", "current_user"))
async def test_every_subject_keeps_inherited_native_executable_refusal(catalog_db, monkeypatch, unsafe):
    resources = catalog_db.resources
    roles = catalog_db.subjects if unsafe != "current_user" else catalog_db.subjects[1:]
    descriptor = await _descriptor(resources, roles)
    selected = resources.roles["reader"] if unsafe == "current_user" else roles[-1]
    owner = resources.roles["outsider"] if unsafe == "final_subject" else selected
    function = await _native_index_function(resources)
    if unsafe == "final_subject":
        await resources.admin.execute(f'GRANT "{owner}" TO "{selected}" WITH INHERIT FALSE, SET FALSE')
        assert await resources.admin.fetchval(
            "SELECT pg_has_role($1::name,$2::name,'MEMBER') AND NOT pg_has_role($1::name,$2::name,'USAGE')",
            selected,
            owner,
        )
    await resources.admin.execute(f'ALTER FUNCTION {function["identity"]} OWNER TO "{owner}"')
    try:
        for is_legacy in (True, False):
            with monkeypatch.context() as verifier:
                if is_legacy:
                    verifier.setattr(artifact, "require_native_read_catalog_subjects", _legacy_subjects)
                with pytest.raises(RuntimeError, match="executable ownership is unsupported"):
                    await _verification(resources, descriptor, roles)
    finally:
        await resources.admin.execute(f"ALTER FUNCTION {function['identity']} OWNER TO {function['owner']}")


@pytest.mark.parametrize("damage", ("missing_relation", "column_added", "invalid_type", "missing_role"))
async def test_batched_verifier_keeps_missing_and_changed_native_catalog_refusals(catalog_db, damage):
    resources = catalog_db.resources
    descriptor = resources.descriptor
    async with resources.reader.transaction(readonly=True):
        if damage == "missing_relation":
            oids = [descriptor["table"]["oid"], 2**32 - 1]
            with pytest.raises(RuntimeError, match="heap topology differs"):
                await native.require_native_read_catalog_subjects(
                    artifact._CatalogSession(resources.reader), oids, catalog_db.subjects
                )
            return
        if damage == "missing_role":
            with pytest.raises(RuntimeError, match="executable ownership is unsupported"):
                await native.require_native_read_catalog_subjects(
                    artifact._CatalogSession(resources.reader),
                    [descriptor["table"]["oid"], descriptor["authority_table_oid"]],
                    (*catalog_db.subjects, "missing_catalog_subject"),
                )
            return
    if damage == "invalid_type":
        await resources.admin.execute(f"CREATE TYPE \"{resources.schema}\".unsupported_type AS ENUM ('value')")
        await resources.admin.execute(
            f'ALTER TABLE "{resources.schema}".entity_address_unified ADD COLUMN changed "{resources.schema}".unsupported_type'
        )
    else:
        await resources.admin.execute(
            f'ALTER TABLE "{resources.schema}".entity_address_unified ADD COLUMN changed text'
        )
    with pytest.raises(
        (artifact.NetworkAddressArtifactError, RuntimeError),
        match="structural_catalog_changed|executable catalog differs",
    ):
        await _verification(resources, descriptor, (resources.roles["reader"],))


@pytest.mark.parametrize("kind", ("OPERATOR", "OPERATOR CLASS", "OPERATOR FAMILY"))
async def test_final_subject_cannot_replace_native_operator_catalog_objects(catalog_db, monkeypatch, kind):
    resources = catalog_db.resources
    roles = catalog_db.subjects
    descriptor = await _descriptor(resources, roles)
    if kind == "OPERATOR":
        identity = "pg_catalog.=(bigint,bigint)"
        owner_query = "SELECT quote_ident(r.rolname) FROM pg_operator o JOIN pg_roles r ON r.oid=o.oprowner WHERE o.oid='pg_catalog.=(bigint,bigint)'::regoperator"
    elif kind == "OPERATOR CLASS":
        identity = "pg_catalog.int8_ops USING btree"
        owner_query = "SELECT quote_ident(r.rolname) FROM pg_opclass o JOIN pg_roles r ON r.oid=o.opcowner WHERE o.opcnamespace='pg_catalog'::regnamespace AND o.opcname='int8_ops' AND o.opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')"
    else:
        identity = "pg_catalog.integer_ops USING btree"
        owner_query = "SELECT quote_ident(r.rolname) FROM pg_opfamily o JOIN pg_roles r ON r.oid=o.opfowner WHERE o.opfnamespace='pg_catalog'::regnamespace AND o.opfname='integer_ops' AND o.opfmethod=(SELECT oid FROM pg_am WHERE amname='btree')"
    owner = await resources.admin.fetchval(owner_query)
    await resources.admin.execute(f'ALTER {kind} {identity} OWNER TO "{roles[-1]}"')
    try:
        for is_legacy in (True, False):
            with monkeypatch.context() as verifier:
                if is_legacy:
                    verifier.setattr(artifact, "require_native_read_catalog_subjects", _legacy_subjects)
                with pytest.raises(RuntimeError, match="executable ownership is unsupported"):
                    await _verification(resources, descriptor, roles)
    finally:
        await resources.admin.execute(f"ALTER {kind} {identity} OWNER TO {owner}")
