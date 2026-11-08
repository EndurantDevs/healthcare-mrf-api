# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native Q reach remains forbidden even without inherited schema privileges."""

import os
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process import network_candidate_custody as custody

pytestmark = pytest.mark.asyncio


def _q(name):
    return '"' + name.replace('"', '""') + '"'


@pytest.fixture
async def q_roles():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select the owned isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    transaction = connection.transaction()
    token = uuid4().hex
    role_by_kind = {kind: "q_reach_" + kind + "_" + token for kind in ("q", "r", "actor", "bridge", "safe")}
    schema_by_kind = {kind: "q_reach_" + kind + "_" + token for kind in ("control", "candidate")}
    await transaction.start()
    try:
        for role in role_by_kind.values():
            await connection.execute(
                f"CREATE ROLE {_q(role)} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION"
            )
        for kind, owner in (("control", "q"), ("candidate", "r")):
            await connection.execute(
                f"CREATE SCHEMA {_q(schema_by_kind[kind])} AUTHORIZATION {_q(role_by_kind[owner])}"
            )
        control_by_field = dict(
            await connection.fetchrow(
                "SELECT nspowner::bigint AS schema_owner_oid FROM pg_namespace WHERE nspname=$1",
                schema_by_kind["control"],
            )
        )
        control_by_field["owner_oid"] = await connection.fetchval(
            "SELECT oid::bigint FROM pg_roles WHERE rolname=$1", role_by_kind["r"]
        )
        candidate_oid = await connection.fetchval(
            "SELECT oid FROM pg_namespace WHERE nspname=$1", schema_by_kind["candidate"]
        )
        yield SimpleNamespace(
            connection=connection,
            role_by_kind=role_by_kind,
            control_by_field=control_by_field,
            candidate_oid=candidate_oid,
        )
    finally:
        await transaction.rollback()
        try:
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", list(role_by_kind.values())
            )
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname=ANY($1::text[]))", list(schema_by_kind.values())
            )
        finally:
            await connection.close()


@pytest.mark.parametrize(
    ("reach", "allow_set"),
    [
        ("q_itself", False),
        ("missing", False),
        ("direct", True),
        ("direct", False),
        ("transitive", True),
        ("transitive", False),
        ("safe", False),
    ],
)
async def test_q_catalog_reach_uses_actual_schema_owner_even_without_effective_create(q_roles, reach, allow_set):
    case, connection = q_roles, q_roles.connection
    actor = case.role_by_kind["actor"]
    if reach in {"direct", "transitive"}:
        parent = case.role_by_kind["q"]
        if reach == "transitive":
            await connection.execute(
                f"GRANT {_q(parent)} TO {_q(case.role_by_kind['bridge'])} WITH ADMIN FALSE, INHERIT FALSE, SET {str(allow_set).upper()}"
            )
            parent = case.role_by_kind["bridge"]
        await connection.execute(
            f"GRANT {_q(parent)} TO {_q(actor)} WITH ADMIN FALSE, INHERIT FALSE, SET {str(allow_set).upper()}"
        )
        assert await connection.fetchval(
            "SELECT pg_has_role($1::name,$2::oid,'MEMBER')", actor, case.control_by_field["schema_owner_oid"]
        )
        assert (
            await connection.fetchval(
                "SELECT pg_has_role($1::name,$2::oid,'SET')", actor, case.control_by_field["schema_owner_oid"]
            )
            is allow_set
        )
        assert not await connection.fetchval(
            "SELECT pg_has_role($1::name,$2::oid,'USAGE')", actor, case.control_by_field["schema_owner_oid"]
        )
        # The existing four-argument census sees candidate storage, not Q.
        assert (
            await connection.fetchval(
                custody._CENSUS_SQL, [actor], case.control_by_field["owner_oid"], case.candidate_oid, []
            )
            is True
        )
    if reach == "q_itself":
        actor = case.role_by_kind["q"]
    elif reach == "missing":
        actor = "q_reach_missing_" + uuid4().hex
    elif reach == "safe":
        actor = case.role_by_kind["safe"]
    if reach == "safe":
        await custody._q_reach_closed(connection, case.control_by_field, (actor,))
    else:
        with pytest.raises(custody.NetworkCandidateCustodyError):
            await custody._q_reach_closed(connection, case.control_by_field, (actor,))
