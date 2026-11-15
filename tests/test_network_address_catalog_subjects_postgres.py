# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Every named reader subject resolves the captured relation by qualified name; changed content is refused."""

from contextlib import AsyncExitStack
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process import network_address_artifact as artifact
from process.network_compact_layout import CompactAddressRelation
from tests.test_network_address_artifact_postgres import address_db


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


async def _verify_as(resources, subject, descriptor):
    """Resolve one descriptor as a reader subject inside a pinned read-only API snapshot."""
    async with resources.admin.transaction(isolation="repeatable_read", readonly=True):
        await resources.admin.execute(f'SET LOCAL ROLE "{subject}"')
        return await artifact.verify_network_address_artifact(
            resources.admin, descriptor, runtime_roles=(subject,), api_read=True
        )


async def test_every_named_subject_reads_the_captured_qualified_relation(catalog_db):
    resources = catalog_db.resources
    descriptor = resources.descriptor
    assert descriptor["revision"] == 2 and descriptor["namespace"] == {"name": resources.schema}
    assert descriptor["table"] == {"schema_name": resources.schema, "table_name": "entity_address_unified"}
    expected = CompactAddressRelation(resources.schema, "entity_address_unified")
    for subject in catalog_db.subjects:
        assert await _verify_as(resources, subject, descriptor) == expected


@pytest.mark.parametrize(
    "change,reason",
    (
        ("table", "descriptor_invalid"),
        ("namespace", "descriptor_invalid"),
        ("digest", "descriptor_digest_invalid"),
        ("generation", "generation_invalid"),
    ),
)
async def test_changed_named_descriptor_content_is_refused(catalog_db, change, reason):
    resources = catalog_db.resources
    changed = deepcopy(resources.descriptor)
    if change == "table":
        changed["table"]["table_name"] = "entity_address_unified_next"
    elif change == "namespace":
        changed["namespace"]["name"] = "other_" + resources.schema[:40]
    elif change == "digest":
        changed["descriptor_sha256"] = "0" * 64
    else:
        changed["generation_id"] = "1" * 64
    with pytest.raises(artifact.NetworkAddressArtifactError, match=reason):
        await _verify_as(resources, catalog_db.subjects[0], changed)


async def test_subject_without_native_read_is_refused_by_postgresql(catalog_db):
    resources = catalog_db.resources
    subject = catalog_db.subjects[-1]
    await resources.admin.execute(f'REVOKE SELECT ON ALL TABLES IN SCHEMA "{resources.schema}" FROM "{subject}"')
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await _verify_as(resources, subject, resources.descriptor)
