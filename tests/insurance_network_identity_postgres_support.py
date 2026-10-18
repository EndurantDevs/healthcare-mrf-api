# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine native candidate setup for insurance-network compatibility writers."""

import hashlib
import json
from contextlib import aclosing

import pytest
import pytest_asyncio
from sqlalchemy import text

from tests import test_provider_directory_cms_resource_batch_postgres as resource
from tests import test_provider_directory_insurance_network_batch_postgres as batch


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def network_candidate_template(request):
    """Use the validated disposable parent through the existing exact child creator."""
    from tests.test_provider_directory_insurance_network_identity import _test_database_url

    with pytest.MonkeyPatch.context() as settings:
        settings.setenv("NETWORK_REGISTRY_TEST_DSN", _test_database_url())
        async with aclosing(resource.cms_resource_template.__wrapped__(request)) as baseline:
            await anext(baseline)
            yield


@pytest_asyncio.fixture
async def network_candidate_db(monkeypatch, network_candidate_template):
    """Keep actual Source roles, protected header and original transaction issuance."""
    async with aclosing(batch.network_batch_db.__wrapped__(monkeypatch, network_candidate_template)) as candidate:
        database = await anext(candidate)
        organizations = [
            {"resourceType": "Organization", "id": key, "name": "Example Network", "type": [{"text": "ntwk"}]}
            for key in ("network-1", "network-2")
        ]
        await batch._seed(database, organizations)
        try:
            yield database
        finally:
            for table in (
                "provider_directory_insurance_network_identity",
                "provider_directory_insurance_network_source_binding",
                "provider_directory_insurance_network_plan_evidence",
            ):
                assert await database.connection.fetchval(f'SELECT count(*) FROM "{database.schema}"."{table}"') == 0


async def candidate_evidence_rows(database):
    """Observe exact private native rows independently of the Publisher transaction."""
    return await database.connection.fetch(
        f"SELECT source_id,release_id,network_resource_id,insurance_plan_resource_id,owned_by_ref,administered_by_ref,"
        f"network_refs::text AS network_refs,plan_payload_json::text AS plan_payload_json,plan_payload_sha256,observed_at "
        f"FROM {database.relation('provider_directory_insurance_network_plan_evidence')} "
        "ORDER BY insurance_plan_resource_id"
    )


async def seed_historical_network_plan(session, plan):
    """Retain the original historical migration lock test without a retired global writer."""
    network_id = "00000000-0000-4000-8000-000000000010"
    await session.execute(
        text("INSERT INTO mrf.provider_directory_insurance_network_identity VALUES(CAST(:id AS uuid),now())"),
        {"id": network_id},
    )
    await session.execute(
        text(
            "INSERT INTO mrf.provider_directory_insurance_network_source_binding "
            "VALUES('source-a','Organization','network-1',CAST(:id AS uuid),now())"
        ),
        {"id": network_id},
    )
    payload = json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    await session.execute(
        text(
            "INSERT INTO mrf.provider_directory_insurance_network_plan_evidence VALUES "
            "('source-a','release-1','Organization','network-1','plan-1',CAST(:refs AS jsonb),"
            "'Organization/insurer-1','Organization/administrator-1',:sha,CAST(:payload AS jsonb),now())"
        ),
        {
            "refs": json.dumps(["Organization/network-1"]),
            "payload": payload,
            "sha": hashlib.sha256(payload.encode()).hexdigest(),
        },
    )
