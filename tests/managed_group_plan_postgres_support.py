# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real group HTTP/source-policy/retained SQL; typed release routing is fixture input."""

import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sanic import Sanic

from api.custom_import_provider_detail import parse_managed_provider_policy
from api.endpoint import pricing
from api.provider_detail_network_read import managed_provider_network_read_scope
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_service_source_policy_postgres import (
    _ALLOWED,
    _EXCLUDED,
    _reader_identity,
    _selection,
)
from tests.test_provider_service_source_policy_postgres import (
    cms_resource_template as cms_resource_template,
)
from tests.test_provider_service_source_policy_postgres import (
    profile_transport as profile_transport,
)
from tests.test_provider_service_source_policy_postgres import (
    published_profile_source as published_profile_source,
)
from tests.test_provider_service_source_policy_postgres import (
    source_admission as source_admission,
)

pytestmark = pytest.mark.asyncio


async def _prepare_group_routes(admission, monkeypatch):
    npi = 1000000491
    held = admission.held
    selection = replace(
        _selection([admission.reviews_by_name[name] for name in ("allowed", "excluded")]),
        plan_release_id="hprelease_" + "0" * 26,
    )
    await held.fixture.connection.execute(
        f'CREATE TABLE "{admission.schema}".ptg2_v3_npi_scope(snapshot_key bigint,npi bigint)'
    )
    await held.fixture.connection.execute(
        f'INSERT INTO "{admission.schema}".ptg2_v3_npi_scope VALUES(71,$1),(72,1000000005)', npi
    )
    await held.fixture.connection.execute(f'GRANT SELECT ON "{admission.schema}".ptg2_v3_npi_scope TO "{held.role}"')
    tables_by_snapshot = {
        binding.snapshot_id: SimpleNamespace(
            uses_shared_blocks=True,
            shared_snapshot_key=71 + index,
            plan_id=binding.plan_id,
            plan_market_type=binding.plan_market_type,
            source_key=binding.source_key,
            v3_schema_name=admission.schema,
        )
        for index, binding in enumerate(selection.in_network_bindings)
    }

    async def release_route(_session, release_id):
        assert release_id == selection.plan_release_id
        return selection

    async def snapshot_route(_session, snapshot_id):
        return tables_by_snapshot[snapshot_id]

    admission.group_prepared_bindings = []
    # This supplies already-admitted typed descriptors, without claiming physical readiness proof.
    monkeypatch.setattr(
        pricing, "prepare_plan_release_bindings", _prepared_group_route(admission, selection, tables_by_snapshot)
    )
    # The fixture supplies routing coordinates, not source-policy or retained-custody decisions.
    monkeypatch.setattr(pricing, "resolve_plan_release_serving", release_route)
    monkeypatch.setattr(pricing, "snapshot_serving_tables", snapshot_route)
    monkeypatch.setattr(pricing, "_ptg2_npi_scope_table", lambda tables, **_: f'"{admission.schema}".ptg2_v3_npi_scope')
    return selection


def _prepared_group_route(admission, selection, tables_by_snapshot):
    original_cache = selection._validated_serving_tables

    async def prepared_route(_session, actual_selection, bindings):
        assert actual_selection is selection and selection._validated_serving_tables == original_cache
        assert type(bindings) is tuple and bindings == tuple(
            binding for binding in selection.in_network_bindings if binding in bindings
        )
        admission.group_prepared_bindings.append(tuple(binding.snapshot_id for binding in bindings))
        return replace(
            selection,
            _validated_serving_tables=tuple(
                (binding.snapshot_id, tables_by_snapshot[binding.snapshot_id]) for binding in bindings
            ),
        )

    return prepared_route


def _group_office_execute(session, admission, prepared_count):
    execute = session.execute

    async def office_execute(statement, *args, **options):
        if "entity_address_unified" in str(statement):
            assert len(admission.group_prepared_bindings) == prepared_count + 1
        return await execute(statement, *args, **options)

    return office_execute


def _group_http_app(admission, selection):
    held = admission.held
    app = Sanic("native_group_" + uuid4().hex)
    observations = []

    @app.post("/group")
    async def read(request):
        document = request.json
        policy_document = v2_envelope({}, excluded_network_ids=document["excluded"])
        _, exclusions, digest, organization = parse_managed_provider_policy(policy_document)
        async with held.database.reader_session() as session:
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            await _reader_identity(driver, held)
            async with managed_provider_network_read_scope(
                session,
                selector=None,
                network_generation=admission.manifest.generation_id,
                excluded_network_ids=exclusions,
                access_scope_sha256=digest,
                organization_policy=organization,
            ):
                request.ctx.sa_session = session
                prepared_count = len(admission.group_prepared_bindings)
                session.execute = _group_office_execute(session, admission, prepared_count)
                reply = await pricing.group_plan_providers(
                    request,
                    native_args={
                        "plan_release_id": selection.plan_release_id,
                        "count": "true",
                        "limit": "1",
                        **document.get("native", {}),
                    },
                )
                assert len(admission.group_prepared_bindings) == prepared_count + 1
                assert tuple(json.loads(reply.body)["resolved_snapshot_ids"]) == tuple(
                    sorted(admission.group_prepared_bindings[-1])
                )
                observations.append(await driver.fetchval("SELECT current_setting('transaction_read_only')"))
                return reply

    return app, observations


async def test_managed_group_handler_real_source_policy_and_retained_offices(source_admission, monkeypatch):
    """Actual publication, source admission and office SQL share one ordinary Reader."""
    admission, npi = source_admission, 1000000491
    selection = await _prepare_group_routes(admission, monkeypatch)
    app, observations = _group_http_app(admission, selection)
    _, visible = await app.asgi_client.post("/group", json={"excluded": [_EXCLUDED]})
    assert visible.status == 200, visible.json
    document = visible.json
    assert document["resolved_snapshot_ids"] == [selection.in_network_bindings[0].snapshot_id]
    assert document["providers"]["total"] == 1
    assert [provider["npi"] for provider in document["providers"]["items"]] == [npi]
    addresses = document["providers"]["items"][0]["addresses"]
    assert len(addresses) == 2 and {address["second_line"] for address in addresses} == {None, "Suite 3"}
    assert document["providers"]["next_cursor"] == str(npi)
    _, all_sources = await app.asgi_client.post("/group", json={"excluded": []})
    assert all_sources.status == 200 and all_sources.json["providers"]["total"] == 1
    assert [provider["npi"] for provider in all_sources.json["providers"]["items"]] == [npi]
    assert len(all_sources.json["resolved_snapshot_ids"]) == 2
    _, exhausted = await app.asgi_client.post("/group", json={"excluded": [_EXCLUDED], "native": {"cursor": str(npi)}})
    assert exhausted.status == 200 and exhausted.json["providers"]["items"] == []
    assert exhausted.json["providers"]["total"] == 1
    _, denied_office = await app.asgi_client.post(
        "/group", json={"excluded": [_EXCLUDED], "native": {"city": "Unmatched City"}}
    )
    assert denied_office.status == 200 and denied_office.json["providers"]["total"] == 0
    _, no_sources = await app.asgi_client.post("/group", json={"excluded": [_ALLOWED, _EXCLUDED]})
    assert no_sources.status == 200 and no_sources.json["providers"]["items"] == []
    assert no_sources.json["resolved_snapshot_ids"] == []
    assert observations == ["on"] * 5
    evidence_by_field = {
        "visible": document,
        "all_sources": all_sources.json,
        "denied_office": denied_office.json,
        "exhausted": exhausted.json,
        "zero": no_sources.json,
        "limits": "Real published catalog/source admission and ordinary Reader HTTP handler. Typed release/table routing fixture. Public copied offices and absent-office denial proven; custom same-NPI office denial, full release resolver and signed Layer acceptance not proven.",
    }
    if os.getenv("MANAGED_GROUP_NATIVE_EVIDENCE"):
        Path(os.environ["MANAGED_GROUP_NATIVE_EVIDENCE"]).write_text(json.dumps(evidence_by_field, indent=2) + "\n")
