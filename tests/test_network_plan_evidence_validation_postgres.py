# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check complete plan evidence without sorting duplicated full payloads."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import network_fhir_retained_capture as retained
from process.network_fhir_retained_capture_contract import RetainedFHIRCaptureError
from tests.test_network_fhir_retained_capture_postgres import (
    legacy_capture_db as legacy_capture_db,
)


async def _wide_plan_source(fixture):
    schema = "registry_fhir_capture_" + uuid4().hex
    fixture.captures.append(schema)
    await fixture.admin.execute(f'CREATE SCHEMA "{schema}"')
    resources = f'"{schema}".{retained.TABLES[1]}'
    plans = f'"{schema}".{retained.TABLES[4]}'
    await fixture.admin.execute(f"""
        CREATE TABLE {resources}(dataset_id varchar(96) NOT NULL,resource_type varchar(32) NOT NULL,
          resource_id varchar(256) NOT NULL,payload_json json NOT NULL,payload_hash varchar(64) NOT NULL,
          PRIMARY KEY(dataset_id,resource_type,resource_id));
        CREATE TABLE {plans}({retained._DERIVED_DDL[retained.TABLES[4]]},
          PRIMARY KEY(source_id,release_id,network_resource_id,insurance_plan_resource_id));
        INSERT INTO {resources}
        SELECT 'edition','InsurancePlan','plan-'||plan,
          json_build_object('network_refs',
            (SELECT json_agg('Organization/network-'||network) FROM generate_series(1,8) network),
            'details',(SELECT string_agg(md5(plan::text||':'||part::text),'')
              FROM generate_series(1,64) part)),repeat('a',64)
        FROM generate_series(1,10000) plan;
        INSERT INTO {resources}
        SELECT 'edition','Organization','network-'||network,'{{}}'::json,repeat('b',64)
        FROM generate_series(1,8) network;
        INSERT INTO {plans}
        SELECT 'source','release','Organization',split_part(ref.value,'/',2),resource_id,
          payload_json::jsonb->'network_refs',NULL,NULL,payload_hash,payload_json::jsonb,
          TIMESTAMPTZ '2026-01-01T00:00:00Z'
        FROM {resources} CROSS JOIN LATERAL
          jsonb_array_elements_text(payload_json::jsonb->'network_refs') ref(value)
        WHERE resource_type='InsurancePlan';
        ANALYZE {resources}; ANALYZE {plans};
        GRANT USAGE ON SCHEMA "{schema}" TO "{fixture.role_by_kind["reader"]}";
        GRANT SELECT ON {resources},{plans} TO "{fixture.role_by_kind["reader"]}";
    """)
    await fixture.admin.execute(f"ALTER ROLE \"{fixture.role_by_kind['reader']}\" SET temp_file_limit='256MB'")
    return schema, plans


@pytest.mark.asyncio
async def test_wide_plan_evidence_fits_reader_limits_and_refuses_payload_tampering(legacy_capture_db):
    fixture = legacy_capture_db
    schema, plans = await _wide_plan_source(fixture)
    origin = SimpleNamespace(dataset_id="edition", source_id="source", unresolved_plan_network_count=0)
    reader = await fixture.connect("reader")
    assert await reader.fetchval("SELECT NOT rolsuper FROM pg_roles WHERE rolname=current_user") is True
    async with reader.transaction(readonly=True):
        await reader.execute("SET LOCAL work_mem='4MB'; SET LOCAL statement_timeout='30s'")
        assert await reader.fetchval("SHOW temp_file_limit") == "256MB"
        await retained._require_modern_plan_evidence(reader, schema, origin, "release")
    await fixture.admin.execute(
        f"UPDATE {plans} SET plan_payload_json=jsonb_set(plan_payload_json,'{{details}}','\"changed\"') "
        "WHERE insurance_plan_resource_id='plan-1' AND network_resource_id='network-1'"
    )
    async with reader.transaction(readonly=True):
        await reader.execute("SET LOCAL work_mem='4MB'; SET LOCAL statement_timeout='30s'")
        with pytest.raises(RetainedFHIRCaptureError):
            await retained._require_modern_plan_evidence(reader, schema, origin, "release")
