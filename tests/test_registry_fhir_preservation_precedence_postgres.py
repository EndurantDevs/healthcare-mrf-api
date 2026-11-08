# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native SQL unit proof; synthetic rows do not establish SOURCE or publication custody."""

import json
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import registry_fhir_serving_preservation as preservation
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_fhir_serving_preservation import receipt


async def _projection_rows(connection):
    await connection.execute("""CREATE TEMP TABLE registry_approved_record(
      approved_revision bigint,record_kind text,record_key text,record_json jsonb);
      CREATE TEMP TABLE historical_binding(provider_system text,provider_id text,location_id uuid,
        location_key text,entity_type text,entity_id text);
      CREATE TEMP TABLE provider_location_binding(LIKE historical_binding);
      CREATE TEMP TABLE historical_address(location_key text,entity_type text,entity_id text,
        canonical_network_ids integer[],label text);
      CREATE TEMP TABLE entity_address_unified(LIKE historical_address)""")
    providers = ("1000000491", "1000000149", "1234567893")
    sites = tuple(UUID(int=index) for index in (1, 2, 3))
    custom_keys = tuple(
        [
            await connection.fetchval(
                "SELECT encode(sha256(convert_to(jsonb_build_array('network_custom','npi',$1::text,$2::uuid)::text,'UTF8')),'hex')",
                provider,
                site,
            )
            for provider, site in zip(providers[:2], sites[:2], strict=True)
        ]
    )
    keys = ("historical-first", custom_keys[1], "historical-third")
    for provider, site, key in zip(providers, sites, keys, strict=True):
        await connection.execute("INSERT INTO historical_binding VALUES('npi',$1,$2,$3,'npi',$1)", provider, site, key)
        await connection.execute(
            "INSERT INTO historical_address VALUES($1,'npi',$2,ARRAY[7],'historical')", key, provider
        )
    memberships = [
        dict(network_id=7, provider_system="npi", provider_id=provider, location_id=str(site))
        for provider, site in zip(providers[:2], sites[:2], strict=True)
    ]
    await connection.execute(
        "INSERT INTO registry_approved_record VALUES(4,'membership','selected',$1::jsonb)",
        json.dumps({"archived": False, "memberships_json": memberships}),
    )
    for site in sites[:2]:
        await connection.execute("INSERT INTO registry_approved_record VALUES(4,'location',$1,'{}'::jsonb)", str(site))
    await connection.execute("INSERT INTO entity_address_unified SELECT * FROM historical_address")
    await connection.execute(
        "UPDATE entity_address_unified SET label='current correction' WHERE location_key=$1", custom_keys[1]
    )
    await connection.execute(
        "INSERT INTO entity_address_unified VALUES($1,'npi',$2,ARRAY[9],'current first')", custom_keys[0], providers[0]
    )
    await connection.execute("INSERT INTO provider_location_binding SELECT * FROM historical_binding")
    await connection.execute(
        "UPDATE provider_location_binding SET location_key=$1 WHERE provider_id=$2", custom_keys[0], providers[0]
    )
    return keys


@pytest.mark.asyncio
async def test_approved_projection_preserves_history_and_rejects_payload_or_binding_drift(serving_schema):
    connection, _schema, _engine = serving_schema
    async with connection.transaction():
        keys = await _projection_rows(connection)
        proof_receipt = receipt()
        proof_receipt.update(carried_rows=3, binding_rows=3, address_rows=3)
        proof = SimpleNamespace(
            address_query="SELECT * FROM pg_temp.historical_address WHERE $1::int=1",
            binding_query="SELECT * FROM pg_temp.historical_binding",
            address_arguments=(1,),
            receipt=proof_receipt,
        )
        target_options_by_name = dict(
            address_relation="(SELECT * FROM pg_temp.entity_address_unified WHERE $2::int=2)", address_arguments=(2,)
        )
        with pytest.raises(preservation.FHIRServingPreservationError, match="address_version_conflict"):
            await preservation.carry_fhir_preservation_addresses(connection, proof, "pg_temp", **target_options_by_name)
        assert await preservation.carry_fhir_preservation_addresses(
            connection, proof, "pg_temp", approved_context=(4, "pg_temp"), **target_options_by_name
        ) == preservation.fhir_serving_preservation_sha256(proof_receipt)
        await connection.execute("UPDATE entity_address_unified SET label='wrong' WHERE location_key=$1", keys[0])
        with pytest.raises(preservation.FHIRServingPreservationError, match="address_version_conflict"):
            await preservation.carry_fhir_preservation_addresses(
                connection, proof, "pg_temp", approved_context=(4, "pg_temp"), **target_options_by_name
            )
        await connection.execute("UPDATE entity_address_unified SET label='historical' WHERE location_key=$1", keys[0])
        await connection.execute(
            "UPDATE provider_location_binding SET entity_id='wrong' WHERE location_key=$1", keys[2]
        )
        with pytest.raises(preservation.FHIRServingPreservationError, match="address_parity"):
            await preservation.carry_fhir_preservation_addresses(
                connection, proof, "pg_temp", approved_context=(4, "pg_temp"), **target_options_by_name
            )


@pytest.mark.asyncio
async def test_coverage_digest_pages_preserve_order_and_empty_carry(serving_schema):
    import hashlib

    connection, _schema, _engine = serving_schema
    async with connection.transaction():
        await connection.execute("SET LOCAL work_mem='4MB'")
        await connection.execute("""CREATE TEMP TABLE digest_coverage(
          side text,slot text COLLATE "C",organization_id text COLLATE "C",payload_hash text,
          PRIMARY KEY(side,slot,organization_id)) ON COMMIT DROP;
          CREATE TEMP TABLE digest_missing(network_id integer PRIMARY KEY) ON COMMIT DROP""")
        assert await preservation._coverage_digest(connection, "digest_coverage") == hashlib.sha256().hexdigest()
        await connection.execute("""INSERT INTO digest_coverage
          SELECT CASE WHEN i%2=0 THEN 'current' ELSE 'previous' END,
            (ARRAY['a','A','é','z'])[1+i%4],
            (ARRAY['A-','a.','Z-','z.'])[1+i%4]||lpad(i::text,10,'0'),repeat('b',64)
          FROM generate_series(1,15003) i""")
        query = (
            "SELECT to_jsonb(row)::text FROM digest_coverage row "
            'ORDER BY side,slot COLLATE "C",organization_id COLLATE "C"'
        )
        count, expected = await preservation._ordered_digest(connection, query)
        assert count == 15003
        assert await preservation._coverage_digest(connection, "digest_coverage") == expected
        # These invalid queries must never execute when the exact missing set is empty.
        counters, coverage = await preservation._carry_counters(
            connection, "digest_missing", "digest_coverage", ("SELECT absent_column",) * 3
        )
        assert coverage == expected
        assert all(counters[name] == 0 for name in ("missing_networks", "carried_rows", "binding_rows", "address_rows"))
        assert all(
            actual_digest == hashlib.sha256().hexdigest()
            for name, actual_digest in counters.items()
            if name.endswith("_sha256")
        )
        await connection.execute(
            "UPDATE digest_coverage SET payload_hash=repeat('c',64) WHERE organization_id='a.0000000001'"
        )
        assert await preservation._coverage_digest(connection, "digest_coverage") != expected
        await connection.execute("INSERT INTO digest_missing VALUES(7)")
        duplicate = "SELECT 7 AS network_id,'same'::text AS evidence UNION ALL SELECT 7,'same'"
        counters, _coverage = await preservation._carry_counters(
            connection,
            "digest_missing",
            "digest_coverage",
            (duplicate, duplicate, "SELECT * FROM (" + duplicate + ") carried WHERE $1::integer=1"),
            (1,),
        )
        expected_count, expected_digest = await preservation._ordered_digest(
            connection, f'SELECT to_jsonb(row)::text FROM ({duplicate}) row ORDER BY to_jsonb(row)::text COLLATE "C"'
        )
        assert expected_count == 2
        for count_name, digest_name in (
            ("carried_rows", "membership_sha256"),
            ("binding_rows", "bindings_sha256"),
            ("address_rows", "addresses_sha256"),
        ):
            assert counters[count_name] == expected_count and counters[digest_name] == expected_digest
