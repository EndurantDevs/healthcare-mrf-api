# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable Provider Directory address corroboration smoke.

This script proves the generated ``provider_directory_address_corroboration``
view against a real PostgreSQL database without touching shared schemas. It
creates a temporary schema with minimal Provider Directory tables, loads
two rows, builds the real view SQL, and verifies:

- NPI + address + matching FHIR InsurancePlan => payer_directory_corroborated_location
- NPI + address without matching FHIR InsurancePlan => provider_directory_address in the raw view
- provider address_verification converts raw view evidence into the public
  contract, including exact network-name corroboration when a network name
  matches a Provider Directory network Organization.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import asyncpg


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _repo_root() -> Path:
    override = os.getenv("HLTHPRT_REPO_ROOT")
    if override:
        return Path(override).resolve()
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from process.provider_directory_fhir import provider_directory_address_corroboration_sql
from api.ptg2_serving import _address_verification_payload


def _validate_identifier(value: str, *, label: str) -> str:
    cleaned = str(value or "").strip()
    if not IDENTIFIER_RE.fullmatch(cleaned):
        raise ValueError(f"{label} must be a PostgreSQL identifier, got {value!r}")
    return cleaned


def _default_schema() -> str:
    return "provider_addr_corrob_" + dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _served_address_verification(row: dict[str, Any], *, network_names: list[str]) -> dict[str, Any]:
    address_payload = dict(row)
    address_payload["provider_directory_network_matches"] = _json_value(
        address_payload.get("provider_directory_network_matches")
    )
    address_payload["address_verification_evidence"] = _json_value(
        address_payload.get("address_verification_evidence")
    )
    item = {
        "network_names": network_names,
        "address": {
            "first_line": "100 Test St",
            "city": "Chicago",
            "state": "IL",
            "postal_code": "60601",
        },
    }
    return _address_verification_payload(item, {}, address_payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default=os.getenv("HLTHPRT_SMOKE_SCHEMA") or _default_schema())
    parser.add_argument("--host", default=os.getenv("HLTHPRT_DB_HOST") or "127.0.0.1")
    parser.add_argument("--port", type=int, default=_env_int("HLTHPRT_DB_PORT", 5440))
    parser.add_argument("--database", default=os.getenv("HLTHPRT_DB_DATABASE") or "healthporta")
    parser.add_argument("--user", default=os.getenv("HLTHPRT_DB_USER") or "nick")
    parser.add_argument("--password", default=os.getenv("HLTHPRT_DB_PASSWORD") or "")
    parser.add_argument("--keep-schema", action="store_true", help="keep the disposable schema after the run")
    return parser.parse_args()


async def _connect(args: argparse.Namespace) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )


async def _seed(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'CREATE SCHEMA "{schema}";')
    await conn.execute(
        f"""
        CREATE TABLE "{schema}".entity_address_unified (
            npi bigint,
            inferred_npi bigint,
            location_key varchar,
            address_key uuid,
            zip5 varchar,
            state_code varchar,
            city_norm varchar,
            type varchar
        );
        CREATE TABLE "{schema}".provider_directory_source (
            source_id varchar,
            org_name varchar,
            plan_name varchar
        );
        CREATE TABLE "{schema}".provider_directory_practitioner (
            source_id varchar,
            resource_id varchar,
            npi bigint,
            active boolean,
            full_name varchar,
            observed_at timestamp
        );
        CREATE TABLE "{schema}".provider_directory_location (
            source_id varchar,
            resource_id varchar,
            name varchar,
            telephone_number varchar,
            phone_number varchar,
            phone_extension varchar,
            fax_number varchar,
            fax_number_digits varchar,
            fax_extension varchar,
            status varchar,
            observed_at timestamp,
            address_key varchar
        );
        CREATE TABLE "{schema}".provider_directory_practitioner_role (
            source_id varchar,
            resource_id varchar,
            practitioner_ref varchar,
            organization_ref varchar,
            location_refs jsonb,
            healthcare_service_refs jsonb,
            network_refs jsonb,
            insurance_plan_refs jsonb,
            specialty_codes jsonb,
            code_codes jsonb,
            active boolean,
            observed_at timestamp
        );
        CREATE TABLE "{schema}".provider_directory_insurance_plan (
            source_id varchar,
            resource_id varchar,
            plan_identifier varchar,
            name varchar,
            network_refs jsonb
        );
        CREATE TABLE "{schema}".provider_directory_organization (
            source_id varchar,
            resource_id varchar,
            npi bigint,
            active boolean,
            name varchar,
            aliases jsonb,
            observed_at timestamp
        );
        CREATE TABLE "{schema}".provider_directory_organization_affiliation (
            source_id varchar,
            resource_id varchar,
            organization_ref varchar,
            participating_organization_ref varchar,
            location_refs jsonb,
            healthcare_service_refs jsonb,
            network_refs jsonb,
            specialty_codes jsonb,
            code_codes jsonb,
            active boolean,
            observed_at timestamp
        );
        CREATE TABLE "{schema}".provider_directory_healthcare_service (
            source_id varchar,
            resource_id varchar,
            active boolean,
            location_refs jsonb,
            observed_at timestamp
        );
        """
    )
    await conn.execute(
        f"""
        INSERT INTO "{schema}".provider_directory_source VALUES
            ('pdfhir_1', 'Example Payer', 'Example Directory');
        INSERT INTO "{schema}".provider_directory_insurance_plan VALUES
            ('pdfhir_1', 'plan-1', '010854205', 'Example Group POS Choice Plus',
             '["https://fhir.example.test/base/Organization/network-c2"]'::jsonb);
        INSERT INTO "{schema}".entity_address_unified (
            npi, inferred_npi, location_key, address_key, zip5, state_code, city_norm, type
        ) VALUES
            (1234567890, NULL, 'loc-plan',
             '00000000-0000-0000-0000-000000000001', '60601', 'IL', 'CHICAGO', 'practice'),
            (1234567891, NULL, 'loc-address-only',
             '00000000-0000-0000-0000-000000000002', '60601', 'IL', 'CHICAGO', 'practice'),
            (1234567892, NULL, 'loc-service',
             '00000000-0000-0000-0000-000000000003', '60601', 'IL', 'CHICAGO', 'practice');
        INSERT INTO "{schema}".provider_directory_practitioner VALUES
            ('pdfhir_1', 'prac-plan', 1234567890, true, 'Plan Matched', now()),
            ('pdfhir_1', 'prac-address-only', 1234567891, true, 'Address Only', now()),
            ('pdfhir_1', 'prac-service', 1234567892, true, 'Service Linked', now());
        INSERT INTO "{schema}".provider_directory_organization VALUES
            ('pdfhir_1', 'network-c2', NULL, true, 'C2', '["C Two"]'::jsonb, now());
        INSERT INTO "{schema}".provider_directory_location VALUES
            ('pdfhir_1', 'location-plan', 'Plan Clinic', '312-555-0100', '3125550100', NULL,
             '312-555-0101', '3125550101', NULL, 'active', now(),
             '00000000-0000-0000-0000-000000000001'),
            ('pdfhir_1', 'location-address-only', 'Address Clinic', '312-555-0200', '3125550200', NULL,
             NULL, NULL, NULL, 'active', now(),
             '00000000-0000-0000-0000-000000000002'),
            ('pdfhir_1', 'location-service', 'Service Clinic', '312-555-0300', '3125550300', NULL,
             NULL, NULL, NULL, 'active', now(),
             '00000000-0000-0000-0000-000000000003');
        INSERT INTO "{schema}".provider_directory_healthcare_service VALUES
            ('pdfhir_1', 'service-location', true,
             '["https://fhir.example.test/base/Location/location-service"]'::jsonb, now());
        INSERT INTO "{schema}".provider_directory_practitioner_role VALUES
            ('pdfhir_1', 'role-plan', 'https://fhir.example.test/base/Practitioner/prac-plan', NULL,
             '["https://fhir.example.test/base/Location/location-plan"]'::jsonb,
             '[]'::jsonb,
             '["https://fhir.example.test/base/Organization/network-c2"]'::jsonb,
             '["https://fhir.example.test/base/InsurancePlan/plan-1"]'::jsonb,
             '[]'::jsonb, '[]'::jsonb, true, now()),
            ('pdfhir_1', 'role-address-only', 'https://fhir.example.test/base/Practitioner/prac-address-only', NULL,
             '["https://fhir.example.test/base/Location/location-address-only"]'::jsonb,
             '[]'::jsonb,
             '["https://fhir.example.test/base/Organization/network-c2"]'::jsonb,
             '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, true, now()),
            ('pdfhir_1', 'role-service', 'https://fhir.example.test/base/Practitioner/prac-service', NULL,
             '[]'::jsonb,
             '["https://fhir.example.test/base/HealthcareService/service-location"]'::jsonb,
             '["https://fhir.example.test/base/Organization/network-c2"]'::jsonb,
             '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, true, now());
        """
    )


async def run_smoke(args: argparse.Namespace) -> dict[str, Any]:
    schema = _validate_identifier(args.schema, label="schema")
    conn = await _connect(args)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await _seed(conn, schema)
        await conn.execute(provider_directory_address_corroboration_sql(schema))
        rows = await conn.fetch(
            f"""
            SELECT npi,
                   address_network_binding,
                   provider_directory_plan_context_matched,
                   provider_directory_network_context_present,
                   provider_directory_network_names,
                   provider_directory_network_matches,
                   address_verification_evidence,
                   address_verification_evidence->>'matched_on' AS matched_on,
                   provider_directory_telephone_number
              FROM "{schema}".provider_directory_address_corroboration
             ORDER BY npi;
            """
        )
        payload = [dict(row) for row in rows]
        for row in payload:
            row["provider_directory_network_matches"] = _json_value(row["provider_directory_network_matches"])
        if len(payload) != 3:
            raise AssertionError(f"expected 3 corroboration rows, got {payload!r}")
        plan_match, address_only, service_linked = payload
        assert plan_match["npi"] == 1234567890, payload
        assert plan_match["address_network_binding"] == "provider_directory_address", payload
        assert plan_match["provider_directory_plan_context_matched"] is False, payload
        assert plan_match["provider_directory_network_names"] == ["C2"], payload
        assert plan_match["provider_directory_network_matches"][0]["name"] == "C2", payload
        assert plan_match["matched_on"] == "npi_address_key_role_location", payload
        assert plan_match["provider_directory_telephone_number"] == "312-555-0100", payload
        assert address_only["npi"] == 1234567891, payload
        assert address_only["address_network_binding"] == "provider_directory_address", payload
        assert address_only["provider_directory_plan_context_matched"] is False, payload
        assert address_only["provider_directory_network_context_present"] is True, payload
        assert address_only["provider_directory_network_names"] == ["C2"], payload
        assert address_only["provider_directory_network_matches"][0]["name"] == "C2", payload
        assert address_only["matched_on"] == "npi_address_key_role_location", payload
        assert address_only["provider_directory_telephone_number"] == "312-555-0200", payload
        assert service_linked["npi"] == 1234567892, payload
        assert service_linked["address_network_binding"] == "provider_directory_address", payload
        assert service_linked["provider_directory_plan_context_matched"] is False, payload
        assert service_linked["provider_directory_network_context_present"] is True, payload
        assert service_linked["provider_directory_network_names"] == ["C2"], payload
        assert service_linked["provider_directory_network_matches"][0]["name"] == "C2", payload
        assert service_linked["matched_on"] == "npi_address_key_role_location", payload
        assert service_linked["provider_directory_telephone_number"] == "312-555-0300", payload
        plan_verification = _served_address_verification(plan_match, network_names=[])
        assert plan_verification["address_network_binding"] == "inferred_from_provider_identity", payload
        assert plan_verification["address_evidence_level"] == "provider_directory_address", payload
        assert plan_verification["network_bound_address"] is False, payload
        assert plan_verification["provider_directory_plan_context_matched"] is False, payload
        assert plan_verification["provider_directory_network_names"] == ["C2"], payload
        assert plan_verification.get("provider_directory_network_name_matched") is None, payload
        assert "provider_directory_network_matches" not in plan_verification, payload
        network_verification = _served_address_verification(address_only, network_names=["C2"])
        assert network_verification["address_network_binding"] == "payer_directory_corroborated_location", payload
        assert network_verification["address_evidence_level"] == "payer_directory_network_location", payload
        assert network_verification["network_bound_address"] is True, payload
        assert network_verification["provider_directory_plan_context_matched"] is False, payload
        assert network_verification["address_verification_evidence"]["matched_on"].endswith("_network_name"), payload
        assert network_verification["provider_directory_network_matches"] == [
            {
                "ptg_network_name": "C2",
                "provider_directory_network_name": "C2",
                "provider_directory_network_key": "c2",
                "provider_directory_network_resource_id": "network-c2",
                "provider_directory_network_ref": "https://fhir.example.test/base/Organization/network-c2",
                "provider_directory_network_match_method": "canonical_network_name",
                "provider_directory_network_match_confidence": "candidate",
                "provider_directory_network_match_key": "c2",
                "provider_directory_source": "provider_directory_fhir",
                "provider_directory_source_id": "pdfhir_1",
                "provider_directory_org_name": "Example Payer",
                "provider_directory_plan_name": "Example Directory",
                "provider_directory_issuer_key": "examplepayer",
                "provider_directory_issuer_network_match_key": "examplepayer:c2",
            }
        ], payload
        address_only_verification = _served_address_verification(address_only, network_names=["Other Network"])
        assert address_only_verification["address_network_binding"] == "inferred_from_provider_identity", payload
        assert address_only_verification["address_evidence_level"] == "provider_directory_address", payload
        assert address_only_verification["network_bound_address"] is False, payload
        assert "provider_directory_network_matches" not in address_only_verification, payload
        return {"schema": schema, "rows": payload}
    finally:
        if not args.keep_schema:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await conn.close()


def main() -> None:
    result = asyncio.run(run_smoke(parse_args()))
    for row in result["rows"]:
        print(row)
    print(f"SMOKE_OK schema={result['schema']}")


if __name__ == "__main__":
    main()
