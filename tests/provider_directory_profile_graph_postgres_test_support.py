# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic normalized graph rows for Profile PostgreSQL proofs."""

from __future__ import annotations

from typing import Callable


GraphPayload = tuple[str, str, dict[str, object]]


def _graph_resource_ids_by_role(marker: str) -> dict[str, str]:
    return {
        resource_role: f"{marker}-{suffix}"
        for resource_role, suffix in (
            ("practitioner", "practitioner"),
            ("role", "role"),
            ("primary_organization", "primary-organization"),
            ("participating_organization", "participating-organization"),
            ("affiliation", "affiliation"),
            ("service", "service"),
            ("endpoint", "endpoint"),
            ("location", "location"),
            ("plan", "plan"),
        )
    }


def _practitioner_role_payloads(
    marker: str,
    npi: int,
    resource_ids_by_role: dict[str, str],
) -> tuple[GraphPayload, ...]:
    observed_at = "2026-08-10T00:00:00"
    practitioner_id = resource_ids_by_role["practitioner"]
    role_id = resource_ids_by_role["role"]
    return (
        (
            "Practitioner",
            practitioner_id,
            {
                "active": True,
                "communications": [{"codes": [{"code": "en"}]}],
                "family_name": "Graph",
                "full_name": f"{marker} practitioner",
                "given_names": [marker],
                "names": [{"text": f"{marker} practitioner"}],
                "npi": npi,
                "resource_id": practitioner_id,
                "updated_at": observed_at,
            },
        ),
        (
            "PractitionerRole",
            role_id,
            {
                "active": True,
                "code_codes": [{"code": f"{marker}-role"}],
                "endpoint_refs": [f"Endpoint/{resource_ids_by_role['endpoint']}"],
                "healthcare_service_refs": [
                    f"HealthcareService/{resource_ids_by_role['service']}"
                ],
                "identifiers": [{"value": f"{marker}-role-identifier"}],
                "insurance_plan_refs": [
                    f"InsurancePlan/{resource_ids_by_role['plan']}"
                ],
                "location_refs": [f"Location/{resource_ids_by_role['location']}"],
                "network_refs": [],
                "organization_ref": (
                    "Organization/" + resource_ids_by_role["participating_organization"]
                ),
                "practitioner_ref": f"Practitioner/{practitioner_id}",
                "resource_id": role_id,
                "specialty_codes": [{"code": f"{marker}-specialty"}],
                "updated_at": observed_at,
            },
        ),
    )


def _organization_payloads(
    marker: str,
    resource_ids_by_role: dict[str, str],
) -> tuple[GraphPayload, ...]:
    observed_at = "2026-08-10T00:00:00"
    primary_id = resource_ids_by_role["primary_organization"]
    participating_id = resource_ids_by_role["participating_organization"]
    return (
        (
            "Organization",
            primary_id,
            {
                "active": True,
                "name": f"{marker} primary organization",
                "resource_id": primary_id,
                "tin_status": "not_applicable",
                "type_codes": [{"code": "pay"}],
                "updated_at": observed_at,
            },
        ),
        (
            "Organization",
            participating_id,
            {
                "active": True,
                "name": f"{marker} participating organization",
                "resource_id": participating_id,
                "tin_status": "not_applicable",
                "type_codes": [{"code": "prov"}],
                "updated_at": observed_at,
            },
        ),
        _affiliation_payload(marker, resource_ids_by_role, observed_at),
    )


def _affiliation_payload(
    marker: str,
    resource_ids_by_role: dict[str, str],
    observed_at: str,
) -> GraphPayload:
    affiliation_id = resource_ids_by_role["affiliation"]
    return (
        "OrganizationAffiliation",
        affiliation_id,
        {
            "active": True,
            "code_codes": [{"code": f"{marker}-affiliation"}],
            "healthcare_service_refs": [
                f"HealthcareService/{resource_ids_by_role['service']}"
            ],
            "identifiers": [{"value": f"{marker}-affiliation-identifier"}],
            "insurance_plan_refs": [f"InsurancePlan/{resource_ids_by_role['plan']}"],
            "location_refs": [f"Location/{resource_ids_by_role['location']}"],
            "network_refs": [],
            "organization_ref": (
                f"Organization/{resource_ids_by_role['primary_organization']}"
            ),
            "participating_organization_ref": (
                "Organization/" + resource_ids_by_role["participating_organization"]
            ),
            "relationship_type": "member_of",
            "resource_id": affiliation_id,
            "updated_at": observed_at,
        },
    )


def _service_endpoint_payloads(
    marker: str,
    resource_ids_by_role: dict[str, str],
) -> tuple[GraphPayload, ...]:
    observed_at = "2026-08-10T00:00:00"
    service_id = resource_ids_by_role["service"]
    endpoint_id = resource_ids_by_role["endpoint"]
    return (
        (
            "HealthcareService",
            service_id,
            {
                "active": True,
                "comment": f"{marker} service comment",
                "identifiers": [{"value": f"{marker}-service-identifier"}],
                "name": f"{marker} service",
                "resource_id": service_id,
                "type_codes": [{"code": f"{marker}-service"}],
                "updated_at": observed_at,
            },
        ),
        (
            "Endpoint",
            endpoint_id,
            {
                "address": f"https://{marker}.example.test/fhir",
                "connection_type_code": "hl7-fhir-rest",
                "name": f"{marker} endpoint",
                "resource_id": endpoint_id,
                "status": "active",
                "updated_at": observed_at,
            },
        ),
    )


def _location_plan_payloads(
    marker: str,
    resource_ids_by_role: dict[str, str],
) -> tuple[GraphPayload, ...]:
    location_id = resource_ids_by_role["location"]
    plan_id = resource_ids_by_role["plan"]
    return (
        (
            "Location",
            location_id,
            {"name": f"{marker} location", "resource_id": location_id},
        ),
        (
            "InsurancePlan",
            plan_id,
            {"name": f"{marker} plan", "resource_id": plan_id},
        ),
    )


def graph_payloads(marker: str, npi: int) -> tuple[GraphPayload, ...]:
    """Return every normalized resource family retained by the rooted graph."""

    resource_ids_by_role = _graph_resource_ids_by_role(marker)
    return (
        *_practitioner_role_payloads(marker, npi, resource_ids_by_role),
        *_organization_payloads(marker, resource_ids_by_role),
        *_service_endpoint_payloads(marker, resource_ids_by_role),
        *_location_plan_payloads(marker, resource_ids_by_role),
    )


async def _insert_typed_practitioner(
    database: object,
    practitioner_ref: str,
    source_id: str,
    npi: int,
) -> None:
    await database.status(
        f"""
        INSERT INTO {practitioner_ref} (
            source_id, resource_id, npi, active, names, full_name, updated_at
        ) VALUES (
            :source_id, 'typed-leak-practitioner', :npi, true,
            '[{{"text":"typed-leak practitioner"}}]'::jsonb,
            'typed-leak practitioner', TIMESTAMP '2026-08-10'
        );
        """,
        source_id=source_id,
        npi=npi,
    )


async def _insert_typed_role(
    database: object,
    role_ref: str,
    source_id: str,
    npi: int,
) -> None:
    await database.status(
        f"""
        INSERT INTO {role_ref} (
            source_id, resource_id, npi, practitioner_ref,
            organization_ref, healthcare_service_refs, endpoint_refs,
            identifiers, specialty_codes, code_codes, active, updated_at
        ) VALUES (
            :source_id, 'typed-leak-role', :npi,
            'Practitioner/typed-leak-practitioner',
            'Organization/typed-leak-participating-organization',
            '["HealthcareService/typed-leak-service"]'::jsonb,
            '["Endpoint/typed-leak-endpoint"]'::jsonb,
            '[{{"value":"typed-leak-role"}}]'::jsonb,
            '[{{"code":"typed-leak-specialty"}}]'::jsonb,
            '[{{"code":"typed-leak-role"}}]'::jsonb,
            true, TIMESTAMP '2026-08-10'
        );
        """,
        source_id=source_id,
        npi=npi,
    )


async def _insert_typed_organizations(
    database: object,
    organization_ref: str,
    source_id: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {organization_ref} (
            source_id, resource_id, name, active, tin_status, updated_at
        ) VALUES
            (:source_id, 'typed-leak-primary-organization',
             'typed-leak primary organization', true, 'not_applicable',
             TIMESTAMP '2026-08-10'),
            (:source_id, 'typed-leak-participating-organization',
             'typed-leak participating organization', true, 'not_applicable',
             TIMESTAMP '2026-08-10');
        """,
        source_id=source_id,
    )


async def _insert_typed_affiliation(
    database: object,
    affiliation_ref: str,
    source_id: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {affiliation_ref} (
            source_id, resource_id, active, organization_ref,
            participating_organization_ref, relationship_type, updated_at
        ) VALUES (
            :source_id, 'typed-leak-affiliation', true,
            'Organization/typed-leak-primary-organization',
            'Organization/typed-leak-participating-organization',
            'member_of', TIMESTAMP '2026-08-10'
        );
        """,
        source_id=source_id,
    )


async def _insert_typed_service(
    database: object,
    service_ref: str,
    source_id: str,
    npi: int,
) -> None:
    await database.status(
        f"""
        INSERT INTO {service_ref} (
            source_id, resource_id, npi, active, name, comment, updated_at
        ) VALUES (
            :source_id, 'typed-leak-service', :npi, true,
            'typed-leak service', 'typed-leak service comment',
            TIMESTAMP '2026-08-10'
        );
        """,
        source_id=source_id,
        npi=npi,
    )


async def _insert_typed_endpoint(
    database: object,
    endpoint_ref: str,
    source_id: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {endpoint_ref} (
            source_id, resource_id, status, name, address, updated_at
        ) VALUES (
            :source_id, 'typed-leak-endpoint', 'active',
            'typed-leak endpoint', 'https://typed-leak.example.test/fhir',
            TIMESTAMP '2026-08-10'
        );
        """,
        source_id=source_id,
    )


async def seed_graph_typed_leak_rows(
    database: object,
    schema: str,
    relation_ref: Callable[[str, str], str],
    *,
    source_id: str,
    npi: int,
    dataset_id: str,
) -> None:
    """Seed source-wide graph rows plus a tempting dataset affiliation leak."""

    await _insert_typed_practitioner(
        database,
        relation_ref(schema, "provider_directory_practitioner"),
        source_id,
        npi,
    )
    await _insert_typed_role(
        database,
        relation_ref(schema, "provider_directory_practitioner_role"),
        source_id,
        npi,
    )
    await _insert_typed_organizations(
        database, relation_ref(schema, "provider_directory_organization"), source_id
    )
    await _insert_typed_affiliation(
        database,
        relation_ref(schema, "provider_directory_organization_affiliation"),
        source_id,
    )
    await _insert_typed_service(
        database,
        relation_ref(schema, "provider_directory_healthcare_service"),
        source_id,
        npi,
    )
    await _insert_typed_endpoint(
        database, relation_ref(schema, "provider_directory_endpoint"), source_id
    )
    await database.status(
        f"""
        INSERT INTO {relation_ref(
            schema,
            'provider_directory_dataset_affiliation_organization',
        )} (
            dataset_id, participating_organization_resource_id,
            affiliation_resource_id
        ) VALUES (
            :dataset_id, 'typed-leak-participating-organization',
            'typed-leak-affiliation'
        );
        """,
        dataset_id=dataset_id,
    )


__all__ = ("graph_payloads", "seed_graph_typed_leak_rows")
