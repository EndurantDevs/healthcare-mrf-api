# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual same-endpoint imports preserve a wholly disappeared serving network."""

import json
from dataclasses import asdict, replace
from uuid import uuid4, uuid5

import asyncpg

from api.network_address_scope import network_address_read_scope, scoped_address_relation_sql
from api.network_provider_read import NetworkOfficeSelection, read_network_provider_detail, read_network_provider_page
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_candidate_custody import seal_network_candidate_custody
from process.network_membership_pipeline import prepare_and_publish_network_candidate, prepare_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from process.registry_candidate_composition import (
    RegistryAddressArtifactCustody,
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
    prepare_registry_composition_address_artifact,
)
from tests import cms_registry_complete_publication_postgres_support as native
from tests import fhir_catalog_sync_integration_support as integration
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_publication_worker_permissions_postgres import _login

OLD_PROVIDER = "1000000491"


def separate_source_provider(monkeypatch):
    original_resources = integration.source_resources
    original_doctors = native._seed_native_doctors

    def resources(original, revision, *, retain_old):
        result = original_resources(original, revision, retain_old=retain_old)
        if retain_old:
            result["Practitioner"].append(
                {
                    "id": OLD_PROVIDER,
                    "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": OLD_PROVIDER}],
                    "name": [{"given": ["Previous"], "family": "Provider"}],
                }
            )
            result["PractitionerRole"][0]["practitioner"] = {"reference": "Practitioner/" + OLD_PROVIDER}
        return result

    async def doctors(database):
        await original_doctors(database)
        await database.status(
            "INSERT INTO mrf.npi(npi,entity_type_code,provider_first_name,provider_last_name) "
            "VALUES(:npi,1,'Previous','Provider')",
            npi=int(OLD_PROVIDER),
        )

    monkeypatch.setattr(integration, "source_resources", resources)
    monkeypatch.setattr(native, "_seed_native_doctors", doctors)


async def approve_edition(case, pair, networks):
    source_pin = pair.recipe.source_pin
    binding_rows = await case.fixture.connection.fetch(
        f'SELECT resource_id,network_id FROM "{source_pin.read_schema_name}".provider_directory_insurance_network_source_binding '
        "WHERE source_id=$1 AND resource_type='Organization' AND resource_id=ANY($2::text[])",
        source_pin.source_id,
        list(networks),
    )
    by_id = {binding_row["resource_id"]: binding_row["network_id"] for binding_row in binding_rows}
    inputs = [
        {
            "binding_id": str(uuid4()),
            **asdict(pair.recipe.binding_coordinates),
            "source_key": resource_id,
            "source_scope_json": {
                "organization_id": resource_id,
                "legacy_uuid": str(by_id[resource_id]),
                "alias_scope": source_pin.alias_scope,
            },
            "network_id": network_id,
            "evidence_id": "complete-source-edition",
            "evidence_sha256": "e" * 64,
            "operation": "bind",
            "expected_revision": 0,
            "expected_network_id": None,
        }
        for resource_id, network_id in networks.items()
    ]
    async with case.fixture.connection.transaction():
        bindings = (
            await apply_network_source_binding_batch(
                case.fixture.connection,
                NetworkSourceBindingBatchCommand(
                    json.dumps(inputs).encode(), "Review exact source networks", uuid4().hex
                ),
                case.seed.actor,
                control_schema=case.fixture.control_schema,
            )
        )["records"]
    approval = await _approve(
        case.fixture.connection,
        case.fixture.control_schema,
        await _command(case.fixture.connection, case.fixture.control_schema, *bindings),
        case.seed.actor,
    )
    async with case.fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            case.fixture.connection,
            approved_revision=approval["approved_revision"],
            control_schema=case.fixture.control_schema,
        )
    return {**approval, "generation_id": approved.generation_id}


async def _prepare_directory_artifact(case, request, approved, predecessor, address_sources, runtime_roles):
    """Prepare the address input using the independent original custodian login."""
    async with case.address_custody.custodian.transaction(isolation="repeatable_read"):
        artifact = await prepare_registry_composition_address_artifact(
            case.address_custody.custodian,
            request_id=request,
            approved_revision=approved["approved_revision"],
            expected_head=predecessor.generation_id,
            source_manifest=predecessor,
            address_sources=address_sources,
            custody=RegistryAddressArtifactCustody(
                case.address_custody.roles["custody"], runtime_roles, case.fixture.control_schema
            ),
        )
    return artifact


async def _seal_directory_candidate(case, execution, copy_target, addresses, readers, runtime_roles):
    """Run actual independent native custody before the ordinary publication path."""
    connection = case.fixture.connection
    await prepare_network_candidate(
        execution,
        copy_target,
        addresses,
        owner_role=case.worker.roles_by_kind["physical"],
        loader_roles=(),
        reader_roles=readers,
        control_schema=case.fixture.control_schema,
    )

    async def is_exact_target(_connection, actual):
        return actual == copy_target

    await seal_network_candidate_custody(
        connection,
        copy_target,
        control_schema=case.fixture.control_schema,
        runtime_roles=runtime_roles,
        authorize=is_exact_target,
    )
    await prepare_and_publish_network_candidate(
        execution,
        copy_target,
        addresses,
        owner_role=case.worker.roles_by_kind["physical"],
        loader_roles=(),
        reader_roles=readers,
        control_schema=case.fixture.control_schema,
    )


async def publish_directory(case, pair, approved, predecessor, *, ordinary=False):
    """Publish one complete native source vector and register exact candidate cleanup."""
    request = uuid4()
    connection = case.fixture.connection
    # Each schema is registered before the corresponding production API creates it.
    case.fixture.composition_ids.append(uuid5(request, "address:" + approved["generation_id"]))
    readers = tuple(sorted({case.fixture.roles["reader"], *pair.runtime_roles}))
    runtime_roles = tuple(
        sorted({*readers, *(role for kind, role in case.worker.roles_by_kind.items() if kind != "queue")})
    )
    address_sources = RegistryCompositionAddressSources(
        PinnedAddressSource(predecessor.schema_name, "entity_address_unified", predecessor.manifest_sha256)
        if ordinary
        else pair.address_source,
        replacement_source_recipes=() if ordinary else (pair.recipe,),
    )
    try:
        artifact = await _prepare_directory_artifact(
            case, request, approved, predecessor, address_sources, runtime_roles
        )
        async with _login(case.worker, "execution") as execution:
            copy_target, addresses = await compose_registry_membership_candidate(
                execution,
                request_id=request,
                approved_revision=approved["approved_revision"],
                expected_head=predecessor.generation_id,
                source_manifest=predecessor,
                address_sources=replace(address_sources, prepared_address_artifact=artifact),
                writer_roles={
                    "owner_role": case.worker.roles_by_kind["physical"],
                    "loader_roles": (),
                    "reader_roles": readers,
                },
                control_schema=case.fixture.control_schema,
            )
            await _seal_directory_candidate(case, execution, copy_target, addresses, readers, runtime_roles)
    finally:
        # Candidate namespaces use a different identity from the address composition.
        # The fixture owner cleanup also owns any interrupted candidate from this request.
        candidates = await connection.fetch(
            f'SELECT schema_name FROM "{case.fixture.control_schema}".network_membership_candidate WHERE dataset_id=$1',
            uuid5(request, "dataset"),
        )
        for candidate_row in candidates:
            if not hasattr(case, "directory_candidates"):
                case.directory_candidates = []
            case.directory_candidates.append(candidate_row["schema_name"])
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        return await resolve_network_serving_manifest(connection, control_schema=case.fixture.control_schema)


async def grant_pair_read(case, pair):
    roles = {
        case.fixture.roles["reader"],
        case.component.reader,
        *(role for kind, role in case.metadata_roles.items() if kind not in {"owner", "lifecycle"}),
    }
    roles.update(role for kind, role in case.worker.roles_by_kind.items() if kind != "queue")
    if hasattr(case, "address_custody"):
        roles.add(case.address_custody.roles["execution"])
    # Existing synthetic fixture actors acquire only the already closed pair's Reader.
    for role in sorted(roles - {case.metadata_roles["owner"], *pair.runtime_roles}):
        await case.fixture.connection.execute(
            f'GRANT "{pair.runtime_roles[0]}" TO "{role}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
        )


async def directory_reads(case, network_id, *, provider=None):
    connection = await asyncpg.connect(
        case.reader_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            manifest = await resolve_network_serving_manifest(connection, control_schema=case.fixture.control_schema)
            with network_address_read_scope(manifest, (network_id,)) as scope:
                page = await read_network_provider_page(connection, scope, control_schema=case.fixture.control_schema)
                provider = provider or next(
                    selected_row for selected_row in page["providers"] if selected_row["provider_id"] == OLD_PROVIDER
                )
                detail = await read_network_provider_detail(
                    connection,
                    scope,
                    provider_system=provider["provider_system"],
                    provider_id=provider["provider_id"],
                    control_schema=case.fixture.control_schema,
                )
                office = await read_network_provider_page(
                    connection,
                    scope,
                    office_filters=NetworkOfficeSelection(location_id=provider["locations"][0]["location_id"]),
                    control_schema=case.fixture.control_schema,
                )
                relation = scoped_address_relation_sql("mrf.entity_address_unified").replace(
                    ":_canonical_network_ids", "$1"
                )
                addresses = await connection.fetch(
                    "SELECT location_key,entity_id,first_line,second_line FROM "
                    + relation
                    + " a ORDER BY location_key",
                    [network_id],
                )
            return (
                manifest,
                provider,
                {
                    "page": page,
                    "detail": detail,
                    "office": office,
                    "addresses": [dict(selected_row) for selected_row in addresses],
                },
            )
    finally:
        await connection.close()
