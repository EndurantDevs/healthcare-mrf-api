# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native admitted ACA archive and published office-scoped plan read proof."""

import hashlib
import json
from dataclasses import asdict
from types import SimpleNamespace
from uuid import UUID, uuid4

import ptg2_address_canon
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from process import reference_family_archive as archive
from process import registry_provider_plan_read as reader
from process.entity_address_unified import _location_key_expr
from process.initial import _build_mrf_address_rows
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_legacy_membership_source import PinnedACAMembershipSource, read_aca_raw_fact_batch
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_record_store import RegistryRecordCommand
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.test_network_custom_address_source_postgres import _draft
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_initial_source_office_bindings_postgres import _insert_canonical_office
from tests.test_network_legacy_membership_source_postgres import (
    _aca_binding,
    _insert_reviewed_lineage,
    _seal_reviewed_archive,
    _write_aca_bindings,
)
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _CountedConnection, _create
from tests.test_registry_candidate_composition_postgres import (
    _approve_composition_intent,
    _approve_composition_source_offices,
    _compose_intent_candidate,
    _roles,
)
from tests.test_registry_network_catalog_read_postgres import _catalog_permissions
from tests.test_registry_retained_site_adoption_postgres import _source_addresses

pytestmark = pytest.mark.asyncio


def _raw_offices():
    first_by_field = {
        "address": "123 Example Street",
        "city": "Sample City",
        "state": "CA",
        "zip": "90210",
        "country": "US",
        "phone": "123",
    }
    return [first_by_field, first_by_field | {"address_2": "Suite 1", "phone": "456"}]


def _source_fixture_metadata(coordinates, role):
    """Reuse ingestion normalization to create exact source office witnesses."""
    metadata = {
        "source_id": coordinates.source_id,
        "release_id": "release-one",
        "import_id": "import-one",
        "issuer_id": 7,
        "year": 2026,
        "source_url": "https://source.example.test/providers",
        "alias_scope": "medical",
        "reviewed_alias_sha256": "a" * 64,
        "runtime_roles": (role,),
    }
    address_evidence_rows = []
    for index, address in enumerate(_raw_offices()):
        _, source_rows = _build_mrf_address_rows(
            {"npi": 1000000491, "addresses": [address]},
            {
                42 + index: {
                    "checksum_network": 42 + index,
                    "network_tier": "tier-" + str(42 + index),
                    "issuer_id": 7,
                    "year": 2026,
                }
            },
            metadata["import_id"],
            metadata["source_url"],
            None,
        )
        address_evidence_rows.extend(source_rows)
    return metadata, address_evidence_rows


async def _insert_source_canonical_offices(session, namespace, address_evidence_rows):
    """Seed canonical offices with the same native encoder as the retained evidence."""
    canonical_addresses = ptg2_address_canon.canonicalize_batch(
        [
            tuple(
                address_evidence[field]
                for field in ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")
            )
            for address_evidence in address_evidence_rows
        ]
    )
    inserted_addresses = set()
    for address_evidence, canonical in zip(address_evidence_rows, canonical_addresses, strict=True):
        assert str(address_evidence["address_key"]) == canonical["address_key"]
        if canonical["address_key"] in inserted_addresses:
            continue
        await _insert_canonical_office(
            session,
            namespace,
            address_evidence["address_key"],
            canonical | {"identity_version": 2, "precision": "street", "source_bits": 16, "merged_into": None},
            typed=False,
        )
        inserted_addresses.add(canonical["address_key"])


async def _archive_source(fixture, coordinates, owner, role, case=None):
    """Seal the exact retained address evidence and its canonical physical offices."""
    namespace = coordinates.dataset_schema
    metadata, address_evidence_rows = _source_fixture_metadata(coordinates, role)
    async with AsyncSession(fixture.engine) as session, session.begin():
        await archive._create_model_family(session, archive.reference_family_spec("mrf"), namespace)
        await _insert_reviewed_lineage(session, namespace)
        await session.execute(
            text(
                f'INSERT INTO "{namespace}".mrf_address_evidence SELECT * FROM jsonb_populate_recordset(NULL::"{namespace}".mrf_address_evidence,CAST(:rows AS jsonb))'
            ),
            {"rows": json.dumps(address_evidence_rows, default=str)},
        )
        await _insert_source_canonical_offices(session, namespace, address_evidence_rows)
        await session.execute(
            text(
                f'UPDATE "{namespace}".plan_npi_raw SET addresses=ARRAY(SELECT value::json FROM jsonb_array_elements(CAST(:rows AS jsonb)))'
            ),
            {"rows": json.dumps(_raw_offices())},
        )
        await session.execute(
            text(f"INSERT INTO \"{namespace}\".issuer(issuer_id,issuer_name,state) VALUES(7,'Retained issuer','CA')")
        )
        if case == "missing-entry":
            await session.execute(
                text(
                    f'UPDATE "{namespace}".plan_npi_raw SET addresses=ARRAY[CAST(:address AS json)] WHERE checksum_network=42'
                ),
                {"address": json.dumps(_raw_offices()[1])},
            )
        if case == "oversized":
            await session.execute(
                text(f'UPDATE "{namespace}".issuer SET issuer_name=:name'), {"name": "x" * reader.MAX_RESPONSE_BYTES}
            )
        manifest = await archive._family_manifest(
            session,
            spec=archive.reference_family_spec("mrf"),
            schema_name=namespace,
            source_metadata={
                "network_membership": metadata,
                "network_bindings": asdict(coordinates) | {"source_key_kind": "hios_plan_id"},
            },
            dependencies={"plan-attributes": "b" * 64},
            auxiliary={"archive_name": archive.archive_table_name(), "publication_sha256": "a" * 64},
        )
        ownership = await archive.capture_reference_family_stage_ownership(
            session, importer_id="mrf", dataset_id=UUID(coordinates.dataset_id)
        )
        owner_oid = await _seal_reviewed_archive(session, ownership, owner, role)
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=ownership,
            manifest=manifest,
            package_id=hashlib.sha256(json.dumps(address_evidence_rows, default=str).encode()).hexdigest(),
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=owner_oid,
        )
    return PinnedACAMembershipSource(
        archive.ReferenceFamilyPreparedSource(manifest, ownership), validation, **metadata
    ), address_evidence_rows


async def _approved_source_fixture(fixture, coordinates, address_evidence_rows):
    """Approve reviewed mappings and prepare full physical source offices."""
    actor = _actor()
    networks = [await _draft(fixture, _create("network"), actor) for _ in range(2)]
    source_rows = [_aca_binding(coordinates, network["record_id"], index + 1) for index, network in enumerate(networks)]
    bound = await _write_aca_bindings(fixture.connection, fixture.control_schema, actor, source_rows)
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *networks, *bound["records"]),
        actor,
    )
    await _catalog_permissions(fixture)
    await _source_addresses(fixture, 2, "npi")
    await fixture.connection.execute(f'UPDATE "{fixture.source_schema}".npi SET npi=1000000491')
    await fixture.connection.execute(
        f"UPDATE \"{fixture.source_schema}\".entity_address_unified SET entity_id='1000000491',npi=1000000491"
    )
    location_key = _location_key_expr(
        dict(
            entity_type="address.entity_type",
            entity_id="address.entity_id",
            npi="address.npi",
            inferred_npi="NULL::bigint",
            address_role_id="4",
            row_origin="'base'::text",
            address_key="source.address_key",
            source_id="2",
            source_record_id="NULL::text",
            zip5="NULL::text",
            state_code="NULL::text",
            city_norm="NULL::text",
        )
    )
    await fixture.connection.execute(
        f'UPDATE "{fixture.source_schema}".entity_address_unified address '
        f"SET address_key=source.address_key,address_precision='street',row_origin='base',location_key={location_key} "
        "FROM jsonb_to_recordset($1::jsonb) AS source(previous_key text,address_key uuid) "
        "WHERE address.location_key=source.previous_key",
        json.dumps(
            [
                {
                    "previous_key": "a" * 64 if index == 0 else f"{index:064x}",
                    "address_key": str(address_evidence["address_key"]),
                }
                for index, address_evidence in enumerate(address_evidence_rows)
            ]
        ),
    )
    return networks, approval


async def _publish_source(fixture, source_pin, coordinates, address_evidence_rows, copy_targets):
    networks, approval = await _approved_source_fixture(fixture, coordinates, address_evidence_rows)
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    copy_targets.append(copy_target)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=approval["approved_revision"], control_schema=fixture.control_schema
        )
        batch = await read_aca_raw_fact_batch(
            fixture.connection,
            source_pin,
            registry_schema=fixture.control_schema,
            approved_source=approved,
            binding_coordinates=coordinates,
            approved_only=True,
        )
        assert batch.membership_rows == 2
        await create_network_candidate(
            fixture.connection,
            copy_target,
            source_generations={
                "unified_address": fixture.base.generation_id,
                "aca": batch.generation_id,
                "custom_membership": approved.generation_id,
            },
            source_recipes=(RegistrySourceMembershipRecipe(source_pin, coordinates),),
            approved_custom_revision=approved.approved_revision,
            expected_head=0,
            expected_rows=2,
            control_schema=fixture.control_schema,
        )
        await fixture.connection.execute(
            f'CREATE TABLE "{copy_target.schema_name}".provider_location_binding(provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,PRIMARY KEY(provider_system,provider_id,location_id))'
        )
        await fixture.connection.execute(
            f'INSERT INTO "{copy_target.schema_name}".provider_location_binding SELECT \'npi\',entity_id,address_key,location_key,entity_type,entity_id FROM "{fixture.source_schema}".entity_address_unified'
        )
        await admit_network_membership_batch(
            fixture.connection,
            copy_target,
            batch_id=uuid4(),
            input_bytes=batch.input_bytes,
            expected_input_sha256=batch.input_sha256,
            control_schema=fixture.control_schema,
        )
        await seal_network_candidate(fixture.connection, copy_target, control_schema=fixture.control_schema)
    await prepare_and_publish_network_candidate(
        fixture.connection, copy_target, fixture.base, **_roles(fixture), control_schema=fixture.control_schema
    )
    return networks, copy_target


@pytest.fixture
async def provider_plans_db(custom_db, monkeypatch, request):
    fixture = custom_db
    dataset = uuid4()
    owner = "provider_aca_owner_" + uuid4().hex
    role = "provider_aca_reader_" + uuid4().hex
    copy_targets = []
    coordinates = RegistryNetworkSourceCoordinates(
        "aca",
        "source-example",
        archive.reference_family_stage_schema(dataset),
        str(dataset),
        "producer-example",
        "edition-example",
    )
    try:
        source_pin, address_evidence_rows = await _archive_source(
            fixture, coordinates, owner, role, getattr(request, "param", None)
        )
        await fixture.connection.execute(f'GRANT "{role}" TO "{fixture.roles["reader"]}" WITH INHERIT TRUE, SET FALSE')
        networks, copy_target = await _publish_source(
            fixture, source_pin, coordinates, address_evidence_rows, copy_targets
        )
        monkeypatch.setattr(
            reader, "NPIData", SimpleNamespace(__table__=SimpleNamespace(schema=fixture.source_schema, name="npi"))
        )
        fixture.extra_targets = copy_targets
        fixture.extra_requests = []
        yield fixture, source_pin, networks, copy_target
    finally:
        for copy_target in copy_targets:
            await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{coordinates.dataset_schema}" CASCADE')
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{fixture.control_schema}" CASCADE')
        for name in (role, owner):
            if await fixture.connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", name):
                await fixture.connection.execute(f'DROP OWNED BY "{name}"')
                await fixture.connection.execute(f'DROP ROLE "{name}"')


async def _read(fixture, excluded=(), npi="1000000491", **changes):
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
        counted = _CountedConnection(fixture.connection)
        result = await reader.read_registry_provider_plans(
            counted,
            reader.RegistryProviderPlansQuery(npi, **changes),
            excluded_network_ids=excluded,
            control_schema=fixture.control_schema,
        )
        return result, counted.statements


async def test_native_default_policy_filters_exact_raw_offices_and_preserves_retained_issuer(provider_plans_db):
    fixture, source_pin, networks, copy_target = provider_plans_db
    all_rows, all_count = await _read(fixture)
    assert len(all_rows["npi_data"]) == 2 and all_rows["plan_data"] == all_rows["issuer_data"] == []
    selected, selected_count = await _read(fixture, (networks[1]["record_id"],))
    assert selected_count == all_count
    assert len(selected["npi_data"]) == 1
    assert selected["npi_data"][0]["npi_info"]["checksum_network"] == 42
    assert selected["npi_data"][0]["npi_info"]["addresses"] == _raw_offices()[:1]
    assert selected["npi_data"][0]["issuer_info"]["issuer_name"] == "Retained issuer"
    assert (await _read(fixture, tuple(sorted(network["record_id"] for network in networks))))[0]["npi_data"] == []
    assert (await _read(fixture, year=2025))[0]["npi_data"] == []
    with pytest.raises(reader.RegistryNetworkCatalogError):
        await _read(fixture, generation_id=9223372036854775807)
    with pytest.raises(reader.RegistryNetworkCatalogError, match="detail_denied"):
        await _read(fixture, npi="0000000000")


@pytest.mark.parametrize(
    "provider_plans_db,reason",
    [("missing-entry", "source_unavailable"), ("oversized", "response_bounds")],
    indirect=["provider_plans_db"],
)
async def test_native_selected_row_missing_entry_and_complete_response_bounds_refuse(provider_plans_db, reason):
    fixture, _, networks, _ = provider_plans_db
    with pytest.raises(reader.RegistryNetworkCatalogError, match=reason):
        await _read(fixture, (networks[1]["record_id"],))


async def test_native_source_membership_without_effective_office_is_not_a_plan_claim(provider_plans_db):
    fixture, _, networks, copy_target = provider_plans_db
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        original = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
    members = await fixture.connection.fetch(
        f'SELECT * FROM "{copy_target.schema_name}".network_membership ORDER BY network_id'
    )
    actor = _actor()
    await _approve_composition_source_offices(fixture, original, copy_target, members, actor)
    member = members[0]
    command = RegistryRecordCommand(
        "membership",
        member["network_id"],
        "create",
        0,
        {
            "memberships_json": [
                {
                    "network_id": member["network_id"],
                    "provider_system": "npi",
                    "provider_id": "1000000491",
                    "location_id": str(member["location_id"]),
                    "evidence_id": "explicit-removal",
                    "operation": "remove",
                }
            ]
        },
        "Review exact office removal",
        uuid4().hex,
    )
    _, revision = await _approve_composition_intent(fixture, actor, command)
    context = (fixture, fixture.extra_targets, fixture.extra_requests, member["network_id"])
    current, effective, retained = await _compose_intent_candidate(context, revision, original, original.generation_id)
    assert not effective and len(retained) == 1
    visible, _ = await _read(fixture)
    assert [plan_row["npi_info"]["checksum_network"] for plan_row in visible["npi_data"]] == [43]
    assert visible["npi_data"][0]["npi_info"]["addresses"] == _raw_offices()[1:]
    previous, _ = await _read(fixture, generation_id=original.generation_id)
    assert len(previous["npi_data"]) == 2 and previous["generation"] != visible["generation"]
