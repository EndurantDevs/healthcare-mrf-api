# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine synthetic Source publication and native v2 fact SQL; no office-policy activation."""

import asyncio
import copy
import hashlib
import json
from collections import Counter
from contextlib import aclosing, asynccontextmanager
from dataclasses import replace

import asyncpg
import pytest

from process import network_fhir_membership_source as fhir
from process import registry_source_contribution_store as facts
from process.registry_source_recipe_composition import _require_recipe_custody
from tests import generic_fhir_capture_postgres_support as retained
from tests import generic_source_successor_postgres_support as source
from tests import test_network_serving_schema_postgres as registry

pytestmark = pytest.mark.asyncio
_ORIGINAL_EDITION_ROWS = source._edition_rows


def _resource(kind, identity, payload, dataset):
    """Synthetic fixture data only; production code constructs all proofs and publication."""
    payload = {**payload, "source_id": source.SOURCE, "resource_id": identity}
    if kind == "Organization":
        from process.provider_directory_organization_hash import canonical_organization_payload

        payload = canonical_organization_payload(payload)
    return {
        "dataset_id": dataset,
        "resource_type": kind,
        "resource_id": identity,
        "payload_json": payload,
        "payload_hash": source.directory.resource_payload_sha256_for_contract(
            payload, source.directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT, resource_type=kind
        ),
    }


def _census_rows(edition, dataset):
    """Valid source syntax with both resolvable and missing reference targets."""
    resource_rows = _ORIGINAL_EDITION_ROWS(edition, dataset)
    template = copy.deepcopy(next(row["payload_json"] for row in resource_rows if row["resource_id"] == "role-office"))
    resource_rows = [row for row in resource_rows if row["resource_type"] != "PractitionerRole"]
    resource_rows.append(
        _resource(
            "Organization",
            "network-unapproved",
            {"name": "Unselected synthetic network", "type_codes": ["ntwk"]},
            dataset,
        )
    )
    changes_by_identity = {
        "A-accepted": {},
        "a-mixed": {"location_refs": ["Location/office-one", "Location/missing-office"]},
        "z-unmapped": {"network_refs": ["Organization/network-unapproved"]},
        "B-inactive": {"active": False},
        "P-provider-only": {"location_refs": []},
        "n-missing-network": {"network_refs": ["Organization/missing-network"]},
    }
    resource_rows.extend(
        _resource("PractitionerRole", identity, {**template, "insurance_plan_refs": [], **change}, dataset)
        for identity, change in changes_by_identity.items()
    )
    return resource_rows


def _empty_then_successor_rows(edition, dataset):
    rows = _ORIGINAL_EDITION_ROWS(edition, dataset)
    # Empty means an admitted Source capture with zero membership resources;
    # the Source still contains its genuine nonmembership resource census.
    return [row for row in rows if row["resource_type"] != "PractitionerRole"] if edition == 1 else rows


@asynccontextmanager
async def _registry_on_source(fixture, monkeypatch):
    monkeypatch.setenv(
        "NETWORK_REGISTRY_TEST_DSN", fixture.component.admin_engine.url.render_as_string(hide_password=False)
    )
    async with aclosing(registry.serving_schema.__wrapped__()) as generator:
        serving = await anext(generator)
        # All Registry calls below use their explicit control schema. Source
        # factory policy remains the original provisioned mrf profile.
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
        monkeypatch.setenv("DB_SCHEMA", "mrf")
        yield serving


@asynccontextmanager
async def _reader(fixture):
    url = fixture.component.admin_engine.url.set(drivername="postgresql", username=fixture.component.reader)
    reader = await asyncpg.connect(url.render_as_string(hide_password=False))
    try:
        yield reader
    finally:
        await reader.close()


async def _identity(reader, fixture):
    identity_by_field = dict(
        await reader.fetchrow(
            "SELECT session_user::text AS login,current_user::text AS actor,current_setting('role') AS role,"
            "pg_backend_pid() AS pid,pg_current_snapshot()::text AS snapshot,"
            "current_setting('transaction_read_only') AS readonly,current_setting('transaction_isolation') AS isolation,"
            "(SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='virtualxid' AND granted) AS virtual_xid,"
            "r.rolcanlogin,r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,"
            "pg_has_role(current_user,$1,'SET') AS can_set_source_owner,"
            "pg_has_role(current_user,$2,'SET') AS can_set_capture_owner FROM pg_roles r WHERE r.rolname=current_user",
            fixture.component.policy.owner_role,
            fixture.role_by_kind["owner"],
        )
    )
    assert identity_by_field["login"] == identity_by_field["actor"] == fixture.component.reader
    assert identity_by_field["role"] == "none" and identity_by_field["rolcanlogin"]
    assert identity_by_field["readonly"] == "on" and identity_by_field["isolation"] == "repeatable read"
    assert not any(
        identity_by_field[field]
        for field in (
            "rolsuper",
            "rolcreatedb",
            "rolcreaterole",
            "rolreplication",
            "rolbypassrls",
            "can_set_source_owner",
            "can_set_capture_owner",
        )
    )
    assert identity_by_field["virtual_xid"] is not None
    return identity_by_field


async def _temp_heaps(reader):
    for table in ("witness_facts", "witness_replay", "witness_cancel"):
        await facts.create_contribution_stage(reader, table)
    await reader.execute(
        "CREATE TEMP TABLE witness_members(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)"
    )


async def _assert_capture_census(reader, capture, expected):
    rows = await reader.fetch(
        f'SELECT resource_type,count(*) AS rows FROM "{capture.schema_name}".provider_directory_dataset_resource '
        'WHERE dataset_id=$1 GROUP BY resource_type ORDER BY resource_type COLLATE "C"',
        capture.origin.dataset_id,
    )
    counts_by_kind = {row["resource_type"]: row["rows"] for row in rows}
    assert sum(counts_by_kind.values()) > 0
    assert all(counts_by_kind.get(kind, 0) == count for kind, count in capture.origin.resource_counts)
    assert sum(counts_by_kind.get(kind, 0) for kind in ("PractitionerRole", "OrganizationAffiliation")) == expected
    assert capture.origin.descriptor_counts == capture.origin.resource_counts


async def _extract(reader, recipe, approved, control, *, limit, target=None):
    reads = await _require_recipe_custody(reader, recipe)
    cursor, ordinal, documents, resource_ids, pages = None, 0, [], [], []
    while True:
        page = await fhir.read_fhir_source_witness_page_v2(
            reader,
            recipe.source_pin,
            registry_schema=control,
            after_resource=cursor,
            limit=limit,
            approved_source=approved,
            binding_coordinates=recipe.binding_coordinates,
            retained_read=reads,
        )
        decoded = facts.decode_fhir_source_witness_page_v2(page)
        pages.append(page)
        documents.extend(decoded)
        resource_ids.extend((row["resource_type"], row["resource_id"]) for row in decoded if row["kind"] == "resource")
        if target is not None:
            ordinal = await facts.copy_fhir_source_witness_page_v2(
                reader, page, target, facts.verified_office_fact_identity(recipe)[0], ordinal
            )
        if not page.source_rows:
            break
        assert page.next_resource is not None and (cursor is None or page.next_resource > cursor)
        cursor = page.next_resource
    assert resource_ids == sorted(set(resource_ids))
    return reads, documents, pages


async def _map(reader, recipe, approved, control, *, table="witness_facts", insert=True):
    return await facts.map_fhir_source_witnesses_v2(
        reader,
        recipe,
        approved,
        f'pg_temp."{table}"',
        "witness_members",
        control,
        insert=insert,
    )


async def _assert_ordinary_parity(reader, recipe, approved, control, reads, descriptor):
    ordinary = await fhir.read_retained_fhir_membership_batch(
        reader,
        recipe.source_pin,
        reads,
        registry_schema=control,
        approved_source=approved,
        binding_coordinates=recipe.binding_coordinates,
    )
    assert tuple(
        descriptor[field]
        for field in (
            "membership_rows",
            "unresolved_rows",
            "omitted_rows",
            "raw_source_rows",
            "accepted_raw_source_rows",
            "unresolved_raw_source_rows",
            "omitted_raw_source_rows",
            "provider_only_rows",
        )
    ) == (
        ordinary.membership_rows,
        ordinary.unresolved_rows,
        ordinary.omitted_rows,
        ordinary.source_rows,
        ordinary.accepted_source_rows,
        ordinary.unresolved_source_rows,
        ordinary.omitted_source_rows,
        ordinary.provider_only_rows,
    )
    actual = await reader.fetch(
        "SELECT network_id,provider_system,provider_id,location_id::text AS location_id,evidence_id FROM pg_temp.witness_members"
    )
    assert Counter(facts._canonical(dict(member_record)) for member_record in actual) == Counter(
        facts._canonical(member_record) for member_record in json.loads(ordinary.input_bytes)
    )


async def _cancel_copy_and_rollback(reader, page, recipe):
    """Only COPY transport is observed; admission and all source reads use the real Reader."""
    started, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    observed_rows = []

    class NativeCopy:
        def is_in_transaction(self):
            return reader.is_in_transaction()

        async def copy_records_to_table(self, table_name, **kwargs):
            original_rows = kwargs.pop("records")

            async def rows():
                for index, record in enumerate(original_rows):
                    observed_rows.append(record)
                    yield record
                    if index == 0:
                        started.set()
                        await release.wait()

            result = await reader.copy_records_to_table(table_name, records=rows(), **kwargs)
            assert observed_rows == original_rows
            completed.set()
            return result

    savepoint = reader.transaction()
    await savepoint.start()
    operation = None
    try:
        operation = asyncio.create_task(
            facts.copy_fhir_source_witness_page_v2(
                NativeCopy(), page, "witness_cancel", facts.verified_office_fact_identity(recipe)[0], 0
            )
        )
        await asyncio.wait_for(started.wait(), timeout=10)
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done() and not completed.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert completed.is_set()
        assert await reader.fetchval("SELECT count(*) FROM pg_temp.witness_cancel") == len(
            facts.decode_fhir_source_witness_page_v2(page)
        )
    finally:
        release.set()
        if operation is not None and not operation.done():
            operation.cancel()
            try:
                await operation
            except asyncio.CancelledError:
                assert operation.cancelled()
        await savepoint.rollback()
    assert await reader.fetchval("SELECT count(*) FROM pg_temp.witness_cancel") == 0


def _assert_census_pages(documents, pages):
    """Keep the complete resource/edge census and stable cursor assertions together."""
    assert [fact["resource_id"] for fact in documents if fact["kind"] == "resource"] == [
        "A-accepted",
        "B-inactive",
        "P-provider-only",
        "a-mixed",
        "n-missing-network",
        "z-unmapped",
    ]
    assert len(documents) == 12 and len(pages) == 7 and pages[-1].source_rows == 0
    assert [page.next_resource for page in pages[:-1]] == [
        ("PractitionerRole", identity)
        for identity in (
            "A-accepted",
            "B-inactive",
            "P-provider-only",
            "a-mixed",
            "n-missing-network",
            "z-unmapped",
        )
    ]
    assert Counter(fact["resource_id"] for fact in documents if fact["kind"] == "edge") == {
        "A-accepted": 1,
        "P-provider-only": 1,
        "a-mixed": 2,
        "n-missing-network": 1,
        "z-unmapped": 1,
    }
    mixed_edges = [fact for fact in documents if fact["kind"] == "edge" and fact["resource_id"] == "a-mixed"]
    assert {fact["location_unresolved"] for fact in mixed_edges} == {False, True}
    missing = next(fact for fact in documents if fact["kind"] == "edge" and fact["source_key"] == "missing-network")
    assert not missing["network_witness_complete"] and not missing["provider_unresolved"]


def _assert_census_descriptor(descriptor):
    """Require the exact raw, accepted, unresolved, omitted and provider-only accounting."""
    assert tuple(
        descriptor[field]
        for field in (
            "raw_source_rows",
            "accepted_raw_source_rows",
            "unresolved_raw_source_rows",
            "omitted_raw_source_rows",
            "membership_rows",
            "unresolved_rows",
            "omitted_rows",
            "provider_only_rows",
        )
    ) == (6, 2, 1, 3, 2, 1, 2, 1)


async def test_genuine_source_v2_census_remap_cursor_replay_and_copy_rollback(monkeypatch, record_property):
    """Keep source custody, census, replay, remapping and cancellation rollback native."""
    monkeypatch.setattr(source, "_edition_rows", _census_rows)  # synthetic payloads only
    async with source.native_generic_source(monkeypatch) as fixture:
        capture = await source.publish_edition(fixture, 1)
        async with _registry_on_source(fixture, monkeypatch) as serving, _reader(fixture) as reader:
            _, control, _ = serving
            recipe, approved, network_id = await retained._approved_capture_recipe(fixture, serving, capture)
            await _temp_heaps(reader)
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                original = await _identity(reader, fixture)
                await _assert_capture_census(reader, capture, 6)
                reads, documents, pages = await _extract(
                    reader, recipe, approved, control, limit=1, target="witness_facts"
                )
                _assert_census_pages(documents, pages)
                descriptor = await _map(reader, recipe, approved, control)
                _assert_census_descriptor(descriptor)
                assert {
                    fact["network_id"] for fact in await reader.fetch("SELECT network_id FROM pg_temp.witness_members")
                } == {network_id}
                await _assert_ordinary_parity(reader, recipe, approved, control, reads, descriptor)
                _, replay, _ = await _extract(reader, recipe, approved, control, limit=1000, target="witness_replay")
                assert replay == documents
                assert await _map(reader, recipe, approved, control, table="witness_replay", insert=False) == descriptor
                await _cancel_copy_and_rollback(reader, pages[0], recipe)
                assert await _identity(reader, fixture) == original
                first_hash = descriptor["fact_sha256"]
                record_property(
                    "database_collation",
                    await reader.fetchval("SELECT datcollate FROM pg_database WHERE datname=current_database()"),
                )
            # Real approval creates a new selected binding; immutable source facts
            # are neither rewritten nor classified permanently at extraction.
            _, selected_missing, _ = await retained._approved_capture_recipe(
                fixture, serving, capture, organization="missing-network"
            )
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                before = await _identity(reader, fixture)
                reads, replay, _ = await _extract(reader, recipe, selected_missing, control, limit=2)
                assert replay == documents
                remapped = await _map(reader, recipe, selected_missing, control, insert=False)
                assert remapped["fact_sha256"] == first_hash
                assert remapped["capture_sha256"] == descriptor["capture_sha256"]
                assert remapped["mapping_sha256"] != descriptor["mapping_sha256"]
                assert (
                    remapped["unresolved_raw_source_rows"],
                    remapped["omitted_raw_source_rows"],
                    remapped["unresolved_rows"],
                    remapped["omitted_rows"],
                ) == (2, 2, 2, 1)
                await _assert_ordinary_parity(reader, recipe, selected_missing, control, reads, remapped)
                assert await _identity(reader, fixture) == before
            assert fixture.commits == [fixture.candidates[0].dataset_id]
            assert fixture.source_writer_rejections == fixture.commits
            record_property(
                "proof_scope",
                "genuine-published-source/native-v2-sql; no verified-office policy or inventory activation",
            )


def _successor_recipe(recipe, physical):
    """Use the genuine successor capture while retaining the original logical binding."""
    origin = physical.origin
    physical_pin = fhir.PinnedFHIRMembershipSource(
        origin.schema_name,
        origin.source_id,
        origin.endpoint_id,
        origin.dataset_id,
        origin.dataset_sha256,
        physical.release_id,
        origin.resource_parent_oid,
        recipe.source_pin.alias_scope,
        origin.semantic_projection_as_of,
        retained_capture=physical,
    )
    return replace(recipe, source_pin=fhir.with_fhir_successor(recipe.source_pin, physical_pin))


async def _assert_incomplete_successor_reads(reader, successor, approved, control, reads):
    """Neither original-only nor successor-only custody may authorize the combined source."""
    for incomplete in (reads.original, reads.physical):
        with pytest.raises(fhir.FHIRMembershipSourceError):
            await fhir.read_fhir_source_witness_page_v2(
                reader,
                successor.source_pin,
                registry_schema=control,
                approved_source=approved,
                binding_coordinates=successor.binding_coordinates,
                retained_read=incomplete,
            )


async def test_genuine_empty_membership_capture_and_successor_bind_both_admissions(monkeypatch, record_property):
    """Require both genuine admissions across an empty membership capture and its successor."""
    monkeypatch.setattr(source, "_edition_rows", _empty_then_successor_rows)  # synthetic payloads only
    async with source.native_generic_source(monkeypatch) as fixture:
        original_capture = await source.publish_edition(fixture, 1)
        async with _registry_on_source(fixture, monkeypatch) as serving, _reader(fixture) as reader:
            _, control, _ = serving
            recipe, approved, _ = await retained._approved_capture_recipe(fixture, serving, original_capture)
            await _temp_heaps(reader)
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                before = await _identity(reader, fixture)
                await _assert_capture_census(reader, original_capture, 0)
                reads, documents, pages = await _extract(
                    reader, recipe, approved, control, limit=1, target="witness_replay"
                )
                assert documents == [] and len(pages) == 1 and pages[0].next_resource is None
                empty = await _map(reader, recipe, approved, control, table="witness_replay", insert=False)
                assert all(empty[field] == 0 for field in facts._WITNESS_COUNTERS_V2)
                assert empty["fact_sha256"] == hashlib.sha256(b"").hexdigest()
                await _assert_ordinary_parity(reader, recipe, approved, control, reads, empty)
                assert await _identity(reader, fixture) == before
            physical = await source.publish_edition(fixture, 2)
            origin = physical.origin
            successor = _successor_recipe(recipe, physical)
            assert successor.binding_coordinates == recipe.binding_coordinates
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                before = await _identity(reader, fixture)
                await _assert_capture_census(reader, original_capture, 0)
                await _assert_capture_census(reader, physical, 2)
                reads, documents, _ = await _extract(
                    reader, successor, approved, control, limit=1, target="witness_facts"
                )
                assert type(reads) is fhir.RetainedFHIRSuccessorReads
                assert reads.original.capture == original_capture and reads.physical.capture == physical
                binding = fhir.fhir_source_witness_binding_v2(successor.source_pin)
                assert len(set(binding.values())) == 3
                assert all(fact["source_binding"] == binding for fact in documents)
                for edge in (fact for fact in documents if fact["kind"] == "edge"):
                    assert edge["source_scope_json"]["capture_sha256"] == original_capture.capture_sha256
                    assert edge["evidence_basis"][:3] == [origin.dataset_id, origin.source_id, physical.release_id]
                descriptor = await _map(reader, successor, approved, control)
                assert (
                    descriptor["raw_source_rows"],
                    descriptor["accepted_raw_source_rows"],
                    descriptor["membership_rows"],
                    descriptor["provider_only_rows"],
                ) == (2, 2, 1, 1)
                assert descriptor["fact_rows"] == len(documents) == 4
                assert descriptor["unresolved_raw_source_rows"] == descriptor["unresolved_rows"] == 0
                await _assert_ordinary_parity(reader, successor, approved, control, reads, descriptor)
                await _assert_incomplete_successor_reads(reader, successor, approved, control, reads)
                assert await _identity(reader, fixture) == before
            assert len(fixture.commits) == 2 and fixture.source_writer_rejections == fixture.commits
            record_property(
                "proof_scope",
                "genuine-original-plus-successor/native-v2-sql; empty membership census, no zero-resource Source claim",
            )


async def test_provider_only_v2_sql_uses_genuine_source_headers(monkeypatch):
    from process import network_provider_only_projection as projection

    monkeypatch.setattr(source, "_edition_rows", _census_rows)
    async with source.native_generic_source(monkeypatch) as fixture:
        capture = await source.publish_edition(fixture, 1)
        async with _registry_on_source(fixture, monkeypatch) as serving, _reader(fixture) as reader:
            _, control, _ = serving
            recipe, approved, network_id = await retained._approved_capture_recipe(fixture, serving, capture)
            await _temp_heaps(reader)
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                await _extract(reader, recipe, approved, control, limit=2, target="witness_facts")
                descriptor = await _map(reader, recipe, approved, control)
                assert descriptor["provider_only_rows"] == 1
                query = projection._source_query(recipe, 'pg_temp."witness_facts"', control, descriptor_version=2)
                parameters = projection._source_arguments(recipe, approved, descriptor_version=2)
                projected = await reader.fetch(query, *parameters)
                assert {(row["network_id"], row["provider_system"], row["provider_id"]) for row in projected} == {
                    (network_id, "npi", "1234567893")
                }
                assert len(projected) == 1 and len(projected[0]["evidence_id"]) == 64
                # Tamper only the unsealed temporary expectation to exercise the SQL header predicate.
                await reader.execute(
                    "UPDATE pg_temp.witness_facts SET fact_json=jsonb_set(fact_json,'{malformed}','true'::jsonb) "
                    "WHERE fact_json->>'kind'='resource' AND fact_json->>'resource_id'='P-provider-only'"
                )
                assert await reader.fetch(query, *parameters) == []
