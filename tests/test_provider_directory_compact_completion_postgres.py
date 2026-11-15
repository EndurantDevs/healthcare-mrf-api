# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native old/compact reduction parity using the existing Core named factory."""

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from db.plan_release_storage import storage_relation
from process import provider_directory_compact_payload as compact
from process import provider_directory_compact_source_completion as compact_completion
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_policy as source
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_organization_hash import canonical_organization_payload
from process.provider_directory_resource_hash import canonical_practitioner_payload
from tests.test_provider_directory_compact_intake_postgres import (
    _prepare,
    directory,
)
from tests.test_provider_directory_compact_intake_postgres import (
    intake_case as intake_case,
)


async def _candidate(case, dataset_id, *, semantic=True):
    metadata = {
        **case.metadata,
        "resource_hash_contract": directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
        if semantic
        else directory.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    }
    await case.connection.execute(
        "INSERT INTO source_schema.provider_directory_endpoint_dataset VALUES "
        "($1,'endpoint','root','acquiring',false,NULL,$2,NULL)",
        dataset_id,
        json.dumps(metadata),
    )
    candidate = SimpleNamespace(
        **vars(case.candidate),
        resource_hash_contract=metadata["resource_hash_contract"],
        semantic_projection_as_of="2026-11-14",
        source_release=None,
    )
    candidate.dataset_id = dataset_id
    adapter = SimpleNamespace(**vars(case.directory))
    adapter._endpoint_dataset_candidate_metadata = lambda _: metadata
    for name in (
        "is_semantic_resource_hash_contract",
        "resource_payload_sha256_for_contract",
        "_validated_semantic_dataset_payload",
        "_assert_practitioner_semantic_projection_as_of",
        "ProviderDirectoryPractitioner",
        "_merged_semantic_dataset_payload",
    ):
        setattr(adapter, name, getattr(directory, name))
    adapter.RESOURCE_HASH_CONTRACTS = directory.RESOURCE_HASH_CONTRACTS
    return SimpleNamespace(**{**vars(case), "candidate": candidate, "directory": adapter, "metadata": metadata})


def _row(case, family, payload, *, acquired=None):
    return dict(
        dataset_id=case.candidate.dataset_id,
        resource_type=family,
        resource_id=payload["resource_id"],
        payload_json=payload,
        acquired_resource_sha256=acquired,
        payload_hash=directory.resource_payload_sha256_for_contract(
            payload, case.candidate.resource_hash_contract, resource_type=family
        ),
    )


def _payloads():
    payloads_by_family = {
        family: {"resource_id": family.lower(), "active": True, "fhir_meta": None}
        for family in directory.anthem.RESOURCES
    }
    payloads_by_family["PractitionerRole"].update(
        network_refs=["Organization/network"],
        insurance_plan_refs=["InsurancePlan/insuranceplan"],
        location_refs=["Location/location"],
        practitioner_refs=["Practitioner/practitioner"],
        healthcare_service_refs=["HealthcareService/" + "a" * 32, "HealthcareService/" + "a" * 32, 'opaque/é"\\\n'],
        fhir_fetch_url="https://example.invalid/z",
        npi="1000000000",
    )
    payloads_by_family["Organization"] = canonical_organization_payload(
        {**payloads_by_family["Organization"], "name": "Clinic One"}
    )
    payloads_by_family["Practitioner"] = canonical_practitioner_payload(
        {**payloads_by_family["Practitioner"], "names": [{"family": "One", "given": ["Sam"]}]}
    )
    return payloads_by_family


async def _append(case, payloads_by_family, *, acquired=None):
    async with source.source_transaction(case.database):
        await storage.append_generic_source_batch(
            case.database,
            case.directory,
            case.candidate,
            [_row(case, family, payload, acquired=acquired) for family, payload in payloads_by_family.items()],
        )


async def _complete(case):
    async with source.source_transaction(case.database):
        return await completion.complete_generic_source_candidate(case.database, case.directory, case.candidate)


async def _stored(case, binding):
    relation = storage_relation("source_schema", binding["tables"][storage.RESOURCE]["name"])
    records = await case.connection.fetch(
        f'SELECT * FROM {relation} ORDER BY resource_type COLLATE "C",resource_id COLLATE "C"'
    )
    return [
        {**dict(record), "dataset_id": "same-dataset", "payload_json": json.loads(record["payload_json"])}
        for record in records
    ]


async def _observed(case, binding):
    relation = storage_relation("source_schema", binding["tables"]["resource_input"]["name"])
    return [
        tuple(record)
        for record in await case.connection.fetch(f"SELECT * FROM {relation} ORDER BY batch_ordinal,row_ordinal")
    ]


@pytest.mark.asyncio
async def test_compact_completion_matches_old_seven_family_semantics(intake_case):
    completed_rows = []
    for dataset_id, payload_format in (("plain", None), ("compact", compact.FORMAT)):
        case = await _candidate(intake_case, dataset_id)
        loading = await _prepare(case, payload_format=payload_format)
        retired = deepcopy(_payloads()["PractitionerRole"])
        retired["network_refs"] = ["Organization/retired"]
        await _append(case, {"PractitionerRole": retired})
        async with source.source_transaction(case.database):
            await storage.reset_generic_source_family(case.database, case.directory, case.candidate, "PractitionerRole")
        first = _payloads()
        await _append(case, first)
        second = deepcopy(first)
        second["PractitionerRole"]["fhir_fetch_url"] = "https://example.invalid/a"
        second["Organization"] = canonical_organization_payload(
            {**first["Organization"], "name": "Clinic Two", "name_variants": ["Clinic Two"]}
        )
        second["Practitioner"] = canonical_practitioner_payload(
            {**first["Practitioner"], "names": [{"family": "Two", "given": ["Sam"]}]}
        )
        await _append(case, second)
        original = await _observed(case, loading)
        body_table = loading["tables"].get("resource_body_blocks", {}).get("name")
        bodies_before = (
            await case.connection.fetchval(
                f"SELECT sum(jsonb_array_length(bodies)) FROM {storage_relation('source_schema', body_table)}"
            )
            if body_table
            else None
        )
        prepared = await _complete(case)
        completed_rows.append(await _stored(case, prepared))
        assert await _observed(case, prepared) == original
        assert prepared["ordinal"] == max(observed_row[-2] for observed_row in original)
        assert (await _complete(case)) == prepared
        if body_table:
            # Only the two genuinely new name-union bodies are added; Role provenance reuses its earlier alias.
            assert (
                await case.connection.fetchval(
                    f"SELECT sum(jsonb_array_length(bodies)) FROM {storage_relation('source_schema', body_table)}"
                )
                == bodies_before + 2
            )
            assert set(prepared["tables"]) == {
                *storage._COMPACT_SUFFIXES,
                "resource_output_aliases",
                *storage.RELATIONS,
            }
            async with source.source_transaction(case.database):
                with pytest.raises(ValueError, match="original_storage_publisher_required"):
                    await completion.prepare_generic_source_handoff(None, prepared)
    assert completed_rows[0] == completed_rows[1] and len(completed_rows[0]) == 7
    by_type = {resource_row["resource_type"]: resource_row for resource_row in completed_rows[1]}
    assert by_type["PractitionerRole"]["payload_json"]["fhir_fetch_url"].endswith("/z")
    assert by_type["PractitionerRole"]["payload_json"]["location_refs"] == ["Location/location"]
    assert by_type["PractitionerRole"]["payload_json"]["network_refs"] == ["Organization/network"]
    assert len(by_type["Organization"]["payload_json"]["name_variants"]) == 2
    assert len(by_type["Practitioner"]["payload_json"]["names"]) == 2


@pytest.mark.asyncio
async def test_compact_completion_preserves_legacy_last_observation_and_reset(intake_case):
    results = []
    for dataset_id, payload_format in (("plain", None), ("compact", compact.FORMAT)):
        case = await _candidate(intake_case, dataset_id, semantic=False)
        loading = await _prepare(case, payload_format=payload_format)
        await _append(case, _payloads(), acquired="a" * 64)
        async with source.source_transaction(case.database):
            await storage.reset_generic_source_family(case.database, case.directory, case.candidate)
        newest = _payloads()
        newest["PractitionerRole"]["network_refs"] = ["Organization/new"]
        await _append(case, newest, acquired="b" * 64)
        newest["PractitionerRole"]["location_refs"] = ["Location/new"]
        await _append(case, {"PractitionerRole": newest["PractitionerRole"]}, acquired="c" * 64)
        prepared = await _complete(case)
        results.append(await _stored(case, prepared))
        assert prepared["resets"]["*"] == 1 and prepared["ordinal"] == 3
        assert len(await _observed(case, loading)) == 15
    assert results[0] == results[1] and len(results[0]) == 7
    role = next(row for row in results[1] if row["resource_type"] == "PractitionerRole")
    assert role["payload_json"]["network_refs"] == ["Organization/new"]
    assert role["payload_json"]["location_refs"] == ["Location/new"] and role["acquired_resource_sha256"] == "c" * 64


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [asyncio.CancelledError, TimeoutError], ids=["cancelled", "timeout"])
async def test_interrupted_reused_alias_copy_rolls_back_and_resumes(intake_case, monkeypatch, interruption):
    case = await _candidate(intake_case, "candidate")
    loading = await _prepare(case, payload_format=compact.FORMAT)
    first = _payloads()
    await _append(case, first)
    second = deepcopy(first["PractitionerRole"])
    second["fhir_fetch_url"] = "https://example.invalid/a"
    await _append(case, {"PractitionerRole": second})
    observations = await _observed(case, loading)
    before = await case.connection.fetchval(
        "SELECT candidate_storage_json::text FROM source_schema.provider_directory_endpoint_dataset WHERE dataset_id=$1",
        case.candidate.dataset_id,
    )
    original_copy = compact._copy
    copied_rows = []

    async def interrupted(connection, tables, root, columns, records):
        await original_copy(connection, tables, root, columns, records)
        if root == "aliases" and tables.aliases.endswith("_oa") and records:
            copied_rows.extend(records)
            raise interruption()

    with monkeypatch.context() as patch:
        patch.setattr(compact, "_copy", interrupted)
        with pytest.raises(interruption):
            await _complete(case)
    assert copied_rows and await _observed(case, loading) == observations
    assert (
        await case.connection.fetchval(
            "SELECT candidate_storage_json::text FROM source_schema.provider_directory_endpoint_dataset WHERE dataset_id=$1",
            case.candidate.dataset_id,
        )
        == before
    )
    output_aliases = storage_relation(
        "source_schema", loading["tables"]["resource_input"]["name"].removesuffix("_ri") + "_oa"
    )
    assert await case.connection.fetchval("SELECT to_regclass($1)::text IS NULL", output_aliases)
    prepared = await _complete(case)
    assert prepared["phase"] == "prepared" and await _observed(case, prepared) == observations
    assert len(await _stored(case, prepared)) == 7
    role = next(
        resource for resource in await _stored(case, prepared) if resource["resource_type"] == "PractitionerRole"
    )
    assert role["payload_json"]["fhir_fetch_url"] == first["PractitionerRole"]["fhir_fetch_url"]
    assert await _complete(case) == prepared


@pytest.mark.asyncio
@pytest.mark.parametrize("payload_format", [None, compact.FORMAT])
async def test_role_conflict_rolls_back_completion_and_reset_resumes(intake_case, payload_format):
    case = await _candidate(intake_case, "candidate")
    loading = await _prepare(case, payload_format=payload_format)
    payload = _payloads()["PractitionerRole"]
    await _append(case, {"PractitionerRole": payload})
    conflicting_by_field = {
        **payload,
        "network_refs": ["Organization/different"],
        "location_refs": ["Location/different"],
    }
    await _append(case, {"PractitionerRole": conflicting_by_field})
    observations = await _observed(case, loading)
    with pytest.raises(ValueError, match="resource_payload_conflict"):
        await _complete(case)
    assert await _observed(case, loading) == observations
    async with source.source_transaction(case.database):
        current = await storage.prepare_generic_source_candidate(case.database, case.directory, case.candidate)
        assert current["phase"] == "loading" and current["ordinal"] == 2
        await storage.reset_generic_source_family(case.database, case.directory, case.candidate, "PractitionerRole")
    await _append(case, {"PractitionerRole": payload})
    prepared = await _complete(case)
    assert len(await _stored(case, prepared)) == 1
    assert len(await _observed(case, prepared)) == 3


@pytest.mark.asyncio
async def test_compact_completion_failure_and_prepared_mutation_refuse(intake_case, monkeypatch):
    case = await _candidate(intake_case, "candidate")
    loading = await _prepare(case, payload_format=compact.FORMAT)
    await _append(case, _payloads())
    original = await _observed(case, loading)
    finish = compact_completion._finish_indexes

    async def fail(*args):
        await finish(*args)
        raise RuntimeError("after indexes")

    with monkeypatch.context() as patch:
        patch.setattr(compact_completion, "_finish_indexes", fail)
        with pytest.raises(RuntimeError, match="after indexes"):
            await _complete(case)
    assert await _observed(case, loading) == original
    prepared = await _complete(case)
    assert (await _complete(case)) == prepared
    aliases = storage_relation("source_schema", prepared["tables"]["resource_output_aliases"]["name"])
    await case.connection.execute(f"UPDATE {aliases} SET npi_text='changed' WHERE resource_type='PractitionerRole'")
    with pytest.raises(ValueError, match="compact_alias_changed"):
        await _complete(case)
    assert len(await _stored(case, prepared)) == 7
    await case.connection.execute(f"UPDATE {aliases} SET npi_text='1000000000' WHERE resource_type='PractitionerRole'")
    async with source.source_transaction(case.database):
        async with source.source_owner_transaction(case.database):
            await case.database.status(
                "UPDATE source_schema.provider_directory_endpoint_dataset SET status='failed' WHERE dataset_id='candidate'"
            )
        await storage.discard_generic_source_inputs(case.database, case.directory, case.candidate, None)
    for table in prepared["tables"].values():
        assert await case.connection.fetchval(
            "SELECT to_regclass($1) IS NULL", storage_relation("source_schema", table["name"])
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage,error",
    [
        ("references", "compact_reference_changed"),
        ("body", "compact_block_changed"),
        ("ordinal", "compact_observation_ordinals_changed"),
    ],
)
async def test_compact_loading_validation_restores_candidate_after_refusal(intake_case, damage, error):
    case = await _candidate(intake_case, "candidate")
    loading = await _prepare(case, payload_format=compact.FORMAT)
    await _append(case, _payloads())
    tables = loading["tables"]
    if damage == "references":
        relation = storage_relation("source_schema", tables["resource_reference_sets"]["name"])
        await case.connection.execute(f"UPDATE {relation} SET fallback_refs=ARRAY['changed']")
    elif damage == "body":
        relation = storage_relation("source_schema", tables["resource_body_blocks"]["name"])
        await case.connection.execute(f"UPDATE {relation} SET bodies=bodies||'[{{}}]'::jsonb")
    else:
        relation = storage_relation("source_schema", tables["resource_aliases"]["name"])
        await case.connection.execute(f"UPDATE {relation} SET row_ordinal=row_ordinal+20")
    header_sql = (
        "SELECT candidate_storage_json::text FROM source_schema.provider_directory_endpoint_dataset WHERE dataset_id=$1"
    )
    before = await case.connection.fetchval(header_sql, case.candidate.dataset_id)
    original = await _observed(case, loading)
    relations_sql = "SELECT relname FROM pg_class WHERE relnamespace='source_schema'::regnamespace ORDER BY relname"
    relations = await case.connection.fetch(relations_sql)
    with pytest.raises(ValueError, match=error):
        await _complete(case)
    assert await case.connection.fetchval(header_sql, case.candidate.dataset_id) == before
    assert await _observed(case, loading) == original
    assert await case.connection.fetch(relations_sql) == relations


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage,error",
    [
        ("references", "compact_reference_changed"),
        ("body", "compact_block_changed"),
        ("ordinal", "compact_observation_ordinals_changed"),
        ("selected", "compact_selected_observation_changed"),
    ],
)
async def test_compact_prepared_replay_checks_complete_storage(intake_case, damage, error):
    case = await _candidate(intake_case, "candidate")
    await _prepare(case, payload_format=compact.FORMAT)
    await _append(case, _payloads())
    prepared = await _complete(case)
    tables = prepared["tables"]
    if damage == "references":
        relation = storage_relation("source_schema", tables["resource_reference_sets"]["name"])
        await case.connection.execute(f"UPDATE {relation} SET fallback_refs=ARRAY['changed']")
    elif damage == "body":
        relation = storage_relation("source_schema", tables["resource_body_blocks"]["name"])
        await case.connection.execute(f"UPDATE {relation} SET bodies=bodies||'[{{}}]'::jsonb")
    elif damage == "ordinal":
        relation = storage_relation("source_schema", tables["resource_aliases"]["name"])
        await case.connection.execute(f"UPDATE {relation} SET row_ordinal=row_ordinal+20")
    else:
        relation = storage_relation("source_schema", tables["resource_output_aliases"]["name"])
        await case.connection.execute(f"UPDATE {relation} SET payload_hash=$1", "0" * 64)
    with pytest.raises(ValueError, match=error):
        await _complete(case)
    stored = await case.connection.fetchval(
        "SELECT candidate_storage_json::text FROM source_schema.provider_directory_endpoint_dataset WHERE dataset_id=$1",
        case.candidate.dataset_id,
    )
    assert json.loads(stored) == prepared


@pytest.mark.asyncio
async def test_compact_completion_retains_each_reference_shape(intake_case):
    case = await _candidate(intake_case, "candidate")
    await _prepare(case, payload_format=compact.FORMAT)
    shapes = (["HealthcareService/" + "a" * 32, "HealthcareService/" + "a" * 32], [], None, "missing")
    expected_rows = []
    for ordinal, references in enumerate(shapes):
        payload = {**_payloads()["PractitionerRole"], "resource_id": str(ordinal)}
        if references == "missing":
            payload.pop("healthcare_service_refs")
        else:
            payload["healthcare_service_refs"] = references
        await _append(case, {"PractitionerRole": payload})
        expected_rows.append({**_row(case, "PractitionerRole", payload), "dataset_id": "same-dataset"})
    prepared = await _complete(case)
    assert await _stored(case, prepared) == expected_rows
    reference_sets = storage_relation("source_schema", prepared["tables"]["resource_reference_sets"]["name"])
    assert (
        await case.connection.fetchval(f"SELECT count(*) FROM {reference_sets} WHERE canonical_refs IS NOT NULL") == 2
    )
    await case.connection.execute(
        f"UPDATE {reference_sets} SET canonical_refs=ARRAY['00000000-0000-0000-0000-000000000001'::uuid]"
    )
    with pytest.raises(ValueError, match="compact_reference_changed"):
        await _complete(case)
