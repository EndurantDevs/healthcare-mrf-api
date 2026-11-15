# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native output repairs retain complete tuples and refuse damaged candidate parity."""

import importlib
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from db import models
from db.plan_release_storage import storage_relation
from process import provider_directory_output_canonical_backfill as canonical
from process import provider_directory_output_contact_backfill as contacts
from process import provider_directory_output_coordinate_backfill as coordinates
from process import provider_directory_output_location_rebuild as locations
from process import provider_directory_output_npi_backfill as npis
from process.provider_directory_generic_source_policy import source_transaction
from tests.cms_output_snapshot_postgres_support import provision_native_outputs
from tests.provider_directory_dataset_rehydrate_postgres_support import _migrations
from tests.provider_directory_source_postgres_support import source_component_database

directory = importlib.import_module("process.provider_directory_fhir")
SOURCE = "repair-example"
OBSERVED_AT = datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None)


async def _seed_canonical_outputs(component):
    """Seed canonical history before the original output adoption transaction."""
    async with component.admin_engine.begin() as connection:
        await connection.execute(
            models.ProviderDirectorySource.__table__.insert().values(
                source_id=SOURCE,
                org_name="Example source",
                canonical_api_base="https://directory.example.test/fhir",
            )
        )
        await connection.execute(
            models.ProviderDirectoryCanonicalResource.__table__.insert(),
            [
                {
                    "canonical_api_base": "https://directory.example.test/fhir",
                    "resource_type": "Location",
                    "resource_id": "office",
                    "first_seen_run_id": "original-first-run",
                },
                {
                    "canonical_api_base": "https://previous.example.test/fhir",
                    "resource_type": "Device",
                    "resource_id": "kept",
                    "first_seen_run_id": "original-first-run",
                },
            ],
        )
        await connection.execute(
            models.ProviderDirectorySourceResource.__table__.insert().values(
                source_id="previous-source",
                canonical_api_base="https://previous.example.test/fhir",
                resource_type="Device",
                resource_id="kept",
                last_seen_run_id="previous-run",
            )
        )


async def _seed_outputs(component, *, canonical_source=False):
    """Seed historical rows and adopt them through the original native output gate.

    Args:
        component: Authenticated native component with its independent administrator.
        canonical_source: Include canonical history and Source control rows.
    Returns:
        None; preserves seeded rows through output custody adoption.
    """
    async with component.admin_engine.begin() as connection:
        for model in (models.ProviderDirectoryOrganization, models.ProviderDirectoryPractitioner):
            await connection.execute(
                model.__table__.insert().values(
                    source_id=SOURCE,
                    resource_id="1234567890",
                    npi=None,
                    fhir_meta={"versionId": "original"},
                    last_seen_run_id="repair-run",
                    observed_at=OBSERVED_AT,
                    updated_at=OBSERVED_AT,
                )
            )
        await connection.execute(
            models.ProviderDirectoryLocation.__table__.insert().values(
                source_id=SOURCE,
                resource_id="office",
                name="Original office",
                country_code="US",
                telephone_number="212-555-0100 ext 42",
                latitude="12.500000",
                longitude="-71.250000",
                fhir_meta={"versionId": "original"},
                last_seen_run_id="repair-run",
                observed_at=OBSERVED_AT,
                updated_at=OBSERVED_AT,
            )
        )
    if canonical_source:
        await _seed_canonical_outputs(component)
    await provision_native_outputs(component)


@asynccontextmanager
async def _repair_scope(monkeypatch, *, canonical_source=False):
    async def seed(component):
        await _seed_outputs(component, canonical_source=canonical_source)

    async with source_component_database(
        monkeypatch, migration_prefixes=_migrations(), before_provision=seed
    ) as component:
        monkeypatch.setattr(directory, "db", component.database)
        yield component


async def _read_outputs(component):
    async with source_transaction(component.database):
        return {
            name: [
                dict(row._mapping)
                for row in await component.database.all(
                    f'SELECT * FROM "mrf"."provider_directory_{name}" ORDER BY source_id,resource_id'
                )
            ]
            for name in ("organization", "practitioner", "location")
        }


@pytest.mark.asyncio
async def test_native_npi_and_location_repairs_publish_complete_preserved_tuples(monkeypatch):
    async with _repair_scope(monkeypatch) as component:
        before = await _read_outputs(component)
        assert await npis.backfill_resource_id_npis(directory, run_id="repair-run", source_ids=[SOURCE]) == {
            "Organization": 1,
            "Practitioner": 1,
        }
        assert await contacts.backfill_location_contacts(directory) == {"location_contact_rows_updated": 1}
        await coordinates.backfill_location_coordinates(directory, run_id="repair-run", source_ids=[SOURCE])
        after = await _read_outputs(component)
        for target in ("organization", "practitioner"):
            expected_by_field = dict(before[target][0], npi=1234567890, updated_at=after[target][0]["updated_at"])
            assert after[target] == [expected_by_field]
        location_by_field = dict(before["location"][0])
        location_by_field.update(
            phone_number="2125550100",
            phone_extension="42",
            latitude=after["location"][0]["latitude"],
            longitude=after["location"][0]["longitude"],
            updated_at=after["location"][0]["updated_at"],
        )
        assert after["location"] == [location_by_field]
        assert await npis.backfill_resource_id_npis(directory, run_id="repair-run", source_ids=[SOURCE]) == {
            "Organization": 0,
            "Practitioner": 0,
        }


@pytest.mark.asyncio
async def test_native_late_location_parity_refusal_preserves_all_incumbent_tuples(monkeypatch):
    async with _repair_scope(monkeypatch) as component:
        before = await _read_outputs(component)
        finish_indexes = locations.stages.finish_output_indexes

        async def corrupt_before_freeze(directory, capability, stage):
            # Fault the writable candidate, then retain normal freeze and complete parity validation.
            await capability.session.execute(
                text(f'UPDATE "{stage.schema}"."{stage.relation}" SET name=\'Changed candidate\'')
            )
            await finish_indexes(directory, capability, stage)

        monkeypatch.setattr(locations.stages, "finish_output_indexes", corrupt_before_freeze)
        with pytest.raises(RuntimeError, match="contact_parity_failed"):
            await contacts.backfill_location_contacts(directory)
        assert await _read_outputs(component) == before


async def _canonical_snapshot(component):
    async with source_transaction(component.database):
        return {
            target: {
                "rows": [
                    dict(row._mapping)
                    for row in await component.database.all(
                        f'SELECT * FROM "mrf"."{target}" ORDER BY resource_type,resource_id'
                    )
                ],
            }
            for target in canonical.TARGETS
        }


async def _retained_rows(component, kind, target):
    """Read complete rollback contents by the exact QName recorded in the publication journal."""
    from process.provider_directory_output_snapshot_catalog import decoded
    from process.provider_directory_output_snapshot_retention import RetainedOutputReceipt

    async with source_transaction(component.database):
        plan = decoded(
            await component.database.scalar('SELECT plan FROM "mrf"."pd_dataset_migration_plan" WHERE singleton')
        )
        entries = [
            entry for entry in plan["output_maintenance"]["operations"].values() if entry["operation"]["kind"] == kind
        ]
        assert len(entries) == 1
        receipts = [RetainedOutputReceipt.from_payload(item) for item in entries[0]["history"][-1]["receipts"]]
        retained = next(receipt for receipt in receipts if receipt.target == target)
        return [
            dict(row._mapping)
            for row in await component.database.all(
                f"SELECT * FROM {storage_relation(retained.schema, retained.retained_name)} ORDER BY resource_id"
            )
        ]


@pytest.mark.asyncio
async def test_native_canonical_two_root_publication_preserves_history_and_unselected_tuples(monkeypatch):
    async with _repair_scope(monkeypatch, canonical_source=True) as component:
        typed = await _read_outputs(component)
        before = await _canonical_snapshot(component)
        publication_by_field = await canonical.backfill_canonical_resources(
            directory, resources="Location,Organization,Practitioner"
        )
        assert publication_by_field["canonical_rows"] == publication_by_field["source_edge_rows"] == 3
        after = await _canonical_snapshot(component)
        assert await _read_outputs(component) == typed
        for relation_name in canonical.TARGETS:
            previous = [resource for resource in before[relation_name]["rows"] if resource["resource_id"] == "kept"]
            assert [
                resource for resource in after[relation_name]["rows"] if resource["resource_id"] == "kept"
            ] == previous
            assert len(after[relation_name]["rows"]) == 4
            assert await _retained_rows(component, "canonical_backfill", relation_name) == before[relation_name]["rows"]
        canonical_by_type = {
            resource["resource_type"]: resource
            for resource in after[canonical.TARGETS[0]]["rows"]
            if resource["resource_id"] != "kept"
        }
        assert set(canonical_by_type) == {"Location", "Organization", "Practitioner"}
        assert canonical_by_type["Location"]["first_seen_run_id"] == "original-first-run"
        assert all(resource["fhir_meta"] == {"versionId": "original"} for resource in canonical_by_type.values())
        assert canonical_by_type["Location"]["payload_json"]["name"] == "Original office"
        assert await canonical.backfill_canonical_resources(directory, resources="Location") == {
            "resources": {"Location": {"canonical_rows": 1, "source_edge_rows": 1}},
            "canonical_rows": 1,
            "source_edge_rows": 1,
        }
        assert await _read_outputs(component) == typed


@pytest.mark.asyncio
async def test_native_canonical_fresh_control_refusal_rolls_back_both_output_candidates(monkeypatch):
    async with _repair_scope(monkeypatch, canonical_source=True) as component:
        before = await _canonical_snapshot(component)
        typed = await _read_outputs(component)
        final_fence = canonical._final_fence

        async def change_committed_control_then_fence(*args):
            async with component.admin_engine.begin() as connection:
                await connection.execute(
                    text("UPDATE mrf.provider_directory_source SET org_name='Changed source' WHERE source_id=:source"),
                    {"source": SOURCE},
                )
            await final_fence(*args)

        monkeypatch.setattr(canonical, "_final_fence", change_committed_control_then_fence)
        with pytest.raises(RuntimeError, match="canonical_control_changed"):
            await canonical.backfill_canonical_resources(directory, resources="Location,Organization,Practitioner")
        assert await _canonical_snapshot(component) == before
        assert await _read_outputs(component) == typed


@pytest.mark.asyncio
async def test_native_named_output_rollback_preserves_both_complete_generations(monkeypatch):
    from process import provider_directory_output_snapshot_custody as custody
    from process import provider_directory_output_snapshot_journal as journal
    from process import provider_directory_output_snapshot_retention as retention
    from process.provider_directory_output_snapshot_catalog import decoded

    async with _repair_scope(monkeypatch) as component:
        previous = await _read_outputs(component)
        await npis.backfill_resource_id_npis(directory, run_id="repair-run", source_ids=[SOURCE])
        published = await _read_outputs(component)
        async with source_transaction(component.database):
            plan = decoded(
                await component.database.scalar('SELECT plan FROM "mrf"."pd_dataset_migration_plan" WHERE singleton')
            )
        forward_digest, entry = next(iter(plan["output_maintenance"]["operations"].items()))
        forward_receipts = tuple(
            retention.RetainedOutputReceipt.from_payload(receipt_payload)
            for receipt_payload in entry["history"][-1]["receipts"]
        )
        rollback_by_field = {**entry["operation"], "kind": "rollback", "rollback_of": forward_digest}
        async with custody.output_snapshot_transaction(
            directory, operation=rollback_by_field, targets=npis.TARGETS
        ) as capability:
            restored = await retention.restore_retained_outputs(capability, forward_receipts)
            preparations = tuple(
                custody.PreparedOutputReceipt(2, receipt.operation_digest, receipt.incumbent, receipt.successor)
                for receipt in forward_receipts
            )
            await journal.append_output_history(capability, preparations, restored)
        assert await _read_outputs(component) == previous
        async with source_transaction(component.database):
            for receipt in restored:
                retained_rows = [
                    dict(retained_row._mapping)
                    for retained_row in await component.database.all(
                        f"SELECT * FROM {storage_relation(receipt.schema, receipt.retained_name)} ORDER BY resource_id"
                    )
                ]
                assert retained_rows == published[receipt.target.removeprefix("provider_directory_")]
                assert receipt.to_payload()["protocol"] == 2


@pytest.mark.asyncio
async def test_native_late_reader_pin_rolls_back_and_reuses_prepared_pair(monkeypatch):
    from process import provider_directory_artifact_bundle_preparation as bundles
    from process import provider_directory_output_snapshot_custody as custody

    async with _repair_scope(monkeypatch) as component:
        previous = await _read_outputs(component)
        selector_by_name = {"run_id": "repair-run", "source_ids": [SOURCE]}
        operation, incumbents_by_target, counts_by_target, decision_at = await npis._capture_operation(
            directory, selector_by_name
        )
        prepared_stages = []
        prepared = await npis._prepare(
            directory, operation, selector_by_name, incumbents_by_target, counts_by_target, decision_at, prepared_stages
        )

        async def journal(capability, retained):
            await npis.journal.append_output_history(capability, prepared, retained)

        readers = await component.database._connect_reader()
        async with readers.session_factory() as reader, reader.begin():
            await reader.execute(text("SET TRANSACTION READ ONLY"))
            pinned = (
                (await reader.execute(text('SELECT * FROM "mrf"."provider_directory_practitioner"'))).mappings().all()
            )
            with pytest.raises(TimeoutError, match="serving_relation_lock_timeout"):
                async with custody.output_snapshot_transaction(
                    directory, operation=operation, targets=npis.TARGETS
                ) as capability:
                    await bundles.apply_prepared_output_bundle(
                        directory, capability, prepared, after_output_publish=journal
                    )
            assert await _read_outputs(component) == previous
            assert [dict(pinned_row) for pinned_row in pinned] == previous["practitioner"]
        async with custody.output_snapshot_transaction(
            directory, operation=operation, targets=npis.TARGETS
        ) as capability:
            _, retained = await bundles.apply_prepared_output_bundle(
                directory, capability, prepared, after_output_publish=journal
            )
        assert len(retained) == 2
        assert all(
            published_row["npi"] == 1234567890 for published_row in (await _read_outputs(component))["practitioner"]
        )
