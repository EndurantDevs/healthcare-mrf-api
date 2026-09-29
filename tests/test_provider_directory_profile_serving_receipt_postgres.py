# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Common receipt continuity with actual Profile pointer, receipt guards and physical WAL projection."""

import importlib
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from db.connection import Database
from process import provider_directory_cms_serving_receipt as receipts
from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_serving_receipt as continuity
from tests.provider_directory_profile_execution_test_support import _wal_tracker_admission
from tests.test_provider_directory_cms_serving_receipt_postgres import _database, _publish_initial
from tests.test_provider_directory_profile_selection_attestation import _catalog, _computed, _variant_registry_rows

fhir = importlib.import_module("process.provider_directory_fhir")


def _ordinary_execution(retain_cms=False):
    computed = _computed()
    if retain_cms:
        catalog = deepcopy(_catalog())
        catalog["items"].append(
            {"entry_id": "directory", "runnable": True, "profile_enabled": True, "source_ids": ["cms-npd"]}
        )
        source_rows = [
            {
                "source_id": "pdfhir_payer",
                "endpoint_id": "endpoint-1",
                "canonical_api_base": "https://payer.example/fhir",
                "org_name": "Payer",
                "plan_name": "Payer Plan",
            },
            {
                "source_id": "cms-npd",
                "endpoint_id": "endpoint",
                "canonical_api_base": "https://directory.example/fhir",
                "org_name": "Directory",
                "plan_name": None,
            },
            *_variant_registry_rows(),
        ]
        datasets = [
            {
                "endpoint_id": endpoint,
                "dataset_id": dataset,
                "acquisition_root_run_id": root,
                "dataset_hash": digest * 64,
                "status": "published",
                "is_current": True,
                "resource_count": 1,
                "validated_at": "2026-01-01",
                "published_at": "2026-01-01",
                "superseded_at": None,
                "publication_metadata_json": {"source_ids": [source_id]},
            }
            for source_id, endpoint, dataset, root, digest in (
                ("pdfhir_payer", "endpoint-1", "dataset-1", "run-root-1", "b"),
                ("cms-npd", "endpoint", "dataset", "run-synthetic", "a"),
            )
        ]
        computed = selection._computed_selection_from_rows(
            catalog, node_id="dev-node", source_rows=source_rows, dataset_rows=datasets
        )
    identity_by_field = {**computed.identity_payload, "authority_revision": 7}
    attestation = selection.validated_profile_selection_attestation(
        {**identity_by_field, "proof_id": selection._proof_id(identity_by_field)}
    )
    return selection.ProviderDirectoryProfileExecution(attestation, 11)


async def _seed_ordinary_dataset(engine, schema):
    """Seed one ordinary published dataset beside the existing directory dataset."""
    async with engine.begin() as connection:
        await connection.execute(
            text(f"INSERT INTO {schema}.provider_directory_source VALUES ('pdfhir_payer','endpoint-1')")
        )
        await connection.execute(
            text(f"""INSERT INTO {schema}.provider_directory_endpoint_dataset
                SELECT 'dataset-1','endpoint-1',:hash,'run-root-1',status,is_current,published_at,
                '{{"source_ids":["pdfhir_payer"]}}',content_proof_admission_sha256,publication_metadata_sha256
                FROM {schema}.provider_directory_endpoint_dataset WHERE dataset_id='dataset'"""),
            {"hash": "b" * 64},
        )


@asynccontextmanager
async def _ordinary_profile(monkeypatch, *, retain_cms=False):
    """Exercise ordinary Profile publication with real pointer guards and common-receipt storage."""
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema)
        execution = _ordinary_execution(retain_cms)
        await _seed_ordinary_dataset(engine, schema)
        database = Database()
        await database.connect()
        monkeypatch.setattr(fhir, "db", database)
        admission = _wal_tracker_admission()
        monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
        monkeypatch.setattr(fhir, "_provider_directory_profile_current_wal_bytes", AsyncMock(return_value=0))
        monkeypatch.setattr(fhir, "_configure_provider_directory_artifact_promotion", AsyncMock(return_value=admission))
        monkeypatch.setattr(fhir, "_lock_and_verify_artifact_dataset_fence", AsyncMock())

        # Baseline Profile metadata catalogs have separate native proofs; this slice measures the real common receipt.
        async def metadata_layouts(_geometry):
            layout = await continuity._receipt_storage_layout(fhir, schema)
            return dict.fromkeys(("build_checkpoint", "serving_generation", "delta_receipt"), layout)

        monkeypatch.setattr(fhir, "_profile_cutover_metadata_layouts", metadata_layouts)
        fence = fhir.ProviderDirectoryArtifactDatasetFence(())
        tokens = [
            (
                fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION,
                fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution),
            ),
            (
                fhir._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE,
                fhir._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(fence),
            ),
        ]

        async def apply_profile(*_args, **_kwargs):
            await fhir._verify_active_profile_selection_at_cutover()
            await database.status(
                f"""UPDATE {schema}.provider_directory_profile_serving_generation
                SET generation_id=:generation,selection_proof_id=:proof,control_generation=11,authority_revision=7,
                    source_vector_json=CAST(:vector AS jsonb) WHERE singleton_key='global'""",
                generation="pdprofile_" + "2" * 32,
                proof=execution.attestation.proof_id,
                vector=json.dumps(
                    [{key: pair[key] for key in ("source_id", "dataset_id")} for pair in execution.attestation.pairs]
                ),
            )

        apply = AsyncMock(side_effect=apply_profile)
        monkeypatch.setattr(fhir, "_apply_prepared_artifact_bundle_in_transaction", apply)
        monkeypatch.setattr(fhir, "_verify_active_profile_selection_at_cutover", AsyncMock())
        try:
            yield database, engine, schema, initial, execution, admission, apply
        finally:
            for variable, token in reversed(tokens):
                variable.reset(token)
            await database.disconnect()


async def _promote(schema):
    await fhir._promote_provider_directory_artifact_bundle_transaction((), profile_delta=SimpleNamespace(schema=schema))


@pytest.mark.asyncio
@pytest.mark.parametrize("retain_cms", [False, True])
async def test_ordinary_profile_advances_exact_vector_without_new_address(monkeypatch, retain_cms):
    async with _ordinary_profile(monkeypatch, retain_cms=retain_cms) as (
        _db,
        engine,
        schema,
        initial,
        execution,
        admission,
        _apply,
    ):
        await _promote(schema)
        async with engine.connect() as connection:
            current = await receipts.read_current_receipt(connection, schema)
            assert current["payload"]["predecessor_receipt_id"] == initial["receipt_id"]
            assert current["payload"]["address"] == initial["payload"]["address"]
            assert current["payload"]["doctors"] == initial["payload"]["doctors"]
            assert current["payload"]["cms"] == initial["payload"]["cms"]
            assert current["payload"]["selection"]["proof_id"] == execution.attestation.proof_id
            assert current["payload"]["desired_datasets"] == [
                {key: pair[key] for key in receipts._PIN_FIELDS} for pair in execution.attestation.pairs
            ]
            assert await receipts.verify_historical_receipt(
                connection, schema, initial["receipt_id"], initial["payload"]
            )
        assert admission.wal_tracker.accounted_metadata_wal_bytes > 6 * 64 * 1024


@pytest.mark.asyncio
async def test_receipt_write_failure_rolls_back_profile_and_chain(monkeypatch):
    async with _ordinary_profile(monkeypatch) as (_db, engine, schema, initial, _execution, _admission, _apply):
        append = receipts.append_serving_receipt

        async def fail(session, name, payload):
            await append(session, name, payload)
            raise RuntimeError("synthetic receipt failure")

        monkeypatch.setattr(receipts, "append_serving_receipt", fail)
        with pytest.raises(RuntimeError, match="synthetic receipt failure"):
            await _promote(schema)
        async with engine.connect() as connection:
            assert await receipts.read_current_receipt(connection, schema) == initial
            assert (
                await connection.scalar(text(f"SELECT count(*) FROM {schema}.provider_directory_cms_serving_receipt"))
                == 1
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["index", "trigger", "body", "default", "check", "replica"])
async def test_unsupported_receipt_shape_fails_before_profile_mutation(monkeypatch, shape):
    async with _ordinary_profile(monkeypatch) as (database, engine, schema, initial, _execution, _admission, apply):
        async with engine.begin() as connection:
            await _tamper_receipt_shape(connection, schema, shape)
        with pytest.raises(
            (RuntimeError, fhir.ProviderDirectoryArtifactBuildStale),
            match="shape_unsupported|index_shape_unsupported|receipt_guard_shape_changed",
        ):
            async with database.transaction():
                if shape == "replica":
                    await database.status("SET LOCAL session_replication_role='replica'")
                await _promote(schema)
        apply.assert_not_awaited()
        async with engine.connect() as connection:
            assert await receipts.read_current_receipt(connection, schema) == initial


async def _tamper_receipt_shape(connection, schema, shape):
    statements_by_shape = {
        "index": f"CREATE INDEX synthetic_extra_index ON {schema}.provider_directory_cms_serving_receipt USING gin (payload)",
        "trigger": f"ALTER TABLE {schema}.provider_directory_cms_serving_receipt DISABLE TRIGGER cms_serving_receipt_insert",
        "body": f"CREATE OR REPLACE FUNCTION {schema}.cms_serving_receipt_insert() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN RETURN NULL; END $$",
        "default": f"ALTER TABLE {schema}.provider_directory_cms_serving_receipt ALTER COLUMN created_at SET DEFAULT now()",
        "check": f"ALTER TABLE {schema}.provider_directory_cms_serving_receipt ADD CHECK (length(receipt_id)>0)",
    }
    if shape in statements_by_shape:
        await connection.execute(text(statements_by_shape[shape]))


@pytest.mark.asyncio
async def test_metadata_pool_exhaustion_rejects_before_native_row_locks(monkeypatch):
    async with _ordinary_profile(monkeypatch) as (_db, engine, schema, initial, _execution, admission, apply):
        admission.wal_tracker.accounted_metadata_wal_bytes = admission.geometry.metadata_wal_upper_bound_bytes
        with pytest.raises(RuntimeError, match="metadata_wal_projected"):
            await _promote(schema)
        fhir._lock_and_verify_artifact_dataset_fence.assert_not_awaited()
        apply.assert_not_awaited()
        async with engine.connect() as connection:
            assert await receipts.read_current_receipt(connection, schema) == initial


async def _change_profile_history(engine, schema):
    """Create a stale predecessor while restoring the real transition guard."""
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f"ALTER TABLE {schema}.provider_directory_profile_serving_generation DISABLE TRIGGER cms_serving_profile_transition"
            )
        )
        await connection.execute(
            text(f"UPDATE {schema}.provider_directory_profile_serving_generation SET authority_revision=2")
        )
        await connection.execute(
            text(
                f"ALTER TABLE {schema}.provider_directory_profile_serving_generation ENABLE TRIGGER cms_serving_profile_transition"
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["history", "selection", "vector", "wal"])
async def test_invalid_result_or_stale_history_rolls_back_profile(monkeypatch, mismatch):
    """Mismatched history, selection, vector or WAL proof rolls back the entire Profile publication."""
    async with _ordinary_profile(monkeypatch) as (database, engine, schema, initial, _execution, admission, apply):
        if mismatch == "history":
            await _change_profile_history(engine, schema)
        elif mismatch in {"selection", "vector"}:
            actual_apply = apply.side_effect

            async def wrong_result(*args, **kwargs):
                await actual_apply(*args, **kwargs)
                field, value = (
                    ("selection_proof_id", "c" * 64) if mismatch == "selection" else ("source_vector_json", "[]")
                )
                await database.status(
                    f"UPDATE {schema}.provider_directory_profile_serving_generation SET {field}=:value", value=value
                )

            apply.side_effect = wrong_result
        else:
            append = receipts.append_serving_receipt

            async def exceed_wal(session, name, payload):
                result = await append(session, name, payload)
                monkeypatch.setattr(
                    fhir,
                    "_provider_directory_profile_current_wal_bytes",
                    AsyncMock(return_value=admission.geometry.reservation_bytes_by_storage_class["wal"]),
                )
                return result

            monkeypatch.setattr(receipts, "append_serving_receipt", exceed_wal)
        with pytest.raises(
            Exception, match="history_inconsistent|receipt_result_mismatch|fresh_receipt_required|total_wal"
        ):
            await _promote(schema)
        async with engine.connect() as connection:
            assert (
                await connection.scalar(text(f"SELECT count(*) FROM {schema}.provider_directory_cms_serving_receipt"))
                == 1
            )
            assert (
                await connection.scalar(
                    text(f"SELECT generation_id FROM {schema}.provider_directory_profile_serving_generation")
                )
                == initial["payload"]["profile"]["generation_id"]
            )
        if mismatch == "history":
            apply.assert_not_awaited()
