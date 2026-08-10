# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundaries for UHC source registration and retained ledgers."""

from __future__ import annotations

import asyncio
import copy
import datetime as dt
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import source as source_module
from process.formulary_fhir import source_artifact_binding
from process.formulary_fhir import source_artifact_storage
from process.formulary_fhir import source_artifacts
from process.formulary_fhir import uhc_source
from process.formulary_fhir import uhc_source_artifacts
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import ExactSourceDefinition
from process.formulary_fhir.types import FHIRSourceConfigurationError
from process.formulary_fhir.uhc_source import UHCFormularySourceError
from process.formulary_fhir.uhc_source import UHCFormularySourceManifest
from process.formulary_fhir.uhc_source_artifacts import (
    UHCSourceArtifactRegistration,
)
from tests.test_formulary_fhir_source_artifacts import SOURCE_ID
from tests.test_formulary_fhir_source_artifacts import _catalog
from tests.test_formulary_fhir_source_artifacts import _database
from tests.test_formulary_fhir_source_artifacts import _observation_row
from tests.test_formulary_fhir_source_artifacts import _row
from tests.test_formulary_fhir_source_artifacts import _set_row
from tests.test_uhc_formulary_source import _Database as SourceDatabase
from tests.test_uhc_formulary_source import _manifest_document
from tests.test_uhc_formulary_source import _source_values


def _identities():
    return uhc_source_artifacts.identities_from_uhc_drug_catalog(
        SOURCE_ID,
        _catalog(),
    )


@pytest.mark.asyncio
async def test_cancel_check_supports_sync_async_and_none() -> None:
    calls: list[str] = []

    def sync_check() -> None:
        calls.append("sync")

    async def async_check() -> None:
        calls.append("async")

    await source_artifacts._invoke_cancel_check(None)
    await source_artifacts._invoke_cancel_check(sync_check)
    await source_artifacts._invoke_cancel_check(async_check)
    assert calls == ["sync", "async"]
    assert await source_artifacts._shielded_to_thread(lambda: "done") == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("helper_name", ("header", "observation"))
async def test_registration_headers_reject_stored_drift(helper_name) -> None:
    identities = _identities()
    database = _database()
    if helper_name == "header":
        database.first.return_value = {
            **_set_row(identities),
            "expected_file_count": 47,
        }
        with pytest.raises(RuntimeError, match="set header changed"):
            await source_artifacts._register_set_header(database, identities)
    else:
        observation = "a" * 64
        database.first.return_value = {
            **_observation_row(identities, observation),
            "source_file_set_sha256": "b" * 64,
        }
        with pytest.raises(RuntimeError, match="observation changed"):
            await source_artifacts._register_source_observation(
                database,
                identities,
                observation,
            )


@pytest.mark.asyncio
async def test_required_header_rejects_missing_or_changed_row() -> None:
    identities = _identities()
    database = _database()
    database.first.return_value = None
    with pytest.raises(RuntimeError, match="set header changed"):
        await source_artifacts._require_set_header(database, identities)


@pytest.mark.asyncio
async def test_pending_files_reject_unknown_ledger_state() -> None:
    identities = _identities()
    database = _database()
    database.first.return_value = _set_row(identities)
    database.all.return_value = [
        _row(identity, status="unknown") if index == 0 else _row(identity)
        for index, identity in enumerate(identities)
    ]
    with pytest.raises(RuntimeError, match="state is invalid"):
        await source_artifacts.pending_source_files(identities, database=database)


@pytest.mark.asyncio
async def test_identity_and_complete_set_loaders_reject_missing_state() -> None:
    identities = _identities()
    missing_database = _database()
    missing_database.all.return_value = []
    with pytest.raises(RuntimeError, match="set is missing"):
        await source_artifacts.load_source_artifact_identities(
            SOURCE_ID,
            identities[0].source_file_set_sha256,
            database=missing_database,
        )

    pending_database = _database()
    pending_database.first.return_value = _set_row(identities)
    pending_database.all.return_value = [_row(identity) for identity in identities]
    with pytest.raises(RuntimeError, match="set is incomplete"):
        await source_artifacts.load_complete_source_artifact_set(
            identities,
            database=pending_database,
        )


@pytest.mark.asyncio
async def test_reopen_requires_expected_artifact_root(monkeypatch) -> None:
    identities = _identities()
    complete_set = SimpleNamespace(artifact_set_sha256="a" * 64)
    monkeypatch.setattr(
        source_artifacts,
        "load_source_artifact_identities",
        AsyncMock(return_value=identities),
    )
    monkeypatch.setattr(
        source_artifacts,
        "load_complete_source_artifact_set",
        AsyncMock(return_value=complete_set),
    )
    with pytest.raises(RuntimeError, match="artifact set changed"):
        await source_artifacts.reopen_source_artifact_set(
            SOURCE_ID,
            identities[0].source_file_set_sha256,
            "b" * 64,
            database=object(),
        )


@pytest.mark.asyncio
async def test_binding_rejects_identity_state_and_fill_drift() -> None:
    identities = _identities()
    identity = identities[0]
    wrong_identity_database = _database()
    wrong_identity_database.first.return_value = _row(identities[1])
    with pytest.raises(RuntimeError, match="identity changed"):
        await source_artifact_binding._artifact_record_for_identity(
            wrong_identity_database,
            identity,
        )

    invalid_state_database = _database()
    invalid_state_database.first.return_value = _row(identity, status="unknown")
    with pytest.raises(RuntimeError, match="state is invalid"):
        await source_artifact_binding.bind_verified_source_artifact(
            identity,
            artifact_sha256="1".zfill(64),
            artifact_byte_count=identity.expected_byte_count or 1,
            database=invalid_state_database,
        )
    pending_database = _database()
    pending_database.first.return_value = _row(identity)
    with pytest.raises(ValueError, match="path is required"):
        await source_artifact_binding.bind_verified_source_artifact(
            identity,
            artifact_sha256="1".zfill(64),
            artifact_byte_count=identity.expected_byte_count or 1,
            database=pending_database,
        )


@pytest.mark.asyncio
async def test_fill_pending_handles_verified_invalid_and_lost_update() -> None:
    identity = _identities()[0]
    byte_count = identity.expected_byte_count or 1
    verified_database = _database()
    verified_database.first.return_value = _row(identity, status="verified")
    verified = await source_artifact_binding._fill_pending_source_artifact(
        verified_database,
        identity,
        "1".zfill(64),
        byte_count,
    )
    assert verified.identity == identity

    invalid_database = _database()
    invalid_database.first.return_value = _row(identity, status="unknown")
    with pytest.raises(RuntimeError, match="state is invalid"):
        await source_artifact_binding._fill_pending_source_artifact(
            invalid_database,
            identity,
            "1".zfill(64),
            byte_count,
        )

    lost_update_database = _database()
    lost_update_database.first.return_value = _row(identity)
    lost_update_database.status.return_value = 0
    with pytest.raises(RuntimeError, match="fill failed"):
        await source_artifact_binding._fill_pending_source_artifact(
            lost_update_database,
            identity,
            "1".zfill(64),
            byte_count,
        )


@pytest.mark.asyncio
async def test_binding_rejects_non_identity_input() -> None:
    with pytest.raises(ValueError, match="identity is invalid"):
        await source_artifact_binding.bind_verified_source_artifact(
            object(),
            source_path=Path("/synthetic"),
            artifact_sha256="1" * 64,
            artifact_byte_count=1,
            database=object(),
        )


def test_exact_source_definition_rejects_config_and_metadata_drift() -> None:
    definition = uhc_source.uhc_formulary_source_manifest().definition
    for changed in (
        {"config": object()},
        {"metadata": []},
        {"metadata": {"unsupported": object()}},
        {"metadata": {1: "value"}},
    ):
        with pytest.raises(ValueError, match="exact source"):
            replace(definition, **changed)


@pytest.mark.asyncio
async def test_exact_source_registration_rejects_write_and_hash_drift(monkeypatch) -> None:
    definition = uhc_source.uhc_formulary_source_manifest().definition
    failed_insert = SimpleNamespace(status=AsyncMock(return_value=0))
    with pytest.raises(FHIRSourceConfigurationError, match="registration failed"):
        await source_module._insert_exact_source(failed_insert, definition)

    with pytest.raises(FHIRSourceConfigurationError, match="definition is invalid"):
        await source_module.register_exact_source(object(), database=object())

    database = SourceDatabase([_source_values()])
    drifted_binding = EnabledSourceBinding(
        source_id=definition.source_id,
        config=definition.config,
        configuration_hash="f" * 64,
        alternative_correction=None,
        launch_mode=source_module.LIBRARY_ONLY_LAUNCH_MODE,
    )
    monkeypatch.setattr(
        source_module,
        "load_enabled_source",
        AsyncMock(return_value=drifted_binding),
    )
    with pytest.raises(FHIRSourceConfigurationError, match="inconsistent"):
        await source_module.register_exact_source(definition, database=database)


@pytest.mark.asyncio
async def test_exact_source_registration_normalizes_internal_errors() -> None:
    definition = uhc_source.uhc_formulary_source_manifest().definition

    class BrokenDatabase:
        def transaction(self):
            raise ValueError("synthetic transaction failure")

    with pytest.raises(FHIRSourceConfigurationError, match="registration failed"):
        await source_module.register_exact_source(
            definition,
            database=BrokenDatabase(),
        )


def test_uhc_manifest_contract_rejects_direct_invalid_values(tmp_path) -> None:
    definition = uhc_source.uhc_formulary_source_manifest().definition
    with pytest.raises(ValueError, match="manifest is invalid"):
        UHCFormularySourceManifest(object(), dt.date(2026, 8, 10))
    with pytest.raises(ValueError, match="manifest is invalid"):
        UHCFormularySourceManifest(definition, "2026-08-10")
    for raw_date in (object(), "20260810"):
        with pytest.raises(ValueError, match="reviewed date"):
            uhc_source._reviewed_date(raw_date)

    missing_path = tmp_path / "missing.json"
    with pytest.raises(UHCFormularySourceError, match="manifest is invalid"):
        uhc_source._read_manifest_document(missing_path)
    scalar_path = tmp_path / "scalar.json"
    scalar_path.write_text("[]", encoding="utf-8")
    with pytest.raises(UHCFormularySourceError, match="manifest is invalid"):
        uhc_source._read_manifest_document(scalar_path)
    with pytest.raises(UHCFormularySourceError, match="manifest is invalid"):
        uhc_source.uhc_formulary_source_manifest("not-a-path")


@pytest.mark.parametrize("mutation", ("top", "source", "identity"))
def test_uhc_manifest_rejects_structural_and_identity_drift(mutation) -> None:
    document = copy.deepcopy(_manifest_document())
    if mutation == "top":
        document.pop("importer")
    elif mutation == "source":
        document["source"].pop("display_name")
    else:
        document["source"]["source_id"] = "different-source"
    with pytest.raises(UHCFormularySourceError, match="manifest is invalid"):
        uhc_source._validated_manifest_document(document)


@pytest.mark.asyncio
async def test_uhc_registration_normalizes_source_configuration_error(monkeypatch) -> None:
    monkeypatch.setattr(
        uhc_source,
        "register_exact_source",
        AsyncMock(side_effect=FHIRSourceConfigurationError("synthetic")),
    )
    with pytest.raises(UHCFormularySourceError, match="registration failed"):
        await uhc_source.register_uhc_formulary_source(database=object())


def test_uhc_source_artifact_registration_rejects_empty_and_drift() -> None:
    catalog = _catalog()
    identities = uhc_source_artifacts.identities_from_uhc_drug_catalog(
        SOURCE_ID,
        catalog,
    )
    with pytest.raises(ValueError, match="identities are invalid"):
        UHCSourceArtifactRegistration("a" * 64, catalog, ())
    with pytest.raises(ValueError, match="identities are inconsistent"):
        UHCSourceArtifactRegistration("a" * 64, catalog, identities[:-1])


@pytest.mark.asyncio
async def test_uhc_source_file_registration_rejects_changed_return(monkeypatch) -> None:
    prepared = UHCSourceArtifactRegistration(
        "a" * 64,
        _catalog(),
        _identities(),
    )
    monkeypatch.setattr(
        uhc_source_artifacts,
        "prepare_uhc_source_artifact_registration",
        lambda *_arguments: prepared,
    )
    monkeypatch.setattr(
        uhc_source_artifacts,
        "register_source_file_set",
        AsyncMock(return_value=prepared.identities[:-1]),
    )
    with pytest.raises(RuntimeError, match="registration changed"):
        await uhc_source_artifacts.register_uhc_source_file_set(
            SOURCE_ID,
            object(),
            database=object(),
        )


def test_source_artifact_open_rejects_non_verified_input() -> None:
    with pytest.raises(ValueError, match="verified source artifact"):
        with source_artifact_storage.open_verified_source_artifact(object()):
            pytest.fail("unverified artifact unexpectedly opened")
