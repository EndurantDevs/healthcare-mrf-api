# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import uuid

import pytest

from tests.test_provider_directory_source_local_outcomes import (
    _disposable_outcome_database,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260904223000_provider_directory_michigan_generation_retirement.py"
)


def _migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_michigan_generation_retirement",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_michigan_generation_retirement_is_exact_and_idempotent():
    database = await _disposable_outcome_database()
    schema = f"provider_directory_michigan_{uuid.uuid4().hex[:12]}"
    migration = _migration()
    metadata = {
        "provider_directory_candidate_status": (
            "pending_two_matching_exhaustive_acquisitions"
        ),
        "provider_directory_verification_campaign_id": (
            "provider-directory-michigan-2026-07-19-v1"
        ),
        "provider_directory_configured_endpoint_id": (
            "ec3b30a95396d3e30e7a433d523a775de9075543a39e058948c6215650cea684"
        ),
        "provider_directory_override": (
            "michigan_mhbapp_public_provider_directory"
        ),
        "provider_directory_acquisition_enabled": True,
        "preserved": "evidence",
    }
    try:
        await database.status(f"CREATE SCHEMA {schema};")
        await database.status(
            f"CREATE TABLE {schema}.provider_directory_source ("
            "source_id varchar(64) PRIMARY KEY, endpoint_id varchar(64), "
            "canonical_api_base text, metadata_json json, updated_at timestamp);"
        )
        await database.status(
            f"INSERT INTO {schema}.provider_directory_source VALUES ("
            ":source_id, :endpoint_id, :api_base, CAST(:metadata AS json), now());",
            source_id="pdfhir_75511676b61b2bddb6f94322",
            endpoint_id=(
                "cce4c9f158fb638bf43b5c659a2b5526aa12f2fc5cca247c622442cd537e4510"
            ),
            api_base="https://mi.fhir.mhbapp.com/pd/api/v1",
            metadata=json.dumps(metadata),
        )
        await database.status(migration._retirement_sql(schema))
        await database.status(migration._retirement_sql(schema))
        stored = await database.first(
            f"SELECT endpoint_id, metadata_json::text AS metadata "
            f"FROM {schema}.provider_directory_source;"
        )
        assert stored._mapping["endpoint_id"] == (
            "cce4c9f158fb638bf43b5c659a2b5526aa12f2fc5cca247c622442cd537e4510"
        )
        assert json.loads(stored._mapping["metadata"]) == {
            key: metadata_value
            for key, metadata_value in metadata.items()
            if key not in {
                "provider_directory_candidate_status",
                "provider_directory_verification_campaign_id",
            }
        }
    finally:
        await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()
