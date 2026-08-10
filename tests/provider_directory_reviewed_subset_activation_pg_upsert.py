# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Executable catalog-upsert parity for an activated reviewed source."""

from __future__ import annotations

import importlib
import json

from sqlalchemy import Column, DateTime, MetaData, String, Table, Text, cast
from sqlalchemy import func, literal
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert

from db.models.system import ProviderDirectorySource
from process.provider_directory_fhir_subset_activation_contract import (
    ACTIVATION_METADATA_KEY,
    ACTIVATION_METADATA_KEY_V2,
    PENDING_STATUS,
    VERIFIED_STATUS,
)
from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    POLICY_VERIFIED_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    reviewed_root_policy_document,
)
from tests.tin_npi_connector_postgres_support import expect_postgres_error


def _source_table(schema_name: str) -> Table:
    """Model the reduced PostgreSQL fixture for expression compilation."""

    return Table(
        "provider_directory_source",
        MetaData(),
        Column("source_id", String, primary_key=True),
        Column("endpoint_id", String),
        Column("canonical_api_base", Text),
        Column("metadata_json", JSONB),
        Column("updated_at", DateTime),
        schema=schema_name,
    )


def _incoming_metadata(note: str) -> dict[str, object]:
    return {
        "provider_directory_candidate_status": PENDING_STATUS,
        ACTIVATION_METADATA_KEY: {"spoofed": True},
        "ordinary_catalog_note": note,
    }


def _values_upsert_sql(
    scenario,
    *,
    note: str,
    source_id: str = "synthetic-source",
    endpoint_id: str = "endpoint-a",
    canonical_api_base: str = "https://directory.example.test/fhir",
    incoming_metadata: dict[str, object] | None = None,
) -> tuple[str, tuple[object, ...]]:
    """Compile the production SQLAlchemy VALUES-upsert expression."""

    importer = importlib.import_module("process.provider_directory_fhir")
    source_table = _source_table(scenario.schema)
    statement = pg_insert(source_table).values(
        source_id=source_id,
        endpoint_id=endpoint_id,
        canonical_api_base=canonical_api_base,
        metadata_json=cast(
            literal(
                json.dumps(
                    incoming_metadata or _incoming_metadata(note),
                    sort_keys=True,
                )
            ),
            JSONB,
        ),
        updated_at=func.transaction_timestamp(),
    )
    metadata_expression = importer._effective_update_expression(
        source_table,
        statement,
        "metadata_json",
    )
    statement = statement.on_conflict_do_update(
        index_elements=[source_table.c.source_id],
        set_={
            "canonical_api_base": statement.excluded.canonical_api_base,
            "metadata_json": metadata_expression,
            "updated_at": func.transaction_timestamp(),
        },
    )
    compiled = statement.compile(
        dialect=postgresql.asyncpg.dialect(paramstyle="numeric_dollar"),
        compile_kwargs={"render_postcompile": True},
    )
    parameters = tuple(
        compiled.params[position] for position in compiled.positiontup or ()
    )
    return str(compiled), parameters


def _copy_upsert_sql(
    scenario,
    *,
    source_id: str = "synthetic-source",
    endpoint_id: str = "endpoint-a",
) -> str:
    """Render the production COPY-conflict metadata expression."""

    importer = importlib.import_module("process.provider_directory_fhir")
    metadata_expression = importer._effective_update_sql(
        ProviderDirectorySource.__table__,
        "metadata_json",
        target_prefix="provider_directory_source",
        incoming_prefix="EXCLUDED",
    )
    return f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            metadata_json, updated_at
        ) VALUES (
            '{source_id}', '{endpoint_id}',
            'https://directory.example.test/fhir',
            $1::jsonb, pg_catalog.transaction_timestamp()
        )
        ON CONFLICT (source_id) DO UPDATE
            SET metadata_json = {metadata_expression},
                updated_at = pg_catalog.transaction_timestamp()
    """


def _legacy_exhaustive_generation(note: str) -> dict[str, object]:
    return {
        "provider_directory_candidate_status": (
            "verified_two_matching_exhaustive_acquisitions"
        ),
        "provider_directory_verification_campaign_id": "reviewed-campaign-v1",
        "ordinary_catalog_note": note,
    }


def _partition_generation(note: str) -> dict[str, object]:
    return {
        "provider_directory_candidate_status": (
            "pending_two_matching_exhaustive_acquisitions"
        ),
        "provider_directory_verification_campaign_id": "reviewed-campaign-v2",
        "provider_directory_last_updated_partition_acquisition": {
            "enabled": True,
            "resources": {
                "PractitionerRole": {
                    "start": "1900-01-01T00:00:00Z",
                    "end": "2026-08-11T00:00:00Z",
                    "ceiling": 3000,
                    "minimum_width_seconds": 1,
                    "boundary_precision_seconds": 1,
                    "page_count": 1000,
                    "maximum_pages_per_window": 3,
                    "volatile_metadata_paths": [],
                }
            },
        },
        "ordinary_catalog_note": note,
    }


async def _insert_legacy_generation(
    scenario,
    source_id: str,
    endpoint_id: str,
) -> None:
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            metadata_json, updated_at
        ) VALUES ($1, $2, 'https://directory.example.test/fhir',
                  $3::jsonb, pg_catalog.transaction_timestamp())
        """,
        source_id,
        endpoint_id,
        json.dumps(_legacy_exhaustive_generation("legacy"), sort_keys=True),
    )


async def _assert_partition_generation(
    scenario,
    source_id: str,
    expected_note: str,
) -> None:
    raw_metadata = await scenario.connection.fetchval(
        f"""
        SELECT metadata_json::text
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = $1
        """,
        source_id,
    )
    metadata = json.loads(raw_metadata)
    assert metadata == _partition_generation(expected_note)
    assert REVIEWED_ROOT_POLICY_METADATA_KEY not in metadata
    assert ACTIVATION_METADATA_KEY not in metadata
    assert ACTIVATION_METADATA_KEY_V2 not in metadata


async def prove_unprotected_generation_replacement(scenario) -> None:
    """Replace one legacy exhaustive campaign through both upsert paths."""

    values_source = "legacy-values-source"
    copy_source = "legacy-copy-source"
    await _insert_legacy_generation(
        scenario,
        values_source,
        "endpoint-generation-values",
    )
    await _insert_legacy_generation(
        scenario,
        copy_source,
        "endpoint-generation-copy",
    )
    values_sql, values_parameters = _values_upsert_sql(
        scenario,
        note="values-path",
        source_id=values_source,
        endpoint_id="endpoint-generation-values",
        incoming_metadata=_partition_generation("values-path"),
    )
    await scenario.connection.execute(values_sql, *values_parameters)
    await scenario.connection.execute(
        _copy_upsert_sql(
            scenario,
            source_id=copy_source,
            endpoint_id="endpoint-generation-copy",
        ),
        json.dumps(_partition_generation("copy-path"), sort_keys=True),
    )
    await _assert_partition_generation(
        scenario,
        values_source,
        "values-path",
    )
    await _assert_partition_generation(
        scenario,
        copy_source,
        "copy-path",
    )


async def _assert_marker_and_note(
    scenario,
    marker_by_field,
    expected_note: str,
) -> None:
    source_state = await scenario.connection.fetchrow(
        f"""
        SELECT metadata_json::jsonb
                   ->> 'provider_directory_candidate_status' AS status,
               metadata_json::jsonb -> '{ACTIVATION_METADATA_KEY}' AS marker,
               metadata_json::jsonb ->> 'ordinary_catalog_note' AS note
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
        """
    )
    assert source_state["status"] == VERIFIED_STATUS
    assert json.loads(source_state["marker"]) == marker_by_field
    assert source_state["note"] == expected_note


async def prove_catalog_upserts_preserve_activation(
    scenario,
    marker_by_field,
) -> None:
    """Execute both normal upsert expressions and reject contract drift."""

    values_sql, values_parameters = _values_upsert_sql(scenario, note="values-path")
    await scenario.connection.execute(values_sql, *values_parameters)
    await _assert_marker_and_note(scenario, marker_by_field, "values-path")
    await scenario.connection.execute(
        _copy_upsert_sql(scenario),
        json.dumps(_incoming_metadata("copy-path"), sort_keys=True),
    )
    await _assert_marker_and_note(scenario, marker_by_field, "copy-path")
    drift_sql, drift_parameters = _values_upsert_sql(
        scenario,
        note="drifted-path",
        canonical_api_base="https://drift.example.test/fhir",
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_reviewed_subset_activation_transition_invalid",
        drift_sql,
        *drift_parameters,
    )
    await _assert_marker_and_note(scenario, marker_by_field, "copy-path")


def _policy_incoming_metadata(note: str) -> dict[str, object]:
    return {
        "provider_directory_candidate_status": POLICY_PENDING_STATUS,
        ACTIVATION_METADATA_KEY: {"spoofed": "legacy"},
        ACTIVATION_METADATA_KEY_V2: {"spoofed": "current"},
        REVIEWED_ROOT_POLICY_METADATA_KEY: reviewed_root_policy_document(2),
        "ordinary_catalog_note": note,
    }


async def _assert_policy_marker_and_note(
    scenario,
    marker_by_field,
    expected_note: str,
) -> None:
    raw_metadata = await scenario.connection.fetchval(
        f"""
        SELECT metadata_json::text
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
        """
    )
    metadata = json.loads(raw_metadata)
    assert metadata["provider_directory_candidate_status"] == (POLICY_VERIFIED_STATUS)
    assert metadata[ACTIVATION_METADATA_KEY_V2] == marker_by_field
    assert ACTIVATION_METADATA_KEY not in metadata
    assert metadata[REVIEWED_ROOT_POLICY_METADATA_KEY] == (
        marker_by_field["root_policy"]
    )
    assert metadata["ordinary_catalog_note"] == expected_note


async def prove_policy_catalog_upserts_preserve_activation(
    scenario,
    marker_by_field,
) -> None:
    """Preserve the v2 marker and policy through both catalog upsert paths."""

    values_metadata = _policy_incoming_metadata("values-path")
    values_sql, values_parameters = _values_upsert_sql(
        scenario,
        note="values-path",
        incoming_metadata=values_metadata,
    )
    await scenario.connection.execute(values_sql, *values_parameters)
    await _assert_policy_marker_and_note(scenario, marker_by_field, "values-path")
    await scenario.connection.execute(
        _copy_upsert_sql(scenario),
        json.dumps(_policy_incoming_metadata("copy-path"), sort_keys=True),
    )
    await _assert_policy_marker_and_note(scenario, marker_by_field, "copy-path")
    drift_sql, drift_parameters = _values_upsert_sql(
        scenario,
        note="drifted-path",
        canonical_api_base="https://drift.example.test/fhir",
        incoming_metadata=_policy_incoming_metadata("drifted-path"),
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_reviewed_subset_activation_transition_invalid",
        drift_sql,
        *drift_parameters,
    )
    await _assert_policy_marker_and_note(scenario, marker_by_field, "copy-path")
