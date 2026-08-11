# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for reviewed source-generation readback."""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
import datetime
import importlib
import json
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from process import provider_directory_fhir_manual_catalog as manual_catalog
from process.provider_directory_fhir_census_binding import (
    bind_current_version_census_contract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD,
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD,
    current_version_census_request,
)
from process.provider_directory_fhir_root_policy import (
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    ReviewedRootPolicy,
)
from process.provider_directory_fhir_subset_profiles import (
    SERVER_ISSUED_SUBSET_BOUNDED_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION,
    SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V5_CAMPAIGN_ID,
)
from tests.provider_directory_fhir_subset_activation_support import (
    single_root_activation_inputs,
)
from tests.provider_directory_fhir_subset_completion_support import (
    build_subset_contract,
)
from tests.provider_directory_reviewed_root_policy_pg import (
    _insert_policy_source,
    _install_policy_predecessors,
)
from tests.provider_directory_reviewed_subset_activation_pg_upsert import (
    _copy_upsert_sql,
    _values_upsert_sql,
)
from tests.tin_npi_connector_postgres_support import TransactionalSchema


importer = importlib.import_module("process.provider_directory_fhir")
CUTOFF = "2026-08-01T12:00:00.000000Z"
SUCCESSOR_CAMPAIGN_ID = (
    "provider-directory-reviewed-subset-2026-08-11-v5-r2"
)


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("reviewed source-generation tests need PostgreSQL")
    if "test" not in database_name.lower():
        pytest.skip("reviewed source-generation tests need a test database")


@asynccontextmanager
async def _reviewed_source_database(monkeypatch):
    schema = f"provider_reviewed_source_{uuid.uuid4().hex[:12]}"
    database = Database()
    observer = Database()
    is_disposable_database_ready = False
    endpoint_table = importer.ProviderDirectoryAPIEndpoint.__table__
    source_table = importer.ProviderDirectorySource.__table__
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        is_disposable_database_ready = True
        await observer.connect()
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setattr(endpoint_table, "schema", schema)
        monkeypatch.setattr(source_table, "schema", schema)
        monkeypatch.setattr(importer, "db", database)
        await database.create_table(endpoint_table)
        await database.create_table(source_table)
        yield database, observer, schema
    finally:
        if is_disposable_database_ready:
            await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await observer.disconnect()
        await database.disconnect()


def _reviewed_v5_source(*, required_root_count: int = 1) -> dict:
    source_id = manual_catalog.reviewed_manual_census_source_id()
    seed_row = manual_catalog.reviewed_manual_census_seed_rows(
        source_id,
        root_policy=ReviewedRootPolicy(required_root_count),
    )[0]
    source_record = importer._source_row_from_seed(seed_row)
    resources = list(seed_row["metadata_json"][
        "provider_directory_server_issued_subset_resources"
    ])
    request = current_version_census_request(
        {
            "provider_directory_acquisition_strategy": (
                "server-issued-traversal-subset"
            ),
            "provider_directory_census_cutoff": CUTOFF,
            "source_ids": [source_id],
            "resources": resources,
            "import_resources": True,
            "full_refresh": True,
            "resource_limit": 0,
            "page_limit": 0,
            "page_count": seed_row["metadata_json"][
                "provider_directory_current_version_census_page_count"
            ],
            "source_concurrency": 1,
            "resource_scan_concurrency": 1,
            "bulk_export": False,
            "stale_cleanup": False,
            "publish_artifacts": False,
            "publish_after_acquisition": False,
            "publish_corroboration": False,
        },
        allowed_resources=importer.DEFAULT_RESOURCES,
        now=datetime.datetime(2026, 8, 2, tzinfo=datetime.UTC),
    )
    assert request is not None
    contract = bind_current_version_census_contract(request, [source_record])
    assert contract.is_terminal_count_window_required
    return source_record


def _prior_v5_source(successor_source: dict) -> dict:
    source_record = deepcopy(successor_source)
    source_record.pop(CURRENT_VERSION_CENSUS_CONTRACT_FIELD, None)
    metadata = source_record["metadata_json"]
    metadata[importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY] = (
        DIRECT_V5_CAMPAIGN_ID
    )
    metadata[REVIEWED_ROOT_POLICY_METADATA_KEY] = ReviewedRootPolicy(1).document()
    return source_record


def _reviewed_v4_source(v5_source: dict) -> dict:
    source_record = deepcopy(v5_source)
    source_record.pop(CURRENT_VERSION_CENSUS_CONTRACT_FIELD, None)
    metadata = source_record["metadata_json"]
    metadata[CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD] = (
        SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION
    )
    metadata[CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD] = list(
        SERVER_ISSUED_SUBSET_BOUNDED_COMPLETION_SCOPES
    )
    metadata[
        importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY
    ] = "synthetic-reviewed-subset-v4"
    return source_record


async def _persisted_source(observer: Database, schema: str, source_id: str):
    rows = await observer.all(
        f"""
        SELECT source_id, endpoint_id, canonical_api_base,
               requires_registration, requires_api_key, auth_type,
               metadata_json
          FROM "{schema}".provider_directory_source
         WHERE source_id = :source_id
        """,
        source_id=source_id,
    )
    assert len(rows) == 1
    return dict(rows[0]._mapping)


@pytest.mark.asyncio
async def test_policy_guard_allows_pending_v4_to_v5_generation(monkeypatch):
    scenario = await TransactionalSchema.create(monkeypatch)
    try:
        await _install_policy_predecessors(scenario)
        contract = build_subset_contract(
            strategy_version=SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
            completion_scopes=SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
            campaign_id="synthetic-terminal-window-profile",
        )
        v5_source, _, _ = single_root_activation_inputs(contract=contract)
        v4_source = _reviewed_v4_source(v5_source)
        await _insert_policy_source(scenario, v4_source)
        statement, parameters = _values_upsert_sql(
            scenario,
            note="v5-refresh",
            source_id=v5_source["source_id"],
            endpoint_id=v5_source["endpoint_id"],
            canonical_api_base=v5_source["canonical_api_base"],
            incoming_metadata=v5_source["metadata_json"],
        )

        await scenario.connection.execute(statement, *parameters)

        raw_metadata = await scenario.connection.fetchval(
            f"""
            SELECT metadata_json::text
              FROM {scenario.quoted_schema}.provider_directory_source
             WHERE source_id = 'synthetic-source'
            """
        )
        persisted_source = deepcopy(v5_source)
        persisted_source["metadata_json"] = json.loads(raw_metadata)
        assert importer._is_reviewed_subset_generation_exact(
            v5_source,
            persisted_source,
        )
    finally:
        await scenario.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("upsert_path", ("values", "copy"))
async def test_policy_guard_allows_pending_v5_successor_rotation(
    monkeypatch,
    upsert_path,
):
    """Admit only the pending campaign and two-root policy transition."""

    scenario = await TransactionalSchema.create(monkeypatch)
    try:
        await _install_policy_predecessors(scenario)
        contract = build_subset_contract(
            strategy_version=SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
            completion_scopes=SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
            campaign_id=SUCCESSOR_CAMPAIGN_ID,
        )
        successor_source, _, _ = single_root_activation_inputs(
            contract=contract
        )
        successor_source["metadata_json"][
            REVIEWED_ROOT_POLICY_METADATA_KEY
        ] = ReviewedRootPolicy(2).document()
        prior_source = _prior_v5_source(successor_source)
        await _insert_policy_source(scenario, prior_source)
        if upsert_path == "values":
            statement, parameters = _values_upsert_sql(
                scenario,
                note="v5-successor-refresh",
                source_id=successor_source["source_id"],
                endpoint_id=successor_source["endpoint_id"],
                canonical_api_base=successor_source["canonical_api_base"],
                incoming_metadata=successor_source["metadata_json"],
            )
            await scenario.connection.execute(statement, *parameters)
        else:
            await scenario.connection.execute(
                _copy_upsert_sql(
                    scenario,
                    source_id=successor_source["source_id"],
                    endpoint_id=successor_source["endpoint_id"],
                ),
                json.dumps(successor_source["metadata_json"], sort_keys=True),
            )

        raw_metadata = await scenario.connection.fetchval(
            f"""
            SELECT metadata_json::text
              FROM {scenario.quoted_schema}.provider_directory_source
             WHERE source_id = 'synthetic-source'
            """
        )
        persisted_source = deepcopy(successor_source)
        persisted_source["metadata_json"] = json.loads(raw_metadata)
        assert importer._is_reviewed_subset_generation_exact(
            successor_source,
            persisted_source,
        )
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_pending_v4_generation_commits_exact_v5_before_return(
    monkeypatch,
):
    async with _reviewed_source_database(monkeypatch) as (
        _database,
        observer,
        schema,
    ):
        v5_source = _reviewed_v5_source()
        v4_source = _reviewed_v4_source(v5_source)
        await importer._upsert_provider_directory_source_rows([v4_source])

        await importer._upsert_provider_directory_source_rows(
            [v5_source],
            require_reviewed_subset_generation=True,
        )

        persisted_source = await _persisted_source(
            observer,
            schema,
            v5_source["source_id"],
        )
        assert importer._is_reviewed_subset_generation_exact(
            v5_source,
            persisted_source,
        )


@pytest.mark.asyncio
async def test_prior_v5_generation_commits_two_root_successor_before_return(
    monkeypatch,
):
    """Commit the new campaign and policy before any caller can probe."""

    async with _reviewed_source_database(monkeypatch) as (
        _database,
        observer,
        schema,
    ):
        successor_source = _reviewed_v5_source(required_root_count=2)
        prior_source = _prior_v5_source(successor_source)
        await importer._upsert_provider_directory_source_rows([prior_source])

        await importer._upsert_provider_directory_source_rows(
            [successor_source],
            require_reviewed_subset_generation=True,
        )

        persisted_source = await _persisted_source(
            observer,
            schema,
            successor_source["source_id"],
        )
        assert importer._is_reviewed_subset_generation_exact(
            successor_source,
            persisted_source,
        )
        metadata = persisted_source["metadata_json"]
        assert metadata[
            importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY
        ] == SUCCESSOR_CAMPAIGN_ID
        assert metadata[REVIEWED_ROOT_POLICY_METADATA_KEY][
            "required_root_count"
        ] == 2


@pytest.mark.asyncio
async def test_readback_failure_rolls_back_v5_generation(
    monkeypatch,
):
    async with _reviewed_source_database(monkeypatch) as (
        _database,
        observer,
        schema,
    ):
        v5_source = _reviewed_v5_source()
        v4_source = _reviewed_v4_source(v5_source)
        await importer._upsert_provider_directory_source_rows([v4_source])
        monkeypatch.setattr(
            importer,
            "_is_reviewed_subset_generation_exact",
            lambda _expected, _persisted: False,
        )

        with pytest.raises(
            RuntimeError,
            match="reviewed_subset_generation_persistence_mismatch",
        ):
            await importer._upsert_provider_directory_source_rows(
                [v5_source],
                require_reviewed_subset_generation=True,
            )

        persisted_source = await _persisted_source(
            observer,
            schema,
            v5_source["source_id"],
        )
        assert persisted_source["metadata_json"][
            CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD
        ] == SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION
