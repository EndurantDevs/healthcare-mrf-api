# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-local adoption preserves other sources, predecessor evidence and rollback."""

import os
import re
from datetime import datetime
from uuid import uuid4

import pytest
from sqlalchemy import insert, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateIndex

from db import models
from process import source_profile_result_archive as archive


def _database_url():
    raw = os.environ.get("HLTHPRT_SOURCE_PROFILE_ARCHIVE_TEST_DSN")
    if not raw:
        pytest.skip("HLTHPRT_SOURCE_PROFILE_ARCHIVE_TEST_DSN is not set")
    url = make_url(raw)
    assert url.host in {"127.0.0.1", "localhost"} and url.port == 5440
    assert re.fullmatch(r"hc_source_profile_[0-9a-f]{32}", url.database or "")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _create_family(session, schema):
    spec = archive.native.ReferenceFamilySpec(
        "source-profile-fixture",
        (*archive.MODELS, models.ProviderProfileSourcePublication, models.ProviderProfileSourcePin),
    )
    await archive.native._create_model_family(session, spec, schema, create_indexes=False)
    metadata = archive.native.MetaData(schema=schema)
    for model in (*archive.MODELS, models.ProviderProfileSourcePin):
        table = model.__table__.to_metadata(metadata, schema=schema)
        for index in table.indexes:
            await session.execute(CreateIndex(index))
    for statement in archive.pins.pin_guard_statements(schema):
        await session.execute(text(statement))
    for statement in archive.pins.pin_policy_statements(schema):
        await session.execute(text(statement))


def _artifact_row(run, artifact_id):
    return {
        "artifact_id": artifact_id,
        "run_id": run["run_id"],
        "source_key": run["source_key"],
        "file_name": "manifest.json",
        "source_url": "https://example.org/profile",
        "category": "profile",
        "content_sha256": "b" * 64,
        "content_bytes": 100,
    }


def _seed_rows(importer):
    run = retained_run(importer)
    source_key, version, _ = archive.SOURCES[importer]
    artifact_id = uuid4().hex * 2
    record_id = uuid4().hex * 2
    fact_id = uuid4().hex * 2
    retained_rows = (
        run,
        _artifact_row(run, artifact_id),
        {
            "record_id": record_id,
            "run_id": run["run_id"],
            "artifact_id": artifact_id,
            "source_key": source_key,
            "source_record_key": "synthetic-license",
            "raw_payload": {},
            "normalized_payload": {"visibility": "public", "schema_version": version},
            "matched_npi": 1234567890,
            "match_status": "deterministic",
            "match_evidence": {"method": "retained_registry", "registry_generation": "d" * 64},
        },
        {
            "fact_id": fact_id,
            "run_id": run["run_id"],
            "source_record_id": record_id,
            "npi": 1234567890,
            "logical_fact_key": "c" * 64,
            "category": "education",
            "fact_type": "education_history",
            "display": "Synthetic school",
            "value_json": {},
            "availability": "available",
            "assertion_type": "reported",
            "verification_status": "source",
            "source_json": {
                **run["source_manifest"]["source"],
                "schema_version": version,
                "source_record_id": record_id,
                "run_id": run["run_id"],
            },
            "sensitive": False,
            "public_default": True,
            "published_at": run["finished_at"],
        },
        {
            "source_key": source_key,
            "current_run_id": run["run_id"],
            "previous_run_id": None,
            "published_at": run["finished_at"],
        },
    )

    return retained_rows


async def _seed(session, schema, importer, *, parent_run_id=None):
    retained_rows = _seed_rows(importer)
    if parent_run_id is not None:
        artifact = (
            (
                await session.execute(
                    text(f'SELECT * FROM "{schema}".provider_profile_artifact WHERE run_id=:run'),
                    {"run": parent_run_id},
                )
            )
            .mappings()
            .one()
        )
        retained_rows[0]["source_manifest"].update(
            categories=["education", "training", "certifications", "specialties"],
            reprocess_from=parent_run_id,
            expected_current_run_id=parent_run_id,
            reprocessing={
                "source_run_id": parent_run_id,
                "artifact_id": artifact["artifact_id"],
                "manifest_sha256": artifact["content_sha256"],
                "response_envelopes_sha256": "e" * 64,
            },
        )
    metadata = archive.native.MetaData(schema=schema)
    for model, stored_row in zip(
        (*archive.MODELS, models.ProviderProfileSourcePublication), retained_rows, strict=True
    ):
        table = model.__table__.to_metadata(metadata, schema=schema)
        if model is models.ProviderProfileSourcePublication:
            await session.execute(
                pg_insert(table)
                .values(stored_row)
                .on_conflict_do_update(
                    index_elements=["source_key"],
                    set_={
                        "previous_run_id": table.c.current_run_id,
                        "current_run_id": stored_row["current_run_id"],
                        "published_at": stored_row["published_at"],
                    },
                )
            )
        else:
            await session.execute(insert(table), stored_row)
    return retained_rows[0]["run_id"]


async def _drop_family(session, schema):
    names = (*archive.TABLES, "provider_profile_source_publication", archive.pins.TABLE)
    await session.execute(text("DROP TABLE " + ", ".join(archive._table(schema, name) for name in names) + " RESTRICT"))
    await session.execute(text(f'DROP FUNCTION "{schema}".provider_profile_pinned_run_guard() RESTRICT'))
    await session.execute(text(f'DROP FUNCTION "{schema}".provider_profile_pinned_truncate_guard() RESTRICT'))
    await session.execute(text(f'DROP SCHEMA "{schema}" RESTRICT'))


def retained_run(importer_id, run_id=None):
    source_key, version, jurisdiction = archive.SOURCES[importer_id]
    categories = {
        "MA": ["education", "training"],
        "KY": ["education"],
        "TN": ["education", "training", "specialties"],
        "RI": ["education", "specialties", "privileges"],
        "NY": ["education", "training", "certifications"],
    }[jurisdiction]
    agency = {
        "MA": "Massachusetts Board of Registration in Medicine",
        "KY": "Kentucky Board of Medical Licensure",
        "TN": "Tennessee Department of Health",
        "RI": "Rhode Island Department of Health",
        "NY": "New York State Department of Health",
    }[jurisdiction]
    coverage = {
        "TN": "regular_md_do_all_ranks_statuses_locations",
        "RI": "active_md_do_excluding_limited_volunteer",
        "NY": "supported_nppes_derived_ny_physician_license_roots",
    }.get(jurisdiction, "synthetic_registry_cohort")
    return {
        "run_id": run_id or uuid4().hex,
        "source_key": source_key,
        "schema_version": version,
        "jurisdiction": jurisdiction,
        "status": "completed",
        "source_manifest": {
            "source": {
                "source_key": source_key,
                "source_kind": "state_regulator",
                "jurisdiction": jurisdiction,
                "agency": agency,
                "coverage_scope": coverage,
                "registry_generation": "d" * 64,
            },
            "max_providers": None,
            "categories": categories,
            "snapshot_sha256": "d" * 64,
            "cohort_sha256": "c" * 64,
            "full_cohort_licenses": 1,
            "requested_licenses": 1,
            "resume_from": None,
            "expected_current_run_id": None,
            "control_run_id": "source-control-provenance-only",
            **({"license_types": ["MD", "DO"]} if jurisdiction == "RI" else {}),
        },
        "metrics": {"published": True},
        "error": None,
        "started_at": datetime(2026, 1, 1),
        "finished_at": datetime(2026, 1, 2),
    }
