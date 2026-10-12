# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in fresh acquisition through the installed operator, without hosted services."""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq
import pytest
from sqlalchemy import func, select, text

from db.models.custom_import import (
    CustomImportCaptureBundle,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportPublicationEvent,
)
from process.custom_import.capture_store import load_replayable_parquet_bundle
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.snowflake_bundle import SnowflakeBundleStatementBuilder
from process.custom_import.snowflake_source_binding import load_snowflake_source_binding
from tests import custom_import_installed_acquisition as upstream
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_installed_operator_postgres import _command
from tests.test_custom_import_installed_operator_postgres import installed_application as installed_application
from tests.test_custom_import_snowflake_operator_cli import _registration_document


def _searchable_registration():
    document = _registration_document()
    document["definition"]["schema"]["root"]["fields"][0]["projection_slot"] = 1
    order_terms = [{"field": "npi", "direction": "asc", "nulls": "last"}]
    document["definition"]["query"] = {"root_fields": ["npi"], "order": order_terms}
    document["definition"]["selection_profiles"] = [
        {"id": "synthetic_profile", "selection": order_terms, "context_dimensions": []}
    ]
    definition = CustomImportDefinition.from_mapping(document["definition"])
    document["source_binding"]["definition_sha256"] = definition.digest
    document["source_binding"]["schema_sha256"] = definition.schema_digest
    return document


async def _reserve_snapshot_identity(case):
    """Isolate physical snapshot IDs before this disposable schema allocates a family."""
    async with case.engine.begin() as connection:
        await connection.execute(
            text(
                "SELECT setval(pg_get_serial_sequence(:registry, 'family_id'), "
                "to_regnamespace(:control)::oid::bigint << 24, false)"
            ),
            {"registry": f"{case.schema_name}.custom_import_snapshot_family", "control": case.schema_name},
        )


@pytest.mark.asyncio
async def test_installed_fresh_acquisition_capture_publication_and_read(installed_application, monkeypatch):
    installed, outside = installed_application
    async with isolated_publication_case() as case:
        # Physical snapshot names are global; give this disposable control schema a disjoint ID range.
        await _reserve_snapshot_identity(case)
        registered = await _command(
            installed_application,
            case,
            "register",
            snowflake=True,
            stdin_payload=canonical_json(_searchable_registration()).encode(),
        )
        async with case.sessions() as session:
            for model in (CustomImportExecution, CustomImportCaptureBundle, CustomImportGeneration):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
            loaded = await load_snowflake_source_binding(
                session,
                definition_revision_id=registered["definition_revision_id"],
                source_binding_revision_id=registered["source_binding_revision_id"],
            )
        builder = SnowflakeBundleStatementBuilder(approved_relations=loaded.approved_relations)
        statement = builder.build_statement(builder.prepare_request(loaded.definition, bindings=loaded.bundle_bindings))
        with TemporaryDirectory(prefix="fresh-acquisition-", dir=outside) as temporary:
            temporary = Path(temporary)
            mount = temporary / "operator"
            upstream.write_synthetic_credentials(mount)
            configuration = temporary / "source.json"
            configuration.write_text(json.dumps({"registered": registered, "sql": statement.sql}))
            bootstrap = temporary / "synthetic_upstream.py"
            shutil.copyfile(upstream.__file__, bootstrap)
            launch = asyncio.create_subprocess_exec

            async def installed_with_upstream(executable, entry, *arguments, **options):
                assert executable == sys.executable and entry == str(installed / "custom_import_snowflake_operator.py")
                return await launch(
                    executable, str(bootstrap), entry, str(configuration), str(mount), *arguments, **options
                )

            with monkeypatch.context() as context:
                context.setattr(asyncio, "create_subprocess_exec", installed_with_upstream)
                receipt = await _command(
                    installed_application,
                    case,
                    "execute",
                    "--definition-revision-id",
                    loaded.definition_revision_id,
                    "--source-binding-revision-id",
                    loaded.source_binding_revision_id,
                    "--idempotency-key",
                    "synthetic-fresh-acquisition",
                    snowflake=True,
                )
        assert not temporary.exists()
        await _verify_retained_publication(case, loaded, receipt)


async def _verify_retained_publication(case, loaded, receipt):
    assert receipt["status"] == "activated"
    async with case.sessions() as session:
        execution = await session.get(CustomImportExecution, receipt["execution_id"])
        assert execution.state == "completed"
        assert execution.source_binding_revision_id == loaded.source_binding_revision_id
        seal = await session.get(CustomImportGenerationSeal, receipt["generation_id"])
        assert seal.root_count == seal.family_count == seal.winner_count == len(upstream.NPIS)
        assert await session.scalar(select(func.count()).select_from(CustomImportPublicationEvent)) == 1
        captures = await load_replayable_parquet_bundle(
            session,
            capture_bundle_id=execution.capture_bundle_id,
            dataset_id=loaded.dataset_id,
            definition_revision_id=loaded.definition_revision_id,
            schema_revision_id=loaded.schema_revision_id,
        )
        assert len(captures) == 1 and captures[0].receipt.source_snapshot_token == upstream.SNAPSHOT
        retained_rows = [
            retained for part in captures[0].parts for retained in pq.read_table(BytesIO(part)).to_pylist()
        ]
        assert retained_rows == [{"npi": npi} for npi in upstream.NPIS]
