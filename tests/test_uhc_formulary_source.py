# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import json
from contextlib import asynccontextmanager

import pytest

import process.formulary_fhir.uhc_source as source_module
from process.formulary_fhir.uhc_source import UHCFormularySourceError
from process.formulary_fhir.uhc_source import register_uhc_formulary_source
from process.formulary_fhir.uhc_source import uhc_formulary_source_manifest


class _Database:
    def __init__(self, source_rows: list[dict[str, object]] | None = None) -> None:
        self.source_rows = list(source_rows or [])
        self.statements: list[str] = []

    @asynccontextmanager
    async def transaction(self):
        yield

    async def all(self, statement: str, **params: object):
        self.statements.append(statement)
        return [
            row
            for row in self.source_rows
            if row["source_id"] == params["source_id"]
            or row["canonical_base"] == params["canonical_base"]
        ]

    async def first(self, statement: str, **params: object):
        self.statements.append(statement)
        return next(
            (
                row
                for row in self.source_rows
                if row["source_id"] == params["source_id"]
            ),
            None,
        )

    async def status(self, statement: str, **params: object):
        self.statements.append(statement)
        if not statement.startswith("INSERT INTO"):
            return None
        self.source_rows.append(
            {
                "source_id": params["source_id"],
                "canonical_base": params["canonical_base"],
                "display_name": params["display_name"],
                "enabled": True,
                "runtime_config_json": json.loads(
                    str(params["runtime_config_json"])
                ),
                "metadata_json": json.loads(str(params["metadata_json"])),
            }
        )
        return 1


def _manifest_document() -> dict[str, object]:
    return json.loads(
        source_module.DEFAULT_UHC_SOURCE_MANIFEST.read_text(encoding="utf-8")
    )


def _source_values() -> dict[str, object]:
    manifest = uhc_formulary_source_manifest()
    definition = manifest.definition
    return {
        "source_id": definition.source_id,
        "canonical_base": definition.config.canonical_base,
        "display_name": definition.display_name,
        "enabled": True,
        "runtime_config_json": {
            "timeout_seconds": definition.config.timeout_seconds,
            "max_attempts": definition.config.max_attempts,
            "page_size": definition.config.page_size,
            "max_pages": definition.config.max_pages,
            "max_total_resources": definition.config.max_total_resources,
            "max_response_bytes": definition.config.max_response_bytes,
        },
        "metadata_json": definition.metadata,
    }


def test_manifest_is_exact_mrf_source_and_does_not_claim_fhir() -> None:
    manifest_document = _manifest_document()
    manifest = uhc_formulary_source_manifest()
    metadata = manifest.definition.metadata

    assert manifest.source_id == "uhc-official-formulary-mrf"
    assert manifest.definition.config.canonical_base == (
        "https://providermrf.uhc.com"
    )
    assert metadata["source_kind"] == "cms-mrf-drug-catalog"
    assert metadata["source_families"] == ["cs", "ifp"]
    assert "fhir_release" not in metadata
    assert "resource_types" not in metadata
    assert metadata["publication_intent"] == "none"
    assert manifest_document["reviewed_at"] == "2026-08-10"
    assert manifest.definition.config.canonical_base not in repr(manifest)
    assert manifest.definition.display_name not in repr(manifest)


@pytest.mark.parametrize(
    "mutation",
    ["schema", "source_kind", "families", "publication", "fhir_claim"],
)
def test_manifest_rejects_contract_drift(mutation: str) -> None:
    document = copy.deepcopy(_manifest_document())
    source = document["source"]
    assert isinstance(source, dict)
    metadata = source["metadata_json"]
    assert isinstance(metadata, dict)
    if mutation == "schema":
        document["schema_version"] = True
    elif mutation == "source_kind":
        metadata["source_kind"] = "fhir-r4"
    elif mutation == "families":
        metadata["source_families"] = ["ifp"]
    elif mutation == "publication":
        metadata["publication_intent"] = "requested"
    else:
        metadata["fhir_release"] = "R4"

    with pytest.raises(UHCFormularySourceError, match="manifest is invalid"):
        source_module._validated_manifest_document(document)


@pytest.mark.asyncio
async def test_registration_is_idempotent_and_never_rewrites() -> None:
    database = _Database()

    first = await register_uhc_formulary_source(database=database)
    second = await register_uhc_formulary_source(database=database)

    assert first == second
    assert database.source_rows == [_source_values()]
    assert sum(
        statement.startswith("INSERT INTO") for statement in database.statements
    ) == 1
    assert all(
        not statement.lstrip().startswith("UPDATE ")
        for statement in database.statements
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("collision", ["source_id", "canonical_base"])
async def test_registration_rejects_collision_without_repair(
    collision: str,
) -> None:
    conflicting = _source_values()
    if collision == "source_id":
        conflicting["display_name"] = "Conflicting source"
    else:
        conflicting["source_id"] = "conflicting-source"
    database = _Database([conflicting])

    with pytest.raises(UHCFormularySourceError, match="registration failed"):
        await register_uhc_formulary_source(database=database)

    assert database.source_rows == [conflicting]
    assert all(
        not statement.startswith("INSERT INTO")
        for statement in database.statements
    )
