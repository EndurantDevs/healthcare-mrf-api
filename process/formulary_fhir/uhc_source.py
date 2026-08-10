# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Checked-in source identity for UHC official formulary MRF artifacts."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from db.models import db
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import ExactSourceDefinition
from process.formulary_fhir.source import register_exact_source
from process.formulary_fhir.types import FHIRSourceConfigurationError
from process.formulary_fhir.types import enabled_source_config


DEFAULT_UHC_SOURCE_MANIFEST = Path(__file__).with_name(
    "uhc_source_manifest.json"
)
UHC_FORMULARY_SOURCE_ID = "uhc-official-formulary-mrf"
UHC_FORMULARY_CANONICAL_BASE = "https://providermrf.uhc.com"
MANIFEST_FIELDS = frozenset(
    {"schema_version", "importer", "reviewed_at", "source"}
)
SOURCE_FIELDS = frozenset(
    {
        "source_id",
        "canonical_base",
        "display_name",
        "enabled",
        "runtime_config_json",
        "metadata_json",
    }
)
METADATA_BY_FIELD = {
    "access_requirement": "none",
    "launch_mode": "manual-library",
    "publication_intent": "none",
    "source_families": ["cs", "ifp"],
    "source_kind": "cms-mrf-drug-catalog",
    "verification_state": "pending-first-reviewed-artifact-twin",
}


class UHCFormularySourceError(RuntimeError):
    """Report a bounded source registration or manifest error."""


@dataclass(frozen=True, slots=True, repr=False)
class UHCFormularySourceManifest:
    """Bind one exact public MRF source without calling it a FHIR endpoint."""

    definition: ExactSourceDefinition = field(repr=False)
    reviewed_at: dt.date

    def __post_init__(self) -> None:
        if type(self.definition) is not ExactSourceDefinition:
            raise ValueError("UHC formulary source manifest is invalid")
        if type(self.reviewed_at) is not dt.date:
            raise ValueError("UHC formulary source manifest is invalid")

    @property
    def source_id(self) -> str:
        """Return the exact registered source identifier."""

        return self.definition.source_id

    def __repr__(self) -> str:
        return (
            "UHCFormularySourceManifest("
            f"source_id={self.source_id!r}, reviewed_at={self.reviewed_at!r})"
        )


def _reviewed_date(raw_value: object) -> dt.date:
    if type(raw_value) is not str:
        raise ValueError("reviewed date is invalid")
    reviewed_at = dt.date.fromisoformat(raw_value)
    if reviewed_at.isoformat() != raw_value:
        raise ValueError("reviewed date is invalid")
    return reviewed_at


def _validated_manifest_document(
    manifest_by_field: object,
) -> UHCFormularySourceManifest:
    try:
        if type(manifest_by_field) is not dict or set(manifest_by_field) != (
            MANIFEST_FIELDS
        ):
            raise ValueError("manifest fields mismatch")
        if (
            manifest_by_field.get("schema_version") != 1
            or type(manifest_by_field.get("schema_version")) is not int
            or manifest_by_field.get("importer") != "formulary-fhir"
        ):
            raise ValueError("manifest identity mismatch")
        source_by_field = manifest_by_field.get("source")
        if type(source_by_field) is not dict or set(source_by_field) != (
            SOURCE_FIELDS
        ):
            raise ValueError("source fields mismatch")
        metadata_by_field = source_by_field.get("metadata_json")
        if metadata_by_field != METADATA_BY_FIELD:
            raise ValueError("source metadata mismatch")
        config = enabled_source_config(
            canonical_base=source_by_field.get("canonical_base"),
            enabled=source_by_field.get("enabled"),
            runtime_config_json=source_by_field.get("runtime_config_json"),
        )
        definition = ExactSourceDefinition(
            source_id=strict_text(
                source_by_field.get("source_id"),
                "source id",
                64,
            ),
            display_name=strict_text(
                source_by_field.get("display_name"),
                "source display name",
                256,
            ),
            config=config,
            metadata=metadata_by_field,
        )
        if (
            definition.source_id != UHC_FORMULARY_SOURCE_ID
            or definition.config.canonical_base != UHC_FORMULARY_CANONICAL_BASE
        ):
            raise ValueError("source identity mismatch")
        return UHCFormularySourceManifest(
            definition=definition,
            reviewed_at=_reviewed_date(manifest_by_field.get("reviewed_at")),
        )
    except (KeyError, TypeError, ValueError, FHIRSourceConfigurationError):
        raise UHCFormularySourceError(
            "UHC formulary source manifest is invalid"
        ) from None


def _read_manifest_document(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise UHCFormularySourceError(
            "UHC formulary source manifest is invalid"
        ) from None
    if type(document) is not dict:
        raise UHCFormularySourceError(
            "UHC formulary source manifest is invalid"
        )
    return document


def uhc_formulary_source_manifest(
    path: Path = DEFAULT_UHC_SOURCE_MANIFEST,
) -> UHCFormularySourceManifest:
    """Read and strictly validate the sole checked-in UHC MRF source."""

    if not isinstance(path, Path):
        raise UHCFormularySourceError(
            "UHC formulary source manifest is invalid"
        )
    return _validated_manifest_document(_read_manifest_document(path))


async def register_uhc_formulary_source(
    *,
    database: Any = db,
) -> EnabledSourceBinding:
    """Idempotently register the exact UHC MRF source without rewriting it."""

    manifest = uhc_formulary_source_manifest()
    try:
        return await register_exact_source(
            manifest.definition,
            database=database,
        )
    except FHIRSourceConfigurationError:
        raise UHCFormularySourceError(
            "UHC formulary source registration failed"
        ) from None


__all__ = (
    "DEFAULT_UHC_SOURCE_MANIFEST",
    "UHCFormularySourceError",
    "UHCFormularySourceManifest",
    "UHC_FORMULARY_CANONICAL_BASE",
    "UHC_FORMULARY_SOURCE_ID",
    "register_uhc_formulary_source",
    "uhc_formulary_source_manifest",
)
