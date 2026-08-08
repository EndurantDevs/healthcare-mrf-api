# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact enabled-source binding for dormant formulary synchronization."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from db.models import db
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.types import AlternativeCorrection
from process.formulary_fhir.types import FHIRSourceConfigurationError
from process.formulary_fhir.types import FormularySourceConfig
from process.formulary_fhir.types import enabled_source_config


SOURCE_CONFIGURATION_DOMAIN = "fhir-formulary-source-configuration-v1"
ALTERNATIVE_CORRECTION_METADATA_FIELD = "alternative_reference_correction"
LIBRARY_ONLY_LAUNCH_MODE = "manual-library"


@dataclass(frozen=True, slots=True, repr=False)
class EnabledSourceBinding:
    """Retain a redacted hash alongside one validated enabled source."""

    source_id: str
    config: FormularySourceConfig = field(repr=False)
    configuration_hash: str = field(repr=False)
    alternative_correction: AlternativeCorrection | None = field(
        default=None,
        repr=False,
    )
    launch_mode: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        strict_text(self.source_id, "source id", 64)
        if type(self.config) is not FormularySourceConfig:
            raise ValueError("FHIR formulary source binding is invalid")
        strict_hash(self.configuration_hash, "source configuration hash")
        if self.alternative_correction is not None and type(
            self.alternative_correction
        ) is not AlternativeCorrection:
            raise ValueError("FHIR formulary source binding is invalid")
        if self.launch_mode is not None:
            strict_text(self.launch_mode, "source launch mode", 64)

    def __repr__(self) -> str:
        return (
            "EnabledSourceBinding("
            f"source_id={self.source_id!r}, configuration=<redacted>)"
        )


async def _source_row(source_id: str, database: Any) -> dict[str, Any]:
    source_row = await database.first(
        f"SELECT source_id, canonical_base, enabled, runtime_config_json, "
        f"metadata_json FROM {table_name('fhir_formulary_source')} "
        "WHERE source_id = :source_id;",
        source_id=source_id,
    )
    return row_mapping(source_row)


def _configuration_hash(
    source_id: str,
    source_by_field: dict[str, Any],
    metadata_by_field: dict[str, Any],
) -> str:
    configuration_by_field = {
        "canonical_base": source_by_field.get("canonical_base"),
        "enabled": source_by_field.get("enabled"),
        "metadata": metadata_by_field,
        "runtime": source_by_field.get("runtime_config_json"),
        "source_id": source_id,
    }
    digest = hashlib.sha256()
    digest.update(SOURCE_CONFIGURATION_DOMAIN.encode("ascii"))
    digest.update(b"\n")
    digest.update(json_text(configuration_by_field).encode("utf-8"))
    return digest.hexdigest()


def _alternative_correction(
    metadata_by_field: dict[str, Any],
) -> AlternativeCorrection | None:
    if ALTERNATIVE_CORRECTION_METADATA_FIELD not in metadata_by_field:
        return None
    correction_by_field = metadata_by_field[
        ALTERNATIVE_CORRECTION_METADATA_FIELD
    ]
    if type(correction_by_field) is not dict or set(correction_by_field) != {
        "prefix",
        "rule_version",
    }:
        raise ValueError("source correction metadata mismatch")
    return AlternativeCorrection(
        prefix=correction_by_field.get("prefix"),
        rule_version=correction_by_field.get("rule_version"),
    )


def _launch_mode(metadata_by_field: dict[str, Any]) -> str | None:
    if "launch_mode" not in metadata_by_field:
        return None
    raw_launch_mode = metadata_by_field.get("launch_mode")
    launch_mode = strict_text(raw_launch_mode, "source launch mode", 64)
    if launch_mode != LIBRARY_ONLY_LAUNCH_MODE:
        raise ValueError("source launch mode mismatch")
    return launch_mode


def _binding_from_row(
    source_id: str,
    source_by_field: dict[str, Any],
) -> EnabledSourceBinding:
    try:
        if source_by_field.get("source_id") != source_id:
            raise ValueError("source identity mismatch")
        metadata_by_field = source_by_field.get("metadata_json")
        if type(metadata_by_field) is not dict:
            raise ValueError("source metadata mismatch")
        config = enabled_source_config(
            canonical_base=source_by_field.get("canonical_base"),
            enabled=source_by_field.get("enabled"),
            runtime_config_json=source_by_field.get("runtime_config_json"),
        )
        return EnabledSourceBinding(
            source_id=source_id,
            config=config,
            configuration_hash=_configuration_hash(
                source_id,
                source_by_field,
                metadata_by_field,
            ),
            alternative_correction=_alternative_correction(metadata_by_field),
            launch_mode=_launch_mode(metadata_by_field),
        )
    except (TypeError, ValueError, RuntimeError):
        raise FHIRSourceConfigurationError(
            "FHIR formulary source configuration is invalid"
        ) from None


async def load_enabled_source(
    source_id: str,
    *,
    database: Any = db,
) -> EnabledSourceBinding:
    """Load one exact pre-registered source without changing its state."""

    try:
        normalized_source_id = strict_text(source_id, "source id", 64)
    except ValueError:
        raise FHIRSourceConfigurationError(
            "FHIR formulary source configuration is invalid"
        ) from None
    source_by_field = await _source_row(normalized_source_id, database)
    return _binding_from_row(normalized_source_id, source_by_field)


async def require_source_unchanged(
    binding: EnabledSourceBinding,
    *,
    database: Any = db,
) -> None:
    """Fail closed if any bound source configuration field changed."""

    if type(binding) is not EnabledSourceBinding:
        raise FHIRSourceConfigurationError(
            "FHIR formulary source configuration is invalid"
        )
    current_binding = await load_enabled_source(
        binding.source_id,
        database=database,
    )
    if current_binding.configuration_hash != binding.configuration_hash:
        raise FHIRSourceConfigurationError(
            "FHIR formulary source configuration changed during synchronization"
        )


__all__ = (
    "ALTERNATIVE_CORRECTION_METADATA_FIELD",
    "EnabledSourceBinding",
    "LIBRARY_ONLY_LAUNCH_MODE",
    "load_enabled_source",
    "require_source_unchanged",
)
