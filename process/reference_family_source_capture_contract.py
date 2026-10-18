# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact portable reference capture contracts, including the fixed Geo family."""

from collections.abc import Mapping
from typing import Any


def _validate_source_capture_contract(manifest_value: Mapping[str, Any]) -> None:
    """Recognize guarded non-Label capture without upgrading legacy provenance."""
    from process.reference_family_archive import (
        CAPTURED_TIGER_CONTRACT,
        CAPTURED_TIGER_SNAPSHOT_CONTRACT,
        GUARDED_SOURCE_CAPTURE_CONTRACT,
        IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT,
        ReferenceFamilyArchiveError,
    )

    if manifest_value["source_capture_contract"] == IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT:
        if (
            manifest_value.get("importer_id") not in {"nucc", "cms-doctors", "geo"}
            or manifest_value.get("publication_authority") != "tracked-generation"
            or (manifest_value.get("importer_id") == "cms-doctors" and len(manifest_value.get("tables", ())) != 3)
            or (manifest_value.get("importer_id") == "geo" and len(manifest_value.get("tables", ())) != 1)
        ):
            raise ReferenceFamilyArchiveError("immutable capture requires a supported sealed family")
    elif manifest_value["source_capture_contract"] in {CAPTURED_TIGER_CONTRACT, CAPTURED_TIGER_SNAPSHOT_CONTRACT}:
        from process.tiger_captured_epoch import validate_captured_origin

        if (
            manifest_value.get("publication_authority") != "captured-epoch"
            or manifest_value.get("importer_id") != "tiger"
        ):
            raise ReferenceFamilyArchiveError("captured epoch requires the TIGER family")
        origin = validate_captured_origin(manifest_value.get("source_metadata"))
        expected = (
            "tiger.captured-origin.v2"
            if manifest_value["source_capture_contract"] == CAPTURED_TIGER_SNAPSHOT_CONTRACT
            else "tiger.captured-origin.v1"
        )
        if origin["contract"] != expected:
            raise ReferenceFamilyArchiveError("captured origin and snapshot contract differ")
    elif (
        manifest_value["source_capture_contract"] != GUARDED_SOURCE_CAPTURE_CONTRACT
        or manifest_value.get("publication_authority") != "tracked-generation"
        or manifest_value.get("importer_id") == "label"
    ):
        raise ReferenceFamilyArchiveError("reference family source capture contract is invalid")
