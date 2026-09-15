# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from uuid import uuid4

import pytest

from process import npi_result_archive as archive
from process import npi_result_generation as generation


def _serving(lineage_id: str, revision: int) -> dict[str, object]:
    return {
        "origin_lineage_id": lineage_id,
        "origin_generation": revision,
        "published_at": "2026-09-14T12:00:00Z",
    }


def _authority_row(**overrides: object) -> dict[str, object]:
    authority_dict = {
        "singleton": True,
        "local_lineage_id": str(uuid4()),
        "local_generation": 4,
        "origin_lineage_id": None,
        "origin_generation": None,
        "published_at": None,
        "relation_oids": None,
        "canonical_publication_ref": None,
        "canonical_publication_generation": None,
        "canonical_chain_ref": None,
        "canonical_import_date": None,
    }
    authority_dict.update(overrides)
    return authority_dict


def test_generation_authority_keeps_legacy_state_explicit() -> None:
    authority = generation.validate_npi_result_generation_authority(_authority_row())

    assert authority.local_generation == 4
    assert authority.serving_generation is None
    assert authority.relation_oids is None
    assert authority.canonical_provenance is None


def test_generation_authority_accepts_complete_tracked_state() -> None:
    lineage_id = str(uuid4())
    authority = generation.validate_npi_result_generation_authority(
        _authority_row(
            origin_lineage_id=lineage_id,
            origin_generation=3,
            published_at=datetime.datetime(2026, 9, 14, 12, tzinfo=datetime.UTC),
            relation_oids=[10, 11, 12, 13, 14, 15],
            canonical_publication_ref="nppub1_" + "a" * 43,
            canonical_publication_generation=2,
            canonical_chain_ref="penpc1_" + "b" * 43,
            canonical_import_date=datetime.date(2026, 9, 14),
        )
    )

    assert authority.serving_generation == generation.NpiServingGeneration(
        lineage_id,
        3,
        datetime.datetime(2026, 9, 14, 12, tzinfo=datetime.UTC),
    )
    assert authority.relation_oids == (10, 11, 12, 13, 14, 15)
    assert authority.canonical_provenance.publication_generation == 2


@pytest.mark.parametrize(
    "overrides",
    [
        {"origin_lineage_id": str(uuid4())},
        {
            "origin_lineage_id": str(uuid4()),
            "origin_generation": 1,
            "published_at": "2026-09-14T12:00:00Z",
            "relation_oids": [10, 11, 12, 13, 14, 14],
        },
        {"canonical_publication_ref": "nppub1_" + "a" * 43},
    ],
)
def test_generation_authority_rejects_partial_or_duplicate_state(overrides) -> None:
    with pytest.raises(RuntimeError, match="authority is invalid"):
        generation.validate_npi_result_generation_authority(_authority_row(**overrides))


def test_automatic_order_requires_strict_same_lineage_revision() -> None:
    lineage_id = str(uuid4())
    generation.require_npi_automatic_generation_order(
        _serving(lineage_id, 2),
        _serving(lineage_id, 1),
    )
    with pytest.raises(ValueError, match="unsupported"):
        generation.require_npi_automatic_generation_order(
            _serving(str(uuid4()), 3),
            _serving(lineage_id, 2),
        )
    with pytest.raises(ValueError, match="unsupported"):
        generation.require_npi_automatic_generation_order(
            _serving(lineage_id, 2),
            _serving(lineage_id, 2),
        )


def test_manifest_does_not_upgrade_legacy_capture_to_tracked() -> None:
    table_receipts = tuple(
        archive.NpiTableReceipt(model.__name__, table_name, "a" * 64, 0)
        for model, table_name in zip(archive._MODEL_TYPES, generation.RELATION_NAMES, strict=True)
    )
    manifest = archive.NpiResultManifest(
        table_receipts,
        {"release": "synthetic"},
        archive._source_metadata({"release": "synthetic"})[1],
        archive._schema_digest(table_receipts),
        "legacy-manual",
        None,
        None,
    )

    assert archive.validate_npi_result_manifest(manifest) == manifest
    modified = manifest.as_dict()
    modified["capture_authority"] = "tracked-generation"
    with pytest.raises(archive.NpiResultArchiveError, match="classification differs"):
        archive.validate_npi_result_manifest(modified)


def test_manifest_digest_accepts_metadata_at_the_source_size_boundary() -> None:
    """A source-accepted payload remains digestible as part of its larger manifest."""

    empty_payload_size = len(archive._canonical_json({"payload": ""}))
    metadata = {"payload": "x" * (archive._MAX_METADATA_BYTES - empty_payload_size)}
    assert len(archive._canonical_json(metadata)) == archive._MAX_METADATA_BYTES
    normalized_metadata, metadata_sha256 = archive._source_metadata(metadata)
    table_receipts = tuple(
        archive.NpiTableReceipt(model.__name__, table_name, "a" * 64, 0)
        for model, table_name in zip(archive._MODEL_TYPES, generation.RELATION_NAMES, strict=True)
    )
    manifest = archive.NpiResultManifest(
        table_receipts,
        normalized_metadata,
        metadata_sha256,
        archive._schema_digest(table_receipts),
        "legacy-manual",
        None,
        None,
    )

    validated = archive.validate_npi_result_manifest(manifest)

    assert len(archive._canonical_json(validated.as_dict())) > archive._MAX_METADATA_BYTES
    assert archive._manifest_digest(validated) == archive._manifest_digest(validated)
    metadata["payload"] += "x"
    with pytest.raises(archive.NpiResultArchiveError, match="metadata is too large"):
        archive._source_metadata(metadata)
