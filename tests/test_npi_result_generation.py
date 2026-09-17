# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from uuid import uuid4

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

from process import npi_result_archive as archive
from process import npi_result_generation as generation


def test_npi_migration_appends_to_the_deployed_hospital_head() -> None:
    script = ScriptDirectory.from_config(Config("alembic.ini"))
    hospital_revision = "20260917100000_hospital_price_csv_v3_label"
    npi_revision = "20260914120000_npi_result_generation"

    assert script.get_heads() == [npi_revision]
    assert script.get_revision(hospital_revision).down_revision == "20260914120000_custom_import_v1_schema"
    assert script.get_revision(npi_revision).down_revision == hospital_revision
    assert [step.revision.revision for step in script._upgrade_revs("head", hospital_revision)] == [npi_revision]


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
    authority_row = _authority_row(
        origin_lineage_id=lineage_id,
        origin_generation=3,
        published_at=datetime.datetime(2026, 9, 14, 12, tzinfo=datetime.UTC),
        relation_oids=[10, 11, 12, 13, 14, 15],
        canonical_publication_ref="nppub1_" + "a" * 43,
        canonical_publication_generation=2,
        canonical_chain_ref="penpc1_" + "b" * 43,
        canonical_import_date=datetime.date(2026, 9, 14),
    )
    authority = generation.validate_npi_result_generation_authority(authority_row)

    assert authority.serving_generation == generation.NpiServingGeneration(
        lineage_id,
        3,
        datetime.datetime(2026, 9, 14, 12, tzinfo=datetime.UTC),
    )
    assert authority.relation_oids == (10, 11, 12, 13, 14, 15)
    assert authority.canonical_provenance.publication_generation == 2
    assert authority.as_dict() == {
        "local_lineage_id": authority_row["local_lineage_id"],
        "local_generation": 4,
        "serving_generation": _serving(lineage_id, 3),
        "relation_oids": [10, 11, 12, 13, 14, 15],
        "canonical_provenance": {
            "publication_ref": "nppub1_" + "a" * 43,
            "publication_generation": 2,
            "chain_ref": "penpc1_" + "b" * 43,
            "import_date": "2026-09-14",
        },
    }


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
        {
            "origin_lineage_id": str(uuid4()),
            "origin_generation": 1,
            "published_at": "2026-09-14T12:00:00Z",
            "relation_oids": [],
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

    missing_field = manifest.as_dict()
    missing_field.pop("schema_sha256")
    with pytest.raises(archive.NpiResultArchiveError, match="manifest is invalid"):
        archive.validate_npi_result_manifest(missing_field)

    unrecognized_authority = manifest.as_dict()
    unrecognized_authority["capture_authority"] = "unrecognized"
    with pytest.raises(archive.NpiResultArchiveError, match="authority classification is invalid"):
        archive.validate_npi_result_manifest(unrecognized_authority)

    tampered_metadata_digest = manifest.as_dict()
    tampered_metadata_digest["source_metadata_sha256"] = "b" * 64
    with pytest.raises(archive.NpiResultArchiveError, match="manifest digest differs"):
        archive.validate_npi_result_manifest(tampered_metadata_digest)


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


@pytest.mark.parametrize(
    "value, message",
    [
        (
            {"origin_lineage_id": "not-a-uuid", "origin_generation": 1, "published_at": "2026-09-14T12:00:00Z"},
            "result generation lineage is invalid",
        ),
        (
            {"origin_lineage_id": str(uuid4()), "origin_generation": 0, "published_at": "2026-09-14T12:00:00Z"},
            "serving generation is invalid",
        ),
        (
            {"origin_lineage_id": str(uuid4()), "origin_generation": 1, "published_at": "not-a-timestamp"},
            "result generation time is invalid",
        ),
        (
            {"origin_lineage_id": str(uuid4()), "origin_generation": 1, "published_at": None},
            "result generation time is invalid",
        ),
        ({}, "serving generation is invalid"),
    ],
)
def test_serving_generation_rejects_nonportable_values(value, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        generation.validate_npi_serving_generation(value)


def test_canonical_provenance_round_trips_and_rejects_nonportable_values() -> None:
    provenance = generation.NpiCanonicalProvenance(
        "nppub1_" + "a" * 43,
        2,
        "penpc1_" + "b" * 43,
        datetime.date(2026, 9, 14),
    )
    assert generation.validate_npi_canonical_provenance(provenance) == provenance

    valid = provenance.as_dict()
    invalid_values = [
        {},
        {**valid, "import_date": "not-a-date"},
        {**valid, "publication_ref": "not-a-publication-reference"},
        {**valid, "publication_generation": generation._MAX_CANONICAL_GENERATION + 1},
    ]
    for value in invalid_values:
        with pytest.raises(ValueError, match="NPI canonical provenance is invalid"):
            generation.validate_npi_canonical_provenance(value)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"singleton": False}, "authority is unavailable"),
        ({"local_generation": -1}, "local result generation is invalid"),
    ],
)
def test_generation_authority_rejects_invalid_singleton_and_local_counter(overrides, message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        generation.validate_npi_result_generation_authority(_authority_row(**overrides))


class _NpiResultRows:
    def __init__(self, rows) -> None:
        self._rows = rows

    def all(self):
        return self._rows


class _NpiResultRowsSession:
    def __init__(self, rows) -> None:
        self._rows = rows

    async def execute(self, _statement, _parameters=None):
        return _NpiResultRows(self._rows)


@pytest.mark.parametrize(
    "rows",
    [
        [(table_name, None) for table_name in generation.RELATION_NAMES],
        [("wrong_relation", 1)]
        + [(table_name, ordinal) for ordinal, table_name in enumerate(generation.RELATION_NAMES[1:], 2)],
    ],
)
async def test_current_relation_oids_fail_closed_for_malformed_catalog_rows(rows) -> None:
    with pytest.raises(RuntimeError, match="serving relations are unavailable"):
        await generation.current_npi_relation_oids(_NpiResultRowsSession(rows), schema_name="mrf")


async def test_capture_serving_generation_requires_current_relation_identity(monkeypatch) -> None:
    lineage_id = str(uuid4())
    authority = generation.validate_npi_result_generation_authority(
        _authority_row(
            origin_lineage_id=lineage_id,
            origin_generation=3,
            published_at="2026-09-14T12:00:00Z",
            relation_oids=[10, 11, 12, 13, 14, 15],
        )
    )

    async def read_authority(_session, *, schema_name: str):
        assert schema_name == "mrf"
        return authority

    async def matching_oids(_session, *, schema_name: str):
        assert schema_name == "mrf"
        return authority.relation_oids

    monkeypatch.setattr(generation, "read_npi_result_generation_authority", read_authority)
    monkeypatch.setattr(generation, "current_npi_relation_oids", matching_oids)
    assert await generation.capture_npi_serving_generation(object(), schema_name="mrf") == authority

    async def drifted_oids(_session, *, schema_name: str):
        assert schema_name == "mrf"
        return (10, 11, 12, 13, 14, 16)

    monkeypatch.setattr(generation, "current_npi_relation_oids", drifted_oids)
    with pytest.raises(RuntimeError, match="unavailable or drifted"):
        await generation.capture_npi_serving_generation(object(), schema_name="mrf")


async def test_stage_mutation_guards_reject_an_incomplete_family_before_ddl() -> None:
    with pytest.raises(ValueError, match="stage family is invalid"):
        await generation.install_npi_stage_mutation_guards(
            object(),
            schema_name="mrf",
            stage_tables=generation.RELATION_NAMES[:-1],
        )


class _TransactionalArchiveSession:
    def in_transaction(self) -> bool:
        return True


async def test_source_capture_requires_exactly_one_metadata_input() -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="exactly one source metadata input"):
        await archive.capture_npi_source(
            _TransactionalArchiveSession(),
            schema_name="mrf",
            source_metadata=None,
        )
