# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed tests for retained formulary artifact contracts."""

from __future__ import annotations

from dataclasses import replace
import datetime as dt
import io

import pytest

import process.formulary_fhir.source_artifact_contract as artifact_contract
import process.formulary_fhir.uhc_drug_normalization as normalization
import process.formulary_fhir.uhc_drug_parser_contract as parser_contract
import process.formulary_fhir.uhc_drug_payload as payload
import process.uhc_drug_file_catalog as drug_catalog
import process.uhc_provider_file_catalog as provider_catalog
from process.uhc_provider_file_catalog_contract import UHCFileCatalogError
from tests.uhc_drug_parser_test_support import artifact_set
from tests.uhc_drug_parser_test_support import source_record
from tests.uhc_provider_file_catalog_test_data import live_catalog_payloads


def _observed_drug_catalog():
    return drug_catalog.observed_drug_catalog_from_payloads(
        live_catalog_payloads(),
        source_raw_set_sha256="a" * 64,
    )


def test_artifact_contract_rejects_invalid_scalar_and_content_claims() -> None:
    artifacts, _bodies = artifact_set()
    identity = artifacts.artifacts[0].identity
    verified = artifacts.artifacts[0]

    with pytest.raises(ValueError, match="expected byte count"):
        replace(identity, expected_byte_count=0)
    with pytest.raises(ValueError, match="identity"):
        artifact_contract.VerifiedSourceArtifact(
            identity=object(),
            artifact_sha256="a" * 64,
            artifact_byte_count=1,
            verified_at=dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
        )
    with pytest.raises(ValueError, match="byte count"):
        replace(verified, artifact_byte_count=verified.artifact_byte_count + 1)


def test_identity_set_rejects_empty_invalid_cross_scope_and_collisions() -> None:
    artifacts, _bodies = artifact_set()
    first = artifacts.artifacts[0].identity
    second = artifacts.artifacts[1].identity

    invalid_sets = (
        [],
        (object(),),
        (first, replace(second, source_id="different-source")),
        (first, replace(second, source_file_id=first.source_file_id)),
        (
            first,
            replace(
                second,
                family=first.family,
                file_name=first.file_name,
            ),
        ),
    )
    for invalid_set in invalid_sets:
        with pytest.raises(ValueError):
            artifact_contract.validated_identity_set(invalid_set)


def test_verified_artifact_set_rejects_empty_invalid_and_reordered_sets() -> None:
    artifacts, _bodies = artifact_set()

    for invalid_artifacts in ((), (object(),)):
        with pytest.raises(ValueError):
            artifact_contract.artifact_set_sha256(invalid_artifacts)
    with pytest.raises(ValueError, match="empty"):
        replace(artifacts, artifacts=())

    reversed_artifacts = tuple(reversed(artifacts.artifacts))
    with pytest.raises(ValueError, match="inconsistent"):
        replace(
            artifacts,
            artifacts=reversed_artifacts,
            artifact_set_sha256=artifact_contract.artifact_set_sha256(
                reversed_artifacts
            ),
        )


def test_plan_contract_rejects_invalid_alias_components() -> None:
    valid_alias = parser_contract.uhc_drug_plan_alias(
        "cs",
        "HIOS",
        "PLAN-1",
        2026,
    )
    invalid_calls = (
        ("other", "HIOS", "PLAN-1", 2026),
        ("cs", "invalid type", "PLAN-1", 2026),
        ("cs", "HIOS", "PLAN-1", 1999),
        ("cs", "HIOS", " padded ", 2026),
    )
    for arguments in invalid_calls:
        with pytest.raises(ValueError):
            parser_contract.uhc_drug_plan_alias(*arguments)

    invalid_keys = (
        dict(family="other", plan_id_type="HIOS", plan_year=2026),
        dict(family="cs", plan_id_type="invalid type", plan_year=2026),
        dict(family="cs", plan_id_type="HIOS", plan_year=1999),
        dict(family="cs", plan_id_type="HIOS", plan_year=2026),
    )
    for index, changed_values in enumerate(invalid_keys):
        with pytest.raises(ValueError):
            parser_contract.UHCDrugPlanKey(
                plan_id="PLAN-1",
                source_plan_identifier=(
                    valid_alias if index < 3 else valid_alias + "x"
                ),
                **changed_values,
            )


def test_spool_contract_rejects_counts_and_accepts_unknown_update_time() -> None:
    artifacts, _bodies = artifact_set()
    valid = parser_contract.UHCDrugSpoolEvidence(
        source_id=artifacts.source_id,
        source_file_set_sha256=artifacts.source_file_set_sha256,
        artifact_set_sha256=artifacts.artifact_set_sha256,
        spool_content_sha256="c" * 64,
        file_count=48,
        raw_record_count=48,
        raw_plan_entry_count=48,
        plan_count=2,
        medication_membership_count=2,
        duplicate_count=0,
        superseded_count=0,
        max_last_updated_at=None,
    )
    assert valid.max_last_updated_at is None
    assert "UHCDrugSpoolEvidence" in repr(valid)

    with pytest.raises(ValueError, match="counts"):
        replace(valid, raw_record_count=-1)
    with pytest.raises(ValueError, match="census"):
        replace(valid, file_count=47)
    with pytest.raises(ValueError, match="materialization"):
        parser_contract.UHCDrugPlanMaterialization(
            key=object(),
            coverage_plan=object(),
            medications=(),
        )


def test_payload_private_boundaries_reject_malformed_event_streams(
    monkeypatch,
) -> None:
    with pytest.raises(payload.UHCDrugPayloadError, match="nesting"):
        payload._close_map([None])
    with pytest.raises(payload.UHCDrugPayloadError, match="nesting"):
        payload._close_array([set()])
    with pytest.raises(payload.UHCDrugPayloadError, match="scalar"):
        payload._json_event_bytes("string", object())

    cancellation_events: list[str] = []

    def cancel() -> None:
        cancellation_events.append("cancelled")
        raise RuntimeError("cancelled")

    many_events = iter(
        [("start_map", None)] + [("number", 1)] * 1_023
    )
    with pytest.raises(RuntimeError, match="cancelled"):
        payload._object_array_item_count(many_events, cancel)
    assert cancellation_events == ["cancelled"]

    monkeypatch.setattr(payload, "MAX_JSON_NESTING_DEPTH", 2)
    with pytest.raises(payload.UHCDrugPayloadError, match="nesting"):
        payload._object_array_item_count(
            iter(
                [
                    ("start_map", None),
                    ("start_array", None),
                    ("start_array", None),
                ]
            ),
            None,
        )
    with pytest.raises(payload.UHCDrugPayloadError, match="incomplete"):
        payload._object_array_item_count(iter([("start_map", None)]), None)


def test_payload_rejects_trailing_root_value(monkeypatch) -> None:
    monkeypatch.setattr(
        payload.ijson,
        "basic_parse",
        lambda *_arguments, **_keywords: iter(
            [
                ("start_array", None),
                ("end_array", None),
                ("start_array", None),
            ]
        ),
    )
    with pytest.raises(payload.UHCDrugPayloadError):
        payload.count_uhc_drug_stream_items(io.BytesIO(b"[]"))


@pytest.mark.parametrize(
    "timestamp",
    [
        "not-a-date",
        "2026-08-10T00:00:00",
        "2026-08-10T01:00:00+01:00",
    ],
)
def test_normalization_rejects_noncanonical_catalog_timestamps(timestamp) -> None:
    with pytest.raises(normalization.UHCDrugNormalizationError):
        normalization._catalog_timestamp(timestamp)


def test_normalization_accepts_date_and_rejects_invalid_record_timestamps() -> None:
    fallback = dt.datetime(2026, 8, 10, tzinfo=dt.UTC)
    timestamp, basis = normalization._record_timestamp("2026-08-09", fallback)
    assert (timestamp, basis) == (
        dt.datetime(2026, 8, 9, tzinfo=dt.UTC),
        "record.last_updated_on",
    )
    timestamp, basis = normalization._record_timestamp(
        "2026-08-09T12:00:00Z",
        fallback,
    )
    assert (timestamp, basis) == (
        dt.datetime(2026, 8, 9, 12, tzinfo=dt.UTC),
        "record.last_updated_on",
    )
    for raw_timestamp in (
        "2026-02-30",
        "2026-08-10T00:00:00",
    ):
        with pytest.raises(normalization.UHCDrugNormalizationError):
            normalization._record_timestamp(raw_timestamp, fallback)


def test_normalization_bounds_unknown_extension_shape_and_bytes(
    monkeypatch,
) -> None:
    normalized = normalization._json_extension(
        {"known": 1, "unknown": [{"nested": True}]},
        frozenset({"known"}),
    )
    assert normalized == {"unknown": [{"nested": True}]}

    monkeypatch.setattr(normalization, "MAX_EXTENSION_JSON_NODES", 1)
    with pytest.raises(normalization.UHCDrugNormalizationError, match="too large"):
        normalization._json_extension(
            {"unknown": [{"nested": True}]},
            frozenset(),
        )
    monkeypatch.setattr(normalization, "MAX_EXTENSION_JSON_NODES", 100)
    monkeypatch.setattr(normalization, "MAX_EXTENSION_JSON_BYTES", 1)
    with pytest.raises(normalization.UHCDrugNormalizationError, match="too large"):
        normalization._json_extension({"unknown": "value"}, frozenset())


def test_normalization_rejects_record_and_plan_field_census() -> None:
    artifacts, _bodies = artifact_set()
    artifact = artifacts.artifacts[0]
    with pytest.raises(normalization.UHCDrugNormalizationError, match="record fields"):
        normalization._validated_record(object(), artifact)

    oversized_record = source_record()
    oversized_record.update(
        {
            f"extra-{index}": index
            for index in range(normalization.MAX_RECORD_FIELDS + 1)
        }
    )
    with pytest.raises(normalization.UHCDrugNormalizationError, match="record fields"):
        normalization._validated_record(oversized_record, artifact)

    with pytest.raises(normalization.UHCDrugNormalizationError, match="plan fields"):
        normalization._validated_plan({})


def test_drug_catalog_rejects_transport_identity_boundaries() -> None:
    entry = live_catalog_payloads()["cs"]["drugs"][0]
    file_name = entry["name"]
    with pytest.raises(UHCFileCatalogError, match="external marker"):
        drug_catalog._drug_source_url(
            "cs",
            file_name,
            {**entry, "isExternal": 1},
        )
    with pytest.raises(UHCFileCatalogError, match="basename"):
        drug_catalog._drug_source_url(
            "cs",
            file_name,
            {
                **entry,
                "isExternal": True,
                "url": (
                    "https://legacy.providerlookuponline.com/different.json"
                ),
            },
        )
    with pytest.raises(UHCFileCatalogError, match="basename"):
        drug_catalog._catalog_file_from_entry(
            "cs",
            {**entry, "name": "../unsafe.json"},
        )
    with pytest.raises(UHCFileCatalogError, match="catalog is invalid"):
        drug_catalog._catalog_files_from_payload("cs", object())


def test_drug_catalog_revalidates_each_file_and_aggregate_boundary() -> None:
    catalog = _observed_drug_catalog()
    catalog_file = catalog.files[0]

    invalid_files = (
        object(),
        replace(
            catalog_file,
            source_url=(
                "https://legacy.providerlookuponline.com/different.json"
            ),
        ),
        replace(catalog_file, catalog_entry_sha256="0" * 64),
    )
    for invalid_file in invalid_files:
        with pytest.raises(UHCFileCatalogError):
            drug_catalog._validate_drug_file(invalid_file)

    invalid_catalogs = (
        object(),
        replace(catalog, files=catalog.files[:-1]),
        replace(catalog, files=(catalog.files[0],) + catalog.files[1:-1] + (catalog.files[0],)),
        replace(catalog, collection_summary=()),
        replace(catalog, source_raw_set_sha256="not-a-hash"),
        replace(catalog, drug_set_sha256="0" * 64),
    )
    for invalid_catalog in invalid_catalogs:
        with pytest.raises(UHCFileCatalogError):
            drug_catalog.validate_observed_drug_catalog(invalid_catalog)

    with pytest.raises(UHCFileCatalogError, match="listing projection"):
        drug_catalog._raw_listing_projection_sha256(
            {
                "cs": {"drugs": object()},
                "ifp": {"drugs": []},
            }
        )


def test_drug_catalog_detects_family_and_cross_collection_identity_collisions(
    monkeypatch,
) -> None:
    payloads_by_family = live_catalog_payloads()
    with pytest.raises(UHCFileCatalogError, match="family set"):
        drug_catalog.observed_drug_catalog_from_payloads(
            {"cs": payloads_by_family["cs"]},
            source_raw_set_sha256="a" * 64,
        )

    first_file = _observed_drug_catalog().files[0]
    monkeypatch.setattr(
        drug_catalog,
        "_catalog_files_from_payload",
        lambda *_arguments: (first_file,),
    )
    with pytest.raises(UHCFileCatalogError, match="collision"):
        drug_catalog.observed_drug_catalog_from_payloads(
            payloads_by_family,
            source_raw_set_sha256="a" * 64,
        )


@pytest.mark.asyncio
async def test_retained_catalog_proof_rejects_missing_and_semantic_drift(
    monkeypatch,
) -> None:
    async def missing_observation(*_arguments, **_keywords):
        return {}

    monkeypatch.setattr(provider_catalog, "_selected_observation", missing_observation)
    with pytest.raises(UHCFileCatalogError, match="not found"):
        await provider_catalog.load_retained_uhc_catalog_proof(database=object())

    async def observation(*_arguments, **_keywords):
        return {"catalog_set_sha256": "a" * 64}

    async def file_records(*_arguments, **_keywords):
        return ()

    async def raw_proof(*_arguments, **_keywords):
        return {"raw_set_sha256": "b" * 64}, "retained"

    monkeypatch.setattr(provider_catalog, "_selected_observation", observation)
    monkeypatch.setattr(provider_catalog, "_catalog_file_records", file_records)
    monkeypatch.setattr(
        provider_catalog,
        "_validated_persisted_catalog",
        lambda *_arguments: ("persisted", {}),
    )
    monkeypatch.setattr(
        provider_catalog,
        "_validated_persisted_raw_proof",
        raw_proof,
    )
    with pytest.raises(UHCFileCatalogError, match="persisted semantics"):
        await provider_catalog.load_retained_uhc_catalog_proof(database=object())

    monkeypatch.setattr(
        provider_catalog,
        "_validated_persisted_catalog",
        lambda *_arguments: ("retained", {}),
    )
    loaded = await provider_catalog.load_retained_uhc_catalog_proof(
        database=object()
    )
    assert loaded == {"raw_set_sha256": "b" * 64}
