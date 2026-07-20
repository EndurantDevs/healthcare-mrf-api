# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt

import pytest

from process import uhc_provider_file_catalog as catalog_module
from process import uhc_provider_file_catalog_store as catalog_store
from process import uhc_provider_file_catalog_types as catalog_types
from tests.test_uhc_provider_file_catalog_store import _live_payloads


class _CatalogDB:
    def __init__(self, catalog, proof_by_field):
        observed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        self.observation_fields = {
            "catalog_set_sha256": catalog.catalog_set_sha256,
            "schema_version": 1,
            "families_json": sorted(catalog_types.CATALOG_URLS),
            "collection_summary_json": catalog_store._source_collection_summary(
                catalog
            ),
            **catalog_store._catalog_counts(catalog),
            "raw_set_sha256": proof_by_field["raw_set_sha256"],
            "raw_documents_json": proof_by_field["documents"],
            "observation_first_observed_at": observed_at,
            "observation_last_observed_at": observed_at,
        }
        self.file_records = catalog_store._file_parameters(catalog)
        self.first_parameters = []

    async def first(self, _statement, **parameters):
        self.first_parameters.append(parameters)
        return self.observation_fields

    async def all(self, _statement, **_parameters):
        return self.file_records


def _assert_catalog_only_inventory(catalog_document):
    summary_by_collection = {
        (collection["family"], collection["collection_kind"]): collection
        for collection in catalog_document["collections"]
    }
    assert catalog_document["catalog_file_count"] == 102
    assert catalog_document["provider_membership_file_count"] == 78
    assert catalog_document["plan_reference_file_count"] == 24
    assert catalog_document["catalog_contract"] == catalog_types.CATALOG_CONTRACT
    assert catalog_document["catalog_limits"] == catalog_types.CATALOG_CONTRACT_LIMITS
    assert summary_by_collection[("cs", "provider_membership")]["file_count"] == 53
    assert summary_by_collection[("cs", "plan_reference")]["file_count"] == 0
    assert summary_by_collection[("ifp", "provider_membership")]["file_count"] == 25
    assert summary_by_collection[("ifp", "plan_reference")]["file_count"] == 24
    for capability_flag in (
        "acquisition_runnable",
        "provider_directory_current",
        "fhir_publication_ready",
        "reference_aware_gc_ready",
    ):
        assert catalog_document[capability_flag] is False


@pytest.mark.asyncio
async def test_read_model_is_explicitly_catalog_only_with_ui_inventory(monkeypatch):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    proof_by_field = {"raw_set_sha256": "b" * 64, "documents": []}
    catalog_database = _CatalogDB(catalog, proof_by_field)
    monkeypatch.setattr(catalog_module, "db", catalog_database)
    monkeypatch.setattr(
        catalog_module,
        "validate_retained_catalog_proof",
        lambda raw_proof: (raw_proof, catalog),
    )

    catalog_document = await catalog_module.uhc_provider_file_catalog(
        catalog_set_sha256=catalog.catalog_set_sha256
    )

    _assert_catalog_only_inventory(catalog_document)
    first_file = catalog_document["items"][0]
    assert len(first_file["catalog_entry_sha256"]) == 64
    assert "source_payload_sha256" not in first_file
    assert first_file["latest_file_outcome"] is None
    assert first_file["data_file_raw_sha256"] is None
    assert first_file["imported"] is False
    assert "actions" not in catalog_document
    assert "latest_root" not in catalog_document
    assert "actions" not in first_file
    assert catalog_database.first_parameters == [
        {"catalog_set_sha256": catalog.catalog_set_sha256}
    ]


@pytest.mark.asyncio
async def test_empty_and_unknown_historical_catalogs_are_distinct(monkeypatch):
    class _EmptyDB:
        async def first(self, _statement, **_parameters):
            return None

    monkeypatch.setattr(catalog_module, "db", _EmptyDB())
    empty_catalog = await catalog_module.uhc_provider_file_catalog()
    assert empty_catalog["catalog_observed"] is False
    assert empty_catalog["acquisition_runnable"] is False
    with pytest.raises(catalog_types.UHCFileCatalogNotFound):
        await catalog_module.uhc_provider_file_catalog(raw_set_sha256="c" * 64)


@pytest.mark.asyncio
async def test_hash_selectors_are_strict_and_mutually_exclusive():
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        await catalog_module.uhc_provider_file_catalog(catalog_set_sha256="ABC")
    with pytest.raises(ValueError, match="mutually exclusive"):
        await catalog_module.uhc_provider_file_catalog(
            catalog_set_sha256="a" * 64,
            raw_set_sha256="b" * 64,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("persisted_value", "message"),
    [("{bad", "JSON"), ({}, "collection summary")],
)
async def test_read_model_rejects_invalid_persisted_collection_summary(
    monkeypatch,
    persisted_value,
    message,
):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    catalog_database = _CatalogDB(
        catalog,
        {"raw_set_sha256": "b" * 64, "documents": []},
    )
    catalog_database.observation_fields["collection_summary_json"] = persisted_value
    monkeypatch.setattr(catalog_module, "db", catalog_database)

    with pytest.raises(catalog_types.UHCFileCatalogError, match=message):
        await catalog_module.uhc_provider_file_catalog()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_summary_member", [None, 17])
async def test_read_model_rejects_scalar_collection_summary_members(
    monkeypatch,
    invalid_summary_member,
):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    catalog_database = _CatalogDB(
        catalog,
        {"raw_set_sha256": "b" * 64, "documents": []},
    )
    catalog_database.observation_fields["collection_summary_json"] = [
        *catalog_database.observation_fields["collection_summary_json"],
        invalid_summary_member,
    ]
    monkeypatch.setattr(catalog_module, "db", catalog_database)

    with pytest.raises(catalog_types.UHCFileCatalogError, match="collection summary"):
        await catalog_module.uhc_provider_file_catalog()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_file_count", [True, "1", -1, None, 1.0])
async def test_read_model_rejects_malformed_collection_file_count(
    monkeypatch,
    invalid_file_count,
):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    catalog_database = _CatalogDB(
        catalog,
        {"raw_set_sha256": "b" * 64, "documents": []},
    )
    catalog_database.observation_fields["collection_summary_json"][0][
        "file_count"
    ] = invalid_file_count
    monkeypatch.setattr(catalog_module, "db", catalog_database)

    with pytest.raises(catalog_types.UHCFileCatalogError, match="collection summary"):
        await catalog_module.uhc_provider_file_catalog()


def test_persisted_json_recursion_fails_as_catalog_error(monkeypatch):
    monkeypatch.setattr(
        catalog_module.json,
        "loads",
        lambda _value: (_ for _ in ()).throw(RecursionError("too deep")),
    )

    with pytest.raises(catalog_types.UHCFileCatalogError, match="persisted catalog JSON"):
        catalog_module._json_value("[]")


@pytest.mark.asyncio
async def test_read_model_rejects_persisted_family_drift(monkeypatch):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    catalog_database = _CatalogDB(catalog, {"raw_set_sha256": "b" * 64, "documents": []})
    catalog_database.observation_fields["families_json"] = ["cs", "wrong"]
    monkeypatch.setattr(catalog_module, "db", catalog_database)

    with pytest.raises(catalog_types.UHCFileCatalogError, match="families"):
        await catalog_module.uhc_provider_file_catalog()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [("availability", "wrong"), ("catalog_support", "wrong")],
)
async def test_read_model_rejects_persisted_file_support_drift(
    monkeypatch,
    field_name,
    invalid_value,
):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    catalog_database = _CatalogDB(catalog, {"raw_set_sha256": "b" * 64, "documents": []})
    catalog_database.file_records[0][field_name] = invalid_value
    monkeypatch.setattr(catalog_module, "db", catalog_database)

    with pytest.raises(catalog_types.UHCFileCatalogError, match="file set"):
        await catalog_module.uhc_provider_file_catalog()


@pytest.mark.asyncio
async def test_read_model_rejects_schema_version_drift(monkeypatch):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    catalog_database = _CatalogDB(catalog, {"raw_set_sha256": "b" * 64, "documents": []})
    catalog_database.observation_fields["schema_version"] = 2
    monkeypatch.setattr(catalog_module, "db", catalog_database)

    with pytest.raises(catalog_types.UHCFileCatalogError, match="schema version"):
        await catalog_module.uhc_provider_file_catalog()


@pytest.mark.asyncio
async def test_read_model_rejects_persisted_count_drift(monkeypatch):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    catalog_database = _CatalogDB(catalog, {"raw_set_sha256": "b" * 64, "documents": []})
    catalog_database.observation_fields["file_count"] += 1
    monkeypatch.setattr(catalog_module, "db", catalog_database)

    with pytest.raises(catalog_types.UHCFileCatalogError, match="counts"):
        await catalog_module.uhc_provider_file_catalog()


def test_catalog_time_helpers_are_utc_and_fail_closed(monkeypatch):
    aware = dt.datetime(2026, 7, 20, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    assert catalog_module._iso_utc(aware) == "2026-07-19T22:00:00Z"
    assert catalog_module._iso_utc("not-a-date") is None
    assert catalog_module._is_stale("not-a-date", 60) is True
    assert catalog_module._is_stale(dt.datetime.now(dt.UTC), 60) is False

    for invalid_value in ("invalid", "0"):
        monkeypatch.setenv("HLTHPRT_UHC_CATALOG_STALE_SECONDS", invalid_value)
        with pytest.raises(catalog_types.UHCFileCatalogError, match="staleness"):
            catalog_module._stale_seconds()
