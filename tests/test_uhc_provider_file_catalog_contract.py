# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from process import uhc_provider_file_catalog_types as catalog_types
from process import uhc_provider_file_catalog_contract as catalog_contract
from tests.uhc_provider_file_catalog_test_data import live_catalog_payloads


def _summary_by_collection(catalog):
    return {
        (collection["family"], collection["collection_kind"]): collection
        for collection in catalog.collection_summary
    }


def test_live_catalog_contract_has_exact_102_file_inventory():
    catalog = catalog_types.observed_catalog_from_payloads(live_catalog_payloads())
    summary_by_collection = _summary_by_collection(catalog)

    assert len(catalog.files) == 102
    assert summary_by_collection[("cs", "provider_membership")]["file_count"] == 53
    assert summary_by_collection[("ifp", "provider_membership")]["file_count"] == 25
    assert summary_by_collection[("ifp", "plan_reference")]["file_count"] == 24
    assert summary_by_collection[("cs", "plan_reference")] == {
        "family": "cs",
        "collection_kind": "plan_reference",
        "availability": "not_published_by_source",
        "catalog_support": "not_applicable",
        "file_count": 0,
    }
    catalog_types.validate_observed_catalog(catalog)
    assert catalog_types.CATALOG_CONTRACT_LIMITS == {
        "max_raw_entries_per_collection": 4_096,
        "max_raw_entries_total": 8_192,
        "max_file_name_length": 256,
        "max_size_bytes": (1 << 63) - 1,
    }


@pytest.mark.parametrize(
    "url",
    [
        "http://providermrf.uhc.com/api/files/ui/cs/",
        "https://user@providermrf.uhc.com/api/files/ui/cs/",
        "https://providermrf.uhc.com:444/api/files/ui/cs/",
        "https://providermrf.uhc.com/api/files/ui/cs/?token=secret",
        "https://providermrf.uhc.com/api/files/ui/cs/#fragment",
        "https://providermrf.uhc.com/api/files/ui/cs/\nheader",
        "https://providermrf.uhc.com/api/files/ui/cs/\tpath",
        "https://example.com/api/files/ui/cs/",
    ],
)
def test_catalog_urls_reject_untrusted_or_credential_bearing_forms(url):
    with pytest.raises(catalog_types.UHCFileCatalogError):
        catalog_types.trusted_public_https_url(url)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("date", "2026-07-20T00:00:00"),
        ("name", "JSON_Providers_CS_000\n.json"),
        ("size", -1),
    ],
)
def test_catalog_rejects_noncanonical_entry_identity(field, value):
    payloads_by_family = live_catalog_payloads()
    payloads_by_family["cs"]["providers"][0][field] = value

    with pytest.raises(catalog_types.UHCFileCatalogError):
        catalog_types.observed_catalog_from_payloads(payloads_by_family)


def test_external_url_must_match_safe_basename_without_query():
    payloads_by_family = live_catalog_payloads()
    catalog_entry = payloads_by_family["cs"]["providers"][0]
    catalog_entry.update(
        {
            "isExternal": True,
            "url": "https://legacy.providerlookuponline.com/not-the-file.json",
        }
    )

    with pytest.raises(catalog_types.UHCFileCatalogError, match="basename"):
        catalog_types.observed_catalog_from_payloads(payloads_by_family)


def test_duplicate_semantic_basename_is_rejected():
    payloads_by_family = live_catalog_payloads()
    duplicate_entry_by_field = dict(payloads_by_family["cs"]["providers"][0])
    payloads_by_family["cs"]["providers"].append(duplicate_entry_by_field)

    with pytest.raises(catalog_types.UHCFileCatalogError, match="duplicate"):
        catalog_types.observed_catalog_from_payloads(payloads_by_family)


def test_semantic_hash_excludes_only_local_catalog_support():
    catalog = catalog_types.observed_catalog_from_payloads(live_catalog_payloads())
    changed_summaries = [dict(collection) for collection in catalog.collection_summary]
    changed_summaries[0]["catalog_support"] = "future-local-state"

    assert catalog_types.catalog_set_sha256(
        catalog.files,
        changed_summaries,
    ) == catalog.catalog_set_sha256


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda payload: payload["cs"]["providers"].append("not-an-object"),
            "not an object",
        ),
        (lambda payload: payload["cs"].update({"providers": []}), "no provider"),
        (lambda payload: payload["ifp"].update({"plans": []}), "no plan-reference"),
        (lambda payload: payload.update({"cs": None}), "catalog is invalid"),
        (
            lambda payload: payload["cs"]["providers"][0].update(
                {"isExternal": "yes"}
            ),
            "external marker",
        ),
        (
            lambda payload: payload["cs"]["providers"][0].update(
                {"blobPath": "../bad.json"}
            ),
            "blob path",
        ),
        (
            lambda payload: payload["cs"]["providers"][0].update({"size": True}),
            "file size",
        ),
    ],
)
def test_catalog_parser_rejects_collection_boundary_failures(mutator, message):
    payloads_by_family = live_catalog_payloads()
    mutator(payloads_by_family)

    with pytest.raises(catalog_types.UHCFileCatalogError, match=message):
        catalog_types.observed_catalog_from_payloads(payloads_by_family)


def test_catalog_parser_skips_non_json_and_accepts_bound_external_url():
    payloads_by_family = live_catalog_payloads()
    payloads_by_family["cs"]["providers"].append(
        {"name": "README.txt", "date": "2026-07-20T00:00:00Z"}
    )
    external_entry = payloads_by_family["cs"]["providers"][0]
    external_entry.update(
        {
            "isExternal": True,
            "url": f"https://legacy.providerlookuponline.com/{external_entry['name']}",
        }
    )

    catalog = catalog_types.observed_catalog_from_payloads(payloads_by_family)

    assert len(catalog.files) == 102
    assert catalog.files[0].source_url.startswith(
        "https://legacy.providerlookuponline.com/"
    )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda catalog: object(), "invalid type"),
        (
            lambda catalog: replace(catalog, catalog_set_sha256="f" * 64),
            "semantic catalog hash",
        ),
        (
            lambda catalog: replace(
                catalog,
                files=(
                    replace(catalog.files[0], file_id="f" * 64),
                    *catalog.files[1:],
                ),
            ),
            "file identity is inconsistent",
        ),
        (
            lambda catalog: replace(
                catalog,
                collection_summary=catalog.collection_summary[:-1],
            ),
            "collection set is incomplete",
        ),
        (
            lambda catalog: replace(
                catalog,
                collection_summary=(
                    catalog.collection_summary[0],
                    {**catalog.collection_summary[1], "file_count": 54},
                    *catalog.collection_summary[2:],
                ),
            ),
            "collection counts",
        ),
    ],
)
def test_catalog_validation_recomputes_semantic_boundaries(mutation, message):
    catalog = catalog_types.observed_catalog_from_payloads(live_catalog_payloads())

    with pytest.raises(catalog_types.UHCFileCatalogError, match=message):
        catalog_types.validate_observed_catalog(mutation(catalog))


@pytest.mark.parametrize(
    "url",
    [
        "https://providermrf.uhc.com:invalid/api/files/ui/cs/",
        "https://providermrf.uhc.com:999999/api/files/ui/cs/",
    ],
)
def test_catalog_url_rejects_invalid_port_syntax(url):
    with pytest.raises(catalog_types.UHCFileCatalogError, match="invalid port"):
        catalog_types.trusted_public_https_url(url)


@pytest.mark.parametrize(
    ("entry_changes", "message"),
    [
        ({"date": ""}, "timestamp is missing"),
        ({"date": "not-a-date"}, "timestamp is invalid"),
        ({"size": "12x"}, "file size is invalid"),
    ],
)
def test_catalog_parser_rejects_timestamp_and_size_syntax(entry_changes, message):
    payloads_by_family = live_catalog_payloads()
    payloads_by_family["cs"]["providers"][0].update(entry_changes)

    with pytest.raises(catalog_types.UHCFileCatalogError, match=message):
        catalog_types.observed_catalog_from_payloads(payloads_by_family)


@pytest.mark.parametrize(
    "invalid_size",
    [catalog_types.MAX_SIZE_BYTES + 1, "9" * 20],
)
def test_catalog_parser_rejects_values_outside_postgresql_bigint(invalid_size):
    payloads_by_family = live_catalog_payloads()
    payloads_by_family["cs"]["providers"][0]["size"] = invalid_size

    with pytest.raises(catalog_types.UHCFileCatalogError, match="file size"):
        catalog_types.observed_catalog_from_payloads(payloads_by_family)


def test_catalog_parser_accepts_bigint_maximum():
    assert catalog_types._size_bytes(
        {"size": str(catalog_types.MAX_SIZE_BYTES)}
    ) == catalog_types.MAX_SIZE_BYTES


def test_catalog_basename_matches_varchar_256_boundary():
    accepted_name = "A" + ("x" * 250) + ".json"
    rejected_name = "A" + ("x" * 251) + ".json"
    accepted_entry_by_field = {
        "name": accepted_name,
        "date": "2026-07-20T00:00:00Z",
        "size": 1,
        "blobPath": f"ui/cs/providers/{accepted_name}",
    }
    rejected_entry_by_field = {
        **accepted_entry_by_field,
        "name": rejected_name,
        "blobPath": f"ui/cs/providers/{rejected_name}",
    }

    assert len(accepted_name) == catalog_types.MAX_FILE_NAME_LENGTH
    assert catalog_types._catalog_file(
        "cs",
        catalog_types.PROVIDER_MEMBERSHIP,
        accepted_entry_by_field,
    ).file_name == accepted_name
    with pytest.raises(catalog_types.UHCFileCatalogError, match="basename"):
        catalog_types._catalog_file(
            "cs",
            catalog_types.PROVIDER_MEMBERSHIP,
            rejected_entry_by_field,
        )


def test_raw_entry_limits_accept_boundaries_before_expansion():
    boundary_payloads_by_family = {
        "cs": {"providers": [None] * 4_096},
        "ifp": {
            "providers": [None] * 4_095,
            "plans": [None],
        },
    }

    catalog_types._validate_raw_entry_limits(boundary_payloads_by_family)


def test_raw_entry_limits_reject_collection_and_total_amplification():
    collection_overflow_by_family = {
        "cs": {"providers": [None] * 4_097},
        "ifp": {"providers": [None], "plans": [None]},
    }
    total_overflow_by_family = {
        "cs": {"providers": [None] * 4_096},
        "ifp": {
            "providers": [None] * 4_096,
            "plans": [None],
        },
    }

    with pytest.raises(catalog_types.UHCFileCatalogError, match="entry bound"):
        catalog_types._validate_raw_entry_limits(collection_overflow_by_family)
    with pytest.raises(catalog_types.UHCFileCatalogError, match="total entry bound"):
        catalog_types._validate_raw_entry_limits(total_overflow_by_family)


def test_catalog_parser_supports_missing_or_nested_size_metadata():
    payloads_by_family = live_catalog_payloads()
    without_size = payloads_by_family["cs"]["providers"][0]
    nested_size = payloads_by_family["cs"]["providers"][1]
    without_size.pop("size")
    nested_size.pop("size")
    nested_size["properties"] = {"contentLength": "1234"}

    catalog = catalog_types.observed_catalog_from_payloads(payloads_by_family)
    size_by_name = {
        catalog_file.file_name: catalog_file.size_bytes
        for catalog_file in catalog.files
    }

    assert size_by_name[without_size["name"]] is None
    assert size_by_name[nested_size["name"]] == 1234


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda catalog: replace(
                catalog,
                collection_summary=(None, *catalog.collection_summary[1:]),
            ),
            "summary is invalid",
        ),
        (
            lambda catalog: replace(
                catalog,
                collection_summary=(
                    {**catalog.collection_summary[0], "family": "unknown"},
                    *catalog.collection_summary[1:],
                ),
            ),
            "summary is invalid",
        ),
        (
            lambda catalog: replace(
                catalog,
                collection_summary=(
                    catalog.collection_summary[0],
                    *catalog.collection_summary[1:2],
                    {**catalog.collection_summary[2], "file_count": 0},
                    *catalog.collection_summary[3:],
                ),
            ),
            "required catalog collection is empty",
        ),
        (
            lambda catalog: replace(
                catalog,
                collection_summary=(
                    catalog.collection_summary[0],
                    {**catalog.collection_summary[1], "availability": "wrong"},
                    *catalog.collection_summary[2:],
                ),
            ),
            "support is inconsistent",
        ),
        (
            lambda catalog: replace(
                catalog,
                files=(
                    replace(catalog.files[0], size_bytes=True),
                    *catalog.files[1:],
                ),
            ),
            "file identity is invalid",
        ),
        (
            lambda catalog: replace(
                catalog,
                files=(
                    replace(
                        catalog.files[0],
                        size_bytes=catalog_types.MAX_SIZE_BYTES + 1,
                    ),
                    *catalog.files[1:],
                ),
            ),
            "file identity is invalid",
        ),
        (
            lambda catalog: replace(
                catalog,
                files=(
                    replace(
                        catalog.files[0],
                        catalog_modified_at="2026-07-20T02:00:00+02:00",
                    ),
                    *catalog.files[1:],
                ),
            ),
            "timestamp is not canonical",
        ),
    ],
)
def test_catalog_validation_rejects_malformed_persisted_semantics(mutation, message):
    catalog = catalog_types.observed_catalog_from_payloads(live_catalog_payloads())

    with pytest.raises(catalog_types.UHCFileCatalogError, match=message):
        catalog_types.validate_observed_catalog(mutation(catalog))


def test_catalog_parser_rejects_non_list_collection_and_file_id_collision(monkeypatch):
    payloads_by_family = live_catalog_payloads()
    payloads_by_family["ifp"]["plans"] = {}
    with pytest.raises(catalog_types.UHCFileCatalogError, match="collection is invalid"):
        catalog_types.observed_catalog_from_payloads(payloads_by_family)

    monkeypatch.setattr(catalog_contract, "sha256_text", lambda _value: "f" * 64)
    with pytest.raises(catalog_types.UHCFileCatalogError, match="identity collision"):
        catalog_types.observed_catalog_from_payloads(live_catalog_payloads())


def test_catalog_url_canonicalizes_the_explicit_tls_port_and_host_case():
    explicit_tls_url = "https://providermrf.uhc.com:443/api/files/ui/cs/"

    assert catalog_types.trusted_public_https_url(explicit_tls_url) == explicit_tls_url
    with pytest.raises(catalog_types.UHCFileCatalogError, match="not canonical"):
        catalog_types.trusted_public_https_url(
            "https://ProviderMRF.uhc.com/api/files/ui/cs/"
        )


def test_collection_parser_rechecks_shape_and_expansion_limits(monkeypatch):
    with pytest.raises(catalog_types.UHCFileCatalogError, match="collection is invalid"):
        catalog_types._catalog_collection(
            "ifp",
            "plans",
            catalog_types.PLAN_REFERENCE,
            {"plans": {}},
            set(),
        )
    with pytest.raises(catalog_types.UHCFileCatalogError, match="entry bound"):
        catalog_types._catalog_collection(
            "ifp",
            "plans",
            catalog_types.PLAN_REFERENCE,
            {"plans": [None] * (catalog_types.MAX_RAW_ENTRIES_PER_COLLECTION + 1)},
            set(),
        )

    monkeypatch.setattr(catalog_types, "_validate_raw_entry_limits", lambda _payloads: None)
    malformed_payloads_by_family = live_catalog_payloads()
    malformed_payloads_by_family["cs"] = None
    with pytest.raises(catalog_types.UHCFileCatalogError, match="catalog is invalid"):
        catalog_types.observed_catalog_from_payloads(malformed_payloads_by_family)
