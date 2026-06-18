# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import types

import pytest

discovery = importlib.import_module("process.mrf_source_discovery")


def test_source_urls_are_loaded_from_registry_file():
    config = discovery._source_config()

    assert config["default_providers"] == ["master-list"]
    assert list(config["providers"]) == ["master-list"]
    assert config["providers"]["master-list"]["path"] == "specs/mrf_payer_master_list.md"
    assert config["platform_resolvers"]["aetna_health1"]["type"] == "healthsparq_public_mrf"
    assert config["platform_resolvers"]["healthsparq"]["type"] == "healthsparq_public_mrf"
    assert config["platform_resolvers"]["highmark_hmhs"]["type"] == "highmark_hmhs_script"
    assert config["platform_resolvers"]["sapphire"]["type"] == "sapphire_html_tocs"
    assert config["platform_resolvers"]["uhc_public_blobs"]["type"] == "uhc_blob_listing"
    assert config["platform_resolvers"]["bcbsma_monthly_tocs"]["type"] == "bcbsma_monthly_tocs"
    assert config["platform_resolvers"]["cigna_static_mrf_lookup"]["type"] == "cigna_static_mrf_lookup"
    assert config["platform_resolvers"]["aetna_health1"]["tenant_overrides"]["MERITAIN_I"] == "aetnacvs"


def test_parse_master_list_preserves_payers_and_urls():
    markdown = """
## A. National parents + subsidiaries
| Payer | Public MRF TOC / landing URL | Notes |
|---|---|---|
| **Cigna** (The Cigna Group) | https://www.cigna.com/legal/compliance/machine-readable-files | public compliance page |
| **Humana** | https://developers.humana.com/cost-transparency | public developer page |

## B. BCBS independent licensees
| Payer | Public MRF TOC / landing URL | Notes |
|---|---|---|
| BCBS Alabama | https://www.bcbsal.org/web/tcr | public transparency page |
"""

    candidates = discovery.parse_master_list(markdown)

    assert [item.payer_name for item in candidates] == ["Cigna", "Humana", "BCBS Alabama"]
    assert candidates[0].parent_group == "Cigna"
    assert candidates[2].entity_type == "blue"
    assert candidates[2].hosting_platform == "custom"
    assert candidates[0].source_coverage == ()
    assert candidates[0].raw_payload["notes"] == "public compliance page"


def test_parse_master_list_preserves_tpa_hint_and_multiple_urls():
    markdown = """
## A. National parents + subsidiaries
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| Meritain Health (TPA) | tpa | https://health1.aetna.com/app/public/#/one/insurerCode=MERITAIN_I&brandCode=MERITAINOVER/ | Aetna Health1 resolver |

## C. Regional, provider-sponsored, Medicaid-MCO, DTC & TPA payers
| Payer | Type | Public TOC / landing URL | Notes |
|---|---|---|---|
| Collective Health | TPA | https://transparency-in-coverage.collectivehealth.com/index.html · https://transparency-in-coverage.collectivehealth.com/ | public indexes |
"""

    candidates = discovery.parse_master_list(markdown)

    assert candidates[0].payer_name == "Meritain Health"
    assert candidates[0].entity_type == "tpa"
    assert candidates[0].index_url.endswith("MERITAINOVER/")
    collective = [item for item in candidates if item.payer_name == "Collective Health"]
    assert len(collective) == 2
    assert {item.entity_type for item in collective} == {"tpa"}


def test_parse_master_list_skips_placeholder_source_urls():
    markdown = """
## A. National parents + subsidiaries
| Payer | Public MRF TOC / landing URL | Notes |
|---|---|---|
| **HCSC** | per-plan `https://www.bcbs{il,tx,nm,ok,mt}.com/asomrf?EIN=...` | expand into state-specific rows |
"""

    [candidate] = discovery.parse_master_list(markdown)

    assert candidate.index_url is None
    assert candidate.status == "needs_review"
    assert candidate.access_model == "unknown"


def test_master_list_importable_source_filter_keeps_only_working_url_rows():
    assert discovery._candidate_is_importable_source(
        discovery.SourceCandidate(payer_name="Active", provider="master-list", index_url="https://example.com/index.json", status="active")
    )
    assert discovery._candidate_is_importable_source(
        discovery.SourceCandidate(payer_name="Stale", provider="master-list", index_url="https://example.com/stale.json", status="stale")
    )
    assert not discovery._candidate_is_importable_source(
        discovery.SourceCandidate(payer_name="Needs Review", provider="master-list", status="needs_review")
    )
    assert not discovery._candidate_is_importable_source(
        discovery.SourceCandidate(payer_name="Unsupported", provider="master-list", index_url="https://example.com/old", status="unsupported")
    )
    assert not discovery._candidate_is_importable_source(
        discovery.SourceCandidate(payer_name="Archived", provider="master-list", index_url="https://example.com/archive", status="archived")
    )


def test_import_control_snapshot_company_fallback_from_index_url():
    assert (
        discovery._company_name_from_index_url(
            "https://transparency-in-coverage.uhc.com/2026-06-01_Heartland-Dental-LLC_index.json"
        )
        == "Heartland Dental LLC"
    )
    enriched = discovery._apply_company_fallback(
        [{"plan_id": "010854205", "plan_market_type": "group", "plan_name": "POS-CHOICE-PLUS"}],
        "Heartland Dental LLC",
    )

    assert enriched[0]["plan_sponsor_name"] == "Heartland Dental LLC"


@pytest.mark.asyncio
async def test_import_control_snapshot_items_skip_non_serving_rate_files(monkeypatch):
    calls = 0

    async def fake_all(_stmt):
        nonlocal calls
        calls += 1
        if calls == 1:
            return []
        plan_info = [{"plan_id": "123", "plan_market_type": "group"}]
        return [
            (
                "source_1",
                "https://example.com/allowed-amounts.json.gz",
                "https://example.com/allowed-amounts.json.gz",
                "allowed-amounts",
                123,
                {"plan_info": plan_info},
                None,
                None,
                None,
                "Allowed",
                "Allowed amounts",
                "https://example.com/index.json",
            ),
            (
                "source_1",
                "https://example.com/in-network-rates.json.gz",
                "https://example.com/in-network-rates.json.gz",
                "in-network",
                456,
                {"plan_info": plan_info},
                None,
                None,
                None,
                "In Network",
                "In-network rates",
                "https://example.com/index.json",
            ),
        ]

    monkeypatch.setattr(discovery.db, "all", fake_all)

    items = await discovery._import_control_snapshot_items(["source_1"])

    assert len(items["source_1"]) == 1
    assert items["source_1"][0]["canonical_url"] == "https://example.com/in-network-rates.json.gz"
    assert items["source_1"][0]["domain"] == "in-network"


def test_dedupe_candidates_prefers_more_specific_url_for_same_canonical_key():
    short = discovery.SourceCandidate(
        payer_name="Blue Shield",
        provider="master-list",
        index_url="https://example.com/transparency",
    )
    specific = discovery.SourceCandidate(
        payer_name="Blue Shield",
        provider="master-list",
        index_url="https://example.com/transparency#machine-readable-files",
    )

    [candidate] = discovery._dedupe_candidates([short, specific])

    assert candidate.index_url == "https://example.com/transparency#machine-readable-files"


def test_classify_hosting_platforms():
    assert discovery.classify_hosting_platform("https://transparency-in-coverage.uhc.com/") == "uhc_public_blobs"
    assert discovery.classify_hosting_platform("https://bci.sapphiremrfhub.com/") == "sapphire"
    assert discovery.classify_hosting_platform("https://mrfdata.hmhs.com/") == "highmark_hmhs"
    assert discovery.classify_hosting_platform("https://www.bcbsil.com/asomrf?EIN=260241222") == "bcbs_asomrf"
    assert discovery.classify_hosting_platform("https://transparency-in-coverage.bluecrossma.com/") == "bcbsma_monthly_tocs"
    assert discovery.classify_hosting_platform("https://www.cigna.com/legal/compliance/machine-readable-files") == "cigna_static_mrf_lookup"


def test_highmark_hmhs_script_expands_current_month_index_urls():
    script = """
    var fileArr = [
      { regName: "Delaware", dl: "/files/070/del/inbound/local/?FIRST_DAY_CUR_MONTH_Highmark_Blue_Cross_Blue_Shield_of_Delaware_index.json", dt: "Highmark Blue Cross Blue Shield Delaware" },
      { regName: "Pennsylvania", dl: "/files/363/pa/inbound/local/?FIRST_DAY_CUR_MONTH_Highmark_Blue_Cross_Blue_Shield_of_Pennsylvania_index.json", dt: "Highmark Blue Cross Blue Shield Pennsylvania" },
    ]
    """

    targets = discovery._parse_highmark_hmhs_script(script, base_url="https://mrfdata.hmhs.com/", month_start="2026-06-01")

    assert len(targets) == 2
    assert targets[0]["url"] == "https://mrfdata.hmhs.com/files/070/del/inbound/local/2026-06-01_Highmark_Blue_Cross_Blue_Shield_of_Delaware_index.json"
    assert targets[0]["label"] == "Highmark Blue Cross Blue Shield Delaware"
    assert targets[1]["region"] == "Pennsylvania"


def test_parse_uhc_blob_listing_extracts_index_targets_only():
    payload = {
        "blobs": [
            {
                "name": "2026-06-01_ABC-COMPANY_index.json",
                "downloadUrl": "https://mrfstore.example/public/2026-06-01_ABC-COMPANY_index.json?sig=abc",
                "size": 1234,
            },
            {
                "name": "2026-06-01_ABC-COMPANY_in-network-rates.json.gz",
                "downloadUrl": "https://mrfstore.example/public/body.json.gz?sig=abc",
                "size": 9999,
            },
        ]
    }

    [target] = discovery._parse_uhc_blob_listing(payload)

    assert target["label"] == "Abc Company"
    assert target["size"] == 1234
    assert target["url"].endswith("?sig=abc")


def test_healthsparq_public_params_from_public_fragment():
    params = discovery._healthsparq_public_params(
        "https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICFI/machine-readable-transparency-in-coverage"
    )

    assert params == {"insurerCode": "AETNACVS_I", "brandCode": "ALICFI"}


def test_healthsparq_direct_metadata_url_uses_configured_template():
    resolver = discovery._source_config()["platform_resolvers"]["aetna_health1"]
    params = {"insurerCode": "AETNACVS_I", "brandCode": "ALICFI"}

    url = discovery._healthsparq_direct_metadata_url(resolver, params)

    assert url == "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICFI/latest_metadata.json"


def test_healthsparq_direct_metadata_url_uses_configured_tenant_override():
    resolver = discovery._source_config()["platform_resolvers"]["aetna_health1"]
    params = {"insurerCode": "MERITAIN_I", "brandCode": "MERITAINOVER"}

    url = discovery._healthsparq_direct_metadata_url(resolver, params)

    assert url == "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/MERITAIN_I/MERITAINOVER/latest_metadata.json"


def test_healthsparq_metadata_rows_include_direct_file_urls_and_plans():
    source = {"source_id": "source_1", "payer_id": "payer_1"}
    metadata_url = "https://mrf.healthsparq.com/example/prd/mrf/AETNACVS_I/ALICFI/latest_metadata.json"
    payload = {
        "files": [
            {
                "reportingEntityName": "Aetna Health",
                "reportingEntityType": "Health Insurance Issuer",
                "reportingPlans": [
                    {
                        "planName": "Aetna Open Access",
                        "planIdType": "hios",
                        "planId": "20523CA003",
                        "planMarketType": "group",
                    }
                ],
                "lastUpdatedOn": "2026-05-05",
                "fileSchema": "IN_NETWORK_RATES",
                "fileName": "rates.json.gz",
                "filePath": "2026-05-05/inNetworkRates/rates.json.gz",
            }
        ]
    }

    plan_rows, file_rows = discovery._healthsparq_rows_from_metadata(source, metadata_url, payload)

    assert len(plan_rows) == 1
    assert plan_rows[0]["plan_id"] == "20523CA003"
    assert len(file_rows) == 1
    assert file_rows[0]["file_type"] == "in-network"
    assert file_rows[0]["url"] == "https://mrf.healthsparq.com/example/prd/mrf/AETNACVS_I/ALICFI/2026-05-05/inNetworkRates/rates.json.gz"
    assert file_rows[0]["from_index_url"] == metadata_url
    assert file_rows[0]["metadata_json"]["resolver"] == "healthsparq_public_mrf"
    assert file_rows[0]["metadata_json"]["plan_info"] == [
        {
            "plan_id": "20523CA003",
            "plan_id_type": "hios",
            "plan_market_type": "group",
            "plan_name": "Aetna Open Access",
        }
    ]


def test_file_column_plan_info_synthesizes_import_control_plan_shape():
    plan_lookup = discovery._plan_lookup_from_rows(
        [
            (
                "source_1",
                "391125346",
                "ein",
                "group",
                "METAL PRODUCTS PPO",
                "Meritain Health",
            )
        ]
    )

    plan_info = discovery._file_column_plan_info(
        source_id="source_1",
        plan_ids=["391125346"],
        plan_names=["METAL PRODUCTS PPO"],
        market_types=["group"],
        plan_lookup=plan_lookup,
    )

    assert plan_info == [
        {
            "plan_id": "391125346",
            "plan_id_type": "ein",
            "plan_market_type": "group",
            "plan_name": "METAL PRODUCTS PPO",
            "issuer_name": None,
            "plan_sponsor_name": None,
        }
    ]
    assert discovery._reporting_entity_from_plan_info("source_1", plan_info, plan_lookup) == "Meritain Health"


def test_split_preview_items_slices_large_plan_info_without_changing_file_identity():
    [first, second, third] = discovery._split_preview_items(
        [
            {
                "canonical_url": "https://example.com/rates.json.gz",
                "domain": "in_network",
                "plan_info": [{"plan_id": str(index)} for index in range(5)],
            }
        ],
        max_plan_info=2,
    )

    assert first["canonical_url"] == "https://example.com/rates.json.gz"
    assert second["canonical_url"] == "https://example.com/rates.json.gz"
    assert third["canonical_url"] == "https://example.com/rates.json.gz"
    assert [plan["plan_id"] for plan in first["plan_info"]] == ["0", "1"]
    assert [plan["plan_id"] for plan in second["plan_info"]] == ["2", "3"]
    assert [plan["plan_id"] for plan in third["plan_info"]] == ["4"]


def test_import_control_seed_item_can_mark_auto_promoted_source():
    item = discovery._import_control_seed_item(
        {
            "source_id": "source_local",
            "index_url": "https://example.com/index.json",
            "display_name": "Example Payer",
            "source_key": "example",
            "seed_provider": "master-list",
            "license_status": "public",
            "confidence": 95,
            "hosting_platform": "sapphire",
            "source_type": "toc_json",
            "domain": "medical",
        },
        review_status="promoted",
        promoted_source_id="ic_source_1",
    )

    assert item is not None
    assert item["seed_url"] == "https://example.com/index.json"
    assert item["review_status"] == "promoted"
    assert item["promoted_source_id"] == "ic_source_1"
    assert item["metadata"]["healthcare_source_id"] == "source_local"
    assert item["reviewed_at"]


@pytest.mark.asyncio
async def test_push_import_control_catalog_marks_successful_seed_promoted(monkeypatch):
    calls = []
    source_upsert_count = 0

    class FakeResponse:
        def __init__(self, payload, status=200):
            self.payload = payload
            self.status = status

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

        async def json(self):
            return self.payload

        async def text(self):
            return "error"

    class FakeSession:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

        def post(self, url, json):
            nonlocal source_upsert_count
            calls.append({"url": url, "json": json})
            if url.endswith("/v1/catalog/sources"):
                source_upsert_count += 1
                return FakeResponse({"source_id": "ic_source_1"})
            if url.endswith("/v1/ptg/discover/ingest-preview"):
                return FakeResponse({"counts": {"plans": 2}})
            if url.endswith("/v1/catalog/seeds/import"):
                return FakeResponse({"count": 1, "items": json.get("items") or []})
            return FakeResponse({}, status=404)

        def get(self, url):
            calls.append({"url": url, "json": None})
            if url.endswith("/v1/catalog/sources/ic_source_1"):
                return FakeResponse({"source_id": "ic_source_1", "visibility": "internal", "status": "needs_review"})
            return FakeResponse({}, status=404)

    async def fake_snapshot(source_ids):
        assert source_ids == ["source_local"]
        return {
            "source_local": [
                {
                    "canonical_url": "https://example.com/rates.json.gz",
                    "domain": "in_network",
                    "reporting_entity_name": "Example Payer",
                    "plan_info": [
                        {"plan_id": "123", "plan_market_type": "group"},
                        {"plan_id": "456", "plan_market_type": "group"},
                    ],
                }
            ]
        }

    monkeypatch.setenv("HLTHPRT_IMPORT_CONTROL_URL", "http://import-control.test")
    monkeypatch.setenv("HLTHPRT_IMPORT_CONTROL_TOKEN", "secret")
    monkeypatch.setattr(discovery, "_import_control_snapshot_items", fake_snapshot)
    monkeypatch.setattr(discovery.aiohttp, "ClientSession", FakeSession)

    sources_synced, plans_synced, errors = await discovery._push_import_control_catalog(
        [
            {
                "source_id": "source_local",
                "index_url": "https://example.com/index.json",
                "human_url": "https://example.com/transparency",
                "display_name": "Example Payer",
                "source_key": "example",
                "seed_provider": "master-list",
                "access_model": "free",
                "source_type": "toc_json",
            }
        ]
    )

    assert sources_synced == 1
    assert plans_synced == 2
    assert errors == []
    assert [call["url"] for call in calls] == [
        "http://import-control.test/v1/catalog/sources",
        "http://import-control.test/v1/catalog/sources/ic_source_1",
        "http://import-control.test/v1/ptg/discover/ingest-preview",
        "http://import-control.test/v1/catalog/seeds/import",
        "http://import-control.test/v1/catalog/sources",
    ]
    assert source_upsert_count == 2
    assert calls[0]["json"]["visibility"] == "internal"
    assert calls[0]["json"]["status"] == "needs_review"
    assert calls[-1]["json"]["visibility"] == "public"
    assert calls[-1]["json"]["status"] == "active"
    assert calls[-1]["json"]["preserve_operator_state"] is False


@pytest.mark.asyncio
async def test_push_import_control_catalog_keeps_failed_source_internal_and_reports_error(monkeypatch):
    calls = []
    source_ids = iter(["ic_source_ok", "ic_source_fail"])

    class FakeResponse:
        def __init__(self, payload, status=200, text="error"):
            self.payload = payload
            self.status = status
            self._text = text

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

        async def json(self):
            return self.payload

        async def text(self):
            return self._text

    class FakeSession:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

        def post(self, url, json):
            calls.append({"url": url, "json": json})
            if url.endswith("/v1/catalog/sources"):
                if json.get("visibility") == "public":
                    return FakeResponse({"source_id": "ic_source_ok"})
                return FakeResponse({"source_id": next(source_ids)})
            if url.endswith("/v1/ptg/discover/ingest-preview"):
                if json["source_id"] == "ic_source_fail":
                    return FakeResponse({}, status=500, text="ingest exploded")
                return FakeResponse({"counts": {"plans": 2}})
            if url.endswith("/v1/catalog/seeds/import"):
                return FakeResponse({"count": 1, "items": json.get("items") or []})
            return FakeResponse({}, status=404)

        def get(self, url):
            calls.append({"url": url, "json": None})
            if url.endswith("/v1/catalog/sources/ic_source_ok"):
                return FakeResponse({"source_id": "ic_source_ok", "visibility": "internal", "status": "needs_review"})
            if url.endswith("/v1/catalog/sources/ic_source_fail"):
                return FakeResponse({"source_id": "ic_source_fail", "visibility": "internal", "status": "needs_review"})
            return FakeResponse({}, status=404)

    async def fake_snapshot(source_ids_arg):
        assert source_ids_arg == ["source_ok", "source_fail"]
        item = {
            "canonical_url": "https://example.com/rates.json.gz",
            "domain": "in_network",
            "reporting_entity_name": "Example Payer",
            "plan_info": [{"plan_id": "123", "plan_market_type": "group"}],
        }
        return {"source_ok": [item], "source_fail": [item]}

    monkeypatch.setenv("HLTHPRT_IMPORT_CONTROL_URL", "http://import-control.test")
    monkeypatch.setenv("HLTHPRT_IMPORT_CONTROL_TOKEN", "secret")
    monkeypatch.setattr(discovery, "_import_control_snapshot_items", fake_snapshot)
    monkeypatch.setattr(discovery.aiohttp, "ClientSession", FakeSession)

    sources_synced, plans_synced, errors = await discovery._push_import_control_catalog(
        [
            {
                "source_id": "source_ok",
                "index_url": "https://example.com/ok-index.json",
                "human_url": "https://example.com/ok",
                "display_name": "Example OK",
                "source_key": "example-ok",
                "seed_provider": "master-list",
                "access_model": "free",
                "source_type": "toc_json",
            },
            {
                "source_id": "source_fail",
                "index_url": "https://example.com/fail-index.json",
                "human_url": "https://example.com/fail",
                "display_name": "Example Fail",
                "source_key": "example-fail",
                "seed_provider": "master-list",
                "access_model": "free",
                "source_type": "toc_json",
            },
        ]
    )

    final_public_sources = [
        call["json"]
        for call in calls
        if call["url"].endswith("/v1/catalog/sources")
        and call["json"].get("visibility") == "public"
    ]

    assert sources_synced == 1
    assert plans_synced == 2
    assert len(errors) == 1
    assert errors[0]["source_id"] == "source_fail"
    assert errors[0]["import_control_source_id"] == "ic_source_fail"
    assert "ingest exploded" in errors[0]["message"]
    assert len(final_public_sources) == 1
    assert final_public_sources[0]["source_key"] == "example-ok"
    seed_calls = [call for call in calls if call["url"].endswith("/v1/catalog/seeds/import")]
    assert len(seed_calls) == 1
    seed_payload = seed_calls[0]["json"]
    assert seed_payload["seed_provider"] == "healthcare-mrf-api"
    [seed_item] = seed_payload["items"]
    assert seed_item["seed_url"] == "https://example.com/ok-index.json"
    assert seed_item["review_status"] == "promoted"
    assert seed_item["promoted_source_id"] == "ic_source_ok"


def test_parse_sapphire_toc_links_extracts_unique_json_hrefs():
    html = """
    <a href="/tocs/202606/2026-06-01_blue-cross-of-idaho_index.json">Download</a>
    <a href="/tocs/202606/2026-06-01_blue-cross-of-idaho_index.json">Copy</a>
    <a href="/not-a-toc/file.json">Ignore</a>
    """

    targets = discovery._parse_sapphire_toc_links(html, base_url="https://bci.sapphiremrfhub.com/")

    assert targets == [
        {
            "url": "https://bci.sapphiremrfhub.com/tocs/202606/2026-06-01_blue-cross-of-idaho_index.json",
            "label": "Blue Cross Of Idaho",
        }
    ]


def test_parse_html_mrf_metadata_links_extracts_meta_txt_files():
    html = """
    <a href="./in-network-rates-meta.txt">In Network</a>
    <a href="./allowed-amount-meta.txt">Allowed</a>
    <a href="./style.css">Ignore</a>
    """

    targets = discovery._parse_html_mrf_metadata_links(html, base_url="https://transparency-in-coverage.collectivehealth.com/index.html")

    assert targets == [
        {
            "url": "https://transparency-in-coverage.collectivehealth.com/in-network-rates-meta.txt",
            "label": "in-network-rates-meta.txt",
        },
        {
            "url": "https://transparency-in-coverage.collectivehealth.com/allowed-amount-meta.txt",
            "label": "allowed-amount-meta.txt",
        },
    ]


def test_parse_html_mrf_links_extracts_tocs_and_body_file_references():
    html = """
    <a href="/mrf/2026-06-01_example_index.json">TOC</a>
    <a href="/files/in-network-rates.zip">In Network ZIP</a>
    <a href="/files/allowed-amounts.json.gz">Allowed Amounts</a>
    <a href="/not-mrf/file.json">Ignore</a>
    """

    targets = discovery._parse_html_mrf_links(html, base_url="https://example.com/machine-readable-files")

    assert targets == [
        {
            "url": "https://example.com/mrf/2026-06-01_example_index.json",
            "label": "TOC",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "href",
        },
        {
            "url": "https://example.com/files/in-network-rates.zip",
            "label": "In Network ZIP",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "zip",
            "html_attr": "href",
        },
        {
            "url": "https://example.com/files/allowed-amounts.json.gz",
            "label": "Allowed Amounts",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "allowed-amounts",
            "container_format": "gzip",
            "html_attr": "href",
        },
    ]


def test_cigna_lookup_html_extracts_configured_and_page_lookup_urls():
    html = """<div data-mrf-lookup-url="/static/mrf/latest.json"></div>"""
    resolver = {"lookup_paths": ["/static/mrf/co/latest.json"]}

    urls = discovery._cigna_lookup_urls_from_html(
        html,
        base_url="https://www.cigna.com/legal/compliance/machine-readable-files",
        resolver=resolver,
    )

    assert urls == [
        "https://www.cigna.com/static/mrf/latest.json",
        "https://www.cigna.com/static/mrf/co/latest.json",
    ]


def test_cigna_lookup_targets_preserve_file_metadata_and_large_toc_limit():
    source = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "Cigna"}
    payload = {
        "mrfs": [
            {
                "reporting_entity_name": "Cigna Health Life Insurance Company",
                "reporting_entity_type": "Health Insurance Issuer",
                "last_updated_on": "2026-06-01",
                "reporting_month": "2026-06",
                "files": [
                    {
                        "file_name": "2026-06-01_cigna-health-life-insurance-company_index.json",
                        "file_size": "68.74 MB",
                        "url": "https://d25kgz5rikkq4n.cloudfront.net/index.json",
                    }
                ],
            }
        ]
    }

    targets = discovery._parse_cigna_lookup_targets(
        payload,
        lookup_url="https://www.cigna.com/static/mrf/latest.json",
        source=source,
        resolver={"toc_max_bytes": 104857600},
    )

    assert len(targets) == 1
    assert targets[0].url == "https://d25kgz5rikkq4n.cloudfront.net/index.json"
    assert targets[0].resolved_from_url == "https://www.cigna.com/static/mrf/latest.json"
    assert targets[0].metadata["resolver"] == "cigna_static_mrf_lookup"
    assert targets[0].metadata["target_max_bytes"] == 104857600
    assert targets[0].metadata["blob_size"] == 68740000
    assert targets[0].metadata["reporting_entity_name"] == "Cigna Health Life Insurance Company"


def test_bcbsma_monthly_tocs_generate_current_issuer_indexes():
    source = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "BCBS Massachusetts"}
    resolver = discovery._source_config()["platform_resolvers"]["bcbsma_monthly_tocs"]

    targets = discovery._bcbsma_monthly_toc_targets(
        source,
        "https://transparency-in-coverage.bluecrossma.com/",
        resolver,
        now=discovery.dt.datetime(2026, 6, 5, 12, 0, 0),
    )

    assert [target.url for target in targets] == [
        "https://transparency-in-coverage.bluecrossma.com/2026-06-01_Blue-Cross-and-Blue-Shield-of-Massachusetts-HMO-Blue-Inc_index.json",
        "https://transparency-in-coverage.bluecrossma.com/2026-06-01_Blue-Cross-and-Blue-Shield-of-Massachusetts-Inc_index.json",
    ]
    assert targets[0].metadata["resolver"] == "bcbsma_monthly_tocs"
    assert targets[0].metadata["month_start"] == "2026-06-01"
    assert targets[1].metadata["issuer_slug"] == "Blue-Cross-and-Blue-Shield-of-Massachusetts-Inc"


def test_metadata_text_rows_only_store_direct_body_files():
    source = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "Collective Health"}
    text = """
File Scope: allowed-amounts | Plan Name: PPO | Sponsor EIN: 010627671 | https://example.com/2026-05-20_PPO_allowed-amounts.json
File scope: In Network | Plan Name: HDHP | Sponsor EIN: 741670067 | https://bcbsil.com/asomrf?EIN=741670067
"""

    plan_rows, file_rows = discovery._metadata_text_rows_from_content(source, "https://example.com/allowed-amount-meta.txt", text)

    assert len(plan_rows) == 1
    assert len(file_rows) == 1
    assert plan_rows[0]["plan_id"] == "010627671"
    assert plan_rows[0]["reporting_entity_type"] == "third_party_administrator"
    assert file_rows[0]["file_type"] == "allowed-amounts"
    assert file_rows[0]["url"] == "https://example.com/2026-05-20_PPO_allowed-amounts.json"
    assert file_rows[0]["metadata_json"]["resolver"] == "html_metadata_text"


def test_metadata_text_rows_accept_zip_body_references():
    source = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "Example TPA"}
    text = "File Scope: In Network | Plan Name: PPO | https://example.com/in-network-rates.zip"

    _, file_rows = discovery._metadata_text_rows_from_content(source, "https://example.com/meta.txt", text)

    assert len(file_rows) == 1
    assert file_rows[0]["file_type"] == "in-network"
    assert file_rows[0]["metadata_json"]["container_format"] == "zip"


def test_crawl_target_limit_prefers_resolved_json_before_landing_pages():
    source = {"source_id": "source_1"}
    landing = discovery.CrawlTarget(source=source, url="https://example.com/mrf")
    resolved = discovery.CrawlTarget(source=source, url="https://example.com/index.json", resolved_from_url="https://example.com/js/script.js")
    direct_json = discovery.CrawlTarget(source=source, url="https://example.com/direct.json")

    ordered = sorted([landing, direct_json, resolved], key=discovery._crawl_target_rank)

    assert ordered == [resolved, direct_json, landing]


def test_direct_toc_url_gate_skips_html_landing_pages():
    assert discovery._looks_direct_toc_url("https://example.com/index.json") is True
    assert discovery._looks_direct_toc_url("https://example.com/app/public/#/one/machine-readable-transparency-in-coverage") is False
    assert discovery._looks_direct_toc_url("https://example.com/transparency") is False


def test_toc_target_file_row_keeps_index_provenance():
    target = discovery.CrawlTarget(
        source={"source_id": "source_1", "payer_id": "payer_1"},
        url="https://mrfstore.example/index.json?sig=abc",
        label="Example Index",
        resolved_from_url="https://example.com/api/v1/blobs/",
        metadata={"resolver": "uhc_blob_listing", "blob_size": 1234},
    )

    row = discovery._toc_target_file_row(target)

    assert row["file_type"] == "table-of-contents"
    assert row["description"] == "Example Index"
    assert row["is_signed_url"] is True
    assert row["size_bytes"] == 1234
    assert row["metadata_json"]["resolved_from_url"] == "https://example.com/api/v1/blobs/"


def test_toc_target_file_row_stores_html_zip_reference_without_body_fetch():
    target = discovery.CrawlTarget(
        source={"source_id": "source_1", "payer_id": "payer_1"},
        url="https://example.com/in-network-rates.zip",
        label="In Network ZIP",
        resolved_from_url="https://example.com/mrf",
        metadata={
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "zip",
            "blob_size": "1.5 GB",
        },
    )

    row = discovery._toc_target_file_row(target)

    assert row["file_type"] == "in-network"
    assert row["size_bytes"] == 1500000000
    assert row["metadata_json"]["target_kind"] == "file_reference"
    assert row["metadata_json"]["container_format"] == "zip"


def test_toc_rows_skip_non_http_body_placeholders(monkeypatch):
    monkeypatch.setattr(
        discovery,
        "parse_toc_catalog_entries",
        lambda *_args: [
            types.SimpleNamespace(
                source_type="in-network",
                original_url="Missing file",
                canonical_url="Missing file",
                from_index_url="https://example.com/index.json",
                description="Missing file",
                domain=None,
                reporting_entity_name="Example",
                reporting_entity_type="third_party_administrator",
                plan_info=(),
            )
        ],
    )

    plan_rows, file_rows = discovery._toc_rows_from_content({"source_id": "source_1"}, "https://example.com/index.json", {})

    assert plan_rows == []
    assert file_rows == []


def test_toc_rows_store_plan_info_on_file_metadata(monkeypatch):
    monkeypatch.setattr(
        discovery,
        "parse_toc_catalog_entries",
        lambda *_args: [
            types.SimpleNamespace(
                source_type="in-network",
                original_url="https://example.com/rates.json.gz",
                canonical_url="https://example.com/rates.json.gz",
                from_index_url="https://example.com/index.json",
                description="In-Network Rates",
                domain="example.com",
                reporting_entity_name="Example Payer",
                reporting_entity_type="third_party_administrator",
                plan_info=(
                    {"plan_id": "123", "plan_id_type": "ein", "plan_market_type": "group", "plan_name": "Plan A"},
                ),
            )
        ],
    )

    plan_rows, file_rows = discovery._toc_rows_from_content(
        {"source_id": "source_1"}, "https://example.com/index.json", {}
    )

    assert len(file_rows) == 1
    # The exact per-file plan list (with plan_id_type) is preserved so the import-control
    # snapshot can be rebuilt from stored rows.
    assert file_rows[0]["metadata_json"]["plan_info"] == [
        {"plan_id": "123", "plan_id_type": "ein", "plan_market_type": "group", "plan_name": "Plan A"}
    ]
    assert file_rows[0]["metadata_json"]["container_format"] == "gzip"
    assert plan_rows[0]["plan_id_type"] == "ein"


def test_eligible_for_public_promotion_only_free_direct_sources():
    assert discovery._eligible_for_public_promotion({"access_model": "free", "source_type": "community_index"}) is True
    assert discovery._eligible_for_public_promotion({"access_model": "paid", "source_type": "vendor_aggregator"}) is False
    # Free but aggregator provenance is not a direct public MRF source.
    assert discovery._eligible_for_public_promotion({"access_model": "free", "source_type": "vendor_aggregator"}) is False
    assert discovery._eligible_for_public_promotion({"access_model": "unknown", "source_type": "curated_registry"}) is False


def test_parse_file_probe_types_defaults_and_dedupes():
    assert discovery._parse_file_probe_types(None) == ("in-network", "allowed-amounts")
    assert discovery._parse_file_probe_types("in-network, allowed-amounts in-network") == ("in-network", "allowed-amounts")


def test_parse_text_filter_values_defaults_and_dedupes():
    assert discovery._parse_text_filter_values(None) == ()
    assert discovery._parse_text_filter_values("tpa, network/tpa TPA") == ("tpa", "network/tpa")


def test_candidate_matches_text_filters_by_entity_type_and_payer_name():
    candidate = discovery.SourceCandidate(payer_name="Collective Health", provider="master-list", entity_type="tpa")

    assert discovery._candidate_matches_text_filters(candidate, entity_types=("tpa",), payer_query=None) is True
    assert discovery._candidate_matches_text_filters(candidate, entity_types=("network/tpa",), payer_query=None) is False
    assert discovery._candidate_matches_text_filters(candidate, entity_types=(), payer_query="collective") is True
    assert discovery._candidate_matches_text_filters(candidate, entity_types=(), payer_query="meritain") is False


def test_discovery_run_mode_combines_probe_files():
    assert discovery._discovery_run_mode(crawl=False, check_urls=False, probe_files=True) == "probe_files"
    assert discovery._discovery_run_mode(crawl=True, check_urls=True, probe_files=True) == "crawl+check_urls+probe_files"


def test_file_probe_observation_and_update_payloads_include_etag_and_last_modified():
    target = {
        "mrf_file_id": "file_1",
        "url": "https://example.com/rates.json.gz",
        "file_type": "in-network",
        "payer_id": "payer_1",
        "payer_name": "Collective Health",
        "entity_type": "tpa",
    }
    checked_at = discovery.dt.datetime(2026, 6, 5, 12, 0, 0)
    head = {
        "status": "ok",
        "http_status": 200,
        "etag": '"abc123"',
        "last_modified": "Fri, 05 Jun 2026 10:00:00 GMT",
        "content_length": 123456,
        "content_type": "application/octet-stream",
        "final_url": "https://example.com/rates.json.gz",
        "checked_at": checked_at,
    }

    observation = discovery._file_probe_observation(target, head, "run_1")
    update_values = discovery._file_probe_update_values(target, head)

    assert observation["url_type"] == "body_file_head"
    assert observation["etag"] == '"abc123"'
    assert observation["last_modified"] == "Fri, 05 Jun 2026 10:00:00 GMT"
    assert observation["content_length"] == 123456
    assert observation["metadata_json"]["mrf_file_id"] == "file_1"
    assert observation["metadata_json"]["payer_name"] == "Collective Health"
    assert observation["metadata_json"]["entity_type"] == "tpa"
    assert update_values == {
        "mrf_file_id": "file_1",
        "size_bytes": 123456,
        "etag": '"abc123"',
        "last_modified": "Fri, 05 Jun 2026 10:00:00 GMT",
    }


def test_interleave_file_probe_targets_by_host():
    targets = [
        {"url": "https://a.example.com/1"},
        {"url": "https://a.example.com/2"},
        {"url": "https://a.example.com/3"},
        {"url": "https://b.example.com/1"},
        {"url": "https://c.example.com/1"},
    ]

    interleaved = discovery._interleave_file_probe_targets_by_host(targets)

    assert [discovery._file_probe_target_host(item) for item in interleaved[:3]] == [
        "a.example.com",
        "b.example.com",
        "c.example.com",
    ]


def test_crawl_sources_dedupe_by_canonical_url_and_prefer_curated_rows():
    duplicate_lower_confidence = {
        "source_id": "vendor",
        "index_url": "https://transparency-in-coverage.uhc.com/",
        "canonical_url": "https://transparency-in-coverage.uhc.com/",
        "source_type": "community_index",
        "seed_provider": "manual",
        "confidence": 95,
    }
    curated = {
        "source_id": "curated",
        "index_url": "https://transparency-in-coverage.uhc.com/",
        "canonical_url": "https://transparency-in-coverage.uhc.com/",
        "source_type": "curated_registry",
        "seed_provider": "master-list",
        "confidence": 85,
    }
    other = {
        "source_id": "other",
        "index_url": "https://mrfdata.hmhs.com/",
        "canonical_url": "https://mrfdata.hmhs.com/",
        "source_type": "curated_registry",
        "seed_provider": "master-list",
        "confidence": 85,
    }

    rows = discovery._dedupe_source_rows_for_crawl([duplicate_lower_confidence, curated, other])

    assert [row["source_id"] for row in rows] == ["curated", "other"]


@pytest.mark.asyncio
async def test_push_crawl_row_batches_chunks_large_model_writes(monkeypatch):
    calls = []

    async def fake_push_objects(rows, model, *, rewrite, use_copy):
        calls.append((model, len(rows), rewrite, use_copy))

    monkeypatch.setattr(discovery, "push_objects", fake_push_objects)
    plan_rows = [{"mrf_plan_id": str(index)} for index in range(5)]
    file_rows = [{"mrf_file_id": str(index)} for index in range(3)]
    observation_rows = [{"observation_id": str(index)} for index in range(1)]

    await discovery._push_crawl_row_batches(plan_rows, file_rows, observation_rows, batch_size=2)

    assert plan_rows == []
    assert file_rows == []
    assert observation_rows == []
    assert calls == [
        (discovery.MRFPlan, 2, True, False),
        (discovery.MRFPlan, 2, True, False),
        (discovery.MRFPlan, 1, True, False),
        (discovery.MRFFile, 2, True, False),
        (discovery.MRFFile, 1, True, False),
        (discovery.MRFUrlObservation, 1, True, False),
    ]


@pytest.mark.asyncio
async def test_store_observations_does_not_emit_live_progress_without_control_run(monkeypatch):
    pushed = []
    progress_calls = []

    async def fake_push_objects(rows, model, *, rewrite, use_copy):
        pushed.extend(rows)

    monkeypatch.setattr(discovery, "push_objects", fake_push_objects)
    monkeypatch.setattr(discovery, "enqueue_live_progress", lambda **payload: progress_calls.append(payload))

    observations = await discovery._store_observations(
        [{"source_id": "source_1", "index_url": "https://example.com/index.json"}],
        test_mode=True,
        run_id="crawl_run_1",
        progress_run_id=None,
        concurrency=1,
    )

    assert len(observations) == 1
    assert pushed == observations
    assert observations[0]["metadata_json"]["run_id"] == "crawl_run_1"
    assert progress_calls == []


@pytest.mark.asyncio
async def test_private_url_rejected_before_fetch():
    with pytest.raises(ValueError):
        await discovery._assert_fetch_url_allowed("http://169.254.169.254/latest/meta-data")

    with pytest.raises(ValueError):
        await discovery._assert_fetch_url_allowed("http://127.0.0.1:8080/index.json")


@pytest.mark.asyncio
async def test_dry_run_uses_master_list_without_database(monkeypatch):
    monkeypatch.setattr(discovery, "_repo_root", lambda: discovery.Path(__file__).resolve().parents[1])

    result = await discovery.main(test_mode=True, provider="master-list", limit=3, dry_run=True)

    assert result["providers"] == ["master-list"]
    assert result["candidates"] == 3
    assert result["payers"] == 3
