# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import types

import pytest

discovery = importlib.import_module("process.mrf_source_discovery")


def test_source_urls_are_loaded_from_registry_file():
    config = discovery._source_config()

    assert "accessmrf" in config["default_providers"]
    assert config["providers"]["accessmrf"]["url"].startswith("https://")
    assert "detail_url_template" not in config["providers"]["accessmrf"]
    assert config["platform_resolvers"]["aetna_health1"]["type"] == "healthsparq_public_mrf"
    assert config["platform_resolvers"]["healthsparq"]["type"] == "healthsparq_public_mrf"
    assert config["platform_resolvers"]["highmark_hmhs"]["type"] == "highmark_hmhs_script"
    assert config["platform_resolvers"]["sapphire"]["type"] == "sapphire_html_tocs"
    assert config["platform_resolvers"]["uhc_public_blobs"]["type"] == "uhc_blob_listing"
    assert config["platform_resolvers"]["aetna_health1"]["tenant_overrides"]["MERITAIN_I"] == "aetnacvs"


def test_parse_master_list_preserves_payers_and_urls():
    markdown = """
## A. National parents + subsidiaries
| Payer (parent) | Sources | Public MRF TOC / landing URL |
|---|---|---|
| **Cigna** (The Cigna Group) | M A P S T B U | https://www.cigna.com/legal/compliance/machine-readable-files |
| **Humana** | M A(archived) P S T B U | https://developers.humana.com/cost-transparency |

## B. BCBS independent licensees
| Blue plan | Sources | Public MRF TOC / landing URL |
|---|---|---|
| BCBS Alabama | M A P U | https://www.bcbsal.org/web/tcr |
"""

    candidates = discovery.parse_master_list(markdown)

    assert [item.payer_name for item in candidates] == ["Cigna", "Humana", "BCBS Alabama"]
    assert candidates[0].parent_group == "Cigna"
    assert candidates[2].entity_type == "blue"
    assert candidates[2].hosting_platform == "custom"
    assert candidates[0].source_coverage == ("M", "A", "P", "S", "T", "B", "U")


def test_parse_master_list_preserves_tpa_hint_and_multiple_urls():
    markdown = """
## A. National parents + subsidiaries
| Payer (parent) | Sources | Public MRF TOC / landing URL |
|---|---|---|
| ↳ Meritain Health (TPA) | A P U | https://health1.aetna.com/app/public/#/one/insurerCode=MERITAIN_I&brandCode=MERITAINOVER/… |

## C. Regional, provider-sponsored, Medicaid-MCO, DTC & TPA payers
| Payer | Type | Sources | Public TOC / landing URL |
|---|---|---|---|
| Collective Health | TPA | P S U | https://transparency-in-coverage.collectivehealth.com/index.html · https://transparency-in-coverage.collectivehealth.com/ |
"""

    candidates = discovery.parse_master_list(markdown)

    assert candidates[0].payer_name == "Meritain Health"
    assert candidates[0].entity_type == "tpa"
    assert candidates[0].index_url.endswith("MERITAINOVER/")
    collective = [item for item in candidates if item.payer_name == "Collective Health"]
    assert len(collective) == 2
    assert {item.entity_type for item in collective} == {"tpa"}


def test_parse_accessmrf_sources_maps_counts_and_platform():
    payload = {
        "sources": [
            {
                "displayName": "Aetna CVS - Fully Insured",
                "status": "current",
                "numPlans": 972,
                "numFiles": 41753,
                "numIndices": 266,
                "latestIndexDate": "2026-05-05",
                "totalCompressedSize": 43219328166311,
                "humanUrl": "https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICFI/machine-readable-transparency-in-coverage",
            }
        ]
    }

    [candidate] = discovery.parse_accessmrf_sources(payload)

    assert candidate.provider == "accessmrf"
    assert candidate.status == "active"
    assert candidate.hosting_platform == "aetna_health1"
    assert candidate.num_plans == 972
    assert candidate.total_compressed_size == 43219328166311


def test_parse_payerset_sitemap_dedupes_slugs():
    text = """
    - /payers/cigna-price-transparency.md
    - /payers/cigna-price-transparency.md
    - /payers/blue-cross-blue-shield-alabama-price-transparency.md
    """

    candidates = discovery.parse_payerset_sitemap(text)

    assert [item.raw_payload["slug"] for item in candidates] == ["cigna", "blue-cross-blue-shield-alabama"]
    assert candidates[0].access_model == "paid"


def test_classify_hosting_platforms():
    assert discovery.classify_hosting_platform("https://transparency-in-coverage.uhc.com/") == "uhc_public_blobs"
    assert discovery.classify_hosting_platform("https://bci.sapphiremrfhub.com/") == "sapphire"
    assert discovery.classify_hosting_platform("https://mrfdata.hmhs.com/") == "highmark_hmhs"
    assert discovery.classify_hosting_platform("https://www.bcbsil.com/asomrf?EIN=260241222") == "bcbs_asomrf"


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
    duplicate_vendor = {
        "source_id": "vendor",
        "index_url": "https://transparency-in-coverage.uhc.com/",
        "canonical_url": "https://transparency-in-coverage.uhc.com/",
        "source_type": "community_index",
        "seed_provider": "accessmrf",
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

    rows = discovery._dedupe_source_rows_for_crawl([duplicate_vendor, curated, other])

    assert [row["source_id"] for row in rows] == ["curated", "other"]


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
    assert result["payers"] == 2
