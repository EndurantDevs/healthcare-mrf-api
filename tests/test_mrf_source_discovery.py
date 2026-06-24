# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import gzip
import importlib
import json
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
    assert config["platform_resolvers"]["auxiant_wordpress"]["type"] == "auxiant_wordpress_directory"
    assert config["platform_resolvers"]["mymedicalshopper_talon"]["type"] == "mymedicalshopper_talon_mrf"
    assert config["platform_resolvers"]["asr_health_benefits"]["type"] == "asr_health_benefits_mrf"
    assert config["platform_resolvers"]["asr_health_benefits"]["seed_list"] == "asr_health_benefits_groups"
    assert config["seed_lists"]["asr_health_benefits_groups"]["schema"] == "group_number_seed_v1"
    assert config["platform_resolvers"]["uhc_public_blobs"]["type"] == "uhc_blob_listing"
    assert config["platform_resolvers"]["bcbsma_monthly_tocs"]["type"] == "bcbsma_monthly_tocs"
    assert config["platform_resolvers"]["cigna_static_mrf_lookup"]["type"] == "cigna_static_mrf_lookup"
    assert config["platform_resolvers"]["meritain_mrf_search"]["type"] == "meritain_mrf_search"
    assert config["platform_resolvers"]["healthcarebluebook_mrf"]["type"] == "healthcarebluebook_mrf"
    assert config["platform_resolvers"]["html_mrf_with_healthcarebluebook"]["type"] == "html_mrf_with_healthcarebluebook"
    assert config["platform_resolvers"]["bcbs_asomrf"]["type"] == "bcbs_asomrf_filelist"
    assert config["platform_resolvers"]["bcbs_asomrf"]["max_targets"] == 250
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
| ASR Health Benefits | tpa | https://www.asrhealthbenefits.com/MRF | public group-number MRF lookup |
"""

    candidates = discovery.parse_master_list(markdown)

    assert candidates[0].payer_name == "Meritain Health"
    assert candidates[0].entity_type == "tpa"
    assert candidates[0].index_url.endswith("MERITAINOVER/")
    collective = [item for item in candidates if item.payer_name == "Collective Health"]
    assert len(collective) == 2
    assert {item.entity_type for item in collective} == {"tpa"}
    [asr] = [item for item in candidates if item.payer_name == "ASR Health Benefits"]
    assert asr.entity_type == "tpa"
    assert asr.hosting_platform == "asr_health_benefits"


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
            (
                "source_1",
                "https://example.com/direct-in-network-rates.json.gz",
                "https://example.com/direct-in-network-rates.json.gz",
                "in-network",
                789,
                {"plan_info": plan_info},
                None,
                None,
                None,
                "Direct In Network",
                "Direct in-network rates",
                None,
            ),
        ]

    monkeypatch.setattr(discovery.db, "all", fake_all)

    items = await discovery._import_control_snapshot_items(["source_1"])

    assert len(items["source_1"]) == 1
    assert items["source_1"][0]["canonical_url"] == "https://example.com/in-network-rates.json.gz"
    assert items["source_1"][0]["domain"] == "in-network"


def test_import_control_snapshot_file_support_excludes_csv_catalog_references():
    assert (
        discovery._import_control_snapshot_file_is_supported(
            "in-network",
            {"source_format": "csv", "domain": "in-network"},
            "https://example.com/index.json",
        )
        is False
    )
    assert (
        discovery._import_control_snapshot_file_is_supported(
            "in-network",
            {"source_format": "zip", "domain": "in-network"},
            "https://example.com/index.json",
        )
        is True
    )


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
    assert discovery.classify_hosting_platform("https://transparency.auxiant.com/directory-of-data-sources/") == "auxiant_wordpress"
    assert discovery.classify_hosting_platform("https://transparency.auxiant.com/healthsmart/") == "auxiant_wordpress"
    assert discovery.classify_hosting_platform("https://www.asrhealthbenefits.com/MRF") == "asr_health_benefits"
    assert discovery.classify_hosting_platform("https://mrfsearch.meritain.com/") == "meritain_mrf_search"
    assert discovery.classify_hosting_platform("https://mrf.healthcarebluebook.com/Lucent") == "healthcarebluebook_mrf"
    assert (
        discovery.classify_hosting_platform("https://www.myhealthbenefits.com/MyHealthBenefits/Home/MRFs/")
        == "html_mrf_with_healthcarebluebook"
    )
    assert discovery.classify_hosting_platform("https://lucenthealth.com/transparency-in-coverage/") == "html_mrf_with_healthcarebluebook"
    assert discovery.classify_hosting_platform("https://www.mymedicalshopper.com/mrf-search/varipro") == "mymedicalshopper_talon"
    assert (
        discovery.classify_hosting_platform("https://www.mymedicalshopper.com/mrf/electrical-workers-cofinity-varipro-77100")
        == "mymedicalshopper_talon"
    )


def test_meritain_mrf_search_parser_extracts_group_healthsparq_links():
    source = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "Meritain Health"}
    html = """
    <table>
      <tr>
        <td>14445</td>
        <td><a href="https://Health1.Meritain.com/app/public/#/one/insurerCode=MERITAIN_I&amp;brandCode=MERITAINOVER/machine-readable-transparency-in-coverage?reportingEntityType=TPA_14445&amp;lock=true">MRF</a></td>
      </tr>
      <tr>
        <td>ignore</td>
        <td><a href="https://meritain.com/">Return to home</a></td>
      </tr>
    </table>
    """

    [target] = discovery._parse_meritain_mrf_search_targets(
        html,
        base_url="https://mrfsearch.meritain.com/",
        source=source,
        resolver_type="meritain_mrf_search",
    )

    assert target.url == (
        "https://Health1.Meritain.com/app/public/#/one/insurerCode=MERITAIN_I&brandCode=MERITAINOVER/"
        "machine-readable-transparency-in-coverage?reportingEntityType=TPA_14445&lock=true"
    )
    assert target.metadata["target_kind"] == "file_reference"
    assert target.metadata["target_file_type"] == "table-of-contents"
    assert target.metadata["group_id"] == "14445"
    assert target.metadata["plan_info"] == [
        {
            "plan_id": "14445",
            "plan_id_type": "group_id",
            "plan_market_type": "group",
            "plan_name": "Meritain group 14445",
        }
    ]


def test_healthcarebluebook_grid_parser_extracts_link_type_pairs():
    html = """
    <div class="grid-item"><a href="/Lucent/350504">Lucent Health</a></div>
    <div class="grid-item">Table of Contents</div>
    <div class="grid-item"><a href="/Lucent/350380">Lucent Health 042171239</a></div>
    <div class="grid-item">Out of Network</div>
    <div class="grid-item"><a href="https://hcbbmrfprod.blob.core.windows.net/mrf/External/example_in-network-rates.json.zip">Center Care</a></div>
    <div class="grid-item">In Network</div>
    """

    items = discovery._healthcarebluebook_grid_items(html, base_url="https://mrf.healthcarebluebook.com/Lucent")

    assert items == [
        {"url": "https://mrf.healthcarebluebook.com/Lucent/350504", "label": "Lucent Health", "text": "Lucent Health"},
        {"url": None, "label": "Table of Contents", "text": "Table of Contents"},
        {"url": "https://mrf.healthcarebluebook.com/Lucent/350380", "label": "Lucent Health 042171239", "text": "Lucent Health 042171239"},
        {"url": None, "label": "Out of Network", "text": "Out of Network"},
        {
            "url": "https://hcbbmrfprod.blob.core.windows.net/mrf/External/example_in-network-rates.json.zip",
            "label": "Center Care",
            "text": "Center Care",
        },
        {"url": None, "label": "In Network", "text": "In Network"},
    ]


@pytest.mark.asyncio
async def test_healthcarebluebook_resolver_catalogs_stable_file_links(monkeypatch):
    source = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "Lucent Health"}
    html = """
    <div class="grid-item"><a href="/Lucent/350504">Lucent Health</a></div>
    <div class="grid-item">Table of Contents</div>
    <div class="grid-item"><a href="/Lucent/350380">Lucent Health 042171239</a></div>
    <div class="grid-item">Out of Network</div>
    <div class="grid-item"><a href="https://hcbbmrfprod.blob.core.windows.net/mrf/External/example_in-network-rates.json.zip">Center Care</a></div>
    <div class="grid-item">In Network</div>
    """

    async def fake_fetch_text(url, **_kwargs):
        assert url == "https://mrf.healthcarebluebook.com/Lucent"
        return html

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    targets = await discovery._resolve_healthcarebluebook_mrf(
        source,
        "https://mrf.healthcarebluebook.com/Lucent",
        {"type": "healthcarebluebook_mrf"},
        None,
    )

    assert [target.url for target in targets] == [
        "https://mrf.healthcarebluebook.com/Lucent/350504",
        "https://mrf.healthcarebluebook.com/Lucent/350380",
        "https://hcbbmrfprod.blob.core.windows.net/mrf/External/example_in-network-rates.json.zip",
    ]
    assert targets[0].metadata["target_file_type"] == "table-of-contents"
    assert targets[0].metadata["source_format"] == "zip"
    assert targets[1].metadata["target_file_type"] == "allowed-amounts"
    assert targets[1].metadata["plan_info"][0]["plan_id"] == "042171239"
    assert targets[1].metadata["plan_info"][0]["plan_id_type"] == "ein"
    assert targets[2].metadata["target_file_type"] == "in-network"
    assert targets[2].metadata["container_format"] == "zip"


@pytest.mark.asyncio
async def test_html_healthcarebluebook_resolver_combines_direct_and_delegated_links(monkeypatch):
    source = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "BRMS"}
    html_by_url = {
        "https://www.myhealthbenefits.com/MyHealthBenefits/Home/MRFs/": """
          <a href="https://www.myhealthbenefits.com/MRF/2026-06-04_ClaimDOC_BRMS_index.json">BRMS Index</a>
          <a href="https://www.myhealthbenefits.com/MRF/2026-06-01_BRMS_allowed-amounts.csv">Zelis In-Network</a>
          <a href="https://mrf.healthcarebluebook.com/BRMS">Out-of-Network</a>
        """,
        "https://mrf.healthcarebluebook.com/BRMS": """
          <div class="grid-item"><a href="/BRMS/326940">Benefit &amp; Risk Management Services, Inc. (BRMS)</a></div>
          <div class="grid-item">Table of Contents</div>
          <div class="grid-item"><a href="/BRMS/314355">Benefit &amp; Risk Management Services, Inc. (BRMS) 030506501</a></div>
          <div class="grid-item">Out of Network</div>
        """,
    }

    async def fake_fetch_text(url, **_kwargs):
        return html_by_url[url]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    targets = await discovery._resolve_html_mrf_with_healthcarebluebook(
        source,
        "https://www.myhealthbenefits.com/MyHealthBenefits/Home/MRFs/",
        {"type": "html_mrf_with_healthcarebluebook"},
        None,
    )

    by_url = {target.url: target for target in targets}
    assert by_url["https://www.myhealthbenefits.com/MRF/2026-06-04_ClaimDOC_BRMS_index.json"].metadata["target_file_type"] == (
        "table-of-contents"
    )
    csv_target = by_url["https://www.myhealthbenefits.com/MRF/2026-06-01_BRMS_allowed-amounts.csv"]
    assert csv_target.metadata["target_file_type"] == "allowed-amounts"
    assert csv_target.metadata["source_format"] == "csv"
    delegated = by_url["https://mrf.healthcarebluebook.com/BRMS/314355"]
    assert delegated.metadata["resolver"] == "html_mrf_with_healthcarebluebook"
    assert delegated.metadata["nested_resolver"] == "healthcarebluebook_mrf"
    assert delegated.metadata["plan_info"][0]["plan_id"] == "030506501"


def test_asr_health_benefits_resolver_expands_configured_group_numbers():
    source = {"source_id": "source_1", "display_name": "ASR Health Benefits"}
    resolver = {"type": "asr_health_benefits_mrf", "toc_path": "/umbraco/surface/mrfdownload", "group_numbers": ["1208"]}

    [target] = discovery._resolve_asr_health_benefits_mrf(source, "https://www.asrhealthbenefits.com/MRF", resolver)

    assert target.url == "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber=1208"
    assert target.label == "ASR Health Benefits group 1208"
    assert target.resolved_from_url == "https://www.asrhealthbenefits.com/MRF"
    assert target.metadata["resolver"] == "asr_health_benefits_mrf"
    assert target.metadata["group_number"] == "1208"


def test_asr_health_benefits_resolver_preserves_direct_group_number():
    source = {"source_id": "source_1", "display_name": "ASR Health Benefits"}
    resolver = {"type": "asr_health_benefits_mrf", "toc_path": "/umbraco/surface/mrfdownload", "group_numbers": ["1208"]}

    targets = discovery._resolve_asr_health_benefits_mrf(
        source,
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber=1194",
        resolver,
    )

    assert [target.metadata["group_number"] for target in targets] == ["1194", "1208"]


def test_asr_health_benefits_resolver_uses_seed_list():
    source = {"source_id": "source_1", "display_name": "ASR Health Benefits"}
    resolver = discovery._source_config()["platform_resolvers"]["asr_health_benefits"]
    expected_groups = discovery._asr_group_numbers_from_seed_list(resolver["seed_list"])

    targets = discovery._resolve_asr_health_benefits_mrf(source, "https://www.asrhealthbenefits.com/MRF", resolver)

    assert {"1194", "1208"}.issubset(set(expected_groups))
    assert [target.metadata["group_number"] for target in targets] == expected_groups
    assert targets[0].url.startswith(
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber="
    )


def test_asr_health_benefits_seed_list_filters_active_rows(tmp_path, monkeypatch):
    seed_path = tmp_path / "asr-groups.csv"
    seed_path.write_text(
        "group_number,status,source_url,first_seen_at,last_verified_at,notes\n"
        "1194,active,https://www.asrhealthbenefits.com/MRF,2026-06-24,2026-06-24,public\n"
        "1208,retired,https://www.asrhealthbenefits.com/MRF,2026-06-24,2026-06-24,old\n"
        "1210,active,https://www.asrhealthbenefits.com/MRF,2026-06-24,2026-06-24,public\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "sources.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": {},
                "seed_lists": {
                    "asr_test": {
                        "schema": "group_number_seed_v1",
                        "path": str(seed_path),
                    }
                },
                "platform_resolvers": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(discovery.SOURCE_CONFIG_ENV, str(config_path))
    monkeypatch.setattr(discovery, "_SOURCE_CONFIG_CACHE", None)

    assert discovery._asr_group_numbers_from_seed_list("asr_test") == ["1194", "1210"]


def test_asr_health_benefits_seed_list_dedupes_direct_and_configured_numbers(tmp_path, monkeypatch):
    seed_path = tmp_path / "asr-groups.csv"
    seed_path.write_text(
        "group_number,status\n1194,active\n1208,active\n1194,active\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "sources.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": {},
                "seed_lists": {
                    "asr_test": {
                        "schema": "group_number_seed_v1",
                        "path": str(seed_path),
                    }
                },
                "platform_resolvers": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(discovery.SOURCE_CONFIG_ENV, str(config_path))
    monkeypatch.setattr(discovery, "_SOURCE_CONFIG_CACHE", None)
    resolver = {
        "type": "asr_health_benefits_mrf",
        "toc_path": "/umbraco/surface/mrfdownload",
        "seed_list": "asr_test",
        "group_numbers": ["1208"],
    }

    assert (
        discovery._asr_group_numbers_for_source(
            "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber=1194",
            resolver,
        )
        == ["1194", "1208"]
    )


def test_asr_health_benefits_seed_list_rejects_non_four_digit_values(tmp_path, monkeypatch):
    seed_path = tmp_path / "asr-groups.csv"
    seed_path.write_text("group_number,status\n1208,active\n12,active\n", encoding="utf-8")
    config_path = tmp_path / "sources.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": {},
                "seed_lists": {
                    "asr_test": {
                        "schema": "group_number_seed_v1",
                        "path": str(seed_path),
                    }
                },
                "platform_resolvers": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(discovery.SOURCE_CONFIG_ENV, str(config_path))
    monkeypatch.setattr(discovery, "_SOURCE_CONFIG_CACHE", None)

    with pytest.raises(ValueError, match="4-digit"):
        discovery._asr_group_numbers_from_seed_list("asr_test")


def test_auxiant_directory_parser_extracts_data_available_networks():
    html = """
    <div class="entry-content">
      <p>
        <a href="https://transparency.auxiant.com/aetna/">*Aetna</a><br>
        <a href="https://transparency.auxiant.com/implementation-in-process/">CHA Health</a><br>
        <a href="https://transparency.auxiant.com/healthsmart/">*HealthSmart</a>
        <a href="https://transparency.auxiant.com/first-choice-health/">First Choice Health</a><br>
        <a href="https://transparency.auxiant.com/first-choice-health/">*First Choice Health</a>
      </p>
    </div><!-- .entry-content -->
    """

    networks = discovery._parse_auxiant_directory_networks(
        html,
        base_url="https://transparency.auxiant.com/directory-of-data-sources/",
    )

    assert networks == [
        {"url": "https://transparency.auxiant.com/aetna/", "label": "Aetna", "data_available": True},
        {"url": "https://transparency.auxiant.com/healthsmart/", "label": "HealthSmart", "data_available": True},
        {"url": "https://transparency.auxiant.com/first-choice-health/", "label": "First Choice Health", "data_available": True},
    ]


def test_auxiant_page_link_parser_extracts_external_and_direct_files():
    html = """
    <div class="entry-content">
      <p><a href="https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&amp;brandCode=ASA/machine-readable-transparency-in-coverage">Aetna hosted files</a></p>
      <table>
        <tr><td><a href="https://s3.us-east-2.amazonaws.com/transparency.auxiant.com/healthsmart/2022-07-01_HealthSmart-Payors-Organization_in-network-rates.json.zip">HealthSmart ZIP</a></td></tr>
        <tr><td><a href="https://transparency.auxiant.com/wp-admin/admin-ajax.php?action=mk_file_folder_manager&amp;cmd=file&amp;target=fls2_Rmlyc3RDaG9pY2VIZWFsdGgvMjAyNjAxMDItcHJvdmlkZXJzMjAyNjAxMDJmY3BuLnppcA">20260102-providers20260102fcpn.zip</a></td></tr>
        <tr><td><a href="https://s3.us-east-2.amazonaws.com/transparency.auxiant.com/Trilogy/2022-08-01_Trilogy_in-network_rates.7z">Trilogy 7z</a></td></tr>
        <tr><td><a href="https://s3.us-east-2.amazonaws.com/transparency.auxiant.com/zelis/OON/Auxiant+OON+MRF+v1+062722.csv">Auxiant OON MRF v1 062722.csv</a></td></tr>
      </table>
      <p><a href="https://transparency.auxiant.com/directory-of-data-sources/">Return to list of networks...</a></p>
    </div><!-- .entry-content -->
    """

    links = discovery._parse_auxiant_page_links(html, base_url="https://transparency.auxiant.com/healthsmart/")

    assert links == [
        {
            "url": "https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ASA/machine-readable-transparency-in-coverage",
            "label": "Aetna hosted files",
            "target_kind": "external_landing",
            "hosting_platform": "aetna_health1",
        },
        {
            "url": "https://s3.us-east-2.amazonaws.com/transparency.auxiant.com/healthsmart/2022-07-01_HealthSmart-Payors-Organization_in-network-rates.json.zip",
            "label": "HealthSmart ZIP",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "zip",
        },
        {
            "url": "https://transparency.auxiant.com/wp-admin/admin-ajax.php?action=mk_file_folder_manager&cmd=file&target=fls2_Rmlyc3RDaG9pY2VIZWFsdGgvMjAyNjAxMDItcHJvdmlkZXJzMjAyNjAxMDJmY3BuLnppcA",
            "label": "20260102-providers20260102fcpn.zip",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "zip",
        },
        {
            "url": "https://s3.us-east-2.amazonaws.com/transparency.auxiant.com/Trilogy/2022-08-01_Trilogy_in-network_rates.7z",
            "label": "Trilogy 7z",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "7z",
        },
        {
            "url": "https://s3.us-east-2.amazonaws.com/transparency.auxiant.com/zelis/OON/Auxiant+OON+MRF+v1+062722.csv",
            "label": "Auxiant OON MRF v1 062722.csv",
            "target_kind": "file_reference",
            "target_file_type": "allowed-amounts",
            "container_format": None,
        },
    ]
    assert discovery._auxiant_file_type("https://example.com/MPI_HST_allowedamounts_20220901.zip") == "allowed-amounts"


def test_auxiant_direct_target_keeps_network_context_searchable():
    source = {"source_id": "source_auxiant", "payer_id": "payer_auxiant"}
    link = {
        "url": "https://s3.us-east-2.amazonaws.com/transparency.auxiant.com/FirstChoiceHealth/20250707-innrfppog07072025.zip",
        "label": "20250707-innrfppog07072025.zip",
        "target_file_type": "in-network",
        "container_format": "zip",
    }

    target = discovery._auxiant_direct_target(
        source,
        link,
        network_name="First Choice Health",
        page_url="https://transparency.auxiant.com/first-choice-health/",
        directory_url="https://transparency.auxiant.com/directory-of-data-sources/",
        resolver_type="auxiant_wordpress_directory",
    )

    assert target.label == "Auxiant - First Choice Health"
    assert target.resolved_from_url == "https://transparency.auxiant.com/first-choice-health/"
    assert target.metadata["resolver"] == "auxiant_wordpress_directory"
    assert target.metadata["target_kind"] == "file_reference"
    assert target.metadata["auxiant_network_name"] == "First Choice Health"
    assert target.metadata["file_label"] == "20250707-innrfppog07072025.zip"


def test_auxiant_landing_target_indexes_unresolved_network_pages():
    source = {"source_id": "source_auxiant", "payer_id": "payer_auxiant"}

    target = discovery._auxiant_landing_target(
        source,
        network_name="HealthLink",
        page_url="https://transparency.auxiant.com/healthlink/",
        directory_url="https://transparency.auxiant.com/directory-of-data-sources/",
        landing_url="https://www.healthlink.com/machine-readable-file/search/",
        resolver_type="auxiant_wordpress_directory",
        reason="external_landing_no_concrete_targets",
        landing_label="HealthLink hosted files",
        nested_error="no links found",
    )

    assert target.label == "Auxiant - HealthLink"
    assert target.url == "https://www.healthlink.com/machine-readable-file/search/"
    assert target.metadata["target_kind"] == "source_landing_page"
    assert target.metadata["target_file_type"] == "source-landing-page"
    assert target.metadata["auxiant_network_name"] == "HealthLink"
    assert target.metadata["landing_reason"] == "external_landing_no_concrete_targets"
    assert target.metadata["nested_error"] == "no links found"


def test_mymedicalshopper_url_helpers_and_employer_selector():
    assert discovery._mymedicalshopper_entity_slug_from_url("https://www.mymedicalshopper.com/mrf-search/varipro") == "varipro"
    assert (
        discovery._mymedicalshopper_employer_slug_from_url(
            "https://www.mymedicalshopper.com/mrf/electrical-workers-cofinity-varipro-77100"
        )
        == "electrical-workers-cofinity-varipro-77100"
    )
    assert discovery._mymedicalshopper_employer_selector("varipro", all_employers_searchable=True) == {
        "tpaSlug": "varipro",
        "status": "Enabled",
    }
    assert discovery._mymedicalshopper_employer_selector("varipro", all_employers_searchable=False) == {
        "tpaSlug": "varipro",
        "status": "Enabled",
        "machineReadableFiles.makeMRFsSearchable": True,
    }


def test_mymedicalshopper_sockjs_frame_and_publication_helpers():
    frame = "a" + json.dumps(
        [
            json.dumps({"msg": "added", "collection": "tabular_records", "id": "EntityMRFEmployers", "fields": {"ids": [{"$type": "oid", "$value": "61a"}], "recordsTotal": 5, "recordsFiltered": 1}}),
            json.dumps({"msg": "added", "collection": "employers", "id": "61a", "fields": {"name": "EWIF - HAP / First Health", "slug": "electrical-workers-cofinity-varipro-77100", "tpaSlug": "varipro", "status": "Enabled"}}),
        ]
    )

    messages = discovery._mymedicalshopper_sockjs_messages(frame)
    info = discovery._mymedicalshopper_tabular_info_from_messages(messages)
    employers = discovery._mymedicalshopper_employer_docs_from_messages(messages)

    assert info["ids"] == [{"$type": "oid", "$value": "61a"}]
    assert info["records_filtered"] == 1
    assert employers == [
        {
            "_id": "61a",
            "name": "EWIF - HAP / First Health",
            "slug": "electrical-workers-cofinity-varipro-77100",
            "tpaSlug": "varipro",
            "status": "Enabled",
        }
    ]


def test_mymedicalshopper_targets_keep_latest_generated_toc_per_plan():
    source = {"source_id": "source_varipro", "payer_id": "payer_varipro"}
    employer = {
        "slug": "electrical-workers-cofinity-varipro-77100",
        "name": "EWIF - HAP / First Health",
        "tpaSlug": "varipro",
        "groupId": "77100",
        "ein": "381393235",
    }
    generated = [
        {
            "planId": "4907",
            "planName": "EWIF In Network 01/01/2023",
            "mrfGeneratedInfo": [
                {"month": "2026-05-01", "mrfGenerated": True, "link": "https://mrf.mmsanalytics.com/2026-05-01_ewif_index.json"},
                {"month": "2026-06-01", "mrfGenerated": True, "link": "https://mrf.mmsanalytics.com/2026-06-01_ewif_index.json"},
                {"month": "2026-07-01", "mrfGenerated": False, "link": "https://mrf.mmsanalytics.com/2026-07-01_ewif_index.json"},
            ],
        },
        {
            "plan": {"id": "4907", "name": "EWIF In Network 01/01/2022"},
            "mrfGeneratedInfo": [
                {"month": "2026-06-01", "mrfGenerated": True, "link": "https://mrf.mmsanalytics.com/2026-06-01_ewif_2022_index.json"}
            ],
        },
    ]

    targets = discovery._mymedicalshopper_targets_from_generated(
        source,
        entity_slug="varipro",
        employer=employer,
        generated=generated,
        resolver_type="mymedicalshopper_talon_mrf",
        resolved_from_url="https://www.mymedicalshopper.com/mrf-search/varipro",
    )

    assert [target.url for target in targets] == [
        "https://mrf.mmsanalytics.com/2026-06-01_ewif_index.json",
        "https://mrf.mmsanalytics.com/2026-06-01_ewif_2022_index.json",
    ]
    assert targets[0].label == "EWIF - HAP / First Health - EWIF In Network 01/01/2023 - 2026-06-01"
    assert targets[0].metadata["target_file_type"] == "table-of-contents"
    assert targets[0].metadata["entity_slug"] == "varipro"
    assert targets[0].metadata["employer_slug"] == "electrical-workers-cofinity-varipro-77100"
    assert targets[0].metadata["history_month_count"] == 3


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


def test_healthsparq_public_params_preserve_filtered_fragment_query():
    params = discovery._healthsparq_public_params(
        "https://health1.meritain.com/app/public/#/one/insurerCode=MERITAIN_I&brandCode=MERITAINOVER/machine-readable-transparency-in-coverage?reportingEntityType=TPA_14445&lock=true"
    )

    assert params == {
        "insurerCode": "MERITAIN_I",
        "brandCode": "MERITAINOVER",
        "reportingEntityType": "TPA_14445",
        "lock": "true",
    }


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


def test_healthsparq_direct_metadata_url_skips_scoped_filters():
    resolver = discovery._source_config()["platform_resolvers"]["aetna_health1"]
    params = {"insurerCode": "MERITAIN_I", "brandCode": "MERITAINOVER", "reportingEntityType": "TPA_14445"}

    assert discovery._healthsparq_direct_metadata_url(resolver, params) is None


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
    <a href="/mrf/hmo_ha_hii_arkbluecross_index">No-extension TOC</a>
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
            "url": "https://example.com/mrf/hmo_ha_hii_arkbluecross_index",
            "label": "No-extension TOC",
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


def test_parse_html_mrf_links_extracts_embedded_escaped_toc_urls():
    html = r"""
    <script>
    window.__DATA__ = {
      "toc": "https:\/\/tic-mrf.regence.com\/mrf\/current\/2026-06-01_Regence%20BlueShield-ASO_index.json",
      "bcbsks": "https:\/\/mrf.secure.bcbsks.com\/api\/filedownloadhttptrigger?name=table-of-contents\u0026ext=json",
      "bcbsm": "https:\/\/bcbsm.sapphiremrfhub.com\/tocs\/current\/blue_cross_blue_shield_of_michigan"
    };
    </script>
    """

    targets = discovery._parse_html_mrf_links(html, base_url="https://example.com/transparency")

    assert targets == [
        {
            "url": "https://tic-mrf.regence.com/mrf/current/2026-06-01_Regence%20BlueShield-ASO_index.json",
            "label": "2026-06-01_Regence%20BlueShield-ASO_index.json",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "text",
        },
        {
            "url": "https://mrf.secure.bcbsks.com/api/filedownloadhttptrigger?name=table-of-contents&ext=json",
            "label": "filedownloadhttptrigger",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "text",
        },
        {
            "url": "https://bcbsm.sapphiremrfhub.com/tocs/current/blue_cross_blue_shield_of_michigan",
            "label": "blue_cross_blue_shield_of_michigan",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "text",
        },
    ]


def test_fetch_text_decode_response_body_handles_raw_gzip_json():
    payload = discovery._decode_response_body(gzip.compress(b'{"ok": true}'))

    assert payload == '{"ok": true}'


def test_direct_toc_url_accepts_no_extension_mrf_index():
    assert discovery._looks_direct_toc_url("https://mrf.example.com/mrf/hmo_ha_hii_example_index")
    assert discovery._looks_direct_toc_url("https://bcbsm.sapphiremrfhub.com/tocs/current/blue_cross_blue_shield_of_michigan")
    assert discovery._looks_direct_toc_url("https://www.bluecrossvt.org/documents/toc-json")
    assert discovery._looks_direct_toc_url("https://mrf.secure.bcbsks.com/api/filedownloadhttptrigger?name=table-of-contents&ext=json")


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


def test_bcbs_asomrf_filelist_html_extracts_filelist_url():
    html = """<script>var filelist = "/content/dam/bcbs/mrf/si-filelist.json";</script>"""

    urls = discovery._bcbs_asomrf_filelist_urls_from_html(html, base_url="https://www.bcbsil.com/asomrf?EIN=260241222")

    assert urls == ["https://www.bcbsil.com/content/dam/bcbs/mrf/si-filelist.json"]


def test_parse_bcbs_asomrf_filelist_targets_expands_index_urls():
    source = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "BCBS Illinois"}
    payload = [
        {
            "last_update_date": "2026-05-21",
            "state": "IL",
            "url": "https://app.example/toc/2026-05-21_Blue-Cross-and-Blue-Shield-of-Illinois_260241222_index.json",
            "name": "2026-05-21_Blue-Cross-and-Blue-Shield-of-Illinois_260241222_index",
            "ein": "260241222",
        },
        {"url": "https://app.example/body/in-network-rates.json.gz", "name": "skip body file"},
    ]

    [target] = discovery._parse_bcbs_asomrf_filelist_targets(
        payload,
        filelist_url="https://www.bcbsil.com/content/dam/bcbs/mrf/si-filelist.json",
        source=source,
        resolver={"toc_max_bytes": 12345},
    )

    assert target.url == "https://app.example/toc/2026-05-21_Blue-Cross-and-Blue-Shield-of-Illinois_260241222_index.json"
    assert target.resolved_from_url == "https://www.bcbsil.com/content/dam/bcbs/mrf/si-filelist.json"
    assert target.metadata["resolver"] == "bcbs_asomrf_filelist"
    assert target.metadata["state"] == "IL"
    assert target.metadata["ein"] == "260241222"
    assert target.metadata["target_max_bytes"] == 12345


def test_parse_bcbs_asomrf_filelist_targets_applies_state_balanced_limit():
    source = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "BCBS Illinois"}
    payload = [
        {
            "state": state,
            "url": f"https://app.example/toc/2026-05-21_Blue-Cross-and-Blue-Shield-of-{state}_{index}_index.json",
            "name": f"2026-05-21_Blue-Cross-and-Blue-Shield-of-{state}_{index}_index",
            "ein": str(index),
        }
        for state in ("TX", "TX", "TX", "IL", "IL", "OK")
        for index in range(2)
    ]

    targets = discovery._parse_bcbs_asomrf_filelist_targets(
        payload,
        filelist_url="https://www.bcbsil.com/content/dam/bcbs/mrf/si-filelist.json",
        source=source,
        resolver={"max_targets": 5},
    )

    assert [target.metadata["state"] for target in targets] == ["TX", "IL", "OK", "TX", "IL"]


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


def test_file_reference_target_rows_preserve_plan_info_for_client_indexing():
    source = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "Lucent Health"}
    target = discovery.CrawlTarget(
        source=source,
        url="https://mrf.healthcarebluebook.com/Lucent/350380",
        label="Lucent Health 042171239",
        resolved_from_url="https://mrf.healthcarebluebook.com/Lucent",
        metadata={
            "resolver": "healthcarebluebook_mrf",
            "target_kind": "file_reference",
            "target_file_type": "allowed-amounts",
            "source_format": "zip",
            "plan_info": [
                {
                    "plan_id": "042171239",
                    "plan_id_type": "ein",
                    "plan_market_type": "group",
                    "plan_name": "Lucent Health 042171239",
                }
            ],
        },
    )

    file_row = discovery._toc_target_file_row(target)
    [plan_row] = discovery._plan_rows_from_target_metadata(target)

    assert file_row["plan_ids"] == ["042171239"]
    assert file_row["market_types"] == ["group"]
    assert file_row["metadata_json"]["source_format"] == "zip"
    assert plan_row["plan_id"] == "042171239"
    assert plan_row["plan_id_type"] == "ein"
    assert plan_row["reporting_entity_name"] == "Lucent Health"


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


def test_toc_rows_truncate_long_schema_version(monkeypatch):
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
                plan_info=({"plan_id": "123", "plan_market_type": "group", "plan_name": "Plan A"},),
            )
        ],
    )

    _, file_rows = discovery._toc_rows_from_content(
        {"source_id": "source_1"},
        "https://example.com/index.json",
        {"version": "3.5.5 f501aab30e8114503c6248f178858c9a27ba9c14"},
    )

    assert file_rows[0]["schema_version"] == "3.5.5 f501aab30e8114503c6248f178"


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


@pytest.mark.asyncio
async def test_direct_discovery_run_emits_import_control_visible_state(monkeypatch):
    pushed = []
    events = []
    progress = []
    flushed = []

    async def fake_load_candidates(_provider, *, test_mode, limit):
        assert test_mode is True
        assert limit == 1
        return [
            discovery.SourceCandidate(
                payer_name="Example Payer",
                provider="master-list",
                index_url="https://example.com/index.json",
                status="active",
            )
        ]

    async def fake_push_objects(rows, model, *, rewrite, use_copy):
        pushed.append((model, rows, rewrite, use_copy))

    async def fake_store_candidates(_candidates):
        return (
            [{"payer_id": "payer_1"}],
            [{"source_id": "source_1", "index_url": "https://example.com/index.json"}],
        )

    async def fake_noop(*_args, **_kwargs):
        return None

    async def fake_flush(timeout_seconds):
        flushed.append(timeout_seconds)

    monkeypatch.setattr(discovery, "_load_candidates", fake_load_candidates)
    monkeypatch.setattr(discovery, "init_db", fake_noop)
    monkeypatch.setattr(discovery, "ensure_database", fake_noop)
    monkeypatch.setattr(discovery, "_ensure_catalog_tables", fake_noop)
    monkeypatch.setattr(discovery, "push_objects", fake_push_objects)
    monkeypatch.setattr(discovery, "_store_candidates", fake_store_candidates)
    monkeypatch.setattr(discovery, "enqueue_status_event", lambda payload: events.append(payload))
    monkeypatch.setattr(discovery, "enqueue_live_progress", lambda **payload: progress.append(payload))
    monkeypatch.setattr(discovery, "flush_status_events", fake_flush)

    result = await discovery.main(test_mode=True, provider="master-list", limit=1)

    control_run_id = result["crawl_run_id"]
    crawl_rows = [rows[0] for model, rows, _rewrite, _use_copy in pushed if model is discovery.MRFCrawlRun]
    assert control_run_id.startswith("mrfcrawl_")
    assert [event["status"] for event in events] == ["running", "succeeded"]
    assert all(event["run_id"] == control_run_id for event in events)
    assert events[0]["importer"] == "mrf-source-discovery"
    assert events[0]["triggered_by"] == "direct_cli"
    assert events[0]["params"]["provider"] == "master-list"
    assert events[1]["metrics"]["crawl_run_id"] == control_run_id
    assert events[1]["metrics"]["crawl_status"] == "succeeded"
    assert events[1]["metrics"]["sources"] == 1
    assert [row["run_id"] for row in crawl_rows] == [control_run_id, control_run_id]
    assert [item["run_id"] for item in progress] == [control_run_id, control_run_id, control_run_id]
    assert flushed == [1.0]
