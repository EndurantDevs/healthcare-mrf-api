# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import gzip
import importlib
import io
import json
import types
import zipfile
from unittest.mock import AsyncMock

import pytest

discovery = importlib.import_module("process.mrf_source_discovery")


class _FakeChunkedContent:
    def __init__(self, body: bytes):
        self._body = body
        self._offset = 0

    async def iter_chunked(self, _size):
        yield self._body

    async def read(self, size=-1):
        if size == 0:
            return b""
        if self._offset >= len(self._body):
            return b""
        if size is None or size < 0:
            end = len(self._body)
        else:
            end = min(self._offset + size, len(self._body))
        chunk = self._body[self._offset : end]
        self._offset = end
        return chunk


class _FakeFetchResponse:
    def __init__(
        self,
        *,
        status: int,
        body: bytes,
        content_type: str,
        url: str = "https://example.test/toc.json",
    ):
        self.status = status
        self.headers = {"Content-Type": content_type}
        self.content = _FakeChunkedContent(body)
        self.charset = "utf-8"
        self.url = url

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    def release(self):
        return None


class _RecordingBrowserFallbackSession:
    observed_user_agents: list[str] = []
    response = _FakeFetchResponse(
        status=200,
        body=b'{"ok": true}',
        content_type="application/json",
    )

    def __init__(self, *, headers, **_kwargs):
        self.observed_user_agents.append(headers["User-Agent"])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    def get(self, _url, *, allow_redirects):
        assert allow_redirects is True
        return self.response


def test_discovery_result_exposes_public_catalog_metrics():
    result = discovery.DiscoveryResult(providers=["master-list"]).as_dict()

    assert set(result) == {
        "catalog_export_version",
        "providers",
        "candidates",
        "payers",
        "sources",
        "urls_checked",
        "plans",
        "files",
        "files_probed",
        "file_probe_ok",
        "crawl_run_id",
        "errors",
        "process_workers",
    }
    assert result["catalog_export_version"] == 1
    assert result["process_workers"] == 1


def test_discovery_command_exposes_public_options():
    process_package = importlib.import_module("process")

    assert [
        parameter.name
        for parameter in process_package.mrf_source_discovery_command.params
    ] == [
        "provider",
        "limit",
        "source_entity_types",
        "source_payer_query",
        "dry_run",
        "check_urls",
        "crawl",
        "probe_files",
        "file_probe_limit",
        "file_probe_types",
        "file_probe_entity_types",
        "file_probe_payer_query",
        "max_toc_bytes",
        "concurrency",
        "crawl_target_limit",
        "test",
    ]


def test_source_urls_are_loaded_from_registry_file():
    """Verify this source-discovery regression contract."""
    config = discovery._source_config()

    assert config["default_providers"] == ["master-list"]
    assert list(config["providers"]) == ["master-list"]
    assert (
        config["providers"]["master-list"]["path"] == "specs/mrf_payer_master_list.md"
    )
    assert (
        config["platform_resolvers"]["aetna_health1"]["type"]
        == "healthsparq_public_mrf"
    )
    assert (
        config["platform_resolvers"]["healthsparq"]["type"] == "healthsparq_public_mrf"
    )
    assert config["platform_resolvers"]["healthsparq"]["max_bytes"] >= 100 * 1024 * 1024
    assert (
        config["platform_resolvers"]["aetna_health1"]["max_bytes"] >= 100 * 1024 * 1024
    )
    assert (
        config["platform_resolvers"]["highmark_hmhs"]["type"] == "highmark_hmhs_script"
    )
    assert (
        config["platform_resolvers"]["bcbswy_hmhs_monthly_toc"]["type"]
        == "monthly_toc_templates"
    )
    assert config["platform_resolvers"]["insightba_html_mrf_links"]["type"] == "html_mrf_links"
    assert config["platform_resolvers"]["insightba_html_mrf_links"]["include_url_patterns"]
    assert config["platform_resolvers"]["midlandschoice_mrf"]["type"] == "midlandschoice_mrf"
    assert config["platform_resolvers"]["bcbswy_hmhs_monthly_toc"][
        "file_templates"
    ] == ["{month_start}_Blue_Cross_Blue_Shield_of_Wyoming_index.json"]
    assert config["platform_resolvers"]["sapphire"]["type"] == "sapphire_html_tocs"
    assert config["platform_resolvers"]["sapphire"]["max_static_queries"] == 8
    assert config["platform_resolvers"]["anthem_s3_mrf"]["type"] == "anthem_s3_mrf"
    assert config["platform_resolvers"]["kaiser_mrf_inventory"]["type"] == (
        "kaiser_monthly_inventory"
    )
    assert config["platform_resolvers"]["kaiser_hawaii_mrf_inventory"][
        "region_codes"
    ] == ["hi"]
    assert (
        config["platform_resolvers"]["hcsc_asomrf_landing"]["type"]
        == "hcsc_asomrf_landing"
    )
    assert (
        config["platform_resolvers"]["point32_azure_mrf_directory"]["type"]
        == "point32_azure_mrf_directory"
    )
    assert (
        config["platform_resolvers"]["html_delegated_mrf_links"]["type"]
        == "html_delegated_mrf_links"
    )
    assert config["platform_resolvers"]["html_mrf_links"]["type"] == "html_mrf_links"
    assert config["platform_resolvers"]["html_mrf_links"]["max_frames"] == 5
    assert (
        config["platform_resolvers"]["wordpress_elfinder_mrf_links"]["type"]
        == "wordpress_elfinder_mrf_links"
    )
    assert (
        config["platform_resolvers"]["wordpress_elfinder_mrf_links"][
            "max_directories"
        ]
        == 100
    )
    assert (
        config["platform_resolvers"]["avmed_html_mrf_links"]["type"]
        == "html_mrf_links"
    )
    assert config["platform_resolvers"]["avmed_html_mrf_links"]["max_directories"] == 20
    assert (
        config["platform_resolvers"]["blueadvantage_html_mrf_links"]["type"]
        == "html_mrf_links"
    )
    assert (
        config["platform_resolvers"]["blueadvantage_html_mrf_links"]["toc_max_bytes"]
        == 100 * 1024 * 1024
    )
    assert config["platform_resolvers"]["direct_mrf_body"]["type"] == "direct_mrf_body"
    assert config["platform_resolvers"]["direct_toc"]["type"] == "direct_toc"
    assert config["platform_resolvers"]["direct_toc"]["toc_max_bytes"] == 200 * 1024 * 1024
    assert (
        config["platform_resolvers"]["socrata_data_json_mrf_catalog"]["type"]
        == "socrata_data_json_mrf_catalog"
    )
    assert (
        config["platform_resolvers"]["socrata_data_json_mrf_catalog"][
            "latest_coverage_month_only"
        ]
        is True
    )
    assert (
        config["platform_resolvers"]["healthplan_html_mrf_links"]["type"]
        == "html_mrf_links"
    )
    assert (
        config["platform_resolvers"]["healthplan_html_mrf_links"]["max_directories"]
        == 20
    )
    assert (
        config["platform_resolvers"]["json_mrf_directory_links"]["type"]
        == "json_mrf_directory_links"
    )
    assert (
        config["platform_resolvers"]["healthspace_machine_readable_files"]["type"]
        == "healthspace_machine_readable_files"
    )
    assert (
        config["platform_resolvers"]["healthez_benefits_mrf"]["type"]
        == "healthez_benefits_mrf"
    )
    assert config["platform_resolvers"]["webtpa_mrf_api"]["type"] == "webtpa_mrf_api"
    assert (
        config["platform_resolvers"]["cmstic_file_info"]["type"] == "cmstic_file_info"
    )
    assert (
        config["platform_resolvers"]["cmstic_keyed_toc_redirect"]["type"]
        == "cmstic_keyed_toc_redirect"
    )
    assert (
        config["platform_resolvers"]["auxiant_wordpress"]["type"]
        == "auxiant_wordpress_directory"
    )
    assert (
        config["platform_resolvers"]["mymedicalshopper_talon"]["type"]
        == "mymedicalshopper_talon_mrf"
    )
    assert (
        config["platform_resolvers"]["html_mrf_links_mixed_directories"]["type"]
        == "html_mrf_links"
    )
    assert (
        config["platform_resolvers"]["html_mrf_links_mixed_directories"][
            "follow_directory_links_when_targets"
        ]
        is True
    )
    assert (
        config["platform_resolvers"]["humana_pct_file_list"]["type"]
        == "humana_pct_file_list"
    )
    assert config["platform_resolvers"]["fchn_payor_search"]["type"] == "fchn_payor_search"
    assert (
        config["platform_resolvers"]["mymedicalshopper_talon_bounded"]["type"]
        == "mymedicalshopper_talon_mrf"
    )
    assert (
        "mymedicalshopper_talon_bounded"
        in config["source_query_expansion_platforms"]
    )
    assert config["platform_resolvers"]["mymedicalshopper_talon_bounded"][
        "max_targets"
    ] == 20
    assert (
        config["platform_resolvers"]["viva_health_mrf"]["type"]
        == "viva_health_mrf"
    )
    assert config["platform_resolvers"]["viva_health_mrf"]["max_employer_links"] == 40
    assert (
        config["platform_resolvers"]["magnacare_transparency_mrf"]["type"]
        == "magnacare_transparency_mrf"
    )
    assert (
        config["platform_resolvers"]["asr_health_benefits"]["type"]
        == "asr_health_benefits_mrf"
    )
    assert (
        config["platform_resolvers"]["asr_health_benefits"]["seed_list"]
        == "asr_health_benefits_groups"
    )
    assert config["platform_resolvers"]["lacare_s3_listing"]["type"] == "s3_xml_listing"
    assert (
        config["platform_resolvers"]["providence_mrf_api"]["type"]
        == "providence_mrf_api"
    )
    assert (
        config["seed_lists"]["asr_health_benefits_groups"]["schema"]
        == "group_number_seed_v1"
    )
    assert (
        config["platform_resolvers"]["uhc_public_blobs"]["type"] == "uhc_blob_listing"
    )
    assert (
        config["platform_resolvers"]["uhc_provider_mrf_files"]["type"]
        == "uhc_provider_mrf_files"
    )
    assert (
        config["platform_resolvers"]["bcbsma_monthly_tocs"]["type"]
        == "bcbsma_monthly_tocs"
    )
    assert (
        config["platform_resolvers"]["bcbsmn_monthly_toc"]["type"]
        == "monthly_toc_templates"
    )
    assert (
        config["platform_resolvers"]["oscar_s3_monthly_toc"]["type"]
        == "monthly_toc_templates"
    )
    assert (
        config["platform_resolvers"]["uha_monthly_toc"]["type"]
        == "monthly_toc_templates"
    )
    assert (
        config["platform_resolvers"]["hmsa_monthly_toc"]["type"]
        == "monthly_toc_templates"
    )
    assert (
        config["platform_resolvers"]["sutter_health_plan_sitecore"]["type"]
        == "monthly_toc_templates"
    )
    assert (
        config["platform_resolvers"]["bcbsri_azure_mrf_listing"]["type"]
        == "azure_mrf_listing"
    )
    assert (
        config["platform_resolvers"]["hostedjson_azure_mrf_listing"]["type"]
        == "azure_mrf_listing"
    )
    assert (
        config["platform_resolvers"]["hostedjson_azure_mrf_listing"][
            "skip_toc_targets"
        ]
        is True
    )
    assert (
        config["platform_resolvers"]["ghcscw_azure_mrf_listing"]["type"]
        == "azure_mrf_listing"
    )
    assert (
        config["platform_resolvers"]["pacificsource_azure_mrf_listing"]["type"]
        == "azure_mrf_listing"
    )
    assert (
        "w2bipdmrfsa.blob.core.windows.net"
        in config["platform_resolvers"]["pacificsource_azure_mrf_listing"][
            "listing_urls"
        ][0]
    )
    assert (
        config["platform_resolvers"]["triples_mtt_api"]["type"] == "triples_mtt_api"
    )
    assert config["platform_resolvers"]["triples_mtt_api"]["latest_month_only"] is True
    assert (
        config["platform_resolvers"]["payercompass_mrf"]["type"] == "payercompass_mrf"
    )
    assert (
        config["platform_resolvers"]["healthsparq_direct_metadata"]["type"]
        == "healthsparq_direct_metadata"
    )
    assert (
        config["platform_resolvers"]["cigna_static_mrf_lookup"]["type"]
        == "cigna_static_mrf_lookup"
    )
    assert "cigna_static_mrf_lookup" in config["source_query_expansion_platforms"]
    assert (
        config["platform_resolvers"]["bcbs_global_solutions_mrf"]["type"]
        == "bcbs_global_solutions_mrf"
    )
    assert (
        config["platform_resolvers"]["meritain_mrf_search"]["type"]
        == "meritain_mrf_search"
    )
    assert (
        config["platform_resolvers"]["healthcarebluebook_mrf"]["type"]
        == "healthcarebluebook_mrf"
    )
    assert (
        config["platform_resolvers"]["html_mrf_with_healthcarebluebook"]["type"]
        == "html_mrf_with_healthcarebluebook"
    )
    assert (
        config["platform_resolvers"]["healthgram"]["type"] == "healthgram_network_index"
    )
    assert (
        config["platform_resolvers"]["github_repo_mrf"]["type"]
        == "github_repo_mrf_tree"
    )
    assert config["platform_resolvers"]["bcbs_asomrf"]["type"] == "bcbs_asomrf_filelist"
    assert config["platform_resolvers"]["bcbs_asomrf"]["max_targets"] == 250
    assert (
        config["platform_resolvers"]["aetna_health1"]["tenant_overrides"]["MERITAIN_I"]
        == "aetnacvs"
    )
    assert (
        config["platform_resolvers"]["ebms_caa_directory"]["type"]
        == "ebms_caa_directory"
    )


def test_classify_hosting_platform_recognizes_public_adapter_pages():
    """Verify this source-discovery regression contract."""
    assert (
        discovery.classify_hosting_platform(
            "https://www.anthem.com/machine-readable-file/search/"
        )
        == "anthem_s3_mrf"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://healthy.kaiserpermanente.org/hawaii/front-door/machine-readable"
        )
        == "kaiser_hawaii_mrf_inventory"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://healthy.kaiserpermanente.org/"
            "northern-california/front-door/machine-readable"
        )
        == "kaiser_mrf_inventory"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.hcsc.com/who-we-are/transparency-in-coverage"
        )
        == "hcsc_asomrf_landing"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.harvardpilgrim.org/public/machine-readable-files"
        )
        == "point32_azure_mrf_directory"
    )
    assert (
        discovery.classify_hosting_platform("https://www.pbaclaims.com/mrfs/")
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.healthnet.com/content/healthnet/en_us/transparency-files.html"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.novahealthcare.com/resources/mrf.html"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.hnas.com/digital-resources/machine-readable-files"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://capitalhealth.com/legal/transparency-in-coverage/"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://transparency.abadmin.com/")
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://mb.mrf.payercompass.com/")
        == "payercompass_mrf"
    )
    assert (
        discovery.classify_hosting_platform("https://api.midlandschoice.com/mrf")
        == "midlandschoice_mrf"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.centene.com/price-transparency-files.html"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.bcbsnd.com/employers/group-insurance-101/understanding-transparency-in-coverage-rule"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://alliantplans.com/json/pt/latestpt_nlc.html"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.amerihealthcaritas.com/price-transparency/"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://aultcare.com/price-transparency")
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.molinamarketplace.com/marketplace/oh/en-us/About/compinfo/PricingTransparency"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.hioscar.com/transparency-in-coverage-files/oscar"
        )
        == "oscar_s3_monthly_toc"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://thealliance.health/about-the-alliance/transparency-in-coverage-cms-9915-machine-readable-files/"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://alamedaalliance.org/about/pricing-transparency/"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.arkansasbluecross.com/interoperability/machine-readable-files"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.healthadvantage-hmo.com/interoperability/machine-readable-files"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.sharphealthplan.com/api-access-for-developers"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.sutterhealthplan.org/healthcare-cost-transparency"
        )
        == "sutter_health_plan_sitecore"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://group-health.com/price-transparency"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://sisconosurprise.com/ppo/phcs/index.html"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://sisconosurprise.com/ppo/hps/index.html"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://www.simplepayhealth.com/")
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://portal.90degreebenefits.com/MemberPortal/MachineReadableFiles"
        )
        == "healthspace_machine_readable_files"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://transparency-in-coverage.collectivehealth.com/index.html"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.modahealth.com/privacy-center/machine-readable-files.shtml"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://caa.ebms.com/")
        == "ebms_caa_directory"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://boonchapman-mrf.zakipointhealth.com/"
        )
        == "html_mrf_links_mixed_directories"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://talltreeadmin.com/machine-readable-files"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.ebam.com/machine-readable-files/"
        )
        == "wordpress_elfinder_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.motivhealth.com/machinereadablefiles/"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.cbabluevt.com/employer-resources/"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://tuition.ebpabenefits.com/employers/machine-readable-file-links"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://healthezbenefits.com/plandocuments/")
        == "healthez_benefits_mrf"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.sanfordhealthplan.com/transparency-in-coverage-rule"
        )
        == "html_mrf_links"
    )
    for url in (
        "https://www.optimahealth.com/transparency-in-coverage",
        "https://www.healthpartners.com/hp/legal-notices/disclosures/transparency/index.html",
        "https://www.mclarenhealthplan.org/mhp/transparency-in-coverage-and-no-surprises-act",
    ):
        assert discovery.classify_hosting_platform(url) == "html_mrf_links"
    assert (
        discovery.classify_hosting_platform(
            "https://insightba.net/transparency-in-coverage-resources/"
        )
        == "insightba_html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://deancare.healthsparq.com/healthsparq/public/#/one/"
            "insurerCode=MEDICAHEALTHPLANS_I&brandCode=DEAN&productCode=MRF/"
            "machine-readable-transparency-in-coverage"
        )
        == "healthsparq"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://mrf.healthsparq.com/example-egress.nophi.kyruushsq.com/prd/mrf/"
            "EXAMPLE_I/EXAMPLE/latest_metadata.json"
        )
        == "healthsparq_direct_metadata"
    )
    for url in (
        "https://www.deancare.com/helpful-links/transparency-in-coverage",
        "https://www.avmed.org/transparency-in-coverage",
        "https://www.vivahealth.com/transparency-in-coverage/",
    ):
        assert discovery.classify_hosting_platform(url) == "custom"
    for url in (
        "https://www.vivahealth.com/mrf/",
        "https://www.vivahealth.com/mrf/employers/",
        "https://www.vivahealth.com/files/mrf/viva-health-commercial-in-network-rates",
        "https://www.vivahealth.com/files/mrf/viva-health-commercial-out-of-network-rates",
    ):
        assert discovery.classify_hosting_platform(url) == "viva_health_mrf"
    assert (
        discovery.classify_hosting_platform(
            "https://www.scrippshealthplan.com/transparency-in-coverage#component_8a5f07e7c7"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://chorushealthplans.org/ifp/past-ifp-member-resources/transparency-in-coverage"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://uhealthplan.utah.edu/machine-readable-data"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://www.healthplan.org/multiplan_mrfs")
        == "healthplan_html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.healthplan.org/machine_readable_files"
        )
        == "healthplan_html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://www.healthplan.org/thp_mrfs")
        == "healthplan_html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://www.healthplan.org/amps_mrfs")
        == "healthplan_html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.westernhealth.com/mywha/price-transparency/"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://www.firstchoicenext.com/json")
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://www.amerihealthcaritasnext.com/json")
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://www.avmed.org/en/for-developers")
        == "avmed_html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.mvphealthcare.com/developers/machine-readable-files"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.upmchealthplan.com/transparency-in-coverage/mrf/"
        )
        == "upmc_monthly_toc"
    )
    assert (
        discovery.classify_hosting_platform("https://www.bcbsal.org/web/tcr")
        == "bcbsal_html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://d3oz7y1cwsecds.cloudfront.net/member-prod/bcbsal"
        )
        == "direct_toc"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.hmaa.com/wp-content/uploads/2022/06/MRF_HMAA.zip"
        )
        == "direct_toc"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://example.test/acadirectory/97176/97176Index.json"
        )
        is None
    )
    assert (
        discovery.classify_hosting_platform(
            "https://mydental.guardianlife.com/secure/json/index.json"
        )
        is None
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.hmsa.com/help-center/transparency-in-coverage-machine-readable-files/"
        )
        == "hmsa_monthly_toc"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.bluecrossmn.com/transparency-coverage-machine-readable-files"
        )
        == "bcbsmn_monthly_toc"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.uhahealth.com/important-notices/transparency-in-coverage-and-no-surprises-act-overview"
        )
        == "uha_monthly_toc"
    )
    assert (
        discovery.classify_hosting_platform("https://app.uhahealth.com/mrf/")
        == "uha_monthly_toc"
    )
    assert (
        discovery.classify_hosting_platform("https://www.bcbsri.com/developers")
        == "bcbsri_azure_mrf_listing"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.bswhealthplan.com/transparency"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://ebu.intermountainhealthcare.org/selecthealth/transparencyincoverage/"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://www.pehp.org/machinereadablefiles")
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://curative.com/transparency-in-coverage-rates"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.groupadministrators.com/machinereadablefiles/"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://mrf.mmsanalytics.com/medcost/")
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://alliedbenefit.sapphiremrfhub.com/")
        == "sapphire"
    )
    assert (
        discovery.classify_hosting_platform("https://www.bcbst.com/tcr")
        == "json_mrf_directory_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://cdn.example.test/tcr/aso_directory.json"
        )
        == "json_mrf_directory_links"
    )
    assert (
        discovery.classify_hosting_platform("https://price-transparency.webtpa.com/")
        == "webtpa_mrf_api"
    )
    assert (
        discovery.classify_hosting_platform("https://providermrf.uhc.com/IFP")
        == "uhc_provider_mrf_files"
    )
    assert (
        discovery.classify_hosting_platform("https://www.ibx.com/cmstic/?brand=qcc")
        == "cmstic_file_info"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.ibx.com/transparency-in-coverage/821410?key=abc123"
        )
        == "cmstic_keyed_toc_redirect"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.reliancematrix.com/privacy-notice/transparency-in-coverage"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.amerihealth.com/developer-resources/index.html"
        )
        == "cmstic_file_info"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://content.eyemedvisioncare.com/EyeMed_HCSC/eyemed_in-network-rates.json"
        )
        == "direct_mrf_body"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://apatpa.com/disclosures-terms-conditions-privacy-policy-american-plan-administrators/"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://caa.imagine360.com/IMAGINE360%20SERVICES%20LLC/index.html"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://developers.humana.com/cost-transparency")
        == "humana_pct_file_list"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://developers.humana.com/syntheticdata/Resource/PCTFilesList?fileType=innetwork"
        )
        == "humana_pct_file_list"
    )
    assert (
        discovery.classify_hosting_platform("https://www.fchn.com/machine-readable-files")
        == "fchn_payor_search"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.fchn.com/PayorSearch/Home/PayorDetail/64647"
        )
        == "fchn_payor_search"
    )
    assert (
        discovery.classify_hosting_platform("https://ehptransparency.org/")
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.ucare.org/legal-notices/transparency-in-coverage"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://stmercycaremrf.z14.web.core.windows.net/in_network.html"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://stmercycaremrf.z14.web.core.windows.net/OON.html"
        )
        == "html_mrf_links"
    )


def test_midlandschoice_resolver_preserves_network_labels():
    """Verify this source-discovery regression contract."""
    html = """
    <table>
      <tr>
        <th>Network</th><th>Name</th><th>File Name</th><th>File Type</th><th>Download</th>
      </tr>
      <tr>
        <td>A1</td>
        <td>Network Alpha</td>
        <td>alpha_in-network-rates.json.gz</td>
        <td>In Network Rates</td>
        <td>
          <a href="/api/v1/fileshare/download?filename=alpha_in-network-rates.json.gz">Download</a>
        </td>
      </tr>
      <tr>
        <td>B2</td>
        <td>Network Beta</td>
        <td>beta_in-network-rates.json.gz</td>
        <td>In Network Rates</td>
        <td>
          <a href="/api/v1/fileshare/download?filename=beta_in-network-rates.json.gz">Download</a>
        </td>
      </tr>
    </table>
    """
    catalog_source_dict = {"source_id": "src-midlands", "display_name": "Synthetic Midlands"}

    network_targets = discovery._parse_midlandschoice_mrf_rows(
        html, base_url="https://api.midlandschoice.com/mrf"
    )

    assert [network_target["network_code"] for network_target in network_targets] == ["A1", "B2"]
    assert [network_target["network_name"] for network_target in network_targets] == [
        "Network Alpha",
        "Network Beta",
    ]
    assert network_targets[0]["url"].startswith(
        "https://api.midlandschoice.com/api/v1/fileshare/download?"
    )

    async def fake_fetch_text(url, *, max_bytes, session):
        assert url == "https://api.midlandschoice.com/mrf"
        assert max_bytes >= 1024
        assert session is None
        return html

    original = discovery._fetch_text
    discovery._fetch_text = fake_fetch_text
    try:
        resolved = asyncio.run(
            discovery._resolve_midlandschoice_mrf(
                catalog_source_dict,
                "https://api.midlandschoice.com/mrf",
                {"type": "midlandschoice_mrf"},
                None,
            )
        )
    finally:
        discovery._fetch_text = original

    assert [crawl_target.label for crawl_target in resolved] == ["Network Alpha", "Network Beta"]
    assert resolved[0].metadata["network_code"] == "A1"
    assert resolved[0].metadata["target_kind"] == "file_reference"
    assert resolved[0].metadata["target_file_type"] == "in-network"
    assert resolved[0].metadata["plan_info"][0]["plan_id"] == "A1"
    assert resolved[0].metadata["plan_info"][0]["plan_id_type"] == "network_code"
    assert resolved[0].metadata["plan_info"][0]["plan_name"] == "Network Alpha"


def test_wordpress_elfinder_config_parser_extracts_ajax_metadata():
    html = """
    <div id="wp_file_manager_front123"></div>
    <script>
      jQuery("#wp_file_manager_front123").elfinder({
        url: "https:\\/\\/example.test\\/wp-admin\\/admin-ajax.php?action=mk_file_folder_manager_shortcode",
        customData: {
          _wpnonce: "nonce-123",
          data_key: "key-123",
        }
      });
    </script>
    """

    configs = discovery._wordpress_elfinder_configs_from_html(
        html, base_url="https://example.test/machine-readable-files/"
    )

    assert configs == [
        {
            "url": "https://example.test/wp-admin/admin-ajax.php?action=mk_file_folder_manager_shortcode",
            "data_key": "key-123",
            "nonce": "nonce-123",
            "file_manager_id": "123",
        }
    ]


def test_wordpress_elfinder_hash_path_decodes_file_path():
    assert (
        discovery._wordpress_elfinder_hash_path(
            "l1_Y2xpZW50X2EvMjAyNi0wNy0wMV9leGFtcGxlX2FsbG93ZWQtYW1vdW50cy5jc3Y"
        )
        == "client_a/2026-07-01_example_allowed-amounts.csv"
    )


@pytest.mark.asyncio
async def test_wordpress_elfinder_resolver_opens_directory_targets(monkeypatch):
    """WordPress elFinder pages can expose MRF files only after opening a child folder."""
    discovery_source_dict = {
        "source_id": "source_example_elfinder",
        "display_name": "Example elFinder",
        "hosting_platform": "wordpress_elfinder_mrf_links",
    }
    page_html = """
    <div id="wp_file_manager_front123"></div>
    <script>
      jQuery("#wp_file_manager_front123").elfinder({
        url: "https://example.test/wp-admin/admin-ajax.php?action=mk_file_folder_manager_shortcode",
        customData: {
          _wpnonce: "nonce-123",
          data_key: "key-123",
        }
      });
    </script>
    """
    directory_hash = "l1_Y2xpZW50X2E"
    file_hash = (
        "l1_Y2xpZW50X2EvMjAyNi0wNy0wMV9leGFtcGxlX2FsbG93ZWQtYW1vdW50cy5jc3Y"
    )
    form_calls = []

    async def fake_fetch_text(url, *, max_bytes, session):
        assert url == "https://example.test/machine-readable-files/"
        assert max_bytes == 1024
        assert session.headers["User-Agent"].startswith("Mozilla/5.0")
        return page_html

    async def fake_post_form_json_value(url, form_fields, *, max_bytes, session):
        form_calls.append(dict(form_fields))
        assert (
            url
            == "https://example.test/wp-admin/admin-ajax.php?action=mk_file_folder_manager_shortcode"
        )
        assert max_bytes == 2048
        assert session.headers["User-Agent"].startswith("Mozilla/5.0")
        assert form_fields["_wpnonce"] == "nonce-123"
        assert form_fields["data_key"] == "key-123"
        if form_fields.get("init") == "1":
            return {
                "cwd": {
                    "options": {
                        "url": "https://files.example.test/root/",
                    }
                },
                "files": [
                    {
                        "mime": "directory",
                        "hash": "l1_Lw",
                        "name": "root",
                        "phash": "",
                    },
                    {
                        "mime": "directory",
                        "hash": directory_hash,
                        "name": "client_a",
                        "phash": "l1_Lw",
                    },
                ],
            }
        assert form_fields["target"] == directory_hash
        return {
            "files": [
                {
                    "mime": "text/csv",
                    "size": "526",
                    "hash": file_hash,
                    "name": "2026-07-01_example_allowed-amounts.csv",
                    "phash": directory_hash,
                }
            ]
        }

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(
        discovery, "_post_form_json_value", fake_post_form_json_value
    )

    crawl_targets = await discovery._resolve_wordpress_elfinder_mrf_links(
        discovery_source_dict,
        "https://example.test/machine-readable-files/",
        {
            "type": "wordpress_elfinder_mrf_links",
            "max_bytes": 1024,
            "ajax_max_bytes": 2048,
            "max_targets": 5,
        },
        session=object(),
    )

    assert [call["cmd"] for call in form_calls] == ["open", "open"]
    assert crawl_targets[0].url == (
        "https://files.example.test/root/client_a/"
        "2026-07-01_example_allowed-amounts.csv"
    )
    assert crawl_targets[0].label == "2026-07-01_example_allowed-amounts.csv"
    assert crawl_targets[0].metadata["resolver"] == "wordpress_elfinder_mrf_links"
    assert crawl_targets[0].metadata["target_kind"] == "file_reference"
    assert crawl_targets[0].metadata["target_file_type"] == "allowed-amounts"
    assert crawl_targets[0].metadata["wordpress_elfinder_path"] == (
        "client_a/2026-07-01_example_allowed-amounts.csv"
    )
    assert crawl_targets[0].metadata["wordpress_elfinder_file_manager_id"] == "123"


def test_discovery_tls_override_is_host_scoped(monkeypatch):
    monkeypatch.delenv(discovery.INCOMPLETE_TLS_CHAIN_HOSTS_ENV, raising=False)

    assert discovery._request_ssl_kwargs("https://api.midlandschoice.com/mrf") == {
        "ssl": False
    }
    assert discovery._request_ssl_kwargs("https://example.com/mrf") == {}

    monkeypatch.setenv(discovery.INCOMPLETE_TLS_CHAIN_HOSTS_ENV, "example.com")
    assert discovery._request_ssl_kwargs("https://api.midlandschoice.com/mrf") == {}
    assert discovery._request_ssl_kwargs("https://example.com/mrf") == {"ssl": False}


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

    assert [item.payer_name for item in candidates] == [
        "Cigna",
        "Humana",
        "BCBS Alabama",
    ]
    assert candidates[0].parent_group == "Cigna"
    assert candidates[2].entity_type == "blue"
    assert candidates[2].hosting_platform == "bcbsal_html_mrf_links"
    assert candidates[0].source_coverage == ()
    assert candidates[0].raw_payload["notes"] == "public compliance page"


def test_parse_master_list_preserves_public_aliases_for_payercompass_source():
    markdown = """
## Public TPA rows
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| MedBen | tpa | https://mb.mrf.payercompass.com/ | public Zelis/PayerCompass MRF repository; aliases: Medical Benefits Administrators |
"""

    [candidate] = discovery.parse_master_list(markdown)

    assert candidate.payer_name == "MedBen"
    assert candidate.entity_type == "tpa"
    assert candidate.hosting_platform == "payercompass_mrf"
    assert candidate.aliases == ("Medical Benefits Administrators",)


def test_parse_master_list_preserves_benefit_lines_for_dental_vision_source():
    markdown = """
## Dental and vision sources
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| Example Dental Vision | dental | https://example.test/mrf | public MRF page; benefit lines: dental, vision; aliases: Example DV |
"""

    [candidate] = discovery.parse_master_list(markdown)

    assert candidate.payer_name == "Example Dental Vision"
    assert candidate.entity_type == "dental"
    assert candidate.benefit_lines == ("dental", "vision")
    assert candidate.aliases == ("Example DV",)


def test_parse_master_list_normalizes_pediatric_dental_benefit_line():
    markdown = """
## Public regional sources
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| Example Regional Plan | regional | https://example.test/json | public JSON directory; benefit lines: medical, pediatric dental; aliases: Example Regional |
"""

    [candidate] = discovery.parse_master_list(markdown)

    assert candidate.payer_name == "Example Regional Plan"
    assert candidate.benefit_lines == ("medical", "dental")
    assert candidate.aliases == ("Example Regional",)


def test_parse_master_list_preserves_coverage_evidence_metadata():
    markdown = """
## Public employer evidence
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| Example Packaging Benefits | group | https://benefits.example.test/find-a-provider | public employer benefits page; source tier: coverage_evidence; target payer query: Example Packaging; benefit lines: medical; source coverage: Example Packaging medical choices; vendor names: Example Virtual Health; network names: Example National PPO; plan names: Example Guided Health Plan; aliases: Example Packaging |
"""

    [candidate] = discovery.parse_master_list(markdown)

    assert candidate.payer_name == "Example Packaging Benefits"
    assert candidate.entity_type == "group"
    assert candidate.source_tier == "coverage_evidence"
    assert candidate.benefit_lines == ("medical",)
    assert candidate.source_coverage == ("Example Packaging medical choices",)
    assert candidate.vendor_names == ("Example Virtual Health",)
    assert candidate.network_names == ("Example National PPO",)
    assert candidate.plan_names == ("Example Guided Health Plan",)
    assert candidate.aliases == ("Example Packaging",)
    assert candidate.raw_payload["target_payer_query"] == "Example Packaging"


def test_parse_master_list_defaults_url_less_rows_to_coverage_evidence():
    markdown = """
## Public ancillary placeholders
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| Example Dental Placeholder | dental | - | needs official public pricing MRF TOC or landing URL; benefit lines: dental; aliases: Example Dental |
| Example Dental Explicit | dental | - | source tier: mrf_importable; benefit lines: dental; aliases: Example Dental Explicit |
| Example Dental Source | dental | https://example.test/toc/index.json | public pricing TOC; benefit lines: dental; aliases: Example Dental Source |
"""

    by_name = {
        candidate.payer_name: candidate for candidate in discovery.parse_master_list(markdown)
    }

    assert by_name["Example Dental Placeholder"].index_url is None
    assert by_name["Example Dental Placeholder"].source_tier == "coverage_evidence"
    assert by_name["Example Dental Explicit"].index_url is None
    assert by_name["Example Dental Explicit"].source_tier == "mrf_importable"
    assert by_name["Example Dental Source"].index_url == "https://example.test/toc/index.json"
    assert by_name["Example Dental Source"].source_tier == "mrf_importable"


def test_parse_master_list_preserves_legacy_public_aliases_for_active_sources():
    markdown = """
## Public regional aliases
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| Example Current Plan | regional | https://example.test/current | curated source row; aliases: Example Legacy Plan, Example Legacy Health Care |
"""

    [candidate] = discovery.parse_master_list(markdown)

    assert candidate.payer_name == "Example Current Plan"
    assert candidate.aliases == (
        "Example Legacy Plan",
        "Example Legacy Health Care",
    )


def test_candidate_text_filter_matches_public_aliases():
    candidate = discovery.SourceCandidate(
        payer_name="The Health Plan",
        provider="master-list",
        index_url="https://www.healthplan.org/machine_readable_files",
        aliases=("The Health Plan of the Upper Ohio Valley", "THP"),
    )

    assert discovery._is_candidate_text_filter_match(
        candidate, entity_types=(), payer_query="Upper Ohio Valley"
    )
    assert discovery._is_candidate_text_filter_match(
        candidate, entity_types=(), payer_query="THP"
    )
    assert discovery._is_candidate_text_filter_match(
        candidate,
        entity_types=(),
        payer_query="Example Group - The Health Plan of the Upper Ohio Valley",
    )
    assert not discovery._is_candidate_text_filter_match(
        candidate, entity_types=(), payer_query="Example Group using THP"
    )
    assert not discovery._is_candidate_text_filter_match(
        candidate, entity_types=(), payer_query="Unrelated"
    )


def test_candidate_query_expansion_keeps_searchable_platform_sources():
    """Verify this source-discovery regression contract."""
    sapphire = discovery.SourceCandidate(
        payer_name="BCBS Louisiana",
        provider="master-list",
        index_url="https://bcbsla.sapphiremrfhub.com/",
        hosting_platform="sapphire",
    )
    aetna = discovery.SourceCandidate(
        payer_name="Aetna",
        provider="master-list",
        index_url=(
            "https://health1.aetna.com/app/public/#/one/"
            "insurerCode=AETNACVS_I&brandCode=ALICSI/"
            "machine-readable-transparency-in-coverage"
        ),
        hosting_platform="aetna_health1",
    )
    healthcarebluebook = discovery.SourceCandidate(
        payer_name="Example TPA",
        provider="master-list",
        index_url="https://mrf.healthcarebluebook.com/ExampleTPA",
        hosting_platform="healthcarebluebook_mrf",
    )
    payercompass = discovery.SourceCandidate(
        payer_name="Example Network",
        provider="master-list",
        index_url="https://example.mrf.payercompass.com/",
        hosting_platform="payercompass_mrf",
    )
    mymedicalshopper = discovery.SourceCandidate(
        payer_name="Example Talon TPA",
        provider="master-list",
        index_url="https://www.mymedicalshopper.com/mrf-search/example-tpa",
        hosting_platform="mymedicalshopper_talon",
    )
    bounded_mymedicalshopper = discovery.SourceCandidate(
        payer_name="Example Bounded Talon TPA",
        provider="master-list",
        index_url="https://www.mymedicalshopper.com/mrf-search/example-bounded-tpa",
        hosting_platform="mymedicalshopper_talon_bounded",
    )
    delegated_healthcarebluebook = discovery.SourceCandidate(
        payer_name="Example Delegated TPA",
        provider="master-list",
        index_url="https://example.test/transparency-in-coverage",
        hosting_platform="html_mrf_with_healthcarebluebook",
    )
    hcsc = discovery.SourceCandidate(
        payer_name="Example Blue",
        provider="master-list",
        index_url="https://example.test/asomrf",
        hosting_platform="hcsc_asomrf_landing",
    )
    html_mrf = discovery.SourceCandidate(
        payer_name="Example Regional",
        provider="master-list",
        index_url="https://example.test/mrf",
        hosting_platform="html_mrf_links",
    )
    bcbsal = discovery.SourceCandidate(
        payer_name="Example Alabama Blue",
        provider="master-list",
        index_url="https://example.test/web/tcr",
        hosting_platform="bcbsal_html_mrf_links",
    )
    blueadvantage = discovery.SourceCandidate(
        payer_name="Example Blue Advantage",
        provider="master-list",
        index_url="https://example.test/machine-readable-files",
        hosting_platform="blueadvantage_html_mrf_links",
    )
    anthem = discovery.SourceCandidate(
        payer_name="Example Anthem",
        provider="master-list",
        index_url="https://example.test/machine-readable-file/search/",
        hosting_platform="anthem_s3_mrf",
    )
    auxiant_directory = discovery.SourceCandidate(
        payer_name="Example Auxiant",
        provider="master-list",
        index_url="https://transparency.example.test/directory-of-data-sources/",
        hosting_platform="auxiant_wordpress",
    )
    direct_group = discovery.SourceCandidate(
        payer_name="Example Employer",
        provider="master-list",
        index_url="https://example.test/current_index.json",
        hosting_platform="direct_toc",
    )

    assert discovery._is_candidate_query_expansion_supported(sapphire)
    assert discovery._is_candidate_query_expansion_supported(aetna)
    assert discovery._is_candidate_query_expansion_supported(healthcarebluebook)
    assert discovery._is_candidate_query_expansion_supported(payercompass)
    assert discovery._is_candidate_query_expansion_supported(mymedicalshopper)
    assert discovery._is_candidate_query_expansion_supported(
        bounded_mymedicalshopper
    )
    assert discovery._is_candidate_query_expansion_supported(
        delegated_healthcarebluebook
    )
    assert discovery._is_candidate_query_expansion_supported(hcsc)
    assert discovery._is_candidate_query_expansion_supported(html_mrf)
    assert discovery._is_candidate_query_expansion_supported(bcbsal)
    assert discovery._is_candidate_query_expansion_supported(blueadvantage)
    assert discovery._is_candidate_query_expansion_supported(anthem)
    assert discovery._is_candidate_query_expansion_supported(auxiant_directory)
    assert not discovery._is_candidate_query_expansion_supported(direct_group)
    expanded = discovery._candidate_with_target_payer_query(
        sapphire, "Example Packaging"
    )
    assert expanded.raw_payload["target_payer_query"] == "Example Packaging"
    assert expanded.raw_payload["query_expansion_source"] is True


def _write_private_query_context_fixture(tmp_path):
    master_list_path = tmp_path / "master.md"
    master_list_path.write_text("# Synthetic catalog\n", encoding="utf-8")
    private_context_path = tmp_path / "private-context.csv"
    private_context_path.write_text(
        (
            "company_name,employer_aliases,ein,erisa_plan_number,medical_carriers,"
            "medical_policy_numbers,medical_carrier_regions,medical_lookup_types,"
            "evidence_plan_year,current_verification_status\n"
            "Example Manufacturing,Example Labs,12-3456789,501,Carrier One;Carrier Two,"
            "POL-1;POL-2,West;Pacific,employer_ein;toc_match,2025,current\n"
        ),
        encoding="utf-8",
    )
    source_config_path = tmp_path / "sources.json"
    source_config_path.write_text(
        json.dumps(
            {
                "providers": {
                    "master-list": {
                        "parser": "master-list",
                        "path": str(master_list_path),
                    }
                },
                "platform_resolvers": {
                    "synthetic_lookup": {"type": "html_mrf_links"}
                },
                "source_query_expansion_platforms": ["synthetic_lookup"],
            }
        ),
        encoding="utf-8",
    )
    return master_list_path, private_context_path, source_config_path


def _synthetic_private_query_carriers():
    return [
        discovery.SourceCandidate(
            payer_name=f"Synthetic Carrier {suffix}",
            provider="master-list",
            index_url=f"https://carrier-{suffix.lower()}.example.test/mrf",
            hosting_platform="synthetic_lookup",
            status="active",
            aliases=(f"Carrier {suffix}",),
            benefit_lines=("medical",),
        )
        for suffix in ("One", "Two")
    ]


@pytest.mark.asyncio
async def test_private_query_context_enriches_carriers_beyond_public_limit(
    tmp_path, monkeypatch
):
    """Mounted employer context must select every matching carrier despite a public limit."""
    _, private_context_path, source_config_path = (
        _write_private_query_context_fixture(tmp_path)
    )
    monkeypatch.setenv(discovery.SOURCE_CONFIG_ENV, str(source_config_path))
    monkeypatch.setenv(
        discovery.PRIVATE_QUERY_CONTEXT_PATHS_ENV, str(private_context_path)
    )
    monkeypatch.setattr(
        discovery,
        "parse_master_list",
        lambda _markdown: _synthetic_private_query_carriers(),
    )

    loaded_candidates = await discovery._load_candidates(
        "master-list", test_mode=True, limit=1
    )
    private_candidates = [
        candidate
        for candidate in loaded_candidates
        if candidate.raw_payload.get("private_query_context")
    ]

    assert len(private_candidates) == 2
    assert {candidate.payer_name for candidate in private_candidates} == {
        "Synthetic Carrier One",
        "Synthetic Carrier Two",
    }
    assert all(
        candidate.raw_payload["target_payer_query"] == "Example Manufacturing"
        for candidate in private_candidates
    )
    assert all(
        candidate.raw_payload["query_context_employer_ein"] == "12-3456789"
        for candidate in private_candidates
    )
    assert {
        candidate.raw_payload["query_context_carrier_policy_number"]
        for candidate in private_candidates
    } == {"POL-1", "POL-2"}


def test_private_query_context_supports_optional_mounts(tmp_path, monkeypatch):
    missing_context_path = tmp_path / "not-mounted.csv"
    monkeypatch.setenv(
        discovery.PRIVATE_QUERY_CONTEXT_PATHS_ENV,
        f"optional={missing_context_path}",
    )

    assert discovery._private_query_context_paths() == []

    monkeypatch.setenv(
        discovery.PRIVATE_QUERY_CONTEXT_PATHS_ENV, str(missing_context_path)
    )
    with pytest.raises(ValueError, match="private query context file does not exist"):
        discovery._private_query_context_paths()


def test_query_expansion_ranking_keeps_targeted_sources_before_generic_pages():
    generic_sources = [
        discovery.SourceCandidate(
            payer_name=f"Example Regional {index}",
            provider="master-list",
            index_url=f"https://example{index}.test/mrf",
            hosting_platform="html_mrf_links",
        )
        for index in range(30)
    ]
    talon = discovery.SourceCandidate(
        payer_name="Example Talon TPA",
        provider="master-list",
        index_url="https://www.mymedicalshopper.com/mrf-search/example-tpa",
        hosting_platform="mymedicalshopper_talon",
    )
    healthcarebluebook = discovery.SourceCandidate(
        payer_name="Example Directory TPA",
        provider="master-list",
        index_url="https://mrf.healthcarebluebook.com/ExampleTPA",
        hosting_platform="healthcarebluebook_mrf",
    )
    mixed = (
        generic_sources[:10]
        + [healthcarebluebook]
        + generic_sources[10:20]
        + [talon]
        + generic_sources[20:]
    )

    ranked = discovery._rank_query_expansion_candidates(mixed)

    assert [candidate.payer_name for candidate in ranked[:2]] == [
        "Example Talon TPA",
        "Example Directory TPA",
    ]
    assert [candidate.payer_name for candidate in ranked[2:5]] == [
        "Example Regional 0",
        "Example Regional 1",
        "Example Regional 2",
    ]


def test_query_expansion_sources_have_query_specific_source_identity():
    base = discovery.SourceCandidate(
        payer_name="Example Aetna",
        provider="master-list",
        index_url=(
            "https://health1.aetna.com/app/public/#/one/"
            "insurerCode=EXAMPLE&brandCode=ALICSI/"
            "machine-readable-transparency-in-coverage"
        ),
        hosting_platform="aetna_health1",
    )
    first = discovery._candidate_with_target_payer_query(base, "Example Packaging")
    second = discovery._candidate_with_target_payer_query(base, "Example Forge")

    _, first_row = discovery._candidate_to_rows(first, discovery._utc_now())
    _, second_row = discovery._candidate_to_rows(second, discovery._utc_now())

    assert first_row is not None
    assert second_row is not None
    assert first_row["source_id"] != second_row["source_id"]
    assert first_row["source_key"] != second_row["source_key"]
    assert first_row["metadata_json"]["target_payer_query"] == "Example Packaging"
    assert second_row["metadata_json"]["target_payer_query"] == "Example Forge"


def test_sapphire_query_slug_variants_probe_common_legal_suffixes():
    assert discovery._sapphire_query_slug_variants("Example Packaging") == [
        "example-packaging",
        "example_packaging",
        "example-packaging-inc",
        "example_packaging_inc",
        "example-packaging-llc",
        "example_packaging_llc",
        "example-packaging-corp",
        "example_packaging_corp",
        "example-packaging-co",
        "example_packaging_co",
    ]


def test_query_expansion_target_uses_query_as_company_label():
    target = discovery.CrawlTarget(
        source={"source_id": "src_aetna", "display_name": "Example Aetna"},
        url="https://example.test/2026-06-01_example-packaging-rates.json.gz",
        label="2026-06-01_example-packaging-rates.json.gz",
        resolved_from_url="https://example.test/latest_metadata.json",
        metadata={
            "plan_info": [
                {
                    "plan_id": "123",
                    "plan_id_type": "ein",
                    "plan_market_type": "group",
                    "plan_name": "Example Packaging HSA Choice POS II",
                }
            ]
        },
    )

    matched = discovery._matched_query_expansion_target(target, "Example Packaging")

    assert matched is not None
    assert matched.metadata["company_name"] == "Example Packaging"
    assert matched.metadata["employer_name"] == "Example Packaging"
    assert matched.metadata["target_payer_query"] == "Example Packaging"
    assert matched.metadata["query_expansion_match"] is True
    assert matched.metadata["plan_info"][0]["plan_name"] == "Example Packaging HSA Choice POS II"


def test_query_expansion_keeps_trusted_resolver_context_match():
    """A trusted identity lookup survives a differing current legal name."""
    crawl_target = discovery.CrawlTarget(
        source={"source_id": "source_example", "display_name": "Example Carrier"},
        url="https://files.example.test/current-employer-rates.json.gz",
        label="Current Employer Benefits Organization",
        metadata={
            "resolver": "example_employer_identity",
            "query_context_match": True,
            "query_context_match_scope": "employer_identity",
            "company_name": "Current Employer Benefits Organization",
        },
    )

    matched_target = discovery._matched_query_expansion_target(
        crawl_target,
        "Historical Example Employer",
    )

    assert matched_target is not None
    assert matched_target.metadata["company_name"] == (
        "Current Employer Benefits Organization"
    )
    assert matched_target.metadata["target_payer_query"] == (
        "Historical Example Employer"
    )
    assert matched_target.metadata["query_expansion_match"] is True
    assert matched_target.metadata["query_expansion_match_scope"] == (
        "resolver_context"
    )


def _synthetic_query_source(
    *,
    source_id="source_static",
    platform=None,
    index_url="https://example.test/machine-readable-files/",
):
    return {
        "source_id": source_id,
        "payer_id": f"payer_{source_id}",
        "display_name": "Example Carrier",
        "index_url": index_url,
        "metadata_json": {
            "raw": {
                "target_payer_query": "Sample Employer",
                "query_expansion_source": True,
            }
        },
        **({"hosting_platform": platform} if platform else {}),
    }


def _synthetic_toc_payload(*plan_specs):
    return {
        "reporting_entity_name": "Example Carrier",
        "reporting_entity_type": "Health Insurance Issuer",
        "version": "1.0.0",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": plan_name,
                        "plan_id_type": "ein",
                        "plan_id": plan_id,
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {
                        "description": f"{plan_id} rates",
                        "location": file_url,
                    }
                ],
            }
            for plan_name, plan_id, file_url in plan_specs
        ],
    }


def _static_lookup_scoped_zip_target(query_source_dict):
    [target] = discovery._parse_cigna_lookup_targets(
        {
            "mrfs": [
                {
                    "files": [
                        {
                            "file_name": "carrier-table-of-contents.zip",
                            "url": "https://example.test/download?id=carrier-index",
                        }
                    ]
                }
            ]
        },
        lookup_url=query_source_dict["index_url"],
        source_row_dict=query_source_dict,
        resolver={},
    )
    return discovery._toc_level_query_expansion_target(target, "Sample Employer")


def _synthetic_scoped_zip_members():
    return [
        (
            "matching-index.json",
            _synthetic_toc_payload(
                (
                    "Sample Employer Choice Plan",
                    "111111111",
                    "https://example.test/matching.json.gz",
                )
            ),
        ),
        (
            "unrelated-index.json",
            _synthetic_toc_payload(
                (
                    "Unrelated Employer Choice Plan",
                    "222222222",
                    "https://example.test/unrelated.json.gz",
                )
            ),
        ),
    ]


def test_query_expansion_toc_plan_fallback_is_resolver_scoped():
    query_source_dict = _synthetic_query_source()
    crawl_targets = [
        discovery.CrawlTarget(
            source=query_source_dict,
            url="https://example.test/static-index.json",
            label="Carrier-wide index",
            metadata={"resolver": "cigna_static_mrf_lookup"},
        ),
        discovery.CrawlTarget(
            source=query_source_dict,
            url="https://example.test/generic-index.json",
            label="Carrier-wide index",
            metadata={"resolver": "html_mrf_links"},
        ),
    ]

    [matched_target] = discovery._filter_query_expansion_targets(
        crawl_targets, "Sample Employer"
    )

    assert matched_target.url == "https://example.test/static-index.json"
    assert matched_target.metadata["query_expansion_match"] is True
    assert matched_target.metadata["query_expansion_match_scope"] == "toc_plan"
    assert matched_target.metadata["company_name"] == "Sample Employer"


def test_monthly_toc_query_expansion_scans_reporting_plan_identity():
    """Monthly carrier indexes must be searched inside reporting plans."""
    query_source_dict = _synthetic_query_source()
    crawl_target = discovery.CrawlTarget(
        source=query_source_dict,
        url="https://example.test/2026-07-01_carrier_index.json",
        label="Carrier-wide monthly index",
        metadata={"resolver": "monthly_toc_templates"},
    )

    [matched_target] = discovery._filter_query_expansion_targets(
        [crawl_target], "Sample Employer"
    )

    assert matched_target.url == crawl_target.url
    assert matched_target.metadata["query_expansion_match"] is True
    assert matched_target.metadata["query_expansion_match_scope"] == "toc_plan"


def _synthetic_scoped_query_source():
    """Build a source carrying fictional scoped employer identity variants."""
    query_source_dict = _synthetic_query_source()
    query_source_dict["metadata_json"]["raw"].update(
        {
            "target_payer_query": "Sample Holdings, LLC",
            "query_context_employer_name": "Sample Holdings, LLC",
            "query_context_employer_aliases": [
                "Sample Employer",
                "Sample Holdings",
            ],
            "query_context_employer_ein": "12-3456789",
            "query_context_carrier_policy_number": "POL-654321",
        }
    )
    return query_source_dict


def test_scoped_query_identities_include_alias_and_identifier_variants():
    """Scoped identity extraction preserves names and normalized identifiers."""
    assert discovery._source_payer_query_candidates(
        _synthetic_scoped_query_source()
    ) == (
        "Sample Holdings, LLC",
        "Sample Employer",
        "Sample Holdings",
        "12-3456789",
        "123456789",
        "POL-654321",
        "654321",
    )


def test_target_payer_query_reads_top_level_generic_context():
    """Generic top-level context must work without a nested raw payload."""
    assert discovery._source_target_payer_query(
        {"metadata_json": {"target_payer_query": "Example Employer"}}
    ) == "Example Employer"


def test_scoped_query_identities_preserve_legacy_context_rows():
    """Rows stored before the generic-key migration must remain searchable."""
    legacy_source_dict = {
        "metadata_json": {
            "raw": {
                "private_context_employer_name": "Legacy Example LLC",
                "private_context_employer_aliases": ["Legacy Example"],
                "private_context_employer_ein": "98-7654321",
                "private_context_carrier_policy_number": "POL-123456",
            }
        }
    }

    assert discovery._source_payer_query_candidates(legacy_source_dict) == (
        "Legacy Example LLC",
        "Legacy Example",
        "98-7654321",
        "987654321",
        "POL-123456",
        "123456",
    )


def test_scoped_query_identities_filter_alias_and_policy_plan_matches():
    """Scoped aliases and stable identifiers find differently labeled plans."""
    query_source_dict = _synthetic_scoped_query_source()
    toc_payload = _synthetic_toc_payload(
        (
            "Sample Employer Choice Plan",
            "111111111",
            "https://example.test/alias-match.json.gz",
        ),
        (
            "Regional Choice Plan",
            "654321",
            "https://example.test/policy-match.json.gz",
        ),
        (
            "Unrelated Employer Choice Plan",
            "222222222",
            "https://example.test/unrelated.json.gz",
        ),
    )

    plan_rows, file_rows = discovery._toc_rows_from_content(
        query_source_dict,
        "https://example.test/carrier-index.json",
        toc_payload,
        filter_to_target_query=True,
    )

    assert [plan_row["plan_id"] for plan_row in plan_rows] == [
        "111111111",
        "654321",
    ]
    assert [file_row["url"] for file_row in file_rows] == [
        "https://example.test/alias-match.json.gz",
        "https://example.test/policy-match.json.gz",
    ]
    assert all(
        file_row["metadata_json"]["plan_info"][0]["company_name"]
        == "Sample Holdings, LLC"
        for file_row in file_rows
    )


def test_toc_rows_filter_to_matching_reporting_plans_and_files():
    query_source_dict = _synthetic_query_source()
    toc_payload = _synthetic_toc_payload(
        (
            "Sample Employer Open Access Plus",
            "111111111",
            "https://example.test/matching-rates.json.gz",
        ),
        (
            "Unrelated Employer Open Access Plus",
            "222222222",
            "https://example.test/unrelated-rates.json.gz",
        ),
    )

    plan_rows, file_rows = discovery._toc_rows_from_content(
        query_source_dict,
        "https://example.test/carrier-index.json",
        toc_payload,
        filter_to_target_query=True,
    )

    assert [plan_row["plan_id"] for plan_row in plan_rows] == ["111111111"]
    assert [plan_row["plan_name"] for plan_row in plan_rows] == ["Open Access Plus"]
    assert [file_row["url"] for file_row in file_rows] == [
        "https://example.test/matching-rates.json.gz"
    ]
    assert file_rows[0]["plan_ids"] == ["111111111"]
    assert file_rows[0]["metadata_json"]["plan_info"][0]["company_name"] == (
        "Sample Employer"
    )


def test_toc_rows_merge_plan_info_before_filtering_shared_file_urls():
    query_source_dict = _synthetic_query_source()
    shared_url = "https://example.test/shared-rates.json.gz"
    toc_payload = _synthetic_toc_payload(
        ("Sample Employer Choice Plan", "111111111", shared_url),
        ("Unrelated Employer Choice Plan", "222222222", shared_url),
    )

    plan_rows, file_rows = discovery._toc_rows_from_content(
        query_source_dict,
        "https://example.test/carrier-index.json",
        toc_payload,
        filter_to_target_query=True,
    )

    assert [plan_row["plan_id"] for plan_row in plan_rows] == ["111111111"]
    assert [file_row["url"] for file_row in file_rows] == [shared_url]
    assert file_rows[0]["plan_ids"] == ["111111111"]


def test_toc_rows_deduplicate_repeated_plan_identity(monkeypatch):
    repeated_plan_dict = {
        "plan_id": "111111111",
        "plan_id_type": "ein",
        "plan_market_type": "group",
        "plan_name": "Example Plan",
    }
    entry_list = [
        types.SimpleNamespace(
            source_type="in-network",
            original_url=f"https://example.test/rates-{index}.json.gz",
            canonical_url=f"https://example.test/rates-{index}.json.gz",
            from_index_url="https://example.test/index.json",
            description=f"Rates {index}",
            domain="example.test",
            reporting_entity_name="Example Payer",
            reporting_entity_type="insurer",
            plan_info=(repeated_plan_dict,),
        )
        for index in range(2)
    ]
    monkeypatch.setattr(
        discovery,
        "parse_toc_catalog_entries",
        lambda *_args, **_kwargs: entry_list,
    )

    plan_row_list, file_row_list = discovery._toc_rows_from_content(
        {"source_id": "source_1"},
        "https://example.test/index.json",
        {},
    )

    assert len(plan_row_list) == 1
    assert len(file_row_list) == 2


@pytest.mark.asyncio
async def test_query_filtered_toc_stream_retains_only_matching_structures(monkeypatch):
    discovery_source = _synthetic_scoped_query_source()
    toc_payload = _synthetic_toc_payload(
        (
            "Sample Employer Choice Plan",
            "111111111",
            "https://example.test/matching-rates.json.gz",
        ),
        (
            "Unrelated Employer Choice Plan",
            "222222222",
            "https://example.test/unrelated-rates.json.gz",
        ),
    )
    crawl_target = discovery.CrawlTarget(
        source=discovery_source,
        url="https://example.test/carrier-index.json",
        label="Carrier index",
        metadata={
            "reporting_entity_name": "Example Carrier",
            "reporting_entity_type": "Health Insurance Issuer",
            "last_updated_on": "2026-07-01",
        },
    )
    response = _FakeFetchResponse(
        status=200,
        body=json.dumps(toc_payload).encode(),
        content_type="application/json",
        url=crawl_target.url,
    )

    class FakeSession:
        def get(self, url, **_kwargs):
            assert url == crawl_target.url
            return response

    async def allow_url(_url):
        return None

    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allow_url)

    filtered_toc = await discovery._fetch_query_filtered_toc(
        crawl_target,
        max_bytes=4096,
        session=FakeSession(),
    )

    assert filtered_toc["reporting_entity_name"] == "Example Carrier"
    assert filtered_toc["last_updated_on"] == "2026-07-01"
    [matching_structure] = filtered_toc["reporting_structure"]
    assert [
        plan["plan_id"] for plan in matching_structure["reporting_plans"]
    ] == ["111111111"]
    assert [
        file_item["location"]
        for file_item in matching_structure["in_network_files"]
    ] == ["https://example.test/matching-rates.json.gz"]


@pytest.mark.asyncio
async def test_query_filtered_toc_stream_enforces_byte_limit(monkeypatch):
    source = _synthetic_query_source()
    target = discovery.CrawlTarget(
        source=source,
        url="https://example.test/carrier-index.json",
        label="Carrier index",
    )
    response = _FakeFetchResponse(
        status=200,
        body=json.dumps(_synthetic_toc_payload()).encode(),
        content_type="application/json",
        url=target.url,
    )

    class FakeSession:
        def get(self, _url, **_kwargs):
            return response

    async def allow_url(_url):
        return None

    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allow_url)

    with pytest.raises(ValueError, match="response exceeds 16 byte"):
        await discovery._fetch_query_filtered_toc(
            target,
            max_bytes=16,
            session=FakeSession(),
        )


def test_healthsparq_query_expansion_filters_before_limit_and_disambiguates_plans():
    """Verify this source-discovery regression contract."""
    catalog_source_dict = {
        "source_id": "src_aetna",
        "display_name": "Aetna",
        "metadata_json": {
            "raw": {
                "target_payer_query": "Example Packaging",
                "query_expansion_source": True,
            }
        },
    }
    metadata_response_dict = {
        "files": [
            {
                "reportingEntityName": "Example Reporting Entity",
                "reportingEntityType": "Third Party Administrator_111",
                "reportingPlans": [
                    {
                        "planId": "111111111",
                        "planIdType": "ein",
                        "planMarketType": "group",
                        "planName": "Unrelated Employer Aetna Choice POS II",
                    }
                ],
                "fileSchema": "IN_NETWORK_RATES",
                "fileName": "2026-06-01_unrelated.json.gz",
                "filePath": "2026-06-01/inNetworkRates/unrelated.json.gz",
            },
            {
                "reportingEntityName": "Example Reporting Entity",
                "reportingEntityType": "Third Party Administrator_222",
                "reportingPlans": [
                    {
                        "planId": "222222222",
                        "planIdType": "ein",
                        "planMarketType": "group",
                        "planName": "Example Packaging HSA Aetna Choice POS II",
                    },
                    {
                        "planId": "222222222",
                        "planIdType": "ein",
                        "planMarketType": "group",
                        "planName": "Example Packaging Aetna Choice POS II",
                    },
                ],
                "fileSchema": "TABLE_OF_CONTENTS",
                "fileName": "2026-06-01_222_index.json.gz",
                "filePath": "2026-06-01/tableOfContents/2026-06-01_222_index.json.gz",
            },
        ]
    }

    matching_targets = discovery._healthsparq_targets_from_metadata(
        catalog_source_dict,
        "https://mrf.healthsparq.com/example/prd/mrf/A/BRAND/latest_metadata.json",
        metadata_response_dict,
        resolved_from_url="https://health1.aetna.com/app/public/#/one/insurerCode=A&brandCode=BRAND/",
        params={"insurerCode": "A", "brandCode": "BRAND"},
    )

    assert len(matching_targets) in {1}
    [matching_target] = matching_targets
    assert matching_target.metadata["file_path"].endswith("2026-06-01_222_index.json.gz")
    plan_info = matching_target.metadata["plan_info"]
    assert [plan["plan_name"] for plan in plan_info] == [
        "HSA Aetna Choice POS II",
        "Aetna Choice POS II",
    ]
    assert [plan["plan_sponsor_name"] for plan in plan_info] == [
        "Example Packaging",
        "Example Packaging",
    ]
    assert len({plan["engine_plan_hash"] for plan in plan_info}) == 2


def test_healthsparq_query_expansion_splits_concatenated_plan_label():
    plan_name, sponsor_name = discovery._healthsparq_query_plan_label(
        "Example Water Co. DBA Example PackagingHSA Aetna Choice POS II",
        "Example Packaging",
    )

    assert plan_name == "HSA Aetna Choice POS II"
    assert sponsor_name == "Example Packaging"


def test_healthsparq_metadata_rows_apply_query_expansion_plan_labels():
    catalog_source_dict = {
        "source_id": "src_aetna",
        "metadata_json": {
            "raw": {
                "target_payer_query": "Example Packaging",
                "query_expansion_source": True,
            }
        },
    }
    metadata_response_dict = {
        "files": [
            {
                "reportingEntityName": "Example Reporting Entity",
                "reportingEntityType": "Third Party Administrator_222",
                "reportingPlans": [
                    {
                        "planId": "222222222",
                        "planIdType": "ein",
                        "planMarketType": "group",
                        "planName": "Example Water Co. DBA Example PackagingHSA Aetna Choice POS II",
                    }
                ],
                "fileSchema": "IN_NETWORK_RATES",
                "fileName": "2026-06-01_example.json.gz",
                "filePath": "2026-06-01/inNetworkRates/example.json.gz",
            }
        ]
    }

    plan_rows, file_rows = discovery._healthsparq_rows_from_metadata(
        catalog_source_dict,
        "https://mrf.healthsparq.com/example/prd/mrf/A/BRAND/latest_metadata.json",
        metadata_response_dict,
    )

    assert [plan_row["plan_name"] for plan_row in plan_rows] == ["HSA Aetna Choice POS II"]
    [file_row] = file_rows
    assert file_row["plan_names"] == ["HSA Aetna Choice POS II"]
    plan_info = file_row["metadata_json"]["plan_info"]
    assert plan_info[0]["plan_name"] == "HSA Aetna Choice POS II"
    assert plan_info[0]["plan_sponsor_name"] == "Example Packaging"
    assert plan_info[0]["company_name"] == "Example Packaging"
    assert plan_info[0]["engine_plan_hash"]


def test_healthsparq_metadata_rows_preserve_engine_plan_hashes_for_catalog_sync():
    catalog_source_dict = {
        "source_id": "src_aetna",
        "payer_id": "payer_aetna",
        "display_name": "Example Carrier",
    }
    metadata_response_dict = {
        "files": [
            {
                "reportingEntityName": "Example Reporting Entity",
                "reportingEntityType": "Third Party Administrator_222",
                "reportingPlans": [
                    {
                        "planId": "222222222",
                        "planIdType": "ein",
                        "planMarketType": "group",
                        "planName": "Example Packaging HSA Choice Plan",
                    },
                    {
                        "planId": "222222222",
                        "planIdType": "ein",
                        "planMarketType": "group",
                        "planName": "Example Packaging Choice Plan",
                    },
                ],
                "fileSchema": "TABLE_OF_CONTENTS",
                "fileName": "2026-06-01_222_index.json.gz",
                "filePath": "2026-06-01/tableOfContents/2026-06-01_222_index.json.gz",
            }
        ]
    }

    _, file_rows = discovery._healthsparq_rows_from_metadata(
        catalog_source_dict,
        "https://mrf.healthsparq.com/example/prd/mrf/A/BRAND/latest_metadata.json",
        metadata_response_dict,
    )

    assert len(file_rows) in {1}
    plan_info = file_rows[0]["metadata_json"]["plan_info"]
    assert [plan["plan_name"] for plan in plan_info] == [
        "Example Packaging HSA Choice Plan",
        "Example Packaging Choice Plan",
    ]
    assert len({plan["engine_plan_hash"] for plan in plan_info}) == 2


def test_healthsparq_reads_nested_manifest():
    source_by_id = {
        "source_id": "src_example",
        "payer_id": "payer_example",
        "display_name": "Example Carrier",
    }
    metadata_url = (
        "https://mrf.healthsparq.com/example/prd/mrf/EXAMPLE_I/EXAMPLE/"
        "latest_metadata.json"
    )
    manifest_by_section = {
        "data": {
            "items": [
                {
                    "reporting_entity_name": "Example Reporting Entity",
                    "reporting_entity_type": "Health Insurance Issuer",
                    "plans": [
                        {
                            "plan_name": "Example Nested Choice",
                            "plan_id_type": "ein",
                            "plan_id": "123456789",
                            "plan_market_type": "group",
                        }
                    ],
                    "updated_at": "2026-07-01",
                    "schema": "in network rates",
                    "name": "nested-rates.json.gz",
                    "downloadUrl": "https://cdn.example.test/nested-rates.json.gz",
                }
            ]
        }
    }

    catalog_plan_rows, catalog_file_rows = discovery._healthsparq_rows_from_metadata(
        source_by_id, metadata_url, manifest_by_section
    )
    crawl_targets = discovery._healthsparq_targets_from_metadata(
        source_by_id,
        metadata_url,
        manifest_by_section,
        resolved_from_url="https://example.healthsparq.com/healthsparq/public/",
        params={"insurerCode": "EXAMPLE_I", "brandCode": "EXAMPLE"},
    )

    assert [plan_row["plan_name"] for plan_row in catalog_plan_rows] == [
        "Example Nested Choice"
    ]
    assert [file_row["file_type"] for file_row in catalog_file_rows] == ["in-network"]
    assert (
        catalog_file_rows[0]["url"] == "https://cdn.example.test/nested-rates.json.gz"
    )
    assert catalog_file_rows[0]["metadata_json"]["file_schema"] == "in network rates"
    assert catalog_file_rows[0]["metadata_json"]["plan_info"][0]["engine_plan_hash"]
    assert [crawl_target.url for crawl_target in crawl_targets] == [
        "https://cdn.example.test/nested-rates.json.gz"
    ]
    assert crawl_targets[0].metadata["target_file_type"] == "in-network"
    assert crawl_targets[0].metadata["plan_info"][0]["plan_id"] == "123456789"


def test_healthsparq_toc_target_plan_hashes_enrich_parsed_file_rows():
    crawl_target = discovery.CrawlTarget(
        source={"source_id": "src_aetna", "display_name": "Example Carrier"},
        url="https://example.com/2026-06-01_222_index.json.gz",
        label="2026-06-01_222_index.json.gz",
        resolved_from_url="https://example.com/latest_metadata.json",
        metadata={
            "plan_info": [
                {
                    "plan_id": "222222222",
                    "plan_id_type": "ein",
                    "plan_market_type": "group",
                    "plan_name": "Example Packaging HSA Choice Plan",
                    "engine_plan_hash": "hash-hsa",
                },
                {
                    "plan_id": "222222222",
                    "plan_id_type": "ein",
                    "plan_market_type": "group",
                    "plan_name": "Example Packaging Choice Plan",
                    "engine_plan_hash": "hash-choice",
                },
            ]
        },
    )
    file_rows = [
        {
            "metadata_json": {
                "plan_info": [
                    {
                        "plan_id": "222222222",
                        "plan_id_type": "ein",
                        "plan_market_type": "group",
                        "plan_name": "Example Packaging HSA Choice Plan",
                    },
                    {
                        "plan_id": "222222222",
                        "plan_id_type": "ein",
                        "plan_market_type": "group",
                        "plan_name": "Example Packaging Choice Plan",
                    },
                ]
            }
        }
    ]

    [annotated] = discovery._apply_crawl_target_context_to_file_rows(file_rows, crawl_target)

    plan_info = annotated["metadata_json"]["plan_info"]
    assert [plan["engine_plan_hash"] for plan in plan_info] == [
        "hash-hsa",
        "hash-choice",
    ]


def test_query_expanded_generic_toc_rows_normalize_plan_labels_before_merge():
    """Verify this source-discovery regression contract."""
    catalog_source_dict = {
        "source_id": "src_aetna",
        "display_name": "Example Carrier",
        "metadata_json": {
            "raw": {
                "target_payer_query": "Example Packaging",
                "query_expansion_source": True,
            }
        },
    }
    toc_document_dict = {
        "reporting_entity_name": "Example Reporting Entity",
        "reporting_entity_type": "Third Party Administrator",
        "version": "1.0.0",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "Example Water Co. DBA Example PackagingHSA Choice Plan",
                        "plan_id_type": "ein",
                        "plan_id": "222222222",
                        "plan_market_type": "group",
                    },
                    {
                        "plan_name": "Example Water Co. DBA Example PackagingChoice Plan",
                        "plan_id_type": "ein",
                        "plan_id": "222222222",
                        "plan_market_type": "group",
                    },
                ],
                "in_network_files": [
                    {
                        "description": "in network file",
                        "location": "https://example.test/in-network.json.gz",
                    }
                ],
            }
        ],
    }
    crawl_target = discovery.CrawlTarget(
        source=catalog_source_dict,
        url="https://example.test/index.json",
        label="Example Packaging TOC",
        resolved_from_url="https://example.test/latest_metadata.json",
        metadata={
            "plan_info": [
                {
                    "plan_id": "222222222",
                    "plan_id_type": "ein",
                    "plan_market_type": "group",
                    "plan_name": "HSA Choice Plan",
                    "engine_plan_hash": "hash-hsa",
                    "company_name": "Example Packaging",
                },
                {
                    "plan_id": "222222222",
                    "plan_id_type": "ein",
                    "plan_market_type": "group",
                    "plan_name": "Choice Plan",
                    "engine_plan_hash": "hash-choice",
                    "company_name": "Example Packaging",
                },
            ]
        },
    )

    plan_rows, file_rows = discovery._toc_rows_from_content(
        catalog_source_dict, "https://example.test/index.json", toc_document_dict
    )

    assert [plan_row["plan_name"] for plan_row in plan_rows] == [
        "HSA Choice Plan",
        "Choice Plan",
    ]
    [file_row] = file_rows
    plan_info = file_row["metadata_json"]["plan_info"]
    assert [plan["plan_name"] for plan in plan_info] == [
        "HSA Choice Plan",
        "Choice Plan",
    ]
    assert {plan["company_name"] for plan in plan_info} == {"Example Packaging"}

    [annotated] = discovery._apply_crawl_target_context_to_file_rows(
        [file_row], crawl_target
    )

    merged = annotated["metadata_json"]["plan_info"]
    assert [plan["engine_plan_hash"] for plan in merged] == [
        "hash-hsa",
        "hash-choice",
    ]
    assert {plan["company_name"] for plan in merged} == {"Example Packaging"}


def test_query_expansion_match_tolerates_legal_suffix_and_concatenated_plan_text():
    assert discovery._is_search_value_query_match(
        ["Example Water Co., et. al DBA Example PackagingHSA Aetna Choice POS II"],
        "Example Packaging Inc",
    )
    assert discovery._is_search_value_query_match(
        ["Example Hospitality Group DBA Example Clubs"],
        "Example Clubs",
    )
    assert not discovery._is_search_value_query_match(
        ["Unrelated PackagingHSA Aetna Choice POS II"],
        "Example Packaging Inc",
    )


@pytest.mark.asyncio
async def test_healthcarebluebook_limits_nested_crawl(
    monkeypatch,
):
    source_mapping = {
        "source_id": "source_example_hbb",
        "display_name": "Example HBB",
        "hosting_platform": "healthcarebluebook_mrf",
    }
    listing_html = """
    <div class="grid-item">
      <a href="https://health1.aetna.com/app/public/#/one/insurerCode=EXAMPLE&brandCode=ALICSI/machine-readable-transparency-in-coverage">
        Example Nested Aetna
      </a>
    </div>
    <div class="grid-item">In-Network</div>
    """
    observed_target_limits = []

    async def fake_fetch_text(*_args, **_kwargs):
        return listing_html

    async def fake_crawl_targets_for_source(
        nested_source, link_url, _session, *, target_limit=None
    ):
        observed_target_limits.append(target_limit)
        return [
            discovery.CrawlTarget(
                source=nested_source,
                url=f"{link_url}/one.json",
                label="one",
                metadata={"resolver": "nested"},
            ),
            discovery.CrawlTarget(
                source=nested_source,
                url=f"{link_url}/two.json",
                label="two",
                metadata={"resolver": "nested"},
            ),
        ]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(
        discovery, "_crawl_targets_for_source", fake_crawl_targets_for_source
    )

    resolved_targets = await discovery._resolve_healthcarebluebook_mrf(
        source_mapping,
        "https://mrf.healthcarebluebook.com/Example",
        {"type": "healthcarebluebook_mrf", "max_targets": 1},
        session=object(),
    )

    assert observed_target_limits == [1]
    assert [resolved_target.label for resolved_target in resolved_targets] == ["one"]


@pytest.mark.asyncio
async def test_query_expansion_uses_sapphire_probe_when_base_resolver_fails(monkeypatch):
    source_dict = {
        "source_id": "source_example_sapphire",
        "display_name": "Example Sapphire",
        "hosting_platform": "sapphire",
        "index_url": "https://example.sapphiremrfhub.com/",
        "metadata_json": {
            "raw": {
                "target_payer_query": "Example Packaging Inc",
                "query_expansion_source": True,
            }
        },
    }

    async def fake_crawl_targets_for_source(*_args, **_kwargs):
        raise ValueError("no configured resolver and URL is not a direct JSON TOC")

    async def fake_sapphire_query_probe_targets(source_arg, url, query, _session):
        assert query == "Example Packaging Inc"
        return [
            discovery.CrawlTarget(
                source=source_arg,
                url="https://example.sapphiremrfhub.com/tocs/current/example-packaging",
                label="Example Packaging",
                resolved_from_url=url,
                metadata={"company_name": "Example Packaging"},
            )
        ]

    monkeypatch.setattr(
        discovery, "_crawl_targets_for_source", fake_crawl_targets_for_source
    )
    monkeypatch.setattr(
        discovery, "_sapphire_query_probe_targets", fake_sapphire_query_probe_targets
    )

    crawl_targets, observations = await discovery._resolve_crawl_targets(
        [source_dict],
        session=object(),
        run_id="run_example",
        concurrency=1,
    )

    assert observations == []
    assert [crawl_target.url for crawl_target in crawl_targets] == [
        "https://example.sapphiremrfhub.com/tocs/current/example-packaging"
    ]
    assert crawl_targets[0].metadata["query_expansion_match"] is True
    assert crawl_targets[0].metadata["target_payer_query"] == "Example Packaging Inc"


def test_crawl_source_dedupe_keeps_distinct_healthsparq_metadata_catalogs():
    source_rows = [
        {
            "source_id": "src_aetna_self_insured",
            "source_key": "src_aetna_self_insured",
            "display_name": "Aetna CVS - Self Insured",
            "hosting_platform": "aetna_health1",
            "source_type": "curated_registry",
            "seed_provider": "master-list",
            "status": "active",
            "index_url": (
                "https://health1.aetna.com/app/public/#/one/"
                "insurerCode=AETNACVS_I&brandCode=ALICSI/"
                "machine-readable-transparency-in-coverage"
            ),
        },
        {
            "source_id": "src_aetna_signature",
            "source_key": "src_aetna_signature",
            "display_name": "Aetna Signature Administrators",
            "hosting_platform": "aetna_health1",
            "source_type": "curated_registry",
            "seed_provider": "master-list",
            "status": "active",
            "index_url": (
                "https://health1.aetna.com/app/public/#/one/"
                "insurerCode=AETNACVS_I&brandCode=ASA/"
                "machine-readable-transparency-in-coverage?searchTerm=ASA_01&lock=true"
            ),
        },
    ]

    deduped = discovery._dedupe_source_rows_for_crawl(source_rows)

    assert {source_row["source_id"] for source_row in deduped} == {
        "src_aetna_self_insured",
        "src_aetna_signature",
    }


def test_crawl_source_dedupe_preserves_base_and_normalized_query_siblings():
    base_source_dict = {
        "source_id": "source_base",
        "source_key": "source_base",
        "display_name": "Example Carrier",
        "hosting_platform": "uhc_public_blobs",
        "source_type": "curated_registry",
        "seed_provider": "master-list",
        "status": "active",
        "index_url": "https://example.test/machine-readable-files/",
        "metadata_json": {},
    }
    queried_source_dict = {
        **base_source_dict,
        "source_id": "source_query",
        "source_key": "source_query",
        "metadata_json": {
            "raw": {"target_payer_query": "Sample Employer"},
        },
    }
    duplicate_query_source_dict = {
        **base_source_dict,
        "source_id": "source_query_duplicate",
        "source_key": "source_query_duplicate",
        "metadata_json": {
            "raw": {"target_payer_query": "  sample   employer  "},
        },
    }

    deduped_source_rows = discovery._dedupe_source_rows_for_crawl(
        [base_source_dict, queried_source_dict, duplicate_query_source_dict]
    )

    assert {source_row["source_id"] for source_row in deduped_source_rows} == {
        "source_base",
        "source_query",
    }


def test_parse_master_list_prefers_active_duplicate_over_unsupported_fragment():
    markdown = """
## C. Regional, provider-sponsored, Medicaid-MCO, DTC & TPA payers
| Payer | Type | Public TOC / landing URL | Notes |
|---|---|---|---|
| Example Plan | regional | https://example.test/machine-readable-files | curated source row |
| Example Plan | regional | https://example.test/machine-readable-files#:~:text=files | observed unsupported |
"""

    [candidate] = discovery.parse_master_list(markdown)

    assert candidate.index_url == "https://example.test/machine-readable-files"
    assert candidate.status == "active"


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
| United Healthcare | national | https://transparency-in-coverage.uhc.com/ | aliases: UHC, UMR, Surest, "Health Plans, Inc" |
"""

    candidates = discovery.parse_master_list(markdown)

    assert candidates[0].payer_name == "Meritain Health"
    assert candidates[0].entity_type == "tpa"
    assert candidates[0].index_url.endswith("MERITAINOVER/")
    collective_list = [item for item in candidates if item.payer_name == "Collective Health"]
    assert len(collective_list) == 2
    assert {item.entity_type for item in collective_list} == {"tpa"}
    [asr_list] = [item for item in candidates if item.payer_name == "ASR Health Benefits"]
    assert asr_list.entity_type == "tpa"
    assert asr_list.hosting_platform == "asr_health_benefits"
    [uhc_list] = [item for item in candidates if item.payer_name == "United Healthcare"]
    assert uhc_list.aliases == ("UHC", "UMR", "Surest", "Health Plans, Inc")


def test_master_list_aliases_are_stored_on_source_and_payer_rows():
    candidate = discovery.SourceCandidate(
        payer_name="United Healthcare",
        provider="master-list",
        index_url="https://transparency-in-coverage.uhc.com/",
        aliases=("UHC", "UMR", "Surest"),
        source_coverage=("national",),
    )

    payer_row, source_row = discovery._candidate_to_rows(
        candidate,
        discovery._utc_now(),
        discovery_run_id="run_example",
    )

    assert payer_row["aliases"] == ["Surest", "UHC", "UMR", "United Healthcare"]
    assert payer_row["metadata_json"]["aliases"] == [
        "Surest",
        "UHC",
        "UMR",
        "United Healthcare",
    ]
    assert payer_row["metadata_json"]["benefit_lines"] == ["medical"]
    assert "discovery_run_id" not in payer_row["metadata_json"]
    assert source_row is not None
    assert source_row["metadata_json"]["discovery_run_id"] == "run_example"
    assert source_row["metadata_json"]["aliases"] == [
        "Surest",
        "UHC",
        "UMR",
        "United Healthcare",
    ]
    assert source_row["metadata_json"]["benefit_lines"] == ["medical"]
    assert source_row["metadata_json"]["source_coverage"] == ["national"]


def test_master_list_coverage_evidence_metadata_is_stored_on_source_rows():
    candidate = discovery.SourceCandidate(
        payer_name="Example Packaging Benefits",
        provider="master-list",
        index_url="https://benefits.example.test/find-a-provider",
        entity_type="group",
        source_tier="coverage_evidence",
        aliases=("Example Packaging",),
        source_coverage=("Example Packaging medical choices",),
        vendor_names=("Example Virtual Health",),
        network_names=("Example National PPO",),
        plan_names=("Example Guided Health Plan",),
    )

    payer_row, source_row = discovery._candidate_to_rows(
        candidate, discovery._utc_now()
    )

    assert payer_row["metadata_json"]["source_tier"] == "coverage_evidence"
    assert source_row is not None
    assert source_row["metadata_json"]["source_tier"] == "coverage_evidence"
    assert source_row["metadata_json"]["aliases"] == [
        "Example Packaging",
        "Example Packaging Benefits",
    ]
    assert source_row["metadata_json"]["source_coverage"] == [
        "Example Packaging medical choices"
    ]
    assert source_row["metadata_json"]["vendor_names"] == ["Example Virtual Health"]
    assert source_row["metadata_json"]["network_names"] == ["Example National PPO"]
    assert source_row["metadata_json"]["plan_names"] == ["Example Guided Health Plan"]


def test_master_list_public_gap_sources_classify_supported_platforms():
    """Verify this source-discovery regression contract."""
    markdown = """
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| 90 Degree Benefits | tpa | https://portal.90degreebenefits.com/MemberPortal/MachineReadableFiles | aliases: 90 Degree, 90DB |
| Banner Aetna | national | https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=BANNERJVFI/machine-readable-transparency-in-coverage | aliases: Banner Aetna, Banner Health Aetna |
| Select Health | regional | https://www.selecthealth.org/disclaimers/machine-readable-data | aliases: SelectHealth |
| EyeMed | vision | https://content.eyemedvisioncare.com/EyeMed_HCSC/eyemed_in-network-rates.json | benefit lines: vision; aliases: EyeMed Vision Care, Eye Med, Ameritas with EyeMed |
| Example Marketplace Family | national | https://www.centene.com/price-transparency-files.html | aliases: Example Marketplace, Example Managed Network |
| Example Northern Blue | blue | https://www.bcbsnd.com/employers/group-insurance-101/understanding-transparency-in-coverage-rule | aliases: Example Northern, Example Blue |
| EMI Health | regional | https://emihealth.com/machinereadables | public machine-readable files page |
| MotivHealth Insurance Company | regional | https://www.motivhealth.com/machinereadablefiles/ | aliases: MotivHealth |
| Angle Health | regional | https://www.anglehealth.com/machine-readable-files | aliases: Angle, Adrem Administrators |
| PacificSource | regional | https://mrf.pacificsource.com/File/Visit/Index | aliases: Pacific Source |
| Allegiance Benefit Plan Management | tpa | https://mrf.healthcarebluebook.com/Allegiance | aliases: AskAllegiance |
| Healthcare Management Administrators | tpa | https://sawus2prdticmrfhma.z5.web.core.windows.net/ | aliases: HMA, AccessHMA |
| VIVA Health | provider_sponsored | https://www.vivahealth.com/mrf/ | public VIVA MRF landing |
| HealthComp | tpa | https://healthcomp.sapphiremrfhub.com/ | aliases: Personify Health, Personify |
| Pinnacle Claims Management | tpa | https://mrf.healthcarebluebook.com/Pinnacle | aliases: PCMI |
| Regency Employee Benefits | tpa | https://www.mymedicalshopper.com/mrf-search/robbins-regency-employee-benefits-inc-regn | aliases: Robbins Regency Employee Benefits |
| Varipro | tpa | https://www.mymedicalshopper.com/mrf-search/varipro | aliases: Varipro TPA, Valipro TPA |
| Reliance Matrix | tpa | https://www.reliancematrix.com/privacy-notice/transparency-in-coverage | aliases: Reliance Standard, Reliance Standard Life Insurance Company |
| ACS Benefit Services | tpa | https://acsbenefitservices.sapphiremrfhub.com/ | aliases: ACS Benefits |
| ATA America | tpa | https://mrf.healthcarebluebook.com/ATA | aliases: American Trust Administrators, ATA |
| American Plan Administrators | tpa | https://apatpa.com/disclosures-terms-conditions-privacy-policy-american-plan-administrators/ | aliases: APA, APA TPA |
| Benefit Plan Administrators | tpa | https://www.mymedicalshopper.com/mrf-search/benefit-plan-administrators | aliases: BPA, BPA TPA |
| Blackhawk Claims Service | tpa | https://www.mymedicalshopper.com/mrf-search/blackhawk | aliases: Blackhawk |
| Brighton Health Plan Solutions | tpa | https://clm.magnacare.com/transparency/ | aliases: Brighton HPS, MagnaCare |
| ByWater | tpa | https://www.mymedicalshopper.com/mrf-search/bywater | aliases: Bywater, Choose ByWater |
| Center Care | tpa | https://ctc.mrf.payercompass.com/ | aliases: CTC |
| Coastal Administrative Services | tpa | https://mrf.healthcarebluebook.com/CAS | aliases: CAS |
| Concierge Administrative Services | tpa | https://www.mymedicalshopper.com/mrf-search/concierge | aliases: Concierge |
| Diversified Group | tpa | https://www.mymedicalshopper.com/mrf-search/diversified-group | aliases: The Diversified Group |
| Dunn & Associates | tpa | https://www.mymedicalshopper.com/mrf-search/dunn-and-associates | aliases: Dunn and Associates |
| Employee Benefit Logistics | tpa | https://ebl.mrf.payercompass.com/ | aliases: EBL |
| Employers Health Network | tpa | https://ehn.mrf.payercompass.com/ | aliases: EHN |
| Fox/Everett | tpa | https://www.mymedicalshopper.com/mrf-search/fox-everett | aliases: Fox Everett |
| HealthChoice - HPI | tpa | https://hcn.mrf.payercompass.com/ | aliases: HealthChoice HPI, HCN |
| Marpai | tpa | https://www.mymedicalshopper.com/mrf-search/marpai | aliases: Marpai Health |
| Insurance Systems | tpa | https://isi.mrf.payercompass.com/ | aliases: ISI |
| Kapnick Insurance Group | tpa | https://bcbsm.sapphiremrfhub.com/tocs/current/kapnick_co_inc | aliases: Kapnick |
| Insight Benefit Administrators | tpa | https://insightba.net/transparency-in-coverage-resources/ | aliases: Insight Benefit Admininistrators, Insight BA |
| Imagine360 | tpa | https://caa.imagine360.com/ExternalINNFiles/Imagine/index.html | aliases: Imagine 360 |
| Patient Advocates | tpa | https://www.mymedicalshopper.com/mrf-search/patient-advocates | aliases: Patient Advocates LLC |
| Planned Administrators Inc | tpa | https://www.paisc.com/compliance-machine-readable-files-mrfs | aliases: PAI, Planned Administrators |
| Prodegi | tpa | https://www.mymedicalshopper.com/mrf-search/prodegi | benefit lines: medical, dental, vision; aliases: Prodegi Benefits |
| ReDirect Health | tpa | https://www.redirecthealth.com/machine-readable-data/ | aliases: Redirect Health |
| Simplified Benefits Administrators | tpa | https://mrf.healthcarebluebook.com/SBA | aliases: SBA |
| SIHO | tpa | https://www.mymedicalshopper.com/mrf-search/siho | aliases: SIHO Insurance Services |
| Stanislaus County Health Plan | tpa | https://schp.mrf.payercompass.com/ | aliases: SCHP, HPNC |
| Transwestern Insurance Administrators | tpa | https://www.trans-western.com/mrf_data/multiplan/MRF_PHCS_TOC_20260531.json | benefit lines: medical; aliases: Transwestern |
| Trustmark Small Business Benefits | tpa | https://mrf.healthcarebluebook.com/trustmarksb | aliases: Trustmark Small Business, Trustmark SB |
| Med-Pay | tpa | https://mrf.healthcarebluebook.com/medpay | aliases: Med Pay, MedPay |
| Municipal Benefit Health Program | network/tpa | https://mhbp.mrf.payercompass.com/ | aliases: MHBP |
| The Care Network | network | https://www.claimsbridge.net/tic/tcn/TCN_in-Network-rates.json | aliases: TCN |
| Nippon Life Benefits | tpa | https://mrf.healthcarebluebook.com/Nippon | aliases: Nippon Life |
| UMWA Health and Retirement Funds | tpa | https://mrf.healthcarebluebook.com/healthsmartfundsaccount | aliases: UMWA Funds |
| PTI Engineered Plastics | group | https://mrf.healthcarebluebook.com/PTIEngineeredPlastics | aliases: PTI |
| RCI Group II, LLC | group | https://mrf.healthcarebluebook.com/RCI | aliases: RCI Group II, RCI |
| Smile Brands Inc | group | https://raw.githubusercontent.com/AmeriBen/MRF/main/allowed-amounts/2026-06-01_ameriben_smile_brands_inc_allowed-amounts.zip | benefit lines: medical; aliases: Smile Brands |
| The Ohio State University | group | https://mrf.healthcarebluebook.com/TheOhioStateUniversity | aliases: Ohio State University, OSU |
| U.S. Renal Care | group | https://mrf.healthcarebluebook.com/USRenalCare | aliases: US Renal Care, U.S. Renal |
| Washington Community Schools | group | https://www.mymedicalshopper.com/mrf/washington-community-schools-hdp-family | aliases: Washington Community Schools HDP Family |
| BlueAdvantage Administrators of Arkansas | tpa | https://www.blueadvantagearkansas.com/interoperability/machine-readable-files | aliases: BlueAdvantage, Skai BCBS |
| BCBS Global Solutions | blue | https://bcbsglobalsolutions.com/transparency-in-coverage/ | aliases: Blue Cross Blue Shield Global Solutions |
| GEHA | network/tpa | https://www.geha.com/transparency-in-coverage | benefit lines: dental, medical; aliases: Connection Dental |
| HealthSmart | network/tpa | https://mrf.healthcarebluebook.com/Healthsmart | benefit lines: medical, dental; aliases: HealthSmart Benefit Solutions, HealthSmart-Dental, HealthSmart Dental |
| Valley Health Plan | medicaid_mco | https://data.sccgov.org/data.json | benefit lines: medical, dental, vision; aliases: VHP |
| UCare | medicaid_mco | https://www.ucare.org/legal-notices/transparency-in-coverage | benefit lines: medical, dental; aliases: UCare Minnesota, UCare IFP |
| Sharp Health Plan | provider_sponsored | https://www.sharphealthplan.com/api-access-for-developers | aliases: Sharp Health Plan of San Diego |
| Mercy/MercyCare | provider_sponsored | https://stmercycaremrf.z14.web.core.windows.net/in_network.html | benefit lines: medical; aliases: MercyCare Health Plans, Mercyhealth |
| Mercy/MercyCare | provider_sponsored | https://stmercycaremrf.z14.web.core.windows.net/OON.html | benefit lines: medical; aliases: MercyCare Health Plans, Mercyhealth |
| Group Health Cooperative of Eau Claire | regional | https://group-health.com/price-transparency | aliases: Group Health Eau Claire |
| S&S Health | tpa | https://mrf.healthcarebluebook.com/SandS | aliases: S&S HealthCare, SandS, Reflect Health |
| SimplePay Health | tpa | https://www.simplepayhealth.com/ | aliases: SimplePay |
| SISCO | tpa | https://sisconosurprise.com/ppo/phcs/index.html | aliases: SISCO Benefits, Self Insured Services Company |
| CBA Blue | tpa | https://www.cbabluevt.com/employer-resources/ | aliases: CBA BLUE |
| EBMS | tpa | https://caa.ebms.com/ | aliases: Employee Benefit Management Services |
| EBAM | tpa | https://www.ebam.com/machine-readable-files/ | aliases: EBA&M, Employee Benefit Administrators and Managers |
| Univera Healthcare | regional | https://univerahc.healthsparq.com/healthsparq/public/#/one/insurerCode=UNVRA_I&brandCode=UNVRA&productCode=MRF/machine-readable-transparency-in-coverage | official HealthSparq MRF link |
| EBPA | tpa | https://tuition.ebpabenefits.com/employers/machine-readable-file-links | aliases: EBPA Benefits |
| HealthNow Administrative Services | tpa | https://www.hnas.com/digital-resources/machine-readable-files | aliases: HNAS |
| Insurance Management Services | tpa | https://mrf.healthcarebluebook.com/IMS | aliases: IMS, IMS TPA |
| Boon-Chapman | tpa | https://boonchapman-mrf.zakipointhealth.com/ | aliases: Boon Chapman |
| HealthEZ | tpa | https://healthezbenefits.com/plandocuments/ | aliases: Health EZ, HealthEZ Benefits |
| Tall Tree Administrators | tpa | https://talltreeadmin.com/machine-readable-files | aliases: Tall Tree |
| Carefactor | tpa | https://mrf.healthcarebluebook.com/Carefactor | aliases: CareFactor |
| Point C | tpa | https://mrf.healthcarebluebook.com/pointc | aliases: Point C Health |
| Unified Group Services | tpa | https://mrf.healthcarebluebook.com/unified | aliases: UGS |
| WellNet | tpa | https://mrf.healthcarebluebook.com/Wellnet | aliases: WellNet Healthcare |
| The Health Plan | regional | https://www.healthplan.org/machine_readable_files | aliases: The Health Plan of West Virginia, THP |
| BCBS Wyoming | blue | https://www.bcbswy.com/machine-readable-files/ | benefit lines: medical, dental, vision; aliases: Blue Cross and Blue Shield of Wyoming, Blue Cross Blue Shield Wyoming, Blue Cross Blue Shield of Wyoming, BCBSWY, BCBSWY Dental, BCBSWY Vision |
| WPS Health | regional | https://www.wpshealth.com/resources/customer-resources/price-transparency.shtml | aliases: Wisconsin Physicians Service, WPS |
| SummaCare | regional | https://files.myplancentral.com/TIC/TOC/ | aliases: SummaCare MEWA, Summa Health System |
| Health Alliance Plan | provider_sponsored | https://hap.healthsparq.com/healthsparq/public/#/one/insurerCode=HAP_I&brandCode=HAP/machine-readable-transparency-in-coverage | aliases: HAP, Alliance Health and Life Insurance Company |
| Peak Health | regional | https://peakhealth.org/transparency/ | aliases: Peak Health Plan |
| Centivo - Rockwell Automation | group | https://eldoradocomputing.hosted-by-files.com/centivopublicRCKWL/ | aliases: Centivo, Centivo Health, Rockwell Automation |
"""

    candidates = discovery.parse_master_list(markdown)
    by_name = {candidate.payer_name: candidate for candidate in candidates}

    assert (
        by_name["90 Degree Benefits"].hosting_platform
        == "healthspace_machine_readable_files"
    )
    assert by_name["90 Degree Benefits"].aliases == ("90 Degree", "90DB")
    assert by_name["Banner Aetna"].hosting_platform == "aetna_health1"
    assert by_name["Banner Aetna"].aliases == ("Banner Aetna", "Banner Health Aetna")
    assert by_name["Select Health"].hosting_platform == "html_mrf_links"
    assert by_name["Select Health"].aliases == ("SelectHealth",)
    assert by_name["EyeMed"].entity_type == "vision"
    assert by_name["EyeMed"].benefit_lines == ("vision",)
    assert by_name["EyeMed"].hosting_platform == "direct_mrf_body"
    assert by_name["EyeMed"].aliases == (
        "EyeMed Vision Care",
        "Eye Med",
        "Ameritas with EyeMed",
    )
    assert by_name["Example Marketplace Family"].hosting_platform == "html_mrf_links"
    assert by_name["Example Marketplace Family"].aliases == (
        "Example Marketplace",
        "Example Managed Network",
    )
    assert by_name["Example Northern Blue"].hosting_platform == "html_delegated_mrf_links"
    assert by_name["Example Northern Blue"].aliases == (
        "Example Northern",
        "Example Blue",
    )
    assert by_name["EMI Health"].hosting_platform == "html_mrf_links"
    assert by_name["MotivHealth Insurance Company"].hosting_platform == "html_mrf_links"
    assert by_name["Angle Health"].hosting_platform == "html_delegated_mrf_links"
    assert by_name["Angle Health"].aliases == ("Angle", "Adrem Administrators")
    assert (
        by_name["PacificSource"].hosting_platform == "pacificsource_azure_mrf_listing"
    )
    assert (
        by_name["Allegiance Benefit Plan Management"].hosting_platform
        == "healthcarebluebook_mrf"
    )
    assert (
        by_name["Healthcare Management Administrators"].hosting_platform
        == "html_mrf_links"
    )
    assert by_name["VIVA Health"].hosting_platform == "viva_health_mrf"
    assert by_name["HealthComp"].hosting_platform == "sapphire"
    assert by_name["HealthComp"].aliases == ("Personify Health", "Personify")
    assert (
        by_name["Pinnacle Claims Management"].hosting_platform
        == "healthcarebluebook_mrf"
    )
    assert (
        by_name["Regency Employee Benefits"].hosting_platform
        == "mymedicalshopper_talon"
    )
    assert by_name["Varipro"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["Varipro"].aliases == ("Varipro TPA", "Valipro TPA")
    assert by_name["Reliance Matrix"].hosting_platform == "html_delegated_mrf_links"
    assert by_name["Reliance Matrix"].aliases == (
        "Reliance Standard",
        "Reliance Standard Life Insurance Company",
    )
    assert by_name["ACS Benefit Services"].hosting_platform == "sapphire"
    assert by_name["ATA America"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["ATA America"].aliases == (
        "American Trust Administrators",
        "ATA",
    )
    assert (
        by_name["American Plan Administrators"].hosting_platform
        == "html_delegated_mrf_links"
    )
    assert by_name["American Plan Administrators"].aliases == ("APA", "APA TPA")
    assert (
        by_name["Benefit Plan Administrators"].hosting_platform
        == "mymedicalshopper_talon"
    )
    assert by_name["Benefit Plan Administrators"].aliases == ("BPA", "BPA TPA")
    assert (
        by_name["Blackhawk Claims Service"].hosting_platform
        == "mymedicalshopper_talon"
    )
    assert (
        by_name["Brighton Health Plan Solutions"].hosting_platform
        == "magnacare_transparency_mrf"
    )
    assert by_name["Brighton Health Plan Solutions"].aliases == (
        "Brighton HPS",
        "MagnaCare",
    )
    assert by_name["ByWater"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["ByWater"].aliases == ("Bywater", "Choose ByWater")
    assert by_name["Center Care"].hosting_platform == "payercompass_mrf"
    assert (
        by_name["Coastal Administrative Services"].hosting_platform
        == "healthcarebluebook_mrf"
    )
    assert by_name["Coastal Administrative Services"].aliases == ("CAS",)
    assert (
        by_name["Concierge Administrative Services"].hosting_platform
        == "mymedicalshopper_talon"
    )
    assert (
        by_name["Diversified Group"].hosting_platform
        == "mymedicalshopper_talon_bounded"
    )
    assert by_name["Diversified Group"].aliases == ("The Diversified Group",)
    assert by_name["Dunn & Associates"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["Employee Benefit Logistics"].hosting_platform == "payercompass_mrf"
    assert by_name["Employers Health Network"].hosting_platform == "payercompass_mrf"
    assert by_name["Fox/Everett"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["HealthChoice - HPI"].hosting_platform == "payercompass_mrf"
    assert by_name["Marpai"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["Insurance Systems"].hosting_platform == "payercompass_mrf"
    assert by_name["Kapnick Insurance Group"].hosting_platform == "sapphire"
    assert (
        by_name["Insight Benefit Administrators"].hosting_platform
        == "insightba_html_mrf_links"
    )
    assert by_name["Insight Benefit Administrators"].aliases == (
        "Insight Benefit Admininistrators",
        "Insight BA",
    )
    assert by_name["Imagine360"].hosting_platform == "html_mrf_links"
    assert (
        by_name["Imagine360"].index_url
        == "https://caa.imagine360.com/ExternalINNFiles/Imagine/index.html"
    )
    assert by_name["Imagine360"].aliases == ("Imagine 360",)
    assert by_name["Patient Advocates"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["Planned Administrators Inc"].hosting_platform == "html_mrf_links"
    assert by_name["Planned Administrators Inc"].aliases == (
        "PAI",
        "Planned Administrators",
    )
    assert by_name["Prodegi"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["Prodegi"].benefit_lines == ("medical", "dental", "vision")
    assert by_name["ReDirect Health"].hosting_platform == "html_mrf_links"
    assert (
        by_name["Simplified Benefits Administrators"].hosting_platform
        == "healthcarebluebook_mrf"
    )
    assert by_name["SIHO"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["Stanislaus County Health Plan"].hosting_platform == "payercompass_mrf"
    assert by_name["Transwestern Insurance Administrators"].hosting_platform == "direct_toc"
    assert by_name["Transwestern Insurance Administrators"].benefit_lines == ("medical",)
    assert (
        by_name["Trustmark Small Business Benefits"].hosting_platform
        == "healthcarebluebook_mrf"
    )
    assert by_name["Med-Pay"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["Municipal Benefit Health Program"].hosting_platform == "payercompass_mrf"
    assert by_name["The Care Network"].hosting_platform == "direct_mrf_body"
    assert by_name["Nippon Life Benefits"].hosting_platform == "healthcarebluebook_mrf"
    assert (
        by_name["UMWA Health and Retirement Funds"].hosting_platform
        == "healthcarebluebook_mrf"
    )
    assert by_name["PTI Engineered Plastics"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["RCI Group II, LLC"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["Smile Brands Inc"].hosting_platform == "direct_mrf_body"
    assert by_name["Smile Brands Inc"].benefit_lines == ("medical",)
    assert by_name["The Ohio State University"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["U.S. Renal Care"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["Washington Community Schools"].hosting_platform == "mymedicalshopper_talon"
    assert (
        by_name["BlueAdvantage Administrators of Arkansas"].hosting_platform
        == "blueadvantage_html_mrf_links"
    )
    assert (
        by_name["BCBS Global Solutions"].hosting_platform
        == "bcbs_global_solutions_mrf"
    )
    assert by_name["BCBS Global Solutions"].aliases == (
        "Blue Cross Blue Shield Global Solutions",
    )
    assert by_name["GEHA"].hosting_platform == "html_delegated_mrf_links"
    assert by_name["GEHA"].benefit_lines == ("dental", "medical")
    assert (
        by_name["Valley Health Plan"].hosting_platform
        == "socrata_data_json_mrf_catalog"
    )
    assert by_name["Valley Health Plan"].benefit_lines == (
        "medical",
        "dental",
        "vision",
    )
    assert by_name["Sharp Health Plan"].hosting_platform == "html_mrf_links"
    assert by_name["Sharp Health Plan"].aliases == ("Sharp Health Plan of San Diego",)
    assert (
        by_name["Group Health Cooperative of Eau Claire"].hosting_platform
        == "html_mrf_links"
    )
    assert by_name["Group Health Cooperative of Eau Claire"].aliases == (
        "Group Health Eau Claire",
    )
    assert by_name["S&S Health"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["S&S Health"].aliases == (
        "S&S HealthCare",
        "SandS",
        "Reflect Health",
    )
    assert by_name["SimplePay Health"].hosting_platform == "html_delegated_mrf_links"
    assert by_name["SISCO"].hosting_platform == "html_mrf_links"
    assert by_name["SISCO"].aliases == (
        "SISCO Benefits",
        "Self Insured Services Company",
    )
    assert by_name["CBA Blue"].hosting_platform == "html_mrf_links"
    assert by_name["EBMS"].hosting_platform == "ebms_caa_directory"
    assert by_name["EBAM"].hosting_platform == "wordpress_elfinder_mrf_links"
    assert by_name["Univera Healthcare"].hosting_platform == "healthsparq"
    assert by_name["EBPA"].hosting_platform == "html_mrf_links"
    assert (
        by_name["HealthNow Administrative Services"].hosting_platform
        == "html_delegated_mrf_links"
    )
    assert (
        by_name["Insurance Management Services"].hosting_platform
        == "healthcarebluebook_mrf"
    )
    assert by_name["Insurance Management Services"].aliases == ("IMS", "IMS TPA")
    assert by_name["Boon-Chapman"].hosting_platform == "html_mrf_links_mixed_directories"
    assert by_name["HealthEZ"].hosting_platform == "healthez_benefits_mrf"
    assert by_name["HealthEZ"].aliases == ("Health EZ", "HealthEZ Benefits")
    assert by_name["Tall Tree Administrators"].hosting_platform == "html_mrf_links"
    assert by_name["Carefactor"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["Carefactor"].aliases == ("CareFactor",)
    assert by_name["Point C"].hosting_platform == "healthcarebluebook_mrf"
    assert (
        by_name["Unified Group Services"].hosting_platform == "healthcarebluebook_mrf"
    )
    assert by_name["WellNet"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["The Health Plan"].hosting_platform == "healthplan_html_mrf_links"
    assert by_name["The Health Plan"].aliases == (
        "The Health Plan of West Virginia",
        "THP",
    )
    assert by_name["BCBS Wyoming"].hosting_platform == "bcbswy_hmhs_monthly_toc"
    assert by_name["BCBS Wyoming"].benefit_lines == (
        "medical",
        "dental",
        "vision",
    )
    assert by_name["BCBS Wyoming"].aliases == (
        "Blue Cross and Blue Shield of Wyoming",
        "Blue Cross Blue Shield Wyoming",
        "Blue Cross Blue Shield of Wyoming",
        "BCBSWY",
        "BCBSWY Dental",
        "BCBSWY Vision",
    )
    assert by_name["WPS Health"].hosting_platform == "html_mrf_links"
    assert by_name["WPS Health"].aliases == ("Wisconsin Physicians Service", "WPS")
    assert by_name["SummaCare"].hosting_platform == "html_mrf_links"
    assert by_name["SummaCare"].aliases == ("SummaCare MEWA", "Summa Health System")
    assert by_name["Health Alliance Plan"].hosting_platform == "healthsparq"
    assert by_name["Peak Health"].hosting_platform == "html_mrf_links"
    assert by_name["Centivo - Rockwell Automation"].hosting_platform == "html_mrf_links"
    assert by_name["HealthSmart"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["HealthSmart"].benefit_lines == ("medical", "dental")
    assert by_name["HealthSmart"].aliases == (
        "HealthSmart Benefit Solutions",
        "HealthSmart-Dental",
        "HealthSmart Dental",
    )
    assert by_name["UCare"].hosting_platform == "html_mrf_links"
    assert by_name["UCare"].benefit_lines == ("medical", "dental")
    assert by_name["UCare"].aliases == ("UCare Minnesota", "UCare IFP")
    mercycare_list = [
        candidate for candidate in candidates if candidate.payer_name == "Mercy/MercyCare"
    ]
    assert len(mercycare_list) == 2
    assert {candidate.hosting_platform for candidate in mercycare_list} == {"html_mrf_links"}
    assert all(candidate.benefit_lines == ("medical",) for candidate in mercycare_list)


@pytest.mark.asyncio
async def test_master_list_uses_current_public_source_urls_for_selected_payers():
    candidates = await discovery._load_candidates(
        "master-list", test_mode=True, limit=2000
    )
    by_name = {}
    for candidate in candidates:
        by_name.setdefault(candidate.payer_name, []).append(candidate)

    healthfirst = by_name["Healthfirst"]
    assert {candidate.index_url for candidate in healthfirst} == {
        "https://tic.healthfirst.org/table-of-contents-hixplan.json",
        "https://tic.healthfirst.org/table-of-contents-hixplan-prof.json",
        "https://tic.healthfirst.org/table-of-contents-hixplan-inst.json",
    }
    assert {candidate.hosting_platform for candidate in healthfirst} == {"direct_toc"}

    assert by_name["Oscar Health"][0].hosting_platform == "oscar_s3_monthly_toc"
    assert by_name["MetroPlus Health"][0].index_url == (
        "https://metroplus.org/machine-readable-files/"
    )
    assert by_name["MetroPlus Health"][0].hosting_platform == "html_mrf_links"
    assert by_name["Tufts Health Plan"][0].index_url == (
        "https://tuftshealthplan.com/legal-notices/machine-readable-files"
    )
    assert by_name["Tufts Health Plan"][0].hosting_platform == (
        "point32_azure_mrf_directory"
    )
    assert by_name["University of Utah HP"][0].index_url == (
        "https://uhealthplan.utah.edu/machine-readable-data"
    )
    assert by_name["University of Utah HP"][0].hosting_platform == "html_mrf_links"
    molina = by_name["Molina Healthcare"]
    assert any(
        candidate.index_url
        == "https://www.molinamarketplace.com/marketplace/oh/en-us/About/compinfo/PricingTransparency"
        and candidate.status == "active"
        and discovery._is_candidate_importable_source(candidate)
        for candidate in molina
    )
    assert any(
        candidate.index_url
        == "https://www.molinahealthcare.com/members/common/mrf.aspx"
        and candidate.status == "unsupported"
        and not discovery._is_candidate_importable_source(candidate)
        for candidate in molina
    )
    assert by_name["Paramount Health Care"][0].index_url == (
        "https://paramount.healthsparq.com/healthsparq/public/#/one/"
        "insurerCode=PARAMOUNT_I&brandCode=PARAMOUNT/"
        "machine-readable-transparency-in-coverage"
    )
    assert by_name["Paramount Health Care"][0].hosting_platform == "healthsparq"


@pytest.mark.asyncio
async def test_master_list_keeps_high_value_public_aliases():
    """Verify this source-discovery regression contract."""
    candidates = await discovery._load_candidates(
        "master-list", test_mode=True, limit=2000
    )
    by_name = {candidate.payer_name: candidate for candidate in candidates}
    active_humana_candidates = [
        candidate
        for candidate in candidates
        if candidate.payer_name == "Humana" and candidate.status == "active"
    ]
    assert len(active_humana_candidates) == 1
    active_humana = active_humana_candidates[0]
    assert active_humana.source_tier == "coverage_evidence"
    assert active_humana.benefit_lines == ("medical", "dental", "vision")
    assert not discovery._is_candidate_importable_source(active_humana)
    aliases_by_name = {}
    for candidate in candidates:
        aliases_by_name.setdefault(candidate.payer_name, set()).update(
            candidate.aliases
        )
    benefit_lines_by_ancillary_name = {
        "Apta Health Coverage": ("medical",),
        "Allegiance Ancillary Claims Coverage": ("dental", "vision"),
        "Auxiant Ancillary Benefits": ("dental", "vision"),
        "ASR Health Benefits Dental and Vision Coverage": ("dental", "vision"),
        "BCBS Alabama Dental Coverage": ("dental",),
        "BCBS Arizona Dental Coverage": ("dental",),
        "BCBS Illinois Dental Coverage": ("dental",),
        "BCBS Kansas Dental and Vision Coverage": ("dental", "vision"),
        "BCBS Kansas City Dental and Vision Coverage": ("dental", "vision"),
        "BCBS Massachusetts Vision Coverage": ("vision",),
        "BCBS Montana Dental Coverage": ("dental",),
        "BCBS Montana Vision Coverage": ("vision",),
        "BCBS New Mexico Dental Coverage": ("dental",),
        "BCBS North Carolina Dental Coverage": ("dental",),
        "BCBS North Carolina Vision Coverage": ("vision",),
        "BCBS Oklahoma Dental Coverage": ("dental",),
        "BCBS South Carolina Dental Coverage": ("dental",),
        "BCBS South Carolina Vision Coverage": ("vision",),
        "BCBS Tennessee Dental and Vision Coverage": ("dental", "vision"),
        "BCBS Texas Dental Coverage": ("dental",),
        "BAS Ancillary Benefits": ("dental", "vision"),
        "Beam Benefits Ancillary Coverage": ("dental", "vision"),
        "BEST Life Dental and Vision Coverage": ("dental", "vision"),
        "ByWater Ancillary Benefits": ("dental", "vision"),
        "CBA Blue Dental Coverage": ("dental",),
        "CarePlus Dental Plans Coverage": ("dental",),
        "ClaimChoice Administrators Coverage": ("medical",),
        "Consociate Health Ancillary Benefits": ("dental", "vision"),
        "Crescent Dental Coverage": ("dental",),
        "Direct Dental Coverage": ("dental",),
        "Diversified Group Ancillary Coverage": ("dental", "vision"),
        "Dominion National Dental": ("dental",),
        "Dominion National Vision": ("vision",),
        "EBPA Medical and Dental Coverage": ("dental",),
        "EMI Health Vision Coverage": ("vision",),
        "Ameritas Dental": ("dental",),
        "Ameritas Vision": ("vision",),
        "HealthPartners Dental Coverage": ("dental",),
        "Guardian Dental": ("dental",),
        "Guardian Vision": ("vision",),
        "Healthgram Dental and Vision Coverage": ("dental", "vision"),
        "Highmark Dental and Vision Coverage": ("dental", "vision"),
        "HNAS Dental Network Coverage": ("dental",),
        "HRI Dental and Vision": ("dental", "vision"),
        "Humana Dental": ("dental",),
        "Humana Vision": ("vision",),
        "IAEC Dental and Vision Plans": ("dental", "vision"),
        "International Medical Solutions Coverage": ("medical", "dental", "vision"),
        "LIBERTY Dental Plan": ("dental",),
        "Loomis Dental Coverage": ("dental",),
        "Lucent Health Ancillary Coverage": ("dental", "vision"),
        "MetLife Dental Coverage": ("dental",),
        "MedBen Dental and Vision Coverage": ("dental", "vision"),
        "Meritain Health Vision Coverage": ("vision",),
        "Mutual of Omaha Dental": ("dental",),
        "Mutual of Omaha Vision": ("vision",),
        "PacificSource Vision Coverage": ("vision",),
        "Principal Dental": ("dental",),
        "Principal Vision": ("vision",),
        "Renaissance Dental": ("dental",),
        "Renaissance Vision": ("vision",),
        "Reliance Matrix Dental and Vision": ("dental", "vision"),
        "Smile Brands Ancillary Coverage": ("dental", "vision"),
        "Sun Life Dental": ("dental",),
        "Sun Life Vision": ("vision",),
        "Tall Tree Dental Coverage": ("dental",),
        "Tall Tree Vision Coverage": ("vision",),
        "The Standard Dental": ("dental",),
        "The Standard Vision": ("vision",),
        "TruAssure Dental": ("dental",),
        "UHA Dental Coverage": ("dental",),
        "UHA Vision Coverage": ("vision",),
        "UPMC Dental and Vision Coverage": ("dental", "vision"),
        "Wellmark Blue Dental Coverage": ("dental",),
    }
    ancillary_by_name = {
        candidate.payer_name: candidate
        for candidate in candidates
        if candidate.payer_name in benefit_lines_by_ancillary_name
    }

    assert "Wellmark Blue Cross and Blue Shield" in by_name["Wellmark"].aliases
    assert "Wellmark Health Plan of Iowa, Inc." in by_name["Wellmark"].aliases
    assert "Wellmark Blue Dental" in by_name["Wellmark"].aliases
    assert (
        "Blue Cross Blue Shield Global Solutions"
        in by_name["BCBS Global Solutions"].aliases
    )
    assert "Blue Cross and Blue Shield of Kansas, Inc." in by_name["BCBS Kansas"].aliases
    assert "Meritain Health An Aetna Company" in by_name["Meritain Health"].aliases
    assert "Meritain Health, An Aetna Company" in by_name["Meritain Health"].aliases
    assert (
        "MERITAIN HEALTH NORTH AMERICAN HEALTH PLAN"
        in by_name["Meritain Health"].aliases
    )
    assert (
        "MERITAIN HEALTH (NORTH AMERICAN HEALTH PLAN)"
        in by_name["Meritain Health"].aliases
    )
    meritain_health1_list = [
        candidate
        for candidate in candidates
        if candidate.payer_name == "Meritain Health"
        and "MERITAINOVER" in candidate.index_url
    ]
    assert len(meritain_health1_list) in {1}
    assert meritain_health1_list[0].benefit_lines == ("medical", "dental")
    assert by_name["Firefly Health"].entity_type == "dtc"
    assert by_name["Firefly Health"].benefit_lines == ("medical",)
    assert by_name["Firefly Health"].source_tier == "coverage_evidence"
    assert by_name["Firefly Health"].status == "active"
    assert by_name["Firefly Health"].index_url == "https://www.fireflyhealth.com/pricing-transparency/"
    assert "Firefly Health Plan" in by_name["Firefly Health"].aliases
    assert by_name["Clover Health"].status == "stale"
    assert by_name["Clover Health Employee Benefits"].entity_type == "group"
    assert (
        by_name["Clover Health Employee Benefits"].hosting_platform
        == "uhc_public_blobs"
    )
    assert by_name["Clover Health Employee Benefits"].benefit_lines == (
        "medical",
        "dental",
        "vision",
    )
    assert (
        by_name["Clover Health Employee Benefits"].raw_payload["target_payer_query"]
        == "Clover Health"
    )
    assert "Clover Health" in by_name["Clover Health Employee Benefits"].aliases
    assert by_name["Devoted Health"].status == "stale"
    assert by_name["Geisinger Health Plan"].status == "unsupported"
    assert by_name["OSF HealthCare"].status == "stale"
    assert by_name["Providence Health Plan"].status == "stale"
    assert by_name["SSM Health Plan"].status == "stale"
    assert by_name["Farm Bureau Health Plans"].status == "unsupported"
    assert "The Standard AHL" in aliases_by_name["Meritain Health"]
    assert "American Heritage Life" in aliases_by_name["Meritain Health"]
    assert (
        "American Heritage Life Insurance Company"
        in aliases_by_name["Meritain Health"]
    )
    assert "Benefits and Risk Management Services" in aliases_by_name["BRMS"]
    assert "Benefits & Risk Management Services" in aliases_by_name["BRMS"]
    assert by_name["BRMS"].benefit_lines == ("medical", "dental")
    assert "Aetna Dental" in aliases_by_name["Aetna"]
    assert by_name["Aetna"].benefit_lines == ("medical", "dental")
    assert by_name["AmeriHealth Caritas Next"].benefit_lines == (
        "medical",
        "dental",
    )
    assert "The Standard AHL" in aliases_by_name["Allied Benefit Systems"]
    assert (
        "American Heritage Life Insurance Company"
        in aliases_by_name["Allied Benefit Systems"]
    )
    assert "Dental Benefit Providers" in by_name["United Healthcare"].aliases
    assert "Dental Benefit Providers DBP" in by_name["United Healthcare"].aliases
    assert "DBP" in by_name["United Healthcare"].aliases
    assert "DBP Network" in by_name["United Healthcare"].aliases
    assert "United Healthcare Dental" in by_name["United Healthcare"].aliases
    assert "UHC Vision" in by_name["United Healthcare"].aliases
    assert "UHC Vision Using Spectera Network" in by_name["United Healthcare"].aliases
    assert "UHC Vision (Using Spectera Network)" in by_name["United Healthcare"].aliases
    assert "UMR (Using Spectera Network)" in by_name["United Healthcare"].aliases
    assert "Lincoln Financial Group - Spectera" in by_name["United Healthcare"].aliases
    assert (
        "The Lincoln National Life Insurance Company"
        in by_name["United Healthcare"].aliases
    )
    assert "UHC Global" in by_name["United Healthcare"].aliases
    assert "Kansas City Life" in by_name["United Healthcare"].aliases
    assert "Kansas City Life Insurance Company" in by_name["United Healthcare"].aliases
    assert by_name["United Healthcare"].benefit_lines == (
        "medical",
        "dental",
        "vision",
    )
    assert by_name["UnitedHealthcare"].benefit_lines == (
        "medical",
        "dental",
        "vision",
    )
    assert by_name["Elevance Health"].benefit_lines == (
        "medical",
        "dental",
        "vision",
    )
    assert "Empire BlueCross BlueShield" in by_name["Anthem"].aliases
    assert "Empire HealthChoice HMO, Inc." in by_name["Anthem"].aliases
    assert "Delta Dental of Kentucky" in by_name["Anthem"].aliases
    assert "DeltaDentalKY" in by_name["Anthem"].aliases
    assert by_name["CareFirst BCBS"].benefit_lines == (
        "medical",
        "dental",
        "vision",
    )
    assert "Employee Benefit Management Services EBMS" in by_name["EBMS"].aliases
    assert "Independence Administrators" in by_name["Independence Blue Cross"].aliases
    assert "Regence BlueShield" in by_name["Regence"].aliases
    assert "Horizon Blue Cross of New Jersey" in by_name["Horizon BCBS NJ"].aliases
    assert by_name["Horizon BCBS NJ"].benefit_lines == ("medical", "dental")
    assert by_name["Delta Dental Plan of Michigan"].entity_type == "dental"
    assert by_name["Delta Dental Plan of Michigan"].hosting_platform == "sapphire"
    assert by_name["Delta Dental Plan of Michigan"].benefit_lines == ("dental",)
    assert (
        "Delta Dental of Michigan" in by_name["Delta Dental Plan of Michigan"].aliases
    )
    assert "Delta Dental of Indiana" in by_name["Delta Dental Plan of Michigan"].aliases
    assert (
        "Delta Dental Plan of Indiana, Inc."
        in by_name["Delta Dental Plan of Michigan"].aliases
    )
    assert (
        "Delta Dental Plan of Ohio" in by_name["Delta Dental Plan of Michigan"].aliases
    )
    assert (
        "Delta Dental Plan of Ohio, Inc."
        in by_name["Delta Dental Plan of Michigan"].aliases
    )
    assert "Cigna Dental PPO" in by_name["Cigna"].aliases
    assert "Allegiance Life & Health Insurance Company" in by_name["Cigna"].aliases
    assert (
        "Allegiance Life & Health Insurance Company, Inc."
        in by_name["Cigna"].aliases
    )
    assert "Yuzu Health" in by_name["Cigna"].aliases
    assert "YUZU HEALTH INC." in by_name["Cigna"].aliases
    assert "Cigna Shared Administration PPO - Yuzu" in by_name["Cigna"].aliases
    assert by_name["Cigna"].benefit_lines == ("medical", "dental")
    assert set(ancillary_by_name) == set(benefit_lines_by_ancillary_name)
    assert all(
        candidate.source_tier == "coverage_evidence"
        for candidate in ancillary_by_name.values()
    )
    for payer_name, benefit_lines in benefit_lines_by_ancillary_name.items():
        assert ancillary_by_name[payer_name].benefit_lines == benefit_lines
    assert "Dental Blue" in ancillary_by_name["BCBS Alabama Dental Coverage"].aliases
    assert "AZ Blue" in ancillary_by_name["BCBS Arizona Dental Coverage"].aliases
    assert (
        "EyeMed"
        in ancillary_by_name["BCBS Kansas Dental and Vision Coverage"].aliases
    )
    assert (
        "Blue KC Vision"
        in ancillary_by_name["BCBS Kansas City Dental and Vision Coverage"].aliases
    )
    assert (
        "Blue 20/20"
        in ancillary_by_name["BCBS Massachusetts Vision Coverage"].aliases
    )
    assert (
        "Blue Cross Blue Shield North Carolina"
        in ancillary_by_name["BCBS North Carolina Dental Coverage"].aliases
    )
    assert (
        "Dental Blue Select"
        in ancillary_by_name["BCBS North Carolina Dental Coverage"].aliases
    )
    assert (
        "Blue 20/20"
        in ancillary_by_name["BCBS North Carolina Vision Coverage"].aliases
    )
    assert (
        "Healthy Vision"
        in ancillary_by_name["BCBS South Carolina Vision Coverage"].aliases
    )
    assert (
        "BlueCross Dental"
        in ancillary_by_name["BCBS Tennessee Dental and Vision Coverage"].aliases
    )
    assert (
        "Blue Cross Blue Shield TN"
        in ancillary_by_name["BCBS Tennessee Dental and Vision Coverage"].aliases
    )
    assert "Apta Health" in ancillary_by_name["Apta Health Coverage"].aliases
    assert (
        "Allegiance Benefit Plan Management Inc"
        in ancillary_by_name["Allegiance Ancillary Claims Coverage"].aliases
    )
    assert "Claim Choice" in ancillary_by_name["ClaimChoice Administrators Coverage"].aliases
    assert "Care Plus" in ancillary_by_name["CarePlus Dental Plans Coverage"].aliases
    assert (
        "Crescent Employee Benefits"
        in ancillary_by_name["Crescent Dental Coverage"].aliases
    )
    assert "Tall Tree" in ancillary_by_name["Tall Tree Vision Coverage"].aliases
    assert "Benefit Allocation Systems" in ancillary_by_name["BAS Ancillary Benefits"].aliases
    assert "MyEnroll360" in ancillary_by_name["BAS Ancillary Benefits"].aliases
    assert "Choose ByWater" in ancillary_by_name["ByWater Ancillary Benefits"].aliases
    assert "Dental Blue Network" in ancillary_by_name["CBA Blue Dental Coverage"].aliases
    assert "Direct Dental Administrators" in ancillary_by_name["Direct Dental Coverage"].aliases
    assert (
        "Lucent Health Solutions"
        in ancillary_by_name["Lucent Health Ancillary Coverage"].aliases
    )
    assert (
        "Diversified Group Brokerage"
        in ancillary_by_name["Diversified Group Ancillary Coverage"].aliases
    )
    assert "VSP Choice" in ancillary_by_name["EMI Health Vision Coverage"].aliases
    assert (
        "HealthPartners Dental"
        in ancillary_by_name["HealthPartners Dental Coverage"].aliases
    )
    assert (
        "Healthgram Dental"
        in ancillary_by_name["Healthgram Dental and Vision Coverage"].aliases
    )
    assert "IAEC Dental Plan" in ancillary_by_name["IAEC Dental and Vision Plans"].aliases
    assert "IAEC Vision Plan" in ancillary_by_name["IAEC Dental and Vision Plans"].aliases
    assert (
        "Blue Edge Dental"
        in ancillary_by_name["Highmark Dental and Vision Coverage"].aliases
    )
    assert "IMS" in ancillary_by_name["International Medical Solutions Coverage"].aliases
    assert (
        "Aetna Vision Preferred"
        in ancillary_by_name["Meritain Health Vision Coverage"].aliases
    )
    assert "Pacific Source" in ancillary_by_name["PacificSource Vision Coverage"].aliases
    assert "ASR Vision" in ancillary_by_name["ASR Health Benefits Dental and Vision Coverage"].aliases
    assert (
        "BEST Life and Health Insurance Company"
        in ancillary_by_name["BEST Life Dental and Vision Coverage"].aliases
    )
    assert "Dominion National" in ancillary_by_name["Dominion National Dental"].aliases
    assert "Dominion National" in ancillary_by_name["Dominion National Vision"].aliases
    assert "Guardian Dental" in ancillary_by_name["Guardian Dental"].aliases
    assert "Guardian Life Insurance Company" in ancillary_by_name["Guardian Vision"].aliases
    assert "HumanaDental Insurance Company" in ancillary_by_name["Humana Dental"].aliases
    assert "CompBenefits Vision" in ancillary_by_name["Humana Vision"].aliases
    assert "Principal Financial Group Dental" in ancillary_by_name["Principal Dental"].aliases
    assert "Principal Financial Group" in ancillary_by_name["Principal Vision"].aliases
    assert "Ameritas Dental" in ancillary_by_name["Ameritas Dental"].aliases
    assert "Fusion Vision" in ancillary_by_name["Ameritas Vision"].aliases
    assert "reliancematrix" in ancillary_by_name["Reliance Matrix Dental and Vision"].aliases
    assert "SunLife Dental" in ancillary_by_name["Sun Life Dental"].aliases
    assert "Sunlife Financial" in ancillary_by_name["Sun Life Vision"].aliases
    assert "Standard Dental" in ancillary_by_name["The Standard Dental"].aliases
    assert "Standard Vision" in ancillary_by_name["The Standard Vision"].aliases
    assert "TruAssure Dental" in ancillary_by_name["TruAssure Dental"].aliases
    assert "Liberty Dental" in ancillary_by_name["LIBERTY Dental Plan"].aliases
    assert "Liberty Dental Plan" in ancillary_by_name["LIBERTY Dental Plan"].aliases
    assert (
        "Mutual of Omaha Insurance Company"
        in ancillary_by_name["Mutual of Omaha Dental"].aliases
    )
    assert (
        "United of Omaha Life Insurance Company"
        in ancillary_by_name["Mutual of Omaha Vision"].aliases
    )
    assert (
        "Renaissance Life & Health Insurance Company"
        in ancillary_by_name["Renaissance Dental"].aliases
    )
    assert (
        "Renaissance Life & Health Insurance Company"
        in ancillary_by_name["Renaissance Vision"].aliases
    )
    assert "HealthNow Administrative Services Dental" in ancillary_by_name["HNAS Dental Network Coverage"].aliases
    assert "Loomis Benefit Administrators" in ancillary_by_name["Loomis Dental Coverage"].aliases
    assert "MedBen VisionPlus" in ancillary_by_name["MedBen Dental and Vision Coverage"].aliases
    assert "Met Life Dental" in ancillary_by_name["MetLife Dental Coverage"].aliases
    assert "Hawaii Dental Service" in ancillary_by_name["UHA Dental Coverage"].aliases
    assert "UHA Vision" in ancillary_by_name["UHA Vision Coverage"].aliases
    assert "UPMC Dental Advantage" in ancillary_by_name["UPMC Dental and Vision Coverage"].aliases
    assert (
        "Wellmark Blue Dental"
        in ancillary_by_name["Wellmark Blue Dental Coverage"].aliases
    )
    assert "Horizon Healthcare Dental" in by_name["Horizon BCBS NJ"].aliases
    assert by_name["HMSA"].benefit_lines == ("medical", "vision")
    assert "HMSA Vision" in by_name["HMSA"].aliases
    assert by_name["Independence Blue Cross"].benefit_lines == ("medical", "vision")
    for alias in (
        "Davis Vision",
        "Davis Vision Network",
        "Davis Vision by MetLife",
        "MetLife Davis Vision",
        "MetLife Vision with Davis Vision",
    ):
        assert alias in by_name["Independence Blue Cross"].aliases
    assert "NVA" in aliases_by_name["Capital Blue Cross"]
    assert "National Vision Administrators" in aliases_by_name["Capital Blue Cross"]
    assert (
        "National Vision Administrators (NVA)"
        in aliases_by_name["Capital Blue Cross"]
    )
    assert "National Vision Administrators NVA" in aliases_by_name["Capital Blue Cross"]
    assert "Capital Blue Cross Vision NVA" in aliases_by_name["Capital Blue Cross"]
    assert "Capital Blue Cross NVA" in aliases_by_name["Capital Blue Cross"]
    assert by_name["Capital Blue Cross"].benefit_lines == ("medical", "vision")
    assert by_name["BCBS North Carolina"].benefit_lines == ("medical",)
    assert by_name["Davis Vision"].status == "active"
    assert by_name["Davis Vision"].index_url == "https://www.ibx.com/cmstic/?brand=qcc"
    assert by_name["Davis Vision"].hosting_platform == "cmstic_file_info"
    assert by_name["Davis Vision"].benefit_lines == ("vision",)
    for alias in (
        "Davis Vision by MetLife",
        "Versant Health Davis Vision",
        "Davis Vision Network",
        "Davis Vision by Versant Health",
        "MetLife Davis Vision",
        "MetLife Vision with Davis Vision",
        "Davis Vision Guardian",
        "Guardian - Davis Vision",
        "Guardian Vision with Davis Vision",
        "Highmark Davis Vision",
    ):
        assert alias in by_name["Davis Vision"].aliases
    assert by_name["Superior Vision"].status == "active"
    assert by_name["Superior Vision"].source_tier == "coverage_evidence"
    assert by_name["Superior Vision"].index_url == "https://superiorvision.com/"
    assert by_name["Superior Vision"].benefit_lines == ("vision",)
    for alias in (
        "Versant Health Superior Vision",
        "Superior Vision Network",
        "Superior Vision by Versant Health",
        "Versant Health Superior Vision Network",
        "MetLife Superior Vision",
        "MetLife Vision Superior Vision",
        "MetLife Vision with Superior Vision",
        "SuperiorVision",
        "Cypress Admin (Superior Vision Network)",
    ):
        assert alias in by_name["Superior Vision"].aliases
    assert by_name["EyeMed"].entity_type == "vision"
    assert by_name["EyeMed"].benefit_lines == ("vision",)
    assert by_name["EyeMed"].hosting_platform == "direct_mrf_body"
    assert "EyeMed Vision Care" in by_name["EyeMed"].aliases
    assert "Eye Med" in by_name["EyeMed"].aliases
    assert "Aetna Vision" in by_name["EyeMed"].aliases
    assert "Aetna Vision Preferred" in by_name["EyeMed"].aliases
    assert "Aetna Vision with EyeMed" in by_name["EyeMed"].aliases
    assert "Aetna Vision EyeMed" in by_name["EyeMed"].aliases
    assert "Meritain Health Vision" in by_name["EyeMed"].aliases
    assert "Meritain Health Aetna Vision Preferred" in by_name["EyeMed"].aliases
    assert "Meritain Health, An Aetna Company" in by_name["EyeMed"].aliases
    assert "Ameritas Vision EyeMed" in by_name["EyeMed"].aliases
    assert "Ameritas Vision with EyeMed" in by_name["EyeMed"].aliases
    assert "Ameritas with EyeMed" in by_name["EyeMed"].aliases
    assert "Ameritas Life Insurance Corp. EyeMed" in by_name["EyeMed"].aliases
    assert "Ameritas Life Ins Corp. EyeMed" in by_name["EyeMed"].aliases
    assert "Mututal of Omaha" in by_name["EyeMed"].aliases
    assert "Mutual of Omaha with EyeMed" in by_name["EyeMed"].aliases
    assert "Mutual of Omaha Vision with EyeMed" in by_name["EyeMed"].aliases
    assert "Mutual of Omaha Insurance Company EyeMed" in by_name["EyeMed"].aliases
    assert (
        "Mutual of Omaha Insurance Company Vision with EyeMed"
        in by_name["EyeMed"].aliases
    )
    assert "BlueCare Vision of Texas (powered by EyeMed)" in by_name["EyeMed"].aliases
    assert (
        "Blue 20/20 of Massachusetts (powered by EyeMed)"
        in by_name["EyeMed"].aliases
    )
    assert (
        "BlueCare Vision of Illinois (powered by EyeMed)"
        in by_name["EyeMed"].aliases
    )
    assert "Cigna Vision serviced by EyeMed" in by_name["EyeMed"].aliases
    assert "Cigna Vision with EyeMed" in by_name["EyeMed"].aliases
    assert "Cigna Vision EyeMed" in by_name["EyeMed"].aliases
    assert "Dearborn EyeMed" in by_name["EyeMed"].aliases
    assert "Dearborn National with EyeMed" in by_name["EyeMed"].aliases
    assert "Delta Vision EyeMed" in by_name["EyeMed"].aliases
    assert "Delta Vision with EyeMed" in by_name["EyeMed"].aliases
    assert "DeltaVision EyeMed" in by_name["EyeMed"].aliases
    assert "DeltaVision administered by EyeMed" in by_name["EyeMed"].aliases
    assert "DeltaVision with EyeMed" in by_name["EyeMed"].aliases
    assert "The Standard Vision EyeMed" in by_name["EyeMed"].aliases
    assert "Standard Insurance Company EyeMed" in by_name["EyeMed"].aliases
    assert "Unum Vision Powered by Eyemed" in by_name["EyeMed"].aliases
    assert "Unum Vision EyeMed" in by_name["EyeMed"].aliases
    assert "Starmount Life EyeMed" in by_name["EyeMed"].aliases
    assert "Starmount Life Insurance Company EyeMed" in by_name["EyeMed"].aliases
    assert "Surency" in by_name["EyeMed"].aliases
    assert "Surency Vision" in by_name["EyeMed"].aliases
    assert "Surency EyeMed" in by_name["EyeMed"].aliases
    assert "Companion Life EyeMed" in by_name["EyeMed"].aliases
    assert "Companion Life Vision EyeMed" in by_name["EyeMed"].aliases
    assert "ProTec" in by_name["EyeMed"].aliases
    assert "Advantica" in by_name["EyeMed"].aliases
    assert "HealthLink Network" in by_name["HealthLink"].aliases
    assert by_name["HealthLink"].hosting_platform == "anthem_s3_mrf"
    active_connecticare = next(
        candidate
        for candidate in candidates
        if candidate.payer_name == "ConnectiCare" and candidate.status == "active"
    )
    assert (
        "https://www.connecticare.com/about-us/transparency-coverage-compliance"
        in discovery._candidate_metadata(
            active_connecticare, active_connecticare.aliases
        )["supersedes_urls"]
    )
    assert by_name["Aspirus Health Plan"].hosting_platform == "healthsparq"
    assert by_name["Aspirus Health Plan"].index_url == (
        "https://aspirus.healthsparq.com/healthsparq/public/#/one/"
        "&insurerCode=ASPIRUS_I&brandCode=ASPIRUS/"
        "machine-readable-transparency-in-coverage"
    )
    assert by_name["Aspirus Health Plan"].raw_payload["notes"]
    assert (
        "https://aspirushealthplan.com/insurance/pricingTransparency"
        in discovery._candidate_metadata(
            by_name["Aspirus Health Plan"], by_name["Aspirus Health Plan"].aliases
        )["supersedes_urls"]
    )
    assert by_name["Moda Health"].benefit_lines == ("medical", "dental")
    assert "Delta Dental of Oregon" in by_name["Moda Health"].aliases
    assert by_name["VSP Vision"].hosting_platform == "sapphire"
    assert by_name["VSP Vision"].benefit_lines == ("vision",)
    assert "VSP" in by_name["VSP Vision"].aliases
    assert "Vision Service Plan (VSP)" in by_name["VSP Vision"].aliases
    assert "Vision Service Plan VSP" in by_name["VSP Vision"].aliases
    assert "Vision Plan Service (VSP)" in by_name["VSP Vision"].aliases
    assert "Vision Plan Service VSP" in by_name["VSP Vision"].aliases
    assert "Vision Serivce Plan (VSP)" in by_name["VSP Vision"].aliases
    assert "VSP Choice Network" in by_name["VSP Vision"].aliases
    assert "EMI Health" in by_name["VSP Vision"].aliases
    assert "EMI Health Vision" in by_name["VSP Vision"].aliases
    assert "EMI Health VSP" in by_name["VSP Vision"].aliases
    assert "Community Eye Care" in by_name["VSP Vision"].aliases
    assert "CEC Community Eye Care" in by_name["VSP Vision"].aliases
    assert "CEC Vision" in by_name["VSP Vision"].aliases
    assert discovery._is_candidate_text_filter_match(
        by_name["VSP Vision"], entity_types=(), payer_query="Community Eye Care"
    )
    assert "Guardian VSP Network" in by_name["VSP Vision"].aliases
    assert "Guardian/VSP" in by_name["VSP Vision"].aliases
    assert "Guardian/VSP Vision" in by_name["VSP Vision"].aliases
    assert "Guardian Vision" in by_name["VSP Vision"].aliases
    assert "Guardian Vision VSP" in by_name["VSP Vision"].aliases
    assert "Guardian Vision with VSP" in by_name["VSP Vision"].aliases
    assert "Guardian Vision Powered by VSP" in by_name["VSP Vision"].aliases
    assert "Guardian Life Insurance Company VSP" in by_name["VSP Vision"].aliases
    assert (
        "Guardian Life Insurance Company of America VSP"
        in by_name["VSP Vision"].aliases
    )
    assert (
        "The Guardian Life Insurance Company VSP" in by_name["VSP Vision"].aliases
    )
    assert (
        "The Guardian Life Insurance Company of America VSP"
        in by_name["VSP Vision"].aliases
    )
    assert "VSP Guardian" in by_name["VSP Vision"].aliases
    assert "VSP Service Plan" in by_name["VSP Vision"].aliases
    assert "VPS Vision Care" in by_name["VSP Vision"].aliases
    assert "Principal Financial Group VSP" in by_name["VSP Vision"].aliases
    assert "Principal Financial Group - VSP" in by_name["VSP Vision"].aliases
    assert "Principal / VSP" in by_name["VSP Vision"].aliases
    assert "Principal Vision VSP" in by_name["VSP Vision"].aliases
    assert "Principal Vision with VSP" in by_name["VSP Vision"].aliases
    assert "Principal Life Insurance Company VSP" in by_name["VSP Vision"].aliases
    assert "MetLife VSP Choice" in by_name["VSP Vision"].aliases
    assert "MetLife Vision VSP" in by_name["VSP Vision"].aliases
    assert "MetLife using VSP Choice Network" in by_name["VSP Vision"].aliases
    assert "Metlife / VSP" in by_name["VSP Vision"].aliases
    assert "Sun Life Vision with VSP" in by_name["VSP Vision"].aliases
    assert "SunLife Vision with VSP" in by_name["VSP Vision"].aliases
    assert "Sun Life Vision VSP" in by_name["VSP Vision"].aliases
    assert "SunLife/VSP" in by_name["VSP Vision"].aliases
    assert (
        "Sun Life and Health Insurance Company VSP"
        in by_name["VSP Vision"].aliases
    )
    assert (
        "Sun Life and Health Insurance Company (U.S.) VSP"
        in by_name["VSP Vision"].aliases
    )
    assert "Ameritas Vision VSP" in by_name["VSP Vision"].aliases
    assert "Ameritas Vision with VSP" in by_name["VSP Vision"].aliases
    assert "Ameritas with VSP" in by_name["VSP Vision"].aliases
    assert "Ameritas Life Insurance Corp. VSP" in by_name["VSP Vision"].aliases
    assert "Ameritas Life Ins Corp. VSP" in by_name["VSP Vision"].aliases
    assert "VSP Ameritas" in by_name["VSP Vision"].aliases
    assert "Equitable Vision VSP" in by_name["VSP Vision"].aliases
    assert "The Standard Vision VSP" in by_name["VSP Vision"].aliases
    assert "Standard Insurance Company VSP" in by_name["VSP Vision"].aliases
    assert "Renaissance Vision VSP" in by_name["VSP Vision"].aliases
    assert (
        "Renaissance Life & Health Insurance Company VSP"
        in by_name["VSP Vision"].aliases
    )
    assert (
        "Renaissance Life & Health Insurance Company of America VSP"
        in by_name["VSP Vision"].aliases
    )
    assert "Beam Benefits VSP" in by_name["VSP Vision"].aliases
    assert "Companion Life VSP" in by_name["VSP Vision"].aliases
    assert "DeltaVision" in by_name["VSP Vision"].aliases
    assert "Delta Vision VSP" in by_name["VSP Vision"].aliases
    assert "Delta Vision with VSP" in by_name["VSP Vision"].aliases
    assert "DeltaVision VSP" in by_name["VSP Vision"].aliases
    assert "DeltaVision with VSP" in by_name["VSP Vision"].aliases
    assert "United Heritage (Using VSP Network)" in by_name["VSP Vision"].aliases
    assert by_name["Vision Benefits of America"].status == "active"
    assert by_name["Vision Benefits of America"].source_tier == "coverage_evidence"
    assert by_name["Vision Benefits of America"].benefit_lines == ("vision",)
    assert "VBA" in by_name["Vision Benefits of America"].aliases
    assert "Vision Benefits of America VBA" in by_name["Vision Benefits of America"].aliases
    assert (
        "Vision Benefits of America (VBA)"
        in by_name["Vision Benefits of America"].aliases
    )
    assert "Vision Benefits of Ameriva" in by_name["Vision Benefits of America"].aliases
    assert "VBA Vision Network" in by_name["Vision Benefits of America"].aliases
    assert by_name["Avesis"].status == "active"
    assert by_name["Avesis"].source_tier == "coverage_evidence"
    assert by_name["Avesis"].benefit_lines == ("vision",)
    assert "Avesis Vision" in by_name["Avesis"].aliases
    assert "Avesis Vision Network" in by_name["Avesis"].aliases
    assert "Guardian Vision Avesis" in by_name["Avesis"].aliases
    assert "Guardian Vision with Avesis" in by_name["Avesis"].aliases
    for coverage_evidence_vision_name in (
        "Dominion National Vision",
        "Group Vision Service",
        "SecureCare Vision",
        "Vision Care Direct",
        "Humana Vision",
        "Guardian Vision",
        "MetLife Vision",
        "Principal Vision",
        "Ameritas Vision",
        "Sun Life Vision",
        "Equitable Vision",
        "The Standard Vision",
        "Renaissance Vision",
        "Pacific Life Vision",
        "Mutual of Omaha Vision",
    ):
        assert by_name[coverage_evidence_vision_name].entity_type == "vision"
        assert by_name[coverage_evidence_vision_name].benefit_lines == ("vision",)
        assert by_name[coverage_evidence_vision_name].status == "active"
        assert by_name[coverage_evidence_vision_name].source_tier == "coverage_evidence"
        assert by_name[coverage_evidence_vision_name].index_url
    for review_only_name in (
        "HMSA Vision",
        "Anthem Vision Plan",
        "Best Life Vision",
    ):
        assert by_name[review_only_name].entity_type == "vision"
        assert by_name[review_only_name].benefit_lines == ("vision",)
        assert by_name[review_only_name].status == "needs_review"
        assert by_name[review_only_name].index_url is None
    assert "Dominion National" in by_name["Dominion National Vision"].aliases
    assert "GVS" in by_name["Group Vision Service"].aliases
    assert "Humana Insurance Company" in by_name["Humana Vision"].aliases
    assert "SecureCare" in by_name["SecureCare Vision"].aliases
    assert "Vision Care Direct" in by_name["Vision Care Direct"].aliases
    assert "Guardian" in by_name["Guardian Vision"].aliases
    assert "Guaridan" in by_name["Guardian Vision"].aliases
    assert "MetLife" in by_name["MetLife Vision"].aliases
    assert "Metropolitan Life Insurance Company" in by_name["MetLife Vision"].aliases
    assert "Principal Financial Group" in by_name["Principal Vision"].aliases
    assert "Principle" in by_name["Principal Vision"].aliases
    assert "Ameritas" in by_name["Ameritas Vision"].aliases
    assert (
        "Standard Life and Casualty Insurance Company"
        in by_name["The Standard Vision"].aliases
    )
    assert "Ameritas Life Ins. Corp." in by_name["Ameritas Vision"].aliases
    assert "The Business Council" in by_name["Ameritas Vision"].aliases
    assert "Fusion Vision" in by_name["Ameritas Vision"].aliases
    assert "SunLife" in by_name["Sun Life Vision"].aliases
    assert "SunLife Financail" in by_name["Sun Life Vision"].aliases
    assert "Equtiable" in by_name["Equitable Vision"].aliases
    assert "The Standard" in by_name["The Standard Vision"].aliases
    assert by_name["Delta Dental"].entity_type == "dental"
    assert by_name["Delta Dental"].benefit_lines == ("dental",)
    assert by_name["Delta Dental"].status == "active"
    assert by_name["Delta Dental"].source_tier == "coverage_evidence"
    assert by_name["Delta Dental"].index_url == "https://www.deltadental.com/group/"
    assert "DeltaDental" in by_name["Delta Dental"].aliases
    assert "Delta Dental of Iowa" in by_name["Delta Dental"].aliases
    assert "Delta Dental of Missouri" in by_name["Delta Dental"].aliases
    for coverage_evidence_dental_name in (
        "Dominion National Dental",
        "Florida Combined Life Dental",
        "HMSA Dental",
        "Delta Dental",
        "Guardian Dental",
        "Humana Dental",
        "Principal Dental",
        "Mutual of Omaha Dental",
        "Ameritas Dental",
        "SecureCare Dental",
        "Sun Life Dental",
        "LIBERTY Dental Plan",
        "TruAssure Dental",
        "Equitable Dental",
        "Superior Dental Care",
        "Unum Dental / Starmount Life",
        "The Standard Dental",
        "Pacific Life Dental",
        "Renaissance Dental",
        "Transwestern Dental",
    ):
        assert by_name[coverage_evidence_dental_name].entity_type == "dental"
        assert by_name[coverage_evidence_dental_name].benefit_lines == ("dental",)
        assert by_name[coverage_evidence_dental_name].status == "active"
        assert by_name[coverage_evidence_dental_name].source_tier == "coverage_evidence"
        assert by_name[coverage_evidence_dental_name].index_url
    assert by_name["Willamette Dental Group"].entity_type == "dental"
    assert by_name["Willamette Dental Group"].benefit_lines == ("dental",)
    assert by_name["Willamette Dental Group"].source_tier == "mrf_importable"
    assert by_name["Willamette Dental Group"].hosting_platform == "html_mrf_links"
    assert by_name["Willamette Dental Group"].index_url == (
        "https://www.regence.com/transparency-in-coverage/"
    )
    assert "Willamette Dental Insurance" in by_name["Willamette Dental Group"].aliases
    assert by_name["Solstice Dental"].entity_type == "dental"
    assert by_name["Solstice Dental"].benefit_lines == ("dental", "vision")
    assert by_name["Solstice Dental"].status == "active"
    assert by_name["Solstice Dental"].source_tier == "coverage_evidence"
    assert by_name["Solstice Dental"].index_url
    assert by_name["Premier Access Dental"].entity_type == "dental"
    assert by_name["Premier Access Dental"].benefit_lines == ("dental", "vision")
    assert by_name["Premier Access Dental"].status == "active"
    assert by_name["Premier Access Dental"].source_tier == "coverage_evidence"
    assert by_name["Premier Access Dental"].index_url
    for review_only_name in (
        "Highmark Blue Edge Dental",
    ):
        assert by_name[review_only_name].entity_type == "dental"
        assert by_name[review_only_name].benefit_lines == ("dental",)
        assert by_name[review_only_name].status == "needs_review"
        assert by_name[review_only_name].index_url is None
    assert "Dominion National" in by_name["Dominion National Dental"].aliases
    assert "Florida Combined Life" in by_name["Florida Combined Life Dental"].aliases
    assert "Blue Edge Dental" in by_name["Highmark Blue Edge Dental"].aliases
    assert "Humana Insurance Company" in by_name["Humana Dental"].aliases
    assert "Premier Access" in by_name["Premier Access Dental"].aliases
    assert "SecureCare" in by_name["SecureCare Dental"].aliases
    assert "Superior Dental Care, Inc." in by_name["Superior Dental Care"].aliases
    assert "Solstice Benefits, Inc." in by_name["Solstice Dental"].aliases
    assert "TruAssure" in by_name["TruAssure Dental"].aliases
    assert by_name["Guardian Dental"].entity_type == "dental"
    assert by_name["Guardian Dental"].benefit_lines == ("dental",)
    assert "Guardian" in by_name["Guardian Dental"].aliases
    assert "Guaridan" in by_name["Guardian Dental"].aliases
    assert "Guardian Life Insurance Company of America" in by_name["Guardian Dental"].aliases
    assert by_name["MetLife Dental"].status == "needs_review"
    assert by_name["MetLife Dental"].index_url is None
    assert by_name["MetLife Dental"].source_tier == "coverage_evidence"
    assert "MetLife" in by_name["MetLife Dental"].aliases
    assert "Metlife DPPO" in by_name["MetLife Dental"].aliases
    assert "Metropolitan Life Insurance Company" in by_name["MetLife Dental"].aliases
    assert "Principal" in by_name["Principal Dental"].aliases
    assert "Principle" in by_name["Principal Dental"].aliases
    assert "Principal Financial Group" in by_name["Principal Dental"].aliases
    assert "United of Omaha Life Insurance Company" in (
        by_name["Mutual of Omaha Dental"].aliases
    )
    assert "Ameritas" in by_name["Ameritas Dental"].aliases
    assert "The Business Council" in by_name["Ameritas Dental"].aliases
    assert "First Ameritas Life Insurance Corp. of New York" in (
        by_name["Ameritas Dental"].aliases
    )
    assert "SunLife" in by_name["Sun Life Dental"].aliases
    assert "SunLife Financail" in by_name["Sun Life Dental"].aliases
    assert by_name["Equitable Dental"].status == "active"
    assert by_name["Equitable Dental"].source_tier == "coverage_evidence"
    assert "Equitable" in by_name["Equitable Dental"].aliases
    assert "Equtiable" in by_name["Equitable Dental"].aliases
    assert by_name["Lincoln Financial DentalConnect"].entity_type == "dental"
    assert by_name["Lincoln Financial DentalConnect"].benefit_lines == ("dental",)
    assert by_name["Lincoln Financial DentalConnect"].status == "needs_review"
    assert by_name["Lincoln Financial DentalConnect"].index_url is None
    assert (
        "Lincoln DentalConnect"
        in by_name["Lincoln Financial DentalConnect"].aliases
    )
    assert "Lincoln Financial Group" in by_name["Lincoln Financial DentalConnect"].aliases
    assert "Lincoln" in by_name["Lincoln Financial DentalConnect"].aliases
    assert by_name["LIBERTY Dental Plan"].entity_type == "dental"
    assert by_name["LIBERTY Dental Plan"].benefit_lines == ("dental",)
    assert "Liberty Dental" in by_name["LIBERTY Dental Plan"].aliases
    assert by_name["United Concordia Dental"].entity_type == "dental"
    assert by_name["United Concordia Dental"].benefit_lines == ("dental",)
    assert by_name["United Concordia Dental"].source_tier == "mrf_importable"
    assert (
        by_name["United Concordia Dental"].index_url
        == "https://bcbsla.sapphiremrfhub.com/"
    )
    assert by_name["United Concordia Dental"].hosting_platform == "sapphire"
    assert "United Concordia" in by_name["United Concordia Dental"].aliases
    assert "United Concorida" in by_name["United Concordia Dental"].aliases
    assert "United Concordia Companies" in by_name["United Concordia Dental"].aliases
    assert by_name["Unum Dental / Starmount Life"].entity_type == "dental"
    assert by_name["Unum Dental / Starmount Life"].benefit_lines == ("dental",)
    for review_only_name in (
        "HRI / Paramount Dental",
    ):
        assert by_name[review_only_name].entity_type == "dental"
        assert by_name[review_only_name].benefit_lines == ("dental",)
        assert by_name[review_only_name].status == "needs_review"
        assert by_name[review_only_name].index_url is None
    assert by_name["Unum Dental / Starmount Life"].status == "active"
    assert by_name["Unum Dental / Starmount Life"].source_tier == "coverage_evidence"
    assert by_name["Unum Dental / Starmount Life"].index_url
    assert "Unum" in by_name["Unum Dental / Starmount Life"].aliases
    assert "Starmount Life Insurance Company" in (
        by_name["Unum Dental / Starmount Life"].aliases
    )
    assert by_name["Transwestern Dental"].entity_type == "dental"
    assert by_name["Transwestern Dental"].benefit_lines == ("dental",)
    assert by_name["Transwestern Dental"].status == "active"
    assert by_name["Transwestern Dental"].source_tier == "coverage_evidence"
    assert by_name["Transwestern Dental"].index_url
    assert "TRANSWESTERN DENTAL" in by_name["Transwestern Dental"].aliases
    assert (
        "Transwestern Insurance Administrators Dental"
        in by_name["Transwestern Dental"].aliases
    )
    assert by_name["GEHA"].hosting_platform == "html_delegated_mrf_links"
    assert by_name["GEHA"].benefit_lines == ("dental", "medical")
    assert "Connection Dental Federal" in by_name["GEHA"].aliases
    assert by_name["The Health Plan"].hosting_platform == "healthplan_html_mrf_links"
    assert "The Health Plan of West Virginia" in by_name["The Health Plan"].aliases
    assert (
        "The Health Plan of the Upper Ohio Valley" in by_name["The Health Plan"].aliases
    )
    assert "THP" in by_name["The Health Plan"].aliases
    assert by_name["Johns Hopkins HealthCare"].hosting_platform == "html_mrf_links"
    assert by_name["Johns Hopkins HealthCare"].index_url == "https://ehptransparency.org/"
    assert by_name["Medical Mutual of Ohio"].hosting_platform == "healthsparq"
    assert by_name["Varipro"].hosting_platform == "mymedicalshopper_talon"
    assert "Varipro TPA" in by_name["Varipro"].aliases
    assert "Valipro" in by_name["Varipro"].aliases
    assert "Valipro TPA" in by_name["Varipro"].aliases
    assert "Professional Benefits Services" in by_name["Varipro"].aliases
    assert "PBS" in by_name["Varipro"].aliases
    assert by_name["SIHO"].hosting_platform == "mymedicalshopper_talon"
    assert "SIHO Insurance Services" in by_name["SIHO"].aliases
    assert by_name["Blackhawk Claims Service"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["Dunn & Associates"].hosting_platform == "mymedicalshopper_talon"
    assert "Dunn and Associates" in by_name["Dunn & Associates"].aliases
    assert by_name["Patient Advocates"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["Fox/Everett"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["Concierge Administrative Services"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["ATA America"].hosting_platform == "healthcarebluebook_mrf"
    assert "American Trust Administrators" in by_name["ATA America"].aliases
    assert by_name["Trustmark Small Business Benefits"].hosting_platform == "healthcarebluebook_mrf"
    assert "Trustmark SB" in by_name["Trustmark Small Business Benefits"].aliases
    assert by_name["Planned Administrators Inc"].hosting_platform == "html_mrf_links"
    assert "PAI" in by_name["Planned Administrators Inc"].aliases
    assert by_name["ReDirect Health"].hosting_platform == "html_mrf_links"
    assert by_name["Employers Health Network"].hosting_platform == "payercompass_mrf"
    assert by_name["Center Care"].hosting_platform == "payercompass_mrf"
    assert by_name["Employee Benefit Logistics"].hosting_platform == "payercompass_mrf"
    assert by_name["HealthChoice - HPI"].hosting_platform == "payercompass_mrf"
    assert by_name["Insurance Systems"].hosting_platform == "payercompass_mrf"
    assert by_name["Stanislaus County Health Plan"].hosting_platform == "payercompass_mrf"
    assert by_name["CSC Zelis Repository"].hosting_platform == "payercompass_mrf"
    assert by_name["Med-Pay"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["Municipal Benefit Health Program"].hosting_platform == "payercompass_mrf"
    assert by_name["The Care Network"].hosting_platform == "direct_mrf_body"
    assert by_name["Kapnick Insurance Group"].hosting_platform == "sapphire"
    assert by_name["PTI Engineered Plastics"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["RCI Group II, LLC"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["The Ohio State University"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["U.S. Renal Care"].hosting_platform == "healthcarebluebook_mrf"
    assert by_name["Washington Community Schools"].hosting_platform == "mymedicalshopper_talon"
    assert by_name["Reliance Matrix"].hosting_platform == "html_delegated_mrf_links"
    assert "Reliance Standard" in by_name["Reliance Matrix"].aliases
    assert (
        "Reliance Standard Life Insurance Company"
        in by_name["Reliance Matrix"].aliases
    )
    assert "Reliance Standard Life Ins Co" in by_name["Reliance Matrix"].aliases
    assert "reliancematrix" in by_name["Reliance Matrix"].aliases
    assert by_name["Reliance Matrix"].benefit_lines == ("medical", "vision")
    assert "Reliance Standard Vision" in by_name["Reliance Matrix"].aliases
    assert "Davis Vision" in by_name["Reliance Matrix"].aliases
    assert "Davis Vision Network" in by_name["Reliance Matrix"].aliases
    assert "Davis Vision by MetLife" in by_name["Reliance Matrix"].aliases
    assert "Guardian - Davis Vision" in by_name["Reliance Matrix"].aliases
    assert "Guardian Vision with Davis Vision" in by_name["Reliance Matrix"].aliases
    assert "Highmark Davis Vision" in by_name["Reliance Matrix"].aliases
    assert "QCC Insurance Company" in by_name["Reliance Matrix"].aliases
    assert by_name["BCBS North Carolina"].hosting_platform == "direct_toc"
    assert "Blue Cross Blue Shield of NC" in aliases_by_name["BCBS North Carolina"]
    assert "BlueCross BlueShield of NC" in aliases_by_name["BCBS North Carolina"]
    assert by_name["BCBS Louisiana"].benefit_lines == (
        "medical",
        "dental",
        "vision",
    )
    assert "Blue Cross Blue Shield of Louisiana" in by_name["BCBS Louisiana"].aliases
    assert by_name["Triple-S Salud"].hosting_platform == "triples_mtt_api"
    assert "Care Plus ELA" in by_name["Triple-S Salud"].aliases
    assert (
        by_name["Contra Costa Health Plan"].hosting_platform
        == "hostedjson_azure_mrf_listing"
    )
    assert "CarePlus" in by_name["Contra Costa Health Plan"].aliases
    assert "Blue Cross Blue Shield of SC" in aliases_by_name["BCBS South Carolina"]
    assert "BlueCross BlueShield of South Carolina" in aliases_by_name["BCBS South Carolina"]
    assert by_name["BCBS Alabama"].hosting_platform == "direct_toc"
    assert "BlueCross BlueShield of Alabama" in by_name["BCBS Alabama"].aliases
    assert by_name["BCBS North Dakota"].benefit_lines == ("medical", "dental")
    assert "BCBSND Dental" in by_name["BCBS North Dakota"].aliases
    assert by_name["BCBS Wyoming"].hosting_platform == "bcbswy_hmhs_monthly_toc"
    assert by_name["BCBS Wyoming"].benefit_lines == (
        "medical",
        "dental",
        "vision",
    )
    assert "Blue Cross and Blue Shield of Wyoming" in by_name["BCBS Wyoming"].aliases
    assert "BCBSWY" in by_name["BCBS Wyoming"].aliases
    assert "BCBSWY Dental" in by_name["BCBS Wyoming"].aliases
    assert "BCBSWY Vision" in by_name["BCBS Wyoming"].aliases
    assert "Blue Cross Blue Shield/Blue Water" in by_name["BCBS Michigan"].aliases
    assert "Blue Cross & Blue Shield of Mississippi" in by_name["BCBS Mississippi"].aliases
    assert "BlueCross BlueShield of Mississippi" in by_name["BCBS Mississippi"].aliases
    assert (
        "Blue Benefit Administrators of Massachusetts"
        in by_name["BCBS Massachusetts"].aliases
    )
    assert "Blue Benefit Administrators" in by_name["BCBS Massachusetts"].aliases
    assert "BBA" in by_name["BCBS Massachusetts"].aliases
    assert "Cobalt Benefits Group BBA" in by_name["BCBS Massachusetts"].aliases
    assert (
        "Blue Cross and Blue Shield of Massachusetts HMO Blue, Inc."
        in by_name["BCBS Massachusetts"].aliases
    )
    assert by_name["BCBS Massachusetts"].benefit_lines == ("medical", "dental")
    assert "Blue Cross Blue Shield Arkansas" in by_name["Arkansas BCBS"].aliases
    assert "Blue Cross Blue Shield Louisiana" in by_name["BCBS Louisiana"].aliases
    assert "MISSOURI BLUE CROSS OF KANSAS CITY" in by_name["BCBS Kansas City"].aliases
    assert "Blue Cross Blue Shield Hawaii" in by_name["HMSA"].aliases
    assert "BlueCross BlueShield of AZ" in by_name["BCBS Arizona"].aliases
    assert "BlueCross BlueShield of Arizona" in by_name["BCBS Arizona"].aliases
    assert "Blue Cross Blue Shield Arizona" in by_name["BCBS Arizona"].aliases
    assert "Blue Cross Blue Shield of Arizona" in by_name["BCBS Arizona"].aliases
    assert "ARIZONA BLUE CROSS" in by_name["BCBS Arizona"].aliases
    assert (
        "Blue Cross and Blue Shield of Arizona, Inc."
        in by_name["BCBS Arizona"].aliases
    )
    assert "Blue Cross Blue Shield of Arizona, Inc." in by_name["BCBS Arizona"].aliases
    assert "BlueCross BlueShield of IL" in by_name["BCBS Illinois"].aliases
    assert "ILLINOIS BLUE CROSS" in by_name["BCBS Illinois"].aliases
    assert by_name["BCBS Illinois"].benefit_lines == ("medical", "vision")
    assert by_name["BCBS Montana"].benefit_lines == ("medical",)
    assert by_name["BCBS New Mexico"].benefit_lines == ("medical", "vision")
    assert "Blue Cross and Blue Shield of Minnesota" in by_name["BCBS Minnesota"].aliases
    assert "BlueCross BlueShield of Minnesota" in by_name["BCBS Minnesota"].aliases
    assert "BlueCross BlueShield of Oklahoma" in by_name["BCBS Oklahoma"].aliases
    assert by_name["BCBS Oklahoma"].benefit_lines == ("medical", "vision")
    assert "BlueCross BlueShield of Nebraska" in by_name["BCBS Nebraska"].aliases
    assert "Blue Cross Blue Shield of NE" in by_name["BCBS Nebraska"].aliases
    assert "Blue Cross Blue Sheild of TX" in by_name["BCBS Texas"].aliases
    assert by_name["BCBS Texas"].benefit_lines == ("medical", "vision")
    assert by_name["BCBS Tennessee"].benefit_lines == ("medical", "dental")
    assert by_name["BCBS Tennessee ASO"].benefit_lines == ("medical", "dental")
    assert "Blue Cross Blue Shield Tennessee" in by_name["BCBS Tennessee"].aliases
    assert "Blue Cross Blue Shield TN" in by_name["BCBS Tennessee"].aliases
    assert "BCBS Tennessee" in by_name["BCBS Tennessee ASO"].aliases
    assert "Blue Cross Blue Shield Tennessee" in by_name["BCBS Tennessee ASO"].aliases
    assert "Blue Cross Blue Shield Rhode Island" in by_name["BCBS Rhode Island"].aliases
    assert "Blue Cross Blue Shield South Carolina" in aliases_by_name["BCBS South Carolina"]
    assert "Blue Cross Blue Shield Vermont" in by_name["BCBS Vermont"].aliases
    assert "Premera Blue Cross WA AK" in by_name["Premera Blue Cross"].aliases
    assert (
        "BLUE CROSS WA/AK PREMERA BLUE CROSS"
        in by_name["Premera Blue Cross"].aliases
    )
    assert "Pacific Source" in by_name["PacificSource"].aliases
    assert "PacificSource Administrators" in by_name["PacificSource"].aliases
    assert "Angle" in by_name["Angle Health"].aliases
    assert "Adrem Administrators" in by_name["Angle Health"].aliases
    assert (
        "ALLEGIANCE BENEFIT PLAN MANAGEMENT INC."
        in by_name["Allegiance Benefit Plan Management"].aliases
    )
    assert "AskAllegiance" in by_name["Allegiance Benefit Plan Management"].aliases
    assert "Allegiance" in by_name["Allegiance Benefit Plan Management"].aliases
    assert "AccessHMA" in by_name["Healthcare Management Administrators"].aliases
    assert "CareFirst BlueChoice, Inc." in by_name["CareFirst"].aliases
    assert "Highmark Blue Shield" in by_name["Highmark"].aliases
    assert "Highmark WV" in by_name["Highmark"].aliases
    assert "Independence Blue Cross (IBX)" in by_name["Independence Blue Cross"].aliases
    assert (
        "Independence Blue Cross (QCC Ins. Co.)"
        in by_name["Independence Blue Cross"].aliases
    )
    assert by_name["Independence Blue Cross"].benefit_lines == ("medical", "vision")
    assert "Medica Insurance Company" in by_name["Medica"].aliases
    assert "UTAH REGENCE BLUE CROSS BLUE SHIELD" in by_name["Regence"].aliases
    assert "First Choice Health Network" in aliases_by_name["First Choice Health"]
    assert "Health Plan of Nevada, Inc." in by_name["Health Plan of Nevada"].aliases
    assert by_name["Health Plan of Nevada"].status == "stale"
    assert "Health Plan of Nevada" in by_name["United Healthcare"].aliases
    assert "HPN" in by_name["United Healthcare"].aliases
    assert by_name["CalOptima"].status == "stale"
    assert "CalOptima" in by_name["Blue Shield of California"].aliases
    assert "CalOptima" in by_name["Kaiser Permanente"].aliases
    assert by_name["CalViva Health"].status == "stale"
    assert "CalViva Health" in by_name["Centene"].aliases
    assert "PCMI" in by_name["Pinnacle Claims Management"].aliases
    assert (
        "Robbins Regency Employee Benefits"
        in by_name["Regency Employee Benefits"].aliases
    )
    assert "Maine Community Health Options" in by_name["Community Health Options"].aliases
    assert "Point-C" in by_name["Point C"].aliases
    assert "Cypress Benefit Administrators" in by_name["Lucent Health"].aliases
    assert "Sierra Health and Life" in by_name["United Healthcare"].aliases
    assert "UnitedHealthcare of Arizona, Inc." in by_name["United Healthcare"].aliases
    assert (
        "Sierra Health and Life Insurance Company, Inc."
        in by_name["United Healthcare"].aliases
    )
    assert "Ameritas Holding Company Health Plan" in by_name["United Healthcare"].aliases
    assert "KCL Group Benefits" in by_name["United Healthcare"].aliases
    assert "Anthem CA" in by_name["Anthem"].aliases
    assert by_name["Anthem"].benefit_lines == ("medical", "dental", "vision")
    assert "Anthem BlueCross & BlueShield Plan" in by_name["Anthem"].aliases
    assert "Anthem Vision Plan" in by_name["Anthem"].aliases
    assert "Anthem Blue View Vision Plan" in by_name["Anthem"].aliases
    assert "Blue Cross of California" in by_name["Anthem"].aliases
    assert "Blue Cross and Blue Shield of Georgia (Anthem)" in by_name["Anthem"].aliases
    assert "Blue Cross and Blue Shield of GA (Anthem)" in by_name["Anthem"].aliases
    assert "Blue Cross and Blue Shield of Georgia, Inc." in by_name["Anthem"].aliases
    assert "Indiana Blue Cross" in by_name["Anthem"].aliases
    assert "HealthKeepers, Inc." in by_name["Anthem"].aliases
    assert "Anthem Health Plans of Kentucky, Inc." in by_name["Anthem"].aliases
    assert "Anthem Health Plans of Virginia, Inc." in by_name["Anthem"].aliases
    assert "Anthem Insurance Companies, Inc." in by_name["Anthem"].aliases
    assert "Kaiser Foundation Health Plan, Inc." in by_name["Kaiser Permanente"].aliases
    assert "Kasier Permanente" in by_name["Kaiser Permanente"].aliases
    assert "Kaiser Hawaii" in by_name["Kaiser Permanente"].aliases
    assert (
        "Kaiser Foundation Health Plan of the Mid-Atlantic States, Inc."
        in by_name["Kaiser Permanente"].aliases
    )
    assert "Sentara Health Plans" in by_name["Sentara"].aliases
    assert "Priority Health of Michigan" in by_name["Priority Health"].aliases
    assert "Sutter Health Plus" in by_name["Sutter Health Plan"].aliases
    assert "UHA" in by_name["UHA Health Insurance"].aliases
    assert by_name["EMI Health"].benefit_lines == ("dental", "medical")
    assert "vision" not in by_name["EMI Health"].benefit_lines
    assert by_name["EMI Health"].hosting_platform == "html_mrf_links"
    assert by_name["EMI Health"].status == "active"
    assert by_name["EMI Health"].index_url == "https://emihealth.com/machinereadables"
    assert "Companion Life dental" in by_name["EMI Health"].aliases
    assert "Companion Life EMI Dental Plans" in by_name["EMI Health"].aliases
    emi_sources = [candidate for candidate in candidates if candidate.payer_name == "EMI Health"]
    assert any(
        candidate.status == "stale"
        and candidate.index_url == "https://emihealth.com/MachineReadables"
        for candidate in emi_sources
    )
    assert by_name["WPS Health"].hosting_platform == "html_mrf_links"
    assert "WPS Health Insurance" in by_name["WPS Health"].aliases
    assert "Wisconsin Physicians Service Insurance Corporation" in (
        by_name["WPS Health"].aliases
    )
    assert by_name["SummaCare"].hosting_platform == "html_mrf_links"
    assert "SummaCare MEWA" in by_name["SummaCare"].aliases
    assert "Summa Health System" in by_name["SummaCare"].aliases
    assert "Health Plans, Inc" in by_name["Health Plans Inc"].aliases
    assert "Health Plans, Inc." in by_name["Health Plans Inc"].aliases
    assert "HealthPlans Inc." in by_name["Health Plans Inc"].aliases
    assert "Auxient TPA" in by_name["Auxiant"].aliases
    assert "90 Degree Benefits, Inc." in by_name["90 Degree Benefits"].aliases
    assert (
        "American Plan Administartors"
        in by_name["American Plan Administrators"].aliases
    )
    assert "Allied" in aliases_by_name["Allied Benefit Systems"]
    assert "Allied Benefits" in aliases_by_name["Allied Benefit Systems"]
    assert (
        "Professional Benefit Administrators (Oak Brook, IL)"
        in aliases_by_name["Professional Benefit Administrators"]
    )
    assert (
        "PROFESSIONAL BENEFIT ADMINISTRATORS (OAK BROOK,IL)"
        in aliases_by_name["Professional Benefit Administrators"]
    )
    assert "Comprehensive Benefits Administrators" in by_name["CBA Blue"].aliases
    assert "Cobalt Benefits Group CBA Blue" in by_name["CBA Blue"].aliases
    assert discovery._is_candidate_text_filter_match(
        by_name["CBA Blue"], entity_types=(), payer_query="Cobalt Benefits Group"
    )
    assert "EBPA Employee Benefits" in by_name["EBPA"].aliases
    assert "Employee Benefit Plan Administrators" in by_name["EBPA"].aliases
    assert "Cobalt Benefits Group EBPA" in by_name["EBPA"].aliases
    assert "AmeriBen: Anthem" in by_name["AmeriBen"].aliases
    assert "AmeriBen Anthem Blue Cross" in by_name["AmeriBen"].aliases
    assert "AmeriBen: Anthem Blue Cross" in by_name["AmeriBen"].aliases
    assert (
        "Employee Benefit Management Services (EBMS)"
        in by_name["EBMS"].aliases
    )
    assert "Anthem/Luminare Health" in by_name["Luminare Health Benefits"].aliases
    assert "Anthem (Administered by EVHC)" in by_name["Luminare Health Benefits"].aliases
    assert (
        "LUMINARE HEALTH AZ IL IN MD MN NC PA"
        in by_name["Luminare Health Benefits"].aliases
    )
    assert "EVHC" in by_name["Luminare Health Benefits"].aliases
    assert "Evolution Healthcare" in by_name["Luminare Health Benefits"].aliases
    assert "Evolution Healthcare EVHC" in by_name["Luminare Health Benefits"].aliases
    assert "Cofinity" in by_name["Luminare Health Benefits"].aliases
    assert "First Health Cofinity" in by_name["Luminare Health Benefits"].aliases
    assert "Medical Benefits Administrators, Inc." in by_name["MedBen"].aliases
    assert "Unified Group" in by_name["Unified Group Services"].aliases
    assert by_name["EMI Health"].benefit_lines == ("dental", "medical")
    assert "vision" not in by_name["EMI Health"].benefit_lines
    assert "TDA Dental" in by_name["EMI Health"].aliases
    assert "Total Dental Administrators" in by_name["EMI Health"].aliases
    assert by_name["HAP"].hosting_platform == "healthsparq"
    assert "Alliance Health and Life Insurance Company" in by_name["HAP"].aliases
    assert by_name["Peak Health"].hosting_platform == "html_mrf_links"
    assert by_name["Centivo - Rockwell Automation"].hosting_platform == "html_mrf_links"
    assert "Centivo" in by_name["Centivo - Rockwell Automation"].aliases


@pytest.mark.asyncio
async def test_master_list_public_alias_queries_match_expected_candidates():
    """Verify this source-discovery regression contract."""
    candidates = await discovery._load_candidates(
        "master-list", test_mode=True, limit=2000
    )

    def matching_names(query: str) -> set[str]:
        return {
            candidate.payer_name
            for candidate in candidates
            if discovery._is_candidate_text_filter_match(
                candidate,
                entity_types=(),
                payer_query=query,
            )
        }

    def matching_importable_names(query: str) -> set[str]:
        return {
            candidate.payer_name
            for candidate in candidates
            if discovery._is_candidate_importable_source(candidate)
            and discovery._is_candidate_text_filter_match(
                candidate,
                entity_types=(),
                payer_query=query,
            )
        }

    assert "United Healthcare" in matching_names("UMR (Using Spectera Network)")
    assert "United Healthcare" in matching_names("UHC Vision (Using Spectera Network)")
    assert "United Healthcare" in matching_names("DBP")
    assert "United Healthcare" in matching_names("Dental Benefit Providers DBP")
    assert "United Healthcare" in matching_names(
        "Sierra Health and Life Insurance Company, Inc."
    )
    assert "Cigna" in matching_importable_names(
        "Allegiance Life & Health Insurance Company, Inc."
    )
    assert "United Healthcare" in matching_importable_names("Kansas City Life")
    assert "United Healthcare" in matching_importable_names(
        "Kansas City Life Insurance Company"
    )
    assert "Aetna" in matching_names("Aetna Dental")
    assert "Aetna" in matching_importable_names("Aetna Dental")
    assert "United Healthcare" in matching_names("UnitedHealthcare of Arizona, Inc.")
    assert "Anthem" in matching_names("Anthem Health Plans of Kentucky, Inc.")
    assert "Anthem" in matching_names("Anthem CA")
    assert "Anthem" in matching_names("Anthem BlueCross & BlueShield Plan")
    assert "Anthem" in matching_importable_names("Anthem Vision Plan")
    assert "Anthem" in matching_names("Blue Cross of California")
    assert "Anthem" in matching_names("Blue Cross and Blue Shield of Georgia, Inc.")
    assert "Anthem" in matching_names("Blue Cross and Blue Shield of GA (Anthem)")
    assert "Anthem" in matching_names("HealthKeepers, Inc.")
    assert "Anthem" in matching_names("INDIANA BLUE CROSS")
    assert "Kaiser Permanente" in matching_names("Kaiser Foundation Health Plan, Inc.")
    assert "Kaiser Permanente" in matching_names("Kaiser Hawaii")
    assert "BCBS Arizona" in matching_names(
        "Blue Cross and Blue Shield of Arizona, Inc."
    )
    assert "BCBS Arizona" in matching_names("Blue Cross Blue Shield  of Arizona")
    assert "BCBS Arizona" in matching_importable_names("Blue Cross Blue Shield Arizona")
    assert "BCBS Arizona" in matching_names("ARIZONA BLUE CROSS")
    assert "BCBS Tennessee" in matching_importable_names("Blue Cross Blue Shield TN")
    assert "BCBS Alabama" in matching_names("BlueCross BlueShield of Alabama")
    assert "BCBS Massachusetts" in matching_names(
        "Blue Cross and Blue Shield of Massachusetts HMO Blue, Inc."
    )
    assert "BCBS Mississippi" in matching_names("Blue Cross & Blue Shield of Mississippi")
    assert "BCBS South Carolina" in matching_names("BlueCross BlueShield of South Carolina")
    assert "BCBS Illinois" in matching_names("BlueCross BlueShield of IL")
    assert "BCBS Minnesota" in matching_names("Blue Cross and Blue Shield of Minnesota")
    assert "BCBS Oklahoma" in matching_names("BlueCross BlueShield of Oklahoma")
    assert "BCBS Nebraska" in matching_names("Blue Cross Blue Shield of NE")
    assert "BCBS Texas" in matching_names("Blue Cross Blue Sheild of TX")
    assert "Premera Blue Cross" in matching_names("BLUE CROSS WA/AK PREMERA BLUE CROSS")
    assert "Wellmark" in matching_names("Wellmark Health Plan of Iowa, Inc.")
    assert "Delta Dental Plan of Michigan" in matching_names(
        "Delta Dental Plan of Indiana, Inc."
    )
    assert "Delta Dental Plan of Michigan" in matching_importable_names(
        "Delta Dental Plan of Ohio, Inc."
    )
    assert "Delta Dental" in matching_names("DeltaDental")
    assert "Delta Dental" not in matching_importable_names("DeltaDental")
    assert "Delta Dental" in matching_names("Delta Dental of Missouri")
    assert "Delta Dental" not in matching_importable_names("Delta Dental of Missouri")
    assert "Guardian Dental" in matching_names("Guardian")
    assert "Guardian Dental" not in matching_importable_names("Guardian")
    assert "Guardian Dental" in matching_names("Guardian Life Insurance Company of America")
    assert "Guardian Dental" not in matching_importable_names(
        "Guardian Life Insurance Company of America"
    )
    assert "CareFirst" in matching_names("CareFirst BlueChoice, Inc.")
    assert "Highmark" in matching_names("Highmark Blue Shield")
    assert "Highmark" in matching_names("Highmark WV")
    assert "Independence Blue Cross" in matching_names("Independence Blue Cross (IBX)")
    assert "Medica" in matching_names("Medica Insurance Company")
    assert "Regence" in matching_names("UTAH REGENCE BLUE CROSS BLUE SHIELD")
    assert "Community Health Options" in matching_names("Maine Community Health Options")
    assert "Point C" in matching_names("Point-C")
    assert "Firefly Health" in matching_names("Firefly Health")
    assert "Firefly Health" not in matching_importable_names("Firefly Health")
    assert "First Choice Health" in matching_names("First Choice Health Network")
    assert "Health Plan of Nevada" in matching_names("Health Plan of Nevada, Inc.")
    assert "Health Plan of Nevada" not in matching_importable_names(
        "Health Plan of Nevada, Inc."
    )
    assert "United Healthcare" in matching_importable_names("Health Plan of Nevada")
    assert "Blue Shield of California" in matching_importable_names("CalOptima")
    assert "Kaiser Permanente" in matching_importable_names("CalOptima")
    assert "CalOptima" not in matching_importable_names("CalOptima")
    assert "Centene" in matching_importable_names("CalViva Health")
    assert "CalViva Health" not in matching_importable_names("CalViva Health")
    assert "Allegiance Benefit Plan Management" in matching_names(
        "ALLEGIANCE BENEFIT PLAN MANAGEMENT INC."
    )
    assert "90 Degree Benefits" in matching_names("90 Degree Benefits, Inc.")
    assert "American Plan Administrators" in matching_names(
        "AMERICAN PLAN ADMINISTARTORS"
    )
    assert "AmeriBen" in matching_names("AmeriBen: Anthem Blue Cross")
    assert "EBMS" in matching_names("Employee Benefit Management Services (EBMS)")
    assert "Kaiser Permanente" in matching_names("Kasier Permanente")
    assert "Sentara" in matching_names("Sentara Health Plans")
    assert "Luminare Health Benefits" in matching_names("Anthem (Administered by EVHC)")
    assert "Luminare Health Benefits" in matching_names("Anthem/Luminare Health")
    assert "MedBen" in matching_names("Medical Benefits Administrators, Inc.")
    assert "Unified Group Services" in matching_names("Unified Group")
    assert "Luminare Health Benefits" in matching_importable_names("Cofinity")
    assert "Luminare Health Benefits" in matching_importable_names(
        "First Health Cofinity"
    )
    assert "Empire BlueCross BlueShield" in matching_names(
        "Empire Blue Cross Blue Sheild"
    )
    assert "Guardian Dental" in matching_names("Guaridan")
    assert "Guardian Vision" in matching_names("Guaridan")
    assert "MetLife Dental" in matching_names("Metlife DPPO")
    assert "MetLife Vision" in matching_names("Metropolitan Life Insurance Company")
    assert "Principal Dental" in matching_names("Principle")
    assert "Principal Vision" in matching_names("Principle")
    assert "Mutual of Omaha Dental" in matching_names(
        "United of Omaha Life Insurance Company"
    )
    assert "Ameritas Vision" in matching_names("Ameritas Life Ins. Corp.")
    assert "Ameritas Dental" in matching_names("The Business Council")
    assert "Ameritas Vision" in matching_names("The Business Council")
    assert "Dominion National Dental" in matching_names("Dominion National Dental")
    assert "Dominion National Vision" in matching_names("Dominion National")
    assert "Humana Dental" in matching_names("Humana Insurance Company")
    assert "Humana Vision" in matching_names("Humana Insurance Company")
    assert "Humana Dental" not in matching_importable_names("Humana Insurance Company")
    assert "Humana Vision" not in matching_importable_names("Humana Insurance Company")
    assert "EyeMed" in matching_importable_names("Mututal of Omaha")
    assert "United Healthcare" in matching_importable_names(
        "The Lincoln National Life Insurance Company"
    )
    assert "Prodegi" in matching_importable_names("Prodegi")
    assert "Wellmark" in matching_importable_names("Wellmark Blue Dental")
    assert "Solstice Dental" in matching_names("Solstice Benefits, Inc.")
    assert "Solstice Dental" not in matching_importable_names("Solstice Benefits, Inc.")
    assert "Sun Life Dental" in matching_names("SunLife Financail")
    assert "Transwestern Insurance Administrators" in matching_importable_names(
        "TRANSWESTERN INSURANCE ADMINISTRATORS, INC"
    )
    assert "Transwestern Dental" in matching_names("TRANSWESTERN DENTAL")
    assert "Transwestern Dental" not in matching_importable_names("TRANSWESTERN DENTAL")
    assert "Smile Brands Inc" in matching_importable_names("Smile Brands")
    assert "United Concordia Dental" in matching_names("United Concorida")
    assert "EMI Health" in matching_importable_names("TDA Dental")
    assert "EMI Health" in matching_importable_names("Total Dental Administrators")
    assert "BRMS" in matching_importable_names("Benefits and Risk Management Services")
    assert "BRMS" in matching_importable_names("Benefit & Risk Management Services")
    assert "HAP" in matching_importable_names(
        "Alliance Health and Life Insurance Company"
    )
    assert "Auxiant" in matching_importable_names(
        "Example Group - Auxient TPA HealthLink Network"
    )
    assert "Peak Health" in matching_importable_names("Peak Health Plan")
    assert "Centivo - Rockwell Automation" in matching_importable_names("Centivo")
    assert "Reliance Matrix" in matching_importable_names("reliancematrix")
    assert "Reliance Matrix" in matching_importable_names("Davis Vision")
    assert "Reliance Matrix" in matching_importable_names("Davis Vision Network")
    assert "Reliance Matrix" in matching_importable_names("Guardian - Davis Vision")
    assert "Reliance Matrix" in matching_importable_names("MetLife Davis Vision")
    assert "Meritain Health" in matching_names("Meritain Health, An Aetna Company")
    assert "Meritain Health" in matching_names(
        "MERITAIN HEALTH (NORTH AMERICAN HEALTH PLAN)"
    )
    assert "Health Plans Inc" in matching_names("Health Plans, Inc")
    assert "Health Plans Inc" in matching_names("Health  Plans Inc")
    assert "Health Plans Inc" in matching_names("HealthPlans Inc.")
    assert "Lincoln Financial DentalConnect" in matching_names("Lincoln Financial Group")
    assert "Lincoln Financial DentalConnect" not in matching_importable_names(
        "Lincoln Financial Group"
    )
    assert "MetLife Dental" in matching_names("MetLife")
    assert "MetLife Dental" not in matching_importable_names("MetLife")
    assert "Principal Dental" in matching_names("Principal Financial Group")
    assert "Principal Dental" not in matching_importable_names("Principal Financial Group")
    assert "Ameritas Dental" in matching_names("Ameritas")
    assert "Ameritas Dental" not in matching_importable_names("Ameritas")
    assert "Sun Life Dental" in matching_names("SunLife")
    assert "Sun Life Dental" not in matching_importable_names("SunLife")
    assert "The Standard Dental" in matching_names("The Standard")
    assert "The Standard Dental" not in matching_importable_names("The Standard")
    assert "Allied Benefit Systems" in matching_names("Allied Benefits")
    assert "Professional Benefit Administrators" in matching_names(
        "Professional Benefit Administrators (Oak Brook, IL)"
    )
    assert "Professional Benefit Administrators" in matching_names(
        "PROFESSIONAL BENEFIT ADMINISTRATORS (OAK BROOK,IL)"
    )
    assert "EyeMed" in matching_names("Aetna Vision")
    assert "EyeMed" in matching_names("Aetna Vision Preferred")
    assert "EyeMed" in matching_importable_names("Aetna Vision with EyeMed")
    assert "EyeMed" in matching_importable_names("Cigna Vision EyeMed")
    assert "EyeMed" in matching_importable_names("Ameritas Vision with EyeMed")
    assert "EyeMed" in matching_importable_names("Mutual of Omaha Vision with EyeMed")
    assert "EyeMed" in matching_importable_names("Unum Vision EyeMed")
    assert "EyeMed" in matching_importable_names("Companion Life Vision EyeMed")
    assert "EyeMed" in matching_importable_names("Delta Vision with EyeMed")
    assert "EyeMed" in matching_names("Ameritas Vision EyeMed")
    assert "EyeMed" in matching_names("Surency")
    assert "EyeMed" in matching_names("Cigna Vision serviced by EyeMed")
    assert "EyeMed" in matching_names("BlueCare Vision of Texas (powered by EyeMed)")
    assert "EyeMed" in matching_names(
        "Blue 20/20 of Massachusetts (powered by EyeMed)"
    )
    assert "EyeMed" in matching_names("BlueCare Vision of Illinois (powered by EyeMed)")
    assert "EyeMed" in matching_names("Dearborn National with EyeMed")
    assert "EyeMed" in matching_names("DeltaVision administered by EyeMed")
    assert "EyeMed" in matching_names("Surency Vision")
    assert "United Healthcare" in matching_names("Lincoln Financial Group - Spectera")
    assert "United Healthcare" in matching_importable_names(
        "UnitedHealthcare Vision Using Spectera Network"
    )
    assert "United Healthcare" in matching_importable_names(
        "United Healthcare Vision (Using Spectera Network)"
    )
    assert "United Healthcare" in matching_importable_names(
        "UMR Vision Using Spectera Network"
    )
    assert "United Healthcare" in matching_importable_names(
        "Lincoln Financial Group Spectera"
    )
    assert "VSP Vision" in matching_names("Vision Serivce Plan")
    assert "VSP Vision" in matching_importable_names("Vision Service Plan VSP")
    assert "VSP Vision" in matching_importable_names("Vision Plan Service VSP")
    assert "VSP Vision" in matching_names("Principal Financial Group - VSP")
    assert "VSP Vision" in matching_importable_names("Principal Vision with VSP")
    assert "VSP Vision" in matching_names("SunLife/VSP")
    assert "VSP Vision" in matching_importable_names("Sun Life Vision with VSP")
    assert "VSP Vision" in matching_importable_names("SunLife Vision with VSP")
    assert "VSP Vision" in matching_names("Guardian/VSP")
    assert "VSP Vision" in matching_importable_names("Guardian Vision with VSP")
    assert "VSP Vision" in matching_names("Guardian/VSP Vision")
    assert "VSP Vision" in matching_names("VSP Service Plan")
    assert "VSP Vision" in matching_names("United Heritage (Using VSP Network)")
    assert "VSP Vision" in matching_names("MetLife using VSP Choice Network")
    assert "VSP Vision" in matching_importable_names("MetLife Vision VSP")
    assert "VSP Vision" in matching_names("Metlife / VSP")
    assert "VSP Vision" in matching_names("Renaissance Vision VSP")
    assert "VSP Vision" in matching_names("Ameritas Vision VSP")
    assert "VSP Vision" in matching_importable_names("Ameritas Vision with VSP")
    assert "VSP Vision" in matching_importable_names("Delta Vision with VSP")
    assert "Davis Vision" in matching_names("Highmark Davis Vision")
    assert "Davis Vision" in matching_importable_names("Highmark Davis Vision")
    assert "Davis Vision" in matching_names("MetLife Vision with Davis Vision")
    assert "Davis Vision" in matching_importable_names(
        "MetLife Vision with Davis Vision"
    )
    assert "Superior Vision" in matching_names("MetLife Superior Vision")
    assert "Superior Vision" not in matching_importable_names("MetLife Superior Vision")
    assert "Superior Vision" in matching_names("Superior Vision Network")
    assert "Superior Vision" not in matching_importable_names("Superior Vision Network")
    assert "Vision Benefits of America" in matching_names("Vision Benefits of Ameriva")
    assert "Vision Benefits of America" not in matching_importable_names(
        "Vision Benefits of Ameriva"
    )
    assert "Vision Benefits of America" in matching_names("VBA Vision Network")
    assert "Vision Benefits of America" not in matching_importable_names(
        "VBA Vision Network"
    )
    assert "Avesis" in matching_names("Guardian Vision with Avesis")
    assert "Avesis" not in matching_importable_names("Guardian Vision with Avesis")
    assert "Capital Blue Cross" in matching_names("Capital Blue Cross Vision NVA")
    assert "Capital Blue Cross" in matching_importable_names("Capital Blue Cross NVA")
    assert "Capital Blue Cross" in matching_importable_names(
        "National Vision Administrators (NVA)"
    )
    assert "BCBS North Carolina" not in matching_importable_names(
        "Community Eye Care (CEC)"
    )
    assert "BCBS North Carolina" not in matching_names("Community Eye Care")
    assert "Superior Vision" in matching_names("Versant Health Superior Vision")
    assert "Guardian Vision" in matching_names("Guardian")
    assert "Guardian Vision" not in matching_importable_names("Guardian")
    assert "MetLife Vision" in matching_names("MetLife")
    assert "MetLife Vision" not in matching_importable_names("MetLife")
    assert "Principal Vision" in matching_names("Principal Financial Group")
    assert "Principal Vision" not in matching_importable_names(
        "Principal Financial Group"
    )
    assert "Ameritas Vision" in matching_names("Ameritas")
    assert "Ameritas Vision" not in matching_importable_names("Ameritas")
    assert "Sun Life Vision" in matching_names("SunLife")
    assert "Sun Life Vision" not in matching_importable_names("SunLife")
    assert "The Standard Vision" in matching_names("The Standard")
    assert "The Standard Vision" not in matching_importable_names("The Standard")
    assert "Valley Health Plan" in matching_importable_names("VHP VSP Vision")
    assert "Lincoln Financial DentalConnect" in matching_names("Lincoln DentalConnect")
    assert "LIBERTY Dental Plan" in matching_names("Liberty Dental")
    assert "United Concordia Dental" in matching_names("United Concordia")
    assert "Unum Dental / Starmount Life" in matching_names("Starmount Life")
    assert "EMI Health" in matching_names("Companion Life dental")


@pytest.mark.asyncio
async def test_sapphire_resolver_keeps_direct_toc_urls_without_fetching(monkeypatch):
    async def fail_fetch(*_args, **_kwargs):
        raise AssertionError("direct Sapphire TOCs should not be fetched as hub pages")

    monkeypatch.setattr(discovery, "_fetch_text", fail_fetch)
    source_dict = {
        "source_id": "source_vsp",
        "payer_id": "payer_vsp",
        "display_name": "VSP Vision",
    }

    targets = await discovery._crawl_targets_for_source(
        source_dict,
        "https://example.sapphiremrfhub.com/tocs/current/example_vision",
        None,
    )

    assert len(targets) in {1}
    assert (
        targets[0].url
        == "https://example.sapphiremrfhub.com/tocs/current/example_vision"
    )
    assert targets[0].metadata["resolver"] == "sapphire_html_tocs"
    assert targets[0].metadata["file_name"] == "example_vision"


@pytest.mark.asyncio
async def test_fetch_json_retries_browser_headers_after_406(monkeypatch):
    async def allow_url(_url):
        return None

    class InitialSession:
        def get(self, _url, *, allow_redirects):
            assert allow_redirects is True
            return _FakeFetchResponse(
                status=406,
                body=b"<html>not acceptable</html>",
                content_type="text/html",
            )

    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allow_url)
    _RecordingBrowserFallbackSession.observed_user_agents = []
    _RecordingBrowserFallbackSession.response = _FakeFetchResponse(
        status=200,
        body=b'{"ok": true}',
        content_type="application/json",
    )
    monkeypatch.setattr(
        discovery.aiohttp, "ClientSession", _RecordingBrowserFallbackSession
    )

    response_by_field = await discovery._fetch_json_value(
        "https://example.test/toc.json",
        max_bytes=1024,
        session=InitialSession(),
    )

    assert response_by_field == {"ok": True}
    assert _RecordingBrowserFallbackSession.observed_user_agents == [
        discovery.BROWSER_FALLBACK_USER_AGENT
    ]


@pytest.mark.asyncio
async def test_fetch_text_retries_browser_headers_after_connection_reset(monkeypatch):
    """Connection resets from HTML directory hosts should use browser fallback."""

    async def allow_url(_url):
        return None

    class InitialSession:
        def get(self, _url, *, allow_redirects):
            assert allow_redirects is True
            raise discovery.aiohttp.ClientOSError(54, "connection reset by peer")

    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allow_url)
    _RecordingBrowserFallbackSession.observed_user_agents = []
    _RecordingBrowserFallbackSession.response = _FakeFetchResponse(
        status=200,
        body=b"<html>ok</html>",
        content_type="text/html",
        url="https://transparency.example.test/INN",
    )
    monkeypatch.setattr(
        discovery.aiohttp, "ClientSession", _RecordingBrowserFallbackSession
    )

    text = await discovery._fetch_text(
        "https://transparency.example.test/INN",
        max_bytes=1024,
        session=InitialSession(),
    )

    assert text == "<html>ok</html>"
    assert _RecordingBrowserFallbackSession.observed_user_agents == [
        discovery.BROWSER_FALLBACK_USER_AGENT
    ]


@pytest.mark.asyncio
async def test_direct_mrf_body_source_becomes_file_reference_without_fetching(
    monkeypatch,
):
    async def fail_fetch(*_args, **_kwargs):
        raise AssertionError("direct MRF bodies should be cataloged without fetching")

    monkeypatch.setattr(discovery, "_fetch_text", fail_fetch)
    source_dict = {
        "source_id": "source_eyemed",
        "payer_id": "payer_eyemed",
        "display_name": "EyeMed",
        "hosting_platform": "direct_mrf_body",
    }

    [crawl_target] = await discovery._crawl_targets_for_source(
        source_dict,
        "https://content.eyemedvisioncare.com/EyeMed_HCSC/eyemed_in-network-rates.json",
        None,
    )

    assert crawl_target.label == "Eyemed"
    assert crawl_target.metadata["resolver"] == "direct_mrf_body"
    assert crawl_target.metadata["target_kind"] == "file_reference"
    assert crawl_target.metadata["target_file_type"] == "in-network"
    assert crawl_target.metadata["plan_info"] == [
        {
            "plan_id": None,
            "plan_id_type": None,
            "plan_market_type": "group",
            "plan_name": "Eyemed",
        }
    ]


@pytest.mark.asyncio
async def test_direct_toc_source_becomes_toc_target_without_fetching(monkeypatch):
    async def fail_fetch(*_args, **_kwargs):
        raise AssertionError("direct TOCs should be cataloged without HTML fetching")

    monkeypatch.setattr(discovery, "_fetch_text", fail_fetch)
    direct_toc_source_dict = {
        "source_id": "source_bcbsnc",
        "payer_id": "payer_bcbsnc",
        "display_name": "BCBS North Carolina",
    }
    url = (
        "https://mrfmftprod.bcbsnc.com/prod/etl/outbound/table-of-contents/non-aso/"
        "2026-05-27_blue-cross-and-blue-shield-of-north-carolina_index.json"
    )

    [toc_target] = await discovery._crawl_targets_for_source(direct_toc_source_dict, url, None)

    assert toc_target.url == url
    assert toc_target.label == "BCBS North Carolina"
    assert toc_target.metadata["resolver"] == "direct_toc"
    assert toc_target.metadata["target_kind"] == "toc_json"
    assert toc_target.metadata["target_file_type"] == "table-of-contents"

    hmaa_source_dict = {
        "source_id": "source_hmaa",
        "payer_id": "payer_hmaa",
        "display_name": "HMAA",
    }
    hmaa_url = "https://www.hmaa.com/wp-content/uploads/2022/06/MRF_HMAA.zip"

    [hmaa_target] = await discovery._crawl_targets_for_source(hmaa_source_dict, hmaa_url, None)

    assert hmaa_target.url == hmaa_url
    assert hmaa_target.metadata["resolver"] == "direct_toc"
    assert hmaa_target.metadata["target_kind"] == "toc_json"
    assert hmaa_target.metadata["target_max_bytes"] == 200 * 1024 * 1024


@pytest.mark.asyncio
async def test_resolve_ebms_caa_directory_discovers_client_tocs(monkeypatch):
    pages_dict = {
        "https://caa.ebms.com/": """
            <html><body>
            <a href="Example Public Group/index.html">Example Public Group</a>
            <a href="Example Nested Group/index.html">Example Nested Group</a>
            </body></html>
        """,
        "https://caa.ebms.com/Example Public Group/index.html": """
            <html><body>
            <a href=" 2026-06-01_EBMS_index.json" download>
                2026-06-01_EBMS_index.json
            </a>
            </body></html>
        """,
        "https://caa.ebms.com/Example Nested Group/index.html": """
            <html><body>
            <a href="Plan A/index.html">Plan A</a>
            </body></html>
        """,
        "https://caa.ebms.com/Example Nested Group/Plan A/index.html": """
            <html><body>
            <a href="2026-06-01_EBMS_index.json">2026-06-01_EBMS_index.json</a>
            </body></html>
        """,
    }

    async def fake_fetch_text(url, *, max_bytes, session=None):
        return pages_dict[url]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    crawl_targets = await discovery._resolve_ebms_caa_directory(
        {"source_id": "source_1", "display_name": "EBMS"},
        "https://caa.ebms.com/",
        {
            "type": "ebms_caa_directory",
            "max_clients": 10,
            "max_nested_pages_per_client": 5,
            "max_targets": 10,
        },
        session=None,
    )

    assert [crawl_target.url for crawl_target in crawl_targets] == [
        "https://caa.ebms.com/Example Public Group/2026-06-01_EBMS_index.json",
        "https://caa.ebms.com/Example Nested Group/Plan A/2026-06-01_EBMS_index.json",
    ]
    assert all(
        crawl_target.metadata["resolver"] == "ebms_caa_directory" for crawl_target in crawl_targets
    )
    assert crawl_targets[0].metadata["ebms_client_label"] == "Example Public Group"
    assert (
        crawl_targets[1].metadata["ebms_nested_url"]
        == "https://caa.ebms.com/Example Nested Group/Plan A/index.html"
    )


def test_crawl_target_context_metadata_carries_benefit_lines_from_source():
    target = discovery.CrawlTarget(
        source={
            "source_id": "source_1",
            "metadata_json": {"benefit_lines": ["dental", "vision"]},
        },
        url="https://example.test/mrf/index.json",
        label="Example MRF",
        metadata={},
    )

    context = discovery._crawl_target_context_metadata(target)

    assert context["benefit_lines"] == ["dental", "vision"]
    assert "benefit_line" not in context


def test_plan_rows_from_target_metadata_keep_target_context():
    """Plan metadata keeps context needed by downstream discovery search."""
    target = discovery.CrawlTarget(
        source={"source_id": "source_1", "display_name": "Example TPA"},
        url="https://example.test/mrf/index.json?groupNumber=1208",
        label="Example group 1208",
        metadata={
            "company_name": "Example Forge LLC",
            "employer_name": "Example Forge LLC",
            "evidence_url": "https://example.test/asr-1208",
            "plan_info": [
                {"plan_id": "1208", "plan_market_type": "group", "plan_name": "Plan"}
            ],
            "plan_name": "Example Forge ASR Plan",
            "resolver": "example_resolver",
        },
    )

    [row] = discovery._plan_rows_from_target_metadata(target)

    metadata = row["metadata_json"]
    assert metadata["group_number"] == "1208"
    assert metadata["company_name"] == "Example Forge LLC"
    assert metadata["employer_name"] == "Example Forge LLC"
    assert metadata["plan_name"] == "Example Forge ASR Plan"
    assert metadata["evidence_url"] == "https://example.test/asr-1208"


def test_filter_crawl_targets_by_resolver_patterns_keeps_configured_urls_only():
    targets = [
        discovery.CrawlTarget(
            source={"source_id": "source_1"},
            url="https://www.ohiohealthchoice.com/FPTIC/FP_in-network-rates_4.json",
        ),
        discovery.CrawlTarget(
            source={"source_id": "source_1"},
            url="https://www.ohiohealthchoice.com/FPTIC/Wrap/MPI_MPI_innetworkrates.json",
        ),
        discovery.CrawlTarget(
            source={"source_id": "source_1"},
            url="https://example.com/expired-signed-file.json.gz?Expires=1",
        ),
    ]

    filtered = discovery._filter_crawl_targets_by_resolver_patterns(
        targets,
        {
            "include_url_patterns": [
                r"^https://www\.ohiohealthchoice\.com/FPTIC/FP_[^?#]+\.json$"
            ],
            "exclude_url_patterns": [r"Expires="],
        },
    )

    assert [target.url for target in filtered] == [
        "https://www.ohiohealthchoice.com/FPTIC/FP_in-network-rates_4.json"
    ]


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
    assert candidate.source_tier == "coverage_evidence"


def test_master_list_importable_source_filter_keeps_only_working_url_rows():
    assert discovery._is_candidate_importable_source(
        discovery.SourceCandidate(
            payer_name="Active",
            provider="master-list",
            index_url="https://example.com/index.json",
            status="active",
        )
    )
    assert not discovery._is_candidate_importable_source(
        discovery.SourceCandidate(
            payer_name="Stale",
            provider="master-list",
            index_url="https://example.com/stale.json",
            status="stale",
        )
    )
    assert not discovery._is_source_row_importable(
        {"source_tier": "mrf_importable", "status": "stale"}
    )
    assert not discovery._is_candidate_importable_source(
        discovery.SourceCandidate(
            payer_name="Needs Review", provider="master-list", status="needs_review"
        )
    )
    assert not discovery._is_candidate_importable_source(
        discovery.SourceCandidate(
            payer_name="Needs Review With Url",
            provider="master-list",
            index_url="https://example.com/unverified",
            status="needs_review",
        )
    )
    assert not discovery._is_candidate_importable_source(
        discovery.SourceCandidate(
            payer_name="Unsupported",
            provider="master-list",
            index_url="https://example.com/old",
            status="unsupported",
        )
    )
    assert not discovery._is_candidate_importable_source(
        discovery.SourceCandidate(
            payer_name="Archived",
            provider="master-list",
            index_url="https://example.com/archive",
            status="archived",
        )
    )


def test_master_list_marks_replaced_viva_transparency_url_archived():
    markdown = """
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| VIVA Health | provider_sponsored | https://www.vivahealth.com/transparency-in-coverage/ | observed archived; replaced by public VIVA MRF landing |
"""

    [candidate] = discovery.parse_master_list(markdown)

    assert candidate.payer_name == "VIVA Health"
    assert candidate.status == "archived"
    assert candidate.hosting_platform == "custom"
    assert not discovery._is_candidate_importable_source(candidate)


def test_master_list_demotes_replaced_transparency_legal_page():
    markdown = """
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| Example Regional Plan | regional | https://transparency.example.test/ | curated source row |
| Example Regional Plan | regional | https://www.example.test/about/transparency-coverage-compliance | observed stale; replaced by dedicated price-transparency machine-readable-files host |
"""

    candidates = discovery.parse_master_list(markdown)

    by_url = {candidate.index_url: candidate for candidate in candidates}
    current = by_url["https://transparency.example.test/"]
    replaced = by_url[
        "https://www.example.test/about/transparency-coverage-compliance"
    ]

    assert current.status == "active"
    assert current.hosting_platform == "custom"
    assert discovery._is_candidate_importable_source(current)
    assert replaced.status == "stale"
    assert not discovery._is_candidate_importable_source(replaced)


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

    assert (
        candidate.index_url == "https://example.com/transparency#machine-readable-files"
    )


def test_classify_hosting_platforms():
    """Verify this source-discovery regression contract."""
    assert (
        discovery.classify_hosting_platform("https://transparency-in-coverage.uhc.com/")
        == "uhc_public_blobs"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://providermrf.uhc.com/api/files/ui/ifp/"
        )
        == "uhc_provider_mrf_files"
    )
    assert (
        discovery.classify_hosting_platform("https://bci.sapphiremrfhub.com/")
        == "sapphire"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://bcbsm.sapphiremrfhub.com/tocs/current/vsp_vision"
        )
        == "sapphire"
    )
    assert (
        discovery.classify_hosting_platform("https://mrfdata.hmhs.com/")
        == "highmark_hmhs"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.bcbswy.com/machine-readable-files/"
        )
        == "bcbswy_hmhs_monthly_toc"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.bcbsil.com/asomrf?EIN=260241222"
        )
        == "bcbs_asomrf"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://bcbsglobalsolutions.com/transparency-in-coverage/"
        )
        == "bcbs_global_solutions_mrf"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://groupadmin.bcbsglobalsolutions.com/transparency-in-coverage-toc-json.cfm?planType=4EverLife"
        )
        == "bcbs_global_solutions_mrf"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.bluecrossnc.com/policies-best-practices/machine-readable-files"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://mrfmftprod.bcbsnc.com/prod/etl/outbound/table-of-contents/non-aso/"
            "2026-05-27_blue-cross-and-blue-shield-of-north-carolina_index.json"
        )
        == "direct_toc"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://transparency-in-coverage.bluecrossma.com/"
        )
        == "bcbsma_monthly_tocs"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.cigna.com/legal/compliance/machine-readable-files"
        )
        == "cigna_static_mrf_lookup"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://transparency.auxiant.com/directory-of-data-sources/"
        )
        == "auxiant_wordpress"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://transparency.auxiant.com/healthsmart/"
        )
        == "auxiant_wordpress"
    )
    assert (
        discovery.classify_hosting_platform("https://www.asrhealthbenefits.com/MRF")
        == "asr_health_benefits"
    )
    assert (
        discovery.classify_hosting_platform("https://mrfsearch.meritain.com/")
        == "meritain_mrf_search"
    )
    assert (
        discovery.classify_hosting_platform("https://mrf.healthcarebluebook.com/Lucent")
        == "healthcarebluebook_mrf"
    )
    assert (
        discovery.classify_hosting_platform("https://mrf.healthgram.com/")
        == "healthgram"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.myhealthbenefits.com/MyHealthBenefits/Home/MRFs/"
        )
        == "html_mrf_with_healthcarebluebook"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://lucenthealth.com/transparency-in-coverage/"
        )
        == "html_mrf_with_healthcarebluebook"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://hpitpa.com/transparency-in-coverage-machine-readable-files/"
        )
        == "html_mrf_with_healthcarebluebook"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.priorityhealth.com/landing/transparency"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.anglehealth.com/machine-readable-files"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://mrf.pacificsource.com/File/Visit/Index"
        )
        == "pacificsource_azure_mrf_listing"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.cchealth.org/health-insurance/my-contra-costa-health-plan/transparency-in-coverage"
        )
        == "hostedjson_azure_mrf_listing"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://hostedjson.blob.core.windows.net/transparencyfiles?restype=container&comp=list"
        )
        == "hostedjson_azure_mrf_listing"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://ghcscw.com/transparency-in-coverage"
        )
        == "ghcscw_azure_mrf_listing"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://salud.grupotriples.com/en/transparency-in-coverage-machine-readable-files/"
        )
        == "triples_mtt_api"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://salud.grupotriples.com/en/wp-json/app/v1/mtt?network=Puerto+Rico"
        )
        == "triples_mtt_api"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://sawus2prdticmrfhma.z5.web.core.windows.net/"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.lacare.org/transparency-coverage-machine-readable-files"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.gravie.com/compliance/transparency-in-coverage/"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.mymedicalshopper.com/mrf-search/varipro"
        )
        == "mymedicalshopper_talon"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.mymedicalshopper.com/mrf-search/diversified-group"
        )
        == "mymedicalshopper_talon_bounded"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.mymedicalshopper.com/mrf/sample-employer-network-varipro-77100"
        )
        == "mymedicalshopper_talon"
    )
    assert (
        discovery.classify_hosting_platform("https://clm.magnacare.com/transparency/")
        == "magnacare_transparency_mrf"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.blueadvantagearkansas.com/interoperability/machine-readable-files"
        )
        == "blueadvantage_html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.geha.com/transparency-in-coverage"
        )
        == "html_delegated_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.sharphealthplan.com/api-access-for-developers"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://group-health.com/price-transparency"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.wpshealth.com/resources/customer-resources/price-transparency.shtml"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://files.myplancentral.com/TIC/TOC/")
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://data.sccgov.org/data.json")
        == "socrata_data_json_mrf_catalog"
    )
    assert (
        discovery.classify_hosting_platform("https://transparency.emblemhealth.com/")
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://transparency.connecticare.com/OON")
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform(
            "https://www.securityhealth.org/insurance-resources/json"
        )
        == "html_mrf_links"
    )
    assert (
        discovery.classify_hosting_platform("https://transparency.lacare.org")
        == "lacare_s3_listing"
    )
    assert (
        discovery.classify_hosting_platform("https://mrfhub.providencehealthplan.com/")
        == "providence_mrf_api"
    )
    assert (
        discovery.classify_hosting_platform("https://github.com/ExampleCarrier/MRF")
        == "github_repo_mrf"
    )


def test_meritain_mrf_search_parser_extracts_group_healthsparq_links():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Meritain Health",
    }
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

    [crawl_target] = discovery._parse_meritain_mrf_search_targets(
        html,
        base_url="https://mrfsearch.meritain.com/",
        source_row_dict=source_dict,
        resolver_type="meritain_mrf_search",
    )

    assert crawl_target.url == (
        "https://Health1.Meritain.com/app/public/#/one/insurerCode=MERITAIN_I&brandCode=MERITAINOVER/"
        "machine-readable-transparency-in-coverage?reportingEntityType=TPA_14445&lock=true"
    )
    assert crawl_target.metadata["target_kind"] == "file_reference"
    assert crawl_target.metadata["target_file_type"] == "table-of-contents"
    assert crawl_target.metadata["group_id"] == "14445"
    assert crawl_target.metadata["plan_info"] == [
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

    importer_items = discovery._healthcarebluebook_grid_items(
        html, base_url="https://mrf.healthcarebluebook.com/Lucent"
    )

    assert importer_items == [
        {
            "url": "https://mrf.healthcarebluebook.com/Lucent/350504",
            "label": "Lucent Health",
            "text": "Lucent Health",
        },
        {"url": None, "label": "Table of Contents", "text": "Table of Contents"},
        {
            "url": "https://mrf.healthcarebluebook.com/Lucent/350380",
            "label": "Lucent Health 042171239",
            "text": "Lucent Health 042171239",
        },
        {"url": None, "label": "Out of Network", "text": "Out of Network"},
        {
            "url": "https://hcbbmrfprod.blob.core.windows.net/mrf/External/example_in-network-rates.json.zip",
            "label": "Center Care",
            "text": "Center Care",
        },
        {"url": None, "label": "In Network", "text": "In Network"},
    ]


def test_healthcarebluebook_grid_parser_extracts_nested_data_href_context():
    html = """
    <article class="card">
      <h2>Lucent Health 042171239</h2>
      <button data-href="/Lucent/350380">Download</button>
      <p>Out of Network</p>
    </article>
    """

    items = discovery._healthcarebluebook_grid_items(
        html, base_url="https://mrf.healthcarebluebook.com/Lucent"
    )

    assert items == [
        {
            "url": "https://mrf.healthcarebluebook.com/Lucent/350380",
            "label": "Lucent Health 042171239 Download Out of Network",
            "text": "Lucent Health 042171239 Download Out of Network",
        },
        {
            "url": None,
            "label": "Lucent Health 042171239 Download Out of Network",
            "text": "Lucent Health 042171239 Download Out of Network",
        },
    ]


@pytest.mark.asyncio
async def test_healthgram_resolver_expands_network_pages_to_toc_links(monkeypatch):
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Healthgram",
    }
    html_by_url = {
        "https://mrf.healthgram.com/": """
          <a href="network/example.cfm">Example Network</a>
          <a href="network/other.cfm">Other Network</a>
        """,
        "https://mrf.healthgram.com/network/example.cfm": """
          <a href="/mrfiles/EXAM/Example_index.json" download>2026-06-01_Example_index.json</a>
        """,
        "https://mrf.healthgram.com/network/other.cfm": """
          <a href="/mrfiles/OTHR/Other_index.json" download>2026-06-01_Other_index.json</a>
        """,
    }

    async def fake_fetch_text(url, **_kwargs):
        return html_by_url[url]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    network_targets = await discovery._resolve_healthgram_network_index(
        catalog_source_dict,
        "https://mrf.healthgram.com/",
        {"type": "healthgram_network_index"},
        None,
    )

    assert [network_target.url for network_target in network_targets] == [
        "https://mrf.healthgram.com/mrfiles/EXAM/Example_index.json",
        "https://mrf.healthgram.com/mrfiles/OTHR/Other_index.json",
    ]
    assert network_targets[0].label == "Example Network"
    assert network_targets[0].metadata["target_file_type"] == "table-of-contents"
    assert (
        network_targets[0].metadata["healthgram_index_label"] == "2026-06-01_Example_index.json"
    )
    assert network_targets[0].metadata["healthgram_network_name"] == "Example Network"
    assert (
        network_targets[0].resolved_from_url == "https://mrf.healthgram.com/network/example.cfm"
    )


@pytest.mark.asyncio
async def test_github_repo_resolver_expands_public_tree_to_raw_mrf_files(monkeypatch):
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Carrier",
    }
    github_response_by_url = {
        "https://api.github.com/repos/ExampleCarrier/MRF": {"default_branch": "main"},
        "https://api.github.com/repos/ExampleCarrier/MRF/git/trees/main?recursive=1": {
            "tree": [
                {
                    "path": "in-network/2026-06-01_example_carrier_alpha_plan_in-network-rates.json.gz",
                    "type": "blob",
                    "sha": "sha_in",
                    "size": 123,
                },
                {
                    "path": "allowed-amounts/2026-06-01_example_carrier_alpha_plan_allowed-amounts.zip",
                    "type": "blob",
                    "sha": "sha_aa",
                    "size": 456,
                },
                {"path": "README.md", "type": "blob", "sha": "sha_readme", "size": 10},
            ]
        },
    }

    async def fake_fetch_json(url, **_kwargs):
        return github_response_by_url[url]

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    repository_targets = await discovery._resolve_github_repo_mrf(
        catalog_source_dict,
        "https://github.com/ExampleCarrier/MRF",
        {"type": "github_repo_mrf_tree"},
        None,
    )

    assert [repository_target.url for repository_target in repository_targets] == [
        "https://raw.githubusercontent.com/ExampleCarrier/MRF/main/in-network/2026-06-01_example_carrier_alpha_plan_in-network-rates.json.gz",
        "https://raw.githubusercontent.com/ExampleCarrier/MRF/main/allowed-amounts/2026-06-01_example_carrier_alpha_plan_allowed-amounts.zip",
    ]
    assert repository_targets[0].metadata["target_kind"] == "file_reference"
    assert repository_targets[0].metadata["target_file_type"] == "in-network"
    assert repository_targets[0].metadata["github_path"].endswith("in-network-rates.json.gz")
    assert repository_targets[0].metadata["blob_size"] == 123
    assert repository_targets[0].metadata["plan_info"][0]["plan_name"] == "Carrier Alpha Plan"
    assert repository_targets[1].metadata["target_file_type"] == "allowed-amounts"


@pytest.mark.asyncio
async def test_crawl_target_limit_caps_persisted_target_rows(monkeypatch):
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Carrier",
    }
    crawl_targets = [
        discovery.CrawlTarget(
            source=catalog_source_dict,
            url=f"https://example.test/mrf/plan-{index}_in-network-rates.json.gz",
            label=f"Example Plan {index}",
            metadata={
                "target_kind": "file_reference",
                "target_file_type": "in-network",
                "plan_info": [
                    {"plan_name": f"Example Plan {index}", "plan_market_type": "group"}
                ],
            },
        )
        for index in range(3)
    ]
    captured_file_batches: list[list[dict]] = []

    async def fake_resolve_crawl_targets(*_args, **_kwargs):
        return crawl_targets, []

    async def fake_push_crawl_row_batches(
        _plan_rows, file_rows, _observation_rows, **_kwargs
    ):
        if file_rows:
            captured_file_batches.append([dict(file_row) for file_row in file_rows])

    monkeypatch.setattr(discovery, "_resolve_crawl_targets", fake_resolve_crawl_targets)
    monkeypatch.setattr(
        discovery, "_push_crawl_row_batches", fake_push_crawl_row_batches
    )

    plans_discovered, files_discovered, observations = (
        await discovery._crawl_toc_metadata(
            [catalog_source_dict],
            test_mode=False,
            run_id="run_1",
            max_toc_bytes=1024,
            concurrency=3,
            crawl_target_limit=2,
        )
    )

    assert plans_discovered == 2
    assert files_discovered == 2
    assert len(observations) == 2
    assert len(captured_file_batches) in {1}
    assert [file_row["description"] for file_row in captured_file_batches[0]] == [
        "Example Plan 0",
        "Example Plan 1",
    ]


@pytest.mark.asyncio
async def test_zipped_toc_plan_scope_filters_crawled_rows(monkeypatch):
    query_source_dict = _synthetic_query_source()
    scoped_zip_target = _static_lookup_scoped_zip_target(query_source_dict)
    assert scoped_zip_target.metadata["target_kind"] == "file_reference"
    assert scoped_zip_target.metadata["container_format"] == "zip"
    zip_members = _synthetic_scoped_zip_members()
    resolver_mock = AsyncMock(return_value=([scoped_zip_target], []))
    zip_fetch_mock = AsyncMock(return_value=zip_members)
    batch_push_mock = AsyncMock()

    monkeypatch.setattr(discovery, "_resolve_crawl_targets", resolver_mock)
    monkeypatch.setattr(discovery, "_fetch_zip_json_values", zip_fetch_mock)
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", batch_push_mock)

    plans_discovered, files_discovered, observations = (
        await discovery._crawl_toc_metadata(
            [query_source_dict],
            test_mode=False,
            run_id="run_example",
            max_toc_bytes=2048,
            concurrency=1,
        )
    )
    pushed_plan_rows = [
        plan_row
        for batch_call in batch_push_mock.await_args_list
        for plan_row in batch_call.args[0]
    ]
    pushed_file_rows = [
        file_row
        for batch_call in batch_push_mock.await_args_list
        for file_row in batch_call.args[1]
    ]

    assert plans_discovered == 1
    assert files_discovered == 2
    assert len(observations) == 1
    assert zip_fetch_mock.await_count == 1
    assert [plan_row["plan_id"] for plan_row in pushed_plan_rows] == ["111111111"]
    assert [
        file_row["url"]
        for file_row in pushed_file_rows
        if file_row["file_type"] == "in-network"
    ] == ["https://example.test/matching.json.gz"]


@pytest.mark.asyncio
async def test_resolve_crawl_targets_filters_query_expansion_matches(monkeypatch):
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "BCBS Louisiana",
        "metadata_json": {"raw": {"target_payer_query": "Example Employer"}},
    }
    candidate_targets = [
        discovery.CrawlTarget(
            source=catalog_source_dict,
            url="https://example.sapphiremrfhub.com/tocs/current/example-employer",
            label="Example Employer",
            metadata={"payer_name": "Example Employer", "file_name": "example-employer"},
        ),
        discovery.CrawlTarget(
            source=catalog_source_dict,
            url="https://example.sapphiremrfhub.com/tocs/current/other-employer",
            label="Other Employer",
            metadata={"payer_name": "Other Employer", "file_name": "other-employer"},
        ),
    ]

    async def fake_crawl_targets_for_source(*_args, **_kwargs):
        return candidate_targets

    monkeypatch.setattr(
        discovery, "_crawl_targets_for_source", fake_crawl_targets_for_source
    )

    resolved, observations = await discovery._resolve_crawl_targets(
        [{**catalog_source_dict, "index_url": "https://example.sapphiremrfhub.com/"}],
        session=None,
        run_id="run_1",
        concurrency=2,
    )

    assert observations == []
    assert [crawl_target.label for crawl_target in resolved] == ["Example Employer"]
    assert resolved[0].metadata["query_expansion_match"] is True
    assert resolved[0].metadata["company_name"] == "Example Employer"


@pytest.mark.asyncio
async def test_resolve_crawl_targets_reports_query_filter_miss(monkeypatch):
    query_source_dict = _synthetic_query_source()
    unrelated_target = discovery.CrawlTarget(
        source=query_source_dict,
        url="https://example.test/unrelated-employer.json.gz",
        label="Unrelated Employer",
    )
    monkeypatch.setattr(
        discovery,
        "_crawl_targets_for_source",
        AsyncMock(return_value=[unrelated_target]),
    )

    resolved_targets, observations = await discovery._resolve_crawl_targets(
        [query_source_dict],
        session=None,
        run_id="run_example",
        concurrency=1,
    )

    assert resolved_targets == []
    assert len(observations) == 1
    assert observations[0]["error"] == (
        "resolved crawl targets did not match the target payer query"
    )


@pytest.mark.asyncio
async def test_query_expansion_filters_and_dedupes_with_crawl_target_limit(
    monkeypatch,
):
    query_source_dict = _synthetic_query_source(
        source_id="source_example",
        platform="html_mrf_links",
        index_url="https://example.test/mrf",
    )
    generic_crawl_targets = [
        discovery.CrawlTarget(
            source=query_source_dict,
            url=f"https://example.test/generic-{index}.json",
            label=f"Generic Employer {index}",
        )
        for index in range(60)
    ]
    first_matching_target = discovery.CrawlTarget(
        source=query_source_dict,
        url="https://example.test/sample-employer-a.json",
        label="Sample Employer Alpha",
    )
    resolver_targets = [
        *generic_crawl_targets,
        first_matching_target,
        first_matching_target,
        discovery.CrawlTarget(
            source=query_source_dict,
            url="https://example.test/sample-employer-b.json",
            label="Sample Employer Beta",
        ),
    ]
    crawl_mock = AsyncMock(return_value=resolver_targets)
    monkeypatch.setattr(discovery, "_crawl_targets_for_source", crawl_mock)

    resolved_targets, observations = await discovery._resolve_crawl_targets(
        [query_source_dict],
        session=object(),
        run_id="run_example",
        concurrency=1,
        crawl_target_limit=2,
    )

    assert observations == []
    assert crawl_mock.await_args.kwargs["target_limit"] == 2
    assert [crawl_target.url for crawl_target in resolved_targets] == [
        "https://example.test/sample-employer-a.json",
        "https://example.test/sample-employer-b.json",
    ]
    assert all(
        crawl_target.metadata["query_expansion_match"] is True
        for crawl_target in resolved_targets
    )


@pytest.mark.asyncio
async def test_html_mrf_resolver_filters_query_before_target_limit(monkeypatch):
    query_source = _synthetic_query_source(
        source_id="source_html",
        platform="html_mrf_links",
        index_url="https://example.test/mrf",
    )
    unrelated_links = "".join(
        f'<a href="unrelated-{index}_in-network-rates.json.gz">'
        f"Unrelated Employer {index}</a>"
        for index in range(4)
    )
    html_text = (
        unrelated_links
        + '<a href="sample-employer-a_in-network-rates.json.gz">Sample Employer A</a>'
        + '<a href="sample-employer-b_in-network-rates.json.gz">Sample Employer B</a>'
    )
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value=html_text))

    matched_targets = await discovery._resolve_html_mrf_links(
        query_source,
        query_source["index_url"],
        {
            "type": "html_mrf_links",
            "max_targets": 2,
            "follow_directory_links": False,
            "follow_iframe_links": False,
        },
        session=None,
    )

    assert [matched_target.url for matched_target in matched_targets] == [
        "https://example.test/sample-employer-a_in-network-rates.json.gz",
        "https://example.test/sample-employer-b_in-network-rates.json.gz",
    ]


@pytest.mark.asyncio
async def test_healthcarebluebook_resolver_filters_nested_links_by_target_query(
    monkeypatch,
):
    discovery_source_dict = {
        "source_id": "source_hbb",
        "display_name": "Example HBB",
        "metadata_json": {"raw": {"target_payer_query": "Example Employer"}},
    }
    html = """
    <section>
      <a href="https://example.sapphiremrfhub.com/">Example Employer</a>
      <span>Table of Contents</span>
    </section>
    <section>
      <a href="https://other.sapphiremrfhub.com/">Other Employer</a>
      <span>Table of Contents</span>
    </section>
    """
    nested_calls = []

    async def fake_fetch_text(url, **_kwargs):
        assert url == "https://mrf.healthcarebluebook.com/example"
        return html

    async def fake_crawl_targets_for_source(nested_source, link_url, _session, **_kwargs):
        nested_calls.append(link_url)
        return [
            discovery.CrawlTarget(
                source=nested_source,
                url="https://example.sapphiremrfhub.com/tocs/current/example-employer",
                label="Example Employer",
                resolved_from_url=link_url,
                metadata={"resolver": "sapphire"},
            )
        ]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(
        discovery, "_crawl_targets_for_source", fake_crawl_targets_for_source
    )

    discovery_targets = await discovery._resolve_healthcarebluebook_mrf(
        discovery_source_dict,
        "https://mrf.healthcarebluebook.com/example",
        {"type": "healthcarebluebook_mrf"},
        session=None,
    )

    assert nested_calls == ["https://example.sapphiremrfhub.com/"]
    assert [discovery_target.label for discovery_target in discovery_targets] == ["Example Employer"]
    assert discovery_targets[0].metadata["healthcarebluebook_link_label"] == "Example Employer"


@pytest.mark.asyncio
async def test_generic_html_file_reference_infers_plan_info_from_filename(monkeypatch):
    discovery_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Carrier",
    }
    html = """
    <html><body>
      <a href="2026-06-01_EXAMPLE_123-Alpha-Benefit-Plan_ffs_in-network.json.gz">
        2026-06-01_EXAMPLE_123-Alpha-Benefit-Plan_ffs_in-n..&gt;
      </a>
    </body></html>
    """

    async def fake_fetch_text(*_args, **_kwargs):
        return html

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    [discovery_target] = await discovery._crawl_targets_for_source(
        discovery_source_dict, "https://example.test/mrf/", None
    )

    assert discovery_target.label == "123 Alpha Benefit Plan Ffs"
    assert discovery_target.metadata["target_kind"] == "file_reference"
    assert discovery_target.metadata["target_file_type"] == "in-network"
    assert discovery_target.metadata["plan_info"] == [
        {
            "plan_id": None,
            "plan_id_type": None,
            "plan_market_type": "group",
            "plan_name": "123 Alpha Benefit Plan Ffs",
        }
    ]


@pytest.mark.asyncio
async def test_html_mrf_resolver_follows_mrf_iframe_pages(monkeypatch):
    discovery_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Group Administrators",
    }
    html_by_url = {
        "https://www.groupadministrators.com/machinereadablefiles/": """
          <iframe src="/mrfhtml/mrf2023082301.html?parm=2023082302"></iframe>
        """,
        "https://www.groupadministrators.com/mrfhtml/mrf2023082301.html?parm=2023082302": """
          <a href="https://mrf.example.test/example_in-network-rates.json">
            Example In-Network
          </a>
          <a href="https://mrf.example.test/example_allowed-amounts.json">
            Example Out-of-Network
          </a>
        """,
    }

    async def fake_fetch_text(url, **_kwargs):
        return html_by_url[url]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    discovery_targets = await discovery._resolve_html_mrf_links(
        discovery_source_dict,
        "https://www.groupadministrators.com/machinereadablefiles/",
        {"type": "html_mrf_links", "max_frames": 2},
        None,
    )

    assert [discovery_target.url for discovery_target in discovery_targets] == [
        "https://mrf.example.test/example_in-network-rates.json",
        "https://mrf.example.test/example_allowed-amounts.json",
    ]
    assert discovery_targets[0].metadata["resolver"] == "html_file_reference"
    assert discovery_targets[0].metadata["target_file_type"] == "in-network"
    assert discovery_targets[0].metadata["frame_url"] == (
        "https://www.groupadministrators.com/mrfhtml/mrf2023082301.html?parm=2023082302"
    )
    assert discovery_targets[1].metadata["target_file_type"] == "allowed-amounts"


@pytest.mark.asyncio
async def test_html_mrf_resolver_can_follow_directories_on_mixed_pages(monkeypatch):
    discovery_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Boon-Chapman",
    }
    html_by_url = {
        "https://boonchapman-mrf.zakipointhealth.com/": """
          <a href="https://mrf-public-collection.s3.amazonaws.com/boonchapman/allowed_amount/division_id=002429/002429.zip">
            002429
          </a>
          <span class="label">Out of network</span>
          <a href="https://www.healthplan.org/first_health_mrfs">First Health</a>
        """,
        "https://www.healthplan.org/first_health_mrfs": """
          <a href="/mrf/first-health/2026-06-01_first-health_index.json">
            First Health index
          </a>
        """,
    }

    async def fake_fetch_text(url, **_kwargs):
        return html_by_url[url]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    discovery_targets = await discovery._resolve_html_mrf_links(
        discovery_source_dict,
        "https://boonchapman-mrf.zakipointhealth.com/",
        {
            "type": "html_mrf_links",
            "follow_directory_links_when_targets": True,
            "max_directories": 2,
        },
        None,
    )

    assert [discovery_target.url for discovery_target in discovery_targets] == [
        (
            "https://mrf-public-collection.s3.amazonaws.com/boonchapman/"
            "allowed_amount/division_id=002429/002429.zip"
        ),
        "https://www.healthplan.org/mrf/first-health/2026-06-01_first-health_index.json",
    ]
    assert discovery_targets[0].metadata["target_file_type"] == "allowed-amounts"
    assert discovery_targets[1].metadata["target_file_type"] == "table-of-contents"
    assert discovery_targets[1].metadata["directory_url"] == (
        "https://www.healthplan.org/first_health_mrfs"
    )


@pytest.mark.asyncio
async def test_html_mrf_resolver_follows_nested_directory_pages(monkeypatch):
    discovery_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Health Plan",
    }
    html_by_url = {
        "https://example.test/transparency": """
          <a href="/vendor/machine-readable-data">Machine-readable data</a>
        """,
        "https://example.test/vendor/machine-readable-data": """
          <script>
            window.__MRF__ = {
              "inn": "https://mrfproddestinationdata.blob.core.windows.net/example-mrf-output/Example-INN_index.html"
            };
          </script>
        """,
        "https://mrfproddestinationdata.blob.core.windows.net/example-mrf-output/Example-INN_index.html": """
          <a href="https://mrfproddestinationdata.blob.core.windows.net/example-mrf-output/2026-06-01_Example-INN_index.json">
            Index
          </a>
        """,
    }

    async def fake_fetch_text(url, **_kwargs):
        return html_by_url[url]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    discovery_targets = await discovery._resolve_html_mrf_links(
        discovery_source_dict,
        "https://example.test/transparency",
        {"type": "html_mrf_links", "max_directories": 2},
        None,
    )

    assert [discovery_target.url for discovery_target in discovery_targets] == [
        "https://mrfproddestinationdata.blob.core.windows.net/example-mrf-output/2026-06-01_Example-INN_index.json"
    ]
    assert discovery_targets[0].metadata["resolver"] == "html_mrf_link"
    assert discovery_targets[0].metadata["target_file_type"] == "table-of-contents"
    assert discovery_targets[0].metadata["directory_url"] == (
        "https://example.test/vendor/machine-readable-data"
    )
    assert discovery_targets[0].metadata["nested_directory_url"] == (
        "https://mrfproddestinationdata.blob.core.windows.net/example-mrf-output/Example-INN_index.html"
    )


@pytest.mark.asyncio
async def test_healthez_resolver_normalizes_legacy_network_links(monkeypatch):
    discovery_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "HealthEZ",
    }
    html = """
    <a href="/api/outbound/latest?fileType=inNetwork&groupName=HealthEZ=AP">
      AP Machine Readable Files
    </a>
    <a href="/api/outbound/latest?fileType=inNetwork&groupName=HealthEZ=AE">
      AE Machine Readable Files
    </a>
    <a href="/api/outbound/latest?fileType=outOfNetwork&groupName=HealthEZ">
      Out of Network Machine Readable Files
    </a>
    """

    async def fake_fetch_text(url, **_kwargs):
        assert url == "https://healthezbenefits.com/plandocuments/"
        return html

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    discovery_targets = await discovery._resolve_healthez_benefits_mrf(
        discovery_source_dict,
        "https://healthezbenefits.com/plandocuments/",
        {"type": "healthez_benefits_mrf"},
        None,
    )

    assert [discovery_target.url for discovery_target in discovery_targets] == [
        (
            "https://healthezbenefits.com/api/outbound/latest?"
            "fileType=inNetwork&groupName=HealthEZ&network=AP"
        ),
        (
            "https://healthezbenefits.com/api/outbound/latest?"
            "fileType=inNetwork&groupName=HealthEZ&network=AE"
        ),
        (
            "https://healthezbenefits.com/api/outbound/latest?"
            "fileType=outOfNetwork&groupName=HealthEZ"
        ),
    ]
    assert [discovery_target.label for discovery_target in discovery_targets] == [
        "HealthEZ AP",
        "HealthEZ AE",
        "HealthEZ",
    ]
    assert [discovery_target.metadata["target_file_type"] for discovery_target in discovery_targets] == [
        "in-network",
        "in-network",
        "allowed-amounts",
    ]
    assert all(discovery_target.metadata["container_format"] == "zip" for discovery_target in discovery_targets)


@pytest.mark.asyncio
async def test_healthcarebluebook_resolver_catalogs_stable_file_links(monkeypatch):
    discovery_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Lucent Health",
    }
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
    monkeypatch.setattr(
        discovery,
        "_is_healthcarebluebook_numeric_url_downloadable",
        AsyncMock(return_value=True),
    )

    discovery_targets = await discovery._resolve_healthcarebluebook_mrf(
        discovery_source_dict,
        "https://mrf.healthcarebluebook.com/Lucent",
        {"type": "healthcarebluebook_mrf"},
        None,
    )

    assert [discovery_target.url for discovery_target in discovery_targets] == [
        "https://mrf.healthcarebluebook.com/Lucent/350504",
        "https://mrf.healthcarebluebook.com/Lucent/350380",
        "https://hcbbmrfprod.blob.core.windows.net/mrf/External/example_in-network-rates.json.zip",
    ]
    assert discovery_targets[0].metadata["target_file_type"] == "table-of-contents"
    assert discovery_targets[0].metadata["source_format"] == "zip"
    assert discovery_targets[1].metadata["target_file_type"] == "allowed-amounts"
    assert discovery_targets[1].metadata["plan_info"][0]["plan_id"] == "042171239"
    assert discovery_targets[1].metadata["plan_info"][0]["plan_id_type"] == "ein"
    assert discovery_targets[2].metadata["target_file_type"] == "in-network"
    assert discovery_targets[2].metadata["container_format"] == "zip"


@pytest.mark.asyncio
async def test_healthcarebluebook_resolver_skips_html_error_numeric_links(
    monkeypatch,
):
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example HBB",
    }
    html = """
    <div class="grid-item"><a href="/Example/111111">Dead file</a></div>
    <div class="grid-item">Table of Contents</div>
    <div class="grid-item"><a href="/Example/222222">Valid file</a></div>
    <div class="grid-item">Table of Contents</div>
    """

    async def fake_fetch_text(url, **_kwargs):
        assert url == "https://mrf.healthcarebluebook.com/Example"
        return html

    async def is_numeric_file_downloadable(url, _session):
        return url.endswith("/222222")

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(
        discovery,
        "_is_healthcarebluebook_numeric_url_downloadable",
        is_numeric_file_downloadable,
    )

    [download_target] = await discovery._resolve_healthcarebluebook_mrf(
        catalog_source_dict,
        "https://mrf.healthcarebluebook.com/Example",
        {"type": "healthcarebluebook_mrf"},
        None,
    )

    assert download_target.url == "https://mrf.healthcarebluebook.com/Example/222222"
    assert download_target.label == "Valid file"


@pytest.mark.asyncio
async def test_healthcarebluebook_resolver_extracts_table_row_data_href(monkeypatch):
    discovery_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Lucent Health",
    }
    html = """
    <table>
      <tr>
        <td>Lucent Health 042171239</td>
        <td>Out of Network</td>
        <td><a data-href="/Lucent/350380">Download</a></td>
      </tr>
    </table>
    """

    async def fake_fetch_text(url, **_kwargs):
        assert url == "https://mrf.healthcarebluebook.com/Lucent"
        return html

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(
        discovery,
        "_is_healthcarebluebook_numeric_url_downloadable",
        AsyncMock(return_value=True),
    )

    [discovery_target] = await discovery._resolve_healthcarebluebook_mrf(
        discovery_source_dict,
        "https://mrf.healthcarebluebook.com/Lucent",
        {"type": "healthcarebluebook_mrf"},
        None,
    )

    assert discovery_target.url == "https://mrf.healthcarebluebook.com/Lucent/350380"
    assert discovery_target.metadata["target_file_type"] == "allowed-amounts"
    assert discovery_target.metadata["plan_info"][0]["plan_id"] == "042171239"
    assert discovery_target.metadata["plan_info"][0]["plan_id_type"] == "ein"


@pytest.mark.asyncio
async def test_healthcarebluebook_resolver_applies_max_targets_early(monkeypatch):
    discovery_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Lucent Health",
    }
    html = """
    <div class="grid-item"><a href="/Lucent/350504">Lucent Health</a></div>
    <div class="grid-item">Table of Contents</div>
    <div class="grid-item"><a href="/Lucent/350380">Lucent Health 042171239</a></div>
    <div class="grid-item">Out of Network</div>
    <div class="grid-item"><a href="/Lucent/350381">Lucent Health 052171239</a></div>
    <div class="grid-item">Out of Network</div>
    """

    async def fake_fetch_text(url, **_kwargs):
        assert url == "https://mrf.healthcarebluebook.com/Lucent"
        return html

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(
        discovery,
        "_is_healthcarebluebook_numeric_url_downloadable",
        AsyncMock(return_value=True),
    )

    discovery_targets = await discovery._resolve_healthcarebluebook_mrf(
        discovery_source_dict,
        "https://mrf.healthcarebluebook.com/Lucent",
        {"type": "healthcarebluebook_mrf", "max_targets": 2},
        None,
    )

    assert [discovery_target.url for discovery_target in discovery_targets] == [
        "https://mrf.healthcarebluebook.com/Lucent/350504",
        "https://mrf.healthcarebluebook.com/Lucent/350380",
    ]


@pytest.mark.asyncio
async def test_html_healthcarebluebook_resolver_combines_direct_and_delegated_links(
    monkeypatch,
):
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "BRMS",
    }
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

    crawl_targets = await discovery._resolve_html_mrf_with_healthcarebluebook(
        source_dict,
        "https://www.myhealthbenefits.com/MyHealthBenefits/Home/MRFs/",
        {"type": "html_mrf_with_healthcarebluebook"},
        None,
    )

    by_url = {crawl_target.url: crawl_target for crawl_target in crawl_targets}
    assert by_url[
        "https://www.myhealthbenefits.com/MRF/2026-06-04_ClaimDOC_BRMS_index.json"
    ].metadata["target_file_type"] == ("table-of-contents")
    csv_target = by_url[
        "https://www.myhealthbenefits.com/MRF/2026-06-01_BRMS_allowed-amounts.csv"
    ]
    assert csv_target.metadata["target_file_type"] == "allowed-amounts"
    assert csv_target.metadata["source_format"] == "csv"
    delegated = by_url["https://mrf.healthcarebluebook.com/BRMS/314355"]
    assert delegated.metadata["resolver"] == "html_mrf_with_healthcarebluebook"
    assert delegated.metadata["nested_resolver"] == "healthcarebluebook_mrf"
    assert delegated.metadata["plan_info"][0]["plan_id"] == "030506501"


@pytest.mark.asyncio
async def test_html_healthcarebluebook_resolver_passes_max_targets_to_nested(
    monkeypatch,
):
    source_dict = {"source_id": "source_1", "payer_id": "payer_1", "display_name": "Lucent"}
    captured_resolvers = []

    async def fake_fetch_text(url, **_kwargs):
        assert url == "https://lucenthealth.com/transparency-in-coverage/"
        return '<a href="https://mrf.healthcarebluebook.com/Lucent">View INN MRFs</a>'

    async def fake_resolve_healthcarebluebook_mrf(
        nested_source, link_url, nested_resolver, _session
    ):
        captured_resolvers.append(dict(nested_resolver))
        return [
            discovery.CrawlTarget(
                source=nested_source,
                url="https://mrf.healthcarebluebook.com/Lucent/350504",
                label="Lucent Health",
                resolved_from_url=link_url,
                metadata={"resolver": "healthcarebluebook_mrf"},
            )
        ]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(
        discovery,
        "_resolve_healthcarebluebook_mrf",
        fake_resolve_healthcarebluebook_mrf,
    )

    crawl_targets = await discovery._resolve_html_mrf_with_healthcarebluebook(
        source_dict,
        "https://lucenthealth.com/transparency-in-coverage/",
        {"type": "html_mrf_with_healthcarebluebook", "max_targets": 5},
        None,
    )

    assert len(crawl_targets) in {1}
    assert captured_resolvers[0]["max_targets"] == 5


@pytest.mark.asyncio
async def test_html_bluebook_direct_nested_error(
    monkeypatch,
):
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Admin",
    }

    async def fake_fetch_text(url, **_kwargs):
        assert url == "https://example-admin.test/mrfs/"
        return """
          <a href="https://cdn.example.test/2026-06-01_example_index.json">Index</a>
          <a href="https://mrf.healthcarebluebook.com/ExampleAdmin">Delegated</a>
        """

    async def fake_resolve_healthcarebluebook_mrf(*_args, **_kwargs):
        raise ValueError("nested source unavailable")

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(
        discovery,
        "_resolve_healthcarebluebook_mrf",
        fake_resolve_healthcarebluebook_mrf,
    )

    resolved_targets = await discovery._resolve_html_mrf_with_healthcarebluebook(
        source_dict,
        "https://example-admin.test/mrfs/",
        {"type": "html_mrf_with_healthcarebluebook"},
        None,
    )

    assert [crawl_target.url for crawl_target in resolved_targets] == [
        "https://cdn.example.test/2026-06-01_example_index.json"
    ]
    assert resolved_targets[0].metadata["target_file_type"] == "table-of-contents"


@pytest.mark.asyncio
async def test_socrata_data_json_resolver_discovers_latest_vhp_mrf_files(monkeypatch):
    """Verify this source-discovery regression contract."""
    catalog_source_dict = {
        "source_id": "source_vhp",
        "payer_id": "payer_vhp",
        "display_name": "Valley Health Plan",
        "metadata_json": {"benefit_lines": ["medical", "dental", "vision"]},
    }
    catalog_response_dict = {
        "dataset": [
            {
                "accessLevel": "public",
                "title": "In Network Rates - Individual and Family Plans - Gold 80 - Santa Clara County - June 2026",
                "description": "Machine-readable file that contains In-network rates.",
                "issued": "2026-05-20",
                "modified": "2026-05-20",
                "contactPoint": {"fn": "Ethan Giang"},
                "keyword": ["price transparency"],
                "identifier": "https://data.sccgov.org/api/views/qq69-6225",
                "landingPage": "https://data.sccgov.org/d/qq69-6225",
                "distribution": [
                    {
                        "downloadURL": "https://data.sccgov.org/download/qq69-6225/application/vnd.geo+json",
                        "mediaType": "application/vnd.geo+json",
                    }
                ],
            },
            {
                "accessLevel": "public",
                "title": "In Network Rates - Pediatric Dental - Covered California and IFP - Santa Clara County - June 2026",
                "description": "Machine-readable file that contains In-network rates.",
                "issued": "2026-05-20",
                "modified": "2026-05-20",
                "contactPoint": {"fn": "ryan.aralar@vhp.sccgov.org"},
                "keyword": ["price transparency"],
                "identifier": "https://data.sccgov.org/api/views/rj4i-khih",
                "landingPage": "https://data.sccgov.org/d/rj4i-khih",
                "distribution": [
                    {
                        "downloadURL": "https://data.sccgov.org/download/rj4i-khih/application/vnd.geo+json",
                        "mediaType": "application/vnd.geo+json",
                    }
                ],
            },
            {
                "accessLevel": "public",
                "title": "In Network Rates - VSP Vision Care Advantage - June 2026",
                "description": "Machine-readable file that contains In-network rates.",
                "issued": "2026-06-05",
                "modified": "2026-06-05",
                "contactPoint": {"fn": "Ethan Giang"},
                "keyword": ["price transparency"],
                "identifier": "https://data.sccgov.org/api/views/bxvw-whxu",
                "landingPage": "https://data.sccgov.org/d/bxvw-whxu",
                "distribution": [
                    {
                        "downloadURL": "https://data.sccgov.org/download/bxvw-whxu/application/vnd.geo+json",
                        "mediaType": "application/vnd.geo+json",
                    }
                ],
            },
            {
                "accessLevel": "public",
                "title": "Out-of-Network Allowed Amounts - Covered California - Gold 80 - June 2026",
                "description": "Allowed amounts paid to providers outside of the VHP network.",
                "issued": "2026-05-20",
                "modified": "2026-05-20",
                "contactPoint": {"fn": "ryan.aralar@vhp.sccgov.org"},
                "keyword": ["price transparency"],
                "identifier": "https://data.sccgov.org/api/views/d6r6-tdzg",
                "landingPage": "https://data.sccgov.org/d/d6r6-tdzg",
                "distribution": [
                    {
                        "downloadURL": "https://data.sccgov.org/download/d6r6-tdzg/application/vnd.geo+json",
                        "mediaType": "application/vnd.geo+json",
                    }
                ],
            },
            {
                "accessLevel": "public",
                "title": "In Network Rates - VSP Vision Care Advantage - May 2026",
                "description": "Machine-readable file that contains In-network rates.",
                "issued": "2026-04-28",
                "contactPoint": {"fn": "Ethan Giang"},
                "keyword": ["price transparency"],
                "identifier": "https://data.sccgov.org/api/views/mhey-u94c",
                "distribution": [
                    {
                        "downloadURL": "https://data.sccgov.org/download/mhey-u94c/application/vnd.geo+json",
                        "mediaType": "application/vnd.geo+json",
                    }
                ],
            },
            {
                "accessLevel": "public",
                "title": "County budget rows",
                "description": "Not a machine-readable rate file.",
                "contactPoint": {"fn": "County"},
                "keyword": ["finance"],
                "distribution": [
                    {
                        "downloadURL": "https://data.sccgov.org/download/abcd-1234/application/json",
                        "mediaType": "application/json",
                    }
                ],
            },
        ]
    }

    async def fake_fetch_json(url, **_kwargs):
        assert url == "https://data.sccgov.org/data.json"
        return catalog_response_dict

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    dataset_targets = await discovery._resolve_socrata_data_json_mrf_catalog(
        catalog_source_dict,
        "https://data.sccgov.org/data.json",
        {
            "type": "socrata_data_json_mrf_catalog",
            "title_regex": "(?i)(in\\s+network\\s+rates|allowed\\s+amounts)",
            "contact_regex": "(?i)(vhp|ryan\\.aralar|ethan\\s+giang)",
            "keyword_any": ["price transparency"],
            "latest_coverage_month_only": True,
        },
        None,
    )

    by_id = {
        dataset_target.metadata["socrata_dataset_id"]: dataset_target
        for dataset_target in dataset_targets
    }
    assert set(by_id) == {"qq69-6225", "rj4i-khih", "bxvw-whxu", "d6r6-tdzg"}
    assert by_id["qq69-6225"].metadata["target_file_type"] == "in-network"
    assert by_id["qq69-6225"].metadata["benefit_line"] == "medical"
    assert by_id["rj4i-khih"].metadata["benefit_line"] == "dental"
    assert by_id["bxvw-whxu"].metadata["benefit_line"] == "vision"
    assert by_id["d6r6-tdzg"].metadata["target_file_type"] == "allowed-amounts"
    assert by_id["d6r6-tdzg"].metadata["socrata_coverage_month"] == "2026-06"
    assert by_id["qq69-6225"].metadata["plan_info"] == [
        {
            "plan_id": "qq69-6225",
            "plan_id_type": "socrata_view_id",
            "plan_market_type": "individual",
            "plan_name": "In Network Rates - Individual and Family Plans - Gold 80 - Santa Clara County - June 2026",
        }
    ]


def test_asr_health_benefits_resolver_expands_configured_group_numbers():
    catalog_source_dict = {"source_id": "source_1", "display_name": "ASR Health Benefits"}
    resolver_config_dict = {
        "type": "asr_health_benefits_mrf",
        "toc_path": "/umbraco/surface/mrfdownload",
        "group_numbers": ["1208"],
    }

    [target] = discovery._resolve_asr_health_benefits_mrf(
        catalog_source_dict, "https://www.asrhealthbenefits.com/MRF", resolver_config_dict
    )

    assert (
        target.url
        == "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber=1208"
    )
    assert target.label == "ASR Health Benefits group 1208"
    assert target.resolved_from_url == "https://www.asrhealthbenefits.com/MRF"
    assert target.metadata["resolver"] == "asr_health_benefits_mrf"
    assert target.metadata["group_number"] == "1208"


def test_asr_health_benefits_resolver_preserves_direct_group_number():
    catalog_source_dict = {"source_id": "source_1", "display_name": "ASR Health Benefits"}
    resolver_config_dict = {
        "type": "asr_health_benefits_mrf",
        "toc_path": "/umbraco/surface/mrfdownload",
        "group_numbers": ["1208"],
    }

    targets = discovery._resolve_asr_health_benefits_mrf(
        catalog_source_dict,
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber=1194",
        resolver_config_dict,
    )

    assert [target.metadata["group_number"] for target in targets] == ["1194", "1208"]


def test_asr_health_benefits_resolver_uses_seed_list():
    source_dict = {"source_id": "source_1", "display_name": "ASR Health Benefits"}
    resolver = discovery._source_config()["platform_resolvers"]["asr_health_benefits"]
    expected_groups = discovery._asr_group_numbers_from_seed_list(resolver["seed_list"])

    targets = discovery._resolve_asr_health_benefits_mrf(
        source_dict, "https://www.asrhealthbenefits.com/MRF", resolver
    )

    assert {"1194", "1208"}.issubset(set(expected_groups))
    assert [target.metadata["group_number"] for target in targets] == expected_groups
    assert targets[0].url.startswith(
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber="
    )


def test_asr_health_benefits_target_context_applies_to_file_rows():
    target = discovery.CrawlTarget(
        source={"source_id": "source_1", "display_name": "ASR Health Benefits"},
        url="https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber=1208",
        label="ASR Health Benefits group 1208",
        resolved_from_url="https://www.asrhealthbenefits.com/MRF",
        metadata={"resolver": "asr_health_benefits_mrf", "group_number": "1208"},
    )
    rows = [{"mrf_file_id": "file_1", "metadata_json": {"container_format": None}}]

    [row] = discovery._apply_crawl_target_context_to_file_rows(rows, target)

    assert row["metadata_json"]["group_id"] == "1208"
    assert row["metadata_json"]["group_number"] == "1208"
    assert row["metadata_json"]["target_label"] == "ASR Health Benefits group 1208"


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
    assert discovery._asr_group_numbers_from_seed_list("asr_test") == ["1194", "1210"]


def test_asr_health_benefits_seed_metadata_becomes_target_context(
    tmp_path, monkeypatch
):
    seed_path = tmp_path / "asr-groups.csv"
    seed_path.write_text(
        "group_number,status,company_name,employer_name,plan_name,evidence_url\n"
        "1208,active,Example Forge LLC,Example Forge LLC,Example Forge ASR Plan,https://example.test/asr-1208\n",
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
    catalog_source_dict = {"source_id": "source_1", "display_name": "ASR Health Benefits"}
    resolver_config_dict = {
        "type": "asr_health_benefits_mrf",
        "toc_path": "/umbraco/surface/mrfdownload",
        "seed_list": "asr_test",
    }

    [group_target] = discovery._resolve_asr_health_benefits_mrf(
        catalog_source_dict, "https://www.asrhealthbenefits.com/MRF", resolver_config_dict
    )
    [contextualized_file_row] = discovery._apply_crawl_target_context_to_file_rows(
        [{"mrf_file_id": "file_1", "metadata_json": {}}],
        group_target,
    )

    assert group_target.label == "ASR Health Benefits group 1208 - Example Forge LLC"
    assert group_target.metadata["company_name"] == "Example Forge LLC"
    assert group_target.metadata["plan_name"] == "Example Forge ASR Plan"
    assert group_target.metadata["evidence_url"] == "https://example.test/asr-1208"
    assert contextualized_file_row["metadata_json"]["group_number"] == "1208"
    assert contextualized_file_row["metadata_json"]["company_name"] == "Example Forge LLC"
    assert contextualized_file_row["metadata_json"]["employer_name"] == "Example Forge LLC"
    assert contextualized_file_row["metadata_json"]["plan_name"] == "Example Forge ASR Plan"
    assert (
        contextualized_file_row["metadata_json"]["target_label"]
        == "ASR Health Benefits group 1208 - Example Forge LLC"
    )


def test_private_seed_context_overlays_public_seed_rows(tmp_path, monkeypatch):
    public_seed_path = tmp_path / "groups.csv"
    public_seed_path.write_text(
        "group_number,status,company_name\n1000,active,Public Label\n",
        encoding="utf-8",
    )
    private_seed_path = tmp_path / "private-groups.csv"
    private_seed_path.write_text(
        (
            "seed_list,group_number,status,company_name\n"
            "synthetic,1000,active,Private Label\n"
            "synthetic,1001,active,Added Label\n"
            "unrelated,1002,active,Ignored Label\n"
        ),
        encoding="utf-8",
    )
    source_config_path = tmp_path / "sources.json"
    source_config_path.write_text(
        json.dumps(
            {
                "providers": {},
                "seed_lists": {
                    "synthetic": {
                        "schema": "group_number_seed_v1",
                        "path": str(public_seed_path),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(discovery.SOURCE_CONFIG_ENV, str(source_config_path))
    monkeypatch.setenv(
        discovery.PRIVATE_SEED_CONTEXT_PATHS_ENV, str(private_seed_path)
    )

    merged_rows = discovery._load_seed_list_rows("synthetic")

    assert [seed_row["group_number"] for seed_row in merged_rows] == ["1000", "1001"]
    assert [seed_row["company_name"] for seed_row in merged_rows] == [
        "Private Label",
        "Added Label",
    ]


def test_asr_health_benefits_seed_list_dedupes_direct_and_configured_numbers(
    tmp_path, monkeypatch
):
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
    resolver_dict = {
        "type": "asr_health_benefits_mrf",
        "toc_path": "/umbraco/surface/mrfdownload",
        "seed_list": "asr_test",
        "group_numbers": ["1208"],
    }

    assert discovery._asr_group_numbers_for_source(
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber=1194",
        resolver_dict,
    ) == ["1194", "1208"]


def test_asr_health_benefits_seed_list_rejects_non_four_digit_values(
    tmp_path, monkeypatch
):
    seed_path = tmp_path / "asr-groups.csv"
    seed_path.write_text(
        "group_number,status\n1208,active\n12,active\n", encoding="utf-8"
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
        {
            "url": "https://transparency.auxiant.com/aetna/",
            "label": "Aetna",
            "data_available": True,
        },
        {
            "url": "https://transparency.auxiant.com/healthsmart/",
            "label": "HealthSmart",
            "data_available": True,
        },
        {
            "url": "https://transparency.auxiant.com/first-choice-health/",
            "label": "First Choice Health",
            "data_available": True,
        },
    ]


def test_auxiant_page_link_parser_extracts_external_and_direct_files():
    """Verify Auxiant pages yield delegated and directly hosted MRF targets."""
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
    links = discovery._parse_auxiant_page_links(
        html, base_url="https://transparency.auxiant.com/healthsmart/"
    )

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
    assert (
        discovery._auxiant_file_type(
            "https://example.com/MPI_HST_allowedamounts_20220901.zip"
        )
        == "allowed-amounts"
    )


def test_auxiant_direct_target_keeps_network_context_searchable():
    source_dict = {"source_id": "source_auxiant", "payer_id": "payer_auxiant"}
    link_dict = {
        "url": "https://s3.us-east-2.amazonaws.com/transparency.auxiant.com/FirstChoiceHealth/20250707-innrfppog07072025.zip",
        "label": "20250707-innrfppog07072025.zip",
        "target_file_type": "in-network",
        "container_format": "zip",
    }

    target = discovery._auxiant_direct_target(
        source_dict,
        link_dict,
        network_name="First Choice Health",
        page_url="https://transparency.auxiant.com/first-choice-health/",
        directory_url="https://transparency.auxiant.com/directory-of-data-sources/",
        resolver_type="auxiant_wordpress_directory",
    )

    assert target.label == "Auxiant - First Choice Health"
    assert (
        target.resolved_from_url
        == "https://transparency.auxiant.com/first-choice-health/"
    )
    assert target.metadata["resolver"] == "auxiant_wordpress_directory"
    assert target.metadata["target_kind"] == "file_reference"
    assert target.metadata["auxiant_network_name"] == "First Choice Health"
    assert target.metadata["file_label"] == "20250707-innrfppog07072025.zip"


def test_auxiant_landing_target_indexes_unresolved_network_pages():
    source_dict = {"source_id": "source_auxiant", "payer_id": "payer_auxiant"}

    target = discovery._auxiant_landing_target(
        source_dict,
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
    assert (
        discovery._mymedicalshopper_entity_slug_from_url(
            "https://www.mymedicalshopper.com/mrf-search/varipro"
        )
        == "varipro"
    )
    assert (
        discovery._mymedicalshopper_employer_slug_from_url(
            "https://www.mymedicalshopper.com/mrf/sample-employer-network-varipro-77100"
        )
        == "sample-employer-network-varipro-77100"
    )
    assert discovery._mymedicalshopper_employer_selector(
        "varipro", all_employers_searchable=True
    ) == {
        "tpaSlug": "varipro",
        "status": "Enabled",
    }
    assert discovery._mymedicalshopper_employer_selector(
        "varipro", all_employers_searchable=False
    ) == {
        "tpaSlug": "varipro",
        "status": "Enabled",
        "machineReadableFiles.makeMRFsSearchable": True,
    }


def test_mymedicalshopper_query_expansion_uses_employer_search_selector():
    catalog_source_dict = {
        "metadata_json": {
            "raw": {
                "target_payer_query": "Example Packaging, Inc.",
                "query_expansion_source": True,
            }
        }
    }
    base_selector_dict = {"tpaSlug": "example-tpa", "status": "Enabled"}
    expected_search_selector_dict = {
        "$and": [
            base_selector_dict,
            {
                "$or": [
                    {"name": {"$regex": "example.*packaging", "$options": "i"}},
                    {"slug": {"$regex": "example.*packaging", "$options": "i"}},
                ]
            },
        ]
    }

    assert (
        discovery._mymedicalshopper_employer_search_selector(
            base_selector_dict, "Example Packaging, Inc."
        )
        == expected_search_selector_dict
    )
    assert discovery._mymedicalshopper_entity_employer_selectors(
        "example-tpa",
        all_employers_searchable=True,
        source=catalog_source_dict,
        resolver={},
    ) == [expected_search_selector_dict]
    assert discovery._mymedicalshopper_entity_employer_selectors(
        "example-tpa",
        all_employers_searchable=True,
        source=catalog_source_dict,
        resolver={"query_search_include_full_table": True},
    ) == [expected_search_selector_dict, base_selector_dict]
    assert discovery._mymedicalshopper_entity_employer_selectors(
        "example-tpa",
        all_employers_searchable=True,
        source={},
        resolver={},
    ) == [base_selector_dict]


def test_mymedicalshopper_sockjs_frame_and_publication_helpers():
    frame = "a" + json.dumps(
        [
            json.dumps(
                {
                    "msg": "added",
                    "collection": "tabular_records",
                    "id": "EntityMRFEmployers",
                    "fields": {
                        "ids": [{"$type": "oid", "$value": "61a"}],
                        "recordsTotal": 5,
                        "recordsFiltered": 1,
                    },
                }
            ),
            json.dumps(
                {
                    "msg": "added",
                    "collection": "employers",
                    "id": "61a",
                    "fields": {
                        "name": "Sample Employer - Network A",
                        "slug": "sample-employer-network-varipro-77100",
                        "tpaSlug": "varipro",
                        "status": "Enabled",
                    },
                }
            ),
        ]
    )

    messages = discovery._mymedicalshopper_sockjs_messages(frame)
    info = discovery._mymedicalshopper_tabular_info_from_messages(messages)
    employers = discovery._mymedicalshopper_employer_docs_from_messages(messages)

    assert info["ids"] == [{"$type": "oid", "$value": "61a"}]
    assert info["records_filtered"] in {1}
    assert employers == [
        {
            "_id": "61a",
            "name": "Sample Employer - Network A",
            "slug": "sample-employer-network-varipro-77100",
            "tpaSlug": "varipro",
            "status": "Enabled",
        }
    ]


def test_mymedicalshopper_targets_keep_latest_generated_toc_per_plan():
    """Verify this source-discovery regression contract."""
    catalog_source_dict = {"source_id": "source_varipro", "payer_id": "payer_varipro"}
    employer_dict = {
        "slug": "sample-employer-network-varipro-77100",
        "name": "Sample Employer - Network A",
        "tpaSlug": "varipro",
        "groupId": "77100",
        "ein": "111222333",
    }
    generated_plan_records = [
        {
            "planId": "4907",
            "planName": "Sample Employer In Network 01/01/2023",
            "mrfGeneratedInfo": [
                {
                    "month": "2026-05-01",
                    "mrfGenerated": True,
                    "link": "https://mrf.mmsanalytics.com/2026-05-01_sample_employer_index.json",
                },
                {
                    "month": "2026-06-01",
                    "mrfGenerated": True,
                    "link": "https://mrf.mmsanalytics.com/2026-06-01_sample_employer_index.json",
                },
                {
                    "month": "2026-07-01",
                    "mrfGenerated": False,
                    "link": "https://mrf.mmsanalytics.com/2026-07-01_sample_employer_index.json",
                },
            ],
        },
        {
            "plan": {"id": "4907", "name": "Sample Employer In Network 01/01/2022"},
            "mrfGeneratedInfo": [
                {
                    "month": "2026-06-01",
                    "mrfGenerated": True,
                    "link": "https://mrf.mmsanalytics.com/2026-06-01_sample_employer_2022_index.json",
                }
            ],
        },
    ]

    toc_targets = discovery._mymedicalshopper_targets_from_generated(
        catalog_source_dict,
        entity_slug="varipro",
        employer=employer_dict,
        generated=generated_plan_records,
        resolver_type="mymedicalshopper_talon_mrf",
        resolved_from_url="https://www.mymedicalshopper.com/mrf-search/varipro",
    )

    assert [toc_target.url for toc_target in toc_targets] == [
        "https://mrf.mmsanalytics.com/2026-06-01_sample_employer_index.json",
        "https://mrf.mmsanalytics.com/2026-06-01_sample_employer_2022_index.json",
    ]
    assert (
        toc_targets[0].label
        == "Sample Employer - Network A - Sample Employer In Network 01/01/2023 - 2026-06-01"
    )
    assert toc_targets[0].metadata["target_file_type"] == "table-of-contents"
    assert toc_targets[0].metadata["entity_slug"] == "varipro"
    assert toc_targets[0].metadata["tpa_slug"] == "varipro"
    assert toc_targets[0].metadata["tpa_name"] == "Varipro"
    assert toc_targets[0].metadata["client_id"] is None
    assert toc_targets[0].metadata["client_name"] == "Sample Employer - Network A"
    assert (
        toc_targets[0].metadata["employer_slug"]
        == "sample-employer-network-varipro-77100"
    )
    assert toc_targets[0].metadata["employer_name"] == "Sample Employer - Network A"
    assert toc_targets[0].metadata["group_id"] == "77100"
    assert toc_targets[0].metadata["group_number"] == "77100"
    assert toc_targets[0].metadata["ein"] == "111222333"
    assert toc_targets[0].metadata["history_month_count"] == 3
    context = discovery._crawl_target_context_metadata(toc_targets[0])
    assert context["client_name"] == "Sample Employer - Network A"
    assert context["tpa_slug"] == "varipro"
    assert context["group_number"] == "77100"


class _MMSHeartbeatOnlyWebSocket:
    def __init__(self):
        self.sent = []

    async def send_str(self, payload):
        self.sent.append(payload)

    async def receive(self):
        await discovery.asyncio.sleep(0)
        return types.SimpleNamespace(
            type=discovery.aiohttp.WSMsgType.TEXT,
            data='a["{\\"msg\\":\\"ping\\",\\"id\\":\\"heartbeat\\"}"]',
        )


def _mms_sent_messages(ws):
    messages = []
    for payload in ws.sent:
        for item in json.loads(payload):
            messages.append(json.loads(item))
    return messages


@pytest.mark.asyncio
async def test_mymedicalshopper_ddp_call_uses_overall_deadline_for_heartbeats():
    ws = _MMSHeartbeatOnlyWebSocket()

    with pytest.raises(TimeoutError, match="method getBenefitPlans timed out"):
        await discovery._mymedicalshopper_ddp_call(
            ws,
            method="getBenefitPlans",
            params=[{"employerSlug": "slow-source"}],
            request_id="mms-plans-slow-source",
            timeout_seconds=0.001,
        )

    assert ws.sent
    assert any(message.get("msg") == "pong" for message in _mms_sent_messages(ws))


@pytest.mark.asyncio
async def test_mymedicalshopper_subscription_uses_overall_deadline_for_heartbeats():
    ws = _MMSHeartbeatOnlyWebSocket()

    with pytest.raises(TimeoutError):
        await discovery._mymedicalshopper_ddp_subscribe_collect(
            ws,
            name="tabular_getInfo",
            params=["EntityMRFEmployers", {}, [["name", "asc"]], 0, 20],
            sub_id="mms-info-slow-source",
            timeout_seconds=0.001,
        )

    assert ws.sent
    assert any(message.get("msg") == "pong" for message in _mms_sent_messages(ws))


@pytest.mark.asyncio
async def test_mymedicalshopper_entity_employers_searches_target_query(monkeypatch):
    """Verify this source-discovery regression contract."""
    calls = []

    async def fake_subscribe_collect(
        _ws, *, name, params, sub_id, timeout_seconds
    ):
        calls.append(
            {
                "name": name,
                "params": params,
                "sub_id": sub_id,
                "timeout_seconds": timeout_seconds,
            }
        )
        if name == "entityMRFsConfig":
            return [
                {
                    "collection": "thirdPartyAdministrators",
                    "fields": {
                        "slug": "example-tpa",
                        "name": "Example TPA",
                        "machineReadableFiles": {"allEmployersSearchable": True},
                    },
                }
            ]
        if name == "tabular_getInfo":
            return [
                {
                    "collection": "tabular_records",
                    "id": "EntityMRFEmployers",
                    "fields": {
                        "ids": [{"$type": "oid", "$value": "61a"}],
                        "recordsTotal": 1,
                        "recordsFiltered": 1,
                    },
                }
            ]
        if name == "entityMRFEmployers":
            return [
                {
                    "msg": "added",
                    "collection": "employers",
                    "id": "61a",
                    "fields": {
                        "name": "Example Packaging Choice",
                        "slug": "example-packaging-choice-example-tpa-10001",
                        "status": "Enabled",
                    },
                },
            ]
        raise AssertionError(f"unexpected subscription: {name}")

    monkeypatch.setattr(
        discovery, "_mymedicalshopper_ddp_subscribe_collect", fake_subscribe_collect
    )
    source_dict = {
        "metadata_json": {
            "raw": {
                "target_payer_query": "Example Packaging Inc",
                "query_expansion_source": True,
            }
        }
    }

    employers = await discovery._mymedicalshopper_entity_employers(
        object(),
        source_record=source_dict,
        entity_slug="example-tpa",
        resolver={"page_size": 20},
        timeout_seconds=5,
    )

    assert employers == [
        {
            "_id": "61a",
            "name": "Example Packaging Choice",
            "slug": "example-packaging-choice-example-tpa-10001",
            "status": "Enabled",
            "tpaSlug": "example-tpa",
            "tpaName": "Example TPA",
        }
    ]
    info_call_by_name = next(
        call for call in calls if call["name"] == "tabular_getInfo"
    )
    assert info_call_by_name["params"][1] == {
        "$and": [
            {"tpaSlug": "example-tpa", "status": "Enabled"},
            {
                "$or": [
                    {"name": {"$regex": "example.*packaging", "$options": "i"}},
                    {"slug": {"$regex": "example.*packaging", "$options": "i"}},
                ]
            },
        ]
    }


@pytest.mark.asyncio
async def test_mms_primary_search_query_passes_local_filter(monkeypatch):
    async def fake_subscribe_collect(_ws, *, name, **_kwargs):
        assert name == "entityMRFsConfig"
        return _mms_fallback_config_messages()

    seen_queries = []

    async def fake_employers_for_query(_ws, employer_query):
        seen_queries.append(employer_query.query_filter)
        return {
            "example-packaging-example-tpa-10001": {
                "slug": "example-packaging-example-tpa-10001",
                "name": "Example Packaging",
            }
        }

    monkeypatch.setattr(
        discovery, "_mymedicalshopper_ddp_subscribe_collect", fake_subscribe_collect
    )
    monkeypatch.setattr(
        discovery,
        "_mymedicalshopper_entity_employers_for_query",
        fake_employers_for_query,
    )

    employers = await discovery._mymedicalshopper_entity_employers(
        object(),
        source_record={
            "metadata_json": {
                "raw": {
                    "target_payer_query": "Example Packaging",
                    "query_expansion_source": True,
                }
            }
        },
        entity_slug="example-tpa",
        resolver={},
        timeout_seconds=5,
    )

    assert seen_queries == ["Example Packaging"]
    assert employers[0]["slug"] == "example-packaging-example-tpa-10001"


def _mms_fallback_config_messages():
    return [
        {
            "collection": "thirdPartyAdministrators",
            "fields": {
                "slug": "example-tpa",
                "name": "Example TPA",
                "machineReadableFiles": {"allEmployersSearchable": True},
            },
        }
    ]


def _mms_fallback_table_messages(selector):
    if "$and" in selector:
        return [
            {
                "collection": "tabular_records",
                "id": "EntityMRFEmployers",
                "fields": {"ids": [], "recordsTotal": 2, "recordsFiltered": 0},
            }
        ]
    return [
        {
            "collection": "tabular_records",
            "id": "EntityMRFEmployers",
            "fields": {
                "ids": [
                    {"$type": "oid", "$value": "61a"},
                    {"$type": "oid", "$value": "61b"},
                ],
                "recordsTotal": 2,
                "recordsFiltered": 2,
            },
        }
    ]


def _mms_fallback_employer_messages():
    return [
        {
            "msg": "added",
            "collection": "employers",
            "id": "61a",
            "fields": {
                "name": "Example Packaging",
                "slug": "example-packaging-example-tpa-10001",
                "status": "Enabled",
            },
        },
        {
            "msg": "added",
            "collection": "employers",
            "id": "61b",
            "fields": {
                "name": "Example Forge",
                "slug": "example-forge-example-tpa-10002",
                "status": "Enabled",
            },
        },
    ]


def _mms_fallback_messages(name, params):
    if name == "entityMRFsConfig":
        return _mms_fallback_config_messages()
    if name == "tabular_getInfo":
        return _mms_fallback_table_messages(params[1])
    if name == "entityMRFEmployers":
        return _mms_fallback_employer_messages()
    raise AssertionError(f"unexpected subscription: {name}")


@pytest.mark.asyncio
async def test_mms_query_fallback_filters_employers(monkeypatch):
    calls = []

    async def fake_subscribe_collect(
        _ws, *, name, params, sub_id, timeout_seconds
    ):
        calls.append(
            {
                "name": name,
                "params": params,
                "sub_id": sub_id,
                "timeout_seconds": timeout_seconds,
            }
        )
        return _mms_fallback_messages(name, params)

    monkeypatch.setattr(
        discovery, "_mymedicalshopper_ddp_subscribe_collect", fake_subscribe_collect
    )
    source_payload_dict = {
        "metadata_json": {
            "raw": {
                "target_payer_query": "Example Forge",
                "query_expansion_source": True,
            }
        }
    }

    employers = await discovery._mymedicalshopper_entity_employers(
        object(),
        source_record=source_payload_dict,
        entity_slug="example-tpa",
        resolver={
            "page_size": 2,
            "max_employers": 10,
            "query_search_fallback_max_employers": 3,
        },
        timeout_seconds=5,
    )

    assert employers == [
        {
            "_id": "61b",
            "name": "Example Forge",
            "slug": "example-forge-example-tpa-10002",
            "status": "Enabled",
            "tpaSlug": "example-tpa",
            "tpaName": "Example TPA",
        }
    ]
    info_selectors = [
        call["params"][1] for call in calls if call["name"] == "tabular_getInfo"
    ]
    assert "$and" in info_selectors[0]
    assert info_selectors[1] == {"tpaSlug": "example-tpa", "status": "Enabled"}


def test_mymedicalshopper_direct_employer_slug_infers_tpa_and_group_context():
    assert (
        discovery._mymedicalshopper_group_id_from_employer_slug(
            "sample-employer-network-varipro-77100"
        )
        == "77100"
    )
    assert (
        discovery._mymedicalshopper_group_id_from_employer_slug(
            "a-plus-portable-restrooms-viva-health-x00977"
        )
        == "x00977"
    )
    assert (
        discovery._mymedicalshopper_tpa_slug_from_employer_slug(
            "sample-employer-network-varipro-77100"
        )
        == "varipro"
    )


@pytest.mark.asyncio
async def test_mymedicalshopper_resolver_honors_max_targets(monkeypatch):
    generated_employer_slugs = []

    class FakeWS:
        async def close(self):
            return None

    async def fake_connect(_session, _url, *, timeout_seconds):
        assert timeout_seconds == 30
        return FakeWS()

    async def fake_entity_employers(
        _ws, *, source_record, entity_slug, resolver, timeout_seconds
    ):
        assert source_record["source_id"] == "source_bywater"
        assert entity_slug == "bywater"
        assert timeout_seconds == 30
        assert resolver["max_targets"] == 2
        return [
            {"slug": "client-one-bywater-10001", "name": "Client One"},
            {"slug": "client-two-bywater-10002", "name": "Client Two"},
            {"slug": "client-three-bywater-10003", "name": "Client Three"},
        ]

    async def fake_generated_for_employer(_ws, *, employer_slug, **_kwargs):
        generated_employer_slugs.append(employer_slug)
        return [
            {
                "planId": employer_slug,
                "planName": employer_slug,
                "mrfGeneratedInfo": [
                    {
                        "month": "2026-06-01",
                        "mrfGenerated": True,
                        "link": f"https://mrf.mmsanalytics.com/{employer_slug}_index.json",
                    }
                ],
            }
        ]

    monkeypatch.setattr(discovery, "_mymedicalshopper_ddp_connect", fake_connect)
    monkeypatch.setattr(
        discovery, "_mymedicalshopper_entity_employers", fake_entity_employers
    )
    monkeypatch.setattr(
        discovery,
        "_mymedicalshopper_generated_for_employer",
        fake_generated_for_employer,
    )

    employer_targets = await discovery._resolve_mymedicalshopper_talon_mrf(
        {"source_id": "source_bywater", "payer_id": "payer_bywater"},
        "https://www.mymedicalshopper.com/mrf-search/bywater",
        {"type": "mymedicalshopper_talon_mrf", "max_targets": 2},
        session=object(),
    )

    assert generated_employer_slugs == ["client-one-bywater-10001", "client-two-bywater-10002"]
    assert [employer_target.metadata["employer_slug"] for employer_target in employer_targets] == generated_employer_slugs


@pytest.mark.asyncio
async def test_viva_health_resolver_adds_commercial_and_employer_landing_targets(
    monkeypatch,
):
    employer_call_by_field = {}

    async def fake_employer_targets(source, *, employer_page_url, resolver, session):
        employer_call_by_field["source"] = source
        employer_call_by_field["url"] = employer_page_url
        employer_call_by_field["resolver"] = resolver
        employer_call_by_field["session"] = session
        return [
            discovery._viva_health_employer_landing_target(
                source=source,
                employer_url="https://www.mymedicalshopper.com/mrf/viva-client-x01234",
                employer_page_url=employer_page_url,
            )
        ]

    monkeypatch.setattr(
        discovery, "_collect_viva_health_employer_landing_targets", fake_employer_targets
    )

    catalog_source_dict = {"source_id": "source_viva", "payer_id": "payer_viva"}
    resolver_config_dict = {
        "type": "viva_health_mrf",
        "employer_path": "/mrf/employers/",
        "max_bytes": 1024,
        "max_employer_links": 7,
        "max_targets": 10,
    }
    discovery_targets = await discovery._resolve_viva_health_mrf(
        catalog_source_dict,
        "https://www.vivahealth.com/mrf/",
        resolver_config_dict,
        session=object(),
    )

    assert [crawl_target.url for crawl_target in discovery_targets[:2]] == [
        "https://www.vivahealth.com/files/mrf/viva-health-commercial-in-network-rates",
        "https://www.vivahealth.com/files/mrf/viva-health-commercial-out-of-network-rates",
    ]
    assert discovery_targets[0].metadata["target_file_type"] == "in-network"
    assert discovery_targets[0].metadata["container_format"] == "zip"
    assert discovery_targets[1].metadata["target_file_type"] == "allowed-amounts"
    assert discovery_targets[2].metadata["target_kind"] == "source_landing_page"
    assert discovery_targets[2].metadata["target_file_type"] == "source-landing-page"
    assert discovery_targets[2].metadata["external_hosting_platform"] == "mymedicalshopper_talon"
    assert discovery_targets[2].metadata["group_id"] == "x01234"
    assert (
        discovery_targets[2].metadata["viva_employer_page_url"]
        == "https://www.vivahealth.com/mrf/employers/"
    )
    assert employer_call_by_field["url"] == "https://www.vivahealth.com/mrf/employers/"
    assert employer_call_by_field["resolver"]["max_employer_links"] == 7


def test_viva_health_direct_commercial_target_handles_extensionless_downloads():
    source_dict = {"source_id": "source_viva", "payer_id": "payer_viva"}

    targets = discovery._collect_viva_health_commercial_targets(
        source_dict,
        "https://www.vivahealth.com/files/mrf/viva-health-commercial-out-of-network-rates",
    )

    assert len(targets) in {1}
    assert targets[0].url.endswith("viva-health-commercial-out-of-network-rates")
    assert targets[0].metadata["target_kind"] == "file_reference"
    assert targets[0].metadata["target_file_type"] == "allowed-amounts"
    assert targets[0].metadata["container_format"] == "zip"


def test_viva_health_employer_landing_target_indexes_group_context():
    source_dict = {"source_id": "source_viva", "payer_id": "payer_viva"}

    target = discovery._viva_health_employer_landing_target(
        source_dict,
        employer_url="https://www.mymedicalshopper.com/mrf/acme-viva-health-x01234",
        employer_page_url="https://www.vivahealth.com/mrf/employers/",
    )

    assert target.label == "Acme Viva Health X01234"
    assert target.metadata["target_kind"] == "source_landing_page"
    assert target.metadata["external_hosting_platform"] == "mymedicalshopper_talon"
    assert target.metadata["employer_slug"] == "acme-viva-health-x01234"
    assert target.metadata["group_id"] == "x01234"
    assert target.metadata["group_number"] == "x01234"
    assert target.metadata["tpa_slug"] == "viva-health"
    assert target.metadata["plan_info"] == [
        {
            "plan_id": "x01234",
            "plan_id_type": "group_number",
            "plan_name": "Acme Viva Health X01234",
            "plan_market_type": "group",
        }
    ]


MAGNACARE_RESULTS_HTML = """
<table id="claimresults">
  <tbody>
    <tr class="default">
      <td>EIN</td>
      <td>Group</td>
      <td>TESTPLAN001</td>
      <td><div>Example Employer Health Benefit Plan - HRA Plan</div></td>
      <td><div>MagnaCare PPO</div></td>
      <td>In-Network</td>
      <td>24 MB</td>
      <td>
        <a href="download" data-rhid="339" data-network="MagnaCare PPO"
           data-fversion="2.0" data-fsize="24 MB"
           data-fname="MagnaCarePPO_In-Network.zip" data-href="">Download</a>
      </td>
    </tr>
    <tr class="default">
      <td>EIN</td>
      <td>Group</td>
      <td>TESTPLAN001</td>
      <td><div>Example Employer Health Benefit Plan - Standard PPO Plan</div></td>
      <td><div>MagnaCare PPO</div></td>
      <td>In-Network</td>
      <td>24 MB</td>
      <td>
        <a href="download" data-rhid="339" data-network="MagnaCare PPO"
           data-fversion="2.0" data-fsize="24 MB"
           data-fname="MagnaCarePPO_In-Network.zip" data-href="">Download</a>
      </td>
    </tr>
    <tr class="default">
      <td>EIN</td>
      <td>Group</td>
      <td>135608135</td>
      <td><div>Delegated First Health Plan</div></td>
      <td><div>First Health</div></td>
      <td>In-Network</td>
      <td>0 MB</td>
      <td>
        <a href="download" data-rhid="0" data-network="First Health"
           data-fversion="1.0" data-fsize="0 MB" data-fname=""
           data-href="https://health1.firsthealth.com/app/public/#/one/insurerCode=FIRSTHEALTH_I&amp;brandCode=FIRSTH/machine-readable-transparency-in-coverage">Download</a>
      </td>
    </tr>
  </tbody>
</table>
"""


def test_magnacare_result_rows_extract_download_metadata():
    rows = discovery._magnacare_result_rows(MAGNACARE_RESULTS_HTML)

    assert len(rows) == 3
    assert rows[0]["plan_id"] == "TESTPLAN001"
    assert rows[0]["plan_name"] == "Example Employer Health Benefit Plan - HRA Plan"
    assert rows[0]["network_name"] == "MagnaCare PPO"
    assert rows[0]["file_type_label"] == "In-Network"
    assert rows[0]["run_history_id"] == "339"
    assert rows[0]["file_name"] == "MagnaCarePPO_In-Network.zip"
    assert rows[2]["run_history_id"] == "0"
    assert rows[2]["external_url"].startswith("https://health1.firsthealth.com/")


@pytest.mark.asyncio
async def test_magnacare_resolver_refreshes_download_urls_and_aggregates_plans(
    monkeypatch,
):
    """Verify this source-discovery regression contract."""
    fetched_result_urls = []
    fetched_download_urls = []

    async def fake_fetch_text(url, *, max_bytes, session):
        fetched_result_urls.append(url)
        assert max_bytes == 1024
        assert session == "session"
        return MAGNACARE_RESULTS_HTML

    async def fake_fetch_json(url, *, max_bytes, session):
        fetched_download_urls.append(url)
        assert "runHistoryID=339" in url
        assert "ipAddress=127.0.0.1" in url
        assert max_bytes == 1024
        assert session == "session"
        return {
            "Data": (
                "https://transparencymrfprod.blob.core.windows.net/mrf-magnacare/"
                "MagnaCarePPO_In-Network.zip?sv=2023&se=2026&sig=secret"
            )
        }

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    crawl_targets = await discovery._resolve_magnacare_transparency_mrf(
        {"source_id": "source_brighton", "payer_id": "payer_brighton"},
        "https://clm.magnacare.com/transparency/",
        {
            "type": "magnacare_transparency_mrf",
            "search_terms": ["magna"],
            "max_bytes": 1024,
            "max_targets": 10,
        },
        session="session",
    )

    assert len(fetched_result_urls) in {1}
    assert "filters=search-by%3Amagna" in fetched_result_urls[0]
    assert len(fetched_download_urls) in {1}
    assert len(crawl_targets) in {1}
    crawl_target = crawl_targets[0]
    assert crawl_target.url.endswith("MagnaCarePPO_In-Network.zip?sv=2023&se=2026&sig=secret")
    assert crawl_target.resolved_from_url == "https://clm.magnacare.com/transparency/"
    assert crawl_target.metadata["target_kind"] == "file_reference"
    assert crawl_target.metadata["target_file_type"] == "in-network"
    assert crawl_target.metadata["run_history_id"] == "339"
    assert crawl_target.metadata["size_bytes"] == 24_000_000
    assert crawl_target.metadata["plan_info"] == [
        {
            "plan_id": "TESTPLAN001",
            "plan_id_type": "EIN",
            "plan_market_type": "group",
            "plan_name": "Example Employer Health Benefit Plan - HRA Plan",
        },
        {
            "plan_id": "TESTPLAN001",
            "plan_id_type": "EIN",
            "plan_market_type": "group",
            "plan_name": "Example Employer Health Benefit Plan - Standard PPO Plan",
        },
    ]


@pytest.mark.asyncio
async def test_magnacare_resolver_uses_target_payer_query(monkeypatch):
    fetched_result_urls = []

    async def fake_fetch_text(requested_url, *, max_bytes, session):
        fetched_result_urls.append(requested_url)
        if "filters=search-by%3Aexample+employer" in requested_url:
            return MAGNACARE_RESULTS_HTML
        return ""

    async def fake_fetch_json(requested_url, *, max_bytes, session):
        return {
            "Data": (
                "https://transparencymrfprod.blob.core.windows.net/mrf-magnacare/"
                "MagnaCarePPO_In-Network.zip?sv=2023&se=2026&sig=secret"
            )
        }

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    resolved_targets = await discovery._resolve_magnacare_transparency_mrf(
        {
            "source_id": "source_abc",
            "payer_id": "payer_abc",
            "metadata_json": {
                "target_payer_query": "example_employer"
            },
        },
        "https://clm.magnacare.com/transparency/",
        {
            "type": "magnacare_transparency_mrf",
            "search_terms": ["magna"],
            "max_bytes": 1024,
            "max_targets": 10,
        },
        session="session",
    )

    assert "filters=search-by%3Aexample+employer" in fetched_result_urls[0]
    assert len(resolved_targets) == 1


def test_highmark_hmhs_script_expands_current_month_index_urls():
    script = """
    var fileArr = [
      { regName: "Delaware", dl: "/files/070/del/inbound/local/?FIRST_DAY_CUR_MONTH_Highmark_Blue_Cross_Blue_Shield_of_Delaware_index.json", dt: "Highmark Blue Cross Blue Shield Delaware" },
      { regName: "Pennsylvania", dl: "/files/363/pa/inbound/local/?FIRST_DAY_CUR_MONTH_Highmark_Blue_Cross_Blue_Shield_of_Pennsylvania_index.json", dt: "Highmark Blue Cross Blue Shield Pennsylvania" },
    ]
    """

    targets = discovery._parse_highmark_hmhs_script(
        script, base_url="https://mrfdata.hmhs.com/", month_start="2026-06-01"
    )

    assert len(targets) == 2
    assert (
        targets[0]["url"]
        == "https://mrfdata.hmhs.com/files/070/del/inbound/local/2026-06-01_Highmark_Blue_Cross_Blue_Shield_of_Delaware_index.json"
    )
    assert targets[0]["label"] == "Highmark Blue Cross Blue Shield Delaware"
    assert targets[1]["region"] == "Pennsylvania"


def test_parse_uhc_blob_listing_extracts_indexes_and_embedded_vision_direct_files():
    response_by_field = {
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
            {
                "name": "2026-06-01_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz",
                "downloadUrl": "https://mrfstore.example/public/vision.json.gz?sig=abc",
                "size": 7777,
            },
        ]
    }

    crawl_targets = discovery._parse_uhc_blob_listing(response_by_field)

    assert [crawl_target["size"] for crawl_target in crawl_targets] == [1234, 7777]
    assert crawl_targets[0]["label"] == "Abc Company"
    assert crawl_targets[0]["target_kind"] == "toc_json"
    assert crawl_targets[0]["target_file_type"] == "table-of-contents"
    assert crawl_targets[0]["url"].endswith("?sig=abc")
    assert crawl_targets[1]["target_kind"] == "file_reference"
    assert crawl_targets[1]["target_file_type"] == "in-network"
    assert crawl_targets[1]["container_format"] == "gzip"
    assert crawl_targets[1]["label"] == "UHC Vision"


@pytest.mark.asyncio
async def test_uhc_blob_query_finds_late_match_before_target_limit(monkeypatch):
    generic_blobs = [
        {
            "name": f"2026-07-01_Generic-Employer-{index:03d}_index.json",
            "downloadUrl": (
                "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/"
                f"download/2026-07-01_Generic-Employer-{index:03d}_index.json"
            ),
            "size": 1000 + index,
        }
        for index in range(60)
    ]
    blob_listing_dict = {
        "blobs": [
            *generic_blobs,
            {
                "name": "2026-07-01_Sample-Employer-LLC_index.json",
                "downloadUrl": (
                    "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/"
                    "download/2026-07-01_Sample-Employer-LLC_index.json"
                ),
                "size": 12345,
            },
        ]
    }

    async def fake_fetch_json(url, *, max_bytes, session):
        assert url == "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/"
        assert max_bytes == 67108864
        assert session == "session"
        return blob_listing_dict

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    query_source_dict = _synthetic_query_source(
        source_id="source_uhc_example",
        platform="uhc_public_blobs",
        index_url="https://transparency-in-coverage.uhc.com/",
    )

    crawl_targets = await discovery._crawl_targets_for_source(
        query_source_dict,
        "https://transparency-in-coverage.uhc.com/",
        session="session",
        target_limit=50,
    )

    assert len(crawl_targets) == 1
    assert crawl_targets[0].label == "Sample Employer Llc"
    assert (
        crawl_targets[0].metadata["blob_name"]
        == "2026-07-01_Sample-Employer-LLC_index.json"
    )


def test_uhc_provider_mrf_targets_from_payload_catalogs_file_references():
    """Verify this source-discovery regression contract."""
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "UnitedHealthcare IFP",
    }
    listing_response_dict = {
        "providers": [
            {
                "name": "providers",
                "date": "2026-06-25T11:12:03.000Z",
                "blobPath": "ui/ifp/providers",
            },
            {
                "name": "JSON_Providers_NMIEX.json",
                "date": "2026-06-26T15:30:52.000Z",
                "blobPath": "ui/ifp/providers/JSON_Providers_NMIEX.json",
            },
            {
                "name": "JSON_Providers_NMIEX.json.trig",
                "date": "2026-06-26T15:30:00.000Z",
                "blobPath": "ui/ifp/providers/JSON_Providers_NMIEX.json.trig",
            },
        ],
        "drugs": [
            {"name": "filename", "url": "URL"},
            {
                "name": "JSON_Drugs_UHCALEX_HIX.json",
                "url": "https://legacy.providerlookuponline.com/mrf/optumrx/drugs/2877216/JSON_Drugs_UHCALEX_HIX.json",
                "date": "2025-09-02T10:41:01.000Z",
                "isExternal": True,
            },
        ],
        "plans": [
            {
                "name": "JSON_PLANS_AL.json",
                "date": "2025-11-11T11:45:20.000Z",
                "blobPath": "ui/ifp/plans/JSON_PLANS_AL.json",
            }
        ],
    }

    file_targets = discovery._uhc_provider_mrf_targets_from_payload(
        catalog_source_dict,
        listing_response_dict,
        listing_url="https://providermrf.uhc.com/api/files/ui/ifp/",
    )

    assert [file_target.url for file_target in file_targets] == [
        "https://providermrf.uhc.com/api/stream/ui/ifp/providers/JSON_Providers_NMIEX.json",
        "https://legacy.providerlookuponline.com/mrf/optumrx/drugs/2877216/JSON_Drugs_UHCALEX_HIX.json",
        "https://providermrf.uhc.com/api/stream/ui/ifp/plans/JSON_PLANS_AL.json",
    ]
    assert [file_target.metadata["target_file_type"] for file_target in file_targets] == [
        "provider-network",
        "payer-drug",
        "plan-reference",
    ]
    assert all(file_target.metadata["target_kind"] == "file_reference" for file_target in file_targets)
    assert file_targets[0].label == "Providers Nmiex"
    assert file_targets[0].metadata["uhc_provider_blob_path"] == (
        "ui/ifp/providers/JSON_Providers_NMIEX.json"
    )
    assert file_targets[1].metadata["uhc_provider_external"] is True


@pytest.mark.asyncio
async def test_uhc_provider_mrf_resolver_fetches_ifp_listing(monkeypatch):
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "UnitedHealthcare IFP",
    }
    listing_response_dict = {
        "providers": [
            {
                "name": "JSON_Providers_NMIEX.json",
                "date": "2026-06-26T15:30:52.000Z",
                "blobPath": "ui/ifp/providers/JSON_Providers_NMIEX.json",
            }
        ]
    }

    async def fake_fetch_json(url, **_kwargs):
        assert url == "https://providermrf.uhc.com/api/files/ui/ifp/"
        return listing_response_dict

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    [provider_file_target] = await discovery._crawl_targets_for_source(
        catalog_source_dict,
        "https://providermrf.uhc.com/IFP",
        None,
    )

    assert provider_file_target.url == (
        "https://providermrf.uhc.com/api/stream/ui/ifp/providers/JSON_Providers_NMIEX.json"
    )
    assert provider_file_target.resolved_from_url == "https://providermrf.uhc.com/api/files/ui/ifp/"
    assert provider_file_target.metadata["resolver"] == "uhc_provider_mrf_files"


def test_humana_pct_targets_from_payload_catalogs_tocs_only_by_default():
    source_dict = {"source_id": "source_1", "display_name": "Humana"}
    payload = {
        "aaData": [
            [
                "2026-06-01_Humana_index.json",
                '<a href="/syntheticdata/Resource/DownloadTOCFile?fileName=2026-06-01_Humana_index.json">TOC</a>',
            ],
            ["000001.csv.gz", "body segment"],
        ],
        "iTotalRecords": 2,
    }

    targets = discovery._humana_pct_targets_from_payload(
        source_dict,
        payload,
        api_url="https://developers.humana.com/syntheticdata/Resource/GetData?fileType=innetwork",
        resolver={"download_path": "/syntheticdata/Resource/DownloadTOCFile"},
        resolver_type="humana_pct_file_list",
    )

    assert len(targets) in {1}
    assert targets[0].url == (
        "https://developers.humana.com/syntheticdata/Resource/"
        "DownloadTOCFile?fileName=2026-06-01_Humana_index.json"
    )
    assert targets[0].metadata["target_kind"] == "toc_json"
    assert targets[0].metadata["target_file_type"] == "table-of-contents"
    assert targets[0].metadata["humana_file_name"] == "2026-06-01_Humana_index.json"


@pytest.mark.asyncio
async def test_humana_pct_resolver_paginates_bounded_file_list(monkeypatch):
    source_dict = {"source_id": "source_1", "display_name": "Humana"}
    calls = []

    async def fake_fetch_json(url, **_kwargs):
        calls.append(url)
        if "iDisplayStart=0" in url:
            return {
                "aaData": [["2026-06-01_Humana_index.json"]],
                "iTotalRecords": 2,
            }
        if "iDisplayStart=1" in url:
            return {
                "aaData": [["2026-06-01_Humana_Dental_index.json"]],
                "iTotalRecords": 2,
            }
        return {"aaData": [], "iTotalRecords": 2}

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    crawl_targets = await discovery._resolve_humana_pct_file_list(
        source_dict,
        "https://developers.humana.com/cost-transparency",
        {
            "type": "humana_pct_file_list",
            "api_url": "https://developers.humana.com/syntheticdata/Resource/GetData",
            "download_path": "/syntheticdata/Resource/DownloadTOCFile",
            "file_types": ["innetwork"],
            "page_size": 1,
            "max_pages": 3,
            "max_targets": 10,
        },
        None,
    )

    assert [
        crawl_target.metadata["humana_file_name"]
        for crawl_target in crawl_targets
    ] == [
        "2026-06-01_Humana_index.json",
        "2026-06-01_Humana_Dental_index.json",
    ]
    assert "iDisplayStart=0" in calls[0]
    assert "iDisplayStart=1" in calls[1]


def test_fchn_detail_parser_extracts_public_zip_file_reference():
    source_dict = {"source_id": "source_1", "display_name": "First Choice Health"}
    html = """
    <table>
      <tr>
        <td>In-Network Negotiated Rate - TPA - Client Network File</td>
        <td><a href="/documents/ppo/providers/payorsearch/64647/innrftpac/example.zip">Download</a></td>
      </tr>
    </table>
    """

    targets = discovery._fchn_targets_from_detail_html(
        source_dict,
        html,
        detail_url="https://www.fchn.com/PayorSearch/Home/PayorDetail/64647",
        resolver_type="fchn_payor_search",
    )

    assert len(targets) in {1}
    assert targets[0].url == (
        "https://www.fchn.com/documents/ppo/providers/payorsearch/"
        "64647/innrftpac/example.zip"
    )
    assert targets[0].metadata["target_kind"] == "file_reference"
    assert targets[0].metadata["target_file_type"] == "in-network"
    assert targets[0].metadata["container_format"] == "zip"
    assert targets[0].metadata["fchn_payor_detail_id"] == "64647"


@pytest.mark.asyncio
async def test_fchn_resolver_reports_cloudflare_challenge(monkeypatch):
    source_dict = {"source_id": "source_1", "display_name": "First Choice Health"}

    async def fake_fetch_text(*_args, **_kwargs):
        return "<html><head><title>Just a moment...</title></head><script>__cf_chl</script></html>"

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    with pytest.raises(ValueError, match="cloudflare_challenge"):
        await discovery._resolve_fchn_payor_search(
            source_dict,
            "https://www.fchn.com/PayorSearch",
            {"type": "fchn_payor_search"},
            None,
        )


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
    params_dict = {"insurerCode": "AETNACVS_I", "brandCode": "ALICFI"}

    url = discovery._healthsparq_direct_metadata_url(resolver, params_dict)

    assert (
        url
        == "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICFI/latest_metadata.json"
    )


def test_healthsparq_direct_metadata_url_uses_configured_tenant_override():
    resolver = discovery._source_config()["platform_resolvers"]["aetna_health1"]
    params_dict = {"insurerCode": "MERITAIN_I", "brandCode": "MERITAINOVER"}

    url = discovery._healthsparq_direct_metadata_url(resolver, params_dict)

    assert (
        url
        == "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/MERITAIN_I/MERITAINOVER/latest_metadata.json"
    )


def test_healthsparq_direct_metadata_url_allows_search_term_only_filter():
    resolver = discovery._source_config()["platform_resolvers"]["aetna_health1"]
    params_dict = {
        "insurerCode": "AETNACVS_I",
        "brandCode": "ASA",
        "searchTerm": "ASA_01",
    }

    url = discovery._healthsparq_direct_metadata_url(resolver, params_dict)

    assert (
        url
        == "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ASA/latest_metadata.json"
    )


def test_healthsparq_direct_metadata_url_skips_scoped_filters():
    resolver = discovery._source_config()["platform_resolvers"]["aetna_health1"]
    params_dict = {
        "insurerCode": "MERITAIN_I",
        "brandCode": "MERITAINOVER",
        "reportingEntityType": "TPA_14445",
    }

    assert discovery._healthsparq_direct_metadata_url(resolver, params_dict) is None


def test_healthsparq_target_is_landing():
    target = discovery._healthsparq_target(
        {"source_id": "source_1", "display_name": "Priority Health"},
        "https://mrf.healthsparq.com/ph/prd/mrf/PH_I/PH/latest_metadata.json",
        "https://web.healthsparq.com/healthsparq/public/",
        {"insurerCode": "PH_I", "brandCode": "PH"},
    )

    assert target.url == "https://web.healthsparq.com/healthsparq/public/"
    assert target.metadata["target_kind"] == "source_landing_page"
    assert target.metadata["target_file_type"] == "source-landing-page"
    assert (
        target.metadata["landing_reason"] == "healthsparq_metadata_unexpanded"
    )


def test_healthsparq_metadata_rows_include_direct_file_urls_and_plans():
    catalog_source_dict = {"source_id": "source_1", "payer_id": "payer_1"}
    metadata_url = "https://mrf.healthsparq.com/example/prd/mrf/AETNACVS_I/ALICFI/latest_metadata.json"
    metadata_response_dict = {
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

    plan_rows, file_rows = discovery._healthsparq_rows_from_metadata(
        catalog_source_dict, metadata_url, metadata_response_dict
    )

    assert len(plan_rows) in {1}
    assert plan_rows[0]["plan_id"] == "20523CA003"
    assert len(file_rows) in {1}
    assert file_rows[0]["file_type"] == "in-network"
    assert (
        file_rows[0]["url"]
        == "https://mrf.healthsparq.com/example/prd/mrf/AETNACVS_I/ALICFI/2026-05-05/inNetworkRates/rates.json.gz"
    )
    assert file_rows[0]["from_index_url"] == metadata_url
    assert file_rows[0]["metadata_json"]["resolver"] == "healthsparq_public_mrf"
    assert [
        {
            key: plan[key]
            for key in ("plan_id", "plan_id_type", "plan_market_type", "plan_name")
        }
        for plan in file_rows[0]["metadata_json"]["plan_info"]
    ] == [
        {
            "plan_id": "20523CA003",
            "plan_id_type": "hios",
            "plan_market_type": "group",
            "plan_name": "Aetna Open Access",
        }
    ]
    assert file_rows[0]["metadata_json"]["plan_info"][0]["engine_plan_hash"]


def test_healthsparq_targets_from_metadata_expand_direct_file_urls_and_plans():
    """Verify this source-discovery regression contract."""
    catalog_source_dict = {"source_id": "source_1", "payer_id": "payer_1"}
    metadata_url = (
        "https://mrf.healthsparq.com/example/prd/mrf/UNVRA_I/UNVRA/latest_metadata.json"
    )
    metadata_response_dict = {
        "files": [
            {
                "reportingEntityName": "Univera Healthcare",
                "reportingEntityType": "Health Insurance Issuer",
                "reportingPlans": [
                    {
                        "planName": "Group PPO",
                        "planIdType": "ein",
                        "planId": "123456789",
                        "planMarketType": "group",
                    }
                ],
                "lastUpdatedOn": "2026-07-01",
                "fileSchema": "IN_NETWORK_RATES",
                "fileName": "rates.json.zip",
                "filePath": "2026-07-01/inNetworkRates/rates.json.zip",
            },
            {
                "reportingEntityName": "Univera Healthcare",
                "reportingEntityType": "Health Insurance Issuer",
                "reportingPlans": [],
                "lastUpdatedOn": "2026-07-01",
                "fileSchema": "TABLE_OF_CONTENTS",
                "fileName": "index.json.gz",
                "filePath": "2026-07-01/tableOfContents/index.json.gz",
            },
        ]
    }

    metadata_targets = discovery._healthsparq_targets_from_metadata(
        catalog_source_dict,
        metadata_url,
        metadata_response_dict,
        resolved_from_url="https://univerahc.healthsparq.com/healthsparq/public/",
        params={"insurerCode": "UNVRA_I", "brandCode": "UNVRA"},
    )

    assert [metadata_target.metadata["target_kind"] for metadata_target in metadata_targets] == [
        "file_reference",
        "file_reference",
    ]
    assert metadata_targets[0].url == (
        "https://mrf.healthsparq.com/example/prd/mrf/UNVRA_I/UNVRA/"
        "2026-07-01/inNetworkRates/rates.json.zip"
    )
    assert metadata_targets[0].metadata["target_file_type"] == "in-network"
    assert metadata_targets[0].metadata["container_format"] == "zip"
    assert metadata_targets[0].metadata["metadata_url"] == metadata_url
    [plan_info] = metadata_targets[0].metadata["plan_info"]
    assert {
        key: plan_info[key]
        for key in ("plan_id", "plan_id_type", "plan_market_type", "plan_name")
    } == {
        "plan_id": "123456789",
        "plan_id_type": "ein",
        "plan_market_type": "group",
        "plan_name": "Group PPO",
    }
    assert plan_info["engine_plan_hash"]
    assert metadata_targets[1].metadata["target_file_type"] == "table-of-contents"


@pytest.mark.asyncio
async def test_healthsparq_resolver_expands_direct_metadata_manifest(monkeypatch):
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Univera Healthcare",
    }
    resolver = discovery._source_config()["platform_resolvers"]["healthsparq"]
    source_url = (
        "https://univerahc.healthsparq.com/healthsparq/public/#/one/"
        "insurerCode=UNVRA_I&brandCode=UNVRA/machine-readable-transparency-in-coverage"
    )
    metadata_url = (
        "https://mrf.healthsparq.com/unvra-egress.nophi.kyruushsq.com/prd/mrf/"
        "UNVRA_I/UNVRA/latest_metadata.json"
    )

    async def fake_fetch_json(url, **_kwargs):
        assert url == metadata_url
        return {
            "files": [
                {
                    "reportingEntityName": "Univera Healthcare",
                    "reportingEntityType": "Health Insurance Issuer",
                    "reportingPlans": [
                        {
                            "planName": "Group PPO",
                            "planIdType": "ein",
                            "planId": "123456789",
                            "planMarketType": "group",
                        }
                    ],
                    "lastUpdatedOn": "2026-07-01",
                    "fileSchema": "IN_NETWORK_RATES",
                    "fileName": "rates.json.zip",
                    "filePath": "2026-07-01/inNetworkRates/rates.json.zip",
                }
            ]
        }

    async def allow_url(_url):
        return None

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allow_url)

    resolved_crawl_targets = await discovery._resolve_healthsparq_public_mrf(
        catalog_source_dict, source_url, resolver, None
    )

    assert len(resolved_crawl_targets) in {1}
    assert resolved_crawl_targets[0].url == (
        "https://mrf.healthsparq.com/unvra-egress.nophi.kyruushsq.com/prd/mrf/"
        "UNVRA_I/UNVRA/2026-07-01/inNetworkRates/rates.json.zip"
    )
    assert resolved_crawl_targets[0].resolved_from_url == metadata_url
    assert resolved_crawl_targets[0].metadata["target_kind"] == "file_reference"
    assert resolved_crawl_targets[0].metadata["plan_info"][0]["plan_id"] == "123456789"


@pytest.mark.asyncio
async def test_healthsparq_resolver_falls_back_to_metadata_target(monkeypatch):
    healthsparq_public_url = (
        "https://univerahc.healthsparq.com/healthsparq/public/#/one/"
        "insurerCode=UNVRA_I&brandCode=UNVRA/machine-readable-transparency-in-coverage"
    )

    async def fake_fetch_json(_url, **_kwargs):
        raise ValueError("too large")

    async def allow_url(_url):
        return None

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allow_url)

    resolved_crawl_targets = await discovery._resolve_healthsparq_public_mrf(
        {
            "source_id": "source_1",
            "payer_id": "payer_1",
            "display_name": "Univera Healthcare",
        },
        healthsparq_public_url,
        discovery._source_config()["platform_resolvers"]["healthsparq"],
        None,
    )

    assert len(resolved_crawl_targets) in {1}
    assert resolved_crawl_targets[0].url == healthsparq_public_url
    assert resolved_crawl_targets[0].metadata["metadata_url"].endswith(
        "/UNVRA_I/UNVRA/latest_metadata.json"
    )
    assert resolved_crawl_targets[0].metadata["target_kind"] == "source_landing_page"
    assert resolved_crawl_targets[0].metadata["target_file_type"] == "source-landing-page"
    assert (
        resolved_crawl_targets[0].metadata["landing_reason"]
        == "healthsparq_metadata_unexpanded"
    )


@pytest.mark.asyncio
async def test_healthsparq_resolver_falls_back_when_metadata_url_is_blocked(
    monkeypatch,
):
    healthsparq_public_url = (
        "https://univerahc.healthsparq.com/healthsparq/public/#/one/"
        "insurerCode=UNVRA_I&brandCode=UNVRA/machine-readable-transparency-in-coverage"
    )
    metadata_url = (
        "https://mrf.healthsparq.com/unvra-egress.nophi.kyruushsq.com/prd/mrf/"
        "UNVRA_I/UNVRA/latest_metadata.json"
    )

    async def fake_fetch_json(_url, **_kwargs):
        raise AssertionError("metadata fetch should not run after URL preflight fails")

    async def block_url(url):
        assert url == metadata_url
        raise ValueError("URL resolves to a non-public address: 127.0.0.1")

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", block_url)

    resolved_crawl_targets = await discovery._resolve_healthsparq_public_mrf(
        {
            "source_id": "source_1",
            "payer_id": "payer_1",
            "display_name": "Univera Healthcare",
        },
        healthsparq_public_url,
        discovery._source_config()["platform_resolvers"]["healthsparq"],
        None,
    )

    assert len(resolved_crawl_targets) == 1
    assert resolved_crawl_targets[0].url == healthsparq_public_url
    assert resolved_crawl_targets[0].metadata["metadata_url"] == metadata_url
    assert resolved_crawl_targets[0].metadata["target_kind"] == "source_landing_page"
    assert resolved_crawl_targets[0].metadata["target_file_type"] == "source-landing-page"
    assert (
        resolved_crawl_targets[0].metadata["landing_reason"]
        == "healthsparq_metadata_unexpanded"
    )


@pytest.mark.asyncio
async def test_hs_metadata_expands_files(monkeypatch):
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Health",
    }
    metadata_url = (
        "https://mrf.healthsparq.com/example-egress.nophi.kyruushsq.com/prd/mrf/"
        "EXAMPLE_I/EXAMPLE/latest_metadata.json"
    )

    async def fake_fetch_json(url, *, max_bytes, session):
        assert url == metadata_url
        assert max_bytes == 2048
        assert session == "session"
        return {
            "files": [
                {
                    "reportingEntityName": "Example Health",
                    "reportingEntityType": "Health Insurance Issuer",
                    "reportingPlans": [
                        {
                            "planName": "Example PPO",
                            "planIdType": "ein",
                            "planId": "123456789",
                            "planMarketType": "group",
                        }
                    ],
                    "lastUpdatedOn": "2026-07-01",
                    "fileSchema": "IN_NETWORK_RATES",
                    "fileName": "rates.json.gz",
                    "filePath": "2026-07-01/inNetworkRates/rates.json.gz",
                }
            ]
        }

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    resolved_targets = await discovery._resolve_healthsparq_direct_metadata(
        source_dict,
        metadata_url,
        {"type": "healthsparq_direct_metadata", "max_bytes": 2048},
        "session",
    )

    assert [crawl_target.url for crawl_target in resolved_targets] == [
        "https://mrf.healthsparq.com/example-egress.nophi.kyruushsq.com/prd/mrf/"
        "EXAMPLE_I/EXAMPLE/2026-07-01/inNetworkRates/rates.json.gz"
    ]
    assert resolved_targets[0].metadata["target_file_type"] == "in-network"
    assert resolved_targets[0].metadata["insurer_code"] == "EXAMPLE_I"
    assert resolved_targets[0].metadata["brand_code"] == "EXAMPLE"
    assert resolved_targets[0].metadata["plan_info"][0]["plan_name"] == "Example PPO"


@pytest.mark.asyncio
async def test_hs_metadata_fallback_landing(
    monkeypatch,
):
    source_dict = {"source_id": "source_1", "display_name": "Example Health"}
    metadata_url = (
        "https://mrf.healthsparq.com/example-egress.nophi.kyruushsq.com/prd/mrf/"
        "EXAMPLE_I/EXAMPLE/latest_metadata.json"
    )

    async def fake_fetch_json(*_args, **_kwargs):
        raise ValueError("not available during discovery")

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    resolved_targets = await discovery._resolve_healthsparq_direct_metadata(
        source_dict,
        metadata_url,
        {"type": "healthsparq_direct_metadata"},
        None,
    )

    assert [crawl_target.url for crawl_target in resolved_targets] == [metadata_url]
    assert resolved_targets[0].metadata["target_kind"] == "source_landing_page"
    assert resolved_targets[0].metadata["target_file_type"] == "source-landing-page"
    assert (
        resolved_targets[0].metadata["landing_reason"]
        == "healthsparq_metadata_unexpanded"
    )


@pytest.mark.asyncio
async def test_providence_resolver_reads_config_and_expands_group_tocs(monkeypatch):
    """Verify this source-discovery regression contract."""
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Providence Health Plan",
    }
    json_by_url = {
        "https://mrfhub.providencehealthplan.com/config.json": {
            "X_API_KEY": "public_key",
            "API_ENDPOINT": "https://api.example.test/PROD/",
        },
        "https://api.example.test/PROD/group/?groupname=Individual+and+Family+Plans": {
            "groups": [
                {
                    "group-id": "106135",
                    "group-name": "INDIVIDUAL AND FAMILY PLANS",
                }
            ]
        },
        "https://api.example.test/PROD/toc/?groupid=106135": {
            "groups": [
                {
                    "group-id": "106135",
                    "group-name": "INDIVIDUAL AND FAMILY PLANS",
                    "tocs": [
                        {
                            "TOC_URL": (
                                "https://cms-price-transparency-hosting-prod.s3.amazonaws.com/"
                                "ExternalPartnerData/Beacon/TOC/"
                                "2026-06-05_ProvidenceHealthPlan_BEACON_Index.json"
                            )
                        }
                    ],
                }
            ]
        },
    }

    async def fake_fetch_json(url, **_kwargs):
        return json_by_url[url]

    async def fake_fetch_json_with_headers(url, *, headers, **_kwargs):
        assert headers == {"X-API-Key": "public_key"}
        return json_by_url[url]

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(
        discovery, "_fetch_json_with_headers", fake_fetch_json_with_headers
    )

    group_toc_targets = await discovery._resolve_providence_mrf_api(
        catalog_source_dict,
        "https://mrfhub.providencehealthplan.com/",
        {
            "type": "providence_mrf_api",
            "group_queries": ["Individual and Family Plans"],
        },
        None,
    )

    assert [group_toc_target.url for group_toc_target in group_toc_targets] == [
        (
            "https://cms-price-transparency-hosting-prod.s3.amazonaws.com/"
            "ExternalPartnerData/Beacon/TOC/"
            "2026-06-05_ProvidenceHealthPlan_BEACON_Index.json"
        )
    ]
    assert group_toc_targets[0].metadata["target_kind"] == "toc_json"
    assert group_toc_targets[0].metadata["target_file_type"] == "table-of-contents"
    assert group_toc_targets[0].metadata["group_id"] == "106135"
    assert group_toc_targets[0].metadata["plan_info"][0]["plan_id_type"] == (
        "providence_group_id"
    )


def test_crawl_target_context_uses_source_display_as_company_name():
    context = discovery._crawl_target_context_metadata(
        discovery.CrawlTarget(
            source={"display_name": "Example Public TPA"},
            url="https://example.test/in-network-rates.json.gz",
            label="In-network rates",
            metadata={},
        )
    )

    assert context["company_name"] == "Example Public TPA"


def test_parse_sapphire_toc_links_extracts_unique_json_hrefs():
    html = """
    <a href="/tocs/202606/2026-06-01_blue-cross-of-idaho_index.json">Download</a>
    <a href="/tocs/202606/2026-06-01_blue-cross-of-idaho_index.json">Copy</a>
    <a href="/not-a-toc/file.json">Ignore</a>
    """

    targets = discovery._parse_sapphire_toc_links(
        html, base_url="https://bci.sapphiremrfhub.com/"
    )

    assert targets == [
        {
            "url": "https://bci.sapphiremrfhub.com/tocs/202606/2026-06-01_blue-cross-of-idaho_index.json",
            "label": "Blue Cross Of Idaho",
            "file_name": "2026-06-01_blue-cross-of-idaho_index.json",
        }
    ]


def test_parse_sapphire_toc_links_extracts_data_href_and_extensionless_tocs():
    html = """
    <button data-href="/tocs/current/example_vision">Download</button>
    <a href="/tocs/current/example_vision">Duplicate</a>
    """

    targets = discovery._parse_sapphire_toc_links(
        html, base_url="https://example.sapphiremrfhub.com/"
    )

    assert targets == [
        {
            "url": "https://example.sapphiremrfhub.com/tocs/current/example_vision",
            "label": "Example Vision",
            "file_name": "example_vision",
        }
    ]


def test_parse_sapphire_toc_links_extracts_embedded_relative_tocs():
    html = """
    <script>
      window.__DATA__ = {"toc": "\\/tocs\\/202606\\/2026-06-01_example_index.json"};
    </script>
    """

    targets = discovery._parse_sapphire_toc_links(
        html, base_url="https://healthcomp.sapphiremrfhub.com/"
    )

    assert targets == [
        {
            "url": "https://healthcomp.sapphiremrfhub.com/tocs/202606/2026-06-01_example_index.json",
            "label": "Example",
            "file_name": "2026-06-01_example_index.json",
        }
    ]


def test_sapphire_query_slug_variants_normalize_company_suffixes():
    assert discovery._sapphire_query_slug_variants("Example Packaging, Inc.") == [
        "example-packaging-inc",
        "example_packaging_inc",
        "example-packaging",
        "example_packaging",
    ]
    assert discovery._sapphire_query_slug_variants("Example Limited Liability Company") == [
        "example-llc",
        "example_llc",
        "example",
    ]


@pytest.mark.asyncio
async def test_sapphire_query_probe_targets_keep_existing_current_tocs(monkeypatch):
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "BCBS Louisiana",
    }

    async def fake_head_url(url, **_kwargs):
        return {
            "status": "ok" if url.endswith("/example_packaging_inc") else "failed",
            "checked_at": discovery._utc_now(),
        }

    monkeypatch.setattr(discovery, "_head_url", fake_head_url)

    targets = await discovery._sapphire_query_probe_targets(
        source_dict,
        "https://bcbsla.sapphiremrfhub.com/",
        "Example Packaging Inc",
        None,
    )

    assert [target.url for target in targets] == [
        "https://bcbsla.sapphiremrfhub.com/tocs/current/example_packaging_inc"
    ]
    assert targets[0].metadata["company_name"] == "Example Packaging Inc"


def test_sapphire_static_query_hashes_are_loaded_from_gatsby_page_data():
    page_data = json.dumps(
        {
            "componentChunkName": "component---src-pages-index-jsx",
            "staticQueryHashes": ["254433488", "3220486668", "254433488"],
        }
    )

    assert discovery._sapphire_static_query_hashes(page_data) == [
        "254433488",
        "3220486668",
    ]


def test_parse_sapphire_static_query_toc_links_extracts_current_tocs():
    query_json = json.dumps(
        {
            "data": {
                "allTocsJson": {
                    "edges": [
                        {
                            "node": {
                                "payer_name": "Example Employer",
                                "file_name": "2026-06-01_example-employer_index.json",
                                "url": "https://healthcomp.sapphiremrfhub.com/tocs/202606/2026-06-01_example-employer_index.json",
                            }
                        },
                        {
                            "node": {
                                "payer_name": "Ignore",
                                "file_name": "provider-data.json",
                                "url": "https://healthcomp.sapphiremrfhub.com/provider-data.json",
                            }
                        },
                    ]
                }
            }
        }
    )

    assert discovery._parse_sapphire_static_query_toc_links(query_json) == [
        {
            "url": "https://healthcomp.sapphiremrfhub.com/tocs/202606/2026-06-01_example-employer_index.json",
            "label": "Example Employer",
            "file_name": "2026-06-01_example-employer_index.json",
            "payer_name": "Example Employer",
        }
    ]


def test_parse_sapphire_static_query_toc_links_recurses_gatsby_nodes():
    query_json = json.dumps(
        {
            "data": {
                "allFile": {
                    "nodes": [
                        {
                            "childTocsJson": {
                                "payer_name": "Nested Employer",
                                "publicURL": "/tocs/current/nested_employer",
                            }
                        }
                    ]
                }
            }
        }
    )

    assert discovery._parse_sapphire_static_query_toc_links(
        query_json, base_url="https://healthcomp.sapphiremrfhub.com/"
    ) == [
        {
            "url": "https://healthcomp.sapphiremrfhub.com/tocs/current/nested_employer",
            "label": "Nested Employer",
            "file_name": "nested_employer",
            "payer_name": "Nested Employer",
        }
    ]


@pytest.mark.asyncio
async def test_sapphire_resolver_falls_back_to_gatsby_static_queries(monkeypatch):
    """Verify Sapphire resolution uses bounded Gatsby queries after API failure."""
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "HealthComp",
    }
    html = "<html><tbody></tbody></html>"
    page_data = json.dumps(
        {
            "componentChunkName": "component---src-pages-index-jsx",
            "staticQueryHashes": ["254433488"],
        }
    )
    static_query = json.dumps(
        {
            "data": {
                "allTocsJson": {
                    "edges": [
                        {
                            "node": {
                                "payer_name": "Example Employer",
                                "file_name": "2026-06-01_example-employer_index.json",
                                "url": "https://healthcomp.sapphiremrfhub.com/tocs/202606/2026-06-01_example-employer_index.json",
                            }
                        }
                    ]
                }
            }
        }
    )
    html_by_url = {
        "https://healthcomp.sapphiremrfhub.com/": html,
        "https://healthcomp.sapphiremrfhub.com/page-data/index/page-data.json": page_data,
        "https://healthcomp.sapphiremrfhub.com/page-data/sq/d/254433488.json": static_query,
    }
    async def fake_fetch_text(url, **_kwargs):
        return html_by_url[url]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    crawl_targets = await discovery._crawl_targets_for_source(
        source_dict,
        "https://healthcomp.sapphiremrfhub.com/",
        None,
    )

    assert crawl_targets == [
        discovery.CrawlTarget(
            source=source_dict,
            url="https://healthcomp.sapphiremrfhub.com/tocs/202606/2026-06-01_example-employer_index.json",
            label="Example Employer",
            resolved_from_url="https://healthcomp.sapphiremrfhub.com/",
            metadata={
                "resolver": "sapphire_html_tocs",
                "file_name": "2026-06-01_example-employer_index.json",
                "payer_name": "Example Employer",
            },
        )
    ]


@pytest.mark.asyncio
async def test_sapphire_resolver_reads_page_data_tocs_before_static_queries(
    monkeypatch,
):
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "HealthComp",
    }
    page_data = json.dumps(
        {
            "staticQueryHashes": ["254433488"],
            "result": {
                "pageContext": {
                    "payer_name": "Page Data Employer",
                    "url": "/tocs/current/page_data_employer",
                }
            },
        }
    )
    html_by_url = {
        "https://healthcomp.sapphiremrfhub.com/": "<html></html>",
        "https://healthcomp.sapphiremrfhub.com/page-data/index/page-data.json": page_data,
    }

    async def fake_fetch_text(url, **_kwargs):
        if url.endswith("/page-data/sq/d/254433488.json"):
            raise AssertionError("page-data TOCs should avoid static-query fetches")
        return html_by_url[url]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    [crawl_target] = await discovery._crawl_targets_for_source(
        source_dict,
        "https://healthcomp.sapphiremrfhub.com/",
        None,
    )

    assert crawl_target == discovery.CrawlTarget(
        source=source_dict,
        url="https://healthcomp.sapphiremrfhub.com/tocs/current/page_data_employer",
        label="Page Data Employer",
        resolved_from_url="https://healthcomp.sapphiremrfhub.com/",
        metadata={
            "resolver": "sapphire_html_tocs",
            "file_name": "page_data_employer",
            "payer_name": "Page Data Employer",
        },
    )


def test_parse_html_mrf_metadata_links_extracts_meta_txt_files():
    html = """
    <a href="./in-network-rates-meta.txt">In Network</a>
    <a href="./allowed-amount-meta.txt">Allowed</a>
    <a href="./style.css">Ignore</a>
    """

    targets = discovery._parse_html_mrf_metadata_links(
        html,
        base_url="https://transparency-in-coverage.collectivehealth.com/index.html",
    )

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
    """Verify this source-discovery regression contract."""
    html = """
    <a href="/mrf/2026-06-01_example_index.json">TOC</a>
    <a href="/mrf/hmo_ha_hii_arkbluecross_index">No-extension TOC</a>
    <a href="/files/in-network-rates.zip">In Network ZIP</a>
    <a href="/files/allowed-amounts.json.gz">Allowed Amounts</a>
    <a href="https://static.example.com/MRFs/CignaMRF.json.gz">here</a>
    <a href="/not-mrf/file.json">Ignore</a>
    <a href="https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/in-network-rates">
      In-network rates schema
    </a>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://example.com/machine-readable-files"
    )

    assert crawl_targets == [
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
        {
            "url": "https://static.example.com/MRFs/CignaMRF.json.gz",
            "label": "Cignamrf",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "gzip",
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "Cignamrf",
                }
            ],
        },
    ]


def test_parse_html_mrf_links_extracts_raw_embedded_rate_files():
    html = """
    <div
      data-files='[
        "/wp-content/uploads/2026/06/2026-06-01_example_allowed-amounts.csv",
        "/wp-content/uploads/2026/06/2026-06-01_example_innetworkrates.json",
        "/wp-content/uploads/theme/app.min.js"
      ]'>
      Machine Readable Files
    </div>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://example-tpa.com/machine-readable-files/"
    )

    assert [
        (crawl_target["url"], crawl_target["target_file_type"], crawl_target["target_kind"])
        for crawl_target in crawl_targets
    ] == [
        (
            "https://example-tpa.com/wp-content/uploads/2026/06/2026-06-01_example_allowed-amounts.csv",
            "allowed-amounts",
            "file_reference",
        ),
        (
            "https://example-tpa.com/wp-content/uploads/2026/06/2026-06-01_example_innetworkrates.json",
            "in-network",
            "file_reference",
        ),
    ]


def test_parse_html_mrf_links_accepts_extensionless_dated_body_files():
    html = """
    <script id="__NEXT_DATA__" type="application/json">
    {
      "props": {
        "pageProps": {
          "links": [
            {
              "url": "https://cdn.example.test/-/media/ExamplePlan/PDF/2026-06-01_example-plan_in-network-rates_large-group-plans",
              "text": "Download JSON File"
            },
            {
              "url": "https://cdn.example.test/-/media/ExamplePlan/PDF/2026-06-01_example-plan_allowed-amounts_large-group-plans",
              "text": "Download JSON File"
            }
          ]
        }
      }
    }
    </script>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://example.test/technical-information"
    )

    assert [
        (crawl_target["url"], crawl_target["target_file_type"], crawl_target["target_kind"])
        for crawl_target in crawl_targets
    ] == [
        (
            "https://cdn.example.test/-/media/ExamplePlan/PDF/2026-06-01_example-plan_in-network-rates_large-group-plans",
            "in-network",
            "file_reference",
        ),
        (
            "https://cdn.example.test/-/media/ExamplePlan/PDF/2026-06-01_example-plan_allowed-amounts_large-group-plans",
            "allowed-amounts",
            "file_reference",
        ),
    ]


def test_parse_html_mrf_links_accepts_spaced_anchor_close_tags():
    html = """
    <a href="/files/2026-06-01_MERCYCARE-COMMERCIAL_in-network-rates.json">
      In Network
    </a    >
    <a href="/files/2026-06-01_MERCYCARE-COMMERCIAL_index.json">
      Table of Contents
    </a    >
    <a href="/files/2026-06-01_MERCYCARE-COMMERCIAL_allowed-amounts.json">
      Allowed Amounts
    </a    >
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://stmercycaremrf.z14.web.core.windows.net/in_network.html"
    )

    assert [
        (crawl_target["url"], crawl_target["target_file_type"], crawl_target["target_kind"])
        for crawl_target in crawl_targets
    ] == [
        (
            "https://stmercycaremrf.z14.web.core.windows.net/files/2026-06-01_MERCYCARE-COMMERCIAL_in-network-rates.json",
            "in-network",
            "file_reference",
        ),
        (
            "https://stmercycaremrf.z14.web.core.windows.net/files/2026-06-01_MERCYCARE-COMMERCIAL_index.json",
            "table-of-contents",
            "toc_json",
        ),
        (
            "https://stmercycaremrf.z14.web.core.windows.net/files/2026-06-01_MERCYCARE-COMMERCIAL_allowed-amounts.json",
            "allowed-amounts",
            "file_reference",
        ),
    ]


def test_parse_html_mrf_links_accepts_unquoted_href_and_gt_in_attrs():
    html = """
    <a class="download" data-title="2 > 1"
       href=/files/2026-06-01_EXAMPLE-COMMERCIAL_index.json>
      Table of Contents
    </a>
    """

    targets = discovery._parse_html_mrf_links(
        html, base_url="https://files.example.test/transparency"
    )

    assert targets == [
        {
            "url": "https://files.example.test/files/2026-06-01_EXAMPLE-COMMERCIAL_index.json",
            "label": "Table of Contents",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "href",
        }
    ]


def test_parse_html_mrf_links_extracts_ucare_landing_toc_download():
    html = """
    <p>
      <a href="https://ucm-p-001.sitecorecontenthub.cloud/api/public/content/ucare_toc.json?download=true">
        Table of Contents
      </a>
    </p>
    """

    targets = discovery._parse_html_mrf_links(
        html, base_url="https://www.ucare.org/legal-notices/transparency-in-coverage"
    )

    assert targets == [
        {
            "url": "https://ucm-p-001.sitecorecontenthub.cloud/api/public/content/ucare_toc.json?download=true",
            "label": "Table of Contents",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "href",
        }
    ]


def test_mrf_json_loader_repairs_missing_commas_between_toc_file_objects():
    toc_text = """
    {
      "reporting_entity_name": "UCare Minnesota",
      "reporting_entity_type": "Health Insurance Issuer",
      "version": "1.0.0",
      "reporting_structure": [
        {
          "reporting_plans": [
            {
              "plan_name": "UCare IFP",
              "plan_id_type": "hios",
              "plan_id": "85736MN023",
              "plan_market_type": "Individual"
            }
          ],
          "in_network_files": [
            {
              "description": "rates for UCare network",
              "location": "https://ucm-p-001.sitecorecontenthub.cloud/api/public/content/UCare_InNetwork.json?download=true"
            }
            {
              "description": "rates for dental network",
              "location": "https://ucm-p-001.sitecorecontenthub.cloud/api/public/content/Dental_InNetwork.json?download=true"
            }
          ],
          "allowed_amount_file": [
            {
              "description": "out-of-network allowed amounts",
              "location": "https://ucm-p-001.sitecorecontenthub.cloud/api/public/content/UCare_AllowedAmount.json?download=true"
            }
          ]
        }
      ]
    }
    """

    toc = discovery._loads_mrf_json_value(toc_text)
    plan_rows, file_rows = discovery._toc_rows_from_content(
        {"source_id": "source_ucare"}, "https://example.test/ucare_toc.json", toc
    )

    assert len(plan_rows) == 1
    assert [source_row["file_type"] for source_row in file_rows] == [
        "in-network",
        "in-network",
        "allowed-amounts",
    ]
    assert file_rows[1]["url"].endswith("/Dental_InNetwork.json?download=true")


def test_mrf_json_loader_repairs_toc_without_mutating_string_literals():
    toc_text = """
    {
      "reporting_entity_name": "Example Payer",
      "reporting_entity_type": "Health Insurance Issuer",
      "reporting_structure": [
        {
          "reporting_plans": [{"plan_name": "Example Plan"}],
          "in_network_files": [
            {
              "description": "keep }{ literal",
              "location": "https://files.example.test/2026-06_in-network-rates.json.gz"
            }
            {
              "description": "next file",
              "location": "https://files.example.test/2026-06_dental-in-network-rates.json.gz"
            }
          ]
        }
      ]
    }
    """

    toc = discovery._loads_mrf_json_value(toc_text)

    descriptions = [
        source_row["description"]
        for source_row in toc["reporting_structure"][0]["in_network_files"]
    ]
    assert descriptions == ["keep }{ literal", "next file"]


def test_parse_html_mrf_links_classifies_singular_table_of_content_indexes():
    html = """
    <a href="/files/McLarenHealthPlan_allowed-amount_table_of_content.json">
      allowed amounts table of content
    </a>
    <a href="/files/McLarenHealthPlan_in-network-rates_table-of-content.json">
      in-network rates table-of-content
    </a>
    <a href="/files/McLarenHealthPlan_allowed-amount.json">Allowed Amounts</a>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html,
        base_url=(
            "https://www.mclarenhealthplan.org/mhp/"
            "transparency-in-coverage-and-no-surprises-act"
        ),
    )

    assert crawl_targets == [
        {
            "url": (
                "https://www.mclarenhealthplan.org/files/"
                "McLarenHealthPlan_allowed-amount_table_of_content.json"
            ),
            "label": "allowed amounts table of content",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "href",
        },
        {
            "url": (
                "https://www.mclarenhealthplan.org/files/"
                "McLarenHealthPlan_in-network-rates_table-of-content.json"
            ),
            "label": "in-network rates table-of-content",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "href",
        },
        {
            "url": (
                "https://www.mclarenhealthplan.org/files/"
                "McLarenHealthPlan_allowed-amount.json"
            ),
            "label": "Allowed Amounts",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "allowed-amounts",
            "container_format": None,
            "html_attr": "href",
        },
    ]


def test_html_mrf_links_accept_dated_oon_index_under_mrf_path():
    html = """
    <a href="https://mrfproddestinationdata.blob.core.windows.net/avmed-mrf-output/2026-06-01_AvMed-Health-Plans-OON_index.json">
      2026-06-01_AvMed-Health-Plans-OON_index.json
    </a>
    """

    targets = discovery._parse_html_mrf_links(
        html,
        base_url=(
            "https://mrfproddestinationdata.blob.core.windows.net/"
            "avmed-mrf-output/AvMed-Health-Plans-OON_index.html"
        ),
    )

    assert targets == [
        {
            "url": (
                "https://mrfproddestinationdata.blob.core.windows.net/"
                "avmed-mrf-output/2026-06-01_AvMed-Health-Plans-OON_index.json"
            ),
            "label": "2026-06-01_AvMed-Health-Plans-OON_index.json",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "href",
        }
    ]


def test_html_mrf_links_accept_plan_named_tic_index():
    html = """
    <a href="/mrf/thp/2026-06-01_The-Health-Plan-of-WV-Inc_index.json">
      2026-06-01_The-Health-Plan-of-WV-Inc_index.json
    </a>
    """

    targets = discovery._parse_html_mrf_links(
        html, base_url="https://www.healthplan.org/thp_mrfs"
    )

    assert targets == [
        {
            "url": (
                "https://www.healthplan.org/mrf/thp/"
                "2026-06-01_The-Health-Plan-of-WV-Inc_index.json"
            ),
            "label": "2026-06-01_The-Health-Plan-of-WV-Inc_index.json",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "href",
        }
    ]


def test_parse_html_mrf_links_uses_section_context_for_split_body_files():
    """Verify this source-discovery regression contract."""
    html = """
    <h2>Alliance Group Care Price Transparency Machine-Readable Files</h2>
    <p>The files below detail costs for items and services.</p>
    <table>
      <tr><td colspan="3">In-network provider rates for covered items and services</td></tr>
      <tr>
        <td>1-5000000_output.json.gz</td>
        <td>631.07MB</td>
        <td>
          <a href="https://cmspt.blob.core.windows.net/cms-pricing/1-1000000_output.json.gz">
            https://cmspt.blob.core.windows.net/cms-pricing/1-1000000_output.json.gz
          </a>
        </td>
      </tr>
      <tr><td colspan="3">Out-of-network allowed amounts and billed charges</td></tr>
      <tr>
        <td>allowed_amt_details_20250401.json</td>
        <td>82.19KB</td>
        <td>
          <a href="https://cmspt.blob.core.windows.net/cms-pricing/allowed_amt_details_20250401.json">
            https://cmspt.blob.core.windows.net/cms-pricing/allowed_amt_details_20250401.json
          </a>
        </td>
      </tr>
    </table>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://alamedaalliance.org/about/pricing-transparency/"
    )

    assert crawl_targets == [
        {
            "url": "https://cmspt.blob.core.windows.net/cms-pricing/1-1000000_output.json.gz",
            "label": "1000000 Output",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "gzip",
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "1000000 Output",
                }
            ],
        },
        {
            "url": "https://cmspt.blob.core.windows.net/cms-pricing/allowed_amt_details_20250401.json",
            "label": "Amt Details 20250401",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "allowed-amounts",
            "container_format": None,
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "Amt Details 20250401",
                }
            ],
        },
    ]


def test_parse_html_mrf_links_uses_neighbor_label_for_zakipoint_rows():
    html = """
    <div class="network-row">
      <a href="https://mrf-public-collection.s3.us-east-1.amazonaws.com/boonchapman/aetna/aetna.gz">
        Aetna Signature Administrators
      </a>
      <span class="label">In-network</span>
    </div>
    <div class="network-row">
      <a href="https://mrf-public-collection.s3.amazonaws.com/boonchapman/allowed_amount/division_id=002429/002429.zip">
        002429
      </a>
      <span class="label">Out of network</span>
    </div>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://boonchapman-mrf.zakipointhealth.com/"
    )

    assert [(link_item["label"], link_item["target_file_type"]) for link_item in crawl_targets] == [
        ("Aetna Signature Administrators", "in-network"),
        ("002429", "allowed-amounts"),
    ]
    assert crawl_targets[0]["plan_info"] == [
        {
            "plan_id": None,
            "plan_id_type": None,
            "plan_market_type": "group",
            "plan_name": "Aetna Signature Administrators",
        }
    ]


def test_parse_html_mrf_links_ignores_static_asset_gzip_files():
    html = """
    <script src="/js/bundles/HeaderJS.min.js.gz"></script>
    <script src="/js/bundles/FooterJS.min.js.gz"></script>
    <a href="/Legal/business-transparency/Out-of-network-liability-and-balance-billing">
      Out-of-network liability and balance billing
    </a>
    <a href="/mrf/2026-06-01_example_in-network-rates.zip">In Network ZIP</a>
    """

    targets = discovery._parse_html_mrf_links(
        html, base_url="https://example.test/transparency-in-coverage"
    )

    assert [target["url"] for target in targets] == [
        "https://example.test/mrf/2026-06-01_example_in-network-rates.zip"
    ]


def test_html_mrf_links_treats_rate_directories_as_directories_not_files():
    html = """
    <a href="https://apps.example.test/PriceTransparency/HealthPlan/InNetwork/">
      In-Network Negotiated Rates
    </a>
    <a href="https://apps.example.test/PriceTransparency/HealthPlan/OutOfNetwork/">
      Out-of-Network File Amounts
    </a>
    """

    targets = discovery._parse_html_mrf_links(
        html, base_url="https://example.test/transparency-in-coverage"
    )
    directory_urls = discovery._html_mrf_directory_urls(
        html, base_url="https://example.test/transparency-in-coverage"
    )

    assert targets == []
    assert directory_urls == [
        "https://apps.example.test/PriceTransparency/HealthPlan/InNetwork/",
        "https://apps.example.test/PriceTransparency/HealthPlan/OutOfNetwork/",
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

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://example.com/transparency"
    )

    assert crawl_targets == [
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


def test_parse_html_mrf_links_extracts_embedded_escaped_relative_urls():
    html = r"""
    <script>
    window.__DATA__ = {
      "toc": "\/mrf\/current\/2026-06-01_example_index.json",
      "rates": "/mrf/current/2026-06-01_example_in-network-rates.json.gz"
    };
    </script>
    """

    targets = discovery._parse_html_mrf_links(
        html, base_url="https://payer.example.test/transparency"
    )

    assert [
        (target["url"], target["target_kind"], target["target_file_type"])
        for target in targets
    ] == [
        (
            "https://payer.example.test/mrf/current/2026-06-01_example_index.json",
            "toc_json",
            "table-of-contents",
        ),
        (
            "https://payer.example.test/mrf/current/2026-06-01_example_in-network-rates.json.gz",
            "file_reference",
            "in-network",
        ),
    ]


def test_parse_html_mrf_links_extracts_data_key_toc_urls():
    html = """
    <a class="download" href="javascript:void(0);"
       data-key="https://files.example.test/mrf/2026-06-01_example hmo_index.json">
      Download
    </a>
    """

    targets = discovery._parse_html_mrf_links(
        html, base_url="https://example.com/machine-readable-files"
    )

    assert targets == [
        {
            "url": "https://files.example.test/mrf/2026-06-01_example hmo_index.json",
            "label": "2026-06-01_example hmo_index.json",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "data-key",
        }
    ]


def test_parse_html_mrf_links_extracts_price_transparency_index_suffix():
    html = """
    <a href="https://example.test/json/pt/2026-05-08_example-plan_index_nlc.json">
      Latest price transparency index file
    </a>
    """

    targets = discovery._parse_html_mrf_links(
        html, base_url="https://example.test/json/pt/latest.html"
    )

    assert targets == [
        {
            "url": "https://example.test/json/pt/2026-05-08_example-plan_index_nlc.json",
            "label": "Latest price transparency index file",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "href",
        }
    ]


def test_parse_html_mrf_links_extracts_sharp_direct_zip_files():
    """Verify this source-discovery regression contract."""
    html = """
    <a href="/docs/default-source/price-transparency/2026-06-tableofcontents.zip">
      Table of Contents
    </a>
    <a href="/docs/default-source/price-transparency/2026-06-allowed_amounts.zip">
      Allowed Amounts
    </a>
    <a href="https://docs.sharphealthplan.com/shp-documents/doc/2026-06-IN_NETWORK_CAP.zip">
      In-Network - CAP
    </a>
    <a href="https://docs.sharphealthplan.com/shp-documents/doc/2026-06-IN_NETWORK_FFS.zip">
      In-Network - FFS
    </a>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://www.sharphealthplan.com/api-access-for-developers"
    )

    assert crawl_targets == [
        {
            "url": "https://www.sharphealthplan.com/docs/default-source/price-transparency/2026-06-allowed_amounts.zip",
            "label": "Allowed Amounts",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "allowed-amounts",
            "container_format": "zip",
            "html_attr": "href",
        },
        {
            "url": "https://docs.sharphealthplan.com/shp-documents/doc/2026-06-IN_NETWORK_CAP.zip",
            "label": "In-Network - CAP",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "zip",
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "In-Network - CAP",
                }
            ],
        },
        {
            "url": "https://docs.sharphealthplan.com/shp-documents/doc/2026-06-IN_NETWORK_FFS.zip",
            "label": "In-Network - FFS",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "zip",
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "In-Network - FFS",
                }
            ],
        },
    ]


def test_parse_html_mrf_links_extracts_group_health_eau_claire_json_files():
    """Verify this source-discovery regression contract."""
    html = """
    <p>
      <a href="/getmedia/fa7482cb-b902-415a-8779-f4abfa2f6bd5/2024-06-03_group-health-cooperative-of-eau-claire_medicaid_in-network-rates.json">
        Medicaid JSON
      </a>
      <a href="/getmedia/55db35a9-0f8c-4bed-acae-7236b11be506/2024-06-03_group-health-cooperative-of-eau-claire_medicare_in-network-rates.json">
        Medicare JSON
      </a>
      <a href="/getmedia/1cda96fc-8e5e-40d3-acd1-a3dd203a730c/2024-06-03_group-health-cooperative-of-eau-claire_commercial_in-network-rates.json">
        Commercial JSON
      </a>
      <a href="/getmedia/e797fd3a-2708-490f-badb-61468ddab7a6/2024-02-05_group-health-cooperative-of-eau-claire_commercial_allowed-amounts.json">
        Commercial Allowed Amounts JSON
      </a>
    </p>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://group-health.com/price-transparency"
    )

    assert [crawl_target["target_file_type"] for crawl_target in crawl_targets] == [
        "in-network",
        "in-network",
        "in-network",
        "allowed-amounts",
    ]
    assert [crawl_target["label"] for crawl_target in crawl_targets] == [
        "Medicaid JSON",
        "Medicare JSON",
        "Commercial JSON",
        "Commercial Allowed Amounts JSON",
    ]
    assert [crawl_target.get("plan_info") for crawl_target in crawl_targets] == [
        [
            {
                "plan_id": None,
                "plan_id_type": None,
                "plan_market_type": "group",
                "plan_name": "Medicaid JSON",
            }
        ],
        [
            {
                "plan_id": None,
                "plan_id_type": None,
                "plan_market_type": "group",
                "plan_name": "Medicare JSON",
            }
        ],
        [
            {
                "plan_id": None,
                "plan_id_type": None,
                "plan_market_type": "group",
                "plan_name": "Commercial JSON",
            }
        ],
        None,
    ]
    assert all(crawl_target["target_kind"] == "file_reference" for crawl_target in crawl_targets)


def test_parse_html_mrf_links_accepts_extensionless_toc_and_oon_files():
    html = """
    <a href="https://files.example.test/mrf-files/opaque-token">
      Download TOC Index File
    </a>
    <a href="https://files.example.test/mrf/2026-06-01_example_OON.json">
      Out-of-Network allowed amounts
    </a>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://example.test/transparency"
    )

    assert crawl_targets == [
        {
            "url": "https://files.example.test/mrf-files/opaque-token",
            "label": "Download TOC Index File",
            "resolver": "html_mrf_link",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": None,
            "html_attr": "href",
        },
        {
            "url": "https://files.example.test/mrf/2026-06-01_example_OON.json",
            "label": "Out-of-Network allowed amounts",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "allowed-amounts",
            "container_format": None,
            "html_attr": "href",
        },
    ]


def test_parse_html_mrf_links_accepts_opaque_download_body_files():
    html = """
    <a class="pdf" href="/documents/getmachinereadablefile/5294676"
       title="2024-01-24_PEHP_in-network-rates">
      2024-01-24_PEHP_in-network-rates
    </a>
    <a class="pdf" href="/documents/getmachinereadablefile/4095233">
      2023-01-26_PEHP_OON_allowed-amounts
    </a>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://www.pehp.org/machinereadablefiles"
    )

    assert crawl_targets == [
        {
            "url": "https://www.pehp.org/documents/getmachinereadablefile/5294676",
            "label": "2024-01-24_PEHP_in-network-rates",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": None,
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "2024-01-24_PEHP_in-network-rates",
                }
            ],
        },
        {
            "url": "https://www.pehp.org/documents/getmachinereadablefile/4095233",
            "label": "2023-01-26_PEHP_OON_allowed-amounts",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "allowed-amounts",
            "container_format": None,
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "2023-01-26_PEHP_OON_allowed-amounts",
                }
            ],
        },
    ]


def test_parse_html_mrf_links_accepts_query_named_download_body_files():
    html = """
    <a href="/Home/GetFile?FileName=2026-05-01_Emblemhealth_041663150_allowed-amounts.json&NetworkType=OON&FileType=Current">
      2026-05-01_Emblemhealth_041663150_allowed-amounts.json
    </a>
    <a href="/Home/GetFile?FileName=2026-05-01_QualCare_EmblemHealth_PPO_in-network-rates0.zip&NetworkType=INN&FileType=Current">
      2026-05-01_QualCare_EmblemHealth_PPO_in-network-rates0.zip
    </a>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://transparency.emblemhealth.com/OON"
    )

    assert crawl_targets == [
        {
            "url": (
                "https://transparency.emblemhealth.com/Home/GetFile?"
                "FileName=2026-05-01_Emblemhealth_041663150_allowed-amounts.json"
                "&NetworkType=OON&FileType=Current"
            ),
            "label": "2026-05-01_Emblemhealth_041663150_allowed-amounts.json",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "allowed-amounts",
            "container_format": None,
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "2026-05-01_Emblemhealth_041663150_allowed-amounts.json",
                }
            ],
        },
        {
            "url": (
                "https://transparency.emblemhealth.com/Home/GetFile?"
                "FileName=2026-05-01_QualCare_EmblemHealth_PPO_in-network-rates0.zip"
                "&NetworkType=INN&FileType=Current"
            ),
            "label": "2026-05-01_QualCare_EmblemHealth_PPO_in-network-rates0.zip",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "zip",
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "2026-05-01_QualCare_EmblemHealth_PPO_in-network-rates0.zip",
                }
            ],
        },
    ]


def test_html_mrf_directory_links_include_connecticare_style_network_folders():
    html = """
    <a href="github.com/CMSgov/price-transparency-guide">CMS guide</a>
    <a href="/INN">In-Network</a>
    <a href="/OON">Out-Of-Network</a>
    """

    urls = discovery._html_mrf_directory_urls(
        html, base_url="https://transparency.example.test/"
    )

    assert "https://transparency.example.test/INN" in urls
    assert "https://transparency.example.test/OON" in urls


def test_parse_html_mrf_links_accepts_zipped_table_of_contents_files():
    html = """
    <a href="https://shp-web-public.s3.amazonaws.com/PRD/table-of-contents/2026-06-01_SECURITY-HEALTH-PLAN_index.zip">
      2026-06-01_SECURITY-HEALTH-PLAN_index.zip
    </a>
    <a href="https://priorityhealthtransparencymrfs.s3.us-east-1.amazonaws.com/2026_06_01_priority_health_index.zip">
      2026_06_01_priority_health_index.zip
    </a>
    <a href="https://shp-web-public.s3.amazonaws.com/PRD/exchange/cms-data-index.json">
      cms-data-index.json
    </a>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html, base_url="https://www.securityhealth.org/insurance-resources/json"
    )

    assert crawl_targets == [
        {
            "url": (
                "https://shp-web-public.s3.amazonaws.com/PRD/table-of-contents/"
                "2026-06-01_SECURITY-HEALTH-PLAN_index.zip"
            ),
            "label": "Health Plan Index",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "table-of-contents",
            "container_format": "zip",
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "Health Plan Index",
                }
            ],
        },
        {
            "url": (
                "https://priorityhealthtransparencymrfs.s3.us-east-1.amazonaws.com/"
                "2026_06_01_priority_health_index.zip"
            ),
            "label": "06 01 Priority Health Index",
            "resolver": "html_file_reference",
            "target_kind": "file_reference",
            "target_file_type": "table-of-contents",
            "container_format": "zip",
            "html_attr": "href",
            "plan_info": [
                {
                    "plan_id": None,
                    "plan_id_type": None,
                    "plan_market_type": "group",
                    "plan_name": "06 01 Priority Health Index",
                }
            ],
        },
    ]


def test_json_values_from_zip_bytes_reads_zipped_toc_member():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("readme.txt", "not json")
        archive.writestr(
            "2026-06-01_example_index.json",
            json.dumps({"reporting_entity_name": "Example Payer"}),
        )

    values = discovery._json_values_from_zip_bytes(buffer.getvalue())

    assert values == [
        (
            "2026-06-01_example_index.json",
            {"reporting_entity_name": "Example Payer"},
        )
    ]


def test_json_values_from_zip_bytes_repairs_zipped_toc_member():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "2026-06-01_example_index.json",
            """
            {
              "reporting_entity_name": "Example Payer",
              "reporting_entity_type": "payer",
              "reporting_structure": [
                {
                  "reporting_plans": [{"plan_name": "Example Plan"}],
                  "in_network_files": [
                    {"location": "https://files.example.test/a_in-network-rates.json.gz"}
                    {"location": "https://files.example.test/b_in-network-rates.json.gz"}
                  ]
                }
              ]
            }
            """,
        )

    values = discovery._json_values_from_zip_bytes(buffer.getvalue())

    assert len(values[0][1]["reporting_structure"][0]["in_network_files"]) == 2


def test_parse_html_mrf_links_ignores_provider_formulary_indexes():
    html = """
    <a href="https://files.example.test/provider-data/cms-data-index.json">
      Download Plan, Provider and Formulary index file
    </a>
    <a href="https://www.alliantplans.com/json/ProvidersGA.json">ProvidersGA.json</a>
    <a href="https://www.alliantplans.com/json/Plans_GA_2026.json">Plans_GA_2026.json</a>
    <a href="https://www.alliantplans.com/json/Formulary_GA_2026.json">Formulary_GA_2026.json</a>
    <a href="https://enroll.pacificsource.com/MRF/ID/drugs.json">Drugs</a>
    <a href="https://enroll.pacificsource.com/MRF/ID/plans.json">Plans</a>
    <a href="https://enroll.pacificsource.com/MRF/ID/providers.json">Providers</a>
    <a href="https://example.test/hospital-price-transparency/standardcharges.csv">
      standardcharges.csv
    </a>
    """

    targets = discovery._parse_html_mrf_links(
        html, base_url="https://example.test/interoperability"
    )

    assert targets == []


def test_mrf_body_rejects_fee_schedule():
    assert (
        discovery._mrf_body_file_type_from_text(
            "https://files.example.test/mrf/medical-fee-schedule.csv",
            "In-network rates",
        )
        is None
    )
    assert (
        discovery._mrf_body_file_type_from_text(
            "https://files.example.test/mrf/example-fee-schedule.xlsx",
            "Negotiated rates",
        )
        is None
    )
    assert (
        discovery._mrf_body_file_type_from_text(
            "https://files.example.test/mrf/example-in-network-rates.csv",
            "In-network rates",
        )
        == "in-network"
    )


def test_html_mrf_rejects_fee_schedule():
    html = """
    <section>
      <h2>In Network Rates</h2>
      <a href="/mrf/example-medical-fee-schedule.csv">In-network fee schedule</a>
      <a href="/mrf/example-dental-fee-schedule.xlsx">Negotiated rates workbook</a>
      <a href="/mrf/example-in-network-rates.json.gz">In-network rates</a>
    </section>
    """

    targets = discovery._parse_html_mrf_links(
        html, base_url="https://example.test/machine-readable-files"
    )

    assert [
        (target["url"], target["target_file_type"], target["target_kind"])
        for target in targets
    ] == [
        (
            "https://example.test/mrf/example-in-network-rates.json.gz",
            "in-network",
            "file_reference",
        )
    ]


def test_html_mrf_directory_urls_extracts_clear_directory_links():
    html = """
    <a href="https://files.example.test/mrf/plan/TOC/">TOC</a>
    <a href="https://files.example.test/mrf/plan/InNetwork/">In Network</a>
    <script>
      window.__MRF__ = {
        "index": "https://mrfproddestinationdata.blob.core.windows.net/mrf-output/Example_In-Network_MRF_Index.html"
      };
    </script>
    <a href="/pricetransparency/MRF/Base">
      Click here to access machine-readable files.
    </a>
    <a href="/thp_mrfs">THP</a>
    <a href="http://20.114.211.146/CHP/">CHP Machine Readable Files</a>
    <a href="https://www.cms.gov/healthplan-price-transparency">CMS guidance</a>
    <a href="https://github.com/CMSgov/price-transparency-guide">CMS code</a>
    <a href="https://github.com/CMSgov/price-transparency-guide/">
      CMS machine-readable schema guide
    </a>
    <a href="/members/MRF%20FAQs.pdf">Download MRF FAQ</a>
    <a href="https://files.example.test/assets/">Assets</a>
    """

    urls = discovery._html_mrf_directory_urls(
        html, base_url="https://example.test/machine-readable-files"
    )

    assert urls == [
        "https://files.example.test/mrf/plan/TOC/",
        "https://files.example.test/mrf/plan/InNetwork/",
        "https://example.test/pricetransparency/MRF/Base",
        "https://example.test/thp_mrfs",
        "http://20.114.211.146/CHP/",
        "https://mrfproddestinationdata.blob.core.windows.net/mrf-output/Example_In-Network_MRF_Index.html",
    ]


def test_html_mrf_directory_urls_extracts_ehp_month_autoindex_links():
    html = """
    <h1>Index of /</h1>
    <a href="/April_2026/">April_2026</a>
    <a href="/May_2026/">May_2026</a>
    <a href="/_autoindex/assets/css/autoindex.css">autoindex.css</a>
    """

    urls = discovery._html_mrf_directory_urls(
        html,
        base_url="https://ehptransparency.org/",
    )

    assert urls == [
        "https://ehptransparency.org/April_2026/",
        "https://ehptransparency.org/May_2026/",
    ]


def test_parse_html_mrf_links_extracts_ehp_autoindex_files():
    html = """
    <h1>Index of /May_2026/</h1>
    <a href="/">Parent Directory</a>
    <a href="/May_2026/Providers/">Providers</a>
    <a href="/May_2026/2026-05-01_jhhc_ehp_allowed-amounts.json">
      2026-05-01_jhhc_ehp_allowed-amounts.json
    </a>
    <a href="/May_2026/2026-05-01_jhhc_ehp_table-of-contents.json">
      2026-05-01_jhhc_ehp_table-of-contents.json
    </a>
    <a href="/May_2026/2026-05-01_jhhc_ehp_table-of-contents.zip">
      2026-05-01_jhhc_ehp_table-of-contents.zip
    </a>
    <a href="/May_2026/innetwork-G-EHPCAREMARK-file-1.json">
      innetwork-G-EHPCAREMARK-file-1.json
    </a>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html,
        base_url="https://ehptransparency.org/May_2026/",
    )

    assert [
        (crawl_target["url"], crawl_target["target_kind"], crawl_target["target_file_type"])
        for crawl_target in crawl_targets
    ] == [
        (
            "https://ehptransparency.org/May_2026/2026-05-01_jhhc_ehp_allowed-amounts.json",
            "file_reference",
            "allowed-amounts",
        ),
        (
            "https://ehptransparency.org/May_2026/2026-05-01_jhhc_ehp_table-of-contents.json",
            "toc_json",
            "table-of-contents",
        ),
        (
            "https://ehptransparency.org/May_2026/2026-05-01_jhhc_ehp_table-of-contents.zip",
            "file_reference",
            "table-of-contents",
        ),
        (
            "https://ehptransparency.org/May_2026/innetwork-G-EHPCAREMARK-file-1.json",
            "file_reference",
            "in-network",
        ),
    ]


def test_parse_html_mrf_links_extracts_myplancentral_gzip_tocs():
    html = """
    <h1>Index of /TIC/TOC/</h1>
    <a href="../">../</a>
    <a href="2026-06-02_SummaCare_index.json.gz">
      2026-06-02_SummaCare_index.json.gz
    </a>
    <a href="2026-06-02_SummaCare_MEWA_index.json.gz">
      2026-06-02_SummaCare_MEWA_index.json.gz
    </a>
    """

    crawl_targets = discovery._parse_html_mrf_links(
        html,
        base_url="https://files.myplancentral.com/TIC/TOC/",
    )

    assert [
        (crawl_target["url"], crawl_target["target_file_type"], crawl_target["container_format"])
        for crawl_target in crawl_targets
    ] == [
        (
            "https://files.myplancentral.com/TIC/TOC/2026-06-02_SummaCare_index.json.gz",
            "table-of-contents",
            "gzip",
        ),
        (
            "https://files.myplancentral.com/TIC/TOC/2026-06-02_SummaCare_MEWA_index.json.gz",
            "table-of-contents",
            "gzip",
        ),
    ]


def test_html_mrf_directory_urls_stop_embedded_urls_at_escaped_html_boundaries():
    html = r"""
    <script>
      window.__HTML__ = "\u003Ca href=\"https:\/\/mrfproddestinationdata.blob.core.windows.net\/sentara-mrf-output\/Sentara_HMO_in-network-rates_MRF.html\"\u003EView\u003C\/a\u003E";
    </script>
    """

    urls = discovery._html_mrf_directory_urls(
        html, base_url="https://www.example.test/transparency"
    )

    assert urls == [
        "https://mrfproddestinationdata.blob.core.windows.net/sentara-mrf-output/Sentara_HMO_in-network-rates_MRF.html"
    ]


def test_html_mrf_frame_urls_extracts_mrf_iframe_pages():
    html = """
    <iframe src="/mrfhtml/mrf2023082301.html?parm=2023082302"></iframe>
    <iframe src="/marketing/widget.html"></iframe>
    """

    urls = discovery._html_mrf_frame_urls(
        html, base_url="https://www.groupadministrators.com/machinereadablefiles/"
    )

    assert urls == [
        "https://www.groupadministrators.com/mrfhtml/mrf2023082301.html?parm=2023082302"
    ]


def test_s3_xml_listing_targets_extract_current_mrf_files_only():
    source_dict = {"source_id": "source_1", "display_name": "LA Care Health Plan"}
    xml_text = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Contents>
        <Key>archive/03312023/innetwork-old.json</Key>
        <LastModified>2023-03-31T23:57:51.000Z</LastModified>
        <ETag>&quot;old&quot;</ETag>
        <Size>123</Size>
      </Contents>
      <Contents>
        <Key>index/2026-06-01_LACare_index.json</Key>
        <LastModified>2026-06-16T17:50:00.000Z</LastModified>
        <ETag>&quot;idx&quot;</ETag>
        <Size>456</Size>
      </Contents>
      <Contents>
        <Key>download/S-CT00000004545-innetwork-1.json</Key>
        <LastModified>2026-06-16T17:53:46.000Z</LastModified>
        <ETag>&quot;inn&quot;</ETag>
        <Size>789</Size>
      </Contents>
      <Contents>
        <Key>download/2026-06-01_LACare_allowed-amounts.json</Key>
        <LastModified>2026-06-16T17:53:47.000Z</LastModified>
        <ETag>&quot;oon&quot;</ETag>
        <Size>987</Size>
      </Contents>
    </ListBucketResult>
    """

    mrf_targets = discovery._s3_xml_listing_targets_from_xml(
        source_dict,
        xml_text,
        listing_url="https://lac-transparency-prod.s3.amazonaws.com",
        resolver={
            "type": "s3_xml_listing",
            "public_base_url": "https://transparency.lacare.org",
            "include_prefixes": ["index/", "download/"],
        },
    )

    assert [mrf_target.url for mrf_target in mrf_targets] == [
        "https://transparency.lacare.org/index/2026-06-01_LACare_index.json",
        "https://transparency.lacare.org/download/S-CT00000004545-innetwork-1.json",
        "https://transparency.lacare.org/download/2026-06-01_LACare_allowed-amounts.json",
    ]
    assert [mrf_target.metadata["target_file_type"] for mrf_target in mrf_targets] == [
        "table-of-contents",
        "in-network",
        "allowed-amounts",
    ]
    assert mrf_targets[0].metadata["content_length"] == "456"
    assert mrf_targets[0].metadata["etag"] == "idx"


@pytest.mark.asyncio
async def test_s3_xml_listing_resolver_caps_newest_targets_first(monkeypatch):
    source_dict = {"source_id": "source_1", "display_name": "LA Care Health Plan"}
    xml_text = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Contents>
        <Key>download/old-innetwork.json</Key>
        <LastModified>2025-06-16T17:53:46.000Z</LastModified>
        <ETag>&quot;old&quot;</ETag>
        <Size>123</Size>
      </Contents>
      <Contents>
        <Key>download/new-innetwork.json</Key>
        <LastModified>2026-06-16T17:53:46.000Z</LastModified>
        <ETag>&quot;new&quot;</ETag>
        <Size>456</Size>
      </Contents>
    </ListBucketResult>
    """

    async def fake_fetch_text(*_args, **_kwargs):
        return xml_text

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    mrf_targets = await discovery._resolve_s3_xml_listing(
        source_dict,
        "https://transparency.lacare.org",
        {
            "type": "s3_xml_listing",
            "listing_urls": ["https://lac-transparency-prod.s3.amazonaws.com"],
            "public_base_url": "https://transparency.lacare.org",
            "include_prefixes": ["download/"],
            "max_targets": 1,
        },
        None,
    )

    assert [mrf_target.url for mrf_target in mrf_targets] == [
        "https://transparency.lacare.org/download/new-innetwork.json"
    ]
    assert mrf_targets[0].metadata["last_modified"] == "2026-06-16T17:53:46.000Z"


def test_healthspace_session_id_from_html_extracts_public_session():
    html = """
    <script>
      window.sessionStorage.setItem('HealthspaceSessionId', '123456789-987654321');
    </script>
    """

    assert discovery._healthspace_session_id_from_html(html) == "123456789-987654321"


def test_healthspace_soap_targets_extract_mrf_files_only():
    """Verify this source-discovery regression contract."""
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "90 Degree Benefits",
    }
    soap_text = """
    <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Body>
        <ExecuteResponse xmlns="https://www.p2phealthcare.com">
          <ExecuteResult>
            <MachineReadableFiles xmlns="">
              <MachineReadableFile
                CompanyId="company-1"
                CompanyName="123456789"
                FileName="2026-06-01_example-plan_Index.ZIP"
                FilePathURL="https://mrfexport.blob.core.windows.net/caa/2026-06-01_example-plan_Index.ZIP" />
              <MachineReadableFile
                CompanyId="company-2"
                CompanyName="987654321"
                FileName="2026-06-01_example-plan_in-network-rates.zip"
                FilePathURL="https://mrfexport.blob.core.windows.net/caa/2026-06-01_example-plan_in-network-rates.zip" />
              <MachineReadableFile
                CompanyId="company-3"
                CompanyName=""
                FileName="2026-06-01_example-plan_out-network-rates.zip"
                FilePathURL="https://mrfexport.blob.core.windows.net/caa/2026-06-01_example-plan_out-network-rates.zip" />
              <MachineReadableFile
                CompanyId="company-4"
                CompanyName="ignore"
                FileName="provider-data.json"
                FilePathURL="https://mrfexport.blob.core.windows.net/caa/provider-data.json" />
            </MachineReadableFiles>
          </ExecuteResult>
        </ExecuteResponse>
      </s:Body>
    </s:Envelope>
    """

    mrf_targets = discovery._healthspace_mrf_targets_from_soap(
        source_dict,
        soap_text,
        resolved_from_url="https://portal.example.test/Healthspace/Healthspace.svc",
        resolver={"type": "healthspace_machine_readable_files"},
    )

    assert [mrf_target.metadata["target_file_type"] for mrf_target in mrf_targets] == [
        "table-of-contents",
        "in-network",
        "allowed-amounts",
    ]
    assert [mrf_target.metadata["target_kind"] for mrf_target in mrf_targets] == [
        "file_reference",
        "file_reference",
        "file_reference",
    ]
    assert mrf_targets[0].metadata["container_format"] == "zip"
    assert mrf_targets[0].metadata["company_id"] == "company-1"
    assert mrf_targets[0].metadata["plan_info"] == [
        {
            "plan_id": "123456789",
            "plan_id_type": "healthspace_company_name",
            "plan_market_type": "group",
            "plan_name": "123456789",
        }
    ]
    assert mrf_targets[2].metadata["plan_info"] == []


@pytest.mark.asyncio
async def test_healthspace_resolver_posts_execute_soap_and_caps_targets(monkeypatch):
    """Verify this source-discovery regression contract."""
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "90 Degree Benefits",
    }
    page_html = """
    <script>
      window.sessionStorage.setItem('HealthspaceSessionId', '123456789-987654321');
    </script>
    """
    soap_text = """
    <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Body>
        <ExecuteResponse xmlns="https://www.p2phealthcare.com">
          <ExecuteResult>
            <MachineReadableFiles xmlns="">
              <MachineReadableFile
                CompanyId="company-1"
                CompanyName="123456789"
                FileName="2026-06-01_example-plan_in-network-rates.zip"
                FilePathURL="https://mrfexport.blob.core.windows.net/caa/2026-06-01_example-plan_in-network-rates.zip" />
              <MachineReadableFile
                CompanyId="company-2"
                CompanyName="987654321"
                FileName="2026-06-01_other-plan_in-network-rates.zip"
                FilePathURL="https://mrfexport.blob.core.windows.net/caa/2026-06-01_other-plan_in-network-rates.zip" />
            </MachineReadableFiles>
          </ExecuteResult>
        </ExecuteResponse>
      </s:Body>
    </s:Envelope>
    """
    post_calls = []

    async def fake_fetch_text(*_args, **_kwargs):
        return page_html

    async def fake_post_text(url, payload, **kwargs):
        post_calls.append(
            {"url": url, "payload": payload, "headers": kwargs["headers"]}
        )
        return soap_text

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(discovery, "_post_text", fake_post_text)

    mrf_targets = await discovery._resolve_healthspace_machine_readable_files(
        source_dict,
        "https://portal.90degreebenefits.com/MemberPortal/MachineReadableFiles",
        {
            "type": "healthspace_machine_readable_files",
            "service_path": "/Healthspace/Healthspace.svc",
            "operation_id": "P2PHC.Document.GetMachineReadableFiles",
            "parameters_xml": "<hslist />",
            "max_targets": 1,
        },
        None,
    )

    assert [mrf_target.url for mrf_target in mrf_targets] == [
        "https://mrfexport.blob.core.windows.net/caa/2026-06-01_example-plan_in-network-rates.zip"
    ]
    assert post_calls[0]["url"] == (
        "https://portal.90degreebenefits.com/Healthspace/Healthspace.svc"
    )
    assert (
        post_calls[0]["headers"]["SOAPAction"]
        == "https://www.p2phealthcare.com/IHealthspace/Execute"
    )
    assert (
        "<ns0:operationId>P2PHC.Document.GetMachineReadableFiles</ns0:operationId>"
        in (post_calls[0]["payload"])
    )
    assert "<hslist />" in post_calls[0]["payload"]


def test_delegated_mrf_source_urls_extracts_supported_links_and_bare_hosts():
    html = """
    <a href="https://www.anthem.com/machine-readable-file/search">Anthem</a>
    <a href="https://bcbsil.com/asomrf?EIN=300088171">BCBS ASO</a>
    <p>MRF Hub: alliedbenefit.sapphiremrfhub.com.</p>
    <p>Delegated TPA: mrf.healthcarebluebook.com/Lucent.</p>
    <p>Health1 source: health1.aetna.com/app/public/#/one/insurerCode=EXAMPLE_I&amp;brandCode=EXAMPLE/machine-readable-transparency-in-coverage.</p>
    <p>HealthSparq metadata: mrf.healthsparq.com/example-egress.nophi.kyruushsq.com/prd/mrf/EXAMPLE_I/EXAMPLE/latest_metadata.json.</p>
    <p>TALON search: www.mymedicalshopper.com/mrf-search/varipro.</p>
    <p>TALON employer: www.mymedicalshopper.com/mrf/example-varipro-77100.</p>
    <p>ASR groups: www.asrhealthbenefits.com/MRF.</p>
    <p>PayerCompass: example.mrf.payercompass.com.</p>
    <p>Cigna compliance: www.cigna.com/legal/compliance/machine-readable-files.</p>
    <p>CMSTIC search: www.ibx.com/cmstic/?brand=qcc.</p>
    <p>WebTPA API: price-transparency.webtpa.com.</p>
    <a href="https://example.com/not-mrf">Ignore</a>
    <a href="https://github.com/CMSgov/price-transparency-guide">CMS docs</a>
    """

    urls = discovery._delegated_mrf_source_urls_from_html(
        html, base_url="https://www.pbaclaims.com/mrfs/"
    )

    assert urls == [
        "https://www.anthem.com/machine-readable-file/search",
        "https://bcbsil.com/asomrf?EIN=300088171",
        "https://alliedbenefit.sapphiremrfhub.com/",
        "https://mrf.healthcarebluebook.com/Lucent",
        "https://example.mrf.payercompass.com/",
        "https://www.cigna.com/legal/compliance/machine-readable-files",
        "https://www.ibx.com/cmstic/?brand=qcc",
        "https://health1.aetna.com/app/public/#/one/insurerCode=EXAMPLE_I&brandCode=EXAMPLE/machine-readable-transparency-in-coverage",
        "https://mrf.healthsparq.com/example-egress.nophi.kyruushsq.com/prd/mrf/EXAMPLE_I/EXAMPLE/latest_metadata.json",
        "https://www.mymedicalshopper.com/mrf-search/varipro",
        "https://www.mymedicalshopper.com/mrf/example-varipro-77100",
        "https://www.asrhealthbenefits.com/MRF",
        "https://price-transparency.webtpa.com/",
    ]


@pytest.mark.asyncio
async def test_crawl_targets_for_source_delegates_plain_mrf_host_text(monkeypatch):
    async def fake_fetch_text(url, *, max_bytes, session):
        assert url == "https://wrapper.example/mrf"
        assert max_bytes == 5 * 1024 * 1024
        assert session is fake_session
        return "<p>See current MRFs at mrf.healthcarebluebook.com/Lucent.</p>"

    async def fake_resolve_healthcarebluebook_mrf(source, url, resolver, session):
        assert source["hosting_platform"] == "healthcarebluebook_mrf"
        assert url == "https://mrf.healthcarebluebook.com/Lucent"
        assert resolver["type"] == "healthcarebluebook_mrf"
        assert session is fake_session
        return [
            discovery.CrawlTarget(
                source=source,
                url="https://cdn.example/lucent_index.json",
                label="Lucent delegated TOC",
                resolved_from_url=url,
                metadata={"resolver": "healthcarebluebook_mrf"},
            )
        ]

    fake_session = object()
    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(
        discovery,
        "_resolve_healthcarebluebook_mrf",
        fake_resolve_healthcarebluebook_mrf,
    )

    crawl_targets = await discovery._crawl_targets_for_source(
        {"display_name": "Wrapper source"},
        "https://wrapper.example/mrf",
        fake_session,
    )

    assert len(crawl_targets) in {1}
    assert crawl_targets[0].source == {"display_name": "Wrapper source"}
    assert crawl_targets[0].url == "https://cdn.example/lucent_index.json"
    assert crawl_targets[0].resolved_from_url == "https://wrapper.example/mrf"
    assert crawl_targets[0].metadata["resolver"] == "healthcarebluebook_mrf"
    assert crawl_targets[0].metadata["delegated_source_url"] == (
        "https://mrf.healthcarebluebook.com/Lucent"
    )
    assert crawl_targets[0].metadata["delegated_source_platform"] == "healthcarebluebook_mrf"


def test_delegated_mrf_source_urls_extracts_ibx_keyed_toc_links():
    html = """
    <a href="https://www.ibx.com/transparency-in-coverage/821410?key=abc123">
      QCC machine-readable files
    </a>
    """

    urls = discovery._delegated_mrf_source_urls_from_html(
        html,
        base_url="https://www.reliancematrix.com/privacy-notice/transparency-in-coverage",
    )

    assert urls == [
        "https://www.ibx.com/transparency-in-coverage/821410?key=abc123"
    ]


def test_delegated_mrf_source_urls_extracts_sharp_network_links():
    html = """
    <a href="https://transparency-in-coverage.optum.com/">
      Optum Behavioral Health MRFs
    </a>
    <a href="https://health1.aetna.com/app/public/#/one/insurerCode=ASA_12&brandCode=AETNAASA/machine-readable-transparency-in-coverage">
      Aetna MRFs
    </a>
    <a href="https://www.health1.firsthealth.com/app/public/#/one/insurerCode=FIRSTHEALTH_I&brandCode=FIRSTH/machine-readable-transparency-in-coverage">
      First Health MRFs
    </a>
    """

    urls = discovery._delegated_mrf_source_urls_from_html(
        html, base_url="https://www.sharphealthplan.com/api-access-for-developers"
    )

    assert urls == [
        "https://transparency-in-coverage.optum.com/",
        "https://health1.aetna.com/app/public/#/one/insurerCode=ASA_12&brandCode=AETNAASA/machine-readable-transparency-in-coverage",
        "https://www.health1.firsthealth.com/app/public/#/one/insurerCode=FIRSTHEALTH_I&brandCode=FIRSTH/machine-readable-transparency-in-coverage",
    ]


def test_json_mrf_directory_links_extract_directory_json_from_html():
    html = """
    <a href="https://cdn.example.test/aso_directory.json">ASO Groups machine-readable files</a>
    <a href="https://cdn.example.test/style.json">Theme</a>
    """

    urls = discovery._json_mrf_directory_links_from_html(
        html, base_url="https://example.com/tcr"
    )

    assert urls == ["https://cdn.example.test/aso_directory.json"]


def test_json_mrf_directory_payload_extracts_toc_targets():
    source_dict = {"source_id": "source_1", "display_name": "Example"}
    payload = {
        "TOC_Files": [
            "https://cdn.example.test/TCR_TOC_Output/ASO/2026-06-01_example-plan_index.json",
            "https://cdn.example.test/TCR_TOC_Output/NON-ASO/2026-06-01_example_index.json",
            "/TCR_TOC_Output/ASO/2026-07-01_relative-plan_index.json",
        ]
    }

    targets = discovery._json_mrf_directory_targets_from_payload(
        source_dict,
        payload,
        directory_url="https://cdn.example.test/aso_directory.json",
        resolver_type="json_mrf_directory_links",
    )

    assert [target.url for target in targets] == [
        "https://cdn.example.test/TCR_TOC_Output/ASO/2026-06-01_example-plan_index.json",
        "https://cdn.example.test/TCR_TOC_Output/NON-ASO/2026-06-01_example_index.json",
        "https://cdn.example.test/TCR_TOC_Output/ASO/2026-07-01_relative-plan_index.json",
    ]
    assert targets[0].metadata["target_kind"] == "toc_json"
    assert targets[0].metadata["target_file_type"] == "table-of-contents"
    assert targets[0].metadata["directory_url"] == (
        "https://cdn.example.test/aso_directory.json"
    )


def test_webtpa_record_target_preserves_plan_metadata():
    source_dict = {"source_id": "source_1", "display_name": "WebTPA"}
    crawl_target = discovery._webtpa_record_target(
        source_dict,
        plan={"mrfBenefitplanId": 239, "benefitplanNm": "Example Plan"},
        file_record={
            "mrfInNetworkRatesId": 32120,
            "fileName": "Preferred PPO",
            "type": "link",
        },
        file_type="in-network",
        file_url="https://files.example.test/2026-06-01_example_in-network-rates.json",
        resolved_from_url=(
            "https://price-transparency.webtpa.com/"
            "machinereadablefile/in-network-rates/32120/location"
        ),
    )

    assert crawl_target is not None
    assert crawl_target.label == "Example Plan - Preferred PPO"
    assert crawl_target.metadata["resolver"] == "webtpa_mrf_api"
    assert crawl_target.metadata["target_kind"] == "file_reference"
    assert crawl_target.metadata["target_file_type"] == "in-network"
    assert crawl_target.metadata["webtpa_plan_id"] == "239"
    assert crawl_target.metadata["webtpa_file_id"] == "32120"
    assert crawl_target.metadata["plan_info"] == [
        {
            "plan_id": "239",
            "plan_id_type": "webtpa_mrf_benefitplan_id",
            "plan_name": "Example Plan",
        }
    ]


def test_cmstic_file_info_payload_builds_toc_target():
    source_dict = {"source_id": "source_1", "display_name": "Independence Blue Cross"}
    target = discovery._cmstic_target_from_payload(
        source_dict,
        {
            "name": "2026-06-01_qcc_index.json",
            "url": "https://storage.googleapis.com/ihg-dart-edw-mrf-prod-public/qcc/2026-06-01_qcc_index.json",
        },
        api_url="https://www.ibx.com/cmsticsvc/api/fi?brand=qcc",
        resolver_type="cmstic_file_info",
    )

    assert target is not None
    assert target.url == (
        "https://storage.googleapis.com/ihg-dart-edw-mrf-prod-public/qcc/"
        "2026-06-01_qcc_index.json"
    )
    assert target.label == "2026-06-01_qcc_index.json"
    assert target.resolved_from_url == "https://www.ibx.com/cmsticsvc/api/fi?brand=qcc"
    assert target.metadata["resolver"] == "cmstic_file_info"
    assert target.metadata["target_kind"] == "toc_json"
    assert target.metadata["target_file_type"] == "table-of-contents"


def test_cmstic_keyed_toc_target_preserves_redirect_provenance():
    source_dict = {"source_id": "source_1", "display_name": "Reliance Matrix"}
    keyed_url = "https://www.ibx.com/transparency-in-coverage/821410?key=abc123"
    final_url = (
        "https://storage.googleapis.com/ihg-dart-edw-mrf-prod-public/qcc/"
        "2026-06-01_821410_index.json"
    )

    assert discovery._is_cmstic_keyed_toc_url(keyed_url) is True
    target = discovery._cmstic_keyed_toc_crawl_target(
        source_dict,
        keyed_url,
        final_url=final_url,
        resolver={"toc_max_bytes": 104857600},
        resolver_type="cmstic_keyed_toc_redirect",
    )

    assert target is not None
    assert target.url == final_url
    assert target.resolved_from_url == keyed_url
    assert target.metadata["resolver"] == "cmstic_keyed_toc_redirect"
    assert target.metadata["target_file_type"] == "table-of-contents"
    assert target.metadata["cmstic_source_id"] == "821410"
    assert target.metadata["target_max_bytes"] == 104857600


def test_cmstic_brand_defaults_cover_amerihealth_developer_page():
    resolver_dict = {
        "default_brands_by_host": {
            "www.amerihealth.com": ["ahpa", "ahnj", "ahnjhmo"],
        }
    }

    brands = discovery._cmstic_brand_candidates_from_url(
        "https://www.amerihealth.com/developer-resources/index.html", resolver_dict
    )

    assert brands == ["ahpa", "ahnj", "ahnjhmo"]
    assert (
        discovery._cmstic_api_url(
            "https://www.amerihealth.com/developer-resources/index.html", "ahpa"
        )
        == "https://www.amerihealth.com/cmsticsvc/api/fi?brand=ahpa"
    )
    assert (
        discovery._cmstic_api_url(
            "https://www.amerihealth.com/developer-resources/index.html", "ahnjhmo"
        )
        == "https://www.amerihealthnj.com/cmsticsvc/api/fi?brand=ahnjhmo"
    )


def test_hcsc_asomrf_urls_extracts_state_pages():
    html = """
    <a href="https://www.bcbsil.com/asomrf?EIN=361236610">Illinois</a>
    <a href="https://www.bcbstx.com/asomrf?EIN=361236610">Texas</a>
    <a href="/documents/not-mrf.pdf">Ignore</a>
    """

    urls = discovery._hcsc_asomrf_urls_from_html(
        html, base_url="https://www.hcsc.com/who-we-are/transparency-in-coverage"
    )

    assert urls == [
        "https://www.bcbsil.com/asomrf?EIN=361236610",
        "https://www.bcbstx.com/asomrf?EIN=361236610",
    ]


def test_point32_directory_urls_extracts_azure_plan_list():
    html = """
    <a href="https://eusprdtransparencymrfp32.z13.web.core.windows.net/hphc">View Plan List</a>
    <a href="https://example.com/">Ignore</a>
    """

    urls = discovery._point32_directory_urls_from_html(
        html, base_url="https://www.harvardpilgrim.org/public/machine-readable-files"
    )

    assert urls == ["https://eusprdtransparencymrfp32.z13.web.core.windows.net/hphc"]


@pytest.mark.asyncio
async def test_point32_resolver_follows_extensionless_mrf_directory(monkeypatch):
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Health Plan",
    }
    html_by_url = {
        "https://example.test/meet-us/transparency-regulations": """
          <a href="/pricetransparency/MRF/Base">
            Click here to access machine-readable files.
          </a>
        """,
        "https://example.test/pricetransparency/MRF/Base": """
          <a href="https://storage.example.test/pricetransparency/MRF/Base/INN/example_in-network-rates.json.gz">
            example_in-network-rates.json.gz
          </a>
          <a href="https://storage.example.test/pricetransparency/MRF/Base/OON/example_allowed-amounts.json.gz">
            example_allowed-amounts.json.gz
          </a>
        """,
    }

    async def fake_fetch_text(url, **_kwargs):
        return html_by_url[url]

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)

    crawl_targets = await discovery._resolve_point32_azure_mrf_directory(
        source_dict,
        "https://example.test/meet-us/transparency-regulations",
        {"type": "point32_azure_mrf_directory"},
        None,
    )

    assert [crawl_target.url for crawl_target in crawl_targets] == [
        "https://storage.example.test/pricetransparency/MRF/Base/INN/example_in-network-rates.json.gz",
        "https://storage.example.test/pricetransparency/MRF/Base/OON/example_allowed-amounts.json.gz",
    ]
    assert crawl_targets[0].metadata["target_kind"] == "file_reference"
    assert crawl_targets[0].metadata["target_file_type"] == "in-network"
    assert crawl_targets[0].metadata["point32_landing_url"] == (
        "https://example.test/meet-us/transparency-regulations"
    )
    assert crawl_targets[0].metadata["point32_directory_url"] == (
        "https://example.test/pricetransparency/MRF/Base"
    )
    assert crawl_targets[1].metadata["target_file_type"] == "allowed-amounts"


def test_anthem_s3_script_parsing_builds_current_month_targets():
    script = """
    var statusUrl1 = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/status.json';
    s3url = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/";
    var tocUrl = s3url +'healthlink/'+year+'-'+month+'-01_healthlink_index.json';
    """
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "HealthLink",
    }
    patterns = discovery._anthem_s3_toc_patterns_from_script(
        script,
        source_url="https://www.healthlink.com/machine-readable-file/search/",
    )

    crawl_targets = discovery._anthem_s3_toc_targets(
        source_dict,
        "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/",
        patterns,
        {"month_offsets": [0]},
        source_url="https://www.healthlink.com/machine-readable-file/search/",
        now=discovery.dt.datetime(2026, 6, 27, 12, 0, 0),
    )

    assert discovery._anthem_s3_status_urls_from_script(script) == [
        "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/status.json"
    ]
    assert discovery._anthem_s3_bases_from_script(script) == [
        "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/"
    ]
    assert patterns == [("healthlink", "healthlink", ".json")]
    assert crawl_targets[0].url == (
        "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/"
        "healthlink/2026-06-01_healthlink_index.json"
    )
    assert crawl_targets[0].metadata["resolver"] == "anthem_s3_mrf"
    assert crawl_targets[0].metadata["month_start"] == "2026-06-01"


def _example_anthem_employer_source_row() -> dict[str, object]:
    return {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Anthem",
        "metadata_json": {
            "raw": {
                "target_payer_query": "Example Employer LLC",
                "query_context_employer_name": "Example Employer LLC",
                "query_context_employer_aliases": [
                    "Example Employer",
                    "Example Holdings",
                ],
                "query_context_employer_ein": "12-3456789",
                "query_context_erisa_plan_number": "501",
                "query_context_carrier_policy_number": "POLICY-A",
                "query_context_evidence_plan_year": "2023",
                "query_context_verification_status": (
                    "historical_current_unconfirmed"
                ),
            }
        },
    }


def _example_anthem_employer_result() -> dict[str, object]:
    return {
        "lastupdated": "2026-07-01",
        "In-Network Negotiated Rates Files": [
            {
                "url": "https://files.example.test/employer-in-network.json.gz",
                "displayname": "employer-in-network.json.gz",
            }
        ],
        "Out-of-Network Allowed Amounts Files": [
            {
                "url": "https://files.example.test/employer-allowed.json.gz",
                "displayname": "employer-allowed.json.gz",
            }
        ],
    }


def _example_anthem_lookup_context() -> discovery.AnthemLookupContext:
    return discovery.AnthemLookupContext(
        base_url="https://files.example.test/",
        prefix="anthem",
        catalog_url=(
            "https://files.example.test/anthem/2026-07-01_anthem_index.json.gz"
        ),
        resolver_by_key={"max_targets": 10},
        source_url="https://www.anthem.example.test/machine-readable-file/search/",
        session=None,
    )


def test_anthem_employer_result_builds_importable_company_targets():
    crawl_targets = discovery._anthem_s3_employer_targets(
        _example_anthem_employer_source_row(),
        _example_anthem_employer_result(),
        lookup_url="https://files.example.test/anthem/123456789.json",
        lookup_context=_example_anthem_lookup_context(),
    )

    assert [
        crawl_target.metadata["target_file_type"]
        for crawl_target in crawl_targets
    ] == [
        "in-network",
        "allowed-amounts",
    ]
    assert crawl_targets[0].metadata["employer_name"] == "Example Employer LLC"
    assert crawl_targets[0].metadata["aliases"] == [
        "Example Employer",
        "Example Holdings",
    ]
    assert crawl_targets[0].metadata["ein"] == "12-3456789"
    assert crawl_targets[0].metadata["carrier_policy_number"] == "POLICY-A"
    assert crawl_targets[0].metadata["query_context_match"] is True
    assert crawl_targets[0].metadata["query_context_match_scope"] == (
        "employer_identity"
    )
    assert crawl_targets[0].metadata["plan_info"] == [
        {
            "plan_id": "123456789",
            "plan_id_type": "ein",
            "plan_market_type": "group",
            "plan_name": "Example Employer LLC",
            "plan_sponsor_name": "Example Employer LLC",
            "issuer_name": "Anthem",
        }
    ]


def test_anthem_employer_target_uses_current_resolved_identity():
    """Current name-index identity must replace stale configured scalars."""
    crawl_targets = discovery._anthem_s3_employer_targets(
        _example_anthem_employer_source_row(),
        _example_anthem_employer_result(),
        lookup_url="https://files.example.test/anthem/987654321.json",
        lookup_context=_example_anthem_lookup_context(),
        matched_employer_name="Example Employer Services LLC",
        matched_ein="987654321",
        name_index_url="https://files.example.test/namesearch/e.json",
    )

    metadata = crawl_targets[0].metadata
    assert metadata["company_name"] == "Example Employer Services LLC"
    assert metadata["employer_name"] == "Example Employer Services LLC"
    assert metadata["ein"] == "987654321"
    assert metadata["anthem_requested_ein"] == "12-3456789"
    assert metadata["plan_info"][0]["plan_name"] == (
        "Example Employer Services LLC"
    )


@pytest.mark.asyncio
async def test_anthem_employer_lookup_gets_object_when_head_is_denied(
    monkeypatch,
):
    head_url = AsyncMock(
        return_value={
            "status": "http_error",
            "http_status": 403,
        }
    )
    fetch_json = AsyncMock(return_value=_example_anthem_employer_result())
    monkeypatch.setattr(discovery, "_head_url", head_url)
    monkeypatch.setattr(discovery, "_fetch_json", fetch_json)

    crawl_targets = await discovery._resolve_anthem_s3_employer_files(
        _example_anthem_employer_source_row(),
        employer_ein="12-3456789",
        lookup_context=_example_anthem_lookup_context(),
    )

    head_url.assert_not_awaited()
    fetch_json.assert_awaited_once_with(
        "https://files.example.test/anthem/123456789.json",
        max_bytes=1024 * 1024,
        session=None,
    )
    assert len(crawl_targets) == 2


def test_anthem_name_index_matches_private_aliases_and_valid_eins():
    name_index = {
        "namesearch": [
            {"ein": "98-7654321", "name": "example employer services llc"},
            {"ein": "invalid", "name": "example employer legacy plan"},
            {"ein": "11-1223333", "name": "unrelated sample company"},
        ]
    }

    matches = discovery._anthem_s3_name_index_matches(
        name_index,
        ("Example Employer", "Example Holdings"),
        index_url="https://files.example.test/namesearch/e.json",
    )

    assert matches == [
        {
            "ein": "987654321",
            "name": "example employer services llc",
            "name_index_url": "https://files.example.test/namesearch/e.json",
        }
    ]
    assert discovery._anthem_s3_name_index_key("Example Employer") == "e"
    assert discovery._anthem_s3_name_index_key("123 Sample") == "others"


def _assert_merged_anthem_context_target(crawl_target, name_index_url):
    """Check complete multi-employer identity on one shared MRF target."""
    metadata = crawl_target.metadata
    assert {
        plan["plan_id"] for plan in metadata["plan_info"]
    } == {"987654321", "111223333"}
    assert {
        match["name"] for match in metadata["anthem_employer_matches"]
    } == {
        "example employer services llc",
        "example holdings benefits llc",
    }
    assert metadata["anthem_matched_eins"] == ["987654321", "111223333"]
    assert "ein" not in metadata
    assert metadata["company_name"] == "Example Employer LLC"
    assert metadata["anthem_employer_lookup_urls"] == [
        "https://files.example.test/anthem/987654321.json",
        "https://files.example.test/anthem/111223333.json",
    ]
    assert metadata["anthem_name_index_url"] == name_index_url
    assert metadata["anthem_catalog_url"].endswith(
        "2026-07-01_anthem_index.json.gz"
    )
    assert crawl_target.resolved_from_url.endswith(
        "2026-07-01_anthem_index.json.gz"
    )


@pytest.mark.asyncio
async def test_anthem_context_uses_name_index_and_merges_shared_company_files(
    monkeypatch,
):
    """Name lookup must merge duplicate URLs without losing plan identities."""
    name_index_url = "https://files.example.test/namesearch/e.json"
    employer_result = _example_anthem_employer_result()
    fetched_urls = []

    async def fake_fetch_json(url, **_kwargs):
        fetched_urls.append(url)
        if url == name_index_url:
            return {
                "namesearch": [
                    {
                        "ein": "98-7654321",
                        "name": "example employer services llc",
                    },
                    {
                        "ein": "11-1223333",
                        "name": "example holdings benefits llc",
                    },
                ]
            }
        if url.endswith(("/987654321.json", "/111223333.json")):
            return employer_result
        raise ValueError("missing employer object")

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    crawl_targets = await discovery._resolve_anthem_s3_context(
        _example_anthem_employer_source_row(),
        employer_ein="12-3456789",
        employer_names=("Example Employer", "Example Holdings"),
        lookup_context=_example_anthem_lookup_context(),
    )

    assert fetched_urls == [
        "https://files.example.test/anthem/123456789.json",
        name_index_url,
        "https://files.example.test/anthem/987654321.json",
        "https://files.example.test/anthem/111223333.json",
    ]
    assert len(crawl_targets) == 2
    _assert_merged_anthem_context_target(crawl_targets[0], name_index_url)


@pytest.mark.asyncio
async def test_anthem_explicit_ein_rejects_different_name_index_employer(
    monkeypatch,
):
    """An exact EIN lookup must not silently select a related organization."""
    source_row = _example_anthem_employer_source_row()
    source_row["metadata_json"]["raw"]["query_context_lookup_type"] = (
        "employer_ein"
    )
    name_index_url = "https://files.example.test/namesearch/e.json"
    fetched_urls = []

    async def fake_fetch_json(url, **_kwargs):
        fetched_urls.append(url)
        if url == name_index_url:
            return {
                "namesearch": [
                    {
                        "ein": "98-7654321",
                        "name": "example employer investment affiliate",
                    }
                ]
            }
        raise ValueError("missing configured employer object")

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    with pytest.raises(
        ValueError,
        match="no current Anthem employer MRF result for configured EIN",
    ):
        await discovery._resolve_anthem_s3_context(
            source_row,
            employer_ein="12-3456789",
            employer_names=("Example Employer",),
            lookup_context=_example_anthem_lookup_context(),
        )

    assert fetched_urls == [
        "https://files.example.test/anthem/123456789.json",
        name_index_url,
    ]


@pytest.mark.asyncio
async def test_anthem_name_index_failure_aborts_context_resolution(monkeypatch):
    """A failed name shard must not look like a complete empty result."""
    monkeypatch.setattr(
        discovery,
        "_fetch_json",
        AsyncMock(
            side_effect=discovery.aiohttp.ClientConnectionError("unavailable")
        ),
    )

    with pytest.raises(ValueError, match="Anthem name index fetch failed"):
        await discovery._fetch_anthem_s3_name_matches(
            "https://files.example.test/",
            ("Example Employer",),
            {"name_index_max_bytes": 1024},
            None,
        )


@pytest.mark.asyncio
async def test_anthem_direct_ein_survives_name_index_failure(monkeypatch):
    """A current trusted EIN remains usable during a name-shard outage."""

    async def fake_fetch_json(url, **_kwargs):
        if url.endswith("/123456789.json"):
            return _example_anthem_employer_result()
        raise ValueError("temporary name-index failure")

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    crawl_targets = await discovery._resolve_anthem_s3_context(
        _example_anthem_employer_source_row(),
        employer_ein="12-3456789",
        employer_names=("Example Employer",),
        lookup_context=_example_anthem_lookup_context(),
    )

    assert len(crawl_targets) == 2
    assert all(
        target.metadata["anthem_name_index_status"]
        == "unavailable_direct_ein_fallback"
        for target in crawl_targets
    )


@pytest.mark.asyncio
async def test_anthem_name_index_match_limit_aborts_partial_resolution(monkeypatch):
    """A bounded lookup must fail rather than truncate employer matches."""
    monkeypatch.setattr(
        discovery,
        "_fetch_json",
        AsyncMock(
            return_value={
                "namesearch": [
                    {"ein": "98-7654321", "name": "example employer one"},
                    {"ein": "11-1223333", "name": "example employer two"},
                ]
            }
        ),
    )

    with pytest.raises(ValueError, match="exceeds configured match limit"):
        await discovery._fetch_anthem_s3_name_matches(
            "https://files.example.test/",
            ("Example Employer",),
            {"max_name_matches": 1},
            None,
        )


@pytest.mark.asyncio
async def test_anthem_context_rejects_partial_indexed_employer_results(monkeypatch):
    """One broken indexed employer object must fail the bounded refresh."""
    name_index_url = "https://files.example.test/namesearch/e.json"

    async def fake_fetch_json(url, **_kwargs):
        if url == name_index_url:
            return {
                "namesearch": [
                    {"ein": "98-7654321", "name": "example employer one"},
                    {"ein": "11-1223333", "name": "example employer two"},
                ]
            }
        if url.endswith("/987654321.json"):
            return _example_anthem_employer_result()
        raise ValueError("temporary employer-object failure")

    monkeypatch.setattr(discovery, "_fetch_json", fake_fetch_json)

    with pytest.raises(
        ValueError,
        match="Anthem name-index employer result could not be resolved",
    ):
        await discovery._resolve_anthem_s3_context(
            _example_anthem_employer_source_row(),
            employer_ein="",
            employer_names=("Example Employer",),
            lookup_context=_example_anthem_lookup_context(),
        )


@pytest.mark.asyncio
async def test_anthem_missing_employer_result_does_not_fallback_to_national_toc(
    monkeypatch,
):
    fetch_json = AsyncMock(side_effect=ValueError("access denied"))
    monkeypatch.setattr(discovery, "_fetch_json", fetch_json)

    with pytest.raises(
        ValueError,
        match="no current Anthem employer MRF result for EIN 12-3456789",
    ):
        await discovery._resolve_anthem_s3_employer_files(
            {"source_id": "source_1"},
            employer_ein="12-3456789",
            lookup_context=_example_anthem_lookup_context(),
        )

    fetch_json.assert_awaited_once()


def test_fetch_text_decode_response_body_handles_raw_gzip_json():
    payload = discovery._decode_response_body(gzip.compress(b'{"ok": true}'))

    assert payload == '{"ok": true}'


def test_direct_toc_url_accepts_no_extension_mrf_index():
    assert discovery._is_direct_toc_url(
        "https://mrf.example.com/mrf/hmo_ha_hii_example_index"
    )
    assert discovery._is_direct_toc_url(
        "https://bcbsm.sapphiremrfhub.com/tocs/current/blue_cross_blue_shield_of_michigan"
    )
    assert discovery._is_direct_toc_url(
        "https://www.bluecrossvt.org/documents/toc-json"
    )
    assert discovery._is_direct_toc_url(
        "https://mrf.secure.bcbsks.com/api/filedownloadhttptrigger?name=table-of-contents&ext=json"
    )
    assert discovery._is_direct_toc_url(
        "https://www.hmaa.com/wp-content/uploads/2022/06/MRF_HMAA.zip"
    )


def test_direct_toc_url_rejects_provider_directory_indexes():
    assert (
        discovery._is_direct_toc_url(
            "https://example.test/acadirectory/97176/97176Index.json"
        )
        is False
    )
    assert (
        discovery._is_direct_toc_url(
            "https://example.test/provider-directory/index.json"
        )
        is False
    )
    assert (
        discovery._is_direct_toc_url(
            "https://example.test/cms-data-index/index.json"
        )
        is False
    )


def test_cigna_lookup_html_extracts_configured_and_page_lookup_urls():
    html = """<div data-mrf-lookup-url="/static/mrf/latest.json"></div>"""
    resolver_dict = {"lookup_paths": ["/static/mrf/co/latest.json"]}

    urls = discovery._cigna_lookup_urls_from_html(
        html,
        base_url="https://www.cigna.com/legal/compliance/machine-readable-files",
        resolver=resolver_dict,
    )

    assert urls == [
        "https://www.cigna.com/static/mrf/latest.json",
        "https://www.cigna.com/static/mrf/co/latest.json",
    ]


def test_bcbs_global_solutions_extracts_toc_links():
    html = """
    <a href="/transparency-in-coverage-toc-json.cfm?planType=4EverLife">4 Ever Life</a>
    <a href="/transparency-in-coverage-toc-json.cfm?planType=GeoBlue">GeoBlue</a>
    """

    links = discovery._bcbs_global_solutions_toc_links_from_html(
        html,
        base_url="https://groupadmin.bcbsglobalsolutions.com/transparency-in-coverage.cfm",
    )

    assert [link["plan_type"] for link in links] == ["4EverLife", "GeoBlue"]
    assert links[0]["url"] == (
        "https://groupadmin.bcbsglobalsolutions.com/"
        "transparency-in-coverage-toc-json.cfm?planType=4EverLife"
    )


@pytest.mark.asyncio
async def test_bcbs_global_solutions_resolver_follows_landing_and_skips_stale_tocs(
    monkeypatch,
):
    """Verify this source-discovery regression contract."""
    public_landing = "https://bcbsglobalsolutions.com/transparency-in-coverage/"
    group_landing = (
        "https://groupadmin.bcbsglobalsolutions.com/transparency-in-coverage.cfm"
    )
    live_toc = (
        "https://groupadmin.bcbsglobalsolutions.com/"
        "transparency-in-coverage-toc-json.cfm?planType=4EverLife"
    )
    stale_toc = (
        "https://groupadmin.bcbsglobalsolutions.com/"
        "transparency-in-coverage-toc-json.cfm?planType=GeoBlue"
    )
    allowed_only_toc = (
        "https://groupadmin.bcbsglobalsolutions.com/"
        "transparency-in-coverage-toc-json.cfm?planType=AllowedOnly"
    )

    async def fake_fetch_text(url, **_kwargs):
        if url == public_landing:
            return f'<a href="{group_landing}">MRF table of contents</a>'
        if url == group_landing:
            return f"""
            <a href="{live_toc}">4 Ever Life</a>
            <a href="{stale_toc}">GeoBlue</a>
            <a href="{allowed_only_toc}">Allowed only</a>
            """
        raise AssertionError(f"unexpected fetch_text URL: {url}")

    async def fake_fetch_json_value(url, **_kwargs):
        if url == stale_toc:
            raise ValueError("response body is not JSON")
        if url == allowed_only_toc:
            return {
                "reporting_entity_name": "Example Reporting Entity",
                "reporting_entity_type": "third-party administrator",
                "reporting_structure": [
                    {
                        "reporting_plans": [
                            {"reporting_entity_name": "Allowed Only Plan"}
                        ],
                        "allowed_amount_file": {
                            "description": "allowed",
                            "location": "https://example.test/allowed.json.gz",
                        },
                    }
                ],
            }
        if url == live_toc:
            return {
                "reporting_entity_name": "Example Reporting Entity",
                "reporting_entity_type": "third-party administrator",
                "reporting_structure": [
                    {
                        "reporting_plans": [
                            {"reporting_entity_name": "Example Live Plan"}
                        ],
                        "in_network_files": [
                            {
                                "description": "in_network_files",
                                "location": "https://example.test/in-network.json.gz",
                            }
                        ],
                    }
                ],
            }
        raise AssertionError(f"unexpected fetch_json URL: {url}")

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(discovery, "_fetch_json_value", fake_fetch_json_value)

    crawl_targets = await discovery._resolve_bcbs_global_solutions_mrf(
        {
            "source_id": "source_1",
            "payer_id": "payer_1",
            "display_name": "BCBS Global Solutions",
        },
        public_landing,
        {"type": "bcbs_global_solutions_mrf", "toc_max_bytes": 12345},
        session=None,
    )

    assert [crawl_target.url for crawl_target in crawl_targets] == [live_toc]
    assert crawl_targets[0].metadata["target_file_type"] == "table-of-contents"
    assert crawl_targets[0].metadata["target_max_bytes"] == 12345
    assert crawl_targets[0].metadata["plan_type"] == "4EverLife"
    assert crawl_targets[0].metadata["reporting_plan_name"] == "Example Live Plan"


def test_bcbs_asomrf_filelist_html_extracts_filelist_url():
    html = (
        """<script>var filelist = "/content/dam/bcbs/mrf/si-filelist.json";</script>"""
    )

    urls = discovery._bcbs_asomrf_filelist_urls_from_html(
        html, base_url="https://www.bcbsil.com/asomrf?EIN=260241222"
    )

    assert urls == ["https://www.bcbsil.com/content/dam/bcbs/mrf/si-filelist.json"]


def test_parse_bcbs_asomrf_filelist_targets_expands_index_urls():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "BCBS Illinois",
    }
    filelist_entries = [
        {
            "last_update_date": "2026-05-21",
            "state": "IL",
            "url": "https://app.example/toc/2026-05-21_Blue-Cross-and-Blue-Shield-of-Illinois_260241222_index.json",
            "name": "2026-05-21_Blue-Cross-and-Blue-Shield-of-Illinois_260241222_index",
            "ein": "260241222",
        },
        {
            "url": "https://app.example/body/in-network-rates.json.gz",
            "name": "skip body file",
        },
    ]

    [toc_target] = discovery._parse_bcbs_asomrf_filelist_targets(
        filelist_entries,
        filelist_url="https://www.bcbsil.com/content/dam/bcbs/mrf/si-filelist.json",
        source_row_dict=source_dict,
        resolver={"toc_max_bytes": 12345},
    )

    assert (
        toc_target.url
        == "https://app.example/toc/2026-05-21_Blue-Cross-and-Blue-Shield-of-Illinois_260241222_index.json"
    )
    assert (
        toc_target.resolved_from_url
        == "https://www.bcbsil.com/content/dam/bcbs/mrf/si-filelist.json"
    )
    assert toc_target.metadata["resolver"] == "bcbs_asomrf_filelist"
    assert toc_target.metadata["state"] == "IL"
    assert toc_target.metadata["ein"] == "260241222"
    assert toc_target.metadata["target_max_bytes"] == 12345


def test_parse_bcbs_asomrf_filelist_targets_applies_state_balanced_limit():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "BCBS Illinois",
    }
    filelist_entries = [
        {
            "state": state,
            "url": f"https://app.example/toc/2026-05-21_Blue-Cross-and-Blue-Shield-of-{state}_{index}_index.json",
            "name": f"2026-05-21_Blue-Cross-and-Blue-Shield-of-{state}_{index}_index",
            "ein": str(index),
        }
        for state in ("TX", "TX", "TX", "IL", "IL", "OK")
        for index in range(2)
    ]

    toc_targets = discovery._parse_bcbs_asomrf_filelist_targets(
        filelist_entries,
        filelist_url="https://www.bcbsil.com/content/dam/bcbs/mrf/si-filelist.json",
        source_row_dict=source_dict,
        resolver={"max_targets": 5},
    )

    assert [toc_target.metadata["state"] for toc_target in toc_targets] == [
        "TX",
        "IL",
        "OK",
        "TX",
        "IL",
    ]


def test_cigna_lookup_targets_preserve_file_metadata_and_large_toc_limit():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Cigna",
    }
    lookup_by_key = {
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

    crawl_targets = discovery._parse_cigna_lookup_targets(
        lookup_by_key,
        lookup_url="https://www.cigna.com/static/mrf/latest.json",
        source_row_dict=source_dict,
        resolver={"toc_max_bytes": 104857600},
    )

    assert len(crawl_targets) in {1}
    assert crawl_targets[0].url == "https://d25kgz5rikkq4n.cloudfront.net/index.json"
    assert (
        crawl_targets[0].resolved_from_url
        == "https://www.cigna.com/static/mrf/latest.json"
    )
    assert crawl_targets[0].metadata["resolver"] == "cigna_static_mrf_lookup"
    assert crawl_targets[0].metadata["target_max_bytes"] == 104857600
    assert crawl_targets[0].metadata["blob_size"] == 68740000
    assert (
        crawl_targets[0].metadata["reporting_entity_name"]
        == "Cigna Health Life Insurance Company"
    )


def test_bcbsma_monthly_tocs_generate_current_issuer_indexes():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "BCBS Massachusetts",
    }
    resolver = discovery._source_config()["platform_resolvers"]["bcbsma_monthly_tocs"]

    targets = discovery._bcbsma_monthly_toc_targets(
        source_dict,
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
    assert (
        targets[1].metadata["issuer_slug"]
        == "Blue-Cross-and-Blue-Shield-of-Massachusetts-Inc"
    )


def test_monthly_toc_templates_generate_current_and_previous_month_targets():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Monthly Plan",
    }

    targets = discovery._monthly_toc_targets(
        source_dict,
        "https://example.test/transparency",
        {
            "type": "monthly_toc_templates",
            "base_url": "https://files.example.test/",
            "file_templates": ["{month_start}_example_index.json"],
            "month_offsets": [0, -1],
            "toc_max_bytes": 123456,
        },
        now=discovery.dt.datetime(2026, 6, 27, 12, 0, 0),
    )

    assert [target.url for target in targets] == [
        "https://files.example.test/2026-06-01_example_index.json",
        "https://files.example.test/2026-05-01_example_index.json",
    ]
    assert targets[0].metadata["target_max_bytes"] == 123456
    assert targets[0].metadata["month_start"] == "2026-06-01"


def test_kaiser_inventory_parses_tocs_rate_files_and_allowed_amounts():
    source_by_field = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Kaiser Permanente",
    }
    resolver = discovery._source_config()["platform_resolvers"]["kaiser_mrf_inventory"]
    in_network_targets = discovery._kaiser_inventory_targets_from_text(
        source_by_field,
        "\n".join(
            [
                "/hi/2026-07-01_KFHP-HI_index.json 26574",
                "/hi/2026-07-01_NEW_HI-COMMERCIAL-01_in-network-rates.zip 650894695",
                "/externaldata/ash/2026-07-01_ASH_KFHP-HI_in-network-rates.json 433813",
            ]
        ),
        inventory_url=(
            "https://healthy.kaiserpermanente.org/pricing/"
            "innetwork/2026-07_List.txt"
        ),
        inventory_month="2026-07",
        category="innetwork",
        resolver=resolver,
    )
    allowed_targets = discovery._kaiser_inventory_targets_from_text(
        source_by_field,
        "/hi/2026-07-01_KFHP_HI-40513_allowed-amounts.zip 6131",
        inventory_url=(
            "https://healthy.kaiserpermanente.org/pricing/"
            "outofnetwork/2026-07_List.txt"
        ),
        inventory_month="2026-07",
        category="outofnetwork",
        resolver=resolver,
    )

    assert [inventory_target.metadata["target_file_type"] for inventory_target in in_network_targets] == [
        "table-of-contents",
        "in-network",
    ]
    assert in_network_targets[0].metadata["target_kind"] == "toc_json"
    assert in_network_targets[0].metadata["size_bytes"] == 26574
    assert in_network_targets[1].metadata["container_format"] == "zip"
    assert all(
        inventory_target.metadata["kaiser_region_code"] == "hi"
        for inventory_target in in_network_targets + allowed_targets
    )
    assert allowed_targets[0].metadata["target_file_type"] == "allowed-amounts"
    assert allowed_targets[0].url.endswith("KFHP_HI-40513_allowed-amounts.zip")


@pytest.mark.asyncio
async def test_kaiser_inventory_uses_previous_month_per_missing_category(monkeypatch):
    source_by_field = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Kaiser Permanente",
    }
    resolver = {
        **discovery._source_config()["platform_resolvers"]["kaiser_mrf_inventory"],
        "month_offsets": [0, -1],
    }
    response_by_url = {
        (
            "https://healthy.kaiserpermanente.org/pricing/"
            "innetwork/2026-07_List.txt"
        ): "/hi/2026-07-01_KFHP-HI_index.json 26574",
        (
            "https://healthy.kaiserpermanente.org/pricing/"
            "outofnetwork/2026-07_List.txt"
        ): "<html>not found</html>",
        (
            "https://healthy.kaiserpermanente.org/pricing/"
            "outofnetwork/2026-06_List.txt"
        ): "/hi/2026-06-01_KFHP_HI-40513_allowed-amounts.zip 6131",
    }

    async def fake_fetch_text(url, **_kwargs):
        return response_by_url.get(url, "")

    monkeypatch.setattr(discovery, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(
        discovery,
        "_utc_now",
        lambda: discovery.dt.datetime(2026, 7, 16, 12, 0, 0),
    )

    inventory_targets = await discovery._resolve_kaiser_monthly_inventory(
        source_by_field,
        "https://healthy.kaiserpermanente.org/front-door/machine-readable",
        resolver,
        None,
    )

    assert [inventory_target.metadata["inventory_month"] for inventory_target in inventory_targets] == [
        "2026-07",
        "2026-06",
    ]


def test_bcbsmn_monthly_toc_template_generates_public_index_targets():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Blue Plan",
    }
    resolver = discovery._source_config()["platform_resolvers"][
        "bcbsmn_monthly_toc"
    ]

    targets = discovery._monthly_toc_targets(
        source_dict,
        "https://www.bluecrossmn.com/transparency-coverage-machine-readable-files",
        resolver,
        now=discovery.dt.datetime(2026, 7, 1, 12, 0, 0),
    )

    assert [target.url for target in targets] == [
        "https://mktg.bluecrossmn.com/mrf/2026/"
        "2026-07-01_Blue_Cross_and_Blue_Shield_of_Minnesota_index.json",
        "https://mktg.bluecrossmn.com/mrf/2026/"
        "2026-06-01_Blue_Cross_and_Blue_Shield_of_Minnesota_index.json",
    ]
    assert targets[0].metadata["resolver"] == "monthly_toc_templates"
    assert targets[0].metadata["target_file_type"] == "table-of-contents"


def test_oscar_monthly_toc_template_uses_compact_month_start():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Direct Plan",
    }
    resolver = discovery._source_config()["platform_resolvers"][
        "oscar_s3_monthly_toc"
    ]

    targets = discovery._monthly_toc_targets(
        source_dict,
        "https://www.hioscar.com/transparency-in-coverage-files/oscar",
        resolver,
        now=discovery.dt.datetime(2026, 7, 23, 12, 0, 0),
    )

    assert [target.url for target in targets] == [
        "https://hioscar-cms-tic-us-east-1.s3.amazonaws.com/oscar/"
        "20260701_oscar_index.json",
        "https://hioscar-cms-tic-us-east-1.s3.amazonaws.com/oscar/"
        "20260601_oscar_index.json",
    ]
    assert targets[0].metadata["month_start"] == "2026-07-01"


def test_sutter_monthly_toc_template_generates_sitecore_index_target():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Monthly Plan",
    }
    resolver = discovery._source_config()["platform_resolvers"][
        "sutter_health_plan_sitecore"
    ]

    targets = discovery._monthly_toc_targets(
        source_dict,
        "https://www.sutterhealthplan.org/technical-information",
        resolver,
        now=discovery.dt.datetime(2026, 6, 27, 12, 0, 0),
    )

    assert [target.url for target in targets] == [
        "https://xmc-sutterhealt962c-sutterhealt8fce-production57cc.sitecorecloud.io/-/media/Project/SutterHealth/SutterHealthPlan/PDF/2026-06-01_sutter-health-plus_index",
        "https://xmc-sutterhealt962c-sutterhealt8fce-production57cc.sitecorecloud.io/-/media/Project/SutterHealth/SutterHealthPlan/PDF/2026-05-01_sutter-health-plus_index",
    ]
    assert targets[0].metadata["target_file_type"] == "table-of-contents"
    assert targets[0].metadata["target_max_bytes"] == 52428800


def test_bcbswy_monthly_toc_template_generates_scoped_hmhs_target():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "BCBS Wyoming",
    }
    resolver = discovery._source_config()["platform_resolvers"][
        "bcbswy_hmhs_monthly_toc"
    ]

    targets = discovery._monthly_toc_targets(
        source_dict,
        "https://www.bcbswy.com/machine-readable-files/",
        resolver,
        now=discovery.dt.datetime(2026, 6, 27, 12, 0, 0),
    )

    assert [target.url for target in targets] == [
        "https://mrfdata.hmhs.com/files/460/wy/inbound/local/2026-06-01_Blue_Cross_Blue_Shield_of_Wyoming_index.json"
    ]
    assert targets[0].metadata["resolver"] == "monthly_toc_templates"
    assert targets[0].metadata["month_start"] == "2026-06-01"


def test_azure_mrf_listing_targets_from_xml_extracts_toc_metadata():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Azure Plan",
    }
    xml = """<?xml version="1.0" encoding="utf-8"?>
    <EnumerationResults ContainerName="https://storage.example.test/container">
      <Blobs>
        <Blob>
          <Name>index/2026-06_example_index.json</Name>
          <Url>https://storage.example.test/container/index/2026-06_example_index.json</Url>
          <Properties>
            <Last-Modified>Mon, 01 Jun 2026 17:12:40 GMT</Last-Modified>
            <Etag>0x123</Etag>
            <Content-Length>2153918</Content-Length>
            <Content-Type>application/octet-stream</Content-Type>
          </Properties>
        </Blob>
      </Blobs>
    </EnumerationResults>
    """
    crawl_targets = discovery._azure_mrf_listing_targets_from_xml(
        source_dict,
        xml,
        listing_url="https://api.example.test/list",
        resolver={"type": "azure_mrf_listing", "toc_max_bytes": 456789},
    )

    assert len(crawl_targets) in {1}
    assert (
        crawl_targets[0].url
        == "https://storage.example.test/container/index/2026-06_example_index.json"
    )
    assert crawl_targets[0].metadata["resolver"] == "azure_mrf_listing"
    assert crawl_targets[0].metadata["target_kind"] == "toc_json"
    assert crawl_targets[0].metadata["content_length"] == "2153918"
    assert crawl_targets[0].metadata["target_max_bytes"] == 456789


def test_azure_mrf_listing_targets_from_xml_extracts_hostedjson_files():
    """Verify Azure XML listings expose hosted JSON MRF targets."""
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Contra Costa Health Plan",
    }
    xml = """<?xml version="1.0" encoding="utf-8"?>
    <EnumerationResults ContainerName="https://hostedjson.blob.core.windows.net/transparencyfiles">
      <Blobs>
        <Blob>
          <Name>2026-05-30_CCHP_ALLOWED_allowed-amounts.json</Name>
          <Url>https://hostedjson.blob.core.windows.net/transparencyfiles/2026-05-30_CCHP_ALLOWED_allowed-amounts.json</Url>
          <Properties><Content-Length>111</Content-Length></Properties>
        </Blob>
        <Blob>
          <Name>2026-05-30_CCHP_index.json</Name>
          <Url>https://hostedjson.blob.core.windows.net/transparencyfiles/2026-05-30_CCHP_index.json</Url>
          <Properties><Content-Length>123</Content-Length></Properties>
        </Blob>
        <Blob>
          <Name>2026-05-30_MEDICAL_2640000002_in-network-rates.json.gz</Name>
          <Url>https://hostedjson.blob.core.windows.net/transparencyfiles/2026-05-30_MEDICAL_2640000002_in-network-rates.json.gz</Url>
          <Properties><Content-Length>456</Content-Length></Properties>
        </Blob>
      </Blobs>
    </EnumerationResults>
    """

    mrf_targets = discovery._azure_mrf_listing_targets_from_xml(
        source_dict,
        xml,
        listing_url=(
            "https://hostedjson.blob.core.windows.net/transparencyfiles"
            "?restype=container&comp=list"
        ),
        resolver={"type": "azure_mrf_listing", "toc_max_bytes": 789},
    )

    assert [mrf_target.metadata["target_file_type"] for mrf_target in mrf_targets] == [
        "table-of-contents",
        "in-network",
        "allowed-amounts",
    ]
    assert mrf_targets[0].metadata["target_kind"] == "toc_json"
    assert mrf_targets[1].metadata["target_kind"] == "file_reference"

    direct_only_targets = discovery._azure_mrf_listing_targets_from_xml(
        source_dict,
        xml,
        listing_url="https://hostedjson.blob.core.windows.net/transparencyfiles"
        "?restype=container&comp=list",
        resolver={"type": "azure_mrf_listing", "skip_toc_targets": True},
    )

    direct_file_types = tuple(mrf_target.metadata["target_file_type"] for mrf_target in direct_only_targets)
    assert direct_file_types == ("in-network", "allowed-amounts")


def test_azure_mrf_listing_targets_from_xml_extracts_group_health_coop_files():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example Group Health Cooperative",
    }
    xml = """<?xml version="1.0" encoding="utf-8"?>
    <EnumerationResults ContainerName="https://transparencyincoverage.blob.core.windows.net/public">
      <Blobs>
        <Blob>
          <Name>2026-05-27_GHC-SCW_index.json</Name>
          <Url>https://transparencyincoverage.blob.core.windows.net/public/2026-05-27_GHC-SCW_index.json</Url>
          <Properties><Content-Length>123</Content-Length></Properties>
        </Blob>
        <Blob>
          <Name>2026-05-27_GHC-SCW_999000-SELF-FUNDED_in-network-rates.json</Name>
          <Url>https://transparencyincoverage.blob.core.windows.net/public/2026-05-27_GHC-SCW_999000-SELF-FUNDED_in-network-rates.json</Url>
          <Properties><Content-Length>456</Content-Length></Properties>
        </Blob>
      </Blobs>
    </EnumerationResults>
    """

    mrf_targets = discovery._azure_mrf_listing_targets_from_xml(
        source_dict,
        xml,
        listing_url=(
            "https://transparencyincoverage.blob.core.windows.net/public"
            "?restype=container&comp=list&prefix=2026"
        ),
        resolver={
            "type": "azure_mrf_listing",
            "toc_max_bytes": 52428800,
        },
    )

    assert [mrf_target.metadata["target_file_type"] for mrf_target in mrf_targets] == [
        "table-of-contents",
        "in-network",
    ]
    assert mrf_targets[0].url.endswith("2026-05-27_GHC-SCW_index.json")
    assert mrf_targets[0].metadata["target_kind"] == "toc_json"


def test_triples_mtt_targets_keep_latest_month_files():
    """Verify this source-discovery regression contract."""
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Triple-S Salud",
    }
    response_by_key = {
        "list": [
            {
                "id": "old",
                "network": "Puerto Rico",
                "plan": "Triple-S Salud, Inc.",
                "year": "2026",
                "month": "04",
                "marketing": "In Network PR - PPO",
                "url": (
                    "https://prodtshcontenportalblob.blob.core.windows.net/"
                    "mrf-files/2026-04/2026-04-10_triples_in-network_PPO.json.gz"
                ),
            },
            {
                "id": "new-in",
                "network": "Puerto Rico",
                "plan": "Triple-S Salud, Inc.",
                "year": "2026",
                "month": "05",
                "marketing": "In Network PR - PPO",
                "url": (
                    "https://prodtshcontenportalblob.blob.core.windows.net/"
                    "mrf-files/2026-05/2026-05-10_triples_in-network_PPO.json.gz"
                ),
            },
            {
                "id": "new-oon",
                "network": "Puerto Rico",
                "plan": "Triple-S Salud, Inc.",
                "year": "2026",
                "month": "05",
                "marketing": "Out of Network PR - Allowed Amounts",
                "url": (
                    "https://prodtshcontenportalblob.blob.core.windows.net/"
                    "mrf-files/2026-05/OutOfNetwork-PPO-20260510124205.json"
                ),
            },
        ]
    }

    file_targets = discovery._triples_mtt_targets_from_payload(
        source_dict,
        response_by_key,
        resolved_from_url="https://salud.grupotriples.com/en/wp-json/app/v1/mtt",
        resolver={"type": "triples_mtt_api", "latest_month_only": True},
    )

    assert [file_target.metadata["target_file_type"] for file_target in file_targets] == [
        "in-network",
        "allowed-amounts",
    ]
    assert {file_target.metadata["triples_id"] for file_target in file_targets} == {
        "new-in",
        "new-oon",
    }
    assert file_targets[0].metadata["plan_info"][0]["plan_name"] == (
        "Triple-S Salud, Inc. - In Network PR - PPO"
    )


@pytest.mark.asyncio
async def test_resolve_triples_mtt_api_fetches_latest_select_month(monkeypatch):
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Triple-S Salud",
    }
    calls = []

    async def fake_fetch_json_value(url, **_kwargs):
        calls.append(url)
        if "month=05" in url:
            return {
                "list": [
                    {
                        "id": "48414",
                        "network": "Puerto Rico",
                        "plan": "Triple-S Salud, Inc.",
                        "year": "2026",
                        "month": "05",
                        "marketing": "In Network PR - PPO",
                        "url": (
                            "https://prodtshcontenportalblob.blob.core.windows.net/"
                            "mrf-files/2026-05/"
                            "2026-05-10_triples_in-network_PPO.json.gz"
                        ),
                    }
                ]
            }
        return {
            "selects": {
                "year": [{"year": "2025"}, {"year": "2026"}],
                "month": [{"month": "04"}, {"month": "05"}],
            },
            "list": [],
        }

    monkeypatch.setattr(discovery, "_fetch_json_value", fake_fetch_json_value)

    [crawl_target] = await discovery._resolve_triples_mtt_api(
        source_dict,
        "https://salud.grupotriples.com/en/transparency-in-coverage-machine-readable-files/",
        {
            "type": "triples_mtt_api",
            "api_url": "https://salud.grupotriples.com/en/wp-json/app/v1/mtt",
            "network": "Puerto Rico",
            "plan": "Triple-S Salud, Inc.",
            "latest_month_only": True,
        },
        None,
    )

    assert len(calls) == 2
    assert "year=2026" in calls[1]
    assert "month=05" in calls[1]
    assert crawl_target.metadata["target_file_type"] == "in-network"


def test_payercompass_targets_from_structure_use_file_list_download_ids():
    """Verify this source-discovery regression contract."""
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "PayerCompass Plan",
    }
    resolver_config_dict = {
        "type": "payercompass_mrf",
        "download_path": "/api/File/Download",
        "max_timeframes": 2,
    }
    structure_response_dict = {
        "mrfConfig": {
            "timeFrames": [
                {
                    "id": "2026-06-01_2",
                    "name": "June 01, 2026",
                    "fileCount": 1,
                    "fileType": 2,
                },
                {
                    "id": "2026-06-01_1",
                    "name": "June 01, 2026",
                    "fileCount": 2,
                    "fileType": 1,
                },
            ]
        }
    }
    files_by_timeframe = {
        "2026-06-01_2": [
            {
                "id": "oon-file",
                "name": "2026-06-01_example_index.json.zip",
                "size": "8.97 KB",
            }
        ],
        "2026-06-01_1": [
            {
                "id": "inn-file",
                "name": "2026-06-01_example_in-network-rates.json.zip",
                "size": "25.85 MB",
            }
        ],
    }

    download_targets = discovery._payercompass_targets_from_structure(
        catalog_source_dict,
        base_url="https://example.mrf.payercompass.com/",
        resolver=resolver_config_dict,
        structure=structure_response_dict,
        file_lists=files_by_timeframe,
    )

    assert [download_target.url for download_target in download_targets] == [
        "https://example.mrf.payercompass.com/api/File/Download?Id=oon-file",
        "https://example.mrf.payercompass.com/api/File/Download?Id=inn-file",
    ]
    assert download_targets[0].metadata["target_file_type"] == "allowed-amounts"
    assert download_targets[0].metadata["container_format"] == "zip"
    assert download_targets[0].metadata["size_bytes"] == 8970
    assert download_targets[1].metadata["target_file_type"] == "in-network"
    assert (
        download_targets[1]
        .metadata["payercompass_file_name"]
        .endswith("in-network-rates.json.zip")
    )


def test_pc_file_list_wrappers():
    assert discovery._payercompass_file_list_items(
        {"files": [{"fileId": "file-1", "fileName": "rates.json.gz"}]}
    ) == [{"fileId": "file-1", "fileName": "rates.json.gz"}]
    assert discovery._payercompass_file_list_items(
        {"data": [{"id": "file-2", "name": "allowed.zip"}]}
    ) == [{"id": "file-2", "name": "allowed.zip"}]
    assert discovery._payercompass_file_list_items(
        {"result": {"items": [{"downloadId": "file-3", "label": "index.zip"}]}}
    ) == [{"downloadId": "file-3", "label": "index.zip"}]


def test_pc_file_key_aliases():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "PayerCompass Plan",
    }
    resolver_dict = {"type": "payercompass_mrf", "download_path": "/api/File/Download"}
    structure_dict = {
        "mrfConfig": {
            "timeFrames": [
                {
                    "id": "2026-06-01_1",
                    "name": "June 01, 2026",
                    "fileCount": 1,
                    "fileType": 1,
                }
            ]
        }
    }

    [crawl_target] = discovery._payercompass_targets_from_structure(
        source_dict,
        base_url="https://example.mrf.payercompass.com/",
        resolver=resolver_dict,
        structure=structure_dict,
        file_lists={
            "2026-06-01_1": [
                {
                    "fileId": "alternate-file-id",
                    "fileName": "2026-06-01_example_in-network-rates.json.gz",
                    "sizeBytes": 12345,
                }
            ]
        },
    )

    assert (
        crawl_target.url
        == "https://example.mrf.payercompass.com/api/File/Download?Id=alternate-file-id"
    )
    assert crawl_target.metadata["payercompass_file_name"].endswith(
        "in-network-rates.json.gz"
    )
    assert crawl_target.metadata["size_bytes"] == 12345


@pytest.mark.asyncio
async def test_pc_resolver_file_list_wrapper(
    monkeypatch,
):
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "PayerCompass Plan",
    }
    resolver_dict = {"type": "payercompass_mrf", "download_path": "/api/File/Download"}
    structure_dict = {
        "mrfConfig": {
            "timeFrames": [
                {
                    "id": "2026-06-01_1",
                    "name": "June 01, 2026",
                    "fileCount": 1,
                    "fileType": 1,
                }
            ]
        }
    }

    async def fake_post_json(url, payload, **_kwargs):
        assert url == "https://example.mrf.payercompass.com/api/Home/GetStructureInfo"
        assert payload == {}
        return structure_dict

    async def fake_post_json_value(url, payload, **_kwargs):
        assert url == "https://example.mrf.payercompass.com/api/File/List"
        assert payload == {"timeFrameId": "2026-06-01_1"}
        return {
            "files": [
                {
                    "fileId": "wrapped-file",
                    "fileName": "2026-06-01_example_in-network-rates.json.gz",
                }
            ]
        }

    monkeypatch.setattr(discovery, "_post_json", fake_post_json)
    monkeypatch.setattr(discovery, "_post_json_value", fake_post_json_value)

    [crawl_target] = await discovery._resolve_payercompass_mrf(
        source_dict,
        "https://example.mrf.payercompass.com/",
        resolver_dict,
        None,
    )

    assert (
        crawl_target.url
        == "https://example.mrf.payercompass.com/api/File/Download?Id=wrapped-file"
    )
    assert crawl_target.metadata["target_file_type"] == "in-network"


@pytest.mark.asyncio
async def test_resolve_payercompass_mrf_enriches_plans_from_index_zip(monkeypatch):
    """Verify this source-discovery regression contract."""
    catalog_source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "PayerCompass Plan",
    }
    resolver_config_dict = {
        "type": "payercompass_mrf",
        "download_path": "/api/File/Download",
        "max_timeframes": 2,
    }
    structure_response_dict = {
        "mrfConfig": {
            "timeFrames": [
                {
                    "id": "2026-06-01_2",
                    "name": "June 01, 2026",
                    "fileCount": 1,
                    "fileType": 2,
                },
                {
                    "id": "2026-06-01_1",
                    "name": "June 01, 2026",
                    "fileCount": 1,
                    "fileType": 1,
                },
            ]
        }
    }
    files_by_timeframe = {
        "2026-06-01_2": [
            {
                "id": "index-file",
                "name": "2026-06-01_example_index.json.zip",
                "size": "8.97 KB",
            }
        ],
        "2026-06-01_1": [
            {
                "id": "inn-file",
                "name": (
                    "2026-06-01_example_11111111-2222-3333-4444-555555555555_"
                    "in-network-rates.json.zip"
                ),
                "size": "25.85 MB",
            }
        ],
    }
    index_toc_dict = {
        "reporting_entity_name": "Example TPA",
        "reporting_entity_type": "third_party_administrator",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "Acme Health Plan",
                        "plan_id_type": "EIN",
                        "plan_id": "123456789",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {
                        "location": (
                            "https://example.mrf.payercompass.com/file/get?name="
                            "2026-06-01_example_11111111-2222-3333-4444-"
                            "555555555555_in-network-rates.json.zip"
                        )
                    }
                ],
            },
            {
                "reporting_plans": [
                    {
                        "plan_name": "Beta Health Plan",
                        "plan_id_type": "EIN",
                        "plan_id": "987654321",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {
                        "location": (
                            "https://example.mrf.payercompass.com/file/get?name="
                            "2026-06-01_example_11111111-2222-3333-4444-"
                            "555555555555_in-network-rates.json.zip"
                        )
                    }
                ],
            },
        ],
    }
    fetch_calls = []

    async def fake_post_json(url, payload, **_kwargs):
        assert url == "https://example.mrf.payercompass.com/api/Home/GetStructureInfo"
        assert payload == {}
        return structure_response_dict

    async def fake_post_json_value(url, payload, **_kwargs):
        assert url == "https://example.mrf.payercompass.com/api/File/List"
        return files_by_timeframe[payload["timeFrameId"]]

    async def fake_fetch_zip_json_values(url, **_kwargs):
        fetch_calls.append(url)
        return [("2026-06-01_example_index.json", index_toc_dict)]

    monkeypatch.setattr(discovery, "_post_json", fake_post_json)
    monkeypatch.setattr(discovery, "_post_json_value", fake_post_json_value)
    monkeypatch.setattr(discovery, "_fetch_zip_json_values", fake_fetch_zip_json_values)

    resolved_targets = await discovery._resolve_payercompass_mrf(
        catalog_source_dict,
        "https://example.mrf.payercompass.com/",
        resolver_config_dict,
        None,
    )

    assert fetch_calls == [
        "https://example.mrf.payercompass.com/api/File/Download?Id=index-file"
    ]
    index_target, in_network_target = resolved_targets
    assert "plan_info" not in index_target.metadata
    assert in_network_target.metadata["payercompass_plan_info_source"] == "index_toc"
    assert in_network_target.metadata["reporting_entity_name"] == "Example TPA"
    assert in_network_target.metadata["plan_info"] == [
        {
            "plan_id": "123456789",
            "plan_id_type": "EIN",
            "plan_market_type": "group",
            "plan_name": "Acme Health Plan",
        },
        {
            "plan_id": "987654321",
            "plan_id_type": "EIN",
            "plan_market_type": "group",
            "plan_name": "Beta Health Plan",
        },
    ]


def test_metadata_text_rows_only_store_direct_body_files():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Collective Health",
    }
    text = """
File Scope: allowed-amounts | Plan Name: PPO | Sponsor EIN: 010627671 | https://example.com/2026-05-20_PPO_allowed-amounts.json
File scope: In Network | Plan Name: HDHP | Sponsor EIN: 741670067 | https://bcbsil.com/asomrf?EIN=741670067
"""

    plan_rows, file_rows = discovery._metadata_text_rows_from_content(
        source_dict, "https://example.com/allowed-amount-meta.txt", text
    )

    assert len(plan_rows) in {1}
    assert len(file_rows) in {1}
    assert plan_rows[0]["plan_id"] == "010627671"
    assert plan_rows[0]["reporting_entity_type"] == "third_party_administrator"
    assert file_rows[0]["file_type"] == "allowed-amounts"
    assert (
        file_rows[0]["url"] == "https://example.com/2026-05-20_PPO_allowed-amounts.json"
    )
    assert file_rows[0]["metadata_json"]["resolver"] == "html_metadata_text"


def test_metadata_text_rows_accept_zip_body_references():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Example TPA",
    }
    text = "File Scope: In Network | Plan Name: PPO | https://example.com/in-network-rates.zip"

    _, file_rows = discovery._metadata_text_rows_from_content(
        source_dict, "https://example.com/meta.txt", text
    )

    assert len(file_rows) in {1}
    assert file_rows[0]["file_type"] == "in-network"
    assert file_rows[0]["metadata_json"]["container_format"] == "zip"


def test_file_reference_target_rows_preserve_plan_info_for_client_indexing():
    source_dict = {
        "source_id": "source_1",
        "payer_id": "payer_1",
        "display_name": "Lucent Health",
    }
    crawl_target = discovery.CrawlTarget(
        source=source_dict,
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

    file_row = discovery._toc_target_file_row(crawl_target)
    [plan_row] = discovery._plan_rows_from_target_metadata(crawl_target)

    assert file_row["plan_ids"] == ["042171239"]
    assert file_row["market_types"] == ["group"]
    assert file_row["metadata_json"]["source_format"] == "zip"
    assert plan_row["plan_id"] == "042171239"
    assert plan_row["plan_id_type"] == "ein"
    assert plan_row["reporting_entity_name"] == "Lucent Health"


def test_crawl_target_limit_prefers_resolved_json_before_landing_pages():
    source_dict = {"source_id": "source_1"}
    landing = discovery.CrawlTarget(source=source_dict, url="https://example.com/mrf")
    resolved = discovery.CrawlTarget(
        source=source_dict,
        url="https://example.com/index.json",
        resolved_from_url="https://example.com/js/script.js",
    )
    direct_json = discovery.CrawlTarget(
        source=source_dict, url="https://example.com/direct.json"
    )

    ordered = sorted([landing, direct_json, resolved], key=discovery._crawl_target_rank)

    assert ordered == [resolved, direct_json, landing]


def test_direct_toc_url_gate_skips_html_landing_pages():
    assert discovery._is_direct_toc_url("https://example.com/index.json") is True
    assert (
        discovery._is_direct_toc_url(
            "https://example.com/app/public/#/one/machine-readable-transparency-in-coverage"
        )
        is False
    )
    assert discovery._is_direct_toc_url("https://example.com/transparency") is False


def test_direct_table_of_contents_json_is_not_body_file():
    url = (
        "https://data.networkhealth.com/price-transparency/"
        "nhpricetransparency_table_of_contents.json"
    )
    source_dict = {"source_id": "source_1", "display_name": "Network Health"}

    assert discovery.classify_hosting_platform(url) == "direct_toc"
    assert discovery._direct_mrf_body_crawl_target(source_dict, url) is None
    target = discovery._direct_toc_crawl_target(source_dict, url)
    assert target is not None
    assert target.metadata["target_kind"] == "toc_json"
    assert target.metadata["target_file_type"] == "table-of-contents"


def test_public_rows_prefer_verified_direct_or_platform_urls():
    markdown = """
| Payer | Type | Public MRF TOC / landing URL | Notes |
|---|---|---|---|
| CareSource | medicaid_mco | https://www.caresource.com/vendor/tic/tic-data-index.json | public direct TOC |
| Independent Health | regional | https://web.healthsparq.com/healthsparq/public/#/one/insurerCode=IHNY_I&brandCode=IHNY&productCode=MRF/machine-readable-transparency-in-coverage | public HealthSparq files |
| Network Health | regional | https://data.networkhealth.com/price-transparency/nhpricetransparency_table_of_contents.json | public direct TOC; aliases: Froedtert Health Plan, Froedtert ThedaCare |
| Froedtert Health Plan | provider_sponsored | https://www.froedtert.com/price-transparency | observed stale; represented by Network Health direct table of contents |
| CommunityCare of OK | regional | https://www.ccok.com/Price-Transparency/Machine-Readable/ | public MRF links; aliases: Community Care of Oklahoma, CCOK |
| Physicians Health Plan | provider_sponsored | https://www.uofmhealthplan.org/members/price-transparency-and-interoperability | public MRF links; aliases: U-M Health Plan, University of Michigan Health Plan |
| Physicians Health Plan of N. Indiana | provider_sponsored | https://services.phpni.com/machine-readable-files/files/phpni/phpni | public MRF links; aliases: PHPNI, PHP Northern Indiana |
| Dean Health Plan | provider_sponsored | https://deancare.healthsparq.com/healthsparq/public/#/one/insurerCode=MEDICAHEALTHPLANS_I&brandCode=DEAN&productCode=MRF/machine-readable-transparency-in-coverage | public HealthSparq files |
| McLaren Health Plan | provider_sponsored | https://www.mclarenhealthplan.org/mhp/transparency-in-coverage-and-no-surprises-act | public HTML MRF files |
| AmeriHealth Caritas Next | regional | https://www.amerihealthcaritasnext.com/json | public HTML MRF files |
| AvMed | regional | https://www.avmed.org/en/for-developers | public AvMed developer page |
| HMAA | regional | https://www.hmaa.com/wp-content/uploads/2022/06/MRF_HMAA.zip | public HMAA MRF ZIP containing table-of-contents JSON; aliases: Hawaii Medical Assurance Association |
| Avera Health Plans | provider_sponsored | https://www.averahealthplans.com/insurance/about/legal-privacy-notices/transparency-in-coverage/ | official transparency page confirms machine-readable files but does not expose direct automated file URLs; source tier: coverage evidence; aliases: Avera, DakotaCare |
| Chorus Community Health Plans | regional | https://chorushealthplans.org/ifp/past-ifp-member-resources/transparency-in-coverage | public transparency URL; aliases: Chorus Community HP, Children's Community Health Plan, Children's Community HP, CCHP, Together with CCHP |
| Children's Community HP | provider_sponsored | - | represented by Chorus Community Health Plans public source; aliases: Children's Community Health Plan, CCHP |
| First Choice Health | regional | https://www.fchn.com/machine-readable-files | official FCHN MRF page, but Cloudflare blocks automated import from the current crawler; source tier: coverage evidence; aliases: First Choice Health Network |
| Piedmont Health Plan | regional | https://pchp.net/index.php/transparency-in-coverage.html | observed stale; no current automated MRF source is verified |
"""
    by_name = {candidate.payer_name: candidate for candidate in discovery.parse_master_list(markdown)}

    assert by_name["CareSource"].hosting_platform == "direct_toc"
    assert by_name["Independent Health"].hosting_platform == "healthsparq"
    assert by_name["Network Health"].hosting_platform == "direct_toc"
    assert "Froedtert Health Plan" in by_name["Network Health"].aliases
    assert by_name["Froedtert Health Plan"].status == "stale"
    assert by_name["CommunityCare of OK"].hosting_platform == "html_mrf_links"
    assert "Community Care of Oklahoma" in by_name["CommunityCare of OK"].aliases
    assert by_name["Physicians Health Plan"].hosting_platform == "html_mrf_links"
    assert "U-M Health Plan" in by_name["Physicians Health Plan"].aliases
    assert (
        by_name["Physicians Health Plan of N. Indiana"].hosting_platform
        == "html_mrf_links"
    )
    assert "PHPNI" in by_name["Physicians Health Plan of N. Indiana"].aliases
    assert by_name["Dean Health Plan"].hosting_platform == "healthsparq"
    assert by_name["McLaren Health Plan"].hosting_platform == "html_mrf_links"
    assert by_name["AmeriHealth Caritas Next"].hosting_platform == "html_mrf_links"
    assert by_name["AvMed"].hosting_platform == "avmed_html_mrf_links"
    assert by_name["HMAA"].hosting_platform == "direct_toc"
    assert by_name["HMAA"].source_tier == "mrf_importable"
    assert "Hawaii Medical Assurance Association" in by_name["HMAA"].aliases
    assert by_name["Avera Health Plans"].source_tier == "coverage_evidence"
    assert "DakotaCare" in by_name["Avera Health Plans"].aliases
    assert "Children's Community Health Plan" in by_name["Chorus Community Health Plans"].aliases
    assert by_name["Children's Community HP"].status == "needs_review"
    assert by_name["First Choice Health"].source_tier == "coverage_evidence"
    assert by_name["Piedmont Health Plan"].status == "stale"


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
    assert (
        row["metadata_json"]["resolved_from_url"] == "https://example.com/api/v1/blobs/"
    )


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


@pytest.mark.asyncio
async def test_resolve_crawl_targets_progress_reports_source_pages(monkeypatch):
    progress = []
    target_limits = []

    async def fake_crawl_targets_for_source(source, url, session, **_kwargs):
        target_limits.append(_kwargs.get("target_limit"))
        return [discovery.CrawlTarget(source=source, url=f"{url}/index.json")]

    monkeypatch.setattr(
        discovery, "_crawl_targets_for_source", fake_crawl_targets_for_source
    )
    monkeypatch.setattr(
        discovery, "enqueue_live_progress", lambda **payload: progress.append(payload)
    )

    crawl_targets, observations = await discovery._resolve_crawl_targets(
        [
            {"source_id": "source_1", "index_url": "https://example.com/source-1"},
            {"source_id": "source_2", "index_url": "https://example.com/source-2"},
        ],
        session=object(),
        run_id="run_1",
        progress_run_id="control_run_1",
        concurrency=1,
        crawl_target_limit=7,
    )

    assert len(crawl_targets) == 2
    assert observations == []
    assert target_limits == [7, 7]
    assert progress[-1]["phase"] == "resolving source pages"
    assert progress[-1]["unit"] == "sources"
    assert progress[-1]["message"] == "resolved 2/2 source pages"
    assert progress[0]["detail"] == "waiting on: source_2"
    assert progress[-1].get("detail") is None


@pytest.mark.asyncio
async def test_resolve_crawl_targets_uses_bounded_default_timeout(monkeypatch):
    observed_timeouts = []

    async def fake_crawl_targets_for_source(source, url, session, **_kwargs):
        return [discovery.CrawlTarget(source=source, url=f"{url}/index.json")]

    async def recording_wait_for(awaitable, *, timeout):
        observed_timeouts.append(timeout)
        return await awaitable

    monkeypatch.delenv("HLTHPRT_MRF_SOURCE_RESOLVE_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setattr(
        discovery, "_crawl_targets_for_source", fake_crawl_targets_for_source
    )
    monkeypatch.setattr(discovery.asyncio, "wait_for", recording_wait_for)

    targets, observations = await discovery._resolve_crawl_targets(
        [{"source_id": "source_1", "index_url": "https://example.com/source-1"}],
        session=object(),
        run_id="run_1",
        progress_run_id=None,
        concurrency=1,
    )

    assert len(targets) == 1
    assert observations == []
    assert observed_timeouts == [60.0]


@pytest.mark.asyncio
async def test_resolve_crawl_targets_times_out_slow_source(monkeypatch):
    async def slow_crawl_targets_for_source(*_args, **_kwargs):
        await asyncio.sleep(0.05)
        return []

    monkeypatch.setattr(
        discovery, "_crawl_targets_for_source", slow_crawl_targets_for_source
    )
    monkeypatch.setenv("HLTHPRT_MRF_SOURCE_RESOLVE_TIMEOUT_SECONDS", "0.001")

    targets, observations = await discovery._resolve_crawl_targets(
        [
            {
                "source_id": "slow_source",
                "index_url": "https://example.com/source",
            }
        ],
        session=object(),
        run_id="run_1",
        progress_run_id=None,
        concurrency=1,
    )

    assert targets == []
    assert len(observations) in {1}
    assert observations[0]["status"] == "crawl_failed"


@pytest.mark.asyncio
async def test_crawl_toc_metadata_reports_expanded_target_count(monkeypatch):
    """Verify this source-discovery regression contract."""
    progress = []

    source_rows = [
        {
            "source_id": "source_1",
            "payer_id": "payer_1",
            "index_url": "https://example.com/source-1",
        },
        {
            "source_id": "source_2",
            "payer_id": "payer_2",
            "index_url": "https://example.com/source-2",
        },
    ]

    async def fake_resolve_crawl_targets(rows, **_kwargs):
        return [
            discovery.CrawlTarget(
                source=rows[0],
                url="https://example.com/source-1/in-network-1.json",
                metadata={
                    "target_kind": "file_reference",
                    "target_file_type": "in-network",
                },
            ),
            discovery.CrawlTarget(
                source=rows[0],
                url="https://example.com/source-1/in-network-2.json",
                metadata={
                    "target_kind": "file_reference",
                    "target_file_type": "in-network",
                },
            ),
            discovery.CrawlTarget(
                source=rows[1],
                url="https://example.com/source-2/in-network-1.json",
                metadata={
                    "target_kind": "file_reference",
                    "target_file_type": "in-network",
                },
            ),
        ], []

    async def fake_push_crawl_row_batches(*_args, **_kwargs):
        return None

    monkeypatch.setattr(discovery, "_resolve_crawl_targets", fake_resolve_crawl_targets)
    monkeypatch.setattr(
        discovery, "_push_crawl_row_batches", fake_push_crawl_row_batches
    )
    monkeypatch.setattr(
        discovery, "enqueue_live_progress", lambda **payload: progress.append(payload)
    )

    await discovery._crawl_toc_metadata(
        source_rows,
        test_mode=False,
        run_id="run_1",
        progress_run_id="control_run_1",
        max_toc_bytes=1024,
        concurrency=1,
        crawl_target_limit=2,
    )

    [expanded_event_list] = [
        event for event in progress if event["phase"] == "resolved source TOCs"
    ]
    assert expanded_event_list["unit"] == "targets"
    assert expanded_event_list["done"] == 3
    assert expanded_event_list["total"] == 3
    assert (
        expanded_event_list["message"]
        == "resolved 3 TOC targets from 2 source pages; crawling first 2"
    )
    assert [
        event["total"]
        for event in progress
        if event["phase"] == "crawling TOC metadata"
    ] == [2, 2]
    assert [
        event["unit"] for event in progress if event["phase"] == "crawling TOC metadata"
    ] == ["targets", "targets"]


@pytest.mark.asyncio
async def test_crawl_toc_metadata_disables_row_write_timeout_by_default(monkeypatch):
    row_write_timeouts = []

    async def fake_resolve_crawl_targets(*_args, **_kwargs):
        return [], []

    async def fake_push_crawl_row_batches(*_args, **kwargs):
        row_write_timeouts.append(kwargs["row_write_timeout"])

    monkeypatch.delenv("HLTHPRT_MRF_CRAWL_ROW_WRITE_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setattr(discovery, "_resolve_crawl_targets", fake_resolve_crawl_targets)
    monkeypatch.setattr(
        discovery, "_push_crawl_row_batches", fake_push_crawl_row_batches
    )

    await discovery._crawl_toc_metadata(
        [
            {
                "source_id": "source_1",
                "payer_id": "payer_1",
                "index_url": "https://example.com/source",
            }
        ],
        test_mode=False,
        run_id="run_1",
        max_toc_bytes=1024,
        concurrency=1,
    )

    assert row_write_timeouts == [0.0, 0.0]


@pytest.mark.asyncio
async def test_crawl_table_of_contents_metadata_times_out_slow_target(monkeypatch):
    pushed_batches = []
    source_rows = [
        {
            "source_id": "source_1",
            "payer_id": "payer_1",
            "index_url": "https://example.com/source",
        }
    ]
    slow_crawl_target = discovery.CrawlTarget(
        source=source_rows[0],
        url="https://example.com/slow-index.json",
    )

    async def fake_resolve_crawl_targets(*_args, **_kwargs):
        return [slow_crawl_target], []

    async def slow_fetch_json(*_args, **_kwargs):
        await asyncio.sleep(0.05)
        return {}

    async def fake_push_crawl_row_batches(plan_rows, file_rows, observation_rows, **_kwargs):
        pushed_batches.append(
            {
                "plan_rows": list(plan_rows),
                "file_rows": list(file_rows),
                "observation_rows": list(observation_rows),
            }
        )

    monkeypatch.setenv("HLTHPRT_MRF_TOC_TARGET_TIMEOUT_SECONDS", "0.001")
    monkeypatch.setattr(discovery, "_resolve_crawl_targets", fake_resolve_crawl_targets)
    monkeypatch.setattr(discovery, "_fetch_json", slow_fetch_json)
    monkeypatch.setattr(
        discovery, "_push_crawl_row_batches", fake_push_crawl_row_batches
    )

    plan_count, file_count, crawl_observations = await discovery._crawl_toc_metadata(
        source_rows,
        test_mode=False,
        run_id="run_1",
        max_toc_bytes=1024,
        concurrency=1,
    )

    failed_observations = [
        observation_row
        for pushed_batch in pushed_batches
        for observation_row in pushed_batch["observation_rows"]
        if observation_row.get("status") == "crawl_failed"
    ]
    assert plan_count == 0
    assert file_count == 1
    assert len(crawl_observations) in {1}
    assert len(failed_observations) == 1
    assert "timed out" in failed_observations[0]["error"]


@pytest.mark.asyncio
async def test_crawl_toc_metadata_expands_zipped_toc_file_reference(monkeypatch):
    """Verify this source-discovery regression contract."""
    pushed_batches = []
    source_rows = [
        {
            "source_id": "source_1",
            "payer_id": "payer_1",
            "index_url": "https://example.com/source",
        }
    ]
    crawl_target = discovery.CrawlTarget(
        source=source_rows[0],
        url="https://example.com/2026-06-01_example_index.zip",
        label="Example ZIP TOC",
        metadata={
            "target_kind": "file_reference",
            "target_file_type": "table-of-contents",
            "container_format": "zip",
        },
    )

    async def fake_resolve_crawl_targets(*_args, **_kwargs):
        return [crawl_target], []

    async def fake_fetch_zip_json_values(*_args, **_kwargs):
        return [("2026-06-01_example_index.json", {"toc": True})]

    def fake_toc_rows_from_content(source, url, toc):
        assert source["source_id"] == "source_1"
        assert url == "https://example.com/2026-06-01_example_index.zip"
        assert toc == {"toc": True}
        return [{"plan_id": "plan_1"}], [{"mrf_file_id": "file_1"}]

    async def fake_push_crawl_row_batches(plan_rows, file_rows, observation_rows, **_kwargs):
        pushed_batches.append(
            {
                "plan_rows": list(plan_rows),
                "file_rows": list(file_rows),
                "observation_rows": list(observation_rows),
            }
        )

    monkeypatch.setattr(discovery, "_resolve_crawl_targets", fake_resolve_crawl_targets)
    monkeypatch.setattr(discovery, "_fetch_zip_json_values", fake_fetch_zip_json_values)
    monkeypatch.setattr(discovery, "_toc_rows_from_content", fake_toc_rows_from_content)
    monkeypatch.setattr(
        discovery, "_push_crawl_row_batches", fake_push_crawl_row_batches
    )

    plans, files, observations = await discovery._crawl_toc_metadata(
        source_rows,
        test_mode=False,
        run_id="run_1",
        max_toc_bytes=1024,
        concurrency=1,
    )

    assert plans in {1}
    assert files == 2
    assert len(observations) in {1}
    assert pushed_batches[0]["file_rows"][0]["file_type"] == "table-of-contents"
    assert pushed_batches[-1]["plan_rows"] == [{"plan_id": "plan_1"}]
    assert pushed_batches[-1]["file_rows"][0]["mrf_file_id"] == "file_1"
    assert (
        pushed_batches[-1]["file_rows"][0]["metadata_json"]["target_label"]
        == "Example ZIP TOC"
    )


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

    plan_rows, file_rows = discovery._toc_rows_from_content(
        {"source_id": "source_1"}, "https://example.com/index.json", {}
    )

    assert plan_rows == []
    assert file_rows == []


def test_parse_toc_catalog_entries_skips_non_mrf_body_locations():
    source_jobs = importlib.import_module("process.ptg_parts.source_jobs")
    toc_dict = {
        "reporting_entity_name": "HealthComp",
        "reporting_entity_type": "third_party_administrator",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_id": "123",
                        "plan_market_type": "group",
                        "plan_name": "Example Plan",
                    }
                ],
                "in_network_files": [
                    {"location": "https://www.zelis.com/"},
                    {
                        "location": "https://cdn.example.com/2026-06_in-network-rates.json.gz"
                    },
                    {
                        "location": (
                            "https://cdn.example.com/2026-06_ELAP_allowed-amounts.json.gz"
                        )
                    },
                    {
                        "location": (
                            "https://www.asrhealthbenefits.com/home/umbraco/surface/"
                            "mrfdownload/index?g=1208&i=595&t=InNetwork"
                        )
                    },
                ],
                "allowed_amount_file": {"location": "Missing file"},
                "drug_file": {"location": "ftp://example.com/ndc.json.gz"},
            }
        ],
    }

    entries = source_jobs.parse_toc_catalog_entries(
        toc_dict, "https://healthcomp.sapphiremrfhub.com/tocs/index.json"
    )

    assert [entry.source_type for entry in entries] == [
        "table-of-contents",
        "in-network",
        "allowed-amounts",
        "in-network",
    ]
    assert [entry.original_url for entry in entries] == [
        "https://healthcomp.sapphiremrfhub.com/tocs/index.json",
        "https://cdn.example.com/2026-06_in-network-rates.json.gz",
        "https://cdn.example.com/2026-06_ELAP_allowed-amounts.json.gz",
        (
            "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload"
            "?groupNumber=1208&fileType=InNetwork&fileId=595"
        ),
    ]


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
                    {
                        "plan_id": "123",
                        "plan_id_type": "ein",
                        "plan_market_type": "group",
                        "plan_name": "Plan A",
                    },
                ),
            )
        ],
    )

    plan_rows, file_rows = discovery._toc_rows_from_content(
        {"source_id": "source_1"}, "https://example.com/index.json", {}
    )

    assert len(file_rows) in {1}
    # The exact per-file plan list, including plan_id_type, is preserved for catalog
    # consumers.
    assert file_rows[0]["metadata_json"]["plan_info"] == [
        {
            "plan_id": "123",
            "plan_id_type": "ein",
            "plan_market_type": "group",
            "plan_name": "Plan A",
        }
    ]
    assert file_rows[0]["metadata_json"]["container_format"] == "gzip"
    assert plan_rows[0]["plan_id_type"] == "ein"


def test_toc_rows_infer_dental_benefit_line_from_file_name(monkeypatch):
    monkeypatch.setattr(
        discovery,
        "parse_toc_catalog_entries",
        lambda *_args: [
            types.SimpleNamespace(
                source_type="in-network",
                original_url=(
                    "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/"
                    "2026-06-01/2026-06-01_UMR--Inc-_Third-Party-Administrator_"
                    "DENTAL-BENEFIT-PROVIDERS-AND-CONNECTION-DENTAL_0D_"
                    "in-network-rates.json.gz"
                ),
                canonical_url=(
                    "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/"
                    "2026-06-01/2026-06-01_UMR--Inc-_Third-Party-Administrator_"
                    "DENTAL-BENEFIT-PROVIDERS-AND-CONNECTION-DENTAL_0D_"
                    "in-network-rates.json.gz"
                ),
                from_index_url="https://transparency-in-coverage.uhc.com/",
                description="UHC dental rates",
                domain="transparency-in-coverage.uhc.com",
                reporting_entity_name="UMR, Inc.",
                reporting_entity_type="third_party_administrator",
                plan_info=(
                    {
                        "plan_id": "123",
                        "plan_market_type": "group",
                        "plan_name": "Dental Benefit Providers",
                    },
                ),
            )
        ],
    )

    _, file_rows = discovery._toc_rows_from_content(
        {
            "source_id": "source_1",
            "metadata_json": {"benefit_lines": ["medical"]},
        },
        "https://transparency-in-coverage.uhc.com/index.json",
        {},
    )

    assert file_rows[0]["metadata_json"]["benefit_lines"] == ["dental"]
    assert file_rows[0]["metadata_json"]["benefit_line"] == "dental"


@pytest.mark.parametrize(
    ("file_label", "description"),
    [
        ("EXAMPLE-NVA-IN-NETWORK-RATES", "Example NVA rates"),
        ("EXAMPLE-VERSANT-HEALTH-IN-NETWORK-RATES", "Example Versant Health rates"),
    ],
)
def test_toc_rows_infer_vision_benefit_line_from_precise_vendor_tokens(
    monkeypatch, file_label, description
):
    monkeypatch.setattr(
        discovery,
        "parse_toc_catalog_entries",
        lambda *_args: [
            types.SimpleNamespace(
                source_type="in-network",
                original_url=f"https://example.com/{file_label}.json.gz",
                canonical_url=f"https://example.com/{file_label}.json.gz",
                from_index_url="https://example.com/index.json",
                description=description,
                domain="example.com",
                reporting_entity_name="Example Payer",
                reporting_entity_type="third_party_administrator",
                plan_info=(
                    {
                        "plan_id": "123",
                        "plan_market_type": "group",
                        "plan_name": "Plan A",
                    },
                ),
            )
        ],
    )

    _, file_rows = discovery._toc_rows_from_content(
        {
            "source_id": "source_1",
            "metadata_json": {"benefit_lines": ["medical"]},
        },
        "https://example.com/index.json",
        {},
    )

    assert file_rows[0]["metadata_json"]["benefit_lines"] == ["vision"]
    assert file_rows[0]["metadata_json"]["benefit_line"] == "vision"


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
                plan_info=(
                    {
                        "plan_id": "123",
                        "plan_market_type": "group",
                        "plan_name": "Plan A",
                    },
                ),
            )
        ],
    )

    _, file_rows = discovery._toc_rows_from_content(
        {"source_id": "source_1"},
        "https://example.com/index.json",
        {"version": "3.5.5 f501aab30e8114503c6248f178858c9a27ba9c14"},
    )

    assert file_rows[0]["schema_version"] == "3.5.5 f501aab30e8114503c6248f178"


def test_parse_file_probe_types_defaults_and_dedupes():
    assert discovery._parse_file_probe_types(None) == ("in-network", "allowed-amounts")
    assert discovery._parse_file_probe_types(
        "in-network, allowed-amounts in-network"
    ) == ("in-network", "allowed-amounts")


def test_parse_text_filter_values_defaults_and_dedupes():
    assert discovery._parse_text_filter_values(None) == ()
    assert discovery._parse_text_filter_values("tpa, network/tpa TPA") == (
        "tpa",
        "network/tpa",
    )


def test_is_candidate_text_filter_match_by_entity_type_and_payer_name():
    candidate = discovery.SourceCandidate(
        payer_name="Collective Health", provider="master-list", entity_type="tpa"
    )

    assert (
        discovery._is_candidate_text_filter_match(
            candidate, entity_types=("tpa",), payer_query=None
        )
        is True
    )
    assert (
        discovery._is_candidate_text_filter_match(
            candidate, entity_types=("network/tpa",), payer_query=None
        )
        is False
    )
    assert (
        discovery._is_candidate_text_filter_match(
            candidate, entity_types=(), payer_query="collective"
        )
        is True
    )
    assert (
        discovery._is_candidate_text_filter_match(
            candidate, entity_types=(), payer_query="meritain"
        )
        is False
    )


def test_discovery_run_mode_combines_probe_files():
    assert (
        discovery._discovery_run_mode(crawl=False, check_urls=False, probe_files=True)
        == "probe_files"
    )
    assert (
        discovery._discovery_run_mode(crawl=True, check_urls=True, probe_files=True)
        == "crawl+check_urls+probe_files"
    )


def test_file_probe_observation_and_update_payloads_include_etag_and_last_modified():
    crawl_target_by_field = {
        "mrf_file_id": "file_1",
        "url": "https://example.com/rates.json.gz",
        "file_type": "in-network",
        "payer_id": "payer_1",
        "payer_name": "Collective Health",
        "entity_type": "tpa",
    }
    checked_at = discovery.dt.datetime(2026, 6, 5, 12, 0, 0)
    head_dict = {
        "status": "ok",
        "http_status": 200,
        "etag": '"abc123"',
        "last_modified": "Fri, 05 Jun 2026 10:00:00 GMT",
        "content_length": 123456,
        "content_type": "application/octet-stream",
        "final_url": "https://example.com/rates.json.gz",
        "checked_at": checked_at,
    }

    observation = discovery._file_probe_observation(
        crawl_target_by_field, head_dict, "run_1"
    )
    update_values = discovery._file_probe_update_values(crawl_target_by_field, head_dict)

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
    lower_confidence_source_dict = {
        "source_id": "vendor",
        "index_url": "https://transparency-in-coverage.uhc.com/",
        "canonical_url": "https://transparency-in-coverage.uhc.com/",
        "source_type": "community_index",
        "seed_provider": "manual",
        "confidence": 95,
    }
    curated_source_dict = {
        "source_id": "curated",
        "index_url": "https://transparency-in-coverage.uhc.com/",
        "canonical_url": "https://transparency-in-coverage.uhc.com/",
        "source_type": "curated_registry",
        "seed_provider": "master-list",
        "confidence": 85,
    }
    other_source_dict = {
        "source_id": "other",
        "index_url": "https://mrfdata.hmhs.com/",
        "canonical_url": "https://mrfdata.hmhs.com/",
        "source_type": "curated_registry",
        "seed_provider": "master-list",
        "confidence": 85,
    }

    crawl_source_rows = discovery._dedupe_source_rows_for_crawl(
        [lower_confidence_source_dict, curated_source_dict, other_source_dict]
    )

    assert [source_row["source_id"] for source_row in crawl_source_rows] == ["curated", "other"]


@pytest.mark.asyncio
async def test_push_crawl_row_batches_chunks_large_model_writes(monkeypatch):
    calls = []

    async def fake_push_objects(rows, model, *, rewrite, use_copy):
        calls.append((model, len(rows), rewrite, use_copy))

    monkeypatch.setattr(discovery, "push_objects", fake_push_objects)
    plan_rows = [{"mrf_plan_id": str(index)} for index in range(5)]
    file_rows = [{"mrf_file_id": str(index)} for index in range(3)]
    observation_rows = [{"observation_id": str(index)} for index in range(1)]

    await discovery._push_crawl_row_batches(
        plan_rows, file_rows, observation_rows, batch_size=2
    )

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
async def test_push_crawl_row_batches_bounds_encoded_message_bytes(monkeypatch):
    batch_sizes = []

    async def fake_push_objects(rows, _model, *, rewrite, use_copy):
        assert rewrite is True
        assert use_copy is False
        batch_sizes.append(len(rows))

    monkeypatch.setattr(discovery, "push_objects", fake_push_objects)
    monkeypatch.setattr(discovery, "WRITE_BATCH_BYTES", 10)
    plan_rows = [
        {"mrf_plan_id": str(index), "payload": "1234"} for index in range(5)
    ]

    await discovery._push_crawl_row_batches(
        plan_rows,
        [],
        [],
        batch_size=5,
    )

    assert plan_rows == []
    assert batch_sizes == [2, 2, 1]


@pytest.mark.asyncio
async def test_push_crawl_row_batches_applies_timeout_per_chunk(monkeypatch):
    observed_timeouts = []

    async def fake_push_objects(_rows, _model, *, rewrite, use_copy):
        assert rewrite is True
        assert use_copy is False

    async def fake_wait_for(awaitable, *, timeout):
        observed_timeouts.append(timeout)
        return await awaitable

    monkeypatch.setattr(discovery, "push_objects", fake_push_objects)
    monkeypatch.setattr(discovery.asyncio, "wait_for", fake_wait_for)

    await discovery._push_crawl_row_batches(
        [{"mrf_plan_id": str(index)} for index in range(5)],
        [],
        [],
        batch_size=2,
        row_write_timeout=45.0,
    )

    assert observed_timeouts == [45.0, 45.0, 45.0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "write_error",
    [
        BufferError("end_message: message is too large"),
        discovery.SQLAlchemyError("connection was closed in the middle of operation"),
    ],
)
async def test_push_crawl_row_batches_splits_retryable_write_failures(
    monkeypatch,
    write_error,
):
    batch_sizes = []

    async def fake_push_objects(rows, _model, *, rewrite, use_copy):
        assert rewrite is True
        assert use_copy is False
        batch_sizes.append(len(rows))
        if len(rows) > 2:
            raise write_error

    monkeypatch.setattr(discovery, "push_objects", fake_push_objects)
    plan_rows = [{"mrf_plan_id": str(index)} for index in range(5)]

    await discovery._push_crawl_row_batches(
        plan_rows,
        [],
        [],
        batch_size=5,
    )

    assert plan_rows == []
    assert batch_sizes == [5, 2, 2, 1]


@pytest.mark.asyncio
async def test_push_crawl_row_batches_preserves_unrelated_write_errors(monkeypatch):
    write_error = discovery.SQLAlchemyError("constraint violation")

    async def fake_push_objects(_rows, _model, *, rewrite, use_copy):
        assert rewrite is True
        assert use_copy is False
        raise write_error

    monkeypatch.setattr(discovery, "push_objects", fake_push_objects)

    with pytest.raises(discovery.SQLAlchemyError, match="constraint violation"):
        await discovery._push_crawl_row_batches(
            [{"mrf_plan_id": "plan_1"}, {"mrf_plan_id": "plan_2"}],
            [],
            [],
            batch_size=2,
        )


@pytest.mark.asyncio
async def test_store_observations_does_not_emit_live_progress_without_control_run(
    monkeypatch,
):
    pushed_list = []
    progress_calls = []

    async def fake_push_objects(rows, model, *, rewrite, use_copy):
        pushed_list.extend(rows)

    monkeypatch.setattr(discovery, "push_objects", fake_push_objects)
    monkeypatch.setattr(
        discovery,
        "enqueue_live_progress",
        lambda **payload: progress_calls.append(payload),
    )

    observations = await discovery._store_observations(
        [{"source_id": "source_1", "index_url": "https://example.com/index.json"}],
        test_mode=True,
        run_id="crawl_run_1",
        progress_run_id=None,
        concurrency=1,
    )

    assert len(observations) in {1}
    assert pushed_list == observations
    assert observations[0]["metadata_json"]["run_id"] == "crawl_run_1"
    assert progress_calls == []


@pytest.mark.asyncio
async def test_private_url_rejected_before_fetch():
    with pytest.raises(ValueError):
        await discovery._assert_fetch_url_allowed(
            "http://169.254.169.254/latest/meta-data"
        )

    with pytest.raises(ValueError):
        await discovery._assert_fetch_url_allowed("http://127.0.0.1:8080/index.json")


@pytest.mark.asyncio
async def test_dry_run_uses_master_list_without_database(monkeypatch):
    monkeypatch.setattr(
        discovery, "_repo_root", lambda: discovery.Path(__file__).resolve().parents[1]
    )

    result = await discovery.main(
        test_mode=True, provider="master-list", limit=3, dry_run=True
    )

    assert result["providers"] == ["master-list"]
    assert result["candidates"] == 3
    assert result["payers"] == 3


@pytest.mark.asyncio
async def test_source_payer_query_filters_before_candidate_limit(monkeypatch):
    observed_limits = []

    async def fake_load_candidates(_provider, *, test_mode, limit):
        observed_limits.append(limit)
        assert test_mode is False
        return [
            discovery.SourceCandidate(
                payer_name="Early Payer",
                provider="master-list",
                index_url="https://example.com/early-index.json",
                status="active",
            ),
            discovery.SourceCandidate(
                payer_name="VSP Vision",
                provider="master-list",
                index_url="https://bcbsm.sapphiremrfhub.com/tocs/current/vsp_vision",
                status="active",
                aliases=("Guardian VSP Network",),
            ),
        ]

    monkeypatch.setattr(discovery, "_load_candidates", fake_load_candidates)

    outcome_by_field = await discovery.main(
        provider="master-list",
        source_payer_query="Guardian VSP",
        limit=1,
        dry_run=True,
    )

    assert observed_limits == [None]
    assert outcome_by_field["candidates"] in {1}
    assert outcome_by_field["payers"] in {1}
    assert outcome_by_field["sources"] in {1}


@pytest.mark.asyncio
async def test_direct_discovery_run_emits_visible_state(monkeypatch):
    """Verify the source-discovery progress and persistence contract."""
    persisted_batches = []
    events = []
    progress = []
    flush_timeouts = []

    async def fake_load_candidates(_provider, *, test_mode, limit):
        assert test_mode is True
        assert limit in {1}
        return [
            discovery.SourceCandidate(
                payer_name="Example Payer",
                provider="master-list",
                index_url="https://example.com/index.json",
                status="active",
            )
        ]

    async def fake_push_objects(row_dicts, model, *, rewrite, use_copy):
        persisted_batches.append((model, row_dicts, rewrite, use_copy))

    async def fake_store_candidates(_candidates, *, discovery_run_id):
        assert discovery_run_id.startswith("mrfcrawl_")
        return (
            [{"payer_id": "payer_1"}],
            [{"source_id": "source_1", "index_url": "https://example.com/index.json"}],
        )

    async def fake_noop(*_args, **_kwargs):
        return None

    async def fake_flush(timeout_seconds):
        flush_timeouts.append(timeout_seconds)

    async def fake_execute_checkpointed_source_batch(**kwargs):
        assert kwargs["root_run_id"].startswith("mrfcrawl_")
        assert kwargs["owner_run_id"] == kwargs["root_run_id"]
        assert kwargs["source_records"] == [
            {
                "source_id": "source_1",
                "index_url": "https://example.com/index.json",
            }
        ]
        return discovery.SourceBatchSummary(
            root_run_id=kwargs["root_run_id"],
            source_set_count=1,
            source_set_sha256="a" * 64,
            completed_source_count=1,
            completed_source_set_sha256="a" * 64,
            failed_source_count=0,
            urls_checked=0,
            plans_discovered=0,
            files_discovered=0,
            bytes_streamed=0,
        )

    monkeypatch.setattr(discovery, "_load_candidates", fake_load_candidates)
    monkeypatch.setattr(discovery, "init_db", fake_noop)
    monkeypatch.setattr(discovery, "ensure_database", fake_noop)
    monkeypatch.setattr(discovery, "_ensure_catalog_tables", fake_noop)
    monkeypatch.setattr(discovery, "push_objects", fake_push_objects)
    monkeypatch.setattr(discovery, "_store_candidates", fake_store_candidates)
    monkeypatch.setattr(
        discovery,
        "execute_checkpointed_source_batch",
        fake_execute_checkpointed_source_batch,
    )
    monkeypatch.setattr(
        discovery, "enqueue_status_event", lambda payload: events.append(payload)
    )
    monkeypatch.setattr(
        discovery, "enqueue_live_progress", lambda **payload: progress.append(payload)
    )
    monkeypatch.setattr(discovery, "flush_status_events", fake_flush)

    discovery_summary = await discovery.main(test_mode=True, provider="master-list", limit=1)

    control_run_id = discovery_summary["crawl_run_id"]
    crawl_rows = [
        row_batch[0]
        for model, row_batch, _rewrite, _use_copy in persisted_batches
        if model is discovery.MRFCrawlRun
    ]
    assert control_run_id.startswith("mrfcrawl_")
    assert [event["status"] for event in events] == ["running", "succeeded"]
    assert all(event["run_id"] == control_run_id for event in events)
    assert events[0]["importer"] == "mrf-source-discovery"
    assert events[0]["triggered_by"] == "direct_cli"
    assert events[0]["params"]["provider"] == "master-list"
    assert events[1]["metrics"]["crawl_run_id"] == control_run_id
    assert events[1]["metrics"]["crawl_status"] == "succeeded"
    assert events[1]["metrics"]["catalog_export_version"] == 1
    assert events[1]["metrics"]["process_workers"] == 1
    assert events[1]["metrics"]["discovery_proof_version"] == 2
    assert events[1]["metrics"]["source_set_count"] == 1
    assert events[1]["metrics"]["completed_source_count"] == 1
    assert events[1]["metrics"]["sources"] in {1}
    assert [crawl_row["run_id"] for crawl_row in crawl_rows] == [control_run_id, control_run_id]
    assert [progress_update["run_id"] for progress_update in progress] == [
        control_run_id,
        control_run_id,
        control_run_id,
    ]
    assert flush_timeouts == [1.0]


@pytest.mark.asyncio
async def test_direct_discovery_run_closes_visible_state_on_failure(monkeypatch):
    """Direct CLI discovery failures must publish terminal visible state."""
    push_calls = []
    control_events = []
    progress_events = []
    flush_timeouts = []

    async def fake_push_objects(row_dicts, orm_model, *, rewrite, use_copy):
        push_calls.append((orm_model, row_dicts, rewrite, use_copy))

    def fake_flush(timeout_seconds):
        flush_timeouts.append(timeout_seconds)

    def record_live_progress(**progress_payload):
        progress_events.append(progress_payload)

    source_candidate = discovery.SourceCandidate(
        payer_name="Example Payer",
        provider="master-list",
        index_url="https://example.com/index.json",
        status="active",
    )
    replacements = [
        ("_load_candidates", AsyncMock(return_value=[source_candidate])),
        ("init_db", AsyncMock()),
        ("ensure_database", AsyncMock()),
        ("_ensure_catalog_tables", AsyncMock()),
        ("push_objects", fake_push_objects),
        ("_store_candidates", AsyncMock(side_effect=RuntimeError("catalog write failed"))),
        ("enqueue_status_event", control_events.append),
        ("enqueue_live_progress", record_live_progress),
        ("flush_status_events", AsyncMock(side_effect=fake_flush)),
    ]
    for attribute_name, replacement_value in replacements:
        monkeypatch.setattr(discovery, attribute_name, replacement_value)

    with pytest.raises(RuntimeError, match="catalog write failed"):
        await discovery.main(test_mode=True, provider="master-list", limit=1)

    crawl_rows = [
        row_dicts[0]
        for pushed_model, row_dicts, rewrite_flag, copy_flag in push_calls
        if pushed_model is discovery.MRFCrawlRun
    ]
    assert all(rewrite_flag is True and copy_flag is False for _, _, rewrite_flag, copy_flag in push_calls)
    assert [event["status"] for event in control_events] == ["running", "failed"]
    assert [crawl_row["status"] for crawl_row in crawl_rows] == ["running", "failed"]
    assert control_events[1]["phase_detail"] == "mrf source discovery failed"
    assert control_events[1]["error"]["message"] == "catalog write failed"
    assert progress_events[-1]["status"] == "failed"
    assert progress_events[-1]["message"] == "catalog write failed"
    assert flush_timeouts == [1.0]
