# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete ordinary direct URL sets never mix selectors or truncate."""

import asyncio
import importlib

import pytest

from process.ptg_parts.canonical import default_ptg2_import_id
from process.ptg_parts.source_jobs import validated_in_network_urls
from tests.test_process_ptg_unit import _install_manifest_case_mocks

ptg = importlib.import_module("process.ptg")


def test_complete_direct_rate_set_and_legacy_scalar_are_distinct():
    urls = [f"https://example.test/rates_{part}_of_2.json.gz" for part in (1, 2)]
    assert validated_in_network_urls({"in_network_urls": urls, "max_files": 2}) == urls
    assert validated_in_network_urls({"in_network_urls": tuple(urls)}) == urls
    assert validated_in_network_urls({"in_network_url": urls[0], "max_files": 1}) is None
    encoded_urls = [
        "https://example.test/rates%20one.json.gz?Policy=signed%20value&Signature=synthetic",
        "http://[2001:db8::1]:8080/rates_two.json.gz",
    ]
    assert validated_in_network_urls({"in_network_urls": encoded_urls}) == encoded_urls
    invalid_params = (
        [{"in_network_urls": selection} for selection in ([], urls[0], [urls[0]], urls + [urls[0]])]
        + [
            {"in_network_urls": urls, parameter_name: parameter_value}
            for parameter_name, parameter_value in (
                ("max_files", 1),
                ("max_files", True),
                ("in_network_url", urls[0]),
                ("allowed_url", urls[0]),
                ("toc_urls", ["https://example.test/index.json"]),
                ("toc_url", urls[0]),
                ("toc_list", "index.txt"),
                ("provider_ref_url", urls[0]),
                ("direct_source_index_url", urls[0]),
                ("direct_rate_file_intent", {}),
                ("file_url_contains", ["rates_1"]),
                ("frozen_rate_files", [{}]),
                ("frozen_rate_file_set_contract", "contract"),
                ("frozen_rate_file_count", 2),
                ("frozen_rate_file_set_sha256", "0" * 64),
                ("direct_rate_file_intent_sha256", "0" * 64),
                ("ordinary_cutover_id", "0" * 64),
            )
        ]
        + [
            {"in_network_urls": [urls[0], malformed_url]}
            for malformed_url in (
                "https://user@example.test/rates.json.gz",
                "https://user&#64;example.test/rates.json.gz",
                "https://example.test/rate&#9;s.json.gz",
                "&#32;https://example.test/rates.json.gz",
                "https://:443/rates.json.gz",
                "https://example.test:wrong/rates.json.gz",
                "https://example.test:65536/rates.json.gz",
                "https://example .test/rates.json.gz",
                "https://example&#32;.test/rates.json.gz",
                "https://example\u00a0.test/rates.json.gz",
                "https://example.test/rate\ts.json.gz",
                "https://example.test/rate\ns.json.gz",
                "https://example.test/rate\x00s.json.gz",
                "https://example.test/rate\x7fs.json.gz",
            )
        ]
    )
    for params_by_name in invalid_params:
        with pytest.raises(ValueError):
            validated_in_network_urls(params_by_name)


def test_complete_rate_set_rejects_native_normalization_aliases():
    urls = [
        "https://www.asrhealthbenefits.com/Home/Umbraco/Surface/MrfDownload/Index?g=1234&t=InNetwork&i=5678",
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?groupNumber=1234&fileType=InNetwork&fileId=5678",
    ]
    with pytest.raises(ValueError, match="duplicate rate URLs"):
        validated_in_network_urls({"in_network_urls": urls})


def test_complete_rate_set_processes_every_selected_url(monkeypatch, tmp_path):
    urls = [f"https://example.test/rates_{part}_of_2.json.gz" for part in (1, 2)]
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(tmp_path / "artifacts"))
    pushed_rows, download_options_by_name = [], {}
    _install_manifest_case_mocks(monkeypatch, pushed_rows, download_options_by_name)
    original_downloads = ptg._iter_downloaded_ptg_jobs
    selected_jobs = []

    async def capture_downloads(jobs, **kwargs):
        selected_jobs.extend(jobs)
        async for job in original_downloads(jobs, **kwargs):
            yield job

    monkeypatch.setattr(ptg, "_iter_downloaded_ptg_jobs", capture_downloads)
    import_report_by_name = asyncio.run(
        ptg.main(
            in_network_urls=urls,
            import_month="2026-10",
            import_id="import_neutral",
            source_key="source_neutral",
            plan_ids=["plan_neutral"],
            plan_market_types=["group"],
            test_mode=True,
        )
    )

    assert [job["url"] for job in selected_jobs] == urls
    assert all(job["type"] == "in_network" for job in selected_jobs)
    assert import_report_by_name["files_processed"] == 2
    assert import_report_by_name["files_failed"] == 0


def test_complete_rate_set_member_failure_prevents_publication(monkeypatch, tmp_path):
    urls = [f"https://example.test/rates_{part}_of_2.json.gz" for part in (1, 2)]
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(tmp_path / "artifacts"))
    pushed_rows, download_options_by_name = [], {}
    _, publish_mock = _install_manifest_case_mocks(
        monkeypatch,
        pushed_rows,
        download_options_by_name,
    )
    original_downloads = ptg._iter_downloaded_ptg_jobs

    async def member_failure(jobs, **kwargs):
        async for downloaded in original_downloads(jobs, **kwargs):
            yield (
                ptg.PTG2DownloadedJob(job=downloaded.job, error="synthetic member download failure")
                if downloaded.job["url"] == urls[1]
                else downloaded
            )

    monkeypatch.setattr(ptg, "_iter_downloaded_ptg_jobs", member_failure)
    with pytest.raises(RuntimeError, match="failed 1 of 2 download"):
        asyncio.run(
            ptg.main(
                in_network_urls=urls,
                import_month="2026-10",
                import_id="import_neutral",
                source_key="source_neutral",
                plan_ids=["plan_neutral"],
                plan_market_types=["group"],
                test_mode=True,
            )
        )

    publish_mock.assert_not_awaited()
    ptg._publish_ptg2_source_pointers.assert_not_awaited()
    assert not [import_row for class_name, import_row in pushed_rows if class_name == "PTG2CurrentSnapshot"]
    import_run_rows = [import_row for class_name, import_row in pushed_rows if class_name == "PTG2ImportRun"]
    assert import_run_rows[-1]["status"] == ptg.PTG2_STATUS_FAILED
    assert import_run_rows[-1]["report"]["files_processed"] == 0
    assert import_run_rows[-1]["report"]["files_failed"] == 1


def test_complete_rate_set_identity_is_distinct_and_legacy_identity_is_unchanged():
    month = ptg.normalize_import_month("2026-10")
    legacy_by_name = {
        "in_network_url": "https://example.test/rates.json.gz",
        "max_files": 1,
        "source_key": "source_neutral",
        "plan_ids": ["plan_neutral"],
        "plan_market_types": ["group"],
    }
    assert (
        ptg._default_ptg2_import_id(
            month,
            "source_neutral",
            in_network_url=legacy_by_name["in_network_url"],
        )
        == "20261001_765cc9ddcf86cb9a"
    )
    assert (
        ptg._ptg2_deterministic_snapshot_id(
            import_month=month,
            import_id="import_neutral",
            option_by_name=legacy_by_name,
        )
        == "ptg2:202610:67d325124467"
    )
    assert ptg._ptg2_snapshot_content_options({**legacy_by_name, "in_network_urls": None}) == (
        ptg._ptg2_snapshot_content_options(legacy_by_name)
    )
    urls = [f"https://example.test/rates_{part}_of_2.json.gz" for part in (1, 2)]
    changed_urls = [urls[0], "https://example.test/other.json.gz"]
    assert default_ptg2_import_id(month, "source_neutral", {"in_network_urls": urls}) != (
        default_ptg2_import_id(month, "source_neutral", {"in_network_urls": changed_urls})
    )
    assert ptg._ptg2_deterministic_snapshot_id(
        import_month=month,
        import_id="import_neutral",
        option_by_name={"in_network_urls": urls},
    ) != ptg._ptg2_deterministic_snapshot_id(
        import_month=month,
        import_id="import_neutral",
        option_by_name={"in_network_urls": changed_urls},
    )
