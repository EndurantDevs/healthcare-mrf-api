"""Small synthetic contracts for complete CMS NPD source acquisition."""

from __future__ import annotations

import fcntl
import json
from collections import Counter
from compression import zstd

import httpx
import pytest

from process.cms_npd_source import (
    RESOURCE_FILES,
    CmsNpdSourceError,
    acquire_release,
    parse_manifest,
)


def _source(rows_by_file=None):
    rows_by_file = rows_by_file or {}
    payloads_by_file = {}
    entries_by_file = {}
    for filename, resource_type in RESOURCE_FILES:
        raw = rows_by_file.get(filename) or (
            json.dumps({"resourceType": resource_type, "id": "synthetic-1"}).encode() + b"\n"
        )
        payloads_by_file[filename] = zstd.compress(raw)
        entries_by_file[filename] = {
            "compressed_bytes": len(payloads_by_file[filename]),
            "original_bytes": len(raw),
        }
    manifest_by_field = {
        "compression_algorithm": "zstd",
        "generated_at": "2026-09-24",
        "files": entries_by_file,
        "totals": {
            "compressed_bytes": sum(entry["compressed_bytes"] for entry in entries_by_file.values()),
            "original_bytes": sum(entry["original_bytes"] for entry in entries_by_file.values()),
        },
    }
    return manifest_by_field, payloads_by_file


def _client(
    manifest_by_field,
    payloads_by_file,
    *,
    change_final_probe=False,
    truncate=None,
    redirect_files=False,
    wrong_range=False,
):
    calls = Counter()

    def respond(request):
        if request.url.path.endswith("manifest.json"):
            return httpx.Response(200, json=manifest_by_field)
        if redirect_files and request.url.host == "example.test":
            return httpx.Response(
                302, headers={"location": f"https://npd-east-prod-bulk-site.s3.amazonaws.com{request.url.path}"}
            )
        filename = request.url.path.rsplit("/", 1)[-1].removesuffix(".zst")
        if filename not in payloads_by_file:
            return httpx.Response(404)
        compressed_bytes = payloads_by_file[filename]
        range_header = request.headers.get("range", "")
        etag = '"synthetic-v1"'
        if range_header == "bytes=0-0":
            calls[(filename, "probe")] += 1
            if change_final_probe and calls[(filename, "probe")] > 1:
                etag = '"synthetic-v2"'
            return httpx.Response(
                206,
                content=compressed_bytes[:1],
                headers={"etag": etag, "content-range": f"bytes 0-0/{len(compressed_bytes)}"},
            )
        calls[(filename, "download")] += 1
        offset = int(range_header.removeprefix("bytes=").removesuffix("-"))
        calls[(filename, f"offset:{offset}")] += 1
        body = compressed_bytes[offset:]
        if filename == truncate:
            body = body[:-1]
        return httpx.Response(
            206,
            content=body,
            headers={
                "etag": etag,
                "content-range": f"bytes {offset + int(wrong_range)}-{len(compressed_bytes) - 1}/{len(compressed_bytes)}",
            },
        )

    return httpx.Client(transport=httpx.MockTransport(respond)), calls


def test_complete_release_is_sealed_and_unchanged_files_are_not_redownloaded(tmp_path):
    manifest_by_field, payloads_by_file = _source()
    client, calls = _client(manifest_by_field, payloads_by_file, redirect_files=True)
    with client:
        release_dir, receipt = acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
        assert len(receipt["files"]) == 8
        assert all(entry["row_count"] == 1 for entry in receipt["files"].values())
        assert (release_dir / "receipt.json").is_file()
        assert acquire_release(tmp_path, client=client, base_url="https://example.test/downloads") == (
            release_dir,
            receipt,
        )
    assert sum(count for (filename, kind), count in calls.items() if kind == "download") == 8


def test_retained_bytes_are_checked_before_reusing_a_receipt(tmp_path):
    manifest_by_field, payloads_by_file = _source()
    client, _ = _client(manifest_by_field, payloads_by_file)
    with client:
        release_dir, _ = acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
        file_path = release_dir / "01-Organization.ndjson.zst"
        with file_path.open("r+b") as output:
            output.write(b"X")
        with pytest.raises(CmsNpdSourceError, match="retained_file_missing"):
            acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")


def test_reused_receipt_date_must_match_pinned_manifest(tmp_path):
    manifest_by_field, payloads_by_file = _source()
    client, _ = _client(manifest_by_field, payloads_by_file)
    with client:
        release_dir, receipt = acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
        (release_dir / "receipt.json").write_text(json.dumps({**receipt, "generated_at": "2025-01-01"}))
        with pytest.raises(CmsNpdSourceError, match="receipt_invalid"):
            acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")


def test_interrupted_download_resumes_the_pinned_bytes(tmp_path):
    manifest_by_field, payloads_by_file = _source()
    filename = "01-Organization.ndjson"
    client, _ = _client(manifest_by_field, payloads_by_file, truncate=filename)
    with client, pytest.raises(CmsNpdSourceError, match="download_incomplete"):
        acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    client, calls = _client(manifest_by_field, payloads_by_file)
    with client:
        _, receipt = acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    assert calls[(filename, f"offset:{len(payloads_by_file[filename]) - 1}")] == 1
    assert receipt["files"][filename]["row_count"] == 1


def test_invalid_unsealed_file_is_replaced_on_retry(tmp_path):
    manifest_by_field, payloads_by_file = _source()
    filename = "01-Organization.ndjson"
    damaged_by_file = {**payloads_by_file, filename: payloads_by_file[filename][:-5] + b"\x00" * 5}
    client, _ = _client(manifest_by_field, damaged_by_file)
    with client, pytest.raises(CmsNpdSourceError):
        acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    client, calls = _client(manifest_by_field, payloads_by_file)
    with client:
        _, receipt = acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    assert calls[(filename, "download")] == 1
    assert receipt["files"][filename]["row_count"] == 1


def test_concurrent_writer_fails_without_changing_the_release(tmp_path):
    manifest_by_field, payloads_by_file = _source()
    client, _ = _client(manifest_by_field, payloads_by_file)
    with client:
        release_dir, _ = acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
        with (release_dir / ".acquire.lock").open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with pytest.raises(CmsNpdSourceError, match="acquisition_in_progress"):
                acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
        assert (release_dir / "receipt.json").exists()


def test_ignored_or_wrong_download_range_never_seals(tmp_path):
    manifest_by_field, payloads_by_file = _source()
    client, _ = _client(manifest_by_field, payloads_by_file, wrong_range=True)
    with client, pytest.raises(CmsNpdSourceError, match="download_range_changed"):
        acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    assert not list(tmp_path.rglob("receipt.json"))


def test_duplicate_fhir_id_requires_equal_content_not_equal_json_format(tmp_path):
    filename = "01-Organization.ndjson"
    rows_by_file = {
        filename: (
            b'{"resourceType":"Organization","id":"synthetic-1","name":"A"}\n'
            b'{"name":"A", "id":"synthetic-1", "resourceType":"Organization"}\n'
        )
    }
    manifest_by_field, payloads_by_file = _source(rows_by_file)
    client, _ = _client(manifest_by_field, payloads_by_file)
    with client:
        _, receipt = acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    assert receipt["files"][filename]["row_count"] == 2
    assert receipt["files"][filename]["distinct_count"] == 1


def test_manifest_requires_exact_eight_files():
    manifest_by_field, _ = _source()
    manifest_by_field["files"].pop("08-OrganizationAffiliation.ndjson")
    with pytest.raises(CmsNpdSourceError, match="manifest_files_invalid"):
        parse_manifest(json.dumps(manifest_by_field).encode())


def test_redirect_to_unreviewed_host_is_rejected_before_request(tmp_path):
    manifest_by_field, _ = _source()
    visited_hosts = []

    def respond(request):
        visited_hosts.append(request.url.host)
        if request.url.path.endswith("manifest.json"):
            return httpx.Response(200, json=manifest_by_field)
        return httpx.Response(302, headers={"location": "http://127.0.0.1/private"})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(CmsNpdSourceError, match="redirect_invalid"):
            acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    assert visited_hosts == ["example.test", "example.test"]


@pytest.mark.parametrize("credential", ["cookie_jar", "cookie_header"])
def test_client_cookies_are_rejected_before_request(tmp_path, credential):
    manifest_by_field, payloads_by_file = _source()
    client, calls = _client(manifest_by_field, payloads_by_file, redirect_files=True)
    with client:
        if credential == "cookie_jar":
            client.cookies.set("session", "synthetic", domain="example.test")
        else:
            client.headers["Cookie"] = "session=synthetic"
        with pytest.raises(CmsNpdSourceError, match="credentials_not_allowed"):
            acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    assert not calls


@pytest.mark.parametrize("header", ("X-API-Key", "User-Agent"))
def test_client_custom_default_headers_are_rejected_before_request(tmp_path, header):
    manifest_by_field, payloads_by_file = _source()
    client, calls = _client(manifest_by_field, payloads_by_file, redirect_files=True)
    with client:
        client.headers[header] = "synthetic-secret"
        with pytest.raises(CmsNpdSourceError, match="credentials_not_allowed"):
            acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    assert not calls


def test_redirect_cannot_forward_a_new_client_default_header(tmp_path):
    manifest_by_field, _ = _source()
    visited_hosts = []

    def respond(request):
        visited_hosts.append(request.url.host)
        if request.url.path.endswith("manifest.json"):
            return httpx.Response(200, json=manifest_by_field)
        client.headers["X-API-Key"] = "synthetic-secret"
        return httpx.Response(
            302,
            headers={"location": f"https://npd-east-prod-bulk-site.s3.amazonaws.com{request.url.path}"},
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(CmsNpdSourceError, match="credentials_not_allowed"):
            acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    assert visited_hosts == ["example.test", "example.test"]


def test_redirect_cookie_cannot_reach_bulk_host(tmp_path):
    manifest_by_field, _ = _source()
    visited_hosts = []

    def respond(request):
        visited_hosts.append(request.url.host)
        if request.url.path.endswith("manifest.json"):
            return httpx.Response(200, json=manifest_by_field)
        return httpx.Response(
            302,
            headers={
                "location": f"https://npd-east-prod-bulk-site.s3.amazonaws.com{request.url.path}",
                "set-cookie": "session=synthetic; Path=/",
            },
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(CmsNpdSourceError, match="credentials_not_allowed"):
            acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    assert visited_hosts == ["example.test", "example.test"]


@pytest.mark.parametrize("failure", ["truncated", "corrupt_zstd", "wrong_type", "conflict", "vector_change"])
def test_incomplete_or_changed_release_never_gets_a_receipt(tmp_path, failure):
    filename = "01-Organization.ndjson"
    rows_by_file = None
    if failure == "wrong_type":
        rows_by_file = {filename: b'{"resourceType":"Practitioner","id":"synthetic-1"}\n'}
    elif failure == "conflict":
        rows_by_file = {
            filename: (
                b'{"resourceType":"Organization","id":"synthetic-1","name":"A"}\n'
                b'{"resourceType":"Organization","id":"synthetic-1","name":"B"}\n'
            )
        }
    manifest_by_field, payloads_by_file = _source(rows_by_file)
    if failure == "corrupt_zstd":
        payloads_by_file[filename] = payloads_by_file[filename][:-5] + b"\x00" * 5
    client, _ = _client(
        manifest_by_field,
        payloads_by_file,
        change_final_probe=failure == "vector_change",
        truncate=filename if failure == "truncated" else None,
    )
    with client, pytest.raises(CmsNpdSourceError):
        acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")
    assert not list(tmp_path.rglob("receipt.json"))
