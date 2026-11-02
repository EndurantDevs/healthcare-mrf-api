# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact-byte collection materialization without network or database access."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import struct
import zipfile
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import cigna_toc_collection as collection
from process.ptg_parts import source_download
from process.ptg_parts.domain import PTG2HeadMetadata, PTG2RawArtifact

_DATE = "2026-11-01"
_URL = (
    "https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/table-of-contents-zip/"
    "reporting_month=2026-11/2026-11-01_cigna-partners-toc-collection.zip"
)
_SIGNED_URL = _URL + "?Expires=1&Key-Pair-Id=synthetic&Signature=synthetic"
_ETAG = '"synthetic-collection"'
_REPORTERS = {
    "cigna-health-life-insurance-company": "Cigna Health Life Insurance Company",
    "health-partners": "Health Partners",
    "mvp": "Mohawk Valley Practitioners (MVP)",
    "priority-health": "Priority Health",
    "sagamore": "Sagamore",
}


def _documents():
    return {
        f"{_DATE}_{reporter}_index.json": json.dumps(
            {
                "reporting_entity_name": name,
                "last_updated_on": _DATE,
                "reporting_structure": [],
            },
            indent=2,
        ).encode()
        for reporter, name in _REPORTERS.items()
    }


def _archive(documents, compression=zipfile.ZIP_DEFLATED):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=compression) as archive:
        for name, content in reversed(list(documents.items())):
            archive.writestr(name, content)
    return buffer.getvalue()


def _subject(tmp_path, documents=None, compression=zipfile.ZIP_DEFLATED):
    documents = _documents() if documents is None else documents
    content = _archive(documents, compression)
    digest = hashlib.sha256(content).hexdigest()
    raw_path = tmp_path / "raw.zip"
    raw_path.write_bytes(content)
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        members = [
            {
                "name": member.filename,
                "bytes": member.file_size,
                "compressed_bytes": member.compress_size,
                "crc32": f"{member.CRC:08x}",
                "sha256": hashlib.sha256(documents[member.filename]).hexdigest(),
            }
            for member in archive.infolist()
        ]
    proof_by_field = {
        "contract": collection.COLLECTION_CONTRACT,
        "lookup_url": "https://www.cigna.com/static/mrf/latest.json",
        "physical_index_url": _URL,
        "selected_file_name": _URL.rsplit("/", 1)[-1],
        "publisher_size": "",
        "last_updated_on": _DATE,
        "expected_size": len(content),
        "collection": {
            "contract": collection.COLLECTION_CONTRACT,
            "archive_sha256": digest,
            "etag_sha256": hashlib.sha256(_ETAG.encode()).hexdigest(),
            "members": members,
        },
    }
    raw = PTG2RawArtifact(
        _SIGNED_URL,
        _URL,
        str(raw_path),
        raw_path.as_uri(),
        digest,
        len(content),
        head=PTG2HeadMetadata(_SIGNED_URL, status=200, etag=_ETAG, content_length=len(content)),
    )
    return SimpleNamespace(raw=raw, proof=proof_by_field, content=content, documents=documents)


def _replace_archive(subject, content):
    subject.content = content
    Path(subject.raw.raw_path).write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    subject.raw = replace(
        subject.raw,
        byte_count=len(content),
        raw_sha256=digest,
        head=replace(subject.raw.head, content_length=len(content)),
    )
    subject.proof["expected_size"] = len(content)
    subject.proof["collection"]["archive_sha256"] = digest


def _download(monkeypatch, subject):
    downloader = AsyncMock(return_value=subject.raw)
    monkeypatch.setattr(source_download, "download_raw_artifact", downloader)
    return downloader


@pytest.mark.parametrize("url", [_URL, _SIGNED_URL])
def test_exact_collection_routes(url):
    assert collection.is_cigna_collection_url(url)


@pytest.mark.parametrize(
    "url",
    [
        _URL.replace("https:", "http:"),
        _URL.replace(".net/", ".net:443/"),
        _URL.replace("reporting_month=2026-11", "reporting_month=2026-10"),
        _URL.replace("2026-11-01_", "2026-11-31_"),
        _URL + "#fragment",
        _URL + "#",
        _URL.replace(".net/", ".net.evil.test/"),
        " " + _URL,
        _URL.replace("cigna-partners-toc-collection.zip", "other.json.zip"),
        None,
    ],
)
def test_unrelated_or_changed_routes_are_not_collections(url):
    assert not collection.is_cigna_collection_url(url)


def test_proof_returns_an_independent_copy(tmp_path):
    subject = _subject(tmp_path)
    validated = collection.validate_collection_proof(subject.proof)
    assert validated == subject.proof and validated is not subject.proof
    subject.proof["collection"]["members"][0]["sha256"] = "0" * 64
    assert validated["collection"]["members"][0]["sha256"] != "0" * 64


@pytest.mark.parametrize(
    "change",
    [
        lambda proof: proof.pop("publisher_size"),
        lambda proof: proof.update(extra="unexpected"),
        lambda proof: proof.update(lookup_url="https://example.test/latest.json"),
        lambda proof: proof.update(physical_index_url=_SIGNED_URL),
        lambda proof: proof.update(physical_index_url=_URL + "?"),
        lambda proof: proof.update(last_updated_on="2026-10-01"),
        lambda proof: proof.update(expected_size=True),
        lambda proof: proof.update(publisher_size=None),
        lambda proof: proof["collection"].update(contract="other"),
        lambda proof: proof["collection"].update(archive_sha256="invalid"),
        lambda proof: proof["collection"]["members"].pop(),
        lambda proof: proof["collection"]["members"].__setitem__(0, copy.deepcopy(proof["collection"]["members"][1])),
        lambda proof: proof["collection"]["members"][0].update(crc32="invalid"),
        lambda proof: proof["collection"]["members"][0].update(compressed_bytes=True),
        lambda proof: proof["collection"]["members"][0].update(bytes=collection.COLLECTION_MAX_BYTES),
    ],
)
def test_incomplete_or_ambiguous_proof_is_rejected(tmp_path, change):
    subject = _subject(tmp_path)
    change(subject.proof)
    with pytest.raises(ValueError):
        collection.validate_collection_proof(subject.proof)


@pytest.mark.parametrize("compression", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
@pytest.mark.asyncio
async def test_materialization_preserves_all_five_exact_members(tmp_path, monkeypatch, compression):
    documents = _documents()
    documents[f"{_DATE}_sagamore_index.json"] = b"\xef\xbb\xbf" + documents[f"{_DATE}_sagamore_index.json"]
    subject = _subject(tmp_path, documents, compression=compression)
    downloader = _download(monkeypatch, subject)
    observations = []
    raw, logical = await collection.materialize_cigna_toc_collection(
        _SIGNED_URL,
        tmp_path / "logical",
        proof=subject.proof,
        reuse_raw_artifacts=False,
        keep_partial_artifacts=False,
        artifact_stage_observer=observations.append,
    )
    assert raw is subject.raw
    downloader.assert_awaited_once_with(
        _SIGNED_URL,
        reuse_raw_artifacts=False,
        max_bytes=collection.COLLECTION_MAX_BYTES,
        keep_partial_artifacts=False,
        exact_get_evidence=True,
    )
    assert [artifact.member_name for artifact in logical] == sorted(subject.documents)
    for artifact in logical:
        exact_bytes = subject.documents[artifact.member_name]
        assert Path(artifact.logical_path).read_bytes() == exact_bytes
        assert artifact.logical_sha256 == hashlib.sha256(exact_bytes).hexdigest()
        assert artifact.byte_count == len(exact_bytes)
        assert artifact.compression == "zip" and not artifact.logical_hash_deferred
        assert Path(artifact.logical_path).name.startswith("member-")
    assert [observation.artifact_kind for observation in observations] == ["raw"] + ["logical_json"] * 5
    assert observations[0].identity_sha256 == subject.raw.raw_sha256
    assert [observation.identity_sha256 for observation in observations[1:]] == [
        artifact.logical_sha256 for artifact in logical
    ]


@pytest.mark.parametrize(
    "invalid_content",
    [
        b"[]",
        b"{bad-json}",
        b'{"reporting_entity_name":"Sagamore","last_updated_on":"2026-11-01","reporting_structure":[],"extra":NaN}',
        b'{"reporting_entity_name":"Sagamore","last_updated_on":"2026-11-01","reporting_structure":[],"extra":{"x":1,"x":2}}',
        b'{"reporting_entity_name":"Other","last_updated_on":"2026-11-01","reporting_structure":[]}',
        b'{"reporting_entity_name":"Sagamore","last_updated_on":"2026-10-01","reporting_structure":[]}',
    ],
)
@pytest.mark.asyncio
async def test_invalid_final_reporter_prevents_every_output(tmp_path, monkeypatch, invalid_content):
    documents = _documents()
    documents[f"{_DATE}_sagamore_index.json"] = invalid_content
    subject = _subject(tmp_path, documents)
    _download(monkeypatch, subject)
    observations = []
    with pytest.raises(ValueError):
        await collection.materialize_cigna_toc_collection(
            _SIGNED_URL,
            tmp_path / "logical",
            proof=subject.proof,
            artifact_stage_observer=observations.append,
        )
    assert not (tmp_path / "logical").exists()
    assert observations == []


@pytest.mark.parametrize("mutation", ["appended", "prepended", "split", "count", "directory"])
@pytest.mark.asyncio
async def test_unsupported_archive_directories_are_rejected(tmp_path, monkeypatch, mutation):
    subject = _subject(tmp_path)
    content = bytearray(subject.content)
    directory = content.rfind(b"PK\x05\x06")
    if mutation == "appended":
        content.extend(b"extra")
    elif mutation == "prepended":
        content[:0] = b"extra"
    elif mutation == "split":
        struct.pack_into("<H", content, directory + 4, 1)
    elif mutation == "count":
        struct.pack_into("<H", content, directory + 10, 4)
    else:
        struct.pack_into("<L", content, directory + 16, 0)
    _replace_archive(subject, bytes(content))
    _download(monkeypatch, subject)
    with pytest.raises(ValueError):
        await collection.materialize_cigna_toc_collection(_SIGNED_URL, tmp_path / "logical", proof=subject.proof)
    assert not (tmp_path / "logical").exists()


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "extra", "unsupported", "zip64", "encrypted", "corrupt"])
@pytest.mark.asyncio
async def test_unsupported_member_inventory_or_storage_is_rejected(tmp_path, monkeypatch, mutation):
    subject = _subject(tmp_path)
    documents = list(subject.documents.items())
    if mutation == "missing":
        documents.pop()
    elif mutation == "duplicate":
        documents[-1] = documents[0]
    elif mutation == "extra":
        documents.append(("unexpected.json", b"{}"))
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in documents:
            member = zipfile.ZipInfo(name)
            member.compress_type = zipfile.ZIP_BZIP2 if mutation == "unsupported" else zipfile.ZIP_DEFLATED
            if mutation == "zip64":
                member.extract_version = 45
            archive.writestr(member, content)
    content = bytearray(buffer.getvalue())
    if mutation == "encrypted":
        struct.pack_into("<H", content, 6, 1)
        struct.pack_into("<H", content, content.find(b"PK\x01\x02") + 8, 1)
    elif mutation == "corrupt":
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            first_member = archive.infolist()[0]
        data_offset = 30 + len(first_member.filename)
        content[data_offset : data_offset + first_member.compress_size] = b"\x00" * first_member.compress_size
    _replace_archive(subject, bytes(content))
    _download(monkeypatch, subject)
    with pytest.raises(ValueError):
        await collection.materialize_cigna_toc_collection(_SIGNED_URL, tmp_path / "logical", proof=subject.proof)
    assert not (tmp_path / "logical").exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("archive_sha256", "0" * 64),
        ("etag_sha256", "0" * 64),
        ("sha256", "0" * 64),
        ("crc32", "00000000"),
        ("bytes", 1),
        ("compressed_bytes", 1),
    ],
)
@pytest.mark.asyncio
async def test_expected_archive_and_member_identity_are_enforced(tmp_path, monkeypatch, field, value):
    subject = _subject(tmp_path)
    target = (
        subject.proof["collection"]
        if field in {"archive_sha256", "etag_sha256"}
        else subject.proof["collection"]["members"][0]
    )
    target[field] = value
    downloader = _download(monkeypatch, subject)
    with pytest.raises(ValueError):
        await collection.materialize_cigna_toc_collection(_SIGNED_URL, tmp_path / "logical", proof=subject.proof)
    downloader.assert_awaited_once()
    assert not (tmp_path / "logical").exists()


@pytest.mark.parametrize(
    "head_changes",
    [
        {"etag": 'W/"weak"'},
        {"etag": None},
        {"status": 206},
        {"content_encoding": "gzip"},
        {"content_length": 1},
        {"url": "https://example.test/redirect.zip"},
    ],
)
@pytest.mark.asyncio
async def test_changed_acquisition_metadata_is_rejected(tmp_path, monkeypatch, head_changes):
    subject = _subject(tmp_path)
    subject.raw = replace(subject.raw, head=replace(subject.raw.head, **head_changes))
    _download(monkeypatch, subject)
    with pytest.raises(ValueError):
        await collection.materialize_cigna_toc_collection(_SIGNED_URL, tmp_path / "logical", proof=subject.proof)
    assert not (tmp_path / "logical").exists()


@pytest.mark.parametrize("max_bytes", [0, -1, True, 1])
@pytest.mark.asyncio
async def test_invalid_or_insufficient_limits_do_not_download(tmp_path, monkeypatch, max_bytes):
    subject = _subject(tmp_path)
    downloader = _download(monkeypatch, subject)
    with pytest.raises(ValueError):
        await collection.materialize_cigna_toc_collection(
            _SIGNED_URL,
            tmp_path / "logical",
            proof=subject.proof,
            max_bytes=max_bytes,
        )
    downloader.assert_not_awaited()


@pytest.mark.asyncio
async def test_partial_output_is_removed_without_touching_existing_files(tmp_path, monkeypatch):
    subject = _subject(tmp_path)
    _download(monkeypatch, subject)
    output_dir = tmp_path / "logical"
    output_dir.mkdir()
    existing = output_dir / "existing.json"
    existing.write_text("preserve")
    original_write = Path.write_bytes

    def fail_second_member(path, content):
        if path.name == "member-1.json":
            raise OSError("synthetic write failure")
        return original_write(path, content)

    monkeypatch.setattr(Path, "write_bytes", fail_second_member)
    with pytest.raises(OSError, match="synthetic"):
        await collection.materialize_cigna_toc_collection(_SIGNED_URL, output_dir, proof=subject.proof)
    assert list(output_dir.iterdir()) == [existing]
    assert existing.read_text() == "preserve"


@pytest.mark.asyncio
async def test_observer_failure_removes_all_member_outputs(tmp_path, monkeypatch):
    subject = _subject(tmp_path)
    _download(monkeypatch, subject)

    def fail_logical_observation(observation):
        if observation.artifact_kind == "logical_json":
            raise RuntimeError("synthetic observer failure")

    output_dir = tmp_path / "logical"
    with pytest.raises(RuntimeError, match="synthetic observer"):
        await collection.materialize_cigna_toc_collection(
            _SIGNED_URL,
            output_dir,
            proof=subject.proof,
            artifact_stage_observer=fail_logical_observation,
        )
    assert list(output_dir.iterdir()) == []
