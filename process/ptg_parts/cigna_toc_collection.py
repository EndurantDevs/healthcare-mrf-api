# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consume every reporter in one source-attested Cigna TOC collection."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import re
import shutil
import struct
import tempfile
import zipfile
import zlib
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from process.ptg_parts.domain import PTG2LogicalArtifact, PTG2RawArtifact

COLLECTION_PROOF_PARAM = "_cigna_toc_collection_proof"
COLLECTION_CONTRACT = "hp_cigna_toc_collection_v1"
COLLECTION_MAX_BYTES = 100 * 1024 * 1024
_LOOKUP_URL = "https://www.cigna.com/static/mrf/latest.json"
_COLLECTION_NAME = re.compile(r"(?P<date>\d{4}-\d{2}-\d{2})_cigna-partners-toc-collection\.zip\Z")
_REPORTERS = {
    "cigna-health-life-insurance-company": "Cigna Health Life Insurance Company",
    "health-partners": "Health Partners",
    "mvp": "Mohawk Valley Practitioners (MVP)",
    "priority-health": "Priority Health",
    "sagamore": "Sagamore",
}
_PROOF_FIELDS = {
    "contract",
    "lookup_url",
    "physical_index_url",
    "selected_file_name",
    "publisher_size",
    "last_updated_on",
    "expected_size",
    "collection",
}
_COLLECTION_FIELDS = {"contract", "archive_sha256", "etag_sha256", "members"}
_MEMBER_FIELDS = {"name", "bytes", "compressed_bytes", "crc32", "sha256"}


def _collection_date(file_name: str) -> str | None:
    match = _COLLECTION_NAME.fullmatch(file_name)
    if match is None:
        return None
    try:
        return date.fromisoformat(match["date"]).isoformat()
    except ValueError:
        return None


def is_cigna_collection_url(url: str) -> bool:
    """Recognize the exact collection route, including transient signed queries."""
    if (
        not isinstance(url, str)
        or "#" in url
        or any(ord(character) <= 32 or ord(character) == 127 for character in url)
    ):
        return False
    try:
        parsed = urlsplit(url)
        selected_date = _collection_date(parsed.path.rsplit("/", 1)[-1])
        return bool(
            selected_date
            and parsed.scheme == "https"
            and parsed.netloc == "d25kgz5rikkq4n.cloudfront.net"
            and not parsed.fragment
            and parsed.path
            == (
                f"/cost_transparency/mrf/table-of-contents-zip/reporting_month={selected_date[:7]}/"
                f"{selected_date}_cigna-partners-toc-collection.zip"
            )
        )
    except ValueError:
        return False


def _has_digest(value: object, length: int = 64) -> bool:
    return isinstance(value, str) and re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is not None


def _validate_members(members: object, selected_date: str, expected_size: int) -> None:
    expected_names = {f"{selected_date}_{reporter}_index.json" for reporter in _REPORTERS}
    if not isinstance(members, list) or len(members) != len(expected_names):
        raise ValueError("Cigna collection member proof is incomplete")
    names, decoded_bytes, compressed_bytes = set(), 0, 0
    for member in members:
        if (
            not isinstance(member, dict)
            or set(member) != _MEMBER_FIELDS
            or not isinstance(member.get("name"), str)
            or member["name"] not in expected_names
            or member["name"] in names
        ):
            raise ValueError("Cigna collection member proof is ambiguous")
        if any(
            type(member[key]) is not int or not 0 < member[key] <= COLLECTION_MAX_BYTES
            for key in ("bytes", "compressed_bytes")
        ):
            raise ValueError("Cigna collection member byte count is invalid")
        if not _has_digest(member["sha256"]) or not _has_digest(member["crc32"], 8):
            raise ValueError("Cigna collection member digest is invalid")
        names.add(member["name"])
        decoded_bytes += member["bytes"]
        compressed_bytes += member["compressed_bytes"]
    if decoded_bytes > COLLECTION_MAX_BYTES or compressed_bytes > expected_size:
        raise ValueError("Cigna collection member proof is oversized")


def validate_collection_proof(value: object) -> dict[str, Any]:
    """Return an independent, closed copy of existing Source locator evidence."""
    if not isinstance(value, dict) or set(value) != _PROOF_FIELDS:
        raise ValueError("Cigna collection locator proof is incomplete")
    physical_url, file_name, collection = value["physical_index_url"], value["selected_file_name"], value["collection"]
    selected_date = _collection_date(file_name) if isinstance(file_name, str) else None
    if (
        value["contract"] != COLLECTION_CONTRACT
        or value["lookup_url"] != _LOOKUP_URL
        or not is_cigna_collection_url(physical_url)
        or urlsplit(physical_url).query
        or urlsplit(physical_url)._replace(query="").geturl() != physical_url
        or not selected_date
        or urlsplit(physical_url).path.rsplit("/", 1)[-1] != file_name
        or value["last_updated_on"] != selected_date
        or not isinstance(value["publisher_size"], str)
        or type(value["expected_size"]) is not int
        or not 0 < value["expected_size"] <= COLLECTION_MAX_BYTES
        or not isinstance(collection, dict)
        or set(collection) != _COLLECTION_FIELDS
        or collection["contract"] != COLLECTION_CONTRACT
    ):
        raise ValueError("Cigna collection locator proof is invalid")
    if not _has_digest(collection["archive_sha256"]) or not _has_digest(collection["etag_sha256"]):
        raise ValueError("Cigna collection archive digest is missing")
    _validate_members(collection["members"], selected_date, value["expected_size"])
    return copy.deepcopy(value)


def _require_directory(content: bytes) -> int:
    position = content.rfind(b"PK\x05\x06", max(0, len(content) - 65557))
    if position < 0 or len(content) - position < 22:
        raise ValueError("Cigna collection ZIP directory is missing")
    _, disk, directory_disk, disk_count, count, size, offset, comment = struct.unpack_from(
        "<4s4H2LH", content, position
    )
    if (
        disk != 0
        or directory_disk != 0
        or disk_count != count
        or count != len(_REPORTERS)
        or offset + size != position
        or position + 22 + comment != len(content)
        or not content.startswith(b"PK\x03\x04")
    ):
        raise ValueError("Cigna collection ZIP directory is unsupported")
    return offset


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    values_by_key = {}
    for key, value in pairs:
        if key in values_by_key:
            raise ValueError("Cigna collection JSON contains duplicate keys")
        values_by_key[key] = value
    return values_by_key


def _reject_json_constant(value: str) -> None:
    raise ValueError("Cigna collection JSON contains a non-finite constant")


def _validate_reporter(content: bytes, name: str, selected_date: str) -> None:
    payload = json.loads(content, object_pairs_hook=_unique_json_object, parse_constant=_reject_json_constant)
    reporter = name[len(selected_date) + 1 : -len("_index.json")]
    if (
        not isinstance(payload, dict)
        or payload.get("reporting_entity_name") != _REPORTERS[reporter]
        or payload.get("last_updated_on") != selected_date
        or not isinstance(payload.get("reporting_structure"), list)
    ):
        raise ValueError("Cigna collection member does not match its reporter and date")


def _read_members(content: bytes, proof: dict[str, Any], max_bytes: int) -> list[tuple[str, bytes, str]]:
    directory_offset = _require_directory(content)
    proof_by_name = {member["name"]: member for member in proof["collection"]["members"]}
    decoded_members, remaining = [], max_bytes
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            members = archive.infolist()
            if (
                archive.start_dir != directory_offset
                or len(members) != len(proof_by_name)
                or {member.filename for member in members} != set(proof_by_name)
                or sum(member.file_size for member in members) > max_bytes
            ):
                raise ValueError("Cigna collection member inventory differs")
            for member in sorted(members, key=lambda item: item.filename):
                member_proof = proof_by_name[member.filename]
                if (
                    member.flag_bits & 1
                    or member.is_dir()
                    or member.extract_version >= 45
                    or member.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
                    or member.header_offset < 0
                    or member.header_offset + member.compress_size >= archive.start_dir
                    or member.file_size != member_proof["bytes"]
                    or member.compress_size != member_proof["compressed_bytes"]
                    or f"{member.CRC:08x}" != member_proof["crc32"]
                ):
                    raise ValueError("Cigna collection member metadata differs")
                with archive.open(member) as stream:
                    decoded = stream.read(remaining + 1)
                if len(decoded) != member.file_size or len(decoded) > remaining:
                    raise ValueError("Cigna collection decoded content is incomplete or too large")
                remaining -= len(decoded)
                digest = hashlib.sha256(decoded).hexdigest()
                if digest != member_proof["sha256"]:
                    raise ValueError("Cigna collection member digest differs")
                _validate_reporter(decoded, member.filename, proof["last_updated_on"])
                decoded_members.append((member.filename, decoded, digest))
    except (OSError, EOFError, RuntimeError, zipfile.BadZipFile, zlib.error) as exc:
        raise ValueError("Cigna collection ZIP or JSON is invalid") from exc
    return decoded_members


def _validate_acquisition(raw_artifact: PTG2RawArtifact, url: str, proof: dict[str, Any], max_bytes: int) -> bytes:
    head = raw_artifact.head
    if (
        raw_artifact.original_url != url
        or raw_artifact.canonical_url != proof["physical_index_url"]
        or raw_artifact.byte_count != proof["expected_size"]
        or raw_artifact.byte_count > max_bytes
        or head is None
        or head.url != url
        or head.status != 200
        or head.content_length != proof["expected_size"]
        or head.content_encoding not in {None, "", "identity"}
        or not isinstance(head.etag, str)
        or re.fullmatch(r'"[\x21\x23-\x7e\x80-\xff]+"', head.etag) is None
        or hashlib.sha256(head.etag.encode()).hexdigest() != proof["collection"]["etag_sha256"]
    ):
        raise ValueError("Cigna collection acquisition differs from Source proof")
    with Path(raw_artifact.raw_path).open("rb") as stream:
        content = stream.read(max_bytes + 1)
    digest = hashlib.sha256(content).hexdigest()
    if (
        len(content) != proof["expected_size"]
        or digest != raw_artifact.raw_sha256
        or digest != proof["collection"]["archive_sha256"]
    ):
        raise ValueError("Cigna collection archive differs from Source proof")
    return content


def _collection_byte_limit(proof: dict[str, Any], max_bytes: int | None) -> int:
    if max_bytes is not None and (type(max_bytes) is not int or max_bytes <= 0):
        raise ValueError("Cigna collection byte limit is invalid")
    byte_limit = min(COLLECTION_MAX_BYTES, max_bytes if max_bytes is not None else COLLECTION_MAX_BYTES)
    if (
        proof["expected_size"] > byte_limit
        or sum(member["bytes"] for member in proof["collection"]["members"]) > byte_limit
    ):
        raise ValueError("Cigna collection exceeds the byte limit")
    return byte_limit


async def materialize_cigna_toc_collection(
    url: str,
    output_dir: str | Path,
    *,
    proof: dict,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    keep_partial_artifacts: bool | None = None,
    artifact_stage_observer=None,
) -> tuple[PTG2RawArtifact, list[PTG2LogicalArtifact]]:
    """Acquire once and return five exact member artifacts after full validation."""
    from process.ptg_parts.domain import PTG2LogicalArtifact
    from process.ptg_parts.source_download import (
        _observe_logical_artifact_stage,
        _observe_raw_artifact_stage,
        download_raw_artifact,
    )

    proof = validate_collection_proof(proof)
    if not is_cigna_collection_url(url) or urlsplit(url)._replace(query="").geturl() != proof["physical_index_url"]:
        raise ValueError("Cigna collection URL differs from Source proof")
    byte_limit = _collection_byte_limit(proof, max_bytes)
    raw_artifact = await download_raw_artifact(
        url,
        reuse_raw_artifacts=reuse_raw_artifacts,
        max_bytes=byte_limit,
        keep_partial_artifacts=keep_partial_artifacts,
        exact_get_evidence=True,
    )
    content = _validate_acquisition(raw_artifact, url, proof, byte_limit)
    members = _read_members(content, proof, byte_limit)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    member_directory = Path(tempfile.mkdtemp(prefix="cigna-toc-", dir=output_root))
    logical_artifacts = []
    try:
        for index, (name, decoded, digest) in enumerate(members):
            logical_path = member_directory / f"member-{index}.json"
            logical_path.write_bytes(decoded)
            logical_artifacts.append(
                PTG2LogicalArtifact(
                    str(logical_path),
                    digest,
                    len(decoded),
                    compression="zip",
                    member_name=name,
                )
            )
        _observe_raw_artifact_stage(artifact_stage_observer, raw_artifact)
        for artifact in logical_artifacts:
            _observe_logical_artifact_stage(artifact_stage_observer, artifact)
    except BaseException:
        shutil.rmtree(member_directory)
        raise
    return raw_artifact, logical_artifacts
