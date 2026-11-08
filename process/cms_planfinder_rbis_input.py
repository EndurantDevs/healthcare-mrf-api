# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Decode the inspected RBIS plan rows without allocating network identities.

Only the six selected fields use ASCII. Other columns remain opaque bytes in
retained source evidence. A caller must exhaust this iterator before accepting
an edition: a later row can refuse a previously yielded candidate batch.
"""

import hashlib
import json
import re
import struct
import tempfile
import zipfile
import zlib
from contextlib import ExitStack
from datetime import date
from pathlib import Path, PurePosixPath

LAYOUT = "rbis-2026-08-06-plan-v1"
RBIS_MEMBER = "2026-08-06-RBIS.zip"
PLAN_ARCHIVE_MEMBER = "RBIS.INSURANCE_PLAN_20260806143015.zip"
PLAN_MEMBER = "RBIS.INSURANCE_PLAN_20260806143015.dat"
HEADERS = (
    "HIOS Plan ID",
    "Plan Marketing Name",
    "HIOS Product ID",
    "Market Coverage",
    "Dental Only Plan",
    "Network ID",
    "Service Area ID",
    "Formulary ID",
    "New\\Existing Plan",
    "Plan Type",
    "Level of Coverage",
    "Unique Plan Design",
    "QHP\\Non QHP",
    "Notice Required for Pregnancy",
    "Is a Referral Required for Specialist",
    "Specialist s  Requiring a Referral",
    "Plan Level Exclusions",
    "Limited Cost Sharing Plan Variation   Est Advanced Payment",
    "Child Only Offering",
    "Child Only Plan ID",
    "Wellness Program Offered",
    "Disease Management Programs Offered",
    "EHB Apportionment for Pediatric Dental",
    "Guaranteed vs  Estimated Rate",
    "Plan Effective Date",
    "Plan Expiration Date",
    "Out of Country Coverage",
    "Out of Country Coverage Description",
    "Out of Service Area Coverage",
    "Out of Service Area Coverage Description",
    "National Network",
    "Enrollment Payment URL",
    "Life Cycle Status Name",
    "EHB Percent of Total Premium",
    "Does this plan offer Composite Rating",
    "Design Type",
    "Plan Published Indicator",
)
SELECTED_HEADERS = (
    "HIOS Plan ID",
    "Plan Marketing Name",
    "HIOS Product ID",
    "Network ID",
    "Plan Effective Date",
    "Plan Expiration Date",
)
MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
MAX_RBIS_BYTES = 512 * 1024 * 1024
MAX_PLAN_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_PLAN_BYTES = 64 * 1024 * 1024
MAX_LINE_BYTES = 32 * 1024
MAX_ROWS = 100_000
MAX_BATCH_BYTES = 8 * 1024 * 1024
MAX_ZIP_MEMBERS = 256
# Each directory entry has three unsigned 16-bit variable-length fields.
MAX_ZIP_DIRECTORY_BYTES = MAX_ZIP_MEMBERS * (46 + 3 * 65535)
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_PLAN_ID = re.compile(r"[0-9]{5}[A-Z]{2}[0-9]{7}\Z")
_NETWORK_ID = re.compile(r"[A-Z]{2}N[0-9]{3}\Z")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")


class PlanFinderPlanInputError(ValueError):
    """The selected source cannot supply a complete validated plan edition."""


def _require(condition, message):
    if not condition:
        raise PlanFinderPlanInputError(message)


def _retained_stream(source, maximum, stack):
    """Hash a bounded immutable local copy and register its close before copying."""
    retained = stack.enter_context(tempfile.TemporaryFile())
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(64 * 1024):
        size += len(chunk)
        _require(size <= maximum, "Plan input exceeds its byte limit")
        digest.update(chunk)
        retained.write(chunk)
    retained.seek(0)
    return retained, digest.hexdigest()


def _check_zip_directory(stream):
    """Bound the exact directory ZipFile will load, including ZIP64 and prefixes."""
    # The pinned stdlib parser reads only the bounded footer and fixed ZIP64 records.
    try:
        end = zipfile._EndRecData(stream)
    except OSError as error:
        raise zipfile.BadZipFile("Malformed ZIP directory footer") from error
    if end is None:
        raise zipfile.BadZipFile("Missing ZIP directory footer")
    size = end[zipfile._ECD_SIZE]
    expected_count = end[zipfile._ECD_ENTRIES_TOTAL]
    _require(size <= MAX_ZIP_DIRECTORY_BYTES, "Plan ZIP directory exceeds its byte limit")
    _require(expected_count <= MAX_ZIP_MEMBERS, "Plan ZIP membership is invalid")
    start = end[zipfile._ECD_LOCATION] - size
    _require(0 <= end[zipfile._ECD_OFFSET] <= start, "Plan ZIP directory bounds are invalid")
    stream.seek(start)
    remaining = size
    count = 0
    while remaining:
        header = stream.read(min(zipfile.sizeCentralDir, remaining))
        if len(header) != zipfile.sizeCentralDir or header[:4] != zipfile.stringCentralDir:
            raise zipfile.BadZipFile("Malformed ZIP directory entry")
        fields = struct.unpack(zipfile.structCentralDir, header)
        entry_size = zipfile.sizeCentralDir + sum(fields[12:15])
        _require(entry_size <= remaining, "Plan ZIP directory bounds are invalid")
        count += 1
        _require(count <= MAX_ZIP_MEMBERS, "Plan ZIP membership is invalid")
        stream.seek(entry_size - zipfile.sizeCentralDir, 1)
        remaining -= entry_size
    _require(count == expected_count == end[zipfile._ECD_ENTRIES_THIS_DISK], "Plan ZIP membership is invalid")


def _archive(stream, maximum, stack):
    """Reject unsafe or ambiguous ZIP members without extracting pathnames."""
    _check_zip_directory(stream)
    archive = stack.enter_context(zipfile.ZipFile(stream))
    members = archive.infolist()
    names = [member.filename for member in members]
    _require(len(members) <= MAX_ZIP_MEMBERS and len(set(names)) == len(names), "Plan ZIP membership is invalid")
    _require(sum(member.file_size for member in members) <= maximum, "Plan ZIP expanded size exceeds its limit")
    for member in members:
        path = PurePosixPath(member.filename)
        _require(
            not member.flag_bits & 1
            and not path.is_absolute()
            and ".." not in path.parts
            and "\\" not in member.filename,
            "Plan ZIP member is unsafe",
        )
    return archive


def _member(archive, name, maximum, stack):
    _require(name in archive.namelist(), "Required plan member is missing")
    member = archive.getinfo(name)
    _require(not member.is_dir() and member.file_size <= maximum, "Plan member exceeds its byte limit")
    with archive.open(member) as source:
        return _retained_stream(source, maximum, stack)


def _plan_input(artifact_path, expected_digest, stack):
    """Bind every container level and raw member before yielding candidate rows."""
    with Path(artifact_path).open("rb") as source:
        artifact, artifact_digest = _retained_stream(source, MAX_ARTIFACT_BYTES, stack)
    _require(artifact_digest == expected_digest, "Artifact SHA256 does not match its pin")
    outer = _archive(artifact, MAX_ARTIFACT_BYTES, stack)
    rbis, rbis_digest = _member(outer, RBIS_MEMBER, MAX_RBIS_BYTES, stack)
    rbis_archive = _archive(rbis, MAX_RBIS_BYTES, stack)
    plans, plans_digest = _member(rbis_archive, PLAN_ARCHIVE_MEMBER, MAX_PLAN_ARCHIVE_BYTES, stack)
    plan_archive = _archive(plans, MAX_PLAN_BYTES, stack)
    raw, raw_digest = _member(plan_archive, PLAN_MEMBER, MAX_PLAN_BYTES, stack)
    metadata = {
        "component": "cms_planfinder_rbis_input",
        "revision": 1,
        "layout": LAYOUT,
        "artifact_sha256": artifact_digest,
        "rbis_member": RBIS_MEMBER,
        "rbis_sha256": rbis_digest,
        "plan_archive_member": PLAN_ARCHIVE_MEMBER,
        "plan_archive_sha256": plans_digest,
        "member": PLAN_MEMBER,
        "member_sha256": raw_digest,
        "headers": SELECTED_HEADERS,
    }
    return raw, metadata


def _record(stream):
    """Read one bounded physical pipe record, preserving selected lexical bytes."""
    line = stream.readline(MAX_LINE_BYTES + 1)
    _require(len(line) <= MAX_LINE_BYTES, "Plan record exceeds its byte limit")
    if not line:
        return None
    fields = line.removesuffix(b"\n").removesuffix(b"\r").split(b"|")
    _require(len(fields) == len(HEADERS), "Plan record width does not match the layout")
    return fields


def _values(fields):
    """Validate exact source IDs and dates; labels convey no ownership authority."""
    selected_fields = [fields[HEADERS.index(header)] for header in SELECTED_HEADERS]
    _require(
        all(value and all(32 <= byte <= 126 for byte in value) for value in selected_fields),
        "Selected plan fields must be nonempty printable ASCII",
    )
    values = [value.decode("ascii") for value in selected_fields]
    plan, label, product, network, effective, expiration = values
    _require(_PLAN_ID.fullmatch(plan) and not plan.startswith("00000"), "HIOS plan identifier is invalid")
    _require(product == plan[:10], "HIOS product identifier does not match the plan")
    _require(_NETWORK_ID.fullmatch(network), "Official network identifier is invalid")
    _require(label.strip() and len(label) <= 512, "Plan marketing name is invalid")
    _require(_DATE.fullmatch(effective) and _DATE.fullmatch(expiration), "Plan date format is invalid")
    try:
        start, end = date.fromisoformat(effective), date.fromisoformat(expiration)
    except ValueError as error:
        raise PlanFinderPlanInputError("Plan date is invalid") from error
    _require(start <= end, "Plan dates are reversed")
    return values


def _rows(stream):
    header = _record(stream)
    _require(header == [value.encode("ascii") for value in HEADERS], "Plan header does not match the inspected layout")
    fingerprints_by_scope = {}
    source_row = 1
    while (fields := _record(stream)) is not None:
        source_row += 1
        _require(source_row <= MAX_ROWS + 1, "Plan row limit exceeded")
        values = _values(fields)
        plan, label, product, network, effective, expiration = values
        key = (plan, network, effective, expiration)
        fingerprint = hashlib.sha256(json.dumps([label, product], separators=(",", ":")).encode("ascii")).digest()
        _require(
            key not in fingerprints_by_scope or fingerprints_by_scope[key] == fingerprint,
            "Conflicting duplicate plan presentation",
        )
        fingerprints_by_scope[key] = fingerprint
        yield {"source_row": source_row, "values": values}


def _batches(stream, metadata, batch_size):
    prefix = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode("ascii")[:-1] + b',"rows":['
    records = []
    size = len(prefix) + 2
    for row in _rows(stream):
        encoded = json.dumps(row, sort_keys=True, separators=(",", ":")).encode("ascii")
        _require(len(prefix) + len(encoded) + 2 <= MAX_BATCH_BYTES, "One plan row exceeds the batch limit")
        if records and (len(records) >= batch_size or size + len(encoded) + 1 > MAX_BATCH_BYTES):
            yield prefix + b",".join(records) + b"]}"
            records = []
            size = len(prefix) + 2
        size += len(encoded) + bool(records)
        records.append(encoded)
    if records:
        yield prefix + b",".join(records) + b"]}"


def iter_cms_planfinder_plan_batches(artifact_path, *, expected_artifact_sha256, batch_size=5000):
    """Yield bounded exact-source JSON batches; exhaust before accepting an edition.

    Identical duplicate rows retain their original positions. A conflicting
    exact plan/network/date tuple refuses the edition, including after earlier
    candidate batches. Source dates remain exact; no plan year or IDs are derived.
    """
    _require(type(batch_size) is int and 1 <= batch_size <= 5000, "Batch size must be between 1 and 5000")
    _require(
        type(expected_artifact_sha256) is str and _DIGEST.fullmatch(expected_artifact_sha256),
        "Canonical artifact SHA256 pin is required",
    )
    try:
        with ExitStack() as stack:
            stream, metadata = _plan_input(artifact_path, expected_artifact_sha256, stack)
            yield from _batches(stream, metadata, batch_size)
    except (zipfile.BadZipFile, KeyError, RuntimeError, EOFError, UnicodeDecodeError, zlib.error) as error:
        raise PlanFinderPlanInputError("Plan container is malformed") from error
