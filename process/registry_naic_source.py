# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit three byte-preserving company-listing members as one closed edition."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import io
import json
import re
import zipfile
from contextlib import ExitStack
from dataclasses import dataclass, replace
from pathlib import Path
from uuid import UUID, uuid4

from process.network_membership_copy import _COPY_HEADER
from process.registry_source_admission import RegistrySourceAdmissionError, RegistrySourceEdition
from process.uhc_flex_practitioner_async_safety import drain_operation

PARSER_VERSION = "naic-company-listings-byte-projection-v1"
MAX_MEMBER_BYTES = 8 * 1024 * 1024
MAX_MEMBER_ROWS = 5_000
MAX_EDITION_ROWS = 3 * MAX_MEMBER_ROWS
MAX_COPY_BYTES = 16 * 1024 * 1024
MEMBERS = ("HLTH.csv", "GPAL.csv", "GPNM.csv")
HEADERS_BY_MEMBER = {
    "HLTH.csv": (
        "COMPANY NAME",
        "STATE OF DOMICILE",
        "GROUP CODE",
        "COMPANY CODE",
        "FEIN NUMBER",
        "RJIN FLAG",
        "CRIN ",
        "RJIN",
        "COMPANY STATUS",
        "ADDRESS",
        "CITY",
        "STATE",
        "ZIP ",
        "PHONE",
    ),
    "GPAL.csv": ("GROUP NAME", "GROUP CODE"),
    "GPNM.csv": (
        "GROUP NAME",
        "GROUP CODE",
        "COMPANY CODE",
        "FEIN NUMBER",
        "COMPANY STATUS",
        "STATE OF DOMICILE",
        "COMPANY NAME",
    ),
}
LAYOUTS_BY_MEMBER = dict(zip(MEMBERS, ("naic-cl-hlth-2026-v1", "naic-cl-gpal-2026-v1", "naic-cl-gpnm-2026-v1")))


def _canonical(field_value):
    return json.dumps(field_value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def _is_digest(field_value):
    return type(field_value) is str and re.fullmatch(r"[0-9a-f]{64}", field_value) is not None


def is_naic_source(source):
    """Recognize the single supported listing parser/source pairing."""
    return (source.source_system, source.source_id, source.parser_version) == (
        "naic",
        "company-listings",
        PARSER_VERSION,
    )


@dataclass(frozen=True)
class RegistryNaicMemberInput:
    member_name: str
    input_path: Path
    input_sha256: str
    input_bytes: int
    header_sha256: str
    layout_pdf_sha256: str

    def __post_init__(self):
        if (
            self.member_name not in MEMBERS
            or not isinstance(self.input_path, Path)
            or type(self.input_bytes) is not int
            or not 0 < self.input_bytes <= MAX_MEMBER_BYTES
            or not all(
                _is_digest(field_value)
                for field_value in (self.input_sha256, self.header_sha256, self.layout_pdf_sha256)
            )
            or self.header_sha256
            != hashlib.sha256(
                json.dumps(HEADERS_BY_MEMBER[self.member_name], separators=(",", ":")).encode()
            ).hexdigest()
        ):
            raise RegistrySourceAdmissionError("Company listing member metadata is invalid")

    def evidence(self):
        """Return the path-free witness used by the combined edition digest."""
        return {
            "member_name": self.member_name,
            "member_sha256": self.input_sha256,
            "member_bytes": self.input_bytes,
            "header_sha256": self.header_sha256,
            "layout": LAYOUTS_BY_MEMBER[self.member_name],
            "layout_pdf_sha256": self.layout_pdf_sha256,
        }


@dataclass(frozen=True)
class RegistryNaicPreparedInput:
    edition: RegistrySourceEdition
    members: tuple[RegistryNaicMemberInput, ...]

    def __post_init__(self):
        if (
            type(self.edition) is not RegistrySourceEdition
            or not is_naic_source(self.edition)
            or type(self.members) is not tuple
            or len(self.members) != 3
            or any(type(member) is not RegistryNaicMemberInput for member in self.members)
            or tuple(member.member_name for member in self.members) != MEMBERS
            or hashlib.sha256(_canonical(self.manifest())).hexdigest() != self.edition.input_sha256
        ):
            raise RegistrySourceAdmissionError("A complete company listing edition is required")
        self.edition.__post_init__()
        for member in self.members:
            member.__post_init__()

    def manifest(self):
        """Return ordered immutable witnesses for all three required members."""
        return [member.evidence() for member in self.members]

    def header_sha256(self):
        """Bind each exact native header in fixed member order."""
        return hashlib.sha256(_canonical([member.header_sha256 for member in self.members])).hexdigest()


def _layout_digest(path):
    from process.registry_source_preparation import _digest as file_digest
    from process.registry_source_preparation import _verified_file

    layout_path = Path(path).absolute()
    with layout_path.open("rb") as stream:
        digest, size = file_digest(stream, MAX_MEMBER_BYTES)
    with _verified_file(layout_path, digest, size, MAX_MEMBER_BYTES) as stream:
        if file_digest(stream, MAX_MEMBER_BYTES) != (digest, size):
            raise RegistrySourceAdmissionError("Company listing layout identity changed")
    return digest


def _stage_naic_members(archive, cleanup, root, layout_paths):
    from process.registry_source_preparation import _members, _stage_member

    members_by_name = _members(archive)
    if not set(MEMBERS) <= set(members_by_name):
        raise RegistrySourceAdmissionError("Company listing archive members are incomplete")
    staged_members = []
    for member_name in MEMBERS:
        layout_sha256 = _layout_digest(layout_paths[member_name])
        stage, digest, size = cleanup.enter_context(
            _stage_member(archive, members_by_name[member_name], root, MAX_MEMBER_BYTES)
        )
        header_digest = hashlib.sha256(
            json.dumps(HEADERS_BY_MEMBER[member_name], separators=(",", ":")).encode()
        ).hexdigest()
        staged_members.append((stage, digest, size, member_name, header_digest, layout_sha256))
    return staged_members


def _retain_naic_members(fetched, receipt, root, layout_paths):
    from process.registry_source_preparation import _bounded_directory, _retain, _verified_file
    from process.registry_source_preparation import _digest as file_digest

    members = []
    with ExitStack() as cleanup:
        with _verified_file(
            fetched.artifact_path, receipt.artifact_sha256, receipt.artifact_bytes, receipt.spec.max_bytes
        ) as stream:
            _bounded_directory(stream)
            with zipfile.ZipFile(stream) as archive:
                staged_members = _stage_naic_members(archive, cleanup, root, layout_paths)
            stream.seek(0)
            if file_digest(stream, receipt.spec.max_bytes) != (receipt.artifact_sha256, receipt.artifact_bytes):
                raise RegistrySourceAdmissionError("Company listing archive identity changed")
        for stage, digest, size, name, header, layout in staged_members:
            members.append(
                RegistryNaicMemberInput(
                    name, _retain(stage, root, digest, size, "company-listings"), digest, size, header, layout
                )
            )
    return members


def _naic_fetch_receipt(fetched, snapshot_id):
    """Validate the closed source and real fetch metadata before retaining bytes."""
    from process.registry_source_fetch import RegistrySourceFetchReceipt, RegistrySourceFetchResult

    if (
        type(fetched) is not RegistrySourceFetchResult
        or type(fetched.receipt) is not RegistrySourceFetchReceipt
        or type(snapshot_id) is not UUID
        or not snapshot_id.int
    ):
        raise RegistrySourceAdmissionError("Company listing preparation metadata is invalid")
    receipt = replace(fetched.receipt, spec=replace(fetched.receipt.spec))
    spec = receipt.spec
    if (spec.source_system, spec.source_id, spec.parser_version, spec.reporting_year) != (
        "naic",
        "company-listings",
        PARSER_VERSION,
        2026,
    ):
        raise RegistrySourceAdmissionError("Company listing preparation identity differs")
    return receipt


def prepare_naic_source_input(fetched, input_dir, layout_paths, *, snapshot_id):
    """Retain exact members and bind caller-supplied layout bytes, without admission."""
    if set(layout_paths) != set(MEMBERS):
        raise RegistrySourceAdmissionError("Company listing preparation metadata is invalid")
    receipt = _naic_fetch_receipt(fetched, snapshot_id)
    spec = receipt.spec
    root = Path(input_dir).absolute()
    root.mkdir(parents=True, exist_ok=True)
    if root.resolve(strict=True) != root:
        raise RegistrySourceAdmissionError("Company listing input directory is invalid")
    members = _retain_naic_members(fetched, receipt, root, layout_paths)
    member_witnesses = [member.evidence() for member in members]
    edition = RegistrySourceEdition(
        snapshot_id,
        spec.source_system,
        spec.source_id,
        spec.edition_id,
        spec.source_url,
        receipt.artifact_sha256,
        hashlib.sha256(_canonical(member_witnesses)).hexdigest(),
        spec.parser_version,
        spec.reporting_year,
        spec.published_at,
        receipt.retrieved_at,
    )
    return RegistryNaicPreparedInput(edition, tuple(members))


def _retain_naic_layout(stage, root, digest, size):
    from process.registry_source_preparation import _verified_file

    path = root / (digest + ".pdf")
    try:
        path.hardlink_to(stage)
    except FileExistsError:
        with _verified_file(path, digest, size, size):
            return path
    return path


def _archive_layout_paths(archive, cleanup, root):
    from process.registry_source_preparation import _members, _stage_member

    members_by_name = _members(archive)
    required = set(MEMBERS) | {name.removesuffix(".csv") + ".pdf" for name in MEMBERS}
    if not required <= members_by_name.keys():
        raise RegistrySourceAdmissionError("Company listing archive members are incomplete")
    paths_by_member = {}
    for name in MEMBERS:
        member = members_by_name[name.removesuffix(".csv") + ".pdf"]
        stage, digest, size = cleanup.enter_context(_stage_member(archive, member, root, MAX_MEMBER_BYTES))
        paths_by_member[name] = _retain_naic_layout(stage, root, digest, size)
    return paths_by_member


def prepare_naic_source_archive(fetched, input_dir, *, snapshot_id):
    """Bind the three fixed layout PDFs from the same verified listing archive."""
    from process.registry_source_preparation import _bounded_directory, _verified_file

    receipt = _naic_fetch_receipt(fetched, snapshot_id)
    root = Path(input_dir).absolute()
    root.mkdir(parents=True, exist_ok=True)
    if root.resolve(strict=True) != root:
        raise RegistrySourceAdmissionError("Company listing input directory is invalid")
    with ExitStack() as cleanup:
        with _verified_file(
            fetched.artifact_path, receipt.artifact_sha256, receipt.artifact_bytes, receipt.spec.max_bytes
        ) as stream:
            _bounded_directory(stream)
            with zipfile.ZipFile(stream) as archive:
                layout_paths = _archive_layout_paths(archive, cleanup, root)
    return prepare_naic_source_input(fetched, root, layout_paths, snapshot_id=snapshot_id)


def read_naic_input_manifest(path, edition):
    """Read a bounded operator manifest; paths never become stable edition identity."""

    def unique(pairs):
        """Refuse duplicate JSON fields before constructing typed member inputs."""
        admission_by_field = {}
        for key, field_value in pairs:
            if key in admission_by_field:
                raise RegistrySourceAdmissionError("Company listing manifest contains duplicate fields")
            admission_by_field[key] = field_value
        return admission_by_field

    with Path(path).open("rb") as stream:
        encoded = stream.read(65_537)
    if len(encoded) > 65_536:
        raise RegistrySourceAdmissionError("Company listing manifest exceeds its bound")
    document = json.loads(encoded, object_pairs_hook=unique)
    if type(document) is not dict or set(document) != {"members"} or type(document["members"]) is not list:
        raise RegistrySourceAdmissionError("Company listing manifest is invalid")
    members = []
    for member_by_field in document["members"]:
        if type(member_by_field) is not dict or set(member_by_field) != {
            "member_name",
            "input_path",
            "input_sha256",
            "input_bytes",
            "header_sha256",
            "layout_pdf_sha256",
        }:
            raise RegistrySourceAdmissionError("Company listing manifest member is invalid")
        if type(member_by_field["input_path"]) is not str:
            raise RegistrySourceAdmissionError("Company listing manifest member is invalid")
        members.append(
            RegistryNaicMemberInput(**{**member_by_field, "input_path": Path(member_by_field["input_path"])})
        )
    return RegistryNaicPreparedInput(edition, tuple(members))


def _encode_member(member, edition):
    from process.registry_source_preparation import _verified_file

    metadata = {
        "snapshot_id": str(edition.snapshot_id),
        "edition_id": edition.edition_id,
        "reporting_year": edition.reporting_year,
        "artifact_sha256": edition.artifact_sha256,
        "member": member.member_name,
        "member_sha256": member.input_sha256,
        "layout": LAYOUTS_BY_MEMBER[member.member_name],
        "layout_pdf_sha256": member.layout_pdf_sha256,
        "header_sha256": member.header_sha256,
        "parser_version": PARSER_VERSION,
    }
    try:
        encoder = importlib.import_module("ptg2_address_canon").encode_naic_company_registry_observations
    except ImportError, AttributeError:
        raise RegistrySourceAdmissionError("Native company listing encoder is unavailable") from None
    with _verified_file(member.input_path, member.input_sha256, member.input_bytes, MAX_MEMBER_BYTES) as stream:
        frame, row_count, encoded_metadata = encoder(stream.read(MAX_MEMBER_BYTES + 1), _canonical(metadata))
    if (
        type(frame) is not bytes
        or not 21 <= len(frame) <= MAX_COPY_BYTES
        or not frame.startswith(_COPY_HEADER)
        or not frame.endswith(b"\xff\xff")
        or type(row_count) is not int
        or not 1 <= row_count <= MAX_MEMBER_ROWS
        or type(encoded_metadata) is not bytes
        or len(encoded_metadata) > MAX_COPY_BYTES
    ):
        raise RegistrySourceAdmissionError("Native company listing framing is invalid")
    admission_by_field = json.loads(encoded_metadata)
    if (
        type(admission_by_field) is not dict
        or set(admission_by_field) != {"edition", "counts", "conflicts"}
        or admission_by_field["edition"] != metadata
    ):
        raise RegistrySourceAdmissionError("Native company listing edition differs")
    counts_by_status = admission_by_field["counts"]
    expected_keys = {
        "input_rows",
        "accepted_rows",
        "unresolved_rows",
        "rejected_rows",
        "distinct_naic_company_codes",
        "distinct_naic_group_codes",
    }
    if (
        type(counts_by_status) is not dict
        or set(counts_by_status) != expected_keys
        or any(type(field_value) is not int or field_value < 0 for field_value in counts_by_status.values())
        or counts_by_status["input_rows"] != row_count
        or sum(counts_by_status[key] for key in ("accepted_rows", "unresolved_rows", "rejected_rows")) != row_count
        or type(admission_by_field["conflicts"]) is not list
    ):
        raise RegistrySourceAdmissionError("Native company listing accounting is invalid")
    return frame, row_count, counts_by_status


async def _copy_naic_landing(connection, prepared, table):
    from process.registry_source_admission import _COPY_COLUMNS
    from process.registry_source_observation_store import RegistryObservationLanding

    edition = prepared.edition
    totals = dict.fromkeys(("input_rows", "accepted_rows", "unresolved_rows", "rejected_rows"), 0)
    copy_digest, copy_size = hashlib.sha256(), 0
    for member in prepared.members:
        frame, row_count, counts_by_status = await drain_operation(
            asyncio.to_thread(_encode_member, member, edition), preserve_cancellation=True
        )
        with io.BytesIO(frame) as stream:
            status = await connection.copy_to_table(table, columns=_COPY_COLUMNS, format="binary", source=stream)
            if status != f"COPY {row_count}" or stream.tell() != len(frame):
                raise RegistrySourceAdmissionError("Company listing COPY accounting is incomplete")
        for key in totals:
            totals[key] += counts_by_status[key]
        copy_digest.update(frame)
        copy_size += len(frame)
    relation = await connection.fetchrow(
        "SELECT c.oid,n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relnamespace=pg_my_temp_schema() AND c.relname=$1",
        table,
    )
    landing = RegistryObservationLanding(
        relation["nspname"],
        table,
        relation["oid"],
        edition.snapshot_id,
        edition.source_system,
        edition.source_id,
        edition.edition_id,
        edition.input_sha256,
        edition.parser_version,
        totals["input_rows"],
        _canonical(prepared.manifest()).decode(),
    )
    return landing, totals, copy_digest.hexdigest(), copy_size


async def admit_naic_edition(connection, prepared, *, control_schema=None):
    """Close all three bounded COPY frames before set resolution or allocation."""
    from db.registry_schema import registry_schema
    from process.network_address_projection import _identifier
    from process.registry_identity_materialization import materialize_registry_source_identities
    from process.registry_source_admission import _register_edition
    from process.registry_source_admission_receipt import seal_registry_source_admission
    from process.registry_source_observation_store import persist_registry_source_observations

    if type(prepared) is not RegistryNaicPreparedInput or not connection.is_in_transaction():
        raise RegistrySourceAdmissionError("Company listing admission requires a complete input and caller transaction")
    prepared = RegistryNaicPreparedInput(prepared.edition, prepared.members)
    schema, edition = control_schema or registry_schema(), prepared.edition
    namespace = _identifier(schema)
    async with connection.transaction():
        original = await connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
        await connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        table = "registry_landing_" + uuid4().hex
        await connection.execute(
            f'CREATE TEMP TABLE "{table}" (LIKE {namespace}.registry_source_observation INCLUDING ALL) ON COMMIT DROP'
        )
        landing, totals, copy_sha256, copy_size = await _copy_naic_landing(connection, prepared, table)
        await _register_edition(connection, namespace, edition)
        identities = await materialize_registry_source_identities(connection, landing, control_schema=schema)
        receipt = await persist_registry_source_observations(connection, landing, control_schema=schema)
        admission_by_field = {
            **receipt,
            "identity_materialization": identities,
            "native_counts": totals,
            "copy_sha256": copy_sha256,
            "copy_bytes": copy_size,
            "copy_batches": 3,
            "source_members": prepared.manifest(),
        }
        await seal_registry_source_admission(
            connection, namespace, schema, edition, prepared.header_sha256(), admission_by_field
        )
        await connection.execute(f'DROP TABLE "{table}"')
        await connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original)
    return admission_by_field
