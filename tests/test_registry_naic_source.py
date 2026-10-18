# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure custody, complete-edition ordering and caller refusal checks."""

import hashlib
import io
import json
import os
import struct
import zipfile
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_naic_source as source
from process import registry_source_admission_receipt as receipts
from process import registry_source_import as caller
from process.registry_naic_observations import validate_naic_manifest
from process.registry_source_admission import RegistrySourceAdmissionError
from process.registry_source_fetch import RegistrySourceFetchReceipt, RegistrySourceFetchResult, RegistrySourceFetchSpec
from process.registry_source_observation_store import RegistryObservationLanding
from process.registry_source_preparation import prepare_naic_registry_source_input
from tests.registry_naic_fixtures import listing_row, prepared_listings, write_manifest
from tests.registry_source_fixtures import _arguments


def test_manifest_keeps_raw_member_identity_and_location_independent_digest(tmp_path):
    prepared = prepared_listings(tmp_path)
    manifest = write_manifest(tmp_path / "members.json", prepared)
    assert source.read_naic_input_manifest(manifest, prepared.edition) == prepared
    assert (
        validate_naic_manifest(source._canonical(prepared.manifest()).decode(), prepared.edition.input_sha256)
        == prepared.manifest()
    )
    relocated = tmp_path / "other"
    relocated.mkdir()
    assert prepared_listings(relocated).edition.input_sha256 == prepared.edition.input_sha256
    assert prepared.edition.published_at is None


def test_preparation_retains_exact_non_utf8_members_and_layout_hashes(tmp_path):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    rows_by_member = {name: [listing_row(ADDRESS=b"Synthetic \xd3 address")] for name in source.MEMBERS}
    original = prepared_listings(inputs, rows_by_member=rows_by_member)
    archive_path = tmp_path / "source.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        for member in original.members:
            archive.writestr(member.member_name, member.input_path.read_bytes())
    archive_bytes = archive_path.read_bytes()
    digest = hashlib.sha256(archive_bytes).hexdigest()
    artifact_path = tmp_path / (digest + ".artifact")
    archive_path.rename(artifact_path)
    spec = RegistrySourceFetchSpec(
        "naic",
        "company-listings",
        "synthetic-2026",
        "https://content.naic.org/synthetic.zip",
        source.PARSER_VERSION,
        2026,
        None,
        1_048_576,
    )
    receipt = RegistrySourceFetchReceipt(spec, digest, len(archive_bytes), datetime.now(timezone.utc))
    fetched = RegistrySourceFetchResult(receipt=receipt, artifact_path=artifact_path, unchanged=False)
    layout_paths_by_member = {}
    for name in source.MEMBERS:
        path = tmp_path / (name + ".pdf")
        path.write_bytes(b"%PDF synthetic layout " + name.encode())
        layout_paths_by_member[name] = path
    prepared = prepare_naic_registry_source_input(
        fetched, tmp_path / "retained", layout_paths_by_member, snapshot_id=original.edition.snapshot_id
    )
    for actual, expected in zip(prepared.members, original.members):
        assert actual.input_path.read_bytes() == expected.input_path.read_bytes()
        assert actual.input_sha256 == expected.input_sha256
        assert (
            actual.layout_pdf_sha256
            == hashlib.sha256(layout_paths_by_member[actual.member_name].read_bytes()).hexdigest()
        )
    assert prepared.edition.retrieved_at == receipt.retrieved_at and prepared.edition.published_at is None
    assert not list((tmp_path / "retained").glob(".registry-input-*"))


@pytest.mark.parametrize("fault", ["type", "bytes", "canonical", "member", "digest"])
def test_member_witness_refuses_noncanonical_or_mismatched_custody(tmp_path, fault):
    prepared = prepared_listings(tmp_path)
    member_rows = prepared.manifest()
    encoded = source._canonical(member_rows).decode()
    if fault == "type":
        encoded = None
    elif fault == "bytes":
        encoded = "\u00e9" * 4097
    elif fault == "canonical":
        encoded = json.dumps(member_rows)
    elif fault == "member":
        member_rows[0]["member_bytes"] = 0
        encoded = source._canonical(member_rows).decode()
    digest = "f" * 64 if fault == "digest" else prepared.edition.input_sha256
    with pytest.raises(RegistrySourceAdmissionError):
        validate_naic_manifest(encoded, digest)


@pytest.mark.parametrize("change", ["missing", "reorder", "duplicate", "digest", "layout", "unknown"])
def test_manifest_refuses_partial_ambiguous_or_changed_interpretation(tmp_path, change):
    prepared = prepared_listings(tmp_path)
    path = write_manifest(tmp_path / "members.json", prepared)
    document = json.loads(path.read_text())
    if change == "missing":
        document["members"].pop()
    if change == "reorder":
        document["members"].reverse()
    if change == "duplicate":
        document["members"][2] = document["members"][0]
    if change == "digest":
        document["members"][0]["input_sha256"] = "f" * 64
    if change == "layout":
        document["members"][0]["layout_pdf_sha256"] = "f" * 64
    if change == "unknown":
        document["members"][0]["unknown"] = True
    path.write_text(json.dumps(document))
    with pytest.raises(RegistrySourceAdmissionError):
        source.read_naic_input_manifest(path, prepared.edition)


def test_combined_bound_does_not_raise_member_or_cms_caps(tmp_path):
    prepared = prepared_listings(tmp_path)
    args = (
        "pg_temp_1",
        "landing",
        1,
        prepared.edition.snapshot_id,
        "naic",
        "company-listings",
        prepared.edition.edition_id,
        prepared.edition.input_sha256,
        source.PARSER_VERSION,
    )
    RegistryObservationLanding(*args, 15_000, source._canonical(prepared.manifest()).decode())
    with pytest.raises(ValueError):
        RegistryObservationLanding(*args, 15_001, source._canonical(prepared.manifest()).decode())
    with pytest.raises(ValueError):
        RegistryObservationLanding(
            "pg_temp_1",
            "landing",
            1,
            prepared.edition.snapshot_id,
            "cms",
            "commercial-mlr",
            "2026",
            "a" * 64,
            "cms-header-v1",
            5_001,
        )
    with pytest.raises(RegistrySourceAdmissionError):
        replace(prepared.members[0], input_bytes=source.MAX_MEMBER_BYTES + 1)


def _native_frame(metadata):
    fields = (
        bytes.fromhex(metadata["snapshot_id"].replace("-", "")),
        b"row",
        struct.pack("!i", 2),
        b"unresolved",
        b"\x01{}",
        b"\x01[]",
    )
    return (
        source._COPY_HEADER
        + struct.pack("!h", 6)
        + b"".join(struct.pack("!i", len(value)) + value for value in fields)
        + b"\xff\xff"
    )


def _native_encoder(member_bytes, encoded_metadata):
    metadata = json.loads(encoded_metadata)
    assert hashlib.sha256(member_bytes).hexdigest() == metadata["member_sha256"]
    counts_by_status = dict(
        input_rows=1,
        accepted_rows=0,
        unresolved_rows=1,
        rejected_rows=0,
        distinct_naic_company_codes=1,
        distinct_naic_group_codes=1,
    )
    return (
        _native_frame(metadata),
        1,
        source._canonical({"edition": metadata, "counts": counts_by_status, "conflicts": []}),
    )


@pytest.mark.parametrize("failure", ["framing", "rows", "metadata", "counts"])
def test_native_contract_refusal_precedes_copy(tmp_path, monkeypatch, failure):
    prepared = prepared_listings(tmp_path)

    def encode(data, metadata):
        frame, rows, encoded = _native_encoder(data, metadata)
        admission_by_field = json.loads(encoded)
        if failure == "framing":
            frame = b"invalid"
        if failure == "rows":
            rows = 5_001
        if failure == "metadata":
            admission_by_field["edition"]["layout_pdf_sha256"] = "c" * 64
        if failure == "counts":
            admission_by_field["counts"]["input_rows"] = 2
        return frame, rows, source._canonical(admission_by_field)

    monkeypatch.setattr(
        source.importlib,
        "import_module",
        lambda _name: SimpleNamespace(encode_naic_company_registry_observations=encode),
    )
    with pytest.raises(RegistrySourceAdmissionError):
        source._encode_member(prepared.members[0], prepared.edition)


class _Connection:
    def __init__(self, events):
        self.events = events
        self.rollback = False

    def is_in_transaction(self):
        return True

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        except BaseException:
            self.rollback = True
            raise

    async def execute(self, *_args):
        return None

    async def fetchval(self, *_args):
        return "public"

    async def fetchrow(self, *_args):
        return {"oid": 1, "nspname": "pg_temp_1"}

    async def copy_to_table(self, _table, *, source, **_kwargs):
        source.read()
        self.events.append("copy")
        return "COPY 1"


def _recording_encoder(events, fail_last):
    def encode(member, edition):
        events.append(member.member_name)
        if fail_last and member.member_name == "GPNM.csv":
            raise RegistrySourceAdmissionError("member rejected")
        frame, rows, metadata = _native_encoder(
            member.input_path.read_bytes(),
            source._canonical({"snapshot_id": str(edition.snapshot_id), "member_sha256": member.input_sha256}),
        )
        return frame, rows, json.loads(metadata)["counts"]

    return encode


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_last", [False, True])
async def test_all_members_close_before_identity_allocation_and_failure_rolls_back(tmp_path, monkeypatch, fail_last):
    from process import registry_identity_materialization as identities
    from process import registry_source_admission as admission
    from process import registry_source_observation_store as observations

    prepared = prepared_listings(tmp_path)
    events = []
    connection = _Connection(events)

    async def register(*_args):
        events.append("register")

    async def materialize(*_args, **_kwargs):
        events.append("materialize")
        return {"source_rows": 3, "unresolved_gpnm_companies": 1}

    async def persist(*_args, **_kwargs):
        events.append("persist")
        return dict.fromkeys(caller._COUNTERS, 0) | {"observations": 3, "unresolved": 3, "replayed": False}

    sealed = AsyncMock()
    monkeypatch.setattr(source, "_encode_member", _recording_encoder(events, fail_last))
    monkeypatch.setattr(admission, "_register_edition", register)
    monkeypatch.setattr(identities, "materialize_registry_source_identities", materialize)
    monkeypatch.setattr(observations, "persist_registry_source_observations", persist)
    monkeypatch.setattr(receipts, "seal_registry_source_admission", sealed)
    if fail_last:
        with pytest.raises(RegistrySourceAdmissionError):
            await source.admit_naic_edition(connection, prepared, control_schema="synthetic")
        assert connection.rollback and "register" not in events and "materialize" not in events
        sealed.assert_not_called()
    else:
        admission_by_field = await source.admit_naic_edition(connection, prepared, control_schema="synthetic")
        assert events == [
            "HLTH.csv",
            "copy",
            "GPAL.csv",
            "copy",
            "GPNM.csv",
            "copy",
            "register",
            "materialize",
            "persist",
        ]
        assert admission_by_field["copy_batches"] == 3 and admission_by_field["source_members"] == prepared.manifest()
        assert (
            json.loads(caller._receipt_json(prepared.edition, admission_by_field))["identity_materialization"][
                "unresolved_gpnm_companies"
            ]
            == 1
        )


def test_operator_caller_passes_complete_manifest_without_reencoding_members(tmp_path, monkeypatch, capsys):
    prepared = prepared_listings(tmp_path)
    path = write_manifest(tmp_path / "members.json", prepared)

    async def run(value, edition, role):
        assert value == prepared and edition == prepared.edition and role is None
        return '{"status":"ok"}'

    monkeypatch.setattr(caller, "_run_import", run)
    assert caller.run_command(_arguments(path, prepared.edition)) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "ok"}


@pytest.mark.parametrize("input_path", [None, 17, {}, []])
def test_operator_rejects_non_string_member_paths_with_fixed_receipt(tmp_path, monkeypatch, capsys, input_path):
    prepared = prepared_listings(tmp_path)
    path = write_manifest(tmp_path / "members.json", prepared)
    document = json.loads(path.read_text())
    document["members"][0]["input_path"] = input_path
    path.write_text(json.dumps(document))

    async def unexpected_import(*_args):
        pytest.fail("Invalid member path reached the database import")

    monkeypatch.setattr(caller, "_run_import", unexpected_import)
    assert caller.run_command(_arguments(path, prepared.edition)) == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err) == {"status": "error", "code": "invalid_arguments"}


def _fetched_listing_archive(root, *, incomplete=False):
    inputs = root / "inputs"
    inputs.mkdir()
    prepared = prepared_listings(inputs)
    artifact = root / "source.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        for member in prepared.members[:2] if incomplete else prepared.members:
            archive.writestr(member.member_name, member.input_path.read_bytes())
    encoded = artifact.read_bytes()
    digest = hashlib.sha256(encoded).hexdigest()
    artifact = artifact.rename(root / (digest + ".artifact"))
    spec = RegistrySourceFetchSpec(
        "naic",
        "company-listings",
        "synthetic-2026",
        "https://content.naic.org/synthetic.zip",
        source.PARSER_VERSION,
        2026,
        None,
        1_048_576,
    )
    fetched = RegistrySourceFetchResult(
        RegistrySourceFetchReceipt(spec, digest, len(encoded), datetime.now(timezone.utc)), artifact, False
    )
    layouts_by_member = {}
    for name in source.MEMBERS:
        layout = root / (name + ".pdf")
        layout.write_bytes(b"%PDF synthetic layout")
        layouts_by_member[name] = layout
    return prepared, fetched, layouts_by_member


@pytest.mark.parametrize("fault", ["fetch", "snapshot", "layouts", "year", "parser", "symlink", "missing_member"])
def test_preparation_refuses_untrusted_or_incomplete_input(tmp_path, monkeypatch, fault):
    prepared, fetched, layouts_by_member = _fetched_listing_archive(tmp_path, incomplete=fault == "missing_member")
    snapshot = prepared.edition.snapshot_id
    root = tmp_path / "retained"
    if fault == "fetch":
        fetched = None
    if fault == "snapshot":
        snapshot = UUID(int=0)
    if fault == "layouts":
        layouts_by_member.pop("GPNM.csv")
    if fault in {"year", "parser"}:
        spec = replace(
            fetched.receipt.spec,
            **({"reporting_year": 2025} if fault == "year" else {"parser_version": "other-parser"}),
        )
        fetched = replace(fetched, receipt=replace(fetched.receipt, spec=spec))
    if fault == "symlink":
        target = tmp_path / "target"
        target.mkdir()
        root.symlink_to(target, target_is_directory=True)
    with pytest.raises(RegistrySourceAdmissionError):
        source.prepare_naic_source_input(fetched, root, layouts_by_member, snapshot_id=snapshot)
    assert not root.exists() or not list(root.iterdir())


@pytest.mark.parametrize("fault", ["layout", "archive"])
def test_preparation_detects_byte_changes_and_cleans_staging(tmp_path, monkeypatch, fault):
    from process import registry_source_preparation as preparation

    prepared, fetched, layouts_by_member = _fetched_listing_archive(tmp_path)
    target = layouts_by_member["HLTH.csv"] if fault == "layout" else fetched.artifact_path
    digest = preparation._digest
    reads_by_target = {"target": 0}

    def change_after_verification(stream, maximum):
        try:
            is_target = os.fstat(stream.fileno()).st_ino == target.stat().st_ino
        except OSError, AttributeError:
            is_target = False
        if is_target:
            reads_by_target["target"] += 1
            if reads_by_target["target"] == (3 if fault == "layout" else 2):
                with target.open("ab") as writer:
                    writer.write(b"changed")
        return digest(stream, maximum)

    monkeypatch.setattr(preparation, "_digest", change_after_verification)
    root = tmp_path / "retained"
    with pytest.raises(RegistrySourceAdmissionError, match="identity changed"):
        source.prepare_naic_source_input(fetched, root, layouts_by_member, snapshot_id=prepared.edition.snapshot_id)
    assert not list(root.iterdir())


@pytest.mark.parametrize("encoded", [b'{"members":[],"members":[]}', b" " * 65537, b"[]", b'{"members":{}}'])
def test_manifest_refuses_duplicate_fields_oversize_and_wrong_shape(tmp_path, encoded):
    prepared = prepared_listings(tmp_path)
    path = tmp_path / "invalid.json"
    path.write_bytes(encoded)
    with pytest.raises(RegistrySourceAdmissionError):
        source.read_naic_input_manifest(path, prepared.edition)


@pytest.mark.parametrize("missing", ["module", "encoder"])
def test_native_unavailability_never_reaches_member_bytes(tmp_path, monkeypatch, missing):
    prepared = prepared_listings(tmp_path)

    def unavailable(_name):
        if missing == "module":
            raise ImportError("Synthetic missing native module")
        return SimpleNamespace()

    monkeypatch.setattr(source.importlib, "import_module", unavailable)
    with pytest.raises(RegistrySourceAdmissionError, match="encoder is unavailable"):
        source._encode_member(prepared.members[0], prepared.edition)


@pytest.mark.parametrize("member_index", [0, 1, 2])
def test_member_frame_has_complete_accounting_contract(tmp_path, monkeypatch, member_index):
    prepared = prepared_listings(tmp_path)
    monkeypatch.setattr(
        source.importlib,
        "import_module",
        lambda _name: SimpleNamespace(encode_naic_company_registry_observations=_native_encoder),
    )
    frame, rows, counts_by_status = source._encode_member(prepared.members[member_index], prepared.edition)
    assert frame.startswith(source._COPY_HEADER) and frame.endswith(b"\xff\xff")
    assert rows == counts_by_status["input_rows"] == 1
    assert sum(counts_by_status[key] for key in ("accepted_rows", "unresolved_rows", "rejected_rows")) == rows


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["status", "partial"])
async def test_copy_refusal_precedes_registration_and_rolls_back(tmp_path, monkeypatch, fault):
    from process import registry_source_admission as admission

    prepared = prepared_listings(tmp_path)
    events = []
    connection = _Connection(events)

    async def refused_copy(_table, *, source, **_options):
        source.read() if fault == "status" else source.read(5)
        return "COPY 0" if fault == "status" else "COPY 1"

    connection.copy_to_table = refused_copy
    register = AsyncMock()
    monkeypatch.setattr(admission, "_register_edition", register)
    monkeypatch.setattr(source, "_encode_member", _recording_encoder(events, False))
    with pytest.raises(RegistrySourceAdmissionError, match="COPY accounting"):
        await source.admit_naic_edition(connection, prepared, control_schema="synthetic")
    assert connection.rollback
    register.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["input", "transaction"])
async def test_admission_refuses_missing_complete_input_or_caller_transaction(tmp_path, fault):
    prepared = prepared_listings(tmp_path)
    connection = _Connection([])
    if fault == "input":
        prepared = None
    else:
        connection.is_in_transaction = lambda: False
    with pytest.raises(RegistrySourceAdmissionError, match="caller transaction"):
        await source.admit_naic_edition(connection, prepared, control_schema="synthetic")
    assert not connection.events


def _listing_with_archive_layouts(root, *, omit=None):
    prepared, original, layouts_by_member = _fetched_listing_archive(root)
    buffer = io.BytesIO()
    with zipfile.ZipFile(original.artifact_path) as inputs, zipfile.ZipFile(buffer, "w") as output:
        for name in inputs.namelist():
            if name != omit:
                output.writestr(name, inputs.read(name))
        for name, path in layouts_by_member.items():
            pdf = name.removesuffix(".csv") + ".pdf"
            if pdf != omit:
                output.writestr(pdf, path.read_bytes())
    encoded = buffer.getvalue()
    digest = hashlib.sha256(encoded).hexdigest()
    path = root / (digest + ".artifact")
    path.write_bytes(encoded)
    receipt = replace(original.receipt, artifact_sha256=digest, artifact_bytes=len(encoded))
    return prepared, replace(original, receipt=receipt, artifact_path=path), layouts_by_member


def test_archive_preparation_binds_same_source_csv_and_pdf_bytes(tmp_path):
    original, fetched, layouts_by_member = _listing_with_archive_layouts(tmp_path)
    root = tmp_path / "retained"
    prepared = source.prepare_naic_source_archive(fetched, root, snapshot_id=original.edition.snapshot_id)
    assert type(prepared) is source.RegistryNaicPreparedInput
    assert prepared.edition.retrieved_at == fetched.receipt.retrieved_at
    assert prepared.edition.published_at is None
    assert tuple(member.member_name for member in prepared.members) == source.MEMBERS
    for member in prepared.members:
        assert (
            member.layout_pdf_sha256 == hashlib.sha256(layouts_by_member[member.member_name].read_bytes()).hexdigest()
        )
        assert (root / (member.layout_pdf_sha256 + ".pdf")).is_file()
        assert member.input_sha256 == hashlib.sha256(member.input_path.read_bytes()).hexdigest()
    assert source.prepare_naic_source_archive(fetched, root, snapshot_id=original.edition.snapshot_id) == prepared
    assert not list(root.glob(".registry-input-*"))


@pytest.mark.parametrize("omit", ["HLTH.csv", "GPAL.csv", "GPNM.csv", "HLTH.pdf", "GPAL.pdf", "GPNM.pdf"])
def test_archive_preparation_refuses_incomplete_closed_six_member_input(tmp_path, omit):
    original, fetched, _ = _listing_with_archive_layouts(tmp_path, omit=omit)
    root = tmp_path / "retained"
    with pytest.raises(ValueError):
        source.prepare_naic_source_archive(fetched, root, snapshot_id=original.edition.snapshot_id)
    assert not list(root.glob(".registry-input-*"))


def test_archive_preparation_rehashes_existing_layout_before_replay(tmp_path):
    original, fetched, _ = _listing_with_archive_layouts(tmp_path)
    root = tmp_path / "retained"
    prepared = source.prepare_naic_source_archive(fetched, root, snapshot_id=original.edition.snapshot_id)
    path = root / (prepared.members[0].layout_pdf_sha256 + ".pdf")
    path.write_bytes(b"changed layout")
    with pytest.raises(ValueError):
        source.prepare_naic_source_archive(fetched, root, snapshot_id=original.edition.snapshot_id)
    assert not list(root.glob(".registry-input-*"))
