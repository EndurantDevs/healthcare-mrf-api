# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS Doctors source-grain and exact artifact preservation contracts."""

import hashlib
import zipfile
from unittest.mock import AsyncMock

import pytest

from process import cms_doctors_artifact as artifact
from process import cms_doctors_groups as groups

NPI = "1000000004"
MANIFEST = {
    "source_key": "cms-doctors",
    "dataset_id": "mj5m-pzi6",
    "content_sha256": "a" * 64,
    "generation_id": "b" * 64,
    "downloaded_at": "2026-09-01T00:00:00",
}


def _row(**overrides):
    source_row_by_name = {
        "NPI": NPI,
        "Ind_enrl_ID": "E-1",
        "Org_PAC_ID": "0012345678",
        "adrs_id": "site-full-id-001",
        "Facility Name": "Example Medical Group",
        "num_org_mem": "12",
        "adr_ln_1": "10 Main Street",
        "City/Town": "Springfield",
        "State": "IL",
        "ZIP Code": "62704",
        "pri_spec": "Internal Medicine",
    }
    source_row_by_name.update(overrides)
    return source_row_by_name


def test_group_site_observations_preserve_group_grain_and_unknown_dates():
    first = groups.doctor_group_site_row(_row(), 1, MANIFEST)
    second = groups.doctor_group_site_row(
        _row(**{"Org_PAC_ID": "0098765432", "Facility Name": "Second Group"}),
        2,
        MANIFEST,
    )
    unlinked = groups.doctor_group_site_row(
        _row(**{"Org_PAC_ID": "", "Facility Name": "", "num_org_mem": ""}),
        3,
        MANIFEST,
    )
    assert first["address_checksum"] == second["address_checksum"]
    assert (first["row_number"], second["row_number"]) == (1, 2)
    assert first["org_pac_id"] == "0012345678"
    assert second["org_pac_id"] == "0098765432"
    assert unlinked["org_pac_id"] is None and unlinked["num_org_mem"] is None
    assert first["adrs_id"] == "site-full-id-001"
    assert first["source_json"]["raw_fields"]["adrs_id"] == "site-full-id-001"
    assert first["membership_start_at"] is first["membership_end_at"] is None


@pytest.mark.parametrize("count", ["twelve", "12.5", "-1"])
def test_group_site_rejects_non_numeric_member_counts(count):
    with pytest.raises(ValueError, match="cms_group_site_invalid_member_count"):
        groups.doctor_group_site_row(_row(num_org_mem=count), 4, MANIFEST)


async def test_group_site_stages_every_physical_row_before_address_dedup(tmp_path, monkeypatch):
    source = tmp_path / "doctors.csv"
    source.write_text(
        "NPI,Ind_enrl_ID,Org_PAC_ID,adrs_id,Facility Name,num_org_mem,adr_ln_1,City/Town,State,ZIP Code,pri_spec\n"
        f"{NPI},E-1,0012345678,site-1,First Group,12,10 Main Street,Springfield,IL,62704,Internal Medicine\n"
        f"{NPI},E-1,0098765432,site-1,Second Group,40,10 Main Street,Springfield,IL,62704,Internal Medicine\n"
        f"{NPI},E-1,,, , ,,,, ,\n"
    )
    staged_rows = []

    async def push(batch, _stage):
        staged_rows.extend(dict(row) for row in batch)

    monkeypatch.setattr(groups.db, "create_table", AsyncMock())
    monkeypatch.setattr(groups, "_copy_sidecar_rows", push)
    monkeypatch.setattr(groups, "raise_if_cancelled", AsyncMock())
    worker_context_by_key = {"import_date": "synthetic", "context": {}}
    receipt = await groups.import_group_site_rows(source, worker_context_by_key, {}, MANIFEST)
    assert receipt == {"source_rows": 3, "generation_id": MANIFEST["generation_id"]}
    assert [row["org_pac_id"] for row in staged_rows] == ["0012345678", "0098765432", None]
    assert staged_rows[0]["address_checksum"] == staged_rows[1]["address_checksum"]
    assert staged_rows[2]["address_checksum"] is None
    assert worker_context_by_key["context"]["group_site_stage_owned"] is True


def test_exact_zip_artifact_is_content_addressed(monkeypatch, tmp_path):
    source = tmp_path / "source.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("doctors.csv", "NPI,Ind_enrl_ID\n1000000004,E-1\n")
    payload = source.read_bytes()
    monkeypatch.setattr(artifact, "_artifact_root", lambda: tmp_path / "retained")
    first = artifact.retain_doctors_artifact(str(source), "https://example.test/source.zip")
    second = artifact.retain_doctors_artifact(str(source), "https://example.test/source.zip")
    assert first == second
    assert first["content_sha256"] == hashlib.sha256(payload).hexdigest()
    assert first["content_bytes"] == len(payload)
    assert (tmp_path / "retained" / first["file_name"]).read_bytes() == payload
    assert len(list((tmp_path / "retained").iterdir())) == 1


def test_artifact_verification_rejects_changed_missing_and_malformed_receipts(monkeypatch, tmp_path):
    source = tmp_path / "source.csv"
    source.write_bytes(b"synthetic")
    monkeypatch.setattr(artifact, "_artifact_root", lambda: tmp_path / "retained")
    receipt = artifact.retain_doctors_artifact(str(source), "https://example.test/source.csv")
    artifact.verify_doctors_artifact(receipt)

    retained = tmp_path / "retained" / receipt["file_name"]
    retained.write_bytes(b"different")
    with pytest.raises(RuntimeError, match="artifact_changed"):
        artifact.verify_doctors_artifact(receipt)
    retained.unlink()
    with pytest.raises(RuntimeError, match="artifact_missing"):
        artifact.verify_doctors_artifact(receipt)
    with pytest.raises(RuntimeError, match="receipt_invalid"):
        artifact.verify_doctors_artifact({**receipt, "file_name": []})


def test_artifact_rejects_missing_or_temporary_root(tmp_path, monkeypatch):
    source = tmp_path / "source.csv"
    source.write_text("synthetic")
    monkeypatch.delenv("HLTHPRT_CMS_DOCTORS_ARTIFACT_ROOT", raising=False)
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", raising=False)
    with pytest.raises(RuntimeError, match="artifact_root_required"):
        artifact.retain_doctors_artifact(str(source), "https://example.test/source.csv")
    for temporary_root in ("/tmp", "/var/tmp", "/private/var/tmp", "/dev/shm", "/run"):
        monkeypatch.setenv("HLTHPRT_CMS_DOCTORS_ARTIFACT_ROOT", f"{temporary_root}/cms-doctors")
        with pytest.raises(RuntimeError, match="artifact_root_not_durable"):
            artifact.retain_doctors_artifact(str(source), "https://example.test/source.csv")


def test_artifact_uses_existing_directory_root_when_dedicated_root_is_unset(monkeypatch):
    monkeypatch.delenv("HLTHPRT_CMS_DOCTORS_ARTIFACT_ROOT", raising=False)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", "/srv/example-evidence")
    assert str(artifact._artifact_root()) == "/srv/example-evidence/cms-doctors"


class _SidecarCopyDriver:
    def __init__(self, state):
        self.state = state

    async def set_type_codec(self, _name, **options):
        self.encoder = options["encoder"]

    def is_in_transaction(self):
        return self.state.driver_started

    def terminate(self):
        self.state.terminated = True

    async def copy_records_to_table(self, table_name, *, schema_name, columns, records, timeout=None):
        if hasattr(records, "__aiter__"):
            rows = [tuple(row) async for row in records]
        else:
            rows = list(records)
        position = tuple(columns).index("source_json")
        for row in rows:
            self.encoder(row[position])
        self.state.batches.append((table_name, schema_name, tuple(columns), rows))
        if self.state.failure is not None and len(self.state.batches) == 2:
            raise self.state.failure
        return f"COPY {len(rows)}"


@pytest.fixture
async def native_sidecar_copy(monkeypatch):
    """Exercise the actual bounded COPY helper and installed JSON codec without sockets."""
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

    from process import cms_doctors_education as education

    state = SimpleNamespace(batches=[], transactions=[], failure=None)
    state.active = state.driver_started = state.terminated = False
    state.acquires = state.begins = 0

    driver = _SidecarCopyDriver(state)
    await PGDialect_asyncpg().setup_asyncpg_json_codec(SimpleNamespace(_connection=driver))

    async def connection():
        async def raw():
            return SimpleNamespace(driver_connection=driver)

        return SimpleNamespace(get_raw_connection=raw)

    async def execute(statement):
        assert str(statement) == "SELECT pg_current_xact_id()"
        state.driver_started = True
        state.begins += 1

    session = SimpleNamespace(in_transaction=lambda: state.active, connection=connection, execute=execute)

    @asynccontextmanager
    async def transaction():
        state.active = True
        try:
            yield session
        except BaseException:
            state.transactions.append("rollback")
            raise
        else:
            state.transactions.append("commit")
        finally:
            state.active = False
            state.driver_started = False

    @asynccontextmanager
    async def legacy_acquire():
        state.acquires += 1
        yield SimpleNamespace(raw_connection=SimpleNamespace(driver_connection=driver))

    def no_insert(*_args, **_kwargs):
        raise AssertionError("sidecar INSERT fallback is forbidden")

    monkeypatch.setattr(education.db, "transaction", transaction)
    monkeypatch.setattr(education.db, "acquire", legacy_acquire)
    monkeypatch.setattr(education.db, "insert", no_insert)
    monkeypatch.setattr(education.db, "create_table", AsyncMock())
    monkeypatch.setattr(education.db, "status", AsyncMock())
    monkeypatch.setattr(education, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(groups, "raise_if_cancelled", AsyncMock())
    return state


def _sidecar_source(tmp_path, family, rows):
    """Write distinct synthetic observations at the production source-row grain."""
    import csv

    source = tmp_path / "sidecars.csv"
    with source.open("w", newline="") as stream:
        writer = csv.writer(stream)
        if family == "education":
            writer.writerow(("NPI", "Med_sch", "Grd_yr"))
            writer.writerows((NPI, f"Example School {index}", "2001") for index in range(rows))
        else:
            writer.writerow(("NPI", "Ind_enrl_ID", "Org_PAC_ID", "adrs_id", "Facility Name", "num_org_mem"))
            writer.writerows(
                (NPI, "E-1", "0012345678", f"site-{index}", "Example Group", "12") for index in range(rows)
            )
    return source


async def _import_sidecar(source, family, ctx):
    from process import cms_doctors_education as education

    if family == "education":
        return await education.import_doctor_education(source, "https://example.test/sidecars.csv", ctx, {})
    return await groups.import_group_site_rows(source, ctx, {}, MANIFEST)


@pytest.mark.parametrize("family", ["education", "group_site"])
async def test_sidecars_use_codec_compatible_copy_only_in_fixed_batches(
    tmp_path,
    native_sidecar_copy,
    family,
    capsys,
):
    """Real readers reach the actual native helper without INSERT or copied_record fallback."""
    import json
    from uuid import uuid4

    source_path = _sidecar_source(tmp_path, family, 10_001)
    worker_context_by_key = {"import_date": uuid4().hex, "context": {}}
    receipt = await _import_sidecar(source_path, family, worker_context_by_key)
    batches = native_sidecar_copy.batches
    assert [len(batch[3]) for batch in batches] == [5000, 5000, 1]
    assert native_sidecar_copy.transactions == ["commit"] * 3
    assert native_sidecar_copy.begins == 3
    assert native_sidecar_copy.acquires == 0
    assert receipt["source_rows"] == 10_001
    copied_rows = [
        dict(zip(columns, copied_record, strict=True))
        for _, _, columns, batch_records in batches
        for copied_record in batch_records
    ]
    source_observations = [json.loads(copied_record["source_json"]) for copied_record in copied_rows]
    assert [copied_record["row_number"] for copied_record in source_observations] == list(range(1, 10_002))
    assert all(
        copied_record["source_json"]
        == json.dumps(
            json.loads(copied_record["source_json"]),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        for copied_record in copied_rows
    )
    if family == "education":
        assert receipt["education_rows"] == 10_001
        assert len({copied_record["education_key"] for copied_record in copied_rows}) == 10_001
    else:
        assert [copied_record["row_number"] for copied_record in copied_rows] == list(range(1, 10_002))
        assert all(
            copied_record["membership_start_at"] is copied_record["membership_end_at"] is None
            for copied_record in copied_rows
        )
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("family", ["education", "group_site"])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_sidecar_copy_failure_rolls_back_batch_and_discards_owned_stage(
    tmp_path,
    native_sidecar_copy,
    family,
    cancelled,
    capsys,
):
    """A failed or cancelled native batch keeps prior semantics and never falls back."""
    import asyncio
    from uuid import uuid4

    source_path = _sidecar_source(tmp_path, family, 5001)
    worker_context_by_key = {"import_date": uuid4().hex, "context": {}}
    error = asyncio.CancelledError() if cancelled else RuntimeError("synthetic native COPY refusal")
    native_sidecar_copy.failure = error
    with pytest.raises(type(error)):
        await _import_sidecar(source_path, family, worker_context_by_key)
    assert native_sidecar_copy.transactions == ["commit", "rollback"]
    assert native_sidecar_copy.acquires == 0
    assert native_sidecar_copy.terminated is cancelled
    ownership = "education_stage_owned" if family == "education" else "group_site_stage_owned"
    assert ownership not in worker_context_by_key["context"]
    groups.db.status.assert_awaited_once()
    assert groups.db.status.await_args.args[0].endswith(worker_context_by_key["import_date"])
    assert capsys.readouterr() == ("", "")
