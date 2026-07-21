from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

import process.uhc_retained_source_registry as source_registry
from process.uhc_retained_locks import retained_source_lock
from process.uhc_retained_native import retain_source_native
from process.uhc_retained_registry_contract import UHCSourceBindingMismatch
from process.uhc_retained_source_registry import (
    admit_retained_source,
    register_verified_source,
)
from process.uhc_retained_types import UHCRetainedAdmissionError
from tests.uhc_retained_registry_test_support import (
    FakeRegistryConnection,
    install_fake_native,
    native_summary,
    native_verified_source,
    records_payload,
    source_binding,
    write_retained_fixture,
)


def _allow_unit_root(monkeypatch) -> None:
    monkeypatch.setattr(
        source_registry,
        "_require_durable_retained_root",
        lambda _output_root: None,
    )


async def _fresh_source(fixture):
    return await retain_source_native(
        source_path=fixture["raw_path"],
        output_root=Path(fixture["raw_path"]).parent,
        expected_sha256=fixture["artifact_sha256"],
        expected_byte_count=fixture["artifact_byte_count"],
        range_count=fixture["range_count"],
    )


def _assert_first_binding_reference_layouts(connection, first_binding) -> None:
    reference_layouts = {
        (key[2], key[3], key[4])
        for key in connection.rows_by_table["reference"]
        if key[:2]
        == (
            first_binding.catalog_set_sha256,
            first_binding.source_file_id,
        )
    }
    assert reference_layouts == {
        ("raw", 0, 0),
        ("manifest", 2, 4),
        ("manifest", 2, 8),
    }


@pytest.mark.asyncio
async def test_registry_persists_normalized_raw_layout_ranges_and_references(
    tmp_path,
    monkeypatch,
):
    fixture, verified_source = await native_verified_source(tmp_path, monkeypatch)
    binding = source_binding(
        verified_source.raw_artifact.sha256,
        verified_source.raw_artifact.byte_count,
    )
    connection = FakeRegistryConnection()
    connection.add_catalog(binding)
    _allow_unit_root(monkeypatch)

    await register_verified_source(
        connection,
        binding=binding,
        source=verified_source,
    )

    assert len(connection.rows_by_table["raw"]) == 1
    assert len(connection.rows_by_table["layout"]) == 1
    assert len(connection.rows_by_table["binding"]) == 1
    assert len(connection.rows_by_table["range"]) == 4
    assert len(connection.rows_by_table["reference"]) == 2
    assert {
        key[2] for key in connection.rows_by_table["reference"]
    } == {"raw", "manifest"}
    raw_row = next(iter(connection.rows_by_table["raw"].values()))
    assert set(raw_row) == {
        "artifact_sha256",
        "byte_count",
        "storage_uri",
        "status",
    }
    layout_row = next(iter(connection.rows_by_table["layout"].values()))
    assert layout_row["record_count"] == fixture["record_count"]
    assert layout_row["manifest_sha256"] == fixture["manifest_sha256"]
    manifest_reference = next(
        reference_row
        for key, reference_row in connection.rows_by_table["reference"].items()
        if key[2] == "manifest"
    )
    assert manifest_reference["layout_artifact_sha256"] == fixture[
        "artifact_sha256"
    ]


@pytest.mark.asyncio
async def test_idempotent_registration_requires_a_fresh_native_attestation(
    tmp_path,
    monkeypatch,
):
    fixture, verified_source = await native_verified_source(tmp_path, monkeypatch)
    binding = source_binding(
        fixture["artifact_sha256"],
        fixture["artifact_byte_count"],
    )
    connection = FakeRegistryConnection()
    connection.add_catalog(binding)
    _allow_unit_root(monkeypatch)
    await register_verified_source(
        connection,
        binding=binding,
        source=verified_source,
    )

    with pytest.raises(UHCRetainedAdmissionError, match="native-verified"):
        await register_verified_source(
            connection,
            binding=binding,
            source=verified_source,
        )

    fresh_source = await _fresh_source(fixture)
    await register_verified_source(
        connection,
        binding=binding,
        source=fresh_source,
    )
    assert [len(table_rows) for table_rows in connection.rows_by_table.values()] == [
        1,
        1,
        1,
        1,
        4,
        2,
    ]


@pytest.mark.asyncio
async def test_same_raw_supports_two_layouts_and_two_catalog_bindings(
    tmp_path,
    monkeypatch,
):
    retained_root = tmp_path / "retained"
    retained_records = records_payload(count=16)
    fixture_four = write_retained_fixture(
        retained_root,
        retained_records,
        range_count=4,
    )
    source_path = tmp_path / "source.json"
    source_path.write_bytes(fixture_four["source_bytes"])
    install_fake_native(tmp_path, monkeypatch, native_summary(fixture_four))
    source_four = await _fresh_source(fixture_four)
    first_binding = source_binding(
        fixture_four["artifact_sha256"],
        fixture_four["artifact_byte_count"],
        catalog_set="catalog-one",
    )
    second_binding = source_binding(
        fixture_four["artifact_sha256"],
        fixture_four["artifact_byte_count"],
        catalog_set="catalog-two",
    )
    connection = FakeRegistryConnection()
    connection.add_catalog(first_binding)
    connection.add_catalog(second_binding)
    _allow_unit_root(monkeypatch)
    await register_verified_source(
        connection,
        binding=first_binding,
        source=source_four,
    )
    await register_verified_source(
        connection,
        binding=second_binding,
        source=await _fresh_source(fixture_four),
    )

    fixture_eight = write_retained_fixture(
        retained_root,
        retained_records,
        range_count=8,
    )
    install_fake_native(tmp_path, monkeypatch, native_summary(fixture_eight))
    await register_verified_source(
        connection,
        binding=first_binding,
        source=await _fresh_source(fixture_eight),
    )

    assert len(connection.rows_by_table["raw"]) == 1
    assert len(connection.rows_by_table["layout"]) == 2
    assert len(connection.rows_by_table["range"]) == 12
    assert len(connection.rows_by_table["binding"]) == 2
    assert len(connection.rows_by_table["reference"]) == 5
    _assert_first_binding_reference_layouts(connection, first_binding)


@pytest.mark.parametrize(
    ("collection_kind", "retained_records"),
    [
        ("provider_membership", records_payload("provider", 16)),
        (
            "plan_reference",
            [
                {"PlanID": f"P{ordinal:03}", "PlanName": f"Plan {ordinal}"}
                for ordinal in range(24)
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_integrated_admission_is_generic_for_provider_and_plan_json(
    tmp_path,
    monkeypatch,
    collection_kind,
    retained_records,
):
    base_root = tmp_path / "durable-provider-directory"
    fake_system_temp = tmp_path / "unrelated-system-temp"
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(base_root))
    monkeypatch.setattr(
        source_registry.tempfile,
        "gettempdir",
        lambda: str(fake_system_temp),
    )
    retained_root = source_registry.uhc_retained_artifact_root()
    fixture = write_retained_fixture(
        retained_root,
        retained_records,
        range_count=4,
    )
    source_path = tmp_path / f"{collection_kind}.json"
    source_path.write_bytes(fixture["source_bytes"])
    install_fake_native(tmp_path, monkeypatch, native_summary(fixture))
    binding = source_binding(
        fixture["artifact_sha256"],
        fixture["artifact_byte_count"],
        collection_kind=collection_kind,
    )
    connection = FakeRegistryConnection()
    connection.add_catalog(binding)

    verified_source = await admit_retained_source(
        connection,
        binding=binding,
        source_path=source_path,
        expected_sha256=fixture["artifact_sha256"],
        expected_byte_count=fixture["artifact_byte_count"],
        range_count=4,
    )

    assert verified_source.raw_artifact.record_count == len(retained_records)
    binding_row = next(iter(connection.rows_by_table["binding"].values()))
    assert binding_row["collection_kind"] == collection_kind
    assert len(connection.rows_by_table["range"]) == 4


@pytest.mark.asyncio
async def test_registry_rejects_released_reference_without_reactivation(
    tmp_path,
    monkeypatch,
):
    fixture, verified_source = await native_verified_source(tmp_path, monkeypatch)
    binding = source_binding(
        fixture["artifact_sha256"],
        fixture["artifact_byte_count"],
    )
    connection = FakeRegistryConnection()
    connection.add_catalog(binding)
    _allow_unit_root(monkeypatch)
    await register_verified_source(
        connection,
        binding=binding,
        source=verified_source,
    )
    raw_reference_key = (
        binding.catalog_set_sha256,
        binding.source_file_id,
        "raw",
        0,
        0,
    )
    connection.rows_by_table["reference"][raw_reference_key]["released_at"] = (
        "2026-07-20T12:00:00Z"
    )

    with pytest.raises(UHCSourceBindingMismatch, match="artifact reference"):
        await register_verified_source(
            connection,
            binding=binding,
            source=await _fresh_source(fixture),
        )
    assert connection.rows_by_table["reference"][raw_reference_key][
        "released_at"
    ] == "2026-07-20T12:00:00Z"


@pytest.mark.parametrize(
    ("catalog_updates", "message"),
    [
        ({"size_bytes": 1}, "catalog file identity"),
        ({"availability": "withdrawn"}, "availability"),
        ({"catalog_support": "probe_only"}, "availability"),
        ({"file_name": "different.json"}, "catalog file identity"),
    ],
)
@pytest.mark.asyncio
async def test_catalog_identity_and_lifecycle_are_exact(
    tmp_path,
    monkeypatch,
    catalog_updates,
    message,
):
    fixture, source = await native_verified_source(tmp_path, monkeypatch)
    binding = source_binding(
        fixture["artifact_sha256"],
        fixture["artifact_byte_count"],
    )
    connection = FakeRegistryConnection()
    connection.add_catalog(binding)
    catalog_row = connection.rows_by_table["catalog"][
        (binding.catalog_set_sha256, binding.source_file_id)
    ]
    catalog_row.update(catalog_updates)
    _allow_unit_root(monkeypatch)

    with pytest.raises(UHCRetainedAdmissionError, match=message):
        await register_verified_source(connection, binding=binding, source=source)
    assert not connection.rows_by_table["raw"]


@pytest.mark.asyncio
async def test_registry_rejects_file_swap_and_forged_proof_before_db_write(
    tmp_path,
    monkeypatch,
):
    fixture, source = await native_verified_source(tmp_path, monkeypatch)
    binding = source_binding(
        fixture["artifact_sha256"],
        fixture["artifact_byte_count"],
    )
    connection = FakeRegistryConnection()
    connection.add_catalog(binding)
    _allow_unit_root(monkeypatch)

    raw_path = Path(source.raw_artifact.path)
    raw_path.write_bytes(raw_path.read_bytes())
    with pytest.raises(UHCRetainedAdmissionError, match="changed"):
        await register_verified_source(connection, binding=binding, source=source)
    assert not connection.rows_by_table["raw"]

    fresh = await _fresh_source(fixture)
    forged = replace(
        fresh,
        raw_artifact=replace(fresh.raw_artifact, record_count=999),
    )
    with pytest.raises(UHCRetainedAdmissionError, match="native-verified"):
        await register_verified_source(connection, binding=binding, source=forged)


@pytest.mark.asyncio
async def test_transaction_failure_rolls_back_all_rows_and_consumes_proof(
    tmp_path,
    monkeypatch,
):
    fixture, source = await native_verified_source(tmp_path, monkeypatch)
    binding = source_binding(
        fixture["artifact_sha256"],
        fixture["artifact_byte_count"],
    )
    connection = FakeRegistryConnection()
    connection.add_catalog(binding)
    _allow_unit_root(monkeypatch)

    def fail_on_range(statement, _connection):
        if "provider_directory_uhc_raw_range" in statement:
            raise RuntimeError("database failure")

    connection.after_execute = fail_on_range
    with pytest.raises(RuntimeError, match="database failure"):
        await register_verified_source(connection, binding=binding, source=source)
    assert not connection.rows_by_table["raw"]
    assert not connection.rows_by_table["layout"]
    assert not connection.rows_by_table["binding"]
    assert not connection.rows_by_table["range"]
    assert not connection.rows_by_table["reference"]
    with pytest.raises(UHCRetainedAdmissionError, match="native-verified"):
        await register_verified_source(connection, binding=binding, source=source)


def test_raw_sha_lock_is_shared_by_all_layout_counts_and_versions(tmp_path):
    artifact_sha256 = hashlib.sha256(b"raw").hexdigest()

    first = retained_source_lock(tmp_path, artifact_sha256)
    second = retained_source_lock(tmp_path, artifact_sha256)

    assert first.path == second.path
    assert artifact_sha256 in first.path.name
    assert "ranges" not in first.path.name


def test_durable_root_rejects_missing_temp_root_and_symlinked_child(
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", raising=False)
    with pytest.raises(UHCRetainedAdmissionError, match="requires"):
        source_registry.uhc_retained_artifact_root()

    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(tmp_path))
    with pytest.raises(UHCRetainedAdmissionError, match="temporary"):
        source_registry.uhc_retained_artifact_root()

    base_root = tmp_path / "durable"
    base_root.mkdir()
    target = tmp_path / "target"
    target.mkdir()
    (base_root / "uhc-provider-files").symlink_to(target, target_is_directory=True)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(base_root))
    monkeypatch.setattr(
        source_registry.tempfile,
        "gettempdir",
        lambda: str(tmp_path / "unrelated-temp"),
    )
    with pytest.raises(UHCRetainedAdmissionError, match="symbolic"):
        source_registry.uhc_retained_artifact_root()


@pytest.mark.asyncio
async def test_registry_rejects_noncanonical_descendant_of_durable_root(
    tmp_path,
    monkeypatch,
):
    fixture, source = await native_verified_source(tmp_path, monkeypatch)
    binding = source_binding(
        fixture["artifact_sha256"],
        fixture["artifact_byte_count"],
    )
    base_root = tmp_path / "base"
    required_root = base_root / "uhc-provider-files"
    required_root.mkdir(parents=True)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(base_root))
    monkeypatch.setattr(
        source_registry.tempfile,
        "gettempdir",
        lambda: str(tmp_path / "other-temp"),
    )

    with pytest.raises(UHCRetainedAdmissionError, match="exact configured"):
        await register_verified_source(
            FakeRegistryConnection(),
            binding=binding,
            source=source,
        )
