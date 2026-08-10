# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

import importlib

from pathlib import Path

from types import SimpleNamespace

from unittest.mock import AsyncMock

import os

import datetime

import uuid

from contextlib import asynccontextmanager

import pytest

from process.nppes_public_evidence_import import NPPES_RIGHTS_PROOF_SHA256

from tests.test_process_npi_unit import (
    ROOT,
    _AmbiguousPublicationConnection,
    _ShutdownRawConnection,
    _build_minimal_row,
    _fake_make_class_factory,
    _install_shutdown_success_collaborators,
    _shutdown_stage_classes,
    npi_module,
)

@pytest.mark.asyncio
async def test_process_npi_chunk_enqueues_basic_payload(monkeypatch, npi_module):
    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)

    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    worker_context_map = {"redis": fake_redis, "import_date": "20251104"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
        "Employer Identification Number (EIN)": "employer_identification_number",
        "Parent Organization TIN": "parent_organization_tin",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    npi_row_map = _build_minimal_row("1215387113")
    npi_row_map["Employer Identification Number (EIN)"] = "private-ein-sentinel"
    npi_row_map["Parent Organization TIN"] = "private-tin-sentinel"

    chunk_task_map = {
        "npi_csv_map": npi_csv_map,
        "npi_csv_map_reverse": npi_csv_map_reverse,
        "taxonomy_int_code_map": {"1223D0001X": 4101},
        "row_list": [npi_row_map],
    }

    await npi_module.process_npi_chunk(worker_context_map, chunk_task_map)

    fake_redis.enqueue_job.assert_awaited_once()
    enqueue_payload_map = fake_redis.enqueue_job.await_args.args[1]

    assert enqueue_payload_map["npi_obj_list"][0]["npi"] == 1215387113
    assert enqueue_payload_map["npi_obj_list"][0]["employer_identification_number"] is None
    assert enqueue_payload_map["npi_obj_list"][0]["parent_organization_tin"] is None
    address_by_type = {
        entry["type"]: entry
        for entry in enqueue_payload_map["npi_address_list"]
    }
    assert address_by_type["primary"]["city_name"] == "AUSTIN"
    assert address_by_type["mail"]["first_line"] == "PO Box 1"

@pytest.mark.asyncio
async def test_process_npi_chunk_precomputes_address_key_when_enabled(monkeypatch, npi_module):
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "nppes")

    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    worker_context_map = {"redis": fake_redis, "import_date": "20251104"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    npi_row_map = _build_minimal_row("1215387113")

    await npi_module.process_npi_chunk(
        worker_context_map,
        {
            "npi_csv_map": npi_csv_map,
            "npi_csv_map_reverse": npi_csv_map_reverse,
            "row_list": [npi_row_map],
        },
    )

    enqueue_payload_map = fake_redis.enqueue_job.await_args.args[1]
    address_by_type = {
        entry["type"]: entry
        for entry in enqueue_payload_map["npi_address_list"]
    }
    assert address_by_type["primary"]["address_key"] == npi_module.address_key_v1(
        "123 Main St",
        "",
        "AUSTIN",
        "TX",
        "78701",
        "US",
    )
    assert address_by_type["mail"]["address_key"] == npi_module.address_key_v1(
        "PO Box 1",
        "",
        "AUSTIN",
        "TX",
        "78702",
        "US",
    )

@pytest.mark.asyncio
async def test_process_npi_chunk_batches_address_key_precompute(monkeypatch, npi_module):
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "nppes")

    seen_batches = []

    def fake_canonicalize_batch(address_rows):
        address_row_list = list(address_rows)
        seen_batches.append(address_row_list)
        return [
            {"address_key": "00000000-0000-4000-8000-000000000001"},
            {"address_key": "00000000-0000-4000-8000-000000000002"},
        ]

    monkeypatch.setattr(npi_module, "canonicalize_address_batch", fake_canonicalize_batch)
    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    worker_context_map = {"redis": fake_redis, "import_date": "20251104"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    await npi_module.process_npi_chunk(
        worker_context_map,
        {
            "npi_csv_map": npi_csv_map,
            "npi_csv_map_reverse": npi_csv_map_reverse,
            "row_list": [_build_minimal_row("1215387113")],
        },
    )

    enqueue_payload_map = fake_redis.enqueue_job.await_args.args[1]
    address_by_type = {
        entry["type"]: entry
        for entry in enqueue_payload_map["npi_address_list"]
    }
    assert len(seen_batches) == 1
    assert seen_batches[0] == [
        ("123 Main St", "", "AUSTIN", "TX", "78701", "US"),
        ("PO Box 1", "", "AUSTIN", "TX", "78702", "US"),
    ]
    assert address_by_type["primary"]["address_key"] == uuid.UUID("00000000-0000-4000-8000-000000000001")
    assert address_by_type["mail"]["address_key"] == uuid.UUID("00000000-0000-4000-8000-000000000002")

@pytest.mark.asyncio
async def test_process_npi_chunk_batches_contact_normalization(monkeypatch, npi_module):
    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)

    seen_batches = []

    def fake_canonicalize_contact_batch(contact_rows):
        contact_row_list = list(contact_rows)
        seen_batches.append(contact_row_list)
        return [
            {
                "phone_number": "5125550100",
                "phone_extension": None,
                "fax_number_digits": None,
                "fax_extension": None,
            },
            {
                "phone_number": "5125550199",
                "phone_extension": None,
                "fax_number_digits": None,
                "fax_extension": None,
            },
        ]

    monkeypatch.setattr(npi_module, "canonicalize_contact_batch", fake_canonicalize_contact_batch)
    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    worker_context_map = {"redis": fake_redis, "import_date": "20251104"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    await npi_module.process_npi_chunk(
        worker_context_map,
        {
            "npi_csv_map": npi_csv_map,
            "npi_csv_map_reverse": npi_csv_map_reverse,
            "row_list": [_build_minimal_row("1215387113")],
        },
    )

    enqueue_payload_map = fake_redis.enqueue_job.await_args.args[1]
    address_by_type = {
        entry["type"]: entry
        for entry in enqueue_payload_map["npi_address_list"]
    }
    assert seen_batches == [
        [
            ("5125550100", "", "US"),
            ("5125550199", "", "US"),
        ]
    ]
    assert address_by_type["primary"]["phone_number"] == "5125550100"
    assert address_by_type["mail"]["phone_number"] == "5125550199"

@pytest.mark.asyncio
async def test_process_npi_chunk_populates_taxonomy_variants(monkeypatch, npi_module):
    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)

    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    worker_context_map = {"redis": fake_redis, "import_date": "20251105"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    npi_row_map = _build_minimal_row("1415980663")
    npi_row_map["Entity Type Code"] = "<UNAVAIL>"
    npi_row_map["Last Update Date"] = "2024-01-15"
    npi_row_map["Healthcare Provider Taxonomy Code_1"] = "1223D0001X"
    npi_row_map["Healthcare Provider Primary Taxonomy Switch_1"] = "Y"
    npi_row_map["Provider License Number_1"] = "12345"
    npi_row_map["Provider License Number State Code_1"] = "TX"
    npi_row_map["Healthcare Provider Taxonomy Group_1"] = "Special Group"
    npi_row_map["Other Provider Identifier_1"] = "ALT123"
    npi_row_map["Other Provider Identifier Type Code_1"] = "05"
    npi_row_map["Other Provider Identifier State_1"] = "TX"
    npi_row_map["Other Provider Identifier Issuer_1"] = "Issuer"

    chunk_task_map = {
        "npi_csv_map": npi_csv_map,
        "npi_csv_map_reverse": npi_csv_map_reverse,
        "taxonomy_int_code_map": {"1223D0001X": 4101},
        "row_list": [npi_row_map],
    }

    await npi_module.process_npi_chunk(worker_context_map, chunk_task_map)

    fake_redis.enqueue_job.assert_awaited_once()
    enqueue_payload_map = fake_redis.enqueue_job.await_args.args[1]

    taxonomy_entry = enqueue_payload_map["npi_taxonomy_list"][0]
    assert taxonomy_entry["healthcare_provider_taxonomy_code"] == "1223D0001X"
    assert taxonomy_entry["provider_license_number_state_code"] == "TX"

    other_identifier = enqueue_payload_map["npi_other_id_list"][0]
    assert other_identifier["other_provider_identifier"] == "ALT123"

    taxonomy_group = enqueue_payload_map["npi_taxonomy_group_list"][0]
    assert taxonomy_group["healthcare_provider_taxonomy_group"] == "Special Group"

    address_by_type = {
        entry["type"]: entry
        for entry in enqueue_payload_map["npi_address_list"]
    }
    assert address_by_type["primary"]["taxonomy_array"] == [4101]
    assert address_by_type["mail"]["taxonomy_array"] == [4101]

@pytest.mark.asyncio
async def test_save_npi_data_dispatch(monkeypatch, npi_module):

    push_calls = []

    async def fake_push(objects, cls, rewrite=False):
        push_calls.append((cls.__tablename__, rewrite, objects))

    monkeypatch.setattr(npi_module, "push_objects", fake_push)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())

    worker_context_map = {"import_date": "20251106"}
    save_task_map = {
        "npi_obj_list": [{"npi": 1, "entity_type_code": 2}],
        "npi_taxonomy_list": [{"npi": 1, "checksum": 10}],
        "npi_other_id_list": [{"npi": 1, "checksum": 11}],
        "npi_taxonomy_group_list": [{"npi": 1, "checksum": 12}],
        "npi_address_list": [{"npi": 1, "checksum": 13, "type": "primary"}],
        "unexpected": [{"value": 99}],
    }

    await npi_module.save_npi_data(worker_context_map, save_task_map)

    rewrite_flags_by_table = {name: flag for name, flag, _ in push_calls}
    assert rewrite_flags_by_table == {
        "npi_20251106": True,
        "npi_taxonomy_20251106": True,
        "npi_other_identifier_20251106": False,
        "npi_taxonomy_group_20251106": True,
        "npi_address_20251106": True,
    }

@pytest.mark.asyncio
async def test_process_data_no_remote_files(monkeypatch, npi_module):

    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "feed.html")

    call_order_events: list[str] = []

    async def empty_listing(*_args, **_kwargs):
        call_order_events.append("listing")
        return ""

    download_mock = AsyncMock(side_effect=empty_listing)
    monkeypatch.setattr(npi_module, "download_it", download_mock)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module, "_ensure_required_extensions", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nucc_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nppes_canonical_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_load_nucc_taxonomy_int_code_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi_module, "_prepare_npi_staging", AsyncMock())
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())
    acquire_lease = AsyncMock(
        side_effect=lambda _context: call_order_events.append("lease")
    )
    assert_lease = AsyncMock(
        side_effect=lambda _context: call_order_events.append("assert")
    )
    release_lease = AsyncMock()
    monkeypatch.setattr(npi_module, "_acquire_npi_import_lease", acquire_lease)
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", assert_lease)
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", release_lease)

    worker_context_map = {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20251107",
    }

    with pytest.raises(
        npi_module.NPIPrerequisiteError,
        match="No NPPES source archives",
    ):
        await npi_module.process_data(worker_context_map)

    assert worker_context_map["context"]["run"] == 0
    download_mock.assert_awaited()
    npi_module._prepare_npi_staging.assert_not_awaited()
    assert call_order_events[:3] == ["lease", "assert", "listing"]
    release_lease.assert_awaited_once_with(
        worker_context_map["context"],
        suppress_errors=True,
    )

@pytest.mark.asyncio
async def test_process_data_required_test_mode_cannot_write_immutable_evidence(
    monkeypatch,
    npi_module,
):
    monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_MODE", "required")
    monkeypatch.setenv(
        "HLTHPRT_NPPES_RIGHTS_PROOF_SHA256",
        NPPES_RIGHTS_PROOF_SHA256,
    )
    prepare_chain = AsyncMock()
    import_chain = AsyncMock()
    ensure_database = AsyncMock()
    monkeypatch.setattr(npi_module, "prepare_nppes_release_chain", prepare_chain)
    monkeypatch.setattr(
        npi_module,
        "import_nppes_public_evidence_chain",
        import_chain,
    )
    monkeypatch.setattr(npi_module, "ensure_database", ensure_database)
    worker_context_map = {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20260809",
    }

    with pytest.raises(
        npi_module.NPIPrerequisiteError,
        match="cannot admit immutable public evidence",
    ):
        await npi_module.process_data(worker_context_map, {"test_mode": True})

    assert worker_context_map["context"]["run"] == 0
    prepare_chain.assert_not_awaited()
    import_chain.assert_not_awaited()
    ensure_database.assert_not_awaited()

@pytest.mark.asyncio
async def test_npi_import_lease_is_session_owned_and_released(
    monkeypatch,
    npi_module,
):
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[True, 731, 731, True, True])
    )
    exit_events: list[tuple[object, object, object]] = []

    @asynccontextmanager
    async def lease_manager():
        try:
            yield connection
        finally:
            exit_events.append((None, None, None))

    manager = lease_manager()
    monkeypatch.setattr(npi_module.db, "acquire_driver", lambda: manager)
    worker_context_by_key: dict[str, object] = {}
    await npi_module._acquire_npi_import_lease(worker_context_by_key)
    await npi_module._assert_npi_import_lease(worker_context_by_key)
    with pytest.raises(npi_module.NPIPrerequisiteError, match="already held"):
        await npi_module._acquire_npi_import_lease(worker_context_by_key)
    await npi_module._release_npi_import_lease(
        worker_context_by_key,
        suppress_errors=False,
    )
    assert npi_module._NPI_IMPORT_LEASE_KEY not in worker_context_by_key
    assert connection.fetchval.await_count == 5
    assert len(exit_events) == 1
