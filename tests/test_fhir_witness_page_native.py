# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native Rust parity tests for shared FHIR witness, typed projection, root folding and address batches."""

import json
import re
from datetime import UTC, datetime

import pytest

from process.network_fhir_retained_capture_contract import registry_location_id
from process.network_fhir_retained_native import postgres_micros


def _native():
    return pytest.importorskip("ptg2_address_canon")


def _sample_rows(dataset_id="ds-1", *, completion_v3=False):
    from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256
    from process.provider_directory_resource_hash import (
        resource_content_hash_payload,
        resource_payload_sha256_for_contract,
    )

    sample_specs = [
        (
            "InsurancePlan",
            "plan-1",
            {"resource_id": "plan-1", "source_id": "src-1", "network_refs": ["Organization/org-1"]},
        ),
        (
            "Location",
            "loc-1",
            {
                "resource_id": "loc-1",
                "source_id": "src-1",
                "status": "active",
                "address_line1": "123 Main St",
                "city": "Boston",
                "state": "MA",
                "postal_code": "02110",
            },
        ),
        (
            "Organization",
            "org-1",
            {
                "resource_id": "org-1",
                "source_id": "src-1",
                "name": 'O"Brien\n\t\x00\x1f Café 🚀',
                "active": True,
            },
        ),
    ]
    witness_rows = []
    for kind, resource_id, resource_doc in sample_specs:
        payload_hash = (
            canonical_payload_sha256(resource_content_hash_payload(resource_doc))
            if completion_v3
            else resource_payload_sha256_for_contract(resource_doc, "transport_neutral_v2", resource_type=kind)
        )
        witness_rows.append(
            {
                "dataset_id": dataset_id,
                "resource_type": kind,
                "resource_id": resource_id,
                "payload_hash": payload_hash,
                "payload_json": json.dumps(resource_doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                "acquired_resource_sha256": ("f" * 64) if completion_v3 else None,
            }
        )
    return tuple(witness_rows)


def test_encode_retained_fhir_capture_rows_matches_batch_json_encoder():
    native = _native()
    witness_rows = _sample_rows()
    observed_at = postgres_micros(datetime(2026, 6, 1, 12, 0, tzinfo=UTC))
    resource_types = ["InsurancePlan", "Location", "Organization"]
    batch_input_by_field = {
        "source_id": "src-1",
        "release_id": "rel-1",
        "observed_at_pg_micros": observed_at,
        "hash_contract": "transport_neutral_v2",
        "resource_types": resource_types,
        "derive": True,
        "rows": [
            {
                "resource_type": witness_row["resource_type"],
                "resource_id": witness_row["resource_id"],
                "payload_hash": witness_row["payload_hash"],
                "payload_json": witness_row["payload_json"],
                "acquired_resource_sha256": witness_row["acquired_resource_sha256"],
                "site_id": str(registry_location_id("src-1", witness_row["resource_id"]))
                if witness_row["resource_type"] == "Location"
                else None,
            }
            for witness_row in witness_rows
        ],
    }
    expected = native.encode_retained_fhir_capture_batch(
        json.dumps(batch_input_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    )
    direct_rows = [
        (
            witness_row["resource_type"],
            witness_row["resource_id"],
            witness_row["payload_hash"],
            witness_row["payload_json"],
            witness_row["acquired_resource_sha256"],
            None,
        )
        for witness_row in witness_rows
    ]
    actual = native.encode_retained_fhir_capture_rows(
        ("src-1", "rel-1", observed_at, "transport_neutral_v2", None, resource_types, True),
        direct_rows,
    )
    assert actual == expected


def _parallel_retained_rows(resource_types, *, derive):
    """Build 32 mixed-family rows exercising parallel Rust validation and hashing."""
    from process.provider_directory_resource_hash import resource_payload_sha256_for_contract

    raw_rows = []
    for index in range(32):
        kind = resource_types[index % len(resource_types)]
        resource_id = f"{kind.lower()}-{index:03d}"
        if kind == "InsurancePlan":
            resource_by_field = {
                "resource_id": resource_id,
                "source_id": "src-1",
                "network_refs": ["Organization/organization-002"],
            }
        elif kind == "Location":
            resource_by_field = {
                "resource_id": resource_id,
                "source_id": "src-1",
                "status": "active",
                "first_line": f"{index} Main",
            }
        elif kind == "Organization":
            resource_by_field = {
                "resource_id": resource_id,
                "source_id": "src-1",
                "name": f"Org {index}",
                "active": True,
            }
        else:
            resource_by_field = {
                "resource_id": resource_id,
                "source_id": "src-1",
                "active": True,
                "practitioner_ref": "Practitioner/p1",
                "network_refs": ["Organization/organization-002"],
                "location_refs": ["Location/location-001"],
            }
        payload_hash = resource_payload_sha256_for_contract(
            resource_by_field, "transport_neutral_v2", resource_type=kind
        )
        raw_rows.append(
            {
                "resource_type": kind,
                "resource_id": resource_id,
                "payload_hash": payload_hash,
                "payload_json": json.dumps(
                    resource_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                ),
                "acquired_resource_sha256": None,
                "site_id": str(registry_location_id("src-1", resource_id)) if derive and kind == "Location" else None,
            }
        )
    return raw_rows


@pytest.mark.parametrize("derive", [False, True])
def test_encode_retained_fhir_capture_rows_parallel_batch_matches_json_encoder(derive):
    """Verify parallel row encoding matches sequential JSON encoding and first-error index."""
    native = _native()
    observed_at = postgres_micros(datetime(2026, 6, 1, 12, 0, tzinfo=UTC))
    resource_types = ["InsurancePlan", "Location", "Organization", "PractitionerRole"]
    raw_rows = _parallel_retained_rows(resource_types, derive=derive)
    batch_input_by_field = {
        "source_id": "src-1",
        "release_id": "rel-1",
        "observed_at_pg_micros": observed_at,
        "hash_contract": "transport_neutral_v2",
        "resource_types": resource_types,
        "derive": derive,
        "rows": raw_rows,
    }
    expected = native.encode_retained_fhir_capture_batch(
        json.dumps(batch_input_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    )
    direct_rows = [
        (
            raw_row["resource_type"],
            raw_row["resource_id"],
            raw_row["payload_hash"],
            raw_row["payload_json"],
            None,
            None,
        )
        for raw_row in raw_rows
    ]
    actual = native.encode_retained_fhir_capture_rows(
        ("src-1", "rel-1", observed_at, "transport_neutral_v2", None, resource_types, derive),
        direct_rows,
    )
    assert actual == expected

    bad_rows = list(direct_rows)
    bad_rows[19] = (bad_rows[19][0], bad_rows[19][1], "0" * 64, bad_rows[19][3], None, None)
    bad_rows[27] = (bad_rows[27][0], "bad/id", bad_rows[27][2], bad_rows[27][3], None, None)
    with pytest.raises(ValueError, match=r'"code":"payload_hash","row_index":19'):
        native.encode_retained_fhir_capture_rows(
            ("src-1", "rel-1", observed_at, "transport_neutral_v2", None, resource_types, derive),
            bad_rows,
        )


def test_prepare_typed_fhir_copy_page_encodes_binary_copy():
    """Verify binary COPY encoding for typed FHIR pages and SQL fallback on malformed shapes."""
    native = _native()
    witness_rows = _sample_rows()
    exact_bytes = len(json.dumps(witness_rows, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
    prepared = native.prepare_typed_fhir_copy_page(
        witness_rows,
        ("ds-1", "src-1"),
        (["InsurancePlan", "Location", "Organization"], ["Location", "Organization"]),
        None,
        False,
        exact_bytes,
    )
    assert prepared is not None
    copy_bytes, projected_count, last_type, last_id, counts_by_kind = prepared
    assert copy_bytes.startswith(b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0")
    assert copy_bytes.endswith(b"\xff\xff")
    assert projected_count == 2
    assert (last_type, last_id) == ("Organization", "org-1")
    assert counts_by_kind == {"InsurancePlan": 1, "Location": 1, "Organization": 1}

    bad_shape_by_field = {
        **witness_rows[2],
        "payload_json": json.dumps({"resource_id": "org-1", "active": "yes"}),
    }
    assert (
        native.prepare_typed_fhir_copy_page(
            (bad_shape_by_field,),
            ("ds-1", "src-1"),
            (["Organization"], ["Organization"]),
            None,
            False,
            65536,
        )
        is None
    )


@pytest.mark.parametrize("completion_v3", [False, True])
def test_validate_and_prepare_typed_fhir_page_match_python_semantics(completion_v3):
    native = _native()
    witness_rows = _sample_rows(completion_v3=completion_v3)
    exact_bytes = len(json.dumps(witness_rows, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
    summary = native.validate_fhir_witness_page(
        witness_rows,
        "ds-1",
        ["InsurancePlan", "Location", "Organization"],
        None,
        (True, completion_v3, True, True),
        exact_bytes,
    )
    assert summary == ("Organization", "org-1", {"InsurancePlan": 1, "Location": 1, "Organization": 1})
    with pytest.raises(ValueError, match="page_bounds"):
        native.validate_fhir_witness_page(
            witness_rows,
            "ds-1",
            ["InsurancePlan", "Location", "Organization"],
            None,
            (True, completion_v3, True, True),
            exact_bytes - 1,
        )

    prepared = native.prepare_typed_fhir_page(
        witness_rows,
        ("ds-1", "src-1"),
        (["InsurancePlan", "Location", "Organization"], ["Location", "Organization"]),
        None,
        completion_v3,
        exact_bytes,
    )
    assert prepared is not None
    encoded_json, projected_count, last_type, last_id, counts_by_kind = prepared
    assert projected_count == 2
    assert (last_type, last_id) == ("Organization", "org-1")
    assert counts_by_kind == {"InsurancePlan": 1, "Location": 1, "Organization": 1}
    decoded = json.loads(encoded_json)
    assert [entry["resource_type"] for entry in decoded] == ["Location", "Organization"]
    assert decoded[0]["site_id"] == str(registry_location_id("src-1", "loc-1"))
    assert decoded[1]["site_id"] is None
    assert decoded[1]["payload_json"] == witness_rows[2]["payload_json"]


def test_witness_page_rejects_bad_inputs_and_dict_fallback():
    native = _native()
    witness_rows = _sample_rows()
    with pytest.raises(ValueError, match="resource_order"):
        native.validate_fhir_witness_page(
            witness_rows, "ds-1", None, ("InsurancePlan", "plan-1"), (True, False, True, True), 65536
        )
    with pytest.raises(ValueError, match="resource_scope"):
        native.validate_fhir_witness_page(witness_rows, "ds-1", None, None, (True, True, True, True), 65536)
    with pytest.raises(ValueError, match="resource_shape"):
        native.validate_fhir_witness_page(
            ({"dataset_id": "ds-1"},), "ds-1", None, None, (True, False, True, True), 65536
        )
    dict_payload_rows = ({**witness_rows[0], "payload_json": {"resource_id": "plan-1"}},)
    assert (
        native.validate_fhir_witness_page(dict_payload_rows, "ds-1", None, None, (True, False, True, True), 65536)
        is None
    )
    assert (
        native.prepare_typed_fhir_page(
            dict_payload_rows, ("ds-1", "src-1"), (["InsurancePlan"], ["Location"]), None, False, 65536
        )
        is None
    )


def test_fold_stored_resource_roots_page_and_compact_json_len_fits():
    native = _native()
    witness_rows = _sample_rows()
    root_bytes, families_by_kind = native.fold_stored_resource_roots_page(
        witness_rows, ["InsurancePlan", "Location", "Organization"]
    )
    expected_lines = [
        json.dumps(
            [witness_row["resource_type"], witness_row["resource_id"], witness_row["payload_hash"]],
            separators=(",", ":"),
        ).encode()
        for witness_row in witness_rows
    ]
    assert root_bytes == b"\n".join(expected_lines)
    assert families_by_kind["Location"] == (expected_lines[1], 1)
    with pytest.raises(ValueError, match="membership_row_changed"):
        native.fold_stored_resource_roots_page(
            [{"resource_type": "Location", "resource_id": "bad/id", "payload_hash": "a" * 64}],
            ["Location"],
        )

    sample_rows = (
        {
            "resource_type": "PractitionerRole",
            "resource_id": "r-1",
            "payload_type": "object",
            "network_refs": '["Organization/n1"]',
        },
        {"resource_type": "PractitionerRole", "resource_id": "r-2", "payload_type": "object", "network_refs": None},
    )
    exact_len = len(json.dumps(sample_rows, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
    assert native.compact_json_utf8_len_fits(sample_rows, exact_len) is True
    assert native.compact_json_utf8_len_fits(sample_rows, exact_len - 1) is False
    assert native.compact_json_utf8_len_fits(({"nested": [1, 2]},), 1024) is None


def test_canonicalize_address_keys_batch_and_batch_json_match_canonicalize_batch():
    native = _native()
    addresses = [
        ("123 Main St", "Ste 200", "Boston", "MA", "02110", "US"),
        ("1 Infinite Loop", None, "Cupertino", "CA", "95014", "US"),
        (None, None, None, None, None, None),
    ]
    full = native.canonicalize_batch(addresses)
    keys = native.canonicalize_address_keys_batch(addresses)
    assert keys == [(item["address_key"], item["identity_key"]) for item in full]

    raw_json = native.canonicalize_batch_json(addresses, 65536)
    assert json.loads(raw_json.decode()) == full
    assert re.fullmatch(r"\[.*\]", raw_json.decode()) is not None
    with pytest.raises(ValueError, match="canonical_bound_exceeded"):
        native.canonicalize_batch_json(addresses, len(raw_json) - 1)


def _sorted_fused_witness_fixture():
    """Build 32 sorted mixed-family witness rows and matching direct native tuples."""
    resource_types = ["Location", "Organization"]
    raw_rows = sorted(
        _parallel_retained_rows(resource_types, derive=False),
        key=lambda raw_row: (raw_row["resource_type"], raw_row["resource_id"]),
    )
    witness_rows = tuple(
        {
            "dataset_id": "ds-1",
            "resource_type": raw_row["resource_type"],
            "resource_id": raw_row["resource_id"],
            "payload_hash": raw_row["payload_hash"],
            "payload_json": raw_row["payload_json"],
            "acquired_resource_sha256": raw_row["acquired_resource_sha256"],
        }
        for raw_row in raw_rows
    )
    direct_rows = [
        (
            raw_row["resource_type"],
            raw_row["resource_id"],
            raw_row["payload_hash"],
            raw_row["payload_json"],
            raw_row["acquired_resource_sha256"],
            None,
        )
        for raw_row in raw_rows
    ]
    return resource_types, witness_rows, direct_rows


def test_validate_and_encode_retained_fhir_witness_page_matches_separate_calls():
    """Verify fused witness validation and retained encoding match separate native calls."""
    from process.network_fhir_retained_native import prepare_retained_fhir_witness_page

    native = _native()
    if not hasattr(native, "validate_and_encode_retained_fhir_witness_page"):
        pytest.skip("validate_and_encode_retained_fhir_witness_page not built in active extension")
    resource_types, witness_rows, direct_rows = _sorted_fused_witness_fixture()
    context_tuple = ("src-1", "rel-1", 0, "transport_neutral_v2", None, resource_types, False)
    context_by_field = {
        "source_id": "src-1",
        "release_id": "rel-1",
        "observed_at_pg_micros": 0,
        "hash_contract": "transport_neutral_v2",
        "resource_types": resource_types,
        "derive": False,
    }
    separate = native.encode_retained_fhir_capture_rows(context_tuple, direct_rows)
    exact_bytes = len(json.dumps(witness_rows, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
    cursor, prepared = prepare_retained_fhir_witness_page(
        witness_rows,
        context_by_field,
        ("ds-1", None, exact_bytes),
        fused_encoder=lambda rows, _ctx, ds, prev, budget: native.validate_and_encode_retained_fhir_witness_page(
            rows, context_tuple, ds, prev, budget
        ),
        reference_validator=lambda _rows: None,
        reference_records=lambda _rows: (),
    )
    assert cursor == (witness_rows[-1]["resource_type"], witness_rows[-1]["resource_id"])
    assert (prepared.root_bytes, prepared.families, prepared.row_count) == (separate[3], separate[4], separate[5])
    with pytest.raises(ValueError, match="page_bounds"):
        native.validate_and_encode_retained_fhir_witness_page(
            witness_rows, context_tuple, "ds-1", None, exact_bytes - 1
        )
    with pytest.raises(ValueError, match="resource_order"):
        native.validate_and_encode_retained_fhir_witness_page(
            witness_rows,
            context_tuple,
            "ds-1",
            (witness_rows[0]["resource_type"], witness_rows[0]["resource_id"]),
            exact_bytes,
        )
    non_str_rows = ({**witness_rows[0], "payload_json": {"resource_id": witness_rows[0]["resource_id"]}},)
    assert (
        native.validate_and_encode_retained_fhir_witness_page(non_str_rows, context_tuple, "ds-1", None, exact_bytes)
        is None
    )


def _pipelined_location_pages():
    """Build a two-page Location origin and witness fixture for pipelined COPY tests."""
    from process.network_fhir_source_witness_contract import source_origin
    from tests.test_named_fhir_typed_graph import origin_fields

    origin = source_origin(
        origin_fields()
        | {
            "resource_counts": (("Location", 2),),
            "resource_hashes": (("Location", "e" * 64),),
            "descriptor_counts": (("Location", 2),),
        }
    )
    pages = tuple(
        (
            {
                "dataset_id": origin.dataset_id,
                "resource_type": "Location",
                "resource_id": f"loc-{index}",
                "payload_hash": ("a" if index == 1 else "b") * 64,
                "payload_json": f'{{"status":"active","first_line":"{index} Main St"}}',
                "acquired_resource_sha256": None,
            },
        )
        for index in (1, 2)
    )
    return origin, pages


@pytest.mark.asyncio
async def test_consume_pipelined_overlaps_prefetch_with_binary_copy():
    """Verify binary COPY pipelining starts fetching page two while page one COPY runs."""
    import asyncio
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from process.network_fhir_source_witness_contract import CONTRACT
    from process.network_fhir_typed_source import _consume

    native = _native()
    if not hasattr(native, "prepare_typed_fhir_copy_page"):
        pytest.skip("prepare_typed_fhir_copy_page not built in active extension")
    origin, pages = _pipelined_location_pages()
    copy_started, page_two_fetched = asyncio.Event(), asyncio.Event()
    events = []

    class _PipelinedConnection:
        async def copy_to_table(self, _table, **copy_options):
            assert (
                copy_options["schema_name"] == "candidate_generation"
                and copy_options["format"] == "binary"
                and len(copy_options["columns"]) == 18
            )
            if not copy_started.is_set():
                copy_started.set()
                await asyncio.wait_for(page_two_fetched.wait(), timeout=2.0)
            events.append(f"copy:{len(bytes(copy_options['source']))}")
            return "COPY 1"

    async def _pages():
        events.append("yield:1")
        yield pages[0]
        await asyncio.wait_for(copy_started.wait(), timeout=2.0)
        events.append("yield:2")
        page_two_fetched.set()
        yield pages[1]

    @asynccontextmanager
    async def _export(**_selection):
        yield {"contract": CONTRACT, "origin": origin, "resources": _pages}

    stage = SimpleNamespace(
        schema_name="candidate_generation",
        names={"cells": "source_cells"},
        require_ready=lambda *, complete: None,
    )
    total = await _consume(_export, _PipelinedConnection(), stage, object(), origin, checker=False, as_of="2026-01-01")
    assert total == 2
    assert events[:2] == ["yield:1", "yield:2"] and events[2].startswith("copy:")


@pytest.mark.asyncio
async def test_consume_pipelined_stops_fail_closed_before_prefetching():
    """Verify invalid page one raises before advancing the witness iterator to page two."""
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from process.network_fhir_source_witness_contract import CONTRACT
    from process.network_fhir_typed_contract import TypedFHIRGraphError
    from process.network_fhir_typed_source import _consume

    native = _native()
    if not hasattr(native, "prepare_typed_fhir_copy_page"):
        pytest.skip("prepare_typed_fhir_copy_page not built in active extension")
    origin, pages = _pipelined_location_pages()
    fetched_pages = []

    async def _bad_pages():
        yield ({**pages[0][0], "dataset_id": "wrong-dataset"},)
        fetched_pages.append("page-2")
        yield pages[1]

    @asynccontextmanager
    async def _bad_export(**_selection):
        yield {"contract": CONTRACT, "origin": origin, "resources": _bad_pages}

    stage = SimpleNamespace(
        schema_name="candidate_generation",
        names={"cells": "source_cells"},
        require_ready=lambda *, complete: None,
    )
    with pytest.raises(TypedFHIRGraphError, match="resource_scope"):
        await _consume(_bad_export, SimpleNamespace(), stage, object(), origin, checker=False, as_of="2026-01-01")
    assert fetched_pages == []


@pytest.mark.asyncio
async def test_drain_prefetch_task_cancels_in_flight_fetch():
    """Verify cancelling an in-flight prefetch task drains cleanly without leaking exceptions."""
    import asyncio

    from process.network_fhir_typed_source import _drain_prefetch_task

    blocked = asyncio.Event()

    async def _never_returns():
        await blocked.wait()

    pending_task = asyncio.create_task(_never_returns())
    await asyncio.sleep(0)
    await _drain_prefetch_task(pending_task)
    assert pending_task.cancelled()
