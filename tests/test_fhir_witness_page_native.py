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
