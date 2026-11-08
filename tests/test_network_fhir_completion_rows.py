# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Existing completion-v3 row semantics remain distinct from ordinary normalized exports."""

import hashlib
import json
from dataclasses import asdict, make_dataclass, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_retained_capture as retained
from process import network_fhir_source_witness_capture as capture
from process import network_fhir_source_witness_contract as contract
from process import network_fhir_typed_source as typed
from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256
from process.provider_directory_resource_hash import resource_content_hash_payload, resource_payload_sha256_for_contract
from tests.test_network_fhir_source_witness_contract import origin_fields


def _origin():
    origin_by_field = origin_fields() | {
        "admission_profile": contract.ADMITTED_TRANSPORT_PROFILE,
        "semantic_projection_as_of": None,
        "completion_required_version": 3,
        "completion_sha256": "8" * 64,
        "resource_counts": (("Location", 1),),
        "resource_hashes": (("Location", "e" * 64),),
        "descriptor_counts": (("Location", 1),),
    }
    return contract.source_origin(origin_by_field)


def _row(origin, **payload_fields):
    payload = {
        "resource_id": "site-one",
        "name": "Café",
        "resource_url": "https://directory.example.test/Location/site-one",
        **payload_fields,
    }
    return {
        "dataset_id": origin.dataset_id,
        "resource_type": "Location",
        "resource_id": "site-one",
        "payload_json": json.dumps(payload),
        "payload_hash": canonical_payload_sha256(resource_content_hash_payload(payload)),
        "acquired_resource_sha256": "a" * 64,
    }


def _roots():
    return {"Location": hashlib.sha256()}, {"Location": 0}, hashlib.sha256()


def _fold(origin, resource_by_field):
    hashes, counts, root = _roots()
    assert retained._fold_rows([resource_by_field], origin, hashes, counts, root, 0) == 1
    return hashes, counts, root


def _replay(origin):
    hashes, counts, root = _roots()
    return capture._SourceReplay(
        origin, "unused", "release-one", datetime(2026, 1, 1, tzinfo=UTC), hashes, counts, root, {}
    )


def test_completion_rows_preserve_subset_hash_and_replay_root():
    origin = _origin()
    resource_by_field = _row(origin, latitude=1.25, integer=1, negative_zero=-0.0)
    hashes, counts, root = _fold(origin, resource_by_field)
    assert resource_by_field["payload_hash"] != resource_payload_sha256_for_contract(
        json.loads(resource_by_field["payload_json"]), "transport_neutral_v2", resource_type="Location"
    )
    origin = replace(
        origin, dataset_sha256=root.hexdigest(), resource_hashes=(("Location", hashes["Location"].hexdigest()),)
    )
    retained._require_roots(origin, hashes, counts, root)
    replay = _replay(origin)
    capture._fold_source_page(replay, [resource_by_field])
    retained._require_roots(origin, replay.hashes_by_family, replay.counts_by_family, replay.root)
    wire, after = typed._page_rows((resource_by_field,), origin, {"Location": 0}, None)
    assert len(wire) == 1 and after == ("Location", "site-one")


def test_installed_source_completion_declaration_uses_the_same_closed_codec():
    origin_by_field = asdict(_origin())
    declaration = make_dataclass("CompletionDeclaration", [(name, object) for name in origin_by_field], frozen=True)
    declared = declaration(**origin_by_field)
    assert contract.is_completion_v3_origin(declared)
    _fold(declared, _row(declared))
    for change in ({"completion_required_version": 2}, {"admission_profile": contract.ADMITTED_UHC_PROFILE}):
        with pytest.raises(ValueError):
            contract.is_completion_v3_origin(declaration(**(origin_by_field | change)))


@pytest.mark.parametrize("marker", [None, "", "a" * 63, "g" * 64, "A" * 64, 1, True, []])
def test_completion_invalid_marker_refuses_capture_typed_intake_and_replay(marker):
    origin = _origin()
    resource_by_field = {**_row(origin), "acquired_resource_sha256": marker}
    for validate in (
        lambda: _fold(origin, resource_by_field),
        lambda: typed._page_rows((resource_by_field,), origin, {"Location": 0}, None),
        lambda: capture._fold_source_page(_replay(origin), [resource_by_field]),
    ):
        with pytest.raises(ValueError):
            validate()


@pytest.mark.parametrize("damage", ["hash", "foreign_source", "resource_id"])
def test_completion_hash_and_identity_failures_are_terminal(damage):
    origin = _origin()
    resource_by_field = _row(origin, **({"source_id": "foreign-source"} if damage == "foreign_source" else {}))
    if damage == "hash":
        resource_by_field["payload_hash"] = "0" * 64
    elif damage == "resource_id":
        resource_by_field["resource_id"] = "other-site"
    with pytest.raises(ValueError):
        _fold(origin, resource_by_field)
    with pytest.raises(ValueError):
        capture._fold_source_page(_replay(origin), [resource_by_field])


def test_noncompletion_transport_rows_remain_null_only_and_keep_old_hash():
    completed = _origin()
    origin_by_field = {
        key: value
        for key, value in completed.__dict__.items()
        if key not in {"completion_required_version", "completion_sha256"}
    }
    ordinary = contract.SourceWitnessOrigin(**origin_by_field)
    resource_by_field = _row(ordinary)
    with pytest.raises(ValueError):
        _fold(ordinary, resource_by_field)
    resource_by_field["acquired_resource_sha256"] = None
    with pytest.raises(ValueError):
        _fold(ordinary, resource_by_field)
    resource_by_field["payload_hash"] = resource_payload_sha256_for_contract(
        json.loads(resource_by_field["payload_json"]), "transport_neutral_v2", resource_type="Location"
    )
    _fold(ordinary, resource_by_field)
    assert not contract.is_completion_v3_origin(SimpleNamespace(**completed.__dict__))


@pytest.mark.asyncio
async def test_original_capture_intake_uses_the_same_completion_marker_gate(monkeypatch):
    origin = _origin()
    resource_by_field = _row(origin)
    hashes, counts, root = _fold(origin, resource_by_field)
    origin = replace(
        origin, dataset_sha256=root.hexdigest(), resource_hashes=(("Location", hashes["Location"].hexdigest()),)
    )
    monkeypatch.setattr(capture, "_virtual_xid", AsyncMock(return_value="1/2"))

    async def fold_batch(state, rows):
        return retained._fold_rows(
            rows, state.origin, state.hashes_by_family, state.counts_by_family, state.root, state.total
        )

    monkeypatch.setattr(capture, "_copy_batch", fold_batch)

    async def resources():
        yield (resource_by_field,)

    await capture._copy_resources(
        object(), "unused", origin, {"resources": resources}, "release-one", datetime(2026, 1, 1, tzinfo=UTC)
    )


@pytest.mark.parametrize(
    "payload_fields,expected_fallback", [({}, ()), ({"latitude": 1.25, "tiny": 1e-7, "negative_zero": -0.0}, (0,))]
)
@pytest.mark.parametrize("derive", [False, True])
def test_fresh_native_completion_hash_and_python_fallback_agree(payload_fields, expected_fallback, derive):
    pytest.importorskip("ptg2_address_canon")
    origin = _origin()
    resource_by_field = _row(origin, **payload_fields)
    prepared = retained._prepare_native_resources(
        [resource_by_field], origin, "release-one", datetime(2026, 1, 1, tzinfo=UTC), derive
    )
    assert prepared.row_count == 1 and prepared.fallback_rows == expected_fallback
    _, _, root = _fold(origin, resource_by_field)
    assert hashlib.sha256(prepared.root_bytes).hexdigest() == root.hexdigest()
    if derive:
        assert resource_by_field["payload_hash"].encode() in prepared.copy_batches[1][0][0]
    else:
        assert prepared.copy_batches == ((), (), ())


def test_native_context_does_not_enable_completion_for_ordinary_origins(monkeypatch):
    origin = _origin()
    captured_contexts = []

    def inspect(rows, context, **_options):
        captured_contexts.append(context)

    monkeypatch.setattr(retained, "prepare_retained_fhir_batch", inspect)
    retained._prepare_native_resources([_row(origin)], origin, "release-one", None, False)
    origin_by_field = asdict(origin)
    declaration = make_dataclass("CompletionDeclaration", [(name, object) for name in origin_by_field], frozen=True)
    declared = declaration(**origin_by_field)
    retained._prepare_native_resources([_row(declared)], declared, "release-one", None, False)
    origin_by_field = {
        key: value
        for key, value in origin.__dict__.items()
        if key not in {"completion_required_version", "completion_sha256"}
    }
    ordinary = contract.SourceWitnessOrigin(**origin_by_field)
    retained._prepare_native_resources([_row(ordinary)], ordinary, "release-one", None, False)
    assert captured_contexts[0]["completion_required_version"] == 3
    assert captured_contexts[1]["completion_required_version"] == 3
    assert "completion_required_version" not in captured_contexts[2]
