# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit bootstrap descriptors preserve generation and closed dependency identity."""

import copy
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_address_artifact as artifact
from process import network_bootstrap_address_artifact as bootstrap_artifact
from process import network_bootstrap_sources as bootstrap
from process import network_compact_custom_preparation as custom
from process import network_compact_retention as retention
from process.network_address_projection import PinnedAddressSource


def _receipt():
    receipt = bootstrap.NetworkBootstrapSourceReceipt(
        bootstrap.NetworkBootstrapSourceSpecification(str(UUID(int=1)), "parent", "parent", 10, 11),
        "owner",
        ("reader",),
        tuple(
            bootstrap.BootstrapParentReceipt("parent", name, 9, oid, 8, oid + 2, "{}", "a" * 64, 1, "b" * 64)
            for name, oid in (("entity_address_unified", 10), ("npi", 11))
        ),
        "1:2:",
        "2026-01-01T00:00:00Z",
        20,
        21,
        (("entity_address_unified", 22), ("npi", 23)),
        (("address_primary", 24), ("npi_primary", 25)),
        "",
    )
    return replace(receipt, generation_sha256=bootstrap._generation(receipt))


def _descriptor(*, sequence_oid=None):
    receipt = _receipt()
    descriptor_by_field = {
        "contract": bootstrap_artifact.CONTRACT,
        "database": {"oid": 1, "name": "example"},
        "namespace": {"name": receipt.address_source.schema_name, "oid": 20, "owner_oid": 21},
        "protected_owner_oid": 21,
        "table": {"oid": 22, "relfilenode": 32},
        "heaps": {
            "entity_address_unified": {"oid": 22, "relfilenode": 32},
            "npi": {"oid": 23, "relfilenode": 33},
        },
        "bootstrap_receipt": receipt.as_dict(),
        "generation_id": receipt.generation_sha256,
        "native_sha256": "c" * 64,
        "dormant_sequence_oid": sequence_oid,
        "dependency_oids": sorted([22, 23, 24, 25] + ([] if sequence_oid is None else [sequence_oid])),
        "runtime_roles": ["reader", "worker"],
    }
    descriptor_by_field = json.loads(artifact._json(descriptor_by_field))
    descriptor_by_field["descriptor_sha256"] = artifact._digest(descriptor_by_field)
    return descriptor_by_field


@pytest.mark.parametrize("sequence_oid", [None, 26])
def test_bootstrap_contract_preserves_receipt_generation_and_both_heap_dependencies(sequence_oid):
    descriptor = _descriptor(sequence_oid=sequence_oid)
    assert artifact._descriptor(descriptor) == descriptor
    assert descriptor["generation_id"] == _receipt().generation_sha256
    assert descriptor["generation_id"] != artifact._digest(descriptor["bootstrap_receipt"])
    assert not {"authority", "authority_table_oid", "alias_generation", "roles_sha256"} & descriptor.keys()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("generation_id", "d" * 64),
        ("heaps", {"entity_address_unified": {"oid": 22, "relfilenode": 32}}),
        ("dependency_oids", [22, 24, 25]),
        ("dependency_oids", [22, 23, 24, 25, 90]),
        ("dependency_oids", [22.0, 23.0, 24.0, 25.0]),
        ("dependency_oids", (22, 23, 24, 25)),
        ("dormant_sequence_oid", 22),
        ("protected_owner_oid", 99),
        ("protected_owner_oid", 21.0),
        ("namespace", {**_descriptor()["namespace"], "oid": 20.0}),
        ("namespace", {**_descriptor()["namespace"], "owner_oid": 21.0}),
        ("namespace", []),
        ("table", {"oid": 22.0, "relfilenode": 32}),
        ("table", {"oid": 22, "relfilenode": 32.0}),
        ("table", []),
        ("runtime_roles", ["reader", "reader"]),
        ("native_sha256", "not-a-digest"),
    ],
)
def test_rehashed_descriptor_cannot_invent_bootstrap_identity(field, value):
    descriptor = _descriptor()
    descriptor[field] = value
    descriptor["descriptor_sha256"] = artifact._digest(
        {key: value for key, value in descriptor.items() if key != "descriptor_sha256"}
    )
    with pytest.raises(artifact.NetworkAddressArtifactError):
        artifact._descriptor(descriptor)


async def test_capture_dispatch_preserves_bootstrap_refusal_without_native_fallback(monkeypatch):
    capture = AsyncMock(side_effect=bootstrap.NetworkBootstrapSourceError("invalid retained receipt"))
    monkeypatch.setattr(bootstrap_artifact, "capture_bootstrap_address_artifact", capture)
    with pytest.raises(bootstrap.NetworkBootstrapSourceError, match="invalid retained receipt"):
        await artifact.capture_network_address_artifact(object(), _receipt().address_source, runtime_roles=("reader",))
    capture.assert_awaited_once()


async def test_unknown_descriptor_contract_cannot_select_bootstrap_verifier(monkeypatch):
    descriptor = _descriptor()
    descriptor["contract"] += ".unknown"
    verify = AsyncMock()
    monkeypatch.setattr(bootstrap_artifact, "verify_bootstrap_address_artifact", verify)
    with pytest.raises(artifact.NetworkAddressArtifactError, match="descriptor_invalid"):
        await artifact.verify_network_address_artifact(object(), descriptor, runtime_roles=("reader",))
    verify.assert_not_awaited()


@pytest.mark.parametrize("sequence_oid", [None, 26])
def test_compact_retention_keeps_both_bootstrap_heaps_indexes_and_existing_dependencies(monkeypatch, sequence_oid):
    descriptor = _descriptor(sequence_oid=sequence_oid)
    source_pin = PinnedAddressSource(
        descriptor["namespace"]["name"], "entity_address_unified", descriptor["generation_id"]
    )
    candidate_by_field = {"state": "published", "validation_json": {"address_source": asdict(source_pin)}}
    retained_id = str(UUID(int=2))
    serving_by_field = {
        "address_artifact": descriptor,
        "edge_artifacts": [
            {
                "candidate_id": retained_id,
                "schema_name": "network_candidate_" + UUID(retained_id).hex,
                "table_oid": 48,
                "seal_sha256": "a" * 64,
            }
        ],
    }
    monkeypatch.setattr(retention, "_scope_report", lambda *_: {})
    monkeypatch.setattr(
        retention, "compact_readiness", lambda *_: ({"edge_heap_oid": 40, "ref_map_oid": 41}, serving_by_field)
    )
    monkeypatch.setattr(
        custom, "compact_carrier_receipt", lambda *_: SimpleNamespace(origin_references=[(0, 0, 0, 46, 47)])
    )
    receipt_by_field = {"closure": {"relation_oids": {"network_office_edge": 40, "network_slice_ref": 41, "other": 42}}}
    before = copy.deepcopy((candidate_by_field, receipt_by_field))
    assert retention._dependencies(candidate_by_field, receipt_by_field) == set(descriptor["dependency_oids"]) | {
        40,
        41,
        42,
        46,
        47,
        48,
    }
    assert (candidate_by_field, receipt_by_field) == before
