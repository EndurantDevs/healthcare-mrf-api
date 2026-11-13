# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit bootstrap descriptors preserve generation and closed dependency identity."""

import json
from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_address_artifact as artifact
from process import network_bootstrap_address_artifact as bootstrap_artifact
from process import network_bootstrap_sources as bootstrap


def _receipt():
    receipt = bootstrap.NetworkBootstrapSourceReceipt(
        bootstrap.NetworkBootstrapSourceSpecification(str(UUID(int=1)), "parent", "parent"),
        tuple(
            bootstrap.BootstrapParentReceipt("parent", name, 1, "b" * 64) for name in ("entity_address_unified", "npi")
        ),
        "2026-01-01T00:00:00Z",
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


def _named_descriptor():
    descriptor = bootstrap_artifact._descriptor(_descriptor())
    descriptor["revision"] = 2
    descriptor["descriptor_sha256"] = artifact._digest(
        {key: value for key, value in descriptor.items() if key != "descriptor_sha256"}
    )
    return descriptor


@pytest.mark.parametrize("sequence_oid", [None, 26])
def test_legacy_bootstrap_projects_names_and_preserves_logical_content_keys(sequence_oid):
    saved = _descriptor(sequence_oid=sequence_oid)
    descriptor = artifact._descriptor(saved)
    assert descriptor["generation_id"] == saved["generation_id"]
    assert descriptor["descriptor_sha256"] == saved["descriptor_sha256"]
    assert descriptor["heaps"] == {
        name: {"schema_name": saved["namespace"]["name"], "table_name": name}
        for name in ("entity_address_unified", "npi")
    }
    assert descriptor["bootstrap_receipt"]["parent_receipts"] == [
        {"schema_name": "parent", "table_name": name, "row_count": 1, "rows_sha256": "b" * 64}
        for name in ("entity_address_unified", "npi")
    ]
    assert artifact._descriptor(descriptor) == descriptor


@pytest.mark.parametrize(
    "field,value",
    [
        ("generation_id", "d" * 64),
        ("heaps", {}),
        ("namespace", {"name": "foreign"}),
        ("revision", True),
        ("unexpected", True),
    ],
)
def test_named_bootstrap_rejects_changed_scope_or_open_shape(field, value):
    descriptor = _named_descriptor()
    descriptor[field] = value
    descriptor["descriptor_sha256"] = artifact._digest(
        {key: value for key, value in descriptor.items() if key != "descriptor_sha256"}
    )
    with pytest.raises((ValueError, TypeError)):
        artifact._descriptor(descriptor)


@pytest.mark.parametrize("field,value", [("row_count", -1), ("rows_sha256", "invalid"), ("table_name", "other")])
def test_bootstrap_preserves_parent_data_validation(field, value):
    descriptor = _named_descriptor()
    descriptor["bootstrap_receipt"]["parent_receipts"][0][field] = value
    descriptor["descriptor_sha256"] = artifact._digest(
        {key: value for key, value in descriptor.items() if key != "descriptor_sha256"}
    )
    with pytest.raises(ValueError):
        artifact._descriptor(descriptor)


async def test_capture_dispatch_preserves_bootstrap_data_error(monkeypatch):
    capture = AsyncMock(side_effect=bootstrap.NetworkBootstrapSourceError("invalid retained receipt"))
    monkeypatch.setattr(bootstrap_artifact, "capture_bootstrap_address_artifact", capture)
    with pytest.raises(bootstrap.NetworkBootstrapSourceError, match="invalid retained receipt"):
        await artifact.capture_network_address_artifact(object(), _receipt().address_source)
    capture.assert_awaited_once()


def _legacy_receipt_document():
    """An exact old encoding keeps its stored edition while obsolete fields are discarded."""
    receipt = _receipt()
    parents = [
        {
            **parent,
            **dict.fromkeys(
                ("schema_oid", "table_oid", "owner_oid", "authority_table_oid", "authority_json", "shape_sha256")
            ),
        }
        for parent in receipt.as_dict()["parent_receipts"]
    ]
    return {
        "component": "network_bootstrap_sources",
        "revision": 1,
        "specification": {
            "bootstrap_id": receipt.specification.bootstrap_id,
            "address_schema": "parent",
            "npi_schema": "parent",
            "address_table_oid": None,
            "npi_table_oid": None,
        },
        "parent_receipts": parents,
        "captured_at": receipt.captured_at,
        "generation_sha256": "c" * 64,
        **dict.fromkeys(
            (
                "owner_role",
                "runtime_roles",
                "capture_snapshot",
                "schema_oid",
                "owner_oid",
                "relation_oids",
                "index_oids",
            )
        ),
    }


def test_legacy_comment_projects_to_named_artifact_without_rewriting_its_edition():
    stored = json.dumps(_legacy_receipt_document())
    decoded = bootstrap._decode(stored)
    assert decoded.generation_sha256 == "c" * 64 != bootstrap._generation(decoded)
    projected = decoded.as_dict()
    assert projected == bootstrap_artifact._receipt(json.loads(stored))
    descriptor = _named_descriptor()
    descriptor["bootstrap_receipt"] = projected
    descriptor["generation_id"] = decoded.generation_sha256
    descriptor["descriptor_sha256"] = artifact._digest(
        {name: value for name, value in descriptor.items() if name != "descriptor_sha256"}
    )
    assert artifact._descriptor(descriptor) == descriptor
    assert bootstrap._decode(stored) == decoded
    # Projection is artifact metadata; the retained schema comment stays in its original encoding.
    with pytest.raises(bootstrap.NetworkBootstrapSourceError):
        bootstrap._decode(json.dumps(projected))


@pytest.mark.parametrize(
    "field,value",
    [
        ("generation_sha256", "c" * 64),
        ("captured_at", "different"),
        ("revision", True),
        ("bootstrap_id", str(UUID(int=2))),
    ],
)
def test_new_bootstrap_comment_requires_its_exact_logical_data_digest(field, value):
    document = _receipt().as_dict()
    document[field] = value
    with pytest.raises(bootstrap.NetworkBootstrapSourceError):
        bootstrap._decode(json.dumps(document))


def _mutate_parent_receipt(parent_by_field, mutation):
    if mutation == "generation":
        parent_by_field["local_generation"] = 1
        return
    if mutation == "boolean_generation":
        parent_by_field["local_generation"] = False
        return
    if mutation == "origin":
        parent_by_field["origin_generation"] = 1
        return
    if mutation == "canonical":
        parent_by_field["canonical_publication_ref"] = "published"
        return
    if mutation == "missing":
        del parent_by_field["origin_generation"]
        return
    if mutation == "lineage":
        parent_by_field["local_lineage_id"] = str(UUID(int=0))
        return
    if mutation == "singleton":
        parent_by_field["singleton"] = False
        return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", [None, "generation", "boolean_generation", "origin", "canonical", "missing", "lineage", "singleton"]
)
async def test_bootstrap_parent_admission_uses_logical_generation_before_content(monkeypatch, mutation):
    from types import SimpleNamespace

    parent_by_field = {
        "singleton": True,
        "local_lineage_id": str(UUID(int=1)),
        "local_generation": 0,
        "origin_lineage_id": None,
        "origin_generation": None,
        "published_at": None,
        "canonical_publication_ref": None,
        "canonical_publication_generation": None,
        "canonical_chain_ref": None,
        "canonical_import_date": None,
    }
    _mutate_parent_receipt(parent_by_field, mutation)
    connection = SimpleNamespace(execute=AsyncMock(), fetch=AsyncMock(return_value=[parent_by_field]))
    monkeypatch.setattr(bootstrap, "_parent_shape", AsyncMock())
    content = AsyncMock(return_value=(3, "b" * 64))
    monkeypatch.setattr(bootstrap, "_content", content)
    if mutation is None:
        assert await bootstrap._parent(connection, "parent", "npi") == bootstrap.BootstrapParentReceipt(
            "parent", "npi", 3, "b" * 64
        )
        content.assert_awaited_once_with(connection, "parent", "npi")
    else:
        with pytest.raises(bootstrap.NetworkBootstrapSourceError):
            await bootstrap._parent(connection, "parent", "npi")
        content.assert_not_awaited()
    connection.execute.assert_awaited_once_with(
        'LOCK TABLE "parent"."npi","parent"."npi_result_generation" IN ACCESS SHARE MODE NOWAIT'
    )
