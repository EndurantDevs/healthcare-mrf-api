# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical generation receipts retain original provenance across explicit family transitions."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.test_reference_family_result_generation import _serving


def nucc_authority():
    return {
        "importer_id": "nucc",
        "local_lineage_id": str(UUID(int=1)),
        "local_generation": 2,
        "serving_generation": _serving(str(UUID(int=2)), 7),
        "relations": [{"schema_name": "published_reference", "relation_name": "nucc_taxonomy"}],
    }


def test_named_nucc_receipt_keeps_remote_origin_and_requires_complete_tracked_provenance():
    receipt = nucc_authority()
    original = deepcopy(receipt)
    accepted = generation._nucc_result_authority(receipt)
    assert accepted.serving_generation.origin_lineage_id == str(UUID(int=2))
    assert accepted.serving_generation.origin_generation == 7 and accepted.local_generation == 2
    assert accepted.relations == (("published_reference", "nucc_taxonomy"),)
    assert receipt == original
    receipt.update(local_generation=0, serving_generation=None, relations=None)
    assert generation._nucc_result_authority(receipt, allow_untracked=True).serving_generation is None
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="untracked"):
        generation._nucc_result_authority(receipt)


@pytest.mark.parametrize("change", ["origin", "missing_family", "foreign_family", "zero_local", "mixed_relations"])
def test_nucc_receipt_refuses_malformed_or_foreign_generation(change):
    receipt = nucc_authority()
    if change == "origin":
        receipt["serving_generation"]["origin_generation"] = True
    elif change == "missing_family":
        del receipt["importer_id"]
    elif change == "foreign_family":
        receipt["importer_id"] = "geo"
    elif change == "zero_local":
        receipt["local_generation"] = 0
    else:
        receipt["relations"].append({"schema_name": "other_reference", "relation_name": "nucc_taxonomy"})
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="predecessor differs"):
        generation._nucc_result_authority(receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["cms-doctors", "geo"])
async def test_ordinary_generation_publication_requires_each_family_atomic_writer(importer):
    with pytest.raises(RuntimeError, match="native_publication_required"):
        await generation.publish_local_reference_family_generation(object(), importer_id=importer, schema_name="mrf")


@pytest.mark.asyncio
async def test_doctors_without_published_predecessor_cannot_borrow_another_named_generation(monkeypatch):
    incumbent = SimpleNamespace(schema_name="published_doctors", relation_oids=(("doctors", None),))
    original = generation.ReferenceFamilyResultGenerationAuthority("cms-doctors", str(UUID(int=1)), 0, None, None)
    read = AsyncMock(return_value=original)
    monkeypatch.setattr(generation, "read_reference_family_result_generation_authority", read)
    manifest = SimpleNamespace(importer_id="cms-doctors")
    assert await generation._read_immutable_activation_predecessor("session", manifest, incumbent) is original
    read.assert_awaited_once_with("session", importer_id="cms-doctors", schema_name="published_doctors", lock=True)
    read.return_value = generation.ReferenceFamilyResultGenerationAuthority(
        "cms-doctors", str(UUID(int=1)), 1, None, (("other_doctors", "doctors"),)
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="predecessor differs"):
        await generation._read_immutable_activation_predecessor("session", manifest, incumbent)
    incumbent.relation_oids = (("doctors", None), ("organizations", 1))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="predecessor is incomplete"):
        await generation._read_immutable_activation_predecessor("session", manifest, incumbent)


def test_historical_nucc_receipt_requires_serving_name_for_republication():
    receipt = nucc_authority()
    receipt.pop("relations")
    receipt["relation_oids"] = [17]
    original = deepcopy(receipt)
    previous = generation._nucc_result_authority(receipt, legacy_read=True)
    assert previous.serving_generation.origin_generation == 7 and previous.local_generation == 2
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="recorded serving name is required"):
        generation._nucc_result_authority(receipt)
    named = generation._nucc_result_authority(receipt, declared_schema="restored_reference")
    assert named.relations == (("restored_reference", "nucc_taxonomy"),)
    assert named.serving_generation == previous.serving_generation
    assert receipt == original


@pytest.mark.parametrize(
    "field,value", [("local_generation", True), ("local_lineage_id", "invalid"), ("importer_id", "geo")]
)
def test_historical_nucc_receipt_rejects_invalid_logical_provenance(field, value):
    receipt = nucc_authority()
    receipt.pop("relations")
    receipt.update(relation_oids=[17], **{field: value})
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="predecessor differs"):
        generation._nucc_result_authority(receipt, legacy_read=True)
