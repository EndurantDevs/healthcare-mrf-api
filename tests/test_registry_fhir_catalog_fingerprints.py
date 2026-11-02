# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Approved metadata may change only when membership-affecting commitments agree."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_fingerprints as fingerprints


def _family_rows(revision, *, metadata="a", membership="b"):
    return [
        {"approved_revision": revision, "family": "metadata", "bounded": True, "sha256": metadata * 64},
        {"approved_revision": revision, "family": "membership", "bounded": True, "sha256": membership * 64},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("active,revisions", [(False, (1,)), (True, ()), (True, (1, 2, 3))])
async def test_fingerprint_read_requires_transaction_and_at_most_two_requested_revisions(active, revisions):
    connection = SimpleNamespace(is_in_transaction=Mock(return_value=active), fetch=AsyncMock())
    with pytest.raises(ValueError, match="registry_fhir_catalog_fingerprint_transaction_required"):
        await fingerprints.approved_fhir_catalog_fingerprints(connection, revisions, control_schema="registry_example")
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", [True, -1, 2**63])
async def test_approved_revision_pins_are_nonnegative_bounded_integers(revision):
    connection = SimpleNamespace(is_in_transaction=Mock(return_value=True), fetch=AsyncMock())
    with pytest.raises(ValueError, match="registry_fhir_catalog_fingerprint_revision_invalid"):
        await fingerprints.approved_fhir_catalog_fingerprints(
            connection, (revision,), control_schema="registry_example"
        )
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_revision_is_aggregated_once_and_keeps_both_map_families():
    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True), fetch=AsyncMock(return_value=_family_rows(6))
    )
    assert await fingerprints.approved_fhir_catalog_fingerprints(
        connection, (6, 6), control_schema="registry_example"
    ) == {6: {"metadata": "a" * 64, "membership": "b" * 64}}
    connection.fetch.assert_awaited_once()
    assert connection.fetch.await_args.args[1:] == (
        [6],
        fingerprints.METADATA_KINDS,
        fingerprints.MEMBERSHIP_KINDS,
        fingerprints._MAX_BYTES,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_family", "unbounded", "missing_digest"])
async def test_incomplete_or_unbounded_aggregate_cannot_become_a_fingerprint(damage):
    rows = _family_rows(6)
    if damage == "missing_family":
        rows.pop()
    elif damage == "unbounded":
        rows[0]["bounded"] = False
    else:
        rows[0]["sha256"] = None
    connection = SimpleNamespace(is_in_transaction=Mock(return_value=True), fetch=AsyncMock(return_value=rows))
    with pytest.raises(ValueError, match="registry_fhir_catalog_fingerprint_bounds"):
        await fingerprints.approved_fhir_catalog_fingerprints(connection, (6,), control_schema="registry_example")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "membership_records", "membership_pin", "metadata_pin"])
async def test_membership_reuse_requires_old_current_and_snapshot_commitments_to_agree(change):
    current_rows = _family_rows(6, membership="f" if change == "membership_records" else "b")
    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True),
        fetch=AsyncMock(return_value=_family_rows(5, metadata="c") + current_rows),
    )
    selected_by_field = {
        "metadata_approved_revision": 6,
        "membership_mapping_sha256": ("f" if change == "membership_pin" else "b") * 64,
        "mapping_sha256": ("f" if change == "metadata_pin" else "a") * 64,
    }
    operation = fingerprints.require_fhir_catalog_membership_reuse(
        connection, SimpleNamespace(approved_custom_revision=5), selected_by_field, control_schema="registry_example"
    )
    if change is None:
        await operation
    else:
        with pytest.raises(ValueError, match="registry_fhir_catalog_.*mapping_changed"):
            await operation
    connection.fetch.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "active,serving,approved",
    [(False, 6, 7), (True, True, 7), (True, 6, True), (True, -1, 7), (True, 7, 7), (True, 8, 7), (True, 6, 2**63)],
)
async def test_pending_mapping_proof_requires_transaction_and_ordered_integer_revisions(active, serving, approved):
    connection = SimpleNamespace(is_in_transaction=Mock(return_value=active), fetchval=AsyncMock())
    with pytest.raises(ValueError, match="membership_mapping_changed"):
        await fingerprints.require_fhir_catalog_pending_mappings(
            connection, serving, approved, control_schema="registry_example"
        )
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("compatible", [True, False, None, 1])
async def test_split_selection_requires_positive_add_only_proof(compatible):
    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True),
        fetch=AsyncMock(return_value=_family_rows(6) + _family_rows(7, metadata="c", membership="d")),
        fetchval=AsyncMock(return_value=compatible),
    )
    operation = fingerprints.select_fhir_catalog_mapping(
        connection, 7, SimpleNamespace(approved_custom_revision=6), control_schema="registry_example"
    )
    if compatible is True:
        assert await operation == (6, {"metadata": "a" * 64, "membership": "b" * 64})
    else:
        with pytest.raises(ValueError, match="membership_mapping_changed"):
            await operation
    assert connection.fetchval.await_args.args[1:] == (
        [6, 7],
        sorted(set(fingerprints.METADATA_KINDS + fingerprints.MEMBERSHIP_KINDS)),
        fingerprints._MAX_BYTES,
        "fhir-catalog/v1",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("attached", [False, True])
async def test_unchanged_membership_keeps_current_metadata_mapping(attached):
    rows = _family_rows(7, metadata="c")
    manifest = SimpleNamespace(approved_custom_revision=6) if attached else None
    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True),
        fetch=AsyncMock(return_value=(_family_rows(6) if attached else []) + rows),
        fetchval=AsyncMock(),
    )
    assert await fingerprints.select_fhir_catalog_mapping(
        connection, 7, manifest, control_schema="registry_example"
    ) == (7, {"metadata": "c" * 64, "membership": "b" * 64})
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "unattached", "manifest_hash", "manifest_revision", "pending_proof"])
async def test_mapping_revision_authenticates_pinned_manifest_independently(monkeypatch, damage):
    snapshot_by_field = {"metadata_approved_revision": 6, "network_generation": 9, "manifest_sha256": "f" * 64}
    if damage == "unattached":
        snapshot_by_field["network_generation"] = None
    resolver = AsyncMock(
        return_value=SimpleNamespace(
            approved_custom_revision=5 if damage == "manifest_revision" else 6,
            manifest_sha256=("e" if damage == "manifest_hash" else "f") * 64,
        )
    )
    monkeypatch.setattr(fingerprints, "resolve_network_serving_manifest", resolver)
    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True), fetchval=AsyncMock(return_value=damage != "pending_proof")
    )
    operation = fingerprints.require_fhir_catalog_mapping_revision(
        connection, snapshot_by_field, 7, control_schema="registry_example"
    )
    if damage is None:
        await operation
    else:
        with pytest.raises(ValueError, match="mapping_(revision_)?changed"):
            await operation
    if damage == "unattached":
        resolver.assert_not_awaited()
    else:
        resolver.assert_awaited_once_with(connection, generation_id=9, control_schema="registry_example")
    if damage in {"unattached", "manifest_hash", "manifest_revision"}:
        connection.fetchval.assert_not_awaited()
    else:
        assert connection.fetchval.await_args.args[1] == [6, 7]
