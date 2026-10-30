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
