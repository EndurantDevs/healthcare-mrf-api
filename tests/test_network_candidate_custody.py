# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical candidate headers, source events and bounded canonical metadata."""

import asyncio
import copy
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_candidate_custody as custody
from process.network_membership_copy import MembershipCopyTarget

TARGET = MembershipCopyTarget(*(str(UUID(int=index)) for index in range(1, 5)), "network_candidate_" + UUID(int=4).hex)


def _candidate_record():
    """Build only the logical candidate fields consumed by the data helpers."""
    return {
        **{field: getattr(TARGET, field) for field in custody._STABLE[:5]},
        "schema_revision": 1,
        "source_generations": {"custom_membership": "a" * 64},
        "approved_custom_revision": 1,
        "expected_head": 0,
        "expected_rows": 2,
        "accepted_rows": 2,
        "index_ready": True,
        "source_recipes_json": [],
        "validation_json": {"component": "logical-validation"},
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "state": "ready",
    }


def _event_assertion(kind):
    checkpoint_by_name = {"event_id": str(UUID(int=7)), "attempt_id": str(UUID(int=8))}
    if kind == "cms":
        coordinates_by_name = dict(
            source_system="fhir",
            source_id="cms-npd",
            dataset_schema="source_schema",
            dataset_id="dataset",
            producer_id="producer",
            edition_id="edition",
        )
        checkpoint_by_name.update(
            notification_sha256="a" * 64,
            pair_sha256="b" * 64,
            recipe_sha256="c" * 64,
            coordinates=coordinates_by_name,
            replaced_coordinates=coordinates_by_name,
        )
    elif kind == "aca":
        checkpoint_by_name.update(
            header={"capture_id": str(UUID(int=9)), "recipe_sha256": "a" * 64, "notification_sha256": "b" * 64},
            reader_identity_sha256="c" * 64,
        )
    else:
        checkpoint_by_name.update(
            header={"capture_id": str(UUID(int=9)), "approval_sha256": "a" * 64, "hold_sha256": "b" * 64},
            reader_identity_sha256="c" * 64,
        )
    return {"revision": 1, "kind": kind, "checkpoint": checkpoint_by_name}


@pytest.mark.asyncio
@pytest.mark.parametrize("lock", [False, True])
async def test_candidate_header_reads_exact_target_in_declared_namespace(lock):
    candidate = _candidate_record()
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=candidate))
    assert await custody._candidate(connection, TARGET, "control", lock=lock) == candidate
    query, candidate_id = connection.fetchrow.await_args.args
    assert 'FROM "control".network_membership_candidate' in query
    assert candidate_id == UUID(TARGET.candidate_id)
    assert query.endswith(" FOR UPDATE") is lock


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"])
async def test_candidate_header_refuses_mismatched_logical_scope(field):
    candidate = _candidate_record()
    candidate[field] = "other" if field == "schema_name" else str(UUID(int=99))
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=candidate))
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody._candidate(connection, TARGET, "control")


@pytest.mark.asyncio
async def test_candidate_header_refuses_missing_row():
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody._candidate(connection, TARGET, "control")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [PermissionError("denied"), asyncio.CancelledError()])
async def test_candidate_header_preserves_access_and_cancellation_errors(failure):
    connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)) as raised:
        await custody._candidate(connection, TARGET, "control")
    assert raised.value is failure
    connection.fetchrow.assert_awaited_once()


@pytest.mark.parametrize("kind", ["cms", "ptg", "aca"])
def test_source_event_assertion_preserves_closed_logical_checkpoint(kind):
    assertion = _event_assertion(kind)
    report_by_name = {custody.SOURCE_EVENT_ASSERTION: assertion}
    key, checkpoint = custody._source_event_assertion(report_by_name)
    assert key == custody._SOURCE_CHECKPOINTS[kind]
    assert checkpoint == assertion["checkpoint"]


@pytest.mark.parametrize(
    "damage",
    ["revision", "bool_revision", "kind", "extra", "checkpoint_type", "checkpoint_extra", "uuid", "sha", "coordinates"],
)
def test_source_event_assertion_rejects_changed_or_unknown_fields(damage):
    assertion = _event_assertion("cms")
    if damage in {"revision", "bool_revision"}:
        assertion["revision"] = True if damage == "bool_revision" else 2
    elif damage in {"kind", "extra"}:
        assertion[damage] = "other"
    elif damage == "checkpoint_type":
        assertion["checkpoint"] = []
    else:
        checkpoint = assertion["checkpoint"]
        changes_by_name = {
            "checkpoint_extra": ("extra", True),
            "uuid": ("attempt_id", "invalid"),
            "sha": ("notification_sha256", "invalid"),
        }
        if damage == "coordinates":
            checkpoint["coordinates"]["extra"] = True
        else:
            field, value = changes_by_name[damage]
            checkpoint[field] = value
    with pytest.raises(custody.NetworkCandidateCustodyError):
        custody._source_event_assertion({custody.SOURCE_EVENT_ASSERTION: assertion})


@pytest.mark.parametrize("kind", ["ptg", "aca"])
@pytest.mark.parametrize("damage", ["missing", "extra", "digest", "capture_id", "other_digest"])
def test_source_event_assertion_rejects_malformed_native_header(kind, damage):
    assertion = _event_assertion(kind)
    header = assertion["checkpoint"]["header"]
    digest_field = "hold_sha256" if kind == "ptg" else "recipe_sha256"
    if damage == "missing":
        del header[digest_field]
    elif damage == "extra":
        header["extra"] = True
    elif damage == "other_digest":
        header["approval_sha256" if kind == "ptg" else "notification_sha256"] = "invalid"
    else:
        header["capture_id" if damage == "capture_id" else digest_field] = "invalid"
    with pytest.raises(custody.NetworkCandidateCustodyError):
        custody._source_event_assertion({custody.SOURCE_EVENT_ASSERTION: assertion})


def test_initial_cms_event_keeps_exact_mode_and_empty_base():
    assertion = _event_assertion("cms")
    assertion["checkpoint"].update(replaced_coordinates=None, mode="initial", base_manifest=None)
    assert custody._source_event_assertion({custody.SOURCE_EVENT_ASSERTION: assertion}) == (
        "cms_source_notification",
        assertion["checkpoint"],
    )
    for field, changed in (("mode", "replace"), ("base_manifest", "unexpected")):
        invalid = copy.deepcopy(assertion)
        invalid["checkpoint"][field] = changed
        with pytest.raises(custody.NetworkCandidateCustodyError):
            custody._source_event_assertion({custody.SOURCE_EVENT_ASSERTION: invalid})


@pytest.mark.parametrize("kind", ["cms", "ptg", "aca"])
def test_exact_atomic_checkpoint_keeps_canonical_candidate_data(kind):
    candidate = _candidate_record()
    assertion = _event_assertion(kind)
    report = candidate["validation_json"]
    report[custody.SOURCE_EVENT_ASSERTION] = assertion
    before = custody._evidence(candidate, revision=2)
    report[custody._SOURCE_CHECKPOINTS[kind]] = copy.deepcopy(assertion["checkpoint"])
    assert custody._evidence(candidate, revision=2) == before
    candidate["state"] = "published"
    assert custody._evidence(candidate, revision=2) == before
    del report[custody._SOURCE_CHECKPOINTS[kind]]
    with pytest.raises(custody.NetworkCandidateCustodyError):
        custody._evidence(candidate, revision=2)


@pytest.mark.parametrize("kind", ["cms", "ptg", "aca"])
@pytest.mark.parametrize("damage", ["checkpoint", "other_checkpoint", "missing_assertion", "bool"])
def test_checkpoint_normalization_refuses_nonidentical_or_foreign_data(kind, damage):
    candidate = _candidate_record()
    assertion = _event_assertion(kind)
    report = candidate["validation_json"]
    report[custody.SOURCE_EVENT_ASSERTION] = assertion
    checkpoint = copy.deepcopy(assertion["checkpoint"])
    if damage == "checkpoint":
        checkpoint["attempt_id"] = str(UUID(int=99))
    elif damage == "bool":
        checkpoint["attempt_id"] = True
    key = (
        custody._SOURCE_CHECKPOINTS["ptg" if kind == "cms" else "cms"]
        if damage == "other_checkpoint"
        else custody._SOURCE_CHECKPOINTS[kind]
    )
    report[key] = checkpoint
    if damage == "missing_assertion":
        del report[custody.SOURCE_EVENT_ASSERTION]
    with pytest.raises(custody.NetworkCandidateCustodyError):
        custody._evidence(candidate, revision=2)


@pytest.mark.parametrize("field", ["producer_id", "validation_json", "source_generations", "created_at"])
def test_canonical_candidate_data_retains_every_changed_value(field):
    candidate = _candidate_record()
    before = custody._encoded(custody._evidence(candidate))
    changes_by_field = {
        "producer_id": str(UUID(int=77)),
        "validation_json": {"extra": True},
        "source_generations": {"custom_membership": "c" * 64},
        "created_at": datetime(2026, 2, 1, tzinfo=timezone.utc),
    }
    candidate[field] = changes_by_field[field]
    assert custody._encoded(custody._evidence(candidate)) != before


@pytest.mark.parametrize("kind", ["cms", "ptg", "aca"])
@pytest.mark.parametrize("damage", ["assertion", "extra_json"])
def test_source_assertion_changes_remain_in_canonical_data(kind, damage):
    candidate = _candidate_record()
    report = candidate["validation_json"]
    report[custody.SOURCE_EVENT_ASSERTION] = _event_assertion(kind)
    before = custody._encoded(custody._evidence(candidate, revision=2))
    if damage == "assertion":
        report[custody.SOURCE_EVENT_ASSERTION]["checkpoint"]["event_id"] = str(UUID(int=99))
    else:
        report["unrelated"] = True
    assert custody._encoded(custody._evidence(candidate, revision=2)) != before


def test_original_canonical_revision_keeps_all_checkpoint_bytes():
    candidate = _candidate_record()
    before = custody._encoded(custody._evidence(candidate))
    assertion = _event_assertion("cms")
    candidate["validation_json"].update(
        {custody.SOURCE_EVENT_ASSERTION: assertion, "cms_source_notification": assertion["checkpoint"]}
    )
    assert custody._encoded(custody._evidence(candidate)) != before


@pytest.mark.parametrize("field", ["source_generations", "source_recipes_json", "validation_json"])
def test_canonical_candidate_decodes_driver_json_without_changing_data(field):
    candidate = _candidate_record()
    before = custody._evidence(candidate)
    candidate[field] = json.dumps(candidate[field])
    assert custody._evidence(candidate) == before


def test_candidate_metadata_enforces_the_existing_complete_byte_bound():
    candidate = _candidate_record()
    candidate["validation_json"] = {"component": ""}
    remaining = custody.MAX_RECEIPT_BYTES - len(custody._encoded(custody._evidence(candidate)))
    candidate["validation_json"]["component"] = "x" * remaining
    assert len(custody._encoded(custody._evidence(candidate))) == custody.MAX_RECEIPT_BYTES
    candidate["validation_json"]["component"] += "x"
    with pytest.raises(custody.NetworkCandidateCustodyError):
        custody._evidence(candidate)


@pytest.mark.parametrize("change", ["missing", "extra", "revision_bool", "revision_unknown"])
def test_candidate_canonical_shape_and_revision_are_closed(change):
    candidate = _candidate_record()
    revision = 1
    if change == "missing":
        del candidate["dataset_id"]
    elif change == "extra":
        candidate["extra"] = True
    else:
        revision = True if change == "revision_bool" else 6
    with pytest.raises(custody.NetworkCandidateCustodyError):
        custody._evidence(candidate, revision=revision)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "names", [(), ("unknown",), ("network_serving_manifest", "network_serving_manifest"), ["network_serving_manifest"]]
)
async def test_control_name_reader_refuses_invalid_selection_before_sql(names):
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(), fetch=AsyncMock())
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody.read_network_control_relation_pins(connection, names, control_schema="control")
    connection.fetchrow.assert_not_awaited()
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [("read committed", "on"), ("repeatable read", "off")])
async def test_control_name_reader_requires_pinned_readonly_transaction(mode):
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=mode), fetch=AsyncMock()
    )
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody.read_network_control_relation_pins(
            connection, ("network_serving_manifest",), control_schema="control"
        )
    connection.fetch.assert_not_awaited()
