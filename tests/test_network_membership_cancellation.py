# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cancellation keeps bounded native work inside the caller's ownership scope."""

import asyncio
import hashlib
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import network_approved_membership_source as approved
from process import network_membership_copy as membership
from process import network_source_binding_store as source_bindings
from process import registry_ptg_office_capture as capture
from process import registry_ptg_office_witness as witness
from process import registry_published_plan_binding_refs as binding_references
from process import registry_record_store as record_store
from process import registry_required_target_review_store as target_reviews
from process import registry_required_target_store as required_targets
from process import registry_source_admission as source_admission
from tests.registry_source_fixtures import _edition as mlr_edition


def _native_gate(native_failure):
    gate = SimpleNamespace(entered=threading.Event(), release=threading.Event(), finished=threading.Event())

    def encode(*_input):
        gate.entered.set()
        try:
            assert gate.release.wait(5), "native fault gate was not released"
            if native_failure:
                raise ValueError("native batch refused")
            return membership._COPY_HEADER + b"\xff\xff", 0
        finally:
            gate.finished.set()

    gate.encode = encode
    return gate


def _copy_context():
    ownership_events = []

    class Transaction:
        async def __aenter__(self):
            ownership_events.append("acquired")

        async def __aexit__(self, *_failure):
            ownership_events.append("released")

    connection = SimpleNamespace(is_in_transaction=lambda: True, transaction=Transaction, copy_to_table=AsyncMock())
    candidate = uuid4()
    copy_target = membership.MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(candidate), "network_candidate_" + candidate.hex
    )
    return connection, copy_target, ownership_events


def _batch_operation(monkeypatch, entrypoint, gate, connection, copy_target):
    if entrypoint in {"copy", "approved_page"}:
        monkeypatch.setattr(membership if entrypoint == "copy" else approved, "_encode", gate.encode)
    else:
        monkeypatch.setattr(capture, "_batch", gate.encode)
        monkeypatch.setattr(capture, "_codec_context", lambda *_args: None)
        monkeypatch.setattr(capture, "_encoder", lambda: None)
    if entrypoint == "copy":
        return membership.copy_network_membership_batch(
            connection,
            copy_target=copy_target,
            input_bytes=b"[]",
            expected_input_sha256="4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
            require_candidate_authority=AsyncMock(return_value=copy_target),
        )
    if entrypoint == "approved_page":
        monkeypatch.setattr(
            approved, "_read_batch", AsyncMock(return_value=SimpleNamespace(input_bytes=b"[]", row_count=0))
        )
        return approved.read_approved_membership_batch(connection, object(), control_schema="synthetic_registry")
    if entrypoint == "office_capture":

        async def batches():
            yield b"[]"

        return capture._copy_office_batches(
            connection,
            "synthetic_offices",
            batches(),
            None,
            {},
            object(),
            (object(), "synthetic_registry"),
            lambda: None,
        )
    return witness._verified_page(
        None,
        connection,
        SimpleNamespace(control_schema="synthetic_registry"),
        object(),
        (None, {}, object()),
        {"after_ordinal": 0},
        [],
        (object(), lambda: None),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["copy", "approved_page", "office_capture", "office_witness"])
@pytest.mark.parametrize("native_failure", [False, True])
async def test_cancel_drains_native_ownership(monkeypatch, entrypoint, native_failure):
    gate = _native_gate(native_failure)
    connection, copy_target, ownership_events = _copy_context()
    work = asyncio.create_task(_batch_operation(monkeypatch, entrypoint, gate, connection, copy_target))
    try:
        async with asyncio.timeout(5):
            while not gate.entered.is_set():
                await asyncio.sleep(0)
        work.cancel()
        await asyncio.sleep(0)
        work.cancel()
        await asyncio.sleep(0.01)
        assert not work.done()
        assert not gate.finished.is_set()
        assert "released" not in ownership_events
        connection.copy_to_table.assert_not_awaited()
    finally:
        gate.release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(work, 5)
    assert gate.finished.is_set()
    assert ownership_events == (["acquired", "released"] if entrypoint == "copy" else [])
    connection.copy_to_table.assert_not_awaited()


def _management_writer(monkeypatch, connection):
    connection.in_transaction = connection.is_in_transaction
    connection.scalar = AsyncMock(return_value=1)
    connection.new, connection.dirty, connection.deleted = (), (), ()
    connection.begin_nested = connection.transaction
    monkeypatch.setattr(record_store, "_resolve_creation_identity", AsyncMock(return_value=7))
    monkeypatch.setattr(record_store, "_read_record_replay", AsyncMock(return_value=None))
    command = record_store.RegistryRecordCommand(
        "membership", 7, "correct", 1, {"memberships_json": []}, "Reviewed example membership", "example-edit"
    )
    return record_store.apply_registry_record_command(
        connection,
        command,
        record_store.RegistryActor("platform_admin", uuid4(), "example-client"),
        schema="example_registry",
        source_schema="example_source",
    )


def _binding_writer(monkeypatch, connection, input_bytes):
    monkeypatch.setattr(binding_references, "require_published_plan_binding_references", AsyncMock())
    connection.fetchrow = AsyncMock(side_effect=[{"draft_revision": 0}, None])
    command = source_bindings.NetworkSourceBindingBatchCommand(input_bytes, "Reviewed example source", "example-bind")
    return source_bindings.apply_network_source_binding_batch(
        connection,
        command,
        record_store.RegistryActor("platform_admin", uuid4(), "example-client"),
        control_schema="example_registry",
    )


def _import_writer(monkeypatch, entrypoint, connection, input_bytes):
    if entrypoint == "mlr":
        monkeypatch.setattr(source_admission, "_register_edition", AsyncMock())
        return source_admission.admit_cms_mlr_edition(
            connection, input_bytes, mlr_edition(input_bytes), control_schema="example_registry"
        )
    edition = required_targets.RegistryRequiredTargetEdition(
        uuid4(), "https://example.test/source", hashlib.sha256(input_bytes).hexdigest()
    )
    if entrypoint == "required_targets":
        return required_targets.admit_registry_required_targets(
            connection, input_bytes, edition, control_schema="example_registry"
        )
    monkeypatch.setattr(target_reviews, "_read_ledger", AsyncMock(return_value={}))
    return target_reviews.admit_registry_required_target_review(
        connection, input_bytes, edition, uuid4(), control_schema="example_registry"
    )


def _writer_operation(monkeypatch, entrypoint, gate, connection):
    encoders_by_name = {
        "mlr": (source_admission, "_encode_edition"),
        "source_bindings": (source_bindings, "_encode"),
        "record_membership": (record_store, "partition_network_membership_commands"),
        "required_targets": (required_targets, "_encode"),
        "target_reviews": (target_reviews, "_encode"),
    }
    module, encoder_name = encoders_by_name[entrypoint]
    monkeypatch.setattr(module, encoder_name, gate.encode)
    connection.execute, connection.fetchval = AsyncMock(), AsyncMock(return_value="public,pg_catalog")
    if entrypoint == "record_membership":
        return _management_writer(monkeypatch, connection)
    if entrypoint == "source_bindings":
        return _binding_writer(monkeypatch, connection, b"[]")
    return _import_writer(monkeypatch, entrypoint, connection, b"[]")


async def _writer_transaction(connection, operation):
    async with connection.transaction():
        await operation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entrypoint", ["mlr", "source_bindings", "record_membership", "required_targets", "target_reviews"]
)
@pytest.mark.parametrize("native_failure", [False, True])
async def test_cancel_drains_writer_ownership(monkeypatch, entrypoint, native_failure):
    gate = _native_gate(native_failure)
    connection, _, ownership_events = _copy_context()
    operation = _writer_operation(monkeypatch, entrypoint, gate, connection)
    work = asyncio.create_task(_writer_transaction(connection, operation))
    try:
        async with asyncio.timeout(5):
            while not gate.entered.is_set():
                if work.done():
                    await work
                await asyncio.sleep(0)
        pre_encode_statements = connection.execute.await_count
        work.cancel()
        await asyncio.sleep(0)
        work.cancel()
        await asyncio.sleep(0.01)
        assert not work.done()
        assert not gate.finished.is_set()
        assert "released" not in ownership_events
        connection.copy_to_table.assert_not_awaited()
    finally:
        gate.release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(work, 5)
        while not gate.finished.is_set():
            await asyncio.sleep(0)
    assert ownership_events.count("acquired") == ownership_events.count("released")
    assert connection.execute.await_count == pre_encode_statements
    connection.copy_to_table.assert_not_awaited()
