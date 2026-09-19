# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused contract coverage for destination PTG archive receive binding."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_receive_binding as subject
from process.ptg_parts.frozen_rate_binding import FROZEN_RATE_FILE_BINDING_OPTION
from process.ptg_parts.frozen_rate_files import FrozenRateFileMismatchError
from process.ptg_parts.ptg2_invalid_price_exclusion import INVALID_PRICE_EXCLUSION_POLICY_FIELD
from process.ptg_parts.result_archive_source_authority import PtgResultArchiveSourceAuthorityError


class _TransactionSession:
    def in_transaction(self) -> bool:
        return True


def _source_receipt(**overrides):
    return {
        "snapshot_id": "source-snapshot",
        "snapshot_manifest_sha256": "a" * 64,
        "frozen_binding_sha256": "b" * 64,
        **overrides,
    }


def test_destination_filing_identity_is_bounded_stable_and_destination_owned() -> None:
    first = subject._destination_filing_id(
        schema_name="mrf",
        destination_snapshot_id="destination-snapshot",
        source_key="source-a",
        source_receipt=_source_receipt(),
    )
    replay = subject._destination_filing_id(
        schema_name="mrf",
        destination_snapshot_id="destination-snapshot",
        source_key="source-a",
        source_receipt=_source_receipt(),
    )
    other_destination = subject._destination_filing_id(
        schema_name="mrf",
        destination_snapshot_id="other-snapshot",
        source_key="source-a",
        source_receipt=_source_receipt(),
    )

    assert first == replay
    assert first.startswith("archive-")
    assert len(first.encode("utf-8")) == 64
    assert other_destination != first


@pytest.mark.asyncio
async def test_receive_binding_requires_caller_transaction_before_database_access() -> None:
    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="requires an already-open caller transaction",
    ):
        await subject.receive_frozen_binding_params(
            object(),
            schema_name="mrf",
            staging_schema_name="stage",
            source_snapshot_key=1,
            destination_snapshot_id="destination-snapshot",
            source_key="source-a",
            authenticated_source_archive_metadata={},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("source_snapshot_key", [True, "1", 1.5, -1])
async def test_receive_binding_rejects_non_integer_snapshot_keys_before_database_access(
    source_snapshot_key,
) -> None:
    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="receive binding is invalid",
    ):
        await subject.receive_frozen_binding_params(
            _TransactionSession(),
            schema_name="mrf",
            staging_schema_name="stage",
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_id="destination-snapshot",
            source_key="source-a",
            authenticated_source_archive_metadata={},
        )


def test_received_parameters_preserve_optional_invalid_price_policy(monkeypatch) -> None:
    policy_mapping = {"contract": "invalid_price_exclusion_v1", "enabled": True}
    staged_mapping = {
        "binding_payload": {INVALID_PRICE_EXCLUSION_POLICY_FIELD: policy_mapping},
        "manifest": {},
    }
    monkeypatch.setattr(subject, "normalize_protected_frozen_rate_params", dict)
    monkeypatch.setattr(subject, "frozen_rate_binding_from_params", lambda params: {"params": params})

    parameters, binding = subject._received_parameters(
        staged=staged_mapping,
        destination_filing_id="archive-local",
        source_key="source-a",
    )

    assert parameters[INVALID_PRICE_EXCLUSION_POLICY_FIELD] == policy_mapping
    assert parameters[INVALID_PRICE_EXCLUSION_POLICY_FIELD] is not policy_mapping
    assert binding == {"params": parameters}


def test_received_parameters_wrap_invalid_frozen_input(monkeypatch) -> None:
    def reject(_parameters):
        raise ValueError("invalid")

    monkeypatch.setattr(subject, "normalize_protected_frozen_rate_params", reject)

    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="received frozen input is invalid",
    ):
        subject._received_parameters(
            staged={},
            destination_filing_id="archive-local",
            source_key="source-a",
        )


def test_received_parameters_require_local_binding(monkeypatch) -> None:
    monkeypatch.setattr(subject, "normalize_protected_frozen_rate_params", dict)
    monkeypatch.setattr(subject, "frozen_rate_binding_from_params", lambda _params: None)

    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="received frozen input is unavailable",
    ):
        subject._received_parameters(
            staged={},
            destination_filing_id="archive-local",
            source_key="source-a",
        )


@pytest.mark.parametrize(
    ("schema_name", "staging_schema_name", "configured_schema", "message"),
    [
        ("mrf", "mrf", "mrf", "must differ"),
        ("other", "stage", "mrf", "must match the configured PTG schema"),
    ],
)
def test_receive_context_rejects_invalid_schema_pair(
    monkeypatch,
    schema_name,
    staging_schema_name,
    configured_schema,
    message,
) -> None:
    monkeypatch.setattr(subject, "resolve_ptg2_schema", lambda: configured_schema)

    with pytest.raises(ValueError, match=message):
        subject._validated_receive_context(
            schema_name=schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=1,
            destination_snapshot_id="destination-snapshot",
            source_key="source-a",
            authenticated_source_archive_metadata={},
        )


@pytest.mark.parametrize("collision", ["snapshot", "filing"])
def test_receive_context_rejects_source_owned_attempt_identity(monkeypatch, collision) -> None:
    destination_snapshot = "destination-snapshot"
    source_receipt_mapping = {
        **_source_receipt(),
        "source_key": "source-a",
        "source_file_import_id": "source-filing",
    }
    if collision == "snapshot":
        destination_snapshot = source_receipt_mapping["snapshot_id"]
    else:
        source_receipt_mapping["source_file_import_id"] = subject._destination_filing_id(
            schema_name="mrf",
            destination_snapshot_id=destination_snapshot,
            source_key="source-a",
            source_receipt=source_receipt_mapping,
        )
    monkeypatch.setattr(subject, "resolve_ptg2_schema", lambda: "mrf")
    monkeypatch.setattr(
        subject,
        "validate_ptg_result_archive_source_authority",
        lambda _metadata: source_receipt_mapping,
    )

    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="requires new local attempt identities",
    ):
        subject._validated_receive_context(
            schema_name="mrf",
            staging_schema_name="stage",
            source_snapshot_key=1,
            destination_snapshot_id=destination_snapshot,
            source_key="source-a",
            authenticated_source_archive_metadata={},
        )


def test_received_source_evidence_uses_candidate_validator_for_binding_mismatch(monkeypatch) -> None:
    expected_binding_mapping = {"source_file_import_id": "source-filing"}
    authenticated = SimpleNamespace(
        source_manifest={FROZEN_RATE_FILE_BINDING_OPTION: {"different": True}},
        source_records=(),
    )
    monkeypatch.setattr(
        initialization,
        "_source_binding_for_receipt",
        lambda _local_binding, _source_filing: expected_binding_mapping,
    )
    with pytest.raises(
        FrozenRateFileMismatchError,
        match="cannot be treated as legacy",
    ):
        subject._validate_received_source_evidence(
            authenticated=authenticated,
            local_binding={},
            source_receipt={"source_file_import_id": "source-filing"},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "domain_error",
    [
        FrozenRateFileMismatchError("frozen evidence differs"),
        PtgResultArchiveSourceAuthorityError("source authority differs"),
    ],
)
async def test_receive_binding_normalizes_known_domain_rejections(monkeypatch, domain_error) -> None:
    def reject(**_kwargs):
        raise domain_error

    monkeypatch.setattr(subject, "_validated_receive_context", reject)

    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="receive binding is invalid",
    ) as captured:
        await subject.receive_frozen_binding_params(
            _TransactionSession(),
            schema_name="mrf",
            staging_schema_name="stage",
            source_snapshot_key=1,
            destination_snapshot_id="destination-snapshot",
            source_key="source-a",
            authenticated_source_archive_metadata={},
        )

    assert captured.value.__cause__ is domain_error


@pytest.mark.asyncio
async def test_receive_binding_preserves_unexpected_runtime_error(monkeypatch) -> None:
    def reject(**_kwargs):
        raise RuntimeError("schema configuration conflicts")

    monkeypatch.setattr(subject, "_validated_receive_context", reject)

    with pytest.raises(RuntimeError, match="schema configuration conflicts"):
        await subject.receive_frozen_binding_params(
            _TransactionSession(),
            schema_name="mrf",
            staging_schema_name="stage",
            source_snapshot_key=1,
            destination_snapshot_id="destination-snapshot",
            source_key="source-a",
            authenticated_source_archive_metadata={},
        )


@pytest.mark.asyncio
async def test_receive_binding_preserves_cancellation(monkeypatch) -> None:
    def cancel(**_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(subject, "_validated_receive_context", cancel)

    with pytest.raises(asyncio.CancelledError):
        await subject.receive_frozen_binding_params(
            _TransactionSession(),
            schema_name="mrf",
            staging_schema_name="stage",
            source_snapshot_key=1,
            destination_snapshot_id="destination-snapshot",
            source_key="source-a",
            authenticated_source_archive_metadata={},
        )
