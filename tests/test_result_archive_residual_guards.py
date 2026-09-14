# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure-path contracts for PTG result-archive publication boundaries."""

from __future__ import annotations

import copy
from dataclasses import replace
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import result_archive_adoption as adoption
from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_candidate_preparation as preparation
from process.ptg_parts import result_archive_candidate_validation as validation
from process.ptg_parts import result_archive_closure as closure
from process.ptg_parts import result_archive_source_authority as source_authority
from process.ptg_parts import source_pointers
from process.ptg_parts.frozen_rate_binding import frozen_rate_binding_from_params, frozen_rate_binding_sha256
from tests.test_result_archive_candidate_preparation_postgres import _candidate_manifest, _frozen_params
from tests.test_ptg_singleton_direct_control import _invalid_price_policy


class _QueryResult:
    def __init__(self, rows=(), *, scalar=None):
        self.rows = list(rows)
        self.scalar_value = scalar

    def __iter__(self):
        return iter(self.rows)

    def all(self):
        return list(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def scalar(self):
        return self.scalar_value

    def scalar_one(self):
        return self.scalar_value

    def one(self):
        if len(self.rows) != 1:
            raise AssertionError("expected one row")
        return self.rows[0]


def _authority_row() -> dict[str, object]:
    binding_by_field = {"source_file_import_id": "source-import", "source_key": "source-a"}
    return {
        "snapshot_id": "source-snapshot",
        "import_run_id": "source-run",
        "status": "validated",
        "manifest": {"serving_index": {"shared_snapshot_key": 7}},
        "source_file_import_id": "source-import",
        "internal_run_id": "source-run",
        "source_key": "source-a",
        "binding_sha256": frozen_rate_binding_sha256(binding_by_field),
        "binding_payload": binding_by_field,
    }


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "message"),
    [
        ("status", "building", "not sealed"),
        ("manifest", None, "manifest is invalid"),
        ("internal_run_id", "other-run", "binding does not match"),
        ("binding_payload", "{", "frozen binding is invalid"),
        ("binding_payload", [], "frozen binding is invalid"),
        ("binding_sha256", "0" * 64, "frozen binding changed"),
        ("source_file_import_id", "other-import", "frozen binding changed"),
        ("source_key", "other-source", "frozen binding changed"),
    ],
)
def test_source_authority_rejects_changed_immutable_provenance(field_name, invalid_value, message) -> None:
    """A locked source row cannot diverge from its frozen binding."""

    row_by_name = _authority_row()
    row_by_name[field_name] = invalid_value
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match=message):
        source_authority._authority_from_row(row_by_name, "operation-1")


@pytest.mark.parametrize("value", [None, "", "x" * 129])
def test_source_authority_rejects_missing_or_unbounded_operation_identity(value) -> None:
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="operation identity"):
        source_authority._operation_id(value)


def test_source_authority_rejects_non_mapping_manifest_and_bad_digest_receipt() -> None:
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="manifest is invalid"):
        source_authority.result_archive_manifest_sha256([])
    receipt = source_authority.PtgResultArchiveSourceAuthority(
        "operation-1", "snapshot-1", "import-1", "source-a", "a" * 64, "b" * 64
    ).as_dict()
    receipt["snapshot_manifest_sha256"] = "not-a-digest"
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="receipt is invalid"):
        source_authority.validate_ptg_result_archive_source_authority(receipt)


@pytest.mark.asyncio
async def test_source_authority_conflicting_pin_never_inserts(monkeypatch) -> None:
    authority = source_authority._authority_from_row(_authority_row(), "operation-1")
    monkeypatch.setattr(
        source_authority,
        "_pin_rows",
        AsyncMock(return_value=[{"snapshot_id": "other", "reason": "other"}]),
    )
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="pin conflicts"):
        await source_authority._insert_or_verify_pin(session, schema='"mrf"', authority=authority)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stored_rows", "acknowledge_absent", "result_count", "message"),
    [
        ([], False, 1, "pin is unavailable"),
        ([{"snapshot_id": "other", "reason": "other"}], True, 1, "pin is unavailable"),
        (None, False, 0, "could not be released"),
    ],
)
async def test_source_authority_release_requires_exact_owned_pin(
    monkeypatch, stored_rows, acknowledge_absent, result_count, message
) -> None:
    authority = source_authority.PtgResultArchiveSourceAuthority(
        "operation-1", "snapshot-1", "import-1", "source-a", "a" * 64, "b" * 64
    )
    expected_rows = [{"snapshot_id": authority.snapshot_id, "reason": authority.pin_reason}]
    monkeypatch.setattr(source_authority, "_acquire_operation_lock", AsyncMock())
    monkeypatch.setattr(
        source_authority, "_pin_rows", AsyncMock(return_value=expected_rows if stored_rows is None else stored_rows)
    )
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=_QueryResult([("snapshot-1",)] * result_count)),
    )
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match=message):
        await source_authority._release_authority_pin(
            session,
            schema_name="mrf",
            authority_by_field=authority.as_dict(),
            acknowledge_absent=acknowledge_absent,
        )


@pytest.mark.asyncio
async def test_source_authority_absent_release_is_only_an_explicit_reconciliation(monkeypatch) -> None:
    authority = source_authority.PtgResultArchiveSourceAuthority(
        "operation-1", "snapshot-1", "import-1", "source-a", "a" * 64, "b" * 64
    )
    monkeypatch.setattr(source_authority, "_acquire_operation_lock", AsyncMock())
    monkeypatch.setattr(source_authority, "_pin_rows", AsyncMock(return_value=[]))
    session = SimpleNamespace(in_transaction=lambda: True)
    assert (
        await source_authority._release_authority_pin(
            session,
            schema_name="mrf",
            authority_by_field=authority.as_dict(),
            acknowledge_absent=True,
        )
        == "already_released"
    )


def _authority() -> source_authority.PtgResultArchiveSourceAuthority:
    return source_authority._authority_from_row(_authority_row(), "operation-1")


@pytest.mark.asyncio
async def test_source_authority_database_guards_reject_missing_or_changed_rows(monkeypatch) -> None:
    empty_session = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult()))
    for loader in (source_authority._authority_row, source_authority._source_key_for_snapshot):
        with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="binding is unavailable"):
            await loader(empty_session, schema='"mrf"', snapshot_id="snapshot-1")

    expected = _authority()
    changed = replace(expected, source_key="other-source")
    monkeypatch.setattr(source_authority, "_acquire_operation_lock", AsyncMock())
    monkeypatch.setattr(source_authority, "acquire_ptg2_source_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(source_authority, "_source_key_for_snapshot", AsyncMock(return_value=expected.source_key))
    monkeypatch.setattr(source_authority, "_authority_row", AsyncMock(return_value=_authority_row()))
    monkeypatch.setattr(source_authority, "_authority_from_row", lambda *_args: changed)
    session = SimpleNamespace(in_transaction=lambda: True)
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="changed while it was captured"):
        await source_authority.prepare_ptg_result_archive_source_authority(
            session,
            schema_name="mrf",
            operation_id=expected.operation_id,
            snapshot_id=expected.snapshot_id,
        )


@pytest.mark.asyncio
async def test_source_authority_pin_insert_rechecks_after_write(monkeypatch) -> None:
    expected = _authority()
    monkeypatch.setattr(
        source_authority,
        "_pin_rows",
        AsyncMock(side_effect=[[], [{"snapshot_id": "other", "reason": "other"}]]),
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult()))
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="pin conflicts"):
        await source_authority._insert_or_verify_pin(session, schema='"mrf"', authority=expected)
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_source_authority_revalidation_and_clone_require_exact_receipt(monkeypatch) -> None:
    expected = _authority()
    monkeypatch.setattr(source_authority, "_acquire_operation_lock", AsyncMock())
    monkeypatch.setattr(source_authority, "acquire_ptg2_source_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(source_authority, "_authority_row", AsyncMock(return_value=_authority_row()))

    monkeypatch.setattr(
        source_authority,
        "_authority_from_row",
        lambda *_args: replace(expected, source_key="changed"),
    )
    session = SimpleNamespace(in_transaction=lambda: True)
    for operation in (
        source_authority.commit_ptg_result_archive_source_authority,
        source_authority.revalidate_ptg_result_archive_source_authority,
    ):
        with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="changed after"):
            await operation(session, schema_name="mrf", authority=expected.as_dict())

    monkeypatch.setattr(source_authority, "_authority_from_row", lambda *_args: expected)
    monkeypatch.setattr(source_authority, "_pin_rows", AsyncMock(return_value=[]))
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="pin is unavailable"):
        await source_authority.revalidate_ptg_result_archive_source_authority(
            session,
            schema_name="mrf",
            authority=expected.as_dict(),
        )
    monkeypatch.setattr(source_authority, "_bounded_clone_lock_reads", asynccontextmanager(_noop_context))
    monkeypatch.setattr(
        source_authority,
        "_authority_from_row",
        lambda *_args: replace(expected, source_key="changed"),
    )
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="changed after capture"):
        await source_authority.lock_ptg_result_archive_for_clone(
            session,
            schema_name="mrf",
            authority=expected.as_dict(),
        )
    monkeypatch.setattr(source_authority, "_authority_from_row", lambda *_args: expected)
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="pin is unavailable"):
        await source_authority.lock_ptg_result_archive_for_clone(
            session,
            schema_name="mrf",
            authority=expected.as_dict(),
        )


async def _noop_context(_session):
    yield


@pytest.mark.asyncio
async def test_source_authority_lock_errors_preserve_retry_classification(monkeypatch) -> None:
    monkeypatch.setattr(source_authority, "configure_ptg2_lifecycle_transaction", AsyncMock())
    session = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("database rejected lock")))
    monkeypatch.setattr(source_authority, "is_retryable_lifecycle_database_error", lambda _error: False)
    with pytest.raises(RuntimeError, match="database rejected lock"):
        await source_authority._acquire_operation_lock(session, "operation-1")

    settings_session = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult([("1s", "2s")])))
    with pytest.raises(RuntimeError, match="clone failed"):
        async with source_authority._bounded_clone_lock_reads(settings_session):
            raise RuntimeError("clone failed")


@pytest.mark.asyncio
async def test_source_authority_release_preserves_nonretryable_database_error(monkeypatch) -> None:
    expected = _authority()
    monkeypatch.setattr(source_authority, "_acquire_operation_lock", AsyncMock())
    monkeypatch.setattr(source_authority, "_pin_rows", AsyncMock(side_effect=RuntimeError("pin read failed")))
    monkeypatch.setattr(source_authority, "is_retryable_lifecycle_database_error", lambda _error: False)
    with pytest.raises(RuntimeError, match="pin read failed"):
        await source_authority._release_authority_pin(
            SimpleNamespace(in_transaction=lambda: True),
            schema_name="mrf",
            authority_by_field=expected.as_dict(),
            acknowledge_absent=False,
        )


def test_source_authority_rejects_noncanonical_operation_and_open_receipt_shape() -> None:
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="operation identity"):
        source_authority._operation_id("not an operation")
    receipt = _authority().as_dict()
    receipt["unexpected"] = True
    with pytest.raises(source_authority.PtgResultArchiveSourceAuthorityError, match="receipt is invalid"):
        source_authority.validate_ptg_result_archive_source_authority(receipt)


@pytest.mark.parametrize(
    ("function", "args", "message"),
    [
        (adoption._safe_identifier, ("unsafe-name",), "simple PostgreSQL identifier"),
        (adoption._required_snapshot_id, ("",), "destination_snapshot_id"),
        (preparation._safe_identifier, ("unsafe-name",), "simple PostgreSQL identifier"),
        (preparation._required_snapshot_id, ("",), "snapshot is required"),
        (initialization._required_text, ("",), "identity is invalid"),
        (initialization._required_coverage_scope, (b"short",), "coverage scope is invalid"),
        (initialization._required_schema, ("unsafe-name",), "simple PostgreSQL identifier"),
    ],
)
def test_archive_identifiers_fail_closed(function, args, message) -> None:
    keyword_arguments_by_name = {}
    if function in {adoption._safe_identifier, preparation._safe_identifier}:
        keyword_arguments_by_name["label"] = "schema"
    elif function is preparation._required_snapshot_id:
        keyword_arguments_by_name["label"] = "snapshot"
    elif function is initialization._required_text:
        keyword_arguments_by_name.update(field_name="identity", maximum=10)
    elif function is initialization._required_schema:
        keyword_arguments_by_name["field_name"] = "schema"
    with pytest.raises((ValueError, RuntimeError), match=message):
        function(*args, **keyword_arguments_by_name)


def _staged_candidate() -> initialization._AuthenticatedStagedCandidate:
    return initialization._AuthenticatedStagedCandidate(
        source_snapshot_id="source-snapshot",
        source_manifest={},
        primary_plan_id="plan-a",
        primary_plan_market_type="market-a",
        coverage_scope_id=b"c" * 32,
        plan_scopes=(("plan-a", "market-a"),),
        source_records=({},),
    )


def test_candidate_initialization_rejects_bad_manifest_and_plan_scope() -> None:
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="manifest coverage scope"):
        initialization._manifest_coverage_scope({"serving_index": {"coverage_scope_id": "not-hex"}})
    staged = replace(_staged_candidate(), plan_scopes=(("plan-b", "market-a"),))
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="plan scope differs"):
        initialization._assert_plan_scope(
            staged,
            {"plan_ids": ("plan-a",), "plan_market_types": ("market-a",)},
        )


def test_candidate_initialization_preserves_admitted_exclusion_policy() -> None:
    params, _params_with_proof, _descriptors = _frozen_params()
    local_binding = frozen_rate_binding_from_params(params)
    assert local_binding is not None
    local_binding[initialization.INVALID_PRICE_EXCLUSION_POLICY_FIELD] = {"contract": "policy-v1"}
    options = initialization._local_run_options(params, local_binding, _authority().as_dict())
    assert options[initialization.INVALID_PRICE_EXCLUSION_POLICY_FIELD] == {"contract": "policy-v1"}


@pytest.mark.asyncio
async def test_candidate_initialization_rejects_conflicting_retry_state(monkeypatch) -> None:
    monkeypatch.setattr(initialization, "_rows", AsyncMock(side_effect=[[], [], []]))
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="source records conflict"):
        await initialization._copy_snapshot_sources(
            object(),
            destination_schema="mrf",
            staging_schema="staging",
            source_snapshot_id="source",
            destination_snapshot_id="destination",
        )

    monkeypatch.setattr(
        initialization,
        "_rows",
        AsyncMock(side_effect=[[], [{"snapshot_id": "wrong"}]]),
    )
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="snapshot conflicts with retry"):
        await initialization._is_new_snapshot_after_insert(
            object(),
            schema_name="mrf",
            snapshot_id="destination",
            import_run_id="run",
            import_month="2026-09-01",
            manifest={},
        )

    monkeypatch.setattr(initialization, "_one", AsyncMock(return_value={"plan_id": "wrong"}))
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="scope conflicts with retry"):
        await initialization._insert_or_verify_scope(
            session,
            schema_name="mrf",
            snapshot_id="destination",
            staged=_staged_candidate(),
        )

    monkeypatch.setattr(initialization, "_rows", AsyncMock(return_value=[]))
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="plan scope conflicts"):
        await initialization._insert_or_verify_plan_scopes(
            session,
            schema_name="mrf",
            snapshot_id="destination",
            plan_scopes=(("plan-a", "market-a"),),
        )


@pytest.mark.asyncio
async def test_candidate_initialization_rejects_shared_run_or_prior_attestation(monkeypatch) -> None:
    monkeypatch.setattr(initialization, "_rows", AsyncMock(return_value=[{"snapshot_id": "other"}]))
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="attached to unrelated state"):
        await initialization._assert_local_attempt_isolated(
            object(),
            schema_name="mrf",
            snapshot_id="destination",
            import_run_id="run",
        )

    monkeypatch.setattr(
        initialization,
        "_rows",
        AsyncMock(side_effect=[[{"snapshot_id": "destination"}], [{"snapshot_id": "destination"}]]),
    )
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="pre-attested candidate"):
        await initialization._assert_local_attempt_isolated(
            object(),
            schema_name="mrf",
            snapshot_id="destination",
            import_run_id="run",
        )


def test_candidate_initialization_local_admission_rejects_aliases_and_missing_proof(monkeypatch) -> None:
    authority = _authority().as_dict()
    base_request_by_name = {
        "schema_name": "mrf",
        "staging_schema_name": "staging",
        "source_snapshot_key": 7,
        "destination_snapshot_id": "destination",
        "frozen_binding_params": {},
        "authenticated_source_archive_metadata": authority,
    }
    monkeypatch.setattr(initialization, "resolve_ptg2_schema", lambda: "mrf")
    for changed, message in (
        ({"staging_schema_name": "mrf"}, "must differ"),
        ({"schema_name": "other"}, "configured PTG schema"),
        ({"source_snapshot_key": True}, "must be non-negative"),
        ({"frozen_binding_params": {"frozen_rate_file_count": -1}}, "local frozen input is invalid"),
        ({}, "requires frozen source-file evidence"),
    ):
        request_by_name = {**base_request_by_name, **changed}
        with pytest.raises((ValueError, initialization.ResultArchiveCandidateInitializationError), match=message):
            initialization._validated_local_admission(**request_by_name)

    params, _params_with_proof, _descriptors = _frozen_params()
    local_binding = frozen_rate_binding_from_params(params)
    assert local_binding is not None
    same_identity_authority = replace(
        _authority(),
        snapshot_id="destination",
        source_key=str(local_binding["source_key"]),
    ).as_dict()
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="new local attempt identities"):
        initialization._validated_local_admission(
            **{
                **base_request_by_name,
                "frozen_binding_params": params,
                "authenticated_source_archive_metadata": same_identity_authority,
            }
        )


def test_candidate_initialization_rejects_changed_source_authority_and_scope() -> None:
    params, params_with_proof, descriptors = _frozen_params()
    local_binding = frozen_rate_binding_from_params(params)
    assert local_binding is not None
    source_binding_by_field = {**local_binding, "source_file_import_id": "source-import"}
    source_manifest = _candidate_manifest(
        frozen_params={
            **params_with_proof,
            "import_id": "source-import",
            "source_file_import_id": "source-import",
        },
        descriptors=descriptors,
    )
    source_manifest[initialization.FROZEN_RATE_FILE_BINDING_OPTION] = source_binding_by_field
    source_manifest["serving_index"] = {"coverage_scope_id": (b"d" * 32).hex()}
    receipt = source_authority.PtgResultArchiveSourceAuthority(
        operation_id="operation-1",
        snapshot_id="source-snapshot",
        source_file_import_id="source-import",
        source_key=str(local_binding["source_key"]),
        snapshot_manifest_sha256=source_authority.result_archive_manifest_sha256(source_manifest),
        frozen_binding_sha256=frozen_rate_binding_sha256(source_binding_by_field),
    ).as_dict()
    staged_by_field = {
        "snapshot_id": "source-snapshot",
        "status": "validated",
        "import_run_id": initialization.frozen_internal_run_id("source-import"),
        "internal_run_id": initialization.frozen_internal_run_id("source-import"),
        "source_file_import_id": "source-import",
        "binding_payload": source_binding_by_field,
        "binding_sha256": frozen_rate_binding_sha256(source_binding_by_field),
        "manifest": source_manifest,
        "coverage_scope_id": b"c" * 32,
    }
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="coverage scope differs"):
        initialization._validated_staged_identity(
            staged_by_field,
            local_binding=local_binding,
            source_receipt=receipt,
        )
    staged_by_field["status"] = "building"
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="authority does not match"):
        initialization._validated_staged_identity(
            staged_by_field,
            local_binding=local_binding,
            source_receipt=receipt,
        )


@pytest.mark.asyncio
async def test_candidate_initialization_persist_rejects_binding_and_partial_retry(monkeypatch) -> None:
    params, _params_with_proof, _descriptors = _frozen_params()
    local_binding = frozen_rate_binding_from_params(params)
    assert local_binding is not None
    admission = initialization._LocalCandidateAdmission(
        destination_schema="mrf",
        staging_schema="staging",
        source_snapshot_key=7,
        destination_snapshot_id="destination",
        import_run_id="local-run",
        binding=local_binding,
        source_receipt=_authority().as_dict(),
    )
    monkeypatch.setattr(initialization, "_is_new_run_after_insert", AsyncMock(return_value=True))
    monkeypatch.setattr(initialization, "_is_new_snapshot_after_insert", AsyncMock(return_value=True))
    monkeypatch.setattr(initialization, "_insert_or_verify_scope", AsyncMock())
    monkeypatch.setattr(initialization, "_copy_source_graph", AsyncMock(return_value=1))
    monkeypatch.setattr(initialization, "_assert_local_attempt_isolated", AsyncMock())
    monkeypatch.setattr(initialization.db, "bind_existing_session", asynccontextmanager(_noop_context))
    monkeypatch.setattr(initialization, "insert_or_compare_frozen_binding", AsyncMock(return_value={"changed": True}))
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="frozen binding changed"):
        await initialization._persist_local_candidate(
            object(),
            admission=admission,
            staged=_staged_candidate(),
            manifest={},
            frozen_binding_params=params,
        )

    monkeypatch.setattr(initialization, "insert_or_compare_frozen_binding", AsyncMock(return_value=local_binding))
    monkeypatch.setattr(initialization, "_is_new_snapshot_after_insert", AsyncMock(return_value=False))
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="incomplete attempt state"):
        await initialization._persist_local_candidate(
            object(),
            admission=admission,
            staged=_staged_candidate(),
            manifest={},
            frozen_binding_params=params,
        )


@pytest.mark.parametrize("rows", [[], [{"value": 1}, {"value": 2}]])
@pytest.mark.asyncio
async def test_archive_single_row_loaders_reject_missing_or_ambiguous_rows(rows) -> None:
    for module, error_type in (
        (adoption, adoption.ResultArchiveAdoptionError),
        (preparation, preparation.ResultArchiveCandidatePreparationError),
        (initialization, initialization.ResultArchiveCandidateInitializationError),
    ):
        session = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult(rows)))
        with pytest.raises(error_type, match="missing or ambiguous"):
            await module._one(session, "SELECT 1", {}, "row")


def _staged_layout() -> dict[str, object]:
    return {
        "generation": adoption.PTG2_V4_SHARED_GENERATION,
        "state": "sealed",
        "map_root_state": "complete",
        "semantic_fingerprint": b"f" * 32,
        "support_digest": b"s" * 32,
        "layout_manifest": {"serving_index": {"shared_snapshot_key": 7}},
    }


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "message"),
    [
        ("generation", "legacy", "only supports V4"),
        ("state", "building", "sealed complete"),
        ("map_root_state", "building", "sealed complete"),
        ("semantic_fingerprint", b"short", "metadata is invalid"),
        ("support_digest", b"short", "metadata is invalid"),
        ("layout_manifest", None, "metadata is invalid"),
    ],
)
def test_adoption_rejects_unsealed_or_incomplete_layout_metadata(field_name, invalid_value, message) -> None:
    layout_by_field = _staged_layout()
    layout_by_field[field_name] = invalid_value
    with pytest.raises(adoption.ResultArchiveAdoptionError, match=message):
        adoption._validated_staged_layout(layout_by_field)


def test_adoption_remaps_only_destination_local_keys_and_rejects_wrong_source_keys() -> None:
    manifest_by_field = {
        "serving_index": {
            "shared_snapshot_key": 7,
            "provider_graph": {"provider_tax_identity": {"snapshot_key": 7, "digest": "retained"}},
        }
    }
    remapped = adoption._remapped_layout_manifest(manifest_by_field, source_snapshot_key=7, destination_snapshot_key=9)
    assert remapped["serving_index"]["shared_snapshot_key"] == 9
    assert remapped["serving_index"]["provider_graph"]["provider_tax_identity"] == {
        "snapshot_key": 9,
        "digest": "retained",
    }
    assert manifest_by_field["serving_index"]["shared_snapshot_key"] == 7
    for field_path in (("serving_index", "shared_snapshot_key"), ("provider_tax_identity", "snapshot_key")):
        changed = copy.deepcopy(manifest_by_field)
        if field_path[0] == "serving_index":
            changed["serving_index"][field_path[1]] = 8
        else:
            changed["serving_index"]["provider_graph"][field_path[0]][field_path[1]] = 8
        with pytest.raises(adoption.ResultArchiveAdoptionError, match="wrong snapshot key"):
            adoption._remapped_layout_manifest(changed, source_snapshot_key=7, destination_snapshot_key=9)


@pytest.mark.asyncio
async def test_adoption_rejects_same_logical_identity_and_unsupported_table_shape() -> None:
    same_identity = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult([{"snapshot_id": "local"}])))
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="remapped destination"):
        await adoption._assert_remapped_logical_identity(
            same_identity,
            staging_schema="staging",
            source_snapshot_key=7,
            destination_snapshot_id="local",
        )
    missing_key = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult([("value",)])))
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="unsupported key shape"):
        await adoption._table_columns(missing_key, schema_name="mrf", table_name="example")


@pytest.mark.asyncio
async def test_adoption_rejects_oversized_or_invalid_staged_cas_closure() -> None:
    for results, message in (
        ((_QueryResult(scalar=4),), "exceeds its bound"),
        ((_QueryResult(scalar=1), _QueryResult(scalar=True)), "CAS metadata is invalid"),
    ):
        session = SimpleNamespace(execute=AsyncMock(side_effect=results))
        with pytest.raises(adoption.ResultArchiveAdoptionError, match=message):
            await adoption._validate_staged_blocks(
                session,
                schema_name="mrf",
                staging_schema_name="staging",
                max_staged_block_rows=3,
            )


@pytest.mark.asyncio
async def test_adoption_optional_rows_and_rekeyed_data_fail_closed(monkeypatch) -> None:
    duplicate_session = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult([{}, {}])))
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="is ambiguous"):
        await adoption._one_or_none(duplicate_session, "SELECT 1", {}, "optional row")

    monkeypatch.setattr(adoption, "_table_columns", AsyncMock(side_effect=[("snapshot_key",), ("other",)]))
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="column contract differs"):
        await adoption._copy_rekeyed_table(
            object(),
            schema_name="mrf",
            staging_schema_name="staging",
            table_name="relation",
            source_snapshot_key=7,
            destination_snapshot_key=9,
        )

    monkeypatch.setattr(adoption, "_table_columns", AsyncMock(side_effect=[("snapshot_key",), ("other",)]))
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="column contract differs"):
        await adoption._assert_rekeyed_table_matches(
            object(),
            schema_name="mrf",
            staging_schema_name="staging",
            table_name="relation",
            source_snapshot_key=7,
            destination_snapshot_key=9,
        )

    monkeypatch.setattr(adoption, "_table_columns", AsyncMock(return_value=("snapshot_key", "value")))
    conflict_session = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult(scalar=True)))
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="conflicts with destination"):
        await adoption._assert_rekeyed_table_matches(
            conflict_session,
            schema_name="mrf",
            staging_schema_name="staging",
            table_name="relation",
            source_snapshot_key=7,
            destination_snapshot_key=9,
        )


def test_adoption_manifest_remap_preserves_layouts_without_nested_local_keys() -> None:
    manifest_without_serving_by_field = {"semantic": "retained"}
    assert (
        adoption._remapped_layout_manifest(
            manifest_without_serving_by_field,
            source_snapshot_key=7,
            destination_snapshot_key=9,
        )
        == manifest_without_serving_by_field
    )
    manifest_without_provider_graph_by_field = {"serving_index": {"shared_snapshot_key": 7, "semantic": "retained"}}
    remapped = adoption._remapped_layout_manifest(
        manifest_without_provider_graph_by_field,
        source_snapshot_key=7,
        destination_snapshot_key=9,
    )
    assert remapped["serving_index"] == {"shared_snapshot_key": 9, "semantic": "retained"}


@pytest.mark.asyncio
async def test_adoption_finalizer_family_rejects_missing_order_or_completion(monkeypatch) -> None:
    monkeypatch.setattr(adoption, "_REKEYED_TABLES", ("ordinary",))
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="missing a finalizer relation"):
        await adoption._copy_staged_layout_rows(
            object(),
            schema_name="mrf",
            staging_schema_name="staging",
            source_snapshot_key=7,
            destination_snapshot_key=9,
        )

    monkeypatch.setattr(adoption, "_FINALIZER_MAP_TABLES", ("root", "child"))
    monkeypatch.setattr(adoption, "_REKEYED_TABLES", ("root", "ordinary", "child"))
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="not contiguous"):
        await adoption._copy_staged_layout_rows(
            object(),
            schema_name="mrf",
            staging_schema_name="staging",
            source_snapshot_key=7,
            destination_snapshot_key=9,
        )

    monkeypatch.setattr(adoption, "_staged_finalizer_root", AsyncMock(return_value={"state": "building"}))
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="root is not complete"):
        await adoption._copy_finalizer_map_rows(
            object(),
            schema_name="mrf",
            staging_schema_name="staging",
            source_snapshot_key=7,
            destination_snapshot_key=9,
        )

    failed_session = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult(scalar=None)))
    for operation, message in (
        (adoption._insert_building_finalizer_root, "enter building state"),
        (adoption._complete_finalizer_root_from_staging, "could not complete"),
    ):
        with pytest.raises(adoption.ResultArchiveAdoptionError, match=message):
            await operation(
                failed_session,
                schema_name="mrf",
                staging_schema_name="staging",
                source_snapshot_key=7,
                destination_snapshot_key=9,
            )


@pytest.mark.asyncio
async def test_adoption_reused_layout_requires_equal_persisted_map_summary(monkeypatch) -> None:
    summaries = [SimpleNamespace(map_digest=b"a" * 32), SimpleNamespace(map_digest=b"b" * 32)]
    monkeypatch.setattr(adoption, "summarize_persisted_v4_snapshot_maps", AsyncMock(side_effect=summaries))
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="summary differs"):
        await adoption._reused_mapping_digest(
            object(),
            schema_name="mrf",
            staging_schema_name="staging",
            source_snapshot_key=7,
            destination_snapshot_key=9,
            support_digest=b"s" * 32,
            layout_manifest={},
            max_staged_block_rows=10,
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"schema_name": "mrf", "staging_schema_name": "mrf"},
        {"source_snapshot_key": -1},
        {"source_snapshot_key": True},
        {"max_staged_block_rows": 0},
        {"max_staged_block_rows": adoption._MAX_STAGED_BLOCK_ROWS + 1},
        {"destination_snapshot_id": ""},
        {"build_token": ""},
    ],
)
def test_adoption_request_rejects_unsafe_local_identity_or_bounds(kwargs) -> None:
    request_by_name = {
        "schema_name": "mrf",
        "staging_schema_name": "staging",
        "source_snapshot_key": 7,
        "destination_snapshot_id": "local-snapshot",
        "build_token": "local-build",
        "max_staged_block_rows": 10,
    }
    request_by_name.update(kwargs)
    with pytest.raises(ValueError):
        adoption._validated_adoption_request(**request_by_name)


@pytest.mark.parametrize(
    ("manifest", "message"),
    [
        ({"activation": {"source_key": ""}}, "no valid activation scope"),
        ({"activation": {"source_key": "source-a", "plan_id": "plan-a"}}, "partial activation plan scope"),
        (
            {"activation": {"source_key": "source-a", "plan_id": "", "plan_market_type": "market-a"}},
            "invalid activation plan scope",
        ),
    ],
)
def test_candidate_preparation_rejects_malformed_activation_scope(manifest, message) -> None:
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match=message):
        preparation._activation_scope(manifest, label="candidate")


def test_candidate_preparation_rejects_missing_database_scope_and_scope_conflict() -> None:
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="no authoritative plan scope"):
        preparation._database_scope({}, label="candidate")
    destination_by_field = {
        "manifest": {"activation": {"source_key": "source-a"}},
        "scope_plan_id": "plan-a",
        "scope_plan_market_type": "market-a",
    }
    staging_by_field = {**destination_by_field, "scope_plan_id": "plan-b"}
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="source scope differs"):
        preparation._assert_scope_matches(destination_by_field, staging_by_field)


@pytest.mark.asyncio
async def test_candidate_preparation_rejects_column_drift_before_copy(monkeypatch) -> None:
    monkeypatch.setattr(
        preparation, "_table_columns", AsyncMock(side_effect=[("snapshot_id",), ("snapshot_id", "value")])
    )
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="column contract differs"):
        await preparation._matching_allowed_amount_columns(
            object(),
            schema_name="mrf",
            staging_schema_name="staging",
            table_name="allowed_amount",
        )


@pytest.mark.asyncio
async def test_candidate_preparation_rejects_malformed_frozen_inputs_and_local_alias(monkeypatch) -> None:
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="requested frozen input is invalid"):
        preparation._assert_portable_frozen_input_matches({}, {}, {"frozen_rate_file_count": -1})
    with pytest.raises(
        preparation.ResultArchiveCandidatePreparationError, match="requires frozen source-file evidence"
    ):
        preparation._assert_portable_frozen_input_matches({}, {}, {})

    missing_key = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult([("value",)])))
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="unsupported key shape"):
        await preparation._table_columns(missing_key, schema_name="mrf", table_name="allowed")

    monkeypatch.setattr(preparation, "_locked_candidate", AsyncMock(return_value={"snapshot_id": "same"}))
    monkeypatch.setattr(preparation, "_locked_staging_snapshot", AsyncMock(return_value={"snapshot_id": "same"}))
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="remapped destination"):
        await preparation._locked_candidate_inputs(
            object(),
            destination_schema="mrf",
            staging_schema="staging",
            source_snapshot_key=7,
            destination_snapshot_id="same",
            frozen_binding_params={},
        )


def test_candidate_preparation_rejects_noncanonical_persisted_binding() -> None:
    params, params_with_proof, descriptors = _frozen_params()
    policy = _invalid_price_policy()
    policy_manifest = _candidate_manifest(
        frozen_params={**params_with_proof, preparation.INVALID_PRICE_EXCLUSION_POLICY_FIELD: policy},
        descriptors=descriptors,
    )
    assert (
        preparation._portable_frozen_binding(policy_manifest, label="candidate")[
            preparation.INVALID_PRICE_EXCLUSION_POLICY_FIELD
        ]
        == policy
    )
    manifest = _candidate_manifest(frozen_params=params_with_proof, descriptors=descriptors)
    manifest[preparation.FROZEN_RATE_FILE_BINDING_OPTION] = {
        **manifest[preparation.FROZEN_RATE_FILE_BINDING_OPTION],
        "unexpected": True,
    }
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="frozen input is invalid"):
        preparation._portable_frozen_binding(manifest, label="candidate")


@pytest.mark.asyncio
async def test_candidate_preparation_local_binding_must_match_attempt_candidate_and_scope() -> None:
    params, _params_with_proof, _descriptors = _frozen_params()
    expected_binding = frozen_rate_binding_from_params(params)
    assert expected_binding is not None
    with pytest.raises(
        preparation.ResultArchiveCandidatePreparationError, match="requires frozen source-file evidence"
    ):
        await preparation._validate_local_frozen_candidate(
            object(),
            schema_name="mrf",
            destination_candidate={"snapshot_id": "destination"},
            frozen_binding_params={},
        )

    candidate_by_field = {"snapshot_id": "destination", "import_run_id": "wrong", "manifest": {}}
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="does not match the local attempt"):
        await preparation._validate_local_frozen_candidate(
            object(),
            schema_name="mrf",
            destination_candidate=candidate_by_field,
            frozen_binding_params=params,
        )

    candidate_by_field["import_run_id"] = preparation.frozen_internal_run_id(
        str(expected_binding["source_file_import_id"])
    )
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="does not match the local candidate"):
        await preparation._validate_local_frozen_candidate(
            object(),
            schema_name="mrf",
            destination_candidate=candidate_by_field,
            frozen_binding_params=params,
        )

    candidate_by_field["manifest"] = {
        preparation.FROZEN_RATE_FILE_BINDING_OPTION: expected_binding,
        "activation": {"source_key": "other_source"},
    }
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="wrong source scope"):
        await preparation._validate_local_frozen_candidate(
            object(),
            schema_name="mrf",
            destination_candidate=candidate_by_field,
            frozen_binding_params=params,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("schema_name", "staging_schema_name", "message"),
    [("mrf", "mrf", "must differ"), ("other", "staging", "configured PTG schema")],
)
async def test_candidate_preparation_rejects_unsafe_schema_pair(
    monkeypatch, schema_name, staging_schema_name, message
) -> None:
    monkeypatch.setattr(preparation, "resolve_ptg2_schema", lambda: "mrf")
    with pytest.raises(ValueError, match=message):
        await preparation.prepare_result_archive_candidate_evidence(
            SimpleNamespace(in_transaction=lambda: True),
            schema_name=schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=7,
            destination_snapshot_id="destination",
            frozen_binding_params={},
        )


def _preparation_receipts():
    prepared_candidate = preparation.PreparedResultArchiveCandidate(
        preparation.RESULT_ARCHIVE_CANDIDATE_PREPARATION_CONTRACT,
        "local-snapshot",
        "source-snapshot",
        {"allowed": 1},
        "a" * 64,
    )
    prepared_layout = adoption.PreparedResultArchiveLayout(
        adoption.RESULT_ARCHIVE_ADOPTION_CONTRACT,
        "local-snapshot",
        9,
        7,
        b"m" * 32,
    )
    return prepared_candidate, prepared_layout


@pytest.mark.parametrize(
    ("candidate_change", "layout_change", "message"),
    [
        ({"contract": "wrong"}, {}, "logical preparation receipt is invalid"),
        ({"requires_fresh_destination_attestation": False}, {}, "logical preparation receipt is invalid"),
        ({}, {"contract": "wrong"}, "layout preparation receipt is invalid"),
        ({}, {"mapping_digest": b"short"}, "layout preparation receipt is invalid"),
        ({"destination_snapshot_id": "other"}, {}, "different snapshots"),
    ],
)
def test_candidate_validation_rejects_untrusted_preparation_receipts(candidate_change, layout_change, message) -> None:
    prepared_candidate, prepared_layout = _preparation_receipts()
    prepared_candidate = replace(prepared_candidate, **candidate_change)
    prepared_layout = replace(prepared_layout, **layout_change)
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match=message):
        validation._validate_preparation_receipts(prepared_candidate, prepared_layout)


def _candidate_layout_row() -> dict[str, object]:
    return {
        "snapshot_key": 9,
        "layout_state": "sealed",
        "layout_generation": "shared_blocks_v4",
        "layout_mapping_digest": b"m" * 32,
        "v4_root_state": "complete",
        "v4_root_map_digest": b"m" * 32,
        "layout_manifest": {"serving_index": {"shared_snapshot_key": 9}},
    }


def test_candidate_validation_rejects_layout_receipt_drift_and_nonlocal_key() -> None:
    _candidate, layout = _preparation_receipts()
    drifted = _candidate_layout_row()
    drifted["v4_root_map_digest"] = b"x" * 32
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="differs from its preparation"):
        validation._validated_layout_serving_index(drifted, layout)
    wrong_key = _candidate_layout_row()
    wrong_key["layout_manifest"] = {"serving_index": {"shared_snapshot_key": 7}}
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="destination-local serving key"):
        validation._validated_layout_serving_index(wrong_key, layout)


def test_candidate_validation_rejects_stale_replay_and_source_metadata() -> None:
    replay_by_field = {
        "status": "validated",
        "run_status": "validated",
        "manifest": {"activation": {"source_key": "source-a"}, "serving_index": {"old": True}},
    }
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="replay serving manifest changed"):
        validation._candidate_attributes(replay_by_field, source_key="source-a", serving_index={"new": True})
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="sealed source key differs"):
        validation._attach_destination_source_identity(
            {"source_key": "other"},
            source_key="source-a",
            source_records=[{"raw_container_sha256": "a" * 64}],
        )
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="sealed source set differs"):
        validation._attach_destination_source_identity(
            {"source_set": {"wrong": True}},
            source_key="source-a",
            source_records=[{"raw_container_sha256": "a" * 64}],
        )


@pytest.mark.asyncio
async def test_candidate_validation_rejects_missing_rows_and_changed_logical_evidence(monkeypatch) -> None:
    monkeypatch.setattr(validation, "resolve_ptg2_schema", lambda: "mrf")
    with pytest.raises(ValueError, match="simple PostgreSQL identifier"):
        validation._validated_schema("unsafe-name")
    with pytest.raises(ValueError, match="configured PTG schema"):
        validation._validated_schema("other")

    missing_session = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult()))
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="missing or ambiguous"):
        await validation._locked_candidate_row(
            missing_session,
            schema_name="mrf",
            snapshot_id="snapshot",
            source_key="source-a",
        )

    prepared_candidate, _prepared_layout = _preparation_receipts()
    monkeypatch.setattr(validation, "_allowed_amount_counts", AsyncMock(return_value={"allowed": 2}))
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="logical evidence differs"):
        await validation._validate_logical_preparation(
            object(),
            schema_name="mrf",
            snapshot_id="snapshot",
            candidate_row={"frozen_binding_sha256": prepared_candidate.frozen_binding_sha256},
            prepared_candidate=prepared_candidate,
        )


@pytest.mark.parametrize(
    ("candidate", "message"),
    [
        (
            {"status": "building", "run_status": "running", "manifest": {"activation": {"state": "changed"}}},
            "building state is not pristine",
        ),
        (
            {"status": "failed", "run_status": "failed", "manifest": {"activation": {"source_key": "source-a"}}},
            "not building or replayable",
        ),
        (
            {
                "status": "validated",
                "run_status": "validated",
                "manifest": {"activation": {"source_key": "other"}, "serving_index": {}},
            },
            "source scope changed",
        ),
    ],
)
def test_candidate_validation_rejects_nonpristine_or_changed_lifecycle_state(candidate, message) -> None:
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match=message):
        validation._candidate_attributes(candidate, source_key="source-a", serving_index={})


@pytest.mark.asyncio
async def test_candidate_validation_rejects_invalid_source_set_and_nonterminal_run(monkeypatch) -> None:
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="source set is invalid"):
        validation._attach_destination_source_identity(
            {},
            source_key="source-a",
            source_records=[{"raw_container_sha256": "not-a-digest"}],
        )
    session = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult()))
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="could not become terminal"):
        await validation._complete_local_run(
            session,
            schema_name="mrf",
            candidate_attributes={"manifest": {}, "import_run_id": "run"},
        )


@pytest.mark.asyncio
async def test_candidate_validation_wraps_normal_audit_target_rejection(monkeypatch) -> None:
    prepared_candidate, prepared_layout = _preparation_receipts()
    candidate_state_by_field = {
        **_candidate_layout_row(),
        "status": "building",
        "run_status": "running",
        "manifest": {"activation": {"state": "building", "source_key": "source_a"}},
        "snapshot_id": "local-snapshot",
        "import_run_id": "local-run",
        "import_month": "2026-09-01",
        "created_at": "created",
        "staged_at": "staged",
        "current_snapshot_id": None,
        "has_attestation": False,
    }
    monkeypatch.setattr(validation, "_validate_logical_preparation", AsyncMock())
    monkeypatch.setattr(
        validation,
        "_source_records",
        AsyncMock(return_value=[{"raw_container_sha256": "a" * 64}]),
    )
    monkeypatch.setattr(
        validation,
        "validate_candidate_audit_target_state",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("rejected evidence")),
    )
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="normal audit target rejected"):
        await validation._validated_audit_target(
            object(),
            schema_name="mrf",
            snapshot_id="local-snapshot",
            source_key="source_a",
            candidate_state=candidate_state_by_field,
            prepared_candidate=prepared_candidate,
            prepared_layout=prepared_layout,
        )


@pytest.mark.parametrize(
    ("schema_name", "source_key", "snapshot_id", "message"),
    [
        ("", "source-a", "snapshot-a", "schema_name"),
        ("mrf", "", "snapshot-a", "source_key"),
        ("mrf", "source-a", "", "source_key"),
    ],
)
def test_candidate_activation_requires_complete_local_identity(schema_name, source_key, snapshot_id, message) -> None:
    with pytest.raises(ValueError, match=message):
        source_pointers._normalized_candidate_activation_inputs(
            schema_name=schema_name,
            source_key=source_key,
            snapshot_id=snapshot_id,
        )


def _closure_layout() -> dict[str, object]:
    serving_by_field = {"storage_generation": closure.PTG2_V4_SHARED_GENERATION, "shared_snapshot_key": 9}
    return {
        "status": "validated",
        "state": "sealed",
        "generation": closure.PTG2_V4_SHARED_GENERATION,
        "map_root_state": "complete",
        "map_format": closure.PTG2_V4_MAP_FORMAT,
        "finalizer_root_state": "complete",
        "finalizer_contract": closure.PTG2_V4_FINALIZER_MAP_CONTRACT,
        "finalizer_map_format": closure.PTG2_V4_MAP_FORMAT,
        "manifest": {"serving_index": dict(serving_by_field)},
        "layout_manifest": {"serving_index": dict(serving_by_field)},
        "snapshot_key": 9,
        "mapping_digest": b"l" * 32,
        "map_digest": b"m" * 32,
        "finalizer_map_digest": b"f" * 32,
    }


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "message"),
    [
        ("status", "building", "completed PTG snapshot"),
        ("state", "building", "sealed layout"),
        ("generation", "legacy", "layout generation"),
        ("map_root_state", "building", "completed map root"),
        ("map_format", "unknown", "unknown format"),
        ("finalizer_root_state", "building", "completed finalizer root"),
        ("finalizer_contract", "unknown", "unknown contract"),
        ("manifest", None, "semantic metadata"),
        ("layout_manifest", None, "semantic metadata"),
    ],
)
def test_closure_rejects_incomplete_layout_authority(field_name, invalid_value, message) -> None:
    layout_by_field = _closure_layout()
    layout_by_field[field_name] = invalid_value
    with pytest.raises(closure.ResultArchiveClosureError, match=message):
        closure._validate_layout(layout_by_field)


@pytest.mark.parametrize("value", [None, True, "bad", -1])
def test_closure_rejects_invalid_nonnegative_bounds(value) -> None:
    with pytest.raises(closure.ResultArchiveClosureError, match="bound is invalid"):
        closure._required_nonnegative_int(value, "bound")


@pytest.mark.parametrize("pin", [None, {}, {"pin_id": "pin"}, {"repeatable_read_token": "token"}])
def test_closure_requires_both_retention_pin_fields(pin) -> None:
    with pytest.raises(closure.ResultArchiveClosureError, match="retention"):
        closure._required_pin(pin)


def _target_metadata(block_hash: bytes, *, stored_bytes: int = 3) -> dict[str, object]:
    return {
        "block_hash": block_hash,
        "format_version": closure.PTG2_V3_SHARED_FORMAT_VERSION,
        "object_kind": "provider",
        "codec": "none",
        "entry_count": 1,
        "raw_byte_count": stored_bytes,
        "stored_byte_count": stored_bytes,
    }


def test_closure_rejects_invalid_target_metadata_and_decoded_identity() -> None:
    block_hash = b"h" * 32
    invalid = _target_metadata(block_hash)
    invalid["codec"] = "unknown"
    with pytest.raises(closure.ResultArchiveClosureError, match="target block is invalid"):
        closure._validated_target_metadata(invalid, coordinate_identity={})
    with pytest.raises(closure.ResultArchiveClosureError, match="decoded target identity"):
        closure._validated_target_metadata(
            _target_metadata(block_hash),
            coordinate_identity={block_hash: ("other-kind", 1)},
        )


@pytest.mark.asyncio
async def test_closure_rejects_empty_reachability_and_dangling_targets(monkeypatch) -> None:
    with pytest.raises(closure.ResultArchiveClosureError, match="reachability exceeds"):
        await closure._validate_target_blocks(
            object(), schema='"mrf"', target_hashes=set(), coordinate_identity={}, max_block_hashes=1
        )
    monkeypatch.setattr(closure, "_load_target_block_metadata", AsyncMock(return_value={}))
    with pytest.raises(closure.ResultArchiveClosureError, match="dangling map targets"):
        await closure._validate_target_blocks(
            object(), schema='"mrf"', target_hashes={b"h" * 32}, coordinate_identity={}, max_block_hashes=1
        )


@pytest.mark.asyncio
async def test_closure_rejects_oversized_or_incomplete_payload_batch(monkeypatch) -> None:
    block_hash = b"h" * 32
    metadata = {block_hash: _target_metadata(block_hash)}
    with pytest.raises(closure.ResultArchiveClosureError, match="payload batch exceeds"):
        await closure._validate_payload_batch(object(), schema='"mrf"', payload_hashes=(), metadata_by_hash=metadata)
    monkeypatch.setattr(closure, "_load_payload_batch", AsyncMock(return_value={}))
    with pytest.raises(closure.ResultArchiveClosureError, match="payload length is inconsistent"):
        await closure._validate_payload_batch(
            object(), schema='"mrf"', payload_hashes=(block_hash,), metadata_by_hash=metadata
        )


def test_closure_rejects_wrong_payload_length_or_hash() -> None:
    block_hash = b"h" * 32
    metadata = {block_hash: _target_metadata(block_hash)}
    with pytest.raises(closure.ResultArchiveClosureError, match="payload length is inconsistent"):
        closure._authenticate_payload_batch(
            (block_hash,),
            payload_by_hash={block_hash: {"payload": b"xx", "payload_byte_count": 2}},
            metadata_by_hash=metadata,
        )
    with pytest.raises(closure.ResultArchiveClosureError, match="block hash is inconsistent"):
        closure._authenticate_payload_batch(
            (block_hash,),
            payload_by_hash={block_hash: {"payload": b"xxx", "payload_byte_count": 3}},
            metadata_by_hash=metadata,
        )


def test_closure_root_geometry_rejects_count_and_target_drift() -> None:
    pack_by_field = {
        "object_kind": "provider",
        "coordinate_count": 1,
        "entry_count": 1,
        "logical_byte_count": 3,
        "stored_byte_count": 3,
    }
    with pytest.raises(closure.ResultArchiveClosureError, match="root disagrees"):
        closure._validate_root_geometry({}, prefix="map", packs=(pack_by_field,))
    root_by_field = {
        "finalizer_object_kind_count": 1,
        "finalizer_map_pack_count": 1,
        "finalizer_coordinate_count": 1,
        "finalizer_entry_count": 1,
        "finalizer_logical_byte_count": 3,
        "finalizer_stored_map_byte_count": 3,
        "finalizer_target_block_count": 2,
    }
    with pytest.raises(closure.ResultArchiveClosureError, match="decoded targets"):
        closure._validate_root_geometry(root_by_field, prefix="finalizer", packs=(pack_by_field,), target_count=1)


def test_closure_basic_row_and_digest_guards_reject_missing_evidence() -> None:
    assert closure._mapping(None) == {}
    with pytest.raises(closure.ResultArchiveClosureError, match="missing or ambiguous"):
        closure._single([], "layout")
    with pytest.raises(closure.ResultArchiveClosureError, match="digest is invalid"):
        closure._required_bytes(b"short", "digest")

    missing_serving = _closure_layout()
    missing_serving["manifest"] = {}
    with pytest.raises(closure.ResultArchiveClosureError, match="no serving-index metadata"):
        closure._validate_layout(missing_serving)
    wrong_key = _closure_layout()
    wrong_key["manifest"]["serving_index"]["shared_snapshot_key"] = 8
    with pytest.raises(closure.ResultArchiveClosureError, match="does not bind this layout"):
        closure._validate_layout(wrong_key)


@pytest.mark.asyncio
async def test_closure_rejects_oversized_map_page_and_relational_reachability(monkeypatch) -> None:
    monkeypatch.setattr(closure, "_MAP_PACK_PAGE_ROWS", 1)
    session = SimpleNamespace(execute=AsyncMock(return_value=_QueryResult([{}, {}])))
    with pytest.raises(closure.ResultArchiveClosureError, match="map page exceeded"):
        await closure._map_pack_page(
            session,
            schema='"mrf"',
            snapshot_key=9,
            table_name="ptg2_v4_snapshot_map_pack",
            after_kind="",
            after_pack=-1,
        )

    monkeypatch.setattr(closure, "_relational_mapping_page", AsyncMock(return_value=()))
    assert (
        await closure._load_relational_mapping_blocks(
            object(),
            schema='"mrf"',
            snapshot_key=9,
            max_block_hashes=1,
            closure_block_hashes=set(),
        )
        == {}
    )

    mapping_record_by_field = {"block_hash": b"h" * 32, "object_kind": "", "entry_count": 1}
    monkeypatch.setattr(
        closure,
        "_relational_mapping_page",
        AsyncMock(return_value=(mapping_record_by_field,)),
    )
    with pytest.raises(closure.ResultArchiveClosureError, match="relational mapping is invalid"):
        await closure._load_relational_mapping_blocks(
            object(),
            schema='"mrf"',
            snapshot_key=9,
            max_block_hashes=1,
            closure_block_hashes=set(),
        )

    mapping_record_by_field["object_kind"] = "provider"
    with pytest.raises(closure.ResultArchiveClosureError, match="reachability exceeds"):
        await closure._load_relational_mapping_blocks(
            object(),
            schema='"mrf"',
            snapshot_key=9,
            max_block_hashes=0,
            closure_block_hashes=set(),
        )


def _map_pack(payload: bytes = b"not-a-map") -> dict[str, object]:
    return {
        "format_version": closure.PTG2_V3_SHARED_FORMAT_VERSION,
        "map_object_kind": closure.PTG2_V4_MAP_BLOCK_KIND,
        "codec": "none",
        "raw_byte_count": len(payload),
        "stored_byte_count": len(payload),
        "payload": payload,
        "map_block_hash": closure.shared_block_hash(
            format_version=closure.PTG2_V3_SHARED_FORMAT_VERSION,
            object_kind=closure.PTG2_V4_MAP_BLOCK_KIND,
            codec="none",
            payload=payload,
        ),
        "object_kind": "provider",
        "coordinate_count": 1,
        "entry_count": 1,
    }


def test_closure_rejects_invalid_or_undecodable_map_pack(monkeypatch) -> None:
    invalid = _map_pack()
    invalid["codec"] = "zstd"
    with pytest.raises(closure.ResultArchiveClosureError, match="map pack block is invalid"):
        closure._append_map_pack_targets(invalid, target_identity_by_hash={})

    monkeypatch.setattr(
        closure,
        "decode_v4_snapshot_map_pack",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("invalid map")),
    )
    with pytest.raises(closure.ResultArchiveClosureError, match="cannot be decoded"):
        closure._append_map_pack_targets(_map_pack(), target_identity_by_hash={})

    coordinate = SimpleNamespace(block_hash=b"t" * 32, object_kind="provider", entry_count=1)
    monkeypatch.setattr(closure, "decode_v4_snapshot_map_pack", lambda *_args, **_kwargs: (coordinate,))
    inconsistent = _map_pack()
    inconsistent["entry_count"] = 2
    with pytest.raises(closure.ResultArchiveClosureError, match="geometry is inconsistent"):
        closure._append_map_pack_targets(inconsistent, target_identity_by_hash={})


def test_closure_payload_batching_honors_native_and_aggregate_byte_bounds(monkeypatch) -> None:
    monkeypatch.setattr(closure, "_MAX_TARGET_BLOCK_PAYLOAD_BYTES", 10)
    with pytest.raises(closure.ResultArchiveClosureError, match="native byte limit"):
        closure._payload_hash_batches({b"a" * 32: {"stored_byte_count": 11}})

    monkeypatch.setattr(closure, "_PAYLOAD_BATCH_BYTES", 5)
    batches = closure._payload_hash_batches(
        {
            b"a" * 32: {"stored_byte_count": 3},
            b"b" * 32: {"stored_byte_count": 6},
            b"c" * 32: {"stored_byte_count": 3},
            b"d" * 32: {"stored_byte_count": 3},
        }
    )
    assert batches == ((b"a" * 32,), (b"b" * 32,), (b"c" * 32,), (b"d" * 32,))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("snapshot_id", "max_block_hashes", "message"),
    [("", 1, "snapshot_id is required"), ("snapshot", 0, "must be positive")],
)
async def test_closure_selection_rejects_unbounded_inputs(snapshot_id, max_block_hashes, message) -> None:
    with pytest.raises(ValueError, match=message):
        await closure.select_result_archive_closure(
            object(),
            schema_name="mrf",
            snapshot_id=snapshot_id,
            retention_pin={"pin_id": "pin", "repeatable_read_token": "token"},
            max_block_hashes=max_block_hashes,
        )


@pytest.mark.asyncio
async def test_closure_rejects_incomplete_finalizer_kinds(monkeypatch) -> None:
    monkeypatch.setattr(closure, "_validate_root_geometry", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(closure, "_validate_finalizer_targets", AsyncMock())
    with pytest.raises(closure.ResultArchiveClosureError, match="object kinds are incomplete"):
        await closure._validate_decoded_map_selection(
            object(),
            schema='"mrf"',
            snapshot_key=9,
            layout_by_field={},
            map_selection=(({"object_kind": "provider"},), {}),
            finalizer_selection=(({"object_kind": "unexpected"},), {}),
            relational_target_identity_by_hash={},
        )


@pytest.mark.asyncio
async def test_closure_rejects_decoded_reachability_not_present_in_selection(monkeypatch) -> None:
    map_pack_by_field = {"map_block_hash": b"m" * 32, "object_kind": "provider"}
    finalizer_pack_by_field = {"map_block_hash": b"f" * 32, "object_kind": "provider"}
    monkeypatch.setattr(
        closure,
        "_load_map_blocks",
        AsyncMock(side_effect=[((map_pack_by_field,), {}), ((finalizer_pack_by_field,), {})]),
    )
    monkeypatch.setattr(closure, "_load_relational_mapping_blocks", AsyncMock(return_value={}))
    monkeypatch.setattr(closure, "_validate_decoded_map_selection", AsyncMock(return_value={}))
    with pytest.raises(closure.ResultArchiveClosureError, match="decoded block reachability is inconsistent"):
        await closure._archive_block_selection(
            object(),
            schema='"mrf"',
            snapshot_key=9,
            layout_by_field={},
            max_block_hashes=10,
        )
