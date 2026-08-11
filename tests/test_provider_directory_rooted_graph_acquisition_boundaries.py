# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed acquisition contract and orchestration boundaries."""

from __future__ import annotations

from dataclasses import replace
import math

import pytest

from process.provider_directory_rooted_graph_acquisition import (
    ProviderDirectoryRootedGraphAcquisitionConfig,
    ProviderDirectoryRootedGraphAcquisitionError,
    ProviderDirectoryRootedGraphAcquisitionReceipt,
    ProviderDirectoryRootedGraphRootReceipt,
    _initialize_revalidated_root,
    _require_sealed_roots,
    _revalidated_snapshot,
    _root_receipt,
    _runtime_dependencies,
    _validate_root_pair,
)
from process.provider_directory_rooted_graph_acquisition_contract import (
    strict_nonnegative_seconds,
)
from process.provider_directory_rooted_graph_acquisition_runtime import (
    _locked_acquisition_status,
    default_dependencies,
    default_session_scope,
    provider_directory_rooted_graph_census_state,
    revalidate_provider_directory_rooted_graph_inputs,
    run_root,
)
from process.provider_directory_rooted_graph_store_support import identity_fields
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    identity,
    snapshot,
)
from tests.provider_directory_rooted_graph_runtime_test_support import (
    RuntimeHarness,
    enabled_config,
)


@pytest.mark.parametrize("invalid_seconds", (True, "1", -1, math.inf))
def test_duration_contract_rejects_nonfinite_or_non_numeric_values(
    invalid_seconds: object,
) -> None:
    with pytest.raises(ValueError, match="duration_invalid"):
        strict_nonnegative_seconds(invalid_seconds, "duration")


def test_config_wraps_invalid_http_bounds() -> None:
    with pytest.raises(ValueError, match="acquisition_config_invalid"):
        ProviderDirectoryRootedGraphAcquisitionConfig(max_page_bytes=0)


@pytest.mark.parametrize(
    "changes",
    (
        {"api_base": "http://directory.synthetic.test/fhir/R4"},
        {"root_dataset_id": ""},
    ),
)
def test_input_snapshot_rejects_transport_or_root_drift(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="input_snapshot_invalid"):
        replace(snapshot(), **changes)


def _root_receipt_for(role: str) -> ProviderDirectoryRootedGraphRootReceipt:
    root = identity(role)
    return ProviderDirectoryRootedGraphRootReceipt(
        acquisition_role=role,
        acquisition_id=root.acquisition_id,
        run_id=root.run_id,
        completed_count=1,
        resource_count=1,
        edge_count=0,
        rooted_graph_sha256="4" * 64,
        elapsed_seconds=0.25,
    )


def test_receipts_reject_invalid_root_or_pair_evidence() -> None:
    with pytest.raises(ValueError, match="root_receipt_invalid"):
        replace(_root_receipt_for("baseline"), acquisition_role="other")
    with pytest.raises(ValueError, match="root_elapsed_seconds_invalid"):
        replace(_root_receipt_for("baseline"), elapsed_seconds=-1)

    baseline = _root_receipt_for("baseline")
    candidate = _root_receipt_for("candidate")
    with pytest.raises(ValueError, match="receipt_invalid"):
        ProviderDirectoryRootedGraphAcquisitionReceipt(
            scope_id=identity().scope_id,
            dataset_intent_id=identity().dataset_intent_id,
            baseline=baseline,
            candidate=candidate,
            rooted_graphs_match=False,
            elapsed_seconds=1,
        )


def test_pair_and_runtime_dependencies_reject_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="root_pair_invalid"):
        _validate_root_pair(identity("baseline"), object())
    with pytest.raises(ValueError, match="config_invalid"):
        _runtime_dependencies(object(), None)
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as disabled:
        _runtime_dependencies(ProviderDirectoryRootedGraphAcquisitionConfig(), None)
    assert disabled.value.code == "disabled"
    with pytest.raises(ValueError, match="dependencies_invalid"):
        _runtime_dependencies(enabled_config(), object())


@pytest.mark.asyncio
async def test_revalidation_rejects_snapshot_and_expected_source_drift() -> None:
    harness = RuntimeHarness()
    dependencies = harness.dependencies()

    async def invalid_snapshot(*_args, **_kwargs):
        return object()

    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as invalid:
        await _revalidated_snapshot(
            identity(),
            dependencies=replace(dependencies, revalidate_inputs=invalid_snapshot),
            database=object(),
        )
    assert invalid.value.code == "input_drift"

    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as drift:
        await _revalidated_snapshot(
            identity(),
            dependencies=dependencies,
            database=object(),
            expected_source=("different",),
        )
    assert drift.value.code == "input_drift"


@pytest.mark.asyncio
async def test_initialization_and_final_fence_reject_invalid_state() -> None:
    harness = RuntimeHarness()
    dependencies = harness.dependencies()

    async def invalid_created_count(*_args, **_kwargs):
        return 2

    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as created:
        await _initialize_revalidated_root(
            identity(),
            dependencies=replace(
                dependencies,
                initialize_root=invalid_created_count,
            ),
            database=object(),
            expected_source=snapshot().source_identity(),
        )
    assert created.value.code == "state"

    async def no_create(*_args, **_kwargs):
        return 0

    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as status:
        await _initialize_revalidated_root(
            identity(),
            dependencies=replace(dependencies, initialize_root=no_create),
            database=object(),
            expected_source=snapshot(status="absent").source_identity(),
        )
    assert status.value.code == "state"

    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as final:
        await _require_sealed_roots(
            (identity(),),
            dependencies=dependencies,
            database=object(),
            expected_source=snapshot().source_identity(),
        )
    assert final.value.code == "state"


@pytest.mark.asyncio
async def test_root_receipt_rejects_error_summary() -> None:
    harness = RuntimeHarness()
    summary = await harness.seal_root(identity("candidate"), database=object())
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as error_info:
        _root_receipt(identity(), summary, 0.25)
    assert error_info.value.code == "state"


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _Database:
    def __init__(self, row: object) -> None:
        self.row = row

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def scalar(self, *_args, **_kwargs):
        return None

    async def first(self, *_args, **_kwargs):
        return self.row


@pytest.mark.asyncio
async def test_locked_status_returns_absent_after_current_root_fence() -> None:
    async def select_current(_database, *, pair):
        return object()

    status = await _locked_acquisition_status(
        identity(),
        _Database(None),
        "synthetic-lock",
        lambda: object(),
        select_current,
        lambda _current, _identity: True,
    )
    assert status == "absent"


@pytest.mark.asyncio
async def test_locked_status_revalidates_existing_acquisition_row() -> None:
    async def select_current(_database, *, pair):
        return object()

    root = identity()
    status = await _locked_acquisition_status(
        root,
        _Database({**identity_fields(root), "status": "building"}),
        "synthetic-lock",
        lambda: object(),
        select_current,
        lambda _current, _identity: True,
    )
    assert status == "building"


@pytest.mark.asyncio
async def test_public_revalidation_rejects_nonidentity_input() -> None:
    with pytest.raises(ValueError, match="identity_invalid"):
        await revalidate_provider_directory_rooted_graph_inputs(object())


@pytest.mark.parametrize(
    ("row", "expected"),
    (
        ({"census_count": 0, "census_status": None}, "absent"),
        ({"census_count": 1, "census_status": "pending"}, "pending"),
    ),
)
@pytest.mark.asyncio
async def test_census_state_accepts_exact_absent_or_singleton(
    row: dict[str, object],
    expected: str,
) -> None:
    assert (
        await provider_directory_rooted_graph_census_state(
            identity().acquisition_id,
            database=_Database(row),
        )
        == expected
    )


@pytest.mark.asyncio
async def test_census_state_rejects_duplicate_or_unknown_rows() -> None:
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as error_info:
        await provider_directory_rooted_graph_census_state(
            identity().acquisition_id,
            database=_Database({"census_count": 2, "census_status": "pending"}),
        )
    assert error_info.value.code == "state"


def test_default_dependency_surface_is_fully_callable() -> None:
    dependencies = default_dependencies()
    assert callable(dependencies.fetch)
    assert callable(dependencies.session_scope)


@pytest.mark.asyncio
async def test_default_session_is_isolated_and_identity_encoded() -> None:
    async with default_session_scope(2) as session:
        assert session.connector.limit == 2
        assert session.auto_decompress is False
        assert session.trust_env is False
        assert session.headers["Accept-Encoding"] == "identity"


@pytest.mark.asyncio
async def test_run_root_rejects_non_summary_seal_result() -> None:
    harness = RuntimeHarness()
    root = identity()
    await harness.initialize_root(root, database=object())

    async def invalid_summary(*_args, **_kwargs):
        return object()

    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as error_info:
        await run_root(
            root,
            snapshot(),
            config=enabled_config(),
            dependencies=replace(harness.dependencies(), seal_root=invalid_summary),
            database=object(),
        )
    assert error_info.value.code == "state"
