# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A closed historical edition never hides a pending contender."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_source_candidate_admission as admission


def candidate(dataset_id="previous", status="validated"):
    return dict(
        dataset_id=dataset_id,
        endpoint_id="endpoint-a",
        acquisition_root_run_id="root-a",
        import_run_id="run-a",
        status=status,
        dataset_hash="a" * 64,
        resource_count=3,
        completion_proof_sha256="b" * 64,
        has_admission_receipt=True,
        retired_candidate=False,
        publication_metadata_json=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [False, True, None, "true"])
async def test_only_original_factory_boolean_can_classify_completed_candidate(monkeypatch, result):
    verify = AsyncMock(return_value=result)
    publisher = SimpleNamespace(
        connection=object(), policy=object(), factory=SimpleNamespace(has_published_source_candidate=verify)
    )
    monkeypatch.setattr(admission, "current_source_storage_binding", lambda: object())
    monkeypatch.setattr(admission, "bound_publisher", lambda: publisher)
    if type(result) is bool:
        assert await admission._has_published_candidate(candidate()) is result
    else:
        with pytest.raises(RuntimeError, match="admission_changed"):
            await admission._has_published_candidate(candidate())
    assert verify.await_args.args == (publisher.connection, publisher.policy)
    assert set(verify.await_args.kwargs["candidate_by_field"]) == set(admission._CANDIDATE_FIELDS)


@pytest.mark.asyncio
async def test_changed_publisher_cannot_accept_a_historical_lookup(monkeypatch):
    publisher = SimpleNamespace(
        connection=object(),
        policy=object(),
        factory=SimpleNamespace(has_published_source_candidate=AsyncMock(return_value=True)),
    )
    monkeypatch.setattr(admission, "current_source_storage_binding", lambda: object())
    monkeypatch.setattr(admission, "bound_publisher", Mock(side_effect=[publisher, object()]))
    with pytest.raises(RuntimeError, match="admission_changed"):
        await admission._has_published_candidate(candidate())


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", [None, object()])
async def test_legacy_or_unintegrated_factory_preserves_blocking_admission(monkeypatch, binding):
    monkeypatch.setattr(admission, "current_source_storage_binding", lambda: binding)
    monkeypatch.setattr(admission, "bound_publisher", lambda: SimpleNamespace(factory=object()))
    assert await admission._has_published_candidate(candidate()) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("next_state", [None, "validated", "acquiring", "incomplete"])
async def test_completed_candidate_is_skipped_but_next_pending_candidate_stays_locked(monkeypatch, next_state):
    previous = candidate()
    next_candidate = candidate("next", next_state) if next_state else None
    directory = SimpleNamespace(
        _private_verification_state_sql=lambda relation: relation,
        _pagination_checkpoint_row_mapping=lambda row: dict(row) if row else {},
        _is_private_candidate_blocking=lambda row: True,
        ENDPOINT_DATASET_VALIDATED="validated",
    )
    observations = []

    async def first(query, **params):
        observations.append(list(params["rejected_dataset_ids"]))
        return previous if len(observations) == 1 else next_candidate

    verify = AsyncMock(side_effect=lambda row: row["dataset_id"] == "previous")
    monkeypatch.setattr(admission, "_has_published_candidate", verify)
    selected = await admission.locked_private_verification_state(
        directory, SimpleNamespace(first=first), "private_header", {"include_twin_states": False}
    )
    assert observations == [[], ["previous"]]
    assert (selected.get("dataset_id") if selected else None) == ("next" if next_state else None)
    assert verify.await_count == (2 if next_state == "validated" else 1)


@pytest.mark.asyncio
async def test_twin_verification_never_uses_historical_closure(monkeypatch):
    directory = SimpleNamespace(
        _private_verification_state_sql=lambda relation: relation,
        _pagination_checkpoint_row_mapping=dict,
        _is_private_candidate_blocking=lambda row: True,
        ENDPOINT_DATASET_VALIDATED="validated",
    )
    verify = AsyncMock(return_value=True)
    monkeypatch.setattr(admission, "_has_published_candidate", verify)
    selected = await admission.locked_private_verification_state(
        directory, SimpleNamespace(first=AsyncMock(return_value=candidate())), "header", {"include_twin_states": True}
    )
    assert selected["dataset_id"] == "previous"
    verify.assert_not_awaited()
