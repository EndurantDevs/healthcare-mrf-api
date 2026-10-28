# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit queue families fail closed before a protected claim can run."""

from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process.registry_publication_queue import claim_registry_publication_request


class _Connection:
    def __init__(self, *, in_transaction=True):
        self.in_transaction = in_transaction
        self.fetchrow = AsyncMock(return_value=None)

    def is_in_transaction(self):
        return self.in_transaction


@pytest.mark.asyncio
@pytest.mark.parametrize("family", [None, "", "all", "publication ", "Publication", "office_prepare", True, 0, [], {}])
async def test_unknown_or_malformed_family_refuses_before_claim(family):
    connection = _Connection()
    with pytest.raises(ValueError, match="registry_queue_operation_family_invalid"):
        await claim_registry_publication_request(connection, operation_family=family)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_omitted_family_cannot_fall_back_to_unfiltered_claim():
    connection = _Connection()
    with pytest.raises(TypeError, match="operation_family"):
        await claim_registry_publication_request(connection)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("request_by_name", [{}, {"request_id": UUID(int=1)}])
@pytest.mark.parametrize(
    "family,ordinary,operations",
    [
        (
            "publication",
            True,
            (
                "reactivate_generation",
                "prepare_source_rollback",
                "publish_source_rollback",
                "publish_initial_inventory",
            ),
        ),
        ("office", False, ("prepare_ptg_office_capture", "approve_ptg_office_capture")),
    ],
)
async def test_family_claim_has_closed_parameters_for_queued_and_expired_rows(
    family, ordinary, operations, request_by_name
):
    connection = _Connection()
    assert (
        await claim_registry_publication_request(
            connection, operation_family=family, control_schema="registry", **request_by_name
        )
        is None
    )
    connection.fetchrow.assert_awaited_once()
    query, lease_id, seconds, accepts_absent_operation, accepted_operations, selected_request_id = (
        connection.fetchrow.await_args.args
    )
    assert isinstance(lease_id, UUID) and lease_id.int and seconds == 300
    assert accepts_absent_operation is ordinary and accepted_operations == operations
    assert selected_request_id == request_by_name.get("request_id")
    assert "WHERE (state='queued' OR state='running' AND lease_expires_at<=clock_timestamp())" in query
    assert "AND ($5::uuid IS NULL OR request_id=$5)" in query
    assert "jsonb_typeof(command_json->'operation')='string'" in query
    assert "NOT (command_json ? 'operation')" in query
    assert "ORDER BY created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1" in query


@pytest.mark.asyncio
@pytest.mark.parametrize("request_id", [UUID(int=0), "00000000-0000-0000-0000-000000000001", 1, True])
async def test_malformed_request_identity_refuses_before_claim(request_id):
    connection = _Connection()
    with pytest.raises(ValueError, match="registry_queue_request_identity_invalid"):
        await claim_registry_publication_request(connection, operation_family="publication", request_id=request_id)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [True, 0, 29, 901, "300"])
async def test_partition_preserves_lease_refusal_before_sql(seconds):
    connection = _Connection()
    with pytest.raises(ValueError, match="registry_queue_lease_invalid"):
        await claim_registry_publication_request(connection, operation_family="publication", lease_seconds=seconds)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_partition_still_requires_caller_owned_transaction():
    connection = _Connection(in_transaction=False)
    with pytest.raises(ValueError, match="registry_queue_lease_invalid"):
        await claim_registry_publication_request(connection, operation_family="publication")
    connection.fetchrow.assert_not_awaited()
