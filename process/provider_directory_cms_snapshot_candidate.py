# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attach two sealed CMS witnesses and allocate the noncurrent generic header.

This is a data primitive, not the protected finisher. Its original-admin caller
authenticates the CMS profile/claim, closed writers and complete content, owns
the bounded cut and writes terminal accounting before the same outer commit.
"""

from db.plan_release_storage import storage_relation
from process import provider_directory_cms_prepared_candidate as candidate
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_cms_snapshot_attach as attachment
from process.provider_directory_generic_source_completion import _frozen_handoff_header

CONTRACT = "cms-attached-noncurrent-candidate-data-v1"


def _require(condition, code):
    if not condition:
        raise ValueError("cms_snapshot_candidate_" + code)


async def _header(connection, request_by_name):
    handoff = request_by_name["handoff"]
    policy = workspace._policy(request_by_name["prepared_inputs"]["prepared_policy"])
    header = candidate._header(await _frozen_handoff_header(connection, policy, handoff.document))
    _require(header["published_at"] is None and header["superseded_at"] is None, "frozen_header_changed")
    frozen = storage_relation(policy.schema, handoff.document["tables"][workspace.storage.HEADER]["name"])
    canonical = await candidate._allocate_header(
        connection, request_by_name["source_binding"].catalog_schema, frozen, header
    )
    actual = candidate._header(
        await connection.fetchrow(f"SELECT * FROM {canonical} WHERE dataset_id=$1", header["dataset_id"])
    )
    _require(
        actual == {**header, "status": "acquiring", "validated_at": None, "candidate_storage_json": None},
        "header_changed",
    )
    return actual


async def attach_cms_snapshot_candidate(connection, request_by_name, *, expected_seal, expected_parent_oids):
    """Return data evidence only; a failed header insert also undoes both ATTACHes.

    Parent DML/cascade/callable/role/DDL custody and protected completion remain
    external requirements. This does not mark the header validated, move serving
    pointers, issue authority or commit. The original generic allocator is unchanged; its four empty candidate leaves
    still require genuine bounded COPY and complete parity before publication.
    """
    _require(connection.is_in_transaction() and not connection.is_closed(), "transaction_required")
    _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    request, retained = attachment._request(request_by_name)
    seal = workspace._copy(expected_seal)
    parents = workspace._copy(expected_parent_oids)
    actor = await attachment._actor(connection, request, retained)
    async with workspace._savepoint(connection):
        snapshots = await attachment.attach_sealed_cms_payload_snapshots(
            connection, request, expected_seal=seal, expected_parent_oids=parents
        )
        header = await _header(connection, request)
        _require(await attachment._actor(connection, request, retained) == actor, "transaction_changed")
        _require(
            attachment._request(request_by_name) == (request, retained)
            and expected_seal == seal
            and expected_parent_oids == parents,
            "inputs_changed",
        )
    return {"contract": CONTRACT, "header": header, "snapshots": snapshots}
