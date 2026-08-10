# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Persistence assertions shared by rooted-graph PostgreSQL lifecycles."""

import hashlib

import pytest
from sqlalchemy.exc import DBAPIError

from process.provider_directory_rooted_graph_query import (
    build_provider_directory_practitioner_role_query,
)
from process.provider_directory_rooted_graph_store_contract import (
    build_provider_directory_rooted_graph_work_spec,
)
from process.provider_directory_rooted_graph_store_support import (
    insert_work_spec,
    set_store_action,
    table_ref,
)
from tests.formulary_fhir_twin_admission_pg_support import assert_sqlstate
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.provider_directory_rooted_graph_pg_support import API_BASE
from tests.provider_directory_rooted_graph_pg_support import EDGE_TABLE
from tests.provider_directory_rooted_graph_pg_support import RESOURCE_TABLE
from tests.provider_directory_rooted_graph_pg_support import ROOT_RESOURCE_ID


async def assert_setwise_root_query_identity(database, identity) -> None:
    """Prove SQL and Python produce the same canonical root-query identity."""

    expected = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_provider_directory_practitioner_role_query(
            API_BASE,
            ROOT_RESOURCE_ID,
        ),
        closure_scope="root",
    )
    row = await database.first(
        "SELECT query_id, query_identity_sha256, query_identity_json_text "
        f"FROM {table_ref('provider_directory_rooted_graph_work')} "
        "WHERE acquisition_id = :acquisition_id AND reference_id = :reference_id",
        acquisition_id=identity.acquisition_id,
        reference_id=ROOT_RESOURCE_ID,
    )
    assert row is not None
    assert row.query_id == expected.query_id
    assert row.query_identity_sha256 == expected.query_identity_sha256
    assert row.query_identity_json_text == expected.query_identity_json_text


async def assert_separate_registration_is_rejected(
    database,
    identity,
    direct_spec,
) -> None:
    """Prove derived work cannot lag its parent's terminal transaction."""

    with pytest.raises(DBAPIError, match="discovery_invalid"):
        async with database.transaction():
            await set_store_action(database, "derive", identity.acquisition_id)
            await insert_work_spec(database, identity.acquisition_id, direct_spec)


async def assert_missing_terminal_witnesses(connection, schema_name: str) -> None:
    """Prove absent exact reads are completed, hashed, non-error witnesses."""

    schema = quoted(schema_name)
    rows = await connection.fetch(
        "SELECT status, missing_http_status, missing_response_sha256, "
        "missing_response_bytes, missing_response_json_text, "
        f"terminal_record_sha256 FROM {schema}.provider_directory_rooted_graph_work "
        "WHERE resource_type = 'Endpoint' ORDER BY acquisition_id"
    )
    assert len(rows) == 2
    assert all(row["status"] == "completed" for row in rows)
    assert all(row["missing_http_status"] == 404 for row in rows)
    assert all(len(row["terminal_record_sha256"]) == 64 for row in rows)
    for row in rows:
        encoded_response = row["missing_response_json_text"].encode("utf-8")
        assert row["missing_response_bytes"] == len(encoded_response)
        assert (
            row["missing_response_sha256"]
            == hashlib.sha256(encoded_response).hexdigest()
        )


async def assert_witness_immutability(connection, schema_name: str) -> None:
    """Prove retained resource and edge witnesses reject mutation."""

    schema = quoted(schema_name)
    await assert_sqlstate(
        connection,
        "55000",
        f"UPDATE {schema}.{RESOURCE_TABLE} SET payload_sha256 = '{'0' * 64}'",
    )
    await assert_sqlstate(
        connection,
        "55000",
        f"UPDATE {schema}.{EDGE_TABLE} SET edge_sha256 = '{'0' * 64}'",
    )


__all__ = (
    "assert_missing_terminal_witnesses",
    "assert_separate_registration_is_rejected",
    "assert_setwise_root_query_identity",
    "assert_witness_immutability",
)
