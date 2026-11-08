# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The native-admin cut grants fixed control authority, never payload hooks."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_source_control_privileges as privileges
from process.provider_directory_generic_source_policy import GenericSourcePolicy


@pytest.mark.asyncio
async def test_fixed_admission_rights_have_no_payload_or_function_ownership_changes():
    connection = SimpleNamespace(execute=AsyncMock())
    policy = SimpleNamespace(schema="source_schema", owner_role="source_owner")
    await privileges.grant_source_admission_controls(connection, policy)
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert len(statements) == 11
    assert statements[0] == (
        'GRANT SELECT(endpoint_id),UPDATE(endpoint_id) ON "source_schema"."provider_directory_api_endpoint" '
        'TO "source_owner"'
    )
    assert statements[1] == (
        "GRANT SELECT(source_id,endpoint_id,metadata_json),UPDATE(source_id) ON "
        '"source_schema"."provider_directory_source" TO "source_owner"'
    )
    assert statements[3:5] == [
        f'GRANT SELECT,SELECT({privileges._CONTROL_READ_COLUMNS[root]}) ON "source_schema"."{root}" TO "source_owner"'
        for root in privileges.SOURCE_CONTROL_READS
    ]
    assert statements[5] == (
        "GRANT SELECT(dataset_id,endpoint_id,acquisition_root_run_id,vector_sha256,prior_status,observed_at),"
        "INSERT(dataset_id,endpoint_id,acquisition_root_run_id,vector_sha256,prior_status,dataset_hash) "
        'ON "source_schema"."provider_directory_cms_npd_stale_candidate" TO "source_owner"'
    )
    assert all(statement.startswith("GRANT EXECUTE ON FUNCTION") for statement in statements[6:])
    assert len(privileges.METADATA_FUNCTIONS) == 5
    assert all(
        not any(operation in statement for operation in ("ALTER", "CREATE", "COPY", "TRIGGER"))
        for statement in statements
    )


def test_claim_query_control_dependencies_match_admission_rights(monkeypatch):
    import importlib
    import re

    fhir = importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(fhir, "_schema", lambda: "source_schema")
    query = fhir._locked_endpoint_verification_state_sql('"source_schema"."provider_directory_endpoint_dataset"')
    assert set(re.findall(r'"source_schema"\."([^"]+)"', query)) == {
        "provider_directory_endpoint_dataset",
        "provider_directory_source",
        "provider_directory_cms_npd_stale_candidate",
        "provider_directory_endpoint_dataset_admission_metadata_sha256",
        "provider_directory_subset_canonical_sha256",
        "provider_directory_subset_content_proof_valid",
    }


@pytest.mark.asyncio
async def test_existing_profile_repair_includes_disposition_insert(monkeypatch):
    from process import plan_release_storage_custody as native
    from process import provider_directory_generic_source_provision as family

    @asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(native, "storage_roles", AsyncMock())
    monkeypatch.setattr(family, "verify_generic_source_family", AsyncMock())
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=transaction,
        execute=AsyncMock(),
        fetchrow=AsyncMock(
            return_value={
                "session": "native_admin",
                "current": "native_admin",
                "native_admin": True,
            }
        ),
    )
    verify = AsyncMock()
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    await privileges.provision_source_control_reads(connection, policy, verify_role_profile=verify)
    assert verify.await_count == 2
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert len(statements) == 11
    assert statements[:3] == [
        'GRANT SELECT(endpoint_id),UPDATE(endpoint_id) ON "source_schema"."provider_directory_api_endpoint" TO "source_owner"',
        'GRANT SELECT(source_id,endpoint_id,metadata_json),UPDATE(source_id) ON "source_schema"."provider_directory_source" TO "source_owner"',
        'GRANT SELECT(run_id,retry_of_run_id,engine,node_id,importer,status,finished_at,params),UPDATE(run_id) ON "source_schema"."import_run" TO "source_owner"',
    ]
    assert statements[5] == (
        "GRANT SELECT(dataset_id,endpoint_id,acquisition_root_run_id,vector_sha256,prior_status,observed_at),"
        "INSERT(dataset_id,endpoint_id,acquisition_root_run_id,vector_sha256,prior_status,dataset_hash) "
        'ON "source_schema"."provider_directory_cms_npd_stale_candidate" TO "source_owner"'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "login,current,admin",
    [
        ("source_publisher", "source_publisher", False),
        ("native_admin", "source_owner", True),
        ("source_owner", "source_owner", True),
    ],
)
async def test_control_read_upgrade_denies_ordinary_or_substituted_admin(login, current, admin):
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value={"session": login, "current": current, "native_admin": admin}),
        execute=AsyncMock(side_effect=AssertionError("grant reached")),
    )
    verify = AsyncMock(side_effect=AssertionError("profile reached"))
    with pytest.raises(RuntimeError, match="source_control_independent_admin_required"):
        await privileges.provision_source_control_reads(connection, policy, verify_role_profile=verify)
    verify.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_post", [False, True])
async def test_read_upgrade_checks_family_before_and_after(monkeypatch, fail_post):
    from process import plan_release_storage_custody as native
    from process import provider_directory_generic_source_provision as family

    events = []

    @asynccontextmanager
    async def savepoint():
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise

    async def verify_family(*_args):
        events.append("family")
        if fail_post and events.count("family") == 2:
            raise RuntimeError("family_changed")

    async def grant_admission(*_args):
        events.append("admission")

    async def verify_profile(*_args, **_kwargs):
        events.append("profile")

    monkeypatch.setattr(native, "storage_roles", AsyncMock())
    monkeypatch.setattr(family, "verify_generic_source_family", verify_family)
    monkeypatch.setattr(privileges, "grant_source_admission_controls", grant_admission)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=savepoint,
        fetchrow=AsyncMock(
            return_value={
                "session": "native_admin",
                "current": "native_admin",
                "native_admin": True,
            }
        ),
    )
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    if fail_post:
        with pytest.raises(RuntimeError, match="family_changed"):
            await privileges.provision_source_control_reads(connection, policy, verify_role_profile=verify_profile)
        assert events == ["profile", "family", "admission", "family", "rollback"]
    else:
        await privileges.provision_source_control_reads(connection, policy, verify_role_profile=verify_profile)
        assert events == ["profile", "family", "admission", "family", "profile"]
