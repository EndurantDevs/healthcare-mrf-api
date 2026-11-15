# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native state locks retain the composite-publication boundary for CMS history."""

import pytest

from process import entity_address_snapshot_preparation as preparation
from tests.test_entity_address_snapshot_preparation import _session


def _state_records():
    return [
        {"name": name, "supported": True}
        for name in ("entity_address_geo_assurance_state", "entity_address_result_generation")
    ]


def _state_session(*, receipt_exists=False, populated=False):
    session = _session()
    session.execute.return_value.mappings.return_value.all.return_value = _state_records()
    session.scalar.side_effect = [receipt_exists, populated] if receipt_exists else [False]
    return session


async def test_cms_guard_bodies_match_actual_migration():
    """Ordinary state locking does not install, remove or replace any database hook."""
    session = _state_session()
    await preparation._lock_publication_state(session, "example")
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert not any(
        command in statement
        for statement in statements
        for command in ("CREATE FUNCTION", "CREATE TRIGGER", "DROP TRIGGER", "ALTER ROLE")
    )


async def test_migrated_hooks_are_exact_oid_exemptions_after_receipt_lock():
    """Lock existing receipt history before permitting an empty-history publication."""
    session = _state_session(receipt_exists=True)
    await preparation._lock_publication_state(session, "example")
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0].endswith("IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    assert statements[-1] == 'LOCK TABLE ONLY "example"."provider_directory_cms_serving_receipt" IN SHARE MODE NOWAIT'
    assert (
        str(session.scalar.await_args.args[0])
        == 'SELECT EXISTS(SELECT 1 FROM "example"."provider_directory_cms_serving_receipt")'
    )


async def test_absent_cms_history_does_not_admit_any_hook():
    session = _state_session()
    await preparation._lock_publication_state(session, "example")
    assert session.execute.await_count == 2
    assert session.scalar.await_count == 1
    assert not any(
        "provider_directory_cms_serving_receipt" in str(call.args[0]) for call in session.execute.await_args_list
    )


@pytest.mark.parametrize("damage", ["missing", "extra", "renamed", "row_security", "partition", "temporary"])
async def test_cms_guard_damage_refuses_publication(damage):
    """Incomplete or unsupported native state prevents reaching the CMS history decision."""
    records = _state_records()
    if damage == "missing":
        records.pop()
    elif damage == "extra":
        records.append({"name": "unexpected", "supported": True})
    elif damage == "renamed":
        records[0]["name"] = "substituted"
    else:
        records[0]["supported"] = False
    session = _state_session(receipt_exists=True)
    session.execute.return_value.mappings.return_value.all.return_value = records
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="state layout"):
        await preparation._lock_publication_state(session, "example")
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("safe,populated", [(False, False), (None, False), (True, True), (True, None)])
async def test_unsupported_or_populated_receipts_refuse_before_guard_admission(safe, populated):
    session = _state_session(receipt_exists=True, populated=populated)
    if safe is not True:
        session.execute.return_value.mappings.return_value.all.return_value = [
            {"name": record["name"], "supported": safe} for record in _state_records()
        ]
    message = "state layout" if safe is not True else "populated CMS receipt history"
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match=message):
        await preparation._lock_publication_state(session, "example")
    assert session.scalar.await_count == (2 if safe is True else 0)


async def test_cms_catalog_attestation_keeps_owner_function_and_constraint_envelopes():
    """Native permissions replace catalog/role attestation while state layout remains checked."""
    session = _state_session()
    await preparation._lock_publication_state(session, "example")
    statements = "\n".join(str(call.args[0]) for call in session.execute.await_args_list)
    assert "relkind='r'" in statements and "relpersistence='p'" in statements
    assert "NOT c.relrowsecurity" in statements
    assert not any(
        token in statements for token in ("relowner", "relacl", "pg_has_role", "prosrc", "pg_relation_filenode")
    )
