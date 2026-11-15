# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""TIGER cutover callers keep working through the named capture receipt and saved (table, oid) pairs."""

import pytest
from sqlalchemy import text

from process import reference_family_archive as archive
from process import tiger_snapshot_inheritance as inherited
from tests.test_tiger_snapshot_inheritance_postgres import (
    _captured_stage,
    _owned_database,
    _prepare_replacement_tiger_leaf,
    _provision_tiger_roots,
)

pytestmark = pytest.mark.asyncio


def _manual_cutover(owner_oid):
    """Build the manual authority whether or not this tree still records owner OIDs on it."""
    values_by_field = {
        "package_id": "a" * 64,
        "profile_contract": archive.CONTRACT,
        "authority": "manual",
        "sealed_owner_oid": owner_oid,
        "expected_stage_owner_oid": owner_oid,
    }
    fields = archive.ReferenceFamilyCutoverAuthority.__dataclass_fields__
    return archive.ReferenceFamilyCutoverAuthority(
        **{field: value for field, value in values_by_field.items() if field in fields}
    )


async def _first_cutover(sessions, roles):
    """Run a first cutover the way the existing caller does: read parent_oids from the capture first."""
    admin_sessions, publisher_sessions, _reader_sessions = sessions
    owner = roles[0]
    first, validation, owner_oid = await _captured_stage(admin_sessions, publisher_sessions, owner, zip_code="00001")
    async with admin_sessions.begin() as session:
        for name in inherited.TABLES:
            await session.execute(text(f'ALTER TABLE ONLY tiger."{name}" OWNER TO "{owner}"'))
    cutover = _manual_cutover(owner_oid)
    async with publisher_sessions.begin() as session:
        capture = await inherited.capture_tiger_snapshot_parents(session)
        # The compatible field keeps pair iteration and gives the named mapping cms_snapshot_attach reads.
        assert dict(capture["parent_oids"]) == {name: {"schema": "tiger", "name": name} for name in inherited.TABLES}
        receipt = await inherited.activate_tiger_snapshot_children(
            session,
            ownership=first.ownership,
            manifest=first.manifest,
            validation_receipt=validation,
            cutover=cutover,
            expected_parent_inventory={"contract": inherited.CONTRACT, "parent_oids": capture["parent_oids"]},
        )
    return first, cutover, receipt, owner_oid


def _named_children(prepared):
    return tuple((prepared.ownership.schema_name, name) for name, _old_token in prepared.ownership.relation_oids)


async def test_first_cutover_and_retained_replacement_through_the_named_receipt():
    async with _owned_database() as (url, sessions, roles):
        await _provision_tiger_roots(url, sessions, roles)
        first, cutover, receipt, owner_oid = await _first_cutover(sessions, roles)
        assert tuple(map(tuple, receipt["relations"])) == _named_children(first)
        second, second_validation = await _prepare_replacement_tiger_leaf(sessions, first, receipt, roles[0], owner_oid)
        async with sessions[1].begin() as session:
            replacement = await inherited.activate_tiger_snapshot_children(
                session,
                ownership=second.ownership,
                manifest=second.manifest,
                validation_receipt=second_validation,
                cutover=cutover,
                expected_children=receipt["relations"],
                expected_parent_inventory=receipt["parent_inventory"],
            )
            assert tuple(map(tuple, replacement["predecessors"])) == _named_children(first)
            assert await inherited.current_tiger_snapshot_children(session) == _named_children(second)


async def test_retained_replacement_starts_from_saved_table_oid_pairs():
    async with _owned_database() as (url, sessions, roles):
        await _provision_tiger_roots(url, sessions, roles)
        first, cutover, receipt, owner_oid = await _first_cutover(sessions, roles)
        second, second_validation = await _prepare_replacement_tiger_leaf(sessions, first, receipt, roles[0], owner_oid)
        # A receipt saved before the named form carries (table, oid) pairs; the OIDs stay opaque tokens.
        saved_child_pairs = [["zcta5", 424242], ["zip_state", 434343]]
        saved_parent_by_field = {"contract": inherited.CONTRACT, "parent_oids": [["zcta5", 101], ["zip_state", 102]]}
        replacement_by_name = dict(
            ownership=second.ownership,
            manifest=second.manifest,
            validation_receipt=second_validation,
            cutover=cutover,
            expected_parent_inventory=saved_parent_by_field,
        )
        async with sessions[1].begin() as session:
            with pytest.raises(archive.ReferenceFamilyArchiveError, match="child names differ"):
                async with session.begin_nested():
                    await inherited.activate_tiger_snapshot_children(
                        session, expected_children=[["zip_state", 1], ["zcta5", 2]], **replacement_by_name
                    )
            replacement = await inherited.activate_tiger_snapshot_children(
                session, expected_children=saved_child_pairs, **replacement_by_name
            )
            assert tuple(map(tuple, replacement["predecessors"])) == _named_children(first)
            assert await inherited.current_tiger_snapshot_children(session) == _named_children(second)
