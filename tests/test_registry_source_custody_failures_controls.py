# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control callable and acquisition custody cannot confer undeclared authority."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_control_custody as controls
from process import provider_directory_source_acquisition_custody as acquisitions
from tests.test_provider_directory_cms_control_custody import _authentic_entry


def _callable_acl(*, owner=17, public=False):
    name = (
        "provider_directory_uhc_flex_practitioner_dataset_valid"
        if public
        else "provider_directory_reviewed_subset_activation_valid"
    )
    entry_by_field = _authentic_entry(name)
    entry_by_field["definition"]["proowner"] = str(owner)
    entry_by_field["acl"] = [[owner, owner, "EXECUTE", False]]
    if public:
        entry_by_field["acl"].append([0, owner, "EXECUTE", False])
    return entry_by_field


@pytest.mark.parametrize("public", [False, True])
def test_control_callable_preserves_declared_execute_scope_without_changing_acl(public):
    entry_by_field = _callable_acl(public=public)
    entry_by_field["acl"].append([19, 17, "EXECUTE", False])
    before = deepcopy(entry_by_field)
    controls._require_callable_acl(entry_by_field, 17, 19, legacy=False)
    assert entry_by_field == before


@pytest.mark.parametrize("grant", [[23, 17, "EXECUTE", False], [19, 17, "EXECUTE", True], [0, 17, "EXECUTE", False]])
def test_private_control_callable_refuses_foreign_public_or_delegable_execute(grant):
    entry_by_field = _callable_acl()
    entry_by_field["acl"].append(grant)
    before = deepcopy(entry_by_field)
    with pytest.raises(ValueError, match="cms_control_callable_acl_changed"):
        controls._require_callable_acl(entry_by_field, 17, 19, legacy=False)
    assert entry_by_field == before


def test_control_callable_builder_ownership_is_only_a_legacy_adoption_input():
    entry_by_field = _callable_acl(owner=19)
    controls._require_callable_acl(entry_by_field, 17, 19, legacy=True)
    with pytest.raises(ValueError, match="cms_control_callable_owner_changed"):
        controls._require_callable_acl(entry_by_field, 17, 19, legacy=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "foreign_signature"])
async def test_control_callable_census_refuses_partial_or_substituted_contracts_before_adoption(fault):
    catalog_records = [dict(signature=signature) for signature in controls._signatures("sample")]
    if fault == "missing":
        catalog_records.pop()
    else:
        catalog_records[0]["signature"] = '"sample"."foreign_callable"()'
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=catalog_records), fetchval=AsyncMock(), execute=AsyncMock()
    )
    with pytest.raises(ValueError, match="cms_control_callable_set_changed"):
        await controls._capture_callables(
            connection, SimpleNamespace(schema="sample", runtime_roles=()), {}, 17, 19, 23
        )
    connection.fetchval.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "replaced", "missing"])
async def test_candidate_lock_rechecks_exact_heap_identity_without_changing_incumbent(fault):
    members = [dict(name="incoming_second", oid=202), dict(name="incoming_first", oid=101)]
    before = deepcopy(members)
    catalog_records = [dict(relname=member["name"], oid=member["oid"]) for member in members]
    if fault == "replaced":
        catalog_records[0]["oid"] = 999
    elif fault == "missing":
        catalog_records.pop()
    connection = SimpleNamespace(execute=AsyncMock(), fetch=AsyncMock(return_value=catalog_records))
    if fault is None:
        await acquisitions._lock_candidates(connection, SimpleNamespace(schema="sample"), members)
    else:
        with pytest.raises(ValueError, match="source_acquisition_family_member_changed"):
            await acquisitions._lock_candidates(connection, SimpleNamespace(schema="sample"), members)
    assert members == before
    assert connection.execute.await_args.args == (
        'LOCK TABLE "sample"."incoming_first","sample"."incoming_second" IN ACCESS EXCLUSIVE MODE NOWAIT',
    )
    assert connection.execute.await_count == 1


@pytest.mark.asyncio
async def test_reused_acquisition_identifier_refuses_before_looking_up_other_families():
    acquisition = "pdpa_" + "1" * 40
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[dict(acquisition_id=acquisition, status="loading")] * 2), execute=AsyncMock()
    )
    with pytest.raises(ValueError, match="source_acquisition_acquisition_identity_reused"):
        await acquisitions._acquisitions(connection, "sample")
    assert connection.fetch.await_count == 1
    connection.execute.assert_not_awaited()
