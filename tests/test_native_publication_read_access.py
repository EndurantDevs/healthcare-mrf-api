# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary heap replacement keeps existing readers and never copies writer grants."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import native_publication_read_access as access


def _relations():
    return [
        dict(
            name=name,
            oid=oid,
            relowner=19,
            owned=True,
            relkind="r",
            relpersistence="p",
            relrowsecurity=False,
            relforcerowsecurity=False,
        )
        for name, oid in (("current", 41), ("stage", 42))
    ]


def _grant(**changes):
    return dict(
        column_name=None,
        rolname="reader",
        grantee=17,
        grantor=19,
        privilege_type="SELECT",
        is_grantable=False,
        **changes,
    )


@pytest.mark.asyncio
async def test_both_exact_heaps_lock_before_existing_reads_are_replayed():
    original = _relations()
    grants = [
        _grant(),
        dict(_grant(), privilege_type="MAINTAIN"),
        dict(_grant(), column_name="selected", is_grantable=True),
        dict(_grant(), grantee=0, rolname=None),
    ]

    native = SimpleNamespace(
        is_in_transaction=lambda: True, fetch=AsyncMock(side_effect=[original, original, grants]), execute=AsyncMock()
    )
    await access.preserve_publication_reads(native, schema="sample", live_table="current", stage_table="stage")
    assert [call.args[0] for call in native.execute.await_args_list] == [
        'LOCK TABLE "sample"."current", "sample"."stage" IN ACCESS EXCLUSIVE MODE',
        'GRANT SELECT ON TABLE "sample"."stage" TO "reader"',
        'GRANT MAINTAIN ON TABLE "sample"."stage" TO "reader"',
        'GRANT SELECT ("selected") ON TABLE "sample"."stage" TO "reader" WITH GRANT OPTION',
        'GRANT SELECT ON TABLE "sample"."stage" TO PUBLIC',
    ]


@pytest.mark.asyncio
async def test_absent_incumbent_locks_only_successor_and_cannot_invent_grants():
    original = _relations()
    original[0]["oid"] = None
    native = SimpleNamespace(
        is_in_transaction=lambda: True, fetch=AsyncMock(side_effect=[original, original]), execute=AsyncMock()
    )
    await access.preserve_publication_reads(native, schema="sample", live_table="current", stage_table="stage")
    native.execute.assert_awaited_once_with('LOCK TABLE "sample"."stage" IN ACCESS EXCLUSIVE MODE')
    assert native.fetch.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["owner", "rls", "kind", "oid", "stage_missing", "transaction"])
async def test_changed_custody_or_identity_refuses_before_any_grant(change):
    before = _relations()
    after = deepcopy(before)
    match change:
        case "owner":
            before[0]["owned"] = False
        case "rls":
            before[0]["relrowsecurity"] = True
        case "kind":
            before[0]["relkind"] = "v"
        case "oid":
            after[0]["oid"] = 99
        case "stage_missing":
            before[1]["oid"] = None
    native = SimpleNamespace(
        is_in_transaction=lambda: change != "transaction",
        fetch=AsyncMock(side_effect=[before, after]),
        execute=AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="publication_read_access"):
        await access.preserve_publication_reads(native, schema="sample", live_table="current", stage_table="stage")
    assert not any(call.args[0].startswith("GRANT") for call in native.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        dict(grantor=99),
        dict(privilege_type="UPDATE"),
        dict(privilege_type="MAINTAIN", column_name="selected"),
        dict(rolname=None),
    ],
)
async def test_foreign_grantors_or_writer_grants_cannot_be_replayed(changes):
    native = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(RuntimeError, match="grant_changed"):
        await access._replay_reads(native, '"sample"."stage"', 19, [dict(_grant(), **changes)])
    native.execute.assert_not_awaited()


def _cross_schema_relations():
    return [
        dict(entry, name="nucc_taxonomy", namespace=namespace, relispartition=False, inherited=False)
        for entry, namespace in zip(_relations(), ("sample", "candidate"), strict=True)
    ]


@pytest.mark.asyncio
async def test_cross_schema_handoff_locks_both_exact_heaps_and_preserves_maintain():
    original = _cross_schema_relations()
    grants = [_grant(), dict(_grant(), privilege_type="MAINTAIN")]
    native = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(side_effect=[original, original, grants]),
        execute=AsyncMock(),
    )
    await access.preserve_publication_reads(
        native, schema="sample", live_table="nucc_taxonomy", stage_schema="candidate", stage_table="nucc_taxonomy"
    )
    assert [call.args[0] for call in native.execute.await_args_list] == [
        'LOCK TABLE "candidate"."nucc_taxonomy" IN ACCESS EXCLUSIVE MODE NOWAIT',
        'LOCK TABLE "sample"."nucc_taxonomy" IN ACCESS EXCLUSIVE MODE',
        'GRANT SELECT ON TABLE "candidate"."nucc_taxonomy" TO "reader"',
        'GRANT MAINTAIN ON TABLE "candidate"."nucc_taxonomy" TO "reader"',
    ]
    assert native.fetch.await_args_list[0].args[1:] == (["sample", "candidate"], ["nucc_taxonomy", "nucc_taxonomy"])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["owner", "namespace", "oid", "partition", "inherited", "transaction"])
async def test_cross_schema_custody_drift_never_copies_grants(change):
    original = _cross_schema_relations()
    after = deepcopy(original)
    if change == "owner":
        original[1]["owned"] = False
    elif change == "namespace":
        original[1]["namespace"] = "other"
    elif change == "oid":
        after[1]["oid"] = 99
    elif change in {"partition", "inherited"}:
        original[1]["relispartition" if change == "partition" else "inherited"] = True
    native = SimpleNamespace(
        is_in_transaction=lambda: change != "transaction",
        fetch=AsyncMock(side_effect=[original, after]),
        execute=AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="publication_read_access"):
        await access.preserve_publication_reads(
            native, schema="sample", live_table="nucc_taxonomy", stage_schema="candidate", stage_table="nucc_taxonomy"
        )
    assert not any(call.args[0].startswith("GRANT") for call in native.execute.await_args_list)
