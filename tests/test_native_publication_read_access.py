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
        is_in_transaction=lambda: True,
        fetch=AsyncMock(side_effect=[original, original, grants, [], grants]),
        execute=AsyncMock(),
    )
    await access.preserve_publication_reads(native, schema="sample", live_table="current", stage_table="stage")
    assert [call.args[0] for call in native.execute.await_args_list] == [
        'LOCK TABLE "sample"."current", "sample"."stage" IN ACCESS SHARE MODE NOWAIT',
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
        is_in_transaction=lambda: True, fetch=AsyncMock(side_effect=[original, original, []]), execute=AsyncMock()
    )
    await access.preserve_publication_reads(native, schema="sample", live_table="current", stage_table="stage")
    native.execute.assert_awaited_once_with('LOCK TABLE "sample"."stage" IN ACCESS SHARE MODE NOWAIT')
    assert native.fetch.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["rls", "kind", "locked_shape", "stage_missing", "transaction"])
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
        case "locked_shape":
            after[0]["relkind"] = "v"
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
        dict(entry, name="nucc_taxonomy", namespace=namespace, owner_access=True, relispartition=False, inherited=False)
        for entry, namespace in zip(_relations(), ("sample", "candidate"), strict=True)
    ]


@pytest.mark.asyncio
async def test_cross_schema_handoff_locks_both_exact_heaps_and_preserves_maintain():
    original = _cross_schema_relations()
    grants = [_grant(), dict(_grant(), privilege_type="MAINTAIN")]
    native = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(side_effect=[original, original, grants, [], grants]),
        execute=AsyncMock(),
    )
    await access.preserve_publication_reads(
        native, schema="sample", live_table="nucc_taxonomy", stage_schema="candidate", stage_table="nucc_taxonomy"
    )
    assert [call.args[0] for call in native.execute.await_args_list] == [
        'LOCK TABLE "candidate"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
        'LOCK TABLE "sample"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
        'GRANT SELECT ON TABLE "candidate"."nucc_taxonomy" TO "reader"',
        'GRANT MAINTAIN ON TABLE "candidate"."nucc_taxonomy" TO "reader"',
    ]
    assert native.fetch.await_args_list[0].args[1:] == (["sample", "candidate"], ["nucc_taxonomy", "nucc_taxonomy"])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["namespace", "locked_shape", "partition", "inherited", "transaction"])
async def test_cross_schema_custody_drift_never_copies_grants(change):
    original = _cross_schema_relations()
    after = deepcopy(original)
    if change == "owner":
        original[1]["owned"] = False
    elif change == "namespace":
        original[1]["namespace"] = "other"
    elif change == "locked_shape":
        after[1]["relispartition"] = True
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


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_owner", [19, 29])
async def test_validated_cross_schema_preserves_reads_across_owner_transition(prior_owner):
    original = _cross_schema_relations()
    original[0].update(relowner=prior_owner, owned=False)
    original[1].update(relowner=29, owned=False)
    grants = [dict(_grant(), grantor=prior_owner, privilege_type=privilege) for privilege in ("SELECT", "MAINTAIN")]
    grants.append(dict(_grant(), grantor=prior_owner, column_name="selected", is_grantable=True))
    native = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(side_effect=[original, original, grants, [], grants]),
        execute=AsyncMock(),
    )
    await access.preserve_publication_reads(
        native,
        schema="sample",
        live_table="nucc_taxonomy",
        stage_schema="candidate",
        stage_table="nucc_taxonomy",
    )
    assert "pg_has_role" not in native.fetch.await_args_list[0].args[0]
    assert native.fetch.await_args_list[0] == native.fetch.await_args_list[1]
    assert [call.args[0] for call in native.execute.await_args_list] == [
        'LOCK TABLE "candidate"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
        'LOCK TABLE "sample"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
        'GRANT SELECT ON TABLE "candidate"."nucc_taxonomy" TO "reader"',
        'GRANT MAINTAIN ON TABLE "candidate"."nucc_taxonomy" TO "reader"',
        'GRANT SELECT ("selected") ON TABLE "candidate"."nucc_taxonomy" TO "reader" WITH GRANT OPTION',
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "stage_access",
        "live_access",
        "stage_owner",
        "live_owner",
        "access_changed",
        "grantor",
    ],
)
async def test_legacy_owner_metadata_is_opaque_during_named_read_grant_replay(fault):
    original = _cross_schema_relations()
    original[0].update(relowner=19, owned=False)
    original[1].update(relowner=29, owned=False)
    if fault in {"stage_access", "live_access"}:
        original[1 if fault == "stage_access" else 0]["owner_access"] = False
    after = deepcopy(original)
    if fault in {"stage_owner", "live_owner"}:
        after[1 if fault == "stage_owner" else 0]["relowner"] = 39
    elif fault == "access_changed":
        after[1]["owner_access"] = False
    grants = [dict(_grant(), grantor=29 if fault == "grantor" else 19)]
    native = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(side_effect=[original, after, grants, [], grants]),
        execute=AsyncMock(),
    )
    await access.preserve_publication_reads(
        native,
        schema="sample",
        live_table="nucc_taxonomy",
        stage_schema="candidate",
        stage_table="nucc_taxonomy",
    )
    assert any(call.args[0].startswith("GRANT SELECT") for call in native.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage_schema", [None, "sample", "candidate"])
async def test_read_grant_replay_for_same_and_cross_schema_stages(stage_schema):
    original = (
        _relations()
        if stage_schema in (None, "sample")
        else [
            dict(entry, name=name) for entry, name in zip(_cross_schema_relations(), ("current", "stage"), strict=True)
        ]
    )
    native = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(side_effect=[original, original, [_grant()], [], [_grant()]]),
        execute=AsyncMock(),
    )
    await access.preserve_publication_reads(
        native,
        schema="sample",
        live_table="current",
        stage_schema=stage_schema,
        stage_table="stage",
    )
    assert any(call.args[0].startswith("GRANT SELECT") for call in native.execute.await_args_list)


@pytest.mark.asyncio
async def test_empty_cross_schema_skips_replay():
    """An absent incumbent permits only the verified successor lock and no grant replay."""
    original = _cross_schema_relations()
    original[0].update(oid=None, relowner=None, owned=False, owner_access=False)
    original[1].update(relowner=29, owned=False)
    native = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(side_effect=[original, original, []]),
        execute=AsyncMock(),
    )
    await access.preserve_publication_reads(
        native,
        schema="sample",
        live_table="nucc_taxonomy",
        stage_schema="candidate",
        stage_table="nucc_taxonomy",
    )
    native.execute.assert_awaited_once_with('LOCK TABLE "candidate"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT')
    assert native.fetch.await_count == 3
    assert native.fetch.await_args_list[0] == native.fetch.await_args_list[1]


@pytest.mark.asyncio
async def test_postgresql_authorization_failure_aborts_before_read_grants():
    original = _cross_schema_relations()
    native = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(return_value=original),
        execute=AsyncMock(side_effect=PermissionError("denied")),
    )
    with pytest.raises(PermissionError, match="denied"):
        await access.preserve_publication_reads(
            native, schema="sample", live_table="nucc_taxonomy", stage_schema="candidate", stage_table="nucc_taxonomy"
        )
    assert native.execute.await_count == 1


@pytest.mark.asyncio
async def test_native_read_replay_trusts_its_own_grants():
    native = SimpleNamespace(execute=AsyncMock(), fetch=AsyncMock(return_value=[]))
    await access._replay_reads(native, '"sample"."stage"', 19, [_grant()])
    assert native.fetch.await_count == 1  # clears candidate reads; no re-read after granting
    assert native.execute.await_args_list[-1].args[0].startswith("GRANT SELECT")


@pytest.mark.asyncio
@pytest.mark.parametrize("stage_schema", [None, "candidate"])
@pytest.mark.parametrize("incumbent", [False, True])
async def test_candidate_only_table_and_column_reads_are_removed_before_current_replay(stage_schema, incumbent):
    relations = _relations() if stage_schema is None else _cross_schema_relations()
    live_name, stage_name = ("current", "stage") if stage_schema is None else ("nucc_taxonomy", "nucc_taxonomy")
    if not incumbent:
        relations[0]["oid"] = None
    current_grants = [_grant()] if incumbent else []
    candidate_grants = [
        dict(_grant(), rolname="stale_reader"),
        dict(_grant(), rolname="stale_reader", column_name="selected"),
        dict(_grant(), rolname="stale_reader", privilege_type="MAINTAIN"),
    ]
    fetch_responses = [relations, relations]
    if incumbent:
        fetch_responses.append(current_grants)
    fetch_responses.append(candidate_grants)
    if incumbent:
        fetch_responses.append(current_grants)
    native = SimpleNamespace(
        is_in_transaction=lambda: True, fetch=AsyncMock(side_effect=fetch_responses), execute=AsyncMock()
    )
    await access.preserve_publication_reads(
        native, schema="sample", live_table=live_name, stage_schema=stage_schema, stage_table=stage_name
    )
    statements = [call.args[0] for call in native.execute.await_args_list]
    target_relation = f'"{stage_schema or "sample"}"."{stage_name}"'
    removals = [
        f'REVOKE SELECT ON TABLE {target_relation} FROM "stale_reader" CASCADE',
        f'REVOKE SELECT ("selected") ON TABLE {target_relation} FROM "stale_reader" CASCADE',
        f'REVOKE MAINTAIN ON TABLE {target_relation} FROM "stale_reader" CASCADE',
    ]
    assert [statement for statement in statements if statement.startswith("REVOKE")] == removals
    additions = [statement for statement in statements if statement.startswith("GRANT")]
    assert additions == ([f'GRANT SELECT ON TABLE {target_relation} TO "reader"'] if incumbent else [])
    if additions:
        assert statements.index(removals[-1]) < statements.index(additions[0])
