# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cohort snapshots have a distinct native key and preserve immutable replay."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_policy as policy_module
from process import provider_directory_source_acquisition_custody as acquisition
from process import provider_directory_source_cohort_custody as custody


def test_cohort_partition_category_never_joins_acquisition_groups():
    assert custody.MEMBER not in acquisition.BULK_ROOTS
    assert custody.MEMBER in acquisition.ACQUISITION_ROOTS
    assert custody._child_name("pdufc_" + "1" * 48) == "pdufcm_" + "1" * 48
    with pytest.raises(ValueError, match="identity_changed"):
        custody._child_name("pdufpa_" + "1" * 48)


def test_header_insert_cost_is_retired_without_weakening_update_delete_guard():
    assert custody._header_hooks(True)["pd_uhc_flex_npi_cohort_guard"][1] == 31
    assert custody._header_hooks(False)["pd_uhc_flex_npi_cohort_guard"] == (custody._HEADER_GUARD, 27, (), None, ())
    assert (
        custody._header_hooks(True)["pd_uhc_flex_npi_cohort_guard_truncate"]
        == (custody._header_hooks(False)["pd_uhc_flex_npi_cohort_guard_truncate"])
    )
    assert custody._history_hooks(False) == {}
    tables, columns = acquisition._writer_rights(custody.HEADER, ("cohort_id", "npi_count"))
    assert tables == {"SELECT"} and columns == {}
    assert "INSERT" not in acquisition._writer_rights(custody.MEMBER, ("cohort_id", "npi"), history=True)[1]


def _expected_header_hooks(legacy):
    guard = "guard_provider_directory_terminal_root_retirement_child"
    return {
        "pd_uhc_flex_npi_cohort_guard": (custody._HEADER_GUARD, 31 if legacy else 27, (), None, ()),
        "pd_uhc_flex_npi_cohort_guard_truncate": (custody._HEADER_GUARD, 34, (), None, ()),
        "pd_trr_flex_cohort_row": (guard, 31, (), None, ()),
        "pd_trr_flex_cohort_truncate": (guard, 34, (), None, ()),
    }


def _header_catalog(legacy):
    hooks_by_name = _expected_header_hooks(legacy)
    function_by_name = {
        name: {"definition": {"oid": str(501 + index)}}
        for index, name in enumerate(sorted({spec[0] for spec in hooks_by_name.values()}))
    }
    triggers = [
        dict(
            tgname=name,
            tgisinternal=False,
            tgfoid=function_by_name[function]["definition"]["oid"],
            tgtype=mask,
            tgenabled="A",
            tgdeferrable=False,
            tginitdeferred=False,
            tgqual=None,
            tgnargs=0,
            tgargs="\\x",
            tgattr=[],
            tgparentid="0",
            tgoldtable=None,
            tgnewtable=None,
        )
        for name, (function, mask, *_) in hooks_by_name.items()
    ]
    return dict(triggers=triggers, attributes=[]), function_by_name


@pytest.mark.parametrize("legacy", [True, False])
def test_header_authenticates_all_retained_guards(legacy):
    assert custody._header_hooks(legacy) == _expected_header_hooks(legacy)
    catalog, function_by_name = _header_catalog(legacy)
    acquisition._require_hooks(catalog, custody._header_hooks(legacy), function_by_name)


@pytest.mark.parametrize("legacy", [True, False])
@pytest.mark.parametrize("name", ["pd_trr_flex_cohort_row", "pd_trr_flex_cohort_truncate"])
def test_header_rejects_missing_retirement_hook(legacy, name):
    catalog, function_by_name = _header_catalog(legacy)
    catalog["triggers"] = [hook for hook in catalog["triggers"] if hook["tgname"] != name]
    with pytest.raises(ValueError, match="hooks_changed"):
        acquisition._require_hooks(catalog, custody._header_hooks(legacy), function_by_name)


@pytest.mark.parametrize("legacy", [True, False])
def test_header_rejects_additional_retirement_hook(legacy):
    catalog, function_by_name = _header_catalog(legacy)
    catalog["triggers"].append(dict(catalog["triggers"][-1], tgname="unexpected_guard"))
    with pytest.raises(ValueError, match="hooks_changed"):
        acquisition._require_hooks(catalog, custody._header_hooks(legacy), function_by_name)


@pytest.mark.parametrize("legacy", [True, False])
@pytest.mark.parametrize("name", ["pd_trr_flex_cohort_row", "pd_trr_flex_cohort_truncate"])
@pytest.mark.parametrize(
    "field,value",
    [
        ("tgfoid", "999"),
        ("tgtype", 27),
        ("tgenabled", "O"),
        ("tgisinternal", True),
        ("tgdeferrable", True),
        ("tginitdeferred", True),
        ("tgqual", "true"),
        ("tgnargs", 1),
        ("tgargs", "\\x666f7267656400"),
        ("tgattr", [1]),
        ("tgparentid", "999"),
        ("tgoldtable", "old_rows"),
        ("tgnewtable", "new_rows"),
    ],
)
def test_header_rejects_retirement_hook_drift(legacy, name, field, value):
    catalog, function_by_name = _header_catalog(legacy)
    next(hook for hook in catalog["triggers"] if hook["tgname"] == name)[field] = value
    with pytest.raises(ValueError, match="hooks_changed"):
        acquisition._require_hooks(catalog, custody._header_hooks(legacy), function_by_name)


def _cohort_delta_relation(name, root, oid, triggers):
    return dict(
        name=name,
        root=root,
        loading=False,
        catalog=dict(
            classes=[dict(oid=oid, relname=name, relowner="102", relacl=None)],
            attributes=[dict(attrelid=oid, attnum=1, attname="cohort_id", attacl=None)],
            table_acl=[[102, 102, "SELECT", False]],
            column_acl=[],
            access=[],
            constraints=[],
            triggers=triggers,
        ),
        composite=dict(typowner="102"),
        dependencies=[],
    )


def _cohort_delta_pair():
    header, _functions = _header_catalog(True)
    for hook, oid in zip(header["triggers"], ("9", "11", "20", "30"), strict=True):
        hook.update(oid=oid, tgrelid="201", tgconstraint="0")
    header["triggers"].append(
        dict(header["triggers"][0], oid="31", tgname="legacy_foreign_guard", tgconstraint="51", tgisinternal=True)
    )
    history_hooks = [
        dict(header["triggers"][0], oid=str(70 + index), tgname=name, tgrelid="202", tgtype=spec[1])
        for index, (name, spec) in enumerate(custody._history_hooks(True).items())
    ]
    relation_by_name = {
        custody.HEADER: _cohort_delta_relation(custody.HEADER, custody.HEADER, "201", header["triggers"]),
        custody.HISTORY: _cohort_delta_relation(custody.HISTORY, custody.MEMBER, "202", history_hooks),
    }
    history = relation_by_name[custody.HISTORY]
    history["catalog"]["constraints"] = [dict(oid="51", contype="f"), dict(oid="52", contype="c")]
    history["dependencies"] = [dict(objid="51"), dict(objid="52")]
    before_by_field = dict(legacy=True, relations=relation_by_name)
    after = deepcopy(before_by_field)
    after["legacy"] = False
    for relation in after["relations"].values():
        relation["catalog"]["classes"][0]["relowner"] = "101"
        relation["composite"]["typowner"] = "101"
        tables, columns = acquisition._expected_family_acl(relation, 101, 102)
        relation["catalog"]["table_acl"] = [list(edge) for edge in sorted(tables)]
        relation["catalog"]["column_acl"] = [list(edge) for edge in sorted(columns)]
    hooks = after["relations"][custody.HEADER]["catalog"]["triggers"]
    hooks[:] = [hook for hook in hooks if hook["tgconstraint"] != "51"]
    hooks[0].update(oid="100", tgtype=27)
    hooks.sort(key=lambda hook: int(hook["oid"]))
    history = after["relations"][custody.HISTORY]
    history["catalog"]["triggers"] = None
    history["catalog"]["constraints"] = [dict(oid="52", contype="c")]
    history["dependencies"] = [dict(objid="52")]
    return before_by_field, after, dict(owner=101, builder=102)


def test_cohort_adoption_reorders_only_recreated_header_guard_by_numeric_oid():
    before, after, roles = _cohort_delta_pair()
    original_before, original_after = deepcopy(before), deepcopy(after)
    custody.require_cohort_adoption_delta(before, after, roles)
    assert before == original_before and after == original_after
    assert [hook["oid"] for hook in after["relations"][custody.HEADER]["catalog"]["triggers"]] == [
        "11",
        "20",
        "30",
        "100",
    ]


@pytest.mark.parametrize(
    "name",
    ["pd_uhc_flex_npi_cohort_guard_truncate", "pd_trr_flex_cohort_row", "pd_trr_flex_cohort_truncate"],
)
@pytest.mark.parametrize(
    "field,value", [("oid", "999"), ("tgfoid", "999"), ("tgenabled", "O"), ("tgargs", "\\x00"), ("tgrelid", "999")]
)
def test_cohort_adoption_preserves_retained_header_guard_identity_and_metadata(name, field, value):
    before, after, roles = _cohort_delta_pair()
    hook = next(hook for hook in after["relations"][custody.HEADER]["catalog"]["triggers"] if hook["tgname"] == name)
    hook[field] = value
    with pytest.raises(ValueError, match="adoption_relation_changed"):
        custody.require_cohort_adoption_delta(before, after, roles)


@pytest.mark.parametrize("field,value", [("tgfoid", "999"), ("tgtype", 31), ("tgenabled", "O"), ("tgqual", "true")])
def test_cohort_adoption_changes_only_recreated_guard_oid_and_expected_events(field, value):
    before, after, roles = _cohort_delta_pair()
    hook = after["relations"][custody.HEADER]["catalog"]["triggers"][-1]
    hook[field] = value
    with pytest.raises(ValueError, match="adoption_relation_changed"):
        custody.require_cohort_adoption_delta(before, after, roles)


@pytest.mark.parametrize("drift", ["class_oid", "history_constraint", "history_dependency", "trigger_order"])
def test_cohort_adoption_keeps_nonreplacement_identity_and_history_delta_strict(drift):
    before, after, roles = _cohort_delta_pair()
    if drift == "class_oid":
        after["relations"][custody.HEADER]["catalog"]["classes"][0]["oid"] = "999"
    elif drift == "history_constraint":
        after["relations"][custody.HISTORY]["catalog"]["constraints"] = []
    elif drift == "history_dependency":
        after["relations"][custody.HISTORY]["dependencies"] = []
    else:
        after["relations"][custody.HEADER]["catalog"]["triggers"].reverse()
    with pytest.raises(ValueError, match="adoption_(identity|relation)_changed"):
        custody.require_cohort_adoption_delta(before, after, roles)


@pytest.mark.parametrize("scope", ["pdufc_" + "1" * 48, "pdufc_" + "2" * 48])
def test_selected_scope_requires_exact_id_and_cannot_be_a_prefix(scope):
    name = custody._child_name(scope)
    entry_by_field = dict(
        name=name,
        catalog=dict(
            constraints=[
                dict(conname=name + "_scope", constraint_definition=f"CHECK ((cohort_id)::text = '{scope}'::text)")
            ]
        ),
    )
    custody._require_scope_check(entry_by_field, [scope])
    with pytest.raises(ValueError, match="scope_check_changed"):
        custody._require_scope_check(entry_by_field, [scope[:-1] + "3"])


def test_history_bound_rejects_extra_id_and_unconditional_expression():
    first, second = "pdufc_" + "1" * 48, "pdufc_" + "2" * 48
    entry_by_field = dict(
        name=custody.HISTORY,
        catalog=dict(
            constraints=[
                dict(
                    conname=custody._BOUND,
                    constraint_definition=f"CHECK (cohort_id IS NOT NULL AND (cohort_id::text = ANY (ARRAY['{first}'::varchar, '{second}'::varchar]::text[])))",
                )
            ]
        ),
    )
    # Use the native full type spelling, never an arbitrary caller expression.
    entry_by_field["catalog"]["constraints"][0]["constraint_definition"] = entry_by_field["catalog"]["constraints"][0][
        "constraint_definition"
    ].replace("::varchar", "::character varying")
    custody._require_scope_check(entry_by_field, [first, second])
    with pytest.raises(ValueError, match="history_scope_changed"):
        custody._require_scope_check(entry_by_field, [first])
    entry_by_field["catalog"]["constraints"][0]["constraint_definition"] = "CHECK (true)"
    with pytest.raises(ValueError, match="history_scope_changed"):
        custody._require_scope_check(entry_by_field, [first, second])


@pytest.mark.asyncio
async def test_factory_facade_requires_exact_original_connection_and_policy(monkeypatch):
    connection, policy = object(), object()
    capture = AsyncMock(return_value={"namespace_roles": ("migration", "namespace")})
    monkeypatch.setattr(policy_module, "bound_publisher", lambda: SimpleNamespace(connection=connection, policy=policy))
    token = policy_module._FACTORY.set(SimpleNamespace(cohort_snapshot_custody=capture))
    try:
        assert await policy_module.source_cohort_snapshot_custody(connection, policy, cohort_id="selected") == {
            "namespace_roles": ("migration", "namespace")
        }
        capture.assert_awaited_once_with(connection, policy, cohort_id="selected")
        with pytest.raises(ValueError, match="cohort_original_publisher_required"):
            await policy_module.source_cohort_snapshot_custody(object(), policy)
    finally:
        policy_module._FACTORY.reset(token)


@pytest.mark.asyncio
async def test_retirement_checks_real_parity_before_any_guard_mutation(monkeypatch):
    events = []
    native = SimpleNamespace(
        execute=AsyncMock(side_effect=lambda sql: events.append(sql)), fetchval=AsyncMock(return_value=True)
    )
    monkeypatch.setattr(custody, "_admin_identity", AsyncMock(return_value="original"))
    monkeypatch.setattr(custody, "_require_cohort_parity", AsyncMock(side_effect=ValueError("parity")))
    with pytest.raises(ValueError, match="parity"):
        await custody.retire_cohort_intake_guards(
            native,
            SimpleNamespace(schema="snapshot_schema", owner_role="owner", runtime_roles=("reader",)),
            {"legacy": True},
        )
    assert not events


@pytest.mark.asyncio
async def test_retirement_changes_only_authentic_member_guards_and_existing_header_events(monkeypatch):
    native = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=True))
    monkeypatch.setattr(custody, "_admin_identity", AsyncMock(return_value="original"))
    parity = AsyncMock()
    monkeypatch.setattr(custody, "_require_cohort_parity", parity)
    await custody.retire_cohort_intake_guards(
        native,
        SimpleNamespace(schema="snapshot_schema", owner_role="owner", runtime_roles=("reader",)),
        {"legacy": True},
    )
    parity.assert_awaited_once()
    statements = [call.args[0] for call in native.execute.await_args_list]
    assert len(statements) == 7
    assert all("DROP FUNCTION" not in statement and "DISABLE TRIGGER" not in statement for statement in statements)
    assert sum("DROP CONSTRAINT" in statement for statement in statements) == 1
    assert sum("CREATE TRIGGER" in statement for statement in statements) == 1
    assert "BEFORE UPDATE OR DELETE" in statements[-2]
    assert "BEFORE INSERT" not in " ".join(statements)
    assert all("pd_trr_flex_cohort_" not in statement for statement in statements)


@pytest.mark.asyncio
async def test_authenticated_current_phase_is_idempotent_without_guard_recreation(monkeypatch):
    native = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=True))
    monkeypatch.setattr(custody, "_admin_identity", AsyncMock(return_value="original"))
    parity = AsyncMock()
    monkeypatch.setattr(custody, "_require_cohort_parity", parity)
    await custody.retire_cohort_intake_guards(
        native,
        SimpleNamespace(schema="snapshot_schema", owner_role="owner", runtime_roles=("reader",)),
        {"legacy": False},
    )
    parity.assert_awaited_once()
    native.execute.assert_not_awaited()


def test_single_historical_bound_uses_exact_scalar_equality():
    cohort = "pdufc_" + "1" * 48
    entry_by_field = dict(
        name=custody.HISTORY,
        catalog=dict(
            constraints=[
                dict(
                    conname=custody._BOUND,
                    constraint_definition=f"CHECK (cohort_id IS NOT NULL AND cohort_id::text = '{cohort}'::text)",
                )
            ]
        ),
    )
    custody._require_scope_check(entry_by_field, [cohort])
    with pytest.raises(ValueError, match="history_scope_changed"):
        custody._require_scope_check(entry_by_field, ["pdufc_" + "2" * 48])
