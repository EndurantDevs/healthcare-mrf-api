# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native role powers must reproduce protected rights before ACL projection."""

import json
from copy import deepcopy

import pytest

from process import provider_directory_cms_owner_role_rights as rights

_TABLE = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"]
_COLUMN = ["INSERT", "REFERENCES", "SELECT", "UPDATE"]


def _relation(identity):
    return dict(
        oid=identity,
        relowner=10,
        native=dict(
            columns=[
                dict(attribute=dict(attnum=1, attname="key", attisdropped=False)),
                dict(attribute=dict(attnum=2, attname="dropped", attisdropped=True)),
                dict(attribute=dict(attnum=3, attname="payload", attisdropped=False)),
            ]
        ),
    )


def _columns(*privileges):
    return [[number, privilege] for number in (1, 3) for privilege in sorted(privileges)]


def _before_rights(actor):
    if actor in (10, 20):
        return ["SELECT", "INSERT", "UPDATE"], _columns("SELECT", "INSERT", "UPDATE")
    if actor == 40:
        return ["SELECT"], _columns("SELECT")
    if actor == 50:
        return ["INSERT", "UPDATE", "DELETE"], _columns("INSERT", "UPDATE")
    if actor == 60:
        return ["MAINTAIN"], []
    if actor == 70:
        return list(_TABLE), _columns(*_COLUMN)
    return [], []


def _after_rights(actor):
    if actor in (10, 20, 40):
        return ["SELECT"], _columns("SELECT")
    if actor == 30:
        return ["SELECT"], [[1, "SELECT"], [1, "UPDATE"], [3, "SELECT"]]
    return _before_rights(actor)


def _inventory():
    actors = list(range(10, 90, 10))
    relation_vectors = []
    for actor in actors:
        tables, columns = _before_rights(actor)
        relation_vectors.extend([[actor, 100, tables, columns], [actor, 101, [], [[3, "SELECT"]]]])
    return dict(
        canonical=dict(
            relations=[_relation(100)],
            dependency_relations=[],
            dependency_histories=[],
            physical=[
                dict(
                    oid=100,
                    native=dict(
                        table_acl=[[10, 10, privilege, False] for privilege in ("SELECT", "INSERT", "UPDATE")],
                        column_acl=[],
                    ),
                )
            ],
        ),
        private=[_relation(101)],
        routines=[
            dict(
                oid=200,
                native=dict(definition=dict(oid="200", proowner="10"), acl=[[10, 10, "EXECUTE", False]]),
            ),
            dict(
                oid=201,
                native=dict(definition=dict(oid="201", proowner="10"), acl=[[0, 10, "EXECUTE", True]]),
            ),
        ],
        roles=dict(
            roles=[dict(oid=str(actor), rolname=f"actor_{actor}", rolsuper=actor == 70) for actor in actors],
            memberships=None,
            reach=[[20, 10, True, True]],
            namespace_rights=[[10, "catalog", True, True]],
            relation_rights=relation_vectors,
            routine_rights=[
                entry for actor in actors for entry in ([actor, 200, actor in (10, 20, 70)], [actor, 201, True])
            ],
        ),
    )


def _plan():
    return dict(
        owner_oid=30,
        ordinary_writer_oid=10,
        relations=[
            dict(
                oid=100,
                owner_oid=30,
                table_acl=[[10, 30, "SELECT", False], [30, 30, "SELECT", False]],
                column_acl=[[1, 30, 30, "UPDATE", False]],
            )
        ],
        routines=[
            dict(oid=200, owner_oid=30, routine_acl=[[30, 30, "EXECUTE", False]]),
            dict(oid=201, owner_oid=10, routine_acl=[[0, 10, "EXECUTE", True]]),
        ],
    )


class _Native:
    def __init__(self):
        self.calls = []
        self.transform = lambda capabilities: capabilities
        self.is_active = True
        self.is_retired = False

    def is_in_transaction(self):
        return self.is_active

    def is_closed(self):
        return self.is_retired

    async def fetch(self, sql, actors, grantees):
        assert sql == rights._CAPABILITIES_SQL
        self.calls.append((deepcopy(actors), deepcopy(grantees)))
        capabilities = [
            dict(
                oid=actor,
                rolname=f"actor_{actor}",
                rolsuper=actor == 70,
                read_all=actor == 40,
                write_all=actor == 50,
                maintain=actor == 60,
                usage=[[grantee, grantee == actor or actor == 20 and grantee == 10] for grantee in grantees],
            )
            for actor in actors
        ]
        return self.transform(capabilities)


@pytest.mark.asyncio
async def test_projects_native_inherited_predefined_and_superuser_powers_without_mutating_inputs():
    inventory, plan, native = _inventory(), _plan(), _Native()
    original = deepcopy((inventory, plan))
    expected = await rights.expected_cms_owner_roles(native, inventory, plan)
    assert (inventory, plan) == original
    assert native.calls == [(list(range(10, 90, 10)), [10, 30])]
    for actor in range(10, 90, 10):
        assert next(entry for entry in expected["relation_rights"] if entry[:2] == [actor, 100]) == [
            actor,
            100,
            *_after_rights(actor),
        ]
        assert [actor, 200, actor in (30, 70)] in expected["routine_rights"]
        assert [actor, 201, True] in expected["routine_rights"]
    for field in ("roles", "memberships", "reach", "namespace_rights"):
        assert expected[field] == inventory["roles"][field]
        if expected[field] is not None:
            assert expected[field] is not inventory["roles"][field]
    assert [entry for entry in expected["relation_rights"] if entry[1] == 101] == [
        entry for entry in inventory["roles"]["relation_rights"] if entry[1] == 101
    ]


@pytest.mark.asyncio
async def test_accepts_raw_asyncpg_json_usage_and_bound_acl_grant_options():
    native = _Native()
    native.transform = lambda capabilities: [{**entry, "usage": json.dumps(entry["usage"])} for entry in capabilities]
    inventory = _inventory()
    inventory["canonical"]["physical"][0]["native"]["table_acl"][0][3] = True
    assert await rights.expected_cms_owner_roles(native, inventory, _plan())


@pytest.mark.asyncio
async def test_column_only_acl_and_public_table_access_preserve_native_semantics():
    inventory, plan = _inventory(), _plan()
    before = inventory["canonical"]["physical"][0]["native"]
    before["table_acl"] = [[0, 10, "SELECT", False]]
    before["column_acl"] = [[3, 10, 10, "UPDATE", True]]
    public_rights_by_actor = {
        10: (["SELECT"], [[1, "SELECT"], [3, "SELECT"], [3, "UPDATE"]]),
        20: (["SELECT"], [[1, "SELECT"], [3, "SELECT"], [3, "UPDATE"]]),
        30: (["SELECT"], _columns("SELECT")),
        40: (["SELECT"], _columns("SELECT")),
        50: (["SELECT", "INSERT", "UPDATE", "DELETE"], _columns("SELECT", "INSERT", "UPDATE")),
        60: (["SELECT", "MAINTAIN"], _columns("SELECT")),
        70: (list(_TABLE), _columns(*_COLUMN)),
        80: (["SELECT"], _columns("SELECT")),
    }
    for entry in inventory["roles"]["relation_rights"]:
        if entry[1] == 100:
            entry[2:] = public_rights_by_actor[entry[0]]
    expected = await rights.expected_cms_owner_roles(_Native(), inventory, plan)
    assert [20, 100, ["SELECT"], _columns("SELECT")] in expected["relation_rights"]


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["rolname", "rolsuper", "read_all", "write_all", "maintain"])
async def test_refuses_changed_native_role_identity_or_capability_shape(field):
    native = _Native()

    def _change(capabilities):
        capabilities[0][field] = (
            "other" if field == "rolname" else not capabilities[0][field] if field == "rolsuper" else 1
        )
        return capabilities

    native.transform = _change
    with pytest.raises(ValueError, match="native_role_changed"):
        await rights.expected_cms_owner_roles(native, _inventory(), _plan())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ["missing", "duplicate", "outside", "reorder", "usage_missing", "usage_duplicate", "usage_outside", "usage_flag"],
)
async def test_refuses_incomplete_or_unbound_native_capability_matrix(mutation):
    native = _Native()

    def _change(capabilities):
        mutation_by_kind = {
            "missing": lambda: capabilities.pop(),
            "duplicate": lambda: capabilities.append(deepcopy(capabilities[0])),
            "outside": lambda: capabilities[0].update(oid=999),
            "reorder": lambda: capabilities.reverse(),
            "usage_missing": lambda: capabilities[0]["usage"].pop(),
            "usage_duplicate": lambda: capabilities[0]["usage"].append(capabilities[0]["usage"][0]),
            "usage_outside": lambda: capabilities[0]["usage"][0].__setitem__(0, 999),
            "usage_flag": lambda: capabilities[0]["usage"][0].__setitem__(1, 1),
        }
        mutation_by_kind[mutation]()
        return capabilities

    native.transform = _change
    with pytest.raises(ValueError, match="native_"):
        await rights.expected_cms_owner_roles(native, _inventory(), _plan())


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["relation_rights", "routine_rights"])
async def test_requires_exact_before_effective_vectors(field):
    inventory = _inventory()
    if field == "relation_rights":
        inventory["roles"][field][0][2:] = [[], []]
    else:
        inventory["roles"][field][0][2] = False
    with pytest.raises(ValueError, match="before_.*_mismatch"):
        await rights.expected_cms_owner_roles(_Native(), inventory, _plan())


@pytest.mark.asyncio
async def test_does_not_trust_predefined_role_flags_when_before_does_not_reproduce():
    native = _Native()

    def _change(capabilities):
        capabilities[3]["read_all"] = False
        return capabilities

    native.transform = _change
    with pytest.raises(ValueError, match="before_relation_mismatch"):
        await rights.expected_cms_owner_roles(native, _inventory(), _plan())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "roles_duplicate",
        "roles_order",
        "roles_unknown",
        "role_flag",
        "relation_missing",
        "relation_duplicate",
        "relation_outside",
        "table_order",
        "table_duplicate",
        "columns_order",
        "columns_duplicate",
        "column_outside",
        "routine_missing",
        "routine_duplicate",
        "routine_order",
        "routine_flag",
    ],
)
async def test_refuses_protected_role_vector_normalization(mutation):
    inventory = _inventory()
    protected = inventory["roles"]
    mutation_by_kind = {
        "roles_duplicate": lambda: protected["roles"].append(deepcopy(protected["roles"][0])),
        "roles_order": lambda: protected["roles"].reverse(),
        "roles_unknown": lambda: protected.update(extra=[]),
        "role_flag": lambda: protected["roles"][0].update(rolsuper=0),
        "relation_missing": lambda: protected["relation_rights"].pop(),
        "relation_duplicate": lambda: protected["relation_rights"].append(deepcopy(protected["relation_rights"][0])),
        "relation_outside": lambda: protected["relation_rights"][0].__setitem__(0, 999),
        "table_order": lambda: protected["relation_rights"][0][2].reverse(),
        "table_duplicate": lambda: protected["relation_rights"][0][2].append("SELECT"),
        "columns_order": lambda: protected["relation_rights"][0][3].reverse(),
        "columns_duplicate": lambda: protected["relation_rights"][0][3].append([1, "SELECT"]),
        "column_outside": lambda: protected["relation_rights"][0][3][0].__setitem__(0, 2),
        "routine_missing": lambda: protected["routine_rights"].pop(),
        "routine_duplicate": lambda: protected["routine_rights"].append(deepcopy(protected["routine_rights"][0])),
        "routine_order": lambda: protected["routine_rights"].reverse(),
        "routine_flag": lambda: protected["routine_rights"][0].__setitem__(2, 1),
    }
    mutation_by_kind[mutation]()
    with pytest.raises(ValueError, match="cms_owner_role_rights_"):
        await rights.expected_cms_owner_roles(_Native(), inventory, _plan())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "relation_missing",
        "routine_missing",
        "relation_duplicate",
        "routine_outside",
        "acl_unbound",
        "acl_duplicate",
        "acl_option",
        "acl_right",
        "owner_outside",
        "column_dropped",
        "physical_missing",
        "private_overlap",
    ],
)
async def test_refuses_unbound_or_unsupported_plan_scope(mutation):
    inventory, plan, native = _inventory(), _plan(), _Native()
    mutation_by_kind = {
        "relation_missing": lambda: plan["relations"].clear(),
        "routine_missing": lambda: plan["routines"].pop(),
        "relation_duplicate": lambda: plan["relations"].append(deepcopy(plan["relations"][0])),
        "routine_outside": lambda: plan["routines"][0].update(oid=999),
        "acl_unbound": lambda: plan["relations"][0]["table_acl"][0].__setitem__(0, 999),
        "acl_duplicate": lambda: plan["routines"][0]["routine_acl"].append([30, 30, "EXECUTE", False]),
        "acl_option": lambda: plan["routines"][0]["routine_acl"][0].__setitem__(3, 1),
        "acl_right": lambda: plan["routines"][0]["routine_acl"][0].__setitem__(2, "SELECT"),
        "owner_outside": lambda: plan["relations"][0].update(owner_oid=999),
        "column_dropped": lambda: plan["relations"][0]["column_acl"][0].__setitem__(0, 2),
        "physical_missing": lambda: inventory["canonical"]["physical"].clear(),
        "private_overlap": lambda: inventory["private"][0].update(oid=100),
    }
    mutation_by_kind[mutation]()
    with pytest.raises(ValueError, match="cms_owner_role_rights_"):
        await rights.expected_cms_owner_roles(native, inventory, plan)
    assert not native.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("is_active,is_retired", [(False, False), (True, True)])
async def test_requires_existing_active_native_transaction(is_active, is_retired):
    native = _Native()
    native.is_active, native.is_retired = is_active, is_retired
    with pytest.raises(ValueError, match="transaction_required"):
        await rights.expected_cms_owner_roles(native, _inventory(), _plan())
    assert not native.calls
