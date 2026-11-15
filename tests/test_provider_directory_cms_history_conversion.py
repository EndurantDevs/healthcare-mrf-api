# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-defined history SQL plans, without native conversion or custody claims."""

from copy import deepcopy

import pytest

from process import provider_directory_cms_history_conversion as history
from process.provider_directory_cms_canonical_layout import _LAYOUTS


@pytest.mark.parametrize("datasets", [[], ["one"], ["quote'", "slash\\value"]])
def test_fixed_prework_sql_is_the_existing_conversion_plan_without_candidate_parents(datasets):
    request = _request(same_schema=True)
    for heap in request["heaps"].values():
        heap["dataset_ids"] = datasets
    statements = history.cms_history_bound_statements("catalog", {root: datasets for root in history.ROOTS})
    plan = history.plan_cms_history_conversion(request)
    assert plan["prework"] == tuple(
        statement for pair in zip(statements["add"], statements["validate"], strict=True) for statement in pair
    )
    assert all(statement.endswith("NOT VALID") for statement in statements["add"])
    assert all("VALIDATE CONSTRAINT cms_history_bound" in statement for statement in statements["validate"])


@pytest.mark.parametrize("datasets", [{}, {"unknown": []}, {root: ["later", "earlier"] for root in history.ROOTS}])
def test_prework_renderer_refuses_unknown_heaps_and_noncanonical_dataset_sets(datasets):
    with pytest.raises(ValueError, match="roots_required|bounds_required"):
        history.cms_history_bound_statements("catalog", datasets)


@pytest.mark.parametrize(
    "schema, rendered", [("mrf", "mrf"), ("catalog", "catalog"), ("MixedCase", '"MixedCase"'), ("select", '"select"')]
)
def test_history_foreign_keys_use_native_postgresql_identifier_spelling(schema, rendered):
    dependencies = history.cms_history_dependencies(schema, history.ROOTS[0])
    assert dependencies["foreign_keys"] == {
        "provider_directory_cms_npd_re_dataset_id_resource_type_res_fkey": "FOREIGN KEY (dataset_id, resource_type, resource_id) REFERENCES "
        + rendered
        + ".provider_directory_dataset_resource(dataset_id, resource_type, resource_id) ON DELETE CASCADE"
    }


def _request(*, same_schema=False):
    request_by_field = {
        "catalog_schema": "catalog",
        "future_schema": "catalog" if same_schema else "future",
        "heaps": {},
    }
    for ordinal, root in enumerate(history.ROOTS):
        name = "future_payload_" + str(ordinal)
        expected = history.cms_history_dependencies("catalog", root)
        old_by_field = {
            "schema": "catalog",
            "name": root,
            "kind": "r",
            "indexes": [index[0] for index in _LAYOUTS[root]["indexes"]],
            **expected,
        }
        parent_by_field = {
            "schema": request_by_field["future_schema"],
            "name": name,
            "kind": "p",
            "indexes": [
                name + "_pkey" if index[-1] else name + "_i" + str(position)
                for position, index in enumerate(_LAYOUTS[root]["indexes"])
            ],
            "foreign_keys": {},
            "hooks": {},
        }
        request_by_field["heaps"][root] = {
            "old": old_by_field,
            "parent": parent_by_field,
            "history_name": "retained_payload_" + str(ordinal),
            "dataset_ids": ["retained-one", "retained-two"],
        }
    return request_by_field


@pytest.mark.parametrize("same_schema", [False, True])
def test_plan_preserves_declared_heap_and_index_names(same_schema):
    request_by_field = _request(same_schema=same_schema)
    before = deepcopy(request_by_field)
    plan = history.plan_cms_history_conversion(request_by_field)
    assert request_by_field == before
    assert tuple(plan["critical"]) == ("foreign_keys", "hooks", "rename", "parents", "attach")
    assert [len(plan["critical"][phase]) for phase in plan["critical"]] == [3, 6, 2, 2 if same_schema else 4, 2]
    assert sum("SET SCHEMA" in statement for statement in plan["critical"]["parents"]) == (0 if same_schema else 2)
    for root, entry in request_by_field["heaps"].items():
        expected = plan["expected"][root]
        assert expected["old"]["indexes"] == entry["old"]["indexes"]
        assert expected["old"]["name"] == entry["history_name"]
        assert expected["old"]["parents"] == [{"schema": "catalog", "name": root}]
        assert expected["parent"]["indexes"] == entry["parent"]["indexes"]
        assert expected["parent"]["schema"] == request_by_field["catalog_schema"]
        assert expected["parent"]["name"] == root
        assert expected["old"]["foreign_keys"] == expected["old"]["hooks"] == {}
        assert expected["acl_change"] is expected["callable_change"] is False
    sql = "\n".join(statement for phase in plan["critical"].values() for statement in phase)
    for forbidden in ("SELECT", "COUNT", "COPY", "COMMIT", "GRANT", "OWNER TO", "CREATE", "VALIDATE", "DEFAULT"):
        assert forbidden not in sql
    assert all("cms_history_bound" in statement for statement in plan["prework"])
    assert sum("VALIDATE CONSTRAINT" in statement for statement in plan["prework"]) == 2


def test_empty_history_keeps_named_heap_detached_with_validated_false_bound():
    request_by_field = _request()
    for entry in request_by_field["heaps"].values():
        entry["dataset_ids"] = []
    plan = history.plan_cms_history_conversion(request_by_field)
    assert plan["critical"]["attach"] == ()
    assert sum("CHECK (false) NOT VALID" in statement for statement in plan["prework"]) == 2
    for root in history.ROOTS:
        expected = plan["expected"][root]
        assert expected["old"]["parents"] == []
        assert expected["bound_constraint"] == {"name": "cms_history_bound", "expression": "false", "validated": True}


def test_dataset_literals_escape_quotes_backslashes_and_statement_text():
    request_by_field = _request()
    values = ["a'); SELECT danger; --", "b\\tail", "c\nline", "é"]
    request_by_field["heaps"][history.ROOTS[0]]["dataset_ids"] = values
    plan = history.plan_cms_history_conversion(request_by_field)
    literals = "E'a''); SELECT danger; --',E'b\\\\tail',E'c\nline',E'é'"
    assert f"CHECK (dataset_id IN ({literals})) NOT VALID" in plan["prework"][0]
    assert plan["critical"]["attach"][0].endswith(f"FOR VALUES IN ({literals})")


@pytest.mark.parametrize("bounds", [["b", "a"], ["a", "a"], [""], ["a\x00b"], ["a" * 97], [False], ("a",)])
def test_bounds_must_be_exact_sorted_unique_text(bounds):
    request_by_field = _request()
    request_by_field["heaps"][history.ROOTS[0]]["dataset_ids"] = bounds
    with pytest.raises(ValueError, match="bounds_required"):
        history.plan_cms_history_conversion(request_by_field)


def _change(target, path, value):
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("heaps", "other"), {}),
        (("parent", "schema"), "unrelated"),
        (("old", "kind"), "p"),
        (("old", "callback"), "untrusted"),
        (("parent", "indexes"), ["unrelated_pkey"]),
        (("old", "name"), "other_heap"),
    ],
)
def test_unrelated_or_inconsistent_native_identities_refuse(path, value):
    request_by_field = _request()
    target = request_by_field if path[0] == "heaps" else request_by_field["heaps"][history.ROOTS[0]]
    _change(target, path, value)
    with pytest.raises(ValueError, match="cms_history_plan_"):
        history.plan_cms_history_conversion(request_by_field)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("foreign_keys", "unrelated_fk"), {}),
        (("foreign_keys",), {}),
        (("foreign_keys", "provider_directory_cms_npd_re_dataset_id_resource_type_res_fkey"), "OTHER"),
        (("hooks", "unrelated_hook"), {}),
        (("hooks", "cms_npd_resource_witness_insert_guard", "function"), "other_function"),
        (("hooks", "cms_npd_resource_witness_insert_guard", "enabled"), "D"),
        (("hooks", "cms_npd_resource_witness_insert_guard", "new_table"), "other_transition"),
    ],
)
def test_only_source_fixed_three_fks_and_six_hooks_can_be_retired(path, value):
    request_by_field = _request()
    old_by_field = request_by_field["heaps"][history.ROOTS[0]]["old"]
    _change(old_by_field, path, value)
    with pytest.raises(ValueError, match="dependencies_changed"):
        history.plan_cms_history_conversion(request_by_field)


@pytest.mark.parametrize("name", ["future;DROP", 'future"name', "x" * 64, "", "pg_temp.x"])
def test_identifiers_cannot_escape_the_sql_plan(name):
    request_by_field = _request()
    request_by_field["future_schema"] = name
    with pytest.raises(ValueError, match="identifier_invalid"):
        history.plan_cms_history_conversion(request_by_field)


@pytest.mark.parametrize("same_schema", [False, True])
def test_catalog_name_collisions_refuse(same_schema):
    request_by_field = _request(same_schema=same_schema)
    entry = request_by_field["heaps"][history.ROOTS[0]]
    entry["history_name"] = history.ROOTS[1]
    with pytest.raises(ValueError, match="name_collision"):
        history.plan_cms_history_conversion(request_by_field)


@pytest.mark.parametrize("parent_root", history.ROOTS)
@pytest.mark.parametrize("relation", ["table", "index"])
def test_incoming_parent_destinations_cannot_collide_with_retained_heaps(parent_root, relation):
    request_by_field = _request()
    parent = request_by_field["heaps"][parent_root]["parent"]
    history_name = parent["name"] if relation == "table" else parent["indexes"][0]
    request_by_field["heaps"][history.ROOTS[0]]["history_name"] = history_name
    before = deepcopy(request_by_field)
    with pytest.raises(ValueError, match="name_collision"):
        history.plan_cms_history_conversion(request_by_field)
    assert request_by_field == before
