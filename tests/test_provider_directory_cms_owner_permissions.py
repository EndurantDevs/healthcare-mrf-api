# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Whole fixed-parent/history permission plans from source-defined offline evidence."""

from copy import deepcopy

import pytest

from db.migration_generic_source_candidates import _canonical_copy_guard_bodies
from db.migration_provider_directory_dataset_candidates import TABLES
from process import provider_directory_cms_owner_permissions as planner

_WRITER, _TARGET, _OWNER, _PUBLISHER, _READER, _PUBLICATION_OWNER = 101, 104, 201, 202, 203, 301


def test_column_only_rights_remain_column_only_under_table_caps():
    rights_by_kind = dict(table=[], columns=[[2, "SELECT"], [2, "INSERT"]])
    assert planner.project_cms_relation_permissions(
        rights_by_kind, ({"SELECT", "INSERT", "MAINTAIN"}, {}), {"first": 1, "second": 2}
    ) == (set(), {"SELECT": ("second",), "INSERT": ("second",)})


@pytest.mark.parametrize("kind", ["name", "attnum", "duplicate_attnum", "duplicate_right", "unknown_cap", "cap_column"])
def test_permission_projection_rejects_unbound_columns_and_caps(kind):
    number_by_name = {"first": 1, "second": 2}
    rights_by_kind = dict(table=["SELECT"], columns=[[1, "SELECT"], [2, "SELECT"]])
    maximum = ({"SELECT"}, {"UPDATE": ("second",)})
    if kind == "name":
        number_by_name[1] = 3
    if kind == "attnum":
        number_by_name["second"] = True
    if kind == "duplicate_attnum":
        number_by_name["second"] = 1
    if kind == "duplicate_right":
        rights_by_kind["table"].append("SELECT")
    if kind == "unknown_cap":
        maximum[0].add("UNKNOWN")
    if kind == "cap_column":
        maximum[1]["UPDATE"] = ("missing",)
    with pytest.raises(ValueError, match="permission_columns_changed|effective_rights_changed|permission_cap_changed"):
        planner.project_cms_relation_permissions(rights_by_kind, maximum, number_by_name)
