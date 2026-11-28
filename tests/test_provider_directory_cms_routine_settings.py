# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite source settings and declaration checks without a database."""

from copy import deepcopy

import pytest

from process import provider_directory_cms_routine_settings as settings

_SCHEMA = "snapshot_schema"
_PREPARATION = "preparation_schema"


@pytest.mark.parametrize(
    "legacy,target",
    [
        (None, None),
        (["search_path=pg_catalog"], ["search_path=pg_catalog, pg_temp"]),
        (["search_path=pg_catalog", "TimeZone=UTC"], ["search_path=pg_catalog, pg_temp", "TimeZone=UTC"]),
        (["search_path=pg_catalog, snapshot_schema"], ["search_path=pg_catalog, snapshot_schema, pg_temp"]),
        (['search_path=pg_catalog, "Mixed_Schema"'], ['search_path=pg_catalog, "Mixed_Schema", pg_temp']),
        (['search_path=pg_catalog, "select"'], ['search_path=pg_catalog, "select", pg_temp']),
    ],
)
def test_exact_source_settings_are_detached(legacy, target):
    before = deepcopy(legacy)
    old, new = settings.approved_cms_routine_settings(legacy)
    assert old == legacy and new == target and legacy == before
    if old is not None:
        old.append("changed")
        new.append("changed")
        assert legacy == before


@pytest.mark.parametrize(
    "legacy",
    [
        [],
        (),
        "search_path=pg_catalog",
        [None],
        ["search_path=pg_temp"],
        ["search_path=pg_catalog, pg_temp"],
        ["search_path=pg_catalog", "timezone=UTC"],
        ["TimeZone=UTC", "search_path=pg_catalog"],
        ["search_path=pg_catalog", "TimeZone=UTC", "jit=off"],
        ["search_path=pg_catalog, public, pg_temp"],
        ["search_path=pg_catalog, public;RESET ALL"],
        ["search_path=pg_catalog, Mixed_Schema"],
        ['search_path=pg_catalog, "bad-name"'],
    ],
)
def test_unknown_or_observed_settings_cannot_become_expectations(legacy):
    with pytest.raises(ValueError, match="source_settings_required"):
        settings.approved_cms_routine_settings(legacy)
