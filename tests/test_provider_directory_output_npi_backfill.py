# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure NPI projection contracts; native authority is qualified separately."""

from contextvars import ContextVar
from datetime import datetime
from types import SimpleNamespace

import pytest

from process import provider_directory_output_npi_backfill as repair


def _directory():
    return SimpleNamespace(
        _schema=lambda: "mrf",
        _q=lambda name: '"' + name + '"',
        CMS_NPD_SOURCE_ID="cms-npd",
        _clean_source_id_list=lambda source_ids: [value.strip() for value in source_ids or () if value.strip()],
        _PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES=ContextVar("npi_overrides", default={}),
    )


@pytest.mark.parametrize("damage", ["schema", "seen", "scratch", "run"])
def test_unsupported_selector_refuses_without_database_access(damage):
    directory = _directory()
    kwargs_by_name = dict(db_schema=None, run_id=None, source_ids=None, seen_table=None)
    if damage == "schema":
        kwargs_by_name["db_schema"] = "other"
    elif damage == "seen":
        kwargs_by_name["seen_table"] = "ordinary_seen"
    elif damage == "scratch":
        directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set({repair.TARGETS[0]: "ordinary_scratch"})
    else:
        kwargs_by_name["run_id"] = ""
    with pytest.raises(RuntimeError, match="provider_directory_output_snapshot_"):
        repair._selector(directory, **kwargs_by_name)


@pytest.mark.parametrize("target", repair.TARGETS)
def test_exact_predicate_and_full_tuple_projection(target):
    directory = _directory()
    selector = repair._selector(directory, None, "run-a", [" source-a ", "source-a"], None)
    inputs, expected, count, params_by_name = repair.npi_projection(directory, target, selector)
    assert "npi IS NULL" in inputs and "source_id<>:cms_npd_source_id" in inputs
    assert "resource_id ~ '^[0-9]{10}$'" in inputs
    assert "last_seen_run_id=CAST(:run_id AS varchar)" in inputs
    assert "SELECT r.*,COALESCE((" in inputs and "),FALSE) AS repair_eligible" in inputs
    assert params_by_name == {"cms_npd_source_id": "cms-npd", "source_ids": ["source-a"], "run_id": "run-a"}
    for column in repair.output_models()[target].__table__.columns:
        assert ' AS "' + str(column.name) + '"' in expected
    assert "i.resource_id::bigint" in expected and ":update_timestamp" in expected
    assert "count(*) FILTER(WHERE repair_eligible)" in count
    assert all("UPDATE " not in statement and "INSERT " not in statement for statement in (inputs, expected, count))


def test_empty_source_filter_preserves_old_all_source_semantics():
    directory = _directory()
    selector = repair._selector(directory, None, None, [" "], None)
    inputs, _expected, _count, params_by_name = repair.npi_projection(directory, repair.TARGETS[0], selector)
    assert selector == {"run_id": None, "source_ids": None}
    assert "ANY(" not in inputs and "last_seen_run_id=" not in inputs
    assert params_by_name == {"cms_npd_source_id": "cms-npd"}


def test_application_conversion_preserves_unchanged_rows_and_metadata():
    timestamp = datetime(2026, 1, 2)
    rows = [
        {
            "resource_id": "0000000000",
            "npi": None,
            "updated_at": None,
            "fhir_meta": {"versionId": 7},
            "repair_eligible": True,
        },
        {"resource_id": "1234567890", "npi": 9, "updated_at": None, "repair_eligible": False},
    ]
    repaired = repair.repair_npi_rows(rows, timestamp)
    assert repaired[0] == {
        "resource_id": "0000000000",
        "npi": 0,
        "updated_at": timestamp,
        "fhir_meta": {"versionId": 7},
    }
    assert repaired[1] == {"resource_id": "1234567890", "npi": 9, "updated_at": None}
    assert rows[0]["npi"] is None and rows[0]["repair_eligible"] is True


@pytest.mark.parametrize("eligible", [None, 0, "true"])
def test_projection_rejects_non_native_eligibility(eligible):
    with pytest.raises(RuntimeError, match="npi_eligibility_changed"):
        repair.repair_npi_rows([{"repair_eligible": eligible}], datetime(2026, 1, 1))
