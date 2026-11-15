# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable NPI metadata and the unchanged mutation revision boundary."""

import importlib.util
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from process import npi_result_generation as generation


def _row():
    return {
        "singleton": True,
        "local_lineage_id": uuid4(),
        "local_generation": 7,
        "origin_lineage_id": uuid4(),
        "origin_generation": 4,
        "published_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "serving_schema": "serving",
        "serving_relation_names": list(generation.RELATION_NAMES),
        "canonical_publication_ref": None,
        "canonical_publication_generation": None,
        "canonical_chain_ref": None,
        "canonical_import_date": None,
    }


def test_legacy_generation_requires_a_declared_location_without_changing_its_edition():
    row = _row()
    expected = generation.validate_npi_result_generation_authority(row)
    legacy_by_field = {
        key: value for key, value in row.items() if key not in {"serving_schema", "serving_relation_names"}
    }
    legacy_by_field["relation_oids"] = ["opaque"] * 6
    before = deepcopy(legacy_by_field)
    with pytest.raises(RuntimeError, match="authority is invalid"):
        generation.validate_npi_result_generation_authority(legacy_by_field)
    assert generation.validate_npi_result_generation_authority(legacy_by_field, declared_schema="serving") == expected
    assert legacy_by_field == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("serving_schema", None),
        ("serving_schema", "bad;schema"),
        ("serving_relation_names", ["npi"] * 6),
        ("serving_relation_names", None),
    ],
)
def test_new_generation_requires_the_complete_declared_family(field, value):
    row = _row()
    row[field] = value
    with pytest.raises(RuntimeError, match="authority is invalid"):
        generation.validate_npi_result_generation_authority(row)


def _migration(name):
    path = Path(__file__).parents[1] / "alembic" / "versions" / name
    spec = importlib.util.spec_from_file_location("npi_revision_" + name[:-3], path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_named_migration_keeps_the_existing_counter_body_and_statement_trigger_paths(monkeypatch):
    old = _migration("20260914120000_npi_result_generation.py")
    new = _migration("20261114160000_npi_named_generation.py")
    statements = []
    monkeypatch.setattr(old.op, "execute", statements.append)
    old._create_revision_function("serving")
    original = re.sub(r"\s+", " ", str(statements[0]).split("$function$")[1]).strip()
    statements.clear()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "serving")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    new.upgrade()
    sql = "\n".join(map(str, statements))
    replacement = next(str(statement) for statement in statements if "CREATE OR REPLACE FUNCTION" in str(statement))
    body = re.sub(r"\s+", " ", replacement.split("$function$")[1]).strip()
    assert body == original.replace(
        "AND relation_oids IS NOT NULL AND TG_RELID::bigint = ANY(relation_oids)",
        "AND serving_schema IS NOT NULL AND TG_TABLE_SCHEMA = serving_schema AND TG_TABLE_NAME = ANY(serving_relation_names)",
    )
    assert "CREATE TRIGGER" not in sql and "DROP TRIGGER" not in sql
    assert "to_regclass" not in sql and "pg_class" not in sql and "pg_roles" not in sql
    assert "DROP COLUMN relation_oids" in sql
    assert statements[1].compile().params == {"schema": "serving", "names": list(generation.RELATION_NAMES)}
    assert "serving_relation_names IS NOT NULL" in sql
    assert "canonical_publication_generation BETWEEN 1 AND 9007199254740991" in sql
