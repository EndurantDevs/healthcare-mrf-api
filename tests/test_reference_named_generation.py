# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named generation mapping preserves old editions and the existing revision body."""

import importlib.util
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.test_reference_family_archive import _validation_receipt


def _row():
    return dict(
        importer_id="places-zcta",
        local_lineage_id=uuid4(),
        local_generation=7,
        origin_lineage_id=uuid4(),
        origin_generation=4,
        published_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        serving_schema="serving",
        serving_relation_names=["pricing_places_zcta"],
    )


def test_historical_generation_needs_a_saved_declaration_without_rewriting_old_tokens():
    row = _row()
    expected = generation.validate_reference_family_result_generation_authority(row)
    legacy_by_field = {
        key: value for key, value in row.items() if key not in {"serving_schema", "serving_relation_names"}
    }
    legacy_by_field["relation_oids"] = ["opaque old edition"]
    before = deepcopy(legacy_by_field)
    with pytest.raises(RuntimeError, match="schema declaration is missing"):
        generation.validate_reference_family_result_generation_authority(legacy_by_field)
    assert (
        generation.validate_reference_family_result_generation_authority(legacy_by_field, declared_schema="serving")
        == expected
    )
    assert legacy_by_field == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("serving_schema", None),
        ("serving_schema", "bad;schema"),
        ("serving_relation_names", ["other"]),
        ("serving_relation_names", None),
    ],
)
def test_named_generation_requires_complete_scope(field, value):
    row = _row()
    row[field] = value
    with pytest.raises(RuntimeError, match="serving generation is invalid"):
        generation.validate_reference_family_result_generation_authority(row)


def test_legacy_receipt_keeps_raw_body_and_token_with_qualified_names():
    receipt = _validation_receipt()
    relations = receipt.pop("relations")
    receipt.update(
        stage_schema_oid="opaque",
        sealed_owner_oid="opaque",
        relation_oids=[[entry["relation_name"], "opaque"] for entry in relations],
    )
    before = deepcopy(receipt)
    decoded = archive.validate_reference_family_validation_receipt(receipt)
    assert decoded.relations == tuple((entry["schema_name"], entry["relation_name"]) for entry in relations)
    assert decoded.as_dict() == before == receipt
    decoded.as_dict()["relation_oids"].clear()
    assert decoded.as_dict() == before


def _migration(name):
    path = Path(__file__).parents[1] / "alembic" / "versions" / name
    spec = importlib.util.spec_from_file_location("reference_revision_" + name[:-3], path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_named_migration_changes_only_the_existing_statement_guard_predicate(monkeypatch):
    old = _migration("20260929040000_reference_source_generation_guard.py")
    new = _migration("20261114180000_reference_family_named_generation.py")
    statements = []
    monkeypatch.setattr(old.op, "execute", statements.append)
    old._revision_function("serving")
    original = re.sub(r"\s+", " ", str(statements[0]).split("$function$")[1]).strip()
    statements.clear()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "serving")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(new, "_map_nucc_generation", lambda *_: None)
    new.upgrade()
    sql = "\n".join(map(str, statements))
    replacement = next(str(item) for item in statements if "CREATE OR REPLACE FUNCTION" in str(item))
    body = re.sub(r"\s+", " ", replacement.split("$function$")[1]).strip()
    assert body == original.replace(
        "source_revision_tracked AND TG_RELID::bigint = ANY(relation_oids)",
        "source_revision_tracked AND serving_schema=TG_TABLE_SCHEMA AND TG_TABLE_NAME=ANY(serving_relation_names)",
    )
    assert "CREATE TRIGGER" not in sql and "DROP TRIGGER" not in sql
    assert "to_regclass" not in sql and "pg_class" not in sql and "pg_roles" not in sql
    assert "DROP COLUMN relation_oids" in sql
    mappings = [item.compile().params for item in statements if hasattr(item, "compile")]
    assert len(mappings) == len(new._NAMES) - 1
    assert next(mapping for mapping in mappings if mapping["importer"] == "tiger")["schema"] == "tiger"
    assert next(mapping for mapping in mappings if mapping["importer"] == "mrf")["schema"] == "serving"


def test_metadata_backfill_flushes_existing_doctors_guard_before_dropping_old_column(monkeypatch):
    migration = _migration("20261114180000_reference_family_named_generation.py")
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    monkeypatch.setattr(migration, "_map_nucc_generation", lambda *_: None)
    monkeypatch.setattr(migration, "_schema", lambda: "serving")
    migration.upgrade()
    sql = "\n".join(map(str, statements))
    immediate = 'SET CONSTRAINTS "serving".cms_serving_doctors_transition IMMEDIATE'
    deferred = 'SET CONSTRAINTS "serving".cms_serving_doctors_transition DEFERRED'
    assert sql.index("UPDATE") < sql.index(immediate) < sql.index(deferred) < sql.index("DROP COLUMN relation_oids")
    assert "DISABLE TRIGGER" not in sql and "SET CONSTRAINTS ALL" not in sql
