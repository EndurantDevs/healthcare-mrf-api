# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib

import pytest

terminology_synonyms = importlib.import_module("process.terminology_synonyms")


def test_curated_provider_aliases_map_common_terms_to_pricing_provider_type():
    rows = terminology_synonyms._curated_rows()
    family_rows = [
        row
        for row in rows
        if row["domain"] == "provider_type" and row["term_key"] == "family medicine"
    ]

    assert family_rows
    assert family_rows[0]["target_system"] == "PROVIDER_TYPE"
    assert family_rows[0]["target_code"] == "Family Practice"
    assert "207Q00000X" in family_rows[0]["metadata_json"]


def test_curated_procedure_aliases_allow_broad_common_names():
    rows = terminology_synonyms._curated_rows()
    office_visit_codes = {
        row["target_code"]
        for row in rows
        if row["domain"] == "procedure" and row["term_key"] == "office visit"
    }

    assert {"99213", "99214", "99215"}.issubset(office_visit_codes)


@pytest.mark.asyncio
async def test_main_passes_test_mode_to_database_bootstrap(monkeypatch):
    calls = []

    async def fake_init_db(_db):
        calls.append(("init_db", None))

    async def fake_ensure_database(test_mode):
        calls.append(("ensure_database", test_mode))

    async def fake_import_terminology_synonyms(*, test_mode, import_id):
        calls.append(("import", test_mode, import_id))
        return {"ok": True}

    async def fake_disconnect():
        calls.append(("disconnect", None))

    monkeypatch.setattr(terminology_synonyms, "init_db", fake_init_db)
    monkeypatch.setattr(terminology_synonyms, "ensure_database", fake_ensure_database)
    monkeypatch.setattr(terminology_synonyms, "import_terminology_synonyms", fake_import_terminology_synonyms)
    monkeypatch.setattr(terminology_synonyms.db, "disconnect", fake_disconnect)

    result = await terminology_synonyms.main(test_mode=True, import_id="smoke")

    assert result == {"ok": True}
    assert calls == [
        ("init_db", None),
        ("ensure_database", True),
        ("import", True, "smoke"),
        ("disconnect", None),
    ]
