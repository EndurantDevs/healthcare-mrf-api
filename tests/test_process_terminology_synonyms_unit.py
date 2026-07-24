# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

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


def test_normalization_and_import_identifiers_are_stable(monkeypatch):
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    assert terminology_synonyms._schema() == "mrf"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tenant")
    assert terminology_synonyms._schema() == "tenant"

    assert terminology_synonyms._normalize_term_key(None) == ""
    assert terminology_synonyms._normalize_term_key("  Brain-MRI / W Contrast  ") == "brain mri w contrast"
    assert terminology_synonyms._import_id(" run-01 / alpha ") == "run01alpha"
    assert terminology_synonyms._import_id("a" * 40) == "a" * 32

    monkeypatch.setattr(
        terminology_synonyms,
        "_now",
        lambda: datetime(2026, 7, 24, 1, 2, 3),
    )
    assert terminology_synonyms._import_id(None) == "20260724010203"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (None, 0),
        (12, 12),
        ("INSERT 0 7", 7),
        ("UPDATE", 0),
        ("", 0),
    ],
)
def test_status_count_accepts_database_status_shapes(status, expected):
    assert terminology_synonyms._status_count(status) == expected


def test_row_mapping_and_stage_index_names_cover_driver_and_postgres_limits():
    row_mapping = {"row_count": "8"}
    assert terminology_synonyms._row_mapping(row_mapping) is row_mapping
    assert (
        terminology_synonyms._row_mapping(
            SimpleNamespace(_mapping=row_mapping)
        )
        is row_mapping
    )

    assert terminology_synonyms._stage_index_name("stage", "term") == "stage_idx_term"
    long_name = terminology_synonyms._stage_index_name("s" * 48, "i" * 48)
    assert len(long_name) == 63
    assert long_name == terminology_synonyms._stage_index_name("s" * 48, "i" * 48)
    assert long_name.startswith("s" * 48)


def test_record_rejects_incomplete_identity_and_applies_display_fallbacks():
    assert (
        terminology_synonyms._record(
            domain="procedure",
            synonym="",
            term_type="curated",
            target_system="CPT",
            target_code="70553",
        )
        is None
    )

    row = terminology_synonyms._record(
        domain="procedure",
        synonym="Brain MRI",
        term_type="curated",
        target_system="cpt",
        target_code=70553,
        canonical_term="MRI of the brain",
        metadata={"contrast": True},
    )

    assert row is not None
    assert row["term_key"] == "brain mri"
    assert row["target_system"] == "CPT"
    assert row["target_code"] == "70553"
    assert row["target_display"] == "MRI of the brain"
    assert row["canonical_term"] == "MRI of the brain"
    assert row["metadata_json"] == '{"contrast": true}'


def test_curated_builders_skip_records_rejected_by_validation(monkeypatch):
    monkeypatch.setattr(terminology_synonyms, "_record", lambda **_kwargs: None)

    assert terminology_synonyms._provider_rows() == []
    assert terminology_synonyms._procedure_rows() == []
    assert terminology_synonyms._specialty_alias_rows() == []


@pytest.mark.asyncio
async def test_create_indexes_supports_named_partial_and_derived_indexes(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(terminology_synonyms.db, "status", status)

    stage_cls = SimpleNamespace(
        __tablename__="terminology_synonym_stage",
        __my_additional_indexes__=[
            {
                "name": "term_lookup",
                "index_elements": ["term_key"],
                "using": "gin",
                "where": "term_key <> ''",
            },
            {
                "index_elements": ["target_system", "target_code"],
            },
        ],
    )

    await terminology_synonyms._create_indexes(stage_cls, "tenant")

    assert status.await_count == 2
    statements = [
        call.args[0] for call in status.await_args_list
    ]
    assert (
        statements[0]
        == "CREATE INDEX IF NOT EXISTS terminology_synonym_stage_idx_term_lookup "
        "ON tenant.terminology_synonym_stage USING gin (term_key) WHERE term_key <> '';"
    )
    assert (
        statements[1]
        == "CREATE INDEX IF NOT EXISTS terminology_synonym_stage_idx_target_system_target_code "
        "ON tenant.terminology_synonym_stage (target_system, target_code);"
    )

    await terminology_synonyms._create_indexes(
        SimpleNamespace(__tablename__="without_indexes"),
        "tenant",
    )
    assert status.await_count == 2


@pytest.mark.parametrize(
    ("insert_name", "expected_fragments"),
    [
        (
            "_insert_code_catalog_rows",
            (
                "FROM tenant.code_catalog",
                "'catalog_display' AS term_type",
                "'RXNORM', 'NDC', 'HP_RX_CODE'",
            ),
        ),
        (
            "_insert_code_synonym_rows",
            (
                "FROM tenant.code_synonym s",
                "LEFT JOIN tenant.code_catalog c",
                "'source_synonym'",
            ),
        ),
        (
            "_insert_observed_provider_rows",
            (
                "FROM tenant.pricing_provider",
                "'observed_provider_type' AS term_type",
                "'PROVIDER_TYPE' AS target_system",
            ),
        ),
        (
            "_insert_nucc_rows",
            (
                "FROM tenant.nucc_taxonomy",
                "'nucc_term' AS term_type",
                "unnest(ARRAY[",
            ),
        ),
        (
            "_insert_observed_procedure_rows",
            (
                "FROM tenant.pricing_provider_procedure",
                "'observed_claims_service_description' AS term_type",
                "'HP_PROCEDURE_CODE' AS target_system",
            ),
        ),
        (
            "_insert_observed_prescription_rows",
            (
                "FROM tenant.pricing_provider_prescription",
                "'observed_prescription_name' AS term_type",
                "UPPER(rx_code_system) AS target_system",
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_source_insert_builders_emit_deduplicating_upserts(
    monkeypatch,
    insert_name,
    expected_fragments,
):
    status = AsyncMock(return_value="INSERT 0 7")
    monkeypatch.setattr(terminology_synonyms.db, "status", status)

    inserted = await getattr(terminology_synonyms, insert_name)(
        "tenant",
        "tenant.terminology_synonym_stage",
    )

    assert inserted == 7
    sql = status.await_args.args[0]
    assert "INSERT INTO tenant.terminology_synonym_stage" in sql
    assert "ON CONFLICT (domain, term_key, target_system, target_code) DO UPDATE SET" in sql
    assert "metadata_json = EXCLUDED.metadata_json" in sql
    for fragment in expected_fragments:
        assert fragment in sql


@pytest.mark.asyncio
async def test_publish_stage_swaps_snapshot_tables_in_order(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(terminology_synonyms.db, "status", status)
    stage_cls = SimpleNamespace(__tablename__="terminology_synonym_stage")

    await terminology_synonyms._publish_stage("tenant", stage_cls)

    assert [call.args[0] for call in status.await_args_list] == [
        "DROP TABLE IF EXISTS tenant.terminology_synonym_old;",
        "ALTER TABLE IF EXISTS tenant.terminology_synonym RENAME TO terminology_synonym_old;",
        "ALTER TABLE tenant.terminology_synonym_stage RENAME TO terminology_synonym;",
        "DROP TABLE IF EXISTS tenant.terminology_synonym_old;",
    ]


def _patch_import_dependencies(monkeypatch, count_rows):
    made_suffixes = []

    def fake_make_class(_model, suffix):
        made_suffixes.append(suffix)
        return SimpleNamespace(
            __tablename__=f"terminology_synonym_{suffix}",
            __table__=SimpleNamespace(name=f"terminology_synonym_{suffix}"),
        )

    monkeypatch.setattr(terminology_synonyms, "_schema", lambda: "tenant")
    monkeypatch.setattr(terminology_synonyms, "make_class", fake_make_class)
    monkeypatch.setattr(terminology_synonyms, "_curated_rows", lambda: [{"term_key": "brain mri"}])
    harness = SimpleNamespace(
        made_suffixes=made_suffixes,
        status=AsyncMock(),
        create_table=AsyncMock(),
        all_rows=AsyncMock(return_value=count_rows),
        push_objects=AsyncMock(),
        create_indexes=AsyncMock(),
        publish_stage=AsyncMock(),
        source_mock_by_name={},
    )
    monkeypatch.setattr(terminology_synonyms.db, "status", harness.status)
    monkeypatch.setattr(terminology_synonyms.db, "create_table", harness.create_table)
    monkeypatch.setattr(terminology_synonyms.db, "all", harness.all_rows)
    monkeypatch.setattr(terminology_synonyms, "push_objects", harness.push_objects)
    monkeypatch.setattr(terminology_synonyms, "_create_indexes", harness.create_indexes)
    monkeypatch.setattr(terminology_synonyms, "_publish_stage", harness.publish_stage)

    source_function_names = (
        "_insert_code_catalog_rows",
        "_insert_code_synonym_rows",
        "_insert_observed_provider_rows",
        "_insert_nucc_rows",
        "_insert_observed_procedure_rows",
        "_insert_observed_prescription_rows",
    )
    for count, function_name in enumerate(source_function_names, start=2):
        source_mock = AsyncMock(return_value=count)
        harness.source_mock_by_name[function_name] = source_mock
        monkeypatch.setattr(terminology_synonyms, function_name, source_mock)
    return harness


def _assert_import_side_effects(harness, expected_import_id):
    stage_table = f"tenant.terminology_synonym_{expected_import_id}"
    assert harness.made_suffixes == [expected_import_id]
    assert [call.args[0] for call in harness.status.await_args_list] == [
        "CREATE SCHEMA IF NOT EXISTS tenant;",
        f"DROP TABLE IF EXISTS {stage_table};",
    ]
    harness.create_table.assert_awaited_once()
    stage_class = harness.publish_stage.await_args.args[1]
    harness.push_objects.assert_awaited_once_with(
        [{"term_key": "brain mri"}],
        stage_class,
        rewrite=True,
    )
    for source_mock in harness.source_mock_by_name.values():
        source_mock.assert_awaited_once_with("tenant", stage_table)
    harness.create_indexes.assert_awaited_once_with(stage_class, "tenant")
    harness.all_rows.assert_awaited_once_with(
        f"SELECT count(*)::bigint AS row_count FROM {stage_table};"
    )
    harness.publish_stage.assert_awaited_once()

@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("test_mode", "count_rows", "expected_import_id", "expected_row_count"),
    [
        (True, [SimpleNamespace(_mapping={"row_count": "11"})], "test_run01", 11),
        (False, [], "run01", 0),
    ],
)
async def test_import_builds_and_publishes_complete_staged_snapshot(
    monkeypatch,
    capsys,
    test_mode,
    count_rows,
    expected_import_id,
    expected_row_count,
):
    harness = _patch_import_dependencies(monkeypatch, count_rows)

    import_result = await terminology_synonyms.import_terminology_synonyms(
        test_mode=test_mode,
        import_id="run-01",
    )

    _assert_import_side_effects(harness, expected_import_id)
    assert import_result == {
        "import_id": expected_import_id,
        "test_mode": test_mode,
        "table": "tenant.terminology_synonym",
        "row_count": expected_row_count,
        "source_counts": {
            "curated_rows": 1,
            "code_catalog_rows": 2,
            "code_synonym_rows": 3,
            "observed_provider_rows": 4,
            "nucc_rows": 5,
            "observed_procedure_rows": 6,
            "observed_prescription_rows": 7,
        },
    }
    assert "Terminology synonym import done:" in capsys.readouterr().out


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

    import_summary = await terminology_synonyms.main(test_mode=True, import_id="smoke")

    assert import_summary == {"ok": True}
    assert calls == [
        ("init_db", None),
        ("ensure_database", True),
        ("import", True, "smoke"),
        ("disconnect", None),
    ]
