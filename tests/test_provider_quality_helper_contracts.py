# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

import pytest

from process.provider_quality_parts import model_helpers, table_helpers


def test_reporting_years_use_nested_sources_and_manifest_year(monkeypatch) -> None:
    monkeypatch.setattr(model_helpers, "PROVIDER_QUALITY_MIN_YEAR", 2020)
    monkeypatch.setattr(model_helpers, "PROVIDER_QUALITY_MAX_YEAR", 2025)
    minimum_year = model_helpers.PROVIDER_QUALITY_MIN_YEAR
    maximum_year = model_helpers.PROVIDER_QUALITY_MAX_YEAR
    manifest_by_field = {
        "sources": {
            "ignored": (),
            "accepted": [
                None,
                {"reporting_year": str(minimum_year)},
                {"reporting_year": "invalid"},
                {"reporting_year": maximum_year + 1},
            ],
        },
        "year": minimum_year + 1,
    }

    assert model_helpers._materialize_reporting_years(manifest_by_field) == (
        minimum_year,
        minimum_year + 1,
    )


def test_reporting_years_default_and_out_of_range_fallbacks(monkeypatch) -> None:
    monkeypatch.setattr(model_helpers, "PROVIDER_QUALITY_MIN_YEAR", 2020)
    monkeypatch.setattr(model_helpers, "PROVIDER_QUALITY_MAX_YEAR", 2025)
    monkeypatch.setattr(
        model_helpers,
        "PROVIDER_QUALITY_YEAR_WINDOW",
        (2023, 2024, 2025),
    )
    assert model_helpers._materialize_reporting_years({"sources": []}) == tuple(
        model_helpers.PROVIDER_QUALITY_YEAR_WINDOW
    )
    assert model_helpers._materialize_reporting_years(
        {
            "sources": {
                "future": [
                    {
                        "reporting_year": (
                            model_helpers.PROVIDER_QUALITY_MAX_YEAR + 1
                        )
                    }
                ]
            }
        }
    ) == (model_helpers.PROVIDER_QUALITY_MAX_YEAR,)


def test_optional_cohort_model_discovery_and_presence(monkeypatch) -> None:
    feature_model = type("FeatureModel", (), {})
    peer_model = type("PeerModel", (), {})
    monkeypatch.setattr(
        model_helpers,
        "PricingProviderQualityFeature",
        feature_model,
    )
    monkeypatch.setattr(model_helpers, "PricingProviderQualityProcedureLSH", None)
    monkeypatch.setattr(
        model_helpers,
        "PricingProviderQualityPeerTarget",
        peer_model,
    )

    assert model_helpers._cohort_model_classes() == (feature_model, peer_model)
    assert not model_helpers._cohort_models_present({})
    complete_model_by_name = {
        name: type(name, (), {}) for name in model_helpers.COHORT_MODEL_CLASS_NAMES
    }
    assert model_helpers._cohort_models_present(complete_model_by_name)

    monkeypatch.setattr(model_helpers, "PricingProviderQualityFeature", None)
    monkeypatch.setattr(model_helpers, "PricingProviderQualityPeerTarget", None)
    assert model_helpers._cohort_model_classes() == ()


def test_model_columns_and_job_identifiers_cover_optional_shapes() -> None:
    assert model_helpers._model_columns(None) == set()
    assert model_helpers._model_columns(object()) == set()
    model = type(
        "ColumnModel",
        (),
        {
            "__table__": SimpleNamespace(
                columns=(
                    SimpleNamespace(name="first"),
                    SimpleNamespace(name="second"),
                )
            )
        },
    )
    assert model_helpers._model_columns(model) == {"first", "second"}
    assert model_helpers._chunk_job_id("run", "qpp", 2, 2025, 7) == (
        "provider_quality_chunk_run_qpp_2025_2_7"
    )
    assert model_helpers._build_stage_suffix("!!!", "run").startswith("import_")


class _QualityDatabase:
    def __init__(self) -> None:
        self.status_statements: list[str] = []
        self.scalar_result = None
        self.column_rows = []

    async def status(self, statement: str) -> None:
        self.status_statements.append(statement)

    async def scalar(self, statement: str, **kwargs):
        self.scalar_call = (statement, kwargs)
        return self.scalar_result

    async def all(self, statement: str, **kwargs):
        self.all_call = (statement, kwargs)
        return self.column_rows


class _NoIndexes:
    __tablename__ = "no_indexes"


class _StageIndexes:
    __tablename__ = "quality_stage"
    __main_table__ = "quality_live"
    __my_index_elements__ = ("provider_id",)
    __my_additional_indexes__ = (
        {"index_elements": ()},
        {
            "index_elements": ("score", "year"),
            "using": "btree",
            "where": "score IS NOT NULL",
        },
    )


class _LiveNamedIndex:
    __tablename__ = "quality_live"
    __main_table__ = "quality_live"
    __my_additional_indexes__ = (
        {"name": "quality_explicit_idx", "index_elements": ("score",)},
    )


@pytest.mark.asyncio
async def test_quality_index_helpers_cover_optional_index_shapes(monkeypatch) -> None:
    quality_database = _QualityDatabase()
    monkeypatch.setattr(table_helpers, "db", quality_database)

    await table_helpers._ensure_indexes(_NoIndexes, "mrf")
    await table_helpers._ensure_indexes(_StageIndexes, "mrf")
    await table_helpers._ensure_indexes(_LiveNamedIndex, "mrf")
    await table_helpers._build_staging_indexes({}, "mrf")
    await table_helpers._build_staging_indexes({"empty": _NoIndexes}, "mrf")

    assert len(quality_database.status_statements) == 3
    assert "CREATE UNIQUE INDEX" in quality_database.status_statements[0]
    assert "quality_stage_quality_stage_score_year_idx" in (
        quality_database.status_statements[1]
    )
    assert " USING btree " in quality_database.status_statements[1]
    assert " WHERE score IS NOT NULL" in quality_database.status_statements[1]
    assert "quality_explicit_idx" in quality_database.status_statements[2]
    assert " USING " not in quality_database.status_statements[2]


@pytest.mark.asyncio
async def test_quality_table_introspection_handles_present_and_missing_values(
    monkeypatch,
) -> None:
    quality_database = _QualityDatabase()
    monkeypatch.setattr(table_helpers, "db", quality_database)
    quality_database.scalar_result = object()
    assert await table_helpers._table_exists("mrf", "quality")
    quality_database.scalar_result = None
    assert not await table_helpers._table_exists("mrf", "missing")
    quality_database.column_rows = [
        SimpleNamespace(column_name=" score "),
        SimpleNamespace(column_name=None),
        SimpleNamespace(),
    ]

    assert await table_helpers._table_columns("mrf", "quality") == {"score"}
    assert quality_database.all_call[1] == {
        "schema": "mrf",
        "table": "quality",
    }
