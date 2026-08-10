# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import importlib
import datetime
from contextlib import asynccontextmanager
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

process_attributes = importlib.import_module("process.attributes")


class _AsyncRows:
    def __init__(self, rows):
        self._rows = list(rows)
        self._iterator = iter(())

    def __aiter__(self):
        self._iterator = iter(self._rows)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration as error:
            raise StopAsyncIteration from error


class _AsyncFileContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


def _install_csv_rows(monkeypatch, rows):
    monkeypatch.setattr(
        process_attributes,
        "async_open",
        lambda *_args, **_kwargs: _AsyncFileContext(),
    )
    monkeypatch.setattr(
        process_attributes,
        "AsyncDictReader",
        lambda *_args, **_kwargs: _AsyncRows(rows),
    )


def _install_download_pipeline(monkeypatch, rows):
    _install_csv_rows(monkeypatch, rows)
    monkeypatch.setattr(
        process_attributes,
        "_prepare_attribute_tables",
        AsyncMock(),
    )
    monkeypatch.setattr(
        process_attributes,
        "download_it_and_save",
        AsyncMock(),
    )
    monkeypatch.setattr(process_attributes, "_safe_unzip", AsyncMock())
    monkeypatch.setattr(
        process_attributes.glob,
        "glob",
        lambda _pattern: ["/tmp/provider-directory-coverage.csv"],
    )
    monkeypatch.setattr(
        process_attributes,
        "get_import_schema",
        lambda *_args, **_kwargs: "mrf_test",
    )
    monkeypatch.setattr(
        process_attributes,
        "make_class",
        lambda model, *_args, **_kwargs: model,
    )


def _benefit_row(**overrides):
    benefit_record_map = {
        "StandardComponentId": "12345678901234",
        "PlanId": "12345678901234-01",
        "BenefitName": "Primary care",
        "CopayInnTier1": "$10",
        "CopayInnTier2": "$20",
        "CopayOutofNet": "$40",
        "CoinsInnTier1": "10%",
        "CoinsInnTier2": "20%",
        "CoinsOutofNet": "40%",
        "LimitUnit": "Visits",
        "Exclusions": "",
        "Explanation": "Covered",
        "EHBVarReason": "",
        "IsEHB": "yes",
        "IsCovered": "covered",
        "QuantLimitOnSvc": "y",
        "IsExclFromInnMOOP": "no",
        "IsExclFromOonMOOP": "n",
        "LimitQty": "12.5",
        "BusinessYear": "2026",
    }
    benefit_record_map.update(overrides)
    return benefit_record_map


def _price_row(age, **overrides):
    price_record_map = {
        "PlanId": "12345678901234-01",
        "StateCode": "tx",
        "RateEffectiveDate": "2026-01-01",
        "RateExpirationDate": "2026-12-31",
        "RatingAreaId": "RATING AREA 1",
        "Tobacco": "No Preference",
        "Age": age,
        "IndividualRate": "100.25",
        "IndividualTobaccoRate": "120.50",
        "Couple": "200.75",
        "PrimarySubscriberAndOneDependent": "220.00",
        "PrimarySubscriberAndTwoDependents": "240.00",
        "PrimarySubscriberAndThreeOrMoreDependents": "260.00",
        "CoupleAndOneDependent": "300.00",
        "CoupleAndTwoDependents": "320.00",
        "CoupleAndThreeOrMoreDependents": "340.00",
    }
    price_record_map.update(overrides)
    return price_record_map


class _IndexedAttributeModel:
    __main_table__ = "indexed"
    __my_additional_indexes__ = [
        {
            "name": "search",
            "index_elements": ["plan_id"],
            "using": "gin",
            "unique": True,
            "where": "plan_id IS NOT NULL",
        },
        {"index_elements": ["year", "plan_id"]},
    ]


class _EmptyIndexAttributeModel:
    __main_table__ = "empty_indexes"
    __my_additional_indexes__ = []


class _PlainAttributeModel:
    __main_table__ = "plain"


class _OtherPlainAttributeModel:
    __main_table__ = "other_plain"


def _install_shutdown_model_fakes(monkeypatch):
    model_by_field = {
        "PlanAttributes": _IndexedAttributeModel,
        "PlanPrices": _EmptyIndexAttributeModel,
        "PlanRatingAreas": _PlainAttributeModel,
        "PlanBenefits": _OtherPlainAttributeModel,
    }
    for field_name, model in model_by_field.items():
        monkeypatch.setattr(process_attributes, field_name, model)

    def fake_make_class(model, import_date, *, schema_override):
        generated_model = SimpleNamespace(
            __main_table__=model.__main_table__,
            __tablename__=f"{model.__main_table__}_{import_date}",
        )
        if hasattr(model, "__my_additional_indexes__"):
            generated_model.__my_additional_indexes__ = (
                model.__my_additional_indexes__
            )
        assert schema_override == "mrf_test"
        return generated_model

    monkeypatch.setattr(process_attributes, "make_class", fake_make_class)


def _install_shutdown_database_fakes(monkeypatch):
    monkeypatch.setattr(process_attributes, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        process_attributes,
        "get_import_schema",
        lambda *_args, **_kwargs: "mrf_test",
    )
    monkeypatch.setattr(
        process_attributes,
        "_is_table_available",
        AsyncMock(return_value=True),
    )
    status_mock = AsyncMock()
    ddl_mock = AsyncMock()
    monkeypatch.setattr(process_attributes.db, "status", status_mock)
    monkeypatch.setattr(process_attributes.db, "execute_ddl", ddl_mock)

    @asynccontextmanager
    async def fake_transaction():
        yield None

    monkeypatch.setattr(process_attributes.db, "transaction", fake_transaction)
    time_mock = MagicMock()
    monkeypatch.setattr(process_attributes, "print_time_info", time_mock)
    return status_mock, ddl_mock, time_mock
