# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed receipt shape checks independent of a PostgreSQL fixture."""

from __future__ import annotations

import importlib
from copy import deepcopy

import pytest


receipt = importlib.import_module("process.entity_address_snapshot_receipt")


def _receipt() -> dict:
    tables = tuple(
        receipt.EntityAddressArchiveTableReceipt(model.__name__, model.__tablename__, "a" * 64, 0, "b" * 64)
        for model in receipt._models()
    )
    return receipt.EntityAddressArchiveReceipt(
        tables,
        receipt._canonical_digest(
            [
                {"model_name": table.model_name, "table_name": table.table_name, "schema_sha256": table.schema_sha256}
                for table in tables
            ]
        ),
        receipt._canonical_digest([table.as_dict() for table in tables]),
    ).as_dict()


def test_receipt_accepts_only_the_model_derived_seven_table_family():
    value = _receipt()
    observed = receipt.validate_entity_address_archive_receipt(value)

    assert observed.as_dict() == value
    assert len(observed.tables) == 7


@pytest.mark.parametrize("tamper", ["content", "schema", "row_count", "table_order", "extra_table"])
def test_receipt_rejects_tampered_or_nonclosed_model_family(tamper: str):
    value = deepcopy(_receipt())
    if tamper == "content":
        value["content_sha256"] = "c" * 64
    elif tamper == "schema":
        value["tables"][0]["schema_sha256"] = "c" * 64
    elif tamper == "row_count":
        value["tables"][0]["row_count"] = -1
    elif tamper == "table_order":
        value["tables"][0], value["tables"][1] = value["tables"][1], value["tables"][0]
    else:
        value["tables"].append(deepcopy(value["tables"][0]))

    with pytest.raises(receipt.EntityAddressArchiveReceiptError):
        receipt.validate_entity_address_archive_receipt(value)
