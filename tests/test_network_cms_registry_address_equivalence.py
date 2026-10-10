# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed logical normalization; native sequence authority is tested separately."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_cms_registry_address_equivalence as equivalence


def _catalog():
    catalog_by_table = {}
    for model in equivalence.receipt._models():
        columns = []
        for number, column in enumerate(model.__table__.columns, 1):
            columns.append(
                {
                    "attnum": number,
                    "attname": column.name,
                    "type": equivalence._declared_type(column),
                    "attnotnull": not column.nullable,
                    "attgenerated": "",
                    "attidentity": "",
                    "collation_schema": None,
                    "collation_name": None,
                    "default_expression": None,
                }
            )
        catalog_by_table[model.__tablename__] = {"columns": columns, "constraints": [], "indexes": []}
    evidence = next(
        column
        for column in catalog_by_table["entity_address_evidence"]["columns"]
        if column["attname"] == "evidence_id"
    )
    evidence["default_expression"] = "nextval('owned_sequence'::regclass)"
    sequence_by_field = {
        "attribute_number": evidence["attnum"],
        "default_expression": evidence["default_expression"],
        "settings": {"seqincrement": 1},
    }
    main = catalog_by_table["entity_address_unified"]
    main["constraints"] = [{"key_columns": "{1,2}", "referenced_table": None, "referenced_columns": None}]
    main["indexes"] = [
        {
            "keys": "1 2 0",
            "key_attributes": [{"attribute_number": number} for number in (1, 2, 0)],
            "expressions": "lower(entity_id)",
        }
    ]
    return catalog_by_table, sequence_by_field


def test_only_active_ordinals_and_verified_owned_sequence_names_normalize():
    catalog_by_table, sequence_by_field = _catalog()
    original = deepcopy(catalog_by_table)
    shifted = deepcopy(catalog_by_table)
    main = shifted["entity_address_unified"]
    for column in main["columns"]:
        column["attnum"] += 1
    main["constraints"][0]["key_columns"] = "{2,3}"
    main["indexes"][0]["keys"] = "2 3 0"
    main["indexes"][0]["key_attributes"] = [{"attribute_number": number} for number in (2, 3, 0)]
    renamed_by_field = dict(sequence_by_field, default_expression="nextval('renamed_sequence'::regclass)")
    next(column for column in shifted["entity_address_evidence"]["columns"] if column["attname"] == "evidence_id")[
        "default_expression"
    ] = renamed_by_field["default_expression"]
    assert (
        equivalence._logical_catalog(catalog_by_table, sequence_by_field)[0]
        == equivalence._logical_catalog(shifted, renamed_by_field)[0]
    )
    assert catalog_by_table == original


@pytest.mark.parametrize("mutation", ["column", "type", "inactive_key", "foreign_key", "sequence_default"])
def test_unknown_native_shapes_fail_closed(mutation):
    catalog_by_table, sequence_by_field = _catalog()
    main = catalog_by_table["entity_address_unified"]
    if mutation == "column":
        main["columns"].append(dict(main["columns"][0], attname="unknown", attnum=100))
    elif mutation == "type":
        main["columns"][0]["type"] = "text"
    elif mutation == "inactive_key":
        main["indexes"][0]["keys"] = "999"
    elif mutation == "foreign_key":
        main["constraints"][0]["referenced_table"] = "foreign_table"
    else:
        sequence_by_field["default_expression"] = "nextval('foreign_sequence'::regclass)"
    with pytest.raises(equivalence.RegistryCMSAddressEquivalenceError):
        equivalence._logical_catalog(catalog_by_table, sequence_by_field)


@pytest.mark.parametrize("mutation", ["default", "expression", "constraint", "sequence_settings"])
def test_other_semantic_changes_remain_distinct(mutation):
    catalog_by_table, sequence_by_field = _catalog()
    expected = equivalence._logical_catalog(catalog_by_table, sequence_by_field)[0]
    main = catalog_by_table["entity_address_unified"]
    if mutation == "default":
        main["columns"][0]["default_expression"] = "'changed'::character varying"
    elif mutation == "expression":
        main["indexes"][0]["expressions"] = "upper(entity_id)"
    elif mutation == "constraint":
        main["constraints"][0]["check_expression"] = "checksum > 0"
    else:
        sequence_by_field["settings"]["seqincrement"] = 2
    assert equivalence._logical_catalog(catalog_by_table, sequence_by_field)[0] != expected


@pytest.mark.asyncio
async def test_json_is_never_native_source_authority():
    with pytest.raises(equivalence.RegistryCMSAddressEquivalenceError, match="natively captured"):
        await equivalence.validate_registry_cms_address_copy(None, source_capture={}, clone_ownership={})


@pytest.mark.asyncio
@pytest.mark.parametrize("mapping_change", ["missing", "extra", "duplicate", "unsafe", "type"])
async def test_stage_mapping_is_closed_before_native_capture(mapping_change):
    names = sorted(model.__tablename__ for model in equivalence.receipt._models())
    stage_names_by_logical = {name: name + "_prepared" for name in names}
    if mapping_change == "missing":
        stage_names_by_logical.pop(names[0])
    elif mapping_change == "extra":
        stage_names_by_logical["unknown"] = "unknown_prepared"
    elif mapping_change == "duplicate":
        stage_names_by_logical[names[0]] = stage_names_by_logical[names[1]]
    elif mapping_change == "unsafe":
        stage_names_by_logical[names[0]] = "unsafe.table"
    else:
        stage_names_by_logical[names[0]] = 7
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(equivalence.receipt.EntityAddressArchiveReceiptError):
        await equivalence.capture_registry_cms_address_copy_source(
            session,
            source_schema="synthetic",
            expected_relation_oids=tuple((name, number + 1) for number, name in enumerate(names)),
            stage_table_names=stage_names_by_logical,
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()
