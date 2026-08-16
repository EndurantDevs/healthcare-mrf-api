# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Python and PostgreSQL evidence-rule parity."""

import pytest

from process.ext import address_canon
from process.ext.address_match import compare_address_pair
from tests.address_evidence_alias_test_support import _match_record, _sql_evidence_rule
from tests.test_address_numeric_grid_alias_runtime_db import (
    _connect_runtime_database,
    _create_alias_probe_schema,
    _requires_test_database,
)


_RULE_CASES = (
    (
        "candidate_confirmed_bare_unit",
        _match_record("4007 Clarksville Pike 301", None),
        _match_record("4007 Clarksville Pike", "Suite 301"),
        False,
    ),
    (
        "unit_designator_punctuation",
        _match_record("3009 North Ballas Road Suite: 141A", None),
        _match_record("3009 N Ballas Rd", "Ste 141A"),
        False,
    ),
    (
        "candidate_confirmed_spaced_unit",
        _match_record("7108 DE SOTO AVE", "105 C"),
        _match_record("7108 De Soto Avenue unit 105c", None),
        True,
    ),
    (
        "direction_relocation",
        _match_record("902 7th Street North", "Suite 4"),
        _match_record("902 N 7TH ST", "Ste 4"),
        False,
    ),
    (
        "terminal_suffix_omission",
        _match_record("15101 Glenwood", "Suite B"),
        _match_record("15101 Glenwood Ave", "Ste B"),
        False,
    ),
    (
        None,
        _match_record("123 US Highway 64", None),
        _match_record("123 US Highway", "Suite 64"),
        False,
    ),
    (
        None,
        _match_record("10 Main St", None),
        _match_record("10 N Main St", None),
        False,
    ),
    (
        None,
        _match_record("902 7th Street North", "Suite 4"),
        _match_record("902 N 7TH ST", "Ste 5"),
        False,
    ),
)


async def _assert_rule_case(connection, schema, rule_case) -> None:
    expected_rule, source_address, target_without_key, reverse_python = rule_case
    target_key = str(
        address_canon.address_key_v1(
            target_without_key.first_line,
            target_without_key.second_line,
            target_without_key.city,
            target_without_key.state,
            target_without_key.zip_code,
            target_without_key.country,
        )
    )
    target_address = _match_record(
        target_without_key.first_line or "",
        target_without_key.second_line,
        address_key=target_key,
        formatted_address=target_without_key.formatted_address,
        visible=True,
    )
    sql_rule = await _sql_evidence_rule(
        connection,
        schema,
        source_address,
        target_address,
    )
    if reverse_python:
        source_key = str(
            address_canon.address_key_v1(
                source_address.first_line,
                source_address.second_line,
                source_address.city,
                source_address.state,
                source_address.zip_code,
                source_address.country,
            )
        )
        python_target = _match_record(
            source_address.first_line or "",
            source_address.second_line,
            address_key=source_key,
            formatted_address=source_address.formatted_address,
            visible=True,
        )
        python_match = compare_address_pair(target_without_key, python_target)
    else:
        python_match = compare_address_pair(source_address, target_address)
    assert sql_rule == expected_rule
    python_exact_rule = (
        python_match.rule
        if python_match and python_match.classification == "exact"
        else None
    )
    assert python_exact_rule == expected_rule
    if expected_rule:
        assert python_match.classification == "exact"


@pytest.mark.asyncio(loop_scope="session")
async def test_postgres_evidence_rules_match_python_exact_oracle():
    """PostgreSQL and Python agree on every promoted rule and hard negative."""
    _requires_test_database()
    schema = "address_evidence_rule_parity_probe"
    connection = await _connect_runtime_database()
    try:
        await _create_alias_probe_schema(connection, schema, "rule_parity")
        for rule_case in _RULE_CASES:
            await _assert_rule_case(connection, schema, rule_case)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await connection.close()
