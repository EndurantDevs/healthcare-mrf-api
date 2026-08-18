# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Python and PostgreSQL evidence-rule parity."""

import tempfile

import pytest

from process import address_evidence_alias_native
from process.address_numeric_grid_alias import run_numeric_grid_alias
from process.ext import address_canon
from process.ext.address_match import compare_address_pair
from tests.address_evidence_alias_test_support import (
    _insert_visible_address,
    _match_record,
    _sql_evidence_rule,
)
from tests.test_address_numeric_grid_alias_db import _insert_archive_address
from tests.test_address_numeric_grid_alias_runtime_db import (
    _connect_runtime_database,
    _create_alias_probe_schema,
    _requires_test_database,
    db,
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


async def _null_completion_shadow(monkeypatch, schema, *, native):
    monkeypatch.setenv(
        address_evidence_alias_native.ADDRESS_EVIDENCE_ALIAS_NATIVE_ENV,
        str(native).lower(),
    )
    monkeypatch.setenv(
        address_evidence_alias_native.ADDRESS_EVIDENCE_ALIAS_SCRATCH_DIR_ENV,
        tempfile.gettempdir(),
    )
    source_key = await _insert_archive_address(
        schema,
        first_line=None,
        second_line="40 Main",
        postal_code="75010",
        strict_source_bits=1,
    )
    target_key = await _insert_archive_address(
        schema,
        first_line="40 Main St",
        second_line=None,
        postal_code="75010",
        strict_source_bits=6,
    )
    for first_line in ("N", "S"):
        await _insert_archive_address(
            schema,
            first_line=first_line,
            second_line=None,
            postal_code="75010",
            strict_source_bits=2,
        )
    for index, address_key in enumerate((source_key, target_key)):
        await _insert_visible_address(
            schema,
            location_key=f"null-completion-{index}",
            npi=1234567893,
            address_key=address_key,
        )
    shadow = await run_numeric_grid_alias(
        mode="shadow",
        schema=schema,
        alias_kind="evidence_gated_address_match_v1",
    )
    candidate = await db.first(
        f"""
        SELECT source_address_key::text, target_address_key::text,
               decision, match_rule
        FROM {schema}.address_alias_candidate_v1
        WHERE run_id = CAST(:run_id AS uuid);
        """,
        run_id=shadow.run_id,
    )
    return shadow, dict(candidate._mapping)


@pytest.mark.asyncio(loop_scope="session")
async def test_null_completion_markers_match_sql_candidate_semantics(monkeypatch):
    """Rust does not invent a PostgreSQL NULL-equality marker bucket."""
    _requires_test_database()
    if address_canon._ptg2_rust_scanner_binary() is None:
        pytest.skip("Rust ptg2_scanner binary is required for alias parity")
    connection = await _connect_runtime_database()
    result_by_engine = {}
    try:
        for engine in ("sql", "rust"):
            schema = f"address_evidence_null_marker_{engine}"
            await _create_alias_probe_schema(connection, schema, "evidence")
            result_by_engine[engine] = await _null_completion_shadow(
                monkeypatch,
                schema,
                native=engine == "rust",
            )
        sql_shadow, sql_candidate = result_by_engine["sql"]
        shadow, candidate = result_by_engine["rust"]
        for field in (
            "candidate_digest",
            "source_count",
            "candidate_sources",
            "candidate_rows",
            "no_candidate",
            "active_skipped",
            "eligible",
            "ambiguous",
            "insufficient_provenance",
            "sample_rows",
        ):
            assert getattr(shadow, field) == getattr(sql_shadow, field)
        assert candidate == sql_candidate
        assert (shadow.eligible, shadow.ambiguous) == (1, 0)
        assert candidate["decision"] == "eligible"
        assert candidate["match_rule"] == "terminal_suffix_omission"
    finally:
        for engine in ("sql", "rust"):
            await connection.execute(
                f'DROP SCHEMA IF EXISTS "address_evidence_null_marker_{engine}" CASCADE;'
            )
        await connection.close()
