# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import re

import pytest

from scripts.research import hospital_hpt_postgres as benchmark
from scripts.research import hospital_hpt_postgres_sql as benchmark_sql


def test_candidate_sql_has_three_distinct_layouts():
    typed = benchmark_sql.create_sql("typed", "hpt_bench_typed_001")
    dictionary = benchmark_sql.create_sql(
        "dictionary", "hpt_bench_dictionary_001"
    )
    blocks = benchmark_sql.create_sql("blocks", "hpt_bench_blocks_001")

    assert "description text NOT NULL" in typed
    assert "hospital_contract_provision" in typed
    assert "billing_class text NOT NULL" in typed
    assert "payer_plan_dictionary" in dictionary
    assert "payload jsonb" in blocks
    assert "row_no / 512" in benchmark_sql.materialize_sql(
        "blocks", "hpt_bench_blocks_001"
    )
    assert "hashtext" not in benchmark_sql.materialize_sql(
        "blocks", "hpt_bench_blocks_001"
    )
    assert all("hospital_tax_identity" in sql for sql in (typed, dictionary, blocks))


def test_generated_schema_is_strictly_bounded():
    assert benchmark._schema("typed", 12) == "hpt_bench_typed_012"
    with pytest.raises(ValueError, match="unsafe benchmark schema"):
        benchmark._schema("other", 12)


def test_benchmark_counts_must_be_positive(tmp_path):
    with pytest.raises(ValueError, match="trial and query counts"):
        benchmark.run_benchmark(
            "postgresql:///unused_test",
            tmp_path / "manifest.json",
            tmp_path,
            measured_trials=0,
        )


def test_fact_stage_columns_match_canonical_order():
    declaration = benchmark_sql.COMMON_DDL.split("fact_stage (", 1)[1].split(
        ");", 1
    )[0]
    columns = tuple(
        segment.strip().split()[0]
        for segment in declaration.replace("\n", " ").split(",")
    )
    assert columns == benchmark_sql.FACT_COLUMNS


@pytest.mark.parametrize("candidate", benchmark_sql.CANDIDATES)
def test_published_view_columns_match_canonical_order(candidate):
    sql = benchmark_sql.publish_sql(candidate, f"hpt_bench_{candidate}_001")
    projection = re.split(
        r"\sFROM\s", sql.split(" AS SELECT ", 1)[1], maxsplit=1
    )[0]
    columns = tuple(
        expression.strip().rsplit(" AS ", 1)[-1].rsplit(".", 1)[-1]
        for expression in projection.replace("\n", " ").split(",")
    )
    assert columns == benchmark_sql.FACT_COLUMNS


def test_block_payload_positions_match_published_projection():
    schema = "hpt_bench_blocks_001"
    materialization = benchmark_sql.materialize_sql("blocks", schema)
    payload = materialization.split("jsonb_build_array(", 1)[1].split(
        ") ORDER BY", 1
    )[0]
    payload_columns = tuple(
        column.strip() for column in payload.replace("\n", " ").split(",")
    )
    lookup_columns = {
        "hospital_id", "service_ordinal", "code_system", "code",
        "payer_name", "plan_name", "negotiated_dollar",
    }
    expected_columns = tuple(
        column for column in benchmark_sql.FACT_COLUMNS
        if column not in lookup_columns
    )
    projection = benchmark_sql.publish_sql("blocks", schema)
    indexed_columns = tuple(
        (int(index), column)
        for index, column in re.findall(
            r"->> (\d+).*? AS ([a-z_0-9]+)", projection
        )
    )

    assert payload_columns == expected_columns
    assert indexed_columns == tuple(enumerate(expected_columns))
