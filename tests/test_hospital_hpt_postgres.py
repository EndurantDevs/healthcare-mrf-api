# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

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
