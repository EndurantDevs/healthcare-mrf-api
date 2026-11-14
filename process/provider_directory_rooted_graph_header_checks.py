# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Apply the Rooted header's data CHECK after the candidate is read-only."""

import importlib.util
from pathlib import Path

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_generic_source_storage import _require

HEADER = "provider_directory_rooted_graph_dataset"
CHECK = "pd_rooted_graph_dataset_check"


def _header_expression(schema):
    path = (
        Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260909120000_fhir_request_failure_budget.py"
    )
    specification = importlib.util.spec_from_file_location("rooted_header_data_check", path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    statement = module._dataset_check_sql(schema)
    return statement.split(" CHECK (", 1)[1].removesuffix(") NOT VALID;")


async def defer_rooted_header_check(connection, catalog_schema, candidate_schema, candidate_name):
    """Move the copied header data check to the complete set-validation step."""
    relation = storage_relation(candidate_schema, candidate_name)
    await connection.execute(f"ALTER TABLE {relation} DROP CONSTRAINT {storage_identifier(CHECK)}")


async def validate_rooted_header_check(connection, document):
    """Evaluate every original header predicate with PostgreSQL CHECK null semantics."""
    table = document["tables"][HEADER]
    relation = storage_relation(table["schema"], table["name"])
    expression = _header_expression(document["storage"]["catalog_schema"])
    accepted = await connection.fetchval(
        f"SELECT count(*)=1 AND bool_and(({expression}) IS NOT FALSE) FROM {relation} WHERE dataset_id=$1",
        document["identity"]["dataset_id"],
    )
    _require(accepted is True, "rooted_header_check_invalid")
