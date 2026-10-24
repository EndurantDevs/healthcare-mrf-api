# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain the candidate owner when later attempts never claimed its rows."""

from db import migration_provider_directory_first_acquisition_retirement as first
from db import migration_provider_directory_terminal_root_retirement_evidence as evidence


def _lineage_shape_sql():
    shape = evidence._lineage_shape_sql()
    leaf_conditions = (
        """pg_catalog.max(lineage_walk.depth) FILTER (
                WHERE (lineage_walk.import_row).run_id = parent.import_run_id
            ) = pg_catalog.max(lineage_walk.depth)""",
        """COALESCE((
                SELECT lineage_edges.child_count FROM lineage_edges
                 WHERE lineage_edges.run_id = parent.import_run_id
            ), -1) = 0""",
    )
    for condition in leaf_conditions:
        shape = first._replace(
            shape,
            "AND " + condition,
            "AND (pg_catalog.bool_and(parent.previous_dataset_id IS NULL) OR " + condition + ")",
        )
    return shape


def replacements(schema):
    """Extend only the two known routines; preserve complete linear history."""
    old_shape = evidence._lineage_shape_sql()
    return tuple(
        (sql, first._replace(sql, old_shape, _lineage_shape_sql()))
        for sql in (first.evidence_sql(schema), first.eligible_sql(schema))
    )
