# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add an administrator role and evidenced company no-affiliation declarations."""

import os
import re

from alembic import op

revision = "20261025010000_company_affiliation_states"
down_revision = "20261023220000_merge_directory_pricing_controls"
branch_labels = None
depends_on = None


def _ddl(schema):
    if type(schema) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    namespace = '"' + schema + '"'
    changes = (
        (
            "company_registry",
            "registry_company_roles",
            "cardinality(roles) > 0 AND array_position(roles, NULL) IS NULL "
            "AND roles <@ ARRAY['insurer','employer','network_operator','benefits_administrator']::varchar[]",
        ),
        (
            "company_registry_role_assertion",
            "company_role_assertion_role",
            "role IN ('insurer','employer','network_operator','benefits_administrator')",
        ),
        (
            "company_registry_group_assertion",
            "company_group_assertion_relationship",
            "relationship_kind IN ('reported_affiliation','verified_ownership','verified_no_affiliation')",
        ),
        (
            "company_registry_group_assertion",
            "company_group_assertion_manual_group",
            "provenance_kind<>'manual_reference' OR group_id IS NOT NULL OR relationship_kind='verified_no_affiliation'",
        ),
    )
    clauses_by_table = {}
    for table, name, check in changes:
        clauses_by_table.setdefault(table, []).extend(
            (f"DROP CONSTRAINT {name}", f"ADD CONSTRAINT {name} CHECK ({check})")
        )
    clauses_by_table["company_registry_group_assertion"].append(
        "ADD CONSTRAINT company_group_assertion_no_affiliation CHECK "
        "(relationship_kind<>'verified_no_affiliation' OR "
        "(group_id IS NULL AND valid_from IS NOT NULL AND provenance_kind='manual_reference'))"
    )
    return [f"ALTER TABLE {namespace}.{table} " + ", ".join(clauses) for table, clauses in clauses_by_table.items()]


def upgrade():
    """Validate additive values atomically without modifying rows or grants."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    op.execute("SET LOCAL lock_timeout='1s'")
    op.execute("SET LOCAL statement_timeout='10s'")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Keep retained declarations until an explicit compatible downgrade is supplied."""
    raise RuntimeError("Company affiliation values require an explicit retained-data migration")
