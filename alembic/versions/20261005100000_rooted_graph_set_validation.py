# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stage and validate rooted witnesses in isolated acquisition storage."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

from alembic import op
from db.migration_rooted_graph_candidates import migrate_rooted_graph_candidates

revision = "20261005100000_rooted_graph_set_validation"
down_revision = "20261005030000_source_profile_statement_pins"
branch_labels = None
depends_on = None


def _previous():
    path = Path(__file__).with_name("20260811020000_provider_directory_rooted_graph_acquisition.py")
    spec = importlib.util.spec_from_file_location("rooted_graph_original_storage", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _q(value):
    return '"' + value.replace('"', '""') + '"'


def _sql(schema):
    sql_directory = Path(__file__).resolve().parents[2] / "db" / "sql"
    previous = _previous()
    network_urls = ", ".join(
        "'" + extension_url.replace("'", "''") + "'" for extension_url in previous._PLAN_NET_NETWORK_EXTENSION_URLS
    )
    # Frozen fields reproduce the replaced edge guard's accepted reference shapes.
    field_contracts = (
        ("PractitionerRole", "practitioner", False, "Practitioner"),
        ("PractitionerRole", "organization", False, "Organization"),
        ("PractitionerRole", "network", True, "Organization"),
        ("PractitionerRole", "location", True, "Location"),
        ("PractitionerRole", "healthcareService", True, "HealthcareService"),
        ("PractitionerRole", "endpoint", True, "Endpoint"),
        ("OrganizationAffiliation", "organization", False, "Organization"),
        ("OrganizationAffiliation", "participatingOrganization", False, "Organization"),
        ("OrganizationAffiliation", "network", True, "Organization"),
        ("OrganizationAffiliation", "location", True, "Location"),
        ("OrganizationAffiliation", "healthcareService", True, "HealthcareService"),
        ("OrganizationAffiliation", "endpoint", True, "Endpoint"),
        ("Organization", "partOf", False, "Organization"),
        ("Organization", "endpoint", True, "Endpoint"),
        ("Location", "managingOrganization", False, "Organization"),
        ("Location", "partOf", False, "Location"),
        ("Location", "endpoint", True, "Endpoint"),
        ("HealthcareService", "providedBy", False, "Organization"),
        ("HealthcareService", "location", True, "Location"),
        ("HealthcareService", "coverageArea", True, "Location"),
        ("HealthcareService", "endpoint", True, "Endpoint"),
        ("InsurancePlan", "ownedBy", False, "Organization"),
        ("InsurancePlan", "administeredBy", False, "Organization"),
        ("InsurancePlan", "coverageArea", True, "Location"),
        ("InsurancePlan", "network", True, "Organization"),
        ("Endpoint", "managingOrganization", False, "Organization"),
    )
    contract = ",\n".join(
        f"('{source_type}', '{field}', {str(repeated).lower()}, '{target_type}')"
        for source_type, field, repeated, target_type in field_contracts
    )
    return (
        (sql_directory / "rooted_graph_set_validation.sql")
        .read_text()
        .replace("__SCHEMA__", _q(schema))
        .replace("__SCHEMA_NAME__", schema)
        .replace("__NETWORK_URLS__", network_urls)
        .replace("__FIELD_CONTRACT__", contract)
    )


def upgrade():
    """Keep historical heaps and admit new candidate witnesses set-wise."""

    schema = _previous()._schema()
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise RuntimeError("rooted_graph_schema_invalid")
    migrate_rooted_graph_candidates(op, schema, _sql(schema))


def downgrade():
    """Require an explicit data migration when returning to shared heaps."""

    raise RuntimeError("rooted_graph_set_validation_downgrade_requires_explicit_data_migration")
