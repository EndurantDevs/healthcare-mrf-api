# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add durable organization records and canonical integer network identities."""

import os
import re

from alembic import op

revision = "20261007010000_managed_network_registry"
down_revision = "20261009000000_custom_import_child_presence_decode"
branch_labels = None
depends_on = None


_TABLE_DEFINITIONS_BY_NAME = {
    "company_group_registry": """
        group_id uuid PRIMARY KEY, group_kind varchar(32) NOT NULL,
        display_name varchar(512) NOT NULL, aliases jsonb NOT NULL DEFAULT '[]',
        archived boolean NOT NULL DEFAULT false, revision bigint NOT NULL DEFAULT 1,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT registry_group_kind CHECK (group_kind IN ('corporate_parent','naic_group')),
        CONSTRAINT registry_group_name CHECK (btrim(display_name) <> ''),
        CONSTRAINT registry_group_revision CHECK (revision > 0),
        CONSTRAINT registry_group_aliases CHECK (jsonb_typeof(aliases) = 'array')
    """,
    "company_registry": """
        company_id uuid PRIMARY KEY, display_name varchar(512) NOT NULL,
        roles varchar(32)[] NOT NULL, aliases jsonb NOT NULL DEFAULT '[]',
        archived boolean NOT NULL DEFAULT false, revision bigint NOT NULL DEFAULT 1,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT registry_company_name CHECK (btrim(display_name) <> ''),
        CONSTRAINT registry_company_revision CHECK (revision > 0),
        CONSTRAINT registry_company_aliases CHECK (jsonb_typeof(aliases) = 'array'),
        CONSTRAINT registry_company_roles CHECK (
            cardinality(roles) > 0 AND array_position(roles, NULL) IS NULL
            AND roles <@ ARRAY['insurer','employer','network_operator']::varchar[])
    """,
    "hios_issuer_registry": """
        hios_issuer_id varchar(5) PRIMARY KEY, business_state varchar(2) NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT registry_hios_id CHECK (hios_issuer_id ~ '^[0-9]{5}$' AND hios_issuer_id<>'00000'),
        CONSTRAINT registry_hios_state CHECK (business_state ~ '^[A-Z]{2}$')
    """,
    "network_registry_identity": """
        network_id integer GENERATED ALWAYS AS IDENTITY
            (MINVALUE 1 MAXVALUE 2147483647 NO CYCLE) PRIMARY KEY,
        allocation_key uuid NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT registry_network_positive CHECK (network_id > 0),
        CONSTRAINT registry_network_allocation_key UNIQUE (allocation_key)
    """,
    "network_registry_record": """
        network_id integer PRIMARY KEY, display_name varchar(512) NOT NULL,
        aliases jsonb NOT NULL DEFAULT '[]', archived boolean NOT NULL DEFAULT false,
        revision bigint NOT NULL DEFAULT 1, created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT registry_network_record_positive CHECK (network_id > 0),
        CONSTRAINT registry_network_name CHECK (btrim(display_name) <> ''),
        CONSTRAINT registry_network_revision CHECK (revision > 0),
        CONSTRAINT registry_network_aliases CHECK (jsonb_typeof(aliases) = 'array')
    """,
    "network_registry_alias": """
        source_system varchar(64) NOT NULL, source_id varchar(128) NOT NULL,
        alias_type varchar(64) NOT NULL, alias_value varchar(512) NOT NULL,
        scope_key varchar(512) NOT NULL, network_id integer NOT NULL,
        evidence_id varchar(512) NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (source_system,source_id,alias_type,alias_value,scope_key),
        CONSTRAINT registry_network_alias_positive CHECK (network_id > 0),
        CONSTRAINT registry_alias_source CHECK (btrim(source_system) <> '' AND btrim(source_id) <> ''),
        CONSTRAINT registry_alias_value CHECK (btrim(alias_type) <> '' AND btrim(alias_value) <> ''),
        CONSTRAINT registry_alias_evidence CHECK (btrim(scope_key) <> '' AND btrim(evidence_id) <> '')
    """,
}


def _ddl(schema):
    """Render additive native DDL with one quoted schema identifier."""
    qualified = '"' + schema.replace('"', '""') + '".'
    statements = [
        f'CREATE TABLE {qualified}"{name}" ({definition})' for name, definition in _TABLE_DEFINITIONS_BY_NAME.items()
    ]
    statements.append(
        f"CREATE INDEX registry_network_alias_network_idx ON {qualified}network_registry_alias(network_id)"
    )
    return statements


def upgrade():
    """Create durable catalogs without modifying live imported tables."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Refuse silent deletion of allocated identities and correction history."""
    raise RuntimeError("Durable registry identities require an explicit retained-data migration")
