# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain manual provider and exact site identities outside imported snapshots."""

import os
import re

from alembic import op

revision = "20261007080000_manual_directory_registry"
down_revision = "20261007070000_registry_company_links"
branch_labels = None
depends_on = None

_DDL = (
    "CREATE TABLE registry_placeholder.manual_provider_registry (\n\tprovider_id UUID NOT NULL, \n\tdisplay_name VARCHAR(256) NOT NULL, \n\tprovider_kind VARCHAR(16) NOT NULL, \n\taliases VARCHAR(512)[] DEFAULT '{}'::varchar[] NOT NULL, \n\tnpi VARCHAR(10), \n\tarchived BOOLEAN DEFAULT 'false' NOT NULL, \n\trevision BIGINT DEFAULT '1' NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tPRIMARY KEY (provider_id), \n\tCONSTRAINT manual_provider_npi CHECK (npi IS NULL OR npi ~ '^[12][0-9]{9}$'), \n\tCONSTRAINT manual_provider_kind CHECK (provider_kind IN ('individual','organization')), \n\tCONSTRAINT manual_provider_aliases CHECK (cardinality(aliases)<=100 AND array_position(aliases,NULL) IS NULL), \n\tCONSTRAINT manual_provider_identity CHECK (provider_id<>'00000000-0000-0000-0000-000000000000'::uuid), \n\tCONSTRAINT manual_provider_record CHECK (btrim(display_name)<>'' AND revision>0), \n\tCONSTRAINT manual_provider_npi_unique UNIQUE (npi)\n)",
    "CREATE TABLE registry_placeholder.manual_location_registry (\n\tlocation_id UUID NOT NULL, \n\tdisplay_name VARCHAR(256) NOT NULL, \n\taliases VARCHAR(512)[] DEFAULT '{}'::varchar[] NOT NULL, \n\taddress_json JSONB NOT NULL, \n\tcanonical_address_json JSONB NOT NULL, \n\tarchived BOOLEAN DEFAULT 'false' NOT NULL, \n\trevision BIGINT DEFAULT '1' NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tPRIMARY KEY (location_id), \n\tCONSTRAINT manual_location_aliases CHECK (cardinality(aliases)<=100 AND array_position(aliases,NULL) IS NULL), \n\tCONSTRAINT manual_location_identity CHECK (location_id<>'00000000-0000-0000-0000-000000000000'::uuid), \n\tCONSTRAINT manual_location_address CHECK (jsonb_typeof(address_json)='object' AND jsonb_typeof(canonical_address_json)='object'), \n\tCONSTRAINT manual_location_record CHECK (btrim(display_name)<>'' AND revision>0)\n)",
    "CREATE TABLE registry_placeholder.manual_provider_location_binding (\n\tprovider_system VARCHAR(32) NOT NULL, \n\tprovider_id VARCHAR(1024) NOT NULL, \n\tlocation_id UUID NOT NULL, \n\tlocation_key VARCHAR(64) NOT NULL, \n\tentity_type VARCHAR(64) NOT NULL, \n\tentity_id VARCHAR(128) NOT NULL, \n\tarchived BOOLEAN DEFAULT 'false' NOT NULL, \n\trevision BIGINT DEFAULT '1' NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tPRIMARY KEY (provider_system, provider_id, location_id), \n\tCONSTRAINT manual_binding_provider CHECK (btrim(provider_id)<>'' AND btrim(entity_type)<>'' AND btrim(entity_id)<>''), \n\tCONSTRAINT manual_binding_record CHECK (location_key ~ '^[0-9a-f]{64}$' AND revision>0), \n\tCONSTRAINT manual_binding_provider_system CHECK (provider_system IN ('npi','provider_directory','manual')), \n\tCONSTRAINT manual_binding_location_key UNIQUE (location_key), \n\tCONSTRAINT manual_binding_location CHECK (location_id<>'00000000-0000-0000-0000-000000000000'::uuid)\n)",
)


def _ddl(schema):
    """Render the frozen native schema without per-record database hooks."""
    namespace = '"' + schema.replace('"', '""') + '"'
    statements = [statement.replace("registry_placeholder.", namespace + ".") for statement in _DDL]
    for table, constraint in (
        ("registry_record_history", "registry_history_kind"),
        ("registry_approved_record", "registry_approved_kind"),
    ):
        statements.extend(
            (
                f"ALTER TABLE {namespace}.{table} DROP CONSTRAINT {constraint}",
                f"ALTER TABLE {namespace}.{table} ADD CONSTRAINT {constraint} CHECK "
                "(record_kind IN ('group','company','network','company_links','provider','location'))",
            )
        )
    return statements


def upgrade():
    """Add durable manual identities while retaining all existing history."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Require an explicit retained-data migration before removing manual identities."""
    raise RuntimeError("Manual directory identities require an explicit retained-data migration")
