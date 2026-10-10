# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain reviewed source sites beside replaceable source snapshots."""

import os
import re

from alembic import op

revision = "20261007110000_registry_site_bindings"
down_revision = "20261007100000_registry_publication_requests"
branch_labels = None
depends_on = None


def _ddl(schema):
    """Create native binding constraints and extend durable record history."""
    namespace = '"' + schema + '"'
    statements = [
        f"""CREATE TABLE {namespace}.registry_site_binding (
      binding_id UUID PRIMARY KEY, source_generation BIGINT NOT NULL,
      provider_system VARCHAR(32) NOT NULL, provider_id VARCHAR(128) NOT NULL,
      location_id UUID NOT NULL, location_key VARCHAR(64) NOT NULL,
      address_row_sha256 VARCHAR(64) NOT NULL, source_receipt_json JSONB NOT NULL,
      archived BOOLEAN NOT NULL DEFAULT false, revision BIGINT NOT NULL DEFAULT 1,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT registry_site_binding_identity CHECK(binding_id<>'00000000-0000-0000-0000-000000000000'::uuid),
      CONSTRAINT registry_site_binding_location CHECK(location_id<>'00000000-0000-0000-0000-000000000000'::uuid),
      CONSTRAINT registry_site_binding_revision CHECK(source_generation>0 AND revision>0),
      CONSTRAINT registry_site_binding_system CHECK(provider_system IN ('npi','provider_directory')),
      CONSTRAINT registry_site_binding_provider CHECK(provider_id<>'' AND provider_id=btrim(provider_id) AND provider_id !~ '[[:cntrl:]]'),
      CONSTRAINT registry_site_binding_npi CHECK(provider_system<>'npi' OR provider_id ~ '^[12][0-9]{{9}}$'),
      CONSTRAINT registry_site_binding_hashes CHECK(location_key ~ '^[0-9a-f]{{64}}$' AND address_row_sha256 ~ '^[0-9a-f]{{64}}$'),
      CONSTRAINT registry_site_binding_receipt CHECK(jsonb_typeof(source_receipt_json)='object' AND octet_length(source_receipt_json::text)<=65536))"""
    ]
    for table, constraint in (
        ("registry_record_history", "registry_history_kind"),
        ("registry_approved_record", "registry_approved_kind"),
    ):
        statements.append(f"ALTER TABLE {namespace}.{table} DROP CONSTRAINT {constraint}")
        statements.append(
            f"ALTER TABLE {namespace}.{table} ADD CONSTRAINT {constraint} CHECK "
            "(record_kind IN ('group','company','network','company_links','provider','location','membership','site_binding'))"
        )
    return statements


def upgrade():
    """Create the retained table and extend the approved-map kind constraints."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Reject destructive removal of retained source-site history."""
    raise RuntimeError("Source site bindings require an explicit retained-data migration")
