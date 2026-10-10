# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add immutable whole-company revision assertions without rewriting history."""

import os
import re

from alembic import op

revision = "20261009020000_company_registry_assertions"
down_revision = "20261009010000_registry_ptg_producer_scope"
branch_labels = None
depends_on = None


def _ddl(schema):
    if type(schema) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    namespace = '"' + schema + '"'
    return [
        f"""CREATE TABLE {namespace}.company_registry_role_assertion (
  assertion_id UUID NOT NULL,
  company_id UUID NOT NULL,
  company_revision BIGINT NOT NULL,
  role VARCHAR(32) NOT NULL,
  valid_from DATE NOT NULL,
  valid_to DATE,
  provenance_kind VARCHAR(32) NOT NULL,
  evidence_ref VARCHAR(128) NOT NULL,
  source_snapshot_id UUID,
  source_record_key VARCHAR(128),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
  PRIMARY KEY (assertion_id, company_id, company_revision),
  CONSTRAINT company_role_assertion_evidence CHECK (evidence_ref<>'' AND evidence_ref=btrim(evidence_ref) AND evidence_ref !~ '[[:cntrl:]]' AND octet_length(evidence_ref)<=128),
  CONSTRAINT company_role_assertion_revision_key UNIQUE (company_id, company_revision, role, valid_from),
  CONSTRAINT company_role_assertion_revision CHECK (company_revision>0),
  CONSTRAINT company_role_assertion_provenance CHECK ((provenance_kind='manual_reference' AND source_snapshot_id IS NULL AND source_record_key IS NULL) OR (provenance_kind='source_reference' AND source_snapshot_id IS NOT NULL AND source_snapshot_id<>'00000000-0000-0000-0000-000000000000'::uuid AND source_record_key IS NOT NULL AND source_record_key<>'' AND source_record_key=btrim(source_record_key) AND source_record_key !~ '[[:cntrl:]]' AND octet_length(source_record_key)<=128)),
  CONSTRAINT company_role_assertion_identity CHECK (assertion_id<>'00000000-0000-0000-0000-000000000000'::uuid AND company_id<>'00000000-0000-0000-0000-000000000000'::uuid),
  CONSTRAINT company_role_assertion_period CHECK (valid_to IS NULL OR valid_to>=valid_from),
  CONSTRAINT company_role_assertion_role CHECK (role IN ('insurer','employer','network_operator'))
)""",
        f"""CREATE TABLE {namespace}.company_registry_identifier_assertion (
  assertion_id UUID NOT NULL,
  company_id UUID NOT NULL,
  company_revision BIGINT NOT NULL,
  identifier_system VARCHAR(32) NOT NULL,
  identifier_scope VARCHAR(128) NOT NULL,
  identifier_value VARCHAR(64) NOT NULL,
  valid_from DATE NOT NULL,
  valid_to DATE,
  provenance_kind VARCHAR(32) NOT NULL,
  evidence_ref VARCHAR(128) NOT NULL,
  source_snapshot_id UUID,
  source_record_key VARCHAR(128),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
  PRIMARY KEY (assertion_id, company_id, company_revision),
  CONSTRAINT company_identifier_assertion_period CHECK (valid_to IS NULL OR valid_to>=valid_from),
  CONSTRAINT company_identifier_assertion_system CHECK (identifier_system IN ('ein','naic_company','external_reference')),
  CONSTRAINT company_identifier_assertion_format CHECK ((identifier_system='ein' AND identifier_value ~ '^[0-9]{{9}}$' AND identifier_value<>'000000000') OR (identifier_system='naic_company' AND identifier_value ~ '^[0-9]{{5}}$' AND identifier_value<>'00000') OR (identifier_system='external_reference' AND provenance_kind='source_reference')),
  CONSTRAINT company_identifier_assertion_evidence CHECK (evidence_ref<>'' AND evidence_ref=btrim(evidence_ref) AND evidence_ref !~ '[[:cntrl:]]' AND octet_length(evidence_ref)<=128),
  CONSTRAINT company_identifier_assertion_revision CHECK (company_revision>0),
  CONSTRAINT company_identifier_assertion_value CHECK (identifier_scope<>'' AND identifier_scope=btrim(identifier_scope) AND identifier_scope !~ '[[:cntrl:]]' AND octet_length(identifier_scope)<=128 AND identifier_value<>'' AND identifier_value=btrim(identifier_value) AND identifier_value !~ '[[:cntrl:]]' AND octet_length(identifier_value)<=64),
  CONSTRAINT company_identifier_assertion_identity CHECK (assertion_id<>'00000000-0000-0000-0000-000000000000'::uuid AND company_id<>'00000000-0000-0000-0000-000000000000'::uuid),
  CONSTRAINT company_identifier_assertion_revision_key UNIQUE (company_id, company_revision, identifier_system, identifier_scope, identifier_value),
  CONSTRAINT company_identifier_assertion_provenance CHECK ((provenance_kind='manual_reference' AND source_snapshot_id IS NULL AND source_record_key IS NULL) OR (provenance_kind='source_reference' AND source_snapshot_id IS NOT NULL AND source_snapshot_id<>'00000000-0000-0000-0000-000000000000'::uuid AND source_record_key IS NOT NULL AND source_record_key<>'' AND source_record_key=btrim(source_record_key) AND source_record_key !~ '[[:cntrl:]]' AND octet_length(source_record_key)<=128))
)""",
        f"REVOKE ALL ON {namespace}.company_registry_role_assertion FROM PUBLIC",
        f"REVOKE ALL ON {namespace}.company_registry_identifier_assertion FROM PUBLIC",
    ]


def upgrade():
    """Create additive revision tables; protected grants use the existing installer."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Require explicit retention handling before removing assertion history."""
    raise RuntimeError("Company assertions require an explicit retained-data migration")
