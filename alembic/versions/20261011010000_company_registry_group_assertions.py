# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain dated group assertions in immutable whole-company revisions."""

import os
import re

from alembic import op

revision = "20261011010000_company_registry_group_assertions"
down_revision = "20261010030000_registry_source_admission_receipt"
branch_labels = None
depends_on = None


def _ddl(schema):
    if type(schema) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    namespace = '"' + schema + '"'
    return [
        f"""CREATE TABLE {namespace}.company_registry_group_assertion (
  assertion_id UUID NOT NULL,
  company_id UUID NOT NULL,
  company_revision BIGINT NOT NULL,
  group_id UUID,
  group_kind VARCHAR(32) NOT NULL,
  relationship_kind VARCHAR(32) NOT NULL,
  valid_from DATE,
  valid_to DATE,
  provenance_kind VARCHAR(32) NOT NULL,
  evidence_ref VARCHAR(128) NOT NULL,
  source_snapshot_id UUID,
  source_record_key VARCHAR(128),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
  PRIMARY KEY (assertion_id, company_id, company_revision),
  CONSTRAINT company_group_assertion_identity CHECK (assertion_id<>'00000000-0000-0000-0000-000000000000'::uuid AND company_id<>'00000000-0000-0000-0000-000000000000'::uuid),
  CONSTRAINT company_group_assertion_revision CHECK (company_revision>0),
  CONSTRAINT company_group_assertion_period CHECK (valid_to IS NULL OR valid_to>=valid_from),
  CONSTRAINT company_group_assertion_evidence CHECK (evidence_ref<>'' AND evidence_ref=btrim(evidence_ref) AND evidence_ref !~ '[[:cntrl:]]' AND octet_length(evidence_ref)<=128),
  CONSTRAINT company_group_assertion_provenance CHECK ((provenance_kind='manual_reference' AND source_snapshot_id IS NULL AND source_record_key IS NULL) OR (provenance_kind='source_reference' AND source_snapshot_id IS NOT NULL AND source_snapshot_id<>'00000000-0000-0000-0000-000000000000'::uuid AND source_record_key IS NOT NULL AND source_record_key<>'' AND source_record_key=btrim(source_record_key) AND source_record_key !~ '[[:cntrl:]]' AND octet_length(source_record_key)<=128)),
  CONSTRAINT company_group_assertion_group_identity CHECK (group_id IS NULL OR group_id<>'00000000-0000-0000-0000-000000000000'::uuid),
  CONSTRAINT company_group_assertion_group_kind CHECK (group_kind IN ('corporate_parent','naic_group')),
  CONSTRAINT company_group_assertion_relationship CHECK (relationship_kind IN ('reported_affiliation','verified_ownership')),
  CONSTRAINT company_group_assertion_manual_group CHECK (provenance_kind<>'manual_reference' OR group_id IS NOT NULL)
)""",
        f"CREATE INDEX company_group_assertion_revision_idx ON {namespace}.company_registry_group_assertion (company_id, company_revision)",
        f"REVOKE ALL ON {namespace}.company_registry_group_assertion FROM PUBLIC",
    ]


def upgrade():
    """Install only the additive assertion table; the protected installer owns grants."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Refuse to discard immutable history without an explicit retained-data migration."""
    raise RuntimeError("Company group assertions require an explicit retained-data migration")
