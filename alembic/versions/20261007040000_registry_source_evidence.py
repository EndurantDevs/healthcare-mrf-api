# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain source periods and identifier evidence outside imported swaps."""

import os
import re

from alembic import op

revision = "20261007040000_registry_source_evidence"
down_revision = "20261007030000_network_serving_control"
branch_labels = None
depends_on = None

_DDL = (
    """CREATE TABLE {qualified}registry_source_snapshot (
	snapshot_id UUID NOT NULL,
	source_system VARCHAR(64) NOT NULL,
	source_id VARCHAR(128) NOT NULL,
	edition_id VARCHAR(128) NOT NULL,
	source_url VARCHAR(2048) NOT NULL,
	artifact_sha256 VARCHAR(64) NOT NULL,
	input_sha256 VARCHAR(64) NOT NULL,
	parser_version VARCHAR(128) NOT NULL,
	reporting_year SMALLINT,
	published_at TIMESTAMP WITH TIME ZONE,
	retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (snapshot_id),
	CONSTRAINT registry_snapshot_source CHECK (btrim(source_system)<>'' AND btrim(source_id)<>'' AND btrim(edition_id)<>'' AND btrim(parser_version)<>''),
	CONSTRAINT registry_snapshot_digests CHECK (artifact_sha256 ~ '^[0-9a-f]{{64}}$' AND input_sha256 ~ '^[0-9a-f]{{64}}$'),
	CONSTRAINT registry_snapshot_year CHECK (reporting_year IS NULL OR reporting_year BETWEEN 2010 AND 2100),
	CONSTRAINT registry_snapshot_identity CHECK (snapshot_id<>'00000000-0000-0000-0000-000000000000'::uuid AND btrim(source_url)<>''),
	CONSTRAINT registry_snapshot_edition UNIQUE (source_system, source_id, edition_id, input_sha256, parser_version)
)""",
    """CREATE TABLE {qualified}registry_source_observation (
	snapshot_id UUID NOT NULL,
	source_record_key VARCHAR(128) NOT NULL,
	source_row_number INTEGER NOT NULL,
	status VARCHAR(16) NOT NULL,
	observation_json JSONB NOT NULL,
	issues_json JSONB NOT NULL,
	PRIMARY KEY (snapshot_id, source_record_key),
	CONSTRAINT registry_observation_key CHECK (source_row_number > 0 AND btrim(source_record_key)<>''),
	CONSTRAINT registry_observation_status CHECK (status IN ('accepted','unresolved','rejected')),
	CONSTRAINT registry_observation_json CHECK (jsonb_typeof(observation_json)='object' AND jsonb_typeof(issues_json)='array')
)""",
    """CREATE TABLE {qualified}registry_identifier_observation (
	snapshot_id UUID NOT NULL,
	source_record_key VARCHAR(128) NOT NULL,
	identifier_system VARCHAR(32) NOT NULL,
	identifier_value VARCHAR(64) NOT NULL,
	entity_kind VARCHAR(16) NOT NULL,
	raw_value VARCHAR(128) NOT NULL,
	entity_id UUID,
	resolution_status VARCHAR(16) NOT NULL,
	PRIMARY KEY (snapshot_id, source_record_key, identifier_system, identifier_value, entity_kind),
	CONSTRAINT registry_identifier_kind CHECK (entity_kind IN ('group','company')),
	CONSTRAINT registry_identifier_system CHECK (identifier_system IN ('ein','naic_company','naic_group')),
	CONSTRAINT registry_identifier_value CHECK (btrim(identifier_value)<>'' AND btrim(raw_value)<>''),
	CONSTRAINT registry_identifier_format CHECK ((entity_kind='company' AND ((identifier_system='ein' AND identifier_value ~ '^[0-9]{{9}}$') OR (identifier_system='naic_company' AND identifier_value ~ '^[0-9]{{5}}$' AND identifier_value<>'00000'))) OR (entity_kind='group' AND identifier_system='naic_group' AND identifier_value ~ '^[1-9][0-9]{{0,63}}$')),
	CONSTRAINT registry_identifier_status CHECK (resolution_status IN ('resolved','unresolved','conflicting')),
	CONSTRAINT registry_identifier_resolution CHECK ((resolution_status='resolved') = (entity_id IS NOT NULL))
)""",
    """CREATE TABLE {qualified}registry_identifier_binding (
	entity_kind VARCHAR(16) NOT NULL,
	identifier_system VARCHAR(32) NOT NULL,
	identifier_value VARCHAR(64) NOT NULL,
	entity_id UUID NOT NULL,
	snapshot_id UUID NOT NULL,
	evidence_key VARCHAR(128) NOT NULL,
	PRIMARY KEY (entity_kind, identifier_system, identifier_value),
	CONSTRAINT registry_binding_kind CHECK (entity_kind IN ('group','company')),
	CONSTRAINT registry_binding_system CHECK (identifier_system IN ('ein','naic_company','naic_group')),
	CONSTRAINT registry_binding_evidence CHECK (btrim(identifier_value)<>'' AND btrim(evidence_key)<>''),
	CONSTRAINT registry_binding_format CHECK ((entity_kind='company' AND ((identifier_system='ein' AND identifier_value ~ '^[0-9]{{9}}$') OR (identifier_system='naic_company' AND identifier_value ~ '^[0-9]{{5}}$' AND identifier_value<>'00000'))) OR (entity_kind='group' AND identifier_system='naic_group' AND identifier_value ~ '^[1-9][0-9]{{0,63}}$')),
	CONSTRAINT registry_binding_identity CHECK (entity_id<>'00000000-0000-0000-0000-000000000000'::uuid)
)""",
    """CREATE TABLE {qualified}registry_issuer_company_assertion (
	snapshot_id UUID NOT NULL,
	source_record_key VARCHAR(128) NOT NULL,
	hios_issuer_id VARCHAR(5) NOT NULL,
	state VARCHAR(2) NOT NULL,
	company_id UUID,
	resolution_status VARCHAR(16) NOT NULL,
	valid_from DATE,
	valid_to DATE,
	PRIMARY KEY (snapshot_id, source_record_key),
	CONSTRAINT registry_issuer_assertion_identity CHECK (hios_issuer_id ~ '^[0-9]{{5}}$' AND hios_issuer_id<>'00000' AND state ~ '^[A-Z]{{2}}$'),
	CONSTRAINT registry_issuer_assertion_status CHECK (resolution_status IN ('resolved','unresolved','conflicting')),
	CONSTRAINT registry_issuer_assertion_resolution CHECK ((resolution_status='resolved') = (company_id IS NOT NULL)),
	CONSTRAINT registry_issuer_assertion_period CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to>=valid_from)
)""",
    """CREATE TABLE {qualified}registry_company_group_assertion (
	snapshot_id UUID NOT NULL,
	source_record_key VARCHAR(128) NOT NULL,
	company_id UUID NOT NULL,
	group_id UUID,
	relationship_kind VARCHAR(32) NOT NULL,
	resolution_status VARCHAR(16) NOT NULL,
	valid_from DATE,
	valid_to DATE,
	PRIMARY KEY (snapshot_id, source_record_key),
	CONSTRAINT registry_group_assertion_kind CHECK (relationship_kind IN ('reported_affiliation','verified_ownership')),
	CONSTRAINT registry_group_assertion_status CHECK (resolution_status IN ('resolved','unresolved','conflicting')),
	CONSTRAINT registry_group_assertion_resolution CHECK ((resolution_status='resolved') = (group_id IS NOT NULL)),
	CONSTRAINT registry_group_assertion_period CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to>=valid_from)
)""",
    """CREATE INDEX registry_observation_status_idx ON {qualified}registry_source_observation (snapshot_id, status)""",
    """CREATE INDEX registry_identifier_lookup_idx ON {qualified}registry_identifier_observation (identifier_system, identifier_value)""",
    """CREATE INDEX registry_binding_entity_idx ON {qualified}registry_identifier_binding (entity_kind, entity_id)""",
    """CREATE INDEX registry_issuer_assertion_lookup_idx ON {qualified}registry_issuer_company_assertion (hios_issuer_id, state)""",
    """CREATE INDEX registry_group_assertion_lookup_idx ON {qualified}registry_company_group_assertion (company_id, valid_from)""",
)


def _ddl(schema):
    """Render additive native evidence tables without per-row hooks."""
    qualified = '"' + schema.replace('"', '""') + '".'
    return [statement.format(qualified=qualified) for statement in _DDL]


def upgrade():
    """Keep immutable source editions independent of custom drafts."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Require explicit retained-data handling before removing evidence."""
    raise RuntimeError("Registry evidence requires an explicit retained-data migration")
