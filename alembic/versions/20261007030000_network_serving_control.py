# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Install candidate controls and retained network serving manifests."""

import os
import re

from alembic import op

revision = "20261007030000_network_serving_control"
down_revision = "20261007020000_registry_revision_history"
branch_labels = None
depends_on = None

_DDL = (
    """CREATE TABLE {qualified}network_membership_candidate (
    candidate_id UUID NOT NULL,
    dataset_id UUID NOT NULL,
    schema_id UUID NOT NULL,
    producer_id UUID NOT NULL,
    schema_name VARCHAR(64) NOT NULL,
    state VARCHAR(16) DEFAULT 'open' NOT NULL,
    schema_revision BIGINT DEFAULT '1' NOT NULL,
    source_generations JSONB NOT NULL,
    approved_custom_revision BIGINT NOT NULL,
    expected_head BIGINT NOT NULL,
    expected_rows BIGINT NOT NULL,
    accepted_rows BIGINT DEFAULT '0' NOT NULL,
    validation_json JSONB,
    index_ready BOOLEAN DEFAULT 'false' NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (candidate_id),
    CONSTRAINT network_candidate_state CHECK (state IN ('open','sealed','validated','ready','published','rejected')),
    CONSTRAINT network_candidate_validated CHECK (state NOT IN ('validated','ready','published') OR validation_json IS NOT NULL),
    CONSTRAINT network_candidate_revisions CHECK (schema_revision > 0 AND approved_custom_revision >= 0 AND expected_head >= 0),
    CONSTRAINT network_candidate_identity CHECK (candidate_id <> '00000000-0000-0000-0000-000000000000'::uuid AND dataset_id <> '00000000-0000-0000-0000-000000000000'::uuid AND schema_id <> '00000000-0000-0000-0000-000000000000'::uuid AND producer_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    CONSTRAINT network_candidate_accounting CHECK (expected_rows >= 0 AND accepted_rows >= 0 AND accepted_rows <= expected_rows),
    CONSTRAINT network_candidate_indexes CHECK (state NOT IN ('ready','published') OR index_ready),
    CONSTRAINT network_candidate_schema CHECK (schema_name = 'network_candidate_' || replace(candidate_id::text,'-','')),
    CONSTRAINT network_candidate_schema_unique UNIQUE (schema_name),
    CONSTRAINT network_candidate_sources CHECK (jsonb_typeof(source_generations) = 'object'),
    CONSTRAINT network_candidate_validation CHECK (validation_json IS NULL OR jsonb_typeof(validation_json) = 'object')
)""",
    """CREATE TABLE {qualified}network_membership_batch (
    candidate_id UUID NOT NULL,
    batch_id UUID NOT NULL,
    row_count INTEGER NOT NULL,
    input_sha256 VARCHAR(64) NOT NULL,
    copy_sha256 VARCHAR(64) NOT NULL,
    input_bytes INTEGER NOT NULL,
    copy_bytes INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (candidate_id, batch_id),
    CONSTRAINT network_batch_rows CHECK (row_count BETWEEN 0 AND 5000),
    CONSTRAINT network_batch_bytes CHECK (input_bytes BETWEEN 0 AND 8388608 AND copy_bytes BETWEEN 21 AND 16777216),
    FOREIGN KEY(candidate_id) REFERENCES {qualified}network_membership_candidate (candidate_id),
    CONSTRAINT network_batch_digests CHECK (input_sha256 ~ '^[0-9a-f]{{64}}$' AND copy_sha256 ~ '^[0-9a-f]{{64}}$')
)""",
    """CREATE TABLE {qualified}network_serving_manifest (
    generation_id BIGINT GENERATED ALWAYS AS IDENTITY (MINVALUE 1 NO CYCLE),
    candidate_id UUID NOT NULL,
    schema_revision BIGINT NOT NULL,
    source_generations JSONB NOT NULL,
    approved_custom_revision BIGINT NOT NULL,
    manifest_sha256 VARCHAR(64) NOT NULL,
    eligible BOOLEAN DEFAULT 'true' NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (generation_id),
    CONSTRAINT network_manifest_revisions CHECK (generation_id > 0 AND schema_revision > 0 AND approved_custom_revision >= 0),
    CONSTRAINT network_manifest_sources CHECK (jsonb_typeof(source_generations) = 'object'),
    CONSTRAINT network_manifest_candidate UNIQUE (candidate_id),
    CONSTRAINT network_manifest_digest CHECK (manifest_sha256 ~ '^[0-9a-f]{{64}}$'),
    FOREIGN KEY(candidate_id) REFERENCES {qualified}network_membership_candidate (candidate_id)
)""",
    """CREATE TABLE {qualified}network_serving_control (
    id INTEGER NOT NULL,
    generation_id BIGINT,
    PRIMARY KEY (id),
    CONSTRAINT network_serving_generation CHECK (generation_id IS NULL OR generation_id > 0),
    FOREIGN KEY(generation_id) REFERENCES {qualified}network_serving_manifest (generation_id),
    CONSTRAINT network_serving_singleton CHECK (id = 1)
)""",
)


def _ddl(schema):
    """Render the frozen native schema; source and custom records stay durable."""
    qualified = '"' + schema.replace('"', '""') + '".'
    statements = [statement.format(qualified=qualified) for statement in _DDL]
    return statements + [f"INSERT INTO {qualified}network_serving_control(id) VALUES(1)"]


def upgrade():
    """Add independent serving controls without changing any live snapshot."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Keep retained manifests until an explicit data-retention migration exists."""
    raise RuntimeError("Network generations require an explicit retained-data migration")
