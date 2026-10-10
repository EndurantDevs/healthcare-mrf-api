# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain explicit approved selections without including other pending drafts."""

import os
import re

from alembic import op

revision = "20261007060000_registry_approved_selection"
down_revision = "20261007050000_canonical_address_network_ids"
branch_labels = None
depends_on = None

_DDL = (
    "CREATE TABLE {qualified}registry_approval_history (\n\tapproved_revision BIGSERIAL NOT NULL, \n\tprevious_approved_revision BIGINT NOT NULL, \n\texpected_draft_revision BIGINT NOT NULL, \n\tactor_key VARCHAR(128) NOT NULL, \n\tactor_json JSONB NOT NULL, \n\tselection_json JSONB NOT NULL, \n\treason VARCHAR(1000) NOT NULL, \n\tidempotency_key VARCHAR(128) NOT NULL, \n\trequest_sha256 VARCHAR(64) NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tPRIMARY KEY (approved_revision), \n\tCONSTRAINT registry_approval_order CHECK (approved_revision > 0 AND previous_approved_revision >= 0 AND approved_revision > previous_approved_revision), \n\tCONSTRAINT registry_approval_draft CHECK (expected_draft_revision >= previous_approved_revision AND approved_revision=expected_draft_revision+1), \n\tCONSTRAINT registry_approval_json CHECK (jsonb_typeof(actor_json)='object' AND jsonb_typeof(selection_json)='array'), \n\tCONSTRAINT registry_approval_reason CHECK (btrim(reason)<>'' AND btrim(idempotency_key)<>''), \n\tCONSTRAINT registry_approval_digest CHECK (request_sha256 ~ '^[0-9a-f]{{64}}$'), \n\tCONSTRAINT registry_approval_replay UNIQUE (actor_key, idempotency_key)\n)",
    "CREATE TABLE {qualified}registry_approved_record (\n\tapproved_revision BIGINT NOT NULL, \n\trecord_kind VARCHAR(16) NOT NULL, \n\trecord_key VARCHAR(36) NOT NULL, \n\trecord_revision BIGINT NOT NULL, \n\tcustom_revision BIGINT NOT NULL, \n\trecord_json JSONB NOT NULL, \n\tPRIMARY KEY (approved_revision, record_kind, record_key), \n\tCONSTRAINT registry_approved_kind CHECK (record_kind IN ('group','company','network')), \n\tCONSTRAINT registry_approved_revision CHECK (approved_revision>0 AND record_revision>0 AND custom_revision>0 AND custom_revision<approved_revision), \n\tCONSTRAINT registry_approved_json CHECK (jsonb_typeof(record_json)='object')\n)",
)


def _ddl(schema):
    """Render native approval storage without custom database hooks."""
    qualified = '"' + schema.replace('"', '""') + '".'
    return [statement.format(qualified=qualified) for statement in _DDL]


def upgrade():
    """Keep exact approved record sets outside source swaps."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Require explicit history retention before removing approvals."""
    raise RuntimeError("Approved registry snapshots require an explicit retained-data migration")
