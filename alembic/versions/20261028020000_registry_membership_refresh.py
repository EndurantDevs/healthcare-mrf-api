# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep exact background refresh requests in the existing protected publication queue."""

import os
import re

from alembic import op

revision = "20261028020000_registry_membership_refresh"
down_revision = "20261028010000_registry_fhir_catalog"
branch_labels = None
depends_on = None


def upgrade():
    """Only the closed refresh service operation may omit a human session hash."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    namespace = '"' + schema + '"'
    op.execute(
        f"ALTER TABLE {namespace}.registry_publication_request ADD CONSTRAINT registry_request_session_actor CHECK("
        "CASE WHEN command_json->>'operation'='refresh_memberships' THEN "
        "session_token_sha256 IS NULL AND actor_json="
        '\'{"kind":"service","service_id":"registry-membership-refresh"}\'::jsonb '
        "ELSE session_token_sha256 IS NOT NULL AND actor_json->>'kind' IS DISTINCT FROM 'service' END)"
    )
    op.execute(f"ALTER TABLE {namespace}.registry_publication_request ALTER COLUMN session_token_sha256 DROP NOT NULL")


def downgrade():
    """Require an explicit retained-request migration before removing service history."""
    raise RuntimeError("Refresh requests require an explicit retained-data migration")
