# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Append-only independently funded failed Profile cleanup claims."""

import os

from alembic import op

revision = "20261001100000_profile_failed_cleanup_claim"
down_revision = "20261004000000_geo_assurance_dependency_bindings"
branch_labels = None
depends_on = None
TABLE = "provider_directory_profile_failed_cleanup_claim"


def upgrade():
    """Keep spent authority durable independently of the disposal transaction."""
    schema = '"' + (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").replace('"', '""') + '"'
    table = f'{schema}."{TABLE}"'
    op.execute(f"""CREATE TABLE {table} (
        operation_id varchar(64) PRIMARY KEY,
        authorization_sha256 varchar(64) NOT NULL UNIQUE CHECK (authorization_sha256 ~ '^[0-9a-f]{{64}}$'),
        reservation_id varchar(64) NOT NULL UNIQUE,
        nonce varchar(64) NOT NULL UNIQUE,
        build_id varchar(37) NOT NULL,
        owner_run_id varchar(64) NOT NULL,
        checkpoint_preimage_sha256 varchar(64) NOT NULL CHECK (checkpoint_preimage_sha256 ~ '^[0-9a-f]{{64}}$'),
        claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        expires_at timestamptz NOT NULL,
        max_operation_deadline timestamptz NOT NULL,
        authorization_json text NOT NULL CHECK (octet_length(authorization_json) <= 65536),
        signature varchar(86) NOT NULL,
        CHECK (claimed_at < expires_at AND claimed_at < max_operation_deadline),
        CHECK ((authorization_json::jsonb)->>'purpose' = 'failed_profile_cleanup'),
        CHECK ((authorization_json::jsonb)->>'operation_id' = operation_id),
        CHECK ((authorization_json::jsonb)->>'reservation_id' = reservation_id),
        CHECK ((authorization_json::jsonb)->>'nonce' = nonce)
    )""")
    op.execute(f"CREATE INDEX pd_profile_cleanup_expiry ON {table}(expires_at)")
    op.execute(f"""CREATE FUNCTION {schema}.reject_profile_cleanup_claim_mutation() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        RAISE EXCEPTION 'failed_profile_cleanup_claim_immutable'; END; $$""")
    op.execute(f"""CREATE TRIGGER pd_profile_cleanup_immutable_rows BEFORE UPDATE OR DELETE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_profile_cleanup_claim_mutation()""")
    op.execute(f"""CREATE TRIGGER pd_profile_cleanup_immutable_truncate BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_profile_cleanup_claim_mutation()""")

    op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER pd_profile_cleanup_immutable_rows")
    op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER pd_profile_cleanup_immutable_truncate")


def downgrade():
    """Never erase spent authority, even after its reservation expiry."""
    raise RuntimeError("failed_profile_cleanup_claim_requires_retention")
