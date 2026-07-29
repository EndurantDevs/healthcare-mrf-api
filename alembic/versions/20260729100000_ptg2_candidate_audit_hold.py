"""Bind PTG candidate attestations to an explicit activation intent.

Revision ID: 20260729100000_ptg2_candidate_audit_hold
Revises: 20260728130000_provider_directory_content_proof_shards
"""

from __future__ import annotations

import hashlib
import os

from alembic import op
import sqlalchemy as sa


revision = "20260729100000_ptg2_candidate_audit_hold"
down_revision = "20260728130000_provider_directory_content_proof_shards"
branch_labels = None
depends_on = None

_ACTIVATION_INTENT = "audit_and_activate"
_DIGEST_DOMAIN = b"PTG2CANDIDATEAUDITINTENT\x01"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _attestation_digest(report_digest: bytes) -> bytes:
    intent_bytes = _ACTIVATION_INTENT.encode("ascii")
    return hashlib.sha256(
        _DIGEST_DOMAIN
        + bytes(report_digest)
        + len(intent_bytes).to_bytes(2, "big")
        + intent_bytes
    ).digest()


def upgrade() -> None:
    schema = _schema()
    table_name = "ptg2_v3_candidate_audit_attestation"
    op.add_column(
        table_name,
        sa.Column(
            "activation_intent",
            sa.String(length=32),
            server_default=_ACTIVATION_INTENT,
            nullable=False,
        ),
        schema=schema,
    )
    op.add_column(
        table_name,
        sa.Column("attestation_digest", sa.LargeBinary(), nullable=True),
        schema=schema,
    )
    intent_bytes = _ACTIVATION_INTENT.encode("ascii")
    digest_prefix_hex = _DIGEST_DOMAIN.hex()
    digest_suffix_hex = (
        len(intent_bytes).to_bytes(2, "big") + intent_bytes
    ).hex()
    op.execute(
        f'UPDATE "{schema}"."{table_name}" '
        "SET attestation_digest = sha256("
        f"decode('{digest_prefix_hex}', 'hex') || report_digest || "
        f"decode('{digest_suffix_hex}', 'hex'))"
    )
    op.alter_column(
        table_name,
        "attestation_digest",
        existing_type=sa.LargeBinary(),
        nullable=False,
        schema=schema,
    )
    op.create_check_constraint(
        "ptg2_v3_candidate_audit_attestation_intent_check",
        table_name,
        "activation_intent IN ('audit_and_activate', 'audit_only')",
        schema=schema,
    )
    op.create_check_constraint(
        "ptg2_v3_candidate_audit_attestation_digest_check",
        table_name,
        "octet_length(attestation_digest) = 32",
        schema=schema,
    )


def downgrade() -> None:
    schema = _schema()
    table_name = "ptg2_v3_candidate_audit_attestation"
    connection = op.get_bind()
    held_count = connection.execute(
        sa.text(
            f'SELECT COUNT(*) FROM "{schema}"."{table_name}" '
            "WHERE activation_intent = 'audit_only'"
        )
    ).scalar_one()
    if int(held_count) > 0:
        raise RuntimeError(
            "cannot remove candidate audit hold while held attestations exist"
        )
    op.drop_constraint(
        "ptg2_v3_candidate_audit_attestation_digest_check",
        table_name,
        type_="check",
        schema=schema,
    )
    op.drop_constraint(
        "ptg2_v3_candidate_audit_attestation_intent_check",
        table_name,
        type_="check",
        schema=schema,
    )
    op.drop_column(table_name, "attestation_digest", schema=schema)
    op.drop_column(table_name, "activation_intent", schema=schema)
