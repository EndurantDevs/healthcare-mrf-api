# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authoritative snapshot/run attachment registry for PTG attempt fencing."""

from __future__ import annotations

from dataclasses import dataclass

from process.ptg_parts.snapshot_tables import _ptg2_snapshot_table_token


@dataclass(frozen=True)
class AttemptAttachment:
    """One durable table whose rows attach work to an import attempt."""

    name: str
    table_name: str
    snapshot_columns: tuple[str, ...] = ()
    run_columns: tuple[str, ...] = ()
    statement_trigger: bool = True


ATTEMPT_ATTACHMENTS = (
    AttemptAttachment(
        "layout_bindings",
        "ptg2_v3_snapshot_binding",
        ("snapshot_id",),
    ),
    AttemptAttachment(
        "snapshot_scope",
        "ptg2_v3_snapshot_scope",
        ("snapshot_id",),
    ),
    AttemptAttachment(
        "snapshot_plan_scope",
        "ptg2_v3_snapshot_plan_scope",
        ("snapshot_id",),
    ),
    AttemptAttachment(
        "snapshot_source",
        "ptg2_v3_snapshot_source",
        ("snapshot_id",),
    ),
    AttemptAttachment(
        "candidate_attestation",
        "ptg2_v3_candidate_audit_attestation",
        ("snapshot_id",),
    ),
    AttemptAttachment("snapshot_pin", "ptg2_snapshot_pin", ("snapshot_id",)),
    AttemptAttachment(
        "plan_release_binding",
        "plan_release_snapshot_binding",
        ("snapshot_id",),
    ),
    AttemptAttachment("plan_month", "ptg2_plan_month", ("snapshot_id",)),
    AttemptAttachment(
        "artifact_manifest",
        "ptg2_artifact_manifest",
        ("snapshot_id",),
        ("import_run_id",),
    ),
    AttemptAttachment(
        "allowed_amount_plan",
        "ptg2_allowed_amount_plan",
        ("snapshot_id",),
        (),
        False,
    ),
    AttemptAttachment(
        "allowed_amount_item",
        "ptg2_allowed_amount_item",
        ("snapshot_id",),
        (),
        False,
    ),
    AttemptAttachment(
        "allowed_amount_payment",
        "ptg2_allowed_amount_payment",
        ("snapshot_id",),
        (),
        False,
    ),
    AttemptAttachment(
        "allowed_amount_provider_payment",
        "ptg2_allowed_amount_provider_payment",
        ("snapshot_id",),
        (),
        False,
    ),
    AttemptAttachment(
        "current_snapshot",
        "ptg2_current_snapshot",
        ("snapshot_id", "previous_snapshot_id"),
    ),
    AttemptAttachment(
        "current_source_snapshot",
        "ptg2_current_source_snapshot",
        ("snapshot_id", "previous_snapshot_id"),
    ),
    AttemptAttachment(
        "current_plan_source",
        "ptg2_current_plan_source",
        ("snapshot_id", "previous_snapshot_id"),
    ),
    AttemptAttachment("import_job", "ptg2_import_job", (), ("import_run_id",)),
    AttemptAttachment(
        "source_catalog",
        "ptg2_source_catalog",
        (),
        ("import_run_id",),
        False,
    ),
    AttemptAttachment(
        "serving_rate",
        "ptg2_serving_rate",
        ("snapshot_id",),
        (),
        False,
    ),
    AttemptAttachment(
        "serving_rate_compact",
        "ptg2_serving_rate_compact",
        ("snapshot_id",),
        (),
        False,
    ),
    AttemptAttachment(
        "price_set_stage",
        "ptg2_price_set_stage",
        ("snapshot_id",),
        (),
        False,
    ),
    AttemptAttachment(
        "serving_rate_stage",
        "ptg2_serving_rate_stage",
        ("snapshot_id",),
        (),
        False,
    ),
    AttemptAttachment(
        "manifest_stage_registration",
        "ptg2_v4_attempt_stage",
        ("snapshot_id",),
        ("internal_run_id",),
    ),
)

ATTEMPT_STATE_TABLES = (
    AttemptAttachment(
        "snapshot_state",
        "ptg2_snapshot",
        ("snapshot_id",),
        ("import_run_id",),
    ),
    AttemptAttachment(
        "run_state",
        "ptg2_import_run",
        (),
        ("import_run_id",),
    ),
)

ATTEMPT_ATTACHMENT_BY_TABLE = {
    attachment.table_name: attachment
    for attachment in (*ATTEMPT_STATE_TABLES, *ATTEMPT_ATTACHMENTS)
}


def attempt_attachment_for_table(
    table_name: str,
) -> AttemptAttachment | None:
    """Return the single registry entry governing a durable PTG table."""

    return ATTEMPT_ATTACHMENT_BY_TABLE.get(str(table_name or ""))

MANIFEST_STAGE_KINDS = (
    "serving",
    "price_atom",
    "price_set_atom",
    "price_set_summary",
)


def manifest_stage_token(source_key: str, snapshot_id: str) -> str:
    """Return the immutable source/snapshot token used by stage tables."""

    return _ptg2_snapshot_table_token(source_key, snapshot_id)


def manifest_stage_name_for_kind(kind: str, token: str) -> str:
    """Return one deterministic manifest-stage table name."""

    safe_token = "".join(
        character if character.isalnum() else "_"
        for character in token.lower()
    ).strip("_")
    return f"ptg2_manifest_stage_{kind}_{safe_token}"[:63]


def manifest_stage_table_names(
    source_key: str,
    snapshot_id: str,
) -> tuple[str, ...]:
    """Return every deterministic stage table owned by one attempt."""

    token = manifest_stage_token(source_key, snapshot_id)
    return tuple(
        manifest_stage_name_for_kind(kind, token)
        for kind in MANIFEST_STAGE_KINDS
    )


__all__ = [
    "ATTEMPT_ATTACHMENTS",
    "ATTEMPT_ATTACHMENT_BY_TABLE",
    "ATTEMPT_STATE_TABLES",
    "AttemptAttachment",
    "MANIFEST_STAGE_KINDS",
    "attempt_attachment_for_table",
    "manifest_stage_name_for_kind",
    "manifest_stage_table_names",
    "manifest_stage_token",
]
