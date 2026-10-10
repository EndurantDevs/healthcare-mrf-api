# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed whole-office action syntax; native source proof is a separate gate."""

import json
import re
from uuid import UUID

OPERATION = "review_registry_ptg_office_capture"
FIELDS = frozenset(
    {
        "operation",
        "capture_id",
        "scope_id",
        "client_id",
        "scope_approval_sha256",
        "canonical_input_sha256",
        "input_row_count",
        "office_evidence_kind",
        "retained_generation_id",
        "retained_serving",
        "retained_serving_sha256",
        "retained_sites",
        "source",
        "reason",
        "idempotency_key",
    }
)
SOURCE_FIELDS = frozenset(
    {
        "coordinates",
        "binding_source_key",
        "snapshot_id",
        "legal_company_id",
        "approved_revision",
        "file_versions",
        "evidence",
        "source_scope",
        "network_id",
    }
)
SCOPE_FIELDS = frozenset(
    {"review_type", "scope_id", "plan_id", "plan_market_type", "selection_mode", "snapshot_id", "approval_sha256"}
)


def _object(document, fields):
    if type(document) is not dict or set(document) != fields:
        raise ValueError("registry_ptg_office_review_invalid")
    return document


def _text(value, maximum):
    if (
        type(value) is not str
        or not 1 <= len(value.encode()) <= maximum
        or value.strip() != value
        or not value.isprintable()
    ):
        raise ValueError("registry_ptg_office_review_invalid")


def _uuid(value):
    if type(value) is not str or not UUID(value).int or str(UUID(value)) != value:
        raise ValueError("registry_ptg_office_review_invalid")


def _digest(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("registry_ptg_office_review_invalid")


def _source(source_by_field, command_by_field):
    _object(source_by_field, SOURCE_FIELDS)
    scope_by_field = _object(source_by_field["source_scope"], SCOPE_FIELDS)
    if (
        scope_by_field["review_type"] != "published_complete_snapshot_plan"
        or scope_by_field["selection_mode"] != "complete_snapshot_source_set"
        or scope_by_field["scope_id"] != command_by_field["scope_id"]
        or scope_by_field["approval_sha256"] != command_by_field["scope_approval_sha256"]
        or scope_by_field["snapshot_id"] != source_by_field["snapshot_id"]
    ):
        raise ValueError("registry_ptg_office_review_invalid")
    _uuid(source_by_field["legal_company_id"])
    for name, maximum in (("binding_source_key", 256), ("snapshot_id", 96)):
        _text(source_by_field[name], maximum)
    for name, maximum in (("plan_id", 64), ("plan_market_type", 32)):
        _text(scope_by_field[name], maximum)
    if scope_by_field["plan_market_type"] != scope_by_field["plan_market_type"].lower():
        raise ValueError("registry_ptg_office_review_invalid")
    for name, maximum in (("network_id", 2147483647), ("approved_revision", 2**63 - 1)):
        if type(source_by_field[name]) is not int or not 1 <= source_by_field[name] <= maximum:
            raise ValueError("registry_ptg_office_review_invalid")
    _object(
        source_by_field["coordinates"],
        {"source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"},
    )
    if source_by_field["coordinates"]["source_system"] != "ptg":
        raise ValueError("registry_ptg_office_review_invalid")
    _object(source_by_field["evidence"], {"source_authority", "graph_identity", "selected_dense_source_keys"})
    if type(source_by_field["file_versions"]) is not list or not 1 <= len(source_by_field["file_versions"]) <= 128:
        raise ValueError("registry_ptg_office_review_invalid")


def validated_office_review_command(document):
    """Authenticate this exact command later; syntax never admits prepared offices."""
    _object(document, FIELDS)
    if document["operation"] != OPERATION or document["office_evidence_kind"] not in {
        "payer_exact_office",
        "reviewed_exact_office",
    }:
        raise ValueError("registry_ptg_office_review_invalid")
    for name in ("capture_id", "scope_id"):
        _uuid(document[name])
    for name in ("scope_approval_sha256", "canonical_input_sha256", "retained_serving_sha256"):
        _digest(document[name])
    for name, maximum in (("client_id", 64), ("reason", 1000), ("idempotency_key", 128)):
        _text(document[name], maximum)
    if document["client_id"] in {"system", "__platform__"}:
        raise ValueError("registry_ptg_office_review_invalid")
    for name, maximum in (("input_row_count", 1000000), ("retained_generation_id", 2**63 - 1)):
        if type(document[name]) is not int or not 1 <= document[name] <= maximum:
            raise ValueError("registry_ptg_office_review_invalid")
    _source(document["source"], document)
    _object(
        document["retained_serving"],
        {
            "generation_id",
            "candidate_id",
            "schema_name",
            "schema_revision",
            "source_generations",
            "approved_custom_revision",
            "manifest_sha256",
            "address_table_oid",
        },
    )
    _object(document["retained_sites"], {"identity", "rows_sha256"})
    _digest(document["retained_sites"]["rows_sha256"])
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    if len(encoded) > 131072:
        raise ValueError("registry_ptg_office_review_invalid")
    return json.loads(encoded)
