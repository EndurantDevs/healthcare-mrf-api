# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed published complete-snapshot review wire grammar."""

import json
import re
from dataclasses import dataclass
from uuid import UUID

REVIEW_TYPE = "published_complete_snapshot_plan"
SELECTION_MODE = "complete_snapshot_source_set"
OPERATION = "approve_ptg_published_plan_scope"
AUTHORITY_CONTRACT = "ptg_published_result_source_authority.v1"
INTENT_FIELDS = frozenset(
    {
        "review_type",
        "scope_id",
        "statement_id",
        "client_id",
        "legal_company_id",
        "network_id",
        "approved_revision",
        "source_file_import_id",
        "plan_id",
        "plan_market_type",
        "selection_mode",
        "file_versions",
        "reason",
        "idempotency_key",
    }
)
FULL_FIELDS = INTENT_FIELDS | {
    "operation",
    "coordinates",
    "ownership",
    "source",
    "published_identity",
}
IDENTITY_FIELDS = frozenset(
    {
        "snapshot_id",
        "import_run_id",
        "import_month",
        "source_key",
        "snapshot_manifest_sha256",
        "snapshot_key",
        "plan_id",
        "plan_market_type",
        "coverage_scope_id",
        "source_set_digest",
        "source_count",
        "plan_scopes_sha256",
        "source_assignments_sha256",
        "layout_mapping_digest",
        "map_digest",
        "finalizer_map_digest",
    }
)
DIGEST_FIELDS = IDENTITY_FIELDS - {
    "snapshot_id",
    "import_run_id",
    "import_month",
    "source_key",
    "snapshot_key",
    "plan_id",
    "plan_market_type",
    "source_count",
}
OWNERSHIP_FIELDS = frozenset(
    {
        "source_file_import_id",
        "client_id",
        "source_file_id",
        "content_version",
        "import_month",
        "assigned_node_id",
        "status",
        "engine_run_id",
        "snapshot_id",
        "source_key",
        "engine_source_identity_hash",
        "engine_source_file_version_id",
    }
)


def _require(condition):
    if not condition:
        raise ValueError("registry_ptg_scope_request_invalid")


def _object(document, fields):
    _require(type(document) is dict and set(document) == fields)
    return document


def _text(value, maximum, *, characters=False):
    _require(
        type(value) is str
        and bool(value)
        and value == value.strip()
        and all(character.isprintable() for character in value)
    )
    encoded = value.encode("utf-8")
    _require((len(value) if characters else len(encoded)) <= maximum)
    return value


def _uuid(value):
    _require(type(value) is str)
    parsed = UUID(value)
    _require(parsed.int and str(parsed) == value)


def _digest(value):
    _require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None)


def _files(versions):
    _require(type(versions) is list and 1 <= len(versions) <= 128)
    selected_versions = []
    for version_by_field in versions:
        _object(version_by_field, {"source_file_version_id", "source_identity_sha256", "raw_sha256"})
        _text(version_by_field["source_file_version_id"], 128)
        _require(
            type(version_by_field["source_identity_sha256"]) is str
            and re.fullmatch(r"(?:[0-9a-f]{16}|[0-9a-f]{32}|[0-9a-f]{64})", version_by_field["source_identity_sha256"])
            is not None
        )
        _digest(version_by_field["raw_sha256"])
        selected_versions.append(dict(version_by_field))
    _require(len({version["source_file_version_id"] for version in selected_versions}) == len(selected_versions))
    return sorted(selected_versions, key=lambda version: version["source_file_version_id"])


def validated_intent(document):
    """No company/cohort labels or producer evidence may be supplied as authority."""
    _object(document, INTENT_FIELDS)
    _require(document["review_type"] == REVIEW_TYPE and document["selection_mode"] == SELECTION_MODE)
    for name in ("scope_id", "statement_id", "legal_company_id"):
        _uuid(document[name])
    _require(type(document["network_id"]) is int and 1 <= document["network_id"] <= 2147483647)
    _require(type(document["approved_revision"]) is int and 1 <= document["approved_revision"] < 2**63)
    for name, maximum in {"client_id": 64, "source_file_import_id": 64, "reason": 1000, "idempotency_key": 128}.items():
        _text(document[name], maximum)
    _require(document["client_id"] not in {"system", "__platform__"})
    _text(document["plan_id"], 64, characters=True)
    _text(document["plan_market_type"], 32, characters=True)
    _require(document["plan_market_type"] == document["plan_market_type"].lower())
    return {**document, "file_versions": _files(document["file_versions"])}


def _identity(document):
    _object(document, IDENTITY_FIELDS)
    for name in DIGEST_FIELDS:
        _digest(document[name])
    for name, maximum in {
        "snapshot_id": 96,
        "import_run_id": 96,
        "import_month": 16,
        "source_key": 256,
        "plan_id": 64,
        "plan_market_type": 32,
    }.items():
        _text(document[name], maximum, characters=True)
    _require(type(document["snapshot_key"]) is int and 0 < document["snapshot_key"] < 2**63)
    _require(type(document["source_count"]) is int and 1 <= document["source_count"] <= 128)
    return dict(document)


def _ownership(document):
    _object(document, OWNERSHIP_FIELDS)
    for name, value in document.items():
        if name == "status" and value == "" and type(value) is str:
            continue
        if name == "engine_source_identity_hash":
            _require(
                type(value) is str and re.fullmatch(r"(?:[0-9a-f]{16}|[0-9a-f]{32}|[0-9a-f]{64})", value) is not None
            )
        else:
            _text(
                value,
                {"snapshot_id": 96, "source_key": 128, "import_month": 16, "status": 32}.get(name, 64),
                characters=True,
            )
    return dict(document)


def validated_command(document):
    """Bind a distinct published plan review without a fabricated frozen witness."""
    _object(document, FULL_FIELDS)
    intent = validated_intent({name: document[name] for name in INTENT_FIELDS})
    _require(document["operation"] == OPERATION)
    identity = _identity(document["published_identity"])
    ownership = _ownership(document["ownership"])
    source_by_field = _object(document["source"], {"binding_source_key", "snapshot_id", "ptg_schema_name"})
    _require(
        type(source_by_field["ptg_schema_name"]) is str
        and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", source_by_field["ptg_schema_name"])
    )
    _require(source_by_field["snapshot_id"] == identity["snapshot_id"] == ownership["snapshot_id"])
    _require(source_by_field["binding_source_key"] == identity["source_key"] == ownership["source_key"])
    _require(identity["import_run_id"] == ownership["engine_run_id"])
    _require(len(intent["file_versions"]) == identity["source_count"])
    _require(all(ownership[name] == intent[name] for name in ("source_file_import_id", "client_id")))
    coordinates = _object(
        document["coordinates"],
        {"source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"},
    )
    _require(
        coordinates
        == {
            "source_system": "ptg",
            "source_id": identity["source_key"],
            "dataset_schema": source_by_field["ptg_schema_name"],
            "dataset_id": identity["snapshot_id"],
            "producer_id": AUTHORITY_CONTRACT,
            "edition_id": identity["snapshot_manifest_sha256"],
        }
    )
    closed_by_field = {
        **intent,
        "operation": OPERATION,
        "coordinates": dict(coordinates),
        "ownership": ownership,
        "source": dict(source_by_field),
        "published_identity": identity,
    }
    encoded = json.dumps(
        closed_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    _require(len(encoded) <= 65536)
    return json.loads(encoded)


@dataclass(frozen=True)
class RegistryPTGPublishedPlanSourceSpecification:
    """Exact published review/source coordinates; the object is not admission."""

    scope_id: str
    ptg_schema_name: str
    snapshot_id: str
    binding_source_key: str

    def __post_init__(self):
        _uuid(self.scope_id)
        _require(
            type(self.ptg_schema_name) is str and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", self.ptg_schema_name)
        )
        _text(self.snapshot_id, 96, characters=True)
        _text(self.binding_source_key, 256, characters=True)

    @property
    def operation_id(self):
        """Reuse the original protected review's operation-owned source pin."""
        return "registry_ptg_published_review_" + self.scope_id.replace("-", "")
