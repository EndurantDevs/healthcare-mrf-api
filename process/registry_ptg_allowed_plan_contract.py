# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed review identity for published allowed evidence without negotiated layouts."""

import datetime
import json

from process import registry_ptg_published_plan_contract as published

REVIEW_TYPE = "published_allowed_amount_plan"
SELECTION_MODE = "complete_allowed_snapshot_source_set"
AUTHORITY_CONTRACT = "ptg_published_allowed_source_authority.v1"
IDENTITY_FIELDS = frozenset(
    {
        "snapshot_id",
        "import_run_id",
        "import_month",
        "source_key",
        "snapshot_manifest_sha256",
        "plan_scopes_sha256",
        "file_versions_sha256",
        "source_count",
    }
)


def validated_identity(document):
    """Validate immutable allowed source coordinates and complete inventory digests."""
    published._object(document, IDENTITY_FIELDS)
    for name in ("snapshot_manifest_sha256", "plan_scopes_sha256", "file_versions_sha256"):
        published._digest(document[name])
    for name, maximum in (("snapshot_id", 96), ("import_run_id", 96), ("source_key", 256)):
        published._text(document[name], maximum, characters=True)
    month = datetime.date.fromisoformat(document["import_month"])
    published._require(month.day == 1 and month.isoformat() == document["import_month"])
    published._require(type(document["source_count"]) is int and 1 <= document["source_count"] <= 128)
    return dict(document)


def validated_intent(document):
    """Validate the explicit allowed review intent using existing client rules."""
    published._object(document, published.INTENT_FIELDS)
    published._require(document["review_type"] == REVIEW_TYPE and document["selection_mode"] == SELECTION_MODE)
    return published._validated_intent_fields(document, allow_empty_market=True)


def validated_command(document):
    """Bind allowed evidence to exact client ownership and complete selected files."""
    published._object(document, published.FULL_FIELDS)
    intent_by_field = validated_intent({name: document[name] for name in published.INTENT_FIELDS})
    published._require(document["operation"] == published.OPERATION)
    identity = validated_identity(document["published_identity"])
    ownership = published._ownership(document["ownership"])
    source_by_field = published._object(document["source"], {"binding_source_key", "snapshot_id", "ptg_schema_name"})
    published.RegistryPTGPublishedPlanSourceSpecification(
        intent_by_field["scope_id"], source_by_field["ptg_schema_name"], identity["snapshot_id"], identity["source_key"]
    )
    published._require(source_by_field["snapshot_id"] == identity["snapshot_id"] == ownership["snapshot_id"])
    published._require(source_by_field["binding_source_key"] == identity["source_key"] == ownership["source_key"])
    published._require(identity["import_run_id"] == ownership["engine_run_id"])
    published._require(identity["import_month"] == ownership["import_month"])
    published._require(len(intent_by_field["file_versions"]) == identity["source_count"])
    published._require(
        sum(
            version["source_file_version_id"] == ownership["engine_source_file_version_id"]
            and version["source_identity_sha256"] == ownership["engine_source_identity_hash"]
            for version in intent_by_field["file_versions"]
        )
        == 1
    )
    from process.registry_ptg_producer_scope import _digest

    published._require(_digest(intent_by_field["file_versions"]) == identity["file_versions_sha256"])
    published._require(all(ownership[name] == intent_by_field[name] for name in ("source_file_import_id", "client_id")))
    coordinates_by_field = {
        "source_system": "ptg",
        "source_id": identity["source_key"],
        "dataset_schema": source_by_field["ptg_schema_name"],
        "dataset_id": identity["snapshot_id"],
        "producer_id": AUTHORITY_CONTRACT,
        "edition_id": identity["snapshot_manifest_sha256"],
    }
    published._require(document["coordinates"] == coordinates_by_field)
    closed_by_field = {
        **intent_by_field,
        "operation": published.OPERATION,
        "coordinates": coordinates_by_field,
        "ownership": ownership,
        "source": dict(source_by_field),
        "published_identity": identity,
    }
    encoded = json.dumps(closed_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    published._require(len(encoded.encode()) <= 65536)
    return json.loads(encoded)
