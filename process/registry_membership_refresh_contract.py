# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed service refresh commands preserve approved manual data and exact source scopes."""

import hashlib
import json

from process.registry_refresh_anchor_contract import (
    _counter,
    _hash,
    _object,
    _text,
    encoded_document,
    validated_enrollment,
)

SERVICE_ACTOR = {"kind": "service", "service_id": "registry-membership-refresh"}
COMMAND_FIELDS = {
    "operation",
    "expected_approved_revision",
    "expected_serving_generation",
    "expected_serving_manifest_sha256",
    "address_artifact_sha256",
    "accepted_source_vector_sha256",
    "accepted_sources",
    "enrolled_scopes",
    "reason",
    "idempotency_key",
}


class RegistryMembershipRefreshUnavailable(RuntimeError):
    """Native address or live service authority cannot prove this exact request."""


def validated_refresh_actor(actor):
    """Accept only the fixed service identity without fabricating a user or session."""
    if type(actor) is not dict or actor != SERVICE_ACTOR:
        raise ValueError("registry_membership_refresh_actor_invalid")
    return dict(actor)


def accepted_membership_source_sha256(accepted_sources):
    """Hash the complete sorted accepted identity vector using the shared wire recipe."""
    if type(accepted_sources) is not list or not 1 <= len(accepted_sources) <= 1000:
        raise ValueError("registry_membership_refresh_sources_invalid")
    identifiers = []
    for source_by_field in accepted_sources:
        _object(source_by_field, {"source_id", "capture_generation", "bindings_sha256", "transform_version"})
        for name, maximum in (("source_id", 256), ("capture_generation", 256), ("transform_version", 128)):
            _text(source_by_field[name], maximum, name)
        _hash(source_by_field["bindings_sha256"])
        identifiers.append(source_by_field["source_id"])
    if identifiers != sorted(set(identifiers)):
        raise ValueError("registry_membership_refresh_sources_invalid")
    return hashlib.sha256(b"registry-membership-accepted-sources-v1:" + encoded_document(accepted_sources)).hexdigest()


def validated_membership_refresh_command(command_by_field):
    """Validate exact pins and syntax; native source admission remains independent."""
    _object(command_by_field, COMMAND_FIELDS)
    if command_by_field["operation"] != "refresh_memberships":
        raise ValueError("registry_membership_refresh_operation_invalid")
    _counter(command_by_field["expected_approved_revision"])
    _counter(command_by_field["expected_serving_generation"])
    if command_by_field["expected_serving_generation"] == 0:
        if command_by_field["expected_serving_manifest_sha256"] is not None:
            raise ValueError("registry_membership_refresh_initial_head_invalid")
    else:
        _hash(command_by_field["expected_serving_manifest_sha256"])
    for name in ("address_artifact_sha256", "accepted_source_vector_sha256"):
        _hash(command_by_field[name])
    if (
        accepted_membership_source_sha256(command_by_field["accepted_sources"])
        != command_by_field["accepted_source_vector_sha256"]
    ):
        raise ValueError("registry_membership_refresh_source_digest_invalid")
    validated_enrollment(command_by_field["enrolled_scopes"])
    _text(command_by_field["reason"], 512, "reason")
    _text(command_by_field["idempotency_key"], 128, "idempotency_key")
    encoded_document(command_by_field)
    return command_by_field


def validated_membership_refresh_envelope(envelope_by_field):
    """The service operation carries a closed actor/command pair and no session hash."""
    _object(envelope_by_field, {"actor", "command"})
    validated_refresh_actor(envelope_by_field["actor"])
    validated_membership_refresh_command(envelope_by_field["command"])
    encoded_document(envelope_by_field)
    return envelope_by_field


def membership_refresh_command_sha256(command_by_field):
    """Bind every validated command field exactly as the App live receipt does."""
    return hashlib.sha256(encoded_document(validated_membership_refresh_command(command_by_field))).hexdigest()


def frozen_membership_refresh_envelope(envelope_by_field):
    """Own the complete validated command before any awaited authority exchange."""
    return json.loads(encoded_document(validated_membership_refresh_envelope(envelope_by_field)))
