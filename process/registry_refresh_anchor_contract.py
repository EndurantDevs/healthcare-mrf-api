# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed approved-refresh queries; validated JSON never confers native authority."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from uuid import UUID

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates

MAX_BYTES = 1024 * 1024
GENERIC_FHIR_PRODUCER = "registry-retained-normalized-fhir-v1"
SCOPE_FIELDS = {"binding_id", "binding_revision", "network_id", "continuity_policy", "source_scope"}


class RegistryRefreshAnchorUnavailable(RuntimeError):
    """The installed native capability cannot establish a complete approved anchor."""


class RegistryRefreshAnchorMismatch(PermissionError):
    """An exact active approved binding or independently derived source scope differs."""


class RegistryRefreshAnchorStale(ValueError):
    """The expected current approved revision has advanced."""


def _identifier(coordinate, maximum, field):
    if (
        type(coordinate) is not str
        or not coordinate
        or len(coordinate) > maximum
        or coordinate != coordinate.strip()
        or any(ord(character) < 32 for character in coordinate)
    ):
        raise ValueError("invalid refresh " + field)


def _uuid(identity):
    if type(identity) is not str:
        raise ValueError("invalid refresh identity")
    parsed = UUID(identity)
    if str(parsed) != identity:
        raise ValueError("invalid refresh identity")
    return parsed


def _network_id(identity):
    if type(identity) is not int or not 1 <= identity <= 2147483647:
        raise ValueError("invalid refresh network")


def _object(document, fields):
    if type(document) is not dict or set(document) != fields:
        raise ValueError("invalid refresh fields")
    return document


def _text(coordinate, maximum, field):
    _identifier(coordinate, maximum, field)
    if not coordinate.isprintable() or len(coordinate.encode("utf-8")) > maximum:
        raise ValueError("invalid refresh coordinate")
    return coordinate


def _counter(counter, minimum=0):
    if type(counter) is not int or not minimum <= counter < 2**63:
        raise ValueError("invalid refresh revision")


def _hash(digest):
    if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("invalid refresh digest")


def _identity(identity):
    if type(identity) is not str or not _uuid(identity).int:
        raise ValueError("invalid refresh identity")


def encoded_document(document):
    """Bound canonical refresh input and output without granting native authority."""
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    if len(encoded) > MAX_BYTES:
        raise ValueError("refresh document exceeds bound")
    return encoded


def _source_scope(scope_by_field, policy):
    """Source continuity uses one closed native anchor grammar per source family."""
    if type(scope_by_field) is not dict:
        raise ValueError("invalid refresh source scope")
    system = scope_by_field.get("source_system")
    if system == "fhir" and scope_by_field.get("producer_id") != GENERIC_FHIR_PRODUCER:
        _fhir_scope(scope_by_field, policy)
    elif system in {"fhir", "aca", "ptg"}:
        _retained_scope(scope_by_field, policy)
    else:
        raise ValueError("unsupported refresh source system")
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", scope_by_field["dataset_schema"]) is None:
        raise ValueError("invalid refresh source namespace")


def _fhir_scope(scope_by_field, policy):
    _object(
        scope_by_field,
        {
            "source_system",
            "source_id",
            "dataset_schema",
            "endpoint_id",
            "producer_id",
            "alias_scope",
            "resource_type",
            "resource_id",
            "legacy_network_id",
        },
    )
    if policy != "accepted_edition" or scope_by_field["resource_type"] != "Organization":
        raise ValueError("unsupported FHIR refresh continuity policy")
    for name, maximum in (
        ("source_id", 256),
        ("dataset_schema", 63),
        ("endpoint_id", 64),
        ("producer_id", 128),
        ("alias_scope", 512),
        ("resource_id", 64),
    ):
        _text(scope_by_field[name], maximum, name)
    if re.fullmatch(r"[A-Za-z0-9\-.]{1,64}", scope_by_field["resource_id"]) is None:
        raise ValueError("invalid FHIR resource ID")
    _identity(scope_by_field["legacy_network_id"])


def _retained_scope(scope_by_field, policy):
    _object(
        scope_by_field,
        {
            "source_system",
            "source_id",
            "dataset_schema",
            "producer_id",
            "source_key",
            "approved_binding_sha256",
            "capture_generation",
        },
    )
    if (
        policy
        != {"fhir": "retained_successor", "aca": "retained_successor", "ptg": "exact_capture"}[
            scope_by_field["source_system"]
        ]
    ):
        raise ValueError("unsupported retained refresh continuity policy")
    for name, maximum in (
        ("source_id", 256),
        ("dataset_schema", 63),
        ("producer_id", 128),
        ("source_key", 512),
    ):
        _text(scope_by_field[name], maximum, name)
    for name in ("approved_binding_sha256", "capture_generation"):
        _hash(scope_by_field[name])


def validated_scope(scope_by_field):
    """Preserve one exact approved binding and its explicit continuity anchor."""
    _object(scope_by_field, SCOPE_FIELDS)
    _identity(scope_by_field["binding_id"])
    _counter(scope_by_field["binding_revision"], 1)
    _network_id(scope_by_field["network_id"])
    _source_scope(scope_by_field["source_scope"], scope_by_field["continuity_policy"])
    encoded_document(scope_by_field)
    return scope_by_field


def validated_enrollment(scopes):
    """Accept a complete sorted unique selection without adding unseen bindings."""
    if type(scopes) is not list or not 1 <= len(scopes) <= 1000:
        raise ValueError("invalid refresh scope count")
    identifiers = [validated_scope(scope)["binding_id"] for scope in scopes]
    if identifiers != sorted(set(identifiers)):
        raise ValueError("refresh scopes must have sorted unique bindings")
    encoded_document(scopes)
    return scopes


def validated_anchor_request(document_by_field):
    """Accept one complete nonempty exact-scope query with no caller capability fields."""
    _object(document_by_field, {"approved_revision", "enrolled_scopes"})
    _counter(document_by_field["approved_revision"])
    validated_enrollment(document_by_field["enrolled_scopes"])
    encoded_document(document_by_field)
    return document_by_field


@dataclass(frozen=True)
class ApprovedRefreshBinding:
    """Native approved row provenance and its existing SQL fingerprint recipe."""

    record_kind: str
    record_key: str
    record_revision: int
    custom_revision: int
    record_json: dict
    approved_binding_sha256: str

    def __post_init__(self):
        if self.record_kind != "network_binding" or type(self.record_json) is not dict:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_native_binding_invalid")
        try:
            _identity(self.record_key)
            _counter(self.record_revision, 1)
            _counter(self.custom_revision)
            _hash(self.approved_binding_sha256)
            _network_id(self.record_json.get("network_id"))
            RegistryNetworkSourceCoordinates(
                **{name: self.record_json.get(name) for name in RegistryNetworkSourceCoordinates.__dataclass_fields__}
            )
            _text(self.record_json.get("source_key"), 512, "source_key")
            if (
                self.record_json.get("binding_id") != self.record_key
                or type(self.record_json.get("revision")) is not int
                or self.record_json["revision"] != self.record_revision
                or self.record_json.get("archived") is not False
                or type(self.record_json.get("source_scope_json")) is not dict
            ):
                raise ValueError("native binding fields differ")
        except (ValueError, TypeError) as error:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_native_binding_invalid") from error

    @property
    def binding_id(self):
        """Expose the exact native approved record key to source adapters."""
        return self.record_key

    @property
    def binding_revision(self):
        """Expose the approved record revision without substituting custom revision."""
        return self.record_revision

    @property
    def network_id(self):
        """Expose the native approved canonical int4 network identity."""
        return self.record_json["network_id"]
