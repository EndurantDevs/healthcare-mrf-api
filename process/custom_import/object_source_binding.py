# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable approved object locations; exact content pins remain per-run input."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from process.custom_import import https, s3, upload
from process.custom_import.definition import (
    MAX_CHILD_COLLECTIONS,
    MAX_DEFINITION_BYTES,
    CustomImportDefinition,
    canonical_json,
    load_json_definition,
)
from process.custom_import.object_processing_policy import ObjectProcessingPolicy

OBJECT_SOURCE_BINDING_CONTRACT = "custom-import/source-binding/v3"
OBJECT_SOURCE_CONNECTORS = frozenset({"https", "s3", "upload"})
_BINDING_KEYS = frozenset(
    {"contract", "connector", "definition_sha256", "schema_sha256", "source_object", "streams", "processing_policy"}
)
_STREAM_KEYS = {
    "https": frozenset({"stream_id", "url"}),
    "s3": frozenset({"stream_id", "endpoint", "region", "bucket", "key", "addressing"}),
    "upload": frozenset({"stream_id", "source_id"}),
}
_SHA256 = re.compile(r"[0-9a-f]{64}")
_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_ERROR = "object_source_binding_invalid"


class ObjectSourceBindingError(ValueError):
    """A binding is incomplete, unsafe, or differs from its immutable definition."""


def _exact(value, keys):
    if not isinstance(value, Mapping) or value.keys() != keys:
        raise ObjectSourceBindingError(_ERROR)
    return dict(value)


def _text(value, pattern):
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise ObjectSourceBindingError(_ERROR)
    return value


def _stream(value, connector):
    document = _exact(value, _STREAM_KEYS[connector])
    _text(document["stream_id"], upload._STREAM_ID)
    if connector == "https":
        https._https_url(document["url"])
    elif connector == "s3":
        document["endpoint"] = s3._validated_locator(
            **{name: document[name] for name in _STREAM_KEYS["s3"] - {"stream_id"}}
        )
    else:
        _text(document["source_id"], upload._OBJECT_ID)
    return MappingProxyType(document)


@dataclass(frozen=True)
class ObjectSourceBinding:
    """A reusable location approval without credentials, acquisition or authority."""

    connector: str
    definition_sha256: str
    schema_sha256: str
    source_object: Mapping[str, str]
    streams: tuple[Mapping[str, str], ...] = field(repr=False)
    processing_policy: ObjectProcessingPolicy
    canonical: str = field(init=False, repr=False)
    digest: str = field(init=False)

    def __post_init__(self) -> None:
        try:
            if type(self.connector) is not str or self.connector not in OBJECT_SOURCE_CONNECTORS:
                raise ValueError
            _text(self.definition_sha256, _SHA256)
            _text(self.schema_sha256, _SHA256)
            source_object = _exact(self.source_object, {"fingerprint_sha256", "version"})
            _text(source_object["fingerprint_sha256"], _SHA256)
            _text(source_object["version"], _VERSION)
            if not isinstance(self.streams, tuple) or not 1 <= len(self.streams) <= MAX_CHILD_COLLECTIONS + 1:
                raise ValueError
            streams = tuple(_stream(stream, self.connector) for stream in self.streams)
            if len({stream["stream_id"] for stream in streams}) != len(streams):
                raise ValueError
            if not isinstance(self.processing_policy, ObjectProcessingPolicy):
                raise ValueError
            policy = ObjectProcessingPolicy.from_mapping(self.processing_policy.to_mapping())
            object.__setattr__(self, "source_object", MappingProxyType(source_object))
            object.__setattr__(self, "streams", tuple(sorted(streams, key=lambda stream: stream["stream_id"])))
            object.__setattr__(self, "processing_policy", policy)
            canonical = canonical_json(self.to_mapping())
            if len(canonical.encode("utf-8")) > MAX_DEFINITION_BYTES:
                raise ValueError
        except TypeError, ValueError, UnicodeError, RecursionError:
            raise ObjectSourceBindingError(_ERROR) from None
        object.__setattr__(self, "canonical", canonical)
        object.__setattr__(
            self,
            "digest",
            hashlib.sha256(self.contract.encode("ascii") + b":" + canonical.encode("utf-8")).hexdigest(),
        )

    @property
    def contract(self) -> str:
        """Return the immutable object-binding version, distinct from legacy sources."""
        return OBJECT_SOURCE_BINDING_CONTRACT

    @classmethod
    def from_json(cls, serialized: str | bytes) -> ObjectSourceBinding:
        """Parse bounded JSON without accepting duplicate keys or extra fields."""

        try:
            return cls.from_mapping(load_json_definition(serialized))
        except TypeError, ValueError, UnicodeError, RecursionError:
            raise ObjectSourceBindingError(_ERROR) from None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ObjectSourceBinding:
        """Select only this closed object variant; legacy bindings remain separate."""

        try:
            document = _exact(value, _BINDING_KEYS)
            if document["contract"] != OBJECT_SOURCE_BINDING_CONTRACT or not isinstance(document["streams"], list):
                raise ValueError
            return cls(
                connector=document["connector"],
                definition_sha256=document["definition_sha256"],
                schema_sha256=document["schema_sha256"],
                source_object=document["source_object"],
                streams=tuple(document["streams"]),
                processing_policy=ObjectProcessingPolicy.from_mapping(document["processing_policy"]),
            )
        except TypeError, ValueError, UnicodeError, RecursionError:
            raise ObjectSourceBindingError(_ERROR) from None

    def to_mapping(self) -> dict[str, Any]:
        """Return fresh nested mappings without exposing mutable retained state."""

        return {
            "contract": self.contract,
            "connector": self.connector,
            "definition_sha256": self.definition_sha256,
            "schema_sha256": self.schema_sha256,
            "source_object": dict(self.source_object),
            "streams": [dict(stream) for stream in self.streams],
            "processing_policy": self.processing_policy.to_mapping(),
        }

    def validate_definition(self, definition: CustomImportDefinition) -> None:
        """Require the exact immutable definition and every declared stream once."""

        if (
            not isinstance(definition, CustomImportDefinition)
            or definition.digest != self.definition_sha256
            or definition.schema_digest != self.schema_sha256
            or {stream.stream_id for stream in definition.source_streams}
            != {stream["stream_id"] for stream in self.streams}
        ):
            raise ObjectSourceBindingError(_ERROR)
