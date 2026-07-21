# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Campaign and item identities for source-neutral retained acquisition."""

from __future__ import annotations

import copy
import hashlib
import json
import weakref
from dataclasses import dataclass, field
from typing import Any, Mapping

from process.provider_directory_retained_artifact_base import (
    ARTIFACT_KINDS,
    FIXED_CATALOG,
    ORDERED_STREAMS,
    PAYLOAD,
    RETAINED_ARTIFACT_CONTRACT_ID,
    TERMINAL_ZERO,
    RetainedArtifactError,
    canonical_json,
    require_digest,
    require_nonnegative_int,
    require_positive_int,
    require_safe_id,
    sha256_json,
)


MAX_PARTITION_METADATA_BYTES = 64 * 1024
MAX_PARTITION_METADATA_DEPTH = 8
MAX_PARTITION_METADATA_NODES = 4096
MAX_SOURCE_LOCATOR_BYTES = 64 * 1024
MAX_CAMPAIGN_ITEMS = 100_000
MAX_EXPECTED_STREAM_IDENTITIES = 4096


def _canonical_partition_metadata(metadata: Mapping[str, Any]) -> bytes:
    if type(metadata) is not dict:
        raise RetainedArtifactError("partition_metadata_invalid")
    try:
        return canonical_json(
            metadata,
            maximum_bytes=MAX_PARTITION_METADATA_BYTES,
            maximum_depth=MAX_PARTITION_METADATA_DEPTH,
            maximum_nodes=MAX_PARTITION_METADATA_NODES,
        )
    except (RetainedArtifactError, RuntimeError) as error:
        raise RetainedArtifactError("partition_metadata_invalid") from error


def partition_digest(metadata: Mapping[str, Any]) -> str:
    """Hash one canonical partition metadata mapping."""

    return hashlib.sha256(_canonical_partition_metadata(metadata)).hexdigest()


class _PrivateSourceLocatorSnapshot:
    """Inert redacted value produced by every generic copy/serialization path."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<private-source-locator-redacted>"

    def __str__(self) -> str:
        return "<private-source-locator-redacted>"

    def __copy__(self) -> "_PrivateSourceLocatorSnapshot":
        return self

    def __deepcopy__(self, _memo: dict[int, Any]) -> "_PrivateSourceLocatorSnapshot":
        return self


class _LivePrivateSourceLocator:
    """One live capability bound by object identity to its original item."""

    __slots__ = ("__owner", "__value")

    def __init__(self, owner: "RetainedCampaignItem", value: Any):
        object.__setattr__(
            self,
            "_LivePrivateSourceLocator__owner",
            weakref.ref(owner),
        )
        object.__setattr__(self, "_LivePrivateSourceLocator__value", value)

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise AttributeError("private_source_locator_immutable")

    def validate(self, owner: "RetainedCampaignItem") -> "_LivePrivateSourceLocator":
        """Validate without copying or returning the private locator text."""

        if self.__owner() is not owner:
            raise RetainedArtifactError("source_locator_owner_mismatch")
        if type(self.__value) is not str or not self.__value:
            raise RetainedArtifactError("source_locator_invalid")
        if len(self.__value) > MAX_SOURCE_LOCATOR_BYTES:
            raise RetainedArtifactError("source_locator_invalid")
        try:
            locator_byte_count = len(self.__value.encode("utf-8"))
        except UnicodeError as error:
            raise RetainedArtifactError("source_locator_invalid") from error
        if (
            not self.__value.isprintable()
            or locator_byte_count > MAX_SOURCE_LOCATOR_BYTES
        ):
            raise RetainedArtifactError("source_locator_invalid")
        return self

    def _open_for_owner(self, owner: "RetainedCampaignItem") -> str:
        self.validate(owner)
        return self.__value

    def __repr__(self) -> str:
        return "<private-source-locator>"

    def __str__(self) -> str:
        return "<private-source-locator>"

    def __copy__(self) -> _PrivateSourceLocatorSnapshot:
        return _PrivateSourceLocatorSnapshot()

    def __deepcopy__(self, _memo: dict[int, Any]) -> _PrivateSourceLocatorSnapshot:
        return _PrivateSourceLocatorSnapshot()

    def __reduce_ex__(self, _protocol: int):
        return (_PrivateSourceLocatorSnapshot, ())


def _open_private_source_locator(campaign_item: "RetainedCampaignItem") -> str:
    """Open only the live capability owned by this exact original item."""

    source_locator = campaign_item.source_locator
    if not isinstance(source_locator, _LivePrivateSourceLocator):
        raise RetainedArtifactError("source_locator_unavailable")
    return source_locator._open_for_owner(campaign_item)


def is_strong_etag(value: Any) -> bool:
    """Return whether a value is a printable strong HTTP entity tag."""

    return (
        isinstance(value, str)
        and 2 <= len(value) <= 1024
        and value.startswith('"')
        and value.endswith('"')
        and not value.startswith("W/")
        and all(ord(character) >= 32 for character in value)
    )


def transport_experience_scope_digest(
    *,
    adapter_id: str,
    endpoint_id: str,
    credential_descriptor_sha256: str,
) -> str:
    """Fence learned transport facts to one exact non-secret runtime scope."""

    return sha256_json(
        {
            "contract": RETAINED_ARTIFACT_CONTRACT_ID,
            "adapter_id": require_safe_id(adapter_id, "adapter_id", maximum=64),
            "endpoint_id": require_digest(endpoint_id, "endpoint_id"),
            "credential_descriptor_sha256": require_digest(
                credential_descriptor_sha256,
                "credential_descriptor_sha256",
            ),
        }
    )


def endpoint_request_fence_digest(endpoint_id: str) -> str:
    """Bind request pressure to one canonical admitted upstream endpoint."""

    return sha256_json(
        {
            "contract": RETAINED_ARTIFACT_CONTRACT_ID,
            "pressure_scope": require_digest(endpoint_id, "endpoint_id"),
        }
    )


@dataclass(frozen=True)
class RetainedCampaignItem:
    """One payload or terminal-zero member of an exact physical census."""

    source_item_id: str
    source_entry_sha256: str
    artifact_kind: str
    family: str
    collection_kind: str
    partition_metadata: Mapping[str, Any]
    stream_identity_sha256: str
    sequence_ordinal: int
    item_role: str = PAYLOAD
    source_locator: (
        _LivePrivateSourceLocator | _PrivateSourceLocatorSnapshot | str | None
    ) = field(
        default=None,
        repr=False,
        compare=False,
    )
    declared_byte_count: int | None = None
    terminal_proof_sha256: str | None = None
    _partition_metadata_canonical: bytes = field(
        init=False,
        repr=False,
        compare=False,
    )
    _partition_metadata_sha256: str = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        canonical_metadata = _canonical_partition_metadata(self.partition_metadata)
        object.__setattr__(
            self,
            "partition_metadata",
            json.loads(canonical_metadata),
        )
        object.__setattr__(
            self,
            "_partition_metadata_canonical",
            canonical_metadata,
        )
        object.__setattr__(
            self,
            "_partition_metadata_sha256",
            hashlib.sha256(canonical_metadata).hexdigest(),
        )
        if type(self.source_locator) is str:
            object.__setattr__(
                self,
                "source_locator",
                _LivePrivateSourceLocator(self, self.source_locator),
            )
        elif isinstance(self.source_locator, _LivePrivateSourceLocator):
            object.__setattr__(
                self,
                "source_locator",
                _PrivateSourceLocatorSnapshot(),
            )

    def _inert_copy(self) -> "RetainedCampaignItem":
        inert_locator = (
            None if self.source_locator is None else _PrivateSourceLocatorSnapshot()
        )
        return RetainedCampaignItem(
            source_item_id=self.source_item_id,
            source_entry_sha256=self.source_entry_sha256,
            artifact_kind=self.artifact_kind,
            family=self.family,
            collection_kind=self.collection_kind,
            partition_metadata=json.loads(self._partition_metadata_canonical),
            stream_identity_sha256=self.stream_identity_sha256,
            sequence_ordinal=self.sequence_ordinal,
            item_role=self.item_role,
            source_locator=inert_locator,
            declared_byte_count=self.declared_byte_count,
            terminal_proof_sha256=self.terminal_proof_sha256,
        )

    def __copy__(self) -> "RetainedCampaignItem":
        return self._inert_copy()

    def __deepcopy__(self, memo: dict[int, Any]) -> "RetainedCampaignItem":
        copied_item = self._inert_copy()
        memo[id(self)] = copied_item
        return copied_item

    def validate(self) -> "RetainedCampaignItem":
        """Validate one payload or terminal-zero census member."""

        from process.provider_directory_retained_store_support import (
            validated_campaign_item_snapshot,
        )

        validated_campaign_item_snapshot(self)
        return self

    @property
    def partition_metadata_sha256(self) -> str:
        """Return the canonical partition metadata digest."""

        return self._partition_metadata_sha256

    def catalog_object_identity_sha256(
        self,
        *,
        adapter_id: str,
        source_census_sha256: str,
    ) -> str:
        """Bind one catalog object to its adapter and exact source census."""

        from process.provider_directory_retained_store_support import (
            validated_campaign_item_snapshot,
        )

        item_snapshot = validated_campaign_item_snapshot(self)
        return sha256_json(
            {
                "contract": RETAINED_ARTIFACT_CONTRACT_ID,
                "identity_kind": "catalog_object",
                "adapter_id": require_safe_id(adapter_id, "adapter_id", maximum=64),
                "source_census_sha256": require_digest(
                    source_census_sha256,
                    "source_census_sha256",
                ),
                "source_item_id": item_snapshot.source_item_id,
                "source_entry_sha256": item_snapshot.source_entry_sha256,
                "declared_byte_count": item_snapshot.declared_byte_count,
            }
        )


@dataclass(frozen=True)
class RetainedCampaignPlan:
    """One fixed catalog or ordered-stream physical acquisition campaign."""

    adapter_id: str
    endpoint_id: str
    request_fence_id: str
    credential_descriptor_sha256: str
    source_census_sha256: str
    census_mode: str
    items: tuple[RetainedCampaignItem, ...]
    per_item_byte_budget: int
    aggregate_byte_budget: int
    expected_stream_identities: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.items) is not tuple or len(self.items) > MAX_CAMPAIGN_ITEMS:
            raise RetainedArtifactError("campaign_items_invalid")
        if (
            type(self.expected_stream_identities) is not tuple
            or len(self.expected_stream_identities) > MAX_EXPECTED_STREAM_IDENTITIES
        ):
            raise RetainedArtifactError("expected_streams_invalid")

    def _inert_copy(
        self,
        campaign_items: tuple[RetainedCampaignItem, ...],
    ) -> "RetainedCampaignPlan":
        return RetainedCampaignPlan(
            adapter_id=self.adapter_id,
            endpoint_id=self.endpoint_id,
            request_fence_id=self.request_fence_id,
            credential_descriptor_sha256=self.credential_descriptor_sha256,
            source_census_sha256=self.source_census_sha256,
            census_mode=self.census_mode,
            items=campaign_items,
            per_item_byte_budget=self.per_item_byte_budget,
            aggregate_byte_budget=self.aggregate_byte_budget,
            expected_stream_identities=self.expected_stream_identities,
        )

    def __copy__(self) -> "RetainedCampaignPlan":
        self.validate()
        return self._inert_copy(tuple(copy.copy(item) for item in self.items))

    def __deepcopy__(self, memo: dict[int, Any]) -> "RetainedCampaignPlan":
        self.validate()
        copied_plan = self._inert_copy(
            tuple(copy.deepcopy(item, memo) for item in self.items)
        )
        memo[id(self)] = copied_plan
        return copied_plan

    def validate(self) -> "RetainedCampaignPlan":
        """Validate census identity, byte budgets, and stream ordering."""

        from process.provider_directory_retained_store_support import (
            validated_campaign_plan_snapshot,
        )

        validated_campaign_plan_snapshot(self)
        return self

    @property
    def campaign_id(self) -> str:
        """Return the stable campaign identity for this exact plan."""

        from process.provider_directory_retained_store_support import (
            campaign_id_from_snapshot,
            validated_campaign_plan_snapshot,
        )

        return campaign_id_from_snapshot(validated_campaign_plan_snapshot(self))

    @property
    def transport_experience_scope_sha256(self) -> str:
        """Return the endpoint/adapter/contract/credential learning fence."""

        from process.provider_directory_retained_store_support import (
            validated_campaign_plan_snapshot,
        )

        campaign_snapshot = validated_campaign_plan_snapshot(self)
        return transport_experience_scope_digest(
            adapter_id=campaign_snapshot.adapter_id,
            endpoint_id=campaign_snapshot.endpoint_id,
            credential_descriptor_sha256=(
                campaign_snapshot.credential_descriptor_sha256
            ),
        )
