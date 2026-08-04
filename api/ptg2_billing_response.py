# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public response shaping for exact PTG billing associations."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from api.ptg2_billing_entity_refs import (
    BILLING_ENTITY_REF_PREFIX,
    PTG2BillingAssociationDataError,
)
from api.ptg2_rate_option_refs import (
    PTG2RateOptionRefError,
    validate_rate_option_ref_consistency,
)


_PROVIDER_GROUP_REF_BYTES = 16
_INTERNAL_ASSOCIATION_FIELDS = frozenset(
    {
        "provider_group_ref",
        "tax_identity_status",
        "tin_type",
        "billing_entity_ref",
        "unavailable_reason",
    }
)
_TAX_IDENTITY_STATES = frozenset(
    {"matched_ein", "missing", "malformed", "unsupported_type"}
)


def _option_association_status(
    associations: list[dict[str, Any]],
) -> str:
    statuses = {
        str(association.get("tax_identity_status") or "")
        for association in associations
    }
    if "unavailable" in statuses:
        return "unavailable"
    resolved_count = sum(
        association.get("billing_entity_ref") not in (None, "")
        for association in associations
    )
    if resolved_count == len(associations):
        return "resolved"
    return "partially_resolved" if resolved_count else "unresolved"


def _validate_public_identity_fields(
    association_by_field: Mapping[str, Any],
    tax_identity_status: Any,
) -> None:
    """Reject inconsistent identity fields before they reach a response."""

    if tax_identity_status == "matched_ein":
        billing_entity_ref = association_by_field.get("billing_entity_ref")
        has_reference_prefix = (
            type(billing_entity_ref) is str
            and billing_entity_ref.startswith(BILLING_ENTITY_REF_PREFIX)
        )
        reference_payload = (
            billing_entity_ref.removeprefix(BILLING_ENTITY_REF_PREFIX)
            if has_reference_prefix
            else ""
        )
        if (
            association_by_field.get("tin_type") != "ein"
            or not has_reference_prefix
            or len(reference_payload) != 64
            or any(
                character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
                for character in reference_payload
            )
            or "unavailable_reason" in association_by_field
        ):
            raise PTG2BillingAssociationDataError(
                "exact billing association contains an invalid matched identity"
            )
    elif tax_identity_status in _TAX_IDENTITY_STATES - {"matched_ein"}:
        if any(
            field_name in association_by_field
            for field_name in ("tin_type", "billing_entity_ref", "unavailable_reason")
        ):
            raise PTG2BillingAssociationDataError(
                "exact billing association contains an invalid unresolved identity"
            )
    elif tax_identity_status == "unavailable":
        if (
            association_by_field.get("unavailable_reason")
            != "legacy_snapshot_without_tax_identity_sidecar"
            or "tin_type" in association_by_field
            or "billing_entity_ref" in association_by_field
        ):
            raise PTG2BillingAssociationDataError(
                "exact billing association contains an invalid unavailable identity"
            )
    else:
        raise PTG2BillingAssociationDataError(
            "exact billing association contains an invalid tax-identity status"
        )


def _public_billing_association(
    raw_association: Mapping[str, Any],
    *,
    association_ordinal: int,
) -> tuple[str, dict[str, Any]]:
    """Remove the guessable group hash and shape one option-local record."""

    if not isinstance(raw_association, Mapping):
        raise PTG2BillingAssociationDataError(
            "exact billing association contains an invalid public record"
        )
    if set(raw_association) - _INTERNAL_ASSOCIATION_FIELDS:
        raise PTG2BillingAssociationDataError(
            "exact billing association contains non-public fields"
        )
    internal_association_by_field = dict(raw_association)
    provider_group_ref = internal_association_by_field.get("provider_group_ref")
    if (
        type(provider_group_ref) is not str
        or len(provider_group_ref) != _PROVIDER_GROUP_REF_BYTES * 2
        or any(character not in "0123456789abcdef" for character in provider_group_ref)
    ):
        raise PTG2BillingAssociationDataError(
            "exact billing association contains an invalid provider-group reference"
        )
    if type(association_ordinal) is not int or association_ordinal < 1:
        raise PTG2BillingAssociationDataError(
            "exact billing association contains an invalid option-local ordinal"
        )
    association_by_field = {
        field_name: field_value
        for field_name, field_value in internal_association_by_field.items()
        if field_name != "provider_group_ref"
    }
    association_by_field["association_ordinal"] = association_ordinal
    _validate_public_identity_fields(
        association_by_field,
        association_by_field.get("tax_identity_status"),
    )
    return provider_group_ref, association_by_field


@dataclass
class _BillingAttachmentSummary:
    association_pairs: set[tuple[str, str]] = field(default_factory=set)
    resolved_refs: set[str] = field(default_factory=set)
    has_unavailable_association: bool = False
    has_unresolved_association: bool = False

    def observe(
        self,
        provider_set_ref: str,
        group_refs: list[str],
        associations: list[dict[str, Any]],
        association_status: str,
    ) -> None:
        """Accumulate distinct exact edges and public billing references."""

        self.has_unavailable_association |= association_status == "unavailable"
        self.has_unresolved_association |= association_status != "resolved"
        for group_ref, association_by_field in zip(
            group_refs,
            associations,
            strict=True,
        ):
            self.association_pairs.add((provider_set_ref, group_ref))
            billing_entity_ref = association_by_field.get("billing_entity_ref")
            if billing_entity_ref is not None:
                self.resolved_refs.add(billing_entity_ref)


def _validate_rate_options_for_billing(raw_options: list[Any]) -> None:
    try:
        validate_rate_option_ref_consistency(raw_options)
    except PTG2RateOptionRefError as exc:
        raise PTG2BillingAssociationDataError(
            "exact billing association response contains an invalid rate option reference"
        ) from exc


def _shape_rate_options_with_billing(
    raw_options: list[Any],
    associations_by_provider_set: Mapping[str, Iterable[Mapping[str, Any]]],
    summary: _BillingAttachmentSummary,
) -> list[dict[str, Any]]:
    """Bind each atomic rate option to its exact, sanitized group edges."""

    _validate_rate_options_for_billing(raw_options)
    shaped_options: list[dict[str, Any]] = []
    for raw_option in raw_options:
        option_by_field = dict(raw_option)
        provider_set_ref = option_by_field.get("provider_set_ref")
        if type(provider_set_ref) is not str or not provider_set_ref:
            raise PTG2BillingAssociationDataError(
                "exact billing association response contains an invalid provider set"
            )
        raw_associations = associations_by_provider_set.get(provider_set_ref)
        if raw_associations is None:
            raise PTG2BillingAssociationDataError(
                "exact billing association response omitted a provider set"
            )
        sanitized_associations = [
            _public_billing_association(
                raw_association,
                association_ordinal=association_ordinal,
            )
            for association_ordinal, raw_association in enumerate(
                raw_associations,
                start=1,
            )
        ]
        group_refs = [group_ref for group_ref, _ in sanitized_associations]
        associations = [
            association_by_field
            for _, association_by_field in sanitized_associations
        ]
        if not associations or len(set(group_refs)) != len(group_refs):
            raise PTG2BillingAssociationDataError(
                "exact billing association response contains invalid group associations"
            )
        association_status = _option_association_status(associations)
        option_by_field["billing_association_status"] = association_status
        option_by_field["billing_association_count"] = len(associations)
        option_by_field["billing_associations"] = associations
        shaped_options.append(option_by_field)
        summary.observe(
            provider_set_ref,
            group_refs,
            associations,
            association_status,
        )
    return shaped_options


def attach_billing_associations(
    provider_items: Iterable[Mapping[str, Any]],
    associations_by_provider_set: Mapping[str, Iterable[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    """Attach exact group evidence to options and derive truthful entity totals."""

    shaped_items: list[dict[str, Any]] = []
    for provider_item in provider_items:
        shaped_item_by_field = dict(provider_item)
        raw_options = provider_item.get("rate_options")
        if not isinstance(raw_options, list) or not raw_options:
            raise PTG2BillingAssociationDataError(
                "exact billing association response is missing rate options"
            )
        summary = _BillingAttachmentSummary()
        shaped_item_by_field["rate_options"] = _shape_rate_options_with_billing(
            raw_options,
            associations_by_provider_set,
            summary,
        )
        shaped_item_by_field["billing_association_count"] = len(
            summary.association_pairs
        )
        shaped_item_by_field["resolved_billing_entity_count"] = (
            None
            if summary.has_unavailable_association
            else len(summary.resolved_refs)
        )
        shaped_item_by_field["billing_entity_count"] = (
            len(summary.resolved_refs)
            if not summary.has_unavailable_association
            and not summary.has_unresolved_association
            else None
        )
        shaped_item_by_field["billing_entity_count_status"] = (
            "unavailable"
            if summary.has_unavailable_association
            else "lower_bound" if summary.has_unresolved_association else "exact"
        )
        shaped_items.append(shaped_item_by_field)
    return shaped_items
