# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-scoped FHIR identifier policy contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from process.tin_npi_connector_support import (
    _FHIR_IDENTIFIER_CODING_SYSTEMS,
    _FHIR_NPI_SYSTEM,
    _IDENTIFIER_POLICY_HASH_DOMAIN,
    _IDENTIFIER_RULE_HASH_DOMAIN,
    _PUBLIC_ID_PATTERN,
    FHIR_TIN_NPI_IDENTIFIER_POLICY_ID,
    TinNpiConnectorError,
)


def _canonical_identifier_selector_values(
    values: object,
    *,
    field_name: str,
) -> tuple[str, ...]:
    if (
        type(values) is not tuple
        or values != tuple(sorted(set(values)))
        or any(
            type(value) is not str
            or not value
            or len(value) > 256
            or not value.isascii()
            or any(
                character.isspace() or character in {'"', "\\"} for character in value
            )
            for value in values
        )
    ):
        raise TinNpiConnectorError(f"FHIR identifier {field_name} are invalid")
    return values


def _canonical_identifier_selector_codings(
    values: object,
    *,
    field_name: str,
) -> tuple[tuple[str, str], ...]:
    if (
        type(values) is not tuple
        or values != tuple(sorted(set(values)))
        or any(
            type(coding) is not tuple
            or len(coding) != 2
            or any(
                type(part) is not str
                or not part
                or len(part) > 256
                or not part.isascii()
                or any(
                    character.isspace() or character in {'"', "\\"}
                    for character in part
                )
                for part in coding
            )
            for coding in values
        )
    ):
        raise TinNpiConnectorError(f"FHIR identifier {field_name} are invalid")
    return values


def _canonical_identifier_scope_id(
    candidate: object,
    *,
    field_name: str,
    limit: int,
) -> str:
    if (
        type(candidate) is not str
        or not 1 <= len(candidate) <= limit
        or _PUBLIC_ID_PATTERN.fullmatch(candidate) is None
    ):
        raise TinNpiConnectorError(f"FHIR identifier {field_name} is invalid")
    return candidate


def _validate_rule_identity(rule: FhirTinNpiIdentifierRule) -> None:
    """Validate the stable IDs that scope one reviewed identifier rule."""

    _canonical_identifier_scope_id(rule.rule_id, field_name="rule ID", limit=128)
    _canonical_identifier_scope_id(rule.source_id, field_name="source ID", limit=64)
    _canonical_identifier_scope_id(
        rule.endpoint_id,
        field_name="endpoint ID",
        limit=64,
    )
    _canonical_identifier_scope_id(
        rule.period_policy_id,
        field_name="period policy ID",
        limit=64,
    )


def _validate_rule_selectors(rule: FhirTinNpiIdentifierRule) -> None:
    """Validate exact disjoint NPI and EIN system/type selectors."""

    _canonical_identifier_selector_values(rule.npi_systems, field_name="NPI systems")
    _canonical_identifier_selector_values(rule.ein_systems, field_name="EIN systems")
    _canonical_identifier_selector_codings(
        rule.npi_type_codings,
        field_name="NPI type codings",
    )
    _canonical_identifier_selector_codings(
        rule.ein_type_codings,
        field_name="EIN type codings",
    )
    if (
        not (rule.npi_systems or rule.npi_type_codings)
        or not (rule.ein_systems or rule.ein_type_codings)
        or set(rule.npi_systems).intersection(rule.ein_systems)
        or set(rule.npi_type_codings).intersection(rule.ein_type_codings)
    ):
        raise TinNpiConnectorError("FHIR identifier rule selectors are invalid")


def _validate_excluded_identifier_uses(rule: FhirTinNpiIdentifierRule) -> None:
    """Validate the sorted identifier-use denylist without fuzzy values."""

    excluded_uses = rule.excluded_identifier_uses
    if (
        type(excluded_uses) is not tuple
        or excluded_uses != tuple(sorted(set(excluded_uses)))
        or any(
            type(identifier_use) is not str
            or not identifier_use
            or len(identifier_use) > 32
            or not identifier_use.isascii()
            or any(
                character.isspace() or character in {'"', "\\"}
                for character in identifier_use
            )
            for identifier_use in excluded_uses
        )
    ):
        raise TinNpiConnectorError("FHIR identifier activity policy is invalid")


@dataclass(frozen=True)
class FhirTinNpiIdentifierRule:
    """Exact identifier selectors reviewed for one source and endpoint."""

    rule_id: str
    source_id: str
    endpoint_id: str
    npi_systems: tuple[str, ...]
    npi_type_codings: tuple[tuple[str, str], ...]
    ein_systems: tuple[str, ...]
    ein_type_codings: tuple[tuple[str, str], ...]
    excluded_identifier_uses: tuple[str, ...] = ("old",)
    period_policy_id: str = "fhir-r4-inclusive-period-at-observed-at-v1"

    def __post_init__(self) -> None:
        """Fail closed unless every rule identity and selector is canonical."""

        _validate_rule_identity(self)
        _validate_rule_selectors(self)
        _validate_excluded_identifier_uses(self)

    def public_payload(self) -> dict[str, Any]:
        """Return the complete non-secret rule descriptor payload."""

        return {
            "endpoint_id": self.endpoint_id,
            "ein_systems": list(self.ein_systems),
            "ein_type_codings": [list(coding) for coding in self.ein_type_codings],
            "excluded_identifier_uses": list(self.excluded_identifier_uses),
            "npi_systems": list(self.npi_systems),
            "npi_type_codings": [list(coding) for coding in self.npi_type_codings],
            "period_policy_id": self.period_policy_id,
            "rule_id": self.rule_id,
            "source_id": self.source_id,
        }

    @property
    def descriptor_canonical_json(self) -> str:
        """Return stable canonical JSON for cross-language rule hashing."""

        return json.dumps(
            self.public_payload(),
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def descriptor_sha256(self) -> str:
        """Return the domain-separated digest of the canonical rule."""

        return hashlib.sha256(
            _IDENTIFIER_RULE_HASH_DOMAIN
            + self.descriptor_canonical_json.encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class FhirTinNpiIdentifierPolicy:
    """Immutable bundle of exact source-scoped identifier rules."""

    policy_id: str
    rules: tuple[FhirTinNpiIdentifierRule, ...]

    def __post_init__(self) -> None:
        _canonical_identifier_scope_id(
            self.policy_id,
            field_name="policy ID",
            limit=128,
        )
        if (
            type(self.rules) is not tuple
            or not self.rules
            or any(type(rule) is not FhirTinNpiIdentifierRule for rule in self.rules)
        ):
            raise TinNpiConnectorError("FHIR identifier policy rules are invalid")
        expected_rules = tuple(
            sorted(
                self.rules,
                key=lambda rule: (
                    rule.source_id.encode("utf-8"),
                    rule.endpoint_id.encode("utf-8"),
                    rule.rule_id.encode("utf-8"),
                ),
            )
        )
        if self.rules != expected_rules:
            raise TinNpiConnectorError("FHIR identifier policy rules are not ordered")
        scope_keys = tuple((rule.source_id, rule.endpoint_id) for rule in self.rules)
        rule_ids = tuple(rule.rule_id for rule in self.rules)
        if len(set(scope_keys)) != len(scope_keys) or len(set(rule_ids)) != len(
            rule_ids
        ):
            raise TinNpiConnectorError("FHIR identifier policy rules are duplicated")

    def rule_for(
        self,
        *,
        source_id: str,
        endpoint_id: str,
    ) -> FhirTinNpiIdentifierRule:
        """Resolve exactly one rule for a reviewed source and endpoint."""

        scope_key = (
            _canonical_identifier_scope_id(
                source_id,
                field_name="source ID",
                limit=64,
            ),
            _canonical_identifier_scope_id(
                endpoint_id,
                field_name="endpoint ID",
                limit=64,
            ),
        )
        matches = tuple(
            rule
            for rule in self.rules
            if (rule.source_id, rule.endpoint_id) == scope_key
        )
        if len(matches) != 1:
            raise TinNpiConnectorError(
                "FHIR identifier policy does not cover source endpoint"
            )
        return matches[0]

    def public_payload(self) -> dict[str, Any]:
        """Return the complete ordered identifier-policy descriptor."""

        return {
            "policy_id": self.policy_id,
            "rules": [
                {
                    **rule.public_payload(),
                    "identifier_rule_sha256": rule.descriptor_sha256,
                }
                for rule in self.rules
            ],
        }

    @property
    def descriptor_canonical_json(self) -> str:
        """Return stable canonical JSON for cross-language policy hashing."""

        return json.dumps(
            self.public_payload(),
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def descriptor_sha256(self) -> str:
        """Return the domain-separated digest of the canonical policy."""

        return hashlib.sha256(
            _IDENTIFIER_POLICY_HASH_DOMAIN
            + self.descriptor_canonical_json.encode("utf-8")
        ).hexdigest()


DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY = FhirTinNpiIdentifierPolicy(
    policy_id=FHIR_TIN_NPI_IDENTIFIER_POLICY_ID,
    rules=(
        FhirTinNpiIdentifierRule(
            rule_id="healthporta.provider-directory.unreviewed-identifiers.v1",
            source_id="unreviewed-source",
            endpoint_id="unreviewed-endpoint",
            npi_systems=(_FHIR_NPI_SYSTEM,),
            npi_type_codings=tuple(
                sorted(
                    (coding_system, "NPI")
                    for coding_system in _FHIR_IDENTIFIER_CODING_SYSTEMS
                )
            ),
            ein_systems=("urn:healthporta:unreviewed-ein-never-use",),
            ein_type_codings=(),
        ),
    ),
)
