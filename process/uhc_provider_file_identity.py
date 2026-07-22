# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-neutral identity and logical-scope contracts for UHC official files."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from process.uhc_provider_file_source_identity import (
    UHCProviderFileIdentityError,
    UHC_PROVIDER_FILE_ENTRY_ID,
    _exact_fields,
    _read_json_document,
)


UHC_CENSUS_CONTRACT = "healthporta-uhc-provider-file-census-v1"
UHC_CENSUS_SEMANTIC_SHA256 = (
    "b19c778496debc8453bffde8672d2df34f4c52f26376ae6bfd5b7fb41e5c14a1"
)
SOURCE_FILE_CONTRACT = "healthporta-uhc-source-file-identity-v1"
LOGICAL_SCOPE_CONTRACT = "healthporta-uhc-logical-scope-v1"

COMMUNITY_AND_STATE_MARKET = "community-and-state"
INDIVIDUAL_EXCHANGE_MARKET = "individual-exchange"
PROVIDER_MEMBERSHIP = "provider_membership"
PLAN_REFERENCE = "plan_reference"
PAIRING_NOT_APPLICABLE = "not_applicable"
PAIRING_PAIRED = "paired"
PAIRING_UNPAIRED_RETAINED_ONLY = "unpaired_retained_only"

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
UHC_CENSUS_PATH = _REPOSITORY_ROOT / "specs/uhc_provider_file_census_v1.json"
_PRODUCT_PATTERN = re.compile(r"^[A-Z0-9_]{2,32}$")
_JURISDICTION_PATTERN = re.compile(r"^[A-Z]{2}$")
_CANONICAL_FILE_PATTERN = re.compile(
    r"^JSON_(?:Providers_[A-Z0-9_]+|PLANS_[A-Z]{2})\.json$"
)


@dataclass(frozen=True)
class UHCProductJurisdiction:
    product: str
    jurisdiction: str | None


@dataclass(frozen=True)
class UHCProviderFileCensus:
    observed_on: str
    provenance: str
    cs_provider_products: tuple[UHCProductJurisdiction, ...]
    ifp_paired_states: tuple[str, ...]
    ifp_unpaired_product: UHCProductJurisdiction


@dataclass(frozen=True)
class UHCSourceFileDescriptor:
    family: str
    collection_kind: str
    file_name: str


@dataclass(frozen=True)
class UHCSourceFileIdentity:
    entry_id: str
    family: str
    collection_kind: str
    file_name: str
    source_file_id: str


@dataclass(frozen=True)
class UHCLogicalScope:
    family: str
    market: str
    product: str
    jurisdiction: str | None
    collection_kind: str
    source_file: UHCSourceFileIdentity
    pairing_status: str
    logical_scope_id: str


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def provider_file_census_from_spec() -> UHCProviderFileCensus:
    """Validate and return the reviewed 102-file census metadata."""

    return _provider_file_census_from_path(UHC_CENSUS_PATH)


def _provider_file_census_from_path(census_path: Path) -> UHCProviderFileCensus:
    """Load a testable path only when it equals the frozen census exactly."""

    census_document = _read_json_document(census_path)
    if _sha256_json(census_document) != UHC_CENSUS_SEMANTIC_SHA256:
        raise UHCProviderFileIdentityError("frozen UHC census identity differs")
    expected_fields = {
        "schema_version",
        "contract",
        "source_entry_id",
        "observed_on",
        "provenance",
        "census_counts",
        "cs_provider_products",
        "ifp_paired_states",
        "ifp_unpaired_provider_products",
    }
    _exact_fields(census_document, expected_fields, "UHC census")
    _validate_census_header(census_document)
    cs_products = _validated_cs_products(census_document["cs_provider_products"])
    ifp_states = _validated_ifp_states(census_document["ifp_paired_states"])
    unpaired_product = _validated_unpaired_product(
        census_document["ifp_unpaired_provider_products"]
    )
    census = UHCProviderFileCensus(
        observed_on=census_document["observed_on"],
        provenance=census_document["provenance"],
        cs_provider_products=cs_products,
        ifp_paired_states=ifp_states,
        ifp_unpaired_product=unpaired_product,
    )
    _validate_declared_counts(census_document["census_counts"], census)
    return census


def _validate_census_header(census_document: dict[str, Any]) -> None:
    if (
        census_document["schema_version"] != 1
        or census_document["contract"] != UHC_CENSUS_CONTRACT
        or census_document["source_entry_id"] != UHC_PROVIDER_FILE_ENTRY_ID
        or census_document["observed_on"] != "2026-07-22"
        or not isinstance(census_document["provenance"], str)
        or not census_document["provenance"].strip()
    ):
        raise UHCProviderFileIdentityError("UHC census header is invalid")


def _validated_cs_products(value: Any) -> tuple[UHCProductJurisdiction, ...]:
    if not isinstance(value, dict) or len(value) != 53:
        raise UHCProviderFileIdentityError("CS product census is invalid")
    if not all(isinstance(product, str) for product in value):
        raise UHCProviderFileIdentityError("CS product census is invalid")
    if list(value) != sorted(value):
        raise UHCProviderFileIdentityError("CS product census is invalid")
    products = []
    for product, jurisdiction in value.items():
        if (
            not isinstance(product, str)
            or not _PRODUCT_PATTERN.fullmatch(product)
            or jurisdiction is not None
        ):
            raise UHCProviderFileIdentityError("CS product census is invalid")
        products.append(UHCProductJurisdiction(product, jurisdiction))
    return tuple(products)


def _validated_ifp_states(value: Any) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or len(value) != 24
        or not all(
            isinstance(state, str) and _JURISDICTION_PATTERN.fullmatch(state)
            for state in value
        )
    ):
        raise UHCProviderFileIdentityError("IFP paired-state census is invalid")
    if value != sorted(value) or len(value) != len(set(value)):
        raise UHCProviderFileIdentityError("IFP paired-state census is invalid")
    return tuple(value)


def _validated_unpaired_product(value: Any) -> UHCProductJurisdiction:
    expected_fields_by_name = {
        "product": "UHCEX_HIX",
        "jurisdiction": None,
        "pairing": "unpaired",
        "retention_policy": "retained_only",
    }
    if value != [expected_fields_by_name]:
        raise UHCProviderFileIdentityError("IFP unpaired-product census is invalid")
    return UHCProductJurisdiction(
        expected_fields_by_name["product"],
        expected_fields_by_name["jurisdiction"],
    )


def _validate_declared_counts(
    counts_by_name: Any,
    census: UHCProviderFileCensus,
) -> None:
    expected_counts_by_name = {
        "catalog_file_count": 102,
        "cs_provider_membership": len(census.cs_provider_products),
        "ifp_provider_membership": len(census.ifp_paired_states) + 1,
        "ifp_plan_reference": len(census.ifp_paired_states),
    }
    if counts_by_name != expected_counts_by_name:
        raise UHCProviderFileIdentityError("UHC census counts are invalid")


def current_census_source_files() -> tuple[UHCSourceFileDescriptor, ...]:
    """Return every expected source basename in deterministic identity order."""

    return _source_files_for_census(provider_file_census_from_spec())


def _source_files_for_census(
    reviewed_census: UHCProviderFileCensus,
) -> tuple[UHCSourceFileDescriptor, ...]:
    source_files = [
        UHCSourceFileDescriptor(
            family="cs",
            collection_kind=PROVIDER_MEMBERSHIP,
            file_name=f"JSON_Providers_{product.product}.json",
        )
        for product in reviewed_census.cs_provider_products
    ]
    source_files.extend(
        UHCSourceFileDescriptor(
            family="ifp",
            collection_kind=PROVIDER_MEMBERSHIP,
            file_name=f"JSON_Providers_{state}IEX.json",
        )
        for state in reviewed_census.ifp_paired_states
    )
    source_files.append(
        UHCSourceFileDescriptor(
            family="ifp",
            collection_kind=PROVIDER_MEMBERSHIP,
            file_name="JSON_Providers_UHCEX_HIX.json",
        )
    )
    source_files.extend(
        UHCSourceFileDescriptor(
            family="ifp",
            collection_kind=PLAN_REFERENCE,
            file_name=f"JSON_PLANS_{state}.json",
        )
        for state in reviewed_census.ifp_paired_states
    )
    return tuple(sorted(source_files, key=_source_file_sort_key))


def _source_file_sort_key(source_file: UHCSourceFileDescriptor) -> tuple[str, str, str]:
    return source_file.family, source_file.collection_kind, source_file.file_name


def logical_scope_for_file(
    source_file: UHCSourceFileDescriptor,
) -> UHCLogicalScope:
    """Map one exact reviewed basename to its normalized logical scope."""

    _validate_source_file_descriptor(source_file)
    return _scope_for_validated_source_file(
        source_file,
        provider_file_census_from_spec(),
    )


def _validate_source_file_descriptor(source_file: Any) -> None:
    if type(source_file) is not UHCSourceFileDescriptor:
        raise UHCProviderFileIdentityError("source file descriptor type is invalid")
    if (
        source_file.family not in ("cs", "ifp")
        or source_file.collection_kind not in (
            PROVIDER_MEMBERSHIP,
            PLAN_REFERENCE,
        )
    ):
        raise UHCProviderFileIdentityError("source file classification is invalid")
    _validate_canonical_file_name(source_file.file_name)


def _scope_for_validated_source_file(
    source_file: UHCSourceFileDescriptor,
    census: UHCProviderFileCensus,
) -> UHCLogicalScope:
    """Build one scope after descriptor and census validation."""

    reviewed_census = census
    metadata_by_key = _scope_metadata_by_source_file(reviewed_census)
    source_key = _source_file_sort_key(source_file)
    metadata = metadata_by_key.get(source_key)
    if metadata is None:
        raise UHCProviderFileIdentityError("source file is not in the reviewed UHC census")
    market, product, jurisdiction, pairing_status = metadata
    source_identity = _source_file_identity(source_file)
    logical_scope_id = _sha256_json(
        {
            "contract": LOGICAL_SCOPE_CONTRACT,
            "entry_id": UHC_PROVIDER_FILE_ENTRY_ID,
            "family": source_file.family,
            "market": market,
            "product": product,
            "jurisdiction": jurisdiction,
        }
    )
    return UHCLogicalScope(
        family=source_file.family,
        market=market,
        product=product,
        jurisdiction=jurisdiction,
        collection_kind=source_file.collection_kind,
        source_file=source_identity,
        pairing_status=pairing_status,
        logical_scope_id=logical_scope_id,
    )


def _validate_canonical_file_name(file_name: Any) -> None:
    if (
        not isinstance(file_name, str)
        or not file_name.isascii()
        or not file_name.isprintable()
        or Path(file_name).name != file_name
        or "/" in file_name
        or "\\" in file_name
        or not _CANONICAL_FILE_PATTERN.fullmatch(file_name)
    ):
        raise UHCProviderFileIdentityError("source filename is not canonical")


def _source_file_identity(source_file: UHCSourceFileDescriptor) -> UHCSourceFileIdentity:
    identity_by_field = {
        "contract": SOURCE_FILE_CONTRACT,
        "entry_id": UHC_PROVIDER_FILE_ENTRY_ID,
        "family": source_file.family,
        "collection_kind": source_file.collection_kind,
        "file_name": source_file.file_name,
    }
    return UHCSourceFileIdentity(
        entry_id=UHC_PROVIDER_FILE_ENTRY_ID,
        family=source_file.family,
        collection_kind=source_file.collection_kind,
        file_name=source_file.file_name,
        source_file_id=_sha256_json(identity_by_field),
    )


def _scope_metadata_by_source_file(
    census: UHCProviderFileCensus,
) -> dict[tuple[str, str, str], tuple[str, str, str | None, str]]:
    metadata_by_key = {
        (
            "cs",
            PROVIDER_MEMBERSHIP,
            f"JSON_Providers_{product.product}.json",
        ): (
            COMMUNITY_AND_STATE_MARKET,
            product.product,
            product.jurisdiction,
            PAIRING_NOT_APPLICABLE,
        )
        for product in census.cs_provider_products
    }
    for state in census.ifp_paired_states:
        shared_metadata = (
            INDIVIDUAL_EXCHANGE_MARKET,
            "IEX",
            state,
            PAIRING_PAIRED,
        )
        provider_key = (
            "ifp",
            PROVIDER_MEMBERSHIP,
            f"JSON_Providers_{state}IEX.json",
        )
        plan_key = ("ifp", PLAN_REFERENCE, f"JSON_PLANS_{state}.json")
        metadata_by_key[provider_key] = shared_metadata
        metadata_by_key[plan_key] = shared_metadata
    unpaired = census.ifp_unpaired_product
    metadata_by_key[("ifp", PROVIDER_MEMBERSHIP, f"JSON_Providers_{unpaired.product}.json")] = (
        INDIVIDUAL_EXCHANGE_MARKET,
        unpaired.product,
        unpaired.jurisdiction,
        PAIRING_UNPAIRED_RETAINED_ONLY,
    )
    return metadata_by_key


def logical_scopes_for_current_census(
    source_files: Iterable[UHCSourceFileDescriptor],
) -> tuple[UHCLogicalScope, ...]:
    """Validate an exact 102-file census and return its normalized scopes."""

    reviewed_census = provider_file_census_from_spec()
    supplied_files = tuple(source_files)
    for source_file in supplied_files:
        _validate_source_file_descriptor(source_file)
    supplied_keys = [_source_file_sort_key(source_file) for source_file in supplied_files]
    if len(supplied_keys) != len(set(supplied_keys)):
        raise UHCProviderFileIdentityError("UHC census contains a duplicate source file")
    expected_files = _source_files_for_census(reviewed_census)
    expected_keys = {_source_file_sort_key(source_file) for source_file in expected_files}
    if set(supplied_keys) != expected_keys:
        raise UHCProviderFileIdentityError(
            "UHC census is incomplete or contains an unknown file"
        )
    scopes = tuple(
        _scope_for_validated_source_file(source_file, reviewed_census)
        for source_file in sorted(supplied_files, key=_source_file_sort_key)
    )
    _validate_scope_pairing(scopes, reviewed_census)
    return scopes


def _validate_scope_pairing(
    scopes: Sequence[UHCLogicalScope],
    census: UHCProviderFileCensus,
) -> None:
    ifp_kinds_by_jurisdiction: dict[str, set[str]] = {}
    for scope in scopes:
        if scope.family == "cs":
            if (
                scope.collection_kind != PROVIDER_MEMBERSHIP
                or scope.pairing_status != PAIRING_NOT_APPLICABLE
            ):
                raise UHCProviderFileIdentityError("CS scope pairing is invalid")
            continue
        if scope.pairing_status == PAIRING_PAIRED:
            ifp_kinds_by_jurisdiction.setdefault(scope.jurisdiction, set()).add(
                scope.collection_kind
            )
        elif (
            scope.product != census.ifp_unpaired_product.product
            or scope.collection_kind != PROVIDER_MEMBERSHIP
            or scope.pairing_status != PAIRING_UNPAIRED_RETAINED_ONLY
        ):
            raise UHCProviderFileIdentityError("IFP unpaired scope is invalid")
    expected_kinds_by_jurisdiction = {
        state: {PROVIDER_MEMBERSHIP, PLAN_REFERENCE}
        for state in census.ifp_paired_states
    }
    if ifp_kinds_by_jurisdiction != expected_kinds_by_jurisdiction:
        raise UHCProviderFileIdentityError("IFP provider and plan scopes are unpaired")


__all__ = [
    "COMMUNITY_AND_STATE_MARKET",
    "INDIVIDUAL_EXCHANGE_MARKET",
    "PAIRING_NOT_APPLICABLE",
    "PAIRING_PAIRED",
    "PAIRING_UNPAIRED_RETAINED_ONLY",
    "PLAN_REFERENCE",
    "PROVIDER_MEMBERSHIP",
    "UHCLogicalScope",
    "UHCProductJurisdiction",
    "UHCProviderFileCensus",
    "UHCProviderFileIdentityError",
    "UHCSourceFileDescriptor",
    "UHCSourceFileIdentity",
    "current_census_source_files",
    "logical_scope_for_file",
    "logical_scopes_for_current_census",
    "provider_file_census_from_spec",
]
