# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Constant-size metadata for independently verified source inventories.

Parsing or folding these receipts establishes neither SOURCE admission, inventory
content, nor native custody. Callers must independently verify the complete root,
the singleton records, physical storage and writer closure before publication.
"""

import hashlib
import json
from dataclasses import asdict

from process.network_fhir_membership_source import FHIR_SOURCE_WITNESS_TRANSFORM_V2
from process.registry_source_contribution_store import (
    _COUNTERS,
    _MAPPING_COUNTERS_V3,
    _WITNESS_COUNTERS_V2,
    CONTRIBUTION_TABLE,
    _canonical,
    _mapping_input_rows_v3,
    validate_registry_source_contribution_receipt,
    validate_source_mapping_descriptor_v3,
    validate_source_witness_descriptor_v2,
)
from process.registry_source_contribution_store import (
    SUMMARY_KEY as CONTRIBUTION_KEY,
)
from process.registry_source_inventory_contract import (
    MAX_INVENTORY_ENTRIES,
    RegistrySourceInventoryError,
    SourceInventoryRoot,
    _document,
)
from process.registry_source_office_summary_v3 import (
    SUMMARY_KEY as OFFICE_KEY,
)
from process.registry_source_office_summary_v3 import (
    _counter,
    _sha,
    validate_derivation,
)
from process.registry_verified_office_derivation import (
    validate_verified_office_context_v2,
    validate_verified_office_derivation_v2,
    verified_office_context_sha256,
)

MAX_SUMMARY_BYTES = 4096
_CONTRIBUTION_FIELDS = {
    "component",
    "revision",
    "table_name",
    "inventory_entries_sha256",
    "source_count",
    "sources_sha256",
    *_COUNTERS,
}
_OFFICE_FIELDS = {
    "component",
    "revision",
    "recipe_sha256",
    "approved_revision",
    "approved_generation_sha256",
    "address_generation_sha256",
    "origin_manifest_sha256",
    "recipe_count",
    "office_rows",
    "page_count",
    "correspondence_sha256",
    "inventory_entries_sha256",
    "derivation_count",
}


def _require(condition):
    if not condition:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid")


def _root(root):
    _require(type(root) is SourceInventoryRoot)
    return SourceInventoryRoot(**asdict(root))


def _metadata(document, fields):
    document = _document(document, MAX_SUMMARY_BYTES)
    _require(set(document) == fields and len(_canonical(document).encode()) <= MAX_SUMMARY_BYTES)
    return json.loads(_canonical(document))


def _singleton_contribution(descriptor):
    return validate_registry_source_contribution_receipt(
        {
            "component": CONTRIBUTION_KEY,
            "revision": 2,
            "table_name": CONTRIBUTION_TABLE,
            "sources": [descriptor],
        }
    )["sources"][0]


class _SummaryFold:
    """One-row streaming state shared by the two concrete metadata accumulators."""

    def __init__(self, expected_count, validate, counters):
        self._expected_count, self._validate = expected_count, validate
        self._digest, self._count, self._previous = hashlib.sha256(b"["), 0, ""
        self._totals_by_name = dict.fromkeys(counters, 0)
        self._has_failed, self._finished = False, None

    def accept(self, document):
        """Commit one valid row atomically; a rejected open-state row poisons the fold."""
        if self._has_failed or self._finished is not None:
            raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid")
        try:
            _require(self._count < self._expected_count and self._count < MAX_INVENTORY_ENTRIES)
            checked = self._validate(document)
            identity = checked["recipe_sha256"]
            _require(identity > self._previous)
            totals_by_name = {name: total + checked[name] for name, total in self._totals_by_name.items()}
            for total in totals_by_name.values():
                _counter(total)
            encoded = _canonical(checked).encode()
        except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
            self._has_failed = True
            raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None
        self._digest.update(b"," if self._count else b"")
        self._digest.update(encoded)
        self._previous, self._totals_by_name = identity, totals_by_name
        self._count += 1

    def finish(self, summary):
        """Finalize complete input once; a failed finish poisons and a successful finish closes."""
        if self._has_failed:
            raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid")
        if self._finished is None:
            try:
                _require(self._count == self._expected_count)
                digest = self._digest.copy()
                digest.update(b"]")
                self._finished = summary(self._count, digest.hexdigest(), self._totals_by_name)
            except ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError:
                self._has_failed = True
                raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None
        return json.loads(_canonical(self._finished))


def _contribution_totals(document):
    for counter in _COUNTERS:
        _counter(document[counter])
    _require(
        document["raw_source_rows"] == document["accepted_raw_source_rows"] + document["omitted_raw_source_rows"]
        and document["fact_rows"] >= document["raw_source_rows"]
        and document["membership_rows"] <= document["fact_rows"]
        and document["omitted_rows"] <= document["fact_rows"]
        and document["provider_only_rows"] <= document["accepted_raw_source_rows"]
    )
    if document["source_count"] == 0:
        _require(
            not any(document[counter] for counter in _COUNTERS)
            and document["sources_sha256"] == hashlib.sha256(b"[]").hexdigest()
        )


def validate_contribution_summary_v3(document, root):
    """Validate closed metadata against root scalars; this does not verify its source stream."""
    try:
        root = _root(root)
        document = _metadata(document, _CONTRIBUTION_FIELDS)
        _require(
            document["component"] == CONTRIBUTION_KEY
            and type(document["revision"]) is int
            and document["revision"] == 3
            and document["table_name"] == CONTRIBUTION_TABLE
        )
        _counter(document["source_count"])
        _sha(document["sources_sha256"])
        _require(
            document["source_count"] == root.contribution_count
            and document["inventory_entries_sha256"] == root.entries_sha256
        )
        _contribution_totals(document)
        return document
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None


class ContributionSummaryAccumulator:
    """Accept revision-2 singleton descriptors across synchronous or asynchronous pages."""

    def __init__(self, root):
        self._root = _root(root)
        self._fold = _SummaryFold(self._root.contribution_count, _singleton_contribution, _COUNTERS)

    def accept(self, descriptor):
        """Validate one next descriptor; an invalid row permanently poisons the fold."""
        self._fold.accept(descriptor)

    def _summary(self, count, sources_sha256, totals_by_name):
        return validate_contribution_summary_v3(
            {
                "component": CONTRIBUTION_KEY,
                "revision": 3,
                "table_name": CONTRIBUTION_TABLE,
                "inventory_entries_sha256": self._root.entries_sha256,
                "source_count": count,
                "sources_sha256": sources_sha256,
                **totals_by_name,
            },
            self._root,
        )

    def finish(self):
        """Require the complete count, close the fold, and return a defensive constant-size receipt."""
        return self._fold.finish(self._summary)


def contribution_summary_v3(root, descriptors):
    """Fold revision-2 singleton descriptors, including explicit null local anchors."""
    try:
        accumulator = ContributionSummaryAccumulator(root)
        for descriptor in descriptors:
            accumulator.accept(descriptor)
        return accumulator.finish()
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None


def validate_office_summary_v4(document, root):
    """Check complete scalar commitments; derivation content still requires independent proof."""
    try:
        root = _root(root)
        document = _metadata(document, _OFFICE_FIELDS)
        _require(
            document["component"] == OFFICE_KEY and type(document["revision"]) is int and document["revision"] == 4
        )
        for field in ("approved_revision", "recipe_count", "derivation_count", "office_rows", "page_count"):
            _counter(document[field])
        for field in (
            "recipe_sha256",
            "approved_generation_sha256",
            "address_generation_sha256",
            "correspondence_sha256",
        ):
            _sha(document[field])
        if document["origin_manifest_sha256"] is not None:
            _sha(document["origin_manifest_sha256"])
        _require(
            document["recipe_count"] == document["derivation_count"] == root.derivation_count == root.entry_count >= 1
            and document["recipe_sha256"] == root.recipes_sha256
            and document["inventory_entries_sha256"] == root.entries_sha256
            and document["office_rows"] + document["page_count"] <= 2**63 - 1
        )
        return document
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None


class OfficeSummaryAccumulator:
    """Accept every recipe derivation across pages, including an entirely raw inventory."""

    def __init__(self, root, approved, address_source, origin_manifest_sha256):
        self._root = _root(root)
        self._approved_revision, self._approved_generation = approved.approved_revision, approved.generation_id
        self._address_generation, self._origin = address_source.generation_id, origin_manifest_sha256
        self._fold = _SummaryFold(self._root.derivation_count, validate_derivation, ("office_rows", "page_count"))

    def accept(self, derivation):
        """Validate one next derivation; page boundaries never enter either hash or totals."""
        self._fold.accept(derivation)

    def _summary(self, count, correspondence_sha256, totals_by_name):
        return validate_office_summary_v4(
            {
                "component": OFFICE_KEY,
                "revision": 4,
                "recipe_sha256": self._root.recipes_sha256,
                "approved_revision": self._approved_revision,
                "approved_generation_sha256": self._approved_generation,
                "address_generation_sha256": self._address_generation,
                "origin_manifest_sha256": self._origin,
                "recipe_count": self._root.entry_count,
                "inventory_entries_sha256": self._root.entries_sha256,
                "derivation_count": count,
                "correspondence_sha256": correspondence_sha256,
                **totals_by_name,
            },
            self._root,
        )

    def finish(self):
        """Validate complete scalar commitments; successful finish is idempotent and closes writes."""
        return self._fold.finish(self._summary)


def office_summary_v4(root, approved, address_source, derivations, origin_manifest_sha256):
    """Fold every recipe derivation; new v4 also supports inventories containing only raw derivations."""
    try:
        accumulator = OfficeSummaryAccumulator(root, approved, address_source, origin_manifest_sha256)
        for derivation in derivations:
            accumulator.accept(derivation)
        return accumulator.finish()
    except ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None


_WITNESS_CONTRIBUTION_FIELDS = _CONTRIBUTION_FIELDS | {"descriptor_version", "transform_version", *_WITNESS_COUNTERS_V2}
_WITNESS_OFFICE_FIELDS = _OFFICE_FIELDS | {"policy_context_sha256", "verified_derivation_count"}


def validate_contribution_summary_v4(document, root):
    """Validate explicit witness-v2 totals without inferring unresolved counts for legacy descriptors."""
    try:
        root = _root(root)
        document = _metadata(document, _WITNESS_CONTRIBUTION_FIELDS)
        _require(
            document["component"] == CONTRIBUTION_KEY
            and type(document["revision"]) is int
            and document["revision"] == 4
            and document["table_name"] == CONTRIBUTION_TABLE
            and type(document["descriptor_version"]) is int
            and document["descriptor_version"] == 2
            and document["transform_version"] == FHIR_SOURCE_WITNESS_TRANSFORM_V2
            and document["source_count"] == root.contribution_count
            and document["inventory_entries_sha256"] == root.entries_sha256
        )
        _counter(document["source_count"])
        _sha(document["sources_sha256"])
        for name in _WITNESS_COUNTERS_V2:
            _counter(document[name])
        _require(
            document["raw_source_rows"]
            == document["accepted_raw_source_rows"]
            + document["unresolved_raw_source_rows"]
            + document["omitted_raw_source_rows"]
            and document["fact_rows"] >= document["raw_source_rows"]
            and (document["raw_source_rows"] != 0 or document["fact_rows"] == 0)
            and document["membership_rows"] + document["omitted_rows"] + document["unresolved_rows"]
            <= document["fact_rows"] - document["raw_source_rows"]
            and document["provider_only_rows"] <= document["raw_source_rows"]
        )
        if document["source_count"] == 0:
            _require(
                not any(document[name] for name in _WITNESS_COUNTERS_V2)
                and document["sources_sha256"] == hashlib.sha256(b"[]").hexdigest()
            )
        return document
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None


class WitnessContributionSummaryAccumulator:
    """Fold only explicit v2 descriptors; all nine counters retain their distinct classifications."""

    def __init__(self, root):
        self._root = _root(root)
        self._fold = _SummaryFold(
            self._root.contribution_count, validate_source_witness_descriptor_v2, _WITNESS_COUNTERS_V2
        )

    def accept(self, descriptor):
        """Commit one next v2 descriptor; a rejected row poisons the fold."""
        self._fold.accept(descriptor)

    def _summary(self, count, sources_sha256, totals_by_name):
        return validate_contribution_summary_v4(
            {
                "component": CONTRIBUTION_KEY,
                "revision": 4,
                "table_name": CONTRIBUTION_TABLE,
                "descriptor_version": 2,
                "transform_version": FHIR_SOURCE_WITNESS_TRANSFORM_V2,
                "inventory_entries_sha256": self._root.entries_sha256,
                "source_count": count,
                "sources_sha256": sources_sha256,
                **totals_by_name,
            },
            self._root,
        )

    def finish(self):
        """Close a complete fold and return its defensive metadata commitment."""
        return self._fold.finish(self._summary)


def contribution_summary_v4(root, descriptors):
    """Fold a single ordered witness-v2 descriptor stream without changing legacy receipt formats."""
    try:
        accumulator = WitnessContributionSummaryAccumulator(root)
        for descriptor in descriptors:
            accumulator.accept(descriptor)
        return accumulator.finish()
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None


_MAPPING_SUMMARY_FIELDS = _CONTRIBUTION_FIELDS | {*_MAPPING_COUNTERS_V3, "descriptor_version", "input_rows"}


def _mapping_summary_counts(document):
    for name in (*_MAPPING_COUNTERS_V3, "input_rows"):
        _counter(document[name])
    _counter(document["membership_rows"] + document["provider_only_evidence_rows"])
    _require(
        document["raw_source_rows"]
        == sum(
            document[name]
            for name in ("accepted_raw_source_rows", "unresolved_raw_source_rows", "omitted_raw_source_rows")
        )
        and document["fact_rows"] >= document["raw_source_rows"]
        and document["fact_rows"] - document["raw_source_rows"] <= document["input_rows"] <= document["fact_rows"]
        and (document["raw_source_rows"] != 0 or document["fact_rows"] == 0)
        and document["input_rows"]
        == sum(
            document[name] for name in ("accepted_input_rows", "ignored_input_rows", "omitted_rows", "unresolved_rows")
        )
        and document["provider_only_rows"] <= min(document["raw_source_rows"], document["accepted_input_rows"])
        and document["provider_only_rows"] <= document["provider_only_evidence_rows"]
        and document["accepted_raw_source_rows"]
        <= min(document["accepted_input_rows"], document["membership_rows"] + document["provider_only_evidence_rows"])
        and (document["accepted_input_rows"] == 0)
        == (document["membership_rows"] + document["provider_only_evidence_rows"] == 0)
        and (document["provider_only_rows"] == 0) == (document["provider_only_evidence_rows"] == 0)
    )
    if document["source_count"] == 0:
        _require(
            not any(document[name] for name in (*_MAPPING_COUNTERS_V3, "input_rows"))
            and document["sources_sha256"] == hashlib.sha256(b"[]").hexdigest()
        )


def validate_contribution_summary_v5(document, root):
    """Validate mapping-v3 totals; the complete child stream still requires independent custody."""
    try:
        root = _root(root)
        document = _metadata(document, _MAPPING_SUMMARY_FIELDS)
        _require(
            document["component"] == CONTRIBUTION_KEY
            and type(document["revision"]) is int
            and document["revision"] == 5
            and type(document["descriptor_version"]) is int
            and document["descriptor_version"] == 3
            and document["table_name"] == CONTRIBUTION_TABLE
            and document["source_count"] == root.contribution_count
            and document["inventory_entries_sha256"] == root.entries_sha256
        )
        _counter(document["source_count"])
        _sha(document["sources_sha256"])
        _mapping_summary_counts(document)
        return document
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None


class MappingContributionSummaryAccumulator:
    """Fold only explicit mapping-v3 descriptors while preserving exact child serialization."""

    def __init__(self, root):
        self._root = _root(root)
        self._input_rows = 0
        self._fold = _SummaryFold(self._root.contribution_count, self._validate, _MAPPING_COUNTERS_V3)

    def _validate(self, document):
        checked = validate_source_mapping_descriptor_v3(document)
        self._input_rows += _mapping_input_rows_v3(checked)
        _counter(self._input_rows)
        return checked

    def accept(self, descriptor):
        """Accept the next ordered v3 child; invalid or old children permanently poison the fold."""
        self._fold.accept(descriptor)

    def _summary(self, count, sources_sha256, totals_by_name):
        return validate_contribution_summary_v5(
            {
                "component": CONTRIBUTION_KEY,
                "revision": 5,
                "descriptor_version": 3,
                "table_name": CONTRIBUTION_TABLE,
                "inventory_entries_sha256": self._root.entries_sha256,
                "source_count": count,
                "sources_sha256": sources_sha256,
                "input_rows": self._input_rows,
                **totals_by_name,
            },
            self._root,
        )

    def finish(self):
        """Require the complete stream and return a defensive constant-size summary."""
        return self._fold.finish(self._summary)


def contribution_summary_v5(root, descriptors):
    """Fold known raw formats only inside explicitly selected mapping-v3 descriptors."""
    try:
        accumulator = MappingContributionSummaryAccumulator(root)
        for descriptor in descriptors:
            accumulator.accept(descriptor)
        return accumulator.finish()
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None


def validate_office_summary_v5(document, root):
    """Check the v2 policy-context commitment and exact verified/all-derivation counts."""
    try:
        root = _root(root)
        document = _metadata(document, _WITNESS_OFFICE_FIELDS)
        _require(type(document["revision"]) is int and document["revision"] == 5)
        legacy_by_field = {field: document[field] for field in _OFFICE_FIELDS}
        validate_office_summary_v4({**legacy_by_field, "revision": 4}, root)
        _sha(document["policy_context_sha256"])
        _counter(document["verified_derivation_count"])
        _require(document["verified_derivation_count"] == root.contribution_count > 0)
        return document
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None


class WitnessOfficeSummaryAccumulator:
    """Fold v2 derivations sharing one expected context plus prepared non-FHIR raw derivations."""

    def __init__(self, root, expected_context):
        try:
            self._root = _root(root)
            self._context = validate_verified_office_context_v2(expected_context)
            self._context_sha256 = verified_office_context_sha256(self._context)
            self._verified_count = 0
            self._fold = _SummaryFold(self._root.derivation_count, self._validate, ("office_rows", "page_count"))
        except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
            raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None

    def _validate(self, document):
        if type(document) is dict and "derivation_version" in document:
            checked = validate_verified_office_derivation_v2(document)
            _require({field: checked[field] for field in self._context} == self._context)
            return checked
        checked = validate_derivation(document)
        _require(checked["mode"] == "raw")
        return checked

    def accept(self, derivation):
        """Commit one next derivation; a mismatched context poisons the fold."""
        self._fold.accept(derivation)
        self._verified_count += "derivation_version" in derivation

    def _summary(self, count, correspondence_sha256, totals_by_name):
        return validate_office_summary_v5(
            {
                "component": OFFICE_KEY,
                "revision": 5,
                "recipe_sha256": self._root.recipes_sha256,
                "approved_revision": self._context["approved_revision"],
                "approved_generation_sha256": self._context["approved_generation_sha256"],
                "address_generation_sha256": self._context["address_source"]["generation_id"],
                "origin_manifest_sha256": self._context["origin_manifest_sha256"],
                "recipe_count": self._root.entry_count,
                "inventory_entries_sha256": self._root.entries_sha256,
                "derivation_count": count,
                "verified_derivation_count": self._verified_count,
                "policy_context_sha256": self._context_sha256,
                "correspondence_sha256": correspondence_sha256,
                **totals_by_name,
            },
            self._root,
        )

    def finish(self):
        """Require exact positive v2 contribution coverage and every recipe derivation before closing."""
        return self._fold.finish(self._summary)


def office_summary_v5(root, expected_context, derivations):
    """Fold one ordered derivation stream with an independently selected common policy context."""
    try:
        accumulator = WitnessOfficeSummaryAccumulator(root, expected_context)
        for derivation in derivations:
            accumulator.accept(derivation)
        return accumulator.finish()
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_summary_invalid") from None
