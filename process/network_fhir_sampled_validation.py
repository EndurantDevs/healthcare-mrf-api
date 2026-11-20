# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sampled semantic validation policy and stored receipt formats."""

import json
import re
from dataclasses import asdict, dataclass

from process.network_fhir_source_witness_contract import digest, source_origin
from process.network_fhir_typed_capture import TypedGraphAnchor, TypedGraphRead
from process.network_fhir_typed_contract import policy_date, require

CONTRACT = "registry-fhir-sampled-semantic-verification-v1"
ALGORITHM = "sha256-bottom-k-v1"
SERIES_COUNT = 10
ITEMS_PER_SERIES = 100
_LIMIT = SERIES_COUNT * ITEMS_PER_SERIES
_ID = re.compile(r"[A-Za-z0-9.-]{1,64}")
_LIMITATIONS = [
    "Ten disjoint series partition one sample without replacement; statistical independence is not claimed.",
    "Source units are role witnesses; derived provider-office pairs do not have equal inclusion probabilities.",
    "Candidate distinct populations above the sample cap are unknown; provenance-row populations remain exact.",
    "Sampling may miss rare or systematic errors and does not establish exhaustive semantic equivalence.",
]


@dataclass(frozen=True)
class SemanticSamplingPolicy:
    """One caller-selected seed; changing seeds after a failure is not a retry policy."""

    seed: str

    def __post_init__(self):
        require(type(self.seed) is str and re.fullmatch(r"[0-9a-f]{64}", self.seed), "sample_seed")


def _series_counts(selected):
    return [max(0, min(ITEMS_PER_SERIES, selected - index * ITEMS_PER_SERIES)) for index in range(SERIES_COUNT)]


def _count(value):
    require(type(value) is int and 0 <= value < 2**63, "sample_receipt")


def _validate_direction(direction, *, source):
    fields = {"unit", "population", "selected", "series_counts", "failures"}
    fields |= {"derived_pair_rows"} if source else {"population_lower_bound", "provenance_rows", "witness_rows"}
    require(type(direction) is dict and set(direction) == fields, "sample_receipt")
    expected_unit = "source-role-witness" if source else "distinct-provider-office-pair"
    require(direction["unit"] == expected_unit, "sample_receipt")
    for field in fields - {"unit", "population", "series_counts"}:
        _count(direction[field])
    selected, population = direction["selected"], direction["population"]
    require(selected <= _LIMIT and direction["series_counts"] == _series_counts(selected), "sample_receipt")
    require(all(type(count) is int for count in direction["series_counts"]), "sample_receipt")
    if population is None:
        require(
            not source and selected == _LIMIT and direction["population_lower_bound"] == _LIMIT + 1, "sample_receipt"
        )
    else:
        _count(population)
        require(selected == min(population, _LIMIT), "sample_receipt")
        if not source:
            require(direction["population_lower_bound"] == population, "sample_receipt")
    if source:
        require(direction["failures"] <= direction["derived_pair_rows"], "sample_receipt")
    else:
        require(
            selected <= direction["witness_rows"] <= direction["provenance_rows"]
            and direction["population_lower_bound"] <= direction["provenance_rows"]
            and direction["failures"] <= direction["witness_rows"],
            "sample_receipt",
        )


def _validate_receipt_networks(networks):
    require(type(networks) is list and 0 < len(networks) <= 2000, "sample_receipt")
    keys = []
    for network in networks:
        require(
            type(network) is dict
            and set(network) == {"network_source_key", "source_to_candidate", "candidate_to_source"},
            "sample_receipt",
        )
        key = network["network_source_key"]
        require(type(key) is str and _ID.fullmatch(key), "sample_receipt")
        keys.append(key)
        _validate_direction(network["source_to_candidate"], source=True)
        _validate_direction(network["candidate_to_source"], source=False)
    require(keys == sorted(set(keys)), "sample_receipt")


def validate_sampled_semantic_receipt(receipt):
    """Validate a payload-free, closed receipt; a receipt alone grants no read authority."""
    require(
        type(receipt) is dict
        and set(receipt)
        == {
            "contract",
            "mode",
            "seed",
            "algorithm",
            "series_count",
            "items_per_series",
            "source_origin_sha256",
            "capture_id",
            "observed_at",
            "capture_sha256",
            "membership_as_of",
            "networks",
            "limitations",
            "receipt_sha256",
        },
        "sample_receipt",
    )
    SemanticSamplingPolicy(receipt["seed"])
    require(
        receipt["contract"] == CONTRACT
        and receipt["mode"] == "sampled"
        and receipt["algorithm"] == ALGORITHM
        and type(receipt["series_count"]) is int
        and receipt["series_count"] == SERIES_COUNT
        and type(receipt["items_per_series"]) is int
        and receipt["items_per_series"] == ITEMS_PER_SERIES
        and receipt["limitations"] == _LIMITATIONS,
        "sample_receipt",
    )
    TypedGraphAnchor(receipt["capture_id"], receipt["observed_at"])
    policy_date(receipt["membership_as_of"])
    for field in ("source_origin_sha256", "capture_sha256", "receipt_sha256"):
        require(type(receipt[field]) is str and re.fullmatch(r"[0-9a-f]{64}", receipt[field]), "sample_receipt")
    _validate_receipt_networks(receipt["networks"])
    require(
        receipt["receipt_sha256"]
        == digest({key: field_value for key, field_value in receipt.items() if key != "receipt_sha256"}),
        "sample_receipt",
    )
    return receipt


@dataclass(frozen=True)
class SampledTypedGraphRead:
    """Sample results bound to a logical graph edition and content capture."""

    graph_read: TypedGraphRead
    receipt_json: str

    @property
    def document(self):
        """Return the selected logical graph description."""
        return self.graph_read.document

    @property
    def capture(self):
        """Return the graph content capture."""
        return self.graph_read.capture

    @property
    def anchor(self):
        """Return the selected logical capture coordinates."""
        return self.graph_read.anchor

    @property
    def receipt(self):
        """Return an independent copy of the sampling results."""
        return json.loads(self.receipt_json)

    def relation(self, kind):
        """Resolve one named table from the selected graph edition."""
        return self.graph_read.relation(kind)

    def require_ready(self, *, complete=True):
        """Validate deterministic sample results and their logical graph edition."""
        require(type(self.graph_read) is TypedGraphRead, "sample_read")
        self.graph_read.require_ready(complete=False)
        receipt = validate_sampled_semantic_receipt(self.receipt)
        require(
            receipt["capture_sha256"] == self.capture["capture_sha256"]
            and receipt["capture_id"] == self.anchor.capture_id
            and receipt["observed_at"] == self.anchor.observed_at
            and receipt["source_origin_sha256"] == digest(asdict(source_origin(self.document["origin"])))
            and receipt["membership_as_of"] == self.document["membership_as_of"]
            and not any(
                direction["failures"]
                for network in receipt["networks"]
                for direction in (network["source_to_candidate"], network["candidate_to_source"])
            ),
            "sample_read",
        )
