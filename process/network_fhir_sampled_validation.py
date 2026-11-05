# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded, explicitly sampled raw Source checks; exhaustive custody remains mandatory."""

import json
import re
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass

from process.network_fhir_source_epoch import _borrowed_epoch_settings, _set_content_settings
from process.network_fhir_source_witness_capture import _original_source_factory
from process.network_fhir_source_witness_contract import canonical, database_identity, digest, source_origin
from process.network_fhir_typed_capture import SealedTypedGraphRead, TypedGraphAnchor, load_typed_graph_capture
from process.network_fhir_typed_contract import PAIR_COLUMNS, TypedFHIRGraphError, policy_date, require
from process.network_fhir_typed_source import _canonicalize, _check_page, _page_rows, _pairs, _workspace
from process.uhc_flex_practitioner_async_safety import drain_operation

CONTRACT = "registry-fhir-sampled-semantic-verification-v1"
SOURCE_CONTRACT = "fhir-source-membership-sample-v1"
ALGORITHM = "sha256-bottom-k-v1"
SERIES_COUNT = 10
ITEMS_PER_SERIES = 100
_LIMIT = SERIES_COUNT * ITEMS_PER_SERIES
_PAIR_FIELDS = PAIR_COLUMNS[:6]
_PAIR_KEYS = ("resource_type", "resource_id", "network_source_key", "location_resource_id")
_ROLE_KINDS = {"OrganizationAffiliation", "PractitionerRole"}
_ID = re.compile(r"[A-Za-z0-9.-]{1,64}")
_READ_BRAND = object()
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


@dataclass(frozen=True)
class SampledCaptureScope:
    """Authenticated caller inputs and optional precreated read-only replay workspace."""

    source_profile: object
    reviewed_origin: object
    expected_anchor: TypedGraphAnchor
    network_source_keys: tuple[str, ...]
    replay_stage: object = None


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
            "database_identity",
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
    database_identity(receipt["database_identity"])
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
    """Distinct transaction-bound capability; it never marks the full graph checked."""

    sealed_read: SealedTypedGraphRead
    receipt_json: str
    brand: object

    @property
    def document(self):
        """Expose the unchanged graph descriptor without exhaustive semantic authority."""
        return self.sealed_read.document

    @property
    def capture(self):
        """Expose the native capture descriptor bound to this sampled read."""
        return self.sealed_read.capture

    @property
    def anchor(self):
        """Return the independently supplied native capture anchor."""
        return self.sealed_read.anchor

    @property
    def receipt(self):
        """Return a detached receipt so callers cannot mutate this capability."""
        return json.loads(self.receipt_json)

    def relation(self, kind):
        """Resolve only a relation already admitted by native capture custody."""
        return self.sealed_read.relation(kind)

    async def verify(self, connection):
        """Recheck transaction scope, native relation identities and sampled receipt bindings."""
        require(self.brand is _READ_BRAND and type(self.sealed_read) is SealedTypedGraphRead, "sample_read")
        await self.sealed_read.verify(connection)
        receipt = validate_sampled_semantic_receipt(self.receipt)
        require(
            receipt["capture_sha256"] == self.capture["capture_sha256"]
            and receipt["capture_id"] == self.anchor.capture_id
            and receipt["observed_at"] == self.anchor.observed_at
            and receipt["source_origin_sha256"] == digest(asdict(source_origin(self.document["origin"])))
            and receipt["database_identity"] == self.document["database_identity"]
            and receipt["membership_as_of"] == self.document["membership_as_of"]
            and not any(
                direction["failures"]
                for network in receipt["networks"]
                for direction in (network["source_to_candidate"], network["candidate_to_source"])
            ),
            "sample_read",
        )


def _candidate_sampler(keys, policy):
    try:
        from ptg2_address_canon import NetworkValidationSampler
    except ImportError:
        raise TypedFHIRGraphError("registry_fhir_typed_native_sampler_unavailable") from None
    sampler = NetworkValidationSampler("candidate-pair", policy.seed, keys, limit=_LIMIT)
    require(
        all(
            callable(getattr(sampler, method, None))
            for method in ("add_candidate_batch", "candidate_selected_indices", "coverage", "network_keys")
        ),
        "native_sampler_unavailable",
    )
    return sampler


def _candidate_wire_rows(rows):
    return tuple(tuple(str(row[field]) for field in _PAIR_FIELDS) for row in rows)


async def _candidate_selection(connection, sealed, keys, policy):
    sampler = _candidate_sampler(keys, policy)
    query = "SELECT " + ",".join(_PAIR_FIELDS) + " FROM " + sealed.relation("pairs")
    cursor = await connection.cursor(query)
    while rows := await cursor.fetch(1000):
        sampler.add_candidate_batch(_candidate_wire_rows(rows))
    return sampler


def _source_selection(export, origin, identity, policy, keys):
    require(
        type(export) is dict and set(export) == {"contract", "origin", "database_identity", "sampling", "resources"},
        "sample_contract",
    )
    require(export["contract"] == SOURCE_CONTRACT and source_origin(export["origin"]) == origin, "sample_contract")
    require(
        database_identity(export["database_identity"]) == database_identity(identity) and callable(export["resources"]),
        "sample_identity",
    )
    sample = export["sampling"]
    require(
        type(sample) is dict
        and set(sample) == {"unit", "algorithm", "seed", "series_count", "items_per_series", "networks"},
        "sample_contract",
    )
    require(
        sample["unit"] == "source-role-witness"
        and sample["algorithm"] == ALGORITHM
        and sample["seed"] == policy.seed
        and type(sample["series_count"]) is int
        and sample["series_count"] == SERIES_COUNT
        and type(sample["items_per_series"]) is int
        and sample["items_per_series"] == ITEMS_PER_SERIES
        and type(sample["networks"]) is list
        and len(sample["networks"]) == len(keys),
        "sample_contract",
    )
    for key, network in zip(keys, sample["networks"], strict=True):
        require(
            type(network) is dict
            and set(network) == {"network_source_key", "population", "series"}
            and network["network_source_key"] == key,
            "sample_contract",
        )
        _count(network["population"])
        series = network["series"]
        require(type(series) is list and len(series) == SERIES_COUNT, "sample_contract")
        selected_roles = []
        for batch in series:
            require(type(batch) is list and len(batch) <= ITEMS_PER_SERIES, "sample_contract")
            for role in batch:
                require(
                    type(role) in {list, tuple}
                    and len(role) == 2
                    and type(role[0]) is str
                    and role[0] in _ROLE_KINDS
                    and type(role[1]) is str
                    and _ID.fullmatch(role[1]),
                    "sample_contract",
                )
                selected_roles.append(tuple(role))
        require(
            len(set(selected_roles)) == len(selected_roles) == min(network["population"], _LIMIT), "sample_contract"
        )
        require([len(batch) for batch in series] == _series_counts(len(selected_roles)), "sample_contract")
    return sample["networks"]


async def _fetch_resources(export, connection, replay, keys, origin, as_of):
    requested, present = set(keys), set()
    require(0 < len(requested) <= 1000, "sample_resource_keys")
    counts, after = dict.fromkeys(dict(origin.resource_counts), 0), None
    iterator = export["resources"](tuple(sorted(requested)))
    try:
        async for page in iterator:
            rows, after = _page_rows(page, origin, counts, after)
            require(
                all((row["resource_type"], row["resource_id"]) in requested for row in page), "sample_resource_scope"
            )
            for row in rows:
                key = row["resource_type"], row["resource_id"]
                require(key in requested and key not in present, "sample_resource_scope")
                present.add(key)
            await _check_page(connection, replay, rows, origin.source_uri.rstrip("/"), as_of)
    finally:
        await drain_operation(iterator.aclose(), preserve_cancellation=True)
    return present


async def _replay_roles(export, connection, replay, roles, origin, as_of, *, required):
    await replay.verify(connection, complete=False)
    for kind in replay.names:
        await connection.execute("DELETE FROM " + replay.relation(kind))
    present = await _fetch_resources(export, connection, replay, roles, origin, as_of)
    require(not required or present == set(roles), "sample_role_missing")
    dependencies = (
        "SELECT subject_kind kind,subject_id id FROM {checks} WHERE subject_id IS NOT NULL UNION "
        "SELECT 'Organization',unnest(network_ids) FROM {checks} UNION SELECT 'Location',unnest(location_ids) FROM {checks}"
    ).format(checks=replay.relation("checks"))
    # The cursor is opened before dependency insertion, so only the sampled role references contribute.
    pending_rows = []
    async for dependency in connection.cursor(dependencies, prefetch=128):
        pending_rows.append(tuple(dependency))
        if len(pending_rows) == 1000:
            await _fetch_resources(export, connection, replay, pending_rows, origin, as_of)
            pending_rows.clear()
    if pending_rows:
        await _fetch_resources(export, connection, replay, pending_rows, origin, as_of)
    await _canonicalize(connection, replay)
    await _pairs(connection, replay, origin, as_of, checker=True)


async def _source_direction(export, connection, sealed, replay, origin, as_of, network):
    roles = [tuple(role) for series in network["series"] for role in series]
    direction_by_field = {
        "unit": "source-role-witness",
        "population": network["population"],
        "selected": len(roles),
        "series_counts": _series_counts(len(roles)),
        "derived_pair_rows": 0,
        "failures": 0,
    }
    if not roles:
        return direction_by_field
    await _replay_roles(export, connection, replay, roles, origin, as_of, required=True)
    equal = " AND ".join(
        "actual." + field + ("=expected." if field in _PAIR_KEYS else " IS NOT DISTINCT FROM expected.") + field
        for field in PAIR_COLUMNS
    )
    counts = await connection.fetchrow(
        "SELECT count(*)::bigint total,count(*) FILTER(WHERE NOT EXISTS(SELECT FROM "
        + sealed.relation("pairs")
        + " actual WHERE "
        + equal
        + "))::bigint failures FROM "
        + replay.relation("pairs")
        + " expected WHERE expected.network_source_key=$1",
        network["network_source_key"],
    )
    direction_by_field.update(derived_pair_rows=counts["total"], failures=counts["failures"])
    return direction_by_field


async def _candidate_batch(export, connection, replay, rows, origin, as_of, directions_by_network):
    roles = sorted({(row["resource_type"], row["resource_id"]) for row in rows})
    await _replay_roles(export, connection, replay, roles, origin, as_of, required=False)
    keys = [[row[field] for row in rows] for field in _PAIR_KEYS]
    join = " AND ".join("pair." + key + "=chosen." + key for key in _PAIR_KEYS)
    actual = await connection.fetch(
        "SELECT pair.* FROM "
        + replay.relation("pairs")
        + " pair JOIN unnest($1::text[],$2::text[],$3::text[],$4::text[]) chosen("
        + ",".join(_PAIR_KEYS)
        + ") ON "
        + join,
        *keys,
    )
    by_key = {tuple(row[field] for field in _PAIR_KEYS): tuple(row[field] for field in PAIR_COLUMNS) for row in actual}
    for row in rows:
        direction = directions_by_network[row["network_source_key"]]
        direction["witness_rows"] += 1
        if by_key.get(tuple(row[field] for field in _PAIR_KEYS)) != tuple(row[field] for field in PAIR_COLUMNS):
            direction["failures"] += 1


def _candidate_coverage(sampler):
    directions_by_network = {}
    fields = ("population", "population_lower_bound", "provenance_rows", "selected", "series_counts")
    for key in sampler.network_keys():
        coverage = sampler.coverage(key)
        directions_by_network[key] = {
            "unit": "distinct-provider-office-pair",
            **{field: coverage[field] for field in fields},
            "witness_rows": 0,
            "failures": 0,
        }
    return directions_by_network


async def _candidate_direction(export, connection, sealed, replay, origin, as_of, sampler):
    directions_by_network = _candidate_coverage(sampler)
    query = "SELECT " + ",".join(PAIR_COLUMNS) + " FROM " + sealed.relation("pairs")
    cursor = await connection.cursor(query)
    while rows := await cursor.fetch(1000):
        selected_indices = sampler.candidate_selected_indices(_candidate_wire_rows(rows))
        if selected_indices:
            selected_rows = [rows[index] for index in selected_indices]
            await _candidate_batch(export, connection, replay, selected_rows, origin, as_of, directions_by_network)
    return directions_by_network


@asynccontextmanager
async def _replay_workspace(connection, replay_stage):
    if replay_stage is None:
        async with _workspace(connection) as replay:
            yield replay
    else:
        await replay_stage.verify(connection, complete=False)
        require(
            replay_stage.schema_name == "pg_temp"
            and set(replay_stage.names) == {"cells", "checks", "pairs", "addresses"},
            "replay_workspace",
        )
        for kind in replay_stage.names:
            require(
                await connection.fetchval("SELECT NOT EXISTS(SELECT FROM " + replay_stage.relation(kind) + ")"),
                "restart_from_zero",
            )
        yield replay_stage


async def _sampling_settings(connection):
    limit = await connection.fetchval(
        "SELECT CASE WHEN current_setting('temp_file_limit')='-1' THEN NULL "
        "ELSE pg_size_bytes(current_setting('temp_file_limit')) END"
    )
    require(type(limit) is int and 0 < limit <= 256 * 1024**2, "sample_spill_limit")
    await _set_content_settings(connection, ["work_mem"], ["16MB"])


async def _verify_samples(source_export, connection, sealed, scope, policy):
    from directory_membership_runtime import open_membership_source_sample

    origin = source_origin(scope.reviewed_origin)
    identity, as_of = sealed.document["database_identity"], policy_date(sealed.document["membership_as_of"])
    async with _borrowed_epoch_settings(connection):
        await _sampling_settings(connection)
        selections_by_network = await _candidate_selection(connection, sealed, scope.network_source_keys, policy)
        async with (
            open_membership_source_sample(
                source_export.args[0],
                source_profile=scope.source_profile,
                origin=scope.reviewed_origin,
                seed=policy.seed,
                network_source_keys=scope.network_source_keys,
                series_count=SERIES_COUNT,
                items_per_series=ITEMS_PER_SERIES,
            ) as export,
            _replay_workspace(connection, scope.replay_stage) as replay,
        ):
            networks = _source_selection(export, origin, identity, policy, scope.network_source_keys)
            source_by_network = {}
            for network in networks:
                source_by_network[network["network_source_key"]] = await _source_direction(
                    export, connection, sealed, replay, origin, as_of, network
                )
            candidate_by_network = await _candidate_direction(
                export, connection, sealed, replay, origin, as_of, selections_by_network
            )
    return source_by_network, candidate_by_network


def _sample_receipt(sealed, policy, source_by_network, candidate_by_network):
    receipt_by_field = {
        "contract": CONTRACT,
        "mode": "sampled",
        "seed": policy.seed,
        "algorithm": ALGORITHM,
        "series_count": SERIES_COUNT,
        "items_per_series": ITEMS_PER_SERIES,
        "source_origin_sha256": digest(asdict(source_origin(sealed.document["origin"]))),
        "capture_id": sealed.capture["capture_id"],
        "observed_at": sealed.capture["observed_at"],
        "capture_sha256": sealed.capture["capture_sha256"],
        "database_identity": sealed.document["database_identity"],
        "membership_as_of": sealed.document["membership_as_of"],
        "networks": [
            {
                "network_source_key": key,
                "source_to_candidate": source_by_network[key],
                "candidate_to_source": candidate_by_network[key],
            }
            for key in sorted(source_by_network)
        ],
        "limitations": list(_LIMITATIONS),
    }
    receipt_by_field["receipt_sha256"] = digest(receipt_by_field)
    return validate_sampled_semantic_receipt(receipt_by_field)


async def verify_sampled_typed_capture(source_export, connection, document, *, scope, policy):
    """Keep full native capture proof, but replay only preselected raw semantic witnesses."""
    require(type(policy) is SemanticSamplingPolicy and type(scope) is SampledCaptureScope, "sample_policy")
    policy.__post_init__()
    keys = scope.network_source_keys
    require(
        type(keys) is tuple
        and 0 < len(keys) <= 2000
        and all(type(key) is str and _ID.fullmatch(key) for key in keys)
        and tuple(sorted(set(keys))) == keys,
        "sample_networks",
    )
    _original_source_factory(source_export, scope.reviewed_origin)
    origin = source_origin(scope.reviewed_origin)
    require(scope.source_profile.schema_name == origin.schema_name, "source_profile")
    sealed = await load_typed_graph_capture(connection, document, expected_anchor=scope.expected_anchor)
    require(source_origin(sealed.document["origin"]) == origin, "verification_scope")
    source_by_network, candidate_by_network = await _verify_samples(source_export, connection, sealed, scope, policy)
    receipt = _sample_receipt(sealed, policy, source_by_network, candidate_by_network)
    if any(
        direction["failures"]
        for network in receipt["networks"]
        for direction in (network["source_to_candidate"], network["candidate_to_source"])
    ):
        error = TypedFHIRGraphError("registry_fhir_typed_sample_failed")
        error.receipt = receipt
        raise error
    sampled_read = SampledTypedGraphRead(sealed, canonical(receipt), _READ_BRAND)
    await sampled_read.verify(connection)
    return sampled_read
