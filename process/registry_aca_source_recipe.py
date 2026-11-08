# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit exact ACA recipes only inside protected reference-export custody.

The protected schema comment is a retention receipt, not an accepted-event
inbox. Discovery, retention holds and refresh scheduling must use an independently
qualified notification store before this source can drive recurring publication.
"""

import hashlib
import json
import re
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text

from process.mrf_publication_receipt import require_completed_publication
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import native_driver
from process.network_legacy_membership_source import (
    PinnedACAMembershipSource,
    _require_archive,
    _require_native_archive,
    aca_checksum_scope,
)
from process.reference_family_archive import (
    _canonical_json,
    _source_metadata,
    mrf_publication_digest,
    prepare_reference_family_activation,
    protected_publisher_owner,
    reference_family_profile_contract,
    reference_family_stage_schema,
)
from process.registry_source_recipe_store import (
    MAX_SOURCE_RECIPES,
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)

CONTRACT = "registry-aca-retained-recipe/v1"
_PREFIX = CONTRACT + ":"
BATCH_CONTRACT = "registry-aca-initial-batch/v1"
_BATCH_KEY = "registry_aca_initial_batch"
_BATCH_PREFIX = BATCH_CONTRACT + ":"
_MEMBERSHIP_FIELDS = {
    "source_id",
    "release_id",
    "import_id",
    "issuer_id",
    "year",
    "source_url",
    "alias_scope",
    "reviewed_alias_sha256",
    "runtime_roles",
}


def _invalid():
    raise ValueError("registry_aca_source_recipe_invalid")


def _scope(metadata, runtime_roles):
    if not isinstance(metadata, dict) or set(metadata) != {"network_membership", "network_bindings"}:
        _invalid()
    membership = metadata["network_membership"]
    bindings = metadata["network_bindings"]
    if type(membership) is not dict or set(membership) != _MEMBERSHIP_FIELDS or type(bindings) is not dict:
        _invalid()
    if set(bindings) != {*RegistryNetworkSourceCoordinates.__dataclass_fields__, "source_key_kind"}:
        _invalid()
    coordinates = RegistryNetworkSourceCoordinates(
        **{key: coordinate for key, coordinate in bindings.items() if key != "source_key_kind"}
    )
    if (
        coordinates.source_system != "aca"
        or coordinates.source_id != membership["source_id"]
        or bindings["source_key_kind"] != "hios_plan_id"
        or type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 32
        or tuple(sorted(set(runtime_roles))) != runtime_roles
        or membership["runtime_roles"] != list(runtime_roles)
    ):
        _invalid()
    aca_checksum_scope(membership["issuer_id"], membership["year"], membership["alias_scope"])
    for name, maximum in (("source_id", 128), ("release_id", 256), ("import_id", 256), ("source_url", 2048)):
        component = membership[name]
        if (
            type(component) is not str
            or not 1 <= len(component.encode()) <= maximum
            or component.strip() != component
            or not component.isprintable()
        ):
            _invalid()
    if type(membership["reviewed_alias_sha256"]) is not str or not re.fullmatch(
        "[0-9a-f]{64}", membership["reviewed_alias_sha256"]
    ):
        _invalid()
    return coordinates, membership


def _bind_dataset(metadata, dataset_id):
    """Derive only the operation-owned physical identity; refuse conflicting claims."""
    if type(dataset_id) is not UUID or not dataset_id.int:
        _invalid()
    metadata = _source_metadata(metadata)[0]
    bindings = metadata.get("network_bindings")
    if type(bindings) is not dict:
        _invalid()
    physical_coordinates_map = {
        "dataset_schema": reference_family_stage_schema(dataset_id),
        "dataset_id": str(dataset_id),
    }
    if any(key in bindings and bindings[key] != value for key, value in physical_coordinates_map.items()):
        _invalid()
    if bool("dataset_schema" in bindings) != bool("dataset_id" in bindings):
        _invalid()
    return {**metadata, "network_bindings": {**bindings, **physical_coordinates_map}}


async def capture_registry_aca_source_metadata(session, metadata, *, schema, runtime_roles, dataset_id):
    """Bind explicit coordinates to the locked, completed summary publication."""
    metadata = _bind_dataset(metadata, dataset_id)
    _coordinates, membership = _scope(metadata, runtime_roles)
    completed = await require_completed_publication(session, schema, native_set=True)
    if completed["import_id"] != membership["import_id"]:
        _invalid()
    completion_map = {
        "attempt_id": str(completed["attempt_id"]),
        "import_id": completed["import_id"],
        "generation": completed["generation"],
        "summary_oid": completed["summary_oid"],
        "summary_inputs": completed["summary_inputs"],
        "address_content_sha256": hashlib.sha256(_canonical_json(completed["native_address_coverage"])).hexdigest(),
    }
    captured_metadata_map = {**metadata, "aca_completed_publication": completion_map}
    return _source_metadata(captured_metadata_map)[0]


def _recipe(prepared, validation, runtime_roles, *, descriptor=None):
    metadata = dict(prepared.manifest.source_metadata)
    completion = metadata.pop("aca_completed_publication", None)
    coordinates, membership = _scope(metadata if descriptor is None else descriptor, runtime_roles)
    if (
        type(completion) is not dict
        or set(completion)
        != {"attempt_id", "import_id", "generation", "summary_oid", "summary_inputs", "address_content_sha256"}
        or prepared.manifest.importer_id != "mrf"
        or prepared.manifest.publication_authority != "tracked-generation"
        or prepared.manifest.source_serving_generation is None
        or completion["generation"]["serving_generation"] != prepared.manifest.source_serving_generation.as_dict()
        or completion["import_id"] != membership["import_id"]
        or coordinates.dataset_schema != prepared.ownership.schema_name
        or coordinates.dataset_id != str(prepared.ownership.dataset_id)
    ):
        _invalid()
    coverage = (prepared.manifest.auxiliary or {}).get("address_coverage")
    if coverage is not None and (
        completion["address_content_sha256"] != hashlib.sha256(_canonical_json(coverage)).hexdigest()
        or prepared.manifest.auxiliary["publication_sha256"]
        != mrf_publication_digest({**completion, "native_address_coverage": coverage})
    ):
        _invalid()
    pin = PinnedACAMembershipSource(
        prepared,
        validation,
        **{key: field_value for key, field_value in membership.items() if key != "runtime_roles"},
        runtime_roles=runtime_roles,
    )
    return RegistrySourceMembershipRecipe(pin, coordinates)


async def admit_registry_aca_source_recipe(session, prepared, *, runtime_roles):
    """Validate and stamp one protected clone before its original lease CAS commits.

    The caller owns the source capture and clone transactions. Its precreation
    custody closes writes before COPY; no role grant or source approval occurs.
    Any validation, recording or cancellation failure rolls back this stamp with
    the operation's original clone transaction.
    """
    if not session.in_transaction():
        _invalid()
    owner_oid = await protected_publisher_owner(session)
    validation = await prepare_reference_family_activation(
        session,
        ownership=prepared.ownership,
        manifest=prepared.manifest,
        package_id=hashlib.sha256(_canonical_json(prepared.manifest.as_dict())).hexdigest(),
        profile_contract=reference_family_profile_contract(prepared.manifest),
        sealed_owner_oid=owner_oid,
    )
    recipe = _recipe(prepared, validation, runtime_roles)
    await _require_archive(await native_driver(session), recipe.source_pin)
    encoded = _PREFIX + canonical_registry_source_recipes((recipe,))
    current = await session.scalar(
        text("SELECT obj_description(CAST(:oid AS oid),'pg_namespace')"), {"oid": prepared.ownership.schema_oid}
    )
    if current not in (None, encoded):
        _invalid()
    await (await native_driver(session)).execute(
        f'COMMENT ON SCHEMA "{prepared.ownership.schema_name}" IS ' + "'" + encoded.replace("'", "''") + "'"
    )
    return recipe


async def require_registry_aca_source_recipe(session, prepared, *, runtime_roles, binding_coordinates=None):
    """Recheck the protected retained receipt and exact native archive after commit."""
    if _BATCH_KEY in prepared.manifest.source_metadata:
        if type(binding_coordinates) is not RegistryNetworkSourceCoordinates:
            _invalid()
        recipes = await require_registry_aca_source_batch(session, prepared, runtime_roles=runtime_roles)
        matches = [recipe for recipe in recipes if recipe.binding_coordinates == binding_coordinates]
        if len(matches) != 1:
            _invalid()
        return matches[0]
    if not session.in_transaction():
        _invalid()
    encoded = await session.scalar(
        text("SELECT obj_description(CAST(:oid AS oid),'pg_namespace')"), {"oid": prepared.ownership.schema_oid}
    )
    if type(encoded) is not str or not encoded.startswith(_PREFIX):
        _invalid()
    recipes = decode_registry_source_recipes(encoded[len(_PREFIX) :])
    if len(recipes) != 1 or type(recipes[0].source_pin) is not PinnedACAMembershipSource:
        _invalid()
    recipe = recipes[0]
    if (
        recipe.source_pin.prepared != prepared
        or recipe != _recipe(prepared, recipe.source_pin.validation, runtime_roles)
        or encoded != _PREFIX + canonical_registry_source_recipes((recipe,))
        or (binding_coordinates is not None and recipe.binding_coordinates != binding_coordinates)
    ):
        _invalid()
    await _require_archive(await native_driver(session), recipe.source_pin)
    return recipe


def _scope_group(descriptor):
    membership = descriptor["network_membership"]
    return {name: membership[name] for name in ("issuer_id", "year", "import_id")}


def _batch_scopes(scopes, runtime_roles):
    if type(scopes) is not list or not 1 <= len(scopes) <= MAX_SOURCE_RECIPES:
        _invalid()
    coordinates, identities = [], []
    for descriptor in scopes:
        binding, membership = _scope(descriptor, runtime_roles)
        coordinates.append(binding.sql_parameters)
        identities.append(tuple(membership[name] for name in ("issuer_id", "year", "import_id", "source_url")))
    if len(set(coordinates)) != len(scopes) or len(set(identities)) != len(scopes):
        _invalid()
    if scopes != sorted(scopes, key=_canonical_json):
        _invalid()
    physical_pairs = {(binding[2], binding[3]) for binding in coordinates}
    if len(physical_pairs) != 1:
        _invalid()
    return scopes


def _batch_descriptors(metadata, runtime_roles):
    """Decode only a complete bounded batch, without performing database work."""
    metadata = _source_metadata(metadata)[0]
    if set(metadata) != {_BATCH_KEY, "aca_completed_publication"}:
        _invalid()
    batch = metadata[_BATCH_KEY]
    if (
        type(batch) is not dict
        or set(batch) != {"contract", "physical", "scopes", "coverage"}
        or batch["contract"] != BATCH_CONTRACT
    ):
        _invalid()
    completion = metadata["aca_completed_publication"]
    if type(completion) is not dict:
        _invalid()
    scopes = _expand_batch_scopes(batch, completion.get("import_id"), runtime_roles)
    _validate_batch_coverage(_batch_coverage(metadata), scopes)
    return scopes


def _compact_batch(scopes, runtime_roles, coverage):
    """Store physical identity once; each logical source keeps all explicit coordinates."""
    first = scopes[0]["network_bindings"]
    entries = []
    for scope in scopes:
        entries.append(
            {
                **{
                    key: value
                    for key, value in scope["network_membership"].items()
                    if key not in {"import_id", "runtime_roles"}
                },
                **{key: scope["network_bindings"][key] for key in ("producer_id", "edition_id")},
            }
        )
    return {
        "contract": BATCH_CONTRACT,
        "physical": {
            **{key: first[key] for key in ("dataset_schema", "dataset_id")},
            "runtime_roles": list(runtime_roles),
        },
        "scopes": entries,
        "coverage": [{key: value for key, value in group.items() if key != "import_id"} for group in coverage],
    }


def _expand_batch_scopes(batch, import_id, runtime_roles):
    """Reconstruct the unchanged exact scope contract without inferred source identity."""
    physical, entries = batch["physical"], batch["scopes"]
    if (
        type(physical) is not dict
        or set(physical) != {"dataset_schema", "dataset_id", "runtime_roles"}
        or physical["runtime_roles"] != list(runtime_roles)
        or type(entries) is not list
        or not 1 <= len(entries) <= MAX_SOURCE_RECIPES
    ):
        _invalid()
    scopes = []
    scope_fields = (_MEMBERSHIP_FIELDS - {"import_id", "runtime_roles"}) | {"producer_id", "edition_id"}
    for entry in entries:
        if type(entry) is not dict or set(entry) != scope_fields:
            _invalid()
        scopes.append(
            {
                "network_membership": {
                    **{key: field_value for key, field_value in entry.items() if key in _MEMBERSHIP_FIELDS},
                    "import_id": import_id,
                    "runtime_roles": physical["runtime_roles"],
                },
                "network_bindings": {
                    **{key: physical[key] for key in ("dataset_schema", "dataset_id")},
                    **{key: entry[key] for key in ("source_id", "producer_id", "edition_id")},
                    "source_system": "aca",
                    "source_key_kind": "hios_plan_id",
                },
            }
        )
    return _batch_scopes(scopes, runtime_roles)


def _batch_coverage(metadata):
    """Restore the shared completed import before exact coverage validation."""
    coverage = metadata[_BATCH_KEY]["coverage"]
    if type(coverage) is not list or len(coverage) > MAX_SOURCE_RECIPES:
        _invalid()
    if any(type(group) is not dict or "import_id" in group for group in coverage):
        _invalid()
    return [{**group, "import_id": metadata["aca_completed_publication"]["import_id"]} for group in coverage]


def _validate_batch_coverage(coverage, scopes):
    groups = {_canonical_json(_scope_group(scope)) for scope in scopes}
    if type(coverage) is not list or len(coverage) != len(groups) or coverage != sorted(coverage, key=_canonical_json):
        _invalid()
    observed_groups = set()
    for group in coverage:
        if type(group) is not dict or set(group) != {
            "issuer_id",
            "year",
            "import_id",
            "raw_rows",
            "covered_raw_rows",
            "urls",
        }:
            _invalid()
        identity_map = {name: group[name] for name in ("issuer_id", "year", "import_id")}
        key = _canonical_json(identity_map)
        if key not in groups or key in observed_groups:
            _invalid()
        observed_groups.add(key)
        if any(
            type(group[name]) is not int or not 0 < group[name] < 2**63 for name in ("raw_rows", "covered_raw_rows")
        ):
            _invalid()
        if group["raw_rows"] != group["covered_raw_rows"]:
            _invalid()
        urls = group["urls"]
        expected = sorted(
            scope["network_membership"]["source_url"] for scope in scopes if _scope_group(scope) == identity_map
        )
        if type(urls) is not list or len(urls) != len(expected):
            _invalid()
        for entry, url in zip(urls, expected, strict=True):
            if type(entry) is not dict or set(entry) != {
                "source_url",
                "raw_rows",
                "evidence_rows",
                "unmatched_evidence_rows",
            }:
                _invalid()
            if (
                entry["source_url"] != url
                or entry["unmatched_evidence_rows"] != 0
                or type(entry["unmatched_evidence_rows"]) is not int
            ):
                _invalid()
            if any(
                type(entry[name]) is not int or not 0 < entry[name] < 2**63 for name in ("raw_rows", "evidence_rows")
            ):
                _invalid()
            if entry["raw_rows"] > group["raw_rows"] or entry["raw_rows"] > entry["evidence_rows"]:
                _invalid()
        if sum(entry["raw_rows"] for entry in urls) < group["covered_raw_rows"]:
            _invalid()


_BATCH_COVERAGE_SQL = """WITH groups AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset(CAST(:groups AS jsonb)) AS entry(issuer_id integer,year integer,import_id text)
), raw AS MATERIALIZED (
 SELECT r.npi,r.checksum_network,r.network_tier,g.issuer_id,g.year,g.import_id
 FROM {schema}.plan_npi_raw r JOIN groups g ON r.issuer_id=g.issuer_id AND r.year=g.year
), evidence AS MATERIALIZED (
 SELECT e.npi,e.checksum_network,e.network_tier,e.source_url,g.issuer_id,g.year,g.import_id
 FROM {schema}.mrf_address_evidence e JOIN groups g
 ON e.issuer_id=g.issuer_id AND e.year=g.year AND e.import_id=g.import_id
), matched AS MATERIALIZED (
 SELECT e.*,r.npi IS NOT NULL AS covered FROM evidence e LEFT JOIN raw r
 ON (e.npi,e.checksum_network,e.network_tier,e.issuer_id,e.year,e.import_id)
 =(r.npi,r.checksum_network,r.network_tier,r.issuer_id,r.year,r.import_id)
), urls AS (
 SELECT issuer_id,year,import_id,source_url,count(*) AS evidence_rows,
 count(DISTINCT (npi,checksum_network)) FILTER(WHERE covered) AS raw_rows,
 count(*) FILTER(WHERE NOT covered) AS unmatched_evidence_rows
 FROM matched GROUP BY issuer_id,year,import_id,source_url
), bounded_urls AS (SELECT * FROM urls ORDER BY issuer_id,year,import_id,source_url COLLATE "C" LIMIT 101)
SELECT coalesce(jsonb_agg(jsonb_build_object(
 'issuer_id',g.issuer_id,'year',g.year,'import_id',g.import_id,
 'raw_rows',(SELECT count(*) FROM raw r WHERE (r.issuer_id,r.year,r.import_id)=(g.issuer_id,g.year,g.import_id)),
 'covered_raw_rows',(SELECT count(DISTINCT (m.npi,m.checksum_network)) FROM matched m
 WHERE covered AND (m.issuer_id,m.year,m.import_id)=(g.issuer_id,g.year,g.import_id)),
 'urls',coalesce((SELECT jsonb_agg(jsonb_build_object('source_url',u.source_url,'raw_rows',u.raw_rows,
 'evidence_rows',u.evidence_rows,'unmatched_evidence_rows',u.unmatched_evidence_rows) ORDER BY u.source_url COLLATE "C")
 FROM bounded_urls u WHERE (u.issuer_id,u.year,u.import_id)=(g.issuer_id,g.year,g.import_id)),'[]'::jsonb))), '[]'::jsonb)
 FROM groups g"""


async def _capture_batch_coverage(session, schema, scopes):
    from process.network_address_projection import _identifier

    groups = sorted({_canonical_json(_scope_group(scope)) for scope in scopes})
    coverage = await session.scalar(
        text(_BATCH_COVERAGE_SQL.format(schema=_identifier(schema))),
        {"groups": "[" + ",".join(group.decode() for group in groups) + "]"},
    )
    if type(coverage) is str:
        coverage = json.loads(coverage)
    if type(coverage) is not list:
        _invalid()
    coverage.sort(key=_canonical_json)
    _validate_batch_coverage(coverage, scopes)
    return coverage


async def capture_registry_aca_source_batch_metadata(session, scopes, *, schema, runtime_roles, dataset_id):
    """Capture the exact finite URL union under the original completed-source fence."""
    if type(scopes) not in (list, tuple) or not 1 <= len(scopes) <= MAX_SOURCE_RECIPES:
        _invalid()
    descriptors = sorted((_bind_dataset(scope, dataset_id) for scope in scopes), key=_canonical_json)
    _batch_scopes(descriptors, runtime_roles)
    # Validate the complete descriptor byte bound before native reads.
    _source_metadata({_BATCH_KEY: _compact_batch(descriptors, runtime_roles, [])})
    captured = await capture_registry_aca_source_metadata(
        session, descriptors[0], schema=schema, runtime_roles=runtime_roles, dataset_id=dataset_id
    )
    completion = captured["aca_completed_publication"]
    if any(scope["network_membership"]["import_id"] != completion["import_id"] for scope in descriptors):
        _invalid()
    coverage = await _capture_batch_coverage(session, schema, descriptors)
    metadata = {
        _BATCH_KEY: _compact_batch(descriptors, runtime_roles, coverage),
        "aca_completed_publication": completion,
    }
    _batch_descriptors(metadata, runtime_roles)
    return _source_metadata(metadata)[0]


def _batch_recipes(prepared, validation, runtime_roles):
    return tuple(
        _recipe(prepared, validation, runtime_roles, descriptor=scope)
        for scope in _batch_descriptors(dict(prepared.manifest.source_metadata), runtime_roles)
    )


async def admit_registry_aca_source_batch(session, prepared, *, runtime_roles):
    """Stamp every exact scope atomically inside the one original clone transaction."""
    if not session.in_transaction():
        _invalid()
    scopes = _batch_descriptors(dict(prepared.manifest.source_metadata), runtime_roles)
    owner_oid = await protected_publisher_owner(session)
    validation = await prepare_reference_family_activation(
        session,
        ownership=prepared.ownership,
        manifest=prepared.manifest,
        package_id=hashlib.sha256(_canonical_json(prepared.manifest.as_dict())).hexdigest(),
        profile_contract=reference_family_profile_contract(prepared.manifest),
        sealed_owner_oid=owner_oid,
    )
    recipes = _batch_recipes(prepared, validation, runtime_roles)
    await _require_native_archive(await native_driver(session), prepared, validation, runtime_roles)
    coverage = await _capture_batch_coverage(session, prepared.ownership.schema_name, scopes)
    if coverage != _batch_coverage(prepared.manifest.source_metadata):
        _invalid()
    encoded = _BATCH_PREFIX + canonical_registry_source_recipes(recipes)
    current = await session.scalar(
        text("SELECT obj_description(CAST(:oid AS oid),'pg_namespace')"), {"oid": prepared.ownership.schema_oid}
    )
    if current not in (None, encoded):
        _invalid()
    await (await native_driver(session)).execute(
        f'COMMENT ON SCHEMA "{prepared.ownership.schema_name}" IS ' + "'" + encoded.replace("'", "''") + "'"
    )
    return recipes


async def require_registry_aca_source_batch(session, prepared, *, runtime_roles):
    """Recheck the complete immutable stamp without rescanning retained payloads."""
    if not session.in_transaction():
        _invalid()
    _batch_descriptors(dict(prepared.manifest.source_metadata), runtime_roles)
    encoded = await session.scalar(
        text("SELECT obj_description(CAST(:oid AS oid),'pg_namespace')"), {"oid": prepared.ownership.schema_oid}
    )
    if type(encoded) is not str or not encoded.startswith(_BATCH_PREFIX):
        _invalid()
    recipes = decode_registry_source_recipes(encoded[len(_BATCH_PREFIX) :])
    if not recipes or any(type(recipe.source_pin) is not PinnedACAMembershipSource for recipe in recipes):
        _invalid()
    validation = recipes[0].source_pin.validation
    expected = _batch_recipes(prepared, validation, runtime_roles)
    if encoded != _BATCH_PREFIX + canonical_registry_source_recipes(expected):
        _invalid()
    await _require_native_archive(await native_driver(session), prepared, validation, runtime_roles)
    return recipes


@dataclass(frozen=True)
class _VerifiedACABatchRead:
    connection: object
    transaction: tuple
    prepared: object
    runtime_roles: tuple
    stamp: str
    source_generations: tuple


_READ_TRANSACTION_SQL = """SELECT virtualtransaction,current_user::text,
 session_user::text,pg_catalog.current_setting('transaction_isolation')
 FROM pg_catalog.pg_locks WHERE pid=pg_catalog.pg_backend_pid()
 AND locktype='virtualxid' AND mode='ExclusiveLock' AND granted
 AND virtualxid=virtualtransaction LIMIT 2"""


async def _batch_read_transaction(connection):
    """Identify a read transaction without assigning a permanent transaction ID."""
    if not connection.is_in_transaction():
        _invalid()
    transactions = await connection.fetch(_READ_TRANSACTION_SQL)
    if len(transactions) != 1 or transactions[0][3] not in {"repeatable read", "serializable"}:
        _invalid()
    return tuple(transactions[0])


def _is_initial_batch_read(source):
    """Keep successor and global authorities on their existing complete checks."""
    return (
        type(source) is PinnedACAMembershipSource
        and source.retained_successor is None
        and source.retained_epoch is None
        and _BATCH_KEY in source.prepared.manifest.source_metadata
    )


async def prepare_registry_aca_batch_read(connection, source, *, verified=None):
    """Decode one physical batch once within its exact native read transaction."""
    from process.network_cms_registry_address_capture import NativeReceiptSession

    if not _is_initial_batch_read(source):
        if verified is not None:
            _invalid()
        return None
    if verified is not None:
        await require_registry_aca_batch_read(connection, source, verified)
        return verified
    transaction = await _batch_read_transaction(connection)
    recipes = await require_registry_aca_source_batch(
        NativeReceiptSession(connection), source.prepared, runtime_roles=source.runtime_roles
    )
    if source not in tuple(recipe.source_pin for recipe in recipes):
        _invalid()
    if await _batch_read_transaction(connection) != transaction:
        _invalid()
    return _VerifiedACABatchRead(
        connection,
        transaction,
        recipes[0].source_pin.prepared,
        source.runtime_roles,
        _BATCH_PREFIX + canonical_registry_source_recipes(recipes),
        tuple(recipe.source_pin.generation_id for recipe in recipes),
    )


async def require_registry_aca_batch_read(connection, source, verified):
    """Recheck the exact stamp and native authority without rebuilding every scope."""
    if (
        type(verified) is not _VerifiedACABatchRead
        or verified.connection is not connection
        or not _is_initial_batch_read(source)
        or source.prepared != verified.prepared
        or source.runtime_roles != verified.runtime_roles
        or source.generation_id not in verified.source_generations
        or await _batch_read_transaction(connection) != verified.transaction
    ):
        _invalid()
    stamp = await connection.fetchval(
        "SELECT pg_catalog.obj_description($1::oid,'pg_namespace')", source.prepared.ownership.schema_oid
    )
    if stamp != verified.stamp:
        _invalid()
    await _require_native_archive(connection, source.prepared, source.validation, source.runtime_roles)
