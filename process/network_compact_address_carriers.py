# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Carry selected native-proven source offices without cloning public addresses."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import ApprovedMembershipSource, pin_retained_approved_membership_source
from process.network_approved_site_source import (
    _address_copy_columns,
    copy_approved_site_rows,
    verify_approved_site_sources,
)
from process.network_compact_address_origin import resolve_compact_address_origin
from process.network_custom_address_source import _APPROVED_CTE, _require_transaction
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_validation import _json_object
from process.registry_aca_address_carry import prepare_aca_address_carry
from process.registry_ptg_address_carry import prepare_ptg_address_carry, retained_site_origin_reference
from process.registry_source_recipe_store import (
    _decode_source_inventory_recipes,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
    verify_registry_source_recipes,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

_COMPONENT = "network_compact_address_carriers"
_MAX_BYTES = 256 * 1024
_TEMP_NAMES = ("entity_address_unified", "provider_location_binding")
_BINDINGS = "provider_system,provider_id,location_id,location_key,entity_type,entity_id"


class CompactAddressCarrierError(ValueError):
    """Exact native source, candidate, expected projection or scratch ownership differs."""


def _require(condition, reason):
    if not condition:
        raise CompactAddressCarrierError("compact_address_carriers_" + reason)


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha(value):
    _require(type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value), "receipt_invalid")
    return value


def _count(value, minimum=0, maximum=2**63 - 1):
    _require(type(value) is int and minimum <= value <= maximum, "receipt_invalid")
    return value


@dataclass(frozen=True)
class CompactAddressCarrierReceipt:
    target: MembershipCopyTarget
    control_schema: str
    approved_revision: int
    approved_generation: str
    base_source: PinnedAddressSource
    base_artifact_sha256: str
    runtime_roles: tuple[str, ...]
    source_recipes_sha256: str
    approved_site_sha256: str
    ptg_carry_sha256: str | None
    aca_carry_sha256: str | None
    origin_references: tuple[tuple, ...]
    selected_address_rows: int
    selected_binding_rows: int
    materialized_address_rows: int
    address_table_oid: int | None
    binding_table_oid: int

    def as_dict(self):
        """Retain bounded descriptive evidence; native rederivation grants authority."""
        document = json.loads(_encoded({"component": _COMPONENT, "revision": 1, **asdict(self)}))
        parse_compact_address_carrier_receipt(document)
        return document


def _parse_origin_references(origins):
    _require(type(origins) is list and len(origins) <= 100, "receipt_invalid")
    references = []
    for origin in origins:
        _require(type(origin) is list and len(origin) == 8, "receipt_invalid")
        _count(origin[0], 1)
        _sha(origin[1])
        for oid in origin[2:6]:
            _count(oid, 1, 2**32 - 1)
        _count(origin[6], 1)
        _sha(origin[7])
        references.append(tuple(origin))
    _require(
        references == sorted(set(references)) and len({origin[:6] for origin in references}) == len(references),
        "receipt_invalid",
    )
    return tuple(references)


def parse_compact_address_carrier_receipt(document):
    """Require an exact bounded receipt without trusting its hashes or native names."""
    fields = {"component", "revision", *CompactAddressCarrierReceipt.__dataclass_fields__}
    _require(type(document) is dict and set(document) == fields, "receipt_invalid")
    try:
        _require(len(_encoded(document)) <= _MAX_BYTES, "receipt_too_large")
        _require(
            document["component"] == _COMPONENT and type(document["revision"]) is int and document["revision"] == 1,
            "receipt_invalid",
        )
        copy_target = document["target"]
        _require(
            type(copy_target) is dict and set(copy_target) == set(MembershipCopyTarget.__dataclass_fields__),
            "receipt_invalid",
        )
        copy_target = MembershipCopyTarget(**copy_target)
        base = document["base_source"]
        _require(type(base) is dict and set(base) == {"schema_name", "table_name", "generation_id"}, "receipt_invalid")
        base = PinnedAddressSource(**base)
        _require(
            base.table_name == "entity_address_unified" and base.schema_name != copy_target.schema_name,
            "receipt_invalid",
        )
        _identifier(document["control_schema"])
        roles = document["runtime_roles"]
        _require(type(roles) is list and 1 <= len(roles) <= 32 and roles == sorted(set(roles)), "receipt_invalid")
        for role in roles:
            _identifier(role)
        for name in ("approved_generation", "base_artifact_sha256", "source_recipes_sha256", "approved_site_sha256"):
            _sha(document[name])
        for name in ("ptg_carry_sha256", "aca_carry_sha256"):
            if document[name] is not None:
                _sha(document[name])
        references = _parse_origin_references(document["origin_references"])
        for name in (
            "approved_revision",
            "selected_address_rows",
            "selected_binding_rows",
            "materialized_address_rows",
        ):
            _count(document[name])
        _require(document["materialized_address_rows"] <= document["selected_address_rows"], "receipt_invalid")
        address_oid = document["address_table_oid"]
        if address_oid is not None:
            _count(address_oid, 1, 2**32 - 1)
        _require(document["materialized_address_rows"] == 0 or address_oid is not None, "receipt_invalid")
        _count(document["binding_table_oid"], 1, 2**32 - 1)
        receipt_by_name = {name: document[name] for name in CompactAddressCarrierReceipt.__dataclass_fields__}
        receipt_by_name.update(
            target=copy_target, base_source=base, runtime_roles=tuple(roles), origin_references=tuple(references)
        )
        return CompactAddressCarrierReceipt(**receipt_by_name)
    except (ValueError, TypeError, KeyError) as error:
        if isinstance(error, CompactAddressCarrierError):
            raise
        raise CompactAddressCarrierError("compact_address_carriers_receipt_invalid") from error


@dataclass(frozen=True)
class CompactAddressCarrierProjection:
    receipt: CompactAddressCarrierReceipt
    address_relation: str
    binding_relation: str
    materialized_address_sql: str
    canonical_binding_sql: str
    arguments: tuple


async def _scope(connection, copy_target, base, approved, roles, schema, *, writing):
    _require(type(copy_target) is MembershipCopyTarget and type(base) is PinnedAddressSource, "scope_invalid")
    _require(
        type(approved) is ApprovedMembershipSource
        and type(roles) is tuple
        and 1 <= len(roles) <= 32
        and roles == tuple(sorted(set(roles))),
        "scope_invalid",
    )
    _require(
        base.table_name == "entity_address_unified" and base.schema_name != copy_target.schema_name, "scope_invalid"
    )
    _identifier(schema)
    for role in roles:
        _identifier(role)
    await _require_transaction(connection)
    control = _identifier(schema)
    candidate = (
        await _locked_candidate(connection, copy_target, control)
        if writing
        else await connection.fetchrow(
            f"SELECT * FROM {control}.network_membership_candidate WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
        )
    )
    _require(
        candidate is not None
        and all(
            str(candidate[field]) == getattr(copy_target, field)
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        ),
        "candidate_changed",
    )
    _require(
        candidate["state"]
        in ({"open", "sealed", "validated"} if writing else {"open", "sealed", "validated", "ready"}),
        "candidate_changed",
    )
    generations = _json_object(candidate["source_generations"])
    _require(
        candidate["approved_custom_revision"] == approved.approved_revision
        and generations.get("custom_membership") == approved.generation_id
        and generations.get("unified_address") == base.generation_id,
        "approved_scope_changed",
    )
    descriptor, physical = await resolve_compact_address_origin(
        connection, candidate, base, runtime_roles=roles, control_schema=schema
    )
    return dict(candidate), descriptor, physical


async def _temp_identity(connection, backend, namespace_oid, owned):
    row = await connection.fetchrow(
        "SELECT pg_backend_pid() AS backend,pg_my_temp_schema()::bigint AS namespace_oid,"
        "to_regclass('pg_temp.entity_address_unified')::oid::bigint AS address_oid,"
        "to_regclass('pg_temp.provider_location_binding')::oid::bigint AS binding_oid"
    )
    _require(row["backend"] == backend, "scratch_backend_changed")
    if namespace_oid is not None:
        _require(row["namespace_oid"] == namespace_oid, "scratch_schema_changed")
    for name, oid in owned.items():
        _require(row["address_oid" if name == _TEMP_NAMES[0] else "binding_oid"] == oid, "scratch_identity_changed")
    return row


@asynccontextmanager
async def _scratch(connection, physical):
    before = await connection.fetchrow(
        "SELECT pg_backend_pid() AS backend,to_regclass('pg_temp.entity_address_unified')::oid AS address_oid,"
        "to_regclass('pg_temp.provider_location_binding')::oid AS binding_oid"
    )
    _require(before["address_oid"] is None and before["binding_oid"] is None, "scratch_names_in_use")
    owned, state = {}, {"started": False, "namespace": None}
    transaction = connection.transaction()

    async def _start():
        await transaction.start()
        state["started"] = True

    async def _commit():
        await transaction.commit()
        state["started"] = False

    async def _create():
        base = _identifier(physical.schema_name) + "." + _identifier(physical.table_name)
        statements = (
            f"CREATE TEMP TABLE entity_address_unified(LIKE {base} INCLUDING DEFAULTS) ON COMMIT DROP",
            "CREATE TEMP TABLE provider_location_binding(provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,PRIMARY KEY(provider_system,provider_id,location_id)) ON COMMIT DROP",
        )
        for name, statement in zip(_TEMP_NAMES, statements, strict=True):
            await connection.execute(statement)
            row = await _temp_identity(connection, before["backend"], state["namespace"], owned)
            state["namespace"] = row["namespace_oid"]
            oid = row["address_oid" if name == _TEMP_NAMES[0] else "binding_oid"]
            _require(type(oid) is int and oid > 0, "scratch_creation_failed")
            owned[name] = oid
        await connection.execute("ALTER TABLE pg_temp.entity_address_unified ADD PRIMARY KEY(location_key)")

    try:
        await drain_operation(_start(), preserve_cancellation=True)
        await drain_operation(_create(), preserve_cancellation=True)
        yield
        await _temp_identity(connection, before["backend"], state["namespace"], owned)
        for name in reversed(_TEMP_NAMES):
            await connection.execute("DROP TABLE pg_temp." + _identifier(name))
        await drain_operation(_commit(), preserve_cancellation=True)
    except BaseException:
        if state["started"]:
            await drain_operation(transaction.rollback(), preserve_cancellation=False)
        raise


def _selection_chunk(origins, reference, rows, digest):
    if reference not in origins:
        _require(len(origins) < 100, "origin_limit")
        origins[reference] = [0, hashlib.sha256()]
    origins[reference][0] += rows
    encoded = _encoded([rows, digest])
    origins[reference][1].update(len(encoded).to_bytes(8, "big"))
    origins[reference][1].update(encoded)


async def _fill(connection, approved, recipes, schema, office_authority, existing_source, *, inventory=False):
    sites = await verify_approved_site_sources(
        connection, approved_revision=approved.approved_revision, control_schema=schema
    )
    site_digest, origins = hashlib.sha256(), {}
    for site in sites:
        encoded = _encoded(site.as_dict())
        site_digest.update(len(encoded).to_bytes(8, "big"))
        site_digest.update(encoded)
        _selection_chunk(
            origins, retained_site_origin_reference(site), len(site.records), hashlib.sha256(encoded).hexdigest()
        )
        await copy_approved_site_rows(
            connection, (site,), "pg_temp", exact_rows=True, reproject_network_ids=True, control_schema=schema
        )
    inventory_options = {"inventory": True} if inventory else {}
    ptg = await prepare_ptg_address_carry(
        connection, recipes, approved, schema, office_authority, "pg_temp", **inventory_options
    )
    for origin in ptg.origin_references:
        _selection_chunk(origins, origin[:6], origin[6], origin[7])
    aca = await prepare_aca_address_carry(
        connection,
        recipes,
        approved,
        schema,
        "pg_temp",
        writing=True,
        existing_source=existing_source,
        **inventory_options,
    )
    references = tuple((*origin, origins[origin][0], origins[origin][1].hexdigest()) for origin in sorted(origins))
    return site_digest.hexdigest(), ptg.sha256, aca.sha256, references


def _projection_sql(physical, schema):
    base = _identifier(physical.schema_name) + "." + _identifier(physical.table_name)
    prefix = _APPROVED_CTE.format(control=_identifier(schema))
    addresses = (
        prefix
        + f"""SELECT carrier.* FROM pg_temp.entity_address_unified carrier
      WHERE NOT EXISTS(SELECT FROM {base} base WHERE base.location_key=carrier.location_key AND base.row_origin<>'network_custom')
      AND NOT EXISTS(SELECT FROM custom WHERE custom.location_key=carrier.location_key)"""
    )
    bindings = (
        prefix
        + """SELECT carrier.* FROM pg_temp.provider_location_binding carrier
      WHERE NOT EXISTS(SELECT FROM custom WHERE (custom.provider_system,custom.provider_id,custom.location_id)=
        (carrier.provider_system,carrier.provider_id,carrier.location_id))"""
    )
    return base, addresses, bindings


async def _base_parity(connection, base):
    invalid = await connection.fetchval(f"""SELECT EXISTS(SELECT FROM pg_temp.entity_address_unified expected
      JOIN {base} base USING(location_key) WHERE base.row_origin<>'network_custom'
      AND (to_jsonb(expected)-'canonical_network_ids') IS DISTINCT FROM (to_jsonb(base)-'canonical_network_ids'))""")
    _require(invalid is False, "base_payload_conflict")


async def _target_oids(connection, target):
    row = await connection.fetchrow(
        "SELECT to_regclass($1)::oid::bigint AS address_oid,to_regclass($2)::oid::bigint AS binding_oid",
        target.schema_name + ".entity_address_unified",
        target.schema_name + ".provider_location_binding",
    )
    _require(type(row["binding_oid"]) is int, "target_binding_missing")
    return row["address_oid"], row["binding_oid"]


async def _projection(connection, target, base, approved, roles, schema, office_authority, *, writing):
    candidate, descriptor, physical = await _scope(connection, target, base, approved, roles, schema, writing=writing)
    recipes, recipe_sha, inventory = await _candidate_source_recipes(connection, target, candidate, schema)
    receipt_by_field = dict(
        target=target,
        control_schema=schema,
        approved_revision=approved.approved_revision,
        approved_generation=approved.generation_id,
        base_source=base,
        base_artifact_sha256=descriptor["descriptor_sha256"],
        runtime_roles=roles,
        source_recipes_sha256=recipe_sha,
    )
    return receipt_by_field, physical, recipes, inventory


async def _candidate_source_recipes(connection, target, candidate, schema):
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_inventory_store import _load_locked_source_inventory

    metadata = verify_source_recipe_metadata(candidate)
    if metadata.inventory is None:
        recipes = verify_registry_source_recipes(candidate)
        return recipes, registry_source_recipes_sha256(canonical_registry_source_recipes(recipes)), None
    inventory_oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint", _identifier(target.schema_name) + ".network_source_inventory"
    )
    reference, entries = await _load_locked_source_inventory(
        connection, target, candidate, expected_table_oid=inventory_oid, control_schema=schema
    )
    return _decode_source_inventory_recipes(entries), reference.root.recipes_sha256, reference


async def _fill_fhir_preservation(connection, target, schema, *, source_witness_runtime=None):
    """Reconstruct historical carry in this phase, after current source selection."""
    from process.registry_fhir_serving_preservation import (
        carry_fhir_preservation_addresses,
        cleanup_fhir_serving_preservation,
        rederive_fhir_serving_preservation,
    )

    candidate = await connection.fetchrow(
        f"SELECT * FROM {_identifier(schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(target.candidate_id),
    )
    _require(candidate is not None, "candidate_changed")
    preservation, primary = None, None
    try:
        preservation = await rederive_fhir_serving_preservation(
            connection,
            target,
            dict(candidate),
            control_schema=schema,
            **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
        )
        await carry_fhir_preservation_addresses(connection, preservation, "pg_temp", writing=True)
    except BaseException as error:
        primary = error
        raise
    finally:
        await cleanup_fhir_serving_preservation(connection, preservation, primary=primary)


def _inventory_recipes_match(recipes, native_recipes):
    return (
        type(recipes) in {tuple, list}
        and len(recipes) == len(native_recipes)
        and all(
            canonical_registry_source_recipes((recipe,)) == canonical_registry_source_recipes((native,))
            for recipe, native in zip(recipes, native_recipes, strict=True)
        )
    )


async def _materialize(connection, copy_target, physical, addresses, bindings, approved):
    namespace = _identifier(copy_target.schema_name)
    carrier_rows = await connection.fetchval(f"SELECT count(*) FROM ({addresses}) expected", approved.approved_revision)
    address_oid, _ = await _target_oids(connection, copy_target)
    if carrier_rows and address_oid is None:
        base = _identifier(physical.schema_name) + "." + _identifier(physical.table_name)
        await connection.execute(f"CREATE TABLE {namespace}.entity_address_unified(LIKE {base} INCLUDING DEFAULTS)")
        await connection.execute(f"ALTER TABLE {namespace}.entity_address_unified ADD PRIMARY KEY(location_key)")
    if carrier_rows:
        conflict = await connection.fetchval(
            f"""WITH expected AS ({addresses}) SELECT EXISTS(
          SELECT FROM expected JOIN {namespace}.entity_address_unified actual USING(location_key)
          WHERE (to_jsonb(expected)-'canonical_network_ids') IS DISTINCT FROM (to_jsonb(actual)-'canonical_network_ids'))""",
            approved.approved_revision,
        )
        _require(conflict is False, "target_payload_conflict")
        columns = await _address_copy_columns(connection, '"pg_temp"', namespace)
        names = ",".join(columns)
        await connection.execute(
            f"INSERT INTO {namespace}.entity_address_unified({names}) SELECT {names} FROM ({addresses}) expected ON CONFLICT(location_key) DO NOTHING",
            approved.approved_revision,
        )
    conflict = await connection.fetchval(
        f"""WITH expected AS ({bindings}) SELECT EXISTS(
      SELECT FROM expected JOIN {namespace}.provider_location_binding actual
      ON (actual.provider_system,actual.provider_id,actual.location_id)=(expected.provider_system,expected.provider_id,expected.location_id)
        OR actual.location_key=expected.location_key
      WHERE ((actual.provider_system,actual.provider_id,actual.location_id)=
          (expected.provider_system,expected.provider_id,expected.location_id)
        AND to_jsonb(actual) IS DISTINCT FROM to_jsonb(expected))
      OR (actual.location_key=expected.location_key AND
        ((actual.provider_system,actual.provider_id,actual.entity_type,actual.entity_id)
          IS DISTINCT FROM (expected.provider_system,expected.provider_id,expected.entity_type,expected.entity_id)
        OR (expected.provider_system='manual' AND actual.location_id<>expected.location_id))))""",
        approved.approved_revision,
    )
    _require(conflict is False, "target_binding_conflict")
    await connection.execute(
        f"INSERT INTO {namespace}.provider_location_binding({_BINDINGS}) SELECT {_BINDINGS} FROM ({bindings}) expected ON CONFLICT DO NOTHING",
        approved.approved_revision,
    )
    return carrier_rows


def _receipt(receipt_by_field, proof, counts, oids):
    return CompactAddressCarrierReceipt(
        **receipt_by_field,
        approved_site_sha256=proof[0],
        ptg_carry_sha256=proof[1],
        aca_carry_sha256=proof[2],
        origin_references=proof[3],
        selected_address_rows=counts[0],
        selected_binding_rows=counts[1],
        materialized_address_rows=counts[2],
        address_table_oid=oids[0],
        binding_table_oid=oids[1],
    )


@asynccontextmanager
async def rederive_compact_address_carrier_projection(
    connection, receipt, *, runtime_roles, control_schema=None, office_authority=None, **source_options
):
    """Yield native-proven carrier SQL using `(approved_revision,)` arguments.
    Compare complete heaps with direct-pair precedence; preserve original source aliases.
    """
    _require(set(source_options) <= {"source_witness_runtime"}, "source_runtime_invalid")
    _require(type(receipt) is CompactAddressCarrierReceipt, "receipt_invalid")
    receipt = parse_compact_address_carrier_receipt(receipt.as_dict())
    _require(
        runtime_roles == receipt.runtime_roles and (control_schema or receipt.control_schema) == receipt.control_schema,
        "scope_changed",
    )
    schema = receipt.control_schema
    approved = await pin_retained_approved_membership_source(
        connection, approved_revision=receipt.approved_revision, control_schema=schema
    )
    receipt_by_field, physical, recipes, inventory = await _projection(
        connection,
        receipt.target,
        receipt.base_source,
        approved,
        runtime_roles,
        schema,
        office_authority,
        writing=False,
    )
    async with _scratch(connection, physical):
        proof = await _fill(
            connection,
            approved,
            recipes,
            schema,
            office_authority,
            physical,
            inventory=inventory is not None,
        )
        await _fill_fhir_preservation(connection, receipt.target, schema, **source_options)
        base, addresses, bindings = _projection_sql(physical, schema)
        await _base_parity(connection, base)
        counts = await connection.fetchrow(
            f"SELECT (SELECT count(*) FROM pg_temp.entity_address_unified),(SELECT count(*) FROM pg_temp.provider_location_binding),(SELECT count(*) FROM ({addresses}) expected)",
            approved.approved_revision,
        )
        actual = _receipt(
            receipt_by_field,
            proof,
            tuple(counts.values()),
            await _target_oids(connection, receipt.target),
        )
        _require(actual == receipt, "native_receipt_changed")
        yield CompactAddressCarrierProjection(
            actual,
            "pg_temp.entity_address_unified",
            "pg_temp.provider_location_binding",
            addresses,
            bindings,
            (approved.approved_revision,),
        )


async def prepare_compact_address_carriers(
    connection, copy_target, base_source, approved, recipes, *, runtime_roles, control_schema, **source_options
):
    """Merge only selected source rows after the caller prepares direct manual rows."""
    _require(set(source_options) <= {"source_witness_runtime", "office_authority"}, "source_runtime_invalid")
    office_authority = source_options.pop("office_authority", None)
    async with connection.transaction():
        receipt_by_field, physical, native_recipes, inventory = await _projection(
            connection,
            copy_target,
            base_source,
            approved,
            runtime_roles,
            control_schema,
            office_authority,
            writing=True,
        )
        _require(
            _inventory_recipes_match(recipes, native_recipes)
            if inventory is not None
            else canonical_registry_source_recipes(recipes) == canonical_registry_source_recipes(native_recipes),
            "recipes_changed",
        )
        _require(
            await connection.fetchval(
                "SELECT pg_has_role(current_user,nspowner,'MEMBER') AND has_schema_privilege(current_user,oid,'CREATE') FROM pg_namespace WHERE nspname=$1",
                copy_target.schema_name,
            )
            is True,
            "target_owner_required",
        )
        async with _scratch(connection, physical):
            proof = await _fill(
                connection,
                approved,
                recipes,
                control_schema,
                office_authority,
                physical,
                inventory=inventory is not None,
            )
            await _fill_fhir_preservation(connection, copy_target, control_schema, **source_options)
            base, addresses, bindings = _projection_sql(physical, control_schema)
            await _base_parity(connection, base)
            carrier_rows = await _materialize(connection, copy_target, physical, addresses, bindings, approved)
            selected = await connection.fetchrow(
                "SELECT (SELECT count(*) FROM pg_temp.entity_address_unified),(SELECT count(*) FROM pg_temp.provider_location_binding)"
            )
            receipt = _receipt(
                receipt_by_field,
                proof,
                (*selected.values(), carrier_rows),
                await _target_oids(connection, copy_target),
            )
            receipt.as_dict()
            return receipt


async def verify_compact_address_carriers(
    connection, receipt, *, runtime_roles, control_schema=None, office_authority=None, source_witness_runtime=None
):
    """Verify every expected carrier row/binding; the parent rejects unrelated rows."""
    async with rederive_compact_address_carrier_projection(
        connection,
        receipt,
        runtime_roles=runtime_roles,
        control_schema=control_schema,
        office_authority=office_authority,
        **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
    ) as projection:
        namespace = _identifier(receipt.target.schema_name)
        if receipt.materialized_address_rows:
            differs = await connection.fetchval(
                f"""WITH expected AS ({projection.materialized_address_sql})
              SELECT EXISTS(SELECT FROM expected LEFT JOIN {namespace}.entity_address_unified actual USING(location_key)
              WHERE actual.location_key IS NULL OR (to_jsonb(actual)-'canonical_network_ids') IS DISTINCT FROM (to_jsonb(expected)-'canonical_network_ids'))""",
                *projection.arguments,
            )
            _require(differs is False, "expected_rows_differ")
        differs = await connection.fetchval(
            f"""WITH expected AS ({projection.canonical_binding_sql})
          SELECT EXISTS(SELECT FROM expected LEFT JOIN {namespace}.provider_location_binding actual
            USING(provider_system,provider_id,location_id) WHERE actual.location_key IS NULL OR to_jsonb(actual) IS DISTINCT FROM to_jsonb(expected))""",
            *projection.arguments,
        )
        _require(differs is False, "expected_bindings_differ")
        return projection.receipt
