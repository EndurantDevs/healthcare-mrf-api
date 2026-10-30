# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read genuine initial publication and inverse witnesses on the caller's actual actor."""

import asyncio
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text

from process import provider_directory_common_serving_journal as common
from process import provider_directory_common_serving_postimages as postimages
from process import provider_directory_ordinary_retention_capacity as capacity
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as initial_contract
from process import provider_directory_profile_initial_history as initial_history
from process import provider_directory_profile_initial_recovery as initial_recovery
from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_cms_preparation import OwnedRelation
from process.provider_directory_output_snapshot_catalog import OUTPUT_CATALOG_SQL, decoded, encoded
from process.provider_directory_projection_stage import _copy_driver

_ACTOR_SQL = (
    "SELECT session_user::text AS session,current_user::text AS current,"
    "current_user::regrole::oid::bigint AS owner,pg_backend_pid() AS backend,"
    "(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid"
)
_METADATA = (
    ("address", receipts.generations.TABLE_NAME, "to_jsonb(state)", "singleton"),
    ("alias", "address_alias_state_v1", "to_jsonb(state)", "singleton"),
    (
        "overlay_alias",
        "address_alias_artifact_state_v1",
        "to_jsonb(state)",
        "artifact_name='provider_directory_address_overlay'",
    ),
    ("profile", "provider_directory_profile_serving_generation", "to_jsonb(state)", "singleton_key='global'"),
    ("doctors", "reference_family_result_generation", "to_jsonb(state)", "importer_id='cms-doctors'"),
    (
        "geo",
        "entity_address_geo_assurance_state",
        "jsonb_build_object('version',active_geo_assurance_version,'table_oid',active_table_oid::bigint,'signature',active_relation_signature,'bindings',active_dependency_bindings)",
        "singleton",
    ),
)


@dataclass(frozen=True)
class _ReadContext:
    fhir: object
    session: object
    driver: object
    task: object
    actor: dict
    isolation: str
    readonly: str
    virtual_xid: str


def _require(condition, reason):
    if not condition:
        raise ValueError("provider_directory_common_initial_witness_" + reason)


@asynccontextmanager
async def _read_transaction(fhir):
    binding = fhir.db._transaction_binding()
    if binding is None:
        async with fhir.db.transaction() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            async with _read_transaction(fhir) as context:
                yield context
        return
    session = binding.session
    _require(session.in_transaction(), "transaction_required")
    driver = await _copy_driver(session)
    if not driver.is_in_transaction():
        await session.execute(text("SELECT 1"))
    _require(driver.is_in_transaction(), "physical_transaction_required")
    actor_by_field = dict(await driver.fetchrow(_ACTOR_SQL))
    isolation = await driver.fetchval("SHOW transaction_isolation")
    readonly = await driver.fetchval("SHOW transaction_read_only")
    _require(isolation in {"repeatable read", "serializable", "read committed"}, "snapshot_required")
    _require(readonly in {"on", "off"}, "snapshot_required")
    from process.network_fhir_source_epoch_pin import _virtual_xid

    virtual_xid = await _virtual_xid(driver)
    context = _ReadContext(
        fhir, session, driver, asyncio.current_task(), actor_by_field, isolation, readonly, virtual_xid
    )
    yield context
    current_binding = fhir.db._transaction_binding()
    _require(
        current_binding is not None and current_binding.session is session and context.task is asyncio.current_task(),
        "actor_changed",
    )
    _require(
        dict(await driver.fetchrow(_ACTOR_SQL)) == actor_by_field and await _virtual_xid(driver) == virtual_xid,
        "actor_changed",
    )


async def _readable(context, schema, names, *, optional=False):
    """Require actual native SELECT rights for the exact named relations."""
    names = sorted(set(names))
    for name in names:
        receipts.qualified(schema, name)
    rows = await context.driver.fetch(
        "SELECT c.relname,c.oid::bigint AS oid,has_schema_privilege(current_user,n.oid,'USAGE') "
        "AND has_table_privilege(current_user,c.oid,'SELECT') AS readable "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=ANY($2::text[])",
        schema,
        names,
    )
    _require(optional or len(rows) == len(names), "relation_missing")
    _require(all(row["readable"] is True for row in rows), "native_read_privilege_required")
    return {row["relname"]: row["oid"] for row in rows}


async def _catalog(context, schema, names, expected_owner):
    await _readable(context, schema, names)
    async with receipts.normalized_receipt_scope(context.fhir.db, context.session, schema):
        rows = await context.driver.fetch(OUTPUT_CATALOG_SQL, schema, list(names), expected_owner, [])
        _require(len(rows) == len(names), "relation_missing")
        catalog_by_name = {}
        for row in rows:
            native_by_field = {
                key: decoded(row[key]) if key in {"topology", "columns", "constraints", "indexes", "acl"} else row[key]
                for key in row.keys()
                if key not in {"acl_closed", "readers_closed", "readers_select"}
            }
            receipts._require_heap(native_by_field, expected_owner)
            catalog_by_name[native_by_field["relname"]] = native_by_field
        return catalog_by_name


async def _receipt(context, schema, build_id):
    """Reuse immutable native layout/payload validation without recursive journal checks."""
    fhir = context.fhir
    await _readable(context, schema, (initial_contract.RECEIPT_TABLE,))
    receipt_row = await fhir.db.first(
        f"SELECT *,payload_sha256=encode(sha256(convert_to(payload::text,'UTF8')),'hex') AS valid_hash FROM {receipts.qualified(schema, initial_contract.RECEIPT_TABLE)} WHERE build_id=:build",
        build=build_id,
    )
    if receipt_row is None:
        return None
    receipt_by_field = dict(fhir._pagination_checkpoint_row_mapping(receipt_row))
    receipt_payload = decoded(receipt_by_field["payload"])
    _require(receipt_by_field["valid_hash"] is True and type(receipt_payload) is dict, "initial_receipt_invalid")
    _require(
        receipt_by_field.get("contract_id") == receipt_payload.get("contract_id") == initial_contract.COMMIT_CONTRACT,
        "initial_receipt_invalid",
    )
    for field, prefix in (("build_id", "pdpb_"), ("generation_id", "pdprofile_"), ("run_id", "run_")):
        _require(
            type(receipt_by_field.get(field)) is str
            and re.fullmatch(prefix + r"[0-9a-f]{32}", receipt_by_field[field]),
            "initial_receipt_invalid",
        )
    _require(
        receipt_by_field["build_id"] == build_id and receipt_by_field.get("committed_at") is not None,
        "initial_receipt_invalid",
    )
    _require(
        re.fullmatch(r"[1-9][0-9]*", str(receipt_by_field.get("publication_xid"))) is not None,
        "initial_receipt_invalid",
    )
    geometry = fhir.profile_capacity.validated_capacity_geometry(receipt_payload.get("capacity_geometry"))
    _require(isinstance(geometry, initial_contract.InitialCapacityGeometry), "initial_receipt_invalid")
    await _receipt_storage(fhir, schema, receipt_by_field, geometry)
    initial._assert_receipt_payload(fhir, receipt_by_field, receipt_payload, geometry)
    await _readable(context, schema, (fhir.ProviderDirectoryProfileBuildCheckpoint.__tablename__,))
    _require(
        not await fhir.db.scalar(
            f"SELECT EXISTS(SELECT 1 FROM {fhir._provider_directory_profile_checkpoint_ref(schema)} WHERE build_id=:build)",
            build=build_id,
        ),
        "initial_checkpoint_not_retired",
    )
    receipt_by_field["payload"] = receipt_payload
    return receipt_by_field


async def _receipt_storage(fhir, schema, receipt_by_field, geometry):
    """Recognize only the exact native layout or authenticated old global-receipt evolution."""
    layout = await initial.receipt_layout(fhir, schema)
    _require(
        layout.relation_oid == geometry.initial_receipt_oid
        and layout.effective_tablespace_oids == (geometry.tablespace_oid,),
        "initial_receipt_storage_changed",
    )
    _require(
        layout.exact_fingerprint == geometry.initial_receipt_storage_fingerprint
        or receipt_by_field.get("singleton_key") == "global"
        and await initial_history.is_historical_fingerprint(
            fhir,
            schema,
            initial_contract.RECEIPT_TABLE,
            layout.relation_oid,
            geometry.initial_receipt_storage_fingerprint,
        ),
        "initial_receipt_storage_changed",
    )


async def _admitted_targets(context, schema, receipt):
    """Recover the full original signed initial snapshot from its consumed immutable lease."""
    fhir, receipt_payload = context.fhir, receipt["payload"]
    names = (
        fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__,
        fhir.ProviderDirectoryProfileCapacityPreflightReceipt.__tablename__,
    )
    await _readable(context, schema, names)
    consumed = await fhir._replay_bound_consumption(
        receipts.qualified(schema, names[0]), receipt["run_id"], receipt["build_id"]
    )
    serving = receipt_payload["serving"]
    receipt_binding_by_field = {
        **serving,
        "build_id": receipt["build_id"],
        "to_source_vector_hash": serving["source_vector_hash"],
        "to_source_context_vector_hash": serving["source_context_vector_hash"],
        "committed_at": receipt["committed_at"],
    }
    geometry = fhir.profile_capacity.validated_capacity_geometry(receipt_payload["capacity_geometry"])
    lease = fhir._verified_provider_directory_profile_replay_lease(consumed, receipt_binding_by_field, geometry)
    _require(
        lease.lease_digest == receipt_payload["lease_digest"]
        and lease.nonce == receipt_payload["preflight_receipt_sha256"]
        and lease.attestation_id == receipt["attestation_id"],
        "initial_lease_changed",
    )
    fhir._assert_replay_timeline(consumed, receipt, lease)
    signed_receipt = lease.signing_preflight_guard["healthcare_receipt"]
    preflight_row = await fhir.db.first(
        f"SELECT receipt_json,consumed_run_id,consumed_attestation_id,consumed_at FROM {receipts.qualified(schema, names[1])} WHERE receipt_sha256=:digest",
        digest=lease.nonce,
    )
    _require(preflight_row is not None, "initial_preflight_missing")
    stored_by_field = fhir._pagination_checkpoint_row_mapping(preflight_row)
    _require(
        decoded(stored_by_field["receipt_json"]) == signed_receipt
        and stored_by_field["consumed_run_id"] == receipt["run_id"]
        and stored_by_field["consumed_attestation_id"] == receipt["attestation_id"]
        and stored_by_field["consumed_at"] is not None,
        "initial_preflight_changed",
    )
    target_by_field = initial_contract.validated_target_state(signed_receipt["serving_generation_preflight"])
    _require(
        initial_contract.target_state_sha256(target_by_field)
        == receipt_payload["initial_target_state_sha256"]
        == signed_receipt["serving_generation_preflight_sha256"],
        "initial_target_hash_changed",
    )
    if target_by_field["contract_id"] == initial_contract.RECOVERED_TARGET_CONTRACT:
        return target_by_field["recovered_initial_publication"]["initial_targets"]
    return target_by_field


def _journal_shape(native):
    """Recognize the existing five-column journal and essential native constraints."""
    _require(
        [(column["name"], column["type"]) for column in native["columns"]]
        == [
            ("generation", "uuid"),
            ("phase", "text"),
            ("source_id", "text"),
            ("document", "jsonb"),
            ("document_sha256", "text"),
        ],
        "journal_catalog_changed",
    )
    _require(
        all(
            column["required"] and column["default"] is None and not column["generated"] and not column["identity"]
            for column in native["columns"]
        ),
        "journal_catalog_changed",
    )
    _require(
        {constraint["name"] for constraint in native["constraints"] if constraint["type"] != "n"}
        == {"pd_serving_recovery_key", *recovery._JOURNAL_CHECKS},
        "journal_catalog_changed",
    )
    _require(
        {
            constraint["name"]: constraint["definition"]
            for constraint in native["constraints"]
            if constraint["type"] == "c"
        }
        == recovery._JOURNAL_CHECKS
        and all(not constraint["deferrable"] and not constraint["deferred"] for constraint in native["constraints"]),
        "journal_catalog_changed",
    )
    _require(
        len(native["indexes"]) == 1
        and native["indexes"][0]["primary"]
        and native["indexes"][0]["keys"] == ["generation", "phase"],
        "journal_catalog_changed",
    )


async def _history(context, schema, receipt):
    """Use the exact immutable receipt worker handle, never a newest-run substitute."""
    generation = recovery.canonical_generation(str(UUID(hex=receipt["run_id"][4:])))
    await _readable(context, schema, (receipts.JOURNAL,))
    journal_rows = await context.driver.fetch(
        f"SELECT phase,source_id,document,document_sha256,document_sha256=encode(sha256(convert_to(document::text,'UTF8')),'hex') AS valid_hash FROM {receipts.qualified(schema, receipts.JOURNAL)} WHERE generation=$1::uuid ORDER BY phase",
        UUID(generation),
    )
    if not journal_rows:
        return {}
    events_by_phase = {}
    for journal_row in journal_rows:
        _require(
            journal_row["valid_hash"] is True and journal_row["phase"] in {"published", "recovered"}, "journal_invalid"
        )
        document = recovery._document(
            journal_row["document"], journal_row["phase"], generation, journal_row["source_id"]
        )
        _require(common.is_common(document["preparation"]), "journal_invalid")
        _require(journal_row["phase"] not in events_by_phase, "journal_invalid")
        events_by_phase[journal_row["phase"]] = {"document": document, "sha256": journal_row["document_sha256"]}
    common.validate_history(events_by_phase)
    if "recovered" in events_by_phase:
        from process.provider_directory_common_serving_recovery import validate_consumed_inverse

        await validate_consumed_inverse(context.fhir.db, schema, events_by_phase)
    published = events_by_phase["published"]["document"]
    native = (await _catalog(context, schema, (receipts.JOURNAL,), published["writer"]["owner"]))[receipts.JOURNAL]
    _journal_shape(native)
    _require(
        all(event["document"]["journal"] == native for event in events_by_phase.values()), "journal_catalog_changed"
    )
    _require(published["writer"]["database_oid"] == context.actor["database_oid"], "database_changed")
    _require(published["writer"]["xid"] == str(receipt["publication_xid"]), "initial_transaction_changed")
    original = initial_contract.validated_target_state(published["before"]["initial_targets"])
    _require(original["contract_id"] == initial_contract.TARGET_CONTRACT, "initial_targets_changed")
    if receipt["payload"]["initial_target_state_sha256"] != initial_contract.target_state_sha256(original):
        _require(await _admitted_targets(context, schema, receipt) == original, "initial_targets_changed")
    receipt_by_field = {key: receipt[key] for key in ("build_id", "generation_id", "run_id", "payload_sha256")}
    _require(published["preparation"]["initial_receipt"] == receipt_by_field, "initial_receipt_changed")
    _require(published["after"]["metadata"]["profile"] is not None, "initial_profile_missing")
    _require(
        all(
            published["after"]["metadata"]["profile"].get(key) == field_value
            for key, field_value in receipt["payload"]["serving"].items()
        ),
        "initial_profile_changed",
    )
    return events_by_phase


async def assert_initial_common_receipt(fhir, schema, build_id, generation_id, publication_xid):
    """Authenticate historical initial publication without requiring it to remain live."""
    async with _read_transaction(fhir) as context:
        receipt = await _receipt(context, schema, build_id)
        if (
            receipt is None
            or receipt["generation_id"] != generation_id
            or str(receipt["publication_xid"]) != str(publication_xid)
        ):
            return False
        return bool(await _history(context, schema, receipt))


async def _metadata(context, schema):
    names = [table for _key, table, _selection, _predicate in _METADATA]
    names.append("provider_directory_cms_serving_receipt")
    await _readable(context, schema, names, optional=True)
    async with receipts.normalized_receipt_scope(context.fhir.db, context.session, schema):
        metadata_by_field = {
            key: await receipts._optional_json(context.driver, schema, table, selection, predicate)
            for key, table, selection, predicate in _METADATA
        }
        metadata_by_field["relations"] = list(
            await receipts.generations._current_relation_oids(context.fhir.db, schema)
        )
        table = receipts.qualified(schema, "provider_directory_cms_serving_receipt")
        if await context.driver.fetchval("SELECT to_regclass($1)::oid", table) is None:
            metadata_by_field["common_receipt"] = None
        else:
            rows = await context.driver.fetch(
                f"SELECT receipt_id,payload FROM {table} receipt WHERE NOT EXISTS(SELECT FROM {table} successor WHERE successor.predecessor_receipt_id=receipt.receipt_id)"
            )
            _require(len(rows) <= 1, "common_history_changed")
            metadata_by_field["common_receipt"] = (
                {"receipt_id": rows[0]["receipt_id"], "payload": decoded(rows[0]["payload"])} if rows else None
            )
        return metadata_by_field


async def _lock_read_committed(context, schema):
    """Require the existing owner freeze before idempotently taking metadata locks."""
    if context.isolation != "read committed":
        return
    _require(context.readonly == "off", "read_committed_unlocked")
    names = tuple(common.documents.FAMILIES.values())
    oids = await _readable(context, schema, names)
    for oid in oids.values():
        await receipts._require_freeze(context.driver, oid)
    await receipts.lock_metadata(context.fhir.db, schema)


async def _retained(context, schema, document):
    """Validate every original copy/scope on its stored owner and current read actor."""
    from process.entity_address_snapshot_receipt import _row_identity

    retained = common.documents.validate_retention(document["preparation"]["retention"])
    _require(retained["creator"]["database_oid"] == context.actor["database_oid"], "database_changed")
    if context.isolation == "read committed":
        _require(context.actor["owner"] == retained["creator"]["owner"], "read_committed_owner_required")
        names = [entry["name"] for entry in (*retained["scopes"], *retained["families"]) if not entry["reused_initial"]]
        await receipts.lock(context.fhir.db, schema, names, mode="SHARE")
    for entry in (*retained["scopes"], *retained["families"]):
        _require(entry["schema"] == schema, "retention_schema_changed")
        if entry["reused_initial"]:
            continue
        oids = await _readable(context, schema, (entry["name"],))
        _require(oids[entry["name"]] == entry["oid"], "retained_oid_changed")
        if context.isolation == "read committed":
            await receipts._require_freeze(context.driver, entry["oid"])
        relation = OwnedRelation(schema, entry["name"], entry["oid"], 0, "p")
        custody = await capacity._custody(context.fhir, relation)
        _require(
            custody["owner_oid"] == retained["creator"]["owner"] and custody["persistence"] == "p",
            "retained_owner_changed",
        )
        kind = entry["family"].removesuffix("_scope") if entry["kind"] == "scope" else None
        catalog, _toast = await capacity._catalog(context.fhir, relation, scope_kind=kind)
        _require(capacity._layout_digest(catalog) == entry["layout_sha256"], "retained_layout_changed")
        async with receipts.normalized_receipt_scope(context.fhir.db, context.session, schema):
            _require(
                await _row_identity(context.session, schema, entry["name"]) == (entry["rows"], entry["data_sha256"]),
                "retained_rows_changed",
            )
    return retained


async def _restored_rows(context, schema, document, retained):
    from process.entity_address_snapshot_receipt import _projected_row_identity

    original = document["before"]["initial_targets"]
    queries = postimages.current_postimage_queries(schema, retained, document["preparation"]["qualified_source_ids"])
    for entry in retained["families"]:
        family = entry["family"]
        projected = queries[family]
        if entry["reused_initial"]:
            layout = await context.fhir._provider_directory_profile_relation_storage_fingerprint(
                entry["oid"], expected_persistence="p"
            )
            _require(
                entry["oid"] == original[family + "_target_oid"]
                and layout.exact_fingerprint == original[family + "_target_storage_fingerprint"],
                "initial_layout_changed",
            )
            count = await context.driver.fetchval(
                f"SELECT count(*) FROM {receipts.qualified(schema, projected.relation)}"
            )
            _require(count == original[family + "_rows"], "initial_rows_changed")
            continue
        async with receipts.normalized_receipt_scope(context.fhir.db, context.session, schema):
            actual = await _projected_row_identity(
                context.session,
                schema,
                projected.relation,
                row_json_sql="row_value.row_json",
                parameters=dict(projected.parameters),
                row_source_sql="(" + projected.query_sql + ")",
            )
            _require(actual == (entry["rows"], entry["data_sha256"]), "restored_rows_changed")


async def _current_inverse(context, schema, events):
    published, recovered = events["published"]["document"], events["recovered"]["document"]
    expected = recovered["after"]
    original = published["before"]["initial_targets"]
    _require(
        type(original) is dict and original.get("contract_id") == initial_contract.TARGET_CONTRACT,
        "initial_targets_missing",
    )
    initial_contract.validated_target_state(original)
    owner = published["writer"]["owner"]
    if context.isolation == "read committed":
        _require(context.actor["owner"] == owner, "read_committed_owner_required")
    retained = await _retained(context, schema, published)
    native = await _catalog(context, schema, tuple(common.documents.FAMILIES.values()), owner)
    _require(native == expected["roots"], "current_roots_changed")
    metadata = await _metadata(context, schema)
    _require(metadata == expected["metadata"], "current_metadata_changed")
    await receipts.generations.read_entity_address_result_generation_authority(context.session, schema_name=schema)
    await _restored_rows(context, schema, published, retained)
    witness_by_field = {
        "initial_targets": original,
        "initial_receipt": published["preparation"]["initial_receipt"],
        "recovery_id": published["generation"],
        "recovered_event_sha256": events["recovered"]["sha256"],
    }
    if original["historical_publication"] is not None:
        await _readable(context, schema, (context.fhir.ImportRun.__tablename__,))
    await initial_recovery.capture_original_history(
        context.fhir,
        schema,
        receipts.qualified(schema, common.documents.FAMILIES["profile"]),
        receipts.qualified(schema, common.documents.FAMILIES["evidence"]),
        witness_by_field,
    )
    _require(
        await _catalog(context, schema, tuple(common.documents.FAMILIES.values()), owner) == expected["roots"],
        "current_roots_changed",
    )
    _require(await _metadata(context, schema) == expected["metadata"], "current_metadata_changed")
    return decoded(encoded(witness_by_field))


async def recovered_initial_targets(fhir, schema):
    """Resolve the exact current inverse, preserving original v1 history across reactivation."""
    async with _read_transaction(fhir) as context:
        await _lock_read_committed(context, schema)
        metadata = await _metadata(context, schema)
        await _readable(context, schema, (receipts.JOURNAL,))
        candidate_events = await context.driver.fetch(
            f"SELECT generation::text,source_id,document,document_sha256,document_sha256=encode(sha256(convert_to(document::text,'UTF8')),'hex') AS valid_hash FROM {receipts.qualified(schema, receipts.JOURNAL)} WHERE phase='recovered' AND document->'preparation'->>'contract_id'=$1 AND document->'after'->'metadata'->'address'=$2::jsonb",
            common.documents.CONTRACT,
            encoded(metadata["address"]),
        )
        _require(len(candidate_events) <= 1, "current_inverse_ambiguous")
        if not candidate_events:
            return None
        event = candidate_events[0]
        _require(event["valid_hash"] is True, "journal_invalid")
        document = recovery._document(event["document"], "recovered", event["generation"], event["source_id"])
        reference = document["preparation"]["initial_receipt"]
        if reference is None or document["after"]["initial_targets"] is None:
            return None
        receipt = await _receipt(context, schema, reference["build_id"])
        _require(receipt is not None, "initial_receipt_missing")
        events = await _history(context, schema, receipt)
        _require(
            "recovered" in events and events["recovered"]["sha256"] == event["document_sha256"],
            "current_inverse_changed",
        )
        return await _current_inverse(context, schema, events)
