# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capture actual ordinary inverse inputs without raw acquisition or invented Profile work."""

import hashlib
from dataclasses import dataclass

from sqlalchemy import text

from process import provider_directory_common_initial_witness as witness
from process import provider_directory_common_serving_journal as common
from process import provider_directory_common_serving_postimages as postimages
from process import provider_directory_serving_inverse_capacity_contract as contract
from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_output_snapshot_catalog import OUTPUT_CATALOG_SQL, decoded, encoded

_METADATA_RELATIONS = (
    "import_run",
    "provider_directory_profile_capacity_preflight_receipt",
    "provider_directory_profile_capacity_lease_consumption",
    "provider_directory_profile_serving_generation",
    "address_alias_artifact_state_v1",
    "entity_address_geo_assurance_state",
    "entity_address_result_generation",
    receipts.JOURNAL,
    "provider_directory_cms_serving_receipt",
)


def document_sha256(value):
    """Reuse canonical finite JSON for immutable native evidence digests."""
    return hashlib.sha256(encoded(value).encode("ascii")).hexdigest()


async def read_common_history(context, generation):
    """Read the actual fixed journal on a read-only snapshot without assigning a transaction ID."""
    schema = context.fhir._schema()
    catalog = await witness._catalog(context, schema, (receipts.JOURNAL,), context.actor["owner"])
    native = catalog[receipts.JOURNAL]
    witness._journal_shape(native)
    from uuid import UUID

    rows = await context.driver.fetch(
        f"SELECT phase,source_id,document,document_sha256,document_sha256=encode(sha256(convert_to(document::text,'UTF8')),'hex') AS valid_hash FROM {receipts.qualified(schema, receipts.JOURNAL)} WHERE generation=$1::uuid ORDER BY phase",
        UUID(contract.canonical_uuid(generation)),
    )
    history_by_field = {}
    for row in rows:
        contract.require(row["valid_hash"] is True and row["phase"] in {"published", "recovered"}, "journal")
        document = recovery._document(row["document"], row["phase"], generation, row["source_id"])
        contract.require(common.is_common(document["preparation"]) and document["journal"] == native, "journal")
        history_by_field[row["phase"]] = dict(document=document, sha256=row["document_sha256"])
    contract.require(bool(history_by_field), "event_missing")
    history_by_field = common.validate_history(history_by_field)
    if "recovered" in history_by_field:
        from process.provider_directory_common_serving_recovery import validate_consumed_inverse

        await validate_consumed_inverse(context.fhir.db, schema, history_by_field)
    return history_by_field


async def _observed_document(context, execution):
    history = await read_common_history(context, execution["generation"])
    contract.require(set(history) == {"published"}, "already_recovered")
    event = history["published"]
    document = event["document"]
    contract.require(
        event["sha256"] == execution["published_event_sha256"]
        and execution["source_ids"] == [document["source"]["source_id"]],
        "event_changed",
    )
    schema = context.fhir._schema()
    native = await witness._catalog(context, schema, tuple(common.documents.FAMILIES.values()), context.actor["owner"])
    actual_by_field = dict(roots=native, metadata=await witness._metadata(context, schema), initial_targets=None)
    contract.require(common.documents.validate_state(actual_by_field) == document["after"], "forward_changed")
    return document


def _profile_provenance(document):
    profile = document["after"]["metadata"]["profile"]
    result = document["preparation"]["profile_result"]
    return {
        key: result[key] if key in {"selection_fingerprint", "profile_input_digest"} else profile[key]
        for key in contract.FIELDS["profile_provenance"]
    }


async def _row_volume(context, query, parameters=()):
    rows = await context.session.execute(
        text(f"SELECT count(*)::bigint,COALESCE(sum(pg_column_size(row_json)),0)::bigint FROM ({query}) observed"),
        dict(parameters),
    )
    count, logical = rows.one()
    contract.integer(count)
    contract.integer(logical)
    return count, logical


async def _families(context, document, baselines):
    schema = context.fhir._schema()
    preparation = document["preparation"]
    retained = common.documents.validate_retention(preparation["retention"])
    queries = postimages.current_postimage_queries(schema, retained, preparation["qualified_source_ids"])
    physical_by_oid = {entry["oid"]: entry for entry in baselines}
    family_observations = []
    for entry in sorted(retained["families"], key=lambda item: item["family"]):
        family = entry["family"]
        if entry["reused_initial"]:
            deleted = inserted = (0, 0)
        else:
            projected = queries[family]
            deleted = await _row_volume(context, projected.query_sql, projected.parameters)
            inserted = await _row_volume(context, postimages._json_rows(schema, entry["name"]))
            contract.require(inserted[0] == entry["rows"], "retained_rows_changed")
        native = document["after"]["roots"][common.documents.FAMILIES[family]]
        family_observations.append(
            dict(
                family=family,
                root_oid=native["oid"],
                native_sha256=physical_by_oid[native["oid"]]["native_sha256"],
                retained_name=entry["name"],
                retained_oid=entry["oid"],
                retained_layout_sha256=entry["layout_sha256"],
                retained_rows=entry["rows"],
                retained_data_sha256=entry["data_sha256"],
                live_rows=deleted[0],
                live_logical_bytes=deleted[1],
                insert_rows=inserted[0],
                insert_logical_bytes=inserted[1],
                expected_postimage=preparation["expected_postimages"][family],
                restore_mode="initial_heap" if entry["reused_initial"] else "rows",
            )
        )
    return family_observations


async def _relation_baselines(context, document):
    schema = context.fhir._schema()
    retained = document["preparation"]["retention"]
    names = set(common.documents.FAMILIES.values()) | {
        entry["name"] for entry in (*retained["scopes"], *retained["families"])
    }
    metadata = await witness._readable(context, schema, _METADATA_RELATIONS, optional=True)
    names.update(metadata)
    await witness._readable(context, schema, names)
    catalog_rows = await context.driver.fetch(OUTPUT_CATALOG_SQL, schema, sorted(names), context.actor["owner"], [])
    contract.require(len(catalog_rows) == len(names), "relation_missing")
    physical_by_oid = await _physical_inventory(context, [catalog_row["oid"] for catalog_row in catalog_rows])
    by_oid = {}
    for catalog_row in catalog_rows:
        native_by_field = {
            key: decoded(catalog_row[key])
            if key in {"topology", "columns", "constraints", "indexes", "acl"}
            else catalog_row[key]
            for key in catalog_row.keys()
            if key not in {"acl_closed", "readers_closed", "readers_select"}
        }
        contract.require(
            native_by_field["topology"]["kind"] == "r" and native_by_field["topology"]["persistence"] == "p",
            "relation_kind",
        )
        contract.require(
            all(index["valid"] and index["ready"] and index["live"] for index in native_by_field["indexes"]),
            "index_changed",
        )
        by_oid[native_by_field["oid"]] = dict(
            schema=schema,
            name=native_by_field["relname"],
            oid=native_by_field["oid"],
            native_sha256=document_sha256(
                dict(
                    catalog=receipts.normalized_native(native_by_field),
                    physical=physical_by_oid[native_by_field["oid"]]["physical"],
                )
            ),
            baseline_total_bytes=physical_by_oid[native_by_field["oid"]]["total_bytes"],
            allowed_names=[native_by_field["relname"]],
        )
    if document["before"]["initial_targets"] is not None:
        for family in ("profile", "evidence"):
            root = common.documents.FAMILIES[family]
            for native_by_field in (document["before"]["roots"][root], document["after"]["roots"][root]):
                by_oid[native_by_field["oid"]]["allowed_names"] = sorted(
                    [root, receipts.retained_name(native_by_field["oid"])]
                )
    return [by_oid[key] for key in sorted(by_oid)]


@dataclass(frozen=True)
class CapturedInverseInputs:
    """Detached evidence is useful for comparison, never resource or write authority."""

    payload_json: str
    document_json: str

    @property
    def payload(self):
        """Return a validated copy of the exact native capture."""
        return contract.validated_inverse_scope(decoded(self.payload_json))

    @property
    def document(self):
        """Return the genuine published event whose bytes were captured."""
        return decoded(self.document_json)


async def capture_inverse_inputs(fhir, execution):
    """Use one original repeatable-read observation; full hashes wait for fresh admission."""
    from process.provider_directory_profile_selection_contract import configured_node_id

    execution = contract.validated_inverse_execution(execution)
    contract.require(execution["node_id"] == configured_node_id(), "node_changed")
    async with witness._read_transaction(fhir) as context:
        document = await _observed_document(context, execution)
        baselines = await _relation_baselines(context, document)
        scope_by_field = dict(
            contract_id=contract.SCOPE,
            operation_sha256=contract.operation_sha256(execution),
            generation=execution["generation"],
            published_event_sha256=execution["published_event_sha256"],
            node_id=execution["node_id"],
            owner_oid=context.actor["owner"],
            schema_oid=next(iter(document["after"]["roots"].values()))["schema_oid"],
            **await _database_identity(context),
            forward_state_sha256=document_sha256(document["after"]),
            retention_payload_sha256=document_sha256(document["preparation"]["retention"]),
            qualified_source_ids=document["preparation"]["qualified_source_ids"],
            initial_receipt=document["preparation"]["initial_receipt"],
            profile_provenance=_profile_provenance(document),
            families=await _families(context, document, baselines),
            scopes=[
                {key: entry[key] for key in contract.FIELDS["retained_scope"]}
                for entry in sorted(document["preparation"]["retention"]["scopes"], key=lambda item: item["family"])
            ],
            relations=baselines,
        )
        scope_by_field["input_sha256"] = contract.digest(contract.INPUT_DOMAIN, scope_by_field)
        return CapturedInverseInputs(encoded(contract.validated_inverse_scope(scope_by_field)), encoded(document))


async def _database_identity(context):
    row = await context.driver.fetchrow(
        "SELECT control.system_identifier::text AS database_system_identifier,d.oid::bigint AS database_oid,d.datname AS database_name FROM pg_database d CROSS JOIN pg_control_system() control WHERE d.datname=current_database()"
    )
    contract.require(row is not None, "database_identity")
    return dict(row)


async def _physical_inventory(context, oids):
    rows = await context.driver.fetch(
        """
        WITH targets AS (SELECT oid,reltoastrelid FROM pg_class WHERE oid=ANY($1::oid[])),
        members(root,oid) AS (
          SELECT oid,oid FROM targets UNION ALL
          SELECT oid,reltoastrelid FROM targets WHERE reltoastrelid<>0 UNION ALL
          SELECT t.oid,i.indexrelid FROM targets t JOIN pg_index i ON i.indrelid IN (t.oid,t.reltoastrelid)
        )
        SELECT t.oid::bigint AS root,pg_total_relation_size(t.oid)::bigint AS total_bytes,
          jsonb_agg(jsonb_build_array(c.oid::bigint,pg_relation_filenode(c.oid)::bigint,c.relkind::text,
                    COALESCE(NULLIF(c.reltablespace,0),(SELECT dattablespace FROM pg_database WHERE datname=current_database()))::bigint)
                    ORDER BY c.oid) AS physical
        FROM targets t JOIN members m ON m.root=t.oid JOIN pg_class c ON c.oid=m.oid
        GROUP BY t.oid ORDER BY t.oid
    """,
        oids,
    )
    contract.require({row["root"] for row in rows} == set(oids), "physical_inventory")
    return {row["root"]: dict(total_bytes=row["total_bytes"], physical=decoded(row["physical"])) for row in rows}


def has_same_captured_inputs(expected, actual):
    """Native bytes may grow but are charged against their original fixed-OID baseline."""
    expected = contract.validated_inverse_scope(expected)
    actual = contract.validated_inverse_scope(actual)
    baseline_by_field = {entry["oid"]: entry["baseline_total_bytes"] for entry in expected["relations"]}
    contract.require({entry["oid"] for entry in actual["relations"]} == set(baseline_by_field), "physical_inventory")
    for entry in actual["relations"]:
        entry["baseline_total_bytes"] = baseline_by_field[entry["oid"]]
    actual["input_sha256"] = contract.digest(
        contract.INPUT_DOMAIN, {key: value for key, value in actual.items() if key != "input_sha256"}
    )
    return actual == expected


async def assert_scope_tablespace(fhir, scope, data_tablespace_oid):
    """Include actual heap, TOAST and index storage on the signed data volume."""
    scope = contract.validated_inverse_scope(scope)
    contract.oid(data_tablespace_oid)
    async with witness._read_transaction(fhir) as context:
        inventory = await _physical_inventory(context, [entry["oid"] for entry in scope["relations"]])
        contract.require(
            all(member[3] == data_tablespace_oid for row in inventory.values() for member in row["physical"]),
            "physical_tablespace_changed",
        )
    return inventory


async def assert_scope_native(fhir, scope, data_tablespace_oid):
    """Keep every signed OID, native method/layout and physical file through allowed renames."""
    scope = contract.validated_inverse_scope(scope)
    async with witness._read_transaction(fhir) as context:
        inventory = await _physical_inventory(context, [entry["oid"] for entry in scope["relations"]])
        located = await context.driver.fetch(
            "SELECT c.oid::bigint,n.nspname AS schema,c.relname AS name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=ANY($1::oid[])",
            [entry["oid"] for entry in scope["relations"]],
        )
        by_oid = {entry["oid"]: entry for entry in scope["relations"]}
        contract.require(
            len(located) == len(by_oid)
            and all(
                catalog_row["schema"] == by_oid[catalog_row["oid"]]["schema"]
                and catalog_row["name"] in by_oid[catalog_row["oid"]]["allowed_names"]
                for catalog_row in located
            ),
            "relation_rebound",
        )
        names = sorted(catalog_row["name"] for catalog_row in located)
        catalog_rows = await context.driver.fetch(OUTPUT_CATALOG_SQL, fhir._schema(), names, scope["owner_oid"], [])
        contract.require(len(catalog_rows) == len(by_oid), "relation_rebound")
        for catalog_row in catalog_rows:
            native_by_field = {
                key: decoded(catalog_row[key])
                if key in {"topology", "columns", "constraints", "indexes", "acl"}
                else catalog_row[key]
                for key in catalog_row.keys()
                if key not in {"acl_closed", "readers_closed", "readers_select"}
            }
            physical = inventory[native_by_field["oid"]]["physical"]
            contract.require(
                all(member[3] == data_tablespace_oid for member in physical), "physical_tablespace_changed"
            )
            actual = document_sha256(dict(catalog=receipts.normalized_native(native_by_field), physical=physical))
            contract.require(actual == by_oid[native_by_field["oid"]]["native_sha256"], "physical_identity_changed")
    return inventory
