# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Internal complete-family identity; portable metadata is never custody authority."""

from dataclasses import dataclass
from datetime import UTC
from uuid import UUID

from process.ptg_parts.db_tables import _quote_ident

PHYSICAL_BINDING_CONTRACT = "ptg.snapshot-local-physical-binding.v1"
_SOURCE_FIELDS = (
    "source_key",
    "source_type",
    "identity_kind",
    "identity_sha256",
    "raw_container_sha256",
    "logical_json_sha256",
    "logical_hash_deferred",
    "source_trace_set_hash",
)


class PTG2PhysicalBindingError(RuntimeError):
    """A local physical family is incomplete, mismatched or not admitted."""


def _local_read_view_columns(is_prepared):
    """Describe the portable named read projection without physical control tokens."""
    names = [
        "contract",
        "destination_snapshot_id",
        "package_id",
        "manifest_sha256",
        "artifact_sha256",
        "native_validation",
        "relation_inventory",
        "sequence_inventory",
    ]
    if not is_prepared:
        names.extend(
            ("generation_id", "publication_fence", "publication_sha256", "generation_inventory", "native_publication")
        )
    return {
        name: "jsonb" if name.endswith("inventory") or name in {"native_validation", "native_publication"} else "text"
        for name in names
    }


def _local_candidate_control_query(
    schema_name, physical_binding, *, snapshot_parameter, payload_parameter, lock_controls=True
):
    """Compile one internal control query for the existing native driver or Session."""
    schema = _quote_ident(schema_name)
    return (
        "SELECT snapshot.*,run.options,run.status AS run_status,run.report AS run_report,"
        "run.options->'invalid_price_exclusion_policy' AS invalid_price_exclusion_policy,"
        "binding.snapshot_key,scope.plan_id,scope.plan_market_type,scope.coverage_scope_id,"
        "layout.state AS layout_state,layout.generation AS layout_generation,layout.generation AS storage_generation,"
        "layout.mapping_digest AS layout_mapping_digest,layout.layout_manifest,"
        "root.state AS v4_root_state,root.map_digest AS v4_root_map_digest,"
        "current_pointer.snapshot_id AS current_snapshot_id,"
        "attestation.report_digest AS audit_report_digest,attestation.report AS audit_report,"
        "attestation.activation_intent AS audit_activation_intent,attestation.activated_at AS audit_activated_at,"
        "attestation.contract AS audit_contract,"
        "attestation.source_key AS attested_source_key,"
        "encode(attestation.coverage_scope_id,'hex') AS attested_coverage_scope_id,"
        "encode(attestation.source_set_digest,'hex') AS attested_source_set_digest,"
        "encode(attestation.audit_sample_digest,'hex') AS attested_audit_sample_digest,"
        "frozen.binding_payload AS frozen_binding_payload,timezone('UTC',statement_timestamp()) AS staged_at "
        f"FROM {schema}.ptg2_snapshot snapshot JOIN {schema}.ptg2_import_run run USING(import_run_id) "
        f"JOIN {schema}.ptg2_v3_snapshot_binding binding USING(snapshot_id) "
        f"JOIN {schema}.ptg2_v3_snapshot_scope scope USING(snapshot_id) "
        f"JOIN {schema}.ptg2_v3_snapshot_layout layout ON layout.snapshot_key=binding.snapshot_key "
        f"JOIN {physical_binding.relation('ptg2_v3_snapshot_layout')} payload_layout "
        f"ON payload_layout.snapshot_key={payload_parameter} AND payload_layout.state='sealed' "
        "AND layout.state=payload_layout.state AND layout.generation=payload_layout.generation "
        "AND layout.mapping_digest=payload_layout.mapping_digest AND layout.support_digest=payload_layout.support_digest "
        "AND layout.layout_manifest=payload_layout.layout_manifest "
        "AND layout.logical_byte_count=payload_layout.logical_byte_count "
        "AND layout.storage_shard_id IS NOT DISTINCT FROM payload_layout.storage_shard_id "
        f"JOIN {physical_binding.relation('ptg2_v4_snapshot_map_root')} root ON root.snapshot_key={payload_parameter} "
        f"LEFT JOIN {schema}.ptg2_current_source_snapshot current_pointer "
        "ON current_pointer.source_key=run.options->>'source_key' "
        f"LEFT JOIN {schema}.ptg2_v3_candidate_audit_attestation attestation ON attestation.snapshot_id=snapshot.snapshot_id "
        f"LEFT JOIN {schema}.ptg2_frozen_source_file_binding frozen ON frozen.internal_run_id=run.import_run_id "
        f"WHERE snapshot.snapshot_id={snapshot_parameter}"
        + (" FOR SHARE OF snapshot,run,binding,scope,layout NOWAIT" if lock_controls else "")
    )


async def _local_candidate_control(session, schema_name, physical_binding, *, lock_controls=True):
    """Read actual destination metadata and the exact isolated immutable map root."""
    from sqlalchemy import text

    return (
        (
            await session.execute(
                text(
                    _local_candidate_control_query(
                        schema_name,
                        physical_binding,
                        snapshot_parameter=":snapshot_id",
                        payload_parameter=":payload_key",
                        lock_controls=lock_controls,
                    )
                ),
                {"snapshot_id": physical_binding.snapshot_id, "payload_key": physical_binding.payload_snapshot_key},
            )
        )
        .mappings()
        .one_or_none()
    )


def _require_local_published_postimage(candidate, plans, evidence, publication, physical_binding):
    """Verify only the permitted staged-to-published control transition."""
    import json
    from copy import deepcopy

    from process.ptg_parts import result_archive_candidate_initialization as initialization
    from process.ptg_parts.ptg2_candidate_attestation import PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
    from process.ptg_parts.source_pointers import candidate_snapshot_attributes

    candidate_by_field = dict(candidate)
    for field_name in ("manifest", "options", "run_report"):
        if isinstance(candidate_by_field[field_name], str):
            candidate_by_field[field_name] = json.loads(candidate_by_field[field_name])
        if not isinstance(candidate_by_field[field_name], dict):
            raise PTG2PhysicalBindingError("PTG local published control encoding differs")
    manifest_by_field = deepcopy(candidate_by_field["manifest"])
    ready = evidence["activation_evidence"]
    activated = publication["destination_activation"]
    plan_scopes = tuple((plan["plan_id"], plan["plan_market_type"]) for plan in plans)
    activation = manifest_by_field.get("activation", {})
    # Snapshot timestamps are UTC-naive; consumed audit timestamps are timestamptz.
    published_at = candidate_by_field["published_at"]
    if published_at is not None and published_at.utcoffset() is None:
        published_at = published_at.replace(tzinfo=UTC)
    if (
        candidate_by_field["status"] != "published"
        or candidate_by_field["run_status"] != "validated"
        or candidate_by_field["import_run_id"] != evidence["initialization"]["destination_import_run_id"]
        or candidate_by_field["snapshot_key"] != physical_binding.destination_layout_key
        or candidate_by_field["previous_snapshot_id"] != ready["expected_current_snapshot_id"]
        or candidate_by_field["plan_id"] != evidence["native_audit"]["identity"]["plan_id"]
        or candidate_by_field["plan_market_type"] != evidence["native_audit"]["identity"]["plan_market_type"]
        or bytes(candidate_by_field["coverage_scope_id"]).hex()
        != evidence["native_audit"]["identity"]["coverage_scope_id"]
        or activation.get("state") != "activated"
        or activation.get("mode") != "reviewed_audit_only_control"
        or candidate_by_field["audit_activation_intent"] != "audit_only"
        or candidate_by_field["audit_contract"] not in PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
        or candidate_by_field["audit_activated_at"] is None
        or candidate_by_field["audit_activated_at"] != published_at
        or activated.get("snapshot_id") != physical_binding.snapshot_id
        or activated.get("source_key") != ready["source_key"]
        or activated.get("previous_snapshot_id") != ready["expected_current_snapshot_id"]
        or activated.get("activated_at") != candidate_by_field["published_at"].isoformat()
        or activated.get("audit_report_digest") != bytes(candidate_by_field["audit_report_digest"] or b"").hex()
        or initialization._local_control_sha256(manifest_by_field, candidate_by_field["options"], plan_scopes)
        != publication["published_control_sha256"]
    ):
        raise PTG2PhysicalBindingError("PTG local published activation differs")
    staged = candidate_snapshot_attributes(
        candidate_by_field, source_key=ready["source_key"], previous_snapshot_id=ready["expected_current_snapshot_id"]
    )
    if (
        initialization._local_control_sha256(staged["manifest"], candidate_by_field["options"], plan_scopes)
        != evidence["control_sha256"]
        or candidate_by_field["run_report"] != staged["manifest"]
    ):
        raise PTG2PhysicalBindingError("PTG local staged preimage changed during publication")


def _require_local_control_postimage(candidate, plans, evidence, physical_binding):
    """Compare actual staged metadata without treating a decoded descriptor as authority."""
    from process.ptg_parts import result_archive_candidate_initialization as initialization

    candidate_by_field = dict(candidate)
    marker_by_field, _key = initialization._local_cleanup_marker(evidence, evidence["initialization"])
    activation_evidence = evidence["activation_evidence"]
    if (
        candidate_by_field["status"] != "validated"
        or candidate_by_field["run_status"] != "validated"
        or candidate_by_field["manifest"].get("physical_binding_contract") != PHYSICAL_BINDING_CONTRACT
        or candidate_by_field["manifest"].get("local_data_preparation") != marker_by_field
        or candidate_by_field["import_run_id"] != evidence["initialization"]["destination_import_run_id"]
        or candidate_by_field["snapshot_key"] != physical_binding.destination_layout_key
        or candidate_by_field["options"].get("source_key") != activation_evidence.get("source_key")
        or candidate_by_field["previous_snapshot_id"] != activation_evidence.get("expected_current_snapshot_id")
        or initialization._local_control_sha256(candidate_by_field["manifest"], candidate_by_field["options"], plans)
        != evidence["control_sha256"]
        or candidate_by_field["run_report"] != candidate_by_field["manifest"]
    ):
        raise PTG2PhysicalBindingError("PTG local candidate control changed")
    return candidate_by_field


async def local_candidate_audit_state(session, *, candidate_run_id=None, snapshot_id=None, schema_name=None):
    """Resolve a prepared candidate in this transaction, never from its metadata marker alone."""
    from sqlalchemy import text

    from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
    from process.ptg_parts.result_archive_candidate_validation import local_data_physical_read_state
    from process.reference_family_archive import _require_transaction

    _require_transaction(session)
    configured_schema = resolve_ptg2_schema()
    if schema_name is not None and schema_name != configured_schema:
        raise PTG2PhysicalBindingError("PTG local control schema differs")
    if not snapshot_id and not candidate_run_id:
        raise PTG2PhysicalBindingError("PTG local candidate selector is missing")
    snapshot_rows = (
        (
            await session.execute(
                text(
                    f"SELECT snapshot_id,import_run_id,manifest FROM {_quote_ident(configured_schema)}.ptg2_snapshot "
                    "WHERE (CAST(:snapshot_id AS text) IS NULL OR snapshot_id=:snapshot_id) "
                    "AND (CAST(:run_id AS text) IS NULL OR import_run_id=:run_id) LIMIT 2"
                ),
                {"snapshot_id": snapshot_id, "run_id": candidate_run_id},
            )
        )
        .mappings()
        .all()
    )
    if len(snapshot_rows) != 1:
        if snapshot_rows or snapshot_id:
            raise PTG2PhysicalBindingError("PTG local candidate selector differs")
        return None
    snapshot_by_field = dict(snapshot_rows[0])
    manifest_by_field = snapshot_by_field["manifest"]
    declared = isinstance(manifest_by_field, dict) and (
        "physical_binding_contract" in manifest_by_field or "local_data_preparation" in manifest_by_field
    )
    if not declared:
        return None
    if manifest_by_field.get("physical_binding_contract") != PHYSICAL_BINDING_CONTRACT:
        raise PTG2PhysicalBindingError("PTG local candidate declaration differs")
    has_view = await session.scalar(
        text("SELECT to_regclass(:table) IS NOT NULL"), {"table": f"{configured_schema}.ptg2_prepared_physical_binding"}
    )
    if not has_view:
        raise PTG2PhysicalBindingError("PTG local preparation authority is unavailable")
    _authority, evidence, physical_binding, candidate = await local_data_physical_read_state(
        session, snapshot_by_field["snapshot_id"], is_prepared=True
    )
    return await _local_candidate_evidence_state(session, candidate, evidence, physical_binding)


async def _local_candidate_evidence_state(session, candidate, evidence, physical_binding):
    """Recompute the existing audit identity from actual isolated source rows under read locks."""
    from sqlalchemy import text

    from process.ptg_parts.ptg2_candidate_attestation import CANDIDATE_SOURCE_RECORDS_SQL, _candidate_identity

    source_records = [
        dict(source_record)
        for source_record in (
            await session.execute(
                text(CANDIDATE_SOURCE_RECORDS_SQL.format(schema=_quote_ident(physical_binding.schema_name))),
                {"snapshot_id": physical_binding.payload_snapshot_id},
            )
        )
        .mappings()
        .all()
    ]
    candidate["raw_container_sha256_values"] = [
        source_record["raw_container_sha256"] for source_record in source_records
    ]
    candidate["frozen_source_records"] = source_records
    identity = _candidate_identity(candidate, physical_binding=physical_binding)
    portable_identity_by_field = {
        key: identity_value.hex() if isinstance(identity_value, bytes) else identity_value
        for key, identity_value in identity.items()
    }
    if portable_identity_by_field != evidence["native_audit"]["identity"]:
        raise PTG2PhysicalBindingError("PTG local candidate audit identity changed")
    return {"candidate": candidate, "source_records": source_records, "physical_binding": physical_binding}


async def resolve_local_physical_binding(session, snapshot_id):
    """Return installed authority only after a fresh same-transaction native read proof."""
    from process.ptg_parts.result_archive_candidate_validation import local_data_physical_read_state

    _authority, _evidence, physical_binding, _candidate = await local_data_physical_read_state(
        session, snapshot_id, is_prepared=False
    )
    return physical_binding


def local_data_family_spec():
    """Close selected payload, provenance and coordinate data, never authority."""
    from process.ptg_parts.result_archive_candidate_preparation import local_data_family_spec as prepare_local_family

    return prepare_local_family()


def local_data_model_digest():
    """Bind the trusted heap, identity, constraint, index and relationship declarations."""
    from process.ptg_parts.result_archive_candidate_preparation import local_data_model_digest as prepared_model_digest

    return prepared_model_digest()


@dataclass(frozen=True)
class PTG2PhysicalBinding:
    """Schema-qualified payload locations and logical destination/payload editions."""

    contract: str
    snapshot_id: str
    destination_layout_key: int
    payload_snapshot_id: str
    payload_snapshot_key: int
    dataset_id: UUID
    schema_name: str
    relations: tuple[tuple[str, str], ...]
    sequences: tuple[tuple[str, str, str, str], ...]

    def __post_init__(self):
        import re

        if self.contract != PHYSICAL_BINDING_CONTRACT or not isinstance(self.dataset_id, UUID):
            raise PTG2PhysicalBindingError("PTG named binding contract or dataset differs")
        if any(
            type(coordinate) is not str
            or not coordinate
            or len(coordinate) > 96
            or coordinate != coordinate.strip()
            or any(ord(character) < 32 for character in coordinate)
            for coordinate in (self.snapshot_id, self.payload_snapshot_id)
        ):
            raise PTG2PhysicalBindingError("PTG named binding snapshot is invalid")
        if any(
            type(key) is not int or not 0 < key < 2**63
            for key in (self.destination_layout_key, self.payload_snapshot_key)
        ):
            raise PTG2PhysicalBindingError("PTG named binding edition is invalid")
        if type(self.schema_name) is not str or re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", self.schema_name) is None:
            raise PTG2PhysicalBindingError("PTG named binding schema is invalid")
        if (
            type(self.relations) is not tuple
            or any(type(pair) is not tuple or len(pair) != 2 for pair in self.relations)
            or any(type(schema) is not str or type(name) is not str for schema, name in self.relations)
            or len(self.relations) != len(set(self.relations))
            or {name for schema, name in self.relations} != set(local_data_family_spec().table_names)
            or any(schema != self.schema_name for schema, _name in self.relations)
        ):
            raise PTG2PhysicalBindingError("PTG named binding complete model inventory differs")
        expected_sequences = {
            (f"{model.__tablename__}_{column.name}_seq", model.__tablename__, column.name)
            for model in local_data_family_spec().model_types
            for column in model.__table__.columns
            if column.identity is not None
        }
        if (
            type(self.sequences) is not tuple
            or any(type(entry) is not tuple or len(entry) != 4 for entry in self.sequences)
            or any(any(type(coordinate) is not str for coordinate in entry) for entry in self.sequences)
            or len(self.sequences) != len(set(self.sequences))
            or {(name, table, column) for schema, name, table, column in self.sequences} != expected_sequences
            or any(schema != self.schema_name for schema, _name, _table, _column in self.sequences)
        ):
            raise PTG2PhysicalBindingError("PTG named binding sequence inventory differs")

    def relation(self, table):
        """Resolve one member of the complete fixed family by its recorded name."""
        if table not in local_data_family_spec().table_names:
            raise PTG2PhysicalBindingError("PTG named binding model is outside the complete family")
        return f"{_quote_ident(self.schema_name)}.{_quote_ident(table)}"


def require_legacy_physical_resolution(declaration):
    """Keep new or unknown physical bindings closed until publisher integration."""
    if declaration is not None:
        raise PTG2PhysicalBindingError("PTG snapshot-local physical binding is not available")


_LOCAL_AUDIT_IDENTITY_FIELDS = frozenset(
    {
        "snapshot_key",
        "storage_generation",
        "source_key",
        "plan_id",
        "plan_market_type",
        "coverage_scope_id",
        "source_set_digest",
        "ordered_source_ordinal_digest",
        "source_witness_manifest",
        "audit_sample_public",
        "source_witness_digest",
        "audit_sample_digest",
        "provider_identifier_quarantine",
        "provider_identifier_quarantine_evidence",
    }
)


def named_local_validation(evidence):
    """Project retained content evidence while discarding historical catalog assertions."""
    from copy import deepcopy

    fields = {
        "contract",
        "metadata_sha256",
        "ownership",
        "initialization",
        "data",
        "control_sha256",
        "native_audit",
        "activation_evidence",
    }
    if type(evidence) is not dict or set(evidence) not in (fields, fields | {"catalog_sha256"}):
        raise PTG2PhysicalBindingError("PTG local validation fields differ")
    normalized_by_field = {name: deepcopy(evidence[name]) for name in fields}
    normalized_by_field["ownership"] = _named_local_ownership(evidence["ownership"])
    audit = normalized_by_field["native_audit"]
    if (
        type(audit) is not dict
        or set(audit)
        not in ({"contract", "model_sha256", "identity"}, {"contract", "model_sha256", "catalog_sha256", "identity"})
        or type(audit.get("identity")) is not dict
        or set(audit["identity"]) != _LOCAL_AUDIT_IDENTITY_FIELDS
    ):
        raise PTG2PhysicalBindingError("PTG local source and selection evidence differs")
    audit.pop("catalog_sha256", None)
    return normalized_by_field


def _named_local_ownership(document):
    """Retain the existing logical dataset and complete named model family."""
    fields = {"importer_id", "dataset_id", "schema_name", "relations", "sequences"}
    legacy_fields = {"importer_id", "dataset_id", "schema_name", "schema_oid", "relation_oids", "sequence_oids"}
    if type(document) is not dict or set(document) not in (fields, legacy_fields):
        raise PTG2PhysicalBindingError("PTG local named storage fields differ")
    if set(document) == fields:
        return {field: document[field] for field in fields}
    schema = document["schema_name"]
    relations, sequences = document["relation_oids"], document["sequence_oids"]
    if (
        not isinstance(relations, (list, tuple))
        or not isinstance(sequences, (list, tuple))
        or any(not isinstance(pair, (list, tuple)) or len(pair) != 2 for pair in relations)
        or any(not isinstance(entry, (list, tuple)) or len(entry) != 4 for entry in sequences)
    ):
        raise PTG2PhysicalBindingError("PTG local retained model names differ")
    return {
        **{field: document[field] for field in ("importer_id", "dataset_id", "schema_name")},
        "relations": [{"schema": schema, "name": pair[0]} for pair in relations],
        "sequences": [
            {"schema": schema, "name": entry[0], "owner_table": entry[2], "owner_column": entry[3]}
            for entry in sequences
        ],
    }


def named_local_binding(evidence, snapshot_id):
    """Decode a complete named payload and its separate destination edition."""
    ownership = evidence["ownership"]
    if ownership["importer_id"] != local_data_family_spec().importer_id:
        raise PTG2PhysicalBindingError("PTG local model family differs")
    relations, sequences = ownership["relations"], ownership["sequences"]
    if (
        not isinstance(relations, list)
        or not isinstance(sequences, list)
        or any(type(pair) is not dict or set(pair) != {"schema", "name"} for pair in relations)
        or any(
            type(entry) is not dict or set(entry) != {"schema", "name", "owner_table", "owner_column"}
            for entry in sequences
        )
    ):
        raise PTG2PhysicalBindingError("PTG local complete named family differs")
    return PTG2PhysicalBinding(
        PHYSICAL_BINDING_CONTRACT,
        snapshot_id,
        evidence["initialization"]["destination_layout_key"],
        evidence["data"]["payload_snapshot_id"],
        evidence["data"]["payload_snapshot_key"],
        UUID(ownership["dataset_id"]),
        ownership["schema_name"],
        tuple((pair["schema"], pair["name"]) for pair in sorted(relations, key=lambda pair: pair["name"])),
        tuple(
            (entry["schema"], entry["name"], entry["owner_table"], entry["owner_column"])
            for entry in sorted(sequences, key=lambda entry: entry["name"])
        ),
    )


def named_local_inventory(relations, sequences, schema):
    """Accept only complete known name records and known historical projections."""
    from copy import deepcopy

    if not isinstance(relations, list) or not isinstance(sequences, list):
        raise PTG2PhysicalBindingError("PTG local ordered model names differ")
    named_relations, named_sequences = [], []
    for ordinal, entry_by_field in enumerate(relations):
        if (
            type(entry_by_field) is not dict
            or type(entry_by_field.get("ordinal")) is not int
            or entry_by_field["ordinal"] != ordinal
        ):
            raise PTG2PhysicalBindingError("PTG local relation order differs")
        if set(entry_by_field) == {"ordinal", "table_name", "relation_oid"}:
            entry_by_field = {"ordinal": ordinal, "schema_name": schema, "relation_name": entry_by_field["table_name"]}
        if set(entry_by_field) != {"ordinal", "schema_name", "relation_name"}:
            raise PTG2PhysicalBindingError("PTG local relation name fields differ")
        named_relations.append(deepcopy(entry_by_field))
    for ordinal, entry_by_field in enumerate(sequences):
        if (
            type(entry_by_field) is not dict
            or type(entry_by_field.get("ordinal")) is not int
            or entry_by_field["ordinal"] != ordinal
        ):
            raise PTG2PhysicalBindingError("PTG local sequence order differs")
        if set(entry_by_field) == {"ordinal", "sequence_name", "sequence_oid", "owner_table", "owner_column"}:
            entry_by_field = {
                **{
                    field: entry_by_field[field]
                    for field in ("ordinal", "sequence_name", "owner_table", "owner_column")
                },
                "schema_name": schema,
            }
        if set(entry_by_field) != {"ordinal", "schema_name", "sequence_name", "owner_table", "owner_column"}:
            raise PTG2PhysicalBindingError("PTG local sequence name fields differ")
        named_sequences.append(deepcopy(entry_by_field))
    return named_relations, named_sequences


def _named_publication_fields(publication_by_field, binding):
    """Accept the named receipt or project its exact historical field set."""
    fields = {
        "contract",
        "destination_snapshot_id",
        "destination_layout_key",
        "payload_snapshot_id",
        "payload_snapshot_key",
        "dataset_id",
        "schema_name",
        "relations",
        "sequences",
        "destination_activation",
        "published_control_sha256",
    }
    legacy = fields - {"schema_name", "relations", "sequences"} | {
        "schema_oid",
        "owner_oid",
        "relation_oids",
        "sequence_oids",
    }
    if type(publication_by_field) is not dict or set(publication_by_field) not in (fields, legacy):
        raise PTG2PhysicalBindingError("PTG local publication fields differ")
    if set(publication_by_field) == legacy:
        projected = _named_local_ownership(
            {
                "importer_id": local_data_family_spec().importer_id,
                "dataset_id": publication_by_field["dataset_id"],
                "schema_name": binding.schema_name,
                "schema_oid": publication_by_field["schema_oid"],
                "relation_oids": publication_by_field["relation_oids"],
                "sequence_oids": publication_by_field["sequence_oids"],
            }
        )
        publication_by_field = {
            **{field: publication_by_field[field] for field in fields - {"schema_name", "relations", "sequences"}},
            **{field: projected[field] for field in ("schema_name", "relations", "sequences")},
        }
    return publication_by_field


def named_local_publication(publication_by_field, binding):
    """Validate a logical publication against its complete named payload family."""
    publication_by_field = _named_publication_fields(publication_by_field, binding)
    expected_by_field = {
        "contract": binding.contract,
        "destination_snapshot_id": binding.snapshot_id,
        "destination_layout_key": binding.destination_layout_key,
        "payload_snapshot_id": binding.payload_snapshot_id,
        "payload_snapshot_key": binding.payload_snapshot_key,
        "dataset_id": str(binding.dataset_id),
        "schema_name": binding.schema_name,
        "relations": [{"schema": schema, "name": name} for schema, name in binding.relations],
        "sequences": [
            {"schema": schema, "name": name, "owner_table": table, "owner_column": column}
            for schema, name, table, column in binding.sequences
        ],
    }
    for field in ("relations", "sequences"):
        entries = publication_by_field[field]
        if not isinstance(entries, list) or any(type(entry) is not dict or "name" not in entry for entry in entries):
            raise PTG2PhysicalBindingError("PTG local publication model names differ")
        publication_by_field = {**publication_by_field, field: sorted(entries, key=lambda entry: entry["name"])}
    activation = publication_by_field["destination_activation"]
    if type(activation) is not dict or set(activation) != {
        "source_key",
        "snapshot_id",
        "previous_snapshot_id",
        "activated_at",
        "audit_report_digest",
    }:
        raise PTG2PhysicalBindingError("PTG local activation fields differ")
    if any(publication_by_field[field] != coordinate for field, coordinate in expected_by_field.items()):
        raise PTG2PhysicalBindingError("PTG local logical publication differs")
    return publication_by_field


def named_local_read_row(row, *, is_prepared):
    """Normalize only the maintained named projection or its exact retained predecessor."""
    from copy import deepcopy

    fields = set(_local_read_view_columns(is_prepared))
    legacy = fields | {"owner_oid", "validation_sha256", "inventory_sha256"}
    if not is_prepared:
        legacy.add("registration_sha256")
    if type(row) is not dict or set(row) not in (fields, legacy):
        raise PTG2PhysicalBindingError("PTG local read projection fields differ")
    normalized_by_field = {field: deepcopy(row[field]) for field in fields}
    evidence = normalized_by_field["native_validation"] = named_local_validation(row["native_validation"])
    binding = named_local_binding(evidence, row["destination_snapshot_id"])
    relations, sequences = named_local_inventory(
        row["relation_inventory"], row["sequence_inventory"], binding.schema_name
    )
    if sorted((entry["schema_name"], entry["relation_name"]) for entry in relations) != list(
        binding.relations
    ) or sorted(
        (entry["schema_name"], entry["sequence_name"], entry["owner_table"], entry["owner_column"])
        for entry in sequences
    ) != list(binding.sequences):
        raise PTG2PhysicalBindingError("PTG local recorded model names differ")
    normalized_by_field.update(relation_inventory=relations, sequence_inventory=sequences)
    if not is_prepared:
        normalized_by_field["generation_inventory"] = _named_generation_inventory(row["generation_inventory"], binding)
        normalized_by_field["native_publication"] = named_local_publication(row["native_publication"], binding)
    return normalized_by_field


def _named_generation_inventory(document, binding):
    """Read portable retained names while ignoring known discarded physical fields."""
    if type(document) is not dict or set(document) not in ({"relations"}, {"database_oid", "relations"}):
        raise PTG2PhysicalBindingError("PTG local generation names differ")
    relations = document["relations"]
    legacy_fields = {
        "schema_name",
        "relation_name",
        "relation_oid",
        "schema_oid",
        "schema_owner_oid",
        "owner_oid",
        "relfilenode",
    }
    if not isinstance(relations, list) or any(
        type(entry) is not dict
        or not {"schema_name", "relation_name"} <= set(entry)
        or set(entry) not in ({"schema_name", "relation_name"}, legacy_fields)
        for entry in relations
    ):
        raise PTG2PhysicalBindingError("PTG local generation name fields differ")
    names = [(entry["schema_name"], entry["relation_name"]) for entry in relations]
    if len(names) != len(binding.relations) or set(names) != set(binding.relations):
        raise PTG2PhysicalBindingError("PTG local generation model family differs")
    return {"relations": [{"schema_name": schema, "relation_name": name} for schema, name in sorted(names)]}
