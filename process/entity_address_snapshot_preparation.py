# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate frozen address candidates separately from bounded native publication."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from contextlib import nullcontext

from sqlalchemy import text

from process import entity_address_snapshot_alias as alias
from process import entity_address_snapshot_destination as destination
from process import entity_address_snapshot_serving as serving

EntityAddressSnapshotDestinationError = destination.EntityAddressSnapshotDestinationError
CONTRACT = "entity_address_archive_preparation.postgres.v1"
SET_CONTRACT = "entity_address_archive_preparation.postgres.v2"
NAMED_CONTRACT = "entity_address_archive_preparation.postgres.v3"
_EVIDENCE_SEQUENCE = "entity_address_evidence_evidence_id_seq"


async def prepare_private_entity_address_archive_destination(
    session,
    *,
    owner,
    semantic_receipt,
    source_alias_receipt,
    destination,
):
    """Prepare provisional contents while preserving the original private inventory."""
    from process import entity_address_snapshot_destination as native_destination
    from process.entity_address_snapshot_receipt import CONTRACT as archive_contract

    _require(
        native_destination.validate_entity_address_archive_receipt(semantic_receipt).contract != archive_contract,
        "v2 requires authenticated publisher load",
    )

    _require(
        isinstance(destination, Mapping)
        and {"db_schema", "import_date"} <= set(destination)
        and set(destination) <= {"db_schema", "import_date", "source_serving_generation", "dependency_bindings"},
        "destination is invalid",
    )
    preparation_by_field = {
        **destination,
        "owner": owner,
        "semantic_receipt": semantic_receipt,
        "source_alias_receipt": source_alias_receipt,
    }
    if preparation_by_field.get("dependency_bindings") is not None:
        preparation_by_field["dependency_bindings"] = native_destination._geo_binding_names(
            preparation_by_field["db_schema"], preparation_by_field["dependency_bindings"]
        )
    native_destination._require_caller_transaction(session)
    async with native_destination.db.bind_existing_session(session):
        return await native_destination._prepare_bound_destination(
            session, preparation_by_field, preserve_private_stage=True
        )


def _require(condition, reason):
    if not condition:
        raise EntityAddressSnapshotDestinationError("entity-address protected preparation " + reason)


def _digest(value):
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as error:
        raise EntityAddressSnapshotDestinationError(
            "entity-address protected preparation payload is invalid"
        ) from error
    return hashlib.sha256(encoded).hexdigest()


def _owner_relation_names(owner):
    return owner.relation_names


def _logical_inventory(inventory, owner):
    """Project only declared schema/table/sequence names; compatibility tokens never locate data."""
    _require(isinstance(inventory, Mapping) and inventory.get("schema_name") == owner.schema_name, "inventory differs")
    relations = inventory.get("relations")
    _require(
        isinstance(relations, (list, tuple)) and all(isinstance(entry, Mapping) for entry in relations),
        "inventory differs",
    )
    names = tuple(entry.get("table_name", entry.get("relation_name")) for entry in relations)
    _require(
        names == _owner_relation_names(owner)
        and all(
            entry.get("schema_name", owner.schema_name) == owner.schema_name
            and entry.get("table_name", name) == name
            and entry.get("relation_name", name) == name
            for entry, name in zip(relations, names, strict=True)
        ),
        "inventory differs",
    )
    sequences = inventory.get("sequences", [])
    sequence_by_field = {
        "sequence_name": _EVIDENCE_SEQUENCE,
        "owner_table": "entity_address_evidence",
        "owner_column": "evidence_id",
    }
    _require(
        isinstance(sequences, (list, tuple))
        and len(sequences) == 1
        and isinstance(sequences[0], Mapping)
        and {field: sequences[0].get(field) for field in sequence_by_field} == sequence_by_field,
        "sequence differs",
    )
    return {
        "schema_name": owner.schema_name,
        "relations": [{"table_name": name} for name in names],
        "sequences": [sequence_by_field],
    }


def _require_proof_inventory(proof, owner):
    """Check retained raw integrity before projecting the operation's logical inventory."""
    inventory = proof.get("inventory")
    _require(_digest(inventory) == proof.get("inventory_sha256"), "inventory differs")
    return _logical_inventory(inventory, owner)


async def _publisher_authority(session):
    """Require the caller transaction; PostgreSQL grants govern its actual reads and DDL."""
    destination._require_caller_transaction(session)


async def _require_alias_authority(session, db_schema, owner_oid=None):
    """Read the declared alias state without interpreting old role or catalog tokens."""
    return await alias._alias_state(session, serving._schema_name(db_schema))


async def require_entity_address_archive_publisher(session, *, db_schema):
    """Check the transaction and actual native alias read before publication preparation."""
    await _publisher_authority(session)
    await _require_alias_authority(session, db_schema)


async def _authenticated_preparation(session, stored, preparation, validation, authenticate_preparation):
    """Use the existing operation callback and logical inventory before touching candidate names."""
    await _publisher_authority(session)
    _require(callable(authenticate_preparation), "authentication is unavailable")
    proof = await authenticate_preparation(session, preparation, validation)
    state = "frozen" if validation is None else "validated"
    _require(isinstance(proof, Mapping) and proof.get("state") == state, "state differs")
    if validation is not None:
        _require(
            isinstance(proof.get("validation"), Mapping) and proof["validation"].get("evidence") == validation,
            "validation differs",
        )
    destination._validated_destination_metadata(stored)
    owner = destination.validate_entity_address_archive_stage_ownership(stored["restored"]["ownership"])
    _require_proof_inventory(proof, owner)
    return proof, owner


async def _protected_catalog(session, proof, owner, *, read_only=False):
    """Hold and validate the closed named candidate family using its actual native layout."""
    _require_proof_inventory(proof, owner)
    await destination.restore._lock_owned_restore_relations(session, owner, read_only=read_only)
    await destination.verify_entity_address_archive_stage_ownership(session, owner=owner)
    return _logical_inventory(proof["inventory"], owner)


async def _move_to_destination(session, stored, owner):
    schema, _date, names = destination.restore._stage_plan(
        db_schema=stored["restored"]["db_schema"], import_date=stored["restored"]["import_date"]
    )
    moved = await destination.restore._move_owned_relations(session, owner=owner, db_schema=schema, stage_names=names)
    expected_relations = tuple(sorted((schema, names[name]) for name in _owner_relation_names(owner) if name in names))
    _require(moved == expected_relations, "relocation differs")
    return schema, names


async def validate_entity_address_archive_preparation(session, *, stored, preparation, authenticate_preparation):
    """Scan frozen named contents with native read locks; validation performs no candidate DDL."""
    from process.entity_address_snapshot_receipt import CONTRACT as archive_contract

    _require(stored.get("contract") != archive_contract, "v2 requires authenticated publisher load")
    proof, owner = await _authenticated_preparation(session, stored, preparation, None, authenticate_preparation)
    await _protected_catalog(session, proof, owner, read_only=True)
    async with destination.db.bind_existing_session(session):
        await _validate_private_candidate_contents(session, stored, owner)
    return {
        "contract": NAMED_CONTRACT,
        "stored_sha256": _digest(stored),
        "inventory": _logical_inventory(proof["inventory"], owner),
        "alias": dict(stored["destination_alias_receipt"]),
    }


async def _validate_private_candidate_contents(session, stored, owner):
    """Recompute all schema/row/input identities at private names without relocating a frozen heap."""
    from process import entity_address_snapshot_receipt as receipts

    source_alias, destination_alias, remap = destination._validated_destination_metadata(stored)
    schema = stored["restored"]["db_schema"]
    _source, current_alias = await destination._destination_alias_binding(
        session, db_schema=schema, source_alias_receipt=source_alias, provisional=True
    )
    _require(current_alias == destination_alias, "destination alias generation changed")
    _schema, _date, stage_names = destination.restore._stage_plan(
        db_schema=schema, import_date=stored["restored"]["import_date"]
    )
    expected = receipts.validate_entity_address_stage_integrity_receipt(
        stored["restored"]["stage_integrity"], stage_table_names=stage_names
    )
    async with destination._preserve_receipt_settings():
        actual = await receipts.capture_entity_address_stage_integrity_receipt(
            session,
            schema_name=owner.schema_name,
            stage_table_names={model.__tablename__: model.__tablename__ for model in receipts._models()},
            contract=expected.contract,
            read_only=True,
        )
        counts = await destination._base_version_counts(
            session,
            schema_name=owner.schema_name,
            table_name=destination.entity_address_unified.EntityAddressUnified.__tablename__,
            source_alias_version=destination.entity_address_unified.ALIAS_BASE_ADDRESS_VERSION_PREFIX
            + str(destination_alias.local_generation),
        )
    _require_frozen_content_identity(actual, expected)
    _require(
        counts == (remap.null_rows_preserved, remap.plain_base_rows_preserved, remap.alias_rows_bound, 0),
        "base_address_version evidence differs",
    )


def _require_frozen_content_identity(actual, expected):
    """Names may move during preparation; every model schema, row and normalized input remains exact."""

    def table_contents(receipt):
        """Retain every validated model schema, count and row digest after its name-only move."""
        return tuple(
            (table.model_name, table.schema_sha256, table.row_count, table.row_sha256) for table in receipt.tables
        )

    _require(
        actual.main_input_sha256 == expected.main_input_sha256 and table_contents(actual) == table_contents(expected),
        "frozen candidate contents differ",
    )


def _require_seal(stored, proof, validation):
    """Validate saved metadata integrity before comparing logical publication inputs."""
    _require(
        isinstance(validation, Mapping) and validation.get("contract") in (CONTRACT, NAMED_CONTRACT),
        "validation is invalid",
    )
    _require(
        validation.get("stored_sha256") == _digest(stored)
        and validation.get("alias") == stored["destination_alias_receipt"],
        "validation differs",
    )
    if validation["contract"] == NAMED_CONTRACT:
        _require(set(validation) == {"contract", "stored_sha256", "inventory", "alias"}, "validation is invalid")
        owner = destination.validate_entity_address_archive_stage_ownership(stored["restored"]["ownership"])
        _require(validation.get("inventory") == _logical_inventory(proof["inventory"], owner), "inventory differs")


async def _require_alias_fence(session, schema, proof, validation):
    """Compare actual alias counters under native locks, independent of old catalog fingerprints."""
    await alias._lock_alias_relations(session, schema)
    expected = alias.validate_entity_address_alias_semantic_receipt(validation["alias"])
    current = await alias._alias_state(session, schema)
    _require(
        current == (expected.alias_schema_version, expected.active_ruleset_version, expected.local_generation),
        "alias generation changed",
    )


def _prepared_adoption(stored):
    schema, date, _names, stage_relations, _integrity, context, validation = (
        destination.restore._validated_rehydration_state(stored["restored"])
    )
    stage, support, swaps, patches, relations, required = destination.adoption._prepared_full_result_stage(
        db_schema=schema, import_date=date
    )
    context.setdefault(
        "stage_relations",
        [{"schema_name": schema_name, "relation_name": name} for schema_name, name in stage_relations],
    )
    context.setdefault(
        "geo_assurance_candidate_relation", {"schema_name": schema, "relation_name": stage.__tablename__}
    )
    return destination.adoption.PreparedEntityAddressSnapshotAdoption(
        db_schema=schema,
        stage_cls=stage,
        support_stage_class_map=support,
        swaps=swaps,
        patch_statements=patches,
        relation_names=relations,
        required_names=required,
        context=context,
        publish_validation=validation,
    )


_CMS_RECEIPT_TABLE = "provider_directory_cms_serving_receipt"


async def _lock_publication_state(session, schema, owner_oid=None):
    """Lock the fixed native state names and preserve the composite CMS publication boundary."""
    schema = serving._schema_name(schema)
    names = (destination.geo_projection.GEO_ASSURANCE_STATE_TABLE, serving.result_generation.TABLE_NAME)
    await session.execute(
        text(
            "LOCK TABLE " + ", ".join(f'"{schema}"."{name}"' for name in names) + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
        )
    )
    state_records = (
        (
            await session.execute(
                text(
                    "SELECT c.relname AS name,c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition "
                    "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AS supported FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema AND c.relname=ANY(:names)"
                ),
                {"schema": schema, "names": list(names)},
            )
        )
        .mappings()
        .all()
    )
    _require(
        {state_record["name"] for state_record in state_records} == set(names)
        and all(state_record["supported"] is True for state_record in state_records),
        "publication state layout differs",
    )
    receipt_exists = await session.scalar(
        text("SELECT to_regclass(:name) IS NOT NULL"), {"name": f'"{schema}"."{_CMS_RECEIPT_TABLE}"'}
    )
    if receipt_exists:
        await session.execute(text(f'LOCK TABLE ONLY "{schema}"."{_CMS_RECEIPT_TABLE}" IN SHARE MODE NOWAIT'))
        populated = await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM "{schema}"."{_CMS_RECEIPT_TABLE}")'))
        _require(populated is False, "populated CMS receipt history requires composite publication")


async def _restore_builder_owner(session, proof, owner, schema, names):
    """Remove only the emptied fixture-owned namespace; native grants govern stage DDL."""
    await destination.restore._drop_empty_owned_schema(session, owner)


async def activate_validated_entity_address_archive_destination(
    session,
    *,
    stored,
    validation,
    preparation,
    expected_incumbent,
    callbacks,
    authenticate_preparation,
):
    """Publish an authenticated frozen candidate without rescanning candidate or alias rows."""
    if (
        isinstance(validation, Mapping)
        and validation.get("contract") in (SET_CONTRACT, NAMED_CONTRACT)
        and "prepared" in validation
    ):
        return await _activate_set_validated_destination(
            session,
            stored=stored,
            validation=validation,
            preparation=preparation,
            expected_incumbent=expected_incumbent,
            callbacks=callbacks,
            authenticate_preparation=authenticate_preparation,
        )
    proof, owner = await _authenticated_preparation(session, stored, preparation, validation, authenticate_preparation)
    _require_seal(stored, proof, validation)
    schema = stored["restored"]["db_schema"]
    await serving.require_entity_address_receive_incumbent(session, schema_name=schema, expected=expected_incumbent)
    await _require_alias_fence(session, schema, proof, validation)
    await _lock_publication_state(session, schema)
    await _protected_catalog(session, proof, owner, read_only=True)
    async with destination.db.bind_existing_session(session):
        schema, names = await _move_to_destination(session, stored, owner)
        prepared = _prepared_adoption(stored)
        dependency_bindings = await _resolve_dependency_bindings(session, schema, prepared)
        expected_geo = destination._logical_geo_preparation(
            destination._validated_geo_preparation(stored["geo_assurance"], db_schema=schema),
            prepared=prepared,
            dependency_bindings=dependency_bindings,
        )
        actual_geo = await destination._capture_geo_preparation(
            session,
            db_schema=schema,
            stage_relation_name=prepared.stage_cls.__tablename__,
            projected_rows=expected_geo.projected_rows,
            dependency_bindings=dependency_bindings,
        )
        _require(actual_geo == expected_geo, "geo assurance changed")
        await _restore_builder_owner(session, proof, owner, schema, names)
        published = await destination.adoption.adopt_prepared_entity_address_snapshot(prepared, callbacks=callbacks)
        return await _publication_receipt(session, schema, owner, prepared, published)


async def _resolve_dependency_bindings(session, schema, prepared):
    """Resolve grouped dependency renames and update the same candidate with compare-and-swap."""
    from process.entity_address_dependency_bindings import resolve_prepared_bindings

    expected_bindings = prepared.context.get("dependency_bindings")
    dependency_bindings = await resolve_prepared_bindings(session, schema, expected_bindings)
    if dependency_bindings != expected_bindings:
        changed = await session.scalar(
            text(
                f'UPDATE "{schema}".entity_address_geo_assurance_state '
                "SET candidate_dependency_bindings=CAST(:bindings AS jsonb) "
                "WHERE singleton IS TRUE AND candidate_dependency_bindings=CAST(:expected AS jsonb) RETURNING singleton"
            ),
            {"bindings": json.dumps(dependency_bindings), "expected": json.dumps(expected_bindings)},
        )
        _require(changed is True, "dependency bindings changed")
    return dependency_bindings


def _loaded_input(owner, semantic_receipt, source_alias_receipt, destination_payload):
    return {
        "ownership": owner.as_dict(),
        "semantic_receipt": semantic_receipt.as_dict(),
        "source_alias_receipt": source_alias_receipt.as_dict(),
        "destination": dict(destination_payload),
    }


async def prepare_loaded_entity_address_archive_destination(
    session,
    *,
    owner,
    semantic_receipt,
    source_alias_receipt,
    destination,
    authenticate_preparation,
    publisher_selected_inputs=None,
    authenticate_selected_mrf=None,
):
    """Transform authenticated handed-off heaps before their native freeze and immutable validation."""
    from process import entity_address_snapshot_destination as native
    from process.entity_address_dependency_bindings import (
        lock_publisher_selected_inputs,
        publisher_selected_input_names,
    )

    owner, semantic, source_alias = _validated_loaded_input(owner, semantic_receipt, source_alias_receipt, destination)
    selected = (
        None
        if publisher_selected_inputs is None
        else publisher_selected_input_names(destination["db_schema"], publisher_selected_inputs)
    )
    _require(
        authenticate_selected_mrf is None or (selected is not None and callable(authenticate_selected_mrf)),
        "selected MRF authentication differs",
    )
    await _publisher_authority(session)
    _require(callable(authenticate_preparation), "authentication is unavailable")
    proof = await authenticate_preparation(session)
    _require_loaded_inventory(proof, owner)
    schema, *_ = native.restore._stage_plan(db_schema=destination["db_schema"], import_date=destination["import_date"])
    _require(schema != owner.schema_name, "destination differs")
    await _require_alias_authority(session, schema)
    await _protected_catalog(session, proof, owner)
    if selected is not None:
        selected = await lock_publisher_selected_inputs(session, schema, selected)
    destination_alias = await alias.capture_entity_address_alias_authority_receipt(session, schema_name=schema)
    async with native.db.bind_existing_session(session):
        prepared = await _prepare_loaded_sets(
            session,
            owner,
            semantic,
            source_alias,
            destination_alias,
            destination,
            selected,
            authenticate_selected_mrf,
        )
    return await _loaded_validation(
        session,
        prepared,
        proof,
        owner,
        _loaded_input(owner, semantic, source_alias, destination),
        destination_alias,
        selected,
    )


async def _loaded_validation(session, prepared, proof, owner, original_input, destination_alias, selected):
    """Record full validated contents and logical names without physical-identity fingerprints."""
    stored = prepared.as_dict()
    await _protected_catalog(session, proof, owner, read_only=True)
    validation_by_field = {
        "contract": NAMED_CONTRACT,
        "prepared": stored,
        "input_sha256": _digest(original_input),
        "stored_sha256": _digest(stored),
        "inventory": _logical_inventory(proof["inventory"], owner),
        "contents": await _loaded_candidate_contents(session, owner),
        "alias": destination_alias.as_dict(),
    }
    if selected is not None:
        validation_by_field["publisher_selected_inputs_sha256"] = _digest(selected)
    return validation_by_field


async def _loaded_candidate_contents(session, owner):
    """Hash every row in the named family with the existing bounded canonical codec."""
    from process import entity_address_snapshot_receipt as receipts

    contents = []
    async with session.begin_nested() as settings:
        await receipts._normalize_receipt_session(session, owner.schema_name)
        for name in owner.relation_names:
            rows, digest = await receipts._row_identity(session, owner.schema_name, name)
            contents.append({"relation_name": name, "rows": rows, "sha256": digest})
        # Rolling back this read-only savepoint restores every normalized caller setting.
        await settings.rollback()
    return contents


def _validated_loaded_contents(value, owner):
    """Require complete ordered named commitments, including the alias authority relation."""
    from process import entity_address_snapshot_receipt as receipts

    names = owner.relation_names
    _require(isinstance(value, list) and len(value) == len(names) == 8, "frozen candidate contents differ")
    for entry, name in zip(value, names, strict=True):
        _require(
            isinstance(entry, dict)
            and set(entry) == {"relation_name", "rows", "sha256"}
            and entry["relation_name"] == name
            and type(entry["rows"]) is int
            and entry["rows"] >= 0
            and isinstance(entry["sha256"], str)
            and receipts._SHA256.fullmatch(entry["sha256"]) is not None,
            "frozen candidate contents differ",
        )
    return value


def _validated_loaded_input(owner, semantic_receipt, source_alias_receipt, destination_options):
    """Validate approved immutable input without replacing its dependency bounds."""
    from process.entity_address_snapshot_receipt import CONTRACT as archive_contract

    owner = destination.validate_entity_address_archive_stage_ownership(owner)
    semantic = destination.validate_entity_address_archive_receipt(semantic_receipt)
    source_alias = alias.validate_entity_address_alias_semantic_receipt(source_alias_receipt)
    _require(semantic.contract == archive_contract and source_alias.contract == alias.SET_CONTRACT, "contracts differ")
    alias_table = next(entry for entry in semantic.tables if entry.table_name == alias.AUTHORITY_TABLE)
    _require(alias_table.row_count == source_alias.active_alias_count, "source alias accounting differs")
    _require(
        isinstance(destination_options, Mapping)
        and {"db_schema", "import_date"} <= set(destination_options)
        and set(destination_options)
        <= {"db_schema", "import_date", "source_serving_generation", "dependency_bindings"},
        "destination differs",
    )
    return owner, semantic, source_alias


def _require_loaded_inventory(proof, owner, owner_oid=None):
    """Require the authenticated mutable handoff before local transforms and native freeze."""
    _require(isinstance(proof, Mapping) and proof.get("state") == "handed_off", "handed-off operation differs")
    _require_proof_inventory(proof, owner)
    names = _owner_relation_names(owner)
    _require(len(names) == 8 and alias.AUTHORITY_TABLE in names, "inventory differs")


async def _prepare_loaded_sets(
    session,
    owner,
    semantic,
    source_alias,
    destination_alias,
    destination_options,
    selected=None,
    authenticate_selected_mrf=None,
):
    """Apply documented local transforms, complete indexes and validate the isolated candidate."""
    from process import entity_address_snapshot_destination as native
    from process.entity_address_snapshot_receipt import CONTRACT as archive_contract
    from process.entity_address_snapshot_receipt import STAGE_INTEGRITY_CONTRACT

    schema, date = destination_options["db_schema"], destination_options["import_date"]
    remap, geo, bindings, stage_relations, names = await _complete_loaded_heaps(
        session, owner, semantic, source_alias, destination_alias, destination_options, selected
    )
    prepared = await _audit_loaded_adoption(session, destination_options, selected, authenticate_selected_mrf)
    prepared.context.update(
        snapshot_contract=archive_contract,
        stage_relations=[{"schema_name": namespace, "relation_name": name} for namespace, name in stage_relations],
        geo_assurance_candidate_relation={"schema_name": schema, "relation_name": prepared.stage_cls.__tablename__},
    )
    if bindings is not None:
        prepared.context["dependency_bindings"] = bindings
    if selected is not None:
        prepared.context["publisher_selected_inputs"] = selected
    integrity = await native.capture_entity_address_stage_integrity_receipt(
        session, schema_name=schema, stage_table_names=names, contract=STAGE_INTEGRITY_CONTRACT
    )
    restored = native._prepared_restore_receipt(
        validated_owner=owner,
        normalized_date=date,
        stage_relations=stage_relations,
        post_remap_receipt=semantic,
        stage_integrity=integrity,
        prepared=prepared,
    )
    await native.restore._return_owned_relations(session, owner=owner, db_schema=schema, stage_names=names)
    return native.PreparedEntityAddressSnapshotDestination(
        restored, semantic, source_alias, destination_alias, remap, geo
    )


async def _complete_loaded_heaps(
    session, owner, semantic, source_alias, destination_alias, destination_options, selected
):
    """Derive model stage names, transform heaps, and complete all indexes before set audit."""
    native = destination
    schema, date, names = native.restore._stage_plan(
        db_schema=destination_options["db_schema"], import_date=destination_options["import_date"]
    )
    remap, _ = await native._remap_base_versions(
        session,
        schema_name=owner.schema_name,
        source_alias=source_alias,
        destination_alias=destination_alias,
        pre_remap_receipt=semantic,
    )
    await native.restore._reset_restored_evidence_sequence(session, owner)
    stage_relations = await native.restore._move_owned_relations(
        session, owner=owner, db_schema=schema, stage_names=names, heaps=True
    )
    projection_options_by_field = dict(destination_options)
    if selected is not None:
        projection_options_by_field["dependency_bindings"] = selected["dependency_bindings"]
    geo, bindings = await _project_loaded_geo(
        session, semantic, projection_options_by_field, names, stage_relations, selected
    )
    await native.restore._return_owned_relations(session, owner=owner, db_schema=schema, stage_names=names)
    await native.restore.complete_entity_address_archive_restore(
        session, owner=owner, db_schema=schema, import_date=date
    )
    await native.restore._actual_receipt(session, schema_name=owner.schema_name, expected=semantic)
    await alias.require_matching_entity_address_alias_authority(
        session, authority_schema=owner.schema_name, alias_schema=schema
    )
    await native.restore._move_owned_relations(session, owner=owner, db_schema=schema, stage_names=names)
    return remap, geo, bindings, stage_relations, names


async def _audit_loaded_adoption(session, destination_options, selected, authenticate_selected_mrf):
    """Audit future archive coverage only after the isolated candidate's indexes are complete."""
    native = destination
    schema, date = destination_options["db_schema"], destination_options["import_date"]
    if authenticate_selected_mrf is None:
        archive_scope = nullcontext(None)
    else:
        archive_scope = native.restore.family_archive.selected_canonical_archive(
            session,
            schema_name=schema,
            owner_oid=None,
            selected_relations={
                "npi": selected["dependency_bindings"][f"{schema}.npi_address"],
                "mrf": selected["dependency_bindings"][f"{schema}.mrf_address"],
            },
            authenticate_inventory=authenticate_selected_mrf,
        )
    async with archive_scope as archive_relation:
        return await native.adoption.prepare_completed_entity_address_snapshot_adoption(
            db_schema=schema,
            import_date=date,
            preserve_unversioned_base_rows=True,
            source_serving_generation=destination_options.get("source_serving_generation"),
            **({} if archive_relation is None else {"archive_relation": archive_relation}),
        )


async def _project_loaded_geo(session, semantic, destination_options, names, stage_relations, selected=None):
    """Derive geo fields once from the explicitly locked local dependency set."""
    native = destination
    schema = destination_options["db_schema"]
    main = names[native.entity_address_unified.EntityAddressUnified.__tablename__]
    main_rows = native._main_table_receipt(semantic).row_count
    bindings = destination_options.get("dependency_bindings")
    if bindings is not None:
        bindings = native._geo_binding_names(schema, bindings)
    options = {} if bindings is None else {"dependency_bindings": bindings}
    projected = await native.entity_address_unified._materialize_geo_assurance(
        schema, main, force=True, context={}, run_id="", stage_rows=main_rows, **options
    )
    _require(projected == main_rows, "geo projection accounting differs")
    geo = await native._capture_geo_preparation(
        session,
        db_schema=schema,
        stage_relation_name=main,
        projected_rows=projected,
        publisher_selected_inputs_sha256=None if selected is None else _digest(selected),
        **options,
    )
    return geo, bindings


async def _activate_set_validated_destination(
    session, *, stored, validation, preparation, expected_incumbent, callbacks, authenticate_preparation
):
    """Recheck validated data and publish all seven named relations in the caller transaction."""
    prepared_stored = _validated_set_prepared(validation)
    proof, owner = await _authenticated_preparation(
        session, prepared_stored, preparation, validation, authenticate_preparation
    )
    _require_set_input(stored, validation, prepared_stored, proof, owner)
    schema = prepared_stored["restored"]["db_schema"]
    await serving.require_entity_address_receive_incumbent(session, schema_name=schema, expected=expected_incumbent)
    await _require_alias_fence(session, schema, proof, validation)
    await _lock_publication_state(session, schema)
    await _protected_catalog(session, proof, owner, read_only=True)
    if validation["contract"] == NAMED_CONTRACT:
        expected_contents = _validated_loaded_contents(validation["contents"], owner)
        _require(
            await _loaded_candidate_contents(session, owner) == expected_contents,
            "frozen candidate contents differ",
        )
    async with destination.db.bind_existing_session(session):
        schema, _names = await _move_to_destination(session, prepared_stored, owner)
        prepared = _prepared_adoption(prepared_stored)
        dependency_bindings = await _resolve_dependency_bindings(session, schema, prepared)
        expected_geo = destination._logical_geo_preparation(
            destination._validated_geo_preparation(prepared_stored["geo_assurance"], db_schema=schema),
            prepared=prepared,
            dependency_bindings=dependency_bindings,
        )
        actual_geo = await destination._capture_geo_preparation(
            session,
            db_schema=schema,
            stage_relation_name=prepared.stage_cls.__tablename__,
            projected_rows=expected_geo.projected_rows,
            dependency_bindings=dependency_bindings,
            publisher_selected_inputs_sha256=expected_geo.publisher_selected_inputs_sha256,
        )
        _require(actual_geo == expected_geo, "geo assurance changed")
        published = await destination.adoption.adopt_prepared_entity_address_snapshot(prepared, callbacks=callbacks)
        return await _publication_receipt(session, schema, owner, prepared, published)


def _validated_set_prepared(validation):
    """Accept the closed named edition and preserve old envelopes as opaque compatibility metadata."""
    fields = {"contract", "prepared", "input_sha256", "stored_sha256", "inventory", "alias"}
    legacy_fields = (fields - {"inventory"}) | {
        "inventory_sha256",
        "protected_owner_oid",
        "builder_oid",
        "catalog_sha256",
        "alias_catalog_sha256",
    }
    _require(
        isinstance(validation, Mapping) and validation.get("contract") in (SET_CONTRACT, NAMED_CONTRACT),
        "validation differs",
    )
    expected = fields | {"contents"} if validation["contract"] == NAMED_CONTRACT else legacy_fields
    _require(set(validation) in (expected, expected | {"publisher_selected_inputs_sha256"}), "validation differs")
    prepared_stored = validation["prepared"]
    _require(
        isinstance(prepared_stored, Mapping) and prepared_stored.get("contract") == destination.SET_CONTRACT,
        "prepared contract differs",
    )
    _require_selected_inputs_seal(prepared_stored, validation)
    return prepared_stored


def _require_selected_inputs_seal(prepared, validation):
    """Bind the separate immutable selection to both native context and geo evidence."""
    from process.entity_address_dependency_bindings import (
        publisher_selected_input_names,
        validate_publisher_selected_inputs,
    )

    context = prepared.get("restored", {}).get("context", {})
    geo = prepared.get("geo_assurance", {})
    selected = context.get("publisher_selected_inputs")
    if not any(
        (
            selected is not None,
            "publisher_selected_inputs_sha256" in validation,
            "publisher_selected_inputs_sha256" in geo,
        )
    ):
        return
    try:
        selected = validate_publisher_selected_inputs(prepared["restored"]["db_schema"], selected)
    except (ValueError, KeyError, TypeError) as error:
        raise EntityAddressSnapshotDestinationError("entity-address selected input seal differs") from error
    named_selected = publisher_selected_input_names(prepared["restored"]["db_schema"], selected)
    accepted_digests = {_digest(selected), _digest(named_selected)}
    _require(
        destination._geo_binding_names(prepared["restored"]["db_schema"], context.get("dependency_bindings"))
        == named_selected["dependency_bindings"]
        and validation.get("publisher_selected_inputs_sha256") in accepted_digests
        and geo.get("publisher_selected_inputs_sha256") in accepted_digests,
        "selected input seal differs",
    )


def _require_set_input(stored, validation, prepared_stored, proof, owner):
    """Bind completed authority to the unchanged approved input and local frozen inventory."""
    from process.entity_address_snapshot_receipt import CONTRACT as archive_contract

    _require_selected_inputs_seal(prepared_stored, validation)
    _require(
        _digest(prepared_stored) == validation["stored_sha256"]
        and validation["alias"] == prepared_stored["destination_alias_receipt"],
        "v2 seal differs",
    )
    if validation["contract"] == NAMED_CONTRACT:
        _require(validation.get("inventory") == _logical_inventory(proof["inventory"], owner), "inventory differs")
    _require(
        isinstance(stored, Mapping)
        and set(stored) == {"contract", "ownership", "semantic_receipt", "source_alias_receipt", "destination"}
        and stored.get("contract") == archive_contract,
        "input contract differs",
    )
    semantic = destination.validate_entity_address_archive_receipt(stored["semantic_receipt"])
    source_alias = alias.validate_entity_address_alias_semantic_receipt(stored["source_alias_receipt"])
    input_owner = destination.validate_entity_address_archive_stage_ownership(stored["ownership"])
    _require(
        (input_owner.dataset_id, input_owner.schema_name, input_owner.relation_names)
        == (owner.dataset_id, owner.schema_name, owner.relation_names)
        and semantic.as_dict() == prepared_stored["source_semantic_receipt"]
        and source_alias.as_dict() == prepared_stored["source_alias_receipt"]
        and validation["input_sha256"] == _digest(_loaded_input(owner, semantic, source_alias, stored["destination"])),
        "input seal differs",
    )
    context = prepared_stored.get("restored", {}).get("context", {})
    if "publisher_selected_inputs" not in context:
        _require(
            destination._geo_binding_names(prepared_stored["restored"]["db_schema"], context.get("dependency_bindings"))
            == destination._geo_binding_names(
                prepared_stored["restored"]["db_schema"], stored["destination"].get("dependency_bindings")
            ),
            "ordinary dependency bounds differ",
        )


async def _publication_receipt(session, schema, owner, prepared, result):
    """Publish and retain only exact qualified names; PostgreSQL supplies their access boundary."""
    relations = [
        {"schema_name": schema, "relation_name": model.__tablename__} for model in destination.restore._models()
    ]
    return {
        **result,
        "publication": {
            "contract": "entity-address-table-publication.v3",
            "schema_name": schema,
            "relations": relations,
            "retained_relations": prepared.context.get("retained_relations", []),
            "alias_authority": {"schema_name": owner.schema_name, "relation_name": alias.AUTHORITY_TABLE}
            if alias.AUTHORITY_TABLE in owner.relation_names
            else None,
        },
    }


__all__ = [
    "prepare_loaded_entity_address_archive_destination",
    "EntityAddressSnapshotDestinationError",
    "require_entity_address_archive_publisher",
    "prepare_private_entity_address_archive_destination",
    "validate_entity_address_archive_preparation",
    "activate_validated_entity_address_archive_destination",
]
