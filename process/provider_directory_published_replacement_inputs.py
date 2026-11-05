# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin ordinary serving peers without borrowing artifact-candidate eligibility."""

import importlib
import os
from datetime import datetime
from types import SimpleNamespace

from process import provider_directory_published_source_replacement as replacement
from process import provider_directory_serving_recovery_receipts as receipts

_PIN_FIELDS = ("source_id", "endpoint_id", "dataset_id", "dataset_hash", "parent_run_id")
_HEADER_FIELDS = (*replacement._HEADER_FIELDS, "validated_at")


def _require(condition, suffix):
    receipts.require(condition, "replacement_inputs_" + suffix)


def _is_text(value):
    return type(value) is str and bool(value) and value == value.strip() and len(value) <= 160


def _is_digest(value):
    return type(value) is str and receipts._HASH.fullmatch(value) is not None


def _is_timestamp(value):
    if type(value) is not str:
        return False
    try:
        return datetime.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _entry(directory, row, *, is_publication=False):
    values = directory._pagination_checkpoint_row_mapping(row)
    entry_by_field = {
        "source_id": values["source_id"],
        "header": {
            name: value.isoformat() if isinstance(value := values[name], datetime) else value for name in _HEADER_FIELDS
        },
    }
    if is_publication:
        entry_by_field["source_member"] = values["source_member"]
    return entry_by_field


def _selection_sql(directory, *, is_publication=False):
    source, datasets = replacement._tables(directory)
    fields = ",".join("dataset." + name for name in _HEADER_FIELDS)
    if is_publication:
        members = "(dataset.publication_metadata_json->'source_ids')::jsonb"
        fields += (
            f",COALESCE(jsonb_typeof({members})='array' "
            f"AND {members} @> jsonb_build_array(source.source_id),false) AS source_member"
        )
    return (
        source,
        datasets,
        f"SELECT source.source_id,{fields} FROM ONLY {source} source JOIN ONLY {datasets} dataset ON ",
    )


async def _serving(directory):
    _source, datasets, selection = _selection_sql(directory)
    rows = await directory.db.all(
        selection + "dataset.endpoint_id=source.endpoint_id "
        f"JOIN (SELECT endpoint_id FROM ONLY {datasets} WHERE is_current GROUP BY endpoint_id HAVING count(*)=1) sole "
        "ON sole.endpoint_id=dataset.endpoint_id WHERE dataset.is_current AND dataset.status='published' "
        "AND dataset.published_at IS NOT NULL AND dataset.superseded_at IS NULL ORDER BY source.source_id"
    )
    return sorted((_entry(directory, row) for row in rows), key=lambda entry: entry["source_id"])


async def _publication(directory, pin):
    _source, datasets, selection = _selection_sql(directory, is_publication=True)
    rows = await directory.db.all(
        selection + "dataset.dataset_id=:dataset_id AND dataset.endpoint_id=:endpoint_id "
        "WHERE source.source_id=:source_id AND dataset.is_current AND dataset.status='published' "
        "AND dataset.published_at IS NOT NULL AND dataset.superseded_at IS NULL "
        f"AND (SELECT count(*) FROM ONLY {datasets} current WHERE current.endpoint_id=dataset.endpoint_id AND current.is_current)=1",
        **{name: pin[name] for name in ("source_id", "endpoint_id", "dataset_id")},
    )
    _require(len(rows) == 1, "publication_changed")
    return _entry(directory, rows[0], is_publication=True)


def _validate_entry(entry, *, require_provenance, is_publication=False):
    fields = {"source_id", "header"} | ({"source_member"} if is_publication else set())
    _require(
        type(entry) is dict
        and set(entry) == fields
        and _is_text(entry["source_id"])
        and (not is_publication or entry["source_member"] is True),
        "entry_invalid",
    )
    header = entry["header"]
    _require(
        type(header) is dict
        and set(header) == set(_HEADER_FIELDS)
        and all(_is_text(header[name]) for name in ("dataset_id", "endpoint_id", "acquisition_root_run_id"))
        and (header["import_run_id"] is None or _is_text(header["import_run_id"]))
        and type(header["resource_count"]) is int
        and header["resource_count"] >= 0
        and _is_timestamp(header["published_at"])
        and (header["validated_at"] is None or _is_timestamp(header["validated_at"]))
        and all(
            header[name] is None or _is_digest(header[name])
            for name in ("dataset_hash", "publication_metadata_sha256", "content_proof_admission_sha256")
        ),
        "header_invalid",
    )
    if require_provenance:
        _require(
            _is_digest(header["dataset_hash"]) and header["validated_at"] is not None,
            "peer_provenance_required",
        )


def _pin(entry):
    header = entry["header"]
    return dict(
        source_id=entry["source_id"],
        endpoint_id=header["endpoint_id"],
        dataset_id=header["dataset_id"],
        dataset_hash=header["dataset_hash"],
        parent_run_id=header["acquisition_root_run_id"],
    )


def validate(snapshot, target_pin=None):
    """Decode only the fixed versioned serving snapshot, including its complete alias set."""
    _require(
        type(snapshot) is dict
        and set(snapshot) == {"version", "source_id", "publication", "serving"}
        and type(snapshot["version"]) is int
        and snapshot["version"] == 2
        and _is_text(snapshot["source_id"])
        and type(snapshot["serving"]) is list,
        "snapshot_invalid",
    )
    _validate_entry(snapshot["publication"], require_provenance=True, is_publication=True)
    _require(snapshot["publication"]["source_id"] == snapshot["source_id"], "publication_changed")
    for entry in snapshot["serving"]:
        _validate_entry(
            entry, require_provenance=not (type(entry) is dict and entry.get("source_id") == snapshot["source_id"])
        )
    sources = [entry["source_id"] for entry in snapshot["serving"]]
    _require(sources == sorted(set(sources)) and sources.count(snapshot["source_id"]) == 1, "aliases_invalid")
    if target_pin is not None:
        _require(
            type(target_pin) is dict
            and set(target_pin) == set(_PIN_FIELDS)
            and _pin(snapshot["publication"]) == target_pin,
            "publication_changed",
        )
    from process.entity_address_candidate_preparation import _validate_source_inputs

    _validate_source_inputs(_desired_pins(snapshot), ())


async def capture(directory, target_fence):
    """Observe current serving aliases without holding locks across the Address build."""
    scope = replacement.selected(directory, target_fence)
    _require(scope is not None and len(target_fence.datasets) == 1, "scope_required")
    selected = target_fence.datasets[0]
    pin_by_field = dict(
        source_id=selected.source_id,
        endpoint_id=selected.endpoint_id,
        dataset_id=selected.dataset_id,
        dataset_hash=selected.dataset_hash,
        parent_run_id=selected.evidence_run_id,
    )
    snapshot_by_field = dict(
        version=2,
        source_id=selected.source_id,
        publication=await _publication(directory, pin_by_field),
        serving=await _serving(directory),
    )
    validate(snapshot_by_field, pin_by_field)
    incumbent = next(entry for entry in snapshot_by_field["serving"] if entry["source_id"] == selected.source_id)
    _require(
        tuple(incumbent["header"][name] for name in replacement._HEADER_FIELDS)
        == tuple(value.isoformat() if isinstance(value, datetime) else value for value in scope.incumbent)
        and tuple(snapshot_by_field["publication"]["header"][name] for name in replacement._HEADER_FIELDS)
        == tuple(value.isoformat() if isinstance(value, datetime) else value for value in scope.publication),
        "capture_changed",
    )
    return snapshot_by_field


def _desired_pins(snapshot):
    from process.entity_address_candidate_preparation import ProviderDirectoryAddressDatasetPin

    pins = []
    for entry in snapshot["serving"]:
        selected = snapshot["publication"] if entry["source_id"] == snapshot["source_id"] else entry
        header = selected["header"]
        pins.append(
            ProviderDirectoryAddressDatasetPin(
                selected["source_id"],
                header["endpoint_id"],
                header["dataset_id"],
                header["dataset_hash"],
                header["acquisition_root_run_id"],
            )
        )
    return tuple(pins)


def desired_pins(snapshot):
    """Substitute only the selected publication; retain every other serving source pin."""
    validate(snapshot)
    return _desired_pins(snapshot)


def peer_pins(snapshot):
    """Expose only non-target canonical peers to the internal ordinary Address input."""
    validate(snapshot)
    return tuple(pin for pin in _desired_pins(snapshot) if pin.source_id != snapshot["source_id"])


def validate_peer_preparation(inputs):
    """Allow canonical peers only inside their exact active ordinary replacement."""
    from process import entity_address_candidate_preparation as address
    from process import provider_directory_generic_source_artifacts as artifacts

    directory = importlib.import_module("process.provider_directory_fhir")
    admission = artifacts.current(directory)
    _require(
        type(inputs) is address.ProviderDirectoryAddressPreparationInput
        and admission is not None
        and admission.factory is None
        and admission.replacement_scope is not None
        and admission.recovery is not None
        and address._native().db is directory.db
        and (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf") == directory._schema(),
        "peer_preparation_required",
    )
    snapshot = admission.recovery.get("serving_inputs")
    validate(snapshot, admission.recovery.get("source"))
    _require(
        admission.recovery.get("source") == _pin(snapshot["publication"])
        and inputs.dataset_pins == desired_pins(snapshot)
        and inputs.ordinary_serving_peer_pins == peer_pins(snapshot),
        "peer_preparation_changed",
    )


async def lock_and_check(directory, snapshot, *, pointer="incumbent"):
    """Freeze serving-vector phantoms only inside the caller's bounded final transaction."""
    validate(snapshot)
    _require(pointer in {"incumbent", "publication"}, "pointer_invalid")
    await receipts.ordinary(directory.db)
    _require(await directory.db.scalar("SHOW transaction_isolation") == "read committed", "snapshot_changed")
    entries = [*snapshot["serving"], snapshot["publication"]]
    endpoints = SimpleNamespace(endpoint_ids=sorted({entry["header"]["endpoint_id"] for entry in entries}))
    await directory._lock_artifact_fence_endpoint_advisories(endpoints, directory.db)
    await directory._lock_artifact_fence_endpoints(endpoints, directory.db)
    source, datasets = replacement._tables(directory)
    # Row locks miss new aliases and newly serving endpoints; freeze both sets only for the final cut.
    await directory.db.status(f"LOCK TABLE {datasets},{source} IN SHARE MODE NOWAIT")
    await directory.db.all(
        f"SELECT source_id FROM ONLY {source} WHERE source_id=ANY(CAST(:source_ids AS varchar[])) ORDER BY source_id FOR SHARE NOWAIT",
        source_ids=[entry["source_id"] for entry in snapshot["serving"]],
    )
    expected_entries = [
        {name: snapshot["publication"][name] for name in ("source_id", "header")}
        if pointer == "publication" and entry["source_id"] == snapshot["source_id"]
        else entry
        for entry in snapshot["serving"]
    ]
    _require(await _serving(directory) == expected_entries, "serving_changed")
    _require(
        await _publication(directory, _pin(snapshot["publication"])) == snapshot["publication"], "publication_changed"
    )
