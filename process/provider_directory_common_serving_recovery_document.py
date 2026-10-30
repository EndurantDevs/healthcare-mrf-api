# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed documents for the existing ordinary common-serving recovery journal."""

import re

from db.plan_release_storage import storage_identifier
from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_output_snapshot_catalog import decoded, encoded

CONTRACT = "provider-directory-common-serving-recovery.v1"
FAMILIES = {
    "profile": "provider_directory_profile",
    "evidence": "provider_directory_profile_evidence",
    "overlay": "provider_directory_address_overlay",
    "unified": "entity_address_unified",
    "address_evidence": "entity_address_evidence",
    "plan_bridge": "entity_address_plan_bridge",
    "network_bridge": "entity_address_network_bridge",
    "procedure_bridge": "entity_address_procedure_bridge",
    "medication_bridge": "entity_address_medication_bridge",
    "facility_candidate": "facility_anchor_npi_candidate",
}
_METADATA_FIELDS = {"address", "alias", "overlay_alias", "profile", "doctors", "geo", "relations", "common_receipt"}
_ACTOR_FIELDS = {"session", "current", "owner", "backend", "xid", "database_oid"}
_NATIVE_FIELDS = {
    "schema_oid",
    "oid",
    "relname",
    "relowner",
    "topology",
    "columns",
    "constraints",
    "indexes",
    "acl",
    "descriptor",
}
_RETAINED_FIELDS = {
    "kind",
    "family",
    "schema",
    "source",
    "source_oid",
    "name",
    "oid",
    "layout_sha256",
    "columns",
    "rows",
    "data_sha256",
    "copied_bytes",
    "reused_initial",
}
_PREPARATION_FIELDS = {
    "contract_id",
    "retention",
    "expected_postimages",
    "profile_result",
    "selection_fingerprint",
    "input_hash",
    "run_uuid",
    "semantic_inputs",
    "qualified_source_ids",
    "initial_receipt",
}


def _require(condition, reason):
    if not condition:
        raise ValueError("provider_directory_common_serving_recovery_" + reason)


def _is_valid_hash(value):
    return type(value) is str and receipts._HASH.fullmatch(value) is not None


def _is_valid_oid(value):
    return type(value) is int and 0 < value < 2**32


def _is_valid_text(value):
    return type(value) is str and bool(value) and value == value.strip()


def common_generation(worker_run_uuid):
    """Identify the genuine worker operation, including reruns of the same acquired root."""
    return recovery.canonical_generation(worker_run_uuid)


def _actor(actor_by_field):
    _require(type(actor_by_field) is dict and set(actor_by_field) == _ACTOR_FIELDS, "actor_invalid")
    _require(
        _is_valid_text(actor_by_field["session"]) and actor_by_field["session"] == actor_by_field["current"],
        "actor_invalid",
    )
    _require(all(_is_valid_oid(actor_by_field[key]) for key in ("owner", "database_oid")), "actor_invalid")
    _require(
        type(actor_by_field["backend"]) is int
        and actor_by_field["backend"] > 0
        and _is_valid_text(actor_by_field["xid"]),
        "actor_invalid",
    )


def _native(root, native_by_field):
    _require(type(native_by_field) is dict and set(native_by_field) == _NATIVE_FIELDS, "native_invalid")
    _require(
        native_by_field["relname"] == root
        and all(_is_valid_oid(native_by_field[key]) for key in ("oid", "relowner", "schema_oid")),
        "native_invalid",
    )
    receipts._require_heap(native_by_field, native_by_field["relowner"])


def validate_state(raw):
    """Preserve ten actual native roots plus genuine published or initial Profile authority."""
    state_by_field = decoded(encoded(decoded(raw)))
    _require(
        type(state_by_field) is dict and set(state_by_field) == {"metadata", "roots", "initial_targets"},
        "state_invalid",
    )
    metadata_by_field, roots_by_name = state_by_field["metadata"], state_by_field["roots"]
    _require(type(metadata_by_field) is dict and set(metadata_by_field) == _METADATA_FIELDS, "metadata_invalid")
    _require(type(roots_by_name) is dict and set(roots_by_name) == set(FAMILIES.values()), "roots_invalid")
    for root, native in roots_by_name.items():
        _native(root, native)
    _require(len({native["oid"] for native in roots_by_name.values()}) == len(FAMILIES), "roots_invalid")
    expected_oids = [roots_by_name[name]["oid"] for name in receipts.generations.RELATION_NAMES]
    _require(metadata_by_field["relations"] == expected_oids, "address_roots_invalid")
    address = metadata_by_field["address"]
    _require(type(address) is dict and address.get("singleton") is True, "address_invalid")
    _require(type(address.get("local_generation")) is int and address["local_generation"] >= 0, "address_invalid")
    _require(address.get("relation_oids") is None or address["relation_oids"] == expected_oids, "address_invalid")
    initial_targets = state_by_field["initial_targets"]
    if metadata_by_field["profile"] is None:
        from process.provider_directory_profile_initial_contract import validated_target_state

        initial_targets = validated_target_state(initial_targets)
        _require(
            initial_targets["profile_target_oid"] == roots_by_name[FAMILIES["profile"]]["oid"]
            and initial_targets["evidence_target_oid"] == roots_by_name[FAMILIES["evidence"]]["oid"],
            "initial_roots_invalid",
        )
    else:
        _require(type(metadata_by_field["profile"]) is dict and initial_targets is None, "profile_authority_invalid")
    return state_by_field


def _retained_entry(entry, kind):
    _require(type(entry) is dict and set(entry) == _RETAINED_FIELDS and entry["kind"] == kind, "retention_invalid")
    family = entry["family"]
    allowed = FAMILIES if kind == "family" else {"npi_scope", "location_scope", "group_scope"}
    _require(family in allowed, "retention_invalid")
    for key in ("schema", "source", "name"):
        storage_identifier(entry[key])
    _require(
        _is_valid_oid(entry["source_oid"]) and _is_valid_oid(entry["oid"]) and _is_valid_hash(entry["layout_sha256"]),
        "retention_invalid",
    )
    _require(all(type(entry[key]) is int and entry[key] >= 0 for key in ("rows", "copied_bytes")), "retention_invalid")
    _require(
        type(entry["columns"]) is list and len(entry["columns"]) == len(set(entry["columns"])), "retention_invalid"
    )
    for column in entry["columns"]:
        storage_identifier(column)
    _require(type(entry["reused_initial"]) is bool, "retention_invalid")
    if kind == "family":
        _require(entry["source"] == FAMILIES[family], "retention_invalid")
    if entry["reused_initial"]:
        _require(
            kind == "family"
            and family in {"profile", "evidence"}
            and entry["source_oid"] == entry["oid"]
            and entry["name"] in {entry["source"], receipts.retained_name(entry["oid"])}
            and not entry["columns"]
            and entry["data_sha256"] is None
            and entry["copied_bytes"] == 0,
            "retention_invalid",
        )
    else:
        _require(re.fullmatch("pd_" + family + r"_before_[0-9a-f]{20}", entry["name"]) is not None, "retention_invalid")
        _require(
            entry["source_oid"] != entry["oid"] and _is_valid_hash(entry["data_sha256"]) and bool(entry["columns"]),
            "retention_invalid",
        )


def validate_retention(raw):
    """Accept only the existing ten-family retained rows and three known scope kinds."""
    retained_by_field = decoded(encoded(decoded(raw)))
    _require(
        type(retained_by_field) is dict
        and set(retained_by_field)
        == {"contract_id", "affected_scope_sha256", "retention_plan_sha256", "creator", "scopes", "families"},
        "retention_invalid",
    )
    _require(
        retained_by_field["contract_id"] == "provider-directory-ordinary-retained-rows.v1"
        and _is_valid_hash(retained_by_field["affected_scope_sha256"])
        and _is_valid_hash(retained_by_field["retention_plan_sha256"]),
        "retention_invalid",
    )
    _actor(retained_by_field["creator"])
    seen_families, seen_scopes, seen_oids = set(), set(), set()
    for kind, entries in (("scope", retained_by_field["scopes"]), ("family", retained_by_field["families"])):
        _require(type(entries) is list, "retention_invalid")
        seen = seen_families if kind == "family" else seen_scopes
        for entry in entries:
            _retained_entry(entry, kind)
            family = entry["family"]
            _require(family not in seen and entry["oid"] not in seen_oids, "retention_invalid")
            seen.add(family)
            seen_oids.add(entry["oid"])
    _require(
        seen_families == set(FAMILIES) and {"location_scope", "group_scope"} <= seen_scopes, "retention_incomplete"
    )
    return retained_by_field


def _profile_result(raw):
    from process import provider_directory_profile_selection_contract as selection

    _require(type(raw) is dict, "profile_result_invalid")
    _require(type(raw.get("generation")) is int and raw["generation"] >= 1, "profile_result_invalid")
    has_desired_date = raw.get("contract_id") == selection.PROFILE_SELECTION_DESIRED_RESULT_CONTRACT_ID
    fields = selection._ATTESTATION_FIELDS - {"contract_id"}
    if has_desired_date:
        fields |= selection._DESIRED_SELECTION_FIELDS
    attestation = selection.validated_profile_selection_attestation(
        {
            "contract_id": selection.PROFILE_SELECTION_DESIRED_ATTESTATION_CONTRACT_ID
            if has_desired_date
            else selection.PROFILE_SELECTION_ATTESTATION_CONTRACT_ID,
            **{key: raw.get(key) for key in fields},
        }
    )
    result = selection.profile_selection_result(
        selection.ProviderDirectoryProfileExecution(attestation, raw.get("generation")),
        profile_generation_id=raw.get("profile_generation_id"),
        profile_as_of=raw.get("profile_as_of"),
        profile_rows=raw.get("row_counts", {}).get("profile_rows"),
        profile_source_evidence_rows=raw.get("row_counts", {}).get("profile_source_evidence_rows"),
    )
    _require(raw == result and result["status"] == "published", "profile_result_invalid")
    return result


def validate_preparation(raw):
    """Decode the closed capture/profile_result evidence without interpreting it as runtime authority."""
    from process.provider_directory_artifact_bundle_preparation import validated_semantic_inputs

    preparation_by_field = decoded(encoded(decoded(raw)))
    _require(
        type(preparation_by_field) is dict
        and set(preparation_by_field) == _PREPARATION_FIELDS
        and preparation_by_field["contract_id"] == CONTRACT,
        "preparation_invalid",
    )
    common_generation(preparation_by_field["run_uuid"])
    validate_retention(preparation_by_field["retention"])
    expected = preparation_by_field["expected_postimages"]
    _require(type(expected) is dict and set(expected) == set(FAMILIES), "postimages_invalid")
    for receipt in expected.values():
        _require(
            type(receipt) is dict
            and set(receipt) == {"rows", "data_sha256"}
            and type(receipt["rows"]) is int
            and receipt["rows"] >= 0
            and _is_valid_hash(receipt["data_sha256"]),
            "postimages_invalid",
        )
    profile_result = _profile_result(preparation_by_field["profile_result"])
    _require(
        preparation_by_field["selection_fingerprint"] == profile_result["selection_fingerprint"]
        and _is_valid_hash(preparation_by_field["input_hash"]),
        "selection_invalid",
    )
    initial_receipt = preparation_by_field["initial_receipt"]
    if initial_receipt is not None:
        _require(
            type(initial_receipt) is dict
            and set(initial_receipt) == {"build_id", "generation_id", "run_id", "payload_sha256"},
            "initial_receipt_invalid",
        )
        _require(
            all(_is_valid_text(initial_receipt[key]) for key in ("build_id", "generation_id", "run_id"))
            and _is_valid_hash(initial_receipt["payload_sha256"]),
            "initial_receipt_invalid",
        )
        _require(initial_receipt["generation_id"] == profile_result["profile_generation_id"], "initial_receipt_invalid")
    semantic_inputs = validated_semantic_inputs(preparation_by_field["semantic_inputs"])
    _require(semantic_inputs == preparation_by_field["semantic_inputs"], "semantic_inputs_invalid")
    qualified = preparation_by_field["qualified_source_ids"]
    _require(
        type(qualified) is list
        and qualified
        and all(_is_valid_text(source_id) for source_id in qualified)
        and qualified == sorted(set(qualified)),
        "qualified_scope_invalid",
    )
    return preparation_by_field


def _envelope(raw, phase, generation, source_id):
    """Validate a common published/recovered event in the existing version-one envelope."""
    document_by_field = decoded(encoded(decoded(raw)))
    _require(
        type(document_by_field) is dict
        and set(document_by_field)
        == {
            "version",
            "generation",
            "phase",
            "source",
            "before",
            "after",
            "writer",
            "predecessor",
            "journal",
            "preparation",
        }
        | ({"inverse"} if phase == "recovered" else set()),
        "document_invalid",
    )
    _require(
        type(document_by_field["version"]) is int
        and document_by_field["version"] == 1
        and phase in {"published", "recovered"}
        and document_by_field["phase"] == phase
        and document_by_field["generation"] == generation,
        "document_invalid",
    )
    if phase == "recovered":
        from process.provider_directory_serving_inverse_capacity_contract import hash64, run_handle

        inverse = document_by_field["inverse"]
        _require(
            type(inverse) is dict and set(inverse) == {"operation_sha256", "run_id", "lease_digest"}, "inverse_invalid"
        )
        hash64(inverse["operation_sha256"])
        hash64(inverse["lease_digest"])
        run_handle(inverse["run_id"])
    source_pin = document_by_field["source"]
    _require(
        type(source_pin) is dict
        and set(source_pin) == set(recovery._PIN_FIELDS)
        and source_pin.get("source_id") == source_id,
        "source_invalid",
    )
    recovery.publication_generation(source_pin)
    _actor(document_by_field["writer"])
    _native(receipts.JOURNAL, document_by_field["journal"])
    _require(
        document_by_field["predecessor"] is None
        if phase == "published"
        else _is_valid_hash(document_by_field["predecessor"]),
        "predecessor_invalid",
    )
    return document_by_field


def _transition(document_by_field, phase, prepared):
    before_by_field = validate_state(document_by_field["before"])
    after_by_field = validate_state(document_by_field["after"])
    old, new = before_by_field["metadata"], after_by_field["metadata"]
    _require(
        new["address"]["local_lineage_id"] == old["address"]["local_lineage_id"]
        and new["address"]["local_generation"] == old["address"]["local_generation"] + 1,
        "address_generation_invalid",
    )
    _require(old["alias"] == new["alias"] and old["doctors"] == new["doctors"], "unrelated_metadata_changed")
    _require(
        all(
            before_by_field["roots"][name] == after_by_field["roots"][name]
            for name in (*receipts.generations.RELATION_NAMES, FAMILIES["overlay"])
        ),
        "address_native_changed",
    )
    if phase == "published":
        for entry in prepared["retention"]["families"]:
            _require(
                entry["source_oid"] == before_by_field["roots"][FAMILIES[entry["family"]]]["oid"],
                "retained_source_changed",
            )
            _require(
                not entry["reused_initial"] or before_by_field["initial_targets"] is not None,
                "initial_retention_invalid",
            )
        _require(
            after_by_field["initial_targets"] is None
            and new["profile"].get("generation_id") == prepared["profile_result"]["profile_generation_id"],
            "profile_generation_invalid",
        )
        if before_by_field["initial_targets"] is None:
            _require(
                all(
                    before_by_field["roots"][FAMILIES[family]] == after_by_field["roots"][FAMILIES[family]]
                    for family in ("profile", "evidence")
                ),
                "profile_native_changed",
            )


def document(raw, phase, generation, source_id):
    """Validate a common published/recovered event in the existing version-one envelope."""
    document_by_field = _envelope(raw, phase, generation, source_id)
    prepared = validate_preparation(document_by_field["preparation"])
    _require(common_generation(prepared["run_uuid"]) == generation, "worker_generation_changed")
    _transition(document_by_field, phase, prepared)
    return document_by_field
