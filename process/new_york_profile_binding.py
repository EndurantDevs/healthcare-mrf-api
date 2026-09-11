# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bind retained NY education to a complete supplied registry snapshot, offline."""

from __future__ import annotations

import copy
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from process.kentucky_profile_acquisition import _read_artifact, _reject_symlinks
from process.massachusetts_profile_acquisition import encoded_json
from process.massachusetts_profile_rows import _text
from process.new_york_nysed_profile import read_acquisition as read_nysed_acquisition
from process.new_york_profile_retained import MAX_METADATA_BYTES, MAX_RESPONSE_ENVELOPE_BYTES, read_acquisition
from process.provider_directory_profile import is_valid_npi
from process.provider_directory_projection_json import decoded_json_object

SNAPSHOT_SCHEMA = "ny-nppes-retained-snapshot/v1"
COVERAGE_SCOPE = "all_current_literal_ny_license_occurrences"
CORROBORATED_METHOD = "exact_ny_license_nysed_legal_name/v1"
# Bound complete state snapshots separately from individual source responses.
MAX_SNAPSHOT_BYTES = 384 * 1024 * 1024
# Reviewed SELECT: literal NY only, nullable NPI/NUCC joins, ordered by npi/checksum.
QUERY_SHA256 = "ccecd6458b1cc71617f5f3404ef300debd9bb976e20827e0940f12400c0c0b7d"
REGISTRY_COLUMNS = [
    "npi",
    "taxonomy_occurrence_checksum",
    "license_number",
    "license_state",
    "taxonomy",
    "primary_taxonomy_switch",
    "joined_npi",
    "entity_type_code",
    "first_name",
    "middle_name",
    "last_name",
    "suffix",
    "joined_taxonomy_code",
    "taxonomy_grouping",
]
NAME_FIELDS = (
    ("firstName", "first_name"),
    ("middleName", "middle_name"),
    ("lastName", "last_name"),
    ("suffix", "suffix"),
)
ACQUISITION_FILES = (
    ("manifest.json", MAX_METADATA_BYTES),
    ("result.json", MAX_METADATA_BYTES),
    ("search.request.json", MAX_METADATA_BYTES),
    ("search.response.json", MAX_RESPONSE_ENVELOPE_BYTES),
    ("education.request.json", MAX_METADATA_BYTES),
    ("education.response.json", MAX_RESPONSE_ENVELOPE_BYTES),
)


def _require(condition, reason):
    if not condition:
        raise ValueError("new_york_binding_" + reason)


def _acquisition_content(destination):
    digests_by_name, responses_by_name = {}, {}
    for name, byte_limit in ACQUISITION_FILES:
        path = destination / name
        _reject_symlinks(path)
        _require(path.is_file() and path.stat().st_size <= byte_limit, "acquisition_file_invalid")
        with path.open("rb") as stream:
            content = stream.read(byte_limit + 1)
        _require(0 < len(content) <= byte_limit, "acquisition_file_invalid")
        digests_by_name[name] = hashlib.sha256(content).hexdigest()
        if name.endswith(".response.json"):
            responses_by_name[name] = decoded_json_object(content)
    return hashlib.sha256(encoded_json(digests_by_name)).hexdigest(), responses_by_name


def acquisition_content_sha256(destination: Path) -> str:
    """Digest the six consumed files; pin this independently at trusted capture time."""
    return _acquisition_content(destination)[0]


def _validated_acquisition(destination, manifest_sha256, acquisition_sha256):
    _require(
        isinstance(acquisition_sha256, str) and re.fullmatch(r"[0-9a-f]{64}", acquisition_sha256),
        "acquisition_pin_invalid",
    )
    content_sha256, responses_by_name = _acquisition_content(destination)
    _require(content_sha256 == acquisition_sha256, "acquisition_changed")
    acquisition_by_field = read_acquisition(destination, manifest_sha256=manifest_sha256)
    source_record = acquisition_by_field["source_record"]
    profile_response = responses_by_name["education.response.json"]
    search_response = responses_by_name["search.response.json"]
    # Match the transport envelopes actually replayed, even if files change and
    # are restored between the original byte digest and the final byte check.
    _require(
        source_record["artifact_id"] == hashlib.sha256(encoded_json(profile_response)).hexdigest()
        and all(
            source_record["match_evidence"]["license_search"][field] == search_response.get(field)
            for field in ("source_url", "request_sha256", "content_sha256", "downloaded_at")
        ),
        "acquisition_replay_changed",
    )
    _require(acquisition_content_sha256(destination) == acquisition_sha256, "acquisition_changed")
    return acquisition_by_field


def _validate_snapshot_metadata(snapshot_by_field):
    _require(
        snapshot_by_field.get("schema_version") == SNAPSHOT_SCHEMA
        and snapshot_by_field.get("coverage_scope") == COVERAGE_SCOPE
        and snapshot_by_field.get("source_schema") == "mrf"
        and snapshot_by_field.get("source_state") == "NY"
        and snapshot_by_field.get("query_sha256") == QUERY_SHA256
        and snapshot_by_field.get("columns") == REGISTRY_COLUMNS,
        "snapshot_scope_invalid",
    )
    _require(
        snapshot_by_field.get("status") == "passed"
        and snapshot_by_field.get("all_rows_received") is True
        and snapshot_by_field.get("connection_closed") is True,
        "snapshot_incomplete",
    )
    transaction_by_field = snapshot_by_field.get("snapshot")
    _require(
        isinstance(transaction_by_field, dict)
        and transaction_by_field.get("read_only") == "on"
        and transaction_by_field.get("isolation") == "repeatable read"
        and isinstance(transaction_by_field.get("snapshot_id"), str)
        and re.fullmatch(r"[0-9]+:[0-9]+:(?:[0-9]+(?:,[0-9]+)*)?", transaction_by_field["snapshot_id"])
        and all(
            isinstance(transaction_by_field.get(field), str) and transaction_by_field[field].strip()
            for field in ("snapshot_started_at", "server_version", "database_name")
        )
        and type(transaction_by_field.get("backend_pid")) is int
        and transaction_by_field["backend_pid"] > 0,
        "snapshot_transaction_invalid",
    )
    relations_by_name = snapshot_by_field.get("registry_relations")
    _require(
        isinstance(relations_by_name, dict)
        and set(relations_by_name) == {"mrf.npi", "mrf.npi_taxonomy", "mrf.nucc_taxonomy"}
        and all(type(oid) is int and oid > 0 for oid in relations_by_name.values()),
        "snapshot_relations_invalid",
    )


def read_registry_snapshot(path: Path, *, snapshot_sha256: str) -> dict:
    """Check a caller-pinned canonical envelope without attesting its acquisition.

    The caller must authenticate the original capture separately. Counts and
    hashes prove retained structure, not source truth, freshness or NY coverage.
    The inherited file bound does not establish that a full NY snapshot fits.
    """
    _require(
        isinstance(snapshot_sha256, str) and re.fullmatch(r"[0-9a-f]{64}", snapshot_sha256), "snapshot_pin_invalid"
    )
    snapshot_by_field = _read_artifact(path, MAX_SNAPSHOT_BYTES)
    _require(hashlib.sha256(encoded_json(snapshot_by_field)).hexdigest() == snapshot_sha256, "snapshot_changed")
    _validate_snapshot_metadata(snapshot_by_field)
    registry_rows = snapshot_by_field.get("registry_rows")
    # Missing or nontext license values could conceal a requested root. Missing
    # enrichment remains visible and makes that literal root fail identity checks.
    _require(
        isinstance(registry_rows, list)
        and all(
            isinstance(candidate, dict)
            and "license_number" in candidate
            and (candidate["license_number"] is None or isinstance(candidate["license_number"], str))
            and set(candidate) <= set(REGISTRY_COLUMNS)
            for candidate in registry_rows
        ),
        "snapshot_rows_invalid",
    )
    _require(
        type(snapshot_by_field.get("expected_source_row_count")) is int
        and type(snapshot_by_field.get("row_count")) is int
        and snapshot_by_field["expected_source_row_count"] == snapshot_by_field["row_count"] == len(registry_rows),
        "snapshot_count_changed",
    )
    registry_bytes = encoded_json(registry_rows)
    _require(
        type(snapshot_by_field.get("registry_rows_bytes")) is int
        and snapshot_by_field["registry_rows_bytes"] == len(registry_bytes)
        and snapshot_by_field.get("registry_rows_sha256") == hashlib.sha256(registry_bytes).hexdigest(),
        "snapshot_rows_changed",
    )
    previous_occurrence = None
    for candidate in registry_rows:
        # Invalid occurrence keys remain candidate conflicts, never exclusions.
        if type(candidate.get("npi")) is not int or type(candidate.get("taxonomy_occurrence_checksum")) is not int:
            continue
        occurrence = candidate["npi"], candidate["taxonomy_occurrence_checksum"]
        _require(previous_occurrence is None or previous_occurrence <= occurrence, "snapshot_order_changed")
        previous_occurrence = occurrence
    return snapshot_by_field


def _has_matching_names(identity_by_field, candidate):
    for source_field, registry_field in NAME_FIELDS:
        if registry_field not in candidate:
            return False
        registry_name = candidate[registry_field]
        # Present NULL optional names are unreported; missing columns are not.
        if not (
            isinstance(registry_name, str) or registry_field in {"middle_name", "suffix"} and registry_name is None
        ):
            return False
        if _text(registry_name).casefold() != _text(identity_by_field[source_field]).casefold():
            return False
    return True


def _physician_candidate(candidate):
    taxonomy = candidate.get("taxonomy")
    return (
        type(candidate.get("npi")) is int
        and is_valid_npi(candidate["npi"])
        and type(candidate.get("joined_npi")) is int
        and candidate["joined_npi"] == candidate["npi"]
        and type(candidate.get("taxonomy_occurrence_checksum")) is int
        and candidate.get("license_state") == "NY"
        and type(candidate.get("entity_type_code")) is int
        and candidate["entity_type_code"] == 1
        and isinstance(taxonomy, str)
        and bool(taxonomy.strip())
        and candidate.get("joined_taxonomy_code") == taxonomy
        and candidate.get("taxonomy_grouping") == "Allopathic & Osteopathic Physicians"
    )


def _compatible_candidate(identity_by_field, candidate):
    return _physician_candidate(candidate) and _has_matching_names(identity_by_field, candidate)


def _registry_match(acquisition_by_field, registry_rows):
    source_record = acquisition_by_field["source_record"]
    identity_by_field = source_record["raw_payload"]["data"]["phyInfo"]
    # Select literal licenses before examining entity, taxonomy, joins or names.
    candidates = [
        candidate for candidate in registry_rows if candidate["license_number"] == source_record["license_number"]
    ]
    binding_by_field = {
        "method": "exact_ny_license_name_components",
        "candidate_rows": candidates,
        "npi": None,
        "status": "identity_conflict",
        "source_npi": identity_by_field["nationalProviderId"],
        "registry_license_profession": "not_supplied_by_nppes",
    }
    if acquisition_by_field["identity_review_required"]:
        return {**binding_by_field, "reason": "search_header_name_disagreement"}
    if not candidates:
        return {**binding_by_field, "status": "unmatched", "reason": "no_exact_license_candidates"}
    if not all(_compatible_candidate(identity_by_field, candidate) for candidate in candidates):
        return {**binding_by_field, "reason": "registry_identity_conflict"}
    candidate_npis = {candidate["npi"] for candidate in candidates}
    if len(candidate_npis) != 1:
        return {**binding_by_field, "status": "ambiguous", "reason": "multiple_matching_npis"}
    matched_npi = next(iter(candidate_npis))
    source_npi = _text(identity_by_field["nationalProviderId"])
    if source_npi and (not is_valid_npi(source_npi) or int(source_npi) != matched_npi):
        return {**binding_by_field, "reason": "source_npi_identity_conflict"}
    return {**binding_by_field, "npi": matched_npi, "status": "deterministic", "reason": "unique_exact_license_name"}


def bind_retained_acquisition(
    destination: Path, *, manifest_sha256: str, acquisition_sha256: str, snapshot_path: Path, snapshot_sha256: str
) -> dict:
    """Return an offline identity decision, preserving all fact provenance.

    Acquisition bytes must match a digest independently retained at trusted
    capture time. The snapshot pin likewise needs independent capture authority;
    structural consistency and an accepted identity do not authorize publication.
    """
    return RegistrySnapshot(snapshot_path, snapshot_sha256=snapshot_sha256).bind_retained_acquisition(
        destination, manifest_sha256=manifest_sha256, acquisition_sha256=acquisition_sha256
    )


def _binding_result(acquisition_by_field, binding_by_field, manifest_sha256, acquisition_sha256, snapshot_sha256):
    binding_by_field.update(
        snapshot_sha256=snapshot_sha256,
        manifest_sha256=manifest_sha256,
        acquisition_sha256=acquisition_sha256,
        coverage_scope=COVERAGE_SCOPE,
    )
    source_record = acquisition_by_field["source_record"]
    source_record.update(matched_npi=binding_by_field["npi"], match_status=binding_by_field["status"])
    source_record["match_evidence"].update(reason=binding_by_field["reason"], registry_binding=binding_by_field)
    for fact in acquisition_by_field["facts"]:
        fact["npi"] = binding_by_field["npi"]
    return {
        "outcome": "accepted" if binding_by_field["npi"] is not None else "held",
        "reason": binding_by_field["reason"],
        "source_record": source_record,
        "facts": acquisition_by_field["facts"],
    }


def _search_name_relationship(acquisition_by_field):
    source_record = acquisition_by_field["source_record"]
    identity_by_field = source_record["raw_payload"]["data"]["phyInfo"]
    search_identity = source_record["match_evidence"]["license_search"]["raw_identity"]
    if search_identity["physicianID"] != str(identity_by_field["physicianID"]):
        return "conflict"
    header_names = tuple(_text(identity_by_field[field]).casefold() for field in ("firstName", "lastName"))
    search_names = tuple(
        _text(search_identity[field]).casefold() for field in ("physicianFirstName", "physicianLastName")
    )
    if header_names == search_names:
        return "equal"
    return "first_last_transposed" if header_names == search_names[::-1] else "conflict"


def _nysed_corroboration(acquisition_by_field, nysed_by_field):
    source_record = acquisition_by_field["source_record"]
    identity_by_field = source_record["raw_payload"]["data"]["phyInfo"]
    nysed_record = nysed_by_field["source_record"]
    nysed_name = nysed_record["raw_payload"]["name"]["value"]
    header_name = " ".join(
        _text(identity_by_field[field]) for field in ("lastName", "firstName", "middleName", "suffix")
    )
    source_evidence = nysed_by_field["facts"][0]["source_json"]
    return {
        "receipt_sha256": nysed_by_field["receipt_sha256"],
        "source_record_id": nysed_record["record_id"],
        "artifact_id": nysed_record["artifact_id"],
        **{field: source_evidence[field] for field in ("source_key", "source_url", "content_sha256", "downloaded_at")},
        "profession_code": nysed_record["profession_code"],
        "license_number": nysed_record["license_number"],
        "legal_name": nysed_name,
        "license_matches": nysed_record["profession_code"] == "060"
        and nysed_record["license_number"] == source_record["license_number"],
        "header_legal_name_matches": _text(header_name).casefold() == _text(nysed_name).casefold(),
    }


def _middle_name_relationship(source_middle, registry_middle):
    if registry_middle is not None and not isinstance(registry_middle, str):
        return "conflict"
    source_middle, registry_middle = _text(source_middle).casefold(), _text(registry_middle).casefold()
    if source_middle == registry_middle:
        return "equal"
    if not registry_middle:
        return "registry_unreported"
    # Only one reported registry initial may abbreviate a single full source name.
    if (
        re.fullmatch(r"[a-z]", registry_middle)
        and len(source_middle) > 1
        and source_middle.isalpha()
        and source_middle.startswith(registry_middle)
    ):
        return "registry_initial"
    return "conflict"


def _corroborated_name_relationship(identity_by_field, candidate):
    if not all(field in candidate for _, field in NAME_FIELDS):
        return "conflict"
    for source_field, registry_field in NAME_FIELDS:
        if registry_field == "middle_name":
            continue
        registry_name = candidate[registry_field]
        if not (isinstance(registry_name, str) or registry_field == "suffix" and registry_name is None):
            return "conflict"
        if _text(registry_name).casefold() != _text(identity_by_field[source_field]).casefold():
            return "conflict"
    return _middle_name_relationship(identity_by_field["middleName"], candidate["middle_name"])


def _corroborated_registry_match(acquisition_by_field, nysed_by_field, registry_rows):
    source_record = acquisition_by_field["source_record"]
    identity_by_field = source_record["raw_payload"]["data"]["phyInfo"]
    candidates = [
        candidate for candidate in registry_rows if candidate["license_number"] == source_record["license_number"]
    ]
    corroboration = _nysed_corroboration(acquisition_by_field, nysed_by_field)
    search_relationship = _search_name_relationship(acquisition_by_field)
    binding_by_field = {
        "method": CORROBORATED_METHOD,
        "candidate_rows": candidates,
        "npi": None,
        "status": "identity_conflict",
        "source_npi": identity_by_field["nationalProviderId"],
        "registry_license_profession": "not_supplied_by_nppes",
        "npi_verification": "not_independently_verified",
        "nysed_corroboration": corroboration,
        "search_header_name_relationship": search_relationship,
    }
    if not corroboration["license_matches"] or not corroboration["header_legal_name_matches"]:
        return {**binding_by_field, "reason": "nysed_identity_conflict"}
    if search_relationship == "conflict":
        return {**binding_by_field, "reason": "search_header_name_disagreement"}
    if not candidates:
        return {**binding_by_field, "status": "unmatched", "reason": "no_exact_license_candidates"}
    name_relationships = [_corroborated_name_relationship(identity_by_field, candidate) for candidate in candidates]
    binding_by_field["registry_middle_name_relationships"] = name_relationships
    if not all(_physician_candidate(candidate) for candidate in candidates) or "conflict" in name_relationships:
        return {**binding_by_field, "reason": "registry_identity_conflict"}
    candidate_npis = {candidate["npi"] for candidate in candidates}
    if len(candidate_npis) != 1:
        return {**binding_by_field, "status": "ambiguous", "reason": "multiple_matching_npis"}
    matched_npi = next(iter(candidate_npis))
    source_npi = _text(identity_by_field["nationalProviderId"])
    if source_npi and (not is_valid_npi(source_npi) or int(source_npi) != matched_npi):
        return {**binding_by_field, "reason": "source_npi_identity_conflict"}
    return {
        **binding_by_field,
        "npi": matched_npi,
        "status": "deterministic",
        "reason": "unique_exact_license_corroborated_name",
    }


def bind_corroborated_acquisition(
    destination: Path,
    *,
    manifest_sha256: str,
    acquisition_sha256: str,
    snapshot_path: Path,
    snapshot_sha256: str,
    nysed_destination: Path,
    nysed_receipt_sha256: str,
) -> dict:
    """Link NYPP facts using independently retained NYSED license/name evidence.

    This explicit method allows registry middle-name omission or a single initial
    and exact search/header transposition. Source disagreement flags remain. The
    caller must authenticate all three capture pins; this is neither independently
    verified NPI identity nor publication authorization. The strict entry point
    keeps its original matching policy.
    """
    return RegistrySnapshot(snapshot_path, snapshot_sha256=snapshot_sha256).bind_corroborated_acquisition(
        destination,
        manifest_sha256=manifest_sha256,
        acquisition_sha256=acquisition_sha256,
        nysed_destination=nysed_destination,
        nysed_receipt_sha256=nysed_receipt_sha256,
    )


def bind_nysed_acquisition(
    destination: Path,
    *,
    manifest_sha256: str,
    acquisition_sha256: str,
    snapshot_path: Path,
    snapshot_sha256: str,
    nysed_destination: Path,
    nysed_receipt_sha256: str,
) -> dict:
    """Bind NYSED's own facts through the same explicitly corroborated source pair.

    Both sources must contain acquired profiles. A no-profile response remains
    on its source's held replay path; this method cannot fabricate a record.
    Binding does not establish independent NPI verification or publication.
    """
    return RegistrySnapshot(snapshot_path, snapshot_sha256=snapshot_sha256).bind_nysed_acquisition(
        destination,
        manifest_sha256=manifest_sha256,
        acquisition_sha256=acquisition_sha256,
        nysed_destination=nysed_destination,
        nysed_receipt_sha256=nysed_receipt_sha256,
    )


@dataclass(frozen=True, slots=True, init=False, repr=False, eq=False)
class RegistrySnapshot:
    """Validate one pinned snapshot and retain every literal occurrence for reuse.

    Bind calls use the loaded capture even if its file later changes. The caller
    must authenticate that capture and choose its lifetime; this is not a source
    freshness check or publication authority. Returned evidence is independent
    of the private index, and both matching policies retain their usual guards.
    """

    _snapshot_sha256: str
    _rows_by_license: dict

    def __init__(self, path: Path, *, snapshot_sha256: str):
        snapshot_by_field = read_registry_snapshot(path, snapshot_sha256=snapshot_sha256)
        rows_by_license = {}
        for candidate in snapshot_by_field["registry_rows"]:
            rows_by_license.setdefault(candidate["license_number"], []).append(candidate)
        object.__setattr__(self, "_snapshot_sha256", snapshot_sha256)
        object.__setattr__(self, "_rows_by_license", rows_by_license)

    def _candidates(self, acquisition_by_field):
        license_number = acquisition_by_field["source_record"]["license_number"]
        return copy.deepcopy(self._rows_by_license.get(license_number, []))

    def bind_retained_acquisition(self, destination: Path, *, manifest_sha256: str, acquisition_sha256: str) -> dict:
        """Apply strict matching to a freshly validated retained acquisition."""
        acquisition_by_field = _validated_acquisition(destination, manifest_sha256, acquisition_sha256)
        binding_by_field = _registry_match(acquisition_by_field, self._candidates(acquisition_by_field))
        return _binding_result(
            acquisition_by_field, binding_by_field, manifest_sha256, acquisition_sha256, self._snapshot_sha256
        )

    def bind_corroborated_acquisition(
        self,
        destination: Path,
        *,
        manifest_sha256: str,
        acquisition_sha256: str,
        nysed_destination: Path,
        nysed_receipt_sha256: str,
    ) -> dict:
        """Apply explicit corroboration with freshly validated NYPP and NYSED evidence."""
        acquisition_by_field, _, binding_by_field = self._corroborated_pair(
            destination, manifest_sha256, acquisition_sha256, nysed_destination, nysed_receipt_sha256
        )
        return _binding_result(
            acquisition_by_field, binding_by_field, manifest_sha256, acquisition_sha256, self._snapshot_sha256
        )

    def _corroborated_pair(
        self, destination, manifest_sha256, acquisition_sha256, nysed_destination, nysed_receipt_sha256
    ):
        acquisition_by_field = _validated_acquisition(destination, manifest_sha256, acquisition_sha256)
        nysed_by_field = read_nysed_acquisition(nysed_destination, receipt_sha256=nysed_receipt_sha256)
        binding_by_field = _corroborated_registry_match(
            acquisition_by_field, nysed_by_field, self._candidates(acquisition_by_field)
        )
        return acquisition_by_field, nysed_by_field, binding_by_field

    def bind_nysed_acquisition(
        self,
        destination: Path,
        *,
        manifest_sha256: str,
        acquisition_sha256: str,
        nysed_destination: Path,
        nysed_receipt_sha256: str,
    ) -> dict:
        """Return NYSED facts only after revalidating both captures and their identity proof."""
        acquisition_by_field, nysed_by_field, binding_by_field = self._corroborated_pair(
            destination, manifest_sha256, acquisition_sha256, nysed_destination, nysed_receipt_sha256
        )
        source_record = acquisition_by_field["source_record"]
        binding_by_field.update(
            bound_source_key=nysed_by_field["source_record"]["source_key"],
            source_npi_source_key=source_record["source_key"],
            nypp_corroboration={
                **{
                    field: source_record[field]
                    for field in ("source_key", "record_id", "artifact_id", "license_number")
                },
                "quality_flags": source_record["normalized_payload"]["quality_flags"],
                "license_search": source_record["match_evidence"]["license_search"],
            },
        )
        return _binding_result(
            nysed_by_field, binding_by_field, manifest_sha256, acquisition_sha256, self._snapshot_sha256
        )
