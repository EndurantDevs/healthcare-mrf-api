# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Require the complete retained active MD/DO source before atomic publication."""

from __future__ import annotations

import hashlib
import re

from sqlalchemy import select, text

from process import provider_profile_source_store as shared
from process.massachusetts_profile_acquisition import encoded_json
from process.provider_profile_source_completion import SourceProfileCompletion
from process.provider_profile_source_store import ProfileSourcePolicy, SourceProfileStore
from process.rhode_island_profile_cohort import COVERAGE_SCOPE
from process.rhode_island_profile_roster import SOURCE_URL
from process.rhode_island_profile_rows import LICENSE_TYPES, SCHEMA_VERSION, SOURCE_KEY, _validated_schema_page

IMPORTER = "rhode-island-doh-profile"
CATEGORIES = ("education", "specialties", "privileges")


def _sha(count_value):
    return isinstance(count_value, str) and re.fullmatch(r"[a-f0-9]{64}", count_value) is not None


def _hash(count_value):
    return hashlib.sha256(encoded_json(count_value)).hexdigest()


def _has_valid_profiles(profiles):
    for license_number, descriptor in profiles.items():
        if (
            not re.fullmatch(r"(?:MD|DO)[0-9]{5}", license_number)
            or not isinstance(descriptor, dict)
            or not _sha(descriptor.get("content_sha256"))
            or not _sha(descriptor.get("record_receipt_sha256"))
            or not _sha(descriptor.get("page_receipt_sha256"))
            or descriptor.get("source_url")
            != "https://datahealth.ri.gov/find/providers/loadRecord.php?id=" + license_number
            or not isinstance(descriptor.get("schema_page"), dict)
            or not _sha(descriptor["schema_page"].get("content_sha256"))
            or not isinstance(descriptor.get("roster_occurrences"), list)
            or not descriptor["roster_occurrences"]
        ):
            return False
        try:
            _validated_schema_page(descriptor["schema_page"], license_number)
        except ValueError:
            return False
    return True


class RhodeIslandProfileStore(SourceProfileStore):
    """Use shared source locking and completion while fencing RI capture evidence."""

    def _manifest(self, run):
        manifest = run.get("source_manifest")
        fields = {
            "control_run_id",
            "expected_current_run_id",
            "max_providers",
            "resume_from",
            "categories",
            "license_types",
            "snapshot_sha256",
            "snapshot_row_count",
            "source",
        }
        if not isinstance(manifest, dict) or set(manifest) != fields:
            raise ValueError("rhode_island_profile_manifest_invalid")
        if (
            manifest["max_providers"] is not None
            or manifest["resume_from"] is not None
            or manifest["categories"] != list(CATEGORIES)
            or manifest["license_types"] != list(LICENSE_TYPES)
        ):
            raise ValueError("rhode_island_profile_complete_pair_required")
        if (
            not _sha(manifest["snapshot_sha256"])
            or type(manifest["snapshot_row_count"]) is not int
            or manifest["snapshot_row_count"] < 0
        ):
            raise ValueError("rhode_island_profile_snapshot_invalid")
        if not isinstance(manifest["control_run_id"], str) or not manifest["control_run_id"].strip():
            raise ValueError("rhode_island_profile_managed_run_required")
        if manifest["expected_current_run_id"] is not None:
            self._run_id(manifest["expected_current_run_id"])
        expected_source_by_field = {
            "source_key": SOURCE_KEY,
            "source_kind": "state_regulator",
            "jurisdiction": "RI",
            "agency": "Rhode Island Department of Health",
            "source_url": SOURCE_URL,
            "coverage_scope": COVERAGE_SCOPE,
            "registry_generation": manifest["snapshot_sha256"],
        }
        if manifest["source"] != expected_source_by_field:
            raise ValueError("rhode_island_profile_manifest_source_invalid")
        return manifest

    def _bundle_counts(self, run, artifacts):
        """Validate the frozen roster/profile inventory, not just an aggregate count."""
        invalid_counts_by_field = {"invalid_bundle_artifacts": 1, "bundle_metrics": None}
        if len(artifacts) != 1:
            return invalid_counts_by_field
        artifact = artifacts[0]
        bundle = artifact.get("metadata_json")
        if (
            artifact.get("artifact_id") != _hash([run["run_id"], SOURCE_KEY])
            or artifact.get("source_key") != SOURCE_KEY
            or artifact.get("run_id") != run["run_id"]
            or artifact.get("category") != "profile"
            or artifact.get("file_name") != "manifest.json"
            or artifact.get("source_url") != SOURCE_URL
            or not isinstance(bundle, dict)
            or bundle.get("schema_version") != SCHEMA_VERSION
            or bundle.get("run_id") != run["run_id"]
            or bundle.get("source_manifest") != self._manifest(run)
            or artifact.get("content_sha256") != _hash(bundle)
            or artifact.get("content_bytes") != len(encoded_json(bundle))
        ):
            return invalid_counts_by_field
        cohort, profiles, metrics = bundle.get("cohort"), bundle.get("profiles"), bundle.get("acquisition")
        if (
            not isinstance(cohort, dict)
            or not isinstance(profiles, dict)
            or not isinstance(metrics, dict)
            or cohort.get("coverage_scope") != COVERAGE_SCOPE
            or not _sha(cohort.get("content_sha256"))
            or set(cohort.get("rosters", {})) != set(LICENSE_TYPES)
        ):
            return invalid_counts_by_field
        for prefix, license_type in LICENSE_TYPES.items():
            roster = cohort["rosters"][prefix]
            if (
                roster.get("license_type") != license_type
                or type(roster.get("input_rows")) is not int
                or roster["input_rows"] <= 0
                or roster["input_rows"] != roster.get("expected_rows")
                or type(roster.get("unique_licenses")) is not int
                or roster["unique_licenses"] <= 0
                or roster["unique_licenses"] > roster["input_rows"]
                or sum(license_number.startswith(prefix) for license_number in profiles) != roster["unique_licenses"]
            ):
                return invalid_counts_by_field
            for name in ("preview", "download"):
                descriptor = roster.get(name)
                if (
                    not isinstance(descriptor, dict)
                    or not _sha(descriptor.get("content_sha256"))
                    or type(descriptor.get("content_bytes")) is not int
                    or descriptor["content_bytes"] <= 0
                ):
                    return invalid_counts_by_field
        if not _has_valid_profiles(profiles):
            return invalid_counts_by_field
        if metrics.get("responses") != len(profiles) or metrics.get("cohort_sha256") != cohort["content_sha256"]:
            return invalid_counts_by_field
        return {"invalid_bundle_artifacts": 0, "bundle_metrics": metrics}

    async def retained_counts(self, run_id):
        """Compare stored profiles/facts with their exact roster and capture inventory."""
        counts = await super().retained_counts(run_id)
        source_table = self._table(shared.ProviderProfileSourceRecord)
        facts = self._table(shared.ProviderProfileFact)
        artifacts = self._table(shared.ProviderProfileArtifact)
        runs = self._table(shared.ProviderProfileImportRun)
        count_row = await shared.db.first(
            text(f"""
            SELECT count(*) FILTER (WHERE left(r.license_number,2) = 'MD') AS md_source_records,
              count(*) FILTER (WHERE left(r.license_number,2) = 'DO') AS do_source_records,
              count(*) - count(DISTINCT r.license_number) AS duplicate_licenses,
              count(*) FILTER (WHERE a.artifact_id IS NULL OR a.run_id IS DISTINCT FROM r.run_id
                OR a.source_key IS DISTINCT FROM r.source_key OR r.license_number !~ '^(MD|DO)[0-9]{{5}}$'
                OR r.source_record_key IS DISTINCT FROM 'rhode-island-doh:' || r.license_number
                OR r.match_evidence->'registry_binding'->>'status' IS DISTINCT FROM r.match_status
                OR r.match_evidence->'registry_binding'->'retained_snapshot'->>'snapshot_sha256'
                    IS DISTINCT FROM source_run.source_manifest->>'snapshot_sha256'
                OR r.normalized_payload->>'cohort_sha256' IS DISTINCT FROM a.metadata_json->'cohort'->>'content_sha256'
                OR json_typeof(r.normalized_payload->'profile_capture') IS DISTINCT FROM 'object'
                OR (r.normalized_payload->'profile_capture')::jsonb IS DISTINCT FROM
                    (a.metadata_json->'profiles'->r.license_number)::jsonb
                OR json_typeof(r.raw_payload->'roster_occurrences') IS DISTINCT FROM 'array'
                OR (r.raw_payload->'roster_occurrences')::jsonb IS DISTINCT FROM
                    (a.metadata_json->'profiles'->r.license_number->'roster_occurrences')::jsonb) AS invalid_bundle_records,
              (SELECT count(*) FROM {facts} f JOIN {source_table} sr ON sr.record_id = f.source_record_id
                WHERE f.run_id = :run_id AND (
                  f.source_json->>'artifact_id' IS DISTINCT FROM sr.artifact_id
                  OR f.source_json->>'content_sha256' IS DISTINCT FROM sr.normalized_payload->'profile_capture'->>'content_sha256'
                  OR f.source_json->>'source_url' IS DISTINCT FROM sr.normalized_payload->'profile_capture'->>'source_url'
                  OR f.source_json->>'downloaded_at' IS DISTINCT FROM sr.normalized_payload->'profile_capture'->>'downloaded_at'
                  OR (f.source_json->'schema_page')::jsonb IS DISTINCT FROM
                      (sr.normalized_payload->'profile_capture'->'schema_page')::jsonb)) AS invalid_capture_facts
            FROM {source_table} r LEFT JOIN {artifacts} a ON a.artifact_id = r.artifact_id
              LEFT JOIN {runs} source_run ON source_run.run_id = r.run_id WHERE r.run_id = :run_id
        """),
            run_id=run_id,
        )
        artifact_rows = await shared.db.all(
            select(shared.ProviderProfileArtifact.__table__).where(
                shared.ProviderProfileArtifact.__table__.c.run_id == run_id
            )
        )
        return {
            **counts,
            **{key: int(count_value or 0) for key, count_value in count_row._mapping.items()},
            **self._bundle_counts(
                await self._read_run(run_id), [dict(count_row._mapping) for count_row in artifact_rows]
            ),
        }

    def _completion_metrics(self, run, metrics, counts):
        self._manifest(run)
        if (
            not isinstance(metrics, dict)
            or metrics.get("acquisition_complete") is not True
            or type(metrics.get("transport_failures")) is not int
            or metrics["transport_failures"] != 0
        ):
            raise RuntimeError("rhode_island_profile_acquisition_incomplete")
        if (
            metrics != counts["bundle_metrics"]
            or type(metrics.get("responses")) is not int
            or metrics["responses"] != counts["retained_source_records"]
            or metrics["responses"] != counts["received_profiles"]
            or counts["md_source_records"] != metrics.get("md_source_records")
            or counts["do_source_records"] != metrics.get("do_source_records")
            or counts["retained_facts"] != metrics.get("facts")
        ):
            raise RuntimeError("rhode_island_profile_retained_count_mismatch")
        if any(
            counts[key]
            for key in (
                "invalid_source_records",
                "invalid_facts",
                "foreign_artifacts",
                "invalid_bundle_artifacts",
                "invalid_bundle_records",
                "invalid_capture_facts",
                "duplicate_licenses",
            )
        ):
            raise RuntimeError("rhode_island_profile_retained_integrity_invalid")
        return {
            **metrics,
            **counts,
            "requested_licenses": metrics["responses"],
            "full_cohort_licenses": metrics["responses"],
        }

    def _publication_volume(self, metrics, incumbent_metrics):
        # Historical complete roster: 6,447 MD / 712 DO licenses. Floors allow ordinary changes, not a preview.
        if (
            metrics["md_source_records"] < 5000
            or metrics["do_source_records"] < 500
            or metrics["matched_public_providers"] <= 0
        ):
            raise RuntimeError("rhode_island_profile_first_publication_too_small")
        if incumbent_metrics is not None:
            for key in ("md_source_records", "do_source_records", "matched_public_providers", "received_profiles"):
                if metrics[key] * 5 < incumbent_metrics[key] * 4:
                    raise RuntimeError("rhode_island_profile_publication_volume_drop:" + key)


store = RhodeIslandProfileStore(
    ProfileSourcePolicy(
        source_key=SOURCE_KEY,
        schema_version=SCHEMA_VERSION,
        jurisdiction="RI",
        categories=CATEGORIES,
        error_prefix="rhode_island_profile",
        received_profile_sql="""normalized_payload->>'visibility' = 'public' AND
        CASE WHEN json_typeof(raw_payload->'values') = 'array' THEN json_array_length(raw_payload->'values') > 0
        ELSE FALSE END""",
        guarded_public_sql="""f.category = 'education' AND f.fact_type = 'education_history'
        AND json_typeof(f.value_json->'institution') = 'string'
        AND lower(btrim(f.value_json->>'institution')) NOT IN ('','other','unknown','n/a','not reported')""",
        invalid_fact_sql="""(f.category = 'education' AND f.fact_type = 'education_history'
        OR f.category = 'specialties' AND f.fact_type = 'specialty'
        OR f.category = 'privileges' AND f.fact_type = 'staff_privilege') IS DISTINCT FROM TRUE
        OR f.source_json->>'run_id' IS DISTINCT FROM f.run_id
        OR f.source_json->>'agency' IS DISTINCT FROM 'Rhode Island Department of Health'
        OR f.source_json->>'jurisdiction' IS DISTINCT FROM 'RI'""",
    )
)
completion = SourceProfileCompletion(store, IMPORTER)
