# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate the paired Tennessee reports before shared atomic publication."""

from __future__ import annotations

import hashlib
import re
from pathlib import PurePath

from sqlalchemy import select, text

from process import provider_profile_source_store as shared_store
from process.massachusetts_profile_acquisition import encoded_json
from process.provider_profile_source_completion import SourceProfileCompletion
from process.provider_profile_source_store import ProfileSourcePolicy, SourceProfileStore, ensure_tables
from process.tennessee_profile_rows import MAX_REPORT_BYTES, SCHEMA_VERSION, SOURCE_KEY

IMPORTER = "tennessee-tdh-profile"
PROFESSIONS = ("1606", "1907")
CATEGORIES = ("education", "training", "specialties")
COVERAGE_SCOPE = "regular_md_do_all_ranks_statuses_locations"
SOURCE_URL = "https://internet.health.tn.gov/LicensureReports"


def _is_sha256(digest):
    return isinstance(digest, str) and re.fullmatch(r"[a-f0-9]{64}", digest) is not None


def _has_valid_profession_counts(counts_by_profession):
    return (
        isinstance(counts_by_profession, dict)
        and set(counts_by_profession) == set(PROFESSIONS)
        and all(type(count) is int and count >= 0 for count in counts_by_profession.values())
    )


def _has_valid_report(report):
    if not isinstance(report, dict):
        return False
    file_name = report.get("file_name")
    return (
        _is_sha256(report.get("content_sha256"))
        and type(report.get("content_bytes")) is int
        and 0 < report["content_bytes"] <= MAX_REPORT_BYTES
        and isinstance(file_name, str)
        and bool(file_name)
        and PurePath(file_name).name == file_name
        and file_name not in {".", "..", "snapshot.json", "manifest.json"}
        and all(
            isinstance(report.get(field), str) and report[field].strip() for field in ("source_url", "downloaded_at")
        )
    )


def _report_pair_sha256(reports, run_id, artifact_id):
    """Hash both descriptors with the same capture evidence as the binder."""
    report_pins_by_profession = {
        profession: {
            "content_sha256": report["content_sha256"],
            "evidence": {
                "run_id": run_id,
                "artifact_id": artifact_id,
                **{field: report[field] for field in ("source_url", "downloaded_at", "content_sha256")},
            },
        }
        for profession, report in reports.items()
    }
    return hashlib.sha256(encoded_json(report_pins_by_profession)).hexdigest()


class TennesseeProfileStore(SourceProfileStore):
    """Keep bulk acquisition counts separate from retained records and physician coverage."""

    def _manifest(self, run_by_field):
        manifest = run_by_field.get("source_manifest")
        required_fields = {
            "control_run_id",
            "expected_current_run_id",
            "max_providers",
            "resume_from",
            "categories",
            "report_professions",
            "snapshot_sha256",
            "snapshot_row_count",
            "source",
        }
        if not isinstance(manifest, dict) or set(manifest) != required_fields:
            raise ValueError("tennessee_profile_manifest_invalid")
        if manifest["max_providers"] is not None or manifest["resume_from"] is not None:
            raise ValueError("tennessee_profile_partial_scope_forbidden")
        if manifest["categories"] != list(CATEGORIES) or manifest["report_professions"] != list(PROFESSIONS):
            raise ValueError("tennessee_profile_manifest_scope_invalid")
        if (
            not _is_sha256(manifest["snapshot_sha256"])
            or type(manifest["snapshot_row_count"]) is not int
            or manifest["snapshot_row_count"] < 0
        ):
            raise ValueError("tennessee_profile_snapshot_identity_invalid")
        if not isinstance(manifest["control_run_id"], str) or not manifest["control_run_id"].strip():
            raise ValueError("tennessee_profile_managed_run_required")
        if manifest["expected_current_run_id"] is not None:
            self._run_id(manifest["expected_current_run_id"])
        expected_source_by_field = {
            "source_key": SOURCE_KEY,
            "source_kind": "state_regulator",
            "jurisdiction": "TN",
            "agency": "Tennessee Department of Health",
            "source_url": SOURCE_URL,
            "coverage_scope": COVERAGE_SCOPE,
            "registry_generation": manifest["snapshot_sha256"],
        }
        descriptor = manifest["source"]
        if not isinstance(descriptor, dict) or any(
            descriptor.get(key) != expected for key, expected in expected_source_by_field.items()
        ):
            raise ValueError("tennessee_profile_manifest_source_invalid")
        return manifest

    def _bundle_counts(self, run_by_field, artifacts):
        """Validate the singleton artifact and both retained report descriptors."""
        invalid_counts_by_field = {
            "invalid_bundle_artifacts": 1,
            "bundle_reports_sha256": None,
            "bundle_source_records_by_profession": None,
            "bundle_facts_by_profession": None,
        }
        if len(artifacts) != 1:
            return invalid_counts_by_field
        artifact = artifacts[0]
        bundle = artifact.get("metadata_json")
        manifest = self._manifest(run_by_field)
        if (
            artifact.get("source_key") != SOURCE_KEY
            or artifact.get("category") != "profile"
            or artifact.get("file_name") != "manifest.json"
            or not isinstance(bundle, dict)
            or bundle.get("schema_version") != SCHEMA_VERSION
            or bundle.get("run_id") != run_by_field["run_id"]
            or bundle.get("source_manifest") != manifest
            or not _is_sha256(bundle.get("reports_sha256"))
        ):
            return invalid_counts_by_field
        canonical = encoded_json(bundle)
        if (
            artifact.get("artifact_id")
            != hashlib.sha256(encoded_json([run_by_field["run_id"], SOURCE_KEY])).hexdigest()
            or artifact.get("content_sha256") != hashlib.sha256(canonical).hexdigest()
            or type(artifact.get("content_bytes")) is not int
            or artifact["content_bytes"] != len(canonical)
        ):
            return invalid_counts_by_field
        snapshot = bundle.get("snapshot")
        reports = bundle.get("reports")
        acquisition = bundle.get("acquisition")
        if (
            not isinstance(snapshot, dict)
            or snapshot.get("content_sha256") != manifest["snapshot_sha256"]
            or snapshot.get("file_name") != "snapshot.json"
            or type(snapshot.get("content_bytes")) is not int
            or snapshot["content_bytes"] <= 0
            or not isinstance(reports, dict)
            or set(reports) != set(PROFESSIONS)
            or not all(_has_valid_report(report) for report in reports.values())
            or len({report["file_name"] for report in reports.values()}) != 2
            or not isinstance(acquisition, dict)
        ):
            return invalid_counts_by_field
        if _report_pair_sha256(reports, run_by_field["run_id"], artifact["artifact_id"]) != bundle["reports_sha256"]:
            return invalid_counts_by_field
        return {
            "invalid_bundle_artifacts": 0,
            "bundle_reports_sha256": bundle["reports_sha256"],
            "bundle_source_records_by_profession": acquisition.get("source_records_by_profession"),
            "bundle_facts_by_profession": acquisition.get("facts_by_profession"),
        }

    def _bundle_integrity_query(self):
        """Keep record/report binding predicates together in one retained-count query."""
        source_table = self._table(shared_store.ProviderProfileSourceRecord)
        fact_table = self._table(shared_store.ProviderProfileFact)
        artifact_table = self._table(shared_store.ProviderProfileArtifact)
        run_table = self._table(shared_store.ProviderProfileImportRun)
        return text(f"""
            SELECT
              count(*) FILTER (WHERE r.profession_code = '1606') AS md_source_records,
              count(*) FILTER (WHERE r.profession_code = '1907') AS do_source_records,
              count(*) FILTER (WHERE a.artifact_id IS NULL OR a.run_id IS DISTINCT FROM r.run_id
                OR a.source_key IS DISTINCT FROM r.source_key
                OR r.profession_code NOT IN ('1606','1907') OR r.profession_code IS NULL
                OR r.source_record_key NOT LIKE 'tennessee-tdh:' || r.profession_code || ':%'
                OR r.normalized_payload->>'visibility' NOT IN ('public','held_identity')
                OR r.normalized_payload->>'visibility' IS NULL
                OR (r.normalized_payload->>'visibility' = 'held_identity' AND r.matched_npi IS NOT NULL)
                OR json_typeof(r.match_evidence->'registry_binding'->'status') IS DISTINCT FROM 'string'
                OR r.match_evidence->'registry_binding'->>'status' IS DISTINCT FROM r.match_status
                OR (r.match_evidence->'registry_binding'->'npi')::jsonb
                    IS DISTINCT FROM coalesce(to_jsonb(r.matched_npi),'null'::jsonb)
                OR r.match_evidence->'registry_binding'->>'snapshot_sha256'
                    IS DISTINCT FROM source_run.source_manifest->>'snapshot_sha256'
                OR r.match_evidence->'registry_binding'->>'reports_sha256'
                    IS DISTINCT FROM a.metadata_json->>'reports_sha256') AS invalid_bundle_records,
              (SELECT count(*) FROM {fact_table} f JOIN {source_table} source_record
                 ON source_record.record_id = f.source_record_id
                WHERE f.run_id = :run_id AND source_record.profession_code = '1606') AS md_facts,
              (SELECT count(*) FROM {fact_table} f JOIN {source_table} source_record
                 ON source_record.record_id = f.source_record_id
                WHERE f.run_id = :run_id AND source_record.profession_code = '1907') AS do_facts,
              (SELECT count(*) FROM {fact_table} f JOIN {source_table} source_record
                 ON source_record.record_id = f.source_record_id
                 LEFT JOIN {artifact_table} artifact ON artifact.artifact_id = source_record.artifact_id
                WHERE f.run_id = :run_id AND (
                  f.source_json->>'artifact_id' IS DISTINCT FROM source_record.artifact_id
                  OR f.source_json->>'content_sha256' IS DISTINCT FROM artifact.metadata_json->'reports'->(source_record.profession_code)->>'content_sha256'
                  OR f.source_json->>'source_url' IS DISTINCT FROM artifact.metadata_json->'reports'->(source_record.profession_code)->>'source_url'
                  OR f.source_json->>'downloaded_at' IS DISTINCT FROM artifact.metadata_json->'reports'->(source_record.profession_code)->>'downloaded_at'
                )) AS invalid_report_facts
              FROM {source_table} r LEFT JOIN {artifact_table} a ON a.artifact_id = r.artifact_id
              LEFT JOIN {run_table} source_run ON source_run.run_id = r.run_id
             WHERE r.run_id = :run_id
        """)

    async def retained_counts(self, run_id):
        """Count retained payloads and bind each public fact to its paired capture evidence."""
        counts = await super().retained_counts(run_id)
        retained = await shared_store.db.first(self._bundle_integrity_query(), run_id=run_id)
        counts.update({key: int(count) for key, count in retained._mapping.items()})
        counts["source_records_by_profession"] = {
            "1606": counts["md_source_records"],
            "1907": counts["do_source_records"],
        }
        counts["facts_by_profession"] = {"1606": counts["md_facts"], "1907": counts["do_facts"]}
        artifact_rows = await shared_store.db.all(
            select(shared_store.ProviderProfileArtifact.__table__).where(
                shared_store.ProviderProfileArtifact.__table__.c.run_id == run_id
            )
        )
        counts["retained_artifacts"] = len(artifact_rows)
        counts.update(
            self._bundle_counts(await self._read_run(run_id), [dict(artifact._mapping) for artifact in artifact_rows])
        )
        return counts

    async def _assert_source_ownership(self, run_ids):
        for run_id in run_ids:
            counts = await self.retained_counts(run_id)
            if not any(counts[name] for name in ("retained_source_records", "retained_facts", "retained_artifacts")):
                continue
            if counts["received_profiles"] != counts["retained_source_records"] or any(
                counts[name]
                for name in (
                    "invalid_source_records",
                    "invalid_facts",
                    "foreign_artifacts",
                    "invalid_bundle_artifacts",
                    "invalid_bundle_records",
                    "invalid_report_facts",
                )
            ):
                raise RuntimeError("tennessee_profile_retention_foreign_payload")

    def _completion_metrics(self, run_by_field, metrics, counts_by_field):
        manifest = self._manifest(run_by_field)
        if not isinstance(metrics, dict) or metrics.get("acquisition_complete") is not True:
            raise RuntimeError("tennessee_profile_acquisition_incomplete")
        if type(metrics.get("transport_failures")) is not int or metrics["transport_failures"] != 0:
            raise RuntimeError("tennessee_profile_transport_failures")
        if (
            type(metrics.get("http_responses")) is not int
            or metrics["http_responses"] != 8
            or type(metrics.get("report_responses")) is not int
            or metrics["report_responses"] != 2
            or type(metrics.get("response_bytes")) is not int
            or metrics["response_bytes"] <= 0
        ):
            raise RuntimeError("tennessee_profile_response_count_mismatch")
        if (
            not _is_sha256(metrics.get("reports_sha256"))
            or metrics["reports_sha256"] != counts_by_field["bundle_reports_sha256"]
            or metrics.get("snapshot_sha256") != manifest["snapshot_sha256"]
        ):
            raise RuntimeError("tennessee_profile_capture_identity_mismatch")
        for name in ("source_records_by_profession", "facts_by_profession"):
            if (
                not _has_valid_profession_counts(metrics.get(name))
                or metrics[name] != counts_by_field[name]
                or metrics[name] != counts_by_field["bundle_" + name]
            ):
                raise RuntimeError("tennessee_profile_retained_count_mismatch")
        if (
            sum(counts_by_field["source_records_by_profession"].values()) != counts_by_field["retained_source_records"]
            or sum(counts_by_field["facts_by_profession"].values()) != counts_by_field["retained_facts"]
            or counts_by_field["received_profiles"] != counts_by_field["retained_source_records"]
            or any(
                counts_by_field[key]
                for key in (
                    "invalid_source_records",
                    "invalid_facts",
                    "foreign_artifacts",
                    "invalid_bundle_artifacts",
                    "invalid_bundle_records",
                    "invalid_report_facts",
                )
            )
        ):
            raise RuntimeError("tennessee_profile_retained_integrity_invalid")
        return {**metrics, **counts_by_field}

    def _publication_volume(self, metrics, incumbent_metrics):
        if incumbent_metrics is None:
            if (
                metrics["md_source_records"] < 60000
                or metrics["do_source_records"] < 6000
                or metrics["matched_public_providers"] < 8000
            ):
                raise RuntimeError("tennessee_profile_first_publication_too_small")
        else:
            for name in ("matched_public_providers", "received_profiles", "md_source_records", "do_source_records"):
                if metrics[name] * 5 < incumbent_metrics[name] * 4:
                    raise RuntimeError("tennessee_profile_publication_volume_drop:" + name)


class TennesseeProfileCompletion(SourceProfileCompletion):
    """Report retained record totals while inheriting atomic source/control completion."""

    def _terminal_progress(self, result):
        completed = result["retained_source_records"]
        return {
            "unit": "record",
            "done": completed,
            "total": completed,
            "pct": 100,
            "phase": f"{self.importer} published",
            "message": "succeeded",
        }


store = TennesseeProfileStore(
    ProfileSourcePolicy(
        source_key=SOURCE_KEY,
        schema_version=SCHEMA_VERSION,
        jurisdiction="TN",
        categories=CATEGORIES,
        error_prefix="tennessee_profile",
        received_profile_sql="""
        normalized_payload->>'visibility' IN ('public','held_identity') AND
        CASE WHEN json_typeof(raw_payload->'rows') = 'array' THEN
          (json_array_length(raw_payload->'rows') > 0 AND json_typeof(raw_payload->'rows'->0->'fields') = 'object') OR
          CASE WHEN json_typeof(raw_payload->'malformed_rows') = 'array'
            THEN normalized_payload->>'visibility' = 'held_identity'
              AND json_array_length(raw_payload->'malformed_rows') > 0
              AND json_typeof(raw_payload->'malformed_rows'->0->'fields') = 'object'
            ELSE FALSE END
        ELSE FALSE END
    """,
        guarded_public_sql="""
        f.category = 'education' AND f.fact_type = 'education_history'
        AND json_typeof(f.value_json->'institution') = 'string'
        AND lower(btrim(regexp_replace(f.value_json->>'institution','[[:space:]]+',' ','g')))
            NOT IN ('','other','unknown','n/a','not reported')
    """,
        invalid_fact_sql="""
        (f.category = 'education' AND f.fact_type = 'education_history'
         OR f.category = 'training' AND f.fact_type = 'other_training'
         OR f.category = 'specialties' AND f.fact_type = 'specialty') IS DISTINCT FROM TRUE
        OR f.source_json->>'run_id' IS DISTINCT FROM f.run_id
        OR f.source_json->>'agency' IS DISTINCT FROM 'Tennessee Department of Health'
        OR f.source_json->>'jurisdiction' IS DISTINCT FROM 'TN'
    """,
    )
)
completion = TennesseeProfileCompletion(store, IMPORTER)
