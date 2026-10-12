# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed company-listing evidence and edition-wide typed identity resolution."""

import hashlib
import json

from process.registry_naic_source import (
    HEADERS_BY_MEMBER,
    LAYOUTS_BY_MEMBER,
    MEMBERS,
    PARSER_VERSION,
    _canonical,
    _is_digest,
)
from process.registry_source_admission import RegistrySourceAdmissionError


def validate_naic_manifest(encoded, input_sha256):
    """Validate a closed, ordered member witness against its edition digest."""
    if type(encoded) is not str or len(encoded.encode()) > 8192:
        raise RegistrySourceAdmissionError("Company listing member witness is invalid")
    manifest = json.loads(encoded)
    if type(manifest) is not list or len(manifest) != 3 or _canonical(manifest).decode() != encoded:
        raise RegistrySourceAdmissionError("Company listing member witness is invalid")
    for member, name in zip(manifest, MEMBERS):
        if (
            type(member) is not dict
            or set(member)
            != {"member_name", "member_sha256", "member_bytes", "header_sha256", "layout", "layout_pdf_sha256"}
            or member["member_name"] != name
            or member["layout"] != LAYOUTS_BY_MEMBER[name]
            or type(member["member_bytes"]) is not int
            or not 0 < member["member_bytes"] <= 8 * 1024 * 1024
            or not all(_is_digest(member[field]) for field in ("member_sha256", "header_sha256", "layout_pdf_sha256"))
            or member["header_sha256"]
            != hashlib.sha256(json.dumps(HEADERS_BY_MEMBER[name], separators=(",", ":")).encode()).hexdigest()
        ):
            raise RegistrySourceAdmissionError("Company listing member witness differs")
    if hashlib.sha256(encoded.encode()).hexdigest() != input_sha256:
        raise RegistrySourceAdmissionError("Company listing combined input digest differs")
    return manifest


def naic_content_sql(landing, namespace):
    """Check native observations against registered edition and raw member witnesses."""
    manifest = validate_naic_manifest(landing.member_manifest_json, landing.input_sha256)
    members = "'" + _canonical(manifest).decode().replace("'", "''") + "'::jsonb"
    evidence = "observation_json->'source_evidence'"
    checks = [
        "snapshot_id=$1",
        "source_row_number>=2",
        "status IN ('accepted','unresolved','rejected')",
        "jsonb_typeof(observation_json)='object'",
        "jsonb_typeof(issues_json)='array'",
        "observation_json->>'source_record_key'=source_record_key",
        "observation_json->>'source_row_number'=source_row_number::text",
        "source_record_key=(observation_json->'source_evidence'->>'member_name')||':row:'||source_row_number::text",
        "observation_json->>'status'=status",
        "observation_json->'issues'=issues_json",
        "jsonb_typeof(observation_json->'raw_fields')='object'",
        "observation_json->'valid_from'='null'::jsonb",
        "observation_json->'valid_to'='null'::jsonb",
        "observation_json->'hios' IS NULL",
        f"observation_json->>'row_kind'=CASE WHEN {evidence}->>'member_name'='GPAL.csv' THEN 'group' ELSE 'company' END",
        f"{evidence}->>'parser_version'='{PARSER_VERSION}'",
        f"{evidence}->>'artifact_sha256'=(SELECT artifact_sha256 FROM {namespace}.registry_source_snapshot WHERE snapshot_id=$1)",
        f"{evidence}->>'raw_record_sha256' ~ '^[0-9a-f]{{64}}$'",
        f"jsonb_typeof({evidence}->'raw_fields_hex')='object'",
        f"""CASE WHEN {evidence}->>'source_byte_start' ~ '^[0-9]{{1,8}}$' AND {evidence}->>'source_byte_end' ~ '^[0-9]{{1,8}}$' THEN
          ({evidence}->>'source_byte_end')::bigint>({evidence}->>'source_byte_start')::bigint ELSE false END""",
        f"""EXISTS(SELECT 1 FROM jsonb_array_elements({members}) m WHERE m->>'member_name'={evidence}->>'member_name'
          AND m->>'member_sha256'={evidence}->>'member_sha256' AND m->>'layout'={evidence}->>'layout'
          AND m->>'layout_pdf_sha256'={evidence}->>'layout_pdf_sha256' AND m->>'header_sha256'={evidence}->>'header_sha256'
          AND CASE WHEN {evidence}->>'source_byte_end' ~ '^[0-9]{{1,8}}$' THEN
            ({evidence}->>'source_byte_end')::bigint<=(m->>'member_bytes')::bigint ELSE false END)""",
        """CASE WHEN jsonb_typeof(issues_json)='array' THEN CASE status WHEN 'accepted' THEN jsonb_array_length(issues_json)=0
          WHEN 'unresolved' THEN jsonb_array_length(issues_json)>0 AND NOT jsonb_path_exists(issues_json,'$[*] ? (@.rejecting == true)')
          WHEN 'rejected' THEN jsonb_path_exists(issues_json,'$[*] ? (@.rejecting == true)') ELSE false END ELSE false END""",
    ]
    for field, pattern, raw in (
        ("normalized_ein", "^[0-9]{9}$", "FEIN NUMBER"),
        ("normalized_naic_company", "^[0-9]{5}$", "COMPANY CODE"),
        ("normalized_naic_group", "^[1-9][0-9]{0,4}$", "GROUP CODE"),
    ):
        checks.append(f"""(observation_json->'{field}'='null'::jsonb OR (jsonb_typeof(observation_json->'{field}')='string'
          AND observation_json->>'{field}' ~ '{pattern}' AND observation_json->>'{field}' !~ '^0+$'
          AND jsonb_typeof(observation_json->'raw_fields'->'{raw}')='string'))""")
    return " AND ".join(f"COALESCE(({check}),false)" for check in checks)


def naic_resolution_sql(namespace, relation):
    """Reduce all members before identity allocation; names never join identities."""
    return f"""WITH fields AS (
      SELECT l.*,observation_json->>'normalized_ein' AS ein,
        observation_json->>'normalized_naic_company' AS naic_company,
        observation_json->>'normalized_naic_group' AS group_code,
        NULL::text AS hios,NULL::text AS state,false AS declared_issuer_conflict,
        observation_json->'source_evidence'->>'member_name' AS member_name,
        NULLIF(regexp_replace(btrim(translate(observation_json->'raw_fields'->>'COMPANY NAME',$1,repeat(' ',char_length($1)))),' +',' ','g'),'') AS legal_name,
        COALESCE(regexp_replace(btrim(translate(observation_json->'raw_fields'->>'GROUP NAME',$1,repeat(' ',char_length($1)))),' +',' ','g'),'') AS group_label
      FROM {relation} l),
    company_conflicts AS (
      SELECT naic_company FROM fields WHERE naic_company IS NOT NULL GROUP BY naic_company
      HAVING COUNT(DISTINCT ein)>1 OR COUNT(DISTINCT group_code)>1
        OR bool_or(jsonb_path_exists(issues_json,'$[*] ? (@.code == "conflicting_company_identifier_evidence")'))),
    ein_conflicts AS (
      SELECT ein FROM fields WHERE ein IS NOT NULL GROUP BY ein HAVING COUNT(DISTINCT naic_company)>1),
    bound AS (
      SELECT f.*,e.entity_id AS ein_company_id,n.entity_id AS naic_binding_id,g.entity_id AS group_id
      FROM fields f LEFT JOIN {namespace}.registry_identifier_binding e
        ON e.entity_kind='company' AND e.identifier_system='ein' AND e.identifier_value=f.ein
      LEFT JOIN {namespace}.registry_identifier_binding n
        ON n.entity_kind='company' AND n.identifier_system='naic_company' AND n.identifier_value=f.naic_company
      LEFT JOIN {namespace}.registry_identifier_binding g
        ON g.entity_kind='group' AND g.identifier_system='naic_group' AND g.identifier_value=f.group_code),
    affected AS (
      SELECT naic_company FROM company_conflicts UNION SELECT naic_company FROM bound
      WHERE ein IN (SELECT ein FROM ein_conflicts) OR ein_company_id<>naic_binding_id),
    flagged AS (
      SELECT b.*,COALESCE(naic_company IN (SELECT naic_company FROM affected),false)
        OR COALESCE(ein IN (SELECT ein FROM ein_conflicts),false) AS company_conflicting FROM bound b)
    SELECT f.*,CASE WHEN status<>'rejected' AND NOT company_conflicting
      THEN COALESCE(naic_binding_id,ein_company_id) END AS company_id,
      CASE WHEN status<>'rejected' AND NOT company_conflicting THEN naic_binding_id END AS naic_company_id
    FROM flagged f"""


async def persist_naic_relationships(connection, namespace, staging):
    """Retain reported affiliations with unknown dates; never create HIOS issuers."""
    await connection.execute(f"""INSERT INTO {namespace}.registry_company_group_assertion
      (snapshot_id,source_record_key,company_id,group_id,relationship_kind,resolution_status,valid_from,valid_to)
      SELECT snapshot_id,source_record_key,company_id,group_id,'reported_affiliation',
        CASE WHEN group_id IS NULL THEN 'unresolved' ELSE 'resolved' END,NULL,NULL
      FROM {staging} WHERE status<>'rejected' AND NOT company_conflicting AND company_id IS NOT NULL
        AND group_code IS NOT NULL AND observation_json->>'relationship_kind'='reported_affiliation'""")
