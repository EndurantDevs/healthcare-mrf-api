# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read immutable Profile history across exact, known metadata-only migrations."""

import hashlib
from copy import deepcopy

from process import provider_directory_profile_initial_guards as guards

_CONSUMPTION = "provider_directory_profile_capacity_lease_consumption"
_SERVING = "provider_directory_profile_serving_generation"
_METADATA = {
    "serving_generation_storage_fingerprint": (_SERVING, "serving_generation_oid"),
    "capacity_consumption_storage_fingerprint": (_CONSUMPTION, "capacity_consumption_oid"),
}
_OLD_PURPOSE = (
    "CHECK (admission_purpose::text = ANY (ARRAY['profile'::character varying, "
    "'cms_nonprofile'::character varying]::text[]))"
)
_NEW_PURPOSE = (
    "CHECK (admission_purpose::text = ANY (ARRAY['profile'::character varying, "
    "'cms_nonprofile'::character varying, 'serving_nonprofile'::character varying]::text[]))"
)


_INVERSE_PURPOSE = (
    "CHECK (admission_purpose::text = ANY (ARRAY['profile'::character varying, "
    "'cms_nonprofile'::character varying, 'serving_nonprofile'::character varying, "
    "'serving_inverse'::character varying]::text[]))"
)


def _consumption_purpose(payload):
    purpose_checks = [
        item["constraint_definition"]
        for item in payload["constraints"]
        if item["constraint_definition"] in {_NEW_PURPOSE, _INVERSE_PURPOSE}
    ]
    if len(purpose_checks) != 1:
        raise RuntimeError("provider_directory_profile_history_catalog_changed")
    return purpose_checks[0]


def _replace_check(payload, before, after):
    checks = payload["constraints"]
    matches = [constraint for constraint in checks if constraint["constraint_definition"] == before]
    if len(matches) != 1 or matches[0]["constraint_type"] != "c" or matches[0]["convalidated"] is not True:
        raise RuntimeError("provider_directory_profile_history_catalog_changed")
    matches[0]["constraint_definition"] = after
    checks.sort(
        key=lambda constraint: (
            constraint["relation_oid"],
            constraint["constraint_type"],
            constraint["constraint_definition"],
        )
    )


def previous_serving_fingerprint_payload(payload, *, schema):
    """Project the exact successor body only, preserving every native identity and definition."""
    from process.provider_directory_profile_initial_inverse_guard import body

    restored = deepcopy(payload)
    relation = restored["relation"]
    if relation["schema_name"] != schema or relation["relation_name"] != _SERVING:
        raise RuntimeError("provider_directory_profile_history_catalog_changed")
    matches = [
        entry for entry in restored["triggers"] if entry.get("trigger_name") == "pd_profile_initial_serving_insert"
    ]
    if len(matches) != 1:
        raise RuntimeError("provider_directory_profile_history_catalog_changed")
    trigger = matches[0]
    current, original = " " + body(schema) + " ", " " + guards._serving_insert_body(schema) + " "
    if trigger["trigger_function_source"] != current or trigger["trigger_function_definition"].count(current) != 1:
        raise RuntimeError("provider_directory_profile_history_catalog_changed")
    trigger["trigger_function_source"] = original
    trigger["trigger_function_definition"] = trigger["trigger_function_definition"].replace(current, original)
    return restored


def original_fingerprint_payload(catalog_payload, *, schema, table):
    """Project only authenticated known changes; keep every physical identity and other guard."""
    restored = deepcopy(catalog_payload)
    relation = restored["relation"]
    if relation["schema_name"] != schema or relation["relation_name"] != table:
        raise RuntimeError("provider_directory_profile_history_catalog_changed")
    if table == _CONSUMPTION:
        _replace_check(restored, _consumption_purpose(restored), _OLD_PURPOSE)
        return restored
    if table not in {guards.RECEIPT_TABLE, _SERVING}:
        raise RuntimeError("provider_directory_profile_history_catalog_changed")
    if table == _SERVING:
        from process.provider_directory_profile_initial_inverse_guard import body

        trigger_sources = [entry.get("trigger_function_source") for entry in restored["triggers"]]
        if " " + body(schema) + " " in trigger_sources:
            restored = previous_serving_fingerprint_payload(restored, schema=schema)
    dependency = restored["guard_dependencies"]
    current_source = " " + guards._reactivated_receipt_matches_sql(schema) + " "
    if dependency["function_source_sha256"] != hashlib.sha256(current_source.encode()).hexdigest():
        raise RuntimeError("provider_directory_profile_history_catalog_changed")
    original_source = " " + guards._receipt_matches_sql(schema) + " "
    dependency["function_source_sha256"] = hashlib.sha256(original_source.encode()).hexdigest()
    if table == guards.RECEIPT_TABLE:
        _replace_check(restored, guards._REACTIVATED_KEY_CHECK, guards._LEGACY_KEY_CHECK)
        keys = [
            attribute
            for attribute in restored["attributes"]
            if attribute["relation_oid"] == relation["relation_oid"] and attribute["attname"] == "singleton_key"
        ]
        if len(keys) != 1 or keys[0]["default_expression"] != guards._REACTIVATED_KEY_DEFAULT:
            raise RuntimeError("provider_directory_profile_history_catalog_changed")
        keys[0]["default_expression"] = guards._LEGACY_KEY_DEFAULT
    return restored


async def is_historical_fingerprint(fhir, schema, table, oid, expected):
    """Compatibility is a readback check, never a fresh admission layout."""
    from process.provider_directory_cms_nonprofile_observation import catalog_observation

    if table not in {guards.RECEIPT_TABLE, _SERVING, _CONSUMPTION}:
        return False
    reader = catalog_observation(fhir, None)
    trigger_count = 3 if table == guards.RECEIPT_TABLE else 2 if table == _CONSUMPTION else 0
    immutable_error = "provider_directory_profile_capacity_consumption_immutable" if table == _CONSUMPTION else None
    relation, toast = await reader._profile_capacity_relation_row(oid, "p", trigger_count)
    attributes, indexes, constraints, triggers = await reader._profile_capacity_relation_catalog(
        [oid] if toast is None else [oid, toast]
    )
    dependency = await fhir.profile_capacity_projection.assert_relation_guards(
        reader, relation, triggers, oid, attributes, indexes, constraints, (trigger_count, immutable_error, False)
    )
    catalog_payload, _structure = fhir._profile_capacity_fingerprint_payloads(
        relation, attributes, indexes, constraints, triggers, oid
    )
    if dependency is not None:
        catalog_payload["guard_dependencies"] = dependency
    try:
        historical = original_fingerprint_payload(catalog_payload, schema=schema, table=table)
    except KeyError, RuntimeError:
        return False
    candidates = [historical]
    if table == _SERVING:
        try:
            previous = previous_serving_fingerprint_payload(catalog_payload, schema=schema)
        except KeyError, RuntimeError:
            previous = None
        if previous is not None:
            candidates.append(previous)
    if table == _CONSUMPTION and _consumption_purpose(catalog_payload) == _INVERSE_PURPOSE:
        previous = deepcopy(historical)
        _replace_check(previous, _OLD_PURPOSE, _NEW_PURPOSE)
        candidates.append(previous)
    return any(fhir._identity_hash(candidate) == expected for candidate in candidates)


async def is_historical_metadata(fhir, schema, observed, expected):
    """Permit only known metadata migrations after checking unchanged identities."""
    if observed.keys() != expected.keys():
        return False
    changed_fields = {name for name in expected if observed[name] != expected[name]}
    if not changed_fields <= _METADATA.keys():
        return False
    for name in changed_fields:
        table, oid_field = _METADATA[name]
        if oid_field not in expected or not await is_historical_fingerprint(
            fhir, schema, table, expected[oid_field], expected[name]
        ):
            return False
    return True


async def assert_replay_database(fhir, schema, serving, geometry, lease):
    """Verify the replay still targets the database signed by its lease."""
    database_identity = await fhir._provider_directory_profile_capacity_database_identity(
        schema,
        serving,
    )
    geometry_fields = (
        "database_system_identifier",
        "database_oid",
        "database_name",
        "tablespace_oid",
        "tablespace_name",
        "postgres_server_version_num",
        "postgres_block_size_bytes",
        "postgres_wal_block_size_bytes",
        "postgres_wal_segment_size_bytes",
        "postgres_full_page_writes",
        "postgres_wal_compression",
        "postgres_wal_level",
        "postgres_wal_log_hints",
        "postgres_data_checksums",
        "postgres_default_toast_compression",
        "postgres_checkpoint_timeout_seconds",
        "postgres_max_wal_size_bytes",
        "evidence_target_storage_fingerprint",
        "profile_target_storage_fingerprint",
        "build_checkpoint_oid",
        "serving_generation_oid",
        "delta_receipt_oid",
        "build_checkpoint_storage_fingerprint",
        "serving_generation_storage_fingerprint",
        "delta_receipt_storage_fingerprint",
    )
    expected_by_field = {field_name: getattr(geometry, field_name) for field_name in geometry_fields}
    observed_by_field = {field_name: getattr(database_identity, field_name) for field_name in expected_by_field}
    if not await is_historical_metadata(fhir, schema, observed_by_field, expected_by_field):
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_profile_replay_database_changed")
    fhir._assert_provider_directory_profile_capacity_tablespaces(
        lease,
        database_identity,
    )
