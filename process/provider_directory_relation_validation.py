# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate freshly observed native relation metadata with the existing FHIR guards."""

from __future__ import annotations

from typing import Any


async def validate_relation_row(
    fhir, relation_row, expected_persistence: str, expected_user_trigger_count: int, *, database=None
) -> tuple[dict[str, Any], int | None]:
    """Validate freshly observed heap and TOAST metadata with the existing native guards."""
    database = fhir.db if database is None else database
    if relation_row is None:
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_profile_capacity_storage_shape_missing")
    relation_map = dict(fhir._pagination_checkpoint_row_mapping(relation_row))
    if (
        expected_user_trigger_count == 0
        and relation_map.get("relation_name") == fhir.ImportRun.__tablename__
        and (fhir.profile_initial.requested(fhir) or int(relation_map.get("user_trigger_count") or 0))
    ):
        from process.provider_directory_import_run_guards import assert_import_run_guards

        await assert_import_run_guards(database, relation_map)
        expected_user_trigger_count = 12
    if (
        expected_user_trigger_count == 0
        and relation_map.get("relation_name") == "provider_directory_profile_serving_generation"
    ):
        from process.provider_directory_cms_receipt_guard import profile_receipt_guard_count

        expected_user_trigger_count = await profile_receipt_guard_count(database, relation_map)
    has_unsafe_identity = (
        relation_map.get("relkind") != "r"
        or relation_map.get("relpersistence") != expected_persistence
        or relation_map.get("table_am") != "heap"
        or (relation_map.get("relreplident") == "f")
        or (relation_map.get("relrowsecurity") is not False)
        or (relation_map.get("relforcerowsecurity") is not False)
        or (relation_map.get("relispartition") is not False)
        or (int(relation_map.get("user_trigger_count") or 0) != expected_user_trigger_count)
        or (int(relation_map.get("rule_count") or 0) != 0)
        or (int(relation_map.get("inheritance_count") or 0) != 0)
    )
    toast_oid = int(relation_map.get("toast_oid") or 0) or None
    has_unsafe_toast = toast_oid is not None and (
        relation_map.get("toast_relkind") != "t"
        or relation_map.get("toast_relpersistence") != expected_persistence
        or relation_map.get("toast_am") != "heap"
    )
    if has_unsafe_identity or has_unsafe_toast:
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_profile_capacity_storage_shape_unsupported")
    return (relation_map, toast_oid)
