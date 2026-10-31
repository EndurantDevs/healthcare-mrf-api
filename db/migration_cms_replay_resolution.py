# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Checked exact-type calls in the existing separate header replay guard.

This installs nothing automatically. The common owner must qualify all body
consumers before using the updater; creator and full writer custody stay separate.
"""

import hashlib
from functools import lru_cache

from db.migration_cms_noncurrent_seal import _migration
from db.migration_cms_snapshot_allocator import _INSTALLER_ADMIN_SQL, _identity
from db.migration_generic_source_candidates import _canonical_copy_guard_snapshot
from db.plan_release_storage import storage_relation

GUARD = "guard_pd_endpoint_dataset_subset_replay_evidence"
ORIGINAL_SHA256 = "1bad9b653fb789345f534559c7818b5e251b5ba4e5b6c1042e5f82629c7126c4"


def _require(condition, code):
    if not condition:
        raise RuntimeError("cms_replay_resolution_" + code)


@lru_cache(maxsize=8)
def replay_guard_bodies(schema):
    """Preserve the complete original body and cast the two varchar arguments."""
    migration = _migration("20260812020000_provider_directory_endpoint_dataset_admission_seal.py")
    source = migration._replay_guard_function_sql(schema)
    original = source.split("AS $function$", 1)[1].split("$function$;", 1)[0]
    normalized = original.replace('"' + schema + '"', '"__schema__"').replace("'" + schema + "'", "'__schema__'")
    _require(hashlib.sha256(normalized.encode()).hexdigest() == ORIGINAL_SHA256, "predecessor_changed")
    argument = "\n            completion_proof_sha256"
    _require(original.count(argument) == 2, "predecessor_changed")
    return original, original.replace(argument, argument + "::pg_catalog.text")


def upgrade_cms_replay_guard_resolution(connection, schema, *, expected):
    """Replace only the approved body, retaining OID, owner, ACL, hooks and comment.

    The original independent Admin owns the exact protected identity, paired
    consumer transition, caller deadline and outer commit/rollback. No profile,
    grant, manifest, role or trigger change is performed here.
    """
    _require(connection.in_transaction(), "transaction_required")
    _require(
        connection.exec_driver_sql("SHOW search_path").scalar_one() in {"pg_catalog", "pg_catalog, pg_temp"},
        "search_path",
    )
    _require(connection.execute(_INSTALLER_ADMIN_SQL).scalar_one_or_none() is True, "original_admin_required")
    signature = storage_relation(schema, GUARD) + "()"
    original, hardened = replay_guard_bodies(schema)
    before = _canonical_copy_guard_snapshot(connection, signature)
    _identity(before, expected, (original, hardened))
    if before["body"] == hardened:
        return
    _require(before["definition"].count(original) == 1, "definition_changed")
    with connection.begin_nested():
        connection.exec_driver_sql(before["definition"].replace(original, hardened, 1))
        after = _canonical_copy_guard_snapshot(connection, signature)
        _identity(after, expected, (hardened,))
        _require(
            after["identity"] == before["identity"] and after["description"] == before["description"],
            "identity_changed",
        )
