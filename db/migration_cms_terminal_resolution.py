# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind the existing abandonment guard's two terminal validators to text.

This installs nothing automatically. Its caller retains authenticated identity,
owner/ACL/hook custody, compatible consumers and the outer deadline/transaction.
"""

import hashlib
from functools import lru_cache

from db.migration_cms_noncurrent_seal import _migration
from db.migration_cms_snapshot_allocator import _INSTALLER_ADMIN_SQL, _identity
from db.migration_generic_source_candidates import _canonical_copy_guard_snapshot
from db.plan_release_storage import storage_relation

GUARD = "guard_provider_directory_subset_abandonment_dataset"
ORIGINAL_SHA256 = "d416653fb3e364905e30b0a9b4c5db61cba103c924e40424bdcec187fc226d58"
CALLEES = ("provider_directory_subset_terminal_disposition_valid", "provider_directory_subset_abandonment_valid")


def _require(condition, code):
    if not condition:
        raise RuntimeError("cms_terminal_resolution_" + code)


@lru_cache(maxsize=8)
def terminal_guard_bodies(schema):
    """Render the exact latest predecessor and only its two explicit text casts."""
    source = _migration("20260811120000_provider_directory_reviewed_subset_v5_http410_disposition.py")
    statement = source._dataset_guard_sql(schema)
    _require(statement.count("AS $function$") == statement.count("$function$;") == 1, "predecessor_changed")
    original = statement.split("AS $function$", 1)[1].split("$function$;", 1)[0]
    canonical = original.replace('"' + schema + '"', '"__schema__"').replace("'" + schema + "'", "'__schema__'")
    _require(hashlib.sha256(canonical.encode()).hexdigest() == ORIGINAL_SHA256, "predecessor_changed")
    hardened = original
    for name in CALLEES:
        call = storage_relation(schema, name) + "(NEW.dataset_id)"
        _require(hardened.count(call) == 1, "callsite_changed")
        hardened = hardened.replace(call, call[:-1] + "::pg_catalog.text)", 1)
    return original, hardened


def upgrade_cms_terminal_guard_resolution(connection, schema, *, expected):
    """Replace this exact existing body; preserve identity and leave commit to caller."""
    _require(connection.in_transaction(), "transaction_required")
    _require(
        connection.exec_driver_sql("SHOW search_path").scalar_one() in {"pg_catalog", "pg_catalog, pg_temp"},
        "search_path",
    )
    _require(connection.execute(_INSTALLER_ADMIN_SQL).scalar_one_or_none() is True, "original_admin_required")
    signature = storage_relation(schema, GUARD) + "()"
    original, hardened = terminal_guard_bodies(schema)
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
