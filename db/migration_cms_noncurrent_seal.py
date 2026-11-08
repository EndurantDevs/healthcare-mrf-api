# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Checked successor for the existing CMS header lifecycle guard.

This helper is not an automatic migration. Its shared owner must first qualify
every fixed consumer and writer; it grants no CMS publication authority.
"""

import hashlib
import importlib.util
from functools import lru_cache
from pathlib import Path

import sqlalchemy as sa

from db.migration_cms_snapshot_allocator import _INSTALLER_ADMIN_SQL, _identity
from db.migration_generic_source_candidates import _canonical_copy_guard_snapshot
from db.plan_release_storage import storage_relation

GUARD = "guard_tin_npi_connector_endpoint_dataset"
ORIGINAL_SHA256 = "d9bacbb47e3d5a3cb296a6af04c27c8970aeddbb40f6c304d6cf2badc99ba2e9"
_HOOKS_SQL = sa.text("""SELECT to_jsonb(t) AS hook FROM pg_catalog.pg_trigger t
 WHERE t.tgfoid=:function ORDER BY t.oid""")


def _require(condition, code):
    if not condition:
        raise RuntimeError("cms_noncurrent_seal_" + code)


@lru_cache(maxsize=2)
def _migration(filename):
    path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / filename
    spec = importlib.util.spec_from_file_location("_cms_seal_" + path.stem, path)
    _require(spec is not None and spec.loader is not None, "predecessor_missing")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _cms_transition_sql():
    # The guard is SECURITY DEFINER: current_user is its owner, not the caller.
    return """(
            EXISTS(SELECT FROM pg_catalog.pg_roles actor WHERE actor.rolname=session_user AND actor.rolsuper)
            AND pg_catalog.current_setting('role')='none'
            AND pg_catalog.current_setting('transaction_read_only')='off'
            AND pg_catalog.current_setting('transaction_isolation')='read committed'
            AND pg_catalog.current_setting('session_replication_role')='origin'
            AND NEW.publication_metadata_json::jsonb->'source_ids'='["cms-npd"]'::jsonb
            AND NEW.publication_metadata_summary_json->'source_ids'='["cms-npd"]'::jsonb
            AND NEW.publication_metadata_json::jsonb->'source_release'->>'source_id'='cms-npd'
            AND NEW.publication_metadata_json::jsonb->'source_release'->>'vector_sha256' ~ '^[0-9a-f]{64}$'
            AND NEW.publication_metadata_json::jsonb->'source_release'->>'manifest_sha256' ~ '^[0-9a-f]{64}$'
            AND NEW.content_proof_admission_version=1 AND NEW.content_proof_admission_kind='generic'
            AND NEW.content_proof_resource_types=ARRAY['Endpoint','HealthcareService','InsurancePlan','Location',
                'Organization','OrganizationAffiliation','Practitioner','PractitionerRole']::varchar[]
            AND OLD.is_current IS FALSE AND OLD.validated_at IS NOT NULL
            AND OLD.created_at<=OLD.validated_at AND OLD.superseded_at IS NULL
            AND NEW.superseded_at IS NULL
            AND (
                (OLD.status='validated' AND OLD.published_at IS NULL
                 AND NEW.status='published' AND NEW.is_current IS FALSE
                 AND NEW.published_at=pg_catalog.statement_timestamp() AT TIME ZONE 'UTC'
                 AND NEW.published_at>=OLD.validated_at
                 AND (pg_catalog.to_jsonb(NEW)-ARRAY['status','published_at'])=
                     (pg_catalog.to_jsonb(OLD)-ARRAY['status','published_at']))
                OR
                (OLD.status='published' AND OLD.published_at IS NOT NULL
                 AND OLD.published_at>=OLD.validated_at
                 AND NEW.status='published' AND NEW.is_current IS TRUE
                 AND (pg_catalog.to_jsonb(NEW)-'is_current')=(pg_catalog.to_jsonb(OLD)-'is_current'))
            )
        )"""


@lru_cache(maxsize=8)
def lifecycle_guard_bodies(schema):
    """Render the exact installed predecessor and its one narrow successor."""
    compact = _migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    repairs = _migration("20261016050000_acquisition_validator_argument_types.py")
    original = compact._function_body(compact._endpoint_guard_sql(schema, compact=True))
    calls = [entry for entry in repairs._calls(schema) if entry[0] == GUARD]
    _require(len(calls) == 4, "predecessor_changed")
    for _caller, old, new, count in calls:
        _require(original.count(old) == count, "predecessor_changed")
        original = original.replace(old, new)
    canonical = original.replace('"' + schema + '"', '"__schema__"').replace("'" + schema + "'", "'__schema__'")
    _require(hashlib.sha256(canonical.encode()).hexdigest() == ORIGINAL_SHA256, "predecessor_changed")
    subset = compact._terminal()._subset()
    lifecycle = subset._endpoint_dataset_lifecycle_sql(
        subset._SUBSET_IMMUTABLE_COMPARISON_SQL, subset_metadata_is_immutable=True
    )
    anchor = "IF " + lifecycle + " THEN"
    _require(original.count(anchor) == 1, "predecessor_changed")
    successor = original.replace(
        anchor, "IF (" + lifecycle + ") AND " + _cms_transition_sql() + " IS DISTINCT FROM TRUE THEN", 1
    )
    return original, successor


def _snapshot(connection, schema, expected, bodies):
    observed = _canonical_copy_guard_snapshot(connection, storage_relation(schema, GUARD) + "()")
    _identity(observed, expected, bodies)
    identity = observed["identity"]
    _require(
        identity["pronargs"] == 0
        and identity["proargtypes"] == []
        and identity["proargnames"] is None
        and identity["proargmodes"] is None
        and identity["proallargtypes"] is None
        and identity["proargdefaults"] is None
        and identity["pronargdefaults"] == 0
        and int(identity["provariadic"]) == 0
        and str(identity["prosupport"]) in {"0", "-"}
        and identity["probin"] is None
        and identity["prosqlbody"] is None
        and identity["protrftypes"] is None
        and not identity["proleakproof"]
        and not identity["proisstrict"]
        and not identity["proretset"]
        and identity["provolatile"] == "v"
        and identity["proparallel"] == "u"
        and identity["procost"] == 100
        and identity["prorows"] == 0,
        "function_properties_changed",
    )
    _require(
        connection.scalar(
            sa.text("SELECT count(*)=1 FROM pg_catalog.pg_proc WHERE pronamespace=:namespace AND proname=:name"),
            {"namespace": int(identity["pronamespace"]), "name": GUARD},
        )
        is True,
        "overload_changed",
    )
    hooks = connection.execute(_HOOKS_SQL, {"function": expected["oid"]}).scalars().all()
    _require(
        len(hooks) == 1
        and hooks[0]["tgname"] == "tin_npi_connector_endpoint_dataset_guard"
        and hooks[0]["tgtype"] == 31
        and hooks[0]["tgenabled"] in {"O", "A"}
        and not hooks[0]["tgisinternal"]
        and hooks[0]["tgnargs"] == 0
        and hooks[0]["tgqual"] is None
        and not hooks[0]["tgdeferrable"]
        and not hooks[0]["tginitdeferred"]
        and hooks[0]["tgoldtable"] is None
        and hooks[0]["tgnewtable"] is None
        and int(hooks[0]["tgrelid"])
        == connection.scalar(
            sa.text("SELECT pg_catalog.to_regclass(:header)::oid::bigint"),
            {"header": storage_relation(schema, "provider_directory_endpoint_dataset")},
        ),
        "hook_changed",
    )
    return observed, hooks


def upgrade_cms_noncurrent_seal(connection, schema, *, expected):
    """Replace only an exact approved body, preserving OID, owner, ACL and hooks.

    The caller owns the outer transaction and accepted common transition. This
    does not commit, issue a manifest, alter privileges or waive serving guards.
    """
    _require(connection.in_transaction(), "transaction_required")
    _require(
        connection.exec_driver_sql("SHOW search_path").scalar_one() in {"pg_catalog", "pg_catalog, pg_temp"},
        "search_path",
    )
    _require(connection.execute(_INSTALLER_ADMIN_SQL).scalar_one_or_none() is True, "original_admin_required")
    old, new = lifecycle_guard_bodies(schema)
    before, hooks = _snapshot(connection, schema, expected, (old, new))
    if before["body"] == new:
        return
    _require(before["definition"].count(old) == 1, "definition_changed")
    with connection.begin_nested():
        connection.exec_driver_sql(before["definition"].replace(old, new, 1))
        after, after_hooks = _snapshot(connection, schema, expected, (new,))
        _require(
            after["identity"] == before["identity"]
            and after["description"] == before["description"]
            and after_hooks == hooks,
            "identity_changed",
        )
