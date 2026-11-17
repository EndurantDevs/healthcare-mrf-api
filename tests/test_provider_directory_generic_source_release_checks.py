# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit catalog transport and original-task binding; native authority is verified separately."""

import asyncio
import re
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.migration_generic_source_candidates import _canonical_copy_guard_bodies, _cms_canonical_copy_guard_body
from db.migration_provider_directory_dataset_candidates import candidate_functions
from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_provision as provision
from process import provider_directory_generic_source_release_checks as checks
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.test_provider_directory_source_callable_custody import _entry
from tests.test_source_validator_argument_types_migration import _load


def legacy_function_rows(monkeypatch):
    """Use genuine migration bodies with synthetic pg_proc/ACL transport rows."""
    statements = [candidate_functions("source_schema")]
    migration = _load("20260930010000_provider_directory_resource_identity.py")
    monkeypatch.setattr(migration, "_schema", lambda: '"source_schema"')
    monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
    migration.upgrade()
    pattern = re.compile(
        r'CREATE FUNCTION "source_schema"\."?(\w+)"?\((.*?)\)\s+RETURNS\s+(.*?)\s+LANGUAGE\s+(.*?)AS \$(\w*)\$(.*?)\$\5\$',
        re.S,
    )
    bodies_by_signature = {
        name + "(" + ",".join(arg.strip().split(None, 1)[1] for arg in args.split(",") if arg) + ")": body
        for name, args, _returns, _options, _delimiter, body in pattern.findall("\n".join(statements))
    }
    bodies_by_signature.pop("guard_pd_dataset_migration_owner()")
    assert bodies_by_signature.keys() == checks._LEGACY_FUNCTIONS.keys()
    return [
        legacy_catalog_entry(index, signature, spec, bodies_by_signature[signature])
        for index, (signature, spec) in enumerate(checks._LEGACY_FUNCTIONS.items(), 501)
    ]


def legacy_catalog_entry(index, signature, spec, body):
    oid_by_type = {"trigger": "2279", "void": "2278", "text[]": "1009", "text": "25", "jsonb": "3802"}
    args, returns, _outputs, language, volatility, strict, parallel, security, _digest = spec
    definition = _entry("guard_provider_directory_terminal_root_retirement_child()")["definition"]
    definition.update(
        oid=str(index),
        proname=signature.split("(")[0],
        proowner="105",
        pronamespace="201",
        prorettype=oid_by_type[returns],
        pronargs=len(args),
        proargtypes=[oid_by_type[kind] for _, kind in args],
        proargnames=[name for name, _ in args] or None,
        prosrc=body,
        provolatile=volatility,
        proisstrict=strict,
        proparallel=parallel,
        prosecdef=security,
    )
    acl_rows = [[105, 105, "EXECUTE", False]]
    if signature == "guard_provider_directory_resource_identity()" or "_rooted_dataset_candidate(" in signature:
        acl_rows.append([0, 105, "EXECUTE", False])
    if "_practitioner_dataset_candidate(" in signature:
        acl_rows.append([103, 105, "EXECUTE", False])
    return dict(
        signature=storage_relation("source_schema", signature.split("(")[0]) + "(" + signature.split("(")[1],
        definition=definition,
        language=language,
        acl=acl_rows,
    )


def allocator_guard_rows(monkeypatch, *, guard_version="v0", additional_execute=True):
    """Retain real source bodies while modeling the exact protected allocator postimage."""
    rows = legacy_function_rows(monkeypatch)
    by_name = {row["definition"]["proname"]: row for row in rows}
    allocator = by_name["prepare_pd_generic_dataset_storage"]
    allocator["definition"]["proowner"] = "104"
    allocator["acl"] = [[104, 104, "EXECUTE", False]]
    guard = by_name["guard_pd_dataset_candidate_parent"]
    if guard_version == "v1":
        guard["definition"]["prosrc"] = _canonical_copy_guard_bodies("source_schema", "hp_snapshot_retention")[1]
    elif guard_version == "v2":
        guard["definition"]["prosrc"] = _cms_canonical_copy_guard_body("source_schema", "hp_snapshot_retention")
    if additional_execute:
        guard["acl"].append([104, 105, "EXECUTE", False])
    return rows, allocator, guard
