# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite proof ACL recognition retains exact Source, body and profile bindings."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from process import provider_directory_cms_callee_inventory as inventory
from process.provider_directory_cms_proof_contract import CMS_PROOF_DEFINERS
from tests.test_provider_directory_cms_callee_custody import _catalog_rows
from tests.test_provider_directory_cms_callee_custody import _verify as legacy_verify
from tests.test_provider_directory_cms_callee_inventory import _arguments, _namespace_source_rows, _source_metadata

_OWNER = 104
_ACTORS = (103, 201, 301, 302)
_PROOFS = (
    "provider_directory_subset_abandonment_valid",
    "provider_directory_subset_terminal_disposition_valid",
    "provider_directory_terminal_root_retirement_evidence",
    "provider_directory_terminal_root_retirement_v2_evidence",
)


def _role(oid, name, login=False):
    return dict(
        oid=oid,
        rolname=name,
        rolcanlogin=login,
        rolsuper=False,
        rolcreatedb=False,
        rolcreaterole=False,
        rolreplication=False,
        rolbypassrls=False,
        elevated=False,
    )


@pytest.fixture
def source_case():
    rows = _namespace_source_rows(_catalog_rows("snapshot_schema"))
    roles = [
        _role(104, "namespace_owner"),
        _role(103, "legacy_writer", True),
        _role(201, "private_owner"),
        _role(301, "publication_owner"),
        _role(302, "publication_publisher", True),
    ]
    document_by_field = dict(
        version=2,
        policy=dict(namespace_owner_role="namespace_owner", owner_role="private_owner"),
        publication=dict(owner_role="publication_owner", publisher_role="publication_publisher"),
        role_graph=dict(roles=deepcopy(roles), memberships=[]),
        storage_custody=dict(
            coordinates=dict(namespace_owner_oid=104, owner_oid=201),
            roles={role["rolname"]: deepcopy(role) for role in roles if role["oid"] != 103},
            canonical_authority=dict(metadata=_source_metadata(rows)),
        ),
    )
    return SimpleNamespace(rows=rows, document=document_by_field)


def _proof_row(case, name):
    row = next(row for row in case.rows if row["definition"]["proname"] == name)
    row["definition"]["proowner"] = str(_OWNER)
    row["acl"] = [[_OWNER, _OWNER, "EXECUTE", False]]
    return row


def _verify(case, *, pins=None):
    arguments = _arguments(case.rows)
    if pins is not None:
        arguments["expected_functions"] = pins
    return inventory.require_cms_source_callee_inventory(case.rows, **arguments, source_document=case.document)


@pytest.mark.parametrize("name", _PROOFS)
@pytest.mark.parametrize("actor", _ACTORS)
def test_namespace_proof_accepts_only_bound_current_source_actor(source_case, name, actor):
    row = _proof_row(source_case, name)
    row["acl"].append([actor, _OWNER, "EXECUTE", False])
    before = deepcopy(source_case)
    assert len(_verify(source_case)) == 32
    assert source_case == before
    with pytest.raises(ValueError, match="acl_changed"):
        inventory.require_cms_callee_inventory(
            source_case.rows, **_arguments(source_case.rows), source_metadata=_source_metadata(source_case.rows)
        )


def test_source_document_keeps_existing_owner_only_frontier(source_case):
    assert len(_verify(source_case)) == 32


def test_finite_proof_set_does_not_expand_fixed32(source_case):
    assert set(_PROOFS) < CMS_PROOF_DEFINERS
    assert len(_verify(source_case)) == 32
    assert CMS_PROOF_DEFINERS.difference(_PROOFS) == {
        "provider_directory_terminal_root_retirement_valid",
        "provider_directory_terminal_root_retirement_v2_valid",
    }


@pytest.mark.parametrize(
    "name",
    ["provider_directory_terminal_root_retirement_valid", "provider_directory_terminal_root_retirement_v2_valid"],
)
def test_source_approved_supplements_are_not_fixed32_entries(source_case, name):
    row = deepcopy(_proof_row(source_case, _PROOFS[0]))
    row["definition"].update(proname=name, oid="9999")
    row["signature"] = '"snapshot_schema"."' + name + '"(text)'
    source_case.rows.append(row)
    with pytest.raises(ValueError, match="callee_inventory_required"):
        _verify(source_case)


@pytest.mark.parametrize("name", _PROOFS)
@pytest.mark.parametrize("fault", ["public", "unknown", "grantor", "option", "missing", "duplicate", "owner", "body"])
def test_repinned_proof_still_requires_exact_contract(source_case, name, fault):
    row = _proof_row(source_case, name)
    row["acl"].append([103, _OWNER, "EXECUTE", False])
    match fault:
        case "public" | "unknown":
            row["acl"][-1][0] = 0 if fault == "public" else 999
        case "grantor":
            row["acl"][-1][1] = 999
        case "option":
            row["acl"][-1][-1] = True
        case "missing":
            row["acl"].pop(0)
        case "duplicate":
            row["acl"].append(deepcopy(row["acl"][-1]))
        case "owner":
            row["definition"]["proowner"] = "105"
            row["acl"] = [[105, 105, "EXECUTE", False], [103, 105, "EXECUTE", False]]
        case "body":
            row["definition"]["prosrc"] += " changed"
    with pytest.raises(ValueError, match="acl_changed|proof_source_required|definition_changed"):
        _verify(source_case)


def test_proof_native_acl_remains_bound_to_original_profile_pin(source_case):
    row = _proof_row(source_case, _PROOFS[0])
    pins = _arguments(source_case.rows)["expected_functions"]
    row["acl"].append([103, _OWNER, "EXECUTE", False])
    with pytest.raises(ValueError, match="callee_inventory_changed"):
        _verify(source_case, pins=pins)


@pytest.mark.parametrize("field", ["oid", "proowner", "prosrc", "language", "acl"])
def test_document_source5_must_match_current_complete_native_rows(source_case, field):
    row = _proof_row(source_case, _PROOFS[0])
    row["acl"].append([103, _OWNER, "EXECUTE", False])
    metadata = next(iter(source_case.document["storage_custody"]["canonical_authority"]["metadata"].values()))
    if field == "acl":
        metadata["acl"] = [
            [_OWNER, _OWNER, "EXECUTE", False],
            [103, _OWNER, "EXECUTE", False],
            [999, _OWNER, "EXECUTE", False],
        ]
    elif field == "language":
        metadata["language"] = "unbound"
    else:
        metadata["definition"][field] = "unbound" if field == "prosrc" else "999"
    with pytest.raises(ValueError, match="metadata_caller_unbound|source_metadata_changed"):
        _verify(source_case)


@pytest.mark.parametrize("fault", ["version", "missing", "role_graph", "roles"])
def test_source_document_entrypoint_rejects_missing_v2_binding(source_case, fault):
    match fault:
        case "version":
            source_case.document["version"] = 1
        case "missing":
            source_case.document = None
        case "role_graph":
            source_case.document["role_graph"] = None
        case "roles":
            source_case.document["role_graph"]["roles"] = None
    with pytest.raises(ValueError, match="source_document_required"):
        _verify(source_case)


def test_unrelated_fixed_callees_keep_owner_only_acl(source_case):
    row = next(
        row
        for row in source_case.rows
        if row["definition"]["proname"] == "provider_directory_subset_coverage_shape_valid"
    )
    row["definition"]["proowner"] = str(_OWNER)
    row["acl"] = [[_OWNER, _OWNER, "EXECUTE", False], [103, _OWNER, "EXECUTE", False]]
    with pytest.raises(ValueError, match="proof_source_required"):
        _verify(source_case)


def test_legacy_role_labels_do_not_authorize_source2_proof_callers():
    rows = _catalog_rows("snapshot_schema")
    row = next(row for row in rows if row["definition"]["proname"] == _PROOFS[0])
    row["acl"].append([103, int(row["definition"]["proowner"]), "EXECUTE", False])
    with pytest.raises(ValueError, match="acl_changed"):
        legacy_verify(rows)
