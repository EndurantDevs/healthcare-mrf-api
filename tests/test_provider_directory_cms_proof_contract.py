# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed Source proof actors, independent of currently observed routine callers."""

from copy import deepcopy

import pytest

from process import provider_directory_cms_proof_contract as contract
from tests.test_provider_directory_cms_owner_permissions import (
    _OWNER,
    _PUBLICATION_OWNER,
    _PUBLISHER,
    _TARGET,
    _WRITER,
)
from tests.test_provider_directory_cms_owner_permissions import (
    owner_case as owner_case,
)


def _actors(case):
    return contract.cms_proof_actor_oids(case.source, case.inventory["roles"]["roles"], namespace_owner_oid=_TARGET)


def test_only_four_genuine_source_callers_resolve_without_input_mutation(owner_case):
    case = owner_case
    before = deepcopy((case.source, case.inventory))
    assert _actors(case) == {_WRITER, _OWNER, _PUBLICATION_OWNER, _PUBLISHER}
    assert len(contract.CMS_PROOF_DEFINERS) == 6
    assert contract.CMS_INVOKER_PROOF not in contract.CMS_PROOF_DEFINERS
    assert (case.source, case.inventory) == before


def test_native_decimal_role_oids_and_namespace_owned_metadata_keep_exact_caller(owner_case):
    case = owner_case
    for role in case.inventory["roles"]["roles"]:
        role["oid"] = str(role["oid"])
    metadata = case.source["storage_custody"]["canonical_authority"]["metadata"]
    for entry in metadata.values():
        entry["definition"]["proowner"] = str(_TARGET)
        entry["acl"] = [[_WRITER, _TARGET, "EXECUTE", False], [_TARGET, _TARGET, "EXECUTE", False]]
    assert _actors(case) == {_WRITER, _OWNER, _PUBLICATION_OWNER, _PUBLISHER}
    next(iter(metadata.values()))["acl"].append([999, _TARGET, "EXECUTE", False])
    with pytest.raises(ValueError, match="metadata_caller_unbound"):
        _actors(case)


@pytest.mark.parametrize(
    "kind",
    [
        "native_oid",
        "source_oid",
        "duplicate_role",
        "private_login",
        "publisher_nologin",
        "publication_alias",
        "coordinate",
    ],
)
def test_source_role_or_native_binding_mismatch_refuses(owner_case, kind):
    case = owner_case
    rows = case.inventory["roles"]["roles"]
    if kind == "native_oid":
        rows[0]["oid"] += 1
    if kind == "source_oid":
        case.source["storage_custody"]["roles"]["preparation_owner"]["oid"] += 1
    if kind == "duplicate_role":
        rows.append(deepcopy(rows[0]))
    if kind == "private_login":
        next(role for role in rows if role["oid"] == _OWNER)["rolcanlogin"] = True
    if kind == "publisher_nologin":
        next(role for role in rows if role["oid"] == _PUBLISHER)["rolcanlogin"] = False
    if kind == "publication_alias":
        case.source["publication"]["owner_role"] = "source_owner"
    if kind == "coordinate":
        case.source["storage_custody"]["coordinates"]["namespace_owner_oid"] += 1
    with pytest.raises(ValueError, match="source_role_changed|roles_changed|metadata_caller_unbound"):
        _actors(case)


@pytest.mark.parametrize("value", [True, "0", "4294967296", "١٠١"])
def test_role_oid_does_not_accept_ambiguous_coercions(owner_case, value):
    case = owner_case
    case.inventory["roles"]["roles"][0]["oid"] = value
    with pytest.raises(ValueError, match="oid_required"):
        _actors(case)
