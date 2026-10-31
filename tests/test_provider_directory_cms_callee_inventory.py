# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact SourceV2 inventories preserve fixed callee and ACL checks without role labels."""

from copy import deepcopy

import pytest

from process import provider_directory_cms_callee_inventory as inventory
from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS
from tests.test_provider_directory_cms_callee_custody import _SOURCE_ROLES, _catalog_rows
from tests.test_provider_directory_cms_callee_custody import _verify as legacy_verify


@pytest.fixture(scope="module")
def function_rows():
    return _catalog_rows("snapshot_schema")


def _arguments(rows):
    return dict(
        catalog_schema="snapshot_schema",
        catalog_schema_oid=42,
        expected_functions={
            row["definition"]["proname"]: dict(
                oid=int(row["definition"]["oid"]), sha256=inventory.callee_fingerprint(row)
            )
            for row in rows
        },
        namespace_owner_oid=_SOURCE_ROLES["namespace_owner"],
        preparation_schema="preparation_schema",
    )


def test_same_fixed_functions_pass_both_contracts_without_v1_labels(function_rows):
    before = deepcopy(function_rows)
    legacy = legacy_verify(function_rows)
    current = inventory.require_cms_callee_inventory(function_rows, **_arguments(function_rows))
    assert current == legacy and function_rows == before
    current.clear()
    assert function_rows


@pytest.mark.parametrize("field", ["prosrc", "proowner", "proacl", "proconfig", "prosecdef", "acl", "language"])
def test_changed_native_field_refuses_against_original_pin(function_rows, field):
    rows = deepcopy(function_rows)
    arguments = _arguments(rows)
    if field == "acl":
        rows[0]["acl"].append([999, 107, "EXECUTE", False])
    elif field == "language":
        rows[0][field] = "sql"
    else:
        rows[0]["definition"][field] = "changed"
    with pytest.raises(ValueError, match="callee_inventory_changed"):
        inventory.require_cms_callee_inventory(rows, **arguments)


@pytest.mark.parametrize(
    "change", ["body", "grantable", "grantor", "public", "missing_caller", "other_caller", "extra_caller"]
)
def test_fixed_source_guards_still_refuse_even_if_caller_repins_bad_rows(function_rows, change):
    rows = deepcopy(function_rows)
    row = next(
        entry for entry in rows if entry["definition"]["proname"] == "provider_directory_subset_canonical_sha256"
    )
    owner = int(row["definition"]["proowner"])
    match change:
        case "body":
            row["definition"]["prosrc"] += "changed"
        case "grantable":
            row["acl"][-1][-1] = True
        case "grantor":
            row["acl"][-1][1] = 999
        case "public":
            row["acl"].append([0, owner, "EXECUTE", False])
        case "missing_caller":
            row["acl"] = [[owner, owner, "EXECUTE", False]]
        case "other_caller":
            row["acl"][-1][0] = 999
        case "extra_caller":
            row["acl"].append([999, owner, "EXECUTE", False])
    with pytest.raises(ValueError):
        legacy_verify(rows)
    with pytest.raises(ValueError):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows))


@pytest.mark.parametrize("change", ["missing", "duplicate", "oid", "fingerprint"])
def test_incomplete_or_rebound_inventory_refuses(function_rows, change):
    rows = deepcopy(function_rows)
    arguments = _arguments(rows)
    first = rows[0]["definition"]["proname"]
    if change == "missing":
        del arguments["expected_functions"][first]
    elif change == "duplicate":
        rows.append(deepcopy(rows[0]))
    elif change == "oid":
        arguments["expected_functions"][first]["oid"] += 1
    else:
        arguments["expected_functions"][first]["sha256"] = "0" * 64
    with pytest.raises(ValueError):
        inventory.require_cms_callee_inventory(rows, **arguments)


def _closed_source_rows(function_rows):
    rows = deepcopy(function_rows)
    names = {signature.split("(")[0] for signature in inventory.callables._SOURCE_CALLEES}
    for ordinal, row in enumerate(rows):
        if row["definition"]["proname"] in names:
            owner = 700 + ordinal
            row["definition"]["proowner"] = str(owner)
            row["acl"] = [[owner, owner, "EXECUTE", False]]
    return rows


def test_owner_only_source_v2_callees_accept_individually_authenticated_owners(function_rows):
    rows = _closed_source_rows(function_rows)
    before = deepcopy(rows)
    verified = inventory.require_cms_callee_inventory(rows, **_arguments(rows))
    assert len(verified) == 32 and rows == before
    with pytest.raises(ValueError):
        legacy_verify(rows)


@pytest.mark.parametrize("change", ["public", "caller", "grantor", "grantable", "missing", "duplicate", "body"])
def test_owner_only_branch_refuses_rebound_bad_acl_and_mixed_caller_shapes(function_rows, change):
    rows = _closed_source_rows(function_rows)
    row = next(
        entry for entry in rows if entry["definition"]["proname"] == "provider_directory_subset_canonical_sha256"
    )
    owner = int(row["definition"]["proowner"])
    match change:
        case "public":
            row["acl"].append([0, owner, "EXECUTE", False])
        case "caller":
            row["acl"].append([999, owner, "EXECUTE", False])
        case "grantor":
            row["acl"][0][1] = 999
        case "grantable":
            row["acl"][0][-1] = True
        case "missing":
            row["acl"] = []
        case "duplicate":
            row["acl"] *= 2
        case "body":
            row["definition"]["prosrc"] += "changed"
    with pytest.raises(ValueError):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows))


def _namespace_source_rows(function_rows):
    rows = deepcopy(function_rows)
    names = {signature.split("(")[0] for signature in inventory.callables._SOURCE_CALLEES}
    target_owner = _SOURCE_ROLES["namespace_owner"]
    for row in rows:
        if row["definition"]["proname"] not in names:
            continue
        previous_owner = int(row["definition"]["proowner"])
        row["definition"]["proowner"] = str(target_owner)
        row["acl"] = [
            [target_owner if grantee == previous_owner else grantee, target_owner, right, grantable]
            for grantee, _grantor, right, grantable in row["acl"]
        ]
    return rows


def _source_metadata(function_rows):
    functions_by_name = {row["definition"]["proname"]: row for row in function_rows}
    return {
        signature.replace("character varying", "varchar"): deepcopy(functions_by_name[signature.split("(")[0]])
        for signature in METADATA_FUNCTIONS
    }


def test_namespace_owner_requires_bound_source(function_rows):
    rows = _namespace_source_rows(function_rows)
    metadata = _source_metadata(rows)
    before = deepcopy((rows, metadata))
    with pytest.raises(ValueError, match="source_metadata_required"):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows))
    verified = inventory.require_cms_callee_inventory(rows, **_arguments(rows), source_metadata=metadata)
    assert len(verified) == 32 and (rows, metadata) == before
    with pytest.raises(ValueError):
        legacy_verify(rows)


@pytest.mark.parametrize("public_caller", [False, True])
def test_namespace_owner_preserves_public_rule(function_rows, public_caller):
    rows = _namespace_source_rows(function_rows)
    row = next(
        entry
        for entry in rows
        if entry["definition"]["proname"] == "provider_directory_uhc_flex_practitioner_dataset_valid"
    )
    if public_caller:
        row["acl"].append([103, _SOURCE_ROLES["namespace_owner"], "EXECUTE", False])
    assert (
        len(inventory.require_cms_callee_inventory(rows, **_arguments(rows), source_metadata=_source_metadata(rows)))
        == 32
    )


@pytest.mark.parametrize("signature", METADATA_FUNCTIONS)
@pytest.mark.parametrize("field", ["oid", "definition", "language", "acl"])
def test_every_metadata_record_is_bound(function_rows, signature, field):
    rows = _namespace_source_rows(function_rows)
    metadata = _source_metadata(rows)
    bound = metadata[signature.replace("character varying", "varchar")]
    if field == "oid":
        bound["definition"]["oid"] = "999999"
    elif field == "definition":
        bound["definition"]["proacl"] = "unbound"
    elif field == "language":
        bound["language"] = "unbound"
    else:
        bound["acl"].append([999, _SOURCE_ROLES["namespace_owner"], "EXECUTE", False])
    with pytest.raises(ValueError, match="source_metadata_changed"):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows), source_metadata=metadata)


@pytest.mark.parametrize("fault", ["missing", "extra", "alias", "entry", "field"])
def test_namespace_owner_rejects_incomplete_metadata(function_rows, fault):
    rows = _namespace_source_rows(function_rows)
    metadata = _source_metadata(rows)
    first = next(iter(metadata))
    if fault == "missing":
        del metadata[first]
    elif fault == "extra":
        metadata["unbound(jsonb)"] = deepcopy(metadata[first])
    elif fault == "alias":
        metadata[first.replace("smallint", "int2")] = metadata.pop(first)
    elif fault == "entry":
        metadata[first] = None
    else:
        del metadata[first]["definition"]
    with pytest.raises(ValueError, match="source_metadata_required|source_metadata_changed"):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows), source_metadata=metadata)


@pytest.mark.parametrize("fault", ["other_caller", "missing_caller", "extra_caller", "mixed_owner", "public_caller"])
def test_namespace_owner_rejects_callee_drift(function_rows, fault):
    rows = _namespace_source_rows(function_rows)
    metadata = _source_metadata(rows)
    name = "provider_directory_subset_payload_canonical_json"
    if fault == "public_caller":
        name = "provider_directory_uhc_flex_practitioner_dataset_valid"
    row = next(entry for entry in rows if entry["definition"]["proname"] == name)
    owner = _SOURCE_ROLES["namespace_owner"]
    if fault == "other_caller":
        row["acl"][-1][0] = 999
    elif fault == "missing_caller":
        row["acl"] = [[owner, owner, "EXECUTE", False]]
    elif fault == "extra_caller":
        row["acl"].append([999, owner, "EXECUTE", False])
    elif fault == "mixed_owner":
        row["definition"]["proowner"] = "999"
        row["acl"] = [[999, 999, "EXECUTE", False], [103, 999, "EXECUTE", False]]
    else:
        row["acl"].append([999, owner, "EXECUTE", False])
    with pytest.raises(ValueError, match="acl_changed|source_acl_changed"):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows), source_metadata=metadata)


def _owner_only_rooted_rows(function_rows):
    rows = _namespace_source_rows(function_rows)
    owner = _SOURCE_ROLES["namespace_owner"]
    rooted = next(
        row for row in rows if row["definition"]["proname"] == "provider_directory_rooted_graph_dataset_intrinsic_valid"
    )
    rooted["acl"] = [[owner, owner, "EXECUTE", False]]
    return rows


def test_namespace_owned_private_rooted_validator_keeps_bound_metadata_callers(function_rows):
    rows = _owner_only_rooted_rows(function_rows)
    metadata = _source_metadata(rows)
    before = deepcopy((rows, metadata))
    assert len(inventory.require_cms_callee_inventory(rows, **_arguments(rows), source_metadata=metadata)) == 32
    assert (rows, metadata) == before
    with pytest.raises(ValueError, match="source_metadata_required"):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows))


@pytest.mark.parametrize("fault", ["old_owner", "public", "grantable", "grantor", "other_caller", "metadata", "body"])
def test_private_rooted_exception_remains_exact_and_source_bound(function_rows, fault):
    rows = _owner_only_rooted_rows(function_rows)
    metadata = _source_metadata(rows)
    rooted = next(
        row for row in rows if row["definition"]["proname"] == "provider_directory_rooted_graph_dataset_intrinsic_valid"
    )
    owner = _SOURCE_ROLES["namespace_owner"]
    match fault:
        case "old_owner":
            rooted["definition"]["proowner"] = "101"
            rooted["acl"] = [[101, 101, "EXECUTE", False]]
        case "public" | "other_caller":
            rooted["acl"].append([0 if fault == "public" else 999, owner, "EXECUTE", False])
        case "grantable":
            rooted["acl"][0][-1] = True
        case "grantor":
            rooted["acl"][0][1] = 999
        case "body":
            rooted["definition"]["prosrc"] += "\n-- changed"
        case _:
            next(iter(metadata.values()))["acl"] = []
    with pytest.raises(ValueError, match="acl_changed|source_acl_changed|source_metadata_changed|definition_changed"):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows), source_metadata=metadata)


@pytest.mark.parametrize("public_uhc", [False, True])
def test_namespace_uhc_preserves_closed_or_public_predecessor_without_new_callers(function_rows, public_uhc):
    rows = _owner_only_rooted_rows(function_rows)
    uhc = next(
        row for row in rows if row["definition"]["proname"] == "provider_directory_uhc_flex_practitioner_dataset_valid"
    )
    owner = _SOURCE_ROLES["namespace_owner"]
    uhc["acl"] = ([[0, owner, "EXECUTE", False]] if public_uhc else []) + [[owner, owner, "EXECUTE", False]]
    metadata = _source_metadata(rows)
    before = deepcopy((rows, metadata))
    assert len(inventory.require_cms_callee_inventory(rows, **_arguments(rows), source_metadata=metadata)) == 32
    assert (rows, metadata) == before
    with pytest.raises(ValueError, match="source_metadata_required"):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows))


@pytest.mark.parametrize(
    "fault", ["old_owner", "grantable", "grantor", "other_caller", "metadata", "body", "missing_owner", "duplicate"]
)
def test_closed_namespace_uhc_is_exact_and_source_bound(function_rows, fault):
    rows = _owner_only_rooted_rows(function_rows)
    uhc = next(
        row for row in rows if row["definition"]["proname"] == "provider_directory_uhc_flex_practitioner_dataset_valid"
    )
    owner = _SOURCE_ROLES["namespace_owner"]
    uhc["acl"] = [[owner, owner, "EXECUTE", False]]
    metadata = _source_metadata(rows)
    match fault:
        case "old_owner":
            uhc["definition"]["proowner"] = "101"
            uhc["acl"] = [[101, 101, "EXECUTE", False]]
        case "grantable":
            uhc["acl"][0][-1] = True
        case "grantor":
            uhc["acl"][0][1] = 999
        case "other_caller":
            uhc["acl"].append([999, owner, "EXECUTE", False])
        case "body":
            uhc["definition"]["prosrc"] += "\n-- changed"
        case "missing_owner":
            uhc["acl"] = []
        case "duplicate":
            uhc["acl"] *= 2
        case _:
            next(iter(metadata.values()))["acl"] = []
    with pytest.raises(ValueError, match="acl_changed|source_acl_changed|source_metadata_changed|definition_changed"):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows), source_metadata=metadata)
