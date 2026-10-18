# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate existing acquisition hooks without changing their native objects."""

from db.migration_expression_adoption import _normalized_expression

_ROOTED_COPY_GUARD = "guard_provider_directory_rooted_graph_copy_owner"
_ROOTED_WORK_GUARD = "guard_provider_directory_rooted_graph_work"
_FROZEN_INSERT = ("guard_pd_uhc_flex_candidate_insert", 6, ("frozen",), None, ())
_HOOK_PREDICATE_SQL = """SELECT t.oid::bigint AS oid,t.tgname::text AS name,
 pg_catalog.pg_get_triggerdef(t.oid,false) AS definition,
 CASE d.refclassid WHEN 'pg_catalog.pg_proc'::regclass THEN 'function'
 WHEN 'pg_catalog.pg_class'::regclass THEN 'relation' ELSE d.refclassid::text END AS reference_kind,
 d.refobjid::bigint AS reference_oid,d.refobjsubid AS reference_column,d.deptype::text AS dependency_type
 FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_depend d
 ON d.classid='pg_catalog.pg_trigger'::regclass AND d.objid=t.oid AND d.objsubid=0
 WHERE t.tgrelid=$1::oid AND t.tgname=ANY($2::text[]) AND NOT t.tgisinternal
 ORDER BY t.oid,d.refclassid,d.refobjid,d.refobjsubid,d.deptype"""


def _require(condition):
    if not condition:
        raise ValueError("source_acquisition_hooks_changed")


def _root_hook_contracts():
    """Keep authentic control and closed-storage guards across owner adoption."""
    from process.provider_directory_source_acquisition_custody import ACQUISITION_ROOTS, BULK_ROOTS, METADATA_ROOTS

    hooks_by_root = {root: {} for root in (*ACQUISITION_ROOTS, *METADATA_ROOTS)}
    for root, prefix, function in (
        (ACQUISITION_ROOTS[2], "pd_uhc_flex_practitioner_acquisition", "guard_pd_uhc_flex_practitioner_acquisition"),
        (ACQUISITION_ROOTS[3], "pd_uhc_flex_practitioner_work", "guard_pd_uhc_flex_practitioner_work"),
    ):
        hooks_by_root[root] = {
            prefix + "_guard": (function, 27 if root in BULK_ROOTS else 31, (), None, ()),
            prefix + "_truncate": (function, 34, (), None, ()),
        }
    for root, prefix in (
        (ACQUISITION_ROOTS[5], "pd_uhc_flex_practitioner_twin_attempt"),
        (ACQUISITION_ROOTS[6], "pd_uhc_flex_practitioner_admission"),
    ):
        insert, immutable = "guard_" + prefix + "_insert", "guard_" + prefix + "_immutable"
        hooks_by_root[root] = {
            prefix + "_insert": (insert, 7, (), None, ()),
            prefix + "_immutable": (immutable, 27, (), None, ()),
            prefix + "_truncate": (immutable, 34, (), None, ()),
        }
    for root, function in (
        (ACQUISITION_ROOTS[7], "guard_provider_directory_rooted_graph_acquisition"),
        (ACQUISITION_ROOTS[11], "guard_provider_directory_rooted_graph_twin_attempt"),
        (ACQUISITION_ROOTS[12], "guard_provider_directory_rooted_graph_twin_admission"),
    ):
        hooks_by_root[root] = {
            (root + "_row_guard")[:63]: (function, 27 if root == ACQUISITION_ROOTS[12] else 31, (), None, ()),
            (root + "_truncate_guard")[:63]: (function, 34, (), None, ()),
        }
    hooks_by_root[ACQUISITION_ROOTS[7]]["rooted_graph_storage_seal"] = (
        "guard_provider_directory_rooted_graph_storage_seal",
        19,
        (),
        None,
        ("status",),
        "O",
        None,
        None,
    )
    for root in BULK_ROOTS:
        hooks_by_root[root] = _bulk_parent_hooks(root)
    hooks_by_root[METADATA_ROOTS[0]] = {
        "pd_practitioner_plan_write": ("pd_practitioner_migration_fence", 62, ("metadata",), None, ())
    }
    hooks_by_root[METADATA_ROOTS[1]] = {"pdrg_plan_owner": (_ROOTED_COPY_GUARD, 62, (), None, ())}
    _specialized_control_hooks(hooks_by_root)
    return hooks_by_root


def _bulk_parent_hooks(root):
    from process.provider_directory_source_acquisition_custody import BULK_ROOTS

    if root == BULK_ROOTS[0]:
        return {
            "pd_uhc_flex_practitioner_work_guard": ("guard_pd_uhc_flex_practitioner_work", 27, (), None, ()),
            "pd_uhc_flex_practitioner_work_truncate": ("guard_pd_uhc_flex_practitioner_work", 34, (), None, ()),
            "pd_uhc_flex_candidate_insert": _FROZEN_INSERT,
        }
    if root == BULK_ROOTS[1]:
        return {
            "pd_uhc_flex_resource_immutable": ("guard_pd_uhc_flex_practitioner_resource", 58, (), None, ()),
            "pd_uhc_flex_candidate_insert": _FROZEN_INSERT,
        }
    if root == BULK_ROOTS[2]:
        return {
            "provider_directory_rooted_graph_work_row_guard": (_ROOTED_WORK_GUARD, 27, (), None, ()),
            "rooted_graph_work_truncate": (_ROOTED_WORK_GUARD, 34, (), None, ()),
            "rooted_graph_copy_owner": (_ROOTED_COPY_GUARD, 6, (), None, ()),
        }
    return {
        "rooted_graph_copy_owner": (_ROOTED_COPY_GUARD, 6, (), None, ()),
        "rooted_graph_witness_immutable": ("reject_provider_directory_rooted_graph_witness_mutation", 58, (), None, ()),
    }


def _specialized_control_hooks(hooks_by_root):
    from process.provider_directory_source_acquisition_custody import ACQUISITION_ROOTS

    for root, prefix, function, contract in (
        (
            ACQUISITION_ROOTS[6],
            "pd_uhc_flex_practitioner_admission_insert",
            "guard_pd_uhc_flex_practitioner_admission_insert",
            "healthporta.provider-directory.uhc-flex-practitioner-matched-admission.v1",
        ),
        (
            ACQUISITION_ROOTS[6],
            "pd_uhc_flex_practitioner_single_root_admission_insert",
            "guard_pd_uhc_flex_practitioner_single_root_admission_insert",
            "healthporta.provider-directory.uhc-flex-practitioner-reviewed-single-root-admission.v1",
        ),
        (
            ACQUISITION_ROOTS[12],
            "provider_directory_rooted_graph_twin_admission_legacy_insert_guard",
            "guard_provider_directory_rooted_graph_twin_admission",
            "healthporta.provider-directory.rooted-graph-matched-admission.v1",
        ),
        (
            ACQUISITION_ROOTS[12],
            "provider_directory_rooted_graph_twin_admission_single_root_insert_guard",
            "guard_provider_directory_rooted_graph_single_root_admission",
            "healthporta.provider-directory.rooted-graph-single-root-admission.v1",
        ),
    ):
        hooks_by_root[root][prefix[:63]] = (function, 7, (), None, (), "A", None, contract)
    for root, column in ((ACQUISITION_ROOTS[7], "acquisition_id"), (ACQUISITION_ROOTS[6], "candidate_acquisition_id")):
        hooks_by_root[root].update(
            pd_dataset_bulk_parent_delete=(
                "guard_pd_dataset_bulk_parent_delete",
                8,
                (),
                None,
                (),
                "O",
                "deleted_rows",
                None,
            ),
            pd_dataset_parent_key_update=(
                "guard_pd_dataset_parent_key_update",
                19,
                (column,),
                None,
                (column,),
                "O",
                None,
                None,
            ),
        )


def _require_hooks(catalog, contracts, function_by_name, *, inherited=False):
    native_by_name = {hook["tgname"]: hook for hook in catalog["triggers"] or () if not hook["tgisinternal"]}
    _require(native_by_name.keys() == contracts.keys())
    number_by_column = {entry["attname"]: entry["attnum"] for entry in catalog["attributes"]}
    for name, contract in contracts.items():
        function, mask, arguments, transition, selected, *extra = contract
        enabled, old_table, predicate = extra or ("A", None, None)
        hook = native_by_name[name]
        _require(
            str(hook["tgfoid"]) == str(function_by_name[function]["definition"]["oid"])
            and hook["tgtype"] == mask
            and hook["tgenabled"] == enabled
            and not hook["tgdeferrable"]
            and not hook["tginitdeferred"]
            and (hook["tgqual"] is not None if predicate is not None else hook["tgqual"] is None)
            and hook["tgnargs"] == len(arguments)
            and hook["tgargs"] == "\\x" + "".join(value.encode().hex() + "00" for value in arguments)
            and hook["tgattr"] == [number_by_column[column] for column in selected]
            and (inherited or hook["tgparentid"] == "0")
            and hook["tgoldtable"] == old_table
            and hook["tgnewtable"] == transition
        )
        if predicate is not None:
            _require_hook_predicate(catalog, hook, predicate, number_by_column["admission_contract_id"])


def _require_hook_predicate(catalog, hook, predicate, column):
    proof = catalog.get("hook_predicates", {}).get(hook["tgname"])
    _require(proof is not None and proof["oid"] == int(hook["oid"]))
    _, separator, suffix = proof["definition"].partition(" WHEN (")
    expression, ending, _ = suffix.rpartition(") EXECUTE FUNCTION ")
    _require(
        separator
        and ending
        and _normalized_expression(expression) == "((new.admission_contract_id)::text='" + predicate + "'::text)"
    )
    expected_dependencies = [
        ("function", int(hook["tgfoid"]), 0, "n"),
        ("relation", int(hook["tgrelid"]), 0, "a"),
        ("relation", int(hook["tgrelid"]), column, "n"),
    ]
    _require(sorted(map(tuple, proof["dependencies"])) == sorted(expected_dependencies))


async def _capture_hook_predicates(connection, catalog, native, contracts):
    names = [name for name, contract in contracts.items() if len(contract) == 8 and contract[-1] is not None]
    if not names:
        return
    proofs_by_name = {}
    for native_row in await connection.fetch(_HOOK_PREDICATE_SQL, int(native["oid"]), names):
        name, oid, definition = native_row["name"], native_row["oid"], native_row["definition"]
        proof = proofs_by_name.setdefault(name, dict(oid=oid, definition=definition, dependencies=[]))
        _require(proof["oid"] == oid and proof["definition"] == definition)
        proof["dependencies"].append(
            tuple(
                native_row[field]
                for field in ("reference_kind", "reference_oid", "reference_column", "dependency_type")
            )
        )
    _require(proofs_by_name.keys() == set(names))
    catalog["hook_predicates"] = proofs_by_name


def _member_hook_contracts(root, *, attached, history, empty=False):
    from process.provider_directory_source_acquisition_custody import BULK_ROOTS

    if root in BULK_ROOTS[:2]:
        return _practitioner_member_hooks(work=root == BULK_ROOTS[0], attached=attached, history=history)
    hook_by_name = {"rooted_graph_copy_owner": (_ROOTED_COPY_GUARD, 6, (), None, ())}
    if root == BULK_ROOTS[2] and not history:
        hook_by_name["rooted_graph_work_truncate"] = (_ROOTED_WORK_GUARD, 34, (), None, ())
    else:
        hook_by_name["rooted_graph_witness_immutable"] = (
            "reject_provider_directory_rooted_graph_witness_mutation",
            58,
            (),
            None,
            (),
        )
    if root == BULK_ROOTS[2] and attached:
        hook_by_name["provider_directory_rooted_graph_work_row_guard"] = (_ROOTED_WORK_GUARD, 27, (), None, ())
    if not history:
        kind = dict(zip(BULK_ROOTS[2:], ("work", "resource", "edge"), strict=True))[root]
        hook_by_name["rooted_graph_witness_budget"] = (
            "account_provider_directory_rooted_graph_" + kind + "_budget",
            4,
            (),
            "inserted_" + kind,
            (),
        )
    return hook_by_name


def _practitioner_member_hooks(*, work, attached, history):
    hook_by_name = {
        "pd_uhc_flex_candidate_insert": _FROZEN_INSERT
        if work or history
        else ("guard_pd_uhc_flex_candidate_insert", 6, ("admission",), None, ())
    }
    if not work:
        hook_by_name["pd_uhc_flex_resource_immutable"] = ("guard_pd_uhc_flex_practitioner_resource", 58, (), None, ())
    elif history:
        hook_by_name["pd_uhc_flex_practitioner_work_truncate"] = (
            "guard_pd_uhc_flex_practitioner_work",
            34,
            (),
            None,
            (),
        )
    if work and attached:
        hook_by_name["pd_uhc_flex_practitioner_work_guard"] = ("guard_pd_uhc_flex_practitioner_work", 27, (), None, ())
    return hook_by_name
