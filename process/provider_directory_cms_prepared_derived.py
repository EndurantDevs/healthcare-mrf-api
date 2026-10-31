# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Copy verified frozen CMS projections into separate private output tables."""

from importlib import import_module

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_context as context
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_cms_canonical_layout import _LAYOUTS
from process.provider_directory_cms_prepared_workspace import CMSPreparedWorkspace, _layouts, _policy
from process.provider_directory_generic_candidate import COPY_MAX_BYTES, COPY_MAX_ROWS
from process.provider_directory_generic_source_policy import _savepoint
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff

DERIVED_ROOTS = storage.RELATIONS[1:4]
INPUT_ROOTS = (storage.RESOURCE, storage.WITNESS)


def _require(condition, code):
    if not condition:
        raise ValueError("cms_prepared_derived_" + code)


async def _coordinates(workspace, handoff, roots):
    _require(type(workspace) is CMSPreparedWorkspace and type(handoff) is FHIRSourceCandidateHandoff, "scope_required")
    receipts = await workspace.verify_current()
    document = handoff.document
    _require(
        document["contract"] == storage.CMS_ISOLATED_CONTRACT
        and handoff.document_sha256 == workspace.binding["handoff_sha256"]
        and document["identity"] == workspace.identity,
        "handoff_changed",
    )
    observed = await context._observe(
        workspace.connection,
        workspace.source_binding,
        _policy(workspace._inputs["source_policy"]),
        workspace.policy,
        workspace._inputs["frozen_inventory"],
    )
    _require(observed == workspace._inputs["native_coordinates"], "frozen_coordinates_changed")
    _require(
        not {receipts[root]["oid"] for root in roots}
        & {row["relation_oid"] for row in workspace._inputs["frozen_inventory"]["relations"]},
        "input_output_alias",
    )
    return document, {root: receipts[root] for root in roots}


def _page(relation, root, columns):
    columns = tuple(str(column) for column in columns)
    keys = completion._KEYS[root]
    # Transport follows the existing PK collation; proofs keep their own order.
    ordered = ",".join(storage_identifier(key) for key in keys)
    after = ",".join("$" + str(index) + "::text" for index in range(1, len(keys) + 1))
    selected = ",".join(
        storage_identifier(column) + ("::text AS " + storage_identifier(column) if column.endswith("_json") else "")
        for column in columns
    )
    width = "2+" + "+".join(
        "4+coalesce(pg_catalog.octet_length(" + storage_identifier(column) + "::text)+1,0)" for column in columns
    )
    output = ",".join(
        f"CASE WHEN row_bytes<=${len(keys) + 2} THEN {storage_identifier(column)} END AS {storage_identifier(column)}"
        for column in columns
    )
    return f"""WITH page AS MATERIALIZED (
 SELECT {selected},{width} AS row_bytes FROM {relation}
 WHERE ({ordered})>({after}) ORDER BY {ordered} LIMIT ${len(keys) + 1}
), bounded AS (
 SELECT *,row_number() OVER(ORDER BY {ordered}) AS ordinal,
 sum(row_bytes) OVER(ORDER BY {ordered}) AS page_bytes FROM page
)
SELECT {output},row_bytes FROM bounded WHERE ordinal=1 OR page_bytes<=${len(keys) + 2} ORDER BY {ordered}"""


async def _semantics(workspace, document, outputs=None):
    selected = document if outputs is None else {**document, "tables": {**document["tables"], **outputs}}
    await completion._frozen_projection_exact(
        workspace.connection, import_module("process.provider_directory_fhir"), workspace.policy, selected
    )


def _report(workspace, handoff, counts, state):
    return {
        "contract": "cms-prepared-derived-v1",
        "state": state,
        "dataset_id": workspace.identity["dataset_id"],
        "handoff_sha256": handoff.document_sha256,
        "preparation_id": workspace.binding["preparation_id"],
        "row_counts": counts,
    }


async def copy_prepared_cms_derived(workspace, handoff):
    """COPY the three already complete projections without recomputing them.

    Frozen input reads remain on the original Publisher. Only the existing
    workspace COPY enters Owner, with its native byte/row limits and rollback.
    The caller indexes all outputs before verify_prepared_cms_derived.
    """
    original = await _coordinates(workspace, handoff, DERIVED_ROOTS)
    _require(not workspace.binding["indexed"], "already_indexed")
    document, outputs = original
    async with _savepoint(workspace.connection):
        await _semantics(workspace, document)
        counts_by_root = await _copy_inputs(workspace, document, outputs)
        _require(await _coordinates(workspace, handoff, DERIVED_ROOTS) == original, "coordinates_changed")
    return _report(workspace, handoff, counts_by_root, "copied")


async def verify_prepared_cms_derived(workspace, handoff):
    """Prove full multiset equality and existing projection semantics after keys.

    Generated plan columns participate in comparison. This private evidence
    grants no canonical writer authority, publication or mutation fence.
    """
    original = await _coordinates(workspace, handoff, DERIVED_ROOTS)
    _require(workspace.binding["indexed"], "indexes_required")
    document, outputs = original
    async with _savepoint(workspace.connection):
        await _semantics(workspace, document)
        counts_by_root = await _compare_inputs(workspace, document, outputs)
        await _semantics(workspace, document, outputs)
        _require(await _coordinates(workspace, handoff, DERIVED_ROOTS) == original, "coordinates_changed")
    return _report(workspace, handoff, counts_by_root, "verified")


async def _copy_inputs(workspace, document, outputs):
    """Use one bounded COPY path for frozen resources, witnesses and projections."""
    for receipt in outputs.values():
        relation = storage_relation(workspace.policy.schema, receipt["name"])
        _require(
            await workspace.connection.fetchval(f"SELECT EXISTS(SELECT FROM {relation})") is False,
            "output_not_empty",
        )
    counts_by_root = {}
    for root in outputs:
        columns, keys = _layouts()[root][1], completion._KEYS[root]
        relation = storage_relation(workspace.policy.schema, document["tables"][root]["name"])
        query, after, count = _page(relation, root, columns), ("",) * len(keys), 0
        while copy_rows := await workspace.connection.fetch(query, *after, COPY_MAX_ROWS, COPY_MAX_BYTES - 21):
            _require(
                len(copy_rows) <= COPY_MAX_ROWS
                and all(
                    type(copy_row.get("row_bytes")) is int and 0 < copy_row["row_bytes"] <= COPY_MAX_BYTES - 21
                    for copy_row in copy_rows
                )
                and sum(copy_row["row_bytes"] for copy_row in copy_rows) <= COPY_MAX_BYTES - 21,
                "copy_page_unbounded",
            )
            next_keys = tuple(copy_rows[-1][key] for key in keys)
            _require(next_keys != after, "copy_key_not_advanced")
            copied = await workspace.copy_rows(
                root, columns, [tuple(copy_row[column] for column in columns) for copy_row in copy_rows]
            )
            _require(copied == len(copy_rows), "copy_incomplete")
            after, count = next_keys, count + copied
        counts_by_root[root] = count
    return counts_by_root


async def _compare_inputs(workspace, document, outputs):
    """Compare every stored and generated value, including both witness hashes."""
    counts_by_root = {}
    for root, receipt in outputs.items():
        columns = ",".join(
            storage_identifier(name) + ("::jsonb" if kind == "jsonb" else "")
            for name, kind, _required, _generated, _expression in _LAYOUTS[root]["columns"]
        )
        frozen = storage_relation(workspace.policy.schema, document["tables"][root]["name"])
        output = storage_relation(workspace.policy.schema, receipt["name"])
        expected, actual = f"SELECT {columns} FROM {frozen}", f"SELECT {columns} FROM {output}"
        proof = await workspace.connection.fetchrow(
            f"SELECT EXISTS(({expected} EXCEPT ALL {actual}) UNION ALL "
            f"({actual} EXCEPT ALL {expected})) AS changed,(SELECT count(*) FROM {output}) AS count"
        )
        _require(proof is not None and proof["changed"] is False, "projection_changed")
        counts_by_root[root] = proof["count"]
    return counts_by_root


async def copy_prepared_cms_input_payloads(workspace, handoff):
    """Copy resource/witness snapshots; preserve the separately frozen Source heaps."""
    original = await _coordinates(workspace, handoff, INPUT_ROOTS)
    _require(not workspace.binding["indexed"], "already_indexed")
    document, outputs = original
    async with _savepoint(workspace.connection):
        counts = await _copy_inputs(workspace, document, outputs)
        _require(await _coordinates(workspace, handoff, INPUT_ROOTS) == original, "coordinates_changed")
    return {**_report(workspace, handoff, counts, "copied"), "contract": "cms-prepared-input-payloads-v1"}


async def verify_prepared_cms_input_payloads(workspace, handoff):
    """Require complete multiset parity under the caller's frozen/private custody."""
    original = await _coordinates(workspace, handoff, INPUT_ROOTS)
    _require(workspace.binding["indexed"], "indexes_required")
    document, outputs = original
    async with _savepoint(workspace.connection):
        counts = await _compare_inputs(workspace, document, outputs)
        _require(await _coordinates(workspace, handoff, INPUT_ROOTS) == original, "coordinates_changed")
    return {**_report(workspace, handoff, counts, "verified"), "contract": "cms-prepared-input-payloads-v1"}
