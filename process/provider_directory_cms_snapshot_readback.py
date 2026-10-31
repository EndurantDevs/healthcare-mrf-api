# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read exact sealed/attached CMS payload postimages; no authority or activation.

The caller loads expected attachment evidence from its protected terminal record.
Self-captured metadata cannot supply that provenance. Content, complete writer
custody, deadlines and the outer transaction remain the caller's responsibility.
"""

import re
from contextlib import asynccontextmanager
from copy import deepcopy

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_attachment_objects as attachment_objects
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_cms_snapshot_attach as attach
from process import provider_directory_cms_snapshot_custody as custody
from process.plan_release_storage_catalog import CATALOG_SQL


def _require(condition, code):
    if not condition:
        raise ValueError("cms_snapshot_readback_" + code)


async def _reader_actor(connection, request, retained):
    return await attach.seal._reader_admin(
        connection,
        request["source_binding"],
        request["prepared_inputs"],
        retained,
        request["owner_role"],
        request["migration_role"],
    )


def _expected(request, retained, expected_attachment):
    """Bind a complete two-table postimage to the original private identities and owner."""
    expected = workspace._copy(expected_attachment)
    roots = set(attach.ROOTS)
    _require(
        type(expected) is dict
        and set(expected) == {"contract", "parents", "tables"}
        and expected["contract"] == attach.CONTRACT
        and type(expected["parents"]) is dict
        and set(expected["parents"]) == roots
        and type(expected["tables"]) is dict
        and set(expected["tables"]) == roots,
        "attachment_required",
    )
    parents = expected["parents"]
    _require(
        all(type(oid) is int and 0 < oid < 2**32 for oid in parents.values()) and len(set(parents.values())) == 2,
        "parents_changed",
    )
    for root, proof in expected["tables"].items():
        original = retained["tables"][root]
        _require(
            type(proof) is dict
            and set(proof) == {"name", "oid", "owner", "layout_sha256"}
            and proof["name"] == original["name"]
            and type(proof["oid"]) is int
            and proof["oid"] == original["oid"]
            and type(proof["owner"]) is int
            and proof["owner"] == request["owner_oid"]
            and type(proof["layout_sha256"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", proof["layout_sha256"]),
            "tables_changed",
        )
    _require(not set(parents.values()) & {proof["oid"] for proof in expected["tables"].values()}, "parents_changed")
    return expected


async def _observe(connection, request, retained, expected):
    """Compare native parent/layout/bound semantics and complete attached postimage proofs."""
    source = request["source_binding"]
    principals = await custody._principals(
        connection, source, retained, request["owner_role"], request["owner_oid"], request["migration_role"]
    )
    await attach._compatible(connection, source, retained, expected["parents"], request["owner_oid"])
    tables, _indexes_by_oid = await attach._capture_attached(
        connection, source, retained, expected["parents"], request["owner_oid"], principals["publisher_oid"]
    )
    _require(tables == expected["tables"], "postimage_changed")


async def read_attached_cms_payload_snapshots(connection, request_by_name, *, expected_attachment):
    """Pin two exact parents and attached heaps, then authenticate their retained postimage.

    SELECTs and one sorted ONLY ACCESS SHARE NOWAIT pin leave unrelated siblings
    available. Successful pins last through the caller's transaction; refusal or
    cancellation rolls back this savepoint. The workspace receipts remain private
    preimages: this never overwrites their owner, parent or layout expectations.
    """
    _require(connection.is_in_transaction() and not connection.is_closed(), "transaction_required")
    _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    request, retained = attach._request(request_by_name)
    expected = _expected(request, retained, expected_attachment)
    actor = await _reader_actor(connection, request, retained)
    source_binding = request["source_binding"]
    relations = sorted(
        [storage_relation(source_binding.catalog_schema, root) for root in attach.ROOTS]
        + [storage_relation(source_binding.storage_schema, proof["name"]) for proof in expected["tables"].values()]
    )
    _require(len(set(relations)) == 4, "tables_changed")
    async with workspace._savepoint(connection):
        await connection.execute(
            "LOCK TABLE " + ",".join("ONLY " + name for name in relations) + " IN ACCESS SHARE MODE NOWAIT"
        )
        await _observe(connection, request, retained, expected)
        await _observe(connection, request, retained, expected)
        _require(await _reader_actor(connection, request, retained) == actor, "transaction_changed")
        current, current_retained = attach._request(request_by_name)
        _require(
            current == request and current_retained == retained and expected_attachment == expected,
            "inputs_changed",
        )
        _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    return workspace._copy(expected)


def _sealed_expected(request, retained, expected_seal):
    """Keep the six-table seal separate from the immutable preparation preimage."""
    expected = workspace._copy(expected_seal)
    source = request["source_binding"]
    _require(
        type(expected) is dict
        and set(expected) == {"contract", "owner_oid", "schema_oid", "namespace_owner_oid", "tables"}
        and expected["contract"] == custody.CONTRACT
        and expected["owner_oid"] == request["owner_oid"]
        and expected["schema_oid"] == source.storage_schema_oid
        and expected["namespace_owner_oid"] == source.namespace_owner_oid
        and type(expected["tables"]) is dict
        and set(expected["tables"]) == set(workspace.storage.RELATIONS),
        "seal_required",
    )
    for root, proof in expected["tables"].items():
        _require(proof == {**retained["tables"][root], "owner": request["owner_oid"]}, "seal_changed")
    return expected


async def _closed_payloads(connection, request, retained, sealed, principals):
    """The four copied inputs remain closed, unattached heaps at their original OIDs."""
    roots = workspace.storage.RELATIONS[:4]
    names = [retained["tables"][root]["name"] for root in roots]
    rows = await connection.fetch(
        CATALOG_SQL,
        [storage_relation(request["source_binding"].storage_schema, name) for name in names],
        request["owner_oid"],
        [principals["publisher_oid"]],
    )
    _require(len(rows) == len(names) and {row["relname"] for row in rows} == set(names), "inventory_changed")
    proofs_by_name = {}
    for row in rows:
        _require(
            row["nspname"] == request["source_binding"].storage_schema
            and row["relowner"] == request["owner_oid"]
            and row["readers_closed"] is True,
            "postimage_changed",
        )
        proofs_by_name[row["relname"]] = workspace.storage._table_proof(row, private=True)
    _require(
        all(proofs_by_name[retained["tables"][root]["name"]] == sealed["tables"][root] for root in roots),
        "postimage_changed",
    )


async def _payload_writers(connection, request, retained, principals):
    """Preserve the existing six-leaf closure check; parent/all-writer custody is external."""
    oids = sorted(retained["tables"][root]["oid"] for root in workspace.storage.RELATIONS)
    _require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhparent=ANY($1::oid[]))", oids
        )
        is True,
        "unexpected_descendant",
    )
    _require(
        await connection.fetchval(
            custody._WRITERS_SQL,
            oids,
            request["owner_oid"],
            principals["namespace_owner_oid"],
            principals["migration_oid"],
        )
        is True,
        "writer_reachable",
    )


async def verify_sealed_cms_workspace(current, *, request, expected_seal, expected_attachment):
    """Verify fixed postimages without replacing original workspace receipts or rights."""
    request, retained = attach._request(request)
    _require(
        current._readonly is True
        and current._indexed is True
        and current.binding == retained
        and current.source_binding == request["source_binding"],
        "workspace_changed",
    )
    sealed = _sealed_expected(request, retained, expected_seal)
    attached = _expected(request, retained, expected_attachment) if expected_attachment is not None else None
    await _reader_actor(current.connection, request, retained)
    principals = await custody._principals(
        current.connection,
        current.source_binding,
        retained,
        request["owner_role"],
        request["owner_oid"],
        request["migration_role"],
    )
    identities_by_root = {
        root: receipt for root, receipt in current.receipts.items() if root not in workspace.storage.RELATIONS
    }
    observed = await workspace.storage._capture(
        current.connection, current.policy, [proof["name"] for proof in identities_by_root.values()]
    )
    _require(all(observed[proof["name"]] == proof for proof in identities_by_root.values()), "identity_changed")
    if attached is None:
        actual = await custody._observe(
            current.connection,
            current.source_binding,
            retained,
            request["owner_role"],
            request["owner_oid"],
            request["migration_role"],
        )
        _require(actual == sealed, "postimage_changed")
    else:
        await _closed_payloads(current.connection, request, retained, sealed, principals)
        actual = await read_attached_cms_payload_snapshots(current.connection, request, expected_attachment=attached)
        _require(actual == attached, "postimage_changed")
    await _payload_writers(current.connection, request, retained, principals)


async def _sealed_read_workspace(database, session, connection, request, expected_seal, expected_attachment):
    """Build the existing readonly workspace on a genuine independent Admin binding."""
    from process import provider_directory_cms_admin_workspace as admin

    request, retained = attach._request(request)
    storage_identifier(request["owner_role"])
    storage_identifier(request["migration_role"])
    _require(
        isinstance(database, workspace.context.Database) and isinstance(session, workspace.context.AsyncSession),
        "session_required",
    )
    storage_binding = request["source_binding"]
    fields = ("source_storage", "source_policy", "prepared_policy", "frozen_inventory")
    expected, _source_policy, policy = admin._inputs(
        storage_binding, {key: request["prepared_inputs"][key] for key in fields}
    )
    task, transaction = workspace.asyncio.current_task(), session.get_transaction()
    _require(task is not None and transaction is not None, "original_transaction_required")
    await workspace.context._physical(database, session, connection, task, transaction)
    original, original_retained, binding, receipts = await admin._observe_native(
        connection, storage_binding, expected, request["handoff"], retained, request["migration_role"], reader=True
    )
    _require(original_retained == retained, "workspace_changed")
    _require(original["native_coordinates"] == request["prepared_inputs"]["native_coordinates"], "inputs_changed")
    await admin._pin_relations(connection, policy, expected, receipts)
    current = workspace.CMSPreparedWorkspace(
        database, session, connection, storage_binding, original, binding, receipts
    )
    current._indexed, current._readonly = True, True
    current._payload_postimages = {
        "request": request,
        "expected_seal": workspace._copy(expected_seal),
        "expected_attachment": workspace._copy(expected_attachment),
    }
    return current


@asynccontextmanager
async def _open_sealed_reader(database, session, connection, request, *, expected_seal, expected_attachment):
    """Share exact postimage pins and input rechecks through the borrowed read scope."""
    from process import provider_directory_cms_admin_workspace as admin

    retained_request, _retained = attach._request(request)
    sealed, attached = workspace._copy(expected_seal), workspace._copy(expected_attachment)
    async with workspace._savepoint(connection):
        current = await _sealed_read_workspace(database, session, connection, retained_request, sealed, attached)
        try:
            await current.verify_current()
            yield current, retained_request["handoff"]
            await current.verify_current()
            observed = await admin._observe_native(
                connection,
                current.source_binding,
                current._inputs,
                retained_request["handoff"],
                retained_request["expected_workspace"],
                retained_request["migration_role"],
                reader=True,
            )
            _require(observed[0] == current._inputs and observed[1] == current.binding, "inputs_changed")
            _require(
                attach._request(request)[0] == retained_request
                and expected_seal == sealed
                and expected_attachment == attached,
                "inputs_changed",
            )
        finally:
            current._active = False


async def read_sealed_cms_outputs(database, session, connection, request, *, expected_seal, expected_attachment):
    """Recompute the retained private report under separate six-payload postimages.

    Request and both expected records require future protected receipt provenance;
    these plain data arguments issue no authority. Caller owns custody, deadline
    and outer rollback/commit. Full scans belong before the short terminal fence.
    Canonical receipt provenance and the protected finisher remain separate.
    """
    from process.provider_directory_cms_prepared_outputs import read_prepared_cms_outputs

    _require(expected_attachment is not None, "attachment_required")
    async with _open_sealed_reader(
        database, session, connection, request, expected_seal=expected_seal, expected_attachment=expected_attachment
    ) as (current, handoff):
        report = await read_prepared_cms_outputs(current, handoff)
    return deepcopy(report)


async def read_sealed_cms_preflight(
    database, session, connection, request, *, expected_seal, allocation, native_publication
):
    """Read all private data and four canonical targets before the short attachment cut.

    The two sealed payload heaps must still be unattached. The result describes
    private and generic data separately; it is not completed canonical evidence.
    Protected allocation, native-copy provenance and common custody stay external.
    """
    from process import provider_directory_cms_prepared_candidate as candidate
    from process.provider_directory_cms_prepared_outputs import read_prepared_cms_outputs

    async with _open_sealed_reader(
        database, session, connection, request, expected_seal=expected_seal, expected_attachment=None
    ) as (current, handoff):
        private = await read_prepared_cms_outputs(current, handoff)
        generic = await candidate.read_prepared_cms_generic_candidate(
            current, handoff, allocation=allocation, native_publication=native_publication
        )
        _require(
            private["tables"] == current.receipts
            and generic["identities"]["identity_parity"] == private["identity_parity"]
            and generic["relationships"] == private["relationship_parity"],
            "reports_changed",
        )
        report_by_section = {"workspace": current.binding, "private": private, "generic": generic}
    return deepcopy(report_by_section)


async def read_sealed_cms_candidate(
    database, session, connection, request, *, expected_seal, expected_attachment, native_publication
):
    """Read private and canonical data without issuing a publication receipt or authority.

    The caller authenticates native receipt provenance and holds its qualified
    canonical custody or pinned snapshot. Full scans precede the terminal fence;
    this data reader neither supplies that fence nor changes the serving pointer.
    """
    from process import provider_directory_cms_prepared_candidate as candidate
    from process.provider_directory_cms_prepared_outputs import read_prepared_cms_outputs

    _require(expected_attachment is not None, "attachment_required")
    async with _open_sealed_reader(
        database, session, connection, request, expected_seal=expected_seal, expected_attachment=expected_attachment
    ) as (current, handoff):
        _, expected_header = await candidate._inputs(current, handoff)
        header = await candidate._read_header(current, expected_header)
        _require(
            header["status"] == "published"
            and header["is_current"] is False
            and header["published_at"] is not None
            and header["superseded_at"] is None,
            "published_noncurrent_required",
        )
        _require(type(native_publication) is dict, "native_publication_required")
        native = workspace._copy(native_publication)
        private = await read_prepared_cms_outputs(current, handoff)
        canonical = await candidate.read_prepared_cms_candidate(current, handoff, native_publication=native)
        _require(
            private["tables"] == current.receipts
            and canonical["header"] == header
            and canonical["identities"]["identity_parity"] == private["identity_parity"]
            and canonical["relationships"] == private["relationship_parity"]
            and canonical["payloads"]["native_publication"] == native,
            "reports_changed",
        )
        _require(
            await candidate._read_header(current, expected_header) == header and native_publication == native,
            "inputs_changed",
        )
        report_by_section = {"workspace": current.binding, "private": private, "canonical": canonical}
    return deepcopy(report_by_section)


def retained_cms_payload_postimages(request, evidence, *, allocation):
    """Extract linked payload postimages from already authenticated terminal data.

    This performs no SQL and authenticates no receipt, profile or allocation.
    The original-Admin recovery caller must load/verify those protected records,
    acquire qualified read custody, and separately recheck retained history.
    Never replace a missing field by capturing current catalog observations.
    """
    from process import provider_directory_cms_history_record as history_records

    request, retained = attach._request(request)
    evidence, allocation = workspace._copy(evidence), workspace._copy(allocation)
    _require(
        type(evidence) is dict
        and set(evidence) == {"contract", "workspace", "private", "canonical"}
        and evidence.get("contract") == "cms-npd-finalization-evidence-v1"
        and evidence.get("workspace") == retained
        and type(evidence.get("canonical")) is dict
        and type(evidence["canonical"].get("payloads")) is dict,
        "terminal_evidence_required",
    )
    postimages = evidence["canonical"]["payloads"].get("snapshot_postimages")
    _require(
        type(postimages) is dict and set(postimages) == {"sealed", "attached", "attachment_objects", "history_storage"},
        "terminal_postimages_required",
    )
    _require(
        type(allocation) is dict and allocation.get("contract") == history_records.ALLOCATION_CONTRACT,
        "history_allocation_required",
    )
    binding = request["source_binding"]
    plan = history_records.require_cms_history_record(
        allocation["history"],
        storage={
            key: getattr(binding, key)
            for key in ("database_oid", "catalog_schema", "catalog_schema_oid", "namespace_owner_oid")
        },
        parents=allocation["parents"],
    )
    sealed = _sealed_expected(request, retained, postimages["sealed"])
    attached = _expected(request, retained, postimages["attached"])
    _require(
        attached["parents"] == {root: entry["oid"] for root, entry in allocation["parents"].items()},
        "terminal_parents_changed",
    )
    history_oids = {
        str(entry[kind]["oid"]) for entry in plan["request"]["heaps"].values() for kind in ("old", "parent")
    }
    _require(
        type(postimages["history_storage"]) is dict and set(postimages["history_storage"]) == history_oids,
        "terminal_history_storage_changed",
    )
    attachment_objects._retained_oids(attached["tables"], postimages["attachment_objects"])
    return {
        "sealed": sealed,
        "attached": attached,
        "attachment_objects": workspace._copy(postimages["attachment_objects"]),
    }
