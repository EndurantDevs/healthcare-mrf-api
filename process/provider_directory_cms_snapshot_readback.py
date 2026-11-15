# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read exact sealed/attached CMS payload postimages; no authority or activation.

The caller loads expected attachment evidence from its protected terminal record.
Self-captured metadata cannot supply that provenance. Content, complete writer
custody, deadlines and the outer transaction remain the caller's responsibility.
"""

from contextlib import asynccontextmanager
from copy import deepcopy

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_canonical_layout as layout
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_cms_snapshot_attach as attach
from process import provider_directory_cms_snapshot_custody as custody


def _require(condition, code):
    if not condition:
        raise ValueError("cms_snapshot_readback_" + code)


def _expected(request, retained, expected_attachment):
    """Bind the complete attachment to the configured qualified parent and child names."""
    expected = workspace._copy(expected_attachment)
    roots = set(attach.ROOTS)
    source = request["source_binding"]
    _require(
        type(expected) is dict
        and set(expected) == {"contract", "parents", "tables"}
        and expected["contract"] == attach.CONTRACT
        and expected["parents"] == {root: {"schema": source.catalog_schema, "name": root} for root in roots}
        and expected["tables"] == {root: retained["tables"][root] for root in roots},
        "attachment_required",
    )
    return expected


async def _observe(connection, request, retained, expected):
    source = request["source_binding"]
    await attach._compatible(connection, source, retained, expected["parents"], request["owner_role"])
    tables = await attach._capture_attached(connection, source, retained, expected["parents"])
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
        current, current_retained = attach._request(request_by_name)
        _require(
            current == request and current_retained == retained and expected_attachment == expected,
            "inputs_changed",
        )
        _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    return workspace._copy(expected)


def _sealed_expected(request, retained, expected_seal):
    """Keep the complete six-table named payload group separate from its preparation."""
    expected = workspace._copy(expected_seal)
    _require(
        expected
        == {
            "contract": custody.CONTRACT,
            "tables": {root: retained["tables"][root] for root in workspace.storage.RELATIONS},
        },
        "seal_required",
    )
    return expected


async def _closed_payloads(connection, request, retained, sealed):
    names = [retained["tables"][root]["name"] for root in workspace.storage.RELATIONS[:4]]
    observed = await workspace.storage._capture(connection, workspace._policy(retained["prepared_policy"]), names)
    _require(
        all(
            observed[retained["tables"][root]["name"]] == sealed["tables"][root]
            for root in workspace.storage.RELATIONS[:4]
        ),
        "postimage_changed",
    )
    for root in workspace.storage.RELATIONS[:4]:
        receipt = retained["tables"][root]
        relation = storage_relation(receipt["schema"], receipt["name"])
        oid = await connection.fetchval("SELECT to_regclass($1)::oid", relation)
        columns = await connection.fetch(layout._COLUMNS_SQL, [oid], sorted(layout._TYPES))
        layout._require_columns(columns, layout._LAYOUTS[root]["columns"], root, storage_identifier(receipt["schema"]))


async def _payload_writers(connection, request, retained):
    """Payloads remain leaves; writes are prevented by the administrator's native grants."""
    names = [
        storage_relation(receipt["schema"], receipt["name"])
        for root, receipt in retained["tables"].items()
        if root in workspace.storage.RELATIONS
    ]
    _require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM pg_catalog.pg_inherits "
            "WHERE inhparent=ANY(ARRAY(SELECT to_regclass(name) FROM unnest($1::text[]) name)))",
            names,
        )
        is True,
        "unexpected_descendant",
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
            request["migration_role"],
        )
        _require(actual == sealed, "postimage_changed")
    else:
        await _closed_payloads(current.connection, request, retained, sealed)
        actual = await read_attached_cms_payload_snapshots(current.connection, request, expected_attachment=attached)
        _require(actual == attached, "postimage_changed")
    await _payload_writers(current.connection, request, retained)


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
    expected, _source_policy, _policy = admin._inputs(
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
