# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private mutable CMS outputs, separate from frozen inputs and canonical custody.

The trusted caller authenticates the preparation/profile and owns the outer
publisher transaction. This workspace creates no publication or writer authority.
"""

import asyncio
import hashlib
import json
import re
from contextlib import asynccontextmanager
from dataclasses import asdict
from uuid import UUID

from sqlalchemy import CheckConstraint, Column
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateColumn

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_context as context
from process import provider_directory_generic_source_storage as storage
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_cms_canonical_layout import _LAYOUTS
from process.provider_directory_cms_identity_candidate import (
    _SECONDARY,
    CMS_IDENTITY_KEYS,
    copy_identity_rows_native,
)
from process.provider_directory_generic_source_policy import GenericSourcePolicy, _savepoint
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff

CONTRACT = "cms-prepared-private-workspace-v2"
OUTPUT_ROOTS = (
    *storage.AUXILIARY_ROOTS,
    *storage.RELATIONS[1:4],
    storage.RELATIONSHIP,
    storage.RESOURCE,
    storage.WITNESS,
)
_GRAPH_COLUMNS = (
    ("dataset_id", "varchar(96) NOT NULL"),
    ("source_id", "varchar(64) NOT NULL"),
    ("release_id", "varchar(256) NOT NULL"),
    ("resource_type", "varchar(64) NOT NULL"),
    ("resource_id", "varchar(256) NOT NULL"),
    ("source_payload_hash", "varchar(64) NOT NULL"),
    ("raw_payload_sha256", "varchar(64) NOT NULL"),
    ("reference_field", "varchar(64) NOT NULL"),
    ("parent_ordinal", "integer NOT NULL"),
    ("reference_ordinal", "integer NOT NULL"),
    ("target_type", "varchar(64) NOT NULL"),
    ("target_reference", "text"),
    ("target_resource_id", "varchar(256)"),
    ("resolution_status", "varchar(16) NOT NULL"),
    ("period_start", "text"),
    ("period_end", "text"),
)


def _require(condition, code):
    if not condition:
        raise ValueError("cms_prepared_workspace_" + code)


def _copy(value):
    return json.loads(storage._encoded(value))


def _payload_constraints(root):
    """Preserve the original status CHECK parse tree required for native ATTACH."""
    return tuple(
        (
            key,
            kind,
            "CHECK (resolution_status IN ('resolved', 'unresolved', 'ambiguous'))"
            if key == "cms_npd_relationship_status_check"
            else definition,
            columns,
        )
        for key, kind, definition, columns in _LAYOUTS[root]["constraints"]
    )


async def _payload_indexes(connection, policy, root, name):
    """Use the same fixed native definitions as the future snapshot parents."""
    relation = storage_relation(policy.schema, name)
    for _key, kind, definition, _columns in _LAYOUTS[root]["constraints"]:
        if kind in {"p", "u"}:
            await connection.execute(f"ALTER TABLE {relation} ADD {definition}")
    for index, (_key, definition, *_semantics, unique, primary) in enumerate(_LAYOUTS[root]["indexes"]):
        if not primary:
            method = definition.split(" USING ", 1)[1]
            await connection.execute(
                f"CREATE {'UNIQUE ' if unique else ''}INDEX {storage_identifier(name + '_i' + str(index))} "
                f"ON {relation} USING {method}"
            )
    await connection.execute(f"ANALYZE {relation}")


def _policy(value):
    _require(
        type(value) is dict
        and set(value) == {"schema", "owner_role", "publisher_role", "runtime_roles", "control_runtime_roles"},
        "policy_changed",
    )
    return GenericSourcePolicy(
        **{
            **value,
            "runtime_roles": tuple(value["runtime_roles"]),
            "control_runtime_roles": tuple(value["control_runtime_roles"]),
        }
    )


def _binding(source_binding, prepared_inputs, handoff, preparation_id, admission_sha256):
    _require(type(handoff) is FHIRSourceCandidateHandoff, "handoff_required")
    _require(type(preparation_id) is str and str(UUID(preparation_id)) == preparation_id, "preparation_invalid")
    _require(type(admission_sha256) is str and re.fullmatch(r"[0-9a-f]{64}", admission_sha256), "admission_invalid")
    _require(prepared_inputs["source_storage"] == asdict(source_binding), "source_binding_changed")
    document = handoff.document
    expected_storage_by_field = {**asdict(source_binding), "producer": document["storage"].get("producer")}
    storage.validate_handoff_document(document, expected_storage_by_field)
    _require(document["contract"] == storage.CMS_ISOLATED_CONTRACT, "cms_handoff_required")
    declared_by_oid = {item["oid"]: item["name"] for item in document["tables"].values()}
    frozen = prepared_inputs["frozen_inventory"]["relations"]
    _require({row["relation_oid"]: row["relation_name"] for row in frozen} == declared_by_oid, "frozen_handoff_changed")
    return _copy(
        {
            "contract": CONTRACT,
            "preparation_id": preparation_id,
            "admission_sha256": admission_sha256,
            "handoff_sha256": handoff.document_sha256,
            "source_storage": asdict(source_binding),
            "producer": document["storage"]["producer"],
            "identity": document["identity"],
            "prepared_policy": prepared_inputs["prepared_policy"],
        }
    )


def _names(binding):
    digest = hashlib.sha256(storage._encoded(binding).encode()).hexdigest()[:32]
    return {root: "pdgs_" + digest + "_" + storage._SUFFIXES[root] for root in OUTPUT_ROOTS}


def _layouts():
    """Render code-owned columns/checks only; no live template, FK or custom hook."""
    from db import models

    names = (
        "ProviderDirectoryEntitySourceBinding",
        "ProviderDirectoryEntityReleaseEvidence",
        "ProviderDirectoryInsuranceNetworkSourceBinding",
        "ProviderDirectoryInsuranceNetworkPlanEvidence",
        "ProviderDirectoryResourceIdentity",
    )
    layouts_by_root = {}
    for root, name in zip(storage.AUXILIARY_ROOTS, names, strict=True):
        table = getattr(models, name).__table__
        _require(table.name == root, "layout_changed")
        columns = [
            Column(
                column.name,
                column.type,
                *([column.computed.copy()] if column.computed is not None else []),
                nullable=column.nullable,
            )
            for column in table.columns
        ]
        layouts_by_root[root] = (
            [str(CreateColumn(column).compile(dialect=dialect())) for column in columns],
            tuple(column.name for column in columns if column.computed is None),
            [str(check.sqltext) for check in table.constraints if isinstance(check, CheckConstraint)],
        )
    # Model metadata omits migration-owned JSONB, defaults and CHECK semantics.
    for root in storage.RELATIONS:
        spec, columns = _LAYOUTS[root], []
        for name, kind, required, generated, expression in spec["columns"]:
            _require(generated in {"", "s"}, "layout_changed")
            definition = storage_identifier(name) + " " + kind
            if generated:
                definition += " GENERATED ALWAYS AS (" + expression + ") STORED"
            elif expression is not None:
                definition += " DEFAULT " + expression
            columns.append(definition + (" NOT NULL" if required else ""))
        checks = [definition[7:-1] for _, kind, definition, _ in _payload_constraints(root) if kind == "c"]
        _require(not any("__schema__" in clause for clause in (*columns, *checks)), "layout_changed")
        layouts_by_root[root] = (
            columns,
            tuple(name for name, _kind, _required, generated, _expression in spec["columns"] if not generated),
            checks,
        )
    return layouts_by_root


class CMSPreparedWorkspace:
    """Task-local mutable private tables; receipts are observations, not authority."""

    def __init__(self, database, session, connection, source_binding, prepared_inputs, binding, receipts):
        self.database, self.session, self.connection = database, session, connection
        self.source_binding = source_binding
        self._inputs, self._binding, self._receipts = _copy(prepared_inputs), _copy(binding), _copy(receipts)
        self.policy = _policy(prepared_inputs["prepared_policy"])
        self._task, self._transaction = asyncio.current_task(), session.get_transaction()
        self._active, self._indexed = True, False
        self._readonly = False
        self._payload_postimages = None

    @property
    def receipts(self):
        """Return detached native output receipts without granting custody."""
        return _copy(self._receipts)

    @property
    def identity(self):
        """Return the exact detached acquisition identity."""
        return _copy(self._binding["identity"])

    @property
    def catalog_schema(self):
        """Return the original canonical schema coordinate."""
        return self.source_binding.catalog_schema

    @property
    def binding(self):
        """Return detached workspace identity and current output receipts."""
        return {**_copy(self._binding), "tables": self.receipts, "indexed": self._indexed}

    async def verify_current(self):
        """Repeat original owner-session and exact private object checks before IO."""
        _require(self._active, "scope_ended")
        _require(asdict(self.source_binding) == self._inputs["source_storage"], "source_binding_changed")
        await context._physical(self.database, self.session, self.connection, self._task, self._transaction)
        actor_by_field = dict(await self.connection.fetchrow(context._ORIGIN_SQL))
        _require(actor_by_field == self._inputs["origin"], "original_publisher_changed")
        if self._payload_postimages is not None:
            from process.provider_directory_cms_snapshot_readback import verify_sealed_cms_workspace

            _require(self._readonly and self._indexed, "readonly_workspace")
            await verify_sealed_cms_workspace(self, **self._payload_postimages)
            return self.receipts
        names = _names(self._binding)
        observed = await storage._capture(self.connection, self.policy, list(names.values()))
        _require({root: observed[name] for root, name in names.items()} == self._receipts, "output_changed")
        return self.receipts

    async def copy_rows(self, root, columns, records):
        """Use the existing bounded typed COPY on one fixed new output heap."""
        _require(not self._readonly, "readonly_workspace")
        _require(root in OUTPUT_ROOTS and tuple(columns) == _layouts()[root][1], "copy_target_changed")
        _require(not self._indexed, "already_indexed")
        await self.verify_current()
        scope_map = {
            "dataset_id": self.identity["dataset_id"],
            "source_id": "cms-npd",
            "release_id": self.identity["metadata"]["source_release"]["vector_sha256"],
        }

        def _scoped_rows():
            for row in records:
                _require(type(row) is tuple and len(row) == len(columns), "copy_record_invalid")
                _require(
                    all(value == scope_map[key] for key, value in zip(columns, row, strict=True) if key in scope_map),
                    "copy_identity_changed",
                )
                yield row

        async with _savepoint(self.connection):
            await self.connection.execute(f"SET LOCAL ROLE {storage_identifier(self.policy.owner_role)}")
            count = await copy_identity_rows_native(
                self.connection, self.session, self.policy.schema, self._receipts[root]["name"], columns, _scoped_rows()
            )
            await self.connection.execute("RESET ROLE")
            await self.verify_current()
        return count

    async def finish_indexes(self):
        """Build only the fixed existing keys/indexes after private COPY finishes."""
        _require(not self._readonly, "readonly_workspace")
        await self.verify_current()
        _require(not self._indexed, "already_indexed")
        async with _savepoint(self.connection):
            await self.connection.execute(f"SET LOCAL ROLE {storage_identifier(self.policy.owner_role)}")
            for root, receipt in self._receipts.items():
                name = receipt["name"]
                relation = storage_relation(self.policy.schema, name)
                if root not in storage.AUXILIARY_ROOTS:
                    await _payload_indexes(self.connection, self.policy, root, name)
                    continue
                secondary = _SECONDARY.get(root, (("source_id", "resource_type", "entity_id"),))
                for index, fields in enumerate(secondary):
                    unique = "UNIQUE " if root == storage.CMS_RESOURCE_IDENTITY_ROOT else ""
                    await self.connection.execute(
                        f"CREATE {unique}INDEX {storage_identifier(name + '_i' + str(index))} ON {relation} "
                        f"({','.join(storage_identifier(key) for key in fields)})"
                    )
                # Match content-proof ordering without changing the native map keys.
                content_keys = CMS_IDENTITY_KEYS.get(root, ("source_id", "resource_type", "resource_id"))
                content_fields = ",".join(storage_identifier(key) + ' COLLATE "C"' for key in content_keys)
                await self.connection.execute(
                    f"CREATE INDEX {storage_identifier(name + '_content_seek')} ON {relation} ({content_fields})"
                )
                await self.connection.execute(f"ANALYZE {relation}")
            observed = await storage._capture(
                self.connection, self.policy, [output_receipt["name"] for output_receipt in self._receipts.values()]
            )
            await self.connection.execute("RESET ROLE")
        self._receipts = {root: observed[receipt["name"]] for root, receipt in self._receipts.items()}
        self._indexed = True
        return await self.verify_current()


async def _allocate_private_outputs(connection, policy, names, source_policy, expected_by_field, dataset_id):
    """Check roles and allocate only the fresh private outputs in the caller savepoint."""
    owner, _ = await storage_roles(connection, policy.schema, **policy.role_arguments())
    _require(owner == expected_by_field["native_coordinates"]["prepared_owner_oid"], "prepared_owner_changed")
    _require(
        await connection.fetchval(
            "SELECT pg_has_role(current_user,$1::oid,'USAGE')",
            owner,
        ),
        "prepared_owner_inheritance_required",
    )
    ordinary_roles = [source_policy.owner_role, source_policy.publisher_role, *source_policy.runtime_roles]
    _require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]) "
            "AND (pg_has_role(oid,$2::oid,'SET') OR pg_has_role(oid,$2::oid,'USAGE')))",
            ordinary_roles,
            owner,
        ),
        "source_reaches_prepared_owner",
    )
    _require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM unnest($1::text[]) name WHERE to_regclass(name) IS NOT NULL)",
            [storage_relation(policy.schema, name) for name in names.values()],
        ),
        "output_exists",
    )
    dataset_literal = await connection.fetchval("SELECT pg_catalog.quote_literal($1::text)", dataset_id)
    _require(type(dataset_literal) is str, "dataset_literal_required")
    await connection.execute(f"SET LOCAL ROLE {storage_identifier(policy.owner_role)}")
    layouts = _layouts()
    for root, name in names.items():
        columns, _, checks = layouts[root]
        definition = columns + ["CHECK (" + check + ")" for check in checks]
        if root in storage.RELATIONS:
            # ATTACH requires matching CHECK names as well as expressions.
            definition = columns + [
                "CONSTRAINT " + storage_identifier(key) + " " + constraint_sql
                for key, kind, constraint_sql, _fields in _payload_constraints(root)
                if kind == "c"
            ]
            definition.append('CONSTRAINT "pd_dataset_bound" CHECK (dataset_id=' + dataset_literal + ")")
        if root in storage.AUXILIARY_ROOTS:
            # Batched identity observations need indexed map keys during loading.
            keys = CMS_IDENTITY_KEYS.get(root, ("source_id", "resource_type", "resource_id"))
            definition.append("PRIMARY KEY (" + ",".join(storage_identifier(key) for key in keys) + ")")
        await connection.execute(
            f"CREATE TABLE {storage_relation(policy.schema, name)} ({','.join(definition)})"
            + storage.PRIVATE_HEAP_OPTIONS
        )
    observed = await storage._capture(connection, policy, list(names.values()))
    await connection.execute("RESET ROLE")
    return observed


@asynccontextmanager
async def prepared_cms_workspace(
    database, session, connection, *, source_binding, prepared_inputs, handoff, preparation_id, admission_sha256
):
    """Create eleven fresh owner-private outputs in the caller's rollback savepoint.

    The supplied observation/handoff is data. The trusted caller must separately
    verify protected profile/claim and frozen content. No frozen header, input,
    canonical root, grant or SOURCE context is modified. Nothing is published.
    Existing names refuse; failed/canceled work rolls back every created object.
    Successful outputs remain in the caller-owned outer transaction.
    """
    expected = _copy(prepared_inputs)
    source_policy, policy = _policy(expected["source_policy"]), _policy(expected["prepared_policy"])
    binding = _binding(source_binding, expected, handoff, preparation_id, admission_sha256)
    names = _names(binding)
    frozen_names = {input_relation["relation_name"] for input_relation in expected["frozen_inventory"]["relations"]}
    _require(not set(names.values()) & frozen_names, "input_output_alias")
    _require(isinstance(database, context.Database) and isinstance(session, context.AsyncSession), "session_required")
    await context._physical(database, session, connection, asyncio.current_task(), session.get_transaction())
    async with (
        _savepoint(connection),
        context.observe_prepared_cms_inputs(
            database,
            session,
            connection,
            source_binding=source_binding,
            source_policy=source_policy,
            prepared_policy=policy,
            frozen_inventory=expected["frozen_inventory"],
        ) as actual,
    ):
        _require(actual == expected and prepared_inputs == expected, "prepared_inputs_changed")
        async with _savepoint(connection):
            observed = await _allocate_private_outputs(
                connection, policy, names, source_policy, expected, binding["identity"]["dataset_id"]
            )
            workspace = CMSPreparedWorkspace(
                database,
                session,
                connection,
                source_binding,
                expected,
                binding,
                {root: observed[name] for root, name in names.items()},
            )
            try:
                await workspace.verify_current()
                yield workspace
                _require(
                    prepared_inputs == expected and handoff.document_sha256 == binding["handoff_sha256"],
                    "prepared_inputs_changed",
                )
                await workspace.verify_current()
            finally:
                workspace._active = False


def _retained_workspace(source_binding, expected, handoff, expected_workspace):
    """Validate exact retained private identities before acquiring native object pins."""
    retained = _copy(expected_workspace)
    _require(type(retained) is dict, "retained_binding_changed")
    binding = _binding(
        source_binding, expected, handoff, retained.get("preparation_id"), retained.get("admission_sha256")
    )
    _require(
        type(retained) is dict
        and set(retained) == {*binding, "tables", "indexed"}
        and retained["indexed"] is True
        and {key: retained[key] for key in binding} == binding,
        "retained_binding_changed",
    )
    names, receipts = _names(binding), retained["tables"]
    _require(type(receipts) is dict and set(receipts) == set(OUTPUT_ROOTS), "retained_outputs_changed")
    for root, name in names.items():
        receipt = receipts[root]
        _require(
            type(receipt) is dict
            and set(receipt) == {"name", "oid", "owner", "layout_sha256"}
            and receipt["name"] == name
            and type(receipt["oid"]) is int
            and 0 < receipt["oid"] < 2**32
            and receipt["owner"] == expected["native_coordinates"]["prepared_owner_oid"]
            and type(receipt["layout_sha256"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", receipt["layout_sha256"]),
            "retained_outputs_changed",
        )
    frozen_oids = {receipt_by_field["relation_oid"] for receipt_by_field in expected["frozen_inventory"]["relations"]}
    _require(
        len({receipt_by_field["oid"] for receipt_by_field in receipts.values()}) == len(OUTPUT_ROOTS)
        and not frozen_oids & {receipt_by_field["oid"] for receipt_by_field in receipts.values()},
        "input_output_alias",
    )
    return retained, binding, receipts


@asynccontextmanager
async def reopen_prepared_cms_workspace(
    database,
    session,
    connection,
    *,
    source_binding,
    prepared_inputs,
    handoff,
    expected_workspace,
):
    """Read exact retained outputs on a fresh original Publisher transaction.

    The protected caller authenticates the retained binding and data evidence.
    SHARE pins exclude peer DML and replacement through the outer transaction.
    Post-commit custody, canonical writer protection and complete authenticated
    content comparisons remain the protected caller's responsibility.
    Existing outputs are never allocated, repaired, indexed or made writable.
    """
    expected = _copy(prepared_inputs)
    source_policy, policy = _policy(expected["source_policy"]), _policy(expected["prepared_policy"])
    retained, binding, receipts = _retained_workspace(source_binding, expected, handoff, expected_workspace)
    _require(isinstance(database, context.Database) and isinstance(session, context.AsyncSession), "session_required")
    await context._physical(database, session, connection, asyncio.current_task(), session.get_transaction())
    async with context.observe_prepared_cms_inputs(
        database,
        session,
        connection,
        source_binding=source_binding,
        source_policy=source_policy,
        prepared_policy=policy,
        frozen_inventory=expected["frozen_inventory"],
    ) as actual:
        _require(actual == expected and prepared_inputs == expected, "prepared_inputs_changed")
        workspace = CMSPreparedWorkspace(
            database,
            session,
            connection,
            source_binding,
            expected,
            binding,
            receipts,
        )
        workspace._indexed, workspace._readonly = True, True
        try:
            await workspace.verify_current()
            relations = sorted(storage_relation(policy.schema, name) for name in _names(binding).values())
            await connection.execute(
                "LOCK TABLE " + ",".join("ONLY " + name for name in relations) + " IN SHARE MODE NOWAIT"
            )
            await workspace.verify_current()
            yield workspace
            _require(
                prepared_inputs == expected
                and expected_workspace == retained
                and handoff.document_sha256 == binding["handoff_sha256"],
                "prepared_inputs_changed",
            )
            await workspace.verify_current()
        finally:
            workspace._active = False
