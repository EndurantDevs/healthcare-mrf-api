# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Initialize derived contexts atomically inside an already fenced output page."""

import hmac

from sqlalchemy import and_, select, text
from sqlalchemy.dialects import postgresql

from db.models.custom_import import CustomImportDefinitionRevision, CustomImportGeneration
from db.models.custom_import_query_projection import CustomImportQueryProjection
from db.models.custom_import_storage import CustomImportSnapshotFamily, CustomImportSnapshotRelation
from process.custom_import.build_source import _flush_page, _prepare_statement
from process.custom_import.execution import lease_token_sha256
from process.custom_import.query_projection_binding import lock_projection_tables, projection_table_identity
from process.custom_import.query_projection_sources import capability_sha256
from process.custom_import.query_projection_storage import QUERY_PROJECTION_CONTRACT, query_projection_load_statements
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import snapshot_schema

_PREPARER = postgresql.dialect().identifier_preparer
_OWNER = text(
    """
    SELECT r.oid::bigint AS current_owner,n.nspowner::bigint AS schema_owner,
        c.relowner::bigint AS table_owner,c.oid::bigint AS table_oid,
        c.relkind::text AS relkind,c.relpersistence::text AS relpersistence
    FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid
    JOIN pg_catalog.pg_roles r ON r.rolname=current_user
    WHERE n.nspname=:schema_name AND c.relname='custom_import_root_record'
    """
)
_RELATIONS = text(
    "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
    "WHERE n.nspname=:schema_name AND c.relname IN ('custom_import_derived_root','custom_import_derived_child')"
)
_ACL = text(
    """
    SELECT DISTINCT acl.grantee::bigint AS grantee,r.rolname::text AS role_name
    FROM pg_catalog.pg_class c,
        LATERAL pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) acl
    LEFT JOIN pg_catalog.pg_roles r ON r.oid=acl.grantee
    WHERE c.oid=CAST(:table_oid AS oid) AND acl.grantee<>c.relowner
        AND (NOT :select_only OR acl.privilege_type='SELECT')
    """
)


async def _execute(session, statement, parameters=None, *, prepare=None):
    await (prepare or _prepare_statement)(session)
    return await session.execute(statement, parameters or {})


def _require_definition(definition, saved_definition, expected_by_name):
    if (
        saved_definition.contract_version != "custom-import/v1"
        or any(
            getattr(saved_definition, name) != expected_by_name[name]
            for name in ("dataset_id", "definition_revision_id", "schema_revision_id")
        )
        or not hmac.compare_digest(bytes(saved_definition.definition_sha256), bytes.fromhex(definition.digest))
    ):
        raise CandidateRunnerError("query projection definition differs")


async def _parent(session, request, build):
    if (
        not session.in_transaction()
        or build.phase != "output"
        or type(build.generation_id) is not int
        or not 0 < build.generation_id < 2**63
    ):
        raise CandidateRunnerError("query projection initialization requires a live output page")
    expected_by_name = {
        **{
            name: getattr(request, name)
            for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id")
        },
        "generation_id": build.generation_id,
        "capture_bundle_id": build.capture_bundle_id,
        "producing_fence": request.fence,
    }
    token = lease_token_sha256(request.lease_token)
    if any(getattr(build, name) != expected for name, expected in expected_by_name.items()) or not hmac.compare_digest(
        bytes(build.producing_token_sha256), token
    ):
        raise CandidateRunnerError("query projection snapshot producer differs")
    return await projection_parent(session, request.definition, expected_by_name, token)


async def projection_parent(session, definition, expected_by_name, token, *, prepare=None):
    """Bind real producer identities independently of their caller's transaction kind."""

    family, generation = CustomImportSnapshotFamily, CustomImportGeneration
    definition_model, relation = CustomImportDefinitionRevision, CustomImportSnapshotRelation
    parent_records = (
        await _execute(
            session,
            select(family, generation, definition_model, relation)
            .join(generation, generation.generation_id == family.generation_id)
            .join(definition_model, definition_model.definition_revision_id == family.definition_revision_id)
            .join(relation, and_(relation.family_id == family.family_id, relation.relation_slot == 1))
            .where(
                family.execution_id == expected_by_name["execution_id"],
                family.producing_fence == expected_by_name["producing_fence"],
            )
            .execution_options(populate_existing=True),
            prepare=prepare,
        )
    ).one_or_none()
    if parent_records is None:
        raise CandidateRunnerError("query projection snapshot parent is missing")
    parent, generation_row, definition_row, root_relation = parent_records
    if parent.frozen_at is not None or any(
        getattr(producer, name) != expected
        for producer in (parent, generation_row)
        for name, expected in expected_by_name.items()
    ):
        raise CandidateRunnerError("query projection snapshot producer differs or is frozen")
    if any(
        not hmac.compare_digest(bytes(producer.producing_token_sha256), token) for producer in (parent, generation_row)
    ):
        raise CandidateRunnerError("query projection snapshot producer differs")
    _require_definition(definition, definition_row, expected_by_name)
    return parent, root_relation


async def _owner(session, parent, root_relation, *, prepare=None):
    row = (
        (await _execute(session, _OWNER, {"schema_name": snapshot_schema(parent.family_id)}, prepare=prepare))
        .mappings()
        .one_or_none()
    )
    if (
        row is None
        or row["current_owner"] != row["schema_owner"]
        or row["current_owner"] != row["table_owner"]
        or row["table_oid"] != root_relation.table_oid
        or row["table_owner"] != root_relation.table_owner
        or row["relkind"] != "r"
        or row["relpersistence"] != "p"
    ):
        raise CandidateRunnerError("query projection requires the existing snapshot owner")
    return row["current_owner"]


def _grantee(row):
    if row["grantee"] == 0:
        return "PUBLIC"
    if not row["role_name"]:
        raise CandidateRunnerError("query projection ACL role is missing")
    return _PREPARER.quote_identifier(row["role_name"])


async def _close_and_copy_acl(session, parent, table_name, table_oid, readers, *, prepare=None):
    target = f"{_PREPARER.quote_identifier(snapshot_schema(parent.family_id))}.{_PREPARER.quote_identifier(table_name)}"
    grants = (
        (await _execute(session, _ACL, {"table_oid": table_oid, "select_only": False}, prepare=prepare))
        .mappings()
        .all()
    )
    await _execute(session, text(f"REVOKE ALL ON TABLE {target} FROM PUBLIC"), prepare=prepare)
    for grant in grants:
        if grant["grantee"] != 0:
            await _execute(session, text(f"REVOKE ALL ON TABLE {target} FROM {_grantee(grant)}"), prepare=prepare)
    for reader in readers:
        if reader["grantee"] != 0:
            await _execute(session, text(f"GRANT SELECT ON TABLE {target} TO {_grantee(reader)}"), prepare=prepare)


async def _create_cells(session, parent, root_relation, definition, *, prepare=None):
    """Close inherited privileges before retaining the two fresh table identities."""

    existing = await _execute(session, _RELATIONS, {"schema_name": snapshot_schema(parent.family_id)}, prepare=prepare)
    if existing.first() is not None:
        raise CandidateRunnerError("query projection refuses unregistered existing cell relations")
    readers = (
        (await _execute(session, _ACL, {"table_oid": root_relation.table_oid, "select_only": True}, prepare=prepare))
        .mappings()
        .all()
    )
    for statement in query_projection_load_statements(parent.family_id, definition):
        await _execute(session, text(statement), prepare=prepare)
    custody_by_column = {}
    for scope in ("root", "child"):
        name = f"custom_import_derived_{scope}"
        await (prepare or _prepare_statement)(session)
        if prepare is None:
            oid, owner, signature = await projection_table_identity(session, parent.family_id, name, definition)
        else:
            oid, owner, signature = await projection_table_identity(
                session, parent.family_id, name, definition, prepare=prepare
            )
        await _close_and_copy_acl(session, parent, name, oid, readers, prepare=prepare)
        custody_by_column.update(
            {f"{scope}_table_oid": oid, f"{scope}_table_owner": owner, f"{scope}_columns_sha256": signature}
        )
    return custody_by_column


async def initialize_projection(session, request, build):
    """Create only absent cells, or replay exact custody without adopting caller tables."""

    parent, root_relation = await _parent(session, request, build)
    return await initialize_projection_parent(session, request.definition, parent, root_relation)


async def initialize_projection_parent(session, definition, parent, root_relation, *, prepare=None, flush=None):
    """Share native owner, ACL and manifest setup after exact parent validation."""

    await _owner(session, parent, root_relation, prepare=prepare)
    expected_definition = bytes.fromhex(definition.digest)
    expected_capability = capability_sha256(definition)
    manifest = (
        await _execute(
            session,
            select(CustomImportQueryProjection)
            .where(CustomImportQueryProjection.family_id == parent.family_id)
            .execution_options(populate_existing=True),
            prepare=prepare,
        )
    ).scalar_one_or_none()
    if manifest is not None:
        if not hmac.compare_digest(bytes(manifest.definition_sha256), expected_definition) or not hmac.compare_digest(
            bytes(manifest.capability_sha256), expected_capability
        ):
            raise CandidateRunnerError("query projection replay definition or capability differs")
        await (prepare or _prepare_statement)(session)
        if prepare is None:
            await lock_projection_tables(session, manifest, definition, require_complete=False)
        else:
            await lock_projection_tables(session, manifest, definition, require_complete=False, prepare=prepare)
        return manifest
    custody_by_column = await _create_cells(session, parent, root_relation, definition, prepare=prepare)
    manifest = CustomImportQueryProjection(
        **{
            name: getattr(parent, name)
            for name in (
                "family_id",
                "generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "producing_fence",
                "producing_token_sha256",
            )
        },
        projection_contract=QUERY_PROJECTION_CONTRACT,
        definition_sha256=expected_definition,
        capability_sha256=expected_capability,
        phase="root",
        after_entity_binding_id=0,
        root_count=0,
        child_count=0,
        index_count=0,
        **custody_by_column,
    )
    session.add(manifest)
    await (flush or _flush_page)(session)
    return manifest
