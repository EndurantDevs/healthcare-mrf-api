# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Equivalent serving ACLs and a real Reader gate for a trusted owner profile."""

import asyncio
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import select, text

from db.connection import current_session
from db.models.custom_import import CustomImportBuildAttempt, CustomImportExecution, CustomImportGeneration
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import family_block_store as store
from process.custom_import.build_graph import _session
from process.custom_import.build_source import (
    _WINDOW,
    _assert_build_identity,
    _page_session,
    _PageWindow,
    _prepare_statement,
    _resolve_build_snapshot,
)
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import (
    FamilyBlockUnavailable,
    resolve_family_block_binding,
    resolve_pinned_family_block_binding,
    validate_family_block_indexes,
)
from process.custom_import.family_block_layout import ROLE_SPECS
from process.custom_import.family_shard_base import _read_deadline, _require_deadline
from process.custom_import.read_contracts import PinnedReadTarget
from process.custom_import.read_identity import resolve_generation_snapshot_binding
from process.custom_import.storage_layout import snapshot_schema

_SERVING = (
    "root_record",
    "entity_binding",
    "generation_family",
    "winner",
    "family",
    "fragment",
    "revision_run",
    "key_dictionary",
    "query_root",
    "query_child",
    "query_record",
)
_EXCLUDED = frozenset({"pack", "rejection", "shard_receipt"})
_CONTRACT = "custom-import/reader-activation/v1"


@dataclass(frozen=True)
class ReaderActivationProfile:
    """One server-owned dataset, original source family and existing Reader login."""

    dataset_id: int
    source_family_id: int
    grantee: str
    grantee_oid: int
    secret: str

    def __post_init__(self):
        for name, maximum in (("dataset_id", 2**63), ("source_family_id", 2**63), ("grantee_oid", 2**32)):
            value = getattr(self, name)
            if type(value) is not int or not 0 < value < maximum:
                raise FamilyBlockUnavailable("Reader activation identity differs")
        if (
            type(self.grantee) is not str
            or re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", self.grantee) is None
            or type(self.secret) is not str
            or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?", self.secret) is None
        ):
            raise FamilyBlockUnavailable("Reader activation profile differs")


def activation_profile(document):
    """Accept only the optional closed binding inside the signed owner profile."""
    if "reader_activation" not in document:
        return None
    value = document["reader_activation"]
    if (
        type(value) is not dict
        or value.keys() != {"contract", "dataset_id", "source_family_id", "grantee", "grantee_oid", "secret"}
        or value["contract"] != _CONTRACT
    ):
        raise FamilyBlockUnavailable("Reader activation profile differs")
    profile = ReaderActivationProfile(
        value["dataset_id"], value["source_family_id"], value["grantee"], value["grantee_oid"], value["secret"]
    )
    if profile.grantee == document["owner_role"] or profile.secret == document["database_secret"]:
        raise FamilyBlockUnavailable("Reader activation requires its independent login")
    return profile


async def _role(session, profile):
    row = (
        await store._execute(
            session,
            """
        SELECT oid::bigint,rolname,rolcanlogin,rolsuper,rolcreaterole,rolbypassrls,
               current_user=rolname AS owner_login
        FROM pg_catalog.pg_roles WHERE oid=:oid AND rolname=:name
    """,
            {"oid": profile.grantee_oid, "name": profile.grantee},
        )
    ).one_or_none()
    if row is None or tuple(row) != (profile.grantee_oid, profile.grantee, True, False, False, False, False):
        raise FamilyBlockUnavailable("Reader login native identity differs")


def _target(request, generation_id):
    return PinnedReadTarget(
        request.dataset_id, generation_id, request.definition_revision_id, request.schema_revision_id, "default"
    )


def _binding_identity(binding):
    return (
        binding.family_id,
        binding.layout.digest,
        binding.owner_oid,
        tuple(sorted(binding.oids_by_role.items())),
        tuple(sorted(binding.columns_sha256_by_role.items())),
    )


async def _source_pin(session, request, profile):
    if request.dataset_id != profile.dataset_id:
        raise FamilyBlockUnavailable("Reader activation dataset differs")
    await _prepare_statement(session)
    family = await session.get(CustomImportSnapshotFamily, profile.source_family_id, populate_existing=True)
    if (
        family is None
        or family.family_id != profile.source_family_id
        or type(family.generation_id) is not int
        or family.generation_id <= 0
        or family.frozen_at is None
        or (family.dataset_id, family.schema_revision_id) != (request.dataset_id, request.schema_revision_id)
    ):
        raise FamilyBlockUnavailable("Reader original source registry differs")
    await _prepare_statement(session)
    generation = await session.get(CustomImportGeneration, family.generation_id, populate_existing=True)
    identity_fields = (
        "generation_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "capture_bundle_id",
        "producing_fence",
        "producing_token_sha256",
    )
    if generation is None or any(getattr(generation, name) != getattr(family, name) for name in identity_fields):
        raise FamilyBlockUnavailable("Reader original source generation differs")
    pinned_source = PinnedReadTarget(
        generation.dataset_id,
        generation.generation_id,
        generation.definition_revision_id,
        generation.schema_revision_id,
        "default",
    )
    pin = await resolve_generation_snapshot_binding(session, pinned_source)
    if pin.family_id != profile.source_family_id or pin.legacy_snapshot is not True:
        raise FamilyBlockUnavailable("Reader activation original source family differs")
    schema = snapshot_schema(profile.source_family_id)
    usage = await store._execute(
        session,
        """
        SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n,
          LATERAL pg_catalog.aclexplode(coalesce(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a
          WHERE n.nspname=:schema AND a.grantee=:role AND a.privilege_type='USAGE')
    """,
        {"schema": schema, "role": profile.grantee_oid},
    )
    if usage.scalar_one() is not True:
        raise FamilyBlockUnavailable("Reader source schema capability is absent")
    for role in _SERVING:
        grantees = await store.reader_capability_plan(
            session, role, prepare=_prepare_statement, source_family_id=pin.family_id
        )
        if profile.grantee not in grantees:
            raise FamilyBlockUnavailable("Reader source capability intersection is absent")
    return pin


async def _acl_state(session, binding, profile, *, complete):
    if set(binding.oids_by_role) != set(_SERVING) | _EXCLUDED:
        raise FamilyBlockUnavailable("Reader candidate role census differs")
    relation_acls = (
        await store._execute(
            session,
            """
        SELECT c.oid::bigint,c.relowner::bigint,a.grantee::bigint,a.privilege_type,a.is_grantable
        FROM pg_catalog.pg_class c LEFT JOIN LATERAL
          pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a ON true
        WHERE c.oid=ANY(CAST(:oids AS oid[]))
    """,
            {"oids": list(binding.oids_by_role.values())},
        )
    ).all()
    by_oid = {oid: role for role, oid in binding.oids_by_role.items()}
    observed, granted = set(), set()
    for oid, owner, grantee, privilege, grantable in relation_acls:
        if owner != binding.owner_oid or oid not in by_oid:
            raise FamilyBlockUnavailable("Reader candidate owner or relation differs")
        observed.add(oid)
        if grantee == owner:
            continue
        if grantee != profile.grantee_oid or by_oid[oid] not in _SERVING or privilege != "SELECT" or grantable:
            raise FamilyBlockUnavailable("Reader candidate contains an unexpected ACL")
        granted.add(by_oid[oid])
    if observed != set(by_oid) or (complete and granted != set(_SERVING)):
        raise FamilyBlockUnavailable("Reader serving ACL is incomplete")
    schema_rows = (
        await store._execute(
            session,
            """
        SELECT n.oid::bigint,n.nspowner::bigint,a.grantee::bigint,a.privilege_type,a.is_grantable
        FROM pg_catalog.pg_namespace n LEFT JOIN LATERAL
          pg_catalog.aclexplode(coalesce(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a ON true
        WHERE n.nspname=:schema
    """,
            {"schema": snapshot_schema(binding.family_id)},
        )
    ).all()
    has_usage = False
    for _oid, owner, grantee, privilege, grantable in schema_rows:
        if owner != binding.owner_oid:
            raise FamilyBlockUnavailable("Reader candidate schema owner differs")
        if grantee == owner:
            continue
        if grantee != profile.grantee_oid or privilege != "USAGE" or grantable:
            raise FamilyBlockUnavailable("Reader candidate schema ACL differs")
        has_usage = True
    if not schema_rows or (complete and not has_usage):
        raise FamilyBlockUnavailable("Reader candidate schema usage is absent")
    return tuple(sorted(tuple(acl) for acl in relation_acls)), tuple(sorted(tuple(acl) for acl in schema_rows))


async def _grant_serving(session, binding, profile):
    quote, _control = await store._control_schema(session)
    schema, grantee = quote.quote_schema(snapshot_schema(binding.family_id)), quote.quote(profile.grantee)
    await store._execute(session, f"GRANT USAGE ON SCHEMA {schema} TO {grantee}")
    for role in _SERVING:
        table = quote.quote(ROLE_SPECS[role][1])
        await store._execute(session, f"GRANT SELECT ON TABLE {schema}.{table} TO {grantee}")


async def _effective_rights(session, binding, profile):
    """USAGE must not expose excluded/default/inherited data or callable objects."""
    quote, control = await store._control_schema(session)
    forbidden_relations = [
        f"{quote.quote_schema(control)}.{name}"
        for name in ("custom_import_family_block_layout", "custom_import_family_block_relation")
    ]
    checked = (
        await store._execute(
            session,
            """
        SELECT NOT EXISTS (
          SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname=:schema AND CASE
            WHEN c.relkind IN ('r','p','v','m','f') THEN (
              pg_catalog.has_table_privilege(CAST(:role AS oid),c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
              OR pg_catalog.has_any_column_privilege(CAST(:role AS oid),c.oid,'INSERT,UPDATE,REFERENCES')
              OR (c.oid<>ALL(CAST(:serving AS oid[])) AND (
                pg_catalog.has_table_privilege(CAST(:role AS oid),c.oid,'SELECT')
                OR pg_catalog.has_any_column_privilege(CAST(:role AS oid),c.oid,'SELECT'))))
            WHEN c.relkind='S' THEN pg_catalog.has_sequence_privilege(CAST(:role AS oid),c.oid,'USAGE,SELECT,UPDATE')
            ELSE false END
        ) AND NOT EXISTS (
          SELECT 1 FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
          JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace,
          LATERAL pg_catalog.aclexplode(a.attacl) acl
          WHERE n.nspname=:schema AND a.attnum>0 AND acl.grantee<>c.relowner
        ) AND NOT EXISTS (
          SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
          WHERE n.nspname=:schema AND pg_catalog.has_function_privilege(CAST(:role AS oid),p.oid,'EXECUTE')
        ) AND NOT EXISTS (
          SELECT 1 FROM unnest(CAST(:control AS text[])) relation
          WHERE pg_catalog.has_table_privilege(CAST(:role AS oid),relation::regclass,'SELECT')
             OR pg_catalog.has_any_column_privilege(CAST(:role AS oid),relation::regclass,'SELECT')
        )
    """,
            {
                "role": profile.grantee_oid,
                "schema": snapshot_schema(binding.family_id),
                "serving": [binding.oids_by_role[role] for role in _SERVING],
                "control": forbidden_relations,
            },
        )
    ).scalar_one()
    if checked is not True:
        raise FamilyBlockUnavailable("Reader has excluded or writable effective candidate capabilities")


async def _sealed_binding(session, request, output, layout):
    generation = await session.get(CustomImportGeneration, output.generation_id)
    execution = await session.get(CustomImportExecution, request.execution_id)
    build = (
        await store._execute(
            session,
            select(CustomImportBuildAttempt).where(
                CustomImportBuildAttempt.execution_id == request.execution_id,
                CustomImportBuildAttempt.producing_fence == request.fence,
                CustomImportBuildAttempt.generation_id == output.generation_id,
            ),
        )
    ).scalar_one_or_none()
    if generation is None or execution is None or build is None or execution.state != "completed":
        raise FamilyBlockUnavailable("Reader publication requires its completed native producer")
    _assert_build_identity(build, request, execution)
    expected_by_name = {
        "dataset_id": request.dataset_id,
        "definition_revision_id": request.definition_revision_id,
        "schema_revision_id": request.schema_revision_id,
        "execution_id": request.execution_id,
        "capture_bundle_id": execution.capture_bundle_id,
        "producing_fence": request.fence,
        "producing_token_sha256": producer_token_sha256(request),
        "base_generation_id": request.expected_base_generation_id,
    }
    if any(getattr(generation, name) != identity for name, identity in expected_by_name.items()):
        raise FamilyBlockUnavailable("Reader sealed generation producer or base differs")
    pin = await resolve_generation_snapshot_binding(session, _target(request, output.generation_id))
    binding = await resolve_pinned_family_block_binding(session, pin, request.definition, layout.collection_slots)
    descriptor = await session.get(CustomImportFamilyBlockLayout, binding.family_id)
    family = await session.get(CustomImportSnapshotFamily, binding.family_id)
    if (
        descriptor is None
        or descriptor.phase != "logged"
        or descriptor.validation_sha256 is None
        or family is None
        or family.frozen_at is None
    ):
        raise FamilyBlockUnavailable("Reader publication candidate is not verified and frozen")
    if any(
        getattr(family, name) != identity for name, identity in expected_by_name.items() if name != "base_generation_id"
    ):
        raise FamilyBlockUnavailable("Reader publication snapshot producer differs")
    await validate_family_block_indexes(session, binding, prepare=_prepare_statement)
    await store._logged_binding(session, binding)
    return pin, binding


@dataclass(frozen=True)
class ReaderActivation:
    """Trusted owner-only setup and repeated prepublication Reader verification."""

    profile: ReaderActivationProfile
    database: object

    def __post_init__(self):
        if type(self.profile) is not ReaderActivationProfile or not callable(
            getattr(self.database, "reader_session", None)
        ):
            raise FamilyBlockUnavailable("Reader activation factory is absent")

    async def install(self, factory, context, *, identity=None):
        """Grant only after original validation/logging, while native writes remain unsealed."""
        request, layout = context.common.request, context.common.layout
        family_id = context.common.source.snapshot_family_id
        async with store._block_session(factory, request, family_id, layout, "logged", expected_identity=identity) as (
            session,
            _descriptor,
            binding,
        ):
            await _role(session, self.profile)
            await _source_pin(session, request, self.profile)
            await _acl_state(session, binding, self.profile, complete=False)
            await _effective_rights(session, binding, self.profile)
            await _grant_serving(session, binding, self.profile)
            await _role(session, self.profile)
            await _source_pin(session, request, self.profile)
            await _acl_state(session, binding, self.profile, complete=True)
            await _effective_rights(session, binding, self.profile)

    async def verify_candidate(self, factory, context):
        """A verified/frozen replay checks durable ACLs without another GRANT."""
        request, layout = context.common.request, context.common.layout
        async with _page_session(factory, request, context.build_id) as (session, _build):
            await store._owner(session)
            family_id = await _resolve_build_snapshot(session, context.build_id)
            if family_id != context.common.source.snapshot_family_id:
                raise FamilyBlockUnavailable("Reader verified build snapshot differs")
            binding = await resolve_family_block_binding(
                session,
                family_id,
                request.definition,
                layout.collection_slots,
                persistence="logged",
                prepare=_prepare_statement,
            )
            descriptor = await session.get(CustomImportFamilyBlockLayout, family_id)
            if (
                binding is None
                or descriptor is None
                or descriptor.phase != "logged"
                or descriptor.validation_sha256 is None
            ):
                raise FamilyBlockUnavailable("Reader verified candidate proof is absent")
            await validate_family_block_indexes(session, binding, prepare=_prepare_statement)
            await _role(session, self.profile)
            await _source_pin(session, request, self.profile)
            await _acl_state(session, binding, self.profile, complete=True)
            await _effective_rights(session, binding, self.profile)

    async def _probe(self, owner, binding, pinned_target, deadline):
        """Use the actual independent Reader and its own protected snapshot pin."""
        async with self.database.reader_session(independent=True) as reader:
            if (
                not self.database.has_reader_session()
                or current_session() is not reader
                or reader.info.get("api_reader_verified") is not True
                or reader.info.get("api_reader_identity", {}).get("role_oid") != self.profile.grantee_oid
            ):
                raise FamilyBlockUnavailable("Reader probe requires the independent native login")
            previous_window = reader.info.get(_WINDOW)
            reader.info[_WINDOW] = _PageWindow(deadline, owner.info[_WINDOW].statement_timeout_ms)
            try:
                await self._probe_snapshot(reader, owner, binding, pinned_target)
            finally:
                if previous_window is None:
                    reader.info.pop(_WINDOW, None)
                else:
                    reader.info[_WINDOW] = previous_window

    async def _probe_snapshot(self, reader, owner, binding, pinned_target):
        quote, control = await store._control_schema(owner)
        schema = quote.quote_schema(snapshot_schema(binding.family_id))
        login = (
            await store._execute(
                reader,
                """
                SELECT current_user,session_user,(SELECT oid::bigint FROM pg_catalog.pg_roles WHERE rolname=current_user),
                       current_setting('transaction_isolation'),current_setting('transaction_read_only'),
                       (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=current_database())
            """,
            )
        ).one()
        database_oid = (
            await store._execute(
                owner, "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=current_database()"
            )
        ).scalar_one()
        if tuple(login) != (
            self.profile.grantee,
            self.profile.grantee,
            self.profile.grantee_oid,
            "repeatable read",
            "on",
            database_oid,
        ):
            raise FamilyBlockUnavailable("Reader probe connection identity differs")
        pin = await resolve_generation_snapshot_binding(reader, pinned_target)
        actual = await resolve_pinned_family_block_binding(
            reader, pin, binding.layout.definition, binding.layout.collection_slots
        )
        if _binding_identity(actual) != _binding_identity(binding):
            raise FamilyBlockUnavailable("Reader probe native snapshot binding differs")
        for role in _SERVING:
            await store._execute(reader, f"SELECT 1 FROM {schema}.{quote.quote(ROLE_SPECS[role][1])} LIMIT 1")
        forbidden_relations = [f"{schema}.{quote.quote(ROLE_SPECS[role][1])}" for role in _EXCLUDED]
        forbidden_relations += [
            f"{quote.quote_schema(control)}.custom_import_family_block_layout",
            f"{quote.quote_schema(control)}.custom_import_family_block_relation",
        ]
        denied = (
            await store._execute(
                reader,
                """
                SELECT bool_and(NOT pg_catalog.has_table_privilege(current_user, relation::regclass, 'SELECT')
                    AND NOT pg_catalog.has_any_column_privilege(current_user, relation::regclass, 'SELECT'))
                FROM unnest(CAST(:relations AS text[])) relation
            """,
                {"relations": forbidden_relations},
            )
        ).scalar_one()
        if denied is not True:
            raise FamilyBlockUnavailable("Reader probe can access excluded import storage")

    @asynccontextmanager
    async def publication_session(self, factory, request, output, layout):
        """Keep native sealed pins, Reader proof and ordinary pointer CAS in one owner transaction."""
        deadline = time.monotonic() + request.statement_timeout_ms / 1000
        native_deadline = _read_deadline(request)
        if native_deadline is not None:
            deadline = min(deadline, native_deadline)
        _require_deadline(deadline)
        async with asyncio.timeout_at(deadline), _session(factory, transaction=True) as session:
            session.info[_WINDOW] = _PageWindow(deadline, request.statement_timeout_ms)
            await _role(session, self.profile)
            await _source_pin(session, request, self.profile)
            pin, binding = await _sealed_binding(session, request, output, layout)
            before = await _acl_state(session, binding, self.profile, complete=True)
            await _effective_rights(session, binding, self.profile)
            await self._probe(session, binding, _target(request, output.generation_id), deadline)
            await _role(session, self.profile)
            await _source_pin(session, request, self.profile)
            after_pin, after_binding = await _sealed_binding(session, request, output, layout)
            if await pin.checked_block_oids(session) != await after_pin.checked_block_oids(
                session
            ) or _binding_identity(after_binding) != _binding_identity(binding):
                raise FamilyBlockUnavailable("Reader candidate identity changed during probe")
            if await _acl_state(session, binding, self.profile, complete=True) != before:
                raise FamilyBlockUnavailable("Reader ACL changed during probe")
            await _effective_rights(session, binding, self.profile)
            yield session
            await _prepare_statement(session)
            await _role(session, self.profile)
            await _source_pin(session, request, self.profile)
            _final_pin, final_binding = await _sealed_binding(session, request, output, layout)
            if _binding_identity(final_binding) != _binding_identity(binding):
                raise FamilyBlockUnavailable("Reader candidate changed before publication commit")
            if await _acl_state(session, binding, self.profile, complete=True) != before:
                raise FamilyBlockUnavailable("Reader ACL changed before publication commit")
            await _effective_rights(session, binding, self.profile)
