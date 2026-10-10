# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain a prepared Doctors family for one caller-owned common publication."""

from __future__ import annotations

import asyncio
import importlib
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import text

from process import provider_directory_cms_serving_receipt as common_receipts
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process.cms_doctors_source_provenance import mint_doctors_source_provenance

_IMPORTER = "cms-doctors"
_SEAL_TRIGGER = "cms_doctors_prepared_write_seal"
_SEAL_BODY = "BEGIN RAISE EXCEPTION 'cms_doctors_prepared_read_only'; END;"


def _native():
    """Load the importer lazily so its public entry points can import this helper."""
    return importlib.import_module("process.cms_doctors")


def _models():
    """Use the archive's closed ordered native family."""
    return archive.reference_family_spec(_IMPORTER).model_types


@dataclass
class PreparedCMSDoctorsGeneration:
    """Exact local stages and the incumbent they may replace, without early authority."""

    schema: str
    import_date: str
    stage_oids: tuple[tuple[str, str, int], ...]
    incumbent: archive.ReferenceFamilyIncumbent
    incumbent_authority: generation.ReferenceFamilyResultGenerationAuthority
    metrics: dict
    context: dict
    native_receipt: generation.ReferenceFamilyResultGenerationAuthority | None = None
    committed: bool = False
    sealed_filenodes: tuple[tuple[int, int], ...] = ()
    source_provenance_json: str | None = None

    @property
    def relation_overrides(self) -> dict[str, str]:
        """Expose staged inputs for dependent preparation without changing public selectors."""
        return {target: stage for target, stage, _oid in self.stage_oids}

    async def mark_committed(self, receipt_id: str, payload: dict) -> None:
        """Consume ownership only from this family's exact committed common receipt."""
        native = _native()
        if native.db._transaction_binding() is not None or self.native_receipt is None:
            raise RuntimeError("cms_doctors_preparation_commit_pending")
        authority = generation.validate_reference_family_result_generation_authority(payload.get("doctors"))
        if authority != self.native_receipt or authority.relation_oids != tuple(oid for _, _, oid in self.stage_oids):
            raise RuntimeError("cms_doctors_preparation_receipt_changed")
        async with native.db.session() as session:
            if not await common_receipts.verify_historical_receipt(session, self.schema, receipt_id, payload):
                raise RuntimeError("cms_doctors_preparation_commit_unproved")
        self.committed = True
        self.context["publication_state"] = "published"
        self.metrics.update(published=True, publication_state="published")


async def _stage_inventory(session, schema, import_date):
    """Capture only existing relations under this completed import's closed family names."""
    owned_stages = []
    for model in _models():
        stage = _native().make_class(model, import_date).__tablename__
        oid = await session.scalar(
            text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": f"{schema}.{stage}"}
        )
        if oid is not None:
            owned_stages.append((model.__tablename__, stage, int(oid)))
    return tuple(owned_stages)


async def _assert_stage(session, schema, stage, expected_oid, *, logged):
    """Reject a missing or replaced physical table before touching its name."""
    observed = (
        await session.execute(
            text(
                "SELECT oid::bigint,relpersistence::text,pg_relation_filenode(oid)::bigint "
                "FROM pg_class WHERE oid=to_regclass(:relation) AND relkind='r'"
            ),
            {"relation": f"{schema}.{stage}"},
        )
    ).one_or_none()
    if observed is None or observed[0] != expected_oid or (logged and observed[1] != "p"):
        raise RuntimeError("cms_doctors_prepared_stage_changed")
    return int(observed[2])


async def _assert_stage_indexes(session, schema, stage_cls):
    """Require all declared serving indexes to be valid on this exact stage."""
    native = _native()
    suffixes = ["primary"] if getattr(stage_cls, "__my_index_elements__", ()) else []
    suffixes += [
        index.get("name", "_".join(index["index_elements"]))
        for index in getattr(stage_cls, "__my_additional_indexes__", ())
    ]
    names = [native._stage_index_name(stage_cls.__tablename__, suffix) for suffix in suffixes]
    count = await session.scalar(
        text(
            "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
            "WHERE i.indrelid=to_regclass(:relation) AND c.relname=ANY(CAST(:names AS text[])) "
            "AND i.indisvalid AND i.indisready AND i.indislive"
        ),
        {"relation": f"{schema}.{stage_cls.__tablename__}", "names": names},
    )
    if count != len(names):
        raise RuntimeError("cms_doctors_prepared_indexes_missing")


async def _finalize_stages(prepared):
    """Finish persistence, indexes, and statistics outside the serving transaction."""
    native = _native()
    for model, (_target, stage, oid) in zip(_models(), prepared.stage_oids, strict=True):
        async with native.db.transaction() as session:
            await archive._lock_family(session, prepared.schema, (stage,), "ACCESS EXCLUSIVE", nowait=True)
            await _assert_stage(session, prepared.schema, stage, oid, logged=False)
            stage_cls = native.make_class(model, prepared.import_date)
            await native._create_stage_indexes(stage_cls, prepared.schema)
            await session.execute(text(f'ALTER TABLE "{prepared.schema}"."{stage}" SET LOGGED'))
            await session.execute(text(f'ANALYZE "{prepared.schema}"."{stage}"'))
            await _assert_stage_indexes(session, prepared.schema, stage_cls)


async def _assert_seal_function(session, schema):
    """Require the migrated unconditional statement rejection function, including its execution settings."""
    function_oid = await session.scalar(
        text(
            "SELECT p.oid::bigint FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang "
            "WHERE p.oid=to_regprocedure(:function) AND p.prosrc=:body AND l.lanname='plpgsql' "
            "AND p.prorettype='trigger'::regtype AND p.pronargs=0 AND p.prokind='f' "
            "AND NOT p.prosecdef AND NOT p.proisstrict AND NOT p.proleakproof AND NOT p.proretset "
            "AND p.provolatile='v' AND p.proparallel='u' AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]"
        ),
        {"function": f'"{schema}".cms_doctors_prepared_immutable()', "body": _SEAL_BODY},
    )
    if function_oid is None:
        raise RuntimeError("cms_doctors_prepared_seal_function_changed")
    return int(function_oid)


async def assert_prepared_cms_doctors_seal(session, prepared, *, applied=False):
    """Check exact guarded heaps while the caller holds their relation locks."""
    if tuple(oid for oid, _filenode in prepared.sealed_filenodes) != tuple(
        oid for _target, _stage, oid in prepared.stage_oids
    ):
        raise RuntimeError("cms_doctors_prepared_physical_seal_changed")
    filenodes_by_oid = dict(prepared.sealed_filenodes)
    function_oid = await _assert_seal_function(session, prepared.schema)
    for target, stage, oid in prepared.stage_oids:
        filenode = await _assert_stage(session, prepared.schema, target if applied else stage, oid, logged=True)
        if filenode != filenodes_by_oid[oid]:
            raise RuntimeError("cms_doctors_prepared_physical_seal_changed")
        valid = await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=:oid AND tgname=:name "
                "AND tgfoid=:function AND tgtype=62 AND tgenabled='A' AND NOT tgisinternal "
                "AND NOT tgdeferrable AND NOT tginitdeferred AND tgconstrrelid=0 AND tgconstraint=0 "
                "AND tgnargs=0 AND octet_length(tgargs)=0 AND tgattr=''::int2vector AND tgqual IS NULL "
                "AND tgoldtable IS NULL AND tgnewtable IS NULL)"
            ),
            {"oid": oid, "name": _SEAL_TRIGGER, "function": function_oid},
        )
        if not valid:
            raise RuntimeError("cms_doctors_prepared_write_seal_changed")


async def _seal_finalized_stages(prepared):
    """Freeze only completed source heaps and retain their guard across every native rename."""
    async with _native().db.transaction() as session, archive._bounded_capture(session):
        stages = tuple(stage for _target, stage, _oid in prepared.stage_oids)
        await archive._lock_family(session, prepared.schema, stages, "ACCESS EXCLUSIVE", nowait=True)
        await _assert_seal_function(session, prepared.schema)
        filenodes = []
        for _target, stage, oid in prepared.stage_oids:
            filenode = await _assert_stage(session, prepared.schema, stage, oid, logged=True)
            filenodes.append((oid, filenode))
            relation = f'"{prepared.schema}"."{stage}"'
            await session.execute(
                text(
                    f"CREATE TRIGGER {_SEAL_TRIGGER} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {relation} "
                    f'FOR EACH STATEMENT EXECUTE FUNCTION "{prepared.schema}".cms_doctors_prepared_immutable()'
                )
            )
            await session.execute(text(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {_SEAL_TRIGGER}"))
        prepared.sealed_filenodes = tuple(filenodes)
        await assert_prepared_cms_doctors_seal(session, prepared)


async def _drop_owned_stages(schema, stage_oids):
    """Recheck OIDs under native locks and preserve any replacement bearing a reused name."""
    native = _native()
    async with native.db.transaction() as session:
        await session.execute(text("SET LOCAL lock_timeout='500ms'"))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        for _target, stage, expected_oid in reversed(stage_oids):
            actual_oid = await session.scalar(
                text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": f"{schema}.{stage}"}
            )
            if actual_oid != expected_oid:
                continue
            await archive._lock_family(session, schema, (stage,), "ACCESS EXCLUSIVE", nowait=True)
            await _assert_stage(session, schema, stage, expected_oid, logged=False)
            await session.execute(text(f'DROP TABLE "{schema}"."{stage}" RESTRICT'))


async def cleanup_prepared_cms_doctors(prepared):
    """Clean an unconsumed family only after the owner's serving transaction finishes."""
    if _native().db._transaction_binding() is not None:
        raise RuntimeError("cms_doctors_preparation_cleanup_transaction_active")
    if not prepared.committed:
        await _shielded_cleanup(prepared.schema, prepared.stage_oids)


async def _shielded_cleanup(schema, stage_oids):
    """Finish bounded owned cleanup even when the preparation task is cancelled."""
    task = asyncio.create_task(_drop_owned_stages(schema, stage_oids))
    is_cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            is_cancelled = True
    task.result()
    if is_cancelled:
        raise asyncio.CancelledError


async def _capture_incumbent(session, schema):
    """Reuse the native archive table fence before reading the local authority."""
    incumbent = await archive.capture_reference_family_incumbent(session, importer_id=_IMPORTER, schema_name=schema)
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id=_IMPORTER,
        schema_name=schema,
    )
    if authority.relation_oids is not None and authority.relation_oids != tuple(
        oid for _, oid in incumbent.relation_oids
    ):
        raise RuntimeError("cms_doctors_incumbent_authority_changed")
    return incumbent, authority


@asynccontextmanager
async def prepare_cms_doctors_generation(ctx):
    """Retain all three completed source stages until common publication or exact cleanup."""
    native = _native()
    context = ctx.get("context") or {}
    import_date = ctx.get("import_date")
    if native.db._transaction_binding() is not None:
        raise RuntimeError("cms_doctors_preparation_requires_own_scope")
    if (
        not context.get("run")
        or context.get("test_mode")
        or not isinstance(import_date, str)
        or not re.fullmatch(r"[A-Za-z0-9]{1,32}", import_date)
    ):
        raise RuntimeError("cms_doctors_preparation_requires_completed_import")
    schema = archive._schema_name(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    await native.ensure_database(False)
    async with native.db.transaction() as session:
        stage_oids = await _stage_inventory(session, schema, import_date)
    prepared = None
    try:
        if len(stage_oids) != len(_models()):
            raise RuntimeError("cms_doctors_preparation_family_incomplete")
        async with native.db.transaction() as session:
            incumbent, authority = await _capture_incumbent(session, schema)
        stage_cls = native.make_class(native.DoctorClinicianAddress, import_date)
        stage_rows = int(await native.db.scalar(f'SELECT count(*) FROM "{schema}"."{stage_cls.__tablename__}"'))
        metrics = await native._prepare_cms_doctors_sources(ctx, stage_cls, schema, stage_rows)
        prepared = PreparedCMSDoctorsGeneration(schema, import_date, stage_oids, incumbent, authority, metrics, context)
        if "artifact" in metrics:
            prepared.source_provenance_json = mint_doctors_source_provenance(metrics)
        await _finalize_stages(prepared)
        await _seal_finalized_stages(prepared)
        context.pop("education_stage_owned", None)
        context.pop("group_site_stage_owned", None)
        context["publication_state"] = "prepared"
        metrics.update(published=False, publication_state="prepared")
        yield prepared
    finally:
        if prepared is None or not prepared.committed:
            if native.db._transaction_binding() is not None:
                raise RuntimeError("cms_doctors_preparation_cleanup_transaction_active")
            await _shielded_cleanup(schema, stage_oids)
        context.pop("education_stage_owned", None)
        context.pop("group_site_stage_owned", None)


async def apply_prepared_cms_doctors_generation(prepared):
    """Apply exact prepared source tables and native authority in the coordinator's transaction."""
    native = _native()
    binding = native.db._transaction_binding()
    if binding is None:
        raise RuntimeError("cms_doctors_publication_requires_transaction")
    if prepared.committed:
        raise RuntimeError("cms_doctors_preparation_already_committed")
    session = binding.session
    stage_cls = native.make_class(native.DoctorClinicianAddress, prepared.import_date)
    await native._lock_cms_doctors_publication(session, stage_cls, prepared.schema, prepared.import_date)
    await archive._verify_incumbent(session, prepared.incumbent)
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id=_IMPORTER,
        schema_name=prepared.schema,
        lock=True,
    )
    if authority != prepared.incumbent_authority:
        raise RuntimeError("cms_doctors_incumbent_authority_changed")
    for model, (_target, stage, oid) in zip(_models(), prepared.stage_oids, strict=True):
        await _assert_stage(session, prepared.schema, stage, oid, logged=True)
        await _assert_stage_indexes(session, prepared.schema, native.make_class(model, prepared.import_date))
    await assert_prepared_cms_doctors_seal(session, prepared)
    prepared.native_receipt = await native._apply_locked_cms_doctors_stage(
        stage_cls,
        prepared.schema,
        prepared.import_date,
    )
    if prepared.native_receipt.relation_oids != tuple(oid for _, _, oid in prepared.stage_oids):
        raise RuntimeError("cms_doctors_preparation_result_changed")
    return prepared.native_receipt
