# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bind address reads and geometry to explicitly prepared native dependencies."""

from __future__ import annotations

import importlib
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text

from api import ptg2_geo_projection as projection

if TYPE_CHECKING:
    from process.cms_doctors_preparation import PreparedCMSDoctorsGeneration
    from process.provider_directory_cms_archive import PreparedArchiveDelta

_ADDRESS = "doctor_clinician_address"
_ADDRESS_ARCHIVE = "address_archive_v2"


def _doctors_preparation():
    """Avoid native importer and archive cycles while the address module initializes."""
    return importlib.import_module("process.cms_doctors_preparation")


def _archive():
    """Resolve existing reference-family locking only when a dependency is checked."""
    return importlib.import_module("process.reference_family_archive")


@dataclass(frozen=True)
class PreparedAddressDependencies:
    """Keep desired physical dependencies separate from the incumbent source fence."""

    schema: str
    doctors: PreparedCMSDoctorsGeneration | None
    stage_oids: tuple[tuple[str, str, int], ...]
    dependency_bindings: dict
    archive: PreparedArchiveDelta | None = None


def _validate_doctors(database, schema, doctors, overrides_by_target):
    """Require the complete native prepared family, never a table-name-only override."""
    doctors_preparation = _doctors_preparation()
    if (
        not isinstance(doctors, doctors_preparation.PreparedCMSDoctorsGeneration)
        or doctors_preparation._native().db is not database
        or doctors.schema != schema
        or doctors.committed
        or doctors.context.get("publication_state") != "prepared"
        or doctors.metrics.get("publication_state") != "prepared"
        or doctors.metrics.get("published") is not False
        or not isinstance(doctors.import_date, str)
        or not re.fullmatch(r"[A-Za-z0-9]{1,32}", doctors.import_date)
    ):
        raise RuntimeError("entity-address Doctors input is not prepared")
    expected_relations = tuple(
        (model.__tablename__, doctors_preparation._native().make_class(model, doctors.import_date).__tablename__)
        for model in doctors_preparation._models()
    )
    if (
        not isinstance(doctors.stage_oids, tuple)
        or tuple((target, stage) for target, stage, _oid in doctors.stage_oids) != expected_relations
        or any(type(oid) is not int or oid <= 0 for _target, _stage, oid in doctors.stage_oids)
        or len({oid for _target, _stage, oid in doctors.stage_oids}) != len(expected_relations)
        or overrides_by_target.get(_ADDRESS) != doctors.relation_overrides[_ADDRESS]
    ):
        raise RuntimeError("entity-address Doctors family differs from its override")


def _validate_bindings(schema, doctors, dependency_bindings):
    """Allow only the proven Doctors address to differ from canonical geo names."""
    bindings = projection.validate_projection_dependency_bindings(schema, dependency_bindings)
    for canonical, binding in bindings.items():
        namespace, table = canonical.split(".")
        expected_oid = None
        if doctors is not None and table == _ADDRESS:
            _target, table, expected_oid = doctors.stage_oids[0]
        if (binding["schema_name"], binding["table_name"]) != (namespace, table) or (
            expected_oid is not None and binding["relation_oid"] != expected_oid
        ):
            raise RuntimeError("entity-address geo dependency is not a proven input")
    return bindings


def _validate_archive(schema, archive, overrides_by_target):
    """Accept only the prepared archive view bound to its consumed native-input admission."""
    archive_preparation = importlib.import_module("process.provider_directory_cms_archive")
    if (
        not isinstance(archive, archive_preparation.PreparedArchiveDelta)
        or not isinstance(archive.admission, archive_preparation.preparation.NonprofileAdmission)
        or not archive.admission._started
        or archive.committed
        or archive.schema != schema
        or not isinstance(archive.native_input_hash, str)
        or not re.fullmatch(r"[0-9a-f]{64}", archive.native_input_hash)
        or archive.native_input_hash != archive.admission.plan.native_address_input_hash
    ):
        raise RuntimeError("entity-address archive input is not prepared")
    if overrides_by_target.get(_ADDRESS_ARCHIVE) != archive.effective_relation:
        raise RuntimeError("entity-address archive differs from its override")
    archive.admission._assert_lease()


async def capture_dependencies(
    database, schema, relation_overrides, *, doctors=None, dependency_bindings=None, archive=None
):
    """Validate the trusted native bundle and copy its complete desired geo binding map."""
    overrides_by_target = dict(relation_overrides)
    if doctors is None and _ADDRESS in overrides_by_target:
        raise RuntimeError("entity-address Doctors override requires a prepared family")
    if archive is None and _ADDRESS_ARCHIVE in overrides_by_target:
        raise RuntimeError("entity-address archive override requires a prepared delta")
    if doctors is None and dependency_bindings is None and archive is None:
        return None
    if doctors is not None:
        _validate_doctors(database, schema, doctors, overrides_by_target)
    if archive is not None:
        _validate_archive(schema, archive, overrides_by_target)
    bindings = _validate_bindings(schema, doctors, dependency_bindings)
    captured = PreparedAddressDependencies(
        schema, doctors, doctors.stage_oids if doctors is not None else (), bindings, archive
    )
    await assert_prepared_dependencies(database, captured)
    return captured


async def _assert_bindings(session, schema, bindings):
    """Recheck exact heap OIDs and filenodes while holding every dependency name."""
    await session.execute(text(projection.projection_dependency_lock_sql(schema, dependency_bindings=bindings)))
    if not await session.scalar(
        text("SELECT " + projection.projection_dependency_bindings_match_sql(schema, bindings))
    ):
        raise RuntimeError("entity-address geo dependency binding changed")


async def lock_prepared_relations(database, dependencies):
    """Check input names on the same locked backend that executes the source query."""
    if dependencies.archive is not None:
        await dependencies.archive.lock_read_backend(database)
    if dependencies.doctors is None:
        return
    schema = dependencies.schema
    relations = ", ".join(f'ONLY "{schema}"."{stage}"' for _target, stage, _oid in dependencies.stage_oids)
    await database.status(f"LOCK TABLE {relations} IN ACCESS SHARE MODE NOWAIT")
    for _target, stage, expected_oid in dependencies.stage_oids:
        actual_oid = await database.scalar(
            "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass(:relation) AND relkind='r' AND relpersistence='p'",
            relation=f'"{schema}"."{stage}"',
        )
        if actual_oid != expected_oid:
            raise RuntimeError("entity-address prepared Doctors identity changed")
    binding = dependencies.dependency_bindings[f"{schema}.{_ADDRESS}"]
    actual_filenode = await database.scalar(
        "SELECT pg_relation_filenode(oid)::bigint FROM pg_class WHERE oid=:oid", oid=binding["relation_oid"]
    )
    if actual_filenode != binding["relfilenode"]:
        raise RuntimeError("entity-address geo dependency binding changed")


async def assert_prepared_dependencies(database, dependencies):
    """Recheck the incumbent authority and the complete prepared source before dependent reads."""
    if dependencies is None:
        return
    doctors_preparation, archive = _doctors_preparation(), _archive()
    async with database.transaction() as session, archive._bounded_capture(session):
        if dependencies.archive is not None:
            _validate_archive(dependencies.schema, dependencies.archive, dependencies.archive.relation_overrides)
            await dependencies.archive.lock_read_backend(database)
        doctors = dependencies.doctors
        if doctors is not None:
            _validate_doctors(database, dependencies.schema, doctors, doctors.relation_overrides)
            if doctors.stage_oids != dependencies.stage_oids:
                raise RuntimeError("entity-address prepared Doctors identity changed")
            incumbent, authority = await doctors_preparation._capture_incumbent(session, dependencies.schema)
            if incumbent != doctors.incumbent or authority != doctors.incumbent_authority:
                raise RuntimeError("entity-address Doctors incumbent changed")
            await archive._lock_family(
                session,
                dependencies.schema,
                tuple(stage for _target, stage, _oid in dependencies.stage_oids),
                "ACCESS SHARE",
                nowait=True,
            )
            for model, (_target, stage, oid) in zip(
                doctors_preparation._models(), dependencies.stage_oids, strict=True
            ):
                await doctors_preparation._assert_stage(session, dependencies.schema, stage, oid, logged=True)
                await doctors_preparation._assert_stage_indexes(
                    session, dependencies.schema, doctors_preparation._native().make_class(model, doctors.import_date)
                )
            await doctors_preparation.assert_prepared_cms_doctors_seal(session, doctors)
        await _assert_bindings(session, dependencies.schema, dependencies.dependency_bindings)


async def assert_applied_dependencies(database, dependencies):
    """Require the prepared source's actual native apply before address's canonical activation."""
    if dependencies is None:
        return
    doctors_preparation, archive = _doctors_preparation(), _archive()
    generation = importlib.import_module("process.reference_family_result_generation")
    binding = database._transaction_binding()
    if binding is None:
        raise RuntimeError("entity-address dependency cutover requires a transaction")
    session, schema, doctors = binding.session, dependencies.schema, dependencies.doctors
    bindings = projection.validate_projection_dependency_bindings(schema, dependencies.dependency_bindings)
    async with archive._bounded_capture(session):
        if dependencies.archive is not None:
            await dependencies.archive.assert_applied_backend(database)
        if doctors is not None:
            if doctors.committed or doctors.stage_oids != dependencies.stage_oids or doctors.native_receipt is None:
                raise RuntimeError("entity-address prepared Doctors publication is missing")
            target_relations = tuple(target_relation for target_relation, _stage, _oid in dependencies.stage_oids)
            await archive._lock_family(session, schema, target_relations, "ACCESS SHARE", nowait=True)
            authority = await generation.read_reference_family_result_generation_authority(
                session, importer_id="cms-doctors", schema_name=schema
            )
            if authority != doctors.native_receipt or authority.relation_oids != tuple(
                oid for _target, _stage, oid in dependencies.stage_oids
            ):
                raise RuntimeError("entity-address prepared Doctors publication changed")
            for target_relation, _stage, oid in dependencies.stage_oids:
                await doctors_preparation._assert_stage(session, schema, target_relation, oid, logged=True)
            await doctors_preparation.assert_prepared_cms_doctors_seal(session, doctors, applied=True)
            bindings[f"{schema}.{_ADDRESS}"]["table_name"] = _ADDRESS
        await _assert_bindings(session, schema, bindings)
