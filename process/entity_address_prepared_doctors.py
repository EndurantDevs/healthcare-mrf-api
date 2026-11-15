# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bind address reads and geometry to explicitly prepared native dependencies."""

from __future__ import annotations

import importlib
import json
import re
from collections.abc import Mapping
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
    """Keep declared prepared dependencies separate from the incumbent source fence."""

    schema: str
    doctors: PreparedCMSDoctorsGeneration | None
    stage_relations: tuple[tuple[str, str], ...]
    dependency_bindings: dict
    archive: PreparedArchiveDelta | None = None


def _validate_doctors(database, schema, doctors, overrides_by_target):
    """Require the complete native prepared family, never a table-name-only override."""
    doctors_preparation = _doctors_preparation()
    if (
        not isinstance(doctors, doctors_preparation.PreparedCMSDoctorsGeneration)
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
        doctors_preparation.prepared_stage_relations(doctors) != expected_relations
        or overrides_by_target.get(_ADDRESS) != doctors.relation_overrides[_ADDRESS]
    ):
        raise RuntimeError("entity-address Doctors family differs from its override")


def _named_bindings(schema, dependency_bindings):
    """Project explicit saved names without interpreting legacy physical tokens."""
    if not isinstance(dependency_bindings, Mapping):
        raise ValueError("geo projection dependency name family is invalid")
    projected_by_name = {}
    for canonical, binding in dependency_bindings.items():
        if not isinstance(binding, Mapping) or set(binding) not in (
            {"schema_name", "table_name"},
            {"schema_name", "table_name", "relation_oid", "relfilenode"},
        ):
            raise ValueError("geo projection dependency name fields are invalid")
        projected_by_name[canonical] = {field: binding[field] for field in ("schema_name", "table_name")}
    return projection.validate_projection_dependency_qnames(schema, projected_by_name)


def _validate_bindings(schema, doctors, dependency_bindings, *, tiger_captured=None):
    """Allow only declared Doctors stages and independently selected captured TIGER names."""
    bindings = _named_bindings(schema, dependency_bindings)
    from process.provider_directory_cms_tiger_inputs import captured_tiger_bindings

    tiger_by_name = {} if tiger_captured is None else captured_tiger_bindings(tiger_captured)
    for canonical, binding in bindings.items():
        if canonical in tiger_by_name:
            if binding != tiger_by_name[canonical]:
                raise RuntimeError("entity-address geo dependency is not a proven input")
            continue
        namespace, table = canonical.split(".")
        if doctors is not None and table == _ADDRESS:
            _target, table = _doctors_preparation().prepared_stage_relations(doctors)[0]
        if (binding["schema_name"], binding["table_name"]) != (namespace, table):
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
    tiger_captured = await _captured_tiger_dependencies(database, dependency_bindings)
    bindings = _validate_bindings(
        schema,
        doctors,
        dependency_bindings,
        **({"tiger_captured": tiger_captured} if tiger_captured is not None else {}),
    )
    captured = PreparedAddressDependencies(
        schema,
        doctors,
        _doctors_preparation().prepared_stage_relations(doctors) if doctors is not None else (),
        bindings,
        archive,
    )
    await assert_prepared_dependencies(database, captured)
    return captured


async def _assert_bindings(session, schema, bindings):
    """Recheck declared native shape while holding every qualified dependency name."""
    bindings = _named_bindings(schema, bindings)
    await session.execute(text(projection.projection_named_dependency_lock_sql(schema, bindings)))
    if not await session.scalar(text("SELECT " + projection.projection_named_dependency_match_sql(schema, bindings))):
        raise RuntimeError("entity-address geo dependency binding changed")


async def lock_prepared_relations(database, dependencies):
    """Use actual native access and read locks on the backend executing the source query."""
    if dependencies.archive is not None:
        await dependencies.archive.lock_read_backend(database)
    if dependencies.doctors is None:
        return
    schema = dependencies.schema
    relations = ", ".join(
        f"ONLY {_archive()._quoted(schema)}.{_archive()._quoted(stage)}"
        for _target, stage in dependencies.stage_relations
    )
    await database.status(f"LOCK TABLE {relations} IN ACCESS SHARE MODE NOWAIT")
    await database.status(projection.projection_named_dependency_lock_sql(schema, dependencies.dependency_bindings))
    if not await database.scalar(
        "SELECT " + projection.projection_named_dependency_match_sql(schema, dependencies.dependency_bindings)
    ):
        raise RuntimeError("entity-address geo dependency binding changed")


def _incumbent_names(incumbent):
    """Compare declared scope and presence, never saved database identifiers."""
    return (
        incumbent.importer_id,
        incumbent.schema_name,
        tuple((name, address is not None) for name, address in incumbent.relation_oids),
    )


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
            if doctors_preparation.prepared_stage_relations(doctors) != dependencies.stage_relations:
                raise RuntimeError("entity-address prepared Doctors identity changed")
            incumbent, authority = await doctors_preparation._capture_incumbent(session, dependencies.schema)
            if (
                _incumbent_names(incumbent) != _incumbent_names(doctors.incumbent)
                or authority != doctors.incumbent_authority
            ):
                raise RuntimeError("entity-address Doctors incumbent changed")
            await archive._lock_family(
                session,
                dependencies.schema,
                tuple(stage for _target, stage in dependencies.stage_relations),
                "ACCESS SHARE",
                nowait=True,
            )
            for model, (_target, stage) in zip(
                doctors_preparation._models(), dependencies.stage_relations, strict=True
            ):
                await doctors_preparation._assert_stage(session, dependencies.schema, stage, logged=True)
                await doctors_preparation._assert_stage_indexes(
                    session, dependencies.schema, doctors_preparation._native().make_class(model, doctors.import_date)
                )
            await doctors_preparation.assert_prepared_cms_doctors_seal(session, doctors)
        await _assert_bindings(session, dependencies.schema, dependencies.dependency_bindings)


async def assert_applied_dependencies(database, dependencies):
    """Require the prepared source's actual native apply before address's canonical activation."""
    if dependencies is None:
        return None
    doctors_preparation, archive = _doctors_preparation(), _archive()
    generation = importlib.import_module("process.reference_family_result_generation")
    binding = database._transaction_binding()
    if binding is None:
        raise RuntimeError("entity-address dependency cutover requires a transaction")
    session, schema, doctors = binding.session, dependencies.schema, dependencies.doctors
    bindings = _named_bindings(schema, dependencies.dependency_bindings)
    async with archive._bounded_capture(session):
        if dependencies.archive is not None:
            await dependencies.archive.assert_applied_backend(database)
        if doctors is not None:
            if (
                doctors.committed
                or doctors_preparation.prepared_stage_relations(doctors) != dependencies.stage_relations
                or doctors.native_receipt is None
            ):
                raise RuntimeError("entity-address prepared Doctors publication is missing")
            target_relations = tuple(target_relation for target_relation, _stage in dependencies.stage_relations)
            await archive._lock_family(session, schema, target_relations, "ACCESS SHARE", nowait=True)
            authority = await generation.read_reference_family_result_generation_authority(
                session, importer_id="cms-doctors", schema_name=schema
            )
            if authority != doctors.native_receipt or authority.relations != tuple(
                (schema, target_relation) for target_relation, _stage in dependencies.stage_relations
            ):
                raise RuntimeError("entity-address prepared Doctors publication changed")
            for target_relation, _stage in dependencies.stage_relations:
                await doctors_preparation._assert_stage(session, schema, target_relation, logged=True)
            await doctors_preparation.assert_prepared_cms_doctors_seal(session, doctors, applied=True)
            bindings[f"{schema}.{_ADDRESS}"]["table_name"] = _ADDRESS
        await _assert_bindings(session, schema, bindings)
    return bindings


async def record_applied_geo_assurance_candidate(database, schema, stage_table, projected_rows, bindings):
    """The Publisher records the prepared projection under the applied input names before activation."""
    native = importlib.import_module("process.entity_address_unified")
    schema, stage_table = native._validate_schema_name(schema), native._validate_schema_name(stage_table)
    stage = native._sql_literal(f"{schema}.{stage_table}")
    bindings = _named_bindings(schema, bindings)
    signature = projection.projection_relation_signature_sql(schema, dependency_bindings=bindings)
    stored_bindings = native._sql_literal(json.dumps(bindings, sort_keys=True, separators=(",", ":")))
    # The existing singleton row is updated in place; activation later requires this exact candidate.
    candidate_sql = f"""UPDATE {schema}.{projection.GEO_ASSURANCE_STATE_TABLE}
       SET candidate_geo_assurance_version={projection.GEO_ASSURANCE_VERSION},
           candidate_table_oid=NULL,
           candidate_relation_signature={signature},
           candidate_dependency_bindings={stored_bindings}::jsonb,
           candidate_projected_rows={int(projected_rows)}::bigint
     WHERE singleton IS TRUE AND to_regclass({stage}) IS NOT NULL
    RETURNING candidate_geo_assurance_version"""
    if await native.preparation_admission.read_scalar(database, candidate_sql) is None:
        raise RuntimeError("geo assurance candidate could not be recorded")


async def _captured_tiger_dependencies(database, dependency_bindings):
    """Independently observe current issued Source authority before noncanonical reads."""
    if dependency_bindings is None:
        return None
    requires_capture = any(
        dependency_bindings.get("tiger." + name, {}).get("schema_name") != "tiger" for name in ("zip_state", "zcta5")
    )
    if not requires_capture:
        return None
    binding = database._transaction_binding()
    if binding is None:
        raise RuntimeError("entity-address captured Tiger requires a Source transaction")
    from process.provider_directory_cms_tiger_inputs import capture_selected_captured_tiger_input

    observed = await capture_selected_captured_tiger_input(binding.session)
    if observed is None:
        raise RuntimeError("entity-address captured Tiger requires native Source authority")
    return observed


def _named_prepared_geo_bindings(db_schema, dependency_bindings):
    """Reuse only the existing issued Source preparation; names do not establish freshness."""
    native = importlib.import_module("process.entity_address_unified")
    captured = native.candidate_preparation.geo_dependency_options().get("dependency_bindings")
    if captured is None:
        if isinstance(dependency_bindings, Mapping) and all(
            isinstance(binding, Mapping) and set(binding) == {"schema_name", "table_name"}
            for binding in dependency_bindings.values()
        ):
            return projection.validate_projection_dependency_qnames(db_schema, dependency_bindings)
        return None
    scope = native.preparation_admission._ADMISSION.get()
    issued = native.candidate_preparation.source_preparation.require_building(scope)
    if (
        native.candidate_preparation.current() is None
        or issued is None
        or scope.db_schema != db_schema
        or issued.factory.policy.schema != db_schema
    ):
        raise RuntimeError("entity_address_named_geo_requires_source_preparation")
    supplied = native.candidate_preparation.prepared_doctors._named_bindings(db_schema, dependency_bindings)
    if supplied != captured:
        raise RuntimeError("entity-address desired dependency bindings differ")
    return supplied


async def _lock_geo_projection_inputs(db_schema, dependency_bindings, named_bindings):
    """Check actual access and native layout under the selected input contract's locks."""
    native = importlib.import_module("process.entity_address_unified")
    if named_bindings is not None:
        lock_sql = projection.projection_named_dependency_lock_sql(db_schema, named_bindings)
        match_sql = projection.projection_named_dependency_match_sql(db_schema, named_bindings)
    else:
        options = {} if dependency_bindings is None else {"dependency_bindings": dependency_bindings}
        lock_sql = projection.projection_dependency_lock_sql(db_schema, **options)
        match_sql = (
            None
            if dependency_bindings is None
            else projection.projection_dependency_bindings_match_sql(db_schema, dependency_bindings)
        )
    await native.db.status(lock_sql)
    if match_sql is not None and not await native.preparation_admission.read_scalar(native.db, f"SELECT {match_sql}"):
        raise RuntimeError("geo assurance dependency binding changed")


def _geo_assurance_candidate_receipt_sql(db_schema, stage_table, projected_rows, binding_options, *, record_candidate):
    """Leave authority recording to the Publisher when preparing a protected handoff."""
    native = importlib.import_module("process.entity_address_unified")
    if record_candidate:
        return native._record_geo_assurance_candidate_sql(
            db_schema,
            stage_table,
            projected_rows,
            require_canonical_publication=native.candidate_preparation.has_prepared_doctors(),
            **binding_options,
        )
    relation = f"{native._validate_schema_name(db_schema)}.{native._validate_schema_name(stage_table)}"
    return f"SELECT CASE WHEN to_regclass({native._sql_literal(relation)}) IS NOT NULL THEN {projection.GEO_ASSURANCE_VERSION} END;"
