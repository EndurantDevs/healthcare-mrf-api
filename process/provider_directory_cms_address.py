# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind a full native address preparation to the admitted CMS serving inputs."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text

from api.ptg2_geo_projection import validate_projection_dependency_bindings
from process import entity_address_prepared_doctors as source_dependencies
from process.cms_doctors_source_provenance import (
    doctors_source_provenance_digest,
    read_doctors_source_provenance,
)
from process.entity_address_candidate_preparation import (
    _RELATION_OVERRIDES,
    ProviderDirectoryAddressDatasetPin,
    ProviderDirectoryAddressPreparationInput,
    cleanup_prepared_entity_address_generation,
    prepare_provider_directory_entity_address,
    validate_preparation_input,
)
from process.provider_directory_cms_native_inputs import (
    capture_native_address_input_fence,
    require_supported_native_address_inputs,
)
from process.provider_directory_cms_preparation import desired_fence_hash
from process.provider_directory_cms_serving_receipt import capture_native_dependencies
from process.reference_family_result_generation import RELATION_NAMES_BY_IMPORTER

_DOMAIN = b"provider-directory-cms-native-address-input-v1\0"
_RECIPE = {"refresh_mode": "full", "serving_only_refresh": False, "test_mode": False, "publish": True}
_MODULES = (
    "process.provider_directory_fhir",
    "process.provider_directory_address_overlay_components",
    "process.provider_directory_cms_overlay_projection",
    "process.provider_directory_cms_native_projection",
    "process.provider_directory_cms_native_inputs",
    "process.provider_directory_cms_archive",
    "process.provider_directory_cms_desired_fence",
    "process.entity_address_unified",
    "process.entity_address_candidate_preparation",
    "process.entity_address_preparation_admission",
    "process.entity_address_prepared_doctors",
    "process.cms_doctors_preparation",
    "process.cms_doctors_source_provenance",
    "process.cms_doctors",
    "process.cms_doctors_artifact",
    "process.cms_doctors_education",
    "process.cms_doctors_groups",
    "process.cms_doctors_rows",
    "process.cms_doctors_organizations",
    "process.cms_doctors_sites",
    "process.entity_address_cutover_contract",
    "process.entity_address_result_generation",
    "process.ext.address_alias_sql",
    "process.ext.address_canon",
    "process.ext.address_format",
    "api.ptg2_geo_projection",
    "api.ptg2_geo_policy",
)
_SEMANTIC_OPTIONS = frozenset(
    {
        "HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_LIMIT",
        "HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES",
        "HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_OTHER_IDENTIFIER",
        "HLTHPRT_IMPORT_NODE_ID",
    }
)


def _native():
    return importlib.import_module("process.entity_address_unified")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _mapping_digest() -> str:
    """Bind mapper code and serving model definitions without local paths in the identity."""
    native = _native()
    modules = set(_MODULES) | {
        model.__module__
        for model in (
            native.EntityAddressUnified,
            *native.SUPPORT_TABLE_MODELS,
            *source_dependencies._doctors_preparation()._models(),
        )
    }
    hashes_by_module = {}
    for name in sorted(modules):
        source_path = importlib.import_module(name).__file__
        if source_path is None or not source_path.endswith(".py"):
            raise RuntimeError("cms_address_mapper_source_unavailable")
        hashes_by_module[name] = hashlib.sha256(Path(source_path).read_bytes()).hexdigest()
    return hashlib.sha256(_DOMAIN + _json(hashes_by_module).encode("ascii")).hexdigest()


def _dataset_pins(fence) -> tuple[ProviderDirectoryAddressDatasetPin, ...]:
    """Carry the complete desired vector, preserving acquisition ownership."""
    return tuple(
        ProviderDirectoryAddressDatasetPin(
            dataset.source_id, dataset.endpoint_id, dataset.dataset_id, dataset.dataset_hash, dataset.evidence_run_id
        )
        for dataset in sorted(fence.datasets, key=lambda item: item.source_id)
    )


def _runtime_recipe() -> dict[str, str]:
    """Reject limited/reused builds and bind every native environment option."""
    native = _native()
    options_by_name = {
        name: value
        for name, value in os.environ.items()
        if name.startswith("HLTHPRT_ENTITY_ADDRESS_UNIFIED_") or name in _SEMANTIC_OPTIONS
    }
    if options_by_name.get("HLTHPRT_ENTITY_ADDRESS_UNIFIED_LIMIT_PER_SOURCE", "").strip() or any(
        native._is_env_enabled(name, False)
        for name in (
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_RAW_STAGE",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEEP_RAW_STAGE",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_COMPACT_SOURCE_RECORD_IDS_BY_REWRITE",
        )
    ):
        raise RuntimeError("cms_address_full_owned_build_required")
    if not native._is_env_enabled("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE", native.DEFAULT_UNLOGGED_STAGE):
        raise RuntimeError("cms_address_unlogged_preparation_required")
    return options_by_name


def _desired_doctors(fhir, doctors):
    """Bind the prepared source evidence and all sealed heaps without granting native acceptance."""
    if doctors is None:
        return None
    source_dependencies._validate_doctors(fhir.db, fhir._schema(), doctors, doctors.relation_overrides)
    if tuple(oid for oid, _filenode in doctors.sealed_filenodes) != tuple(
        oid for _target, _stage, oid in doctors.stage_oids
    ) or any(type(filenode) is not int or not 0 < filenode < 2**32 for _oid, filenode in doctors.sealed_filenodes):
        raise RuntimeError("cms_address_doctors_physical_seal_required")
    return {
        "source_provenance": read_doctors_source_provenance(doctors.source_provenance_json),
        "source_provenance_sha256": doctors_source_provenance_digest(doctors.source_provenance_json),
        "stage_oids": doctors.stage_oids,
        "sealed_filenodes": doctors.sealed_filenodes,
    }


def _desired_geo_bindings(schema, native_input_fence, doctors, dependency_bindings):
    """Keep the incumbent physical fence intact and substitute only the exact sealed source."""
    expected_by_name = source_dependencies._validate_bindings(schema, None, native_input_fence["geo_bindings"])
    if doctors is not None:
        _target, stage, oid = doctors.stage_oids[0]
        expected_by_name[f"{schema}.doctor_clinician_address"] = {
            "schema_name": schema,
            "table_name": stage,
            "relation_oid": oid,
            "relfilenode": dict(doctors.sealed_filenodes)[oid],
        }
        if dependency_bindings is None:
            raise RuntimeError("cms_address_desired_geo_bindings_required")
    supplied_by_name = validate_projection_dependency_bindings(
        schema, expected_by_name if dependency_bindings is None else dependency_bindings
    )
    if supplied_by_name != expected_by_name:
        raise RuntimeError("cms_address_desired_geo_bindings_changed")
    return supplied_by_name


def _build_input(
    fhir,
    execution,
    fence,
    dependencies,
    native_input_fence,
    worker_count: int,
    temp_limit: int,
) -> str:
    """Bind source stages and semantics while excluding newly generated address output identities."""
    if (
        type(worker_count) is not int
        or worker_count <= 0
        or type(temp_limit) is not int
        or temp_limit <= 0
        or temp_limit % 1024
    ):
        raise ValueError("cms_address_execution_bounds_invalid")
    attestation = execution.attestation
    require_supported_native_address_inputs(native_input_fence)
    if attestation.operation != "publish" or not any(pin.source_id == "cms-npd" for pin in _dataset_pins(fence)):
        raise RuntimeError("cms_address_publish_selection_required")
    desired_geo_bindings = _desired_geo_bindings(fhir._schema(), native_input_fence, None, None)
    return _json(
        {
            "contract_id": "provider-directory-cms-native-address-input.v1",
            "database_schema": _native()._validate_schema_name(fhir._schema()),
            "desired_fence_hash": desired_fence_hash(fence),
            "dataset_pins": [asdict(pin) for pin in _dataset_pins(fence)],
            "source_context_digest": attestation.source_context_digest,
            "profile_as_of": attestation.desired_profile_as_of,
            "native_dependencies": dependencies,
            "native_input_fence": native_input_fence,
            "desired_doctors": None,
            "desired_geo_bindings": desired_geo_bindings,
            "native_targets": sorted(_native().result_generation.RELATION_NAMES),
            "mapper_digest": _mapping_digest(),
            "recipe": _RECIPE,
            "runtime_options": _runtime_recipe(),
            "worker_count": worker_count,
            "temp_file_limit_bytes_per_backend": temp_limit,
            "max_parallel_workers_per_gather": 0,
            "max_parallel_maintenance_workers": 0,
        }
    )


def _with_doctors_input(fhir, input_json, doctors, dependency_bindings):
    """Extend a serving input with a sealed source; this confers no source-build admission."""
    if doctors is None:
        raise RuntimeError("cms_address_prepared_doctors_required")
    input_by_field = json.loads(input_json)
    input_by_field["desired_doctors"] = _desired_doctors(fhir, doctors)
    input_by_field["desired_geo_bindings"] = _desired_geo_bindings(
        fhir._schema(), input_by_field["native_input_fence"], doctors, dependency_bindings
    )
    return _json(input_by_field)


@dataclass(frozen=True)
class CMSAddressPreparation:
    """Internal factory consumed by the full serving preparation context."""

    fhir: Any
    execution: Any
    run_id: str
    input_json: str
    doctors: Any = None

    @property
    def input_hash(self) -> str:
        """Identify the complete native recipe and its admitted semantic inputs."""
        return hashlib.sha256(_DOMAIN + self.input_json.encode("ascii")).hexdigest()

    @property
    def native_targets(self) -> tuple[str, ...]:
        """Return the closed native family required by the capacity reservation."""
        return tuple(json.loads(self.input_json)["native_targets"])

    @property
    def source_relation_overrides(self) -> dict[str, str]:
        """Expose all three signed source inputs for full/Profile preparation."""
        desired = json.loads(self.input_json)["desired_doctors"]
        return {} if desired is None else {target: stage for target, stage, _oid in desired["stage_oids"]}

    def with_prepared_doctors(self, doctors, dependency_bindings) -> CMSAddressPreparation:
        """Bind independently prepared source inputs before the serving capacity plan is signed."""
        return CMSAddressPreparation(
            self.fhir,
            self.execution,
            self.run_id,
            _with_doctors_input(self.fhir, self.input_json, doctors, dependency_bindings),
            doctors,
        )

    def _current_input(self, fence, expected):
        """Recompute semantic inputs and the desired physical source without changing its incumbent fence."""
        input_json = _build_input(
            self.fhir,
            self.execution,
            fence,
            expected["native_dependencies"],
            expected["native_input_fence"],
            expected["worker_count"],
            expected["temp_file_limit_bytes_per_backend"],
        )
        if self.doctors is not None:
            input_json = _with_doctors_input(self.fhir, input_json, self.doctors, expected["desired_geo_bindings"])
        return input_json

    def _assert_inputs(self, fence, admission):
        expected = json.loads(self.input_json)
        if (
            admission.plan.native_address_input_hash != self.input_hash
            or admission.plan.native_address_targets != self.native_targets
            or admission.plan.worker_count != expected["worker_count"]
            or admission.plan.temp_file_limit_bytes_per_backend != expected["temp_file_limit_bytes_per_backend"]
            or self._current_input(fence, expected) != self.input_json
        ):
            raise RuntimeError("cms_address_admitted_inputs_changed")
        return expected

    async def _assert_dependencies(self, expected):
        """Compare every native input in one repeatable, read-only snapshot."""
        async with self.fhir.db.session() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            actual = await capture_native_dependencies(session, self.fhir._schema())
            native_input_fence = await capture_native_address_input_fence(session, self.fhir._schema())
        if actual != expected["native_dependencies"] or native_input_fence != expected["native_input_fence"]:
            raise RuntimeError("cms_address_native_dependencies_changed")

    @asynccontextmanager
    async def __call__(self, fence, relation_overrides, overlay, admission, *, archive=None):
        """Prepare the complete desired address family before Profile materialization."""
        if self.fhir.db is not _native().db or self.fhir.db._transaction_binding() is not None:
            raise RuntimeError("cms_address_preparation_requires_shared_unbound_database")
        expected = self._assert_inputs(fence, admission)
        await self._assert_dependencies(expected)
        overrides_by_name = dict(relation_overrides)
        for name in RELATION_NAMES_BY_IMPORTER["cms-doctors"]:
            stage = self.source_relation_overrides.get(name)
            if name in overrides_by_name and overrides_by_name[name] != stage:
                raise RuntimeError("cms_address_doctors_override_unproved")
            if stage is not None:
                overrides_by_name[name] = stage
        inputs = _preparation_inputs(
            fence, overrides_by_name, overlay, expected["native_dependencies"], expected["profile_as_of"]
        )
        await source_dependencies.capture_dependencies(
            self.fhir.db,
            self.fhir._schema(),
            inputs.relation_overrides,
            doctors=self.doctors,
            dependency_bindings=expected["desired_geo_bindings"],
            archive=archive,
        )
        context = await _new_context(self.run_id, self.fhir._schema())
        prepared = await prepare_provider_directory_entity_address(
            context,
            dict(_RECIPE),
            preparation_input=inputs,
            admission=admission,
            native_input_hash=self.input_hash,
            doctors=self.doctors,
            dependency_bindings=expected["desired_geo_bindings"],
            archive=archive,
        )
        if prepared is None:
            raise RuntimeError("cms_address_preparation_incomplete")
        try:
            self._assert_inputs(fence, admission)
            await self._assert_dependencies(expected)
            if tuple(sorted(table for table, _stage, _oid in prepared.stage_oids)) != self.native_targets:
                raise RuntimeError("cms_address_native_targets_changed")
            prepared.context["cms_native_address_input_json"] = self.input_json
            yield prepared
        finally:
            await cleanup_prepared_entity_address_generation(prepared)


def _preparation_inputs(fence, overrides, overlay, dependencies, semantic_as_of):
    if overlay is None or type(overlay.oid) is not int or overlay.oid <= 0:
        raise RuntimeError("cms_address_staged_overlay_required")
    inputs = ProviderDirectoryAddressPreparationInput(
        _dataset_pins(fence),
        overlay.relation,
        overlay.oid,
        dependencies["alias_generation"],
        tuple((name, overrides[name]) for name in sorted(_RELATION_OVERRIDES) if name in overrides),
        semantic_as_of=semantic_as_of,
    )
    validate_preparation_input(inputs)
    return inputs


async def _new_context(run_id: str, schema: str) -> dict[str, Any]:
    """Use a unique suffix and refuse every occupied native stage name before building."""
    native = _native()
    context_by_field = {"control_run_id": run_id}
    await native.startup(context_by_field)
    context_by_field["import_date"] = "cms" + uuid.uuid4().hex[:20]
    stage_table_names = [
        native.make_class(model, context_by_field["import_date"]).__tablename__
        for model in (native.EntityAddressUnified, *native.SUPPORT_TABLE_MODELS)
    ]
    stage_table_names.extend(
        builder(stage_table_names[0])
        for builder in (
            native._raw_stage_table_name,
            native._evidence_stage_table_name,
            native._compact_stage_table_name,
        )
    )
    for name in stage_table_names:
        if (
            await native.db.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=f"{schema}.{name}")
            is not None
        ):
            raise RuntimeError("cms_address_stage_name_occupied")
    return context_by_field


def cms_address_preparation(
    fhir,
    execution,
    fence,
    native_dependencies,
    *,
    native_input_fence: dict,
    run_id: str,
    worker_count: int,
    temp_file_limit_bytes_per_backend: int,
) -> CMSAddressPreparation:
    """Build the deterministic input used by both capacity geometry and native preparation."""
    if not isinstance(run_id, str) or not re.fullmatch(r"run_[0-9a-f]{32}", run_id):
        raise ValueError("cms_address_control_run_invalid")
    return CMSAddressPreparation(
        fhir,
        execution,
        run_id,
        _build_input(
            fhir,
            execution,
            fence,
            native_dependencies,
            native_input_fence,
            worker_count,
            temp_file_limit_bytes_per_backend,
        ),
    )


def admitted_native_input_fence(prepared, address, native_dependencies) -> dict:
    """Recover the native fence only from this prepared family's signed input."""
    admission = prepared.nonprofile_admission
    input_json = address.context.get("cms_native_address_input_json")
    if (
        admission is None
        or not isinstance(input_json, str)
        or (
            hashlib.sha256(_DOMAIN + input_json.encode("ascii")).hexdigest() != admission.plan.native_address_input_hash
        )
    ):
        raise RuntimeError("cms_address_admitted_input_proof_required")
    archive = getattr(prepared, "archive_delta", None)
    address_archive = getattr(getattr(address, "native_dependencies", None), "archive", None)
    if (archive is not None or address_archive is not None) and (
        archive is not address_archive
        or archive.admission is not admission
        or archive.native_input_hash != admission.plan.native_address_input_hash
    ):
        raise RuntimeError("cms_address_prepared_archive_changed")
    input_by_field = json.loads(input_json)
    if input_by_field["native_dependencies"] != native_dependencies:
        raise RuntimeError("cms_address_admitted_native_dependencies_changed")
    native_input_fence = input_by_field["native_input_fence"]
    require_supported_native_address_inputs(native_input_fence)
    return native_input_fence
