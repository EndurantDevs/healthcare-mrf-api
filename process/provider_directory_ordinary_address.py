# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run the existing partial Address builder on its original ordinary admission."""

import importlib
from contextlib import asynccontextmanager

from process import entity_address_candidate_preparation as candidates
from process import entity_address_ordinary_preparation as ordinary
from process.provider_directory_cms_address import _new_context, _preparation_inputs
from process.provider_directory_ordinary_address_admission import require_capture


@asynccontextmanager
async def prepare_ordinary_address(fence, relation_overrides, overlay, admission, *, run_id):
    """Keep genuine candidates alive until their caller commits or cleans them up."""
    fhir = importlib.import_module("process.provider_directory_fhir")
    native = importlib.import_module("process.entity_address_unified")
    if fhir.db is not native.db or fhir.db._transaction_binding() is not None:
        raise RuntimeError("provider_directory_ordinary_address_requires_unbound_database")
    capture = require_capture(fhir, admission.plan, admission.captured_inputs)
    if capture._fence is not fence or not capture.address_task:
        raise RuntimeError("provider_directory_ordinary_address_scope_changed")
    await admission.assert_ready(fhir, fhir._schema())
    observed = capture.predecessor["native"]
    inputs = _preparation_inputs(
        fence,
        relation_overrides,
        overlay,
        {"alias_generation": observed["observed"]["alias_state"]["generation"]},
        admission.plan.desired_profile_as_of,
    )
    prepared = await candidates.prepare_provider_directory_entity_address(
        await _new_context(run_id, fhir._schema()),
        capture.address_task,
        preparation_input=inputs,
        admission=admission,
        native_input_hash=admission.plan.native_address_input_hash,
        dependency_bindings=observed["current_dependency_bindings"],
    )
    if prepared is None:
        raise RuntimeError("provider_directory_ordinary_address_preparation_incomplete")
    try:
        ordinary.require_prepared(prepared, admission)
        await admission.assert_ready(fhir, fhir._schema())
        yield prepared
    finally:
        await candidates.cleanup_prepared_entity_address_generation(prepared)
