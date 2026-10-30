# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual issued ordinary lifecycle with explicitly synthetic native observations."""

import importlib
import json
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

from process import entity_address_ordinary_dependencies as observed
from process import entity_address_ordinary_preparation as ordinary
from process import provider_directory_cms_preparation as preparation
from tests.provider_directory_ordinary_scratch_npi_support import scratch_case
from tests.test_entity_address_prepared_doctors_postgres import _example_bindings
from tests.test_tiger_held_inputs_guards import _inventory

fhir = importlib.import_module("process.provider_directory_fhir")


def _native_inputs():
    selected_by_name = {relation["relation_name"]: relation for relation in _inventory()["relations"]}
    bindings_by_name = _example_bindings()
    for name, relation in selected_by_name.items():
        bindings_by_name["tiger." + name] = dict(
            schema_name=relation["schema_name"],
            table_name=name,
            relation_oid=relation["relation_oid"],
            relfilenode=relation["relfilenode"],
        )
    proof_by_family = {family: None for family in ("npi", "mrf-address", "cms-doctors", "geo")}
    proof_by_family["tiger"] = dict(kind="immutable", identity=list(selected_by_name.values()))
    native_by_field = dict(
        selected_relations=selected_by_name,
        current_dependency_bindings=bindings_by_name,
        semantic_dependency_proofs=proof_by_family,
    )
    return native_by_field


@contextmanager
def dependency_case(monkeypatch):
    case = scratch_case()
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(return_value=True))
    binding = SimpleNamespace(session=session)
    case.directory.db._transaction_binding = lambda: binding
    native_by_field = _native_inputs()
    selected_by_name = native_by_field["selected_relations"]
    bindings_by_name = native_by_field["current_dependency_bindings"]
    proof_by_family = native_by_field["semantic_dependency_proofs"]
    payload_by_field = json.loads(case.capture._payload_json)
    payload_by_field["affected_scope"]["address_task"] = {"refresh_mode": "provider-directory-partial"}
    payload_by_field["predecessor"] = dict(native=native_by_field)
    capture = replace(case.capture, _payload_json=json.dumps(payload_by_field))
    case.admission.captured_inputs = capture
    monkeypatch.setattr(fhir, "db", case.directory.db)
    monkeypatch.setattr(fhir, "_schema", lambda: "mrf")
    actor_by_field = dict(
        owner=capture.scope_payload["owner_oid"],
        database_oid=case.admission.lease.database_oid,
        backend=17,
        xid="29",
    )
    actor = AsyncMock(return_value=(session, object(), actor_by_field))
    selection = AsyncMock(return_value=deepcopy((selected_by_name, bindings_by_name)))
    proofs = AsyncMock(return_value=deepcopy(proof_by_family))

    async def protected(_session, relation, *, expected_owner):
        assert _session is session
        name = relation.split('"')[-2]
        relation_by_field = selected_by_name[name]
        assert expected_owner == relation_by_field["owner_oid"]
        return deepcopy(relation_by_field)

    protection = AsyncMock(side_effect=protected)
    monkeypatch.setattr(observed.receipts, "ordinary", actor)
    monkeypatch.setattr(observed.bindings, "capture_selected_publication_bindings", selection)
    monkeypatch.setattr(observed, "capture_semantic_dependencies", proofs)
    monkeypatch.setattr(observed.held, "_protected_relation", protection)
    token = preparation._ACTIVE.set(case.admission)
    try:
        issued = ordinary.issue(case.admission, case.admission.plan.native_address_input_hash)
        yield SimpleNamespace(
            database=case.directory.db,
            session=session,
            actor=actor,
            actor_by_field=actor_by_field,
            selection=selection,
            proofs=proofs,
            protection=protection,
            issued=issued,
            admission=case.admission,
            capture=capture,
            bindings=bindings_by_name,
            selected=selected_by_name,
        )
    finally:
        preparation._ACTIVE.reset(token)
