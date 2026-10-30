# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The used inference inputs retain their original published NUCC authority."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_dependency_bindings as dependencies
from process import provider_directory_cms_native_inputs as native_inputs
from process import reference_family_result_generation as reference
from tests.test_geo_assurance_dependency_bindings import _binding_result


def _observed_session(*, kind="p", protected=True, absent=None):
    relations_by_name = {
        name: {
            "relation_oid": 100 + ordinal,
            "relfilenode": 200 + ordinal,
            "kind": kind if name == "nucc_taxonomy" else "r",
            "protected_owner": protected,
        }
        for ordinal, name in enumerate(("npi", "npi_address", "npi_taxonomy", "nucc_taxonomy"))
    }
    if kind == "p":
        relations_by_name["nucc_taxonomy"]["relfilenode"] = None
    if absent is not None:
        relations_by_name[absent] = None

    async def execute(_sql, parameters):
        name = parameters["name"].split(".")[-1].strip('"')
        return _binding_result(deepcopy(relations_by_name[name]))

    return SimpleNamespace(execute=AsyncMock(side_effect=execute)), relations_by_name


@pytest.mark.parametrize("absent", ["npi", "nucc_taxonomy"])
async def test_unavailable_inference_inputs_do_not_require_unused_nucc(monkeypatch, absent):
    session, _relations = _observed_session(absent=absent)
    reader = AsyncMock()
    monkeypatch.setattr(native_inputs, "capture_protected_nucc_input", reader)
    observed = await dependencies.capture_inference_inputs(session, "mrf", {})
    assert observed["prerequisites"][absent] is None and observed["nucc"] is None
    reader.assert_not_awaited()


@pytest.mark.parametrize("changed", [False, True])
async def test_native_parent_reuses_the_existing_exact_child_generation_proof(monkeypatch, changed):
    session, relations = _observed_session()
    proof_by_field = {"physical": {"parent_oid": 103 + changed}, "generation": {"local_generation": 4}}
    reader = AsyncMock(return_value=proof_by_field)
    monkeypatch.setattr(native_inputs, "capture_protected_nucc_input", reader)
    if changed:
        with pytest.raises(RuntimeError, match="NUCC input changed"):
            await dependencies.capture_inference_inputs(session, "mrf", {})
    else:
        observed = await dependencies.capture_inference_inputs(session, "mrf", {})
        assert observed["nucc"] == proof_by_field
        assert observed["prerequisites"]["nucc_taxonomy"]["relation_oid"] == relations["nucc_taxonomy"]["relation_oid"]
    reader.assert_awaited_once_with(session, schema="mrf")


@pytest.mark.parametrize("protected", [False, True])
async def test_plain_nucc_keeps_native_or_existing_revision_custody(monkeypatch, protected):
    session, _relations = _observed_session(kind="r", protected=protected)
    authority = SimpleNamespace(
        serving_generation=object(), relation_oids=(103,), as_dict=lambda: {"local_generation": 4}
    )
    monkeypatch.setattr(
        reference, "read_reference_family_result_generation_authority", AsyncMock(return_value=authority)
    )
    native_reader, legacy_reader = AsyncMock(), AsyncMock()
    monkeypatch.setattr(reference, "require_nucc_native_predecessor_storage", native_reader)
    monkeypatch.setattr(reference, "capture_reference_family_serving_generation", legacy_reader)
    observed = await dependencies.capture_inference_inputs(session, "fixture", {})
    assert observed["nucc"] == {"generation": {"local_generation": 4}}
    assert native_reader.await_count == int(protected)
    assert legacy_reader.await_count == int(not protected)


@pytest.mark.parametrize("changed", ["generation", "oid", "kind"])
async def test_unpublished_or_changed_nucc_is_not_an_inference_authority(monkeypatch, changed):
    session, _relations = _observed_session(kind="v" if changed == "kind" else "r")
    authority = SimpleNamespace(
        serving_generation=None if changed == "generation" else object(),
        relation_oids=(999,) if changed == "oid" else (103,),
    )
    monkeypatch.setattr(
        reference, "read_reference_family_result_generation_authority", AsyncMock(return_value=authority)
    )
    with pytest.raises(RuntimeError, match="NUCC (generation|input) changed"):
        await dependencies.capture_inference_inputs(session, "fixture", {})


async def test_selected_npi_physical_drift_is_refused(monkeypatch):
    session, relations = _observed_session()
    selected_by_name = {
        "npi": {"schema_name": "native_npi", "relation_oid": 999, "relfilenode": relations["npi"]["relfilenode"]}
    }
    with pytest.raises(RuntimeError, match="selected input changed"):
        await dependencies.capture_inference_inputs(session, "mrf", selected_by_name)
