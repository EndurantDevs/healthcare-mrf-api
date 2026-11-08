# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact inverse alternatives preserve the original callable and reader fences."""

from copy import deepcopy

import pytest

from db import migration_cms_noncurrent_seal as cms
from db import migration_fhir_source_rollback as rollback
from process import network_fhir_source_custody as reader
from process import provider_directory_source_callable_custody as custody
from tests.test_cms_noncurrent_seal_guard import _reader_lifecycle, _reader_lifecycle_hook
from tests.test_provider_directory_source_callable_custody import _entry, _roles


@pytest.mark.parametrize("schema", ["sample", "Mixed_Catalog23"])
@pytest.mark.parametrize("version", [2, 3])
def test_exact_inverse_is_recognized_by_both_consumers(schema, version):
    bodies = rollback.rollback_guard_bodies(schema)
    assert bodies[:2] == cms.lifecycle_guard_bodies(schema)
    body = bodies[version]
    entry = _entry(cms.GUARD + "()")
    entry["definition"]["prosrc"] = body
    custody._require_definition(entry, custody._CONTRACTS[cms.GUARD + "()"], schema, 201, custody._TYPE_OIDS, schema)
    for function in (_reader_lifecycle(schema, 2), _reader_lifecycle_hook(schema, 2)):
        function["prosrc"] = body
        before = deepcopy(function)
        reader._require_function(function, schema, 100)
        assert function == before
    assert body.count("RAISE EXCEPTION") == bodies[version - 2].count("RAISE EXCEPTION")
    assert cms._cms_transition_sql() in body if version == 3 else cms._cms_transition_sql() not in body
    assert "EXCEPTION WHEN data_exception" in body
    assert "transaction_isolation')='serializable'" in body


@pytest.mark.parametrize("version", [2, 3])
@pytest.mark.parametrize("fault", ["body", "namespace", "security", "owner"])
def test_inverse_recognition_does_not_accept_near_matches(version, fault):
    function = _reader_lifecycle("sample", 2)
    function["prosrc"] = rollback.rollback_guard_bodies("sample")[version]
    if fault == "body":
        function["prosrc"] += "\nRETURN NULL;"
    elif fault == "namespace":
        function["prosrc"] = function["prosrc"].replace("TG_TABLE_SCHEMA='sample'", "TG_TABLE_SCHEMA='other'")
    elif fault == "security":
        function["prosecdef"] = False
    else:
        function["proowner"] = 999
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_function(function, "sample", 100)


@pytest.mark.parametrize("version", [2, 3])
@pytest.mark.parametrize("fault", ["body", "namespace", "owner"])
def test_callable_custody_refuses_inverse_near_matches(version, fault):
    key = cms.GUARD + "()"
    entry = _entry(key, owner=101)
    entry["definition"]["prosrc"] = rollback.rollback_guard_bodies("sample")[version]
    entry["acl"].append([103, 101, "EXECUTE", False])
    custody._require_definition(entry, custody._CONTRACTS[key], "sample", 201, custody._TYPE_OIDS, "sample")
    custody._require_acl(entry, _roles(), legacy=False)
    if fault == "owner":
        entry["definition"]["proowner"] = "999"
        with pytest.raises(ValueError, match="owner_changed"):
            custody._require_acl(entry, _roles(), legacy=False)
    else:
        body = entry["definition"]["prosrc"]
        entry["definition"]["prosrc"] = (
            body + "\nRETURN NULL;"
            if fault == "body"
            else body.replace("TG_TABLE_SCHEMA='sample'", "TG_TABLE_SCHEMA='other'")
        )
        with pytest.raises(ValueError, match="definition_changed"):
            custody._require_definition(entry, custody._CONTRACTS[key], "sample", 201, custody._TYPE_OIDS, "sample")
