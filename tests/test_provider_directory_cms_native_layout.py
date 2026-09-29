# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native dispatch and exact observed index-state rejection without projection changes."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_layout as layout
from process.provider_directory_cms_preparation import NonprofileAdmissionCheck, OwnedRelation
from tests.test_provider_directory_cms_nonprofile_capacity import _producer


@pytest.mark.asyncio
async def test_only_declared_native_stages_use_native_validator(monkeypatch):
    """The same aggregate check keeps FHIR relations on the unchanged Profile validator."""
    producer = _producer()
    native = OwnedRelation("synthetic", "entity_address_unified_cms" + "a" * 20 + "_raw", 11, 100, "u")
    artifact = OwnedRelation("synthetic", "artifact_stage", 12, 100, "u")
    native_capture = AsyncMock(return_value=layout.NativeRelationLayout(11, (42,), "a" * 64))
    monkeypatch.setattr(layout, "capture_native_layout", native_capture)
    profile_capture = AsyncMock(return_value=SimpleNamespace(relation_oid=12, effective_tablespace_oids=(42,)))
    producer.fhir._provider_directory_profile_relation_storage_fingerprint = profile_capture
    producer._current_wal_bytes = AsyncMock(return_value=0)
    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, (native, artifact))
    await producer._assert_physical(request, {"data_tablespace_oid": 42})
    native_capture.assert_awaited_once_with(producer.fhir, native, producer.plan.native_address_targets)
    profile_capture.assert_awaited_once_with(12, expected_persistence="u")


@pytest.mark.parametrize("field", ["indisvalid", "indisready", "indislive", "indimmediate"])
def test_nonready_native_index_is_rejected(field):
    """A declaration cannot authorize an unfinished or invalid physical index."""
    name = "entity_address_unified_cms" + "a" * 20
    index_by_field = {"indisvalid": True, "indisready": True, "indislive": True, "indimmediate": True, field: False}
    relation = OwnedRelation("synthetic", name, 11, 100, "u")
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        layout._assert_indexes([index_by_field], [], relation, layout.ENTITY_ADDRESS_RESULT_MODELS[0], None)
