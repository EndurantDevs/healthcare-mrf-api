# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh catalogs cannot admit hooks, deferred identity or ambiguous physical members."""

import pytest

from process import provider_directory_cms_native_layout as layout
from tests.test_cms_native_catalog_batch import _catalog_backend


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["custom_hook", "deferred_key", "unvalidated_key"])
async def test_native_catalog_cannot_fingerprint_executable_or_unfinished_integrity(fault):
    backend, relations, _relation_maps, catalog_by_name = _catalog_backend()
    await layout.observe_native_catalog_batch(backend, relations)
    before_relations = tuple(relations)
    if fault == "custom_hook":
        catalog_by_name["triggers"].append(dict(relation_oid=11, trigger_name="foreign_write_hook"))
    else:
        catalog_by_name["constraints"].append(
            dict(
                relation_oid=11,
                constraint_type="p",
                constraint_definition="PRIMARY KEY (identity)",
                condeferrable=fault == "deferred_key",
                condeferred=False,
                convalidated=fault != "unvalidated_key",
            )
        )
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.observe_native_catalog_batch(backend, relations)
    assert tuple(relations) == before_relations


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", [None, dict(relation_oid=11), dict(relation_oid=11, attnum=[])])
async def test_native_catalog_member_requires_an_exact_hashable_attribute_identity(malformed):
    backend, relations, _relation_maps, catalog_by_name = _catalog_backend()
    catalog_by_name["attributes"].append(malformed)
    backend.db.first.side_effect = None
    backend.db.first.return_value = catalog_by_name
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.observe_native_catalog_batch(backend, relations)
