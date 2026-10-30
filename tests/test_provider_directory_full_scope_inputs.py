# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A nested Profile scratch mapping cannot replace projected canonical inputs."""

import pytest

from tests.test_provider_directory_cms_preparation import _admission, _fake_fhir, _prepare
from tests.test_provider_directory_cms_preparation import (
    emulate_only_in_memory_sql_backend as emulate_only_in_memory_sql_backend,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("abort", [False, True])
async def test_nested_profile_inputs_do_not_replace_full_scope_projection(monkeypatch, abort):
    fake, execution, fence, _ = _fake_fhir([])
    original = fake._materialize_artifact_scope_payload
    inherited_by_root = {"provider_directory_source": "profile_source_scratch"}
    token = fake.scope_relations.set(inherited_by_root)

    async def materialize(*arguments):
        assert fake.scope_relations.get() == {}
        await original(*arguments)

    monkeypatch.setattr(fake, "_materialize_artifact_scope_payload", materialize)
    try:
        try:
            async with _prepare(
                fake,
                execution,
                fence,
                run_id="run-a",
                control_run_id=None,
                metrics={},
                nonprofile_admission=_admission(),
            ):
                if abort:
                    raise RuntimeError("stop preparation")
        except RuntimeError as error:
            assert abort and str(error) == "stop preparation"
        assert fake.materialized and not fake.db.relations
        assert fake.scope_relations.get() == inherited_by_root
    finally:
        fake.scope_relations.reset(token)
