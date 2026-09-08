# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep complete release authority while reading identical snapshots once."""

from copy import deepcopy
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_projection_build as build
from api import plan_pricing_projection_contract as contract
from .test_plan_pricing_projection_coverage import _binding


@pytest.mark.asyncio
async def test_alias_reads_preserve_manifest_and_first_ordinal(monkeypatch):
    first = _binding(ordinal=4, source_file_id="first-logical-file")
    alias = _binding(ordinal=7, source_file_id="second-logical-file")
    alias.pop("market_type")
    alias["plan_market_type"] = " GROUP "
    alias["plan_id"] = " plan "
    bindings = [first, alias]
    original = deepcopy(bindings)
    reader = AsyncMock(side_effect=lambda _session, binding, **_kwargs: SimpleNamespace(
        binding=binding, raw_code_row_count=build.MAX_PROJECTION_CODE_ROWS,
    ))
    materialize = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(build, "binding_projection", reader)
    monkeypatch.setattr(build, "materialize_factorized_projection", materialize)

    await build._materialize_all_codes(object(), "candidate", bindings)

    reader.assert_awaited_once()
    assert materialize.call_args.args[2][0].binding["ordinal"] == 4
    assert len(materialize.call_args.args[2]) == 1
    assert bindings == original
    assert contract.normalized_bindings(bindings) == original


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["snapshot_id", "source_key", "plan_id", "market_type"])
async def test_distinct_read_scope_is_not_collapsed(monkeypatch, field):
    reader = AsyncMock(side_effect=lambda _session, binding, **_kwargs: SimpleNamespace(
        binding=binding, raw_code_row_count=1,
    ))
    materialize = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(build, "binding_projection", reader)
    monkeypatch.setattr(build, "materialize_factorized_projection", materialize)

    await build._materialize_all_codes(object(), "candidate", [
        _binding(), _binding(ordinal=1, **{field: "different"}),
    ])

    assert reader.await_count == 2
    assert len(materialize.call_args.args[2]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinal", [0, False, float("inf")])
async def test_alias_cannot_hide_invalid_raw_ordinal(monkeypatch, ordinal):
    reader = AsyncMock()
    monkeypatch.setattr(build, "binding_projection", reader)
    with pytest.raises(ValueError, match="ordinal"):
        await build._materialize_all_codes(object(), "candidate", [
            _binding(), _binding(ordinal=ordinal),
        ])
    reader.assert_not_awaited()


def test_new_build_identity_preserves_storage_contract():
    digest, provider = "b" * 64, "c" * 64
    old_identity = hashlib.sha256(
        f"plan_pricing_factorized_v4\0{digest}\0{provider}".encode("ascii")
    ).hexdigest()
    assert contract.PROJECTION_CONTRACT == "plan_pricing_factorized_v4"
    assert contract.projection_id(digest, provider) != old_identity


@pytest.mark.asyncio
async def test_role_scoped_ordinals_preserve_mixed_manifest(monkeypatch):
    bindings = [_binding(role="allowed_amounts"), _binding()]
    reader = AsyncMock(return_value=SimpleNamespace(raw_code_row_count=1))
    monkeypatch.setattr(build, "binding_projection", reader)
    monkeypatch.setattr(build, "materialize_factorized_projection", AsyncMock())

    assert contract.normalized_bindings(bindings) == bindings
    await build._materialize_all_codes(object(), "candidate", bindings)

    reader.assert_awaited_once()
    assert reader.call_args.args[1] == bindings[1]
