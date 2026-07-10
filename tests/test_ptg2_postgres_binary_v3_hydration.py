# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from api import ptg2_serving
from process.ptg_parts.ptg2_serving_binary_v3 import PTG2V3PriceAtomRecord


class FakeResult:
    def __init__(self, rows):
        self.rows = list(rows)

    def __iter__(self):
        return iter(self.rows)


class FakeSession:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((statement, dict(params)))
        return FakeResult(self.rows)


def _version_three_tables(**table_overrides_by_key):
    table_kwargs_by_key = {
        "arch_version": "postgres_binary_v3",
        "storage": "manifest_snapshot",
        "serving_binary_table": "mrf.ptg2_serving_binary_v3",
        "serving_table_layout": "lean_provider_key_v1",
        "code_count_table": "mrf.ptg2_code_count_v3",
        "provider_set_dictionary_table": "mrf.ptg2_provider_set_dictionary_v3",
        "atom_key_bits": 24,
        "price_key_block_span": 512,
        "atom_key_block_span": 512,
    }
    table_kwargs_by_key.update(table_overrides_by_key)
    return ptg2_serving.PTG2ServingTables(**table_kwargs_by_key)


@pytest.mark.asyncio
async def test_v3_price_hydration_reads_dense_atoms_without_price_atom_table(monkeypatch):
    attribute_keys = (0, 1, 2, 3, 4, 5, 6)
    first_atom = PTG2V3PriceAtomRecord("100.00", attribute_keys)
    second_atom = PTG2V3PriceAtomRecord("125.00", attribute_keys)

    async def memberships(_session, table_name, price_keys, *, atom_key_bits=None, block_span=None):
        assert table_name == "mrf.ptg2_serving_binary_v3"
        assert tuple(price_keys) == (10, 11)
        assert atom_key_bits == 24
        assert block_span == 512
        return {10: (4, 5), 11: (5,)}

    async def atoms(_session, table_name, atom_keys, *, atom_key_bits=None, block_span=None):
        assert table_name == "mrf.ptg2_serving_binary_v3"
        assert tuple(atom_keys) == (4, 5)
        assert atom_key_bits == 24
        assert block_span == 512
        return {4: first_atom, 5: second_atom}

    dictionary_rows = [
        {"attr_kind": "negotiated_type", "attr_key": 0, "text_value": "negotiated"},
        {"attr_kind": "expiration_date", "attr_key": 1, "text_value": "2027-01-01"},
        {"attr_kind": "service_code", "attr_key": 2, "text_array": ["11"]},
        {"attr_kind": "billing_class", "attr_key": 3, "text_value": "professional"},
        {"attr_kind": "setting", "attr_key": 4, "text_value": "outpatient"},
        {"attr_kind": "billing_code_modifier", "attr_key": 5, "text_array": ["25"]},
        {"attr_kind": "additional_information", "attr_key": 6, "text_value": "note"},
    ]
    monkeypatch.setattr(ptg2_serving, "lookup_price_atom_memberships_from_db", memberships)
    monkeypatch.setattr(ptg2_serving, "lookup_price_atoms_from_db", atoms)
    session = FakeSession(dictionary_rows)
    tables = _version_three_tables(price_atom_dictionary_table="mrf.ptg2_price_atom_dictionary_v3")

    prices_by_set = await ptg2_serving._ptg2_manifest_prices_for_price_sets(
        session,
        tables,
        ["00000000000000000000000000000011", "00000000000000000000000000000012"],
        price_key_by_set_id={
            "00000000000000000000000000000011": 10,
            "00000000000000000000000000000012": 11,
        },
    )

    assert [price["negotiated_rate"] for price in prices_by_set["00000000000000000000000000000011"]] == [
        "100.00",
        "125.00",
    ]
    assert prices_by_set["00000000000000000000000000000012"][0]["service_code"] == ["11"]
    assert "price_atom_global_id_128" not in str(session.calls[0][0])


@pytest.mark.asyncio
async def test_v3_price_hydration_raises_when_membership_key_is_missing(monkeypatch):
    async def incomplete_memberships(
        _session,
        _table_name,
        _price_keys,
        *,
        atom_key_bits=None,
        block_span=None,
    ):
        return {10: (4,)}

    monkeypatch.setattr(
        ptg2_serving,
        "lookup_price_atom_memberships_from_db",
        incomplete_memberships,
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="price-membership artifact"):
        await ptg2_serving._version_three_prices_by_key(
            object(),
            _version_three_tables(),
            [10, 11],
        )


@pytest.mark.asyncio
async def test_v3_price_hydration_raises_when_atom_key_is_missing(monkeypatch):
    async def memberships(
        _session,
        _table_name,
        _price_keys,
        *,
        atom_key_bits=None,
        block_span=None,
    ):
        return {10: (4, 5)}

    async def incomplete_atoms(
        _session,
        _table_name,
        _atom_keys,
        *,
        atom_key_bits=None,
        block_span=None,
    ):
        return {4: PTG2V3PriceAtomRecord("100.00", (None,) * 7)}

    monkeypatch.setattr(ptg2_serving, "lookup_price_atom_memberships_from_db", memberships)
    monkeypatch.setattr(ptg2_serving, "lookup_price_atoms_from_db", incomplete_atoms)

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="price-atom artifact"):
        await ptg2_serving._version_three_prices_by_key(
            object(),
            _version_three_tables(),
            [10],
        )


@pytest.mark.asyncio
async def test_v3_price_hydration_requires_forward_price_key():
    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="forward row"):
        await ptg2_serving._ptg2_manifest_prices_for_price_sets(
            object(),
            _version_three_tables(),
            ["00000000000000000000000000000011"],
            price_key_by_set_id={},
        )


@pytest.mark.asyncio
async def test_v3_missing_binary_table_never_resolves_local_sidecars(monkeypatch):
    tables = _version_three_tables(serving_binary_table=None)
    monkeypatch.setattr(
        ptg2_serving,
        "_resolve_ptg2_manifest_sidecar_path",
        lambda *_args: (_ for _ in ()).throw(AssertionError("v3 must not touch the filesystem")),
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="missing serving_binary_table"):
        await ptg2_serving._ptg2_manifest_lookup_serving_by_code_sidecar(FakeSession(), tables, 7)
