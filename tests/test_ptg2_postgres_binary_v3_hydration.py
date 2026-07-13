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
        "shared_snapshot_key": 41,
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "serving_table_layout": "lean_provider_key_v1",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "atom_key_bits": 24,
        "price_key_block_span": 512,
        "atom_key_block_span": 512,
        "price_dictionary_item_count": 1024,
        "price_dictionary_block_bytes": 65536,
    }
    table_kwargs_by_key.update(table_overrides_by_key)
    return ptg2_serving.PTG2ServingTables(**table_kwargs_by_key)


@pytest.mark.asyncio
async def test_v3_price_hydration_reads_dense_atoms_without_price_atom_table(monkeypatch):
    attribute_keys = (0, 1, 2, 3, 4, 5, 6)
    first_atom = PTG2V3PriceAtomRecord("100.00", attribute_keys)
    second_atom = PTG2V3PriceAtomRecord("125.00", attribute_keys)

    async def memberships(_session, snapshot_key, price_keys, *, atom_key_bits=None, block_span=None, schema_name=None):
        assert snapshot_key == 41
        assert schema_name == "mrf"
        assert tuple(price_keys) == (10, 11)
        assert atom_key_bits == 24
        assert block_span == 512
        return {10: (4, 5), 11: (5,)}

    async def atoms(_session, snapshot_key, atom_keys, *, atom_key_bits=None, block_span=None, schema_name=None):
        assert snapshot_key == 41
        assert schema_name == "mrf"
        assert tuple(atom_keys) == (4, 5)
        assert atom_key_bits == 24
        assert block_span == 512
        return {4: first_atom, 5: second_atom}

    dictionary_rows = [
        {"attribute_kind": "negotiated_type", "attribute_key": 0, "value": "negotiated"},
        {"attribute_kind": "expiration_date", "attribute_key": 1, "value": "2027-01-01"},
        {"attribute_kind": "service_code", "attribute_key": 2, "value": '["11"]'},
        {"attribute_kind": "billing_class", "attribute_key": 3, "value": "professional"},
        {"attribute_kind": "setting", "attribute_key": 4, "value": "outpatient"},
        {"attribute_kind": "billing_code_modifier", "attribute_key": 5, "value": '["25"]'},
        {"attribute_kind": "additional_information", "attribute_key": 6, "value": "note"},
    ]
    monkeypatch.setattr(ptg2_serving, "lookup_shared_price_atom_memberships_from_db", memberships)
    monkeypatch.setattr(ptg2_serving, "lookup_shared_price_atoms_from_db", atoms)
    session = FakeSession(dictionary_rows)
    tables = _version_three_tables()

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
        schema_name=None,
    ):
        return {10: (4,)}

    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_price_atom_memberships_from_db",
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
        schema_name=None,
    ):
        return {10: (4, 5)}

    async def incomplete_atoms(
        _session,
        _table_name,
        _atom_keys,
        *,
        atom_key_bits=None,
        block_span=None,
        schema_name=None,
    ):
        return {4: PTG2V3PriceAtomRecord("100.00", (None,) * 7)}

    monkeypatch.setattr(ptg2_serving, "lookup_shared_price_atom_memberships_from_db", memberships)
    monkeypatch.setattr(ptg2_serving, "lookup_shared_price_atoms_from_db", incomplete_atoms)

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
async def test_v3_missing_shared_contract_fails_without_filesystem_resolution():
    tables = _version_three_tables(shared_snapshot_key=None)

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="shared-block contract"):
        await ptg2_serving._lookup_shared_forward_rows(FakeSession(), tables, 7)
