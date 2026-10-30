# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent correspondence retains coherent aliases and rejects key collisions."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from process.network_initial_source_office_bindings import copy_source_office_binding_rows
from process.registry_initial_source_composition import RegistryInitialSourceError
from process.registry_source_office_custody import _binding_parity
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["separate", "aliases", "cross_provider"])
async def test_independent_parity_checks_office_keys_across_pages(serving_schema, mode):
    connection, control_schema, _ = serving_schema
    namespace = "source_office_parity_" + uuid4().hex
    expected_table = "source_office_expected_" + uuid4().hex
    await connection.execute(f'CREATE SCHEMA "{namespace}"')
    try:
        async with connection.transaction(isolation="repeatable_read"):
            for ordinal in range(2):
                office_key = "a" * 64 if mode != "separate" or ordinal == 0 else "b" * 64
                provider_id = "1000000491" if mode == "cross_provider" and ordinal == 1 else "1234567893"
                bindings = [("npi", provider_id, uuid4(), office_key, "npi", provider_id)]
                await copy_source_office_binding_rows(connection, namespace, "provider_location_binding", bindings)
                await copy_source_office_binding_rows(connection, "pg_temp", expected_table, bindings)
            await connection.execute(
                f'CREATE TABLE "{namespace}".network_membership_source AS '
                f'SELECT provider_system,provider_id,location_id FROM "{namespace}".provider_location_binding'
            )
            arguments = (
                connection,
                SimpleNamespace(schema_name=namespace),
                'pg_temp."' + expected_table + '"',
                {"approved_custom_revision": 0},
                control_schema,
            )
            if mode == "cross_provider":
                with pytest.raises(RegistryInitialSourceError, match="bindings_differ"):
                    await _binding_parity(*arguments)
            else:
                await _binding_parity(*arguments)
            await connection.execute(f'DROP TABLE pg_temp."{expected_table}"')
    finally:
        await connection.execute(f'DROP SCHEMA "{namespace}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
