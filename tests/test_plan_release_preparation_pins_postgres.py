# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native future-headerless custody through ambiguous network admission."""

import pytest

from process.plan_release_preparation_pin_contract import PreparationPinInputs
from process.plan_release_preparation_pins import retain_preparation_pins
from process.ptg_parts.ptg2_source_snapshot_gc import _CURRENT_SNAPSHOT_IDS_SQL
from tests.plan_release_preparation_pin_postgres_support import prepare_pin_case
from tests.plan_release_storage_postgres_support import NEW_REVISION, storage_case


@pytest.mark.asyncio
async def test_future_pin_retention_precedes_network_and_replays_ambiguous_admission():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        query = _CURRENT_SNAPSHOT_IDS_SQL.replace("__SCHEMA__", case.schema)
        gc_reader = await case.connect()
        assert "new-snapshot" not in {snapshot_row["snapshot_id"] for snapshot_row in await gc_reader.fetch(query)}
        receipt = await retain_preparation_pins(
            prepared.publisher,
            case.schema,
            prepared.scope,
            PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
            authority=prepared.authority,
            **case.profile,
        )
        assert "new-snapshot" in {snapshot_row["snapshot_id"] for snapshot_row in await gc_reader.fetch(query)}
        assert not await prepared.publisher.fetchval(
            f"SELECT EXISTS(SELECT FROM {case.relation('plan_release_serving_revision')} WHERE serving_revision_id=$1)",
            NEW_REVISION,
        )
        admin = await case.connect()
        await admin.execute(
            f"UPDATE {prepared.tables['admission']} SET status='dispatching',dispatches='[{{}}]'::jsonb"
        )
        replay = await retain_preparation_pins(
            prepared.publisher,
            case.schema,
            prepared.scope,
            PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
            authority=prepared.authority,
            **case.profile,
        )
        assert replay == receipt


@pytest.mark.asyncio
async def test_missing_native_authority_does_not_attach_or_accept_late_pins():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        with pytest.raises(ValueError, match="native_authority_required"):
            await retain_preparation_pins(
                prepared.publisher,
                case.schema,
                prepared.scope,
                PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
                **case.profile,
            )
        admin = await case.connect()
        await admin.execute(
            f"UPDATE {prepared.tables['admission']} SET status='dispatching',dispatches='[{{}}]'::jsonb"
        )
        with pytest.raises(ValueError, match="late_pin_admission_refused"):
            await retain_preparation_pins(
                prepared.publisher,
                case.schema,
                prepared.scope,
                PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
                authority=prepared.authority,
                **case.profile,
            )
        assert not await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", prepared.pin_heap.oid)
