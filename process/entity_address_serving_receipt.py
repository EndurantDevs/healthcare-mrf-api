# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep ordinary address publications in an existing common serving receipt chain."""

from sqlalchemy import text

from process import provider_directory_cms_serving_receipt as receipts
from process.entity_address_cutover_contract import (
    EntityAddressCutoverCallbacks,
    preserve_transaction_sql_settings,
)


def _publication_session(database):
    """Require the session already owning the native publication transaction."""
    binding = database._transaction_binding()
    if binding is None or not binding.session.in_transaction():
        raise RuntimeError("entity_address_common_receipt_requires_transaction")
    return binding.session


def ordinary_address_receipt_callbacks(database, schema, quote_literal):
    """Fence the predecessor before swapping and append its successor after native authority."""
    quoted_schema = receipts._schema(schema)
    table = f"{quoted_schema}.{receipts._TABLE}"
    publication_by_field = {}

    async def before_cutover():
        """Protect the immutable tip and its live native dependencies before any table swap."""
        publication_by_field.clear()
        if not await database.scalar("SELECT to_regclass(:relation) IS NOT NULL", relation=table):
            return
        owner_session = _publication_session(database)
        await owner_session.execute(text(f"LOCK TABLE {table} IN SHARE MODE NOWAIT"))
        if not await owner_session.scalar(text(f"SELECT EXISTS (SELECT 1 FROM {table})")):
            return
        async with preserve_transaction_sql_settings(database, ["lock_timeout"], quote_literal):
            snapshot = await receipts.capture_native_dependencies(owner_session, schema, lock=True)
        predecessor = await receipts.read_current_receipt(owner_session, schema)
        if predecessor is None or await owner_session.scalar(
            text(f"SELECT EXISTS (SELECT 1 FROM {table} WHERE predecessor_receipt_id=:receipt_id)"),
            {"receipt_id": predecessor["receipt_id"]},
        ):
            raise RuntimeError("entity_address_common_receipt_history_inconsistent")
        publication_by_field.update(predecessor=predecessor, snapshot=snapshot, session=owner_session)

    async def after_publish():
        """Append the successor only after address authority advances on the same session."""
        if not publication_by_field:
            return
        predecessor = publication_by_field["predecessor"]
        snapshot, owner_session = publication_by_field["snapshot"], publication_by_field["session"]
        if _publication_session(database) is not owner_session:
            raise RuntimeError("entity_address_common_receipt_transaction_changed")
        result = await receipts.capture_native_dependencies(owner_session, schema)
        if any(result[key] != value for key, value in snapshot.items() if key != "address"):
            raise RuntimeError("entity_address_common_receipt_dependencies_changed")
        previous_address, address = snapshot["address"], result["address"]
        if (
            address["local_lineage_id"] != previous_address["local_lineage_id"]
            or address["local_generation"] != previous_address["local_generation"] + 1
            or address["origin_lineage_id"] != address["local_lineage_id"]
            or address["origin_generation"] != address["local_generation"]
        ):
            raise RuntimeError("entity_address_common_receipt_address_not_advanced")
        payload = {
            **predecessor["payload"],
            **result,
            "predecessor_receipt_id": predecessor["receipt_id"],
            "expected_incumbent": {key: predecessor["payload"]["cms"][key] for key in receipts._PIN_FIELDS},
        }
        await receipts.append_serving_receipt(owner_session, schema, payload)

    return EntityAddressCutoverCallbacks(before_cutover=before_cutover, after_publish=after_publish)
