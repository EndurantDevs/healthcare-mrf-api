# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed metadata plans for a separately admitted common serving inverse."""

from dataclasses import dataclass

from db.plan_release_storage import storage_identifier
from process import provider_directory_cms_serving_receipt as common
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_output_snapshot_catalog import decoded, encoded


@dataclass(frozen=True)
class MetadataRestore:
    """Trusted fixed-table SQL only; this plan grants no mutation or capacity authority."""

    family: str
    sql: str
    parameters: tuple


def _require(condition):
    receipts.require(condition, "common_inverse_metadata_changed")


def _restore_sql(schema, family, model, key, value, payload):
    table = receipts.qualified(schema, model.__tablename__)
    key_sql = storage_identifier(key)
    if payload is None:
        return MetadataRestore(family, f"DELETE FROM {table} WHERE {key_sql}=:restore_key", (("restore_key", value),))
    columns = tuple(column.name for column in model.__table__.columns)
    _require(type(payload) is dict and set(payload) == set(columns) and payload[key] == value)
    fields = ",".join(storage_identifier(column) for column in columns if column != key)
    sql = (
        f"UPDATE {table} SET ({fields})=(SELECT {fields} "
        f"FROM jsonb_populate_record(NULL::{table},CAST(:restore_metadata AS jsonb))) WHERE {key_sql}=:restore_key"
    )
    return MetadataRestore(family, sql, (("restore_key", value), ("restore_metadata", encoded(payload))))


def metadata_restore_sql(schema, baseline):
    """Restore the exact Profile head/absence and Overlay alias timestamps, never a new date."""
    from db.models import AddressAliasArtifactStateV1, ProviderDirectoryProfileServingGeneration
    from process.provider_directory_common_serving_recovery_document import validate_state

    metadata = validate_state(baseline)["metadata"]
    return (
        _restore_sql(
            schema, "profile", ProviderDirectoryProfileServingGeneration, "singleton_key", "global", metadata["profile"]
        ),
        _restore_sql(
            schema,
            "overlay_alias",
            AddressAliasArtifactStateV1,
            "artifact_name",
            "provider_directory_address_overlay",
            metadata["overlay_alias"],
        ),
    )


def common_receipt_payload(baseline_metadata, forward_metadata, restored_native):
    """Continue the existing CMS tip with both restored Profile selection and advanced Address."""
    previous, forward = baseline_metadata["common_receipt"], forward_metadata["common_receipt"]
    if previous is None or forward is None:
        _require(previous is forward)
        return None
    old, current = map(common.validate_receipt_payload, (previous["payload"], forward["payload"]))
    _require(old["cms"] == current["cms"] and old.get("archive") == current.get("archive"))
    _require(set(restored_native) == {"address", "profile", "doctors", "alias_generation", "overlay_oid"})
    _require(all(restored_native[key] == old[key] for key in ("profile", "doctors", "alias_generation", "overlay_oid")))
    old_address, address = current["address"], restored_native["address"]
    _require(
        address["local_lineage_id"] == old_address["local_lineage_id"]
        and address["local_generation"] == old_address["local_generation"] + 1
        and address["origin_lineage_id"] == address["local_lineage_id"]
        and address["origin_generation"] == address["local_generation"]
    )
    return common.validate_receipt_payload(
        {
            **old,
            **restored_native,
            "predecessor_receipt_id": forward["receipt_id"],
            "expected_incumbent": {key: current["cms"][key] for key in common._PIN_FIELDS},
        }
    )


async def assert_common_ancestry(driver, schema, baseline_metadata, forward_metadata):
    """Read only the exact existing one/two-hop forward continuation, with unchanged CMS evidence."""
    baseline, forward = baseline_metadata["common_receipt"], forward_metadata["common_receipt"]
    if baseline is None or forward is None:
        _require(baseline is forward)
        return
    table = receipts.qualified(schema, common._TABLE)
    rows = await driver.fetch(
        f"WITH RECURSIVE path AS (SELECT receipt_id,payload,predecessor_receipt_id,0 AS depth FROM {table} "
        "WHERE receipt_id=$1 UNION ALL "
        f"SELECT parent.receipt_id,parent.payload,parent.predecessor_receipt_id,path.depth+1 FROM {table} parent "
        "JOIN path ON parent.receipt_id=path.predecessor_receipt_id WHERE path.depth<2 AND path.receipt_id<>$2) "
        "SELECT receipt_id,payload,depth,receipt_id=encode(sha256(convert_to(payload::text,'UTF8')),'hex') AS valid_hash "
        "FROM path ORDER BY depth",
        forward["receipt_id"],
        baseline["receipt_id"],
    )
    _require(2 <= len(rows) <= 3 and rows[-1]["receipt_id"] == baseline["receipt_id"])
    _require([row["depth"] for row in rows] == list(range(len(rows))))
    _require(len({row["receipt_id"] for row in rows}) == len(rows))
    for index, row in enumerate(rows):
        payload = common.validate_receipt_payload(decoded(row["payload"]))
        if index + 1 < len(rows):
            _require(payload["predecessor_receipt_id"] == rows[index + 1]["receipt_id"])
        _require(row["valid_hash"] is True and payload["cms"] == baseline["payload"]["cms"])
        _require(payload.get("archive") == baseline["payload"].get("archive"))
    _require(decoded(rows[0]["payload"]) == forward["payload"] and decoded(rows[-1]["payload"]) == baseline["payload"])
