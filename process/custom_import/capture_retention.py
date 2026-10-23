# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Application-owned retained-byte checks while the caller holds the dataset lock."""

from sqlalchemy import Numeric, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportCaptureUsage,
)
from db.models.custom_import_prepared_source import CustomImportPreparedSource
from db.models.custom_import_raw_capture import CustomImportCaptureRawObject
from process.custom_import.capture_store import (
    CaptureBundleConflict,
    CaptureStoreError,
    _positive_id,
    _require_clean_session,
    _require_transaction,
)

_MAX_BIGINT = 2**63 - 1


async def require_capture_retention_read_access(session: AsyncSession) -> None:
    """Check quota metadata privileges without reading retained rows or payloads."""

    _require_transaction(session)
    _require_clean_session(session)
    for columns in (
        (CustomImportCaptureUsage.dataset_id, CustomImportCaptureUsage.retained_bytes),
        (CustomImportCaptureRawObject.dataset_id, CustomImportCaptureRawObject.byte_count),
        (CustomImportPreparedSource.dataset_id, CustomImportPreparedSource.retained_byte_count),
    ):
        await session.execute(select(*columns).limit(0))


async def validate_dataset_retained_bytes(
    session: AsyncSession,
    *,
    dataset_id: int,
    new_byte_count: int,
    maximum_retained_bytes: int,
) -> None:
    """Check every retained revision; the caller must already own the dataset lock."""

    _require_transaction(session)
    _require_clean_session(session)
    dataset_id = _positive_id(dataset_id, "dataset_id")
    for label, value in (("new_byte_count", new_byte_count), ("maximum_retained_bytes", maximum_retained_bytes)):
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _MAX_BIGINT:
            raise CaptureStoreError(f"{label} must be a bounded non-negative integer")
    isolation, retained_bytes = (await session.execute(_retained_bytes_statement(dataset_id, new_byte_count))).one()
    if isolation != "read committed":
        raise CaptureStoreError("capture retention requires READ COMMITTED")
    if retained_bytes < 0 or retained_bytes > maximum_retained_bytes:
        raise CaptureBundleConflict("capture dataset retained-byte quota exceeded")


def _retained_bytes_statement(dataset_id: int, new_byte_count: int):
    usage = (
        select(CustomImportCaptureUsage.retained_bytes)
        .where(CustomImportCaptureUsage.dataset_id == dataset_id)
        .scalar_subquery()
    )
    parquet_bytes = (
        select(
            func.coalesce(
                func.sum(
                    cast(CustomImportCaptureParquetPart.byte_count, Numeric)
                    + func.coalesce(
                        func.octet_length(
                            func.convert_to(CustomImportCaptureParquetPart.canonical_capture_manifest, "UTF8")
                        ),
                        0,
                    )
                ),
                0,
            )
        )
        .join(
            CustomImportCaptureBundle,
            CustomImportCaptureBundle.capture_bundle_id == CustomImportCaptureParquetPart.capture_bundle_id,
        )
        .where(CustomImportCaptureBundle.dataset_id == dataset_id)
        .scalar_subquery()
    )
    raw_bytes = (
        select(func.coalesce(func.sum(cast(CustomImportCaptureRawObject.byte_count, Numeric)), 0))
        .where(CustomImportCaptureRawObject.dataset_id == dataset_id)
        .scalar_subquery()
    )
    prepared_bytes = (
        select(func.coalesce(func.sum(cast(CustomImportPreparedSource.retained_byte_count, Numeric)), 0))
        .where(CustomImportPreparedSource.dataset_id == dataset_id)
        .scalar_subquery()
    )
    return select(
        func.current_setting("transaction_isolation"),
        func.coalesce(usage, parquet_bytes) + raw_bytes + prepared_bytes + new_byte_count,
    )
