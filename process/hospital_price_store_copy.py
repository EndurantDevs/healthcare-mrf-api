# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""COPY staging and relational validation for hospital price artifacts."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from typing import Any

from process.hospital_price_native import (
    HOSPITAL_MRF_PACKED_COPY_COLUMNS,
    HOSPITAL_MRF_TEXT_COPY_COLUMNS,
    HospitalParserReceipt,
)
from process.ptg_parts.db_tables import _quote_ident


_TEXT_TARGET = {
    "mrf": "hospital_price_version", "location": "hospital_price_version_location",
    "npi": "hospital_price_version_npi", "license": "hospital_price_version_license",
    "contract_provision": "hospital_price_contract_provision", "modifier": "hospital_price_modifier",
    "modifier_payer": "hospital_price_modifier_payer",
}
_TARGET = {
    **_TEXT_TARGET,
    "service_block": "hospital_price_data_block",
    "fact_block": "hospital_price_data_block",
    "selector_page": "hospital_price_data_block",
}
_KEYS = {
    "mrf": ("version_id",), "location": ("version_id", "location_ordinal"),
    "npi": ("version_id", "npi_ordinal"), "license": ("version_id", "license_ordinal"),
    "contract_provision": ("version_id", "provision_ordinal"),
    "modifier": ("version_id", "modifier_ordinal"),
    "modifier_payer": ("version_id", "modifier_ordinal", "payer_ordinal"),
}
_CHILDREN = tuple(kind for kind in _TEXT_TARGET if kind != "mrf")
_REFERENCES = (
    ("modifier_payer", "modifier", ("version_id", "modifier_ordinal")),
)
_PACKED_KINDS = {
    "service_block": 1,
    "fact_block": 2,
    "selector_page": None,
}


class _DigestingSource:
    def __init__(self, source: Any) -> None:
        self.source, self.digest, self.byte_count = source, hashlib.sha256(), 0

    def read(self, size: int = -1) -> bytes:
        """Read bytes while measuring the exact COPY stream."""

        chunk = self.source.read(size)
        self.digest.update(chunk)
        self.byte_count += len(chunk)
        return chunk


async def copy_stages(
    connection: Any,
    receipt: HospitalParserReceipt,
    stages: dict[str, str],
    schema_name: str,
) -> None:
    """COPY parser artifacts into private temporary staging tables."""

    schema = _quote_ident(schema_name)
    driver = getattr(connection.raw_connection, "driver_connection", connection.raw_connection)
    copy = getattr(driver, "copy_to_table", None)
    if copy is None:
        raise NotImplementedError("active database driver does not expose text COPY")
    artifacts_by_kind = {artifact.kind: artifact for artifact in receipt.artifacts}
    for kind, columns in HOSPITAL_MRF_TEXT_COPY_COLUMNS.items():
        artifact = artifacts_by_kind[kind]
        quoted_columns = ", ".join(map(_quote_ident, columns))
        stage = _quote_ident(stages[kind])
        await connection.status(
            f"CREATE TEMP TABLE {stage} ON COMMIT DROP AS SELECT {quoted_columns} "
            f"FROM {schema}.{_quote_ident(_TEXT_TARGET[kind])} WITH NO DATA"
        )
        with artifact.path.open("rb") as copy_file:
            measured = _DigestingSource(copy_file)
            await copy(
                stages[kind], source=measured, columns=list(columns),
                format="text", delimiter="\t", null="\\N",
            )
        if measured.byte_count != artifact.bytes or measured.digest.hexdigest() != artifact.sha256:
            raise RuntimeError(f"hospital parser {kind} COPY changed before staging")


async def validate_stages(
    connection: Any,
    receipt: HospitalParserReceipt,
    stages: dict[str, str],
) -> None:
    """Validate staging counts, uniqueness, and child references."""

    artifacts_by_kind = {artifact.kind: artifact for artifact in receipt.artifacts}
    for kind in HOSPITAL_MRF_TEXT_COPY_COLUMNS:
        artifact = artifacts_by_kind[kind]
        stage = _quote_ident(stages[kind])
        count, wrong_version = await connection.first(
            f"SELECT COUNT(*), COUNT(*) FILTER (WHERE version_id <> :version_id) FROM {stage}",
            version_id=receipt.version_id,
        )
        if int(count) != artifact.rows or int(wrong_version):
            raise RuntimeError(f"hospital parser {artifact.kind} staging count is invalid")
        if kind == "mrf" and artifact.rows != 1:
            raise RuntimeError("hospital parser must produce one MRF header")
        if kind == "mrf" and await connection.scalar(
            f"SELECT template_version FROM {stage}"
        ) != receipt.schema_version:
            raise RuntimeError("hospital parser template version conflicts with its receipt")
        keys = ", ".join(map(_quote_ident, _KEYS[kind]))
        await connection.status(f"CREATE UNIQUE INDEX ON {stage} ({keys})")
        await connection.status(f"ANALYZE {stage}")
    for child_kind, parent_kind, keys in _REFERENCES:
        child, parent = map(_quote_ident, (stages[child_kind], stages[parent_kind]))
        predicate = " AND ".join(
            f"parent.{_quote_ident(key)}=child.{_quote_ident(key)}" for key in keys
        )
        if await connection.scalar(
            f"SELECT EXISTS (SELECT 1 FROM {child} child LEFT JOIN {parent} parent "
            f"ON {predicate} WHERE parent.{_quote_ident(keys[0])} IS NULL)"
        ):
            raise RuntimeError(f"hospital parser {child_kind} has an unresolved reference")


def _copy_count(status_value: Any) -> int:
    match = re.search(r"(\d+)$", str(status_value))
    if match is None:
        raise RuntimeError("hospital packed COPY row count is missing")
    return int(match.group(1))


async def copy_packed_blocks(
    connection: Any,
    receipt: HospitalParserReceipt,
    schema_name: str,
) -> None:
    """Stream authenticated PostgreSQL binary COPY files into final storage."""

    raw_connection = connection.raw_connection
    driver = getattr(raw_connection, "driver_connection", raw_connection)
    copy = getattr(driver, "copy_to_table", None)
    if copy is None:
        raise NotImplementedError("active database driver does not expose binary COPY")
    artifacts_by_kind = {artifact.kind: artifact for artifact in receipt.artifacts}
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    if not getattr(os, "O_NOFOLLOW", 0):
        raise RuntimeError("hospital packed COPY requires non-symlink file opens")
    for kind in _PACKED_KINDS:
        artifact = artifacts_by_kind[kind]
        descriptor = os.open(artifact.path, flags)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_size != artifact.bytes:
                raise RuntimeError(f"hospital parser {kind} COPY file is invalid")
            with os.fdopen(descriptor, "rb", closefd=False) as copy_file:
                measured = _DigestingSource(copy_file)
                copy_status = await copy(
                    "hospital_price_data_block",
                    schema_name=schema_name,
                    source=measured,
                    columns=list(HOSPITAL_MRF_PACKED_COPY_COLUMNS),
                    format="binary",
                )
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if (
            _copy_count(copy_status) != artifact.rows
            or measured.byte_count != artifact.bytes
            or measured.digest.hexdigest() != artifact.sha256
            or (before.st_dev, before.st_ino, before.st_size)
            != (after.st_dev, after.st_ino, after.st_size)
        ):
            raise RuntimeError(f"hospital parser {kind} COPY changed during storage")


async def validate_packed_storage(
    connection: Any,
    receipt: HospitalParserReceipt,
    schema_name: str,
) -> None:
    """Prove dense packed ranges and selector page completeness before commit."""

    schema = _quote_ident(schema_name)
    await _validate_packed_root(connection, receipt, schema)
    block_counts_by_kind = await _validate_block_ordinals(
        connection, receipt, schema
    )
    await _validate_logical_ranges(
        connection, receipt, schema, block_counts_by_kind
    )
    await _validate_selector_pages(connection, receipt, schema)


async def _validate_packed_root(
    connection: Any,
    receipt: HospitalParserReceipt,
    schema: str,
) -> None:
    root = receipt.root
    stored_root = await connection.first(
        f"""SELECT root.format_version, root.service_count, root.charge_count,
        root.fact_count, root.code_selector_key_count,
        root.payer_plan_selector_key_count, root.code_selector_ref_count,
        root.payer_plan_selector_ref_count, root.service_block_count,
        root.fact_block_count, root.code_selector_page_count,
        root.payer_plan_selector_page_count, root.code_selector_block_count,
        root.payer_plan_selector_block_count, version.service_count,
        version.charge_count, version.payer_charge_count
        FROM {schema}.hospital_price_packed_root root
        JOIN {schema}.hospital_price_version version USING (version_id)
        WHERE root.version_id=:version""",
        version=receipt.version_id,
    )
    expected_root = (
        2,
        root.service_count,
        root.charge_count,
        root.fact_count,
        root.code_selector_key_count,
        root.payer_plan_selector_key_count,
        root.code_selector_ref_count,
        root.payer_plan_selector_ref_count,
        root.service_block_count,
        root.fact_block_count,
        root.code_selector_page_count,
        root.payer_plan_selector_page_count,
        root.code_selector_block_count,
        root.payer_plan_selector_block_count,
        root.service_count,
        root.charge_count,
        root.fact_count,
    )
    if stored_root is None or tuple(stored_root) != expected_root:
        raise RuntimeError("hospital packed root conflicts with stored projection")


async def _validate_block_ordinals(
    connection: Any,
    receipt: HospitalParserReceipt,
    schema: str,
) -> dict[int, int]:
    root = receipt.root
    block_counts_by_kind = {
        1: root.service_block_count,
        2: root.fact_block_count,
        3: root.code_selector_block_count,
        4: root.payer_plan_selector_block_count,
    }
    physical_rows = await connection.all(
        f"SELECT block_kind, COUNT(*), MIN(block_ordinal), MAX(block_ordinal) "
        f"FROM {schema}.hospital_price_data_block WHERE version_id=:version "
        "GROUP BY block_kind ORDER BY block_kind",
        version=receipt.version_id,
    )
    block_stats_by_kind = {
        int(kind): (int(count), int(first), int(last))
        for kind, count, first, last in physical_rows
    }
    for kind, expected_count in block_counts_by_kind.items():
        if expected_count == 0:
            if kind in block_stats_by_kind:
                raise RuntimeError("hospital packed empty block kind is not empty")
            continue
        if block_stats_by_kind.get(kind) != (
            expected_count,
            0,
            expected_count - 1,
        ):
            raise RuntimeError("hospital packed block ordinals are not dense")
    return block_counts_by_kind


async def _validate_logical_ranges(
    connection: Any,
    receipt: HospitalParserReceipt,
    schema: str,
    block_counts_by_kind: dict[int, int],
) -> None:
    root = receipt.root
    service_blocks, first_service, service_end, service_contiguous = (
        await connection.first(
            f"SELECT COUNT(*), MIN(logical_first), "
            f"MAX(logical_first + logical_count), "
            "COALESCE(BOOL_AND(logical_first <= prior_end "
            "AND logical_first >= GREATEST(prior_end - 1, 0)), false) FROM ("
            "SELECT logical_first, logical_count, "
            "COALESCE(MAX(logical_first + logical_count) OVER ("
            "ORDER BY block_ordinal ROWS BETWEEN UNBOUNDED PRECEDING "
            "AND 1 PRECEDING), 0) AS prior_end "
            f"FROM {schema}.hospital_price_data_block "
            "WHERE version_id=:version AND block_kind=1) packed",
            version=receipt.version_id,
        )
    )
    if (
        int(service_blocks) != root.service_block_count
        or int(first_service) != 0
        or int(service_end) != root.service_count
        or not service_contiguous
    ):
        raise RuntimeError("hospital packed logical ranges are not contiguous")

    for kind, first_column, count_column, expected in (
        (1, "secondary_first", "secondary_count", root.charge_count),
        (2, "logical_first", "logical_count", root.fact_count),
    ):
        if expected == 0:
            continue
        count, covered, contiguous = await connection.first(
            f"SELECT COUNT(*), COALESCE(SUM({count_column}), 0), "
            f"COALESCE(BOOL_AND({first_column}=prior), false) FROM ("
            f"SELECT {first_column}, {count_column}, "
            f"COALESCE(SUM({count_column}) OVER (ORDER BY block_ordinal "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS prior "
            f"FROM {schema}.hospital_price_data_block "
            "WHERE version_id=:version AND block_kind=:kind) packed",
            version=receipt.version_id,
            kind=kind,
        )
        if (
            int(count) != block_counts_by_kind[kind]
            or int(covered) != expected
            or not contiguous
        ):
            raise RuntimeError("hospital packed logical ranges are not contiguous")


async def _validate_selector_pages(
    connection: Any, receipt: HospitalParserReceipt, schema: str,
) -> None:
    root = receipt.root
    selector_summary = await connection.all(
        f"SELECT block_kind, COUNT(*), COALESCE(SUM(secondary_count), 0) "
        f"FROM {schema}.hospital_price_data_block "
        "WHERE version_id=:version AND block_kind IN (3, 4) "
        "GROUP BY block_kind ORDER BY block_kind",
        version=receipt.version_id,
    )
    observed_selectors_by_kind = {
        int(kind): (int(pages), int(refs))
        for kind, pages, refs in selector_summary
    }
    expected_selectors_by_kind = {
        3: (root.code_selector_block_count, root.code_selector_ref_count),
        4: (root.payer_plan_selector_block_count,
            root.payer_plan_selector_ref_count),
    }
    if observed_selectors_by_kind != {
        kind: counts
        for kind, counts in expected_selectors_by_kind.items()
        if counts[0]
    }:
        raise RuntimeError("hospital packed selector totals are invalid")
    selector_keys = root.code_selector_key_count + root.payer_plan_selector_key_count
    grouped_keys = await connection.all(
        f"SELECT block_kind, logical_first, MIN(logical_count), "
        f"MAX(logical_count) FROM {schema}.hospital_price_data_block "
        "WHERE version_id=:version AND block_kind IN (3, 4) "
        "GROUP BY block_kind, logical_first ORDER BY logical_first",
        version=receipt.version_id,
    )
    next_ordinal = 0
    keys_by_kind = {3: 0, 4: 0}
    for kind, logical_first, minimum_count, maximum_count in grouped_keys:
        if (
            int(logical_first) != next_ordinal
            or int(minimum_count) != int(maximum_count)
        ):
            raise RuntimeError("hospital packed selector key ordinals are not dense")
        next_ordinal += int(minimum_count)
        keys_by_kind[int(kind)] += int(minimum_count)
    if next_ordinal != selector_keys or keys_by_kind != {
        3: root.code_selector_key_count,
        4: root.payer_plan_selector_key_count,
    }:
        raise RuntimeError("hospital packed selector key ordinals are not dense")
    await _validate_selector_page_shapes(connection, receipt, schema)


async def _validate_selector_page_shapes(
    connection: Any, receipt: HospitalParserReceipt, schema: str,
) -> None:
    invalid_pages = await connection.scalar(
        f"SELECT EXISTS (SELECT 1 FROM (SELECT block_kind, logical_first, "
        "COUNT(*) AS rows, MIN(page_count) AS min_pages, "
        "MAX(page_count) AS max_pages, MIN(page_index) AS first_page, "
        "MAX(page_index) AS last_page, COUNT(DISTINCT page_index) AS pages, "
        "MIN(logical_count) AS min_keys, MAX(logical_count) AS max_keys, "
        "COUNT(parent_sha256) AS parent_values, "
        "COUNT(DISTINCT key_sha256) AS key_hashes, "
        "COUNT(DISTINCT parent_sha256) AS parent_hashes "
        f"FROM {schema}.hospital_price_data_block WHERE version_id=:version "
        "AND block_kind IN (3, 4) GROUP BY block_kind, logical_first) grouped "
        "WHERE rows<>min_pages OR min_pages<>max_pages OR first_page<>0 "
        "OR last_page<>rows-1 OR pages<>rows OR min_keys<>max_keys "
        "OR (min_keys>1 AND (rows<>1 OR first_page<>0)) "
        "OR key_hashes<>1 OR parent_values<>rows OR parent_hashes<>1)",
        version=receipt.version_id,
    )
    if invalid_pages:
        raise RuntimeError("hospital packed selector pages are incomplete")
    overlapping_ranges = await connection.scalar(
        f"SELECT EXISTS (SELECT 1 FROM (SELECT block_kind, key_sha256, "
        "parent_sha256, LAG(parent_sha256) OVER "
        "(PARTITION BY block_kind ORDER BY logical_first) AS prior_last "
        "FROM (SELECT block_kind, logical_first, MIN(key_sha256) AS key_sha256, "
        "MIN(parent_sha256) AS parent_sha256 "
        f"FROM {schema}.hospital_price_data_block WHERE version_id=:version "
        "AND block_kind IN (3, 4) GROUP BY block_kind, logical_first) grouped) ranged "
        "WHERE key_sha256>parent_sha256 OR prior_last>=key_sha256)",
        version=receipt.version_id,
    )
    if overlapping_ranges:
        raise RuntimeError("hospital packed selector digest ranges are invalid")
