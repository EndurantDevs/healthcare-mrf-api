# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""COPY staging and relational validation for hospital price artifacts."""

from __future__ import annotations

import hashlib
from typing import Any

from process.hospital_price_native import (
    HOSPITAL_MRF_COPY_COLUMNS,
    HospitalParserReceipt,
)
from process.ptg_parts.db_tables import _quote_ident


_TARGET = {
    "mrf": "hospital_price_version", "location": "hospital_price_version_location",
    "npi": "hospital_price_version_npi", "license": "hospital_price_version_license",
    "contract_provision": "hospital_price_contract_provision", "service": "hospital_price_service",
    "code": "hospital_price_service_code", "charge": "hospital_price_charge",
    "payer_charge": "hospital_price_payer_charge", "modifier": "hospital_price_modifier",
    "modifier_payer": "hospital_price_modifier_payer",
}
_KEYS = {
    "mrf": ("version_id",), "location": ("version_id", "location_ordinal"),
    "npi": ("version_id", "npi_ordinal"), "license": ("version_id", "license_ordinal"),
    "contract_provision": ("version_id", "provision_ordinal"),
    "service": ("version_id", "service_ordinal"),
    "code": ("version_id", "service_ordinal", "code_ordinal"),
    "charge": ("version_id", "service_ordinal", "charge_ordinal"),
    "payer_charge": ("version_id", "service_ordinal", "charge_ordinal", "payer_ordinal"),
    "modifier": ("version_id", "modifier_ordinal"),
    "modifier_payer": ("version_id", "modifier_ordinal", "payer_ordinal"),
}
_CHILDREN = tuple(kind for kind in _TARGET if kind != "mrf")
_REFERENCES = (
    ("code", "service", ("version_id", "service_ordinal")),
    ("charge", "service", ("version_id", "service_ordinal")),
    ("payer_charge", "charge", ("version_id", "service_ordinal", "charge_ordinal")),
    ("modifier_payer", "modifier", ("version_id", "modifier_ordinal")),
)


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
    for artifact in receipt.artifacts:
        columns = HOSPITAL_MRF_COPY_COLUMNS[artifact.kind]
        quoted_columns = ", ".join(map(_quote_ident, columns))
        stage = _quote_ident(stages[artifact.kind])
        await connection.status(
            f"CREATE TEMP TABLE {stage} ON COMMIT DROP AS SELECT {quoted_columns} "
            f"FROM {schema}.{_quote_ident(_TARGET[artifact.kind])} WITH NO DATA"
        )
        with artifact.path.open("rb") as source:
            measured = _DigestingSource(source)
            await copy(
                stages[artifact.kind], source=measured, columns=list(columns),
                format="text", delimiter="\t", null="\\N",
            )
        if measured.byte_count != artifact.bytes or measured.digest.hexdigest() != artifact.sha256:
            raise RuntimeError(f"hospital parser {artifact.kind} COPY changed before staging")


async def validate_stages(
    connection: Any,
    receipt: HospitalParserReceipt,
    stages: dict[str, str],
) -> None:
    """Validate staging counts, uniqueness, and child references."""

    for artifact in receipt.artifacts:
        stage = _quote_ident(stages[artifact.kind])
        count, wrong_version = await connection.first(
            f"SELECT COUNT(*), COUNT(*) FILTER (WHERE version_id <> :version_id) FROM {stage}",
            version_id=receipt.version_id,
        )
        if int(count) != artifact.rows or int(wrong_version):
            raise RuntimeError(f"hospital parser {artifact.kind} staging count is invalid")
        if artifact.kind == "mrf" and artifact.rows != 1:
            raise RuntimeError("hospital parser must produce one MRF header")
        keys = ", ".join(map(_quote_ident, _KEYS[artifact.kind]))
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
