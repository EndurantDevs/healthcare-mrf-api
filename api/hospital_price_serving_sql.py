# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed indexed SQL for packed hospital-price serving."""

from __future__ import annotations

import os
import re

from sqlalchemy import text


def _schema() -> str:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise RuntimeError("hospital price database schema is invalid")
    return '"' + schema.replace('"', '""') + '"'


_SCHEMA = _schema()
VERSION_SQL = text(
    f"""SELECT version.version_id, version.parser_contract_sha256,
    version.source_format, version.template_version,
    version.service_count AS version_service_count,
    version.charge_count AS version_charge_count,
    version.payer_charge_count AS version_fact_count,
    current.service_count AS current_service_count,
    current.charge_count AS current_charge_count,
    current.payer_charge_count AS current_fact_count,
    root.format_version, root.service_count, root.charge_count, root.fact_count
    FROM {_SCHEMA}.hospital_price_hospital hospital
    JOIN {_SCHEMA}.hospital_price_current current
      ON current.hospital_id=hospital.hospital_id
    JOIN {_SCHEMA}.hospital_price_version_hospital binding
      ON binding.hospital_id=hospital.hospital_id
     AND binding.version_id=current.version_id
    JOIN {_SCHEMA}.hospital_price_version version
      ON version.version_id=current.version_id
    LEFT JOIN {_SCHEMA}.hospital_price_packed_root root
      ON root.version_id=version.version_id
    WHERE hospital.hospital_id=:hospital_id
      AND (
        CAST(:version_id AS varchar) IS NULL
        OR version.version_id=CAST(:version_id AS varchar)
      )"""
)
CODE_SELECTOR_SQL = text(
    f"""WITH selected AS (
      (SELECT block_ordinal, logical_first, secondary_first, secondary_count,
              page_index, page_count, payload
         FROM {_SCHEMA}.hospital_price_data_block
        WHERE version_id=:version_id AND block_kind=3
          AND key_sha256=:key_sha256 AND secondary_first<=:after_key
        ORDER BY secondary_first DESC LIMIT 1)
      UNION ALL
      (SELECT block_ordinal, logical_first, secondary_first, secondary_count,
              page_index, page_count, payload
         FROM {_SCHEMA}.hospital_price_data_block
        WHERE version_id=:version_id AND block_kind=3
          AND key_sha256=:key_sha256 AND secondary_first>:after_key
        ORDER BY secondary_first LIMIT 2)
    ) SELECT * FROM selected ORDER BY secondary_first"""
)
SERVICE_BLOCK_SQL = text(
    f"""WITH selected AS (
    SELECT DISTINCT block.block_ordinal
    FROM unnest(CAST(:charge_keys AS bigint[])) wanted(charge_key)
    CROSS JOIN LATERAL (
      SELECT candidate.block_ordinal, candidate.secondary_first,
             candidate.secondary_count
      FROM {_SCHEMA}.hospital_price_data_block candidate
      WHERE candidate.version_id=:version_id AND candidate.block_kind=1
        AND candidate.secondary_first<=wanted.charge_key
      ORDER BY candidate.secondary_first DESC LIMIT 1
    ) block
    WHERE wanted.charge_key < block.secondary_first + block.secondary_count
    ) SELECT block.block_ordinal, block.logical_first, block.logical_count,
             block.secondary_first, block.secondary_count, block.payload
        FROM selected JOIN {_SCHEMA}.hospital_price_data_block block
          ON block.version_id=:version_id AND block.block_kind=1
         AND block.block_ordinal=selected.block_ordinal
       ORDER BY block.block_ordinal"""
)
PAYER_SELECTOR_SQL = text(
    f"""WITH wanted(first_fact, fact_end) AS (
      SELECT * FROM unnest(
        CAST(:fact_starts AS bigint[]), CAST(:fact_ends AS bigint[])
      )
    ), selected AS (
      SELECT anchor.block_ordinal FROM wanted
      CROSS JOIN LATERAL (
        SELECT candidate.block_ordinal, candidate.secondary_first
        FROM {_SCHEMA}.hospital_price_data_block candidate
        WHERE candidate.version_id=:version_id AND candidate.block_kind=4
          AND candidate.key_sha256=:key_sha256
          AND candidate.secondary_first<=wanted.first_fact
        ORDER BY candidate.secondary_first DESC LIMIT 1
      ) anchor
      UNION
      SELECT block.block_ordinal
      FROM {_SCHEMA}.hospital_price_data_block block JOIN wanted
        ON block.secondary_first>=wanted.first_fact
       AND block.secondary_first<wanted.fact_end
      WHERE block.version_id=:version_id AND block.block_kind=4
        AND block.key_sha256=:key_sha256
    ) SELECT block.block_ordinal, block.logical_first,
             block.secondary_first, block.secondary_count,
             block.page_index, block.page_count, block.payload
        FROM selected JOIN {_SCHEMA}.hospital_price_data_block block
          ON block.version_id=:version_id AND block.block_kind=4
         AND block.block_ordinal=selected.block_ordinal
       ORDER BY block.page_index"""
)
FACT_BLOCK_SQL = text(
    f"""WITH selected AS (
    SELECT DISTINCT block.block_ordinal
    FROM unnest(CAST(:fact_ordinals AS bigint[])) wanted(fact_ordinal)
    CROSS JOIN LATERAL (
      SELECT candidate.block_ordinal, candidate.logical_first,
             candidate.logical_count
      FROM {_SCHEMA}.hospital_price_data_block candidate
      WHERE candidate.version_id=:version_id AND candidate.block_kind=2
        AND candidate.logical_first<=wanted.fact_ordinal
      ORDER BY candidate.logical_first DESC LIMIT 1
    ) block
    WHERE wanted.fact_ordinal < block.logical_first + block.logical_count
    ) SELECT block.block_ordinal, block.logical_first,
             block.logical_count, block.payload
        FROM selected JOIN {_SCHEMA}.hospital_price_data_block block
          ON block.version_id=:version_id AND block.block_kind=2
         AND block.block_ordinal=selected.block_ordinal
       ORDER BY block.block_ordinal"""
)


__all__ = (
    "CODE_SELECTOR_SQL",
    "FACT_BLOCK_SQL",
    "PAYER_SELECTOR_SQL",
    "SERVICE_BLOCK_SQL",
    "VERSION_SQL",
)
