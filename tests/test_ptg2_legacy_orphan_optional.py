# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""In-memory contracts for optional legacy-sweeper stage relations."""

from __future__ import annotations

import pytest

from process.ptg_parts.ptg2_legacy_orphan_contract import canonical_sha256
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _MRF_OPTIONAL_TABLES,
    _MRF_REQUIRED_TABLES,
)
from process.ptg_parts.ptg2_legacy_orphan_store_mutation import (
    lock_legacy_sweep_authority,
)
from process.ptg_parts.ptg2_legacy_orphan_store_references import (
    _blocking_attachment_statements,
)
from process.ptg_parts.ptg2_legacy_orphan_store_schema import (
    _validated_optional_authority,
)
from process.ptg_parts.ptg2_v4_attempt_registry import ATTEMPT_ATTACHMENTS


def _optional_authority_row(
    table_name: str,
    *,
    relation_oid: int | None = None,
) -> dict[str, object]:
    is_relation_present = relation_oid is not None
    return {
        "expected_table_name": table_name,
        "qualified_name": f'"mrf"."{table_name}"',
        "relation_present": is_relation_present,
        "table_schema": "mrf" if is_relation_present else None,
        "table_name": table_name if is_relation_present else None,
        "relation_oid": relation_oid,
        "owner_oid": 10 if is_relation_present else None,
        "relkind": "r" if is_relation_present else None,
        "relpersistence": "u" if is_relation_present else None,
        "column_shape": [[1, "snapshot_id", 25, -1, False]],
    }


def test_optional_authority_matches_the_attempt_registry() -> None:
    assert set(_MRF_OPTIONAL_TABLES).isdisjoint(_MRF_REQUIRED_TABLES)
    assert {
        attachment.table_name
        for attachment in ATTEMPT_ATTACHMENTS
        if attachment.optional_relation
    } == set(_MRF_OPTIONAL_TABLES)


def test_optional_authority_absence_digest_is_exact_and_deterministic() -> None:
    absent_rows = [
        _optional_authority_row(table_name)
        for table_name in reversed(_MRF_OPTIONAL_TABLES)
    ]

    first = _validated_optional_authority(absent_rows, schema_name="mrf")
    second = _validated_optional_authority(
        list(reversed(absent_rows)),
        schema_name="mrf",
    )

    assert first == second
    assert first[1:] == ((), ())
    assert canonical_sha256(first[0]) == canonical_sha256(second[0])
    assert first[0] == tuple(
        {
            "table_name": table_name,
            "qualified_name": f'"mrf"."{table_name}"',
            "present": False,
        }
        for table_name in _MRF_OPTIONAL_TABLES
    )


@pytest.mark.parametrize(
    "present_names",
    (
        frozenset(),
        frozenset({"ptg2_price_set_stage"}),
        frozenset(_MRF_OPTIONAL_TABLES),
    ),
)
def test_residue_sql_references_only_present_optional_relations(
    present_names: frozenset[str],
) -> None:
    sql = " UNION ALL ".join(
        _blocking_attachment_statements(
            "mrf",
            present_optional_table_names=present_names,
        )
    )

    for table_name in _MRF_OPTIONAL_TABLES:
        assert (f'"{table_name}"' in sql) is (table_name in present_names)


@pytest.mark.parametrize(
    ("present_names", "expected_oids"),
    (
        (frozenset(), ()),
        (frozenset({"ptg2_price_set_stage"}), (101,)),
        (frozenset(_MRF_OPTIONAL_TABLES), (101, 102)),
    ),
)
def test_optional_authority_binds_each_present_catalog_shape(
    present_names: frozenset[str],
    expected_oids: tuple[int, ...],
) -> None:
    optional_relation_rows = [
        _optional_authority_row(
            table_name,
            relation_oid=101 + index if table_name in present_names else None,
        )
        for index, table_name in enumerate(_MRF_OPTIONAL_TABLES)
    ]

    authority_payload, names, oids = _validated_optional_authority(
        optional_relation_rows,
        schema_name="mrf",
    )

    assert names == tuple(sorted(present_names))
    assert oids == expected_oids
    for authority_entry in authority_payload:
        if authority_entry["table_name"] in present_names:
            assert authority_entry["present"] is True
            assert authority_entry["catalog"]["table_schema"] == "mrf"
            assert authority_entry["catalog"]["column_shape"] == [
                [1, "snapshot_id", 25, -1, False]
            ]
        else:
            assert authority_entry == {
                "table_name": authority_entry["table_name"],
                "qualified_name": (
                    f'"mrf"."{authority_entry["table_name"]}"'
                ),
                "present": False,
            }


@pytest.mark.parametrize(
    "relation_rows",
    (
        [_optional_authority_row("ptg2_price_set_stage")],
        [
            {
                **_optional_authority_row("ptg2_price_set_stage"),
                "qualified_name": '"other"."ptg2_price_set_stage"',
            },
            _optional_authority_row("ptg2_serving_rate_stage"),
        ],
        [
            {
                **_optional_authority_row("ptg2_price_set_stage"),
                "owner_oid": 10,
            },
            _optional_authority_row("ptg2_serving_rate_stage"),
        ],
    ),
)
def test_optional_authority_rejects_invalid_absence_probes(
    relation_rows: list[dict[str, object]],
) -> None:
    with pytest.raises(
        RuntimeError,
        match="legacy_sweep_optional_relations_probe_invalid",
    ):
        _validated_optional_authority(relation_rows, schema_name="mrf")


def test_optional_authority_rejects_present_catalog_drift() -> None:
    relation_rows = [
        {
            **_optional_authority_row(
                "ptg2_price_set_stage",
                relation_oid=101,
            ),
            "table_schema": "other",
        },
        _optional_authority_row("ptg2_serving_rate_stage"),
    ]

    with pytest.raises(
        RuntimeError,
        match="legacy_sweep_optional_relations_catalog_invalid",
    ):
        _validated_optional_authority(relation_rows, schema_name="mrf")


class _StatusExecutor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def status(self, statement: str, **_parameters) -> None:
        self.statements.append(statement)


@pytest.mark.asyncio
async def test_authority_lock_rejects_unrecognized_optional_table() -> None:
    executor = _StatusExecutor()

    with pytest.raises(
        ValueError,
        match="legacy sweep optional authority is invalid",
    ):
        await lock_legacy_sweep_authority(
            executor,
            schema_name="mrf",
            control_schema_name="control_plane",
            lock_timeout="5s",
            present_optional_table_names=("unknown_stage",),
        )

    assert executor.statements[-1] == (
        "LOCK TABLE pg_catalog.pg_class IN SHARE MODE"
    )


def test_residue_sql_rejects_unrecognized_optional_table() -> None:
    with pytest.raises(
        ValueError,
        match="legacy sweep optional authority is invalid",
    ):
        _blocking_attachment_statements(
            "mrf",
            present_optional_table_names=frozenset({"unknown_stage"}),
        )
