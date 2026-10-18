# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native command stages and set-based effective membership construction."""

import hashlib
import io
import json
import re
from dataclasses import dataclass
from uuid import UUID, uuid5

from process.network_approved_membership_source import (
    ApprovedMembershipSource,
    read_approved_membership_commands,
)
from process.network_membership_candidate_lifecycle import _control_namespace, _locked_candidate, _require_open
from process.network_membership_copy import COPY_COLUMNS, MembershipCopyTarget
from process.registry_source_retention import (
    SOURCE_TABLE,
    RegistrySourceRetentionError,
    _stage_projection,
    verify_registry_source_retention,
)


@dataclass(frozen=True)
class RegistryMembershipCommandStage:
    source: ApprovedMembershipSource
    add_table: str
    remove_table: str
    add_rows: int
    remove_rows: int

    def __post_init__(self):
        if type(self.source) is not ApprovedMembershipSource or any(
            type(count) is not int or not 0 <= count < 2**63 for count in (self.add_rows, self.remove_rows)
        ):
            raise RegistrySourceRetentionError("registry_composition_command_stage_invalid")
        for name, prefix in ((self.add_table, "add"), (self.remove_table, "remove")):
            if type(name) is not str or re.fullmatch("registry_command_" + prefix + "_[0-9a-f]{32}", name) is None:
                raise RegistrySourceRetentionError("registry_composition_command_stage_invalid")
        if self.add_table.rsplit("_", 1)[1] != self.remove_table.rsplit("_", 1)[1]:
            raise RegistrySourceRetentionError("registry_composition_command_stage_invalid")
        if self.add_rows + self.remove_rows != self.source.total_rows:
            raise RegistrySourceRetentionError("registry_composition_command_accounting_invalid")


def _relation(table_name):
    return 'pg_temp."' + table_name + '"'


async def _copy_commands(connection, table_name, copy_bytes, row_count):
    if row_count:
        with io.BytesIO(copy_bytes) as stream:
            status = await connection.copy_to_table(
                table_name, schema_name="pg_temp", columns=COPY_COLUMNS, format="binary", source=stream
            )
            if status != f"COPY {row_count}" or stream.tell() != len(copy_bytes):
                raise RegistrySourceRetentionError("registry_composition_command_copy_invalid")


async def stage_approved_membership_commands(connection, approved_source, request_id, *, control_schema=None):
    """Read each complete document once and COPY its two native partitions."""
    if type(request_id) is not UUID or not request_id.int or type(approved_source) is not ApprovedMembershipSource:
        raise RegistrySourceRetentionError("registry_composition_command_stage_invalid")
    if not connection.is_in_transaction():
        raise RegistrySourceRetentionError("registry_composition_requires_transaction")
    suffix = uuid5(request_id, "commands:" + approved_source.generation_id).hex
    tables = ("registry_command_add_" + suffix, "registry_command_remove_" + suffix)
    async with connection.transaction():
        for table in tables:
            await connection.execute(f"""CREATE TEMP TABLE {_relation(table)} (
              network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,
              location_id uuid NOT NULL,evidence_id text NOT NULL)""")
        await connection.execute(
            f"ALTER TABLE {_relation(tables[1])} ADD PRIMARY KEY(network_id,provider_system,provider_id,location_id)"
        )
        cursor, additions, removals = None, 0, 0
        while True:
            batch = await read_approved_membership_commands(
                connection, approved_source, after_network_key=cursor, control_schema=control_schema
            )
            if batch is None:
                break
            if batch.source != approved_source or hashlib.sha256(batch.input_bytes).hexdigest() != batch.input_sha256:
                raise RegistrySourceRetentionError("registry_composition_command_stage_invalid")
            await _copy_commands(connection, tables[0], batch.additions_copy_bytes, batch.additions_count)
            await _copy_commands(connection, tables[1], batch.removals_copy_bytes, batch.removals_count)
            additions += batch.additions_count
            removals += batch.removals_count
            cursor = batch.network_key
        return RegistryMembershipCommandStage(approved_source, *tables, additions, removals)


def _source_stage_relation(source_stage):
    from process.registry_source_recipe_composition import RegistrySourceRecipeStage

    if (
        type(source_stage) is not RegistrySourceRecipeStage
        or type(source_stage.table_name) is not str
        or re.fullmatch("registry_source_page_[0-9a-f]{32}", source_stage.table_name) is None
    ):
        raise RegistrySourceRetentionError("registry_source_retention_stage_invalid")
    return _relation(source_stage.table_name)


def _allowed_source(source_relation, command_stage):
    return f"""SELECT source.* FROM {source_relation} source WHERE NOT EXISTS (
      SELECT 1 FROM {_relation(command_stage.remove_table)} removed
      WHERE (removed.network_id,removed.provider_system,removed.provider_id,removed.location_id)=
            (source.network_id,source.provider_system,source.provider_id,source.location_id))"""


async def build_registry_source_retention(connection, source_stage, command_stage):
    """Account for all source observations before applying approved removals."""
    if type(command_stage) is not RegistryMembershipCommandStage or not connection.is_in_transaction():
        raise RegistrySourceRetentionError("registry_composition_command_stage_invalid")
    source = _stage_projection(_source_stage_relation(source_stage))
    counts = await connection.fetchrow(f"""WITH source AS MATERIALIZED ({source}),
      allowed AS ({_allowed_source("source", command_stage)})
      SELECT (SELECT count(*) FROM source) AS source_rows,
        (SELECT count(*) FROM source)-(SELECT count(*) FROM allowed) AS suppressed_source_rows,
        (SELECT count(*) FROM {_relation(command_stage.add_table)}) AS add_rows,
        (SELECT count(*) FROM {_relation(command_stage.remove_table)}) AS remove_rows""")
    if (counts["source_rows"], counts["add_rows"], counts["remove_rows"]) != (
        source_stage.membership_rows,
        command_stage.add_rows,
        command_stage.remove_rows,
    ):
        raise RegistrySourceRetentionError("registry_composition_command_accounting_invalid")
    return {
        "component": "registry_source_retention",
        "revision": 1,
        "table_name": SOURCE_TABLE,
        "source_generation_sha256": source_stage.generation_sha256,
        **dict(counts),
        "effective_rows": counts["source_rows"] - counts["suppressed_source_rows"] + counts["add_rows"],
    }


def _command_scope(candidate, command_stage):
    receipt = verify_registry_source_retention(candidate)
    generations = candidate["source_generations"]
    generations = json.loads(generations) if type(generations) is str else generations
    if (
        receipt is None
        or type(command_stage) is not RegistryMembershipCommandStage
        or candidate["approved_custom_revision"] != command_stage.source.approved_revision
        or generations.get("custom_membership") != command_stage.source.generation_id
        or (receipt["add_rows"], receipt["remove_rows"]) != (command_stage.add_rows, command_stage.remove_rows)
    ):
        raise RegistrySourceRetentionError("registry_composition_command_scope_invalid")
    return receipt


async def verify_registry_composition_rows(connection, copy_target, candidate, command_stage):
    """Compare the complete effective multiset, including duplicate evidence."""
    receipt = _command_scope(candidate, command_stage)
    fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    if (
        type(copy_target) is not MembershipCopyTarget
        or not connection.is_in_transaction()
        or any(str(candidate[field]) != getattr(copy_target, field) for field in fields)
    ):
        raise RegistrySourceRetentionError("registry_source_retention_scope_invalid")
    namespace = '"' + copy_target.schema_name + '"'
    expected = _allowed_source(namespace + "." + SOURCE_TABLE, command_stage)
    expected += " UNION ALL SELECT * FROM " + _relation(command_stage.add_table)
    counts = await connection.fetchrow(f"""WITH expected AS MATERIALIZED ({expected}), differences AS (
      (SELECT * FROM {namespace}.network_membership EXCEPT ALL SELECT * FROM expected)
      UNION ALL (SELECT * FROM expected EXCEPT ALL SELECT * FROM {namespace}.network_membership))
      SELECT (SELECT count(*) FROM {_relation(command_stage.add_table)}) AS add_rows,
        (SELECT count(*) FROM {_relation(command_stage.remove_table)}) AS remove_rows,
        (SELECT count(*) FROM {namespace}.{SOURCE_TABLE}) AS source_rows,
        (SELECT count(*) FROM expected) AS effective_rows,
        EXISTS(SELECT 1 FROM differences) AS differs""")
    if counts["differs"] or any(counts[name] != receipt[name] for name in counts.keys() if name != "differs"):
        raise RegistrySourceRetentionError("registry_composition_effective_parity_invalid")


async def materialize_registry_composition(connection, copy_target, command_stage, *, control_schema=None):
    """Set-insert effective output once; the caller seals and closes it atomically."""
    if not connection.is_in_transaction():
        raise RegistrySourceRetentionError("registry_composition_requires_transaction")
    namespace = _control_namespace(control_schema)
    candidate = await _locked_candidate(connection, copy_target, namespace)
    _require_open(candidate)
    receipt = _command_scope(candidate, command_stage)
    table = '"' + copy_target.schema_name + '".network_membership'
    await connection.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    if candidate["accepted_rows"] or await connection.fetchval(f"SELECT EXISTS(SELECT 1 FROM {table})"):
        raise RegistrySourceRetentionError("registry_composition_effective_not_empty")
    source = '"' + copy_target.schema_name + '".' + SOURCE_TABLE
    status = await connection.execute(
        f"INSERT INTO {table} {_allowed_source(source, command_stage)}"
        " UNION ALL SELECT * FROM " + _relation(command_stage.add_table)
    )
    if status != f"INSERT 0 {receipt['effective_rows']}":
        raise RegistrySourceRetentionError("registry_composition_command_accounting_invalid")
    await verify_registry_composition_rows(connection, copy_target, candidate, command_stage)
    await connection.execute(
        f"UPDATE {namespace}.network_membership_candidate SET accepted_rows=$2 WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
        receipt["effective_rows"],
    )


async def drop_registry_command_stage(connection, command_stage):
    """Remove only this invocation's two exact temporary command heaps."""
    for name in (command_stage.add_table, command_stage.remove_table):
        await connection.execute(f"DROP TABLE {_relation(name)}")
