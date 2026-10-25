# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Join installed directory and pricing controls without replaying either owner."""

from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from alembic import command
from tests.provider_directory_retirement_native_pg_support import (
    _parent_control_identity,
    _parent_update_migration,
    _set_parent_scope,
)
from tests.provider_directory_terminal_root_retirement_v2_pg_support import retirement_trigger_state
from tests.test_provider_directory_terminal_root_retirement_postgres import (
    _scenario,
    _standalone_retirement_database,
)

ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR = "20261022120000_provider_directory_first_retirement"
DIRECTORY = "20261023100000_provider_directory_retirement_parent_updates"
PRICING = "20261023110000_code_pricing_import_idempotency"
JOIN = "20261023220000_merge_directory_pricing_controls"
HEAD = "20261024050000_provider_directory_first_retirement_owner_lineage"
SERVING_RECOVERY = "20261023100000_provider_directory_serving_recovery"
COMPANY_AFFILIATION = "20261025010000_company_affiliation_states"
COMPANY_JOIN = "20261025020000_company_affiliation_join"
INDEX = "import_run_code_pricing_idempotency_idx"


def _config(connection=None):
    configuration = Config(str(ROOT / "alembic.ini"))
    configuration.set_main_option("script_location", str(ROOT / "alembic"))
    if connection is not None:
        configuration.attributes["connection"] = connection
    return configuration


def test_directory_pricing_join_preserves_exact_parents_without_database_work():
    scripts = ScriptDirectory.from_config(_config())
    assert scripts.get_heads() == [COMPANY_JOIN]
    assert scripts.get_revision(COMPANY_JOIN).down_revision == (SERVING_RECOVERY, COMPANY_AFFILIATION)
    assert scripts.get_revision(COMPANY_AFFILIATION).down_revision == JOIN
    assert scripts.get_revision(SERVING_RECOVERY).down_revision == HEAD
    assert scripts.get_revision(HEAD).down_revision == JOIN
    assert scripts.get_revision(JOIN).down_revision == (DIRECTORY, PRICING)
    assert scripts.get_revision(DIRECTORY).down_revision == PREDECESSOR
    assert scripts.get_revision(PRICING).down_revision == PREDECESSOR
    migration = scripts.get_revision(JOIN).module
    assert migration.branch_labels is None and migration.depends_on is None
    assert migration.upgrade() is None and migration.downgrade() is None


@pytest.mark.parametrize(
    "installed,expected",
    (
        (DIRECTORY, [PRICING, JOIN]),
        (PRICING, [DIRECTORY, JOIN]),
        ((DIRECTORY, PRICING), [JOIN]),
        (PREDECESSOR, [PRICING, DIRECTORY, JOIN]),
    ),
    ids=("directory", "pricing", "both", "common-predecessor"),
)
def test_directory_pricing_join_does_not_replay_installed_history(installed, expected):
    scripts = ScriptDirectory.from_config(_config())
    assert [step.revision.revision for step in scripts._upgrade_revs(JOIN, installed)] == expected


async def _index_oid(scenario):
    return await scenario.connection.fetchval("SELECT to_regclass($1)::oid", f"{scenario.schema_name}.{INDEX}")


async def _parent_scope(scenario):
    return await scenario.connection.fetchval(
        "SELECT cardinality(tgattr::smallint[]) FROM pg_trigger "
        "WHERE tgrelid=$1::regclass AND tgname='pd_trr_dataset_row'",
        f"{scenario.schema_name}.provider_directory_endpoint_dataset",
    )


def _migrate(connection, action, target):
    getattr(command, action)(_config(connection), target)


async def _require_join(connection, scenario, index_before):
    assert await connection.scalar(text(f"SELECT version_num FROM {scenario.schema}.alembic_version")) == JOIN
    index_oid = await connection.scalar(
        text("SELECT to_regclass(:index)::oid"), {"index": f"{scenario.schema_name}.{INDEX}"}
    )
    assert index_oid is not None
    if index_before is not None:
        assert index_oid == index_before
    assert await connection.scalar(
        text("SELECT indisvalid AND indisready AND indisunique FROM pg_index WHERE indexrelid=:index"),
        {"index": index_oid},
    )
    assert (
        await connection.scalar(
            text(
                "SELECT cardinality(tgattr::smallint[]) FROM pg_trigger "
                "WHERE tgrelid=CAST(:relation AS regclass) AND tgname='pd_trr_dataset_row'"
            ),
            {"relation": f"{scenario.schema_name}.provider_directory_endpoint_dataset"},
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("installed", (DIRECTORY, PRICING), ids=("directory", "pricing"))
async def test_directory_pricing_join_preserves_native_controls_rows_and_outer_transaction(monkeypatch, installed):
    """Run genuine upgrades from each parent, roll back once, then commit and replay."""
    async with _scenario(monkeypatch) as scenario:
        await _set_parent_scope(scenario, _parent_update_migration()._LEGACY_COLUMNS)
        # The reduced retirement fixture predates this existing ImportRun column.
        await scenario.connection.execute(f"ALTER TABLE {scenario.schema}.import_run ADD COLUMN idempotency_key text")
        async with _standalone_retirement_database(scenario, administrator=True) as database:
            async with database.engine.connect() as connection:
                async with connection.begin():
                    await connection.run_sync(_migrate, "stamp", PREDECESSOR)
                    await connection.run_sync(_migrate, "upgrade", installed)
                rows_before = await scenario.snapshot()
                controls_before = await _parent_control_identity(scenario)
                triggers_before = await retirement_trigger_state(scenario)
                scope_before = await _parent_scope(scenario)
                index_before = await _index_oid(scenario)
                assert (index_before is not None) is (installed == PRICING)
                assert scope_before == (0 if installed == DIRECTORY else 17)

                transaction = await connection.begin()
                try:
                    await connection.run_sync(_migrate, "upgrade", JOIN)
                    assert transaction.is_active and not connection.closed
                    await _require_join(connection, scenario, index_before)
                finally:
                    await transaction.rollback()
                assert await scenario.snapshot() == rows_before
                assert await _parent_control_identity(scenario) == controls_before
                assert await retirement_trigger_state(scenario) == triggers_before
                assert await _parent_scope(scenario) == scope_before
                assert await _index_oid(scenario) == index_before
                assert (
                    await scenario.connection.fetchval(f"SELECT version_num FROM {scenario.schema}.alembic_version")
                    == installed
                )

                async with connection.begin() as transaction:
                    await connection.run_sync(_migrate, "upgrade", JOIN)
                    assert transaction.is_active and not connection.closed
                    await _require_join(connection, scenario, index_before)
                index_after = await _index_oid(scenario)
                async with connection.begin():
                    await connection.run_sync(_migrate, "upgrade", JOIN)
                assert await _index_oid(scenario) == index_after
                assert await _parent_control_identity(scenario) == controls_before
                assert await retirement_trigger_state(scenario) == triggers_before
                assert await scenario.snapshot() == rows_before


@pytest.mark.parametrize(
    "installed,expected",
    (
        (JOIN, [COMPANY_AFFILIATION, HEAD, SERVING_RECOVERY, COMPANY_JOIN]),
        (SERVING_RECOVERY, [COMPANY_AFFILIATION, COMPANY_JOIN]),
        (COMPANY_AFFILIATION, [HEAD, SERVING_RECOVERY, COMPANY_JOIN]),
        ((SERVING_RECOVERY, COMPANY_AFFILIATION), [COMPANY_JOIN]),
    ),
    ids=("shared-predecessor", "directory-complete", "company-complete", "both-complete"),
)
def test_company_join_preserves_each_installed_branch(installed, expected):
    scripts = ScriptDirectory.from_config(_config())
    assert [step.revision.revision for step in scripts._upgrade_revs("head", installed)] == expected
    assert [step.revision.revision for step in scripts._upgrade_revs(COMPANY_AFFILIATION, JOIN)] == [
        COMPANY_AFFILIATION
    ]
