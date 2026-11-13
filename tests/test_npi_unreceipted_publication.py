# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An import without public evidence publishes without a receipt and still tracks its tables."""

import datetime as dt
import json
from uuid import uuid4

import pytest

from process.npi_canonical_publication import NpiCanonicalPublicationError
from process.npi_canonical_publication_store import NpiCanonicalPublicationCommit
from process.npi_result_generation import publish_unreceipted_npi_result_generation
from process.npi_unreceipted_publication import commit_unreceipted_npi_publication, load_unreceipted_npi_commit
from process.nppes_public_evidence_listing import listed_nppes_archive_names
from tests.test_npi_result_generation import _authority_row

RUN_ID = "run_" + "a" * 32
ATTEMPT = (RUN_ID, RUN_ID + ":" + "b" * 32, "2026-01-01T00:00:00.000000+00:00")
LIVE_OIDS = (21, 22, 23, 24, 25, 26)
FINISHED = dt.datetime(2026, 1, 1, 1, tzinfo=dt.UTC)


class Connection:
    """Answer the generation read, the live relation lookup, and both updates in order."""

    def __init__(self, authority, *, run_row=None):
        self.authority, self.run_row, self.statements = authority, run_row, []

    async def fetchrow(self, statement, *arguments):
        self.statements.append((statement, arguments))
        if statement.startswith("SELECT singleton"):
            return self.authority
        if statement.startswith("SELECT to_regclass"):
            return {f"relation_{ordinal}": oid for ordinal, oid in enumerate(LIVE_OIDS, 1)}
        if statement.startswith("UPDATE") and "npi_result_generation" in statement:
            return {
                **self.authority,
                "local_generation": arguments[0],
                "origin_lineage_id": self.authority["local_lineage_id"],
                "origin_generation": arguments[0],
                "published_at": FINISHED,
                "relation_oids": arguments[1],
                "canonical_publication_ref": None,
                "canonical_publication_generation": None,
                "canonical_chain_ref": None,
                "canonical_import_date": None,
            }
        return self.run_row


def _tracked_authority():
    lineage = str(uuid4())
    return _authority_row(
        local_lineage_id=lineage,
        origin_lineage_id=lineage,
        origin_generation=4,
        published_at=FINISHED,
        relation_oids=[11, 12, 13, 14, 15, 16],
        canonical_publication_ref="nppub1_" + "c" * 43,
        canonical_publication_generation=2,
        canonical_chain_ref="penpc1_" + "d" * 43,
        canonical_import_date=dt.date(2026, 1, 1),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("authority", [_authority_row(), _tracked_authority()], ids=["untracked", "tracked"])
async def test_unreceipted_generation_binds_the_live_tables_without_provenance(authority):
    connection = Connection(authority)
    published = await publish_unreceipted_npi_result_generation(connection, schema_name="mrf")
    assert published.local_generation == authority["local_generation"] + 1
    assert published.relation_oids == LIVE_OIDS and published.canonical_provenance is None
    assert published.serving_generation.origin_generation == published.local_generation
    update, arguments = connection.statements[-1]
    assert "canonical_publication_ref=NULL" in update and "canonical_chain_ref=NULL" in update
    assert arguments == (authority["local_generation"] + 1, list(LIVE_OIDS))


@pytest.mark.asyncio
async def test_unreceipted_commit_marks_the_exact_attempt_without_a_snapshot():
    connection = Connection(_authority_row(), run_row={"heartbeat_at": FINISHED, "finished_at": FINISHED})
    progress_by_name, metrics_by_name = {"phase": "npi published"}, {"nppes_public_evidence": {"mode": "off"}}
    commit = await commit_unreceipted_npi_publication(
        connection, schema="mrf", attempt=ATTEMPT, progress_by_name=progress_by_name, metrics_by_name=metrics_by_name
    )
    assert commit == NpiCanonicalPublicationCommit(
        None, FINISHED.isoformat(timespec="microseconds"), commit.finished_at
    )
    update, arguments = connection.statements[-1]
    assert "snapshot_id=NULL" in update and "status='running'" in update and "importer='npi'" in update
    assert arguments[:3] == ATTEMPT
    assert json.loads(arguments[3]) == {
        **progress_by_name,
        "attempt_id": ATTEMPT[1],
        "attempt_started_at": ATTEMPT[2],
    }
    assert json.loads(arguments[4]) == metrics_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize("run_row", [None, {"heartbeat_at": "invalid", "finished_at": FINISHED}])
async def test_unreceipted_commit_refuses_a_changed_attempt_or_malformed_result(run_row):
    connection = Connection(_authority_row(), run_row=run_row)
    with pytest.raises(NpiCanonicalPublicationError):
        await commit_unreceipted_npi_publication(
            connection, schema="mrf", attempt=ATTEMPT, progress_by_name={}, metrics_by_name={}
        )


@pytest.mark.asyncio
async def test_unreceipted_generation_requires_the_authority_row():
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await publish_unreceipted_npi_result_generation(Connection(None), schema_name="mrf")


@pytest.mark.asyncio
async def test_uncertain_exit_reloads_the_exact_attempt_after_a_later_generation():
    # A receipted publication already replaced the serving generation before the readback.
    connection = Connection(_tracked_authority(), run_row={"heartbeat_at": FINISHED, "finished_at": FINISHED})
    progress_by_name, metrics_by_name = {"phase": "npi published"}, {"nppes_public_evidence": {"mode": "off"}}
    commit = await load_unreceipted_npi_commit(
        connection, schema="mrf", attempt=ATTEMPT, progress_by_name=progress_by_name, metrics_by_name=metrics_by_name
    )
    assert commit.receipt is None and commit.finished_at == FINISHED.isoformat(timespec="microseconds")
    statement, arguments = connection.statements[-1]
    assert "run.status='succeeded'" in statement and "run.snapshot_id IS NULL" in statement
    assert "npi_result_generation" not in statement and len(connection.statements) == 1
    assert arguments[0] == RUN_ID and len(arguments) == 3
    assert json.loads(arguments[1]) == {
        **progress_by_name,
        "attempt_id": ATTEMPT[1],
        "attempt_started_at": ATTEMPT[2],
    }
    connection.run_row = None
    assert (
        await load_unreceipted_npi_commit(
            connection,
            schema="mrf",
            attempt=ATTEMPT,
            progress_by_name=progress_by_name,
            metrics_by_name=metrics_by_name,
        )
        is None
    )


def test_listing_links_on_one_line_stay_separate_archives():
    listing = (
        '<a href="NPPES_Data_Dissemination_November_2026_V2.zip">monthly</a> '
        '<a href="./NPPES_Data_Dissemination_110226_110826_Weekly_V2.zip">weekly</a>'
        '<a href="NPPES_Deactivated_NPI_Report_110926_V2.zip">other</a>'
        '<a href="NPPES_Data_Dissemination_November_2026_V2.zip">again</a>'
    )
    assert listed_nppes_archive_names(listing) == [
        "NPPES_Data_Dissemination_November_2026_V2.zip",
        "NPPES_Data_Dissemination_110226_110826_Weekly_V2.zip",
    ]
