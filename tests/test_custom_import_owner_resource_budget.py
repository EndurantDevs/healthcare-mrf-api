# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admission reserves retained cohorts and independent overlapping work owners."""

import pytest

from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_cli as cli
from tests.test_control_owner_handoff import _profile


def _admit(profile_document):
    return cli.profile(
        {cli.PROFILE_ENV: handoff.canonical(profile_document).decode(), cli.NODE_ENV: profile_document["node_name"]}
    )


def test_completed_cohort_cannot_fit_in_a_worker_concurrency_allowance():
    profile_document, _signed = _profile()
    profile_document["cpu_millis"] = 8000
    profile_document["source_limits"]["maximum_workers"] = 8
    profile_document["source_limits"]["input"]["shard_count"] = 256
    # The old admission accepted 1 MiB although just 256 retained 4 KiB audits
    # and 256 retained 4 KiB outputs have a 2 MiB declared allowance.
    assert profile_document["disk_bytes"] == 1024**2
    with pytest.raises(ValueError):
        _admit(profile_document)


@pytest.mark.parametrize("shards,workers,minimum", [(1, 1, 239616), (256, 8, 4489216)])
def test_disk_admission_rejects_one_byte_below_complete_declared_allowance(shards, workers, minimum):
    profile_document, _signed = _profile()
    profile_document["cpu_millis"] = workers * 1000
    profile_document["source_limits"]["maximum_workers"] = workers
    profile_document["source_limits"]["input"]["shard_count"] = shards
    # Twenty independent fact allowances cover ten recovery slots, six READY
    # roles, and descriptor/scratch/route/worker-checkpoint owners. Other roots
    # reserve input + two plans + journal + common COPY + checkpoint allowance.
    profile_document["disk_bytes"] = minimum - 1
    with pytest.raises(ValueError):
        _admit(profile_document)
    profile_document["disk_bytes"] = minimum
    assert _admit(profile_document)[0] == profile_document


@pytest.mark.parametrize("kind", ["audit", "output"])
def test_retained_worker_allowance_grows_with_shards_not_running_processes(kind):
    profile_document, _signed = _profile()
    profile_document["source_limits"]["input"]["shard_count"] = 256
    limits = cli.source_limits(profile_document["source_limits"])
    profile_document["disk_bytes"] = cli._disk_allowance(limits)
    original = profile_document["source_limits"]["worker"][kind]
    profile_document["source_limits"]["worker"][kind] = original | {
        "max_spool_bytes": original["max_spool_bytes"] + 1024
    }
    # One more KiB remains possible in every shard, including completed ones.
    profile_document["disk_bytes"] += 2 * 256 * 1024 - 1
    with pytest.raises(ValueError):
        _admit(profile_document)
    profile_document["disk_bytes"] += 1
    _admit(profile_document)


def test_temporary_worker_facts_follow_concurrency_and_keep_memory_admission():
    profile_document, _signed = _profile()
    profile_document["cpu_millis"] = 4000
    profile_document["source_limits"]["maximum_workers"] = 4
    profile_document["source_limits"]["input"]["shard_count"] = 256
    profile_document["disk_bytes"] = cli._disk_allowance(cli.source_limits(profile_document["source_limits"]))
    original = profile_document["source_limits"]["worker"]["facts"]
    profile_document["source_limits"]["worker"]["facts"] = original | {
        "max_spool_bytes": original["max_spool_bytes"] + 1024
    }
    profile_document["disk_bytes"] += 2 * 4 * 1024 - 1
    with pytest.raises(ValueError):
        _admit(profile_document)
    profile_document["disk_bytes"] += 1
    _admit(profile_document)
    profile_document["memory_bytes"] = 1024**2
    with pytest.raises(ValueError):
        _admit(profile_document)


def test_independent_fact_roots_do_not_share_one_spool_allowance():
    profile_document, _signed = _profile()
    profile_document["disk_bytes"] = cli._disk_allowance(cli.source_limits(profile_document["source_limits"]))
    original = profile_document["source_limits"]["facts"]
    profile_document["source_limits"]["facts"] = original | {"max_spool_bytes": original["max_spool_bytes"] + 1024}
    profile_document["disk_bytes"] += 2 * 20 * 1024 - 1
    with pytest.raises(ValueError):
        _admit(profile_document)
    profile_document["disk_bytes"] += 1
    _admit(profile_document)
