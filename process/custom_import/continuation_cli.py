# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Run only the remaining phases of an explicitly authorized retained build."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import stat
import sys
from pathlib import Path

from db.connection import db
from process.custom_import.build_continuation import parse_build_continuation_authorization, start_build_continuation
from process.custom_import.build_graph import build_graph
from process.custom_import.build_output import build_output
from process.custom_import.build_source import _set_timeout
from process.custom_import.continuation_projection import continuation_projection_transport
from process.custom_import.execution import finish_execution
from process.custom_import.failure_diagnostics import FailurePhase, failure_context, failure_identifiers, failure_phase
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from process.custom_import.snowflake_operator_cli import _connected_worker_database
from process.custom_import.snowflake_segmented_runner import _activate

_DIRECTORY = Path("/var/run/custom-import-processing")


class _Parser(argparse.ArgumentParser):
    def error(self, _message):
        """Reject arguments without echoing protected input values."""

        self.exit(2, '{"error":"invalid_arguments"}\n')


def _read(path, name, maximum):
    if path != _DIRECTORY / name:
        raise ValueError("processing inputs require fixed paths")
    with path.open("rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError("processing input is not a regular file")
        contents = source.read(maximum + 1)
    if not 0 < len(contents) <= maximum:
        raise ValueError("processing input exceeds its bound")
    return contents


def _inputs(arguments):
    from api.control_execution_evidence import _object

    authorization = parse_build_continuation_authorization(
        json.loads(_read(arguments.authorization_file, "authorization.json", 16384), object_pairs_hook=_object)
    )
    nonce = _read(arguments.nonce_file, "nonce", 32)
    if len(nonce) != 32:
        raise ValueError("invalid processing nonce")
    return authorization, nonce


async def run_continuation(database, authorization, nonce, *, lease_seconds):
    """Never reacquire, replay, or readmit source records during continuation."""

    async with _connected_worker_database(database):
        async with database.session_factory() as session, session.begin():
            await _set_timeout(session, 2000)
            with failure_phase(FailurePhase.RESUME_VALIDATION):
                request = await start_build_continuation(session, authorization, nonce, lease_seconds=lease_seconds)
            failure_identifiers(execution_id=request.execution_id, build_id=request.continuation.build_id)
        try:
            with failure_phase(FailurePhase.GRAPH):
                generation_id = await build_graph(database.session_factory, request, request.continuation.build_id)
            if generation_id is None:
                raise ValueError("retained graph did not create a generation")
            with failure_phase(FailurePhase.OUTPUT):
                async with continuation_projection_transport(authorization, request) as projection_transport:
                    output = await build_output(
                        database.session_factory,
                        request,
                        request.continuation.build_id,
                        projection_transport=projection_transport,
                    )
        except CancellationRequested:
            async with database.session_factory() as session, session.begin():
                await _set_timeout(session, 2000)
                transition = await finish_execution(
                    session,
                    execution_id=request.execution_id,
                    fence=request.fence,
                    token=nonce,
                    terminal_state="canceled",
                    terminal_reason="cancellation_requested",
                    continuation_id=request.continuation.continuation_id,
                )
            return {
                "status": "canceled" if transition.state == "canceled" else "lease_lost",
                "execution_id": request.execution_id,
            }
        except LeaseAuthorityLost:
            return {"status": "lease_lost", "execution_id": request.execution_id}
        with failure_phase(FailurePhase.ACTIVATE):
            publication = output.no_change or await _activate(database.session_factory, request, output)
        return {
            "status": "no_change" if output.no_change else "activated" if publication else "sealed_unpublished",
            "execution_id": request.execution_id,
            "generation_id": output.generation_id,
            "build_id": request.continuation.build_id,
            "continuation_id": request.continuation.continuation_id,
        }


def main(argv=None):
    """Run from fixed protected inputs and emit only a closed terminal receipt."""

    parser = _Parser(description=__doc__)
    parser.add_argument("command", choices=("run",))
    parser.add_argument("--authorization-file", type=Path, required=True)
    parser.add_argument("--nonce-file", type=Path, required=True)
    parser.add_argument("--lease-seconds", type=int, default=3600)
    arguments = parser.parse_args(argv)
    with failure_context() as failure:
        try:
            authorization, nonce = _inputs(arguments)
            receipt = asyncio.run(run_continuation(db, authorization, nonce, lease_seconds=arguments.lease_seconds))
        except Exception as error:
            print(json.dumps({"error": "continuation_failed", "diagnostic": failure.snapshot(error)}, sort_keys=True))
            return 1
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return 0 if receipt["status"] in {"activated", "no_change", "sealed_unpublished"} else 1


if __name__ == "__main__":
    sys.exit(main())
