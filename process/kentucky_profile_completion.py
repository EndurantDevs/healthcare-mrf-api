# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed Kentucky binding for atomic source and managed control completion."""

from process import kentucky_profile_store as store
from process.provider_profile_source_completion import SourceProfileCompletion

IMPORTER = "kentucky-kbml-profile"
_completion = SourceProfileCompletion(store._store, IMPORTER)

_attempt = _completion._attempt
_locked_control_run = _completion._locked_control_run
reconcile_failed_control_runs = _completion.reconcile_failed_control_runs
_is_attempt = _completion._is_attempt
_finish_source = _completion._finish_source
_terminal_progress = _completion._terminal_progress
_commit_control = _completion._commit_control
_reconcile_commit = _completion._reconcile_commit
_shielded_reconciliation = _completion._shielded_reconciliation
_install_committed_result = _completion._install_committed_result
complete_run = _completion.complete_run
