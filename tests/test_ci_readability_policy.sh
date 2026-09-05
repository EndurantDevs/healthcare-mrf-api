#!/usr/bin/env bash
set -euo pipefail

workflow_path=".github/workflows/ci.yml"
caller_path=".github/workflows/trusted-pr-ci.yml"
prepush_path="scripts/ci/prepush"
trigger_block="$(sed -n '/^on:/,/^permissions:/p' "$caller_path")"
quality_job="$(sed -n '/^  python-quality:/,/^  python-tests:/p' "$workflow_path")"
policy_block="$(sed -n '/^run_quality() {/,/^}/p' "$prepush_path")"

test -n "$trigger_block"
test -n "$quality_job"
test -n "$policy_block"
printf '%s\n' "$trigger_block" | grep -Fq -- 'types: [opened, synchronize, reopened]'
printf '%s\n' "$quality_job" | grep -Fq -- 'run: scripts/ci/prepush quality'
printf '%s\n' "$quality_job" | grep -Fq -- 'BASE_SHA:'
printf '%s\n' "$policy_block" | grep -Fq -- 'python scripts/readability_budget.py --base "$BASE_SHA"'
printf '%s\n' "$policy_block" | grep -Fq -- 'python scripts/coverage_reports.py --check'
! grep -Fq -- 'READABILITY_ZERO_GROWTH_APPROVED' "$workflow_path" "$prepush_path"
! grep -Fq -- 'required_reduction_percent' "$prepush_path"
