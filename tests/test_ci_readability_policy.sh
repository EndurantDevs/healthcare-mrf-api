#!/usr/bin/env bash
set -euo pipefail

workflow_path=".github/workflows/ci.yml"
policy_block="$(sed -n '/git show "\$BASE_SHA:readability-baseline.json"/,/          else/p' "$workflow_path")"

test -n "$policy_block"
printf '%s\n' "$policy_block" | grep -Fq -- '--required-reduction-percent 0'
if printf '%s\n' "$policy_block" | grep -Fq -- '--required-reduction-percent 1'; then
  echo "normal pull-request readability checks must not require cleanup debt" >&2
  exit 1
fi
