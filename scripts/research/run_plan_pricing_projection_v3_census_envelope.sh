#!/usr/bin/env bash
set -Eeuo pipefail

readonly OPT_IN_ENV=HLTHPRT_PLAN_PRICING_V3_CENSUS_ENVELOPE_RUN
readonly EXPECTED_HOST=ns1033171
readonly KUBECONFIG=/etc/rancher/k3s/k3s.yaml
readonly STATE_ROOT=${HLTHPRT_PLAN_PRICING_V3_CENSUS_STATE_ROOT:-/data/healthporta/plan-pricing-census-envelope}
readonly BUILD_LOCK=/data/healthporta/build/github-dev/locks/dev-build.lock
readonly DEV_NAMESPACE=healthporta-dev
readonly ARC_HOLD_NAMESPACE=arc-runners
readonly ARC_NAMESPACES=(arc-runners arc-import-jobs arc-mrf-jobs)
readonly CLEANUP_RESERVE_SECONDS=300
readonly OPERATION_TIMEOUT_SECONDS=25
readonly ENGINE_SELECTOR='app.kubernetes.io/managed-by in (healthporta-worker-launcher,healthporta-ptg-wave-controller)'

MODE=plan
OWNER_TOKEN=
STATE_DIR=
SOURCE_SHA=
REPO_DIR=
DEADLINE_SECONDS=
CENSUS_JOB=
CENSUS_CONFIGMAP=
CENSUS_RECEIPT=
RUNTIME_ATTESTATION=
EXPECTED_ENVELOPE_SCRIPT_SHA256=
EXPECTED_CHILD_COMMAND_SHA256=
EXPECTED_CHILD_EXECUTABLE_SHA256=
EXPECTED_SOURCE_MANIFEST_SHA256=
EXPECTED_HARNESS_MANIFEST_SHA256=
EXPECTED_SOURCE_OVERLAY_SHA256=
POSTGRESQL_TABLESPACE_PATH=
MINIMUM_HOST_AVAILABLE_MEMORY_BYTES=
MINIMUM_HOST_SWAP_FREE_BYTES=
MINIMUM_POSTGRESQL_TABLESPACE_FREE_BYTES=
DRAIN_DEPLOYMENT=
IMPORT_SCHEDULER_DEPLOYMENT=
IMPORT_NODE_ID=
IMPORT_TOKEN_ENV=
CHILD_COMMAND=()

LOCK_UNIT=
LOCK_INVOCATION_ID=
LOCK_EXEC_START_SHA256=
LOCK_MARKER=
LOCK_OWNERSHIP=
LOCK_STARTED=false
LOCK_START_UNCERTAIN=false
QUOTA_NAME=
QUOTA_UID=
POLICY_NAME=
POLICY_UID=
BINDING_NAME=
BINDING_UID=
DENIAL_MARKER=
PRIOR_DRAIN_MODE=
CHILD_COMMAND_SHA256=
CHILD_EXECUTABLE_SHA256=
CHILD_EXECUTABLE_DESCRIPTOR=
CHILD_CLEANUP_PROOF=
CENSUS_RECEIPT_SHA256=
RUNTIME_ATTESTATION_SHA256=
CAPTURED_RUNTIME_ATTESTATION_SHA256=
RUNTIME_ATTESTATION_VALIDATED=false
ATTESTED_POD_NAME=
ATTESTED_POD_UID=
HOST_AVAILABLE_MEMORY_BYTES=
HOST_SWAP_FREE_BYTES=
POSTGRESQL_TABLESPACE_FREE_BYTES=
CAPACITY_VERIFIED=false
CHILD_EXIT_CODE=
CHILD_PID=
CHILD_LAUNCHED=false
CHILD_KILL_TIMER_PID=
CHILD_DEADLINE_TIMER_PID=
CHILD_DEADLINE_MARKER=
CHILD_TERMINATION_GRACE_SECONDS=30
CHILD_KILL_VERIFY_SECONDS=5
CHILD_SIGNAL_FORWARDED=false
INTERRUPT_EXIT=0
INTERRUPT_SIGNAL=
TIMED_OUT=false
QUOTA_ATTEMPTED=false
QUOTA_CREATE_UNCERTAIN=false
QUOTA_CREATED=false
QUOTA_PROBE_VERIFIED=false
DRAIN_CAPTURED=false
POLICY_ATTEMPTED=false
POLICY_CREATE_UNCERTAIN=false
POLICY_CREATED=false
BINDING_ATTEMPTED=false
BINDING_CREATE_UNCERTAIN=false
BINDING_CREATED=false
PROBE_VERIFIED=false
PRE_CHILD_FENCE_VERIFIED=false
POST_CHILD_FENCE_VERIFIED=false
BINDING_REMOVED=false
POLICY_REMOVED=false
DRAIN_RESTORED=false
QUOTA_REMOVED=false
LOCK_RELEASED=false
CLEANUP_COMPLETE=false
EXIT_TRAP_ACTIVE=false
RECEIPT_FINALIZING=false
START_SECONDS=-1

usage() {
  printf '%s\n' \
    'usage: run_plan_pricing_projection_v3_census_envelope.sh [plan|run]' \
    '  --owner-token TOKEN --state-dir PATH --source-sha SHA --repo-dir PATH' \
    '  --deadline-seconds SECONDS --census-job NAME --census-configmap NAME' \
    '  --census-receipt PATH --runtime-attestation PATH' \
    '  --expected-envelope-script-sha256 SHA256' \
    '  --expected-child-command-sha256 SHA256' \
    '  --expected-child-executable-sha256 SHA256' \
    '  --expected-source-manifest-sha256 SHA256' \
    '  --expected-harness-manifest-sha256 SHA256' \
    '  --expected-source-overlay-sha256 SHA256' \
    '  --postgresql-tablespace-path PATH' \
    '  --minimum-host-available-memory-bytes BYTES' \
    '  --minimum-host-swap-free-bytes BYTES' \
    '  --minimum-postgresql-tablespace-free-bytes BYTES' \
    '  --drain-deployment NAME --import-scheduler-deployment NAME' \
    '  --import-node-id ID --import-token-env NAME' \
    '  -- CENSUS_COMMAND [ARG ...]'
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[plan-pricing-v3-envelope] %s\n' "$*"
}

require_value() {
  [ "$#" -ge 2 ] && [ -n "$2" ] || die "missing value for $1"
}

parse_args() {
  if [ "${1:-}" = plan ] || [ "${1:-}" = run ]; then
    MODE=$1
    shift
  fi
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --owner-token)
        require_value "$@"; OWNER_TOKEN=$2; shift 2 ;;
      --state-dir)
        require_value "$@"; STATE_DIR=$2; shift 2 ;;
      --source-sha)
        require_value "$@"; SOURCE_SHA=$2; shift 2 ;;
      --repo-dir)
        require_value "$@"; REPO_DIR=$2; shift 2 ;;
      --deadline-seconds)
        require_value "$@"; DEADLINE_SECONDS=$2; shift 2 ;;
      --census-job)
        require_value "$@"; CENSUS_JOB=$2; shift 2 ;;
      --census-configmap)
        require_value "$@"; CENSUS_CONFIGMAP=$2; shift 2 ;;
      --census-receipt)
        require_value "$@"; CENSUS_RECEIPT=$2; shift 2 ;;
      --runtime-attestation)
        require_value "$@"; RUNTIME_ATTESTATION=$2; shift 2 ;;
      --expected-envelope-script-sha256)
        require_value "$@"; EXPECTED_ENVELOPE_SCRIPT_SHA256=$2; shift 2 ;;
      --expected-child-command-sha256)
        require_value "$@"; EXPECTED_CHILD_COMMAND_SHA256=$2; shift 2 ;;
      --expected-child-executable-sha256)
        require_value "$@"; EXPECTED_CHILD_EXECUTABLE_SHA256=$2; shift 2 ;;
      --expected-source-manifest-sha256)
        require_value "$@"; EXPECTED_SOURCE_MANIFEST_SHA256=$2; shift 2 ;;
      --expected-harness-manifest-sha256)
        require_value "$@"; EXPECTED_HARNESS_MANIFEST_SHA256=$2; shift 2 ;;
      --expected-source-overlay-sha256)
        require_value "$@"; EXPECTED_SOURCE_OVERLAY_SHA256=$2; shift 2 ;;
      --postgresql-tablespace-path)
        require_value "$@"; POSTGRESQL_TABLESPACE_PATH=$2; shift 2 ;;
      --minimum-host-available-memory-bytes)
        require_value "$@"; MINIMUM_HOST_AVAILABLE_MEMORY_BYTES=$2; shift 2 ;;
      --minimum-host-swap-free-bytes)
        require_value "$@"; MINIMUM_HOST_SWAP_FREE_BYTES=$2; shift 2 ;;
      --minimum-postgresql-tablespace-free-bytes)
        require_value "$@"; MINIMUM_POSTGRESQL_TABLESPACE_FREE_BYTES=$2; shift 2 ;;
      --drain-deployment)
        require_value "$@"; DRAIN_DEPLOYMENT=$2; shift 2 ;;
      --import-scheduler-deployment)
        require_value "$@"; IMPORT_SCHEDULER_DEPLOYMENT=$2; shift 2 ;;
      --import-node-id)
        require_value "$@"; IMPORT_NODE_ID=$2; shift 2 ;;
      --import-token-env)
        require_value "$@"; IMPORT_TOKEN_ENV=$2; shift 2 ;;
      --)
        shift
        CHILD_COMMAND=("$@")
        break ;;
      -h|--help)
        usage
        exit 0 ;;
      *) die "unknown argument: $1" ;;
    esac
  done
}

create_state_directory() {
  python3 - "${STATE_ROOT}" "${STATE_DIR}" <<'PY'
import os
import pathlib
import stat
import sys

root_argument, state_argument = map(pathlib.Path, sys.argv[1:])
if not root_argument.is_absolute() or not state_argument.is_absolute():
    raise SystemExit(1)
if state_argument.parent != root_argument:
    raise SystemExit(1)
flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
root_fd = os.open("/", flags)
try:
    for component in root_argument.parts[1:]:
        next_fd = os.open(component, flags, dir_fd=root_fd)
        os.close(root_fd)
        root_fd = next_fd
    descriptor_path = pathlib.Path(f"/proc/self/fd/{root_fd}")
    if descriptor_path.exists() and pathlib.Path(os.path.realpath(descriptor_path)) != root_argument:
        raise SystemExit(1)
    os.mkdir(state_argument.name, mode=0o700, dir_fd=root_fd)
    state_fd = os.open(state_argument.name, flags, dir_fd=root_fd)
    try:
        state_stat = os.fstat(state_fd)
        path_stat = os.stat(state_argument, follow_symlinks=False)
        if (
            not stat.S_ISDIR(path_stat.st_mode)
            or stat.S_IMODE(path_stat.st_mode) != 0o700
            or (state_stat.st_dev, state_stat.st_ino)
            != (path_stat.st_dev, path_stat.st_ino)
        ):
            raise SystemExit(1)
    finally:
        os.close(state_fd)
finally:
    os.close(root_fd)
PY
}

state_dir_is_confined() {
  python3 - "${STATE_ROOT}" "${STATE_DIR}" <<'PY'
import pathlib
import sys

root, state = map(pathlib.Path, sys.argv[1:])
if not root.is_absolute() or state.parent != root:
    raise SystemExit(1)
PY
}

validate_args() {
  local state_leaf
  [[ "${OWNER_TOKEN}" =~ ^[a-z0-9][a-z0-9-]{7,31}$ ]] \
    || die "owner token must be 8-32 lowercase DNS-label characters"
  [[ "${SOURCE_SHA}" =~ ^[0-9a-f]{40}$ ]] \
    || die "exact reviewed source SHA is required"
  [[ "${DEADLINE_SECONDS}" =~ ^[0-9]+$ ]] \
    && [ "${DEADLINE_SECONDS}" -ge 900 ] \
    && [ "${DEADLINE_SECONDS}" -le 86400 ] \
    || die "deadline must be 900-86400 seconds"
  [[ "${REPO_DIR}" = /* ]] || die "repo directory must be absolute"
  state_leaf=${STATE_DIR#"${STATE_ROOT}/"}
  [[ "${STATE_DIR}" = "${STATE_ROOT}/"* ]] \
    && [ -n "${state_leaf}" ] \
    && [[ "${state_leaf}" != */* ]] \
    || die "state directory must be a child of ${STATE_ROOT}"
  [[ "${CENSUS_JOB}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
    && [ "${#CENSUS_JOB}" -le 63 ] \
    || die "exact census Job name is invalid"
  [[ "${CENSUS_CONFIGMAP}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
    && [ "${#CENSUS_CONFIGMAP}" -le 63 ] \
    || die "exact census ConfigMap name is invalid"
  [ "${CENSUS_RECEIPT}" = "${STATE_DIR}/census-receipt.json" ] \
    || die "exact census receipt must be the task state receipt"
  [ "${RUNTIME_ATTESTATION}" = "${STATE_DIR}/runtime-attestation.json" ] \
    || die "exact runtime attestation must be the task state attestation"
  [[ "${EXPECTED_ENVELOPE_SCRIPT_SHA256}" =~ ^[0-9a-f]{64}$ ]] \
    || die "reviewed envelope script SHA-256 is invalid"
  [[ "${EXPECTED_CHILD_COMMAND_SHA256}" =~ ^[0-9a-f]{64}$ ]] \
    || die "reviewed child command SHA-256 is invalid"
  [[ "${EXPECTED_CHILD_EXECUTABLE_SHA256}" =~ ^[0-9a-f]{64}$ ]] \
    || die "reviewed child executable SHA-256 is invalid"
  [[ "${EXPECTED_SOURCE_MANIFEST_SHA256}" =~ ^[0-9a-f]{64}$ ]] \
    || die "reviewed source manifest SHA-256 is invalid"
  [[ "${EXPECTED_HARNESS_MANIFEST_SHA256}" =~ ^[0-9a-f]{64}$ ]] \
    || die "reviewed harness manifest SHA-256 is invalid"
  [[ "${EXPECTED_SOURCE_OVERLAY_SHA256}" =~ ^[0-9a-f]{64}$ ]] \
    || die "reviewed source overlay SHA-256 is invalid"
  [[ "${POSTGRESQL_TABLESPACE_PATH}" = /* ]] \
    || die "PostgreSQL tablespace path must be absolute"
  [[ "${MINIMUM_HOST_AVAILABLE_MEMORY_BYTES}" =~ ^[0-9]+$ ]] \
    && [ "${MINIMUM_HOST_AVAILABLE_MEMORY_BYTES}" -gt 0 ] \
    || die "minimum host available memory must be positive bytes"
  [[ "${MINIMUM_HOST_SWAP_FREE_BYTES}" =~ ^[0-9]+$ ]] \
    && [ "${MINIMUM_HOST_SWAP_FREE_BYTES}" -gt 0 ] \
    || die "minimum host swap must be positive bytes"
  [[ "${MINIMUM_POSTGRESQL_TABLESPACE_FREE_BYTES}" =~ ^[0-9]+$ ]] \
    && [ "${MINIMUM_POSTGRESQL_TABLESPACE_FREE_BYTES}" -gt 0 ] \
    || die "minimum PostgreSQL tablespace free space must be positive bytes"
  [[ "${DRAIN_DEPLOYMENT}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
    && [ "${#DRAIN_DEPLOYMENT}" -le 63 ] \
    || die "import API deployment name is invalid"
  [[ "${IMPORT_SCHEDULER_DEPLOYMENT}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
    && [ "${#IMPORT_SCHEDULER_DEPLOYMENT}" -le 63 ] \
    || die "import scheduler deployment name is invalid"
  [[ "${IMPORT_NODE_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]] \
    || die "import node identity is invalid"
  [[ "${IMPORT_TOKEN_ENV}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] \
    || die "import token environment name is invalid"
  [ "${#CHILD_COMMAND[@]}" -gt 0 ] || die "foreground census command is required"

  LOCK_UNIT="hp-pv3-census-${OWNER_TOKEN}-lock.service"
  LOCK_MARKER="${STATE_DIR}/build-lock.acquired"
  LOCK_OWNERSHIP="${STATE_DIR}/build-lock.ownership"
  CHILD_CLEANUP_PROOF="${STATE_DIR}/packet-cleanup-complete"
  QUOTA_NAME="hp-pv3-census-${OWNER_TOKEN}"
  POLICY_NAME="hp-pv3-census-${OWNER_TOKEN}.healthporta.com"
  BINDING_NAME="${POLICY_NAME}"
  DENIAL_MARKER="hp-pv3-census-deny-${OWNER_TOKEN}"
}

quota_manifest() {
  printf '%s\n' \
    'apiVersion: v1' \
    'kind: ResourceQuota' \
    'metadata:' \
    "  name: ${QUOTA_NAME}" \
    "  namespace: ${ARC_HOLD_NAMESPACE}" \
    '  labels:' \
    "    healthporta.com/task-owner: ${OWNER_TOKEN}" \
    'spec:' \
    '  hard:' \
    '    pods: "0"'
}

quota_probe_manifest() {
  printf '%s\n' \
    'apiVersion: v1' \
    'kind: Pod' \
    'metadata:' \
    "  name: hp-pv3-census-quota-probe-${OWNER_TOKEN}" \
    "  namespace: ${ARC_HOLD_NAMESPACE}" \
    'spec:' \
    '  automountServiceAccountToken: false' \
    '  restartPolicy: Never' \
    '  securityContext:' \
    '    runAsNonRoot: true' \
    '    seccompProfile: {type: RuntimeDefault}' \
    '  containers:' \
    '    - name: probe' \
    '      image: busybox:1.36' \
    '      command: ["true"]' \
    '      securityContext:' \
    '        allowPrivilegeEscalation: false' \
    '        capabilities: {drop: ["ALL"]}' \
    '        readOnlyRootFilesystem: true' \
    '        runAsUser: 65532'
}

policy_manifest() {
  printf '%s\n' \
    'apiVersion: admissionregistration.k8s.io/v1' \
    'kind: ValidatingAdmissionPolicy' \
    'metadata:' \
    "  name: ${POLICY_NAME}" \
    '  labels:' \
    "    healthporta.com/task-owner: ${OWNER_TOKEN}" \
    'spec:' \
    '  failurePolicy: Fail' \
    '  matchConstraints:' \
    '    matchPolicy: Exact' \
    '    resourceRules:' \
    '      - apiGroups: ["batch"]' \
    '        apiVersions: ["v1"]' \
    '        operations: ["CREATE"]' \
    '        resources: ["jobs"]' \
    '        scope: Namespaced' \
    '  matchConditions:' \
    '    - name: exact-engine-worker-launcher' \
    "      expression: request.namespace == '${DEV_NAMESPACE}' && request.userInfo.username == 'system:serviceaccount:${DEV_NAMESPACE}:engine-worker-launcher'" \
    '  validations:' \
    '    - expression: "false"' \
    "      message: ${DENIAL_MARKER}" \
    '      reason: Forbidden'
}

binding_manifest() {
  printf '%s\n' \
    'apiVersion: admissionregistration.k8s.io/v1' \
    'kind: ValidatingAdmissionPolicyBinding' \
    'metadata:' \
    "  name: ${BINDING_NAME}" \
    '  labels:' \
    "    healthporta.com/task-owner: ${OWNER_TOKEN}" \
    'spec:' \
    "  policyName: ${POLICY_NAME}" \
    '  validationActions: [Deny]' \
    '  matchResources:' \
    '    matchPolicy: Exact' \
    '    namespaceSelector:' \
    '      matchLabels:' \
    "        kubernetes.io/metadata.name: ${DEV_NAMESPACE}"
}

probe_manifest() {
  printf '%s\n' \
    'apiVersion: batch/v1' \
    'kind: Job' \
    'metadata:' \
    "  name: hp-pv3-census-fence-probe-${OWNER_TOKEN}" \
    "  namespace: ${DEV_NAMESPACE}" \
    'spec:' \
    '  template:' \
    '    metadata:' \
    '      labels:' \
    '        app.kubernetes.io/name: healthporta-import-worker' \
    '        app.kubernetes.io/managed-by: healthporta-worker-launcher' \
    '    spec:' \
    '      serviceAccountName: import-worker' \
    '      restartPolicy: Never' \
    '      containers:' \
    '        - name: worker' \
    '          image: ghcr.io/endurantdevs/healthcare-mrf-api-dev:dev-envelope-probe' \
    '          command: ["true"]'
}

render_plan() {
  printf '%s\n' \
    'mode: plan' \
    "reviewed_source_sha: ${SOURCE_SHA}" \
    "owner_token: ${OWNER_TOKEN}" \
    'hold_scope: temporary global DEV build, ARC, and import hold' \
    'runtime_authority: separate direct authority is required' \
    'postgresql_boundary: Kubernetes QoS does not reserve or cap off-node PostgreSQL.' \
    'phases:' \
    '  - validate exact DEV/source/resource absence' \
    '  - acquire bounded dev-build flock' \
    '  - create UID-bound ARC pods=0 quota, prove admission, and drain ARC naturally' \
    '  - capture and set the exact import-node drain state' \
    '  - create UID-bound engine-worker deny policy and prove denial marker' \
    '  - require scheduler=0 and three stable zero-work samples' \
    '  - require reviewed host memory, swap, and PostgreSQL tablespace headroom' \
    '  - run foreground census command under remaining deadline' \
    '  - restore drain, remove binding and policy, remove quota, release flock'
  printf '%s\n' '--- quota ---'
  quota_manifest
  printf '%s\n' '--- quota server-dry-run probe ---'
  quota_probe_manifest
  printf '%s\n' '--- policy ---'
  policy_manifest
  printf '%s\n' '--- binding ---'
  binding_manifest
  printf '%s\n' '--- server-dry-run probe ---'
  probe_manifest
}

operation_timeout() {
  local remaining=${OPERATION_TIMEOUT_SECONDS}
  if [ "${START_SECONDS}" -ge 0 ]; then
    remaining=$((DEADLINE_SECONDS - (SECONDS - START_SECONDS)))
    [ "${remaining}" -gt 0 ] || return 124
    [ "${remaining}" -le "${OPERATION_TIMEOUT_SECONDS}" ] \
      || remaining=${OPERATION_TIMEOUT_SECONDS}
  fi
  printf '%s\n' "${remaining}"
}

run_bounded() {
  local limit
  limit=$(operation_timeout) || return $?
  timeout --foreground --signal=TERM --kill-after=2s "${limit}s" "$@"
}

kctl() {
  local limit
  limit=$(operation_timeout) || return $?
  kctl_with_limit "${limit}" "$@"
}

kctl_with_limit() {
  local limit=$1
  shift
  [ "${limit}" -gt 0 ] || return 124
  [ "${limit}" -le "${OPERATION_TIMEOUT_SECONDS}" ] \
    || limit=${OPERATION_TIMEOUT_SECONDS}
  timeout --foreground --signal=TERM --kill-after=2s "${limit}s" \
    k3s kubectl --kubeconfig="${KUBECONFIG}" \
    --request-timeout="${limit}s" "$@"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

resource_is_absent() {
  local limit
  limit=$(operation_timeout) || return $?
  resource_is_absent_with_limit "${limit}" "$@"
}

resource_is_absent_with_limit() {
  local limit=$1 kind=$2 name=$3 namespace=${4:-} observed
  if [ -n "${namespace}" ]; then
    observed=$(kctl_with_limit "${limit}" -n "${namespace}" \
      get "${kind}" "${name}" \
      --ignore-not-found -o name) || return 1
  else
    observed=$(kctl_with_limit "${limit}" get "${kind}" "${name}" \
      --ignore-not-found -o name) || return 1
  fi
  [ -z "${observed}" ]
}

verify_absent() {
  local kind=$1 name=$2 namespace=${3:-}
  resource_is_absent "${kind}" "${name}" "${namespace}" \
    || die "present or unreadable ${kind}/${name} blocks the envelope"
}

verify_seed_absent() {
  verify_absent job seed-import-nodes "${DEV_NAMESPACE}"
}

seed_is_absent() {
  resource_is_absent job seed-import-nodes "${DEV_NAMESPACE}"
}

unit_load_state() {
  local state
  state=$(run_bounded systemctl show "${LOCK_UNIT}" \
    --property=LoadState --value 2>/dev/null) || return $?
  [ -n "${state}" ] || return 1
  printf '%s\n' "${state}"
}

verify_admission_v1() {
  kctl get --raw /version | python3 -c '
import json, re, sys

version = json.load(sys.stdin)
major = version.get("major")
minor = version.get("minor")
if (not isinstance(major, str) or not re.fullmatch(r"[0-9]+", major)
    or not isinstance(minor, str) or not re.fullmatch(r"[0-9]+", minor)
    or (int(major), int(minor)) < (1, 30)):
    raise SystemExit("Kubernetes server must be an exact version at least 1.30")
'
  kctl get --raw /apis/admissionregistration.k8s.io/v1 | python3 -c '
import json, sys

required = {
    "validatingadmissionpolicies",
    "validatingadmissionpolicybindings",
}
available = {item.get("name") for item in json.load(sys.stdin).get("resources", [])}
if not required <= available:
    raise SystemExit("required admissionregistration.k8s.io/v1 resources are absent")
'
}

verify_reviewed_hashes() {
  local child_executable observed_executable_sha observed_script_sha observed_child_sha
  read -r observed_script_sha _ < <(sha256sum "$0")
  [ "${observed_script_sha}" = "${EXPECTED_ENVELOPE_SCRIPT_SHA256}" ] \
    || die "reviewed envelope script identity changed"
  read -r observed_child_sha _ \
    < <(printf '%s\0' "${CHILD_COMMAND[@]}" | sha256sum)
  [ "${observed_child_sha}" = "${EXPECTED_CHILD_COMMAND_SHA256}" ] \
    || die "reviewed child command identity changed"
  child_executable=${CHILD_COMMAND[0]}
  [[ "${child_executable}" = /* ]] \
    && [ -f "${child_executable}" ] \
    && [ ! -L "${child_executable}" ] \
    && [ -x "${child_executable}" ] \
    || die "reviewed child executable is not an absolute regular executable"
  read -r observed_executable_sha _ < <(sha256sum "${child_executable}")
  [ "${observed_executable_sha}" = "${EXPECTED_CHILD_EXECUTABLE_SHA256}" ] \
    || die "reviewed child executable identity changed"
  CHILD_COMMAND_SHA256=${observed_child_sha}
  CHILD_EXECUTABLE_SHA256=${observed_executable_sha}
}

open_reviewed_child_descriptor() {
  local copy_path="${STATE_DIR}/reviewed-child-executable"
  local observed_executable_sha
  [ ! -e "${copy_path}" ] && [ ! -L "${copy_path}" ] || return 1
  run_bounded python3 - reviewed-child-copy "${CHILD_COMMAND[0]}" \
    "${copy_path}" "${EXPECTED_CHILD_EXECUTABLE_SHA256}" <<'PY' || return 1
import hashlib
import os
import stat
import sys

_, source_path, copy_path, expected_sha256 = sys.argv[1:]
maximum_bytes = 16 * 1024 * 1024
source_fd = os.open(
    source_path,
    os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW,
)
try:
    source_stat = os.fstat(source_fd)
    if not stat.S_ISREG(source_stat.st_mode) or not 0 < source_stat.st_size <= maximum_bytes:
        raise SystemExit("reviewed child executable is not a bounded regular file")
    chunks, total = [], 0
    while chunk := os.read(source_fd, min(1024 * 1024, maximum_bytes + 1 - total)):
        chunks.append(chunk)
        total += len(chunk)
        if total > maximum_bytes:
            raise SystemExit("reviewed child executable exceeds the byte limit")
finally:
    os.close(source_fd)
payload = b"".join(chunks)
if len(payload) != source_stat.st_size or hashlib.sha256(payload).hexdigest() != expected_sha256:
    raise SystemExit("reviewed child executable changed during bounded copy")
copy_fd = os.open(
    copy_path,
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
    0o500,
)
try:
    with os.fdopen(copy_fd, "wb", closefd=False) as target:
        target.write(payload)
        target.flush()
        os.fsync(copy_fd)
finally:
    os.close(copy_fd)
PY
  exec 9<"${copy_path}" || return 1
  rm -f -- "${copy_path}" || return 1
  if [ -r /proc/self/fd/9 ]; then
    CHILD_EXECUTABLE_DESCRIPTOR=/proc/self/fd/9
  else
    CHILD_EXECUTABLE_DESCRIPTOR=/dev/fd/9
  fi
  read -r observed_executable_sha _ \
    < <(sha256sum "${CHILD_EXECUTABLE_DESCRIPTOR}") || return 1
  if [ "${observed_executable_sha}" != "${EXPECTED_CHILD_EXECUTABLE_SHA256}" ]; then
    exec 9<&-
    return 1
  fi
  CHILD_EXECUTABLE_SHA256=${observed_executable_sha}
}

verify_source_and_target() {
  local host live_head status nodes context
  host=$(hostname -s)
  [ "${host}" = "${EXPECTED_HOST}" ] \
    || die "refusing non-DEV host"
  context=$(kctl config current-context)
  [ "${context}" = default ] || die "unexpected Kubernetes context"
  nodes=$(kctl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  [ "${nodes}" = "${EXPECTED_HOST}" ] || die "unexpected Kubernetes node identity"
  live_head=$(git -c "safe.directory=${REPO_DIR}" -C "${REPO_DIR}" rev-parse HEAD)
  [ "${live_head}" = "${SOURCE_SHA}" ] || die "reviewed source SHA changed"
  status=$(git --no-optional-locks -c "safe.directory=${REPO_DIR}" \
    -C "${REPO_DIR}" status --porcelain --untracked-files=all)
  [ -z "${status}" ] || die "reviewed source checkout is not clean"
  [ ! -e "${STATE_DIR}" ] || die "state directory already exists"
  [ -d "${STATE_ROOT}" ] || die "state root is unavailable"
  verify_admission_v1
  verify_absent resourcequota "${QUOTA_NAME}" "${ARC_HOLD_NAMESPACE}"
  verify_absent validatingadmissionpolicy "${POLICY_NAME}"
  verify_absent validatingadmissionpolicybinding "${BINDING_NAME}"
  verify_absent job "${CENSUS_JOB}" "${DEV_NAMESPACE}"
  verify_absent configmap "${CENSUS_CONFIGMAP}" "${DEV_NAMESPACE}"
  census_inventory_absent \
    || die "preexisting labeled census resource blocks the envelope"
  verify_seed_absent
  [ "$(unit_load_state)" = not-found ] \
    || die "owner lock unit already exists"
}

require_lock_held() {
  local active exec_start_sha invocation marker ownership
  [ -n "${LOCK_INVOCATION_ID}" ] \
    && [ -n "${LOCK_EXEC_START_SHA256}" ] || return 1
  active=$(run_bounded systemctl is-active "${LOCK_UNIT}" 2>/dev/null) \
    || return 1
  [ "${active}" = active ] || return 1
  invocation=$(run_bounded systemctl show "${LOCK_UNIT}" \
    --property=InvocationID --value) || return 1
  [ "${invocation}" = "${LOCK_INVOCATION_ID}" ] || return 1
  exec_start_sha=$(lock_exec_start_sha256) || return 1
  [ "${exec_start_sha}" = "${LOCK_EXEC_START_SHA256}" ] || return 1
  [ -f "${LOCK_MARKER}" ] || return 1
  marker=$(<"${LOCK_MARKER}")
  [ "${marker}" = "${LOCK_INVOCATION_ID}:${OWNER_TOKEN}" ] || return 1
  [ -f "${LOCK_OWNERSHIP}" ] && [ ! -L "${LOCK_OWNERSHIP}" ] || return 1
  ownership=$(<"${LOCK_OWNERSHIP}")
  [ "${ownership}" \
    = "${LOCK_INVOCATION_ID}:${OWNER_TOKEN}:${LOCK_EXEC_START_SHA256}" ]
}

lock_exec_start_sha256() {
  local exec_start observed_sha
  exec_start=$(run_bounded systemctl show "${LOCK_UNIT}" \
    --property=ExecStart --value) || return 1
  [ -n "${exec_start}" ] || return 1
  read -r observed_sha _ < <(printf '%s' "${exec_start}" | sha256sum) \
    || return 1
  [[ "${observed_sha}" =~ ^[0-9a-f]{64}$ ]] || return 1
  printf '%s\n' "${observed_sha}"
}

persist_lock_ownership() {
  local invocation=$1 exec_start_sha=$2
  python3 - "${LOCK_OWNERSHIP}" "${invocation}" "${OWNER_TOKEN}" \
    "${exec_start_sha}" <<'PY'
import os
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
payload = (":".join(sys.argv[2:]) + "\n").encode()
descriptor = os.open(
    path,
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
    0o600,
)
try:
    view = memoryview(payload)
    while view:
        view = view[os.write(descriptor, view) :]
    os.fsync(descriptor)
finally:
    os.close(descriptor)
directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY
}

start_lock() {
  local active create_exit=0 exec_start_sha invocation marker
  LOCK_STARTED=true
  LOCK_START_UNCERTAIN=true
  set +e
  run_bounded systemd-run --quiet --unit="${LOCK_UNIT}" --property=Type=exec \
    --property=Restart=no \
    --property="RuntimeMaxSec=$((DEADLINE_SECONDS + CLEANUP_RESERVE_SECONDS))s" \
    /usr/bin/flock -n "${BUILD_LOCK}" /bin/sh -c \
    'umask 077; printf "%s:%s\n" "$INVOCATION_ID" "$1" > "$2"; exec /usr/bin/sleep infinity' \
    sh "${OWNER_TOKEN}" "${LOCK_MARKER}"
  create_exit=$?
  set -e
  [ "${create_exit}" -eq 0 ] \
    || die "build lock creation returned an ambiguous failure"
  for _ in {1..20}; do
    if [ -f "${LOCK_MARKER}" ]; then
      marker=$(<"${LOCK_MARKER}")
      invocation=${marker%%:*}
      active=$(run_bounded systemctl is-active "${LOCK_UNIT}" 2>/dev/null) \
        || active=
      if [ -n "${invocation}" ] \
          && [ "${marker}" = "${invocation}:${OWNER_TOKEN}" ] \
          && [ "${active}" = active ] \
          && [ "$(run_bounded systemctl show "${LOCK_UNIT}" \
            --property=InvocationID --value)" = "${invocation}" ]; then
        exec_start_sha=$(lock_exec_start_sha256) \
          || die "build lock ExecStart identity is unavailable"
        persist_lock_ownership "${invocation}" "${exec_start_sha}" \
          || die "build lock ownership could not be recorded"
        LOCK_INVOCATION_ID=${invocation}
        LOCK_EXEC_START_SHA256=${exec_start_sha}
        require_lock_held || die "build lock ownership changed after recording"
        LOCK_START_UNCERTAIN=false
        return 0
      fi
    fi
    sleep 0.25
  done
  die "build lock was not acquired"
}

quota_identity() {
  kctl -n "${ARC_HOLD_NAMESPACE}" get resourcequota "${QUOTA_NAME}" -o json | \
    python3 -c '
import json, sys
item = json.load(sys.stdin)
meta = item.get("metadata", {})
if (meta.get("name") != sys.argv[1]
    or meta.get("namespace") != sys.argv[2]
    or meta.get("labels", {}).get("healthporta.com/task-owner") != sys.argv[3]
    or item.get("spec", {}) != {"hard": {"pods": "0"}}):
    raise SystemExit("quota identity changed")
print(meta.get("uid") or "")
' "${QUOTA_NAME}" "${ARC_HOLD_NAMESPACE}" "${OWNER_TOKEN}"
}

create_quota() {
  local create_exit=0 observed_uid
  QUOTA_ATTEMPTED=true
  QUOTA_CREATE_UNCERTAIN=true
  set +e
  quota_manifest | kctl create -f - >/dev/null
  create_exit=$?
  set -e
  observed_uid=$(kctl -n "${ARC_HOLD_NAMESPACE}" get resourcequota \
    "${QUOTA_NAME}" --ignore-not-found -o jsonpath='{.metadata.uid}')
  if [ -z "${observed_uid}" ]; then
    [ "${create_exit}" -eq 0 ] && die "quota create returned without a resource"
    die "quota create failed before a resource appeared"
  fi
  QUOTA_UID=$(quota_identity)
  [ -n "${QUOTA_UID}" ] || die "quota UID is unavailable"
  QUOTA_CREATED=true
  QUOTA_CREATE_UNCERTAIN=false
  [ "${create_exit}" -eq 0 ] \
    || die "quota appeared after its create command failed"
}

prove_quota_admission() {
  local exit_code=0 probe_name remaining
  probe_name="hp-pv3-census-quota-probe-${OWNER_TOKEN}"
  while :; do
    remaining=$(seconds_before_cleanup)
    [ "${remaining}" -gt 0 ] || break
    check_interrupted
    set +e
    quota_probe_manifest | kctl_with_limit "${remaining}" \
      -n "${ARC_HOLD_NAMESPACE}" create --dry-run=server -f - \
      >"${STATE_DIR}/quota-probe.stdout" \
      2>"${STATE_DIR}/quota-probe.stderr"
    exit_code=$?
    set -e
    remaining=$(seconds_before_cleanup)
    [ "${remaining}" -gt 0 ] || break
    resource_is_absent_with_limit "${remaining}" pod "${probe_name}" \
      "${ARC_HOLD_NAMESPACE}" \
      || die "quota probe Pod absence is unreadable"
    if [ "${exit_code}" -ne 0 ] \
        && [[ "$(<"${STATE_DIR}/quota-probe.stderr")" \
          = *"exceeded quota: ${QUOTA_NAME}"* ]]; then
      QUOTA_PROBE_VERIFIED=true
      return 0
    fi
    remaining=$(seconds_before_cleanup)
    [ "${remaining}" -gt 0 ] || break
    sleep 0.25
  done
  die "ARC quota admission did not prove the exact owner quota"
}

active_arc_count() {
  kctl get pods -A -o json | python3 -c '
import json, sys
runner_namespace = sys.argv[1]
workload_namespaces = set(sys.argv[2:])
print(sum(
    1 for pod in json.load(sys.stdin).get("items", [])
    if pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"}
    and (
        pod.get("metadata", {}).get("namespace") in workload_namespaces
        or (
            pod.get("metadata", {}).get("namespace") == runner_namespace
            and str(pod.get("metadata", {}).get("labels", {}).get(
                "actions-ephemeral-runner", ""
            )).lower() == "true"
        )
    )
))
' "${ARC_NAMESPACES[@]}"
}

seconds_before_cleanup() {
  printf '%s\n' "$((DEADLINE_SECONDS - (SECONDS - START_SECONDS) - CLEANUP_RESERVE_SECONDS))"
}

check_interrupted() {
  [ "${INTERRUPT_EXIT}" -eq 0 ] || exit "${INTERRUPT_EXIT}"
}

wait_for_arc_idle() {
  local count remaining stable=0
  for namespace in "${ARC_NAMESPACES[@]}"; do
    kctl get namespace "${namespace}" >/dev/null
  done
  while :; do
    check_interrupted
    count=$(active_arc_count)
    if [ "${count}" -eq 0 ]; then
      stable=$((stable + 1))
      [ "${stable}" -eq 3 ] && return 0
    else
      stable=0
    fi
    remaining=$(seconds_before_cleanup)
    [ "${remaining}" -gt 5 ] || die "ARC did not drain before cleanup reserve"
    sleep 5
  done
}

node_drain_mode() {
  local desired=$1
  kctl -n "${DEV_NAMESPACE}" exec "deployment/${DRAIN_DEPLOYMENT}" -- \
    python -c '
import json, os, sys, urllib.request
from urllib.parse import quote

desired = sys.argv[1]
node_id = sys.argv[2]
token = os.environ[sys.argv[3]]
data = None
method = "GET"
if desired in {"true", "false"}:
    method = "PATCH"
    data = json.dumps({"drain_mode": desired == "true"}).encode()
request = urllib.request.Request(
    "http://127.0.0.1:8095/v1/nodes/" + quote(node_id, safe=""),
    data=data,
    method=method,
    headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
)
with urllib.request.urlopen(request, timeout=15) as response:
    payload = json.load(response)
mode = payload.get("drain_mode")
if payload.get("node_id") != node_id or type(mode) is not bool:
    raise SystemExit("import API returned an invalid node identity")
if desired in {"true", "false"} and mode is not (desired == "true"):
    raise SystemExit("import API drain update did not persist")
print(str(mode).lower())
' "${desired}" "${IMPORT_NODE_ID}" "${IMPORT_TOKEN_ENV}"
}

set_import_drain() {
  verify_seed_absent
  PRIOR_DRAIN_MODE=$(node_drain_mode read)
  [[ "${PRIOR_DRAIN_MODE}" = true || "${PRIOR_DRAIN_MODE}" = false ]] \
    || die "prior import drain state is invalid"
  DRAIN_CAPTURED=true
  if [ "${PRIOR_DRAIN_MODE}" = false ]; then
    [ "$(node_drain_mode true)" = true ] || die "import drain did not enable"
  fi
  verify_seed_absent
  [ "$(node_drain_mode read)" = true ] || die "import drain is not active"
}

policy_identity() {
  kctl get validatingadmissionpolicy "${POLICY_NAME}" -o json | python3 -c '
import json, sys
item = json.load(sys.stdin)
meta, spec = item.get("metadata", {}), item.get("spec", {})
expected_spec = {
    "failurePolicy": "Fail",
    "matchConstraints": {
        "matchPolicy": "Exact",
        "resourceRules": [{
            "apiGroups": ["batch"],
            "apiVersions": ["v1"],
            "operations": ["CREATE"],
            "resources": ["jobs"],
            "scope": "Namespaced",
        }],
    },
    "matchConditions": [{
        "name": "exact-engine-worker-launcher",
        "expression": sys.argv[4],
    }],
    "validations": [{
        "expression": "false",
        "message": sys.argv[3],
        "reason": "Forbidden",
    }],
}
if (meta.get("name") != sys.argv[1]
    or meta.get("labels", {}).get("healthporta.com/task-owner") != sys.argv[2]
    or spec != expected_spec):
    raise SystemExit("policy identity changed")
print(meta.get("uid") or "")
' "${POLICY_NAME}" "${OWNER_TOKEN}" "${DENIAL_MARKER}" \
  "request.namespace == '${DEV_NAMESPACE}' && request.userInfo.username == 'system:serviceaccount:${DEV_NAMESPACE}:engine-worker-launcher'"
}

binding_identity() {
  kctl get validatingadmissionpolicybinding "${BINDING_NAME}" -o json | \
    python3 -c '
import json, sys
item = json.load(sys.stdin)
meta, spec = item.get("metadata", {}), item.get("spec", {})
expected_spec = {
    "policyName": sys.argv[3],
    "validationActions": ["Deny"],
    "matchResources": {
        "matchPolicy": "Exact",
        "namespaceSelector": {"matchLabels": {
            "kubernetes.io/metadata.name": sys.argv[4]
        }},
    },
}
if (meta.get("name") != sys.argv[1]
    or meta.get("labels", {}).get("healthporta.com/task-owner") != sys.argv[2]
    or spec != expected_spec):
    raise SystemExit("binding identity changed")
print(meta.get("uid") or "")
' "${BINDING_NAME}" "${OWNER_TOKEN}" "${POLICY_NAME}" "${DEV_NAMESPACE}"
}

create_worker_fence() {
  local create_exit=0 observed_uid
  POLICY_ATTEMPTED=true
  POLICY_CREATE_UNCERTAIN=true
  set +e
  policy_manifest | kctl create -f - >/dev/null
  create_exit=$?
  set -e
  observed_uid=$(kctl get validatingadmissionpolicy "${POLICY_NAME}" \
    --ignore-not-found -o jsonpath='{.metadata.uid}')
  if [ -z "${observed_uid}" ]; then
    [ "${create_exit}" -eq 0 ] && die "policy create returned without a resource"
    die "policy create failed before a resource appeared"
  fi
  POLICY_UID=$(policy_identity)
  [ -n "${POLICY_UID}" ] || die "policy UID is unavailable"
  POLICY_CREATED=true
  POLICY_CREATE_UNCERTAIN=false
  [ "${create_exit}" -eq 0 ] \
    || die "policy appeared after its create command failed"

  BINDING_ATTEMPTED=true
  BINDING_CREATE_UNCERTAIN=true
  set +e
  binding_manifest | kctl create -f - >/dev/null
  create_exit=$?
  set -e
  observed_uid=$(kctl get validatingadmissionpolicybinding "${BINDING_NAME}" \
    --ignore-not-found -o jsonpath='{.metadata.uid}')
  if [ -z "${observed_uid}" ]; then
    [ "${create_exit}" -eq 0 ] && die "binding create returned without a resource"
    die "binding create failed before a resource appeared"
  fi
  BINDING_UID=$(binding_identity)
  [ -n "${BINDING_UID}" ] || die "binding UID is unavailable"
  BINDING_CREATED=true
  BINDING_CREATE_UNCERTAIN=false
  [ "${create_exit}" -eq 0 ] \
    || die "binding appeared after its create command failed"

  if probe_manifest | kctl \
      --as="system:serviceaccount:${DEV_NAMESPACE}:engine-worker-launcher" \
      -n "${DEV_NAMESPACE}" create --dry-run=server -f - \
      >"${STATE_DIR}/probe.stdout" 2>"${STATE_DIR}/probe.stderr"; then
    die "engine-worker dry-run unexpectedly succeeded"
  fi
  [[ "$(<"${STATE_DIR}/probe.stderr")" = *"${DENIAL_MARKER}"* ]] \
    || die "engine-worker dry-run did not prove the owner denial marker"
  PROBE_VERIFIED=true
  verify_absent job "hp-pv3-census-fence-probe-${OWNER_TOKEN}" "${DEV_NAMESPACE}"
}

active_engine_count() {
  kctl -n "${DEV_NAMESPACE}" get jobs,pods -l "${ENGINE_SELECTOR}" -o json | \
    python3 -c '
import json, sys
count = 0
for item in json.load(sys.stdin).get("items", []):
    status = item.get("status", {})
    if item.get("kind") == "Job":
        terminal = any(
            condition.get("status") == "True"
            and condition.get("type") in {"Complete", "Failed"}
            for condition in status.get("conditions", [])
        )
    else:
        terminal = status.get("phase") in {"Succeeded", "Failed"}
    count += not terminal
print(count)
'
}

verify_stable_zero_work() {
  local scheduler count check
  scheduler=$(kctl -n "${DEV_NAMESPACE}" get deployment \
    "${IMPORT_SCHEDULER_DEPLOYMENT}" -o jsonpath='{.spec.replicas}')
  [ "${scheduler}" = 0 ] || die "import scheduler is not held at zero"
  for check in 1 2 3; do
    check_interrupted
    count=$(active_engine_count)
    [ "${count}" -eq 0 ] || die "active engine work blocks the census"
    verify_absent job "${CENSUS_JOB}" "${DEV_NAMESPACE}"
    verify_absent configmap "${CENSUS_CONFIGMAP}" "${DEV_NAMESPACE}"
    [ "${check}" -eq 3 ] || sleep 5
  done
}

census_resources_absent() {
  resource_is_absent job "${CENSUS_JOB}" "${DEV_NAMESPACE}" \
    && resource_is_absent configmap "${CENSUS_CONFIGMAP}" "${DEV_NAMESPACE}" \
    && attested_pod_is_absent \
    && census_inventory_absent
}

child_cleanup_is_complete() {
  local observed
  [ -f "${CHILD_CLEANUP_PROOF}" ] \
    && [ ! -L "${CHILD_CLEANUP_PROOF}" ] || return 1
  read -r observed _ < <(sha256sum "${CHILD_CLEANUP_PROOF}") || return 1
  [ "${observed}" \
    = a17fcf0a2f50e2d495e4f90ce263410edc183add6c62699a2facbccf60410f74 ]
}

attested_pod_is_absent() {
  [ "${CHILD_LAUNCHED}" = true ] || return 0
  [ "${RUNTIME_ATTESTATION_VALIDATED}" = true ] || return 1
  resource_is_absent pod "${ATTESTED_POD_NAME}" "${DEV_NAMESPACE}"
}

census_inventory_absent() {
  local observed
  observed=$(kctl -n "${DEV_NAMESPACE}" get jobs,pods,configmaps \
    -l "app.kubernetes.io/name=healthporta-plan-pricing-v3-census" \
    -o name) || return 1
  [ -z "${observed}" ]
}

verify_child_fences() {
  require_lock_held \
    && [ "${QUOTA_PROBE_VERIFIED}" = true ] \
    && [ "$(quota_identity)" = "${QUOTA_UID}" ] \
    && [ "$(policy_identity)" = "${POLICY_UID}" ] \
    && [ "$(binding_identity)" = "${BINDING_UID}" ] \
    && seed_is_absent \
    && [ "$(node_drain_mode read)" = true ] \
    && [ "$(kctl -n "${DEV_NAMESPACE}" get deployment \
      "${IMPORT_SCHEDULER_DEPLOYMENT}" -o jsonpath='{.spec.replicas}')" = 0 ] \
    && [ "$(active_engine_count)" -eq 0 ] \
    && [ "$(active_arc_count)" -eq 0 ] \
    && census_resources_absent
}

child_group_absent() {
  ! kill -0 -- "-$1" >/dev/null 2>&1
}

signal_child_group() {
  local pid=$1 number=$2
  kill -s "${number}" -- "-${pid}" >/dev/null 2>&1 \
    || kill -s "${number}" "${pid}" >/dev/null 2>&1 \
    || true
}

arm_child_shutdown() {
  local pid=$1 number=$2
  [ -z "${CHILD_KILL_TIMER_PID}" ] || return 0
  if [ "${CHILD_SIGNAL_FORWARDED}" = false ]; then
    signal_child_group "${pid}" "${number}"
    CHILD_SIGNAL_FORWARDED=true
  fi
  python3 -c '
import os, signal, sys, time
time.sleep(float(sys.argv[1]))
try:
    os.killpg(int(sys.argv[2]), signal.SIGKILL)
except ProcessLookupError:
    raise SystemExit(0)
time.sleep(float(sys.argv[3]))
' "${CHILD_TERMINATION_GRACE_SECONDS}" "${pid}" \
    "${CHILD_KILL_VERIFY_SECONDS}" &
  CHILD_KILL_TIMER_PID=$!
}

capture_child_pid() {
  CHILD_PID=$1
  [ "${INTERRUPT_EXIT}" -eq 0 ] \
    || arm_child_shutdown "${CHILD_PID}" "${INTERRUPT_SIGNAL}"
}

stop_child_kill_timer() {
  [ -n "${CHILD_KILL_TIMER_PID}" ] || return 0
  kill "${CHILD_KILL_TIMER_PID}" >/dev/null 2>&1 || true
  wait "${CHILD_KILL_TIMER_PID}" >/dev/null 2>&1 || true
  CHILD_KILL_TIMER_PID=
}

start_child_deadline_timer() {
  local pid=$1 remaining=$2 marker=$3
  python3 -c '
import os, signal, sys, time
delay, pid, marker, grace, verify = (
    float(sys.argv[1]), int(sys.argv[2]), sys.argv[3],
    float(sys.argv[4]), float(sys.argv[5]),
)
time.sleep(delay)
try:
    os.killpg(pid, 0)
except ProcessLookupError:
    raise SystemExit(0)
with open(marker + ".tmp", "w", encoding="utf-8") as target:
    target.write("deadline\n")
os.replace(marker + ".tmp", marker)
os.killpg(pid, signal.SIGTERM)
time.sleep(grace)
try:
    os.killpg(pid, signal.SIGKILL)
except ProcessLookupError:
    raise SystemExit(0)
time.sleep(verify)
' "${remaining}" "${pid}" "${marker}" \
    "${CHILD_TERMINATION_GRACE_SECONDS}" "${CHILD_KILL_VERIFY_SECONDS}" &
  CHILD_DEADLINE_TIMER_PID=$!
}

stop_child_deadline_timer() {
  [ -n "${CHILD_DEADLINE_TIMER_PID}" ] || return 0
  kill "${CHILD_DEADLINE_TIMER_PID}" >/dev/null 2>&1 || true
  wait "${CHILD_DEADLINE_TIMER_PID}" >/dev/null 2>&1 || true
  if [ -n "${CHILD_DEADLINE_MARKER}" ] \
      && [ -e "${CHILD_DEADLINE_MARKER}.tmp" ] \
      && [ ! -e "${CHILD_DEADLINE_MARKER}" ]; then
    mv "${CHILD_DEADLINE_MARKER}.tmp" "${CHILD_DEADLINE_MARKER}"
  fi
  CHILD_DEADLINE_TIMER_PID=
}

child_shutdown_timer_alive() {
  { [ -n "${CHILD_KILL_TIMER_PID}" ] \
      && kill -0 "${CHILD_KILL_TIMER_PID}" >/dev/null 2>&1; } \
    || { [ -n "${CHILD_DEADLINE_TIMER_PID}" ] \
      && kill -0 "${CHILD_DEADLINE_TIMER_PID}" >/dev/null 2>&1; }
}

reap_child_group() {
  local pid=$1 wait_exit=0 forced_group=false
  while :; do
    set +e
    wait "${pid}"
    wait_exit=$?
    set -e
    kill -0 "${pid}" >/dev/null 2>&1 || break
    if [ "${INTERRUPT_EXIT}" -ne 0 ] \
        && kill -0 "${pid}" >/dev/null 2>&1; then
      arm_child_shutdown "${pid}" "${INTERRUPT_SIGNAL}"
    fi
  done
  CHILD_EXIT_CODE=${wait_exit}
  if ! child_group_absent "${pid}"; then
    forced_group=true
    if [ ! -e "${CHILD_DEADLINE_MARKER}" ]; then
      stop_child_deadline_timer
      arm_child_shutdown "${pid}" TERM
    fi
    child_shutdown_timer_alive || arm_child_shutdown "${pid}" TERM
    while ! child_group_absent "${pid}"; do
      child_shutdown_timer_alive || return 1
      /bin/sleep 0.1
    done
  fi
  stop_child_kill_timer
  stop_child_deadline_timer
  [ ! -e "${CHILD_DEADLINE_MARKER}" ] || TIMED_OUT=true
  if [ "${forced_group}" = true ] \
      && [ "${INTERRUPT_EXIT}" -eq 0 ] \
      && [ "${TIMED_OUT}" = false ] \
      && [ "${CHILD_EXIT_CODE}" -eq 0 ]; then
    CHILD_EXIT_CODE=1
  fi
  CHILD_PID=
}

capture_capacity() {
  local limit observed
  limit=$(seconds_before_cleanup)
  [ "${limit}" -gt 0 ] || die "no time remains for the capacity admission"
  [ "${limit}" -le "${OPERATION_TIMEOUT_SECONDS}" ] \
    || limit=${OPERATION_TIMEOUT_SECONDS}
  observed=$(timeout --foreground --signal=TERM --kill-after=2s "${limit}s" \
    python3 - "${POSTGRESQL_TABLESPACE_PATH}" \
      "${MINIMUM_HOST_AVAILABLE_MEMORY_BYTES}" \
      "${MINIMUM_HOST_SWAP_FREE_BYTES}" \
      "${MINIMUM_POSTGRESQL_TABLESPACE_FREE_BYTES}" <<'PY'
import os
import sys

values = {}
with open("/proc/meminfo", encoding="utf-8") as source:
    for line in source:
        name, value = line.split(":", 1)
        if name in {"MemAvailable", "SwapFree"}:
            amount, unit = value.split()
            if unit != "kB":
                raise SystemExit("unexpected host memory unit")
            values[name] = int(amount) * 1024
if set(values) != {"MemAvailable", "SwapFree"}:
    raise SystemExit("host memory admission values are incomplete")
filesystem = os.statvfs(sys.argv[1])
tablespace_free = filesystem.f_bavail * filesystem.f_frsize
observed = (values["MemAvailable"], values["SwapFree"], tablespace_free)
minimums = tuple(int(value) for value in sys.argv[2:])
if any(value < minimum for value, minimum in zip(observed, minimums, strict=True)):
    raise SystemExit("host or PostgreSQL capacity is below the reviewed minimum")
print(*observed)
PY
  ) || die "host and PostgreSQL capacity admission failed"
  read -r HOST_AVAILABLE_MEMORY_BYTES HOST_SWAP_FREE_BYTES \
    POSTGRESQL_TABLESPACE_FREE_BYTES <<<"${observed}"
  CAPACITY_VERIFIED=true
}

validate_runtime_attestation() {
  python3 - "${RUNTIME_ATTESTATION}" "${CENSUS_JOB}" "${CENSUS_CONFIGMAP}" \
    "${SOURCE_SHA}" "${EXPECTED_SOURCE_MANIFEST_SHA256}" \
    "${EXPECTED_HARNESS_MANIFEST_SHA256}" \
    "${EXPECTED_SOURCE_OVERLAY_SHA256}" <<'PY'
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    attestation = json.load(source)
expected = {
    "contract", "job_name", "job_uid", "pod_name", "pod_uid",
    "pod_owner_job_name", "pod_owner_job_uid", "container_name", "image_id",
    "source_sha", "source_manifest_sha256", "harness_manifest_sha256",
    "source_overlay_sha256", "configmap_name", "configmap_uid",
    "job_source_configmap_name", "pod_source_configmap_name",
}
if set(attestation) != expected or not all(
    isinstance(value, str) and value for value in attestation.values()
):
    raise SystemExit("runtime attestation schema is invalid")
if (
    attestation["contract"]
    != "healthporta.plan-pricing-v3-census-runtime-attestation.v1"
    or attestation["job_name"] != sys.argv[2]
    or attestation["configmap_name"] != sys.argv[3]
    or attestation["job_source_configmap_name"] != sys.argv[3]
    or attestation["pod_source_configmap_name"] != sys.argv[3]
    or attestation["source_sha"] != sys.argv[4]
    or attestation["source_manifest_sha256"] != sys.argv[5]
    or attestation["harness_manifest_sha256"] != sys.argv[6]
    or attestation["source_overlay_sha256"] != sys.argv[7]
    or attestation["pod_owner_job_name"] != attestation["job_name"]
    or attestation["pod_owner_job_uid"] != attestation["job_uid"]
    or attestation["container_name"] != "census"
    or not re.fullmatch(r"[a-z0-9](?:[-.a-z0-9]*[a-z0-9])?", attestation["pod_name"])
    or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", attestation["pod_uid"])
    or not re.fullmatch(
        r"(?:containerd://sha256:[0-9a-f]{64}|[^\s@]+@sha256:[0-9a-f]{64})",
        attestation["image_id"],
    )
):
    raise SystemExit("runtime attestation identity is invalid")
print(attestation["pod_name"], attestation["pod_uid"], sep="\t")
PY
}

child_job_is_running() {
  local job_pid jobs_path="${STATE_DIR}/child-jobs.tmp"
  jobs -pr >"${jobs_path}" || return 1
  while IFS= read -r job_pid; do
    if [ "${job_pid}" = "${CHILD_PID}" ]; then
      rm -f -- "${jobs_path}"
      return 0
    fi
  done <"${jobs_path}"
  rm -f -- "${jobs_path}"
  return 1
}

capture_runtime_attestation() {
  local attempt attested_identity attestation_file_sha256 configmap_after_json
  local configmap_json job_after_json job_json job_uid pods_after_json pods_json
  local remaining status
  for ((attempt = 1; attempt <= 60; attempt++)); do
    [ "${INTERRUPT_EXIT}" -eq 0 ] || return 1
    child_job_is_running || return 1
    [ ! -e "${RUNTIME_ATTESTATION}" ] \
      && [ ! -L "${RUNTIME_ATTESTATION}" ] || return 1
    remaining=$(operation_timeout) || return $?
    job_json=$(kctl_with_limit "${remaining}" -n "${DEV_NAMESPACE}" \
      get job "${CENSUS_JOB}" --ignore-not-found -o json) || return 1
    child_job_is_running || return 1
    if [ -z "${job_json}" ]; then
      sleep 1
      continue
    fi
    job_uid=$(python3 -c '
import json, sys
job = json.load(sys.stdin)
metadata = job.get("metadata") if isinstance(job, dict) else None
if (
    job.get("apiVersion") != "batch/v1"
    or job.get("kind") != "Job"
    or not isinstance(metadata, dict)
    or metadata.get("name") != sys.argv[1]
    or metadata.get("namespace") != sys.argv[2]
    or not isinstance(metadata.get("uid"), str)
    or not metadata["uid"]
):
    raise SystemExit("exact census Job identity is invalid")
print(metadata["uid"])
' "${CENSUS_JOB}" "${DEV_NAMESPACE}" <<<"${job_json}") || return 1
    remaining=$(operation_timeout) || return $?
    configmap_json=$(kctl_with_limit "${remaining}" -n "${DEV_NAMESPACE}" \
      get configmap "${CENSUS_CONFIGMAP}" --ignore-not-found -o json) || return 1
    child_job_is_running || return 1
    [ -n "${configmap_json}" ] || return 1
    remaining=$(operation_timeout) || return $?
    pods_json=$(kctl_with_limit "${remaining}" -n "${DEV_NAMESPACE}" \
      get pods -l "batch.kubernetes.io/controller-uid=${job_uid}" \
      -o json) || return 1
    child_job_is_running || return 1
    remaining=$(operation_timeout) || return $?
    job_after_json=$(kctl_with_limit "${remaining}" -n "${DEV_NAMESPACE}" \
      get job "${CENSUS_JOB}" --ignore-not-found -o json) || return 1
    child_job_is_running || return 1
    remaining=$(operation_timeout) || return $?
    configmap_after_json=$(kctl_with_limit "${remaining}" -n "${DEV_NAMESPACE}" \
      get configmap "${CENSUS_CONFIGMAP}" --ignore-not-found -o json) || return 1
    child_job_is_running || return 1
    remaining=$(operation_timeout) || return $?
    pods_after_json=$(kctl_with_limit "${remaining}" -n "${DEV_NAMESPACE}" \
      get pods -l "batch.kubernetes.io/controller-uid=${job_uid}" \
      -o json) || return 1
    child_job_is_running || return 1
    if attested_identity=$(printf '%s\0' \
      "${job_json}" "${pods_json}" "${configmap_json}" \
      "${job_after_json}" "${pods_after_json}" "${configmap_after_json}" \
      | python3 /dev/fd/3 "${RUNTIME_ATTESTATION}.tmp" "${CENSUS_JOB}" \
        "${DEV_NAMESPACE}" "${CENSUS_CONFIGMAP}" "${SOURCE_SHA}" \
        "${EXPECTED_SOURCE_MANIFEST_SHA256}" \
        "${EXPECTED_HARNESS_MANIFEST_SHA256}" \
        "${EXPECTED_SOURCE_OVERLAY_SHA256}" 3<<'PY'
import base64
import binascii
import hashlib
import json
import re
import sys
from pathlib import Path

expected_source_annotations = {
    "healthporta.com/source-sha": sys.argv[5],
    "healthporta.com/source-manifest-sha256": sys.argv[6],
    "healthporta.com/harness-manifest-sha256": sys.argv[7],
    "healthporta.com/source-overlay-sha256": sys.argv[8],
}


def source_configmap_name(spec):
    if not isinstance(spec, dict):
        raise SystemExit("census source spec is invalid")
    volumes = spec.get("volumes")
    source_volumes = [
        row for row in volumes
        if isinstance(row, dict) and row.get("name") == "source"
    ] if isinstance(volumes, list) else []
    containers = spec.get("containers")
    census_containers = [
        row for row in containers
        if isinstance(row, dict) and row.get("name") == "census"
    ] if isinstance(containers, list) else []
    if len(source_volumes) != 1 or len(census_containers) != 1:
        raise SystemExit("census source volume is not singular")
    configmap = source_volumes[0].get("configMap")
    mounts = census_containers[0].get("volumeMounts")
    source_mounts = [
        row for row in mounts
        if isinstance(row, dict) and row.get("name") == "source"
    ] if isinstance(mounts, list) else []
    if (
        not isinstance(configmap, dict)
        or configmap.get("name") != sys.argv[4]
        or configmap.get("optional") is True
        or len(source_mounts) != 1
        or source_mounts[0].get("mountPath") != "/source"
        or source_mounts[0].get("readOnly") is not True
    ):
        raise SystemExit("census source volume identity is invalid")
    return configmap["name"]


def configmap_identity(configmap):
    metadata = configmap.get("metadata") if isinstance(configmap, dict) else None
    binary_data = configmap.get("binaryData") if isinstance(configmap, dict) else None
    if (
        configmap.get("apiVersion") != "v1"
        or configmap.get("kind") != "ConfigMap"
        or not isinstance(metadata, dict)
        or metadata.get("name") != sys.argv[4]
        or metadata.get("namespace") != sys.argv[3]
        or not isinstance(metadata.get("uid"), str)
        or not metadata["uid"]
        or configmap.get("immutable") is not True
        or configmap.get("data") not in (None, {})
        or not isinstance(binary_data, dict)
        or set(binary_data) != {"overlay.tar.gz"}
        or not isinstance(binary_data["overlay.tar.gz"], str)
    ):
        raise SystemExit("census source ConfigMap identity is invalid")
    try:
        overlay = base64.b64decode(binary_data["overlay.tar.gz"], validate=True)
    except (ValueError, binascii.Error) as exc:
        raise SystemExit("census source overlay is invalid") from exc
    overlay_sha256 = hashlib.sha256(overlay).hexdigest()
    if overlay_sha256 != sys.argv[8]:
        raise SystemExit("census source overlay identity changed")
    return metadata["uid"], overlay_sha256


def attestation_from(job, pods, configmap):
    job_metadata = job.get("metadata") if isinstance(job, dict) else None
    if (
        job.get("apiVersion") != "batch/v1"
        or job.get("kind") != "Job"
        or not isinstance(job_metadata, dict)
        or job_metadata.get("name") != sys.argv[2]
        or job_metadata.get("namespace") != sys.argv[3]
        or not isinstance(job_metadata.get("uid"), str)
        or not job_metadata["uid"]
    ):
        raise SystemExit("exact census Job identity is invalid")
    job_spec = job.get("spec")
    template = job_spec.get("template") if isinstance(job_spec, dict) else None
    template_spec = template.get("spec") if isinstance(template, dict) else None
    job_source_configmap_name = source_configmap_name(template_spec)
    configmap_uid, source_overlay_sha256 = configmap_identity(configmap)
    expected_annotations = {
        **expected_source_annotations,
        "healthporta.com/source-configmap-uid": configmap_uid,
    }
    job_annotations = job_metadata.get("annotations")
    template_metadata = template.get("metadata") if isinstance(template, dict) else None
    template_annotations = (
        template_metadata.get("annotations")
        if isinstance(template_metadata, dict)
        else None
    )
    if not all(
        isinstance(annotations, dict)
        and all(
            annotations.get(key) == value
            for key, value in expected_annotations.items()
        )
        for annotations in (job_annotations, template_annotations)
    ):
        raise SystemExit("census Job source identity is invalid")
    pod_rows = pods.get("items") if isinstance(pods, dict) else None
    if not isinstance(pod_rows, list):
        raise SystemExit("census Pod inventory is invalid")
    if not pod_rows:
        raise SystemExit(75)
    if len(pod_rows) != 1 or not isinstance(pod_rows[0], dict):
        raise SystemExit("census Pod inventory is not singular")
    pod = pod_rows[0]
    pod_metadata = pod.get("metadata")
    pod_spec = pod.get("spec")
    pod_status = pod.get("status")
    if (
        not isinstance(pod_metadata, dict)
        or not isinstance(pod_spec, dict)
        or not isinstance(pod_status, dict)
    ):
        raise SystemExit("census Pod identity is invalid")
    pod_source_configmap_name = source_configmap_name(pod_spec)
    pod_annotations = pod_metadata.get("annotations")
    if not isinstance(pod_annotations, dict) or any(
        pod_annotations.get(key) != value
        for key, value in expected_annotations.items()
    ):
        raise SystemExit("census Pod source identity is invalid")
    owners = pod_metadata.get("ownerReferences")
    controllers = [
        owner
        for owner in owners
        if isinstance(owner, dict) and owner.get("controller") is True
    ] if isinstance(owners, list) else []
    expected_controller = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "name": sys.argv[2],
        "uid": job_metadata["uid"],
        "controller": True,
    }
    if len(controllers) != 1:
        raise SystemExit("census Pod owner identity is invalid")
    controller = controllers[0]
    if (
        any(controller.get(key) != value for key, value in expected_controller.items())
        or set(controller) - set(expected_controller) - {"blockOwnerDeletion"}
        or (
            "blockOwnerDeletion" in controller
            and type(controller["blockOwnerDeletion"]) is not bool
        )
    ):
        raise SystemExit("census Pod owner identity is invalid")
    container_rows = pod_status.get("containerStatuses")
    census_rows = [
        row
        for row in container_rows
        if isinstance(row, dict) and row.get("name") == "census"
    ] if isinstance(container_rows, list) else []
    if not census_rows:
        raise SystemExit(75)
    if len(census_rows) != 1:
        raise SystemExit("census container status is not singular")
    image_id = census_rows[0].get("imageID")
    if image_id in (None, ""):
        raise SystemExit(75)
    if (
        pod_metadata.get("namespace") != sys.argv[3]
        or not isinstance(pod_metadata.get("name"), str)
        or not pod_metadata["name"]
        or not isinstance(pod_metadata.get("uid"), str)
        or not pod_metadata["uid"]
        or not isinstance(image_id, str)
        or re.fullmatch(
            r"(?:containerd://sha256:[0-9a-f]{64}|[^\s@]+@sha256:[0-9a-f]{64})",
            image_id,
        ) is None
    ):
        raise SystemExit("census Pod runtime identity is invalid")
    return {
        "contract": "healthporta.plan-pricing-v3-census-runtime-attestation.v1",
        "job_name": sys.argv[2],
        "job_uid": job_metadata["uid"],
        "pod_name": pod_metadata["name"],
        "pod_uid": pod_metadata["uid"],
        "pod_owner_job_name": sys.argv[2],
        "pod_owner_job_uid": job_metadata["uid"],
        "container_name": "census",
        "image_id": image_id,
        "source_sha": sys.argv[5],
        "source_manifest_sha256": sys.argv[6],
        "harness_manifest_sha256": sys.argv[7],
        "source_overlay_sha256": source_overlay_sha256,
        "configmap_name": sys.argv[4],
        "configmap_uid": configmap_uid,
        "job_source_configmap_name": job_source_configmap_name,
        "pod_source_configmap_name": pod_source_configmap_name,
    }


payloads = sys.stdin.buffer.read().split(b"\0")
if payloads[-1:] == [b""]:
    payloads.pop()
if len(payloads) != 6:
    raise SystemExit("census Kubernetes snapshots are incomplete")
job, pods, configmap, job_after, pods_after, configmap_after = (
    json.loads(payload) for payload in payloads
)
attestation = attestation_from(job, pods, configmap)
attestation_after = attestation_from(job_after, pods_after, configmap_after)
if attestation_after != attestation:
    raise SystemExit("census Job, Pod, or ConfigMap changed during attestation")
serialized = (json.dumps(attestation, sort_keys=True, separators=(",", ":")) + "\n").encode()
with Path(sys.argv[1]).open("xb") as target:
    target.write(serialized)
print(
    attestation["pod_name"],
    attestation["pod_uid"],
    hashlib.sha256(serialized).hexdigest(),
    sep="\t",
)
PY
    ); then
      [ ! -e "${RUNTIME_ATTESTATION}" ] \
        && [ ! -L "${RUNTIME_ATTESTATION}" ] || {
          rm -f "${RUNTIME_ATTESTATION}.tmp"
          return 1
      }
      IFS=$'\t' read -r ATTESTED_POD_NAME ATTESTED_POD_UID \
        CAPTURED_RUNTIME_ATTESTATION_SHA256 <<<"${attested_identity}"
      chmod 0600 "${RUNTIME_ATTESTATION}.tmp" || return 1
      read -r attestation_file_sha256 _ \
        < <(sha256sum "${RUNTIME_ATTESTATION}.tmp") || return 1
      [ "${attestation_file_sha256}" \
          = "${CAPTURED_RUNTIME_ATTESTATION_SHA256}" ] || return 1
      mv "${RUNTIME_ATTESTATION}.tmp" "${RUNTIME_ATTESTATION}" || return 1
      return 0
    else
      status=$?
      rm -f "${RUNTIME_ATTESTATION}.tmp"
      [ "${status}" -eq 75 ] || return 1
    fi
    sleep 1
  done
  return 1
}

run_child() {
  local attested_pod_identity remaining shutdown_signal
  verify_reviewed_hashes
  capture_capacity
  verify_reviewed_hashes
  verify_child_fences || die "final pre-child envelope fence changed"
  PRE_CHILD_FENCE_VERIFIED=true
  check_interrupted
  remaining=$(seconds_before_cleanup)
  [ "${remaining}" -gt 0 ] || die "no execution time remains before cleanup reserve"
  CHILD_DEADLINE_MARKER="${STATE_DIR}/child-deadline-fired"
  [ ! -e "${CENSUS_RECEIPT}" ] && [ ! -L "${CENSUS_RECEIPT}" ] \
    || die "census receipt already exists"
  [ ! -e "${RUNTIME_ATTESTATION}" ] && [ ! -L "${RUNTIME_ATTESTATION}" ] \
    || die "runtime attestation already exists"
  check_interrupted
  verify_reviewed_hashes
  open_reviewed_child_descriptor \
    || die "reviewed child executable descriptor changed"
  setsid "${CHILD_EXECUTABLE_DESCRIPTOR}" "${CHILD_COMMAND[@]:1}" &
  capture_child_pid "$!"
  exec 9<&-
  CHILD_LAUNCHED=true
  start_child_deadline_timer "${CHILD_PID}" "${remaining}" \
    "${CHILD_DEADLINE_MARKER}"
  if ! capture_runtime_attestation; then
    shutdown_signal=${INTERRUPT_SIGNAL:-TERM}
    if child_job_is_running; then
      arm_child_shutdown "${CHILD_PID}" "${shutdown_signal}"
    fi
    reap_child_group "${CHILD_PID}" \
      || die "census process group did not terminate after attestation failure"
    die "independent census runtime attestation failed"
  fi
  reap_child_group "${CHILD_PID}" \
    || die "census process group did not terminate"
  child_cleanup_is_complete \
    || die "census child cleanup was not proven complete"
  if [ -e "${CENSUS_RECEIPT}" ] || [ -L "${CENSUS_RECEIPT}" ]; then
    [ -f "${CENSUS_RECEIPT}" ] && [ ! -L "${CENSUS_RECEIPT}" ] \
      || die "census receipt is not a regular file"
    read -r CENSUS_RECEIPT_SHA256 _ < <(sha256sum "${CENSUS_RECEIPT}")
  elif [ "${CHILD_EXIT_CODE}" -eq 0 ]; then
    die "successful census process did not write its receipt"
  fi
  if [ -e "${RUNTIME_ATTESTATION}" ] || [ -L "${RUNTIME_ATTESTATION}" ]; then
    [ -f "${RUNTIME_ATTESTATION}" ] && [ ! -L "${RUNTIME_ATTESTATION}" ] \
      || die "census process did not write a regular runtime attestation"
    read -r RUNTIME_ATTESTATION_SHA256 _ \
      < <(sha256sum "${RUNTIME_ATTESTATION}")
    [ "${RUNTIME_ATTESTATION_SHA256}" \
        = "${CAPTURED_RUNTIME_ATTESTATION_SHA256}" ] \
      || die "census runtime attestation changed after capture"
    attested_pod_identity=$(validate_runtime_attestation) \
      || die "census runtime attestation is invalid"
    [ "${attested_pod_identity}" \
        = "${ATTESTED_POD_NAME}"$'\t'"${ATTESTED_POD_UID}" ] \
      || die "census runtime attestation changed after capture"
    RUNTIME_ATTESTATION_VALIDATED=true
  elif [ "${CHILD_EXIT_CODE}" -eq 0 ]; then
    die "successful census process did not write a regular runtime attestation"
  fi
  verify_child_fences || die "post-child envelope fence changed"
  POST_CHILD_FENCE_VERIFIED=true
}

on_signal() {
  local number=$1 exit_code=$2
  [ "${INTERRUPT_EXIT}" -eq 0 ] || return 0
  INTERRUPT_EXIT=${exit_code}
  INTERRUPT_SIGNAL=${number}
  if [ "${RECEIPT_FINALIZING}" = true ]; then
    rm -f "${STATE_DIR}/envelope-receipt.json" \
      "${STATE_DIR}/envelope-receipt.json.tmp"
    trap - EXIT INT TERM
    exit "${exit_code}"
  fi
  if [ -n "${CHILD_PID}" ]; then
    signal_child_group "${CHILD_PID}" "${number}"
    CHILD_SIGNAL_FORWARDED=true
  fi
}

delete_uid_bound() {
  local kind=$1 name=$2 expected_uid=$3 namespace=${4:-} observed path remaining
  if [ -n "${namespace}" ]; then
    observed=$(kctl -n "${namespace}" get "${kind}" "${name}" \
      --ignore-not-found -o jsonpath='{.metadata.uid}')
  else
    observed=$(kctl get "${kind}" "${name}" --ignore-not-found \
      -o jsonpath='{.metadata.uid}')
  fi
  [ "${observed}" = "${expected_uid}" ] \
    || { log "cleanup retained outer fences after ${kind} UID drift"; return 1; }
  case "${kind}" in
    resourcequota)
      [ -n "${namespace}" ] || return 1
      path="/api/v1/namespaces/${namespace}/resourcequotas/${name}"
      ;;
    validatingadmissionpolicy)
      [ -z "${namespace}" ] || return 1
      path="/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/${name}"
      ;;
    validatingadmissionpolicybinding)
      [ -z "${namespace}" ] || return 1
      path="/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/${name}"
      ;;
    *) return 1 ;;
  esac
  python3 -c '
import json, sys
json.dump({
    "apiVersion": "v1",
    "kind": "DeleteOptions",
    "preconditions": {"uid": sys.argv[1]},
    "propagationPolicy": "Foreground",
}, sys.stdout, separators=(",", ":"))
' "${expected_uid}" | kctl delete --raw="${path}" -f - >/dev/null \
    || return 1
  while :; do
    remaining=$(operation_timeout) || return $?
    if [ -n "${namespace}" ]; then
      observed=$(kctl_with_limit "${remaining}" -n "${namespace}" \
        get "${kind}" "${name}" --ignore-not-found \
        -o jsonpath='{.metadata.uid}') || return 1
    else
      observed=$(kctl_with_limit "${remaining}" get "${kind}" "${name}" \
        --ignore-not-found -o jsonpath='{.metadata.uid}') || return 1
    fi
    [ -n "${observed}" ] || return 0
    [ "${observed}" = "${expected_uid}" ] \
      || { log "cleanup retained outer fences after ${kind} UID drift"; return 1; }
    remaining=$(operation_timeout) || return $?
    [ "${remaining}" -gt 1 ] || return 1
    sleep 0.25
  done
}

reconcile_ambiguous_creates() {
  [ "${BINDING_CREATE_UNCERTAIN}" = false ] || return 1
  [ "${POLICY_CREATE_UNCERTAIN}" = false ] || return 1
  [ "${QUOTA_CREATE_UNCERTAIN}" = false ] || return 1
  { [ "${BINDING_ATTEMPTED}" = false ] \
      || [ "${BINDING_CREATED}" = true ]; } || return 1
  { [ "${POLICY_ATTEMPTED}" = false ] \
      || [ "${POLICY_CREATED}" = true ]; } || return 1
  { [ "${QUOTA_ATTEMPTED}" = false ] \
      || [ "${QUOTA_CREATED}" = true ]; } || return 1
}

outer_fence_identities_match() {
  if [ -n "${LOCK_INVOCATION_ID}" ]; then
    require_lock_held || return 1
  fi
  if [ "${QUOTA_CREATED}" = true ]; then
    [ "$(quota_identity)" = "${QUOTA_UID}" ] || return 1
  fi
  if [ "${POLICY_CREATED}" = true ]; then
    [ "$(policy_identity)" = "${POLICY_UID}" ] || return 1
  fi
  if [ "${BINDING_CREATED}" = true ]; then
    [ "$(binding_identity)" = "${BINDING_UID}" ] || return 1
  fi
}

retain_import_drain() {
  [ "${DRAIN_CAPTURED}" = true ] || return 0
  [ "${PRIOR_DRAIN_MODE}" = false ] || return 0
  DRAIN_RESTORED=false
  node_drain_mode true >/dev/null 2>&1 || true
  [ "$(node_drain_mode read)" = true ] || return 1
  seed_is_absent || return 1
}

release_lock() {
  local state
  [ "${LOCK_START_UNCERTAIN}" = false ] || return 1
  state=$(unit_load_state) || return 1
  if [ "${state}" = not-found ]; then
    [ -z "${LOCK_INVOCATION_ID}" ] || return 1
    LOCK_RELEASED=true
    return 0
  fi
  require_lock_held || return 1
  run_bounded systemctl stop "${LOCK_UNIT}" >/dev/null 2>&1 || return 1
  run_bounded systemctl reset-failed "${LOCK_UNIT}" >/dev/null 2>&1 || true
  state=$(unit_load_state) || return 1
  [ "${state}" != loaded ] || return 1
  LOCK_RELEASED=true
}

cleanup_envelope() {
  set +e
  rm -f -- "${STATE_DIR}/child-jobs.tmp" \
    "${STATE_DIR}/reviewed-child-executable"
  if [ -n "${CHILD_PID}" ] \
      && { kill -0 "${CHILD_PID}" >/dev/null 2>&1 \
        || ! child_group_absent "${CHILD_PID}"; }; then
    log 'cleanup retained every outer fence because the census process remains'
    return 1
  fi
  if [ "${CHILD_LAUNCHED}" = true ]; then
    child_cleanup_is_complete || {
      log 'cleanup retained every outer fence because child cleanup is unproven'
      return 1
    }
  fi
  census_resources_absent || {
    log 'cleanup retained every outer fence because census resources remain'
    return 1
  }
  reconcile_ambiguous_creates || return 1
  outer_fence_identities_match || {
    log 'cleanup retained every outer fence after semantic identity drift'
    return 1
  }
  if [ "${DRAIN_CAPTURED}" = true ]; then
    seed_is_absent || return 1
    node_drain_mode "${PRIOR_DRAIN_MODE}" >/dev/null 2>&1 || true
    seed_is_absent || {
      retain_import_drain
      return 1
    }
    [ "$(node_drain_mode read)" = "${PRIOR_DRAIN_MODE}" ] || {
      retain_import_drain
      return 1
    }
    DRAIN_RESTORED=true
  fi
  if [ "${BINDING_CREATED}" = true ]; then
    delete_uid_bound validatingadmissionpolicybinding "${BINDING_NAME}" \
      "${BINDING_UID}" || {
        retain_import_drain
        return 1
      }
    BINDING_REMOVED=true
  fi
  if [ "${POLICY_CREATED}" = true ]; then
    delete_uid_bound validatingadmissionpolicy "${POLICY_NAME}" \
      "${POLICY_UID}" || {
        retain_import_drain
        return 1
      }
    POLICY_REMOVED=true
  fi
  if [ "${QUOTA_CREATED}" = true ]; then
    [ "$(quota_identity)" = "${QUOTA_UID}" ] || {
      retain_import_drain
      return 1
    }
    delete_uid_bound resourcequota "${QUOTA_NAME}" "${QUOTA_UID}" \
      "${ARC_HOLD_NAMESPACE}" || {
        retain_import_drain
        return 1
      }
    QUOTA_REMOVED=true
  fi
  if [ "${LOCK_STARTED}" = true ]; then
    release_lock || return 1
  fi
  CLEANUP_COMPLETE=true
  return 0
}

write_receipt() {
  local exit_code=$1 script_sha status=failed
  script_sha=$(sha256sum "$0") || return 1
  script_sha=${script_sha%% *}
  [ "${exit_code}" -eq 0 ] && [ "${CLEANUP_COMPLETE}" = true ] && status=complete
  RECEIPT_STATUS=${status} \
  RECEIPT_EXIT_CODE=${exit_code} \
  RECEIPT_SOURCE_SHA=${SOURCE_SHA} \
  RECEIPT_SCRIPT_SHA=${script_sha} \
  RECEIPT_EXPECTED_SCRIPT_SHA=${EXPECTED_ENVELOPE_SCRIPT_SHA256} \
  RECEIPT_OWNER_TOKEN=${OWNER_TOKEN} \
  RECEIPT_QUOTA_UID=${QUOTA_UID} \
  RECEIPT_POLICY_UID=${POLICY_UID} \
  RECEIPT_BINDING_UID=${BINDING_UID} \
  RECEIPT_LOCK_INVOCATION_ID=${LOCK_INVOCATION_ID} \
  RECEIPT_PRIOR_DRAIN_MODE=${PRIOR_DRAIN_MODE} \
  RECEIPT_CHILD_COMMAND_SHA=${CHILD_COMMAND_SHA256} \
  RECEIPT_EXPECTED_CHILD_COMMAND_SHA=${EXPECTED_CHILD_COMMAND_SHA256} \
  RECEIPT_CHILD_EXECUTABLE_SHA=${CHILD_EXECUTABLE_SHA256} \
  RECEIPT_EXPECTED_CHILD_EXECUTABLE_SHA=${EXPECTED_CHILD_EXECUTABLE_SHA256} \
  RECEIPT_EXPECTED_SOURCE_MANIFEST_SHA=${EXPECTED_SOURCE_MANIFEST_SHA256} \
  RECEIPT_EXPECTED_HARNESS_MANIFEST_SHA=${EXPECTED_HARNESS_MANIFEST_SHA256} \
  RECEIPT_EXPECTED_SOURCE_OVERLAY_SHA=${EXPECTED_SOURCE_OVERLAY_SHA256} \
  RECEIPT_CENSUS_JOB=${CENSUS_JOB} \
  RECEIPT_CENSUS_CONFIGMAP=${CENSUS_CONFIGMAP} \
  RECEIPT_CENSUS_RECEIPT_SHA=${CENSUS_RECEIPT_SHA256} \
  RECEIPT_RUNTIME_ATTESTATION_PATH=${RUNTIME_ATTESTATION} \
  RECEIPT_RUNTIME_ATTESTATION_SHA=${RUNTIME_ATTESTATION_SHA256} \
  RECEIPT_RUNTIME_ATTESTATION_VALIDATED=${RUNTIME_ATTESTATION_VALIDATED} \
  RECEIPT_CHILD_EXIT_CODE=${CHILD_EXIT_CODE} \
  RECEIPT_TIMED_OUT=${TIMED_OUT} \
  RECEIPT_PROBE_VERIFIED=${PROBE_VERIFIED} \
  RECEIPT_QUOTA_PROBE_VERIFIED=${QUOTA_PROBE_VERIFIED} \
  RECEIPT_PRE_CHILD_FENCE_VERIFIED=${PRE_CHILD_FENCE_VERIFIED} \
  RECEIPT_POST_CHILD_FENCE_VERIFIED=${POST_CHILD_FENCE_VERIFIED} \
  RECEIPT_CAPACITY_VERIFIED=${CAPACITY_VERIFIED} \
  RECEIPT_HOST_AVAILABLE_MEMORY_BYTES=${HOST_AVAILABLE_MEMORY_BYTES} \
  RECEIPT_MINIMUM_HOST_AVAILABLE_MEMORY_BYTES=${MINIMUM_HOST_AVAILABLE_MEMORY_BYTES} \
  RECEIPT_HOST_SWAP_FREE_BYTES=${HOST_SWAP_FREE_BYTES} \
  RECEIPT_MINIMUM_HOST_SWAP_FREE_BYTES=${MINIMUM_HOST_SWAP_FREE_BYTES} \
  RECEIPT_POSTGRESQL_TABLESPACE_PATH=${POSTGRESQL_TABLESPACE_PATH} \
  RECEIPT_POSTGRESQL_TABLESPACE_FREE_BYTES=${POSTGRESQL_TABLESPACE_FREE_BYTES} \
  RECEIPT_MINIMUM_POSTGRESQL_TABLESPACE_FREE_BYTES=${MINIMUM_POSTGRESQL_TABLESPACE_FREE_BYTES} \
  RECEIPT_BINDING_REMOVED=${BINDING_REMOVED} \
  RECEIPT_POLICY_REMOVED=${POLICY_REMOVED} \
  RECEIPT_DRAIN_RESTORED=${DRAIN_RESTORED} \
  RECEIPT_QUOTA_REMOVED=${QUOTA_REMOVED} \
  RECEIPT_LOCK_RELEASED=${LOCK_RELEASED} \
  RECEIPT_CLEANUP_COMPLETE=${CLEANUP_COMPLETE} \
    python3 - "${STATE_DIR}/envelope-receipt.json.tmp" <<'PY' || return 1
import json, os, sys

def optional_bool(value):
    if value == "":
        return None
    if value not in {"false", "true"}:
        raise ValueError("invalid receipt boolean")
    return value == "true"

def optional_int(value):
    return None if value == "" else int(value)

runtime_attestation = None
if optional_bool(os.environ["RECEIPT_RUNTIME_ATTESTATION_VALIDATED"]):
    with open(
        os.environ["RECEIPT_RUNTIME_ATTESTATION_PATH"], encoding="utf-8"
    ) as source:
        runtime_attestation = json.load(source)

receipt = {
    "contract": "healthporta.plan-pricing-v3-census-envelope.v1",
    "status": os.environ["RECEIPT_STATUS"],
    "exit_code": int(os.environ["RECEIPT_EXIT_CODE"]),
    "reviewed_source_sha": os.environ["RECEIPT_SOURCE_SHA"],
    "envelope_script_sha256": os.environ["RECEIPT_SCRIPT_SHA"],
    "expected_envelope_script_sha256": os.environ[
        "RECEIPT_EXPECTED_SCRIPT_SHA"
    ],
    "owner_token": os.environ["RECEIPT_OWNER_TOKEN"],
    "resource_uids": {
        "quota": os.environ["RECEIPT_QUOTA_UID"],
        "policy": os.environ["RECEIPT_POLICY_UID"],
        "binding": os.environ["RECEIPT_BINDING_UID"],
        "lock_invocation": os.environ["RECEIPT_LOCK_INVOCATION_ID"],
    },
    "prior_drain_mode": optional_bool(os.environ["RECEIPT_PRIOR_DRAIN_MODE"]),
    "child_command_sha256": os.environ["RECEIPT_CHILD_COMMAND_SHA"],
    "expected_child_command_sha256": os.environ[
        "RECEIPT_EXPECTED_CHILD_COMMAND_SHA"
    ],
    "child_executable_sha256": os.environ["RECEIPT_CHILD_EXECUTABLE_SHA"],
    "expected_child_executable_sha256": os.environ[
        "RECEIPT_EXPECTED_CHILD_EXECUTABLE_SHA"
    ],
    "expected_source_manifest_sha256": os.environ[
        "RECEIPT_EXPECTED_SOURCE_MANIFEST_SHA"
    ],
    "expected_harness_manifest_sha256": os.environ[
        "RECEIPT_EXPECTED_HARNESS_MANIFEST_SHA"
    ],
    "expected_source_overlay_sha256": os.environ[
        "RECEIPT_EXPECTED_SOURCE_OVERLAY_SHA"
    ],
    "census_job": os.environ["RECEIPT_CENSUS_JOB"],
    "census_configmap": os.environ["RECEIPT_CENSUS_CONFIGMAP"],
    "census_receipt_sha256": os.environ["RECEIPT_CENSUS_RECEIPT_SHA"],
    "runtime_attestation": runtime_attestation,
    "runtime_attestation_sha256": os.environ[
        "RECEIPT_RUNTIME_ATTESTATION_SHA"
    ],
    "child_exit_code": optional_int(os.environ["RECEIPT_CHILD_EXIT_CODE"]),
    "timed_out": optional_bool(os.environ["RECEIPT_TIMED_OUT"]),
    "probe_verified": optional_bool(os.environ["RECEIPT_PROBE_VERIFIED"]),
    "quota_probe_verified": optional_bool(
        os.environ["RECEIPT_QUOTA_PROBE_VERIFIED"]
    ),
    "pre_child_fence_verified": optional_bool(
        os.environ["RECEIPT_PRE_CHILD_FENCE_VERIFIED"]
    ),
    "post_child_fence_verified": optional_bool(
        os.environ["RECEIPT_POST_CHILD_FENCE_VERIFIED"]
    ),
    "capacity": {
        "verified": optional_bool(os.environ["RECEIPT_CAPACITY_VERIFIED"]),
        "host_available_memory_bytes": optional_int(
            os.environ["RECEIPT_HOST_AVAILABLE_MEMORY_BYTES"]
        ),
        "minimum_host_available_memory_bytes": optional_int(
            os.environ["RECEIPT_MINIMUM_HOST_AVAILABLE_MEMORY_BYTES"]
        ),
        "host_swap_free_bytes": optional_int(
            os.environ["RECEIPT_HOST_SWAP_FREE_BYTES"]
        ),
        "minimum_host_swap_free_bytes": optional_int(
            os.environ["RECEIPT_MINIMUM_HOST_SWAP_FREE_BYTES"]
        ),
        "postgresql_tablespace_path": os.environ[
            "RECEIPT_POSTGRESQL_TABLESPACE_PATH"
        ],
        "postgresql_tablespace_free_bytes": optional_int(
            os.environ["RECEIPT_POSTGRESQL_TABLESPACE_FREE_BYTES"]
        ),
        "minimum_postgresql_tablespace_free_bytes": optional_int(
            os.environ["RECEIPT_MINIMUM_POSTGRESQL_TABLESPACE_FREE_BYTES"]
        ),
    },
    "cleanup": {
        "binding_removed": optional_bool(os.environ["RECEIPT_BINDING_REMOVED"]),
        "policy_removed": optional_bool(os.environ["RECEIPT_POLICY_REMOVED"]),
        "drain_restored": optional_bool(os.environ["RECEIPT_DRAIN_RESTORED"]),
        "quota_removed": optional_bool(os.environ["RECEIPT_QUOTA_REMOVED"]),
        "lock_released": optional_bool(os.environ["RECEIPT_LOCK_RELEASED"]),
        "complete": optional_bool(os.environ["RECEIPT_CLEANUP_COMPLETE"]),
    },
    "postgresql_boundary": (
        "Kubernetes QoS does not reserve or cap off-node PostgreSQL"
    ),
}
with open(sys.argv[1], "w", encoding="utf-8") as target:
    json.dump(receipt, target, sort_keys=True, separators=(",", ":"))
    target.write("\n")
PY
  chmod 0600 "${STATE_DIR}/envelope-receipt.json.tmp" || return 1
  mv "${STATE_DIR}/envelope-receipt.json.tmp" \
    "${STATE_DIR}/envelope-receipt.json" || return 1
}

finish() {
  local incoming=$1 cleanup_exit=0 final_exit
  final_exit=${incoming}
  [ "${EXIT_TRAP_ACTIVE}" = true ] || return 0
  EXIT_TRAP_ACTIVE=false
  cleanup_envelope || cleanup_exit=$?
  RECEIPT_FINALIZING=true
  if [ "${INTERRUPT_EXIT}" -ne 0 ]; then
    final_exit=${INTERRUPT_EXIT}
  elif [ "${TIMED_OUT}" = true ]; then
    final_exit=124
  elif [ -n "${CHILD_EXIT_CODE}" ] && [ "${CHILD_EXIT_CODE}" -ne 0 ]; then
    final_exit=${CHILD_EXIT_CODE}
  elif [ "${cleanup_exit}" -ne 0 ]; then
    final_exit=1
  fi
  write_receipt "${final_exit}" || final_exit=1
  trap '' INT TERM
  RECEIPT_FINALIZING=false
  trap - EXIT
  exit "${final_exit}"
}

run_envelope() {
  [ "${!OPT_IN_ENV:-}" = run ] \
    || die "run mode requires ${OPT_IN_ENV}=run"
  for command in git hostname k3s python3 setsid sha256sum systemctl systemd-run timeout; do
    require_command "${command}"
  done
  verify_reviewed_hashes
  verify_source_and_target
  state_dir_is_confined \
    || die "state directory escaped its reviewed root"
  create_state_directory \
    || die "state directory could not be created inside its reviewed root"
  START_SECONDS=${SECONDS}
  EXIT_TRAP_ACTIVE=true
  trap 'on_signal TERM 143' TERM
  trap 'on_signal INT 130' INT
  trap 'finish $?' EXIT

  log 'acquiring the DEV build lock'
  start_lock
  require_lock_held || die "build lock was not retained after acquisition"
  check_interrupted
  log 'holding ARC admission and waiting for natural drain'
  create_quota
  prove_quota_admission
  wait_for_arc_idle
  check_interrupted
  log 'draining the supported import node'
  set_import_drain
  check_interrupted
  log 'installing the exact engine-worker creation fence'
  create_worker_fence
  check_interrupted
  log 'proving stable zero engine work'
  verify_stable_zero_work
  check_interrupted
  log 'running the foreground census lifecycle'
  run_child
}

main() {
  parse_args "$@"
  validate_args
  if [ "${MODE}" = plan ]; then
    render_plan
    return 0
  fi
  run_envelope
}

main "$@"
