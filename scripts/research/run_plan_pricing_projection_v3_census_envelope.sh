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
DRAIN_DEPLOYMENT=
IMPORT_SCHEDULER_DEPLOYMENT=
IMPORT_NODE_ID=
IMPORT_TOKEN_ENV=
CHILD_COMMAND=()

LOCK_UNIT=
LOCK_INVOCATION_ID=
LOCK_MARKER=
LOCK_STARTED=false
QUOTA_NAME=
QUOTA_UID=
POLICY_NAME=
POLICY_UID=
BINDING_NAME=
BINDING_UID=
DENIAL_MARKER=
PRIOR_DRAIN_MODE=
CHILD_COMMAND_SHA256=
CENSUS_RECEIPT_SHA256=
CHILD_EXIT_CODE=
CHILD_PID=
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
QUOTA_CREATED=false
QUOTA_PROBE_VERIFIED=false
DRAIN_CAPTURED=false
POLICY_ATTEMPTED=false
POLICY_CREATED=false
BINDING_ATTEMPTED=false
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
    '  --census-receipt PATH' \
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

validate_args() {
  [[ "${OWNER_TOKEN}" =~ ^[a-z0-9][a-z0-9-]{7,31}$ ]] \
    || die "owner token must be 8-32 lowercase DNS-label characters"
  [[ "${SOURCE_SHA}" =~ ^[0-9a-f]{40}$ ]] \
    || die "exact reviewed source SHA is required"
  [[ "${DEADLINE_SECONDS}" =~ ^[0-9]+$ ]] \
    && [ "${DEADLINE_SECONDS}" -ge 900 ] \
    && [ "${DEADLINE_SECONDS}" -le 86400 ] \
    || die "deadline must be 900-86400 seconds"
  [[ "${REPO_DIR}" = /* ]] || die "repo directory must be absolute"
  [[ "${STATE_DIR}" = "${STATE_ROOT}/"* ]] \
    && [ "${STATE_DIR}" != "${STATE_ROOT}/" ] \
    || die "state directory must be a child of ${STATE_ROOT}"
  [[ "${CENSUS_JOB}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
    && [ "${#CENSUS_JOB}" -le 63 ] \
    || die "exact census Job name is invalid"
  [[ "${CENSUS_CONFIGMAP}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
    && [ "${#CENSUS_CONFIGMAP}" -le 63 ] \
    || die "exact census ConfigMap name is invalid"
  [ "${CENSUS_RECEIPT}" = "${STATE_DIR}/census-receipt.json" ] \
    || die "exact census receipt must be the task state receipt"
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
    '  - run foreground census command under remaining deadline' \
    '  - remove binding, policy, restore drain, remove quota, release flock'
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
  census_pods_absent || die "preexisting census Pod blocks the envelope"
  verify_seed_absent
  [ "$(unit_load_state)" = not-found ] \
    || die "owner lock unit already exists"
}

require_lock_held() {
  local active invocation marker
  active=$(run_bounded systemctl is-active "${LOCK_UNIT}" 2>/dev/null) \
    || return 1
  [ "${active}" = active ] || return 1
  invocation=$(run_bounded systemctl show "${LOCK_UNIT}" \
    --property=InvocationID --value) || return 1
  [ -n "${LOCK_INVOCATION_ID}" ] \
    && [ "${invocation}" = "${LOCK_INVOCATION_ID}" ] || return 1
  [ -f "${LOCK_MARKER}" ] || return 1
  marker=$(<"${LOCK_MARKER}")
  [ "${marker}" = "${LOCK_INVOCATION_ID}:${OWNER_TOKEN}" ]
}

start_lock() {
  local create_exit=0 marker
  LOCK_STARTED=true
  set +e
  run_bounded systemd-run --quiet --unit="${LOCK_UNIT}" --property=Type=exec \
    --property=Restart=no \
    --property="RuntimeMaxSec=$((DEADLINE_SECONDS + CLEANUP_RESERVE_SECONDS))s" \
    /usr/bin/flock -n "${BUILD_LOCK}" /bin/sh -c \
    'umask 077; printf "%s:%s\n" "$INVOCATION_ID" "$1" > "$2"; exec /usr/bin/sleep infinity' \
    sh "${OWNER_TOKEN}" "${LOCK_MARKER}"
  create_exit=$?
  set -e
  for _ in {1..20}; do
    if [ -f "${LOCK_MARKER}" ]; then
      marker=$(<"${LOCK_MARKER}")
      LOCK_INVOCATION_ID=${marker%%:*}
      if [ -n "${LOCK_INVOCATION_ID}" ] \
          && [ "${marker}" = "${LOCK_INVOCATION_ID}:${OWNER_TOKEN}" ] \
          && require_lock_held; then
        [ "${create_exit}" -eq 0 ] \
          || die "build lock appeared after its create command failed"
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
  [ "${create_exit}" -eq 0 ] \
    || die "policy appeared after its create command failed"

  BINDING_ATTEMPTED=true
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
    && census_pods_absent
}

census_pods_absent() {
  local observed
  observed=$(kctl -n "${DEV_NAMESPACE}" get pods \
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

run_child() {
  local remaining
  remaining=$(seconds_before_cleanup)
  [ "${remaining}" -gt 0 ] || die "no execution time remains before cleanup reserve"
  read -r CHILD_COMMAND_SHA256 _ \
    < <(printf '%q\0' "${CHILD_COMMAND[@]}" | sha256sum)
  CHILD_DEADLINE_MARKER="${STATE_DIR}/child-deadline-fired"
  [ ! -e "${CENSUS_RECEIPT}" ] && [ ! -L "${CENSUS_RECEIPT}" ] \
    || die "census receipt already exists"
  check_interrupted
  setsid "${CHILD_COMMAND[@]}" &
  capture_child_pid "$!"
  start_child_deadline_timer "${CHILD_PID}" "${remaining}" \
    "${CHILD_DEADLINE_MARKER}"
  reap_child_group "${CHILD_PID}" \
    || die "census process group did not terminate"
  if [ -e "${CENSUS_RECEIPT}" ] || [ -L "${CENSUS_RECEIPT}" ]; then
    [ -f "${CENSUS_RECEIPT}" ] && [ ! -L "${CENSUS_RECEIPT}" ] \
      || die "census receipt is not a regular file"
    read -r CENSUS_RECEIPT_SHA256 _ < <(sha256sum "${CENSUS_RECEIPT}")
  elif [ "${CHILD_EXIT_CODE}" -eq 0 ]; then
    die "successful census process did not write its receipt"
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
  local observed
  if [ "${BINDING_ATTEMPTED}" = true ] \
      && [ "${BINDING_CREATED}" = false ]; then
    observed=$(kctl get validatingadmissionpolicybinding "${BINDING_NAME}" \
      --ignore-not-found -o jsonpath='{.metadata.uid}') || return 1
    if [ -n "${observed}" ]; then
      BINDING_UID=$(binding_identity) || return 1
      BINDING_CREATED=true
    else
      BINDING_REMOVED=true
    fi
  fi
  if [ "${POLICY_ATTEMPTED}" = true ] \
      && [ "${POLICY_CREATED}" = false ]; then
    observed=$(kctl get validatingadmissionpolicy "${POLICY_NAME}" \
      --ignore-not-found -o jsonpath='{.metadata.uid}') || return 1
    if [ -n "${observed}" ]; then
      POLICY_UID=$(policy_identity) || return 1
      POLICY_CREATED=true
    else
      POLICY_REMOVED=true
    fi
  fi
  if [ "${QUOTA_ATTEMPTED}" = true ] \
      && [ "${QUOTA_CREATED}" = false ]; then
    observed=$(kctl -n "${ARC_HOLD_NAMESPACE}" get resourcequota \
      "${QUOTA_NAME}" --ignore-not-found \
      -o jsonpath='{.metadata.uid}') || return 1
    if [ -n "${observed}" ]; then
      QUOTA_UID=$(quota_identity) || return 1
      QUOTA_CREATED=true
    else
      QUOTA_REMOVED=true
    fi
  fi
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

release_lock() {
  local invocation marker state
  state=$(unit_load_state) || return 1
  if [ "${state}" = not-found ]; then
    [ -z "${LOCK_INVOCATION_ID}" ] || return 1
    LOCK_RELEASED=true
    return 0
  fi
  invocation=$(run_bounded systemctl show "${LOCK_UNIT}" \
    --property=InvocationID --value) || return 1
  [ -n "${invocation}" ] || return 1
  if [ -n "${LOCK_INVOCATION_ID}" ]; then
    [ "${invocation}" = "${LOCK_INVOCATION_ID}" ] || return 1
    [ -f "${LOCK_MARKER}" ] || return 1
    marker=$(<"${LOCK_MARKER}")
    [ "${marker}" = "${LOCK_INVOCATION_ID}:${OWNER_TOKEN}" ] || return 1
  else
    LOCK_INVOCATION_ID=${invocation}
  fi
  run_bounded systemctl stop "${LOCK_UNIT}" >/dev/null 2>&1 || return 1
  run_bounded systemctl reset-failed "${LOCK_UNIT}" >/dev/null 2>&1 || true
  state=$(unit_load_state) || return 1
  [ "${state}" != loaded ] || return 1
  LOCK_RELEASED=true
}

cleanup_envelope() {
  set +e
  if [ -n "${CHILD_PID}" ] \
      && { kill -0 "${CHILD_PID}" >/dev/null 2>&1 \
        || ! child_group_absent "${CHILD_PID}"; }; then
    log 'cleanup retained every outer fence because the census process remains'
    return 1
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
  if [ "${BINDING_CREATED}" = true ]; then
    delete_uid_bound validatingadmissionpolicybinding "${BINDING_NAME}" \
      "${BINDING_UID}" || return 1
    BINDING_REMOVED=true
  fi
  if [ "${POLICY_CREATED}" = true ]; then
    delete_uid_bound validatingadmissionpolicy "${POLICY_NAME}" \
      "${POLICY_UID}" || return 1
    POLICY_REMOVED=true
  fi
  if [ "${DRAIN_CAPTURED}" = true ]; then
    seed_is_absent || return 1
    [ "$(node_drain_mode "${PRIOR_DRAIN_MODE}")" = "${PRIOR_DRAIN_MODE}" ] \
      || return 1
    seed_is_absent || return 1
    DRAIN_RESTORED=true
  fi
  if [ "${QUOTA_CREATED}" = true ]; then
    [ "$(quota_identity)" = "${QUOTA_UID}" ] || return 1
    delete_uid_bound resourcequota "${QUOTA_NAME}" "${QUOTA_UID}" \
      "${ARC_HOLD_NAMESPACE}" || return 1
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
  RECEIPT_OWNER_TOKEN=${OWNER_TOKEN} \
  RECEIPT_QUOTA_UID=${QUOTA_UID} \
  RECEIPT_POLICY_UID=${POLICY_UID} \
  RECEIPT_BINDING_UID=${BINDING_UID} \
  RECEIPT_LOCK_INVOCATION_ID=${LOCK_INVOCATION_ID} \
  RECEIPT_PRIOR_DRAIN_MODE=${PRIOR_DRAIN_MODE} \
  RECEIPT_CHILD_COMMAND_SHA=${CHILD_COMMAND_SHA256} \
  RECEIPT_CENSUS_JOB=${CENSUS_JOB} \
  RECEIPT_CENSUS_RECEIPT_SHA=${CENSUS_RECEIPT_SHA256} \
  RECEIPT_CHILD_EXIT_CODE=${CHILD_EXIT_CODE} \
  RECEIPT_TIMED_OUT=${TIMED_OUT} \
  RECEIPT_PROBE_VERIFIED=${PROBE_VERIFIED} \
  RECEIPT_QUOTA_PROBE_VERIFIED=${QUOTA_PROBE_VERIFIED} \
  RECEIPT_PRE_CHILD_FENCE_VERIFIED=${PRE_CHILD_FENCE_VERIFIED} \
  RECEIPT_POST_CHILD_FENCE_VERIFIED=${POST_CHILD_FENCE_VERIFIED} \
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

receipt = {
    "contract": "healthporta.plan-pricing-v3-census-envelope.v1",
    "status": os.environ["RECEIPT_STATUS"],
    "exit_code": int(os.environ["RECEIPT_EXIT_CODE"]),
    "reviewed_source_sha": os.environ["RECEIPT_SOURCE_SHA"],
    "envelope_script_sha256": os.environ["RECEIPT_SCRIPT_SHA"],
    "owner_token": os.environ["RECEIPT_OWNER_TOKEN"],
    "resource_uids": {
        "quota": os.environ["RECEIPT_QUOTA_UID"],
        "policy": os.environ["RECEIPT_POLICY_UID"],
        "binding": os.environ["RECEIPT_BINDING_UID"],
        "lock_invocation": os.environ["RECEIPT_LOCK_INVOCATION_ID"],
    },
    "prior_drain_mode": optional_bool(os.environ["RECEIPT_PRIOR_DRAIN_MODE"]),
    "child_command_sha256": os.environ["RECEIPT_CHILD_COMMAND_SHA"],
    "census_job": os.environ["RECEIPT_CENSUS_JOB"],
    "census_receipt_sha256": os.environ["RECEIPT_CENSUS_RECEIPT_SHA"],
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
  verify_source_and_target
  mkdir -m 0700 "${STATE_DIR}"
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
  verify_child_fences || die "pre-child envelope fence changed"
  check_interrupted
  PRE_CHILD_FENCE_VERIFIED=true
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
