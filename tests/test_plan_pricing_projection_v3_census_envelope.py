from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/research/run_plan_pricing_projection_v3_census_envelope.sh"
SOURCE_SHA = "a" * 40
OWNER = "testowner1"


_FAKE_COMMAND = r"""#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sys
name = Path(sys.argv[0]).name
args = sys.argv[1:]
state = Path(os.environ["FAKE_STATE"])
events = state / "events"
def event(value: str) -> None:
    with events.open("a", encoding="utf-8") as target:
        target.write(value + "\n")
def raise_exit(code: int) -> None: raise SystemExit(code)
def resource_path(kind: str) -> Path: return state / kind
if name == "hostname": print("ns1033171")
elif name == "git":
    if "rev-parse" in args:
        print(os.environ["FAKE_SOURCE_SHA"])
elif name == "sleep": pass
elif name == "systemd-run":
    event("lock_create")
    resource_path("lock").touch()
    if os.environ.get("FAKE_LOCK_CONTENDED") != "1":
        Path(args[-1]).write_text(
            "lock-invocation-uid:" + args[-2] + "\n", encoding="utf-8"
        )
    if os.environ.get("FAKE_LOCK_CLIENT_ERROR") == "1": raise SystemExit(75)
elif name == "systemctl":
    operation = args[0]
    after_child = events.exists() and "child" in events.read_text()
    lock_drift = os.environ.get("FAKE_LOCK_DRIFT_AFTER_CHILD") if after_child else None
    lock_exists = resource_path("lock").exists() and lock_drift != "gone"
    if operation == "is-active":
        if lock_exists:
            print("active")
        raise SystemExit(0 if lock_exists else 3)
    if operation == "show":
        if any("LoadState" in argument for argument in args):
            print("loaded" if lock_exists else "not-found")
        else:
            print("replacement-invocation" if lock_drift == "replaced" else "lock-invocation-uid")
    elif operation == "stop":
        event("lock_stop")
        resource_path("lock").unlink(missing_ok=True)
    elif operation == "reset-failed":
        pass
elif name == "setsid": os.setsid(); os.execvp(args[0], args)
elif name == "timeout":
    command_index = next(
        index + 1
        for index, argument in enumerate(args)
        if not argument.startswith("-")
    )
    command = Path(args[command_index]).name
    os.execvp(args[command_index], args[command_index:])
elif name == "census-child":
    event("child")
    if args[:1] != ["--receipt"]: raise SystemExit("missing census receipt path")
    Path(args[1]).write_text(json.dumps({"accepted": True}, indent=2, sort_keys=True) + "\n")
    args = args[2:]
    if os.environ.get("FAKE_CHILD_MODE") == "exit7":
        raise SystemExit(7)
    if os.environ.get("FAKE_CHILD_MODE") == "native124":
        if os.environ.get("FAKE_CHILD_ORPHAN") == "1":
            resource_path("job").touch()
            resource_path("configmap").touch()
        raise SystemExit(124)
    if os.environ.get("FAKE_CHILD_MODE") == "linger":
        if os.fork() == 0:
            signal.signal(signal.SIGTERM, lambda *_: raise_exit(0))
            signal.signal(signal.SIGALRM, lambda *_: raise_exit(0))
            signal.alarm(3)
            while True:
                signal.pause()
        raise SystemExit(0)
    if os.environ.get("FAKE_CHILD_ORPHAN_POD") == "1":
        resource_path("pod").touch()
    if os.environ.get("FAKE_DRAIN_DRIFT_AFTER_CHILD") == "1":
        resource_path("drain").write_text("false")
    if os.environ.get("FAKE_CHILD_MODE") == "wait":
        def finish(number, _frame):
            event("child_signal_" + str(number))
            raise SystemExit(128 + number)
        signal.signal(signal.SIGTERM, finish)
        signal.signal(signal.SIGINT, finish)
        while True:
            signal.pause()
elif name == "k3s":
    if not args or args.pop(0) != "kubectl":
        raise SystemExit("expected kubectl")
    args = [arg for arg in args if not arg.startswith("--kubeconfig=")
            and not arg.startswith("--request-timeout=")]
    namespace = None
    cleaned: list[str] = []
    index = 0
    while index < len(args):
        if args[index] in {"-n", "--namespace"}:
            namespace = args[index + 1]
            index += 2
        elif args[index].startswith("--as="):
            index += 1
        else:
            cleaned.append(args[index])
            index += 1
    args = cleaned
    operation = args[0]
    if operation == "config":
        print("default")
    elif operation == "exec":
        desired, node_id, token_env = args[-3:]
        if node_id != os.environ["FAKE_IMPORT_NODE_ID"]:
            raise SystemExit("unexpected import node")
        if token_env != os.environ["FAKE_IMPORT_TOKEN_ENV"]:
            raise SystemExit("unexpected import token environment")
        drain_path = resource_path("drain")
        current = drain_path.read_text().strip() if drain_path.exists() else "false"
        if desired in {"true", "false"}:
            current = desired
            drain_path.write_text(current)
            event("drain_set_" + current)
        else:
            event("drain_read")
        print(current)
    elif operation == "create":
        manifest = sys.stdin.read()
        if "--dry-run=server" in args:
            if "namespace: arc-runners" in manifest:
                if os.environ.get("FAKE_QUOTA_PROBE_ALLOWED") == "1":
                    event("quota_probe_allowed")
                    print("pod/quota-probe created (server dry run)")
                else:
                    event("quota_probe_denied")
                    print("exceeded quota: " + os.environ["FAKE_QUOTA"], file=sys.stderr)
                    raise SystemExit(1)
            else:
                event("probe_denied")
                print(os.environ["FAKE_DENIAL_MARKER"], file=sys.stderr)
                raise SystemExit(1)
        if "kind: ResourceQuota" in manifest:
            kind = "quota"
        elif "kind: ValidatingAdmissionPolicyBinding" in manifest:
            kind = "binding"
        elif "kind: ValidatingAdmissionPolicy" in manifest:
            kind = "policy"
        else:
            raise SystemExit("unexpected manifest")
        resource_path(kind).touch()
        event(kind + "_create")
        if os.environ.get("FAKE_CREATE_ERROR") == kind:
            raise SystemExit(75)
    elif operation == "delete":
        raw_path = next(
            argument.removeprefix("--raw=")
            for argument in args
            if argument.startswith("--raw=")
        )
        if "/resourcequotas/" in raw_path:
            kind = "quota"
        elif "/validatingadmissionpolicies/" in raw_path:
            kind = "policy"
        elif "/validatingadmissionpolicybindings/" in raw_path:
            kind = "binding"
        else:
            raise SystemExit("unexpected raw delete path")
        options = json.load(sys.stdin)
        if options != {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "preconditions": {"uid": kind + "-uid"},
            "propagationPolicy": "Foreground",
        }:
            raise SystemExit("missing UID delete precondition")
        if os.environ.get("FAKE_REPLACE_ON_DELETE") == kind:
            event(kind + "_replace")
            raise SystemExit("UID precondition failed")
        if os.environ.get("FAKE_DELETE_LINGER") == kind: resource_path(kind + "-deleting").touch()
        else: resource_path(kind).unlink(missing_ok=True)
        event(kind + "_delete")
    elif operation == "get":
        kind = args[1]
        if kind == "--raw":
            if args[2] == "/version":
                print(json.dumps({"major": os.environ.get("FAKE_SERVER_MAJOR", "1"), "minor": os.environ.get("FAKE_SERVER_MINOR", "35")}))
                raise SystemExit(0)
            resources = ["validatingadmissionpolicies"]
            if os.environ.get("FAKE_V1_ADMISSION_MISSING") != "1":
                resources.append("validatingadmissionpolicybindings")
            print(json.dumps({"resources": [{"name": value} for value in resources]}))
            raise SystemExit(0)
        elif (
            os.environ.get("FAKE_GET_ERROR_AFTER_CHILD") == kind
            and events.exists()
            and "child" in events.read_text()
        ):
            raise SystemExit(75)
        if kind == "nodes":
            print("ns1033171")
        elif kind == "namespace":
            pass
        elif kind == "pods" and "-A" in args:
            items = []
            if os.environ.get("FAKE_ARC_LISTENER") == "1":
                items.append({
                    "metadata": {
                        "namespace": "arc-runners",
                        "labels": {"app.kubernetes.io/component": "listener"},
                    },
                    "status": {"phase": "Running"},
                })
            drain_kind = os.environ.get("FAKE_ARC_DRAIN")
            drain_marker = resource_path("arc-drained")
            if drain_kind and not drain_marker.exists():
                if drain_kind == "runner":
                    namespace = "arc-runners"
                    labels = {"actions-ephemeral-runner": "True"}
                elif drain_kind == "import":
                    namespace, labels = "arc-import-jobs", {}
                elif drain_kind == "mrf":
                    namespace, labels = "arc-mrf-jobs", {}
                else:
                    raise SystemExit("unexpected ARC drain fixture")
                items.append({
                    "metadata": {"namespace": namespace, "labels": labels},
                    "status": {"phase": "Running"},
                })
                drain_marker.touch()
                event("arc_active_" + drain_kind)
            late_call = int(os.environ.get("FAKE_ARC_LATE_CALL", "0"))
            late_counter = resource_path("arc-call-count")
            call_count = int(late_counter.read_text()) + 1 if late_counter.exists() else 1
            late_counter.write_text(str(call_count))
            if late_call == call_count:
                items.append({
                    "metadata": {
                        "namespace": "arc-runners",
                        "labels": {"actions-ephemeral-runner": "true"},
                    },
                    "status": {"phase": "Running"},
                })
                event("arc_late_" + str(call_count))
            if (
                os.environ.get("FAKE_ARC_AFTER_CHILD") == "1"
                and events.exists()
                and "child" in events.read_text()
            ):
                items.append({
                    "metadata": {
                        "namespace": "arc-runners",
                        "labels": {"actions-ephemeral-runner": "true"},
                    },
                    "status": {"phase": "Running"},
                })
            print(json.dumps({"items": items}))
        elif kind == "jobs,pods":
            event("zero_sample")
            active_after_child = (
                os.environ.get("FAKE_ACTIVE_WORK_AFTER_CHILD") == "1"
                and events.exists()
                and "child" in events.read_text()
            )
            if os.environ.get("FAKE_ACTIVE_WORK") == "1" or active_after_child:
                print('{"items":[{"kind":"Pod","status":{"phase":"Running"}}]}')
            else:
                print('{"items":[]}')
        elif kind == "deployment" and args[2] == os.environ["FAKE_IMPORT_SCHEDULER"]:
            scheduler_drift = (
                os.environ.get("FAKE_SCHEDULER_DRIFT_AFTER_CHILD") == "1"
                and events.exists()
                and "child" in events.read_text()
            )
            print("1" if scheduler_drift else "0", end="")
        elif kind in {"job", "configmap"}:
            if (
                kind == "job"
                and args[2] == "seed-import-nodes"
                and os.environ.get("FAKE_SEED_REAPPEAR") == "cleanup"
                and events.exists()
                and "policy_delete" in events.read_text()
            ):
                print("job/seed-import-nodes")
            elif resource_path(kind).exists():
                print(f"{kind}/{args[2]}")
        elif kind == "pods":
            if (
                resource_path("pod").exists()
                or os.environ.get("FAKE_PREEXISTING_CENSUS_POD") == "1"
            ):
                print("pod/plan-pricing-v3-census-test-orphan")
        elif kind == "pod":
            pass
        else:
            file_kind = {
                "resourcequota": "quota",
                "validatingadmissionpolicy": "policy",
                "validatingadmissionpolicybinding": "binding",
            }.get(kind)
            if file_kind is None or not resource_path(file_kind).exists():
                if os.environ.get("FAKE_PREEXISTING") == file_kind:
                    print(f"{kind}/{args[2]}")
            elif "name" in args[-1]:
                print(f"{kind}/{args[2]}")
            elif "jsonpath" in args[-1]:
                deleting = resource_path(file_kind + "-deleting")
                if deleting.exists():
                    delete_reads = resource_path(file_kind + "-delete-reads")
                    count = int(delete_reads.read_text()) if delete_reads.exists() else 0
                    if count == 2:
                        resource_path(file_kind).unlink(missing_ok=True); deleting.unlink(); delete_reads.unlink(missing_ok=True)
                    else:
                        delete_reads.write_text(str(count + 1)); print(file_kind + "-uid", end="")
                else:
                    drift = os.environ.get("FAKE_DRIFT") == file_kind \
                        and "binding_delete" in events.read_text()
                    print("drifted-uid" if drift else file_kind + "-uid", end="")
            elif args[-1] == "json":
                owner = os.environ["FAKE_OWNER"]
                uid = file_kind + "-uid"
                if file_kind == "quota":
                    payload = {
                        "metadata": {"name": args[2], "namespace": namespace,
                                     "uid": uid,
                                     "labels": {"healthporta.com/task-owner": owner}},
                        "spec": {"hard": {"pods": "0"}},
                    }
                elif file_kind == "policy":
                    payload = {
                        "metadata": {"name": args[2], "uid": uid,
                                     "labels": {"healthporta.com/task-owner": owner}},
                        "spec": {
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
                                "expression": (
                                    "request.namespace == 'healthporta-dev' && "
                                    "request.userInfo.username == "
                                    "'system:serviceaccount:healthporta-dev:"
                                    "engine-worker-launcher'"
                                ),
                            }],
                            "validations": [{
                                "expression": "false",
                                "message": os.environ["FAKE_DENIAL_MARKER"],
                                "reason": "Forbidden",
                            }],
                        },
                    }
                else:
                    payload = {
                        "metadata": {"name": args[2], "uid": uid,
                                     "labels": {"healthporta.com/task-owner": owner}},
                        "spec": {"policyName": os.environ["FAKE_POLICY"],
                                 "validationActions": ["Deny"],
                                 "matchResources": {
                                     "matchPolicy": "Exact",
                                     "namespaceSelector": {"matchLabels": {
                                         "kubernetes.io/metadata.name":
                                             "healthporta-dev"
                                     }},
                                 }},
                    }
                if (
                    os.environ.get("FAKE_SPEC_DRIFT") == file_kind
                    and events.exists()
                    and "child" in events.read_text()
                ):
                    if file_kind == "quota":
                        payload["spec"]["scopeSelector"] = {}
                    elif file_kind == "policy":
                        payload["spec"]["validations"][0]["expression"] = "true"
                    else:
                        payload["spec"]["matchResources"]["objectSelector"] = {}
                print(json.dumps(payload))
    else:
        raise SystemExit("unexpected kubectl operation: " + operation)
else:
    raise SystemExit("unexpected fake command: " + name)
"""


def _arguments(state_root: Path, repo: Path) -> list[str]:
    receipt_path = str(state_root / "run/census-receipt.json")
    return [
        "--owner-token",
        OWNER,
        "--state-dir",
        str(state_root / "run"),
        "--source-sha",
        SOURCE_SHA,
        "--repo-dir",
        str(repo),
        "--deadline-seconds",
        "900",
        "--census-job",
        "plan-pricing-v3-census-test",
        "--census-configmap",
        "plan-pricing-v3-census-src-test",
        "--census-receipt",
        receipt_path,
        "--drain-deployment",
        "control-api",
        "--import-scheduler-deployment",
        "control-scheduler",
        "--import-node-id",
        "plan-node",
        "--import-token-env",
        "TEST_IMPORT_TOKEN",
        "--",
        "census-child",
        "--receipt",
        receipt_path,
    ]


def _fake_environment(
    tmp_path: Path, **overrides: str
) -> tuple[dict[str, str], Path, Path]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    dispatcher = fake_bin / "fake-command"
    dispatcher.write_text(_FAKE_COMMAND, encoding="utf-8")
    dispatcher.chmod(0o755)
    for command in (
        "census-child",
        "git",
        "hostname",
        "k3s",
        "setsid",
        "sleep",
        "systemctl",
        "systemd-run",
        "timeout",
    ):
        (fake_bin / command).symlink_to(dispatcher)
    fake_state = tmp_path / "fake-state"
    fake_state.mkdir()
    state_root = tmp_path / "envelopes"
    state_root.mkdir()
    checkout = tmp_path / "repo"
    checkout.mkdir()
    env_by_name = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DENIAL_MARKER": f"hp-pv3-census-deny-{OWNER}",
        "FAKE_IMPORT_NODE_ID": "plan-node",
        "FAKE_IMPORT_SCHEDULER": "control-scheduler",
        "FAKE_IMPORT_TOKEN_ENV": "TEST_IMPORT_TOKEN",
        "FAKE_OWNER": OWNER,
        "FAKE_POLICY": f"hp-pv3-census-{OWNER}.healthporta.com",
        "FAKE_QUOTA": f"hp-pv3-census-{OWNER}",
        "FAKE_SOURCE_SHA": SOURCE_SHA,
        "FAKE_STATE": str(fake_state),
        "HLTHPRT_PLAN_PRICING_V3_CENSUS_ENVELOPE_RUN": "run",
        "HLTHPRT_PLAN_PRICING_V3_CENSUS_STATE_ROOT": str(state_root),
        **overrides,
    }
    return env_by_name, state_root, checkout


def _receipt(state_root) -> dict:
    return json.loads((state_root / "run/envelope-receipt.json").read_text())


def _run_envelope(tmp_path, **overrides) -> tuple[subprocess.CompletedProcess, Path]:
    env_by_name, state_root, checkout = _fake_environment(tmp_path, **overrides)
    result = subprocess.run(
        ["/bin/bash", str(SCRIPT), "run", *_arguments(state_root, checkout)],
        env=env_by_name,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result, state_root
