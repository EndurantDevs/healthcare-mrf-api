# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Own a temporary native ARC acquisition drain inside the census envelope."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import time

SCHEMA = "healthporta.census.arc-drain.v1"
OWNER_LABEL = "healthporta.com/task-owner"
FENCE_RESOURCES = {
    "policy": "validatingadmissionpolicies",
    "binding": "validatingadmissionpolicybindings",
}
CAPACITY_KEYS = ("minRunners", "maxRunners")
COUNT_KEYS = ("currentReplicas", "pendingEphemeralRunners", "runningEphemeralRunners")

def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)

def _capacity(spec: dict, *, defaults: bool = False) -> tuple[int, int]:
    values = tuple(spec.get(key, 0 if defaults else None) for key in CAPACITY_KEYS)
    _require(all(type(value) is int and value >= 0 for value in values), "ARC capacity must be explicit nonnegative integers")
    _require(values[0] <= values[1], "ARC minimum exceeds maximum")
    return values

def _held_spec(spec: dict) -> dict:
    return {**spec, "minRunners": 0, "maxRunners": 0}

def _metadata_identity(item: dict) -> tuple:
    meta = item["metadata"]
    _require(all(meta.get(key) for key in ("name", "uid", "resourceVersion")), "Kubernetes resource identity is incomplete")
    return tuple(meta.get(key) for key in ("namespace", "name", "uid", "generation", "resourceVersion"))

def _is_controlled_by(item: dict, kind: str, uid: str) -> bool:
    return any(ref.get("controller") is True and ref.get("kind") == kind
               and ref.get("uid") == uid
               for ref in item["metadata"].get("ownerReferences", []))

def fence_manifests(owner: str, namespace: str) -> dict[str, dict]:
    """Fence ASR capacity and membership while leaving native status updates allowed."""
    name = f"hp-pv3-arc-{owner}.healthporta.com"
    metadata = {"name": name, "labels": {OWNER_LABEL: owner}}
    policy_spec_by_field = {
        "failurePolicy": "Fail",
        "matchConstraints": {
            "matchPolicy": "Exact", "namespaceSelector": {}, "objectSelector": {},
            "resourceRules": [{
                "apiGroups": ["actions.github.com"], "apiVersions": ["v1alpha1"],
                "operations": ["CREATE", "UPDATE", "DELETE"],
                "resources": ["autoscalingrunnersets"], "scope": "Namespaced"}],
        },
        "matchConditions": [{"name": "exact-arc-namespace",
                             "expression": f"request.namespace == '{namespace}'"}],
        "validations": [{
            "expression": "request.operation == 'UPDATE' && "
                          "has(object.spec.minRunners) && has(object.spec.maxRunners) && "
                          "object.spec.minRunners == 0 && object.spec.maxRunners == 0",
            "message": f"hp-pv3-arc-deny-{owner}", "reason": "Forbidden"}],
    }
    binding_spec_by_field = {
        "policyName": name, "validationActions": ["Deny"],
        "matchResources": {
            "matchPolicy": "Exact", "objectSelector": {},
            "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": namespace}}},
    }
    return {role: {"apiVersion": "admissionregistration.k8s.io/v1", "kind": kind,
                   "metadata": metadata, "spec": spec}
            for role, kind, spec in (("policy", "ValidatingAdmissionPolicy", policy_spec_by_field),
                                     ("binding", "ValidatingAdmissionPolicyBinding", binding_spec_by_field))}


class ArcDrain:
    """Durable intent precedes each mutation; uncertain results retain the hold."""

    def __init__(self, state: str | Path, owner: str, namespace: str = "arc-runners",
                 deadline_seconds: int = 300):
        _require(re.fullmatch(r"[a-z0-9][a-z0-9-]{6,30}[a-z0-9]", owner) is not None, "owner must be an 8-32 character DNS label")
        _require(namespace == "arc-runners", "only the census ARC namespace is supported")
        _require(type(deadline_seconds) is int and 0 < deadline_seconds <= 86400, "deadline must be 1-86400 seconds")
        self.path, self.owner, self.namespace = Path(state), owner, namespace
        self.deadline = time.monotonic() + deadline_seconds
        self.manifests = fence_manifests(owner, namespace)
        self.data: dict = {}
        self.inode: tuple[int, int] | None = None

    def _remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        _require(remaining > 0, "ARC acquisition drain deadline elapsed")
        return remaining

    def pause(self) -> None:
        """Wait briefly without crossing the operation deadline."""
        time.sleep(min(1, self._remaining()))

    def kubectl(self, *args: str, payload=None, check: bool = True):
        """Run one bounded API operation; never interpret an API error as absence."""
        limit = min(25, self._remaining())
        command_args = ["k3s", "kubectl", "--kubeconfig=/etc/rancher/k3s/k3s.yaml", f"--request-timeout={limit:.3f}s", *args]
        result = subprocess.run(command_args, input=None if payload is None else json.dumps(payload),
                                capture_output=True, text=True, timeout=limit, check=False)
        if check:
            _require(result.returncode == 0, f"kubectl failed: {result.stderr.strip()}")
        return result

    def _get(self, resource: str, name: str = "", *, namespace: str = "", optional=False):
        args = ["get", resource, *([name] if name else [])]
        args += ["-n", namespace] if namespace else []
        args += ["--ignore-not-found"] if optional else []
        text = self.kubectl(*args, "-o", "json").stdout.strip()
        _require(bool(text) or optional, "Kubernetes returned no resource")
        return json.loads(text) if text else None

    def _save(self, *, create: bool = False) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        if create:
            fd, temporary = os.open(self.path, flags, 0o600), None
        else:
            observed = self.path.lstat()
            _require((observed.st_dev, observed.st_ino) == self.inode, "ARC state identity changed")
            fd, temporary = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w") as target:
                json.dump(self.data, target, sort_keys=True, separators=(",", ":"))
                target.write("\n")
                target.flush()
                os.fsync(target.fileno())
            if temporary:
                os.replace(temporary, self.path)
            observed = self.path.lstat()
            self.inode = (observed.st_dev, observed.st_ino)
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)

    def _load(self) -> None:
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd) as source:
            observed = os.fstat(source.fileno())
            _require(stat.S_ISREG(observed.st_mode) and observed.st_size <= 1024 * 1024
                    and stat.S_IMODE(observed.st_mode) == 0o600
                    and observed.st_uid == os.geteuid(), "ARC state is not a private regular file")
            self.data = json.load(source)
            self.inode = (observed.st_dev, observed.st_ino)
        _require(self.data.get("schema") == SCHEMA and self.data.get("owner") == self.owner
                and self.data.get("namespace") == self.namespace, "ARC state ownership/schema mismatch")
        _require(all(type(self.data.get(key)) is bool for key in ("held", "restored")), "ARC state lifecycle flags are invalid")
        rows = self.data.get("original", [])
        _require(bool(rows) and len({row["name"] for row in rows}) == len(rows), "ARC original inventory is empty or duplicated")
        for row in rows:
            _capacity(row["spec"])
            _require(row.get("uid") and row.get("resourceVersion")
                    and type(row.get("generation")) is int, "ARC snapshot identity is incomplete")
        _require(set(self.data["changes"]) == {row["name"] for row in rows}
                and set(self.data["fences"]) == set(FENCE_RESOURCES), "ARC state inventory is invalid")

    def _snapshot(self) -> list[dict]:
        items = self._get("autoscalingrunnersets", namespace=self.namespace)["items"]
        _require(bool(items), "ARC inventory is empty")
        rows = []
        for item in items:
            _metadata_identity(item)
            meta = item["metadata"]
            _require(meta.get("namespace") == self.namespace and not meta.get("deletionTimestamp")
                    and type(meta.get("generation")) is int, "ARC set is not live")
            _capacity(item["spec"])
            rows.append({key: meta[key] for key in ("name", "uid", "resourceVersion", "generation")} | {"spec": item["spec"]})
        return sorted(rows, key=lambda row: row["name"])

    def _asrs(self) -> dict[str, dict]:
        items = self._get("autoscalingrunnersets", namespace=self.namespace)["items"]
        by_name = {item["metadata"]["name"]: item for item in items}
        _require(len(by_name) == len(items) == len(self.data["original"])
                and set(by_name) == {row["name"] for row in self.data["original"]},
                "ARC set inventory changed")
        for row in self.data["original"]:
            item = by_name[row["name"]]
            _metadata_identity(item)
            _require(item["metadata"]["uid"] == row["uid"]
                    and item["metadata"].get("namespace") == self.namespace
                    and not item["metadata"].get("deletionTimestamp"), "ARC set UID/liveness changed")
            _require(_held_spec(item["spec"]) == _held_spec(row["spec"]), "ARC non-capacity specification changed")
            _capacity(item["spec"])
        return by_name

    def _check_fence(self, role: str, item: dict) -> str:
        expected, meta = self.manifests[role], item.get("metadata", {})
        _require(item.get("apiVersion") == expected["apiVersion"] and item.get("kind") == expected["kind"]
                and meta.get("name") == expected["metadata"]["name"]
                and meta.get("labels", {}).get(OWNER_LABEL) == self.owner
                and not meta.get("deletionTimestamp") and bool(meta.get("uid"))
                and item.get("spec") == expected["spec"], "ARC admission fence identity/spec changed")
        return meta["uid"]

    def identity(self) -> dict:
        """Reject uncertain mutations or drift in the captured resource identities."""
        self._load()
        self._asrs()
        for role, record in self.data["fences"].items():
            _require(record["phase"] in {"no_attempt", "present", "deleted"}, "ARC admission mutation result is uncertain")
            item = self._get(FENCE_RESOURCES[role], self.manifests[role]["metadata"]["name"], optional=True)
            if record["phase"] == "present":
                _require(item is not None and self._check_fence(role, item) == record["uid"],
                        "ARC admission fence UID changed or disappeared")
            else:
                _require(item is None, "unexpected ARC admission fence exists")
        _require(all(record["phase"] in {"no_attempt", "confirmed", "restored"}
                    for record in self.data["changes"].values()), "ARC capacity mutation result is uncertain")
        return self.data

    def _create_fence(self, role: str) -> None:
        self.data["fences"][role] = {"phase": "intent"}
        self._save()
        item = json.loads(self.kubectl("create", "-f", "-", "-o", "json", payload=self.manifests[role]).stdout)
        self.data["fences"][role] = {"phase": "present", "uid": self._check_fence(role, item)}
        self._save()

    def _patch_capacity(self, item: dict, spec: dict, *, dry_run=False, check=True):
        meta = item["metadata"]
        patch_operations = [{"op": "test", "path": f"/metadata/{key}", "value": meta[key]} for key in ("uid", "resourceVersion")]
        patch_operations += [{"op": "test", "path": "/spec", "value": item["spec"]}]
        patch_operations += [{"op": "test", "path": f"/spec/{key}", "value": item["spec"][key]}
                  for key in CAPACITY_KEYS]
        patch_operations += [{"op": "replace", "path": f"/spec/{key}", "value": spec[key]}
                  for key in CAPACITY_KEYS]
        args = ["patch", "autoscalingrunnersets", meta["name"], "-n", self.namespace,
                "--type=json", "-p", json.dumps(patch_operations), "-o", "json"]
        args += ["--dry-run=server"] if dry_run else []
        return self.kubectl(*args, check=check)

    def _prove_admission(self) -> None:
        marker = f"hp-pv3-arc-deny-{self.owner}"
        while True:
            self.identity()
            item = self._asrs()[self.data["original"][0]["name"]]
            probe_spec_by_field = {**item["spec"], "minRunners": 0, "maxRunners": 1}
            result = self._patch_capacity(item, probe_spec_by_field, dry_run=True, check=False)
            if result.returncode:
                _require(re.search(rf"(?<![-\w]){re.escape(marker)}(?![-\w])", result.stderr) is not None,
                        "ARC dry-run did not return the exact owner denial marker")
                self.identity()
                return
            self.pause()

    def _zero(self, row: dict) -> None:
        item = self._asrs()[row["name"]]
        _require(item["spec"] == row["spec"], "ARC capacity changed before owned drain")
        if _capacity(row["spec"]) == (0, 0):
            return
        self.data["changes"][row["name"]] = {"phase": "intent", "resourceVersion": item["metadata"]["resourceVersion"]}
        self._save()
        result = json.loads(self._patch_capacity(item, _held_spec(row["spec"])).stdout)
        _require(result["metadata"]["uid"] == row["uid"] and result["spec"] == _held_spec(row["spec"]),
                "ARC drain mutation response is ambiguous")
        self.data["changes"][row["name"]] = {"phase": "confirmed", "resourceVersion": result["metadata"]["resourceVersion"]}
        self._save()

    def hold(self) -> dict:
        """Create an owned admission fence and drain the native ARC controllers."""
        original = self._snapshot()
        self.data = {"schema": SCHEMA, "owner": self.owner, "namespace": self.namespace,
                     "original": original, "held": False, "restored": False,
                     "fences": {role: {"phase": "no_attempt"} for role in FENCE_RESOURCES},
                     "changes": {row["name"]: {"phase": "no_attempt"} for row in original}}
        self._save(create=True)
        self.identity()
        for role in FENCE_RESOURCES:
            self._create_fence(role)
        self._prove_admission()
        for row in original:
            self.identity()
            self._zero(row)
        self._stable(held=True)
        self.data["held"] = True
        self._save()
        return self.data

    def _topology(self, asrs: dict[str, dict], *, held: bool) -> tuple | None:
        inventory = json.loads(self.kubectl("get", "autoscalinglisteners,ephemeralrunnersets", "-A", "-o", "json").stdout)["items"]
        pods = json.loads(self.kubectl("get", "pods", "-A", "-o", "json").stdout)["items"]
        listeners = [resource for resource in inventory if resource["kind"] == "AutoscalingListener"
                     and resource["spec"].get("autoscalingRunnerSetNamespace") == self.namespace]
        sets = [resource for resource in inventory if resource["kind"] == "EphemeralRunnerSet"
                and resource["metadata"].get("namespace") == self.namespace]
        if len(listeners) != len(asrs):
            return None
        seen, selected = set(), []
        for listener in listeners:
            name = listener["spec"].get("autoscalingRunnerSetName")
            if name not in asrs or name in seen:
                return None
            seen.add(name)
            linked = self._listener_graph(listener, asrs[name], sets, pods)
            if linked is None:
                return None
            selected.extend([listener, *linked])
        listener_pods = [pod for pod in pods
                         if pod["metadata"].get("labels", {}).get("app.kubernetes.io/component")
                         == "runner-scale-set-listener"
                         and pod["metadata"].get("labels", {}).get("actions.github.com/scale-set-namespace")
                         == self.namespace]
        expected_pods = {(resource["metadata"]["namespace"], resource["metadata"]["name"]) for resource in listeners}
        if {(pod["metadata"]["namespace"], pod["metadata"]["name"])
                for pod in listener_pods} != expected_pods:
            return None
        if held:
            pending = self._get("ephemeralrunners", namespace=self.namespace)["items"]
            if pending or any(not self._is_healthy_set(resource, zero=True) for resource in sets):
                return None
            if any(self._is_active_runner(pod) for pod in pods):
                return None
        return tuple(sorted((_metadata_identity(resource) for resource in [*asrs.values(), *sets, *selected]),
                            key=lambda value: str(value)))

    def _listener_graph(self, listener: dict, asr: dict, sets: list, pods: list) -> list | None:
        meta, spec = listener["metadata"], listener["spec"]
        if meta.get("deletionTimestamp") or meta.get("generation") != 1:
            return None
        if _capacity(spec, defaults=True) != _capacity(asr["spec"]):
            return None
        scale_id = asr["metadata"].get("annotations", {}).get("actions.github.com/runner-scale-set-id")
        if not scale_id or str(spec.get("runnerScaleSetId")) != str(scale_id):
            return None
        live_sets = [runner_set for runner_set in sets if not runner_set["metadata"].get("deletionTimestamp")
                and _is_controlled_by(runner_set, "AutoscalingRunnerSet", asr["metadata"]["uid"])]
        if len(live_sets) != 1 or live_sets[0]["metadata"]["name"] != spec.get("ephemeralRunnerSetName"):
            return None
        if not self._is_healthy_set(live_sets[0]):
            return None
        listener_pods = [pod for pod in pods if pod["metadata"].get("namespace") == meta["namespace"]
                 and pod["metadata"]["name"] == meta["name"]]
        if len(listener_pods) != 1:
            return None
        pod = listener_pods[0]
        if pod["metadata"].get("deletionTimestamp") or pod.get("status", {}).get("phase") != "Running":
            return None
        if not _is_controlled_by(pod, "AutoscalingListener", meta["uid"]):
            return None
        containers = [status for status in pod.get("status", {}).get("containerStatuses", []) if status.get("name") == "listener"]
        if len(containers) != 1 or containers[0].get("ready") is not True:
            return None
        if not isinstance(containers[0].get("state", {}).get("running"), dict):
            return None
        return [live_sets[0], pod]

    @staticmethod
    def _is_healthy_set(item: dict, *, zero=False) -> bool:
        values = [item.get("spec", {}).get("replicas", 0)]
        values += [item.get("status", {}).get(key, None if key == "currentReplicas" else 0)
                   for key in COUNT_KEYS]
        return all(type(value) is int and (value == 0 if zero else value >= 0) for value in values)

    def _is_active_runner(self, pod: dict) -> bool:
        meta = pod["metadata"]
        is_runner = str(meta.get("labels", {}).get("actions-ephemeral-runner", "")).lower() == "true"
        is_runner |= any(ref.get("kind") == "EphemeralRunner" for ref in meta.get("ownerReferences", []))
        return (meta.get("namespace") == self.namespace and is_runner
                and pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"})

    def _stable(self, *, held: bool) -> None:
        previous, count = None, 0
        while True:
            self.identity()
            asrs = self._asrs()
            spec_by_name = {row["name"]: _held_spec(row["spec"]) if held else row["spec"] for row in self.data["original"]}
            _require(all(asrs[name]["spec"] == spec for name, spec in spec_by_name.items()),
                    "ARC capacity does not match the owned lifecycle phase")
            current = self._topology(asrs, held=held)
            count = count + 1 if current is not None and current == previous else 0
            if count >= 2:
                return
            previous = current
            self.pause()

    def verify(self) -> dict:
        """Require three stable samples of the owned zero-capacity acquisition graph."""
        self.identity()
        _require(self.data["held"] and not self.data["restored"], "ARC drain is not held")
        _require(all(record["phase"] == "present" for record in self.data["fences"].values()),
                "ARC acquisition fence is not active")
        self._stable(held=True)
        return self.data

    def _delete_fence(self, role: str) -> None:
        record = self.data["fences"][role]
        if record["phase"] in {"no_attempt", "deleted"}:
            return
        self.identity()
        name, uid = self.manifests[role]["metadata"]["name"], record["uid"]
        resource = FENCE_RESOURCES[role]
        self.data["fences"][role]["phase"] = "delete_intent"
        self._save()
        self.kubectl("delete", f"--raw=/apis/admissionregistration.k8s.io/v1/{resource}/{name}",
                     "-f", "-", payload={"apiVersion": "v1", "kind": "DeleteOptions",
                                          "preconditions": {"uid": uid},
                                          "propagationPolicy": "Foreground"})
        while True:
            item = self._get(resource, name, optional=True)
            if item is None:
                break
            _require(item["metadata"]["uid"] == uid, "ARC fence replaced during deletion")
            self.pause()
        self.data["fences"][role]["phase"] = "deleted"
        self._save()

    def _restore_capacity(self, row: dict) -> None:
        name = row["name"]
        while True:
            item = self._asrs()[name]
            if item["spec"] == row["spec"]:
                self.data["changes"][name]["phase"] = "restored"
                self._save()
                return
            _require(self.data["changes"][name]["phase"] == "confirmed"
                    and item["spec"] == _held_spec(row["spec"]), "ARC capacity is not an owned change")
            self.data["changes"][name]["phase"] = "restore_intent"
            self._save()
            result = self._patch_capacity(item, row["spec"], check=False)
            if result.returncode and re.search(rf"(?<![-\w])hp-pv3-arc-deny-{self.owner}(?![-\w])", result.stderr):
                self.data["changes"][name]["phase"] = "confirmed"
                self._save()
                self.pause()
                continue
            _require(result.returncode == 0, "ARC capacity restoration result is uncertain")
            restored = json.loads(result.stdout)
            _require(restored["metadata"]["uid"] == row["uid"] and restored["spec"] == row["spec"],
                    "ARC capacity restoration response is ambiguous")
            self.data["changes"][name]["phase"] = "restored"
            self._save()
            return

    def restore(self) -> dict:
        """Restore confirmed changes after the parent proves census and quota absence."""
        self.identity()
        asrs = self._asrs()
        for row in self.data["original"]:
            spec = asrs[row["name"]]["spec"]
            has_owned_change = self.data["changes"][row["name"]]["phase"] == "confirmed"
            _require(spec == row["spec"] or (has_owned_change and spec == _held_spec(row["spec"])),
                    "ARC capacity drift blocks restoration")
        for role in ("binding", "policy"):
            self._delete_fence(role)
        for row in self.data["original"]:
            self._restore_capacity(row)
        self._stable(held=False)
        self.data.update(restored=True, held=False)
        self._save()
        return self.data


def main(argv=None) -> int:
    """Dispatch the bounded lifecycle command and emit its durable outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("hold", "verify", "identity", "restore"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--namespace", default="arc-runners")
    parser.add_argument("--deadline-seconds", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        drain = ArcDrain(args.state, args.owner, args.namespace, args.deadline_seconds)
        result = getattr(drain, args.command)()
        print(json.dumps({key: result[key] for key in ("schema", "owner", "namespace", "held", "restored")}))
        return 0
    except (RuntimeError, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
        print(f"ARC acquisition drain failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
