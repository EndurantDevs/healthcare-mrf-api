"""Native drain lifecycle against a stateful API model, not live CEL admission."""

from copy import deepcopy
import json
from pathlib import Path
import subprocess

import pytest

from scripts.research import plan_pricing_projection_v3_census_arc as arc


class Cluster:
    """Model atomic JSON Patch, immutable UIDs, and ARC listener replacement."""

    def __init__(self):
        self.revision = 1
        self.asrs = {}
        for name, maximum in (("ci-a", 3), ("ci-b", 2)):
            self.asrs[name] = {
                "apiVersion": "actions.github.com/v1alpha1",
                "kind": "AutoscalingRunnerSet",
                "metadata": {"name": name, "namespace": "arc-runners",
                             "uid": name + "-uid", "resourceVersion": "1", "generation": 1,
                             "annotations": {"actions.github.com/runner-scale-set-id": "7"}},
                "spec": {"minRunners": 0, "maxRunners": maximum,
                         "githubConfigUrl": "https://github.com/example",
                         "runnerScaleSetName": name, "runnerGroup": "trusted",
                         "template": {"spec": {"containers": [{"name": "runner"}]}}},
            }
        self.fences = {}
        self.events = []
        self.bad_graph = None
        self.runner_objects = []
        self.fail_patch = None
        self.fail_create = None
        self.conflict_once = False
        self.pauses = 0

    def _listener_resources(self, name, asr):
        """Return the resources ARC would recreate for the current ASR generation."""
        uid = name + "-listener-" + str(asr["metadata"]["generation"])
        listener_mapping = {
                "kind": "AutoscalingListener",
                "metadata": {"name": name + "-listener", "namespace": "arc-systems",
                             "uid": uid, "generation": 1, "resourceVersion": "1"},
                "spec": {"autoscalingRunnerSetName": name,
                         "autoscalingRunnerSetNamespace": "arc-runners",
                         "runnerScaleSetId": 7, "ephemeralRunnerSetName": name + "-ers",
                         "minRunners": asr["spec"]["minRunners"],
                         "maxRunners": asr["spec"]["maxRunners"]},
        }
        ers_mapping = {
                "kind": "EphemeralRunnerSet",
                "metadata": {"name": name + "-ers", "namespace": "arc-runners",
                             "uid": name + "-ers-uid", "resourceVersion": "1",
                             "ownerReferences": [{"kind": "AutoscalingRunnerSet",
                                 "name": name, "uid": asr["metadata"]["uid"], "controller": True}]},
                "spec": {},
                "status": {"currentReplicas": 0, "pendingEphemeralRunners": 0,
                           "runningEphemeralRunners": 0},
        }
        pod_mapping = {"kind": "Pod", "metadata": {
                "name": name + "-listener", "namespace": "arc-systems", "uid": uid + "-pod",
                "resourceVersion": "1", "labels": {
                    "app.kubernetes.io/component": "runner-scale-set-listener",
                    "actions.github.com/scale-set-namespace": "arc-runners"},
                "ownerReferences": [{"kind": "AutoscalingListener", "name": name + "-listener",
                                     "uid": uid, "controller": True}]},
                "status": {"phase": "Running", "containerStatuses": [
                    {"name": "listener", "state": {"running": {}}, "ready": True}]}}
        return listener_mapping, ers_mapping, pod_mapping

    def graph(self):
        """Return native resources with the selected stale or unhealthy condition."""
        listeners, sets, pods = [], [], []
        for name, asr in self.asrs.items():
            listener_mapping, ers_mapping, pod_mapping = self._listener_resources(name, asr)
            if self.bad_graph == "wrong-ers-owner":
                ers_mapping["metadata"]["ownerReferences"][0]["uid"] = "replaced-asr"
            if self.bad_graph == "missing-ers":
                listener_mapping["spec"]["ephemeralRunnerSetName"] = "deleted-ers"
            if self.bad_graph == "old-pod":
                pod_mapping["metadata"]["ownerReferences"][0]["uid"] = "old-listener"
            if self.bad_graph == "pending-ers":
                ers_mapping["status"]["pendingEphemeralRunners"] = 1
            if self.bad_graph == "missing-current":
                del ers_mapping["status"]["currentReplicas"]
            if self.bad_graph == "null-pending":
                ers_mapping["status"]["pendingEphemeralRunners"] = None
            if self.bad_graph == "wrong-scale-id":
                listener_mapping["spec"]["runnerScaleSetId"] = 99
            if self.bad_graph == "unready-listener":
                pod_mapping["status"]["containerStatuses"][0].update(
                    ready=False, state={"waiting": {"reason": "CrashLoopBackOff"}})
            listeners.append(listener_mapping)
            sets.append(ers_mapping)
            pods.append(pod_mapping)
        if self.bad_graph == "worker-pod":
            pods.append({"metadata": {"name": "worker", "namespace": "arc-runners",
                        "labels": {"actions-ephemeral-runner": "true"}},
                         "status": {"phase": "Running"}})
        if self.bad_graph == "orphan-listener":
            orphan = deepcopy(pods[0])
            orphan["metadata"].update(name="previous-listener", uid="previous-listener-pod")
            orphan["metadata"]["ownerReferences"][0].update(
                name="previous-listener", uid="deleted-listener")
            pods.append(orphan)
        return listeners, sets, pods

    def admitted(self, operation, obj):
        """Evaluate only the deliberately small predicate asserted below."""
        bound = any(item["kind"] == "ValidatingAdmissionPolicyBinding"
                    for item in self.fences.values())
        if not bound or obj.get("kind") != "AutoscalingRunnerSet":
            return True
        return operation == "UPDATE" and all(
            type(obj["spec"].get(key)) is int and obj["spec"][key] == 0
            for key in ("minRunners", "maxRunners")
        )

    def call(self, *args, payload=None, check=True):
        """Return kubectl-shaped results and raise on checked API failures."""
        args = list(args)
        code, error, value = 0, "", None
        try:
            value = self._dispatch_request(args, payload)
        except (KeyError, ValueError) as exc:
            code, error = 1, str(exc)
        output = "" if value is None else json.dumps(value)
        result = subprocess.CompletedProcess(args, code, output, error)
        if code and check:
            raise RuntimeError(error)
        return result

    def _dispatch_request(self, args, payload):
        operation = args[0]
        is_dry_run = any(arg.startswith("--dry-run") for arg in args)
        if operation == "get":
            return self._get_resources(args)
        if operation == "create":
            return self._create_fence(payload, is_dry_run)
        if operation == "patch":
            return self._patch_asr(args, is_dry_run)
        if operation == "delete":
            return self._delete_fence(args, payload)
        raise AssertionError(f"unmodeled API operation: {args}")

    def _get_resources(self, args):
        resource = args[1]
        name = args[2] if len(args) > 2 and not args[2].startswith("-") else None
        listeners, sets, pods = self.graph()
        objects_by_resource = {"autoscalingrunnersets": list(self.asrs.values()),
                               "autoscalinglisteners": listeners, "ephemeralrunnersets": sets,
                               "pods": pods, "ephemeralrunners": self.runner_objects}
        if resource in ("validatingadmissionpolicies", "validatingadmissionpolicybindings"):
            return deepcopy(self.fences.get((resource, name)))
        if name:
            return deepcopy(self.asrs[name])
        return {"items": deepcopy([item for kind in resource.split(",")
                                    for item in objects_by_resource[kind]])}

    def _create_fence(self, payload, is_dry_run):
        fence_mapping = deepcopy(payload)
        if not self.admitted("CREATE", fence_mapping):
            raise ValueError("hp-pv3-arc-deny-test-owner")
        if is_dry_run:
            return fence_mapping
        name = fence_mapping["metadata"]["name"]
        resource = {"ValidatingAdmissionPolicy": "validatingadmissionpolicies",
                    "ValidatingAdmissionPolicyBinding": "validatingadmissionpolicybindings"}[fence_mapping["kind"]]
        if self.fail_create == fence_mapping["kind"]:
            raise ValueError("injected create failure")
        if (resource, name) in self.fences:
            raise ValueError("AlreadyExists")
        fence_mapping["metadata"].update(uid=name + "-uid", resourceVersion="1", generation=1)
        self.fences[resource, name] = fence_mapping
        self.events.append(("create", fence_mapping["kind"]))
        return fence_mapping

    def _patch_asr(self, args, is_dry_run):
        """Apply JSON tests atomically before admitting and recording an ASR update."""
        name = args[2]
        current_mapping = self.asrs[name]
        patch = json.loads(args[args.index("-p") + 1])
        if self.conflict_once and not is_dry_run:
            self.conflict_once = False
            current_mapping["metadata"]["resourceVersion"] = "999"
        updated_mapping = deepcopy(current_mapping)
        for change in patch:
            parts = change["path"].strip("/").split("/")
            target_mapping = updated_mapping
            for key in parts[:-1]:
                target_mapping = target_mapping[key]
            key = parts[-1]
            if change["op"] == "test":
                if target_mapping.get(key) != change["value"]:
                    raise ValueError("JSON test failed")
            elif change["op"] == "remove":
                del target_mapping[key]
            else:
                target_mapping[key] = change["value"]
        if not self.admitted("UPDATE", updated_mapping):
            raise ValueError("hp-pv3-arc-deny-test-owner")
        if is_dry_run:
            return updated_mapping
        if self.fail_patch and self.fail_patch(name, updated_mapping):
            raise ValueError("injected patch failure")
        self.revision += 1
        updated_mapping["metadata"]["resourceVersion"] = str(self.revision)
        updated_mapping["metadata"]["generation"] += 1
        self.asrs[name] = updated_mapping
        self.events.append(("patch", name, updated_mapping["spec"]["maxRunners"]))
        return updated_mapping

    def _delete_fence(self, args, payload):
        endpoint = next(arg.removeprefix("--raw=") for arg in args if arg.startswith("--raw="))
        name = endpoint.rsplit("/", 1)[-1]
        resource = endpoint.rsplit("/", 2)[-2]
        fence_mapping = self.fences.get((resource, name))
        if fence_mapping is None:
            return None
        if payload["preconditions"]["uid"] != fence_mapping["metadata"]["uid"]:
            raise ValueError("UID precondition failed")
        self.events.append(("delete", fence_mapping["kind"]))
        del self.fences[resource, name]
        return {"status": "Success"}


@pytest.fixture
def lifecycle(tmp_path: Path, monkeypatch):
    cluster = Cluster()
    monkeypatch.setattr(arc.ArcDrain, "kubectl", lambda _, *args, **kw: cluster.call(*args, **kw))
    def bounded_pause(_):
        cluster.pauses += 1
        if cluster.pauses > 12:
            raise RuntimeError("model retry budget elapsed")
    monkeypatch.setattr(arc.ArcDrain, "pause", bounded_pause)
    drain = arc.ArcDrain(tmp_path / "arc-state.json", "test-owner", deadline_seconds=30)
    return drain, cluster


def test_native_hold_restore_preserves_desired_capacity_and_listener_health(lifecycle):
    drain, cluster = lifecycle
    original_spec_by_name = {name: deepcopy(item["spec"]) for name, item in cluster.asrs.items()}
    drain.hold()
    drain.verify()
    assert all(item["spec"]["maxRunners"] == 0 for item in cluster.asrs.values())
    assert not cluster.runner_objects
    assert all(pod["metadata"]["namespace"] == "arc-systems" for pod in cluster.graph()[2])
    drain.restore()
    assert {name: item["spec"] for name, item in cluster.asrs.items()} == original_spec_by_name
    assert not cluster.fences


def test_policy_predicate_denies_create_delete_and_nonzero_update():
    manifest = arc.fence_manifests("test-owner", "arc-runners")
    policy, binding = manifest["policy"], manifest["binding"]
    assert policy["spec"]["failurePolicy"] == "Fail"
    assert binding["spec"]["validationActions"] == ["Deny"]
    rule = policy["spec"]["matchConstraints"]["resourceRules"][0]
    assert set(rule["operations"]) == {"CREATE", "UPDATE", "DELETE"}
    assert rule["resources"] == ["autoscalingrunnersets"]
    expression = " ".join(policy["spec"]["validations"][0]["expression"].split())
    assert expression == ("request.operation == 'UPDATE' && has(object.spec.minRunners) && "
                          "has(object.spec.maxRunners) && object.spec.minRunners == 0 && "
                          "object.spec.maxRunners == 0")


def test_flux_cannot_restore_positive_capacity_while_held(lifecycle):
    drain, cluster = lifecycle
    drain.hold()
    intent = deepcopy(cluster.asrs["ci-a"])
    intent["spec"]["maxRunners"] = 3
    assert not cluster.admitted("UPDATE", intent)
    assert not cluster.admitted("DELETE", intent)
    assert not cluster.admitted("CREATE", intent)
    assert cluster.asrs["ci-a"]["spec"]["maxRunners"] == 0


@pytest.mark.parametrize("damage", ["missing-ers", "wrong-ers-owner", "old-pod", "pending-ers",
                                    "missing-current", "null-pending", "wrong-scale-id",
                                    "unready-listener", "worker-pod", "orphan-listener"])
def test_stale_graph_or_pending_work_rejects_quiescence(lifecycle, damage):
    drain, cluster = lifecycle
    drain.hold()
    cluster.bad_graph = damage
    with pytest.raises(RuntimeError):
        drain.verify()
    assert cluster.fences


@pytest.mark.parametrize("damage", ["deleted", "unknown", "uid", "spec", "runner"])
def test_identity_or_inventory_drift_preserves_hold(lifecycle, damage):
    drain, cluster = lifecycle
    drain.hold()
    if damage == "deleted":
        del cluster.asrs["ci-a"]
    elif damage == "unknown":
        cluster.asrs["unknown"] = deepcopy(cluster.asrs["ci-a"])
        cluster.asrs["unknown"]["metadata"]["name"] = "unknown"
    elif damage == "uid":
        cluster.asrs["ci-a"]["metadata"]["uid"] = "replacement"
    elif damage == "spec":
        cluster.asrs["ci-a"]["spec"]["runnerGroup"] = "different"
    else:
        cluster.runner_objects = [{"metadata": {"name": "pending-runner"}}]
    with pytest.raises(RuntimeError):
        drain.verify()
    assert cluster.fences


def test_partial_hold_failure_never_claims_success(lifecycle):
    drain, cluster = lifecycle
    cluster.fail_patch = lambda name, _: name == "ci-b"
    with pytest.raises(RuntimeError):
        drain.hold()
    assert cluster.asrs["ci-a"]["spec"]["maxRunners"] == 0
    assert cluster.asrs["ci-b"]["spec"]["maxRunners"] == 2
    with pytest.raises(RuntimeError):
        drain.identity()


def test_create_error_preserves_unmodified_capacity(lifecycle):
    drain, cluster = lifecycle
    cluster.fail_create = "ValidatingAdmissionPolicyBinding"
    with pytest.raises(RuntimeError):
        drain.hold()
    assert [item["spec"]["maxRunners"] for item in cluster.asrs.values()] == [3, 2]


def test_interrupted_restoration_does_not_claim_cleanup(lifecycle):
    drain, cluster = lifecycle
    drain.hold()
    cluster.fail_patch = lambda name, item: name == "ci-b" and item["spec"]["maxRunners"] > 0
    with pytest.raises(RuntimeError):
        drain.restore()
    assert cluster.asrs["ci-b"]["spec"]["maxRunners"] == 0


def test_resource_version_race_never_blindly_patches(lifecycle):
    drain, cluster = lifecycle
    cluster.conflict_once = True
    with pytest.raises(RuntimeError):
        drain.hold()
    assert [item["spec"]["maxRunners"] for item in cluster.asrs.values()] == [3, 2]
