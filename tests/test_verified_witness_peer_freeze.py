# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional peer tests use a declared, hash-bound package from the frozen copy."""

import hashlib
import json
import sys
from uuid import uuid4

import pytest

from tests.verified_witness_readiness_refusal_support import frozen_ic_recovery


def _freeze(tmp_path, monkeypatch, package):
    """Create matching synthetic packages and an explicit frozen descriptor."""
    origin, copied = tmp_path / "origin", tmp_path / "copy"
    hashes_by_path = {}
    for filename, content in {
        "__init__.py": "",
        "registry_membership_inputs.py": "KIND = 'inputs'\n",
        "registry_membership_refresh_dispatch.py": "KIND = 'dispatch'\n",
    }.items():
        relative = package + "/" + filename
        for root in (origin, copied):
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        hashes_by_path[relative] = hashlib.sha256(content.encode()).hexdigest()
    descriptor = tmp_path / "freeze.json"
    descriptor.write_text(
        json.dumps(
            {
                "ic_root": str(origin),
                "ic_snapshot_root": str(copied),
                "ic_sha256": hashes_by_path,
                "module_package": package,
            }
        )
    )
    monkeypatch.setenv("NETWORK_IC_TEST_ROOT", str(origin))
    monkeypatch.setenv("NETWORK_IC_TEST_FREEZE", str(descriptor))
    monkeypatch.setattr(sys, "path", sys.path[:])
    return origin, copied


def test_declared_peer_loads_only_the_matching_frozen_copy(tmp_path, monkeypatch):
    package = "synthetic_peer_" + uuid4().hex
    _, copied = _freeze(tmp_path, monkeypatch, package)
    try:
        peer = frozen_ic_recovery()
        assert peer.inputs.KIND == "inputs" and peer.dispatch.KIND == "dispatch"
        assert str(copied) in sys.path
        assert peer.inputs.__file__.startswith(str(copied) + "/")
        assert peer.dispatch.__file__.startswith(str(copied) + "/")
    finally:
        for name in tuple(sys.modules):
            if name == package or name.startswith(package + "."):
                sys.modules.pop(name)


def test_changed_copy_refuses_before_import(tmp_path, monkeypatch):
    package = "synthetic_peer_" + uuid4().hex
    _, copied = _freeze(tmp_path, monkeypatch, package)
    (copied / package / "registry_membership_inputs.py").write_text("KIND = 'changed'\n")
    with pytest.raises(AssertionError):
        frozen_ic_recovery()
    assert not any(name == package or name.startswith(package + ".") for name in sys.modules)
