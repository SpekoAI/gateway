from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import aiohttp

import speko_gateway


def test_source_import_without_distribution_metadata(tmp_path: Path) -> None:
    # Use the real package and installed dependencies, without its dist-info.
    # No metadata lookup or import function is mocked.
    isolated = tmp_path / "isolated"
    isolated.mkdir()
    shutil.copytree(
        Path(speko_gateway.__file__).parent,
        isolated / "speko_gateway",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    for entry in Path(aiohttp.__file__).parent.parent.iterdir():
        name = entry.name.lower().replace("-", "_")
        if name.startswith("speko_gateway") or entry.suffix == ".pth":
            continue
        (isolated / entry.name).symlink_to(entry, target_is_directory=entry.is_dir())

    script = """
import importlib.metadata
import json
import sys

def no_network(event, args):
    if event.startswith('socket.'):
        raise AssertionError('network is forbidden during source import')

sys.addaudithook(no_network)
assert not any('site-packages' in path for path in sys.path)
sys.path.insert(0, sys.argv[1])
try:
    importlib.metadata.version('speko-gateway')
except importlib.metadata.PackageNotFoundError:
    pass
else:
    raise AssertionError('test environment unexpectedly contains package metadata')

from speko_gateway.client import GatewayClient, USER_AGENT as local_marker
from speko_gateway.relay import RelayLLMClient, USER_AGENT as router_marker

assert GatewayClient.__module__ == 'speko_gateway.client'
assert RelayLLMClient.__module__ == 'speko_gateway.relay'
assert local_marker == router_marker == 'speko-gateway/unknown'
print(json.dumps({'metadata_absent': True, 'local_marker': local_marker,
                  'router_marker': router_marker}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script, str(isolated)],
        cwd=tmp_path,
        env={"PATH": os.defpath, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "metadata_absent": True,
        "local_marker": "speko-gateway/unknown",
        "router_marker": "speko-gateway/unknown",
    }
