"""Kernel process death/file limit and injected storage failure during state persistence."""

import errno
import json
import os
import subprocess
import sys

import pytest

from pg_logical_repl.state import write_private

pytestmark = pytest.mark.quality


@pytest.mark.parametrize("boundary", ["file_fsync", "before_replace", "after_replace", "directory_fsync"])
def test_killed_state_writer_never_leaves_partial_json(tmp_path, boundary):
    path = tmp_path / "state.json"
    write_private(path, json.dumps(dict(generation="old")))
    program = """
import json,os,signal,sys
from pathlib import Path
from pg_logical_repl import state
boundary=sys.argv[2]
fsync,replace=state.os.fsync,state.os.replace
calls=0
def die(): os.kill(os.getpid(),signal.SIGKILL)
def sync(fd):
 global calls
 calls+=1
 fsync(fd)
 if (boundary=='file_fsync' and calls==1) or (boundary=='directory_fsync' and calls==2): die()
def rename(a,b):
 if boundary=='before_replace': die()
 replace(a,b)
 if boundary=='after_replace': die()
state.os.fsync=sync
state.os.replace=rename
state.write_private(Path(sys.argv[1]),json.dumps(dict(generation='new',payload='x'*100000)))
"""
    result = subprocess.run([sys.executable, "-c", program, str(path), boundary], timeout=10)
    assert result.returncode == -9
    value = json.loads(path.read_text())
    assert value["generation"] == ("old" if boundary in {"file_fsync", "before_replace"} else "new")


def test_real_kernel_file_size_limit_preserves_previous_state(tmp_path):
    path = tmp_path / "state.json"
    write_private(path, '{"generation":"old"}')
    program = """
import errno,resource,signal,sys
from pathlib import Path
from pg_logical_repl.state import write_private
signal.signal(signal.SIGXFSZ,signal.SIG_IGN)
resource.setrlimit(resource.RLIMIT_FSIZE,(64,64))
try: write_private(Path(sys.argv[1]),'x'*10000)
except OSError as exc:
 assert exc.errno==errno.EFBIG
else: raise AssertionError('Kernel file limit did not fire')
"""
    subprocess.run([sys.executable, "-c", program, str(path)], check=True, timeout=10)
    assert json.loads(path.read_text()) == dict(generation="old")
    assert not list(tmp_path.glob(".write-*"))


def test_enospc_before_rename_preserves_state(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    write_private(path, '{"generation":"old"}')

    def full(fd):
        raise OSError(errno.ENOSPC, "test quota exhausted")

    monkeypatch.setattr(os, "fsync", full)
    with pytest.raises(OSError) as error:
        write_private(path, '{"generation":"new"}')
    assert error.value.errno == errno.ENOSPC
    assert json.loads(path.read_text()) == dict(generation="old")
