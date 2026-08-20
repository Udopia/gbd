# MIT License

# Copyright (c) 2025 Ashlin Iser, Karlsruhe Institute of Technology (KIT)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

"""Driver for external GBD tools (extractors and transformers).

External tools follow the GBD external-tool contract:
They emit a stream of ``<feature> <value>`` lines on stdout,
and called with --feature-names they emit ``<feature> [default]`` lines.

The wrapper establishes the limits in a ``preexec_fn`` callback (runtime, memory, filesize).

Platform support: 
Linux enforces all three limits. 
MacOS enforces CPU and file-size limits but not the memory limit.
Platforms without the ``resource`` module (e.g. Windows) get only the wall-clock timeout.
"""

import shlex
import signal
import subprocess

from gbd_core.util import resource


class ExternalToolException(Exception):
    pass


def _make_preexec_fn(limits):
    """Return a preexec_fn that applies rlimits in the child process before exec.

    Returns ``None`` when the platform has no ``resource`` module, so the caller
    spawns the tool without a preexec hook and only the wall-clock timeout applies.
    On macOS the memory limit is ignored by the kernel.
    """
    if resource is None:
        return None
    tlim = limits.get("tlim", 0)
    mlim = limits.get("mlim", 0)
    flim = limits.get("flim", 0)
    def apply():
        if tlim > 0:
            resource.setrlimit(resource.RLIMIT_CPU, (tlim, tlim))
        if mlim > 0:
            mem_bytes = mlim * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (mem_bytes, mem_bytes))
        if flim > 0:
            file_bytes = flim * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_FSIZE, (file_bytes, file_bytes))
    return apply


# Maps OS signal numbers to status strings returned to callers.
# Built defensively so the module stays importable on non-POSIX platforms.
_SIGNAL_STATUS = {}
for _signame, _status in (("SIGXCPU", "timeout"), ("SIGXFSZ", "fileout"), ("SIGKILL", "memout")):
    _sig = getattr(signal, _signame, None)
    if _sig is not None:
        _SIGNAL_STATUS[int(_sig)] = _status


def _run(cmd, limits=None):
    """Run cmd, capture and return stdout.
    If resource limits are given, apply them in a preexec_fn callback.
    Raises ``ExternalToolException`` for tool-not-found or unexpected non-zero exits.
    """
    try:
        if limits is None:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        else:
            tlim = limits.get("tlim", 0)
            proc = subprocess.run(cmd, capture_output=True, text=True, preexec_fn=_make_preexec_fn(limits), timeout=tlim if tlim > 0 else None, check=False)
    except FileNotFoundError:
        raise ExternalToolException(f"External tool not found: {cmd[0]}")
    except subprocess.TimeoutExpired:
        return {}, "timeout"
    if proc.returncode < 0:
        return {}, _SIGNAL_STATUS.get(-proc.returncode, "killed")
    # Parse the gbd-format stream
    values = {}
    for line in proc.stdout.splitlines():
        parts = line.split(maxsplit=1)
        if not parts:
            continue
        key = parts[0]
        value = parts[1].strip() if len(parts) > 1 else ""
        values[key] = value
    return values, "success" if proc.returncode == 0 else "failure"


def feature_names(tool):
    """Return the features a tool produces as ``[(name, default_or_None)]``.

    According to GBD conventions, a present default denotes a unique (1:1) feature,
    and its absence denotes a non-unique (1:n) feature.
    """
    values, status = _run([*shlex.split(tool), "--feature-names"])
    if status != "success":
        raise ExternalToolException(f"Failed to run {tool} --feature-names: {status}")
    return [(name, value if value else None) for name, value in values.items()]


def run_external_tool(tool, path, limits=None, output=None, compress=None):
    """Run an external tool on ``path`` and return ``(values, status)``.
    If limits are given, apply them in a preexec_fn callback.
    ``output`` and ``compress`` control the output file path and compression for transformers; they are ignored for extractors.
    
    ``values`` is a dict of ``{feature: value}``.
    ``status`` is one of ``success``, ``failure``, ``timeout``, ``memout``, or ``fileout``.
    
    Raises ``ExternalToolException`` for tool-not-found or unexpected non-zero exits.
    """
    cmd = [*shlex.split(tool)]
    if output is not None:
        cmd += ["-o", output]
    if compress is not None and compress != "none":
        cmd += ["-z", compress]
    cmd.append(path)
    return _run(cmd, limits)
