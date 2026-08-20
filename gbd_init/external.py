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

External tools follow the GBD external-tool contract: invoked with ``--gbd`` they
emit a stream of ``<feature> <value>`` lines on stdout, plus two reserved lines
``status <success|timeout|memout>`` and ``runtime <seconds>``. ``--feature-names
--gbd`` prints ``<feature> [default]`` per line (a default marks a unique feature).

Resource limits are no longer part of the contract: gbd does not pass ``-t/-m/-f``
to tools and tools need not implement their own resource guards. Instead this wrapper
enforces the limits externally via ``resource.setrlimit`` in a ``preexec_fn`` callback
(CPU time, virtual memory, file size) plus a ``subprocess`` wall-clock timeout, and
maps limit breaches to the reserved ``status`` values (``timeout``/``memout``/
``fileout``).

Platform support: Linux enforces all three limits; macOS enforces CPU and file-size
limits but not the address-space (memory) limit; platforms without the ``resource``
module (e.g. Windows) get only the wall-clock timeout. Which limits take effect on the
current system is reported in ``gbd init``/``gbd transform`` ``--help`` (see
``gbd_core.util.resource_limits_help_note``).
"""

import shlex
import signal
import subprocess

from gbd_core.util import resource


class ExternalToolException(Exception):
    pass


def convert(value):
    """Convert a textual feature value to int/float when numeric, else leave as str."""
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            number = float(value)
            return int(number) if number.is_integer() else number
        except (TypeError, ValueError):
            return value


def _make_preexec_fn(limits):
    """Return a preexec_fn that applies rlimits in the child process before exec.

    Returns ``None`` when the platform has no ``resource`` module, so the caller
    spawns the tool without a preexec hook (only the wall-clock timeout applies).
    On macOS the address-space (memory) limit is skipped because the kernel ignores
    it. Which limits take effect per platform is surfaced in ``gbd``'s ``--help``
    (see ``gbd_core.util.resource_limits_help_note``).
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


def _run(cmd):
    """Run cmd without resource limits (used for metadata queries like --feature-names)."""
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except FileNotFoundError:
        raise ExternalToolException(f"External tool not found: {cmd[0]}")
    if proc.returncode != 0:
        raise ExternalToolException(f"{cmd[0]} failed (exit {proc.returncode}): {proc.stderr.strip()}")
    return proc.stdout


# Maps OS signal numbers to status strings returned to callers. Built defensively so
# the module stays importable on platforms lacking these POSIX signals.
_SIGNAL_STATUS = {}
for _signame, _status in (("SIGXCPU", "timeout"), ("SIGXFSZ", "fileout"), ("SIGKILL", "memout")):
    _sig = getattr(signal, _signame, None)
    if _sig is not None:
        _SIGNAL_STATUS[int(_sig)] = _status


def _run_limited(cmd, limits):
    """Run cmd under resource limits; returns ``(stdout, kill_status_or_None)``.

    Returns ``("", status)`` on resource-limit kills so callers can propagate the
    status without raising.  Raises ``ExternalToolException`` for tool-not-found or
    unexpected non-zero exits.
    """
    tlim = limits.get("tlim", 0)
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=_make_preexec_fn(limits),
            timeout=tlim if tlim > 0 else None,
        )
    except FileNotFoundError:
        raise ExternalToolException(f"External tool not found: {cmd[0]}")
    except subprocess.TimeoutExpired:
        return "", "timeout"
    if proc.returncode < 0:
        status = _SIGNAL_STATUS.get(-proc.returncode, "killed")
        return "", status
    if proc.returncode != 0:
        raise ExternalToolException(f"{cmd[0]} failed (exit {proc.returncode}): {proc.stderr.strip()}")
    return proc.stdout, None


def _parse(stdout):
    """Parse the gbd-format stream into ``(values, status)``; reserved lines are handled."""
    values, status = {}, "success"
    for line in stdout.splitlines():
        parts = line.split(maxsplit=1)
        if not parts:
            continue
        key = parts[0]
        value = parts[1].strip() if len(parts) > 1 else ""
        if key == "status":
            status = value
        elif key == "runtime":
            continue
        else:
            values[key] = value
    return values, status


def feature_names(tool):
    """Return the features a tool produces as ``[(name, default_or_None)]``.

    A present default denotes a unique (1:1) feature; its absence denotes a
    non-unique (1:n) feature (stored by gbd as ``(name, None)``).
    """
    features = []
    for line in _run([*shlex.split(tool), "--feature-names", "--gbd"]).splitlines():
        parts = line.split(maxsplit=1)
        if not parts:
            continue
        name = parts[0]
        default = parts[1].strip() if len(parts) > 1 else None
        features.append((name, default))
    return features


def run_extractor(tool, path, limits):
    """Run an extractor on ``path`` and return ``(values, status)``."""
    stdout, kill_status = _run_limited([*shlex.split(tool), "--gbd", path], limits)
    if kill_status is not None:
        return {}, kill_status
    return _parse(stdout)


def run_transformer(tool, path, output, compress, limits):
    """Run a transformer on ``path``, writing the instance to ``output`` (optionally
    compressed), and return the produced ``(values, status)`` metadata."""
    cmd = [*shlex.split(tool), "--gbd", "-o", output]
    if compress and compress != "none":
        cmd += ["-z", compress]
    cmd.append(path)
    stdout, kill_status = _run_limited(cmd, limits)
    if kill_status is not None:
        return {}, kill_status
    return _parse(stdout)
