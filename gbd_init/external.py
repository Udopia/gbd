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
"""

import shlex
import subprocess


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


def _limit_args(limits):
    return ["-t", str(limits.get("tlim", 0)), "-m", str(limits.get("mlim", 0)), "-f", str(limits.get("flim", 0))]


def _run(cmd):
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except FileNotFoundError:
        raise ExternalToolException("External tool not found: {}".format(cmd[0]))
    if proc.returncode != 0:
        raise ExternalToolException("{} failed (exit {}): {}".format(cmd[0], proc.returncode, proc.stderr.strip()))
    return proc.stdout


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
    return _parse(_run([*shlex.split(tool), "--gbd", *_limit_args(limits), path]))


def run_transformer(tool, path, output, compress, limits):
    """Run a transformer on ``path``, writing the instance to ``output`` (optionally
    compressed), and return the produced ``(values, status)`` metadata."""
    cmd = [*shlex.split(tool), "--gbd", *_limit_args(limits), "-o", output]
    if compress and compress != "none":
        cmd += ["-z", compress]
    cmd.append(path)
    return _parse(_run(cmd))
