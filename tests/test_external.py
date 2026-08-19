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

import os
import signal
import sys
import tempfile
import unittest

from gbd_init import external

try:
    import resource
except ImportError:
    resource = None

# Fake external tool: prints gbd feature-name metadata for --feature-names, otherwise
# emits a gbd-format value stream. Serves both feature_names() and run_extractor().
_TOOL_ECHO = """
import sys
if "--feature-names" in sys.argv:
    print("alpha 0")
    print("beta")
else:
    print("alpha 42")
    print("beta 7")
    print("status success")
    print("runtime 1")
"""

# Fake external tool: reports the soft resource limits it was launched with, so tests
# can verify the wrapper applied them in the child process.
_TOOL_LIMIT_REPORTER = """
import resource
print("cpu_soft", resource.getrlimit(resource.RLIMIT_CPU)[0])
print("mem_soft", resource.getrlimit(resource.RLIMIT_AS)[0])
print("fsize_soft", resource.getrlimit(resource.RLIMIT_FSIZE)[0])
print("status success")
print("runtime 0")
"""

# Fake external tool: sleeps far longer than any test time limit (triggers wall-clock).
_TOOL_SLEEP = """
import time
time.sleep(30)
"""

# Fake external tool: writes well past the file-size limit (triggers SIGXFSZ).
# CPython ignores SIGXFSZ by default (raising OSError instead); restore the default
# disposition so the child is killed by the signal like a native tool (e.g. gbdc).
_TOOL_BIG_WRITE = """
import os, signal
signal.signal(signal.SIGXFSZ, signal.SIG_DFL)
with open(os.environ["FAKE_OUT"], "wb") as f:
    chunk = b"x" * (1024 * 1024)
    for _ in range(50):
        f.write(chunk)
        f.flush()
"""


class ExternalPureTestCase(unittest.TestCase):
    """Platform-independent tests for the pure helpers."""

    def test_convert(self):
        self.assertEqual(external.convert("42"), 42)
        self.assertEqual(external.convert("3.5"), 3.5)
        self.assertEqual(external.convert("4.0"), 4)
        self.assertEqual(external.convert("foo"), "foo")

    def test_parse_extracts_status_and_skips_runtime(self):
        values, status = external._parse("a 1\nruntime 12\nb hello\nstatus timeout\n")
        self.assertEqual(values, {"a": "1", "b": "hello"})
        self.assertEqual(status, "timeout")

    def test_parse_defaults_to_success(self):
        values, status = external._parse("a 1\n")
        self.assertEqual(status, "success")

    def test_signal_status_mapping(self):
        if hasattr(signal, "SIGXCPU"):
            self.assertEqual(external._SIGNAL_STATUS[int(signal.SIGXCPU)], "timeout")
        if hasattr(signal, "SIGXFSZ"):
            self.assertEqual(external._SIGNAL_STATUS[int(signal.SIGXFSZ)], "fileout")
        if hasattr(signal, "SIGKILL"):
            self.assertEqual(external._SIGNAL_STATUS[int(signal.SIGKILL)], "memout")

    def test_no_resource_module_disables_preexec(self):
        original = external.resource
        external.resource = None
        try:
            self.assertIsNone(external._make_preexec_fn({"tlim": 1, "mlim": 1, "flim": 1}))
        finally:
            external.resource = original


@unittest.skipIf(sys.platform == "win32", "resource-based limits require a POSIX platform")
class ExternalSubprocessTestCase(unittest.TestCase):
    """Tests that spawn a fake tool under the resource-limiting wrapper."""

    def setUp(self):
        self._scripts = []

    def tearDown(self):
        for path in self._scripts:
            if os.path.exists(path):
                os.remove(path)

    def _script(self, body):
        fd, path = tempfile.mkstemp(suffix=".py", prefix="faketool_")
        with os.fdopen(fd, "w") as f:
            f.write(body)
        self._scripts.append(path)
        return "{} {}".format(sys.executable, path)

    def test_feature_names(self):
        tool = self._script(_TOOL_ECHO)
        self.assertEqual(external.feature_names(tool), [("alpha", "0"), ("beta", None)])

    def test_run_extractor_success(self):
        tool = self._script(_TOOL_ECHO)
        values, status = external.run_extractor(tool, "dummy.cnf", {"tlim": 5, "mlim": 100, "flim": 100})
        self.assertEqual(status, "success")
        self.assertEqual(values, {"alpha": "42", "beta": "7"})

    @unittest.skipIf(resource is None, "resource module unavailable")
    def test_limits_applied_in_child(self):
        tool = self._script(_TOOL_LIMIT_REPORTER)
        # mlim must exceed the child interpreter's own address-space footprint; the
        # limit is virtual (a ceiling), so a large value allocates nothing.
        tlim, mlim, flim = 7, 4096, 5  # seconds, MB, MB
        values, status = external.run_extractor(tool, "dummy.cnf", {"tlim": tlim, "mlim": mlim, "flim": flim})
        self.assertEqual(status, "success")
        self.assertEqual(external.convert(values["cpu_soft"]), tlim)
        self.assertEqual(external.convert(values["fsize_soft"]), flim * 1024 * 1024)
        if sys.platform == "linux":
            self.assertEqual(external.convert(values["mem_soft"]), mlim * 1024 * 1024)
        else:  # macOS ignores RLIMIT_AS; the wrapper leaves it unlimited
            self.assertEqual(external.convert(values["mem_soft"]), resource.RLIM_INFINITY)

    @unittest.skipIf(resource is None, "resource module unavailable")
    def test_macos_skips_memory_limit(self):
        original = external.IS_MACOS
        external.IS_MACOS = True  # force the macOS branch on any host
        try:
            tool = self._script(_TOOL_LIMIT_REPORTER)
            tlim, mlim, flim = 7, 4096, 5  # seconds, MB, MB
            values, status = external.run_extractor(tool, "dummy.cnf", {"tlim": tlim, "mlim": mlim, "flim": flim})
            self.assertEqual(status, "success")
            self.assertEqual(external.convert(values["cpu_soft"]), tlim)
            self.assertEqual(external.convert(values["fsize_soft"]), flim * 1024 * 1024)
            # RLIMIT_AS is left untouched even though a memory limit was requested.
            self.assertEqual(external.convert(values["mem_soft"]), resource.RLIM_INFINITY)
        finally:
            external.IS_MACOS = original

    def test_wall_clock_timeout(self):
        tool = self._script(_TOOL_SLEEP)
        values, status = external.run_extractor(tool, "dummy.cnf", {"tlim": 1, "mlim": 0, "flim": 0})
        self.assertEqual(status, "timeout")
        self.assertEqual(values, {})

    @unittest.skipUnless(sys.platform == "linux", "file-size kill is verified on Linux")
    def test_file_size_limit_kills_tool(self):
        tool = self._script(_TOOL_BIG_WRITE)
        fd, out = tempfile.mkstemp(prefix="fakeout_")
        os.close(fd)
        os.environ["FAKE_OUT"] = out
        try:
            values, status = external.run_extractor(tool, "dummy.cnf", {"tlim": 30, "mlim": 0, "flim": 1})
            self.assertEqual(status, "fileout")
            self.assertEqual(values, {})
        finally:
            os.environ.pop("FAKE_OUT", None)
            if os.path.exists(out):
                os.remove(out)


if __name__ == "__main__":
    unittest.main()
