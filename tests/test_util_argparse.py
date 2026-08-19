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

import argparse
import unittest

from gbd_core import util
from gbd_core.util_argparse import add_resource_limits_arguments


class ResourceLimitsHelpNoteTestCase(unittest.TestCase):
    """The platform check that decides which limits apply is surfaced in the help."""

    def _note(self, resource_obj, is_macos):
        orig_res, orig_mac = util.resource, util.IS_MACOS
        util.resource, util.IS_MACOS = resource_obj, is_macos
        try:
            return util.resource_limits_help_note()
        finally:
            util.resource, util.IS_MACOS = orig_res, orig_mac

    def test_note_linux_all_enforced(self):
        note = self._note(object(), False)
        self.assertIn("--tlim, --mlim and --flim are all enforced", note)

    def test_note_macos_skips_memory(self):
        note = self._note(object(), True)
        self.assertIn("--mlim is not enforced", note)
        self.assertIn("macOS", note)

    def test_note_no_resource_module(self):
        note = self._note(None, False)
        self.assertIn("only --tlim is enforced", note)
        self.assertIn("no POSIX resource module", note)

    def test_help_group_carries_platform_note(self):
        parser = argparse.ArgumentParser()
        add_resource_limits_arguments(parser)
        group = next((g for g in parser._action_groups if g.title == "resource limits (per instance)"), None)
        self.assertIsNotNone(group)
        self.assertEqual(group.description, util.resource_limits_help_note())
        self.assertIn("resource limits (per instance)", parser.format_help())


if __name__ == "__main__":
    unittest.main()
