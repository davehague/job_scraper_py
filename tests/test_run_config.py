import os
import unittest
from unittest import mock

from run_config import emails_enabled, env_flag, log_to_file


class EnvFlagTests(unittest.TestCase):
    def test_default_used_when_unset(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(env_flag("SEND_EMAILS", default=True))
            self.assertFalse(env_flag("SEND_EMAILS", default=False))

    def test_false_values(self):
        for value in ["false", "False", "FALSE", "0", "no", "off", " false "]:
            with mock.patch.dict(os.environ, {"SEND_EMAILS": value}, clear=True):
                self.assertFalse(env_flag("SEND_EMAILS", default=True), value)

    def test_true_values(self):
        for value in ["true", "True", "1", "yes", "on"]:
            with mock.patch.dict(os.environ, {"SEND_EMAILS": value}, clear=True):
                self.assertTrue(env_flag("SEND_EMAILS", default=False), value)

    def test_unrecognised_value_falls_back_to_default(self):
        with mock.patch.dict(os.environ, {"SEND_EMAILS": "maybe"}, clear=True):
            self.assertTrue(env_flag("SEND_EMAILS", default=True))
            self.assertFalse(env_flag("SEND_EMAILS", default=False))


class EmailsEnabledTests(unittest.TestCase):
    def test_defaults_to_enabled(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(emails_enabled())

    def test_disabled_by_env(self):
        with mock.patch.dict(os.environ, {"SEND_EMAILS": "false"}, clear=True):
            self.assertFalse(emails_enabled())


class LogToFileTests(unittest.TestCase):
    def test_defaults_to_file_logging(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(log_to_file())

    def test_disabled_by_env(self):
        with mock.patch.dict(os.environ, {"LOG_TO_FILE": "false"}, clear=True):
            self.assertFalse(log_to_file())


if __name__ == "__main__":
    unittest.main()
