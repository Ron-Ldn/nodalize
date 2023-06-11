import logging
from unittest import TestCase

from nodalize.tools.static_func_tools import log_warning_once

log_messages = []


class LocalLoggingHandler(logging.StreamHandler):
    def emit(self, record):
        log_messages.append(self.format(record))


class TestStaticFuncTools(TestCase):
    def setUp(self):
        logger = logging.getLogger()
        logger.addHandler(LocalLoggingHandler())

    def test_log_warning_once(self):
        self.assertEqual(0, len(log_messages))

        log_warning_once("flag1", "aaa")
        log_warning_once("flag1", "abb")
        log_warning_once("flag1", "abc")
        log_warning_once("flag2", "ddd")
        log_warning_once("flag1", "abc")
        log_warning_once("flag2", "def")

        self.assertEqual(2, len(log_messages))
        self.assertIn("aaa", log_messages)
        self.assertIn("ddd", log_messages)
