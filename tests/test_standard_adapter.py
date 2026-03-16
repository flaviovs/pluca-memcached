from __future__ import annotations

import os
import unittest

import pluca_memcached
from pluca.test import AdapterTester


def _endpoint() -> str:
    return os.environ.get("MEMCACHED_ENDPOINT", "127.0.0.1:11211")


class TestMemcachedAdapter(AdapterTester, unittest.TestCase):
    def get_adapter(self) -> pluca_memcached.Adapter:
        return pluca_memcached.Adapter(endpoint=_endpoint(), namespace="test")
