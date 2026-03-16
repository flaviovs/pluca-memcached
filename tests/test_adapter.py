from __future__ import annotations

import unittest

import pluca_memcached


class TestConstructorValidation(unittest.TestCase):
    def test_empty_endpoint_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            pluca_memcached.Adapter(endpoint="")

    def test_bad_endpoint_format_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            pluca_memcached.Adapter(endpoint="localhost")

    def test_empty_namespace_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            pluca_memcached.Adapter(endpoint="127.0.0.1:11211", namespace="")
