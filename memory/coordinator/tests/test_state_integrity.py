"""Startup must never interpret damaged persisted ownership as a new server."""
import copy
import types
import unittest

from coordinator import Coordinator


class StateIntegrityTests(unittest.TestCase):
    def assert_invalid(self, value):
        store = types.SimpleNamespace(load=lambda: copy.deepcopy(value), save=lambda state: None)
        health = types.SimpleNamespace(get=lambda: {})
        with self.assertRaises((ValueError, TypeError)):
            Coordinator(store, health)

    def test_falsy_json_is_not_empty_initialized_state(self):
        for value in (None, {}, [], False, 0, ""):
            with self.subTest(value=value):
                self.assert_invalid(value)

    def test_unknown_request_state_cannot_hide_ownership(self):
        self.assert_invalid({"version": 1, "requests": {
            "request-a": {"request_id": "request-a", "state": "unknown"}
        }, "queue": [], "active": None})

    def test_missing_active_field_is_not_an_empty_slot(self):
        self.assert_invalid({"version": 1, "requests": {}, "queue": []})
