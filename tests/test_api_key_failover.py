import importlib.util
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_module():
    temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    os.chdir(temp_dir.name)
    (Path(temp_dir.name) / "security_config.json").write_text(
        '{"admin_username":"test","password":{},"client_api_keys":[{"id":"default","hash":"x","plain":"test-client-key","enabled":true}]}',
        encoding="utf-8",
    )
    module_name = "api_pool_server_test_module"
    spec = importlib.util.spec_from_file_location(module_name, PROJECT_ROOT / "api_pool_server.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module, temp_dir


class ApiKeyFailoverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module, cls.temp_dir = load_module()
        cls.module.APIPool._check_new_endpoint_health = lambda self, ep_id: None

    @classmethod
    def tearDownClass(cls):
        os.chdir(str(PROJECT_ROOT))
        cls.temp_dir.cleanup()
        sys.modules.pop("api_pool_server_test_module", None)

    def make_pool(self, keys):
        ep = self.module.Endpoint(
            id="ep-1",
            name="test",
            base_url="https://example.invalid/v1",
            api_key=keys[0],
            api_keys=keys,
            model="demo-model",
            public_model="demo-model",
            upstream_model="demo-model",
            health_mode="none",
        )
        pool = self.module.APIPool()
        pool.add_endpoint(ep)
        return pool, ep

    def test_legacy_single_key_is_normalized(self):
        ep = self.module.Endpoint(api_key="legacy-key")
        self.module.APIPool._normalize_api_keys(ep)
        self.assertEqual(ep.api_keys, ["legacy-key"])
        self.assertEqual(ep.api_key, "legacy-key")

    def test_keys_are_attempted_in_input_order_and_429_cools_only_failed_key(self):
        pool, ep = self.make_pool(["key-1", "key-2", "key-3"])
        calls = []

        def fake_try_endpoint(endpoint, payload, timeout, log_usage=True, force_no_retry=False, api_key=None):
            calls.append(api_key)
            if api_key == "key-1":
                return None, "HTTP 429: rate limited (429 rate-limited)"
            return {"ok": True}, ""

        pool._try_endpoint = fake_try_endpoint
        result, error, used_key = pool._try_endpoint_with_keys(
            ep, {"model": "demo-model", "messages": []}, timeout=1
        )

        self.assertEqual(calls, ["key-1", "key-2"])
        self.assertEqual(result, {"ok": True})
        self.assertEqual(error, "")
        self.assertEqual(used_key, "key-2")
        self.assertGreater(ep._key_cooldown_until["key-1"], time.time())
        self.assertNotIn("key-2", ep._key_cooldown_until)

    def test_first_key_returns_to_priority_after_its_cooldown_expires(self):
        pool, ep = self.make_pool(["key-1", "key-2"])
        ep._key_cooldown_until["key-1"] = time.time() + 60
        calls = []

        def fake_try_endpoint(endpoint, payload, timeout, log_usage=True, force_no_retry=False, api_key=None):
            calls.append(api_key)
            return {"ok": True}, ""

        pool._try_endpoint = fake_try_endpoint
        pool._try_endpoint_with_keys(ep, {"model": "demo-model"}, timeout=1)
        self.assertEqual(calls, ["key-2"])

        calls.clear()
        ep._key_cooldown_until["key-1"] = time.time() - 1
        pool._try_endpoint_with_keys(ep, {"model": "demo-model"}, timeout=1)
        self.assertEqual(calls, ["key-1"])

    def test_endpoint_payload_exposes_each_key_for_authenticated_admin_ui(self):
        pool, ep = self.make_pool(["key-one-1234", "key-two-5678"])
        data = pool._ep_to_dict(ep, False, time.time())
        self.assertEqual(data["api_keys_full"], ["key-one-1234", "key-two-5678"])
        self.assertEqual(data["api_keys"], ["key-one-1***", "key-two-5***"])


if __name__ == "__main__":
    unittest.main()
