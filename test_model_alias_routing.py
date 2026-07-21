import unittest
from unittest.mock import patch
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

with patch.object(threading.Thread, "start", lambda self: None):
    import api_pool_server as server


class ModelAliasRoutingTests(unittest.TestCase):
    def test_legacy_model_field_uses_the_same_name_in_both_directions(self):
        pool = server.APIPool()
        pool.add_endpoint({"name": "legacy", "model": "gpt-5.6"})

        endpoint = pool.list_endpoints()[0]
        self.assertEqual("gpt-5.6", endpoint["public_model"])
        self.assertEqual("gpt-5.6", endpoint["upstream_model"])

    def test_routes_one_public_model_to_each_endpoint_upstream_model(self):
        pool = server.APIPool()
        pool.add_endpoint({
            "name": "first",
            "model": "gpt-5.6",
            "upstream_model": "张三 gpt-5.6 ABc123",
            "priority": 1,
            "max_retries": 0,
        })
        pool.add_endpoint({
            "name": "second",
            "model": "gpt-5.6",
            "upstream_model": "李四-gpt-5.6-v2",
            "priority": 2,
            "max_retries": 0,
        })

        upstream_models = []

        def try_endpoint(endpoint, payload, timeout, **_kwargs):
            upstream_models.append(payload["model"])
            if endpoint.name == "first":
                return None, "unavailable"
            return {"model": endpoint.upstream_model, "choices": [{"message": {"content": "ok"}}]}, ""

        pool._try_endpoint = try_endpoint
        pool._probe_endpoint = lambda _endpoint: False

        with patch.object(server.time, "sleep"):
            result = pool.chat([{"role": "user", "content": "hello"}], model="gpt-5.6")

        self.assertEqual(["张三 gpt-5.6 ABc123", "李四-gpt-5.6-v2"], upstream_models)
        self.assertEqual("gpt-5.6", result["model"])

    def test_global_alias_routes_to_mapped_pool_model(self):
        pool = server.APIPool()
        pool.add_endpoint({
            "name": "real",
            "model": "gpt-5.6-sol",
            "upstream_model": "gpt-5.6-sol",
            "priority": 1,
            "max_retries": 0,
        })
        pool.model_aliases = {"gpt-4o": "gpt-5.6-sol"}

        upstream_models = []

        def try_endpoint(endpoint, payload, timeout, **_kwargs):
            upstream_models.append(payload["model"])
            return {"model": endpoint.upstream_model, "choices": [{"message": {"content": "ok"}}]}, ""

        pool._try_endpoint = try_endpoint

        result = pool.chat([{"role": "user", "content": "hello"}], model="gpt-4o")

        self.assertEqual(["gpt-5.6-sol"], upstream_models)
        # 响应回显别名，而不是真实模型
        self.assertEqual("gpt-4o", result["model"])

    def test_unmapped_model_name_passes_through_unchanged(self):
        pool = server.APIPool()
        pool.model_aliases = {"gpt-4o": "gpt-5.6-sol"}
        self.assertEqual("grok-4.5", pool.resolve_model_alias("grok-4.5"))
        self.assertEqual("", pool.resolve_model_alias(""))

    def test_alias_to_unconfigured_model_raises_not_found(self):
        pool = server.APIPool()
        pool.add_endpoint({"name": "real", "model": "gpt-5.6-sol", "priority": 1, "max_retries": 0})
        pool.model_aliases = {"gpt-4o": "no-such-model"}

        with self.assertRaises(server.ModelRouteError) as ctx:
            pool.chat([{"role": "user", "content": "hi"}], model="gpt-4o")
        self.assertEqual("model_not_found", ctx.exception.error_type)


if __name__ == "__main__":
    unittest.main()
