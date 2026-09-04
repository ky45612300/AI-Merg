import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
_TEMP_DIR = None
SERVER = None


def setUpModule():
    global _TEMP_DIR, SERVER
    original_cwd = os.getcwd()
    scratch_dir = os.environ.get("PI_SCRATCH_DIR")
    _TEMP_DIR = tempfile.mkdtemp(prefix="api-pool-test-", dir=scratch_dir)
    os.chdir(_TEMP_DIR)
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        sys.modules.pop("api_pool_server", None)
        SERVER = importlib.import_module("api_pool_server")
    finally:
        os.chdir(original_cwd)
        sys.path.remove(str(PROJECT_ROOT))


class EndpointListCacheTests(unittest.TestCase):
    def setUp(self):
        self.pool = SERVER.APIPool()
        self.pool._check_new_endpoint_health = lambda _ep_id: None

    def test_list_endpoints_includes_endpoint_added_after_cache_warmup(self):
        self.pool.add_endpoint({
            "name": "A",
            "base_url": "https://a.example/v1",
            "api_key": "test-key",
            "model": "gpt-4",
        })
        self.assertEqual([endpoint["model"] for endpoint in self.pool.list_endpoints()], ["gpt-4"])

        self.pool.add_endpoint({
            "name": "A",
            "base_url": "https://a.example/v1",
            "api_key": "test-key",
            "model": "claude-5",
        })

        self.assertEqual(
            [endpoint["model"] for endpoint in self.pool.list_endpoints()],
            ["gpt-4", "claude-5"],
        )

    def test_duplicate_guard_matches_station_and_normalized_upstream_model(self):
        self.pool.add_endpoint({
            "name": "A",
            "base_url": "https://a.example/v1",
            "api_key": "test-key",
            "model": "public-gpt-4",
            "upstream_model": "GPT_4",
        })

        self.assertTrue(self.pool.has_duplicate_endpoint(
            "https://a.example/another-path", "gpt-4"
        ))
        self.assertFalse(self.pool.has_duplicate_endpoint(
            "https://b.example/v1", "gpt-4"
        ))
        self.assertFalse(self.pool.has_duplicate_endpoint(
            "https://a.example/v1", "claude-5"
        ))

    def test_batch_api_skips_duplicates_for_the_same_station_only(self):
        original_pool = SERVER.pool
        original_sync = SERVER._sync_to_config
        SERVER.pool = self.pool
        SERVER._sync_to_config = lambda: None
        try:
            self.pool.add_endpoint({
                "name": "A",
                "base_url": "https://a.example/v1",
                "api_key": "test-key",
                "model": "gpt-4",
                "upstream_model": "gpt-4",
            })
            status, body, _ = SERVER.api_handler("POST", "/api/endpoints/batch", {
                "base": {"base_url": "https://a.example/v1", "api_key": "test-key"},
                "endpoints": [
                    {"model": "duplicate", "upstream_model": "GPT_4"},
                    {"model": "claude-5", "upstream_model": "claude-5"},
                    {
                        "base_url": "https://b.example/v1",
                        "model": "gpt-4",
                        "upstream_model": "gpt-4",
                    },
                ],
            })
        finally:
            SERVER.pool = original_pool
            SERVER._sync_to_config = original_sync

        self.assertEqual(status, 201)
        self.assertEqual(body["added"], 2)
        self.assertEqual(body["skipped"], ["GPT_4"])
        self.assertEqual(
            sorted((endpoint["base_url"], endpoint["upstream_model"])
                   for endpoint in self.pool.list_endpoints()),
            [
                ("https://a.example/v1", "claude-5"),
                ("https://a.example/v1", "gpt-4"),
                ("https://b.example/v1", "gpt-4"),
            ],
        )


class CapabilityDetectionTests(unittest.TestCase):
    def test_capability_result_distinguishes_supported_unsupported_and_unknown(self):
        self.assertTrue(hasattr(SERVER.APIPool, "test_capabilities"))
        self.assertIn("item.state==='supported'", SERVER.GUI_HTML)
        self.assertIn("item.state==='unsupported'", SERVER.GUI_HTML)
        self.assertIn("state:'unknown'", SERVER.GUI_HTML)

    def test_capability_error_classification_keeps_ambiguous_failures_unknown(self):
        self.assertEqual(SERVER.APIPool._capability_error_state("model does not support embeddings"), "unsupported")
        self.assertEqual(SERVER.APIPool._capability_error_state("HTTP 401: invalid API key"), "unknown")
        self.assertEqual(SERVER.APIPool._capability_error_state("timed out"), "unknown")

    def test_capability_api_uses_the_requested_candidate_model(self):
        original_method = SERVER.pool.test_capabilities
        seen = {}

        def fake_test_capabilities(base_url, api_key, model, **kwargs):
            seen.update({"base_url": base_url, "api_key": api_key, "model": model, **kwargs})
            return {"chat": {"state": "supported"}}

        SERVER.pool.test_capabilities = fake_test_capabilities
        try:
            status, body, _ = SERVER.api_handler("POST", "/api/test-capabilities", {
                "base_url": "https://candidate.example/v1",
                "api_key": "test-key",
                "model": "candidate-model",
                "use_proxy": False,
                "protocol": "openai",
                "timeout": 12,
            })
        finally:
            SERVER.pool.test_capabilities = original_method

        self.assertEqual(status, 200)
        self.assertEqual(body["chat"]["state"], "supported")
        self.assertEqual(seen["model"], "candidate-model")
        self.assertEqual(seen["base_url"], "https://candidate.example/v1")
        self.assertFalse(seen["use_proxy"])


class ModelBrowserDuplicateGuardSourceTests(unittest.TestCase):
    def test_active_chain_displays_upstream_model_name_only(self):
        self.assertIn("<div class=\"model\">${esc(it.upstream_model||it.model)} ${vis}</div>", SERVER.GUI_HTML)

    def test_model_browser_uses_compact_two_cell_overlay_for_model_and_alias(self):
        self.assertIn(".mb-row .name-cell{grid-column:2", SERVER.GUI_HTML)
        self.assertIn(".name-overflow{position:absolute", SERVER.GUI_HTML)
        self.assertIn(".mb-row .alias-cell{grid-column:3", SERVER.GUI_HTML)
        self.assertIn("background:rgba(9,10,15,.98)", SERVER.GUI_HTML)
        self.assertIn("overflow-y:auto", SERVER.GUI_HTML)
        self.assertNotIn("min-width:1000px", SERVER.GUI_HTML)

    def test_model_name_and_selected_alias_input_are_vertically_centered(self):
        self.assertIn("top:50%;transform:translateY(-50%)", SERVER.GUI_HTML)
        self.assertIn("height:24px;line-height:normal", SERVER.GUI_HTML)

    def test_selected_alias_input_uses_opaque_background(self):
        self.assertIn(".mb-row.selected .alias-cell input{background:rgba(9,10,15,.98)", SERVER.GUI_HTML)

    def test_selected_alias_input_stays_opaque_when_focused(self):
        self.assertIn(".mb-row .alias-cell input:focus{border-color:var(--accent);background:rgba(9,10,15,.98)", SERVER.GUI_HTML)

    def test_selected_model_alias_is_used_when_saving_single_endpoint(self):
        self.assertIn("Object.prototype.hasOwnProperty.call(modelAliases,upstreamModel)", SERVER.GUI_HTML)
        self.assertIn("const publicModel=(aliasEdited?modelAliases[upstreamModel].trim():document.getElementById('fModel').value.trim())||upstreamModel;", SERVER.GUI_HTML)

    def test_station_model_edit_mode_allows_existing_models_to_be_toggled_and_reconciled(self):
        html = SERVER.GUI_HTML
        self.assertIn("let stationEditModelMode=false", html)
        self.assertIn("existingSelectionInitialized", html)
        self.assertIn("if(stationEditModelMode)", html)
        self.assertIn("in_pool:selected.has(canonicalModelKey(m.id))", html)
        self.assertIn('ep.in_pool = selection["in_pool"]', Path(SERVER.__file__).read_text(encoding="utf-8"))
        self.assertIn("existingStationModelEndpoints", html)
        self.assertIn("saveStationModelSelection", html)

    def test_station_model_edit_saves_existing_endpoint_in_pool_and_alias(self):
        original_pool = SERVER.pool
        original_sync = SERVER._sync_to_config
        pool = SERVER.APIPool()
        pool._check_new_endpoint_health = lambda _ep_id: None
        pool.add_endpoint({
            "id": "station-model-1",
            "name": "A",
            "base_url": "https://a.example/v1",
            "api_key": "test-key",
            "model": "gpt-4-public",
            "public_model": "gpt-4-public",
            "upstream_model": "gpt-4",
            "in_pool": True,
        })
        SERVER.pool = pool
        SERVER._sync_to_config = lambda: None
        try:
            status, body, _ = SERVER.api_handler("PUT", "/api/endpoints/station-model-1", {
                "model": "gpt-4-alias",
                "public_model": "gpt-4-alias",
                "upstream_model": "gpt-4",
                "in_pool": False,
            })
        finally:
            SERVER.pool = original_pool
            SERVER._sync_to_config = original_sync

        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])
        endpoint = next(e for e in pool.list_endpoints() if e["id"] == "station-model-1")
        self.assertFalse(endpoint["in_pool"])
        self.assertEqual(endpoint["model"], "gpt-4-alias")

    def test_station_model_pool_api_reconciles_existing_endpoint_state(self):
        original_pool = SERVER.pool
        original_sync = SERVER._sync_to_config
        pool = SERVER.APIPool()
        pool._check_new_endpoint_health = lambda _ep_id: None
        pool.add_endpoint({
            "id": "station-model-pool-1",
            "name": "A",
            "base_url": "https://a.example/v1",
            "api_key": "test-key",
            "model": "gpt-4-public",
            "upstream_model": "gpt-4",
            "in_pool": True,
        })
        pool.add_endpoint({
            "id": "station-model-pool-2",
            "name": "A backup",
            "base_url": "https://a.example/v1",
            "api_key": "test-key",
            "model": "gpt-4-public",
            "upstream_model": "gpt-4",
            "in_pool": True,
        })
        SERVER.pool = pool
        SERVER._sync_to_config = lambda: None
        try:
            status, body, _ = SERVER.api_handler("POST", "/api/stations/a.example/model-pool", {
                "models": [{
                    "upstream_model": "gpt-4",
                    "public_model": "gpt-4-alias",
                    "in_pool": False,
                }],
                "base": {"base_url": "https://a.example/v1", "api_key": "test-key"},
            })
            off_pool = pool.list_endpoints()
            restore_status, restore_body, _ = SERVER.api_handler("POST", "/api/stations/a.example/model-pool", {
                "models": [{
                    "upstream_model": "gpt-4",
                    "public_model": "gpt-4-restored",
                    "in_pool": True,
                }],
                "base": {"base_url": "https://a.example/v1", "api_key": "test-key"},
            })
        finally:
            SERVER.pool = original_pool
            SERVER._sync_to_config = original_sync

        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])
        self.assertEqual(len(off_pool), 2)
        self.assertTrue(all(not endpoint["in_pool"] for endpoint in off_pool))
        self.assertEqual(restore_status, 200)
        self.assertTrue(restore_body["ok"])
        restored = pool.list_endpoints()
        self.assertTrue(all(endpoint["in_pool"] for endpoint in restored))
        self.assertTrue(all(endpoint["model"] == "gpt-4-restored" for endpoint in restored))

    def test_existing_station_models_are_disabled_and_skipped_in_batch_selection(self):
        html = SERVER.GUI_HTML
        self.assertIn("if(isExistingModel(id)&&!stationEditModelMode)", html)
        self.assertIn("stationEditModelMode||!isExistingModel(m.id)", html)


if __name__ == "__main__":
    unittest.main()
