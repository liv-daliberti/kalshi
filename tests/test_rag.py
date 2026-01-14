import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()

import src.rag.rag as rag


class RagTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._saved_state = dict(rag._CLIENT_STATE)
        rag._CLIENT_STATE["client"] = None
        rag._CLIENT_STATE["uses_v1"] = False
        rag._CLIENT_STATE["cfg"] = None
        self._saved_embed_state = dict(rag._EMBED_CLIENT_STATE)
        rag._EMBED_CLIENT_STATE["client"] = None
        rag._EMBED_CLIENT_STATE["cfg"] = None

    def tearDown(self) -> None:
        rag._CLIENT_STATE.clear()
        rag._CLIENT_STATE.update(self._saved_state)
        rag._EMBED_CLIENT_STATE.clear()
        rag._EMBED_CLIENT_STATE.update(self._saved_embed_state)


class TestRagConfigAndClient(RagTestCase):
    def test_load_yaml_without_yaml_module(self) -> None:
        with patch.object(rag, "_yaml", None):
            self.assertEqual(rag._load_yaml("missing.yaml"), {})

    def test_load_yaml_no_path(self) -> None:
        self.assertEqual(rag._load_yaml(None), {})

    def test_load_yaml_missing_file(self) -> None:
        class FakeYaml:
            def safe_load(self, handle):
                raise AssertionError("should not read missing file")

        with patch.object(rag, "_yaml", FakeYaml()):
            self.assertEqual(rag._load_yaml("does-not-exist.yaml"), {})

    def test_load_yaml_reads_file(self) -> None:
        class FakeYaml:
            def __init__(self) -> None:
                self.called = False

            def safe_load(self, handle):
                self.called = True
                return {"endpoint": "https://yaml"}

        fake_yaml = FakeYaml()
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as handle:
            handle.write("endpoint: https://yaml\n")
            path = handle.name
        try:
            with patch.object(rag, "_yaml", fake_yaml):
                data = rag._load_yaml(path)
        finally:
            os.unlink(path)
        self.assertTrue(fake_yaml.called)
        self.assertEqual(data, {"endpoint": "https://yaml"})

    def test_load_azure_config(self) -> None:
        with patch.object(rag, "_load_yaml", return_value={"deployment": "dep"}):
            with patch.dict(
                os.environ,
                {
                    "AZURE_OPENAI_ENDPOINT": "https://example/",
                    "AZURE_OPENAI_API_KEY": "key",
                    "AZURE_OPENAI_API_VERSION": "2024-12-01-preview",
                    "AZURE_OPENAI_USE_V1": "1",
                },
            ):
                cfg = rag._load_azure_config()
        self.assertEqual(cfg.endpoint, "https://example")
        self.assertEqual(cfg.api_key, "key")
        self.assertEqual(cfg.deployment, "dep")
        self.assertTrue(cfg.use_v1)

    def test_load_azure_config_missing(self) -> None:
        with patch.object(rag, "_load_yaml", return_value={}):
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(RuntimeError):
                    rag._load_azure_config()

    def test_build_preferred_client_requires_azure(self) -> None:
        cfg = rag.AzureCfg(
            endpoint="https://example",
            api_key="key",
            deployment="",
            api_version="2024-12-01-preview",
            use_v1=False,
        )
        with patch.object(rag, "AzureOpenAI", None):
            with self.assertRaises(RuntimeError):
                rag._build_preferred_client(cfg)

    def test_build_preferred_client_v1_success(self) -> None:
        class FakeOpenAI:
            def __init__(self, base_url, api_key):
                self.base_url = base_url
                self.api_key = api_key

        cfg = rag.AzureCfg(
            endpoint="https://example/",
            api_key="key",
            deployment="dep",
            api_version="2024-12-01-preview",
            use_v1=True,
        )
        with patch.object(rag, "OpenAI", FakeOpenAI), \
             patch.object(rag, "AzureOpenAI", object), \
             patch.object(rag, "OpenAIError", Exception):
            client, uses_v1 = rag._build_preferred_client(cfg)
        self.assertTrue(uses_v1)
        self.assertEqual(client.base_url, "https://example/openai/v1/")

    def test_build_preferred_client_v1_fallback(self) -> None:
        class FakeOpenAI:
            def __init__(self, base_url, api_key):
                raise rag.OpenAIError("fail")

        class FakeAzure:
            def __init__(self, api_key, azure_endpoint, api_version):
                self.api_key = api_key
                self.azure_endpoint = azure_endpoint
                self.api_version = api_version

        cfg = rag.AzureCfg(
            endpoint="https://example/",
            api_key="key",
            deployment="dep",
            api_version="2024-12-01-preview",
            use_v1=True,
        )
        with patch.object(rag, "OpenAI", FakeOpenAI), \
             patch.object(rag, "AzureOpenAI", FakeAzure), \
             patch.object(rag, "OpenAIError", Exception):
            client, uses_v1 = rag._build_preferred_client(cfg)
        self.assertFalse(uses_v1)
        self.assertEqual(client.azure_endpoint, "https://example")

    def test_get_client_cached(self) -> None:
        rag._CLIENT_STATE["client"] = "client"
        rag._CLIENT_STATE["uses_v1"] = True
        rag._CLIENT_STATE["cfg"] = "cfg"
        client, uses_v1, cfg = rag._get_client()
        self.assertEqual(client, "client")
        self.assertTrue(uses_v1)
        self.assertEqual(cfg, "cfg")

    def test_get_client_initializes(self) -> None:
        cfg = rag.AzureCfg(
            endpoint="https://example",
            api_key="key",
            deployment="dep",
            api_version="2024-12-01-preview",
            use_v1=True,
        )
        with patch("src.rag.rag._load_azure_config", return_value=cfg) as load_cfg, \
             patch("src.rag.rag._build_preferred_client", return_value=("client", True)) as build:
            client, uses_v1, cfg_out = rag._get_client()
        self.assertEqual(client, "client")
        self.assertTrue(uses_v1)
        self.assertIs(cfg_out, cfg)
        load_cfg.assert_called_once()
        build.assert_called_once_with(cfg)


class TestRagParsing(RagTestCase):
    def test_extract_text_fallback(self) -> None:
        self.assertEqual(rag._extract_text(object(), uses_v1=False), "")

    def test_json_from_text_paths(self) -> None:
        self.assertIsNone(rag._json_from_text(""))
        self.assertIsNone(rag._json_from_text("{bad}"))
        self.assertIsNone(rag._json_from_text("prefix {bad} suffix"))
        self.assertIsNone(rag._json_from_text("no json here"))
        self.assertEqual(rag._json_from_text('{"yes_prob": 0.5}'), {"yes_prob": 0.5})
        self.assertEqual(
            rag._json_from_text("prefix {\"yes_prob\": 0.5} suffix"),
            {"yes_prob": 0.5},
        )


class TestRagEmbeddings(RagTestCase):
    def test_embed_texts_requires_deployment(self) -> None:
        with patch("src.rag.rag._get_embedding_client", return_value=object()), \
             patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                rag.embed_texts(["a"])

    def test_embed_texts_success(self) -> None:
        class Embeddings:
            def create(self, model, input):
                self.model = model
                self.input = input
                return SimpleNamespace(data=[SimpleNamespace(embedding=[0.1, 0.2])])

        client = SimpleNamespace(embeddings=Embeddings())
        with patch("src.rag.rag._get_embedding_client", return_value=client), \
             patch.dict(os.environ, {"RAG_EMBEDDING_DEPLOYMENT": "embed"}):
            vectors = rag.embed_texts(["text"])
        self.assertEqual(vectors, [[0.1, 0.2]])


class TestRagPredict(RagTestCase):
    def test_predict_market_requires_deployment(self) -> None:
        cfg = rag.AzureCfg(
            endpoint="https://example",
            api_key="key",
            deployment="",
            api_version="2024-12-01-preview",
            use_v1=False,
        )
        with patch("src.rag.rag._get_client", return_value=(object(), False, cfg)), \
             patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                rag.predict_market({}, {}, [], "prompt")

    def test_predict_market_v1(self) -> None:
        class Responses:
            def create(self, **kwargs):
                self.kwargs = kwargs
                return SimpleNamespace(output_text='{"yes_prob": 0.7, "confidence": 0.4}')

        client = SimpleNamespace(responses=Responses())
        cfg = rag.AzureCfg(
            endpoint="https://example",
            api_key="key",
            deployment="dep",
            api_version="2024-12-01-preview",
            use_v1=True,
        )
        docs = [
            {"source": "doc", "source_id": "1", "content": "text"},
            {"source": "doc", "source_id": "2"},
        ]
        with patch("src.rag.rag._get_client", return_value=(client, True, cfg)), \
             patch.dict(os.environ, {"RAG_LLM_DEPLOYMENT": "llm"}):
            result = rag.predict_market(
                {"title": "Event"},
                {"ticker": "M1", "market_subtitle": "Sub", "rules_primary": "Rules"},
                docs,
                "Prompt",
            )
        self.assertEqual(result["yes_prob"], 0.7)
        self.assertEqual(result["confidence"], 0.4)

    def test_predict_market_chat(self) -> None:
        class Completions:
            def create(self, **kwargs):
                self.kwargs = kwargs
                msg = SimpleNamespace(content='{"yes_prob": 0.2, "rationale": "ok"}')
                return SimpleNamespace(choices=[SimpleNamespace(message=msg)])

        client = SimpleNamespace(chat=SimpleNamespace(completions=Completions()))
        cfg = rag.AzureCfg(
            endpoint="https://example",
            api_key="key",
            deployment="dep",
            api_version="2024-12-01-preview",
            use_v1=False,
        )
        with patch("src.rag.rag._get_client", return_value=(client, False, cfg)), \
             patch.dict(os.environ, {"RAG_LLM_DEPLOYMENT": "llm"}):
            result = rag.predict_market(
                {"title": "Event"},
                {"market_title": "Market"},
                [],
                "Prompt",
            )
        self.assertEqual(result["yes_prob"], 0.2)
        self.assertEqual(result["rationale"], "ok")
