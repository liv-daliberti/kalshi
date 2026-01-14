import os
import sys
import types
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

import src.rag.rag_handler as rag_handler


class FakeCursor:
    def __init__(self, row=None, rows=None):
        self.row = row
        self.rows = rows or []
        self.executes = []

    def execute(self, sql, params=None):
        self.executes.append((sql, params))

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, cursor_obj=None):
        self.cursor_obj = cursor_obj or FakeCursor()
        self.closed = False

    def cursor(self, *args, **kwargs):
        return self.cursor_obj

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def close(self):
        self.closed = True


def make_cfg(
    embedding_dim=2,
    embedding_model="m",
    embedding_space="s",
    vector_space="cosine",
    top_k=1,
    doc_limit=1,
    normalize=False,
    use_baseline=False,
    embedding_handler=None,
    llm_handler=None,
):
    return rag_handler.RagConfig(
        embedding=rag_handler.EmbeddingConfig(
            embedding_dim=embedding_dim,
            embedding_model=embedding_model,
            embedding_space=embedding_space,
            vector_space=vector_space,
            normalize=normalize,
        ),
        retrieval=rag_handler.RetrievalConfig(
            top_k=top_k,
            doc_limit=doc_limit,
            use_baseline=use_baseline,
        ),
        handlers=rag_handler.HandlerConfig(
            embedding_handler=embedding_handler,
            llm_handler=llm_handler,
        ),
    )


class TestRagHandlerConfig(unittest.TestCase):
    def test_parse_int_and_bool(self) -> None:
        self.assertEqual(rag_handler._parse_int(None, 3), 3)
        self.assertEqual(rag_handler._parse_int("bad", 3), 3)
        self.assertEqual(rag_handler._parse_int("1", 3, minimum=5), 5)
        self.assertTrue(rag_handler._parse_bool("yes", False))
        self.assertFalse(rag_handler._parse_bool(None, False))

    def test_load_rag_config_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            cfg = rag_handler.load_rag_config()
        self.assertEqual(cfg.embedding.embedding_dim, 384)
        self.assertTrue(cfg.embedding.normalize)
        self.assertTrue(cfg.retrieval.use_baseline)

    def test_load_rag_config_overrides(self) -> None:
        with patch.dict(
            os.environ,
            {
                "RAG_EMBEDDING_DIM": "8",
                "RAG_TOP_K": "0",
                "RAG_DOC_LIMIT": "1",
                "RAG_EMBEDDING_MODEL": "model",
                "RAG_EMBEDDING_SPACE": "space",
                "RAG_VECTOR_SPACE": "dot",
                "RAG_EMBEDDING_NORMALIZE": "0",
                "RAG_USE_BASELINE": "0",
            },
            clear=True,
        ):
            cfg = rag_handler.load_rag_config()
        self.assertEqual(cfg.embedding.embedding_dim, 16)
        self.assertEqual(cfg.retrieval.top_k, 1)
        self.assertFalse(cfg.embedding.normalize)
        self.assertFalse(cfg.retrieval.use_baseline)


class TestRagHandlerHandlers(unittest.TestCase):
    def test_resolve_handler_callable(self) -> None:
        module = types.ModuleType("fake_mod")

        def handler():
            return None

        module.handler = handler
        sys.modules["fake_mod"] = module
        try:
            result = rag_handler._resolve_handler("fake_mod:handler")
        finally:
            sys.modules.pop("fake_mod", None)
        self.assertIs(result, handler)

    def test_resolve_handler_not_callable(self) -> None:
        module = types.ModuleType("fake_mod2")
        module.handler = "nope"
        sys.modules["fake_mod2"] = module
        try:
            with self.assertRaises(TypeError):
                rag_handler._resolve_handler("fake_mod2:handler")
        finally:
            sys.modules.pop("fake_mod2", None)


class TestRagHandlerEmbedding(unittest.TestCase):
    def test_hash_embedding(self) -> None:
        vec = rag_handler._hash_embedding("", 4)
        self.assertEqual(vec, [0.0, 0.0, 0.0, 0.0])
        vec = rag_handler._hash_embedding("hello hello", 8)
        self.assertEqual(len(vec), 8)
        self.assertTrue(any(abs(val) > 0 for val in vec))

    def test_normalize(self) -> None:
        vec = rag_handler._normalize([0.0, 0.0])
        self.assertEqual(vec, [0.0, 0.0])
        vec = rag_handler._normalize([3.0, 4.0])
        self.assertAlmostEqual(vec[0], 0.6)
        self.assertAlmostEqual(vec[1], 0.8)

    def test_embed_texts_default_normalized(self) -> None:
        cfg = make_cfg(normalize=True)
        with patch.object(rag_handler, "_hash_embedding", return_value=[3.0, 4.0]):
            vectors = rag_handler.embed_texts(["a"], cfg)
        self.assertAlmostEqual(vectors[0][0], 0.6)
        self.assertAlmostEqual(vectors[0][1], 0.8)

    def test_embed_texts_handler_success(self) -> None:
        module = types.ModuleType("fake_embed")

        def handler(texts):
            return [[1.0, 0.0] for _ in texts]

        module.handler = handler
        sys.modules["fake_embed"] = module
        cfg = make_cfg(embedding_handler="fake_embed:handler")
        try:
            vectors = rag_handler.embed_texts(["a"], cfg)
        finally:
            sys.modules.pop("fake_embed", None)
        self.assertEqual(vectors, [[1.0, 0.0]])

    def test_embed_texts_handler_failure_fallback(self) -> None:
        cfg = make_cfg(embedding_handler="fake:missing")
        with patch.object(rag_handler, "_resolve_handler", side_effect=RuntimeError("boom")), \
             patch.object(rag_handler, "_hash_embedding", return_value=[0.5, 0.5]):
            vectors = rag_handler.embed_texts(["a"], cfg)
        self.assertEqual(vectors, [[0.5, 0.5]])


class TestRagHandlerMath(unittest.TestCase):
    def test_cosine(self) -> None:
        self.assertIsNone(rag_handler._cosine([], [1.0]))
        self.assertEqual(rag_handler._cosine([0.0], [0.0]), 0.0)
        self.assertEqual(rag_handler._cosine([1.0, 2.0], [2.0, 3.0]), 8.0)

    def test_normalize_prob(self) -> None:
        self.assertIsNone(rag_handler._normalize_prob("bad"))
        self.assertIsNone(rag_handler._normalize_prob(2))
        self.assertEqual(
            rag_handler._normalize_prob("0.1234567"),
            Decimal("0.123457"),
        )


class TestRagHandlerDocs(unittest.TestCase):
    def test_build_docs(self) -> None:
        event_doc = rag_handler._build_event_doc(
            {"title": "Event", "event_ticker": "EV", "product_metadata": {"x": 1}}
        )
        self.assertIn('"x": 1', event_doc["content"])
        market_doc = rag_handler._build_market_doc(
            "EV",
            {"ticker": "M1", "market_title": "Market"},
        )
        self.assertEqual(market_doc["source"], "market")

    def test_metadata_and_clamp(self) -> None:
        cfg = make_cfg(normalize=True, use_baseline=True)
        metadata = rag_handler._metadata_for_embedding(cfg)
        self.assertTrue(metadata["normalized"])
        self.assertEqual(rag_handler._clamp_text("abcd", limit=2), "ab")
        self.assertEqual(rag_handler._clamp_text(None), "")

    def test_upsert_doc_update(self) -> None:
        cursor = FakeCursor(row={"id": 5})
        conn = FakeConn(cursor)
        rag_handler._upsert_doc(conn, {"source": "event", "source_id": "1"}, [0.1], {})
        self.assertEqual(len(cursor.executes), 2)
        self.assertIn("UPDATE rag_documents", cursor.executes[1][0])

    def test_upsert_doc_insert(self) -> None:
        cursor = FakeCursor(row=None)
        conn = FakeConn(cursor)
        rag_handler._upsert_doc(conn, {"source": "event", "source_id": "1"}, [0.1], {})
        self.assertEqual(len(cursor.executes), 2)
        self.assertIn("INSERT INTO rag_documents", cursor.executes[1][0])

    def test_fetch_docs(self) -> None:
        cursor = FakeCursor(rows=[{"id": 1}])
        conn = FakeConn(cursor)
        rows = rag_handler._fetch_docs(conn, "EV", ["M1"], 10)
        self.assertEqual(rows, [{"id": 1}])

    def test_embed_missing_docs(self) -> None:
        cursor = FakeCursor()
        conn = FakeConn(cursor)
        docs = [{"id": 1, "content": "a", "embedding": None}, {"id": 2, "content": "b"}]
        cfg = make_cfg(use_baseline=True)
        with patch.object(rag_handler, "embed_texts", return_value=[[0.1, 0.2]]):
            rag_handler._embed_missing_docs(conn, docs, cfg)
        self.assertEqual(docs[0]["embedding"], [0.1, 0.2])
        self.assertEqual(len(cursor.executes), 1)

    def test_embed_missing_docs_no_missing(self) -> None:
        cursor = FakeCursor()
        conn = FakeConn(cursor)
        docs = [{"id": 1, "content": "a", "embedding": [0.1, 0.2]}]
        cfg = make_cfg(use_baseline=True)
        with patch.object(rag_handler, "embed_texts") as embed:
            rag_handler._embed_missing_docs(conn, docs, cfg)
        embed.assert_not_called()
        self.assertEqual(cursor.executes, [])

    def test_retrieve_docs_filters(self) -> None:
        docs = [
            {"id": 1, "event_ticker": "EV", "market_ticker": "M1", "embedding": [1.0, 0.0], "content": "ok"},
            {"id": 2, "event_ticker": "OTHER", "embedding": [1.0, 0.0], "content": "skip"},
            {"id": 3, "event_ticker": "EV", "market_ticker": "M2", "embedding": [1.0, 0.0], "content": "skip"},
            {"id": 4, "event_ticker": "EV", "market_ticker": "M1", "embedding": "bad", "content": "skip"},
            {"id": 5, "event_ticker": "EV", "market_ticker": "M1", "embedding": [1.0, 0.0], "content": "x" * 3000},
        ]
        results = rag_handler._retrieve_docs(docs, [1.0, 0.0], "M1", "EV", 2)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["id"], 1)
        self.assertEqual(len(results[1]["content"]), 2000)

    def test_retrieve_docs_skips_bad_score(self) -> None:
        docs = [
            {"id": 1, "event_ticker": "EV", "market_ticker": "M1", "embedding": [1.0, 0.0], "content": "ok"},
        ]
        results = rag_handler._retrieve_docs(docs, [1.0], "M1", "EV", 2)
        self.assertEqual(results, [])

    def test_baseline_prob(self) -> None:
        market = {"implied_yes_mid": "0.6"}
        self.assertEqual(rag_handler._baseline_prob(market), Decimal("0.600000"))
        market = {"implied_yes_mid": None, "candle_close": "0.4"}
        self.assertEqual(rag_handler._baseline_prob(market), Decimal("0.400000"))


class TestRagHandlerPredict(unittest.TestCase):
    def test_predict_event_missing_ticker(self) -> None:
        result = rag_handler.predict_event({}, [], {}, "prompt")
        self.assertEqual(result, [])

    def test_predict_event_llm_handler_load_failure(self) -> None:
        cfg = make_cfg(use_baseline=True, llm_handler="bad:handler")
        stored_docs = [
            {"id": 1, "event_ticker": "EV", "market_ticker": "M1", "embedding": [1.0, 0.0], "content": "doc"},
        ]
        with patch.object(rag_handler, "load_rag_config", return_value=cfg), \
             patch.object(rag_handler, "embed_texts", side_effect=lambda texts, _cfg: [[1.0, 0.0] for _ in texts]), \
             patch.object(rag_handler, "_resolve_handler", side_effect=RuntimeError("boom")), \
             patch.object(rag_handler, "_upsert_doc"), \
             patch.object(rag_handler, "_embed_missing_docs"), \
             patch.object(rag_handler, "_fetch_docs", return_value=stored_docs), \
             patch.object(rag_handler.logger, "exception") as logged, \
             patch("src.rag.rag_handler.psycopg.connect", return_value=FakeConn(), create=True), \
             patch.dict(os.environ, {"DATABASE_URL": "db"}):
            preds = rag_handler.predict_event(
                {"event_ticker": "EV", "title": "Event"},
                [{"ticker": "M1", "implied_yes_mid": "0.7"}],
                {},
                "Prompt",
            )
        self.assertEqual(preds[0]["yes_prob"], Decimal("0.700000"))
        logged.assert_called_once()

    def test_predict_event_baseline(self) -> None:
        cfg = make_cfg(use_baseline=True)
        stored_docs = [
            {"id": 1, "event_ticker": "EV", "market_ticker": "M1", "embedding": [1.0, 0.0], "content": "doc"},
        ]
        with patch.object(rag_handler, "load_rag_config", return_value=cfg), \
             patch.object(rag_handler, "embed_texts", side_effect=lambda texts, _cfg: [[1.0, 0.0] for _ in texts]), \
             patch.object(rag_handler, "_upsert_doc"), \
             patch.object(rag_handler, "_embed_missing_docs"), \
             patch.object(rag_handler, "_fetch_docs", return_value=stored_docs), \
             patch("src.rag.rag_handler.psycopg.connect", return_value=FakeConn(), create=True), \
             patch.dict(os.environ, {"DATABASE_URL": "db"}):
            preds = rag_handler.predict_event(
                {"event_ticker": "EV", "title": "Event"},
                [{"ticker": "M1", "implied_yes_mid": "0.7"}],
                {},
                "Prompt",
        )
        self.assertEqual(preds[0]["yes_prob"], Decimal("0.700000"))

    def test_predict_event_llm_handler(self) -> None:
        cfg = make_cfg(llm_handler="fake:handler")
        stored_docs = [
            {"id": 1, "event_ticker": "EV", "market_ticker": "M1", "embedding": [1.0, 0.0], "content": "doc"},
        ]

        def handler(_event, _market, _docs, _prompt):
            return {"yes_prob": 0.2, "confidence": 0.4, "rationale": "ok"}

        with patch.object(rag_handler, "load_rag_config", return_value=cfg), \
             patch.object(rag_handler, "embed_texts", side_effect=lambda texts, _cfg: [[1.0, 0.0] for _ in texts]), \
             patch.object(rag_handler, "_resolve_handler", return_value=handler), \
             patch.object(rag_handler, "_upsert_doc"), \
             patch.object(rag_handler, "_embed_missing_docs"), \
             patch.object(rag_handler, "_fetch_docs", return_value=stored_docs), \
             patch("src.rag.rag_handler.psycopg.connect", return_value=FakeConn(), create=True), \
             patch.dict(os.environ, {"DATABASE_URL": "db"}):
            preds = rag_handler.predict_event(
                {"event_ticker": "EV", "title": "Event"},
                [{"ticker": "M1", "implied_yes_mid": "0.6"}],
                {},
                "Prompt",
            )
        self.assertEqual(preds[0]["yes_prob"], Decimal("0.200000"))
        self.assertEqual(preds[0]["confidence"], Decimal("0.400000"))

    def test_predict_event_llm_handler_error(self) -> None:
        cfg = make_cfg(use_baseline=True, llm_handler="fake:handler")
        stored_docs = [
            {"id": 1, "event_ticker": "EV", "market_ticker": "M1", "embedding": [1.0, 0.0], "content": "doc"},
        ]

        def handler(_event, _market, _docs, _prompt):
            raise RuntimeError("boom")

        with patch.object(rag_handler, "load_rag_config", return_value=cfg), \
             patch.object(rag_handler, "embed_texts", side_effect=lambda texts, _cfg: [[1.0, 0.0] for _ in texts]), \
             patch.object(rag_handler, "_resolve_handler", return_value=handler), \
             patch.object(rag_handler, "_upsert_doc"), \
             patch.object(rag_handler, "_embed_missing_docs"), \
             patch.object(rag_handler, "_fetch_docs", return_value=stored_docs), \
             patch.object(rag_handler.logger, "exception") as logged, \
             patch("src.rag.rag_handler.psycopg.connect", return_value=FakeConn(), create=True), \
             patch.dict(os.environ, {"DATABASE_URL": "db"}):
            preds = rag_handler.predict_event(
                {"event_ticker": "EV", "title": "Event"},
                [{"ticker": "M1", "implied_yes_mid": "0.4"}],
                {},
                "Prompt",
            )
        self.assertEqual(preds[0]["yes_prob"], Decimal("0.400000"))
        logged.assert_called()

    def test_predict_event_skips_missing_market_ticker(self) -> None:
        cfg = make_cfg(use_baseline=True)
        with patch.object(rag_handler, "load_rag_config", return_value=cfg), \
             patch.object(rag_handler, "embed_texts", side_effect=lambda texts, _cfg: [[1.0, 0.0] for _ in texts]), \
             patch.object(rag_handler, "_upsert_doc"), \
             patch.object(rag_handler, "_embed_missing_docs"), \
             patch.object(rag_handler, "_fetch_docs", return_value=[]), \
             patch("src.rag.rag_handler.psycopg.connect", return_value=FakeConn(), create=True), \
             patch.dict(os.environ, {"DATABASE_URL": "db"}):
            preds = rag_handler.predict_event(
                {"event_ticker": "EV", "title": "Event"},
                [{"ticker": None, "implied_yes_mid": "0.7"}],
                {},
                "Prompt",
            )
        self.assertEqual(preds, [])

    def test_predict_event_skips_missing_yes_prob(self) -> None:
        cfg = make_cfg()
        stored_docs = [
            {"id": 1, "event_ticker": "EV", "market_ticker": "M1", "embedding": [1.0, 0.0], "content": "doc"},
        ]
        with patch.object(rag_handler, "load_rag_config", return_value=cfg), \
             patch.object(rag_handler, "embed_texts", side_effect=lambda texts, _cfg: [[1.0, 0.0] for _ in texts]), \
             patch.object(rag_handler, "_upsert_doc"), \
             patch.object(rag_handler, "_embed_missing_docs"), \
             patch.object(rag_handler, "_fetch_docs", return_value=stored_docs), \
             patch("src.rag.rag_handler.psycopg.connect", return_value=FakeConn(), create=True), \
             patch.dict(os.environ, {"DATABASE_URL": "db"}):
            preds = rag_handler.predict_event(
                {"event_ticker": "EV", "title": "Event"},
                [{"ticker": "M1", "implied_yes_mid": "0.5"}],
                {},
                "Prompt",
            )
        self.assertEqual(preds, [])
