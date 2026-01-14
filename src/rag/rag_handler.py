"""RAG prediction handler with embedding persistence."""

from __future__ import annotations

import hashlib
import logging
import json
import math
import os
import re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Optional

import psycopg  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error
from dateutil.parser import isoparse

from src.core.env_utils import parse_bool, parse_int
from src.core.guardrails import assert_state_write_allowed
from src.core.number_utils import normalize_probability
from src.rag.rag_documents import fetch_rag_documents
from src.predictions.prediction_utils import baseline_market_prob

logger = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_MAX_DOC_CHARS = 2000
_RAG_CALL_WINDOW_KEY = "rag_24h_calls"
_RAG_CALL_WINDOW_SECONDS = 24 * 60 * 60
_RAG_CALL_LIMIT_DEFAULT = 50000

EmbeddingHandler = Callable[[list[str]], list[list[float]]]
LLMHandler = Callable[[dict[str, Any], dict[str, Any], list[dict[str, Any]], str], dict[str, Any]]


@dataclass(frozen=True)
class EmbeddingConfig:
    """Embedding configuration for RAG."""

    embedding_dim: int
    embedding_model: str
    embedding_space: str
    vector_space: str
    normalize: bool


@dataclass(frozen=True)
class RetrievalConfig:
    """Retrieval configuration for RAG."""

    top_k: int
    doc_limit: int
    use_baseline: bool


@dataclass(frozen=True)
class HandlerConfig:
    """Optional handler overrides for RAG."""

    embedding_handler: Optional[str]
    llm_handler: Optional[str]


@dataclass(frozen=True)
class RagConfig:
    """Aggregate configuration for the RAG handler."""

    embedding: EmbeddingConfig
    retrieval: RetrievalConfig
    handlers: HandlerConfig


@dataclass(frozen=True)
class DocBatch:
    """Documents and embeddings ready for persistence."""

    docs: list[dict[str, Any]]
    embeddings: list[list[float]]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class RetrievalContext:
    """Context used for document retrieval."""

    event_ticker: str
    market_tickers: list[str]
    context_docs: list[dict[str, Any]]


@dataclass(frozen=True)
class PredictionContext:
    """Context used for per-market prediction."""

    event: dict[str, Any]
    stored_docs: list[dict[str, Any]]
    cfg: RagConfig
    prompt: str
    llm_handler: LLMHandler | None
    event_ticker: str


def load_rag_config() -> RagConfig:
    """Load RAG config values from the environment."""
    embedding_dim = parse_int(os.getenv("RAG_EMBEDDING_DIM"), 384, minimum=16)
    embedding_model = os.getenv("RAG_EMBEDDING_MODEL") or f"hashing-{embedding_dim}"
    embedding_space = os.getenv("RAG_EMBEDDING_SPACE") or "hashing-tf"
    vector_space = os.getenv("RAG_VECTOR_SPACE") or "cosine"
    top_k = parse_int(os.getenv("RAG_TOP_K"), 8, minimum=1)
    doc_limit = parse_int(os.getenv("RAG_DOC_LIMIT"), 200, minimum=1)
    normalize = parse_bool(os.getenv("RAG_EMBEDDING_NORMALIZE"), default=True)
    use_baseline = parse_bool(os.getenv("RAG_USE_BASELINE"), default=True)
    embedding_handler = os.getenv("RAG_EMBEDDING_HANDLER")
    llm_handler = os.getenv("RAG_LLM_HANDLER")
    return RagConfig(
        embedding=EmbeddingConfig(
            embedding_dim=embedding_dim,
            embedding_model=embedding_model,
            embedding_space=embedding_space,
            vector_space=vector_space,
            normalize=normalize,
        ),
        retrieval=RetrievalConfig(
            top_k=top_k,
            doc_limit=doc_limit,
            use_baseline=use_baseline,
        ),
        handlers=HandlerConfig(
            embedding_handler=embedding_handler,
            llm_handler=llm_handler,
        ),
    )


def _resolve_handler(path: str) -> Callable[..., Any]:
    module_name, attr = path.rsplit(":", 1)
    module = __import__(module_name, fromlist=[attr])
    handler = getattr(module, attr)
    if not callable(handler):
        raise TypeError(f"Handler not callable: {path}")
    return handler


def _hash_embedding(text: str, dim: int) -> list[float]:
    vec = [0.0] * dim
    tokens = _TOKEN_RE.findall(text.lower())
    if not tokens:
        return vec
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        idx = int.from_bytes(digest[:4], "big") % dim
        sign = 1.0 if (digest[4] & 1) else -1.0
        vec[idx] += sign
    return vec


def _normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0.0:
        return vec
    return [v / norm for v in vec]


def embed_texts(texts: list[str], cfg: RagConfig) -> list[list[float]]:
    """Generate embeddings for the provided texts."""
    if cfg.handlers.embedding_handler:
        try:
            handler = _resolve_handler(cfg.handlers.embedding_handler)
            vectors = handler(texts)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Embedding handler failed; using hashing fallback")
            vectors = [_hash_embedding(text, cfg.embedding.embedding_dim) for text in texts]
    else:
        vectors = [_hash_embedding(text, cfg.embedding.embedding_dim) for text in texts]
    if cfg.embedding.normalize:
        vectors = [_normalize(vec) for vec in vectors]
    return vectors


def _cosine(vec_a: list[float], vec_b: list[float]) -> Optional[float]:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return None
    dot = sum(x * y for x, y in zip(vec_a, vec_b))
    if dot == 0.0:
        return 0.0
    return dot


def _normalize_prob(value: Any) -> Optional[Decimal]:
    return normalize_probability(value, quantize=Decimal("0.000001"))


def _build_event_doc(event: dict[str, Any]) -> dict[str, Any]:
    parts = [
        event.get("title"),
        event.get("sub_title"),
        event.get("category"),
        event.get("strike_period"),
    ]
    metadata = event.get("product_metadata")
    if metadata:
        parts.append(json.dumps(metadata, ensure_ascii=True))
    content = "\n".join(str(p) for p in parts if p)
    return {
        "source": "event",
        "source_id": event.get("event_ticker"),
        "event_ticker": event.get("event_ticker"),
        "market_ticker": None,
        "content": content,
    }


def _build_market_doc(event_ticker: str, market: dict[str, Any]) -> dict[str, Any]:
    parts = [
        market.get("market_title"),
        market.get("market_subtitle"),
        market.get("yes_sub_title"),
        market.get("no_sub_title"),
        market.get("rules_primary"),
        market.get("rules_secondary"),
    ]
    content = "\n".join(str(p) for p in parts if p)
    return {
        "source": "market",
        "source_id": market.get("ticker"),
        "event_ticker": event_ticker,
        "market_ticker": market.get("ticker"),
        "content": content,
    }


def _metadata_for_embedding(cfg: RagConfig) -> dict[str, Any]:
    return {
        "embedding_dim": cfg.embedding.embedding_dim,
        "embedding_model": cfg.embedding.embedding_model,
        "embedding_space": cfg.embedding.embedding_space,
        "vector_space": cfg.embedding.vector_space,
        "normalized": cfg.embedding.normalize,
    }


def _clamp_text(text: Any, limit: int = _MAX_DOC_CHARS) -> str:
    if text is None:
        return ""
    raw = str(text)
    if len(raw) <= limit:
        return raw
    return raw[:limit]


def _upsert_doc(
    conn: psycopg.Connection,
    doc: dict[str, Any],
    embedding: list[float],
    metadata: dict[str, Any],
) -> None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id
            FROM rag_documents
            WHERE source = %s
              AND source_id = %s
              AND event_ticker IS NOT DISTINCT FROM %s
              AND market_ticker IS NOT DISTINCT FROM %s
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (
                doc.get("source"),
                doc.get("source_id"),
                doc.get("event_ticker"),
                doc.get("market_ticker"),
            ),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE rag_documents
                SET content=%s,
                    embedding=%s,
                    metadata=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (
                    doc.get("content"),
                    json.dumps(embedding),
                    json.dumps(metadata),
                    row["id"],
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO rag_documents(
                  source, source_id, event_ticker, market_ticker,
                  content, embedding, metadata, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (
                    doc.get("source"),
                    doc.get("source_id"),
                    doc.get("event_ticker"),
                    doc.get("market_ticker"),
                    doc.get("content"),
                    json.dumps(embedding),
                    json.dumps(metadata),
                ),
            )


def _fetch_docs(
    conn: psycopg.Connection,
    event_ticker: str,
    market_tickers: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    return fetch_rag_documents(conn, event_ticker, market_tickers, limit)


def _embed_missing_docs(
    conn: psycopg.Connection,
    docs: list[dict[str, Any]],
    cfg: RagConfig,
) -> None:
    missing = [doc for doc in docs if not doc.get("embedding")]
    if not missing:
        return
    texts = [doc.get("content") or "" for doc in missing]
    vectors = embed_texts(texts, cfg)
    metadata = _metadata_for_embedding(cfg)
    with conn.cursor() as cur:
        for doc, embedding in zip(missing, vectors):
            cur.execute(
                """
                UPDATE rag_documents
                SET embedding=%s,
                    metadata=COALESCE(metadata, %s),
                    updated_at=NOW()
                WHERE id=%s
                """,
                (json.dumps(embedding), json.dumps(metadata), doc.get("id")),
            )
            doc["embedding"] = embedding


def _retrieve_docs(
    docs: list[dict[str, Any]],
    query_vec: list[float],
    market_ticker: str,
    event_ticker: str,
    top_k: int,
) -> list[dict[str, Any]]:
    scored = []
    for doc in docs:
        if doc.get("event_ticker") not in {event_ticker, None}:
            continue
        doc_market = doc.get("market_ticker")
        if doc_market and doc_market != market_ticker:
            continue
        embedding = doc.get("embedding")
        if not isinstance(embedding, list):
            continue
        score = _cosine(query_vec, embedding)
        if score is None:
            continue
        scored.append(
            {
                "id": doc.get("id"),
                "score": score,
                "source": doc.get("source"),
                "source_id": doc.get("source_id"),
                "market_ticker": doc.get("market_ticker"),
                "content": _clamp_text(doc.get("content")),
                "metadata": doc.get("metadata"),
            }
        )
    scored.sort(key=lambda item: item["score"], reverse=True)
    return scored[:top_k]


def _baseline_prob(market: dict[str, Any]) -> Optional[Decimal]:
    yes_prob, _ = baseline_market_prob(market, _normalize_prob)
    return yes_prob


def _kalshi_chance_score(market: dict[str, Any]) -> Optional[Decimal]:
    chance = _baseline_prob(market)
    if chance is not None:
        return chance
    return _normalize_prob(market.get("price_dollars"))


def _parse_window_start(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = isoparse(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _parse_call_window(raw: str | None) -> tuple[datetime | None, int | None, int | None]:
    if not raw:
        return None, None, None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None, None, None
    window_start = _parse_window_start(payload.get("window_start"))
    count_value = payload.get("count")
    limit_value = payload.get("limit")
    try:
        count = int(count_value)
    except (TypeError, ValueError):
        count = None
    try:
        limit = int(limit_value)
    except (TypeError, ValueError):
        limit = None
    return window_start, count, limit


def _serialize_call_window(
    window_start: datetime,
    count: int,
    limit: int,
) -> str:
    payload = {
        "window_start": window_start.isoformat(),
        "count": max(0, int(count)),
        "limit": max(1, int(limit)),
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=True)


def _rag_call_limit() -> int:
    return parse_int(
        os.getenv("RAG_DAILY_LIMIT"),
        _RAG_CALL_LIMIT_DEFAULT,
        minimum=1,
    )


def _reserve_rag_calls(desired_calls: int, now: datetime) -> int:
    if desired_calls <= 0:
        return 0
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.warning("RAG daily limit skipped; DATABASE_URL is not set.")
        return desired_calls
    assert_state_write_allowed(_RAG_CALL_WINDOW_KEY)
    limit = _rag_call_limit()
    default_payload = _serialize_call_window(now, 0, limit)
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingest_state(key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO NOTHING
                """,
                (_RAG_CALL_WINDOW_KEY, default_payload),
            )
            cur.execute(
                "SELECT value FROM ingest_state WHERE key = %s FOR UPDATE",
                (_RAG_CALL_WINDOW_KEY,),
            )
            row = cur.fetchone()
            window_start, count, stored_limit = _parse_call_window(
                row[0] if row else None
            )
            if window_start is None:
                window_start = now
                count = 0
            if count is None:
                count = 0
            if (now - window_start).total_seconds() >= _RAG_CALL_WINDOW_SECONDS:
                window_start = now
                count = 0
            if stored_limit is None or stored_limit < 1:
                stored_limit = limit
            remaining = max(0, limit - count)
            allowed = min(remaining, desired_calls)
            payload = _serialize_call_window(window_start, count + allowed, limit)
            cur.execute(
                "UPDATE ingest_state SET value = %s, updated_at = NOW() WHERE key = %s",
                (payload, _RAG_CALL_WINDOW_KEY),
            )
    if allowed < desired_calls:
        logger.warning(
            "RAG daily limit reached; allowing %d of %d calls.",
            allowed,
            desired_calls,
        )
    return allowed


def _has_valid_nonzero_chance(market: dict[str, Any]) -> bool:
    chance = _kalshi_chance_score(market)
    return chance is not None and chance > 0


def _build_docs(event: dict[str, Any], markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the list of documents to embed for an event and its markets."""
    event_ticker = event.get("event_ticker")
    docs = [_build_event_doc(event)]
    if event_ticker:
        docs.extend(_build_market_doc(event_ticker, market) for market in markets)
    return docs


def _persist_docs(
    conn: psycopg.Connection,
    doc_batch: DocBatch,
    retrieval_ctx: RetrievalContext,
    cfg: RagConfig,
) -> list[dict[str, Any]]:
    """Persist embeddings and load stored documents for retrieval."""
    for doc, embedding in zip(doc_batch.docs, doc_batch.embeddings):
        _upsert_doc(conn, doc, embedding, doc_batch.metadata)
    _embed_missing_docs(conn, retrieval_ctx.context_docs, cfg)
    stored_docs = _fetch_docs(
        conn,
        retrieval_ctx.event_ticker,
        retrieval_ctx.market_tickers,
        cfg.retrieval.doc_limit,
    )
    _embed_missing_docs(conn, stored_docs, cfg)
    return stored_docs


def _build_query_texts(
    event: dict[str, Any],
    markets: list[dict[str, Any]],
    prompt: str,
) -> list[str]:
    """Build query strings for each market."""
    query_texts = []
    for market in markets:
        parts = [
            prompt,
            event.get("title"),
            event.get("sub_title"),
            market.get("market_title"),
            market.get("market_subtitle"),
            market.get("rules_primary"),
        ]
        query_texts.append("\n".join(str(p) for p in parts if p))
    return query_texts


def _load_llm_handler(cfg: RagConfig) -> LLMHandler | None:
    """Resolve the configured LLM handler when present."""
    if not cfg.handlers.llm_handler:
        return None
    try:
        return _resolve_handler(cfg.handlers.llm_handler)
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("LLM handler load failed; falling back to baseline")
        return None


def _prepare_stored_docs(
    event: dict[str, Any],
    markets: list[dict[str, Any]],
    context: dict[str, Any],
    cfg: RagConfig,
    event_ticker: str,
) -> list[dict[str, Any]]:
    """Persist embeddings and return stored docs for retrieval."""
    market_tickers = [m.get("ticker") for m in markets if m.get("ticker")]
    docs = _build_docs(event, markets)
    texts = [doc.get("content") or "" for doc in docs]
    embeddings = embed_texts(texts, cfg)
    metadata = _metadata_for_embedding(cfg)
    if embeddings:
        metadata["embedding_dim"] = len(embeddings[0])
    doc_batch = DocBatch(docs=docs, embeddings=embeddings, metadata=metadata)
    retrieval_ctx = RetrievalContext(
        event_ticker=event_ticker,
        market_tickers=market_tickers,
        context_docs=context.get("documents") or [],
    )

    conn = psycopg.connect(os.environ["DATABASE_URL"])
    try:
        with conn:
            stored_docs = _persist_docs(conn, doc_batch, retrieval_ctx, cfg)
    finally:
        conn.close()
    return stored_docs


def _predict_market(
    market: dict[str, Any],
    query_vec: list[float],
    context: PredictionContext,
) -> dict[str, Any] | None:
    """Build a prediction payload for a single market, if possible."""
    ticker = market.get("ticker")
    if not ticker:
        return None
    retrieved = _retrieve_docs(
        context.stored_docs,
        query_vec,
        ticker,
        context.event_ticker,
        context.cfg.retrieval.top_k,
    )
    pred: dict[str, Any] = {}
    if context.llm_handler is not None:
        try:
            pred = context.llm_handler(context.event, market, retrieved, context.prompt) or {}
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("LLM handler failed for market=%s", ticker)
            pred = {}
    yes_prob = _normalize_prob(pred.get("yes_prob") or pred.get("predicted_yes_prob"))
    confidence = _normalize_prob(pred.get("confidence"))
    rationale = pred.get("rationale")

    if yes_prob is None and context.cfg.retrieval.use_baseline:
        yes_prob = _baseline_prob(market)
    if yes_prob is None:
        return None
    return {
        "market_ticker": ticker,
        "yes_prob": yes_prob,
        "confidence": confidence,
        "rationale": rationale or "rag-retrieval",
        "raw": {
            "retrieved": retrieved,
            "embedding_model": context.cfg.embedding.embedding_model,
            "embedding_space": context.cfg.embedding.embedding_space,
            "vector_space": context.cfg.embedding.vector_space,
        },
    }


def _build_prediction_context(
    event: dict[str, Any],
    markets: list[dict[str, Any]],
    context: dict[str, Any],
    prompt: str,
    cfg: RagConfig,
    llm_handler: LLMHandler | None,
) -> tuple[PredictionContext, list[list[float]]] | None:
    """Assemble the prediction context and query vectors for an event."""
    event_ticker = event.get("event_ticker")
    if not event_ticker:
        return None
    stored_docs = _prepare_stored_docs(event, markets, context, cfg, event_ticker)
    query_texts = _build_query_texts(event, markets, prompt)
    query_vectors = embed_texts(query_texts, cfg)
    prediction_ctx = PredictionContext(
        event=event,
        stored_docs=stored_docs,
        cfg=cfg,
        prompt=prompt,
        llm_handler=llm_handler,
        event_ticker=event_ticker,
    )
    return prediction_ctx, query_vectors


def _predict_markets(
    markets: list[dict[str, Any]],
    query_vectors: list[list[float]],
    prediction_ctx: PredictionContext,
    concurrency: int,
) -> list[dict[str, Any]]:
    """Generate predictions for each market, optionally concurrent."""
    predictions: list[dict[str, Any]] = []
    if concurrency <= 1 or len(markets) <= 1:
        for market, query_vec in zip(markets, query_vectors):
            pred = _predict_market(
                market,
                query_vec,
                prediction_ctx,
            )
            if pred is not None:
                predictions.append(pred)
        return predictions

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_map = {
            executor.submit(_predict_market, market, query_vec, prediction_ctx): market
            for market, query_vec in zip(markets, query_vectors)
        }
        for future in as_completed(future_map):
            market = future_map[future]
            ticker = market.get("ticker") or market.get("market_ticker") or "unknown"
            try:
                pred = future.result()
            except Exception:  # pylint: disable=broad-exception-caught
                logger.exception("Prediction failed for market=%s", ticker)
                continue
            if pred is not None:
                predictions.append(pred)
    return predictions


def predict_event(
    event: dict[str, Any],
    markets: list[dict[str, Any]],
    context: dict[str, Any],
    prompt: str,
) -> list[dict[str, Any]]:
    """Predict outcome probabilities for each market in an event."""
    eligible_markets = [market for market in markets if _has_valid_nonzero_chance(market)]
    if not eligible_markets:
        return []
    cfg = load_rag_config()
    llm_handler = _load_llm_handler(cfg)
    if llm_handler is not None:
        now = datetime.now(timezone.utc)
        allowed_calls = _reserve_rag_calls(len(eligible_markets), now)
        if allowed_calls <= 0:
            return []
        if allowed_calls < len(eligible_markets):
            eligible_markets = eligible_markets[:allowed_calls]
    prepared = _build_prediction_context(
        event,
        eligible_markets,
        context,
        prompt,
        cfg,
        llm_handler,
    )
    if prepared is None:
        return []
    prediction_ctx, query_vectors = prepared
    concurrency = parse_int(os.getenv("RAG_LLM_CONCURRENCY"), 5, minimum=1)
    return _predict_markets(eligible_markets, query_vectors, prediction_ctx, concurrency)
