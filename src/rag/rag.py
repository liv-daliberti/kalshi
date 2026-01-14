"""Azure-based RAG embedding + LLM handlers."""

from __future__ import annotations

import json
import logging
import os
from urllib.parse import urlsplit
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)


try:
    from openai import AzureOpenAI as _AzureOpenAI
    from openai import OpenAI as _OpenAI
    from openai import OpenAIError as _OpenAIError
except ImportError:  # pragma: no cover - optional dependency
    _OpenAI = None
    _AzureOpenAI = None

    class _OpenAIError(Exception):
        """Fallback OpenAI error when openai package is absent."""


OpenAI = _OpenAI
AzureOpenAI = _AzureOpenAI
OpenAIError = _OpenAIError

try:  # pragma: no cover - optional dependency
    import yaml as _yaml
except ImportError:  # pragma: no cover - optional dependency
    _yaml = None  # type: ignore


@dataclass(frozen=True)
class AzureCfg:
    """Azure OpenAI configuration values."""

    endpoint: str
    api_key: str
    deployment: str
    api_version: str
    use_v1: bool


_CLIENT_STATE: dict[str, Any] = {"client": None, "uses_v1": False, "cfg": None}
_EMBED_CLIENT_STATE: dict[str, Any] = {"client": None, "cfg": None}


def _load_yaml(path: Optional[str]) -> dict[str, Any]:
    if not path:
        return {}
    if _yaml is None:
        return {}
    path_obj = Path(path)
    if not path_obj.exists():
        return {}
    with path_obj.open("r", encoding="utf-8") as handle:
        return _yaml.safe_load(handle) or {}


def _normalize_endpoint(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return raw
    parsed = urlsplit(raw)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    marker = raw.lower().find("/openai/")
    if marker != -1:
        return raw[:marker].rstrip("/")
    return raw.rstrip("/")


def _load_azure_config() -> AzureCfg:
    yaml_path = os.getenv("RAG_AZURE_CONFIG_PATH")
    ycfg = _load_yaml(yaml_path)
    defaults = {
        "endpoint": "",
        "api_key": "",
        "deployment": "",
        "api_version": "2024-12-01-preview",
        "use_v1": False,
    }
    endpoint = _normalize_endpoint(
        os.getenv("AZURE_OPENAI_ENDPOINT", ycfg.get("endpoint", defaults["endpoint"]))
        or defaults["endpoint"]
    )
    api_key = os.getenv("AZURE_OPENAI_API_KEY", ycfg.get("api_key", defaults["api_key"]))
    deployment = os.getenv(
        "AZURE_OPENAI_DEPLOYMENT",
        ycfg.get("deployment", defaults["deployment"]),
    )
    api_version = os.getenv(
        "AZURE_OPENAI_API_VERSION",
        ycfg.get("api_version", defaults["api_version"]),
    )
    use_v1 = bool(
        int(
            os.getenv(
                "AZURE_OPENAI_USE_V1",
                str(int(ycfg.get("use_v1", defaults["use_v1"]))),
            )
        )
    )
    if not endpoint or not api_key or not api_version:
        raise RuntimeError(
            "Missing Azure config. Set AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, "
            "AZURE_OPENAI_API_VERSION (or provide RAG_AZURE_CONFIG_PATH)."
        )
    return AzureCfg(
        endpoint=endpoint,
        api_key=api_key,
        deployment=deployment,
        api_version=api_version,
        use_v1=use_v1,
    )


def _load_embedding_config() -> AzureCfg:
    base = _load_azure_config()
    endpoint_override = (
        os.getenv("RAG_EMBEDDING_ENDPOINT") or os.getenv("AZURE_OPENAI_EMBEDDING_ENDPOINT")
    )
    api_version_override = (
        os.getenv("RAG_EMBEDDING_API_VERSION")
        or os.getenv("AZURE_OPENAI_EMBEDDING_API_VERSION")
    )
    endpoint = _normalize_endpoint(endpoint_override) if endpoint_override else base.endpoint
    api_version = api_version_override or base.api_version
    return AzureCfg(
        endpoint=endpoint,
        api_key=base.api_key,
        deployment=base.deployment,
        api_version=api_version,
        use_v1=False,
    )


def _build_preferred_client(cfg: AzureCfg) -> Tuple[object, bool]:
    endpoint = cfg.endpoint.rstrip("/")
    if cfg.use_v1 and OpenAI is not None:
        base_url = f"{endpoint}/openai/v1/"
        try:
            client = OpenAI(base_url=base_url, api_key=cfg.api_key)
            logger.info("Using Responses API (v1) at %s", base_url)
            return client, True
        except OpenAIError as exc:
            logger.warning("Failed to init v1 Responses client (%s): %s", base_url, exc)

    if AzureOpenAI is None:
        raise RuntimeError("openai>=1.x with AzureOpenAI is required (pip install -U openai)")

    client = AzureOpenAI(
        api_key=cfg.api_key,
        azure_endpoint=endpoint,
        api_version=cfg.api_version,
    )
    logger.info("Using Azure Chat Completions at %s (api_version=%s)", endpoint, cfg.api_version)
    return client, False


def _get_client() -> Tuple[object, bool, AzureCfg]:
    if _CLIENT_STATE["client"] is not None:
        return _CLIENT_STATE["client"], _CLIENT_STATE["uses_v1"], _CLIENT_STATE["cfg"]
    cfg = _load_azure_config()
    client, uses_v1 = _build_preferred_client(cfg)
    _CLIENT_STATE["client"] = client
    _CLIENT_STATE["uses_v1"] = uses_v1
    _CLIENT_STATE["cfg"] = cfg
    return client, uses_v1, cfg


def _get_embedding_client() -> object:
    if _EMBED_CLIENT_STATE["client"] is not None:
        return _EMBED_CLIENT_STATE["client"]
    cfg = _load_embedding_config()
    if AzureOpenAI is None:
        raise RuntimeError("openai>=1.x with AzureOpenAI is required (pip install -U openai)")
    client = AzureOpenAI(
        api_key=cfg.api_key,
        azure_endpoint=cfg.endpoint.rstrip("/"),
        api_version=cfg.api_version,
    )
    _EMBED_CLIENT_STATE["client"] = client
    _EMBED_CLIENT_STATE["cfg"] = cfg
    logger.info(
        "Using Azure Embeddings at %s (api_version=%s)",
        cfg.endpoint,
        cfg.api_version,
    )
    return client


def _extract_text(resp: Any, uses_v1: bool) -> str:
    if uses_v1 and hasattr(resp, "output_text"):
        return resp.output_text or ""
    try:
        return resp.choices[0].message.content or ""
    except (AttributeError, IndexError, TypeError):
        return ""


def _json_from_text(text: str) -> Optional[dict[str, Any]]:
    text = (text or "").strip()
    if not text:
        return None
    if text.startswith("{") and text.endswith("}"):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return None
    return None


def _build_docs_blob(retrieved_docs: list[dict[str, Any]]) -> str:
    return "\n\n".join(
        f"[{doc.get('source')}:{doc.get('source_id')}] {doc.get('content')}"
        for doc in retrieved_docs
        if doc.get("content")
    )


def _build_llm_prompts(
    event: dict[str, Any],
    market: dict[str, Any],
    retrieved_docs: list[dict[str, Any]],
    prompt: str,
) -> tuple[str, str]:
    docs_blob = _build_docs_blob(retrieved_docs)
    system_prompt = (
        "You are a market forecasting assistant. Return ONLY a JSON object with keys: "
        "yes_prob (0-1), confidence (0-1, optional), rationale (short string)."
    )
    user_prompt = (
        f"{prompt}\n\n"
        f"Event: {event.get('title')}\n"
        f"Market: {market.get('market_title') or market.get('ticker')}\n"
        f"Market subtitle: {market.get('market_subtitle')}\n"
        f"Rules: {market.get('rules_primary')}\n"
        f"Context:\n{docs_blob}"
    )
    return system_prompt, user_prompt


def _resolve_llm_deployment(cfg: AzureCfg) -> str:
    deployment = os.getenv("RAG_LLM_DEPLOYMENT") or cfg.deployment
    if not deployment:
        raise RuntimeError("Set RAG_LLM_DEPLOYMENT (or AZURE_OPENAI_DEPLOYMENT).")
    return deployment


def _load_llm_settings() -> tuple[float, int]:
    temperature = float(os.getenv("RAG_LLM_TEMPERATURE", "0.2"))
    max_tokens = int(os.getenv("RAG_LLM_MAX_TOKENS", "200"))
    return temperature, max_tokens


def _format_prediction_payload(payload: Optional[dict[str, Any]]) -> dict[str, Any]:
    payload = payload or {}
    return {
        "yes_prob": payload.get("yes_prob"),
        "confidence": payload.get("confidence"),
        "rationale": payload.get("rationale"),
        "raw": payload,
    }


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Embed text strings using the configured Azure deployment."""
    client = _get_embedding_client()
    deployment = (
        os.getenv("RAG_EMBEDDING_DEPLOYMENT")
        or os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")
        or ""
    )
    if not deployment:
        raise RuntimeError("Set RAG_EMBEDDING_DEPLOYMENT for Azure embeddings.")
    resp = client.embeddings.create(model=deployment, input=texts)
    return [item.embedding for item in resp.data]


def predict_market(
    event: dict[str, Any],
    market: dict[str, Any],
    retrieved_docs: list[dict[str, Any]],
    prompt: str,
) -> dict[str, Any]:
    """Predict market probabilities using RAG context and Azure OpenAI."""
    client, uses_v1, cfg = _get_client()
    deployment = _resolve_llm_deployment(cfg)
    system_prompt, user_prompt = _build_llm_prompts(
        event,
        market,
        retrieved_docs,
        prompt,
    )
    temperature, max_tokens = _load_llm_settings()

    if uses_v1:
        text = _extract_text(
            client.responses.create(
                model=deployment,
                input=[
                    {"role": "system", "content": [{"type": "text", "text": system_prompt}]},
                    {"role": "user", "content": [{"type": "text", "text": user_prompt}]},
                ],
                temperature=temperature,
                max_output_tokens=max_tokens,
            ),
            uses_v1=True,
        )
    else:
        text = _extract_text(
            client.chat.completions.create(
                model=deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=temperature,
                max_tokens=max_tokens,
            ),
            uses_v1=False,
        )

    return _format_prediction_payload(_json_from_text(text))
