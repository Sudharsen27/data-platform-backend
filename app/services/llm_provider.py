"""Unified LLM provider abstraction — Groq and Azure OpenAI with fallback."""

from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.parse import urlparse

import httpx

from app.services.ai_copilot import ai_provider

_LLM_PROVIDERS = frozenset({"groq", "azure_openai"})


def groq_api_key() -> str:
    return os.getenv("GROQ_API_KEY", "").strip()


def groq_model() -> str:
    return os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip() or "llama-3.3-70b-versatile"


def groq_timeout_seconds() -> float:
    try:
        return max(10.0, float(os.getenv("GROQ_TIMEOUT_SECONDS", "60")))
    except ValueError:
        return 60.0


def groq_is_configured() -> bool:
    return bool(groq_api_key())


def azure_openai_endpoint() -> str:
    return os.getenv("AZURE_OPENAI_ENDPOINT", "").strip().rstrip("/")


def azure_openai_api_key() -> str:
    return os.getenv("AZURE_OPENAI_API_KEY", "").strip()


def azure_openai_deployment() -> str:
    return os.getenv("AZURE_OPENAI_DEPLOYMENT", "").strip()


def azure_openai_api_version() -> str:
    return os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview").strip()


def azure_openai_timeout_seconds() -> float:
    try:
        return max(10.0, float(os.getenv("AZURE_OPENAI_TIMEOUT_SECONDS", "60")))
    except ValueError:
        return 60.0


def azure_openai_is_configured() -> bool:
    endpoint = azure_openai_endpoint()
    if not endpoint or not azure_openai_api_key() or not azure_openai_deployment():
        return False
    parsed = urlparse(endpoint)
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)


def resolve_active_provider() -> str | None:
    """
    Resolve which cloud LLM to use based on AI_PROVIDER and configuration.
    When AI_PROVIDER=azure_openai and Azure is unavailable, fall back to Groq.
    """
    configured = ai_provider()
    if configured == "azure_openai":
        if azure_openai_is_configured():
            return "azure_openai"
        if groq_is_configured():
            return "groq"
        return None
    if configured == "groq":
        return "groq" if groq_is_configured() else None
    return None


def llm_is_available() -> bool:
    return resolve_active_provider() is not None


def active_model_name(provider: str | None = None) -> str:
    active = provider or resolve_active_provider() or ""
    if active == "azure_openai":
        return azure_openai_deployment()
    if active == "groq":
        return groq_model()
    return ""


def _strip_json_fences(raw: str) -> str:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _post_groq(*, messages: list[dict[str, str]], temperature: float, max_tokens: int) -> str:
    api_key = groq_api_key()
    if not api_key:
        raise ValueError("GROQ_API_KEY is not configured")

    with httpx.Client(timeout=groq_timeout_seconds()) as client:
        response = client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": groq_model(),
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
        )
        if response.status_code != 200:
            raise RuntimeError(f"Groq API error ({response.status_code}): {response.text[:300]}")
        return _extract_message_text(response.json(), provider_label="Groq")


def _post_azure_openai(*, messages: list[dict[str, str]], temperature: float, max_tokens: int) -> str:
    endpoint = azure_openai_endpoint()
    deployment = azure_openai_deployment()
    api_key = azure_openai_api_key()
    if not azure_openai_is_configured():
        raise ValueError("Azure OpenAI is not fully configured")

    url = (
        f"{endpoint}/openai/deployments/{deployment}/chat/completions"
        f"?api-version={azure_openai_api_version()}"
    )
    with httpx.Client(timeout=azure_openai_timeout_seconds()) as client:
        response = client.post(
            url,
            headers={
                "api-key": api_key,
                "Content-Type": "application/json",
            },
            json={
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Azure OpenAI API error ({response.status_code}): {response.text[:300]}"
            )
        return _extract_message_text(response.json(), provider_label="Azure OpenAI")


def _extract_message_text(data: dict[str, Any], *, provider_label: str) -> str:
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError(f"{provider_label} API returned no choices")
    message = choices[0].get("message") or {}
    text = (message.get("content") or "").strip()
    if not text:
        raise RuntimeError(f"{provider_label} API returned empty content")
    return text


def chat_completion_text(
    *,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.2,
    max_tokens: int = 1024,
    provider: str | None = None,
) -> tuple[str, str]:
    """
    Run a chat completion using the resolved provider.
    Returns (text, source_engine) where source_engine is groq or azure_openai.
    """
    active = provider or resolve_active_provider()
    if not active:
        raise ValueError("No LLM provider is configured")

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    if active == "azure_openai":
        text = _post_azure_openai(messages=messages, temperature=temperature, max_tokens=max_tokens)
        return re.sub(r"\s+", " ", text).strip(), "azure_openai"
    if active == "groq":
        text = _post_groq(messages=messages, temperature=temperature, max_tokens=max_tokens)
        return re.sub(r"\s+", " ", text).strip(), "groq"
    raise ValueError(f"Unsupported LLM provider: {active}")


def chat_completion_json(
    *,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.25,
    max_tokens: int = 900,
    provider: str | None = None,
) -> tuple[dict[str, Any], str]:
    """Chat completion expecting a JSON object response."""
    text, engine = chat_completion_text(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=temperature,
        max_tokens=max_tokens,
        provider=provider,
    )
    raw = _strip_json_fences(text)
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise RuntimeError("LLM response was not a JSON object")
    return data, engine


def get_llm_status() -> dict[str, Any]:
    """Status block for health / AI status endpoints."""
    configured = ai_provider()
    active = resolve_active_provider()
    fallback = configured == "azure_openai" and active == "groq"
    return {
        "configured_provider": configured,
        "active_provider": active,
        "model": active_model_name(active),
        "groq_configured": groq_is_configured(),
        "azure_openai_configured": azure_openai_is_configured(),
        "fallback_to_groq": fallback,
        "available": active is not None,
    }
