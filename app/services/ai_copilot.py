"""AI copilot: optional Ollama LLM with rule-based fallback (no Azure/cloud required)."""

from __future__ import annotations

import os
import re

import httpx

_TRUE = frozenset({"1", "true", "yes", "on"})


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in _TRUE


def ai_enabled() -> bool:
    return _env_bool("AI_ENABLED", default=True)


def ai_provider() -> str:
    return os.getenv("AI_PROVIDER", "heuristic").strip().lower() or "heuristic"


def ollama_base_url() -> str:
    return os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")


def ollama_model() -> str:
    return os.getenv("OLLAMA_MODEL", "llama3.2:3b").strip() or "llama3.2:3b"


def ollama_timeout_seconds() -> float:
    try:
        return max(5.0, float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "90")))
    except ValueError:
        return 90.0


def ollama_is_reachable() -> bool:
    try:
        with httpx.Client(timeout=3.0) as client:
            response = client.get(f"{ollama_base_url()}/api/tags")
            return response.status_code == 200
    except (httpx.HTTPError, OSError):
        return False


def get_ai_status() -> dict:
    enabled = ai_enabled()
    provider = ai_provider()
    model = ""

    if not enabled:
        return {
            "enabled": False,
            "provider": provider,
            "model": "",
            "available": False,
            "mode": "disabled",
        }

    if provider in ("groq", "azure_openai"):
        from app.services.llm_provider import (
            active_model_name,
            get_llm_status,
            llm_is_available,
            resolve_active_provider,
        )

        status = get_llm_status()
        active = resolve_active_provider()
        return {
            "enabled": True,
            "provider": provider,
            "active_provider": active,
            "model": active_model_name(active),
            "available": llm_is_available(),
            "mode": active if llm_is_available() else "heuristics",
            "fallback_to_groq": status.get("fallback_to_groq", False),
            "azure_openai_configured": status.get("azure_openai_configured", False),
            "groq_configured": status.get("groq_configured", False),
        }

    if provider == "ollama":
        model = ollama_model()
        reachable = ollama_is_reachable()
        return {
            "enabled": True,
            "provider": "ollama",
            "model": model,
            "available": reachable,
            "mode": "ollama" if reachable else "heuristics",
        }

    return {
        "enabled": True,
        "provider": "heuristic",
        "model": "rule-engine",
        "available": True,
        "mode": "heuristics",
    }


def explain_quarantine_heuristic(*, name: str, email: str, error: str) -> str:
    err = (error or "").strip()
    err_low = err.lower()
    email_val = (email or "").strip()
    name_val = (name or "").strip()

    if not err:
        return (
            "This record is in quarantine but has no error message attached. "
            "Review name and email against active quality rules, then clear or fix the row."
        )

    if "invalid email" in err_low or ("email" in err_low and "@" not in email_val):
        return (
            f"The email '{email_val or '(empty)'}' failed validation — it must include '@' "
            "and a domain (e.g. name@company.com). Correct the email or approve an exception "
            "after steward review."
        )

    if "duplicate" in err_low:
        return (
            f"Duplicate detected for '{email_val or name_val or 'this record'}'. "
            "Another source system or an existing master row may already own this identifier. "
            "Merge with the golden record or reject the duplicate source row."
        )

    if "required" in err_low or "missing" in err_low:
        missing = "name" if not name_val else "email" if not email_val else "a required field"
        return (
            f"Required data is missing ({missing}). Populate mandatory attributes before "
            "re-running the pipeline or approving stewardship."
        )

    if "format" in err_low or "pattern" in err_low:
        return (
            f"Field format does not match the rule: {err}. "
            "Align the value with the catalog definition or update the rule if the source is authoritative."
        )

    if "timeout" in err_low or "connection" in err_low:
        return (
            f"Operational error during ingest: {err}. "
            "This is usually transient — retry sync after connectivity is restored."
        )

    return (
        f"Quality rule violation: {err}. "
        f"Record context — name: '{name_val or '(empty)'}', email: '{email_val or '(empty)'}'. "
        "Fix the underlying value, waive with documented approval, or route to a data steward."
    )


def _ollama_explain(*, name: str, email: str, error: str) -> str | None:
    prompt = (
        "You are a data governance copilot. In 2-4 short sentences, explain this "
        "quarantined master-data row to a business user and suggest one fix.\n"
        f"Name: {name or '(empty)'}\n"
        f"Email: {email or '(empty)'}\n"
        f"Error: {error or '(none)'}\n"
    )
    try:
        with httpx.Client(timeout=ollama_timeout_seconds()) as client:
            response = client.post(
                f"{ollama_base_url()}/api/generate",
                json={
                    "model": ollama_model(),
                    "prompt": prompt,
                    "stream": False,
                },
            )
            if response.status_code != 200:
                return None
            text = (response.json().get("response") or "").strip()
            return text or None
    except (httpx.HTTPError, OSError, ValueError, TypeError):
        return None


def explain_quarantine(*, name: str, email: str, error: str) -> dict:
    if not ai_enabled():
        return {
            "explanation": explain_quarantine_heuristic(name=name, email=email, error=error),
            "source": "rules",
        }

    provider = ai_provider()
    if provider == "ollama" and ollama_is_reachable():
        llm_text = _ollama_explain(name=name, email=email, error=error)
        if llm_text:
            cleaned = re.sub(r"\s+", " ", llm_text).strip()
            return {"explanation": cleaned, "source": "ollama"}

    return {
        "explanation": explain_quarantine_heuristic(name=name, email=email, error=error),
        "source": "heuristics",
    }
