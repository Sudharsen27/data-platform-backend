"""Tests for unified LLM provider resolution and fallback."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.services import llm_provider


@pytest.fixture(autouse=True)
def _clear_llm_env(monkeypatch):
    for name in (
        "AI_PROVIDER",
        "GROQ_API_KEY",
        "GROQ_MODEL",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_DEPLOYMENT",
    ):
        monkeypatch.delenv(name, raising=False)


def test_resolve_groq_when_configured(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "groq")
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    assert llm_provider.resolve_active_provider() == "groq"
    assert llm_provider.llm_is_available() is True


def test_resolve_groq_missing_key(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "groq")
    assert llm_provider.resolve_active_provider() is None
    assert llm_provider.llm_is_available() is False


def test_resolve_azure_when_configured(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "azure_openai")
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://example.openai.azure.com")
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "azure-key")
    monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
    assert llm_provider.resolve_active_provider() == "azure_openai"
    assert llm_provider.active_model_name() == "gpt-4o"


def test_azure_falls_back_to_groq(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "azure_openai")
    monkeypatch.setenv("GROQ_API_KEY", "fallback-groq")
    assert llm_provider.resolve_active_provider() == "groq"
    status = llm_provider.get_llm_status()
    assert status["configured_provider"] == "azure_openai"
    assert status["active_provider"] == "groq"
    assert status["fallback_to_groq"] is True


def test_chat_completion_json_uses_active_provider(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "groq")
    monkeypatch.setenv("GROQ_API_KEY", "test-key")

    payload = {"choices": [{"message": {"content": '{"title": "Test"}'}}]}

    with patch("app.services.llm_provider.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = payload
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        data, engine = llm_provider.chat_completion_json(
            system_prompt="sys",
            user_prompt="user",
        )

    assert data == {"title": "Test"}
    assert engine == "groq"
    mock_client.post.assert_called_once()


def test_chat_completion_text_azure_path(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "azure_openai")
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://example.openai.azure.com")
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "azure-key")
    monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

    payload = {"choices": [{"message": {"content": "Azure answer"}}]}

    with patch("app.services.llm_provider.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = payload
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        text, engine = llm_provider.chat_completion_text(
            system_prompt="sys",
            user_prompt="user",
        )

    assert text == "Azure answer"
    assert engine == "azure_openai"
    call_kwargs = mock_client.post.call_args
    assert "openai.azure.com" in call_kwargs[0][0]
    assert call_kwargs[1]["headers"]["api-key"] == "azure-key"
