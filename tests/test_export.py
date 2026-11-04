import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from ctfbridge.models.challenge import Challenge

from ctfdl.challenges.entry import run_export
from ctfdl.core.config import ExportConfig
from ctfdl.rendering.context import TemplateEngineContext


def test_export_index_keeps_skipped_challenges(tmp_path, monkeypatch):
    challenges = [Challenge(id="1", name="First challenge", categories=["web"])]

    async def iter_all(**_filters):
        for challenge in challenges:
            yield challenge

    client = SimpleNamespace(challenges=SimpleNamespace(iter_all=iter_all))
    monkeypatch.setattr(
        "ctfdl.challenges.downloader.get_authenticated_client", AsyncMock(return_value=client)
    )
    monkeypatch.setattr(TemplateEngineContext, "_instance", None)
    config = ExportConfig(url="https://ctf.example", output=tmp_path)

    asyncio.run(run_export(config))
    index_path = tmp_path / "README.md"
    first_readme = tmp_path / "web" / "first-challenge" / "README.md"
    assert "First challenge" in index_path.read_text(encoding="utf-8")
    first_readme.write_text("My notes", encoding="utf-8")

    challenges.append(Challenge(id="2", name="Second challenge", categories=["web"]))
    asyncio.run(run_export(config))

    index = index_path.read_text(encoding="utf-8")
    assert "[Link](web/first-challenge)" in index
    assert "[Link](web/second-challenge)" in index
    assert first_readme.read_text(encoding="utf-8") == "My notes"
