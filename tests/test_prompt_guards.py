from __future__ import annotations

from pathlib import Path

import pytest


SENTINEL = "STRATEGY.md is PO-controlled and read-only; STOP/BLOCKED on write attempts."


@pytest.mark.parametrize(
    "relative_path",
        [
            "docs/props/proposal_manager.md",
            "docs/props/strategy_agent.md",
            "skills/prompt_standard/contract.md",
            "docs/gov/AGENTS.md",
            "docs/specs/CLI_COMMANDS.md",
        ],
    )
def test_strategy_read_only_guard_present(relative_path: str) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    content = (repo_root / relative_path).read_text(encoding="utf-8")
    assert (
        SENTINEL in content
    ), f"Missing sentinel guard text in {relative_path}; expected '{SENTINEL}'"
