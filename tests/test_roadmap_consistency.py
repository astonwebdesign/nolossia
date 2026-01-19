from __future__ import annotations

from pathlib import Path

from scripts import roadmap_tools


def test_roadmap_matches_generated_output() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    index_path = repo_root / "docs/props/INDEX.md"
    roadmap_path = repo_root / "docs/roadmap/ROADMAP.md"

    expected = roadmap_tools.render_roadmap(repo_root=repo_root, index_path=index_path)
    actual = roadmap_path.read_text(encoding="utf-8")
    assert actual == expected, "ROADMAP.md drift detected; run `python docs/scripts/update_roadmap.py`."

