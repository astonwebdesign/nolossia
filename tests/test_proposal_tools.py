from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from scripts import proposal_tools


def _write_proposal(
    path: Path,
    proposal_id: str,
    *,
    status: str = "accepted",
    validation_notes: str | None = "",
) -> None:
    path.write_text(
        textwrap.dedent(
            f"""\
            ---
            id: {proposal_id}
            source: test
            created: 2025-12-21
            status: {status}
            priority_hint: P1
            owner: test
            links:
              - test
            success_metric: ensure stuff works
            validation_notes: {validation_notes or ""}
            ---

            ## Finding
            test

            ## Evidence
            - test
            """
        ),
        encoding="utf-8",
    )


def test_validate_id_accepts_allowed_prefix() -> None:
    proposal_id = proposal_tools.validate_id("GOV-2025-007")
    assert proposal_id.prefix == "GOV"
    assert proposal_id.year == 2025
    assert proposal_id.number == 7


def test_validate_id_rejects_invalid_prefix() -> None:
    with pytest.raises(proposal_tools.ProposalIDError):
        proposal_tools.validate_id("BAD-2025-001")


def test_extract_filename_id_supports_suffix() -> None:
    candidate = proposal_tools.extract_filename_id(
        Path("PERF-2025-001-executor-selection.md")
    )
    assert candidate == "PERF-2025-001"


def test_collect_and_validate_records(tmp_path: Path) -> None:
    props = tmp_path / "props"
    accepted = props / "accepted"
    accepted.mkdir(parents=True)
    valid = accepted / "GOV-2025-002.md"
    invalid = accepted / "UX-2025-003-extra.md"

    _write_proposal(valid, "GOV-2025-002", validation_notes="ok")
    _write_proposal(invalid, "DOCS-2025-003")

    records = proposal_tools.collect_proposals(base_path=props)
    errors = proposal_tools.validate_records(records)

    assert len(records) == 2
    assert any("Mismatch between filename ID" in error for error in errors)
    assert any(rec.validation_notes == "ok" for rec in records if rec.proposal_id)


def test_collect_proposals_includes_released(tmp_path: Path) -> None:
    repo_docs = tmp_path / "docs"
    props = repo_docs / "props"
    accepted = props / "accepted"
    accepted.mkdir(parents=True)
    roadmap = repo_docs / "roadmap" / "released"
    roadmap.mkdir(parents=True)
    _write_proposal(accepted / "GOV-2025-010.md", "GOV-2025-010", validation_notes="done")
    _write_proposal(
        roadmap / "SAFETY-2025-010.md",
        "SAFETY-2025-010",
        status="released",
        validation_notes="tests",
    )

    records = proposal_tools.collect_proposals(base_path=props)
    lifecycles = {rec.lifecycle for rec in records}
    assert "released" in lifecycles
    released_paths = [rec.path for rec in records if rec.lifecycle == "released"]
    assert any("roadmap" in str(path) for path in released_paths)


def test_next_proposal_id_increments(tmp_path: Path) -> None:
    props = tmp_path / "props"
    accepted = props / "accepted"
    accepted.mkdir(parents=True)
    _write_proposal(accepted / "GOV-2025-001.md", "GOV-2025-001")
    _write_proposal(accepted / "GOV-2025-002.md", "GOV-2025-002")

    next_id = proposal_tools.next_proposal_id(prefix="GOV", year=2025, base_path=props)
    assert next_id == "GOV-2025-003"


def test_next_proposal_id_cli(tmp_path: Path) -> None:
    props = tmp_path / "props"
    accepted = props / "accepted"
    accepted.mkdir(parents=True)
    _write_proposal(accepted / "UX-2025-005.md", "UX-2025-005")

    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "docs/scripts/next_proposal_id.py",
            "UX",
            "--year",
            "2025",
            "--base",
            str(props),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "UX-2025-006"


def test_check_proposal_ids_cli_reports_errors(tmp_path: Path) -> None:
    props = tmp_path / "props"
    accepted = props / "accepted"
    accepted.mkdir(parents=True)
    _write_proposal(accepted / "PERF-2025-001.md", "PERF-2025-002")

    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "docs/scripts/check_proposal_ids.py",
            "--base",
            str(props),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "Mismatch between filename ID" in result.stdout or result.stderr


def test_check_release_candidates_cli(tmp_path: Path) -> None:
    repo_docs = tmp_path / "docs"
    props = repo_docs / "props"
    accepted = props / "accepted"
    accepted.mkdir(parents=True)
    roadmap = repo_docs / "roadmap" / "released"
    roadmap.mkdir(parents=True)
    tasks_dir = repo_docs / "tasks"
    tasks_dir.mkdir(parents=True)
    todo = tasks_dir / "TODO.md"
    todo.write_text("# TODO\n", encoding="utf-8")

    ready_file = accepted / "GOV-2025-020.md"
    blocked_file = accepted / "UX-2025-020.md"
    _write_proposal(ready_file, "GOV-2025-020", validation_notes="tests pass")
    _write_proposal(blocked_file, "UX-2025-020")

    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "docs/scripts/check_release_candidates.py",
            "--base",
            str(props),
            "--todo",
            str(todo),
            "--show-blockers",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "GOV-2025-020" in result.stdout
    assert "UX-2025-020" in result.stdout
    assert "missing_validation_notes" in result.stdout
