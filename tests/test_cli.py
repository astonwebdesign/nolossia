import io
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pytest
from PIL import Image

from src import cli, hashing
from src.cli_formatter import CLIFormatter, FormatterConfig, DEFAULT_LINE_WIDTH
from src.exceptions import MergeExecutionError, StorageError
from src.models.actions import MarkNearDuplicateAction, MoveMasterAction, MoveToQuarantineExactAction
from src.models.cluster import DuplicateCluster
from src.models.fileinfo import FileInfo
from src.models.mergeplan import MergePlan


def test_scan_read_only(tmp_path, capsys, monkeypatch):
    img = tmp_path / "img.jpg"
    Image.new("RGB", (2, 2), color="red").save(img)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", str(tmp_path)])

    inputs = iter(["y", "n"])  # proceed to dedupe, then stop before merge
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))

    cli.main()
    captured = capsys.readouterr()
    assert "STEP 1/3" in captured.out
    assert "STEP 2/3" in captured.out
    non_empty = [line for line in captured.out.splitlines() if line.strip()]
    assert any("SUMMARY" in line for line in non_empty[:20])
    # file should remain (read-only command)
    assert img.exists()


def test_no_supported_images_message(tmp_path, capsys, monkeypatch):
    txt = tmp_path / "note.txt"
    txt.write_text("hello")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", str(tmp_path)])
    inputs = iter(["", ""])  # defaults
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))
    cli.main()
    captured = capsys.readouterr()
    assert "No supported image files were found" in captured.out


def test_scan_invalid_path(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", "/not/real"])
    with pytest.raises(SystemExit):
        cli.main()
    captured = capsys.readouterr()
    assert "[ERROR] One or more input paths are invalid." in captured.err or captured.out


def test_wizard_dry_run_no_mutation(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    img = src_dir / "img.jpg"
    Image.new("RGB", (2, 2), color="blue").save(img)
    target = tmp_path / "out"

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", str(src_dir)])
    inputs = iter(["y", "y", str(target), ""])  # dedupe yes, merge yes, target, do not execute
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 0
    captured = capsys.readouterr()
    assert "STEP 3/3" in captured.out
    assert "Masters storage" in captured.out
    assert "Exact → Isolated for safety" in captured.out
    assert "STOP/BLOCKED" in captured.out
    assert not target.exists()
    assert img.exists()


def test_wizard_quick_mode_shorter_steps(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    img = src_dir / "img.jpg"
    Image.new("RGB", (2, 2), color="purple").save(img)
    target = tmp_path / "out"

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", "--quick", str(src_dir)])
    inputs = iter(["y", "y", str(target), ""])  # continue, build plan, target, do not execute
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 0
    captured = capsys.readouterr()
    assert "STEP 1/2" in captured.out
    assert "STEP 2/2" in captured.out
    assert "Glossary" not in captured.out


def test_start_pipe_mode_blocked(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "pipe", "start"])
    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert '"status":"BLOCKED"' in captured.out
    assert '"phase":"START"' in captured.out


def test_start_glossary_toggle(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "--no-banner", "start"])
    inputs = iter(["exit"])
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))
    cli.main()
    captured = capsys.readouterr()
    assert "Glossary" in captured.out

    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "--no-banner", "start", "--no-glossary"])
    inputs = iter(["exit"])
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))
    cli.main()
    captured = capsys.readouterr()
    assert "Glossary" not in captured.out


def test_wizard_execute_moves_files(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    img = src_dir / "img.jpg"
    Image.new("RGB", (2, 2), color="green").save(img)
    target = tmp_path / "out"

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", str(src_dir)])
    inputs = iter(["y", "y", str(target), "EXECUTE"])  # dedupe yes, merge yes, target, execute
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))

    cli.main()
    captured = capsys.readouterr()
    assert "Merge execution complete" in captured.out
    assert "merge_report.html" in captured.out
    moved_files = list(target.rglob("img.jpg"))
    assert moved_files


def test_execute_aborted_does_not_move(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    img = src_dir / "img.jpg"
    Image.new("RGB", (2, 2), color="yellow").save(img)
    target = tmp_path / "out"

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", str(src_dir)])
    inputs = iter(["", "y", str(target), ""])  # final prompt empty
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))

    cli.main()
    captured = capsys.readouterr()
    assert "No files have been changed." in captured.out
    assert not target.exists()
    assert img.exists()


def test_dedupe_flow_outputs_report_link(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("src.cli._prompt", lambda *args, **kwargs: "n")

    photo = tmp_path / "a.jpg"
    photo.write_bytes(b"123")
    fi = FileInfo(
        path=str(photo),
        size=photo.stat().st_size,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="abc",
        phash="abcd",
        is_raw=False,
    )
    clusters: list[DuplicateCluster] = []
    formatter = CLIFormatter(FormatterConfig(show_banner=False))

    cli._dedupe_flow([], [fi], clusters, unique_size=fi.size, formatter=formatter)
    captured = capsys.readouterr()
    assert "dedupe_report.html" in captured.out
    assert "per-file review" in captured.out
    assert "Duplicate clustering finished" in captured.out


def test_dedupe_flow_link_fallback_when_osc8_disabled(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("src.cli._prompt", lambda *args, **kwargs: "n")
    photo = tmp_path / "b.jpg"
    photo.write_bytes(b"123")
    fi = FileInfo(
        path=str(photo),
        size=photo.stat().st_size,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="def",
        phash="efgh",
        is_raw=False,
    )
    formatter = CLIFormatter(FormatterConfig(show_banner=False))
    formatter.config.osc8_links = False

    cli._dedupe_flow([], [fi], [], unique_size=fi.size, formatter=formatter)
    captured = capsys.readouterr()
    assert "dedupe_report.html" in captured.out


def test_merge_pipe_mode_emits_single_line(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="pink").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["prog", "--mode", "pipe", "merge", str(src_dir), "--out", str(dest_dir)]
    )
    cli.main()
    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert len(lines) == 1
    summary_line = lines[0]
    assert "Review the reports above first." not in summary_line
    payload = json.loads(summary_line)
    assert payload["schema_version"] == "1.0"
    assert payload["status"] == "DRY_RUN"
    assert payload["phase"] == "merge"
    assert "required" in payload["storage"]
    assert payload["reports"]
    assert "\u001b[" not in summary_line


def test_merge_pipe_mode_stream_json_emits_multiple_lines(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "stream_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="salmon").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "stream_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "pipe", "--stream-json", "merge", str(src_dir), "--out", str(dest_dir)],
    )
    cli.main()
    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert len(lines) == 3
    phases = []
    for line in lines:
        payload = json.loads(line)
        assert payload["schema_version"] == "1.0"
        phases.append(payload["phase"])
    assert phases == ["scan", "dedupe", "merge"]


def test_merge_pipe_mode_kv_format(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "kv_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="green").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "kv_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "pipe", "--pipe-format", "kv", "merge", str(src_dir), "--out", str(dest_dir)],
    )
    cli.main()
    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert len(lines) == 1
    summary_line = lines[0]
    assert summary_line.startswith("status=DRY_RUN")
    assert "reports=" in summary_line
    assert "storage_breakdown=" in summary_line
    assert "\u001b[" not in summary_line
    assert "Advanced settings" not in summary_line
    assert "Look-alike sensitivity" not in summary_line


def test_merge_preview_mode_includes_no_changes_yet_and_no_execute_cues(
    tmp_path, capsys, monkeypatch
):
    src_dir = tmp_path / "preview_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="teal").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "preview_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--no-banner", "merge", str(src_dir), "--out", str(dest_dir)],
    )
    cli.main()
    captured = capsys.readouterr()
    assert "Preview only (no changes yet)" in captured.out
    assert "Review the plan, then decide whether to move files." in captured.out
    assert "Review the reports above first." in captured.out
    assert "EXECUTE is live" not in captured.out
    assert "Type 'EXECUTE' to move files now" not in captured.out
    assert "near-duplicate" not in captured.out


def test_merge_non_default_sensitivity_shows_banner(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "sensitivity_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="olive").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "sensitivity_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--no-banner", "merge", str(src_dir), "--out", str(dest_dir)],
    )

    prompts = iter(["y", "balanced", "__DEFAULT__"])

    def prompt_with_default(_formatter, _message, default=""):
        value = next(prompts, "")
        return default if value == "__DEFAULT__" else value

    monkeypatch.setattr(cli, "_prompt", prompt_with_default)

    cli.main()
    captured = capsys.readouterr()
    assert "Look-alike sensitivity: Balanced" in captured.out


def test_merge_aggressive_sensitivity_requires_confirmation(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "aggressive_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="purple").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "aggressive_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--no-banner", "merge", str(src_dir), "--out", str(dest_dir)],
    )

    prompts = iter(["y", "aggressive", "", "__DEFAULT__"])

    def prompt_with_default(_formatter, _message, default=""):
        value = next(prompts, "")
        return default if value == "__DEFAULT__" else value

    monkeypatch.setattr(cli, "_prompt", prompt_with_default)

    cli.main()
    captured = capsys.readouterr()
    assert "Aggressive sensitivity not confirmed" in captured.out
    assert "Look-alike sensitivity: Aggressive" not in captured.out


def test_merge_aggressive_sensitivity_confirmation_shows_banner(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "aggressive_confirm_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="indigo").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "aggressive_confirm_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--no-banner", "merge", str(src_dir), "--out", str(dest_dir)],
    )

    prompts = iter(["y", "aggressive", "AGGRESSIVE", "__DEFAULT__"])

    def prompt_with_default(_formatter, _message, default=""):
        value = next(prompts, "")
        return default if value == "__DEFAULT__" else value

    monkeypatch.setattr(cli, "_prompt", prompt_with_default)

    cli.main()
    captured = capsys.readouterr()
    assert "Look-alike sensitivity: Aggressive (may increase review load)" in captured.out


def test_merge_execute_mode_includes_execute_cues_and_no_preview_only(
    tmp_path, capsys, monkeypatch
):
    src_dir = tmp_path / "execute_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="maroon").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "execute_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "--mode",
            "tty",
            "--no-banner",
            "merge",
            str(src_dir),
            "--out",
            str(dest_dir),
            "--execute",
        ],
    )
    prompts = iter(["__DEFAULT__", "__DEFAULT__", "EXECUTE"])

    def prompt_with_default(_formatter, _message, default=""):
        value = next(prompts, "")
        return default if value == "__DEFAULT__" else value

    monkeypatch.setattr(cli, "_prompt", prompt_with_default)
    cli.main()
    captured = capsys.readouterr()
    assert "EXECUTE is live. Files will move now." in captured.out
    assert "Preview only (no changes yet)" not in captured.out
    assert "Thank you" not in captured.out


def test_merge_plain_mode_includes_preview_cue_without_color(
    tmp_path, capsys, monkeypatch
):
    src_dir = tmp_path / "plain_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="black").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "plain_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "--mode",
            "tty",
            "--plain",
            "--no-banner",
            "merge",
            str(src_dir),
            "--out",
            str(dest_dir),
        ],
    )
    cli.main()
    captured = capsys.readouterr()
    assert "Preview only (no changes yet)" in captured.out
    assert "EXECUTE is live" not in captured.out


def test_merge_ascii_execute_includes_execute_cue_without_color(
    tmp_path, capsys, monkeypatch
):
    src_dir = tmp_path / "ascii_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="white").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "ascii_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "--mode",
            "tty",
            "--ascii",
            "--no-banner",
            "merge",
            str(src_dir),
            "--out",
            str(dest_dir),
            "--execute",
        ],
    )
    prompts = iter(["__DEFAULT__", "__DEFAULT__", "EXECUTE"])

    def prompt_with_default(_formatter, _message, default=""):
        value = next(prompts, "")
        return default if value == "__DEFAULT__" else value

    monkeypatch.setattr(cli, "_prompt", prompt_with_default)
    cli.main()
    captured = capsys.readouterr()
    assert "EXECUTE is live. Files will move now." in captured.out
    assert "Preview only (no changes yet)" not in captured.out


def test_merge_tty_includes_reports_block(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "tty_reports_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="silver").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "tty_reports_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--no-banner", "merge", str(src_dir), "--out", str(dest_dir)],
    )
    cli.main()
    captured = capsys.readouterr()
    assert "Reports:" in captured.out
    assert "FILE:" in captured.out
    assert "PATH:" in captured.out
    assert "OPEN:" in captured.out


def test_merge_reports_block_compact_pattern(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "reports_compact_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="red").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "reports_compact_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--no-banner", "merge", str(src_dir), "--out", str(dest_dir)],
    )
    cli.main()
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    file_lines = [line for line in lines if "FILE:" in line]
    path_lines = [line for line in lines if "PATH:" in line]
    open_lines = [line for line in lines if "OPEN:" in line]
    assert file_lines
    assert path_lines
    assert open_lines
    assert all("PATH:" not in line and "OPEN:" not in line for line in file_lines)
    assert all("FILE:" not in line and "OPEN:" not in line for line in path_lines)
    assert all("FILE:" not in line and "PATH:" not in line for line in open_lines)


def test_merge_pipe_mode_excludes_tty_guidance(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "pipe_reports_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="gray").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "pipe_reports_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["prog", "--mode", "pipe", "merge", str(src_dir), "--out", str(dest_dir)]
    )
    cli.main()
    captured = capsys.readouterr()
    assert "Reports:" not in captured.out
    assert "FILE:" not in captured.out
    assert "PATH:" not in captured.out
    assert "OPEN:" not in captured.out
    assert "Next steps:" not in captured.out


def test_merge_pipe_mode_non_visual_contract(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "pipe_non_visual_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="olive").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "pipe_non_visual_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["prog", "--mode", "pipe", "merge", str(src_dir), "--out", str(dest_dir)]
    )
    cli.main()
    captured = capsys.readouterr()
    forbidden = ["┌", "┐", "└", "┘", "│", "─", "•", "◆", "◇", "✓", "✗", "⚠", "ℹ", "→", "⇒"]
    assert all(token not in captured.out for token in forbidden)


def test_merge_pipe_stream_json_non_visual_contract(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "pipe_stream_non_visual_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="tan").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "pipe_stream_non_visual_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "pipe", "--stream-json", "merge", str(src_dir), "--out", str(dest_dir)],
    )
    cli.main()
    captured = capsys.readouterr()
    forbidden = ["┌", "┐", "└", "┘", "│", "─", "•", "◆", "◇", "✓", "✗", "⚠", "ℹ", "→", "⇒"]
    assert all(token not in captured.out for token in forbidden)


def test_merge_storage_error_failure_summary(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    dest_dir = tmp_path / "dest"
    src_dir.mkdir()
    dest_dir.mkdir()
    Image.new("RGB", (2, 2), color="cyan").save(src_dir / "img.jpg")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--no-banner", "merge", str(src_dir), "--out", str(dest_dir)],
    )

    prompts = iter(["__DEFAULT__", "__DEFAULT__"])

    def fake_prompt(_formatter, _message, default=""):
        value = next(prompts, "__DEFAULT__")
        return default if value == "__DEFAULT__" else value

    monkeypatch.setattr(cli, "_prompt", fake_prompt)

    def boom(*_args, **_kwargs):
        raise StorageError(
            "Insufficient storage for merge operation (required 2 GB, available 1 GB)"
        )

    monkeypatch.setattr(cli.merge_engine, "build_merge_plan", boom)

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert "STOP/BLOCKED" in captured.out
    assert "Insufficient storage" in captured.out
    assert "Log file" in captured.out


def test_merge_storage_error_pipe_failure_summary(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    dest_dir = tmp_path / "dest"
    src_dir.mkdir()
    dest_dir.mkdir()
    Image.new("RGB", (2, 2), color="purple").save(src_dir / "img.jpg")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "pipe", "merge", str(src_dir), "--out", str(dest_dir)],
    )

    def raise_storage(*_args, **_kwargs):
        raise StorageError(
            "Insufficient storage for merge operation (required 2 GB, available 1 GB)"
        )

    monkeypatch.setattr(cli.merge_engine, "build_merge_plan", raise_storage)

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["schema_version"] == "1.0"
    assert payload["status"] == "FAILED"
    assert payload["phase"] == "Merge plan"
    assert payload["reason"].startswith("Insufficient storage")
    assert payload["log"].endswith("nolossia.log")


def test_merge_execute_abort_outputs_summary(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    dest_dir = tmp_path / "dest"
    src_dir.mkdir()
    dest_dir.mkdir()
    Image.new("RGB", (2, 2), color="lime").save(src_dir / "img.jpg")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--no-banner", "merge", str(src_dir), "--out", str(dest_dir), "--execute"],
    )

    prompts = iter(["__DEFAULT__", "__DEFAULT__", ""])

    def aborting_prompt(_formatter, _message, default=""):
        value = next(prompts, "")
        return default if value == "__DEFAULT__" else value

    monkeypatch.setattr(cli, "_prompt", aborting_prompt)

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 0
    captured = capsys.readouterr()
    assert "STOP/BLOCKED" in captured.out
    assert "User cancelled merge execution" in captured.out
    assert not any(dest_dir.rglob("img.jpg"))


def test_merge_execute_failure_shows_summary(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    dest_dir = tmp_path / "dest"
    src_dir.mkdir()
    dest_dir.mkdir()
    Image.new("RGB", (2, 2), color="navy").save(src_dir / "img.jpg")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--no-banner", "merge", str(src_dir), "--out", str(dest_dir), "--execute"],
    )

    prompts = iter(["__DEFAULT__", "__DEFAULT__", "EXECUTE"])

    def prompt_with_default(_formatter, _message, default=""):
        value = next(prompts, "")
        return default if value == "__DEFAULT__" else value

    monkeypatch.setattr(cli, "_prompt", prompt_with_default)

    def explode(_plan):
        raise MergeExecutionError("Merge execution failed: Hash mismatch for sample.jpg")

    monkeypatch.setattr(cli.merge_engine, "execute_merge", explode)

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert "STOP/BLOCKED" in captured.out
    assert "Hash mismatch" in captured.out


def test_merge_dry_run_step_three_within_line_budget(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="orange").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "merge", str(src_dir), "--out", str(dest_dir)])
    cli.main()
    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    step_index = next(i for i, line in enumerate(lines) if "STEP 3/3" in line)
    phase_lines = lines[step_index:]
    assert any("SUMMARY" in line for line in phase_lines[:10])
    assert len(phase_lines) <= 40


def test_print_merge_plan_summary_shows_review_samples():
    formatter = CLIFormatter(FormatterConfig(show_banner=False), stream=io.StringIO())
    summary = {
        "masters_count": 1,
        "masters_size": "1 MB",
        "exact_to_quar_count": 0,
        "exact_to_quar_size": "0 B",
        "near_marks": 0,
        "near_marks_size": "0 B",
        "review_count": 1,
        "review_samples": ["REVIEW/orphan.jpg — Missing photo date (EXIF)"],
        "required": "1 MB",
        "free": "10 MB",
        "free_after": "9 MB",
        "corrupt_count": 0,
        "reports": [],
        "chronology_rows": [],
        "out_path": "/library",
        "source_paths": "/input",
        "folder_merges_count": 0,
        "filename_collisions_count": 0,
        "mergeplan_json_path": "merge_plan.json",
        "dedupe_report_path": "dedupe_report.html",
        "merge_report_path": "merge_report.html",
        "manifest_json_path": "source_manifest.json",
        "manifest_csv_path": "source_manifest.csv",
        "manifest_html_path": "source_manifest.html",
        "storage_breakdown": {"masters": "0 B", "quarantine": "0 B", "review": "1 MB"},
        "storage_breakdown_bytes": {"masters": 0, "quarantine": 0, "review": 1_000_000},
    }
    cli._print_merge_plan_summary(summary, formatter, pipe_status=None)
    output = formatter.stream.getvalue()
    assert "Sample review files" in output
    assert "REVIEW/orphan.jpg" in output
    assert "merge_report.html" in output


def test_cli_verbose_flag_emits_duplicate_messages(monkeypatch, tmp_path, capsys):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    raw_path = src_dir / "raw.dng"
    raw_path.write_bytes(b"raw")
    jpeg_path = src_dir / "dup.jpg"
    Image.new("RGB", (2, 2), color="white").save(jpeg_path)

    dt = datetime(2020, 1, 1)
    raw_info = FileInfo(
        path=str(raw_path),
        size=raw_path.stat().st_size,
        format="dng",
        resolution=(4000, 3000),
        exif_datetime=dt,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="same",
        phash="aaaa",
        is_raw=True,
    )
    jpeg_info = FileInfo(
        path=str(jpeg_path),
        size=jpeg_path.stat().st_size,
        format="jpg",
        resolution=(4000, 3000),
        exif_datetime=dt,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="same",
        phash="aaab",
        is_raw=False,
    )

    monkeypatch.setattr(cli.scanner, "scan_paths", lambda paths: [raw_info, jpeg_info])
    monkeypatch.setattr(cli.metadata, "enrich_metadata", lambda files: files)
    monkeypatch.setattr(cli.hashing, "add_hashes", lambda files: files)

    inputs = iter(["y", "n"])
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--verbose", "scan", str(src_dir)],
    )

    cli.main()
    captured = capsys.readouterr()
    assert "[duplicates]" in captured.out
    assert "RAW_BEATS_JPEG" in captured.out
    assert "kept because" in captured.out


def test_scan_flow_uses_cli_formatter(monkeypatch, tmp_path, capsys):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="purple").save(src_dir / "img.jpg")

    inputs = iter(["y", "n"])
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", str(src_dir)])

    calls: list[tuple[str, tuple]] = []

    class RecordingFormatter:
        def __init__(self, *args, stream=None, **kwargs):
            calls.append(("init", ()))
            self.config = FormatterConfig()
            self.line_width = DEFAULT_LINE_WIDTH
            self.stream = stream

        def print_banner(self):
            calls.append(("print_banner", ()))

        def section(self, title, icon="◆"):
            calls.append(("section", (title, icon)))

        def line(self, text=""):
            calls.append(("line", (text,)))

        def blank(self):
            calls.append(("blank", ()))

        def kv(self, label, value, width=28, indent=1):
            calls.append(("kv", (label, value, width, indent)))

        def warning(self, text):
            calls.append(("warning", (text,)))

        def success(self, text):
            calls.append(("success", (text,)))

        def info(self, text):
            calls.append(("info", (text,)))

        def error(self, text):
            calls.append(("error", (text,)))

        def muted(self, text):
            calls.append(("muted", (text,)))

        def bullet(self, text, indent="  - "):
            calls.append(("bullet", (text, indent)))

        def link(self, path, label=None):
            result = label or path
            calls.append(("link", (path, result)))
            return result

        def prompt(self, message):
            calls.append(("prompt", (message,)))
            return message

        def label(self, text, level="info", bold=True):
            result = f"[{level}]{text}"
            calls.append(("label", (text, level, bold)))
            return result

        def style(self, text, color=None, bold=False):
            calls.append(("style", (text, color, bold)))
            return text

    monkeypatch.setattr(cli, "CLIFormatter", RecordingFormatter)
    monkeypatch.setattr(cli, "detect_terminal_capabilities", lambda **_: FormatterConfig())

    cli.main()
    captured = capsys.readouterr()
    assert "Scan complete:" not in captured.out  # no direct print output
    formatter_calls = [call for call in calls if call[0] != "init"]
    assert not any(name == "print_banner" for name, _ in formatter_calls)
    assert any(
        name == "label" and "STEP" in args[0]
        for name, args in formatter_calls
        if name == "label"
    )
    assert any("SUMMARY" in args[0] for name, args in formatter_calls if name == "line")
    prompt_messages = [args[0] for name, args in calls if name == "prompt"]
    assert prompt_messages[0] == "→ Proceed to the next step? [y/N] (Enter cancels): "
    assert prompt_messages[1] == "→ Start the 'merge wizard'? [y/N] (Enter exits): "


def test_scan_direct_omits_banner_and_glossary(tmp_path, capsys, monkeypatch):
    img = tmp_path / "img.jpg"
    Image.new("RGB", (2, 2), color="red").save(img)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NOLOSSIA_TAGLINE", "TESTTAG")
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", str(tmp_path)])
    inputs = iter([""])
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))

    cli.main()
    captured = capsys.readouterr()
    assert "TESTTAG" not in captured.out
    assert "Glossary" not in captured.out


def test_dedupe_direct_omits_banner(tmp_path, capsys, monkeypatch):
    img = tmp_path / "img.jpg"
    Image.new("RGB", (2, 2), color="blue").save(img)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NOLOSSIA_TAGLINE", "TESTTAG")
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "dedupe", str(tmp_path)])
    monkeypatch.setattr(cli, "_prompt", lambda *args, **kwargs: "n")

    cli.main()
    captured = capsys.readouterr()
    assert "TESTTAG" not in captured.out


def test_merge_direct_omits_banner(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "merge_src"
    src_dir.mkdir()
    Image.new("RGB", (2, 2), color="green").save(src_dir / "img.jpg")
    dest_dir = tmp_path / "merge_dest"
    dest_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NOLOSSIA_TAGLINE", "TESTTAG")
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "merge", str(src_dir), "--out", str(dest_dir)],
    )
    monkeypatch.setattr(cli, "_prompt", lambda *_args, default="": default)

    cli.main()
    captured = capsys.readouterr()
    assert "TESTTAG" not in captured.out


def test_cli_unexpected_error_mentions_canonical_log(tmp_path, monkeypatch, capsys):
    def boom(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", str(tmp_path)])
    monkeypatch.setattr(cli, "_scan_flow", boom)

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert "STOP/BLOCKED" in captured.out
    assert "Log file" in captured.out
    assert "Unexpected failure: boom" in captured.out


def test_cli_plain_flag_disables_banner_and_color(monkeypatch, tmp_path):
    recorded_detect: dict[str, object] = {}
    formatter_config = FormatterConfig(use_color=True, unicode_enabled=True, show_banner=True, plain_mode=False)

    def fake_detect(**kwargs):
        recorded_detect.update(kwargs)
        return formatter_config

    formatter_records: dict[str, FormatterConfig] = {}

    class StubFormatter:
        def __init__(self, config, stream=None):
            formatter_records["config"] = config
            self.stream = stream

    def fake_scan_flow(paths, formatter, **_kwargs):
        formatter_records["formatter"] = formatter

    monkeypatch.setattr(cli, "detect_terminal_capabilities", fake_detect)
    monkeypatch.setattr(cli, "CLIFormatter", StubFormatter)
    monkeypatch.setattr(cli, "_scan_flow", fake_scan_flow)
    monkeypatch.setattr(cli.reporting, "write_log", lambda entries: None)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "--plain", "scan", str(tmp_path)])

    cli.main()

    assert recorded_detect["plain_mode"] is True
    config = formatter_records["config"]
    assert config.plain_mode is True
    assert not config.use_color
    assert not config.unicode_enabled
    assert not config.show_banner


def test_scan_fast_flag_uses_fast_flow(monkeypatch, tmp_path):
    called = {"fast": False, "full": False}

    def fake_fast(paths, formatter):
        called["fast"] = True

    def fake_full(paths, formatter):
        called["full"] = True

    monkeypatch.setattr(cli, "_scan_fast_flow", fake_fast)
    monkeypatch.setattr(cli, "_scan_flow", fake_full)
    monkeypatch.setattr(cli.reporting, "write_log", lambda entries: None)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "scan", "--fast", str(tmp_path)])

    cli.main()

    assert called["fast"] is True
    assert called["full"] is False


def test_cli_ascii_flag_propagates_to_detector(monkeypatch, tmp_path):
    recorded_detect: dict[str, object] = {}

    def fake_detect(**kwargs):
        recorded_detect.update(kwargs)
        return FormatterConfig()

    class StubFormatter:
        def __init__(self, config, stream=None):
            self.config = config
            self.stream = stream

    monkeypatch.setattr(cli, "detect_terminal_capabilities", fake_detect)
    monkeypatch.setattr(cli, "CLIFormatter", StubFormatter)
    monkeypatch.setattr(cli, "_scan_flow", lambda paths, formatter, **_kwargs: None)
    monkeypatch.setattr(cli.reporting, "write_log", lambda entries: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--mode", "tty", "--ascii", "--color=always", "scan", str(tmp_path)],
    )

    cli.main()

    assert recorded_detect["force_ascii"] is True
    assert recorded_detect["color_preference"] == "always"


def test_destination_prompt_message_variants(tmp_path):
    assert (
        cli._destination_prompt_message("")
        == "> Destination path (Enter cancels). Example: /Library/2024/2024-05: "
    )
    sample = str(tmp_path / "out")
    assert (
        cli._destination_prompt_message(sample)
        == f"> Destination path [{sample}] (Enter keeps current). Example: /Library/2024/2024-05: "
    )


def test_merge_flow_destination_preflight_loop(monkeypatch, tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    photo = src_dir / "img.jpg"
    Image.new("RGB", (2, 2), color="white").save(photo)
    fileinfo = FileInfo(
        path=str(photo),
        size=photo.stat().st_size,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="abc",
        phash=None,
        is_raw=False,
    )

    bad_dir = tmp_path / "bad"
    bad_dir.mkdir()
    (bad_dir / "random.txt").write_text("note")
    good_dir = tmp_path / "good"
    good_dir.mkdir()

    plan = MergePlan(
        required_space=0,
        destination_free=0,
        duplicate_count=0,
        total_files=1,
        actions=[],
        destination_path=str(good_dir),
    )
    monkeypatch.setattr(cli.merge_engine, "build_merge_plan", lambda *args, **kwargs: plan)
    monkeypatch.setattr(cli.merge_engine, "dry_run", lambda _plan: {})
    summary_stub = {
        "required": "0 B",
        "free": "0 B",
        "free_after": "0 B",
        "masters_count": 0,
        "masters_size": "0 B",
        "exact_to_quar_count": 0,
        "exact_to_quar_size": "0 B",
        "near_marks": 0,
        "near_marks_size": "0 B",
        "source_paths": "",
        "out_path": "",
        "total_photos": 0,
        "year_month_breakdown": "none",
        "missing_exif_count": 0,
        "review_count": 0,
        "folder_merges_count": 0,
        "filename_collisions_count": 0,
        "corrupt_count": 0,
        "quarantine_path": "",
        "near_clusters": 0,
        "near_files_count": 0,
        "mergeplan_json_path": "",
        "dedupe_report_path": "",
        "merge_report_path": "",
        "review_samples": [],
        "storage_breakdown": {"masters": "0 B", "quarantine": "0 B", "review": "0 B"},
        "storage_breakdown_bytes": {"masters": 0, "quarantine": 0, "review": 0},
        "reports": [],
    }
    monkeypatch.setattr(cli, "_build_merge_summary", lambda *args, **kwargs: summary_stub)
    summary_calls: list[dict] = []
    monkeypatch.setattr(
        cli,
        "_print_merge_plan_summary",
        lambda summary, formatter, **_kwargs: summary_calls.append(summary),
    )

    responses = iter([str(bad_dir), "2", str(good_dir)])

    def fake_prompt(_formatter, _message, default=""):
        try:
            value = next(responses)
        except StopIteration:
            return default
        return default if value == "__DEFAULT__" else value

    monkeypatch.setattr(cli, "_prompt", fake_prompt)

    formatter_stream = io.StringIO()
    formatter = CLIFormatter(FormatterConfig(show_banner=False), stream=formatter_stream)

    cli._merge_flow(
        [str(src_dir)],
        [fileinfo],
        [],
        target_override=None,
        execute_requested=False,
        formatter=formatter,
    )

    output = formatter_stream.getvalue()
    assert "DESTINATION SAFETY CHECK" in output
    assert "[1] Reorganize" in output
    assert "[2] Choose" in output
    assert summary_calls  # merge summary invoked after successful validation


def test_merge_flow_verbose_destination_logging(monkeypatch, tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    photo = src_dir / "img.jpg"
    Image.new("RGB", (2, 2), color="white").save(photo)
    dest = tmp_path / "dest"
    dest.mkdir()

    fileinfo = FileInfo(
        path=str(photo),
        size=photo.stat().st_size,
        format="jpg",
        resolution=(1, 1),
        exif_datetime=datetime(2021, 1, 1),
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="abc",
        phash="abcd",
        is_raw=False,
    )

    responses = iter([str(dest)])
    monkeypatch.setattr(cli, "_prompt", lambda *_args, **_kwargs: next(responses, ""))

    plan = MergePlan(
        required_space=0,
        destination_free=0,
        duplicate_count=0,
        total_files=1,
        actions=[],
        destination_path=str(dest),
    )
    monkeypatch.setattr(cli.merge_engine, "build_merge_plan", lambda *args, **kwargs: plan)
    monkeypatch.setattr(cli.merge_engine, "dry_run", lambda _plan: {})

    summary_stub = {
        "required": "0 B",
        "free": "0 B",
        "free_after": "0 B",
        "masters_count": 0,
        "masters_size": "0 B",
        "exact_to_quar_count": 0,
        "exact_to_quar_size": "0 B",
        "near_marks": 0,
        "near_marks_size": "0 B",
        "source_paths": "",
        "out_path": str(dest),
        "total_photos": 0,
        "near_clusters": 0,
        "near_files_count": 0,
        "year_month_breakdown": "none",
        "missing_exif_count": 0,
        "review_count": 0,
        "folder_merges_count": 0,
        "filename_collisions_count": 0,
        "corrupt_count": 0,
        "quarantine_path": "",
        "mergeplan_json_path": "",
        "dedupe_report_path": "",
        "merge_report_path": "",
        "review_samples": [],
    }

    monkeypatch.setattr(cli, "_build_merge_summary", lambda *args, **kwargs: summary_stub)
    monkeypatch.setattr(
        cli,
        "_print_merge_plan_summary",
        lambda summary, formatter, **_kwargs: None,
    )

    def fake_validate_destination(path, reporter=None):
        if reporter:
            reporter("Inspecting existing folders for YEAR/YEAR-MONTH compliance")

    monkeypatch.setattr(cli.merge_engine, "validate_destination", fake_validate_destination)

    formatter = CLIFormatter(
        FormatterConfig(show_banner=False, verbose=True, use_color=False),
        stream=io.StringIO(),
    )

    cli._merge_flow(
        [str(src_dir)],
        [fileinfo],
        [],
        target_override=None,
        execute_requested=False,
        formatter=formatter,
    )

    output = formatter.stream.getvalue()
    assert "[destination]" in output
    assert "YEAR/YEAR-MONTH compliance" in output


def test_dry_run_summary_warns_when_storage_insufficient(monkeypatch, tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    photo = src_dir / "img.jpg"
    Image.new("RGB", (2, 2), color="white").save(photo)
    dest = tmp_path / "dest"
    dest.mkdir()

    fileinfo = FileInfo(
        path=str(photo),
        size=photo.stat().st_size,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="abc",
        phash=None,
        is_raw=False,
    )

    actions = [
        MoveMasterAction(src=str(photo), dst=str(dest / "2024" / "2024-01" / "img.jpg"), sha256="abc", size=4096),
        MoveToQuarantineExactAction(
            src=str(photo),
            dst=str(dest / "QUARANTINE" / "img-dupe.jpg"),
            sha256="def",
            size=2048,
        ),
        MarkNearDuplicateAction(
            src=str(photo),
            master=str(dest / "2024" / "2024-01" / "img.jpg"),
            sha256="ghi",
            size=1024,
        ),
    ]
    plan = MergePlan(
        required_space=2_000_000,
        destination_free=1_000_000,
        duplicate_count=0,
        total_files=1,
        actions=actions,
        destination_path=str(dest),
    )

    monkeypatch.setattr(cli.merge_engine, "build_merge_plan", lambda *args, **kwargs: plan)
    monkeypatch.setattr(
        cli.merge_engine,
        "dry_run",
        lambda _plan: {
            "MOVE_MASTER": 1,
            "MOVE_TO_QUARANTINE_EXACT": 1,
            "MARK_NEAR_DUPLICATE": 1,
        },
    )

    responses = iter([str(dest)])

    def fake_prompt(_formatter, _message, default=""):
        return next(responses, default)

    monkeypatch.setattr(cli, "_prompt", fake_prompt)

    formatter_stream = io.StringIO()
    formatter = CLIFormatter(FormatterConfig(show_banner=False), stream=formatter_stream)

    cli._merge_flow(
        [str(src_dir)],
        [fileinfo],
        [],
        target_override=None,
        execute_requested=False,
        formatter=formatter,
    )

    output = formatter_stream.getvalue()
    assert "Free after merge" in output
    assert "Masters storage" in output
    assert "Exact → Isolated for safety" in output
    assert "Look-alikes" in output
    assert "Required storage exceeds available free space" in output


def test_format_year_month_breakdown_sorted(tmp_path):
    actions = [
        MoveMasterAction(src=str(tmp_path / "a"), dst=str(tmp_path / "2023" / "2023-05" / "a.jpg"), sha256=None, size=1),
        MoveMasterAction(src=str(tmp_path / "b"), dst=str(tmp_path / "2024" / "2024-01" / "b.jpg"), sha256=None, size=1),
        MoveMasterAction(src=str(tmp_path / "c"), dst=str(tmp_path / "2023" / "2023-01" / "c.jpg"), sha256=None, size=1),
    ]
    text = cli._format_year_month_breakdown(actions, str(tmp_path))
    assert text.split(", ")[0].startswith("2023/2023-01")
    assert text.split(", ")[1].startswith("2023/2023-05")
    assert text.split(", ")[2].startswith("2024/2024-01")


def _normalize_output(text: str, tmp_path: Path) -> str:
    text = text.replace("\r\n", "\n")
    root = Path(tmp_path).resolve()
    for candidate in {str(root), root.as_posix()}:
        text = text.replace(candidate, "<TMP>")
    text = text.replace("\x1b]8;;", "<OSC8>")
    text = text.replace("\x1b\\", "<OSC8_END>")
    text = text.replace("\x1b[", "<ESC>[")
    text = text.replace("\x07", "<BEL>")
    return text.strip()


def _dedupe_snapshot(monkeypatch, tmp_path: Path, formatter_config: FormatterConfig) -> str:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("src.cli._prompt", lambda *args, **kwargs: "n")
    photo = tmp_path / "snap.jpg"
    photo.write_bytes(b"123")
    fi = FileInfo(
        path=str(photo.resolve()),
        size=123,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="snap",
        phash="snap",
        is_raw=False,
    )
    formatter = CLIFormatter(formatter_config, stream=io.StringIO())
    cli._dedupe_flow([], [fi], [], unique_size=fi.size, formatter=formatter)
    return _normalize_output(formatter.stream.getvalue(), tmp_path)


def test_dedupe_output_color_snapshot(monkeypatch, tmp_path):
    config = FormatterConfig(use_color=True, unicode_enabled=True, show_banner=False, osc8_links=True)
    output = _dedupe_snapshot(monkeypatch, tmp_path, config)
    assert "STEP 2/3" in output
    assert "SUMMARY" in output
    assert "Duplicate clustering finished" in output
    assert "dedupe_report.html" in output


def test_dedupe_output_ascii_snapshot(monkeypatch, tmp_path):
    config = FormatterConfig(use_color=True, unicode_enabled=False, show_banner=False, osc8_links=True)
    output = _dedupe_snapshot(monkeypatch, tmp_path, config)
    assert "STEP 2/3" in output
    assert "SUMMARY" in output
    assert "Duplicates total" in output
    assert "Run the merge wizard" in output


def test_dedupe_output_plain_snapshot(monkeypatch, tmp_path):
    config = FormatterConfig(
        use_color=False,
        unicode_enabled=False,
        show_banner=False,
        plain_mode=True,
        osc8_links=False,
    )
    output = _dedupe_snapshot(monkeypatch, tmp_path, config)
    assert "STEP 2/3" in output
    assert "SUMMARY" in output
    assert "\x1b[" not in output


def test_cli_verbose_flag_writes_near_duplicate_diagnostics(monkeypatch, tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    a = src_dir / "a.jpg"
    Image.new("RGB", (2, 2), color="red").save(a)
    b = src_dir / "b.jpg"
    Image.new("RGB", (2, 2), color="blue").save(b)

    dt = datetime(2023, 1, 1)
    file_a = FileInfo(
        path=str(a),
        size=a.stat().st_size,
        format="jpg",
        resolution=(4000, 2000),
        exif_datetime=dt,
        exif_gps=None,
        exif_camera="cam",
        exif_orientation=None,
        sha256="sha-a",
        phash="ffff000000000000",
        is_raw=False,
    )
    file_b = FileInfo(
        path=str(b),
        size=b.stat().st_size,
        format="jpg",
        resolution=(1000, 500),
        exif_datetime=dt,
        exif_gps=None,
        exif_camera="cam",
        exif_orientation=None,
        sha256="sha-b",
        phash="f000000000000000",
        is_raw=False,
    )

    monkeypatch.setattr(cli.scanner, "scan_paths", lambda paths: [file_a, file_b])
    monkeypatch.setattr(cli.metadata, "enrich_metadata", lambda files: files)
    monkeypatch.setattr(cli.hashing, "add_hashes", lambda files: files)

    inputs = iter(["y", "n"])
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs, ""))
    monkeypatch.chdir(tmp_path)

    logged: list[str] = []

    def fake_log(entries):
        logged.extend(entries)

    monkeypatch.setattr(cli.reporting, "write_log", fake_log)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "--verbose", "scan", str(src_dir)])

    cli.main()

    diag_entries = [entry for entry in logged if "[VERBOSE][NEAR_DUP]" in entry]
    assert diag_entries
    payload = json.loads(diag_entries[0].split(" ", 1)[1])
    assert payload["decision"] == "REJECT"
    assert "resolution_ratio" in payload or payload["reason"] in ("distance_over_threshold", "phash_missing")


def _write_undo_manifest(tmp_path, *, batch_id, original_path, new_path, sha256):
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir(exist_ok=True)
    payload = {
        "schema_version": "1.0",
        "batch_id": batch_id,
        "entries": [
            {
                "original_path": str(original_path),
                "original_folder": str(Path(original_path).parent),
                "new_path": str(new_path),
                "hash": sha256,
                "batch_id": batch_id,
            }
        ],
    }
    (artifacts / "source_manifest.json").write_text(json.dumps(payload))


def test_undo_preview_no_move(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    lib_dir = tmp_path / "library" / "2024" / "2024-01"
    src_dir.mkdir()
    lib_dir.mkdir(parents=True)
    original = src_dir / "photo.jpg"
    original.write_bytes(b"preview")
    sha = hashing.compute_sha256(str(original))
    new_path = lib_dir / "photo.jpg"
    shutil.move(str(original), str(new_path))
    batch_id = "preview123"
    _write_undo_manifest(tmp_path, batch_id=batch_id, original_path=original, new_path=new_path, sha256=sha)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "undo", "--last", "--preview"])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 0
    captured = capsys.readouterr()
    assert "Undo preview complete." in captured.out
    assert new_path.exists()
    assert not original.exists()


def test_undo_requires_confirmation(tmp_path, capsys, monkeypatch):
    src_dir = tmp_path / "src"
    lib_dir = tmp_path / "library" / "2024" / "2024-02"
    src_dir.mkdir()
    lib_dir.mkdir(parents=True)
    original = src_dir / "photo.jpg"
    original.write_bytes(b"confirm")
    sha = hashing.compute_sha256(str(original))
    new_path = lib_dir / "photo.jpg"
    shutil.move(str(original), str(new_path))
    batch_id = "confirm123"
    _write_undo_manifest(tmp_path, batch_id=batch_id, original_path=original, new_path=new_path, sha256=sha)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "tty", "undo", "--last"])
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: "")

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 0
    captured = capsys.readouterr()
    assert "User cancelled undo." in captured.out
    assert new_path.exists()
    assert not original.exists()


def test_undo_pipe_mode_blocked(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "--mode", "pipe", "undo", "--last"])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip())
    assert payload["status"] == "BLOCKED"
    assert payload["phase"].lower() == "undo"
