"""
Module: cli
Purpose: Command-line interface entry point.
"""

import argparse
import io
import json
import os
import shlex
import sys
from collections import Counter
from typing import Callable, List, Sequence, Tuple

from . import duplicates, hashing, metadata, merge_engine, organizer, reporting, scanner
from .review import describe_review_reason
from .cli_formatter import CLIFormatter, detect_terminal_capabilities
from .exceptions import (
    NolossiaError,
    MergeExecutionError,
    MergePlanError,
    StorageError,
    UndoInputError,
    UndoSafetyError,
)
from .models.cluster import DuplicateCluster
from .models.fileinfo import FileInfo
from .models.mergeplan import MergePlan
from .models.actions import (
    MergeAction,
    MoveMasterAction,
    MarkNearDuplicateAction,
    MoveToQuarantineExactAction,
)
from .utils import (
    DEFAULT_PIXEL_LIMIT,
    MAX_OVERRIDE_LIMIT,
    PIXEL_LIMIT_ENV,
    configure_pixel_limit,
    configure_executor_mode,
    EXECUTOR_ENV,
    current_pixel_limit,
    human_readable_size,
)

MAX_PHASE_LINES = 35

_RUN_LOG_PATH: str | None = None
_DEFAULT_ARTIFACTS = [
    reporting.artifact_path("merge_plan.json"),
    reporting.artifact_path("dedupe_report.html"),
    reporting.artifact_path("merge_report.html"),
]
_UNDO_ARTIFACTS = [
    reporting.artifact_path("undo_report.html"),
    reporting.artifact_path("undo_manifest.json"),
]


def _ensure_run_log_path() -> str:
    """
    Guarantee nolossia.log exists and return its absolute path.
    """
    global _RUN_LOG_PATH
    if _RUN_LOG_PATH:
        return _RUN_LOG_PATH
    _RUN_LOG_PATH = reporting.ensure_log_initialized()
    return _RUN_LOG_PATH


def _current_log_path() -> str:
    """
    Return the best-known log path (absolute) without reinitializing the log.
    """
    if _RUN_LOG_PATH:
        return _RUN_LOG_PATH
    return os.path.abspath(reporting.LOG_FILE_NAME)


def _render_step_header(formatter: CLIFormatter, step: int, total: int, title: str, subtitle: str | None = None) -> None:
    """
    Print a standardized step header per DESIGN-TUI contract.
    """
    if formatter.config.pipe_mode:
        return
    header = f"STEP {step}/{total} — {title}"
    if formatter.config.plain_mode or not formatter.config.unicode_enabled:
        header = header.upper()
    formatter.blank()
    formatter.line(formatter.label(header, level="info"))
    if subtitle:
        formatter.muted(subtitle)


def _render_mode_label(formatter: CLIFormatter, mode_label: str) -> None:
    """
    Render a plain-language mode label in TTY output.
    """
    if formatter.config.pipe_mode:
        return
    formatter.blank()
    formatter.line(formatter.label(f"MODE: {mode_label}", level="info"))
    if mode_label.upper() == "PREVIEW":
        formatter.muted("Preview only — no changes yet.")


def _render_status_block(formatter: CLIFormatter, entries: list[tuple[str, str, str]]) -> None:
    """
    Render compact action status list; entries = (status, label, detail).
    status: "ok" | "warn" | "error"
    """
    if formatter.config.pipe_mode:
        return
    icon_map = {
        "ok": ("✓", "success"),
        "warn": ("!", "warn"),
        "error": ("✖", "error"),
    }
    for status, label, detail in entries:
        icon, level = icon_map.get(status, ("•", "info"))
        glyph = icon if formatter.config.unicode_enabled else level.upper()
        formatter.line(f"{glyph} {label}")
        if detail:
            formatter.muted(f"   {detail}")


def _render_summary_box(formatter: CLIFormatter, rows: list[tuple[str, str]], *, title: str = "SUMMARY") -> None:
    """
    Render SUMMARY box with deterministic width.
    """
    if formatter.config.pipe_mode:
        return
    width = min(formatter.line_width, 96)
    unicode = formatter.config.unicode_enabled and not formatter.config.plain_mode
    tl, tr, bl, br, horiz, vert = ("┌", "┐", "└", "┘", "─", "│") if unicode else ("+", "+", "+", "+", "-", "|")
    box_title = f" {title} "
    top = f"{tl}{horiz * (width - 2)}{tr}"
    title_line = f"{vert}{box_title.center(width - 2)}{vert}"
    formatter.line(top)
    formatter.line(title_line)
    formatter.line(f"{vert}{' ' * (width - 2)}{vert}")
    for label, value in rows:
        text = f"{label:<24} {value}"
        if len(text) > width - 4:
            text = text[: width - 7] + "..."
        formatter.line(f"{vert} {text.ljust(width - 3)}{vert}")
    formatter.line(f"{vert}{' ' * (width - 2)}{vert}")
    formatter.line(f"{bl}{horiz * (width - 2)}{br}")


def _render_undo_summary(formatter: CLIFormatter, summary: dict) -> None:
    if formatter.config.pipe_mode:
        return
    counts = summary.get("counts", {})
    rows = [
        ("Operation ID", str(summary.get("operation_id") or "-")),
        ("Mode", str(summary.get("mode") or "-").upper()),
        ("Restores", str(counts.get("restore", 0))),
        ("Conflicts", str(counts.get("conflict", 0))),
        ("Skipped", str(counts.get("skipped", 0))),
    ]
    _render_summary_box(formatter, rows, title="UNDO SUMMARY")
    undo_report = reporting.artifact_path("undo_report.html")
    undo_manifest = reporting.artifact_path("undo_manifest.json")
    formatter.line(_link_with_fallback(formatter, undo_report, "undo_report.html"))
    formatter.line(_link_with_fallback(formatter, undo_manifest, "undo_manifest.json"))
    if counts.get("conflict", 0):
        conflict_root = summary.get("conflict_root") or ""
        if conflict_root:
            formatter.warning(f"Conflicts routed to {conflict_root}")
        formatter.muted("Manual restore needed for conflicted items.")


def _render_next_actions(formatter: CLIFormatter, actions: list[str]) -> None:
    _render_next_steps(formatter, actions)


def _render_reports_block(
    formatter: CLIFormatter,
    reports: list[tuple[str, str]],
    *,
    max_items: int = 3,
) -> None:
    if formatter.config.pipe_mode or not reports:
        return
    bullet = "•" if formatter.config.unicode_enabled and not formatter.config.plain_mode else "*"
    formatter.line("Reports:")
    visible = reports[:max_items]
    for path, label in visible:
        display = formatter.link(path, label)
        open_cmd = _report_open_command(path)
        formatter.line(f"  {bullet} FILE: {display}")
        formatter.line(f"    PATH: {path}")
        formatter.line(f"    OPEN: {open_cmd}")
    remaining = len(reports) - len(visible)
    if remaining > 0:
        formatter.line(f"  {bullet} +{remaining} more (full list: merge_report.html)")


def _render_warnings_frame(formatter: CLIFormatter, warnings: list[str]) -> None:
    if formatter.config.pipe_mode or not warnings:
        return
    formatter.frame("WARNINGS", warnings)


def _render_next_steps(formatter: CLIFormatter, steps: list[str]) -> None:
    if formatter.config.pipe_mode or not steps:
        return
    formatter.line("Next steps:")
    for step in steps:
        formatter.line(f"  • {step}")


def _undo_flow(operation_id: str, preview: bool, formatter: CLIFormatter) -> int:
    manifest_path = reporting.artifact_path("source_manifest.json")
    try:
        plan = merge_engine.prepare_undo_plan(manifest_path, operation_id)
    except UndoInputError as exc:
        _render_failure_summary(
            formatter,
            status="FAILED",
            phase="UNDO",
            reason=str(exc),
            last_step="Load source manifest",
            remediation=[
                "Run a merge with --execute to generate a source manifest.",
                "Verify the operation_id matches the manifest batch_id.",
            ],
            artifacts=_UNDO_ARTIFACTS,
        )
        return 1

    undo_report = reporting.artifact_path("undo_report.html")
    undo_manifest = reporting.artifact_path("undo_manifest.json")

    if preview:
        summary = merge_engine.preview_undo(plan)
        reporting.write_undo_manifest(summary, undo_manifest)
        reporting.write_undo_report(summary, undo_report)
        _render_mode_label(formatter, "PREVIEW")
        _render_undo_summary(formatter, summary)
        formatter.success("Undo preview complete.")
        return 0

    confirm = _prompt(formatter, "Type 'UNDO' to move files back now (Enter cancels): ")
    if confirm != "UNDO":
        _render_failure_summary(
            formatter,
            status="ABORTED",
            phase="UNDO",
            reason="User cancelled undo.",
            last_step="Undo confirmation",
            remediation=["Re-run with UNDO to proceed."],
            artifacts=_UNDO_ARTIFACTS,
        )
        return 0

    try:
        summary = merge_engine.execute_undo(plan)
    except UndoSafetyError as exc:
        blocked_summary = merge_engine.preview_undo(plan)
        blocked_summary["mode"] = "execute"
        reporting.write_undo_manifest(blocked_summary, undo_manifest)
        reporting.write_undo_report(blocked_summary, undo_report)
        _render_failure_summary(
            formatter,
            status="BLOCKED",
            phase="UNDO",
            reason=str(exc),
            last_step="Undo safety checks",
            remediation=[
                "Review undo_report.html for the impacted files.",
                "Resolve conflicts or integrity issues, then retry undo.",
            ],
            artifacts=_UNDO_ARTIFACTS,
        )
        return 2
    except Exception as exc:
        _render_failure_summary(
            formatter,
            status="FAILED",
            phase="UNDO",
            reason=f"Undo failed: {exc}",
            last_step="Undo execution",
            remediation=[
                "Review nolossia.log for details.",
                "Resolve the issue and retry undo.",
            ],
            artifacts=_UNDO_ARTIFACTS,
        )
        return 1

    reporting.write_undo_manifest(summary, undo_manifest)
    reporting.write_undo_report(summary, undo_report)
    _render_mode_label(formatter, "EXECUTE")
    _render_undo_summary(formatter, summary)
    formatter.success("Undo execution complete.")
    return 0


def _render_settings_tiers(
    formatter: CLIFormatter,
    *,
    execute_requested: bool,
    source_paths: list[str],
    destination_path: str | None,
) -> None:
    if formatter.config.pipe_mode:
        return
    def normalize_sensitivity(value: str) -> str:
        normalized = value.strip().lower()
        if normalized in {"c", "conservative"}:
            return "conservative"
        if normalized in {"b", "balanced"}:
            return "balanced"
        if normalized in {"a", "aggressive"}:
            return "aggressive"
        return "conservative"

    formatter.section("Settings", icon="◇")
    formatter.line("Basic settings:")
    mode_label = "Execute (requires confirmation)" if execute_requested else "Preview-only (default)"
    formatter.kv("Mode", mode_label)
    formatter.kv("Sources", f"{len(source_paths)} path(s)")
    formatter.kv("Destination", destination_path or "Prompted during setup")
    formatter.kv("Output", "Reports in artifacts/ (dedupe_report.html, merge_report.html, merge_plan.json)")
    formatter.kv("Speed", "Normal (hash + dedupe)")
    formatter.kv("Theme", formatter.config.theme)
    formatter.kv("Plain output", "on" if formatter.config.plain_mode else "off")
    formatter.kv("ASCII output", "on" if not formatter.config.unicode_enabled else "off")

    advanced = _prompt(formatter, "Advanced settings? [y/N] ", default="").lower()
    if advanced != "y":
        return
    formatter.warning("Advanced settings (power users; can affect accuracy or performance)")
    sensitivity_prompt = (
        "Look-alike sensitivity [C]onservative/[B]alanced/[A]ggressive "
        "(default Conservative): "
    )
    sensitivity_choice = _prompt(formatter, sensitivity_prompt, default="").strip()
    if sensitivity_choice:
        formatter.config.look_alike_sensitivity = normalize_sensitivity(sensitivity_choice)
    if formatter.config.look_alike_sensitivity == "aggressive":
        confirm = _prompt(
            formatter,
            "Type 'AGGRESSIVE' to confirm higher-risk matching (Enter cancels): ",
            default="",
        ).strip()
        if confirm != "AGGRESSIVE":
            formatter.warning("Aggressive sensitivity not confirmed; using Conservative.")
            formatter.config.look_alike_sensitivity = "conservative"
    sensitivity_label = formatter.config.look_alike_sensitivity.title()
    if formatter.config.look_alike_sensitivity == "conservative":
        sensitivity_label = f"{sensitivity_label} (default)"
    formatter.kv("Look-alike sensitivity", sensitivity_label)
    pixel_source = formatter.config.pixel_limit_source
    if pixel_source != "default" and formatter.config.pixel_limit:
        source_label = "CLI flag" if pixel_source == "cli" else f"${PIXEL_LIMIT_ENV}"
        pixel_label = f"{formatter.config.pixel_limit:,} via {source_label}"
    else:
        pixel_label = f"off (default {DEFAULT_PIXEL_LIMIT:,})"
    formatter.kv("Pixel limit override", pixel_label)
    formatter.kv("Pipe output format", formatter.config.pipe_format)
    formatter.kv("Report verbosity", "full (default)")
    formatter.kv("Diagnostics logging", "on" if formatter.config.verbose else "off")
    reporting.write_log([f"[INFO] Look-alike sensitivity set to {formatter.config.look_alike_sensitivity}"])


def _render_sensitivity_banner(formatter: CLIFormatter) -> None:
    if formatter.config.pipe_mode:
        return
    sensitivity = formatter.config.look_alike_sensitivity
    if sensitivity == "balanced":
        formatter.line("Look-alike sensitivity: Balanced")
    elif sensitivity == "aggressive":
        formatter.line("Look-alike sensitivity: Aggressive (may increase review load)")


def _render_start_intro(formatter: CLIFormatter) -> None:
    """
    Render a short intro for the start command.
    """
    if formatter.config.pipe_mode:
        return
    formatter.section("Start", icon="◇")
    formatter.line("Interactive launcher for Nolossia commands.")
    formatter.line("Nothing changes until you confirm an execute step.")
    formatter.line("Choose a command to continue.")


def _render_glossary(formatter: CLIFormatter) -> None:
    """
    Render a short glossary for core terms in wizard output.
    """
    if formatter.config.pipe_mode or not formatter.config.show_glossary:
        return
    formatter.section("Glossary", icon="◇")
    formatter.bullet("Dedupe: read-only duplicate detection and clustering.", indent="  - ")
    formatter.bullet(
        "Exact duplicate: identical content; all copies are the same image.",
        indent="  - ",
    )
    formatter.bullet("Look-alike: similar photos set aside for review only.", indent="  - ")
    formatter.bullet(
        "Visual match check: compares photos visually to find look-alikes (pHash).",
        indent="  - ",
    )
    formatter.bullet(
        "Master selection: RAW > higher resolution > larger file > more EXIF > GPS > oldest when equal.",
        indent="  - ",
    )
    formatter.bullet("Isolated for safety: exact duplicates moved; never deleted automatically.", indent="  - ")
    formatter.bullet("Set aside for review: files missing reliable dates or needing manual review.", indent="  - ")


def _start_flow(formatter: CLIFormatter, *, show_glossary: bool = True) -> int | None:
    if formatter.config.pipe_mode:
        _render_failure_summary(
            formatter,
            status="BLOCKED",
            phase="START",
            reason="Start is interactive only and not available in pipe mode.",
            last_step="Non-interactive output mode",
            remediation=[
                "Re-run start in interactive mode (tty/plain/ascii).",
                "Run scan/dedupe/merge directly for non-interactive usage.",
            ],
            artifacts=[],
        )
        return 2

    formatter.print_banner()
    _render_start_intro(formatter)

    previous_glossary = formatter.config.show_glossary
    formatter.config.show_glossary = show_glossary
    _render_glossary(formatter)
    formatter.config.show_glossary = previous_glossary

    while True:
        choice = _prompt(
            formatter,
            "→ Command [scan/dedupe/merge/help/exit]: ",
            default="help",
        ).lower()
        if choice in {"exit", "quit", "q"}:
            formatter.line("Exiting. No files have been changed.")
            return None
        if choice in {"help", "h", "?"}:
            formatter.section("Commands", icon="◇")
            formatter.bullet("scan   — read-only inventory and duplicate analysis.", indent="  - ")
            formatter.bullet("dedupe — duplicate + look-alike clustering (read-only).", indent="  - ")
            formatter.bullet("merge  — preview or execute a merge (requires confirmation).", indent="  - ")
            formatter.bullet("exit   — quit without doing anything.", indent="  - ")
            continue
        if choice not in {"scan", "dedupe", "merge"}:
            formatter.warning("Unknown command. Type 'help' to see available options.")
            continue

        paths_input = _prompt(
            formatter,
            "→ Source paths (space-separated): ",
            default="",
        )
        paths = shlex.split(paths_input)
        if not paths:
            formatter.warning("No source paths provided.")
            continue

        if choice == "scan":
            _scan_flow(paths, formatter, show_banner=False, show_glossary=False)
            return None

        if choice == "dedupe":
            hashed, clusters, skipped, skipped_symlinks = _scan_and_group(paths, formatter=formatter)
            unique_size = _estimate_unique_size(hashed, clusters)
            proceed, stats = _dedupe_flow(
                paths,
                hashed,
                clusters,
                unique_size,
                formatter,
                skipped_files=skipped,
            )
            if proceed:
                scan_summary = {
                    "supported": len(hashed),
                    "size": sum((f.size or 0) for f in hashed),
                    "skipped": skipped,
                    "skipped_symlinks": skipped_symlinks,
                }
                _merge_flow(
                    paths,
                    hashed,
                    clusters,
                    skipped_files=skipped,
                    formatter=formatter,
                    show_banner=False,
                    show_intro=False,
                    scan_totals=scan_summary,
                    dedupe_stats=stats,
                )
            return None

        if choice == "merge":
            if not formatter.config.pipe_mode:
                _render_settings_tiers(
                    formatter,
                    execute_requested=False,
                    source_paths=paths,
                    destination_path=None,
                )
                _render_sensitivity_banner(formatter)
            hashed, clusters, skipped, skipped_symlinks = _scan_and_group(paths, formatter=formatter)
            scan_summary = {
                "supported": len(hashed),
                "size": sum((f.size or 0) for f in hashed),
                "skipped": skipped,
                "skipped_symlinks": skipped_symlinks,
            }
            unique_size = _estimate_unique_size(hashed, clusters)
            stats = _calculate_dedupe_stats(hashed, clusters, unique_size)
            if formatter.config.pipe_mode and formatter.config.stream_json:
                _emit_pipe_summary(
                    formatter,
                    status="scan",
                    phase="scan",
                    masters=scan_summary["supported"],
                    duplicates=0,
                    near=0,
                    required="0B",
                    available="0B",
                    review=0,
                    skipped=scan_summary["skipped"],
                    reports=[],
                    review_samples=[],
                    storage_breakdown={},
                )
                _emit_pipe_summary(
                    formatter,
                    status="dedupe",
                    phase="dedupe",
                    masters=stats["masters"],
                    duplicates=stats["exact_redundant"],
                    near=stats["near_redundant"],
                    required="0B",
                    available="0B",
                    review=0,
                    skipped=skipped,
                    reports=[],
                    review_samples=[],
                    storage_breakdown={},
                )
            _merge_flow(
                paths,
                hashed,
                clusters,
                skipped_files=skipped,
                execute_requested=False,
                formatter=formatter,
                show_banner=False,
                scan_totals=scan_summary,
                dedupe_stats=stats,
            )
            return None

    return None


def _emit_pipe_summary(
    formatter: CLIFormatter,
    *,
    status: str,
    phase: str,
    masters: int,
    duplicates: int,
    near: int,
    required: str,
    available: str,
    review: int,
    skipped: int,
    reports: list[str],
    review_samples: list[str] | None = None,
    storage_breakdown: dict[str, str] | None = None,
) -> None:
    if not formatter.config.pipe_mode:
        return
    pipe_format = getattr(formatter.config, "pipe_format", "json")
    storage_breakdown = storage_breakdown or {}
    report_counts: dict[str, int] = {}
    for report in reports:
        name = os.path.basename(report) if report else ""
        if not name:
            continue
        report_counts[name] = report_counts.get(name, 0) + 1
    if pipe_format == "kv":
        reports_value = ",".join(reports) if reports else "-"
        samples_value = ",".join(review_samples or []) or "-"
        breakdown_value = ",".join(
            f"{key}:{storage_breakdown.get(key, '-')}"
            for key in ("masters", "quarantine", "review")
        )
        reports_summary = ",".join(f"{key}:{value}" for key, value in report_counts.items()) or "-"
        line = (
            f"status={status.upper()} phase={phase} totals=masters:{masters},dups:{duplicates},near:{near} "
            f"storage=required:{required},available:{available} storage_breakdown={breakdown_value} "
            f"review={review} review_samples={samples_value} skipped={skipped} reports={reports_value} "
            f"reports_count={len(report_counts)} reports_summary={reports_summary}"
        )
    else:
        payload = {
            "schema_version": "1.0",
            "status": status.upper(),
            "phase": phase,
            "masters": masters,
            "duplicates": duplicates,
            "near": near,
            "storage": {
                "required": required,
                "available": available,
                "breakdown": storage_breakdown,
            },
            "review": {"count": review, "samples": review_samples or []},
            "skipped": skipped,
            "reports": reports,
            "reports_count": len(report_counts),
            "reports_summary": report_counts,
        }
        line = json.dumps(payload, separators=(",", ":"))
    target = getattr(formatter, "pipe_target", sys.stdout)
    target.write(line + "\n")


def _render_chronology_table(formatter: CLIFormatter, rows: list[tuple[str, int, str]]) -> None:
    if formatter.config.pipe_mode or not rows:
        return
    formatter.blank()
    formatter.line(formatter.label("Top chronology (Year-Month)", level="info"))
    header = "Year Month Count Percent"
    formatter.line(header)
    formatter.line("-" * len(header))
    for label, count, percent in rows:
        year, month = label.split("-", 1) if "-" in label else (label, "")
        formatter.line(f"{year:<5} {month:<5} {count:<6} {percent}")


def _is_exact_cluster(cluster) -> bool:
    """
    Determine if all files in cluster share the same non-empty SHA256.
    """
    sha_values = {f.sha256 for f in cluster.files if f.sha256 is not None}
    return len(sha_values) == 1 and all(f.sha256 is not None for f in cluster.files)


def _scan_and_group(
    paths: List[str],
    formatter: CLIFormatter | None = None,
) -> tuple[List[FileInfo], List[DuplicateCluster], int, int]:
    """
    Helper to scan paths, enrich metadata, hash, and group duplicates.
    """
    fileinfos, skipped_symlinks = scanner.scan_paths_with_stats(paths)
    enriched = metadata.enrich_metadata(fileinfos)
    metadata_skipped = len(fileinfos) - len(enriched)
    hashed = hashing.add_hashes(enriched)
    hashing_skipped = len(enriched) - len(hashed)
    skipped_total = metadata_skipped + hashing_skipped
    reporter: Callable[[str], None] | None = None
    diagnostics_logger: Callable[[dict], None] | None = None
    if formatter and formatter.config.verbose:
        def reporter(message: str) -> None:
            formatter.verbose(f"[duplicates] {message}")
        def diagnostics_logger(payload: dict) -> None:
            reporting.write_log(
                [f"[VERBOSE][NEAR_DUP] {json.dumps(payload, sort_keys=True)}"]
            )
    sensitivity = formatter.config.look_alike_sensitivity if formatter else "conservative"
    clusters = duplicates.group_duplicates(
        hashed,
        reporter=reporter,
        diagnostics_logger=diagnostics_logger,
        sensitivity=sensitivity,
    )
    if formatter and skipped_total:
        formatter.warning(
            "Skipped "
            f"{skipped_total} files after hitting the {current_pixel_limit():,} pixel limit "
            f"or encountering unreadable data. See {reporting.LOG_FILE_NAME} for per-file details."
        )
    return hashed, clusters, skipped_total, skipped_symlinks


def _pixel_limit_arg(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Pixel limit must be an integer.") from exc
    if parsed < DEFAULT_PIXEL_LIMIT or parsed > MAX_OVERRIDE_LIMIT:
        raise argparse.ArgumentTypeError(
            f"Pixel limit must be between {DEFAULT_PIXEL_LIMIT} and {MAX_OVERRIDE_LIMIT}."
        )
    return parsed


def _collect_files(paths: List[str]) -> tuple[List[str], int]:
    """
    Collect all files under provided paths for summary purposes.
    """
    all_files: List[str] = []
    skipped_symlinks = 0
    for path in paths:
        normalized = os.path.abspath(path)
        if not os.path.exists(normalized):
            raise NolossiaError(f"One or more input paths are invalid.\n  Offending path: {path}")
        if not os.path.isdir(normalized):
            raise NolossiaError(f"One or more input paths are invalid.\n  Offending path: {path}")
        for root, dirs, files in os.walk(normalized, topdown=True, followlinks=False):
            safe_dirs: List[str] = []
            for dirname in dirs:
                dir_path = os.path.join(root, dirname)
                if os.path.islink(dir_path):
                    skipped_symlinks += 1
                    continue
                safe_dirs.append(dirname)
            dirs[:] = safe_dirs
            for name in files:
                file_path = os.path.abspath(os.path.join(root, name))
                if os.path.islink(file_path):
                    skipped_symlinks += 1
                    continue
                all_files.append(file_path)
    return all_files, skipped_symlinks


def _partition_clusters(clusters: List[DuplicateCluster]) -> Tuple[List[DuplicateCluster], List[DuplicateCluster]]:
    """
    Split clusters into exact and near-duplicate groups.
    """
    exact = []
    near = []
    for cluster in clusters:
        if _is_exact_cluster(cluster):
            exact.append(cluster)
        else:
            near.append(cluster)
    return exact, near


def _prompt(formatter: CLIFormatter, message: str, default: str = "") -> str:
    """
    Prompt the user and return input or default on empty/EOF.
    """
    if formatter.config.pipe_mode:
        return default
    try:
        value = input(formatter.prompt(message))
    except (EOFError, OSError):
        return default
    value = value.strip()
    return value if value else default


def _link_with_fallback(formatter: CLIFormatter, path: str, label: str) -> str:
    """
    Render hyperlink with textual fallback when OSC8 is unavailable.
    """
    link_text = formatter.link(path, label)
    if formatter.config.osc8_links:
        return link_text
    return f"{link_text} (open {path})"


def _report_open_command(path: str) -> str:
    if os.name == "nt" or sys.platform.startswith("win"):
        return f'start "" "{path}"'
    if sys.platform.startswith("darwin"):
        return f'open "{path}"'
    return f'xdg-open "{path}"'


def _emit_pipe_failure(
    formatter: CLIFormatter,
    *,
    status: str,
    phase: str,
    reason: str,
    files_changed: str,
    remediation: list[str],
    log_path: str,
    reports: list[str] | None = None,
    last_step: str | None = None,
) -> None:
    if not formatter.config.pipe_mode:
        return
    payload = {
        "schema_version": "1.0",
        "status": status.upper(),
        "phase": phase,
        "reason": reason,
        "files_changed": files_changed,
        "remediation": remediation,
        "log": log_path,
        "reports": reports or [],
    }
    if last_step:
        payload["last_step"] = last_step
    pipe_format = getattr(formatter.config, "pipe_format", "json")
    target = getattr(formatter, "pipe_target", sys.stdout)
    if pipe_format == "kv":
        reports_value = ",".join(payload["reports"]) if payload["reports"] else "-"
        remediation_value = ";".join(remediation) if remediation else "-"
        last_step_value = payload.get("last_step") or "-"
        line = (
            f"status={payload['status']} phase={phase} reason={reason} last_step={last_step_value} "
            f"files_changed={files_changed} remediation={remediation_value} log={log_path} reports={reports_value}"
        )
        target.write(line + "\n")
        return
    target.write(json.dumps(payload, separators=(",", ":")) + "\n")


def _render_failure_summary(
    formatter: CLIFormatter,
    *,
    status: str,
    phase: str,
    reason: str,
    remediation: list[str],
    last_step: str | None = None,
    files_changed: str = "None",
    details: list[tuple[str, str]] | None = None,
    artifacts: list[str] | None = None,
) -> None:
    log_path = _current_log_path()
    artifact_paths = artifacts or []
    absolute_artifacts: list[str] = []
    seen: set[str] = set()
    for path in artifact_paths:
        normalized = os.path.abspath(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        absolute_artifacts.append(normalized)
    _emit_pipe_failure(
        formatter,
        status=status,
        phase=phase,
        reason=reason,
        files_changed=files_changed,
        remediation=remediation,
        log_path=log_path,
        reports=absolute_artifacts,
        last_step=last_step,
    )
    if formatter.config.pipe_mode:
        return
    log_hint = _link_with_fallback(formatter, log_path, "nolossia.log")
    formatted_artifacts = [
        _link_with_fallback(formatter, path, os.path.basename(path)) for path in absolute_artifacts
    ]
    normalized_status = status.upper()
    header_label = "FAILURE" if normalized_status == "FAILED" else normalized_status
    header = f"{header_label} SUMMARY — {phase}"
    formatter.failure_summary(
        header=header,
        reason=reason,
        last_step=last_step,
        files_changed=files_changed,
        log_hint=log_hint,
        artifacts=formatted_artifacts,
        remediation=remediation,
        details=details,
    )


def _destination_prompt_message(current_default: str) -> str:
    """
    Build destination prompt hint with explicit default instructions.
    """
    if current_default:
        return (
            f"> Destination path [{current_default}] (Enter keeps current). "
            "Example: /Library/2024/2024-05: "
        )
    return "> Destination path (Enter cancels). Example: /Library/2024/2024-05: "


def _print_destination_preflight_intro(formatter: CLIFormatter) -> None:
    """
    Print preflight summary describing destination requirements.
    """
    formatter.muted(
        "DESTINATION SAFETY CHECK → Target must be empty or already follow YEAR / YEAR-MONTH (e.g., /Library/2024/2024-05)."
    )
    formatter.muted(
        "If the folder fails validation choose [1] to reorganize it, [2] to select a different empty folder, "
        "or press Enter at the destination prompt to cancel."
    )


def _scan_flow(
    paths: List[str],
    formatter: CLIFormatter,
    *,
    quick_mode: bool = False,
    show_banner: bool = True,
    show_glossary: bool = True,
):
    """
    Implements Phase 6 scan → dedupe → merge wizard.
    """

    previous_glossary = formatter.config.show_glossary
    formatter.config.show_glossary = show_glossary
    if show_banner:
        formatter.print_banner()
    total_steps = 2 if quick_mode else 3
    step_title = "Quick Scan & Plan" if quick_mode else "Scan & Inventory"
    step_subtitle = "scan + duplicate analysis (read-only)" if quick_mode else "read-only pass over selected folders"
    _render_step_header(formatter, 1, total_steps, step_title, step_subtitle)

    all_files, _ = _collect_files(paths)
    supported_files = scanner.filter_supported_files(all_files)
    total_size_supported = sum(os.path.getsize(p) for p in supported_files)

    hashed, clusters, skipped_total, skipped_symlinks = _scan_and_group(paths, formatter=formatter)
    total_photos = len(hashed)
    if total_photos == 0:
        formatter.warning("No supported image files were found in the provided locations.")
        formatter.line("Nothing to do. No files have been changed.")
        formatter.config.show_glossary = previous_glossary
        return

    scan_summary = {
        "supported": total_photos,
        "size": total_size_supported,
        "skipped": skipped_total,
        "skipped_symlinks": skipped_symlinks,
    }
    if quick_mode:
        summary_rows = [
            ("Photos", f"{total_photos:,}"),
            ("Total size", human_readable_size(total_size_supported)),
            ("Skipped files", str(skipped_total + skipped_symlinks)),
        ]
    else:
        summary_rows = [
            ("Supported photos", f"{total_photos:,}"),
            ("Total size", human_readable_size(total_size_supported)),
            ("Skipped (pixel cap)", str(skipped_total)),
            ("Skipped (symlinks)", str(skipped_symlinks)),
        ]
    _render_summary_box(formatter, summary_rows)
    if quick_mode:
        _render_status_block(
            formatter,
            [
                ("ok", "Scan complete", f"{total_photos:,} photos"),
                (
                    "warn",
                    "Skipped files",
                    str(skipped_total + skipped_symlinks) if (skipped_total + skipped_symlinks) else "None",
                ),
            ],
        )
        _render_next_actions(
            formatter,
            [
                "Proceed to quick duplicate check.",
            ],
        )
    else:
        _render_status_block(
            formatter,
            [
                ("ok", "Scan complete", f"{total_photos:,} supported photos"),
                (
                    "warn",
                    "Skipped files",
                    f"{skipped_total} skipped due to pixel limits" if skipped_total else "No skips detected",
                ),
                (
                    "warn",
                    "Skipped symlinks",
                    f"{skipped_symlinks} symlinked items skipped" if skipped_symlinks else "No symlink skips detected",
                ),
            ],
        )
        _render_glossary(formatter)
        _render_next_actions(
            formatter,
            [
                "Inspect nolossia.log for skipped-file reasons." if skipped_total else "All files scanned successfully.",
                "Proceed to duplicate analysis (read-only).",
            ],
        )

    if formatter.config.pipe_mode:
        _emit_pipe_summary(
            formatter,
            status="scan",
            phase="scan",
            masters=total_photos,
            duplicates=0,
            near=0,
            required="0B",
            available="0B",
            review=0,
            skipped=skipped_total,
            reports=[],
            review_samples=[],
            storage_breakdown={},
        )
    if formatter.config.pipe_mode:
        formatter.config.show_glossary = previous_glossary
        return

    prompt_message = (
        "→ Continue to quick plan? [y/N] (Enter cancels): " if quick_mode else "→ Proceed to the next step? [y/N] (Enter cancels): "
    )
    proceed = _prompt(formatter, prompt_message, default="").lower()
    if proceed != "y":
        formatter.line("Stopping after scan. No files have been changed.")
        formatter.config.show_glossary = previous_glossary
        return

    unique_size = _estimate_unique_size(hashed, clusters)
    proceed, dedupe_stats = _dedupe_flow(
        paths,
        hashed,
        clusters,
        unique_size,
        formatter,
        skipped_files=skipped_total,
        quick_mode=quick_mode,
    )
    if not proceed:
        formatter.config.show_glossary = previous_glossary
        return

    _merge_flow(
        paths,
        hashed,
        clusters,
        skipped_files=skipped_total,
        formatter=formatter,
        show_intro=False,
        show_banner=False,
        scan_totals=scan_summary,
        dedupe_stats=dedupe_stats,
        total_steps=total_steps,
    )
    formatter.config.show_glossary = previous_glossary


def _scan_fast_flow(
    paths: List[str],
    formatter: CLIFormatter,
    *,
    show_banner: bool = True,
    show_glossary: bool = True,
) -> None:
    """
    Fast scan-only flow: summarize supported files without hashing or dedupe.
    """
    previous_glossary = formatter.config.show_glossary
    formatter.config.show_glossary = show_glossary
    if show_banner:
        formatter.print_banner()
    _render_step_header(formatter, 1, 1, "Scan (fast)", "read-only inventory summary")

    all_files, skipped_symlinks = _collect_files(paths)
    supported_files = scanner.filter_supported_files(all_files)
    total_size_supported = sum(os.path.getsize(p) for p in supported_files)
    total_photos = len(supported_files)
    skipped_total = max(0, len(all_files) - total_photos)

    if total_photos == 0:
        formatter.warning("No supported image files were found in the provided locations.")
        formatter.line("Nothing to do. No files have been changed.")
        formatter.config.show_glossary = previous_glossary
        return

    summary_rows = [
        ("Supported photos", f"{total_photos:,}"),
        ("Total size", human_readable_size(total_size_supported)),
        ("Skipped files", str(skipped_total)),
        ("Skipped (symlinks)", str(skipped_symlinks)),
    ]
    _render_summary_box(formatter, summary_rows)
    _render_status_block(
        formatter,
        [
            ("ok", "Fast scan complete", f"{total_photos:,} supported photos"),
            (
                "warn",
                "Skipped files",
                f"{skipped_total} unsupported files" if skipped_total else "No skips detected",
            ),
        ],
    )
    _render_glossary(formatter)
    _render_next_actions(
        formatter,
        [
            "Run full scan for duplicate analysis.",
            "Run merge wizard to simulate changes.",
        ],
    )

    if formatter.config.pipe_mode:
        _emit_pipe_summary(
            formatter,
            status="scan_fast",
            phase="scan",
            masters=total_photos,
            duplicates=0,
            near=0,
            required="0B",
            available="0B",
            review=0,
            skipped=skipped_total,
            reports=[],
            review_samples=[],
            storage_breakdown={},
        )
    formatter.config.show_glossary = previous_glossary


def _dedupe_flow(
    paths: List[str],
    hashed: List[FileInfo],
    clusters: List[DuplicateCluster],
    unique_size: int,
    formatter: CLIFormatter,
    skipped_files: int = 0,
    *,
    quick_mode: bool = False,
) -> tuple[bool, dict]:
    """
    Display dedupe summary and optionally proceed to merge.
    """
    if not quick_mode:
        _render_step_header(
            formatter,
            2,
            3,
            "Duplicate analysis",
            "grouping exact + look-alikes (read-only)",
        )

    stats = _calculate_dedupe_stats(hashed, clusters, unique_size)
    report_path = reporting.artifact_path("dedupe_report.html")
    reporting.write_dedupe_report(hashed, clusters, report_path, unique_size)

    if quick_mode:
        formatter.blank()
        formatter.line(formatter.label("Quick duplicate check", level="info"))
        formatter.kv("Exact duplicates", f"{stats['exact_redundant']:,}")
        formatter.kv("Look-alikes", f"{stats['near_redundant']:,}")
        formatter.kv("Duplicates total", f"{stats['duplicate_total']:,}")
    else:
        summary_rows = [
            ("Masters", f"{stats['masters']:,} ({human_readable_size(stats['unique_size'])})"),
            ("Exact duplicates", f"{stats['exact_redundant']:,} ({human_readable_size(stats['exact_size'])})"),
            ("Look-alikes", f"{stats['near_redundant']:,} ({human_readable_size(stats['near_size'])})"),
            ("Duplicates total", f"{stats['duplicate_total']:,} ({human_readable_size(stats['duplicate_size_total'])})"),
            ("Skipped files", str(skipped_files)),
        ]
        _render_summary_box(formatter, summary_rows)
        _render_status_block(
            formatter,
            [
                ("ok", "Duplicate clustering finished", f"{stats['duplicate_total']:,} redundant files isolated"),
            ],
        )
        _render_reports_block(
            formatter,
            [(report_path, "dedupe_report.html")],
            max_items=3,
        )
        _render_next_actions(
            formatter,
            [
                f"Open {_link_with_fallback(formatter, report_path, 'dedupe_report.html')} for per-file review.",
                "Run the merge wizard to build a preview-only plan (no changes yet).",
            ],
        )

    if formatter.config.pipe_mode:
        _emit_pipe_summary(
            formatter,
            status="dedupe",
            phase="dedupe",
            masters=stats["masters"],
            duplicates=stats["exact_redundant"],
            near=stats["near_redundant"],
            required="0B",
            available="0B",
            review=0,
            skipped=skipped_files,
            reports=[report_path],
            review_samples=[],
            storage_breakdown={},
        )
    if formatter.config.pipe_mode:
        return False, stats

    prompt_message = (
        "→ Build the preview-only plan now? (no changes yet) [y/N] (Enter exits): "
        if quick_mode
        else "→ Start the 'merge wizard'? [y/N] (Enter exits): "
    )
    proceed = _prompt(formatter, prompt_message, default="").lower()
    if proceed != "y":
        formatter.line("Exiting Nolossia...")
        return False, stats

    return True, stats


def _merge_flow(
    paths: List[str],
    hashed: List[FileInfo],
    clusters: List[DuplicateCluster],
    skipped_files: int = 0,
    target_override: str | None = None,
    execute_requested: bool = True,
    formatter: CLIFormatter | None = None,
    *,
    show_intro: bool = True,
    show_banner: bool = True,
    scan_totals: dict | None = None,
    dedupe_stats: dict | None = None,
    total_steps: int = 3,
):
    """
    Merge setup, preview-only (no changes yet), and optional execution.
    """
    if formatter is None:
        formatter = CLIFormatter()
    _ensure_run_log_path()

    if show_banner and not formatter.config.pipe_mode:
        formatter.print_banner()

    scan_totals = scan_totals or {
        "supported": len(hashed),
        "size": sum((f.size or 0) for f in hashed),
        "skipped": skipped_files,
        "skipped_symlinks": 0,
    }
    unique_size_estimate = _estimate_unique_size(hashed, clusters)
    dedupe_stats = dedupe_stats or _calculate_dedupe_stats(hashed, clusters, unique_size_estimate)

    if show_intro and not formatter.config.pipe_mode:
        _render_mode_label(formatter, "PREVIEW")
        _render_step_header(formatter, 1, 3, "Scan & Inventory", "read-only pass over selected folders")
        _render_summary_box(
            formatter,
            [
                ("Supported photos", f"{scan_totals['supported']:,}"),
                ("Total size", human_readable_size(scan_totals["size"])),
                ("Skipped files", str(scan_totals["skipped"])),
                ("Skipped (symlinks)", str(scan_totals.get("skipped_symlinks", 0))),
            ],
        )
        _render_step_header(
            formatter,
            2,
            3,
            "Duplicate analysis",
            "exact + look-alikes (still read-only)",
        )
        _render_summary_box(
            formatter,
            [
                ("Masters", f"{dedupe_stats['masters']:,}"),
                ("Exact duplicates", f"{dedupe_stats['exact_redundant']:,}"),
                ("Look-alikes", f"{dedupe_stats['near_redundant']:,}"),
                ("Duplicates total", f"{dedupe_stats['duplicate_total']:,}"),
            ],
        )

    if not formatter.config.pipe_mode:
        formatter.blank()
        formatter.line(formatter.label("MERGE SETUP — Destination validation", level="info"))
        formatter.muted("Enter or confirm the destination folder for the merged library:")
        _print_destination_preflight_intro(formatter)

    target_abs: str | None = None
    target_default = target_override or ""
    while target_abs is None:
        prompt_message = _destination_prompt_message(target_default)
        target_input = _prompt(formatter, prompt_message, default=target_default)
        if not target_input:
            _render_failure_summary(
                formatter,
                status="ABORTED",
                phase="Destination validation",
                reason="User cancelled merge setup.",
                last_step="Destination selection",
                files_changed="None",
                remediation=[
                    "Re-run the merge command when ready.",
                    "Select an empty or YEAR/YEAR-MONTH destination before retrying.",
                    "Copy/paste example: /Library/2024/2024-05",
                ],
                artifacts=_DEFAULT_ARTIFACTS,
            )
            raise SystemExit(0)
        candidate = os.path.abspath(target_input)
        formatter.blank()
        formatter.line("Analyzing destination folder...")
        dest_reporter: Callable[[str], None] | None = None
        if formatter.config.verbose:
            def dest_reporter(message: str) -> None:
                formatter.verbose(f"[destination] {message}")
        try:
            merge_engine.validate_destination(candidate, reporter=dest_reporter)
            target_abs = candidate
        except MergePlanError as exc:
            formatter.warning(str(exc))
            _print_destination_warning(formatter)
            choice = _prompt(formatter, "Pick an option [1/2] (Enter retries the same folder): ", default="")
            if choice.strip() == "2":
                formatter.line("\n[2] Restart SET UP:")
                formatter.line("    Enter the destination folder for the merged library:")
                target_default = ""
                continue
            formatter.warning(
                "Please reorganize the destination to YEAR/YEAR-MONTH, then press Enter to re-check."
            )
            target_default = candidate

    if not formatter.config.pipe_mode:
        formatter.section("[1] Building merge plan...", icon=None)
    try:
        plan = merge_engine.build_merge_plan(
            hashed, clusters, target_abs, mode="on", skipped_files=skipped_files
        )
    except StorageError as exc:
        required_size = human_readable_size(_estimate_unique_size(hashed, clusters))
        available = human_readable_size(merge_engine.available_space(target_abs))
        _render_failure_summary(
            formatter,
            status="FAILED",
            phase="Merge plan",
            reason=str(exc),
            last_step="Preview-only (no changes yet) planning",
            files_changed="None",
            remediation=[
                "Free up storage at the destination or pick a different drive.",
                "Re-run the preview-only plan once space is available.",
            ],
            details=[
                ("Required storage", required_size),
                ("Available space", available),
            ],
            artifacts=_DEFAULT_ARTIFACTS,
        )
        raise SystemExit(1)

    merge_engine.dry_run(plan)
    summary = _build_merge_summary(plan, clusters, target_abs, paths, hashed)
    plan_reports = summary.get("reports", []) or _DEFAULT_ARTIFACTS
    pipe_status = (
        None
        if (execute_requested and formatter.config.pipe_mode and not formatter.config.stream_json)
        else "dry_run"
    )
    _print_merge_plan_summary(
        summary,
        formatter,
        pipe_status=pipe_status,
        total_steps=total_steps,
        storage_warning=plan.required_space > plan.destination_free,
    )

    if formatter.config.pipe_mode and not execute_requested:
        return

    if not execute_requested:
        formatter.muted("Preview only (no changes yet). Review the plan, then decide whether to move files.")
        return

    confirm = "EXECUTE" if formatter.config.pipe_mode else _prompt(
        formatter,
        "Type 'EXECUTE' to move files now (Enter cancels): ",
        default="",
    )
    if confirm != "EXECUTE":
        _render_failure_summary(
            formatter,
            status="ABORTED",
            phase="Merge execution",
            reason="User cancelled merge execution.",
            last_step="Preview-only complete — no changes yet",
            files_changed="None",
            remediation=[
                "Review the reports above if you need to double-check the plan.",
                "Re-run with --execute and confirm when you are ready to move files.",
            ],
            artifacts=plan_reports,
        )
        raise SystemExit(0)

    _render_mode_label(formatter, "EXECUTE")
    if not formatter.config.pipe_mode:
        formatter.section(f"[MERGE PHASE {total_steps}/{total_steps} - EXECUTE]", icon="⇒")
        formatter.info("Executing merge plan…")
        formatter.warning("EXECUTE is live. Files will move now.")
    try:
        execution_metadata = merge_engine.execute_merge(plan)
    except MergeExecutionError as exc:
        _render_failure_summary(
            formatter,
            status="FAILED",
            phase="Merge execution",
            reason=str(exc),
            last_step="Executing merge actions",
            files_changed="Unknown — review nolossia.log",
            remediation=[
                "Open nolossia.log for the stack trace and failing file path.",
                "Resolve the reported issue (e.g., hash mismatch) before re-running with --execute.",
            ],
            artifacts=plan_reports,
        )
        raise SystemExit(1)
    except StorageError as exc:
        _render_failure_summary(
            formatter,
            status="FAILED",
            phase="Merge execution",
            reason=str(exc),
            last_step="Executing merge actions",
            files_changed="Unknown — review nolossia.log",
            remediation=[
                "Free up additional space at the destination.",
                "Re-run the merge once the destination has more free space.",
            ],
            artifacts=plan_reports,
        )
        raise SystemExit(1)
    summary = _build_merge_summary(plan, clusters, target_abs, paths, hashed)
    if formatter.config.pipe_mode:
        _emit_pipe_summary(
            formatter,
            status="executed",
            phase="merge",
            masters=summary["masters_count"],
            duplicates=summary["exact_to_quar_count"],
            near=summary["near_marks"],
            required=summary["required"],
            available=summary["free"],
            review=summary["review_count"],
            skipped=summary["corrupt_count"],
            reports=summary.get("reports", []),
            review_samples=summary.get("review_samples", []),
            storage_breakdown=summary.get("storage_breakdown", {}),
        )
        return

    formatter.success("✓ Merge execution complete")
    formatter.kv("Files moved", str(summary["masters_count"]))
    formatter.kv("Isolated for safety", str(summary["exact_to_quar_count"]))
    formatter.kv("Look-alikes marked", str(summary["near_marks"]))
    rename_events = execution_metadata.get("renamed", [])
    if rename_events:
        formatter.warning("Filename collisions resolved with hash suffixes:")
        preview = rename_events[:5]
        for original, new in preview:
            formatter.line(f"  - {os.path.basename(original)} → {os.path.basename(new)}")
        if len(rename_events) > len(preview):
            formatter.muted(
                f"... +{len(rename_events) - len(preview)} more renames. "
                f"See {reporting.LOG_FILE_NAME} for the full list."
            )
    formatter.blank()
    formatter.line("To view the results you can now open:")
    formatter.bullet(_link_with_fallback(formatter, summary["merge_report_path"], "merge_report.html"))
    formatter.bullet(_link_with_fallback(formatter, summary["dedupe_report_path"], "dedupe_report.html"))
    formatter.line("The previous dedupe_report.html was refreshed after EXECUTE to avoid stale guidance.")


def _print_destination_warning(formatter: CLIFormatter) -> None:
    """
    Display destination structure warning per Phase 6.
    """
    formatter.warning("⚠ Destination is not empty nor YEAR/YEAR-MONTH structured.")
    formatter.line("To continue, pick one of these remediation options:")
    formatter.line("Fix 1: Reorganize this folder into YEAR/YEAR-MONTH.")
    formatter.bullet(
        "[1] Reorganize this folder into YEAR/YEAR-MONTH (e.g., /Library/2024/2024-05), then select option 1 to re-check.",
        indent="    ",
    )
    formatter.line("Fix 2: Choose a different destination folder.")
    formatter.bullet(
        "[2] Choose a different empty or already chronological folder (you'll be prompted again).",
        indent="    ",
    )
    formatter.bullet(
        "[Enter] Leave the next prompt empty to cancel merge setup with no changes.",
        indent="    ",
    )
    formatter.line("Copy/paste example: /Library/2024/2024-05")
    formatter.line("Docs: CLI_COMMANDS.md (destination requirements)")


def _format_year_month_breakdown(actions: Sequence[MergeAction], out_path: str) -> str:
    """
    Build compact YEAR/YEAR-MONTH breakdown from move actions.
    """
    counter: Counter[str] = Counter()
    for action in actions:
        if isinstance(action, MoveMasterAction):
            rel = os.path.relpath(action.dst, out_path)
            parts = rel.split(os.sep)
            if len(parts) >= 2:
                year, year_month = parts[0], parts[1]
                counter[f"{year}/{year_month}"] += 1
    if not counter:
        return "No chronological mapping detected"
    return ", ".join(f"{key} : {value} photos" for key, value in sorted(counter.items()))


def _collect_review_actions(actions: List[MergeAction], out_path: str) -> list[MoveMasterAction]:
    """
    Return MOVE_MASTER actions targeting the REVIEW/ bucket.
    """
    review_root = os.path.join(os.path.abspath(out_path), "REVIEW")
    matches: list[MoveMasterAction] = []
    for action in actions:
        if not isinstance(action, MoveMasterAction):
            continue
        dst = os.path.abspath(action.dst)
        try:
            if os.path.commonpath([dst, review_root]) == review_root:
                matches.append(action)
        except ValueError:
            continue
    return matches


def _review_samples(actions: list[MoveMasterAction], out_path: str, limit: int = 5) -> list[str]:
    """
    Build a short list of REVIEW file paths relative to the destination.
    """
    base = os.path.abspath(out_path)
    samples: list[str] = []
    for action in actions:
        rel = os.path.relpath(os.path.abspath(action.dst), base)
        if rel.startswith(".."):
            rel = os.path.basename(action.dst)
        reason_text = describe_review_reason(getattr(action, "review_reason", None))
        entry = rel if not reason_text else f"{rel} — {reason_text}"
        samples.append(entry)
        if len(samples) >= limit:
            break
    return samples


def _count_folder_merges(actions: List[MergeAction]) -> int:
    """
    Count folders receiving multiple files (merge collisions risk).
    """
    folder_counts = Counter(os.path.dirname(action.dst) for action in actions if isinstance(action, MoveMasterAction))
    return sum(1 for count in folder_counts.values() if count > 1)


def _count_filename_collisions(actions: List[MergeAction]) -> int:
    """
    Count filename collisions based on planned destinations.
    """
    dest_counts = Counter(action.dst for action in actions if isinstance(action, MoveMasterAction))
    return sum(count - 1 for count in dest_counts.values() if count > 1)


def _chronology_rows(actions: List[MergeAction], out_path: str, limit: int = 5) -> list[tuple[str, int, str]]:
    """
    Build sorted chronology rows (Year-Month, count, percent).
    """
    counter: Counter[str] = Counter()
    for action in actions:
        if not isinstance(action, MoveMasterAction):
            continue
        rel = os.path.relpath(action.dst, out_path)
        parts = rel.split(os.sep)
        if len(parts) >= 2:
            year, year_month = parts[0], parts[1]
            label = f"{year}-{year_month[-2:]}"
            counter[label] += 1
    total = sum(counter.values()) or 1
    rows: list[tuple[str, int, str]] = []
    for label, count in counter.most_common(limit):
        percent = f"{round((count / total) * 100)}%"
        rows.append((label, count, percent))
    remaining = total - sum(count for _, count, _ in rows)
    if remaining > 0:
        percent = f"{round((remaining / total) * 100)}%"
        rows.append(("...", remaining, percent))
    return rows


def _build_merge_summary(
    plan: MergePlan,
    clusters: List[DuplicateCluster],
    out_path: str,
    source_paths: List[str],
    files: List[FileInfo],
) -> dict:
    """
    Prepare merge summary values for CLI output.
    """
    exact_clusters, near_clusters = _partition_clusters(clusters)
    actions = plan.actions
    masters_actions = sorted(
        (a for a in actions if isinstance(a, MoveMasterAction)),
        key=lambda action: (os.path.dirname(action.dst), os.path.basename(action.dst)),
    )
    near_mark_actions = sorted(
        (a for a in actions if isinstance(a, MarkNearDuplicateAction)),
        key=lambda action: (action.master, action.src),
    )
    near_marks = len(near_mark_actions)
    near_marks_size_bytes = sum(a.size or 0 for a in near_mark_actions)
    near_marks_size = human_readable_size(near_marks_size_bytes)
    exact_quarantine_actions = sorted(
        (a for a in actions if isinstance(a, MoveToQuarantineExactAction)),
        key=lambda action: (os.path.dirname(action.dst), os.path.basename(action.dst)),
    )
    exact_quarantine_size = sum(a.size or 0 for a in exact_quarantine_actions)
    quarantine_path = (
        os.path.dirname(exact_quarantine_actions[0].dst) if exact_quarantine_actions else os.path.join(out_path, "QUARANTINE_EXACT")
    )

    year_month_breakdown = _format_year_month_breakdown(masters_actions, out_path)
    free_after = max(plan.destination_free - plan.required_space, 0)
    chrono_rows = _chronology_rows(masters_actions, out_path)

    review_actions = _collect_review_actions(masters_actions, out_path)
    review_samples = _review_samples(review_actions, out_path)
    review_ids = {id(action) for action in review_actions}
    masters_storage_bytes = sum((action.size or 0) for action in masters_actions if id(action) not in review_ids)
    review_storage_bytes = sum((action.size or 0) for action in review_actions)
    storage_breakdown_bytes = {
        "masters": masters_storage_bytes,
        "quarantine": exact_quarantine_size,
        "review": review_storage_bytes,
    }
    storage_breakdown = {key: human_readable_size(value) for key, value in storage_breakdown_bytes.items()}
    masters_total_size = storage_breakdown_bytes["masters"] + storage_breakdown_bytes["review"]

    return {
        "source_paths": ", ".join(sorted(os.path.abspath(p) for p in source_paths)),
        "out_path": os.path.abspath(out_path),
        "required": human_readable_size(plan.required_space),
        "free": human_readable_size(plan.destination_free),
        "free_after": human_readable_size(free_after),
        "total_photos": plan.total_files,
        "masters_count": len(masters_actions),
        "masters_size": human_readable_size(masters_total_size),
        "exact_to_quar_count": len(exact_quarantine_actions),
        "exact_to_quar_size": human_readable_size(exact_quarantine_size),
        "near_clusters": len(near_clusters),
        "near_files_count": sum(len(c.files) for c in near_clusters),
        "near_marks": near_marks,
        "near_marks_size": near_marks_size,
        "year_month_breakdown": year_month_breakdown,
        "chronology_rows": chrono_rows,
        "missing_exif_count": sum(1 for f in files if f.exif_datetime is None),
        "review_count": len(review_actions),
        "review_samples": review_samples,
        "storage_breakdown": storage_breakdown,
        "storage_breakdown_bytes": storage_breakdown_bytes,
        "folder_merges_count": _count_folder_merges(masters_actions),
        "filename_collisions_count": _count_filename_collisions(masters_actions),
        "corrupt_count": plan.skipped_files,
        "quarantine_path": quarantine_path,
        "mergeplan_json_path": reporting.artifact_path("merge_plan.json"),
        "dedupe_report_path": reporting.artifact_path("dedupe_report.html"),
        "merge_report_path": reporting.artifact_path("merge_report.html"),
        "manifest_json_path": reporting.artifact_path("source_manifest.json"),
        "manifest_csv_path": reporting.artifact_path("source_manifest.csv"),
        "manifest_html_path": reporting.artifact_path("source_manifest.html"),
        "reports": [
            reporting.artifact_path("merge_plan.json"),
            reporting.artifact_path("dedupe_report.html"),
            reporting.artifact_path("merge_report.html"),
            reporting.artifact_path("source_manifest.json"),
            reporting.artifact_path("source_manifest.csv"),
            reporting.artifact_path("source_manifest.html"),
        ],
    }


def _print_merge_plan_summary(
    summary: dict,
    formatter: CLIFormatter,
    *,
    pipe_status: str | None = "dry_run",
    total_steps: int = 3,
    storage_warning: bool = False,
) -> None:
    """
    Render merge plan summary per Phase 6 spec.
    """
    statuses = [
        ("ok", "Merge plan generated", f"{summary['masters_count']:,} masters scheduled"),
        ("ok", "Exact duplicates isolated for safety", f"{summary['exact_to_quar_count']:,} files"),
    ]
    if summary["filename_collisions_count"]:
        statuses.append(("warn", "Filename collisions detected", f"{summary['filename_collisions_count']} planned renames"))
    if summary["corrupt_count"]:
        statuses.append(("warn", "Skipped unreadable files", str(summary["corrupt_count"])))

    _render_step_header(formatter, total_steps, total_steps, "Preview-only simulation", "storage planning & duplicate routing")
    summary_rows = [
        (
            "Masters storage",
            f"{summary['storage_breakdown']['masters']} • {summary['masters_count']:,} files",
        ),
        (
            "Exact → Isolated for safety storage",
            f"{summary['storage_breakdown']['quarantine']} • {summary['exact_to_quar_count']:,} files",
        ),
        (
            "Set aside for review storage",
            f"{summary['storage_breakdown']['review']} • {summary['review_count']:,} files",
        ),
        ("Look-alike marks", f"{summary['near_marks']:,} ({summary['near_marks_size']})"),
        ("Required storage (total)", summary["required"]),
        ("Free space", summary["free"]),
        ("Free after merge", summary["free_after"]),
        ("Skipped files", str(summary["corrupt_count"])),
    ]
    _render_summary_box(formatter, summary_rows)
    if not formatter.config.pipe_mode:
        formatter.muted(
            "RAW + sidecars: RAW files stay intact; sidecars (e.g., .xmp) are not processed. "
            "Keep them next to RAW files and copy them after the merge."
        )
    if formatter.config.verbose:
        _render_status_block(formatter, statuses)
    if summary.get("review_samples") and not formatter.config.pipe_mode:
        formatter.blank()
        formatter.line(formatter.label("Sample review files", level="info"))
        for entry in summary["review_samples"]:
            formatter.bullet(entry, indent="  - ")
        formatter.muted("Full review listings will be captured in merge_report.html after changes are applied.")
    if pipe_status:
        _emit_pipe_summary(
            formatter,
            status=pipe_status,
            phase="merge",
            masters=summary["masters_count"],
            duplicates=summary["exact_to_quar_count"],
            near=summary["near_marks"],
            required=summary["required"],
            available=summary["free"],
            review=summary["review_count"],
            skipped=summary["corrupt_count"],
            reports=summary.get("reports", []),
            review_samples=summary.get("review_samples", []),
            storage_breakdown=summary.get("storage_breakdown", {}),
        )
    if formatter.config.pipe_mode:
        return
    _render_chronology_table(formatter, summary.get("chronology_rows", []))
    formatter.muted(f"Destination: {summary['out_path']}")
    formatter.muted(f"Sources: {summary['source_paths']}")

    report_links = [
        (summary["mergeplan_json_path"], "merge_plan.json"),
        (summary["dedupe_report_path"], "dedupe_report.html"),
        (summary["merge_report_path"], "merge_report.html"),
        (summary["manifest_json_path"], "source_manifest.json"),
        (summary["manifest_csv_path"], "source_manifest.csv"),
        (summary["manifest_html_path"], "source_manifest.html"),
    ]
    _render_reports_block(formatter, report_links, max_items=2)

    warnings: list[str] = []
    if storage_warning:
        warnings.append(
            "Required storage exceeds available free space. Free up space before you move files."
        )
    if summary["folder_merges_count"]:
        warnings.append(
            f"{summary['folder_merges_count']} folders will receive multiple masters. Verify YEAR/YEAR-MONTH structure."
        )
    if summary["filename_collisions_count"]:
        warnings.append(
            f"{summary['filename_collisions_count']} filename collisions will be resolved with hash suffixes."
        )
    _render_warnings_frame(formatter, warnings)

    next_steps = [
        "Review the reports above first.",
    ]
    if summary["review_count"]:
        next_steps.append("Check the set-aside-for-review list before you decide.")
    next_steps.append("Nothing moves until you confirm the execute step. Re-run when you are ready to move files.")
    _render_next_steps(formatter, next_steps)


def _estimate_unique_size(files: List[FileInfo], clusters: List[DuplicateCluster]) -> int:
    """
    Estimate unique size counting one master per cluster plus singletons.
    """
    unique_paths: set[str] = set()
    size = 0
    for cluster in clusters:
        master = cluster.master or duplicates.select_master(cluster)
        if master.path not in unique_paths:
            unique_paths.add(master.path)
            size += master.size
    clustered_paths = {f.path for c in clusters for f in c.files}
    for file in files:
        if file.path not in clustered_paths and file.path not in unique_paths:
            unique_paths.add(file.path)
            size += file.size
    return size


def _calculate_dedupe_stats(
    hashed: List[FileInfo],
    clusters: List[DuplicateCluster],
    unique_size: int,
) -> dict:
    exact_clusters, near_clusters = _partition_clusters(clusters)
    exact_redundant = sum(len(c.redundant) for c in exact_clusters)
    near_redundant = sum(len(c.redundant) for c in near_clusters)
    masters = len(hashed) - exact_redundant - near_redundant
    exact_size = sum(f.size for c in exact_clusters for f in c.redundant)
    near_size = sum(f.size for c in near_clusters for f in c.redundant)
    duplicate_total = exact_redundant + near_redundant
    duplicate_size_total = exact_size + near_size
    return {
        "masters": masters,
        "unique_size": unique_size,
        "exact_redundant": exact_redundant,
        "near_redundant": near_redundant,
        "duplicate_total": duplicate_total,
        "exact_size": exact_size,
        "near_size": near_size,
        "duplicate_size_total": duplicate_size_total,
        "exact_clusters": exact_clusters,
        "near_clusters": near_clusters,
    }


def main():
    """
    Argument parser entry point.

    Args:
        None

    Returns:
        None

    Raises:
        SystemExit: When execution fails.
    """
    parser = argparse.ArgumentParser(
        prog="nolossia",
        description="Nolossia CLI. Designed to prevent data loss.",
        epilog="Docs alias: /docs/cli (CLI_COMMANDS.md quick reference).",
    )
    parser.add_argument(
        "--no-banner",
        action="store_true",
        help="Suppress the Nolossia banner (also disables Unicode art).",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colors regardless of terminal support.",
    )
    parser.add_argument(
        "--plain",
        action="store_true",
        help="Plain mode: no banner, ASCII-only separators, no ANSI colors.",
    )
    parser.add_argument(
        "--ascii",
        dest="force_ascii",
        action="store_true",
        help="Force ASCII output even if the terminal supports Unicode.",
    )
    parser.add_argument(
        "--color",
        choices=["auto", "always", "never"],
        default=None,
        help="Force color usage: auto (default), always, or never.",
    )
    parser.add_argument(
        "--theme",
        choices=["light", "dark", "high-contrast-light", "high-contrast-dark"],
        default="light",
        help="Theme palette: light (default), dark, high-contrast-light, or high-contrast-dark.",
    )
    parser.add_argument(
        "--docs-alias",
        action="store_true",
        help="Print docs alias path: /docs/cli (quick reference).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print duplicate rule decisions and destination validation steps.",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "tty", "plain", "pipe"],
        default="auto",
        help="Force output mode: auto (default), tty, plain, or pipe (single-line).",
    )
    parser.add_argument(
        "--pipe-format",
        choices=["json", "kv"],
        default="json",
        help="When writing to pipe/redirect, output single-line JSON (default) or key/value pairs.",
    )
    parser.add_argument(
        "--stream-json",
        action="store_true",
        help="When in pipe mode, emit JSON progress events for each phase (JSON format only).",
    )
    parser.add_argument(
        "--max-pixels",
        type=_pixel_limit_arg,
        default=None,
        help=(
            f"Override Pillow decompression guard (default {DEFAULT_PIXEL_LIMIT} pixels). "
            f"Maximum allowed is {MAX_OVERRIDE_LIMIT}. Also configurable via ${PIXEL_LIMIT_ENV}."
        ),
    )
    parser.add_argument(
        "--executor",
        choices=["auto", "process", "thread"],
        default=None,
        help=(
            "Executor mode for hashing/metadata: auto (default), process, or thread. "
            f"Also configurable via ${EXECUTOR_ENV}."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    start_parser = subparsers.add_parser(
        "start",
        help="Interactive launcher with banner, intro, and glossary",
        description=(
            "Start is an interactive launcher that shows the Nolossia banner, a short intro, "
            "and a glossary before prompting you to run scan, dedupe, or merge."
        ),
    )
    start_group = start_parser.add_mutually_exclusive_group()
    start_group.add_argument(
        "--glossary",
        dest="glossary",
        action="store_true",
        help="Show the glossary (default).",
    )
    start_group.add_argument(
        "--no-glossary",
        dest="glossary",
        action="store_false",
        help="Hide the glossary.",
    )
    start_parser.set_defaults(glossary=True)

    scan_parser = subparsers.add_parser(
        "scan",
        help="Scan directories for photos",
        description=(
            "Scan is a read-only operation. It walks the provided paths, identifies supported image files, "
            "collects basic metadata and hashes, and prepares duplicate statistics.\n"
            "SCAN does not move, delete, or modify any files. It is the safe first step before running dedupe "
            "or planning a merge."
        ),
    )
    scan_parser.add_argument("paths", nargs="+", help="One or more paths to scan")
    scan_parser.add_argument(
        "--fast",
        action="store_true",
        help="Skip hashing/dedupe and print a short scan summary only",
    )
    scan_parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode: shorter wizard output with fewer steps (read-only).",
    )

    dedupe_parser = subparsers.add_parser(
        "dedupe",
        help="Analyze duplicate and look-alike photos",
        description=(
            "Dedupe is a read-only analysis that detects, groups, and classifies duplicate and look-alike photos.\n"
            "It does not move, delete, or modify any files; it only prepares information "
            "used later by the merge preview-only plan."
        ),
    )
    dedupe_parser.add_argument("paths", nargs="+", help="One or more paths to analyze")

    organize_parser = subparsers.add_parser(
        "organize",
        help="Preview chronological organization plan",
        description=(
            "Organize computes and prints a YEAR / YEAR-MONTH target path for each photo, using the same mandatory "
            "chronological structure that merge applies.\n"
            "It does not move or rename any files; it only shows the planned destinations so you can review the "
            "structure before running a merge."
        ),
    )
    organize_parser.add_argument("paths", nargs="+", help="One or more paths to organize")
    organize_parser.add_argument("--out", required=True, help="Destination library path for organization preview")

    merge_parser = subparsers.add_parser(
        "merge",
        help="Merge photos into a unified library with preview-only (no changes yet) and safety checks",
        description=(
            "Merge runs the full scan → dedupe → merge-planning flow for the specified paths.\n"
            "By default it performs a preview-only plan (no changes yet), building a MergePlan and reporting what would be moved and "
            "isolated for safety without changing any files.\n"
            "Only when you explicitly pass --execute and confirm will Nolossia perform the actual filesystem "
            "operations.\n"
            "On errors or user aborts Nolossia prints a FAILURE SUMMARY with remediation steps and log/report paths."
        ),
    )
    merge_parser.add_argument("paths", nargs="+", help="One or more source paths to merge")
    merge_parser.add_argument("--out", required=True, help="Destination library path")

    merge_group = merge_parser.add_mutually_exclusive_group()
    merge_group.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Preview only (no changes yet): simulate merge without changes (default)",
    )
    merge_group.add_argument("--execute", dest="dry_run", action="store_false", help="Execute merge after confirmation")
    merge_parser.set_defaults(dry_run=True)

    undo_parser = subparsers.add_parser(
        "undo",
        help="Undo a prior merge using the source manifest",
        description=(
            "Undo rolls back a prior merge using artifacts/source_manifest.json.\n"
            "It never overwrites user files and routes conflicts to REVIEW/UNDO_CONFLICTS."
        ),
    )
    undo_parser.add_argument("operation_id", nargs="?", help="Operation ID (batch_id) to undo")
    undo_parser.add_argument("--last", action="store_true", help="Undo the last merge")
    undo_parser.add_argument("--preview", action="store_true", help="Preview undo actions without moving files")

    args = parser.parse_args()
    if args.docs_alias:
        print("Docs alias: /docs/cli (CLI_COMMANDS.md quick reference)")
        raise SystemExit(0)

    pixel_limit, pixel_source = configure_pixel_limit(args.max_pixels)
    formatter_config = detect_terminal_capabilities(
        color_preference=args.color or "auto",
        plain_mode=args.plain,
        force_ascii=args.force_ascii,
        no_color_flag=args.no_color,
        stdout_isatty=sys.stdout.isatty(),
        mode_preference=args.mode,
        theme_preference=args.theme,
    )
    formatter_config.verbose = args.verbose
    formatter_config.pipe_format = args.pipe_format
    formatter_config.stream_json = args.stream_json and args.pipe_format == "json"
    formatter_config.pixel_limit = pixel_limit
    formatter_config.pixel_limit_source = pixel_source
    if args.no_banner:
        formatter_config.show_banner = False
    if args.plain:
        formatter_config.plain_mode = True
        formatter_config.show_banner = False
        formatter_config.use_color = False
        formatter_config.unicode_enabled = False
        formatter_config.osc8_links = False
    pipe_stream = None
    if formatter_config.pipe_mode:
        pipe_stream = io.StringIO()
    formatter = CLIFormatter(formatter_config, stream=pipe_stream or sys.stdout)
    if pipe_stream:
        formatter.pipe_target = sys.stdout

    _ensure_run_log_path()
    if args.stream_json and args.pipe_format != "json":
        reporting.write_log(
            ["[WARN] --stream-json requires JSON output; ignoring because --pipe-format=kv."]
        )
    if pixel_source != "default":
        origin = "CLI flag" if pixel_source == "cli" else f"${PIXEL_LIMIT_ENV}"
        ui_warning_text = (
            f"Pixel safety limit set to {pixel_limit:,} via {origin}. "
            "Oversized images may consume significant memory."
        )
        log_warning_text = f"[WARNING] {ui_warning_text}"
        formatter.warning(ui_warning_text)
        reporting.write_log([log_warning_text])
    configure_executor_mode(args.executor)

    try:
        reporting.write_log([f"[INFO] Command {args.command} started"])

        if args.command == "start":
            exit_code = _start_flow(formatter, show_glossary=args.glossary)
            if exit_code:
                sys.exit(exit_code)
        elif args.command == "scan":
            if getattr(args, "fast", False):
                _scan_fast_flow(args.paths, formatter, show_banner=False, show_glossary=False)
            else:
                _scan_flow(
                    args.paths,
                    formatter,
                    quick_mode=getattr(args, "quick", False),
                    show_banner=False,
                    show_glossary=False,
                )
        elif args.command == "dedupe":
            hashed, clusters, skipped, skipped_symlinks = _scan_and_group(args.paths, formatter=formatter)
            unique_size = _estimate_unique_size(hashed, clusters)
            proceed, stats = _dedupe_flow(args.paths, hashed, clusters, unique_size, formatter, skipped_files=skipped)
            if proceed:
                scan_summary = {
                    "supported": len(hashed),
                    "size": sum((f.size or 0) for f in hashed),
                    "skipped": skipped,
                    "skipped_symlinks": skipped_symlinks,
                }
                _merge_flow(
                    args.paths,
                    hashed,
                    clusters,
                    skipped_files=skipped,
                    formatter=formatter,
                    show_banner=False,
                    show_intro=False,
                    scan_totals=scan_summary,
                    dedupe_stats=stats,
                )
        elif args.command == "organize":
            fileinfos = scanner.scan_paths(args.paths)
            enriched = metadata.enrich_metadata(fileinfos)
            hashed = hashing.add_hashes(enriched)
            for fileinfo in hashed:
                target = organizer.determine_target_path(
                    fileinfo, args.out, merge_mode="on", source_root=os.path.commonpath(args.paths)
                )
                label = formatter.label("NEW", level="success")
                formatter.line(f"{label} {fileinfo.path} -> {target}")
            reporting.write_log([f"[INFO] Organization plan generated for {len(hashed)} files"])

        elif args.command == "merge":
            if not formatter.config.pipe_mode:
                _render_settings_tiers(
                    formatter,
                    execute_requested=not args.dry_run,
                    source_paths=args.paths,
                    destination_path=args.out,
                )
                _render_sensitivity_banner(formatter)
            hashed, clusters, skipped, skipped_symlinks = _scan_and_group(args.paths, formatter=formatter)
            scan_summary = {
                "supported": len(hashed),
                "size": sum((f.size or 0) for f in hashed),
                "skipped": skipped,
                "skipped_symlinks": skipped_symlinks,
            }
            unique_size = _estimate_unique_size(hashed, clusters)
            stats = _calculate_dedupe_stats(hashed, clusters, unique_size)
            if formatter.config.pipe_mode and formatter.config.stream_json:
                _emit_pipe_summary(
                    formatter,
                    status="scan",
                    phase="scan",
                    masters=scan_summary["supported"],
                    duplicates=0,
                    near=0,
                    required="0B",
                    available="0B",
                    review=0,
                    skipped=scan_summary["skipped"],
                    reports=[],
                    review_samples=[],
                    storage_breakdown={},
                )
                _emit_pipe_summary(
                    formatter,
                    status="dedupe",
                    phase="dedupe",
                    masters=stats["masters"],
                    duplicates=stats["exact_redundant"],
                    near=stats["near_redundant"],
                    required="0B",
                    available="0B",
                    review=0,
                    skipped=skipped,
                    reports=[],
                    review_samples=[],
                    storage_breakdown={},
                )
            _merge_flow(
                args.paths,
                hashed,
                clusters,
                skipped_files=skipped,
                target_override=args.out,
                execute_requested=not args.dry_run,
                formatter=formatter,
                show_banner=False,
                scan_totals=scan_summary,
                dedupe_stats=stats,
            )
        elif args.command == "undo":
            if formatter.config.pipe_mode:
                _render_failure_summary(
                    formatter,
                    status="BLOCKED",
                    phase="UNDO",
                    reason="Undo is not available in pipe mode yet.",
                    last_step="Non-interactive output mode",
                    remediation=[
                        "Re-run undo in interactive mode (tty/plain/ascii).",
                        "PIPE_SCHEMA.md does not define an undo contract yet.",
                    ],
                    artifacts=_UNDO_ARTIFACTS,
                )
                sys.exit(2)
            if args.last and args.operation_id:
                _render_failure_summary(
                    formatter,
                    status="FAILED",
                    phase="UNDO",
                    reason="Choose either --last or an explicit operation_id, not both.",
                    last_step="Undo argument validation",
                    remediation=["Run 'nolossia undo --last' or 'nolossia undo <operation_id>'."],
                    artifacts=_UNDO_ARTIFACTS,
                )
                sys.exit(1)
            if not args.last and not args.operation_id:
                _render_failure_summary(
                    formatter,
                    status="FAILED",
                    phase="UNDO",
                    reason="Missing operation id. Provide --last or an operation_id.",
                    last_step="Undo argument validation",
                    remediation=["Run 'nolossia undo --last' or 'nolossia undo <operation_id>'."],
                    artifacts=_UNDO_ARTIFACTS,
                )
                sys.exit(1)
            operation_id = args.operation_id
            if args.last:
                try:
                    operation_id, _ = merge_engine.load_source_manifest(
                        reporting.artifact_path("source_manifest.json")
                    )
                except UndoInputError as exc:
                    _render_failure_summary(
                        formatter,
                        status="FAILED",
                        phase="UNDO",
                        reason=str(exc),
                        last_step="Load source manifest",
                        remediation=[
                            "Run a merge with --execute to generate a source manifest.",
                            "Verify artifacts/source_manifest.json exists.",
                        ],
                        artifacts=_UNDO_ARTIFACTS,
                    )
                    sys.exit(1)
            exit_code = _undo_flow(operation_id, args.preview, formatter)
            sys.exit(exit_code)

    except KeyboardInterrupt:
        reporting.write_log(["[WARN] Operation aborted via Ctrl+C"])
        _render_failure_summary(
            formatter,
            status="ABORTED",
            phase="CLI",
            reason="Interrupted by user (Ctrl+C).",
            last_step="Interactive prompt",
            files_changed="Unknown — review nolossia.log",
            remediation=[
                "Re-run the command when ready.",
                "Inspect nolossia.log for any partial progress details.",
            ],
            artifacts=_DEFAULT_ARTIFACTS,
        )
        sys.exit(1)
    except NolossiaError as exc:
        reporting.write_log([f"[ERROR] {exc}"])
        _render_failure_summary(
            formatter,
            status="FAILED",
            phase="CLI",
            reason=str(exc),
            last_step="Command initialization",
            files_changed="Unknown — review nolossia.log",
            remediation=[
                "Review the error message and nolossia.log for details.",
                "Address the reported issue, then rerun the command.",
            ],
            artifacts=_DEFAULT_ARTIFACTS,
        )
        sys.exit(1)
    except Exception as exc:  # pragma: no cover - defensive catch for CLI UX
        reporting.write_log([f"[ERROR] Unexpected failure: {exc}"])
        _render_failure_summary(
            formatter,
            status="FAILED",
            phase="CLI",
            reason=f"Unexpected failure: {exc}",
            last_step="Command execution",
            files_changed="Unknown — review nolossia.log",
            remediation=[
                "Inspect nolossia.log for the traceback.",
                "Report the issue with the captured log if it persists.",
            ],
            artifacts=_DEFAULT_ARTIFACTS,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
