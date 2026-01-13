"""
Module: reporting
Purpose: Logging and report generation utilities.
"""

import csv
import hashlib
import html
import json
import os
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, List
from urllib.parse import quote

from .hashing import phash_distance
from .models.actions import (
    MarkNearDuplicateAction,
    MoveMasterAction,
    MoveToQuarantineExactAction,
)
from .models.cluster import DuplicateCluster
from .models.fileinfo import FileInfo
from .models.mergeplan import MergePlan
from .review import describe_review_reason
from .utils import human_readable_size as humanize_bytes


ARTIFACTS_DIR = "artifacts"
LOG_FILE_NAME = os.path.join(ARTIFACTS_DIR, "nolossia.log")
NEAR_DUP_CLUSTER_LIMIT = 25
NEAR_DUP_CANDIDATE_LIMIT = 12


def artifact_path(filename: str) -> str:
    return os.path.abspath(os.path.join(ARTIFACTS_DIR, filename))


def ensure_log_initialized() -> str:
    """Ensure the Nolossia log file exists and return its absolute path."""
    path = os.path.abspath(LOG_FILE_NAME)
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    with open(path, "a", encoding="utf-8"):
        pass
    return path


def _stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(value.encode("utf-8"), usedforsecurity=False).hexdigest()
    return f"{prefix}-{digest[:12]}"


def _attr(value: str) -> str:
    return html.escape(value, quote=True)


def _text(value: str) -> str:
    return html.escape(value, quote=True)


def _file_uri(path: str) -> str:
    absolute = os.path.abspath(path)
    return "file://" + quote(absolute, safe="/:\\")


def _js_arg(value: str) -> str:
    return json.dumps(value)


class _HtmlBuffer:
    def __init__(self, handle, flush_every: int = 200):
        self._handle = handle
        self._buffer: list[str] = []
        self._flush_every = flush_every

    def append(self, line: str) -> None:
        self._buffer.append(line)
        if len(self._buffer) >= self._flush_every:
            self.flush()

    def extend(self, lines: list[str]) -> None:
        for line in lines:
            self.append(line)

    def flush(self) -> None:
        if not self._buffer:
            return
        self._handle.write("\n".join(self._buffer) + "\n")
        self._buffer.clear()

    def close(self) -> None:
        if not self._buffer:
            return
        self._handle.write("\n".join(self._buffer))
        self._buffer.clear()


def _resolution_area(info: FileInfo | None) -> int:
    if not info or not info.resolution:
        return 0
    width, height = info.resolution
    return width * height


def _datetime_score(info: FileInfo | None) -> int:
    if not info or not info.exif_datetime:
        return 0
    return int(info.exif_datetime.timestamp())


def _phash_distance(master: FileInfo | None, candidate: FileInfo | None) -> int:
    if not master or not candidate:
        return 999
    if not master.phash or not candidate.phash:
        return 999
    return phash_distance(master.phash, candidate.phash)


def _near_cluster_priority(cluster: DuplicateCluster) -> tuple[int, int, int, str]:
    master = cluster.master or (cluster.files[0] if cluster.files else None)
    if not master:
        return (999, 0, 0, "")
    best_distance = min((_phash_distance(master, item) for item in cluster.redundant), default=999)
    return (
        best_distance,
        -_resolution_area(master),
        -_datetime_score(master),
        os.path.abspath(master.path),
    )


def _near_candidate_priority(master: FileInfo | None, candidate: FileInfo) -> tuple[int, int, int, str]:
    return (
        _phash_distance(master, candidate),
        -_resolution_area(candidate),
        -_datetime_score(candidate),
        os.path.abspath(candidate.path),
    )


def _selection_reason_text(file: FileInfo | None) -> str | None:
    if not file:
        return None
    reason = getattr(file, "selection_reason", None)
    if not reason:
        return None
    mapping = {
        "RAW_BEATS_JPEG": "RAW beats JPEG/HEIC",
        "HIGHER_RESOLUTION_WINS": "Higher resolution wins",
        "LARGER_FILESIZE_WINS": "Larger file size wins",
        "MORE_EXIF_WINS": "More EXIF metadata wins",
        "GPS_PRESENT_BEATS_GPS_ABSENT": "GPS present wins",
        "OLDEST_CAPTURE_WINS": "Oldest capture date wins",
    }
    return mapping.get(reason, reason.replace("_", " ").title())


def write_log(entries: List[str], outfile: str = LOG_FILE_NAME):
    """
    Append entries to logfile.
    """
    directory = os.path.dirname(os.path.abspath(outfile)) or "."
    os.makedirs(directory, exist_ok=True)
    timestamp = datetime.utcnow().isoformat()
    with open(outfile, "a", encoding="utf-8") as handle:
        for entry in entries:
            normalized = entry if entry.startswith("[") else f"[INFO] {entry}"
            handle.write(f"[{timestamp}] {normalized}\n")


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if is_dataclass(o):
            return asdict(o)
        return super().default(o)


def _is_review_destination(dst: str, destination_root: str | None) -> bool:
    if not destination_root:
        return False
    review_root = os.path.join(os.path.abspath(destination_root), "REVIEW")
    try:
        return os.path.commonpath([os.path.abspath(dst), review_root]) == review_root
    except ValueError:
        return False


def _storage_breakdown(plan: MergePlan) -> dict[str, int]:
    breakdown = {"masters": 0, "quarantine": 0, "review": 0}
    for action in plan.actions:
        if isinstance(action, MoveMasterAction):
            target = "review" if _is_review_destination(action.dst, plan.destination_path) else "masters"
            breakdown[target] += action.size or 0
        elif isinstance(action, MoveToQuarantineExactAction):
            breakdown["quarantine"] += action.size or 0
    return breakdown


def write_json_report(plan: MergePlan, outfile: str):
    """
    Save structured JSON summary.
    """
    os.makedirs(os.path.dirname(os.path.abspath(outfile)) or ".", exist_ok=True)
    review_files = _review_file_entries(plan)
    breakdown = _storage_breakdown(plan)
    report = {
        "schema_version": "1.0",
        "required_space": plan.required_space,
        "destination_free": plan.destination_free,
        "destination_path": plan.destination_path,
        "actions": plan.actions,
        "duplicate_count": plan.duplicate_count,
        "total_files": plan.total_files,
        "skipped_files": plan.skipped_files,
        "review_files": review_files,
        "required_breakdown": breakdown,
    }
    with open(outfile, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, cls=EnhancedJSONEncoder)


def _source_manifest_entries(plan: MergePlan) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for action in plan.actions:
        if not isinstance(action, (MoveMasterAction, MoveToQuarantineExactAction)):
            continue
        src_path = os.path.abspath(action.src)
        dst_path = os.path.abspath(action.dst)
        entries.append(
            {
                "original_path": src_path,
                "original_folder": os.path.dirname(src_path),
                "new_path": dst_path,
                "hash": action.sha256,
            }
        )
    entries.sort(key=lambda entry: (entry["original_path"], entry["new_path"]))
    return entries


def _manifest_batch_id(entries: list[dict[str, Any]]) -> str:
    seed = "\n".join(
        f"{entry['original_path']}->{entry['new_path']}:{entry.get('hash') or ''}"
        for entry in entries
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12]


def write_source_manifest(plan: MergePlan, json_path: str, csv_path: str, html_path: str) -> None:
    entries = _source_manifest_entries(plan)
    batch_id = _manifest_batch_id(entries)
    for entry in entries:
        entry["batch_id"] = batch_id

    os.makedirs(os.path.dirname(json_path) or ".", exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"schema_version": "1.0", "batch_id": batch_id, "entries": entries},
            handle,
            indent=2,
        )

    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    with open(csv_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["batch_id", "original_path", "original_folder", "new_path", "hash"],
        )
        writer.writeheader()
        writer.writerows(entries)

    os.makedirs(os.path.dirname(html_path) or ".", exist_ok=True)
    rows = []
    for entry in entries:
        rows.append(
            "<tr>"
            f"<td>{_text(entry['original_path'])}</td>"
            f"<td>{_text(entry['original_folder'])}</td>"
            f"<td>{_text(entry['new_path'])}</td>"
            f"<td>{_text(entry.get('hash') or '')}</td>"
            "</tr>"
        )
    html_doc = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        '<meta charset="UTF-8" />',
        "<title>Nolossia Source Manifest</title>",
        "<style>"
        "body{font-family:system-ui,-apple-system,sans-serif;margin:0;padding:24px;background:#f8fafc;color:#0f172a;}"
        "h1{margin:0 0 6px;font-size:26px;color:#0f172a;}"
        ".meta{color:#64748b;font-size:13px;margin-bottom:16px;}"
        "table{width:100%;border-collapse:collapse;background:#fff;border-radius:10px;overflow:hidden;}"
        "th,td{text-align:left;padding:10px;border-bottom:1px solid #e2e8f0;font-size:13px;word-break:break-all;}"
        "th{background:#f1f5f9;color:#475569;text-transform:uppercase;letter-spacing:0.04em;font-size:12px;}"
        "</style>",
        "</head>",
        "<body>",
        "<h1>Nolossia Source Manifest</h1>",
        f"<div class='meta'>Batch: {batch_id} • Entries: {len(entries)}</div>",
        "<table>",
        "<tr><th>Original path</th><th>Original folder</th><th>New path</th><th>SHA256</th></tr>",
    ]
    if rows:
        html_doc.extend(rows)
    else:
        html_doc.append("<tr><td colspan='4'>No entries</td></tr>")
    html_doc.extend(["</table>", "</body>", "</html>"])
    with open(html_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(html_doc))


def write_undo_manifest(summary: dict, outfile: str) -> None:
    """
    Save structured undo manifest JSON for audit trail.
    """
    payload = {
        "schema_version": "1.0",
        "operation_id": summary.get("operation_id"),
        "mode": summary.get("mode"),
        "generated_at": summary.get("generated_at"),
        "counts": summary.get("counts", {}),
        "library_root": summary.get("library_root"),
        "conflict_root": summary.get("conflict_root"),
        "entries": summary.get("entries", []),
    }
    os.makedirs(os.path.dirname(os.path.abspath(outfile)) or ".", exist_ok=True)
    with open(outfile, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def write_undo_report(summary: dict, outfile: str) -> None:
    """
    Generate an undo HTML report mirroring merge reporting.
    """
    entries = summary.get("entries", [])
    counts = summary.get("counts", {})
    operation_id = summary.get("operation_id") or "unknown"
    mode = (summary.get("mode") or "execute").upper()
    conflict_root = summary.get("conflict_root")
    total_entries = counts.get("total", len(entries))
    restored = counts.get("restore", 0)
    conflicts = counts.get("conflict", 0)
    skipped = counts.get("skipped", 0)

    os.makedirs(os.path.dirname(os.path.abspath(outfile)) or ".", exist_ok=True)
    with open(outfile, "w", encoding="utf-8") as handle:
        html_doc = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            '<meta charset="UTF-8" />',
            "<title>Nolossia Undo Report</title>",
            "<style>",
            "body{font-family:system-ui,-apple-system,sans-serif;margin:0;padding:24px;background:#f8fafc;color:#0f172a;}",
            "h1{margin:0 0 6px;font-size:26px;color:#0f172a;}",
            ".meta{color:#64748b;font-size:13px;margin-bottom:16px;}",
            ".summary{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:18px;}",
            ".pill{background:#e2e8f0;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;}",
            "table{width:100%;border-collapse:collapse;background:#fff;border-radius:10px;overflow:hidden;}",
            "th,td{text-align:left;padding:10px;border-bottom:1px solid #e2e8f0;font-size:13px;word-break:break-all;}",
            "th{background:#f1f5f9;color:#475569;text-transform:uppercase;letter-spacing:0.04em;font-size:12px;}",
            ".note{color:#64748b;font-size:13px;margin-top:12px;}",
            "</style>",
            "</head>",
            "<body>",
            "<h1>Nolossia Undo Report</h1>",
            f"<div class='meta'>Operation: { _text(operation_id) } • Mode: { _text(mode) }</div>",
            "<div class='summary'>",
            f"<span class='pill'>Entries: {total_entries}</span>",
            f"<span class='pill'>Restored: {restored}</span>",
            f"<span class='pill'>Conflicts: {conflicts}</span>",
            f"<span class='pill'>Skipped: {skipped}</span>",
            "</div>",
            "<table>",
            "<tr><th>Status</th><th>Original path</th><th>New path</th><th>Target path</th><th>SHA256</th><th>Notes</th></tr>",
        ]
        if entries:
            for entry in entries:
                html_doc.append(
                    "<tr>"
                    f"<td>{_text(str(entry.get('status') or ''))}</td>"
                    f"<td>{_text(str(entry.get('original_path') or ''))}</td>"
                    f"<td>{_text(str(entry.get('new_path') or ''))}</td>"
                    f"<td>{_text(str(entry.get('target_path') or ''))}</td>"
                    f"<td>{_text(str(entry.get('hash') or ''))}</td>"
                    f"<td>{_text(str(entry.get('reason') or ''))}</td>"
                    "</tr>"
                )
        else:
            html_doc.append("<tr><td colspan='6'>No entries</td></tr>")
        html_doc.extend(["</table>"])
        if conflicts and conflict_root:
            html_doc.append(
                f"<div class='note'>Conflicts routed to {_text(conflict_root)}. "
                "Manual restore needed for conflicted items.</div>"
            )
        html_doc.extend(["</body>", "</html>"])
        handle.write("\n".join(html_doc))


def _review_file_entries(plan: MergePlan) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for action in plan.actions:
        if not isinstance(action, MoveMasterAction):
            continue
        if not _is_review_destination(action.dst, plan.destination_path):
            continue
        destination_root = os.path.abspath(plan.destination_path)
        relative = os.path.relpath(os.path.abspath(action.dst), destination_root)
        reason_code = getattr(action, "review_reason", None)
        entries.append(
            {
                "source": os.path.abspath(action.src),
                "destination": os.path.abspath(action.dst),
                "relative_destination": relative,
                "size": action.size,
                "sha256": action.sha256,
                "reason_code": reason_code,
                "reason": describe_review_reason(reason_code),
            }
        )
    return entries


def write_dedupe_report(files: List[FileInfo], clusters: List[DuplicateCluster], outfile: str, unique_size: int):
    """
    Generate a full dedupe HTML report summarizing masters and duplicate clusters.

    Args:
        files: All FileInfo objects from scan/dedupe.
        clusters: DuplicateCluster objects with master and redundant assignments.
        outfile: Destination HTML path.
        unique_size: Total size of master files.

    Returns:
        None
    """
    total_photos = len(files)
    total_size = sum(f.size for f in files)

    def _cluster_sort_key(cluster: DuplicateCluster) -> tuple[str, str]:
        master = cluster.master or (cluster.files[0] if cluster.files else None)
        if not master:
            return ("", "")
        base = os.path.abspath(master.path)
        return (os.path.dirname(base), os.path.basename(base))

    exact_pool = [c for c in clusters if len(c.redundant) > 0 and len({f.sha256 for f in c.files}) == 1]
    exact_clusters = sorted(exact_pool, key=_cluster_sort_key)
    near_clusters = sorted(
        [c for c in clusters if c not in exact_pool and c.redundant],
        key=_near_cluster_priority,
    )
    near_total = len(near_clusters)
    display_near_clusters = near_clusters[:NEAR_DUP_CLUSTER_LIMIT]

    masters = []
    master_paths = set()
    clustered_paths = {f.path for c in clusters for f in c.files}
    for cluster in clusters:
        master = cluster.master or (cluster.files[0] if cluster.files else None)
        if master and master.path not in master_paths:
            masters.append(master)
            master_paths.add(master.path)
    for f in files:
        if f.path not in master_paths and f.path not in clustered_paths:
            masters.append(f)
            master_paths.add(f.path)

    format_counts: dict[str, int] = {}
    masters.sort(key=lambda info: os.path.abspath(info.path))
    for m in masters:
        fmt = (m.format or "").lower()
        format_counts[fmt] = format_counts.get(fmt, 0) + 1
    raw_count = sum(1 for m in masters if getattr(m, "is_raw", False))

    def thumb(path: str) -> str:
        return _file_uri(path)

    def res_text(info) -> str:
        if getattr(info, "resolution", None):
            w, h = info.resolution
            return f"{w}×{h}"
        return "Unknown"

    def fmt_text(info) -> str:
        fmt = getattr(info, "format", None)
        return (fmt or "Unknown").upper()

    def name_text(info) -> str:
        return os.path.basename(getattr(info, "path", "")) or "(unknown)"

    def phash_text(info) -> str:
        phash_value = getattr(info, "phash", None)
        return phash_value if phash_value else "SKIPPED (visual match unavailable)"

    os.makedirs(os.path.dirname(os.path.abspath(outfile)) or ".", exist_ok=True)
    with open(outfile, "w", encoding="utf-8") as handle:
        html = _HtmlBuffer(handle)
        try:
            html.append("<!DOCTYPE html>")
            html.append("<html>")
            html.append("<head>")
            html.append('<meta charset="UTF-8" />')
            html.append("<title>Nolossia Dedupe Report</title>")
            html.append("<style>")
            html.append(
                ":root {"
                "  --primary: #25a4c4;"
                "  --accent: #3b82f6;"
                "  --ok: #6ad69b;"
                "  --error: #d05b6d;"
                "  --warn: #f5c542;"
                "  --link: #3b82f6;"
                "  --muted: #64748b;"
                "  --bg: #f9fafb;"
                "  --panel: #ffffff;"
                "  --border: #e0e7f1;"
                "  --pill: rgba(37, 164, 196, 0.14);"
                "}"
            )
            html.append(
                "body { font-family: 'Inter', system-ui, -apple-system, sans-serif; margin: 0; padding: 28px; "
                "background: var(--bg); color: #0f172a; font-size: 18px; }"
            )
            html.append("h1, h2, h3 { margin: 0 0 12px 0; color: var(--accent); }")
            html.append("h1 { letter-spacing: 0.01em; }")
            html.append(".grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; }")
            html.append(
                ".card { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 14px; "
                "box-shadow: 0 8px 20px rgba(15,23,42,0.08); }"
            )
            html.append(
                ".pill { display: inline-block; padding: 4px 8px; border-radius: 999px; font-size: 13px; background: var(--pill); "
                "color: var(--primary); margin-right: 6px; letter-spacing: 0.02em; }"
            )
            html.append(".section { margin-top: 28px; }")
            html.append(
                ".tile { background: #ffffff; border: 1px solid var(--border); border-radius: 10px; padding: 12px; "
                "display: grid; grid-template-columns: 180px 1fr; gap: 12px; margin-bottom: 12px; "
                "box-shadow: 0 6px 14px rgba(15,23,42,0.06); }"
            )
            html.append(
                ".tile img { width: 168px; height: 168px; object-fit: cover; border-radius: 8px; "
                "background: #eef2f7; border: 1px solid var(--border); }"
            )
            html.append(
                ".meta-grid { display:grid; grid-template-columns:auto 1fr; gap:2px 8px; font-size:14px; }"
            )
            html.append(".meta-label { color: var(--muted); }")
            html.append(".meta-value { font-weight:500; }")
            html.append(".path { font-size: 13px; color: var(--muted); word-break: break-all; }")
            html.append(
                ".tag { display: inline-block; padding: 4px 8px; border-radius: 8px; font-size: 12px; "
                "background: var(--primary); color: #0b1120; margin-left: 6px; font-weight: 700; }"
            )
            html.append(".controls { display: flex; flex-direction: column; align-items: stretch; gap: 6px; margin-top: 8px; }")
            html.append(
                ".btn { border: 1px solid var(--border); padding: 7px 11px; border-radius: 8px; cursor: pointer; "
                "font-size: 13px; transition: all 0.15s ease; font-weight: 600; display: flex; align-items: center; gap: 6px; "
                "width: 100%; justify-content: flex-start; }"
            )
            html.append(
                ".btn-master { background: rgba(106, 214, 155, 0.18); color: #0f5132; border-color: rgba(106, 214, 155, 0.5); }"
            )
            html.append(".btn-master:hover { background: rgba(106, 214, 155, 0.28); }")
            html.append(
                ".btn-exact { background: rgba(208, 91, 109, 0.15); color: #7f1d1d; border-color: rgba(208, 91, 109, 0.45); }"
            )
            html.append(".btn-exact:hover { background: rgba(208, 91, 109, 0.25); }")
            html.append(".note { font-size: 14px; color: var(--muted); margin-top: 4px; }")
            html.append(".glossary { margin: 8px 0 0 18px; font-size: 14px; color: var(--muted); }")
            html.append(".glossary li { margin: 4px 0; }")
            html.append(".glossary strong { color: #0f172a; }")
            html.append(
                ".cluster { margin-bottom: 22px; padding: 14px; border-radius: 12px; background: var(--panel); "
                "border: 1px solid var(--border); }"
            )
            html.append(".flex { display: flex; gap: 10px; align-items: center; }")
            html.append(".eyebrow { font-size: 12px; letter-spacing: 0.08em; color: var(--muted); text-transform: uppercase; }")
            html.append(".divider { height: 1px; background: linear-gradient(90deg, var(--accent) 0%, transparent 80%); border: none; margin: 18px 0; }")
            html.append("details.section-toggle { margin-top: 8px; }")
            html.append(
                "details.section-toggle > summary { list-style: none; cursor: pointer; padding: 4px 0; font-weight: 600; "
                "display: flex; align-items: center; gap: 8px; }"
            )
            html.append("details.section-toggle > summary::-webkit-details-marker { display: none; }")
            html.append(
                "details.cluster > summary { list-style: none; cursor: pointer; margin-bottom: 8px; display: flex; "
                "justify-content: space-between; align-items: baseline; }"
            )
            html.append("details.cluster > summary::-webkit-details-marker { display: none; }")
            # Arrow indicators for collapsible headers
            html.append(
                "details.section-toggle > summary::before { content: '▸'; font-size: 20px; margin-right: 6px; color: var(--accent); }"
            )
            html.append(
                "details.section-toggle[open] > summary::before { content: '▾'; }"
            )
            html.append(
                "details.cluster > summary::before { content: '▸'; font-size: 18px; margin-right: 6px; color: var(--accent); }"
            )
            html.append(
                "details.cluster[open] > summary::before { content: '▾'; }"
            )
            # Compare panel styles
            html.append(
                ".compare-panel { display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); "
                "gap: 12px; margin: 12px 0; }"
            )
            html.append(
                ".compare-slot { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 10px; }"
            )
            html.append(
                ".compare-img { width: 100%; max-height: 260px; object-fit: contain; border-radius: 8px; "
                "background: #eef2f7; border: 1px solid var(--border); }"
            )
            html.append("</style>")
            html.append("<script>")
            html.append("""
    function markIndependentMaster(button, cardId) {
        const card = document.getElementById(cardId);
        if (!card) return;
        card.dataset.status = "independent-master";
        button.textContent = "Marked as independent master";
    }
    function markClusterMaster(button, cardId) {
        const card = document.getElementById(cardId);
        if (!card) return;
        card.dataset.status = "cluster-master";
        button.textContent = "Marked as cluster master";
    }
    function markExact(button, cardId) {
        const card = document.getElementById(cardId);
        if (!card) return;
        card.dataset.status = "exact";
        button.textContent = "Marked as exact copy";
    }
    function setCompare(clusterId, src, name) {
        const img = document.getElementById("compare-img-" + clusterId);
        const label = document.getElementById("compare-label-" + clusterId);
        if (img) img.src = src;
        if (label) label.textContent = name;
    }
    """)
            html.append("</script>")
            html.append("</head>")
            html.append("<body>")

            html.append("<h1>Nolossia Dedupe Report</h1>")
            html.append("<div class='note'>Designed to prevent data loss. This report is read-only.</div>")
            html.append(
                "<div class='card' style='margin-top:10px;'>"
                f"<div><strong>{total_photos}</strong> photos analyzed, "
                f"<strong>{len(masters)}</strong> masters retained.</div>"
                f"<div class='note'>{len(exact_clusters)} exact-duplicate clusters, "
                f"{near_total} look-alike (near-duplicate) groups detected.</div>"
                "<div class='note'>This report is read-only; no files are modified.</div>"
                "</div>"
            )
            html.append(
                "<div class='card' style='margin-top:10px;'>"
                "<div><strong>Next steps (preview only — no changes yet)</strong></div>"
                "<div class='note'>Review this report, then run the merge wizard to build a preview-only plan. "
                "If the plan looks right, re-run with --execute and confirm when prompted.</div>"
                "</div>"
            )
            html.append(
                "<div class='note' style='margin-top:8px;'>"
                "<button class='btn' onclick='window.print()'>Print / Save as PDF</button> "
                "Use your browser's print dialog to print or save as PDF."
                "</div>"
            )
            html.append("<details class='section-toggle' style='margin-top:10px;'>")
            html.append("<summary><h2>Glossary</h2></summary>")
            html.append("<ul class='glossary'>")
            html.append("<li><strong>Exact duplicate</strong>: files that are identical (same content).</li>")
            html.append("<li><strong>Look-alike</strong>: photos that look the same but are not identical.</li>")
            html.append("<li><strong title='Compares photos visually to find look-alikes (pHash).'>Visual match check</strong> (pHash): visual comparison used to find look-alikes.</li>")
            html.append("<li><strong>Set aside for review</strong> (REVIEW/): needs your decision before moving.</li>")
            html.append("<li><strong>Isolated for safety</strong> (QUARANTINE_EXACT/): exact duplicates moved so nothing is lost.</li>")
            html.append("<li><strong>Master selection</strong>: RAW > higher resolution > larger file > more EXIF > GPS > oldest when equal.</li>")
            html.append("</ul>")
            html.append("</details>")
            html.append(
                "<div class='note'>All actions in this report are for review only. Buttons do not move, delete, or modify files or clusters. "
                "Use them purely as on-page annotations to mark which files you would keep as independent masters, which should become the preferred master within a cluster, "
                "and which you would treat as exact copies.</div>"
            )
            html.append("<div class='section'>")
            html.append("<div class='grid'>")
            html.append(
                f"<div class='card'><h3>Total photos</h3><div>{total_photos}</div>"
                f"<div class='note'>{humanize_bytes(total_size)}</div></div>"
            )
            html.append(
                f"<div class='card'><h3>Total masters</h3><div>{len(masters)}</div>"
                f"<div class='note'>{humanize_bytes(unique_size)}</div></div>"
            )
            html.append(
                f"<div class='card'><h3>Total duplicates</h3><div>{len(files) - len(masters)}</div>"
                f"<div class='note'>{humanize_bytes(total_size - unique_size)}</div></div>"
            )
            missing_phash = sum(1 for f in files if not getattr(f, "phash", None))
            html.append(f"<div class='card'><h3>RAW masters</h3><div>{raw_count}</div></div>")
            html.append(
                f"<div class='card'><h3>Visual match check unavailable</h3><div>{missing_phash}</div>"
                f"<div class='note'>phash=None (incl. oversize guards)</div></div>"
            )
            html.append("</div>")
            html.append("</div>")

            html.append("<div class='section'>")
            html.append("<details class='section-toggle' open>")
            html.append("<summary><h2>Masters overview</h2></summary>")
            html.append("<div class='grid'>")
            for fmt, count in sorted(format_counts.items()):
                label = fmt.upper() if fmt else "UNKNOWN"
                html.append(
                    "<div class='card'>"
                    f"<div>{_text(label)}</div>"
                    f"<div class='note'>{count} files</div>"
                    "</div>"
                )
            html.append("</div>")
            html.append("</details>")
            html.append("</div>")

            html.append("<div class='section'>")
            html.append("<details class='section-toggle' open>")
            html.append("<summary><h2>Exact copies</h2></summary>")
            if not exact_clusters:
                html.append("<div class='note'>No exact duplicates detected.</div>")
            else:
                for cluster in exact_clusters:
                    master = cluster.master or (cluster.files[0] if cluster.files else None)
                    if not master:
                        continue
                    html.append("<details class='cluster' open>")
                    html.append(
                        f"<summary><h3>Exact cluster</h3>"
                        f"<span class='note'>{len(cluster.redundant)} exact copy(ies)</span></summary>"
                    )
                    html.append("<div>")
                    html.append("<div class='flex'>")
                    html.append(
                        f"<img src='{_attr(thumb(master.path))}' alt='master' width='168' height='168' "
                        "style='object-fit:cover;border-radius:8px;'/>"
                    )
                    reason_text = _selection_reason_text(master)
                    reason_line = (
                        f"<div class='meta-label'>Kept because</div><div class='meta-value'>{_text(reason_text)}</div>"
                        if reason_text
                        else ""
                    )
                    html.append(
                        f"<div><div><strong>{_text(name_text(master))} (MASTER)</strong></div>"
                        f"<div class='meta-grid'>"
                        f"<div class='meta-label'>Format</div><div class='meta-value'>{_text(fmt_text(master))}</div>"
                        f"<div class='meta-label'>Size</div><div class='meta-value'>{humanize_bytes(master.size)}</div>"
                        f"<div class='meta-label'>Resolution</div><div class='meta-value'>{_text(res_text(master))}</div>"
                        f"<div class='meta-label' title='Compares photos visually to find look-alikes (pHash).'>Visual match check</div><div class='meta-value'>{_text(phash_text(master))}</div>"
                        f"{reason_line}"
                        f"</div></div>"
                    )
                    html.append("</div>")
                    for idx, dup in enumerate(sorted(cluster.redundant, key=lambda info: os.path.abspath(info.path)), 1):
                        card_id = _stable_id("exact", os.path.abspath(dup.path))
                        html.append(f"<div class='tile' id='{_attr(card_id)}'>")
                        html.append(f"<img src='{_attr(thumb(dup.path))}' alt='duplicate' />")
                        html.append("<div>")
                        html.append(f"<div><strong>{_text(name_text(dup))}</strong></div>")
                        html.append("<div class='meta-grid'>")
                        html.append(
                            f"<div class='meta-label'>Format</div><div class='meta-value'>{_text(fmt_text(dup))}</div>"
                        )
                        html.append(
                            f"<div class='meta-label'>Size</div><div class='meta-value'>{humanize_bytes(dup.size)}</div>"
                        )
                        html.append(
                            f"<div class='meta-label'>Resolution</div><div class='meta-value'>{_text(res_text(dup))}</div>"
                        )
                        html.append(
                            f"<div class='meta-label' title='Compares photos visually to find look-alikes (pHash).'>Visual match check</div><div class='meta-value'>{_text(phash_text(dup))}</div>"
                        )
                        html.append("</div>")
                        html.append("</div>")
                        html.append("</div>")
                    html.append("</div>")  # close inner content div
                    html.append("</details>")  # close exact cluster details
            html.append("</details>")  # close Exact copies section details
            html.append("</div>")

            html.append("<div class='section'>")
            html.append("<details class='section-toggle' open>")
            html.append("<summary><h2>Look-alike groups</h2></summary>")
            if not near_clusters:
                html.append("<div class='note'>No look-alike candidates detected.</div>")
            else:
                if near_total > NEAR_DUP_CLUSTER_LIMIT:
                    html.append(
                        f"<div class='note'>Showing top {NEAR_DUP_CLUSTER_LIMIT} of {near_total} look-alike "
                        "groups ranked by confidence, resolution, and date.</div>"
                    )
                else:
                    html.append("<div class='note'>Clusters ranked by confidence, resolution, and date.</div>")
                cluster_counter = 1
                for cluster in display_near_clusters:
                    master = cluster.master or (cluster.files[0] if cluster.files else None)
                    if not master:
                        continue
                    cluster_id = f"c{cluster_counter}"
                    html.append("<details class='cluster' open>")
                    html.append(
                        f"<summary><h3>Cluster {cluster_counter}</h3>"
                        f"<span class='note'>{len(cluster.redundant)} look-alike candidate(s)</span></summary>"
                    )
                    cluster_counter += 1
                    html.append("<div>")
                    html.append("<div class='flex'>")
                    html.append(
                        f"<img src='{_attr(thumb(master.path))}' alt='master' width='168' height='168' "
                        "style='object-fit:cover;border-radius:10px;'/>"
                    )
                    reason_text = _selection_reason_text(master)
                    reason_line = (
                        f"<div class='meta-label'>Kept because</div><div class='meta-value'>{_text(reason_text)}</div>"
                        if reason_text
                        else ""
                    )
                    html.append(
                        f"<div><div><strong>{_text(name_text(master))}</strong> <span class='tag'>MASTER</span></div>"
                        f"<div class='meta-grid'>"
                        f"<div class='meta-label'>Format</div><div class='meta-value'>{_text(fmt_text(master))}</div>"
                        f"<div class='meta-label'>Size</div><div class='meta-value'>{humanize_bytes(master.size)}</div>"
                        f"<div class='meta-label'>Resolution</div><div class='meta-value'>{_text(res_text(master))}</div>"
                        f"<div class='meta-label' title='Compares photos visually to find look-alikes (pHash).'>Visual match check</div><div class='meta-value'>{_text(phash_text(master))}</div>"
                        f"{reason_line}"
                        f"</div></div>"
                    )
                    html.append("</div>")
                    # Compare panel for this cluster
                    html.append("<div class='compare-panel'>")
                    html.append("<div class='compare-slot'>")
                    html.append("<div class='note'>Master</div>")
                    html.append(
                        f"<img src='{_attr(thumb(master.path))}' alt='master compare' class='compare-img' />"
                    )
                    html.append("</div>")
                    html.append("<div class='compare-slot'>")
                    html.append(f"<div class='note' id='compare-label-{_attr(cluster_id)}'>Candidate</div>")
                    html.append(
                        f"<img src='{_attr(thumb(master.path))}' alt='candidate compare' class='compare-img' id='compare-img-{_attr(cluster_id)}' />"
                    )
                    html.append("</div>")
                    html.append("</div>")
                    html.append("<div class='note'>Click any candidate below to load it into the compare panel.</div>")
                    candidate_total = len(cluster.redundant)
                    sorted_candidates = sorted(
                        cluster.redundant,
                        key=lambda info: _near_candidate_priority(master, info),
                    )
                    display_candidates = sorted_candidates[:NEAR_DUP_CANDIDATE_LIMIT]
                    if candidate_total > NEAR_DUP_CANDIDATE_LIMIT:
                        html.append(
                            f"<div class='note'>Showing top {NEAR_DUP_CANDIDATE_LIMIT} of {candidate_total} "
                            "candidates sorted by confidence, resolution, and date.</div>"
                        )
                    for idx, item in enumerate(display_candidates, 1):
                        card_id = _stable_id("near", os.path.abspath(item.path))
                        html.append(f"<div class='tile' id='{_attr(card_id)}'>")
                        onclick_value = (
                            f"setCompare({_js_arg(cluster_id)}, {_js_arg(thumb(item.path))}, {_js_arg(name_text(item))})"
                        )
                        html.append(
                            f"<img src='{_attr(thumb(item.path))}' alt='look-alike' "
                            f"onclick=\"{_attr(onclick_value)}\" />"
                        )
                        html.append("<div>")
                        html.append(f"<div><strong>{_text(name_text(item))}</strong></div>")
                        html.append("<div class='meta-grid'>")
                        html.append(
                            f"<div class='meta-label'>Format</div><div class='meta-value'>{_text(fmt_text(item))}</div>"
                        )
                        html.append(
                            f"<div class='meta-label'>Size</div><div class='meta-value'>{humanize_bytes(item.size)}</div>"
                        )
                        html.append(
                            f"<div class='meta-label'>Resolution</div><div class='meta-value'>{_text(res_text(item))}</div>"
                        )
                        html.append(
                            f"<div class='meta-label' title='Compares photos visually to find look-alikes (pHash).'>Visual match check</div><div class='meta-value'>{_text(phash_text(item))}</div>"
                        )
                        html.append("</div>")
                        html.append("<div class='controls'>")
                        html.append(
                            f"<button class='btn btn-master' onclick=\"markIndependentMaster(this, '{_attr(card_id)}')\">Mark as independent master</button>"
                        )
                        html.append(
                            f"<button class='btn btn-master' onclick=\"markClusterMaster(this, '{_attr(card_id)}')\">Promote to cluster master</button>"
                        )
                        html.append(
                            f"<button class='btn btn-exact' onclick=\"markExact(this, '{_attr(card_id)}')\">Mark as exact copy</button>"
                        )
                        html.append("</div>")
                        html.append("</div>")
                        html.append("</div>")
                    html.append("</div>")  # close inner content div
                    html.append("</details>")  # close near-duplicate cluster details
            html.append("</details>")  # close Near-duplicate clusters section details
            html.append("</div>")

            html.append("</body></html>")
        finally:
            html.close()


def write_merge_report(plan: MergePlan, outfile: str, mode_label: str | None = None):
    """
    Generate a merge execution HTML summary.
    """
    os.makedirs(os.path.dirname(os.path.abspath(outfile)) or ".", exist_ok=True)
    doc_path = os.path.abspath(os.path.join(os.getcwd(), "docs", "specs", "CLI_COMMANDS.md"))
    doc_href = f"file://{quote(doc_path)}"
    mode_text = ""
    if mode_label:
        normalized = mode_label.strip().upper()
        if normalized == "PREVIEW":
            mode_text = "Preview only — no changes yet"
        elif normalized == "EXECUTE":
            mode_text = "Execute — files moved"
        else:
            mode_text = mode_label.strip()
    masters = sorted(
        (a for a in plan.actions if isinstance(a, MoveMasterAction)),
        key=lambda action: (os.path.dirname(action.dst), os.path.basename(action.dst)),
    )
    quarantine = sorted(
        (a for a in plan.actions if isinstance(a, MoveToQuarantineExactAction)),
        key=lambda action: (os.path.dirname(action.dst), os.path.basename(action.dst)),
    )
    near_duplicates = sorted(
        (a for a in plan.actions if isinstance(a, MarkNearDuplicateAction)),
        key=lambda action: (
            os.path.abspath(action.master or ""),
            os.path.basename(action.src),
        ),
    )

    review_actions = [
        action for action in masters if _is_review_destination(action.dst, plan.destination_path)
    ]
    master_size = sum(a.size or 0 for a in masters)
    quarantine_size = sum(a.size or 0 for a in quarantine)
    near_size = sum(a.size or 0 for a in near_duplicates)
    review_size = sum(a.size or 0 for a in review_actions)
    breakdown = _storage_breakdown(plan)

    def _rows(actions: list[MoveMasterAction | MoveToQuarantineExactAction]) -> str:
        if not actions:
            return "<tr><td colspan='3'>None</td></tr>"
        rows = []
        for action in actions:
            src_name = _text(os.path.basename(action.src))
            dst_name = _text(os.path.basename(action.dst))
            rows.append(
                "<tr>"
                f"<td>{src_name}</td>"
                f"<td>{dst_name}</td>"
                f"<td>{humanize_bytes(action.size or 0)}</td>"
                "</tr>"
            )
        return "\n".join(rows)

    os.makedirs(os.path.dirname(os.path.abspath(outfile)) or ".", exist_ok=True)
    with open(outfile, "w", encoding="utf-8") as handle:
        html = _HtmlBuffer(handle)
        try:
            html.extend([
                "<!DOCTYPE html>",
                "<html>",
                "<head>",
                '<meta charset="UTF-8" />',
                "<title>Nolossia Merge Report</title>",
                "<style>"
                "body{font-family:system-ui,-apple-system,sans-serif;margin:0;padding:24px;background:#f4f6fb;color:#0f172a;}"
                "h1{margin:0 0 12px;font-size:28px;color:#1d4ed8;}"
                "h2{margin-top:24px;color:#1d4ed8;}"
                ".summary{display:flex;gap:18px;flex-wrap:wrap;margin-bottom:16px;}"
                ".card{background:#fff;border-radius:12px;padding:16px;box-shadow:0 6px 18px rgba(15,23,42,0.1);min-width:220px;}"
                ".metric{font-size:14px;color:#64748b;margin:0;}"
                ".value{font-size:22px;font-weight:600;margin:4px 0 0;}"
                ".meta{color:#64748b;font-size:13px;margin-bottom:12px;}"
                ".badge{display:inline-block;font-size:12px;font-weight:600;padding:4px 10px;border-radius:999px;"
                "margin-left:8px;vertical-align:middle;}"
                ".badge-raw{background:#e0f2fe;color:#075985;border:1px solid #bae6fd;}"
                "table{width:100%;border-collapse:collapse;margin-top:12px;}"
                "th,td{text-align:left;padding:8px;border-bottom:1px solid #e2e8f0;font-size:14px;}"
                "th{font-size:13px;text-transform:uppercase;letter-spacing:0.05em;color:#475569;}"
                ".glossary{margin:8px 0 0 18px;font-size:14px;color:#64748b;}"
                ".glossary li{margin:4px 0;}"
                ".glossary strong{color:#0f172a;}"
                "details.section-toggle > summary{list-style:none;cursor:pointer;font-weight:600;display:flex;align-items:center;gap:8px;}"
                "details.section-toggle > summary::-webkit-details-marker{display:none;}"
                "details.section-toggle > summary::before{content:'▸';font-size:18px;margin-right:6px;color:#1d4ed8;}"
                "details.section-toggle[open] > summary::before{content:'▾';}"
                "</style>",
                "</head>",
                "<body>",
                "<h1>Nolossia Merge Report"
                " <span class='badge badge-raw'>RAW + sidecars kept intact</span></h1>",
                f"<div class='meta'>Docs alias: /docs/cli · <a href='{doc_href}'>CLI_COMMANDS.md</a></div>",
                (f"<div class='meta'>Mode: {mode_text}</div>" if mode_text else ""),
                "<div class='note'>Designed to prevent data loss. Review this report before you execute.</div>",
                "<div class='card' style='margin-top:10px;'>"
                f"<div><strong>{len(masters)}</strong> masters planned, "
                f"<strong>{len(quarantine)}</strong> exact duplicates isolated for safety, "
                f"<strong>{len(near_duplicates)}</strong> look-alikes (near-duplicates) flagged.</div>"
                f"<div class='note'>Set aside for review: {len(review_actions)} items. "
                f"Required space: {humanize_bytes(plan.required_space)}.</div>"
                "<div class='note'>This report summarizes the latest merge plan and outcomes.</div>"
                "</div>",
                "<div class='card' style='margin-top:10px;'>"
                "<div><strong>RAW + sidecar confirmation</strong></div>"
                "<div class='note'>RAW files stay intact. Sidecar files (for example .xmp) are not processed yet, "
                "so keep them next to RAW files and copy them after the merge.</div>"
                "</div>",
                "<div class='note' style='margin-top:8px;'>"
                "<button class='card' style='display:inline-block;padding:8px 12px;border:1px solid #e2e8f0;"
                "border-radius:10px;background:#fff;cursor:pointer;' "
                "onclick='window.print()'>Print / Save as PDF</button> "
                "Use your browser's print dialog to print or save as PDF. "
                "Note: PDF link clickability depends on your PDF viewer."
                "</div>",
                "<div class='note'>Docs alias: /docs/cli (destination requirements).</div>",
                "<details class='section-toggle' style='margin-top:12px;'>",
                "<summary><h2>Glossary</h2></summary>",
                "<ul class='glossary'>",
                "<li><strong>Exact duplicate</strong>: files that are identical (same content).</li>",
                "<li><strong>Look-alike</strong>: photos that look the same but are not identical.</li>",
                "<li><strong title='Compares photos visually to find look-alikes (pHash).'>Visual match check</strong> (pHash): visual comparison used to find look-alikes.</li>",
                "<li><strong>Set aside for review</strong> (REVIEW/): needs your decision before moving.</li>",
                "<li><strong>Isolated for safety</strong> (QUARANTINE_EXACT/): exact duplicates moved so nothing is lost.</li>",
                "<li><strong>Master selection</strong>: RAW > higher resolution > larger file > more EXIF > GPS > oldest when equal.</li>",
                "</ul>",
                "</details>",
                "<div class='card' style='margin-top:12px;'>"
                "<div><strong>Guidance</strong></div>"
                "<div class='note'>RAW sidecars (for example .xmp) are not processed yet. "
                "Keep them next to RAW files and copy them after the merge.</div>"
                f"<div class='note'>Docs: <a href='{doc_href}'>CLI_COMMANDS.md</a> "
                "(destination requirements).</div>"
                "<div class='note'>Compliance reminder: keep logs and reports until review is complete.</div>"
                "<div class='note'>Large datasets (L): run on local disks and split inputs if the system slows down.</div>"
                "<div class='note'>Apple Photos: export unmodified originals to a local folder before running Nolossia. "
                "Screenshot reference: docs/research/reports/apple_photos_export_reference.md.</div>"
                "</div>",
                "<div class='summary'>",
                f"<div class='card'><p class='metric'>Masters moved</p><p class='value'>{len(masters)} • {humanize_bytes(master_size)}</p></div>",
                f"<div class='card'><p class='metric'>Isolated for safety</p><p class='value'>{len(quarantine)} • {humanize_bytes(quarantine_size)}</p></div>",
                f"<div class='card'><p class='metric'>Look-alikes (marked)</p><p class='value'>{len(near_duplicates)} • {humanize_bytes(near_size)}</p></div>",
                f"<div class='card'><p class='metric'>Set aside for review</p><p class='value'>{len(review_actions)} • {humanize_bytes(review_size)}</p></div>",
                f"<div class='card'><p class='metric'>Planned total</p><p class='value'>{plan.total_files} files</p></div>",
                f"<div class='card'><p class='metric'>Storage breakdown</p>"
                f"<p class='value'>{humanize_bytes(plan.required_space)}</p>"
                f"<p class='metric'>Masters • {humanize_bytes(breakdown['masters'])}</p>"
                f"<p class='metric'>Isolated for safety • {humanize_bytes(breakdown['quarantine'])}</p>"
                f"<p class='metric'>Set aside for review • {humanize_bytes(breakdown['review'])}</p></div>",
                f"<div class='card'><p class='metric'>Skipped files (logged)</p><p class='value'>{plan.skipped_files}</p><p class='metric'>See nolossia.log for details.</p></div>",
                "</div>",
                "<h2>Master files moved</h2>",
                "<table>",
                "<tr><th>Source</th><th>Destination</th><th>Size</th></tr>",
                _rows(masters),
                "</table>",
                "<h2>Duplicates isolated for safety</h2>",
                "<table>",
                "<tr><th>Source</th><th>Safety folder path</th><th>Size</th></tr>",
                _rows(quarantine),
                "</table>",
            ])

            html.append("<h2>Set aside for review (date needs confirmation)</h2>")
            if review_actions:
                html.append("<table>")
                html.append("<tr><th>Source</th><th>Review destination</th><th>Reason</th><th>Size</th></tr>")
                destination_root = os.path.abspath(plan.destination_path)
                for action in review_actions:
                    reason_text = describe_review_reason(getattr(action, "review_reason", None))
                    html.append(
                        "<tr>"
                        f"<td>{_text(os.path.basename(action.src))}</td>"
                        f"<td>{_text(os.path.relpath(action.dst, destination_root))}</td>"
                        f"<td>{_text(reason_text)}</td>"
                        f"<td>{humanize_bytes(action.size or 0)}</td>"
                        "</tr>"
                    )
                html.append("</table>")
            else:
                html.append("<p>No files were set aside for review during this merge.</p>")

            html.append("<h2>Look-alikes (manual review)</h2>")
            if near_duplicates:
                html.append("<table>")
                html.append("<tr><th>File</th><th>Master reference</th><th>Size</th></tr>")
                for action in near_duplicates:
                    html.append(
                        "<tr>"
                        f"<td>{os.path.basename(action.src)}</td>"
                        f"<td>{os.path.basename(action.master)}</td>"
                        f"<td>{humanize_bytes(action.size or 0)}</td>"
                        "</tr>"
                    )
                html.append("</table>")
            else:
                html.append("<p>No look-alikes were marked in this merge.</p>")

            html.append("</body>")
            html.append("</html>")
        finally:
            html.close()
