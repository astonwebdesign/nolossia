"""
Module: merge_engine
Purpose: Build and execute merge plans with safety checks.
"""

import json
import os
import shutil
from datetime import datetime
from typing import Callable, List, Set, Tuple, Union

from .duplicates import select_master
from .exceptions import MergeExecutionError, MergePlanError, StorageError, UndoInputError, UndoSafetyError
from .hashing import compute_sha256
from .models.actions import (
    CreateFolderAction,
    MarkNearDuplicateAction,
    MergeAction,
    MoveMasterAction,
    MoveToQuarantineExactAction,
)
from .models.cluster import DuplicateCluster
from .models.fileinfo import FileInfo
from .models.mergeplan import MergePlan
from .organizer import determine_target_path, ensure_structure
from . import reporting
from .reporting import write_log, write_json_report, write_merge_report, write_source_manifest
from .utils import (
    ensure_directory,
    human_readable_size,
    log_error,
    log_info,
    log_warning,
    path_violation_message,
    safe_move,
)


def build_merge_plan(
    files: List[FileInfo],
    clusters: List[DuplicateCluster],
    out_path: str,
    mode: str = "on",
    skipped_files: int = 0,
) -> MergePlan:
    """
    Calculate required space and file actions.

    Args:
        files: All FileInfo objects to merge.
        clusters: Duplicate clusters with master/redundant marked.
        out_path: Destination library path.
        mode: Organization mode ("off" preserves hierarchy, "on" uses YEAR/YEAR-MONTH).

    Returns:
        MergePlan containing actions and storage calculations.

    Raises:
        MergePlanError: When destination validation or plan creation fails.
        StorageError: When destination has insufficient space.
    """
    try:
        destination = os.path.abspath(out_path)
        _validate_destination(destination)
        effective_mode = "on"  # chronological organization is mandatory for merge

        source_root = _common_source_root(files)
        file_actions, folders = _create_file_actions(
            files, clusters, destination, effective_mode, source_root
        )
        folder_actions = _create_folder_actions(folders)
        
        actions = folder_actions + file_actions

        required_space, destination_free = _calculate_storage(actions, destination)

        if required_space > destination_free:
            log_error("Insufficient storage for merge operation")
            raise StorageError(
                f"Insufficient storage for merge operation "
                f"(required {human_readable_size(required_space)}, "
                f"available {human_readable_size(destination_free)})"
            )

        return MergePlan(
            required_space=required_space,
            destination_free=destination_free,
            duplicate_count=len(clusters),
            total_files=len(files),
            actions=actions,
            destination_path=destination,
            skipped_files=skipped_files,
        )
    except StorageError:
        raise
    except Exception as exc:
        log_error(f"Failed to build merge plan: {exc}")
        raise MergePlanError("Failed to build merge plan") from exc


def _create_file_actions(
    files: List[FileInfo],
    clusters: List[DuplicateCluster],
    destination: str,
    effective_mode: str,
    source_root: str | None,
) -> Tuple[List[MergeAction], Set[str]]:
    """Creates all file-related actions for the merge plan."""
    actions: List[MergeAction] = []
    folders = set()
    quarantine_exact_dir = os.path.join(destination, "QUARANTINE_EXACT")

    files_in_clusters = set()
    for cluster in clusters:
        master = cluster.master or select_master(cluster)
        redundant = [f for f in cluster.files if f.path != master.path]
        files_in_clusters.update(f.path for f in cluster.files)

        target_master = determine_target_path(
            master, destination, merge_mode=effective_mode, source_root=source_root
        )
        folders.add(os.path.dirname(target_master))
        actions.append(
            MoveMasterAction(
                src=os.path.abspath(master.path),
                dst=os.path.abspath(target_master),
                sha256=master.sha256,
                size=master.size,
                review_reason=getattr(master, "review_reason", None),
            )
        )

        redundant_exact_duplicates = [f for f in redundant if f.sha256 == master.sha256]
        redundant_near_duplicates = [f for f in redundant if f.sha256 != master.sha256]

        for file in redundant_exact_duplicates:
            target = os.path.join(quarantine_exact_dir, os.path.basename(file.path))
            folders.add(os.path.dirname(target))
            actions.append(
                MoveToQuarantineExactAction(
                    src=os.path.abspath(file.path),
                    dst=os.path.abspath(target),
                    sha256=file.sha256,
                    size=file.size,
                )
            )

        for file in redundant_near_duplicates:
            actions.append(
                MarkNearDuplicateAction(
                    src=os.path.abspath(file.path),
                    master=os.path.abspath(master.path),
                    sha256=file.sha256,
                    size=file.size,
                )
            )

    for file in files:
        if file.path in files_in_clusters:
            continue
        target = determine_target_path(
            file, destination, merge_mode=effective_mode, source_root=source_root
        )
        folders.add(os.path.dirname(target))
        actions.append(
            MoveMasterAction(
                src=os.path.abspath(file.path),
                dst=os.path.abspath(target),
                sha256=file.sha256,
                size=file.size,
                review_reason=getattr(file, "review_reason", None),
            )
        )
    
    return actions, folders


def _create_folder_actions(folders: Set[str]) -> List[CreateFolderAction]:
    """Creates folder creation actions."""
    return [CreateFolderAction(path=os.path.abspath(folder)) for folder in folders]


def _calculate_storage(
    actions: List[MergeAction], destination: str
) -> Tuple[int, int]:
    """Calculates required and available storage."""
    required_space = sum(
        action.size
        for action in actions
        if isinstance(action, (MoveMasterAction, MoveToQuarantineExactAction))
    )
    destination_free = _available_space(destination)
    return required_space, destination_free


def dry_run(plan: MergePlan) -> dict[str, int]:
    """
    Produce a dry-run summary; no filesystem changes or console output.

    Args:
        plan: MergePlan to simulate.

    Returns:
        Dictionary mapping action types to planned counts.
    """
    entries = [f"[INFO] Dry run - action count: {len(plan.actions)}"]
    action_summary: dict[str, int] = {}
    for action in plan.actions:
        action_summary[action.type] = action_summary.get(action.type, 0) + 1

    for action_type, count in action_summary.items():
        entries.append(f"[INFO] {action_type}: {count}")
    write_log(entries)
    return action_summary


def execute_merge(plan: MergePlan) -> dict[str, list[tuple[str, str]]]:
    """
    Perform real merge with safety checks.

    Args:
        plan: MergePlan to execute.

    Returns:
        Metadata about execution (e.g., renamed paths).

    Raises:
        MergeExecutionError: If any action fails or validation fails.
    """
    merge_report_path = reporting.artifact_path("merge_report.html")
    dedupe_report_path = reporting.artifact_path("dedupe_report.html")
    manifest_json_path = reporting.artifact_path("source_manifest.json")
    manifest_csv_path = reporting.artifact_path("source_manifest.csv")
    manifest_html_path = reporting.artifact_path("source_manifest.html")
    renamed_paths: list[tuple[str, str]] = []
    try:
        # Persist merge plan report before mutating filesystem
        plan_report_path = reporting.artifact_path("merge_plan.json")
        write_json_report(plan, plan_report_path)

        folder_targets = [
            os.path.abspath(action.path)
            for action in plan.actions
            if isinstance(action, CreateFolderAction)
        ]
        destination_root = plan.destination_path or _infer_destination_root(plan)
        if destination_root:
            ensure_structure(destination_root, "on", folders=folder_targets)

        base_dirs = {
            os.path.abspath(_get_parent(a.path) or "")
            for a in plan.actions
            if isinstance(a, CreateFolderAction)
        }
        for base in base_dirs:
            if base:
                ensure_directory(base)
        for action in plan.actions:
            if isinstance(action, CreateFolderAction):
                ensure_directory(action.path)
            elif isinstance(action, (MoveMasterAction, MoveToQuarantineExactAction)):
                original_dst = action.dst
                allowed_root = (
                    destination_root
                    or _infer_destination_root(plan)
                    or os.path.abspath(os.path.dirname(action.dst))
                )
                new_dst = _execute_move(action, allowed_root)
                if os.path.abspath(new_dst) != os.path.abspath(original_dst):
                    renamed_paths.append((original_dst, new_dst))
                action.dst = new_dst
                original_hash = action.sha256
                if original_hash:
                    new_hash = compute_sha256(action.dst)
                    if new_hash != original_hash:
                        raise MergeExecutionError(
                            f"Hash mismatch after move for {action.dst}"
                        )
            elif isinstance(action, MarkNearDuplicateAction):
                write_log(
                    [f"[INFO] Marked look-alike for review: {action.src}"]
                )
            else:
                raise MergeExecutionError(f"Unknown action type: {action.type}")
        write_merge_report(plan, merge_report_path, mode_label="EXECUTE")
        write_source_manifest(plan, manifest_json_path, manifest_csv_path, manifest_html_path)
        _remove_file_if_exists(dedupe_report_path)
        write_log(
            [
                "[INFO] Merge execution completed",
                f"[INFO] merge_report.html saved to {merge_report_path}",
                f"[INFO] source_manifest.json saved to {manifest_json_path}",
                f"[INFO] source_manifest.csv saved to {manifest_csv_path}",
                f"[INFO] source_manifest.html saved to {manifest_html_path}",
            ]
        )
    except Exception as exc:
        log_error(f"Merge execution failed: {exc}")
        raise MergeExecutionError(f"Merge execution failed: {exc}") from exc
    return {"renamed": renamed_paths}


def load_source_manifest(manifest_path: str) -> tuple[str, list[dict]]:
    """
    Load and validate the source manifest for undo operations.
    """
    normalized = os.path.abspath(manifest_path)
    if not os.path.exists(normalized):
        raise UndoInputError("source_manifest.json not found; no undo data available.")
    try:
        with open(normalized, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError as exc:
        raise UndoInputError("source_manifest.json is not valid JSON.") from exc
    if not isinstance(payload, dict):
        raise UndoInputError("source_manifest.json is malformed.")
    batch_id = payload.get("batch_id")
    entries = payload.get("entries", [])
    if not batch_id:
        raise UndoInputError("source_manifest.json is missing batch_id.")
    if not isinstance(entries, list):
        raise UndoInputError("source_manifest.json entries must be a list.")
    return batch_id, entries


def _infer_library_root(entries: list[dict]) -> str | None:
    paths = [
        os.path.dirname(entry.get("new_path"))
        for entry in entries
        if entry.get("new_path")
    ]
    if not paths:
        return None
    root = os.path.abspath(os.path.commonpath(paths))
    while True:
        base = os.path.basename(root)
        parent = os.path.dirname(root)
        if base in ("QUARANTINE_EXACT", "REVIEW"):
            root = parent
            continue
        if is_year(base):
            root = parent
            continue
        if parent and is_year_month(base, os.path.basename(parent)):
            root = parent
            continue
        break
    return root


def _hash_matches(path: str, expected: str | None) -> bool:
    if not expected:
        return False
    return compute_sha256(path) == expected


def _existing_parent(path: str) -> str | None:
    current = os.path.abspath(path)
    while True:
        if os.path.exists(current):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return None
        current = parent


def _assert_same_filesystem(src: str, target_path: str) -> None:
    try:
        src_dev = os.stat(src).st_dev
    except OSError as exc:
        raise UndoSafetyError(f"Unable to stat source for undo: {src}") from exc
    target_parent = _existing_parent(os.path.dirname(target_path))
    if not target_parent:
        raise UndoSafetyError(f"Unable to locate existing parent for undo target: {target_path}")
    try:
        target_dev = os.stat(target_parent).st_dev
    except OSError as exc:
        raise UndoSafetyError(f"Unable to stat undo target parent: {target_parent}") from exc
    if src_dev != target_dev:
        raise UndoSafetyError("Undo blocked: source and target are on different filesystems.")


def _unique_destination(path: str, src_hash: str | None) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    suffix = (src_hash or "conflict")[:12]
    candidate = f"{base}-{suffix}{ext}"
    if not os.path.exists(candidate):
        return candidate
    for idx in range(1, 1000):
        numbered = f"{base}-{suffix}-{idx}{ext}"
        if not os.path.exists(numbered):
            return numbered
    raise UndoSafetyError(f"Unable to resolve undo conflict destination for {path}")


def prepare_undo_plan(manifest_path: str, operation_id: str) -> dict:
    """
    Build an undo plan based on the source manifest.
    """
    batch_id, entries = load_source_manifest(manifest_path)
    if operation_id != batch_id:
        raise UndoInputError("Unknown operation id; no matching manifest found.")
    library_root = _infer_library_root(entries)
    if not library_root:
        raise UndoInputError("Unable to infer library root from source manifest.")
    conflict_root = os.path.join(library_root, "REVIEW", "UNDO_CONFLICTS", operation_id)
    planned_entries: list[dict] = []
    for entry in entries:
        original_path = entry.get("original_path")
        new_path = entry.get("new_path")
        sha256 = entry.get("hash")
        if not original_path or not new_path:
            planned_entries.append(
                {
                    "original_path": original_path,
                    "new_path": new_path,
                    "hash": sha256,
                    "status": "skipped_invalid",
                    "reason": "Manifest entry missing required paths.",
                    "target_path": None,
                }
            )
            continue
        original_exists = os.path.exists(original_path)
        new_exists = os.path.exists(new_path)
        target_path = None
        if new_exists:
            if original_exists:
                target_path = os.path.join(conflict_root, os.path.basename(new_path))
                planned_entries.append(
                    {
                        "original_path": original_path,
                        "new_path": new_path,
                        "hash": sha256,
                        "status": "conflict",
                        "reason": "Original path occupied; routed to REVIEW.",
                        "target_path": target_path,
                    }
                )
            else:
                planned_entries.append(
                    {
                        "original_path": original_path,
                        "new_path": new_path,
                        "hash": sha256,
                        "status": "restore",
                        "reason": "Ready to restore.",
                        "target_path": original_path,
                    }
                )
        else:
            if original_exists and _hash_matches(original_path, sha256):
                planned_entries.append(
                    {
                        "original_path": original_path,
                        "new_path": new_path,
                        "hash": sha256,
                        "status": "skipped_restored",
                        "reason": "Already restored.",
                        "target_path": None,
                    }
                )
            elif original_exists:
                planned_entries.append(
                    {
                        "original_path": original_path,
                        "new_path": new_path,
                        "hash": sha256,
                        "status": "conflict_missing_source",
                        "reason": "Original path occupied; source missing.",
                        "target_path": None,
                    }
                )
            else:
                planned_entries.append(
                    {
                        "original_path": original_path,
                        "new_path": new_path,
                        "hash": sha256,
                        "status": "skipped_missing",
                        "reason": "Source missing; nothing to restore.",
                        "target_path": None,
                    }
                )
    return {
        "operation_id": operation_id,
        "library_root": library_root,
        "conflict_root": conflict_root,
        "entries": planned_entries,
    }


def _undo_counts(entries: list[dict]) -> dict:
    counts = {
        "total": len(entries),
        "restore": 0,
        "conflict": 0,
        "skipped": 0,
        "already_restored": 0,
        "missing": 0,
    }
    for entry in entries:
        status = entry.get("status")
        if status in ("restore", "would_restore", "restored"):
            counts["restore"] += 1
        elif status in ("conflict", "would_conflict", "conflict_routed", "conflict_missing_source"):
            counts["conflict"] += 1
        elif status in ("skipped_restored", "skipped_missing", "skipped_invalid"):
            counts["skipped"] += 1
            if status == "skipped_restored":
                counts["already_restored"] += 1
            if status in ("skipped_missing", "skipped_invalid"):
                counts["missing"] += 1
    return counts


def preview_undo(plan: dict) -> dict:
    """
    Generate a preview summary without moving files.
    """
    entries: list[dict] = []
    for entry in plan["entries"]:
        cloned = dict(entry)
        if cloned.get("status") == "restore":
            cloned["status"] = "would_restore"
        elif cloned.get("status") == "conflict":
            cloned["status"] = "would_conflict"
        entries.append(cloned)
    summary = {
        "operation_id": plan["operation_id"],
        "mode": "preview",
        "generated_at": datetime.now().isoformat(),
        "library_root": plan["library_root"],
        "conflict_root": plan["conflict_root"],
        "entries": entries,
    }
    summary["counts"] = _undo_counts(entries)
    log_info("Undo preview generated; no files were moved.")
    return summary


def execute_undo(plan: dict) -> dict:
    """
    Execute the undo plan with integrity and safety checks.
    """
    conflict_root = plan["conflict_root"]
    ensure_directory(conflict_root)
    results: list[dict] = []
    for entry in plan["entries"]:
        status = entry.get("status")
        src = entry.get("new_path")
        original = entry.get("original_path")
        sha256 = entry.get("hash")
        if status not in ("restore", "conflict"):
            results.append(entry)
            continue
        if not src or not os.path.exists(src):
            refreshed = dict(entry)
            if original and os.path.exists(original) and _hash_matches(original, sha256):
                refreshed["status"] = "skipped_restored"
                refreshed["reason"] = "Already restored."
            else:
                refreshed["status"] = "skipped_missing"
                refreshed["reason"] = "Source missing at execution."
            refreshed["target_path"] = None
            results.append(refreshed)
            continue
        if status == "restore":
            target_path = original
        else:
            target_path = os.path.join(conflict_root, os.path.basename(src))
        if not target_path:
            refreshed = dict(entry)
            refreshed["status"] = "skipped_invalid"
            refreshed["reason"] = "Missing target path for undo."
            results.append(refreshed)
            continue
        _assert_same_filesystem(src, target_path)
        current_hash = compute_sha256(src)
        if sha256 and current_hash != sha256:
            log_error(f"Undo integrity mismatch before move: {src}")
            raise UndoSafetyError("Integrity mismatch before undo move.")
        log_info(f"Undo precheck OK: {src} sha256={current_hash}")
        if os.path.exists(target_path):
            if status == "restore":
                target_path = os.path.join(conflict_root, os.path.basename(src))
            target_path = _unique_destination(target_path, sha256)
        ensure_directory(os.path.dirname(target_path))
        shutil.move(src, target_path)
        if not os.path.exists(target_path):
            log_error(f"Undo move failed: {src} -> {target_path}")
            raise UndoSafetyError("Undo move failed.")
        moved_hash = compute_sha256(target_path)
        if sha256 and moved_hash != sha256:
            log_error(f"Undo integrity mismatch after move: {target_path}")
            raise UndoSafetyError("Integrity mismatch after undo move.")
        log_info(f"Undo postcheck OK: {target_path} sha256={moved_hash}")
        updated = dict(entry)
        updated["target_path"] = target_path
        if status == "restore" and os.path.abspath(target_path) == os.path.abspath(original or ""):
            updated["status"] = "restored"
            updated["reason"] = "Restored to original path."
            log_info(f"Undo restored: {src} -> {target_path}")
        else:
            updated["status"] = "conflict_routed"
            updated["reason"] = "Original path occupied; routed to REVIEW."
            log_info(f"Undo conflict routed: {src} -> {target_path}")
        results.append(updated)
    summary = {
        "operation_id": plan["operation_id"],
        "mode": "execute",
        "generated_at": datetime.now().isoformat(),
        "library_root": plan["library_root"],
        "conflict_root": plan["conflict_root"],
        "entries": results,
    }
    summary["counts"] = _undo_counts(results)
    return summary


def _remove_file_if_exists(path: str) -> bool:
    if not path or not os.path.exists(path):
        return False
    try:
        os.remove(path)
        write_log([f"[INFO] Removed stale dedupe report: {path}"])
        return True
    except OSError as exc:
        log_error(f"Failed to remove dedupe report {path}: {exc}")
        return False


def _validate_destination(
    destination: str,
    reporter: Callable[[str], None] | None = None,
) -> None:
    """
    Ensure destination is empty or already organized YEAR/YEAR-MONTH.

    Raises:
        MergePlanError: When destination violates chronological structure requirement.
    """
    normalized = os.path.abspath(destination)
    if reporter:
        reporter(f"Inspecting destination: {normalized}")

    if os.path.islink(normalized):
        message = (
            f"Destination '{normalized}' is a symlink. Remove it or choose a "
            "real directory inside your merge target."
        )
        if reporter:
            reporter(message)
        log_warning(message)
        raise MergePlanError(message)

    if os.path.isfile(destination):
        if reporter:
            reporter("Destination references a file, not a folder.")
        raise MergePlanError("Destination must be a directory.")

    if not os.path.exists(destination):
        if reporter:
            reporter("Destination does not exist yet; treated as empty.")
        return

    entries = [entry for entry in os.listdir(destination) if not entry.startswith(".")]
    if not entries:
        if reporter:
            reporter("Destination directory is empty.")
        return

    for entry in entries:
        year_path = os.path.join(destination, entry)
        if reporter:
            reporter(f"Checking YEAR folder '{entry}'.")
        violation = path_violation_message(
            year_path, normalized, label="Destination folder"
        )
        if violation:
            log_warning(f"Destination safety violation: {violation}")
            if reporter:
                reporter(violation)
            raise MergePlanError(violation)
        if not os.path.isdir(year_path):
            raise MergePlanError(
                f"Destination entry '{entry}' is not a directory. Only YEAR folders are allowed."
            )
        if not is_year(entry):
            raise MergePlanError(
                f"Destination entry '{entry}' is not a valid YEAR folder (expected YYYY)."
            )

        month_entries = [m for m in os.listdir(year_path) if not m.startswith(".")]
        for month_entry in month_entries:
            month_path = os.path.join(year_path, month_entry)
            if reporter:
                reporter(f"Checking MONTH folder '{month_entry}' inside '{entry}'.")
            violation = path_violation_message(
                month_path, normalized, label="Destination folder"
            )
            if violation:
                log_warning(f"Destination safety violation: {violation}")
                if reporter:
                    reporter(violation)
                raise MergePlanError(violation)
            if not os.path.isdir(month_path):
                raise MergePlanError(
                    f"Destination entry '{month_entry}' under '{entry}' must be a directory."
                )
            if not is_year_month(month_entry, entry):
                raise MergePlanError(
                    f"Destination folder '{month_entry}' under '{entry}' is not a valid YEAR-MONTH (YYYY-MM)."
                )


def validate_destination(
    destination: str,
    reporter: Callable[[str], None] | None = None,
) -> None:
    """
    Public wrapper for _validate_destination.
    """
    _validate_destination(destination, reporter=reporter)








def is_year(name: str) -> bool:


    return len(name) == 4 and name.isdigit() and (name.startswith("19") or name.startswith("20"))








def is_year_month(name: str, year: str) -> bool:


    if len(name) != 7 or name[4] != "-":


        return False


    y, m = name.split("-", 1)


    if y != year:


        return False


    return m.isdigit() and 1 <= int(m) <= 12








def _get_parent(path: str | None) -> str | None:


    """


    Return parent directory for a given path, if any.


    """


    if not path:


        return None


    return os.path.abspath(os.path.dirname(path))








def _execute_move(
    action: Union[MoveMasterAction, MoveToQuarantineExactAction],
    allowed_root: str,
) -> str:


    """


    Helper to perform move actions safely.


    """


    return safe_move(action.src, action.dst, allowed_root=allowed_root)








def _common_source_root(files: List[FileInfo]) -> str | None:


    """


    Determine a common source root for preserving hierarchy when organization is off.


    """


    paths = [os.path.abspath(f.path) for f in files]


    if not paths:


        return None


    try:


        return os.path.commonpath(paths)


    except ValueError:


        return None








def _available_space(path: str) -> int:


    """


    Safely compute available disk space without creating directories.


    """


    probe = os.path.abspath(path)


    while probe and not os.path.exists(probe):


        parent = os.path.dirname(probe)


        if parent == probe:


            return 0


        probe = parent


    try:


        return shutil.disk_usage(probe or ".").free


    except FileNotFoundError:


        return 0








def available_space(path: str) -> int:


    """


    Public wrapper for _available_space.


    """


    return _available_space(path)


def _infer_destination_root(plan: MergePlan) -> str | None:
    """
    Derive the common destination root from planned actions.
    """
    move_targets = [
        os.path.abspath(action.dst)
        for action in plan.actions
        if isinstance(action, (MoveMasterAction, MoveToQuarantineExactAction))
    ]
    if move_targets:
        return os.path.commonpath(move_targets)
    folder_targets = [
        os.path.abspath(action.path)
        for action in plan.actions
        if isinstance(action, CreateFolderAction)
    ]
    if folder_targets:
        return os.path.commonpath(folder_targets)
    return None
