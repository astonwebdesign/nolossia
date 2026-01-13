"""
Module: duplicates
Purpose: Duplicate grouping and master selection.
"""

import os
from typing import Any, Callable, Dict, Iterable, List

from .exceptions import DuplicateDetectionError
from .hashing import phash_distance
from .models.cluster import DuplicateCluster
from .models.fileinfo import FileInfo
from .utils import log_error

VerboseReporter = Callable[[str], None]
DiagnosticsReporter = Callable[[dict[str, Any]], None]


# Perceptual hash thresholds (spec default: distance <= 5 counts as near-duplicate)
# distance <= PHASH_STRONG: strong near-duplicate signal (minimal validation)
# PHASH_STRONG < distance <= PHASH_WEAK: weak signal, requires strict validation
# distance > PHASH_WEAK: not a near-duplicate
PHASH_STRONG = 2
PHASH_WEAK = 5
SENSITIVITY_THRESHOLDS = {
    "conservative": (PHASH_STRONG, PHASH_WEAK),
    "balanced": (3, 7),
    "aggressive": (4, 10),
}


def _normalize_sensitivity(value: str | None) -> str:
    if not value:
        return "conservative"
    normalized = value.strip().lower()
    if normalized in {"conservative", "balanced", "aggressive"}:
        return normalized
    return "conservative"


class UnionFind:
    """
    A Union-Find data structure for grouping connected components.
    Each element is a hashable FileInfo object.
    """
    def __init__(self, elements: Iterable[FileInfo]):
        self.parent: Dict[FileInfo, FileInfo] = {el: el for el in elements}
        self.rank: Dict[FileInfo, int] = {el: 0 for el in elements}

    def find(self, element: FileInfo) -> FileInfo:
        """Finds the representative (root) of the set containing element."""
        if self.parent[element] == element:
            return element
        self.parent[element] = self.find(self.parent[element]) # Path compression
        return self.parent[element]

    def union(self, element1: FileInfo, element2: FileInfo) -> None:
        """Merges the sets containing element1 and element2."""
        root1 = self.find(element1)
        root2 = self.find(element2)

        if root1 != root2:
            # Union by rank (or size)
            if self.rank[root1] < self.rank[root2]:
                self.parent[root1] = root2
            elif self.rank[root1] > self.rank[root2]:
                self.parent[root2] = root1
            else:
                self.parent[root2] = root1
                self.rank[root1] += 1


def group_duplicates(
    files: List[FileInfo],
    reporter: VerboseReporter | None = None,
    diagnostics_logger: DiagnosticsReporter | None = None,
    *,
    sensitivity: str = "conservative",
) -> List[DuplicateCluster]:
    """
    Group files into clusters using SHA256 and perceptual hash,
    leveraging Union-Find for efficient clustering.

    Args:
        files: List of FileInfo objects to analyze.
        reporter: Optional callback for verbose master-selection tracing.
        diagnostics_logger: Optional callback for near-duplicate diagnostics payloads.

    Returns:
        List of DuplicateCluster instances.

    Raises:
        DuplicateDetectionError: If grouping fails.
    """
    try:
        if not files:
            return []

        # Initialize Union-Find structure with all files
        uf = UnionFind(files)

        # Phase 1: Group exact duplicates using SHA256
        exact_groups: Dict[str, List[FileInfo]] = {}
        for file in files:
            if file.sha256:
                exact_groups.setdefault(file.sha256, []).append(file)
        
        for group in exact_groups.values():
            if len(group) > 1:
                first_file = group[0]
                for i in range(1, len(group)):
                    uf.union(first_file, group[i])
        
        # Phase 2: Group near-duplicates using phash and a windowed comparison
        phash_candidates = [f for f in files if f.phash] # Only consider files with phash
        # Sort candidates by phash to bring similar hashes closer
        phash_candidates.sort(key=lambda x: x.phash if x.phash else "") # Sorting by string is okay for proximity
        
        PHASH_SEARCH_WINDOW = 20 # Compare with files within this window after sorting by phash

        sensitivity = _normalize_sensitivity(sensitivity)
        for i, base in enumerate(phash_candidates):
            for j in range(i + 1, min(i + 1 + PHASH_SEARCH_WINDOW, len(phash_candidates))):
                candidate = phash_candidates[j]
                diag_callback: DiagnosticsReporter | None = None
                if diagnostics_logger is not None:
                    def diag_callback(payload: dict[str, Any], *, _logger=diagnostics_logger) -> None:
                        _logger(payload)
                if are_near_duplicates(
                    base,
                    candidate,
                    diagnostics_logger=diag_callback,
                    sensitivity=sensitivity,
                ):
                    uf.union(base, candidate)

        # Extract clusters from the Union-Find structure
        raw_clusters: Dict[FileInfo, List[FileInfo]] = {} # Key is representative FileInfo
        for file in files:
            root_file = uf.find(file)
            raw_clusters.setdefault(root_file, []).append(file)

        final_clusters: List[DuplicateCluster] = []
        cluster_index = 1
        for root, cluster_files in raw_clusters.items():
            # If a cluster only has one file and that file has no sha256 or phash, it's just a unique file.
            # We still represent it as a cluster of 1, as per previous behavior.
            cluster_id = f"cluster_{cluster_index:04d}"
            cluster_index += 1
            master = select_master(
                DuplicateCluster(cluster_id, cluster_files, None, []),
                reporter=reporter,
            )
            redundant = [f for f in cluster_files if f is not master]
            final_clusters.append(DuplicateCluster(cluster_id, cluster_files, master, redundant))

        return final_clusters
    except Exception as exc:
        log_error(f"Failed to group duplicates: {exc}")
        raise DuplicateDetectionError("Failed to group duplicates") from exc


def _describe_file(file: FileInfo) -> str:
    fmt = (file.format or "unknown").upper()
    res = file.resolution or (0, 0)
    if res[0] and res[1]:
        res_label = f"{res[0]}x{res[1]}"
    else:
        res_label = "resolution?"
    gps_label = "GPS" if file.exif_gps else "no-GPS"
    raw_label = "RAW" if file.is_raw else "STD"
    return f"{fmt} {raw_label} {res_label} {gps_label}"


def select_master(
    cluster: DuplicateCluster,
    reporter: VerboseReporter | None = None,
) -> FileInfo:
    """
    Choose master based on 6 conflict resolution rules.

    Args:
        cluster: Cluster containing duplicate candidates.
        reporter: Optional callback for verbose rule evaluation logging.

    Returns:
        FileInfo selected as master.

    Raises:
        DuplicateDetectionError: If master cannot be selected.
    """
    def metadata_advantage(
        left_exif_count: int,
        right_exif_count: int,
        left_has_gps: bool,
        right_has_gps: bool,
    ) -> bool:
        if left_exif_count > right_exif_count:
            return True
        if left_exif_count == right_exif_count and left_has_gps and not right_has_gps:
            return True
        return False

    def is_better(candidate: FileInfo, current: FileInfo) -> tuple[bool, str | None]:
        candidate_res = candidate.resolution or (0, 0)
        current_res = current.resolution or (0, 0)
        candidate_fmt = (candidate.format or "").lower()
        current_fmt = (current.format or "").lower()

        candidate_pixels = candidate_res[0] * candidate_res[1]
        current_pixels = current_res[0] * current_res[1]

        candidate_exif_count = sum(
            value is not None
            for value in (
                candidate.exif_datetime,
                candidate.exif_gps,
                candidate.exif_camera,
                candidate.exif_orientation,
            )
        )
        current_exif_count = sum(
            value is not None
            for value in (
                current.exif_datetime,
                current.exif_gps,
                current.exif_camera,
                current.exif_orientation,
            )
        )
        candidate_has_gps = candidate.exif_gps is not None
        current_has_gps = current.exif_gps is not None

        if candidate.is_raw != current.is_raw:
            return candidate.is_raw, "RAW_BEATS_JPEG"

        if candidate_pixels != current_pixels:
            return candidate_pixels > current_pixels, "HIGHER_RESOLUTION_WINS"

        if candidate_res == current_res:
            heic_vs_jpeg = candidate_fmt == "heic" and current_fmt in {"jpg", "jpeg"}
            jpeg_vs_heic = candidate_fmt in {"jpg", "jpeg"} and current_fmt == "heic"
            if heic_vs_jpeg:
                if metadata_advantage(
                    candidate_exif_count,
                    current_exif_count,
                    candidate_has_gps,
                    current_has_gps,
                ):
                    return True, "MORE_EXIF_WINS"
            if jpeg_vs_heic:
                if metadata_advantage(
                    current_exif_count,
                    candidate_exif_count,
                    current_has_gps,
                    candidate_has_gps,
                ):
                    return False, "MORE_EXIF_WINS"

        if candidate.size != current.size:
            return candidate.size > current.size, "LARGER_FILESIZE_WINS"

        if candidate_exif_count != current_exif_count:
            return candidate_exif_count > current_exif_count, "MORE_EXIF_WINS"

        if candidate_has_gps != current_has_gps:
            return candidate_has_gps, "GPS_PRESENT_BEATS_GPS_ABSENT"

        candidate_ts = candidate.exif_datetime
        current_ts = current.exif_datetime
        if candidate_ts is None and current_ts is None:
            return False, None
        if candidate_ts is None:
            return False, "OLDEST_CAPTURE_WINS"
        if current_ts is None:
            return True, "OLDEST_CAPTURE_WINS"
        if candidate_ts != current_ts:
            return (candidate_ts < current_ts), "OLDEST_CAPTURE_WINS"
        return False, None

    master: FileInfo | None = None
    master_reason: str | None = None
    for file in cluster.files:
        if master is None:
            master = file
            if reporter:
                reporter(f"[{cluster.cluster_id}] INITIAL_MASTER → {_describe_file(file)}")
            continue
        better, reason = is_better(file, master)
        if better:
            if reporter:
                desc_candidate = _describe_file(file)
                desc_master = _describe_file(master)
                rule_label = reason or "RULE_APPLIED"
                reporter(
                    f"[{cluster.cluster_id}] kept because {rule_label} → {desc_candidate} outranks {desc_master}"
                )
            if reason:
                master_reason = reason
            master = file
        elif reason:
            if reporter:
                desc_candidate = _describe_file(file)
                desc_master = _describe_file(master)
                reporter(
                    f"[{cluster.cluster_id}] kept because {reason} → {desc_master} over {desc_candidate}"
                )
            master_reason = reason

    if master is None:
        log_error("Unable to select master for cluster")
        raise DuplicateDetectionError("Unable to select master for cluster")
    master.selection_reason = master_reason
    return master



def are_near_duplicates(
    a: FileInfo,
    b: FileInfo,
    diagnostics_logger: DiagnosticsReporter | None = None,
    *,
    sensitivity: str = "conservative",
) -> bool:
    """
    Determine whether a and b should be treated as near-duplicates using
    layered thresholds and additional validation:
    - pHash distance (spec default threshold 5)
    - Strong band (<=2): lenient acceptance once hashes exist.
    - Weak band (<=5): strict validation with resolution/date/camera checks.

    Diagnostics include decision, reason, hamming distance, and relevant ratios/deltas.
    """
    distance_value: int | None = None
    sensitivity = _normalize_sensitivity(sensitivity)
    strong_threshold, weak_threshold = SENSITIVITY_THRESHOLDS[sensitivity]

    def emit(decision: str, reason: str, **extra: Any) -> None:
        if diagnostics_logger is None:
            return
        payload: dict[str, Any] = {
            "pair": [os.path.basename(a.path), os.path.basename(b.path)],
            "decision": decision,
            "reason": reason,
            "sensitivity": sensitivity,
        }
        if distance_value is not None:
            payload["distance"] = distance_value
        for key, value in extra.items():
            if value is not None:
                payload[key] = value
        diagnostics_logger(payload)

    if not a.phash or not b.phash:
        emit("REJECT", "phash_missing", distance=None)
        return False

    distance_value = phash_distance(a.phash, b.phash)

    # Case 1: strong signal (lenient validation)
    if distance_value <= strong_threshold:
        emit("ACCEPT", "distance_strong_band")
        return True

    # Case 2: weak signal with strict checks
    if strong_threshold < distance_value <= weak_threshold:
        if not (a.resolution and b.resolution):
            emit("REJECT", "resolution_missing")
            return False
        area_a = a.resolution[0] * a.resolution[1]
        area_b = b.resolution[0] * b.resolution[1]
        if area_a == 0 or area_b == 0:
            emit("REJECT", "resolution_zero")
            return False
        ratio = max(area_a, area_b) / min(area_a, area_b)
        if ratio > 1.3:
            emit("REJECT", "resolution_ratio_exceeded", resolution_ratio=round(ratio, 3))
            return False

        if not (a.exif_datetime and b.exif_datetime):
            emit("REJECT", "timestamp_missing")
            return False
        delta_seconds = abs((a.exif_datetime - b.exif_datetime).total_seconds())
        if delta_seconds > 60:
            emit("REJECT", "timestamp_delta_exceeded", timestamp_delta_seconds=int(delta_seconds))
            return False

        if a.exif_camera and b.exif_camera and a.exif_camera != b.exif_camera:
            emit("REJECT", "camera_mismatch")
            return False

        emit(
            "ACCEPT",
            "distance_weak_band",
            resolution_ratio=round(ratio, 3),
            timestamp_delta_seconds=int(delta_seconds),
        )
        return True

    # Case 3: not a near duplicate
    emit("REJECT", "distance_over_threshold")
    return False
