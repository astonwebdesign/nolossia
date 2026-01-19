import json
from datetime import datetime, timedelta

from src import reporting
from src.models.actions import MarkNearDuplicateAction, MoveMasterAction, MoveToQuarantineExactAction
from src.models.cluster import DuplicateCluster
from src.models.fileinfo import FileInfo
from src.models.mergeplan import MergePlan
from src.review import REVIEW_REASON_MISSING_EXIF, describe_review_reason


def test_log_file_name_constant():
    assert reporting.LOG_FILE_NAME == "artifacts/nolossia.log"


def test_write_dedupe_report(tmp_path):
    m1 = FileInfo(
        path=str(tmp_path / "m1.jpg"),
        size=10,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="aaa",
        phash="aaaa",
        is_raw=False,
    )
    d1 = FileInfo(
        path=str(tmp_path / "d1.jpg"),
        size=5,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="aaa",
        phash="aaab",
        is_raw=False,
    )
    m2 = FileInfo(
        path=str(tmp_path / "m2.dng"),
        size=20,
        format="dng",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="bbb",
        phash="bbbb",
        is_raw=True,
    )
    m2.selection_reason = "RAW_BEATS_JPEG"
    n1 = FileInfo(
        path=str(tmp_path / "n1.jpg"),
        size=8,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="ccc",
        phash="bbbc",
        is_raw=False,
    )

    exact_cluster = DuplicateCluster("c1", [m1, d1], m1, [d1])
    near_cluster = DuplicateCluster("c2", [m2, n1], m2, [n1])

    outfile = tmp_path / "dedupe_report.html"
    reporting.write_dedupe_report([m1, d1, m2, n1], [exact_cluster, near_cluster], str(outfile), unique_size=30)

    text = outfile.read_text()
    assert "Nolossia Dedupe Report" in text
    assert "Designed to prevent data loss." in text
    assert "Print / Save as PDF" in text
    assert "Masters overview" in text
    assert "Exact copies" in text
    assert "Look-alike groups" in text
    assert "Mark as independent master" in text
    assert "Kept because" in text
    assert "RAW beats JPEG/HEIC" in text
    assert "zoomable" not in text
    assert "scale(" not in text


def test_write_dedupe_report_deterministic(tmp_path):
    files = []
    for idx in range(4):
        files.append(
            FileInfo(
                path=str(tmp_path / f"file{idx}.jpg"),
                size=idx + 1,
                format="jpg",
                resolution=None,
                exif_datetime=None,
                exif_gps=None,
                exif_camera=None,
                exif_orientation=None,
                sha256=str(idx),
                phash=str(idx),
                is_raw=False,
            )
        )
    exact = DuplicateCluster("exact", [files[0], files[1]], files[0], [files[1]])
    near = DuplicateCluster("near", [files[2], files[3]], files[2], [files[3]])

    out1 = tmp_path / "report1.html"
    out2 = tmp_path / "report2.html"

    reporting.write_dedupe_report(files, [exact, near], str(out1), unique_size=10)
    reporting.write_dedupe_report(list(reversed(files)), [near, exact], str(out2), unique_size=10)

    assert out1.read_text() == out2.read_text()


def test_write_dedupe_report_limits_near_duplicates(tmp_path):
    total_clusters = reporting.NEAR_DUP_CLUSTER_LIMIT + 2
    extra_candidates = reporting.NEAR_DUP_CANDIDATE_LIMIT + 3
    files = []
    clusters = []
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    for idx in range(total_clusters):
        master = FileInfo(
            path=str(tmp_path / f"master_{idx}.jpg"),
            size=10 + idx,
            format="jpg",
            resolution=(1000 + idx, 800 + idx),
            exif_datetime=base_time + timedelta(days=idx),
            exif_gps=None,
            exif_camera=None,
            exif_orientation=None,
            sha256=f"m-{idx}",
            phash=f"{idx:016x}",
            is_raw=False,
        )
        redundant = []
        candidate_count = extra_candidates if idx == 0 else 1
        for jdx in range(candidate_count):
            candidate = FileInfo(
                path=str(tmp_path / f"near_{idx}_{jdx}.jpg"),
                size=5 + jdx,
                format="jpg",
                resolution=(900 + jdx, 700 + jdx),
                exif_datetime=base_time + timedelta(days=idx, minutes=jdx),
                exif_gps=None,
                exif_camera=None,
                exif_orientation=None,
                sha256=f"n-{idx}-{jdx}",
                phash=f"{idx + jdx + 1:016x}",
                is_raw=False,
            )
            redundant.append(candidate)
            files.append(candidate)
        clusters.append(DuplicateCluster(f"near-{idx}", [master] + redundant, master, redundant))
        files.append(master)

    outfile = tmp_path / "dedupe_report_limited.html"
    reporting.write_dedupe_report(files, clusters, str(outfile), unique_size=100)

    text = outfile.read_text()
    assert f"Showing top {reporting.NEAR_DUP_CLUSTER_LIMIT} of {total_clusters} look-alike" in text
    assert f"Showing top {reporting.NEAR_DUP_CANDIDATE_LIMIT} of {extra_candidates} candidates" in text
    assert text.count("<h3>Cluster") == reporting.NEAR_DUP_CLUSTER_LIMIT


def test_write_dedupe_report_escapes_special_characters(tmp_path):
    tricky_name = "bad\"'&.jpg"
    tricky_master = FileInfo(
        path=str(tmp_path / tricky_name),
        size=5,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="xxx",
        phash="yyy",
        is_raw=False,
    )
    duplicate = FileInfo(
        path=str(tmp_path / "copy.jpg"),
        size=5,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256="xxx",
        phash="yyz",
        is_raw=False,
    )
    cluster = DuplicateCluster("c-special", [tricky_master, duplicate], tricky_master, [duplicate])
    outfile = tmp_path / "escaped.html"

    reporting.write_dedupe_report([tricky_master, duplicate], [cluster], str(outfile), unique_size=5)

    text = outfile.read_text()
    assert "&quot;" in text
    assert "&#x27;" in text
    assert tricky_name not in text  # raw string should be escaped


def test_write_merge_report_deterministic(tmp_path):
    plan = MergePlan(
        required_space=100,
        destination_free=200,
        duplicate_count=0,
        total_files=3,
        actions=[
            MoveToQuarantineExactAction(src="b2", dst="q2", sha256=None, size=2),
            MoveMasterAction(src="a1", dst="lib/2024/2024-01/a1", sha256=None, size=1),
            MarkNearDuplicateAction(src="c3", master="lib/2024/2024-01/a1", sha256=None, size=3),
        ],
        destination_path="lib",
    )

    out1 = tmp_path / "merge1.html"
    out2 = tmp_path / "merge2.html"

    reporting.write_merge_report(plan, str(out1))
    plan.actions = list(reversed(plan.actions))
    reporting.write_merge_report(plan, str(out2))

    assert out1.read_text() == out2.read_text()


def test_write_json_report_includes_skipped(tmp_path):
    outfile = tmp_path / "plan.json"
    plan = MergePlan(
        required_space=123,
        destination_free=456,
        duplicate_count=2,
        total_files=10,
        actions=[],
        destination_path="lib",
        skipped_files=3,
    )

    reporting.write_json_report(plan, str(outfile))

    data = json.loads(outfile.read_text())
    assert data["schema_version"] == "1.0"
    assert data["skipped_files"] == 3
    assert data["destination_path"] == "lib"
    assert data["review_files"] == []
    assert data["required_breakdown"] == {"masters": 0, "quarantine": 0, "review": 0}


def test_write_json_report_lists_review_files(tmp_path):
    outfile = tmp_path / "plan.json"
    plan = MergePlan(
        required_space=1,
        destination_free=10,
        duplicate_count=0,
        total_files=1,
        actions=[
            MoveMasterAction(
                src="src.jpg",
                dst="lib/REVIEW/src.jpg",
                sha256="abc",
                size=1,
                review_reason=REVIEW_REASON_MISSING_EXIF,
            )
        ],
        destination_path="lib",
    )
    reporting.write_json_report(plan, str(outfile))
    data = json.loads(outfile.read_text())
    assert data["review_files"]
    entry = data["review_files"][0]
    assert entry["destination"].endswith("REVIEW/src.jpg")
    assert entry["relative_destination"].startswith("REVIEW/")
    assert entry["source"].endswith("src.jpg")
    assert entry["reason_code"] == REVIEW_REASON_MISSING_EXIF
    assert entry["reason"] == describe_review_reason(REVIEW_REASON_MISSING_EXIF)
    breakdown = data["required_breakdown"]
    assert breakdown["review"] == 1
    assert breakdown["masters"] == 0


def test_write_merge_report_includes_review_section(tmp_path):
    plan = MergePlan(
        required_space=1,
        destination_free=5,
        duplicate_count=0,
        total_files=1,
        actions=[
            MoveMasterAction(
                src="src.jpg",
                dst="library/REVIEW/src.jpg",
                sha256=None,
                size=1,
                review_reason=REVIEW_REASON_MISSING_EXIF,
            ),
        ],
        destination_path="library",
    )
    outfile = tmp_path / "merge.html"
    reporting.write_merge_report(plan, str(outfile))
    text = outfile.read_text()
    assert "Designed to prevent data loss." in text
    assert "Print / Save as PDF" in text
    assert "Set aside for review" in text
    assert "REVIEW/src.jpg" in text
    assert describe_review_reason(REVIEW_REASON_MISSING_EXIF) in text
    assert "Storage breakdown" in text


def test_write_merge_report_mentions_skipped(tmp_path):
    plan = MergePlan(
        required_space=10,
        destination_free=100,
        duplicate_count=0,
        total_files=1,
        actions=[
            MoveMasterAction(src="src.jpg", dst="dest.jpg", sha256=None, size=1),
        ],
        destination_path="lib",
        skipped_files=5,
    )
    outfile = tmp_path / "merge.html"

    reporting.write_merge_report(plan, str(outfile))

    text = outfile.read_text()
    assert "Skipped files" in text
    assert "5" in text
