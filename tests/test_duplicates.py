from datetime import datetime

from src import duplicates
from src.models.fileinfo import FileInfo
from src.models.cluster import DuplicateCluster


def make_file(
    path: str,
    size: int = 1,
    sha: str | None = None,
    phash: str | None = None,
    dt: datetime | None = None,
    resolution: tuple[int, int] | None = (10, 10),
    fmt: str = "jpg",
):
    return FileInfo(
        path=path,
        size=size,
        format=fmt,
        resolution=resolution,
        exif_datetime=dt,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=sha,
        phash=phash,
        is_raw=False,
    )


def test_group_duplicates_exact_and_near():
    f1 = make_file("a.jpg", sha="x1", phash="abcd", dt=datetime(2020, 1, 1))
    f2 = make_file("b.jpg", sha="x1", phash="abce", dt=datetime(2020, 1, 1))
    f3 = make_file("c.jpg", sha="x3", phash="abcf", dt=datetime(2020, 1, 1))
    files = [f1, f2, f3]
    clusters = duplicates.group_duplicates(files, sensitivity="balanced")
    assert len(clusters) == 1
    cluster = clusters[0]
    assert cluster.master in (f1, f2)
    assert len(cluster.redundant) == 2


def test_group_duplicates_conservative_disables_near_grouping():
    dt = datetime(2020, 1, 1)
    f1 = make_file("a.jpg", sha="x1", phash="abcd", dt=dt)
    f2 = make_file("b.jpg", sha="x1", phash="abce", dt=dt)
    f3 = make_file("c.jpg", sha="x3", phash="abcf", dt=dt)
    clusters = duplicates.group_duplicates([f1, f2, f3], sensitivity="conservative")
    assert len(clusters) == 2
    ids = sorted(len(c.files) for c in clusters)
    assert ids == [1, 2]


def test_select_master_prefers_resolution_and_size():
    low = make_file("low.jpg", size=10, sha="s1", resolution=(10, 10))
    high = make_file("high.jpg", size=20, sha="s1", resolution=(20, 20))
    cluster = DuplicateCluster("c1", [low, high], None, [])
    master = duplicates.select_master(cluster)
    assert master is high


def test_are_near_duplicates_respects_thresholds():
    dt = datetime(2021, 1, 1)
    a = make_file("a.jpg", phash="ffff", dt=dt)
    b = make_file("b.jpg", phash="fffe", dt=dt)  # distance 1 strong
    c = make_file("c.jpg", phash="fff0", dt=dt)  # distance 4 within weak
    d = make_file("d.jpg", phash="f0f0", dt=dt)  # distance > 5
    assert duplicates.are_near_duplicates(a, b)
    assert duplicates.are_near_duplicates(a, c)
    assert not duplicates.are_near_duplicates(a, d)


def test_master_selection_prefers_raw_and_exif():
    dt_old = datetime(2020, 1, 1)
    dt_new = datetime(2021, 1, 1)
    raw_file = make_file("raw.nef", size=15, sha="s2", phash="abcd", dt=dt_new, resolution=(15, 15))
    jpg_file = make_file("img.jpg", size=20, sha="s2", phash="abce", dt=dt_old, resolution=(20, 20))
    raw_file.is_raw = True
    cluster = DuplicateCluster("c2", [raw_file, jpg_file], None, [])
    master = duplicates.select_master(cluster)
    assert master is raw_file


def select_master(files: list[FileInfo]) -> FileInfo:
    cluster = DuplicateCluster("ctest", files, None, [])
    return duplicates.select_master(cluster)


def test_heic_preference_requires_metadata_advantage():
    dt = datetime(2022, 1, 1)
    heic = make_file(
        "img.heic",
        size=5,
        fmt="heic",
        dt=dt,
        resolution=(4000, 3000),
    )
    heic.exif_camera = "cam"
    heic.exif_orientation = "1"
    heic.exif_gps = (1.0, 2.0)

    jpeg = make_file(
        "img.jpg",
        size=40,
        fmt="jpg",
        dt=dt,
        resolution=(4000, 3000),
    )

    master_forward = select_master([jpeg, heic])
    master_reverse = select_master([heic, jpeg])
    assert master_forward is heic
    assert master_reverse is heic


def test_heic_not_preferred_without_metadata_advantage():
    dt = datetime(2022, 6, 1)
    heic = make_file(
        "noop.heic",
        size=5,
        fmt="heic",
        dt=dt,
        resolution=(3000, 2000),
    )
    jpeg = make_file(
        "noop.jpg",
        size=50,
        fmt="jpg",
        dt=dt,
        resolution=(3000, 2000),
    )

    master_forward = select_master([heic, jpeg])
    master_reverse = select_master([jpeg, heic])
    assert master_forward is jpeg
    assert master_reverse is jpeg


def test_select_master_reports_verbose_reason():
    dt = datetime(2021, 5, 1)
    raw = make_file("raw.dng", size=15, sha="sha1", fmt="dng", dt=dt)
    raw.is_raw = True
    jpeg = make_file("dup.jpg", size=15, sha="sha1", fmt="jpg", dt=dt)
    cluster = DuplicateCluster("c_verbose", [jpeg, raw], None, [])
    messages: list[str] = []
    master = duplicates.select_master(cluster, reporter=messages.append)
    assert master is raw
    assert any("RAW_BEATS_JPEG" in message for message in messages)


def test_are_near_duplicates_logs_acceptance():
    dt = datetime(2022, 1, 1, 0, 0, 0)
    a = make_file("a.jpg", phash="ffff000000000000", dt=dt, resolution=(2000, 1000))
    b = make_file("b.jpg", phash="fffe000000000000", dt=dt, resolution=(2100, 1050))
    logs: list[dict] = []

    def record(payload):
        logs.append(payload)

    assert duplicates.are_near_duplicates(a, b, diagnostics_logger=record)
    assert logs
    assert logs[-1]["decision"] == "ACCEPT"
    assert logs[-1]["reason"] in {"distance_strong_band", "distance_weak_band"}


def test_are_near_duplicates_logs_rejection_reason():
    dt = datetime(2022, 1, 1, 0, 0, 0)
    a = make_file("a.jpg", phash="ffff000000000000", dt=dt, resolution=(4000, 2000))
    b = make_file("b.jpg", phash="f000000000000000", dt=dt, resolution=(1000, 500))
    logs: list[dict] = []

    def record(payload):
        logs.append(payload)

    assert not duplicates.are_near_duplicates(a, b, diagnostics_logger=record)
    assert logs
    assert logs[-1]["decision"] == "REJECT"
    assert "reason" in logs[-1]
