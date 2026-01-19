import os
from datetime import datetime

import pytest

from src import organizer
from src.exceptions import NolossiaError
from src.models.fileinfo import FileInfo
from src.review import REVIEW_REASON_MISSING_EXIF, REVIEW_REASON_UNRELIABLE_TIMESTAMP


def make_file(dt=None, path: str | None = None, reliable: bool = True):
    return FileInfo(
        path=path or "/tmp/photo.jpg",
        size=1,
        format="jpg",
        resolution=None,
        exif_datetime=dt,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
        timestamp_reliable=reliable,
    )


def test_determine_target_path_with_date():
    fi = make_file(datetime(2021, 5, 4))
    target = organizer.determine_target_path(fi, "/library", merge_mode="on")
    assert "2021/2021-05" in target
    assert target.endswith("photo.jpg")


def test_determine_target_path_without_date_goes_to_review():
    fi = make_file(None)
    target = organizer.determine_target_path(fi, "/library", merge_mode="on")
    assert "REVIEW" in target
    assert fi.review_reason == REVIEW_REASON_MISSING_EXIF


def test_determine_target_path_preserves_hierarchy_when_off(tmp_path):
    source_root = tmp_path / "src"
    nested = source_root / "subdir"
    nested.mkdir(parents=True)
    file_path = nested / "keep.jpg"
    file_path.write_bytes(b"123")
    fi = FileInfo(
        path=str(file_path),
        size=3,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
    )
    target = organizer.determine_target_path(fi, "/library", merge_mode="off", source_root=str(source_root))
    assert target.endswith("subdir/keep.jpg")


def test_determine_target_path_existing_year_month_is_authoritative(tmp_path):
    source_root = tmp_path / "photos" / "2023" / "2023-05"
    source_root.mkdir(parents=True)
    img = source_root / "pic.jpg"
    img.write_bytes(b"1")
    fi = FileInfo(
        path=str(img),
        size=1,
        format="jpg",
        resolution=None,
        exif_datetime=datetime(1999, 1, 1),
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
    )
    target = organizer.determine_target_path(fi, "/library", merge_mode="on", source_root=str(tmp_path / "photos"))
    assert "2023/2023-05" in target


def test_determine_target_path_with_unreliable_timestamp_routes_review():
    fi = make_file(datetime(2022, 1, 1), reliable=False)
    target = organizer.determine_target_path(fi, "/library", merge_mode="on")
    assert "REVIEW" in target
    assert fi.review_reason == REVIEW_REASON_UNRELIABLE_TIMESTAMP


def test_existing_chronology_with_missing_exif_still_review(tmp_path):
    source_root = tmp_path / "photos" / "2023" / "2023-05"
    source_root.mkdir(parents=True)
    img = source_root / "pic.jpg"
    img.write_bytes(b"1")
    fi = FileInfo(
        path=str(img),
        size=1,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
    )
    target = organizer.determine_target_path(fi, "/library", merge_mode="on", source_root=str(tmp_path / "photos"))
    assert "REVIEW" in target
    assert fi.review_reason == REVIEW_REASON_MISSING_EXIF


def test_determine_target_path_review_when_no_date_and_no_pattern(tmp_path):
    img = tmp_path / "random" / "file.jpg"
    img.parent.mkdir(parents=True)
    img.write_bytes(b"1")
    fi = FileInfo(
        path=str(img),
        size=1,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
    )
    target = organizer.determine_target_path(fi, "/library", merge_mode="on", source_root=str(tmp_path / "random"))
    assert "REVIEW" in target


def test_invalid_existing_year_month_routes_to_review(tmp_path, monkeypatch):
    log_entries: list[str] = []

    def fake_log(entries, outfile="nolossia.log"):
        log_entries.extend(entries)

    monkeypatch.setattr("src.reporting.write_log", fake_log)
    invalid_dir = tmp_path / "photos" / "2022" / "2022-13"
    invalid_dir.mkdir(parents=True)
    img = invalid_dir / "bad.jpg"
    img.write_bytes(b"1")
    fi = make_file(datetime(2022, 5, 1), path=str(img))

    target = organizer.determine_target_path(fi, "/library", merge_mode="on")
    assert "REVIEW" in target
    assert any("WARNING" in entry and "2022-13" in entry for entry in log_entries)


def test_invalid_month_only_folder_routes_to_review(tmp_path, monkeypatch):
    log_entries: list[str] = []

    def fake_log(entries, outfile="nolossia.log"):
        log_entries.extend(entries)

    monkeypatch.setattr("src.reporting.write_log", fake_log)
    invalid_dir = tmp_path / "photos" / "2021" / "13"
    invalid_dir.mkdir(parents=True)
    img = invalid_dir / "bad2.jpg"
    img.write_bytes(b"1")
    fi = make_file(datetime(2021, 7, 1), path=str(img))

    target = organizer.determine_target_path(fi, "/library", merge_mode="on")
    assert "REVIEW" in target
    assert any("WARNING" in entry and "13" in entry for entry in log_entries)


def test_ensure_structure_off_creates_base(tmp_path):
    base = tmp_path / "out"
    organizer.ensure_structure(str(base), "off")
    assert base.exists()
    assert not list(base.iterdir())


def test_ensure_structure_on_creates_year_month(tmp_path):
    base = tmp_path / "out"
    year_month = base / "2022" / "2022-01"
    organizer.ensure_structure(str(base), "on", folders=[str(year_month)])
    assert base.exists()
    assert year_month.exists()


def test_ensure_structure_rejects_folders_outside_destination(tmp_path):
    base = tmp_path / "out"
    outside = tmp_path / "other" / "2022" / "2022-01"
    with pytest.raises(NolossiaError):
        organizer.ensure_structure(str(base), "on", folders=[str(outside)])


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks unsupported on this platform")
def test_ensure_structure_rejects_symlink_targets(tmp_path):
    base = tmp_path / "out"
    base.mkdir()
    real_year = tmp_path / "real_year"
    real_year.mkdir()
    year_link = base / "2024"
    os.symlink(real_year, year_link)
    target_folder = year_link / "2024-01"
    with pytest.raises(NolossiaError, match="symlink"):
        organizer.ensure_structure(str(base), "on", folders=[str(target_folder)])


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks unsupported on this platform")
def test_ensure_structure_rejects_symlink_base(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    os.symlink(real, link)
    target_folder = link / "2024" / "2024-01"
    with pytest.raises(NolossiaError, match="symlink"):
        organizer.ensure_structure(str(link), "on", folders=[str(target_folder)])
