import json
import os
import shutil
from datetime import datetime
from unittest.mock import patch

import pytest

from src import duplicates, hashing, merge_engine
from src.exceptions import MergeExecutionError, MergePlanError, StorageError, UndoSafetyError
from src.models.actions import (
    CreateFolderAction,
    MoveMasterAction,
    MoveToQuarantineExactAction,
)
from src.models.fileinfo import FileInfo
from src.models.mergeplan import MergePlan


def make_file(path, content: bytes, dt: datetime | None = None, phash: str | None = None):
    path.write_bytes(content)
    sha = hashing.compute_sha256(str(path))
    return FileInfo(
        path=str(path),
        size=len(content),
        format="jpg",
        resolution=(10, 10),
        exif_datetime=dt,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=sha,
        phash=phash,
        is_raw=False,
    )


def test_build_and_execute_merge_plan(tmp_path, monkeypatch):
    src_dir = tmp_path / "src"
    dst_dir = tmp_path / "dst"
    src_dir.mkdir()
    monkeypatch.chdir(tmp_path)
    dt = datetime(2021, 1, 1)

    f1 = make_file(src_dir / "a1.jpg", b"aaa", dt=dt, phash="aaaa")
    f2 = make_file(src_dir / "a2.jpg", b"aaa", dt=dt, phash="aaab")
    f3 = make_file(src_dir / "b1.jpg", b"bbb", dt=dt, phash="cccc")
    f4 = make_file(src_dir / "b2.jpg", b"ccc", dt=dt, phash="cccd")

    files = [f1, f2, f3, f4]
    clusters = duplicates.group_duplicates(files, sensitivity="balanced")

    plan = merge_engine.build_merge_plan(files, clusters, str(dst_dir))
    # required_space counts masters and quarantined exact duplicates
    assert plan.required_space == f1.size + f2.size + f3.size

    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    dedupe_report = artifacts_dir / "dedupe_report.html"
    dedupe_report.write_text("stale")
    merge_report = artifacts_dir / "merge_report.html"
    if merge_report.exists():
        merge_report.unlink()

    # Dry-run should not create reports or move files
    merge_engine.dry_run(plan)
    assert not (tmp_path / "near_duplicates.html").exists()
    # Ensure no folders were created during dry-run
    assert not dst_dir.exists()
    assert not (tmp_path / "dst/QUARANTINE_EXACT").exists()

    merge_engine.execute_merge(plan)

    # Masters moved into organized structure
    moved_files = list(dst_dir.rglob("*.jpg"))
    assert moved_files

    # Redundant exact goes to QUARANTINE_EXACT
    quarantine_file = dst_dir / "QUARANTINE_EXACT" / "a2.jpg"
    assert quarantine_file.exists()

    # Near-duplicate redundant should remain in source
    assert (src_dir / "b2.jpg").exists()
    assert merge_report.exists()
    assert "<title>Nolossia Merge Report" in merge_report.read_text()
    assert not dedupe_report.exists()
    manifest_json = artifacts_dir / "source_manifest.json"
    manifest_csv = artifacts_dir / "source_manifest.csv"
    manifest_html = artifacts_dir / "source_manifest.html"
    assert manifest_json.exists()
    assert manifest_csv.exists()
    assert manifest_html.exists()
    manifest_payload = json.loads(manifest_json.read_text())
    assert manifest_payload["schema_version"] == "1.0"
    assert manifest_payload["entries"]
    first_entry = manifest_payload["entries"][0]
    assert {"original_path", "original_folder", "new_path", "hash", "batch_id"} <= set(first_entry.keys())


def test_build_merge_plan_rejects_non_chron_destination(tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "img.jpg").write_bytes(b"123")
    fi = make_file(src_dir / "img.jpg", b"123", dt=datetime(2021, 1, 1), phash="abcd")
    dest = tmp_path / "dest"
    dest.mkdir()
    # Non-chron structure (random folder)
    (dest / "misc").mkdir()

    with pytest.raises(MergePlanError):
        merge_engine.build_merge_plan([fi], [], str(dest))


def test_validate_destination_accepts_valid_structure(tmp_path):
    dest = tmp_path / "dest"
    (dest / "2024" / "2024-01").mkdir(parents=True)
    try:
        merge_engine.validate_destination(str(dest))
    except MergePlanError:
        pytest.fail("validate_destination unexpectedly rejected a valid structure.")


def test_validate_destination_rejects_invalid_year_month(tmp_path):
    dest = tmp_path / "dest"
    (dest / "2024" / "2024-13").mkdir(parents=True)
    with pytest.raises(MergePlanError, match="2024-13"):
        merge_engine.validate_destination(str(dest))


def test_validate_destination_rejects_month_only_folder(tmp_path):
    dest = tmp_path / "dest"
    (dest / "2024" / "13").mkdir(parents=True)
    with pytest.raises(MergePlanError, match="13"):
        merge_engine.validate_destination(str(dest))


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks unsupported on this platform")
def test_validate_destination_rejects_symlink_year_folder(tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    os.symlink(outside, dest / "2024")
    with pytest.raises(MergePlanError, match="symlink"):
        merge_engine.validate_destination(str(dest))


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks unsupported on this platform")
def test_validate_destination_rejects_symlink_month_folder(tmp_path):
    dest = tmp_path / "dest"
    month_target = tmp_path / "outside" / "2024-01"
    month_target.mkdir(parents=True)
    year_dir = dest / "2024"
    year_dir.mkdir(parents=True)
    os.symlink(month_target, year_dir / "2024-01")
    with pytest.raises(MergePlanError, match="symlink"):
        merge_engine.validate_destination(str(dest))


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks unsupported on this platform")
def test_validate_destination_rejects_symlink_root(tmp_path):
    real = tmp_path / "real"
    (real / "2024" / "2024-01").mkdir(parents=True)
    link = tmp_path / "link"
    os.symlink(real, link)
    with pytest.raises(MergePlanError, match="symlink"):
        merge_engine.validate_destination(str(link))


def test_execute_merge_handles_collision_suffix(tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dst_dir = tmp_path / "library"
    conflict_dir = dst_dir / "2024" / "2024-01"
    conflict_dir.mkdir(parents=True)
    existing = conflict_dir / "photo.jpg"
    existing.write_bytes(b"original")
    src = src_dir / "photo.jpg"
    payload = b"newdata"
    src.write_bytes(payload)
    src_hash = hashing.compute_sha256(str(src))

    plan = MergePlan(
        required_space=src.stat().st_size,
        destination_free=10**9,
        duplicate_count=0,
        total_files=1,
        actions=[
            CreateFolderAction(path=str(conflict_dir)),
            MoveMasterAction(
                src=str(src),
                dst=str(existing),
                sha256=src_hash,
                size=src.stat().st_size,
            ),
        ],
        destination_path=str(dst_dir),
    )

    metadata = merge_engine.execute_merge(plan)

    renamed = conflict_dir / f"photo-{src_hash}.jpg"
    assert renamed.exists()
    assert renamed.read_bytes() == payload
    assert existing.read_bytes() == b"original"
    moved_action = next(a for a in plan.actions if isinstance(a, MoveMasterAction))
    assert moved_action.dst == str(renamed)
    assert metadata["renamed"] == [(str(existing), str(renamed))]


def test_execute_merge_aborts_on_hash_mismatch(tmp_path):
    src = tmp_path / "a.jpg"
    src.write_bytes(b"data")
    dst_dir = tmp_path / "out"
    dst_path = dst_dir / "a.jpg"

    plan = MergePlan(
        required_space=src.stat().st_size,
        destination_free=10**9,
        duplicate_count=0,
        total_files=1,
        actions=[
            CreateFolderAction(path=str(dst_dir)),
            MoveMasterAction(
                src=str(src),
                dst=str(dst_path),
                sha256="0" * 64,  # incorrect hash to force mismatch
                size=src.stat().st_size,
            ),
        ],
        destination_path=str(dst_dir),
    )

    with pytest.raises(MergeExecutionError):
        merge_engine.execute_merge(plan)


def test_execute_merge_quarantine_rehash_mismatch_fails(tmp_path):
    src = tmp_path / "a.jpg"
    src.write_bytes(b"data")
    dst_dir = tmp_path / "out"
    dst_path = dst_dir / "a.jpg"
    original_hash = "original_hash"
    corrupted_hash = "corrupted_hash"

    plan = MergePlan(
        required_space=src.stat().st_size,
        destination_free=10**9,
        duplicate_count=1,
        total_files=1,
        actions=[
            CreateFolderAction(path=str(dst_dir)),
            MoveToQuarantineExactAction(
                src=str(src),
                dst=str(dst_path),
                sha256=original_hash,
                size=src.stat().st_size,
            ),
        ],
        destination_path=str(dst_dir),
    )

    with patch("src.merge_engine.compute_sha256", return_value=corrupted_hash):
        with pytest.raises(MergeExecutionError, match="Merge execution failed"):
            merge_engine.execute_merge(plan)


def test_execute_merge_quarantine_rehash_match_succeeds(tmp_path):
    src = tmp_path / "a.jpg"
    src.write_bytes(b"data")
    dst_dir = tmp_path / "out"
    dst_path = dst_dir / "a.jpg"
    original_hash = "original_hash"

    plan = MergePlan(
        required_space=src.stat().st_size,
        destination_free=10**9,
        duplicate_count=1,
        total_files=1,
        actions=[
            CreateFolderAction(path=str(dst_dir)),
            MoveToQuarantineExactAction(
                src=str(src),
                dst=str(dst_path),
                sha256=original_hash,
                size=src.stat().st_size,
            ),
        ],
        destination_path=str(dst_dir),
    )

    with patch("src.merge_engine.compute_sha256", return_value=original_hash):
        try:
            merge_engine.execute_merge(plan)
        except MergeExecutionError:
            pytest.fail("MergeExecutionError raised unexpectedly on hash match.")


def test_build_merge_plan_calculates_quarantined_space(tmp_path, monkeypatch):
    src_dir = tmp_path / "src"
    dst_dir = tmp_path / "dst"
    src_dir.mkdir()
    monkeypatch.chdir(tmp_path)
    dt = datetime(2021, 1, 1)

    f1 = make_file(src_dir / "master.jpg", b"master", dt=dt, phash="aaaa")
    f2 = make_file(src_dir / "duplicate.jpg", b"master", dt=dt, phash="aaaa")  # Exact duplicate of f1
    f3 = make_file(src_dir / "unique.jpg", b"unique", dt=dt, phash="ffff")

    files = [f1, f2, f3]
    clusters = duplicates.group_duplicates(files, sensitivity="balanced")  # f1 and f2 should be in a cluster

    plan = merge_engine.build_merge_plan(files, clusters, str(dst_dir))
    
    # Expected required_space should be size of master (f1) + size of quarantined exact duplicate (f2) + size of unique file (f3)
    expected_space = f1.size + f2.size + f3.size
    assert plan.required_space == expected_space


def test_build_merge_plan_raises_storage_error_when_space_insufficient(tmp_path, monkeypatch):
    src_dir = tmp_path / "src"
    dst_dir = tmp_path / "dst"
    src_dir.mkdir()
    monkeypatch.chdir(tmp_path)
    dt = datetime(2021, 1, 1)

    f1 = make_file(src_dir / "master.jpg", b"master", dt=dt, phash="aaaa")
    f2 = make_file(src_dir / "duplicate.jpg", b"master", dt=dt, phash="aaaa")
    files = [f1, f2]
    clusters = duplicates.group_duplicates(files, sensitivity="balanced")

    required = f1.size + f2.size

    monkeypatch.setattr("src.merge_engine._available_space", lambda path: required - 1)

    with pytest.raises(StorageError, match="Insufficient storage"):
        merge_engine.build_merge_plan(files, clusters, str(dst_dir))


def test_execute_merge_rehashes_destination_even_without_phash(tmp_path, monkeypatch):
    src = tmp_path / "photo.jpg"
    src.write_bytes(b"original-data")
    dst_dir = tmp_path / "library"
    dst_path = dst_dir / "2021" / "2021-01" / "photo.jpg"

    sha = hashing.compute_sha256(str(src))
    file_info = FileInfo(
        path=str(src),
        size=src.stat().st_size,
        format="jpg",
        resolution=None,
        exif_datetime=datetime(2021, 1, 1),
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=sha,
        phash=None,
        is_raw=False,
    )

    plan = merge_engine.build_merge_plan([file_info], [], str(dst_dir))

    hashed_paths: list[str] = []

    def fake_compute_sha256(path: str) -> str:
        hashed_paths.append(path)
        return sha

    monkeypatch.setattr("src.merge_engine.compute_sha256", fake_compute_sha256)

    merge_engine.execute_merge(plan)

    assert str(dst_path) in hashed_paths


def _write_manifest(path, *, batch_id, original_path, new_path, sha256):
    payload = {
        "schema_version": "1.0",
        "batch_id": batch_id,
        "entries": [
            {
                "original_path": str(original_path),
                "original_folder": os.path.dirname(str(original_path)),
                "new_path": str(new_path),
                "hash": sha256,
                "batch_id": batch_id,
            }
        ],
    }
    path.write_text(json.dumps(payload))


def test_execute_undo_restores_and_idempotent(tmp_path):
    src_dir = tmp_path / "src"
    lib_dir = tmp_path / "library"
    src_dir.mkdir()
    (lib_dir / "2024" / "2024-01").mkdir(parents=True)
    original = src_dir / "photo.jpg"
    original.write_bytes(b"undo-me")
    sha = hashing.compute_sha256(str(original))
    new_path = lib_dir / "2024" / "2024-01" / "photo.jpg"
    shutil.move(str(original), str(new_path))

    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    manifest_path = artifacts / "source_manifest.json"
    batch_id = "batch123"
    _write_manifest(manifest_path, batch_id=batch_id, original_path=original, new_path=new_path, sha256=sha)

    plan = merge_engine.prepare_undo_plan(str(manifest_path), batch_id)
    summary = merge_engine.execute_undo(plan)
    assert summary["counts"]["restore"] == 1
    assert original.exists()
    assert not new_path.exists()

    plan_again = merge_engine.prepare_undo_plan(str(manifest_path), batch_id)
    summary_again = merge_engine.execute_undo(plan_again)
    assert summary_again["counts"]["restore"] == 0
    assert summary_again["counts"]["already_restored"] == 1
    assert original.exists()


def test_execute_undo_routes_conflicts(tmp_path):
    src_dir = tmp_path / "src"
    lib_dir = tmp_path / "library"
    src_dir.mkdir()
    (lib_dir / "2024" / "2024-02").mkdir(parents=True)
    original = src_dir / "photo.jpg"
    original.write_bytes(b"conflict-source")
    sha = hashing.compute_sha256(str(original))
    new_path = lib_dir / "2024" / "2024-02" / "photo.jpg"
    shutil.move(str(original), str(new_path))
    original.write_bytes(b"occupied")

    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    manifest_path = artifacts / "source_manifest.json"
    batch_id = "batch456"
    _write_manifest(manifest_path, batch_id=batch_id, original_path=original, new_path=new_path, sha256=sha)

    plan = merge_engine.prepare_undo_plan(str(manifest_path), batch_id)
    summary = merge_engine.execute_undo(plan)
    conflict_root = lib_dir / "REVIEW" / "UNDO_CONFLICTS" / batch_id
    conflict_files = list(conflict_root.rglob("photo*.jpg"))
    assert conflict_files
    assert original.read_bytes() == b"occupied"
    assert not new_path.exists()
    assert summary["counts"]["conflict"] == 1


def test_execute_undo_hash_mismatch_blocks(tmp_path):
    src_dir = tmp_path / "src"
    lib_dir = tmp_path / "library"
    src_dir.mkdir()
    (lib_dir / "2024" / "2024-03").mkdir(parents=True)
    original = src_dir / "photo.jpg"
    original.write_bytes(b"bad-hash")
    sha = hashing.compute_sha256(str(original))
    new_path = lib_dir / "2024" / "2024-03" / "photo.jpg"
    shutil.move(str(original), str(new_path))

    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    manifest_path = artifacts / "source_manifest.json"
    batch_id = "batch789"
    _write_manifest(
        manifest_path,
        batch_id=batch_id,
        original_path=original,
        new_path=new_path,
        sha256=sha[::-1],
    )

    plan = merge_engine.prepare_undo_plan(str(manifest_path), batch_id)
    with pytest.raises(UndoSafetyError):
        merge_engine.execute_undo(plan)
    assert new_path.exists()
