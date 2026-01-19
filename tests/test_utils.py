import os

import pytest

from src import hashing, utils
from src.exceptions import NolossiaError


def test_osc8_link_format():
    link = utils.osc8_link("/tmp/file", "Label")
    assert link.startswith("\033]8;;file:///tmp/file\aLabel\033]8;;\a")
    # default label falls back to path
    default_link = utils.osc8_link("/tmp/other")
    assert "/tmp/other" in default_link
    assert "Label" not in default_link


def test_safe_move_no_collision(tmp_path):
    src_file = tmp_path / "source.txt"
    dst_file = tmp_path / "destination.txt"
    src_file.write_text("hello")

    result = utils.safe_move(str(src_file), str(dst_file), allowed_root=str(tmp_path))

    assert not src_file.exists()
    assert dst_file.exists()
    assert dst_file.read_text() == "hello"
    assert result == str(dst_file)


def test_safe_move_identical_file_overwrites(tmp_path):
    src_file = tmp_path / "source.txt"
    dst_file = tmp_path / "destination.txt"
    src_file.write_text("same content")
    dst_file.write_text("same content") # Identical content

    result = utils.safe_move(str(src_file), str(dst_file), allowed_root=str(tmp_path))

    assert not src_file.exists()
    assert dst_file.exists()
    assert dst_file.read_text() == "same content"
    assert result == str(dst_file)


def test_safe_move_different_file_suffixes_name(tmp_path):
    src_file = tmp_path / "source.txt"
    dst_file = tmp_path / "destination.txt"
    src_file.write_text("source content")
    dst_file.write_text("destination content") # Different content

    src_hash = hashing.compute_sha256(str(src_file))
    expected_dst_file = tmp_path / f"destination-{src_hash}.txt"

    result = utils.safe_move(str(src_file), str(dst_file), allowed_root=str(tmp_path))

    assert not src_file.exists()
    assert dst_file.exists() # Original destination should remain untouched
    assert dst_file.read_text() == "destination content"
    assert expected_dst_file.exists()
    assert expected_dst_file.read_text() == "source content"
    assert result == str(expected_dst_file)


def test_safe_move_logs_collision_suffix(tmp_path, monkeypatch):
    src_file = tmp_path / "source.txt"
    dst_file = tmp_path / "destination.txt"
    src_file.write_text("source content")
    dst_file.write_text("destination content")
    src_hash = hashing.compute_sha256(str(src_file))
    log_entries: list[str] = []

    def fake_write_log(entries, outfile="nolossia.log"):
        log_entries.extend(entries)

    monkeypatch.setattr("src.reporting.write_log", fake_write_log)

    utils.safe_move(str(src_file), str(dst_file), allowed_root=str(tmp_path))

    assert any("Filename collision" in entry for entry in log_entries)
    assert any(src_hash[:12] in entry for entry in log_entries)


def test_safe_move_unresolvable_collision(tmp_path, monkeypatch):
    src_file = tmp_path / "source.txt"
    dst_file = tmp_path / "destination.txt"

    src_file.write_text("source content")
    dst_file.write_text("different content")
    
    # Manually create the file that would cause an unresolvable collision
    src_hash = hashing.compute_sha256(str(src_file))
    unresolvable_dst = tmp_path / f"destination-{src_hash}.txt"
    unresolvable_dst.write_text("another different content")

    logged: list[str] = []

    def fake_write_log(entries, outfile="nolossia.log"):
        logged.extend(entries)

    monkeypatch.setattr("src.reporting.write_log", fake_write_log)

    with pytest.raises(NolossiaError, match="Unresolvable collision"):
        utils.safe_move(str(src_file), str(dst_file), allowed_root=str(tmp_path))
    
    # Assert that source file still exists and destination remains untouched
    assert src_file.exists()
    assert dst_file.exists()
    assert unresolvable_dst.exists()
    assert any("Unresolvable collision" in entry for entry in logged)


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks unsupported on this platform")
def test_safe_move_rejects_symlink_directory(tmp_path):
    base = tmp_path / "library"
    real_year = tmp_path / "real_year"
    real_year.mkdir(parents=True)
    base.mkdir()
    year_link = base / "2024"
    os.symlink(real_year, year_link)
    src_file = tmp_path / "source.jpg"
    src_file.write_text("payload")
    dst = year_link / "2024-01" / "photo.jpg"
    with pytest.raises(NolossiaError, match="symlink"):
        utils.safe_move(str(src_file), str(dst), allowed_root=str(base))
    assert src_file.exists()


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks unsupported on this platform")
def test_safe_move_rejects_escape_outside_root(tmp_path):
    base = tmp_path / "library"
    base.mkdir()
    src_file = tmp_path / "src.bin"
    src_file.write_text("payload")
    outside = tmp_path / "outside" / "target.bin"
    outside.parent.mkdir(parents=True)
    with pytest.raises(NolossiaError, match="escapes destination"):
        utils.safe_move(str(src_file), str(outside), allowed_root=str(base))
    assert src_file.exists()


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks unsupported on this platform")
def test_safe_move_rejects_symlink_target_file(tmp_path):
    base = tmp_path / "library"
    target_dir = base / "2024" / "2024-01"
    target_dir.mkdir(parents=True)
    src = tmp_path / "source.jpg"
    src.write_text("payload")
    outside = tmp_path / "outside" / "file.jpg"
    outside.parent.mkdir(parents=True)
    outside.write_text("existing")
    link = target_dir / "photo.jpg"
    os.symlink(outside, link)
    with pytest.raises(NolossiaError, match="symlink"):
        utils.safe_move(str(src), str(link), allowed_root=str(base))
    assert src.exists()


def test_configure_pixel_limit_cli_override(monkeypatch):
    monkeypatch.delenv(utils.PIXEL_LIMIT_ENV, raising=False)
    utils.configure_pixel_limit(None)
    limit, source = utils.configure_pixel_limit(utils.DEFAULT_PIXEL_LIMIT + 1_000_000)
    assert source == "cli"
    assert limit == utils.DEFAULT_PIXEL_LIMIT + 1_000_000
    utils.configure_pixel_limit(None)


def test_configure_pixel_limit_env_override(monkeypatch):
    monkeypatch.setenv(utils.PIXEL_LIMIT_ENV, str(utils.DEFAULT_PIXEL_LIMIT + 2_000_000))
    limit, source = utils.configure_pixel_limit(None)
    assert source == "env"
    assert limit == utils.DEFAULT_PIXEL_LIMIT + 2_000_000
    monkeypatch.delenv(utils.PIXEL_LIMIT_ENV, raising=False)
    utils.configure_pixel_limit(None)


def _reset_executor_state(monkeypatch):
    monkeypatch.setattr(utils, "_EXECUTOR_MODE", None)
    monkeypatch.setattr(utils, "_EXECUTOR_SOURCE", None)
    monkeypatch.setattr(utils, "_EXECUTOR_LOGGED", False)
    monkeypatch.setattr(utils, "_PROCESS_POOL_SUPPORTED", None)


def test_configure_executor_mode_auto_fallback(monkeypatch):
    _reset_executor_state(monkeypatch)
    monkeypatch.delenv(utils.EXECUTOR_ENV, raising=False)
    monkeypatch.setattr(utils, "_supports_process_pool", lambda: False)
    mode, source = utils.configure_executor_mode(None)
    assert mode == "thread"
    assert source == "auto"


def test_configure_executor_mode_env_thread(monkeypatch):
    _reset_executor_state(monkeypatch)
    monkeypatch.setenv(utils.EXECUTOR_ENV, "thread")
    monkeypatch.setattr(utils, "_supports_process_pool", lambda: True)
    mode, source = utils.configure_executor_mode(None)
    assert mode == "thread"
    assert source == "env"
    monkeypatch.delenv(utils.EXECUTOR_ENV, raising=False)


def test_configure_executor_mode_env_process_fallback(monkeypatch):
    _reset_executor_state(monkeypatch)
    monkeypatch.setenv(utils.EXECUTOR_ENV, "process")
    monkeypatch.setattr(utils, "_supports_process_pool", lambda: False)
    mode, source = utils.configure_executor_mode(None)
    assert mode == "thread"
    assert source == "env"
    monkeypatch.delenv(utils.EXECUTOR_ENV, raising=False)
