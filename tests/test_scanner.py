
from src import scanner


def test_scan_paths_filters_supported(tmp_path):
    root = tmp_path / "photos"
    root.mkdir()
    jpg = root / "a.jpg"
    jpg.write_bytes(b"123")
    txt = root / "note.txt"
    txt.write_text("ignore")

    res = scanner.scan_paths([str(root)])
    assert len(res) == 1
    fi = res[0]
    assert fi.path.endswith("a.jpg")
    assert fi.size == 3
    assert fi.format == "jpg"
    assert fi.sha256 is None and fi.phash is None
    assert fi.resolution is None


def test_scan_paths_skips_unreadable_files(monkeypatch, tmp_path):
    root = tmp_path / "photos"
    root.mkdir()
    good = root / "good.jpg"
    bad = root / "bad.jpg"
    good.write_bytes(b"ok")
    bad.write_bytes(b"x")

    original_getsize = scanner.os.path.getsize

    def fake_getsize(path):
        if path.endswith("bad.jpg"):
            raise OSError("boom")
        return original_getsize(path)

    logged: list[str] = []

    def fake_log(message: str):
        logged.append(message)

    monkeypatch.setattr(scanner, "log_error", fake_log)
    monkeypatch.setattr(scanner.os.path, "getsize", fake_getsize)

    res = scanner.scan_paths([str(root)])

    assert len(res) == 1
    assert res[0].path.endswith("good.jpg")
    assert any("bad.jpg" in entry for entry in logged)
