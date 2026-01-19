from unittest.mock import patch

from PIL import Image

from src import hashing
from src.exceptions import HashingError
from src.models.fileinfo import FileInfo


def test_compute_sha256(tmp_path):
    f = tmp_path / "file.bin"
    f.write_bytes(b"hello")
    digest = hashing.compute_sha256(str(f))
    assert digest == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"


def test_add_hashes_populates_fields(tmp_path):
    f = tmp_path / "file.bin"
    f.write_bytes(b"world")
    fi = FileInfo(
        path=str(f),
        size=f.stat().st_size,
        format="bin",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
    )
    hashed = hashing.add_hashes([fi])[0]
    assert hashed.sha256
    assert hashed.phash is None


def test_compute_phash_is_stable(tmp_path):
    img = tmp_path / "img.png"
    Image.new("RGB", (8, 8), color="red").save(img)
    h1 = hashing.compute_phash(str(img))
    h2 = hashing.compute_phash(str(img))
    assert h1 == h2


def test_add_hashes_sets_phash_for_images(tmp_path):
    img = tmp_path / "img.png"
    Image.new("RGB", (8, 8), color="blue").save(img)
    fi = FileInfo(
        path=str(img),
        size=img.stat().st_size,
        format="png",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
    )
    hashed = hashing.add_hashes([fi])[0]
    assert hashed.sha256
    assert hashed.phash

def test_add_hashes_phash_failure_sets_none(tmp_path):
    f = tmp_path / "file.bin"
    f.write_bytes(b"world")
    fi = FileInfo(
        path=str(f),
        size=f.stat().st_size,
        format="bin",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
    )
    
    with patch("src.hashing.compute_phash", side_effect=HashingError("phash failed")):
        hashed = hashing.add_hashes([fi])[0]
        assert hashed.sha256 is not None
        assert hashed.phash is None


def _serialize_hashed(files):
    return [(fi.path, fi.sha256, fi.phash) for fi in files]


def _build_fileinfos(paths):
    fileinfos = []
    for path in paths:
        fileinfos.append(
            FileInfo(
                path=str(path),
                size=path.stat().st_size,
                format=path.suffix.lstrip("."),
                resolution=None,
                exif_datetime=None,
                exif_gps=None,
                exif_camera=None,
                exif_orientation=None,
                sha256=None,
                phash=None,
                is_raw=False,
            )
        )
    return fileinfos


def test_add_hashes_deterministic_across_runs(tmp_path):
    paths = []
    for idx, color in enumerate(("red", "green", "blue")):
        image_path = tmp_path / f"img_{idx}.png"
        Image.new("RGB", (8 + idx, 8 + idx), color=color).save(image_path)
        paths.append(image_path)

    inputs_first = _build_fileinfos(paths)
    inputs_second = _build_fileinfos(paths)

    first_result = hashing.add_hashes(inputs_first)
    second_result = hashing.add_hashes(inputs_second)

    assert _serialize_hashed(first_result) == _serialize_hashed(second_result)


def test_add_hashes_skips_decompression_bomb(tmp_path, monkeypatch):
    img = tmp_path / "oversized.png"
    img.write_bytes(b"data")
    fi = FileInfo(
        path=str(img),
        size=img.stat().st_size,
        format="png",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
    )

    def raise_bomb(*args, **kwargs):
        raise Image.DecompressionBombError("bomb")

    monkeypatch.setattr("src.hashing.Image.open", raise_bomb)

    hashed = hashing.add_hashes([fi])
    assert len(hashed) == 1
    assert hashed[0].sha256 is not None
    assert hashed[0].phash is None
