from datetime import datetime
from unittest.mock import patch

import pytest
from PIL import Image

from src import metadata
from src.exceptions import MetadataError, OversizedImageError
from src.models.fileinfo import FileInfo


def test_enrich_metadata_sets_resolution_and_fallback_datetime(tmp_path):
    img_path = tmp_path / "img.jpg"
    Image.new("RGB", (10, 20)).save(img_path)

    fi = FileInfo(
        path=str(img_path),
        size=img_path.stat().st_size,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
        timestamp_reliable=False,
    )

    enriched = metadata.enrich_metadata([fi])[0]
    assert enriched.resolution == (10, 20)
    assert isinstance(enriched.exif_datetime, datetime)
    assert enriched.timestamp_reliable is False


def test_enrich_metadata_marks_timestamp_reliable_with_exif(tmp_path, monkeypatch):
    img_path = tmp_path / "exif.jpg"
    Image.new("RGB", (4, 4)).save(img_path)
    fi = FileInfo(
        path=str(img_path),
        size=img_path.stat().st_size,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
        timestamp_reliable=False,
    )

    def fake_extract_exif(path: str):
        return {"datetime": datetime(2020, 1, 1, 12, 0, 0)}

    monkeypatch.setattr(metadata, "extract_exif", fake_extract_exif)

    enriched = metadata.enrich_metadata([fi])[0]
    assert enriched.timestamp_reliable is True
    assert enriched.exif_datetime.year == 2020


def test_enrich_metadata_skips_corrupted_image(tmp_path):
    bad = tmp_path / "bad.jpg"
    bad.write_bytes(b"not-an-image")
    fi = FileInfo(
        path=str(bad),
        size=bad.stat().st_size,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
        timestamp_reliable=False,
    )
    enriched = metadata.enrich_metadata([fi])
    # Corrupted file should be logged and fall back to modified timestamp, not raise
    assert len(enriched) == 1
    assert enriched[0].exif_datetime is not None
    assert enriched[0].timestamp_reliable is False


def test_extract_resolution_handles_decompression_bomb(tmp_path):
    img_path = tmp_path / "bomb.jpg"
    img_path.touch()

    with patch("src.reporting.write_log") as mock_write_log:
        with patch("PIL.Image.open", side_effect=Image.DecompressionBombError("DOS attack")):
            with pytest.raises(MetadataError):
                metadata.extract_resolution(str(img_path))

    mock_write_log.assert_called_once()
    logged_message = mock_write_log.call_args[0][0][0]
    assert "[WARNING]" in logged_message
    assert "Skipped resolution extraction" in logged_message


def test_enrich_metadata_skips_oversized_images(tmp_path, monkeypatch):
    img_path = tmp_path / "oversized.jpg"
    img_path.write_bytes(b"data")
    fi = FileInfo(
        path=str(img_path),
        size=img_path.stat().st_size,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
        timestamp_reliable=False,
    )

    def raise_bomb(_path: str):
        raise OversizedImageError("bomb")

    monkeypatch.setattr(metadata, "extract_resolution", raise_bomb)
    monkeypatch.setattr(metadata, "extract_exif", lambda _: {})

    enriched = metadata.enrich_metadata([fi])
    assert enriched == []


def _serialize_metadata(files):
    serialized = []
    for fi in files:
        serialized.append(
            (
                fi.path,
                fi.resolution,
                fi.exif_camera,
                fi.exif_orientation,
                fi.exif_datetime.isoformat() if fi.exif_datetime else None,
            )
        )
    return serialized


def _build_metadata_inputs(paths):
    result = []
    for path in paths:
        result.append(
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
    return result


def test_enrich_metadata_deterministic_across_runs(tmp_path):
    paths = []
    for idx in range(3):
        image_path = tmp_path / f"photo_{idx}.jpg"
        Image.new("RGB", (16 + idx, 24 + idx)).save(image_path)
        paths.append(image_path)

    inputs_first = _build_metadata_inputs(paths)
    inputs_second = _build_metadata_inputs(paths)

    first = metadata.enrich_metadata(inputs_first)
    second = metadata.enrich_metadata(inputs_second)

    assert _serialize_metadata(first) == _serialize_metadata(second)


def test_enrich_metadata_keeps_file_when_timestamp_unavailable(tmp_path, monkeypatch):
    img_path = tmp_path / "no_ts.jpg"
    Image.new("RGB", (8, 8)).save(img_path)

    fi = FileInfo(
        path=str(img_path),
        size=img_path.stat().st_size,
        format="jpg",
        resolution=None,
        exif_datetime=None,
        exif_gps=None,
        exif_camera=None,
        exif_orientation=None,
        sha256=None,
        phash=None,
        is_raw=False,
        timestamp_reliable=False,
    )

    class InlineExecutor:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def map(self, func, iterable):
            return map(func, iterable)

        def __exit__(self, exc_type, exc, tb):
            return False

    def failing_timestamp(path: str):
        raise MetadataError("stat failed")

    monkeypatch.setattr(metadata, "ProcessPoolExecutor", InlineExecutor)
    monkeypatch.setattr(metadata, "safe_modified_timestamp", failing_timestamp)

    enriched = metadata.enrich_metadata([fi])

    assert len(enriched) == 1
    assert enriched[0].path == str(img_path)
    assert enriched[0].exif_datetime is None
    assert enriched[0].timestamp_reliable is False
