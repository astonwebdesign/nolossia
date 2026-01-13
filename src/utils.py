"""
Module: utils
Purpose: Shared helper utilities for Nolossia.
"""

import os
import shutil
import urllib.parse
from concurrent.futures import ProcessPoolExecutor
from typing import Tuple

from .exceptions import NolossiaError

DEFAULT_PIXEL_LIMIT = 50_000_000  # ≤50 MP safety default
MAX_OVERRIDE_LIMIT = 90_000_000   # Hard cap for expert override
PIXEL_LIMIT_ENV = "NOLOSSIA_MAX_PIXELS"
EXECUTOR_ENV = "NOLOSSIA_EXECUTOR"
_PIXEL_LIMIT = DEFAULT_PIXEL_LIMIT
_PIXEL_LIMIT_SOURCE = "default"
_EXECUTOR_MODE: str | None = None
_EXECUTOR_SOURCE: str | None = None
_EXECUTOR_LOGGED = False
_PROCESS_POOL_SUPPORTED: bool | None = None


def _apply_pillow_limit(limit: int) -> None:
    try:
        from PIL import Image
        Image.MAX_IMAGE_PIXELS = limit
    except Exception as exc:
        log_warning(
            f"Unable to update Pillow pixel safety limit to {limit:,} pixels: {exc}"
        )


def _validate_pixel_limit(value: int) -> int:
    if value < DEFAULT_PIXEL_LIMIT or value > MAX_OVERRIDE_LIMIT:
        raise ValueError(
            f"Pixel limit must be between {DEFAULT_PIXEL_LIMIT:,} and {MAX_OVERRIDE_LIMIT:,}."
        )
    return value


def configure_pixel_limit(cli_override: int | None = None) -> tuple[int, str]:
    """
    Determine and apply the effective Pillow pixel limit.
    Preference order: CLI override > environment variable > default.
    Returns tuple of (limit, source).
    """
    global _PIXEL_LIMIT, _PIXEL_LIMIT_SOURCE
    source = "default"
    limit = DEFAULT_PIXEL_LIMIT

    if cli_override is not None:
        limit = _validate_pixel_limit(cli_override)
        source = "cli"
    else:
        env_value = os.getenv(PIXEL_LIMIT_ENV)
        if env_value:
            try:
                parsed = _validate_pixel_limit(int(env_value))
                limit = parsed
                source = "env"
            except ValueError:
                log_warning(
                    f"Ignoring invalid {PIXEL_LIMIT_ENV} value '{env_value}'. "
                    f"Expected integer between {DEFAULT_PIXEL_LIMIT} and {MAX_OVERRIDE_LIMIT}."
                )

    if source == "cli":
        os.environ[PIXEL_LIMIT_ENV] = str(limit)

    _PIXEL_LIMIT = limit
    _PIXEL_LIMIT_SOURCE = source
    _apply_pillow_limit(limit)
    return limit, source


def current_pixel_limit() -> int:
    return _PIXEL_LIMIT


def enforce_pixel_limit() -> None:
    try:
        from PIL import Image
    except Exception:
        return
    limit = current_pixel_limit()
    if Image.MAX_IMAGE_PIXELS != limit:
        Image.MAX_IMAGE_PIXELS = limit


def ensure_heif_registered() -> None:
    try:
        from pillow_heif import register_heif_opener
    except Exception:
        return
    try:
        register_heif_opener()
    except Exception as exc:
        log_error(f"HEIF registration failed: {exc}")


def pixel_limit_source() -> str:
    return _PIXEL_LIMIT_SOURCE


def _process_pool_probe() -> int:
    return 1


def _supports_process_pool() -> bool:
    global _PROCESS_POOL_SUPPORTED
    if _PROCESS_POOL_SUPPORTED is not None:
        return _PROCESS_POOL_SUPPORTED
    try:
        with ProcessPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_process_pool_probe)
            future.result(timeout=2)
        _PROCESS_POOL_SUPPORTED = True
    except Exception:
        _PROCESS_POOL_SUPPORTED = False
    return _PROCESS_POOL_SUPPORTED


def configure_executor_mode(cli_override: str | None = None) -> tuple[str, str]:
    """
    Determine executor mode for hashing/metadata.
    Preference order: CLI override > environment variable > auto.
    Returns tuple of (mode, source), where mode is "process" or "thread".
    """
    global _EXECUTOR_MODE, _EXECUTOR_SOURCE, _EXECUTOR_LOGGED
    source = "auto"
    requested = "auto"
    if cli_override:
        requested = cli_override.lower()
        source = "cli"
    else:
        env_value = os.getenv(EXECUTOR_ENV)
        if env_value:
            requested = env_value.lower()
            source = "env"

    if requested not in {"auto", "process", "thread"}:
        log_warning(
            f"Ignoring invalid {EXECUTOR_ENV} value '{requested}'. Expected auto, process, or thread."
        )
        requested = "auto"
        source = "auto"

    if requested == "process":
        if _supports_process_pool():
            mode = "process"
        else:
            log_warning(
                "ProcessPool unavailable; falling back to ThreadPool for executor selection."
            )
            mode = "thread"
    elif requested == "thread":
        mode = "thread"
    else:
        mode = "process" if _supports_process_pool() else "thread"

    _EXECUTOR_MODE = mode
    _EXECUTOR_SOURCE = source
    if not _EXECUTOR_LOGGED:
        log_info(f"Executor selected: {mode} (source={source}, requested={requested})")
        _EXECUTOR_LOGGED = True
    return mode, source


def executor_mode() -> str:
    if _EXECUTOR_MODE is None:
        configure_executor_mode(None)
    return _EXECUTOR_MODE or "thread"


def executor_source() -> str | None:
    return _EXECUTOR_SOURCE


COLOR_RESET = "\033[0m"
COLOR_GREEN = "\033[32m"
COLOR_RED = "\033[31m"
COLOR_YELLOW = "\033[33m"
COLOR_CYAN = "\033[36m"
COLOR_PINK = "\033[95m"

RESET = "\033[0m"
BOLD = "\033[1m"


def color_256(code: int) -> str:
    return f"\033[38;5;{code}m"

FG256_PRIMARY = color_256(74)   # teal / brand header
FG256_ACCENT = color_256(141)   # violet / accent
FG256_OK = color_256(64)        # success/OK
FG256_ERROR = color_256(88)     # soft error red
FG256_WARN = color_256(221)     # warning gold
FG256_LINK = color_256(33)      # paths / links
FG256_MUTED = color_256(245)    # muted/secondary
FG256_1 = color_256(75)
FG256_2 = color_256(74)
FG256_3 = color_256(38)
FG256_4 = color_256(33)
FG256_5 = color_256(25)


def _colored_logo_lines() -> list[str]:
    accent_logo = FG256_1 + BOLD
    primary_logo = FG256_2 + BOLD
    mid_logo = FG256_3 + BOLD
    frame_color = FG256_4 + BOLD
    shadow_logo = FG256_5 + BOLD
    reset_style = RESET
    return [
        frame_color + "┌──────────────────────────────────────────────────────────────────────┐",
        frame_color + "│                                                                      │",
        frame_color
        + "│     "
        + accent_logo
        + "███"
        + frame_color
        + "╗   "
        + accent_logo
        + "███"
        + frame_color
        + "╗ "
        + accent_logo
        + "█████"
        + frame_color
        + "╗ "
        + accent_logo
        + "██████"
        + frame_color
        + "╗ "
        + accent_logo
        + "██"
        + frame_color
        + "╗  "
        + accent_logo
        + "██"
        + frame_color
        + "╗ "
        + accent_logo
        + "██████"
        + frame_color
        + "╗ "
        + accent_logo
        + "██████"
        + frame_color
        + "╗ "
        + accent_logo
        + "██████"
        + frame_color
        + "╗      │",
        frame_color
        + "│     "
        + primary_logo
        + "████"
        + frame_color
        + "╗ "
        + primary_logo
        + "████"
        + frame_color
        + "║"
        + primary_logo
        + "██"
        + frame_color
        + "╔══"
        + primary_logo
        + "██"
        + frame_color
        + "╗"
        + primary_logo
        + "██"
        + frame_color
        + "╔══"
        + primary_logo
        + "██"
        + frame_color
        + "╗"
        + primary_logo
        + "██"
        + frame_color
        + "║  "
        + primary_logo
        + "██"
        + frame_color
        + "║"
        + primary_logo
        + "██"
        + frame_color
        + "╔═══"
        + primary_logo
        + "██"
        + frame_color
        + "╗╚═"
        + primary_logo
        + "██"
        + frame_color
        + "╔═╝"
        + primary_logo
        + "██"
        + frame_color
        + "╔═══"
        + primary_logo
        + "██"
        + frame_color
        + "╗     │",
        frame_color
        + "│     "
        + mid_logo
        + "██"
        + frame_color
        + "╔"
        + mid_logo
        + "████"
        + frame_color
        + "╔"
        + mid_logo
        + "██"
        + frame_color
        + "║"
        + mid_logo
        + "███████"
        + frame_color
        + "║"
        + mid_logo
        + "██████"
        + frame_color
        + "╔╝"
        + mid_logo
        + "███████"
        + frame_color
        + "║"
        + mid_logo
        + "██"
        + frame_color
        + "║   "
        + mid_logo
        + "██"
        + frame_color
        + "║  "
        + mid_logo
        + "██"
        + frame_color
        + "║  "
        + mid_logo
        + "██"
        + frame_color
        + "║   "
        + mid_logo
        + "██"
        + frame_color
        + "║     │",
        frame_color
        + "│     "
        + frame_color
        + "██"
        + frame_color
        + "║╚"
        + frame_color
        + "██"
        + frame_color
        + "╔╝"
        + frame_color
        + "██"
        + frame_color
        + "║"
        + frame_color
        + "██"
        + frame_color
        + "╔══"
        + frame_color
        + "██"
        + frame_color
        + "║"
        + frame_color
        + "██"
        + frame_color
        + "╔═══╝ "
        + frame_color
        + "██"
        + frame_color
        + "╔══"
        + frame_color
        + "██"
        + frame_color
        + "║"
        + frame_color
        + "██"
        + frame_color
        + "║   "
        + frame_color
        + "██"
        + frame_color
        + "║  "
        + frame_color
        + "██"
        + frame_color
        + "║  "
        + frame_color
        + "██"
        + frame_color
        + "║   "
        + frame_color
        + "██"
        + frame_color
        + "║     │",
        frame_color
        + "│     "
        + shadow_logo
        + "██"
        + frame_color
        + "║ ╚═╝ "
        + shadow_logo
        + "██"
        + frame_color
        + "║"
        + shadow_logo
        + "██"
        + frame_color
        + "║  "
        + shadow_logo
        + "██"
        + frame_color
        + "║"
        + shadow_logo
        + "██"
        + frame_color
        + "║     "
        + shadow_logo
        + "██"
        + frame_color
        + "║  "
        + shadow_logo
        + "██"
        + frame_color
        + "║╚"
        + shadow_logo
        + "██████"
        + frame_color
        + "╔╝  "
        + shadow_logo
        + "██"
        + frame_color
        + "║  ╚"
        + shadow_logo
        + "██████"
        + frame_color
        + "╔╝     │",
        frame_color + "│     ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═════╝      │",
        frame_color + "│                                                                      │",
        frame_color + "│                 P R E V I E W   T H E N   M E R G E                  │",
        "└──────────────────────────────────────────────────────────────────────┘" + reset_style,
    ]


def _ascii_logo_lines() -> list[str]:
    return [
        "+----------------------------------------------------------------------+",
        "|                                                                      |",
        "|     NOLOSSIA                                                         |",
        "|                                                                      |",
        "|     ███╗   ███╗ █████╗ ██████╗ ██╗  ██╗ ██████╗ ██████╗ ██████╗      |",
        "|     ████╗ ████║██╔══██╗██╔══██╗██║  ██║██╔═══██╗  ██║  ██╔═══██╗     |",
        "|     ██╔████╔██║███████║██████╔╝███████║██║   ██║  ██║  ██║   ██║     |",
        "|     ██║╚██╔╝██║██╔══██║██╔═══╝ ██╔══██║██║   ██║  ██║  ██║   ██║     |",
        "|     ██║ ╚═╝ ██║██║  ██║██║     ██║  ██║╚██████╔╝  ██║  ╚██████╔╝     |",
        "|     ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═════╝      |",
        "|                                                                      |",
        "|                 P R E V I E W   T H E N   M E R G E                  |",
        "+----------------------------------------------------------------------+",
    ]


def print_nolossia_logo(return_string: bool = False) -> str | None:
    """
    Print the official Nolossia ASCII logo with brand colors.
    """
    art = "\n".join(_colored_logo_lines())
    if return_string:
        return art
    print(art)
    return None


def print_nolossia_logo_ascii(return_string: bool = False) -> str | None:
    """
    Print the official Nolossia ASCII logo without ANSI colors or box characters.
    """
    art = "\n".join(_ascii_logo_lines())
    if return_string:
        return art
    print(art)
    return None


def print_nolossia_logo_simple(return_string: bool = False) -> str | None:  # pragma: no cover - compatibility alias
    return print_nolossia_logo_ascii(return_string=return_string)


def human_readable_size(bytes: int) -> str:
    """
    Convert byte size into human-readable string.

    Args:
        bytes: Number of bytes.

    Returns:
        Human-readable string representation.

    Raises:
        None
    """
    thresholds: Tuple[Tuple[str, int], ...] = (("GB", 1024**3), ("MB", 1024**2), ("KB", 1024))
    for suffix, size in thresholds:
        if bytes >= size:
            value = bytes / size
            return f"{value:.2f} {suffix}"
    return f"{bytes} B"


def ensure_directory(path: str):
    """
    Create directory if it does not exist.

    Args:
        path: Directory path to create.

    Returns:
        None

    Raises:
        NolossiaError: If the directory cannot be created.
    """
    normalized = os.path.abspath(path)
    try:
        os.makedirs(normalized, exist_ok=True)
    except OSError as exc:
        log_error(f"[ERROR] Failed to create directory: {normalized} ({exc})")
        raise NolossiaError(f"Unable to create directory: {normalized}") from exc


def safe_copy(src: str, dst: str):
    """
    Copy file safely with validation.

    Args:
        src: Source file path.
        dst: Destination file path.

    Returns:
        None

    Raises:
        NolossiaError: If the copy operation fails.
    """
    normalized_src = os.path.abspath(src)
    normalized_dst = os.path.abspath(dst)
    try:
        ensure_directory(os.path.dirname(normalized_dst))
        shutil.copy2(normalized_src, normalized_dst)
        if not os.path.exists(normalized_dst):
            raise FileNotFoundError(f"Copy verification failed for {normalized_dst}")
    except Exception as exc:
        log_error(f"Failed to copy {normalized_src} to {normalized_dst}: {exc}")
        raise NolossiaError(f"Failed to copy {normalized_src} to {normalized_dst}") from exc


def path_violation_message(target: str, root: str, *, label: str) -> str | None:
    """
    Return a descriptive error message when `target` is outside `root`
    or when a symlink exists along the path. Returns None if the path is safe.
    """
    normalized_root = os.path.abspath(root)
    normalized_target = os.path.abspath(target)
    try:
        relative = os.path.relpath(normalized_target, normalized_root)
    except ValueError:
        return (
            f"{label} '{normalized_target}' lives on a different device than '{normalized_root}'. "
            "Choose a destination under the requested library."
        )
    if relative.startswith(os.pardir):
        return (
            f"{label} '{normalized_target}' escapes destination '{normalized_root}'. "
            "Remove '..' segments or pick another folder."
        )
    try:
        root_real = os.path.realpath(normalized_root)
        target_real = os.path.realpath(normalized_target)
        if os.path.commonpath([target_real, root_real]) != root_real:
            return (
                f"{label} '{normalized_target}' resolves outside '{normalized_root}'. "
                "Remove symlinks or select a different destination."
            )
    except ValueError:
        return (
            f"{label} '{normalized_target}' resolves outside '{normalized_root}'. "
            "Remove symlinks or select a different destination."
        )
    parts = [part for part in relative.split(os.sep) if part not in ("", ".")]
    current = normalized_root
    for part in parts:
        if part == os.pardir:
            return (
                f"{label} '{normalized_target}' escapes destination '{normalized_root}'. "
                "Remove '..' segments or relocate the folder."
            )
        current = os.path.join(current, part)
        if os.path.islink(current):
            return (
                f"{label} '{current}' is a symlink under '{normalized_root}'. "
                "Remove the symlinked folder or choose another library."
            )
    return None


def safe_move(src: str, dst: str, *, allowed_root: str) -> str:
    """
    Move file safely with validation and collision detection.

    Args:
        src: Source file path.
        dst: Destination file path.
        allowed_root: Absolute root directory the move must stay within.

    Returns:
        Final destination path used after move (with hash suffix if applied).

    Raises:
        NolossiaError: If the move operation fails or an unresolvable collision occurs.
    """
    if not allowed_root:
        raise ValueError("allowed_root is required for safe_move")
    normalized_src = os.path.abspath(src)
    normalized_dst_dir = os.path.dirname(os.path.abspath(dst))
    normalized_dst_base = os.path.basename(os.path.abspath(dst))
    normalized_root = os.path.abspath(allowed_root)

    from . import hashing  # Local import to break circular dependency

    def _ensure_safe(target: str, *, label: str) -> None:
        violation = path_violation_message(target, normalized_root, label=label)
        if violation:
            log_warning(f"Destination safety violation: {violation}")
            raise NolossiaError(violation)

    _ensure_safe(normalized_dst_dir, label="Destination folder")

    ensure_directory(normalized_dst_dir)

    target_dst = os.path.join(normalized_dst_dir, normalized_dst_base)
    _ensure_safe(target_dst, label="Destination file")

    if os.path.exists(target_dst):
        src_hash = hashing.compute_sha256(normalized_src)
        dst_hash = hashing.compute_sha256(target_dst)

        if src_hash != dst_hash:
            base, ext = os.path.splitext(normalized_dst_base)
            suffixed_dst = os.path.join(normalized_dst_dir, f"{base}-{src_hash}{ext}")

            if os.path.exists(suffixed_dst):
                log_error(
                    f"Unresolvable collision for {src} to {dst}. Both original and suffixed destination exist and differ."
                )
                raise NolossiaError(f"Unresolvable collision for {src} to {dst}.")
            log_info(
                f"Filename collision detected for '{normalized_dst_base}'. "
                f"Stored incoming file as '{os.path.basename(suffixed_dst)}' using hash {src_hash[:12]}."
            )
            _ensure_safe(suffixed_dst, label="Destination file")
            target_dst = suffixed_dst
        # If src_hash == dst_hash, we allow the overwrite, as it's an identical file.
    else:
        _ensure_safe(target_dst, label="Destination file")

    try:
        shutil.move(normalized_src, target_dst)
        if not os.path.exists(target_dst):
            raise FileNotFoundError(f"Move verification failed for {target_dst}")
    except Exception as exc:
        log_error(f"Failed to move {normalized_src} to {target_dst}: {exc}")
        raise NolossiaError(f"Failed to move {normalized_src} to {target_dst}") from exc
    return target_dst


def log_error(message: str):
    """
    Log an error message.

    Args:
        message: Error message to log.

    Returns:
        None

    Raises:
        None
    """
    from . import reporting  # Local import to avoid circular dependency rules

    reporting.write_log([f"[ERROR] {message}"])


def log_warning(message: str):
    """
    Log a warning message.

    Args:
        message: Warning message to log.

    Returns:
        None
    """
    from . import reporting  # Local import to avoid circular dependency rules

    reporting.write_log([f"[WARNING] {message}"])


def log_info(message: str):
    """
    Log an informational message.
    """
    from . import reporting  # Local import to avoid circular dependency rules

    reporting.write_log([f"[INFO] {message}"])


def osc8_link(path: str, label: str | None = None) -> str:
    """
    Build an OSC-8 hyperlink escape for supported terminals.

    Args:
        path: Target path or URL.
        label: Optional label; defaults to path.

    Returns:
        String containing OSC-8 wrapped label.

    Raises:
        None
    """

    abs_path = os.path.abspath(path)
    uri = "file://" + urllib.parse.quote(abs_path)
    display = label if label is not None else abs_path
    # OSC 8: ESC ] 8 ; ; URI BEL  label  ESC ] 8 ; ; BEL
    return f"\033]8;;{uri}\a{display}\033]8;;\a"

def color_text(text: str, color: str) -> str:
    """
    Wrap text with ANSI color codes.

    Args:
        text: Text to wrap.
        color: ANSI color code.

    Returns:
        Colored text string.

    Raises:
        None
    """
    return f"{color}{text}{COLOR_RESET}"


# Apply initial pixel limit (default or env) on import.
configure_pixel_limit(None)
