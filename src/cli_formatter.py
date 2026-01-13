"""
Module: cli_formatter
Purpose: Centralized CLI formatting utilities enforcing Nolossia output contract.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
import textwrap
from dataclasses import dataclass
from typing import Iterable, TextIO

from .utils import BOLD, COLOR_RESET, color_256, osc8_link

DEFAULT_LINE_WIDTH = 96
DEFAULT_KV_WIDTH = 32
PRIMARY_INDENT = "  "
BULLET_INDENT = f"{PRIMARY_INDENT}- "
ANSI_SGR_PATTERN = re.compile(r"\x1b\[[0-9;]*m")
OSC8_PATTERN = re.compile(r"\x1b]8;;.*?\x1b\\")
THEME_PALETTES: dict[str, dict[str, int]] = {
    "light": {
        "primary": 74,
        "accent": 141,
        "ok": 64,
        "warn": 221,
        "error": 160,
        "link": 33,
        "muted": 243,
    },
    "dark": {
        "primary": 75,
        "accent": 105,
        "ok": 64,
        "warn": 221,
        "error": 160,
        "link": 33,
        "muted": 245,
    },
    "high-contrast-light": {
        "primary": 21,
        "accent": 75,
        "ok": 46,
        "warn": 226,
        "error": 196,
        "link": 27,
        "muted": 250,
    },
    "high-contrast-dark": {
        "primary": 21,
        "accent": 75,
        "ok": 46,
        "warn": 226,
        "error": 196,
        "link": 27,
        "muted": 250,
    },
}
THEME_DISPLAY_NAMES = {
    "light": "light",
    "dark": "dark",
    "high-contrast-light": "high-contrast light",
    "high-contrast-dark": "high-contrast dark",
}


@dataclass
class FormatterConfig:
    """
    Configuration options governing CLIFormatter output.
    """

    use_color: bool = True
    unicode_enabled: bool = True
    show_banner: bool = True
    show_glossary: bool = True
    plain_mode: bool = False
    osc8_links: bool = True
    verbose: bool = False
    mode: str = "tty"
    pipe_mode: bool = False
    pipe_format: str = "json"
    stream_json: bool = False
    theme: str = "light"
    pixel_limit: int | None = None
    pixel_limit_source: str = "default"
    look_alike_sensitivity: str = "conservative"


class CLIFormatter:
    """
    Render Nolossia CLI output via a centralized contract.
    """

    def __init__(
        self,
        config: FormatterConfig | None = None,
        stream: TextIO | None = None,
    ) -> None:
        self.config = config or FormatterConfig()
        self.stream = stream or sys.stdout
        self.line_width = DEFAULT_LINE_WIDTH
        self.palette = _resolve_palette(self.config.theme)

    # ------------------------------------------------------------------ banners
    def print_banner(self) -> None:
        """
        Print the Nolossia banner with capability-aware fallback.
        """
        if not self.config.show_banner or self.config.plain_mode or self.config.pipe_mode:
            return
        banner_style = os.environ.get("NOLOSSIA_BANNER_STYLE", "logo").strip().lower() or "logo"
        tagline = os.environ.get("NOLOSSIA_TAGLINE", "Preview Then Merge").strip() or "Preview Then Merge"
        version_value = os.environ.get("NOLOSSIA_VERSION", "").strip()
        if not version_value:
            version_value = "dev"
        if not version_value.lower().startswith("v"):
            version_value = f"v{version_value}"
        theme_label = THEME_DISPLAY_NAMES.get(self.config.theme, self.config.theme)
        columns = shutil.get_terminal_size((80, 20)).columns

        full_lockup = f"Nolossia — {tagline} ({version_value}) • Theme {theme_label}"
        short_lockup = f"Nolossia ({version_value})"
        lockup = full_lockup if columns >= 80 else short_lockup

        if banner_style == "none":
            return
        if banner_style == "logo" and self.config.unicode_enabled and columns >= 60:
            logo_lines = [
                "   ▄▄     ▄▄▄    ▄▄",
                "   ██▄   ██▀      ██",
                "   ███▄  ██       ██                   ▀▀",
                "   ██ ▀█▄██ ▄███▄ ██ ▄███▄ ▄██▀█ ▄██▀█ ██ ▄▀▀█▄",
                "   ██   ▀██ ██ ██ ██ ██ ██ ▀███▄ ▀███▄ ██ ▄█▀██",
                " ▀██▀    ██▄▀███▀▄██▄▀███▀█▄▄██▀█▄▄██▀▄██▄▀█▄██",
                lockup,
            ]
            for line in logo_lines:
                self._write(self._style(line, self.palette["primary"], bold=True))
            return
        self._write(self._style(lockup, self.palette["primary"], bold=True))

    # ------------------------------------------------------------------- styles
    def line(self, text: str = "") -> None:
        """Print a plain line."""
        self._write(text)

    def blank(self) -> None:
        """Print an empty line."""
        self._write("")

    def section(self, title: str, icon: str | None = "◆") -> None:
        """Print a branded section heading."""
        icon_symbol = icon
        if icon_symbol:
            if self.config.plain_mode or not self.config.unicode_enabled:
                icon_symbol = ">"
            label = f"{icon_symbol} {title}"
        else:
            label = title
        self.blank()
        self._write(self._style(label, self.palette["primary"], bold=True))

    def info(self, text: str) -> None:
        """Print informational text."""
        self._write(self._style(text, self.palette["primary"]))

    def success(self, text: str) -> None:
        """Print success text."""
        self._write(self._style(text, self.palette["ok"], bold=True))

    def warning(self, text: str) -> None:
        """Print warning text."""
        self._write(self._style(text, self.palette["warn"], bold=True))

    def error(self, text: str) -> None:
        """Print error text."""
        self._write(self._style(text, self.palette["error"], bold=True))

    def failure_summary(
        self,
        *,
        header: str,
        reason: str,
        last_step: str | None = None,
        files_changed: str = "None",
        log_hint: str | None = None,
        artifacts: list[str] | None = None,
        remediation: list[str] | None = None,
        details: list[tuple[str, str]] | None = None,
    ) -> None:
        """
        Render a standardized failure/abort summary block.
        """
        if self.config.pipe_mode:
            return
        required = ""
        if remediation:
            required = remediation[0]
        elif log_hint:
            required = f"Review {log_hint} for details."
        else:
            required = "Review the error and rerun when ready."
        self.blank()
        lines = [f"Reason: {reason}"]
        if log_hint:
            lines.append(f"Log file: {log_hint}")
        lines.append(f"Required: {required}")
        self.frame(
            "STOP/BLOCKED",
            lines,
        )

    def frame(self, title: str, lines: list[str]) -> None:
        """
        Render a framed block with wrapped content.
        """
        width = min(self.line_width, DEFAULT_LINE_WIDTH)
        unicode = self.config.unicode_enabled and not self.config.plain_mode
        horiz = "─" if unicode else "-"
        vert = "│" if unicode else "|"
        tl, tr, bl, br = ("┌", "┐", "└", "┘") if unicode else ("+", "+", "+", "+")
        title_text = f"{horiz} {title} "
        top = f"{tl}{title_text}{horiz * max(0, width - 2 - len(title_text))}{tr}"
        self.line(top)
        content_width = width - 4
        for line in lines:
            wrapped = textwrap.wrap(line, width=content_width) or [""]
            for chunk in wrapped:
                self.line(f"{vert} {chunk.ljust(content_width)} {vert}")
        bottom = f"{bl}{horiz * (width - 2)}{br}"
        self.line(bottom)

    def divider(self, width: int = DEFAULT_LINE_WIDTH) -> None:
        """Print a horizontal divider line."""
        char = "─" if self.config.unicode_enabled else "-"
        self._write(self._style(char * width, self.palette["accent"]))

    def kv(self, label: str, value: str, width: int = DEFAULT_KV_WIDTH, indent: int = 1) -> None:
        """Print an aligned key/value line with wrapping support."""
        indent = max(indent, 0)
        indent_str = PRIMARY_INDENT * indent
        prefix = f"{indent_str}{label:<{width}} : "
        prefix_len = len(prefix)
        line = prefix + value
        if self._contains_control(value):
            self._write(line)
            return
        available = self.line_width - prefix_len
        if available < 10 or self._visible_length(value) <= available:
            self._write(line)
            return
        wrapped = textwrap.wrap(value, width=available) or [value]
        for index, chunk in enumerate(wrapped):
            if index == 0:
                self._write(prefix + chunk)
            else:
                self._write(" " * prefix_len + chunk)

    def bullet(self, text: str, indent: str | None = None) -> None:
        """Print a bullet item respecting layout width."""
        indent_str = indent if indent is not None else BULLET_INDENT
        prefix_len = len(indent_str)
        if self._contains_control(text):
            self._write(f"{indent_str}{text}")
            return
        available = self.line_width - prefix_len
        if available < 10 or self._visible_length(text) <= available:
            self._write(f"{indent_str}{text}")
            return
        wrapped = textwrap.wrap(text, width=available) or [text]
        for index, chunk in enumerate(wrapped):
            if index == 0:
                self._write(f"{indent_str}{chunk}")
            else:
                self._write(" " * prefix_len + chunk)

    def list_lines(self, lines: Iterable[str]) -> None:
        """Print multiple lines sequentially."""
        for line in lines:
            self._write(line)

    def prompt(self, message: str) -> str:
        """Return a formatted prompt string for input()."""
        return self._style(message, self.palette["accent"], bold=True)

    def link(self, path: str, label: str | None = None) -> str:
        """Return a styled hyperlink for capable terminals."""
        target = label or path
        if not self.config.use_color or not self._osc8_enabled():
            return target
        if not self._osc8_enabled():
            return target
        return self._style(osc8_link(path, target), self.palette["link"])

    def muted(self, text: str) -> None:
        """Print muted informational text."""
        self._write(self._style(text, self.palette["muted"]))

    def verbose(self, text: str) -> None:
        """Print verbose diagnostics when enabled."""
        if not self.config.verbose:
            return
        self.muted(f"[verbose] {text}")

    def label(self, text: str, level: str = "info", *, bold: bool = True) -> str:
        """Return a styled inline label for embedding in other strings."""
        color = {
            "info": self.palette["primary"],
            "accent": self.palette["accent"],
            "success": self.palette["ok"],
            "warn": self.palette["warn"],
            "error": self.palette["error"],
            "muted": self.palette["muted"],
        }.get(level, self.palette["primary"])
        if level == "plain":
            color = None
        return self._style(text, color, bold=bold)

    def style(self, text: str, color: str | None = None, *, bold: bool = False) -> str:
        """Return a styled string respecting formatter configuration."""
        return self._style(text, color, bold)

    # ----------------------------------------------------------------- internals
    def _write(self, text: str) -> None:
        self.stream.write(text + "\n")

    def _style(self, text: str, color: str | None = None, bold: bool = False) -> str:
        if not self.config.use_color or not text:
            return text
        prefix = ""
        if bold:
            prefix += BOLD
        if color:
            prefix += color
        if not prefix:
            return text
        return f"{prefix}{text}{COLOR_RESET}"

    @staticmethod
    def _visible_length(text: str) -> int:
        stripped = ANSI_SGR_PATTERN.sub("", text)
        return len(OSC8_PATTERN.sub("", stripped))

    @staticmethod
    def _contains_control(text: str) -> bool:
        return bool(ANSI_SGR_PATTERN.search(text) or OSC8_PATTERN.search(text))

    def _osc8_enabled(self) -> bool:
        return self.config.osc8_links and self.config.use_color and not self.config.plain_mode


def detect_terminal_capabilities(
    *,
    color_preference: str = "auto",
    plain_mode: bool = False,
    force_ascii: bool = False,
    no_color_flag: bool = False,
    stdout_isatty: bool | None = None,
    mode_preference: str = "auto",
    theme_preference: str | None = None,
) -> FormatterConfig:
    """
    Determine formatter configuration based on environment cues.
    """
    mode_normalized = (mode_preference or "auto").lower()
    if mode_normalized not in {"auto", "tty", "plain", "pipe"}:
        mode_normalized = "auto"

    if stdout_isatty is None:
        stdout_isatty = sys.stdout.isatty()

    env_no_color = bool(os.environ.get("NO_COLOR"))
    env_plain = bool(os.environ.get("NOLOSSIA_PLAIN"))
    env_force_ascii = bool(os.environ.get("NOLOSSIA_FORCE_ASCII"))
    env_no_banner = bool(os.environ.get("NOLOSSIA_NO_BANNER"))
    env_force_osc8 = bool(os.environ.get("NOLOSSIA_FORCE_OSC8"))
    env_disable_osc8 = bool(os.environ.get("NOLOSSIA_DISABLE_OSC8"))
    auto_pipe = mode_normalized == "auto" and not stdout_isatty
    pipe_mode = mode_normalized == "pipe" or auto_pipe
    auto_plain = False
    if not pipe_mode:
        auto_plain = mode_normalized == "auto" and not stdout_isatty
    explicit_plain = plain_mode or env_plain or mode_normalized == "plain" or pipe_mode
    if explicit_plain:
        osc8_allowed = env_force_osc8 and not pipe_mode
        return FormatterConfig(
            use_color=False,
            unicode_enabled=False,
            show_banner=False,
            plain_mode=True,
            osc8_links=osc8_allowed,
            mode="pipe" if pipe_mode else ("plain" if (mode_normalized in {"plain", "auto"} or auto_plain) else "tty"),
            pipe_mode=pipe_mode,
            pipe_format="json",
            theme=_resolve_theme(theme_preference),
        )

    term = os.environ.get("TERM", "").lower()
    preference = (color_preference or os.environ.get("NOLOSSIA_COLOR", "auto")).lower()
    if preference not in {"auto", "always", "never"}:
        preference = "auto"

    if pipe_mode:
        use_color = False
    elif preference == "always":
        use_color = True
    elif preference == "never":
        use_color = False
    else:
        use_color = (
            not no_color_flag
            and not env_no_color
            and stdout_isatty
            and term not in {"dumb"}
        )

    ascii_forced = force_ascii or env_force_ascii or term == "dumb" or pipe_mode
    unicode_enabled = not ascii_forced and _supports_unicode()

    osc8_links = (
        use_color
        and not env_disable_osc8
        and stdout_isatty
        and not plain_mode
    )
    if env_force_osc8:
        osc8_links = True

    config = FormatterConfig(
        use_color=use_color,
        unicode_enabled=unicode_enabled,
        show_banner=not env_no_banner and not pipe_mode and mode_normalized != "plain",
        plain_mode=False,
        osc8_links=osc8_links and not pipe_mode,
        mode=(
            "pipe"
            if pipe_mode
            else (
                mode_normalized
                if mode_normalized in {"tty", "plain"}
                else ("plain" if auto_plain else ("tty" if stdout_isatty else "plain"))
            )
        ),
        pipe_mode=pipe_mode,
        pipe_format="json",
        theme=_resolve_theme(theme_preference),
    )
    if auto_plain:
        config.plain_mode = True
        config.show_banner = False
        if preference != "always":
            config.use_color = False
        if not env_force_osc8:
            config.osc8_links = False
        config.unicode_enabled = False
    return config


def _resolve_theme(theme_preference: str | None) -> str:
    theme_value = theme_preference or os.environ.get("NOLOSSIA_THEME", "light")
    theme = theme_value.strip().lower()
    if theme in THEME_PALETTES:
        return theme
    return "light"


def _resolve_palette(theme: str) -> dict[str, str]:
    palette = THEME_PALETTES.get(theme, THEME_PALETTES["light"])
    return {key: color_256(code) for key, code in palette.items()}


def _supports_unicode() -> bool:
    encoding = getattr(sys.stdout, "encoding", None)
    if not encoding:
        return False
    try:
        "┌".encode(encoding)
        return True
    except UnicodeEncodeError:
        return False
