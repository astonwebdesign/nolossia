import io
import re
import sys

from src.cli_formatter import (
    BULLET_INDENT,
    CLIFormatter,
    DEFAULT_KV_WIDTH,
    DEFAULT_LINE_WIDTH,
    FormatterConfig,
    detect_terminal_capabilities,
)


def _make_formatter(**config_overrides):
    config = FormatterConfig(**config_overrides)
    stream = io.StringIO()
    return CLIFormatter(config=config, stream=stream), stream


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_formatter_banner_colored_contains_unicode_and_ansi(monkeypatch):
    monkeypatch.setenv("NOLOSSIA_VERSION", "1.2.3")
    monkeypatch.setenv("NOLOSSIA_TAGLINE", "Canonical Renderer")
    formatter, stream = _make_formatter(use_color=True, unicode_enabled=True, show_banner=True)
    formatter.print_banner()
    output = stream.getvalue()
    assert "\u001b[" in output  # ANSI sequence introduced by colored banner
    lines = output.splitlines()
    stripped = [_strip_ansi(line) for line in lines if line]
    assert stripped[0].startswith("   ▄▄")
    assert stripped[-1] == "Nolossia — Canonical Renderer (v1.2.3)"


def test_formatter_banner_ascii_has_no_ansi_sequences(monkeypatch):
    monkeypatch.setenv("NOLOSSIA_VERSION", "v9.9")
    monkeypatch.delenv("NOLOSSIA_TAGLINE", raising=False)
    formatter, stream = _make_formatter(use_color=False, unicode_enabled=False, show_banner=True)
    formatter.print_banner()
    output = stream.getvalue().strip()
    assert output == "Nolossia — Preview Then Merge (v9.9)"
    assert "\u001b[" not in output  # ASCII mode must not emit ANSI escapes


def test_failure_summary_renders_details():
    formatter, stream = _make_formatter(use_color=False, unicode_enabled=False, show_banner=False)
    formatter.failure_summary(
        header="ABORTED — Merge execution",
        reason="Test failure",
        last_step="Execute",
        files_changed="None",
        log_hint="nolossia.log",
        artifacts=["merge_plan.json"],
        remediation=["Retry once the error is resolved."],
        details=[("Destination", "/tmp/photos")],
    )
    output = stream.getvalue()
    assert "STOP/BLOCKED" in output
    assert "Reason: Test failure" in output
    assert "Required: Retry once the error is resolved." in output


def test_formatter_sections_and_links_are_structured():
    formatter, stream = _make_formatter(use_color=False, unicode_enabled=False, show_banner=False)
    formatter.section("Test Section", icon=">")
    formatter.kv("Total photos", "10")
    formatter.line(f"Link: {formatter.link('/tmp/report', 'report')}")
    formatter.bullet("Done")

    lines = stream.getvalue().splitlines()
    assert lines[0] == ""  # leading newline from section()
    assert lines[1] == "> Test Section"
    assert lines[2].startswith("  Total photos")
    assert lines[2].endswith(": 10")
    assert lines[3] == "Link: report"
    assert lines[4] == "  - Done"


def test_plain_mode_config_suppresses_banner():
    formatter, stream = _make_formatter(show_banner=True)
    formatter.config.plain_mode = True
    formatter.print_banner()
    assert stream.getvalue() == ""  # banner suppressed in plain mode


def test_section_falls_back_to_ascii_icon():
    formatter, stream = _make_formatter(use_color=False, unicode_enabled=False, show_banner=False)
    formatter.section("ASCII Mode")
    lines = stream.getvalue().splitlines()
    assert lines[1].startswith("> ASCII Mode")


def test_detect_terminal_capabilities_plain_mode(monkeypatch):
    config = detect_terminal_capabilities(plain_mode=True)
    assert config.plain_mode
    assert not config.use_color
    assert not config.show_banner
    assert not config.unicode_enabled


def test_detect_terminal_capabilities_no_color_env(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    config = detect_terminal_capabilities()
    assert not config.use_color


def test_detect_terminal_capabilities_force_ascii():
    config = detect_terminal_capabilities(force_ascii=True)
    assert not config.unicode_enabled


def test_detect_terminal_capabilities_color_preference_overrides_tty(monkeypatch):
    class DummyStdout(io.StringIO):
        encoding = "utf-8"

        def isatty(self):
            return False

    monkeypatch.setattr(sys, "stdout", DummyStdout())
    config = detect_terminal_capabilities(color_preference="always", mode_preference="tty")
    assert config.use_color


def test_frame_respects_line_width_ascii():
    formatter, stream = _make_formatter(use_color=False, unicode_enabled=False, show_banner=False)
    formatter.line_width = 80
    formatter.frame("STOP/BLOCKED", ["Reason: A failure occurred.", "Required: Retry later."])
    lines = stream.getvalue().splitlines()
    assert all(len(line) <= 80 for line in lines)
    assert all("┌" not in line for line in lines)


def test_detect_terminal_capabilities_env_no_banner(monkeypatch):
    monkeypatch.setenv("NOLOSSIA_NO_BANNER", "1")
    config = detect_terminal_capabilities()
    assert not config.show_banner


def test_detect_terminal_capabilities_no_color_flag(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    config = detect_terminal_capabilities(no_color_flag=True)
    assert not config.use_color


def test_kv_wraps_long_values_and_aligns_continuations():
    formatter, stream = _make_formatter(use_color=False, unicode_enabled=False, show_banner=False)
    long_value = "This is a very long description that should wrap within the layout width " * 2
    formatter.kv("Long Label", long_value)
    lines = [line for line in stream.getvalue().splitlines() if line]
    assert len(lines) >= 2
    prefix = f"  {'Long Label':<{DEFAULT_KV_WIDTH}} : "
    assert lines[0].startswith(prefix)
    continuation_prefix = " " * len(prefix)
    assert lines[1].startswith(continuation_prefix)


def test_kv_supports_nested_indent_levels():
    formatter, stream = _make_formatter(use_color=False, unicode_enabled=False, show_banner=False)
    formatter.kv("Nested", "value", indent=2)
    line = stream.getvalue().splitlines()[0]
    assert line.startswith("    Nested")


def test_bullet_wraps_long_text_with_consistent_indent():
    formatter, stream = _make_formatter(use_color=False, unicode_enabled=False, show_banner=False)
    long_text = " ".join(["bullet"] * 20)
    formatter.bullet(long_text)
    lines = [line for line in stream.getvalue().splitlines() if line]
    assert len(lines) >= 2
    assert lines[0].startswith(BULLET_INDENT)
    continuation_prefix = " " * len(BULLET_INDENT)
    assert lines[1].startswith(continuation_prefix)


def test_divider_matches_configured_line_width():
    formatter, stream = _make_formatter(use_color=False, unicode_enabled=False, show_banner=False)
    formatter.divider()
    divider_line = stream.getvalue().splitlines()[0]
    assert len(divider_line) == DEFAULT_LINE_WIDTH
def test_detect_terminal_capabilities_disables_color_on_non_tty(monkeypatch):
    config = detect_terminal_capabilities(stdout_isatty=False)
    assert not config.use_color
    assert not config.osc8_links


def test_detect_terminal_capabilities_force_osc8_env(monkeypatch):
    monkeypatch.setenv("NOLOSSIA_FORCE_OSC8", "1")
    config = detect_terminal_capabilities(
        stdout_isatty=False, color_preference="always", mode_preference="tty"
    )
    assert config.osc8_links
    monkeypatch.delenv("NOLOSSIA_FORCE_OSC8", raising=False)


def test_detect_terminal_capabilities_auto_pipe(monkeypatch):
    config = detect_terminal_capabilities(stdout_isatty=False, mode_preference="auto")
    assert config.pipe_mode
    assert config.mode == "pipe"


def test_formatter_link_suppressed_when_osc8_disabled():
    formatter, _ = _make_formatter(use_color=True, unicode_enabled=True, show_banner=False)
    formatter.config.osc8_links = False
    output = formatter.link("/tmp/report.html", "report")
    assert output == "report"
