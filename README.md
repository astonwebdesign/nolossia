# Nolossia — Preview Then Merge (CLI)

Nolossia is a safety-first, CLI-only tool for scanning, deduplicating, and merging photo libraries. Designed to prevent data loss. It defaults to preview only (no changes yet) and requires explicit confirmation before moving anything. HTML/JSON reports are generated from the active merge plan and are strictly review-only. Nolossia must be run locally/offline; it does not talk to the cloud or require credentials.

Canonical positioning (one sentence): **CLI safety-first dedupe/merge for datahoarders.**

> **Migration note:** MAPhoto has been renamed to **Nolossia** (“Preview Then Merge”). You may still see the old name in archived reports and historical decision logs.

> **Primary persona:** Privacy-first NAS steward who wants deterministic, offline dedupe and chronological merges without cloud exposure. 

## Status (MVP)
- CLI wizard: SCAN → DEDUPE → MERGE PREVIEW → MERGE EXECUTE.
- No deletions; exact duplicates go to QUARANTINE, look-alikes (near-duplicates) are marked for review only.
- Merge always organizes into a unified chronological structure (YEAR / YEAR-MONTH). A merge cannot proceed unless the destination folder is empty or already chronological.
- Future items (not in MVP): AI face/event features, GUI, advanced clustering (see TODO.md).

## Supported formats (Phase 1)
- JPEG / JPG / PNG
- HEIC
- TIFF
- RAW: DNG, NEF, CR2, CR3, ARW, RW2

### Who Nolossia is for (MVP persona)
- Privacy-focused photo hoarders and NAS owners who distrust cloud sync tools or online AI sorters.
- CLI/TUI-friendly users who prefer deterministic, auditable outputs and can run offline workflows.
- People migrating scattered folders/exports into one offline, chronological library without sharing data upstream.

### Explicit limitations (Phase 1)
- CLI-only workflow (GUI planned for a later phase).
- No deletions, compression, or metadata stripping in MVP.
- No cloud import/export APIs; NAS must be mounted by the OS beforehand.
- Offline/local operation only; Nolossia never uploads or reaches out to remote services.
- Photo-only scope (no video/Live Photo handling yet; still images from Live Photos are treated as photos).
- Preview only (no changes yet) is mandatory before any merge executes; even `merge` runs simulate + confirm (type `EXECUTE`).

### Hashing & Thresholds
- Exact duplicates: SHA256.
- Near-duplicates: perceptual hash (simple aHash), with Hamming distance bands:
  - strong ≤ 2 (lenient validation),
  - weak ≤ 5 (strict validation with resolution/date/camera checks).

### What “dedupe” means
In Nolossia, *dedupe* is short for *de-duplication*: the read‑only process of detecting, grouping, and classifying duplicate and near‑duplicate photos.  
It does **not** move, delete, or modify any files.  
Dedupe performs:
- exact‑duplicate detection (SHA256),
- near‑duplicate detection (perceptual hashing),
- clustering of related files,
- selecting a logical master per cluster (read-only; RAW > resolution > file size > EXIF > GPS > oldest when equal),
- preparing information used later by the merge preview‑only (dry‑run).

Only the merge **EXECUTE** phase performs actual filesystem operations.

### Glossary (short)
- **Near‑duplicate:** similar photos marked for review only (not moved).
- **Quarantine:** exact duplicates moved; never deleted automatically.
- **REVIEW:** files missing reliable EXIF or ambiguous dates.

## Installation

- **Install & compliance notes:** see `installation/installation.md` for retention/audit guidance.
- **Manual (current):**
  ```bash
  git clone https://github.com/astonwebdesign/nolossia
  cd nolossia
  python3 -m venv .venv
  . .venv/bin/activate
  pip install --require-hashes -r requirements.txt
  python3 -m src.cli --help
  ```
- **Packaging roadmap:** see `installation/installation.md` for Homebrew, Winget/Chocolatey, AppImage/Debian/RPM, and container plans (with integrity checklist).
- **Exit codes + UNC examples:** see `installation/installation.md`.

## Development & Lint Workflow
- Follow [`CONTRIBUTING.md`](CONTRIBUTING.md) for the full governance-aware workflow.
- Required gates (local + CI):
  - `ruff check src tests`
  - `bandit -q -r src`
  - `pytest`
- Install the provided `pre-commit` hooks (`pre-commit install`) to run Ruff and Bandit on staged files before committing.
- Profiling helper (writes to `artifacts/profile.pstats` by default):
  - `docs/scripts/profile.sh -m src.cli --plain --no-banner --no-color merge artifacts/data/set_s --out artifacts/dest_s --dry-run`
  - `docs/scripts/profile.sh -m pytest tests/test_scanner.py`

## CLI Commands
- `python3 -m src.cli scan <paths...>` — read-only scan and dedupe wizard entry.
- `python3 -m src.cli dedupe <paths...>` — optional direct dedupe (read-only) if you want to skip the wizard.
- `python3 -m src.cli organize <paths...> --out <library_path>` — preview-only (no changes yet) chronological organization preview.
- `python3 -m src.cli merge <paths...> --out <target>` — direct merge (runs preview-only before execute; automatically organizes chronologically).

### Automation (pipe JSON)
Use pipe mode to emit single-line JSON summaries (stable `schema_version`), or stream JSON events per phase for long runs.

```bash
python3 -m src.cli --mode pipe merge ~/Photos --out ~/PhotoLibrary
python3 -m src.cli --mode pipe --stream-json merge ~/Photos --out ~/PhotoLibrary
```

Example JSON summary line:

```json
{"schema_version":"1.0","status":"DRY_RUN","phase":"merge","masters":1200,"duplicates":340,"near":58,"storage":{"required":"48.2GB","available":"512GB","breakdown":{"masters":"41.3GB","quarantine":"6.1GB","review":"0.8GB"}},"review":{"count":12,"samples":["REVIEW/2021/IMG_1023.jpg"]},"skipped":2,"reports":["/abs/path/merge_plan.json","/abs/path/dedupe_report.html"],"reports_count":2,"reports_summary":{"merge_plan.json":1,"dedupe_report.html":1}}
```
`status: DRY_RUN` means preview-only (no changes yet).

Automation FAQ (output changes):
- New fields may be added to JSON, but existing fields remain stable for the same `schema_version`.
- Log `schema_version` per run to detect changes across releases.
- Use `--stream-json` for long runs to get phase updates without waiting for completion.
- Pipe schema reference: `docs/specs/PIPE_SCHEMA.md`.
- Changelog: see `docs/roadmap/ROADMAP.md` for release notes affecting automation output.
- Versioning timeline:
  - `schema_version` 1.0 introduced with pipe JSON summaries and streaming events.

L dataset benchmark (reference run):
- Machine: iMac 24-inch (M1, 16 GB RAM), macOS Sequoia 15.6.1.
- Dataset: `artifacts/data_public/L/`, command: `python -m src.cli --plain --no-banner merge artifacts/data_public/L --out artifacts/dest_l --dry-run`.
- Result: ~27.5s wall time, peak RSS ~1.18 GB, required storage 1.96 GB.

### Global flags
- `--verbose` — print rule evaluations (e.g., RAW_BEATS_JPEG, storage validation checks) for duplicate clustering and destination validation.
- `--plain`, `--ascii`, `--color`, `--no-banner` — control formatter output per AGENTS spec (colors, OSC8 links, ASCII fallback).
- `--theme` — choose a palette: `light`, `dark`, `high-contrast-light`, `high-contrast-dark`.
- `--executor [auto|process|thread]` — choose the hashing/metadata executor (default auto), override with `$NOLOSSIA_EXECUTOR`.

### Wizard Flow (Phase 6)
1) **SCAN (read-only)**
   - Validates input paths.
   - Recursively scans supported images, extracts metadata/hashes.
   - Prints SCAN SUMMARY: total files, supported/unsupported, duplicate counts, sizes.
   - CLI reminders: SCAN, DEDUPE, and MERGE definitions are printed before the first prompt.
   - Prompt: “→ Proceed to the next step? [y/N] (Enter cancels):”

2) **DEDUPE (read-only)**
   - Uses scan results (no rescan).
   - Shows ANALYSIS SUMMARY: masters, exact duplicates, look-alikes, clusters.
   - Explainer:
     - Exact duplicates → quarantined safely during merge execution.
     - Near-duplicates → masters stay put; similar copies remain in the destination for manual review.
   - Prompt: “→ Start the 'merge wizard'? [y/N] (Enter exits):”
   - When terminals do not support OSC8 hyperlinks (plain mode, pipes), Nolossia prints explicit `(open <path>)` instructions next to each report link.

3) **MERGE SETUP + PREVIEW (non-mutating)**
   - Prompt for target library path (empty aborts).
   - Prompt text: “> Destination path (Enter cancels). Example: /Library/2024/2024-05:” or, when re-checking, “> Destination path [/existing] (Enter keeps current). Example: /Library/2024/2024-05:”
   - Nolossia requires the destination to be:
       • empty, or
       • already organized in YEAR / YEAR‑MONTH structure.
    If the destination is not chronological, the user must choose to:
       1) reorganize the destination (dry‑run first), or
       2) choose a different empty folder, or
       3) abort.
   - STOP/BLOCKED guidance includes a copy/paste example: `/Library/2024/2024-05`.
   - Merge plan is always built using chronological organization.
	   - Shows MERGE PLAN summary: target, grouped action counts (MOVE_MASTER / MOVE_TO_QUARANTINE_EXACT / MARK_NEAR_DUPLICATE with size impact), and explicit “Required vs Free vs Free After” storage lines (with warnings if insufficient). ANSI colors + OSC8 links auto-disable when piping/redirecting output; use `--color=always` to override color, and `NOLOSSIA_FORCE_OSC8=1` only if your terminal viewer supports links.
	   - Prints: “Preview only — no changes yet.”
	   - Report-first reminder: review the generated reports before you execute (e.g., open `dedupe_report.html` for per-file review).
	   - Reminder in summary: RAW files stay intact; sidecars (e.g., .xmp) are not processed and must be copied separately.
	   - Storage math now lists the bytes that will land under Masters, QUARANTINE_EXACT, and REVIEW so the required total already equals the sum of those components.

4) **MERGE EXECUTE (mutating)**
   - Prompt: “Do you want to EXECUTE this merge plan now? Type 'EXECUTE' to move files now, or press Enter to abort:”
   - Only executes when the user types exactly `EXECUTE`.
   - Applies plan: creates folders, moves masters, quarantines exact duplicates, marks look-alikes (reports).
   - Generates HTML reports with exec summaries: `dedupe_report.html` (root, combined look-alikes + quarantine overview; plan-based, read-only) and `merge_report.html` (post-execute summary).
  - Reports include a "Print / Save as PDF" button that uses your browser print dialog for sharing. PDF link clickability depends on the viewer.
   - Reports/logs can contain absolute paths and filenames; treat them as sensitive and retain only as long as needed.
   - Preserves source context via a manifest: `source_manifest.json`, `source_manifest.csv`, `source_manifest.html` (original path, original folder, new path, hash, batch id). No sidecars are written unless a future opt-in flag is added.
   - On failures or aborts (StorageError, hash mismatch, Ctrl+C, manual cancel) Nolossia prints a framed FAILURE SUMMARY / ABORTED SUMMARY with the reason, last completed phase, files-changed statement, nolossia.log path, report links, and numbered remediation steps. Pipe mode emits the same payload as a single JSON/KV line for automation.

## Organization Rules (Always On)
- All merges produce or extend a unified chronological library (YEAR / YEAR‑MONTH / filename).
- Exact duplicates are quarantined; near‑duplicates are marked only.
- Existing YEAR / YEAR‑MONTH structures in source or destination are authoritative.
- A merge cannot proceed unless the destination library is chronological or empty.

## Safety Properties (MVP)
- Preview only is non-mutating.
- No deletions in MVP.
- Exact duplicates are moved to QUARANTINE, never deleted.
- Near-duplicates are marked for review only.
- Files with missing/corrupt EXIF timestamps or `timestamp_reliable=False` are routed to the REVIEW/ folder; the CLI preview-only lists the REVIEW count plus up to five sample paths, and merge_report.html enumerates every item for manual dating.
- EXECUTE confirmation required before any merge moves.
- Merging always applies chronological organization; never reorganizes existing non‑chronological destinations without explicit user consent.

## Repository Structure
```
nolossia/
  README.md
  LICENSE.txt
  requirements.txt
  src/
    cli.py
    cli_formatter.py
    scanner.py
    metadata.py
    hashing.py
    duplicates.py
    organizer.py
    merge_engine.py
    reporting.py
    exceptions.py
    utils.py
    models/
      fileinfo.py
      cluster.py
      mergeplan.py
      actions.py
```

## Notes
- HTML/JSON reports are generated from the active merge plan; no static fixtures.
- OSC-8 terminal hyperlinks are used in CLI output when supported.

