# Journal

## Implemented

### 2026-03-16

- Corrected the project documentation root to `docs/` and added `mkdocs.yml`.
- Rewrote the root `README.md` so it points to `docs/`, `astro/tasks.md`, and
  `astro/journal.md`.
- Added `scheduled_jobs.yaml` as the canonical repo-managed ETL schedule file.
- Replaced the dashboard scaffold with a project-specific multipage monitor for
  source data, pricing hydration, and curve health.
- Added `src/instruments/bootstrap.py` and updated the ETL runners so runtime
  registration is explicit.
- Added `scripts/validate_runtime.py` as a pricing-runtime smoke test.
- Repaired `build_position_from_sheet()` so it imports a valid local
  `PROJECT_BUCKET_NAME`.
- Fixed `ImportValmer` metadata to match the stored schema and changed the table
  frequency to daily.
- Fixed `MexDerTIIE28Zero` metadata so it no longer derives schema by executing
  a live update.
- Fixed the wrong documentation-root instruction text so it now points at
  `docs/`.
- Clarified that `MexDerTIIE28Zero` is an unused standalone implementation in
  this repo, not a checked-in execution path.
- Confirmed that `MC_BONOS` and `MP_BONOS` are intentionally modeled as
  zero-coupon instruments in the current mapping logic.
- Clarified that non-`MPS` convention support is intentionally out of scope
  until explicitly implemented.
- Added `xlrd` to the project with `uv` so the repository can read legacy
  `.xls` Valmer source files directly.
- Corrected `BUCKET_NAME_HISTORICAL_VECTORS` to `Historical Valmer Vector Analytico`.
- Expanded `ImportValmer.update()` so it persists the full 60-column sample
  workbook schema with translated English output columns plus derived OHLC
  fields.
- Updated `docs/data-nodes.md` so the documented bucket name and stored schema
  match the code.
- Reverted `BUCKET_NAME_HISTORICAL_VECTORS` and the docs back to
  `Hitorical Valmer Vector Analytico` to match the backend resource name
  actually used by this project.
- Made `ImportValmer._get_target_bonds(...)` work against both the raw ingest
  schema and the translated stored schema so dashboard logic can reuse the same
  target-universe rule set.
- Reworked the sample Streamlit dashboard to use the translated Valmer schema
  directly and reused MainSequence sidebar components for logged-user context
  and asset lookup.
- Renamed the dashboard package from `dashboards/sample_app` to
  `dashboards/valmer_monitor` so the deployed app path is project-specific
  instead of scaffold-derived.

## Failed

### 2026-03-16

- `.venv/bin/mainsequence --help`
- `.venv/bin/mainsequence project sdk-status --path .`
- `.venv/bin/mainsequence sdk latest`

All three commands failed before argument parsing because the installed
`mainsequence` package attempted a backend request during logger startup and the
sandboxed environment could not resolve `main-sequence.app`.

## Failed Due to Possible MainSequence Issue

### 2026-03-16

- The MainSequence CLI currently performs
  `GET /orm/api/pods/job/get_job_startup_state/` while importing the package,
  even for `--help`.
- This may be an SDK issue because basic local CLI operations should not depend
  on a live backend before command parsing.
- The SDK should provide an explicit local-mode or no-startup-probe path so
  offline help, status inspection, and dry-run workflows do not fail during
  import.

## Current Tasks Snapshot

### 2026-03-16

- Run the live SDK upgrade, token refresh, sync, image creation, batch job
  scheduling, and verification commands from a networked/authenticated shell.
- Validate `scripts/validate_runtime.py` against the live backend.
- Decide whether fixing-rate ETLs should remain external or move into this
  repository.
- Confirm the full translated Valmer schema against a live backend update once
  networked validation is available.

## Error Resolution Check

### 2026-03-16

- No prior `astro/journal.md` existed, so this startup failure had no historical
  record in the project.
- No previously documented fix was available locally.
- The new record above should be checked before the next live CLI attempt.
- A later offline import attempt hit the same startup-probe behavior while
  trying to inspect `mainsequence.client`, so the previously documented issue is
  still active and unresolved locally.
