# General Instructions for Extensions, Documentation, Development, and Maintenance of the Project for Coding Assistants

You are working on this project and must always follow these instructions as persistent context.

## General Rules

- Keep all documentation clear, concise, and accurate.
- Correct inconsistencies as soon as you find them.
- Use strict code. Avoid defensive guards on the hot path. Fail fast, especially when updating `DataNodes`.
- Do not hide failures. Record them clearly and explain the cause.
- If a failure may be caused by the MainSequence library or SDK, state that explicitly and suggest a concrete improvement to the SDK.
- Before starting any work, upgrade to the latest MainSequence SDK version using the CLI.
- Always compare the implementation against the latest MainSequence SDK behavior and public documentation.
- Before running validations, run `mainsequence project refresh_token`.
- Verify all relevant resources using the CLI: `DataNodes`, updates, stored data, jobs, dashboards, assets, portfolios, and related platform objects.
- For `DataNodes` that may contain a large amount of data, always test first in a test namespace and with a smaller time range before running a full update or backfill.
- Any new implementation must be compared against the documentation and verified to ensure nothing breaks.
- When an error appears, first check the journal to see whether the same issue already happened and whether a solution was already documented.

## Required Project Structure

Use a project root called:

`astro/`

Be strict about creating and maintaining the following paths and about preserving their purpose.

### `astro/journal.md`
Create this file as the historical record of the project.

Its purpose is to preserve:

- what was implemented
- what failed
- what may have failed because of the MainSequence SDK or library
- what improvements should be suggested to MainSequence
- what tasks existed at a given moment
- whether a known error had already been solved before

This file is historical. Do not overwrite history. Append to it and keep it organized.

### `astro/tasks.md`
Create this file as the active task list for the current implementation state.

Its purpose is to contain only:

- tasks that still need to be performed
- open documentation fixes
- open validation work
- open SDK-related follow-ups
- open implementation tasks discovered during review

This file is not historical. Remove completed, obsolete, or superseded tasks.

Do not use `tasks.md` as a journal.

### `docs/`
Create this folder as the documentation root for the project.

Its purpose is to contain the formal project documentation in a structure compatible with `MkDocs`.

## Documentation Standard

- All formal project documentation must live under `docs/`.
- Documentation must follow an `MkDocs`-compatible structure.
- The documentation navigation must match the actual file structure.
- The root `README.md` must remain the project entry point. It should provide a high-level overview, explain the purpose of the library, and point readers to the documentation.
- Each major area of the project must have its own documentation page under `docs/`.
- Local `README.md` files should only be added when a package or component needs usage instructions close to the code. They must not replace the main project documentation.
- Operational and verification procedures, including CLI checks and backend validation, must be documented in a dedicated page under `docs/`.
- Any new feature, workflow, component, or integration must be reflected in the documentation.

## Very Important: Dashboard Development

- When building a dashboard using Streamlit, always refer to the documentation at:

  `https://github.com/mainsequence-sdk/mainsequence-sdk/tree/main/docs/knowledge/dashboards/streamlit`

- When building dashboards, separate UI components into:

  `dashboards/components/`

- Prefer reusable components whenever possible.
- If you build a component that is not currently in the MainSequence platform and you believe it would be generally useful, open a pull request in the public `mainsequence-sdk` repository.

## Journal Requirements

Keep an ongoing journal in:

`astro/journal.md`

Organize it with these sections:

### Implemented
Record what was successfully implemented.

### Failed
Record what failed, including the exact step, command, or workflow.

### Failed Due to Possible MainSequence Issue
Record failures that may be caused by the MainSequence library or SDK.

For each such issue, include:

- what failed
- why it may be an SDK or library issue
- what should be improved in the MainSequence SDK to avoid the error in the future

### Current Tasks Snapshot
Record the current task list in the journal for historical tracking.

### Error Resolution Check
When a new error appears, record whether:

- the same error was already documented
- a solution was already present in the journal
- the previous solution worked
- a new solution or SDK improvement is needed

## Tasks File Requirements

Keep the active task list in:

`astro/tasks.md`

Rules for `tasks.md`:

- It must contain only the current tasks to perform.
- It must not be historical.
- Remove completed or obsolete tasks.
- Keep it synchronized with the current implementation state.
- The same tasks should also be recorded in the journal as a historical snapshot.
- Any inconsistencies, missing documentation, SDK usability issues, or project improvements discovered during review must be converted into actionable tasks in `astro/tasks.md`.

## Project Path Conventions

Do not hardcode machine-specific local paths such as:

`/Users/jose/mainsequence/main-sequence-workbench/projects/project-ID`

Use a standard placeholder path instead, for example:

`<MAINSEQUENCE_WORKBENCH>/projects/project-ID`

If this library depends on another local project, document that dependency using the same standard path convention and follow that project as a reference standard where relevant.

## Documentation Content Requirements

All documentation must be written under `docs/` and organized for `MkDocs`.

### 1. Introduction
Explain what the library does. This section should closely follow the main project README and summarize the purpose of the library clearly.

### 2. DataNodes
Explain:

- which `DataNodes` are created
- what each `DataNode` stores
- the type of data each one contains
- how updates are performed
- any important constraints, namespaces, or validation rules

Also include operational guidance for high-volume nodes:

- test first in a test namespace
- use a smaller time range before running a full update

### 3. Markets
Explain how the project interacts with the MainSequence platform, including:

- which assets are created
- which portfolios are created or used
- which market objects are registered or updated
- how those objects relate to the project workflow

### 4. Instruments
Explain how `mainsequence.instruments` is used, including:

- which instrument types are mapped
- how the mapping logic works
- how identifiers are resolved
- any transformation or normalization rules
- any assumptions or limitations in the mapping

### 5. Dashboards
Explain which dashboards are created, what they show, and how they relate to the underlying data and workflows.

### 6. Documentation Map
The root `README.md` must explain how the documentation is organized and where each topic lives.

## MainSequence SDK Review and Contribution Rules

Review the MainSequence SDK documentation here:

`https://github.com/mainsequence-sdk/mainsequence-sdk/tree/main/docs`

Then identify any inconsistencies, missing explanations, unclear behavior, or possible improvements relevant to this project.

Do not create a separate improvement file. Instead:

- convert findings into actionable open tasks in `astro/tasks.md`
- record the review results and historical context in `astro/journal.md`

This review should include:

- inconsistencies between this project and MainSequence documentation
- missing documentation in this project
- missing or unclear documentation in MainSequence
- SDK usability issues discovered while working on this project
- concrete suggestions to improve the MainSequence SDK or documentation

If you find any error, bug, inefficiency, or anything important to highlight in `mainsequence-sdk`, open an issue ticket on the public repository.

If you open an issue or a pull request related to a project finding, record it in:

- `astro/journal.md`
- `astro/tasks.md` if further work is still required

## CLI Verification Requirements

Use the CLI to verify the actual state of the project, including at minimum:

- `DataNodes`
- `DataNode` updates
- data availability
- jobs
- dashboards
- assets
- portfolios
- related platform resources used by the project

Before verification:

1. Upgrade to the latest MainSequence SDK with the CLI.
2. Run `mainsequence project refresh_token`.

If live verification is not possible, state that clearly and provide the exact CLI commands that must be run.

## Expected Output Style

- Be concise but complete.
- Prefer explicit facts over vague statements.
- Do not use machine-specific assumptions.
- Surface failures early.
- When unsure, verify with the CLI.
- When something looks like an SDK problem, document it, suggest a concrete improvement, and open a public issue when appropriate.
