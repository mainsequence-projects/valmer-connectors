# ADR 0009: External Reference Rates Use Canonical Daily Index Values

## Status

Superseded by the canonical `IndexValuesTS.1d` implementation and
`docs/implementation/valmer-curve-quote-index-pipeline-refactor.md`.

## Decision

FRED Treasury yields, the Federal Funds target upper limit, and the Banco de
Mexico policy target are source-published `Index` observations. They are stored
in `IndexValuesTS.1d`, not in project-specific reference-rate storage and not in
pricing `IndexFixingsStorage`.

The six stable Index identifiers remain:

- `US_TREASURY_CMT_2Y`
- `US_TREASURY_CMT_5Y`
- `US_TREASURY_CMT_10Y`
- `US_TREASURY_CMT_30Y`
- `FED_FUNDS_TARGET_UPPER`
- `BANXICO_POLICY_TARGET`

Values are normalized to decimal, `definition_uid` is null, and source
provenance is stored in bounded metadata. Benchmark selection, date alignment,
spread calculations, and policy-rate analytics remain downstream concerns.

## Migration

Project revision `0004` copied and reconciled all 8,637 observations into the
canonical daily storage and dropped the obsolete physical table. No runtime
compatibility reader, translator, dual writer, or fallback remains.
