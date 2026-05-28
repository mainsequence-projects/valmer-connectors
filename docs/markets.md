# Markets

## MainSequence Objects Used

This repository interacts with MainSequence in these ways:

- reads Valmer source artifacts from a bucket
- registers or reuses custom `Asset` objects
- attaches pricing details to target bond assets
- refreshes a Valmer TIIE 28 curve through the canonical `discount_curves`
  DataNode

## Assets

Assets are keyed as:

- `tipovalor_emisora_serie`

New assets are registered with the same value for:

- `unique_identifier`
- `snapshot.name`
- `snapshot.ticker`

## Pricing Reference Data

The bootstrap module upserts these Mexican reference-rate `Index` rows with
`index_type=interest_rate`:

- `TIIE_OVERNIGHT`
- `TIIE_28`
- `TIIE_91`
- `TIIE_182`
- `CETE_28`
- `CETE_91`
- `CETE_182`

Pricing conventions are stored in `IndexConventionDetails`, and the Valmer
curve identity is stored as `Curve.unique_identifier = "VALMER_TIIE_28"`.

The project no longer uses Main Sequence constants as the durable identity
layer for reference rates or curves.

## Objects Not Created

This repository still does not create:

- MainSequence portfolios
- asset translation tables
- dashboard images or resource releases by itself

Those actions remain part of the deployment workflow in `docs/deployment.md`.
