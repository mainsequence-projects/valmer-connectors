# Markets

## MainSequence Objects Used

This repository interacts with MainSequence in these ways:

- reads Valmer source artifacts from a bucket
- registers or reuses custom `Asset` objects
- attaches pricing details to target bond assets
- refreshes a Valmer TIIE 28 discount curve

## Assets

Assets are keyed as:

- `tipovalor_emisora_serie`

New assets are registered with the same value for:

- `unique_identifier`
- `snapshot.name`
- `snapshot.ticker`

## Constants

The bootstrap module seeds or resolves these constants:

- `REFERENCE_RATE__TIIE_OVERNIGHT`
- `REFERENCE_RATE__TIIE_28`
- `REFERENCE_RATE__TIIE_91`
- `REFERENCE_RATE__TIIE_182`
- `REFERENCE_RATE__CETE_28`
- `REFERENCE_RATE__CETE_91`
- `REFERENCE_RATE__CETE_182`
- `ZERO_CURVE__VALMER_TIIE_28`

The CETE pricing registration can also use:

- `ZERO_CURVE__BANXICO_M_BONOS_OTR`

when that external curve constant already exists in the runtime.

## Objects Not Created

This repository still does not create:

- MainSequence portfolios
- asset translation tables
- dashboard images or resource releases by itself

Those actions remain part of the deployment workflow in `docs/deployment.md`.
