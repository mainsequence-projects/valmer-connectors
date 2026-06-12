# Valmer Monitoring Dashboard

This folder contains the project-specific Streamlit dashboard for
`valmer-connectors`.

## Scope

The dashboard now exposes:

- `app.py`: top-level monitoring overview
- `pages/1_Source_Data.py`: recent `vector_de_precios_valmer` coverage plus
  source exploration for a focused asset
- `pages/2_Pricing_Hydration.py`: asset pricing-detail hydration status
- `pages/3_Curve_Health.py`: canonical `discount_curves` DataNode checks for
  the Valmer TIIE and MXN government curves

The app reuses MainSequence Streamlit scaffolding and sidebar components for
logged-user context and asset search.

## Local Run

```bash
streamlit run dashboards/valmer_monitor/app.py
```

## Deployment Note

The project currently has no verified dashboard resources or project images for
the current remote head. See `docs/deployment.md` for the latest
deployment procedure and CLI verification commands.
