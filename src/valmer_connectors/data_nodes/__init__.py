from typing import Any

from valmer_connectors.data_nodes.canonical_index_values import (
    DailyIndexValuesNode,
    MarketsDailyIndexValuesStorage,
    ReferenceRateIndexValuesNode,
    ReferenceRateObservationConfiguration,
    ValmerDailyIndexValuesStorage,
)

__all__ = [
    "DailyIndexValuesNode",
    "MarketsDailyIndexValuesStorage",
    "ReferenceRateIndexValuesNode",
    "ReferenceRateObservationConfiguration",
    "ValmerDailyIndexValuesStorage",
]


def __getattr__(name: str) -> Any:
    if name == "DailyIndexValuesStorage":
        from valmer_connectors.data_nodes import canonical_index_values

        # Warns: a deprecated alias of ValmerDailyIndexValuesStorage.
        return canonical_index_values.DailyIndexValuesStorage
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
