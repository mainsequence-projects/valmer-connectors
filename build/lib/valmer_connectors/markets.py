from __future__ import annotations

from typing import ClassVar

from msm.base import MarketsMetaTableMixin, MarketsTimeIndexMetaTableMixin

VALMER_MARKETS_NAMESPACE = "valmer_connectors"
VALMER_MARKETS_STORAGE_APP = "valmer_connectors"


class ValmerMarketsMetaTableMixin(MarketsMetaTableMixin):
    __abstract__ = True
    __metatable_namespace__: ClassVar[str] = VALMER_MARKETS_NAMESPACE
    __markets_storage_app__: ClassVar[str] = VALMER_MARKETS_STORAGE_APP


class ValmerMarketsTimeIndexMetaTableMixin(MarketsTimeIndexMetaTableMixin):
    __abstract__ = True
    __metatable_namespace__: ClassVar[str] = VALMER_MARKETS_NAMESPACE
    __markets_storage_app__: ClassVar[str] = VALMER_MARKETS_STORAGE_APP


__all__ = [
    "VALMER_MARKETS_NAMESPACE",
    "VALMER_MARKETS_STORAGE_APP",
    "ValmerMarketsMetaTableMixin",
    "ValmerMarketsTimeIndexMetaTableMixin",
]
