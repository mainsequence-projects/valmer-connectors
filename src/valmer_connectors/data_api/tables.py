"""Valmer's own tables for the data API, found by MetaTable identifier (ADR 0010).

The data API attaches no markets runtime. It finds the registered MetaTable of
each Valmer table by identifier, as the control plane does, and builds
SQLAlchemy statements against copies of the table definitions bound to the
registered physical names. Statements compile offline for PostgreSQL and run
through ``MetaTable.execute_operation`` as the API's workload User.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Column, MetaData, Table

from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable

VECTOR_TABLE_IDENTIFIER = str(ValmerVectorPricesStorage.__table__.name)
ASSET_DETAILS_TABLE_IDENTIFIER = str(ValmerAssetDetailsTable.__table__.name)
STATEMENT_TIMEOUT_MS = 60_000
REQUEST_TIMEOUT_SECONDS = 90


class DataApiError(RuntimeError):
    """A Valmer table could not be read; the API answers 503."""


class DataApiNotFound(LookupError):
    """The requested Valmer data does not exist; the API answers 404."""


class DataApiBadRequest(ValueError):
    """The request names something the contract does not allow; the API answers 400."""


@dataclass(frozen=True)
class BoundTables:
    """The registered Valmer tables, as SQLAlchemy tables with their physical names."""

    vector: Table
    details: Table
    data_source_uid: str
    vector_meta_table: Any
    details_meta_table: Any


def bind_table(source: Table, meta_table: Any) -> Table:
    """Copy ``source``'s columns onto the registered physical table of ``meta_table``."""

    name = str(getattr(meta_table, "physical_table_name", "") or "").strip()
    if not name:
        raise DataApiError(
            f"Registered MetaTable {getattr(meta_table, 'identifier', '?')!r} has no physical table."
        )
    schema = str(getattr(meta_table, "physical_schema", "") or "").strip() or None
    return Table(
        name,
        MetaData(),
        *(Column(column.name, column.type) for column in source.columns),
        schema=schema,
    )


def _find_registered(model: type, identifier: str) -> Any:
    matches = list(model.filter(identifier=identifier, timeout=60))
    if not matches:
        raise DataApiError(f"The Valmer table {identifier!r} is not registered.")
    if len(matches) != 1:
        raise DataApiError(f"Expected one registered MetaTable for {identifier!r}; found {len(matches)}.")
    return matches[0]


def _registered_tables() -> tuple[Any, Any]:
    from metatables import MetaTable, TimeIndexMetaTable

    return (
        _find_registered(TimeIndexMetaTable, VECTOR_TABLE_IDENTIFIER),
        _find_registered(MetaTable, ASSET_DETAILS_TABLE_IDENTIFIER),
    )


def _execute(operation: Any) -> Mapping[str, Any] | list[Any]:
    from metatables import MetaTable

    return MetaTable.execute_operation(operation, timeout=REQUEST_TIMEOUT_SECONDS)


class ValmerTables:
    """Resolves the registered Valmer tables once and runs read statements on them.

    ``finder`` returns the (vector, details) MetaTables and ``executor`` runs one
    compiled operation; tests pass fakes for both.
    """

    def __init__(
        self,
        *,
        finder: Callable[[], tuple[Any, Any]] | None = None,
        executor: Callable[[Any], Mapping[str, Any] | list[Any]] | None = None,
    ) -> None:
        self._finder = finder or _registered_tables
        self._executor = executor or _execute
        self._bound: BoundTables | None = None
        self._lock = threading.Lock()

    def bound(self) -> BoundTables:
        if self._bound is None:
            with self._lock:
                if self._bound is None:
                    vector_meta_table, details_meta_table = self._finder()
                    data_source_uid = str(getattr(vector_meta_table, "data_source_uid", "") or "")
                    if not data_source_uid:
                        raise DataApiError("The Valmer vector MetaTable names no DataSource.")
                    details_source = str(getattr(details_meta_table, "data_source_uid", "") or "")
                    if details_source and details_source != data_source_uid:
                        raise DataApiError("The Valmer tables live in different DataSources.")
                    self._bound = BoundTables(
                        vector=bind_table(ValmerVectorPricesStorage.__table__, vector_meta_table),
                        details=bind_table(ValmerAssetDetailsTable.__table__, details_meta_table),
                        data_source_uid=data_source_uid,
                        vector_meta_table=vector_meta_table,
                        details_meta_table=details_meta_table,
                    )
        return self._bound

    def rows(self, statement: Any, *, max_rows: int) -> list[dict[str, Any]]:
        """Run one read statement and return at most ``max_rows`` rows."""

        from metatables.compiled_sql.v1 import compile_sqlalchemy_statement
        from msm.api.base import operation_result_rows

        operation = compile_sqlalchemy_statement(
            statement,
            operation="select",
            data_source_uid=self.bound().data_source_uid,
            dialect="postgresql",
            limits={"max_rows": max_rows, "statement_timeout_ms": STATEMENT_TIMEOUT_MS},
        )
        try:
            result = self._executor(operation)
        except Exception as exc:
            raise DataApiError(f"The Valmer table read failed: {exc}") from exc
        if isinstance(result, Mapping) and result.get("truncated"):
            raise DataApiBadRequest(f"The request matches more than {max_rows:,} rows; narrow it.")
        return [dict(row) for row in operation_result_rows(result)]

    def vector_ref(self) -> Any:
        """A read-only ``TimeIndexTableRef`` on the registered vector table."""

        from metatables import TimeIndexTableRef

        return TimeIndexTableRef.from_meta_table(self.bound().vector_meta_table)


__all__ = [
    "ASSET_DETAILS_TABLE_IDENTIFIER",
    "VECTOR_TABLE_IDENTIFIER",
    "BoundTables",
    "DataApiBadRequest",
    "DataApiError",
    "DataApiNotFound",
    "ValmerTables",
    "bind_table",
]
