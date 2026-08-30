from __future__ import annotations

import datetime as dt
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

HealthStatus = Literal["healthy", "warning", "stale", "running", "failed"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PageInfo(StrictModel):
    pageIndex: int = Field(ge=0)
    pageSize: int = Field(ge=1)
    totalItems: int = Field(ge=0)
    hasNextPage: bool
    hasPreviousPage: bool


class ResourceCollection(StrictModel):
    items: list[dict[str, Any]]
    pageInfo: PageInfo


class ExplicitSelection(StrictModel):
    mode: Literal["explicit"]
    uids: list[str] = Field(min_length=1)

    @field_validator("uids")
    @classmethod
    def unique_uids(cls, value: list[str]) -> list[str]:
        normalized = [uid.strip() for uid in value]
        if any(not uid for uid in normalized):
            raise ValueError("selection UIDs cannot be empty")
        if len(normalized) != len(set(normalized)):
            raise ValueError("selection UIDs must be unique")
        return normalized


class AllMatchingQuery(StrictModel):
    search: str | None = None
    filters: dict[str, Any]


class AllMatchingSelection(StrictModel):
    mode: Literal["all_matching"]
    query: AllMatchingQuery


class BulkActionExecution(StrictModel):
    selection: ExplicitSelection | AllMatchingSelection
    options: dict[str, Any]


class BulkActionPreflight(BaseModel):
    model_config = ConfigDict(extra="allow")

    allowed: bool
    detail: str | None = None
    matched_count: int = Field(default=0, ge=0)
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class Metric(StrictModel):
    id: str
    label: str
    display: str
    value: int | float | str | None
    detail: str
    status: HealthStatus


class OverviewResponse(StrictModel):
    generated_at: dt.datetime
    status: HealthStatus
    environment: str | None
    metrics: list[Metric]
    failures: list[str]


class PipelineAction(StrictModel):
    key: str
    name: str
    description: str
    execution_path: str
    dependencies: list[str]
    available: bool
    job_uid: str | None
    status: str
    last_run_status: str | None
    last_run_at: str | None
    image_status: str | None
    automatic_deployment: bool | None


class PipelineStage(StrictModel):
    id: str
    label: str
    description: str
    actions: list[PipelineAction]


class PipelineResponse(StrictModel):
    stages: list[PipelineStage]
    action_dependencies: dict[str, list[str]]


class LaunchResponse(StrictModel):
    request_uid: str
    requested_by_user_uid: str
    job_uid: str
    job_run_uid: str | None
    status: str
    requested_at: dt.datetime


class CurrentUserResponse(StrictModel):
    uid: str
    role: Literal["viewer", "operator"]


__all__ = [
    "BulkActionExecution",
    "BulkActionPreflight",
    "CurrentUserResponse",
    "HealthStatus",
    "LaunchResponse",
    "Metric",
    "OverviewResponse",
    "PageInfo",
    "PipelineAction",
    "PipelineResponse",
    "PipelineStage",
    "ResourceCollection",
]
