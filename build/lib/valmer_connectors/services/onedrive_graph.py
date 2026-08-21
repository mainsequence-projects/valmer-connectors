from __future__ import annotations

import datetime as dt
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

from valmer_connectors.settings import (
    DEFAULT_VALMER_ONEDRIVE_DRIVE_ID_CONSTANT_NAME,
    resolve_valmer_onedrive_cache_path,
    resolve_valmer_onedrive_client_id_secret_name,
    resolve_valmer_onedrive_client_secret_secret_name,
    resolve_valmer_onedrive_folder_path,
    resolve_valmer_onedrive_graph_page_size,
    resolve_valmer_onedrive_tenant_id_secret_name,
)

LOGGER = structlog.get_logger(__name__)

GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
VALMER_VECTOR_FILE_RE = re.compile(r"^VectorAnalitico24h_.*\.xls[x]?$", re.IGNORECASE)


@dataclass(frozen=True)
class OneDriveGraphConfig:
    tenant_id_secret_name: str
    client_id_secret_name: str
    client_secret_secret_name: str
    drive_id: str
    folder_path: str
    cache_path: Path
    page_size: int


@dataclass(frozen=True)
class OneDriveGraphFile:
    item_id: str
    name: str
    size: int | None
    last_modified: str | None


def _secret_value(name: str) -> str:
    env_value = os.environ.get(name)
    if env_value not in (None, ""):
        return env_value

    from mainsequence.client.models_foundry import Secret

    try:
        secret = Secret.get(name=name)
    except Exception as exc:
        raise RuntimeError(
            f"OneDrive Graph credential {name!r} was not found. Set environment "
            f"variable {name} or create a Main Sequence Secret with that name."
        ) from exc
    value = secret.value
    if value is None:
        raise RuntimeError(f"Main Sequence Secret {name!r} did not return a readable value.")
    if hasattr(value, "get_secret_value"):
        resolved = value.get_secret_value()
    else:
        resolved = str(value)
    if not resolved:
        raise RuntimeError(f"Main Sequence Secret {name!r} is empty.")
    return resolved


def _constant_value(name: str) -> str | None:
    from mainsequence.client.models_foundry import Constant

    try:
        value = Constant.get_value(name)
    except Exception:
        return None
    if value in (None, ""):
        return None
    return str(value)


def resolve_onedrive_graph_config(
    *,
    drive_id: str | None = None,
    folder_path: str | None = None,
    cache_path: str | None = None,
    tenant_id_secret_name: str | None = None,
    client_id_secret_name: str | None = None,
    client_secret_secret_name: str | None = None,
    page_size: int | None = None,
) -> OneDriveGraphConfig:
    resolved_drive_id = (
        drive_id
        or os.environ.get("VALMER_ONEDRIVE_DRIVE_ID")
        or _constant_value(DEFAULT_VALMER_ONEDRIVE_DRIVE_ID_CONSTANT_NAME)
    )
    if not resolved_drive_id:
        raise RuntimeError(
            "OneDrive Graph source requires drive id. Set CLI --onedrive-drive-id, "
            "environment VALMER_ONEDRIVE_DRIVE_ID, or Main Sequence Constant "
            f"{DEFAULT_VALMER_ONEDRIVE_DRIVE_ID_CONSTANT_NAME}."
        )

    return OneDriveGraphConfig(
        tenant_id_secret_name=resolve_valmer_onedrive_tenant_id_secret_name(
            tenant_id_secret_name
        ),
        client_id_secret_name=resolve_valmer_onedrive_client_id_secret_name(
            client_id_secret_name
        ),
        client_secret_secret_name=resolve_valmer_onedrive_client_secret_secret_name(
            client_secret_secret_name
        ),
        drive_id=resolved_drive_id,
        folder_path=resolve_valmer_onedrive_folder_path(folder_path),
        cache_path=Path(resolve_valmer_onedrive_cache_path(cache_path)).expanduser(),
        page_size=resolve_valmer_onedrive_graph_page_size(page_size),
    )


def _http_json(
    request: urllib.request.Request,
    *,
    timeout: int = 120,
) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Graph HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Graph request failed: {exc.reason}") from exc
    return json.loads(payload.decode("utf-8"))


def _acquire_graph_token(config: OneDriveGraphConfig) -> str:
    tenant_id = _secret_value(config.tenant_id_secret_name)
    client_id = _secret_value(config.client_id_secret_name)
    client_secret = _secret_value(config.client_secret_secret_name)

    form = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": GRAPH_SCOPE,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    payload = _http_json(request)
    token = payload.get("access_token")
    if not token:
        raise RuntimeError("Microsoft Graph token response did not include access_token.")
    return str(token)


def _graph_request(url: str, token: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )


def list_onedrive_vector_files(
    *,
    config: OneDriveGraphConfig,
    token: str,
) -> list[OneDriveGraphFile]:
    folder = urllib.parse.quote(config.folder_path.strip("/"), safe="/")
    url = (
        f"{GRAPH_ROOT}/drives/{urllib.parse.quote(config.drive_id)}/root:/{folder}:/children"
        f"?$top={config.page_size}"
    )
    files: list[OneDriveGraphFile] = []
    while url:
        payload = _http_json(_graph_request(url, token))
        for item in payload.get("value", []):
            if not isinstance(item, dict) or "file" not in item:
                continue
            name = str(item.get("name") or "")
            if not VALMER_VECTOR_FILE_RE.match(name):
                continue
            files.append(
                OneDriveGraphFile(
                    item_id=str(item["id"]),
                    name=name,
                    size=item.get("size"),
                    last_modified=item.get("lastModifiedDateTime"),
                )
            )
        url = payload.get("@odata.nextLink")
    return files


def _file_time_index_from_name(name: str) -> dt.datetime | None:
    match = re.search(r"(\d{4}-\d{2}-\d{2}|\d{8})", name)
    if match is None:
        return None
    raw_date = match.group(1)
    if "-" in raw_date:
        valuation_date = dt.date.fromisoformat(raw_date)
    else:
        valuation_date = dt.datetime.strptime(raw_date, "%Y%m%d").date()
    return dt.datetime.combine(valuation_date, dt.time(23, 59, 59, tzinfo=dt.UTC))


def select_onedrive_vector_files_for_update(
    files: Iterable[OneDriveGraphFile],
    *,
    latest_time_index: dt.datetime | None,
) -> list[OneDriveGraphFile]:
    if latest_time_index is not None:
        latest_time_index = latest_time_index.astimezone(dt.UTC)
    selected: list[OneDriveGraphFile] = []
    for item in files:
        file_time_index = _file_time_index_from_name(item.name)
        if latest_time_index is None or file_time_index is None or file_time_index > latest_time_index:
            selected.append(item)
    return selected


def _cache_file_is_current(path: Path, item: OneDriveGraphFile) -> bool:
    if not path.exists() or not path.is_file():
        return False
    if item.size is None:
        return True
    return path.stat().st_size == int(item.size)


def download_onedrive_vector_files(
    *,
    config: OneDriveGraphConfig,
    token: str,
    files: list[OneDriveGraphFile],
) -> list[Path]:
    config.cache_path.mkdir(parents=True, exist_ok=True)
    local_paths: list[Path] = []
    for item in files:
        destination = config.cache_path / item.name
        local_paths.append(destination)
        if _cache_file_is_current(destination, item):
            continue

        tmp_destination = destination.with_suffix(f"{destination.suffix}.part")
        url = f"{GRAPH_ROOT}/drives/{urllib.parse.quote(config.drive_id)}/items/{urllib.parse.quote(item.item_id)}/content"
        request = _graph_request(url, token)
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                with tmp_destination.open("wb") as handle:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        handle.write(chunk)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Graph download failed for {item.name}: HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Graph download failed for {item.name}: {exc.reason}") from exc

        tmp_destination.replace(destination)
    return local_paths


def stage_onedrive_vector_files(
    *,
    latest_time_index: dt.datetime | None,
    drive_id: str | None = None,
    folder_path: str | None = None,
    cache_path: str | None = None,
    tenant_id_secret_name: str | None = None,
    client_id_secret_name: str | None = None,
    client_secret_secret_name: str | None = None,
) -> list[Path]:
    config = resolve_onedrive_graph_config(
        drive_id=drive_id,
        folder_path=folder_path,
        cache_path=cache_path,
        tenant_id_secret_name=tenant_id_secret_name,
        client_id_secret_name=client_id_secret_name,
        client_secret_secret_name=client_secret_secret_name,
    )
    token = _acquire_graph_token(config)
    remote_files = list_onedrive_vector_files(config=config, token=token)
    selected_files = select_onedrive_vector_files_for_update(
        remote_files,
        latest_time_index=latest_time_index,
    )
    LOGGER.info(
        "Selected OneDrive Graph Valmer vector files",
        remote_file_count=len(remote_files),
        selected_file_count=len(selected_files),
        cache_path=str(config.cache_path),
    )
    return download_onedrive_vector_files(
        config=config,
        token=token,
        files=selected_files,
    )
