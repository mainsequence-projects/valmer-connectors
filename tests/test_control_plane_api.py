from __future__ import annotations

import datetime as dt
import importlib
import json
import os
import pathlib
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import jwt
import msm
import msm_pricing.bootstrap
import pandas as pd
import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric import ed25519
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from metatables import MetaTable, MetaTableCompiledSQLOperation
from msm.models import AssetTable
from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable

from mainsequence import code_repository_context as sdk_code_repository_context
from mainsequence.client.models_foundry import CodeRepositoryBranch
from mainsequence.client.models_user import OrganizationEnvironment
from mainsequence.code_repository_context import (
    CodeRepositoryBranchContextRequiredError,
    CodeRepositoryContext,
    CodeRepositoryEnvironmentContextRequiredError,
    GitCodeRepositorySourceContext,
)
from mainsequence.server.caller_assertions import (
    ASSERTION_HEADER,
    ASSERTION_TYPE,
    CallerAssertionVerifier,
)
from valmer_connectors.control_plane import service as control_plane_service
from valmer_connectors.control_plane.api import _request_user_uid, create_app
from valmer_connectors.control_plane.catalog import (
    CANONICAL_ASSET_TABLE_IDENTIFIER,
    CURRENT_PRICING_DETAILS_TABLE_IDENTIFIER,
    DATA_PRODUCTS,
    DISCOUNT_CURVE_TABLE_IDENTIFIER,
    INDEX_FIXINGS_TABLE_IDENTIFIER,
    INDEX_VALUES_TABLE_IDENTIFIER,
    JOB_LAUNCH_PROFILES,
    VALMER_ASSET_DETAILS_TABLE_IDENTIFIER,
    VECTOR_TABLE_IDENTIFIER,
)
from valmer_connectors.control_plane.service import (
    ControlPlaneError,
    ControlPlaneService,
    Job,
    PlatformControlPlaneGateway,
    _job_launch_profile,
    _qualified_table_name,
    _select_meta_table_rows,
)
from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable
from valmer_connectors.services.migrations import MIGRATION_WORKFLOW_PATH

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[1]


class FakeControlPlaneGateway:
    def __init__(self) -> None:
        self.launched_job_uids: list[str] = []
        self.launched_command_args: list[list[str]] = []
        self.job_status = "succeeded"
        self.validated_metatable_sources = 0

    def data_products(self) -> list[dict[str, object]]:
        base = {
            "table_identifier": "example",
            "cadence": "1d",
            "latest_observation": "2026-08-30T00:00:00+00:00",
            "lag_hours": 1.0,
            "latest_rows": 4,
            "description": "Test product",
            "error": None,
        }
        return [
            {
                **base,
                "uid": "valmer-assets",
                "name": "Registered Valmer assets",
                "category": "registry",
                "status": "healthy",
                "latest_rows": 12,
            },
            {
                **base,
                "uid": "pricing-details",
                "name": "Current pricing details",
                "category": "registry",
                "status": "healthy",
                "latest_rows": 10,
            },
            {
                **base,
                "uid": "valmer-vector",
                "name": "Valmer vector",
                "category": "source",
                "status": "healthy",
            },
            *[
                {
                    **base,
                    "uid": f"curve-{curve}",
                    "name": f"{curve.upper()} curve",
                    "category": "curve",
                    "status": "healthy",
                }
                for curve in ("tiie", "sofr", "xccy", "government")
            ],
        ]

    def assets(self) -> list[dict[str, object]]:
        return [
            {
                "uid": "BI_CETES_260101",
                "name": "CETES 260101",
                "security_type": "BI",
                "issuer": "CETES",
                "series": "260101",
                "currency": "MPS",
                "sector": "Government",
                "dirty_price": 99.2,
                "yield_rate": 0.091,
                "duration": 0.25,
                "latest_observation": "2026-08-30T00:00:00+00:00",
                "pricing_target": True,
                "status": "healthy",
            }
        ]

    def jobs(self) -> list[dict[str, object]]:
        return [
            {
                "uid": f"job-{index}",
                "key": profile.key,
                "name": f"Job {profile.key}",
                "description": f"Purpose of {profile.key}.",
                "status": self.job_status,
                "last_run_status": "SUCCEEDED",
                "last_run_at": "2026-08-30T00:00:00+00:00",
                "execution_path": profile.execution_path,
                "schedule": None,
                "image_status": "ready",
                "automatic_deployment": True,
                "dependencies": list(profile.dependencies),
                "parameter_count": len(profile.parameters),
            }
            for index, profile in enumerate(JOB_LAUNCH_PROFILES, start=1)
        ]

    def environment_name(self) -> str:
        return "Test"

    def job_runs(self) -> list[dict[str, object]]:
        return [
            {
                "uid": "run-1",
                "job_uid": "job-1",
                "job_name": "Valmer Vector Refresh",
                "status": "SUCCEEDED",
                "execution_start": "2026-08-30T00:00:00+00:00",
                "execution_end": "2026-08-30T00:02:00+00:00",
                "commit_hash": "a" * 40,
                "triggered_by": "user",
                "logs_url": None,
                "resource_usage_url": None,
            }
        ]

    def validate_configured_vector_source(self) -> dict[str, object]:
        self.validated_metatable_sources += 1
        return {
            "uid": "metatable-1",
            "identifier": "ValmerSource",
            "physical_table_name": "vector_precios_gubernamental",
        }

    def run_job(
        self,
        job_uid: str,
        command_args: list[str],
    ) -> tuple[SimpleNamespace, dict[str, str]]:
        self.launched_job_uids.append(job_uid)
        self.launched_command_args.append(command_args)
        return SimpleNamespace(name="Valmer Vector Refresh"), {
            "uid": "run-2",
            "status": "PENDING",
        }


# create_app() installs the SDK request identity, so these tests authenticate the
# way real callers do: with a platform-signed caller assertion when hosted, and with
# a Bearer token that MAINSEQUENCE_ENDPOINT/api/v1/users/me/ accepts locally.
# Nothing overrides the caller dependency.
VIEWER_UID = "9f68191a-54c2-4e3f-bd07-0f1c6dbe60e9"
OPERATOR_UID = "f90e2937-6360-4e9b-96ac-eca8964e8213"
RELEASE_UID = "c417f27d-8da0-49cf-8b1c-42349cea002c"
OTHER_RELEASE_UID = "1a35c6db-5371-4f50-a983-d440ff56c507"
HOSTED_ENVIRONMENT_UID = "d1838f05-d157-41cb-af9a-3e9509fb05b9"
ASSERTION_ISSUER = "https://backend.example.test"
ASSERTION_KEY_ID = "caller-key"
LOCAL_BEARER_TOKENS = {"viewer-token": VIEWER_UID, "operator-token": OPERATOR_UID}
LOCAL_AUTHENTICATOR_OUTAGE_TOKEN = "authenticator-outage"
# Every variable that selects or configures the SDK's hosted (assertion) mode.
HOSTED_RUNTIME_VARIABLES = (
    "APP_NAME",
    "FASTAPI_PUBLIC_BASE_URL",
    "MAINSEQUENCE_CALLER_AUTH_MODE",
    "MAINSEQUENCE_CALLER_ASSERTION_ISSUER",
    "MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL",
    "MAINSEQUENCE_ORGANIZATION_ENVIRONMENT_UID",
    "FASTAPI_PUBLIC_INGRESS",
)
ENTRY_MODULE = "api.valmer_control_plane.main"
_REAL_VERIFIER_FROM_ENVIRONMENT = CallerAssertionVerifier.from_environment.__func__


def _deployed_public_ingress() -> list[dict[str, str]]:
    """Return the API's workflow `public_ingress`, which the platform projects as
    FASTAPI_PUBLIC_INGRESS (an absent declaration is an empty list)."""
    workflow = yaml.safe_load((REPOSITORY_ROOT / MIGRATION_WORKFLOW_PATH).read_text("utf-8"))
    (api,) = [resource for resource in workflow["resources"] if resource["kind"] == "fastapi"]
    return list(api["spec"].get("public_ingress", []))


def _public_ingress_pairs() -> tuple[tuple[str, str], ...]:
    return tuple((route["method"], route["path"]) for route in _deployed_public_ingress())


def _service(gateway: object | None = None) -> ControlPlaneService:
    return ControlPlaneService(
        gateway=gateway or FakeControlPlaneGateway(),
        operator_uids={OPERATOR_UID},
        now=lambda: dt.datetime(2026, 8, 30, 12, tzinfo=dt.UTC),
    )


class UnreachableGateway:
    """Fails the test when a rejected request reaches a route handler."""

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"A rejected request reached the gateway ({name}).")


class CallerAssertionAuthority:
    """Sign caller assertions as the platform does, with a key made for the test."""

    def __init__(self) -> None:
        self.key = ed25519.Ed25519PrivateKey.generate()
        public_key = jwt.algorithms.OKPAlgorithm.to_jwk(self.key.public_key(), as_dict=True)
        self.jwks = {
            "keys": [
                {
                    "kid": ASSERTION_KEY_ID,
                    "alg": "EdDSA",
                    "use": "sig",
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "x": public_key["x"],
                }
            ]
        }
        self.key_discovery_available = True

    def fetch_jwks(self) -> dict[str, object]:
        if not self.key_discovery_available:
            raise RuntimeError("Caller key discovery is unavailable.")
        return self.jwks

    def sign(
        self,
        user_uid: str,
        *,
        key: ed25519.Ed25519PrivateKey | None = None,
        omit: tuple[str, ...] = (),
        **claims: object,
    ) -> str:
        now = int(time.time())
        payload = {
            "iss": ASSERTION_ISSUER,
            "aud": f"urn:mainsequence:fapi:{RELEASE_UID}",
            "sub": user_uid,
            "resource_release_uid": RELEASE_UID,
            "organization_environment_uid": HOSTED_ENVIRONMENT_UID,
            "iat": now,
            "nbf": now,
            "exp": now + 120,
            # What the platform states about every caller: whether it is an
            # Organization admin. SDK 9.0.29 and later (which ms-markets 3.0
            # requires) refuse an assertion without it and one that lists the
            # caller's teams; membership is checked on demand instead.
            "is_organization_admin": False,
            **claims,
        }
        for name in omit:
            del payload[name]
        return jwt.encode(
            payload,
            key or self.key,
            algorithm="EdDSA",
            headers={"kid": ASSERTION_KEY_ID, "typ": ASSERTION_TYPE},
        )


@pytest.fixture
def hosted_runtime(monkeypatch) -> CallerAssertionAuthority:
    """Project the configuration the platform gives a deployed FastAPI release.

    Only the download of the platform's public key is replaced; the verifier still
    reads its issuer, release and Environment from these variables.
    """
    authority = CallerAssertionAuthority()
    for name, value in {
        "APP_NAME": RELEASE_UID,
        "FASTAPI_PUBLIC_BASE_URL": "https://valmer-control-plane.example.test",
        "MAINSEQUENCE_CALLER_AUTH_MODE": "assertion",
        "MAINSEQUENCE_CALLER_ASSERTION_ISSUER": ASSERTION_ISSUER,
        "MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL": f"{ASSERTION_ISSUER}/fastapi/caller-keys/",
        "MAINSEQUENCE_ORGANIZATION_ENVIRONMENT_UID": HOSTED_ENVIRONMENT_UID,
        "FASTAPI_PUBLIC_INGRESS": json.dumps(_deployed_public_ingress()),
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(
        CallerAssertionVerifier,
        "from_environment",
        classmethod(
            lambda cls, **_kwargs: _REAL_VERIFIER_FROM_ENVIRONMENT(
                cls, fetch_jwks=authority.fetch_jwks
            )
        ),
    )
    return authority


@pytest.fixture(scope="module")
def users_me_endpoint():
    """Serve a loopback MAINSEQUENCE_ENDPOINT whose /api/v1/users/me/ knows two tokens."""
    received_authorizations: list[str] = []

    class UsersMeHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - http.server handler name
            authorization = self.headers.get("Authorization", "")
            received_authorizations.append(authorization)
            scheme, _, token = authorization.partition(" ")
            if self.path != "/api/v1/users/me/":
                status_code, body = 404, {"detail": "Not found."}
            elif token == LOCAL_AUTHENTICATOR_OUTAGE_TOKEN:
                status_code, body = 503, {"detail": "Authentication is unavailable."}
            elif scheme == "Bearer" and token in LOCAL_BEARER_TOKENS:
                status_code, body = 200, {"uid": LOCAL_BEARER_TOKENS[token], "username": token}
            else:
                status_code, body = 401, {"detail": "Invalid token."}
            payload = json.dumps(body).encode()
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args: object) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), UsersMeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield SimpleNamespace(
            url=f"http://127.0.0.1:{server.server_port}",
            received_authorizations=received_authorizations,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def local_runtime(monkeypatch, users_me_endpoint) -> SimpleNamespace:
    """Run without hosted markers: the SDK validates Bearer tokens against
    MAINSEQUENCE_ENDPOINT, here the loopback users/me endpoint."""
    for name in HOSTED_RUNTIME_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", users_me_endpoint.url)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    users_me_endpoint.received_authorizations.clear()
    return users_me_endpoint


@pytest.fixture
def api_client(hosted_runtime):
    """Build a TestClient for the hosted app that calls as one user."""

    def build(user_uid: str, *, gateway: FakeControlPlaneGateway | None = None) -> TestClient:
        return TestClient(
            create_app(_service(gateway)),
            headers={ASSERTION_HEADER: hosted_runtime.sign(user_uid)},
        )

    return build


def _route_requests(app: FastAPI) -> list[tuple[str, str]]:
    """One (method, path) request for every route the app mounts."""
    requests = []
    for route in app.routes:
        methods = sorted(set(getattr(route, "methods", None) or ()) - {"HEAD"})
        if methods:
            requests.append((methods[0], route.path.replace("{job_uid}", "job-1")))
    return requests


JOB_RUN_REQUEST = {"selection": {"mode": "explicit", "uids": ["job-1"]}, "options": {}}


def _assert_every_route_rejects(
    app: FastAPI,
    headers: dict[str, str],
    status_code: int,
) -> None:
    requests = _route_requests(app)
    assert ("GET", "/api/v1/control-plane/me") in requests
    assert ("POST", "/api/v1/control-plane/jobs/actions/run") in requests
    with TestClient(app) as client:
        for method, path in requests:
            response = client.request(
                method,
                path,
                headers=headers,
                json=JOB_RUN_REQUEST if method == "POST" else None,
            )
            assert response.status_code == status_code, (method, path, response.text)


def test_hosted_app_declares_assertion_identity_and_the_deployment_public_ingress(
    hosted_runtime,
) -> None:
    app = create_app(_service())

    assert app.state.mainsequence_request_identity == {
        "installed": True,
        "mode": "assertion",
        "public_ingress": _public_ingress_pairs(),
    }
    assert [middleware.cls.__name__ for middleware in app.user_middleware] == [
        "_RequestIdentityMiddleware"
    ]


def test_hosted_markers_select_assertion_mode_without_an_explicit_mode(
    hosted_runtime,
    monkeypatch,
) -> None:
    monkeypatch.delenv("MAINSEQUENCE_CALLER_AUTH_MODE")

    assert os.environ["FASTAPI_PUBLIC_BASE_URL"]
    assert os.environ["MAINSEQUENCE_CALLER_ASSERTION_ISSUER"]
    assert create_app(_service()).state.mainsequence_request_identity["mode"] == "assertion"


def test_local_app_declares_local_identity(local_runtime) -> None:
    app = create_app(_service())

    assert app.state.mainsequence_request_identity == {
        "installed": True,
        "mode": "local",
        "public_ingress": (),
    }


def test_the_deployment_declares_no_public_route() -> None:
    """Every route reads or acts on platform state for a known caller."""
    assert _deployed_public_ingress() == []
    assert _public_ingress_pairs() == ()


@pytest.mark.parametrize(
    ("environment", "error"),
    [
        (
            {"FASTAPI_PUBLIC_BASE_URL": "https://valmer-control-plane.example.test"},
            "Trusted runtime target configuration is invalid",
        ),
        (
            {"APP_NAME": RELEASE_UID, "MAINSEQUENCE_CALLER_AUTH_MODE": "local"},
            "Invalid caller authentication mode",
        ),
    ],
    ids=["hosted-without-trust-configuration", "hosted-asking-for-local-mode"],
)
def test_hosted_app_without_caller_verification_fails_at_creation(
    monkeypatch,
    environment: dict[str, str],
    error: str,
) -> None:
    for name in HOSTED_RUNTIME_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(RuntimeError, match=error):
        create_app(_service(UnreachableGateway()))


def test_each_created_app_installs_request_identity_exactly_once(local_runtime) -> None:
    first = create_app(_service())
    second = create_app(_service())

    for app in (first, second):
        assert app.state.mainsequence_request_identity["installed"] is True
        assert [middleware.cls.__name__ for middleware in app.user_middleware] == [
            "_RequestIdentityMiddleware"
        ]
    with pytest.raises(RuntimeError, match="already installed"):
        create_app(_service(), app=first)


def test_deployed_entry_module_installs_request_identity(hosted_runtime, monkeypatch) -> None:
    """The launcher imports the workflow's source_path and checks the app it exports."""
    # Both need a MetaTables runtime and are unrelated to caller identity.
    monkeypatch.setattr(msm, "start_engine", lambda **_kwargs: None)
    monkeypatch.setattr(msm_pricing.bootstrap, "attach_pricing_schemas", lambda **_kwargs: None)
    monkeypatch.delitem(sys.modules, ENTRY_MODULE, raising=False)
    try:
        entry_module = importlib.import_module(ENTRY_MODULE)
        first_app = entry_module.app
        reloaded_app = importlib.reload(entry_module).app
    finally:
        sys.modules.pop(ENTRY_MODULE, None)

    assert reloaded_app is not first_app
    for app in (first_app, reloaded_app):
        assert app.state.mainsequence_request_identity == {
            "installed": True,
            "mode": "assertion",
            "public_ingress": _public_ingress_pairs(),
        }


def test_platform_launcher_accepts_the_hosted_app(hosted_runtime) -> None:
    """Run PodDeploymentOrchestrator's own startup check where it is installed."""
    launcher = pytest.importorskip(
        "pod_orchestrator.fastapi_auth",
        reason="PodDeploymentOrchestrator is not installed in this environment.",
    )
    public_ingress = tuple(
        (route["method"], route["path"])
        for route in json.loads(os.environ["FASTAPI_PUBLIC_INGRESS"])
    )

    with pytest.raises(RuntimeError, match="not installed"):
        launcher.require_request_identity(FastAPI(), public_ingress=public_ingress)
    launcher.require_request_identity(create_app(_service()), public_ingress=public_ingress)


def test_hosted_handlers_see_the_caller_of_the_signed_assertion(hosted_runtime) -> None:
    with TestClient(create_app(_service())) as client:
        viewer = client.get(
            "/api/v1/control-plane/me",
            headers={
                ASSERTION_HEADER: hosted_runtime.sign(VIEWER_UID),
                "X-User-UID": OPERATOR_UID,
            },
        )
        operator = client.get(
            "/api/v1/control-plane/me",
            headers={ASSERTION_HEADER: hosted_runtime.sign(OPERATOR_UID)},
        )

    assert viewer.status_code == 200
    assert viewer.json() == {"uid": VIEWER_UID, "role": "viewer"}
    assert operator.status_code == 200
    assert operator.json() == {"uid": OPERATOR_UID, "role": "operator"}


def _expired_assertion(authority: CallerAssertionAuthority) -> dict[str, str]:
    issued_at = int(time.time()) - 600
    return {
        ASSERTION_HEADER: authority.sign(
            OPERATOR_UID, iat=issued_at, nbf=issued_at, exp=issued_at + 120
        )
    }


HOSTED_REJECTIONS = {
    "no-credentials": lambda authority: {},
    "user-uid-header-only": lambda authority: {"X-User-UID": OPERATOR_UID},
    "bearer-token-only": lambda authority: {"Authorization": "Bearer operator-token"},
    "malformed-assertion": lambda authority: {ASSERTION_HEADER: "not-a-caller-assertion"},
    "expired-assertion": _expired_assertion,
    "assertion-for-another-release": lambda authority: {
        ASSERTION_HEADER: authority.sign(
            OPERATOR_UID,
            aud=f"urn:mainsequence:fapi:{OTHER_RELEASE_UID}",
            resource_release_uid=OTHER_RELEASE_UID,
        )
    },
    "assertion-signed-by-an-untrusted-key": lambda authority: {
        ASSERTION_HEADER: authority.sign(
            OPERATOR_UID,
            key=ed25519.Ed25519PrivateKey.generate(),
        )
    },
    "assertion-without-the-caller-admin-flag": lambda authority: {
        ASSERTION_HEADER: authority.sign(OPERATOR_UID, omit=("is_organization_admin",))
    },
    "assertion-that-lists-the-caller-teams": lambda authority: {
        ASSERTION_HEADER: authority.sign(
            OPERATOR_UID, team_uids=["0b5e6a3c-7d2f-4c1e-9a8b-3f4e5d6c7b8a"]
        )
    },
    "numeric-user-id-beside-a-valid-assertion": lambda authority: {
        ASSERTION_HEADER: authority.sign(OPERATOR_UID),
        "X-User-ID": "42",
    },
}


@pytest.mark.parametrize("case", sorted(HOSTED_REJECTIONS))
def test_hosted_app_rejects_a_missing_or_invalid_caller_on_every_route(
    hosted_runtime,
    case: str,
) -> None:
    app = create_app(_service(UnreachableGateway()))

    _assert_every_route_rejects(app, HOSTED_REJECTIONS[case](hosted_runtime), 401)


def test_hosted_caller_key_outage_answers_503_without_a_fallback(hosted_runtime) -> None:
    hosted_runtime.key_discovery_available = False
    app = create_app(_service(UnreachableGateway()))

    _assert_every_route_rejects(
        app,
        {ASSERTION_HEADER: hosted_runtime.sign(OPERATOR_UID), "X-User-UID": OPERATOR_UID},
        503,
    )


def test_local_handlers_see_the_caller_of_a_valid_bearer_token(local_runtime) -> None:
    with TestClient(create_app(_service())) as client:
        viewer = client.get(
            "/api/v1/control-plane/me", headers={"Authorization": "Bearer viewer-token"}
        )
        operator = client.get(
            "/api/v1/control-plane/me", headers={"Authorization": "Bearer operator-token"}
        )

    assert viewer.status_code == 200
    assert viewer.json() == {"uid": VIEWER_UID, "role": "viewer"}
    assert operator.status_code == 200
    assert operator.json() == {"uid": OPERATOR_UID, "role": "operator"}
    assert local_runtime.received_authorizations == [
        "Bearer viewer-token",
        "Bearer operator-token",
    ]


@pytest.mark.parametrize(
    ("headers", "status_code"),
    [
        ({}, 401),
        ({"X-User-UID": OPERATOR_UID}, 401),
        ({"Authorization": "Bearer unknown-token"}, 401),
        ({"Authorization": "Basic b3BlcmF0b3I6c2VjcmV0"}, 401),
        ({"Authorization": "Bearer viewer-token", "X-User-UID": OPERATOR_UID}, 401),
        ({"Authorization": f"Bearer {LOCAL_AUTHENTICATOR_OUTAGE_TOKEN}"}, 503),
    ],
    ids=[
        "no-credentials",
        "user-uid-header-only",
        "unknown-token",
        "not-a-bearer-token",
        "user-uid-header-conflicting-with-the-token",
        "authenticator-outage",
    ],
)
def test_local_app_rejects_a_missing_or_invalid_caller_on_every_route(
    local_runtime,
    headers: dict[str, str],
    status_code: int,
) -> None:
    app = create_app(_service(UnreachableGateway()))

    _assert_every_route_rejects(app, headers, status_code)


def test_a_route_reached_without_a_caller_refuses_the_request(monkeypatch, hosted_runtime) -> None:
    """Public admission lets an anonymous request through; a route that needs a
    caller still refuses it, and the app mirrors whatever the deployment declares."""
    overview = {"method": "GET", "path": "/api/v1/control-plane/overview"}
    health = {"method": "GET", "path": "/health"}
    monkeypatch.setenv("FASTAPI_PUBLIC_INGRESS", json.dumps([overview, health]))
    app = create_app(_service(UnreachableGateway()))
    anonymous = {"X-Public-Ingress": "1"}

    with TestClient(app) as client:
        public_overview = client.get(overview["path"], headers=anonymous)
        public_health = client.get(health["path"], headers=anonymous)
        protected = client.get("/api/v1/control-plane/me", headers=anonymous)

    assert app.state.mainsequence_request_identity["public_ingress"] == (
        ("GET", overview["path"]),
        ("GET", health["path"]),
    )
    assert public_overview.status_code == 401
    assert public_overview.json() == {
        "detail": "The platform-authenticated request user is unavailable."
    }
    assert public_health.status_code == 200
    assert protected.status_code == 401


def test_caller_dependency_rejects_outside_an_authenticated_request() -> None:
    with pytest.raises(HTTPException) as rejected:
        _request_user_uid()

    assert rejected.value.status_code == 401


def test_health_answers_an_authenticated_caller(api_client) -> None:
    with api_client(VIEWER_UID) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "valmer-control-plane"}


def test_registry_reads_use_compiled_metatable_operations(monkeypatch) -> None:
    table = SimpleNamespace(
        uid="table-1",
        data_source_uid="data-source-1",
        identifier="ValmerAssetDetails",
        physical_schema="public",
        physical_table_name="valmer_asset_details",
    )
    captured: list[tuple[dict[str, object], int]] = []

    def execute(operation: dict[str, object], *, timeout: int) -> dict[str, object]:
        captured.append((operation, timeout))
        return {"rows": [{"asset_count": 12}], "truncated": False}

    monkeypatch.setattr(MetaTable, "execute_operation", execute)
    rows = _select_meta_table_rows(
        table,
        f"SELECT COUNT(*) AS asset_count FROM {_qualified_table_name(table)}",
        max_rows=1,
    )

    assert rows == [{"asset_count": 12}]
    operation = MetaTableCompiledSQLOperation(**captured[0][0])
    assert operation.operation == "select"
    assert operation.data_source_uid == "data-source-1"
    assert operation.statement.sql == (
        'SELECT COUNT(*) AS asset_count FROM "public"."valmer_asset_details"'
    )
    assert operation.limits.max_rows == 1
    assert operation.limits.statement_timeout_ms == 60_000
    assert captured[0][1] == 90


def test_registry_read_rejects_unsafe_physical_identity() -> None:
    table = SimpleNamespace(
        identifier="ValmerAssetDetails",
        physical_schema="public",
        physical_table_name='details"; DROP TABLE assets; --',
    )

    try:
        _qualified_table_name(table)
    except ControlPlaneError as exc:
        assert "unsafe physical table identity" in str(exc)
    else:
        raise AssertionError("Unsafe physical MetaTable names must be rejected.")


def test_control_plane_uses_registered_namespaced_valmer_table_identifiers() -> None:
    assert CANONICAL_ASSET_TABLE_IDENTIFIER == AssetTable.__table__.name
    assert (
        CURRENT_PRICING_DETAILS_TABLE_IDENTIFIER
        == AssetCurrentPricingDetailsTable.__table__.name
    )
    assert (
        VALMER_ASSET_DETAILS_TABLE_IDENTIFIER
        == ValmerAssetDetailsTable.__table__.name
    )
    assert VECTOR_TABLE_IDENTIFIER == ValmerVectorPricesStorage.__table__.name
    # Valmer's own curve, fixing and daily Index-value tables.
    assert INDEX_VALUES_TABLE_IDENTIFIER == "valmer_connectors__index_values__t_1d"
    assert INDEX_FIXINGS_TABLE_IDENTIFIER == "valmer_connectors__indexfixingsts__valmer"
    assert DISCOUNT_CURVE_TABLE_IDENTIFIER == "valmer_connectors__discountcurvests__valmer"
    registered_assets = next(
        product for product in DATA_PRODUCTS if product.key == "valmer-assets"
    )
    assert registered_assets.table_identifier == ValmerAssetDetailsTable.__table__.name


def test_empty_registered_asset_collection_is_not_a_failure() -> None:
    class EmptyRegistryGateway(FakeControlPlaneGateway):
        def data_products(self) -> list[dict[str, object]]:
            products = super().data_products()
            products[0]["latest_rows"] = 0
            return products

    overview = ControlPlaneService(gateway=EmptyRegistryGateway()).overview()
    registered_assets = next(
        metric for metric in overview.metrics if metric.id == "registered-assets"
    )

    assert registered_assets.value == 0
    assert registered_assets.display == "0"
    assert registered_assets.status == "healthy"
    assert registered_assets.detail == "No Valmer assets are registered yet."
    assert overview.failures == []


def test_overview_does_not_expose_upstream_html_error_pages() -> None:
    class FailedJobsGateway(FakeControlPlaneGateway):
        def jobs(self) -> list[dict[str, object]]:
            raise RuntimeError(
                "502 GET https://example.test/jobs: "
                "<!DOCTYPE html><html><body>Bad Gateway</body></html>"
            )

    overview = ControlPlaneService(gateway=FailedJobsGateway()).overview()

    assert overview.failures[-1] == "Jobs: upstream service returned HTTP 502"
    assert "DOCTYPE" not in overview.failures[-1]


def test_job_launch_profile_is_resolved_from_execution_path() -> None:
    configured = SimpleNamespace(
        name="A renamed platform Job",
        execution_path="scripts/update_vector_valmer.py",
    )
    unconfigured = SimpleNamespace(
        name="Valmer Vector Refresh",
        execution_path="scripts/another_job.py",
    )

    assert _job_launch_profile(configured) is not None
    assert _job_launch_profile(unconfigured) is None


def test_authenticated_overview_and_resource_collection(api_client) -> None:
    with api_client(VIEWER_UID) as client:
        overview = client.get("/api/v1/control-plane/overview")
        products = client.get(
            "/api/v1/control-plane/data-products",
            params={"pageIndex": 0, "pageSize": 2, "category": "curve"},
        )

    assert overview.status_code == 200
    assert overview.json()["status"] == "healthy"
    assert overview.json()["metrics"][0]["display"] == "12"
    vector_metric = next(
        metric
        for metric in overview.json()["metrics"]
        if metric["id"] == "latest-vector-observation"
    )
    assert vector_metric == {
        "id": "latest-vector-observation",
        "label": "Latest vector observation",
        "display": "2026-08-30",
        "value": "2026-08-30T00:00:00+00:00",
        "detail": "Most recent persisted Valmer vector time index.",
        "status": "healthy",
    }
    assert products.status_code == 200
    assert products.json()["pageInfo"] == {
        "pageIndex": 0,
        "pageSize": 2,
        "totalItems": 4,
        "hasNextPage": True,
        "hasPreviousPage": False,
    }


def test_overview_reports_missing_pipeline_jobs_as_a_failure() -> None:
    class MissingJobsGateway(FakeControlPlaneGateway):
        def jobs(self) -> list[dict[str, object]]:
            return []

    overview = ControlPlaneService(gateway=MissingJobsGateway()).overview()
    available_jobs = next(
        metric for metric in overview.metrics if metric.id == "available-jobs"
    )

    assert overview.status == "failed"
    assert available_jobs.display == "0"
    assert available_jobs.status == "failed"
    assert overview.failures[-1] == (
        f"Pipeline Jobs: 0 of {len(JOB_LAUNCH_PROFILES)} configured pipeline Jobs are "
        "available in this branch."
    )


ENVIRONMENT_UID = "0b9a4f8e-7c2d-4e51-9a63-5d2f1c7e8b40"


def _git_source(branch: str) -> GitCodeRepositorySourceContext:
    return GitCodeRepositorySourceContext(
        repository_root=pathlib.Path("/workspace/valmer-connectors"),
        canonical_repository_identity="github.com/example/valmer-connectors",
        repository_branch=branch,
        repository_ref=f"refs/heads/{branch}",
        commit_sha="a" * 40,
    )


def _registered_branch_context(
    *,
    environment_uid: str | None = ENVIRONMENT_UID,
    environment_name: str | None = "Production",
) -> CodeRepositoryContext:
    return CodeRepositoryContext(
        source_context=_git_source("main"),
        code_repository_uid="code-repository-1",
        code_repository_branch_uid="code-repository-branch-1",
        organization_environment_uid=environment_uid,
        status="resolved",
        process_id=os.getpid(),
        code_repository_branch=CodeRepositoryBranch.model_construct(
            uid="code-repository-branch-1",
            code_repository_uid="code-repository-1",
            repository_branch="main",
            organization_environment_uid=environment_uid,
            organization_environment_name=environment_name,
        ),
    )


def _unregistered_branch_context() -> CodeRepositoryContext:
    source = _git_source("feature/rates")
    return CodeRepositoryContext(
        source_context=source,
        code_repository_uid=None,
        code_repository_branch_uid=None,
        organization_environment_uid=None,
        status="code_repository_branch_not_registered",
        process_id=os.getpid(),
        code_repository_branch=None,
        detail=(
            "No visible CodeRepositoryBranch matches Git repository "
            f"{source.canonical_repository_identity!r} and branch "
            f"{source.repository_branch!r}."
        ),
    )


def _use_code_repository_context(monkeypatch, context: CodeRepositoryContext) -> None:
    """Install an offline SDK process context; Environment reads fail fast."""
    # The SDK guards read the process context through this function, so the
    # SDK's own guard logic and errors stay in place.
    monkeypatch.setattr(
        sdk_code_repository_context,
        "get_code_repository_context",
        lambda **_kwargs: context,
    )

    def unexpected_lookup(cls, uid, timeout=None):
        raise AssertionError("The resolved branch already names its Environment.")

    monkeypatch.setattr(OrganizationEnvironment, "get_by_uid", classmethod(unexpected_lookup))


class BranchEnvironmentGateway(PlatformControlPlaneGateway):
    """SDK-resolved Environment, fixed read models, and no MetaTable access."""

    def _time_index_tables(self):
        raise AssertionError("MetaTables do not carry an Organization Environment.")

    def data_products(self) -> list[dict[str, object]]:
        return FakeControlPlaneGateway().data_products()

    def jobs(self) -> list[dict[str, object]]:
        return FakeControlPlaneGateway().jobs()


def test_environment_name_comes_from_the_running_code_repository_branch(
    monkeypatch,
) -> None:
    _use_code_repository_context(monkeypatch, _registered_branch_context())

    assert BranchEnvironmentGateway().environment_name() == "Production"


def test_environment_name_reads_the_environment_when_the_branch_omits_its_name(
    monkeypatch,
) -> None:
    _use_code_repository_context(
        monkeypatch,
        _registered_branch_context(environment_name=None),
    )
    lookups: list[tuple[str, object]] = []

    def get_by_uid(cls, uid, timeout=None):
        lookups.append((uid, timeout))
        return OrganizationEnvironment(
            uid=uid,
            name="Production",
            organization_owner_uid="organization-1",
            required_repository_branch="main",
            is_production=True,
        )

    monkeypatch.setattr(OrganizationEnvironment, "get_by_uid", classmethod(get_by_uid))

    assert BranchEnvironmentGateway().environment_name() == "Production"
    assert lookups == [(ENVIRONMENT_UID, 60)]


def test_overview_names_the_environment_of_the_running_branch(monkeypatch) -> None:
    _use_code_repository_context(monkeypatch, _registered_branch_context())

    overview = ControlPlaneService(gateway=BranchEnvironmentGateway()).overview()

    assert overview.environment == "Production"
    assert overview.status == "healthy"
    assert overview.failures == []


@pytest.mark.parametrize(
    ("context", "expected_error"),
    [
        (_unregistered_branch_context(), CodeRepositoryBranchContextRequiredError),
        (
            _registered_branch_context(environment_uid=None, environment_name=None),
            CodeRepositoryEnvironmentContextRequiredError,
        ),
    ],
    ids=["unregistered-branch", "branch-without-environment"],
)
def test_overview_reports_an_unresolved_environment_with_the_sdk_error(
    monkeypatch,
    context: CodeRepositoryContext,
    expected_error: type[Exception],
) -> None:
    _use_code_repository_context(monkeypatch, context)
    with pytest.raises(expected_error) as sdk_error:
        BranchEnvironmentGateway().environment_name()

    overview = ControlPlaneService(gateway=BranchEnvironmentGateway()).overview()

    assert type(sdk_error.value) is expected_error
    assert overview.environment is None
    assert overview.status == "failed"
    assert overview.failures == [f"Environment: {sdk_error.value}"]
    assert repr(context.repository_branch) in overview.failures[0]


def test_platform_gateway_reads_current_pricing_details() -> None:
    class StubbedPlatformGateway(PlatformControlPlaneGateway):
        def _time_index_tables(self):
            return {
                definition.table_identifier: SimpleNamespace(cadence="1d")
                for definition in DATA_PRODUCTS
                if definition.time_indexed
            }

        def _registered_valmer_asset_count(self) -> int:
            return 12

        def _current_pricing_details_count(self) -> int:
            return 10

        def _latest_observation(self, definition):
            return pd.DataFrame(
                {"time_index": [pd.Timestamp("2026-08-30T00:00:00Z")]}
            )

    products = StubbedPlatformGateway().data_products()
    pricing_details = next(
        product
        for product in products
        if product["uid"] == "pricing-details"
    )
    vector = next(product for product in products if product["uid"] == "valmer-vector")

    assert pricing_details["status"] == "healthy"
    assert pricing_details["latest_rows"] == 10
    assert pricing_details["error"] is None
    assert "identity_count" not in pricing_details
    assert "identity_count" not in vector


def test_platform_gateway_bulk_loads_all_branch_jobs_and_runs(monkeypatch) -> None:
    first_profile, second_profile = JOB_LAUNCH_PROFILES[:2]
    jobs = [
        SimpleNamespace(
            uid="job-1",
            name="First platform Job",
            description="Description owned by the first platform Job.",
            execution_path=first_profile.execution_path,
            task_schedule=None,
            image_status="ready",
            automatic_deployment=True,
        ),
        SimpleNamespace(
            uid="job-2",
            name="Second platform Job",
            description="Description owned by the second platform Job.",
            execution_path=second_profile.execution_path,
            task_schedule=None,
            image_status="ready",
            automatic_deployment=True,
        ),
        SimpleNamespace(
            uid="job-3",
            name="New branch Job",
            description="Automatically discovered without a launch profile.",
            execution_path="scripts/new_branch_job.py",
            task_schedule=None,
            image_status="ready",
            automatic_deployment=True,
        ),
    ]
    runs = [
        SimpleNamespace(
            uid="run-1",
            job_uid="job-1",
            status="SUCCEEDED",
            execution_start=dt.datetime(2026, 8, 30, tzinfo=dt.UTC),
        )
    ]
    job_queries: list[dict[str, object]] = []
    run_queries: list[dict[str, object]] = []

    def filter_jobs(*, timeout, **kwargs):
        job_queries.append({"timeout": timeout, **kwargs})
        return jobs

    def filter_runs(*, timeout, **kwargs):
        run_queries.append({"timeout": timeout, **kwargs})
        return runs

    monkeypatch.setattr(control_plane_service.Job, "filter", filter_jobs)
    monkeypatch.setattr(control_plane_service.JobRun, "filter", filter_runs)

    result = PlatformControlPlaneGateway().jobs()

    assert [item["uid"] for item in result] == ["job-1", "job-2", "job-3"]
    assert result[0]["description"] == "Description owned by the first platform Job."
    assert result[2]["key"] is None
    assert result[2]["parameter_count"] == 0
    assert job_queries == [{"timeout": 60}]
    assert run_queries == [
        {"timeout": 60, "job__uid__in": ["job-1", "job-2", "job-3"]}
    ]


def test_assets_use_persisted_pricing_details_to_identify_pricing_targets() -> None:
    class StubbedPlatformGateway(PlatformControlPlaneGateway):
        def _vector_snapshot(self) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "unique_identifier": ["target", "non-target"],
                    "time_index": [
                        pd.Timestamp("2026-08-30T00:00:00Z"),
                        pd.Timestamp("2026-08-30T00:00:00Z"),
                    ],
                    "dirty_price": [99.0, 101.0],
                }
            )

        def _asset_details(self, identifiers):
            return {}

        def _pricing_target_identifiers(self) -> set[str]:
            return {"target"}

    assets = StubbedPlatformGateway().assets()

    assert [asset["pricing_target"] for asset in assets] == [True, False]


def test_assets_use_asset_details_for_static_fields_and_vector_time_for_freshness(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        control_plane_service,
        "_utc_now",
        lambda: dt.datetime(2026, 8, 30, 12, tzinfo=dt.UTC),
    )

    class StubbedPlatformGateway(PlatformControlPlaneGateway):
        def _vector_snapshot(self) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "unique_identifier": ["fresh", "stale"],
                    "time_index": [
                        pd.Timestamp("2026-08-30T00:00:00Z"),
                        pd.Timestamp("2026-08-27T00:00:00Z"),
                    ],
                    "issue_currency": ["WRONG", "WRONG"],
                    "sector": ["WRONG", "WRONG"],
                }
            )

        def _asset_details(self, identifiers):
            return {
                identifier: {
                    "valmer_full_name": identifier.title(),
                    "valmer_issue_currency": "MXN",
                    "valmer_sector": "Government",
                }
                for identifier in identifiers
            }

        def _pricing_target_identifiers(self) -> set[str]:
            return set()

    assets = StubbedPlatformGateway().assets()
    assets_by_uid = {asset["uid"]: asset for asset in assets}

    assert [asset["currency"] for asset in assets] == ["MXN", "MXN"]
    assert [asset["sector"] for asset in assets] == ["Government", "Government"]
    assert assets_by_uid["fresh"]["status"] == "healthy"
    assert assets_by_uid["stale"]["status"] == "stale"


def test_registered_valmer_asset_count_uses_valmer_asset_details_relation(
    monkeypatch,
) -> None:
    table = SimpleNamespace(
        uid="details-table",
        identifier=VALMER_ASSET_DETAILS_TABLE_IDENTIFIER,
        physical_schema="public",
        physical_table_name="valmer_connectors__valmerassetdetails",
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        control_plane_service,
        "_optional_registered_meta_table",
        lambda identifier: table if identifier == VALMER_ASSET_DETAILS_TABLE_IDENTIFIER else None,
    )

    def select_rows(selected_table, sql, *, max_rows):
        captured.update(table=selected_table, sql=sql, max_rows=max_rows)
        return [{"asset_count": 6_669}]

    monkeypatch.setattr(control_plane_service, "_select_meta_table_rows", select_rows)

    assert PlatformControlPlaneGateway()._registered_valmer_asset_count() == 6_669
    assert captured["table"] is table
    assert captured["max_rows"] == 1
    assert "valmer_connectors__valmerassetdetails" in str(captured["sql"])


def test_pricing_targets_read_current_pricing_details_through_markets(
    monkeypatch,
) -> None:
    """Valmer's own detail table names the assets; Markets says which have details."""

    from msm_pricing.api.pricing_details import AssetCurrentPricingDetails

    table = SimpleNamespace(
        uid="details-table",
        identifier=VALMER_ASSET_DETAILS_TABLE_IDENTIFIER,
        physical_schema="public",
        physical_table_name="valmer_connectors__valmerassetdetails",
    )
    priced, unpriced = str(uuid.uuid4()), str(uuid.uuid4())
    captured: dict[str, object] = {"batches": []}
    monkeypatch.setattr(
        control_plane_service,
        "_optional_registered_meta_table",
        lambda identifier: table if identifier == VALMER_ASSET_DETAILS_TABLE_IDENTIFIER else None,
    )

    def select_rows(selected_table, sql, *, max_rows):
        captured.update(sql=sql, max_rows=max_rows)
        return [
            {"asset_uid": priced, "asset_identifier": "M_BONOS_1"},
            {"asset_uid": unpriced, "asset_identifier": "M_BONOS_2"},
        ]

    def get_many_by_asset_uid(cls, asset_uids, *, batch_size=1000):
        captured["batches"].append(list(asset_uids))
        return {uuid.UUID(priced): SimpleNamespace(asset_uid=uuid.UUID(priced))}

    monkeypatch.setattr(control_plane_service, "_select_meta_table_rows", select_rows)
    monkeypatch.setattr(
        AssetCurrentPricingDetails,
        "get_many_by_asset_uid",
        classmethod(get_many_by_asset_uid),
    )

    gateway = PlatformControlPlaneGateway()

    assert gateway._pricing_target_identifiers() == {"M_BONOS_1"}
    assert gateway._current_pricing_details_count() == 1
    assert captured["batches"] == [sorted([priced, unpriced])]
    assert captured["max_rows"] == 100_000
    assert "valmer_connectors__valmerassetdetails" in str(captured["sql"])


def test_job_discovery_is_authorized_per_human(api_client) -> None:
    with api_client(VIEWER_UID) as viewer, api_client(OPERATOR_UID) as operator:
        viewer_payload = viewer.get("/api/v1/control-plane/jobs/discovery/").json()
        operator_payload = operator.get("/api/v1/control-plane/jobs/discovery/").json()

    assert viewer_payload["bulk_actions"] == []
    assert operator_payload["bulk_actions"][0]["id"] == "run"
    assert operator_payload["bulk_actions"][0]["selection_modes"] == ["explicit"]


def test_viewer_cannot_execute_job_action(api_client) -> None:
    payload = {"selection": {"mode": "explicit", "uids": ["job-1"]}, "options": {}}

    with api_client(VIEWER_UID) as client:
        preflight = client.post(
            "/api/v1/control-plane/jobs/actions/run/preflight", json=payload
        )
        execution = client.post("/api/v1/control-plane/jobs/actions/run", json=payload)

    assert preflight.status_code == 200
    assert preflight.json()["allowed"] is False
    assert execution.status_code == 403


def test_an_organization_admin_outside_the_operator_list_is_a_viewer(
    hosted_runtime,
) -> None:
    """Operators are the explicit VALMER_CONTROL_PLANE_OPERATOR_UIDS list. The
    admin flag in the caller assertion grants nothing here."""
    payload = {"selection": {"mode": "explicit", "uids": ["job-1"]}, "options": {}}
    gateway = FakeControlPlaneGateway()
    admin_assertion = hosted_runtime.sign(VIEWER_UID, is_organization_admin=True)

    with TestClient(
        create_app(_service(gateway)), headers={ASSERTION_HEADER: admin_assertion}
    ) as client:
        me = client.get("/api/v1/control-plane/me")
        discovery = client.get("/api/v1/control-plane/jobs/discovery/")
        preflight = client.post(
            "/api/v1/control-plane/jobs/actions/run/preflight", json=payload
        )
        execution = client.post("/api/v1/control-plane/jobs/actions/run", json=payload)

    assert me.status_code == 200
    assert me.json() == {"uid": VIEWER_UID, "role": "viewer"}
    assert discovery.json()["bulk_actions"] == []
    assert preflight.json()["allowed"] is False
    assert execution.status_code == 403
    assert gateway.launched_job_uids == []


def test_operator_preflights_and_launches_one_branch_job(api_client) -> None:
    gateway = FakeControlPlaneGateway()
    payload = {"selection": {"mode": "explicit", "uids": ["job-1"]}, "options": {}}

    with api_client(OPERATOR_UID, gateway=gateway) as client:
        preflight = client.post(
            "/api/v1/control-plane/jobs/actions/run/preflight", json=payload
        )
        execution = client.post("/api/v1/control-plane/jobs/actions/run", json=payload)

    assert preflight.status_code == 200
    assert preflight.json()["allowed"] is True
    assert execution.status_code == 202
    assert execution.json()["job_run_uid"] == "run-2"
    assert execution.json()["requested_by_user_uid"] == OPERATOR_UID
    assert gateway.launched_job_uids == ["job-1"]
    assert gateway.launched_command_args == [
        ["--force-pricing-details-patch", "--no-bypass-vector-cursor-filter"]
    ]


def test_unprofiled_branch_job_is_parameterless_and_launchable(api_client) -> None:
    class DynamicJobsGateway(FakeControlPlaneGateway):
        def jobs(self) -> list[dict[str, object]]:
            return [
                *super().jobs(),
                {
                    "uid": "job-dynamic",
                    "key": None,
                    "name": "New branch Job",
                    "description": "Discovered from the platform Job.",
                    "status": "not-run",
                    "last_run_status": None,
                    "last_run_at": None,
                    "execution_path": "scripts/new_branch_job.py",
                    "schedule": None,
                    "image_status": "ready",
                    "automatic_deployment": True,
                    "dependencies": [],
                    "parameter_count": 0,
                },
            ]

    gateway = DynamicJobsGateway()
    payload = {
        "selection": {"mode": "explicit", "uids": ["job-dynamic"]},
        "options": {},
    }

    with api_client(OPERATOR_UID, gateway=gateway) as client:
        parameters = client.get(
            "/api/v1/control-plane/jobs/job-dynamic/run-parameters"
        )
        execution = client.post(
            "/api/v1/control-plane/jobs/actions/run",
            json=payload,
        )

    assert parameters.status_code == 200
    assert parameters.json()["job_key"] == "job-dynamic"
    assert parameters.json()["parameters"] == []
    assert execution.status_code == 202
    assert gateway.launched_job_uids == ["job-dynamic"]
    assert gateway.launched_command_args == [[]]


def test_deployment_migration_job_is_listed_but_never_launched_by_hand(api_client) -> None:
    class MigrationJobGateway(FakeControlPlaneGateway):
        def jobs(self) -> list[dict[str, object]]:
            return [
                *super().jobs(),
                {
                    "uid": "job-migrate",
                    "key": None,
                    "name": "Valmer migrations",
                    "description": "Apply the Valmer MetaTables migrations.",
                    "status": "succeeded",
                    "last_run_status": "SUCCEEDED",
                    "last_run_at": None,
                    "execution_path": "scripts/migrate_valmer.py",
                    "schedule": None,
                    "image_status": "ready",
                    "automatic_deployment": True,
                    "dependencies": [],
                    "parameter_count": 0,
                },
            ]

    gateway = MigrationJobGateway()
    payload = {
        "selection": {"mode": "explicit", "uids": ["job-migrate"]},
        "options": {},
    }

    with api_client(OPERATOR_UID, gateway=gateway) as client:
        listing = client.get("/api/v1/control-plane/jobs")
        preflight = client.post(
            "/api/v1/control-plane/jobs/actions/run/preflight", json=payload
        )
        execution = client.post("/api/v1/control-plane/jobs/actions/run", json=payload)

    assert listing.status_code == 200
    assert "job-migrate" in str(listing.json())
    assert preflight.status_code == 200
    assert preflight.json()["allowed"] is False
    assert any(
        "only from the deployment workflow" in blocker
        for blocker in preflight.json()["blockers"]
    )
    assert execution.status_code == 409
    assert gateway.launched_job_uids == []


def test_job_run_parameters_expose_runtime_controls_not_metatable_selection(api_client) -> None:
    with api_client(OPERATOR_UID) as client:
        response = client.get("/api/v1/control-plane/jobs/job-3/run-parameters")

    assert response.status_code == 200
    payload = response.json()
    assert payload["job_key"] == "vector-metatable-refresh"
    assert {parameter["key"] for parameter in payload["parameters"]} == {
        "force_pricing_details_patch",
        "bypass_vector_cursor_filter",
    }
    assert "source_metatable_uid" not in str(payload)
    assert "fixed by the repository configuration" in payload["configuration_summary"]


def test_metatable_job_preflight_validates_the_repository_configured_source(api_client) -> None:
    gateway = FakeControlPlaneGateway()
    payload = {"selection": {"mode": "explicit", "uids": ["job-3"]}, "options": {}}

    with api_client(OPERATOR_UID, gateway=gateway) as client:
        response = client.post(
            "/api/v1/control-plane/jobs/actions/run/preflight", json=payload
        )

    assert response.status_code == 200
    assert response.json()["allowed"] is True
    assert gateway.validated_metatable_sources == 1


def test_job_launch_forwards_only_catalogued_command_arguments(api_client) -> None:
    gateway = FakeControlPlaneGateway()
    payload = {
        "selection": {"mode": "explicit", "uids": ["job-1"]},
        "options": {
            "force_pricing_details_patch": False,
            "bypass_vector_cursor_filter": True,
        },
    }

    with api_client(OPERATOR_UID, gateway=gateway) as client:
        response = client.post("/api/v1/control-plane/jobs/actions/run", json=payload)

    assert response.status_code == 202
    assert gateway.launched_command_args == [
        ["--no-force-pricing-details-patch", "--bypass-vector-cursor-filter"]
    ]


def test_pipeline_reconciles_declared_actions_with_registered_jobs() -> None:
    class PartialJobsGateway(FakeControlPlaneGateway):
        def jobs(self) -> list[dict[str, object]]:
            return super().jobs()[:1]

    pipeline = ControlPlaneService(gateway=PartialJobsGateway()).pipeline()
    actions = [action for stage in pipeline.stages for action in stage.actions]
    vector = next(action for action in actions if action.key == "vector-refresh")
    missing = next(
        action for action in actions if action.key == "irs-mxn-quotes-refresh"
    )

    assert vector.available is True
    assert vector.job_uid == "job-1"
    assert vector.name == "Job vector-refresh"
    assert vector.description == "Purpose of vector-refresh."
    assert vector.status == "succeeded"
    assert missing.available is False
    assert missing.job_uid is None
    assert missing.name is None
    assert missing.description is None
    assert missing.status == "missing"
    assert {action.key for action in actions} == {
        profile.key for profile in JOB_LAUNCH_PROFILES
    }


def test_launch_accepts_platform_response_without_run_status() -> None:
    class IncompleteLaunchGateway(FakeControlPlaneGateway):
        def run_job(self, job_uid: str, command_args: list[str]):
            return SimpleNamespace(name="Valmer Vector Refresh"), {"uid": "run-2"}

    service = ControlPlaneService(
        gateway=IncompleteLaunchGateway(),
        operator_uids={"operator-user"},
    )
    request = control_plane_service.BulkActionExecution.model_validate(
        {"selection": {"mode": "explicit", "uids": ["job-1"]}, "options": {}}
    )

    result = service.launch("operator-user", request)

    assert result.job_run_uid == "run-2"
    assert result.status == "ACCEPTED"


def test_operator_cannot_launch_an_already_running_job(api_client) -> None:
    gateway = FakeControlPlaneGateway()
    gateway.job_status = "running"
    payload = {"selection": {"mode": "explicit", "uids": ["job-1"]}, "options": {}}

    with api_client(OPERATOR_UID, gateway=gateway) as client:
        response = client.post("/api/v1/control-plane/jobs/actions/run", json=payload)

    assert response.status_code == 409
    assert gateway.launched_job_uids == []
