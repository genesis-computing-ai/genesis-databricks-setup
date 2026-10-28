# Databricks notebook source
# Copyright (c) 2026 Genesis Computing, Inc.
"""Install, check and uninstall Genesis as a Databricks App — run by a workspace admin.

The package ships this file at its root in Databricks notebook source format: import it
into the workspace as a notebook and run it with the widgets at the top. It uses the
Databricks SDK as the signed-in admin; the App itself never needs these permissions.

Ownership: preinstall gives each install a random **install id**, keeps it as the value of
the ``genesis-owner`` key in the App's secret scope, and writes it into the mark on
everything it creates (schema and volume comment, Lakebase display name). Unity Catalog
is shared by every workspace on the metastore, so a name alone proves nothing; this
notebook only ever adopts or deletes what carries its own install id.

- **preinstall** — before installing the App: creates the UC schema
  ``<catalog>.<app_name>`` with volume ``genesis``, the Genesis lakehouse schema
  ``<catalog>.<app_name>_lakehouse`` (Iceberg tables Genesis writes, e.g. flow outputs),
  the Lakebase Autoscaling project
  ``<app_name>`` and the secret scope ``<app_name>`` with the install id and a generated
  ``master-key``. Stops first, creating nothing, when Lakebase Postgres is off in the
  workspace (a preview: workspace avatar -> Previews). Idempotent; an existing master key
  is never replaced (a new key makes every secret Genesis stored unreadable). A resource with the same name that carries
  another install id, or none, is never adopted: pick another ``app_name``. Ends by printing
  what to enter in the install form, field by field: the App name, each resource it
  created, the newest Claude Sonnet and the default embedding endpoint when they are ready,
  and a SQL warehouse (serverless and running first).
- **postinstall** — after installing: checks that the App binds every resource
  ``app.yaml`` needs with the least privilege it needs, runs one instance, and has the
  user-authorization scopes Genesis needs (sign-in, and ``sql`` for agent SQL as the user)
  and no other; then grants the App's service principal what its lakehouse needs on the
  lakehouse schema only (:data:`LAKEHOUSE_PRIVILEGES`; the App can exist only after
  preinstall, so this is the first moment its principal can be named); prints its state
  and URL, and schedules the watchdog (below) when run from the imported notebook.
- **grant_data** — after postinstall: lets Genesis read the customer's existing data with
  no person signed in (scheduled jobs, missions, scans) through its
  ``genesis_databricks_app`` connection. That SQL never runs as the App's own service
  principal, which can read the App's master key: the first run creates a **data reader**,
  the service principal ``<app_name>-data-reader`` with no access to the App's secret scope,
  keeps its OAuth secret in the App's scope (``data-reader``) for the App to sign in as it
  with, and lets it use the App's SQL warehouse. ``data`` lists catalogs or
  ``catalog.schema`` names; the reader gets read only (:data:`DATA_CATALOG_PRIVILEGES` on
  each catalog, :data:`DATA_READ_PRIVILEGES` on each schema, or on the whole catalog when
  only a catalog is named). Without ``confirm`` set to the App name it only prints what it
  would do; nothing is granted silently. Every Genesis user can then read that data,
  whatever their own Unity Catalog permissions. **revoke_data** takes back what the reader
  holds: a catalog named on its own loses it on the catalog and every schema in it, a
  schema only its own. **rotate_data_reader**
  gives the reader a new OAuth secret (each lasts two years) and deletes the old ones.
- **install_watchdog** — what postinstall does last, on its own: schedules a job (``<app_name>-watchdog``, every five minutes, tagged
  with the install id) that runs this notebook with ``action=watchdog``. Databricks never
  restarts a crashed App; the job does. Idempotent.
- **watchdog** — when this install's App is ``CRASHED``, deploys its active source again
  (the same tag or path). An App that is running, deploying or stopped on purpose is left
  alone.
- **uninstall** — needs ``confirm`` set to the App name, and deletes the App only if it
  binds this install's master key. To delete the data too, ``delete_data`` must read
  ``delete <app_name> data``: then the Lakebase project (threads, metadata,
  configuration), the volume, the schema, the lakehouse schema with its tables, the data
  reader and the secret scope go — only after checking all
  of them carry this install's id and hold nothing else (no other UC object, counting ones
  the admin cannot read; no other Lakebase branch or database), so a refusal leaves
  everything in place. Only the Lakebase project can be recovered, within Lakebase's
  soft-delete window, and its name stays reserved until then — but the master key goes
  with the scope, so a recovered project's stored secrets stay unreadable.

Cost: a handful of workspace API calls; creating the Lakebase project takes a minute to
finish in the background. The project scales to zero after five idle minutes.
"""

from __future__ import annotations

import base64
import json
import re
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, DatabricksError, NotFound, PermissionDenied
from databricks.sdk.service.catalog import VolumeType


VOLUME_NAME = "genesis"
LAKEHOUSE_SUFFIX = "_lakehouse"
TELEMETRY_SUFFIX = "_telemetry"
"""App telemetry (its logs, metrics and traces) goes to ``<app_name>_telemetry``: kept apart
from the App's state schema, whose uninstall check allows only the volume, and from the
lakehouse, which every Genesis user can browse."""
TELEMETRY_TABLES = {
    "logs_table": "otel_logs",
    "metrics_table": "otel_metrics",
    "traces_table": "otel_spans",
}
"""The Genesis lakehouse schema is ``<app_name>_lakehouse``; Genesis derives the same name."""
LAKEHOUSE_PRIVILEGES = ("USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY", "EXTERNAL_USE_SCHEMA")
"""What the App needs on its lakehouse schema. ``EXTERNAL_USE_SCHEMA`` lets Unity Catalog's
Iceberg REST endpoint vend storage credentials; ``ALL_PRIVILEGES`` does not include it.
``USE_CATALOG`` comes with the App's volume binding."""
DATA_READER_SUFFIX = "-data-reader"
"""The data reader is the service principal ``<app_name>-data-reader``."""
DATA_READER_KEY = "data-reader"
"""Key in the App's secret scope holding the reader's ``{"client_id", "client_secret"}``."""
DATA_READER_SECRET_LIFETIME = f"{730 * 24 * 3600}s"
"""The longest an OAuth secret may live (two years); rotate_data_reader issues a new one."""
SCIM_SERVICE_PRINCIPALS = "/api/2.0/preview/scim/v2/ServicePrincipals"
SCIM_SERVICE_PRINCIPAL_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"
SERVICE_PRINCIPAL_SECRETS = "/api/2.0/accounts/servicePrincipals"
NEW_PRINCIPAL_WAIT_SECONDS = 60
NEW_PRINCIPAL_RETRY_SECONDS = 5
"""The workspace's proxy for a service principal's OAuth secrets (the SCIM id is next)."""
DATA_CATALOG_PRIVILEGES = ("USE_CATALOG", "BROWSE")
"""What grant_data gives on each catalog it names: reaching it, and seeing what is in it
(``BROWSE`` shows names and metadata only, never data)."""
DATA_READ_PRIVILEGES = ("USE_SCHEMA", "SELECT")
"""What grant_data gives on each schema it names, or on a whole catalog: read, never write."""
DATA_GRANTS_REACH = (
    "Every Genesis user can then read this data through the genesis_databricks_app "
    "connection, whatever their own Unity Catalog permissions."
)
_DATA_NAME_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,254}")
# A SQL warehouse id: letters and digits only, so it can never reshape the permissions path.
_WAREHOUSE_ID_RE = re.compile(r"[A-Za-z0-9]+")
LAKEBASE_BRANCH = "production"
LAKEBASE_DATABASE = "databricks-postgres"
# A Databricks-hosted Claude Sonnet chat endpoint and its version, e.g. ``-5-5``.
_CLAUDE_SONNET_ENDPOINT = re.compile(r"databricks-claude-sonnet-(\d+(?:-\d+)*)")
DEFAULT_EMBEDDING_ENDPOINT = "databricks-gte-large-en"
"""The embedding endpoint the bundle binds by default; preinstall names it when it is ready."""
SQL_WAREHOUSE_USE = (
    "Can use: Genesis runs Unity Catalog queries on it as each signed-in user, so give the "
    "users who will sign in Can use on it too"
)
MASTER_KEY = "master-key"
PREVIOUS_MASTER_KEY = "master-key-previous"
"""Holds the key in force before a rotation, while what it sealed is still read."""
MASTER_KEY_BYTES = 48
OWNER_KEY = "genesis-owner"
"""Key in the App's secret scope holding the install id."""
INSTALL_ID_BYTES = 8
REQUIRED_USER_SCOPE = "iam.current-user:read"
SQL_USER_SCOPE = "sql"
ALLOWED_USER_SCOPES = frozenset({REQUIRED_USER_SCOPE, "iam.access-control:read", SQL_USER_SCOPE})
"""The defaults every App gets, plus ``sql``: agent SQL runs as the signed-in user."""
_MISSING_HINTS = {
    "sql-warehouse": (
        ": bind a SQL warehouse your users can use (Can use) under that key, then redeploy; "
        "Genesis runs Unity Catalog queries on it as each signed-in user"
    ),
}
"""What to do about a missing binding, where the key alone does not say."""
APP_NAME_RE = re.compile(r"[a-z0-9][a-z0-9-]{1,28}[a-z0-9]")
CATALOG_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,254}")
ACTIONS = (
    "preinstall",
    "postinstall",
    "grant_data",
    "revoke_data",
    "rotate_data_reader",
    "install_watchdog",
    "watchdog",
    "uninstall",
)
WATCHDOG_SCHEDULE = "0 0/5 * * * ?"
"""Every five minutes: Databricks never restarts a crashed App, so this bounds the outage."""
WATCHDOG_TAG = "genesis_install"
CRASHED = "CRASHED"
CRASH_LOOP_WINDOW = timedelta(minutes=30)
"""A deployment that crashes this soon is not redeployed: the launcher already stops after
three crashes in ten minutes so a broken release does not loop unseen, and the watchdog
must not undo that. Its run fails instead, which is what the job's failure alerts see."""

Report = Callable[[str], None]


class LifecycleError(RuntimeError):
    """A lifecycle step cannot proceed; the message says what to fix."""


@dataclass(frozen=True)
class Install:
    """One Genesis App and every resource named after it."""

    app_name: str
    catalog: str

    @property
    def schema(self) -> str:
        return f"{self.catalog}.{self.app_name}"

    @property
    def volume(self) -> str:
        return f"{self.schema}.{VOLUME_NAME}"

    @property
    def lakehouse_schema_name(self) -> str:
        return f"{self.app_name}{LAKEHOUSE_SUFFIX}"

    @property
    def lakehouse_schema(self) -> str:
        return f"{self.catalog}.{self.lakehouse_schema_name}"

    @property
    def data_reader(self) -> str:
        return f"{self.app_name}{DATA_READER_SUFFIX}"

    @property
    def watchdog_job(self) -> str:
        return f"{self.app_name}-watchdog"

    @property
    def telemetry_schema_name(self) -> str:
        return f"{self.app_name}{TELEMETRY_SUFFIX}"

    @property
    def telemetry_schema(self) -> str:
        return f"{self.catalog}.{self.telemetry_schema_name}"

    def telemetry_destination(self) -> dict[str, Any]:
        """Where the App's telemetry should be exported (``telemetry_export_destinations``)."""
        return {
            "unity_catalog": {
                field: f"{self.telemetry_schema}.{table}"
                for field, table in TELEMETRY_TABLES.items()
            }
        }

    def table_schemas(self) -> tuple[tuple[str, str, str], ...]:
        """(name, full name, what it holds) of each schema that holds only tables."""
        return (
            (self.lakehouse_schema_name, self.lakehouse_schema, "lakehouse"),
            (self.telemetry_schema_name, self.telemetry_schema, "telemetry"),
        )

    @property
    def project(self) -> str:
        return f"projects/{self.app_name}"

    @property
    def branch(self) -> str:
        return f"{self.project}/branches/{LAKEBASE_BRANCH}"

    @property
    def data_confirmation(self) -> str:
        """What ``delete_data`` must read to delete the App's data."""
        return f"delete {self.app_name} data"

    def mark(self, install_id: str) -> str:
        """Written on what preinstall creates; uninstall deletes only what carries it."""
        return f"Genesis App {self.app_name} [{install_id}] (genesis_lifecycle)"

    def validate(self) -> None:
        """Refuse names the workspace would reject or read as something else.

        Raises:
            LifecycleError: the App name or catalog is unusable.
        """
        if not APP_NAME_RE.fullmatch(self.app_name):
            raise LifecycleError(
                f"app_name {self.app_name!r} must be 3-30 lowercase letters, digits or hyphens"
            )
        if not CATALOG_RE.fullmatch(self.catalog):
            raise LifecycleError(f"catalog {self.catalog!r} is not a catalog name")

    def optional_bindings(self) -> dict[str, tuple[str, dict[str, Any]]]:
        """Resource key → (binding type, binding) the App may have; checked only if bound.

        ``genesis-master-key-previous``: during a master-key rotation, the old key, kept
        in this install's scope so what it sealed is still read.
        """
        return {
            "genesis-master-key-previous": (
                "secret",
                {"scope": self.app_name, "key": PREVIOUS_MASTER_KEY, "permission": "READ"},
            ),
        }

    def expected_bindings(self) -> dict[str, tuple[str, dict[str, Any]]]:
        """Resource key → (binding type, binding) the App must have, as ``app.yaml`` reads them.

        ``sql-warehouse``: the SQL warehouse the seeded ``genesis_databricks`` connection
        runs on, as each signed-in user. Required: without it the App starts with no
        Databricks connection, and each user would have to make one by hand.
        """
        return {
            "postgres": (
                "postgres",
                {
                    "branch": self.branch,
                    "database": f"{self.branch}/databases/{LAKEBASE_DATABASE}",
                    "permission": "CAN_CONNECT_AND_CREATE",
                },
            ),
            "volume": (
                "uc_securable",
                {
                    "securable_full_name": self.volume,
                    "securable_type": "VOLUME",
                    "permission": "WRITE_VOLUME",
                },
            ),
            "genesis-master-key": (
                "secret",
                {"scope": self.app_name, "key": MASTER_KEY, "permission": "READ"},
            ),
            "chat-endpoint": ("serving_endpoint", {"permission": "CAN_QUERY"}),
            "embedding-endpoint": ("serving_endpoint", {"permission": "CAN_QUERY"}),
            "sql-warehouse": ("sql_warehouse", {"permission": "CAN_USE"}),
        }


def _api(client: Any, method: str, path: str, **kwargs: Any) -> dict:
    """One REST call through the SDK's authenticated client (Lakebase Autoscaling has no SDK
    service in every runtime).
    """
    return client.api_client.do(method, path, **kwargs) or {}


def _get(call: Any, *args: Any) -> Any:
    """The resource, or ``None`` when it does not exist; any other error is raised."""
    try:
        return call(*args)
    except NotFound:
        return None


def _project(client: Any, install: Install) -> dict | None:
    return _get(_api, client, "GET", f"/api/2.0/postgres/{install.project}")


def _display_name(project: dict) -> str | None:
    # A project still being created may report its spec before its status.
    status = (project.get("status") or {}).get("display_name")
    return status or (project.get("spec") or {}).get("display_name")


def _scope_keys(client: Any, install: Install) -> set[str] | None:
    if install.app_name not in {scope.name for scope in client.secrets.list_scopes()}:
        return None
    return {entry.key for entry in client.secrets.list_secrets(scope=install.app_name)}


def recorded_install_id(client: Any, install: Install) -> str | None:
    """The install id kept in the App's secret scope, or ``None`` when there is none."""
    keys = _scope_keys(client, install)
    if not keys or OWNER_KEY not in keys:
        return None
    value = client.secrets.get_secret(scope=install.app_name, key=OWNER_KEY).value
    return base64.b64decode(value).decode() if value else None


def ownership_problems(client: Any, install: Install, install_id: str | None) -> list[str]:
    """Existing resources named after the App that are not this install's.

    With ``install_id`` ``None`` (a new install) every existing resource is one.
    Cost: up to seven workspace API calls.
    """
    mark = install.mark(install_id) if install_id else None
    problems = []
    for schema_name in (install.schema, install.lakehouse_schema, install.telemetry_schema):
        schema = _get(client.schemas.get, schema_name)
        if schema is not None and (mark is None or schema.comment != mark):
            problems.append(f"schema {schema_name} exists and is not this install's")
    volume = _get(client.volumes.read, install.volume)
    if volume is not None and (mark is None or volume.comment != mark):
        problems.append(f"volume {install.volume} exists and is not this install's")
    project = _project(client, install)
    if project is not None and (mark is None or _display_name(project) != mark):
        problems.append(f"Lakebase {install.project} exists and is not this install's")
    keys = _scope_keys(client, install)
    if keys is not None and install_id is None:
        problems.append(
            f"secret scope {install.app_name} exists without this notebook's install id "
            "(if a previous preinstall stopped right after creating it, delete the empty "
            "scope and run preinstall again)"
        )
    elif keys is not None and keys - {MASTER_KEY, OWNER_KEY, PREVIOUS_MASTER_KEY, DATA_READER_KEY}:
        problems.append(
            f"secret scope {install.app_name} holds other keys ({', '.join(sorted(keys))})"
        )
    return problems


def require_lakebase(client: Any) -> None:
    """Refuse when the workspace has Lakebase Postgres off, before anything is created.

    Lakebase is a preview some workspaces have off; without this check preinstall would
    stop at the project, after creating the schemas, volume and secret scope.
    Cost: one workspace API call.

    Raises:
        LifecycleError: the workspace answered that Lakebase is not enabled.
    """
    try:
        _api(client, "GET", "/api/2.0/postgres/projects", query={"page_size": 1})
    except BadRequest as exc:
        raise LifecycleError(
            f"Lakebase Postgres is not available in this workspace ({exc!s}). A workspace "
            "admin turns it on under the workspace avatar -> Previews -> Lakebase Postgres; "
            "it takes a few minutes to take effect. Then run preinstall again."
        ) from exc


def ready_serving_endpoints(client: Any) -> set[str]:
    """Names of the serving endpoints ready in the workspace. One call.

    Suggestions for the endpoint bindings: which models a workspace serves varies by cloud
    and region. A workspace that will not list its endpoints gets no suggestion.
    """
    try:
        endpoints = _api(client, "GET", "/api/2.0/serving-endpoints").get("endpoints") or []
    except DatabricksError:
        return set()
    return {
        str(endpoint.get("name") or "")
        for endpoint in endpoints
        if (endpoint.get("state") or {}).get("ready") == "READY"
    }


def newest_claude_sonnet(names: set[str]) -> str | None:
    """The newest Claude Sonnet chat endpoint among ``names``, or None. PURE."""
    versions = {
        name: tuple(int(part) for part in match.group(1).split("-"))
        for name in names
        if (match := _CLAUDE_SONNET_ENDPOINT.fullmatch(name))
    }
    return max(versions, key=versions.__getitem__) if versions else None


def suggested_sql_warehouse(client: Any) -> str | None:
    """A SQL warehouse to bind, as ``<name> (id <id>)``, or None. One call.

    Serverless before classic, running before stopped: users' first query on it should not
    wait for a cluster to start. A workspace that will not list its warehouses, or shows
    the admin none, gets no suggestion.
    """
    try:
        warehouses = _api(client, "GET", "/api/2.0/sql/warehouses").get("warehouses") or []
    except DatabricksError:
        return None
    if not warehouses:
        return None
    best = min(
        warehouses,
        key=lambda warehouse: (
            not warehouse.get("enable_serverless_compute"),
            warehouse.get("state") != "RUNNING",
        ),
    )
    return f"{best.get('name')} (id {best.get('id')})"


def preinstall(client: Any, install: Install) -> list[str]:
    """Create the App's schema, volume, Lakebase project, secret scope and master key.

    Cost: up to seventeen workspace API calls; the Lakebase project finishes creating in the
    background within a minute. Returns what to enter in the install form, field by field.

    Raises:
        LifecycleError: Lakebase is off in the workspace, or a resource with the App's name
            exists that is not this install's.
    """
    install.validate()
    require_lakebase(client)
    install_id = recorded_install_id(client, install)
    if problems := ownership_problems(client, install, install_id):
        raise LifecycleError("; ".join(problems) + " — choose another app_name")
    if install_id is None:
        install_id = secrets.token_hex(INSTALL_ID_BYTES)
        client.secrets.create_scope(scope=install.app_name)
        client.secrets.put_secret(scope=install.app_name, key=OWNER_KEY, string_value=install_id)
    mark = install.mark(install_id)
    for schema_name, full_name in (
        (install.app_name, install.schema),
        (install.lakehouse_schema_name, install.lakehouse_schema),
        (install.telemetry_schema_name, install.telemetry_schema),
    ):
        if _get(client.schemas.get, full_name) is None:
            client.schemas.create(name=schema_name, catalog_name=install.catalog, comment=mark)
    if _get(client.volumes.read, install.volume) is None:
        client.volumes.create(
            catalog_name=install.catalog,
            schema_name=install.app_name,
            name=VOLUME_NAME,
            volume_type=VolumeType.MANAGED,
            comment=mark,
        )
    if _project(client, install) is None:
        _api(
            client,
            "POST",
            "/api/2.0/postgres/projects",
            query={"project_id": install.app_name},
            body={
                "spec": {
                    "display_name": mark,
                    "pg_version": 17,
                    "default_endpoint_settings": {
                        "autoscaling_limit_min_cu": 0.5,
                        "autoscaling_limit_max_cu": 2,
                        "suspend_timeout_duration": "300s",
                    },
                }
            },
        )
    generated_key = MASTER_KEY not in (_scope_keys(client, install) or set())
    if generated_key:
        client.secrets.put_secret(
            scope=install.app_name,
            key=MASTER_KEY,
            string_value=secrets.token_urlsafe(MASTER_KEY_BYTES),
        )
    ready = ready_serving_endpoints(client)
    sonnet = newest_claude_sonnet(ready)
    warehouse = suggested_sql_warehouse(client)
    mapping = [
        "Enter these in the install form:",
        f"App name            -> {install.app_name} (postinstall and uninstall find the App by "
        "this name)",
        f"postgres            -> Lakebase {install.branch}, database {LAKEBASE_DATABASE}, "
        "Can connect and create",
        f"volume              -> {install.volume}, Can read and write",
        f"genesis-master-key  -> secret {install.app_name}/{MASTER_KEY}, Can read",
        f"chat-endpoint       -> {sonnet} (the newest Claude Sonnet ready here), Can query"
        if sonnet
        else "chat-endpoint       -> a chat model serving endpoint, Can query",
        f"embedding-endpoint  -> {DEFAULT_EMBEDDING_ENDPOINT}, Can query"
        if DEFAULT_EMBEDDING_ENDPOINT in ready
        else "embedding-endpoint  -> an embedding serving endpoint, Can query",
        f"sql-warehouse       -> {warehouse}, or another SQL warehouse your users can use, "
        f"{SQL_WAREHOUSE_USE}"
        if warehouse
        else f"sql-warehouse       -> a SQL warehouse your users can use, {SQL_WAREHOUSE_USE}",
    ]
    if generated_key:
        mapping.append(
            f"keep a copy of the master key (secret {install.app_name}/{MASTER_KEY}) somewhere "
            "safe: losing it makes every secret Genesis stores unreadable"
        )
    return mapping


def binding_problems(app: dict, install: Install) -> list[str]:
    """What is wrong with an installed App's bindings and settings. PURE.

    Missing or differently-bound resources, broader permissions than needed, more than one
    instance, and user-authorization scopes other than the ones Genesis needs.
    """
    bound = {resource.get("name"): resource for resource in app.get("resources") or []}
    problems: list[str] = []
    optional = {key: spec for key, spec in install.optional_bindings().items() if key in bound}
    for key, (kind, wanted) in {**install.expected_bindings(), **optional}.items():
        binding = (bound.get(key) or {}).get(kind)
        if binding is None:
            problems.append(f"resource {key!r} is not bound as {kind}{_MISSING_HINTS.get(key, '')}")
            continue
        problems.extend(
            f"resource {key!r}: {field} is {binding.get(field)!r}, expected {value!r}"
            for field, value in wanted.items()
            if binding.get(field) != value
        )
    # Unset is the platform's default: one instance.
    problems.extend(
        f"{field} must be 1: Genesis stores assume one writer"
        for field in ("compute_min_instances", "compute_max_instances")
        if app.get(field) not in (None, 1)
    )
    scopes = set(app.get("effective_user_api_scopes") or [])
    if REQUIRED_USER_SCOPE not in scopes:
        problems.append(
            f"user authorization scope {REQUIRED_USER_SCOPE} is missing: sign-in cannot verify "
            "users; enable user authorization for Apps in the workspace"
        )
    if SQL_USER_SCOPE not in scopes:
        problems.append(
            f"user authorization scope {SQL_USER_SCOPE} is missing: agent SQL on Databricks "
            f"connections cannot run as the signed-in user; add the {SQL_USER_SCOPE} scope to the App"
        )
    if extra := sorted(scopes - ALLOWED_USER_SCOPES):
        problems.append(f"user authorization scopes Genesis does not need: {', '.join(extra)}")
    return problems


def postinstall(client: Any, install: Install, notebook_path: str = "") -> list[str]:
    """Check the installed App, grant it its lakehouse, schedule its watchdog; report each.

    The watchdog (:func:`install_watchdog`) runs this notebook, so it is scheduled only when
    ``notebook_path`` names it; without one the report says how to schedule it.

    Cost: three API calls (the App, the lakehouse schema, one grant), and with
    ``notebook_path`` those of :func:`install_watchdog`.

    Raises:
        LifecycleError: listing every binding or setting to fix, or the lakehouse schema
            is missing (run preinstall again: it creates only what is missing).
    """
    install.validate()
    app = _api(client, "GET", f"/api/2.0/apps/{install.app_name}")
    problems = binding_problems(app, install)
    principal = app.get("service_principal_client_id")
    if not principal:
        problems.append("the App has no service principal yet: wait until it is created")
    if _get(client.schemas.get, install.lakehouse_schema) is None:
        problems.append(
            f"lakehouse schema {install.lakehouse_schema} is missing: run preinstall again"
        )
    if problems:
        raise LifecycleError("; ".join(problems))
    _api(
        client,
        "PATCH",
        f"/api/2.1/unity-catalog/permissions/schema/{install.lakehouse_schema}",
        body={"changes": [{"principal": principal, "add": list(LAKEHOUSE_PRIVILEGES)}]},
    )
    state = (app.get("app_status") or {}).get("state", "UNKNOWN")
    report = (
        f"{install.app_name}: {state} at {app.get('url')}; its lakehouse is "
        f"{install.lakehouse_schema} ({', '.join(LAKEHOUSE_PRIVILEGES)})"
    )
    if app.get("telemetry_export_destinations") == [install.telemetry_destination()]:
        # Configuration only: Databricks applies a telemetry change at the App's next deploy.
        report += (
            f"; App telemetry is set to {install.telemetry_schema} (it starts at the App's "
            "next deploy after a change: check that otel_logs fills)"
        )
    else:
        # Not a refusal: the App runs without it, but its logs then live only in the short
        # buffer the Logs tab shows, and are gone after a restart.
        report += (
            f"; App telemetry is not exported to {install.telemetry_schema}: in the App's "
            "settings, turn on telemetry with logs, metrics and traces tables "
            f"{', '.join(TELEMETRY_TABLES.values())} in that schema, so its logs outlive restarts"
        )
    if not notebook_path:
        return [
            report,
            "no watchdog scheduled: run postinstall from the imported notebook, or "
            "install_watchdog there, so a crashed App is restarted",
        ]
    return [report, install_watchdog(client, install, notebook_path)]


@dataclass(frozen=True)
class DataGrant:
    """The privileges grant_data gives on one securable, or revoke_data takes back."""

    kind: str
    """``catalog`` or ``schema``."""
    full_name: str
    privileges: tuple[str, ...]

    def statement(self, verb: str, principal: str) -> str:
        """The SQL an admin would run for this grant (``GRANT``) or revoke (``REVOKE``). PURE."""
        names = ".".join(f"`{part}`" for part in self.full_name.split("."))
        privileges = ", ".join(privilege.replace("_", " ") for privilege in self.privileges)
        direction = "TO" if verb == "GRANT" else "FROM"
        return f"{verb} {privileges} ON {self.kind.upper()} {names} {direction} `{principal}`;"


def _data_names(data: str) -> list[str]:
    """The catalogs and ``catalog.schema`` names ``data`` lists. PURE.

    Raises:
        LifecycleError: ``data`` names nothing, or a name that is not a plain catalog or
            ``catalog.schema``.
    """
    names = [name.strip() for name in data.split(",") if name.strip()]
    if not names:
        raise LifecycleError("data must name at least one catalog or catalog.schema")
    bad = [
        name
        for name in names
        if name.count(".") > 1 or not all(_DATA_NAME_RE.fullmatch(part) for part in name.split("."))
    ]
    if bad:
        raise LifecycleError(
            f"data must list catalogs or catalog.schema names, comma-separated: "
            f"{', '.join(bad)} is not one"
        )
    return names


def data_grants(data: str) -> list[DataGrant]:
    """What grant_data gives for ``data``: catalogs and ``catalog.schema`` names. PURE.

    Each catalog once, before the schemas in it; a catalog named on its own gets read on
    every schema in it.

    Raises:
        LifecycleError: see :func:`_data_names`.
    """
    names = _data_names(data)
    whole_catalogs = {name for name in names if "." not in name}
    grants: dict[str, DataGrant] = {}
    for name in names:
        catalog = name.split(".", 1)[0]
        catalog_privileges = DATA_CATALOG_PRIVILEGES + (
            DATA_READ_PRIVILEGES if catalog in whole_catalogs else ()
        )
        grants.setdefault(catalog, DataGrant("catalog", catalog, catalog_privileges))
        if "." in name and catalog not in whole_catalogs:
            grants.setdefault(name, DataGrant("schema", name, DATA_READ_PRIVILEGES))
    return list(grants.values())


@dataclass(frozen=True)
class DataReader:
    """The data reader service principal of one install."""

    scim_id: str
    application_id: str


def data_reader(client: Any, install: Install) -> DataReader | None:
    """The install's data reader, or ``None`` when it has none yet.

    The reader is this install's only when the client id kept in the App's secret scope
    is its application id. Cost: one SCIM lookup, plus one secret read when the key
    exists.

    Raises:
        LifecycleError: a service principal of the reader's name exists that is not this
            install's.
    """
    found = (
        _api(
            client,
            "GET",
            SCIM_SERVICE_PRINCIPALS,
            query={"filter": f'displayName eq "{install.data_reader}"'},
        ).get("Resources")
        or []
    )
    if not found:
        return None
    stored = _stored_reader(client, install).get("client_id")
    ours = [entry for entry in found if entry.get("applicationId") == stored]
    if not ours:
        raise LifecycleError(
            f"service principal {install.data_reader} exists and is not this install's (its "
            f"client id is not the one in secret scope {install.app_name}): delete it or "
            "choose another app_name"
        )
    return DataReader(ours[0]["id"], ours[0]["applicationId"])


def _stored_reader(client: Any, install: Install) -> dict:
    """What the App's scope keeps for the reader, or ``{}`` when nothing is kept.

    ``client_id`` once the reader exists, and ``client_secret`` once it is issued.
    """
    if DATA_READER_KEY not in (_scope_keys(client, install) or set()):
        return {}
    value = client.secrets.get_secret(scope=install.app_name, key=DATA_READER_KEY).value
    try:
        stored = json.loads(base64.b64decode(value))
    except (TypeError, ValueError):
        stored = None
    if not isinstance(stored, dict):
        raise LifecycleError(
            f"secret {install.app_name}/{DATA_READER_KEY} is not the data reader's record: "
            "restore it, or delete it and the reader service principal, then run grant_data "
            "again; nothing was changed"
        )
    return stored


def _keep_reader(client: Any, install: Install, reader: DataReader, client_secret: str) -> None:
    kept = {"client_id": reader.application_id}
    if client_secret:
        kept["client_secret"] = client_secret
    client.secrets.put_secret(
        scope=install.app_name, key=DATA_READER_KEY, string_value=json.dumps(kept)
    )


def _create_data_reader(client: Any, install: Install) -> DataReader:
    """Create the data reader with an OAuth secret kept in the App's scope. Four API calls.

    It is given no access to the App's secret scope, so SQL run as it cannot read the App's
    master key. Its client id is kept before its secret is issued, so a run that stops in
    between leaves a reader the next run recognises as this install's and finishes.
    """
    created = _api(
        client,
        "POST",
        SCIM_SERVICE_PRINCIPALS,
        body={"schemas": [SCIM_SERVICE_PRINCIPAL_SCHEMA], "displayName": install.data_reader},
    )
    reader = DataReader(created["id"], created["applicationId"])
    _keep_reader(client, install, reader, "")
    _new_reader_secret(client, install, reader, just_created=True)
    return reader


def _new_reader_secret(
    client: Any, install: Install, reader: DataReader, *, just_created: bool = False
) -> int:
    """Give the reader a new OAuth secret, keep it in the App's scope, delete its others.

    Returns how many older secrets were deleted, so failed runs leave none behind. A
    principal created moments ago (``just_created``) may not be visible to the secrets API
    yet, which then refuses it: retried for up to :data:`NEW_PRINCIPAL_WAIT_SECONDS`.
    Cost: three API calls, plus one per older secret.
    """
    path = f"{SERVICE_PRINCIPAL_SECRETS}/{reader.scim_id}/credentials/secrets"
    deadline = time.monotonic() + (NEW_PRINCIPAL_WAIT_SECONDS if just_created else 0)
    while True:
        try:
            issued = _api(client, "POST", path, body={"lifetime": DATA_READER_SECRET_LIFETIME})
            break
        except (PermissionDenied, NotFound):
            if time.monotonic() >= deadline:
                raise
            time.sleep(NEW_PRINCIPAL_RETRY_SECONDS)
    _keep_reader(client, install, reader, issued["secret"])
    older = [
        entry["id"]
        for entry in _api(client, "GET", path).get("secrets") or []
        if entry["id"] != issued["id"]
    ]
    for secret_id in older:
        _api(client, "DELETE", f"{path}/{secret_id}")
    return len(older)


def _bound_warehouse(app: dict) -> str:
    """The id of the SQL warehouse bound to the App. PURE.

    Raises:
        LifecycleError: the App has no ``sql-warehouse`` bound.
    """
    for resource in app.get("resources") or []:
        warehouse = (resource.get("sql_warehouse") or {}).get("id", "")
        if resource.get("name") == "sql-warehouse" and _WAREHOUSE_ID_RE.fullmatch(warehouse):
            return warehouse
    raise LifecycleError(
        "the App has no sql-warehouse bound with a valid warehouse id: bind the SQL warehouse "
        "its data reader queries on"
    )


def grant_data_access(client: Any, install: Install, data: str, confirm: str) -> list[str]:
    """Let this install's data reader read ``data``, creating the reader first if needed.

    Without ``confirm`` set to the App name, only prints what it would do. Cost: one API
    call for the App, one per named securable to check it exists, the reader lookup, and
    when confirmed three to create the reader, one for the warehouse and one per
    securable to grant.

    Raises:
        LifecycleError: ``data`` is unusable, the App has no SQL warehouse, a named catalog
            or schema does not exist, or the reader's name is taken (nothing is changed).
    """
    install.validate()
    grants = data_grants(data)
    warehouse = _bound_warehouse(_api(client, "GET", f"/api/2.0/apps/{install.app_name}"))
    _require_securables(client, grants)
    reader = data_reader(client, install)
    # Unity Catalog names a service principal by its application id, known once created.
    principal = reader.application_id if reader else f"<application id of {install.data_reader}>"
    setup = (
        []
        if reader
        else [
            f"create service principal {install.data_reader}, with no access to the App's "
            f"secret scope, and keep its OAuth secret in that scope, {install.app_name} "
            f"({DATA_READER_KEY})"
        ]
    )
    setup.append(f"let {install.data_reader} use SQL warehouse {warehouse}")
    statements = [item.statement("GRANT", principal) for item in grants]
    reach = [*_browse_reach(grants), DATA_GRANTS_REACH]
    if confirm != install.app_name:
        return [
            "Dry run: nothing was changed. Set confirm to "
            f"{install.app_name} to " + "; ".join(setup) + "; and run:",
            *statements,
            *reach,
        ]
    if reader is None:
        reader = _create_data_reader(client, install)
        statements = [item.statement("GRANT", reader.application_id) for item in grants]
    elif not _stored_reader(client, install).get("client_secret"):
        # A run that stopped after creating the reader, before its secret was kept; it may
        # have been moments ago.
        _new_reader_secret(client, install, reader, just_created=True)
    _api(
        client,
        "PATCH",
        f"/api/2.0/permissions/sql/warehouses/{warehouse}",
        body={
            "access_control_list": [
                {"service_principal_name": reader.application_id, "permission_level": "CAN_USE"}
            ]
        },
    )
    _change_grants(client, grants, reader.application_id, "add")
    return [
        f"Data reader {install.data_reader} ({reader.application_id}) can use SQL warehouse "
        f"{warehouse}. Granted:",
        *statements,
        *reach,
    ]


def revoke_data_access(client: Any, install: Install, data: str, confirm: str) -> list[str]:
    """Take back what the data reader holds on ``data``, read from its current grants.

    A catalog named on its own loses the reader's privileges on the catalog and on every
    schema in it, so a later grant on the catalog brings none of them back. A schema loses
    only its own, so the other schemas of its catalog stay readable; a schema the reader
    reads through read on its whole catalog is refused, since no revoke on the schema can
    take that away. Without ``confirm`` set to the App name, only prints the statements.
    Cost: the reader lookup, one API call per named securable to check it exists and one
    to read its grants, for a catalog one per page of its schemas and one per schema, and
    when confirmed one per securable that changes.

    Raises:
        LifecycleError: ``data`` is unusable, the install has no data reader, a named
            catalog or schema does not exist, or a named schema is read through its
            catalog (nothing is changed then).
    """
    install.validate()
    names = _data_names(data)
    reader = data_reader(client, install)
    if reader is None:
        raise LifecycleError(
            f"this install has no data reader ({install.data_reader}): nothing to revoke"
        )
    named = [DataGrant("schema" if "." in name else "catalog", name, ()) for name in names]
    _require_securables(client, named)
    revokes = _held_by_reader(client, names, reader.application_id)
    statements = [item.statement("REVOKE", reader.application_id) for item in revokes]
    if not statements:
        return [f"{install.data_reader} holds nothing on {', '.join(names)}: nothing to revoke"]
    if confirm != install.app_name:
        return [
            f"Dry run: nothing was revoked. Set confirm to {install.app_name} to run:",
            *statements,
        ]
    _change_grants(client, revokes, reader.application_id, "remove")
    return ["Revoked:", *statements]


def _held_by_reader(client: Any, names: list[str], principal: str) -> list[DataGrant]:
    """What the reader holds on each named catalog (and its schemas) or schema.

    Raises:
        LifecycleError: a named schema is read through read on its whole catalog.
    """
    held: dict[str, DataGrant] = {}
    whole_catalogs = [name for name in names if "." not in name]
    for catalog in whole_catalogs:
        held[catalog] = DataGrant(
            "catalog", catalog, _privileges(client, "catalog", catalog, principal)
        )
        # include_browse: schemas the admin cannot read are listed too, so none is skipped.
        for schema in _list_names(
            client,
            "/api/2.1/unity-catalog/schemas",
            {"catalog_name": catalog, "include_browse": "true"},
            "schemas",
        ):
            if not _DATA_NAME_RE.fullmatch(schema):
                raise LifecycleError(
                    f"catalog {catalog} lists a schema named {schema!r}, which this notebook "
                    "cannot name in a revoke; nothing was changed"
                )
            full_name = f"{catalog}.{schema}"
            held[full_name] = DataGrant(
                "schema", full_name, _privileges(client, "schema", full_name, principal)
            )
    for name in names:
        catalog = name.split(".", 1)[0]
        if "." not in name or catalog in whole_catalogs:
            continue
        inherited = set(_privileges(client, "catalog", catalog, principal)) & set(
            DATA_READ_PRIVILEGES
        )
        if inherited:
            raise LifecycleError(
                f"{name} is read through read on the whole catalog {catalog}: revoke_data "
                f"{catalog}, then grant_data the schemas to keep; nothing was changed"
            )
        held[name] = DataGrant("schema", name, _privileges(client, "schema", name, principal))
    return [item for item in held.values() if item.privileges]


def _privileges(client: Any, kind: str, full_name: str, principal: str) -> tuple[str, ...]:
    """The privileges ``principal`` holds directly on one catalog or schema; one API call."""
    granted = _api(
        client,
        "GET",
        f"/api/2.1/unity-catalog/permissions/{kind}/{full_name}",
        query={"principal": principal},
    )
    return tuple(
        privilege
        for assignment in granted.get("privilege_assignments") or []
        if assignment.get("principal") == principal
        for privilege in assignment.get("privileges") or []
    )


def rotate_data_reader(client: Any, install: Install, confirm: str) -> str:
    """Give the data reader a new OAuth secret and delete its others.

    The App reads the new secret within five minutes, at once when its cached one is
    refused. Cost: the reader lookup, then three API calls and one per old secret.

    Raises:
        LifecycleError: the install has no data reader, or ``confirm`` is not the App name.
    """
    install.validate()
    reader = data_reader(client, install)
    if reader is None:
        raise LifecycleError(
            f"this install has no data reader ({install.data_reader}): run grant_data first"
        )
    if confirm != install.app_name:
        raise LifecycleError(f"confirm must be exactly the App name {install.app_name!r}")
    deleted = _new_reader_secret(client, install, reader)
    return (
        f"{install.data_reader} has a new OAuth secret in secret scope {install.app_name}; "
        f"deleted {deleted} older secret(s)"
    )


def _browse_reach(grants: list[DataGrant]) -> list[str]:
    """What BROWSE shows beyond the named schemas, for each catalog not read whole. PURE."""
    return [
        f"BROWSE on catalog {item.full_name} also shows the names and metadata (never data) "
        "of everything in it, not only the named schemas."
        for item in grants
        if item.kind == "catalog" and "SELECT" not in item.privileges
    ]


def _require_securables(client: Any, grants: list[DataGrant]) -> None:
    missing = [
        item.full_name
        for item in grants
        if _get(_api, client, "GET", f"/api/2.1/unity-catalog/{item.kind}s/{item.full_name}")
        is None
    ]
    if missing:
        raise LifecycleError(
            f"{', '.join(missing)} does not exist or you cannot see it; nothing was changed"
        )


def _change_grants(client: Any, grants: list[DataGrant], principal: str, change: str) -> None:
    for item in grants:
        _api(
            client,
            "PATCH",
            f"/api/2.1/unity-catalog/permissions/{item.kind}/{item.full_name}",
            body={"changes": [{"principal": principal, change: list(item.privileges)}]},
        )


def data_problems(client: Any, install: Install) -> list[str]:
    """Why deleting the App's data could delete something that is not this install's.

    Everything must carry this install's id; the schema may hold only the ``genesis``
    volume — no other volume, table, function or model, counting objects the admin has no
    privilege on (``include_browse``, which reveals those only when the admin has BROWSE on
    the catalog); the Lakebase project only the ``production`` branch and its
    ``databricks-postgres`` database. A schema listing that does not show the install's own
    volume cannot be trusted to show the rest, so it is a problem too. Cost: about thirteen
    API calls, one more per extra page of a schema or Lakebase listing. The lakehouse
    schema may hold tables (Genesis's lakehouse data, deleted with it) and nothing else.
    """
    install_id = recorded_install_id(client, install)
    if install_id is None:
        return [
            f"secret scope {install.app_name} or its install id is gone: "
            "nothing proves the data is this install's"
        ]
    problems = ownership_problems(client, install, install_id)
    if _get(client.schemas.get, install.schema) is not None:
        volumes = _schema_objects(client, install, "volumes")
        if VOLUME_NAME not in volumes and _get(client.volumes.read, install.volume) is not None:
            problems.append(
                f"listing schema {install.schema} did not show its own volume {VOLUME_NAME}, "
                "so it cannot show what else the schema holds; report it"
            )
        others = [name for name in volumes if name != VOLUME_NAME]
        others += _schema_objects(client, install, "tables")
        others += _schema_objects(client, install, "functions")
        others += _schema_objects(client, install, "models")
        if others:
            problems.append(
                f"schema {install.schema} holds other objects: {', '.join(sorted(others))}"
            )
    for name, full_name, holds in install.table_schemas():
        if _get(client.schemas.get, full_name) is None:
            continue
        # Its tables are the install's (lakehouse data, App telemetry) and go with it.
        others = _schema_objects(client, install, "volumes", name)
        others += _schema_objects(client, install, "functions", name)
        others += _schema_objects(client, install, "models", name)
        if others:
            problems.append(
                f"schema {full_name} holds objects other than {holds} tables: "
                f"{', '.join(sorted(others))}"
            )
    if _project(client, install) is not None:
        # Deleting the project deletes every branch and database in it: read every page.
        branches = _list_names(
            client, f"/api/2.0/postgres/{install.project}/branches", {}, "branches"
        )
        databases = _list_names(
            client, f"/api/2.0/postgres/{install.branch}/databases", {}, "databases"
        )
        others = [name for name in branches if name != install.branch]
        others += [
            name for name in databases if name != f"{install.branch}/databases/{LAKEBASE_DATABASE}"
        ]
        if others:
            problems.append(
                f"Lakebase {install.project} holds other branches or databases: "
                f"{', '.join(sorted(others))}"
            )
    return problems


def _schema_objects(
    client: Any, install: Install, kind: str, schema_name: str | None = None
) -> list[str]:
    """Names of every ``kind`` (volumes, tables, functions, models) in one of the App's
    schemas (``schema_name``, by default the App's own),
    counting objects the admin has no privilege on (``include_browse``) — which the server
    reveals only when the admin has BROWSE on the catalog.

    Through REST: the databricks-sdk a serverless notebook ships (0.20) has no
    ``include_browse`` on any list method. Cost: one API call per page, at the server's page
    size (``max_results`` 0; unset is also the server's page size for volumes, functions and
    models, but would return every table in one response).

    Raises:
        LifecycleError: the listing repeated a page token, so it would never end.
    """
    key = "registered_models" if kind == "models" else kind
    query = {
        "catalog_name": install.catalog,
        "schema_name": schema_name or install.app_name,
        "include_browse": "true",
        "max_results": "0",
    }
    if kind == "tables":
        # Only names are read; columns and properties would make each page far larger.
        query |= {"omit_columns": "true", "omit_properties": "true"}
    return _list_names(client, f"/api/2.1/unity-catalog/{kind}", query, key)


def _list_names(client: Any, path: str, query: dict[str, str], key: str) -> list[str]:
    """Names of every item a paged REST listing returns under ``key``, following
    ``next_page_token``. Cost: one API call per page.

    Raises:
        LifecycleError: the listing repeated a page token, so it would never end.
    """
    names: list[str] = []
    seen_tokens: set[str] = set()
    while True:
        page = _api(client, "GET", path, query=query)
        names += [item["name"] for item in page.get(key) or []]
        token = page.get("next_page_token")
        if not token:
            return names
        if token in seen_tokens:
            raise LifecycleError(
                f"listing {path} repeated its page token; report it; nothing was deleted"
            )
        seen_tokens.add(token)
        query = {**query, "page_token": token}


def app_problems(app: dict, install: Install) -> list[str]:
    """Why an App with the install's name is not this install's Genesis App. PURE."""
    bound = {resource.get("name"): resource for resource in app.get("resources") or []}
    secret = (bound.get("genesis-master-key") or {}).get("secret") or {}
    if (secret.get("scope"), secret.get("key")) != (install.app_name, MASTER_KEY):
        return [
            f"app {install.app_name} does not bind this install's master key "
            f"({install.app_name}/{MASTER_KEY}); it is not this Genesis install"
        ]
    return []


def watchdog(client: Any, install: Install, now: datetime) -> str:
    """Redeploy this install's App from its active source when it has crashed.

    Cost: one API call, two when it redeploys.

    Raises:
        LifecycleError: the App is missing, is not this install's Genesis App, or crashed
            within :data:`CRASH_LOOP_WINDOW` of its deployment.
    """
    install.validate()
    app = _get(_api, client, "GET", f"/api/2.0/apps/{install.app_name}")
    if app is None:
        raise LifecycleError(f"app {install.app_name} does not exist")
    if problems := app_problems(app, install):
        raise LifecycleError("; ".join(problems))
    state = (app.get("app_status") or {}).get("state", "UNKNOWN")
    if state != CRASHED:
        return f"{install.app_name} is {state}: nothing to do"
    active = app.get("active_deployment") or {}
    deployed = active.get("create_time")
    if not deployed:
        raise LifecycleError(
            f"{install.app_name} crashed and its deployment's age is unknown: not redeploying "
            "what may be a release that keeps crashing; read the App's logs"
        )
    if now - datetime.fromisoformat(deployed) < CRASH_LOOP_WINDOW:
        raise LifecycleError(
            f"{install.app_name} crashed within {CRASH_LOOP_WINDOW.seconds // 60} minutes of "
            f"its deployment ({deployed}): not redeploying a release that keeps crashing; "
            "read the App's logs"
        )
    _api(
        client,
        "POST",
        f"/api/2.0/apps/{install.app_name}/deployments",
        body=_redeployment(active),
    )
    return f"{install.app_name} was {state}: redeployed its active source"


def _redeployment(active: dict) -> dict:
    """The deployment request that deploys ``active``'s source again, as it ran. PURE.

    Keeps the deployment's own ``command`` and ``env_vars`` overrides, if any.
    """
    overrides = {key: active[key] for key in ("command", "env_vars") if active.get(key)}
    if git_source := active.get("git_source"):
        # Only what selects the source; the App knows its repository.
        keep = ("tag", "branch", "commit", "source_code_path")
        source = {key: git_source[key] for key in keep if key in git_source}
        return {"git_source": source, **overrides}
    if path := active.get("source_code_path"):
        return {"source_code_path": path, "mode": active.get("mode", "SNAPSHOT"), **overrides}
    raise LifecycleError("the App has no active deployment to redeploy")


def install_watchdog(client: Any, install: Install, notebook_path: str) -> str:
    """Create or update this install's watchdog job; return what it does.

    Cost: one API call per page of jobs named after the watchdog, then one write.

    Raises:
        LifecycleError: no notebook path, no install id (run preinstall), or a job of that
            name that is not this install's.
    """
    install.validate()
    if not notebook_path:
        raise LifecycleError("the watchdog needs this notebook's workspace path")
    install_id = recorded_install_id(client, install)
    if install_id is None:
        raise LifecycleError(f"no install id in secret scope {install.app_name}: run preinstall")
    settings = {
        "name": install.watchdog_job,
        "schedule": {"quartz_cron_expression": WATCHDOG_SCHEDULE, "timezone_id": "UTC"},
        "max_concurrent_runs": 1,
        "tags": {WATCHDOG_TAG: install_id},
        "tasks": [
            {
                "task_key": "watchdog",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "action": "watchdog",
                        "app_name": install.app_name,
                        "catalog": install.catalog,
                    },
                },
            }
        ],
    }
    jobs = _watchdog_jobs(client, install)
    if others := [
        job for job in jobs if job["settings"].get("tags", {}).get(WATCHDOG_TAG) != install_id
    ]:
        raise LifecycleError(
            f"job {install.watchdog_job} exists and is not this install's "
            f"(job ids {', '.join(str(job['job_id']) for job in others)})"
        )
    if jobs:
        # A partial update: settings this action does not own (the failure notifications an
        # operator attached) are kept.
        _api(
            client,
            "POST",
            "/api/2.2/jobs/update",
            body={"job_id": jobs[0]["job_id"], "new_settings": settings},
        )
    else:
        _api(client, "POST", "/api/2.2/jobs/create", body=settings)
    return f"job {install.watchdog_job} redeploys {install.app_name} within five minutes of a crash"


def _watchdog_jobs(client: Any, install: Install) -> list[dict]:
    """Every job named after this install's watchdog; one API call per page of jobs."""
    jobs: list[dict] = []
    query: dict[str, str] = {"name": install.watchdog_job}
    seen_tokens: set[str] = set()
    while True:
        response = _api(client, "GET", "/api/2.2/jobs/list", query=query)
        jobs += response.get("jobs") or []
        token = response.get("next_page_token")
        if not token or token in seen_tokens:
            return jobs
        seen_tokens.add(token)
        query = {**query, "page_token": token}


def uninstall(
    client: Any, install: Install, confirm: str, delete_data: str, report: Report
) -> None:
    """Delete the App and, when ``delete_data`` reads ``delete <app_name> data``, its data.

    ``confirm`` must be the App name. Everything is checked before the first deletion, and
    each deletion is reported as it happens. Cost: about twenty API calls.

    Raises:
        LifecycleError: a confirmation does not match, the App is not this install's
            Genesis App, or the data is not only this install's; nothing is deleted then.
    """
    install.validate()
    if confirm != install.app_name:
        raise LifecycleError(f"confirm must be exactly the App name {install.app_name!r}")
    if delete_data and delete_data != install.data_confirmation:
        raise LifecycleError(
            f"to delete the data, delete_data must read {install.data_confirmation!r}; "
            "leave it empty to remove only the App"
        )
    app = _get(_api, client, "GET", f"/api/2.0/apps/{install.app_name}")
    problems = app_problems(app, install) if app is not None else []
    if delete_data:
        problems += data_problems(client, install)
    if problems:
        raise LifecycleError("refusing to uninstall: " + "; ".join(problems))
    # Looked up before anything is deleted: a reader that is not this install's refuses.
    reader = data_reader(client, install) if delete_data else None
    install_id = recorded_install_id(client, install)
    for job in _watchdog_jobs(client, install):
        # Only the job tagged with this install's id: another of that name is not ours.
        if install_id and job["settings"].get("tags", {}).get(WATCHDOG_TAG) == install_id:
            _api(client, "POST", "/api/2.2/jobs/delete", body={"job_id": job["job_id"]})
            report(f"deleted watchdog job {install.watchdog_job}")
    if app is not None:
        _api(client, "DELETE", f"/api/2.0/apps/{install.app_name}")
        report(f"deleted app {install.app_name}")
    if delete_data:
        _delete_data(client, install, reader, report)


def _delete_data(client: Any, install: Install, reader: DataReader | None, report: Report) -> None:
    """Delete the install's data, after uninstall has checked it is only this install's."""
    if _project(client, install) is not None:
        _api(client, "DELETE", f"/api/2.0/postgres/{install.project}")
        report(f"deleted Lakebase project {install.project}")
    if _get(client.volumes.read, install.volume) is not None:
        client.volumes.delete(install.volume)
        report(f"deleted volume {install.volume}")
    if _get(client.schemas.get, install.schema) is not None:
        client.schemas.delete(install.schema)
        report(f"deleted schema {install.schema}")
    for name, full_name, holds in install.table_schemas():
        if _get(client.schemas.get, full_name) is None:
            continue
        # Its tables go with it (data_problems has checked it holds nothing else); anyone
        # with CREATE_TABLE there could have added one, so each is named in the report.
        tables = sorted(_schema_objects(client, install, "tables", name))
        _api(
            client, "DELETE", f"/api/2.1/unity-catalog/schemas/{full_name}", query={"force": "true"}
        )
        named = f": {', '.join(tables)}" if tables else ""
        report(f"deleted {holds} schema {full_name} with its {len(tables)} tables{named}")
    if reader is not None:
        _api(client, "DELETE", f"{SCIM_SERVICE_PRINCIPALS}/{reader.scim_id}")
        report(f"deleted data reader {install.data_reader}")
    if _scope_keys(client, install) is not None:
        client.secrets.delete_scope(scope=install.app_name)
        report(f"deleted secret scope {install.app_name} (with the master key)")


def run(
    client: Any,
    action: str,
    install: Install,
    confirm: str = "",
    delete_data: str = "",
    report: Report = print,
    *,
    notebook_path: str = "",
    now: datetime | None = None,
    data: str = "",
) -> None:
    """Run one lifecycle action, reporting each line to the admin as it happens."""
    match action:
        case "preinstall":
            for line in preinstall(client, install):
                report(line)
        case "postinstall":
            for line in postinstall(client, install, notebook_path):
                report(line)
        case "grant_data":
            for line in grant_data_access(client, install, data, confirm):
                report(line)
        case "revoke_data":
            for line in revoke_data_access(client, install, data, confirm):
                report(line)
        case "rotate_data_reader":
            report(rotate_data_reader(client, install, confirm))
        case "install_watchdog":
            report(install_watchdog(client, install, notebook_path))
        case "watchdog":
            report(watchdog(client, install, now or datetime.now(UTC)))
        case "uninstall":
            uninstall(client, install, confirm, delete_data, report)
        case _:
            raise LifecycleError(f"action must be one of {', '.join(ACTIONS)}, not {action!r}")


# COMMAND ----------

if __name__ == "__main__" and "dbutils" in globals():
    widgets = dbutils.widgets  # noqa: F821 -- provided by the notebook runtime
    widgets.dropdown("action", "postinstall", list(ACTIONS))
    # The defaults the install form's help text names: App name genesis, and the
    # workspace's default catalog.
    widgets.text("app_name", "genesis")
    widgets.text("catalog", spark.catalog.currentCatalog())  # noqa: F821 -- notebook runtime
    widgets.text("confirm", "")
    widgets.text("delete_data", "")
    # grant_data / revoke_data: catalogs or catalog.schema names, comma-separated.
    widgets.text("data", "")
    run(
        WorkspaceClient(),
        widgets.get("action"),
        Install(widgets.get("app_name").strip(), widgets.get("catalog").strip()),
        widgets.get("confirm").strip(),
        widgets.get("delete_data").strip(),
        notebook_path=dbutils.notebook.entry_point.getDbutils()  # noqa: F821 -- notebook runtime
        .notebook()
        .getContext()
        .notebookPath()
        .get(),
        data=widgets.get("data").strip(),
    )
