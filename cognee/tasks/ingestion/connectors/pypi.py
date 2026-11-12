"""PyPI connector for cognee: Python package metadata and documentation as documents.

A coding assistant needs structured context about the libraries a codebase depends on —
summary, README, license, project URLs — and today a developer scrapes or copy-pastes it.
This source queries the official, zero-authentication PyPI JSON API and publishes one
document per package, so ``recall("how does fastapi handle dependencies")`` answers from
the real documentation. See ``pypi.md``.
"""

import json
from collections.abc import Iterator
from typing import Any

from cognee.shared.logging_utils import get_logger

logger = get_logger("pypi_connector")

PYPI_API_URL = "https://pypi.org/pypi/{package}/json"
_TIMEOUT = (10, 30)
NODE_SET_COLUMN = "cognee_node_set"


class PyPIError(Exception):
    """Base error for PyPI connector errors."""


class PyPIPackageNotFoundError(PyPIError):
    """The package does not exist on PyPI (HTTP 404)."""


class PyPIRateLimitError(PyPIError):
    """PyPI rate-limited the request (HTTP 429)."""


class PyPIClient:
    """HTTP client for the PyPI JSON API. ``session`` is injectable for tests."""

    def __init__(self, session: Any = None):
        if session is None:
            import requests

            session = requests.Session()
        self._session = session

    def get_package(self, package_name: str) -> dict[str, Any]:
        """The full JSON payload of one package: ``info``, ``releases`` and ``urls``."""
        url = PYPI_API_URL.format(package=package_name.strip())
        response = self._session.get(
            url,
            headers={"User-Agent": "cognee-pypi-connector/1.0", "Accept": "application/json"},
            timeout=_TIMEOUT,
        )
        if response.status_code == 404:
            raise PyPIPackageNotFoundError(f"Package {package_name!r} does not exist on PyPI.")
        if response.status_code == 429:
            raise PyPIRateLimitError("PyPI rate limit exceeded; retry later.")
        response.raise_for_status()
        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError) as error:
            raise PyPIError(f"PyPI returned a non-JSON payload for {package_name!r}.") from error
        if not isinstance(payload, dict) or not isinstance(payload.get("info"), dict):
            raise PyPIError(f"PyPI returned a payload without an info object for {package_name!r}.")
        return payload


def format_pypi_document(payload: dict[str, Any]) -> dict[str, Any]:
    """One PyPI payload as a document row: metadata lines and the full README."""
    info = payload.get("info") or {}
    name = str(info.get("name") or "").strip()
    if not name:
        raise PyPIError("The payload's info carries no package name.")

    lines = [f"# {name}"]
    version = info.get("version")
    if version:
        lines.append(f"Version: {version}")
    summary = info.get("summary")
    if summary:
        lines.append(f"Summary: {summary}")
    author = info.get("author") or info.get("maintainer")
    if author:
        lines.append(f"Author: {author}")
    email = info.get("author_email") or info.get("maintainer_email")
    if email:
        lines.append(f"Email: {email}")
    license_field = info.get("license") or (
        info.get("license_expression") if isinstance(info.get("license_expression"), str) else None
    )
    if license_field:
        lines.append(f"License: {license_field}")
    requires_python = info.get("requires_python")
    if requires_python:
        lines.append(f"Requires Python: {requires_python}")
    keywords = info.get("keywords")
    if keywords:
        rendered = ", ".join(keywords) if isinstance(keywords, list) else str(keywords)
        lines.append(f"Keywords: {rendered}")
    project_urls = info.get("project_urls") or {}
    if isinstance(project_urls, dict):
        for label, url in project_urls.items():
            if label and url:
                lines.append(f"{label}: {url}")
    home_page = info.get("home_page")
    if home_page:
        lines.append(f"Home page: {home_page}")

    description = info.get("description")
    if description:
        lines.append("")
        lines.append(description)

    return {
        "id": f"pypi:package:{name}",
        "title": f"PyPI: {name}",
        "content": "\n".join(lines).strip(),
        "url": (project_urls or {}).get("Homepage")
        or home_page
        or f"https://pypi.org/project/{name}/",
        "_deleted": False,
        NODE_SET_COLUMN: "pypi:package",
    }


def pypi_source(
    package_name: str | list[str],
    session: Any = None,
) -> Iterator[dict[str, Any]]:
    """Yield one document per package, querying the official PyPI JSON API.

    Hand the result to ``cognee.remember(...)``. A package's id is its name, so re-syncing
    a package after a new release updates the document in place instead of duplicating it.
    A package that does not exist raises ``PyPIPackageNotFoundError`` — an explicit name
    that misses is a caller error, never silently skipped.

    Args:
        package_name: One package name, or a list of them (duplicates are ingested once).
        session: Optional HTTP session for custom connections and test mocking.
    """
    names = [package_name] if isinstance(package_name, str) else list(package_name)
    if not names or not all(isinstance(name, str) and name.strip() for name in names):
        raise ValueError("package_name must be a non-empty string or a list of them.")
    seen: set[str] = set()
    client = PyPIClient(session=session)
    for name in names:
        cleaned = name.strip()
        if cleaned in seen:
            continue
        seen.add(cleaned)
        payload = client.get_package(cleaned)
        document = format_pypi_document(payload)
        logger.info(
            "PyPI: ingested %s (version %s).", cleaned, payload.get("info", {}).get("version")
        )
        yield document
