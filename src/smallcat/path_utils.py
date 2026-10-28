"""Path and URI utilities for POSIX-style joining and normalization.

These helpers keep URI schemes intact (e.g., `file://`, `gs://`) while
joining paths using POSIX semantics. They also provide a safe way to coerce
absolute-like inputs into relative POSIX paths.
"""

import posixpath
import re


def _safe_relative(rel: str) -> str:
    """Normalize a relative path and refuse any that escapes its base.

    Leading slashes are dropped (the path is always relative), ``.`` segments
    and duplicate slashes are collapsed, and ``..`` segments are resolved. A
    path that would climb above its starting point (``../x``, ``a/../../x``)
    raises, since joined under a base it would reach outside of it. Forward
    slashes and backslashes both count as separators for the check, so
    Windows-style traversal is caught too.

    Args:
        rel: Relative path, possibly with a leading slash.

    Returns:
        The normalized relative POSIX path. Empty if ``rel`` is empty or
        resolves to the base itself.

    Raises:
        ValueError: If ``rel`` escapes the base location.
    """
    depth = 0
    for segment in rel.replace("\\", "/").split("/"):
        if segment in {"", "."}:
            continue
        depth += -1 if segment == ".." else 1
        if depth < 0:
            msg = f"Path escapes the base location: {rel!r}"
            raise ValueError(msg)
    normalized = posixpath.normpath(rel.lstrip("/"))
    if normalized == ".":
        return ""
    # Keep a trailing slash: it marks a directory for DuckDB/glob consumers.
    return normalized + "/" if rel.endswith("/") else normalized


def norm_join_uri(base_uri: str, rel: str) -> str:
    """Join a base URI with a relative path using POSIX rules.

    Preserves the original scheme (e.g., `file://`, `gs://`), removes
    duplicate slashes, and trims trailing slashes from the base before joining.
    If `base_uri` does not include a scheme, it is treated as a local path
    base. A bare `file://` base has no location, so `rel` is returned as a
    relative POSIX path (resolved against the current directory).

    Args:
        base_uri: Base URI or local path. Can include a scheme like
            `file://` or `gs://`.
        rel: Relative path to join against `base_uri`. Leading slashes are
            ignored to prevent accidental absolute paths, and the path is
            normalized (``a/./b//c`` -> ``a/b/c``).

    Returns:
        The joined path string with the scheme preserved when present.

    Raises:
        ValueError: If `rel` escapes `base_uri` through ``..`` segments.

    Examples:
        >>> norm_join_uri("gs://bucket/prefix", "a/b.parquet")
        'gs://bucket/prefix/a/b.parquet'
        >>> norm_join_uri("file:///tmp/data/", "/table/part-000.parquet")
        'file:///tmp/data/table/part-000.parquet'
        >>> norm_join_uri("/var/data", "logs/2025-10-15")
        '/var/data/logs/2025-10-15'
    """
    m = re.match(r"^(?P<scheme>[a-zA-Z0-9+.-]+)://(?P<body>.*)$", base_uri)
    if not m:
        # Treat as local path base (no scheme present).
        return posixpath.join(base_uri.rstrip("/"), _safe_relative(rel))
    scheme, body = m.group("scheme"), m.group("body")
    if body == "" and scheme == "file":
        # A bare `file://` carries no location. Joining would make the first
        # segment a host or turn the path into an absolute one, so return a
        # relative path, resolved against the current directory.
        return to_relative_posix_path(rel)
    rel = _safe_relative(rel)
    if body and not body.strip("/"):
        # Root base such as `file:///`: keep the empty authority and the root.
        return f"{scheme}:///{rel}"
    joined = posixpath.join(body.rstrip("/"), rel)
    return f"{scheme}://{joined}"


def to_relative_posix_path(path: str) -> str:
    """Return a POSIX-style relative path from any input.

    Strips a leading slash (if present) and normalizes the path so the result
    is guaranteed to be relative, POSIX-normalized and inside its starting
    point.

    Args:
        path: Input path that may start with `/` or already be relative.

    Returns:
        A POSIX-style relative path.

    Raises:
        ValueError: If the path climbs above its starting point (``..``).

    Examples:
        >>> to_relative_posix_path("/foo/bar")
        'foo/bar'
        >>> to_relative_posix_path("foo/bar")
        'foo/bar'
    """
    return posixpath.join("", _safe_relative(path))
