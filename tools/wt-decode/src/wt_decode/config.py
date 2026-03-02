"""Configuration file support for wt-decode.

Searches for configuration in this order:
1. .wtd.toml in the current working directory
2. ~/.config/wtd/config.toml
3. ~/.wtd.toml

Example .wtd.toml::

    [defaults]
    page_server = "172.17.0.1:20044"
    key_file = "/data/db/job0/mongorunner/decrypt_key"
    log_id = 1
    decryptor_path = "/path/to/pagedecryptor"
"""

import tomllib
from pathlib import Path
from typing import Any, Optional


def _search_paths() -> list[Path]:
    """Return the list of candidate config file paths in priority order."""
    return [
        Path.cwd() / ".wtd.toml",
        Path.home() / ".config" / "wtd" / "config.toml",
        Path.home() / ".wtd.toml",
    ]


_loaded: Optional[dict[str, Any]] = None
_loaded_path: Optional[Path] = None


def _load() -> dict[str, Any]:
    """Load the config file (cached after first call)."""
    global _loaded, _loaded_path
    if _loaded is not None:
        return _loaded
    for path in _search_paths():
        if path.is_file():
            with open(path, "rb") as f:
                _loaded = tomllib.load(f)
                _loaded_path = path
                return _loaded
    _loaded = {}
    return _loaded


def get(key: str, default: Any = None) -> Any:
    """Return a value from the ``[defaults]`` section of the config file."""
    return _load().get("defaults", {}).get(key, default)


def loaded_path() -> Optional[Path]:
    """Return the path of the config file that was loaded, or ``None``."""
    _load()
    return _loaded_path


def all_defaults() -> dict[str, Any]:
    """Return all key/value pairs from the ``[defaults]`` section."""
    return dict(_load().get("defaults", {}))
