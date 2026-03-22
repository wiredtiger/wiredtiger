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

    # 1. Load built-in defaults
    _loaded = {}
    defaults_path = Path(__file__).parent / "defaults.toml"
    if defaults_path.is_file():
        try:
            with open(defaults_path, "rb") as f:
                _loaded = tomllib.load(f)
        except Exception:
            pass

    # 2. Layer user config on top
    for path in _search_paths():
        if path.is_file():
            try:
                with open(path, "rb") as f:
                    user_config = tomllib.load(f)
                    if "defaults" in user_config:
                        if "defaults" not in _loaded:
                            _loaded["defaults"] = {}
                        _loaded["defaults"].update(user_config["defaults"])
                    _loaded_path = path
                    break
            except Exception:
                continue

    return _loaded


def get(key: str, default: Any = None) -> Any:
    """Return a value from the ``[defaults]`` section of the config file."""
    return _load().get("defaults", {}).get(key, default)


def get_path(key: str, default: Optional[Path] = None) -> Optional[Path]:
    """Return a Path from the config, expanding ~."""
    val = get(key)
    if val is None:
        return default
    return Path(val).expanduser()


def get_path_list(key: str, default: list[Path] = []) -> list[Path]:
    """Return a list of Paths from the config, expanding ~ for each."""
    val = get(key)
    if val is None:
        return default
    return [Path(p).expanduser() for p in val]


def loaded_path() -> Optional[Path]:
    """Return the path of the config file that was loaded, or ``None``."""
    _load()
    return _loaded_path


def all_defaults() -> dict[str, Any]:
    """Return all key/value pairs from the ``[defaults]`` section."""
    return dict(_load().get("defaults", {}))
