from dataclasses import dataclass, field
from typing import Any, Optional

@dataclass(frozen=True)
class PageTuple:
    """Identifies a single page version in the page service."""
    page_id: int
    lsn: int

@dataclass
class DecodedPageInfo:
    """Summary information collected for each visited page."""
    page_id: int
    lsn: int
    page_type: str = "UNKNOWN"
    write_gen: Optional[int] = None
    decrypted_size: int = 0
    num_children: int = 0
    children: list[dict[str, Any]] = field(default_factory=list)
    # File paths of saved artifacts.
    page_json_path: str = ""
    decrypted_path: str = ""
    decoded_path: str = ""
    has_deltas: bool = False
    num_deltas: int = 0
    decoded_delta_paths: list[str] = field(default_factory=list)
