"""Write watermark used to bound which legacy records may be migrated."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from datetime import datetime
from typing import Optional, Union


def parse_event_time(value: Union[str, datetime]) -> datetime:
    """Parse an ISO-8601 business timestamp (a trailing ``Z`` is accepted)."""
    if isinstance(value, datetime):
        return value
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


@dataclass
class WriteWatermark:
    """High-water mark for the legacy writer.

    Records whose business timestamp is newer than ``cutoff`` are not yet
    stable and must wait for a later migration round. ``cutoff`` may be
    ``None`` while the legacy writer is still backfilling without a known
    boundary, in which case every record is eligible.
    """

    cutoff: Optional[datetime] = None

    @classmethod
    def from_iso(cls, value: Optional[str]) -> "WriteWatermark":
        return cls(parse_event_time(value) if value else None)

    def allows(self, event_time: Union[str, datetime]) -> bool:
        parsed = parse_event_time(event_time)
        return self.cutoff is None or parsed <= self.cutoff

    def to_dict(self) -> dict:
        return {"cutoff": self.cutoff.isoformat() if self.cutoff else None}

    def save(self, path: str) -> None:
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        tmp = os.path.join(parent, f".{os.path.basename(path)}.tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(self.to_dict(), handle, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    @classmethod
    def load(cls, path: str) -> "WriteWatermark":
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        return cls.from_iso(payload.get("cutoff"))
