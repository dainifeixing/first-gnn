from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def ensure_parent(path: str | Path) -> Path:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def write_json_file(output_path: str | Path, payload: Any) -> Path:
    path = ensure_parent(output_path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def write_markdown_lines(output_path: str | Path, lines: list[str]) -> Path:
    path = ensure_parent(output_path)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
