from __future__ import annotations
from pathlib import Path


def resolve_workdir(raw: str, project_root: Path) -> Path:
    """把 [run] workdir / step workdir 的原始字串解析成絕對路徑。

    唯一規則：
      - 絕對路徑 → 照用
      - 相對路徑 → 一律以 project_root 為基底
      - 空字串 / "." → project_root
    UI 顯示與 Runner 實際 cwd 都走這裡，確保兩邊一致。
    """
    raw = (raw or "").strip()
    if not raw:
        return project_root
    base = Path(raw)
    if base.is_absolute():
        return base
    return (project_root / base).resolve()