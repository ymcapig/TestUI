from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class RunHistoryService:
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)

    def runs_root(self) -> Path:
        return self.project_root / "runs"

    def latest_run_path(self, sn: str) -> Path:
        return self.runs_root() / sn / "latest_run.json"

    def load_latest_run_info(self, sn: str):
        latest_path = self.latest_run_path(sn)
        if not latest_path.exists():
            return None, latest_path, None
        try:
            data = json.loads(latest_path.read_text(encoding="utf-8-sig"))
        except Exception:
            return None, latest_path, None

        run_id = data.get("run_id")
        run_dir = self.runs_root() / sn / run_id if run_id else None
        return data, latest_path, run_dir

    def find_report_file(self, sn: str, run_dir: Path, info: dict[str, Any]) -> Path | None:
        report_dir = run_dir / "reports"
        run_id = info.get("run_id")
        if run_id:
            candidate = report_dir / f"report_{sn}_{run_id}.json"
            if candidate.exists():
                return candidate
        if report_dir.exists():
            json_files = sorted(report_dir.glob("*.json"))
            if json_files:
                return json_files[-1]
        return None

    def load_report_rows(self, sn: str, run_dir: Path, info: dict[str, Any]):
        report_file = self.find_report_file(sn, run_dir, info)
        if not report_file:
            return [], None
        try:
            rows = json.loads(report_file.read_text(encoding="utf-8-sig"))
        except Exception:
            rows = []
        return rows, report_file

    def mark_any_running_aborted(self, status: str = "aborted") -> int:
        """掃 runs 底下所有 latest_run.json，把仍是 running/pending 的標記為 aborted。
        用於 closeEvent 拿不到正確 SN 時的備援。回傳標記到的數量。"""
        root = self.runs_root()
        if not root.exists():
            return 0
        for latest_path in root.glob("*/latest_run.json"):
            try:
                data = json.loads(latest_path.read_text(encoding="utf-8-sig"))
            except Exception:
                continue
            if (data.get("status") or "").lower() not in ("running", "pending"):
                continue
            data["status"] = status
            import time
            data["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            try:
                tmp = latest_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(data, indent=2), encoding="utf-8-sig")
                tmp.replace(latest_path)
            except Exception:
                pass

    def clear_snapshot(self, snapshot: dict[str, Any]) -> None:
        latest_path = snapshot.get("latest_path")
        run_dir = snapshot.get("run_dir")

        if latest_path and Path(latest_path).exists():
            Path(latest_path).unlink()
        if run_dir and Path(run_dir).exists():
            for state_file in Path(run_dir).glob("steps/*/state.json"):
                try:
                    state_file.unlink()
                except Exception:
                    pass