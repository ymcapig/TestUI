import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from .events import StepState


@dataclass
class StepFlag:
    step_id: str
    status: str
    updated_at: str
    note: str = ""
    attempt: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_pass(self) -> bool:
        return self.status == StepState.PASS

    @property
    def is_fail(self) -> bool:
        return self.status in {StepState.FAIL, StepState.TIMEOUT}


class StepStateStore:
    def __init__(self, run_dir: Path):
        self.run_dir = Path(run_dir)
        self.steps_dir = self.run_dir / "steps"

    def flag_path(self, step_id: str) -> Path:
        return self.steps_dir / step_id / "state.json"

    def read(self, step_id: str) -> Optional[StepFlag]:
        path = self.flag_path(step_id)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            return None
        status = str(data.get("status", "")).strip().upper()
        if status not in {
            StepState.NOT_RUN,
            StepState.RUNNING,
            StepState.PASS,
            StepState.FAIL,
            StepState.TIMEOUT,
            StepState.SKIPPED,
            StepState.IGNORED,
        }:
            status = StepState.NOT_RUN
        extra_payload = data.get("extra")
        if not isinstance(extra_payload, dict):
            extra_payload = {}
        known_keys = {"step_id", "status", "updated_at", "note", "attempt", "extra"}
        for key, value in data.items():
            if key not in known_keys and key not in extra_payload:
                extra_payload[key] = value
        return StepFlag(
            step_id=step_id,
            status=status,
            updated_at=str(data.get("updated_at", "")),
            note=str(data.get("note", "")),
            attempt=int(data.get("attempt", 0) or 0),
            extra=extra_payload,
        )

    def load_all(self) -> Dict[str, StepFlag]:
        result: Dict[str, StepFlag] = {}
        if not self.steps_dir.exists():
            return result
        for step_dir in self.steps_dir.iterdir():
            if not step_dir.is_dir():
                continue
            flag = self.read(step_dir.name)
            if flag:
                result[flag.step_id] = flag
        return result

    def write(
        self,
        step_id: str,
        status: str,
        *,
        note: str = "",
        attempt: int = 0,
        extra: Optional[Dict[str, Any]] = None,
    ) -> StepFlag:
        status = status or StepState.NOT_RUN
        merged_extra = {k: v for k, v in (extra or {}).items() if v is not None}
        record = {
            "step_id": step_id,
            "status": status,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "note": note or "",
            "attempt": attempt,
            "extra": merged_extra,
        }
        for key, value in merged_extra.items():
            if key not in {"exit_code", "matched_rule"}:
                record[key] = value
        path = self.flag_path(step_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(record, indent=2), encoding="utf-8-sig")
        tmp_path.replace(path)
        return StepFlag(
            step_id=step_id,
            status=status,
            updated_at=record["updated_at"],
            note=record["note"],
            attempt=attempt,
            extra=merged_extra,
        )

    def remove(self, step_id: str) -> None:
        path = self.flag_path(step_id)
        if path.exists():
            try:
                path.unlink()
            except Exception:
                pass

    def reset_all(self) -> None:
        if not self.steps_dir.exists():
            return
        for step_dir in self.steps_dir.iterdir():
            if not step_dir.is_dir():
                continue
            flag = step_dir / "state.json"
            if flag.exists():
                try:
                    flag.unlink()
                except Exception:
                    pass
