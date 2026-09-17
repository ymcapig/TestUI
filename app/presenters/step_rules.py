"""
Step 規則服務：集中管理每一筆 step 的 ignore 狀態與備註文字。

這個類別原本是 MainWindow 裡的 _effective_ignore / _ignore_note /
_combined_note / _step_notes / ignore_overrides / step_ignore_defaults 的
散裝欄位與 method。拆出來是因為這些完全是「資料 + 規則」，跟 Qt widget
無關，UI 只需要問它「這個 sid 的 ignore 與 note 是什麼？」。
"""

from __future__ import annotations

from typing import Callable, Dict, Optional


class StepRulesService:
    def __init__(self, translator: Callable[..., str]):
        self._ = translator
        # 由 station.ini 讀出的每個 step 預設 ignore
        self.defaults: Dict[str, bool] = {}
        # 使用者在 UI 上臨時覆寫的 ignore 值
        self.overrides: Dict[str, bool] = {}
        # 每個 step 跑完後累積的備註文字
        self.notes: Dict[str, str] = {}

    # ---- 設定 ----------------------------------------------------------------

    def reset_defaults(self, defaults: Dict[str, bool]) -> None:
        """套用新的 station.ini 設定。未出現在新設定裡的 override 會被清掉。"""
        self.defaults = dict(defaults)
        self.overrides = {
            sid: val for sid, val in self.overrides.items() if sid in self.defaults
        }

    def sync_notes_with_steps(self, sids: list[str]) -> None:
        """只保留當前 model 裡還存在的 sid 的備註。"""
        existing = dict(self.notes)
        self.notes = {sid: existing.get(sid, "") for sid in sids}

    def clear_notes(self) -> None:
        self.notes = {sid: "" for sid in self.defaults}

    def remap_for_sid_change(self, sid_mapping: Dict[str, str]) -> None:
        """step 順序存檔後 sid 可能改名，這裡跟著改。"""
        self.overrides = {sid_mapping.get(k, k): v for k, v in self.overrides.items()}
        self.notes = {sid_mapping.get(k, k): v for k, v in self.notes.items()}

    # ---- 讀取 ----------------------------------------------------------------

    def effective_ignore(self, sid: str) -> bool:
        return self.overrides.get(sid, self.defaults.get(sid, False))

    def ignore_note(self, sid: str) -> str:
        default = self.defaults.get(sid, False)
        if sid in self.overrides:
            if self.overrides[sid]:
                return self._("ignore.note.force_on", default="Ignore result: forced on")
            return self._("ignore.note.force_off", default="Ignore result: forced off")
        if default:
            return self._("ignore.note.config", default="Ignore result: configuration default")
        return ""

    def combined_note(self, sid: str) -> str:
        parts: list[str] = []
        base = (self.notes.get(sid) or "").strip()
        ignore = self.ignore_note(sid)
        if base:
            parts.append(base)
        if ignore:
            parts.append(ignore)
        return " | ".join(parts)

    # ---- 改變 ----------------------------------------------------------------

    def set_run_note(self, sid: str, note: Optional[str]) -> None:
        self.notes[sid] = (note or "").strip()

    def toggle_override(self, sid: str) -> bool:
        """切換 override，回傳切換後的有效值。若切換後等於 default 就自動清掉 override。"""
        default = self.defaults.get(sid, False)
        current = self.overrides.get(sid, default)
        new_value = not current
        if new_value == default:
            self.overrides.pop(sid, None)
        else:
            self.overrides[sid] = new_value
        return new_value
