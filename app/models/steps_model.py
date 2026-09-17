"""
StepsModel：QTableView 的資料模型。

只做 Qt model 的本份：
- 保存每一筆 step 的當前狀態（status、time、attempts、note、ignore）
- 回應 View 的查詢（data / headerData / rowCount / columnCount / flags）
- 狀態變動時 emit dataChanged 通知 View 重繪

刻意不做：
- 讀寫檔案（交給 ConfigService）
- 決定顏色（查 app.ui.theme）
- 業務規則驗證（目前沒有；以前的「上下限」功能已從此檔移除）
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Optional

from PyQt5.QtCore import QAbstractTableModel, QModelIndex, Qt, QVariant, pyqtSignal
from PyQt5.QtGui import QBrush, QColor

from app.ui.theme import (
    STEP_STATUS_BG,
    STEP_STATUS_BG_DEFAULT,
    STEP_STATUS_FG,
    STEP_STATUS_FG_DEFAULT,
)
from core.events import StepState

log = logging.getLogger(__name__)


class Col(IntEnum):
    """Table 的欄位索引。新增 / 移除欄位時只需要改這裡跟 COLUMNS。"""
    STEP = 0
    STATUS = 1
    LOWER_LIMIT = 2
    UPPER_LIMIT = 3
    IGNORE = 4
    TIME = 5
    ATTEMPTS = 6
    NOTE = 7


# (i18n key, 預設英文) —— 順序必須跟 Col 一致
COLUMNS: list[tuple[str, str]] = [
    ("table.column.step", "Step"),
    ("table.column.status", "Status"),
    ("table.column.lower_limit", "Lower Limit"),
    ("table.column.upper_limit", "Upper Limit"),
    ("table.column.ignore", "Ignore Result"),
    ("table.column.time", "Time(s)"),
    ("table.column.attempts", "Attempts"),
    ("table.column.note", "Note"),
]


@dataclass
class StepRow:
    """Table 的一列資料。"""
    sid: str
    name: str
    max_attempts: int
    status: str = StepState.NOT_RUN
    ignore: bool = False
    time: float = 0.0
    attempts: int = 0
    note: str = ""
    idx: Optional[int] = None   # reorder 後才會被填入，預設 None
    # =================
    # 新增：上下限
    # =================
    lower_limit: Optional[str] = None
    upper_limit: Optional[str] = None
    pass_by: str = "exit_code:0" # TEST: pass_by 規則需要再改


class StepsModel(QAbstractTableModel):
    # 當使用者輸入值違反規則時發出（題外話：目前的 model 沒有 editable 欄位，
    # 這個 signal 暫時用不到，但保留 API 形狀讓日後要加欄位時不用改動周邊）
    error_occurred = pyqtSignal(str, str)
    limit_changed = pyqtSignal(str, str, str) # (sid, key, value)

    def __init__(
        self,
        steps: list[dict[str, Any]],
        step_runtime: Optional[dict[str, dict[str, Any]]] = None,
        step_limits: Optional[dict[str, dict[str, Any]]] = None, # 接收 limit 參數
        translator: Optional[Callable[..., str]] = None,
    ):
        """
        Args:
            steps: 每筆 {"sid": str, "name": str, "ignore": bool} 的清單。
            step_runtime: {sid: {"retry": int}} 的對照表，由 ConfigService 提供。
                          缺項會以預設值 (retry=0 → max_attempts=1) 填補。
            translator: i18n 函數；None 時用預設英文。
        """
        super().__init__()
        self._translator = translator
        runtime = step_runtime or {}
        limits = step_limits or {}    # 新增


        self.rows: list[StepRow] = []
        for s in steps:
            sid = s["sid"]
            retry = int(runtime.get(sid, {}).get("retry", 0))
            lim = limits.get(sid, {})
            self.rows.append(StepRow(
                sid=sid,
                name=s["name"],
                ignore=bool(s.get("ignore", False)),
                max_attempts=retry + 1,
                lower_limit=lim.get("lower_limit"),
                upper_limit=lim.get("upper_limit"),
                pass_by=lim.get("pass_by", "exit_code:0"),
            ))

    # ---- Qt model protocol --------------------------------------------------

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self.rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(COLUMNS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return QVariant()
        if orientation == Qt.Horizontal:
            key, default = COLUMNS[section]
            return self._tr(key, default)
        return section + 1
    
    @staticmethod
    def _uses_limit(pass_by: str, side: str) -> bool:
        """
        判斷某個 step 的 pass_by 規則是否需要顯示/編輯這側的 limit。
        
        side: "lower" or "upper"
        
        將來若新增規則，只要改這個函數，data/flags/setData 都不用動。
        """
        rule = (pass_by or "").strip().lower().split(":", 1)[0]
        if side == "lower":
            return rule in ("within_range", "upper_threshold")
        if side == "upper":
            return rule in ("within_range", "lower_threshold")
        return False

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return QVariant()
        row = self.rows[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            if col == Col.STEP:
                return row.name
            if col == Col.STATUS:
                return row.status
            
            if col == Col.LOWER_LIMIT:
                if self._uses_limit(row.pass_by, "lower"):
                    return row.lower_limit if row.lower_limit else "-9999.99"
                return "(N/A)"
            
            if col == Col.UPPER_LIMIT:
                if self._uses_limit(row.pass_by, "upper"):
                    return row.upper_limit if row.upper_limit else "9999.99"
                return "(N/A)"
            
            if col == Col.IGNORE:
                return (
                    self._tr("table.ignore.yes", "Y") if row.ignore
                    else self._tr("table.ignore.no", "N")
                )
            if col == Col.TIME:
                return row.time
            if col == Col.ATTEMPTS:
                return f"{row.attempts}/{row.max_attempts}"
            if col == Col.NOTE:
                return row.note

        if role == Qt.ForegroundRole:
            if col == Col.STATUS:
                return STEP_STATUS_FG.get(row.status, STEP_STATUS_FG_DEFAULT)
            if col == Col.LOWER_LIMIT and not self._uses_limit(row.pass_by, "lower"):
                return QBrush(QColor("#aaaaaa"))
            if col == Col.UPPER_LIMIT and not self._uses_limit(row.pass_by, "upper"):
                return QBrush(QColor("#aaaaaa"))

        if role == Qt.BackgroundRole and col == Col.STATUS:
            return STEP_STATUS_BG.get(row.status, STEP_STATUS_BG_DEFAULT)

        return QVariant()

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        if not index.isValid():
            return Qt.NoItemFlags
        fl = Qt.ItemIsEnabled | Qt.ItemIsSelectable
        
        row = self.rows[index.row()]
        col = index.column()
        
        if col == Col.LOWER_LIMIT and self._uses_limit(row.pass_by, "lower"):
            fl |= Qt.ItemIsEditable
        if col == Col.UPPER_LIMIT and self._uses_limit(row.pass_by, "upper"):
            fl |= Qt.ItemIsEditable
        
        return fl

    # ---- 對外的更新 API -----------------------------------------------------

    def set_status(
        self,
        sid: str,
        status: str,
        time_s: Optional[float] = None,
        attempts: Optional[int] = None,
        note: Optional[str] = None,
    ) -> None:
        for i, row in enumerate(self.rows):
            if row.sid != sid:
                continue
            row.status = status
            if time_s is not None:
                row.time = time_s
            if attempts is not None:
                row.attempts = attempts
            if note is not None:
                row.note = note
            tl = self.index(i, 0)
            br = self.index(i, self.columnCount() - 1)
            self.dataChanged.emit(tl, br, [])
            return

    def set_ignore(self, sid: str, ignore: bool) -> None:
        for i, row in enumerate(self.rows):
            if row.sid != sid:
                continue
            row.ignore = bool(ignore)
            idx = self.index(i, int(Col.IGNORE))
            self.dataChanged.emit(idx, idx, [])
            return

    def reset_states(self) -> None:
        if not self.rows:
            return
        for row in self.rows:
            row.status = StepState.NOT_RUN
            row.time = 0
            row.attempts = 0
            row.note = ""
        tl = self.index(0, 0)
        br = self.index(len(self.rows) - 1, self.columnCount() - 1)
        self.dataChanged.emit(tl, br, [])

    def apply_flags(self, flags: Optional[dict[str, Any]]) -> None:
        """
        把外部（FlowRunner 或歷史紀錄）提供的 step 旗標套用到 model。
        flags 可以是 {sid: object} 或 {sid: dict}，只要有 status / note 屬性/鍵即可。
        """
        if not flags:
            return
        valid_statuses = {
            StepState.NOT_RUN, StepState.RUNNING, StepState.PASS, StepState.FAIL,
            StepState.TIMEOUT, StepState.SKIPPED, StepState.IGNORED,
        }
        for row in self.rows:
            flag = flags.get(row.sid)
            if not flag:
                continue
            status = self._extract(flag, "status")
            note = self._extract(flag, "note", default="")
            if not status:
                continue
            if status not in valid_statuses:
                status = StepState.NOT_RUN
            self.set_status(row.sid, status, note=note)
        
    def setData(self, index: QModelIndex, value: Any, role: int = Qt.EditRole) -> bool:
        if not index.isValid() or role != Qt.EditRole:
            return False
        
        col = index.column()
        if col not in (Col.LOWER_LIMIT, Col.UPPER_LIMIT):
            return False
        
        row = self.rows[index.row()]
        try:
            new_val = float(value)
        except (TypeError, ValueError):
            return False
        
        # 跨 limit 檢驗
        if col == Col.LOWER_LIMIT and row.upper_limit is not None:
            try:
                if new_val > float(row.upper_limit):
                    self.error_occurred.emit(
                        self._tr("dialog.error.title", "Input Error"),
                        self._tr("dialog.error.lower_high", "Lower limit cannot > Upper limit"),
                    )
                    return False
            except ValueError:
                pass
        
        if col == Col.UPPER_LIMIT and row.lower_limit is not None:
            try:
                if new_val < float(row.lower_limit):
                    self.error_occurred.emit(
                        self._tr("dialog.error.title", "Input Error"),
                        self._tr("dialog.error.higher_low", "Upper limit cannot < Lower limit"),
                    )
                    return False
            except ValueError:
                pass
        
        # 更新記憶體
        new_str = str(new_val)
        key = "lower_limit" if col == Col.LOWER_LIMIT else "upper_limit"
        if col == Col.LOWER_LIMIT:
            row.lower_limit = new_str
        else:
            row.upper_limit = new_str
        
        # 發 signal 請求寫 ini
        self.limit_changed.emit(row.sid, key, new_str)
        
        # 通知 view 更新
        self.dataChanged.emit(index, index, [Qt.DisplayRole])
        return True

    # ---- 內部 ---------------------------------------------------------------

    def _tr(self, key: str, default: str) -> str:
        if self._translator:
            return self._translator(key, default=default)
        return default

    @staticmethod
    def _extract(flag: Any, name: str, default: Any = None) -> Any:
        """flag 可能是 dataclass-like 物件或 dict，統一取出欄位。"""
        if isinstance(flag, dict):
            return flag.get(name, default)
        return getattr(flag, name, default)
