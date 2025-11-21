from PyQt5.QtCore import QAbstractTableModel, Qt, QVariant, QModelIndex
from PyQt5.QtGui import QColor, QBrush
from core.events import StepState
from typing import Callable, Optional

COLUMNS = [
    ("table.column.step", "Step"),
    ("table.column.status", "Status"),
    ("table.column.ignore", "Ignore Result"),
    ("table.column.time", "Time(s)"),
    ("table.column.attempts", "Attempts"),
    ("table.column.note", "Note"),
]


class StepsModel(QAbstractTableModel):
    def __init__(self, steps, translator: Optional[Callable] = None):
        super().__init__()
        self._translator = translator
        self.rows = []
        for s in steps:
            self.rows.append({
                "sid": s["sid"],
                "name": s["name"],
                "status": StepState.NOT_RUN,
                "ignore": bool(s.get("ignore", False)),
                "time": 0,
                "attempts": 0,
                "note": "",
            })

    def rowCount(self, parent=None):
        return len(self.rows)

    def columnCount(self, parent=None):
        return len(COLUMNS)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole: return QVariant()
        if orientation == Qt.Horizontal:
            key, default = COLUMNS[section]
            return self._tr(key, default)
        return section+1

    def data(self, index: QModelIndex, role=Qt.DisplayRole):
        if not index.isValid(): return QVariant()
        row = self.rows[index.row()]
        col = index.column()
        if role == Qt.DisplayRole:
            if col == 0: return row["name"]
            if col == 1: return row["status"]
            if col == 2: return self._tr("table.ignore.yes", "Y") if row.get("ignore") else self._tr("table.ignore.no", "N")
            if col == 3: return row["time"]
            if col == 4: return row["attempts"]
            if col == 5: return row["note"]
        if col == 1:
            color_map = {
                StepState.NOT_RUN: QColor("#808080"),
                StepState.RUNNING: QColor("#1976d2"),
                StepState.PASS: QColor("#2e7d32"),
                StepState.FAIL: QColor("#c62828"),
                StepState.TIMEOUT: QColor("#c62828"),
                StepState.SKIPPED: QColor("#f9a825"),
                StepState.IGNORED: QColor("#9e9e9e"),
            }
            if role == Qt.ForegroundRole:
                return color_map.get(row["status"], QColor("#000000"))
            if role == Qt.BackgroundRole:
                bg_map = {
                    StepState.NOT_RUN: QBrush(QColor("#f5f5f5")),
                    StepState.RUNNING: QBrush(QColor("#e3f2fd")),
                    StepState.PASS: QBrush(QColor("#c8e6c9")),
                    StepState.FAIL: QBrush(QColor("#ffcdd2")),
                    StepState.TIMEOUT: QBrush(QColor("#ffcdd2")),
                    StepState.SKIPPED: QBrush(QColor("#fff9c4")),
                    StepState.IGNORED: QBrush(QColor("#eeeeee")),
                }
                return bg_map.get(row["status"], QBrush(QColor("#ffffff")))
        return QVariant()

    def set_status(self, sid, status, time_s=None, attempts=None, note=None):
        for i, r in enumerate(self.rows):
            if r["sid"] == sid:
                r["status"] = status
                if time_s is not None: r["time"] = time_s
                if attempts is not None: r["attempts"] = attempts
                if note is not None: r["note"] = note
                tl = self.index(i, 0); br = self.index(i, self.columnCount()-1)
                self.dataChanged.emit(tl, br, [])
                break

    def set_ignore(self, sid, ignore):
        for i, r in enumerate(self.rows):
            if r["sid"] == sid:
                r["ignore"] = bool(ignore)
                idx = self.index(i, 2)
                self.dataChanged.emit(idx, idx, [])
                break

    def reset_states(self):
        if not self.rows:
            return
        for r in self.rows:
            r["status"] = StepState.NOT_RUN
            r["time"] = 0
            r["attempts"] = 0
            r["note"] = ""
        tl = self.index(0, 0)
        br = self.index(len(self.rows)-1, self.columnCount()-1)
        self.dataChanged.emit(tl, br, [])

    def apply_flags(self, flags):
        if not flags:
            return
        for r in self.rows:
            sid = r["sid"]
            flag = flags.get(sid)
            if not flag:
                continue
            status = getattr(flag, "status", None)
            if status is None and isinstance(flag, dict):
                status = flag.get("status")
            note = getattr(flag, "note", "") if hasattr(flag, "note") else ""
            if isinstance(flag, dict):
                note = flag.get("note", note)
            if not status:
                continue
            valid_statuses = {
                StepState.NOT_RUN,
                StepState.RUNNING,
                StepState.PASS,
                StepState.FAIL,
                StepState.TIMEOUT,
                StepState.SKIPPED,
                StepState.IGNORED,
            }
            if status not in valid_statuses:
                status = StepState.NOT_RUN
            self.set_status(sid, status, note=note)

    def _tr(self, key: str, default: str) -> str:
        if self._translator:
            return self._translator(key, default=default)
        return default
