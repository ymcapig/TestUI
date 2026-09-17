"""
可重用的 Qt 元件（widgets / delegates / threads）。

放在這裡的條件：
- 繼承 Qt 基底類別（QThread、QStyledItemDelegate、QWidget...）
- 不知道本專案的業務概念（station.ini、SN、debug mode...）
- 理論上能被別的 Qt 專案抄走就用
"""

from __future__ import annotations

from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtWidgets import QStyledItemDelegate, QLineEdit
from PyQt5.QtGui import QDoubleValidator


from core.flow_runner import FlowRunner


class RunnerThread(QThread):
    """
    把 FlowRunner 放進 QThread，並把它的 callback 轉成 Qt signal。

    因果：FlowRunner.on_xxx (callback) → self.xxx (signal) → UI slot
    """

    status_changed = pyqtSignal(str)
    step_started = pyqtSignal(str, int)
    step_finished = pyqtSignal(str, dict)
    log_line = pyqtSignal(str)

    def __init__(self, runner: FlowRunner | None = None):
        super().__init__()
        self.runner: FlowRunner | None = None
        if runner is not None:
            self.set_runner(runner)

    def set_runner(self, runner: FlowRunner) -> None:
        if self.isRunning():
            raise RuntimeError("Cannot set runner while thread is running")

        self.runner = runner
        runner.on_status_changed = lambda s: self.status_changed.emit(s)
        runner.on_step_started = lambda sid, attempt: self.step_started.emit(sid, attempt)
        runner.on_step_finished = lambda sid, res: self.step_finished.emit(
            sid,
            {
                "state": res.state,
                "duration_s": res.duration_s,
                "attempt": res.attempt,
                "note": res.note,
            },
        )
        runner.on_log_line = lambda line: self.log_line.emit(line)

    def run(self) -> None:
        if self.runner:
            self.runner.run_all()

class NumericDelegate(QStyledItemDelegate):
    """限制 cell 編輯器只能輸入數字（用於 limit 欄位）。"""
    
    def createEditor(self, parent, option, index):
        editor = QLineEdit(parent)
        validator = QDoubleValidator(-9999.99, 9999.99, 2, editor)
        editor.setValidator(validator)
        return editor