"""
把按鈕 enable/disable 與 status label 顏色的規則集中在這裡。

原本 MainWindow.refresh_ui_state 直接碰七八個 widget 實例；現在把
「需要動哪些 widget」交給呼叫端透過 dataclass 傳進來，這層只負責決策。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
from typing import Optional

from PyQt5.QtWidgets import QAbstractButton, QLabel, QSpinBox, QTableView

from app.ui.theme import OVERALL_STATUS_BG, OVERALL_STATUS_BG_DEFAULT


@dataclass
class UiWidgets:
    """UiStateManager 需要控制的所有 widget。由 MainWindow 在載入 .ui 後填入。"""

    status_label: QLabel
    table: QTableView
    stop_button: QAbstractButton
    reset_flags_button: QAbstractButton
    stress_spin: QSpinBox
    stress_label: QLabel
    debug_buttons: Iterable[QAbstractButton]


class UiStateManager:
    def __init__(self, widgets: UiWidgets):
        self.widgets = widgets
        self.debug_enabled: bool = False
        self.run_locked: bool = False
        self.has_finished_snapshot: bool = False
        self.controller_running_getter = lambda: False  # 由 MainWindow 注入

    # ---- 狀態變更入口 --------------------------------------------------------

    def set_debug_enabled(self, enabled: bool) -> None:
        self.debug_enabled = enabled
        self.refresh()

    def lock_run(self) -> None:
        self.run_locked = True
        self.refresh()

    def unlock_run(self) -> None:
        self.run_locked = False
        self.refresh()

    def set_has_finished_snapshot(self, has_snapshot: bool) -> None:
        self.has_finished_snapshot = has_snapshot
        self.refresh()

    def on_run_state_changed(self, running: bool) -> None:
        if running:
            self.lock_run()
        else:
            self.unlock_run()

    # ---- 套用到 widget -------------------------------------------------------

    def refresh(self) -> None:
        w = self.widgets
        debug_controls_enabled = self.debug_enabled and not self.run_locked
        for btn in w.debug_buttons:
            btn.setEnabled(debug_controls_enabled)

        w.table.setEnabled(not self.controller_running_getter())
        w.stress_spin.setEnabled(debug_controls_enabled)
        w.stress_spin.setVisible(self.debug_enabled)
        w.stress_label.setVisible(self.debug_enabled)
        w.stop_button.setEnabled(self.run_locked)
        w.reset_flags_button.setEnabled((not self.run_locked) and self.has_finished_snapshot)

        if not self.debug_enabled and not self.run_locked:
            w.stress_spin.setValue(1)

    # ---- status label 顏色 ---------------------------------------------------

    def apply_status(self, status: str, display_text: Optional[str] = None) -> None:
        self.widgets.status_label.setText(display_text or status)
        color = OVERALL_STATUS_BG.get(status, OVERALL_STATUS_BG_DEFAULT)
        self.widgets.status_label.setStyleSheet(
            f"font-size:60px; font-weight:bold; background:{color}; padding:24px;"
        )
