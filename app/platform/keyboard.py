"""
強制 QLineEdit 使用英文鍵盤輸入。Windows 會實際切鍵盤配置，
其他平台則只會設定 Qt 的 input method hints。
"""

from __future__ import annotations

import ctypes
import sys
from typing import Set

from PyQt5.QtCore import QEvent, QObject, Qt
from PyQt5.QtWidgets import QLineEdit


class LatinInputHelper(QObject):
    """
    把需要英文輸入的 QLineEdit 交給這個 helper：
      helper.attach(line_edit)

    它會：
    1. 幫 line_edit 設上 Qt input method hints（跨平台）
    2. 裝上 event filter，當 widget 取得焦點時，在 Windows 下切換成美式鍵盤
    """

    def __init__(self, parent: QObject | None = None):
        super().__init__(parent)
        self._tracked: Set[QLineEdit] = set()

    def attach(self, line_edit: QLineEdit | None) -> None:
        if line_edit is None:
            return
        hints = Qt.ImhPreferLatin | Qt.ImhNoPredictiveText | Qt.ImhNoAutoUppercase
        line_edit.setInputMethodHints(hints)
        if line_edit not in self._tracked:
            line_edit.installEventFilter(self)
            self._tracked.add(line_edit)
        self._force_english_keyboard()

    def eventFilter(self, obj, event):  # noqa: N802 (Qt naming)
        if obj in self._tracked and event.type() == QEvent.FocusIn:
            self._force_english_keyboard()
        return super().eventFilter(obj, event)

    @staticmethod
    def _force_english_keyboard() -> None:
        if not sys.platform.startswith("win"):
            return
        try:
            user32 = ctypes.windll.user32
            hkl = user32.LoadKeyboardLayoutW("00000409", 1)
            if hkl:
                user32.ActivateKeyboardLayout(hkl, 0)
        except Exception:
            # 平台 API 失敗不應該擋住整個 UI
            pass
