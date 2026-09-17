"""
啟動流程的編排：決定要走 debug mode 還是自動跑一輪，以及 SN 的取得。

把這段抽出來主要是為了讓 MainWindow 的 __init__ 後半段（_handle_startup_flow、
_auto_start_sequence）看起來不那麼像「在 UI 類別裡做流程控制」。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from PyQt5.QtWidgets import (
    QInputDialog,
    QLineEdit,
    QMessageBox,
    QWidget,
)

from app.platform.keyboard import LatinInputHelper


class StartupCoordinator:
    def __init__(
        self,
        parent: QWidget,
        translator: Callable[..., str],
        latin_input_helper: LatinInputHelper,
    ):
        self.parent = parent
        self._ = translator
        self.latin_input_helper = latin_input_helper

    # ---- debug title --------------------------------------------------------

    def tag_debug_title(self) -> None:
        title = self.parent.windowTitle()
        if "[DEBUG]" not in title:
            self.parent.setWindowTitle(title + " [DEBUG]")

    def show_debug_mode_notice(self) -> None:
        QMessageBox.information(
            self.parent,
            "Debug Mode",
            "目前為 Debug Mode。",
        )

    # ---- SN 輸入 (目前沒用，不再需要手動輸入 SN 了)------------------------------------------------------------
    def prompt_sn(self) -> Optional[str]:
        """
        依 config.require_sn 決定是否跳出輸入框。使用者取消就回 None，
        否則回傳非空字串。
        """
        return self._prompt_text(
            field_key="field.dut_sn",
            title_key="dialog.prompt.dut_sn.title",
        )

    def _prompt_text(self, field_key: str, title_key: str) -> Optional[str]:
        field_name = self._(field_key, default=field_key)
        label = self._(
            "dialog.prompt.label",
            default="Please enter {field}:",
            field=field_name,
        )
        while True:
            dlg = QInputDialog(self.parent)
            dlg.setWindowTitle(self._(title_key, default=title_key))
            dlg.setLabelText(label)
            dlg.setTextValue("")
            line_edit = dlg.findChild(QLineEdit)
            if line_edit is not None:
                self.latin_input_helper.attach(line_edit)
            if not dlg.exec_():
                return None
            value = (dlg.textValue() or "").strip()
            if value:
                return value
            QMessageBox.warning(
                self.parent,
                self._("dialog.input_error.title", default="Input Error"),
                self._(
                    "dialog.input_error.message",
                    default="{field} cannot be empty.",
                    field=field_name,
                ),
            )
