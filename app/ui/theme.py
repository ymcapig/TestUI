"""
集中管理 UI 的顏色主題。

原本 StepsModel 跟 UiStateManager 各自有一份顏色表：
- StepsModel 的 STATUS_COLORS / STATUS_BG：對應 StepState(PASS/FAIL/...)
- UiStateManager 的 STATUS_COLORS：對應 Status(READY/RUNNING/...)

兩者語意不同（step 級別 vs. 整體狀態），但都是「顏色決策」，
放在同一個檔方便日後調色、換主題。
"""

from __future__ import annotations

from PyQt5.QtGui import QBrush, QColor

from core.events import Status, StepState


# 整體狀態（左上大 label）的底色
OVERALL_STATUS_BG = {
    Status.READY: "#e0e0e0",
    Status.RUNNING: "#90caf9",
    Status.PAUSED: "#ffe082",
    Status.STOPPED: "#ef9a9a",
    Status.PASS: "#a5d6a7",
    Status.FAIL: "#ef9a9a",
}

# 表格中每個 step 的狀態文字顏色
STEP_STATUS_FG = {
    StepState.NOT_RUN: QColor("#808080"),
    StepState.RUNNING: QColor("#1976d2"),
    StepState.PASS: QColor("#2e7d32"),
    StepState.FAIL: QColor("#c62828"),
    StepState.TIMEOUT: QColor("#c62828"),
    StepState.SKIPPED: QColor("#f9a825"),
    StepState.IGNORED: QColor("#9e9e9e"),
}

# 表格中每個 step 的狀態格底色
STEP_STATUS_BG = {
    StepState.NOT_RUN: QBrush(QColor("#f5f5f5")),
    StepState.RUNNING: QBrush(QColor("#e3f2fd")),
    StepState.PASS: QBrush(QColor("#c8e6c9")),
    StepState.FAIL: QBrush(QColor("#ffcdd2")),
    StepState.TIMEOUT: QBrush(QColor("#ffcdd2")),
    StepState.SKIPPED: QBrush(QColor("#fff9c4")),
    StepState.IGNORED: QBrush(QColor("#eeeeee")),
}

# 狀態未知時的預設值（避免 View 拿到 None）
STEP_STATUS_FG_DEFAULT = QColor("#000000")
STEP_STATUS_BG_DEFAULT = QBrush(QColor("#ffffff"))
OVERALL_STATUS_BG_DEFAULT = "#e0e0e0"
