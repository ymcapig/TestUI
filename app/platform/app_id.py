"""
Windows 專用：設定 AppUserModelID，讓工作列 icon 能正確分組。
非 Windows 平台上呼叫會是 no-op。
"""

from __future__ import annotations

import ctypes
import sys


def set_app_user_model_id(app_id: str) -> None:
    if not sys.platform.startswith("win"):
        return
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(app_id)
    except Exception:
        pass
