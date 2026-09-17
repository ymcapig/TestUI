"""
跨平台的「在檔案管理員裡開啟資料夾」函式。
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def open_folder_in_explorer(folder: Path) -> None:
    folder_str = str(folder)
    if sys.platform.startswith("win"):
        # os.startfile 只有 Windows 有，但原本的 main_window 就是這樣用的
        os.startfile(folder_str)  # type: ignore[attr-defined]
        return
    if sys.platform == "darwin":
        subprocess.Popen(["open", folder_str])
        return
    subprocess.Popen(["xdg-open", folder_str])
