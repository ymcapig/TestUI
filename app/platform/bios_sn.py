"""從 Windows BIOS 讀取機器序號。"""
import subprocess
from typing import Optional

# 1
def read_bios_sn_doc() -> str:
    try:
        with open(r"C:\Diag\sn.txt", "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""

# 2
def read_bios_sn_win32(timeout: float = 5.0) -> Optional[str]:
    """
    透過 PowerShell 呼叫 Get-CimInstance 讀取 BIOS 序號。
    成功回 SN 字串、失敗回 None。
    """
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "(Get-CimInstance -ClassName Win32_BIOS).SerialNumber"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        )
        sn = (result.stdout or "").strip()
        return sn or None
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return None
    
# 3 
def read_baseboard_sn_win32(timeout: float = 5.0) -> Optional[str]:
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "(Get-CimInstance -ClassName Win32_BaseBoard).SerialNumber"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        )
        sn = (result.stdout or "").strip()
        return sn or None
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return None

def read_bios_sn(mode: int | str) -> str | None:
    safe_mode = str(mode).strip()

    if safe_mode == "1":
        return read_bios_sn_doc()
    elif safe_mode == "2":
        return read_bios_sn_win32()
    elif safe_mode == "3":
        return read_baseboard_sn_win32()
    else:
        # 預防設定檔填了 4 或是亂打字
        print(f"[ERROR] : {mode}")
        return None