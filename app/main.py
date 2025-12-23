import sys
from pathlib import Path

def resource_path(*parts: str) -> Path:
    """Return absolute path to resource, works for dev and PyInstaller builds."""
    # 這裡維持您原本的邏輯，定位專案根目錄
    if getattr(sys, 'frozen', False):
        base = Path(sys._MEIPASS)
    else:
        base = Path(__file__).resolve().parents[1]
    
    candidate = base.joinpath(*parts)
    if candidate.exists():
        return candidate
    return Path.cwd().joinpath(*parts)

# Ensure project root on sys.path for module imports
sys.path.append(str(resource_path()))

# --- [修改 1] 導入 interactive_launcher ---
# 因為有這行 import，PyInstaller 就會知道要把 interactive_launcher.py 包進去
import interactive_launcher 
from app.ui.main_window import launch

if __name__ == "__main__":
    # --- [修改 2] 偵測 --interactive 參數 ---
    # 這是為了配合 flow_runner.py 在 frozen 模式下的呼叫方式:
    # cmd = f'"{sys.executable}" --interactive "{cmd}"'
    
    if "--interactive" in sys.argv:
        # 移除旗標，避免干擾 launcher 的參數解析
        sys.argv.remove("--interactive")
        
        # 直接執行互動邏輯，不啟動 UI
        interactive_launcher.run_interactive()
        
    else:
        # 正常啟動 UI
        cfg = resource_path("config", "station.ini")
        launch(cfg)
