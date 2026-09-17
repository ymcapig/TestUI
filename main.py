import sys
import argparse
from pathlib import Path
from core.interactive_launcher import run_interactive
from app.ui.main_window import launch

def resource_path(*parts: str) -> Path:
    """
    路徑搜尋策略：
    1. [Frozen/打包模式] 優先檢查 EXE 旁邊是否有該檔案 (外部 Config/i18n)。
    2. [Frozen/打包模式] 如果 EXE 旁邊沒有，才去找打包在內部的 (內部資源)。
    3. [Dev/開發模式] 回傳專案根目錄下的路徑。
    """
    if getattr(sys, 'frozen', False):
        # --- 策略 1: 找 EXE 旁邊 (外部檔案) ---
        # sys.executable 是 FactoryTestTool.exe 的完整路徑
        # .parent 就是 EXE 所在的資料夾 (例如 dist 資料夾)
        exe_dir = Path(sys.executable).parent
        external_path = exe_dir.joinpath(*parts)
        
        if external_path.exists():
            return external_path
            
        # --- 策略 2: 找打包內容 (內部檔案) ---
        # 如果您某些圖片或 ICON 還是想打包在裡面，這個邏輯保留是好的
        bundled_path = Path(sys._MEIPASS).joinpath(*parts)
        if bundled_path.exists():
            return bundled_path
            
    else:
        # --- 策略 3: 開發模式 ---
        # 假設 main.py 在 app/ 資料夾，專案根目錄就是上兩層
        base = Path(__file__).resolve().parent
        dev_path = base.joinpath(*parts)
        if dev_path.exists():
            return dev_path

    # 最後的 fallback (通常用不到，但以防萬一)
    return Path.cwd().joinpath(*parts)

# Ensure project root on sys.path
sys.path.append(str(resource_path()))

if __name__ == "__main__":
    if "--interactive" in sys.argv:
        sys.argv.remove("--interactive")
        run_interactive()
    # else:
    #     # 以下參數方是進入 debug mode
    #     force_debug = False
    #     if "--debug" in sys.argv:
    #         sys.argv.remove("--debug")
    #         force_debug = True
    else:
        parser = argparse.ArgumentParser(
            prog="FactoryTestTool",
            description="NB TestUI - 工站測試介面",
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="強制進入 Debug 模式（略過密碼）",
        )
        # parse_known_args 讓 Qt 等框架的參數能順利通過
        args, _ = parser.parse_known_args()

        # 這裡傳入的路徑，會透過上面的 resource_path 找到 EXE 旁邊的 station.ini
        cfg = resource_path("config", "station.ini")
        # launch(cfg, force_debug=force_debug)
        launch(cfg, force_debug=args.debug)