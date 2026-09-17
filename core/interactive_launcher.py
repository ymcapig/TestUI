import sys
import subprocess
import os
import traceback
import datetime
import os
import re
from pathlib import Path

def write_log(msg):
    print(msg, flush=True)
    
    try:
        with open("debug_launcher.log", "a", encoding="utf-8") as f:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{timestamp}] {msg}\n")
    except:
        pass

def preflight_check(target_cmd: str, base_dir: str | None = None):
    """
    事前檢查：利用 Regex 解析 cmd 中的 cd 與 call，
    若發現欲執行的外部檔案不存在，則印出錯誤並中斷程式。
    """
    try:
        # 預設起點為 flow_runner 傳進來的工作目錄 (workdir)
        if base_dir:
            current_folder = Path(base_dir)
        else:
            current_folder = Path(os.getcwd())
        
        # 1. 抓取 cd 後面的路徑
        m_cd = re.search(r'cd\s+(?:/d\s+)?(.+?)\s*&&', target_cmd, flags=re.IGNORECASE)
        if m_cd:
            raw_path = m_cd.group(1).strip().strip('"').strip("'")
            cd_folder = Path(raw_path)
            # 判斷是絕對路徑還是相對路徑，並拼湊起來
            if not cd_folder.is_absolute():
                current_folder = (current_folder / cd_folder).resolve()
            else:
                current_folder = cd_folder.resolve()
                
        # 2. 抓取 call 後面的檔案名稱
        m_call = re.search(r'call\s+([^\s&]+)', target_cmd, flags=re.IGNORECASE)
        if m_call:
            target_file = m_call.group(1).strip().strip('"').strip("'")
            full_target_path = current_folder / target_file
            
            # 事前攔截：檔案不存在就直接報錯，提早結束
            if not full_target_path.exists():
                write_log(f"\n[FATAL ERROR] 執行失敗！事前檢查未通過。")
                write_log(f"[FATAL ERROR] 原因：找不到外部檔案 '{target_file}'")
                write_log(f"[FATAL ERROR] 預期絕對路徑：{full_target_path}")
                write_log(f"[FATAL ERROR] 請檢查 station.ini 的 cmd 路徑設定。\n")
                sys.exit(1)
                
    except SystemExit:
        raise # 遇到 sys.exit(1) 必須往上拋出，確實中斷
    except Exception as e:
        write_log(f"[WARNING] 路徑解析發生例外 (忽略並繼續執行): {e}")

def run_interactive():
    write_log("========== Launcher Started ==========")
    write_log(f"Original Sys.argv: {sys.argv}")
    
    if len(sys.argv) < 2:
        write_log("Error: No command provided.")
        sys.exit(1)

    # 先抽出 --base 參數（flow_runner 傳進來的 workdir）
    args = sys.argv[1:]
    base_dir = None
    if "--base" in args:
        i = args.index("--base")
        try:
            base_dir = args[i + 1]
            del args[i:i + 2]   # 把 --base 和它的值一起移除
        except IndexError:
            base_dir = None

    # 1. 取得完整指令字串
    target_cmd = " ".join(args)
    write_log(f"Target Command: {target_cmd}")
    write_log(f"Base Dir: {base_dir}")

    # 呼叫事前檢查函數
    preflight_check(target_cmd, base_dir)

    try:
        # 2. 設定 Windows 旗標 (新視窗)
        creation_flags = subprocess.CREATE_NEW_CONSOLE
        
        # 3. 強制顯示視窗 (SW_SHOWNORMAL = 1, SW_SHOW = 5)
        # 有時候 SW_SHOW (5) 會被忽略，改用 SW_SHOWNORMAL (1) 比較穩
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 1 

        write_log("Attempting subprocess.Popen (Direct cmd.exe call)...")
        
        # 4. 直接呼叫 cmd.exe，並將 shell=False
        # 這樣可以避免中間層干擾，確保視窗設定生效
        final_args = ["cmd.exe", "/c", target_cmd]

        # 加入 stderr=subprocess.PIPE
        process = subprocess.Popen(
            final_args,
            creationflags=creation_flags,
            shell=False,   # <--- 改為 False
            startupinfo=startupinfo,
            # stderr=subprocess.PIPE,
            close_fds=True
        )
        
        write_log(f"Process started successfully. PID: {process.pid}")
        write_log("Waiting for process to finish...")
        
        process.wait()
        write_log(f"Process finished with Return Code: {process.returncode}")
        sys.exit(process.returncode)

        # # 攔截 CMD 吐出的錯誤訊息
        # _, stderr_data = process.communicate()
        # rc = process.returncode
        
        # # 如果有錯誤訊息，就解碼並印出來
        # if stderr_data:
        #     try:
        #         err_msg = stderr_data.decode('utf-8')
        #     except UnicodeDecodeError:
        #         # 預設編碼 Big5/mbcs
        #         err_msg = stderr_data.decode('mbcs', errors='ignore')
                
        #     if err_msg.strip():
        #         write_log(f"\n[CMD 錯誤訊息：]\n{err_msg.strip()}\n")

        # write_log(f"Process finished with Return Code: {rc}")
        # sys.exit(rc)

    except Exception:
        error_msg = traceback.format_exc()
        write_log(f"CRITICAL EXCEPTION:\n{error_msg}")
        sys.exit(1)

if __name__ == "__main__":
    run_interactive()