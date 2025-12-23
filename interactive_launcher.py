import sys
import subprocess
import os

def run_interactive():
    # 1. 取得要執行的指令 (從命令列參數)
    # 例如: python interactive_launcher.py my_tool.exe arg1
    if len(sys.argv) < 2:
        print("Error: No command provided.")
        sys.exit(1)

    target_cmd = sys.argv[1:]  # 取得真正的指令與參數
    
    print(f"Launching interactive window for: {' '.join(target_cmd)}")

    try:
        # 2. 設定 Windows 旗標以開啟新視窗
        # CREATE_NEW_CONSOLE = 0x00000010
        creation_flags = subprocess.CREATE_NEW_CONSOLE

        # 3. 啟動子程 (這會彈出一個黑底白字的視窗)
        # 注意：這裡不設定 stdout/stderr/stdin 為 PIPE，讓它直接使用新視窗的 I/O
        process = subprocess.Popen(
            target_cmd,
            creationflags=creation_flags,
            shell=True
        )

        # 4. 等待該視窗關閉 (OP 操作結束)
        process.wait()

        # 5. 取得 Return Code 並回傳給 TestUI
        # 如果工具成功 (exit 0)，TestUI 就會判斷 PASS
        sys.exit(process.returncode)

    except Exception as e:
        print(f"Failed to launch interactive process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_interactive()
