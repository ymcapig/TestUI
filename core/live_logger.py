"""
即時日誌寫入 (LiveLogger)

專職做一件事:把測項的 stdout/stderr「一有輸出就即時 append」到檔案,
讓程式突然關閉/reboot 時,已產生的 log 不會遺失。

寫入兩個檔:
  - step.log  : 該測項自己的即時 log
  - full_log  : 所有測項彙整的即時 log

不負責:結果摘要(State/ExitCode)、report json、log 合併等——那些留給 flow_runner 收尾處理。
"""
from pathlib import Path


class LiveLogger:
    def __init__(self, full_log_path: Path):
        """full_log_path: 這次執行的彙整 log 檔(整個流程共用一個)。"""
        self.full_log_path = Path(full_log_path)
        try:
            self.full_log_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    def _append(self, path: Path, text: str) -> None:
        """把 text 即時 append 到指定檔案,失敗不影響流程。"""
        try:
            with Path(path).open("a", encoding="utf-8-sig") as f:
                f.write(text)
        except Exception:
            pass

    def step_header(self, step_log_path: Path, step_name: str, attempt: int) -> None:
        """測項每次 attempt 開始時,寫一行可辨識的標頭到 step.log 與 full_log。"""
        header = f"\n=== Step: {step_name} (attempt {attempt}) ===\n"
        self._append(step_log_path, header)
        self._append(self.full_log_path, header)

    def write_line(self, step_log_path: Path, prefix: str, text: str) -> None:
        """每讀到一行 std,先寫 step.log,再寫 full_log。"""
        line = f"[{prefix}] {text}"
        self._append(step_log_path, line)          # 先 step
        self._append(self.full_log_path, line)     # 再 full
