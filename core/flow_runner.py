import subprocess, threading, time, os, json, re
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from .result_evaluator import ResultEvaluator
from .events import Status, StepState
from .state_store import StepStateStore, StepFlag
from .paths import resolve_workdir
from .config_service import load_config, STEP_DEFAULTS
from .live_logger import LiveLogger


def _get_ui_version() -> str:
    """
    從 version.txt 抽出 FileVersion 字串。

    打包後（PyInstaller --add-data）：版本檔在 sys._MEIPASS 解壓區
    開發環境：版本檔在專案根目錄
    讀檔失敗：回 "unknown"，不影響執行
    """
    base = getattr(sys, "_MEIPASS", None)
    if base:
        version_file = Path(base) / "version.txt"
    else:
        # core/flow_runner.py → 上一層是 core/ → 再上一層是專案根目錄
        version_file = Path(__file__).parent.parent / "version.txt"
    try:
        text = version_file.read_text(encoding="utf-8")
        match = re.search(r"StringStruct\(\s*'FileVersion'\s*,\s*'([^']+)'\s*\)", text)
        if match:
            return match.group(1)
    except Exception:
        pass
    return "unknown"

@dataclass
class Step:
    order: int
    name: str
    type: str
    cmd: str
    workdir: str = ""
    timeout: int = 0
    retry: int = 0
    retry_interval_sec: int = 0
    ignore_result: bool = False
    pass_by: str = ""
    kill_tree: bool = True
    stdout_encoding: str = "utf-8"
    artifacts: str = ""
    pending_exit_codes: List[int] = field(default_factory=list)
    extra_config: Dict[str, Any] = field(default_factory=dict) # 2026/2/2 新增(傳參數)(目前沒用)

@dataclass
class StepResult:
    state: str = StepState.NOT_RUN
    exit_code: Optional[int]=None
    matched_rule: str = ""
    duration_s: float = 0.0
    attempt: int = 0
    manual: bool = False
    note: str = ""
    log: str = ""

class FlowRunner:
    def __init__(self, config_path: Path, sn: str, debug_enabled: bool=False):
        self.config_path = Path(config_path)
        self.sn = sn
        self.debug_enabled = debug_enabled
        self.config_dir = self.config_path.resolve().parent
        self.project_root = self.config_dir.parent if self.config_dir.name.lower() == "config" else self.config_dir

        self.global_status = Status.READY
        self.stop_requested = False
        self.pause_requested = False

        self.cfg = load_config(self.config_path)

        self.station_meta = {
            "station_id": self.cfg.get("meta", "station_id", fallback="").strip(),
            "line_id": self.cfg.get("meta", "line_id", fallback="").strip(),
            "model": self.cfg.get("meta", "model", fallback="").strip(),
        }
        self.supports_reboot = True

        self.run_mode = self.cfg.get("run","run_mode", fallback="stop_on_fail")
        self.default_timeout = self.cfg.getint("run","default_timeout_sec", fallback=60)
        self.report_emit_on = [x.strip() for x in self.cfg.get("run","report.emit_on", fallback="stopped_on_fail").split(",")]
        run_workdir_str = self.cfg.get("run","workdir", fallback=".")
        self.workdir_global = self._resolve_workdir(Path(run_workdir_str))
        # FAIL 時要呼叫的工具(bat/exe/py),沒設就只建 flag 不呼叫
        self.fail_tool = self.cfg.get("run", "fail_tool", fallback="").strip()

        # Prepare run dir (resume if unfinished run exists)
        self._latest_info_path = None
        self._resume_existing = False
        self.run_dir, self.ts = self._prepare_run_directory()

        # 即時日誌:每有一行 std 就 append，程式突然關閉也不丟失
        self.live_logger = LiveLogger(self.run_dir / "reports" / f"full_log_{self.sn}_{self.ts}.txt")

        # Load steps
        self.steps: List[Step] = []
        for sec in self.cfg.sections():
            if sec.startswith("step."):
                order_name = sec[5:]
                try:
                    order_str, name = order_name.split("_",1)
                    order = int(order_str)
                except Exception:
                    continue
                s = self.cfg[sec]
                step = Step(
                    order=order,
                    name=name,
                    type=s.get("type", STEP_DEFAULTS["type"]),
                    cmd=s.get("cmd",""),
                    workdir=s.get("workdir",""),
                    timeout=s.getint("timeout", self.default_timeout),
                    retry=s.getint("retry", STEP_DEFAULTS["retry"]),
                    retry_interval_sec=s.getint("retry_interval_sec", STEP_DEFAULTS["retry_interval_sec"]),
                    ignore_result=s.getboolean("ignore_result", STEP_DEFAULTS["ignore_result"]),
                    pass_by=s.get("pass_by", STEP_DEFAULTS["pass_by"]),
                    kill_tree=s.getboolean("kill_tree", STEP_DEFAULTS["kill_tree"]),
                    stdout_encoding=s.get("stdout_encoding", STEP_DEFAULTS["stdout_encoding"]),
                    artifacts=s.get("artifacts",""),
                    pending_exit_codes=self._parse_pending_exit_codes(s.get("pending_exit_codes", "")),
                    extra_config=dict(s), # 2026/2/2 新增(傳參數)(目前沒用)
                )
                self.steps.append(step)
        self.steps.sort(key=lambda x: x.order)

        self.state_store = StepStateStore(self.run_dir)
        self.step_flags: Dict[str, StepFlag] = self.state_store.load_all()

        self.steps_manifest = []
        for s in self.steps:
            sid = self._sid(s)
            flag = self.step_flags.get(sid)
            self.steps_manifest.append({
                "id": sid,
                "order": s.order,
                "name": s.name,
                "type": s.type,
                "cmd": s.cmd,
                "workdir": s.workdir,
                "timeout": s.timeout,
                "retry": s.retry,
                "retry_interval_sec": s.retry_interval_sec,
                "ignore_result": s.ignore_result,
                "pass_by": s.pass_by,
                "kill_tree": s.kill_tree,
                "stdout_encoding": s.stdout_encoding,
                "artifacts": s.artifacts,
                "pending_exit_codes": s.pending_exit_codes,
                "flag_status": flag.status if flag else StepState.NOT_RUN,
                "flag_path": str(self.state_store.flag_path(sid)),
                "flag_updated_at": flag.updated_at if flag else "",
            })

        # runtime results
        self.results: Dict[str, StepResult] = {}
        self.current_proc = None # TEST: 紀錄目前跑的 step
        for s in self.steps:
            sid = self._sid(s)
            initial = self.step_flags.get(sid)
            result = StepResult()
            if initial:
                result.state = initial.status
                result.note = initial.note
            self.results[sid] = result

        # placeholders
        self.placeholders = {
            "SN": self.sn,
            "RUN_ID": self.ts,
            "RUN_DIR": str(self.run_dir.resolve()),
            "TOOLS_DIR": str((self.project_root/"tools").resolve()),
        }
        print(self.placeholders)
        self.resumed = self._resume_existing

        # callbacks (to be wired by UI)
        self.on_status_changed = lambda status: None # 執行到這行就會觸發 pyqtSignal
        self.on_step_started = lambda sid, attempt: None # TEST: 新增 attempt
        self.on_step_finished = lambda sid, result: None
        self.on_log_line = lambda text: None

    def _sid(self, s: Step) -> str:
        return f"{s.order}_{s.name}"

    def _expand(self, text: str, extra: Optional[Dict[str, str]] = None) -> str:
        if not text: return text
        extra_map = extra or {}
        def repl(m):
            key = m.group(1)
            if key.startswith("ENV:"):
                return os.environ.get(key[4:], "")
            if key in extra_map:
                return extra_map[key]
            return self.placeholders.get(key, "{"+key+"}")
        return re.sub(r"\{([A-Z0-9_:\-]+)\}", repl, text)

    def _resolve_workdir(self, workdir: Path) -> Path:
        return resolve_workdir(str(workdir), self.project_root)

    def _prepare_run_directory(self) :
        sn_dir = (self.project_root / "runs" / self.sn)
        sn_dir.mkdir(parents=True, exist_ok=True)
        latest_path = sn_dir / "latest_run.json"
        resume = False
        run_id = None
        if latest_path.exists():
            try:
                info = json.loads(latest_path.read_text(encoding="utf-8-sig"))
                stored_sn = info.get("sn")
                candidate = info.get("run_id")
                status = (info.get("status") or "").lower()
                if candidate and (stored_sn in (None, "", self.sn)) and status in {"running", "pending", "finished_fail", "stopped", "aborted"}:
                    candidate_dir = sn_dir / candidate
                    if candidate_dir.exists():
                        run_id = candidate
                        resume = True
            except Exception:
                run_id = None
        if not run_id:
            run_id = time.strftime("%Y%m%d_%H%M%S")
        run_dir = sn_dir / run_id
        (run_dir / "steps").mkdir(parents=True, exist_ok=True)
        (run_dir / "reports").mkdir(parents=True, exist_ok=True)
        self._latest_info_path = latest_path
        self._resume_existing = resume
        self._write_latest_info(run_id, "running")
        return run_dir, run_id

    def _write_latest_info(self, run_id: str, status: str) -> None:
        sn_dir = (self.project_root / "runs" / self.sn)
        sn_dir.mkdir(parents=True, exist_ok=True)
        if self._latest_info_path is None:
            self._latest_info_path = sn_dir / "latest_run.json"
        payload = {
            "sn": self.sn,
            "run_id": run_id,
            "status": status,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        tmp_path = self._latest_info_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8-sig")

        # TEST: 避免 "PermissionError: [WinError 5] 存取被拒" 的嘗試
        for i in range(10): # retry 10次
            try:
                tmp_path.replace(self._latest_info_path)
                break
            except PermissionError:
                time.sleep(0.1)
        else:
            raise

    def _parse_pending_exit_codes(self, value: str) -> List[int]:
        if not value:
            return []
        codes: List[int] = []
        for part in value.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                codes.append(int(part))
            except ValueError:
                continue
        return codes

    def _update_manifest_flag(self, sid: str, flag: Optional[StepFlag]) -> None:
        for entry in self.steps_manifest:
            if entry.get("id") == sid:
                if flag:
                    entry["flag_status"] = flag.status
                    entry["flag_updated_at"] = flag.updated_at
                else:
                    entry["flag_status"] = StepState.NOT_RUN
                    entry["flag_updated_at"] = ""
                break

    def write_run_meta(self):
        def _prune_strings(data: Dict[str, object]) -> Dict[str, object]:
            cleaned = {}
            for key, value in data.items():
                if isinstance(value, str):
                    value = value.strip()
                    if not value:
                        continue
                cleaned[key] = value
            return cleaned

        sn_dir = self.project_root / "runs" / self.sn
        if sn_dir.exists():
            run_dirs = [d for d in sn_dir.iterdir() if d.is_dir()]
            self.test_times = len(run_dirs)
        else:
            self.test_times = 1

        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        meta = {
            "sn": self.sn,
            "run_id": self.ts,
            "test_times": self.test_times, 
            "timestamp": timestamp,
            "ui_version": _get_ui_version(),
            "run_mode": self.run_mode,
            "debug": bool(self.debug_enabled),
            "global_status": self.global_status,
            "station": _prune_strings(self.station_meta),
            "supports_reboot": bool(self.supports_reboot),
            "run_context": {
                "config_path": str(self.config_path.resolve()),
                "project_root": str(self.project_root.resolve()),
                "run_dir": str(self.run_dir.resolve()),
                "debug_enabled": bool(self.debug_enabled),
            },
            "steps_manifest": self.steps_manifest,
        }
        (self.run_dir/"run_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8-sig")

    def _spawn(self, cmd: str, cwd: Path):
        env = os.environ.copy()
        # env.setdefault("PYTHONIOENCODING", "utf-8-sig")
        env.setdefault("PYTHONIOENCODING", "utf-8")
        return subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            env=env,
        )

    def _kill_tree(self, p: subprocess.Popen):
        try:
            if os.name == "nt":
                subprocess.run(f"taskkill /PID {p.pid} /T /F", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                p.terminate()
        except Exception:
            pass

    def run_all(self):
        self.global_status = Status.RUNNING
        self.on_status_changed(self.global_status)
        self._write_latest_info(self.ts, "running")
        self.write_run_meta()

        # === 防呆空跑 ===
        if not self.steps:
            self.global_status = Status.FAIL
            self.on_log_line("[ERROR] Could not find steps! Please check if station.ini is missing or has format errors.\n")
            self.on_status_changed(self.global_status)
            self._write_latest_info(self.ts, "finished_fail")
            self.write_run_meta()
            # 強制產出報告，確保產線腳本有檔案可抓
            self._emit_report()
            return

        evaluator = ResultEvaluator(self.run_dir)
        stopped_on_fail = False
        pending_wait = False

        for s in self.steps:
            sid = self._sid(s)
            res = self._run_step(s, evaluator)
            self.results[sid] = res

            if self.stop_requested:
                self.global_status = Status.STOPPED
                break

            if res.state == StepState.RUNNING:
                pending_wait = True
                self.global_status = Status.RUNNING
                break

            if res.state in (StepState.FAIL, StepState.TIMEOUT) and not s.ignore_result:
                # 有測項 FAIL → 建立 fail flag 檔 + 呼叫指定工具(不 block)
                self._handle_fail(self._sid(s))
                # --- 修改開始 ---
                # 讓 stop_on_fail_retry 也能觸發停止
                if self.run_mode in ["stop_on_fail", "stop_on_fail_retry"]:
                    stopped_on_fail = True
                    self._mark_remaining_skipped(s.order)
                    self.global_status = Status.FAIL
                    break
        else:
            failed = False
            for s in self.steps:
                sid = self._sid(s)
                if sid in self.results:
                    r = self.results[sid]
                    if r.state in (StepState.FAIL, StepState.TIMEOUT) and not s.ignore_result:
                        failed = True
                        break
            self.global_status = Status.PASS if not failed else Status.FAIL

        self.on_status_changed(self.global_status)
        self.write_run_meta()

        if pending_wait:
            self._write_latest_info(self.ts, "pending")
        elif self.global_status == Status.PASS:
            self._write_latest_info(self.ts, "finished_pass")
        elif self.global_status == Status.FAIL:
            self._write_latest_info(self.ts, "finished_fail")
        elif self.global_status == Status.STOPPED:
            self._write_latest_info(self.ts, "stopped")
        else:
            self._write_latest_info(self.ts, "running")

        if "always" in self.report_emit_on or (
            stopped_on_fail and "stopped_on_fail" in self.report_emit_on
        ):
            self._emit_report()
            # full_log 已由 LiveLogger 即時寫入，不再收尾合併

    def _handle_fail(self, failed_sid: str) -> None:
        """測項 FAIL 時:在 C:\\Diag\\FLAG 建立 FactoryTestUI.flg(內容=失敗測項 section 名),
        再呼叫 station.ini [run] fail_tool 指定的工具(不 block UI)。"""
        # 1. 建立 flag 檔(固定路徑/檔名，內容=失敗的 section 名，覆蓋舊的)
        try:
            flag_dir = Path(r"C:\Diag\FLAG")
            flag_dir.mkdir(parents=True, exist_ok=True)
            (flag_dir / "FactoryTestUI.flg").write_text(failed_sid, encoding="utf-8")
        except Exception as e:
            self.on_log_line(f"[WARN] write fail flag failed: {e}\n")

        # 2. 呼叫工具(沒設就跳過)；相對路徑以 workdir 為基準；依副檔名決定怎麼跑；不 block
        if not self.fail_tool:
            return
        try:
            tool = Path(self.fail_tool)
            if not tool.is_absolute():
                tool = self.workdir_global / tool
            ext = tool.suffix.lower()
            if ext == ".py":
                cmd = f'"{sys.executable}" "{tool}"'
            else:  # .bat / .exe / 其他，直接跑
                cmd = f'"{tool}"'
            subprocess.Popen(cmd, shell=True, cwd=str(self.workdir_global))
        except Exception as e:
            self.on_log_line(f"[WARN] call fail_tool failed: {e}\n")

    def _run_step(self, s: Step, evaluator: ResultEvaluator) -> StepResult:
        sid = self._sid(s)
        res = StepResult()
        existing_flag = self.state_store.read(sid)

        if self.run_mode == "stop_on_fail_retry":
            # print("stop_on_fail_retry")
            skip_statuses = {StepState.PASS}
        else:
            skip_statuses = {StepState.PASS, StepState.FAIL, StepState.TIMEOUT}

        # 檢查是否符合跳過條件 (非 debug 模式的路線)
        if not self.debug_enabled and existing_flag and existing_flag.status in skip_statuses:
            res.state = StepState.PASS if existing_flag.status == StepState.PASS else existing_flag.status
            res.note = existing_flag.note or f"resume flag={existing_flag.status}"
            self.step_flags[sid] = existing_flag
            self._update_manifest_flag(sid, existing_flag)
            self.on_log_line(f"[RESUME] {sid} flagged as {existing_flag.status}, skip execution.\n")
            self.on_step_started(sid, 0)
            self.on_step_finished(sid, res)
            return res
 
        attempts = s.retry + 1
        workdir = self.workdir_global
        if s.workdir:
            workdir = self._resolve_workdir(Path(s.workdir))
        file_contains_rules = [r.strip() for r in s.pass_by.split("|") if r.strip().startswith("file_contains:")]

        step_dir = (self.run_dir / "steps" / sid)
        step_dir.mkdir(parents=True, exist_ok=True)
        log_path = step_dir / "step.log"
        log_reset = False
        context_placeholders = {
            "STEP_ID": sid,
            "STEP_NAME": s.name,
            "STEP_ORDER": str(s.order),
            "STEP_DIR": str(step_dir.resolve()),
        }

        for attempt in range(1, attempts + 1):
            if self.stop_requested:
                res.state = StepState.SKIPPED
                self.on_step_finished(sid, res)
                return res
            
            # 目前沒用，沒有 pause 這功能。如果有，也只能停 runner 而不能停外部程式
            while self.pause_requested and not self.stop_requested: 
                time.sleep(0.1)

            res.attempt = attempt
            self.on_step_started(sid, attempt)
            self.on_log_line(f"\n========== Enter {sid} (attempt {attempt}) ==========\n")
            self.live_logger.step_header(log_path, s.name, attempt)
            start = time.time()

            cmd = self._expand(s.cmd, context_placeholders)
            proc = None
            stdout_acc: List[str] = []
            stderr_acc: List[str] = []
            try:
                # interactive
                if s.type == "interactive":
                    if getattr(sys, 'frozen', False):
                        cmd = f'"{sys.executable}" --interactive --base "{workdir}" "{cmd}"'
                    else:
                        launcher_script = Path(__file__).resolve().parent / "interactive_launcher.py"
                        cmd = f'"{sys.executable}" "{launcher_script}" --base "{workdir}" "{cmd}"'

                # 預設 type 皆為 process
                
                proc = self._spawn(cmd, workdir)
                self.current_proc = proc # TEST: 紀錄目前跑的 step
                def reader(stream, acc, prefix):
                    for line in iter(stream.readline, b""):
                        txt = line.decode(s.stdout_encoding, errors="ignore")
                        acc.append(txt)
                        self.on_log_line(f"[{prefix}] {txt}")
                        self.live_logger.write_line(log_path, prefix, txt)

                t1 = threading.Thread(target=reader, args=(proc.stdout, stdout_acc, "STDOUT"), daemon=True) # 測項的 stdout 的來源 proc.stdout
                t2 = threading.Thread(target=reader, args=(proc.stderr, stderr_acc, "STDERR"), daemon=True)
                t1.start()
                t2.start()

                try:
                    rc = proc.wait(timeout=s.timeout if s.timeout > 0 else None)
                except subprocess.TimeoutExpired:
                    if s.kill_tree:
                        self.on_log_line(f"[TIMEOUT] Killing process tree for {sid}\n")
                        self._kill_tree(proc)
                    else:
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                    time.sleep(1.0)
                    rc = None

                duration = time.time() - start
                t1.join(timeout=0.2)
                t2.join(timeout=0.2)

                stdout_text = "".join(stdout_acc)
                stderr_text = "".join(stderr_acc)
                

                if not log_reset:
                    if log_path.exists():
                        try:
                            with log_path.open("a", encoding="utf-8-sig") as f:
                                f.write(f"\n{'='*20} RESUME / RETRY SESSION {'='*20}\n")
                        except Exception:
                            pass
                    log_reset = True

                if rc is None:
                    res.state = StepState.TIMEOUT
                    res.exit_code = -1
                    res.duration_s = round(duration, 2)
                    res.matched_rule = "timeout"
                else:
                    is_pass, matched = evaluator.evaluate( # result_evaluator.py 的 evaluate
                        s.pass_by, 
                        rc, 
                        stdout_text, 
                        stderr_text, 
                        workdir, 
                        step_data=s.extra_config # 2026/2/2 新增(傳參數)(目前沒用)
                        )
                    res.exit_code = rc
                    res.duration_s = round(duration, 2)
                    res.matched_rule = matched
                    if rc in s.pending_exit_codes:
                        res.state = StepState.RUNNING
                        res.note = f"pending exit code {rc}"
                    else:
                        res.state = StepState.PASS if (is_pass or s.ignore_result) else StepState.FAIL
                    for rule in file_contains_rules:
                        self._log_file_contains_output(rule, workdir, sid)

                self._append_step_log(
                    log_path,
                    attempt,
                    stdout_text,
                    stderr_text,
                    res.state,
                    res.exit_code,
                    res.duration_s,
                    res,
                )

                if res.state == StepState.PASS:
                    break

                if res.state == StepState.RUNNING:
                    break

                if attempt < attempts and res.state in (StepState.FAIL, StepState.TIMEOUT):
                    time.sleep(max(0, s.retry_interval_sec))
                else:
                    break

            finally:
                self.current_proc = None # TEST: 目前跑的 step
                if proc and proc.poll() is None:
                    try:
                        if s.kill_tree:
                            self._kill_tree(proc)
                        else:
                            proc.terminate()
                    except Exception:
                        pass

        if s.ignore_result and res.state not in (StepState.PASS, StepState.RUNNING):
            res.note = f"ignored underlying={res.state}"
            res.state = StepState.PASS

        flag: Optional[StepFlag] = None
        if res.state in {StepState.PASS, StepState.FAIL, StepState.TIMEOUT, StepState.RUNNING}:
            existing_snapshot = self.state_store.read(sid)
            extra_payload: Dict[str, Any] = {}
            if existing_snapshot and existing_snapshot.extra:
                extra_payload.update(existing_snapshot.extra)
            if res.exit_code is not None:
                extra_payload["exit_code"] = res.exit_code
            if res.matched_rule:
                extra_payload["matched_rule"] = res.matched_rule
            flag = self.state_store.write(
                sid,
                res.state,
                note=res.note,
                attempt=res.attempt,
                extra=extra_payload,
            )
            self.step_flags[sid] = flag
            self._update_manifest_flag(sid, flag)
        else:
            self.step_flags.pop(sid, None)
            self._update_manifest_flag(sid, None)

        self.on_log_line(f"========== End {sid} (state={res.state}) ==========\n")
        self.on_log_line(f"{'-' * 150}\n")
        self.on_step_finished(sid, res)

        return res
    
    def pause(self): # 目前沒用
        self.pause_requested = True
        self.on_status_changed(Status.PAUSED)

    def resume(self):
        self.pause_requested = False
        self.on_status_changed(Status.RUNNING)

    def stop(self):
        self.stop_requested = True

        # TEST: 把目前跑的 step kill 掉
        proc = self.current_proc
        if proc and proc.poll() is None:
            try:
                self._kill_tree(proc)
            except Exception:
                pass

    def _mark_remaining_skipped(self, current_order: int):
        for s in self.steps:
            if s.order > current_order:
                self.results[self._sid(s)].state = StepState.SKIPPED

    def _step_by_sid(self, sid: str) -> Step: # 目前沒用到
        for s in self.steps:
            if self._sid(s) == sid:
                return s
        raise KeyError(sid)

    def reset_step_flags(self) -> None:
        self.state_store.reset_all()
        self.step_flags.clear()
        for entry in self.steps_manifest:
            entry["flag_status"] = StepState.NOT_RUN
            entry["flag_updated_at"] = ""
        for res in self.results.values():
            res.state = StepState.NOT_RUN
            res.note = ""
            res.matched_rule = ""
            res.exit_code = None
            res.duration_s = 0.0
            res.attempt = 0
        self._write_latest_info(self.ts, "running")



    def _emit_report(self):
            rows = []
            for s in self.steps:
                sid = self._sid(s)
                r = self.results.get(sid, StepResult())
                rows.append({
                    "sn": self.sn,              # 方便對應
                    "test_times": getattr(self, "test_times", 1), # <--- 新增這行
                    "step_id": sid,
                    "state": r.state,
                    "exit_code": r.exit_code,
                    "matched_rule": r.matched_rule,
                    "duration_s": r.duration_s,
                    "attempt": r.attempt,
                    "note": r.note,
                    "message": r.log,
                    "pass_criteria": s.pass_by,
                })
            report_dir = self.run_dir/"reports"
            report_dir.mkdir(exist_ok=True, parents=True)
            json_path = report_dir/f"report_{self.sn}_{self.ts}.json"
            (json_path).write_text(json.dumps(rows, indent=2), encoding="utf-8-sig")

    # 寫入檔案的 log 
    def _append_step_log(
        self,
        log_path: Path,
        attempt: int,
        stdout_text: str,
        stderr_text: str,
        state: str,
        exit_code: Optional[int],
        duration_s: float,
        result: StepResult,
    ) -> None:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        exit_display = "" if exit_code is None else exit_code
        header = (
            f"\n===== Attempt {attempt} @ {timestamp} =====\n"
            f"State: {state} | Exit Code: {exit_display} | Duration: {duration_s:.2f}s\n\n"
        )
        # std 已由 LiveLogger 即時寫入 step.log / full_log，這裡只補這段 header 摘要，不重複寫 std。
        self.live_logger._append(log_path, header)
        self.live_logger._append(self.live_logger.full_log_path, header)

        # result.log 仍保留完整內容(含 std)，供 report 等其他地方使用
        stdout_block = "[STDOUT]" + stdout_text
        if not stdout_block.endswith("\n"):
            stdout_block += "\n"
        stderr_block = "[STDERR]" + stderr_text
        if not stderr_block.endswith("\n"):
            stderr_block += "\n"
        result.log = (result.log or "") + header + stdout_block + stderr_block

    # 跟 pass_by 的 file_contains 相關
    def _log_file_contains_output(self, rule: str, workdir: Path, sid: str):
        try:
            _, rest = rule.split(":", 1)
            file_part, _ = rest.rsplit(":", 1)
        except ValueError:
            return
        file_path = Path(file_part)
        fp = (workdir / file_path) if not file_path.is_absolute() else file_path
        if not fp.exists():
            self.on_log_line(f"[FILE][{sid}] Missing file for rule '{rule}': {fp}\n")
            return
        try:
            content = fp.read_text(encoding="utf-8-sig", errors="ignore")
        except Exception as exc:
            self.on_log_line(f"[FILE][{sid}] Failed to read {fp}: {exc}\n")
            return
        self.on_log_line(f"[FILE][{sid}] Contents of {fp}:\n{content}\n")

    def _emit_aggregated_log(self):
            """
            將所有步驟的 step.log 讀取並合併成一份完整的 Log 檔案
            只包含有實際執行或從歷史紀錄恢復的步驟 (忽略 NOT_RUN 和 SKIPPED)
            """
            full_log_name = f"full_log_{self.sn}_{self.ts}.txt"
            full_log_path = self.run_dir / "reports" / full_log_name
            
            full_log_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                with full_log_path.open("w", encoding="utf-8-sig") as out_f:
                    out_f.write(f"========== Full Execution Log ==========\n")
                    out_f.write(f"SN: {self.sn}\n")
                    out_f.write(f"Run ID: {self.ts}\n")
                    out_f.write(f"Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    out_f.write("=" * 40 + "\n\n")

                    for s in self.steps:
                        sid = self._sid(s)
                        step_dir = self.run_dir / "steps" / sid
                        step_log_path = step_dir / "step.log"
                        
                        # 取得當前狀態
                        r = self.results.get(sid)
                        state = r.state if r else StepState.NOT_RUN
                        has_log = step_log_path.exists()

                        # --- 過濾邏輯 ---
                        # 如果沒有 Log 檔，且狀態是 NOT_RUN 或 SKIPPED，代表完全沒測到，直接跳過不印
                        if not has_log and state in [StepState.NOT_RUN, StepState.SKIPPED]:
                            continue
                        # ----------------

                        # 寫入步驟標題
                        out_f.write(f"{'-'*40}\n")
                        out_f.write(f"Step: {s.order}_{s.name}\n")
                        out_f.write(f"Pass Criteria: {s.pass_by}\n")
                        out_f.write(f"{'-'*40}\n")

                        if has_log:
                            try:
                                content = step_log_path.read_text(encoding="utf-8-sig", errors="replace")
                                if not content.endswith("\n"):
                                    content += "\n"
                                out_f.write(content)
                            except Exception as e:
                                out_f.write(f"[ERROR] Failed to read log file: {e}\n")
                        else:
                            # 這裡只會印出 PASS/FAIL/TIMEOUT 等 Resume 的狀態
                            # 因為 NOT_RUN/SKIPPED 已經在上面被 continue 掉了
                            out_f.write(f"[INFO] No log file available. (Resumed from State: {state})\n")
                        
                        out_f.write("\n")
                
                self.on_log_line(f"[REPORT] Full log generated: {full_log_path}\n")

            except Exception as e:
                print(f"Failed to generate aggregated log : {e}")