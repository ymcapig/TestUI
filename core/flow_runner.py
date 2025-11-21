import subprocess, threading, time, os, json, re
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import configparser
from .result_evaluator import ResultEvaluator
from .events import Status, StepState
from .state_store import StepStateStore, StepFlag

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

        self.cfg = configparser.ConfigParser()
        self.cfg.optionxform = str  # preserve case
        self.cfg.read(self.config_path, encoding="utf-8-sig")

        self.station_meta = {
            "station_id": self.cfg.get("meta", "station_id", fallback="").strip(),
            "line_id": self.cfg.get("meta", "line_id", fallback="").strip(),
            "model": self.cfg.get("meta", "model", fallback="").strip(),
        }
        self.supports_reboot = True

        self.run_mode = self.cfg.get("run","run_mode", fallback="stop_on_fail")
        self.default_timeout = self.cfg.getint("run","default_timeout_sec", fallback=120)
        self.report_emit_on = [x.strip() for x in self.cfg.get("run","report.emit_on", fallback="stopped_on_fail").split(",")]
        run_workdir_str = self.cfg.get("run","workdir", fallback=".")
        self.workdir_global = self._resolve_workdir(Path(run_workdir_str))

        # Prepare run dir (resume if unfinished run exists)
        self._latest_info_path = None
        self._resume_existing = False
        self.run_dir, self.ts = self._prepare_run_directory()

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
                    type=s.get("type","process"),
                    cmd=s.get("cmd",""),
                    workdir=s.get("workdir",""),
                    timeout=s.getint("timeout", self.default_timeout),
                    retry=s.getint("retry",0),
                    retry_interval_sec=s.getint("retry_interval_sec",0),
                    ignore_result=s.getboolean("ignore_result", False),
                    pass_by=s.get("pass_by",""),
                    kill_tree=s.getboolean("kill_tree", True),
                    stdout_encoding=s.get("stdout_encoding","utf-8"),
                    artifacts=s.get("artifacts",""),
                    pending_exit_codes=self._parse_pending_exit_codes(s.get("pending_exit_codes", "")),
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
        self.on_status_changed = lambda status: None
        self.on_step_started = lambda sid: None
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
        if workdir.is_absolute():
            return workdir
        candidate_config = (self.config_dir / workdir)
        candidate_project = (self.project_root / workdir)
        if candidate_project.exists() or not candidate_config.exists():
            return candidate_project
        return candidate_config

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
                if candidate and (stored_sn in (None, "", self.sn)) and status in {"running", "pending"}:
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
        tmp_path.replace(self._latest_info_path)

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

        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        meta = {
            "sn": self.sn,
            "run_id": self.ts,
            "timestamp": timestamp,
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
                if self.run_mode == "stop_on_fail":
                    stopped_on_fail = True
                    self._mark_remaining_skipped(s.order)
                    self.global_status = Status.FAIL
                    break
        else:
            failed = any((r.state in (StepState.FAIL, StepState.TIMEOUT)) and not self._step_by_sid(k).ignore_result
                         for k, r in self.results.items())
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

    def pause(self):
        self.pause_requested = True
        self.on_status_changed(Status.PAUSED)

    def resume(self):
        self.pause_requested = False
        self.on_status_changed(Status.RUNNING)

    def stop(self):
        self.stop_requested = True

    def _mark_remaining_skipped(self, current_order: int):
        for s in self.steps:
            if s.order > current_order:
                self.results[self._sid(s)].state = StepState.SKIPPED

    def _step_by_sid(self, sid: str) -> Step:
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

    def _run_step(self, s: Step, evaluator: ResultEvaluator) -> StepResult:
        sid = self._sid(s)
        res = StepResult()
        existing_flag = self.state_store.read(sid)
        if existing_flag and existing_flag.status in {StepState.PASS, StepState.FAIL, StepState.TIMEOUT}:
            res.state = StepState.PASS if existing_flag.status == StepState.PASS else existing_flag.status
            res.note = existing_flag.note or f"resume flag={existing_flag.status}"
            self.step_flags[sid] = existing_flag
            self._update_manifest_flag(sid, existing_flag)
            self.on_log_line(f"[RESUME] {sid} flagged as {existing_flag.status}, skip execution.\n")
            self.on_step_started(sid)
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

            while self.pause_requested and not self.stop_requested:
                time.sleep(0.1)

            res.attempt = attempt
            self.on_step_started(sid)
            self.on_log_line(f"\n========== Enter {sid} (attempt {attempt}) ==========\n")
            start = time.time()

            cmd = self._expand(s.cmd, context_placeholders)
            proc = None
            stdout_acc: List[str] = []
            stderr_acc: List[str] = []

            try:
                proc = self._spawn(cmd, workdir)

                def reader(stream, acc, prefix):
                    for line in iter(stream.readline, b""):
                        txt = line.decode(errors="ignore")
                        acc.append(txt)
                        self.on_log_line(f"[{prefix}] {txt}")

                t1 = threading.Thread(target=reader, args=(proc.stdout, stdout_acc, "STDOUT"), daemon=True)
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
                            log_path.unlink()
                        except Exception:
                            pass
                    log_reset = True

                if rc is None:
                    res.state = StepState.TIMEOUT
                    res.exit_code = -1
                    res.duration_s = round(duration, 2)
                    res.matched_rule = "timeout"
                else:
                    is_pass, matched = evaluator.evaluate(s.pass_by, rc, stdout_text, stderr_text, workdir)
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
        self.on_step_finished(sid, res)

        return res

    def _emit_report(self):
        rows = []
        for s in self.steps:
            sid = self._sid(s)
            r = self.results.get(sid, StepResult())
            rows.append({
                "step_id": sid,
                "state": r.state,
                "exit_code": r.exit_code,
                "matched_rule": r.matched_rule,
                "duration_s": r.duration_s,
                "attempt": r.attempt,
                "note": r.note,
                "message": r.log,
            })
        report_dir = self.run_dir/"reports"
        report_dir.mkdir(exist_ok=True, parents=True)
        json_path = report_dir/f"report_{self.sn}_{self.ts}.json"
        (json_path).write_text(json.dumps(rows, indent=2), encoding="utf-8-sig")

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
            f"State: {state} | Exit Code: {exit_display} | Duration: {duration_s:.2f}s\n"
        )
        stdout_block = stdout_text or ""
        if not stdout_block.endswith("\n"):
            stdout_block += "\n"
        stderr_block = stderr_text or ""
        if not stderr_block.endswith("\n"):
            stderr_block += "\n"
        body = f"[STDOUT]\n{stdout_block}[STDERR]\n{stderr_block}"
        chunk = header + body
        with log_path.open("a", encoding="utf-8-sig") as log_file:
            log_file.write(chunk)
        result.log = (result.log or "") + chunk

    def _log_file_contains_output(self, rule: str, workdir: Path, sid: str):
        try:
            _, rest = rule.split(":", 1)
            file_part, _ = rest.split(":", 1)
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
