"""
RunController：一次測試「計畫（RunPlan）」的生命週期管理者。

職責範圍：
1. 收到 start 請求 → 檢查該 SN 的歷史紀錄 → 決定是否擋下
2. 啟動 FlowRunner（透過 RunnerThread）→ 觀察 step 進度
3. plan 完成後 → 依 plan 類型 / 歷史紀錄決定最終顯示
4. 提供 reset flags 的兩種路徑（清 runner / 清歷史）

這層故意不碰 widget，對外全部用 Qt signal 溝通。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional
import configparser

from PyQt5.QtCore import QObject, pyqtSignal

from core.events import Status
from core.flow_runner import FlowRunner

from app.services.run_history_service import RunHistoryService

from app.ui.ui_components import RunnerThread


# 歷史紀錄的 status 字串 → 顯示用 Status 的對照
_FINISHED_STATUS_MAP = {
    "finished_pass": Status.PASS,
    "finished_fail": Status.FAIL,
    "stopped": Status.STOPPED,
}
_FINISHED_STATUS_LABEL = {
    "finished_pass": "PASS",
    "finished_fail": "FAIL",
    "stopped": "STOPPED",
}


@dataclass
class LoopCounter:
    total: int = 1
    remaining: int = 0
    current: int = 0

    def reset(self, total: int) -> None:
        total = max(1, int(total))
        self.total = total
        self.remaining = total
        self.current = 0

    def next(self) -> bool:
        if self.remaining <= 0:
            return False
        self.remaining -= 1
        self.current += 1
        return True

    def clear(self) -> None:
        self.total = 1
        self.remaining = 0
        self.current = 0


@dataclass
class RunPlan:
    sn: str
    selected_sids: Optional[list[str]] = None
    manual_tag: str = ""
    loops: int = 1
    continue_on_fail: bool = False

    @property
    def is_manual(self) -> bool:
        return bool(self.selected_sids)


@dataclass
class StartResult:
    """controller.start() 的回傳。started=False 時 caller 要處理 blocked_reason。"""
    started: bool
    blocked_reason: Optional[str] = None    # "config_load_failed" | "already_running" | "prior_pass"
    prior_sn: Optional[str] = None           # blocked_reason == "prior_pass" 時有值
    prior_status_label: Optional[str] = None


@dataclass
class ClearFlagsResult:
    """reset flags 動作的結果。"""
    outcome: str                             # "runner_cleared" | "snapshot_cleared" | "none" | "error" | "running"
    sn: Optional[str] = None
    error: Optional[Exception] = None


@dataclass
class FinishedRunPayload:
    """當偵測到有可顯示的歷史紀錄時，controller 丟給 UI 去渲染的資料。"""
    sn: str
    run_dir: Path
    info: dict[str, Any]
    mapped_status: str                       # 來自 Status.PASS/FAIL/STOPPED


class RunController(QObject):
    # ---- 既有訊號 -----------------------------------------------------------
    status_changed = pyqtSignal(str)
    step_started = pyqtSignal(str, int)
    step_finished = pyqtSignal(str, dict)
    log_line = pyqtSignal(str)
    run_state_changed = pyqtSignal(bool)
    plan_finished = pyqtSignal(object)

    # ---- 新增訊號 -----------------------------------------------------------
    # 當 pre-run check 或 plan 結束後，需要把歷史紀錄畫回 UI 時發出
    finished_run_available = pyqtSignal(object)    # payload: FinishedRunPayload
    # 有 finished-pass snapshot 可以 reset 時為 True，被清掉或沒有時為 False
    snapshot_available_changed = pyqtSignal(bool)

    def __init__(
        self,
        cfg_path: Path,
        translator: Callable[..., str],
        debug_enabled_provider: Callable[[], bool],
        prepare_runner: Callable[[FlowRunner, RunPlan], None],
        history_service: RunHistoryService,
        config_load_failed_provider: Callable[[], bool] = lambda: False,
    ):
        super().__init__()
        self.cfg_path = Path(cfg_path)
        self._ = translator
        self._debug_enabled_provider = debug_enabled_provider
        self._prepare_runner = prepare_runner
        self._history = history_service
        self._config_load_failed_provider = config_load_failed_provider

        self.runner_thread = RunnerThread()
        self.runner_thread.status_changed.connect(self.status_changed)
        self.runner_thread.step_started.connect(self.step_started)
        self.runner_thread.step_finished.connect(self._on_thread_step_finished)
        self.runner_thread.log_line.connect(self.log_line)
        self.runner_thread.finished.connect(self._on_thread_finished)

        self._plan: Optional[RunPlan] = None
        self._loop = LoopCounter()
        self._stop_requested = False
        self._current_runner: Optional[FlowRunner] = None

        # 歷史 snapshot（finished_pass 時會被記住，用於 reset flags）
        self._snapshot: Optional[dict[str, Any]] = None

    # ---- 查詢屬性 -----------------------------------------------------------

    @property
    def current_runner(self) -> Optional[FlowRunner]:
        return self._current_runner

    @property
    def is_running(self) -> bool:
        return self.runner_thread.isRunning()

    @property
    def has_snapshot(self) -> bool:
        return self._snapshot is not None

    # ---- start / stop -------------------------------------------------------

    def start(self, plan: RunPlan) -> StartResult:
        """
        嘗試啟動一個 RunPlan。若因為設定錯誤、正在跑、或該 SN 上次 PASS
        過而被擋下，會回 started=False。
        """
        if self._config_load_failed_provider():
            return StartResult(started=False, blocked_reason="config_load_failed")
        if self.is_running:
            return StartResult(started=False, blocked_reason="already_running")
        
        # # 先試讀 ini，格式錯誤就擋下來
        # try:
        #     self._config_service.load_app_config()
        # except configparser.Error as exc:
        #     return StartResult(
        #         started=False,
        #         blocked_reason="config_error",
        #         error=str(exc),
        #     )

        # 手動跑（debug 模式選擇性執行）不走 pre-run check
        if not plan.is_manual:
            blocked = self._check_prior_run(plan.sn)
            if blocked is not None:
                return blocked

        self._begin_plan(plan)
        return StartResult(started=True)

    def stop(self) -> None:
        self._stop_requested = True
        if self._current_runner:
            self._current_runner.stop()

    # ---- pre-run check ------------------------------------------------------

    def _check_prior_run(self, sn: str) -> Optional[StartResult]:
        """
        查該 SN 上次的結果。會 emit finished_run_available 讓 UI 先把前次
        結果畫出來。上次是 finished_pass 就回一個 blocked 的 StartResult；
        其他情況回 None（代表可以繼續跑）。
        """
        info, latest_path, run_dir = self._history.load_latest_run_info(sn)
        if not info:
            self.log_line.emit(
                f"[INFO] No previous finished-run info found for SN={sn}.\n"
            )
            return None

        status = (info.get("status") or "").lower()
        if status not in _FINISHED_STATUS_MAP:
            self.log_line.emit(
                f"[INFO] Latest run status '{status}' is not displayable/finished "
                f"for SN={sn}; continue testing.\n"
            )
            return None

        if not run_dir or not run_dir.exists():
            self.log_line.emit(
                f"[WARN] Latest run directory missing for SN={sn}: {run_dir}\n"
            )
            return None

        mapped_status = _FINISHED_STATUS_MAP[status]
        self.finished_run_available.emit(
            FinishedRunPayload(sn=sn, run_dir=run_dir, info=info, mapped_status=mapped_status)
        )

        if status == "finished_pass":
            self._set_snapshot({
                "sn": sn,
                "latest_path": latest_path,
                "run_dir": run_dir,
                "status": status,
            })
            label = _FINISHED_STATUS_LABEL.get(status, status)
            return StartResult(
                started=False,
                blocked_reason="prior_pass",
                prior_sn=sn,
                prior_status_label=label,
            )

        label = _FINISHED_STATUS_LABEL.get(status, status)
        self.log_line.emit(
            "[INFO] "
            + self._(
                "log.previous_status_continue",
                default="SN {sn} last result: {status}; continuing tests.",
                sn=sn,
                status=label,
            )
            + "\n"
        )
        return None

    # ---- reset flags --------------------------------------------------------

    def clear_flags(self) -> ClearFlagsResult:
        """
        集中處理「按下 reset flags」的全部路徑：
        - 正在跑 → "running"，caller 彈警告
        - 剛跑完 runner 還在 → 清 runner 的 flag
        - 否則有歷史 snapshot → 清 snapshot
        - 什麼都沒有 → "none"
        """
        if self.is_running:
            return ClearFlagsResult(outcome="running")

        if self._current_runner is not None:
            try:
                self._current_runner.reset_step_flags()
            except Exception as exc:
                return ClearFlagsResult(outcome="error", error=exc)
            self.log_line.emit(
                f"[INFO] {self._('log.reset_flags_cleared', default='Step result flags cleared.')}\n"
            )
            return ClearFlagsResult(outcome="runner_cleared")

        if self._snapshot is None:
            return ClearFlagsResult(outcome="none")

        sn = self._snapshot.get("sn")
        try:
            self._history.clear_snapshot(self._snapshot)
        except Exception as exc:
            return ClearFlagsResult(outcome="error", sn=sn, error=exc)
        self.log_line.emit(
            "[INFO] "
            + self._(
                "log.latest_snapshot_cleared",
                default="Cleared latest run snapshot for SN {sn}.",
                sn=sn,
            )
            + "\n"
        )
        self._set_snapshot(None)
        return ClearFlagsResult(outcome="snapshot_cleared", sn=sn)

    # ---- 內部：plan 驅動 ----------------------------------------------------

    def _begin_plan(self, plan: RunPlan) -> None:
        normalized_sids = [sid for sid in (plan.selected_sids or []) if sid]
        self._plan = RunPlan(
            sn=plan.sn,
            selected_sids=normalized_sids or None,
            manual_tag=(plan.manual_tag or "").strip(),
            loops=max(1, int(plan.loops or 1)),
            continue_on_fail=bool(plan.continue_on_fail),
        )
        self._stop_requested = False
        self._loop.reset(self._plan.loops)
        self._start_next_iteration()

    def _start_next_iteration(self) -> None:
        plan = self._plan
        if not plan:
            self._finish_plan(stopped=self._stop_requested, final_status=None)
            return

        if self._stop_requested or self._loop.remaining <= 0:
            self._finish_plan(stopped=self._stop_requested, final_status=None)
            return

        self._loop.next()
        runner = FlowRunner(self.cfg_path, plan.sn, self._debug_enabled_provider())
        self._prepare_runner(runner, plan)
        self._filter_steps(runner, plan.selected_sids)

        self._current_runner = runner
        self.run_state_changed.emit(True)

        if getattr(runner, "resumed", False):
            self.log_line.emit(
                f"[INFO] {self._('log.resuming_run', default='Resuming run {run_id} for SN {sn}', run_id=runner.ts, sn=plan.sn)}\n"
            )

        if self._loop.total > 1:
            label = self._(
                "log.stress_progress.manual" if plan.is_manual else "log.stress_progress.auto",
                default="--- Run {current}/{total} ---",
                current=self._loop.current,
                total=self._loop.total,
            )
            self.log_line.emit(f"\n{label}\n")

        self.runner_thread.set_runner(runner)
        self.runner_thread.start()

    def _filter_steps(self, runner: FlowRunner, selected_sids: Optional[list[str]]) -> None:
        if not selected_sids:
            return
        order_map = {sid: idx for idx, sid in enumerate(selected_sids)}
        filtered = []
        for step in runner.steps:
            sid = f"{step.order}_{step.name}"
            if sid in order_map:
                filtered.append(step)
        filtered.sort(key=lambda step: order_map[f"{step.order}_{step.name}"])
        runner.steps = filtered

    def _on_thread_step_finished(self, sid: str, res: dict) -> None:
        payload = dict(res)
        plan = self._plan
        if plan and plan.manual_tag:
            note = payload.get("note") or ""
            payload["note"] = f"{plan.manual_tag} {note}".strip() if note else plan.manual_tag
        self.step_finished.emit(sid, payload)

    def _on_thread_finished(self) -> None:
        plan = self._plan
        runner = self._current_runner
        final_status = runner.global_status if runner else None
        self._current_runner = None

        should_continue = bool(
            plan
            and not self._stop_requested
            and self._loop.remaining > 0
            and (plan.continue_on_fail or final_status == Status.PASS)
        )
        if should_continue:
            self._start_next_iteration()
            return

        self._finish_plan(stopped=self._stop_requested, final_status=final_status)

    def _finish_plan(self, stopped: bool, final_status: Optional[str]) -> None:
        plan = self._plan
        context = {
            "plan": plan,
            "stopped": bool(stopped),
            "final_status": final_status,
            "completed_loops": self._loop.current,
            "total_loops": self._loop.total,
        }
        self._plan = None
        self._current_runner = None
        self._stop_requested = False
        self._loop.clear()
        self.run_state_changed.emit(False)

        # plan 結束後，對於非手動 run 做歷史紀錄檢查
        if plan and not plan.is_manual:
            self._post_plan_history_check(plan.sn, stopped, final_status)

        self.plan_finished.emit(context)

    def _post_plan_history_check(
        self,
        sn: str,
        stopped: bool,
        final_status: Optional[str],
    ) -> None:
        """
        plan 跑完後，重新讀 SN 的歷史紀錄。如果有可顯示的結果就 emit
        讓 UI 渲染；沒有的話清 snapshot 讓 reset flags 按鈕灰掉。
        """
        info, latest_path, run_dir = self._history.load_latest_run_info(sn)
        if info and run_dir and run_dir.exists():
            status = (info.get("status") or "").lower()
            if status == "finished_pass":
                self.finished_run_available.emit(
                    FinishedRunPayload(
                        sn=sn,
                        run_dir=run_dir,
                        info=info,
                        mapped_status=_FINISHED_STATUS_MAP[status],
                    )
                )
                # 不管 PASS / FAIL / STOPPED，都記下 snapshot 讓 reset flags
                # 有對象可清，保持按鈕狀態跟實際可操作性一致
                self._set_snapshot({
                    "sn": sn,
                    "latest_path": latest_path,
                    "run_dir": run_dir,
                    "status": status,
                })
                return

        self._set_snapshot(None)
        if stopped:
            self.status_changed.emit(Status.STOPPED)
        elif final_status:
            self.status_changed.emit(final_status)

    # ---- snapshot helpers ---------------------------------------------------

    def _set_snapshot(self, snapshot: Optional[dict[str, Any]]) -> None:
        self._snapshot = snapshot
        self.snapshot_available_changed.emit(snapshot is not None)
