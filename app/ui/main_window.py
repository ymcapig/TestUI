"""
MainWindow：主畫面的 QMainWindow。

職責範圍：
- 承載 widget（layout、setup）
- 接收 Qt 事件（按鈕點擊、表格互動）
- 顯示狀態（更新 label、table、log）

所有「決策邏輯」都不在這個檔案：
- 歷史紀錄相關決策 → RunController
- 測試執行的流程協調 → RunController
- ignore / note 規則 → StepRulesService
- 按鈕 enable/disable 規則 → UiStateManager
- 啟動流程 → StartupCoordinator
- ini 解析 → ConfigService
- 平台 API → app.platform
"""

from __future__ import annotations

import configparser
import sys
from pathlib import Path
from typing import Any, Optional

from PyQt5 import QtWidgets, uic
from PyQt5.QtCore import QDir, QLockFile, QTimer
from PyQt5.QtGui import QIcon, QCursor
from PyQt5.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QHeaderView,
    QLabel,
    QMainWindow,
    QMessageBox,
    QSpinBox,
    QTableView,
    QTextEdit,
    QToolTip,
)

from app.i18n.localization import LocalizationManager
from app.models.steps_model import StepsModel, Col
from core.config_service import AppConfig, ConfigService
from core.events import Status, StepState
from core.flow_runner import FlowRunner

from app.platform.bios_sn import read_bios_sn
from app.platform.app_id import set_app_user_model_id
from app.platform.keyboard import LatinInputHelper
from app.platform.shell import open_folder_in_explorer
from app.services.run_history_service import RunHistoryService

from app.presenters.startup_coordinator import StartupCoordinator
from app.presenters.step_rules import StepRulesService
from app.presenters.ui_state_manager import UiStateManager, UiWidgets

from app.presenters.run_controller import (
    ClearFlagsResult,
    FinishedRunPayload,
    RunController,
    RunPlan,
    StartResult,
)

from app.ui.ui_components import NumericDelegate


class MainWindow(QMainWindow):
    def __init__(self, cfg_path: Path, force_debug: bool = False):
        super().__init__()

        # ---- 基本欄位 -------------------------------------------------------
        self.force_debug = force_debug
        self.cfg_path = Path(cfg_path)
        self.config_dir = self.cfg_path.resolve().parent
        self.project_root = (
            self.config_dir.parent
            if self.config_dir.name.lower() == "config"
            else self.config_dir
        )

        # ---- Services -------------------------------------------------------
        self.config_service = ConfigService(self.cfg_path)
        self.history_service = RunHistoryService(self.project_root)

        # ---- i18n -----------------------------------------------------------
        self._init_localization()

        # ---- window 基本屬性 ------------------------------------------------
        self.setWindowTitle(self._("window.title", default="NB TestUI"))
        self.resize(1000, 680)

        current_folder = Path(__file__).parent.resolve()
        ui_path = current_folder / "DIAG.ui"
        uic.loadUi(ui_path, self)

        # IDE 提示用（uic.loadUi 會動態塞 widget 上去）
        self.lbl_status: QLabel
        self.table: QTableView
        self.btn_stop
        self.btn_reset_flags
        self.btn_reload
        self.btn_up
        self.btn_down
        self.btn_save_ini
        self.btn_run_selected
        self.btn_run_from
        self.btn_toggle_ignore
        self.lbl_stress: QLabel
        self.btn_showlog
        self.txt_out: QTextEdit
        self.lbl_station: QLabel
        self.spin_stress: QSpinBox

        # ---- 狀態欄位 -------------------------------------------------------
        self.model: Optional[StepsModel] = None
        self.station_id = ""
        self.require_sn = True
        self.sn_default = "SN000"
        self.read_sn = 1
        self.debug_config: dict[str, str] = {}
        self.debug_mode = False
        self.debug_enabled = False
        self.config_load_failed = False
        self.debug_step_sequence: list[str] = []
        self._current_step_name: str = ""
        self._run_mode: str = "stop_on_fail"

        # ---- 子系統 ---------------------------------------------------------
        self.step_rules = StepRulesService(translator=self._)
        self.latin_input_helper = LatinInputHelper(self)
        self.startup_coordinator = StartupCoordinator(
            self, translator=self._, latin_input_helper=self.latin_input_helper
        )

        self.controller = RunController(
            self.cfg_path,
            translator=self._,
            debug_enabled_provider=lambda: self.debug_enabled,
            prepare_runner=self._prepare_runner,
            history_service=self.history_service,
            config_load_failed_provider=lambda: self.config_load_failed,
        )

        self.ui_state = UiStateManager(
            UiWidgets(
                status_label=self.lbl_status,
                table=self.table,
                stop_button=self.btn_stop,
                reset_flags_button=self.btn_reset_flags,
                stress_spin=self.spin_stress,
                stress_label=self.lbl_stress,
                debug_buttons=(
                    self.btn_reload,
                    self.btn_up,
                    self.btn_down,
                    self.btn_save_ini,
                    self.btn_run_selected,
                    self.btn_run_from,
                    self.btn_toggle_ignore,
                ),
            )
        )
        self.ui_state.controller_running_getter = lambda: self.controller.is_running

        # ---- 接線 -----------------------------------------------------------
        self._wire_controller_signals()
        self._setup_ui(current_folder)
        self.rebuild_model_from_config()
        self._update_station_label()

        QTimer.singleShot(0, self._handle_startup_flow)

    # ------------------------------------------------------------------------
    # 初始化
    # ------------------------------------------------------------------------

    def _init_localization(self) -> None:
        locale, fallback = self.config_service.read_localization_settings()
        if getattr(sys, "frozen", False):
            ext = Path(sys.executable).parent / "i18n"
            locales_dir = ext if ext.exists() else Path(sys._MEIPASS) / "i18n"
        else:
            locales_dir = self.project_root / "i18n"
        self.localization = LocalizationManager(locales_dir, locale, fallback)
        self._ = lambda key, **kwargs: self.localization.gettext(key, **kwargs)

    def _wire_controller_signals(self) -> None:
        self.controller.status_changed.connect(self.set_status)
        self.controller.step_started.connect(self.on_step_started)
        self.controller.step_finished.connect(self.on_step_finished)
        self.controller.log_line.connect(self.append_log)
        self.controller.run_state_changed.connect(self.ui_state.on_run_state_changed)
        self.controller.plan_finished.connect(self._on_plan_finished)
        # 新增：controller 要求渲染歷史紀錄 / snapshot 狀態變更
        self.controller.finished_run_available.connect(self._render_finished_run)
        self.controller.snapshot_available_changed.connect(
            self.ui_state.set_has_finished_snapshot
        )

    def _setup_ui(self, current_folder: Path) -> None:
        self.lbl_status.setText(Status.READY)
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setSelectionMode(QTableView.ExtendedSelection)
        self.table.doubleClicked.connect(self.on_table_double_clicked)
        self.table.setMouseTracking(True)
        self.table.entered.connect(self.on_table_hovered)

        self.btn_stop.setText(self._("button.stop", default="Stop"))
        self.btn_stop.clicked.connect(self.on_stop)
        self.btn_reset_flags.setText(self._("button.reset_flags", default="Reset Flags"))
        self.btn_reset_flags.clicked.connect(self.on_reset_flags)

        self.Debug.setText(self._("label.debug_section", default="Debug:"))
        self.btn_reload.setText(self._("button.reload", default="Reload"))
        self.btn_up.setText(self._("button.up", default="↑ Up"))
        self.btn_down.setText(self._("button.down", default="↓ Down"))
        self.btn_save_ini.setText(self._("button.save_ini", default="Save ini"))
        self.btn_run_selected.setText(self._("button.run_selected", default="Run Selected"))
        self.btn_run_from.setText(self._("button.run_from", default="Run From Here"))
        self.btn_toggle_ignore.setText(self._("button.toggle_ignore", default="Toggle Ignore"))
        self.lbl_stress.setText(self._("label.stress_count", default="Stress Count:"))

        self.btn_reload.clicked.connect(self.on_reload_config)
        self.btn_up.clicked.connect(lambda: self.on_reorder(-1))
        self.btn_down.clicked.connect(lambda: self.on_reorder(+1))
        self.btn_save_ini.clicked.connect(self.on_save_debug_order)
        self.btn_run_selected.clicked.connect(self.on_run_selected)
        self.btn_run_from.clicked.connect(self.on_run_from_here)
        self.btn_toggle_ignore.clicked.connect(self.on_toggle_ignore)

        self.btn_showlog.setText(self._("button.showlog_off", default="Show log: off"))
        self.btn_showlog.clicked.connect(self.toggle_log_visibility)

        self.txt_out.setPlaceholderText(self._("placeholder.output", default="Output..."))
        self.txt_out.hide()

        self.setWindowIcon(QIcon(str(current_folder / "input.ico")))
        self.ui_state.refresh()

    # ------------------------------------------------------------------------
    # Config 載入
    # ------------------------------------------------------------------------

    def rebuild_model_from_config(self) -> None:
        try:
            app_config = self.config_service.load_app_config()
        except configparser.DuplicateSectionError as exc:
            self._handle_config_duplicate_error(exc)
            return

        self.config_load_failed = False
        self.ui_state.run_locked = False
        self._apply_config(app_config)

    def _handle_config_duplicate_error(
        self, exc: configparser.DuplicateSectionError
    ) -> None:
        """偵測到 station.ini 有重複 section：彈窗告知後直接退出程式。"""
        duplicate_id = exc.section or ""
        lineno = getattr(exc, "lineno", None)
        if lineno:
            detail = self._(
                "dialog.config_duplicate_section.message_with_line",
                default="Duplicate test ID detected: {identifier}\nLine: {line}\nPlease fix and reload.",
                identifier=duplicate_id,
                line=lineno,
            )
        else:
            detail = self._(
                "dialog.config_duplicate_section.message",
                default="Duplicate test ID detected: {identifier}.",
                identifier=duplicate_id,
                line="",
            )
        QMessageBox.critical(
            self,
            self._("dialog.config_duplicate_section.title", default="Configuration Error"),
            detail,
        )

        app = QApplication.instance()
        if app is not None:
            app.quit()
        sys.exit(1)

    def _apply_config(self, app_config: AppConfig) -> None:
        try:
            self.localization.set_locale(app_config.locale, app_config.fallback_locale)
        except Exception:
            pass

        try:
            from core.config_service import load_config
            cfg = load_config(self.cfg_path)
            self._run_mode = cfg.get("run", "run_mode", fallback="stop_on_fail").strip()
        except Exception:
            self._run_mode = "stop_on_fail"

        self.step_rules.reset_defaults(app_config.step_ignore_defaults)

        steps = []
        for row in app_config.steps:
            sid = row["sid"]
            steps.append({
                "sid": sid,
                "name": row["name"],
                "ignore": self.step_rules.effective_ignore(sid),
            })

        # 先跟 ConfigService 拿 runtime（retry 等），避免 StepsModel 自己開檔案
        sids = [s["sid"] for s in steps]
        step_runtime = self.config_service.read_step_runtime(sids)
        step_limits = self.config_service.read_step_limits(sids)        # 新增
        self.model = StepsModel(
            steps,
            step_runtime=step_runtime,
            step_limits=step_limits,                                     # 新增
            translator=self._,
        )
        self.model.error_occurred.connect(self.show_warning_dialog)
        self.model.limit_changed.connect(self._on_limit_changed)         # 新增
        self.table.setModel(self.model)

        # 給兩個 limit 欄位掛數字限制器
        self.table.setItemDelegateForColumn(
            int(Col.LOWER_LIMIT), NumericDelegate(self.table)
        )
        self.table.setItemDelegateForColumn(
            int(Col.UPPER_LIMIT), NumericDelegate(self.table)
        )

        header = self.table.horizontalHeader()
        # 欄位：STEP, STATUS, LOWER, UPPER, IGNORE, TIME, ATTEMPTS, NOTe
        column_widths = [340, 180, 100, 100, 120, 100, 100, 40]
        for idx, width in enumerate(column_widths):
            if idx < self.model.columnCount():
                self.table.setColumnWidth(idx, width)
                header.setSectionResizeMode(idx, QHeaderView.Interactive)
        header.setStretchLastSection(True)

        sids = [row.sid for row in self.model.rows]
        self.step_rules.sync_notes_with_steps(sids)
        for sid in sids:
            self._render_step_note(sid)

        self.debug_step_sequence = list(sids)
        self.debug_config = dict(app_config.debug_config)
        self.debug_mode = bool(app_config.debug_mode) or self.force_debug
        self.station_id = app_config.station_id
        self.require_sn = bool(app_config.require_sn)
        self.read_sn = int(app_config.read_sn)
        self.sn_default = app_config.sn_default
        self._update_station_label()
        self.ui_state.refresh()

    # ------------------------------------------------------------------------
    # Controller signal 的 slot
    # ------------------------------------------------------------------------

    def _on_limit_changed(self, sid: str, key: str, value: str) -> None:
        """Model 報告使用者改了 limit，把它寫進 ini。"""
        ok = self.config_service.write_step_limit(sid, key, value)
        if not ok:
            # 寫失敗的話彈個訊息（用既有的 show_warning_dialog 機制）
            self.show_warning_dialog(
                self._("dialog.error.title", default="Error"),
                self._(
                    "dialog.error.write_limit_failed",
                    default="Failed to save {key} for {sid}",
                    key=key,
                    sid=sid,
                ),
            )

    def set_status(self, status: str) -> None:
        no_stop_mode = self._run_mode not in ("stop_on_fail", "stop_on_fail_retry")
        if status == Status.FAIL and no_stop_mode:
            display = status
        elif status in (Status.RUNNING, Status.FAIL) and self._current_step_name:
            display = f"{status}: {self._current_step_name}"
        else:
            display = status
        self.ui_state.apply_status(status, display_text=display)

    def append_log(self, line: str) -> None:
        self.txt_out.moveCursor(self.txt_out.textCursor().End)
        self.txt_out.insertPlainText(line)
        self.txt_out.moveCursor(self.txt_out.textCursor().End)

    def show_warning_dialog(self, title: str, message: str) -> None:
        QMessageBox.warning(self, title, message)

    def on_step_started(self, sid: str, attempt: int) -> None:

        self._current_step_name = ""
        if self.model:
            for row in self.model.rows:
                if row.sid == sid:
                    self._current_step_name = row.name
                    break
        
            self.set_status(Status.RUNNING)

        if self.model:
            self.model.set_status(sid, StepState.RUNNING, time_s=0, attempts=attempt)
        self._focus_step_row(sid)

    def on_step_finished(self, sid: str, res: dict) -> None:
        self.step_rules.set_run_note(sid, res.get("note"))
        if self.model:
            self.model.set_status(
                sid,
                res["state"],
                time_s=res.get("duration_s"),
                attempts=res.get("attempt"),
                note=self.step_rules.combined_note(sid),
            )
        self._focus_step_row(sid)

    def _prepare_runner(self, runner: FlowRunner, plan: RunPlan) -> None:
        self.txt_out.clear()
        for step in runner.steps:
            sid = f"{step.order}_{step.name}"
            step.ignore_result = self.step_rules.effective_ignore(sid)
        if self.model:
            if not plan.is_manual:
                self.model.apply_flags(runner.step_flags)
            self._render_all_notes()

    def _on_plan_finished(self, context: object) -> None:
        data = context or {}
        plan: Optional[RunPlan] = data.get("plan")
        stopped = bool(data.get("stopped"))
        self.ui_state.refresh()

        if plan and plan.is_manual:
            if stopped:
                self.append_log("\n--- Debug Run Stopped ---\n")
            else:
                self.append_log("\n--- Debug Run Completed ---\n")

    def _render_finished_run(self, payload: FinishedRunPayload) -> None:
        """
        收到 controller 說「有歷史紀錄可以畫了」，把 report 塞回 model，
        更新 status 與 log。這是純渲染，不做任何決策。
        """
        if self.model:
            self.model.reset_states()
        try:
            report_rows, report_file = self.history_service.load_report_rows(
                payload.sn, payload.run_dir, payload.info
            )
        except Exception as exc:
            self.append_log(f"[ERROR] load history report failed: {exc}\n")
            self.set_status(payload.mapped_status)
            return
        
        if report_rows:
            for row in report_rows:
                sid = row.get("step_id")
                if not sid:
                    continue
                state = row.get("state") or StepState.NOT_RUN
                duration = row.get("duration_s")
                attempt = row.get("attempt")
                note = (row.get("note") or "").strip()
                self._apply_row_result(sid, state, duration, attempt, note)

        self._render_all_notes()
        self.set_status(payload.mapped_status)

        summary = [
            self._(
                "log.previous_run_summary",
                default="Loaded previous run: SN={sn}, run_id={run_id}, status={status}",
                sn=payload.sn,
                run_id=payload.info.get("run_id"),
                status=payload.info.get("status"),
            )
        ]
        if report_file:
            summary.append(
                self._(
                    "log.previous_run_report",
                    default="Report file: {path}",
                    path=str(report_file),
                )
            )
        for line in summary:
            self.append_log(f"[INFO] {line}\n")

    def _apply_row_result(
        self,
        sid: str,
        state: str,
        duration: Optional[float],
        attempt: Optional[int],
        note: str,
    ) -> None:
        if not self.model:
            return
        self.step_rules.set_run_note(sid, note)
        self.model.set_status(
            sid, state, time_s=duration, attempts=attempt,
            note=self.step_rules.combined_note(sid),
        )

    # ------------------------------------------------------------------------
    # 渲染小工具
    # ------------------------------------------------------------------------

    def _render_step_note(self, sid: str) -> None:
        if not self.model:
            return
        for row in self.model.rows:
            if row.sid == sid:
                self.model.set_status(
                    sid, row.status, note=self.step_rules.combined_note(sid)
                )
                self.model.set_ignore(sid, self.step_rules.effective_ignore(sid))
                break

    def _render_all_notes(self) -> None:
        if not self.model:
            return
        for row in self.model.rows:
            self._render_step_note(row.sid)

    def _update_station_label(self) -> None:
        if self.station_id:
            text = self._(
                "label.station.with_id",
                default="Station: {station}",
                station=self.station_id,
            )
        else:
            text = self._("label.station.na", default="Station: N/A")
        self.lbl_station.setText(text)

    def _focus_step_row(self, sid: str) -> None:
        if not self.model:
            return
        for row_idx, row in enumerate(self.model.rows):
            if row.sid == sid:
                index = self.model.index(row_idx, 0)
                if index.isValid():
                    self.table.selectRow(row_idx)
                    self.table.scrollTo(index, QAbstractItemView.PositionAtCenter)
                break

    # ------------------------------------------------------------------------
    # Table 互動
    # ------------------------------------------------------------------------

    def on_table_double_clicked(self, index) -> None:
        if not index.isValid() or index.column() != 0 or not self.model:
            return
        sid = self.model.rows[index.row()].sid
        folder = self.config_service.get_step_work_folder(sid, self.project_root)
        if not folder:
            QMessageBox.information(self, "Open Folder", f"找不到 {sid} 對應的資料夾。")
            return
        if not folder.exists():
            QMessageBox.warning(self, "Open Folder", f"資料夾不存在：\n{folder}")
            return
        open_folder_in_explorer(folder)

    def on_table_hovered(self, index) -> None:
        if not index.isValid() or index.column() != 0 or not self.model:
            QToolTip.hideText()
            return
        sid = self.model.rows[index.row()].sid
        tool_name = self.config_service.get_step_call_target(sid)
        if not tool_name:
            QToolTip.hideText()
            return
        QToolTip.showText(QCursor.pos(), f"{tool_name}", self.table)

    # ------------------------------------------------------------------------
    # 基本按鈕 handler
    # ------------------------------------------------------------------------

    def on_stop(self) -> None:
        self.controller.stop()

    def on_reset_flags(self) -> None:
        result = self.controller.clear_flags()
        self._show_clear_flags_result(result)

    def _show_clear_flags_result(self, result: ClearFlagsResult) -> None:
        if result.outcome == "running":
            QMessageBox.warning(
                self,
                self._("dialog.reset_running.title", default="Reset Flags"),
                self._(
                    "dialog.reset_running.message",
                    default="A test is running. Stop it before resetting flags.",
                ),
            )
            return
        if result.outcome == "runner_cleared":
            # log 已經由 controller 發出，這裡不彈 dialog
            return
        if result.outcome == "snapshot_cleared":
            QMessageBox.information(
                self,
                self._("dialog.reset_cleared.title", default="Reset Flags"),
                self._(
                    "dialog.reset_cleared.message",
                    default="Cleared latest run snapshot for SN {sn}.",
                    sn=result.sn,
                ),
            )
            self._reset_run_ui_state()
            return
        if result.outcome == "none":
            QMessageBox.information(
                self,
                self._("dialog.reset_none.title", default="Reset Flags"),
                self._(
                    "dialog.reset_none.message",
                    default="No finished run is available to reset.",
                ),
            )
            return
        if result.outcome == "error":
            QMessageBox.critical(
                self,
                self._("dialog.clear_failed.title", default="Reset Flags"),
                self._(
                    "dialog.clear_failed.message",
                    default="Failed to clear flags: {error}",
                    error=result.error,
                ),
            )

    def _reset_run_ui_state(self) -> None:
        self.step_rules.clear_notes()
        if self.model:
            self.model.reset_states()
            self._render_all_notes()
        self.table.clearSelection()
        self.txt_out.clear()
        self.set_status(Status.READY)

    def toggle_log_visibility(self) -> None:
        if self.txt_out.isVisible():
            self.txt_out.hide()
            self.splitter.setSizes([0, 1])
            self.table.horizontalHeader().setSectionResizeMode(
                QtWidgets.QHeaderView.Interactive
            )
            self.table.horizontalHeader().setStretchLastSection(True)
            self.btn_showlog.setText(self._("button.showlog_off", default="Show log: off"))
        else:
            self.txt_out.show()
            self.splitter.setSizes([400, 600])
            self.btn_showlog.setText(self._("button.showlog_on", default="Show log: on"))

    # ------------------------------------------------------------------------
    # 啟動流程
    # ------------------------------------------------------------------------

    def _handle_startup_flow(self) -> None:
        # 開啟時先檢查失敗旗標:若上次的 FactoryTestUI.flg 還在，提示先刪除，UI 停住不跑測項
        # 先載入並顯示上次的測試結果(像 stop_on_fail 那樣)，再彈窗提示刪除，UI 停住不跑測項。
        if Path(r"C:\Diag\FLAG\FactoryTestUI.flg").exists():
            try:
                sn = read_bios_sn(self.read_sn) or self.sn_default
                self.controller._check_prior_run(sn)   # 顯示上次結果到 UI
            except Exception:
                pass
            QMessageBox.warning(
                self,
                "偵測到失敗旗標",
                "偵測到 C:\\Diag\\FLAG\\FactoryTestUI.flg。\n"
                "請先刪除此旗標，再重新開啟本程式。",
            )
            return   # 不繼續啟動流程，UI 停住(但已顯示上次結果)
        
        if self.read_sn not in (1, 2, 3):
            self.set_status(Status.FAIL)
            QMessageBox.critical(
                self,
                "設定檔錯誤",
                f"station.ini 中的讀取序號模式 (read_sn) 設定錯誤！\n"
                f"當前數值為：{self.read_sn}\n\n"
                f"請設定為：\n"
                f"1 = 從檔案讀取系統 SN \n"
                f"2 = 透過 OS 讀取系統 SN \n"
                f"3 = 透過 OS 讀取主機板 SN\n\n"
                f"請修改 station.ini 後重新啟動。"
            )
            QApplication.quit()
            return

        # debug 模式
        if self.debug_mode:
            self.startup_coordinator.show_debug_mode_notice()
            self.startup_coordinator.tag_debug_title()
            self.debug_enabled = True
            self.ui_state.set_debug_enabled(True)

            # 讀取 SN
            sn = read_bios_sn(self.read_sn)
            if not sn:
                # 若讀取失敗，則使用預設的 SN
                sn = self.sn_default
                QMessageBox.warning(
                    self,
                    self._("dialog.bios_sn_failed.title", default="SN Read Failed"),
                    self._(
                        "dialog.bios_sn_failed.fallback",
                        default="Failed to read SN from BIOS. Using ini default: {sn}",
                        sn=sn,
                    ),
                )
            self.sn = sn
            return
        
        # 非 debug 模式
        self._auto_start_sequence()

    def _auto_start_sequence(self) -> None:
        # # 舊版讀 SN 的方式
        # if self.require_sn:
        #     sn = self.startup_coordinator.prompt_sn()
        #     if sn is None:
        #         return
        # else:
        #     sn = self.sn_default

        # 新版讀 SN 的方式
        sn = read_bios_sn(self.read_sn)
        if not sn:
            QMessageBox.critical(
                self,
                self._("dialog.bios_sn_failed.title", default="SN Read Failed"),
                self._(
                    "dialog.bios_sn_failed.message",
                    default="Failed to read SN from BIOS. Cannot continue testing.",
                ),
            )
            QApplication.quit()
            return
        
        if not self.cfg_path.exists():
            QMessageBox.critical(self, "Error", "Could not find station.ini!\n\nTest will FAIL.")
        
        self.start_full_run(sn)

    def start_full_run(self, sn: Optional[str] = None) -> None:
        sn = (sn or self.sn_default or "").strip()
        if self.require_sn and not sn:
            QMessageBox.warning(
                self,
                self._("dialog.missing_sn.title", default="Missing Serial Number"),
                self._(
                    "dialog.missing_sn.message",
                    default="Please enter the DUT serial number.",
                ),
            )
            return
        if not self.require_sn and not sn:
            sn = self.sn_default

        plan = RunPlan(sn=sn, loops=1, continue_on_fail=False)
        result = self.controller.start(plan)
        self._handle_start_result(result)

    def _handle_start_result(self, result: StartResult) -> None:
        if result.started:
            self.txt_out.clear()
            self.ui_state.refresh()
            return

        if result.blocked_reason == "config_load_failed":
            QMessageBox.critical(
                self,
                self._("dialog.config_load_failed.title", default="Configuration Error"),
                self._(
                    "dialog.config_load_failed.message",
                    default="Configuration failed to load. Please fix duplicate test IDs first.",
                ),
            )
            return
        if result.blocked_reason == "already_running":
            self._warn_test_in_progress()
            return
        if result.blocked_reason == "prior_pass":
            QMessageBox.information(
                self,
                self._("dialog.finished_pass_required.title", default="Reset Required"),
                self._(
                    "dialog.finished_pass_required.message",
                    default="SN {sn} had a previous PASS result (status: {status}). "
                            "Reset flags before testing again.",
                    sn=result.prior_sn,
                    status=result.prior_status_label,
                ),
            )
            return

    def _warn_test_in_progress(self) -> None:
        QMessageBox.warning(
            self,
            self._("dialog.test_in_progress.title", default="Test In Progress"),
            self._(
                "dialog.test_in_progress.message",
                default="A test is currently running. Please stop it or wait until it finishes.",
            ),
        )

    # ========================================================================
    # ---- Debug Mode Actions ------------------------------------------------
    # ========================================================================
    # 以下全部只在 debug_enabled 時才可用，是 debug 按鈕群組對應的 handler。
    # 按鈕的 enable/disable 由 UiStateManager 統一管理。

    def on_reload_config(self) -> None:
        """重新讀取 station.ini 並套用。ini 格式錯誤時保留現有狀態不覆蓋。"""
        if not self._can_debug_act():
            return
        try:
            new_config = self.config_service.load_app_config()
        except configparser.DuplicateSectionError as exc:
            # 有重複 section，重用現有的錯誤顯示邏輯（但不覆蓋當前狀態）
            duplicate_id = exc.section or ""
            lineno = getattr(exc, "lineno", None)
            if lineno:
                detail = self._(
                    "dialog.config_duplicate_section.message_with_line",
                    default="Duplicate test ID detected: {identifier}\nLine: {line}\nPlease fix and reload.",
                    identifier=duplicate_id,
                    line=lineno,
                )
            else:
                detail = self._(
                    "dialog.config_duplicate_section.message",
                    default="Duplicate test ID detected: {identifier}.",
                    identifier=duplicate_id,
                    line="",
                )
            QMessageBox.critical(
                self,
                self._("dialog.config_duplicate_section.title", default="Configuration Error"),
                detail,
            )
            return
        except Exception as exc:
            QMessageBox.critical(
                self,
                self._("dialog.reload_failed.title", default="Reload Failed"),
                self._(
                    "dialog.reload_failed.message",
                    default="Failed to reload {filename}:\n{error}", 
                    error=exc, 
                    filename=self.cfg_path.name,
                ),
            )
            return

        self._apply_config(new_config)
        QMessageBox.information(
            self,
            self._("dialog.reload_success.title", default="Reload"),
            self._(
                "dialog.reload_success.message", 
                default="{filename} reloaded.",
                filename=self.cfg_path.name,),
        )

    def on_reorder(self, delta: int) -> None:
        """上下移動選中的 step。"""
        if not self._can_debug_act():
            return
        sel = self.table.selectionModel().selectedRows()
        if not sel:
            return
        row = sel[0].row()
        new_row = max(0, min(self.model.rowCount() - 1, row + delta))
        if new_row == row:
            return
        self.table.blockSignals(True)
        self.model.beginResetModel()
        self.model.rows.insert(new_row, self.model.rows.pop(row))
        for i, record in enumerate(self.model.rows, start=1):
            record.idx = i
        self.model.endResetModel()
        self.debug_step_sequence = [record.sid for record in self.model.rows]
        self.table.selectRow(new_row)
        self.table.blockSignals(False)

    def on_run_selected(self) -> None:
        """跑選中的 step。"""
        if not self._can_debug_act():
            return
        sel = self.table.selectionModel().selectedRows()
        if not sel:
            return
        ordered_rows = sorted(sel, key=lambda idx: idx.row())
        sids = [self.model.rows[item.row()].sid for item in ordered_rows]
        self._start_manual_run(sids)

    def on_run_from_here(self) -> None:
        """從選中的 step 開始往下跑到底。"""
        if not self._can_debug_act():
            return
        sel = self.table.selectionModel().selectedRows()
        if not sel:
            return
        start_row = sel[0].row()
        sids = [self.model.rows[i].sid for i in range(start_row, self.model.rowCount())]
        self._start_manual_run(sids)

    def on_toggle_ignore(self) -> None:
        """切換選中 step 的 ignore 覆寫。"""
        if not self._can_debug_act():
            return
        sel = self.table.selectionModel().selectedRows()
        if not sel:
            return
        sid = self.model.rows[sel[0].row()].sid
        self.step_rules.toggle_override(sid)
        self._render_step_note(sid)

    def on_save_debug_order(self) -> None:
        """把當前 step 順序寫回 station.ini。"""
        if self.controller.is_running:
            return
        if not self.debug_enabled:
            QMessageBox.warning(
                self,
                self._("dialog.debug_save_requires_mode.title", default="Debug Mode"),
                self._(
                    "dialog.debug_save_requires_mode.message",
                    default="Enable debug mode before saving the sequence.",
                ),
            )
            return
        if not self.model or not self.model.rows:
            QMessageBox.information(
                self,
                self._("dialog.debug_save_empty.title", default="Debug Mode"),
                self._("dialog.debug_save_empty.message", default="There are no steps to save."),
            )
            return
        try:
            # save_step_order 吃的是 dict list（key "sid"），轉一下格式
            row_dicts = [{"sid": r.sid} for r in self.model.rows]
            sid_mapping = self.config_service.save_step_order(row_dicts)
        except Exception as exc:
            QMessageBox.critical(
                self,
                self._("dialog.debug_save_error.title", default="Save Failed"),
                self._(
                    "dialog.debug_save_error.message",
                    default="Failed to write configuration: {error}",
                    error=exc,
                ),
            )
            return

        self.step_rules.remap_for_sid_change(sid_mapping)
        self.debug_step_sequence = [
            sid_mapping.get(sid, sid) for sid in self.debug_step_sequence
        ]
        self.rebuild_model_from_config()
        QMessageBox.information(
            self,
            self._("dialog.debug_save_success.title", default="Debug Mode"),
            self._(
                "dialog.debug_save_success.message",
                default="Test order saved to configuration.",
            ),
        )

    # ---- Debug 共用輔助 ------------------------------------------------------

    def _can_debug_act(self) -> bool:
        return (
            not self.controller.is_running
            and self.debug_enabled
            and self.model is not None
        )

    def _start_manual_run(self, sids: list[str]) -> None:
        if self.config_load_failed or not sids:
            return
        if self.controller.is_running:
            self._warn_test_in_progress()
            return

        plan = RunPlan(
            sn=self.sn,
            selected_sids=sids,
            manual_tag=self._("tag.manual", default="(manual)"),
            loops=max(1, self.spin_stress.value()),
            continue_on_fail=True,
        )
        self.txt_out.clear()
        result = self.controller.start(plan)
        self._handle_start_result(result)

    def closeEvent(self, event):
        """關閉視窗時，若測試仍在進行(latest_run 尚未結束)，標記為 aborted，
        讓外部 monitor 能分辨『UI 中途被關』而非正常結束。
        只在 running/pending 時改，不覆蓋已結束結果。
        """
        try:
            self.history_service.mark_any_running_aborted("aborted")
        except Exception as e:
            pass
        super().closeEvent(event)

# 啟動 UI
def launch(cfg_path: Path, force_debug: bool = False) -> None:
    app = QApplication(sys.argv)

    lock_file_path = str(Path(QDir.tempPath()) / "NB_TestUI_Instance.lock")
    lock_file = QLockFile(lock_file_path)

    set_app_user_model_id("mycompany.factorytool.testui.v1")
    lock_file.setStaleLockTime(0)

    if not lock_file.tryLock(100):
        QMessageBox.warning(
            None,
            "程式已執行 (Already Running)",
            "NB TestUI 已經在執行中，請勿重複開啟。\nAnother instance is already running.",
        )
        sys.exit(1)

    window = MainWindow(cfg_path, force_debug=force_debug)

    # 例外崩潰保險網:未捕捉的 Python 例外導致崩潰前，先把未完成的測試標記 aborted，
    # 讓外部 monitor 分辨出是異常跳出(closeEvent 接不到崩潰，這裡補上)。
    _prev_hook = sys.excepthook

    def _excepthook(exc_type, exc_value, exc_tb):
        try:
            sn = getattr(window, "sn", None)
            if sn and window.controller.is_running:
                window.history_service.mark_aborted(sn, "aborted")
        except Exception:
            pass
        _prev_hook(exc_type, exc_value, exc_tb)

    sys.excepthook = _excepthook
    
    window.showMaximized()
    sys.exit(app.exec_())