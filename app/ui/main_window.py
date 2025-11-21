import sys, base64, ctypes, json
from pathlib import Path
from PyQt5.QtWidgets import (QMainWindow, QWidget, QLabel, QLineEdit, QPushButton, 
    QVBoxLayout, QHBoxLayout, QTableView, QTextEdit, QMessageBox, QInputDialog, QMenu, QApplication, QSplitter, QHeaderView, QSpinBox, QAbstractItemView)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QEvent
from core.flow_runner import FlowRunner
from core.events import Status, StepState
from app.models.steps_model import StepsModel
from app.i18n.localization import LocalizationManager
import configparser

class RunnerThread(QThread):
    status_changed = pyqtSignal(str)
    step_started = pyqtSignal(str)
    step_finished = pyqtSignal(str, dict)
    log_line = pyqtSignal(str)

    def __init__(self, runner: FlowRunner):
        super().__init__()
        self.runner = runner
        self.runner.on_status_changed = lambda s: self.status_changed.emit(s)
        self.runner.on_step_started = lambda sid: self.step_started.emit(sid)
        self.runner.on_step_finished = lambda sid, res: self.step_finished.emit(sid, {
            "state": res.state,
            "duration_s": res.duration_s,
            "attempt": res.attempt,
            "note": res.note,
        })
        self.runner.on_log_line = lambda t: self.log_line.emit(t)

    def run(self):
        self.runner.run_all()

class MainWindow(QMainWindow):
    def __init__(self, cfg_path: Path):
        super().__init__()

        self.cfg_path = cfg_path
        self.config_dir = self.cfg_path.resolve().parent
        self.project_root = self.config_dir.parent if self.config_dir.name.lower() == "config" else self.config_dir
        self._init_localization()

        self.setWindowTitle(self._("window.title", default="NB TestUI"))
        self.resize(1000, 680)

        self.debug_enabled = False
        self.debug_mode = False
        self.runner_thread = None
        self.runner = None
        self.step_ignore_defaults = {}
        self.ignore_overrides = {}
        self.debug_step_sequence = []
        self.manual_threads = []
        self.station_id = ""
        self.require_sn = True
        self.sn_default = "SN000"
        self.debug_config = {}
        self.config_load_failed = False
        self._stress_total = 1
        self._stress_runs_remaining = 0
        self._stress_stop_requested = False
        self._stress_current_run = 0
        self._queued_sn = ""
        self._latin_inputs = set()
        self._latest_run_snapshot = None

        # Top status
        self.lbl_station = QLabel("")
        self.lbl_station.setAlignment(Qt.AlignCenter)
        self.lbl_station.setStyleSheet("font-size:26px; font-weight:bold; padding:8px; background:#fff59f; border:1px solid #f9a825;")
        self.lbl_station.setMinimumHeight(85)
        self.lbl_status = QLabel(Status.READY)
        self.lbl_status.setAlignment(Qt.AlignCenter)
        self.lbl_status.setMinimumHeight(200)
        self.lbl_status.setStyleSheet("font-size:60px; font-weight:bold; background:#e0e0e0; padding:24px;")
        
        # Login row
        top = QWidget()
        tl = QHBoxLayout(top)
        self.ed_sn = QLineEdit()
        self._prepare_latin_input(self.ed_sn)
        self.ed_sn.setPlaceholderText(self._("placeholder.dut_sn", default="DUT SN"))
        self.btn_start = QPushButton(self._("button.start", default="Start Test"))
        self.btn_start.clicked.connect(self.on_start)
        tl.addWidget(QLabel(self._("label.dut_sn", default="DUT SN:")))
        tl.addWidget(self.ed_sn)
        tl.addWidget(self.btn_start)

        # Table
        self.table = QTableView()
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setSelectionMode(QTableView.ExtendedSelection)
        self.model = None

        # Controls
        ctrl = QWidget()
        cl = QHBoxLayout(ctrl)
        self.btn_stop = QPushButton(self._("button.stop", default="Stop"))
        self.btn_stop.clicked.connect(self.on_stop)
        self.btn_reset_flags = QPushButton(self._("button.reset_flags", default="Reset Flags"))
        self.btn_reset_flags.setEnabled(False)
        self.btn_reset_flags.clicked.connect(self.on_reset_flags)
        self.btn_up = QPushButton(self._("button.up", default="↑ Up"))
        self.btn_down = QPushButton(self._("button.down", default="↓ Down"))
        self.btn_save_ini = QPushButton(self._("button.save_ini", default="Save ini"))
        self.btn_run_selected = QPushButton(self._("button.run_selected", default="Run Selected"))
        self.btn_run_from = QPushButton(self._("button.run_from", default="Run From Here"))
        self.btn_toggle_ignore = QPushButton(self._("button.toggle_ignore", default="Toggle Ignore"))
        self.lbl_stress = QLabel(self._("label.stress_count", default="Stress Count:"))
        self.spin_stress = QSpinBox()
        self.spin_stress.setRange(1, 1000)
        self.spin_stress.setValue(1)
        self.spin_stress.setEnabled(False)
        self.lbl_stress.setVisible(False)
        self.spin_stress.setVisible(False)
        for b in [self.btn_up, self.btn_down, self.btn_save_ini, self.btn_run_selected, self.btn_run_from, self.btn_toggle_ignore]:
            b.setEnabled(False)
        self.btn_up.clicked.connect(lambda: self.reorder(-1))
        self.btn_down.clicked.connect(lambda: self.reorder(+1))
        self.btn_save_ini.clicked.connect(self.save_debug_order)
        self.btn_run_selected.clicked.connect(self.run_selected)
        self.btn_run_from.clicked.connect(self.run_from_here)
        self.btn_toggle_ignore.clicked.connect(self.toggle_ignore)
        cl.addWidget(self.btn_stop)
        cl.addWidget(self.btn_reset_flags)
        cl.addStretch(1)
        cl.addWidget(QLabel(self._("label.debug_section", default="Debug:")))
        cl.addWidget(self.btn_up)
        cl.addWidget(self.btn_down)
        cl.addWidget(self.btn_save_ini)
        cl.addWidget(self.btn_run_selected)
        cl.addWidget(self.btn_run_from)
        cl.addWidget(self.btn_toggle_ignore)
        cl.addWidget(self.lbl_stress)
        cl.addWidget(self.spin_stress)

        # Output
        self.txt_out = QTextEdit()
        self.txt_out.setReadOnly(True)
        self.txt_out.setPlaceholderText(self._("placeholder.output", default="Output..."))
        self.splitter = QSplitter(Qt.Horizontal)
        self.splitter.addWidget(self.txt_out)
        self.splitter.addWidget(self.table)
        self.splitter.setStretchFactor(0, 2)
        self.splitter.setStretchFactor(1, 3)
        self.splitter.setSizes([520, 440])

        # Central layout
        cw = QWidget()
        lay = QVBoxLayout(cw)
        lay.addWidget(self.lbl_station)
        lay.addWidget(self.lbl_status)
        lay.addWidget(top)
        lay.addWidget(self.splitter, 1)
        lay.addWidget(ctrl)
        self.setCentralWidget(cw)

        # Load config
        self.rebuild_model_from_config()
        self._update_station_label()
        QTimer.singleShot(0, self._handle_startup_flow)

    def _init_localization(self):
        locale = "zh-TW"
        fallback = "en-US"
        cfg = configparser.ConfigParser(strict=False)
        cfg.optionxform = str
        try:
            cfg.read(self.cfg_path, encoding="utf-8-sig")
            locale = cfg.get("ui", "locale", fallback=locale).strip() or locale
            fallback = cfg.get("ui", "fallback_locale", fallback=fallback).strip() or fallback
        except Exception:
            locale = "zh-TW"
            fallback = "en-US"
        locales_dir = self.project_root / "i18n"
        self.localization = LocalizationManager(locales_dir, locale, fallback)
        self._ = lambda key, **kwargs: self.localization.gettext(key, **kwargs)

    def rebuild_model_from_config(self):
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        try:
            cfg.read(self.cfg_path, encoding="utf-8-sig")
        except configparser.DuplicateSectionError as exc:
            duplicate_id = exc.section or ""
            lineno = getattr(exc, "lineno", None)
            detail = self._(
                "dialog.config_duplicate_section.message",
                default="Duplicate test ID detected: {identifier}.",
                identifier=duplicate_id,
                line=lineno if lineno else "",
            )
            if lineno:
                detail = self._(
                    "dialog.config_duplicate_section.message_with_line",
                    default="Duplicate test ID detected: {identifier}\nLine: {line}\nPlease fix and reload.",
                    identifier=duplicate_id,
                    line=lineno,
                )
            QMessageBox.critical(
                self,
                self._("dialog.config_duplicate_section.title", default="Configuration Error"),
                detail,
            )
            self.step_ignore_defaults = {}
            self.model = StepsModel([], translator=self._)
            self.table.setModel(self.model)
            self.station_id = ""
            self._update_station_label()
            self.config_load_failed = True
            return
        self.config_load_failed = False
        try:
            locale_value = cfg.get("ui", "locale", fallback=self.localization.primary).strip()
            fallback_value = cfg.get("ui", "fallback_locale", fallback=self.localization.fallback or "en-US").strip()
            self.localization.set_locale(locale_value or self.localization.primary, fallback_value or self.localization.fallback)
        except Exception:
            pass
        self.step_ignore_defaults = {}
        steps = []
        for sec in cfg.sections():
            if sec.startswith("step."):
                order_name = sec[5:]
                try:
                    order_str, name = order_name.split("_",1)
                    order = int(order_str)
                except Exception: continue
                sid = f"{order}_{name}"
                self.step_ignore_defaults[sid] = cfg[sec].getboolean("ignore_result", False)
                steps.append({"sid": sid, "name": name})
        steps = sorted(steps, key=lambda x: int(x["sid"].split("_",1)[0]))
        self.ignore_overrides = {sid: val for sid, val in self.ignore_overrides.items() if sid in self.step_ignore_defaults}
        for row in steps:
            row["ignore"] = self._effective_ignore(row["sid"])
        self.model = StepsModel(steps, translator=self._)
        self.table.setModel(self.model)
        header = self.table.horizontalHeader()
        column_widths = [300, 200, 140, 100, 100, 240]
        for idx, width in enumerate(column_widths):
            if idx < self.model.columnCount():
                self.table.setColumnWidth(idx, width)
                header.setSectionResizeMode(idx, QHeaderView.Interactive)

        for row in self.model.rows:
            self._refresh_step_note(row["sid"])
        self.debug_step_sequence = [row["sid"] for row in self.model.rows]
        if cfg.has_section("debug"):
            self.debug_config = cfg["debug"]
            self.debug_mode = cfg.getboolean("debug", "debug_mode", fallback=False)
        else:
            self.debug_config = {}
            self.debug_mode = False
        self.station_id = cfg.get("meta", "station_id", fallback="").strip()
        self.require_sn = cfg.getboolean("run", "require_sn", fallback=True)
        self.sn_default = cfg.get("run", "sn_default", fallback="SN000").strip()
        self._update_station_label()

    # ---------- Actions ----------
    def set_status(self, s):
        self.lbl_status.setText(s)
        color = {
            Status.READY: "#e0e0e0",
            Status.RUNNING: "#90caf9",
            Status.PAUSED: "#ffe082",
            Status.STOPPED: "#ef9a9a",
            Status.PASS: "#a5d6a7",
            Status.FAIL: "#ef9a9a",
        }.get(s, "#e0e0e0")
        self.lbl_status.setStyleSheet(f"font-size:60px; font-weight:bold; background:{color}; padding:24x;")

    def _focus_step_row(self, sid: str) -> None:
        if not self.model:
            return
        for row_idx, row in enumerate(self.model.rows):
            if row.get("sid") == sid:
                index = self.model.index(row_idx, 0)
                if index.isValid():
                    self.table.selectRow(row_idx)
                    self.table.scrollTo(index, QAbstractItemView.PositionAtCenter)
                break

    def _update_station_label(self) -> None:
        if self.station_id:
            text = self._("label.station.with_id", default="Station: {station}", station=self.station_id)
        else:
            text = self._("label.station.na", default="Station: N/A")
        self.lbl_station.setText(text)

    def on_start(self):
        if self.config_load_failed:
            QMessageBox.critical(
                self,
                self._("dialog.config_load_failed.title", default="Configuration Error"),
                self._("dialog.config_load_failed.message", default="Configuration failed to load. Please fix duplicate test IDs first."),
            )
            return
        if self.runner_thread and self.runner_thread.isRunning():
            QMessageBox.warning(
                self,
                self._("dialog.test_in_progress.title", default="Test In Progress"),
                self._("dialog.test_in_progress.message", default="A test is currently running. Please stop it or wait until it finishes."),
            )
            return
        sn = self.ed_sn.text().strip()
        if not self.debug_enabled:
            if self.require_sn and not sn:
                QMessageBox.warning(
                    self,
                    self._("dialog.missing_sn.title", default="Missing Serial Number"),
                    self._("dialog.missing_sn.message", default="Please enter the DUT serial number."),
                )
                return
            if not self.require_sn and not sn:
                sn = self.sn_default
        else:
            if not sn:
                sn = self.sn_default
        self.ed_sn.setText(sn)
        if self._handle_finished_run(sn):
            return
        self._queued_sn = sn
        stress_total = self.spin_stress.value() if self.debug_enabled else 1
        self._stress_total = max(1, stress_total)
        self._stress_runs_remaining = self._stress_total
        self._stress_stop_requested = False
        self._stress_current_run = 0
        self.reset_run_ui_state()
        self.btn_start.setEnabled(False)
        if self.debug_enabled:
            self.spin_stress.setEnabled(False)
        self._start_next_run()

    def on_step_started(self, sid):
        self.model.set_status(sid, StepState.RUNNING)
        self._focus_step_row(sid)

    def on_step_finished(self, sid, res):
        self.model.set_status(sid, res["state"], time_s=res["duration_s"], attempts=res["attempt"], note=res["note"])
        self._focus_step_row(sid)

    def append_log(self, line):
        self.txt_out.moveCursor(self.txt_out.textCursor().End)
        self.txt_out.insertPlainText(line)
        self.txt_out.moveCursor(self.txt_out.textCursor().End)



    def on_stop(self):
        self._stress_stop_requested = True
        if self.runner:
            self.runner.stop()

    def on_reset_flags(self):
        if self.runner_thread and self.runner_thread.isRunning():
            QMessageBox.warning(
                self,
                self._("dialog.reset_running.title", default="Reset Flags"),
                self._("dialog.reset_running.message", default="A test is running. Stop it before resetting flags."),
            )
            return
        if self.runner:
            try:
                self.runner.reset_step_flags()
            except Exception as exc:
                QMessageBox.critical(
                    self,
                    self._("dialog.reset_failed.title", default="Reset Flags"),
                    self._("dialog.reset_failed.message", default="Failed to reset flags: {error}", error=exc),
                )
                return
            if self.model:
                self.model.reset_states()
                for row in self.model.rows:
                    self._refresh_step_note(row["sid"])
            self.append_log(f"[INFO] {self._('log.reset_flags_cleared', default='Step result flags cleared.')}\n")
            return
        # No active runner, fall back to clearing captured snapshot
        if not self._latest_run_snapshot:
            QMessageBox.information(
                self,
                self._("dialog.reset_none.title", default="Reset Flags"),
                self._("dialog.reset_none.message", default="No finished run is available to reset."),
            )
            return
        snapshot = self._latest_run_snapshot
        sn = snapshot.get("sn")
        latest_path = snapshot.get("latest_path")
        run_dir = snapshot.get("run_dir")
        try:
            if latest_path and latest_path.exists():
                latest_path.unlink()
            if run_dir and run_dir.exists():
                for state_file in run_dir.glob("steps/*/state.json"):
                    try:
                        state_file.unlink()
                    except Exception:
                        pass
        except Exception as exc:
            QMessageBox.critical(
                self,
                self._("dialog.clear_failed.title", default="Reset Flags"),
                self._("dialog.clear_failed.message", default="Failed to clear flags: {error}", error=exc),
            )
            return
        self.append_log(f"[INFO] {self._('log.latest_snapshot_cleared', default='Cleared latest run snapshot for SN {sn}.', sn=sn)}\n")
        QMessageBox.information(
            self,
            self._("dialog.reset_cleared.title", default="Reset Flags"),
            self._("dialog.reset_cleared.message", default="Cleared latest run snapshot for SN {sn}.", sn=sn),
        )
        self.reset_run_ui_state()

    def _start_next_run(self):
        if self._stress_stop_requested or self._stress_runs_remaining <= 0:
            self._finalize_stress()
            return
        self.txt_out.clear()
        self._stress_runs_remaining -= 1
        self._stress_current_run += 1
        sn = self._queued_sn
        self.runner = FlowRunner(self.cfg_path, sn, self.debug_enabled)
        self._apply_runner_overrides(self.runner, apply_order=True)
        if self.model:
            self.model.apply_flags(self.runner.step_flags)
            for row in self.model.rows:
                self._refresh_step_note(row["sid"])
        self.btn_reset_flags.setEnabled(True)
        if getattr(self.runner, "resumed", False):
            self.append_log(f"[INFO] {self._('log.resuming_run', default='Resuming run {run_id} for SN {sn}', run_id=self.runner.ts, sn=sn)}\n")
        self.runner_thread = RunnerThread(self.runner)
        self.runner_thread.status_changed.connect(self.set_status)
        self.runner_thread.step_started.connect(self.on_step_started)
        self.runner_thread.step_finished.connect(self.on_step_finished)
        self.runner_thread.log_line.connect(self.append_log)
        self.runner_thread.finished.connect(self.on_runner_finished)
        if self._stress_total > 1:
            self.append_log(
                f"\n{self._('log.stress_progress', default='--- Stress run {current}/{total} ---', current=self._stress_current_run, total=self._stress_total)}\n"
            )
        self.runner_thread.start()

    def _finalize_stress(self):
        self._stress_runs_remaining = 0
        self._stress_stop_requested = False
        self._stress_current_run = 0
        self.runner_thread = None
        self.runner = None
        self.btn_reset_flags.setEnabled(False)
        if self.debug_enabled:
            self.spin_stress.setEnabled(True)
        else:
            self.spin_stress.setValue(1)
        self.btn_start.setEnabled(True)

    def _run_single_step(self, sid, manual=False):
        manual_note = self._("tag.manual", default="(manual)") if manual else None
        self._execute_steps([sid], manual_note)

    # Debug / Startup Flow
    def _handle_startup_flow(self):
        if self.debug_mode:
            success = self._prompt_debug_password()
            if not success:
                self.debug_enabled = False
                self._set_debug_controls_enabled(False)
        else:
            self._auto_start_sequence()

    def _auto_start_sequence(self):
        def prompt(field_key, title_key):
            field_name = self._(field_key, default=field_key)
            label = self._("dialog.prompt.label", default="Please enter {field}:", field=field_name)
            while True:
                dlg = QInputDialog(self)
                dlg.setWindowTitle(self._(title_key, default=title_key))
                dlg.setLabelText(label)
                dlg.setTextValue("")
                line_edit = dlg.findChild(QLineEdit)
                if line_edit is not None:
                    self._prepare_latin_input(line_edit)
                if not dlg.exec_():
                    return None
                value = dlg.textValue().strip()
                if value:
                    return value
                QMessageBox.warning(
                    self,
                    self._("dialog.input_error.title", default="Input Error"),
                    self._("dialog.input_error.message", default="{field} cannot be empty.", field=field_name),
                )

        if not self.debug_enabled:
            if self.require_sn:
                sn = prompt("field.dut_sn", "dialog.prompt.dut_sn.title")
                if sn is None:
                    return
            else:
                sn = self.sn_default
        else:
            sn = self.ed_sn.text().strip() or self.sn_default

        self.ed_sn.setText(sn)
        self.on_start()

    def _runs_root(self) -> Path:
        return self.project_root / "runs"

    def _latest_run_path(self, sn: str) -> Path:
        return self._runs_root() / sn / "latest_run.json"

    def _load_latest_run_info(self, sn: str):
        latest_path = self._latest_run_path(sn)
        if not latest_path.exists():
            return None, latest_path, None
        try:
            data = json.loads(latest_path.read_text(encoding="utf-8-sig"))
        except Exception:
            return None, latest_path, None
        run_id = data.get("run_id")
        run_dir = None
        if run_id:
            run_dir = self._runs_root() / sn / run_id
        return data, latest_path, run_dir

    def _handle_finished_run(self, sn: str) -> bool:
        info, latest_path, run_dir = self._load_latest_run_info(sn)
        if not info:
            return False
        status = (info.get("status") or "").lower()
        finished_map = {
            "finished_pass": Status.PASS,
            "finished_fail": Status.FAIL,
            "stopped": Status.STOPPED,
        }
        if status not in finished_map:
            return False
        if not run_dir or not run_dir.exists():
            return False
        mapped_status = finished_map[status]
        status_label = {
            "finished_pass": "PASS",
            "finished_fail": "FAIL",
            "stopped": "STOPPED",
        }.get(status, status)
        self._display_finished_run(sn, run_dir, info, mapped_status)
        if status == "finished_pass":
            self._latest_run_snapshot = {
                "sn": sn,
                "latest_path": latest_path,
                "run_dir": run_dir,
                "status": status,
            }
            self.btn_reset_flags.setEnabled(True)
            QMessageBox.information(
                self,
                self._("dialog.finished_pass_required.title", default="Reset Required"),
                self._("dialog.finished_pass_required.message", default="SN {sn} had a previous PASS result (status: {status}). Reset flags before testing again.", sn=sn, status=status_label),
            )
            return True
        self.append_log(f"[INFO] {self._('log.previous_status_continue', default='SN {sn} last result: {status}; continuing tests.', sn=sn, status=status_label)}\n")
        return False

    def _display_finished_run(self, sn: str, run_dir: Path, info: dict, mapped_status: str) -> None:
        if self.model:
            self.model.reset_states()
        self.txt_out.clear()
        report_dir = run_dir / "reports"
        report_file = None
        run_id = info.get("run_id")
        if run_id:
            candidate = report_dir / f"report_{sn}_{run_id}.json"
            if candidate.exists():
                report_file = candidate
        if report_file is None and report_dir.exists():
            json_files = sorted(report_dir.glob("*.json"))
            if json_files:
                report_file = json_files[-1]
        if report_file and self.model:
            try:
                report_data = json.loads(report_file.read_text(encoding="utf-8-sig"))
            except Exception:
                report_data = []
            for row in report_data:
                sid = row.get("step_id")
                if not sid:
                    continue
                state = row.get("state") or StepState.NOT_RUN
                duration = row.get("duration_s")
                attempt = row.get("attempt")
                note = row.get("note")
                self.model.set_status(sid, state, time_s=duration, attempts=attempt, note=note)
        if self.model:
            for row in self.model.rows:
                self._refresh_step_note(row["sid"])
        self.set_status(mapped_status)
        summary = [
            self._("log.previous_run_summary", default="Loaded previous run: SN={sn}, run_id={run_id}, status={status}", sn=sn, run_id=info.get('run_id'), status=info.get('status'))
        ]
        if report_file:
            summary.append(self._("log.previous_run_report", default="Report file: {path}", path=str(report_file)))
        for line in summary:
            self.append_log(f"[INFO] {line}\n")

    def _prompt_debug_password(self):
        cfg = self.debug_config or {}
        encoded = cfg.get("password_encoded", "").strip()
        max_attempts = int((cfg.get("max_attempts", "5") or "5"))
        target_password = self._decode_password(encoded) if encoded else ""
        if not target_password:
            QMessageBox.critical(
                self,
                self._("dialog.debug_password_missing.title", default="Configuration Error"),
                self._("dialog.debug_password_missing.message", default="Debug password is not set or invalid."),
            )
            return False
        self._fail_count = getattr(self, "_fail_count", 0)

        while True:
            dlg = QInputDialog(self)
            dlg.setWindowTitle(self._("dialog.debug_password_prompt.title", default="Debug Password"))
            dlg.setLabelText(self._("dialog.debug_password_prompt.label", default="Please enter password:"))
            dlg.setTextEchoMode(QLineEdit.Password)
            line_edit = dlg.findChild(QLineEdit)
            if line_edit is not None:
                self._prepare_latin_input(line_edit)
            if not dlg.exec_():
                app = QApplication.instance()
                if app:
                    app.quit()
                return False
            pwd = dlg.textValue() or ""
            matched = (pwd == target_password)
            if matched:
                self.debug_enabled = True
                self._set_debug_controls_enabled(True)
                QMessageBox.information(
                    self,
                    self._("dialog.debug_password_success.title", default="Debug Mode"),
                    self._("dialog.debug_password_success.message", default="Debug mode enabled."),
                )
                self._tag_debug_title()
                self._fail_count = 0
                return True

            self.debug_enabled = False
            self._set_debug_controls_enabled(False)
            self._fail_count += 1
            if self._fail_count >= max_attempts:
                QMessageBox.critical(
                    self,
                    self._("dialog.debug_password_limit.title", default="Verification Failed"),
                    self._("dialog.debug_password_limit.message", default="Password attempts exceeded. Application will close."),
                )
                app = QApplication.instance()
                if app:
                    app.quit()
                return False

            QMessageBox.warning(
                self,
                self._("dialog.debug_password_retry.title", default="Incorrect Password"),
                self._("dialog.debug_password_retry.message", default="Incorrect password. Try again ({count}/{total}).", count=self._fail_count, total=max_attempts),
            )

    def _set_debug_controls_enabled(self, enabled: bool):
        for b in [self.btn_up, self.btn_down, self.btn_save_ini, self.btn_run_selected, self.btn_run_from, self.btn_toggle_ignore]:
            b.setEnabled(enabled)
        self.spin_stress.setEnabled(enabled)
        self.spin_stress.setVisible(enabled)
        self.lbl_stress.setVisible(enabled)
        if not enabled:
            self.spin_stress.setValue(1)

    def _tag_debug_title(self):
        title = self.windowTitle()
        if "[DEBUG]" not in title:
            self.setWindowTitle(title + " [DEBUG]")

    def _decode_password(self, encoded: str) -> str:
        try:
            raw = base64.b64decode(encoded.encode("utf-8"), validate=True)
        except Exception:
            return ""
        try:
            decoded = bytes(b ^ 0x5A for b in raw)
            return decoded.decode("utf-8")
        except Exception:
            return ""

    def eventFilter(self, obj, event):
        if getattr(self, "_latin_inputs", None) and obj in self._latin_inputs and event.type() == QEvent.FocusIn:
            self._force_english_keyboard()
        return super().eventFilter(obj, event)

    def _prepare_latin_input(self, line_edit: QLineEdit):
        if line_edit is None:
            return
        hints = Qt.ImhPreferLatin | Qt.ImhNoPredictiveText | Qt.ImhNoAutoUppercase
        line_edit.setInputMethodHints(hints)
        if getattr(self, "_latin_inputs", None) is not None and line_edit not in self._latin_inputs:
            line_edit.installEventFilter(self)
            self._latin_inputs.add(line_edit)
        self._force_english_keyboard()

    def _force_english_keyboard(self):
        if not sys.platform.startswith("win"):
            return
        try:
            user32 = ctypes.windll.user32
            hkl = user32.LoadKeyboardLayoutW("00000409", 1)
            if hkl:
                user32.ActivateKeyboardLayout(hkl, 0)
        except Exception:
            pass

    # Debug helpers
    def reorder(self, delta):
        sel = self.table.selectionModel().selectedRows()
        if not sel: return
        row = sel[0].row()
        new_row = max(0, min(self.model.rowCount()-1, row+delta))
        if new_row == row: return
        self.table.blockSignals(True)
        self.model.beginResetModel()
        self.model.rows.insert(new_row, self.model.rows.pop(row))
        for i, r in enumerate(self.model.rows, start=1):
            r["idx"] = i
        self.model.endResetModel()
        self.debug_step_sequence = [row["sid"] for row in self.model.rows]
        self.table.selectRow(new_row)
        self.table.blockSignals(False)

    def run_selected(self):
        sel = self.table.selectionModel().selectedRows()
        if not sel: return
        ordered_rows = sorted(sel, key=lambda idx: idx.row())
        sids = [self.model.rows[s.row()]["sid"] for s in ordered_rows]
        self._execute_steps(sids, manual_note=self._("tag.manual", default="(manual)"))

    def run_from_here(self):
        sel = self.table.selectionModel().selectedRows()
        if not sel: return
        start_row = sel[0].row()
        sids = [self.model.rows[i]["sid"] for i in range(start_row, self.model.rowCount())]
        self._execute_steps(sids, manual_note=self._("tag.manual", default="(manual)"))

    def toggle_ignore(self):
        sel = self.table.selectionModel().selectedRows()
        if not sel: return
        i = sel[0].row()
        sid = self.model.rows[i]["sid"]
        default = self.step_ignore_defaults.get(sid, False)
        current = self.ignore_overrides.get(sid, default)
        new_value = not current
        if new_value == default:
            self.ignore_overrides.pop(sid, None)
        else:
            self.ignore_overrides[sid] = new_value
        self._refresh_step_note(sid)

    def save_debug_order(self):
        if not self.debug_enabled:
            QMessageBox.warning(
                self,
                self._("dialog.debug_save_requires_mode.title", default="Debug Mode"),
                self._("dialog.debug_save_requires_mode.message", default="Enable debug mode before saving the sequence."),
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
            sid_mapping = self._persist_debug_order()
        except Exception as exc:
            QMessageBox.critical(
                self,
                self._("dialog.debug_save_error.title", default="Save Failed"),
                self._("dialog.debug_save_error.message", default="Failed to write configuration: {error}", error=exc),
            )
            return
        self.ignore_overrides = {sid_mapping.get(k, k): v for k, v in self.ignore_overrides.items()}
        self.debug_step_sequence = [sid_mapping.get(sid, sid) for sid in self.debug_step_sequence]
        self.rebuild_model_from_config()
        QMessageBox.information(
            self,
            self._("dialog.debug_save_success.title", default="Debug Mode"),
            self._("dialog.debug_save_success.message", default="Test order saved to configuration."),
        )

    def _execute_steps(self, sids, manual_note=None):
        sids = [sid for sid in sids if sid]
        if not sids:
            return
        sn = self.ed_sn.text().strip() or self.sn_default
        fr = FlowRunner(self.cfg_path, sn, self.debug_enabled)
        self._apply_runner_overrides(fr, apply_order=False)
        order_map = {sid: idx for idx, sid in enumerate(sids)}
        filtered = []
        for step in fr.steps:
            sid = f"{step.order}_{step.name}"
            if sid in order_map:
                filtered.append(step)
        if not filtered:
            return
        filtered.sort(key=lambda step: order_map[f"{step.order}_{step.name}"])
        fr.steps = filtered

        manual_tag = manual_note.strip() if manual_note else ""

        thread = RunnerThread(fr)
        thread.step_started.connect(self.on_step_started)
        if manual_tag:
            thread.step_finished.connect(lambda sid, res, tag=manual_tag: self._handle_manual_step_finished(sid, res, tag))
        else:
            thread.step_finished.connect(self.on_step_finished)
        thread.log_line.connect(self.append_log)
        thread.finished.connect(lambda thr=thread: self._cleanup_manual_thread(thr))
        self.manual_threads.append(thread)
        thread.start()

    def _handle_manual_step_finished(self, sid, res, manual_tag):
        res = dict(res)
        note = res.get("note") or ""
        res["note"] = f"{manual_tag} {note}".strip() if note else manual_tag
        self.on_step_finished(sid, res)

    def _cleanup_manual_thread(self, thread):
        if thread in self.manual_threads:
            self.manual_threads.remove(thread)
        thread.deleteLater()

    def _persist_debug_order(self):
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read(self.cfg_path, encoding="utf-8-sig")
        new_cfg = configparser.ConfigParser()
        new_cfg.optionxform = str

        # 保留非 step.* 區段的原始資料
        for sec in cfg.sections():
            if not sec.startswith("step."):
                new_cfg.add_section(sec)
                for key, val in cfg[sec].items():
                    new_cfg.set(sec, key, val)

        rows = list(self.model.rows)
        sid_mapping = {}
        for idx, row in enumerate(rows, start=1):
            old_sid = row["sid"]
            base_name = old_sid.split("_", 1)[1] if "_" in old_sid else old_sid
            new_order = idx * 10
            new_sid = f"{new_order}_{base_name}"
            old_section = f"step.{old_sid}"
            if old_section not in cfg:
                continue
            new_section = f"step.{new_sid}"
            if new_cfg.has_section(new_section):
                # 若產生衝突，後綴索引避免覆寫
                suffix = 1
                temp_section = f"{new_section}_{suffix}"
                while new_cfg.has_section(temp_section):
                    suffix += 1
                    temp_section = f"{new_section}_{suffix}"
                new_section = temp_section
                new_sid = new_section[5:]
            new_cfg.add_section(new_section)
            for key, val in cfg[old_section].items():
                new_cfg.set(new_section, key, val)
            sid_mapping[old_sid] = new_sid

        # 保留未列入的 step 區段
        for sec in cfg.sections():
            if not sec.startswith("step."):
                continue
            sid = sec[5:]
            if sid in sid_mapping:
                continue
            new_cfg.add_section(sec)
            for key, val in cfg[sec].items():
                new_cfg.set(sec, key, val)
            sid_mapping.setdefault(sid, sid)

        with open(self.cfg_path, "w", encoding="utf-8-sig") as f:
            new_cfg.write(f)

        return sid_mapping

    def reset_run_ui_state(self):
        if self.model:
            self.model.reset_states()
            for row in self.model.rows:
                self._refresh_step_note(row["sid"])
        self.table.clearSelection()
        self.txt_out.clear()
        self.set_status(Status.READY)
        self.btn_reset_flags.setEnabled(False)
        self._latest_run_snapshot = None

    def on_runner_finished(self):
        status = self.runner.global_status if self.runner else None
        self.runner = None
        self.runner_thread = None
        if (self.debug_enabled and not self._stress_stop_requested and
                self._stress_runs_remaining > 0 and status == Status.PASS):
            self._start_next_run()
        else:
            self._finalize_stress()

    def _apply_runner_overrides(self, runner: FlowRunner, apply_order: bool):
        for step in runner.steps:
            sid = f"{step.order}_{step.name}"
            step.ignore_result = self._effective_ignore(sid)
        if apply_order and self.debug_enabled and self.debug_step_sequence:
            priority = {sid: idx for idx, sid in enumerate(self.debug_step_sequence)}
            runner.steps.sort(key=lambda step: (priority.get(f"{step.order}_{step.name}", float("inf")), step.order))

    def _effective_ignore(self, sid: str) -> bool:
        return self.ignore_overrides.get(sid, self.step_ignore_defaults.get(sid, False))

    def _note_for_ignore_state(self, sid: str) -> str:
        default = self.step_ignore_defaults.get(sid, False)
        if sid in self.ignore_overrides:
            key = "ignore.note.force_on" if self.ignore_overrides[sid] else "ignore.note.force_off"
            return self._(key, default="Ignore result: forced on" if self.ignore_overrides[sid] else "Ignore result: forced off")
        if default:
            return self._("ignore.note.config", default="Ignore result: configuration default")
        return ""

    def _refresh_step_note(self, sid: str):
        if not self.model:
            return
        ignore_note = self._note_for_ignore_state(sid)
        for row in self.model.rows:
            if row["sid"] == sid:
                existing_note = row.get("note", "")
                combined_note = existing_note or ""
                if ignore_note:
                    if combined_note:
                        if ignore_note not in combined_note:
                            combined_note = f"{combined_note} | {ignore_note}"
                    else:
                        combined_note = ignore_note
                self.model.set_status(sid, row["status"], note=combined_note)
                self.model.set_ignore(sid, self._effective_ignore(sid))
                break

def launch(cfg_path: Path):
    app = QApplication(sys.argv)
    w = MainWindow(cfg_path)
    w.showMaximized()
    sys.exit(app.exec_())




