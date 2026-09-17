from __future__ import annotations

import configparser
import re
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from core.paths import resolve_workdir


@dataclass
class AppConfig:
    locale: str
    fallback_locale: str
    steps: list[dict[str, Any]]
    step_ignore_defaults: dict[str, bool]
    debug_mode: bool
    debug_config: dict[str, str]
    station_id: str
    require_sn: bool
    read_sn: int
    sn_default: str


# ── 唯一的 ini 讀取來源 + step 各欄位唯一預設值 ──────────────
# flow_runner 與 ConfigService 都走這裡，避免同一欄位在兩處各有一套預設值。
STEP_DEFAULTS: dict[str, Any] = {
    "type": "process",
    "retry": 2,
    "retry_interval_sec": 2,
    "ignore_result": False,
    "kill_tree": True,
    "stdout_encoding": "utf-8",
    "pass_by": "exit_code:0",
}


def load_config(cfg_path: Path) -> configparser.ConfigParser:
    """唯一的 ini 讀取入口。保留大小寫、utf-8-sig。"""
    cfg = configparser.ConfigParser()
    cfg.optionxform = str
    cfg.read(cfg_path, encoding="utf-8-sig")
    return cfg


class ConfigService:
    def __init__(self, cfg_path: Path):
        self.cfg_path = Path(cfg_path)

    def _new_parser(self) -> configparser.ConfigParser:
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        return cfg

    def read_localization_settings(self) -> tuple[str, str]:
        locale = "zh-TW"
        fallback = "en-US"
        cfg = configparser.ConfigParser(strict=False)
        cfg.optionxform = str
        try:
            cfg.read(self.cfg_path, encoding="utf-8-sig")
            locale = cfg.get("ui", "locale", fallback=locale).strip() or locale
            fallback = cfg.get("ui", "fallback_locale", fallback=fallback).strip() or fallback
        except Exception:
            pass
        return locale, fallback

    def load_app_config(self) -> AppConfig:
        cfg = load_config(self.cfg_path)

        locale = cfg.get("ui", "locale", fallback="zh-TW").strip() or "zh-TW"
        fallback_locale = cfg.get("ui", "fallback_locale", fallback="en-US").strip() or "en-US"

        step_ignore_defaults: dict[str, bool] = {}
        steps: list[dict[str, Any]] = []
        for sec in cfg.sections():
            if not sec.startswith("step."):
                continue
            order_name = sec[5:]
            try:
                order_str, name = order_name.split("_", 1)
                order = int(order_str)
            except Exception:
                continue
            sid = f"{order}_{name}"
            step_ignore_defaults[sid] = cfg[sec].getboolean("ignore_result", False)
            steps.append({"sid": sid, "name": name})

        steps.sort(key=lambda row: int(row["sid"].split("_", 1)[0]))

        debug_mode = cfg.getboolean("debug", "debug_mode", fallback=False) if cfg.has_section("debug") else False
        debug_config = dict(cfg["debug"].items()) if cfg.has_section("debug") else {}
        station_id = cfg.get("meta", "station_id", fallback="").strip()
        require_sn = cfg.getboolean("run", "require_sn", fallback=True)
        read_sn = cfg.getint("run", "read_sn", fallback=1)
        sn_default = cfg.get("run", "sn_default", fallback="SN000").strip() or "SN000"

        return AppConfig(
            locale=locale,
            fallback_locale=fallback_locale,
            steps=steps,
            step_ignore_defaults=step_ignore_defaults,
            debug_mode=debug_mode,
            debug_config=debug_config,
            station_id=station_id,
            require_sn=require_sn,
            read_sn=read_sn,
            sn_default=sn_default,
        )

    def save_step_order(self, rows: list[dict[str, Any]]) -> dict[str, str]:
        cfg = self._new_parser()
        cfg.read(self.cfg_path, encoding="utf-8-sig")

        new_cfg = self._new_parser()
        for sec in cfg.sections():
            if sec.startswith("step."):
                continue
            new_cfg.add_section(sec)
            for key, val in cfg[sec].items():
                new_cfg.set(sec, key, val)

        sid_mapping: dict[str, str] = {}
        for idx, row in enumerate(rows, start=1):
            old_sid = row["sid"]
            base_name = old_sid.split("_", 1)[1] if "_" in old_sid else old_sid
            new_order = idx * 10
            new_sid = f"{new_order}_{base_name}"
            old_section = f"step.{old_sid}"
            if old_section not in cfg:
                sid_mapping[old_sid] = old_sid
                continue

            new_section = f"step.{new_sid}"
            if new_cfg.has_section(new_section):
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

        for sec in cfg.sections():
            if not sec.startswith("step."):
                continue
            sid = sec[5:]
            if sid in sid_mapping:
                continue
            new_cfg.add_section(sec)
            for key, val in cfg[sec].items():
                new_cfg.set(sec, key, val)
            sid_mapping[sid] = sid

        with open(self.cfg_path, "w", encoding="utf-8-sig") as fh:
            new_cfg.write(fh)

        return sid_mapping

    # ---- 解析 step 的 cmd 欄位 -----------------------------------------------
    # 每次呼叫都重開一次 ini 讀取，確保拿到磁碟上的最新值。

    def _read_step_cmd(self, sid: str) -> Optional[str]:
        # cfg = self._new_parser()
        # cfg.read(self.cfg_path, encoding="utf-8-sig")
        # section = f"step.{sid}"
        # if not cfg.has_section(section):
        #     return None
        # cmd = cfg.get(section, "cmd", fallback="").strip()
        # return cmd or None
        try:
            cfg = load_config(self.cfg_path)
        except configparser.Error:
            return None   # ini 壞了就回 None，呼叫端會 hideText
        section = f"step.{sid}"
        if not cfg.has_section(section):
            return None
        cmd = cfg.get(section, "cmd", fallback="").strip()
        return cmd or None

    def get_step_work_folder(self, sid: str, project_root: Path) -> Optional[Path]:
        """從 step 的 cmd 裡抽出 cd 目的地，轉成絕對路徑。找不到就回 None。"""
        # 以 [run] workdir 為基底
        base = self._resolve_run_workdir(project_root)

        cmd = self._read_step_cmd(sid)
        if not cmd:
            return None
        match = re.search(r'cd\s+(?:/d\s+)?(.+?)\s*&&', cmd, flags=re.IGNORECASE)
        if not match:
            return base
        
        raw_path = match.group(1).strip().strip('"').strip("'")
        folder = Path(raw_path)

        if folder.is_absolute():
            return folder
        
        # 相對路徑：以 [run] workdir 為基底
        return (base / folder).resolve()
    
    def _resolve_run_workdir(self, project_root: Path) -> Path:
        """讀 [run] workdir 的值並轉成絕對路徑。讀不到就 fallback 到 project_root。"""
        cfg = self._new_parser()
        try:
            cfg.read(self.cfg_path, encoding="utf-8-sig")
        except configparser.Error:
            return project_root

        raw = cfg.get("run", "workdir", fallback="")
        return resolve_workdir(raw, project_root)


    def get_step_call_target(self, sid: str) -> Optional[str]:
        """從 step 的 cmd 裡抽出 call 的檔名，用在 tooltip。"""
        cmd = self._read_step_cmd(sid)
        if not cmd:
            return None
        match = re.search(r'call\s+["\']?([^"\'>\s&]+)', cmd, flags=re.IGNORECASE)
        if not match:
            return None
        return Path(match.group(1)).name

    def read_step_runtime(self, sids: list[str]) -> dict[str, dict[str, Any]]:
        """
        一次讀入一批 step 的執行時設定（目前只有 retry 次數）。
        用於 StepsModel 建構時避免它自己開 configparser。

        回傳: {sid: {"retry": int}}；找不到 section 的 sid 會得到空 dict。
        """
        try:
            cfg = load_config(self.cfg_path)
        except configparser.Error:
            return {sid: {} for sid in sids}

        result: dict[str, dict[str, Any]] = {}
        for sid in sids:
            section = f"step.{sid}"
            if not cfg.has_section(section):
                result[sid] = {}
                continue
            result[sid] = {"retry": cfg[section].getint("retry", STEP_DEFAULTS["retry"])}
        return result

    # ===================
    # 新增：spec 的上下限
    # ===================
    def read_step_limits(self, sids: list[str]) -> dict[str, dict[str, Optional[str]]]:
        """
        一次讀入一批 step 的上下限與 pass_by 規則。

        回傳: {sid: {"lower_limit": str|None, "upper_limit": str|None, "pass_by": str}}
        pass_by 缺項給預設 "exit_code:0"，跟舊版一致。
        """
        try:
            cfg = load_config(self.cfg_path)
        except configparser.Error:
            return {sid: {} for sid in sids}

        result: dict[str, dict[str, Any]] = {}
        for sid in sids:
            section = f"step.{sid}"
            if not cfg.has_section(section):
                result[sid] = {}
                continue
            result[sid] = {
                "lower_limit": cfg.get(section, "lower_limit", fallback=None),
                "upper_limit": cfg.get(section, "upper_limit", fallback=None),
                "pass_by": cfg.get(section, "pass_by", fallback=STEP_DEFAULTS["pass_by"]),
            }
        return result

    def write_step_limit(self, sid: str, key: str, value: str) -> bool:
        """
        更新某個 step 的單一限制欄位（lower_limit / upper_limit）並寫回 ini。
        回傳 True/False 表示成功與否。失敗時不拋例外，呼叫端自己決定怎麼處理。
        """
        log = logging.getLogger(__name__)

        if key not in ("lower_limit", "upper_limit"):
            log.warning("write_step_limit: unsupported key %r", key)
            return False
        cfg = self._new_parser()
        try:
            cfg.read(self.cfg_path, encoding="utf-8-sig")
        except configparser.Error as exc:
            log.error("write_step_limit read failed: %s", exc)
            return False

        section = f"step.{sid}"
        if not cfg.has_section(section):
            log.warning("write_step_limit: section %s not found", section)
            return False

        cfg.set(section, key, str(value))
        try:
            with open(self.cfg_path, "w", encoding="utf-8-sig") as fh:
                cfg.write(fh)
            return True
        except OSError as exc:
            log.error("write_step_limit write failed: %s", exc)
            return False