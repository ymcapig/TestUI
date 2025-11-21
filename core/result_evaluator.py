import json, re, xml.etree.ElementTree as ET
from pathlib import Path

class ResultEvaluator:
    def __init__(self, run_dir: Path):
        self.run_dir = Path(run_dir)

    def evaluate(self, rule_str: str, exit_code: int, stdout_text: str, stderr_text: str, workdir: Path):
        """
        Evaluate rules; return (is_pass: bool, matched_rule: str)
        rule_str example: "exit_code:0|stdout_regex:PASS|file_exists:output.txt"
        """
        if not rule_str or rule_str.strip() == "":
            return (exit_code == 0, f"exit_code=={exit_code} (default)")

        rules = [r.strip() for r in rule_str.split("|") if r.strip()]
        stdout_text = stdout_text or ""
        stderr_text = stderr_text or ""

        for rule in rules:
            try:
                if rule.startswith("exit_code:"):
                    tgt = int(rule.split(":",1)[1].strip())
                    if exit_code == tgt: return (True, rule)

                elif rule.startswith("stdout_regex:"):
                    pat = rule.split(":",1)[1]
                    if re.search(pat, stdout_text, re.MULTILINE): return (True, rule)

                elif rule.startswith("stdout_contains:"):
                    needle = rule.split(":",1)[1]
                    if needle in stdout_text: return (True, rule)

                elif rule.startswith("stdout_equal:"):
                    expected = rule.split(":",1)[1]
                    if stdout_text.strip() == expected.strip(): return (True, rule)

                elif rule.startswith("stderr_regex:"):
                    pat = rule.split(":",1)[1]
                    if re.search(pat, stderr_text, re.MULTILINE): return (True, rule)

                elif rule.startswith("stderr_contains:"):
                    needle = rule.split(":",1)[1]
                    if needle in stderr_text: return (True, rule)

                elif rule.startswith("file_exists:"):
                    p = rule.split(":",1)[1]
                    fp = (Path(workdir)/p) if not Path(p).is_absolute() else Path(p)
                    if fp.exists(): return (True, rule)
                elif rule.startswith("file_contains:"):
                    # file_contains:<file>:<substring>
                    _, rest = rule.split(":", 1)
                    try:
                        file_part, needle = rest.split(":", 1)
                    except ValueError:
                        continue
                    file_path = Path(file_part)
                    fp = (workdir / file_path) if not file_path.is_absolute() else file_path
                    if fp.exists():
                        try:
                            content = fp.read_text(encoding="utf-8-sig", errors="ignore")
                        except Exception:
                            continue
                        if needle in content:
                            return (True, rule)

                elif rule.startswith("json_field:"):
                    # json_field:<file>:$.a.b=value
                    body = rule[len("json_field:"):]
                    file_part, rest = body.split(":",1)
                    jsonpath, expected = rest.split("=",1)
                    fpath = Path(workdir)/file_part
                    if fpath.exists():
                        data = json.loads(fpath.read_text(encoding="utf-8-sig"))
                        if jsonpath.startswith("$."):
                            keys = jsonpath[2:].split(".")
                            cur = data
                            ok=True
                            for k in keys:
                                if isinstance(cur, dict) and k in cur:
                                    cur = cur[k]
                                else:
                                    ok=False; break
                            if ok and str(cur) == expected:
                                return (True, rule)

                elif rule.startswith("junit_result:"):
                    # junit_result:<file>=PASS
                    file_part, expected = rule[len("junit_result:"):].split("=",1)
                    fpath = Path(workdir)/file_part
                    if fpath.exists():
                        tree = ET.parse(str(fpath))
                        root = tree.getroot()
                        failures = root.attrib.get("failures","0")
                        errors = root.attrib.get("errors","0")
                        if failures == "0" and errors == "0" and expected.upper()=="PASS":
                            return (True, rule)
            except Exception:
                # ignore this rule and continue
                pass
        return (False, "no rule matched")
