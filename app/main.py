import sys
from pathlib import Path


def resource_path(*parts: str) -> Path:
    """Return absolute path to resource, works for dev and PyInstaller builds."""
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parents[1]))
    candidate = base.joinpath(*parts)
    if candidate.exists():
        return candidate
    # fallback to current working directory (e.g. when resources placed beside exe)
    return Path.cwd().joinpath(*parts)


# Ensure project root on sys.path for module imports
sys.path.append(str(resource_path()))
from app.ui.main_window import launch  # noqa: E402


if __name__ == "__main__":
    cfg = resource_path("config", "station.ini")
    launch(cfg)
