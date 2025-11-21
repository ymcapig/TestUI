import json
from pathlib import Path
from typing import Callable, Dict, Optional


class LocalizationManager:
    """
    Simple JSON-based localization manager.
    Loads locale files from the given directory and resolves translation keys.
    """

    def __init__(self, locales_dir: Path, primary: str, fallback: Optional[str] = None):
        self.locales_dir = Path(locales_dir)
        self.primary = primary
        self.fallback = fallback if fallback and fallback != primary else None
        self._cache: Dict[str, Dict[str, str]] = {}

    def set_locale(self, primary: str, fallback: Optional[str] = None) -> None:
        if primary != self.primary:
            self.primary = primary
        self.fallback = fallback if fallback and fallback != primary else None

    def gettext(self, key: str, default: Optional[str] = None, **kwargs) -> str:
        template = self._resolve(key)
        if template is None:
            template = default if default is not None else key
        if kwargs:
            try:
                template = template.format(**kwargs)
            except Exception:
                pass
        return template

    def _resolve(self, key: str) -> Optional[str]:
        for locale in self._locale_order():
            catalog = self._load_locale(locale)
            if key in catalog:
                return catalog[key]
        return None

    def _locale_order(self):
        yield self.primary
        if self.fallback:
            yield self.fallback

    def _load_locale(self, locale: str) -> Dict[str, str]:
        if locale in self._cache:
            return self._cache[locale]
        path = self.locales_dir / f"{locale}.json"
        if not path.exists():
            catalog: Dict[str, str] = {}
        else:
            try:
                catalog = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(catalog, dict):
                    catalog = {}
            except Exception:
                catalog = {}
        self._cache[locale] = catalog
        return catalog
