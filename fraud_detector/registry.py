import logging
import os
import shutil
from pathlib import Path

from fraud_core.bundle import LATEST_FILE, load_bundle, read_latest, write_latest

logger = logging.getLogger(__name__)


class ModelRegistry:
    def __init__(self, models_dir, initial_dir):
        self.models_dir = Path(models_dir)
        self.initial_dir = Path(initial_dir)
        self.bundle = None
        self._latest_mtime = None

    def ensure_initial(self):
        os.umask(0)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        if read_latest(self.models_dir) is not None:
            return
        target = self.models_dir / "initial"
        if not target.exists():
            shutil.copytree(self.initial_dir, target)
        write_latest(self.models_dir, "initial", source="bundled with fraud_detector image")
        logger.info("No published model yet, using bundled initial model")

    def refresh(self):
        latest = self.models_dir / LATEST_FILE
        try:
            mtime = latest.stat().st_mtime
        except FileNotFoundError:
            return self.bundle
        if self.bundle is not None and mtime == self._latest_mtime:
            return self.bundle

        version = read_latest(self.models_dir)
        if self.bundle is not None and version == self.bundle.version:
            self._latest_mtime = mtime
            return self.bundle
        try:
            bundle = load_bundle(self.models_dir / version)
        except Exception:
            logger.exception("Could not load model %s, keeping %s", version,
                             self.bundle.version if self.bundle else None)
            return self.bundle

        previous = self.bundle.version if self.bundle else None
        self.bundle = bundle
        self._latest_mtime = mtime
        logger.info("Switched model %s -> %s (threshold %.4f)", previous, bundle.version, bundle.threshold)
        return self.bundle
