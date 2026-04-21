"""Bootstrap package so `python -m gva_pipeline.cli` works from the repo root."""

from __future__ import annotations

from pathlib import Path

_PACKAGE_DIR = Path(__file__).resolve().parent
_SRC_PACKAGE_DIR = _PACKAGE_DIR.parent / "src" / "gva_pipeline"

if _SRC_PACKAGE_DIR.is_dir():
    __path__.append(str(_SRC_PACKAGE_DIR))  # type: ignore[name-defined]

from .pipeline import run_pipeline

__all__ = ["run_pipeline"]
