"""
Path safety helpers (QA fix 3).

Silver and Gold writes use Spark's overwrite mode, which deletes the target folder
first. If a target folder were the raw (Bronze) folder, sat inside it, or contained
it, a run could delete the original CSVs. These helpers canonicalize paths and
refuse any such overlap before anything is written.
"""

from __future__ import annotations

import os
from pathlib import Path


def canonical_path(path: str | Path) -> Path:
    """Absolute, symlink-resolved, case-normalized (on Windows) version of ``path``.

    ``os.path.realpath`` resolves ``..`` segments and symlinks even when the path
    does not exist yet; ``normcase`` makes ``D:\\Data`` and ``d:\\data`` compare equal.
    """
    return Path(os.path.normcase(os.path.realpath(os.path.abspath(str(path)))))


def paths_overlap(a: str | Path, b: str | Path) -> bool:
    """True if the two paths are the same folder or one is nested inside the other."""
    ca, cb = canonical_path(a), canonical_path(b)
    return ca == cb or ca in cb.parents or cb in ca.parents


def ensure_no_overlap(protected: str | Path, target: str | Path, protected_label: str, target_label: str) -> None:
    """Raise ValueError if writing to ``target`` could touch ``protected``."""
    if paths_overlap(protected, target):
        raise ValueError(
            f"Unsafe output path: {target_label} '{canonical_path(target)}' overlaps "
            f"{protected_label} '{canonical_path(protected)}'. Choose a separate folder."
        )
