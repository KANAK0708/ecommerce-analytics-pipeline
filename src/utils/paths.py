"""
Path safety helpers (QA fix 3).

Silver and Gold writes use Spark's overwrite mode, which deletes the target folder
first. If a target folder were the raw (Bronze) folder, sat inside it, or contained
it, a run could delete the original CSVs. These helpers canonicalize paths and
refuse any such overlap before anything is written.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Iterable


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


def is_link_or_reparse_point(path: str | Path) -> bool:
    """True for symlinks, Windows junctions and any other NTFS reparse point."""
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return False
    attrs = getattr(st, "st_file_attributes", 0)  # Windows only
    return stat.S_ISLNK(st.st_mode) or bool(attrs & stat.FILE_ATTRIBUTE_REPARSE_POINT)


def validate_write_destinations(
    protected: str | Path, output_root: str | Path, destinations: Iterable[str | Path], protected_label: str
) -> None:
    """Check every folder Spark will overwrite, before the first write.

    For each destination:
      1. Its fully resolved path (links and junctions followed, including on parts
         that already exist below the output root) must not equal, contain, or sit
         inside ``protected``.
      2. No existing path component from below ``output_root`` down to the
         destination may be a symlink, junction or other reparse point, so a link
         created under the output root cannot redirect a write elsewhere.

    Raises:
        ValueError: on the first unsafe destination.
    """
    root = Path(os.path.abspath(str(output_root)))
    for dest in destinations:
        dest = Path(os.path.abspath(str(dest)))
        ensure_no_overlap(protected, dest, protected_label, "write destination")
        try:
            relative = dest.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"Unsafe output path: write destination '{dest}' is outside output root '{root}'") from exc
        current = root
        for part in relative.parts:
            current = current / part
            if is_link_or_reparse_point(current):
                raise ValueError(
                    f"Unsafe output path: '{current}' is a symlink, junction or reparse point "
                    f"(resolves to '{canonical_path(current)}'). Remove it before running."
                )
