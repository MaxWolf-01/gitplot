"""Git subprocess wrappers and repo resolution."""

import hashlib
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path

BLAME_RE = re.compile(r"\((.+?)\s+(\d{10})\s+[+-]\d{4}\s+\d+\)")


def run_git(cmd: list[str], cwd: str) -> str:
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"git failed: {' '.join(cmd)}\n{r.stderr}")
    return r.stdout


CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "gitplot" / "repos"


def resolve_repo(repo: str) -> tuple[Path, str]:
    """Return (local_path, display_name).

    Local paths are used directly. URLs are cloned into ~/.cache/gitplot/repos/.
    """
    p = Path(repo)
    if p.is_dir() and (p / ".git").exists():
        return p, p.name

    name = repo.rstrip("/").split("/")[-1].replace(".git", "")
    url_hash = hashlib.md5(repo.encode()).hexdigest()[:8]
    dest = CACHE_DIR / f"{name}-{url_hash}"
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists():
        subprocess.run(["git", "fetch", "--all"], cwd=dest, capture_output=True)
    else:
        subprocess.run(["git", "clone", repo, str(dest)], capture_output=True, check=True)

    return dest, name


def get_all_commits(repo: str) -> list[tuple[str, datetime]]:
    out = run_git(["git", "log", "--format=%H %at", "--reverse"], repo)
    commits = []
    for line in out.strip().split("\n"):
        if line:
            h, ts = line.split()
            commits.append((h, datetime.fromtimestamp(int(ts))))
    return commits


def sample_evenly(commits: list[tuple[str, datetime]], n: int) -> list[tuple[str, datetime]]:
    if len(commits) <= n:
        return commits
    step = len(commits) / n
    indices = [int(i * step) for i in range(n)]
    indices[-1] = len(commits) - 1
    return [commits[i] for i in indices]


def tracked_files(repo: str, commit: str, exts: list[str] | None) -> list[str]:
    out = run_git(["git", "ls-tree", "-r", "--name-only", commit], repo)
    files = [f for f in out.strip().split("\n") if f]
    if exts:
        files = [f for f in files if any(f.endswith(e) for e in exts)]
    return files


def blame_lines(repo: str, commit: str, path: str) -> list[tuple[int, str]]:
    """Return (unix_timestamp, author) for each line in a file at a given commit."""
    try:
        out = run_git(["git", "blame", "-t", commit, "--", path], repo)
    except RuntimeError, UnicodeDecodeError:
        return []
    return [(int(m.group(2)), m.group(1).strip()) for line in out.split("\n") if line and (m := BLAME_RE.search(line))]
