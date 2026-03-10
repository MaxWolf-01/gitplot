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


def get_log_numstat(repo: str) -> list[tuple[str, float, str, str, int, int]]:
    """Parse git log --numstat. Returns (hash, timestamp, author, file, insertions, deletions)."""
    out = run_git(["git", "log", "--numstat", "--no-merges", "--format=COMMIT %H %at %aN"], repo)
    rows: list[tuple[str, float, str, str, int, int]] = []
    current_hash = current_author = None
    current_ts = 0.0

    for line in out.split("\n"):
        if line.startswith("COMMIT "):
            parts = line.split(" ", 3)
            current_hash = parts[1]
            current_ts = float(parts[2])
            current_author = parts[3]
        elif line and "\t" in line and current_hash:
            parts = line.split("\t")
            if len(parts) == 3 and parts[0] != "-":
                rows.append((current_hash, current_ts, current_author, parts[2], int(parts[0]), int(parts[1])))

    return rows


def get_coauthor_map(repo: str) -> dict[str, list[str]]:
    """Parse all commit messages for Co-Authored-By trailers.

    Returns short_hash (8 chars) -> [co-author names].
    Uses short hashes to match git blame output.
    """
    out = run_git(["git", "log", "--format=%H%n%b%nEND_COMMIT"], repo)
    result: dict[str, list[str]] = {}
    current_hash = None
    coauthors: list[str] = []

    for line in out.split("\n"):
        if line == "END_COMMIT":
            if current_hash and coauthors:
                result[current_hash[:8]] = coauthors
            current_hash = None
            coauthors = []
        elif len(line) == 40 and all(c in "0123456789abcdef" for c in line):
            current_hash = line
            coauthors = []
        elif "Co-Authored-By:" in line or "Co-authored-by:" in line:
            part = line.split(":", 1)[1].strip()
            name = part.split("<")[0].strip() if "<" in part else part
            if name:
                coauthors.append(name)

    return result


def blame_lines_with_hash(repo: str, commit: str, path: str) -> list[tuple[str, int, str]]:
    """Return (commit_hash, unix_timestamp, author) for each line in a file."""
    try:
        out = run_git(["git", "blame", "-t", commit, "--", path], repo)
    except RuntimeError, UnicodeDecodeError:
        return []
    results = []
    for line in out.split("\n"):
        if not line:
            continue
        m = BLAME_RE.search(line)
        if m:
            line_hash = line.split()[0].lstrip("^")
            results.append((line_hash, int(m.group(2)), m.group(1).strip()))
    return results


def blame_lines(repo: str, commit: str, path: str) -> list[tuple[int, str]]:
    """Return (unix_timestamp, author) for each line in a file at a given commit."""
    try:
        out = run_git(["git", "blame", "-t", commit, "--", path], repo)
    except RuntimeError, UnicodeDecodeError:
        return []
    return [(int(m.group(2)), m.group(1).strip()) for line in out.split("\n") if line and (m := BLAME_RE.search(line))]
