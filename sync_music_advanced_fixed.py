#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sync Music (Windows/macOS/Android via ADB) — Fixed + Playlist support
- Reliable direction metadata
- Android mkdir -p / rm -rf
- Hash cache keyed by (backend, root, rel)
- Quick-mode fallback to hash when metadata is unreliable
- Safer ADB existence checks via return code
- Unicode NFC on output paths
- NEW: Playlist rewrite support for .m3u/.m3u8 (simple mode: make entries relative basenames)

Flags:
  --rewrite-playlist      Enable playlist rewriting when copying .m3u/.m3u8
  --playlist-ext         Extensions to treat as playlists (default: .m3u8,.m3u)

Usage examples:
    python sync_music_advanced_fixed.py "D:\\Music" "adb://storage/emulated/0/Music" --mode hash --dry-run --protect-android --rewrite-playlist
"""
from __future__ import annotations
import argparse
import hashlib
import os
import shutil
import sqlite3
import sys
import tempfile
import time
import unicodedata
import subprocess
import shlex
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Dict, Iterable, List, Optional, Set, Tuple, BinaryIO

# ========= Config =========
MUSIC_EXTS = {".mp3", ".flac", ".wav", ".aac", ".m4a", ".ogg", ".wma", ".alac", ".aiff"}
DEFAULT_EXCLUDES = {".DS_Store", "Thumbs.db", ".Spotlight-V100", ".Trashes", "._*"}
MTIME_TOLERANCE = 2.0  # FAT/exFAT 2-second granularity tolerance
CHUNK_SIZE = 1024 * 1024
CACHE_FILE = os.path.join(os.getcwd(), "sync_cache.sqlite")

# ========= Utils =========

def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)

def lower_key(p: str) -> str:
    return p.lower()

# progress printer (stdout)
_def_width = 30
_last_tick = 0.0

def _progress(done: int, total: int, prefix: str = ""):
    global _last_tick
    now = time.time()
    if now - _last_tick < 0.05:
        return
    _last_tick = now
    frac = min(1.0, max(0.0, (done / total) if total else 1.0))
    filled = int(frac * _def_width)
    bar = "#" * filled + "-" * (_def_width - filled)
    pct = int(frac * 100)
    sys.stdout.write(f"\r{prefix:>6} [" + bar + f"] {pct:3d}%  {done}/{total} bytes")
    sys.stdout.flush()

def sha1sum_stream(stream: BinaryIO, total_size: Optional[int] = None, label: str = "hash") -> str:
    h = hashlib.sha1()
    read = 0
    while True:
        chunk = stream.read(CHUNK_SIZE)
        if not chunk:
            break
        h.update(chunk)
        if total_size is not None:
            read += len(chunk)
            _progress(read, total_size, label)
    if total_size is not None:
        _progress(total_size, total_size, label)
        sys.stdout.write("\n"); sys.stdout.flush()
    return h.hexdigest()

def safe_relpath(path: str, root: str) -> str:
    rp = os.path.relpath(path, root)
    return nfc(rp.replace(os.sep, "/"))

def ensure_dir_local(dst_path: str) -> None:
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

def atomic_copy_with_progress(src: str, dst: str) -> None:
    ensure_dir_local(dst)
    total = os.path.getsize(src)
    copied = 0
    with tempfile.NamedTemporaryFile(delete=False, dir=os.path.dirname(dst)) as tf:
        tmp = tf.name
    try:
        with open(src, "rb") as fsrc, open(tmp, "wb") as fdst:
            while True:
                chunk = fsrc.read(CHUNK_SIZE)
                if not chunk:
                    break
                fdst.write(chunk)
                copied += len(chunk)
                _progress(copied, total, "copy")
        shutil.copystat(src, tmp, follow_symlinks=True)
        os.replace(tmp, dst)
        _progress(total, total, "copy"); sys.stdout.write("\n"); sys.stdout.flush()
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

# ========= SQLite cache =========

def cache_init(db_path: str = CACHE_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS file_hash (
            backend TEXT NOT NULL,
            root    TEXT NOT NULL,
            rel     TEXT NOT NULL,
            size    INTEGER,
            mtime   REAL,
            sha1    TEXT,
            updated_at REAL,
            PRIMARY KEY (backend, root, rel)
        )
        """
    )
    return conn

def cache_get(conn: sqlite3.Connection, backend: str, root: str, rel: str, size: int, mtime: float) -> Optional[str]:
    cur = conn.execute(
        "SELECT sha1,size,mtime FROM file_hash WHERE backend=? AND root=? AND rel=?",
        (backend, root, rel),
    )
    row = cur.fetchone()
    if not row:
        return None
    sha1, s, t = row
    if (s == size) and (abs((t or 0) - (mtime or 0)) <= MTIME_TOLERANCE):
        return sha1
    return None

def cache_put(conn: sqlite3.Connection, backend: str, root: str, rel: str, size: int, mtime: float, sha1: str) -> None:
    conn.execute(
        "REPLACE INTO file_hash (backend,root,rel,size,mtime,sha1,updated_at) VALUES (?,?,?,?,?,?,?)",
        (backend, root, rel, size, mtime, sha1, time.time()),
    )
    conn.commit()

# ========= Filesystem backends =========

@dataclass
class FileInfo:
    rel: str
    size: int
    mtime: float
    abspath: str
    key: str

class FS:
    backend_id: str = "base"

    def list_files(self, root: str, exts: Optional[Set[str]], excludes: List[str], include_all: bool, skip_hidden: bool) -> Dict[str, FileInfo]:
        raise NotImplementedError

    def open_for_hash(self, abspath: str) -> BinaryIO:
        return open(abspath, "rb")

    def read_bytes(self, abspath: str) -> bytes:
        with open(abspath, "rb") as f:
            return f.read()

    def write_bytes(self, abspath: str, data: bytes) -> None:
        ensure_dir_local(abspath)
        with open(abspath, "wb") as f:
            f.write(data)

    def copy_to(self, src_abspath: str, dst_fs: "FS", dst_abspath: str) -> None:
        raise NotImplementedError

    def delete_path(self, abspath: str) -> None:
        raise NotImplementedError

class LocalFS(FS):
    backend_id = "local"

    def list_files(self, root, exts, excludes, include_all, skip_hidden) -> Dict[str, FileInfo]:
        out: Dict[str, FileInfo] = {}
        root = os.path.abspath(root)
        for dirpath, dirnames, filenames in os.walk(root):
            if skip_hidden:
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
            kept = []
            rel_dir = safe_relpath(dirpath, root)
            rel_dir = "" if rel_dir == "." else rel_dir
            for d in dirnames:
                r = (rel_dir + "/" + d) if rel_dir else d
                if not any(fnmatch(r, pat) or fnmatch(d, pat) for pat in excludes):
                    kept.append(d)
            dirnames[:] = kept
            for fn in filenames:
                if skip_hidden and fn.startswith("."):
                    continue
                full = os.path.join(dirpath, fn)
                rel = safe_relpath(full, root)
                name = os.path.basename(rel)
                if any(fnmatch(rel, pat) or fnmatch(name, pat) for pat in excludes):
                    continue
                # include playlists always
                _, ext = os.path.splitext(fn)
                ext_l = ext.lower()
                if not include_all and (ext_l not in (exts or MUSIC_EXTS)) and (ext_l not in {'.m3u','.m3u8'}):
                    continue
                try:
                    st = os.stat(full)
                except FileNotFoundError:
                    continue
                key = lower_key(rel)
                out[key] = FileInfo(rel=rel, size=st.st_size, mtime=st.st_mtime, abspath=full, key=key)
        return out

    def copy_to(self, src_abspath: str, dst_fs: "FS", dst_abspath: str) -> None:
        if isinstance(dst_fs, LocalFS):
            atomic_copy_with_progress(src_abspath, dst_abspath)
        elif isinstance(dst_fs, AdbFS):
            dst_fs.ensure_remote_dir(os.path.dirname(dst_abspath))
            subprocess.run(["adb"] + (['-s', dst_fs.serial] if dst_fs.serial else []) + ["push", "-a", src_abspath, dst_abspath], check=True)
        else:
            raise NotImplementedError

    def delete_path(self, abspath: str) -> None:
        try:
            os.remove(abspath)
        except IsADirectoryError:
            shutil.rmtree(abspath)
        except FileNotFoundError:
            pass

class AdbFS(FS):
    backend_id = "adb"

    def __init__(self, serial: Optional[str] = None):
        self.serial = serial

    # low-level runners
    def _run(self, *args, check=True, text=True, input_bytes=None):
        cmd = ["adb"]
        if self.serial:
            cmd += ["-s", self.serial]
        cmd += list(args)
        return subprocess.run(cmd, check=check, capture_output=True, text=text, input=input_bytes)

    def _adb_out(self, *args, check=True) -> str:
        return self._run(*args, check=check).stdout

    def _ensure_device(self):
        if self.serial:
            return
        out = self._adb_out("devices", check=True)
        lines = [l.strip() for l in out.splitlines() if "\tdevice" in l]
        if not lines:
            raise RuntimeError("No ADB device detected. Enable USB debugging and check connection.")
        self.serial = lines[0].split("\t")[0]

    # helpers
    def ensure_remote_dir(self, d: str):
        self._ensure_device()
        if not d:
            return
        d_nfc = nfc(d)
        q = shlex.quote(d_nfc)

        # Try portable quoted mkdir -p via sh -lc (works on most Androids)
        cp = self._run("shell", "sh", "-lc", f"mkdir -p {q}", check=False)
        if cp.returncode == 0:
            return

        # Fallback: step-by-step mkdir (no -p), tolerate already exists
        parts = [p for p in d_nfc.split("/") if p]
        cur = "" if d_nfc.startswith("/") else None
        for seg in parts:
            cur = ("/" + seg) if cur == "" else (seg if cur is None else f"{cur}/{seg}")
            abs_cur = cur if cur.startswith("/") else "/" + cur
            seg_q = shlex.quote(abs_cur)
            cp2 = self._run("shell", "mkdir", seg_q, check=False)
            # Accept already-existing directories; error only on other failures
            if cp2.returncode != 0 and (cp2.stderr or "").strip().lower().find("file exists") == -1:
                msg = (cp2.stderr or "").strip() or "mkdir failed (unknown error)"
                raise RuntimeError(f"ADB mkdir failed for {d_nfc}: {msg}")

    def exists(self, path: str) -> bool:
        self._ensure_device()
        cp = self._run("shell", "sh", "-lc", f"test -e \"{path}\"", check=False)
        return cp.returncode == 0

    def list_files(self, root, exts, excludes, include_all, skip_hidden) -> Dict[str, FileInfo]:
        self._ensure_device()
        out: Dict[str, FileInfo] = {}

        # 1) gather paths (try toybox find -> find)
        paths_raw = None
        for find_cmd in (["shell", "toybox", "find", root, "-type", "f", "-print0"],
                         ["shell", "find", root, "-type", "f", "-print0"]):
            cp = self._run(*find_cmd, check=False)
            if cp.returncode == 0 and cp.stdout:
                paths_raw = cp.stdout
                break
        if paths_raw is None:
            return out
        paths = [p for p in paths_raw.split("\x00") if p]

        def want_this(path: str) -> bool:
            rel = nfc(path[len(root):].lstrip("/"))
            if not rel:
                return False
            name = os.path.basename(rel)
            if any(fnmatch(rel, pat) or fnmatch(name, pat) for pat in excludes):
                return False
            if skip_hidden and name.startswith('.'):
                return False
            if include_all:
                return True
            _, ext = os.path.splitext(name)
            ext_l = ext.lower()
            return (ext_l in (exts or MUSIC_EXTS)) or (ext_l in {'.m3u','.m3u8'})
        cand = [p for p in paths if p.startswith(root) and want_this(p)]

        # 2) stat for size/mtime (try toybox stat -> fallback)
        try:
            tmp_list = "/sdcard/.sync_music_list.txt"
            file_bytes = ("\n".join(cand) + "\n").encode("utf-8", "surrogatepass")
            self._run("push", "-", tmp_list, check=True, text=False, input_bytes=file_bytes)
            fmt = r"%n|%s|%Y"
            cp = self._run("shell", "sh", "-lc", f"while IFS= read -r p; do toybox stat -c '{fmt}' \"$p\"; done < {tmp_list}")
            self._run("shell", "rm", "-f", tmp_list, check=False)
            if cp.returncode != 0:
                raise RuntimeError("toybox stat not available")
            stat_out = cp.stdout
            for line in stat_out.splitlines():
                try:
                    fp, size, mtime = line.split("|", 3)
                    rel = nfc(fp[len(root):].lstrip("/"))
                    key = lower_key(rel)
                    out[key] = FileInfo(rel=rel, size=int(size), mtime=float(mtime), abspath=fp, key=key)
                except Exception:
                    continue
        except Exception:
            for fp in cand:
                rel = nfc(fp[len(root):].lstrip("/"))
                key = lower_key(rel)
                out[key] = FileInfo(rel=rel, size=-1, mtime=0.0, abspath=fp, key=key)
        return out

    def open_for_hash(self, abspath: str) -> BinaryIO:
        self._ensure_device()
        p = subprocess.Popen(["adb"] + (["-s", self.serial] if self.serial else []) + ["exec-out", "toybox", "cat", abspath], stdout=subprocess.PIPE)
        assert p.stdout is not None
        return p.stdout

    def read_bytes(self, abspath: str) -> bytes:
        self._ensure_device()
        cp = self._run("exec-out", "toybox", "cat", abspath, check=True, text=False)
        return cp.stdout

    def write_bytes(self, abspath: str, data: bytes) -> None:
        self._ensure_device()
        d = os.path.dirname(abspath)
        self.ensure_remote_dir(d)
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tmp = tf.name
            tf.write(data)
        try:
            self._run("push", tmp, abspath, check=True)
        finally:
            try: os.remove(tmp)
            except Exception: pass

    def copy_to(self, src_abspath: str, dst_fs: "FS", dst_abspath: str) -> None:
        self._ensure_device()
        if isinstance(dst_fs, LocalFS):
            ensure_dir_local(dst_abspath)
            subprocess.run(["adb"] + (["-s", self.serial] if self.serial else []) + ["pull", src_abspath, dst_abspath], check=True)
        elif isinstance(dst_fs, AdbFS):
            dst_fs.ensure_remote_dir(os.path.dirname(dst_abspath))
            with tempfile.NamedTemporaryFile(delete=False) as tf:
                tmp = tf.name
            try:
                subprocess.run(["adb"] + (["-s", self.serial] if self.serial else []) + ["pull", src_abspath, tmp], check=True)
                subprocess.run(["adb"] + (["-s", dst_fs.serial] if dst_fs.serial else []) + ["push", "-a", tmp, dst_abspath], check=True)
            finally:
                try: os.remove(tmp)
                except Exception: pass
        else:
            raise NotImplementedError

    def delete_path(self, abspath: str) -> None:
        self._ensure_device()
        subprocess.run(["adb"] + (["-s", self.serial] if self.serial else []) + ["shell", "rm", "-rf", abspath], check=True)

# ========= Diff & plan =========

@dataclass
class FileInfo:
    rel: str
    size: int
    mtime: float
    abspath: str
    key: str

@dataclass
class Action:
    op: str                # 'copy' | 'delete' | 'skip'
    src: Optional[FileInfo]
    dst_rel: Optional[str]
    direction: Optional[str] = None   # 'L2R' | 'R2L' (for copy)
    target_side: Optional[str] = None # 'left' | 'right' (for delete)
    note: str = ""

def equal_quick(a: FileInfo, b: FileInfo) -> bool:
    if a.size >= 0 and b.size >= 0 and a.size != b.size:
        return False
    if a.mtime > 0 and b.mtime > 0:
        return abs(a.mtime - b.mtime) <= MTIME_TOLERANCE and (a.size < 0 or b.size < 0 or a.size == b.size)
    return False

def plan_sync(left_root, right_root, left_idx, right_idx, mode, conflict, mirror_delete) -> List[Action]:
    actions: List[Action] = []
    keys = set(left_idx.keys()) | set(right_idx.keys())
    for k in sorted(keys):
        L = left_idx.get(k)
        R = right_idx.get(k)
        if L and not R:
            actions.append(Action("copy", L, L.rel, direction="L2R", note="L→R create"))
            continue
        if R and not L:
            actions.append(Action("copy", R, R.rel, direction="R2L", note="R→L create"))
            continue
        assert L and R
        # quick equal
        same_quick = (mode == "quick" and equal_quick(L, R))
        if same_quick:
            continue
        if conflict == "newer":
            newer = L if L.mtime >= R.mtime else R
            if newer is L:
                actions.append(Action("copy", L, L.rel, direction="L2R", note="newer L→R"))
            else:
                actions.append(Action("copy", R, R.rel, direction="R2L", note="newer R→L"))
        elif conflict == "left":
            actions.append(Action("copy", L, L.rel, direction="L2R", note="left wins"))
        elif conflict == "right":
            actions.append(Action("copy", R, R.rel, direction="R2L", note="right wins"))
        elif conflict == "skip":
            actions.append(Action("skip", None, None, note=f"conflict skip: {L.rel}"))
        elif conflict == "duplicate":
            if L.mtime >= R.mtime:
                actions.append(Action("copy", L, L.rel, direction="L2R", note="dup main L→R"))
                base, ext = os.path.splitext(L.rel)
                actions.append(Action("copy", R, f"{base} (conflict from RIGHT){ext}", direction="R2L", note="dup keep both"))
            else:
                actions.append(Action("copy", R, R.rel, direction="R2L", note="dup main R→L"))
                base, ext = os.path.splitext(R.rel)
                actions.append(Action("copy", L, f"{base} (conflict from LEFT){ext}", direction="L2R", note="dup keep both"))
        else:
            raise ValueError("Unknown conflict policy")

    if mirror_delete:
        for k in sorted(set(right_idx.keys()) - set(left_idx.keys())):
            actions.append(Action("delete", None, right_idx[k].rel, target_side="right", note="mirror delete R-only"))
        for k in sorted(set(left_idx.keys()) - set(right_idx.keys())):
            actions.append(Action("delete", None, left_idx[k].rel, target_side="left", note="mirror delete L-only"))

    return actions

# ========= CLI =========

def parse_args():
    p = argparse.ArgumentParser(description="Music folder sync (Win/mac/Android ADB) — fixed + playlist")
    p.add_argument("left", help="Source path or adb URI (e.g. adb://storage/emulated/0/Music)")
    p.add_argument("right", help="Target path or adb URI")
    p.add_argument("--mode", choices=["quick", "hash"], default="quick")
    p.add_argument("--conflict", choices=["newer", "left", "right", "skip", "duplicate"], default="newer")
    p.add_argument("--delete", action="store_true", help="Mirror delete (⚠️ dry-run first)")
    p.add_argument("--all", action="store_true", help="Sync all files (default: music only + playlists)")
    p.add_argument("--exclude", action="append", default=[])
    p.add_argument("--no-skip-hidden", action="store_true")
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--apply", action="store_true", help="Actually perform actions (disables dry-run)")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--protect-left", action="store_true")
    p.add_argument("--protect-right", action="store_true")
    p.add_argument("--protect-android", action="store_true", help="Protect whichever side is Android")
    p.add_argument("--cache", default=CACHE_FILE)
    # playlist
    p.add_argument("--rewrite-playlist", action="store_true", help="Rewrite .m3u/.m3u8 entries to relative basenames on copy")
    p.add_argument("--playlist-ext", default=".m3u8,.m3u", help="Comma-separated playlist extensions")
    return p.parse_args()

def parse_endpoint(ep: str):
    if ep.startswith("adb://"):
        rest = ep[len("adb://"):]
        serial = None
        if rest.startswith("device:"):
            parts = rest.split("/", 1)
            serial = parts[0].split(":", 1)[1]
            root = "/" + (parts[1] if len(parts) > 1 else "")
        else:
            root = "/" + rest.lstrip("/")
        return AdbFS(serial), root
    else:
        return LocalFS(), os.path.abspath(ep)

# compute or fetch hash with cache

def get_hash(fs: FS, root: str, fi: FileInfo, cache: sqlite3.Connection) -> str:
    # try cache first
    h = cache_get(cache, fs.backend_id, root, fi.rel, fi.size, fi.mtime)
    if h:
        return h
    # compute
    with fs.open_for_hash(fi.abspath) as f:
        total = fi.size if fi.size and fi.size >= 0 else None
        h = sha1sum_stream(f, total, label=f"hash:{os.path.basename(fi.rel)}")
    cache_put(cache, fs.backend_id, root, fi.rel, fi.size, fi.mtime, h)
    return h

# ========= Playlist rewrite =========

def is_playlist(path: str, playlist_exts: Set[str]) -> bool:
    _, ext = os.path.splitext(path)
    return ext.lower() in playlist_exts

def rewrite_m3u_simple(content: str) -> str:
    """Very simple rewrite: keep comments; for paths, keep only basename, normalize to '/'."""
    out_lines = []
    for line in content.splitlines():
        if not line or line.lstrip().startswith("#"):
            out_lines.append(line)
            continue
        p = line.strip().replace("\\", "/")
        base = os.path.basename(p)
        out_lines.append(base)
    return "\n".join(out_lines) + "\n"

def read_text_fs(fs: FS, abspath: str, encoding="utf-8") -> str:
    data = fs.read_bytes(abspath)
    try:
        return data.decode(encoding)
    except UnicodeDecodeError:
        return data.decode("utf-8", "ignore")

def write_text_fs(fs: FS, abspath: str, text: str, encoding="utf-8"):
    fs.write_bytes(abspath, text.encode(encoding))

# ========= Main =========

def main():
    args = parse_args()
    fsL, left_root = parse_endpoint(args.left)
    fsR, right_root = parse_endpoint(args.right)

    include_all = bool(args.all)
    exts = None if include_all else MUSIC_EXTS
    excludes = list(DEFAULT_EXCLUDES) + args.exclude
    skip_hidden = not args.no_skip_hidden
    dry_run = not args.apply if args.apply else args.dry_run
    playlist_exts = {e.strip().lower() for e in args.playlist_ext.split(",") if e.strip()}

    # Protection
    protect_left = args.protect_left or (args.protect_android and isinstance(fsL, AdbFS))
    protect_right = args.protect_right or (args.protect_android and isinstance(fsR, AdbFS))

    print(f"Source: {args.left}  → root={left_root}  (backend={fsL.backend_id})")
    print(f"Target: {args.right} → root={right_root} (backend={fsR.backend_id})")
    print(f"Mode : {args.mode}, Conflict: {args.conflict}, Mirror delete: {args.delete}")
    print(f"Dry-run: {dry_run}, Include all: {include_all}, Skip hidden: {skip_hidden}")
    print(f"Protect: source={protect_left}, target={protect_right}")
    if args.exclude:
        print(f"Extra excludes: {args.exclude}")
    if args.rewrite_playlist:
        print(f"Playlist rewrite: ON (exts={sorted(playlist_exts)})")

    cache = cache_init(args.cache)

    if args.verbose:
        print("→ Indexing source…")
    left_idx = fsL.list_files(left_root, exts, excludes, include_all, skip_hidden)
    if args.verbose:
        print(f"  Source files: {len(left_idx)}")

    if args.verbose:
        print("→ Indexing target…")
    right_idx = fsR.list_files(right_root, exts, excludes, include_all, skip_hidden)
    if args.verbose:
        print(f"  Target files: {len(right_idx)}")

    actions = plan_sync(left_root, right_root, left_idx, right_idx, mode=args.mode, conflict=args.conflict, mirror_delete=args.delete)

    copies = deletes = skips = 0

    for a in actions:
        if a.op == "copy" and a.src and a.dst_rel and a.direction in ("L2R", "R2L"):
            src = a.src
            if a.direction == "L2R":
                src_fs, dst_fs = fsL, fsR
                src_root, dst_root = left_root, right_root
                dst_protected = protect_right
                other = right_idx.get(src.key)
            else:
                src_fs, dst_fs = fsR, fsL
                src_root, dst_root = right_root, left_root
                dst_protected = protect_left
                other = left_idx.get(src.key)

            # Build destination absolute
            if isinstance(dst_fs, AdbFS):
                dst_abs = nfc((dst_root.rstrip("/") + "/" + a.dst_rel.lstrip("/")).replace("//", "/"))
            else:
                dst_abs = os.path.join(dst_root, a.dst_rel.replace("/", os.sep))

            # Protection: skip overwrite if target exists
            if dst_protected:
                try:
                    exists = dst_fs.exists(dst_abs) if isinstance(dst_fs, AdbFS) else os.path.exists(dst_abs)
                except Exception:
                    exists = False
                if exists:
                    print(f"[SKIP] protected dst exists: {dst_abs}")
                    skips += 1
                    continue

            # Decide if we need a content hash check
            need_hash = (args.mode == "hash") or (args.mode == "quick" and (src.size < 0 or src.mtime == 0 or (other and (other.size < 0 or other.mtime == 0))))
            if other and need_hash:
                try:
                    h_src = get_hash(src_fs, src_root, src, cache)
                    h_dst = get_hash(dst_fs, dst_root, other, cache)
                    if h_src == h_dst:
                        if dry_run or args.verbose:
                            print(f"[SKIP] equal by hash: {src.rel}")
                        skips += 1
                        continue
                except Exception:
                    pass

            # If playlist rewrite
            _, ext = os.path.splitext(src.rel)
            if args.rewrite_playlist and ext.lower() in playlist_exts:
                if dry_run:
                    print(f"[COPY][playlist] {src.abspath}  ->  {dst_abs}  (rewrite)")
                else:
                    # read, rewrite, write
                    try:
                        txt = read_text_fs(src_fs, src.abspath)
                    except Exception:
                        txt = ""
                    new_txt = rewrite_m3u_simple(txt)
                    write_text_fs(dst_fs, dst_abs, new_txt)
                copies += 1
                continue

            if dry_run:
                print(f"[COPY] {a.note}: {src.abspath}  ->  {dst_abs}")
            else:
                if isinstance(src_fs, LocalFS) and isinstance(dst_fs, LocalFS):
                    atomic_copy_with_progress(src.abspath, dst_abs)
                else:
                    src_fs.copy_to(src.abspath, dst_fs, dst_abs)
            copies += 1

        elif a.op == "delete" and a.dst_rel and a.target_side in ("left", "right"):
            dst_fs, dst_root, prot = (fsL, left_root, protect_left) if a.target_side == "left" else (fsR, right_root, protect_right)
            if prot:
                print(f"[SKIP] protected delete ({a.target_side}): {a.dst_rel}")
                skips += 1
                continue
            if isinstance(dst_fs, AdbFS):
                dst_abs = nfc((dst_root.rstrip("/") + "/" + a.dst_rel.lstrip("/")).replace("//", "/"))
            else:
                dst_abs = os.path.join(dst_root, a.dst_rel.replace("/", os.sep))
            if dry_run or args.verbose:
                print(f"[DELETE] {a.note}: {dst_abs}")
            if not dry_run:
                dst_fs.delete_path(dst_abs)
            deletes += 1

        elif a.op == "skip":
            if dry_run or args.verbose:
                print(f"[SKIP] {a.note}")
            skips += 1

    print(f"Summary: copy={copies}, delete={deletes}, skip/conflict={skips}")
    if dry_run:
        print("(Hint: use --apply to actually perform changes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
