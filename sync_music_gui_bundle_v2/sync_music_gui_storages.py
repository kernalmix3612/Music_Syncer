#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sync Music GUI (Windows/macOS/Android via ADB) — Source/Target, SD detection, playlist flag
- PySide6 GUI for sync_music_advanced_fixed.py (playlist-enabled)
- Non-blocking via QProcess (true Start/Stop)
- ADB device pickers (Source/Target) + storage picker (Internal/SD UUID) + Subpath
- Streams engine stdout/stderr to console with progress lines
"""
from __future__ import annotations
import os
import sys
import subprocess
from typing import List, Tuple

from PySide6 import QtCore, QtGui, QtWidgets

ENGINE_FILENAME = "sync_music_advanced_fixed.py"

# ---- Helpers ----

def list_adb_devices() -> List[str]:
    try:
        cp = subprocess.run(["adb", "devices"], capture_output=True, text=True, check=False)
        lines = [l.strip() for l in cp.stdout.splitlines() if "\tdevice" in l]
        return [l.split("\t", 1)[0] for l in lines]
    except Exception:
        return []

def list_storages(serial: str) -> List[Tuple[str,str]]:
    """Return list of (label, path_tail) under /storage, e.g. [("Internal","emulated/0"), ("XXXX-YYYY","XXXX-YYYY")]"""
    if not serial or serial == "Auto":
        return []
    out = subprocess.run(["adb","-s",serial,"shell","ls","-1","/storage"], capture_output=True, text=True, check=False).stdout
    entries = [e.strip() for e in out.splitlines() if e.strip()]
    stor = []
    # internal
    if "emulated" in entries:
        # ensure 0 exists
        check0 = subprocess.run(["adb","-s",serial,"shell","ls","-1","/storage/emulated"], capture_output=True, text=True, check=False).stdout
        if "0" in check0.split():
            stor.append(("Internal","emulated/0"))
    # UUID-like and typical alt names
    for e in entries:
        if e in ("emulated","self","enc_emulated","sdcard0","sdcard") or e.startswith("."):
            continue
        if len(e) >= 4:
            stor.append((e,e))
    # dedup
    seen=set(); out_list=[]
    for lbl, tail in stor:
        if tail not in seen:
            seen.add(tail); out_list.append((lbl, tail))
    return out_list

class ADBPicker(QtWidgets.QGroupBox):
    def __init__(self, title: str, parent=None):
        super().__init__(title, parent)
        self.dev = QtWidgets.QComboBox(); self.dev.addItem("Auto")
        self.storage = QtWidgets.QComboBox(); self.storage.addItem("(choose storage)")
        self.subpath = QtWidgets.QLineEdit("Music")
        self.endpoint = QtWidgets.QLineEdit()  # final editable endpoint
        self.endpoint.setPlaceholderText("Local path or adb://...  (will be auto-filled if ADB device/storage selected)")
        self.btnRefresh = QtWidgets.QPushButton("Refresh ADB / Storage")
        form = QtWidgets.QGridLayout(self)
        form.addWidget(QtWidgets.QLabel("ADB device:"), 0,0); form.addWidget(self.dev, 0,1)
        form.addWidget(self.btnRefresh, 0,2)
        form.addWidget(QtWidgets.QLabel("Storage:"), 1,0); form.addWidget(self.storage, 1,1)
        form.addWidget(QtWidgets.QLabel("Subpath:"), 1,2); form.addWidget(self.subpath, 1,3)
        form.addWidget(QtWidgets.QLabel("Endpoint:"), 2,0); form.addWidget(self.endpoint, 2,1,1,3)
        # events
        self.btnRefresh.clicked.connect(self.refresh_all)
        self.dev.currentIndexChanged.connect(self.refresh_storage)
        self.storage.currentIndexChanged.connect(self.update_endpoint)
        self.subpath.textChanged.connect(lambda *_: self.update_endpoint())
    def refresh_devices(self, keep=True):
        cur = self.dev.currentText()
        devs = list_adb_devices()
        self.dev.blockSignals(True)
        self.dev.clear(); self.dev.addItem("Auto")
        for d in devs: self.dev.addItem(d)
        if keep and (cur in devs or cur=="Auto"):
            self.dev.setCurrentText(cur)
        self.dev.blockSignals(False)
    def refresh_storage(self):
        serial = self.dev.currentText()
        self.storage.blockSignals(True)
        self.storage.clear(); self.storage.addItem("(choose storage)")
        for lbl, tail in list_storages(serial):
            self.storage.addItem(f"{lbl}  (/storage/{tail})", userData=tail)
        self.storage.blockSignals(False)
        self.update_endpoint()
    def refresh_all(self):
        self.refresh_devices(); self.refresh_storage()
    def update_endpoint(self):
        serial = self.dev.currentText()
        tail = self.storage.currentData()
        sub = self.subpath.text().strip().lstrip("/")
        if serial and serial!="Auto" and tail:
            ep = f"adb://device:{serial}/storage/{tail}"
            if sub: ep += f"/{sub}"
            self.endpoint.setText(ep)

# ---- Main Window ----
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Sync Music — Source → Target")
        self.resize(1120, 820)
        cw = QtWidgets.QWidget(self)
        self.setCentralWidget(cw)

        # Source & Target pickers
        self.src = ADBPicker("同步來源 (Source)")
        self.tgt = ADBPicker("被同步目標 (Target)")
        self.src.endpoint.setText(os.path.expanduser("~/Music"))
        self.tgt.subpath.setText("Music")

        # Options
        self.mode = QtWidgets.QComboBox(); self.mode.addItems(["quick", "hash"]) ; self.mode.setCurrentText("hash")
        self.conflict = QtWidgets.QComboBox(); self.conflict.addItems(["newer","left","right","skip","duplicate"]) ; self.conflict.setCurrentText("newer")
        self.deleteMirror = QtWidgets.QCheckBox("Mirror delete (--delete)")
        self.includeAll = QtWidgets.QCheckBox("All files (--all)")
        self.skipHidden = QtWidgets.QCheckBox("Skip hidden") ; self.skipHidden.setChecked(True)
        self.protectAndroid = QtWidgets.QCheckBox("Protect Android (--protect-android)") ; self.protectAndroid.setChecked(True)
        self.protectLeft = QtWidgets.QCheckBox("Protect Source (--protect-left)")
        self.protectRight = QtWidgets.QCheckBox("Protect Target (--protect-right)")
        self.playlistRewrite = QtWidgets.QCheckBox("Rewrite playlists (.m3u8/.m3u)")
        self.playlistRewrite.setChecked(True)

        # Excludes / cache
        self.excludeEdit = QtWidgets.QLineEdit(".DS_Store;Thumbs.db;._*;*.cue")
        self.cachePath = QtWidgets.QLineEdit(os.path.join(os.getcwd(), "sync_cache.sqlite"))
        self.btnCacheBrowse = QtWidgets.QPushButton("…")
        self.btnCacheBrowse.setFixedWidth(28)
        self.btnCacheBrowse.clicked.connect(self.on_pick_cache)

        # Buttons
        self.btnPreview = QtWidgets.QPushButton("Preview (dry-run)")
        self.btnRun = QtWidgets.QPushButton("Run (apply)")
        self.btnStop = QtWidgets.QPushButton("Stop") ; self.btnStop.setEnabled(False)

        # Console
        self.console = QtWidgets.QPlainTextEdit(); self.console.setReadOnly(True)
        f = QtGui.QFontDatabase.systemFont(QtGui.QFontDatabase.FixedFont)
        self.console.setFont(f)

        # Layouts
        v = QtWidgets.QVBoxLayout(cw)
        v.addWidget(self.src)
        v.addWidget(self.tgt)
        grid = QtWidgets.QGridLayout()
        grid.addWidget(QtWidgets.QLabel("Mode:"), 0,0); grid.addWidget(self.mode, 0,1)
        grid.addWidget(QtWidgets.QLabel("Conflict:"), 0,2); grid.addWidget(self.conflict, 0,3)
        grid.addWidget(self.deleteMirror, 1,0,1,2)
        grid.addWidget(self.includeAll, 1,2)
        grid.addWidget(self.skipHidden, 1,3)
        grid.addWidget(self.protectAndroid, 2,0,1,2)
        grid.addWidget(self.protectLeft, 2,2)
        grid.addWidget(self.protectRight, 2,3)
        grid.addWidget(self.playlistRewrite, 3,0,1,2)
        grid.addWidget(QtWidgets.QLabel("Exclude (semicolon ; separated):"), 4,0,1,2)
        grid.addWidget(self.excludeEdit, 4,2,1,2)
        grid.addWidget(QtWidgets.QLabel("Cache path:"), 5,0)
        cacheLine = QtWidgets.QHBoxLayout(); cacheLine.addWidget(self.cachePath, 1); cacheLine.addWidget(self.btnCacheBrowse)
        grid.addLayout(cacheLine, 5,1,1,3)
        v.addLayout(grid)

        bar = QtWidgets.QHBoxLayout(); bar.addWidget(self.btnPreview); bar.addWidget(self.btnRun); bar.addStretch(1); bar.addWidget(self.btnStop)
        v.addLayout(bar)
        v.addWidget(self.console, 1)

        # Connections
        self.btnPreview.clicked.connect(lambda: self.launch(dry_run=True))
        self.btnRun.clicked.connect(lambda: self.launch(dry_run=False))
        self.btnStop.clicked.connect(self.stop)

        self.proc: QtCore.QProcess | None = None

    def on_pick_cache(self):
        fn, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Cache file", self.cachePath.text(), "SQLite (*.sqlite *.db);;All files (*)")
        if fn:
            self.cachePath.setText(fn)

    def build_argv(self, dry_run: bool) -> list[str]:
        left = self.src.endpoint.text().strip()
        right = self.tgt.endpoint.text().strip()
        argv: list[str] = [left, right]
        argv += ["--mode", self.mode.currentText()]
        argv += ["--conflict", self.conflict.currentText()]
        if self.deleteMirror.isChecked(): argv += ["--delete"]
        if self.includeAll.isChecked(): argv += ["--all"]
        if not self.skipHidden.isChecked(): argv += ["--no-skip-hidden"]
        if dry_run: argv += ["--dry-run"]
        else: argv += ["--apply"]
        if self.protectAndroid.isChecked(): argv += ["--protect-android"]
        if self.protectLeft.isChecked(): argv += ["--protect-left"]
        if self.protectRight.isChecked(): argv += ["--protect-right"]
        if self.playlistRewrite.isChecked(): argv += ["--rewrite-playlist"]
        ex = [x.strip() for x in self.excludeEdit.text().split(";") if x.strip()]
        for pat in ex: argv += ["--exclude", pat]
        cp = self.cachePath.text().strip()
        if cp: argv += ["--cache", cp]
        return argv

    def append_console(self, text: str):
        self.console.moveCursor(QtGui.QTextCursor.End)
        text = text.replace("\r", "\n")
        self.console.insertPlainText(text)
        self.console.moveCursor(QtGui.QTextCursor.End)

    def launch(self, dry_run: bool):
        if self.proc is not None:
            QtWidgets.QMessageBox.warning(self, "Busy", "A sync is already running.")
            return
        left = self.src.endpoint.text().strip(); right = self.tgt.endpoint.text().strip()
        if not left or not right:
            QtWidgets.QMessageBox.warning(self, "Missing", "Please provide both Source and Target endpoints.")
            return
        self.console.clear()
        argv = self.build_argv(dry_run)
        script_dir = os.path.abspath(os.path.dirname(__file__))
        script_path = os.path.join(script_dir, ENGINE_FILENAME)
        if not os.path.exists(script_path):
            QtWidgets.QMessageBox.critical(self, "Engine missing", f"Can't find {ENGINE_FILENAME} in {script_dir}")
            return

        self.console.appendPlainText("$ " + sys.executable + " " + ENGINE_FILENAME + " " + " ".join(argv) + "\n\n")

        self.proc = QtCore.QProcess(self)
        self.proc.setProgram(sys.executable)
        self.proc.setArguments([script_path] + argv)
        self.proc.setProcessChannelMode(QtCore.QProcess.MergedChannels)
        self.proc.readyReadStandardOutput.connect(lambda: self.append_console(bytes(self.proc.readAllStandardOutput()).decode(errors="ignore")))
        self.proc.readyReadStandardError.connect(lambda: self.append_console(bytes(self.proc.readAllStandardError()).decode(errors="ignore")))
        self.proc.finished.connect(self.on_finished)
        self.proc.start()
        self.btnStop.setEnabled(True)

    def on_finished(self, code: int, _status: QtCore.QProcess.ExitStatus):
        self.append_console(f"\n[Exit] code={code}\n")
        self.proc = None
        self.btnStop.setEnabled(False)

    def stop(self):
        if not self.proc:
            return
        self.append_console("\n[Stop] Terminating...\n")
        self.proc.terminate()
        QtCore.QTimer.singleShot(2000, lambda: self.proc and self.proc.kill())

def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    return app.exec()

if __name__ == "__main__":
    raise SystemExit(main())
