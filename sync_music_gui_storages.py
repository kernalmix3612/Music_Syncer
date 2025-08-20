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
from PySide6.QtWidgets import QProgressBar

ENGINE_FILENAME = "sync_music_advanced_fixed.py"

LANG_STRINGS = {
    'en': {
        'title': 'Sync Music — Source → Target',
        'group_source': 'Source (sync from)',
        'group_target': 'Target (sync to)',
        'adb_device': 'ADB device:',
        'refresh': 'Refresh ADB / Storage',
        'storage': 'Storage:',
        'subpath': 'Subpath:',
        'endpoint': 'Endpoint:',
        'mode': 'Mode:',
        'conflict': 'Conflict:',
        'delete': 'Mirror delete (--delete)',
        'all': 'All files (--all)',
        'skip_hidden': 'Skip hidden',
        'protect_android': 'Protect Android (--protect-android)',
        'protect_left': 'Protect Source (--protect-left)',
        'protect_right': 'Protect Target (--protect-right)',
        'playlist': 'Rewrite playlists (.m3u8/.m3u)',
        'exclude': 'Exclude (semicolon ; separated):',
        'cache': 'Cache path:',
        'preview': 'Preview (dry-run)',
        'run': 'Run (apply)',
        'stop': 'Stop',
        'busy_preview': 'Preview in progress…',
        'missing': 'Please provide both Source and Target endpoints.',
        'busy': 'A sync is already running.',
        'engine_missing': "Can't find {} in {}",
    },
    'zh': {
        'title': '音樂同步 — 同步來源 → 被同步目標',
        'group_source': '同步來源 (Source)',
        'group_target': '被同步目標 (Target)',
        'adb_device': 'ADB 裝置：',
        'refresh': '刷新 ADB / 儲存體',
        'storage': '儲存體：',
        'subpath': '子路徑：',
        'endpoint': '端點：',
        'mode': '模式：',
        'conflict': '衝突策略：',
        'delete': '鏡像刪除 (--delete)',
        'all': '包含所有檔案 (--all)',
        'skip_hidden': '略過隱藏檔',
        'protect_android': '保護 Android (--protect-android)',
        'protect_left': '保護來源 (--protect-left)',
        'protect_right': '保護目標 (--protect-right)',
        'playlist': '重寫播放清單 (.m3u8/.m3u)',
        'exclude': '排除（以分號 ; 分隔）：',
        'cache': '快取路徑：',
        'preview': '預覽 (dry-run)',
        'run': '執行 (apply)',
        'stop': '停止',
        'busy_preview': '預覽進行中…',
        'missing': '請填入來源與目標端點',
        'busy': '目前已有同步在執行',
        'engine_missing': '{} 無法在 {} 找到',
    }
}

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
        self.lblDevice = QtWidgets.QLabel("ADB device:")
        form.addWidget(self.lblDevice, 0,0)
        form.addWidget(self.dev, 0,1)
        form.addWidget(self.btnRefresh, 0,2)
        self.lblStorage = QtWidgets.QLabel("Storage:")
        form.addWidget(self.lblStorage, 1,0)
        form.addWidget(self.storage, 1,1)
        self.lblSubpath = QtWidgets.QLabel("Subpath:")
        form.addWidget(self.lblSubpath, 1,2)
        form.addWidget(self.subpath, 1,3)
        self.lblEndpoint = QtWidgets.QLabel("Endpoint:")
        form.addWidget(self.lblEndpoint, 2,0)
        form.addWidget(self.endpoint, 2,1,1,3)
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

        v = QtWidgets.QVBoxLayout(cw)

        self.lang = QtWidgets.QComboBox()
        self.lang.addItems(["中文","English"])
        langBar = QtWidgets.QHBoxLayout()
        self.lblLang = QtWidgets.QLabel("介面語言 / Language:")
        langBar.addWidget(self.lblLang)
        langBar.addWidget(self.lang)
        langBar.addStretch(1)
        v.addLayout(langBar)

        # Source & Target pickers
        self.src = ADBPicker("同步來源 (Source)")
        self.tgt = ADBPicker("被同步目標 (Target)")
        self.src.endpoint.setText(os.path.expanduser("~/Music"))
        self.tgt.subpath.setText("Music")

        # Options
        self.lblMode = QtWidgets.QLabel("Mode:")
        self.mode = QtWidgets.QComboBox(); self.mode.addItems(["quick", "hash"]) ; self.mode.setCurrentText("hash")
        self.lblConflict = QtWidgets.QLabel("Conflict:")
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
        self.lblExclude = QtWidgets.QLabel("Exclude (semicolon ; separated):")
        self.excludeEdit = QtWidgets.QLineEdit(".DS_Store;Thumbs.db;._*;*.cue")
        self.lblCache = QtWidgets.QLabel("Cache path:")
        self.cachePath = QtWidgets.QLineEdit(os.path.join(os.getcwd(), "sync_cache.sqlite"))
        self.btnCacheBrowse = QtWidgets.QPushButton("…")
        self.btnCacheBrowse.setFixedWidth(28)
        self.btnCacheBrowse.clicked.connect(self.on_pick_cache)

        # Buttons
        self.btnPreview = QtWidgets.QPushButton("Preview (dry-run)")
        self.btnRun = QtWidgets.QPushButton("Run (apply)")
        self.btnStop = QtWidgets.QPushButton("Stop") ; self.btnStop.setEnabled(False)

        # Preview progress bar
        self.previewBar = QProgressBar()
        self.previewBar.setVisible(False)
        self.previewBar.setRange(0,0)

        # Console
        self.console = QtWidgets.QPlainTextEdit(); self.console.setReadOnly(True)
        f = QtGui.QFontDatabase.systemFont(QtGui.QFontDatabase.FixedFont)
        self.console.setFont(f)

        # Layouts
        v.addWidget(self.src)
        v.addWidget(self.tgt)
        grid = QtWidgets.QGridLayout()
        grid.addWidget(self.lblMode, 0,0); grid.addWidget(self.mode, 0,1)
        grid.addWidget(self.lblConflict, 0,2); grid.addWidget(self.conflict, 0,3)
        grid.addWidget(self.deleteMirror, 1,0,1,2)
        grid.addWidget(self.includeAll, 1,2)
        grid.addWidget(self.skipHidden, 1,3)
        grid.addWidget(self.protectAndroid, 2,0,1,2)
        grid.addWidget(self.protectLeft, 2,2)
        grid.addWidget(self.protectRight, 2,3)
        grid.addWidget(self.playlistRewrite, 3,0,1,2)
        grid.addWidget(self.lblExclude, 4,0,1,2)
        grid.addWidget(self.excludeEdit, 4,2,1,2)
        grid.addWidget(self.lblCache, 5,0)
        cacheLine = QtWidgets.QHBoxLayout(); cacheLine.addWidget(self.cachePath, 1); cacheLine.addWidget(self.btnCacheBrowse)
        grid.addLayout(cacheLine, 5,1,1,3)
        v.addLayout(grid)

        bar = QtWidgets.QHBoxLayout(); bar.addWidget(self.btnPreview); bar.addWidget(self.btnRun); bar.addStretch(1); bar.addWidget(self.btnStop)
        v.addLayout(bar)
        v.addWidget(self.previewBar)
        v.addWidget(self.console, 1)

        # Connections
        self.btnPreview.clicked.connect(lambda: self.launch(dry_run=True))
        self.btnRun.clicked.connect(lambda: self.launch(dry_run=False))
        self.btnStop.clicked.connect(self.stop)

        self.lang.currentIndexChanged.connect(self.apply_language)

        self.proc: QtCore.QProcess | None = None

        self.apply_language()

    def apply_language(self):
        lang = 'zh' if self.lang.currentText().startswith('中') else 'en'
        strings = LANG_STRINGS[lang]
        self._lang = lang
        self.setWindowTitle(strings['title'])
        self.src.setTitle(strings['group_source'])
        self.tgt.setTitle(strings['group_target'])

        self.src.lblDevice.setText(strings['adb_device'])
        self.tgt.lblDevice.setText(strings['adb_device'])
        self.src.btnRefresh.setText(strings['refresh'])
        self.tgt.btnRefresh.setText(strings['refresh'])
        self.src.lblStorage.setText(strings['storage'])
        self.tgt.lblStorage.setText(strings['storage'])
        self.src.lblSubpath.setText(strings['subpath'])
        self.tgt.lblSubpath.setText(strings['subpath'])
        self.src.lblEndpoint.setText(strings['endpoint'])
        self.tgt.lblEndpoint.setText(strings['endpoint'])

        self.lblMode.setText(strings['mode'])
        self.lblConflict.setText(strings['conflict'])
        self.deleteMirror.setText(strings['delete'])
        self.includeAll.setText(strings['all'])
        self.skipHidden.setText(strings['skip_hidden'])
        self.protectAndroid.setText(strings['protect_android'])
        self.protectLeft.setText(strings['protect_left'])
        self.protectRight.setText(strings['protect_right'])
        self.playlistRewrite.setText(strings['playlist'])
        self.lblExclude.setText(strings['exclude'])
        self.lblCache.setText(strings['cache'])

        self.btnPreview.setText(strings['preview'])
        self.btnRun.setText(strings['run'])
        self.btnStop.setText(strings['stop'])

        self.lblLang.setText("介面語言 / Language:")

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
        strings = LANG_STRINGS[self._lang]
        if self.proc is not None:
            QtWidgets.QMessageBox.warning(self, strings['busy'], strings['busy'])
            return
        left = self.src.endpoint.text().strip(); right = self.tgt.endpoint.text().strip()
        if not left or not right:
            QtWidgets.QMessageBox.warning(self, strings['missing'], strings['missing'])
            return
        self.console.clear()
        argv = self.build_argv(dry_run)
        script_dir = os.path.abspath(os.path.dirname(__file__))
        script_path = os.path.join(script_dir, ENGINE_FILENAME)
        if not os.path.exists(script_path):
            QtWidgets.QMessageBox.critical(self, strings['engine_missing'].format(ENGINE_FILENAME, script_dir), strings['engine_missing'].format(ENGINE_FILENAME, script_dir))
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

        if dry_run:
            self.previewBar.setVisible(True)
            self.previewBar.setRange(0,0)
            self.previewBar.setFormat(strings['busy_preview'])
        else:
            self.previewBar.setVisible(False)

    def on_finished(self, code: int, _status: QtCore.QProcess.ExitStatus):
        self.append_console(f"\n[Exit] code={code}\n")
        self.proc = None
        self.btnStop.setEnabled(False)
        self.previewBar.setVisible(False)

    def stop(self):
        if not self.proc:
            return
        self.append_console("\n[Stop] Terminating...\n")
        self.proc.terminate()
        QtCore.QTimer.singleShot(2000, lambda: self.proc and self.proc.kill())
        self.previewBar.setVisible(False)

def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    return app.exec()

if __name__ == "__main__":
    raise SystemExit(main())
