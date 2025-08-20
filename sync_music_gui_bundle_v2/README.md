# Sync Music (Win/macOS/Android via ADB) — GUI Bundle (Source→Target, SD, Playlists)

## Files
- `sync_music_advanced_fixed.py` — engine with cache/progress/protection + **playlist rewrite**.
- `sync_music_gui_storages.py` — PySide6 GUI (Source/Target wording, ADB device & storage picker, true Start/Stop).
- `requirements.txt` — Python deps.

## Install
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

## Run
```bash
python sync_music_gui_storages.py
```

### Tips
- Plug Android, enable **USB debugging**.
- Pick device and storage (Internal or SD UUID). Subpath defaults to `Music`.- GUI會自動組出 `adb://device:<serial>/storage/<tail>/<subpath>` Endpoint。
- First run: **Preview (dry-run)** with **Mode=hash** and **Rewrite playlists**.  After verifying, click **Run (apply)**.- Playlist rewrite is **simple**: keeps comments, changes each entry to just the filename (basename).
  This works best when songs are in the same folder as the playlist.
