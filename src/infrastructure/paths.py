"""インフラ層で共有するパスユーティリティ."""
from pathlib import Path

# Base directory of the project (assumes this file lives in src/infrastructure)
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"

# File pattern expected for input Excel files
EXCEL_PATTERN = "★機器遠隔操作履歴＆機器状態変化履歴_*.xlsx"


def list_input_files():
    """パターンに一致するExcelをソートして返す（mmddで新しい順判定に利用）。"""
    return sorted(DATA_DIR.glob(EXCEL_PATTERN))
