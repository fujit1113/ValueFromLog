"""簡易 .env ローダーと環境値ヘルパー."""
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Sequence


def load_env_file(file_path: Path | None = None) -> None:
    """プロジェクト直下の .env を読み込み、未設定の環境変数だけを追加する。

    シンプルな `KEY=VALUE` 形式（#で始まる行はコメント）をサポート。
    """
    if file_path is None:
        base = Path(__file__).resolve().parent.parent
        file_path = base / ".env"
    if not file_path.exists():
        return

    for line in file_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        # 既にOS環境にある場合は上書きしない
        if key not in os.environ:
            os.environ[key] = value


def get_env_list(key: str, default: Sequence[str]) -> List[str]:
    """環境変数をカンマ区切りリストとして取得。未設定時は default。"""
    raw = os.getenv(key)
    if raw is None:
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]
