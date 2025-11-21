"""シンプルなローカルキャッシュ (Parquet ベース)。"""
from __future__ import annotations

from pathlib import Path
from typing import Tuple, Optional

import pandas as pd
import os
import logging

# プロジェクト直下に .cache を作成
BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_DIR = BASE_DIR / ".cache"
CACHE_DIR.mkdir(exist_ok=True)


def cache_paths(key: str) -> Tuple[Path, Path]:
    """キャッシュファイル(操作/状態)のパスを返す。"""
    base = CACHE_DIR / key
    return base.with_suffix(".op.parquet"), base.with_suffix(".state.parquet")


def load_cached(key: str) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    """キャッシュを読み込み、存在しなければ None。"""
    op_path, state_path = cache_paths(key)
    if not (op_path.exists() and state_path.exists()):
        return None
    try:
        op_df = pd.read_parquet(op_path)
        state_df = pd.read_parquet(state_path)
        return op_df, state_df
    except Exception as exc:  # pyarrow の ExtensionType 破損など
        logging.warning("cache load failed (%s); regenerating cache", exc)
        # 壊れたキャッシュは削除して再生成させる
        for p in (op_path, state_path):
            try:
                p.unlink()
            except OSError:
                pass
        return None


def store_cache(key: str, op_df: pd.DataFrame, state_df: pd.DataFrame) -> None:
    """DataFrame ペアをキャッシュに保存する (Parquet)。"""
    op_path, state_path = cache_paths(key)
    op_df.to_parquet(op_path, index=False)
    state_df.to_parquet(state_path, index=False)
