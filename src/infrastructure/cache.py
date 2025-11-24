"""シンプルなローカルキャッシュ (Parquet ベース)。"""
from __future__ import annotations

from pathlib import Path
from typing import Tuple, Optional, Sequence
import datetime as dt
import hashlib

import pandas as pd
import os
import logging

# プロジェクト直下に .cache を作成
BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_DIR = BASE_DIR / ".cache"
CACHE_DIR.mkdir(exist_ok=True)


def build_cache_key(
    latest_path: Path,
    operation_cols: Sequence[str],
    state_cols: Sequence[str],
    contract_ids: Sequence[str],
    start_date: dt.datetime,
    end_date: Optional[dt.datetime] = None,
) -> str:
    """キャッシュキーを期間と対象者で一意になるよう生成する。

    Parameters
    ----------
    latest_path : pathlib.Path
        対象ファイルのパス。
    operation_cols : Sequence[str]
        操作ログで読み込む列一覧。
    state_cols : Sequence[str]
        状態変化ログで読み込む列一覧。
    contract_ids : Sequence[str]
        抽出対象の ContractId 群。
    start_date : datetime.datetime
        取得期間の開始日時。
    end_date : datetime.datetime or None, default None
        取得期間の終了日時。None の場合は最新日時まで。

    Returns
    -------
    str
        期間と対象者を含んだ一意のキャッシュキー。
    """
    hasher = hashlib.sha256()
    hasher.update(str(latest_path).encode("utf-8"))
    hasher.update(str(latest_path.stat().st_mtime_ns).encode("utf-8"))
    hasher.update(",".join(operation_cols + state_cols).encode("utf-8"))
    hasher.update(",".join(sorted(contract_ids)).encode("utf-8"))
    hasher.update(str(pd.to_datetime(start_date)).encode("utf-8"))
    if end_date:
        hasher.update(str(pd.to_datetime(end_date)).encode("utf-8"))
    else:
        hasher.update(b"<end=latest>")
    return hasher.hexdigest()


def cache_paths(key: str) -> Tuple[Path, Path]:
    """キャッシュファイル(操作/状態)のパスを返す。

    Parameters
    ----------
    key : str
        キャッシュを識別するキー。

    Returns
    -------
    tuple[pathlib.Path, pathlib.Path]
        操作ログ用と状態変化ログ用 Parquet のパス。
    """
    base = CACHE_DIR / key
    return base.with_suffix(".op.parquet"), base.with_suffix(".state.parquet")


def load_cached(key: str) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    """キャッシュを読み込み、存在しなければ None を返す。

    Parameters
    ----------
    key : str
        キャッシュを識別するキー。

    Returns
    -------
    tuple[pandas.DataFrame, pandas.DataFrame] or None
        キャッシュが存在する場合は (操作DF, 状態DF)。存在しない/壊れている場合は None。
    """
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
    """DataFrame ペアをキャッシュに保存する (Parquet)。

    Parameters
    ----------
    key : str
        キャッシュを識別するキー。
    op_df : pandas.DataFrame
        操作ログ DataFrame。
    state_df : pandas.DataFrame
        状態変化ログ DataFrame。
    """
    op_path, state_path = cache_paths(key)
    op_df.to_parquet(op_path, index=False)
    state_df.to_parquet(state_path, index=False)
