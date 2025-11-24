"""後方互換ラッパー: クリーンアーキ構成に委譲する。"""
from typing import Sequence
import pandas as pd
import datetime as dt
from .interface.notebook import load_for_notebook


def load_latest(
    contract_ids: Sequence[str], start_date: dt.datetime, end_date: dt.datetime | None = None
) -> pd.DataFrame:
    """旧Notebook互換: `load_for_notebook()` を呼び出すだけ。

    Parameters
    ----------
    contract_ids : Sequence[str]
        抽出したい ContractId のリスト（必須）。
    start_date : datetime.datetime
        取得期間開始日時（必須）。
    end_date : datetime.datetime or None, default None
        取得期間終了日時。None の場合は最新まで。

    Returns
    -------
    pandas.DataFrame
        突合済みのデータフレーム。
    """
    return load_for_notebook(
        contract_ids=contract_ids, start_date=start_date, end_date=end_date
    )
