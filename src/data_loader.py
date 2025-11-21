"""後方互換ラッパー: クリーンアーキ構成に委譲する。"""
from typing import Tuple, Sequence, Optional
import pandas as pd
from .interface.notebook import load_for_notebook


def load_latest(
    contract_ids: Optional[Sequence[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """旧Notebook互換: `load_for_notebook()` を呼び出すだけ。

    Args:
        contract_ids: 抽出したい ContractId のリスト。None なら ValueError を返す。
    """
    return load_for_notebook(contract_ids=contract_ids)
