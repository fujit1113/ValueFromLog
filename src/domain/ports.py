"""データ取得に関する抽象ポート (インタフェース) 定義。"""
from typing import Protocol, Tuple
import pandas as pd


class LogRepository(Protocol):
    def fetch_latest(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """最新のログ(操作履歴, 状態変化)を DataFrame で返す."""
        ...
