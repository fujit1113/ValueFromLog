"""データ取得に関する抽象ポート (インタフェース) 定義。"""
from typing import Protocol, Sequence, Optional
import datetime as dt
from .models import MergedLogDataset


class LogRepository(Protocol):
    def fetch_range(
        self,
        contract_ids: Sequence[str],
        start_date: dt.datetime,
        end_date: Optional[dt.datetime] = None,
    ) -> MergedLogDataset:
        """指定期間のログを突合したデータセットで返す。"""
        ...
