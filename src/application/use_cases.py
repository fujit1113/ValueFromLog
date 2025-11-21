"""アプリケーション層のユースケース集。"""
from typing import Tuple, Sequence, Optional
import pandas as pd
from ..domain.ports import LogRepository


class LoadLatestLogsUseCase:
    """最新Excelを取得するユースケース。

    依存するリポジトリを差し替えることで、ファイル/gcs/DB等に切替可能。
    """

    def __init__(self, repo: LogRepository):
        """`repo` は LogRepository 実装を受け取る."""
        self.repo = repo

    def execute(
        self, contract_ids: Optional[Sequence[str]] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """最新の操作履歴・状態変化をペアで返却する。

        Args:
            contract_ids: 抽出対象の ContractId 一覧。None の場合は全件。
        """
        op_df, state_df = self.repo.fetch_latest()
        if contract_ids:
            op_df = op_df[op_df["ContractId"].isin(contract_ids)]
            state_df = state_df[state_df["ContractId"].isin(contract_ids)]
        return op_df, state_df
