"""Notebook 向けファサード層."""
from typing import Sequence, Optional, Tuple
import pandas as pd
from ..application.use_cases import LoadLatestLogsUseCase
from ..infrastructure.excel_repository import ExcelLogRepository


def load_for_notebook(
    contract_ids: Optional[Sequence[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """最新Excelから (操作履歴DF, 状態変化DF) を返す高水準API.

    Args:
        contract_ids: 抽出したい ContractId のリスト。None なら全件 → 処理負荷のため禁止。

    Raises:
        ValueError: contract_ids が None のとき（全件取得禁止）。
    """
    if contract_ids is None:
        raise ValueError(
            "全件取得は処理が重いため禁止です。ContractId のリストを指定してください。"
        )
    repo = ExcelLogRepository()
    use_case = LoadLatestLogsUseCase(repo)
    return use_case.execute(contract_ids=contract_ids)
