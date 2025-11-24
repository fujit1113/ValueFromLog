"""Notebook 向けファサード層."""
from typing import Sequence, Optional
import pandas as pd
from ..application.use_cases import LoadLogsUseCase
from ..infrastructure.excel_repository import ExcelLogRepository


def load_for_notebook(
    contract_ids: Sequence[str],
    start_date: pd.Timestamp,
    end_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """最新Excelから突合済み DataFrame を返す高水準API.

    Args:
        contract_ids: 抽出したい ContractId のリスト（必須）。
        start_date: 取得期間開始日時（必須）。
        end_date: 取得期間終了日時。None の場合は最新日時まで取得。

    Raises:
        ValueError: contract_ids が None のとき（全件取得禁止）。
    """
    if contract_ids is None:
        raise ValueError(
            "全件取得は処理が重いため禁止です。ContractId のリストを指定してください。"
        )
    if start_date is None:
        raise ValueError("start_date は必須です。")
    repo = ExcelLogRepository()
    use_case = LoadLogsUseCase(repo)
    merged = use_case.execute(
        contract_ids=contract_ids, start_date=start_date, end_date=end_date
    )
    return merged.df
