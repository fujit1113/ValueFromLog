"""アプリケーション層のユースケース集。"""
from typing import Sequence, Optional
import datetime as dt
import warnings
from ..domain.ports import LogRepository
from ..domain.models import MergedLogDataset


class LoadLogsUseCase:
    """指定期間のログを取得するユースケース。

    依存するリポジトリを差し替えることで、ファイル/GCS/DB 等に切替可能。
    """

    def __init__(self, repo: LogRepository):
        """LogRepository 実装を受け取る。

        Parameters
        ----------
        repo : LogRepository
            操作履歴・状態変化を取得するリポジトリ実装。
        """
        self.repo = repo

    def execute(
        self,
        contract_ids: Sequence[str],
        start_date: dt.datetime,
        end_date: Optional[dt.datetime] = None,
    ) -> MergedLogDataset:
        """指定期間の操作履歴・状態変化を突合した結果を返却する。

        Parameters
        ----------
        contract_ids : Sequence[str]
            抽出対象の ContractId 一覧（必須）。
        start_date : datetime.datetime
            取得期間の開始日時（必須）。
        end_date : datetime.datetime or None, default None
            取得期間の終了日時。None の場合は最新日時まで取得。

        Returns
        -------
        MergedLogDataset
            突合済みデータセット。
        """
        if start_date is None:
            raise ValueError("start_date は必須です。")
        if contract_ids is None:
            raise ValueError("contract_ids は必須です。")

        # 要素数が大きい場合は警告だけ出して処理は続行
        try:
            count = len(contract_ids)
            if count > 10_000:
                warnings.warn(
                    f"ContractId の件数が多いため処理が重くなる可能性があります（{count}件）。",
                    ResourceWarning,
                    stacklevel=2,
                )
        except TypeError:
            pass

        return self.repo.fetch_range(
            contract_ids=contract_ids, start_date=start_date, end_date=end_date
        )
