"""インフラ層: pandas で Excel ログを読むリポジトリ実装。"""
from typing import Optional, Sequence
import datetime as dt
import pandas as pd
from .paths import list_input_files
from .env_settings import load_env_file, get_env_list
from .cache import load_cached, store_cache, build_cache_key
from ..domain.models import MergedLogDataset

# デフォルト列（環境変数で上書き可能）
DEFAULT_OPERATION_COLS = [
    "ContractId",
    "OrderReceiptDate",
    "TimerDiv",
    "FloorCode",
    "RoomName",
    "EquipmentTypeId",
    "EquipmentName",
    "PropertyCode",
    "PropertyName",
    "PropertyValue",
]

DEFAULT_STATE_COLS = [
    "MessageName",
    "ContractId",
    "ReportedDate",
    "FloorCode",
    "RoomName",
    "EquipmentTypeId",
    "EquipmentName",
    "PropertyCode1",
    "PropertyName1",
    "PropertyValue1",
]


class ExcelLogRepository:
    """data/ 配下の最新ファイルを読み込むリポジトリ。"""

    def fetch_range(
        self,
        contract_ids: Sequence[str],
        start_date: dt.datetime,
        end_date: Optional[dt.datetime] = None,
    ) -> MergedLogDataset:
        """最新Excelから期間指定で突合済みデータセットを返す。

        Parameters
        ----------
        contract_ids : Sequence[str]
            抽出対象の ContractId 一覧（必須）。
        start_date : datetime.datetime
            取得期間の開始日時（必須）。
        end_date : datetime.datetime or None, default None
            取得期間の終了日時。None の場合は最新日時まで取得。

        Raises
        ------
        ValueError
            必須引数が渡されていない場合。
        FileNotFoundError
            対象ファイルが見つからない場合。

        Returns
        -------
        MergedLogDataset
            突合済みデータセット。
        """
        if start_date is None:
            raise ValueError("start_date は必須です。")
        if contract_ids is None:
            raise ValueError("contract_ids は必須です。")

        # .env を読み込んで環境変数を準備
        load_env_file()
        operation_cols = get_env_list("OPERATION_COLS", DEFAULT_OPERATION_COLS)
        state_cols = get_env_list("STATE_COLS", DEFAULT_STATE_COLS)

        files = list_input_files()
        if not files:
            raise FileNotFoundError("data フォルダに入力Excelが見つかりません。")
        latest = files[-1]  # pattern includes mmdd, sorted lexicographically

        cache_key = build_cache_key(
            latest_path=latest,
            operation_cols=operation_cols,
            state_cols=state_cols,
            contract_ids=contract_ids,
            start_date=start_date,
            end_date=end_date,
        )

        cached = load_cached(cache_key)
        if cached:
            op_df, state_df = cached
            return MergedLogDataset.build_merged_dataset(
                operation_df=op_df, state_df=state_df
            )

        op_df = pd.read_excel(
            latest,
            sheet_name="機器遠隔操作履歴",
            usecols=operation_cols,
            engine="openpyxl",
        )
        state_df = pd.read_excel(
            latest,
            sheet_name="機器状態変化履歴",
            usecols=state_cols,
            engine="openpyxl",
        )
        op_df["OrderReceiptDate"] = pd.to_datetime(
            op_df["OrderReceiptDate"], utc=True, cache=True
        )
        state_df["ReportedDate"] = pd.to_datetime(
            state_df["ReportedDate"], utc=True, cache=True
        )
        op_df = op_df[op_df["ContractId"].isin(contract_ids)]
        state_df = state_df[state_df["ContractId"].isin(contract_ids)]

        start_ts = pd.to_datetime(start_date, utc=True)
        op_df = op_df[op_df["OrderReceiptDate"] >= start_ts]
        state_df = state_df[state_df["ReportedDate"] >= start_ts]
        if end_date:
            end_ts = pd.to_datetime(end_date, utc=True)
            op_df = op_df[op_df["OrderReceiptDate"] <= end_ts]
            state_df = state_df[state_df["ReportedDate"] <= end_ts]
        store_cache(cache_key, op_df, state_df)
        return MergedLogDataset.build_merged_dataset(
            operation_df=op_df, state_df=state_df
        )
