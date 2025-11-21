"""インフラ層: pandas で Excel ログを読むリポジトリ実装."""
from typing import Tuple
import pandas as pd
from .paths import list_input_files
from .env_settings import load_env_file, get_env_list
from .cache import load_cached, store_cache
import hashlib

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
    """data/ 配下の最新ファイルを読み込むリポジトリ."""

    def fetch_latest(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """最新Excelから (操作履歴DF, 状態変化DF) を返す。

        Raises:
            FileNotFoundError: 対象ファイルが見つからない場合。
        """
        # .env を読み込んで環境変数を準備
        load_env_file()
        operation_cols = get_env_list("OPERATION_COLS", DEFAULT_OPERATION_COLS)
        state_cols = get_env_list("STATE_COLS", DEFAULT_STATE_COLS)

        files = list_input_files()
        if not files:
            raise FileNotFoundError("data フォルダに入力Excelが見つかりません。")
        latest = files[-1]  # pattern includes mmdd, sorted lexicographically

        # キャッシュキー: ファイルパス + 更新時刻 + 列構成のハッシュ
        hasher = hashlib.sha256()
        hasher.update(str(latest).encode("utf-8"))
        hasher.update(str(latest.stat().st_mtime_ns).encode("utf-8"))
        hasher.update(",".join(operation_cols + state_cols).encode("utf-8"))
        cache_key = hasher.hexdigest()

        cached = load_cached(cache_key)
        if cached:
            return cached

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
        store_cache(cache_key, op_df, state_df)
        return op_df, state_df
