"""遠隔操作ログと状態変化ログのモデルと突合ロジックをまとめたモジュール。

OperationRecord は「アプリからの遠隔操作」ログのみを含み、StateChangeRecord は機器状態の全変化
（アプリ起因/非起因の両方）を含む。状態変化がアプリ起因かを判定するため、時間と属性を突合した
MergedRecord と、そのデータセットを扱う MergedLogDataset を提供する。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence
import datetime as dt
import pandas as pd
import os


@dataclass(frozen=True)
class OperationRecord:
    """機器遠隔操作履歴の1行。

    Attributes
    ----------
    contract_id : str
        契約ID。
    order_receipt_date : datetime.datetime
        受信日時。
    timer_div : str | None
        タイマー区分。
    floor_code : str | None
        フロアコード。
    room_name : str | None
        部屋名。
    equipment_type_id : str | None
        機器種別ID。
    equipment_name : str | None
        機器名称。
    property_code : str | None
        プロパティコード。
    property_name : str | None
        プロパティ名。
    property_value : str | None
        プロパティ値。
    """
    contract_id: str
    order_receipt_date: dt.datetime
    timer_div: Optional[str]
    floor_code: Optional[str]
    room_name: Optional[str]
    equipment_type_id: Optional[str]
    equipment_name: Optional[str]
    property_code: Optional[str]
    property_name: Optional[str]
    property_value: Optional[str]


@dataclass(frozen=True)
class StateChangeRecord:
    """機器状態変化履歴の1行（アプリ起因/非起因の両方を含む）。

    Attributes
    ----------
    message_name : str | None
        メッセージ種別。
    contract_id : str
        契約ID。
    reported_date : datetime.datetime
        状態変化が記録された日時。
    floor_code : str | None
        フロアコード。
    room_name : str | None
        部屋名。
    equipment_type_id : str | None
        機器種別ID。
    equipment_name : str | None
        機器名称。
    property_code1 : str | None
        プロパティコード。
    property_name1 : str | None
        プロパティ名。
    property_value1 : str | None
        プロパティ値。
    """
    message_name: Optional[str]
    contract_id: str
    reported_date: dt.datetime
    floor_code: Optional[str]
    room_name: Optional[str]
    equipment_type_id: Optional[str]
    equipment_name: Optional[str]
    property_code1: Optional[str]
    property_name1: Optional[str]
    property_value1: Optional[str]


@dataclass(frozen=True)
class MergedRecord:
    """遠隔操作と状態変化を突合した1行。

    Attributes
    ----------
    contract_id : str
        契約ID。
    state_time : datetime.datetime
        状態変化日時（ReportedDate）。
    operation_time : datetime.datetime | None
        突合した操作日時（OrderReceiptDate）。見つからない場合は ``None``。
    time_diff_seconds : float | None
        ``|state_time - operation_time|`` の秒差。操作なしの場合は ``None``。
    is_remote_operation : bool
        許容差以内で突合できたかを示す。
    floor_code : str | None
        フロアコード（状態変化側）。
    room_name : str | None
        部屋名（状態変化側）。
    equipment_type_id : str | None
        機器種別ID（状態変化側）。
    equipment_name : str | None
        機器名称（状態変化側）。
    property_code : str | None
        プロパティコード（状態変化側）。
    property_name : str | None
        プロパティ名（状態変化側）。
    property_value : str | None
        プロパティ値（状態変化側）。
    """
    contract_id: str
    state_time: dt.datetime
    operation_time: Optional[dt.datetime]
    time_diff_seconds: Optional[float]
    is_remote_operation: bool = False
    floor_code: Optional[str] = None
    room_name: Optional[str] = None
    equipment_type_id: Optional[str] = None
    equipment_name: Optional[str] = None
    property_code: Optional[str] = None
    property_name: Optional[str] = None
    property_value: Optional[str] = None


class MergedLogDataset:
    """突合結果を保持し、保存・復元を提供する DataFrame ラッパー。"""

    def __init__(self, df: pd.DataFrame):
        self.df = df

    @classmethod
    def from_parquet(cls, path: str) -> "MergedLogDataset":
        """Parquet から復元する。

        Parameters
        ----------
        path : str
            読み込むファイルパス。

        Returns
        -------
        MergedLogDataset
            復元されたデータセット。
        """
        return cls(pd.read_parquet(path))

    def to_parquet(self, path: str) -> None:
        """Parquet に保存する。

        Parameters
        ----------
        path : str
            書き出すファイルパス。
        """
        self.df.to_parquet(path, index=False)

    def to_csv(self, path: str) -> None:
        """CSV に保存する（再利用や別ツール連携用）。

        Parameters
        ----------
        path : str
            書き出すファイルパス。
        """
        self.df.to_csv(path, index=False)

    @classmethod
    def build_merged_dataset(
        cls,
        operation_df: pd.DataFrame,
        state_df: pd.DataFrame,
        tolerance_minutes: Optional[int] = None,
    ) -> "MergedLogDataset":
        """操作ログと状態変化ログを属性＋時間で突合し、分析用 DataFrame を生成する。
        一致判定に使った属性は状態変化側を残し、重複する操作側列は捨てる。
        突合成功行には ``is_remote=True`` を付与する。

        Parameters
        ----------
        operation_df : pandas.DataFrame
            機器遠隔操作履歴（OperationRecord 相当）。
        state_df : pandas.DataFrame
            機器状態変化履歴（StateChangeRecord 相当）。
        tolerance_minutes : int or None, default None
            操作と状態変化を同一事象とみなす許容差（分）。``None`` の場合は環境変数
            ``MERGE_TOLERANCE_MINUTES`` を参照し、未設定なら 5 分を用いる。

        Returns
        -------
        MergedLogDataset
            突合済みデータセット。
        """
        # 最低限必要な列だけをコピーしてメモリ確保とコピー回数を削減
        op_cols = [
            "ContractId",
            "OrderReceiptDate",
            "FloorCode",
            "RoomName",
            "EquipmentTypeId",
            "EquipmentName",
            "PropertyCode",
            "PropertyName",
            "PropertyValue",
        ]
        op = operation_df.loc[:, [c for c in op_cols if c in operation_df.columns]].copy()
        st = state_df.copy()

        if tolerance_minutes is None:
            env_val = os.getenv("MERGE_TOLERANCE_MINUTES")
            tolerance_minutes = int(env_val) if env_val else 5

        tol = pd.Timedelta(minutes=tolerance_minutes)

        # to_datetime は cache を有効化して繰り返し値を高速変換
        op["OrderReceiptDate"] = pd.to_datetime(op["OrderReceiptDate"], utc=True, cache=True)
        st["ReportedDate"] = pd.to_datetime(st["ReportedDate"], utc=True, cache=True)

        # 状態と操作で共通して比較できる属性を合わせ込んだハッシュキーを作り、
        # その単位で merge_asof することで時間以外も一致したものだけを突合。
        op.rename(
            columns={
                "PropertyCode": "PropertyCode1",
                "PropertyName": "PropertyName1",
                "PropertyValue": "PropertyValue1",
            },
            inplace=True,
        )

        key_cols = [
            "ContractId",
            "FloorCode",
            "RoomName",
            "EquipmentTypeId",
            "EquipmentName",
            "PropertyCode1",
            "PropertyName1",
            "PropertyValue1",
        ]

        for df in (op, st):
            for col in key_cols:
                if col not in df:
                    df[col] = None

        make_key = lambda df: pd.util.hash_pandas_object(  # noqa: E731
            df[key_cols].fillna(""), index=False
        )

        op_sorted = op.assign(_k=make_key(op)).sort_values(["_k", "OrderReceiptDate"])
        st_sorted = st.assign(_k=make_key(st)).sort_values(["_k", "ReportedDate"])

        merged = pd.merge_asof(
            st_sorted,
            op_sorted,
            left_on="ReportedDate",
            right_on="OrderReceiptDate",
            by="_k",
            direction="nearest",
            tolerance=tol,
            suffixes=("_state", "_op"),
        )

        merged["is_remote_operation"] = merged["OrderReceiptDate"].notna()
        merged["time_diff_seconds"] = (
            (merged["ReportedDate"] - merged["OrderReceiptDate"])
            .abs()
            .dt.total_seconds()
        )
        merged.loc[~merged["is_remote_operation"], "time_diff_seconds"] = None

        # 状態変化側の列を優先して残し、suffix を除去する
        state_cols = {c[:-6]: c for c in merged.columns if c.endswith("_state")}
        merged.rename(columns={v: k for k, v in state_cols.items()}, inplace=True)

        # 出力用の汎用プロパティ名を付与（状態側を優先）
        merged["property_code"] = merged["PropertyCode1"] if "PropertyCode1" in merged else None
        merged["property_name"] = merged["PropertyName1"] if "PropertyName1" in merged else None
        merged["property_value"] = merged["PropertyValue1"] if "PropertyValue1" in merged else None

        ensure_cols = [
            "FloorCode",
            "RoomName",
            "EquipmentTypeId",
            "EquipmentName",
            "property_code",
            "property_name",
            "property_value",
        ]
        for col in ensure_cols:
            if col not in merged:
                merged[col] = None

        cols_front = [
            "ContractId",
            "ReportedDate",
            "OrderReceiptDate",
            "time_diff_seconds",
            "is_remote_operation",
            "FloorCode",
            "RoomName",
            "EquipmentTypeId",
            "EquipmentName",
            "property_code",
            "property_name",
            "property_value",
        ]
        drop_cols = set(state_cols.values()) | {"_k"}
        remaining = [
            c
            for c in merged.columns
            if c not in cols_front and c not in drop_cols and not c.endswith("_op")
        ]
        final_df = merged[cols_front + remaining]

        return cls(final_df)
