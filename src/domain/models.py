"""ドメインエンティティと分析用マージモデル定義。

OperationRecord は「アプリからの遠隔操作」ログのみを含む。
StateChangeRecord は機器状態の全変化（アプリ起因/非起因どちらも）を含む。
したがって状態変化がアプリ起因かどうかは、Operation と State を突合して
時間乖離が閾値以内かどうかで判定する必要がある。

本モジュールでは、突合結果を保持する MergedRecord と DataFrame ラッパーを提供する。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence
import datetime as dt
import pandas as pd
import os


@dataclass(frozen=True)
class OperationRecord:
    """機器遠隔操作履歴1行分（アプリ発、遠隔操作のみ）。"""
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
    """機器状態変化履歴1行分（アプリ起因/非起因の両方を含む）。"""
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

    Attributes:
        contract_id: 契約ID
        state_time: 状態変化日時 (ReportedDate)
        operation_time: 突合した操作日時 (OrderReceiptDate) または None
        time_diff_seconds: |state_time - operation_time| の秒差。操作なしの場合 None
        is_remote_operation: 閾値以内に突合できたら True、操作なしなら False
        state_payload: 状態変化側の属性を辞書で保持
        operation_payload: 操作側の属性を辞書で保持（存在しない場合は {}）
    """
    contract_id: str
    state_time: dt.datetime
    operation_time: Optional[dt.datetime]
    time_diff_seconds: Optional[float]
    is_remote_operation: bool
    state_payload: dict
    operation_payload: dict


class MergedLogDataset:
    """突合結果を保持し、保存/復元を提供する DataFrame ラッパー。"""

    def __init__(self, df: pd.DataFrame):
        self.df = df

    @classmethod
    def from_parquet(cls, path: str) -> "MergedLogDataset":
        """Parquet から復元。"""
        return cls(pd.read_parquet(path))

    def to_parquet(self, path: str) -> None:
        """Parquet に保存。"""
        self.df.to_parquet(path, index=False)

    def to_csv(self, path: str) -> None:
        """CSV に保存（再利用・別ツール連携用）。"""
        self.df.to_csv(path, index=False)


def build_merged_dataset(
    operation_df: pd.DataFrame,
    state_df: pd.DataFrame,
    tolerance_minutes: Optional[int] = None,
) -> MergedLogDataset:
    """操作ログと状態変化ログを時間突合し、分析用 DataFrame を返す。

    Args:
        operation_df: 機器遠隔操作履歴 (OperationRecord 相当)
        state_df: 機器状態変化履歴 (StateChangeRecord 相当)
        tolerance_minutes: 操作と状態変化を同一事象とみなす許容差（分）。
            None の場合、環境変数 `MERGE_TOLERANCE_MINUTES` を参照し、
            未設定なら 5 分。

    Returns:
        MergedLogDataset: 突合済みデータセット
    """
    # 許容差を決定（環境変数優先）
    if tolerance_minutes is None:
        env_val = os.getenv("MERGE_TOLERANCE_MINUTES")
        tolerance_minutes = int(env_val) if env_val else 5

    # 時刻を datetime に確実に変換
    op = operation_df.copy()
    st = state_df.copy()
    op["OrderReceiptDate"] = pd.to_datetime(op["OrderReceiptDate"])
    st["ReportedDate"] = pd.to_datetime(st["ReportedDate"])

    # ContractId ごとに時間順ソートして merge_asof を実施
    op_sorted = op.sort_values(["ContractId", "OrderReceiptDate"])
    st_sorted = st.sort_values(["ContractId", "ReportedDate"])

    merged = pd.merge_asof(
        left=st_sorted,
        right=op_sorted,
        left_on="ReportedDate",
        right_on="OrderReceiptDate",
        by="ContractId",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=tolerance_minutes),
        suffixes=("_state", "_op"),
    )

    # 判定フラグと時間差（秒）を算出
    merged["is_remote_operation"] = merged["OrderReceiptDate"].notna()
    merged["time_diff_seconds"] = (
        (merged["ReportedDate"] - merged["OrderReceiptDate"])
        .abs()
        .dt.total_seconds()
    )

    # 分析しやすい列順に並べ替え
    cols_front = [
        "ContractId",
        "ReportedDate",
        "OrderReceiptDate",
        "time_diff_seconds",
        "is_remote_operation",
    ]
    remaining = [c for c in merged.columns if c not in cols_front]
    final_df = merged[cols_front + remaining]

    return MergedLogDataset(final_df)
