from __future__ import annotations

from pathlib import Path

import nbformat as nbf


def jp_header(what: str, why: str, check: str) -> str:
    return (
        f"# WHAT: {what}\n"
        f"# WHY : {why}\n"
        f"# CHECK: {check}\n"
    )


def build_notebook() -> nbf.NotebookNode:
    nb = nbf.v4.new_notebook()
    cells = []

    intro_md = (
        "# PoC: 操作ログと状態イベントの初期分析\n\n"
        "このノートブックでは、`equipment_control_logs` と `equipment_status_events` の両データセットを用いて、"
        "操作ログの傾向把握と状態イベントとの関係性を検証します。分析要件は `PLAN.md` に基づき、"
        "再現性を重視した手順を記録します。"
    )
    cells.append(nbf.v4.new_markdown_cell(intro_md))

    cell1_code = (
        jp_header(
            "\u5206\u6790\u3067\u5229\u7528\u3059\u308b\u4e3b\u8981\u30e9\u30a4\u30d6\u30e9\u30ea\u306e\u8aad\u307f\u8fbc\u307f\u3068\u8868\u793a\u8a2d\u5b9a\u306e\u521d\u671f\u5316",
            "\u518d\u73fe\u6027\u306e\u9ad8\u3044\u74b0\u5883\u3092\u6e96\u5099\u3057\u7d50\u679c\u306e\u8aad\u307f\u3084\u3059\u3055\u3092\u78ba\u4fdd\u3059\u308b\u305f\u3081",
            "pandas \u3068 matplotlib/seaborn \u306e\u30d0\u30fc\u30b8\u30e7\u30f3\u304c\u8868\u793a\u3055\u308c\u308b\u3053\u3068",
        )
        + """from pathlib import Path
import warnings

import pandas as pd

try:
    import matplotlib.pyplot as plt  # noqa: F401
    HAS_MPL = True
except ImportError:
    plt = None
    HAS_MPL = False

try:
    import seaborn as sns  # noqa: F401
except ImportError:
    sns = None

pd.set_option("display.max_columns", 40)
pd.set_option("display.max_rows", 20)
pd.set_option("display.float_format", "{:.3f}".format)

print(f"pandas={pd.__version__}")
if HAS_MPL:
    import matplotlib as mpl  # noqa: WPS433

    print(f"matplotlib={mpl.__version__}")
else:
    print("matplotlib=NOT INSTALLED")
if sns is not None:
    print(f"seaborn={sns.__version__}")
else:
    print("seaborn=NOT INSTALLED")
"""
    )
    cells.append(nbf.v4.new_code_cell(cell1_code))

    cell2_code = (
        jp_header(
            "\u30c7\u30fc\u30bf\u30d5\u30a1\u30a4\u30eb\u306e\u30d1\u30b9\u3068\u8aad\u307f\u8fbc\u307f\u30ed\u30b8\u30c3\u30af\u3092\u5b9a\u7fa9\u3059\u308b",
            "\u30c7\u30fc\u30bf\u30bd\u30fc\u30b9\u3092\u4e00\u5143\u7ba1\u7406\u3057\u524d\u51e6\u7406\u3092\u518d\u5229\u7528\u3067\u304d\u308b\u3088\u3046\u306b\u3059\u308b\u305f\u3081",
            "\u8aad\u307f\u8fbc\u307f\u95a2\u6570\u3092\u547c\u3076\u3068 DataFrame \u306e\u57fa\u672c\u60c5\u5831\u304c\u5f97\u3089\u308c\u308b\u3053\u3068",
        )
        + """from typing import List

DATA_DIR = Path("data")
CONTROL_PATH = DATA_DIR / "equipment_control_logs.csv"
STATUS_PATH = DATA_DIR / "equipment_status_events.csv"
NA_TOKENS = ["NULL"]
BOOL_MAP = {"TRUE": True, "FALSE": False, "true": True, "false": False}


def parse_datetimes(df: pd.DataFrame, columns: List[str], *, utc: bool = True) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            continue
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            df[column] = pd.to_datetime(df[column], errors="coerce", utc=utc)
    return df


def load_control_logs() -> pd.DataFrame:
    df = pd.read_csv(CONTROL_PATH, encoding="utf-8-sig", na_values=NA_TOKENS)
    datetime_cols = [
        "OrderReceiptDate",
        "TimerSetDate",
        "CompleteDate",
        "CreatedDate",
        "UpdatedDate",
        "DeletedDate",
    ]
    df = parse_datetimes(df, datetime_cols)
    df["IsDelete"] = df["IsDelete"].fillna(0).astype(bool)
    df["ControlResult"] = df["ControlResult"].astype("category")
    df["EquipmentTypeId"] = df["EquipmentTypeId"].astype("category")
    df["duration_to_complete_min"] = (
        df["CompleteDate"] - df["OrderReceiptDate"]
    ).dt.total_seconds() / 60
    df["has_error"] = df[["ErrorCode", "ErrorReason"]].notna().any(axis=1)
    return df


def load_status_events() -> pd.DataFrame:
    df = pd.read_csv(STATUS_PATH, encoding="utf-8-sig", na_values=NA_TOKENS)
    datetime_cols = [
        "ReportedDate",
        "DetectionDate",
        "ErrorDate",
        "CreatedDate",
        "UpdatedDate",
        "DeletedDate",
    ]
    df = parse_datetimes(df, datetime_cols)
    df["IsDelete"] = df["IsDelete"].fillna(0).astype(bool)
    if "MessageName" in df.columns:
        df["MessageName"] = df["MessageName"].astype("category")
    if "AliveStatus" in df.columns:
        df["AliveStatus"] = df["AliveStatus"].astype("category")
    return df


def extract_status_properties(df: pd.DataFrame, prefix: str, limit: int) -> pd.DataFrame:
    records = []
    for ordinal in range(1, limit + 1):
        code_col = f"{prefix}Code{ordinal}"
        name_col = f"{prefix}Name{ordinal}"
        value_col = f"{prefix}Value{ordinal}"
        desc_col = f"{prefix}Description{ordinal}"
        available = [
            col
            for col in (code_col, name_col, value_col, desc_col)
            if col in df.columns
        ]
        if not available:
            continue
        subset = df[["Id"] + available].copy()
        subset.rename(
            columns={
                "Id": "event_id",
                code_col: "code",
                name_col: "name",
                value_col: "value",
                desc_col: "description",
            },
            inplace=True,
        )
        subset["ordinal"] = ordinal
        subset.dropna(subset=["code", "name", "value"], how="all", inplace=True)
        subset["value"] = subset["value"].replace(BOOL_MAP)
        records.append(subset)
    if not records:
        return pd.DataFrame(
            columns=["event_id", "code", "name", "value", "description", "ordinal"]
        )
    return pd.concat(records, ignore_index=True)
"""
    )
    cells.append(nbf.v4.new_code_cell(cell2_code))

    cell3_code = (
        jp_header(
            "\u64cd\u4f5c\u30ed\u30b0\u3068\u72b6\u614b\u30a4\u30d9\u30f3\u30c8\u3092\u8aad\u307f\u8fbc\u307f\u3001\u4ef6\u6570\u3084\u6b20\u640d\u306e\u6982\u6cc1\u3092\u78ba\u8a8d\u3059\u308b",
            "\u4ee5\u964d\u306e\u96c6\u8a08\u3067\u524d\u63d0\u3068\u3059\u308b\u30c7\u30fc\u30bf\u54c1\u8cea\u3068\u91cf\u3092\u628a\u63a7\u3059\u308b\u305f\u3081",
            "\u5404\u30c7\u30fc\u30bf\u30bb\u30c3\u30c8\u306e\u5f62\u72b6\u3068\u4e3b\u306a\u6b20\u640d\u7387\u304c\u51fa\u529b\u3055\u308c\u308b\u3053\u3068",
        )
        + """
control_df = load_control_logs()
status_df = load_status_events()

summary = (
    pd.DataFrame(
        {
            "rows": [len(control_df), len(status_df)],
            "columns": [control_df.shape[1], status_df.shape[1]],
        },
        index=["equipment_control_logs", "equipment_status_events"],
    ).assign(
        non_null_ratio=[
            1 - control_df.isna().mean().mean(),
            1 - status_df.isna().mean().mean(),
        ]
    )
)
summary
"""
    )
    cells.append(nbf.v4.new_code_cell(cell3_code))

    cell4_code = (
        jp_header(
            "\u30c7\u30fc\u30bf\u30b5\u30f3\u30d7\u30eb\u3068\u8ffd\u52a0\u6307\u6a19\u3092\u53ef\u8996\u5316\u524d\u306b\u70b9\u691c\u3059\u308b",
            "\u30ab\u30c6\u30b4\u30ea\u5024\u3084\u6d3b\u7528\u6307\u6a19\u306e\u59a5\u5f53\u6027\u3092\u624b\u901f\u304f\u78ba\u8a8d\u3059\u308b\u305f\u3081",
            "\u30b5\u30f3\u30d7\u30eb\u8868\u793a\u3068\u6d3b\u7528\u5217\u306e\u7d71\u8a08\u91cf\u304c\u78ba\u8a8d\u3067\u304d\u308b\u3053\u3068",
        )
        + """
control_sample = control_df[
    [
        "Id",
        "BuildingId",
        "EquipmentTypeId",
        "OrderReceiptDate",
        "CompleteDate",
        "ControlResult",
        "duration_to_complete_min",
        "has_error",
    ]
].head()
status_sample = status_df[
    ["Id", "MessageName", "AliveStatus", "ReportedDate", "EquipmentTypeId", "EquipmentId"]
].head()

control_stats = control_df["duration_to_complete_min"].describe(percentiles=[0.5, 0.9])
control_sample, status_sample, control_stats
"""
    )
    cells.append(nbf.v4.new_code_cell(cell4_code))

    cell5_code = (
        jp_header(
            "\u72b6\u614b\u30a4\u30d9\u30f3\u30c8\u306e\u30d7\u30ed\u30d1\u30c6\u30a3\u5217\u3092\u6b63\u898f\u5316\u3057\u3001\u8a73\u7d30\u9805\u76ee\u306e\u628a\u63a7\u3092\u5bb9\u6613\u306b\u3059\u308b",
            "\u53ef\u5909\u9577\u306e\u30d7\u30ed\u30d1\u30c6\u30a3\u60c5\u5831\u3092\u96c6\u8a08\u3084\u7d50\u5408\u306b\u6d3b\u7528\u3067\u304d\u308b\u3088\u3046\u306b\u3059\u308b\u305f\u3081",
            "\u62bd\u51fa\u3057\u305f\u30d7\u30ed\u30d1\u30c6\u30a3\u306e\u4ef6\u6570\u3068\u30b5\u30f3\u30d7\u30eb\u304c\u78ba\u8a8d\u3067\u304d\u308b\u3053\u3068",
        )
        + """
status_props = extract_status_properties(status_df, prefix="Property", limit=10)
status_error_props = extract_status_properties(status_df, prefix="ErrorProperty", limit=5)

props_summary = {
    "property_records": len(status_props),
    "error_property_records": len(status_error_props),
}
status_props.head(), status_error_props.head(), props_summary
"""
    )
    cells.append(nbf.v4.new_code_cell(cell5_code))

    cell6_code = (
        jp_header(
            "\u64cd\u4f5c\u30ed\u30b0\u306e\u4ef6\u6570\u63a8\u79fb\u3068\u5236\u5fa1\u7d50\u679c\u306e\u5185\u8a33\u3092\u96c6\u8a08\u30fb\u53ef\u8996\u5316\u3059\u308b",
            "\u64cd\u4f5c\u6d3b\u52d5\u306e\u30d4\u30fc\u30af\u3084\u6210\u529f\u7387\u3092\u628a\u63a7\u3057\u91cd\u70b9\u7684\u306b\u8abf\u67fb\u3059\u3079\u304d\u9818\u57df\u3092\u62bd\u51fa\u3059\u308b\u305f\u3081",
            "\u65e5\u6b21\u4ef6\u6570\u30c6\u30fc\u30d6\u30eb\u3068\u5236\u5fa1\u7d50\u679c\u306e\u96c6\u8a08\u3001\u304a\u3088\u3073\u53ef\u8996\u5316\u307e\u305f\u306f\u4ee3\u66ff\u51fa\u529b\u304c\u5f97\u3089\u308c\u308b\u3053\u3068",
        )
        + """
control_daily = (
    control_df.dropna(subset=["CompleteDate"])
    .assign(complete_local=lambda df: df["CompleteDate"].dt.tz_convert("Asia/Tokyo"))
    .groupby([pd.Grouper(key="complete_local", freq="D"), "EquipmentTypeId"], observed=True)
    .size()
    .reset_index(name="count")
)

result_by_type = (
    control_df.groupby(["EquipmentTypeId", "ControlResult"], observed=True)
    .size()
    .unstack(fill_value=0)
    .assign(total=lambda df: df.sum(axis=1))
    .sort_values("total", ascending=False)
)

if HAS_MPL and sns is not None:
    plt.figure(figsize=(8, 4))
    sns.lineplot(data=control_daily, x="complete_local", y="count", hue="EquipmentTypeId", marker="o")
    plt.title("日次の操作件数（設備種別別）")
    plt.xlabel("完了日 (JST)")
    plt.ylabel("件数")
    plt.xticks(rotation=45)
    plt.tight_layout()
else:
    print("INFO: matplotlib / seaborn が未インストールのためグラフ描画をスキップしました。")
    print("以下の集計結果を参考にしてください。")

control_daily, result_by_type
"""
    )
    cells.append(nbf.v4.new_code_cell(cell6_code))

    cell7_code = (
        jp_header(
            "\u64cd\u4f5c\u30ed\u30b0\u3068\u72b6\u614b\u30a4\u30d9\u30f3\u30c8\u3092\u6a5f\u5668\u5358\u4f4d\u30fb\u6642\u9593\u8fd1\u90e8\u3067\u7a81\u304d\u5408\u308f\u305b\u308b",
            "\u64cd\u4f5c\u5f8c\u306e\u72b6\u614b\u901a\u77e5\u306e\u6709\u7121\u3084\u6642\u5dee\u3092\u691c\u8a3c\u3057\u3001\u9023\u643a\u72b6\u6cc1\u3092\u628a\u63a7\u3059\u308b\u305f\u3081",
            "\u30de\u30c3\u30c1\u30f3\u30b0\u4ef6\u6570\u30fb\u9045\u5ef6\u5206\u6570\u30fb\u4ee3\u8868\u30b5\u30f3\u30d7\u30eb\u304c\u78ba\u8a8d\u3067\u304d\u308b\u3053\u3068",
        )
        + """
join_keys = ["EdgeManagedEquipmentId"]
matchable_controls = (
    control_df.dropna(subset=["CompleteDate"] + join_keys).sort_values("CompleteDate")
)
matchable_status = (
    status_df.dropna(subset=["ReportedDate"] + join_keys).sort_values("ReportedDate")
)

matched = pd.merge_asof(
    matchable_controls,
    matchable_status,
    left_on="CompleteDate",
    right_on="ReportedDate",
    by="EdgeManagedEquipmentId",
    direction="nearest",
    tolerance=pd.Timedelta("1h"),
    suffixes=("_control", "_status"),
)

matched["lag_minutes"] = (
    matched["ReportedDate"] - matched["CompleteDate"]
).dt.total_seconds() / 60
match_summary = matched["Id_status"].notna().value_counts().rename(
    index={True: "matched", False: "unmatched"}
)
lag_stats = (
    matched.loc[matched["Id_status"].notna(), "lag_minutes"].describe()
    if matched["Id_status"].notna().any()
    else "状態イベントとのマッチがありません"
)

matched_sample = matched.loc[
    matched["Id_status"].notna(),
    [
        "Id_control",
        "Id_status",
        "EdgeManagedEquipmentId",
        "CompleteDate",
        "ReportedDate",
        "lag_minutes",
        "ControlResult",
        "MessageName",
    ],
].head()

match_summary, lag_stats, matched_sample
"""
    )
    cells.append(nbf.v4.new_code_cell(cell7_code))

    outro_md = (
        "## メモ\n\n"
        "- グラフ描画がスキップされた場合は、`pip install matplotlib seaborn` を実行して再度セルを実行してください。\n"
        "- 追加で確認したいメトリクスがあれば、`control_df` や `status_df` を基にセルを追記してください。"
    )
    cells.append(nbf.v4.new_markdown_cell(outro_md))

    nb["cells"] = cells
    nb["metadata"]["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }
    nb["metadata"]["language_info"] = {
        "name": "python",
        "version": "3.11",
    }
    return nb


def main() -> None:
    notebook = build_notebook()
    output_path = Path("PoC.ipynb")
    output_path.write_text(nbf.writes(notebook), encoding="utf-8")
    print(f"Notebook written to {output_path.resolve()}")


if __name__ == "__main__":
    main()

