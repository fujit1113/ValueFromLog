# AGENT_RULES

## 0. 目的 (Goal)
- PoC のための **再現性が高く信頼できる分析用コード** を Jupyter Notebook に生成すること。
- **推測しないこと。** 必要な入力が欠けている場合は **処理を中止**し、`TODO:` リストを返すこと。

## 1. 入力 (ground truth)
- **DATA_INFO:** `data/schema.dbml` ← テーブル・カラム・リレーションの **唯一の正**。
- **PLAN:** `PLAN.md`（Demand/Design、タスク、受け入れ基準を含む）

## 2. 出力 (必須)
- **Notebook:** `PoC.ipynb`
- すべての **新規コードセル** の先頭に **日本語**で次の3行コメントを付けること:
  ```python
  # WHAT: （このセルが何をするか）
  # WHY : （なぜそれが必要か）
  # CHECK: （簡単な検証や期待値）
