"""アプリケーション設定を管理するモジュール。

環境変数から設定を読み込み、アプリケーション全体で使用する設定値を提供します。
"""

import os
from typing import Any, Dict, List

import yaml
from dotenv import load_dotenv

# .envファイルがあれば読み込む
load_dotenv()


class Config:
    """設定クラス。"""

    def __init__(self) -> None:
        """設定クラスの初期化。"""
        # GCPプロジェクト設定
        self.project_id = os.getenv("GCP_PROJECT_ID")

        # BigQuery設定
        self.source_dataset = os.getenv(
            "BQ_SOURCE_DATASET", "analytics_XXXXXXXX"
        )  # GA4のデータセット
        self.target_dataset = os.getenv(
            "BQ_TARGET_DATASET", "ga4_processed"
        )  # 処理後のデータセット

        # GA4テーブル設定
        self.events_table = os.getenv(
            "GA4_EVENTS_TABLE", "events_*"
        )  # GA4イベントテーブル（ワイルドカード可）

        # 処理対象日付範囲
        self.start_date = os.getenv("START_DATE")  # 全量処理時の開始日（YYYY-MM-DD）
        self.end_date = os.getenv("END_DATE")  # 全量処理時の終了日（YYYY-MM-DD）

        # 日次処理設定
        self.days_back = int(
            os.getenv("DAYS_BACK", "1")
        )  # 日次処理時の何日前のデータを処理するか

        # Slack通知設定
        self.slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        self.slack_channel = os.getenv("SLACK_CHANNEL", "#ga4-etl-notifications")
        self.slack_username = os.getenv("SLACK_USERNAME", "GA4 ETL Bot")

        # 処理モード
        self.processing_mode = os.getenv(
            "PROCESSING_MODE", "daily"
        )  # 'daily' または 'full'

        # ログ設定
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

    def load_table_schema(self, schema_file: str) -> List[Dict[str, Any]]:
        """テーブルスキーマをYAMLファイルから読み込む。"""
        try:
            with open(schema_file, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise Exception(f"スキーマファイルの読み込みに失敗しました: {e}")

    def validate(self) -> bool:
        """設定の検証。"""
        required_vars = ["GCP_PROJECT_ID"]

        # 全量処理モードの場合は日付範囲が必要
        if self.processing_mode == "full":
            required_vars.extend(["START_DATE", "END_DATE"])

        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(
                f"必須環境変数が設定されていません: {', '.join(missing_vars)}"
            )

        return True


# 設定インスタンスを作成
config = Config()
