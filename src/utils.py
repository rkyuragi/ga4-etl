"""ユーティリティ関数を提供するモジュール。

BigQueryクライアントの作成、日付操作、ロギング設定などの共通機能を提供します。
"""

import logging
from datetime import datetime, timedelta

from google.cloud import bigquery

from config import config


# ロガーの設定
def setup_logger() -> logging.Logger:
    """ロガーを設定する。"""
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)

    # ルートロガーの設定
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # コンソールハンドラの設定
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    # フォーマッタの設定
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    # ハンドラの追加（既存のハンドラがあれば追加しない）
    if not logger.handlers:
        logger.addHandler(console_handler)

    return logger


# 日付関連のユーティリティ関数
def get_target_date(days_back=1) -> str:
    """処理対象日を取得する。

    Args:
        days_back (int): 何日前のデータを処理するか

    Returns:
        str: YYYY-MM-DD形式の日付文字列
    """
    target_date = datetime.now() - timedelta(days=days_back)
    return target_date.strftime("%Y-%m-%d")


def get_date_range(start_date: str, end_date: str) -> list[str]:
    """日付範囲を取得する。

    Args:
        start_date (str): 開始日（YYYY-MM-DD）
        end_date (str): 終了日（YYYY-MM-DD）

    Returns:
        list: YYYY-MM-DD形式の日付文字列のリスト
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    date_range = []
    current = start

    while current <= end:
        date_range.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return date_range


def get_partition_suffix(date_str: str) -> str:
    """BigQueryパーティションサフィックスを取得する。

    Args:
        date_str (str): YYYY-MM-DD形式の日付文字列

    Returns:
        str: YYYYMMDD形式のパーティションサフィックス
    """
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj.strftime("%Y%m%d")


# BigQuery関連のユーティリティ関数
def create_bq_client() -> bigquery.Client:
    """BigQueryクライアントを作成する。"""
    return bigquery.Client(project=config.project_id)


def ensure_dataset_exists(client: bigquery.Client, dataset_id: str) -> bigquery.Dataset:
    """データセットが存在することを確認し、存在しなければ作成する。

    Args:
        client: BigQueryクライアント
        dataset_id (str): データセットID

    Returns:
        google.cloud.bigquery.dataset.Dataset: データセットオブジェクト
    """
    dataset_ref = client.dataset(dataset_id)

    try:
        dataset = client.get_dataset(dataset_ref)
        logging.info(f"データセット {dataset_id} は既に存在します")
    except Exception:
        # データセットが存在しない場合は作成
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # ロケーションを設定
        dataset = client.create_dataset(dataset)
        logging.info(f"データセット {dataset_id} を作成しました")

    return dataset


def table_exists(client: bigquery.Client, dataset_id: str, table_id: str) -> bool:
    """テーブルが存在するかどうかを確認する。

    Args:
        client: BigQueryクライアント
        dataset_id (str): データセットID
        table_id (str): テーブルID

    Returns:
        bool: テーブルが存在する場合はTrue、存在しない場合はFalse
    """
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        client.get_table(table_ref)
        return True
    except Exception:
        return False


def get_table_schema(schema_file: str) -> list[bigquery.SchemaField]:
    """テーブルスキーマを取得する。

    Args:
        schema_file (str): スキーマファイルのパス

    Returns:
        list: BigQueryスキーマフィールドのリスト
    """
    schema_data = config.load_table_schema(schema_file)

    # スキーマフィールドのリストを作成
    schema = []
    for field in schema_data:
        if "fields" in field:
            # ネストされたフィールドの場合
            nested_fields = [
                bigquery.SchemaField(
                    f["name"],
                    f["type"],
                    mode=f.get("mode", "NULLABLE"),
                    description=f.get("description", ""),
                )
                for f in field["fields"]
            ]

            schema.append(
                bigquery.SchemaField(
                    field["name"],
                    field["type"],
                    mode=field.get("mode", "NULLABLE"),
                    description=field.get("description", ""),
                    fields=nested_fields,
                )
            )
        else:
            # 通常のフィールドの場合
            schema.append(
                bigquery.SchemaField(
                    field["name"],
                    field["type"],
                    mode=field.get("mode", "NULLABLE"),
                    description=field.get("description", ""),
                )
            )

    return schema


# エラーハンドリング関数
def format_error(e: Exception) -> str:
    """例外オブジェクトをフォーマットする。

    Args:
        e (Exception): 例外オブジェクト

    Returns:
        str: フォーマットされたエラーメッセージ
    """
    error_type = type(e).__name__
    error_message = str(e)

    return f"{error_type}: {error_message}"
