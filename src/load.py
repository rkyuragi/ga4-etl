"""GA4データをBigQueryにロードするモジュール。

変換されたGA4データを適切なテーブル構造でBigQueryにロードする機能を提供します。
"""

import logging

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from config import config
from src.utils import create_bq_client, ensure_dataset_exists, get_partition_suffix

logger = logging.getLogger(__name__)


class GA4Loader:
    """GA4データロードクラス。"""

    def __init__(self) -> None:
        """GA4データロードクラスの初期化。"""
        self.client = create_bq_client()
        self.project_id = config.project_id
        self.target_dataset = config.target_dataset

        # ターゲットデータセットが存在することを確認
        ensure_dataset_exists(self.client, self.target_dataset)

    def load_events_table(self, df: pd.DataFrame, target_date: str) -> bool:
        """イベントテーブルをロードする。

        Args:
            df (pandas.DataFrame): 変換済みのイベントデータ
            target_date (str): 対象日（YYYY-MM-DD形式）

        Returns:
            bool: 成功時はTrue、失敗時はFalse
        """
        if df.empty:
            logger.warning(
                f"{target_date}のイベントデータが空のため、ロードをスキップします"
            )
            return False

        table_id = "events"
        partition_suffix = get_partition_suffix(target_date)

        # パーティションテーブル名を作成
        full_table_id = (
            f"{self.project_id}.{self.target_dataset}.{table_id}${partition_suffix}"
        )

        logger.info(f"{full_table_id}にデータをロードします（{len(df)}行）")

        try:
            # テーブルが存在するか確認
            if not self._ensure_events_table_exists(table_id):
                logger.error(f"テーブル{table_id}の作成に失敗しました")
                return False

            # パーティションが存在する場合は削除
            self._delete_partition_if_exists(table_id, partition_suffix)

            # データをロード
            job_config = bigquery.LoadJobConfig(
                # スキーマは自動検出
                autodetect=True,
                # 書き込みモード
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                # パーティション情報
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY, field="date"
                ),
            )

            # DataFrameをBigQueryにロード
            job = self.client.load_table_from_dataframe(
                df, full_table_id, job_config=job_config
            )

            # ジョブの完了を待機
            job.result()

            logger.info(f"{full_table_id}へのデータロードが完了しました")
            return True

        except Exception as e:
            logger.error(
                f"{full_table_id}へのデータロード中にエラーが発生しました: {e}"
            )
            return False

    def load_sessions_table(self, df: pd.DataFrame, target_date: str) -> bool:
        """セッションテーブルをロードする。

        Args:
            df (pandas.DataFrame): セッションデータ
            target_date (str): 対象日（YYYY-MM-DD形式）

        Returns:
            bool: 成功時はTrue、失敗時はFalse
        """
        if df.empty:
            logger.warning(
                f"{target_date}のセッションデータが空のため、ロードをスキップします"
            )
            return False

        table_id = "sessions"
        partition_suffix = get_partition_suffix(target_date)

        # パーティションテーブル名を作成
        full_table_id = (
            f"{self.project_id}.{self.target_dataset}.{table_id}${partition_suffix}"
        )

        logger.info(f"{full_table_id}にデータをロードします（{len(df)}行）")

        try:
            # テーブルが存在するか確認
            if not self._ensure_sessions_table_exists(table_id):
                logger.error(f"テーブル{table_id}の作成に失敗しました")
                return False

            # パーティションが存在する場合は削除
            self._delete_partition_if_exists(table_id, partition_suffix)

            # データをロード
            job_config = bigquery.LoadJobConfig(
                # スキーマは自動検出
                autodetect=True,
                # 書き込みモード
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                # パーティション情報
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY, field="date"
                ),
            )

            # DataFrameをBigQueryにロード
            job = self.client.load_table_from_dataframe(
                df, full_table_id, job_config=job_config
            )

            # ジョブの完了を待機
            job.result()

            logger.info(f"{full_table_id}へのデータロードが完了しました")
            return True

        except Exception as e:
            logger.error(
                f"{full_table_id}へのデータロード中にエラーが発生しました: {e}"
            )
            return False

    def load_user_profiles(self, df: pd.DataFrame) -> bool:
        """ユーザープロファイルテーブルをロードする。

        Args:
            df (pandas.DataFrame): ユーザープロファイルデータ

        Returns:
            bool: 成功時はTrue、失敗時はFalse
        """
        if df.empty:
            logger.warning(
                "ユーザープロファイルデータが空のため、ロードをスキップします"
            )
            return False

        table_id = "user_profiles"

        # テーブル名を作成
        full_table_id = f"{self.project_id}.{self.target_dataset}.{table_id}"

        logger.info(f"{full_table_id}にデータをロードします（{len(df)}行）")

        try:
            # テーブルが存在するか確認
            if not self._ensure_user_profiles_table_exists(table_id):
                logger.error(f"テーブル{table_id}の作成に失敗しました")
                return False

            # 既存のユーザーIDを取得
            existing_users_query = f"""
            SELECT user_pseudo_id
            FROM `{full_table_id}`
            WHERE user_pseudo_id IN UNNEST(@user_ids)
            """

            user_ids = df["user_pseudo_id"].tolist()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("user_ids", "STRING", user_ids)
                ]
            )

            existing_users_df = self.client.query(
                existing_users_query, job_config=job_config
            ).to_dataframe()

            existing_user_ids = set(existing_users_df["user_pseudo_id"].tolist())

            # 新規ユーザーと既存ユーザーに分割
            new_users_df = df[~df["user_pseudo_id"].isin(existing_user_ids)]
            update_users_df = df[df["user_pseudo_id"].isin(existing_user_ids)]

            # 新規ユーザーを挿入
            if not new_users_df.empty:
                logger.info(f"新規ユーザー{len(new_users_df)}件を挿入します")

                job_config = bigquery.LoadJobConfig(
                    autodetect=True,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                )

                job = self.client.load_table_from_dataframe(
                    new_users_df, full_table_id, job_config=job_config
                )

                job.result()

            # 既存ユーザーを更新
            if not update_users_df.empty:
                logger.info(f"既存ユーザー{len(update_users_df)}件を更新します")

                # 一時テーブルを作成して既存ユーザーを更新
                temp_table_id = f"{full_table_id}_temp"

                # 一時テーブルにデータをロード
                job_config = bigquery.LoadJobConfig(
                    autodetect=True,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                )

                job = self.client.load_table_from_dataframe(
                    update_users_df, temp_table_id, job_config=job_config
                )

                job.result()

                # MERGEクエリで更新
                merge_query = f"""
                MERGE `{full_table_id}` T
                USING `{temp_table_id}` S
                ON T.user_pseudo_id = S.user_pseudo_id
                WHEN MATCHED THEN
                  UPDATE SET
                    T.last_seen = S.last_seen,
                    T.session_count = S.session_count,
                    T.event_count = S.event_count,
                    T.most_used_device = S.most_used_device,
                    T.most_used_os = S.most_used_os,
                    T.country = S.country,
                    T.last_updated = S.last_updated
                """

                self.client.query(merge_query).result()

                # 一時テーブルを削除
                self.client.delete_table(temp_table_id, not_found_ok=True)

            logger.info(f"{full_table_id}へのデータロードが完了しました")
            return True

        except Exception as e:
            logger.error(
                f"{full_table_id}へのデータロード中にエラーが発生しました: {e}"
            )
            return False

    def _ensure_events_table_exists(self, table_id: str) -> bool:
        """イベントテーブルが存在することを確認し、存在しなければ作成する。"""
        table_ref = self.client.dataset(self.target_dataset).table(table_id)

        try:
            self.client.get_table(table_ref)
            logger.info(f"テーブル{table_id}は既に存在します")
            return True
        except NotFound:
            # テーブルが存在しない場合は作成
            logger.info(f"テーブル{table_id}を作成します")

            # テーブルスキーマを定義
            schema = [
                bigquery.SchemaField("event_name", "STRING", description="イベント名"),
                bigquery.SchemaField("user_id", "STRING", description="ユーザーID"),
                bigquery.SchemaField(
                    "user_pseudo_id", "STRING", description="匿名ユーザーID"
                ),
                bigquery.SchemaField(
                    "platform", "STRING", description="プラットフォーム"
                ),
                bigquery.SchemaField("date", "DATE", description="イベント日付"),
                bigquery.SchemaField(
                    "timestamp", "TIMESTAMP", description="イベントタイムスタンプ"
                ),
                # デバイス情報
                bigquery.SchemaField(
                    "device_category", "STRING", description="デバイスカテゴリ"
                ),
                bigquery.SchemaField(
                    "device_mobile_brand_name",
                    "STRING",
                    description="モバイルブランド名",
                ),
                bigquery.SchemaField(
                    "device_mobile_model_name", "STRING", description="モバイルモデル名"
                ),
                bigquery.SchemaField(
                    "device_operating_system", "STRING", description="OS"
                ),
                bigquery.SchemaField("device_language", "STRING", description="言語"),
                # 地理情報
                bigquery.SchemaField("geo_country", "STRING", description="国"),
                bigquery.SchemaField("geo_region", "STRING", description="地域"),
                bigquery.SchemaField("geo_city", "STRING", description="都市"),
                # トラフィックソース
                bigquery.SchemaField(
                    "traffic_source_name", "STRING", description="トラフィックソース名"
                ),
                bigquery.SchemaField(
                    "traffic_source_medium",
                    "STRING",
                    description="トラフィックソースメディアム",
                ),
                bigquery.SchemaField(
                    "traffic_source_source", "STRING", description="トラフィックソース"
                ),
                # 共通パラメータ
                bigquery.SchemaField(
                    "page_location", "STRING", description="ページURL"
                ),
                bigquery.SchemaField(
                    "page_title", "STRING", description="ページタイトル"
                ),
                bigquery.SchemaField(
                    "page_referrer", "STRING", description="リファラー"
                ),
                bigquery.SchemaField(
                    "session_id", "STRING", description="セッションID"
                ),
                bigquery.SchemaField(
                    "session_engaged",
                    "BOOLEAN",
                    description="エンゲージメントセッション",
                ),
                bigquery.SchemaField(
                    "engagement_time_msec",
                    "INTEGER",
                    description="エンゲージメント時間（ミリ秒）",
                ),
                bigquery.SchemaField(
                    "ga_session_id", "STRING", description="GAセッションID"
                ),
                bigquery.SchemaField(
                    "ga_session_number", "INTEGER", description="GAセッション番号"
                ),
                # イベント固有パラメータ（一部）
                bigquery.SchemaField("link_url", "STRING", description="リンクURL"),
                bigquery.SchemaField(
                    "link_text", "STRING", description="リンクテキスト"
                ),
                bigquery.SchemaField(
                    "link_classes", "STRING", description="リンククラス"
                ),
                bigquery.SchemaField("link_id", "STRING", description="リンクID"),
                bigquery.SchemaField("outbound", "BOOLEAN", description="外部リンク"),
                bigquery.SchemaField(
                    "percent_scrolled", "FLOAT", description="スクロール率"
                ),
                # Eコマース関連
                bigquery.SchemaField("currency", "STRING", description="通貨"),
                bigquery.SchemaField("value", "FLOAT", description="値"),
                bigquery.SchemaField(
                    "transaction_id", "STRING", description="トランザクションID"
                ),
                bigquery.SchemaField("tax", "FLOAT", description="税"),
                bigquery.SchemaField("shipping", "FLOAT", description="送料"),
                bigquery.SchemaField("item_id", "STRING", description="アイテムID"),
                bigquery.SchemaField("item_name", "STRING", description="アイテム名"),
                bigquery.SchemaField(
                    "item_quantity", "INTEGER", description="アイテム数量"
                ),
            ]

            # テーブルを作成
            table = bigquery.Table(table_ref, schema=schema)

            # パーティション設定
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="date"
            )

            # クラスタリング設定
            table.clustering_fields = ["event_name", "user_pseudo_id"]

            try:
                self.client.create_table(table)
                logger.info(f"テーブル{table_id}を作成しました")
                return True
            except Exception as e:
                logger.error(f"テーブル{table_id}の作成中にエラーが発生しました: {e}")
                return False

        except Exception as e:
            logger.error(f"テーブル{table_id}の確認中にエラーが発生しました: {e}")
            return False

    def _ensure_sessions_table_exists(self, table_id: str) -> bool:
        """セッションテーブルが存在することを確認し、存在しなければ作成する。"""
        table_ref = self.client.dataset(self.target_dataset).table(table_id)

        try:
            self.client.get_table(table_ref)
            logger.info(f"テーブル{table_id}は既に存在します")
            return True
        except NotFound:
            # テーブルが存在しない場合は作成
            logger.info(f"テーブル{table_id}を作成します")

            # テーブルスキーマを定義
            schema = [
                bigquery.SchemaField(
                    "user_pseudo_id", "STRING", description="匿名ユーザーID"
                ),
                bigquery.SchemaField(
                    "session_id", "STRING", description="セッションID"
                ),
                bigquery.SchemaField(
                    "session_start_time", "TIMESTAMP", description="セッション開始時間"
                ),
                bigquery.SchemaField(
                    "session_end_time", "TIMESTAMP", description="セッション終了時間"
                ),
                bigquery.SchemaField(
                    "session_duration_seconds",
                    "FLOAT",
                    description="セッション時間（秒）",
                ),
                bigquery.SchemaField(
                    "pageviews", "INTEGER", description="ページビュー数"
                ),
                bigquery.SchemaField(
                    "engagement_time_msec",
                    "INTEGER",
                    description="エンゲージメント時間（ミリ秒）",
                ),
                bigquery.SchemaField("referrer", "STRING", description="リファラー"),
                bigquery.SchemaField(
                    "device_category", "STRING", description="デバイスカテゴリ"
                ),
                bigquery.SchemaField("operating_system", "STRING", description="OS"),
                bigquery.SchemaField("country", "STRING", description="国"),
                bigquery.SchemaField("city", "STRING", description="都市"),
                bigquery.SchemaField(
                    "traffic_source", "STRING", description="トラフィックソース"
                ),
                bigquery.SchemaField(
                    "traffic_medium", "STRING", description="トラフィックメディアム"
                ),
                bigquery.SchemaField("date", "DATE", description="セッション日付"),
            ]

            # テーブルを作成
            table = bigquery.Table(table_ref, schema=schema)

            # パーティション設定
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="date"
            )

            # クラスタリング設定
            table.clustering_fields = ["user_pseudo_id"]

            try:
                self.client.create_table(table)
                logger.info(f"テーブル{table_id}を作成しました")
                return True
            except Exception as e:
                logger.error(f"テーブル{table_id}の作成中にエラーが発生しました: {e}")
                return False

        except Exception as e:
            logger.error(f"テーブル{table_id}の確認中にエラーが発生しました: {e}")
            return False

    def _ensure_user_profiles_table_exists(self, table_id: str) -> bool:
        """ユーザープロファイルテーブルが存在することを確認し、存在しなければ作成する。"""
        table_ref = self.client.dataset(self.target_dataset).table(table_id)

        try:
            self.client.get_table(table_ref)
            logger.info(f"テーブル{table_id}は既に存在します")
            return True
        except NotFound:
            # テーブルが存在しない場合は作成
            logger.info(f"テーブル{table_id}を作成します")

            # テーブルスキーマを定義
            schema = [
                bigquery.SchemaField(
                    "user_pseudo_id", "STRING", description="匿名ユーザーID"
                ),
                bigquery.SchemaField(
                    "first_seen", "TIMESTAMP", description="初回アクセス時間"
                ),
                bigquery.SchemaField(
                    "last_seen", "TIMESTAMP", description="最終アクセス時間"
                ),
                bigquery.SchemaField(
                    "session_count", "INTEGER", description="セッション数"
                ),
                bigquery.SchemaField(
                    "event_count", "INTEGER", description="イベント数"
                ),
                bigquery.SchemaField(
                    "most_used_device", "STRING", description="最も使用されるデバイス"
                ),
                bigquery.SchemaField(
                    "most_used_os", "STRING", description="最も使用されるOS"
                ),
                bigquery.SchemaField("country", "STRING", description="国"),
                bigquery.SchemaField(
                    "last_updated", "TIMESTAMP", description="最終更新時間"
                ),
            ]

            # テーブルを作成
            table = bigquery.Table(table_ref, schema=schema)

            try:
                self.client.create_table(table)
                logger.info(f"テーブル{table_id}を作成しました")
                return True
            except Exception as e:
                logger.error(f"テーブル{table_id}の作成中にエラーが発生しました: {e}")
                return False

        except Exception as e:
            logger.error(f"テーブル{table_id}の確認中にエラーが発生しました: {e}")
            return False

    def _delete_partition_if_exists(self, table_id: str, partition_suffix: str) -> bool:
        """パーティションが存在する場合は削除する。"""
        full_table_id = (
            f"{self.project_id}.{self.target_dataset}.{table_id}${partition_suffix}"
        )

        # パーティションが存在するか確認するクエリ
        query = f"""
        SELECT COUNT(*) as count
        FROM `{full_table_id}`
        """

        try:
            # クエリを実行
            result = self.client.query(query).result()

            # 結果を取得
            for row in result:
                if row.count > 0:
                    # パーティションにデータが存在する場合は削除
                    delete_query = f"""
                    DELETE FROM `{self.project_id}.{self.target_dataset}.{table_id}`
                    WHERE DATE(_PARTITIONTIME) = PARSE_DATE('%Y%m%d', '{partition_suffix}')
                    """

                    self.client.query(delete_query).result()
                    logger.info(
                        f"パーティション{partition_suffix}のデータを削除しました"
                    )

                    return True

            return False

        except Exception as e:
            # パーティションが存在しない場合はエラーが発生するが、無視して続行
            logger.info(
                f"パーティション{partition_suffix}は存在しないか、確認中にエラーが発生しました: {e}"
            )
            return False


# ローダーインスタンスを作成
loader = GA4Loader()
