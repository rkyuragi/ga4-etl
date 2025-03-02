"""GA4データを抽出するモジュール。

BigQueryに保存されたGA4イベントデータを抽出し、分析用に整形する機能を提供します。
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from config import config
from src.utils import create_bq_client, get_partition_suffix

logger = logging.getLogger(__name__)


class GA4Extractor:
    """GA4データ抽出クラス。"""

    def __init__(self) -> None:
        """GA4データ抽出クラスの初期化。"""
        self.client = create_bq_client()
        self.project_id = config.project_id
        self.source_dataset = config.source_dataset

    def extract_events_for_date(self, target_date: str) -> pd.DataFrame:
        """特定の日付のGA4イベントデータを抽出する。

        Args:
            target_date (str): 対象日（YYYY-MM-DD形式）

        Returns:
            pandas.DataFrame: 抽出したデータのDataFrame
        """
        logger.info(f"{target_date}のGA4イベントデータを抽出します")

        # パーティション日付を取得（YYYYMMDD形式）
        partition_date = get_partition_suffix(target_date)

        # クエリの構築
        # GA4のeventsテーブルから必要なデータを抽出
        query = f"""
        SELECT
            event_date,
            event_timestamp,
            event_name,
            event_params,
            event_previous_timestamp,
            event_value_in_usd,
            event_bundle_sequence_id,
            event_server_timestamp_offset,
            user_id,
            user_pseudo_id,
            user_properties,
            user_first_touch_timestamp,
            user_ltv,
            device,
            geo,
            app_info,
            traffic_source,
            stream_id,
            platform,
            ecommerce,
            items
        FROM
            `{self.project_id}.{self.source_dataset}.{config.events_table}`
        WHERE
            _TABLE_SUFFIX = '{partition_date}'
        """

        try:
            # クエリを実行
            df = self.client.query(query).to_dataframe()

            # 結果の確認
            row_count = len(df)
            logger.info(f"{target_date}のデータを{row_count}行抽出しました")

            if row_count == 0:
                logger.warning(f"{target_date}のデータは0行でした")

            return df

        except Exception as e:
            logger.error(f"{target_date}のデータ抽出中にエラーが発生しました: {e}")
            raise

    def extract_event_params(self, df: pd.DataFrame, param_name: str) -> pd.Series:
        """イベントパラメータを抽出する。

        Args:
            df (pandas.DataFrame): イベントデータのDataFrame
            param_name (str): 抽出するパラメータ名

        Returns:
            pandas.Series: 抽出したパラメータの値
        """
        # event_paramsはARRAY<STRUCT<key STRING, value STRUCT<...>>>形式
        # 特定のキーを持つパラメータの値を抽出する

        def extract_param(
            params: Optional[List[Dict[str, Any]]], key: str
        ) -> Optional[Any]:
            """パラメータ配列から特定のキーの値を抽出する。"""
            if params is None:
                return None

            for param in params:
                if param["key"] == key:
                    # valueはSTRUCT<string_value, int_value, float_value, double_value>形式
                    value = param["value"]

                    # 値のタイプに応じて適切な値を返す
                    if "string_value" in value and value["string_value"] is not None:
                        return value["string_value"]
                    elif "int_value" in value and value["int_value"] is not None:
                        return value["int_value"]
                    elif "float_value" in value and value["float_value"] is not None:
                        return value["float_value"]
                    elif "double_value" in value and value["double_value"] is not None:
                        return value["double_value"]

            return None

        # 各行のevent_paramsから指定されたパラメータの値を抽出
        return df["event_params"].apply(
            lambda params: extract_param(params, param_name)
        )

    def extract_user_properties(
        self, df: pd.DataFrame, property_name: str
    ) -> pd.Series:
        """ユーザープロパティを抽出する。

        Args:
            df (pandas.DataFrame): イベントデータのDataFrame
            property_name (str): 抽出するプロパティ名

        Returns:
            pandas.Series: 抽出したプロパティの値
        """
        # user_propertiesはARRAY<STRUCT<key STRING, value STRUCT<...>>>形式
        # 特定のキーを持つプロパティの値を抽出する

        def extract_property(
            properties: Optional[List[Dict[str, Any]]], key: str
        ) -> Optional[Any]:
            """プロパティ配列から特定のキーの値を抽出する。"""
            if properties is None:
                return None

            for prop in properties:
                if prop["key"] == key:
                    # valueはSTRUCT<string_value, int_value, float_value, double_value, set_timestamp_micros>形式
                    value = prop["value"]

                    # 値のタイプに応じて適切な値を返す
                    if "string_value" in value and value["string_value"] is not None:
                        return value["string_value"]
                    elif "int_value" in value and value["int_value"] is not None:
                        return value["int_value"]
                    elif "float_value" in value and value["float_value"] is not None:
                        return value["float_value"]
                    elif "double_value" in value and value["double_value"] is not None:
                        return value["double_value"]

            return None

        # 各行のuser_propertiesから指定されたプロパティの値を抽出
        return df["user_properties"].apply(
            lambda props: extract_property(props, property_name)
        )


# GA4抽出インスタンスを作成
extractor = GA4Extractor()
