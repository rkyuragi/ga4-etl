"""GA4データを変換するモジュール。

抽出されたGA4イベントデータを分析しやすい形式に変換し、セッションやユーザープロファイルを作成する機能を提供します。
"""

import logging
from datetime import datetime

import pandas as pd

from src.extract import extractor

logger = logging.getLogger(__name__)


class GA4Transformer:
    """GA4データ変換クラス。"""

    def __init__(self) -> None:
        """GA4データ変換クラスの初期化。"""
        pass

    def transform_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """GA4イベントデータを変換する。

        Args:
            df (pandas.DataFrame): 抽出したGA4イベントデータ

        Returns:
            pandas.DataFrame: 変換後のデータ
        """
        if df.empty:
            logger.warning("変換対象のデータが空です")
            return pd.DataFrame()

        logger.info(f"GA4イベントデータの変換を開始します（{len(df)}行）")

        # 変換前のデータ構造を確認
        logger.info(f"変換前のカラム: {df.columns.tolist()}")

        # 基本的なカラムをコピー
        transformed_df = df[
            [
                "event_date",
                "event_timestamp",
                "event_name",
                "user_id",
                "user_pseudo_id",
                "platform",
            ]
        ].copy()

        # 日付と時刻の変換
        transformed_df["date"] = pd.to_datetime(
            transformed_df["event_date"], format="%Y%m%d"
        )
        transformed_df["timestamp"] = pd.to_datetime(
            transformed_df["event_timestamp"], unit="us"
        )

        # デバイス情報の展開
        if "device" in df.columns:
            transformed_df["device_category"] = df["device"].apply(
                lambda x: x.get("category") if x else None
            )
            transformed_df["device_mobile_brand_name"] = df["device"].apply(
                lambda x: x.get("mobile_brand_name") if x else None
            )
            transformed_df["device_mobile_model_name"] = df["device"].apply(
                lambda x: x.get("mobile_model_name") if x else None
            )
            transformed_df["device_operating_system"] = df["device"].apply(
                lambda x: x.get("operating_system") if x else None
            )
            transformed_df["device_language"] = df["device"].apply(
                lambda x: x.get("language") if x else None
            )

        # 地理情報の展開
        if "geo" in df.columns:
            transformed_df["geo_country"] = df["geo"].apply(
                lambda x: x.get("country") if x else None
            )
            transformed_df["geo_region"] = df["geo"].apply(
                lambda x: x.get("region") if x else None
            )
            transformed_df["geo_city"] = df["geo"].apply(
                lambda x: x.get("city") if x else None
            )

        # トラフィックソース情報の展開
        if "traffic_source" in df.columns:
            transformed_df["traffic_source_name"] = df["traffic_source"].apply(
                lambda x: x.get("name") if x else None
            )
            transformed_df["traffic_source_medium"] = df["traffic_source"].apply(
                lambda x: x.get("medium") if x else None
            )
            transformed_df["traffic_source_source"] = df["traffic_source"].apply(
                lambda x: x.get("source") if x else None
            )

        # 共通のイベントパラメータを抽出
        common_params = [
            "page_location",
            "page_title",
            "page_referrer",
            "session_id",
            "session_engaged",
            "engagement_time_msec",
            "ga_session_id",
            "ga_session_number",
        ]

        for param in common_params:
            transformed_df[param] = extractor.extract_event_params(df, param)

        # イベント固有のパラメータを抽出
        # ここでは一般的なイベントタイプに対応するパラメータを抽出
        self._extract_page_view_params(df, transformed_df)
        self._extract_click_params(df, transformed_df)
        self._extract_scroll_params(df, transformed_df)
        self._extract_ecommerce_params(df, transformed_df)

        # 不要なカラムを削除
        transformed_df = transformed_df.drop(["event_date", "event_timestamp"], axis=1)

        logger.info(f"GA4イベントデータの変換が完了しました（{len(transformed_df)}行）")
        logger.info(f"変換後のカラム: {transformed_df.columns.tolist()}")

        return transformed_df

    def _extract_page_view_params(
        self, source_df: pd.DataFrame, target_df: pd.DataFrame
    ) -> None:
        """ページビューイベント固有のパラメータを抽出する。"""
        # page_viewイベントのみをフィルタリング
        page_view_df = source_df[source_df["event_name"] == "page_view"]

        if not page_view_df.empty:
            # page_viewイベント固有のパラメータを抽出
            page_params = ["page_location", "page_title", "page_referrer"]

            # インデックスを使用して元のDataFrameにマッピング
            for param in page_params:
                param_values = extractor.extract_event_params(page_view_df, param)
                if not param_values.empty:
                    # 既に抽出済みの場合は上書きしない
                    if param not in target_df.columns:
                        # インデックスを使用して元のDataFrameにマッピング
                        target_df.loc[page_view_df.index, param] = param_values

    def _extract_click_params(
        self, source_df: pd.DataFrame, target_df: pd.DataFrame
    ) -> None:
        """クリックイベント固有のパラメータを抽出する。"""
        # clickイベントのみをフィルタリング
        click_df = source_df[source_df["event_name"] == "click"]

        if not click_df.empty:
            # clickイベント固有のパラメータを抽出
            click_params = [
                "link_url",
                "link_text",
                "link_classes",
                "link_id",
                "outbound",
            ]

            for param in click_params:
                param_values = extractor.extract_event_params(click_df, param)
                if not param_values.empty:
                    # インデックスを使用して元のDataFrameにマッピング
                    target_df.loc[click_df.index, param] = param_values

    def _extract_scroll_params(
        self, source_df: pd.DataFrame, target_df: pd.DataFrame
    ) -> None:
        """スクロールイベント固有のパラメータを抽出する。"""
        # scrollイベントのみをフィルタリング
        scroll_df = source_df[source_df["event_name"] == "scroll"]

        if not scroll_df.empty:
            # scrollイベント固有のパラメータを抽出
            scroll_params = ["percent_scrolled"]

            for param in scroll_params:
                param_values = extractor.extract_event_params(scroll_df, param)
                if not param_values.empty:
                    # インデックスを使用して元のDataFrameにマッピング
                    target_df.loc[scroll_df.index, param] = param_values

    def _extract_ecommerce_params(
        self, source_df: pd.DataFrame, target_df: pd.DataFrame
    ) -> None:
        """Eコマースイベント固有のパラメータを抽出する。"""
        # Eコマース関連イベントをフィルタリング
        ecommerce_events = ["view_item", "add_to_cart", "begin_checkout", "purchase"]
        ecommerce_df = source_df[source_df["event_name"].isin(ecommerce_events)]

        if not ecommerce_df.empty:
            # Eコマース共通パラメータを抽出
            ecommerce_params = [
                "currency",
                "value",
                "transaction_id",
                "tax",
                "shipping",
            ]

            for param in ecommerce_params:
                param_values = extractor.extract_event_params(ecommerce_df, param)
                if not param_values.empty:
                    # インデックスを使用して元のDataFrameにマッピング
                    target_df.loc[ecommerce_df.index, param] = param_values

            # itemsの処理（複雑なため簡略化）
            if "items" in source_df.columns:
                # 最初のアイテムの情報のみを抽出
                target_df.loc[ecommerce_df.index, "item_id"] = ecommerce_df[
                    "items"
                ].apply(
                    lambda items: items[0].get("item_id")
                    if items and len(items) > 0
                    else None
                )
                target_df.loc[ecommerce_df.index, "item_name"] = ecommerce_df[
                    "items"
                ].apply(
                    lambda items: items[0].get("item_name")
                    if items and len(items) > 0
                    else None
                )
                target_df.loc[ecommerce_df.index, "item_quantity"] = ecommerce_df[
                    "items"
                ].apply(
                    lambda items: items[0].get("quantity")
                    if items and len(items) > 0
                    else None
                )

    def create_user_sessions_table(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """ユーザーセッションテーブルを作成する。

        Args:
            events_df (pandas.DataFrame): 変換済みのイベントデータ

        Returns:
            pandas.DataFrame: ユーザーセッションテーブル
        """
        if events_df.empty:
            logger.warning("ユーザーセッションテーブル作成のためのデータが空です")
            return pd.DataFrame()

        logger.info("ユーザーセッションテーブルの作成を開始します")

        # セッションIDが存在することを確認
        if "ga_session_id" not in events_df.columns:
            logger.error("ga_session_idカラムが存在しません")
            return pd.DataFrame()

        # セッションごとにグループ化
        sessions = events_df.groupby(["user_pseudo_id", "ga_session_id"])

        # セッション情報を集計
        session_data = []

        for (user_id, session_id), session_events in sessions:
            # セッションの開始時間と終了時間
            start_time = session_events["timestamp"].min()
            end_time = session_events["timestamp"].max()

            # セッション時間（秒）
            session_duration = (end_time - start_time).total_seconds()

            # ページビュー数
            pageviews = len(session_events[session_events["event_name"] == "page_view"])

            # エンゲージメント時間（ミリ秒）
            engagement_time = session_events["engagement_time_msec"].sum()

            # 最初のページのリファラー
            first_event = session_events.iloc[0]
            referrer = first_event.get("page_referrer", None)

            # デバイス情報
            device_category = first_event.get("device_category", None)
            operating_system = first_event.get("device_operating_system", None)

            # 地域情報
            country = first_event.get("geo_country", None)
            city = first_event.get("geo_city", None)

            # トラフィックソース
            source = first_event.get("traffic_source_source", None)
            medium = first_event.get("traffic_source_medium", None)

            # セッションデータを追加
            session_data.append(
                {
                    "user_pseudo_id": user_id,
                    "session_id": session_id,
                    "session_start_time": start_time,
                    "session_end_time": end_time,
                    "session_duration_seconds": session_duration,
                    "pageviews": pageviews,
                    "engagement_time_msec": engagement_time,
                    "referrer": referrer,
                    "device_category": device_category,
                    "operating_system": operating_system,
                    "country": country,
                    "city": city,
                    "traffic_source": source,
                    "traffic_medium": medium,
                    "date": first_event["date"],
                }
            )

        # DataFrameを作成
        sessions_df = pd.DataFrame(session_data)

        logger.info(
            f"ユーザーセッションテーブルの作成が完了しました（{len(sessions_df)}行）"
        )

        return sessions_df

    def create_user_profile_table(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """ユーザープロファイルテーブルを作成する。

        Args:
            events_df (pandas.DataFrame): 変換済みのイベントデータ

        Returns:
            pandas.DataFrame: ユーザープロファイルテーブル
        """
        if events_df.empty:
            logger.warning("ユーザープロファイルテーブル作成のためのデータが空です")
            return pd.DataFrame()

        logger.info("ユーザープロファイルテーブルの作成を開始します")

        # ユーザーごとにグループ化
        users = events_df.groupby("user_pseudo_id")

        # ユーザー情報を集計
        user_data = []

        for user_id, user_events in users:
            # 最初と最後のイベント時間
            first_seen = user_events["timestamp"].min()
            last_seen = user_events["timestamp"].max()

            # セッション数
            if "ga_session_id" in user_events.columns:
                session_count = user_events["ga_session_id"].nunique()
            else:
                session_count = 0

            # よく使うデバイス
            if "device_category" in user_events.columns:
                device_counts = user_events["device_category"].value_counts()
                most_used_device = (
                    device_counts.index[0] if not device_counts.empty else None
                )
            else:
                most_used_device = None

            # よく使うOS
            if "device_operating_system" in user_events.columns:
                os_counts = user_events["device_operating_system"].value_counts()
                most_used_os = os_counts.index[0] if not os_counts.empty else None
            else:
                most_used_os = None

            # 国
            if "geo_country" in user_events.columns:
                country_counts = user_events["geo_country"].value_counts()
                most_common_country = (
                    country_counts.index[0] if not country_counts.empty else None
                )
            else:
                most_common_country = None

            # ユーザーデータを追加
            user_data.append(
                {
                    "user_pseudo_id": user_id,
                    "first_seen": first_seen,
                    "last_seen": last_seen,
                    "session_count": session_count,
                    "event_count": len(user_events),
                    "most_used_device": most_used_device,
                    "most_used_os": most_used_os,
                    "country": most_common_country,
                    "last_updated": datetime.now(),
                }
            )

        # DataFrameを作成
        users_df = pd.DataFrame(user_data)

        logger.info(
            f"ユーザープロファイルテーブルの作成が完了しました（{len(users_df)}行）"
        )

        return users_df


# 変換インスタンスを作成
transformer = GA4Transformer()
