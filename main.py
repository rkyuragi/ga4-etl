#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GA4 ETLパイプラインのメインモジュール。

GA4データの抽出、変換、ロードを行うETLパイプラインの実行を制御します。
日次処理と全量処理の両方をサポートしています。
"""

import argparse
import logging
import sys
import traceback
from typing import Any, Dict

from config import config
from src.extract import extractor
from src.load import loader
from src.notification import notify_error, notify_start, notify_success
from src.transform import transformer
from src.utils import format_error, get_date_range, get_target_date, setup_logger


def process_single_date(target_date: str) -> Dict[str, Any]:
    """単一日付のデータを処理する。

    Args:
        target_date (str): 処理対象日（YYYY-MM-DD形式）

    Returns:
        dict: 処理統計情報
    """
    logger = logging.getLogger(__name__)
    logger.info(f"{target_date}のデータ処理を開始します")

    stats = {
        "target_date": target_date,
        "events_processed": 0,
        "sessions_processed": 0,
        "users_processed": 0,
    }

    try:
        # 1. データ抽出
        events_df = extractor.extract_events_for_date(target_date)

        if events_df.empty:
            logger.warning(f"{target_date}のデータが空です")
            return stats

        # 抽出したイベント数を記録
        stats["events_extracted"] = len(events_df)

        # 2. データ変換
        transformed_events_df = transformer.transform_events(events_df)
        stats["events_processed"] = len(transformed_events_df)

        # セッションテーブルの作成
        sessions_df = transformer.create_user_sessions_table(transformed_events_df)
        stats["sessions_processed"] = len(sessions_df)

        # ユーザープロファイルテーブルの作成
        users_df = transformer.create_user_profile_table(transformed_events_df)
        stats["users_processed"] = len(users_df)

        # 3. データロード
        # イベントテーブルのロード
        events_loaded = loader.load_events_table(transformed_events_df, target_date)
        stats["events_loaded"] = events_loaded

        # セッションテーブルのロード
        sessions_loaded = loader.load_sessions_table(sessions_df, target_date)
        stats["sessions_loaded"] = sessions_loaded

        # ユーザープロファイルテーブルのロード
        users_loaded = loader.load_user_profiles(users_df)
        stats["users_loaded"] = users_loaded

        logger.info(f"{target_date}のデータ処理が完了しました")
        return stats

    except Exception as e:
        error_message = format_error(e)
        logger.error(
            f"{target_date}のデータ処理中にエラーが発生しました: {error_message}"
        )
        logger.error(traceback.format_exc())
        raise


def run_daily_process() -> int:
    """日次処理を実行する。

    Returns:
        int: 終了コード（0: 成功、1: 失敗）
    """
    logger = logging.getLogger(__name__)
    logger.info("GA4 ETL日次処理を開始します")

    # 処理対象日を取得
    target_date = get_target_date(config.days_back)
    logger.info(f"処理対象日: {target_date}")

    # 処理開始を通知
    notify_start(mode="daily", target_date=target_date)

    try:
        # 単一日付の処理を実行
        stats = process_single_date(target_date)

        # 処理成功を通知
        notify_success(mode="daily", target_date=target_date, stats=stats)

        logger.info("GA4 ETL日次処理が正常に完了しました")
        return 0

    except Exception as e:
        error_message = format_error(e)
        logger.error(f"GA4 ETL日次処理中にエラーが発生しました: {error_message}")
        logger.error(traceback.format_exc())

        # エラーを通知
        notify_error(error_message, mode="daily", target_date=target_date)

        return 1


def run_full_process() -> int:
    """全量処理を実行する。

    Returns:
        int: 終了コード（0: 成功、1: 失敗）
    """
    logger = logging.getLogger(__name__)
    logger.info("GA4 ETL全量処理を開始します")

    # 開始日と終了日を取得
    start_date = config.start_date
    end_date = config.end_date

    if not start_date or not end_date:
        error_message = "全量処理には開始日と終了日の指定が必要です"
        logger.error(error_message)
        notify_error(error_message, mode="full")
        return 1

    logger.info(f"処理対象期間: {start_date} から {end_date}")

    # 処理開始を通知
    notify_start(mode="full")

    try:
        # 日付範囲を取得
        date_range = get_date_range(start_date, end_date)

        # 各日付を処理
        all_stats = []
        success_count = 0
        error_count = 0

        for target_date in date_range:
            try:
                logger.info(f"日付 {target_date} の処理を開始します")
                stats = process_single_date(target_date)
                all_stats.append(stats)
                success_count += 1
                logger.info(f"日付 {target_date} の処理が完了しました")
            except Exception as e:
                error_message = format_error(e)
                logger.error(
                    f"日付 {target_date} の処理中にエラーが発生しました: {error_message}"
                )
                error_count += 1

        # 集計統計
        total_stats = {
            "total_dates": len(date_range),
            "success_count": success_count,
            "error_count": error_count,
            "total_events_processed": sum(
                s.get("events_processed", 0) for s in all_stats
            ),
            "total_sessions_processed": sum(
                s.get("sessions_processed", 0) for s in all_stats
            ),
            "total_users_processed": sum(
                s.get("users_processed", 0) for s in all_stats
            ),
        }

        # 処理成功を通知
        notify_success(mode="full", stats=total_stats)

        logger.info("GA4 ETL全量処理が完了しました")

        # エラーがあった場合は非ゼロの終了コードを返す
        return 1 if error_count > 0 else 0

    except Exception as e:
        error_message = format_error(e)
        logger.error(f"GA4 ETL全量処理中にエラーが発生しました: {error_message}")
        logger.error(traceback.format_exc())

        # エラーを通知
        notify_error(error_message, mode="full")

        return 1


def main() -> int:
    """メイン関数。

    Returns:
        int: 終了コード（0: 成功、1: 失敗）
    """
    # コマンドライン引数の解析
    parser = argparse.ArgumentParser(description="GA4 ETL処理")
    parser.add_argument(
        "--mode",
        choices=["daily", "full"],
        default="daily",
        help="処理モード（daily: 日次処理, full: 全量処理）",
    )
    parser.add_argument("--start-date", help="全量処理の開始日（YYYY-MM-DD形式）")
    parser.add_argument("--end-date", help="全量処理の終了日（YYYY-MM-DD形式）")
    parser.add_argument(
        "--days-back", type=int, help="何日前のデータを処理するか（日次処理時）"
    )

    args = parser.parse_args()

    # 引数から設定を上書き
    if args.mode:
        config.processing_mode = args.mode

    if args.start_date:
        config.start_date = args.start_date

    if args.end_date:
        config.end_date = args.end_date

    if args.days_back is not None:
        config.days_back = args.days_back

    # 設定の検証
    try:
        config.validate()
    except ValueError as e:
        print(f"設定エラー: {e}")
        return 1

    # ロガーの設定
    setup_logger()

    # 処理モードに応じて実行
    if config.processing_mode == "daily":
        return run_daily_process()
    else:
        return run_full_process()


if __name__ == "__main__":
    sys.exit(main())
