"""Slack通知機能を提供するモジュール。

ETL処理の開始、成功、エラーなどの状態をSlackに通知する機能を提供します。
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from slack_sdk.webhook import WebhookClient

from config import config

logger = logging.getLogger(__name__)


class SlackNotifier:
    """Slack通知クラス。"""

    def __init__(self) -> None:
        """Slack通知クラスの初期化。"""
        self.webhook_url = config.slack_webhook_url

        if not self.webhook_url:
            logger.warning(
                "Slack Webhook URLが設定されていません。Slack通知は無効です。"
            )

    def send_notification(
        self,
        message: str,
        status: str = "info",
        attachments: Optional[List[str]] = None,
    ) -> bool:
        """Slackに通知を送信する。

        Args:
            message (str): 通知メッセージ
            status (str): ステータス（"success", "warning", "error", "info"のいずれか）
            attachments (list, optional): 添付ファイル

        Returns:
            bool: 送信成功時はTrue、失敗時はFalse
        """
        if not self.webhook_url:
            logger.info(f"Slack通知（無効）: {message}")
            return False

        # ステータスに応じた絵文字とカラーを設定
        status_emoji = {
            "success": ":white_check_mark:",
            "warning": ":warning:",
            "error": ":x:",
            "info": ":information_source:",
        }

        emoji = status_emoji.get(status, ":information_source:")
        # colorは使用されていないため削除

        # 現在時刻
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # メッセージブロックを作成
        blocks = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"{emoji} *GA4 ETL処理通知*"},
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": message}},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"処理モード: `{config.processing_mode}` | 実行時間: {now}",
                    }
                ],
            },
            {"type": "divider"},
        ]

        # 添付ファイルがあれば追加
        if attachments:
            for attachment in attachments:
                blocks.append(
                    {"type": "section", "text": {"type": "mrkdwn", "text": attachment}}
                )

        try:
            # WebhookClientを使用して送信
            webhook = WebhookClient(self.webhook_url)
            response = webhook.send(
                text=f"GA4 ETL処理通知: {message}",
                blocks=blocks,
            )

            if response.status_code == 200:
                logger.info(f"Slack通知を送信しました: {message}")
                return True
            else:
                logger.error(
                    f"Slack通知の送信に失敗しました: {response.status_code}, {response.body}"
                )
                return False

        except Exception as e:
            logger.exception(f"Slack通知の送信中にエラーが発生しました: {e}")
            return False


# Slack通知インスタンスを作成
notifier = SlackNotifier()


def notify_start(mode: str = "daily", target_date: Optional[str] = None) -> bool:
    """ETL処理開始を通知する。

    Args:
        mode (str): 処理モード（"daily"または"full"）
        target_date (str, optional): 対象日（YYYY-MM-DD形式）

    Returns:
        bool: 送信成功時はTrue、失敗時はFalse
    """
    if mode == "daily":
        message = f"GA4 ETL日次処理を開始しました。対象日: {target_date}"
    else:
        message = f"GA4 ETL全量処理を開始しました。期間: {config.start_date} から {config.end_date}"

    return notifier.send_notification(message, status="info")


def notify_success(
    mode: str = "daily",
    target_date: Optional[str] = None,
    stats: Optional[Dict[str, Any]] = None,
) -> bool:
    """ETL処理成功を通知する。

    Args:
        mode (str): 処理モード（"daily"または"full"）
        target_date (str, optional): 対象日（YYYY-MM-DD形式）
        stats (dict, optional): 処理統計情報

    Returns:
        bool: 送信成功時はTrue、失敗時はFalse
    """
    if mode == "daily":
        message = f"GA4 ETL日次処理が正常に完了しました。対象日: {target_date}"
    else:
        message = f"GA4 ETL全量処理が正常に完了しました。期間: {config.start_date} から {config.end_date}"

    # 統計情報があれば追加
    attachments = []
    if stats:
        stats_text = "*処理統計:*\n"
        for key, value in stats.items():
            stats_text += f"• {key}: {value}\n"
        attachments.append(stats_text)

    return notifier.send_notification(
        message, status="success", attachments=attachments
    )


def notify_error(
    error_message: str, mode: str = "daily", target_date: Optional[str] = None
) -> bool:
    """ETL処理エラーを通知する。

    Args:
        error_message (str): エラーメッセージ
        mode (str): 処理モード（"daily"または"full"）
        target_date (str, optional): 対象日（YYYY-MM-DD形式）

    Returns:
        bool: 送信成功時はTrue、失敗時はFalse
    """
    if mode == "daily":
        message = f"GA4 ETL日次処理中にエラーが発生しました。対象日: {target_date}"
    else:
        message = f"GA4 ETL全量処理中にエラーが発生しました。期間: {config.start_date} から {config.end_date}"

    attachments = [f"*エラー詳細:*\n```{error_message}```"]

    return notifier.send_notification(message, status="error", attachments=attachments)
