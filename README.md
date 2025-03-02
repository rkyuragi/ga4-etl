# GA4 ETL パイプライン

Google Analytics 4（GA4）のデータをBigQueryから抽出し、解析しやすいデータ構造に変換して再度BigQueryにロードするETL（Extract, Transform, Load）パイプラインです。

## 機能

- GA4のイベントデータを抽出し、フラット化された構造に変換
- セッションテーブルとユーザープロファイルテーブルの自動生成
- 日次バッチ処理と全量処理の両方をサポート
- Cloud Run Jobsによる実行
- Slack通知機能によるエラー監視

## 生成されるテーブル

1. **events** - フラット化されたイベントデータ
   - 日付パーティション分割
   - event_nameとuser_pseudo_idでクラスタリング

2. **sessions** - セッション単位の集計データ
   - 日付パーティション分割
   - user_pseudo_idでクラスタリング

3. **user_profiles** - ユーザープロファイル情報
   - 非パーティション
   - 増分更新（新規ユーザーの追加と既存ユーザーの更新）

## 前提条件

- Google Cloud Projectのセットアップ
- BigQueryのGA4エクスポートが設定済み
- Cloud Run Jobsの実行権限
- Slack Webhook URL（通知機能を使用する場合）
- Poetry（依存関係管理ツール）

## 環境変数

| 変数名 | 説明 | デフォルト値 |
|--------|------|-------------|
| GCP_PROJECT_ID | Google Cloud プロジェクトID | (必須) |
| BQ_SOURCE_DATASET | GA4データのソースデータセット | analytics_XXXXXXXX |
| BQ_TARGET_DATASET | 処理後のデータを格納するデータセット | ga4_processed |
| GA4_EVENTS_TABLE | GA4イベントテーブル名 | events_* |
| PROCESSING_MODE | 処理モード（daily または full） | daily |
| DAYS_BACK | 日次処理時の何日前のデータを処理するか | 1 |
| START_DATE | 全量処理の開始日（YYYY-MM-DD形式） | (全量処理時必須) |
| END_DATE | 全量処理の終了日（YYYY-MM-DD形式） | (全量処理時必須) |
| SLACK_WEBHOOK_URL | Slack通知用のWebhook URL | (オプション) |
| SLACK_CHANNEL | 通知先のSlackチャンネル | #ga4-etl-notifications |
| SLACK_USERNAME | 通知時に表示するユーザー名 | GA4 ETL Bot |
| LOG_LEVEL | ログレベル | INFO |

## 開発環境のセットアップ

### Poetryのインストール

```bash
# Poetryのインストール
curl -sSL https://install.python-poetry.org | python3 -

# パスを通す（必要に応じて）
export PATH="$HOME/.local/bin:$PATH"
```

### 依存関係のインストール

```bash
# プロジェクトディレクトリで実行
poetry install
```

### コード品質ツールのセットアップ

このプロジェクトでは以下のコード品質ツールを使用しています：

- **black**: Pythonコードフォーマッター
- **isort**: インポート文の整理
- **flake8**: コードリンター
- **mypy**: 静的型チェッカー
- **pre-commit**: コミット前にこれらのチェックを自動実行

pre-commitフックをインストールするには：

```bash
# pre-commitフックのインストール
pre-commit install
```

これにより、コミット時に自動的にコード品質チェックが実行されます。

#### 手動でのコード品質チェック

```bash
# コードフォーマット
poetry run black .

# インポート文の整理
poetry run isort .

# リンター
poetry run flake8

# 型チェック
poetry run mypy src

# すべてのpre-commitフックを実行
poetry run pre-commit run --all-files
```

### 環境変数の設定

`.env.sample`ファイルを`.env`にコピーして、必要な環境変数を設定します。

```bash
cp .env.sample .env
# .envファイルを編集して必要な値を設定
```

## ローカルでの実行方法

### Poetryの仮想環境内で実行

1. 日次処理の実行

```bash
poetry run python main.py --mode daily --days-back 1
```

2. 全量処理の実行

```bash
poetry run python main.py --mode full --start-date 2023-01-01 --end-date 2023-01-31
```

### または、Poetryのスクリプトを使用して実行

```bash
# 日次処理
poetry run ga4-etl --mode daily --days-back 1

# 全量処理
poetry run ga4-etl --mode full --start-date 2023-01-01 --end-date 2023-01-31
```

## Cloud Run Jobsでの実行

### 手動デプロイ

```bash
# イメージのビルド
docker build -t gcr.io/[PROJECT_ID]/ga4-etl:latest .

# イメージのプッシュ
docker push gcr.io/[PROJECT_ID]/ga4-etl:latest

# Cloud Run Jobの作成
gcloud run jobs create ga4-etl-job \
  --image gcr.io/[PROJECT_ID]/ga4-etl:latest \
  --region asia-northeast1 \
  --memory 2Gi \
  --cpu 1 \
  --max-retries 3 \
  --task-timeout 3600s \
  --set-env-vars GCP_PROJECT_ID=[PROJECT_ID] \
  --set-secrets SLACK_WEBHOOK_URL=ga4-etl-slack-webhook:latest

# ジョブの実行
gcloud run jobs execute ga4-etl-job --region asia-northeast1
```

### Cloud Buildによる自動デプロイ

リポジトリをCloud Source Repositoriesにプッシュし、Cloud Buildトリガーを設定することで、コードの変更時に自動的にデプロイできます。

```bash
# Cloud Buildトリガーの作成
gcloud builds triggers create github \
  --repo-name=ga4-etl \
  --branch-pattern=main \
  --build-config=cloudbuild.yaml
```

### 定期実行の設定

Cloud Schedulerを使用して、ジョブを定期的に実行するよう設定できます。

```bash
# 毎日午前3時に実行するスケジュールの作成
gcloud scheduler jobs create http ga4-etl-daily-job \
  --schedule="0 3 * * *" \
  --uri="https://asia-northeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/[PROJECT_ID]/jobs/ga4-etl-job:run" \
  --http-method=POST \
  --oauth-service-account-email=[PROJECT_NUMBER]-compute@developer.gserviceaccount.com \
  --location=asia-northeast1
```

## Slack通知

処理の開始、成功、エラー時にSlackに通知が送信されます。通知には以下の情報が含まれます：

- 処理モード（日次/全量）
- 対象日付または期間
- 処理統計（処理されたイベント数、セッション数、ユーザー数など）
- エラー発生時はエラーの詳細

## ライセンス

MIT
