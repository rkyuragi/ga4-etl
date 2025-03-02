# ベースイメージとしてPython 3.9を使用
FROM python:3.9-slim

# 作業ディレクトリを設定
WORKDIR /app

# 必要なシステムパッケージをインストール
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Poetryをインストール
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

# Poetryの設定（仮想環境を作成しない）
RUN poetry config virtualenvs.create false

# 依存関係ファイルをコピー
COPY pyproject.toml poetry.lock* ./

# 依存関係をインストール
RUN poetry install --no-dev --no-interaction --no-ansi

# アプリケーションコードをコピー
COPY . .

# 実行権限を付与
RUN chmod +x main.py

# 環境変数の設定
ENV PYTHONUNBUFFERED=1

# エントリーポイントを設定
ENTRYPOINT ["python", "main.py"]

# デフォルト引数（日次処理モード）
CMD ["--mode", "daily"]
