# Databricks Appsデプロイ手順

## 前提条件

- Python環境とDatabricks CLIをセットアップ済み
- Databricksワークスペースへのアクセス権限
- 環境変数の設定: `DATABRICKS_USER_EMAIL`を自分のメールアドレスに設定

## デプロイ手順

### 1. ソースファイルをDatabricksに同期

```bash
# プロジェクトディレクトリに移動
cd /Users/tyoshimura/Document/claude/databricks-cost-calculator

# ソースファイルをDatabricksワークスペースに同期
databricks sync --watch . /Workspace/Users/YOUR_EMAIL/databricks-cost-calculator
```

### 2. Databricks Appsにデプロイ

```bash
# 初回デプロイ
databricks apps deploy databricks-cost-calculator --source-code-path /Workspace/Users/YOUR_EMAIL/databricks-cost-calculator

# 後続のデプロイ（パス省略可能）
databricks apps deploy databricks-cost-calculator
```

### 3. アプリにアクセス

デプロイ完了後、ブラウザでアプリURLを開いてアクセスできます。

## 設定ファイル

### app.yaml
- アプリケーションの起動コマンドと環境変数を定義
- Streamlitサーバーの設定を含む

### requirements.txt
- 必要なPythonパッケージを定義
- デプロイ時に自動インストールされる

## トラブルシューティング

| 問題 | 解決策 |
|------|--------|
| パッケージがない/バージョン違い | requirements.txtに追加・修正 |
| 権限の問題 | アプリにリソースアクセス権を付与 |
| 環境変数がない | app.yamlのenvセクションに追加 |
| 起動コマンドエラー | app.yamlのcommandセクションを修正 |

## ログ確認

```bash
# アプリのログを確認
databricks apps logs databricks-cost-calculator
```

## アプリの管理

```bash
# アプリの状態確認
databricks apps list

# アプリの停止
databricks apps stop databricks-cost-calculator

# アプリの削除
databricks apps delete databricks-cost-calculator
```