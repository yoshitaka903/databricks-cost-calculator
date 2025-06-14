# Databricks Appsデプロイ手順

## 前提条件

- Databricks CLI v0.213.0+ をインストール済み
- Databricksワークスペースへのアクセス権限
- Databricks CLI認証設定済み: `databricks auth login`

## デプロイ手順

### 1. ワークスペースにファイルをアップロード

```bash
# プロジェクトのsrcディレクトリに移動
cd /path/to/your/databricks-cost-calculator/src

# ワークスペースにディレクトリをインポート
databricks workspace import-dir . /Workspace/Users/YOUR_EMAIL/databricks-cost-calculator --overwrite
```

### 2. Databricks Appsにデプロイ

```bash
# 初回デプロイ（アプリ作成とデプロイを同時に実行）
databricks apps deploy databricks-cost-calculator --source-code-path /Workspace/Users/YOUR_EMAIL/databricks-cost-calculator

# 後続のデプロイ（更新時はファイルアップロード後に実行）
databricks workspace import-dir . /Workspace/Users/YOUR_EMAIL/databricks-cost-calculator --overwrite
databricks apps deploy databricks-cost-calculator
```

### 3. アプリの確認とアクセス

```bash
# デプロイ状況確認
databricks apps list

# アプリの詳細確認
databricks apps describe databricks-cost-calculator
```

デプロイ完了後、出力されるアプリURLにブラウザでアクセスできます。

## 必要なファイル構成

```
src/
├── app.py                # メインアプリケーション
├── app.yaml             # Databricks Apps設定
├── requirements.txt     # 依存関係
└── data/               # 料金データ
    ├── databricks_compute_pricing_updated.json
    ├── ec2_pricing_tokyo.json
    └── sql_warehouse_sizes.json
```

### app.yaml
- アプリケーションの起動コマンドと環境変数を定義
- Streamlitサーバーの設定を含む

### requirements.txt
- 必要なPythonパッケージを定義
- デプロイ時に自動インストールされる

## よくある問題と解決策

| 問題 | 解決策 |
|------|--------|
| `error reading app.yaml file` | YAML構文エラー - commandをリスト形式で記述 |
| パッケージインストールエラー | requirements.txtのバージョン確認・修正 |
| `streamlit: command not found` | requirements.txtにstreamlit追加 |
| データファイルが見つからない | dataフォルダがsrc/内に存在することを確認 |
| アプリが起動しないエラー | app.yamlのcommandとポート設定確認 |

## アプリの管理コマンド

```bash
# アプリの状態確認
databricks apps list
databricks apps describe databricks-cost-calculator

# ログ確認
databricks apps logs databricks-cost-calculator

# アプリの停止・再開
databricks apps stop databricks-cost-calculator
databricks apps start databricks-cost-calculator

# アプリの削除
databricks apps delete databricks-cost-calculator
```

## 注意点

- デプロイ時は必ずsrcディレクトリから実行する
- app.yamlとrequirements.txtが正しく配置されていることを確認
- `YOUR_EMAIL`は実際のDatabricksアカウントのメールアドレスに置き換える
- ワークスペースパスは必ず`/Workspace/Users/`で始まる形式を使用する
- ファイル更新時は必ずworkspace import-dirを実行してからデプロイする