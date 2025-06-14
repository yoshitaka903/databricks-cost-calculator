# Databricks Cost Calculator

Databricksワークロードの料金を計算するStreamlitアプリケーションです。クラスター型ワークロードとSQL Warehouse Serverlessの両方に対応し、詳細な料金分析とスプレッドシート出力機能を提供します。

## フォルダ構成

```
databricks-cost-calculator/
├── src/                           # 本番デプロイ用ソース
│   ├── simple_app.py             # メインStreamlitアプリケーション
│   ├── app.py                    # 旧バージョン（参考用）
│   ├── requirements.txt          # 本番用依存関係
│   ├── app.yaml                 # Databricks Apps設定
│   └── data/                    # 本番用データファイル
│       ├── databricks_compute_pricing_updated.json  # Databricks料金データ
│       ├── ec2_pricing_tokyo.json                   # EC2料金データ（東京）
│       └── sql_warehouse_sizes.json                 # SQL Warehouseサイズ情報
├── dev/                          # 開発・テスト用
│   ├── streamlit_app.py         # 開発用アプリ
│   └── requirements-dev.txt     # 開発用依存関係
├── docs/                         # ドキュメント
│   ├── README.md               # 詳細ドキュメント
│   └── DEPLOY.md               # デプロイガイド
├── scripts/                      # ユーティリティスクリプト
│   ├── fetch_ec2_pricing.py    # EC2料金取得スクリプト
│   └── pricing_updater.py      # 料金データ更新スクリプト
└── data/                        # 共通データファイル
    ├── databricks_compute_pricing_updated.json
    ├── ec2_pricing_tokyo.json
    ├── instance_dbu_rates.json
    └── sql_warehouse_sizes.json
```

## ローカル開発環境での実行

```bash
# 依存関係のインストール
pip install -r src/requirements.txt

# アプリケーションの起動
streamlit run src/simple_app.py
```

## 本番デプロイ（Databricks Apps）

`src/`フォルダ全体をDatabricks Appsにデプロイしてください。

**必要なファイル:**
- `src/simple_app.py` - メインアプリケーション
- `src/app.yaml` - Databricks Apps設定
- `src/requirements.txt` - 依存関係
- `src/data/` - 料金データ

## 主な機能

### ワークロード設定
- **クラスター型ワークロード**: All-Purpose、Jobs、DLT-Advancedクラスター
- **SQL Warehouse**: サーバーレス型（サイズベース料金）
- **Executorインスタンス**: "Driverと同じ"オプション対応
- **利用時間設定**: 1日あたり時間 × 月間日数で自動計算

### 料金計算
- **DBU料金**: ワークロード別DBU単価 × DBU消費量
- **EC2料金**: インスタンス別時間単価 × 利用時間
- **Photon対応**: 専用料金体系とDBU消費量

### データ出力
- **Excel出力**: 詳細データ + サマリーシート
- **CSV出力**: 詳細データ
- **計算式表示**: 透明性のある料金計算プロセス

### 管理機能
- **ワークロード編集**: クラスター型・SQL Warehouse両対応
- **複数ワークロード管理**: 統合された料金表示
- **リアルタイム計算**: 設定変更時の即座な再計算

## データ仕様

### 対応リージョン
- **ap-northeast-1 (東京)**

### 料金データ
- **Databricks料金**: 6種類のワークロードパターン
  - all-purpose, jobs, dlt-advanced
  - all-purpose-photon, jobs-photon, dlt-advanced-photon
- **EC2料金**: 480+ インスタンスタイプ（97%+ カバー率）
- **SQL Warehouse**: サイズ別DBU消費量（2X-Small〜Large）

### DBU単価（参考値）
- **All-Purpose**: ワークロード別可変料金
- **Jobs**: ワークロード別可変料金
- **DLT-Advanced**: ワークロード別可変料金
- **SQL Warehouse**: $1.0/DBU（固定）

## 技術仕様

- **フレームワーク**: Streamlit
- **Python**: 3.9+
- **主要ライブラリ**: pandas, openpyxl, boto3
- **データ形式**: JSON
- **デプロイ**: Databricks Apps対応