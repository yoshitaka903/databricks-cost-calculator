# Databricks Cost Calculator

Databricksワークロードの料金を計算するStreamlitアプリケーションです。All-Purpose、Jobs、DLTなどのクラスター型ワークロードやSQL Warehouse Serverlessについて、DatabricksとAWS EC2の料金を自動計算し、スプレッドシート出力機能を提供します。

## 前提条件・制限事項

- **対象リージョン**: AWS 東京リージョン（ap-northeast-1）のみ
- **料金精度**: 算出価格は目安です。正確な金額は各ベンダーの公式情報をご確認ください
- **対応ワークロード**: All-Purpose、Jobs、DLT、SQL Warehouse Serverless

## プロジェクト構成

```
databricks-cost-calculator/
├── src/                           # デプロイ対象（本番用）
│   ├── app.py                    # メインStreamlitアプリケーション
│   ├── app.yaml                 # Databricks Apps設定
│   ├── requirements.txt          # 依存関係
│   └── data/                    # 料金データファイル
│       ├── databricks_compute_pricing_updated.json
│       ├── ec2_pricing_tokyo.json
│       ├── ec2_specs.json
│       ├── instance_dbu_rates.json
│       └── sql_warehouse_sizes.json
├── docs/                         # ドキュメント
│   ├── README.md
│   └── DEPLOY.md               # デプロイガイド
├── scripts/                      # ユーティリティスクリプト
│   ├── fetch_ec2_pricing.py    # EC2料金取得
│   ├── test_ec2_pricing.py     # テスト用スクリプト
│   ├── pricing_updater.py      # 料金データ更新
│   ├── process_pricing_data.py # データ処理
│   └── debug_aws_pricing.py    # デバッグ用
├── config/                       # 設定ファイル
├── blog_article_draft.md        # ハンズオン記事
└── README.md                    # このファイル
```

## 🚀 クイックスタート

### ローカル実行
```bash
# リポジトリをクローン
git clone https://github.com/yoshitaka903/databricks-cost-calculator.git
cd databricks-cost-calculator

# 依存関係のインストール
pip install -r src/requirements.txt

# アプリケーションの起動
streamlit run src/app.py
```

### Databricks Appsデプロイ
```bash
# srcディレクトリに移動
cd src

# Databricks Appsにデプロイ
databricks apps deploy .
```

**📋 デプロイに必要なファイル:**
- `app.py` - メインアプリケーション
- `app.yaml` - Databricks Apps設定
- `requirements.txt` - 依存関係
- `data/` - 料金データ

**👉 詳細手順**: [ハンズオン記事](./blog_article_draft.md)を参照

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
- **自動計算**: 設定変更時のリアルタイム更新

### データ出力・分析
- **Excel出力**: 詳細データ + サマリーシート
- **CSV出力**: 詳細データ
- **計算式表示**: 透明性のある料金計算プロセス
- **コスト比較**: 複数インスタンスタイプの並列比較

### 管理機能
- **ワークロード編集**: クラスター型・SQL Warehouse両対応
- **複数ワークロード管理**: 統合された料金表示
- **設定保存**: ブラウザセッション間での設定維持

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

- **フレームワーク**: Streamlit 1.28.0
- **Python**: 3.9+
- **主要ライブラリ**: pandas, openpyxl
- **データ形式**: JSON
- **デプロイ**: Databricks Apps対応

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 作者

- [@yoshitaka903](https://github.com/yoshitaka903)

---
**⭐ このプロジェクトが役に立った場合は、スターをつけていただけると嬉しいです！**